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
const antfly = @import("../../cli_root.zig");
const antfly_client = @import("antfly-client");
const cli = @import("mod.zig");
const index_readiness = @import("index_readiness.zig");
const platform_time = antfly.platform_time;

const default_wait_timeout_ms: u64 = 10 * 60 * 1000;
const default_wait_poll_ms: u64 = 1000;
// Preserve the HTTP client's normal per-attempt ceiling even when the overall
// wait budget is much larger, so stalled requests cannot suppress retries and
// progress reporting for minutes.
const max_wait_request_timeout_ms: u64 = 30_000;
const max_wait_retry_delay_ms: u64 = 5000;
const wait_progress_report_interval_ns: u64 = 10 * std.time.ns_per_s;

pub fn run(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    var command_args = args.*;
    const route = parseRoute(args.*);
    if (route.missing_value_arg) |arg| cli.fatal("{s} requires a value", .{arg});
    if (route.duplicate_arg) |arg| cli.fatal("{s} may only be provided once", .{arg});
    if (route.unknown_arg) |arg| cli.fatal("unknown index option or subcommand: {s}", .{arg});
    const tbl = route.table_name orelse cli.fatal("--table is required for index commands", .{});

    if (route.subcommand) |cmd| {
        if (std.mem.eql(u8, cmd, "create")) return createIndex(allocator, client, tbl, &command_args);
        if (std.mem.eql(u8, cmd, "drop")) return dropIndex(client, tbl, null, &command_args);
        if (std.mem.eql(u8, cmd, "list")) return listIndexes(allocator, io, client, tbl, &command_args);
        if (std.mem.eql(u8, cmd, "get")) return getIndex(allocator, io, client, tbl, null, &command_args);
        if (std.mem.eql(u8, cmd, "wait")) return waitForIndex(allocator, io, client, tbl, null, &command_args);
        cli.fatal("unknown index subcommand: {s}", .{cmd});
    }

    if (route.index_name) |idx| {
        _ = idx;
        return getIndex(allocator, io, client, tbl, null, &command_args);
    }
    return listIndexes(allocator, io, client, tbl, &command_args);
}

const Route = struct {
    table_name: ?[]const u8 = null,
    index_name: ?[]const u8 = null,
    subcommand: ?[]const u8 = null,
    unknown_arg: ?[]const u8 = null,
    duplicate_arg: ?[]const u8 = null,
    missing_value_arg: ?[]const u8 = null,
};

fn nextRouteValue(
    args: *std.process.Args.Iterator,
    flag: []const u8,
    missing_value_arg: *?[]const u8,
) ?[]const u8 {
    const value = args.next() orelse {
        missing_value_arg.* = missing_value_arg.* orelse flag;
        return null;
    };
    if (std.mem.startsWith(u8, value, "-")) {
        missing_value_arg.* = missing_value_arg.* orelse flag;
        return null;
    }
    return value;
}

fn parseRoute(iterator: std.process.Args.Iterator) Route {
    var args = iterator;
    var route: Route = .{};
    var create_only_arg: ?[]const u8 = null;
    var list_only_arg: ?[]const u8 = null;
    var wait_only_arg: ?[]const u8 = null;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            if (route.table_name != null and route.duplicate_arg == null) route.duplicate_arg = arg;
            route.table_name = nextRouteValue(&args, arg, &route.missing_value_arg);
        } else if (std.mem.eql(u8, arg, "--index") or std.mem.eql(u8, arg, "-i")) {
            if (route.index_name != null and route.duplicate_arg == null) route.duplicate_arg = arg;
            route.index_name = nextRouteValue(&args, arg, &route.missing_value_arg);
        } else if (std.mem.eql(u8, arg, "--type") or std.mem.eql(u8, arg, "--field") or
            std.mem.eql(u8, arg, "--template") or std.mem.eql(u8, arg, "--embedder") or
            std.mem.eql(u8, arg, "--generator") or std.mem.eql(u8, arg, "--summarizer") or
            std.mem.eql(u8, arg, "--chunker") or std.mem.eql(u8, arg, "--dimension") or
            std.mem.eql(u8, arg, "--coverage-policy"))
        {
            if (create_only_arg == null) create_only_arg = arg;
            _ = nextRouteValue(&args, arg, &route.missing_value_arg);
        } else if (std.mem.eql(u8, arg, "--output") or std.mem.eql(u8, arg, "-o")) {
            if (list_only_arg == null) list_only_arg = arg;
            _ = nextRouteValue(&args, arg, &route.missing_value_arg);
        } else if (std.mem.eql(u8, arg, "--timeout") or std.mem.eql(u8, arg, "--poll-interval")) {
            if (wait_only_arg == null) wait_only_arg = arg;
            _ = nextRouteValue(&args, arg, &route.missing_value_arg);
        } else if (std.mem.eql(u8, arg, "create") or std.mem.eql(u8, arg, "drop") or
            std.mem.eql(u8, arg, "list") or std.mem.eql(u8, arg, "get") or std.mem.eql(u8, arg, "wait"))
        {
            if (route.subcommand == null) {
                route.subcommand = arg;
            } else if (route.duplicate_arg == null) {
                route.duplicate_arg = arg;
            }
        } else if (std.mem.eql(u8, arg, "--verbose")) {
            if (list_only_arg == null) list_only_arg = arg;
        } else {
            if (route.unknown_arg == null) route.unknown_arg = arg;
        }
    }

    const action = route.subcommand orelse if (route.index_name != null) "get" else "list";
    if (std.mem.eql(u8, action, "create")) {
        route.unknown_arg = route.unknown_arg orelse list_only_arg orelse wait_only_arg;
    } else if (std.mem.eql(u8, action, "list")) {
        route.unknown_arg = route.unknown_arg orelse create_only_arg orelse wait_only_arg;
    } else if (std.mem.eql(u8, action, "wait")) {
        route.unknown_arg = route.unknown_arg orelse create_only_arg orelse list_only_arg;
    } else {
        route.unknown_arg = route.unknown_arg orelse create_only_arg orelse list_only_arg orelse wait_only_arg;
    }
    return route;
}

test "index route accepts flags before and after the action" {
    var documented_argv = [_][*:0]const u8{ "list", "--table", "wikipedia" };
    const documented = parseRoute(std.process.Args.Iterator.init(.{ .vector = documented_argv[0..] }));
    try std.testing.expectEqualStrings("list", documented.subcommand.?);
    try std.testing.expectEqualStrings("wikipedia", documented.table_name.?);

    var legacy_argv = [_][*:0]const u8{ "--table", "wikipedia", "list" };
    const legacy = parseRoute(std.process.Args.Iterator.init(.{ .vector = legacy_argv[0..] }));
    try std.testing.expectEqualStrings("list", legacy.subcommand.?);
    try std.testing.expectEqualStrings("wikipedia", legacy.table_name.?);

    var value_argv = [_][*:0]const u8{ "create", "--table", "docs", "--type", "list" };
    const value = parseRoute(std.process.Args.Iterator.init(.{ .vector = value_argv[0..] }));
    try std.testing.expectEqualStrings("create", value.subcommand.?);

    var flags_first_argv = [_][*:0]const u8{ "--table", "docs", "--type", "list", "create" };
    const flags_first = parseRoute(std.process.Args.Iterator.init(.{ .vector = flags_first_argv[0..] }));
    try std.testing.expectEqualStrings("create", flags_first.subcommand.?);

    var policy_argv = [_][*:0]const u8{ "create", "--table", "docs", "--coverage-policy", "partial" };
    const policy = parseRoute(std.process.Args.Iterator.init(.{ .vector = policy_argv[0..] }));
    try std.testing.expectEqualStrings("create", policy.subcommand.?);
    try std.testing.expect(policy.unknown_arg == null);

    var shorthand_json_argv = [_][*:0]const u8{ "--table", "docs", "--output", "json" };
    const shorthand_json = parseRoute(std.process.Args.Iterator.init(.{ .vector = shorthand_json_argv[0..] }));
    try std.testing.expect(shorthand_json.subcommand == null);
    try std.testing.expect(shorthand_json.index_name == null);
    try std.testing.expect(shorthand_json.unknown_arg == null);

    var shorthand_invalid_argv = [_][*:0]const u8{ "--table", "docs", "--dimension", "3" };
    const shorthand_invalid = parseRoute(std.process.Args.Iterator.init(.{ .vector = shorthand_invalid_argv[0..] }));
    try std.testing.expectEqualStrings("--dimension", shorthand_invalid.unknown_arg.?);

    var duplicate_table_argv = [_][*:0]const u8{ "list", "--table", "docs", "-t", "other" };
    const duplicate_table = parseRoute(std.process.Args.Iterator.init(.{ .vector = duplicate_table_argv[0..] }));
    try std.testing.expectEqualStrings("-t", duplicate_table.duplicate_arg.?);

    var missing_index_argv = [_][*:0]const u8{ "--table", "docs", "--index" };
    const missing_index = parseRoute(std.process.Args.Iterator.init(.{ .vector = missing_index_argv[0..] }));
    try std.testing.expectEqualStrings("--index", missing_index.missing_value_arg.?);

    var swallowed_dimension_argv = [_][*:0]const u8{ "create", "--table", "docs", "--coverage-policy", "--dimension", "3" };
    const swallowed_dimension = parseRoute(std.process.Args.Iterator.init(.{ .vector = swallowed_dimension_argv[0..] }));
    try std.testing.expectEqualStrings("--coverage-policy", swallowed_dimension.missing_value_arg.?);

    var typo_argv = [_][*:0]const u8{ "create", "--table", "docs", "--dimensoin", "512" };
    const typo = parseRoute(std.process.Args.Iterator.init(.{ .vector = typo_argv[0..] }));
    try std.testing.expectEqualStrings("--dimensoin", typo.unknown_arg.?);
}

const IndexCreateConfigInput = struct {
    index_type: []const u8,
    field: ?[]const u8 = null,
    template: ?[]const u8 = null,
    embedder_json: ?[]const u8 = null,
    summarizer_json: ?[]const u8 = null,
    chunker_json: ?[]const u8 = null,
    dimension: ?i64 = null,
    coverage_policy: ?[]const u8 = null,
};

fn isValidJsonObject(allocator: std.mem.Allocator, raw: []const u8) !bool {
    var parsed = std.json.parseFromSlice(std.json.Value, allocator, raw, .{}) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return false,
    };
    defer parsed.deinit();
    return parsed.value == .object;
}

fn buildIndexCreateConfig(
    allocator: std.mem.Allocator,
    input: IndexCreateConfigInput,
) !std.json.Parsed(antfly_client.types.CreateIndexRequest) {
    if (!std.mem.eql(u8, input.index_type, "full_text") and
        !std.mem.eql(u8, input.index_type, "embeddings") and
        !std.mem.eql(u8, input.index_type, "graph") and
        !std.mem.eql(u8, input.index_type, "algebraic"))
    {
        return error.InvalidIndexType;
    }
    if (input.coverage_policy != null and !std.mem.eql(u8, input.index_type, "embeddings")) {
        return error.CoveragePolicyRequiresEmbeddingsIndex;
    }
    if (input.embedder_json) |raw| {
        if (!try isValidJsonObject(allocator, raw)) return error.InvalidEmbedderJson;
    }
    if (input.summarizer_json) |raw| {
        if (!try isValidJsonObject(allocator, raw)) return error.InvalidSummarizerJson;
    }
    if (input.chunker_json) |raw| {
        if (!try isValidJsonObject(allocator, raw)) return error.InvalidChunkerJson;
    }
    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();
    const writer = &out.writer;

    try writer.writeAll("{");
    try writer.print("\"type\":{f}", .{std.json.fmt(input.index_type, .{})});
    if (input.field) |field| try writer.print(",\"field\":{f}", .{std.json.fmt(field, .{})});
    if (input.template) |template| try writer.print(",\"template\":{f}", .{std.json.fmt(template, .{})});
    if (input.embedder_json) |embedder| try writer.print(",\"embedder\":{s}", .{embedder});
    if (input.summarizer_json) |summarizer| try writer.print(",\"summarizer\":{s}", .{summarizer});
    if (input.chunker_json) |chunker| try writer.print(",\"chunker\":{s}", .{chunker});
    if (input.dimension) |dimension| try writer.print(",\"dimension\":{d}", .{dimension});
    if (input.coverage_policy) |policy| try writer.print(",\"coverage_policy\":{f}", .{std.json.fmt(policy, .{})});
    try writer.writeAll("}");

    return std.json.parseFromSlice(antfly_client.types.CreateIndexRequest, allocator, out.written(), .{
        .allocate = .alloc_always,
    });
}

fn createIndex(allocator: std.mem.Allocator, client: *antfly_client.AntflyClient, table_name: []const u8, args: *std.process.Args.Iterator) !void {
    var idx_name: ?[]const u8 = null;
    var idx_type: ?[]const u8 = null;
    var field: ?[]const u8 = null;
    var template: ?[]const u8 = null;
    var embedder_json: ?[]const u8 = null;
    var summarizer_json: ?[]const u8 = null;
    var chunker_json: ?[]const u8 = null;
    var dimension: ?i64 = null;
    var coverage_policy: ?[]const u8 = null;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "create")) continue;
        if (std.mem.eql(u8, arg, "--index") or std.mem.eql(u8, arg, "-i")) {
            if (idx_name != null) cli.fatal("--index may only be provided once", .{});
            idx_name = args.next() orelse cli.fatal("{s} requires a value", .{arg});
        } else if (std.mem.eql(u8, arg, "--type")) {
            if (idx_type != null) cli.fatal("--type may only be provided once", .{});
            idx_type = args.next() orelse cli.fatal("--type requires a value", .{});
        } else if (std.mem.eql(u8, arg, "--field")) {
            if (field != null) cli.fatal("--field may only be provided once", .{});
            field = args.next() orelse cli.fatal("--field requires a value", .{});
        } else if (std.mem.eql(u8, arg, "--template")) {
            if (template != null) cli.fatal("--template may only be provided once", .{});
            template = args.next() orelse cli.fatal("--template requires a value", .{});
        } else if (std.mem.eql(u8, arg, "--embedder")) {
            if (embedder_json != null) cli.fatal("--embedder may only be provided once", .{});
            embedder_json = args.next() orelse cli.fatal("--embedder requires a JSON value", .{});
        } else if (std.mem.eql(u8, arg, "--generator") or std.mem.eql(u8, arg, "--summarizer")) {
            if (summarizer_json != null) cli.fatal("use only one of --summarizer or its legacy --generator alias", .{});
            summarizer_json = args.next() orelse cli.fatal("{s} requires a JSON value", .{arg});
        } else if (std.mem.eql(u8, arg, "--chunker")) {
            if (chunker_json != null) cli.fatal("--chunker may only be provided once", .{});
            chunker_json = args.next() orelse cli.fatal("--chunker requires a JSON value", .{});
        } else if (std.mem.eql(u8, arg, "--dimension")) {
            if (dimension != null) cli.fatal("--dimension may only be provided once", .{});
            const raw = args.next() orelse cli.fatal("--dimension requires a value", .{});
            dimension = std.fmt.parseInt(i64, raw, 10) catch cli.fatal("invalid --dimension value: {s}", .{raw});
            if (dimension.? <= 0) cli.fatal("--dimension must be greater than zero", .{});
        } else if (std.mem.eql(u8, arg, "--coverage-policy")) {
            if (coverage_policy != null) cli.fatal("--coverage-policy may only be provided once", .{});
            const raw = args.next() orelse cli.fatal("--coverage-policy requires a value", .{});
            if (!std.mem.eql(u8, raw, "strict") and
                !std.mem.eql(u8, raw, "partial") and
                !std.mem.eql(u8, raw, "best_effort"))
            {
                cli.fatal("invalid --coverage-policy value: {s}; expected strict, partial, or best_effort", .{raw});
            }
            coverage_policy = raw;
        } else if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            _ = args.next() orelse cli.fatal("{s} requires a value", .{arg}); // already parsed
        } else {
            cli.fatal("unknown index create option: {s}", .{arg});
        }
    }

    const name = idx_name orelse cli.fatal("--index is required", .{});
    const index_type = idx_type orelse cli.fatal("--type is required", .{});
    if (coverage_policy != null and !std.mem.eql(u8, index_type, "embeddings")) {
        cli.fatal("--coverage-policy is only valid for embeddings indexes", .{});
    }

    var parsed = buildIndexCreateConfig(allocator, .{
        .index_type = index_type,
        .field = field,
        .template = template,
        .embedder_json = embedder_json,
        .summarizer_json = summarizer_json,
        .chunker_json = chunker_json,
        .dimension = dimension,
        .coverage_policy = coverage_policy,
    }) catch |err| switch (err) {
        error.InvalidIndexType => cli.fatal("unsupported --type: {s}; expected full_text, embeddings, graph, or algebraic", .{index_type}),
        error.InvalidEmbedderJson => cli.fatal("--embedder must be a valid JSON object", .{}),
        error.InvalidSummarizerJson => cli.fatal("--summarizer must be a valid JSON object", .{}),
        error.InvalidChunkerJson => cli.fatal("--chunker must be a valid JSON object", .{}),
        else => cli.fatal("failed to build index config: {}", .{err}),
    };
    defer parsed.deinit();

    var response = try client.createIndex(table_name, name, parsed.value);
    defer response.deinit();
    std.debug.print("Create index command successful.\n", .{});
}

fn dropIndex(client: *antfly_client.AntflyClient, table_name: []const u8, pre_index: ?[]const u8, args: *std.process.Args.Iterator) !void {
    var idx_name = pre_index;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "drop")) continue;
        if (std.mem.eql(u8, arg, "--index") or std.mem.eql(u8, arg, "-i")) {
            const value = args.next() orelse cli.fatal("{s} requires a value", .{arg});
            if (idx_name != null) cli.fatal("--index may only be provided once", .{});
            idx_name = value;
        } else if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            _ = args.next() orelse cli.fatal("{s} requires a value", .{arg});
        } else {
            cli.fatal("unknown index drop option: {s}", .{arg});
        }
    }
    const name = idx_name orelse cli.fatal("--index is required", .{});
    try client.dropIndex(table_name, name);
    std.debug.print("Drop index command successful.\n", .{});
}

const ListOutput = enum { summary, json };

fn listIndexes(
    allocator: std.mem.Allocator,
    io: std.Io,
    client: *antfly_client.AntflyClient,
    table_name: []const u8,
    args: *std.process.Args.Iterator,
) !void {
    var output: ListOutput = .summary;
    var explicitly_selected = false;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "list")) continue;
        if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            _ = args.next() orelse cli.fatal("{s} requires a value", .{arg});
        } else if (std.mem.eql(u8, arg, "--verbose")) {
            if (explicitly_selected) cli.fatal("use only one of --verbose or --output json", .{});
            output = .json;
            explicitly_selected = true;
        } else if (std.mem.eql(u8, arg, "--output") or std.mem.eql(u8, arg, "-o")) {
            if (explicitly_selected) cli.fatal("use only one of --verbose or --output json", .{});
            const value = args.next() orelse cli.fatal("--output requires json", .{});
            if (!std.mem.eql(u8, value, "json")) cli.fatal("only JSON output is supported for index list", .{});
            output = .json;
            explicitly_selected = true;
        } else {
            cli.fatal("unknown index list option: {s}", .{arg});
        }
    }
    return listIndexesMode(allocator, io, client, table_name, output);
}

fn listIndexesMode(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, table_name: []const u8, output: ListOutput) !void {
    var resp = try client.listIndexes(table_name);
    defer resp.deinit();
    cli.expectHttpSuccess(resp);
    if (resp.data) |parsed| {
        if (output == .json) return cli.writeJson(allocator, io, parsed.value);
        cli.writeStdout(io, "NAME\tTYPE\tSTATE\tPROGRESS\tINDEXED\tVISIBLE\n");
        for (parsed.value) |index| try writeIndexSummary(allocator, io, index);
    }
}

fn getIndex(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, table_name: []const u8, pre_index: ?[]const u8, args: *std.process.Args.Iterator) !void {
    var idx_name = pre_index;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "get")) continue;
        if (std.mem.eql(u8, arg, "--index") or std.mem.eql(u8, arg, "-i")) {
            const value = args.next() orelse cli.fatal("{s} requires a value", .{arg});
            if (idx_name != null) cli.fatal("--index may only be provided once", .{});
            idx_name = value;
        } else if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            _ = args.next() orelse cli.fatal("{s} requires a value", .{arg});
        } else {
            cli.fatal("unknown index get option: {s}", .{arg});
        }
    }
    const name = idx_name orelse cli.fatal("--index is required", .{});
    return getIndexByName(allocator, io, client, table_name, name);
}

fn getIndexByName(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, table_name: []const u8, index_name: []const u8) !void {
    var resp = try client.getIndex(table_name, index_name);
    defer resp.deinit();
    cli.expectHttpSuccess(resp);
    if (resp.data) |parsed| {
        try cli.writeJson(allocator, io, parsed.value);
    }
}

const IndexSummary = struct {
    state: []const u8,
    progress: ?f64,
    indexed: ?i64,
    visible: ?i64,
    ready: bool,
    failed: bool,
};

fn summarizeIndex(index: antfly_client.types.IndexStatus) IndexSummary {
    return switch (index.status) {
        inline else => |stats| summarizeStats(stats),
    };
}

fn summarizeStats(stats: anytype) IndexSummary {
    const Stats = @TypeOf(stats);
    const error_text: ?[]const u8 = if (@hasField(Stats, "error")) stats.@"error" else null;
    const rebuilding: ?bool = if (@hasField(Stats, "rebuilding")) stats.rebuilding else null;
    const reported_state: ?[]const u8 = if (@hasField(Stats, "backfill_state")) stats.backfill_state else null;
    const config_mismatch = if (@hasField(Stats, "coverage")) blk: {
        const coverage = stats.coverage orelse break :blk false;
        break :blk coverage.config_mismatch_group_count > 0;
    } else false;
    const coverage_missing = if (@hasField(Stats, "coverage")) stats.coverage == null else false;
    const coverage_incomplete = if (@hasField(Stats, "coverage")) blk: {
        const coverage = stats.coverage orelse break :blk true;
        break :blk !index_readiness.coverageReady(coverage);
    } else false;
    const state = if (error_text != null)
        "failed"
    else if (config_mismatch)
        "config_mismatch"
    else if (coverage_incomplete and ((reported_state != null and std.mem.eql(u8, reported_state.?, "ready")) or
        (reported_state == null and rebuilding == false)))
        if (coverage_missing) "coverage_unavailable" else "coverage_incomplete"
    else if (reported_state) |value|
        value
    else if (rebuilding) |value|
        if (value) "running" else "ready"
    else
        "unknown";
    const progress: ?f64 = if (@hasField(Stats, "backfill_progress")) stats.backfill_progress else null;
    const indexed: ?i64 = if (@hasField(Stats, "total_indexed"))
        stats.total_indexed
    else if (@hasField(Stats, "doc_count"))
        stats.doc_count
    else
        null;
    const visible: ?i64 = if (@hasField(Stats, "query_visible_doc_count"))
        stats.query_visible_doc_count
    else if (@hasField(Stats, "doc_count"))
        stats.doc_count
    else
        indexed;
    const ready = !coverage_incomplete and (std.mem.eql(u8, state, "ready") or
        (reported_state == null and rebuilding == false and error_text == null and !config_mismatch));
    const failed = error_text != null or std.mem.eql(u8, state, "failed") or std.mem.eql(u8, state, "degraded");
    return .{
        .state = state,
        .progress = progress,
        .indexed = indexed,
        .visible = visible,
        .ready = ready,
        .failed = failed,
    };
}

const WaitProgressReporter = struct {
    last_state: ?[]const u8 = null,
    last_report_ns: ?u64 = null,

    fn shouldReport(self: *@This(), state: []const u8, now_ns: u64) bool {
        const stable_state = canonicalWaitState(state);
        const state_changed = self.last_state == null or !std.mem.eql(u8, self.last_state.?, stable_state);
        const interval_elapsed = if (self.last_report_ns) |last| now_ns -| last >= wait_progress_report_interval_ns else true;
        if (!state_changed and !interval_elapsed) return false;
        self.last_state = stable_state;
        self.last_report_ns = now_ns;
        return true;
    }
};

fn printWaitProgress(index_name: []const u8, summary: IndexSummary) void {
    std.debug.print("Waiting for index {s}: {s}", .{ index_name, summary.state });
    if (summary.progress) |progress| {
        std.debug.print(" {d:.1}%", .{@max(0.0, @min(1.0, progress)) * 100.0});
    }
    if (summary.indexed) |indexed| std.debug.print(" indexed={d}", .{indexed});
    if (summary.visible) |visible| std.debug.print(" visible={d}", .{visible});
    std.debug.print("\n", .{});
}

const WaitDisposition = enum { ready, waiting, failed };

fn waitDisposition(summary: IndexSummary) WaitDisposition {
    if (summary.ready) return .ready;
    if (summary.failed) return .failed;
    return .waiting;
}

fn writeIndexSummary(allocator: std.mem.Allocator, io: std.Io, index: antfly_client.types.IndexStatus) !void {
    const summary = summarizeIndex(index);
    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();
    const writer = &out.writer;
    try writer.print("{s}\t{s}\t{s}\t", .{ index.config.name, @tagName(index.config.type), summary.state });
    if (summary.progress) |progress| {
        try writer.print("{d:.1}%", .{@max(0.0, @min(1.0, progress)) * 100.0});
    } else {
        try writer.writeAll("-");
    }
    try writer.writeAll("\t");
    if (summary.indexed) |indexed| try writer.print("{d}", .{indexed}) else try writer.writeAll("-");
    try writer.writeAll("\t");
    if (summary.visible) |visible| try writer.print("{d}", .{visible}) else try writer.writeAll("-");
    try writer.writeAll("\n");
    cli.writeStdout(io, out.written());
}

fn waitForIndex(
    allocator: std.mem.Allocator,
    io: std.Io,
    client: *antfly_client.AntflyClient,
    table_name: []const u8,
    pre_index: ?[]const u8,
    args: *std.process.Args.Iterator,
) !void {
    var index_name = pre_index;
    var timeout_ms = default_wait_timeout_ms;
    var poll_ms = default_wait_poll_ms;
    var timeout_set = false;
    var poll_set = false;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "wait")) continue;
        if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            _ = args.next() orelse cli.fatal("{s} requires a value", .{arg});
        } else if (std.mem.eql(u8, arg, "--index") or std.mem.eql(u8, arg, "-i")) {
            const value = args.next() orelse cli.fatal("{s} requires a value", .{arg});
            if (index_name != null) cli.fatal("--index may only be provided once", .{});
            index_name = value;
        } else if (std.mem.eql(u8, arg, "--timeout")) {
            if (timeout_set) cli.fatal("--timeout may only be provided once", .{});
            const raw = args.next() orelse cli.fatal("--timeout requires a duration", .{});
            timeout_ms = parseDurationMs(raw) catch cli.fatal("invalid --timeout duration: {s}", .{raw});
            timeout_set = true;
        } else if (std.mem.eql(u8, arg, "--poll-interval")) {
            if (poll_set) cli.fatal("--poll-interval may only be provided once", .{});
            const raw = args.next() orelse cli.fatal("--poll-interval requires a duration", .{});
            poll_ms = parseDurationMs(raw) catch cli.fatal("invalid --poll-interval duration: {s}", .{raw});
            poll_set = true;
        } else {
            cli.fatal("unknown index wait option: {s}", .{arg});
        }
    }

    const name = index_name orelse cli.fatal("--index is required", .{});
    const outcome = try waitForIndexWithFetcher(
        allocator,
        io,
        .fromClient(client),
        .system(),
        table_name,
        name,
        timeout_ms,
        poll_ms,
    );
    switch (outcome) {
        .ready => {},
        .timed_out => |last_state| fatalWaitTimeout(timeout_ms, name, last_state),
    }
}

const IndexStatusResponse = antfly_client.openapi.ApiResponse(antfly_client.types.IndexStatus);

const IndexStatusFetcher = struct {
    ptr: *anyopaque,
    fetch_fn: *const fn (ptr: *anyopaque, table_name: []const u8, index_name: []const u8, timeout_ms: u64) anyerror!IndexStatusResponse,

    fn fromClient(client: *antfly_client.AntflyClient) IndexStatusFetcher {
        return .{ .ptr = client, .fetch_fn = fetchFromClient };
    }

    fn fetch(self: IndexStatusFetcher, table_name: []const u8, index_name: []const u8, timeout_ms: u64) !IndexStatusResponse {
        return self.fetch_fn(self.ptr, table_name, index_name, timeout_ms);
    }

    fn fetchFromClient(ptr: *anyopaque, table_name: []const u8, index_name: []const u8, timeout_ms: u64) anyerror!IndexStatusResponse {
        const client: *antfly_client.AntflyClient = @ptrCast(@alignCast(ptr));
        return client.getIndexResponseWithTimeout(table_name, index_name, timeout_ms);
    }
};

const WaitClock = struct {
    ptr: ?*anyopaque = null,
    now_fn: *const fn (ptr: ?*anyopaque) u64 = systemNow,

    fn system() WaitClock {
        return .{};
    }

    fn now(self: WaitClock) u64 {
        return self.now_fn(self.ptr);
    }

    fn systemNow(_: ?*anyopaque) u64 {
        return platform_time.monotonicNs();
    }
};

const WaitOutcome = union(enum) {
    ready: void,
    timed_out: []const u8,
};

fn timedOut(last_state: ?[]const u8) WaitOutcome {
    return .{ .timed_out = last_state orelse "unknown" };
}

fn waitForIndexWithFetcher(
    allocator: std.mem.Allocator,
    io: std.Io,
    fetcher: IndexStatusFetcher,
    clock: WaitClock,
    table_name: []const u8,
    name: []const u8,
    timeout_ms: u64,
    poll_ms: u64,
) !WaitOutcome {
    const started_ns = clock.now();
    const timeout_ns = std.math.mul(u64, timeout_ms, std.time.ns_per_ms) catch std.math.maxInt(u64);
    var progress_reporter = WaitProgressReporter{};
    var consecutive_failures: u32 = 0;
    while (true) {
        const request_timeout_ms = requestWaitTimeoutMs(started_ns, timeout_ns, clock.now()) orelse
            return timedOut(progress_reporter.last_state);
        var resp = fetcher.fetch(table_name, name, request_timeout_ms) catch |err| {
            const now_ns = clock.now();
            if (remainingWaitNs(started_ns, timeout_ns, now_ns) == null) {
                return timedOut(progress_reporter.last_state);
            }
            if (!retryableWaitTransportError(err)) return err;
            consecutive_failures +|= 1;
            if (progress_reporter.shouldReport("unavailable", now_ns)) {
                std.debug.print("Waiting for index {s}: unavailable ({s}); retrying\n", .{ name, @errorName(err) });
            }
            sleepForNextWaitAttempt(io, clock, started_ns, timeout_ns, retryDelayMs(poll_ms, consecutive_failures, now_ns), name);
            continue;
        };
        const response_ns = clock.now();
        if (remainingWaitNs(started_ns, timeout_ns, response_ns) == null) {
            resp.deinit();
            return timedOut(progress_reporter.last_state);
        }
        if (resp.status_code >= 300) {
            if (!retryableWaitHttpStatus(resp.status_code)) {
                cli.expectHttpSuccess(resp);
                cli.fatal("index {s} returned HTTP {d}", .{ name, resp.status_code });
            }
            consecutive_failures +|= 1;
            if (progress_reporter.shouldReport("unavailable", response_ns)) {
                std.debug.print("Waiting for index {s}: unavailable (HTTP {d}); retrying\n", .{ name, resp.status_code });
            }
            resp.deinit();
            sleepForNextWaitAttempt(io, clock, started_ns, timeout_ns, retryDelayMs(poll_ms, consecutive_failures, response_ns), name);
            continue;
        }

        consecutive_failures = 0;
        if (resp.data) |parsed| {
            const summary = summarizeIndex(parsed.value);
            switch (waitDisposition(summary)) {
                .ready => {
                    try writeIndexSummary(allocator, io, parsed.value);
                    resp.deinit();
                    return .{ .ready = {} };
                },
                .failed => cli.fatal("index {s} entered terminal state {s}; run index list --output json for diagnostics", .{ name, summary.state }),
                .waiting => {},
            }
            if (progress_reporter.shouldReport(summary.state, response_ns)) {
                printWaitProgress(name, summary);
            }
        } else {
            cli.fatal("index {s} returned an unreadable HTTP {d} response", .{ name, resp.status_code });
        }
        resp.deinit();
        sleepForNextWaitAttempt(io, clock, started_ns, timeout_ns, poll_ms, name);
    }
}

fn retryableWaitHttpStatus(status: u16) bool {
    return status == 404 or status == 408 or status == 425 or status == 429 or
        status == 500 or status == 502 or status == 503 or status == 504;
}

fn retryableWaitTransportError(err: anyerror) bool {
    const name = @errorName(err);
    // Fail closed: only errors known to represent transient transport or pool
    // availability are retried. New parser, TLS, configuration, and resource
    // errors must surface immediately instead of silently consuming the full
    // user-visible wait budget.
    const transient = [_][]const u8{
        "Timeout",
        "ConnectionFailed",
        "ConnectionReset",
        "ConnectionResetByPeer",
        "ConnectionTimeout",
        "ConnectionTimedOut",
        "ConnectionRefused",
        "ConnectionAborted",
        "ConnectionClosed",
        "BrokenPipe",
        "HostUnreachable",
        "NetworkUnreachable",
        "NetworkDown",
        "NetworkSubsystemFailed",
        "DnsResolutionFailed",
        "TemporaryNameServerFailure",
        "PoolExhausted",
        "PoolExhaustedForHost",
        "EndOfStream",
        "UnexpectedEndOfStream",
        "ReadFailed",
        "WriteFailed",
        "StreamError",
        "FlowControlError",
        "FrameError",
        "Http2Error",
        "Http3Error",
        "QuicError",
    };
    for (transient) |value| if (std.mem.eql(u8, name, value)) return true;
    return false;
}

fn retryDelayMs(base_ms: u64, consecutive_failures: u32, entropy: u64) u64 {
    const exponent: u6 = @intCast(@min(consecutive_failures -| 1, 3));
    const scaled = base_ms *| (@as(u64, 1) << exponent);
    const bounded = @min(scaled, @max(base_ms, max_wait_retry_delay_ms));
    const jitter_span = bounded / 5;
    if (jitter_span == 0) return bounded;
    return bounded - jitter_span / 2 + entropy % (jitter_span + 1);
}

fn remainingWaitNs(started_ns: u64, timeout_ns: u64, now_ns: u64) ?u64 {
    const elapsed_ns = now_ns -| started_ns;
    if (elapsed_ns >= timeout_ns) return null;
    return timeout_ns - elapsed_ns;
}

fn requestWaitTimeoutMs(started_ns: u64, timeout_ns: u64, now_ns: u64) ?u64 {
    const remaining_ns = remainingWaitNs(started_ns, timeout_ns, now_ns) orelse return null;
    return @min(@max(remaining_ns / std.time.ns_per_ms, 1), max_wait_request_timeout_ms);
}

fn sleepForNextWaitAttempt(
    io: std.Io,
    clock: WaitClock,
    started_ns: u64,
    timeout_ns: u64,
    requested_delay_ms: u64,
    index_name: []const u8,
) void {
    const remaining_ns = remainingWaitNs(started_ns, timeout_ns, clock.now()) orelse return;
    const delay_ns = @min(requested_delay_ms *| std.time.ns_per_ms, remaining_ns);
    io.sleep(std.Io.Duration.fromNanoseconds(@intCast(delay_ns)), .awake) catch {
        cli.fatal("interrupted while waiting for index {s}", .{index_name});
    };
}

fn fatalWaitTimeout(timeout_ms: u64, index_name: []const u8, last_state: []const u8) noreturn {
    cli.fatal("timed out after {d}ms waiting for index {s} (last state: {s}); run index list --output json for diagnostics", .{
        timeout_ms,
        index_name,
        last_state,
    });
}

fn canonicalWaitState(state: []const u8) []const u8 {
    const known = [_][]const u8{
        "ready",
        "running",
        "retrying",
        "degraded",
        "failed",
        "config_mismatch",
        "coverage_incomplete",
        "coverage_unavailable",
        "unavailable",
        "unknown",
    };
    for (known) |value| if (std.mem.eql(u8, state, value)) return value;
    return "other";
}

fn parseDurationMs(raw: []const u8) !u64 {
    if (raw.len == 0) return error.InvalidDuration;
    const suffix_len: usize = if (std.mem.endsWith(u8, raw, "ms")) 2 else 1;
    if (raw.len <= suffix_len) return error.InvalidDuration;
    const suffix = raw[raw.len - suffix_len ..];
    const multiplier: u64 = if (std.mem.eql(u8, suffix, "ms"))
        1
    else if (std.mem.eql(u8, suffix, "s"))
        1000
    else if (std.mem.eql(u8, suffix, "m"))
        60 * 1000
    else if (std.mem.eql(u8, suffix, "h"))
        60 * 60 * 1000
    else
        return error.InvalidDuration;
    const amount = try std.fmt.parseUnsigned(u64, raw[0 .. raw.len - suffix_len], 10);
    if (amount == 0) return error.InvalidDuration;
    return try std.math.mul(u64, amount, multiplier);
}

test "index wait parses bounded human durations" {
    try std.testing.expectEqual(@as(u64, 250), try parseDurationMs("250ms"));
    try std.testing.expectEqual(@as(u64, 30_000), try parseDurationMs("30s"));
    try std.testing.expectEqual(@as(u64, 600_000), try parseDurationMs("10m"));
    try std.testing.expectError(error.InvalidDuration, parseDurationMs("0s"));
    try std.testing.expectError(error.InvalidDuration, parseDurationMs("10"));
}

test "index create config preserves dimension escaping and summarizer" {
    var parsed = try buildIndexCreateConfig(std.testing.allocator, .{
        .index_type = "embeddings",
        .field = "body\"quoted",
        .template = "{{title}}\n{{body}}",
        .dimension = 512,
        .embedder_json = "{\"provider\":\"openai\",\"model\":\"embed\"}",
        .summarizer_json = "{\"provider\":\"openai\",\"model\":\"summary\"}",
        .coverage_policy = "partial",
    });
    defer parsed.deinit();

    const config = switch (parsed.value) {
        .create_embeddings_index_request => |value| value,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqual(@as(?i64, 512), config.dimension);
    try std.testing.expectEqualStrings("body\"quoted", config.field.?);
    try std.testing.expectEqualStrings("{{title}}\n{{body}}", config.template.?);
    try std.testing.expectEqualStrings("summary", config.summarizer.?.model.?);
    try std.testing.expectEqual(antfly_client.types.DerivedCoveragePolicy.partial, config.coverage_policy.?);
}

test "index create config rejects malformed nested JSON and unknown types" {
    try std.testing.expectError(error.CoveragePolicyRequiresEmbeddingsIndex, buildIndexCreateConfig(std.testing.allocator, .{
        .index_type = "full_text",
        .coverage_policy = "partial",
    }));

    try std.testing.expectError(error.InvalidEmbedderJson, buildIndexCreateConfig(std.testing.allocator, .{
        .index_type = "embeddings",
        .embedder_json = "{",
    }));
    try std.testing.expectError(error.InvalidSummarizerJson, buildIndexCreateConfig(std.testing.allocator, .{
        .index_type = "embeddings",
        .summarizer_json = "[]",
    }));
    try std.testing.expectError(error.InvalidChunkerJson, buildIndexCreateConfig(std.testing.allocator, .{
        .index_type = "embeddings",
        .chunker_json = "null",
    }));
    try std.testing.expectError(error.InvalidIndexType, buildIndexCreateConfig(std.testing.allocator, .{
        .index_type = "typo",
    }));
}

test "index wait requires complete compatible coverage" {
    var coverage = antfly_client.types.DerivedCoverageStatus{
        .policy = .strict,
        .observation_complete = true,
        .observation_incomplete_reasons = &.{},
        .config_fingerprint = "0123456789abcdef",
        .summary_ready = true,
        .config_mismatch_group_count = 0,
        .source_total = 10,
        .produced = 10,
        .skipped = 0,
        .terminal_failed = 0,
        .covered = 10,
        .settled = 10,
        .uncovered = 0,
        .pending = 0,
        .complete = true,
        .healthy = true,
        .degraded = false,
    };
    var summary = summarizeStats(antfly_client.types.EmbeddingsIndexStats{
        .index_type = .embeddings,
        .rebuilding = false,
        .backfill_state = "ready",
    });
    try std.testing.expectEqualStrings("coverage_unavailable", summary.state);
    try std.testing.expect(!summary.ready);
    try std.testing.expect(!summary.failed);

    summary = summarizeStats(antfly_client.types.EmbeddingsIndexStats{
        .index_type = .embeddings,
        .rebuilding = false,
    });
    try std.testing.expectEqualStrings("coverage_unavailable", summary.state);
    try std.testing.expect(!summary.ready);

    summary = summarizeStats(antfly_client.types.EmbeddingsIndexStats{
        .index_type = .embeddings,
        .rebuilding = false,
        .backfill_state = "ready",
        .coverage = coverage,
    });
    try std.testing.expect(summary.ready);
    try std.testing.expect(!summary.failed);

    coverage.observation_complete = false;
    coverage.complete = false;
    summary = summarizeStats(antfly_client.types.EmbeddingsIndexStats{
        .index_type = .embeddings,
        .rebuilding = false,
        .backfill_state = "ready",
        .coverage = coverage,
    });
    try std.testing.expectEqualStrings("coverage_incomplete", summary.state);
    try std.testing.expect(!summary.ready);
    try std.testing.expect(!summary.failed);

    coverage.config_mismatch_group_count = 1;
    summary = summarizeStats(antfly_client.types.EmbeddingsIndexStats{
        .index_type = .embeddings,
        .rebuilding = true,
        .backfill_state = "running",
        .coverage = coverage,
    });
    try std.testing.expectEqualStrings("config_mismatch", summary.state);
    try std.testing.expect(!summary.failed);

    coverage.policy = .external;
    coverage.observation_complete = true;
    coverage.config_mismatch_group_count = 0;
    coverage.complete = false;
    coverage.healthy = false;
    summary = summarizeStats(antfly_client.types.EmbeddingsIndexStats{
        .index_type = .embeddings,
        .rebuilding = false,
        .backfill_state = "ready",
        .coverage = coverage,
    });
    try std.testing.expectEqualStrings("ready", summary.state);
    try std.testing.expect(summary.ready);
}

test "index wait progress reporting is immediate periodic and state sensitive" {
    var reporter = WaitProgressReporter{};
    try std.testing.expect(reporter.shouldReport("running", 0));
    try std.testing.expect(!reporter.shouldReport("running", wait_progress_report_interval_ns - 1));
    try std.testing.expect(reporter.shouldReport("retrying", wait_progress_report_interval_ns - 1));
    try std.testing.expect(!reporter.shouldReport("retrying", wait_progress_report_interval_ns));
    try std.testing.expect(reporter.shouldReport("retrying", 2 * wait_progress_report_interval_ns));
}

test "index wait disposition retries mismatch and fails only terminal states" {
    try std.testing.expectEqual(WaitDisposition.waiting, waitDisposition(.{
        .state = "config_mismatch",
        .progress = 0,
        .indexed = 0,
        .visible = 0,
        .ready = false,
        .failed = false,
    }));
    try std.testing.expectEqual(WaitDisposition.failed, waitDisposition(.{
        .state = "degraded",
        .progress = 1,
        .indexed = 10,
        .visible = 10,
        .ready = false,
        .failed = true,
    }));
    try std.testing.expectEqual(WaitDisposition.ready, waitDisposition(.{
        .state = "ready",
        .progress = 1,
        .indexed = 10,
        .visible = 10,
        .ready = true,
        .failed = false,
    }));
}

fn fakeExternalIndexStatusResponse(
    allocator: std.mem.Allocator,
    status_code: u16,
    state: []const u8,
    rebuilding: bool,
) !IndexStatusResponse {
    const body = try std.fmt.allocPrint(allocator,
        \\{{"shard_status":{{}},"config":{{"name":"external_idx","type":"embeddings","external":true,"dimension":3}},"status":{{"index_type":"embeddings","rebuilding":{s},"backfill_state":"{s}","total_indexed":1,"query_visible_doc_count":1,"coverage":{{"policy":"external","observation_complete":true,"observation_incomplete_reasons":[],"config_fingerprint":"0123456789abcdef","summary_ready":true,"config_mismatch_group_count":0,"source_total":10,"produced":1,"skipped":0,"terminal_failed":0,"covered":1,"settled":1,"uncovered":9,"pending":9,"complete":false,"healthy":false,"degraded":false}}}}}}
    , .{ if (rebuilding) "true" else "false", state });
    defer allocator.free(body);
    const parsed = try std.json.parseFromSlice(antfly_client.types.IndexStatus, allocator, body, .{ .allocate = .alloc_always });
    return .{ .status_code = status_code, .data = parsed, .allocator = allocator };
}

test "index wait retries bounded HTTP and transport failures" {
    try std.testing.expect(retryableWaitHttpStatus(404));
    try std.testing.expect(retryableWaitHttpStatus(429));
    try std.testing.expect(retryableWaitHttpStatus(502));
    try std.testing.expect(!retryableWaitHttpStatus(400));
    try std.testing.expect(!retryableWaitHttpStatus(401));
    try std.testing.expect(retryableWaitTransportError(error.ConnectionResetByPeer));
    try std.testing.expect(retryableWaitTransportError(error.Timeout));
    try std.testing.expect(!retryableWaitTransportError(error.InvalidUri));
    try std.testing.expect(!retryableWaitTransportError(error.CertificateVerificationFailed));
    try std.testing.expect(!retryableWaitTransportError(error.InvalidHeader));
    try std.testing.expect(!retryableWaitTransportError(error.InvalidChunkSize));
    try std.testing.expect(!retryableWaitTransportError(error.CompressionError));
    try std.testing.expect(!retryableWaitTransportError(error.TlsHandshakeFailed));
    try std.testing.expect(retryDelayMs(1000, 4, 0) <= max_wait_retry_delay_ms);

    const Fake = struct {
        allocator: std.mem.Allocator,
        calls: usize = 0,
        smallest_timeout_ms: u64 = std.math.maxInt(u64),

        fn fetch(ptr: *anyopaque, table_name: []const u8, index_name: []const u8, timeout_ms: u64) anyerror!IndexStatusResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqualStrings("external_idx", index_name);
            try std.testing.expect(timeout_ms > 0);
            try std.testing.expect(timeout_ms <= 1000);
            self.smallest_timeout_ms = @min(self.smallest_timeout_ms, timeout_ms);
            const call = self.calls;
            self.calls += 1;
            if (call == 0) return error.ConnectionResetByPeer;
            const state = if (call == 2) "running" else "ready";
            // A retryable HTTP status is authoritative even if an intermediary
            // supplies a stale but otherwise parseable success-shaped body.
            return fakeExternalIndexStatusResponse(self.allocator, if (call == 1) 503 else 200, state, call == 2);
        }
    };

    var fake = Fake{ .allocator = std.testing.allocator };
    const outcome = try waitForIndexWithFetcher(
        std.testing.allocator,
        std.testing.io,
        .{ .ptr = &fake, .fetch_fn = Fake.fetch },
        .system(),
        "docs",
        "external_idx",
        1000,
        1,
    );
    try std.testing.expect(outcome == .ready);
    try std.testing.expectEqual(@as(usize, 4), fake.calls);
    try std.testing.expect(fake.smallest_timeout_ms > 0);
}

test "index wait deadline rejects a response that arrives late" {
    const Fake = struct {
        allocator: std.mem.Allocator,
        now_ns: u64 = 10 * std.time.ns_per_s,
        calls: usize = 0,
        request_timeout_ms: ?u64 = null,

        fn now(ptr: ?*anyopaque) u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            return self.now_ns;
        }

        fn fetch(ptr: *anyopaque, table_name: []const u8, index_name: []const u8, timeout_ms: u64) anyerror!IndexStatusResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqualStrings("external_idx", index_name);
            self.calls += 1;
            self.request_timeout_ms = timeout_ms;
            self.now_ns += 2 * std.time.ns_per_ms;
            return fakeExternalIndexStatusResponse(self.allocator, 200, "ready", false);
        }
    };

    var fake = Fake{ .allocator = std.testing.allocator };
    const outcome = try waitForIndexWithFetcher(
        std.testing.allocator,
        std.testing.io,
        .{ .ptr = &fake, .fetch_fn = Fake.fetch },
        .{ .ptr = &fake, .now_fn = Fake.now },
        "docs",
        "external_idx",
        1,
        1,
    );
    try std.testing.expect(outcome == .timed_out);
    try std.testing.expectEqualStrings("unknown", outcome.timed_out);
    try std.testing.expectEqual(@as(usize, 1), fake.calls);
    try std.testing.expectEqual(@as(?u64, 1), fake.request_timeout_ms);
}

test "index wait does not fetch after its deadline" {
    try std.testing.expectEqual(@as(?u64, std.time.ns_per_ms), remainingWaitNs(100, std.time.ns_per_ms, 100));
    try std.testing.expectEqual(@as(?u64, 1), requestWaitTimeoutMs(100, std.time.ns_per_ms, 100));
    try std.testing.expectEqual(
        @as(?u64, max_wait_request_timeout_ms),
        requestWaitTimeoutMs(100, 20 * 60 * std.time.ns_per_s, 100),
    );
    try std.testing.expect(remainingWaitNs(100, std.time.ns_per_ms, 100 + std.time.ns_per_ms) == null);
    try std.testing.expect(requestWaitTimeoutMs(100, std.time.ns_per_ms, 100 + std.time.ns_per_ms) == null);

    const Fake = struct {
        allocator: std.mem.Allocator,
        base_ns: u64 = 20 * std.time.ns_per_s,
        clock_calls: usize = 0,
        fetch_calls: usize = 0,

        fn now(ptr: ?*anyopaque) u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            self.clock_calls += 1;
            return self.base_ns + if (self.clock_calls >= 4) @as(u64, std.time.ns_per_ms) else 0;
        }

        fn fetch(ptr: *anyopaque, _: []const u8, _: []const u8, timeout_ms: u64) anyerror!IndexStatusResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.fetch_calls += 1;
            try std.testing.expectEqual(@as(u64, 1), timeout_ms);
            return fakeExternalIndexStatusResponse(self.allocator, 200, "running", true);
        }
    };

    var fake = Fake{ .allocator = std.testing.allocator };
    const outcome = try waitForIndexWithFetcher(
        std.testing.allocator,
        std.testing.io,
        .{ .ptr = &fake, .fetch_fn = Fake.fetch },
        .{ .ptr = &fake, .now_fn = Fake.now },
        "docs",
        "external_idx",
        1,
        1,
    );
    try std.testing.expect(outcome == .timed_out);
    try std.testing.expectEqualStrings("running", outcome.timed_out);
    try std.testing.expectEqual(@as(usize, 1), fake.fetch_calls);
}
