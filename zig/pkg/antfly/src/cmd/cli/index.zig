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
const platform_time = antfly.platform_time;

const default_wait_timeout_ms: u64 = 10 * 60 * 1000;
const default_wait_poll_ms: u64 = 1000;
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

fn parseRoute(iterator: std.process.Args.Iterator) Route {
    var args = iterator;
    var route: Route = .{};
    var create_only_arg: ?[]const u8 = null;
    var list_only_arg: ?[]const u8 = null;
    var wait_only_arg: ?[]const u8 = null;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            if (route.table_name != null and route.duplicate_arg == null) route.duplicate_arg = arg;
            route.table_name = args.next() orelse {
                route.missing_value_arg = route.missing_value_arg orelse arg;
                continue;
            };
        } else if (std.mem.eql(u8, arg, "--index") or std.mem.eql(u8, arg, "-i")) {
            if (route.index_name != null and route.duplicate_arg == null) route.duplicate_arg = arg;
            route.index_name = args.next() orelse {
                route.missing_value_arg = route.missing_value_arg orelse arg;
                continue;
            };
        } else if (std.mem.eql(u8, arg, "--type") or std.mem.eql(u8, arg, "--field") or
            std.mem.eql(u8, arg, "--template") or std.mem.eql(u8, arg, "--embedder") or
            std.mem.eql(u8, arg, "--generator") or std.mem.eql(u8, arg, "--summarizer") or
            std.mem.eql(u8, arg, "--chunker") or std.mem.eql(u8, arg, "--dimension"))
        {
            if (create_only_arg == null) create_only_arg = arg;
            _ = args.next() orelse {
                route.missing_value_arg = route.missing_value_arg orelse arg;
                continue;
            };
        } else if (std.mem.eql(u8, arg, "--output") or std.mem.eql(u8, arg, "-o")) {
            if (list_only_arg == null) list_only_arg = arg;
            _ = args.next() orelse {
                route.missing_value_arg = route.missing_value_arg orelse arg;
                continue;
            };
        } else if (std.mem.eql(u8, arg, "--timeout") or std.mem.eql(u8, arg, "--poll-interval")) {
            if (wait_only_arg == null) wait_only_arg = arg;
            _ = args.next() orelse {
                route.missing_value_arg = route.missing_value_arg orelse arg;
                continue;
            };
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

    var typo_argv = [_][*:0]const u8{ "create", "--table", "docs", "--dimensoin", "512" };
    const typo = parseRoute(std.process.Args.Iterator.init(.{ .vector = typo_argv[0..] }));
    try std.testing.expectEqualStrings("--dimensoin", typo.unknown_arg.?);
}

const IndexCreateConfigInput = struct {
    name: []const u8,
    index_type: []const u8,
    field: ?[]const u8 = null,
    template: ?[]const u8 = null,
    embedder_json: ?[]const u8 = null,
    summarizer_json: ?[]const u8 = null,
    chunker_json: ?[]const u8 = null,
    dimension: ?i64 = null,
};

fn buildIndexCreateConfig(
    allocator: std.mem.Allocator,
    input: IndexCreateConfigInput,
) !std.json.Parsed(antfly_client.types.IndexConfig) {
    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();
    const writer = &out.writer;

    try writer.writeAll("{");
    try writer.print("\"name\":{f}", .{std.json.fmt(input.name, .{})});
    try writer.print(",\"type\":{f}", .{std.json.fmt(input.index_type, .{})});
    if (input.field) |field| try writer.print(",\"field\":{f}", .{std.json.fmt(field, .{})});
    if (input.template) |template| try writer.print(",\"template\":{f}", .{std.json.fmt(template, .{})});
    if (input.embedder_json) |embedder| try writer.print(",\"embedder\":{s}", .{embedder});
    if (input.summarizer_json) |summarizer| try writer.print(",\"summarizer\":{s}", .{summarizer});
    if (input.chunker_json) |chunker| try writer.print(",\"chunker\":{s}", .{chunker});
    if (input.dimension) |dimension| try writer.print(",\"dimension\":{d}", .{dimension});
    try writer.writeAll("}");

    return std.json.parseFromSlice(antfly_client.types.IndexConfig, allocator, out.written(), .{});
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
        } else if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            _ = args.next() orelse cli.fatal("{s} requires a value", .{arg}); // already parsed
        } else {
            cli.fatal("unknown index create option: {s}", .{arg});
        }
    }

    const name = idx_name orelse cli.fatal("--index is required", .{});
    const index_type = idx_type orelse cli.fatal("--type is required", .{});

    var parsed = buildIndexCreateConfig(allocator, .{
        .name = name,
        .index_type = index_type,
        .field = field,
        .template = template,
        .embedder_json = embedder_json,
        .summarizer_json = summarizer_json,
        .chunker_json = chunker_json,
        .dimension = dimension,
    }) catch |err| {
        cli.fatal("failed to build index config: {}", .{err});
    };
    defer parsed.deinit();

    var resp = try client.createIndex(table_name, name, parsed.value);
    defer resp.deinit();
    cli.expectHttpSuccess(resp);
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
    var resp = try client.dropIndex(table_name, name);
    defer resp.deinit();
    cli.expectHttpSuccess(resp);
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
        break :blk !coverage.observation_complete or !coverage.complete;
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
    const started_ns = platform_time.monotonicNs();
    const timeout_ns = std.math.mul(u64, timeout_ms, std.time.ns_per_ms) catch std.math.maxInt(u64);
    var progress_reporter = WaitProgressReporter{};
    while (true) {
        var resp = try client.getIndex(table_name, name);
        if (resp.data) |parsed| {
            const summary = summarizeIndex(parsed.value);
            switch (waitDisposition(summary)) {
                .ready => {
                    try writeIndexSummary(allocator, io, parsed.value);
                    resp.deinit();
                    return;
                },
                .failed => cli.fatal("index {s} entered terminal state {s}; run index list --output json for diagnostics", .{ name, summary.state }),
                .waiting => {},
            }
            if (progress_reporter.shouldReport(summary.state, platform_time.monotonicNs())) {
                printWaitProgress(name, summary);
            }
        } else if (resp.status_code != 404 and resp.status_code != 503) {
            cli.expectHttpSuccess(resp);
            cli.fatal("index {s} returned an unreadable HTTP {d} response", .{ name, resp.status_code });
        } else if (progress_reporter.shouldReport("unavailable", platform_time.monotonicNs())) {
            std.debug.print("Waiting for index {s}: unavailable (HTTP {d})\n", .{ name, resp.status_code });
        }
        resp.deinit();

        const elapsed_ns = platform_time.monotonicNs() -| started_ns;
        if (elapsed_ns >= timeout_ns) {
            cli.fatal("timed out after {d}ms waiting for index {s} (last state: {s}); run index list --output json for diagnostics", .{
                timeout_ms,
                name,
                progress_reporter.last_state orelse "unknown",
            });
        }
        const delay_ns = @min(poll_ms *| std.time.ns_per_ms, timeout_ns - elapsed_ns);
        io.sleep(std.Io.Duration.fromNanoseconds(@intCast(delay_ns)), .awake) catch {
            cli.fatal("interrupted while waiting for index {s}", .{name});
        };
    }
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
        .name = "title_body",
        .index_type = "embeddings",
        .field = "body\"quoted",
        .template = "{{title}}\n{{body}}",
        .dimension = 512,
        .embedder_json = "{\"provider\":\"openai\",\"model\":\"embed\"}",
        .summarizer_json = "{\"provider\":\"openai\",\"model\":\"summary\"}",
    });
    defer parsed.deinit();

    try std.testing.expectEqual(@as(?i64, 512), parsed.value.dimension);
    try std.testing.expectEqualStrings("body\"quoted", parsed.value.field.?);
    try std.testing.expectEqualStrings("{{title}}\n{{body}}", parsed.value.template.?);
    try std.testing.expectEqualStrings("summary", parsed.value.summarizer.?.model.?);
}

test "index create config rejects malformed nested JSON and unknown types" {
    try std.testing.expectError(error.MissingField, buildIndexCreateConfig(std.testing.allocator, .{
        .name = "broken",
        .index_type = "embeddings",
        .embedder_json = "{",
    }));
    try std.testing.expectError(error.UnexpectedToken, buildIndexCreateConfig(std.testing.allocator, .{
        .name = "broken",
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
