// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Remote client commands compiled independently from the final CLI dispatcher.

const std = @import("std");
const cli = @import("cmd/cli/mod.zig");
const httpx = @import("httpx");

pub fn runFromIterator(
    init: std.process.Init,
    command: []const u8,
    args: *std.process.Args.Iterator,
) !void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var initial_args = args.*;
    var explicit_agent: ?bool = null;
    while (initial_args.next()) |flag| {
        const takes_value = @import("cli_invocation.zig").takesValue(flag) orelse break;
        if (std.mem.eql(u8, flag, "--agent") or std.mem.eql(u8, flag, "--no-agent")) {
            if (explicit_agent == null) explicit_agent = std.mem.eql(u8, flag, "--agent");
        }
        if (takes_value) _ = initial_args.next() orelse break;
    }
    cli.output.configure(io, .{ .enabled = cli.agent.activation(explicit_agent, @import("antfly_platform").env.getenv("ANTFLY_AGENT_MODE"), recognizedAgent(), std.Io.File.stdout().isTty(io) catch false) catch true });
    runConfigured(init, command, args, io) catch |err| {
        if (cli.output.reportFailure(err)) return error.CliDiagnosticReported;
        return err;
    };
}

const ProgressTicker = struct {
    fn run(io: std.Io, started: std.Io.Timestamp) std.Io.Cancelable!void {
        while (true) {
            try io.sleep(.fromSeconds(1), .awake);
            cli.output.progress(io, "command_running", started.durationTo(std.Io.Clock.awake.now(io)).toMilliseconds());
        }
    }
};

fn recognizedAgent() bool {
    const env = @import("antfly_platform").env;
    for ([_][*:0]const u8{ "CODEX_THREAD_ID", "CLAUDECODE", "CURSOR_AGENT", "CLINE_ACTIVE" }) |name| {
        if (env.getenv(name)) |value| if (value.len > 0 and !std.mem.eql(u8, value, "0") and !std.mem.eql(u8, value, "false")) return true;
    }
    return false;
}

const ExecutionOptions = struct {
    deadline_ms: ?u64 = null,
    scan_rows: ?u64 = null,
    memory_bytes: ?u64 = null,
    spill_bytes: ?u64 = null,
};

fn parseGlobalOptions(command: []const u8, args: *std.process.Args.Iterator, io: std.Io) !ExecutionOptions {
    var explicit_agent: ?bool = null;
    var deadline_ms: ?u64 = null;
    var scan_rows: ?u64 = null;
    var memory_bytes: ?u64 = null;
    var spill_bytes: ?u64 = null;
    const limits = @import("antfly_public_limits").Execution;
    var output_seen = false;
    var display_seen = [_]bool{ false, false, false };
    // Global options form a prefix after the command (or are forwarded here
    // from before the command). Local values are never scanned as global flags.
    while (true) {
        var probe = args.*;
        const flag = probe.next() orelse break;
        if (std.mem.eql(u8, flag, "--agent") or std.mem.eql(u8, flag, "--no-agent")) {
            if (explicit_agent != null) return error.DuplicateAgentOption;
            explicit_agent = std.mem.eql(u8, flag, "--agent");
            cli.output.policy.enabled = explicit_agent.?;
        } else if (std.mem.eql(u8, flag, "--deadline-ms") or std.mem.eql(u8, flag, "--max-scan-rows") or
            std.mem.eql(u8, flag, "--memory-bytes") or std.mem.eql(u8, flag, "--spill-bytes"))
        {
            const slot = if (std.mem.eql(u8, flag, "--deadline-ms")) &deadline_ms else if (std.mem.eql(u8, flag, "--max-scan-rows")) &scan_rows else if (std.mem.eql(u8, flag, "--memory-bytes")) &memory_bytes else &spill_bytes;
            if (slot.* != null) return error.DuplicateExecutionBudget;
            const raw = probe.next() orelse return error.MissingGlobalOptionValue;
            slot.* = try limits.positive(raw, if (slot == &deadline_ms) limits.max_timeout_ms else std.math.maxInt(usize));
        } else if (std.mem.eql(u8, flag, "--progress")) {
            if (cli.output.progress_enabled) return error.DuplicateGlobalOption;
            cli.output.progress_enabled = true;
        } else if (std.mem.eql(u8, flag, "--fingerprint")) {
            if (cli.output.policy.fingerprint != null) return error.DuplicateGlobalOption;
            const value = probe.next() orelse return error.MissingGlobalOptionValue;
            cli.output.policy.fingerprint = std.meta.stringToEnum(cli.agent.fingerprint.Ordering, value) orelse return error.InvalidFingerprintOrdering;
        } else if (std.mem.eql(u8, flag, "--output")) {
            if (output_seen) return error.DuplicateGlobalOption;
            output_seen = true;
            const value = probe.next() orelse return error.MissingGlobalOptionValue;
            try cli.output.selectFormat(std.meta.stringToEnum(cli.agent.Format, value) orelse return error.InvalidOutputFormat);
        } else if (std.mem.eql(u8, flag, "--display-rows") or std.mem.eql(u8, flag, "--display-bytes") or std.mem.eql(u8, flag, "--display-cell-bytes")) {
            const index: usize = if (std.mem.eql(u8, flag, "--display-rows")) 0 else if (std.mem.eql(u8, flag, "--display-bytes")) 1 else 2;
            if (display_seen[index]) return error.DuplicateGlobalOption;
            display_seen[index] = true;
            const raw = probe.next() orelse return error.MissingGlobalOptionValue;
            const value = std.fmt.parseInt(usize, raw, 10) catch return error.InvalidDisplayBudget;
            if (value == 0) return error.InvalidDisplayBudget;
            if (std.mem.eql(u8, flag, "--display-rows")) cli.output.policy.limits.rows = value else if (std.mem.eql(u8, flag, "--display-cell-bytes")) cli.output.policy.limits.cell_bytes = value else {
                if (value < 1024) return error.InvalidDisplayBudget;
                cli.output.policy.limits.bytes = value;
            }
        } else break;
        args.* = probe;
    }
    cli.output.policy.enabled = try cli.agent.activation(explicit_agent, @import("antfly_platform").env.getenv("ANTFLY_AGENT_MODE"), recognizedAgent(), std.Io.File.stdout().isTty(io) catch false);
    try cli.output.validatePolicy();
    if ((scan_rows != null or memory_bytes != null or spill_bytes != null) and !std.mem.eql(u8, command, "sql")) return error.WorkBudgetsRequireSql;
    cli.output.execution_bounded = deadline_ms != null or scan_rows != null or memory_bytes != null or spill_bytes != null;

    const descriptor = @import("completion.zig").findCommand(command) orelse return error.UnknownCommand;
    if (descriptor.route != .cli) return error.AgentOptionsRequireClientCommand;
    return .{ .deadline_ms = deadline_ms, .scan_rows = scan_rows, .memory_bytes = memory_bytes, .spill_bytes = spill_bytes };
}

fn runConfigured(init: std.process.Init, command: []const u8, args: *std.process.Args.Iterator, io: std.Io) !void {
    const options = parseGlobalOptions(command, args, io) catch |err| return cli.output.argumentFailure(err);
    const deadline_ms = options.deadline_ms;
    const scan_rows = options.scan_rows;
    const memory_bytes = options.memory_bytes;
    const spill_bytes = options.spill_bytes;
    const limits = @import("antfly_public_limits").Execution;
    const started = std.Io.Clock.awake.now(io);
    cli.output.progress(io, "command_started", null);
    var progress_group: std.Io.Group = .init;
    if (cli.isAgent() and cli.output.progress_enabled) {
        try progress_group.concurrent(io, ProgressTicker.run, .{ io, started });
    }
    defer {
        progress_group.cancel(io);
        cli.output.progress(io, "command_finished", started.durationTo(std.Io.Clock.awake.now(io)).toMilliseconds());
    }

    // Dispatch offline inspection before global configuration, credentials,
    // HTTP setup, or catalog-scope argument processing.
    if (std.mem.eql(u8, command, "backup")) {
        var probe = args.*;
        if (probe.next()) |subcommand| {
            if (std.mem.eql(u8, subcommand, "inspect")) {
                _ = args.next();
                return cli.backup.runInspect(init.gpa, io, args);
            }
        }
    }
    if (helpRequested(args)) {
        var probe = args.*;
        while (probe.next()) |arg| {
            if (std.mem.eql(u8, arg, "maintenance")) {
                const commands = @import("maintenance_commands.zig");
                if (std.mem.eql(u8, command, "index")) {
                    std.debug.print("{s}", .{commands.usage(.index)});
                    return;
                }
                if (std.mem.eql(u8, command, "artifact")) {
                    std.debug.print("{s}", .{commands.usage(.artifact)});
                    return;
                }
            }
        }
        cli.printCommandUsage(command);
        return;
    }

    if (std.mem.eql(u8, command, "capabilities")) return cli.discovery.capabilities(init.gpa, io, args);

    if (std.mem.eql(u8, command, "connections") and
        try cli.connections.runLocalProviderIfRequested(init.gpa, io, args)) return;

    var header_storage: [4][2][]const u8 = undefined;
    var value_storage: [4][24]u8 = undefined;
    var header_count: usize = 0;
    const names = [_][]const u8{ limits.timeout_header, limits.scan_rows_header, limits.memory_header, limits.spill_header };
    for ([_]?u64{ deadline_ms, scan_rows, memory_bytes, spill_bytes }, names, 0..) |value, name, i| if (value) |number| {
        header_storage[header_count] = .{ name, try std.fmt.bufPrint(&value_storage[i], "{d}", .{number}) };
        header_count += 1;
    };
    var http = httpx.Client.initWithConfig(init.gpa, io, .{
        .retry_policy = .{ .max_retries = 0 },
        .redirect_policy = .{ .follow_redirects = false },
        .default_headers = header_storage[0..header_count],
    });
    defer http.deinit();

    var config = cli.parseGlobalFlags();
    var client_arguments = extractUrlOption(init.gpa, args.*, &config) catch |err| return cli.output.argumentFailure(err);
    defer client_arguments.deinit(init.gpa);
    args.* = std.process.Args.Iterator.init(.{ .vector = client_arguments.items });
    var client = cli.initClient(init.gpa, &http, config) catch |err| return cli.output.argumentFailure(err);
    defer client.deinit();

    var scoped_arguments: std.ArrayList([*:0]const u8) = .empty;
    defer scoped_arguments.deinit(init.gpa);
    const supports_scope = blk: {
        for ([_][]const u8{ "table", "index", "query", "lookup", "load", "insert", "delete", "backup", "restore", "schema" }) |name| if (std.mem.eql(u8, command, name)) break :blk true;
        break :blk false;
    };
    if (supports_scope) {
        var scope = cli.CatalogFlags.defaultsFromEnv();
        while (args.next()) |arg| {
            if (std.mem.eql(u8, arg, "--database")) scope.database = args.next() orelse return error.InvalidArguments else if (std.mem.eql(u8, arg, "--namespace")) scope.namespace = args.next() orelse return error.InvalidArguments else try scoped_arguments.append(init.gpa, arg.ptr);
        }
        if (scope.database != null or scope.namespace != null) client.catalog_scope = .{ .database = scope.database orelse "default", .namespace = scope.namespace orelse "public" };
        args.* = std.process.Args.Iterator.init(.{ .vector = scoped_arguments.items });
    }

    if (std.mem.eql(u8, command, "schema")) return cli.discovery.schema(init.gpa, io, &client, args);
    if (std.mem.eql(u8, command, "table")) return cli.table.run(init.gpa, io, &client, args);
    if (std.mem.eql(u8, command, "database")) return cli.database_cmd.run(init.gpa, io, &client, args);
    if (std.mem.eql(u8, command, "namespace")) return cli.namespace_cmd.run(init.gpa, io, &client, args);
    if (std.mem.eql(u8, command, "tablespace")) return cli.tablespace.run(init.gpa, io, &client, args);
    if (std.mem.eql(u8, command, "index")) return cli.index.run(init.gpa, io, &client, args);
    if (std.mem.eql(u8, command, "artifact")) return cli.artifact.run(init.gpa, io, &client, args);
    if (std.mem.eql(u8, command, "query")) return cli.query.run(init.gpa, io, &client, args);
    if (std.mem.eql(u8, command, "sql")) return cli.sql.run(init.gpa, io, &client, args);
    if (std.mem.eql(u8, command, "lookup")) return cli.query.lookup(init.gpa, io, &client, args);
    if (std.mem.eql(u8, command, "load")) return cli.data.load(init.gpa, io, &client, args);
    if (std.mem.eql(u8, command, "insert")) return cli.data.insert(init.gpa, io, &client, args);
    if (std.mem.eql(u8, command, "delete")) return cli.data.delete(init.gpa, io, &client, args);
    if (std.mem.eql(u8, command, "agents")) return cli.agents.run(init.gpa, io, &client, args);
    if (std.mem.eql(u8, command, "backup")) return cli.backup.runBackup(init.gpa, io, &client, args);
    if (std.mem.eql(u8, command, "restore")) return cli.backup.runRestore(init.gpa, io, &client, args);
    if (std.mem.eql(u8, command, "connections")) return cli.connections.run(init.gpa, io, &client, args);
    if (std.mem.eql(u8, command, "auth")) return cli.auth.run(init.gpa, io, &client, args);
    if (std.mem.eql(u8, command, "internal")) return cli.internal.run(init.gpa, io, &client, args);
    return error.InvalidArguments;
}

fn helpRequested(args: *std.process.Args.Iterator) bool {
    return cli.helpRequestedIn(args.*);
}

test "client runtime recognizes help without consuming arguments" {
    var argv = [_][*:0]const u8{ "--table", "docs", "--help" };
    var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    try std.testing.expect(helpRequested(&args));
    try std.testing.expectEqualStrings("--table", args.next().?);
}

test "client runtime treats help as a flag value when it follows an option" {
    var argv = [_][*:0]const u8{ "wait", "--table", "help", "--index", "vectors" };
    var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    try std.testing.expect(!helpRequested(&args));
    var sub_argv = [_][*:0]const u8{"help"};
    var sub_args = std.process.Args.Iterator.init(.{ .vector = sub_argv[0..] });
    try std.testing.expect(helpRequested(&sub_args));
}

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};

/// Apply connection selection before command parsing so every remote command,
/// including nested maintenance commands, shares the same URL precedence.
fn extractUrlOption(alloc: std.mem.Allocator, input: std.process.Args.Iterator, config: *cli.GlobalConfig) !std.ArrayList([*:0]const u8) {
    var args = input;
    var result: std.ArrayList([*:0]const u8) = .empty;
    errdefer result.deinit(alloc);
    var seen = false;
    var positional = false;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--")) positional = true;
        if (!positional and (std.mem.eql(u8, arg, "--url") or std.mem.startsWith(u8, arg, "--url="))) {
            if (seen) return error.DuplicateOption;
            seen = true;
            const value = if (std.mem.eql(u8, arg, "--url")) args.next() orelse return error.MissingOptionValue else arg[6..];
            if (value.len == 0 or std.mem.startsWith(u8, value, "--")) return error.MissingOptionValue;
            const uri = std.Uri.parse(value) catch return error.InvalidServerUrl;
            if ((!std.ascii.eqlIgnoreCase(uri.scheme, "http") and !std.ascii.eqlIgnoreCase(uri.scheme, "https")) or uri.host == null) return error.InvalidServerUrl;
            config.url = value;
        } else try result.append(alloc, arg.ptr);
    }
    return result;
}

test "client URL option applies to nested index maintenance and preserves command options" {
    const a = std.testing.allocator;
    const argv = [_][*:0]const u8{ "maintenance", "rebuild", "--table", "docs", "--url", "http://localhost:8194", "--index", "text" };
    var config: cli.GlobalConfig = .{ .url = "http://environment:8080" };
    var result = try extractUrlOption(a, std.process.Args.Iterator.init(.{ .vector = &argv }), &config);
    defer result.deinit(a);
    try std.testing.expectEqualStrings("http://localhost:8194", config.url);
    try std.testing.expectEqual(@as(usize, 6), result.items.len);
    try std.testing.expectEqualStrings("rebuild", std.mem.span(result.items[1]));
    const positional = [_][*:0]const u8{ "--", "--url", "literal-key" };
    var literals = try extractUrlOption(a, std.process.Args.Iterator.init(.{ .vector = &positional }), &config);
    defer literals.deinit(a);
    try std.testing.expectEqual(@as(usize, 3), literals.items.len);
    try std.testing.expectEqualStrings("http://localhost:8194", config.url);
    const missing = [_][*:0]const u8{"--url"};
    try std.testing.expectError(error.MissingOptionValue, extractUrlOption(a, std.process.Args.Iterator.init(.{ .vector = &missing }), &config));
    const duplicate = [_][*:0]const u8{ "--url=http://localhost:8194", "--url=http://localhost:8195" };
    try std.testing.expectError(error.DuplicateOption, extractUrlOption(a, std.process.Args.Iterator.init(.{ .vector = &duplicate }), &config));
}
