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

const builtin = @import("builtin");
const std = @import("std");
const structlog = @import("structlog");
const cmd = @import("cmd/mod.zig");
const httpx = @import("httpx");
const antfly_client = @import("antfly-client");
const platform = @import("antfly_platform");
const build_options = @import("build_options");
const linked_runtime_options = @import("linked_runtime_options");
const runtime_bridge = @import("runtime_bridge.zig");

// usermgr/storage_imports.zig depends back on these through the executable
// root in the monolithic diagnostic control. They stay unreferenced in the
// linked dispatcher, so that build does not pull either storage graph in.
pub const storage_backend_erased = @import("storage/backend_erased.zig");
pub const lsm_backend = @import("storage/lsm_backend/mod.zig");

const antfly_cloud_binary = "antfly-cloud";

pub const std_options: std.Options = .{
    .logFn = structlog.logFn,
};

pub fn main(init: std.process.Init) void {
    mainImpl(init) catch |err| failMain(err);
}

fn failMain(err: anyerror) noreturn {
    const message = switch (err) {
        error.FileNotFound => "required file was not found; check the configured path",
        error.AddressInUse => "listen address is already in use",
        error.InvalidCharacter, error.InvalidArguments => "invalid command-line value; run with --help",
        else => "startup failed; see the preceding diagnostic for details",
    };
    std.debug.print("antfly: {s}\n", .{message});
    std.process.exit(1);
}

fn mainImpl(init: std.process.Init) !void {
    structlog.init(.{ .formatter = .json, .level = .info });

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer args.deinit();

    const argv0 = args.next() orelse "antfly";
    const subcommand = args.next() orelse {
        printUsage(argv0);
        return;
    };

    if (std.mem.eql(u8, subcommand, "--help") or std.mem.eql(u8, subcommand, "-h") or std.mem.eql(u8, subcommand, "help")) {
        printUsage(argv0);
        return;
    }
    if (std.mem.eql(u8, subcommand, "--version") or std.mem.eql(u8, subcommand, "version")) {
        printVersion();
        return;
    }

    // Server-side subcommands
    if (comptime linked_runtime_options.enabled) {
        if (std.mem.eql(u8, subcommand, "data")) return runLinkedRuntime(.data, subcommand, init, &args);
        if (std.mem.eql(u8, subcommand, "ha")) return runLinkedRuntime(.cli, subcommand, init, &args);
        if (std.mem.eql(u8, subcommand, "inference")) return runLinkedRuntime(.inference, subcommand, init, &args);
        if (std.mem.eql(u8, subcommand, "lite")) return runLinkedRuntime(.cli, subcommand, init, &args);
        if (std.mem.eql(u8, subcommand, "metadata")) return runLinkedRuntime(.metadata, subcommand, init, &args);
        if (std.mem.eql(u8, subcommand, "serverless")) return runLinkedRuntime(.cli, subcommand, init, &args);
        if (std.mem.eql(u8, subcommand, "standalone")) return runLinkedRuntime(.standalone, subcommand, init, &args);
    } else {
        if (std.mem.eql(u8, subcommand, "data")) return try cmd.data.runFromIterator(runtimeInit(init), argv0, &args);
        if (std.mem.eql(u8, subcommand, "inference")) return try cmd.inference.runFromIterator(runtimeInit(init), argv0, &args);
        if (std.mem.eql(u8, subcommand, "metadata")) return try cmd.metadata.runFromIterator(runtimeInit(init), argv0, &args);
        if (std.mem.eql(u8, subcommand, "standalone")) return try cmd.standalone.runFromIterator(runtimeInit(init), argv0, &args);
        if (std.mem.eql(u8, subcommand, "serverless")) return try cmd.serverless.runFromIterator(runtimeInit(init), argv0, &args);
        if (std.mem.eql(u8, subcommand, "lite")) return try cmd.lite.runFromIterator(runtimeInit(init), argv0, &args);
        if (std.mem.eql(u8, subcommand, "ha")) return try cmd.ha.runFromIterator(runtimeInit(init), argv0, &args);
    }

    if (std.mem.eql(u8, subcommand, "cloud")) {
        const code = try runAntflyCloud(init.gpa, init.io, &args);
        std.process.exit(code);
    }

    // CLI client subcommands — these talk to a remote Antfly server via HTTP
    const cli_commands = [_][]const u8{
        "table",    "index",  "artifact", "query",
        "lookup",   "load",   "insert",   "delete",
        "agents",   "backup", "restore",  "auth",
        "internal",
    };
    for (cli_commands) |cli_cmd| {
        if (std.mem.eql(u8, subcommand, cli_cmd)) {
            if (comptime linked_runtime_options.enabled)
                return runLinkedRuntime(.cli, subcommand, init, &args);
            if (cliHelpRequested(&args)) {
                cmd.cli.printCommandUsage(cli_cmd);
                return;
            }
            return runCliCommand(init.gpa, cli_cmd, &args) catch |err| switch (err) {
                error.ApiError => std.process.exit(1),
                else => return err,
            };
        }
    }

    std.debug.print("unknown subcommand: {s}\n", .{subcommand});
    printUsage(argv0);
    return error.InvalidArguments;
}

const LinkedRuntimeRole = enum { cli, data, inference, metadata, standalone };

extern fn antfly_runtime_cli(context: *const runtime_bridge.Context) callconv(.c) c_int;
extern fn antfly_runtime_data(context: *const runtime_bridge.Context) callconv(.c) c_int;
extern fn antfly_runtime_inference(context: *const runtime_bridge.Context) callconv(.c) c_int;
extern fn antfly_runtime_metadata(context: *const runtime_bridge.Context) callconv(.c) c_int;
extern fn antfly_runtime_standalone(context: *const runtime_bridge.Context) callconv(.c) c_int;

fn runLinkedRuntime(
    comptime role: LinkedRuntimeRole,
    command: []const u8,
    init: std.process.Init,
    args: *std.process.Args.Iterator,
) void {
    const context = runtime_bridge.Context{
        .init = @ptrCast(&init),
        .args = @ptrCast(args),
        .command_ptr = command.ptr,
        .command_len = command.len,
    };
    const code = switch (role) {
        .cli => antfly_runtime_cli(&context),
        .data => antfly_runtime_data(&context),
        .inference => antfly_runtime_inference(&context),
        .metadata => antfly_runtime_metadata(&context),
        .standalone => antfly_runtime_standalone(&context),
    };
    if (code != 0) std.process.exit(@intCast(code));
}

fn cliHelpRequested(args: *std.process.Args.Iterator) bool {
    var probe = args.*;
    var first = true;
    while (probe.next()) |arg| {
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) return true;
        if (first and std.mem.eql(u8, arg, "help")) return true;
        first = false;
    }
    return false;
}

fn runAntflyCloud(allocator: std.mem.Allocator, io: std.Io, args: *std.process.Args.Iterator) !u8 {
    var argv_list = std.ArrayListUnmanaged([]const u8).empty;
    defer argv_list.deinit(allocator);

    try argv_list.append(allocator, antfly_cloud_binary);
    while (args.next()) |arg| {
        try argv_list.append(allocator, arg);
    }

    return runAntflyCloudArgv(io, argv_list.items);
}

fn runAntflyCloudArgv(io: std.Io, argv: []const []const u8) !u8 {
    return runAntflyCloudArgvMaybeReport(io, argv, true);
}

fn runAntflyCloudArgvMaybeReport(io: std.Io, argv: []const []const u8, report_missing: bool) !u8 {
    var child = std.process.spawn(io, .{
        .argv = argv,
        .stdin = .inherit,
        .stdout = .inherit,
        .stderr = .inherit,
    }) catch |err| switch (err) {
        error.FileNotFound => {
            if (report_missing) printMissingAntflyCloud();
            return 127;
        },
        else => return err,
    };

    const term = try child.wait(io);
    return switch (term) {
        .exited => |code| code,
        else => 1,
    };
}

fn printMissingAntflyCloud() void {
    std.debug.print(
        \\{s} is not installed.
        \\
        \\The `antfly cloud` command delegates to the separate Antfly Cloud CLI.
        \\Install it with:
        \\
        \\  brew install antflydb/taps/antfly-cloud
        \\
        \\Then rerun this command.
        \\
    , .{antfly_cloud_binary});
}

fn runCliCommand(allocator: std.mem.Allocator, subcommand: []const u8, args: *std.process.Args.Iterator) !void {
    // Read global config from env vars (ANTFLY_URL, ANTFLY_TOKEN)
    const config = cmd.cli.parseGlobalFlags();

    // Initialize IO and HTTP client
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var http = httpx.Client.initWithConfig(allocator, io, .{});
    defer http.deinit();

    // Initialize Antfly client
    var client = try cmd.cli.initClient(allocator, &http, config);
    defer client.deinit();

    // Dispatch to the specific command
    if (std.mem.eql(u8, subcommand, "table")) return cmd.cli.table.run(allocator, io, &client, args);
    if (std.mem.eql(u8, subcommand, "index")) return cmd.cli.index.run(allocator, io, &client, args);
    if (std.mem.eql(u8, subcommand, "artifact")) return cmd.cli.artifact.run(allocator, io, &client, args);
    if (std.mem.eql(u8, subcommand, "query")) return cmd.cli.query.run(allocator, io, &client, args);
    if (std.mem.eql(u8, subcommand, "lookup")) return cmd.cli.query.lookup(allocator, io, &client, args);
    if (std.mem.eql(u8, subcommand, "load")) return cmd.cli.data.load(allocator, io, &client, args);
    if (std.mem.eql(u8, subcommand, "insert")) return cmd.cli.data.insert(allocator, io, &client, args);
    if (std.mem.eql(u8, subcommand, "delete")) return cmd.cli.data.delete(allocator, io, &client, args);
    if (std.mem.eql(u8, subcommand, "agents")) return cmd.cli.agents.run(allocator, io, &client, args);
    if (std.mem.eql(u8, subcommand, "backup")) return cmd.cli.backup.runBackup(allocator, io, &client, args);
    if (std.mem.eql(u8, subcommand, "restore")) return cmd.cli.backup.runRestore(allocator, io, &client, args);
    if (std.mem.eql(u8, subcommand, "auth")) return cmd.cli.auth.run(allocator, io, &client, args);
    if (std.mem.eql(u8, subcommand, "internal")) return cmd.cli.internal.run(allocator, io, &client, args);
}

fn printUsage(argv0: []const u8) void {
    std.debug.print(
        \\usage: {s} <subcommand> [options]
        \\
        \\server subcommands:
        \\  data
        \\  metadata
        \\  standalone
        \\  inference
        \\  serverless
        \\  lite           Embedded Antfly Lite databases (*.aflite)
        \\  ha             Local hot-standby HA administration
        \\
        \\client subcommands:
        \\  table          Manage tables (create, drop, list, get)
        \\  index          Manage indexes (create, drop, list, get)
        \\  artifact       Manage generated artifact enrichments and reprocessing
        \\  query          Query data from a table
        \\  lookup         Look up a document by key
        \\  load           Bulk load data from NDJSON file
        \\  insert         Insert a single document
        \\  delete         Delete a single document
        \\  agents         Run AI agents (retrieval, query-builder)
        \\  backup         Backup tables
        \\  restore        Restore tables from backup, including Lite *.aflite input
        \\  auth           Manage data-plane users, roles, permissions, row filters, and API keys
        \\  internal       Internal cluster management
        \\  cloud          Delegate to the separate Antfly Cloud CLI
        \\
    , .{argv0});
}

fn printVersion() void {
    std.debug.print("antfly {s} (zig runtime)\n", .{build_options.antfly_version});
}

fn runtimeInit(init: std.process.Init) std.process.Init {
    return .{
        .minimal = init.minimal,
        .arena = init.arena,
        .gpa = runtimeAllocator(init),
        .io = init.io,
        .environ_map = init.environ_map,
        .preopens = init.preopens,
    };
}

fn runtimeAllocator(init: std.process.Init) std.mem.Allocator {
    const fallback = if (!builtin.single_threaded) std.heap.smp_allocator else init.gpa;
    return platform.allocator.processAllocator(fallback);
}

test "main cmd compiles" {
    _ = main;
}

test "client help is recognized before command execution" {
    var argv = [_][*:0]const u8{ "--table", "docs", "--help" };
    var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    try std.testing.expect(cliHelpRequested(&args));
    try std.testing.expectEqualStrings("--table", args.next().?);
    try std.testing.expect(cmd.cli.commandUsage("query") != null);
    try std.testing.expect(cmd.cli.commandUsage("load") != null);

    var value_argv = [_][*:0]const u8{ "--key", "help" };
    var value_args = std.process.Args.Iterator.init(.{ .vector = value_argv[0..] });
    try std.testing.expect(!cliHelpRequested(&value_args));
}

test "cloud shim argv starts with antfly-cloud and preserves args" {
    const allocator = std.testing.allocator;

    var argv_list = std.ArrayListUnmanaged([]const u8).empty;
    defer argv_list.deinit(allocator);

    try argv_list.append(allocator, antfly_cloud_binary);
    try argv_list.append(allocator, "status");
    try argv_list.append(allocator, "--json");

    try std.testing.expectEqualStrings("antfly-cloud", argv_list.items[0]);
    try std.testing.expectEqualStrings("status", argv_list.items[1]);
    try std.testing.expectEqualStrings("--json", argv_list.items[2]);
}

test "cloud shim reports missing antfly-cloud as 127" {
    const code = try runAntflyCloudArgvMaybeReport(std.testing.io, &.{"definitely-missing-antfly-cloud-for-test"}, false);
    try std.testing.expectEqual(@as(u8, 127), code);
}

test "cloud shim propagates child exit code" {
    const code = try runAntflyCloudArgv(std.testing.io, &.{ "/bin/sh", "-c", "exit 23" });
    try std.testing.expectEqual(@as(u8, 23), code);
}
