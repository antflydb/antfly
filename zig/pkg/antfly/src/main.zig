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
const antfly = @import("antfly-zig");
const structlog = @import("structlog");
const cmd = @import("cmd/mod.zig");
const httpx = @import("httpx");
const antfly_client = @import("antfly-client");
const platform = @import("antfly_platform");

const antfly_cloud_binary = "antfly-cloud";

pub const std_options: std.Options = .{
    .logFn = structlog.logFn,
};

pub fn main(init: std.process.Init) !void {
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
    if (std.mem.eql(u8, subcommand, "data")) return try cmd.data.runFromIterator(runtimeInit(init), argv0, &args);
    if (std.mem.eql(u8, subcommand, "metadata")) return try cmd.metadata.runFromIterator(runtimeInit(init), argv0, &args);
    if (std.mem.eql(u8, subcommand, "swarm")) return try cmd.swarm.runFromIterator(runtimeInit(init), argv0, &args);
    if (std.mem.eql(u8, subcommand, "inference")) return try cmd.inference.runFromIterator(runtimeInit(init), argv0, &args);
    if (std.mem.eql(u8, subcommand, "serverless")) return try cmd.serverless.runFromIterator(runtimeInit(init), argv0, &args);

    if (std.mem.eql(u8, subcommand, "cloud")) {
        const code = try runAntflyCloud(init.gpa, init.io, &args);
        std.process.exit(code);
    }

    // CLI client subcommands — these talk to a remote Antfly server via HTTP
    const cli_commands = [_][]const u8{
        "table",  "index",   "query",    "lookup",
        "load",   "insert",  "delete",   "agents",
        "backup", "restore", "internal",
    };
    for (cli_commands) |cli_cmd| {
        if (std.mem.eql(u8, subcommand, cli_cmd)) {
            return runCliCommand(init.gpa, cli_cmd, &args);
        }
    }

    std.debug.print("unknown subcommand: {s}\n", .{subcommand});
    printUsage(argv0);
    return error.InvalidArguments;
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
    var child = std.process.spawn(io, .{
        .argv = argv,
        .stdin = .inherit,
        .stdout = .inherit,
        .stderr = .inherit,
    }) catch |err| switch (err) {
        error.FileNotFound => {
            printMissingAntflyCloud();
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
    if (std.mem.eql(u8, subcommand, "query")) return cmd.cli.query.run(allocator, io, &client, args);
    if (std.mem.eql(u8, subcommand, "lookup")) return cmd.cli.query.lookup(allocator, io, &client, args);
    if (std.mem.eql(u8, subcommand, "load")) return cmd.cli.data.load(allocator, io, &client, args);
    if (std.mem.eql(u8, subcommand, "insert")) return cmd.cli.data.insert(allocator, io, &client, args);
    if (std.mem.eql(u8, subcommand, "delete")) return cmd.cli.data.delete(allocator, io, &client, args);
    if (std.mem.eql(u8, subcommand, "agents")) return cmd.cli.agents.run(allocator, io, &client, args);
    if (std.mem.eql(u8, subcommand, "backup")) return cmd.cli.backup.runBackup(allocator, io, &client, args);
    if (std.mem.eql(u8, subcommand, "restore")) return cmd.cli.backup.runRestore(allocator, io, &client, args);
    if (std.mem.eql(u8, subcommand, "internal")) return cmd.cli.internal.run(allocator, io, &client, args);
}

fn printUsage(argv0: []const u8) void {
    std.debug.print(
        \\usage: {s} <subcommand> [options]
        \\
        \\server subcommands:
        \\  data
        \\  metadata
        \\  swarm
        \\  inference
        \\  serverless
        \\
        \\client subcommands:
        \\  table          Manage tables (create, drop, list, get)
        \\  index          Manage indexes (create, drop, list, get)
        \\  query          Query data from a table
        \\  lookup         Look up a document by key
        \\  load           Bulk load data from NDJSON file
        \\  insert         Insert a single document
        \\  delete         Delete a single document
        \\  agents         Run AI agents (retrieval, query-builder)
        \\  backup         Backup tables
        \\  restore        Restore tables from backup
        \\  internal       Internal cluster management
        \\  cloud          Delegate to the separate Antfly Cloud CLI
        \\
    , .{argv0});
}

fn printVersion() void {
    std.debug.print("antfly {s} (zig runtime)\n", .{antfly.build_options.antfly_version});
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
    const code = try runAntflyCloudArgv(std.testing.io, &.{"definitely-missing-antfly-cloud-for-test"});
    try std.testing.expectEqual(@as(u8, 127), code);
}

test "cloud shim propagates child exit code" {
    const code = try runAntflyCloudArgv(std.testing.io, &.{ "/bin/sh", "-c", "exit 23" });
    try std.testing.expectEqual(@as(u8, 23), code);
}
