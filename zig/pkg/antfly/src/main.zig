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
const platform = @import("antfly_platform");
const build_options = @import("build_options");
const runtime_bridge = @import("runtime_bridge.zig");

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

    // Server-side subcommands dispatch into independently generated runtime
    // units through the narrow internal ABI.
    if (std.mem.eql(u8, subcommand, "data")) return runRuntimeUnit(.data, subcommand, init, &args);
    if (std.mem.eql(u8, subcommand, "ha")) return runRuntimeUnit(.cli, subcommand, init, &args);
    if (std.mem.eql(u8, subcommand, "inference")) return runRuntimeUnit(.inference, subcommand, init, &args);
    // Lite serve embeds the standalone runtime, so keep the entire Lite
    // command in that codegen unit instead of pulling it into the CLI.
    if (std.mem.eql(u8, subcommand, "lite")) return runRuntimeUnit(.standalone, subcommand, init, &args);
    if (std.mem.eql(u8, subcommand, "metadata")) return runRuntimeUnit(.metadata, subcommand, init, &args);
    if (std.mem.eql(u8, subcommand, "serverless")) return runRuntimeUnit(.serverless, subcommand, init, &args);
    if (std.mem.eql(u8, subcommand, "standalone")) return runRuntimeUnit(.standalone, subcommand, init, &args);

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
            return runRuntimeUnit(.cli, subcommand, init, &args);
        }
    }

    std.debug.print("unknown subcommand: {s}\n", .{subcommand});
    printUsage(argv0);
    return error.InvalidArguments;
}

const RuntimeRole = enum { cli, data, inference, metadata, serverless, standalone };

extern fn antfly_runtime_cli(context: *const runtime_bridge.Context) callconv(.c) c_int;
extern fn antfly_runtime_data(context: *const runtime_bridge.Context) callconv(.c) c_int;
extern fn antfly_runtime_inference(context: *const runtime_bridge.Context) callconv(.c) c_int;
extern fn antfly_runtime_metadata(context: *const runtime_bridge.Context) callconv(.c) c_int;
extern fn antfly_runtime_serverless(context: *const runtime_bridge.Context) callconv(.c) c_int;
extern fn antfly_runtime_standalone(context: *const runtime_bridge.Context) callconv(.c) c_int;

fn runRuntimeUnit(
    comptime role: RuntimeRole,
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
        .serverless => antfly_runtime_serverless(&context),
        .standalone => antfly_runtime_standalone(&context),
    };
    if (code != 0) std.process.exit(@intCast(code));
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
