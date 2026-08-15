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
const structlog = @import("structlog");
const build_options = @import("build_options");
const completion = @import("completion.zig");
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

    if (std.mem.eql(u8, subcommand, "--help") or std.mem.eql(u8, subcommand, "-h")) {
        printUsage(argv0);
        return;
    }
    if (std.mem.eql(u8, subcommand, "--version")) {
        printVersion();
        return;
    }

    const command = completion.findCommand(subcommand) orelse {
        std.debug.print("unknown subcommand: {s}\n", .{subcommand});
        printUsage(argv0);
        return error.InvalidArguments;
    };

    // Server-side subcommands dispatch into independently generated runtime
    // units through the narrow internal ABI. Lite uses standalone because its
    // serve command embeds that runtime.
    switch (command.route) {
        .cli => return runRuntimeUnit(.cli, subcommand, init, &args),
        .data => return runRuntimeUnit(.data, subcommand, init, &args),
        .ha => return runRuntimeUnit(.ha, subcommand, init, &args),
        .inference => return runRuntimeUnit(.inference, subcommand, init, &args),
        .metadata => return runRuntimeUnit(.metadata, subcommand, init, &args),
        .serverless => return runRuntimeUnit(.serverless, subcommand, init, &args),
        .standalone => return runRuntimeUnit(.standalone, subcommand, init, &args),
        .cloud => {
            const code = try runAntflyCloud(init.gpa, init.io, &args);
            std.process.exit(code);
        },
        .completion => return runCompletion(init.io, &args),
        .help => printUsage(argv0),
        .version => printVersion(),
    }
}

const RuntimeRole = enum { cli, data, ha, inference, metadata, serverless, standalone };

extern fn antfly_runtime_cli(context: *const runtime_bridge.Context) callconv(.c) c_int;
extern fn antfly_runtime_data(context: *const runtime_bridge.Context) callconv(.c) c_int;
extern fn antfly_runtime_ha(context: *const runtime_bridge.Context) callconv(.c) c_int;
extern fn antfly_runtime_inference(context: *const runtime_bridge.Context) callconv(.c) c_int;
extern fn antfly_runtime_metadata(context: *const runtime_bridge.Context) callconv(.c) c_int;
extern fn antfly_runtime_serverless(context: *const runtime_bridge.Context) callconv(.c) c_int;
extern fn antfly_runtime_standalone(context: *const runtime_bridge.Context) callconv(.c) c_int;

fn runRuntimeUnit(
    comptime role: RuntimeRole,
    command: []const u8,
    init: std.process.Init,
    args: *std.process.Args.Iterator,
) !void {
    var argument_views: std.ArrayListUnmanaged(runtime_bridge.Bytes) = .empty;
    defer argument_views.deinit(init.gpa);
    while (args.next()) |arg| try argument_views.append(init.gpa, .init(arg));

    const environment_names = init.environ_map.keys();
    const environment_values = init.environ_map.values();
    std.debug.assert(environment_names.len == environment_values.len);
    const environment = try init.gpa.alloc(runtime_bridge.EnvironmentEntry, environment_names.len);
    defer init.gpa.free(environment);
    for (environment, environment_names, environment_values) |*entry, name, value| {
        entry.* = .{ .name = .init(name), .value = .init(value) };
    }

    const context = runtime_bridge.Context{
        .command = .init(command),
        .arguments_ptr = if (argument_views.items.len == 0) null else argument_views.items.ptr,
        .arguments_len = argument_views.items.len,
        .environment_ptr = if (environment.len == 0) null else environment.ptr,
        .environment_len = environment.len,
    };
    const code = switch (role) {
        .cli => antfly_runtime_cli(&context),
        .data => antfly_runtime_data(&context),
        .ha => antfly_runtime_ha(&context),
        .inference => antfly_runtime_inference(&context),
        .metadata => antfly_runtime_metadata(&context),
        .serverless => antfly_runtime_serverless(&context),
        .standalone => antfly_runtime_standalone(&context),
    };
    if (code != 0) std.process.exit(@intCast(code));
}

fn isStandaloneSubcommand(subcommand: []const u8) bool {
    const command = completion.findCommand(subcommand) orelse return false;
    return command.route == .standalone;
}

test "legacy swarm subcommand selects the standalone runtime" {
    try std.testing.expect(isStandaloneSubcommand("standalone"));
    try std.testing.expect(isStandaloneSubcommand("swarm"));
    try std.testing.expect(!isStandaloneSubcommand("data"));
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

fn runCompletion(io: std.Io, args: *std.process.Args.Iterator) !void {
    const shell_name = args.next() orelse {
        std.debug.print("usage: antfly completion <bash|zsh|fish>\n", .{});
        return error.InvalidArguments;
    };
    if (args.next() != null) return error.InvalidArguments;

    const shell = completion.Shell.parse(shell_name) catch {
        std.debug.print("unsupported shell: {s}; expected bash, zsh, or fish\n", .{shell_name});
        return error.InvalidArguments;
    };
    var buffer: [4096]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(io, &buffer);
    try completion.write(shell, &stdout_writer.interface);
    try stdout_writer.flush();
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
        \\  completion     Generate shell completions (bash, zsh, fish)
        \\
    , .{argv0});
}

fn printVersion() void {
    std.debug.print("antfly {s} (zig runtime)\n", .{build_options.antfly_version});
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
