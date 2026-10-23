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
    // Dispatch offline inspection before global configuration, credentials,
    // HTTP setup, or catalog-scope argument processing.
    if (std.mem.eql(u8, command, "backup")) {
        var probe = args.*;
        if (probe.next()) |subcommand| {
            if (std.mem.eql(u8, subcommand, "inspect")) {
                _ = args.next();
                var inspection_io = std.Io.Threaded.init(std.heap.page_allocator, .{});
                defer inspection_io.deinit();
                return cli.backup.runInspect(init.gpa, inspection_io.io(), args);
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

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    if (std.mem.eql(u8, command, "connections") and
        try cli.connections.runLocalProviderIfRequested(init.gpa, io, args)) return;

    var http = httpx.Client.initWithConfig(init.gpa, io, .{});
    defer http.deinit();

    var config = cli.parseGlobalFlags();
    var client_arguments = try extractUrlOption(init.gpa, args.*, &config);
    defer client_arguments.deinit(init.gpa);
    args.* = std.process.Args.Iterator.init(.{ .vector = client_arguments.items });
    var client = try cli.initClient(init.gpa, &http, config);
    defer client.deinit();

    var scoped_arguments: std.ArrayList([*:0]const u8) = .empty;
    defer scoped_arguments.deinit(init.gpa);
    const supports_scope = blk: {
        for ([_][]const u8{ "table", "index", "query", "lookup", "load", "insert", "delete", "backup", "restore" }) |name| if (std.mem.eql(u8, command, name)) break :blk true;
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
    var probe = args.*;
    var first = true;
    while (probe.next()) |arg| {
        if (cli.isHelpArg(arg)) return true;
        if (first and std.mem.eql(u8, arg, "help")) return true;
        first = false;
    }
    return false;
}

test "client runtime recognizes help without consuming arguments" {
    var argv = [_][*:0]const u8{ "--table", "docs", "--help" };
    var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    try std.testing.expect(helpRequested(&args));
    try std.testing.expectEqualStrings("--table", args.next().?);
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
