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

pub fn run(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const subcommand = args.next() orelse return listNamespaces(allocator, io, client, args);

    if (std.mem.eql(u8, subcommand, "list")) return listNamespaces(allocator, io, client, args);
    if (std.mem.eql(u8, subcommand, "create")) return createNamespace(allocator, io, client, args);
    if (std.mem.eql(u8, subcommand, "drop")) return dropNamespace(allocator, io, client, args);

    cli.fatal("unknown namespace subcommand: {s}", .{subcommand});
}

fn listNamespaces(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const parsed = parseNamespaceArgs(args, .{ .require_name = false });
    var resp = try client.inner.listNamespaces(parsed.database_name);
    defer resp.deinit();
    if (resp.data) |data| {
        try cli.writeJson(allocator, io, data.value);
    }
}

fn createNamespace(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const parsed = parseNamespaceArgs(args, .{ .require_name = true });
    var resp = try client.inner.createNamespace(parsed.database_name, parsed.namespace_name.?);
    defer resp.deinit();
    if (resp.data) |data| {
        try cli.writeJson(allocator, io, data.value);
    }
}

fn dropNamespace(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const parsed = parseNamespaceArgs(args, .{ .require_name = true });
    var resp = try client.inner.dropNamespace(parsed.database_name, parsed.namespace_name.?);
    defer resp.deinit();
    if (resp.data) |data| {
        try cli.writeJson(allocator, io, data.value);
    }
}

const NamespaceArgsOptions = struct {
    require_name: bool,
};

const NamespaceArgs = struct {
    database_name: []const u8,
    namespace_name: ?[]const u8,
};

fn parseNamespaceArgs(args: *std.process.Args.Iterator, options: NamespaceArgsOptions) NamespaceArgs {
    var database_name: ?[]const u8 = null;
    var namespace_name: ?[]const u8 = null;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--database") or std.mem.eql(u8, arg, "-d")) {
            database_name = args.next() orelse cli.fatal("--database requires a value", .{});
        } else if (std.mem.eql(u8, arg, "--namespace") or std.mem.eql(u8, arg, "-n")) {
            namespace_name = args.next() orelse cli.fatal("--namespace requires a value", .{});
        } else if (!std.mem.startsWith(u8, arg, "--") and namespace_name == null) {
            namespace_name = arg;
        }
    }

    return .{
        .database_name = database_name orelse cli.fatal("--database is required", .{}),
        .namespace_name = if (options.require_name) namespace_name orelse cli.fatal("namespace name is required", .{}) else namespace_name,
    };
}

test "namespace cli module compiles" {
    _ = run;
}
