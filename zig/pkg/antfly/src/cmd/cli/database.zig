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
    const subcommand = args.next() orelse return listDatabases(allocator, io, client);

    if (std.mem.eql(u8, subcommand, "list")) return listDatabases(allocator, io, client);
    if (std.mem.eql(u8, subcommand, "get")) return getDatabase(allocator, io, client, args);
    if (std.mem.eql(u8, subcommand, "create")) return createDatabase(allocator, io, client, args);
    if (std.mem.eql(u8, subcommand, "drop")) return dropDatabase(allocator, io, client, args);
    if (std.mem.eql(u8, subcommand, "set-tablespace")) return setDatabaseTablespace(allocator, io, client, args);
    if (std.mem.eql(u8, subcommand, "clear-tablespace")) return clearDatabaseTablespace(allocator, io, client, args);

    if (std.mem.eql(u8, subcommand, "rename")) {
        const name = args.next() orelse cli.fatal("resource name is required", .{});
        const new_name = args.next() orelse cli.fatal("new name is required", .{});
        if (args.next() != null) cli.fatal("unexpected rename argument", .{});
        var response = try client.inner.renameDatabase(name, .{ .name = new_name });
        defer response.deinit();
        if (response.status_code != 204) cli.fatal("rename failed with HTTP {d}", .{response.status_code});
        return;
    }

    cli.fatal("unknown database subcommand: {s}", .{subcommand});
}

fn listDatabases(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient) !void {
    var resp = try client.inner.listDatabases();
    defer resp.deinit();
    if (resp.status_code < 200 or resp.status_code >= 300) cli.fatal("catalog request failed with HTTP {d}", .{resp.status_code});
    if (resp.data) |parsed| {
        try cli.writeJson(allocator, io, parsed.value);
    }
}

fn getDatabase(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const database_name = nextNameArg(args, "database name is required");
    var resp = try client.inner.getDatabase(database_name);
    defer resp.deinit();
    if (resp.status_code < 200 or resp.status_code >= 300) cli.fatal("catalog request failed with HTTP {d}", .{resp.status_code});
    if (resp.data) |parsed| {
        try cli.writeJson(allocator, io, parsed.value);
    }
}

fn createDatabase(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const database_name = nextNameArg(args, "database name is required");
    var resp = try client.inner.createDatabase(database_name);
    defer resp.deinit();
    if (resp.status_code < 200 or resp.status_code >= 300) cli.fatal("catalog request failed with HTTP {d}", .{resp.status_code});
    if (resp.data) |parsed| {
        try cli.writeJson(allocator, io, parsed.value);
    }
}

fn dropDatabase(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const database_name = nextNameArg(args, "database name is required");
    var resp = try client.inner.dropDatabase(database_name);
    defer resp.deinit();
    if (resp.status_code < 200 or resp.status_code >= 300) cli.fatal("catalog request failed with HTTP {d}", .{resp.status_code});
    if (resp.data) |parsed| {
        try cli.writeJson(allocator, io, parsed.value);
    }
}

fn setDatabaseTablespace(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const parsed = parseTablespaceBindingArgs(args);
    var resp = try client.inner.setDatabaseTablespace(parsed.database_name, .{ .tablespace_name = parsed.tablespace_name });
    defer resp.deinit();
    if (resp.status_code < 200 or resp.status_code >= 300) cli.fatal("catalog request failed with HTTP {d}", .{resp.status_code});
    if (resp.data) |data| {
        try cli.writeJson(allocator, io, data.value);
    }
}

fn clearDatabaseTablespace(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const database_name = nextNameArg(args, "database name is required");
    var resp = try client.inner.clearDatabaseTablespace(database_name);
    defer resp.deinit();
    if (resp.status_code < 200 or resp.status_code >= 300) cli.fatal("catalog request failed with HTTP {d}", .{resp.status_code});
    if (resp.data) |data| {
        try cli.writeJson(allocator, io, data.value);
    }
}

const TablespaceBindingArgs = struct {
    database_name: []const u8,
    tablespace_name: []const u8,
};

fn parseTablespaceBindingArgs(args: *std.process.Args.Iterator) TablespaceBindingArgs {
    var database_name: ?[]const u8 = null;
    var tablespace_name: ?[]const u8 = null;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--database")) {
            database_name = args.next() orelse cli.fatal("--database requires a value", .{});
        } else if (std.mem.eql(u8, arg, "--tablespace")) {
            tablespace_name = args.next() orelse cli.fatal("--tablespace requires a value", .{});
        } else if (!std.mem.startsWith(u8, arg, "--") and database_name == null) {
            database_name = arg;
        } else if (!std.mem.startsWith(u8, arg, "--") and tablespace_name == null) {
            tablespace_name = arg;
        }
    }
    return .{
        .database_name = database_name orelse cli.fatal("database name is required", .{}),
        .tablespace_name = tablespace_name orelse cli.fatal("tablespace name is required", .{}),
    };
}

fn nextNameArg(args: *std.process.Args.Iterator, comptime missing_message: []const u8) []const u8 {
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--database")) {
            return args.next() orelse cli.fatal("--database requires a value", .{});
        }
        if (!std.mem.startsWith(u8, arg, "--")) return arg;
    }
    cli.fatal(missing_message, .{});
}

test "database cli module compiles" {
    _ = run;
}
