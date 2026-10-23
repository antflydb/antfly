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
        const name = args.next() orelse cli.argumentError("resource name is required", .{});
        const new_name = args.next() orelse cli.argumentError("new name is required", .{});
        if (args.next() != null) cli.argumentError("unexpected rename argument", .{});
        var response = try client.inner.renameDatabase(name, .{ .name = new_name });
        defer response.deinit();
        cli.expectHttpSuccess(response);
        if (response.status_code != 204) return error.InvalidApiResponse;
        return;
    }

    cli.argumentError("unknown database subcommand: {s}", .{subcommand});
}

pub fn listDatabases(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient) !void {
    cli.output.context = .{ .scope = .discovery, .execution = .complete };
    var resp = try client.inner.listDatabases();
    defer resp.deinit();
    cli.expectHttpSuccess(resp);
    if (resp.data) |parsed| {
        try cli.writeJson(allocator, io, parsed.value);
    }
}

pub fn getDatabase(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    cli.output.context = .{ .scope = .discovery, .execution = .complete };
    const database_name = nextNameArg(args, "database name is required");
    var resp = try client.inner.getDatabase(database_name);
    defer resp.deinit();
    cli.expectHttpSuccess(resp);
    if (resp.data) |parsed| {
        try cli.writeJson(allocator, io, parsed.value);
    }
}

pub fn createDatabase(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const database_name = nextNameArg(args, "database name is required");
    var resp = try client.inner.createDatabase(database_name);
    defer resp.deinit();
    cli.expectHttpSuccess(resp);
    if (resp.data) |parsed| {
        try cli.writeJson(allocator, io, parsed.value);
    }
}

pub fn dropDatabase(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const database_name = nextNameArg(args, "database name is required");
    var resp = try client.inner.dropDatabase(database_name);
    defer resp.deinit();
    cli.expectHttpSuccess(resp);
    if (resp.data) |parsed| {
        try cli.writeJson(allocator, io, parsed.value);
    }
}

pub fn setDatabaseTablespace(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const parsed = parseTablespaceBindingArgs(args);
    var resp = try client.inner.setDatabaseTablespace(parsed.database_name, .{ .tablespace_name = parsed.tablespace_name });
    defer resp.deinit();
    cli.expectHttpSuccess(resp);
    if (resp.data) |data| {
        try cli.writeJson(allocator, io, data.value);
    }
}

pub fn clearDatabaseTablespace(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const database_name = nextNameArg(args, "database name is required");
    var resp = try client.inner.clearDatabaseTablespace(database_name);
    defer resp.deinit();
    cli.expectHttpSuccess(resp);
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
            database_name = args.next() orelse cli.argumentError("--database requires a value", .{});
        } else if (std.mem.eql(u8, arg, "--tablespace")) {
            tablespace_name = args.next() orelse cli.argumentError("--tablespace requires a value", .{});
        } else if (!std.mem.startsWith(u8, arg, "--") and database_name == null) {
            database_name = arg;
        } else if (!std.mem.startsWith(u8, arg, "--") and tablespace_name == null) {
            tablespace_name = arg;
        }
    }
    return .{
        .database_name = database_name orelse cli.argumentError("database name is required", .{}),
        .tablespace_name = tablespace_name orelse cli.argumentError("tablespace name is required", .{}),
    };
}

fn nextNameArg(args: *std.process.Args.Iterator, comptime missing_message: []const u8) []const u8 {
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--database")) {
            return args.next() orelse cli.argumentError("--database requires a value", .{});
        }
        if (!std.mem.startsWith(u8, arg, "--")) return arg;
    }
    cli.argumentError(missing_message, .{});
}

test "database cli module compiles" {
    _ = run;
}
