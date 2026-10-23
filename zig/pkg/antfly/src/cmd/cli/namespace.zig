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
    const subcommand = args.next() orelse return listNamespaces(allocator, io, client, args);

    if (std.mem.eql(u8, subcommand, "list")) return listNamespaces(allocator, io, client, args);
    if (std.mem.eql(u8, subcommand, "create")) return createNamespace(allocator, io, client, args);
    if (std.mem.eql(u8, subcommand, "drop")) return dropNamespace(allocator, io, client, args);
    if (std.mem.eql(u8, subcommand, "set-tablespace")) return setNamespaceTablespace(allocator, io, client, args);
    if (std.mem.eql(u8, subcommand, "clear-tablespace")) return clearNamespaceTablespace(allocator, io, client, args);

    if (std.mem.eql(u8, subcommand, "rename")) {
        const database = args.next() orelse cli.argumentError("database name is required", .{});
        const name = args.next() orelse cli.argumentError("namespace name is required", .{});
        const new_name = args.next() orelse cli.argumentError("new name is required", .{});
        if (args.next() != null) cli.argumentError("unexpected rename argument", .{});
        var response = try client.inner.renameNamespace(database, name, .{ .name = new_name });
        defer response.deinit();
        cli.expectHttpSuccess(response);
        if (response.status_code != 204) return error.InvalidApiResponse;
        return;
    }

    cli.argumentError("unknown namespace subcommand: {s}", .{subcommand});
}

pub fn listNamespaces(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    cli.output.context = .{ .scope = .discovery, .execution = .complete };
    const parsed = parseNamespaceArgs(args, .{ .require_name = false });
    var resp = try client.inner.listNamespaces(parsed.database_name);
    defer resp.deinit();
    cli.expectHttpSuccess(resp);
    if (resp.data) |data| {
        try cli.writeJson(allocator, io, data.value);
    }
}

pub fn createNamespace(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const parsed = parseNamespaceArgs(args, .{ .require_name = true });
    var resp = try client.inner.createNamespace(parsed.database_name, parsed.namespace_name.?);
    defer resp.deinit();
    cli.expectHttpSuccess(resp);
    if (resp.data) |data| {
        try cli.writeJson(allocator, io, data.value);
    }
}

pub fn dropNamespace(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const parsed = parseNamespaceArgs(args, .{ .require_name = true });
    var resp = try client.inner.dropNamespace(parsed.database_name, parsed.namespace_name.?);
    defer resp.deinit();
    cli.expectHttpSuccess(resp);
    if (resp.data) |data| {
        try cli.writeJson(allocator, io, data.value);
    }
}

pub fn setNamespaceTablespace(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const parsed = parseNamespaceTablespaceArgs(args);
    var resp = try client.inner.setNamespaceTablespace(parsed.database_name, parsed.namespace_name, .{ .tablespace_name = parsed.tablespace_name });
    defer resp.deinit();
    cli.expectHttpSuccess(resp);
    if (resp.data) |data| {
        try cli.writeJson(allocator, io, data.value);
    }
}

pub fn clearNamespaceTablespace(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const parsed = parseNamespaceArgs(args, .{ .require_name = true });
    var resp = try client.inner.clearNamespaceTablespace(parsed.database_name, parsed.namespace_name.?);
    defer resp.deinit();
    cli.expectHttpSuccess(resp);
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

const NamespaceTablespaceArgs = struct {
    database_name: []const u8,
    namespace_name: []const u8,
    tablespace_name: []const u8,
};

fn parseNamespaceTablespaceArgs(args: *std.process.Args.Iterator) NamespaceTablespaceArgs {
    var database_name: ?[]const u8 = null;
    var namespace_name: ?[]const u8 = null;
    var tablespace_name: ?[]const u8 = null;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--database") or std.mem.eql(u8, arg, "-d")) {
            database_name = args.next() orelse cli.argumentError("--database requires a value", .{});
        } else if (std.mem.eql(u8, arg, "--namespace") or std.mem.eql(u8, arg, "-n")) {
            namespace_name = args.next() orelse cli.argumentError("--namespace requires a value", .{});
        } else if (std.mem.eql(u8, arg, "--tablespace")) {
            tablespace_name = args.next() orelse cli.argumentError("--tablespace requires a value", .{});
        } else if (!std.mem.startsWith(u8, arg, "--") and namespace_name == null) {
            namespace_name = arg;
        } else if (!std.mem.startsWith(u8, arg, "--") and tablespace_name == null) {
            tablespace_name = arg;
        }
    }

    const defaults = cli.CatalogFlags.defaultsFromEnv();
    return .{
        .database_name = database_name orelse defaults.database orelse cli.argumentError("--database is required or ANTFLY_DATABASE must be configured", .{}),
        .namespace_name = namespace_name orelse cli.argumentError("namespace name is required", .{}),
        .tablespace_name = tablespace_name orelse cli.argumentError("tablespace name is required", .{}),
    };
}

fn parseNamespaceArgs(args: *std.process.Args.Iterator, options: NamespaceArgsOptions) NamespaceArgs {
    var database_name: ?[]const u8 = null;
    var namespace_name: ?[]const u8 = null;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--database") or std.mem.eql(u8, arg, "-d")) {
            database_name = args.next() orelse cli.argumentError("--database requires a value", .{});
        } else if (std.mem.eql(u8, arg, "--namespace") or std.mem.eql(u8, arg, "-n")) {
            namespace_name = args.next() orelse cli.argumentError("--namespace requires a value", .{});
        } else if (!std.mem.startsWith(u8, arg, "--") and namespace_name == null) {
            namespace_name = arg;
        }
    }

    const defaults = cli.CatalogFlags.defaultsFromEnv();
    return .{
        .database_name = database_name orelse defaults.database orelse cli.argumentError("--database is required or ANTFLY_DATABASE must be configured", .{}),
        .namespace_name = if (options.require_name) namespace_name orelse cli.argumentError("namespace name is required", .{}) else namespace_name,
    };
}

test "namespace cli module compiles" {
    _ = run;
}
