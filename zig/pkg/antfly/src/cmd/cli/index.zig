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
    var command_args = args.*;
    const route = parseRoute(args.*);
    const tbl = route.table_name orelse cli.fatal("--table is required for index commands", .{});

    if (route.subcommand) |cmd| {
        if (std.mem.eql(u8, cmd, "create")) return createIndex(allocator, client, tbl, &command_args);
        if (std.mem.eql(u8, cmd, "drop")) return dropIndex(client, tbl, route.index_name, &command_args);
        if (std.mem.eql(u8, cmd, "list")) return listIndexes(allocator, io, client, tbl);
        if (std.mem.eql(u8, cmd, "get")) return getIndex(allocator, io, client, tbl, route.index_name, &command_args);
    }

    if (route.index_name) |idx| {
        return getIndexByName(allocator, io, client, tbl, idx);
    }
    return listIndexes(allocator, io, client, tbl);
}

const Route = struct {
    table_name: ?[]const u8 = null,
    index_name: ?[]const u8 = null,
    subcommand: ?[]const u8 = null,
};

fn parseRoute(iterator: std.process.Args.Iterator) Route {
    var args = iterator;
    var route: Route = .{};
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            route.table_name = args.next();
        } else if (std.mem.eql(u8, arg, "--index") or std.mem.eql(u8, arg, "-i")) {
            route.index_name = args.next();
        } else if (std.mem.eql(u8, arg, "--type") or std.mem.eql(u8, arg, "--field") or
            std.mem.eql(u8, arg, "--template") or std.mem.eql(u8, arg, "--embedder") or
            std.mem.eql(u8, arg, "--generator") or std.mem.eql(u8, arg, "--chunker"))
        {
            _ = args.next();
        } else if (std.mem.eql(u8, arg, "create") or std.mem.eql(u8, arg, "drop") or
            std.mem.eql(u8, arg, "list") or std.mem.eql(u8, arg, "get"))
        {
            if (route.subcommand == null) route.subcommand = arg;
        }
    }
    return route;
}

test "index route accepts flags before and after the action" {
    var documented_argv = [_][*:0]const u8{ "list", "--table", "wikipedia" };
    const documented = parseRoute(std.process.Args.Iterator.init(.{ .vector = documented_argv[0..] }));
    try std.testing.expectEqualStrings("list", documented.subcommand.?);
    try std.testing.expectEqualStrings("wikipedia", documented.table_name.?);

    var legacy_argv = [_][*:0]const u8{ "--table", "wikipedia", "list" };
    const legacy = parseRoute(std.process.Args.Iterator.init(.{ .vector = legacy_argv[0..] }));
    try std.testing.expectEqualStrings("list", legacy.subcommand.?);
    try std.testing.expectEqualStrings("wikipedia", legacy.table_name.?);

    var value_argv = [_][*:0]const u8{ "create", "--table", "docs", "--type", "list" };
    const value = parseRoute(std.process.Args.Iterator.init(.{ .vector = value_argv[0..] }));
    try std.testing.expectEqualStrings("create", value.subcommand.?);

    var flags_first_argv = [_][*:0]const u8{ "--table", "docs", "--type", "list", "create" };
    const flags_first = parseRoute(std.process.Args.Iterator.init(.{ .vector = flags_first_argv[0..] }));
    try std.testing.expectEqualStrings("create", flags_first.subcommand.?);
}

fn createIndex(allocator: std.mem.Allocator, client: *antfly_client.AntflyClient, table_name: []const u8, args: *std.process.Args.Iterator) !void {
    var idx_name: ?[]const u8 = null;
    var idx_type: ?[]const u8 = null;
    var field: ?[]const u8 = null;
    var template: ?[]const u8 = null;
    var embedder_json: ?[]const u8 = null;
    var generator_json: ?[]const u8 = null;
    var chunker_json: ?[]const u8 = null;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--index") or std.mem.eql(u8, arg, "-i")) {
            idx_name = args.next();
        } else if (std.mem.eql(u8, arg, "--type")) {
            idx_type = args.next();
        } else if (std.mem.eql(u8, arg, "--field")) {
            field = args.next();
        } else if (std.mem.eql(u8, arg, "--template")) {
            template = args.next();
        } else if (std.mem.eql(u8, arg, "--embedder")) {
            embedder_json = args.next();
        } else if (std.mem.eql(u8, arg, "--generator")) {
            generator_json = args.next();
        } else if (std.mem.eql(u8, arg, "--chunker")) {
            chunker_json = args.next();
        } else if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            _ = args.next(); // already parsed
        }
    }

    const name = idx_name orelse cli.fatal("--index is required", .{});

    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();
    const writer = &out.writer;

    try writer.writeAll("{");
    try writer.print("\"name\":\"{s}\"", .{name});
    if (idx_type) |t| try writer.print(",\"type\":\"{s}\"", .{t});
    if (field) |f| try writer.print(",\"field\":\"{s}\"", .{f});
    if (template) |t| try writer.print(",\"template\":\"{s}\"", .{t});
    if (embedder_json) |e| try writer.print(",\"embedder\":{s}", .{e});
    if (generator_json) |g| try writer.print(",\"generator\":{s}", .{g});
    if (chunker_json) |c| try writer.print(",\"chunker\":{s}", .{c});
    try writer.writeAll("}");

    const json_body = out.written();
    var parsed = std.json.parseFromSlice(antfly_client.AntflyClient.IndexConfig, allocator, json_body, .{}) catch |err| {
        cli.fatal("failed to build index config: {}", .{err});
    };
    defer parsed.deinit();

    try client.createIndex(table_name, name, parsed.value);
    std.debug.print("Create index command successful.\n", .{});
}

fn dropIndex(client: *antfly_client.AntflyClient, table_name: []const u8, pre_index: ?[]const u8, args: *std.process.Args.Iterator) !void {
    var idx_name = pre_index;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--index") or std.mem.eql(u8, arg, "-i")) {
            idx_name = args.next();
        }
    }
    const name = idx_name orelse cli.fatal("--index is required", .{});
    try client.dropIndex(table_name, name);
    std.debug.print("Drop index command successful.\n", .{});
}

fn listIndexes(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, table_name: []const u8) !void {
    var resp = try client.listIndexes(table_name);
    defer resp.deinit();
    if (resp.data) |parsed| {
        try cli.writeJson(allocator, io, parsed.value);
    }
}

fn getIndex(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, table_name: []const u8, pre_index: ?[]const u8, args: *std.process.Args.Iterator) !void {
    var idx_name = pre_index;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--index") or std.mem.eql(u8, arg, "-i")) {
            idx_name = args.next();
        }
    }
    const name = idx_name orelse cli.fatal("--index is required", .{});
    return getIndexByName(allocator, io, client, table_name, name);
}

fn getIndexByName(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, table_name: []const u8, index_name: []const u8) !void {
    var resp = try client.getIndex(table_name, index_name);
    defer resp.deinit();
    if (resp.data) |parsed| {
        try cli.writeJson(allocator, io, parsed.value);
    }
}
