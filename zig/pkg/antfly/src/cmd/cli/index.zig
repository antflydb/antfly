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
    var table_name: ?[]const u8 = null;
    var index_name: ?[]const u8 = null;
    var subcommand: ?[]const u8 = null;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            table_name = args.next();
        } else if (std.mem.eql(u8, arg, "--index") or std.mem.eql(u8, arg, "-i")) {
            index_name = args.next();
        } else if (std.mem.eql(u8, arg, "create") or std.mem.eql(u8, arg, "drop") or
            std.mem.eql(u8, arg, "list") or std.mem.eql(u8, arg, "get"))
        {
            subcommand = arg;
            break;
        }
    }

    const tbl = table_name orelse cli.fatal("--table is required for index commands", .{});

    if (subcommand) |cmd| {
        if (std.mem.eql(u8, cmd, "create")) return createIndex(allocator, client, tbl, args);
        if (std.mem.eql(u8, cmd, "drop")) return dropIndex(client, tbl, index_name, args);
        if (std.mem.eql(u8, cmd, "list")) return listIndexes(allocator, io, client, tbl);
        if (std.mem.eql(u8, cmd, "get")) return getIndex(allocator, io, client, tbl, index_name, args);
    }

    if (index_name) |idx| {
        return getIndexByName(allocator, io, client, tbl, idx);
    }
    return listIndexes(allocator, io, client, tbl);
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

    try writer.writeAll("{\"config\":{");
    try writer.print("\"name\":\"{s}\"", .{name});
    if (idx_type) |t| try writer.print(",\"type\":\"{s}\"", .{t});
    if (field) |f| try writer.print(",\"field\":\"{s}\"", .{f});
    if (template) |t| try writer.print(",\"template\":\"{s}\"", .{t});
    if (embedder_json) |e| try writer.print(",\"embedder\":{s}", .{e});
    if (generator_json) |g| try writer.print(",\"generator\":{s}", .{g});
    if (chunker_json) |c| try writer.print(",\"chunker\":{s}", .{c});
    try writer.writeAll("}}");

    const json_body = out.written();
    var parsed = std.json.parseFromSlice(antfly_client.AntflyClient.IndexConfig, allocator, json_body, .{ .ignore_unknown_fields = true }) catch |err| {
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
