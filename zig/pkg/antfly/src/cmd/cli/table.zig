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
    const subcommand = args.next() orelse {
        return listTables(allocator, io, client);
    };

    if (std.mem.eql(u8, subcommand, "create")) return createTable(allocator, io, client, args);
    if (std.mem.eql(u8, subcommand, "drop")) return dropTable(client, args);
    if (std.mem.eql(u8, subcommand, "list")) return listTablesWithArgs(allocator, io, client, args);
    if (std.mem.eql(u8, subcommand, "get")) return getTable(allocator, io, client, args);

    if (std.mem.startsWith(u8, subcommand, "--")) {
        return runWithFlags(allocator, io, client, subcommand, args);
    }

    cli.fatal("unknown table subcommand: {s}", .{subcommand});
}

fn runWithFlags(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, first_arg: []const u8, args: *std.process.Args.Iterator) !void {
    var table_name: ?[]const u8 = null;
    var output: ?[]const u8 = null;
    var current_arg: ?[]const u8 = first_arg;

    while (current_arg) |arg| : (current_arg = args.next()) {
        if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            table_name = args.next();
        } else if (std.mem.eql(u8, arg, "--output") or std.mem.eql(u8, arg, "-o")) {
            output = args.next();
        }
    }

    if (output) |value| {
        if (!std.mem.eql(u8, value, "json")) {
            cli.fatal("only JSON output is supported for table", .{});
        }
    }

    if (table_name) |name| {
        return getTableByName(allocator, io, client, name);
    }
    return listTablesMode(allocator, io, client, if (output != null) .json else .summary);
}

fn createTable(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    var table_name: ?[]const u8 = null;
    var shards: ?i64 = null;
    var file_path: ?[]const u8 = null;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            table_name = args.next();
        } else if (std.mem.eql(u8, arg, "--shards")) {
            if (args.next()) |s| {
                shards = std.fmt.parseInt(i64, s, 10) catch null;
            }
        } else if (std.mem.eql(u8, arg, "--file") or std.mem.eql(u8, arg, "-f")) {
            file_path = args.next();
        }
    }

    const name = table_name orelse cli.fatal("--table is required", .{});

    var body = antfly_client.types.CreateTableRequest{};
    if (file_path) |path| {
        const file_data = cli.readFileAlloc(io, allocator, path, 10 * 1024 * 1024) catch |err| {
            cli.fatal("reading config file {s}: {}", .{ path, err });
        };
        defer allocator.free(file_data);
        var parsed = std.json.parseFromSlice(antfly_client.types.CreateTableRequest, allocator, file_data, .{}) catch |err| {
            cli.fatal("parsing config file {s}: {}", .{ path, err });
        };
        defer parsed.deinit();
        if (shards != null) {
            cli.fatal("--shards with --file is not supported; put num_shards in {s}", .{path});
        }
        try client.createTable(name, parsed.value);
        std.debug.print("Create table command successful.\n", .{});
        return;
    }

    if (shards) |s| body.num_shards = s;

    try client.createTable(name, body);
    std.debug.print("Create table command successful.\n", .{});
}

fn dropTable(client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    var table_name: ?[]const u8 = null;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            table_name = args.next();
        }
    }
    const name = table_name orelse cli.fatal("--table is required", .{});
    try client.dropTable(name);
    std.debug.print("Drop table command successful.\n", .{});
}

fn listTables(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient) !void {
    return listTablesMode(allocator, io, client, .summary);
}

const ListOutput = enum { summary, json };

fn listTablesWithArgs(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    var output: ListOutput = .summary;
    var explicitly_selected = false;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--verbose")) {
            if (explicitly_selected) cli.fatal("use only one of --verbose or --output json", .{});
            output = .json;
            explicitly_selected = true;
        } else if (std.mem.eql(u8, arg, "--output") or std.mem.eql(u8, arg, "-o")) {
            if (explicitly_selected) cli.fatal("use only one of --verbose or --output json", .{});
            const value = args.next() orelse cli.fatal("--output requires json", .{});
            if (!std.mem.eql(u8, value, "json")) cli.fatal("only JSON output is supported for table", .{});
            output = .json;
            explicitly_selected = true;
        } else {
            cli.fatal("unknown table list option: {s}", .{arg});
        }
    }
    return listTablesMode(allocator, io, client, output);
}

fn listTablesMode(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, output: ListOutput) !void {
    var resp = try client.listTables();
    defer resp.deinit();
    if (resp.data) |parsed| {
        if (output == .json) return cli.writeJson(allocator, io, parsed.value);
        cli.writeStdout(io, "NAME\tSHARDS\tINDEXES\tSTORAGE\n");
        for (parsed.value) |table| {
            const storage = if (table.storage_status.empty orelse false)
                "empty"
            else if (table.storage_status.disk_usage != null)
                "used"
            else
                "unknown";
            const line = try std.fmt.allocPrint(
                allocator,
                "{s}\t{d}\t{d}\t{s}\n",
                .{ table.name, table.shards.map.count(), table.indexes.map.count(), storage },
            );
            defer allocator.free(line);
            cli.writeStdout(io, line);
        }
    }
}

fn getTable(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    var table_name: ?[]const u8 = null;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            table_name = args.next();
        }
    }
    const name = table_name orelse cli.fatal("--table is required", .{});
    return getTableByName(allocator, io, client, name);
}

fn getTableByName(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, name: []const u8) !void {
    var resp = try client.getTable(name);
    defer resp.deinit();
    if (resp.data) |parsed| {
        try cli.writeJson(allocator, io, parsed.value);
    }
}
