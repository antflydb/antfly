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
        return listTablesWithCatalog(allocator, io, client, .{}, .summary);
    };

    if (std.mem.eql(u8, subcommand, "create")) return createTable(allocator, io, client, args);
    if (std.mem.eql(u8, subcommand, "drop")) return dropTable(allocator, io, client, args);
    if (std.mem.eql(u8, subcommand, "list")) return listTablesWithArgs(allocator, io, client, args);
    if (std.mem.eql(u8, subcommand, "get")) return getTable(allocator, io, client, args);

    if (std.mem.startsWith(u8, subcommand, "--")) {
        return runWithFlags(allocator, io, client, subcommand, args);
    }

    cli.fatal("unknown table subcommand: {s}", .{subcommand});
}

fn runWithFlags(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, first_arg: []const u8, args: *std.process.Args.Iterator) !void {
    var parsed = TableArgs{ .catalog = cli.CatalogFlags.defaultsFromEnv() };
    var output: ?[]const u8 = null;
    var current_arg: ?[]const u8 = first_arg;

    while (current_arg) |arg| : (current_arg = args.next()) {
        if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t") or cli.isCatalogFlag(arg)) {
            parseTableArg(&parsed, arg, args);
        } else if (std.mem.eql(u8, arg, "--output") or std.mem.eql(u8, arg, "-o")) {
            output = args.next() orelse cli.fatal("--output requires a value", .{});
        }
    }

    if (output) |value| {
        if (!std.mem.eql(u8, value, "json")) {
            cli.fatal("only JSON output is supported for table", .{});
        }
    }

    if (parsed.table_name) |name| {
        return getTableByName(allocator, io, client, parsed.catalog, name);
    }
    return listTablesWithCatalog(allocator, io, client, parsed.catalog, if (output != null) .json else .summary);
}

fn createTable(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    var parsed_args = TableArgs{ .catalog = cli.CatalogFlags.defaultsFromEnv() };
    var shards: ?i64 = null;
    var file_path: ?[]const u8 = null;

    while (args.next()) |arg| {
        if (isTableCatalogArg(arg)) {
            parseTableArg(&parsed_args, arg, args);
        } else if (std.mem.eql(u8, arg, "--shards")) {
            if (args.next()) |s| {
                shards = std.fmt.parseInt(i64, s, 10) catch null;
            }
        } else if (std.mem.eql(u8, arg, "--file") or std.mem.eql(u8, arg, "-f")) {
            file_path = args.next();
        }
    }

    const name = parsed_args.table_name orelse cli.fatal("--table is required", .{});

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
        return submitCreateTable(allocator, io, client, parsed_args.catalog, name, parsed.value);
    }

    if (shards) |s| body.num_shards = s;

    return submitCreateTable(allocator, io, client, parsed_args.catalog, name, body);
}

fn submitCreateTable(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, catalog_flags: cli.CatalogFlags, name: []const u8, body: antfly_client.types.CreateTableRequest) !void {
    if (catalog_flags.explicit()) |catalog| {
        var resp = try client.inner.createNamespaceTable(catalog.database, catalog.namespace, name, body);
        defer resp.deinit();
        if (resp.data) |parsed| return try cli.writeJson(allocator, io, parsed.value);
        return;
    }
    try client.createTable(name, body);
    cli.writeStdout(io, "Create table command successful.\n");
}

fn dropTable(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    var parsed_args = TableArgs{ .catalog = cli.CatalogFlags.defaultsFromEnv() };
    while (args.next()) |arg| {
        parseTableArg(&parsed_args, arg, args);
    }
    const name = parsed_args.table_name orelse cli.fatal("--table is required", .{});
    if (parsed_args.catalog.explicit()) |catalog| {
        var resp = try client.inner.dropNamespaceTable(catalog.database, catalog.namespace, name);
        defer resp.deinit();
        if (resp.data) |parsed| return try cli.writeJson(allocator, io, parsed.value);
        return;
    }
    try client.dropTable(name);
    cli.writeStdout(io, "Drop table command successful.\n");
}

const ListOutput = enum { summary, json };

fn listTablesWithArgs(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    var catalog = cli.CatalogFlags.defaultsFromEnv();
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
        } else if (cli.parseCatalogFlag(&catalog, arg, args)) {
            continue;
        } else {
            cli.fatal("unknown table list option: {s}", .{arg});
        }
    }
    return listTablesWithCatalog(allocator, io, client, catalog, output);
}

fn listTablesWithCatalog(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, catalog: cli.CatalogFlags, output: ListOutput) !void {
    if (catalog.explicit()) |explicit| {
        var resp = try client.inner.listNamespaceTables(explicit.database, explicit.namespace, .{});
        defer resp.deinit();
        if (resp.data) |parsed| try writeTableList(allocator, io, parsed.value, output);
        return;
    }
    var resp = try client.listTables();
    defer resp.deinit();
    if (resp.data) |parsed| try writeTableList(allocator, io, parsed.value, output);
}

fn writeTableList(allocator: std.mem.Allocator, io: std.Io, tables: anytype, output: ListOutput) !void {
    if (output == .json) return cli.writeJson(allocator, io, tables);
    cli.writeStdout(io, "NAME\tSHARDS\tINDEXES\tSTORAGE\n");
    for (tables) |table| {
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

fn getTable(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    var parsed_args = TableArgs{ .catalog = cli.CatalogFlags.defaultsFromEnv() };
    while (args.next()) |arg| {
        parseTableArg(&parsed_args, arg, args);
    }
    const name = parsed_args.table_name orelse cli.fatal("--table is required", .{});
    return getTableByName(allocator, io, client, parsed_args.catalog, name);
}

fn getTableByName(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, catalog: cli.CatalogFlags, name: []const u8) !void {
    if (catalog.explicit()) |explicit| {
        var resp = try client.inner.getNamespaceTable(explicit.database, explicit.namespace, name);
        defer resp.deinit();
        if (resp.data) |parsed| {
            try cli.writeJson(allocator, io, parsed.value);
        }
        return;
    }
    var resp = try client.getTable(name);
    defer resp.deinit();
    if (resp.data) |parsed| {
        try cli.writeJson(allocator, io, parsed.value);
    }
}

const TableArgs = struct {
    table_name: ?[]const u8 = null,
    catalog: cli.CatalogFlags = .{},
};

fn isTableCatalogArg(arg: []const u8) bool {
    return std.mem.eql(u8, arg, "--table") or
        std.mem.eql(u8, arg, "-t") or
        cli.isCatalogFlag(arg);
}

fn parseTableArg(parsed: *TableArgs, arg: []const u8, args: *std.process.Args.Iterator) void {
    if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
        parsed.table_name = args.next() orelse cli.fatal("--table requires a value", .{});
    } else _ = cli.parseCatalogFlag(&parsed.catalog, arg, args);
}
