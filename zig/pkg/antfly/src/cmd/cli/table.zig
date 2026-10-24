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
const ant_json = @import("antfly-json");
const antfly_client = @import("antfly-client");
const httpx = antfly_client.httpx;
const cli = @import("mod.zig");

const TableCreateOption = enum { table, shards, file, index, schema };

fn tableCreateOption(arg: []const u8) ?TableCreateOption {
    if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) return .table;
    if (std.mem.eql(u8, arg, "--shards")) return .shards;
    if (std.mem.eql(u8, arg, "--file") or std.mem.eql(u8, arg, "-f")) return .file;
    if (std.mem.eql(u8, arg, "--index")) return .index;
    if (std.mem.eql(u8, arg, "--schema")) return .schema;
    return null;
}

fn parseInlineSchemaConfig(allocator: std.mem.Allocator, raw: []const u8) !antfly_client.types.TableSchema {
    return std.json.parseFromSliceLeaky(
        antfly_client.types.TableSchema,
        allocator,
        raw,
        .{ .allocate = .alloc_always },
    );
}

const InlineIndexConfig = struct {
    name: []const u8,
    request: antfly_client.types.CreateIndexRequest,
};

fn parseInlineIndexConfig(allocator: std.mem.Allocator, raw: []const u8) !InlineIndexConfig {
    var value = try std.json.parseFromSliceLeaky(std.json.Value, allocator, raw, .{
        .allocate = .alloc_always,
    });
    const object = switch (value) {
        .object => |*object| object,
        else => return error.UnexpectedToken,
    };
    const name_value = object.get("name") orelse return error.MissingField;
    const name = switch (name_value) {
        .string => |string| string,
        else => return error.UnexpectedToken,
    };
    _ = object.swapRemove("name");
    return .{
        .name = name,
        .request = try std.json.parseFromValueLeaky(
            antfly_client.types.CreateIndexRequest,
            allocator,
            value,
            .{},
        ),
    };
}

fn putInlineIndexConfig(
    allocator: std.mem.Allocator,
    indexes: *std.json.ArrayHashMap(antfly_client.types.CreateIndexRequest),
    index: InlineIndexConfig,
) !void {
    if (index.name.len == 0) return error.EmptyIndexName;
    if (indexes.map.contains(index.name)) return error.DuplicateIndexName;
    try indexes.map.put(allocator, index.name, index.request);
}

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

    cli.argumentError("unknown table subcommand: {s}", .{subcommand});
}

fn runWithFlags(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, first_arg: []const u8, args: *std.process.Args.Iterator) !void {
    var table_name: ?[]const u8 = null;
    var output: ?[]const u8 = null;
    var current_arg: ?[]const u8 = first_arg;

    while (current_arg) |arg| : (current_arg = args.next()) {
        if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            if (table_name != null) cli.argumentError("--table may only be provided once", .{});
            table_name = args.next() orelse cli.argumentError("{s} requires a value", .{arg});
        } else if (std.mem.eql(u8, arg, "--output") or std.mem.eql(u8, arg, "-o")) {
            if (output != null) cli.argumentError("--output may only be provided once", .{});
            output = args.next() orelse cli.argumentError("{s} requires a value", .{arg});
        } else {
            cli.argumentError("unknown table option: {s}", .{arg});
        }
    }

    if (output) |value| {
        try cli.output.selectFormat(.json);
        if (!std.mem.eql(u8, value, "json")) {
            cli.argumentError("only JSON output is supported for table", .{});
        }
    }

    if (table_name) |name| {
        return getTableByName(allocator, io, client, name);
    }
    return listTablesMode(allocator, io, client, if (output != null) .json else .summary);
}

pub fn createTable(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    var table_name: ?[]const u8 = null;
    var shards: ?i64 = null;
    var file_path: ?[]const u8 = null;
    var schema_json: ?[]const u8 = null;
    var inline_indexes = std.ArrayListUnmanaged([]const u8).empty;
    defer inline_indexes.deinit(allocator);

    while (args.next()) |arg| {
        const option = tableCreateOption(arg) orelse cli.argumentError("unknown table create option: {s}", .{arg});
        switch (option) {
            .table => {
                if (table_name != null) cli.argumentError("--table may only be provided once", .{});
                table_name = args.next() orelse cli.argumentError("{s} requires a value", .{arg});
            },
            .shards => {
                if (shards != null) cli.argumentError("--shards may only be provided once", .{});
                const raw = args.next() orelse cli.argumentError("--shards requires a value", .{});
                shards = std.fmt.parseInt(i64, raw, 10) catch cli.argumentError("invalid --shards value: {s}", .{raw});
                if (shards.? <= 0) cli.argumentError("--shards must be greater than zero", .{});
            },
            .file => {
                if (file_path != null) cli.argumentError("--file may only be provided once", .{});
                file_path = args.next() orelse cli.argumentError("{s} requires a value", .{arg});
            },
            .index => {
                const raw = args.next() orelse cli.argumentError("--index requires a JSON value", .{});
                inline_indexes.append(allocator, raw) catch |err| cli.fatal("recording --index: {}", .{err});
            },
            .schema => {
                if (schema_json != null) cli.argumentError("--schema may only be provided once", .{});
                schema_json = args.next() orelse cli.argumentError("--schema requires a JSON value", .{});
            },
        }
    }

    const name = table_name orelse cli.argumentError("--table is required", .{});

    var body = antfly_client.types.CreateTableRequest{};
    if (file_path) |path| {
        if (inline_indexes.items.len != 0) {
            cli.argumentError("--index with --file is not supported; put indexes in {s}", .{path});
        }
        if (schema_json != null) {
            cli.argumentError("--schema with --file is not supported; put schema in {s}", .{path});
        }
        const file_data = cli.readFileAlloc(io, allocator, path, 10 * 1024 * 1024) catch |err| {
            cli.fatal("reading config file {s}: {}", .{ path, err });
        };
        defer allocator.free(file_data);
        var parsed = std.json.parseFromSlice(antfly_client.types.CreateTableRequest, allocator, file_data, .{}) catch |err| {
            cli.argumentError("parsing config file {s}: {}", .{ path, err });
        };
        defer parsed.deinit();
        if (shards != null) {
            cli.argumentError("--shards with --file is not supported; put num_shards in {s}", .{path});
        }
        try client.createTable(name, parsed.value);
        try cli.output.acknowledge("create_table");
        if (!cli.isAgent()) cli.output.notice("Create table command successful.\n", .{});
        return;
    }

    if (shards) |s| body.num_shards = s;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    if (schema_json) |raw| {
        body.schema = parseInlineSchemaConfig(arena.allocator(), raw) catch |err| cli.argumentError("invalid JSON for --schema: {}", .{err});
    }
    if (inline_indexes.items.len != 0) {
        var indexes = std.json.ArrayHashMap(antfly_client.types.CreateIndexRequest){};
        for (inline_indexes.items) |raw| {
            const index = parseInlineIndexConfig(arena.allocator(), raw) catch |err| cli.argumentError("invalid JSON for --index: {}", .{err});
            putInlineIndexConfig(arena.allocator(), &indexes, index) catch |err| switch (err) {
                error.EmptyIndexName => cli.argumentError("--index name must not be empty", .{}),
                error.DuplicateIndexName => cli.argumentError("duplicate --index definition: {s}", .{index.name}),
                else => cli.fatal("recording --index {s}: {}", .{ index.name, err }),
            };
        }
        body.indexes = indexes;
    }

    try client.createTable(name, body);
    try cli.output.acknowledge("create_table");
    if (!cli.isAgent()) cli.output.notice("Create table command successful.\n", .{});
}

pub fn dropTable(client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    var table_name: ?[]const u8 = null;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            if (table_name != null) cli.argumentError("--table may only be provided once", .{});
            table_name = args.next() orelse cli.argumentError("{s} requires a value", .{arg});
        } else {
            cli.argumentError("unknown table drop option: {s}", .{arg});
        }
    }
    const name = table_name orelse cli.argumentError("--table is required", .{});
    try client.dropTable(name);
    try cli.output.acknowledge("table");
    if (!cli.isAgent()) cli.output.notice("Drop table command successful.\n", .{});
}

pub fn listTables(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient) !void {
    return listTablesMode(allocator, io, client, .summary);
}

const ListOutput = enum { summary, json };

fn listTablesWithArgs(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    var output: ListOutput = .summary;
    var explicitly_selected = false;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--verbose")) {
            if (explicitly_selected) cli.argumentError("use only one of --verbose or --output json", .{});
            output = .json;
            try cli.output.selectFormat(.json);
            explicitly_selected = true;
        } else if (std.mem.eql(u8, arg, "--output") or std.mem.eql(u8, arg, "-o")) {
            if (explicitly_selected) cli.argumentError("use only one of --verbose or --output json", .{});
            const value = args.next() orelse cli.argumentError("--output requires json", .{});
            if (!std.mem.eql(u8, value, "json")) cli.argumentError("only JSON output is supported for table", .{});
            output = .json;
            try cli.output.selectFormat(.json);
            explicitly_selected = true;
        } else {
            cli.argumentError("unknown table list option: {s}", .{arg});
        }
    }
    return listTablesMode(allocator, io, client, output);
}

fn listTablesMode(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, output: ListOutput) !void {
    cli.output.context = .{ .scope = .discovery, .execution = .complete };
    var resp = try client.listTables();
    defer resp.deinit();
    cli.expectHttpSuccess(resp);
    if (resp.data) |parsed| {
        if (output == .json or cli.useJson()) return cli.writeJson(allocator, io, parsed.value);
        const Summary = struct { name: []const u8, shards: usize, indexes: usize, storage: []const u8 };
        const summaries = try allocator.alloc(Summary, parsed.value.len);
        defer allocator.free(summaries);
        for (parsed.value, summaries) |table, *summary| summary.* = .{
            .name = table.name,
            .shards = table.shards.map.count(),
            .indexes = table.indexes.map.count(),
            .storage = if (table.storage_status.empty orelse false) "empty" else if (table.storage_status.disk_usage != null) "used" else "unknown",
        };
        try cli.writeJson(allocator, io, summaries);
    }
}

pub fn getTable(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    var table_name: ?[]const u8 = null;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            if (table_name != null) cli.argumentError("--table may only be provided once", .{});
            table_name = args.next() orelse cli.argumentError("{s} requires a value", .{arg});
        } else {
            cli.argumentError("unknown table get option: {s}", .{arg});
        }
    }
    const name = table_name orelse cli.argumentError("--table is required", .{});
    return getTableByName(allocator, io, client, name);
}

fn getTableByName(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, name: []const u8) !void {
    cli.output.context = .{ .scope = .discovery, .execution = .complete };
    var resp = try client.getTable(name);
    defer resp.deinit();
    cli.expectHttpSuccess(resp);
    if (resp.data) |parsed| {
        try cli.writeJson(allocator, io, parsed.value);
    }
}

test "table create option classification fails closed" {
    try std.testing.expectEqual(TableCreateOption.index, tableCreateOption("--index").?);
    try std.testing.expectEqual(TableCreateOption.schema, tableCreateOption("--schema").?);
    try std.testing.expect(tableCreateOption("--indx") == null);
}

test "table create inline schema and repeatable indexes build a typed request" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const schema = try parseInlineSchemaConfig(alloc,
        \\{"default_type":"doc","enforce_types":false}
    );
    try std.testing.expectEqualStrings("doc", schema.default_type.?);

    var indexes = std.json.ArrayHashMap(antfly_client.types.CreateIndexRequest){};
    const dense = try parseInlineIndexConfig(alloc,
        \\{"name":"title_body","type":"embeddings","field":"body","dimension":512}
    );
    try putInlineIndexConfig(alloc, &indexes, dense);
    const text = try parseInlineIndexConfig(alloc,
        \\{"name":"body_text","type":"full_text"}
    );
    try putInlineIndexConfig(alloc, &indexes, text);

    const request = antfly_client.types.CreateTableRequest{
        .num_shards = 3,
        .schema = schema,
        .indexes = indexes,
    };
    const encoded = try std.json.Stringify.valueAlloc(std.testing.allocator, request, .{});
    defer std.testing.allocator.free(encoded);
    try ant_json.testing.expectSubsetJsonText(
        std.testing.allocator,
        "{\"num_shards\":3,\"indexes\":{\"title_body\":{\"type\":\"embeddings\",\"dimension\":512},\"body_text\":{\"type\":\"full_text\"}}}",
        encoded,
    );
}

test "table create inline configs reject malformed unknown and duplicate definitions" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    try std.testing.expectError(error.UnexpectedEndOfInput, parseInlineIndexConfig(alloc, "{"));
    try std.testing.expectError(error.UnknownField, parseInlineIndexConfig(alloc,
        \\{"name":"bad","type":"embeddings","typo":true}
    ));

    var indexes = std.json.ArrayHashMap(antfly_client.types.CreateIndexRequest){};
    const first = try parseInlineIndexConfig(alloc,
        \\{"name":"duplicate","type":"full_text"}
    );
    try putInlineIndexConfig(alloc, &indexes, first);
    const second = try parseInlineIndexConfig(alloc,
        \\{"name":"duplicate","type":"embeddings","dimension":3,"external":true}
    );
    try std.testing.expectError(error.DuplicateIndexName, putInlineIndexConfig(alloc, &indexes, second));

    try std.testing.expectError(error.UnknownField, parseInlineSchemaConfig(alloc,
        \\{"unknown_schema_field":true}
    ));
}

test "table create sends the exact quickstart inline index through the HTTP client" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;

    const Assert = struct {
        fn request(info: httpx.testing_mod.RequestInfo) !void {
            try ant_json.testing.expectSubsetJsonText(
                std.testing.allocator,
                "{\"indexes\":{\"title_body\":{\"type\":\"embeddings\",\"template\":\"{{title}} {{body}}\",\"embedder\":{\"model\":\"antflydb/clipclap\"},\"chunker\":{\"text\":{\"target_tokens\":200}}}}}",
                info.body,
            );
            var parsed = try ant_json.parseFromSlice(ant_json.Value, std.testing.allocator, info.body, .{});
            defer parsed.deinit();
            const root = parsed.value.object;
            const indexes = root.get("indexes") orelse return error.MissingIndexes;
            const title_body = indexes.object.get("title_body") orelse return error.MissingTitleBody;
            // The map key owns the index name in the breaking-major table
            // contract. Do not regress to two independently mutable names.
            try std.testing.expect(title_body.object.get("name") == null);
        }
    };

    var server = try httpx.TestServer.start(alloc, io, &.{.{
        .method = .POST,
        .path = "/db/v1/tables/wikipedia",
        .respond = .{ .body = "{\"name\":\"wikipedia\",\"indexes\":{},\"shards\":{}}" },
        .assert_request = Assert.request,
    }});
    defer server.deinit();

    var http = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
    defer http.deinit();
    var client = try antfly_client.AntflyClient.init(alloc, &http, server.baseUrl());
    defer client.deinit();
    var succeeded = std.atomic.Value(bool).init(false);
    var group = std.Io.Group.init;
    const ClientTask = struct {
        fn run(test_io: std.Io, c: *antfly_client.AntflyClient, success: *std.atomic.Value(bool)) std.Io.Cancelable!void {
            var argv = [_][*:0]const u8{
                "--table",
                "wikipedia",
                "--index",
                \\{"name":"title_body","type":"embeddings","template":"{{title}} {{body}}","embedder":{"provider":"antfly","model":"antflydb/clipclap"},"chunker":{"provider":"antfly","text":{"target_tokens":200,"overlap_tokens":25}}}
                ,
            };
            var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
            createTable(std.testing.allocator, test_io, c, &args) catch return;
            success.store(true, .release);
        }
    };
    try group.concurrent(io, ClientTask.run, .{ io, &client, &succeeded });
    try server.handleOne();
    try group.await(io);
    try std.testing.expect(succeeded.load(.acquire));
}
