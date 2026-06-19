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
const antfly = @import("antfly-zig");
const cli = @import("cli/mod.zig");

const Allocator = std.mem.Allocator;
const max_json_file_bytes: usize = 64 * 1024 * 1024;

const db_mod = antfly.db;
const db_types = db_mod.types;
const batch_api = antfly.public_api.batch;
const query_api = antfly.public_api.query;

const LiteDb = struct {
    allocator: Allocator,
    storage: *antfly.lsm_backend.AfliteContainerStorage,
    db: db_mod.DB,

    fn open(allocator: Allocator, path: []const u8) !LiteDb {
        var storage = try allocator.create(antfly.lsm_backend.AfliteContainerStorage);
        errdefer allocator.destroy(storage);

        storage.* = try antfly.lsm_backend.AfliteContainerStorage.open(allocator, path);
        errdefer storage.deinit();

        var opts = db_mod.OpenOptions{};
        pinLiteStorage(&opts, storage.storage());

        const db = db_mod.DB.open(allocator, path, opts) catch |err| {
            storage.deinit();
            allocator.destroy(storage);
            return err;
        };

        return .{
            .allocator = allocator,
            .storage = storage,
            .db = db,
        };
    }

    fn close(self: *LiteDb) void {
        self.db.close();
        self.storage.deinit();
        self.allocator.destroy(self.storage);
        self.* = undefined;
    }
};

const ParsedLookupRequest = struct {
    fields: ?[]const []const u8 = null,
};

const OwnedLookupRequest = struct {
    fields: [][]const u8 = &.{},
    lookup_opts: db_types.LookupOptions = .{},

    fn deinit(self: *OwnedLookupRequest, alloc: Allocator) void {
        freeFieldList(alloc, self.fields);
        self.* = undefined;
    }
};

const ParsedScanRequest = struct {
    from: ?[]const u8 = null,
    to: ?[]const u8 = null,
    inclusive_from: ?bool = null,
    exclusive_to: ?bool = null,
    include_documents: ?bool = null,
    fields: ?[]const []const u8 = null,
    limit: ?u32 = null,
};

const OwnedScanRequest = struct {
    from: []const u8 = "",
    to: []const u8 = "",
    fields: [][]const u8 = &.{},
    scan_opts: db_types.ScanOptions = .{},

    fn deinit(self: *OwnedScanRequest, alloc: Allocator) void {
        if (self.from.len > 0) alloc.free(self.from);
        if (self.to.len > 0) alloc.free(self.to);
        freeFieldList(alloc, self.fields);
        self.* = undefined;
    }
};

pub fn runFromIterator(init: std.process.Init, argv0: []const u8, args: *std.process.Args.Iterator) !void {
    const subcommand = args.next() orelse {
        printUsage(argv0);
        return error.InvalidArguments;
    };

    if (std.mem.eql(u8, subcommand, "--help") or std.mem.eql(u8, subcommand, "-h") or std.mem.eql(u8, subcommand, "help")) {
        printUsage(argv0);
        return;
    }

    if (std.mem.eql(u8, subcommand, "init")) return try initLite(init.gpa, init.io, args);
    if (std.mem.eql(u8, subcommand, "status") or std.mem.eql(u8, subcommand, "info")) return try status(init.gpa, init.io, args);
    if (std.mem.eql(u8, subcommand, "batch")) return try batch(init.gpa, init.io, args);
    if (std.mem.eql(u8, subcommand, "lookup")) return try lookup(init.gpa, init.io, args);
    if (std.mem.eql(u8, subcommand, "scan")) return try scan(init.gpa, init.io, args);
    if (std.mem.eql(u8, subcommand, "query")) return try query(init.gpa, init.io, args);
    if (std.mem.eql(u8, subcommand, "run-until-idle")) return try runUntilIdle(init.gpa, init.io, args);

    std.debug.print("unknown lite subcommand: {s}\n", .{subcommand});
    printUsage(argv0);
    return error.InvalidArguments;
}

fn initLite(allocator: Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    const path = args.next() orelse cli.fatal("database path is required", .{});
    try requireAflitePath(path);

    var lite = try LiteDb.open(allocator, path);
    defer lite.close();

    cli.writeStdout(io, "{\"format\":\"aflite\",\"path\":");
    try writeJsonString(allocator, io, path);
    cli.writeStdout(io, "}\n");
}

fn status(allocator: Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    const path = args.next() orelse cli.fatal("database path is required", .{});
    try requireAflitePath(path);

    var lite = try LiteDb.open(allocator, path);
    defer lite.close();

    const stats_value = try lite.db.stats(allocator);
    defer db_types.freeDBStats(allocator, stats_value);
    const json = try std.json.Stringify.valueAlloc(allocator, stats_value, .{});
    defer allocator.free(json);
    writeJsonLine(io, json);
}

fn batch(allocator: Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    const path = args.next() orelse cli.fatal("database path is required", .{});
    try requireAflitePath(path);
    const file_path = parseFileFlag(args);

    const body = try cli.readFileAlloc(io, allocator, file_path, max_json_file_bytes);
    defer allocator.free(body);

    var lite = try LiteDb.open(allocator, path);
    defer lite.close();

    const json = try batchJson(allocator, &lite.db, body);
    defer allocator.free(json);
    writeJsonLine(io, json);
}

fn lookup(allocator: Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    const path = args.next() orelse cli.fatal("database path is required", .{});
    try requireAflitePath(path);

    var key: ?[]const u8 = null;
    var file_path: ?[]const u8 = null;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--key") or std.mem.eql(u8, arg, "-k")) {
            key = args.next();
        } else if (std.mem.eql(u8, arg, "--file") or std.mem.eql(u8, arg, "-f")) {
            file_path = args.next();
        } else if (!std.mem.startsWith(u8, arg, "-") and key == null) {
            key = arg;
        } else {
            cli.fatal("unknown lookup argument: {s}", .{arg});
        }
    }

    const resolved_key = key orelse cli.fatal("--key is required", .{});
    const body = if (file_path) |request_path|
        try cli.readFileAlloc(io, allocator, request_path, max_json_file_bytes)
    else
        try allocator.dupe(u8, "");
    defer allocator.free(body);

    var lite = try LiteDb.open(allocator, path);
    defer lite.close();

    const json = try lookupJson(allocator, &lite.db, resolved_key, body);
    defer allocator.free(json);
    writeJsonLine(io, json);
}

fn scan(allocator: Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    const path = args.next() orelse cli.fatal("database path is required", .{});
    try requireAflitePath(path);
    const file_path = parseFileFlag(args);

    const body = try cli.readFileAlloc(io, allocator, file_path, max_json_file_bytes);
    defer allocator.free(body);

    var lite = try LiteDb.open(allocator, path);
    defer lite.close();

    const json = try scanJson(allocator, &lite.db, body);
    defer allocator.free(json);
    writeJsonLine(io, json);
}

fn query(allocator: Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    const path = args.next() orelse cli.fatal("database path is required", .{});
    try requireAflitePath(path);
    const file_path = parseFileFlag(args);

    const body = try cli.readFileAlloc(io, allocator, file_path, max_json_file_bytes);
    defer allocator.free(body);

    var lite = try LiteDb.open(allocator, path);
    defer lite.close();

    const json = try searchJson(allocator, &lite.db, body);
    defer allocator.free(json);
    writeJsonLine(io, json);
}

fn runUntilIdle(allocator: Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    const path = args.next() orelse cli.fatal("database path is required", .{});
    try requireAflitePath(path);

    var lite = try LiteDb.open(allocator, path);
    defer lite.close();

    try lite.db.maintenanceDriver().runUntilIdle();
    const json = try std.json.Stringify.valueAlloc(allocator, lite.db.maintenanceDriver().pendingWorkStats(), .{});
    defer allocator.free(json);
    writeJsonLine(io, json);
}

fn pinLiteStorage(opts: *db_mod.OpenOptions, storage: antfly.lsm_backend.Storage) void {
    opts.storage = storage;
    opts.index_backends.text_lsm_storage = storage;
    opts.index_backends.dense_lsm_storage = storage;
    opts.index_backends.sparse_lsm_storage = storage;
    opts.index_backends.graph_lsm_storage = storage;
}

fn batchJson(allocator: Allocator, db: *db_mod.DB, body: []const u8) ![]u8 {
    var owned = try batch_api.parseBatchRequest(allocator, body);
    defer owned.deinit(allocator);

    try db.batch(owned.req);
    return try batch_api.encodeBatchResponse(allocator, owned.result());
}

fn lookupJson(allocator: Allocator, db: *db_mod.DB, key: []const u8, body: []const u8) ![]u8 {
    var opts = try parseLookupRequest(allocator, body);
    defer opts.deinit(allocator);

    var result = (try db.lookup(allocator, key, opts.lookup_opts)) orelse {
        return try allocator.dupe(u8, "{\"found\":false}");
    };
    defer result.deinit(allocator);

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(allocator);

    try out.appendSlice(allocator, "{\"found\":true,\"_id\":");
    try appendJsonString(allocator, &out, key);
    try out.appendSlice(allocator, ",\"_source\":");
    try out.appendSlice(allocator, result.json);
    try out.append(allocator, '}');
    return try out.toOwnedSlice(allocator);
}

fn scanJson(allocator: Allocator, db: *db_mod.DB, body: []const u8) ![]u8 {
    var req = try parseScanRequest(allocator, body);
    defer req.deinit(allocator);

    var result = try db.scan(allocator, req.from, req.to, req.scan_opts);
    defer result.deinit(allocator);

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(allocator);

    try out.appendSlice(allocator, "{\"hashes\":[");
    for (result.hashes, 0..) |entry, i| {
        if (i > 0) try out.append(allocator, ',');
        try out.appendSlice(allocator, "{\"_id\":");
        try appendJsonString(allocator, &out, entry.id);
        try out.appendSlice(allocator, ",\"hash\":");
        var hash_buf: [32]u8 = undefined;
        const rendered = try std.fmt.bufPrint(&hash_buf, "{d}", .{entry.hash});
        try out.appendSlice(allocator, rendered);
        try out.append(allocator, '}');
    }
    try out.appendSlice(allocator, "],\"documents\":[");
    for (result.documents, 0..) |doc, i| {
        if (i > 0) try out.append(allocator, ',');
        try out.appendSlice(allocator, "{\"_id\":");
        try appendJsonString(allocator, &out, doc.id);
        try out.appendSlice(allocator, ",\"_source\":");
        try out.appendSlice(allocator, doc.json);
        try out.append(allocator, '}');
    }
    try out.appendSlice(allocator, "]}");
    return try out.toOwnedSlice(allocator);
}

fn searchJson(allocator: Allocator, db: *db_mod.DB, body: []const u8) ![]u8 {
    var owned = try query_api.parsePublicQueryRequest(
        allocator,
        null,
        "docs",
        body,
    );
    defer owned.deinit(allocator);

    var result = try db.search(allocator, owned.req);
    defer result.deinit();

    var response = try query_api.encodeQueryResponses(
        allocator,
        "docs",
        owned.req,
        .{},
        result,
    );
    defer response.deinit(allocator);
    return try allocator.dupe(u8, response.json);
}

fn parseLookupRequest(alloc: Allocator, body: []const u8) !OwnedLookupRequest {
    if (body.len == 0) return .{};

    var parsed = try std.json.parseFromSlice(ParsedLookupRequest, alloc, body, .{
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();

    const fields: [][]const u8 = if (parsed.value.fields) |raw_fields|
        try cloneFieldList(alloc, raw_fields)
    else
        @constCast((&[_][]const u8{})[0..]);
    errdefer freeFieldList(alloc, fields);

    return .{
        .fields = fields,
        .lookup_opts = .{
            .fields = fields,
            .include_all_fields = fields.len == 0,
        },
    };
}

fn parseScanRequest(alloc: Allocator, body: []const u8) !OwnedScanRequest {
    if (body.len == 0) return .{};

    var parsed = try std.json.parseFromSlice(ParsedScanRequest, alloc, body, .{
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();

    const fields: [][]const u8 = if (parsed.value.fields) |raw_fields|
        try cloneFieldList(alloc, raw_fields)
    else
        @constCast((&[_][]const u8{})[0..]);
    errdefer freeFieldList(alloc, fields);

    const from = if (parsed.value.from) |value| try alloc.dupe(u8, value) else "";
    errdefer if (from.len > 0) alloc.free(from);
    const to = if (parsed.value.to) |value| try alloc.dupe(u8, value) else "";
    errdefer if (to.len > 0) alloc.free(to);

    return .{
        .from = from,
        .to = to,
        .fields = fields,
        .scan_opts = .{
            .inclusive_from = parsed.value.inclusive_from orelse false,
            .exclusive_to = parsed.value.exclusive_to orelse false,
            .include_documents = parsed.value.include_documents orelse false,
            .limit = parsed.value.limit orelse 0,
            .fields = fields,
            .include_all_fields = fields.len == 0,
        },
    };
}

fn cloneFieldList(alloc: Allocator, raw_fields: []const []const u8) ![][]const u8 {
    const fields = try alloc.alloc([]const u8, raw_fields.len);
    var initialized: usize = 0;
    errdefer {
        for (fields[0..initialized]) |field| alloc.free(field);
        alloc.free(fields);
    }
    for (raw_fields, 0..) |field, i| {
        fields[i] = try alloc.dupe(u8, field);
        initialized += 1;
    }
    return fields;
}

fn freeFieldList(alloc: Allocator, fields: [][]const u8) void {
    for (fields) |field| alloc.free(field);
    if (fields.len > 0) alloc.free(fields);
}

fn appendJsonString(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    value: []const u8,
) !void {
    const escaped = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
    defer alloc.free(escaped);
    try out.appendSlice(alloc, escaped);
}

fn parseFileFlag(args: *std.process.Args.Iterator) []const u8 {
    var file_path: ?[]const u8 = null;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--file") or std.mem.eql(u8, arg, "-f")) {
            file_path = args.next();
        } else {
            cli.fatal("unknown argument: {s}", .{arg});
        }
    }
    return file_path orelse cli.fatal("--file is required", .{});
}

fn requireAflitePath(path: []const u8) !void {
    if (!std.mem.endsWith(u8, path, ".aflite")) {
        std.debug.print("error: Antfly Lite database paths must end in .aflite: {s}\n", .{path});
        return error.InvalidArguments;
    }
}

fn writeJsonLine(io: std.Io, json: []const u8) void {
    cli.writeStdout(io, json);
    cli.writeStdout(io, "\n");
}

fn writeJsonString(allocator: Allocator, io: std.Io, value: []const u8) !void {
    const json = try std.fmt.allocPrint(allocator, "{f}", .{std.json.fmt(value, .{})});
    defer allocator.free(json);
    cli.writeStdout(io, json);
}

fn printUsage(argv0: []const u8) void {
    std.debug.print(
        \\usage: {s} lite <subcommand> [options]
        \\
        \\subcommands:
        \\  init <db.aflite>
        \\  status <db.aflite>
        \\  batch <db.aflite> --file request.json
        \\  lookup <db.aflite> --key <key> [--file request.json]
        \\  scan <db.aflite> --file request.json
        \\  query <db.aflite> --file request.json
        \\  run-until-idle <db.aflite>
        \\
    , .{argv0});
}

test "lite path validation requires aflite extension" {
    try requireAflitePath("app.aflite");
    try std.testing.expectError(error.InvalidArguments, requireAflitePath("app.afl"));
}
