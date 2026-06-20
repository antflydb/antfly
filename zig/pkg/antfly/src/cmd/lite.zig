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
const httpx = @import("httpx");
const lite_restore_staging = @import("lite_restore_staging.zig");
const fs_paths = antfly.common.fs_paths;

const Allocator = std.mem.Allocator;
const max_json_file_bytes: usize = 64 * 1024 * 1024;

const db_mod = antfly.db;
const db_types = db_mod.types;
const batch_api = antfly.public_api.batch;
const query_api = antfly.public_api.query;
const portable_backup = antfly.portable_backup;

const LiteDb = struct {
    backend: antfly.lite.backend.Handle,
    db: db_mod.DB,

    fn open(allocator: Allocator, path: []const u8, open_mode: db_mod.OpenOptions.OpenMode) !LiteDb {
        var backend = try antfly.lite.backend.Handle.open(allocator, path, .{
            .read_only = openModeRequiresReadOnlyBackends(open_mode),
        });
        errdefer backend.deinit();

        var opts = db_mod.OpenOptions{
            .open_mode = open_mode,
            .external_derived_checkpoints = false,
        };
        try backend.configureDbOpenOptions(&opts);

        const db = db_mod.DB.open(allocator, path, opts) catch |err| {
            return err;
        };

        return .{
            .backend = backend,
            .db = db,
        };
    }

    fn close(self: *LiteDb) void {
        self.db.close();
        self.backend.deinit();
        self.* = undefined;
    }
};

const ParsedLookupRequest = struct {
    fields: ?[]const []const u8 = null,
};

const max_afb_file_bytes: usize = 16 * 1024 * 1024 * 1024;

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
    if (isStatusSubcommand(subcommand)) return try status(init.gpa, init.io, args);
    if (std.mem.eql(u8, subcommand, "batch")) return try batch(init.gpa, init.io, args);
    if (std.mem.eql(u8, subcommand, "lookup")) return try lookup(init.gpa, init.io, args);
    if (std.mem.eql(u8, subcommand, "scan")) return try scan(init.gpa, init.io, args);
    if (std.mem.eql(u8, subcommand, "query")) return try query(init.gpa, init.io, args);
    if (std.mem.eql(u8, subcommand, "index")) return try indexCommand(init.gpa, init.io, args);
    if (std.mem.eql(u8, subcommand, "enrichment")) return try enrichmentCommand(init.gpa, init.io, args);
    if (std.mem.eql(u8, subcommand, "schema")) return try schemaCommand(init.gpa, init.io, args);
    if (std.mem.eql(u8, subcommand, "run-until-idle")) return try runUntilIdle(init.gpa, init.io, args);
    if (std.mem.eql(u8, subcommand, "backup") or std.mem.eql(u8, subcommand, "export")) return try backup(init.gpa, init.io, args);
    if (std.mem.eql(u8, subcommand, "snapshot")) return try snapshot(init.gpa, init.io, args);
    if (std.mem.eql(u8, subcommand, "restore")) return try restore(init.gpa, init.io, args);
    if (std.mem.eql(u8, subcommand, "import")) return try importBackup(init.gpa, init.io, args);
    if (std.mem.eql(u8, subcommand, "promote")) return try promote(init.gpa, init.io, args);
    if (std.mem.eql(u8, subcommand, "check")) return try check(init.gpa, init.io, args);
    if (std.mem.eql(u8, subcommand, "compact") or std.mem.eql(u8, subcommand, "vacuum")) return try vacuum(init.gpa, init.io, args);
    if (std.mem.eql(u8, subcommand, "serve")) return try serve(init.gpa, init.io, args);

    std.debug.print("unknown lite subcommand: {s}\n", .{subcommand});
    printUsage(argv0);
    return error.InvalidArguments;
}

fn isStatusSubcommand(subcommand: []const u8) bool {
    return std.mem.eql(u8, subcommand, "status") or std.mem.eql(u8, subcommand, "info");
}

fn initLite(allocator: Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    const path = args.next() orelse cli.fatal("database path is required", .{});
    try requireAflitePath(path);
    requireNoMoreArgs(args);
    if (initTargetExists(io, path)) {
        cli.fatal("database already exists: {s}", .{path});
    }

    var lite = try LiteDb.open(allocator, path, .writer);
    defer lite.close();

    cli.writeStdout(io, "{\"format\":\"aflite\",\"path\":");
    try writeJsonString(allocator, io, path);
    cli.writeStdout(io, "}\n");
}

fn status(allocator: Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    const path = args.next() orelse cli.fatal("database path is required", .{});
    try requireAflitePath(path);

    var lite = try LiteDb.open(allocator, path, .status_only);
    defer lite.close();

    const json = try statusJson(allocator, &lite, .native);
    defer allocator.free(json);
    writeJsonLine(io, json);
}

fn batch(allocator: Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    const path = args.next() orelse cli.fatal("database path is required", .{});
    try requireAflitePath(path);
    const file_path = parseFileFlag(args);

    const body = try cli.readFileAlloc(io, allocator, file_path, max_json_file_bytes);
    defer allocator.free(body);

    var lite = try LiteDb.open(allocator, path, .writer);
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
        } else if (std.mem.eql(u8, arg, "--readonly")) {
            // Lookup always uses a query-readonly handle; accept the flag so
            // scripts can spell the mode explicitly.
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

    var lite = try LiteDb.open(allocator, path, .query_readonly);
    defer lite.close();

    const json = try lookupJson(allocator, &lite.db, resolved_key, body);
    defer allocator.free(json);
    writeJsonLine(io, json);
}

fn scan(allocator: Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    const path = args.next() orelse cli.fatal("database path is required", .{});
    try requireAflitePath(path);
    const file_path = parseReadFileFlag(args);

    const body = try cli.readFileAlloc(io, allocator, file_path, max_json_file_bytes);
    defer allocator.free(body);

    var lite = try LiteDb.open(allocator, path, .query_readonly);
    defer lite.close();

    const json = try scanJson(allocator, &lite.db, body);
    defer allocator.free(json);
    writeJsonLine(io, json);
}

fn query(allocator: Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    const path = args.next() orelse cli.fatal("database path is required", .{});
    try requireAflitePath(path);
    const file_path = parseReadFileFlag(args);

    const body = try cli.readFileAlloc(io, allocator, file_path, max_json_file_bytes);
    defer allocator.free(body);

    var lite = try LiteDb.open(allocator, path, .query_readonly);
    defer lite.close();

    const json = try searchJson(allocator, &lite.db, body);
    defer allocator.free(json);
    writeJsonLine(io, json);
}

fn indexCommand(allocator: Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    const action = args.next() orelse cli.fatal("index subcommand is required", .{});
    if (std.mem.eql(u8, action, "list")) return try indexList(allocator, io, args);
    if (std.mem.eql(u8, action, "create")) return try indexCreate(allocator, io, args);
    if (std.mem.eql(u8, action, "drop")) return try indexDrop(allocator, io, args);
    cli.fatal("unknown index subcommand: {s}", .{action});
}

fn indexList(allocator: Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    const path = args.next() orelse cli.fatal("database path is required", .{});
    try requireAflitePath(path);
    requireNoMoreArgs(args);

    var lite = try LiteDb.open(allocator, path, .status_only);
    defer lite.close();

    const configs = try lite.db.listIndexes(allocator);
    defer db_types.freeIndexConfigs(allocator, configs);
    const json = try std.json.Stringify.valueAlloc(allocator, configs, .{});
    defer allocator.free(json);
    writeJsonLine(io, json);
}

fn indexCreate(allocator: Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    const path = args.next() orelse cli.fatal("database path is required", .{});
    try requireAflitePath(path);
    const file_path = parseFileFlag(args);

    const body = try cli.readFileAlloc(io, allocator, file_path, max_json_file_bytes);
    defer allocator.free(body);

    var parsed = try std.json.parseFromSlice(db_types.IndexConfig, allocator, body, .{
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();

    var lite = try LiteDb.open(allocator, path, .writer);
    defer lite.close();

    try lite.db.addIndex(parsed.value);
    const json = try mutationJson(allocator, "created", parsed.value.name, true);
    defer allocator.free(json);
    writeJsonLine(io, json);
}

fn indexDrop(allocator: Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    const path = args.next() orelse cli.fatal("database path is required", .{});
    try requireAflitePath(path);
    const name = parseNameFlag(args, "--index");

    var lite = try LiteDb.open(allocator, path, .writer);
    defer lite.close();

    const removed = try lite.db.deleteIndex(name);
    const json = try mutationJson(allocator, "removed", name, removed);
    defer allocator.free(json);
    writeJsonLine(io, json);
}

fn enrichmentCommand(allocator: Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    const action = args.next() orelse cli.fatal("enrichment subcommand is required", .{});
    if (std.mem.eql(u8, action, "list")) return try enrichmentList(allocator, io, args);
    if (std.mem.eql(u8, action, "create")) return try enrichmentCreate(allocator, io, args);
    if (std.mem.eql(u8, action, "drop")) return try enrichmentDrop(allocator, io, args);
    cli.fatal("unknown enrichment subcommand: {s}", .{action});
}

fn enrichmentList(allocator: Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    const path = args.next() orelse cli.fatal("database path is required", .{});
    try requireAflitePath(path);
    requireNoMoreArgs(args);

    var lite = try LiteDb.open(allocator, path, .status_only);
    defer lite.close();

    const configs = try lite.db.listEnrichments(allocator);
    defer db_types.freeEnrichmentConfigs(allocator, configs);
    const json = try std.json.Stringify.valueAlloc(allocator, configs, .{});
    defer allocator.free(json);
    writeJsonLine(io, json);
}

fn enrichmentCreate(allocator: Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    const path = args.next() orelse cli.fatal("database path is required", .{});
    try requireAflitePath(path);
    const file_path = parseFileFlag(args);

    const body = try cli.readFileAlloc(io, allocator, file_path, max_json_file_bytes);
    defer allocator.free(body);

    var parsed = try std.json.parseFromSlice(db_types.EnrichmentConfig, allocator, body, .{
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();

    var lite = try LiteDb.open(allocator, path, .writer);
    defer lite.close();

    try lite.db.addEnrichment(parsed.value);
    const json = try mutationJson(allocator, "created", parsed.value.name, true);
    defer allocator.free(json);
    writeJsonLine(io, json);
}

fn enrichmentDrop(allocator: Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    const path = args.next() orelse cli.fatal("database path is required", .{});
    try requireAflitePath(path);

    var kind: ?db_types.EnrichmentKind = null;
    var name: ?[]const u8 = null;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--kind")) {
            kind = parseEnrichmentKind(args.next() orelse cli.fatal("--kind requires a value", .{}));
        } else if (std.mem.eql(u8, arg, "--name")) {
            name = args.next();
        } else {
            cli.fatal("unknown enrichment drop argument: {s}", .{arg});
        }
    }

    const resolved_kind = kind orelse cli.fatal("--kind is required", .{});
    const resolved_name = name orelse cli.fatal("--name is required", .{});

    var lite = try LiteDb.open(allocator, path, .writer);
    defer lite.close();

    const removed = try lite.db.deleteEnrichment(resolved_kind, resolved_name);
    const json = try mutationJson(allocator, "removed", resolved_name, removed);
    defer allocator.free(json);
    writeJsonLine(io, json);
}

fn schemaCommand(allocator: Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    const action = args.next() orelse cli.fatal("schema subcommand is required", .{});
    if (std.mem.eql(u8, action, "get")) return try schemaGet(allocator, io, args);
    if (std.mem.eql(u8, action, "set")) return try schemaSet(allocator, io, args);
    cli.fatal("unknown schema subcommand: {s}", .{action});
}

fn schemaGet(allocator: Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    const path = args.next() orelse cli.fatal("database path is required", .{});
    try requireAflitePath(path);
    requireNoMoreArgs(args);

    var lite = try LiteDb.open(allocator, path, .status_only);
    defer lite.close();

    const schema_json = try lite.db.getSchemaJson(allocator);
    if (schema_json) |json| {
        defer allocator.free(json);
        writeJsonLine(io, json);
    } else {
        writeJsonLine(io, "null");
    }
}

fn schemaSet(allocator: Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    const path = args.next() orelse cli.fatal("database path is required", .{});
    try requireAflitePath(path);
    const file_path = parseFileFlag(args);

    const body = try cli.readFileAlloc(io, allocator, file_path, max_json_file_bytes);
    defer allocator.free(body);

    var lite = try LiteDb.open(allocator, path, .writer);
    defer lite.close();

    try lite.db.setSchemaJson(allocator, body);
    writeJsonLine(io, "{\"updated\":true}");
}

fn runUntilIdle(allocator: Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    const path = args.next() orelse cli.fatal("database path is required", .{});
    try requireAflitePath(path);

    var lite = try LiteDb.open(allocator, path, .writer);
    defer lite.close();

    try lite.db.maintenanceDriver().runUntilIdle();
    const json = try std.json.Stringify.valueAlloc(allocator, lite.db.maintenanceDriver().pendingWorkStats(), .{});
    defer allocator.free(json);
    writeJsonLine(io, json);
}

fn backup(allocator: Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    const path = args.next() orelse cli.fatal("database path is required", .{});
    try requireAflitePath(path);
    const out_path = parseOutFlag(args);
    try requireAfbPath(out_path);

    var lite = try LiteDb.open(allocator, path, .query_readonly);
    defer lite.close();

    var out = std.ArrayList(u8).empty;
    defer out.deinit(allocator);
    try portable_backup.exportPortable(allocator, lite.db.core.store, &out);
    try writeFileAtomically(allocator, io, out_path, out.items);

    cli.writeStdout(io, "{\"format\":\"afb\",\"path\":");
    try writeJsonString(allocator, io, out_path);
    cli.writeStdout(io, "}\n");
}

const SnapshotOptions = struct {
    out_path: []const u8,
    replace: bool = false,
};

fn snapshot(allocator: Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    const path = args.next() orelse cli.fatal("database path is required", .{});
    try requireAflitePath(path);
    const opts = parseSnapshotOptions(args);
    try snapshotStableAflite(allocator, io, path, opts.out_path, opts.replace);
}

fn parseSnapshotOptions(args: *std.process.Args.Iterator) SnapshotOptions {
    var out_path: ?[]const u8 = null;
    var replace = false;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--out") or std.mem.eql(u8, arg, "-o")) {
            out_path = args.next();
        } else if (std.mem.eql(u8, arg, "--replace")) {
            replace = true;
        } else {
            cli.fatal("unknown snapshot argument: {s}", .{arg});
        }
    }
    return .{
        .out_path = out_path orelse cli.fatal("--out is required", .{}),
        .replace = replace,
    };
}

fn snapshotStableAflite(allocator: Allocator, io: std.Io, path: []const u8, out_path: []const u8, replace: bool) !void {
    try requireAflitePath(path);
    try requireAflitePath(out_path);
    if (std.mem.eql(u8, path, out_path)) {
        cli.fatal("source and output snapshot paths must be different: {s}", .{path});
    }
    if (pathExists(io, out_path) and !replace) {
        cli.fatal("output snapshot already exists; pass --replace to overwrite: {s}", .{out_path});
    }

    var backend = try antfly.lite.backend.Handle.open(allocator, path, .{ .read_only = true });
    defer backend.deinit();

    const report = try backend.copyStableSnapshot(out_path, replace);
    const json = try std.json.Stringify.valueAlloc(allocator, report, .{});
    defer allocator.free(json);
    writeJsonLine(io, json);
}

fn restore(allocator: Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    const source_path = args.next() orelse cli.fatal("backup path is required", .{});
    try requireRestoreSourcePath(source_path);
    var out_path: ?[]const u8 = null;
    var replace = false;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--out") or std.mem.eql(u8, arg, "-o")) {
            out_path = args.next();
        } else if (std.mem.eql(u8, arg, "--replace")) {
            replace = true;
        } else {
            cli.fatal("unknown restore argument: {s}", .{arg});
        }
    }
    const resolved_out = out_path orelse cli.fatal("--out is required", .{});
    try restoreFromSourceFile(allocator, io, source_path, resolved_out, replace);
}

fn importBackup(allocator: Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    const path = args.next() orelse cli.fatal("database path is required", .{});
    try requireAflitePath(path);
    var from_path: ?[]const u8 = null;
    var replace = false;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--from")) {
            from_path = args.next();
        } else if (std.mem.eql(u8, arg, "--replace")) {
            replace = true;
        } else {
            cli.fatal("unknown import argument: {s}", .{arg});
        }
    }
    const resolved_from = from_path orelse cli.fatal("--from is required", .{});
    try restoreFromSourceFile(allocator, io, resolved_from, path, replace);
}

fn restoreFromSourceFile(
    allocator: Allocator,
    io: std.Io,
    source_path: []const u8,
    out_path: []const u8,
    replace: bool,
) !void {
    try requireRestoreSourcePath(source_path);
    try requireAflitePath(out_path);
    if (std.mem.eql(u8, source_path, out_path)) {
        cli.fatal("source and output database paths must be different: {s}", .{source_path});
    }

    const body = try readPortableRestoreSourceAlloc(allocator, io, source_path);
    defer allocator.free(body);

    if (pathExists(io, out_path)) {
        if (!replace) cli.fatal("output database already exists; pass --replace to overwrite: {s}", .{out_path});
        try deleteFilePath(io, out_path);
    }

    var lite = try LiteDb.open(allocator, out_path, .writer);
    defer lite.close();

    try portable_backup.importPortable(allocator, lite.db.core.store, body);

    cli.writeStdout(io, "{\"format\":\"aflite\",\"path\":");
    try writeJsonString(allocator, io, out_path);
    cli.writeStdout(io, "}\n");
}

fn readPortableRestoreSourceAlloc(allocator: Allocator, io: std.Io, source_path: []const u8) ![]u8 {
    if (std.mem.endsWith(u8, source_path, ".afb")) {
        return try cli.readFileAlloc(io, allocator, source_path, max_afb_file_bytes);
    }
    if (std.mem.endsWith(u8, source_path, ".aflite")) {
        var source = try LiteDb.open(allocator, source_path, .query_readonly);
        defer source.close();

        var out = std.ArrayList(u8).empty;
        errdefer out.deinit(allocator);
        try portable_backup.exportPortable(allocator, source.db.core.store, &out);
        return try out.toOwnedSlice(allocator);
    }
    return error.InvalidArguments;
}

const PromoteOptions = struct {
    target: []const u8,
    table: []const u8,
    backup_id: []const u8,
    location: []const u8,
};

fn promote(allocator: Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    const path = args.next() orelse cli.fatal("database path is required", .{});
    try requireAflitePath(path);

    const opts = try parsePromoteOptions(allocator, path, args);
    defer allocator.free(opts.backup_id);

    var staged = try lite_restore_staging.stageAfliteRestoreBackup(allocator, path, opts.table, opts.backup_id, opts.location);
    defer staged.deinit(allocator);

    const global_config = cli.parseGlobalFlags();
    var http = httpx.Client.initWithConfig(allocator, io, .{});
    defer http.deinit();
    var client = try cli.initClient(allocator, &http, global_config);
    defer client.deinit();
    try client.setBaseUrl(opts.target);
    try client.restoreTable(opts.table, .{
        .backup_id = staged.backup_id,
        .location = staged.location,
        .format = "portable",
    });

    cli.writeStdout(io, "{\"promoted\":true,\"table\":");
    try writeJsonString(allocator, io, opts.table);
    cli.writeStdout(io, ",\"target\":");
    try writeJsonString(allocator, io, opts.target);
    cli.writeStdout(io, ",\"backup_id\":");
    try writeJsonString(allocator, io, staged.backup_id);
    cli.writeStdout(io, ",\"location\":");
    try writeJsonString(allocator, io, staged.location);
    cli.writeStdout(io, "}\n");
}

fn parsePromoteOptions(allocator: Allocator, path: []const u8, args: *std.process.Args.Iterator) !PromoteOptions {
    var target: ?[]const u8 = null;
    var table: ?[]const u8 = null;
    var backup_id: ?[]const u8 = null;
    var location: []const u8 = "file:///tmp/antfly_backups";
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--target") or std.mem.eql(u8, arg, "--url")) {
            target = args.next();
        } else if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            table = args.next();
        } else if (std.mem.eql(u8, arg, "--backup-id")) {
            backup_id = args.next();
        } else if (std.mem.eql(u8, arg, "--location")) {
            location = args.next() orelse location;
        } else {
            cli.fatal("unknown promote argument: {s}", .{arg});
        }
    }
    const resolved_target = target orelse cli.fatal("--target is required", .{});
    const resolved_table = table orelse cli.fatal("--table is required", .{});
    return .{
        .target = resolved_target,
        .table = resolved_table,
        .backup_id = if (backup_id) |id| try allocator.dupe(u8, id) else try lite_restore_staging.defaultBackupIdAlloc(allocator, path),
        .location = location,
    };
}

fn check(allocator: Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    const path = args.next() orelse cli.fatal("database path is required", .{});
    try requireAflitePath(path);
    requireNoMoreArgs(args);

    const report = try antfly.lite.backend.checkFile(allocator, path);
    const json = try std.json.Stringify.valueAlloc(allocator, report, .{});
    defer allocator.free(json);
    writeJsonLine(io, json);
}

fn vacuum(allocator: Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    const path = args.next() orelse cli.fatal("database path is required", .{});
    try requireAflitePath(path);
    requireNoMoreArgs(args);

    var lite = try LiteDb.open(allocator, path, .writer);
    defer lite.close();

    const report = try lite.backend.vacuum();
    const json = try std.json.Stringify.valueAlloc(allocator, report, .{});
    defer allocator.free(json);
    writeJsonLine(io, json);
}

const ServeOptions = struct {
    path: []const u8,
    addr: []const u8 = "127.0.0.1:8080",
};

fn serve(_: Allocator, _: std.Io, args: *std.process.Args.Iterator) !void {
    const opts = try parseServeOptions(args);
    try requireAflitePath(opts.path);
    std.debug.print(
        "error: antfly lite serve is not included in this build; use a lite-dev build with HTTP serve support\n",
        .{},
    );
    return error.UnsupportedLiteServe;
}

fn parseServeOptions(args: *std.process.Args.Iterator) !ServeOptions {
    const path = args.next() orelse {
        std.debug.print("error: database path is required\n", .{});
        return error.InvalidArguments;
    };
    var opts: ServeOptions = .{ .path = path };
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--addr")) {
            opts.addr = args.next() orelse {
                std.debug.print("error: --addr value is required\n", .{});
                return error.InvalidArguments;
            };
        } else {
            std.debug.print("error: unknown serve argument: {s}\n", .{arg});
            return error.InvalidArguments;
        }
    }
    return opts;
}

fn openModeRequiresReadOnlyBackends(open_mode: db_mod.OpenOptions.OpenMode) bool {
    return open_mode == .query_readonly or open_mode == .status_only;
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

const StorageStatus = struct {
    format: []const u8 = "aflite",
    engine: []const u8,
    format_version: ?u32 = null,
    page_size: ?u32 = null,
    active_checkpoint: ?u8 = null,
    checkpoint_sequence: ?u64 = null,
    page_count: ?u64 = null,
};

fn storageStatus(lite: *LiteDb) StorageStatus {
    return switch (lite.backend.engine) {
        .native_single_file => blk: {
            const file = &lite.backend.native_docstore.?.file;
            const checkpoint = file.activeCheckpoint();
            break :blk .{
                .engine = @tagName(lite.backend.engine),
                .format_version = antfly.lite.backend.native.format_version,
                .page_size = file.header.page_size,
                .active_checkpoint = file.header.active_checkpoint,
                .checkpoint_sequence = checkpoint.commit_sequence,
                .page_count = checkpoint.page_count,
            };
        },
        .bridge_lsm_container => .{
            .format = "aflite-internal",
            .engine = @tagName(lite.backend.engine),
        },
    };
}

fn statusJson(allocator: Allocator, lite: *LiteDb, profile: antfly.lite.backend.Profile) ![]u8 {
    const stats_value = try lite.db.stats(allocator);
    defer db_types.freeDBStats(allocator, stats_value);

    const storage_json = try std.json.Stringify.valueAlloc(allocator, storageStatus(lite), .{});
    defer allocator.free(storage_json);
    const stats_json = try std.json.Stringify.valueAlloc(allocator, stats_value, .{});
    defer allocator.free(stats_json);
    const pending_json = try std.json.Stringify.valueAlloc(allocator, lite.db.maintenanceDriver().pendingWorkStats(), .{});
    defer allocator.free(pending_json);
    const capabilities_json = try std.json.Stringify.valueAlloc(allocator, antfly.lite.backend.capabilitiesForProfile(profile), .{});
    defer allocator.free(capabilities_json);

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(allocator);
    try out.appendSlice(allocator, "{\"storage\":");
    try out.appendSlice(allocator, storage_json);
    try out.appendSlice(allocator, ",\"stats\":");
    try out.appendSlice(allocator, stats_json);
    try out.appendSlice(allocator, ",\"pending_work\":");
    try out.appendSlice(allocator, pending_json);
    try out.appendSlice(allocator, ",\"capabilities\":");
    try out.appendSlice(allocator, capabilities_json);
    try out.append(allocator, '}');
    return try out.toOwnedSlice(allocator);
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

fn parseReadFileFlag(args: *std.process.Args.Iterator) []const u8 {
    var file_path: ?[]const u8 = null;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--file") or std.mem.eql(u8, arg, "-f")) {
            file_path = args.next();
        } else if (std.mem.eql(u8, arg, "--readonly")) {
            // Read commands already open query-readonly; this flag documents
            // intent and keeps Lite CLI examples portable.
        } else {
            cli.fatal("unknown argument: {s}", .{arg});
        }
    }
    return file_path orelse cli.fatal("--file is required", .{});
}

fn parseOutFlag(args: *std.process.Args.Iterator) []const u8 {
    var out_path: ?[]const u8 = null;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--out") or std.mem.eql(u8, arg, "-o")) {
            out_path = args.next();
        } else {
            cli.fatal("unknown argument: {s}", .{arg});
        }
    }
    return out_path orelse cli.fatal("--out is required", .{});
}

fn parseNameFlag(args: *std.process.Args.Iterator, flag_name: []const u8) []const u8 {
    var name: ?[]const u8 = null;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, flag_name) or std.mem.eql(u8, arg, "--name")) {
            name = args.next();
        } else {
            cli.fatal("unknown argument: {s}", .{arg});
        }
    }
    return name orelse cli.fatal("{s} is required", .{flag_name});
}

fn parseEnrichmentKind(value: []const u8) db_types.EnrichmentKind {
    if (std.mem.eql(u8, value, "chunk")) return .chunk;
    if (std.mem.eql(u8, value, "asset")) return .asset;
    if (std.mem.eql(u8, value, "embedding")) return .embedding;
    cli.fatal("unknown enrichment kind: {s}", .{value});
}

fn requireNoMoreArgs(args: *std.process.Args.Iterator) void {
    if (args.next()) |arg| cli.fatal("unknown argument: {s}", .{arg});
}

fn initTargetExists(io: std.Io, path: []const u8) bool {
    return pathExists(io, path);
}

fn requireAflitePath(path: []const u8) !void {
    if (!std.mem.endsWith(u8, path, ".aflite")) {
        std.debug.print("error: Antfly Lite database paths must end in .aflite: {s}\n", .{path});
        return error.InvalidArguments;
    }
}

fn requireAfbPath(path: []const u8) !void {
    if (!std.mem.endsWith(u8, path, ".afb")) {
        std.debug.print("error: Antfly portable backup paths must end in .afb: {s}\n", .{path});
        return error.InvalidArguments;
    }
}

fn requireRestoreSourcePath(path: []const u8) !void {
    if (std.mem.endsWith(u8, path, ".afb") or std.mem.endsWith(u8, path, ".aflite")) return;
    std.debug.print("error: Antfly Lite restore sources must end in .afb or .aflite: {s}\n", .{path});
    return error.InvalidArguments;
}

fn writeJsonLine(io: std.Io, json: []const u8) void {
    cli.writeStdout(io, json);
    cli.writeStdout(io, "\n");
}

fn writeFileAtomically(allocator: Allocator, io: std.Io, path: []const u8, contents: []const u8) !void {
    if (std.fs.path.dirname(path)) |parent| {
        try fs_paths.createDirPathPortable(io, parent);
    }

    const tmp_path = try std.fmt.allocPrint(allocator, "{s}.tmp", .{path});
    defer allocator.free(tmp_path);

    {
        var file = try fs_paths.createFilePortable(io, tmp_path, .{ .truncate = true });
        defer file.close(io);
        var buf: [8192]u8 = undefined;
        var writer = file.writer(io, &buf);
        try writer.interface.writeAll(contents);
        try writer.end();
        try file.sync(io);
    }

    renameFilePath(io, tmp_path, path) catch |err| {
        deleteFilePath(io, tmp_path) catch {};
        return err;
    };
}

fn pathExists(io: std.Io, path: []const u8) bool {
    if (std.fs.path.isAbsolute(path)) {
        std.Io.Dir.accessAbsolute(io, path, .{}) catch return false;
    } else {
        std.Io.Dir.cwd().access(io, path, .{}) catch return false;
    }
    return true;
}

fn renameFilePath(io: std.Io, old_path: []const u8, new_path: []const u8) !void {
    if (std.fs.path.isAbsolute(old_path) and std.fs.path.isAbsolute(new_path)) {
        try std.Io.Dir.renameAbsolute(old_path, new_path, io);
    } else {
        try std.Io.Dir.rename(std.Io.Dir.cwd(), old_path, std.Io.Dir.cwd(), new_path, io);
    }
}

fn deleteFilePath(io: std.Io, path: []const u8) !void {
    if (std.fs.path.isAbsolute(path)) {
        try std.Io.Dir.deleteFileAbsolute(io, path);
    } else {
        try std.Io.Dir.cwd().deleteFile(io, path);
    }
}

fn writeJsonString(allocator: Allocator, io: std.Io, value: []const u8) !void {
    const json = try std.fmt.allocPrint(allocator, "{f}", .{std.json.fmt(value, .{})});
    defer allocator.free(json);
    cli.writeStdout(io, json);
}

fn mutationJson(allocator: Allocator, field_name: []const u8, name: []const u8, value: bool) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(allocator);

    try out.append(allocator, '{');
    try appendJsonString(allocator, &out, field_name);
    try out.append(allocator, ':');
    try out.appendSlice(allocator, if (value) "true" else "false");
    try out.appendSlice(allocator, ",\"name\":");
    try appendJsonString(allocator, &out, name);
    try out.append(allocator, '}');
    return try out.toOwnedSlice(allocator);
}

fn printUsage(argv0: []const u8) void {
    std.debug.print(
        \\usage: {s} lite <subcommand> [options]
        \\
        \\subcommands:
        \\  init <db.aflite>
        \\  status <db.aflite>
        \\  info <db.aflite> (alias for status)
        \\  batch <db.aflite> --file request.json
        \\  lookup <db.aflite> --key <key> [--file request.json] [--readonly]
        \\  scan <db.aflite> --file request.json [--readonly]
        \\  query <db.aflite> --file request.json [--readonly]
        \\  index list <db.aflite>
        \\  index create <db.aflite> --file index.json
        \\  index drop <db.aflite> --index <name>
        \\  enrichment list <db.aflite>
        \\  enrichment create <db.aflite> --file enrichment.json
        \\  enrichment drop <db.aflite> --kind <chunk|asset|embedding> --name <name>
        \\  schema get <db.aflite>
        \\  schema set <db.aflite> --file schema.json
        \\  run-until-idle <db.aflite>
        \\  backup <db.aflite> --out backup.afb
        \\  export <db.aflite> --out backup.afb
        \\  snapshot <db.aflite> --out copy.aflite [--replace]
        \\  restore <backup.afb|source.aflite> --out <db.aflite> [--replace]
        \\  import <db.aflite> --from <backup.afb|source.aflite> [--replace]
        \\  promote <db.aflite> --target <url> --table <name> [--backup-id <id>] [--location <uri>]
        \\  check <db.aflite>
        \\  compact <db.aflite>
        \\  vacuum <db.aflite>
        \\  serve <db.aflite> --addr 127.0.0.1:8080 (lite-dev only)
        \\
    , .{argv0});
}

test "lite info subcommand aliases status" {
    try std.testing.expect(isStatusSubcommand("status"));
    try std.testing.expect(isStatusSubcommand("info"));
    try std.testing.expect(!isStatusSubcommand("check"));
}

test "lite path validation requires aflite extension" {
    try requireAflitePath("app.aflite");
    try std.testing.expectError(error.InvalidArguments, requireAflitePath("app.afl"));
}

test "lite backup path validation requires afb extension" {
    try requireAfbPath("app.afb");
    try std.testing.expectError(error.InvalidArguments, requireAfbPath("app.aflite"));
}

test "lite restore source validation accepts afb and aflite" {
    try requireRestoreSourcePath("app.afb");
    try requireRestoreSourcePath("app.aflite");
    try std.testing.expectError(error.InvalidArguments, requireRestoreSourcePath("app.db"));
}

test "lite serve parser preserves optional addr and rejects unknown args" {
    {
        const argv = [_][*:0]const u8{"app.aflite"};
        var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
        const opts = try parseServeOptions(&args);
        try std.testing.expectEqualStrings("app.aflite", opts.path);
        try std.testing.expectEqualStrings("127.0.0.1:8080", opts.addr);
    }
    {
        const argv = [_][*:0]const u8{ "app.aflite", "--addr", "127.0.0.1:9090" };
        var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
        const opts = try parseServeOptions(&args);
        try std.testing.expectEqualStrings("app.aflite", opts.path);
        try std.testing.expectEqualStrings("127.0.0.1:9090", opts.addr);
    }
    {
        const argv = [_][*:0]const u8{ "app.aflite", "--port", "9090" };
        var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
        try std.testing.expectError(error.InvalidArguments, parseServeOptions(&args));
    }
}

test "lite read file parser accepts explicit readonly flag" {
    const argv = [_][*:0]const u8{ "--readonly", "--file", "query.json" };
    var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    try std.testing.expectEqualStrings("query.json", parseReadFileFlag(&args));
}

test "lite init target check treats existing aflite as occupied" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/init-existing.aflite", .{tmp.sub_path});
    defer allocator.free(path);

    try std.testing.expect(!initTargetExists(io, path));
    {
        var lite = try LiteDb.open(allocator, path, .writer);
        defer lite.close();
    }
    try std.testing.expect(initTargetExists(io, path));
}

test "lite backup writer handles absolute output paths" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const cwd_tmp = try std.Io.Dir.cwd().realPathFileAlloc(io, ".zig-cache/tmp", allocator);
    defer allocator.free(cwd_tmp);
    const path = try std.fmt.allocPrint(allocator, "{s}/{s}/abs/backup.afb", .{ cwd_tmp, tmp.sub_path });
    defer allocator.free(path);

    try writeFileAtomically(allocator, io, path, "portable-backup");

    const body = try std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(64));
    defer allocator.free(body);
    try std.testing.expectEqualStrings("portable-backup", body);
}

test "lite restore source can be an aflite database" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const src_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/restore-source.aflite", .{tmp.sub_path});
    defer allocator.free(src_path);
    const dst_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/restore-dst.aflite", .{tmp.sub_path});
    defer allocator.free(dst_path);

    {
        var source = try LiteDb.open(allocator, src_path, .writer);
        defer source.close();
        const json = try batchJson(allocator, &source.db, "{\"inserts\":{\"doc:lite-restore\":{\"title\":\"from aflite source\"}}}");
        defer allocator.free(json);
        try std.testing.expect(std.mem.indexOf(u8, json, "\"inserted\":1") != null);
    }

    const portable = try readPortableRestoreSourceAlloc(allocator, io, src_path);
    defer allocator.free(portable);

    {
        var dest = try LiteDb.open(allocator, dst_path, .writer);
        defer dest.close();
        try portable_backup.importPortable(allocator, dest.db.core.store, portable);
    }

    {
        var reopened = try LiteDb.open(allocator, dst_path, .query_readonly);
        defer reopened.close();
        const json = try lookupJson(allocator, &reopened.db, "doc:lite-restore", "");
        defer allocator.free(json);
        try std.testing.expect(std.mem.indexOf(u8, json, "\"from aflite source\"") != null);
    }
}

test "lite status json includes pending work" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/status-pending.aflite", .{tmp.sub_path});
    defer allocator.free(path);

    var lite = try LiteDb.open(allocator, path, .writer);
    defer lite.close();

    const index_json =
        \\{"name":"full_text_index_v0","kind":"full_text","config_json":"{}"}
    ;
    var parsed = try std.json.parseFromSlice(db_types.IndexConfig, allocator, index_json, .{
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();
    try lite.db.addIndex(parsed.value);

    const batch_response = try batchJson(allocator, &lite.db, "{\"inserts\":{\"doc:status-pending\":{\"body\":\"pending work visible\"}}}");
    defer allocator.free(batch_response);
    try std.testing.expect(std.mem.indexOf(u8, batch_response, "\"inserted\":1") != null);

    const json = try statusJson(allocator, &lite, .native);
    defer allocator.free(json);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"storage\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"format\":\"aflite\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"engine\":\"native_single_file\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"format_version\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"checkpoint_sequence\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"stats\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"pending_work\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"capabilities\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"manual_maintenance\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"background_enrichment_runtime\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"inference_mode\":\"caller_supplied_or_disabled\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"no_inference_configured_ok\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"caller_supplied_artifacts\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"local_inference_runtime\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"raft_replication\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"cluster_placement\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"remote_shard_fanout\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"distributed_transaction_coordination\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"has_async_indexes\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"derived_target_sequence\":") != null);
}

test "lite snapshot copies stable aflite prefix without source tail" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const src_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/snapshot-source.aflite", .{tmp.sub_path});
    defer allocator.free(src_path);
    const snapshot_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/snapshot-copy.aflite", .{tmp.sub_path});
    defer allocator.free(snapshot_path);

    const source_size = blk: {
        var source = try LiteDb.open(allocator, src_path, .writer);
        defer source.close();
        const json = try batchJson(allocator, &source.db, "{\"inserts\":{\"doc:lite-snapshot\":{\"title\":\"stable snapshot\"}}}");
        defer allocator.free(json);
        try std.testing.expect(std.mem.indexOf(u8, json, "\"inserted\":1") != null);
        break :blk (try source.backend.native_docstore.?.file.file.stat(source.backend.native_docstore.?.file.io_impl.io())).size;
    };

    {
        var file = try std.Io.Dir.cwd().openFile(io, src_path, .{ .mode = .read_write });
        defer file.close(io);
        try file.writePositionalAll(io, "tail", source_size);
    }

    const source_report = try antfly.lite.backend.checkFile(allocator, src_path);
    try std.testing.expect(!source_report.valid);
    try std.testing.expectEqualStrings("tail_bytes", source_report.issue.?);

    try snapshotStableAflite(allocator, io, src_path, snapshot_path, false);

    const snapshot_report = try antfly.lite.backend.checkFile(allocator, snapshot_path);
    try std.testing.expect(snapshot_report.valid);
    try std.testing.expectEqual(@as(u64, 0), snapshot_report.tail_bytes);

    var snapshot_db = try LiteDb.open(allocator, snapshot_path, .query_readonly);
    defer snapshot_db.close();
    const json = try lookupJson(allocator, &snapshot_db.db, "doc:lite-snapshot", "");
    defer allocator.free(json);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"stable snapshot\"") != null);
}

test "lite promote stages portable afb and table manifest" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const src_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/promote-src.aflite", .{tmp.sub_path});
    defer allocator.free(src_path);
    const cwd_tmp = try std.Io.Dir.cwd().realPathFileAlloc(io, ".zig-cache/tmp", allocator);
    defer allocator.free(cwd_tmp);
    const backup_root = try std.fmt.allocPrint(allocator, "{s}/{s}/promote-backups", .{ cwd_tmp, tmp.sub_path });
    defer allocator.free(backup_root);
    const location = try std.fmt.allocPrint(allocator, "file://{s}", .{backup_root});
    defer allocator.free(location);

    {
        var source = try LiteDb.open(allocator, src_path, .writer);
        defer source.close();
        const json = try batchJson(allocator, &source.db, "{\"inserts\":{\"doc:promote\":{\"title\":\"portable promote\"}}}");
        defer allocator.free(json);
        try std.testing.expect(std.mem.indexOf(u8, json, "\"inserted\":1") != null);
    }

    var staged = try lite_restore_staging.stageAfliteRestoreBackup(allocator, src_path, "docs", "lite-promote-test", location);
    defer staged.deinit(allocator);
    try std.testing.expectEqualStrings("lite-promote-test", staged.backup_id);
    try std.testing.expectEqualStrings("lite-promote-test.afb", staged.snapshot_path);

    var backup_location = try antfly.public_api.backups.openBackupLocation(allocator, location);
    defer backup_location.deinit(allocator);
    var manifest = try antfly.public_api.backups.readManifestFromLocation(allocator, &backup_location, "lite-promote-test");
    defer manifest.deinit(allocator);
    try std.testing.expectEqualStrings("docs", manifest.table_name);
    try std.testing.expectEqual(@as(usize, 1), manifest.shards.len);
    try std.testing.expectEqualStrings("lite-promote-test.afb", manifest.shards[0].snapshot_path);

    const afb_path = try std.fmt.allocPrint(allocator, "{s}/lite-promote-test.afb", .{backup_root});
    defer allocator.free(afb_path);
    const portable = try std.Io.Dir.cwd().readFileAlloc(io, afb_path, allocator, .limited(max_afb_file_bytes));
    defer allocator.free(portable);
    try std.testing.expect(portable.len > 0);
}
