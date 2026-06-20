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
const platform_sync = @import("antfly_platform").sync;
const fs_paths = antfly.common.fs_paths;

const Allocator = std.mem.Allocator;
const max_json_file_bytes: usize = 64 * 1024 * 1024;

const db_mod = antfly.db;
const db_types = db_mod.types;
const batch_api = antfly.public_api.batch;
const query_api = antfly.public_api.query;
const backup_codec = antfly.backup_codec;
const portable_backup = antfly.portable_backup;

var active_lite_http_state: ?*LiteHttpState = null;
const lite_http_state_key = "antfly.lite.state";

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

    fn create(allocator: Allocator, path: []const u8, exclusive: bool) !LiteDb {
        var backend = try antfly.lite.backend.Handle.create(allocator, path, exclusive);
        errdefer backend.deinit();

        var opts = db_mod.OpenOptions{
            .open_mode = .writer,
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

const CompactReport = struct {
    compacted: bool,
    vacuum: antfly.lite.backend.VacuumReport,
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
    if (std.mem.eql(u8, subcommand, "compact")) return try compact(init.gpa, init.io, args);
    if (std.mem.eql(u8, subcommand, "vacuum")) return try vacuum(init.gpa, init.io, args);
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

    var lite = try LiteDb.create(allocator, path, true);
    defer lite.close();

    cli.writeStdout(io, "{\"format\":\"aflite\",\"path\":");
    try writeJsonString(allocator, io, path);
    cli.writeStdout(io, "}\n");
}

fn status(allocator: Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    const path = args.next() orelse cli.fatal("database path is required", .{});
    try requireAflitePath(path);
    requireNoMoreArgs(args);

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
    requireNoMoreArgs(args);

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
    try portable_backup.validatePortable(allocator, out.items);
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
    const report = try copyStableAfliteToPath(allocator, io, path, out_path, replace);
    const json = try std.json.Stringify.valueAlloc(allocator, report, .{});
    defer allocator.free(json);
    writeJsonLine(io, json);
}

fn copyStableAfliteToPath(allocator: Allocator, io: std.Io, path: []const u8, out_path: []const u8, replace: bool) !antfly.lite.backend.StableSnapshotReport {
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

    return try backend.copyStableSnapshot(out_path, replace);
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
    if (!replace and pathExists(io, path)) {
        if (!std.mem.endsWith(u8, resolved_from, ".afb")) {
            cli.fatal("import into an existing Lite database requires a portable .afb source; use restore --replace for .aflite snapshots", .{});
        }
        importPortableIntoExistingLite(allocator, io, resolved_from, path) catch |err| switch (err) {
            error.LiteImportTargetNotEmpty => cli.fatal("target database is not empty; pass --replace to replace it: {s}", .{path}),
            else => return err,
        };
        return;
    }
    try restoreFromSourceFile(allocator, io, resolved_from, path, replace);
}

fn importPortableIntoExistingLite(
    allocator: Allocator,
    io: std.Io,
    source_path: []const u8,
    target_path: []const u8,
) !void {
    try requireAfbPath(source_path);
    try requireAflitePath(target_path);
    if (try pathsReferToSameExistingFile(allocator, io, source_path, target_path)) {
        std.debug.print("error: source and target database paths must be different: {s}\n", .{source_path});
        return error.InvalidArguments;
    }

    {
        var lite = try LiteDb.open(allocator, target_path, .writer);
        defer lite.close();
        if (!(try liteImportTargetIsEmpty(allocator, &lite))) {
            return error.LiteImportTargetNotEmpty;
        }
    }

    try restoreFromSourceFile(allocator, io, source_path, target_path, true);
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
    if (std.mem.eql(u8, source_path, out_path) or try pathsReferToSameExistingFile(allocator, io, source_path, out_path)) {
        std.debug.print("error: source and output database paths must be different: {s}\n", .{source_path});
        return error.InvalidArguments;
    }

    const target_exists = pathExists(io, out_path);
    if (target_exists and !replace) cli.fatal("output database already exists; pass --replace to overwrite: {s}", .{out_path});
    if (target_exists) {
        var target_probe = try LiteDb.open(allocator, out_path, .writer);
        target_probe.close();
    }

    if (std.mem.endsWith(u8, source_path, ".aflite")) {
        const tmp_path = try restoreTempPathAlloc(allocator, out_path);
        defer allocator.free(tmp_path);
        try deleteFileIfExists(io, tmp_path);
        errdefer deleteFilePath(io, tmp_path) catch {};

        _ = try copyStableAfliteToPath(allocator, io, source_path, tmp_path, true);
        renameFilePath(io, tmp_path, out_path) catch |err| {
            deleteFilePath(io, tmp_path) catch {};
            return err;
        };

        cli.writeStdout(io, "{\"format\":\"aflite\",\"path\":");
        try writeJsonString(allocator, io, out_path);
        cli.writeStdout(io, "}\n");
        return;
    }

    const body = try readPortableRestoreSourceAlloc(allocator, io, source_path);
    defer allocator.free(body);
    try portable_backup.validatePortable(allocator, body);

    const tmp_path = try restoreTempPathAlloc(allocator, out_path);
    defer allocator.free(tmp_path);
    try deleteFileIfExists(io, tmp_path);
    errdefer deleteFilePath(io, tmp_path) catch {};

    {
        var lite = try LiteDb.create(allocator, tmp_path, true);
        defer lite.close();
        try portable_backup.importPortable(allocator, lite.db.core.store, body);
    }

    try finishRestoredLiteImportPath(allocator, tmp_path);

    renameFilePath(io, tmp_path, out_path) catch |err| {
        deleteFilePath(io, tmp_path) catch {};
        return err;
    };

    cli.writeStdout(io, "{\"format\":\"aflite\",\"path\":");
    try writeJsonString(allocator, io, out_path);
    cli.writeStdout(io, "}\n");
}

fn readPortableRestoreSourceAlloc(allocator: Allocator, io: std.Io, source_path: []const u8) ![]u8 {
    if (std.mem.endsWith(u8, source_path, ".afb")) {
        return try cli.readFileAlloc(io, allocator, source_path, max_afb_file_bytes);
    }
    return error.InvalidArguments;
}

fn finishRestoredLiteImportPath(allocator: Allocator, path: []const u8) !void {
    var lite = try LiteDb.open(allocator, path, .writer);
    defer lite.close();
    _ = try lite.db.rebuildDenseIndexesForTargetCoverage(allocator);
    _ = try lite.db.replayGeneratedEnrichmentsFromStoredDocs(allocator);
    try lite.db.runUntilIdle();
    try lite.db.sync(true);
    try lite.db.syncIndexes(true);
}

fn liteImportTargetIsEmpty(allocator: Allocator, lite: *LiteDb) !bool {
    if (try lite.db.primaryDocCount(allocator) != 0) return false;
    if (lite.db.core.indexCount() != 0) return false;

    const enrichments = try lite.db.listEnrichments(allocator);
    defer db_types.freeEnrichmentConfigs(allocator, enrichments);
    if (enrichments.len != 0) return false;

    const resolvers = try lite.db.listResolvers(allocator);
    defer {
        for (resolvers) |*resolver| resolver.deinit(allocator);
        if (resolvers.len > 0) allocator.free(resolvers);
    }
    if (resolvers.len != 0) return false;

    if (try lite.db.getSchemaJson(allocator)) |schema_json| {
        allocator.free(schema_json);
        return false;
    }

    return true;
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

    const opts = parsePromoteOptions(allocator, path, args) catch cli.fatal("invalid promote arguments", .{});
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
            target = try nextRequiredArg(args);
        } else if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            table = try nextRequiredArg(args);
        } else if (std.mem.eql(u8, arg, "--backup-id")) {
            backup_id = try nextRequiredArg(args);
        } else if (std.mem.eql(u8, arg, "--location")) {
            location = try nextRequiredArg(args);
        } else {
            return error.UnknownArgument;
        }
    }
    const resolved_target = target orelse return error.MissingArgument;
    const resolved_table = table orelse return error.MissingArgument;
    return .{
        .target = resolved_target,
        .table = resolved_table,
        .backup_id = if (backup_id) |id| try allocator.dupe(u8, id) else try lite_restore_staging.defaultBackupIdAlloc(allocator, path),
        .location = location,
    };
}

fn nextRequiredArg(args: *std.process.Args.Iterator) ![]const u8 {
    return args.next() orelse error.MissingArgument;
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

    const report = try vacuumAflitePath(allocator, path);
    const json = try std.json.Stringify.valueAlloc(allocator, report, .{});
    defer allocator.free(json);
    writeJsonLine(io, json);
}

fn compact(allocator: Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    const path = args.next() orelse cli.fatal("database path is required", .{});
    try requireAflitePath(path);
    requireNoMoreArgs(args);

    {
        var lite = try LiteDb.open(allocator, path, .writer);
        defer lite.close();
        try prepareCompactLite(&lite);
    }

    const report = CompactReport{
        .compacted = true,
        .vacuum = try vacuumAflitePath(allocator, path),
    };
    const json = try std.json.Stringify.valueAlloc(allocator, report, .{});
    defer allocator.free(json);
    writeJsonLine(io, json);
}

fn vacuumAflitePath(allocator: Allocator, path: []const u8) !antfly.lite.backend.VacuumReport {
    var backend = try antfly.lite.backend.Handle.open(allocator, path, .{});
    defer backend.deinit();
    return try backend.vacuum();
}

fn prepareCompactLite(lite: *LiteDb) !void {
    try lite.db.maintenanceDriver().runUntilIdle();
    try lite.db.forceCompactTextIndexes();
    try lite.db.drainScheduledTextMerges();
    try lite.db.sync(true);
    try lite.db.syncIndexes(true);
}

fn compactLite(lite: *LiteDb) !CompactReport {
    try prepareCompactLite(lite);
    return .{
        .compacted = true,
        .vacuum = try lite.backend.vacuum(),
    };
}

const ServeOptions = struct {
    path: []const u8,
    addr: []const u8 = "127.0.0.1:8080",
};

const LiteHttpState = struct {
    lite: *LiteDb,
    mutex: std.atomic.Mutex = .unlocked,
};

const LiteListenAddress = struct {
    host: []const u8,
    port: u16,
};

fn serve(allocator: Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    const opts = try parseServeOptions(args);
    try requireAflitePath(opts.path);

    const listen = try parseLiteListenAddress(opts.addr);
    var lite = try LiteDb.open(allocator, opts.path, .writer);
    defer lite.close();

    var state = LiteHttpState{ .lite = &lite };
    if (active_lite_http_state != null) return error.UnsupportedLiteServe;
    active_lite_http_state = &state;
    defer active_lite_http_state = null;

    var server = httpx.Server.initWithConfig(allocator, io, .{
        .host = listen.host,
        .port = listen.port,
        .max_body_size = max_json_file_bytes,
    });
    defer server.deinit();

    try registerLiteHttpRoutes(&server);
    try server.bind();
    if (server.boundAddress()) |addr| {
        std.debug.print("antfly lite serving {s} on http://{}\n", .{ opts.path, addr });
    }
    try server.listen();
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

fn parseLiteListenAddress(addr: []const u8) !LiteListenAddress {
    const sep = std.mem.lastIndexOfScalar(u8, addr, ':') orelse return error.InvalidArguments;
    if (sep == 0 or sep + 1 >= addr.len) return error.InvalidArguments;
    const port = try std.fmt.parseInt(u16, addr[sep + 1 ..], 10);
    return .{ .host = addr[0..sep], .port = port };
}

fn registerLiteHttpRoutes(server: *httpx.Server) !void {
    try server.preRoute(liteHttpInjectState);
    try server.get("/healthz", liteHttpHealth);
    try server.get("/lite/v1/status", liteHttpStatus);
    try server.get("/lite/v1/capabilities", liteHttpCapabilities);
    try server.post("/lite/v1/batch", liteHttpBatch);
    try server.post("/lite/v1/lookup/:key", liteHttpLookup);
    try server.post("/lite/v1/scan", liteHttpScan);
    try server.post("/lite/v1/query", liteHttpQuery);
    try server.get("/lite/v1/indexes", liteHttpIndexList);
    try server.post("/lite/v1/indexes", liteHttpIndexCreate);
    try server.delete("/lite/v1/indexes/:name", liteHttpIndexDrop);
    try server.get("/lite/v1/enrichments", liteHttpEnrichmentList);
    try server.post("/lite/v1/enrichments", liteHttpEnrichmentCreate);
    try server.delete("/lite/v1/enrichments/:kind/:name", liteHttpEnrichmentDrop);
    try server.get("/lite/v1/schema", liteHttpSchemaGet);
    try server.put("/lite/v1/schema", liteHttpSchemaSet);
    try server.post("/lite/v1/run-until-idle", liteHttpRunUntilIdle);
    try server.get("/lite/v1/check", liteHttpCheck);
    try server.post("/lite/v1/compact", liteHttpCompact);
    try server.post("/lite/v1/vacuum", liteHttpVacuum);
}

fn liteHttpInjectState(ctx: *httpx.Context) anyerror!void {
    const state = active_lite_http_state orelse return error.LiteHttpServerUnavailable;
    try ctx.setData(lite_http_state_key, state, null);
}

fn liteHttpState(ctx: *httpx.Context) !*LiteHttpState {
    const raw = ctx.getData(lite_http_state_key) orelse return error.LiteHttpServerUnavailable;
    return @ptrCast(@alignCast(raw));
}

fn lockLiteHttpState(state: *LiteHttpState) void {
    platform_sync.lockYielding(&state.mutex);
}

fn liteHttpJson(ctx: *httpx.Context, status_code: u16, body: []const u8) !httpx.Response {
    _ = ctx.response.status(status_code);
    _ = try ctx.response.header("Content-Type", "application/json");
    _ = ctx.response.body(body);
    return ctx.response.build();
}

fn liteHttpError(ctx: *httpx.Context, err: anyerror) anyerror!httpx.Response {
    const status_code: u16 = switch (err) {
        error.FileNotFound, error.NotFound => 404,
        error.WouldBlock, error.WriterLocked, error.FileBusy => 409,
        error.InvalidArguments,
        error.InvalidArgument,
        error.InvalidQueryRequest,
        error.UnsupportedQueryRequest,
        error.UnsupportedLiteServe,
        error.InvalidNativeMagic,
        error.TruncatedNativeHeader,
        error.UnsupportedNativeFormatVersion,
        error.InvalidNativeHeaderSize,
        error.NativeHeaderChecksumMismatch,
        error.InvalidNativePageSize,
        error.InvalidNativeCheckpoint,
        error.TruncatedNativeFile,
        error.EndOfStream,
        error.Truncated,
        error.InvalidMagic,
        error.HeaderCrcMismatch,
        error.UnsupportedVersion,
        error.BlockCrcMismatch,
        => 400,
        else => 500,
    };
    var buf: [256]u8 = undefined;
    const body = std.fmt.bufPrint(&buf, "{{\"error\":\"{s}\"}}", .{@errorName(err)}) catch "{\"error\":\"internal\"}";
    return liteHttpJson(ctx, status_code, body);
}

fn liteHttpRequestBody(ctx: *httpx.Context) ![]const u8 {
    return (try ctx.body()) orelse "";
}

fn liteHttpHealth(ctx: *httpx.Context) anyerror!httpx.Response {
    return liteHttpJson(ctx, 200, "{\"status\":\"ok\"}");
}

fn liteHttpStatus(ctx: *httpx.Context) anyerror!httpx.Response {
    const state = liteHttpState(ctx) catch |err| return liteHttpError(ctx, err);
    lockLiteHttpState(state);
    defer state.mutex.unlock();
    const json = statusJson(ctx.allocator, state.lite, .native) catch |err| return liteHttpError(ctx, err);
    defer ctx.allocator.free(json);
    return liteHttpJson(ctx, 200, json);
}

fn liteHttpCapabilities(ctx: *httpx.Context) anyerror!httpx.Response {
    const json = std.json.Stringify.valueAlloc(ctx.allocator, antfly.lite.backend.capabilitiesForProfile(.native), .{}) catch |err| return liteHttpError(ctx, err);
    defer ctx.allocator.free(json);
    return liteHttpJson(ctx, 200, json);
}

fn liteHttpBatch(ctx: *httpx.Context) anyerror!httpx.Response {
    const body = liteHttpRequestBody(ctx) catch |err| return liteHttpError(ctx, err);
    const state = liteHttpState(ctx) catch |err| return liteHttpError(ctx, err);
    lockLiteHttpState(state);
    defer state.mutex.unlock();
    const json = batchJson(ctx.allocator, &state.lite.db, body) catch |err| return liteHttpError(ctx, err);
    defer ctx.allocator.free(json);
    return liteHttpJson(ctx, 200, json);
}

fn liteHttpLookup(ctx: *httpx.Context) anyerror!httpx.Response {
    const key = ctx.param("key") orelse return liteHttpError(ctx, error.InvalidArguments);
    const body = liteHttpRequestBody(ctx) catch |err| return liteHttpError(ctx, err);
    const state = liteHttpState(ctx) catch |err| return liteHttpError(ctx, err);
    lockLiteHttpState(state);
    defer state.mutex.unlock();
    const json = lookupJson(ctx.allocator, &state.lite.db, key, body) catch |err| return liteHttpError(ctx, err);
    defer ctx.allocator.free(json);
    return liteHttpJson(ctx, 200, json);
}

fn liteHttpScan(ctx: *httpx.Context) anyerror!httpx.Response {
    const body = liteHttpRequestBody(ctx) catch |err| return liteHttpError(ctx, err);
    const state = liteHttpState(ctx) catch |err| return liteHttpError(ctx, err);
    lockLiteHttpState(state);
    defer state.mutex.unlock();
    const json = scanJson(ctx.allocator, &state.lite.db, body) catch |err| return liteHttpError(ctx, err);
    defer ctx.allocator.free(json);
    return liteHttpJson(ctx, 200, json);
}

fn liteHttpQuery(ctx: *httpx.Context) anyerror!httpx.Response {
    const body = liteHttpRequestBody(ctx) catch |err| return liteHttpError(ctx, err);
    const state = liteHttpState(ctx) catch |err| return liteHttpError(ctx, err);
    lockLiteHttpState(state);
    defer state.mutex.unlock();
    const json = searchJson(ctx.allocator, &state.lite.db, body) catch |err| return liteHttpError(ctx, err);
    defer ctx.allocator.free(json);
    return liteHttpJson(ctx, 200, json);
}

fn liteHttpIndexList(ctx: *httpx.Context) anyerror!httpx.Response {
    const state = liteHttpState(ctx) catch |err| return liteHttpError(ctx, err);
    lockLiteHttpState(state);
    defer state.mutex.unlock();
    const configs = state.lite.db.listIndexes(ctx.allocator) catch |err| return liteHttpError(ctx, err);
    defer db_types.freeIndexConfigs(ctx.allocator, configs);
    const json = std.json.Stringify.valueAlloc(ctx.allocator, configs, .{}) catch |err| return liteHttpError(ctx, err);
    defer ctx.allocator.free(json);
    return liteHttpJson(ctx, 200, json);
}

fn liteHttpIndexCreate(ctx: *httpx.Context) anyerror!httpx.Response {
    const body = liteHttpRequestBody(ctx) catch |err| return liteHttpError(ctx, err);
    var parsed = std.json.parseFromSlice(db_types.IndexConfig, ctx.allocator, body, .{
        .ignore_unknown_fields = true,
    }) catch |err| return liteHttpError(ctx, err);
    defer parsed.deinit();
    const state = liteHttpState(ctx) catch |err| return liteHttpError(ctx, err);
    lockLiteHttpState(state);
    defer state.mutex.unlock();
    state.lite.db.addIndex(parsed.value) catch |err| return liteHttpError(ctx, err);
    const json = mutationJson(ctx.allocator, "created", parsed.value.name, true) catch |err| return liteHttpError(ctx, err);
    defer ctx.allocator.free(json);
    return liteHttpJson(ctx, 200, json);
}

fn liteHttpIndexDrop(ctx: *httpx.Context) anyerror!httpx.Response {
    const name = ctx.param("name") orelse return liteHttpError(ctx, error.InvalidArguments);
    const state = liteHttpState(ctx) catch |err| return liteHttpError(ctx, err);
    lockLiteHttpState(state);
    defer state.mutex.unlock();
    const removed = state.lite.db.deleteIndex(name) catch |err| return liteHttpError(ctx, err);
    const json = mutationJson(ctx.allocator, "removed", name, removed) catch |err| return liteHttpError(ctx, err);
    defer ctx.allocator.free(json);
    return liteHttpJson(ctx, 200, json);
}

fn liteHttpEnrichmentList(ctx: *httpx.Context) anyerror!httpx.Response {
    const state = liteHttpState(ctx) catch |err| return liteHttpError(ctx, err);
    lockLiteHttpState(state);
    defer state.mutex.unlock();
    const configs = state.lite.db.listEnrichments(ctx.allocator) catch |err| return liteHttpError(ctx, err);
    defer db_types.freeEnrichmentConfigs(ctx.allocator, configs);
    const json = std.json.Stringify.valueAlloc(ctx.allocator, configs, .{}) catch |err| return liteHttpError(ctx, err);
    defer ctx.allocator.free(json);
    return liteHttpJson(ctx, 200, json);
}

fn liteHttpEnrichmentCreate(ctx: *httpx.Context) anyerror!httpx.Response {
    const body = liteHttpRequestBody(ctx) catch |err| return liteHttpError(ctx, err);
    var parsed = std.json.parseFromSlice(db_types.EnrichmentConfig, ctx.allocator, body, .{
        .ignore_unknown_fields = true,
    }) catch |err| return liteHttpError(ctx, err);
    defer parsed.deinit();
    const state = liteHttpState(ctx) catch |err| return liteHttpError(ctx, err);
    lockLiteHttpState(state);
    defer state.mutex.unlock();
    state.lite.db.addEnrichment(parsed.value) catch |err| return liteHttpError(ctx, err);
    const json = mutationJson(ctx.allocator, "created", parsed.value.name, true) catch |err| return liteHttpError(ctx, err);
    defer ctx.allocator.free(json);
    return liteHttpJson(ctx, 200, json);
}

fn liteHttpEnrichmentDrop(ctx: *httpx.Context) anyerror!httpx.Response {
    const kind_raw = ctx.param("kind") orelse return liteHttpError(ctx, error.InvalidArguments);
    const name = ctx.param("name") orelse return liteHttpError(ctx, error.InvalidArguments);
    const kind = parseLiteHttpEnrichmentKind(kind_raw) catch |err| return liteHttpError(ctx, err);
    const state = liteHttpState(ctx) catch |err| return liteHttpError(ctx, err);
    lockLiteHttpState(state);
    defer state.mutex.unlock();
    const removed = state.lite.db.deleteEnrichment(kind, name) catch |err| return liteHttpError(ctx, err);
    const json = mutationJson(ctx.allocator, "removed", name, removed) catch |err| return liteHttpError(ctx, err);
    defer ctx.allocator.free(json);
    return liteHttpJson(ctx, 200, json);
}

fn parseLiteHttpEnrichmentKind(value: []const u8) !db_types.EnrichmentKind {
    if (std.mem.eql(u8, value, "chunk")) return .chunk;
    if (std.mem.eql(u8, value, "asset")) return .asset;
    if (std.mem.eql(u8, value, "embedding")) return .embedding;
    return error.InvalidArguments;
}

fn liteHttpSchemaGet(ctx: *httpx.Context) anyerror!httpx.Response {
    const state = liteHttpState(ctx) catch |err| return liteHttpError(ctx, err);
    lockLiteHttpState(state);
    defer state.mutex.unlock();
    const schema_json = state.lite.db.getSchemaJson(ctx.allocator) catch |err| return liteHttpError(ctx, err);
    if (schema_json) |json| {
        defer ctx.allocator.free(json);
        return liteHttpJson(ctx, 200, json);
    }
    return liteHttpJson(ctx, 200, "null");
}

fn liteHttpSchemaSet(ctx: *httpx.Context) anyerror!httpx.Response {
    const body = liteHttpRequestBody(ctx) catch |err| return liteHttpError(ctx, err);
    const state = liteHttpState(ctx) catch |err| return liteHttpError(ctx, err);
    lockLiteHttpState(state);
    defer state.mutex.unlock();
    state.lite.db.setSchemaJson(ctx.allocator, body) catch |err| return liteHttpError(ctx, err);
    return liteHttpJson(ctx, 200, "{\"updated\":true}");
}

fn liteHttpRunUntilIdle(ctx: *httpx.Context) anyerror!httpx.Response {
    const state = liteHttpState(ctx) catch |err| return liteHttpError(ctx, err);
    lockLiteHttpState(state);
    defer state.mutex.unlock();
    state.lite.db.maintenanceDriver().runUntilIdle() catch |err| return liteHttpError(ctx, err);
    const json = std.json.Stringify.valueAlloc(ctx.allocator, state.lite.db.maintenanceDriver().pendingWorkStats(), .{}) catch |err| return liteHttpError(ctx, err);
    defer ctx.allocator.free(json);
    return liteHttpJson(ctx, 200, json);
}

fn liteHttpCheck(ctx: *httpx.Context) anyerror!httpx.Response {
    const state = liteHttpState(ctx) catch |err| return liteHttpError(ctx, err);
    lockLiteHttpState(state);
    defer state.mutex.unlock();
    const report = state.lite.backend.check() catch |err| return liteHttpError(ctx, err);
    const json = std.json.Stringify.valueAlloc(ctx.allocator, report, .{}) catch |err| return liteHttpError(ctx, err);
    defer ctx.allocator.free(json);
    return liteHttpJson(ctx, 200, json);
}

fn liteHttpCompact(ctx: *httpx.Context) anyerror!httpx.Response {
    const state = liteHttpState(ctx) catch |err| return liteHttpError(ctx, err);
    lockLiteHttpState(state);
    defer state.mutex.unlock();
    const report = compactLite(state.lite) catch |err| return liteHttpError(ctx, err);
    const json = std.json.Stringify.valueAlloc(ctx.allocator, report, .{}) catch |err| return liteHttpError(ctx, err);
    defer ctx.allocator.free(json);
    return liteHttpJson(ctx, 200, json);
}

fn liteHttpVacuum(ctx: *httpx.Context) anyerror!httpx.Response {
    const state = liteHttpState(ctx) catch |err| return liteHttpError(ctx, err);
    lockLiteHttpState(state);
    defer state.mutex.unlock();
    const report = state.lite.backend.vacuum() catch |err| return liteHttpError(ctx, err);
    const json = std.json.Stringify.valueAlloc(ctx.allocator, report, .{}) catch |err| return liteHttpError(ctx, err);
    defer ctx.allocator.free(json);
    return liteHttpJson(ctx, 200, json);
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

fn statusJson(allocator: Allocator, lite: *LiteDb, profile: antfly.lite.backend.Profile) ![]u8 {
    var status_value = try lite.backend.fullStatus(allocator, &lite.db, profile);
    defer status_value.deinit(allocator);
    return try std.json.Stringify.valueAlloc(allocator, status_value, .{});
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

fn restoreTempPathAlloc(allocator: Allocator, out_path: []const u8) ![]u8 {
    return try std.fmt.allocPrint(allocator, "{s}.restore-tmp.aflite", .{out_path});
}

fn pathExists(io: std.Io, path: []const u8) bool {
    if (std.fs.path.isAbsolute(path)) {
        std.Io.Dir.accessAbsolute(io, path, .{}) catch return false;
    } else {
        std.Io.Dir.cwd().access(io, path, .{}) catch return false;
    }
    return true;
}

fn pathsReferToSameExistingFile(allocator: Allocator, io: std.Io, a: []const u8, b: []const u8) !bool {
    const a_real = realPathExistingAlloc(allocator, io, a) catch |err| switch (err) {
        error.FileNotFound, error.NotDir => return false,
        else => return err,
    };
    defer allocator.free(a_real);

    const b_real = realPathExistingAlloc(allocator, io, b) catch |err| switch (err) {
        error.FileNotFound, error.NotDir => return false,
        else => return err,
    };
    defer allocator.free(b_real);

    return std.mem.eql(u8, a_real, b_real);
}

fn realPathExistingAlloc(allocator: Allocator, io: std.Io, path: []const u8) ![:0]u8 {
    if (std.fs.path.isAbsolute(path)) {
        return try std.Io.Dir.realPathFileAbsoluteAlloc(io, path, allocator);
    }
    return try std.Io.Dir.cwd().realPathFileAlloc(io, path, allocator);
}

fn deleteFileIfExists(io: std.Io, path: []const u8) !void {
    deleteFilePath(io, path) catch |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    };
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
        \\  serve <db.aflite> --addr 127.0.0.1:8080
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
        const listen = try parseLiteListenAddress(opts.addr);
        try std.testing.expectEqualStrings("127.0.0.1", listen.host);
        try std.testing.expectEqual(@as(u16, 8080), listen.port);
    }
    {
        const argv = [_][*:0]const u8{ "app.aflite", "--addr", "127.0.0.1:9090" };
        var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
        const opts = try parseServeOptions(&args);
        try std.testing.expectEqualStrings("app.aflite", opts.path);
        try std.testing.expectEqualStrings("127.0.0.1:9090", opts.addr);
        const listen = try parseLiteListenAddress(opts.addr);
        try std.testing.expectEqualStrings("127.0.0.1", listen.host);
        try std.testing.expectEqual(@as(u16, 9090), listen.port);
    }
    {
        const argv = [_][*:0]const u8{ "app.aflite", "--port", "9090" };
        var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
        try std.testing.expectError(error.InvalidArguments, parseServeOptions(&args));
    }
    try std.testing.expectError(error.InvalidArguments, parseLiteListenAddress("127.0.0.1"));
    try std.testing.expectError(error.InvalidArguments, parseLiteListenAddress(":8080"));
}

test "lite promote parser requires values and derives default backup id" {
    const allocator = std.testing.allocator;

    {
        const argv = [_][*:0]const u8{ "--target", "http://localhost:8080", "--table", "docs", "--location", "file:///tmp/backups" };
        var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
        const opts = try parsePromoteOptions(allocator, "app.aflite", &args);
        defer allocator.free(opts.backup_id);
        try std.testing.expectEqualStrings("http://localhost:8080", opts.target);
        try std.testing.expectEqualStrings("docs", opts.table);
        try std.testing.expectEqualStrings("lite-app", opts.backup_id);
        try std.testing.expectEqualStrings("file:///tmp/backups", opts.location);
    }

    {
        const argv = [_][*:0]const u8{ "--target", "http://localhost:8080", "--table", "docs", "--backup-id", "explicit-id" };
        var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
        const opts = try parsePromoteOptions(allocator, "app.aflite", &args);
        defer allocator.free(opts.backup_id);
        try std.testing.expectEqualStrings("explicit-id", opts.backup_id);
        try std.testing.expectEqualStrings("file:///tmp/antfly_backups", opts.location);
    }

    {
        const argv = [_][*:0]const u8{ "--target", "http://localhost:8080", "--table", "docs", "--location" };
        var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
        try std.testing.expectError(error.MissingArgument, parsePromoteOptions(allocator, "app.aflite", &args));
    }

    {
        const argv = [_][*:0]const u8{ "--target", "http://localhost:8080", "--table", "docs", "--bogus" };
        var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
        try std.testing.expectError(error.UnknownArgument, parsePromoteOptions(allocator, "app.aflite", &args));
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

    try restoreFromSourceFile(allocator, io, src_path, dst_path, false);

    {
        var reopened = try LiteDb.open(allocator, dst_path, .query_readonly);
        defer reopened.close();
        const json = try lookupJson(allocator, &reopened.db, "doc:lite-restore", "");
        defer allocator.free(json);
        try std.testing.expect(std.mem.indexOf(u8, json, "\"from aflite source\"") != null);
    }
}

test "lite restore publishes staged aflite from aflite source" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const src_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/restore-publish-source.aflite", .{tmp.sub_path});
    defer allocator.free(src_path);
    const dst_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/restore-publish-dst.aflite", .{tmp.sub_path});
    defer allocator.free(dst_path);
    const tmp_path = try restoreTempPathAlloc(allocator, dst_path);
    defer allocator.free(tmp_path);

    {
        var source = try LiteDb.create(allocator, src_path, true);
        defer source.close();
        const json = try batchJson(allocator, &source.db, "{\"inserts\":{\"doc:publish\":{\"title\":\"published from staged restore\"}}}");
        defer allocator.free(json);
        try std.testing.expect(std.mem.indexOf(u8, json, "\"inserted\":1") != null);
    }

    try restoreFromSourceFile(allocator, io, src_path, dst_path, false);
    try std.testing.expect(pathExists(io, dst_path));
    try std.testing.expect(!pathExists(io, tmp_path));

    {
        var restored = try LiteDb.open(allocator, dst_path, .query_readonly);
        defer restored.close();
        const json = try lookupJson(allocator, &restored.db, "doc:publish", "");
        defer allocator.free(json);
        try std.testing.expect(std.mem.indexOf(u8, json, "\"published from staged restore\"") != null);
    }
}

test "lite backup output restores schema indexes enrichments and documents" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const src_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/backup-roundtrip-src.aflite", .{tmp.sub_path});
    defer allocator.free(src_path);
    const backup_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/backup-roundtrip.afb", .{tmp.sub_path});
    defer allocator.free(backup_path);
    const dst_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/backup-roundtrip-dst.aflite", .{tmp.sub_path});
    defer allocator.free(dst_path);
    const import_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/backup-import-empty.aflite", .{tmp.sub_path});
    defer allocator.free(import_path);
    const nonempty_import_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/backup-import-nonempty.aflite", .{tmp.sub_path});
    defer allocator.free(nonempty_import_path);

    {
        var source = try LiteDb.create(allocator, src_path, true);
        defer source.close();

        try source.db.setSchemaJson(allocator,
            \\{"version":1,"default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","required":["title"]}}}}
        );
        try source.db.addEnrichment(.{
            .name = "body_chunks_v1",
            .kind = .chunk,
            .field = "body",
            .chunk_size = 8,
            .chunk_overlap = 2,
        });
        try source.db.addIndex(.{
            .name = "ft_body_v1",
            .kind = .full_text,
            .config_json = "{\"chunk_name\":\"body_chunks_v1\"}",
        });
        try source.db.addIndex(.{
            .name = "ft_direct_v1",
            .kind = .full_text,
            .config_json = "{}",
        });

        const json = try batchJson(allocator, &source.db, "{\"inserts\":{\"doc:backup-roundtrip\":{\"title\":\"portable backup restore\",\"body\":\"schema and catalog survive\"}}}");
        defer allocator.free(json);
        try std.testing.expect(std.mem.indexOf(u8, json, "\"inserted\":1") != null);
    }

    const src_path_z = try allocator.dupeZ(u8, src_path);
    defer allocator.free(src_path_z);
    const backup_path_z = try allocator.dupeZ(u8, backup_path);
    defer allocator.free(backup_path_z);
    const backup_argv = [_][*:0]const u8{ src_path_z.ptr, "--out", backup_path_z.ptr };
    var backup_args = std.process.Args.Iterator.init(.{ .vector = backup_argv[0..] });
    try backup(allocator, io, &backup_args);
    try std.testing.expect(pathExists(io, backup_path));

    try restoreFromSourceFile(allocator, io, backup_path, dst_path, false);

    {
        var restored = try LiteDb.open(allocator, dst_path, .status_only);
        defer restored.close();

        const schema = (try restored.db.getSchemaJson(allocator)) orelse return error.MissingLiteSchemaJson;
        defer allocator.free(schema);
        try std.testing.expect(std.mem.indexOf(u8, schema, "\"required\":[\"title\"]") != null);

        const indexes = try restored.db.listIndexes(allocator);
        defer db_types.freeIndexConfigs(allocator, indexes);
        try std.testing.expectEqual(@as(usize, 2), indexes.len);
        var saw_chunk_index = false;
        var saw_direct_index = false;
        for (indexes) |index| {
            if (std.mem.eql(u8, index.name, "ft_body_v1")) {
                saw_chunk_index = true;
                try std.testing.expectEqual(db_types.IndexKind.full_text, index.kind);
                try std.testing.expect(std.mem.indexOf(u8, index.config_json, "body_chunks_v1") != null);
            } else if (std.mem.eql(u8, index.name, "ft_direct_v1")) {
                saw_direct_index = true;
                try std.testing.expectEqual(db_types.IndexKind.full_text, index.kind);
            }
        }
        try std.testing.expect(saw_chunk_index);
        try std.testing.expect(saw_direct_index);

        const enrichments = try restored.db.listEnrichments(allocator);
        defer db_types.freeEnrichmentConfigs(allocator, enrichments);
        try std.testing.expectEqual(@as(usize, 1), enrichments.len);
        try std.testing.expectEqualStrings("body_chunks_v1", enrichments[0].name);
        try std.testing.expectEqual(db_types.EnrichmentKind.chunk, enrichments[0].kind);
        try std.testing.expectEqualStrings("body", enrichments[0].field);
    }

    {
        var restored = try LiteDb.open(allocator, dst_path, .query_readonly);
        defer restored.close();
        const json = try lookupJson(allocator, &restored.db, "doc:backup-roundtrip", "");
        defer allocator.free(json);
        try std.testing.expect(std.mem.indexOf(u8, json, "\"portable backup restore\"") != null);
    }

    {
        var empty_target = try LiteDb.create(allocator, import_path, true);
        empty_target.close();
    }
    try importPortableIntoExistingLite(allocator, io, backup_path, import_path);

    {
        var imported = try LiteDb.open(allocator, import_path, .status_only);
        defer imported.close();

        const schema = (try imported.db.getSchemaJson(allocator)) orelse return error.MissingLiteSchemaJson;
        defer allocator.free(schema);
        try std.testing.expect(std.mem.indexOf(u8, schema, "\"required\":[\"title\"]") != null);

        const indexes = try imported.db.listIndexes(allocator);
        defer db_types.freeIndexConfigs(allocator, indexes);
        try std.testing.expectEqual(@as(usize, 2), indexes.len);

        const enrichments = try imported.db.listEnrichments(allocator);
        defer db_types.freeEnrichmentConfigs(allocator, enrichments);
        try std.testing.expectEqual(@as(usize, 1), enrichments.len);
        try std.testing.expectEqualStrings("body_chunks_v1", enrichments[0].name);
    }

    {
        var nonempty = try LiteDb.create(allocator, nonempty_import_path, true);
        defer nonempty.close();
        const json = try batchJson(allocator, &nonempty.db, "{\"inserts\":{\"doc:existing\":{\"title\":\"existing import target\"}}}");
        defer allocator.free(json);
        try std.testing.expect(std.mem.indexOf(u8, json, "\"inserted\":1") != null);
    }
    try std.testing.expectError(error.LiteImportTargetNotEmpty, importPortableIntoExistingLite(allocator, io, backup_path, nonempty_import_path));

    {
        var nonempty = try LiteDb.open(allocator, nonempty_import_path, .query_readonly);
        defer nonempty.close();
        const json = try lookupJson(allocator, &nonempty.db, "doc:existing", "");
        defer allocator.free(json);
        try std.testing.expect(std.mem.indexOf(u8, json, "\"existing import target\"") != null);
    }
}

test "lite restore replace fails before truncating active writer target" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const src_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/restore-replace-source.aflite", .{tmp.sub_path});
    defer allocator.free(src_path);
    const dst_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/restore-replace-dst.aflite", .{tmp.sub_path});
    defer allocator.free(dst_path);

    {
        var source = try LiteDb.create(allocator, src_path, true);
        defer source.close();
        const json = try batchJson(allocator, &source.db, "{\"inserts\":{\"doc:source\":{\"title\":\"source\"}}}");
        defer allocator.free(json);
    }

    var target = try LiteDb.create(allocator, dst_path, true);
    errdefer target.close();
    {
        const json = try batchJson(allocator, &target.db, "{\"inserts\":{\"doc:target\":{\"title\":\"target survives\"}}}");
        defer allocator.free(json);
    }

    try std.testing.expectError(error.WouldBlock, restoreFromSourceFile(allocator, io, src_path, dst_path, true));

    const live_json = try lookupJson(allocator, &target.db, "doc:target", "");
    defer allocator.free(live_json);
    try std.testing.expect(std.mem.indexOf(u8, live_json, "\"target survives\"") != null);
    target.close();

    var reopened = try LiteDb.open(allocator, dst_path, .query_readonly);
    defer reopened.close();
    const reopened_json = try lookupJson(allocator, &reopened.db, "doc:target", "");
    defer allocator.free(reopened_json);
    try std.testing.expect(std.mem.indexOf(u8, reopened_json, "\"target survives\"") != null);
}

test "lite restore rejects same existing aflite through different path spelling" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const src_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/restore-self.aflite", .{tmp.sub_path});
    defer allocator.free(src_path);
    const nested_dir = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/nested", .{tmp.sub_path});
    defer allocator.free(nested_dir);
    const alias_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/nested/../restore-self.aflite", .{tmp.sub_path});
    defer allocator.free(alias_path);

    try fs_paths.createDirPathPortable(io, nested_dir);
    {
        var source = try LiteDb.create(allocator, src_path, true);
        defer source.close();
        const json = try batchJson(allocator, &source.db, "{\"inserts\":{\"doc:self\":{\"title\":\"self restore rejected\"}}}");
        defer allocator.free(json);
    }

    try std.testing.expectError(error.InvalidArguments, restoreFromSourceFile(allocator, io, src_path, alias_path, true));

    var reopened = try LiteDb.open(allocator, src_path, .query_readonly);
    defer reopened.close();
    const json = try lookupJson(allocator, &reopened.db, "doc:self", "");
    defer allocator.free(json);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"self restore rejected\"") != null);
}

test "lite restore malformed backup leaves target untouched" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const backup_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/malformed.afb", .{tmp.sub_path});
    defer allocator.free(backup_path);
    const logical_malformed_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/logical-malformed.afb", .{tmp.sub_path});
    defer allocator.free(logical_malformed_path);
    const dst_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/restore-malformed-dst.aflite", .{tmp.sub_path});
    defer allocator.free(dst_path);
    const missing_dst_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/restore-malformed-missing.aflite", .{tmp.sub_path});
    defer allocator.free(missing_dst_path);
    const dst_tmp_path = try restoreTempPathAlloc(allocator, dst_path);
    defer allocator.free(dst_tmp_path);
    const missing_tmp_path = try restoreTempPathAlloc(allocator, missing_dst_path);
    defer allocator.free(missing_tmp_path);

    try writeFileAtomically(allocator, io, backup_path, "not an afb");
    {
        var malformed = std.ArrayList(u8).empty;
        defer malformed.deinit(allocator);
        try backup_codec.writeHeader(&malformed, allocator, .{
            .format_version = backup_codec.format_version,
            .flags = 0,
            .created_at_ns = 0,
            .backup_id = [_]u8{0} ** 16,
            .table_count = 1,
            .shard_count = 1,
        });
        const malformed_doc_payload = [_]u8{ 1, 0, 0, 0 };
        try backup_codec.writeBlock(&malformed, allocator, .document_batch, &malformed_doc_payload);
        try writeFileAtomically(allocator, io, logical_malformed_path, malformed.items);
    }
    {
        var target = try LiteDb.create(allocator, dst_path, true);
        defer target.close();
        const json = try batchJson(allocator, &target.db, "{\"inserts\":{\"doc:target\":{\"title\":\"target survives malformed restore\"}}}");
        defer allocator.free(json);
    }

    try std.testing.expectError(error.EndOfStream, restoreFromSourceFile(allocator, io, backup_path, dst_path, true));
    try std.testing.expect(!pathExists(io, dst_tmp_path));

    {
        var reopened = try LiteDb.open(allocator, dst_path, .query_readonly);
        defer reopened.close();
        const json = try lookupJson(allocator, &reopened.db, "doc:target", "");
        defer allocator.free(json);
        try std.testing.expect(std.mem.indexOf(u8, json, "\"target survives malformed restore\"") != null);
    }

    try std.testing.expectError(error.Truncated, restoreFromSourceFile(allocator, io, logical_malformed_path, dst_path, true));
    try std.testing.expect(!pathExists(io, dst_tmp_path));

    {
        var reopened = try LiteDb.open(allocator, dst_path, .query_readonly);
        defer reopened.close();
        const json = try lookupJson(allocator, &reopened.db, "doc:target", "");
        defer allocator.free(json);
        try std.testing.expect(std.mem.indexOf(u8, json, "\"target survives malformed restore\"") != null);
    }

    try std.testing.expectError(error.EndOfStream, restoreFromSourceFile(allocator, io, backup_path, missing_dst_path, false));
    try std.testing.expect(!pathExists(io, missing_dst_path));
    try std.testing.expect(!pathExists(io, missing_tmp_path));
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
    try std.testing.expect(std.mem.indexOf(u8, json, "\"inference\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"mode\":\"caller_supplied_or_disabled\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"configured\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"remote_provider_configured\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"local_runtime_configured\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"local_runtime_available\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"capabilities\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"manual_maintenance\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"background_enrichment_runtime\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"inference_mode\":\"caller_supplied_or_disabled\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"no_inference_configured_ok\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"caller_supplied_artifacts\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"local_inference_runtime\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"raft_replication\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"cluster_placement\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"cross_node_joins\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"remote_shard_fanout\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"distributed_transaction_coordination\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"cluster_heartbeat_status_aggregation\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"has_async_indexes\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"derived_target_sequence\":") != null);
}

test "lite status rejects internal bridge aflite files" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/status-internal-bridge.aflite", .{tmp.sub_path});
    defer allocator.free(path);
    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);

    {
        var handle = try antfly.lite.backend.Handle.open(allocator, path, .{ .engine = .bridge_lsm_container });
        defer handle.deinit();
    }

    var argv = [_][*:0]const u8{path_z.ptr};
    var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    try std.testing.expectError(error.TruncatedNativeHeader, status(allocator, std.testing.io, &args));
}

fn testLiteHttpCall(
    allocator: Allocator,
    state: ?*LiteHttpState,
    comptime handler: httpx.Handler,
    method: httpx.Method,
    url: []const u8,
    body: []const u8,
    params: []const @import("httpx").router.RouteParam,
) !httpx.Response {
    var req = try httpx.Request.init(allocator, method, url);
    defer req.deinit();
    if (body.len > 0) try req.setBody(body);

    var ctx = httpx.Context.init(allocator, std.testing.io, &req);
    defer ctx.deinit();
    ctx.params = params;
    if (state) |s| try ctx.setData(lite_http_state_key, s, null);
    return try handler(&ctx);
}

fn testLiteHttpBody(resp: *const httpx.Response) []const u8 {
    return resp.body orelse "";
}

test "lite http handlers expose narrow embedded api" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/http-api.aflite", .{tmp.sub_path});
    defer allocator.free(path);

    var lite = try LiteDb.create(allocator, path, true);
    defer lite.close();

    var state = LiteHttpState{ .lite = &lite };
    {
        var resp = try testLiteHttpCall(allocator, null, liteHttpHealth, .GET, "http://lite.test/healthz", "", &.{});
        defer resp.deinit();
        try std.testing.expectEqual(@as(u16, 200), resp.status.code);
        try std.testing.expect(std.mem.indexOf(u8, testLiteHttpBody(&resp), "\"status\":\"ok\"") != null);
    }
    {
        var resp = try testLiteHttpCall(
            allocator,
            &state,
            liteHttpBatch,
            .POST,
            "http://lite.test/lite/v1/batch",
            "{\"inserts\":{\"doc:http\":{\"title\":\"served lite\"}}}",
            &.{},
        );
        defer resp.deinit();
        try std.testing.expectEqual(@as(u16, 200), resp.status.code);
        try std.testing.expect(std.mem.indexOf(u8, testLiteHttpBody(&resp), "\"inserted\":1") != null);
    }
    {
        const params = [_]@import("httpx").router.RouteParam{.{ .name = "key", .value = "doc:http" }};
        var resp = try testLiteHttpCall(
            allocator,
            &state,
            liteHttpLookup,
            .POST,
            "http://lite.test/lite/v1/lookup/doc:http",
            "{}",
            params[0..],
        );
        defer resp.deinit();
        try std.testing.expectEqual(@as(u16, 200), resp.status.code);
        try std.testing.expect(std.mem.indexOf(u8, testLiteHttpBody(&resp), "\"served lite\"") != null);
    }
    {
        var resp = try testLiteHttpCall(allocator, &state, liteHttpStatus, .GET, "http://lite.test/lite/v1/status", "", &.{});
        defer resp.deinit();
        try std.testing.expectEqual(@as(u16, 200), resp.status.code);
        try std.testing.expect(std.mem.indexOf(u8, testLiteHttpBody(&resp), "\"format\":\"aflite\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, testLiteHttpBody(&resp), "\"engine\":\"native_single_file\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, testLiteHttpBody(&resp), "\"capabilities\":") != null);
    }
    {
        var req = try httpx.Request.init(allocator, .GET, "http://lite.test/lite/v1/status");
        defer req.deinit();

        var ctx = httpx.Context.init(allocator, std.testing.io, &req);
        defer ctx.deinit();

        active_lite_http_state = &state;
        defer active_lite_http_state = null;
        try liteHttpInjectState(&ctx);
        try std.testing.expect(ctx.getData(lite_http_state_key) != null);

        var resp = try liteHttpStatus(&ctx);
        defer resp.deinit();
        try std.testing.expectEqual(@as(u16, 200), resp.status.code);
        try std.testing.expect(std.mem.indexOf(u8, testLiteHttpBody(&resp), "\"format\":\"aflite\"") != null);
    }
}

test "lite full text query survives writer close and readonly reopen" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/query-reopen.aflite", .{tmp.sub_path});
    defer allocator.free(path);

    {
        var lite = try LiteDb.create(allocator, path, true);
        defer lite.close();

        try lite.db.addIndex(.{
            .name = "ft_body",
            .kind = .full_text,
            .config_json = "{}",
        });

        const batch_response = try batchJson(allocator, &lite.db,
            \\{"inserts":{"doc:query-reopen":{"body":"native lite full text"}},"sync_level":"full_index"}
        );
        defer allocator.free(batch_response);
        try std.testing.expect(std.mem.indexOf(u8, batch_response, "\"inserted\":1") != null);
    }

    {
        var reopened = try LiteDb.open(allocator, path, .query_readonly);
        defer reopened.close();

        const query_response = try searchJson(allocator, &reopened.db,
            \\{"full_text_search":{"match":{"field":"body","text":"full text"}},"limit":1}
        );
        defer allocator.free(query_response);
        try std.testing.expect(std.mem.indexOf(u8, query_response, "\"doc:query-reopen\"") != null);
    }
}

test "lite compact drains text merges before vacuuming aflite file" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/compact-text.aflite", .{tmp.sub_path});
    defer allocator.free(path);

    {
        var lite = try LiteDb.create(allocator, path, true);
        defer lite.close();

        try lite.db.addIndex(.{
            .name = "ft_body",
            .kind = .full_text,
            .config_json = "{}",
        });

        const batch_response = try batchJson(allocator, &lite.db,
            \\{"inserts":{"doc:compact:a":{"body":"native lite compact alpha"},"doc:compact:b":{"body":"native lite compact beta"}},"sync_level":"full_index"}
        );
        defer allocator.free(batch_response);
        try std.testing.expect(std.mem.indexOf(u8, batch_response, "\"inserted\":2") != null);

        const report = try compactLite(&lite);
        try std.testing.expect(report.compacted);
        try std.testing.expect(report.vacuum.after_size <= report.vacuum.before_size);
    }

    {
        var reopened = try LiteDb.open(allocator, path, .query_readonly);
        defer reopened.close();

        const query_response = try searchJson(allocator, &reopened.db,
            \\{"full_text_search":{"match":{"field":"body","text":"compact alpha"}},"limit":1}
        );
        defer allocator.free(query_response);
        try std.testing.expect(std.mem.indexOf(u8, query_response, "\"doc:compact:a\"") != null);
    }
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
