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
const antfly = @import("../cli_root.zig");
const antfly_client = @import("antfly-client");
const cli = @import("cli/mod.zig");
const httpx = @import("httpx");
const runtime_bridge = @import("../runtime_bridge.zig");
const kernel_owner_abi = @import("kernel_owner_abi");
const local_query_client = @import("local_query_client");
const standalone_runtime = if (antfly.build_options.storage_kernel_experiment)
    struct {}
else
    @import("../standalone/runtime.zig");
const fs_paths = antfly.common.fs_paths;

extern fn antfly_runtime_standalone_lite(context: *const runtime_bridge.LiteServeContext) callconv(.c) c_int;

const Allocator = std.mem.Allocator;
const max_json_file_bytes: usize = 64 * 1024 * 1024;

const db_mod = antfly.db;
const db_types = db_mod.types;
const batch_api = antfly.public_api.batch;
const query_api = antfly.public_api.query;
const backup_codec = antfly.backup_codec;
const portable_backup = antfly.portable_backup;
const lite_paths = antfly.lite.paths;
const lite_restore_staging = antfly.lite.restore_staging;
const LiteDb = antfly.lite.connection.Connection;

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

    return try dispatchSubcommand(init, argv0, subcommand, args);
}

fn dispatchSubcommand(init: std.process.Init, argv0: []const u8, subcommand: []const u8, args: *std.process.Args.Iterator) !void {
    const allocator = init.gpa;
    const io = init.io;
    if (std.mem.eql(u8, subcommand, "init")) return try initLite(allocator, io, args);
    if (isStatusSubcommand(subcommand)) return try status(allocator, io, args);
    if (std.mem.eql(u8, subcommand, "batch")) return try batch(allocator, io, args);
    if (std.mem.eql(u8, subcommand, "lookup")) return try lookup(allocator, io, args);
    if (std.mem.eql(u8, subcommand, "scan")) return try scan(allocator, io, args);
    if (std.mem.eql(u8, subcommand, "query")) return try query(allocator, io, args);
    if (std.mem.eql(u8, subcommand, "index")) return try indexCommand(allocator, io, args);
    if (std.mem.eql(u8, subcommand, "enrichment")) return try enrichmentCommand(allocator, io, args);
    if (std.mem.eql(u8, subcommand, "schema")) return try schemaCommand(allocator, io, args);
    if (std.mem.eql(u8, subcommand, "run-until-idle")) return try runUntilIdle(allocator, io, args);
    if (std.mem.eql(u8, subcommand, "backup") or std.mem.eql(u8, subcommand, "export")) return try backup(allocator, io, args);
    if (std.mem.eql(u8, subcommand, "snapshot")) return try snapshot(allocator, io, args);
    if (std.mem.eql(u8, subcommand, "restore")) return try restore(allocator, io, args);
    if (std.mem.eql(u8, subcommand, "import")) return try importBackup(allocator, io, args);
    if (std.mem.eql(u8, subcommand, "promote")) return try promote(allocator, io, args);
    if (std.mem.eql(u8, subcommand, "check")) return try check(allocator, io, args);
    if (std.mem.eql(u8, subcommand, "compact")) return try compact(allocator, io, args);
    if (std.mem.eql(u8, subcommand, "vacuum")) return try vacuum(allocator, io, args);
    if (std.mem.eql(u8, subcommand, "serve")) return try serve(init, args);

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

fn openEmbeddedDataSurface(allocator: Allocator, path: []const u8, mode: db_mod.OpenOptions.OpenMode) !LiteDb {
    var lite = try LiteDb.open(allocator, path, mode);
    errdefer lite.close();
    if (try lite.backend.isStandaloneArtifact() and !lite.backend.hasStandaloneRootAdoption()) {
        std.debug.print("error: this Lite artifact contains standalone table namespaces; use antfly lite serve and the /db/v1 API\n", .{});
        return error.StandaloneLiteRequiresApi;
    }
    return lite;
}

fn batch(allocator: Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    const path = args.next() orelse cli.fatal("database path is required", .{});
    try requireAflitePath(path);
    const file_path = parseFileFlag(args);

    const body = try cli.readFileAlloc(io, allocator, file_path, max_json_file_bytes);
    defer allocator.free(body);

    var lite = try openEmbeddedDataSurface(allocator, path, .writer);
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

    var lite = try openEmbeddedDataSurface(allocator, path, .query_readonly);
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

    var lite = try openEmbeddedDataSurface(allocator, path, .query_readonly);
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

    var lite = try openEmbeddedDataSurface(allocator, path, .query_readonly);
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

    var lite = try openEmbeddedDataSurface(allocator, path, .status_only);
    defer lite.close();

    const configs = try lite.db.listIndexes(allocator);
    defer db_types.freeIndexConfigs(allocator, configs);
    const public_configs = try db_types.publicIndexConfigsAlloc(allocator, configs);
    defer allocator.free(public_configs);
    const json = try std.json.Stringify.valueAlloc(allocator, public_configs, .{});
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
    parsed.value.coverage_generation = 0;

    var lite = try openEmbeddedDataSurface(allocator, path, .writer);
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

    var lite = try openEmbeddedDataSurface(allocator, path, .writer);
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

    var lite = try openEmbeddedDataSurface(allocator, path, .status_only);
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

    var lite = try openEmbeddedDataSurface(allocator, path, .writer);
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

    var lite = try openEmbeddedDataSurface(allocator, path, .writer);
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

    var lite = try openEmbeddedDataSurface(allocator, path, .status_only);
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

    var lite = try openEmbeddedDataSurface(allocator, path, .writer);
    defer lite.close();

    try lite.db.setSchemaJson(allocator, body);
    writeJsonLine(io, "{\"updated\":true}");
}

fn runUntilIdle(allocator: Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    const path = args.next() orelse cli.fatal("database path is required", .{});
    try requireAflitePath(path);
    requireNoMoreArgs(args);

    var lite = try openEmbeddedDataSurface(allocator, path, .writer);
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
    ensureOfflinePortableBackupSupported(&lite) catch |err| {
        std.debug.print("error: standalone Lite databases must use the authenticated /db/v1 backup API; use 'antfly lite snapshot' for a complete physical copy\n", .{});
        return err;
    };

    var out = std.ArrayList(u8).empty;
    defer out.deinit(allocator);
    try portable_backup.exportPortable(allocator, lite.db.core.store, &out);
    try portable_backup.validatePortable(allocator, out.items);
    try writeFileAtomically(allocator, io, out_path, out.items);

    cli.writeStdout(io, "{\"format\":\"afb\",\"path\":");
    try writeJsonString(allocator, io, out_path);
    cli.writeStdout(io, "}\n");
}

fn ensureOfflinePortableBackupSupported(lite: *const LiteDb) !void {
    if (lite.backend.hasStandaloneRootAdoption() or try lite.backend.isStandaloneArtifact()) {
        return error.StandaloneLitePortableBackupRequiresApi;
    }
}

fn ensureOfflineRootOnlyOperationSupported(lite: *const LiteDb) !void {
    if (try lite.backend.isStandaloneArtifact()) return error.StandaloneLiteRequiresApi;
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
    if (std.mem.eql(u8, path, out_path) or try pathsReferToSameExistingFile(allocator, io, path, out_path)) {
        std.debug.print("error: source and output snapshot paths must be different: {s}\n", .{path});
        return error.InvalidArguments;
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
    importFromSourceFile(allocator, io, resolved_from, path, replace) catch |err| switch (err) {
        error.AfliteImportRequiresReplace => cli.fatal("importing a .aflite snapshot over an existing target requires --replace", .{}),
        error.LiteImportTargetNotEmpty => cli.fatal("target database is not empty; pass --replace to replace it: {s}", .{path}),
        else => return err,
    };
}

fn importFromSourceFile(
    allocator: Allocator,
    io: std.Io,
    source_path: []const u8,
    target_path: []const u8,
    replace: bool,
) !void {
    try requireRestoreSourcePath(source_path);
    try requireAflitePath(target_path);
    if (!replace and pathExists(io, target_path)) {
        if (!std.mem.endsWith(u8, source_path, ".afb")) {
            return error.AfliteImportRequiresReplace;
        }
        try importPortableIntoExistingLite(allocator, io, source_path, target_path);
        return;
    }
    try restoreFromSourceFile(allocator, io, source_path, target_path, replace);
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

    var target_lock = try antfly.lite.native.lockWriterPath(allocator, target_path);
    defer target_lock.close();

    {
        var lite = try LiteDb.open(allocator, target_path, .status_only);
        defer lite.close();
        try ensureOfflineRootOnlyOperationSupported(&lite);
        if (!(try lite_restore_staging.isImportTargetEmpty(allocator, &lite.db))) {
            return error.LiteImportTargetNotEmpty;
        }
    }

    try publishRestoreFromSourceFileLocked(allocator, io, source_path, target_path);
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

    var target_lock = try antfly.lite.native.lockWriterPath(allocator, out_path);
    defer target_lock.close();

    if (!target_exists and !replace and pathExists(io, out_path)) {
        cli.fatal("output database already exists; pass --replace to overwrite: {s}", .{out_path});
    }

    try publishRestoreFromSourceFileLocked(allocator, io, source_path, out_path);
}

fn publishRestoreFromSourceFileLocked(
    allocator: Allocator,
    io: std.Io,
    source_path: []const u8,
    out_path: []const u8,
) !void {
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

        const json = try restoreResultJsonAlloc(allocator, "snapshot", "aflite", out_path);
        defer allocator.free(json);
        writeJsonLine(io, json);
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
        try lite_restore_staging.importPortableIntoLiteDb(allocator, &lite.db, body);
    }

    renameFilePath(io, tmp_path, out_path) catch |err| {
        deleteFilePath(io, tmp_path) catch {};
        return err;
    };

    const json = try restoreResultJsonAlloc(allocator, "portable_restore", "afb", out_path);
    defer allocator.free(json);
    writeJsonLine(io, json);
}

fn restoreResultJsonAlloc(
    allocator: Allocator,
    operation: []const u8,
    source_format: []const u8,
    path: []const u8,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(allocator);

    try out.appendSlice(allocator, "{\"format\":\"aflite\",\"operation\":");
    try appendJsonString(allocator, &out, operation);
    try out.appendSlice(allocator, ",\"source_format\":");
    try appendJsonString(allocator, &out, source_format);
    try out.appendSlice(allocator, ",\"path\":");
    try appendJsonString(allocator, &out, path);
    try out.append(allocator, '}');
    return try out.toOwnedSlice(allocator);
}

fn readPortableRestoreSourceAlloc(allocator: Allocator, io: std.Io, source_path: []const u8) ![]u8 {
    if (std.mem.endsWith(u8, source_path, ".afb")) {
        return try cli.readFileAlloc(io, allocator, source_path, max_afb_file_bytes);
    }
    return error.InvalidArguments;
}

const PromoteOptions = struct {
    target: []const u8,
    table: []const u8,
    backup_id: []const u8,
    location: []const u8,
    connection: []const u8,
    idempotency_key: ?[]const u8 = null,
    wait: bool = true,
    wait_timeout_ms: u64 = 30 * 60 * 1000,

    fn deinit(self: *PromoteOptions, allocator: Allocator) void {
        allocator.free(self.backup_id);
        allocator.free(self.location);
        self.* = undefined;
    }
};

const PromoteRestoreAcceptance = struct {
    job_id: []u8,
    encoded: []u8,
};

const PromoteRestoreFn = *const fn (ctx: *anyopaque, allocator: Allocator, table: []const u8, request: antfly_client.types.RestoreRequest, idempotency_key: ?[]const u8) anyerror!PromoteRestoreAcceptance;

const PromoteSubmission = struct {
    staged: lite_restore_staging.StagedRestore,
    restore_job_id: []u8,
    accepted_json: []u8,

    fn deinit(self: *PromoteSubmission, allocator: Allocator) void {
        self.staged.deinit(allocator);
        allocator.free(self.restore_job_id);
        allocator.free(self.accepted_json);
        self.* = undefined;
    }
};

fn promote(allocator: Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    const path = args.next() orelse cli.fatal("database path is required", .{});
    try requireAflitePath(path);

    {
        var lite = try LiteDb.open(allocator, path, .status_only);
        defer lite.close();
        try ensureOfflineRootOnlyOperationSupported(&lite);
    }

    const opts = parsePromoteOptions(allocator, path, args) catch cli.fatal("invalid promote arguments", .{});
    defer {
        var owned_opts = opts;
        owned_opts.deinit(allocator);
    }

    const global_config = cli.parseGlobalFlags();
    var http = httpx.Client.initWithConfig(allocator, io, .{});
    defer http.deinit();
    var client = try cli.initClient(allocator, &http, global_config);
    defer client.deinit();
    try client.setBaseUrl(opts.target);

    var submission = try promoteWithRestore(allocator, path, opts, &client, promoteRestoreWithClient);
    defer submission.deinit(allocator);

    if (!opts.wait) {
        writeJsonLine(io, submission.accepted_json);
        return;
    }

    var response = try cli.backup.waitForRestoreJob(&client, io, submission.restore_job_id, opts.wait_timeout_ms);
    defer response.deinit();
    const job = if (response.data) |*data| data.value else return error.InvalidRestoreResponse;
    try cli.writeJson(allocator, io, job);
    try cli.backup.restorePhaseResult(job.phase);
}

fn promoteWithRestore(
    allocator: Allocator,
    path: []const u8,
    opts: PromoteOptions,
    restore_ctx: *anyopaque,
    restore_fn: PromoteRestoreFn,
) !PromoteSubmission {
    var staged = try lite_restore_staging.stageAfliteRestoreBackup(allocator, path, opts.table, opts.backup_id, opts.location);
    errdefer staged.deinit(allocator);

    const accepted = try restore_fn(restore_ctx, allocator, opts.table, .{
        .backup_id = staged.backup_id,
        .location = staged.location,
        .connection = opts.connection,
    }, opts.idempotency_key);
    errdefer {
        allocator.free(accepted.job_id);
        allocator.free(accepted.encoded);
    }

    return .{ .staged = staged, .restore_job_id = accepted.job_id, .accepted_json = accepted.encoded };
}

fn promoteRestoreWithClient(ctx: *anyopaque, allocator: Allocator, table: []const u8, request: antfly_client.types.RestoreRequest, idempotency_key: ?[]const u8) !PromoteRestoreAcceptance {
    const client: *antfly_client.AntflyClient = @ptrCast(@alignCast(ctx));
    var resp = try client.restoreTableWithOptions(table, request, .{ .idempotency_key = idempotency_key });
    defer resp.deinit();
    cli.expectHttpSuccess(resp);
    const job = if (resp.data) |*data| data.value else return error.InvalidRestoreResponse;
    const job_id = try allocator.dupe(u8, job.job_id);
    errdefer allocator.free(job_id);
    const encoded = try std.json.Stringify.valueAlloc(allocator, job, .{});
    return .{ .job_id = job_id, .encoded = encoded };
}

fn parsePromoteOptions(allocator: Allocator, path: []const u8, args: *std.process.Args.Iterator) !PromoteOptions {
    var target: ?[]const u8 = null;
    var table: ?[]const u8 = null;
    var backup_id: ?[]const u8 = null;
    var location: ?[]const u8 = null;
    var connection: ?[]const u8 = null;
    var idempotency_key: ?[]const u8 = null;
    var wait = true;
    var wait_timeout_ms: u64 = 30 * 60 * 1000;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--target") or std.mem.eql(u8, arg, "--url")) {
            target = try nextRequiredArg(args);
        } else if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            table = try nextRequiredArg(args);
        } else if (std.mem.eql(u8, arg, "--backup-id")) {
            backup_id = try nextRequiredArg(args);
        } else if (std.mem.eql(u8, arg, "--location")) {
            location = try nextRequiredArg(args);
        } else if (std.mem.eql(u8, arg, "--connection")) {
            connection = try nextRequiredArg(args);
        } else if (std.mem.eql(u8, arg, "--idempotency-key")) {
            idempotency_key = try nextRequiredArg(args);
        } else if (std.mem.eql(u8, arg, "--no-wait")) {
            wait = false;
        } else if (std.mem.eql(u8, arg, "--wait-timeout")) {
            const seconds = try std.fmt.parseUnsigned(u64, try nextRequiredArg(args), 10);
            wait_timeout_ms = try std.math.mul(u64, seconds, 1000);
            wait = true;
        } else {
            return error.UnknownArgument;
        }
    }
    const resolved_target = target orelse return error.MissingArgument;
    const resolved_table = table orelse return error.MissingArgument;
    const resolved_backup_id = if (backup_id) |id| try allocator.dupe(u8, id) else try lite_restore_staging.defaultBackupIdAlloc(allocator, path);
    errdefer allocator.free(resolved_backup_id);
    const resolved_location = try allocator.dupe(u8, location orelse return error.MissingArgument);
    return .{
        .target = resolved_target,
        .table = resolved_table,
        .backup_id = resolved_backup_id,
        .location = resolved_location,
        .connection = connection orelse return error.MissingArgument,
        .idempotency_key = idempotency_key,
        .wait = wait,
        .wait_timeout_ms = wait_timeout_ms,
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
    if (!report.valid) return error.LiteCheckFailed;
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
        try ensureOfflineRootOnlyOperationSupported(&lite);
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
    fsync: bool = true,
    standalone_args: std.ArrayListUnmanaged([]const u8) = .empty,

    fn deinit(self: *ServeOptions, alloc: std.mem.Allocator) void {
        self.standalone_args.deinit(alloc);
    }
};

const LiteListenAddress = struct {
    host: []const u8,
    port: u16,
};

fn serve(init: std.process.Init, args: *std.process.Args.Iterator) !void {
    var opts = try parseServeOptions(init.gpa, args);
    defer opts.deinit(init.gpa);
    return try serveWithOptions(init, opts);
}

fn serveWithOptions(init: std.process.Init, opts: ServeOptions) !void {
    try requireAflitePath(opts.path);
    const listen = try parseLiteListenAddress(opts.addr);
    if (comptime antfly.build_options.storage_kernel_experiment) {
        const extra_args = try init.gpa.alloc(runtime_bridge.BorrowedBytes, opts.standalone_args.items.len);
        defer init.gpa.free(extra_args);
        for (opts.standalone_args.items, 0..) |arg, i| extra_args[i] = .fromSlice(arg);
        const code = antfly_runtime_standalone_lite(&.{
            .init = @ptrCast(&init),
            .path = .fromSlice(opts.path),
            .host = .fromSlice(listen.host),
            .extra_args = if (extra_args.len == 0) null else extra_args.ptr,
            .extra_args_len = extra_args.len,
            .port = listen.port,
            .fsync = @intFromBool(opts.fsync),
        });
        if (code != 0) return error.LinkedStandaloneStartupFailed;
        return;
    }
    return try standalone_runtime.runLite(init, opts.path, listen.host, listen.port, opts.fsync, opts.standalone_args.items);
}

fn parseServeOptions(alloc: std.mem.Allocator, args: *std.process.Args.Iterator) !ServeOptions {
    const path = args.next() orelse {
        std.debug.print("error: database path is required\n", .{});
        return error.InvalidArguments;
    };
    var opts: ServeOptions = .{ .path = path };
    errdefer opts.deinit(alloc);
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--addr")) {
            opts.addr = args.next() orelse {
                std.debug.print("error: --addr value is required\n", .{});
                return error.InvalidArguments;
            };
        } else if (std.mem.eql(u8, arg, "--fsync")) {
            opts.fsync = parseLiteBool(args.next() orelse return error.InvalidArguments) orelse return error.InvalidArguments;
        } else if (std.mem.startsWith(u8, arg, "--fsync=")) {
            opts.fsync = parseLiteBool(arg["--fsync=".len..]) orelse return error.InvalidArguments;
        } else if (isReservedLiteServeFlag(arg)) {
            std.debug.print("error: {s} is controlled by antfly lite serve\n", .{arg});
            return error.InvalidArguments;
        } else {
            try opts.standalone_args.append(alloc, arg);
        }
    }
    return opts;
}

fn isReservedLiteServeFlag(arg: []const u8) bool {
    for ([_][]const u8{ "--storage-engine", "--storage-path", "--host", "--port" }) |flag| {
        if (std.mem.eql(u8, arg, flag) or (arg.len > flag.len and std.mem.startsWith(u8, arg, flag) and arg[flag.len] == '=')) return true;
    }
    return false;
}

fn parseLiteBool(value: []const u8) ?bool {
    if (std.mem.eql(u8, value, "true")) return true;
    if (std.mem.eql(u8, value, "false")) return false;
    return null;
}

fn parseLiteListenAddress(addr: []const u8) !LiteListenAddress {
    const sep = std.mem.lastIndexOfScalar(u8, addr, ':') orelse return error.InvalidArguments;
    if (sep == 0 or sep + 1 >= addr.len) return error.InvalidArguments;
    const port = try std.fmt.parseInt(u16, addr[sep + 1 ..], 10);
    const host = addr[0..sep];
    if (!isLiteLocalListenHost(host)) return error.InvalidArguments;
    return .{ .host = host, .port = port };
}

fn isLiteLocalListenHost(host: []const u8) bool {
    return std.mem.eql(u8, host, "localhost") or
        std.mem.eql(u8, host, "127.0.0.1") or
        std.mem.eql(u8, host, "::1") or
        std.mem.eql(u8, host, "[::1]");
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
    if (comptime antfly.build_options.storage_kernel_experiment) {
        var failure: kernel_owner_abi.FailureIdentity = .{};
        const response = try local_query_client.executeJsonAlloc(
            allocator,
            @ptrCast(db),
            "docs",
            body,
            .public,
            .{},
            null,
            null,
            &failure,
        );
        return response.json;
    }

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
        \\  restore <backup.afb> --out <db.aflite> [--replace]
        \\  restore <source.aflite> --out <db.aflite> [--replace] (stable snapshot copy)
        \\  import <db.aflite> --from <backup.afb|source.aflite> [--replace]
        \\  promote <db.aflite> --target <url> --table <name> --connection <reader> --location <shared-uri> [--backup-id <id>] [--idempotency-key <key>] [--no-wait] [--wait-timeout <seconds>]
        \\  check <db.aflite>
        \\  compact <db.aflite>
        \\  vacuum <db.aflite>
        \\  serve <db.aflite> --addr 127.0.0.1:8080 [--fsync <true|false>] [standalone options]
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

test "lite serve parser preserves convenience flags and forwards standalone options" {
    {
        const argv = [_][*:0]const u8{"app.aflite"};
        var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
        var opts = try parseServeOptions(std.testing.allocator, &args);
        defer opts.deinit(std.testing.allocator);
        try std.testing.expectEqualStrings("app.aflite", opts.path);
        try std.testing.expectEqualStrings("127.0.0.1:8080", opts.addr);
        try std.testing.expect(opts.fsync);
        const listen = try parseLiteListenAddress(opts.addr);
        try std.testing.expectEqualStrings("127.0.0.1", listen.host);
        try std.testing.expectEqual(@as(u16, 8080), listen.port);
    }
    {
        const argv = [_][*:0]const u8{ "app.aflite", "--fsync=false" };
        var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
        var opts = try parseServeOptions(std.testing.allocator, &args);
        defer opts.deinit(std.testing.allocator);
        try std.testing.expect(!opts.fsync);
    }
    {
        const argv = [_][*:0]const u8{ "app.aflite", "--addr", "127.0.0.1:9090" };
        var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
        var opts = try parseServeOptions(std.testing.allocator, &args);
        defer opts.deinit(std.testing.allocator);
        try std.testing.expectEqualStrings("app.aflite", opts.path);
        try std.testing.expectEqualStrings("127.0.0.1:9090", opts.addr);
        const listen = try parseLiteListenAddress(opts.addr);
        try std.testing.expectEqualStrings("127.0.0.1", listen.host);
        try std.testing.expectEqual(@as(u16, 9090), listen.port);
    }
    {
        const argv = [_][*:0]const u8{ "app.aflite", "--port", "9090" };
        var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
        try std.testing.expectError(error.InvalidArguments, parseServeOptions(std.testing.allocator, &args));
    }
    {
        const argv = [_][*:0]const u8{ "app.aflite", "--config", "production.json", "--admin-api-token", "secret" };
        var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
        var opts = try parseServeOptions(std.testing.allocator, &args);
        defer opts.deinit(std.testing.allocator);
        try std.testing.expectEqualSlices([]const u8, &.{ "--config", "production.json", "--admin-api-token", "secret" }, opts.standalone_args.items);
    }
    try std.testing.expectError(error.InvalidArguments, parseLiteListenAddress("127.0.0.1"));
    try std.testing.expectError(error.InvalidArguments, parseLiteListenAddress(":8080"));
    try std.testing.expectError(error.InvalidArguments, parseLiteListenAddress("0.0.0.0:8080"));
    try std.testing.expectError(error.InvalidArguments, parseLiteListenAddress("192.168.1.10:8080"));
    try std.testing.expectError(error.InvalidArguments, parseLiteListenAddress("[::]:8080"));
    {
        const listen = try parseLiteListenAddress("localhost:8080");
        try std.testing.expectEqualStrings("localhost", listen.host);
        try std.testing.expectEqual(@as(u16, 8080), listen.port);
    }
}

test "lite promote parser requires values and derives default backup id" {
    const allocator = std.testing.allocator;

    {
        const argv = [_][*:0]const u8{ "--target", "http://localhost:8080", "--table", "docs", "--connection", "local-reader", "--location", "file:///tmp/backups" };
        var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
        var opts = try parsePromoteOptions(allocator, "app.aflite", &args);
        defer opts.deinit(allocator);
        try std.testing.expectEqualStrings("http://localhost:8080", opts.target);
        try std.testing.expectEqualStrings("docs", opts.table);
        try std.testing.expectEqualStrings("lite-app", opts.backup_id);
        try std.testing.expectEqualStrings("file:///tmp/backups", opts.location);
        try std.testing.expectEqualStrings("local-reader", opts.connection);
    }

    {
        const argv = [_][*:0]const u8{ "--target", "http://localhost:8080", "--table", "docs", "--connection", "local-reader", "--backup-id", "explicit-id", "--location", "s3://promotions/app", "--idempotency-key", "promote-app", "--no-wait" };
        var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
        var opts = try parsePromoteOptions(allocator, "app.aflite", &args);
        defer opts.deinit(allocator);
        try std.testing.expectEqualStrings("explicit-id", opts.backup_id);
        try std.testing.expectEqualStrings("s3://promotions/app", opts.location);
        try std.testing.expectEqualStrings("promote-app", opts.idempotency_key.?);
        try std.testing.expect(!opts.wait);
    }

    {
        const argv = [_][*:0]const u8{ "--target", "http://localhost:8080", "--table", "docs", "--connection", "local-reader" };
        var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
        try std.testing.expectError(error.MissingArgument, parsePromoteOptions(allocator, "app.aflite", &args));
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

test "lite schema index and enrichment commands round trip catalogs" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/catalog-commands.aflite", .{tmp.sub_path});
    defer allocator.free(path);
    const schema_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/catalog-schema.json", .{tmp.sub_path});
    defer allocator.free(schema_path);
    const index_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/catalog-index.json", .{tmp.sub_path});
    defer allocator.free(index_path);
    const enrichment_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/catalog-enrichment.json", .{tmp.sub_path});
    defer allocator.free(enrichment_path);

    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);
    const schema_path_z = try allocator.dupeZ(u8, schema_path);
    defer allocator.free(schema_path_z);
    const index_path_z = try allocator.dupeZ(u8, index_path);
    defer allocator.free(index_path_z);
    const enrichment_path_z = try allocator.dupeZ(u8, enrichment_path);
    defer allocator.free(enrichment_path_z);

    {
        const argv = [_][*:0]const u8{path_z.ptr};
        var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
        try initLite(allocator, io, &args);
    }

    const schema_json =
        \\{"version":0,"default_type":"doc","enforce_types":false,"document_schemas":{"doc":{"schema":{"type":"object","additionalProperties":true}}}}
    ;
    try writeFileAtomically(allocator, io, schema_path, schema_json);
    {
        const argv = [_][*:0]const u8{ "set", path_z.ptr, "--file", schema_path_z.ptr };
        var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
        try schemaCommand(allocator, io, &args);
    }
    {
        const argv = [_][*:0]const u8{ "get", path_z.ptr };
        var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
        try schemaCommand(allocator, io, &args);
    }

    try writeFileAtomically(allocator, io, index_path,
        \\{"name":"cmd_ft_body","kind":"full_text","config_json":"{}"}
    );
    {
        const argv = [_][*:0]const u8{ "create", path_z.ptr, "--file", index_path_z.ptr };
        var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
        try indexCommand(allocator, io, &args);
    }
    {
        const argv = [_][*:0]const u8{ "list", path_z.ptr };
        var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
        try indexCommand(allocator, io, &args);
    }

    try writeFileAtomically(allocator, io, enrichment_path,
        \\{"name":"cmd_body_chunks","kind":"chunk","field":"body","chunk_size":64,"chunk_overlap":8}
    );
    {
        const argv = [_][*:0]const u8{ "create", path_z.ptr, "--file", enrichment_path_z.ptr };
        var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
        try enrichmentCommand(allocator, io, &args);
    }
    {
        const argv = [_][*:0]const u8{ "list", path_z.ptr };
        var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
        try enrichmentCommand(allocator, io, &args);
    }

    {
        var lite = try LiteDb.open(allocator, path, .status_only);
        defer lite.close();

        const schema = (try lite.db.getSchemaJson(allocator)) orelse return error.MissingLiteSchemaJson;
        defer allocator.free(schema);
        try std.testing.expectEqualStrings(schema_json, schema);

        const indexes = try lite.db.listIndexes(allocator);
        defer db_types.freeIndexConfigs(allocator, indexes);
        try std.testing.expectEqual(@as(usize, 1), indexes.len);
        try std.testing.expectEqualStrings("cmd_ft_body", indexes[0].name);

        const enrichments = try lite.db.listEnrichments(allocator);
        defer db_types.freeEnrichmentConfigs(allocator, enrichments);
        try std.testing.expectEqual(@as(usize, 1), enrichments.len);
        try std.testing.expectEqualStrings("cmd_body_chunks", enrichments[0].name);
        try std.testing.expectEqual(db_types.EnrichmentKind.chunk, enrichments[0].kind);
    }

    {
        const argv = [_][*:0]const u8{ "drop", path_z.ptr, "--index", "cmd_ft_body" };
        var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
        try indexCommand(allocator, io, &args);
    }
    {
        const argv = [_][*:0]const u8{ "drop", path_z.ptr, "--kind", "chunk", "--name", "cmd_body_chunks" };
        var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
        try enrichmentCommand(allocator, io, &args);
    }

    {
        var lite = try LiteDb.open(allocator, path, .status_only);
        defer lite.close();

        const indexes = try lite.db.listIndexes(allocator);
        defer db_types.freeIndexConfigs(allocator, indexes);
        try std.testing.expectEqual(@as(usize, 0), indexes.len);

        const enrichments = try lite.db.listEnrichments(allocator);
        defer db_types.freeEnrichmentConfigs(allocator, enrichments);
        try std.testing.expectEqual(@as(usize, 0), enrichments.len);
    }
}

test "lite query readonly runs while writer handle is open" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/query-readonly-active-writer.aflite", .{tmp.sub_path});
    defer allocator.free(path);
    const query_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/query-readonly-active-writer.json", .{tmp.sub_path});
    defer allocator.free(query_path);
    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);
    const query_path_z = try allocator.dupeZ(u8, query_path);
    defer allocator.free(query_path_z);

    var writer = try LiteDb.create(allocator, path, true);
    defer writer.close();

    try writer.db.addIndex(.{
        .name = "ft_body",
        .kind = .full_text,
        .config_json = "{}",
    });

    const batch_response = try batchJson(allocator, &writer.db,
        \\{"inserts":{"doc:readonly-query":{"body":"readonly query active writer"}},"sync_level":"full_index"}
    );
    defer allocator.free(batch_response);
    try std.testing.expect(std.mem.indexOf(u8, batch_response, "\"inserted\":1") != null);

    try writeFileAtomically(allocator, io, query_path,
        \\{"full_text_search":{"match":{"field":"body","text":"active writer"}},"limit":1}
    );

    const argv = [_][*:0]const u8{ path_z.ptr, "--readonly", "--file", query_path_z.ptr };
    var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    try query(allocator, io, &args);
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
        var lite = try LiteDb.create(allocator, path, true);
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

test "lite backup command exports stable data while writer has open transaction" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/backup-active-writer.aflite", .{tmp.sub_path});
    defer allocator.free(path);
    const backup_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/backup-active-writer.afb", .{tmp.sub_path});
    defer allocator.free(backup_path);
    const restored_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/backup-active-writer-restored.aflite", .{tmp.sub_path});
    defer allocator.free(restored_path);
    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);
    const backup_path_z = try allocator.dupeZ(u8, backup_path);
    defer allocator.free(backup_path_z);

    var writer = try LiteDb.create(allocator, path, true);
    defer writer.close();

    const committed = try batchJson(allocator, &writer.db,
        \\{"inserts":{"doc:backup-committed":{"title":"backup committed"}}}
    );
    defer allocator.free(committed);
    try std.testing.expect(std.mem.indexOf(u8, committed, "\"inserted\":1") != null);

    const txn_id: db_types.TxnId = .{ 0x6c, 0x69, 0x74, 0x65, 0x2d, 0x63, 0x6c, 0x69, 0x2d, 0x62, 0x61, 0x63, 0x6b, 0, 0, 1 };
    _ = try writer.db.beginTransactionWithId(txn_id, 2_000);
    try writer.db.writeTransaction(txn_id, .{
        .writes = &.{.{ .key = "doc:backup-pending", .value = "{\"title\":\"backup pending\"}" }},
    });

    const argv = [_][*:0]const u8{ path_z.ptr, "--out", backup_path_z.ptr };
    var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    try backup(allocator, io, &args);

    try writer.db.abortTransaction(txn_id, 0);

    const body = try std.Io.Dir.cwd().readFileAlloc(io, backup_path, allocator, .limited(lite_restore_staging.max_afb_file_bytes));
    defer allocator.free(body);
    try portable_backup.validatePortable(allocator, body);

    try restoreFromSourceFile(allocator, io, backup_path, restored_path, false);

    var restored = try LiteDb.open(allocator, restored_path, .query_readonly);
    defer restored.close();

    const committed_lookup = try lookupJson(allocator, &restored.db, "doc:backup-committed", "");
    defer allocator.free(committed_lookup);
    try std.testing.expect(std.mem.indexOf(u8, committed_lookup, "\"backup committed\"") != null);

    const pending_lookup = try lookupJson(allocator, &restored.db, "doc:backup-pending", "");
    defer allocator.free(pending_lookup);
    try std.testing.expect(std.mem.indexOf(u8, pending_lookup, "\"found\":false") != null);
}

test "lite export subcommand dispatches portable backup alias" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/export-alias.aflite", .{tmp.sub_path});
    defer allocator.free(path);
    const backup_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/export-alias.afb", .{tmp.sub_path});
    defer allocator.free(backup_path);
    const restored_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/export-alias-restored.aflite", .{tmp.sub_path});
    defer allocator.free(restored_path);
    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);
    const backup_path_z = try allocator.dupeZ(u8, backup_path);
    defer allocator.free(backup_path_z);

    {
        var lite = try LiteDb.create(allocator, path, true);
        defer lite.close();

        const json = try batchJson(allocator, &lite.db,
            \\{"inserts":{"doc:export-alias":{"title":"export alias portable"}}}
        );
        defer allocator.free(json);
        try std.testing.expect(std.mem.indexOf(u8, json, "\"inserted\":1") != null);
    }

    const argv = [_][*:0]const u8{ path_z.ptr, "--out", backup_path_z.ptr };
    var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    var init: std.process.Init = undefined;
    init.gpa = allocator;
    init.io = io;
    try dispatchSubcommand(init, "antfly lite", "export", &args);

    const body = try std.Io.Dir.cwd().readFileAlloc(io, backup_path, allocator, .limited(lite_restore_staging.max_afb_file_bytes));
    defer allocator.free(body);
    try portable_backup.validatePortable(allocator, body);

    try restoreFromSourceFile(allocator, io, backup_path, restored_path, false);

    var restored = try LiteDb.open(allocator, restored_path, .query_readonly);
    defer restored.close();

    const lookup_response = try lookupJson(allocator, &restored.db, "doc:export-alias", "");
    defer allocator.free(lookup_response);
    try std.testing.expect(std.mem.indexOf(u8, lookup_response, "\"export alias portable\"") != null);
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
        var source = try LiteDb.create(allocator, src_path, true);
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

test "lite restore target writer lock is held before staging" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const src_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/restore-lock-source.aflite", .{tmp.sub_path});
    defer allocator.free(src_path);
    const dst_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/restore-lock-dst.aflite", .{tmp.sub_path});
    defer allocator.free(dst_path);
    const tmp_path = try restoreTempPathAlloc(allocator, dst_path);
    defer allocator.free(tmp_path);

    {
        var source = try LiteDb.create(allocator, src_path, true);
        defer source.close();

        const json = try batchJson(allocator, &source.db, "{\"inserts\":{\"doc:locked-restore\":{\"title\":\"restore waits for target writer\"}}}");
        defer allocator.free(json);
        try std.testing.expect(std.mem.indexOf(u8, json, "\"inserted\":1") != null);
    }

    var target_lock = try antfly.lite.native.lockWriterPath(allocator, dst_path);
    defer target_lock.close();

    try std.testing.expectError(error.WouldBlock, restoreFromSourceFile(allocator, io, src_path, dst_path, false));
    try std.testing.expect(!pathExists(io, dst_path));
    try std.testing.expect(!pathExists(io, tmp_path));
}

test "lite restore result json distinguishes portable restore and snapshot copy" {
    const allocator = std.testing.allocator;

    const portable = try restoreResultJsonAlloc(allocator, "portable_restore", "afb", "restored.aflite");
    defer allocator.free(portable);
    try std.testing.expectEqualStrings(
        "{\"format\":\"aflite\",\"operation\":\"portable_restore\",\"source_format\":\"afb\",\"path\":\"restored.aflite\"}",
        portable,
    );

    const snapshot_json = try restoreResultJsonAlloc(allocator, "snapshot", "aflite", "copy.aflite");
    defer allocator.free(snapshot_json);
    try std.testing.expectEqualStrings(
        "{\"format\":\"aflite\",\"operation\":\"snapshot\",\"source_format\":\"aflite\",\"path\":\"copy.aflite\"}",
        snapshot_json,
    );
}

test "lite check returns an error for invalid aflite files after writing report" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/check-invalid.aflite", .{tmp.sub_path});
    defer allocator.free(path);
    try writeFileAtomically(allocator, io, path, "short native lite header");

    const report = try antfly.lite.backend.checkFile(allocator, path);
    try std.testing.expect(!report.valid);
    try std.testing.expectEqualStrings("truncated_header", report.issue.?);

    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);
    const argv = [_][*:0]const u8{path_z.ptr};
    var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    try std.testing.expectError(error.LiteCheckFailed, check(allocator, io, &args));
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
    const locked_import_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/backup-import-locked.aflite", .{tmp.sub_path});
    defer allocator.free(locked_import_path);
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
            .name = "ft_direct_v1",
            .kind = .full_text,
            .config_json = "{}",
        });
        try source.db.addIndex(.{
            .name = "dense_external_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}",
        });
        try source.db.addIndex(.{
            .name = "sparse_external_v1",
            .kind = .sparse_vector,
            .config_json = "{\"field\":\"sparse_embedding\",\"external\":true}",
        });
        try source.db.addIndex(.{
            .name = "graph_links_v1",
            .kind = .graph,
            .config_json = "{}",
        });

        try source.db.batch(.{
            .writes = &.{
                .{
                    .key = "doc:backup-roundtrip",
                    .value = "{\"title\":\"portable backup restore\",\"body\":\"schema and catalog survive hybrid alpha\",\"_embeddings\":{\"dense_external_v1\":[1,0],\"sparse_external_v1\":{\"indices\":[7,42],\"values\":[1.5,0.5]}},\"_edges\":{\"graph_links_v1\":{\"links\":[{\"target\":\"doc:backup-target\",\"weight\":1.0}]}}}",
                },
                .{
                    .key = "doc:backup-other",
                    .value = "{\"title\":\"other restore\",\"body\":\"schema and catalog survive beta\",\"_embeddings\":{\"dense_external_v1\":[0,1],\"sparse_external_v1\":{\"indices\":[99],\"values\":[2.0]}}}",
                },
                .{
                    .key = "doc:backup-target",
                    .value = "{\"title\":\"portable graph target\"}",
                },
            },
            .sync_level = .full_index,
        });
        try source.db.runUntilIdle();
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
        try std.testing.expectEqual(@as(usize, 4), indexes.len);
        var saw_direct_index = false;
        var saw_dense_index = false;
        var saw_sparse_index = false;
        var saw_graph_index = false;
        for (indexes) |index| {
            if (std.mem.eql(u8, index.name, "ft_direct_v1")) {
                saw_direct_index = true;
                try std.testing.expectEqual(db_types.IndexKind.full_text, index.kind);
            } else if (std.mem.eql(u8, index.name, "dense_external_v1")) {
                saw_dense_index = true;
                try std.testing.expectEqual(db_types.IndexKind.dense_vector, index.kind);
            } else if (std.mem.eql(u8, index.name, "sparse_external_v1")) {
                saw_sparse_index = true;
                try std.testing.expectEqual(db_types.IndexKind.sparse_vector, index.kind);
            } else if (std.mem.eql(u8, index.name, "graph_links_v1")) {
                saw_graph_index = true;
                try std.testing.expectEqual(db_types.IndexKind.graph, index.kind);
            }
        }
        try std.testing.expect(saw_direct_index);
        try std.testing.expect(saw_dense_index);
        try std.testing.expect(saw_sparse_index);
        try std.testing.expect(saw_graph_index);

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

        const dense = try searchJson(
            allocator,
            &restored.db,
            "{\"embeddings\":{\"dense_external_v1\":[1,0]},\"indexes\":[\"dense_external_v1\"],\"limit\":1}",
        );
        defer allocator.free(dense);
        try std.testing.expect(std.mem.indexOf(u8, dense, "\"doc:backup-roundtrip\"") != null);

        const sparse = try searchJson(
            allocator,
            &restored.db,
            "{\"embeddings\":{\"sparse_external_v1\":{\"indices\":[7,42],\"values\":[1.5,0.5]}},\"indexes\":[\"sparse_external_v1\"],\"limit\":1}",
        );
        defer allocator.free(sparse);
        try std.testing.expect(std.mem.indexOf(u8, sparse, "\"doc:backup-roundtrip\"") != null);

        const graph = try searchJson(
            allocator,
            &restored.db,
            "{\"graph_queries\":{\"neighbors\":{\"index\":\"graph_links_v1\",\"traverse\":{\"start\":{\"keys\":[\"doc:backup-roundtrip\"]},\"edge_types\":[\"links\"],\"max_depth\":1}}},\"limit\":10}",
        );
        defer allocator.free(graph);
        try std.testing.expect(std.mem.indexOf(u8, graph, "\"doc:backup-target\"") != null);

        const hybrid = try searchJson(
            allocator,
            &restored.db,
            "{\"full_text_search\":{\"match\":{\"field\":\"body\",\"text\":\"hybrid alpha\"}},\"embeddings\":{\"dense_external_v1\":[1,0]},\"indexes\":[\"dense_external_v1\"],\"merge_config\":{\"strategy\":\"rrf\"},\"limit\":3}",
        );
        defer allocator.free(hybrid);
        try std.testing.expect(std.mem.indexOf(u8, hybrid, "\"doc:backup-roundtrip\"") != null);
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
        try std.testing.expectEqual(@as(usize, 4), indexes.len);

        const enrichments = try imported.db.listEnrichments(allocator);
        defer db_types.freeEnrichmentConfigs(allocator, enrichments);
        try std.testing.expectEqual(@as(usize, 1), enrichments.len);
        try std.testing.expectEqualStrings("body_chunks_v1", enrichments[0].name);
    }

    {
        var locked_empty_target = try LiteDb.create(allocator, locked_import_path, true);
        locked_empty_target.close();
    }
    const locked_tmp_path = try restoreTempPathAlloc(allocator, locked_import_path);
    defer allocator.free(locked_tmp_path);
    {
        var target_lock = try antfly.lite.native.lockWriterPath(allocator, locked_import_path);
        defer target_lock.close();

        try std.testing.expectError(error.WouldBlock, importPortableIntoExistingLite(allocator, io, backup_path, locked_import_path));
    }
    try std.testing.expect(!pathExists(io, locked_tmp_path));
    {
        var locked_empty_target = try LiteDb.open(allocator, locked_import_path, .status_only);
        defer locked_empty_target.close();
        try std.testing.expect(try lite_restore_staging.isImportTargetEmpty(allocator, &locked_empty_target.db));
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

test "lite import from aflite requires replace for existing target" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const src_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/import-aflite-source.aflite", .{tmp.sub_path});
    defer allocator.free(src_path);
    const dst_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/import-aflite-dst.aflite", .{tmp.sub_path});
    defer allocator.free(dst_path);

    {
        var source = try LiteDb.create(allocator, src_path, true);
        defer source.close();
        const json = try batchJson(allocator, &source.db, "{\"inserts\":{\"doc:source-import\":{\"title\":\"source import\"}}}");
        defer allocator.free(json);
    }

    {
        var target = try LiteDb.create(allocator, dst_path, true);
        defer target.close();
        const json = try batchJson(allocator, &target.db, "{\"inserts\":{\"doc:target-import\":{\"title\":\"target import survives\"}}}");
        defer allocator.free(json);
    }

    try std.testing.expectError(error.AfliteImportRequiresReplace, importFromSourceFile(allocator, io, src_path, dst_path, false));

    {
        var target = try LiteDb.open(allocator, dst_path, .query_readonly);
        defer target.close();
        const json = try lookupJson(allocator, &target.db, "doc:target-import", "");
        defer allocator.free(json);
        try std.testing.expect(std.mem.indexOf(u8, json, "\"target import survives\"") != null);
    }

    try importFromSourceFile(allocator, io, src_path, dst_path, true);

    {
        var replaced = try LiteDb.open(allocator, dst_path, .query_readonly);
        defer replaced.close();
        const source_json = try lookupJson(allocator, &replaced.db, "doc:source-import", "");
        defer allocator.free(source_json);
        try std.testing.expect(std.mem.indexOf(u8, source_json, "\"source import\"") != null);

        const target_json = try lookupJson(allocator, &replaced.db, "doc:target-import", "");
        defer allocator.free(target_json);
        try std.testing.expect(std.mem.indexOf(u8, target_json, "\"found\":false") != null);
    }
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

test "lite snapshot rejects same existing aflite through different path spelling" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const src_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/snapshot-self.aflite", .{tmp.sub_path});
    defer allocator.free(src_path);
    const nested_dir = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/nested", .{tmp.sub_path});
    defer allocator.free(nested_dir);
    const alias_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/nested/../snapshot-self.aflite", .{tmp.sub_path});
    defer allocator.free(alias_path);

    try fs_paths.createDirPathPortable(io, nested_dir);
    {
        var source = try LiteDb.create(allocator, src_path, true);
        defer source.close();
        const json = try batchJson(allocator, &source.db, "{\"inserts\":{\"doc:self-snapshot\":{\"title\":\"self snapshot rejected\"}}}");
        defer allocator.free(json);
    }

    try std.testing.expectError(error.InvalidArguments, copyStableAfliteToPath(allocator, io, src_path, alias_path, true));

    var reopened = try LiteDb.open(allocator, src_path, .query_readonly);
    defer reopened.close();
    const json = try lookupJson(allocator, &reopened.db, "doc:self-snapshot", "");
    defer allocator.free(json);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"self snapshot rejected\"") != null);
}

test "lite offline portable backup rejects standalone-adopted artifacts" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/adopted-backup.aflite", .{tmp.sub_path});
    defer allocator.free(path);

    var lite = try LiteDb.create(allocator, path, true);
    defer lite.close();
    try ensureOfflinePortableBackupSupported(&lite);
    try ensureOfflineRootOnlyOperationSupported(&lite);
    try lite.backend.adoptEmbeddedRootAsNamespace("group-42/table-db");
    try std.testing.expectError(
        error.StandaloneLitePortableBackupRequiresApi,
        ensureOfflinePortableBackupSupported(&lite),
    );

    // Fresh standalone artifacts without an adopted embedded root must also
    // reject the root-only offline archive path.
    var fresh_tmp = std.testing.tmpDir(.{});
    defer fresh_tmp.cleanup();
    const fresh_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/standalone-backup.aflite", .{fresh_tmp.sub_path});
    defer allocator.free(fresh_path);
    var fresh = try LiteDb.create(allocator, fresh_path, true);
    defer fresh.close();
    try fresh.backend.markStandaloneArtifact();
    try std.testing.expectError(
        error.StandaloneLitePortableBackupRequiresApi,
        ensureOfflinePortableBackupSupported(&fresh),
    );
    try std.testing.expectError(
        error.StandaloneLiteRequiresApi,
        ensureOfflineRootOnlyOperationSupported(&fresh),
    );
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

    var lite = try LiteDb.create(allocator, path, true);
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
    try std.testing.expect(std.mem.indexOf(u8, json, "\"primary_layout\":\"native_document_pages\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"replay_layout\":\"native_replay_lanes_in_document_catalog\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"index_layout\":\"native_index_catalog_pages\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"index_layout\":\"lsm") == null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"index_namespace\":\"__antfly_lite\"") != null);
    const expected_format_version = try std.fmt.allocPrint(allocator, "\"format_version\":{d}", .{antfly.lite.native.format_version});
    defer allocator.free(expected_format_version);
    try std.testing.expect(std.mem.indexOf(u8, json, expected_format_version) != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"page_size\":4096") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"fsync\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"active_checkpoint\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"checkpoint_sequence\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"stats\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"pending_work\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"inference\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"mode\":\"caller_supplied_or_disabled\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"configured\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"remote_provider_configured\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"local_runtime_configured\":false") != null);
    const native_local_runtime_available = antfly.lite.backend.capabilitiesForProfile(.native).local_inference_runtime;
    try std.testing.expect(std.mem.indexOf(u8, json, if (native_local_runtime_available) "\"local_runtime_available\":true" else "\"local_runtime_available\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"capabilities\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"manual_maintenance\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"background_enrichment_runtime\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"inference_mode\":\"caller_supplied_or_disabled\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"no_inference_configured_ok\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"caller_supplied_artifacts\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, if (native_local_runtime_available) "\"local_inference_runtime\":true" else "\"local_inference_runtime\":false") != null);
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

test "lite writer close syncs unsynced batch before readonly reopen" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/writer-close-sync.aflite", .{tmp.sub_path});
    defer allocator.free(path);

    {
        var lite = try LiteDb.create(allocator, path, true);
        defer lite.close();

        const batch_response = try batchJson(allocator, &lite.db,
            \\{"inserts":{"doc:writer-close-sync":{"body":"close persists unsynced batch"}}}
        );
        defer allocator.free(batch_response);
        try std.testing.expect(std.mem.indexOf(u8, batch_response, "\"inserted\":1") != null);
    }

    {
        var reopened = try LiteDb.open(allocator, path, .query_readonly);
        defer reopened.close();

        const lookup_response = try lookupJson(allocator, &reopened.db, "doc:writer-close-sync", "");
        defer allocator.free(lookup_response);
        try std.testing.expect(std.mem.indexOf(u8, lookup_response, "\"close persists unsynced batch\"") != null);
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
        var source = try LiteDb.create(allocator, src_path, true);
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

    const schema_json =
        \\{"version":0,"default_type":"doc","enforce_types":false,"document_schemas":{"doc":{"schema":{"type":"object","additionalProperties":true}}}}
    ;
    const enrichment_json = "{\"name\":\"promote_chunks_v1\",\"kind\":\"chunk\",\"field\":\"body\",\"chunk_size\":192,\"chunk_overlap\":24}";
    const index_json = "{\"name\":\"promote_ft_body\",\"kind\":\"full_text\",\"config_json\":\"{\\\"chunk_name\\\":\\\"promote_chunks_v1\\\"}\"}";

    {
        var source = try LiteDb.create(allocator, src_path, true);
        defer source.close();

        try source.db.setSchemaJson(allocator, schema_json);

        var enrichment = try std.json.parseFromSlice(db_types.EnrichmentConfig, allocator, enrichment_json, .{
            .ignore_unknown_fields = true,
        });
        defer enrichment.deinit();
        try source.db.addEnrichment(enrichment.value);

        var index = try std.json.parseFromSlice(db_types.IndexConfig, allocator, index_json, .{
            .ignore_unknown_fields = true,
        });
        defer index.deinit();
        try source.db.addIndex(index.value);

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
    try std.testing.expectEqualStrings(schema_json, manifest.schema_json);
    try std.testing.expectEqual(@as(usize, 1), manifest.shards.len);
    try std.testing.expectEqualStrings("lite-promote-test.afb", manifest.shards[0].snapshot_path);
    try std.testing.expect(std.mem.indexOf(u8, manifest.indexes_json, "\"promote_ft_body\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest.indexes_json, "\"enrichments\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest.indexes_json, "\"promote_chunks_v1\"") != null);

    var parsed_indexes = try std.json.parseFromSlice(std.json.Value, allocator, manifest.indexes_json, .{});
    defer parsed_indexes.deinit();
    try std.testing.expect(parsed_indexes.value == .object);
    try std.testing.expect(parsed_indexes.value.object.get("promote_ft_body") != null);
    const enrichments = parsed_indexes.value.object.get("enrichments") orelse return error.TestExpectedEqual;
    try std.testing.expect(enrichments == .array);
    try std.testing.expectEqual(@as(usize, 1), enrichments.array.items.len);

    const afb_path = try std.fmt.allocPrint(allocator, "{s}/lite-promote-test.afb", .{backup_root});
    defer allocator.free(afb_path);
    const portable = try std.Io.Dir.cwd().readFileAlloc(io, afb_path, allocator, .limited(max_afb_file_bytes));
    defer allocator.free(portable);
    try std.testing.expect(portable.len > 0);
}

test "lite promote helper stages backup then submits normal restore request" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const src_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/promote-command-src.aflite", .{tmp.sub_path});
    defer allocator.free(src_path);
    const cwd_tmp = try std.Io.Dir.cwd().realPathFileAlloc(io, ".zig-cache/tmp", allocator);
    defer allocator.free(cwd_tmp);
    const backup_root = try std.fmt.allocPrint(allocator, "{s}/{s}/promote-command-backups", .{ cwd_tmp, tmp.sub_path });
    defer allocator.free(backup_root);
    const location = try std.fmt.allocPrint(allocator, "file://{s}", .{backup_root});
    defer allocator.free(location);

    {
        var source = try LiteDb.create(allocator, src_path, true);
        defer source.close();

        const json = try batchJson(allocator, &source.db, "{\"inserts\":{\"doc:promote-command\":{\"title\":\"restore request\"}}}");
        defer allocator.free(json);
        try std.testing.expect(std.mem.indexOf(u8, json, "\"inserted\":1") != null);
    }

    const location_z = try allocator.dupeZ(u8, location);
    defer allocator.free(location_z);
    const argv = [_][*:0]const u8{ "--target", "http://restore.test", "--table", "docs", "--connection", "local-reader", "--backup-id", "lite-promote-command", "--location", location_z.ptr };
    var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    var opts = try parsePromoteOptions(allocator, src_path, &args);
    defer opts.deinit(allocator);

    const Capture = struct {
        called: bool = false,
        table: []const u8 = "",
        backup_id: []const u8 = "",
        location: []const u8 = "",
        connection: []const u8 = "",

        fn restore(ctx: *anyopaque, allocator_inner: Allocator, table: []const u8, request: antfly_client.types.RestoreRequest, _: ?[]const u8) !PromoteRestoreAcceptance {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.called = true;
            self.table = table;
            self.backup_id = request.backup_id;
            self.location = request.location;
            self.connection = request.connection;
            const job_id = try allocator_inner.dupe(u8, "9223372036854775807");
            errdefer allocator_inner.free(job_id);
            const encoded = try allocator_inner.dupe(u8, "{\"job_id\":\"9223372036854775807\",\"phase\":\"queued\"}");
            return .{ .job_id = job_id, .encoded = encoded };
        }
    };

    var capture = Capture{};
    var submission = try promoteWithRestore(allocator, src_path, opts, &capture, Capture.restore);
    defer submission.deinit(allocator);
    const staged = submission.staged;

    try std.testing.expect(capture.called);
    try std.testing.expectEqualStrings("9223372036854775807", submission.restore_job_id);
    try std.testing.expectEqualStrings("{\"job_id\":\"9223372036854775807\",\"phase\":\"queued\"}", submission.accepted_json);
    try std.testing.expectEqualStrings("docs", capture.table);
    try std.testing.expectEqualStrings(staged.backup_id, capture.backup_id);
    try std.testing.expectEqualStrings(staged.location, capture.location);
    try std.testing.expectEqualStrings("local-reader", capture.connection);
    try std.testing.expectEqualStrings("lite-promote-command.afb", staged.snapshot_path);

    var backup_location = try antfly.public_api.backups.openBackupLocation(allocator, location);
    defer backup_location.deinit(allocator);
    var manifest = try antfly.public_api.backups.readManifestFromLocation(allocator, &backup_location, capture.backup_id);
    defer manifest.deinit(allocator);
    try std.testing.expectEqualStrings("docs", manifest.table_name);
    try std.testing.expectEqualStrings(staged.snapshot_path, manifest.shards[0].snapshot_path);
}
