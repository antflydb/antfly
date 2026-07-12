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
const antfly_client = @import("antfly-client");
const cli = @import("mod.zig");

const backup_codec = antfly.backup_codec;
const lite_paths = antfly.lite.paths;
const lite_restore_staging = antfly.lite.restore_staging;
const portable_backup = antfly.portable_backup;

const default_backup_location = "file:///tmp/antfly_backups";

const BackupArgs = struct {
    help: bool = false,
    table_name: ?[]const u8 = null,
    tables_str: ?[]const u8 = null,
    backup_id: ?[]const u8 = null,
    location: []const u8 = default_backup_location,
    format: ?[]const u8 = null,
    url: ?[]const u8 = null,
    output: ?[]const u8 = null,
    out_path: ?[]const u8 = null,
    connection: ?[]const u8 = null,
    list_backups: bool = false,
};

const RestoreArgs = struct {
    help: bool = false,
    table_name: ?[]const u8 = null,
    tables_str: ?[]const u8 = null,
    backup_id: ?[]const u8 = null,
    location: []const u8 = default_backup_location,
    location_explicit: bool = false,
    format: ?[]const u8 = null,
    restore_mode: ?[]const u8 = null,
    url: ?[]const u8 = null,
    input_path: ?[]const u8 = null,
    connection: ?[]const u8 = null,
};

const InputRestorePlan = struct {
    staged: lite_restore_staging.StagedRestore,
    request: antfly_client.types.RestoreRequest,

    fn tableName(self: InputRestorePlan) []const u8 {
        return self.staged.table_name;
    }

    fn deinit(self: *InputRestorePlan, allocator: std.mem.Allocator) void {
        self.staged.deinit(allocator);
        self.* = undefined;
    }
};

pub fn runBackup(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const opts = parseBackupArgs(args) catch cli.fatal("invalid backup arguments", .{});
    if (opts.help) {
        printBackupUsage();
        return;
    }
    const connection = opts.connection orelse cli.fatal("--connection is required", .{});

    if (opts.url) |value| try client.setBaseUrl(value);

    if (opts.list_backups) {
        if (opts.output) |value| {
            if (!std.mem.eql(u8, value, "json")) {
                cli.fatal("only JSON output is supported for backup --list", .{});
            }
        }
        var resp = try client.listBackups(.{ .location = opts.location, .connection = connection });
        defer resp.deinit();
        if (resp.data) |data| {
            try cli.writeJson(allocator, io, data.value);
        }
        return;
    }

    if (opts.table_name) |tbl| {
        const selected_format = if (opts.out_path != null and opts.format == null) "portable" else opts.format orelse "native";
        if (!std.mem.eql(u8, selected_format, "native") and !std.mem.eql(u8, selected_format, "portable")) {
            cli.fatal("unsupported backup format: {s}", .{selected_format});
        }

        var out_plan: ?PortableOutPlan = null;
        defer if (out_plan) |*plan| plan.deinit(allocator);
        const bid = if (opts.out_path) |out_path| blk: {
            if (!std.mem.eql(u8, selected_format, "portable")) {
                cli.fatal("--out is only supported with --format portable", .{});
            }
            out_plan = try PortableOutPlan.init(allocator, out_path, opts.backup_id);
            break :blk out_plan.?.backup_id;
        } else opts.backup_id orelse cli.fatal("--backup-id is required", .{});
        const location = if (out_plan) |plan| plan.location else opts.location;
        try client.backupTable(tbl, .{ .backup_id = bid, .location = location, .connection = connection, .format = selected_format });
        if (out_plan) |plan| {
            validatePortableOutputFile(allocator, io, plan.out_path) catch |err| switch (err) {
                error.EmptyPortableOutput => cli.fatal("portable backup completed but local --out file is empty: {s}", .{plan.out_path}),
                error.FileNotFound => cli.fatal("portable backup completed but local --out file is not readable: {s}", .{plan.out_path}),
                else => cli.fatal("portable backup completed but local --out file is not a valid portable AFB: {s}", .{plan.out_path}),
            };
            std.debug.print("Portable backup written to {s}.\n", .{plan.out_path});
        } else {
            std.debug.print("Backup command successful.\n", .{});
        }
        return;
    }

    const bid = opts.backup_id orelse cli.fatal("--backup-id is required", .{});

    var table_names: ?[]const []const u8 = null;
    if (opts.tables_str) |ts| {
        var names = std.ArrayListUnmanaged([]const u8).empty;
        var it = std.mem.splitScalar(u8, ts, ',');
        while (it.next()) |name| {
            try names.append(allocator, std.mem.trim(u8, name, " "));
        }
        table_names = names.items;
    }

    var resp = try client.clusterBackup(.{
        .backup_id = bid,
        .location = opts.location,
        .connection = connection,
        .table_names = table_names,
    });
    defer resp.deinit();
    if (resp.data) |data| {
        try cli.writeJson(allocator, io, data.value);
    }
    std.debug.print("Backup command successful.\n", .{});
}

pub fn runRestore(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const opts = parseRestoreArgs(args) catch cli.fatal("invalid restore arguments", .{});
    if (opts.help) {
        printRestoreUsage();
        return;
    }
    const connection = opts.connection orelse cli.fatal("--connection is required", .{});

    validateRestoreArgs(opts) catch |err| switch (err) {
        error.RestoreInputModeUnsupported => cli.fatal("--input restore targets one table; omit --mode", .{}),
        else => cli.fatal("invalid restore arguments", .{}),
    };

    if (opts.url) |value| try client.setBaseUrl(value);

    if (opts.input_path) |input| {
        if (opts.tables_str != null) cli.fatal("--input restore supports exactly one --table", .{});
        const tbl = opts.table_name orelse cli.fatal("--table is required with --input", .{});
        if (opts.format) |value| {
            if (!std.mem.eql(u8, value, "portable")) {
                cli.fatal("--input restore is portable; omit --format or use --format portable", .{});
            }
        }

        const location = try restoreInputLocationAlloc(allocator, input, opts);
        defer allocator.free(location);
        var plan = try prepareInputRestorePlan(allocator, input, tbl, opts.backup_id, location, connection);
        defer plan.deinit(allocator);
        var resp = try client.restoreTable(plan.tableName(), plan.request);
        defer resp.deinit();
        std.debug.print("Restore command successfully initiated.\n", .{});
        return;
    }

    const bid = opts.backup_id orelse cli.fatal("--backup-id is required", .{});

    if (opts.table_name) |tbl| {
        var resp = try client.restoreTable(tbl, .{ .backup_id = bid, .location = opts.location, .connection = connection, .format = opts.format });
        defer resp.deinit();
        std.debug.print("Restore command successfully initiated.\n", .{});
        return;
    }

    var table_names: ?[]const []const u8 = null;
    if (opts.tables_str) |ts| {
        var names = std.ArrayListUnmanaged([]const u8).empty;
        var it = std.mem.splitScalar(u8, ts, ',');
        while (it.next()) |name| {
            try names.append(allocator, std.mem.trim(u8, name, " "));
        }
        table_names = names.items;
    }

    var resp = try client.clusterRestore(.{
        .backup_id = bid,
        .location = opts.location,
        .connection = connection,
        .table_names = table_names,
        .restore_mode = opts.restore_mode,
    });
    defer resp.deinit();
    if (resp.data) |data| {
        try cli.writeJson(allocator, io, data.value);
    }
    std.debug.print("Restore command successfully initiated.\n", .{});
}

fn prepareInputRestorePlan(
    allocator: std.mem.Allocator,
    input_path: []const u8,
    table_name: []const u8,
    backup_id: ?[]const u8,
    location: []const u8,
    connection: []const u8,
) !InputRestorePlan {
    var owned_backup_id: ?[]u8 = null;
    defer if (owned_backup_id) |value| allocator.free(value);
    const resolved_backup_id = backup_id orelse blk: {
        owned_backup_id = try lite_restore_staging.defaultBackupIdAlloc(allocator, input_path);
        break :blk owned_backup_id.?;
    };

    const staged = try lite_restore_staging.stageInputRestoreBackup(allocator, input_path, table_name, resolved_backup_id, location);
    errdefer {
        var owned_staged = staged;
        owned_staged.deinit(allocator);
    }

    return .{
        .staged = staged,
        .request = .{
            .backup_id = staged.backup_id,
            .location = staged.location,
            .connection = connection,
            .format = "portable",
        },
    };
}

fn parseBackupArgs(args: *std.process.Args.Iterator) !BackupArgs {
    var out = BackupArgs{};
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h") or std.mem.eql(u8, arg, "help")) {
            out.help = true;
        } else if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            out.table_name = try nextRequired(args);
        } else if (std.mem.eql(u8, arg, "--tables")) {
            out.tables_str = try nextRequired(args);
        } else if (std.mem.eql(u8, arg, "--backup-id")) {
            out.backup_id = try nextRequired(args);
        } else if (std.mem.eql(u8, arg, "--location")) {
            out.location = try nextRequired(args);
        } else if (std.mem.eql(u8, arg, "--connection")) {
            out.connection = try nextRequired(args);
        } else if (std.mem.eql(u8, arg, "--format")) {
            out.format = try nextRequired(args);
        } else if (std.mem.eql(u8, arg, "--url")) {
            out.url = try nextRequired(args);
        } else if (std.mem.eql(u8, arg, "--output") or std.mem.eql(u8, arg, "-o")) {
            out.output = try nextRequired(args);
        } else if (std.mem.eql(u8, arg, "--out")) {
            out.out_path = try nextRequired(args);
        } else if (std.mem.eql(u8, arg, "--list")) {
            out.list_backups = true;
        } else {
            return error.UnknownArgument;
        }
    }
    return out;
}

fn parseRestoreArgs(args: *std.process.Args.Iterator) !RestoreArgs {
    var out = RestoreArgs{};
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h") or std.mem.eql(u8, arg, "help")) {
            out.help = true;
        } else if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            out.table_name = try nextRequired(args);
        } else if (std.mem.eql(u8, arg, "--tables")) {
            out.tables_str = try nextRequired(args);
        } else if (std.mem.eql(u8, arg, "--backup-id")) {
            out.backup_id = try nextRequired(args);
        } else if (std.mem.eql(u8, arg, "--location")) {
            out.location = try nextRequired(args);
            out.location_explicit = true;
        } else if (std.mem.eql(u8, arg, "--connection")) {
            out.connection = try nextRequired(args);
        } else if (std.mem.eql(u8, arg, "--format")) {
            out.format = try nextRequired(args);
        } else if (std.mem.eql(u8, arg, "--mode")) {
            out.restore_mode = try nextRequired(args);
        } else if (std.mem.eql(u8, arg, "--input") or std.mem.eql(u8, arg, "-i")) {
            out.input_path = try nextRequired(args);
        } else if (std.mem.eql(u8, arg, "--url")) {
            out.url = try nextRequired(args);
        } else {
            return error.UnknownArgument;
        }
    }
    return out;
}

fn validateRestoreArgs(opts: RestoreArgs) !void {
    if (opts.input_path == null) return;
    if (opts.restore_mode != null) return error.RestoreInputModeUnsupported;
    if (!isPortableRestoreInputPath(opts.input_path.?)) return error.InvalidRestoreInputPath;
}

fn isPortableRestoreInputPath(path: []const u8) bool {
    return std.mem.endsWith(u8, path, ".aflite") or std.mem.endsWith(u8, path, ".afb");
}

fn restoreInputLocationAlloc(allocator: std.mem.Allocator, input_path: []const u8, opts: RestoreArgs) ![]u8 {
    if (opts.location_explicit or !std.mem.endsWith(u8, input_path, ".aflite")) {
        return try allocator.dupe(u8, opts.location);
    }
    return try defaultLiteInputRestoreLocationAlloc(allocator);
}

fn defaultLiteInputRestoreLocationAlloc(allocator: std.mem.Allocator) ![]u8 {
    return try lite_paths.defaultBackupsLocationAlloc(allocator);
}

fn printBackupUsage() void {
    std.debug.print(
        \\usage:
        \\  antfly backup --table <name> --backup-id <id> --connection <id> [--location <uri>] [--format native|portable] [--url <url>]
        \\  antfly backup --table <name> --format portable --out <backup.afb> --connection <id> [--url <url>]
        \\  antfly backup --tables <a,b> --backup-id <id> --connection <id> [--location <uri>] [--url <url>]
        \\  antfly backup --list --connection <id> [--location <uri>] [--output json] [--url <url>]
        \\
        \\notes:
        \\  `--out` writes a single portable AFB file for Lite restore/import.
        \\
    , .{});
}

fn printRestoreUsage() void {
    std.debug.print(
        \\usage:
        \\  antfly restore --table <name> --backup-id <id> --connection <id> [--location <uri>] [--format native|portable] [--url <url>]
        \\  antfly restore --tables <a,b> --backup-id <id> --connection <id> [--location <uri>] [--mode <mode>] [--url <url>]
        \\  antfly restore --input <db.aflite|backup.afb> --table <name> --connection <id> [--backup-id <id>] [--location <uri>] [--url <url>]
        \\
        \\notes:
        \\  `--input db.aflite` stages an Antfly Lite database as a portable backup,
        \\  then restores it through the normal Antfly table restore path.
        \\  Without --location, .aflite input stages under ~/.antfly/lite/backups.
        \\
    , .{});
}

fn nextRequired(args: *std.process.Args.Iterator) ![]const u8 {
    return args.next() orelse error.MissingArgument;
}

const PortableOutPlan = struct {
    out_path: []u8,
    backup_id: []u8,
    location: []u8,

    fn init(allocator: std.mem.Allocator, out_path: []const u8, requested_backup_id: ?[]const u8) !PortableOutPlan {
        if (!std.mem.endsWith(u8, out_path, ".afb")) cli.fatal("--out path must end in .afb: {s}", .{out_path});

        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        const cwd = try std.Io.Dir.cwd().realPathFileAlloc(io_impl.io(), ".", allocator);
        defer allocator.free(cwd);
        const absolute_out_path = if (std.fs.path.isAbsolute(out_path))
            try allocator.dupe(u8, out_path)
        else
            try std.fs.path.join(allocator, &.{ cwd, out_path });
        errdefer allocator.free(absolute_out_path);

        const base = std.fs.path.basename(absolute_out_path);
        const derived_backup_id = try allocator.dupe(u8, base[0 .. base.len - ".afb".len]);
        errdefer allocator.free(derived_backup_id);
        if (requested_backup_id) |backup_id| {
            if (!std.mem.eql(u8, backup_id, derived_backup_id)) {
                cli.fatal("--backup-id must match the --out file stem for portable file output", .{});
            }
        }

        const parent = std.fs.path.dirname(absolute_out_path) orelse cwd;
        const location = try std.fmt.allocPrint(allocator, "file://{s}", .{parent});
        errdefer allocator.free(location);

        return .{
            .out_path = absolute_out_path,
            .backup_id = derived_backup_id,
            .location = location,
        };
    }

    fn deinit(self: *PortableOutPlan, allocator: std.mem.Allocator) void {
        allocator.free(self.out_path);
        allocator.free(self.backup_id);
        allocator.free(self.location);
        self.* = undefined;
    }
};

fn validatePortableOutputFile(allocator: std.mem.Allocator, io: std.Io, path: []const u8) !void {
    const body = try readOutputFileAlloc(allocator, io, path, lite_restore_staging.max_afb_file_bytes);
    defer allocator.free(body);
    if (body.len == 0) return error.EmptyPortableOutput;
    try portable_backup.validatePortable(allocator, body);
}

fn readOutputFileAlloc(allocator: std.mem.Allocator, io: std.Io, path: []const u8, max_bytes: usize) ![]u8 {
    if (!std.fs.path.isAbsolute(path)) {
        return try std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(max_bytes));
    }
    var file = try std.Io.Dir.openFileAbsolute(io, path, .{});
    defer file.close(io);
    const size = (try file.stat(io)).size;
    if (size > max_bytes or size > std.math.maxInt(usize)) return error.FileTooBig;
    var buf: [8192]u8 = undefined;
    var reader = file.reader(io, &buf);
    return try reader.interface.readAlloc(allocator, @intCast(size));
}

test "backup cli parser accepts help flag" {
    var argv = [_][*:0]const u8{"--help"};
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    const opts = try parseBackupArgs(&iter);
    try std.testing.expect(opts.help);
}

test "backup cli parser rejects unknown arguments" {
    var argv = [_][*:0]const u8{ "--backup-id", "daily", "--bogus" };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    try std.testing.expectError(error.UnknownArgument, parseBackupArgs(&iter));
}

test "backup cli parser accepts portable out path" {
    var argv = [_][*:0]const u8{
        "--table",
        "docs",
        "--format",
        "portable",
        "--out",
        "docs.afb",
    };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    const opts = try parseBackupArgs(&iter);
    try std.testing.expectEqualStrings("docs", opts.table_name.?);
    try std.testing.expectEqualStrings("portable", opts.format.?);
    try std.testing.expectEqualStrings("docs.afb", opts.out_path.?);
}

test "portable out plan derives file location and backup id" {
    const allocator = std.testing.allocator;
    var plan = try PortableOutPlan.init(allocator, "docs.afb", null);
    defer plan.deinit(allocator);
    try std.testing.expectEqualStrings("docs", plan.backup_id);
    try std.testing.expect(std.mem.endsWith(u8, plan.out_path, "/docs.afb"));
    try std.testing.expect(std.mem.startsWith(u8, plan.location, "file://"));
}

test "portable output validation requires a valid afb" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const valid_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/valid.afb", .{tmp.sub_path});
    defer allocator.free(valid_path);
    const malformed_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/malformed.afb", .{tmp.sub_path});
    defer allocator.free(malformed_path);

    var valid = std.ArrayList(u8).empty;
    defer valid.deinit(allocator);
    try backup_codec.writeHeader(&valid, allocator, .{
        .format_version = backup_codec.format_version,
        .flags = 0,
        .created_at_ns = 0,
        .backup_id = [_]u8{0} ** 16,
        .table_count = 1,
        .shard_count = 1,
    });
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = valid_path, .data = valid.items });
    try validatePortableOutputFile(allocator, io, valid_path);

    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = malformed_path, .data = "not an afb" });
    try std.testing.expectError(error.EndOfStream, validatePortableOutputFile(allocator, io, malformed_path));
}

test "restore cli parser accepts aflite input shape" {
    var argv = [_][*:0]const u8{
        "--input",
        "app.aflite",
        "--table",
        "docs",
        "--format",
        "portable",
        "--location",
        "file:///tmp/backups",
        "--backup-id",
        "lite-app",
    };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    const opts = try parseRestoreArgs(&iter);
    try std.testing.expectEqualStrings("app.aflite", opts.input_path.?);
    try std.testing.expectEqualStrings("docs", opts.table_name.?);
    try std.testing.expectEqualStrings("portable", opts.format.?);
    try std.testing.expectEqualStrings("file:///tmp/backups", opts.location);
    try std.testing.expect(opts.location_explicit);
    try std.testing.expectEqualStrings("lite-app", opts.backup_id.?);
}

test "restore input location defaults aflite staging under lite workspace" {
    const allocator = std.testing.allocator;

    var argv = [_][*:0]const u8{
        "--input",
        "app.aflite",
        "--table",
        "docs",
    };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    const opts = try parseRestoreArgs(&iter);
    try std.testing.expect(!opts.location_explicit);

    const location = try restoreInputLocationAlloc(allocator, opts.input_path.?, opts);
    defer allocator.free(location);
    try std.testing.expect(std.mem.startsWith(u8, location, "file://"));
    try std.testing.expect(std.mem.endsWith(u8, location, "/.antfly/lite/backups"));
}

test "restore input location keeps generic default for afb input" {
    const allocator = std.testing.allocator;

    var argv = [_][*:0]const u8{
        "--input",
        "app.afb",
        "--table",
        "docs",
    };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    const opts = try parseRestoreArgs(&iter);
    try std.testing.expect(!opts.location_explicit);

    const location = try restoreInputLocationAlloc(allocator, opts.input_path.?, opts);
    defer allocator.free(location);
    try std.testing.expectEqualStrings(default_backup_location, location);
}

test "restore input plan stages aflite as portable table restore" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const src_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/restore-input-plan-src.aflite", .{tmp.sub_path});
    defer allocator.free(src_path);
    const restored_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/restore-input-plan-normal-db", .{tmp.sub_path});
    defer allocator.free(restored_path);
    const cwd_tmp = try std.Io.Dir.cwd().realPathFileAlloc(io, ".zig-cache/tmp", allocator);
    defer allocator.free(cwd_tmp);
    const backup_root = try std.fmt.allocPrint(allocator, "{s}/{s}/restore-input-plan-backups", .{ cwd_tmp, tmp.sub_path });
    defer allocator.free(backup_root);
    const location = try std.fmt.allocPrint(allocator, "file://{s}", .{backup_root});
    defer allocator.free(location);

    const schema_json =
        \\{"version":0,"default_type":"doc","enforce_types":false,"document_schemas":{"doc":{"schema":{"type":"object","additionalProperties":true}}}}
    ;

    {
        var lite = try antfly.lite.connection.Connection.create(allocator, src_path, true);
        defer lite.close();
        const db = &lite.db;
        try db.setSchemaJson(allocator, schema_json);
        try db.addEnrichment(.{
            .name = "restore_input_chunks_v1",
            .kind = .chunk,
            .field = "body",
            .chunk_size = 96,
            .chunk_overlap = 12,
        });
        try db.addIndex(.{
            .name = "restore_input_ft_body",
            .kind = .full_text,
            .config_json = "{\"chunk_name\":\"restore_input_chunks_v1\"}",
        });
        try db.batch(.{
            .writes = &.{.{
                .key = "doc:restore-input",
                .value = "{\"title\":\"restore input document\",\"body\":\"normal restore input staging\"}",
            }},
            .sync_level = .full_index,
        });
        try db.runUntilIdle();
    }

    var plan = try prepareInputRestorePlan(allocator, src_path, "docs", null, location, "local-reader");
    defer plan.deinit(allocator);

    try std.testing.expectEqualStrings("docs", plan.tableName());
    try std.testing.expectEqualStrings("lite-restore-input-plan-src", plan.request.backup_id);
    try std.testing.expectEqualStrings(location, plan.request.location);
    try std.testing.expectEqualStrings("portable", plan.request.format.?);
    try std.testing.expectEqualStrings("lite-restore-input-plan-src.afb", plan.staged.snapshot_path);

    var backup_location = try antfly.public_api.backups.openBackupLocation(allocator, location);
    defer backup_location.deinit(allocator);
    var manifest = try antfly.public_api.backups.readManifestFromLocation(allocator, &backup_location, plan.request.backup_id);
    defer manifest.deinit(allocator);

    try std.testing.expectEqualStrings("docs", manifest.table_name);
    try std.testing.expectEqualStrings(plan.request.backup_id, manifest.backup_id);
    try std.testing.expectEqualStrings(schema_json, manifest.schema_json);
    try std.testing.expect(std.mem.indexOf(u8, manifest.indexes_json, "\"restore_input_ft_body\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest.indexes_json, "\"enrichments\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest.indexes_json, "\"restore_input_chunks_v1\"") != null);
    try std.testing.expectEqual(@as(usize, 1), manifest.shards.len);
    try std.testing.expectEqualStrings(plan.staged.snapshot_path, manifest.shards[0].snapshot_path);

    const afb_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ backup_root, plan.staged.snapshot_path });
    defer allocator.free(afb_path);
    const portable = try std.Io.Dir.cwd().readFileAlloc(io, afb_path, allocator, .limited(lite_restore_staging.max_afb_file_bytes));
    defer allocator.free(portable);
    try portable_backup.validatePortable(allocator, portable);

    var restored = try antfly.db.DB.open(allocator, restored_path, .{});
    defer restored.close();
    try portable_backup.importPortable(allocator, restored.core.store, portable);
    const value = (try restored.get(allocator, "doc:restore-input")) orelse return error.TestExpectedEqual;
    defer allocator.free(value);
    try std.testing.expect(std.mem.indexOf(u8, value, "restore input document") != null);
}

test "restore cli parser accepts help flag" {
    var argv = [_][*:0]const u8{"help"};
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    const opts = try parseRestoreArgs(&iter);
    try std.testing.expect(opts.help);
}

test "restore cli parser keeps mode visible for input validation" {
    var argv = [_][*:0]const u8{
        "--input",
        "app.aflite",
        "--table",
        "docs",
        "--mode",
        "replace",
    };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    const opts = try parseRestoreArgs(&iter);
    try std.testing.expectEqualStrings("app.aflite", opts.input_path.?);
    try std.testing.expectEqualStrings("replace", opts.restore_mode.?);
}

test "restore cli validation rejects mode with input restore" {
    var argv = [_][*:0]const u8{
        "--input",
        "app.aflite",
        "--table",
        "docs",
        "--mode",
        "replace",
    };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    const opts = try parseRestoreArgs(&iter);
    try std.testing.expectError(error.RestoreInputModeUnsupported, validateRestoreArgs(opts));
}

test "restore cli validation rejects unsupported input extension" {
    var argv = [_][*:0]const u8{
        "--input",
        "app.db",
        "--table",
        "docs",
    };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    const opts = try parseRestoreArgs(&iter);
    try std.testing.expectError(error.InvalidRestoreInputPath, validateRestoreArgs(opts));
}

test "restore cli parser rejects unknown arguments" {
    var argv = [_][*:0]const u8{ "--input", "app.aflite", "--table", "docs", "--bogus" };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    try std.testing.expectError(error.UnknownArgument, parseRestoreArgs(&iter));
}
