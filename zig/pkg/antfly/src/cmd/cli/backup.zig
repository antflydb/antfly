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
const platform_time = antfly.platform_time;

const lite_restore_staging = antfly.lite.restore_staging;
const portable_backup = antfly.portable_backup;

const default_restore_wait_timeout_ms: u64 = 30 * 60 * 1000;
const restore_poll_interval_ms: u64 = 1000;
const max_restore_tables: usize = 256;

const BackupArgs = struct {
    help: bool = false,
    table_name: ?[]const u8 = null,
    tables_str: ?[]const u8 = null,
    backup_id: ?[]const u8 = null,
    location: []const u8 = "",
    format: ?[]const u8 = null,
    url: ?[]const u8 = null,
    output: ?[]const u8 = null,
    connection: ?[]const u8 = null,
    location_explicit: bool = false,
    list_backups: bool = false,
};

const RestoreArgs = struct {
    help: bool = false,
    table_name: ?[]const u8 = null,
    tables_str: ?[]const u8 = null,
    backup_id: ?[]const u8 = null,
    location: []const u8 = "",
    location_explicit: bool = false,
    restore_mode: ?[]const u8 = null,
    url: ?[]const u8 = null,
    input_path: ?[]const u8 = null,
    connection: ?[]const u8 = null,
    idempotency_key: ?[]const u8 = null,
    wait: bool = false,
    wait_timeout_ms: u64 = default_restore_wait_timeout_ms,
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
    validateBackupArgs(opts) catch |err| switch (err) {
        error.BackupLocationRequired => cli.fatal("--location is required", .{}),
        else => cli.fatal("invalid backup arguments", .{}),
    };

    if (opts.url) |value| try client.setBaseUrl(value);

    if (opts.list_backups) {
        if (opts.output) |value| {
            if (!std.mem.eql(u8, value, "json")) {
                cli.fatal("only JSON output is supported for backup --list", .{});
            }
        }
        var resp = try client.listBackups(.{ .location = opts.location, .connection = connection });
        defer resp.deinit();
        const result = if (resp.data) |*data| data.value else cli.fatal("invalid backup-list response", .{});
        try cli.writeJson(allocator, io, result);
        return;
    }

    if (opts.table_name) |tbl| {
        const selected_format = opts.format orelse "portable";
        if (!std.mem.eql(u8, selected_format, "native") and !std.mem.eql(u8, selected_format, "portable")) {
            cli.fatal("unsupported backup format: {s}", .{selected_format});
        }
        const bid = opts.backup_id orelse cli.fatal("--backup-id is required", .{});
        try client.backupTable(tbl, .{ .backup_id = bid, .location = opts.location, .connection = connection, .format = selected_format });
        std.debug.print("Backup command successful.\n", .{});
        return;
    }

    const bid = opts.backup_id orelse cli.fatal("--backup-id is required", .{});

    var names = std.ArrayListUnmanaged([]const u8).empty;
    defer names.deinit(allocator);
    var table_names: ?[]const []const u8 = null;
    if (opts.tables_str) |ts| {
        parseTableNames(allocator, &names, ts) catch |err| tableListFatal(err);
        table_names = names.items;
    }

    var resp = try client.clusterBackup(.{
        .backup_id = bid,
        .location = opts.location,
        .connection = connection,
        .format = opts.format orelse "portable",
        .table_names = table_names,
    });
    defer resp.deinit();
    const result = if (resp.data) |*data| data.value else cli.fatal("invalid cluster backup response", .{});
    try cli.writeJson(allocator, io, result);
    validateClusterBackupResult(result.status, result.tables) catch |err| switch (err) {
        error.ClusterBackupIncomplete => cli.fatal("cluster backup incomplete; inspect the result above", .{}),
        error.InvalidBackupResponse => cli.fatal("invalid or internally inconsistent cluster backup response", .{}),
    };
    std.debug.print("Backup command successful.\n", .{});
}

fn validateClusterBackupResult(status: []const u8, tables: []const antfly_client.types.TableBackupStatus) !void {
    var completed: usize = 0;
    var incomplete: usize = 0;
    for (tables) |table| {
        if (std.mem.eql(u8, table.status, "completed")) {
            completed += 1;
        } else if (std.mem.eql(u8, table.status, "failed") or std.mem.eql(u8, table.status, "skipped")) {
            incomplete += 1;
        } else {
            return error.InvalidBackupResponse;
        }
    }
    const expected = if (completed == 0) "failed" else if (incomplete > 0) "partial" else "completed";
    if (!std.mem.eql(u8, status, expected)) return error.InvalidBackupResponse;
    if (!std.mem.eql(u8, expected, "completed")) return error.ClusterBackupIncomplete;
}

pub fn runRestore(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const opts = parseRestoreArgs(args) catch cli.fatal("invalid restore arguments", .{});
    if (opts.help) {
        printRestoreUsage();
        return;
    }
    const connection = opts.connection orelse cli.fatal("--connection is required", .{});

    validateRestoreArgs(opts) catch |err| switch (err) {
        error.RestoreLocationRequired => cli.fatal("--location is required", .{}),
        error.RestoreInputModeUnsupported => cli.fatal("--input restore targets one table; omit --mode", .{}),
        else => cli.fatal("invalid restore arguments", .{}),
    };

    if (opts.url) |value| try client.setBaseUrl(value);

    if (opts.input_path) |input| {
        if (opts.tables_str != null) cli.fatal("--input restore supports exactly one --table", .{});
        const tbl = opts.table_name orelse cli.fatal("--table is required with --input", .{});
        const location = try restoreInputLocationAlloc(allocator, input, opts);
        defer allocator.free(location);
        var plan = try prepareInputRestorePlan(allocator, input, tbl, opts.backup_id, location, connection);
        defer plan.deinit(allocator);
        var resp = try client.restoreTableWithOptions(plan.tableName(), plan.request, .{ .idempotency_key = opts.idempotency_key });
        defer resp.deinit();
        try writeRestoreResponse(allocator, io, client, &resp, opts.wait, opts.wait_timeout_ms);
        return;
    }

    const bid = opts.backup_id orelse cli.fatal("--backup-id is required", .{});

    if (opts.table_name) |tbl| {
        var resp = try client.restoreTableWithOptions(tbl, .{ .backup_id = bid, .location = opts.location, .connection = connection }, .{ .idempotency_key = opts.idempotency_key });
        defer resp.deinit();
        try writeRestoreResponse(allocator, io, client, &resp, opts.wait, opts.wait_timeout_ms);
        return;
    }

    var names = std.ArrayListUnmanaged([]const u8).empty;
    defer names.deinit(allocator);
    var table_names: ?[]const []const u8 = null;
    if (opts.tables_str) |ts| {
        parseTableNames(allocator, &names, ts) catch |err| tableListFatal(err);
        table_names = names.items;
    }

    var resp = try client.clusterRestoreWithOptions(.{
        .backup_id = bid,
        .location = opts.location,
        .connection = connection,
        .table_names = table_names,
        .restore_mode = opts.restore_mode,
    }, .{ .idempotency_key = opts.idempotency_key });
    defer resp.deinit();
    try writeRestoreResponse(allocator, io, client, &resp, opts.wait, opts.wait_timeout_ms);
}

fn writeRestoreResponse(
    allocator: std.mem.Allocator,
    io: std.Io,
    client: *antfly_client.AntflyClient,
    accepted: *antfly_client.openapi.ApiResponse(antfly_client.types.RestoreJob),
    wait: bool,
    timeout_ms: u64,
) !void {
    const initial = if (accepted.data) |*data| data.value else return error.InvalidRestoreResponse;
    if (!wait or isTerminalRestorePhase(initial.phase)) {
        try cli.writeJson(allocator, io, initial);
        return restorePhaseResult(initial.phase);
    }

    var terminal = try waitForRestoreJob(client, io, initial.job_id, timeout_ms);
    defer terminal.deinit();
    const job = if (terminal.data) |*data| data.value else return error.InvalidRestoreResponse;
    try cli.writeJson(allocator, io, job);
    return restorePhaseResult(job.phase);
}

pub fn waitForRestoreJob(
    client: *antfly_client.AntflyClient,
    io: std.Io,
    job_id: i64,
    timeout_ms: u64,
) !antfly_client.openapi.ApiResponse(antfly_client.types.RestoreJob) {
    var job_id_buf: [32]u8 = undefined;
    const job_id_text = try std.fmt.bufPrint(&job_id_buf, "{d}", .{job_id});
    const started_ns = platform_time.monotonicNs();
    const timeout_ns = std.math.mul(u64, timeout_ms, std.time.ns_per_ms) catch std.math.maxInt(u64);
    while (true) {
        var response = try client.getRestoreJob(job_id_text);
        if (response.data) |*data| {
            if (isTerminalRestorePhase(data.value.phase)) return response;
        } else {
            response.deinit();
            return error.InvalidRestoreResponse;
        }
        response.deinit();
        const elapsed_ns = platform_time.monotonicNs() -| started_ns;
        if (elapsed_ns >= timeout_ns) return error.RestoreWaitTimeout;
        const poll_ns = restore_poll_interval_ms * std.time.ns_per_ms;
        const delay_ns = @min(poll_ns, timeout_ns - elapsed_ns);
        io.sleep(std.Io.Duration.fromNanoseconds(@intCast(delay_ns)), .awake) catch return error.RestoreWaitInterrupted;
    }
}

pub fn isTerminalRestorePhase(phase: []const u8) bool {
    return std.mem.eql(u8, phase, "succeeded") or std.mem.eql(u8, phase, "failed") or std.mem.eql(u8, phase, "cancelled");
}

pub fn restorePhaseResult(phase: []const u8) !void {
    if (std.mem.eql(u8, phase, "failed")) return error.RestoreJobFailed;
    if (std.mem.eql(u8, phase, "cancelled")) return error.RestoreJobCancelled;
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
            out.location_explicit = true;
        } else if (std.mem.eql(u8, arg, "--connection")) {
            out.connection = try nextRequired(args);
        } else if (std.mem.eql(u8, arg, "--format")) {
            out.format = try nextRequired(args);
        } else if (std.mem.eql(u8, arg, "--url")) {
            out.url = try nextRequired(args);
        } else if (std.mem.eql(u8, arg, "--output") or std.mem.eql(u8, arg, "-o")) {
            out.output = try nextRequired(args);
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
        } else if (std.mem.eql(u8, arg, "--mode")) {
            out.restore_mode = try nextRequired(args);
        } else if (std.mem.eql(u8, arg, "--input") or std.mem.eql(u8, arg, "-i")) {
            out.input_path = try nextRequired(args);
        } else if (std.mem.eql(u8, arg, "--url")) {
            out.url = try nextRequired(args);
        } else if (std.mem.eql(u8, arg, "--idempotency-key")) {
            out.idempotency_key = try nextRequired(args);
        } else if (std.mem.eql(u8, arg, "--wait")) {
            out.wait = true;
        } else if (std.mem.eql(u8, arg, "--wait-timeout")) {
            const seconds = try std.fmt.parseUnsigned(u64, try nextRequired(args), 10);
            out.wait_timeout_ms = try std.math.mul(u64, seconds, 1000);
            out.wait = true;
        } else {
            return error.UnknownArgument;
        }
    }
    return out;
}

fn validateBackupArgs(opts: BackupArgs) !void {
    if (!opts.location_explicit) return error.BackupLocationRequired;
    if (opts.table_name != null and opts.tables_str != null) return error.ConflictingTableSelection;
    if (opts.list_backups and (opts.table_name != null or opts.tables_str != null or opts.backup_id != null or opts.format != null))
        return error.ConflictingBackupListArguments;
    if (opts.format) |format| {
        if (!std.mem.eql(u8, format, "native") and !std.mem.eql(u8, format, "portable")) return error.UnsupportedBackupFormat;
    }
}

fn validateRestoreArgs(opts: RestoreArgs) !void {
    if (opts.table_name != null and opts.tables_str != null) return error.ConflictingTableSelection;
    if (opts.input_path) |input| {
        if (opts.tables_str != null or opts.table_name == null) return error.InvalidRestoreInputTarget;
        if (opts.restore_mode != null) return error.RestoreInputModeUnsupported;
        if (!isPortableRestoreInputPath(input)) return error.InvalidRestoreInputPath;
    }
    if (!opts.location_explicit) return error.RestoreLocationRequired;
}

fn isPortableRestoreInputPath(path: []const u8) bool {
    return std.mem.endsWith(u8, path, ".aflite") or std.mem.endsWith(u8, path, ".afb");
}

fn restoreInputLocationAlloc(allocator: std.mem.Allocator, input_path: []const u8, opts: RestoreArgs) ![]u8 {
    _ = input_path;
    if (!opts.location_explicit) return error.RestoreInputLocationRequired;
    return try allocator.dupe(u8, opts.location);
}

fn parseTableNames(allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged([]const u8), raw: []const u8) !void {
    var it = std.mem.splitScalar(u8, raw, ',');
    while (it.next()) |candidate| {
        const name = std.mem.trim(u8, candidate, " \t\r\n");
        if (name.len == 0) return error.InvalidTableName;
        if (out.items.len >= max_restore_tables) return error.TooManyRestoreTables;
        for (out.items) |existing| if (std.mem.eql(u8, existing, name)) return error.DuplicateTableName;
        try out.append(allocator, name);
    }
    if (out.items.len == 0) return error.InvalidTableName;
}

fn tableListFatal(err: anyerror) noreturn {
    switch (err) {
        error.InvalidTableName => cli.fatal("--tables contains an empty table name", .{}),
        error.DuplicateTableName => cli.fatal("--tables contains a duplicate table name", .{}),
        error.TooManyRestoreTables => cli.fatal("--tables supports at most {d} table names", .{max_restore_tables}),
        else => cli.fatal("invalid --tables value: {s}", .{@errorName(err)}),
    }
}

fn printBackupUsage() void {
    std.debug.print(
        \\usage:
        \\  antfly backup --table <name> --backup-id <id> --connection <id> --location <uri> [--format native|portable] [--url <url>]
        \\  antfly backup --tables <a,b> --backup-id <id> --connection <id> --location <uri> [--format native|portable] [--url <url>]
        \\  antfly backup --list --connection <id> --location <uri> [--output json] [--url <url>]
        \\
        \\notes:
        \\  Network backups are written by the server through the named connection.
        \\  Table and cluster backups default to the portable format.
        \\  Use `antfly lite backup <db.aflite> --out <backup.afb>` for a local AFB artifact.
        \\
    , .{});
}

fn printRestoreUsage() void {
    std.debug.print(
        \\usage:
        \\  antfly restore --table <name> --backup-id <id> --connection <id> --location <uri> [--idempotency-key <key>] [--wait] [--wait-timeout <seconds>] [--url <url>]
        \\  antfly restore --tables <a,b> --backup-id <id> --connection <id> --location <uri> [--mode <mode>] [--idempotency-key <key>] [--wait] [--wait-timeout <seconds>] [--url <url>]
        \\  antfly restore --input <db.aflite|backup.afb> --table <name> --connection <id> --location <shared-uri> [--backup-id <id>] [--idempotency-key <key>] [--wait] [--wait-timeout <seconds>] [--url <url>]
        \\
        \\notes:
        \\  `--input db.aflite` stages an Antfly Lite database as a portable backup,
        \\  then restores it through the normal Antfly table restore path.
        \\  Input restore requires a location writable by this client and readable
        \\  by the target's named connection. These may use different credentials.
        \\
    , .{});
}

fn nextRequired(args: *std.process.Args.Iterator) ![]const u8 {
    return args.next() orelse error.MissingArgument;
}

test "backup cli parser accepts help flag" {
    var argv = [_][*:0]const u8{"--help"};
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    const opts = try parseBackupArgs(&iter);
    try std.testing.expect(opts.help);
}

test "cluster backup CLI fails closed on incomplete and unknown results" {
    const completed = [_]antfly_client.types.TableBackupStatus{.{ .name = "docs", .status = "completed" }};
    const failed = [_]antfly_client.types.TableBackupStatus{.{ .name = "docs", .status = "failed" }};
    const partial = [_]antfly_client.types.TableBackupStatus{
        .{ .name = "docs", .status = "completed" },
        .{ .name = "events", .status = "failed" },
    };
    const unknown = [_]antfly_client.types.TableBackupStatus{.{ .name = "docs", .status = "queued" }};
    try validateClusterBackupResult("completed", &completed);
    try std.testing.expectError(error.ClusterBackupIncomplete, validateClusterBackupResult("partial", &partial));
    try std.testing.expectError(error.ClusterBackupIncomplete, validateClusterBackupResult("failed", &failed));
    try std.testing.expectError(error.InvalidBackupResponse, validateClusterBackupResult("completed", &failed));
    try std.testing.expectError(error.InvalidBackupResponse, validateClusterBackupResult("queued", &unknown));
}

test "backup cli parser rejects unknown arguments" {
    var argv = [_][*:0]const u8{ "--backup-id", "daily", "--bogus" };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    try std.testing.expectError(error.UnknownArgument, parseBackupArgs(&iter));
}

test "backup cli parser rejects local out path" {
    var argv = [_][*:0]const u8{
        "--table",
        "docs",
        "--format",
        "portable",
        "--out",
        "docs.afb",
    };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    try std.testing.expectError(error.UnknownArgument, parseBackupArgs(&iter));
}

test "network backup requires explicit location" {
    var argv = [_][*:0]const u8{ "--backup-id", "daily", "--connection", "archive-writer" };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    const opts = try parseBackupArgs(&iter);
    try std.testing.expectError(error.BackupLocationRequired, validateBackupArgs(opts));
}

test "backup rejects conflicting table selectors" {
    var argv = [_][*:0]const u8{ "--table", "one", "--tables", "two,three", "--location", "s3://archive/backups" };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    const opts = try parseBackupArgs(&iter);
    try std.testing.expectError(error.ConflictingTableSelection, validateBackupArgs(opts));
}

test "restore cli parser accepts aflite input shape" {
    var argv = [_][*:0]const u8{
        "--input",
        "app.aflite",
        "--table",
        "docs",
        "--location",
        "file:///tmp/backups",
        "--backup-id",
        "lite-app",
    };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    const opts = try parseRestoreArgs(&iter);
    try std.testing.expectEqualStrings("app.aflite", opts.input_path.?);
    try std.testing.expectEqualStrings("docs", opts.table_name.?);
    try std.testing.expectEqualStrings("file:///tmp/backups", opts.location);
    try std.testing.expect(opts.location_explicit);
    try std.testing.expectEqualStrings("lite-app", opts.backup_id.?);
}

test "restore CLI rejects ignored format selection" {
    var argv = [_][*:0]const u8{ "--table", "docs", "--format", "portable" };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    try std.testing.expectError(error.UnknownArgument, parseRestoreArgs(&iter));
}

test "restore input location requires explicit shared staging" {
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

    try std.testing.expectError(error.RestoreInputLocationRequired, restoreInputLocationAlloc(allocator, opts.input_path.?, opts));
}

test "restore afb input also requires explicit shared staging" {
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

    try std.testing.expectError(error.RestoreInputLocationRequired, restoreInputLocationAlloc(allocator, opts.input_path.?, opts));
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

test "network restore requires explicit location" {
    var argv = [_][*:0]const u8{ "--backup-id", "daily", "--connection", "archive-reader" };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    const opts = try parseRestoreArgs(&iter);
    try std.testing.expectError(error.RestoreLocationRequired, validateRestoreArgs(opts));
}

test "restore rejects conflicting table selectors" {
    var argv = [_][*:0]const u8{ "--table", "one", "--tables", "two,three", "--location", "s3://archive/backups" };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    const opts = try parseRestoreArgs(&iter);
    try std.testing.expectError(error.ConflictingTableSelection, validateRestoreArgs(opts));
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

test "restore table list trims names and rejects ambiguous input" {
    var names = std.ArrayListUnmanaged([]const u8).empty;
    defer names.deinit(std.testing.allocator);
    try parseTableNames(std.testing.allocator, &names, " alpha, beta ");
    try std.testing.expectEqual(@as(usize, 2), names.items.len);
    try std.testing.expectEqualStrings("alpha", names.items[0]);
    try std.testing.expectEqualStrings("beta", names.items[1]);

    names.clearRetainingCapacity();
    try std.testing.expectError(error.InvalidTableName, parseTableNames(std.testing.allocator, &names, "alpha,,beta"));
    names.clearRetainingCapacity();
    try std.testing.expectError(error.DuplicateTableName, parseTableNames(std.testing.allocator, &names, "alpha,alpha"));
}
