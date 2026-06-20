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
const lite_restore_staging = @import("../lite_restore_staging.zig");

const BackupArgs = struct {
    help: bool = false,
    table_name: ?[]const u8 = null,
    tables_str: ?[]const u8 = null,
    backup_id: ?[]const u8 = null,
    location: []const u8 = "file:///tmp/antfly_backups",
    format: ?[]const u8 = null,
    url: ?[]const u8 = null,
    output: ?[]const u8 = null,
    out_path: ?[]const u8 = null,
    list_backups: bool = false,
};

const RestoreArgs = struct {
    help: bool = false,
    table_name: ?[]const u8 = null,
    tables_str: ?[]const u8 = null,
    backup_id: ?[]const u8 = null,
    location: []const u8 = "file:///tmp/antfly_backups",
    format: ?[]const u8 = null,
    restore_mode: ?[]const u8 = null,
    url: ?[]const u8 = null,
    input_path: ?[]const u8 = null,
};

pub fn runBackup(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const opts = parseBackupArgs(args) catch cli.fatal("invalid backup arguments", .{});
    if (opts.help) {
        printBackupUsage();
        return;
    }

    if (opts.url) |value| try client.setBaseUrl(value);

    if (opts.list_backups) {
        if (opts.output) |value| {
            if (!std.mem.eql(u8, value, "json")) {
                cli.fatal("only JSON output is supported for backup --list", .{});
            }
        }
        var resp = try client.listBackups(.{ .location = opts.location });
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
        try client.backupTable(tbl, .{ .backup_id = bid, .location = location, .format = selected_format });
        if (out_plan) |plan| {
            try requireReadableOutputFile(allocator, io, plan.out_path);
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

    if (opts.url) |value| try client.setBaseUrl(value);

    if (opts.input_path) |input| {
        if (opts.tables_str != null) cli.fatal("--input restore supports exactly one --table", .{});
        const tbl = opts.table_name orelse cli.fatal("--table is required with --input", .{});
        if (opts.format) |value| {
            if (!std.mem.eql(u8, value, "portable")) {
                cli.fatal("--input restore is portable; omit --format or use --format portable", .{});
            }
        }

        var owned_backup_id: ?[]u8 = null;
        defer if (owned_backup_id) |value| allocator.free(value);
        const bid = opts.backup_id orelse blk: {
            owned_backup_id = try lite_restore_staging.defaultBackupIdAlloc(allocator, input);
            break :blk owned_backup_id.?;
        };

        var staged = try lite_restore_staging.stageInputRestoreBackup(allocator, input, tbl, bid, opts.location);
        defer staged.deinit(allocator);
        try client.restoreTable(tbl, .{
            .backup_id = staged.backup_id,
            .location = staged.location,
            .format = "portable",
        });
        std.debug.print("Restore command successfully initiated.\n", .{});
        return;
    }

    const bid = opts.backup_id orelse cli.fatal("--backup-id is required", .{});

    if (opts.table_name) |tbl| {
        try client.restoreTable(tbl, .{ .backup_id = bid, .location = opts.location, .format = opts.format });
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
        .table_names = table_names,
        .restore_mode = opts.restore_mode,
    });
    defer resp.deinit();
    if (resp.data) |data| {
        try cli.writeJson(allocator, io, data.value);
    }
    std.debug.print("Restore command successfully initiated.\n", .{});
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

fn printBackupUsage() void {
    std.debug.print(
        \\usage:
        \\  antfly backup --table <name> --backup-id <id> [--location <uri>] [--format native|portable] [--url <url>]
        \\  antfly backup --table <name> --format portable --out <backup.afb> [--url <url>]
        \\  antfly backup --tables <a,b> --backup-id <id> [--location <uri>] [--url <url>]
        \\  antfly backup --list [--location <uri>] [--output json] [--url <url>]
        \\
        \\notes:
        \\  `--out` writes a single portable AFB file for Lite restore/import.
        \\
    , .{});
}

fn printRestoreUsage() void {
    std.debug.print(
        \\usage:
        \\  antfly restore --table <name> --backup-id <id> [--location <uri>] [--format native|portable] [--url <url>]
        \\  antfly restore --tables <a,b> --backup-id <id> [--location <uri>] [--mode <mode>] [--url <url>]
        \\  antfly restore --input <db.aflite|backup.afb> --table <name> [--backup-id <id>] [--location <uri>] [--url <url>]
        \\
        \\notes:
        \\  `--input db.aflite` stages an Antfly Lite database as a portable backup,
        \\  then restores it through the normal Antfly table restore path.
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

fn requireReadableOutputFile(_: std.mem.Allocator, io: std.Io, path: []const u8) !void {
    var file = (if (std.fs.path.isAbsolute(path))
        std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        std.Io.Dir.cwd().openFile(io, path, .{})) catch {
        cli.fatal("portable backup completed but local --out file is not readable: {s}", .{path});
    };
    defer file.close(io);
    const stat = try file.stat(io);
    if (stat.size == 0) cli.fatal("portable backup completed but local --out file is empty: {s}", .{path});
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
    try std.testing.expectEqualStrings("lite-app", opts.backup_id.?);
}

test "restore cli parser accepts help flag" {
    var argv = [_][*:0]const u8{"help"};
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    const opts = try parseRestoreArgs(&iter);
    try std.testing.expect(opts.help);
}

test "restore cli parser rejects unknown arguments" {
    var argv = [_][*:0]const u8{ "--input", "app.aflite", "--table", "docs", "--bogus" };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    try std.testing.expectError(error.UnknownArgument, parseRestoreArgs(&iter));
}
