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

pub fn runBackup(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    var table_name: ?[]const u8 = null;
    var tables_str: ?[]const u8 = null;
    var backup_id: ?[]const u8 = null;
    var location: []const u8 = "file:///tmp/antfly_backups";
    var list_backups = false;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            table_name = args.next();
        } else if (std.mem.eql(u8, arg, "--tables")) {
            tables_str = args.next();
        } else if (std.mem.eql(u8, arg, "--backup-id")) {
            backup_id = args.next();
        } else if (std.mem.eql(u8, arg, "--location")) {
            location = args.next() orelse location;
        } else if (std.mem.eql(u8, arg, "--list")) {
            list_backups = true;
        }
    }

    if (list_backups) {
        var resp = try client.listBackups(.{ .location = location });
        defer resp.deinit();
        if (resp.data) |data| {
            try cli.writeJson(allocator, io, data.value);
        }
        return;
    }

    const bid = backup_id orelse cli.fatal("--backup-id is required", .{});

    if (table_name) |tbl| {
        try client.backupTable(tbl, .{ .backup_id = bid, .location = location });
        std.debug.print("Backup command successful.\n", .{});
        return;
    }

    var table_names: ?[]const []const u8 = null;
    if (tables_str) |ts| {
        var names = std.ArrayListUnmanaged([]const u8).empty;
        var it = std.mem.splitScalar(u8, ts, ',');
        while (it.next()) |name| {
            try names.append(allocator, std.mem.trim(u8, name, " "));
        }
        table_names = names.items;
    }

    var resp = try client.clusterBackup(.{
        .backup_id = bid,
        .location = location,
        .table_names = table_names,
    });
    defer resp.deinit();
    if (resp.data) |data| {
        try cli.writeJson(allocator, io, data.value);
    }
    std.debug.print("Backup command successful.\n", .{});
}

pub fn runRestore(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    var table_name: ?[]const u8 = null;
    var tables_str: ?[]const u8 = null;
    var backup_id: ?[]const u8 = null;
    var location: []const u8 = "file:///tmp/antfly_backups";
    var restore_mode: ?[]const u8 = null;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            table_name = args.next();
        } else if (std.mem.eql(u8, arg, "--tables")) {
            tables_str = args.next();
        } else if (std.mem.eql(u8, arg, "--backup-id")) {
            backup_id = args.next();
        } else if (std.mem.eql(u8, arg, "--location")) {
            location = args.next() orelse location;
        } else if (std.mem.eql(u8, arg, "--mode")) {
            restore_mode = args.next();
        }
    }

    const bid = backup_id orelse cli.fatal("--backup-id is required", .{});

    if (table_name) |tbl| {
        try client.restoreTable(tbl, .{ .backup_id = bid, .location = location });
        std.debug.print("Restore command successfully initiated.\n", .{});
        return;
    }

    var table_names: ?[]const []const u8 = null;
    if (tables_str) |ts| {
        var names = std.ArrayListUnmanaged([]const u8).empty;
        var it = std.mem.splitScalar(u8, ts, ',');
        while (it.next()) |name| {
            try names.append(allocator, std.mem.trim(u8, name, " "));
        }
        table_names = names.items;
    }

    var resp = try client.clusterRestore(.{
        .backup_id = bid,
        .location = location,
        .table_names = table_names,
        .restore_mode = restore_mode,
    });
    defer resp.deinit();
    if (resp.data) |data| {
        try cli.writeJson(allocator, io, data.value);
    }
    std.debug.print("Restore command successfully initiated.\n", .{});
}
