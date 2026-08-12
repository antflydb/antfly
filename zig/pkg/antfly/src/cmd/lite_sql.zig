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

//! Storage-runtime facade for the API-owned Lite SQL implementation.

const antfly = @import("antfly-zig");
const std = @import("std");
const builtin = @import("builtin");
const cli = @import("cli/mod.zig");
const linked_runtime_enabled = if (builtin.is_test) false else @import("linked_runtime_options").enabled;
const bridge = if (linked_runtime_enabled) antfly.public_api.lite_sql_bridge else struct {};
const runtime = antfly.public_api.lite_sql_runtime;
const LiteDb = antfly.lite.connection.Connection;
const LiteSqlDbSource = antfly.lite.sql_source.DbSource;

pub const max_sql_file_bytes = runtime.max_sql_file_bytes;
pub const max_repl_statement_bytes = runtime.max_repl_statement_bytes;
pub const Session = if (linked_runtime_enabled) bridge.Session else runtime.Session;
pub const firstStatementEnd = runtime.firstStatementEnd;

pub fn executeOneJsonAlloc(
    allocator: std.mem.Allocator,
    db: *antfly.db.DB,
    session: *Session,
    sql: []const u8,
) ![]u8 {
    var storage = LiteSqlDbSource.init(db);
    if (comptime linked_runtime_enabled) {
        return try session.executeJsonAlloc(allocator, storage.source(), sql);
    }
    return try runtime.executeOneWithSourceJsonAlloc(allocator, storage.source(), session, sql);
}

const RunOptions = struct {
    path: []const u8,
    command: ?[]const u8 = null,
    file_path: ?[]const u8 = null,
    catalog: cli.CatalogFlags,
};

pub fn runFromArgs(allocator: std.mem.Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    const path = args.next() orelse cli.fatal("database path is required", .{});
    return runWithOptions(allocator, io, try parseRunOptions(args, path, false));
}

pub fn runFromSqlArgs(allocator: std.mem.Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    return runWithOptions(allocator, io, try parseRunOptions(args, null, true));
}

fn parseRunOptions(args: *std.process.Args.Iterator, initial_path: ?[]const u8, sql_command_form: bool) !RunOptions {
    var path = initial_path;
    if (path) |value| try requireAflitePath(value);

    var command: ?[]const u8 = null;
    var file_path: ?[]const u8 = null;
    var catalog = cli.CatalogFlags.defaultsFromEnv();
    while (args.next()) |arg| {
        if (sql_command_form and std.mem.eql(u8, arg, "--lite")) {
            if (path != null) cli.fatal("--lite may only be specified once", .{});
            path = args.next() orelse cli.fatal("--lite requires a .aflite path", .{});
            try requireAflitePath(path.?);
        } else if (std.mem.eql(u8, arg, "-c") or std.mem.eql(u8, arg, "--command")) {
            command = args.next() orelse cli.fatal("{s} requires a SQL statement", .{arg});
        } else if (std.mem.eql(u8, arg, "-f") or std.mem.eql(u8, arg, "--file")) {
            file_path = args.next() orelse cli.fatal("{s} requires a path", .{arg});
        } else if (cli.parseCatalogFlag(&catalog, arg, args)) {
            continue;
        } else if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            if (sql_command_form) printSqlUsage() else printUsage();
            std.process.exit(0);
        } else {
            cli.fatal("unknown lite sql option: {s}", .{arg});
        }
    }

    if (command != null and file_path != null) cli.fatal("use only one of -c/--command or -f/--file", .{});
    return .{
        .path = path orelse cli.fatal("--lite requires a .aflite path", .{}),
        .command = command,
        .file_path = file_path,
        .catalog = catalog,
    };
}

fn runWithOptions(allocator: std.mem.Allocator, io: std.Io, options: RunOptions) !void {
    var session = try Session.init(allocator, .{
        .database = options.catalog.database,
        .namespace = options.catalog.namespace,
    });
    defer session.deinit(allocator);

    if (options.command) |sql| {
        if (!try executeSqlText(allocator, io, options.path, &session, sql, true)) return error.SqlCommandFailed;
        return;
    }
    if (options.file_path) |sql_path| {
        const sql = cli.readFileAlloc(io, allocator, sql_path, max_sql_file_bytes) catch |err| {
            cli.fatal("reading SQL file {s}: {}", .{ sql_path, err });
        };
        defer allocator.free(sql);
        if (!try executeSqlText(allocator, io, options.path, &session, sql, true)) return error.SqlCommandFailed;
        return;
    }
    return repl(allocator, io, options.path, &session);
}

pub fn executeSqlText(
    allocator: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    session: *Session,
    sql_text: []const u8,
    execute_trailing: bool,
) !bool {
    var ok = true;
    var rest = sql_text;
    while (true) {
        if (firstStatementEnd(rest)) |end| {
            const statement = std.mem.trim(u8, rest[0..end], " \t\r\n");
            if (statement.len != 0 and !try executeOne(allocator, io, path, session, statement)) ok = false;
            rest = rest[end + 1 ..];
            continue;
        }
        const trailing = std.mem.trim(u8, rest, " \t\r\n");
        if (execute_trailing and trailing.len != 0 and !try executeOne(allocator, io, path, session, trailing)) ok = false;
        return ok;
    }
}

pub fn repl(allocator: std.mem.Allocator, io: std.Io, path: []const u8, session: *Session) !void {
    var stdin_buf: [8192]u8 = undefined;
    var stdin_reader = std.Io.File.stdin().readerStreaming(io, &stdin_buf);
    var statement = std.ArrayListUnmanaged(u8).empty;
    defer statement.deinit(allocator);

    while (true) {
        cli.writeStdout(io, if (statement.items.len == 0) "antfly-lite=> " else "antfly-lite-> ");
        const line_raw = (try stdin_reader.interface.takeDelimiter('\n')) orelse break;
        const line = std.mem.trim(u8, line_raw, "\r\n");
        const trimmed = std.mem.trim(u8, line, " \t\r\n");
        if (statement.items.len == 0 and (std.mem.eql(u8, trimmed, "\\q") or std.mem.eql(u8, trimmed, ".quit"))) break;
        if (trimmed.len == 0 and statement.items.len == 0) continue;
        if (statement.items.len + line.len + 1 > max_repl_statement_bytes) {
            statement.clearRetainingCapacity();
            std.debug.print("statement too large\n", .{});
            continue;
        }
        try statement.appendSlice(allocator, line);
        try statement.append(allocator, '\n');
        if (firstStatementEnd(statement.items) == null) continue;
        _ = try executeSqlText(allocator, io, path, session, statement.items, false);
        statement.clearRetainingCapacity();
    }
    if (std.mem.trim(u8, statement.items, " \t\r\n").len != 0) std.debug.print("discarding incomplete SQL statement\n", .{});
}

fn executeOne(allocator: std.mem.Allocator, io: std.Io, path: []const u8, session: *Session, sql: []const u8) !bool {
    const read_only = (if (comptime linked_runtime_enabled)
        bridge.statementIsReadOnly(allocator, sql)
    else
        runtime.statementIsReadOnly(allocator, sql)) catch |err| {
        std.debug.print("SQL error: {}\n", .{err});
        return false;
    };
    var lite = try LiteDb.open(allocator, path, if (read_only) .query_readonly else .writer);
    defer lite.close();
    const body = executeOneJsonAlloc(allocator, &lite.db, session, sql) catch |err| {
        std.debug.print("SQL error: {}\n", .{err});
        return false;
    };
    defer allocator.free(body);
    cli.writeStdout(io, body);
    cli.writeStdout(io, "\n");
    return true;
}

fn requireAflitePath(path: []const u8) !void {
    if (std.mem.endsWith(u8, path, ".aflite")) return;
    std.debug.print("lite database path must end with .aflite: {s}\n", .{path});
    return error.InvalidArguments;
}

fn printUsage() void {
    std.debug.print("usage: antfly lite sql <db.aflite> [-c <sql> | -f <path>] [--database <name>] [--namespace <name>]\n", .{});
}

fn printSqlUsage() void {
    std.debug.print("usage: antfly sql --lite <db.aflite> [-c <sql> | -f <path>] [--database <name>] [--namespace <name>]\n", .{});
}

test "sql command form extracts lite path without consuming options early" {
    var argv = [_][*:0]const u8{ "--database", "analytics", "--lite", "local.aflite", "-c", "SELECT 1;" };
    var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    const options = try parseRunOptions(&args, null, true);
    try std.testing.expectEqualStrings("local.aflite", options.path);
    try std.testing.expectEqualStrings("SELECT 1;", options.command.?);
    try std.testing.expectEqualStrings("analytics", options.catalog.database.?);
}
