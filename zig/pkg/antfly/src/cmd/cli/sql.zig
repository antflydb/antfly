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

const max_sql_file_bytes = 64 * 1024 * 1024;
const max_repl_statement_bytes = 16 * 1024 * 1024;

const SqlCliOptions = struct {
    command: ?[]const u8 = null,
    file_path: ?[]const u8 = null,
    catalog: cli.CatalogFlags = .{},
};

const SqlSession = struct {
    session_id: ?i64 = null,
};

pub fn run(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const opts = parseArgs(args);
    if (opts.command != null and opts.file_path != null) {
        cli.fatal("use only one of -c/--command or -f/--file", .{});
    }

    var session: SqlSession = .{};
    if (opts.command) |sql| {
        if (!try executeSqlText(allocator, io, client, &session, opts.catalog, sql, true)) {
            return error.SqlCommandFailed;
        }
        return;
    }

    if (opts.file_path) |path| {
        const sql = cli.readFileAlloc(io, allocator, path, max_sql_file_bytes) catch |err| {
            cli.fatal("reading SQL file {s}: {}", .{ path, err });
        };
        defer allocator.free(sql);
        if (!try executeSqlText(allocator, io, client, &session, opts.catalog, sql, true)) {
            return error.SqlCommandFailed;
        }
        return;
    }

    return repl(allocator, io, client, &session, opts.catalog);
}

fn parseArgs(args: *std.process.Args.Iterator) SqlCliOptions {
    var opts = SqlCliOptions{ .catalog = cli.CatalogFlags.defaultsFromEnv() };
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "-c") or std.mem.eql(u8, arg, "--command")) {
            opts.command = args.next() orelse cli.fatal("{s} requires a SQL statement", .{arg});
        } else if (std.mem.eql(u8, arg, "-f") or std.mem.eql(u8, arg, "--file")) {
            opts.file_path = args.next() orelse cli.fatal("{s} requires a path", .{arg});
        } else if (cli.parseCatalogFlag(&opts.catalog, arg, args)) {
            continue;
        } else if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            printUsage();
            std.process.exit(0);
        } else {
            cli.fatal("unknown sql option: {s}", .{arg});
        }
    }
    return opts;
}

fn repl(
    allocator: std.mem.Allocator,
    io: std.Io,
    client: *antfly_client.AntflyClient,
    session: *SqlSession,
    catalog: cli.CatalogFlags,
) !void {
    var stdin_buf: [8192]u8 = undefined;
    var stdin_reader = std.Io.File.stdin().readerStreaming(io, &stdin_buf);

    var statement = std.ArrayListUnmanaged(u8).empty;
    defer statement.deinit(allocator);

    while (true) {
        cli.writeStdout(io, if (statement.items.len == 0) "antfly=> " else "antfly-> ");
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
        _ = try executeSqlText(allocator, io, client, session, catalog, statement.items, false);
        statement.clearRetainingCapacity();
    }

    const trailing = std.mem.trim(u8, statement.items, " \t\r\n");
    if (trailing.len != 0) {
        std.debug.print("discarding incomplete SQL statement\n", .{});
    }
}

fn executeSqlText(
    allocator: std.mem.Allocator,
    io: std.Io,
    client: *antfly_client.AntflyClient,
    session: *SqlSession,
    catalog: cli.CatalogFlags,
    sql_text: []const u8,
    execute_trailing: bool,
) !bool {
    var ok = true;
    var rest = sql_text;
    while (true) {
        if (firstStatementEnd(rest)) |end| {
            const statement = std.mem.trim(u8, rest[0..end], " \t\r\n");
            if (statement.len != 0 and !try executeOne(allocator, io, client, session, catalog, statement)) ok = false;
            rest = rest[end + 1 ..];
            continue;
        }

        const trailing = std.mem.trim(u8, rest, " \t\r\n");
        if (execute_trailing and trailing.len != 0) {
            if (!try executeOne(allocator, io, client, session, catalog, trailing)) ok = false;
        }
        return ok;
    }
}

fn executeOne(
    allocator: std.mem.Allocator,
    io: std.Io,
    client: *antfly_client.AntflyClient,
    session: *SqlSession,
    catalog: cli.CatalogFlags,
    sql: []const u8,
) !bool {
    var resp = try client.inner.executeSql(.{
        .sql = sql,
        .session_id = session.session_id,
        .database = catalog.database,
        .namespace = catalog.namespace,
    });
    defer resp.deinit();

    if (resp.status_code >= 300) {
        if (resp.err_body) |body| {
            std.debug.print("SQL error {d}: {s}\n", .{ resp.status_code, body });
        } else {
            std.debug.print("SQL error {d}\n", .{resp.status_code});
        }
        return false;
    }

    if (resp.data) |data| {
        session.session_id = data.value.session_id;
        try cli.writeJson(allocator, io, data.value);
        return true;
    }

    std.debug.print("SQL response {d} did not include a body\n", .{resp.status_code});
    return false;
}

fn firstStatementEnd(sql: []const u8) ?usize {
    var i: usize = 0;
    var state: enum { normal, single_quote, double_quote, line_comment, block_comment, dollar_quote } = .normal;
    var dollar_delim: []const u8 = "";
    while (i < sql.len) {
        switch (state) {
            .normal => {
                if (sql[i] == ';') return i;
                if (sql[i] == '\'') {
                    state = .single_quote;
                    i += 1;
                    continue;
                }
                if (sql[i] == '"') {
                    state = .double_quote;
                    i += 1;
                    continue;
                }
                if (sql[i] == '-' and i + 1 < sql.len and sql[i + 1] == '-') {
                    state = .line_comment;
                    i += 2;
                    continue;
                }
                if (sql[i] == '/' and i + 1 < sql.len and sql[i + 1] == '*') {
                    state = .block_comment;
                    i += 2;
                    continue;
                }
                if (sql[i] == '$') {
                    if (dollarQuoteDelimiter(sql[i..])) |delim| {
                        dollar_delim = delim;
                        state = .dollar_quote;
                        i += delim.len;
                        continue;
                    }
                }
                i += 1;
            },
            .single_quote => {
                if (sql[i] == '\'' and i + 1 < sql.len and sql[i + 1] == '\'') {
                    i += 2;
                    continue;
                }
                if (sql[i] == '\'') state = .normal;
                i += 1;
            },
            .double_quote => {
                if (sql[i] == '"' and i + 1 < sql.len and sql[i + 1] == '"') {
                    i += 2;
                    continue;
                }
                if (sql[i] == '"') state = .normal;
                i += 1;
            },
            .line_comment => {
                if (sql[i] == '\n') state = .normal;
                i += 1;
            },
            .block_comment => {
                if (sql[i] == '*' and i + 1 < sql.len and sql[i + 1] == '/') {
                    state = .normal;
                    i += 2;
                    continue;
                }
                i += 1;
            },
            .dollar_quote => {
                if (std.mem.startsWith(u8, sql[i..], dollar_delim)) {
                    state = .normal;
                    i += dollar_delim.len;
                    continue;
                }
                i += 1;
            },
        }
    }
    return null;
}

fn dollarQuoteDelimiter(sql: []const u8) ?[]const u8 {
    if (sql.len == 0 or sql[0] != '$') return null;
    var i: usize = 1;
    while (i < sql.len and sql[i] != '$') : (i += 1) {
        if (!std.ascii.isAlphanumeric(sql[i]) and sql[i] != '_') return null;
    }
    if (i >= sql.len or sql[i] != '$') return null;
    return sql[0 .. i + 1];
}

fn printUsage() void {
    std.debug.print(
        \\usage: antfly sql [-c <sql> | -f <path>] [--database <name>] [--namespace <name>]
        \\
        \\Without -c or -f, starts a small psql-style REPL. End statements with
        \\a semicolon. Use \q or .quit to exit.
        \\
    , .{});
}

test "sql cli statement splitter ignores quoted semicolons" {
    try std.testing.expectEqual(@as(?usize, 40), firstStatementEnd("select ';' as semi, \"x;y\" from docs;"));
    try std.testing.expectEqual(@as(?usize, 22), firstStatementEnd("select $$a;b$$ as body;"));
    try std.testing.expectEqual(@as(?usize, null), firstStatementEnd("select 'unterminated;"));
}

test "sql cli dollar quote delimiter parser" {
    try std.testing.expectEqualStrings("$body$", dollarQuoteDelimiter("$body$select 1$body$").?);
    try std.testing.expectEqualStrings("$$", dollarQuoteDelimiter("$$select 1$$").?);
    try std.testing.expect(dollarQuoteDelimiter("$bad-tag$") == null);
}
