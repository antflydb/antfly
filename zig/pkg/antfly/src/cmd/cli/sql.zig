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
const platform = @import("antfly_platform");
const linked_runtime_options = @import("linked_runtime_options");
const lite_sql = if (linked_runtime_options.enabled) struct {} else @import("../lite_sql.zig");
const cli = @import("mod.zig");

const max_sql_file_bytes = 64 * 1024 * 1024;
const max_repl_statement_bytes = 16 * 1024 * 1024;

const SqlCliOptions = struct {
    command: ?[]const u8 = null,
    file_path: ?[]const u8 = null,
    lite_path: ?[]const u8 = null,
    http_host: ?[]const u8 = null,
    http_port: ?u16 = null,
    pgwire_host: []const u8 = "127.0.0.1",
    pgwire_host_set: bool = false,
    pgwire_port: ?u16 = null,
    pgwire_user: []const u8 = "antfly",
    pgwire_password: ?[]const u8 = null,
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
    if (opts.lite_path != null and opts.pgwire_port != null) {
        cli.fatal("use only one of --lite or --pgwire-port", .{});
    }
    if (opts.pgwire_host_set and opts.pgwire_port == null) {
        cli.fatal("--pgwire-host requires --pgwire-port", .{});
    }
    if ((opts.http_host != null or opts.http_port != null) and opts.lite_path != null) {
        cli.fatal("--host/--port are only supported for remote SQL", .{});
    }
    if ((opts.http_host != null or opts.http_port != null) and opts.pgwire_port != null) {
        cli.fatal("use --pgwire-host/--pgwire-port for pgwire SQL", .{});
    }

    if (opts.lite_path) |path| {
        if (comptime linked_runtime_options.enabled) {
            // The thin dispatcher sends this form to the storage-owning Lite runtime.
            return error.InvalidArguments;
        } else {
            var session = try lite_sql.Session.init(allocator, opts.catalog);
            defer session.deinit(allocator);

            if (opts.command) |sql| {
                if (!try lite_sql.executeSqlText(allocator, io, path, &session, sql, true)) {
                    return error.SqlCommandFailed;
                }
                return;
            }

            if (opts.file_path) |sql_path| {
                const sql = cli.readFileAlloc(io, allocator, sql_path, lite_sql.max_sql_file_bytes) catch |err| {
                    cli.fatal("reading SQL file {s}: {}", .{ sql_path, err });
                };
                defer allocator.free(sql);
                if (!try lite_sql.executeSqlText(allocator, io, path, &session, sql, true)) {
                    return error.SqlCommandFailed;
                }
                return;
            }

            return lite_sql.repl(allocator, io, path, &session);
        }
    }

    if (opts.pgwire_port) |port| {
        return runPgwire(allocator, io, opts, port);
    }

    var session: SqlSession = .{};
    if (opts.http_host != null or opts.http_port != null) {
        const base_url = try httpBaseUrlAlloc(allocator, opts);
        defer allocator.free(base_url);
        try client.setBaseUrl(base_url);
    }
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
    opts.pgwire_user = defaultPgwireUser();
    opts.pgwire_password = defaultPgwirePassword();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "-c") or std.mem.eql(u8, arg, "--command")) {
            opts.command = args.next() orelse cli.fatal("{s} requires a SQL statement", .{arg});
        } else if (std.mem.eql(u8, arg, "-f") or std.mem.eql(u8, arg, "--file")) {
            opts.file_path = args.next() orelse cli.fatal("{s} requires a path", .{arg});
        } else if (std.mem.eql(u8, arg, "--lite")) {
            opts.lite_path = args.next() orelse cli.fatal("--lite requires a .aflite path", .{});
        } else if (std.mem.eql(u8, arg, "--host")) {
            opts.http_host = args.next() orelse cli.fatal("--host requires a host", .{});
        } else if (std.mem.eql(u8, arg, "--port")) {
            const raw = args.next() orelse cli.fatal("--port requires a port", .{});
            opts.http_port = std.fmt.parseInt(u16, raw, 10) catch cli.fatal("invalid --port: {s}", .{raw});
        } else if (std.mem.eql(u8, arg, "--pgwire-host")) {
            opts.pgwire_host = args.next() orelse cli.fatal("--pgwire-host requires a host", .{});
            opts.pgwire_host_set = true;
        } else if (std.mem.eql(u8, arg, "--pgwire-port")) {
            const raw = args.next() orelse cli.fatal("--pgwire-port requires a port", .{});
            opts.pgwire_port = std.fmt.parseInt(u16, raw, 10) catch cli.fatal("invalid --pgwire-port: {s}", .{raw});
        } else if (std.mem.eql(u8, arg, "--pgwire-user")) {
            opts.pgwire_user = args.next() orelse cli.fatal("--pgwire-user requires a user", .{});
        } else if (std.mem.eql(u8, arg, "--pgwire-password")) {
            opts.pgwire_password = args.next() orelse cli.fatal("--pgwire-password requires a password", .{});
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

fn httpBaseUrlAlloc(allocator: std.mem.Allocator, opts: SqlCliOptions) ![]u8 {
    return try std.fmt.allocPrint(
        allocator,
        "http://{s}:{d}",
        .{ opts.http_host orelse "127.0.0.1", opts.http_port orelse 8080 },
    );
}

fn runPgwire(allocator: std.mem.Allocator, io: std.Io, opts: SqlCliOptions, port: u16) !void {
    var address = try std.Io.net.IpAddress.resolve(io, opts.pgwire_host, port);
    const stream = try address.connect(io, .{ .mode = .stream });
    defer stream.close(io);

    var read_buffer: [16 * 1024]u8 = undefined;
    var write_buffer: [16 * 1024]u8 = undefined;
    var reader_state = stream.reader(io, &read_buffer);
    var writer_state = stream.writer(io, &write_buffer);
    var pgwire = PgwireConnection{
        .allocator = allocator,
        .reader = &reader_state.interface,
        .writer = &writer_state.interface,
    };
    try pgwire.startup(opts.catalog, opts.pgwire_user, opts.pgwire_password);

    if (opts.command) |sql| {
        if (!try executePgwireSqlText(allocator, io, &pgwire, sql, true)) {
            return error.SqlCommandFailed;
        }
        return;
    }

    if (opts.file_path) |path| {
        const sql = cli.readFileAlloc(io, allocator, path, max_sql_file_bytes) catch |err| {
            cli.fatal("reading SQL file {s}: {}", .{ path, err });
        };
        defer allocator.free(sql);
        if (!try executePgwireSqlText(allocator, io, &pgwire, sql, true)) {
            return error.SqlCommandFailed;
        }
        return;
    }

    return replPgwire(allocator, io, &pgwire);
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

fn replPgwire(
    allocator: std.mem.Allocator,
    io: std.Io,
    pgwire: *PgwireConnection,
) !void {
    var stdin_buf: [8192]u8 = undefined;
    var stdin_reader = std.Io.File.stdin().readerStreaming(io, &stdin_buf);

    var statement = std.ArrayListUnmanaged(u8).empty;
    defer statement.deinit(allocator);

    while (true) {
        cli.writeStdout(io, if (statement.items.len == 0) "antfly-pgwire=> " else "antfly-pgwire-> ");
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
        _ = try executePgwireSqlText(allocator, io, pgwire, statement.items, false);
        statement.clearRetainingCapacity();
    }

    const trailing = std.mem.trim(u8, statement.items, " \t\r\n");
    if (trailing.len != 0) {
        std.debug.print("discarding incomplete SQL statement\n", .{});
    }
}

fn executePgwireSqlText(
    allocator: std.mem.Allocator,
    io: std.Io,
    pgwire: *PgwireConnection,
    sql_text: []const u8,
    execute_trailing: bool,
) !bool {
    var ok = true;
    var rest = sql_text;
    while (true) {
        if (firstStatementEnd(rest)) |end| {
            const statement = std.mem.trim(u8, rest[0..end], " \t\r\n");
            if (statement.len != 0 and !try executeOnePgwire(allocator, io, pgwire, statement)) ok = false;
            rest = rest[end + 1 ..];
            continue;
        }

        const trailing = std.mem.trim(u8, rest, " \t\r\n");
        if (execute_trailing and trailing.len != 0) {
            if (!try executeOnePgwire(allocator, io, pgwire, trailing)) ok = false;
        }
        return ok;
    }
}

fn executeOnePgwire(
    allocator: std.mem.Allocator,
    io: std.Io,
    pgwire: *PgwireConnection,
    sql: []const u8,
) !bool {
    var result = try pgwire.simpleQuery(sql);
    defer result.deinit(allocator);
    if (result.error_message) |message| {
        if (result.error_sqlstate) |sqlstate| {
            std.debug.print("SQL error {s}: {s}\n", .{ sqlstate, message });
        } else {
            std.debug.print("SQL error: {s}\n", .{message});
        }
        return false;
    }
    try result.writeJson(allocator, io);
    return true;
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
        \\usage: antfly sql [--lite <db.aflite> | --pgwire-port <port>] [-c <sql> | -f <path>] [--database <name>] [--namespace <name>]
        \\
        \\Without -c or -f, starts a small psql-style REPL. End statements with
        \\a semicolon. Use \q or .quit to exit.
        \\
        \\HTTP options:
        \\  --host <host>        HTTP API host (default: 127.0.0.1 when --port is used)
        \\  --port <port>        HTTP API port (default: 8080 when --host is used)
        \\
        \\Pgwire options:
        \\  --pgwire-host <host>      Pgwire host (default: 127.0.0.1)
        \\  --pgwire-port <port>      Connect through the PostgreSQL wire adapter
        \\  --pgwire-user <user>      Pgwire user (default: ANTFLY_PGWIRE_USER, PGUSER, or antfly)
        \\  --pgwire-password <pass>  Pgwire password (default: ANTFLY_PGWIRE_PASSWORD or PGPASSWORD)
        \\
    , .{});
}

const PgwireConnection = struct {
    allocator: std.mem.Allocator,
    reader: *std.Io.Reader,
    writer: *std.Io.Writer,

    fn startup(self: *PgwireConnection, catalog: cli.CatalogFlags, user: []const u8, password: ?[]const u8) !void {
        var payload: std.Io.Writer.Allocating = .init(self.allocator);
        defer payload.deinit();
        try payload.writer.writeInt(i32, 196608, .big);
        try writeStartupParam(&payload.writer, "user", user);
        if (catalog.database) |database| try writeStartupParam(&payload.writer, "database", database);
        if (catalog.namespace) |namespace| {
            const options = try std.fmt.allocPrint(self.allocator, "-c search_path={s}", .{namespace});
            defer self.allocator.free(options);
            try writeStartupParam(&payload.writer, "options", options);
        }
        try payload.writer.writeByte(0);

        try self.writer.writeInt(i32, @intCast(payload.written().len + 4), .big);
        try self.writer.writeAll(payload.written());
        try self.writer.flush();

        while (true) {
            const tag = try self.reader.takeByte();
            const message = try self.readMessagePayload();
            defer self.allocator.free(message);
            switch (tag) {
                'R' => {
                    if (message.len < 4) return error.InvalidPgwireMessage;
                    const auth_code = std.mem.readInt(i32, message[0..4], .big);
                    switch (auth_code) {
                        0 => {},
                        3 => {
                            const value = password orelse {
                                std.debug.print("pgwire server requested a password; use --pgwire-password or PGPASSWORD\n", .{});
                                return error.PgwirePasswordRequired;
                            };
                            try self.sendPassword(value);
                        },
                        else => return error.PgwireAuthenticationUnsupported,
                    }
                },
                'S', 'K' => {},
                'E' => {
                    var err = try parsePgwireError(self.allocator, message);
                    defer err.deinit(self.allocator);
                    if (err.sqlstate) |sqlstate| {
                        if (err.message) |text| std.debug.print("pgwire startup error {s}: {s}\n", .{ sqlstate, text });
                    } else if (err.message) |text| {
                        std.debug.print("pgwire startup error: {s}\n", .{text});
                    }
                    return error.PgwireStartupFailed;
                },
                'Z' => return,
                else => {},
            }
        }
    }

    fn sendPassword(self: *PgwireConnection, password: []const u8) !void {
        try self.writer.writeByte('p');
        try self.writer.writeInt(i32, @intCast(password.len + 5), .big);
        try self.writer.writeAll(password);
        try self.writer.writeByte(0);
        try self.writer.flush();
    }

    fn simpleQuery(self: *PgwireConnection, sql: []const u8) !PgwireResult {
        var payload: std.Io.Writer.Allocating = .init(self.allocator);
        defer payload.deinit();
        try payload.writer.writeAll(sql);
        try payload.writer.writeByte(0);
        try self.writer.writeByte('Q');
        try self.writer.writeInt(i32, @intCast(payload.written().len + 4), .big);
        try self.writer.writeAll(payload.written());
        try self.writer.flush();

        var result = PgwireResult{};
        errdefer result.deinit(self.allocator);

        while (true) {
            const tag = try self.reader.takeByte();
            const message = try self.readMessagePayload();
            defer self.allocator.free(message);
            switch (tag) {
                'T' => {
                    try result.replaceColumns(self.allocator, message);
                },
                'D' => {
                    try result.appendDataRow(self.allocator, message);
                },
                'C' => {
                    if (result.command_tag) |old| self.allocator.free(old);
                    result.command_tag = try self.allocator.dupe(u8, std.mem.sliceTo(message, 0));
                },
                'E' => {
                    var err = try parsePgwireError(self.allocator, message);
                    defer err.deinit(self.allocator);
                    if (err.sqlstate) |sqlstate| result.error_sqlstate = try self.allocator.dupe(u8, sqlstate);
                    if (err.message) |text| result.error_message = try self.allocator.dupe(u8, text);
                },
                'I' => {
                    result.empty_query = true;
                },
                'Z' => return result,
                'n' => {},
                else => {},
            }
        }
    }

    fn readMessagePayload(self: *PgwireConnection) ![]u8 {
        const len = try self.reader.takeInt(i32, .big);
        if (len < 4 or len > 16 * 1024 * 1024) return error.InvalidPgwireMessage;
        return try self.reader.readAlloc(self.allocator, @intCast(len - 4));
    }
};

fn defaultPgwireUser() []const u8 {
    return platform.env.getenv("ANTFLY_PGWIRE_USER") orelse
        platform.env.getenv("PGUSER") orelse
        "antfly";
}

fn defaultPgwirePassword() ?[]const u8 {
    return platform.env.getenv("ANTFLY_PGWIRE_PASSWORD") orelse
        platform.env.getenv("PGPASSWORD");
}

const PgwireResult = struct {
    columns: []const []const u8 = &.{},
    rows: []PgwireRow = &.{},
    command_tag: ?[]u8 = null,
    error_sqlstate: ?[]u8 = null,
    error_message: ?[]u8 = null,
    empty_query: bool = false,

    fn deinit(self: *PgwireResult, allocator: std.mem.Allocator) void {
        for (self.columns) |column| allocator.free(@constCast(column));
        if (self.columns.len > 0) allocator.free(self.columns);
        for (self.rows) |*row| row.deinit(allocator);
        if (self.rows.len > 0) allocator.free(self.rows);
        if (self.command_tag) |tag| allocator.free(tag);
        if (self.error_sqlstate) |sqlstate| allocator.free(sqlstate);
        if (self.error_message) |message| allocator.free(message);
        self.* = undefined;
    }

    fn replaceColumns(self: *PgwireResult, allocator: std.mem.Allocator, payload: []const u8) !void {
        for (self.columns) |column| allocator.free(@constCast(column));
        if (self.columns.len > 0) allocator.free(self.columns);
        self.columns = &.{};

        var index: usize = 0;
        if (payload.len < 2) return error.InvalidPgwireMessage;
        const count_i16 = readI16(payload, &index);
        if (count_i16 < 0) return error.InvalidPgwireMessage;
        const count: usize = @intCast(count_i16);
        var columns = try allocator.alloc([]const u8, count);
        errdefer allocator.free(columns);
        var filled: usize = 0;
        errdefer for (columns[0..filled]) |column| allocator.free(@constCast(column));

        while (filled < count) : (filled += 1) {
            const name = try readCString(payload, &index);
            columns[filled] = try allocator.dupe(u8, name);
            if (index + 18 > payload.len) return error.InvalidPgwireMessage;
            index += 18;
        }
        self.columns = columns;
    }

    fn appendDataRow(self: *PgwireResult, allocator: std.mem.Allocator, payload: []const u8) !void {
        var index: usize = 0;
        if (payload.len < 2) return error.InvalidPgwireMessage;
        const count_i16 = readI16(payload, &index);
        if (count_i16 < 0) return error.InvalidPgwireMessage;
        const count: usize = @intCast(count_i16);
        var values = try allocator.alloc(?[]u8, count);
        errdefer allocator.free(values);
        @memset(values, null);
        var filled: usize = 0;
        errdefer {
            for (values[0..filled]) |value| if (value) |text| allocator.free(text);
        }

        while (filled < count) : (filled += 1) {
            if (index + 4 > payload.len) return error.InvalidPgwireMessage;
            const len = readI32(payload, &index);
            if (len == -1) {
                values[filled] = null;
                continue;
            }
            if (len < 0) return error.InvalidPgwireMessage;
            const value_len: usize = @intCast(len);
            if (index + value_len > payload.len) return error.InvalidPgwireMessage;
            values[filled] = try allocator.dupe(u8, payload[index .. index + value_len]);
            index += value_len;
        }

        const old_len = self.rows.len;
        const grown = try allocator.realloc(self.rows, old_len + 1);
        self.rows = grown;
        self.rows[old_len] = .{ .values = values };
    }

    fn writeJson(self: PgwireResult, allocator: std.mem.Allocator, io: std.Io) !void {
        var out: std.Io.Writer.Allocating = .init(allocator);
        defer out.deinit();
        const writer = &out.writer;
        const tag = self.command_tag orelse if (self.empty_query) "EMPTY" else "OK";
        const verb = pgwireCommandVerb(tag);
        const statement_kind = pgwireStatementKind(verb);
        const kind = pgwireResponseKind(verb);
        try writer.print("{{\"kind\":{f},\"statement_kind\":{f},\"result\":{{", .{
            std.json.fmt(kind, .{}),
            std.json.fmt(statement_kind, .{}),
        });
        if (std.mem.eql(u8, kind, "read")) {
            try writer.writeAll("\"rows\":");
            try self.writeRowsJson(writer);
        } else if (std.mem.eql(u8, kind, "write")) {
            const count_field = pgwireWriteCountField(verb);
            if (count_field) |field| {
                try writer.print("{f}:{d}", .{ std.json.fmt(field, .{}), pgwireCommandCount(tag) });
                if (self.rows.len != 0) try writer.writeByte(',');
            }
            if (self.rows.len != 0) {
                try writer.writeAll("\"returning\":");
                try self.writeRowsJson(writer);
            }
        }
        try writer.writeAll("}}");
        cli.writeStdout(io, out.written());
        cli.writeStdout(io, "\n");
    }

    fn writeRowsJson(self: PgwireResult, writer: *std.Io.Writer) !void {
        try writer.writeByte('[');
        for (self.rows, 0..) |row, row_i| {
            if (row_i != 0) try writer.writeByte(',');
            try writer.writeByte('{');
            const field_count = @min(self.columns.len, row.values.len);
            for (self.columns[0..field_count], 0..) |column, col_i| {
                if (col_i != 0) try writer.writeByte(',');
                try writer.print("{f}:", .{std.json.fmt(column, .{})});
                if (row.values[col_i]) |value| {
                    try writer.print("{f}", .{std.json.fmt(value, .{})});
                } else {
                    try writer.writeAll("null");
                }
            }
            try writer.writeByte('}');
        }
        try writer.writeByte(']');
    }
};

const PgwireRow = struct {
    values: []?[]u8,

    fn deinit(self: *PgwireRow, allocator: std.mem.Allocator) void {
        for (self.values) |value| if (value) |text| allocator.free(text);
        allocator.free(self.values);
        self.* = undefined;
    }
};

const PgwireError = struct {
    sqlstate: ?[]u8 = null,
    message: ?[]u8 = null,

    fn deinit(self: *PgwireError, allocator: std.mem.Allocator) void {
        if (self.sqlstate) |sqlstate| allocator.free(sqlstate);
        if (self.message) |message| allocator.free(message);
        self.* = undefined;
    }
};

fn parsePgwireError(allocator: std.mem.Allocator, payload: []const u8) !PgwireError {
    var err = PgwireError{};
    errdefer err.deinit(allocator);
    var index: usize = 0;
    while (index < payload.len and payload[index] != 0) {
        const code = payload[index];
        index += 1;
        const value = try readCString(payload, &index);
        switch (code) {
            'C' => err.sqlstate = try allocator.dupe(u8, value),
            'M' => err.message = try allocator.dupe(u8, value),
            else => {},
        }
    }
    return err;
}

fn writeStartupParam(writer: *std.Io.Writer, key: []const u8, value: []const u8) !void {
    try writer.writeAll(key);
    try writer.writeByte(0);
    try writer.writeAll(value);
    try writer.writeByte(0);
}

fn readCString(payload: []const u8, index: *usize) ![]const u8 {
    const start = index.*;
    while (index.* < payload.len and payload[index.*] != 0) : (index.* += 1) {}
    if (index.* >= payload.len) return error.InvalidPgwireMessage;
    const value = payload[start..index.*];
    index.* += 1;
    return value;
}

fn readI16(payload: []const u8, index: *usize) i16 {
    const value = std.mem.readInt(i16, payload[index.*..][0..2], .big);
    index.* += 2;
    return value;
}

fn readI32(payload: []const u8, index: *usize) i32 {
    const value = std.mem.readInt(i32, payload[index.*..][0..4], .big);
    index.* += 4;
    return value;
}

fn pgwireCommandVerb(tag: []const u8) []const u8 {
    const end = std.mem.indexOfScalar(u8, tag, ' ') orelse tag.len;
    return tag[0..end];
}

fn pgwireStatementKind(verb: []const u8) []const u8 {
    if (std.ascii.eqlIgnoreCase(verb, "SELECT")) return "query";
    if (std.ascii.eqlIgnoreCase(verb, "INSERT")) return "insert";
    if (std.ascii.eqlIgnoreCase(verb, "UPDATE")) return "update";
    if (std.ascii.eqlIgnoreCase(verb, "DELETE")) return "delete";
    if (std.ascii.eqlIgnoreCase(verb, "MERGE")) return "merge";
    if (std.ascii.eqlIgnoreCase(verb, "CREATE") or
        std.ascii.eqlIgnoreCase(verb, "ALTER") or
        std.ascii.eqlIgnoreCase(verb, "DROP") or
        std.ascii.eqlIgnoreCase(verb, "DDL"))
    {
        return "ddl";
    }
    if (std.ascii.eqlIgnoreCase(verb, "EMPTY")) return "empty";
    if (std.ascii.eqlIgnoreCase(verb, "OK")) return "ok";
    return "ddl";
}

fn pgwireResponseKind(verb: []const u8) []const u8 {
    if (std.ascii.eqlIgnoreCase(verb, "SELECT")) return "read";
    if (std.ascii.eqlIgnoreCase(verb, "INSERT") or
        std.ascii.eqlIgnoreCase(verb, "UPDATE") or
        std.ascii.eqlIgnoreCase(verb, "DELETE") or
        std.ascii.eqlIgnoreCase(verb, "MERGE"))
    {
        return "write";
    }
    if (std.ascii.eqlIgnoreCase(verb, "CREATE") or
        std.ascii.eqlIgnoreCase(verb, "ALTER") or
        std.ascii.eqlIgnoreCase(verb, "DROP") or
        std.ascii.eqlIgnoreCase(verb, "DDL"))
    {
        return "ddl";
    }
    return "ok";
}

fn pgwireWriteCountField(verb: []const u8) ?[]const u8 {
    if (std.ascii.eqlIgnoreCase(verb, "INSERT")) return "inserted";
    if (std.ascii.eqlIgnoreCase(verb, "UPDATE")) return "updated";
    if (std.ascii.eqlIgnoreCase(verb, "DELETE")) return "deleted";
    if (std.ascii.eqlIgnoreCase(verb, "MERGE")) return "matched";
    return null;
}

fn pgwireCommandCount(tag: []const u8) u64 {
    var it = std.mem.splitScalar(u8, tag, ' ');
    var count: u64 = 0;
    while (it.next()) |part| {
        if (part.len == 0) continue;
        count = std.fmt.parseInt(u64, part, 10) catch continue;
    }
    return count;
}

test "sql cli statement splitter ignores quoted semicolons" {
    try std.testing.expectEqual(@as(?usize, 35), firstStatementEnd("select ';' as semi, \"x;y\" from docs;"));
    try std.testing.expectEqual(@as(?usize, 22), firstStatementEnd("select $$a;b$$ as body;"));
    try std.testing.expectEqual(@as(?usize, null), firstStatementEnd("select 'unterminated;"));
}

test "sql cli dollar quote delimiter parser" {
    try std.testing.expectEqualStrings("$body$", dollarQuoteDelimiter("$body$select 1$body$").?);
    try std.testing.expectEqualStrings("$$", dollarQuoteDelimiter("$$select 1$$").?);
    try std.testing.expect(dollarQuoteDelimiter("$bad-tag$") == null);
}
