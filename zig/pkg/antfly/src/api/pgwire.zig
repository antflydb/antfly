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
const http_common = @import("../raft/transport/http_common.zig");
const http_server = @import("http_server.zig");
const platform_clock = @import("../platform/clock.zig");
const sql_adapter = @import("../sql/mod.zig");

const pgwire_module = @This();

const protocol_version_3: i32 = 196608;
const ssl_request_code: i32 = 80877103;
const cancel_request_code: i32 = 80877102;
const max_packet_len: i32 = 16 * 1024 * 1024;
const text_oid: i32 = 25;
const text_type_size: i16 = -1;

pub const Config = struct {
    bind_host: []const u8,
    bind_port: u16,
    api_server: *http_server.ApiHttpServer,
};

pub const Server = struct {
    state: ?*State,

    pub fn deinit(self: *Server) void {
        const state = self.state orelse return;
        self.state = null;
        state.stop();
    }
};

const State = struct {
    alloc: std.mem.Allocator,
    owned_host: []u8,
    bind_port: u16,
    api_server: *http_server.ApiHttpServer,
    io_impl: std.Io.Threaded,
    listener: ?std.Io.net.Server = null,
    thread: ?std.Thread = null,
    stopping: std.atomic.Value(bool) = .init(false),
    active_connection_threads: std.atomic.Value(u32) = .init(0),

    fn stop(self: *State) void {
        const io = self.io_impl.io();
        const bound_addr = self.boundAddress();
        self.stopping.store(true, .release);
        if (self.thread) |thread| {
            if (bound_addr) |addr| {
                const wake_io = std.Io.Threaded.global_single_threaded.io();
                if (addr.connect(wake_io, .{ .mode = .stream })) |stream| {
                    var wake_stream = stream;
                    wake_stream.close(wake_io);
                } else |_| {}
            }
            thread.join();
            self.thread = null;
        }
        if (self.listener) |*listener| {
            listener.deinit(io);
            self.listener = null;
        }

        var wait_ms: u16 = 0;
        while (self.active_connection_threads.load(.acquire) != 0 and wait_ms < 5000) : (wait_ms += 1) {
            platform_clock.Clock.real().sleepMs(1);
        }
        if (self.active_connection_threads.load(.acquire) != 0) {
            std.log.warn("pgwire shutdown timed out waiting for active connections; leaking listener state to avoid use-after-free", .{});
            return;
        }

        self.io_impl.deinit();
        self.alloc.free(self.owned_host);
        self.alloc.destroy(self);
    }

    fn boundAddress(self: *const State) ?std.Io.net.IpAddress {
        const listener = self.listener orelse return null;
        return listener.socket.address;
    }
};

pub fn start(alloc: std.mem.Allocator, cfg: Config) !Server {
    const state = try alloc.create(State);
    errdefer alloc.destroy(state);
    const owned_host = try alloc.dupe(u8, cfg.bind_host);
    errdefer alloc.free(owned_host);
    state.* = .{
        .alloc = alloc,
        .owned_host = owned_host,
        .bind_port = cfg.bind_port,
        .api_server = cfg.api_server,
        .io_impl = std.Io.Threaded.init(alloc, .{}),
    };
    errdefer state.io_impl.deinit();

    const io = state.io_impl.io();
    var address = try std.Io.net.IpAddress.resolve(io, owned_host, cfg.bind_port);
    state.listener = try address.listen(io, .{ .reuse_address = true });
    errdefer {
        state.listener.?.deinit(io);
        state.listener = null;
    }

    state.thread = try std.Thread.spawn(.{}, serve, .{state});
    return .{ .state = state };
}

fn serve(state: *State) void {
    const io = state.io_impl.io();
    std.debug.print("pgwire listening on {s}:{d}\n", .{ state.owned_host, state.bind_port });

    while (true) {
        if (state.stopping.load(.acquire)) return;
        const stream = if (state.listener) |*listener|
            listener.accept(io) catch |err| switch (err) {
                error.SocketNotListening, error.Canceled => return,
                else => {
                    if (state.stopping.load(.acquire)) return;
                    std.log.warn("pgwire accept failed err={}", .{err});
                    platform_clock.Clock.real().sleepMs(1);
                    continue;
                },
            }
        else
            return;
        if (state.stopping.load(.acquire)) {
            var wake_stream = stream;
            wake_stream.close(io);
            return;
        }
        _ = state.active_connection_threads.fetchAdd(1, .acq_rel);
        const thread = std.Thread.spawn(.{}, serveConnection, .{ state, stream }) catch |err| {
            _ = state.active_connection_threads.fetchSub(1, .acq_rel);
            std.log.warn("pgwire connection spawn failed err={}", .{err});
            stream.close(io);
            continue;
        };
        thread.detach();
    }
}

fn serveConnection(
    state: *State,
    stream: std.Io.net.Stream,
) void {
    defer _ = state.active_connection_threads.fetchSub(1, .acq_rel);
    const alloc = state.alloc;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    defer stream.close(io);

    var read_buffer: [16 * 1024]u8 = undefined;
    var write_buffer: [16 * 1024]u8 = undefined;
    var reader_state = stream.reader(io, &read_buffer);
    var writer_state = stream.writer(io, &write_buffer);
    var conn = Connection{
        .alloc = alloc,
        .api_server = state.api_server,
        .reader = &reader_state.interface,
        .writer = &writer_state.interface,
    };
    defer conn.deinit();
    conn.run() catch |err| switch (err) {
        error.EndOfStream => {},
        else => std.log.warn("pgwire connection closed err={}", .{err}),
    };
}

const Connection = struct {
    alloc: std.mem.Allocator,
    api_server: *http_server.ApiHttpServer,
    reader: *std.Io.Reader,
    writer: *std.Io.Writer,
    session_id: ?u64 = null,
    database: ?[]u8 = null,
    namespace: ?[]u8 = null,

    fn deinit(self: *Connection) void {
        if (self.database) |database| self.alloc.free(database);
        if (self.namespace) |namespace| self.alloc.free(namespace);
        self.* = undefined;
    }

    fn run(self: *Connection) !void {
        try self.startup();
        while (true) {
            const tag = self.reader.takeByte() catch |err| switch (err) {
                error.EndOfStream => return,
                else => return err,
            };
            const len = try self.reader.takeInt(i32, .big);
            if (len < 4 or len > max_packet_len) return error.InvalidPgwireMessage;
            const payload = try self.reader.readAlloc(self.alloc, @intCast(len - 4));
            defer self.alloc.free(payload);

            switch (tag) {
                'Q' => try self.handleSimpleQuery(payload),
                'X' => return,
                'S' => try self.sendReadyForQuery(),
                else => {
                    try self.sendError("0A000", "unsupported pgwire message; simple query protocol only");
                    try self.sendReadyForQuery();
                },
            }
        }
    }

    fn startup(self: *Connection) !void {
        while (true) {
            const len = try self.reader.takeInt(i32, .big);
            if (len < 8 or len > max_packet_len) return error.InvalidPgwireStartup;
            const payload = try self.reader.readAlloc(self.alloc, @intCast(len - 4));
            defer self.alloc.free(payload);
            const code = std.mem.readInt(i32, payload[0..4], .big);
            switch (code) {
                ssl_request_code => {
                    try self.writer.writeByte('N');
                    try self.writer.flush();
                    continue;
                },
                cancel_request_code => return error.PgwireCancelNotSupported,
                protocol_version_3 => {
                    try self.applyStartupParams(payload[4..]);
                    if (self.api_server.cfg.auth_enabled) {
                        try self.sendError("0A000", "pgwire authentication is not implemented");
                        try self.writer.flush();
                        return error.PgwireAuthNotSupported;
                    }
                    try self.sendAuthenticationOk();
                    try self.sendParameterStatus("server_version", "16.0-antfly");
                    try self.sendParameterStatus("server_encoding", "UTF8");
                    try self.sendParameterStatus("client_encoding", "UTF8");
                    try self.sendParameterStatus("DateStyle", "ISO, MDY");
                    try self.sendParameterStatus("integer_datetimes", "on");
                    try self.sendParameterStatus("standard_conforming_strings", "on");
                    try self.sendParameterStatus("TimeZone", "UTC");
                    try self.sendBackendKeyData();
                    try self.sendReadyForQuery();
                    return;
                },
                else => {
                    try self.sendError("08P01", "unsupported postgres protocol version");
                    try self.writer.flush();
                    return error.UnsupportedPgwireStartup;
                },
            }
        }
    }

    fn applyStartupParams(self: *Connection, payload: []const u8) !void {
        var index: usize = 0;
        while (index < payload.len and payload[index] != 0) {
            const key_start = index;
            while (index < payload.len and payload[index] != 0) : (index += 1) {}
            if (index >= payload.len) return error.InvalidPgwireStartup;
            const key = payload[key_start..index];
            index += 1;

            const value_start = index;
            while (index < payload.len and payload[index] != 0) : (index += 1) {}
            if (index >= payload.len) return error.InvalidPgwireStartup;
            const value = payload[value_start..index];
            index += 1;

            if (std.mem.eql(u8, key, "database") and value.len != 0) {
                if (self.database) |old| self.alloc.free(old);
                self.database = try self.alloc.dupe(u8, value);
            } else if (std.mem.eql(u8, key, "options")) {
                if (startupSearchPath(value)) |namespace| {
                    if (self.namespace) |old| self.alloc.free(old);
                    self.namespace = try self.alloc.dupe(u8, namespace);
                }
            }
        }
    }

    fn handleSimpleQuery(self: *Connection, payload: []const u8) !void {
        const sql = std.mem.sliceTo(payload, 0);
        var executed = false;
        var rest = sql;
        while (true) {
            if (firstStatementEnd(rest)) |end| {
                const statement = std.mem.trim(u8, rest[0..end], " \t\r\n");
                if (statement.len != 0) {
                    executed = true;
                    if (!try self.executeAndEncodeOne(statement)) return;
                }
                rest = rest[end + 1 ..];
                continue;
            }

            const trailing = std.mem.trim(u8, rest, " \t\r\n");
            if (trailing.len != 0) {
                executed = true;
                if (!try self.executeAndEncodeOne(trailing)) return;
            }
            break;
        }

        if (!executed) {
            try self.sendEmptyQueryResponse();
        }
        try self.sendReadyForQuery();
    }

    fn executeAndEncodeOne(self: *Connection, sql: []const u8) !bool {
        var response = self.executeSql(sql) catch |err| {
            std.log.warn("pgwire sql execution failed err={}", .{err});
            try self.sendError("XX000", "internal sql execution error");
            try self.sendReadyForQuery();
            return false;
        };
        defer response.deinit(self.api_server.alloc);

        if (response.status < 200 or response.status >= 300) {
            try self.sendError(pgwire_module.sqlstateForHttpStatus(response.status), response.body);
            try self.sendReadyForQuery();
            return false;
        }
        try self.encodeSqlResponse(sql, response.body);
        return true;
    }

    fn executeSql(self: *Connection, sql: []const u8) !http_common.HttpResponse {
        var body: std.Io.Writer.Allocating = .init(self.alloc);
        defer body.deinit();
        const writer = &body.writer;
        try writer.print("{{\"sql\":{f}", .{std.json.fmt(sql, .{})});
        if (self.session_id) |id| try writer.print(",\"session_id\":{d}", .{id});
        if (self.database) |database| try writer.print(",\"database\":{f}", .{std.json.fmt(database, .{})});
        if (self.namespace) |namespace| try writer.print(",\"namespace\":{f}", .{std.json.fmt(namespace, .{})});
        try writer.writeAll(",\"read_only\":false");
        try writer.writeByte('}');
        return try self.api_server.handlePublicSql(body.written(), null);
    }

    fn encodeSqlResponse(self: *Connection, sql: []const u8, body: []const u8) !void {
        var parsed = std.json.parseFromSlice(std.json.Value, self.alloc, body, .{ .allocate = .alloc_always }) catch {
            try self.sendError("XX000", "invalid sql response");
            return;
        };
        defer parsed.deinit();
        if (parsed.value != .object) {
            try self.sendError("XX000", "invalid sql response");
            return;
        }
        if (pgwire_module.jsonU64(parsed.value.object.get("session_id"))) |id| self.session_id = id;
        const kind = pgwire_module.jsonString(parsed.value.object.get("kind")) orelse "ok";
        const statement_kind = pgwire_module.jsonString(parsed.value.object.get("statement_kind")) orelse kind;
        if (std.mem.eql(u8, kind, "ddl")) {
            try self.sendCommandComplete(pgwire_module.commandTagForDdlSql(self.alloc, sql) orelse pgwire_module.commandTagForDdlResponse(parsed.value));
            return;
        }
        if (std.mem.eql(u8, kind, "read")) {
            const result = parsed.value.object.get("result");
            const rows = pgwire_module.responseRows(result) orelse &.{};
            const columns = try pgwire_module.responseColumnNamesAlloc(self.alloc, result);
            defer if (columns) |names| self.alloc.free(names);
            const tag = try pgwire_module.commandTagForRows(self.alloc, "SELECT", rows.len);
            defer self.alloc.free(tag);
            try self.sendRows(rows, columns, tag);
            return;
        }
        if (std.mem.eql(u8, kind, "write")) {
            const result = parsed.value.object.get("result");
            if (pgwire_module.responseReturningRows(result)) |rows| {
                const columns = try pgwire_module.responseColumnNamesAlloc(self.alloc, result);
                defer if (columns) |names| self.alloc.free(names);
                const tag = try pgwire_module.commandTagForRows(self.alloc, statement_kind, rows.len);
                defer self.alloc.free(tag);
                try self.sendRows(rows, columns, tag);
            } else {
                const tag = try pgwire_module.commandTagForWrite(self.alloc, statement_kind, result);
                defer self.alloc.free(tag);
                try self.sendCommandComplete(tag);
            }
            return;
        }
        try self.sendCommandComplete(statement_kind);
    }

    fn sendRows(
        self: *Connection,
        rows: []const std.json.Value,
        schema_columns: ?[]const []const u8,
        tag: []const u8,
    ) !void {
        var row_description: std.Io.Writer.Allocating = .init(self.alloc);
        defer row_description.deinit();
        if (schema_columns) |columns| {
            try row_description.writer.writeInt(i16, @intCast(columns.len), .big);
            for (columns) |column| try pgwire_module.appendTextColumnDescription(&row_description.writer, column);
            try self.sendMessage('T', row_description.written());
            try self.sendDataRowsForColumnNames(rows, columns);
            try self.sendCommandComplete(tag);
            return;
        }

        if (rows.len == 0 or rows[0] != .object) {
            try row_description.writer.writeInt(i16, 0, .big);
            try self.sendMessage('T', row_description.written());
            try self.sendCommandComplete(tag);
            return;
        }

        const first_row_columns = rows[0].object;
        try row_description.writer.writeInt(i16, @intCast(first_row_columns.count()), .big);
        var column_it = first_row_columns.iterator();
        while (column_it.next()) |entry| try pgwire_module.appendTextColumnDescription(&row_description.writer, entry.key_ptr.*);
        try self.sendMessage('T', row_description.written());

        for (rows) |row| {
            var data: std.Io.Writer.Allocating = .init(self.alloc);
            defer data.deinit();
            try data.writer.writeInt(i16, @intCast(first_row_columns.count()), .big);
            var value_it = first_row_columns.iterator();
            while (value_it.next()) |entry| {
                try self.appendDataValue(&data.writer, row, entry.key_ptr.*);
            }
            try self.sendMessage('D', data.written());
        }

        try self.sendCommandComplete(tag);
    }

    fn sendDataRowsForColumnNames(self: *Connection, rows: []const std.json.Value, columns: []const []const u8) !void {
        for (rows) |row| {
            var data: std.Io.Writer.Allocating = .init(self.alloc);
            defer data.deinit();
            try data.writer.writeInt(i16, @intCast(columns.len), .big);
            for (columns) |column| try self.appendDataValue(&data.writer, row, column);
            try self.sendMessage('D', data.written());
        }
    }

    fn appendDataValue(self: *Connection, writer: *std.Io.Writer, row: std.json.Value, column: []const u8) !void {
        if (row != .object) {
            try writer.writeInt(i32, -1, .big);
            return;
        }
        const value = row.object.get(column) orelse {
            try writer.writeInt(i32, -1, .big);
            return;
        };
        if (value == .null) {
            try writer.writeInt(i32, -1, .big);
            return;
        }
        const text = try pgwire_module.jsonValueTextAlloc(self.alloc, value);
        defer self.alloc.free(text);
        try writer.writeInt(i32, @intCast(text.len), .big);
        try writer.writeAll(text);
    }

    fn sendAuthenticationOk(self: *Connection) !void {
        var payload: [4]u8 = undefined;
        std.mem.writeInt(i32, &payload, 0, .big);
        try self.sendMessage('R', &payload);
    }

    fn sendParameterStatus(self: *Connection, name: []const u8, value: []const u8) !void {
        var payload: std.Io.Writer.Allocating = .init(self.alloc);
        defer payload.deinit();
        try payload.writer.writeAll(name);
        try payload.writer.writeByte(0);
        try payload.writer.writeAll(value);
        try payload.writer.writeByte(0);
        try self.sendMessage('S', payload.written());
    }

    fn sendBackendKeyData(self: *Connection) !void {
        var payload: [8]u8 = undefined;
        std.mem.writeInt(i32, payload[0..4], 0, .big);
        std.mem.writeInt(i32, payload[4..8], 0, .big);
        try self.sendMessage('K', &payload);
    }

    fn sendReadyForQuery(self: *Connection) !void {
        try self.sendMessage('Z', "I");
        try self.writer.flush();
    }

    fn sendEmptyQueryResponse(self: *Connection) !void {
        try self.sendMessage('I', "");
    }

    fn sendCommandComplete(self: *Connection, tag: []const u8) !void {
        var payload: std.Io.Writer.Allocating = .init(self.alloc);
        defer payload.deinit();
        try payload.writer.writeAll(tag);
        try payload.writer.writeByte(0);
        try self.sendMessage('C', payload.written());
    }

    fn sendError(self: *Connection, sqlstate: []const u8, message: []const u8) !void {
        var payload: std.Io.Writer.Allocating = .init(self.alloc);
        defer payload.deinit();
        try payload.writer.writeByte('S');
        try payload.writer.writeAll("ERROR");
        try payload.writer.writeByte(0);
        try payload.writer.writeByte('C');
        try payload.writer.writeAll(sqlstate);
        try payload.writer.writeByte(0);
        try payload.writer.writeByte('M');
        try payload.writer.writeAll(std.mem.trim(u8, message, " \t\r\n"));
        try payload.writer.writeByte(0);
        try payload.writer.writeByte(0);
        try self.sendMessage('E', payload.written());
    }

    fn sendMessage(self: *Connection, tag: u8, payload: []const u8) !void {
        try self.writer.writeByte(tag);
        try self.writer.writeInt(i32, @intCast(payload.len + 4), .big);
        try self.writer.writeAll(payload);
    }
};

fn responseRows(result: ?std.json.Value) ?[]const std.json.Value {
    const value = result orelse return null;
    if (value != .object) return null;
    const rows = value.object.get("rows") orelse return null;
    if (rows != .array) return null;
    return rows.array.items;
}

fn responseReturningRows(result: ?std.json.Value) ?[]const std.json.Value {
    const value = result orelse return null;
    if (value != .object) return null;
    const rows = value.object.get("returning") orelse return null;
    if (rows != .array) return null;
    return rows.array.items;
}

fn responseColumnNamesAlloc(alloc: std.mem.Allocator, result: ?std.json.Value) !?[]const []const u8 {
    const value = result orelse return null;
    if (value != .object) return null;
    const result_schema = value.object.get("result_schema") orelse return null;
    if (result_schema != .array or result_schema.array.items.len == 0) return null;
    var names: std.ArrayListUnmanaged([]const u8) = .empty;
    errdefer names.deinit(alloc);
    for (result_schema.array.items) |item| {
        if (item != .object) continue;
        const name = jsonString(item.object.get("display_name")) orelse jsonString(item.object.get("name")) orelse continue;
        try names.append(alloc, name);
    }
    if (names.items.len == 0) return null;
    return try names.toOwnedSlice(alloc);
}

fn commandTagForDdlSql(alloc: std.mem.Allocator, sql: []const u8) ?[]const u8 {
    var parsed_sql = sql_adapter.ParsedSql.initAlloc(alloc, sql) catch return null;
    defer parsed_sql.deinit(alloc);
    return commandTagForParsedDdl(&parsed_sql);
}

fn commandTagForParsedDdl(parsed_sql: *const sql_adapter.ParsedSql) []const u8 {
    const tokens = parsed_sql.items();
    const raw = parsed_sql.raw_statement;
    if (raw.token_start >= raw.token_end or raw.token_end > tokens.len) return "DDL";
    const first = tokens[raw.token_start];
    if (first.matchesKeywordTag(.create)) return commandTagForCreateDdl(tokens[raw.token_start + 1 .. raw.token_end]);
    if (first.matchesKeywordTag(.alter)) return commandTagForDdlObject("ALTER", tokens[raw.token_start + 1 .. raw.token_end]);
    if (first.matchesKeywordTag(.drop)) return commandTagForDdlObject("DROP", tokens[raw.token_start + 1 .. raw.token_end]);
    if (first.matchesKeywordTag(.truncate)) return "TRUNCATE";
    if (first.matchesKeywordTag(.grant)) return "GRANT";
    if (first.matchesKeywordTag(.revoke)) return "REVOKE";
    if (first.matchesKeywordTag(.set)) return "SET";
    if (first.matchesKeywordTag(.show)) return "SHOW";
    if (first.matchesKeywordTag(.reset)) return "RESET";
    if (first.matchesKeywordTag(.discard)) return "DISCARD";
    if (first.matchesKeywordTag(.begin)) return "BEGIN";
    if (first.matchesKeywordTag(.commit)) return "COMMIT";
    if (first.matchesKeywordTag(.rollback)) return "ROLLBACK";
    if (first.matchesKeywordTag(.listen)) return "LISTEN";
    if (first.matchesKeywordTag(.notify)) return "NOTIFY";
    if (first.matchesKeywordTag(.unlisten)) return "UNLISTEN";
    if (first.matchesKeywordTag(.prepare)) return "PREPARE";
    if (first.matchesKeywordTag(.execute)) return "EXECUTE";
    if (first.matchesKeywordTag(.deallocate)) return "DEALLOCATE";
    if (first.matchesKeywordTag(.call)) return "CALL";
    return "DDL";
}

fn commandTagForCreateDdl(tokens: []const sql_adapter.Token) []const u8 {
    var index: usize = 0;
    while (index < tokens.len and createDdlModifier(tokens[index])) : (index += 1) {}
    if (index >= tokens.len) return "CREATE";
    if (tokens[index].matchesKeywordTag(.index)) return "CREATE INDEX";
    return commandTagForDdlObject("CREATE", tokens[index..]);
}

fn createDdlModifier(token: sql_adapter.Token) bool {
    return token.matchesKeywordTag(.unique) or
        token.matchesKeywordTag(.@"or") or
        token.matchesKeywordTag(.replace) or
        token.matchesKeyword("temporary") or
        token.matchesKeyword("temp") or
        token.matchesKeyword("unlogged") or
        token.matchesKeyword("concurrently");
}

fn commandTagForDdlObject(comptime verb: []const u8, tokens: []const sql_adapter.Token) []const u8 {
    for (tokens) |token| {
        if (ddlObjectModifier(token)) continue;
        if (token.matchesKeywordTag(.table)) return verb ++ " TABLE";
        if (token.matchesKeywordTag(.index)) return verb ++ " INDEX";
        if (token.matchesKeywordTag(.database)) return verb ++ " DATABASE";
        if (token.matchesKeywordTag(.schema)) return verb ++ " SCHEMA";
        if (token.matchesKeyword("tablespace")) return verb ++ " TABLESPACE";
        if (token.matchesKeywordTag(.extension)) return verb ++ " EXTENSION";
        if (token.matchesKeyword("role")) return verb ++ " ROLE";
        if (token.matchesKeywordTag(.function)) return verb ++ " FUNCTION";
        if (token.matchesKeywordTag(.procedure)) return verb ++ " PROCEDURE";
        if (token.matchesKeywordTag(.trigger)) return verb ++ " TRIGGER";
        if (token.matchesKeywordTag(.view)) return verb ++ " VIEW";
        if (token.matchesKeywordTag(.policy)) return verb ++ " POLICY";
        return verb;
    }
    return verb;
}

fn ddlObjectModifier(token: sql_adapter.Token) bool {
    return token.matchesKeywordTag(.@"if") or
        token.matchesKeywordTag(.not) or
        token.matchesKeywordTag(.exists) or
        token.matchesKeywordTag(.only) or
        token.matchesKeyword("concurrently") or
        token.matchesKeywordTag(.@"or") or
        token.matchesKeywordTag(.replace) or
        token.matchesKeyword("temporary") or
        token.matchesKeyword("temp") or
        token.matchesKeyword("unlogged");
}

fn jsonString(value: ?std.json.Value) ?[]const u8 {
    const raw = value orelse return null;
    if (raw != .string) return null;
    return raw.string;
}

fn jsonU64(value: ?std.json.Value) ?u64 {
    const raw = value orelse return null;
    return switch (raw) {
        .integer => |v| if (v >= 0) @intCast(v) else null,
        .float => |v| if (v >= 0 and @floor(v) == v) @intFromFloat(v) else null,
        else => null,
    };
}

fn jsonBool(value: ?std.json.Value) bool {
    const raw = value orelse return false;
    if (raw != .bool) return false;
    return raw.bool;
}

fn jsonCount(value: ?std.json.Value, field: []const u8) ?u64 {
    const raw = value orelse return null;
    if (raw != .object) return null;
    return jsonU64(raw.object.get(field));
}

fn commandTagForDdlResponse(response: std.json.Value) []const u8 {
    if (response != .object) return "DDL";
    const applied = response.object.get("applied") orelse return "DDL";
    if (applied != .object) return "DDL";
    const object = applied.object;
    if (jsonBool(object.get("created_table"))) return "CREATE TABLE";
    if (jsonBool(object.get("dropped_table"))) return "DROP TABLE";
    if (jsonBool(object.get("created_database"))) return "CREATE DATABASE";
    if (jsonBool(object.get("dropped_database"))) return "DROP DATABASE";
    if (jsonBool(object.get("created_namespace"))) return "CREATE SCHEMA";
    if (jsonBool(object.get("renamed_namespace"))) return "ALTER SCHEMA";
    if (jsonBool(object.get("dropped_namespace"))) return "DROP SCHEMA";
    if (jsonBool(object.get("created_tablespace"))) return "CREATE TABLESPACE";
    if (jsonBool(object.get("renamed_tablespace"))) return "ALTER TABLESPACE";
    if (jsonBool(object.get("dropped_tablespace"))) return "DROP TABLESPACE";
    if (jsonBool(response.object.get("noop"))) return "DDL";
    return "ALTER TABLE";
}

fn commandTagForRows(alloc: std.mem.Allocator, statement_kind: []const u8, row_count: usize) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s} {d}", .{ commandVerb(statement_kind), row_count });
}

fn commandTagForWrite(alloc: std.mem.Allocator, statement_kind: []const u8, result: ?std.json.Value) ![]u8 {
    const verb = commandVerb(statement_kind);
    const count = jsonCount(result, "inserted") orelse jsonCount(result, "deleted") orelse jsonCount(result, "transformed") orelse jsonCount(result, "matched") orelse jsonCount(result, "staged") orelse 0;
    return try std.fmt.allocPrint(alloc, "{s} {d}", .{ verb, count });
}

fn commandVerb(statement_kind: []const u8) []const u8 {
    if (std.ascii.eqlIgnoreCase(statement_kind, "insert") or std.ascii.eqlIgnoreCase(statement_kind, "insert_source")) return "INSERT 0";
    if (std.ascii.eqlIgnoreCase(statement_kind, "update") or std.mem.indexOf(u8, statement_kind, "update") != null) return "UPDATE";
    if (std.ascii.eqlIgnoreCase(statement_kind, "delete") or std.mem.indexOf(u8, statement_kind, "delete") != null) return "DELETE";
    if (std.ascii.eqlIgnoreCase(statement_kind, "truncate")) return "TRUNCATE";
    if (std.ascii.eqlIgnoreCase(statement_kind, "merge")) return "MERGE";
    if (std.ascii.eqlIgnoreCase(statement_kind, "select") or std.ascii.eqlIgnoreCase(statement_kind, "query")) return "SELECT";
    if (std.ascii.eqlIgnoreCase(statement_kind, "ddl")) return "OK";
    return statement_kind;
}

fn jsonValueTextAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]u8 {
    return switch (value) {
        .string => |text| try alloc.dupe(u8, text),
        .integer, .float, .bool, .number_string, .array, .object => try std.json.Stringify.valueAlloc(alloc, value, .{}),
        .null => try alloc.dupe(u8, ""),
    };
}

fn appendTextColumnDescription(writer: *std.Io.Writer, name: []const u8) !void {
    try writer.writeAll(name);
    try writer.writeByte(0);
    try writer.writeInt(i32, 0, .big);
    try writer.writeInt(i16, 0, .big);
    try writer.writeInt(i32, text_oid, .big);
    try writer.writeInt(i16, text_type_size, .big);
    try writer.writeInt(i32, -1, .big);
    try writer.writeInt(i16, 0, .big);
}

fn sqlstateForHttpStatus(status: u16) []const u8 {
    return switch (status) {
        400 => "42601",
        401, 403 => "28000",
        404 => "42P01",
        408 => "57014",
        501 => "0A000",
        else => "XX000",
    };
}

fn startupSearchPath(options: []const u8) ?[]const u8 {
    const needle = "search_path=";
    const option_start = std.mem.indexOf(u8, options, needle) orelse return null;
    var value = options[option_start + needle.len ..];
    if (std.mem.startsWith(u8, value, "'")) {
        value = value[1..];
        const end = std.mem.indexOfScalar(u8, value, '\'') orelse value.len;
        return value[0..end];
    }
    const end = std.mem.indexOfAny(u8, value, " \t\r\n") orelse value.len;
    return value[0..end];
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

test "pgwire ddl command tags preserve concrete statement shape" {
    const cases = [_]struct {
        sql: []const u8,
        tag: []const u8,
    }{
        .{ .sql = "CREATE TABLE events (id text PRIMARY KEY);", .tag = "CREATE TABLE" },
        .{ .sql = "CREATE INDEX events_status_idx ON events (status);", .tag = "CREATE INDEX" },
        .{ .sql = "CREATE UNIQUE INDEX events_id_idx ON events (id);", .tag = "CREATE INDEX" },
        .{ .sql = "DROP INDEX IF EXISTS events_status_idx;", .tag = "DROP INDEX" },
        .{ .sql = "ALTER TABLE events ADD COLUMN status text;", .tag = "ALTER TABLE" },
        .{ .sql = "CREATE SCHEMA analytics;", .tag = "CREATE SCHEMA" },
    };
    for (cases) |case| {
        try std.testing.expectEqualStrings(case.tag, commandTagForDdlSql(std.testing.allocator, case.sql).?);
    }
}
