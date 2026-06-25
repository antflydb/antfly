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
const http_server = @import("http_server.zig");
const platform_clock = @import("../platform/clock.zig");
const sql_adapter = @import("../sql/mod.zig");
const runtime_schema = @import("../storage/schema.zig");

const pgwire_module = @This();

const protocol_version_3: i32 = 196608;
const ssl_request_code: i32 = 80877103;
const cancel_request_code: i32 = 80877102;
const max_packet_len: i32 = 16 * 1024 * 1024;
const bool_oid: i32 = 16;
const bool_type_size: i16 = 1;
const text_oid: i32 = 25;
const text_type_size: i16 = -1;
const numeric_oid: i32 = 1700;
const jsonb_oid: i32 = 3802;
const timestamptz_oid: i32 = 1184;
const timestamptz_type_size: i16 = 8;
const text_format: i16 = 0;
const binary_format: i16 = 1;

const PgwireColumn = struct {
    name: []const u8,
    type_oid: i32 = text_oid,
    type_size: i16 = text_type_size,
    antfly_type: ?runtime_schema.AntflyType = null,
    array_item_type: ?runtime_schema.AntflyType = null,
};

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
    unnamed_statement: ?[]u8 = null,
    unnamed_portal: ?[]u8 = null,
    unnamed_portal_described: bool = false,
    prepared_statements: std.StringHashMapUnmanaged([]u8) = .empty,
    portals: std.StringHashMapUnmanaged([]u8) = .empty,
    described_portals: std.StringHashMapUnmanaged(void) = .empty,

    fn deinit(self: *Connection) void {
        if (self.database) |database| self.alloc.free(database);
        if (self.namespace) |namespace| self.alloc.free(namespace);
        if (self.unnamed_statement) |sql| self.alloc.free(sql);
        if (self.unnamed_portal) |sql| self.alloc.free(sql);
        pgwire_module.freeSqlMap(self.alloc, &self.prepared_statements);
        pgwire_module.freeSqlMap(self.alloc, &self.portals);
        pgwire_module.freeVoidMapKeys(self.alloc, &self.described_portals);
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
                'P' => try self.handleParse(payload),
                'B' => try self.handleBind(payload),
                'D' => try self.handleDescribe(payload),
                'E' => try self.handleExecute(payload),
                'C' => try self.handleClose(payload),
                'H' => try self.writer.flush(),
                'X' => return,
                'S' => try self.sendReadyForQuery(),
                else => {
                    try self.sendError("0A000", "unsupported pgwire message");
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
                    if (!try self.executeAndEncodeOne(statement, true, true)) return;
                }
                rest = rest[end + 1 ..];
                continue;
            }

            const trailing = std.mem.trim(u8, rest, " \t\r\n");
            if (trailing.len != 0) {
                executed = true;
                if (!try self.executeAndEncodeOne(trailing, true, true)) return;
            }
            break;
        }

        if (!executed) {
            try self.sendEmptyQueryResponse();
        }
        try self.sendReadyForQuery();
    }

    fn handleParse(self: *Connection, payload: []const u8) !void {
        var cursor = PayloadCursor.init(payload);
        const name = try cursor.takeCString();
        const sql = try cursor.takeCString();
        const parameter_count = try cursor.takeInt(i16);
        if (parameter_count < 0) return error.InvalidPgwireMessage;
        var i: usize = 0;
        while (i < @as(usize, @intCast(parameter_count))) : (i += 1) _ = try cursor.takeInt(i32);
        try cursor.expectEnd();
        try self.setPreparedStatement(name, sql);
        try self.sendParseComplete();
    }

    fn handleBind(self: *Connection, payload: []const u8) !void {
        var cursor = PayloadCursor.init(payload);
        const portal_name = try cursor.takeCString();
        const statement_name = try cursor.takeCString();
        const statement_sql = self.preparedStatementSql(statement_name) orelse {
            try self.sendError("26000", "prepared statement does not exist");
            return;
        };

        const parameter_format_count = try cursor.takeInt(i16);
        if (parameter_format_count < 0) return error.InvalidPgwireMessage;
        var parameter_formats: [16]i16 = undefined;
        const parameter_format_len: usize = @intCast(parameter_format_count);
        if (parameter_format_len > parameter_formats.len) return error.InvalidPgwireMessage;
        for (parameter_formats[0..parameter_format_len]) |*format| format.* = try cursor.takeInt(i16);

        const parameter_count = try cursor.takeInt(i16);
        if (parameter_count < 0) return error.InvalidPgwireMessage;
        const parameter_len: usize = @intCast(parameter_count);
        const parameters = try self.alloc.alloc(?[]const u8, parameter_len);
        defer self.alloc.free(parameters);
        for (parameters, 0..) |*parameter, index| {
            if (pgwire_module.parameterFormat(parameter_formats[0..parameter_format_len], index) == binary_format) {
                try self.sendError("0A000", "binary pgwire parameters are not implemented");
                return;
            }
            const value_len = try cursor.takeInt(i32);
            if (value_len < -1) return error.InvalidPgwireMessage;
            if (value_len == -1) {
                parameter.* = null;
            } else {
                parameter.* = try cursor.takeBytes(@intCast(value_len));
            }
        }

        const result_format_count = try cursor.takeInt(i16);
        if (result_format_count < 0) return error.InvalidPgwireMessage;
        var i: usize = 0;
        while (i < @as(usize, @intCast(result_format_count))) : (i += 1) {
            const format = try cursor.takeInt(i16);
            if (format == binary_format) {
                try self.sendError("0A000", "binary pgwire results are not implemented");
                return;
            }
        }
        try cursor.expectEnd();

        const bound_sql = try pgwire_module.bindSqlParametersAlloc(self.alloc, statement_sql, parameters);
        errdefer self.alloc.free(bound_sql);
        try self.setPortal(portal_name, bound_sql);
        try self.sendBindComplete();
    }

    fn handleDescribe(self: *Connection, payload: []const u8) !void {
        if (payload.len == 0) return error.InvalidPgwireMessage;
        const target = payload[0];
        var cursor = PayloadCursor.init(payload[1..]);
        const name = try cursor.takeCString();
        try cursor.expectEnd();
        switch (target) {
            'S' => if (self.preparedStatementSql(name) == null) {
                try self.sendError("26000", "prepared statement does not exist");
            } else {
                try self.sendNoData();
            },
            'P' => {
                const sql = self.portalSql(name) orelse {
                    try self.sendError("34000", "portal does not exist");
                    return;
                };
                if (try self.describeAndEncodeOne(sql)) try self.markPortalDescribed(name);
            },
            else => return error.InvalidPgwireMessage,
        }
    }

    fn handleExecute(self: *Connection, payload: []const u8) !void {
        var cursor = PayloadCursor.init(payload);
        const portal_name = try cursor.takeCString();
        _ = try cursor.takeInt(i32);
        try cursor.expectEnd();
        const sql = self.portalSql(portal_name) orelse {
            try self.sendError("34000", "portal does not exist");
            return;
        };
        _ = try self.executeAndEncodeOne(sql, false, !self.portalDescribed(portal_name));
    }

    fn handleClose(self: *Connection, payload: []const u8) !void {
        if (payload.len == 0) return error.InvalidPgwireMessage;
        const target = payload[0];
        var cursor = PayloadCursor.init(payload[1..]);
        const name = try cursor.takeCString();
        try cursor.expectEnd();
        switch (target) {
            'S' => try self.removePreparedStatement(name),
            'P' => try self.removePortal(name),
            else => return error.InvalidPgwireMessage,
        }
        try self.sendCloseComplete();
    }

    fn setPreparedStatement(self: *Connection, name: []const u8, sql: []const u8) !void {
        const owned_sql = try self.alloc.dupe(u8, sql);
        errdefer self.alloc.free(owned_sql);
        if (name.len == 0) {
            if (self.unnamed_statement) |old| self.alloc.free(old);
            self.unnamed_statement = owned_sql;
            return;
        }
        try pgwire_module.putOwnedSql(self.alloc, &self.prepared_statements, name, owned_sql);
    }

    fn preparedStatementSql(self: *Connection, name: []const u8) ?[]const u8 {
        if (name.len == 0) return self.unnamed_statement;
        return self.prepared_statements.get(name);
    }

    fn removePreparedStatement(self: *Connection, name: []const u8) !void {
        if (name.len == 0) {
            if (self.unnamed_statement) |old| self.alloc.free(old);
            self.unnamed_statement = null;
            return;
        }
        if (self.prepared_statements.fetchRemove(name)) |removed| {
            self.alloc.free(removed.key);
            self.alloc.free(removed.value);
        }
    }

    fn setPortal(self: *Connection, name: []const u8, owned_sql: []u8) !void {
        if (name.len == 0) {
            if (self.unnamed_portal) |old| self.alloc.free(old);
            self.unnamed_portal = owned_sql;
            self.unnamed_portal_described = false;
            return;
        }
        self.removeDescribedPortal(name);
        try pgwire_module.putOwnedSql(self.alloc, &self.portals, name, owned_sql);
    }

    fn portalSql(self: *Connection, name: []const u8) ?[]const u8 {
        if (name.len == 0) return self.unnamed_portal;
        return self.portals.get(name);
    }

    fn removePortal(self: *Connection, name: []const u8) !void {
        if (name.len == 0) {
            if (self.unnamed_portal) |old| self.alloc.free(old);
            self.unnamed_portal = null;
            self.unnamed_portal_described = false;
            return;
        }
        self.removeDescribedPortal(name);
        if (self.portals.fetchRemove(name)) |removed| {
            self.alloc.free(removed.key);
            self.alloc.free(removed.value);
        }
    }

    fn markPortalDescribed(self: *Connection, name: []const u8) !void {
        if (name.len == 0) {
            self.unnamed_portal_described = true;
            return;
        }
        const owned_name = try self.alloc.dupe(u8, name);
        errdefer self.alloc.free(owned_name);
        if (self.described_portals.fetchRemove(name)) |removed| self.alloc.free(removed.key);
        try self.described_portals.put(self.alloc, owned_name, {});
    }

    fn removeDescribedPortal(self: *Connection, name: []const u8) void {
        if (name.len == 0) {
            self.unnamed_portal_described = false;
            return;
        }
        if (self.described_portals.fetchRemove(name)) |removed| self.alloc.free(removed.key);
    }

    fn portalDescribed(self: *Connection, name: []const u8) bool {
        if (name.len == 0) return self.unnamed_portal_described;
        return self.described_portals.contains(name);
    }

    fn describeAndEncodeOne(self: *Connection, sql: []const u8) !bool {
        var outcome = self.describeSql(sql) catch |err| {
            std.log.warn("pgwire sql describe failed err={}", .{err});
            try self.sendError("XX000", "internal sql describe error");
            return false;
        };
        switch (outcome) {
            .response => |*response| {
                defer response.deinit(self.api_server.alloc);
                try self.sendError(pgwire_module.sqlstateForHttpStatus(response.status), response.body);
                return false;
            },
            .result => |*result| {
                defer result.deinit(self.api_server.alloc);
                self.session_id = result.session_id;
                if (!result.has_row_description) {
                    try self.sendNoData();
                    return false;
                }
                const columns = try pgwire_module.pgwireColumnsForRelationalColumnsAlloc(self.alloc, result.columns);
                defer self.alloc.free(columns);
                try self.sendRowDescription(columns);
                return true;
            },
        }
    }

    fn executeAndEncodeOne(self: *Connection, sql: []const u8, send_ready_on_error: bool, include_row_description: bool) !bool {
        var outcome = self.executeSql(sql) catch |err| {
            std.log.warn("pgwire sql execution failed err={}", .{err});
            try self.sendError("XX000", "internal sql execution error");
            if (send_ready_on_error) try self.sendReadyForQuery();
            return false;
        };
        switch (outcome) {
            .response => |*response| {
                defer response.deinit(self.api_server.alloc);
                try self.sendError(pgwire_module.sqlstateForHttpStatus(response.status), response.body);
                if (send_ready_on_error) try self.sendReadyForQuery();
                return false;
            },
            .result => |*result| {
                defer result.deinit(self.api_server.alloc);
                try self.encodeSqlResult(sql, result, include_row_description);
                return true;
            },
        }
    }

    fn describeSql(self: *Connection, sql: []const u8) !http_server.ApiHttpServer.PublicSqlDescribeResultOrResponse {
        return try self.api_server.handlePublicSqlDescribeRequestResult(.{
            .sql = sql,
            .session_id = self.session_id,
            .database = self.database,
            .namespace = self.namespace,
            .read_only = true,
        }, null);
    }

    fn executeSql(self: *Connection, sql: []const u8) !http_server.ApiHttpServer.PublicSqlResultOrResponse {
        return try self.api_server.handlePublicSqlRequestResult(.{
            .sql = sql,
            .session_id = self.session_id,
            .database = self.database,
            .namespace = self.namespace,
            .read_only = false,
        }, null);
    }

    fn encodeSqlResult(self: *Connection, sql: []const u8, result: *const http_server.ApiHttpServer.PublicSqlResult, include_row_description: bool) !void {
        self.session_id = result.session_id;
        switch (result.result) {
            .ddl => |applied| try self.sendCommandComplete(pgwire_module.commandTagForDdlSql(self.alloc, sql) orelse pgwire_module.commandTagForDdlApplied(applied)),
            .read => |read| {
                const rows = pgwire_module.readResultRows(read);
                const tag = try pgwire_module.commandTagForRows(self.alloc, "SELECT", rows.len);
                defer self.alloc.free(tag);
                const columns = try pgwire_module.readResultColumnsAlloc(self.alloc, read);
                defer if (columns) |typed_columns| self.alloc.free(typed_columns);
                try self.sendJsonRows(rows, columns, tag, include_row_description);
            },
            .rows_batch => |rows_batch| {
                if (rows_batch.returning_rows.len > 0) {
                    const tag = try pgwire_module.commandTagForRows(self.alloc, result.statement_kind, rows_batch.returning_rows.len);
                    defer self.alloc.free(tag);
                    try self.sendJsonRows(rows_batch.returning_rows, null, tag, include_row_description);
                } else {
                    const tag = try pgwire_module.commandTagForRowsBatch(self.alloc, result.statement_kind, rows_batch);
                    defer self.alloc.free(tag);
                    try self.sendCommandComplete(tag);
                }
            },
            .mutation_source => |mutation_source| {
                if (mutation_source.returning_rows.len > 0) {
                    const tag = try pgwire_module.commandTagForRows(self.alloc, result.statement_kind, mutation_source.returning_rows.len);
                    defer self.alloc.free(tag);
                    try self.sendJsonRows(mutation_source.returning_rows, null, tag, include_row_description);
                } else {
                    const tag = try pgwire_module.commandTagForMutationSource(self.alloc, result.statement_kind, mutation_source);
                    defer self.alloc.free(tag);
                    try self.sendCommandComplete(tag);
                }
            },
        }
    }

    fn sendRows(
        self: *Connection,
        rows: []const std.json.Value,
        schema_columns: ?[]const PgwireColumn,
        tag: []const u8,
    ) !void {
        var row_description: std.Io.Writer.Allocating = .init(self.alloc);
        defer row_description.deinit();
        if (schema_columns) |columns| {
            try row_description.writer.writeInt(i16, @intCast(columns.len), .big);
            for (columns) |column| try pgwire_module.appendColumnDescription(&row_description.writer, column);
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
        while (column_it.next()) |entry| try pgwire_module.appendColumnDescription(&row_description.writer, .{ .name = entry.key_ptr.* });
        try self.sendMessage('T', row_description.written());

        for (rows) |row| {
            var data: std.Io.Writer.Allocating = .init(self.alloc);
            defer data.deinit();
            try data.writer.writeInt(i16, @intCast(first_row_columns.count()), .big);
            var value_it = first_row_columns.iterator();
            while (value_it.next()) |entry| {
                try self.appendDataValue(&data.writer, row, .{ .name = entry.key_ptr.* });
            }
            try self.sendMessage('D', data.written());
        }

        try self.sendCommandComplete(tag);
    }

    fn sendJsonRows(
        self: *Connection,
        rows: []const []const u8,
        schema_columns: ?[]const PgwireColumn,
        tag: []const u8,
        include_row_description: bool,
    ) !void {
        if (schema_columns) |columns| {
            if (include_row_description) try self.sendRowDescription(columns);
            for (rows) |row_json| {
                var parsed = try std.json.parseFromSlice(std.json.Value, self.alloc, row_json, .{ .allocate = .alloc_always });
                defer parsed.deinit();
                try self.sendDataRowForColumnNames(parsed.value, columns);
            }
            try self.sendCommandComplete(tag);
            return;
        }

        var row_description: std.Io.Writer.Allocating = .init(self.alloc);
        defer row_description.deinit();
        if (rows.len == 0) {
            if (include_row_description) {
                try row_description.writer.writeInt(i16, 0, .big);
                try self.sendMessage('T', row_description.written());
            }
            try self.sendCommandComplete(tag);
            return;
        }

        var first_row = try std.json.parseFromSlice(std.json.Value, self.alloc, rows[0], .{ .allocate = .alloc_always });
        defer first_row.deinit();
        if (first_row.value != .object) {
            if (include_row_description) {
                try row_description.writer.writeInt(i16, 0, .big);
                try self.sendMessage('T', row_description.written());
            }
            try self.sendCommandComplete(tag);
            return;
        }

        const first_row_columns = first_row.value.object;
        const columns = try self.alloc.alloc(PgwireColumn, first_row_columns.count());
        defer self.alloc.free(columns);
        var column_it = first_row_columns.iterator();
        var column_index: usize = 0;
        while (column_it.next()) |entry| : (column_index += 1) columns[column_index] = .{ .name = entry.key_ptr.* };

        if (include_row_description) try self.sendRowDescription(columns);
        try self.sendDataRowForColumnNames(first_row.value, columns);
        for (rows[1..]) |row_json| {
            var parsed = try std.json.parseFromSlice(std.json.Value, self.alloc, row_json, .{ .allocate = .alloc_always });
            defer parsed.deinit();
            try self.sendDataRowForColumnNames(parsed.value, columns);
        }
        try self.sendCommandComplete(tag);
    }

    fn sendRowDescription(self: *Connection, columns: []const PgwireColumn) !void {
        var row_description: std.Io.Writer.Allocating = .init(self.alloc);
        defer row_description.deinit();
        try row_description.writer.writeInt(i16, @intCast(columns.len), .big);
        for (columns) |column| try pgwire_module.appendColumnDescription(&row_description.writer, column);
        try self.sendMessage('T', row_description.written());
    }

    fn sendDataRowsForColumnNames(self: *Connection, rows: []const std.json.Value, columns: []const PgwireColumn) !void {
        for (rows) |row| {
            try self.sendDataRowForColumnNames(row, columns);
        }
    }

    fn sendDataRowForColumnNames(self: *Connection, row: std.json.Value, columns: []const PgwireColumn) !void {
        var data: std.Io.Writer.Allocating = .init(self.alloc);
        defer data.deinit();
        try data.writer.writeInt(i16, @intCast(columns.len), .big);
        for (columns) |column| try self.appendDataValue(&data.writer, row, column);
        try self.sendMessage('D', data.written());
    }

    fn appendDataValue(self: *Connection, writer: *std.Io.Writer, row: std.json.Value, column: PgwireColumn) !void {
        if (row != .object) {
            try writer.writeInt(i32, -1, .big);
            return;
        }
        const value = row.object.get(column.name) orelse {
            try writer.writeInt(i32, -1, .big);
            return;
        };
        if (value == .null) {
            try writer.writeInt(i32, -1, .big);
            return;
        }
        const text = try pgwire_module.pgwireValueTextAlloc(self.alloc, value, column);
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

    fn sendParseComplete(self: *Connection) !void {
        try self.sendMessage('1', "");
    }

    fn sendBindComplete(self: *Connection) !void {
        try self.sendMessage('2', "");
    }

    fn sendCloseComplete(self: *Connection) !void {
        try self.sendMessage('3', "");
    }

    fn sendNoData(self: *Connection) !void {
        try self.sendMessage('n', "");
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

const PayloadCursor = struct {
    payload: []const u8,
    index: usize = 0,

    fn init(payload: []const u8) PayloadCursor {
        return .{ .payload = payload };
    }

    fn takeCString(self: *PayloadCursor) ![]const u8 {
        const start_index = self.index;
        while (self.index < self.payload.len and self.payload[self.index] != 0) : (self.index += 1) {}
        if (self.index >= self.payload.len) return error.InvalidPgwireMessage;
        const out = self.payload[start_index..self.index];
        self.index += 1;
        return out;
    }

    fn takeInt(self: *PayloadCursor, comptime T: type) !T {
        const size = @sizeOf(T);
        if (self.index + size > self.payload.len) return error.InvalidPgwireMessage;
        const out = std.mem.readInt(T, self.payload[self.index..][0..size], .big);
        self.index += size;
        return out;
    }

    fn takeBytes(self: *PayloadCursor, len: usize) ![]const u8 {
        if (self.index + len > self.payload.len) return error.InvalidPgwireMessage;
        const out = self.payload[self.index .. self.index + len];
        self.index += len;
        return out;
    }

    fn expectEnd(self: *const PayloadCursor) !void {
        if (self.index != self.payload.len) return error.InvalidPgwireMessage;
    }
};

fn putOwnedSql(
    alloc: std.mem.Allocator,
    map: *std.StringHashMapUnmanaged([]u8),
    name: []const u8,
    owned_sql: []u8,
) !void {
    const owned_name = try alloc.dupe(u8, name);
    errdefer alloc.free(owned_name);
    if (map.fetchRemove(name)) |removed| {
        alloc.free(removed.key);
        alloc.free(removed.value);
    }
    try map.put(alloc, owned_name, owned_sql);
}

fn freeSqlMap(alloc: std.mem.Allocator, map: *std.StringHashMapUnmanaged([]u8)) void {
    var it = map.iterator();
    while (it.next()) |entry| {
        alloc.free(entry.key_ptr.*);
        alloc.free(entry.value_ptr.*);
    }
    map.deinit(alloc);
}

fn freeVoidMapKeys(alloc: std.mem.Allocator, map: *std.StringHashMapUnmanaged(void)) void {
    var it = map.iterator();
    while (it.next()) |entry| alloc.free(entry.key_ptr.*);
    map.deinit(alloc);
}

fn parameterFormat(formats: []const i16, index: usize) i16 {
    if (formats.len == 0) return text_format;
    if (formats.len == 1) return formats[0];
    if (index < formats.len) return formats[index];
    return text_format;
}

fn bindSqlParametersAlloc(alloc: std.mem.Allocator, sql: []const u8, parameters: []const ?[]const u8) ![]u8 {
    if (parameters.len == 0) return try alloc.dupe(u8, sql);
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();

    var i: usize = 0;
    var state: enum { normal, single_quote, double_quote, line_comment, block_comment, dollar_quote } = .normal;
    var dollar_delim: []const u8 = "";
    while (i < sql.len) {
        switch (state) {
            .normal => {
                if (sql[i] == '$') {
                    if (dollarQuoteDelimiter(sql[i..])) |delim| {
                        dollar_delim = delim;
                        state = .dollar_quote;
                        try out.writer.writeAll(delim);
                        i += delim.len;
                        continue;
                    }
                    if (i + 1 < sql.len and std.ascii.isDigit(sql[i + 1])) {
                        var end = i + 1;
                        while (end < sql.len and std.ascii.isDigit(sql[end])) : (end += 1) {}
                        const ordinal = try std.fmt.parseInt(usize, sql[i + 1 .. end], 10);
                        if (ordinal == 0 or ordinal > parameters.len) return error.InvalidPgwireParameter;
                        try appendSqlLiteral(&out.writer, parameters[ordinal - 1]);
                        i = end;
                        continue;
                    }
                }
                if (sql[i] == '\'') state = .single_quote;
                if (sql[i] == '"') state = .double_quote;
                if (sql[i] == '-' and i + 1 < sql.len and sql[i + 1] == '-') {
                    state = .line_comment;
                    try out.writer.writeAll(sql[i .. i + 2]);
                    i += 2;
                    continue;
                }
                if (sql[i] == '/' and i + 1 < sql.len and sql[i + 1] == '*') {
                    state = .block_comment;
                    try out.writer.writeAll(sql[i .. i + 2]);
                    i += 2;
                    continue;
                }
                try out.writer.writeByte(sql[i]);
                i += 1;
            },
            .single_quote => {
                try out.writer.writeByte(sql[i]);
                if (sql[i] == '\'' and i + 1 < sql.len and sql[i + 1] == '\'') {
                    try out.writer.writeByte(sql[i + 1]);
                    i += 2;
                    continue;
                }
                if (sql[i] == '\'') state = .normal;
                i += 1;
            },
            .double_quote => {
                try out.writer.writeByte(sql[i]);
                if (sql[i] == '"' and i + 1 < sql.len and sql[i + 1] == '"') {
                    try out.writer.writeByte(sql[i + 1]);
                    i += 2;
                    continue;
                }
                if (sql[i] == '"') state = .normal;
                i += 1;
            },
            .line_comment => {
                try out.writer.writeByte(sql[i]);
                if (sql[i] == '\n') state = .normal;
                i += 1;
            },
            .block_comment => {
                try out.writer.writeByte(sql[i]);
                if (sql[i] == '*' and i + 1 < sql.len and sql[i + 1] == '/') {
                    try out.writer.writeByte(sql[i + 1]);
                    state = .normal;
                    i += 2;
                    continue;
                }
                i += 1;
            },
            .dollar_quote => {
                if (std.mem.startsWith(u8, sql[i..], dollar_delim)) {
                    try out.writer.writeAll(dollar_delim);
                    state = .normal;
                    i += dollar_delim.len;
                    continue;
                }
                try out.writer.writeByte(sql[i]);
                i += 1;
            },
        }
    }
    return try out.toOwnedSlice();
}

fn appendSqlLiteral(writer: *std.Io.Writer, value: ?[]const u8) !void {
    const text = value orelse {
        try writer.writeAll("NULL");
        return;
    };
    try writer.writeByte('\'');
    for (text) |byte| {
        if (byte == '\'') try writer.writeByte('\'');
        try writer.writeByte(byte);
    }
    try writer.writeByte('\'');
}

fn readResultRows(read: anytype) []const []const u8 {
    return switch (read.result) {
        .query => |query| query.rows,
        .document_query => |query| query.rows,
        .set_operation => |query| query.rows,
        .recursive_cte => |query| query.rows,
        .aggregate => |aggregate| aggregate.rows,
        .window => |window| window.rows,
        .join => |join| join.rows,
        .lateral => |lateral| lateral.rows,
    };
}

fn readResultColumnsAlloc(alloc: std.mem.Allocator, read: anytype) !?[]const PgwireColumn {
    if (read.columns.len == 0) return null;
    return try pgwireColumnsForRelationalColumnsAlloc(alloc, read.columns);
}

fn pgwireColumnsForRelationalColumnsAlloc(alloc: std.mem.Allocator, relational_columns: []const runtime_schema.RelationalColumn) ![]const PgwireColumn {
    const columns = try alloc.alloc(PgwireColumn, relational_columns.len);
    for (relational_columns, 0..) |column, i| columns[i] = pgwireColumnForRelationalColumn(column);
    return columns;
}

fn pgwireColumnForRelationalColumn(column: runtime_schema.RelationalColumn) PgwireColumn {
    const pg_type = pgwireTypeForAntflyType(column.field_type, column.array_item_type);
    return .{
        .name = column.name,
        .type_oid = pg_type.oid,
        .type_size = pg_type.size,
        .antfly_type = column.field_type,
        .array_item_type = column.array_item_type,
    };
}

fn pgwireTypeForAntflyType(
    field_type: runtime_schema.AntflyType,
    array_item_type: ?runtime_schema.AntflyType,
) struct { oid: i32, size: i16 } {
    _ = array_item_type;
    return switch (field_type) {
        .keyword, .text, .link, .html, .search_as_you_type => .{ .oid = text_oid, .size = text_type_size },
        .numeric => .{ .oid = numeric_oid, .size = text_type_size },
        .boolean => .{ .oid = bool_oid, .size = bool_type_size },
        .datetime => .{ .oid = timestamptz_oid, .size = timestamptz_type_size },
        .json, .array => .{ .oid = jsonb_oid, .size = text_type_size },
        .embedding, .geopoint, .geoshape, .blob => .{ .oid = text_oid, .size = text_type_size },
    };
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

fn commandTagForDdlApplied(applied: anytype) []const u8 {
    if (applied.created_table) return "CREATE TABLE";
    if (applied.dropped_table) return "DROP TABLE";
    if (applied.created_database) return "CREATE DATABASE";
    if (applied.dropped_database) return "DROP DATABASE";
    if (applied.created_namespace) return "CREATE SCHEMA";
    if (applied.renamed_namespace) return "ALTER SCHEMA";
    if (applied.dropped_namespace) return "DROP SCHEMA";
    if (applied.created_tablespace) return "CREATE TABLESPACE";
    if (applied.renamed_tablespace) return "ALTER TABLESPACE";
    if (applied.dropped_tablespace) return "DROP TABLESPACE";
    if (applied.noop) return "DDL";
    return "ALTER TABLE";
}

fn commandTagForRows(alloc: std.mem.Allocator, statement_kind: []const u8, row_count: usize) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s} {d}", .{ commandVerb(statement_kind), row_count });
}

fn commandTagForRowsBatch(alloc: std.mem.Allocator, statement_kind: []const u8, result: anytype) ![]u8 {
    const count = result.inserted + result.deleted + result.transformed;
    return try std.fmt.allocPrint(alloc, "{s} {d}", .{ commandVerb(statement_kind), count });
}

fn commandTagForMutationSource(alloc: std.mem.Allocator, statement_kind: []const u8, result: anytype) ![]u8 {
    const count = result.staged;
    return try std.fmt.allocPrint(alloc, "{s} {d}", .{ commandVerb(statement_kind), count });
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

fn pgwireValueTextAlloc(alloc: std.mem.Allocator, value: std.json.Value, column: PgwireColumn) ![]u8 {
    if (column.antfly_type == .datetime) {
        if (try jsonValueNanoseconds(value)) |ns| return try timestampNsTextAlloc(alloc, ns);
    }
    return try jsonValueTextAlloc(alloc, value);
}

fn jsonValueNanoseconds(value: std.json.Value) !?u64 {
    return switch (value) {
        .integer => |number| if (number >= 0) @intCast(number) else null,
        .float => |number| if (number >= 0) @intFromFloat(number) else null,
        .number_string => |text| try std.fmt.parseInt(u64, text, 10),
        else => null,
    };
}

fn timestampNsTextAlloc(alloc: std.mem.Allocator, ns: u64) ![]u8 {
    const seconds = @divFloor(ns, std.time.ns_per_s);
    const micros = @divFloor(ns % std.time.ns_per_s, std.time.ns_per_us);
    const epoch_seconds = std.time.epoch.EpochSeconds{ .secs = seconds };
    const year_day = epoch_seconds.getEpochDay().calculateYearDay();
    const month_day = year_day.calculateMonthDay();
    const day_seconds = epoch_seconds.getDaySeconds();
    return try std.fmt.allocPrint(alloc, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}.{d:0>6}Z", .{
        year_day.year,
        month_day.month.numeric(),
        month_day.day_index + 1,
        day_seconds.getHoursIntoDay(),
        day_seconds.getMinutesIntoHour(),
        day_seconds.getSecondsIntoMinute(),
        micros,
    });
}

fn appendColumnDescription(writer: *std.Io.Writer, column: PgwireColumn) !void {
    try writer.writeAll(column.name);
    try writer.writeByte(0);
    try writer.writeInt(i32, 0, .big);
    try writer.writeInt(i16, 0, .big);
    try writer.writeInt(i32, column.type_oid, .big);
    try writer.writeInt(i16, column.type_size, .big);
    try writer.writeInt(i32, -1, .big);
    try writer.writeInt(i16, text_format, .big);
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

test "pgwire extended bind substitutes text parameters outside literals and comments" {
    const params = [_]?[]const u8{ "row:1", "ready's" };
    const sql = try bindSqlParametersAlloc(
        std.testing.allocator,
        "SELECT '$1' AS literal, id FROM docs WHERE id = $1 AND status = $2 -- $1\n",
        &params,
    );
    defer std.testing.allocator.free(sql);
    try std.testing.expectEqualStrings(
        "SELECT '$1' AS literal, id FROM docs WHERE id = 'row:1' AND status = 'ready''s' -- $1\n",
        sql,
    );
}

test "pgwire extended bind renders null parameters" {
    const params = [_]?[]const u8{null};
    const sql = try bindSqlParametersAlloc(std.testing.allocator, "SELECT * FROM docs WHERE deleted_at IS $1", &params);
    defer std.testing.allocator.free(sql);
    try std.testing.expectEqualStrings("SELECT * FROM docs WHERE deleted_at IS NULL", sql);
}

test "pgwire relational column descriptions use postgres-compatible text types" {
    const typed_columns = [_]runtime_schema.RelationalColumn{
        .{ .name = "id", .path = "id", .field_type = .keyword },
        .{ .name = "amount", .path = "amount", .field_type = .numeric },
        .{ .name = "active", .path = "active", .field_type = .boolean },
        .{ .name = "created_at", .path = "created_at", .field_type = .datetime },
        .{ .name = "attrs", .path = "attrs", .field_type = .json },
        .{ .name = "tags", .path = "tags", .field_type = .array, .array_item_type = .keyword },
    };
    const expected_oids = [_]i32{
        text_oid,
        numeric_oid,
        bool_oid,
        timestamptz_oid,
        jsonb_oid,
        jsonb_oid,
    };
    const expected_sizes = [_]i16{
        text_type_size,
        text_type_size,
        bool_type_size,
        timestamptz_type_size,
        text_type_size,
        text_type_size,
    };

    for (typed_columns, expected_oids, expected_sizes) |column, expected_oid, expected_size| {
        const pg_column = pgwireColumnForRelationalColumn(column);
        try std.testing.expectEqualStrings(column.name, pg_column.name);
        try std.testing.expectEqual(expected_oid, pg_column.type_oid);
        try std.testing.expectEqual(expected_size, pg_column.type_size);
        try std.testing.expectEqual(column.field_type, pg_column.antfly_type.?);
    }

    const rendered = try timestampNsTextAlloc(std.testing.allocator, 123_456_789);
    defer std.testing.allocator.free(rendered);
    try std.testing.expectEqualStrings("1970-01-01T00:00:00.123456Z", rendered);
}
