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
const platform_sync = @import("antfly_platform").sync;
const sql_adapter = @import("../sql/mod.zig");
const runtime_schema = @import("../storage/schema.zig");

const pgwire_module = @This();

const protocol_version_3: i32 = 196608;
const ssl_request_code: i32 = 80877103;
const cancel_request_code: i32 = 80877102;
const max_packet_len: i32 = 16 * 1024 * 1024;
const bool_oid: i32 = 16;
const bool_type_size: i16 = 1;
const int8_oid: i32 = 20;
const int2_oid: i32 = 21;
const int4_oid: i32 = 23;
const text_oid: i32 = 25;
const text_type_size: i16 = -1;
const numeric_oid: i32 = 1700;
const jsonb_oid: i32 = 3802;
const timestamptz_oid: i32 = 1184;
const timestamptz_type_size: i16 = 8;
const postgres_epoch_unix_seconds: i64 = 946_684_800;
const text_format: i16 = 0;
const binary_format: i16 = 1;

const PgwireColumn = struct {
    name: []const u8,
    type_oid: i32 = text_oid,
    type_size: i16 = text_type_size,
    antfly_type: ?runtime_schema.AntflyType = null,
    array_item_type: ?runtime_schema.AntflyType = null,
};

const PreparedStatement = struct {
    parsed_sql: sql_adapter.ParsedSql,
    parameter_oids: []i32 = &.{},
};

const Portal = struct {
    parsed_sql: sql_adapter.ParsedSql,
    params: []sql_adapter.SqlValue = &.{},
    result_formats: []i16 = &.{},
};

const FrontendMessage = struct {
    tag: u8,
    payload: []u8,
};

const CancelEntry = struct {
    secret_key: i32,
    connection: *Connection,
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
    next_backend_pid: std.atomic.Value(u32) = .init(1),
    cancel_mutex: std.atomic.Mutex = .unlocked,
    cancel_connections: std.AutoHashMapUnmanaged(i32, CancelEntry) = .empty,

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

        self.cancel_connections.deinit(self.alloc);
        self.io_impl.deinit();
        self.alloc.free(self.owned_host);
        self.alloc.destroy(self);
    }

    fn boundAddress(self: *const State) ?std.Io.net.IpAddress {
        const listener = self.listener orelse return null;
        return listener.socket.address;
    }

    fn registerCancelHandle(self: *State, connection: *Connection) !void {
        if (connection.backend_pid != 0) return;
        platform_sync.lockYielding(&self.cancel_mutex);
        defer self.cancel_mutex.unlock();
        while (true) {
            const pid = self.allocateBackendPid();
            if (self.cancel_connections.contains(pid)) continue;
            const secret_key = try randomPositiveI32(self.alloc);
            try self.cancel_connections.put(self.alloc, pid, .{
                .secret_key = secret_key,
                .connection = connection,
            });
            connection.backend_pid = pid;
            connection.cancel_key = secret_key;
            return;
        }
    }

    fn unregisterCancelHandle(self: *State, connection: *Connection) void {
        if (connection.backend_pid == 0) return;
        platform_sync.lockYielding(&self.cancel_mutex);
        defer self.cancel_mutex.unlock();
        _ = self.cancel_connections.remove(connection.backend_pid);
        connection.backend_pid = 0;
        connection.cancel_key = 0;
    }

    fn cancelBackendRequest(self: *State, backend_pid: i32, secret_key: i32) bool {
        platform_sync.lockYielding(&self.cancel_mutex);
        defer self.cancel_mutex.unlock();
        const entry = self.cancel_connections.get(backend_pid) orelse return false;
        if (entry.secret_key != secret_key) return false;
        if (!entry.connection.active_execution.load(.acquire)) return true;
        entry.connection.cancel_requested.store(true, .release);
        return true;
    }

    fn allocateBackendPid(self: *State) i32 {
        const raw = self.next_backend_pid.fetchAdd(1, .acq_rel);
        const positive = (raw & 0x7fff_ffff) + 1;
        return @intCast(positive);
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
        .state = state,
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
    state: *State,
    api_server: *http_server.ApiHttpServer,
    reader: *std.Io.Reader,
    writer: *std.Io.Writer,
    session_id: ?u64 = null,
    startup_user: ?[]u8 = null,
    authenticated_identity: ?http_server.AuthenticatedIdentity = null,
    database: ?[]u8 = null,
    namespace: ?[]u8 = null,
    ready_for_query_status: u8 = 'I',
    unnamed_statement: ?PreparedStatement = null,
    unnamed_portal: ?Portal = null,
    unnamed_portal_described: bool = false,
    prepared_statements: std.StringHashMapUnmanaged(PreparedStatement) = .empty,
    portals: std.StringHashMapUnmanaged(Portal) = .empty,
    described_portals: std.StringHashMapUnmanaged(void) = .empty,
    backend_pid: i32 = 0,
    cancel_key: i32 = 0,
    active_execution: std.atomic.Value(bool) = .init(false),
    cancel_requested: std.atomic.Value(bool) = .init(false),

    fn deinit(self: *Connection) void {
        self.state.unregisterCancelHandle(self);
        if (self.authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (self.startup_user) |startup_user| self.alloc.free(startup_user);
        if (self.database) |database| self.alloc.free(database);
        if (self.namespace) |namespace| self.alloc.free(namespace);
        if (self.unnamed_statement) |statement| pgwire_module.freePreparedStatement(self.alloc, statement);
        if (self.unnamed_portal) |unnamed| pgwire_module.freePortal(self.alloc, unnamed);
        pgwire_module.freePreparedStatementMap(self.alloc, &self.prepared_statements);
        pgwire_module.freePortalMap(self.alloc, &self.portals);
        pgwire_module.freeVoidMapKeys(self.alloc, &self.described_portals);
        self.* = undefined;
    }

    fn run(self: *Connection) !void {
        if (!try self.startup()) return;
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

    fn startup(self: *Connection) !bool {
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
                cancel_request_code => {
                    try self.handleCancelRequest(payload);
                    return false;
                },
                protocol_version_3 => {
                    try self.applyStartupParams(payload[4..]);
                    try self.authenticateStartupIfRequired();
                    try self.state.registerCancelHandle(self);
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
                    return true;
                },
                else => {
                    try self.sendError("08P01", "unsupported postgres protocol version");
                    try self.writer.flush();
                    return error.UnsupportedPgwireStartup;
                },
            }
        }
    }

    fn authenticateStartupIfRequired(self: *Connection) !void {
        if (!self.pgwireAuthenticationRequired()) return;
        const username = self.startup_user orelse {
            try self.sendError("28000", "pgwire startup requires user");
            try self.writer.flush();
            return error.PgwireAuthenticationFailed;
        };
        if (username.len == 0) {
            try self.sendError("28000", "pgwire startup requires user");
            try self.writer.flush();
            return error.PgwireAuthenticationFailed;
        }
        if (self.api_server.cfg.user_manager == null) {
            try self.sendError("0A000", "pgwire password authentication requires user manager");
            try self.writer.flush();
            return error.PgwireAuthNotSupported;
        }

        try self.sendAuthenticationCleartextPassword();
        try self.writer.flush();
        const message = try self.readFrontendMessageAlloc();
        defer self.alloc.free(message.payload);
        if (message.tag != 'p') {
            try self.sendError("08P01", "expected pgwire password message");
            try self.writer.flush();
            return error.InvalidPgwireMessage;
        }
        const password_end = std.mem.indexOfScalar(u8, message.payload, 0) orelse {
            try self.sendError("08P01", "invalid pgwire password message");
            try self.writer.flush();
            return error.InvalidPgwireMessage;
        };
        var identity = self.api_server.authenticateUserPassword(username, message.payload[0..password_end]) catch |err| switch (err) {
            error.InvalidPassword, error.UserNotFound, error.Unauthorized => {
                try self.sendError("28P01", "password authentication failed");
                try self.writer.flush();
                return error.PgwireAuthenticationFailed;
            },
            else => return err,
        };
        errdefer identity.deinit(self.api_server.alloc);
        self.authenticated_identity = identity;
    }

    fn pgwireAuthenticationRequired(self: *Connection) bool {
        return self.api_server.cfg.auth_enabled or self.api_server.cfg.trusted_principal_secret != null;
    }

    fn handleCancelRequest(self: *Connection, payload: []const u8) !void {
        if (payload.len != 12) return error.InvalidPgwireStartup;
        const backend_pid = std.mem.readInt(i32, payload[4..8], .big);
        const secret_key = std.mem.readInt(i32, payload[8..12], .big);
        _ = self.state.cancelBackendRequest(backend_pid, secret_key);
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
            } else if (std.mem.eql(u8, key, "user")) {
                if (self.startup_user) |old| self.alloc.free(old);
                self.startup_user = try self.alloc.dupe(u8, value);
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
                    if (!try self.executeAndEncodeOne(statement, &.{}, &.{}, true, true)) return;
                }
                rest = rest[end + 1 ..];
                continue;
            }

            const trailing = std.mem.trim(u8, rest, " \t\r\n");
            if (trailing.len != 0) {
                executed = true;
                if (!try self.executeAndEncodeOne(trailing, &.{}, &.{}, true, true)) return;
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
        var parameter_oids = try self.alloc.alloc(i32, @intCast(parameter_count));
        errdefer if (parameter_oids.len > 0) self.alloc.free(parameter_oids);
        var i: usize = 0;
        while (i < @as(usize, @intCast(parameter_count))) : (i += 1) {
            const oid = try cursor.takeInt(i32);
            parameter_oids[i] = if (oid == 0) text_oid else oid;
        }
        try cursor.expectEnd();
        if (parameter_oids.len == 0) {
            parameter_oids = try pgwire_module.inferredTextParameterOidsAlloc(self.alloc, sql);
        }
        self.setPreparedStatement(name, sql, parameter_oids) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => {
                try self.sendError("42601", "invalid sql statement");
                return;
            },
        };
        try self.sendParseComplete();
    }

    fn handleBind(self: *Connection, payload: []const u8) !void {
        var cursor = PayloadCursor.init(payload);
        const portal_name = try cursor.takeCString();
        const statement_name = try cursor.takeCString();
        const statement = self.preparedStatement(statement_name) orelse {
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
        if (parameter_len != statement.parameter_oids.len) {
            try self.sendError("08P01", "bind parameter count does not match prepared statement");
            return;
        }

        const params = try self.alloc.alloc(sql_adapter.SqlValue, parameter_len);
        var initialized_params: usize = 0;
        var params_transferred = false;
        errdefer {
            if (!params_transferred) {
                for (params[0..initialized_params]) |value| http_server.ApiHttpServer.freePublicSqlParam(self.alloc, value);
                if (params.len > 0) self.alloc.free(params);
            }
        }
        for (params, 0..) |*parameter, index| {
            const format = pgwire_module.parameterFormat(parameter_formats[0..parameter_format_len], index);
            const value_len = try cursor.takeInt(i32);
            if (value_len < -1) return error.InvalidPgwireMessage;
            if (value_len == -1) {
                parameter.* = .null;
            } else {
                const encoded = try cursor.takeBytes(@intCast(value_len));
                parameter.* = pgwire_module.sqlValueFromPgwireParameterAlloc(self.alloc, statement.parameter_oids[index], format, encoded) catch |err| switch (err) {
                    error.InvalidPgwireParameter => {
                        try self.sendError("22023", "invalid pgwire parameter");
                        return;
                    },
                    error.UnsupportedPgwireParameterFormat => {
                        try self.sendError("0A000", "unsupported pgwire parameter format");
                        return;
                    },
                    else => return err,
                };
            }
            initialized_params += 1;
        }

        const result_format_count = try cursor.takeInt(i16);
        if (result_format_count < 0) return error.InvalidPgwireMessage;
        const result_format_len: usize = @intCast(result_format_count);
        const result_formats = try self.alloc.alloc(i16, result_format_len);
        var result_formats_transferred = false;
        errdefer if (!result_formats_transferred and result_formats.len > 0) self.alloc.free(result_formats);
        for (result_formats) |*format| {
            format.* = try cursor.takeInt(i16);
            if (format.* != text_format and format.* != binary_format) return error.InvalidPgwireMessage;
        }
        try cursor.expectEnd();

        const owned_portal_source = try self.alloc.dupe(u8, statement.parsed_sql.sql());
        var source_transferred = false;
        errdefer if (!source_transferred) self.alloc.free(owned_portal_source);
        var portal_parsed_sql = try sql_adapter.ParsedSql.initFromTokenSliceAlloc(self.alloc, owned_portal_source, statement.parsed_sql.items());
        var parsed_transferred = false;
        errdefer if (!parsed_transferred) portal_parsed_sql.deinit(self.alloc);
        try self.setPortal(portal_name, .{ .parsed_sql = portal_parsed_sql, .params = params, .result_formats = result_formats });
        source_transferred = true;
        parsed_transferred = true;
        params_transferred = true;
        result_formats_transferred = true;
        try self.sendBindComplete();
    }

    fn handleDescribe(self: *Connection, payload: []const u8) !void {
        if (payload.len == 0) return error.InvalidPgwireMessage;
        const target = payload[0];
        var cursor = PayloadCursor.init(payload[1..]);
        const name = try cursor.takeCString();
        try cursor.expectEnd();
        switch (target) {
            'S' => {
                const statement = self.preparedStatement(name) orelse {
                    try self.sendError("26000", "prepared statement does not exist");
                    return;
                };
                try self.sendParameterDescription(statement.parameter_oids);
                if (statement.parameter_oids.len != 0) {
                    try self.sendNoData();
                } else {
                    _ = try self.describeAndEncodeOne(statement.parsed_sql.sql(), &.{}, &.{});
                }
            },
            'P' => {
                const active_portal = self.portal(name) orelse {
                    try self.sendError("34000", "portal does not exist");
                    return;
                };
                if (try self.describeAndEncodeOne(active_portal.parsed_sql.sql(), active_portal.params, active_portal.result_formats)) try self.markPortalDescribed(name);
            },
            else => return error.InvalidPgwireMessage,
        }
    }

    fn handleExecute(self: *Connection, payload: []const u8) !void {
        var cursor = PayloadCursor.init(payload);
        const portal_name = try cursor.takeCString();
        _ = try cursor.takeInt(i32);
        try cursor.expectEnd();
        const active_portal = self.portal(portal_name) orelse {
            try self.sendError("34000", "portal does not exist");
            return;
        };
        _ = try self.executeAndEncodeOne(active_portal.parsed_sql.sql(), active_portal.params, active_portal.result_formats, false, !self.portalDescribed(portal_name));
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

    fn setPreparedStatement(self: *Connection, name: []const u8, sql: []const u8, owned_parameter_oids: []i32) !void {
        const owned_source = try self.alloc.dupe(u8, sql);
        var source_owned_by_statement = false;
        var oids_owned_by_statement = false;
        errdefer {
            if (!source_owned_by_statement) self.alloc.free(owned_source);
            if (!oids_owned_by_statement and owned_parameter_oids.len > 0) self.alloc.free(owned_parameter_oids);
        }
        var parsed_sql = try sql_adapter.ParsedSql.initAlloc(self.alloc, owned_source);
        var parsed_owned_by_statement = false;
        errdefer if (!parsed_owned_by_statement) parsed_sql.deinit(self.alloc);
        const owned_statement: PreparedStatement = .{
            .parsed_sql = parsed_sql,
            .parameter_oids = owned_parameter_oids,
        };
        source_owned_by_statement = true;
        oids_owned_by_statement = true;
        parsed_owned_by_statement = true;
        var statement_transferred = false;
        errdefer if (!statement_transferred) pgwire_module.freePreparedStatement(self.alloc, owned_statement);
        if (name.len == 0) {
            if (self.unnamed_statement) |old| pgwire_module.freePreparedStatement(self.alloc, old);
            self.unnamed_statement = owned_statement;
            statement_transferred = true;
            return;
        }
        try pgwire_module.putOwnedPreparedStatement(self.alloc, &self.prepared_statements, name, owned_statement);
        statement_transferred = true;
    }

    fn preparedStatement(self: *Connection, name: []const u8) ?*const PreparedStatement {
        if (name.len == 0) return if (self.unnamed_statement) |*statement| statement else null;
        return self.prepared_statements.getPtr(name);
    }

    fn removePreparedStatement(self: *Connection, name: []const u8) !void {
        if (name.len == 0) {
            if (self.unnamed_statement) |old| pgwire_module.freePreparedStatement(self.alloc, old);
            self.unnamed_statement = null;
            return;
        }
        if (self.prepared_statements.fetchRemove(name)) |removed| {
            self.alloc.free(removed.key);
            pgwire_module.freePreparedStatement(self.alloc, removed.value);
        }
    }

    fn setPortal(self: *Connection, name: []const u8, owned_portal: Portal) !void {
        if (name.len == 0) {
            if (self.unnamed_portal) |old| pgwire_module.freePortal(self.alloc, old);
            self.unnamed_portal = owned_portal;
            self.unnamed_portal_described = false;
            return;
        }
        self.removeDescribedPortal(name);
        try pgwire_module.putOwnedPortal(self.alloc, &self.portals, name, owned_portal);
    }

    fn portal(self: *Connection, name: []const u8) ?Portal {
        if (name.len == 0) return self.unnamed_portal;
        return self.portals.get(name);
    }

    fn removePortal(self: *Connection, name: []const u8) !void {
        if (name.len == 0) {
            if (self.unnamed_portal) |old| pgwire_module.freePortal(self.alloc, old);
            self.unnamed_portal = null;
            self.unnamed_portal_described = false;
            return;
        }
        self.removeDescribedPortal(name);
        if (self.portals.fetchRemove(name)) |removed| {
            self.alloc.free(removed.key);
            pgwire_module.freePortal(self.alloc, removed.value);
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

    fn describeAndEncodeOne(self: *Connection, sql: []const u8, params: []const sql_adapter.SqlValue, result_formats: []const i16) !bool {
        var outcome = self.describeSql(sql, params) catch |err| {
            std.log.warn("pgwire sql describe failed err={}", .{err});
            try self.sendError("XX000", "internal sql describe error");
            return false;
        };
        switch (outcome) {
            .response => |*response| {
                defer response.deinit(self.api_server.alloc);
                self.markTransactionError();
                try self.sendError(pgwire_module.sqlstateForHttpResponse(response.status, response.body), response.body);
                return false;
            },
            .result => |*result| {
                defer result.deinit(self.api_server.alloc);
                self.session_id = result.session_id;
                self.ready_for_query_status = pgwire_module.readyForQueryStatus(result.transaction_status);
                if (!result.has_row_description) {
                    try self.sendNoData();
                    return false;
                }
                const columns = try pgwire_module.pgwireColumnsForRelationalColumnsAlloc(self.alloc, result.columns);
                defer self.alloc.free(columns);
                try self.sendRowDescription(columns, result_formats);
                return true;
            },
        }
    }

    fn executeAndEncodeOne(
        self: *Connection,
        sql: []const u8,
        params: []const sql_adapter.SqlValue,
        result_formats: []const i16,
        send_ready_on_error: bool,
        include_row_description: bool,
    ) !bool {
        if (self.consumeCancelRequested()) {
            self.markTransactionError();
            try self.sendError("57014", "canceling statement due to user request");
            if (send_ready_on_error) try self.sendReadyForQuery();
            return false;
        }
        self.active_execution.store(true, .release);
        var outcome = self.executeSql(sql, params) catch |err| {
            self.active_execution.store(false, .release);
            std.log.warn("pgwire sql execution failed err={}", .{err});
            try self.sendError("XX000", "internal sql execution error");
            if (send_ready_on_error) try self.sendReadyForQuery();
            return false;
        };
        self.active_execution.store(false, .release);
        if (self.consumeCancelRequested()) {
            switch (outcome) {
                .response => |*response| response.deinit(self.api_server.alloc),
                .result => |*result| result.deinit(self.api_server.alloc),
            }
            self.markTransactionError();
            try self.sendError("57014", "canceling statement due to user request");
            if (send_ready_on_error) try self.sendReadyForQuery();
            return false;
        }
        switch (outcome) {
            .response => |*response| {
                defer response.deinit(self.api_server.alloc);
                self.markTransactionError();
                try self.sendError(pgwire_module.sqlstateForHttpResponse(response.status, response.body), response.body);
                if (send_ready_on_error) try self.sendReadyForQuery();
                return false;
            },
            .result => |*result| {
                defer result.deinit(self.api_server.alloc);
                self.ready_for_query_status = pgwire_module.readyForQueryStatus(result.transaction_status);
                try self.encodeSqlResult(result, result_formats, include_row_description);
                return true;
            },
        }
    }

    fn describeSql(self: *Connection, sql: []const u8, params: []const sql_adapter.SqlValue) !http_server.ApiHttpServer.PublicSqlDescribeResultOrResponse {
        return try self.api_server.handlePublicSqlDescribeRequestResult(.{
            .sql = sql,
            .session_id = self.session_id,
            .database = self.database,
            .namespace = self.namespace,
            .read_only = true,
            .params = params,
        }, self.authenticated_identity);
    }

    fn executeSql(self: *Connection, sql: []const u8, params: []const sql_adapter.SqlValue) !http_server.ApiHttpServer.PublicSqlResultOrResponse {
        return try self.api_server.executePublicSqlRequestResult(.{
            .sql = sql,
            .session_id = self.session_id,
            .database = self.database,
            .namespace = self.namespace,
            .read_only = false,
            .params = params,
        }, self.authenticated_identity);
    }

    fn encodeSqlResult(
        self: *Connection,
        result: *const http_server.ApiHttpServer.PublicSqlResult,
        result_formats: []const i16,
        include_row_description: bool,
    ) !void {
        self.session_id = result.session_id;
        switch (result.result) {
            .ddl => |ddl| try self.sendCommandComplete(if (ddl.command_tag.len != 0) ddl.command_tag else pgwire_module.commandTagForDdlApplied(ddl.applied)),
            .read => |read| {
                const rows = pgwire_module.readResultRows(read);
                const tag = try pgwire_module.commandTagForRows(self.alloc, "SELECT", rows.len);
                defer self.alloc.free(tag);
                const columns = try pgwire_module.readResultColumnsAlloc(self.alloc, read);
                defer if (columns) |typed_columns| self.alloc.free(typed_columns);
                try self.sendJsonRows(rows, columns, tag, result_formats, include_row_description);
            },
            .rows_batch => |rows_batch| {
                if (rows_batch.result.returning_rows.len > 0) {
                    const tag = try pgwire_module.commandTagForRows(self.alloc, result.statement_kind, rows_batch.result.returning_rows.len);
                    defer self.alloc.free(tag);
                    const columns = if (rows_batch.columns.len == 0) null else try pgwire_module.pgwireColumnsForRelationalColumnsAlloc(self.alloc, rows_batch.columns);
                    defer if (columns) |owned| self.alloc.free(owned);
                    try self.sendJsonRows(rows_batch.result.returning_rows, columns, tag, result_formats, include_row_description);
                } else {
                    const tag = try pgwire_module.commandTagForRowsBatch(self.alloc, result.statement_kind, rows_batch.result);
                    defer self.alloc.free(tag);
                    try self.sendCommandComplete(tag);
                }
            },
            .mutation_source => |mutation_source| {
                if (mutation_source.result.returning_rows.len > 0) {
                    const tag = try pgwire_module.commandTagForRows(self.alloc, result.statement_kind, mutation_source.result.returning_rows.len);
                    defer self.alloc.free(tag);
                    const columns = if (mutation_source.columns.len == 0) null else try pgwire_module.pgwireColumnsForRelationalColumnsAlloc(self.alloc, mutation_source.columns);
                    defer if (columns) |owned| self.alloc.free(owned);
                    try self.sendJsonRows(mutation_source.result.returning_rows, columns, tag, result_formats, include_row_description);
                } else {
                    const tag = try pgwire_module.commandTagForMutationSource(self.alloc, result.statement_kind, mutation_source.result);
                    defer self.alloc.free(tag);
                    try self.sendCommandComplete(tag);
                }
            },
            .bulk_io => |bulk_io| {
                const tag = try pgwire_module.commandTagForBulkIo(self.alloc, bulk_io);
                defer self.alloc.free(tag);
                try self.sendCommandComplete(tag);
            },
        }
    }

    fn sendRows(
        self: *Connection,
        rows: []const std.json.Value,
        schema_columns: ?[]const PgwireColumn,
        tag: []const u8,
        result_formats: []const i16,
    ) !void {
        var row_description: std.Io.Writer.Allocating = .init(self.alloc);
        defer row_description.deinit();
        if (schema_columns) |columns| {
            try row_description.writer.writeInt(i16, @intCast(columns.len), .big);
            for (columns, 0..) |column, index| try pgwire_module.appendColumnDescription(&row_description.writer, column, pgwire_module.resultFormat(result_formats, index));
            try self.sendMessage('T', row_description.written());
            try self.sendDataRowsForColumnNames(rows, columns, result_formats);
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
        var column_index: usize = 0;
        while (column_it.next()) |entry| : (column_index += 1) {
            const format = pgwire_module.resultFormat(result_formats, column_index);
            try pgwire_module.appendColumnDescription(&row_description.writer, .{ .name = entry.key_ptr.* }, format);
        }
        try self.sendMessage('T', row_description.written());

        for (rows) |row| {
            var data: std.Io.Writer.Allocating = .init(self.alloc);
            defer data.deinit();
            try data.writer.writeInt(i16, @intCast(first_row_columns.count()), .big);
            var value_it = first_row_columns.iterator();
            var value_index: usize = 0;
            while (value_it.next()) |entry| : (value_index += 1) {
                try self.appendDataValue(&data.writer, row, .{ .name = entry.key_ptr.* }, pgwire_module.resultFormat(result_formats, value_index));
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
        result_formats: []const i16,
        include_row_description: bool,
    ) !void {
        if (schema_columns) |columns| {
            if (include_row_description) try self.sendRowDescription(columns, result_formats);
            for (rows) |row_json| {
                var parsed = try std.json.parseFromSlice(std.json.Value, self.alloc, row_json, .{ .allocate = .alloc_always });
                defer parsed.deinit();
                try self.sendDataRowForColumnNames(parsed.value, columns, result_formats);
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

        if (include_row_description) try self.sendRowDescription(columns, result_formats);
        try self.sendDataRowForColumnNames(first_row.value, columns, result_formats);
        for (rows[1..]) |row_json| {
            var parsed = try std.json.parseFromSlice(std.json.Value, self.alloc, row_json, .{ .allocate = .alloc_always });
            defer parsed.deinit();
            try self.sendDataRowForColumnNames(parsed.value, columns, result_formats);
        }
        try self.sendCommandComplete(tag);
    }

    fn sendRowDescription(self: *Connection, columns: []const PgwireColumn, result_formats: []const i16) !void {
        var row_description: std.Io.Writer.Allocating = .init(self.alloc);
        defer row_description.deinit();
        try row_description.writer.writeInt(i16, @intCast(columns.len), .big);
        for (columns, 0..) |column, index| try pgwire_module.appendColumnDescription(&row_description.writer, column, pgwire_module.resultFormat(result_formats, index));
        try self.sendMessage('T', row_description.written());
    }

    fn sendDataRowsForColumnNames(self: *Connection, rows: []const std.json.Value, columns: []const PgwireColumn, result_formats: []const i16) !void {
        for (rows) |row| {
            try self.sendDataRowForColumnNames(row, columns, result_formats);
        }
    }

    fn sendDataRowForColumnNames(self: *Connection, row: std.json.Value, columns: []const PgwireColumn, result_formats: []const i16) !void {
        var data: std.Io.Writer.Allocating = .init(self.alloc);
        defer data.deinit();
        try data.writer.writeInt(i16, @intCast(columns.len), .big);
        for (columns, 0..) |column, index| try self.appendDataValue(&data.writer, row, column, pgwire_module.resultFormat(result_formats, index));
        try self.sendMessage('D', data.written());
    }

    fn appendDataValue(self: *Connection, writer: *std.Io.Writer, row: std.json.Value, column: PgwireColumn, format: i16) !void {
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
        if (format == binary_format) {
            var encoded = try pgwire_module.pgwireValueBinaryAlloc(self.alloc, value, column);
            defer encoded.deinit();
            try writer.writeInt(i32, @intCast(encoded.written().len), .big);
            try writer.writeAll(encoded.written());
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

    fn sendAuthenticationCleartextPassword(self: *Connection) !void {
        var payload: [4]u8 = undefined;
        std.mem.writeInt(i32, &payload, 3, .big);
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
        std.mem.writeInt(i32, payload[0..4], self.backend_pid, .big);
        std.mem.writeInt(i32, payload[4..8], self.cancel_key, .big);
        try self.sendMessage('K', &payload);
    }

    fn sendReadyForQuery(self: *Connection) !void {
        const payload = [_]u8{self.ready_for_query_status};
        try self.sendMessage('Z', &payload);
        try self.writer.flush();
    }

    fn markTransactionError(self: *Connection) void {
        if (self.ready_for_query_status == 'T') self.ready_for_query_status = 'E';
    }

    fn consumeCancelRequested(self: *Connection) bool {
        if (!self.cancel_requested.load(.acquire)) return false;
        self.cancel_requested.store(false, .release);
        return true;
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

    fn sendParameterDescription(self: *Connection, parameter_oids: []const i32) !void {
        var payload: std.Io.Writer.Allocating = .init(self.alloc);
        defer payload.deinit();
        try payload.writer.writeInt(i16, @intCast(parameter_oids.len), .big);
        for (parameter_oids) |oid| try payload.writer.writeInt(i32, oid, .big);
        try self.sendMessage('t', payload.written());
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

    fn readFrontendMessageAlloc(self: *Connection) !FrontendMessage {
        const tag = try self.reader.takeByte();
        const len = try self.reader.takeInt(i32, .big);
        if (len < 4 or len > max_packet_len) return error.InvalidPgwireMessage;
        return .{
            .tag = tag,
            .payload = try self.reader.readAlloc(self.alloc, @intCast(len - 4)),
        };
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

fn putOwnedPortal(
    alloc: std.mem.Allocator,
    map: *std.StringHashMapUnmanaged(Portal),
    name: []const u8,
    owned_portal: Portal,
) !void {
    const owned_name = try alloc.dupe(u8, name);
    errdefer alloc.free(owned_name);
    if (map.fetchRemove(name)) |removed| {
        alloc.free(removed.key);
        freePortal(alloc, removed.value);
    }
    try map.put(alloc, owned_name, owned_portal);
}

fn putOwnedPreparedStatement(
    alloc: std.mem.Allocator,
    map: *std.StringHashMapUnmanaged(PreparedStatement),
    name: []const u8,
    owned_statement: PreparedStatement,
) !void {
    const owned_name = try alloc.dupe(u8, name);
    errdefer alloc.free(owned_name);
    if (map.fetchRemove(name)) |removed| {
        alloc.free(removed.key);
        freePreparedStatement(alloc, removed.value);
    }
    try map.put(alloc, owned_name, owned_statement);
}

fn freePreparedStatement(alloc: std.mem.Allocator, statement: PreparedStatement) void {
    var owned_statement = statement;
    const source = owned_statement.parsed_sql.sql();
    owned_statement.parsed_sql.deinit(alloc);
    alloc.free(@constCast(source));
    if (statement.parameter_oids.len > 0) alloc.free(statement.parameter_oids);
}

fn freePreparedStatementMap(alloc: std.mem.Allocator, map: *std.StringHashMapUnmanaged(PreparedStatement)) void {
    var it = map.iterator();
    while (it.next()) |entry| {
        alloc.free(entry.key_ptr.*);
        freePreparedStatement(alloc, entry.value_ptr.*);
    }
    map.deinit(alloc);
}

fn freePortal(alloc: std.mem.Allocator, portal: Portal) void {
    var owned_portal = portal;
    const source = owned_portal.parsed_sql.sql();
    owned_portal.parsed_sql.deinit(alloc);
    alloc.free(@constCast(source));
    for (portal.params) |value| http_server.ApiHttpServer.freePublicSqlParam(alloc, value);
    if (portal.params.len > 0) alloc.free(portal.params);
    if (portal.result_formats.len > 0) alloc.free(portal.result_formats);
}

fn freePortalMap(alloc: std.mem.Allocator, map: *std.StringHashMapUnmanaged(Portal)) void {
    var it = map.iterator();
    while (it.next()) |entry| {
        alloc.free(entry.key_ptr.*);
        freePortal(alloc, entry.value_ptr.*);
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

fn resultFormat(formats: []const i16, index: usize) i16 {
    return parameterFormat(formats, index);
}

fn sqlValueFromPgwireParameterAlloc(alloc: std.mem.Allocator, oid: i32, format: i16, encoded: []const u8) !sql_adapter.SqlValue {
    return switch (format) {
        text_format => try sqlValueFromTextParameterAlloc(alloc, oid, encoded),
        binary_format => try sqlValueFromBinaryParameterAlloc(alloc, oid, encoded),
        else => error.UnsupportedPgwireParameterFormat,
    };
}

fn sqlValueFromTextParameterAlloc(alloc: std.mem.Allocator, oid: i32, text: []const u8) !sql_adapter.SqlValue {
    return switch (oid) {
        bool_oid => .{ .bool = try boolSqlValueFromText(text) },
        int2_oid, int4_oid, int8_oid => .{ .integer = try std.fmt.parseInt(i64, text, 10) },
        numeric_oid => numericSqlValueFromText(text) catch .{ .string = try alloc.dupe(u8, text) },
        jsonb_oid => blk: {
            var parsed = std.json.parseFromSlice(std.json.Value, alloc, text, .{}) catch return error.InvalidPgwireParameter;
            defer parsed.deinit();
            break :blk .{ .json = try alloc.dupe(u8, text) };
        },
        else => .{ .string = try alloc.dupe(u8, text) },
    };
}

fn sqlValueFromBinaryParameterAlloc(alloc: std.mem.Allocator, oid: i32, encoded: []const u8) !sql_adapter.SqlValue {
    return switch (oid) {
        bool_oid => blk: {
            if (encoded.len != 1) return error.InvalidPgwireParameter;
            break :blk .{ .bool = encoded[0] != 0 };
        },
        int2_oid => blk: {
            if (encoded.len != 2) return error.InvalidPgwireParameter;
            break :blk .{ .integer = std.mem.readInt(i16, encoded[0..2], .big) };
        },
        int4_oid => blk: {
            if (encoded.len != 4) return error.InvalidPgwireParameter;
            break :blk .{ .integer = std.mem.readInt(i32, encoded[0..4], .big) };
        },
        int8_oid => blk: {
            if (encoded.len != 8) return error.InvalidPgwireParameter;
            break :blk .{ .integer = std.mem.readInt(i64, encoded[0..8], .big) };
        },
        numeric_oid => blk: {
            const text = try numericTextFromPgBinaryAlloc(alloc, encoded);
            defer alloc.free(text);
            break :blk numericSqlValueFromText(text) catch .{ .string = try alloc.dupe(u8, text) };
        },
        timestamptz_oid => blk: {
            if (encoded.len != 8) return error.InvalidPgwireParameter;
            const pg_micros = std.mem.readInt(i64, encoded[0..8], .big);
            const unix_micros = pg_micros + postgres_epoch_unix_seconds * std.time.us_per_s;
            break :blk .{ .integer = unix_micros * std.time.ns_per_us };
        },
        jsonb_oid => blk: {
            if (encoded.len == 0 or encoded[0] != 1) return error.InvalidPgwireParameter;
            const json = encoded[1..];
            var parsed = std.json.parseFromSlice(std.json.Value, alloc, json, .{}) catch return error.InvalidPgwireParameter;
            defer parsed.deinit();
            break :blk .{ .json = try alloc.dupe(u8, json) };
        },
        text_oid => .{ .string = try alloc.dupe(u8, encoded) },
        else => .{ .string = try alloc.dupe(u8, encoded) },
    };
}

fn boolSqlValueFromText(text: []const u8) !bool {
    if (std.ascii.eqlIgnoreCase(text, "t") or
        std.ascii.eqlIgnoreCase(text, "true") or
        std.mem.eql(u8, text, "1"))
    {
        return true;
    }
    if (std.ascii.eqlIgnoreCase(text, "f") or
        std.ascii.eqlIgnoreCase(text, "false") or
        std.mem.eql(u8, text, "0"))
    {
        return false;
    }
    return error.InvalidPgwireParameter;
}

fn numericSqlValueFromText(text: []const u8) !sql_adapter.SqlValue {
    if (std.fmt.parseInt(i64, text, 10)) |integer| {
        return .{ .integer = integer };
    } else |_| {}
    return .{ .float = try std.fmt.parseFloat(f64, text) };
}

fn numericTextFromPgBinaryAlloc(alloc: std.mem.Allocator, encoded: []const u8) ![]u8 {
    if (encoded.len < 8 or encoded.len % 2 != 0) return error.InvalidPgwireParameter;
    const ndigits = std.mem.readInt(i16, encoded[0..2], .big);
    const weight = std.mem.readInt(i16, encoded[2..4], .big);
    const sign = std.mem.readInt(i16, encoded[4..6], .big);
    const dscale = std.mem.readInt(i16, encoded[6..8], .big);
    if (ndigits < 0 or dscale < 0) return error.InvalidPgwireParameter;
    const digit_count: usize = @intCast(ndigits);
    if (encoded.len != 8 + digit_count * 2) return error.InvalidPgwireParameter;
    if (sign == @as(i16, @bitCast(@as(u16, 0xC000)))) return try alloc.dupe(u8, "NaN");
    if (sign != 0 and sign != @as(i16, @bitCast(@as(u16, 0x4000)))) return error.InvalidPgwireParameter;
    if (digit_count == 0) return try alloc.dupe(u8, "0");

    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    if (sign != 0) try out.writer.writeByte('-');

    const integer_groups: isize = @as(isize, weight) + 1;
    if (integer_groups <= 0) {
        try out.writer.writeByte('0');
    } else {
        var group_index: isize = 0;
        while (group_index < integer_groups) : (group_index += 1) {
            const digit = if (group_index < digit_count) pgNumericDigit(encoded, @intCast(group_index)) else 0;
            if (group_index == 0) {
                try out.writer.print("{d}", .{digit});
            } else {
                try out.writer.print("{d:0>4}", .{digit});
            }
        }
    }

    if (dscale > 0) {
        try out.writer.writeByte('.');
        var remaining: usize = @intCast(dscale);
        var group_index: isize = @max(integer_groups, 0);
        while (remaining > 0) : (group_index += 1) {
            const digit = if (group_index >= 0 and group_index < digit_count) pgNumericDigit(encoded, @intCast(group_index)) else 0;
            var group_buf: [4]u8 = undefined;
            const group_text = try std.fmt.bufPrint(&group_buf, "{d:0>4}", .{digit});
            const n = @min(remaining, group_text.len);
            try out.writer.writeAll(group_text[0..n]);
            remaining -= n;
        }
    }

    return try out.toOwnedSlice();
}

fn pgNumericDigit(encoded: []const u8, index: usize) u16 {
    return std.mem.readInt(u16, encoded[8 + index * 2 ..][0..2], .big);
}

fn inferredTextParameterOidsAlloc(alloc: std.mem.Allocator, sql: []const u8) ![]i32 {
    const max_ordinal = try maxSqlParameterOrdinal(sql);
    const oids = try alloc.alloc(i32, max_ordinal);
    @memset(oids, text_oid);
    return oids;
}

fn maxSqlParameterOrdinal(sql: []const u8) !usize {
    var max_ordinal: usize = 0;
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
                        i += delim.len;
                        continue;
                    }
                    if (i + 1 < sql.len and std.ascii.isDigit(sql[i + 1])) {
                        var end = i + 1;
                        while (end < sql.len and std.ascii.isDigit(sql[end])) : (end += 1) {}
                        const ordinal = try std.fmt.parseInt(usize, sql[i + 1 .. end], 10);
                        if (ordinal == 0 or ordinal > std.math.maxInt(i16)) return error.InvalidPgwireParameter;
                        max_ordinal = @max(max_ordinal, ordinal);
                        i = end;
                        continue;
                    }
                }
                if (sql[i] == '\'') state = .single_quote;
                if (sql[i] == '"') state = .double_quote;
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
    return max_ordinal;
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

fn readyForQueryStatus(status: http_server.ApiHttpServer.PublicSqlTransactionStatus) u8 {
    return switch (status) {
        .idle => 'I',
        .in_transaction => 'T',
        .failed_transaction => 'E',
    };
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

fn commandTagForBulkIo(alloc: std.mem.Allocator, bulk_io: http_server.ApiHttpServer.PublicSqlResult.BulkIo) ![]u8 {
    const verb = switch (bulk_io.operation) {
        .import_rows, .export_rows => "COPY",
    };
    if (bulk_io.row_count) |row_count| return try std.fmt.allocPrint(alloc, "{s} {d}", .{ verb, row_count });
    return try alloc.dupe(u8, verb);
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

fn pgwireValueBinaryAlloc(alloc: std.mem.Allocator, value: std.json.Value, column: PgwireColumn) !std.Io.Writer.Allocating {
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    switch (column.type_oid) {
        bool_oid => try out.writer.writeByte(if (jsonValueBool(value) orelse return error.InvalidPgwireValue) 1 else 0),
        int2_oid => try out.writer.writeInt(i16, @intCast(try jsonValueI64(value)), .big),
        int4_oid => try out.writer.writeInt(i32, @intCast(try jsonValueI64(value)), .big),
        int8_oid => try out.writer.writeInt(i64, try jsonValueI64(value), .big),
        numeric_oid => {
            const text = try jsonValueTextAlloc(alloc, value);
            defer alloc.free(text);
            try appendPgBinaryNumeric(alloc, &out.writer, text);
        },
        timestamptz_oid => {
            const ns = (try jsonValueNanoseconds(value)) orelse return error.InvalidPgwireValue;
            const unix_micros: i64 = @intCast(@divFloor(ns, std.time.ns_per_us));
            const pg_micros = unix_micros - postgres_epoch_unix_seconds * std.time.us_per_s;
            try out.writer.writeInt(i64, pg_micros, .big);
        },
        jsonb_oid => {
            const text = try jsonValueTextAlloc(alloc, value);
            defer alloc.free(text);
            try out.writer.writeByte(1);
            try out.writer.writeAll(text);
        },
        else => {
            const text = try pgwireValueTextAlloc(alloc, value, column);
            defer alloc.free(text);
            try out.writer.writeAll(text);
        },
    }
    return out;
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

fn jsonValueBool(value: std.json.Value) ?bool {
    return switch (value) {
        .bool => |boolean| boolean,
        .string => |text| boolSqlValueFromText(text) catch null,
        else => null,
    };
}

fn jsonValueI64(value: std.json.Value) !i64 {
    return switch (value) {
        .integer => |number| number,
        .float => |number| @intFromFloat(number),
        .number_string, .string => |text| std.fmt.parseInt(i64, text, 10) catch return error.InvalidPgwireValue,
        else => error.InvalidPgwireValue,
    };
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

fn appendPgBinaryNumeric(alloc: std.mem.Allocator, writer: *std.Io.Writer, text: []const u8) !void {
    if (std.mem.indexOfAny(u8, text, "eE") != null) return error.InvalidPgwireValue;
    var body = text;
    var sign: i16 = 0;
    if (std.mem.startsWith(u8, body, "-")) {
        sign = @bitCast(@as(u16, 0x4000));
        body = body[1..];
    } else if (std.mem.startsWith(u8, body, "+")) {
        body = body[1..];
    }

    const decimal = std.mem.indexOfScalar(u8, body, '.');
    const integer_raw = if (decimal) |pos| body[0..pos] else body;
    const fractional_raw = if (decimal) |pos| body[pos + 1 ..] else "";
    var integer_start: usize = 0;
    while (integer_start < integer_raw.len and integer_raw[integer_start] == '0') : (integer_start += 1) {}
    const integer_digits = integer_raw[integer_start..];
    const dscale: i16 = @intCast(fractional_raw.len);

    var groups: std.ArrayListUnmanaged(u16) = .empty;
    defer groups.deinit(alloc);

    const integer_group_count: usize = if (integer_digits.len == 0) 0 else (integer_digits.len + 3) / 4;
    if (integer_group_count != 0) {
        const first_group_len = integer_digits.len - (integer_group_count - 1) * 4;
        var group_start: usize = 0;
        var group_len = first_group_len;
        while (group_start < integer_digits.len) {
            const group = try std.fmt.parseInt(u16, integer_digits[group_start .. group_start + group_len], 10);
            try groups.append(alloc, group);
            group_start += group_len;
            group_len = 4;
        }
    }

    var frac_index: usize = 0;
    while (frac_index < fractional_raw.len) : (frac_index += 4) {
        var group_buf = [_]u8{'0'} ** 4;
        const len = @min(@as(usize, 4), fractional_raw.len - frac_index);
        @memcpy(group_buf[0..len], fractional_raw[frac_index .. frac_index + len]);
        const group = try std.fmt.parseInt(u16, &group_buf, 10);
        try groups.append(alloc, group);
    }

    while (groups.items.len > 0 and groups.items[groups.items.len - 1] == 0) _ = groups.pop();

    const weight: i16 = if (integer_group_count == 0) -1 else @intCast(integer_group_count - 1);
    try writer.writeInt(i16, @intCast(groups.items.len), .big);
    try writer.writeInt(i16, weight, .big);
    try writer.writeInt(i16, sign, .big);
    try writer.writeInt(i16, dscale, .big);
    for (groups.items) |group| try writer.writeInt(u16, group, .big);
}

fn appendColumnDescription(writer: *std.Io.Writer, column: PgwireColumn, format: i16) !void {
    try writer.writeAll(column.name);
    try writer.writeByte(0);
    try writer.writeInt(i32, 0, .big);
    try writer.writeInt(i16, 0, .big);
    try writer.writeInt(i32, column.type_oid, .big);
    try writer.writeInt(i16, column.type_size, .big);
    try writer.writeInt(i32, -1, .big);
    try writer.writeInt(i16, format, .big);
}

fn sqlstateForHttpResponse(status: u16, body: []const u8) []const u8 {
    const message = std.mem.trim(u8, body, " \t\r\n");
    if (std.mem.eql(u8, message, "current transaction is aborted")) return "25P02";
    if (std.mem.indexOf(u8, message, "read-only transaction") != null) return "25006";
    if (std.mem.eql(u8, message, "unsupported sql statement")) return "0A000";
    if (std.mem.startsWith(u8, message, "document_sql_") and std.mem.indexOf(u8, message, "unsupported") != null) return "0A000";
    if (std.mem.eql(u8, message, "not found")) return "42P01";
    if (std.mem.eql(u8, message, "invalid sql request")) return "42601";
    if (std.mem.eql(u8, message, "invalid sql write")) return "22023";
    if (std.mem.eql(u8, message, "unsupported rows selector")) return "0A000";
    if (std.mem.eql(u8, message, "sql statement timeout")) return "57014";
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

fn randomPositiveI32(alloc: std.mem.Allocator) !i32 {
    var bytes: [4]u8 = undefined;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    try io_impl.io().randomSecure(&bytes);
    const raw = std.mem.readInt(u32, &bytes, .big) & 0x7fff_ffff;
    return @intCast(if (raw == 0) 1 else raw);
}

test "pgwire cancel registry matches backend key data" {
    const alloc = std.testing.allocator;
    var state = State{
        .alloc = alloc,
        .owned_host = try alloc.dupe(u8, "127.0.0.1"),
        .bind_port = 0,
        .api_server = undefined,
        .io_impl = std.Io.Threaded.init(alloc, .{}),
    };
    defer {
        state.cancel_connections.deinit(alloc);
        state.io_impl.deinit();
        alloc.free(state.owned_host);
    }

    var conn = Connection{
        .alloc = alloc,
        .state = &state,
        .api_server = undefined,
        .reader = undefined,
        .writer = undefined,
    };
    defer conn.deinit();

    try state.registerCancelHandle(&conn);
    try std.testing.expect(conn.backend_pid != 0);
    try std.testing.expect(conn.cancel_key != 0);
    try std.testing.expect(!conn.cancel_requested.load(.acquire));

    try std.testing.expect(!state.cancelBackendRequest(conn.backend_pid, conn.cancel_key +% 1));
    try std.testing.expect(!conn.cancel_requested.load(.acquire));

    try std.testing.expect(state.cancelBackendRequest(conn.backend_pid, conn.cancel_key));
    try std.testing.expect(!conn.consumeCancelRequested());

    conn.active_execution.store(true, .release);
    try std.testing.expect(state.cancelBackendRequest(conn.backend_pid, conn.cancel_key));
    conn.active_execution.store(false, .release);
    try std.testing.expect(conn.consumeCancelRequested());
    try std.testing.expect(!conn.consumeCancelRequested());
}

test "pgwire parse infers text parameter oids outside literals and comments" {
    const sql =
        \\SELECT '$1' AS literal, id
        \\FROM docs
        \\WHERE id = $2 AND status = $1 -- $3
        \\AND note = $$body $4 body$$
    ;
    const oids = try inferredTextParameterOidsAlloc(std.testing.allocator, sql);
    defer if (oids.len > 0) std.testing.allocator.free(oids);
    try std.testing.expectEqual(@as(usize, 2), oids.len);
    try std.testing.expectEqual(@as(i32, text_oid), oids[0]);
    try std.testing.expectEqual(@as(i32, text_oid), oids[1]);
}

test "pgwire text parameters decode to typed sql values without rewriting sql" {
    const alloc = std.testing.allocator;

    const text_value = try sqlValueFromTextParameterAlloc(alloc, text_oid, "10");
    defer http_server.ApiHttpServer.freePublicSqlParam(alloc, text_value);
    try std.testing.expect(text_value == .string);
    try std.testing.expectEqualStrings("10", text_value.string);

    const numeric_integer = try sqlValueFromTextParameterAlloc(alloc, numeric_oid, "10");
    defer http_server.ApiHttpServer.freePublicSqlParam(alloc, numeric_integer);
    try std.testing.expect(numeric_integer == .integer);
    try std.testing.expectEqual(@as(i64, 10), numeric_integer.integer);

    const numeric_float = try sqlValueFromTextParameterAlloc(alloc, numeric_oid, "10.5");
    defer http_server.ApiHttpServer.freePublicSqlParam(alloc, numeric_float);
    try std.testing.expect(numeric_float == .float);
    try std.testing.expectEqual(@as(f64, 10.5), numeric_float.float);

    const bool_value = try sqlValueFromTextParameterAlloc(alloc, bool_oid, "t");
    defer http_server.ApiHttpServer.freePublicSqlParam(alloc, bool_value);
    try std.testing.expect(bool_value == .bool);
    try std.testing.expect(bool_value.bool);

    const json_value = try sqlValueFromTextParameterAlloc(alloc, jsonb_oid, "{\"ok\":true}");
    defer http_server.ApiHttpServer.freePublicSqlParam(alloc, json_value);
    try std.testing.expect(json_value == .json);
    try std.testing.expectEqualStrings("{\"ok\":true}", json_value.json);

    try std.testing.expectError(error.InvalidPgwireParameter, sqlValueFromTextParameterAlloc(alloc, jsonb_oid, "{bad"));
}

test "pgwire binary parameters decode to typed sql values" {
    const alloc = std.testing.allocator;

    const bool_value = try sqlValueFromPgwireParameterAlloc(alloc, bool_oid, binary_format, &.{1});
    defer http_server.ApiHttpServer.freePublicSqlParam(alloc, bool_value);
    try std.testing.expect(bool_value == .bool);
    try std.testing.expect(bool_value.bool);

    var int4_bytes: [4]u8 = undefined;
    std.mem.writeInt(i32, &int4_bytes, 42, .big);
    const int4_value = try sqlValueFromPgwireParameterAlloc(alloc, int4_oid, binary_format, &int4_bytes);
    defer http_server.ApiHttpServer.freePublicSqlParam(alloc, int4_value);
    try std.testing.expect(int4_value == .integer);
    try std.testing.expectEqual(@as(i64, 42), int4_value.integer);

    var ts_bytes: [8]u8 = undefined;
    std.mem.writeInt(i64, &ts_bytes, 1_000_000, .big);
    const ts_value = try sqlValueFromPgwireParameterAlloc(alloc, timestamptz_oid, binary_format, &ts_bytes);
    defer http_server.ApiHttpServer.freePublicSqlParam(alloc, ts_value);
    try std.testing.expect(ts_value == .integer);
    try std.testing.expectEqual(@as(i64, (postgres_epoch_unix_seconds + 1) * std.time.ns_per_s), ts_value.integer);

    const json_value = try sqlValueFromPgwireParameterAlloc(alloc, jsonb_oid, binary_format, "\x01{\"ok\":true}");
    defer http_server.ApiHttpServer.freePublicSqlParam(alloc, json_value);
    try std.testing.expect(json_value == .json);
    try std.testing.expectEqualStrings("{\"ok\":true}", json_value.json);
}

test "pgwire binary result encoders use postgres wire layouts" {
    const alloc = std.testing.allocator;

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"active":true,"amount":12.5,"created_at":946684800000000000,"attrs":{"tier":"gold"}}
    , .{ .allocate = .alloc_always });
    defer parsed.deinit();
    const row = parsed.value;

    var bool_binary = try pgwireValueBinaryAlloc(alloc, row.object.get("active").?, .{ .name = "active", .type_oid = bool_oid, .type_size = bool_type_size, .antfly_type = .boolean });
    defer bool_binary.deinit();
    try std.testing.expectEqualSlices(u8, &.{1}, bool_binary.written());

    var numeric_binary = try pgwireValueBinaryAlloc(alloc, row.object.get("amount").?, .{ .name = "amount", .type_oid = numeric_oid, .type_size = text_type_size, .antfly_type = .numeric });
    defer numeric_binary.deinit();
    try std.testing.expectEqualSlices(u8, &.{ 0, 2, 0, 0, 0, 0, 0, 1, 0, 12, 0x13, 0x88 }, numeric_binary.written());

    var ts_binary = try pgwireValueBinaryAlloc(alloc, row.object.get("created_at").?, .{ .name = "created_at", .type_oid = timestamptz_oid, .type_size = timestamptz_type_size, .antfly_type = .datetime });
    defer ts_binary.deinit();
    try std.testing.expectEqualSlices(u8, &.{ 0, 0, 0, 0, 0, 0, 0, 0 }, ts_binary.written());

    var json_binary = try pgwireValueBinaryAlloc(alloc, row.object.get("attrs").?, .{ .name = "attrs", .type_oid = jsonb_oid, .type_size = text_type_size, .antfly_type = .json });
    defer json_binary.deinit();
    try std.testing.expect(json_binary.written().len > 1);
    try std.testing.expectEqual(@as(u8, 1), json_binary.written()[0]);
    try std.testing.expect(std.mem.indexOf(u8, json_binary.written()[1..], "\"tier\"") != null);
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

test "pgwire sqlstate mapping preserves postgres error classes" {
    const cases = [_]struct {
        status: u16,
        body: []const u8,
        sqlstate: []const u8,
    }{
        .{ .status = 400, .body = "current transaction is aborted", .sqlstate = "25P02" },
        .{ .status = 400, .body = "cannot execute write statement in a read-only transaction", .sqlstate = "25006" },
        .{ .status = 501, .body = "unsupported sql statement", .sqlstate = "0A000" },
        .{ .status = 400, .body = "document_sql_unsupported_join", .sqlstate = "0A000" },
        .{ .status = 404, .body = "not found", .sqlstate = "42P01" },
        .{ .status = 400, .body = "invalid sql request", .sqlstate = "42601" },
        .{ .status = 400, .body = "invalid sql write", .sqlstate = "22023" },
        .{ .status = 408, .body = "sql statement timeout", .sqlstate = "57014" },
    };
    for (cases) |case| {
        try std.testing.expectEqualStrings(case.sqlstate, sqlstateForHttpResponse(case.status, case.body));
    }
}
