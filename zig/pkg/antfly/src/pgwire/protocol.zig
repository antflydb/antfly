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

//! PostgreSQL v3 framing and simple/extended-query state machine. Storage and
//! query/expression binding belong behind backend.zig. Connection commands own
//! prepared lifetimes here; this module never interpolates or rewrites SQL.
const std = @import("std");
pub const backend = @import("backend.zig");
const values = @import("values.zig");
const commands = @import("session_commands.zig");
const Spool = @import("cursor_spool.zig").Store;
const Budget = @import("budget.zig").Budget;

pub const Limits = struct {
    frame_bytes: u32 = 1024 * 1024,
    connection_bytes: usize = 16 * 1024 * 1024,
    prepared_statements: u16 = 64,
    portals: u16 = 16,
    parameters: u16 = 256,
    columns: u16 = 1024,
    result_rows: u32 = 4096,
    cursor_rows: u32 = 65536,
    cursor_bytes: usize = 8 << 20,
    startup_timeout_ms: u32 = 5000,
    idle_timeout_ms: u32 = 60000,
    statement_timeout_ms: u32 = 30000,
};

pub const CancelHooks = struct {
    context: *anyopaque,
    register: *const fn (*anyopaque, *Session) anyerror!void,
    unregister: *const fn (*anyopaque, *Session) void,
    cancel: *const fn (*anyopaque, i32, i32) void,
};

const Prepared = struct {
    arena: std.heap.ArenaAllocator,
    statement: []const u8,
    parameter_oids: []const u32,
    description: backend.Description,
    namespace: []const u8,
};

const Portal = struct {
    arena: std.heap.ArenaAllocator,
    statement: []const u8,
    parameters: []const std.json.Value,
    types: []const backend.Type,
    formats: []const u16,
    description: backend.Description,
    namespace: ?[]const u8 = null,
    result: ?backend.Result = null,
    stream: ?backend.ReadStream = null,
    stream_opened: bool = false,
    stream_complete: bool = false,
    offset: usize = 0,
    failed: bool = false,
};

const SqlCursor = struct {
    arena: std.heap.ArenaAllocator,
    statement: []const u8,
    description: backend.Description,
    stream: ?backend.ReadStream,
    session_id: []const u8,
    fetched: u64 = 0,
    exhausted: bool = false,
    spool: ?Spool = null,
    scroll: bool = false,
    hold: bool = false,
    committed: bool = false,
    database: ?[]const u8 = null,
    namespace: ?[]const u8 = null,
    creation_epoch: u64 = 0,
};

pub const Session = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    source: backend.Backend,
    reader: *std.Io.Reader,
    writer: *std.Io.Writer,
    limits: Limits = .{},
    hooks: ?CancelHooks = null,
    identity: ?backend.Identity = null,
    database: ?[]u8 = null,
    namespace: ?[]u8 = null,
    session_id: ?[]u8 = null,
    status: backend.TransactionStatus = .idle,
    prepared: std.StringHashMapUnmanaged(Prepared) = .empty,
    portals: std.StringHashMapUnmanaged(Portal) = .empty,
    sql_cursors: std.StringHashMapUnmanaged(SqlCursor) = .empty,
    cursor_budget: ?*Budget = null,
    cursor_epoch: u64 = 0,
    statement_timeout: ?u32 = null,
    application_name: commands.ApplicationName = .{},
    namespace_setting: ?commands.Namespace = null,
    request_namespace: ?[]const u8 = null,
    transaction_namespace: ?commands.Namespace = null,
    timeout_transaction: ?struct { before: ?u32, committed: ?u32, namespace_before: ?commands.Namespace, namespace_committed: ?commands.Namespace, application_before: commands.ApplicationName, application_committed: commands.ApplicationName } = null,
    savepoints: std.ArrayList(struct { name: []const u8, epoch: u64, timeout: ?u32, committed_timeout: ?u32, namespace: ?commands.Namespace, committed_namespace: ?commands.Namespace, application: commands.ApplicationName, committed_application: commands.ApplicationName }) = .empty,
    skip_until_sync: bool = false,
    backend_pid: i32 = 0,
    cancel_key: i32 = 0,
    cancel_requested: std.atomic.Value(bool) = .init(false),
    executing: std.atomic.Value(bool) = .init(false),
    // Listener watchdog uses the same owner clock as backend requests.
    deadline_ns: std.atomic.Value(i64) = .init(0),
    deadline_changed: std.Io.Event = .unset,
    diagnostic: backend.Diagnostic = .{},
    mutation_ack_pending: bool = false,
    discard_after_reply: bool = false,

    pub fn deinit(self: *Session) void {
        if (self.hooks) |hooks| hooks.unregister(hooks.context, self);
        // Retained readers may still reference the authenticated credential.
        // Close them before disconnecting/releasing that credential.
        self.clearPortals();
        self.clearSqlCursors();
        self.clearCursorSavepoints();
        self.savepoints.deinit(self.alloc);
        if (self.cursor_budget) |budget| self.alloc.destroy(budget);
        if (self.identity) |identity| {
            self.source.vtable.disconnect(self.source.context, identity, self.session_id);
            identity.release(identity.context, self.alloc);
        }
        self.portals.deinit(self.alloc);
        self.sql_cursors.deinit(self.alloc);
        self.clearPrepared();
        self.prepared.deinit(self.alloc);
        if (self.database) |value| self.alloc.free(value);
        if (self.namespace) |value| self.alloc.free(value);
        if (self.session_id) |value| self.alloc.free(value);
    }

    pub fn setDeadline(self: *Session, ms: u32) void {
        const now: i64 = @intCast(std.Io.Clock.awake.now(self.io).nanoseconds);
        self.deadline_ns.store(now +| @as(i64, ms) * std.time.ns_per_ms, .release);
        self.deadline_changed.set(self.io);
    }

    pub fn run(self: *Session) !void {
        self.setDeadline(self.limits.startup_timeout_ms);
        const started = self.startup() catch |err| {
            if (err == error.Canceled) return err;
            try self.sendError(sqlstate(err), @errorName(err));
            try self.writer.flush();
            return err;
        };
        if (!started) return;
        while (true) {
            self.setDeadline(self.limits.idle_timeout_ms);
            const tag = self.reader.takeByte() catch |err| switch (err) {
                error.EndOfStream => return,
                else => return err,
            };
            const payload = try self.readPayload(4);
            defer self.alloc.free(payload);
            if (tag == 'X') {
                if (payload.len != 0) return error.ProtocolViolation;
                return;
            }
            if (self.skip_until_sync and tag != 'S') continue;
            const timeout = self.statement_timeout orelse self.limits.statement_timeout_ms;
            self.setDeadline(if (timeout == 0) self.limits.statement_timeout_ms else @min(timeout, self.limits.statement_timeout_ms));
            self.diagnostic = .{};
            self.mutation_ack_pending = false;
            self.dispatch(tag, payload) catch |err| {
                if (err == error.Canceled) return err;
                self.skip_until_sync = tag != 'Q';
                if (self.diagnostic.transaction_status) |status| {
                    self.status = status;
                    if (status == .idle) {
                        if (self.session_id) |id| self.alloc.free(id);
                        self.session_id = null;
                        if (self.timeout_transaction) |settings| {
                            self.statement_timeout = settings.before;
                            self.namespace_setting = settings.namespace_before;
                            self.application_name = settings.application_before;
                        }
                        self.timeout_transaction = null;
                        self.transaction_namespace = null;
                        self.clearCursorSavepoints();
                    }
                } else if (self.status == .in_transaction) {
                    self.markTransactionFailed("");
                    self.status = .failed;
                }
                if (self.status == .idle) self.finishSqlCursors(false);
                if (self.mutation_ack_pending) self.diagnostic.set("40003", "mutation committed but its acknowledgement failed; do not replay this statement", self.diagnostic.transaction_id, false);
                if (self.diagnostic.code != null) try self.sendDiagnostic('E', self.diagnostic, null) else try self.sendError(sqlstate(err), @errorName(err));
                if (tag == 'Q') try self.ready();
            };
            try self.writer.flush();
        }
    }

    fn startup(self: *Session) !bool {
        var negotiation_count: u8 = 0;
        while (true) {
            const payload = try self.readPayload(8);
            defer self.alloc.free(payload);
            var cursor = Cursor{ .bytes = payload };
            const version = try cursor.int(u32);
            switch (version) {
                80877103, 80877104 => {
                    try cursor.finish();
                    negotiation_count += 1;
                    if (negotiation_count > 2) return error.ProtocolViolation;
                    try self.writer.writeByte('N');
                    try self.writer.flush();
                },
                80877102 => {
                    const pid = try cursor.int(i32);
                    const secret = try cursor.int(i32);
                    try cursor.finish();
                    if (self.hooks) |hooks| hooks.cancel(hooks.context, pid, secret);
                    return false;
                },
                196608 => {
                    var username: ?[]const u8 = null;
                    var database: ?[]const u8 = null;
                    while (true) {
                        const key = try cursor.string();
                        if (key.len == 0) break;
                        const value = try cursor.string();
                        if (std.mem.eql(u8, key, "user")) {
                            if (username != null) return error.ProtocolViolation;
                            username = value;
                        } else if (std.mem.eql(u8, key, "database")) {
                            if (database != null) return error.ProtocolViolation;
                            database = value;
                        } else if (std.mem.eql(u8, key, "client_encoding")) {
                            if (!std.ascii.eqlIgnoreCase(value, "UTF8") and !std.ascii.eqlIgnoreCase(value, "UTF-8")) return error.UnsupportedEncoding;
                        } else if (std.mem.eql(u8, key, "application_name")) {
                            self.application_name = try commands.ApplicationName.init(value);
                        } else {
                            // Never accept unchecked search_path/options that
                            // could silently change resolution or permissions.
                            return error.UnsupportedStartupOption;
                        }
                    }
                    try cursor.finish();
                    const user = username orelse return error.AuthenticationFailed;
                    if (user.len == 0) return error.AuthenticationFailed;
                    try self.message('R', &.{ 0, 0, 0, 3 });
                    try self.writer.flush();
                    if (try self.reader.takeByte() != 'p') return error.ProtocolViolation;
                    const password_payload = try self.readPayload(5);
                    defer {
                        std.crypto.secureZero(u8, password_payload);
                        self.alloc.free(password_payload);
                    }
                    var password_cursor = Cursor{ .bytes = password_payload };
                    const password = try password_cursor.string();
                    try password_cursor.finish();
                    self.identity = self.source.vtable.authenticate(self.source.context, self.alloc, user, password) catch {
                        try self.sendError("28P01", "authentication failed");
                        try self.writer.flush();
                        return false;
                    };
                    // Authenticate first; database is only an untrusted
                    // resolution hint carried to the authorized backend.
                    if (database) |value| self.database = try self.alloc.dupe(u8, value);
                    if (self.hooks) |hooks| try hooks.register(hooks.context, self);
                    try self.message('R', &.{ 0, 0, 0, 0 });
                    try self.parameterStatus("server_version", "16.0-antfly");
                    try self.parameterStatus("server_encoding", "UTF8");
                    try self.parameterStatus("client_encoding", "UTF8");
                    try self.parameterStatus("DateStyle", "ISO, MDY");
                    try self.parameterStatus("TimeZone", "UTC");
                    try self.parameterStatus("integer_datetimes", "on");
                    try self.parameterStatus("standard_conforming_strings", "on");
                    var key: [8]u8 = undefined;
                    std.mem.writeInt(i32, key[0..4], self.backend_pid, .big);
                    std.mem.writeInt(i32, key[4..8], self.cancel_key, .big);
                    try self.message('K', &key);
                    try self.ready();
                    try self.writer.flush();
                    return true;
                },
                else => return error.ProtocolViolation,
            }
        }
    }

    fn readPayload(self: *Session, minimum: u32) ![]u8 {
        const len = try self.reader.takeInt(u32, .big);
        if (len < minimum or len > self.limits.frame_bytes) return error.ProtocolViolation;
        return self.reader.readAlloc(self.alloc, len - 4);
    }

    fn effectiveNamespace(self: *const Session) []const u8 {
        return if (self.namespace_setting) |*value| value.slice() else self.namespace orelse "public";
    }

    fn request(self: *Session, statement: []const u8, parameters: []const std.json.Value, types: []const backend.Type) backend.Request {
        return .{
            .statement = statement,
            .parameters = parameters,
            .parameter_types = types,
            .database = self.database,
            .namespace = self.request_namespace orelse self.effectiveNamespace(),
            .session_namespace = if (self.transaction_namespace) |*value| value.slice() else null,
            .session_id = self.session_id,
            .limit = self.limits.result_rows,
            .io = self.io,
            .deadline = .{ .clock = .awake, .raw = .{ .nanoseconds = self.deadline_ns.load(.acquire) } },
            .cancel_requested = &self.cancel_requested,
            .diagnostics = &self.diagnostic,
        };
    }

    fn describe(self: *Session, alloc: std.mem.Allocator, statement: []const u8, types: []const backend.Type) !backend.Description {
        if (try commands.settingCommand(alloc, statement)) |setting| {
            if (types.len != 0) return error.InvalidParameter;
            return .{ .columns = switch (setting) {
                .search_path => |value| if (value == .show) &.{.{ .name = "search_path", .type = .string }} else &.{},
                .statement_timeout => |value| if (value == .show) &.{.{ .name = "statement_timeout", .type = .string }} else &.{},
                .application_name => |value| if (value == .show) &.{.{ .name = "application_name", .type = .string }} else &.{},
                .client_encoding => |value| if (value == .show) &.{.{ .name = "client_encoding", .type = .string }} else &.{},
                .reset_all => &.{},
                .discard_all => &.{},
            } };
        }
        self.cancel_requested.store(false, .release);
        self.executing.store(true, .release);
        defer self.executing.store(false, .release);
        const req = self.request(statement, &.{}, types);
        try req.check();
        const result = try self.source.vtable.describe(self.source.context, alloc, self.identity orelse return error.AuthenticationFailed, req);
        if (result.columns.len > self.limits.columns or result.parameter_types.len > self.limits.parameters) return error.ProgramLimitExceeded;
        return result;
    }

    fn execute(self: *Session, alloc: std.mem.Allocator, statement: []const u8, parameters: []const std.json.Value, types: []const backend.Type, binding_guard: ?[]const u8) !backend.Result {
        if (try commands.settingCommand(alloc, statement)) |setting| {
            if (parameters.len != 0 or types.len != 0) return error.InvalidParameter;
            return switch (setting) {
                .search_path => |value| self.executeSearchPathSetting(alloc, statement, value),
                .statement_timeout => |value| self.executeTimeoutSetting(alloc, value),
                .application_name => |value| self.executeApplicationNameSetting(alloc, value),
                .client_encoding => |value| self.executeEncodingSetting(alloc, value),
                .reset_all => self.executeResetAll(),
                .discard_all => self.executeDiscardAll(),
            };
        }
        self.cancel_requested.store(false, .release);
        self.executing.store(true, .release);
        defer self.executing.store(false, .release);
        var req = self.request(statement, parameters, types);
        req.binding_guard = binding_guard;
        try req.check();
        const control = try commands.control(alloc, statement);
        var savepoint_name: ?[]const u8 = null;
        defer if (savepoint_name) |name| self.alloc.free(name);
        if (control) |value| if (value == .savepoint) {
            if (self.savepoints.items.len >= 64) return error.ProgramLimitExceeded;
            savepoint_name = try self.alloc.dupe(u8, value.savepoint);
            try self.savepoints.ensureUnusedCapacity(self.alloc, 1);
        };
        if (commands.isCommit(statement)) try self.materializeHeldCursors();
        const previous_status = self.status;
        var result = try self.source.vtable.execute(self.source.context, alloc, self.identity orelse return error.AuthenticationFailed, req);
        errdefer result.deinit();
        if (result.mutation_outcome != null) {
            self.mutation_ack_pending = true;
            self.diagnostic.transaction_id = result.transaction_id;
        }
        // Do not turn a cancellation arriving after a mutation committed into
        // a misleading safe-abort response; the backend owns that boundary.
        if (result.continuation != null) return error.UnsupportedContinuation;
        if (result.rows.len > self.limits.result_rows or result.columns.len > self.limits.columns) return error.ProgramLimitExceeded;
        for (result.rows) |row| if (row.len != result.columns.len) return error.InvalidResult;
        const session = if (result.session_id) |id| try self.alloc.dupe(u8, id) else null;
        if (self.session_id) |old| self.alloc.free(old);
        self.session_id = session;
        self.status = result.transaction_status;
        if (previous_status == .idle and self.status == .in_transaction) {
            self.timeout_transaction = .{ .before = self.statement_timeout, .committed = self.statement_timeout, .namespace_before = self.namespace_setting, .namespace_committed = self.namespace_setting, .application_before = self.application_name, .application_committed = self.application_name };
            self.transaction_namespace = try commands.Namespace.init(req.namespace orelse "public");
        }
        if (control) |value| switch (value) {
            .savepoint => {
                self.savepoints.appendAssumeCapacity(.{ .name = savepoint_name.?, .epoch = self.cursor_epoch, .timeout = self.statement_timeout, .committed_timeout = if (self.timeout_transaction) |settings| settings.committed else self.statement_timeout, .namespace = self.namespace_setting, .committed_namespace = if (self.timeout_transaction) |settings| settings.namespace_committed else self.namespace_setting, .application = self.application_name, .committed_application = if (self.timeout_transaction) |settings| settings.application_committed else self.application_name });
                savepoint_name = null;
            },
            .release, .rollback_to => |name| {
                var index = self.savepoints.items.len;
                while (index != 0) {
                    index -= 1;
                    if (std.mem.eql(u8, name, self.savepoints.items[index].name)) {
                        if (value == .rollback_to) {
                            self.discardCursorsAfter(self.savepoints.items[index].epoch);
                            self.statement_timeout = self.savepoints.items[index].timeout;
                            self.namespace_setting = self.savepoints.items[index].namespace;
                            self.application_name = self.savepoints.items[index].application;
                            if (self.timeout_transaction) |*settings| {
                                settings.committed = self.savepoints.items[index].committed_timeout;
                                settings.namespace_committed = self.savepoints.items[index].committed_namespace;
                                settings.application_committed = self.savepoints.items[index].committed_application;
                            }
                        }
                        const keep = index + @as(usize, if (value == .rollback_to) 1 else 0);
                        for (self.savepoints.items[keep..]) |point| self.alloc.free(point.name);
                        self.savepoints.shrinkRetainingCapacity(keep);
                        break;
                    }
                }
            },
        };
        if (self.status == .idle) self.clearCursorSavepoints();
        if (self.status == .idle) if (self.timeout_transaction) |settings| {
            self.statement_timeout = if (std.ascii.eqlIgnoreCase(result.command_tag, "COMMIT")) settings.committed else settings.before;
            self.namespace_setting = if (std.ascii.eqlIgnoreCase(result.command_tag, "COMMIT")) settings.namespace_committed else settings.namespace_before;
            self.application_name = if (std.ascii.eqlIgnoreCase(result.command_tag, "COMMIT")) settings.application_committed else settings.application_before;
            self.timeout_transaction = null;
            self.transaction_namespace = null;
        };
        if (self.status == .idle) self.finishSqlCursors(std.ascii.eqlIgnoreCase(result.command_tag, "COMMIT"));
        return result;
    }

    fn executeSearchPathSetting(self: *Session, alloc: std.mem.Allocator, statement: []const u8, setting: commands.SearchPathSetting) !backend.Result {
        if (self.status == .failed) return error.InFailedSqlTransaction;
        var result: backend.Result = .{ .command_tag = if (setting == .show) "SHOW" else if (setting == .reset) "RESET" else "SET", .transaction_status = self.status, .session_id = self.session_id };
        if (setting == .show) {
            result.columns = &.{.{ .name = "search_path", .type = .string }};
            const row = try alloc.alloc(std.json.Value, 1);
            row[0] = .{ .string = try alloc.dupe(u8, self.effectiveNamespace()) };
            const rows = try alloc.alloc([]const std.json.Value, 1);
            rows[0] = row;
            result.rows = rows;
            return result;
        }
        const update = if (setting == .reset) (commands.SearchPathSetting{ .set = .{ .local = false, .namespace = null } }).set else setting.set;
        if (update.local and self.status != .in_transaction) return error.NoActiveSqlTransaction;
        const validate = self.source.vtable.validate_namespace orelse return error.UnsupportedSqlExecution;
        var request_value = self.request(statement, &.{}, &.{});
        request_value.namespace = if (update.namespace) |*value| value.slice() else self.namespace orelse "public";
        self.cancel_requested.store(false, .release);
        self.executing.store(true, .release);
        defer self.executing.store(false, .release);
        try request_value.check();
        try validate(self.source.context, alloc, self.identity orelse return error.AuthenticationFailed, request_value);
        self.namespace_setting = update.namespace;
        if (!update.local) if (self.timeout_transaction) |*settings| {
            settings.namespace_committed = update.namespace;
        };
        return result;
    }

    fn executeTimeoutSetting(self: *Session, alloc: std.mem.Allocator, setting: commands.TimeoutSetting) !backend.Result {
        if (self.status == .failed) return error.InFailedSqlTransaction;
        var result: backend.Result = .{ .command_tag = if (setting == .show) "SHOW" else if (setting == .reset) "RESET" else "SET", .transaction_status = self.status, .session_id = self.session_id };
        if (setting == .show) {
            result.columns = &.{.{ .name = "statement_timeout", .type = .string }};
            const row = try alloc.alloc(std.json.Value, 1);
            row[0] = .{ .string = try std.fmt.allocPrint(alloc, "{d}ms", .{self.statement_timeout orelse self.limits.statement_timeout_ms}) };
            const rows = try alloc.alloc([]const std.json.Value, 1);
            rows[0] = row;
            result.rows = rows;
            return result;
        }
        const update = if (setting == .reset) (commands.TimeoutSetting{ .set = .{ .local = false, .milliseconds = null } }).set else setting.set;
        if (update.local and self.status != .in_transaction) return error.NoActiveSqlTransaction;
        // A client may shorten its deadline or request zero, but cannot remove
        // the operator-configured hard statement timeout.
        self.statement_timeout = update.milliseconds;
        if (!update.local) if (self.timeout_transaction) |*settings| {
            settings.committed = update.milliseconds;
        };
        return result;
    }

    fn executeApplicationNameSetting(self: *Session, alloc: std.mem.Allocator, setting: commands.ApplicationNameSetting) !backend.Result {
        if (self.status == .failed) return error.InFailedSqlTransaction;
        var result: backend.Result = .{ .command_tag = if (setting == .show) "SHOW" else if (setting == .reset) "RESET" else "SET", .transaction_status = self.status, .session_id = self.session_id };
        if (setting == .show) {
            result.columns = &.{.{ .name = "application_name", .type = .string }};
            const row = try alloc.alloc(std.json.Value, 1);
            row[0] = .{ .string = try alloc.dupe(u8, self.application_name.slice()) };
            const rows = try alloc.alloc([]const std.json.Value, 1);
            rows[0] = row;
            result.rows = rows;
            return result;
        }
        const update = if (setting == .reset) (commands.ApplicationNameSetting{ .set = .{ .local = false, .value = .{} } }).set else setting.set;
        if (update.local and self.status != .in_transaction) return error.NoActiveSqlTransaction;
        self.application_name = update.value;
        if (!update.local) if (self.timeout_transaction) |*settings| {
            settings.application_committed = update.value;
        };
        return result;
    }

    fn executeEncodingSetting(self: *Session, alloc: std.mem.Allocator, setting: commands.EncodingSetting) !backend.Result {
        if (self.status == .failed) return error.InFailedSqlTransaction;
        var result: backend.Result = .{ .command_tag = if (setting == .show) "SHOW" else if (setting == .reset) "RESET" else "SET", .transaction_status = self.status, .session_id = self.session_id };
        if (setting == .show) {
            result.columns = &.{.{ .name = "client_encoding", .type = .string }};
            const row = try alloc.alloc(std.json.Value, 1);
            row[0] = .{ .string = "UTF8" };
            const rows = try alloc.alloc([]const std.json.Value, 1);
            rows[0] = row;
            result.rows = rows;
        } else if (setting == .set and setting.set.local and self.status != .in_transaction) return error.NoActiveSqlTransaction;
        return result;
    }

    fn executeResetAll(self: *Session) !backend.Result {
        if (self.status == .failed) return error.InFailedSqlTransaction;
        self.statement_timeout = null;
        self.namespace_setting = null;
        self.application_name = .{};
        // RESET ALL is a session-level change even inside a transaction.
        // Savepoint snapshots can still roll it back locally; a committed
        // transaction publishes the reset values.
        if (self.timeout_transaction) |*settings| {
            settings.committed = null;
            settings.namespace_committed = null;
            settings.application_committed = .{};
        }
        return .{ .command_tag = "RESET", .transaction_status = self.status, .session_id = self.session_id };
    }

    fn executeDiscardAll(self: *Session) !backend.Result {
        if (self.status == .failed) return error.InFailedSqlTransaction;
        if (self.status != .idle) return error.ActiveSqlTransaction;
        _ = try self.executeResetAll();
        // The current extended-protocol portal may own the statement and
        // result being serialized. Release resources only after CommandComplete.
        self.discard_after_reply = true;
        return .{ .command_tag = "DISCARD ALL", .transaction_status = .idle, .session_id = self.session_id };
    }

    fn finishDiscardAll(self: *Session) void {
        if (!self.discard_after_reply) return;
        self.discard_after_reply = false;
        self.clearPortals();
        self.clearSqlCursors();
        self.clearPrepared();
    }

    fn dispatch(self: *Session, tag: u8, payload: []const u8) !void {
        var cursor = Cursor{ .bytes = payload };
        switch (tag) {
            'Q' => {
                const statement = try cursor.string();
                try cursor.finish();
                self.removePortal("");
                self.removePrepared("");
                if (std.mem.trim(u8, statement, " \t\r\n;").len == 0) {
                    try self.message('I', "");
                } else {
                    var arena = std.heap.ArenaAllocator.init(self.alloc);
                    defer arena.deinit();
                    if (try self.sessionCommand(arena.allocator(), statement)) {
                        if (self.status == .idle) {
                            self.clearPortals();
                            self.finishSqlCursors(false);
                        }
                        try self.ready();
                        return;
                    }
                    if (try self.simpleStream(statement)) {
                        if (self.status == .idle) self.clearPortals();
                        try self.ready();
                        return;
                    }
                    var result = try self.execute(arena.allocator(), statement, &.{}, &.{}, null);
                    defer result.deinit();
                    if (result.columns.len > 0) try self.rowDescription(result.columns, &.{});
                    if (result.sql_nulls) |flags| if (flags.len != result.rows.len) return error.InvalidResult;
                    for (result.rows, 0..) |row, index| try self.dataRow(result.columns, &.{}, row, if (result.sql_nulls) |flags| flags[index] else null);
                    try self.complete(result);
                    self.finishDiscardAll();
                }
                if (self.status == .idle) {
                    self.clearPortals();
                    self.finishSqlCursors(false);
                }
                try self.ready();
            },
            'P' => {
                const name = try cursor.string();
                const statement = try cursor.string();
                const count = try cursor.int(u16);
                if (count > self.limits.parameters) return error.ProgramLimitExceeded;
                if (name.len != 0 and self.prepared.contains(name)) return error.DuplicatePreparedStatement;
                if (!self.prepared.contains(name) and self.prepared.count() >= self.limits.prepared_statements) return error.ProgramLimitExceeded;
                var arena = std.heap.ArenaAllocator.init(self.alloc);
                var transferred = false;
                errdefer if (!transferred) arena.deinit();
                const a = arena.allocator();
                const declared = try a.alloc(u32, count);
                const types = try a.alloc(backend.Type, count);
                for (declared, types) |*oid, *kind| {
                    oid.* = try cursor.int(u32);
                    kind.* = try values.fromOid(oid.*);
                }
                try cursor.finish();
                const description = try self.describe(a, statement, types);
                if (count > description.parameter_types.len) return error.InvalidParameter;
                const oids = try a.alloc(u32, description.parameter_types.len);
                for (oids, description.parameter_types, 0..) |*oid, kind, index| {
                    oid.* = if (index < declared.len and declared[index] != 0) declared[index] else values.oid(kind);
                }
                const owned_statement = try a.dupe(u8, statement);
                const owned_name = try self.alloc.dupe(u8, name);
                errdefer if (!transferred) self.alloc.free(owned_name);
                self.removePrepared(name);
                if (name.len == 0) self.removePortal("");
                const namespace = try a.dupe(u8, self.request_namespace orelse self.effectiveNamespace());
                try self.prepared.put(self.alloc, owned_name, .{ .arena = arena, .statement = owned_statement, .parameter_oids = oids, .description = description, .namespace = namespace });
                transferred = true;
                try self.message('1', "");
            },
            'B' => {
                const name = try cursor.string();
                const statement_name = try cursor.string();
                const statement = self.prepared.get(statement_name) orelse return error.InvalidStatementName;
                if (name.len != 0 and self.portals.contains(name)) return error.DuplicatePortal;
                if (!self.portals.contains(name) and self.portals.count() >= self.limits.portals) return error.ProgramLimitExceeded;
                var arena = std.heap.ArenaAllocator.init(self.alloc);
                var transferred = false;
                errdefer if (!transferred) arena.deinit();
                const a = arena.allocator();
                const format_count = try cursor.int(u16);
                if (format_count > self.limits.parameters) return error.ProgramLimitExceeded;
                const formats = try a.alloc(u16, format_count);
                for (formats) |*format| {
                    format.* = try cursor.int(u16);
                    if (format.* > 1) return error.UnsupportedParameterFormat;
                }
                const count = try cursor.int(u16);
                if (count != statement.parameter_oids.len or (formats.len > 1 and formats.len != count)) return error.InvalidParameter;
                const parameters = try a.alloc(std.json.Value, count);
                const types = try a.alloc(backend.Type, count);
                for (parameters, types, statement.parameter_oids, 0..) |*parameter, *kind, oid, index| {
                    kind.* = try values.fromOid(oid);
                    const len = try cursor.int(i32);
                    if (len == -1) {
                        parameter.* = .null;
                        continue;
                    }
                    if (len < 0) return error.ProtocolViolation;
                    parameter.* = try values.decode(a, oid, formatAt(formats, index), try cursor.take(@intCast(len)));
                }
                const result_count = try cursor.int(u16);
                if (result_count > self.limits.columns or (result_count > 1 and result_count != statement.description.columns.len)) return error.UnsupportedResultFormat;
                const result_formats = try a.alloc(u16, result_count);
                for (result_formats) |*format| {
                    format.* = try cursor.int(u16);
                    if (format.* > 1) return error.UnsupportedResultFormat;
                }
                try cursor.finish();
                const text = try a.dupe(u8, statement.statement);
                const description = backend.Description{
                    .columns = try cloneColumns(a, statement.description.columns),
                    .parameter_types = types,
                    .binding_guard = if (statement.description.binding_guard) |guard| try a.dupe(u8, guard) else null,
                };
                const owned_name = try self.alloc.dupe(u8, name);
                errdefer if (!transferred) self.alloc.free(owned_name);
                self.removePortal(name);
                const namespace = try a.dupe(u8, statement.namespace);
                try self.portals.put(self.alloc, owned_name, .{ .arena = arena, .statement = text, .parameters = parameters, .types = types, .formats = result_formats, .description = description, .namespace = namespace });
                transferred = true;
                try self.message('2', "");
            },
            'D' => {
                const target = try cursor.int(u8);
                const name = try cursor.string();
                try cursor.finish();
                switch (target) {
                    'S' => {
                        const prepared = self.prepared.get(name) orelse return error.InvalidStatementName;
                        var bytes = std.Io.Writer.Allocating.init(self.alloc);
                        defer bytes.deinit();
                        try bytes.writer.writeInt(u16, @intCast(prepared.parameter_oids.len), .big);
                        for (prepared.parameter_oids) |oid| try bytes.writer.writeInt(u32, oid, .big);
                        try self.message('t', bytes.written());
                        try self.rowDescription(prepared.description.columns, &.{});
                    },
                    'P' => {
                        const portal = self.portals.get(name) orelse return error.InvalidPortalName;
                        try self.rowDescription(portal.description.columns, portal.formats);
                    },
                    else => return error.ProtocolViolation,
                }
            },
            'E' => {
                const name = try cursor.string();
                const requested = try cursor.int(i32);
                if (requested < 0) return error.ProtocolViolation;
                try cursor.finish();
                const portal = self.portals.getPtr(name) orelse return error.InvalidPortalName;
                const previous_namespace = self.request_namespace;
                self.request_namespace = portal.namespace;
                defer self.request_namespace = previous_namespace;
                if (portal.failed) return error.PortalExecutionFailed;
                portal.failed = true;
                errdefer if (portal.stream) |stream| {
                    stream.close(stream.context);
                    portal.stream = null;
                };
                if (!portal.stream_opened) {
                    portal.stream_opened = true;
                    if (self.source.vtable.open_stream) |open| if (try commands.settingCommand(portal.arena.allocator(), portal.statement) == null) {
                        self.cancel_requested.store(false, .release);
                        self.executing.store(true, .release);
                        defer self.executing.store(false, .release);
                        var req = self.request(portal.statement, portal.parameters, portal.types);
                        req.binding_guard = portal.description.binding_guard;
                        portal.stream = try open(self.source.context, self.alloc, self.identity orelse return error.AuthenticationFailed, req);
                    };
                }
                if (portal.stream != null or portal.stream_complete) {
                    try self.executeStream(portal, requested);
                    portal.failed = false;
                    return;
                }
                if (portal.result == null) {
                    portal.result = try self.execute(portal.arena.allocator(), portal.statement, portal.parameters, portal.types, portal.description.binding_guard);
                    const result = portal.result.?;
                    if (!columnsEqual(portal.description.columns, result.columns)) return error.ResultShapeChanged;
                }
                const result = portal.result.?;
                if (result.mutation_outcome != null) {
                    self.mutation_ack_pending = true;
                    self.diagnostic.transaction_id = result.transaction_id;
                }
                const count = if (requested == 0) result.rows.len - portal.offset else @min(@as(usize, @intCast(requested)), result.rows.len - portal.offset);
                if (result.sql_nulls) |flags| if (flags.len != result.rows.len) return error.InvalidResult;
                for (result.rows[portal.offset..][0..count], portal.offset..) |row, index| try self.dataRow(result.columns, portal.formats, row, if (result.sql_nulls) |flags| flags[index] else null);
                portal.offset += count;
                if (portal.offset < result.rows.len) try self.message('s', "") else try self.complete(result);
                if (result.ddl_receipt_json != null and std.mem.eql(u8, result.command_tag, "DDL PENDING")) {
                    self.skip_until_sync = true;
                    return;
                }
                portal.failed = false;
                self.finishDiscardAll();
            },
            'C' => {
                const target = try cursor.int(u8);
                const name = try cursor.string();
                try cursor.finish();
                switch (target) {
                    'S' => self.removePrepared(name),
                    'P' => self.removePortal(name),
                    else => return error.ProtocolViolation,
                }
                try self.message('3', "");
            },
            'S' => {
                try cursor.finish();
                self.skip_until_sync = false;
                if (self.status == .idle) self.clearPortals();
                try self.ready();
            },
            'H' => {
                try cursor.finish();
                try self.writer.flush();
            },
            else => return error.UnsupportedProtocolMessage,
        }
    }

    fn sessionCommand(self: *Session, alloc: std.mem.Allocator, text: []const u8) !bool {
        const parsed_command = (try @import("session_commands.zig").parse(alloc, text, self.limits.parameters)) orelse return false;
        if (self.status == .failed) return error.InFailedSqlTransaction;
        switch (parsed_command) {
            .prepare => |prepare| {
                if (self.prepared.contains(prepare.name)) return error.DuplicatePreparedStatement;
                if (self.prepared.count() >= self.limits.prepared_statements) return error.ProgramLimitExceeded;
                var arena = std.heap.ArenaAllocator.init(self.alloc);
                errdefer arena.deinit();
                const a = arena.allocator();
                const description = try self.describe(a, prepare.statement, prepare.types);
                if (prepare.types.len > description.parameter_types.len) return error.InvalidParameter;
                const oids = try a.alloc(u32, description.parameter_types.len);
                for (oids, description.parameter_types) |*oid, kind| oid.* = values.oid(kind);
                const sql = try a.dupe(u8, prepare.statement);
                const name = try self.alloc.dupe(u8, prepare.name);
                errdefer self.alloc.free(name);
                const namespace = try a.dupe(u8, self.request_namespace orelse self.effectiveNamespace());
                try self.prepared.put(self.alloc, name, .{ .arena = arena, .statement = sql, .parameter_oids = oids, .description = description, .namespace = namespace });
                // Map ownership has transferred before writing an acknowledgement.
                // A failed socket is handled by connection cleanup, never replay.
            },
            .deallocate => |name| {
                if (name) |named| {
                    if (!self.prepared.contains(named)) return error.InvalidStatementName;
                    self.removePrepared(named);
                } else {
                    var iterator = self.prepared.iterator();
                    while (iterator.next()) |entry| {
                        self.alloc.free(entry.key_ptr.*);
                        entry.value_ptr.arena.deinit();
                    }
                    self.prepared.clearRetainingCapacity();
                }
            },
            .execute => |execute_command| {
                const prepared = self.prepared.get(execute_command.name) orelse return error.InvalidStatementName;
                const previous_namespace = self.request_namespace;
                self.request_namespace = prepared.namespace;
                defer self.request_namespace = previous_namespace;
                if (execute_command.expressions.len != prepared.description.parameter_types.len) return error.InvalidParameter;
                const evaluator = self.source.vtable.evaluate_parameters orelse return error.UnsupportedSqlExecution;
                self.cancel_requested.store(false, .release);
                self.executing.store(true, .release);
                defer self.executing.store(false, .release);
                const req = self.request(prepared.statement, &.{}, prepared.description.parameter_types);
                try req.check();
                const parameters = try evaluator(self.source.context, alloc, self.identity orelse return error.AuthenticationFailed, req, execute_command.expressions);
                if (parameters.len != prepared.description.parameter_types.len) return error.InvalidParameter;
                if (try self.simpleStreamParameters(prepared.statement, parameters, prepared.description.parameter_types, prepared.description.binding_guard)) return true;
                var result = try self.execute(alloc, prepared.statement, parameters, prepared.description.parameter_types, prepared.description.binding_guard);
                defer result.deinit();
                if (result.columns.len > 0) try self.rowDescription(result.columns, &.{});
                if (result.sql_nulls) |flags| if (flags.len != result.rows.len) return error.InvalidResult;
                for (result.rows, 0..) |row, index| try self.dataRow(result.columns, &.{}, row, if (result.sql_nulls) |flags| flags[index] else null);
                try self.complete(result);
                return true;
            },
            .declare_cursor => |declaration| {
                try self.declareCursor(declaration.name, declaration.statement, declaration.scroll, declaration.hold);
                return true;
            },
            .fetch_cursor => |fetch| {
                self.fetchCursor(fetch) catch |err| {
                    self.removeSqlCursor(fetch.name);
                    return err;
                };
                return true;
            },
            .close_cursor => |name| {
                if (name) |cursor_name| {
                    if (!self.sql_cursors.contains(cursor_name)) return error.InvalidCursorName;
                    self.removeSqlCursor(cursor_name);
                } else self.clearSqlCursors();
                try self.command("CLOSE CURSOR");
                return true;
            },
        }
        try self.complete(.{ .command_tag = switch (parsed_command) {
            .prepare => "PREPARE",
            .deallocate => "DEALLOCATE",
            else => unreachable,
        }, .transaction_status = self.status });
        return true;
    }

    fn declareCursor(self: *Session, name: []const u8, statement: []const u8, scroll: bool, hold: bool) !void {
        if (self.status == .failed) return error.SqlTransactionAborted;
        if (!hold and (self.status != .in_transaction or self.session_id == null)) return error.CursorMustBeInTransaction;
        if (self.sql_cursors.contains(name)) return error.DuplicateCursorName;
        if (self.sql_cursors.count() >= self.limits.portals) return error.ProgramLimitExceeded;
        var arena = std.heap.ArenaAllocator.init(self.alloc);
        var arena_moved = false;
        defer if (!arena_moved) arena.deinit();
        const description = try self.describe(arena.allocator(), statement, &.{});
        if (description.parameter_types.len != 0) return error.InvalidParameter;
        self.cancel_requested.store(false, .release);
        self.executing.store(true, .release);
        defer self.executing.store(false, .release);
        var req = self.request(statement, &.{}, &.{});
        req.binding_guard = description.binding_guard;
        try req.check();
        const identity = self.identity orelse return error.AuthenticationFailed;
        const stream = if (self.source.vtable.open_stream) |open| try open(self.source.context, self.alloc, identity, req) else null;
        var stream_moved = false;
        defer if (!stream_moved) if (stream) |opened| opened.close(opened.context);
        var spool: ?Spool = null;
        var spool_moved = false;
        defer if (!spool_moved) if (spool) |*owned| owned.deinit();
        if (stream) |opened| {
            if (!columnsEqual(description.columns, opened.columns)) return error.InvalidResult;
            if (scroll or hold) if (opened.validate == null or opened.detach == null) return error.UnsupportedSqlExecution;
        }
        if (stream == null or scroll or hold) {
            if (self.cursor_budget == null) {
                const budget = try self.alloc.create(Budget);
                budget.* = .{ .child = self.alloc, .limit = @min(self.limits.cursor_bytes, self.limits.connection_bytes) };
                self.cursor_budget = budget;
            }
            spool = Spool.init(self.cursor_budget.?);
        }
        if (stream == null) {
            // Blocking sorts/aggregates decline pull execution. Materialize
            // once under the statement row cap, then copy into the shared
            // cursor byte/row budget before exposing a cursor name.
            var result = try self.source.vtable.execute(self.source.context, arena.allocator(), identity, req);
            defer result.deinit();
            const same_session = if (self.session_id) |id|
                if (result.session_id) |observed| std.mem.eql(u8, id, observed) else false
            else
                result.session_id == null;
            if (!columnsEqual(description.columns, result.columns) or !std.mem.startsWith(u8, result.command_tag, "SELECT") or
                result.mutation_outcome != null or result.ddl_receipt_json != null or result.continuation != null or
                result.transaction_status != self.status or !same_session or result.rows.len > self.limits.result_rows) return error.InvalidResult;
            spool.?.append(result, self.limits.cursor_rows) catch |err| return if (err == error.OutOfMemory) error.ProgramLimitExceeded else err;
        }
        const owned_statement = try arena.allocator().dupe(u8, statement);
        const session_id = try arena.allocator().dupe(u8, self.session_id orelse "");
        const key = try self.alloc.dupe(u8, name);
        var key_moved = false;
        defer if (!key_moved) self.alloc.free(key);
        const database = if (self.database) |value| try arena.allocator().dupe(u8, value) else null;
        const namespace = try arena.allocator().dupe(u8, self.request_namespace orelse self.effectiveNamespace());
        self.cursor_epoch = std.math.add(u64, self.cursor_epoch, 1) catch return error.ProgramLimitExceeded;
        try self.sql_cursors.put(self.alloc, key, .{ .arena = arena, .statement = owned_statement, .description = description, .stream = stream, .session_id = session_id, .scroll = scroll, .hold = hold, .committed = hold and self.status == .idle, .spool = spool, .exhausted = stream == null, .database = database, .namespace = namespace, .creation_epoch = self.cursor_epoch });
        key_moved = true;
        arena_moved = true;
        stream_moved = true;
        spool_moved = true;
        errdefer self.removeSqlCursor(name);
        if (hold and self.status == .idle) try self.fillCursor(self.sql_cursors.getPtr(name).?, std.math.maxInt(usize));
        try self.command("DECLARE CURSOR");
    }

    fn fetchCursor(self: *Session, fetch: commands.Fetch) !void {
        if (self.status == .failed) return error.SqlTransactionAborted;
        const cursor = self.sql_cursors.getPtr(fetch.name) orelse return error.InvalidCursorName;
        if (!cursor.committed and (self.status != .in_transaction or self.session_id == null or !std.mem.eql(u8, cursor.session_id, self.session_id.?))) return error.CursorMustBeInTransaction;
        self.cancel_requested.store(false, .release);
        self.executing.store(true, .release);
        defer self.executing.store(false, .release);
        var req = self.request(cursor.statement, &.{}, &.{});
        req.binding_guard = cursor.description.binding_guard;
        req.database = cursor.database;
        req.namespace = cursor.namespace;
        try req.check();
        if (cursor.spool != null) {
            self.fetchSpooled(cursor, fetch, req) catch |err| {
                self.removeSqlCursor(fetch.name);
                return err;
            };
            return;
        }
        if (fetch.direction != .forward) return error.CursorNotScrollable;
        var completed = false;
        const fetched_before = cursor.fetched;
        errdefer if (!completed) {
            if (cursor.stream) |stream| {
                cursor.stream = null;
                stream.close(stream.context);
            }
        };
        if (!fetch.move) try self.rowDescription(cursor.description.columns, &.{});
        var remaining: usize = if (fetch.count == std.math.maxInt(u32)) std.math.maxInt(usize) else fetch.count;
        while (!cursor.exhausted and remaining != 0) {
            try req.check();
            const stream = cursor.stream orelse {
                cursor.exhausted = true;
                break;
            };
            var page_arena = std.heap.ArenaAllocator.init(self.alloc);
            defer page_arena.deinit();
            const wanted: u32 = @intCast(@min(remaining, @min(self.limits.result_rows, 256)));
            var page = stream.next(stream.context, page_arena.allocator(), req, wanted) catch |err| {
                cursor.stream = null;
                stream.close(stream.context);
                return err;
            };
            defer page.result.deinit();
            if (page.result.mutation_outcome != null or page.result.continuation != null or page.result.session_id != null or page.result.rows.len > wanted or !columnsEqual(cursor.description.columns, page.result.columns)) return error.InvalidResult;
            if (page.result.rows.len == 0 and !page.exhausted) return error.InvalidResult;
            if (page.result.sql_nulls) |flags| if (flags.len != page.result.rows.len) return error.InvalidResult;
            if (!fetch.move) for (page.result.rows, 0..) |row, index| try self.dataRow(page.result.columns, &.{}, row, if (page.result.sql_nulls) |flags| flags[index] else null);
            cursor.fetched += page.result.rows.len;
            remaining -= page.result.rows.len;
            cursor.exhausted = page.exhausted;
            if (cursor.exhausted) {
                cursor.stream = null;
                stream.close(stream.context);
            }
            try self.writer.flush();
        }
        var tag: [64]u8 = undefined;
        try self.command(try std.fmt.bufPrint(&tag, "{s} {d}", .{ if (fetch.move) "MOVE" else "FETCH", cursor.fetched - fetched_before }));
        completed = true;
    }

    fn markTransactionFailed(self: *Session, statement: []const u8) void {
        const fail = self.source.vtable.fail_transaction orelse return;
        const identity = self.identity orelse return;
        fail(self.source.context, identity, self.request(statement, &.{}, &.{})) catch {};
    }

    fn cursorRequest(self: *Session, cursor: *SqlCursor) backend.Request {
        var req = self.request(cursor.statement, &.{}, &.{});
        req.database = cursor.database;
        req.namespace = cursor.namespace;
        req.session_id = if (cursor.committed or cursor.session_id.len == 0) null else cursor.session_id;
        req.binding_guard = cursor.description.binding_guard;
        return req;
    }

    fn fillCursor(self: *Session, cursor: *SqlCursor, through: usize) !void {
        const spool = &cursor.spool.?;
        const req = self.cursorRequest(cursor);
        while (!cursor.exhausted and spool.rows.items.len < through) {
            try req.check();
            const stream = cursor.stream orelse return error.InvalidResult;
            var arena = std.heap.ArenaAllocator.init(self.alloc);
            defer arena.deinit();
            const wanted: u32 = @intCast(@min(through - spool.rows.items.len, @min(self.limits.result_rows, 256)));
            var page = try stream.next(stream.context, arena.allocator(), req, wanted);
            defer page.result.deinit();
            if (page.result.mutation_outcome != null or page.result.continuation != null or page.result.session_id != null or page.result.rows.len > wanted or !columnsEqual(cursor.description.columns, page.result.columns)) return error.InvalidResult;
            if (page.result.rows.len == 0 and !page.exhausted) return error.InvalidResult;
            spool.append(page.result, self.limits.cursor_rows) catch |err| return if (err == error.OutOfMemory) error.ProgramLimitExceeded else err;
            cursor.exhausted = page.exhausted;
            if (cursor.exhausted) stream.detach.?(stream.context);
        }
    }

    fn materializeHeldCursors(self: *Session) !void {
        if (self.status != .in_transaction) return;
        var iterator = self.sql_cursors.valueIterator();
        while (iterator.next()) |cursor| if (cursor.hold and !cursor.committed) try self.fillCursor(cursor, std.math.maxInt(usize));
    }

    fn fetchSpooled(self: *Session, cursor: *SqlCursor, fetch: commands.Fetch, req: backend.Request) !void {
        if (!cursor.scroll and fetch.direction != .forward) return error.CursorNotScrollable;
        var validation = std.heap.ArenaAllocator.init(self.alloc);
        defer validation.deinit();
        try self.validateSpooledCursor(cursor, validation.allocator(), req);
        const spool = &cursor.spool.?;
        if (fetch.direction == .absolute and fetch.offset < 0) try self.fillCursor(cursor, std.math.maxInt(usize));
        var target: i64 = switch (fetch.direction) {
            .forward => spool.position +| @as(i64, if (fetch.count == 0) 0 else 1),
            .backward => spool.position -| @as(i64, if (fetch.count == 0) 0 else 1),
            .absolute => if (fetch.offset < 0) @as(i64, @intCast(spool.rows.items.len)) + 1 +| fetch.offset else fetch.offset,
            .relative => spool.position +| fetch.offset,
        };
        const single = fetch.direction == .absolute or fetch.direction == .relative or fetch.count == 0;
        var remaining: u64 = if (single) 1 else fetch.count;
        if (!fetch.move) try self.rowDescription(cursor.description.columns, &.{});
        var emitted: u64 = 0;
        while (remaining != 0) : (remaining -= 1) {
            try req.check();
            if (target > 0 and @as(u64, @intCast(target)) > spool.rows.items.len and !cursor.exhausted) {
                const wanted: usize = @intCast(@min(@as(u64, @intCast(target)) +| @min(remaining - 1, 255), std.math.maxInt(usize)));
                try self.fillCursor(cursor, wanted);
            }
            const end: i64 = @intCast(spool.rows.items.len + 1);
            spool.position = std.math.clamp(target, 0, end);
            if (target <= 0 or target >= end) break;
            if (emitted != 0 and emitted % 256 == 0) {
                _ = validation.reset(.retain_capacity);
                try self.validateSpooledCursor(cursor, validation.allocator(), req);
            }
            const row = spool.rows.items[@intCast(target - 1)];
            if (!fetch.move) try self.dataRow(cursor.description.columns, &.{}, row.values, row.nulls);
            emitted += 1;
            if (emitted % 256 == 0) try self.writer.flush();
            if (single) break;
            target += if (fetch.direction == .backward) @as(i64, -1) else 1;
        }
        var tag: [64]u8 = undefined;
        try self.command(try std.fmt.bufPrint(&tag, "{s} {d}", .{ if (fetch.move) "MOVE" else "FETCH", emitted }));
    }

    fn validateSpooledCursor(self: *Session, cursor: *SqlCursor, alloc: std.mem.Allocator, req: backend.Request) !void {
        if (cursor.stream) |stream| return stream.validate.?(stream.context, alloc, req);
        if (!cursor.exhausted) return error.InvalidCursorName;
        const identity = self.identity orelse return error.AuthenticationFailed;
        const current = try self.source.vtable.describe(self.source.context, alloc, identity, req);
        if (!columnsEqual(cursor.description.columns, current.columns) or
            !std.mem.eql(u8, cursor.description.binding_guard orelse "", current.binding_guard orelse "")) return error.CatalogGenerationChanged;
    }

    fn finishSqlCursors(self: *Session, committed: bool) void {
        if (committed) {
            var values_it = self.sql_cursors.valueIterator();
            while (values_it.next()) |cursor| if (cursor.hold and cursor.exhausted) {
                cursor.committed = true;
            };
        }
        while (true) {
            var iterator = self.sql_cursors.iterator();
            const remove = while (iterator.next()) |entry| {
                if (!entry.value_ptr.committed) break entry.key_ptr.*;
            } else break;
            self.removeSqlCursor(remove);
        }
    }

    fn clearCursorSavepoints(self: *Session) void {
        for (self.savepoints.items) |point| self.alloc.free(point.name);
        self.savepoints.clearRetainingCapacity();
    }

    fn discardCursorsAfter(self: *Session, epoch: u64) void {
        while (true) {
            var iterator = self.sql_cursors.iterator();
            const remove = while (iterator.next()) |entry| {
                if (!entry.value_ptr.committed and entry.value_ptr.creation_epoch > epoch) break entry.key_ptr.*;
            } else break;
            self.removeSqlCursor(remove);
        }
    }

    fn removeSqlCursor(self: *Session, name: []const u8) void {
        if (self.sql_cursors.fetchRemove(name)) |entry| {
            self.alloc.free(entry.key);
            var cursor = entry.value;
            if (cursor.stream) |stream| stream.close(stream.context);
            if (cursor.spool) |*spool| spool.deinit();
            cursor.arena.deinit();
        }
    }

    fn clearSqlCursors(self: *Session) void {
        var iterator = self.sql_cursors.iterator();
        while (iterator.next()) |entry| {
            self.alloc.free(entry.key_ptr.*);
            if (entry.value_ptr.stream) |stream| stream.close(stream.context);
            if (entry.value_ptr.spool) |*spool| spool.deinit();
            entry.value_ptr.arena.deinit();
        }
        self.sql_cursors.clearRetainingCapacity();
    }

    fn removePrepared(self: *Session, name: []const u8) void {
        if (self.prepared.fetchRemove(name)) |entry| {
            self.alloc.free(entry.key);
            var value = entry.value;
            value.arena.deinit();
        }
    }

    fn clearPrepared(self: *Session) void {
        var it = self.prepared.iterator();
        while (it.next()) |entry| {
            self.alloc.free(entry.key_ptr.*);
            entry.value_ptr.arena.deinit();
        }
        self.prepared.clearRetainingCapacity();
    }

    fn removePortal(self: *Session, name: []const u8) void {
        if (self.portals.fetchRemove(name)) |entry| {
            self.alloc.free(entry.key);
            var value = entry.value;
            if (value.stream) |stream| stream.close(stream.context);
            if (value.result) |*result| result.deinit();
            value.arena.deinit();
        }
    }

    fn clearPortals(self: *Session) void {
        var it = self.portals.iterator();
        while (it.next()) |entry| {
            self.alloc.free(entry.key_ptr.*);
            if (entry.value_ptr.stream) |stream| stream.close(stream.context);
            if (entry.value_ptr.result) |*result| result.deinit();
            entry.value_ptr.arena.deinit();
        }
        self.portals.clearRetainingCapacity();
    }

    fn executeStream(self: *Session, portal: *Portal, requested: i32) !void {
        self.cancel_requested.store(false, .release);
        self.executing.store(true, .release);
        defer self.executing.store(false, .release);
        var req = self.request(portal.statement, portal.parameters, portal.types);
        req.binding_guard = portal.description.binding_guard;
        var remaining: usize = if (requested == 0) std.math.maxInt(usize) else @intCast(requested);
        while (!portal.stream_complete and remaining != 0) {
            try req.check();
            var arena = std.heap.ArenaAllocator.init(self.alloc);
            defer arena.deinit();
            const stream = portal.stream orelse return error.InvalidResult;
            const wanted: u32 = @intCast(@min(remaining, @min(self.limits.result_rows, 256)));
            var page = try stream.next(stream.context, arena.allocator(), req, wanted);
            defer page.result.deinit();
            const result = page.result;
            if (result.rows.len > wanted or !columnsEqual(portal.description.columns, result.columns) or
                result.mutation_outcome != null or result.continuation != null or result.session_id != null)
                return error.InvalidResult;
            if (!page.exhausted and result.rows.len == 0) return error.InvalidResult;
            if (result.sql_nulls) |flags| if (flags.len != result.rows.len) return error.InvalidResult;
            for (result.rows, 0..) |row, index| try self.dataRow(result.columns, portal.formats, row, if (result.sql_nulls) |flags| flags[index] else null);
            remaining -= result.rows.len;
            portal.offset += result.rows.len;
            portal.stream_complete = page.exhausted;
            // Flush every bounded page. Slow clients exert backpressure before
            // another storage page is read, rather than buffering the result.
            try self.writer.flush();
        }
        if (portal.stream_complete) {
            if (portal.stream) |stream| stream.close(stream.context);
            portal.stream = null;
            var tag: [64]u8 = undefined;
            try self.command(try std.fmt.bufPrint(&tag, "SELECT {d}", .{portal.offset}));
        } else try self.message('s', "");
    }

    fn simpleStream(self: *Session, statement: []const u8) !bool {
        return self.simpleStreamParameters(statement, &.{}, &.{}, null);
    }

    fn simpleStreamParameters(self: *Session, statement: []const u8, parameters: []const std.json.Value, types: []const backend.Type, binding_guard: ?[]const u8) !bool {
        var settings_arena = std.heap.ArenaAllocator.init(self.alloc);
        defer settings_arena.deinit();
        if (try commands.settingCommand(settings_arena.allocator(), statement) != null) return false;
        const open = self.source.vtable.open_stream orelse return false;
        self.cancel_requested.store(false, .release);
        self.executing.store(true, .release);
        defer self.executing.store(false, .release);
        var req = self.request(statement, parameters, types);
        req.binding_guard = binding_guard;
        const stream = (try open(self.source.context, self.alloc, self.identity orelse return error.AuthenticationFailed, req)) orelse return false;
        var portal = Portal{
            .arena = std.heap.ArenaAllocator.init(self.alloc),
            .statement = statement,
            .parameters = parameters,
            .types = types,
            .formats = &.{},
            .description = .{ .columns = stream.columns, .parameter_types = types, .binding_guard = binding_guard },
            .stream = stream,
            .stream_opened = true,
        };
        defer portal.arena.deinit();
        defer if (portal.stream) |remaining| remaining.close(remaining.context);
        if (stream.columns.len > self.limits.columns) return error.ProgramLimitExceeded;
        try self.rowDescription(stream.columns, &.{});
        try self.executeStream(&portal, 0);
        return true;
    }

    fn message(self: *Session, tag: u8, payload: []const u8) !void {
        if (payload.len > self.limits.frame_bytes - 4) return error.ProgramLimitExceeded;
        try self.writer.writeByte(tag);
        try self.writer.writeInt(u32, @intCast(payload.len + 4), .big);
        try self.writer.writeAll(payload);
    }

    fn parameterStatus(self: *Session, name: []const u8, value: []const u8) !void {
        const payload = try std.mem.concat(self.alloc, u8, &.{ name, &.{0}, value, &.{0} });
        defer self.alloc.free(payload);
        try self.message('S', payload);
    }

    pub fn sendError(self: *Session, code: []const u8, message_text: []const u8) !void {
        const payload = try std.mem.concat(self.alloc, u8, &.{ "SERROR\x00C", code, "\x00M", message_text, "\x00\x00" });
        defer self.alloc.free(payload);
        try self.message('E', payload);
    }

    fn sendDiagnostic(self: *Session, tag: u8, diagnostic: backend.Diagnostic, outcome: ?backend.MutationOutcome) !void {
        var bytes = std.Io.Writer.Allocating.init(self.alloc);
        defer bytes.deinit();
        try bytes.writer.writeAll(if (tag == 'N') "SNOTICE\x00C" else "SERROR\x00C");
        try bytes.writer.writeAll(&diagnostic.code.?);
        try bytes.writer.writeAll("\x00M");
        try bytes.writer.writeAll(diagnostic.message[0..diagnostic.message_len]);
        try bytes.writer.writeAll("\x00D");
        try std.json.Stringify.value(.{
            .mutation_outcome = if (outcome) |value| @tagName(value) else null,
            .transaction_id = if (diagnostic.transaction_id) |*id| @as([]const u8, id) else null,
            .retryable = diagnostic.retryable,
        }, .{ .emit_null_optional_fields = false }, &bytes.writer);
        try bytes.writer.writeAll("\x00\x00");
        try self.message(tag, bytes.written());
    }

    fn complete(self: *Session, result: backend.Result) !void {
        if (result.ddl_receipt_json) |receipt| {
            var bytes = std.Io.Writer.Allocating.init(self.alloc);
            defer bytes.deinit();
            const pending = std.mem.eql(u8, result.command_tag, "DDL PENDING");
            const unknown = pending and result.mutation_outcome == null;
            try bytes.writer.writeAll(if (unknown) "SERROR\x00C40003\x00MDDL admission outcome is unknown; do not replay; inspect receipt\x00D" else if (pending) "SERROR\x00C55000\x00MDDL declaration committed but is not ready; do not replay; inspect receipt\x00D" else "SNOTICE\x00C01000\x00MDDL declaration committed; inspect receipt for validation readiness\x00D");
            try bytes.writer.writeAll(receipt);
            try bytes.writer.writeAll("\x00\x00");
            try self.message(if (pending) 'E' else 'N', bytes.written());
            if (pending) return;
        }
        if (result.mutation_outcome) |outcome| {
            if (outcome != .committed or result.transaction_id != null) {
                var receipt = backend.Diagnostic{};
                receipt.set("01000", "mutation committed; receipt records asynchronous completion state", result.transaction_id, false);
                try self.sendDiagnostic('N', receipt, outcome);
            }
        }
        try self.command(result.command_tag);
    }

    fn ready(self: *Session) !void {
        try self.message('Z', &.{@intFromEnum(self.status)});
    }

    fn command(self: *Session, tag: []const u8) !void {
        if (std.mem.indexOfScalar(u8, tag, 0) != null) return error.InvalidResult;
        const payload = try std.mem.concat(self.alloc, u8, &.{ tag, &.{0} });
        defer self.alloc.free(payload);
        try self.message('C', payload);
    }

    fn rowDescription(self: *Session, columns: []const backend.Column, formats: []const u16) !void {
        if (columns.len == 0) return self.message('n', "");
        var bytes = std.Io.Writer.Allocating.init(self.alloc);
        defer bytes.deinit();
        try bytes.writer.writeInt(u16, @intCast(columns.len), .big);
        for (columns, 0..) |column, index| {
            if (std.mem.indexOfScalar(u8, column.name, 0) != null) return error.InvalidResult;
            try bytes.writer.writeAll(column.name);
            try bytes.writer.writeByte(0);
            try bytes.writer.writeInt(u32, 0, .big);
            try bytes.writer.writeInt(u16, 0, .big);
            try bytes.writer.writeInt(u32, values.oid(column.type), .big);
            try bytes.writer.writeInt(i16, values.typeSize(column.type), .big);
            try bytes.writer.writeInt(i32, -1, .big);
            try bytes.writer.writeInt(u16, formatAt(formats, index), .big);
        }
        try self.message('T', bytes.written());
    }

    fn dataRow(self: *Session, columns: []const backend.Column, formats: []const u16, row: []const std.json.Value, null_flags: ?[]const bool) !void {
        if (row.len != columns.len) return error.InvalidResult;
        if (null_flags) |flags| if (flags.len != row.len) return error.InvalidResult;
        var bytes = std.Io.Writer.Allocating.init(self.alloc);
        defer bytes.deinit();
        try bytes.writer.writeInt(u16, @intCast(row.len), .big);
        for (row, columns, 0..) |value, column, index| {
            const sql_null = if (null_flags) |flags| flags[index] else value == .null;
            if (sql_null) {
                if (value != .null) return error.InvalidResult;
                try bytes.writer.writeInt(i32, -1, .big);
                continue;
            }
            const encoded = try values.encode(self.alloc, column.type, formatAt(formats, index), value);
            defer self.alloc.free(encoded);
            if (encoded.len > self.limits.frame_bytes) return error.ProgramLimitExceeded;
            try bytes.writer.writeInt(i32, @intCast(encoded.len), .big);
            try bytes.writer.writeAll(encoded);
        }
        try self.message('D', bytes.written());
    }
};

pub const Cursor = struct {
    bytes: []const u8,
    offset: usize = 0,
    pub fn take(self: *Cursor, len: usize) ![]const u8 {
        if (len > self.bytes.len - self.offset) return error.ProtocolViolation;
        const value = self.bytes[self.offset..][0..len];
        self.offset += len;
        return value;
    }
    pub fn int(self: *Cursor, comptime T: type) !T {
        return std.mem.readInt(T, (try self.take(@sizeOf(T)))[0..@sizeOf(T)], .big);
    }
    pub fn string(self: *Cursor) ![]const u8 {
        const end = std.mem.indexOfScalarPos(u8, self.bytes, self.offset, 0) orelse return error.ProtocolViolation;
        const value = self.bytes[self.offset..end];
        self.offset = end + 1;
        if (!std.unicode.utf8ValidateSlice(value)) return error.ProtocolViolation;
        return value;
    }
    pub fn finish(self: Cursor) !void {
        if (self.offset != self.bytes.len) return error.ProtocolViolation;
    }
};

fn formatAt(formats: []const u16, index: usize) u16 {
    return if (formats.len == 0) 0 else if (formats.len == 1) formats[0] else formats[index];
}
fn cloneColumns(alloc: std.mem.Allocator, columns: []const backend.Column) ![]const backend.Column {
    const result = try alloc.dupe(backend.Column, columns);
    for (result) |*column| column.name = try alloc.dupe(u8, column.name);
    return result;
}
fn columnsEqual(a: []const backend.Column, b: []const backend.Column) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| if (left.type != right.type or !std.mem.eql(u8, left.name, right.name)) return false;
    return true;
}

fn sqlstate(err: anyerror) []const u8 {
    return switch (err) {
        error.AuthenticationFailed, error.Unauthorized => "28000",
        error.InvalidPassword => "28P01",
        error.Forbidden, error.AccessDenied => "42501",
        error.QueryCanceled, error.Timeout => "57014",
        error.UniqueConstraintViolation => "23505",
        error.ForeignKeyParentMissing, error.ForeignKeyReferenced => "23503",
        error.InvalidParameter => "22P02",
        error.InvalidStatementName => "26000",
        error.InvalidPortalName => "34000",
        error.InvalidCursorName => "34000",
        error.DuplicatePreparedStatement => "42P05",
        error.DuplicatePortal => "42P03",
        error.DuplicateCursorName => "42P03",
        error.ProtocolViolation => "08P01",
        error.OutOfMemory, error.ProgramLimitExceeded => "54000",
        error.SyntaxError => "42601",
        error.InvalidSqlSyntax => "42601",
        error.InFailedSqlTransaction => "25P02",
        error.CursorMustBeInTransaction, error.NoActiveSqlTransaction => "25P01",
        error.CursorNotScrollable => "55000",
        error.InvalidSqlParameters, error.InvalidSqlParameter, error.InvalidSqlNumber, error.SqlTypeMismatch, error.InvalidSqlLimit => "22023",
        error.SqlNotNullViolation => "23502",
        error.SqlProgramLimitExceeded, error.SqlResultTooLarge, error.SqlLimitExceeded => "54000",
        error.UnknownColumn => "42703",
        error.UnknownTable => "42P01",
        error.UnsupportedSqlExecution, error.UnsupportedSqlShape, error.SqlStatementSnapshotRequired => "0A000",
        error.UnsupportedStartupOption => "0A000",
        error.UnsupportedEncoding => "22021",
        error.PortalExecutionFailed => "55000",
        error.TooManyConnections, error.SqlWriteCapacityUnavailable => "53300",
        error.UnsupportedParameterType, error.UnsupportedParameterFormat, error.UnsupportedResultFormat, error.UnsupportedProtocolMessage, error.UnsupportedContinuation, error.UnsupportedResultPrecision, error.UnsupportedStatement, error.ResultShapeChanged => "0A000",
        else => "XX000",
    };
}
