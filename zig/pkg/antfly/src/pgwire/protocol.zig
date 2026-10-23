// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! PostgreSQL v3 framing and simple/extended-query state machine. Storage and
//! query/expression binding belong behind backend.zig. Connection commands own
//! prepared lifetimes here; this module never interpolates or rewrites SQL.
const std = @import("std");
pub const backend = @import("backend.zig");
const values = @import("values.zig");
const formatting = @import("formatting.zig");
const commands = @import("session_commands.zig");
const settings_catalog = @import("antfly_local_sources").sql_setting_catalog;
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
    retained_bytes: usize = 8 << 20,
    idle_transaction_timeout_ms: u32 = 30_000,
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
    arena: *std.heap.ArenaAllocator,
    statement: []const u8,
    parameter_oids: []const u32,
    description: backend.Description,
    namespace: []const u8,
    search_path: ?commands.SearchPath = null,
};

const Portal = struct {
    arena: *std.heap.ArenaAllocator,
    statement: []const u8,
    parameters: []const std.json.Value,
    parameter_cells: ?[]const @import("antfly_local_sources").sql_scalar.Datum = null,
    parameter_owner: ?*@import("antfly_local_sources").sql_parameter_frame.Ingress = null,
    types: []const backend.Type,
    formats: []const u16,
    description: backend.Description,
    namespace: ?[]const u8 = null,
    search_path: ?commands.SearchPath = null,
    result: ?backend.Result = null,
    stream: ?backend.ReadStream = null,
    stream_opened: bool = false,
    stream_complete: bool = false,
    offset: usize = 0,
    failed: bool = false,
};

const SqlCursor = struct {
    arena: *std.heap.ArenaAllocator,
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
    search_path: ?commands.SearchPath = null,
    creation_epoch: u64 = 0,
};

/// Only the session thread allocates or physically destroys retained arenas.
/// Backend release may run on another thread; its final atomic decrement is
/// the last access to this header. No allocator or wakeup borrows escape it.
const RetainedArena = struct {
    arena: std.heap.ArenaAllocator,
    references: std.atomic.Value(usize) = .init(1),
    next: ?*RetainedArena = null,

    fn lifetime(arena: *std.heap.ArenaAllocator) backend.RetainedLifetime {
        const self: *RetainedArena = @fieldParentPtr("arena", arena);
        return .{ .context = self, .retain_fn = retain, .release_fn = release };
    }

    fn retain(raw: *anyopaque) void {
        const self: *RetainedArena = @ptrCast(@alignCast(raw));
        const previous = self.references.fetchAdd(1, .monotonic);
        std.debug.assert(previous != 0 and previous != std.math.maxInt(usize));
    }

    fn release(raw: *anyopaque) void {
        const self: *RetainedArena = @ptrCast(@alignCast(raw));
        const previous = self.references.fetchSub(1, .acq_rel);
        std.debug.assert(previous != 0);
    }
};

pub const Session = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    source: backend.Backend,
    reader: *std.Io.Reader,
    writer: *std.Io.Writer,
    limits: Limits = .{},
    frame_memory: ?*@import("../common/allocation_budget.zig").Partitioned = null,
    control_allocator: ?std.mem.Allocator = null,
    is_protected_control: ?*const fn ([]const u8) bool = null,
    hooks: ?CancelHooks = null,
    identity: ?backend.Identity = null,
    database: ?[]u8 = null,
    namespace: ?[]u8 = null,
    session_id: ?[]u8 = null,
    status: backend.TransactionStatus = .idle,
    completion_drain: bool = false,
    prepared: std.StringHashMapUnmanaged(Prepared) = .empty,
    // Results borrow their portal allocator until release. Registry growth and
    // removal must never move that allocator.
    portals: std.StringHashMapUnmanaged(*Portal) = .empty,
    sql_cursors: std.StringHashMapUnmanaged(SqlCursor) = .empty,
    cursor_budget: ?*Budget = null,
    retained_shared: ?*@import("budget.zig").Shared = null,
    retained_budget: ?*Budget = null,
    retired_arenas: ?*RetainedArena = null,
    cursor_epoch: u64 = 0,
    statement_timeout: ?u32 = null,
    application_name: commands.ApplicationName = .{},
    lake_visibility: commands.ReadRequirements = .{},
    read_receipt_budget: @import("antfly_local_sources").sql_read_requirements.WaitBudget = .{},
    namespace_setting: ?commands.SearchPath = null,
    catalog_settings: ?settings_catalog.OverlayState = null,
    request_namespace: ?[]const u8 = null,
    request_search_path: ?commands.SearchPath = null,
    transaction_namespace: ?commands.Namespace = null,
    timeout_transaction: ?struct { before: ?u32, committed: ?u32, namespace_before: ?commands.SearchPath, namespace_committed: ?commands.SearchPath, application_before: commands.ApplicationName, application_committed: commands.ApplicationName, lake_before: commands.ReadRequirements, lake_committed: commands.ReadRequirements } = null,
    savepoints: std.ArrayList(struct { name: []const u8, epoch: u64, timeout: ?u32, committed_timeout: ?u32, namespace: ?commands.SearchPath, committed_namespace: ?commands.SearchPath, application: commands.ApplicationName, committed_application: commands.ApplicationName, lake: commands.ReadRequirements, committed_lake: commands.ReadRequirements, catalog: ?settings_catalog.OverlayState.Savepoint = null }) = .empty,
    skip_until_sync: bool = false,
    backend_pid: i32 = 0,
    cancel_key: i32 = 0,
    cancel_requested: std.atomic.Value(bool) = .init(false),
    executing: std.atomic.Value(bool) = .init(false),
    // Hard transport deadline, read by the listener watchdog. A shorter
    // client SQL deadline must leave the socket available for its error reply.
    // Both use the owner clock; only the session task reads the SQL deadline.
    statement_deadline_ns: ?i64 = null,
    deadline_ns: std.atomic.Value(i64) = .init(0),
    deadline_changed: std.Io.Event = .unset,
    /// Listener-owned deadline service; borrowed until joined session cleanup.
    deadline_wakeup: ?*std.Io.Event = null,
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
        self.savepoints.deinit(self.retainedAlloc());
        if (self.catalog_settings) |*state| state.deinit();
        self.clearPrepared();
        // A stream close can leave a physical cleanup owner holding a lease.
        // Keep the connection allocator, cancellation flag, identity and all
        // parent budgets alive until those owners release their last use.
        self.drainRetainedArenas();
        if (self.cursor_budget) |budget| self.alloc.destroy(budget);
        if (self.identity) |identity| {
            self.source.vtable.disconnect(self.source.context, identity, self.session_id);
            identity.release(identity.context, self.alloc);
        }
        self.portals.deinit(self.retainedAlloc());
        self.sql_cursors.deinit(self.retainedAlloc());
        self.prepared.deinit(self.retainedAlloc());
        if (self.database) |value| self.alloc.free(value);
        if (self.namespace) |value| self.alloc.free(value);
        if (self.session_id) |value| self.alloc.free(value);
        if (self.retained_budget) |budget| {
            std.debug.assert(budget.used.load(.acquire) == 0);
            self.alloc.destroy(budget);
        }
    }

    fn retainedAllocator(self: *Session) !std.mem.Allocator {
        if (self.retained_budget == null) {
            const budget = try self.alloc.create(Budget);
            budget.* = .{ .child = self.alloc, .limit = self.limits.retained_bytes, .shared = self.retained_shared };
            self.retained_budget = budget;
        }
        return self.retainedAlloc();
    }

    fn newRetainedArena(self: *Session) !*std.heap.ArenaAllocator {
        self.reapRetainedArenas();
        return self.newArena(try self.retainedAllocator());
    }

    fn newRequestArena(self: *Session) !*std.heap.ArenaAllocator {
        self.reapRetainedArenas();
        // Simple queries own invocation state through the bounded connection
        // allocator; prepared-state exhaustion must not disable that path.
        return self.newArena(self.alloc);
    }

    fn newArena(self: *Session, child: std.mem.Allocator) !*std.heap.ArenaAllocator {
        const owner = try self.alloc.create(RetainedArena);
        owner.* = .{ .arena = std.heap.ArenaAllocator.init(child) };
        return &owner.arena;
    }

    fn destroyRetainedArena(self: *Session, arena: *std.heap.ArenaAllocator) void {
        const owner: *RetainedArena = @fieldParentPtr("arena", arena);
        const previous = owner.references.fetchSub(1, .acq_rel);
        std.debug.assert(previous != 0);
        if (previous == 1) {
            arena.deinit();
            self.alloc.destroy(owner);
        } else {
            owner.next = self.retired_arenas;
            self.retired_arenas = owner;
        }
        self.reapRetainedArenas();
    }

    fn reapRetainedArenas(self: *Session) void {
        var link = &self.retired_arenas;
        while (link.*) |owner| {
            if (owner.references.load(.acquire) == 0) {
                link.* = owner.next;
                owner.arena.deinit();
                self.alloc.destroy(owner);
            } else link = &owner.next;
        }
    }

    fn drainRetainedArenas(self: *Session) void {
        self.reapRetainedArenas();
        if (self.retired_arenas == null) return;
        const protection = self.io.swapCancelProtection(.blocked);
        defer _ = self.io.swapCancelProtection(protection);
        while (self.retired_arenas != null) {
            // A callback must not touch its owner after its last decrement,
            // including to wake a waiter in that owner's allocator chain.
            // Poll only during teardown with outstanding physical ownership.
            self.io.sleep(.fromMilliseconds(1), .awake) catch unreachable;
            self.reapRetainedArenas();
        }
    }

    fn snapshotStreamRequest(self: *Session, arena: *std.heap.ArenaAllocator, borrowed: backend.Request) !backend.Request {
        _ = self;
        const alloc = arena.allocator();
        var snapshot = borrowed;
        snapshot.statement = try alloc.dupe(u8, borrowed.statement);
        const parameters = try alloc.alloc(std.json.Value, borrowed.parameters.len);
        for (parameters, borrowed.parameters) |*out, value| out.* = try @import("cursor_spool.zig").cloneValue(alloc, value);
        snapshot.parameters = parameters;
        snapshot.parameter_types = try alloc.dupe(backend.Type, borrowed.parameter_types);
        snapshot.database = if (borrowed.database) |value| try alloc.dupe(u8, value) else null;
        snapshot.namespace = if (borrowed.namespace) |value| try alloc.dupe(u8, value) else null;
        snapshot.session_namespace = if (borrowed.session_namespace) |value| try alloc.dupe(u8, value) else null;
        snapshot.session_id = if (borrowed.session_id) |value| try alloc.dupe(u8, value) else null;
        snapshot.binding_guard = if (borrowed.binding_guard) |value| try alloc.dupe(u8, value) else null;
        const overlay = try alloc.alloc(settings_catalog.OverlayEntry, borrowed.setting_overlay.len);
        for (overlay, borrowed.setting_overlay) |*out, entry| {
            out.* = entry;
            if (entry.value == .string) out.value = .{ .string = try alloc.dupe(u8, entry.value.string) };
        }
        snapshot.setting_overlay = overlay;
        // search_path and setting identities are inline values. I/O, flags,
        // diagnostics and the identity/service chain remain joined by the
        // session's lease fence rather than copied into another owner.
        snapshot.retained_lifetime = RetainedArena.lifetime(arena);
        return snapshot;
    }

    fn retainedAlloc(self: *Session) std.mem.Allocator {
        return if (self.retained_budget) |budget| budget.allocator() else self.alloc;
    }

    pub fn idleTimeoutMs(self: *const Session) u32 {
        return if (self.status != .idle) @min(self.limits.idle_timeout_ms, self.limits.idle_transaction_timeout_ms) else self.limits.idle_timeout_ms;
    }

    pub fn setDeadline(self: *Session, ms: u32) void {
        const now: i64 = @intCast(std.Io.Clock.awake.now(self.io).nanoseconds);
        self.statement_deadline_ns = null;
        self.deadline_ns.store(now +| @as(i64, ms) * std.time.ns_per_ms, .release);
        self.deadline_changed.set(self.io);
        if (self.deadline_wakeup) |event| event.set(self.io);
    }

    fn setStatementDeadline(self: *Session) void {
        // Keep the configured hard cutoff independent of a client hint. This
        // does not extend backend authority: every request still gets the
        // original, earlier SQL deadline, including its admission wait.
        self.setDeadline(self.limits.statement_timeout_ms);
        const hard = self.deadline_ns.load(.acquire);
        const timeout = self.statement_timeout orelse self.limits.statement_timeout_ms;
        const effective = if (timeout == 0) self.limits.statement_timeout_ms else @min(timeout, self.limits.statement_timeout_ms);
        self.statement_deadline_ns = hard - @as(i64, self.limits.statement_timeout_ms - effective) * std.time.ns_per_ms;
    }

    pub fn run(self: *Session) !void {
        self.setDeadline(self.limits.startup_timeout_ms);
        const started = self.startup() catch |err| {
            if (err == error.Canceled) return err;
            if (self.diagnostic.code != null) try self.sendDiagnostic('E', self.diagnostic, null) else try self.sendError(sqlstate(err), @errorName(err));
            try self.writer.flush();
            return err;
        };
        if (!started) return;
        while (true) {
            self.setDeadline(self.idleTimeoutMs());
            const tag = self.reader.takeByte() catch |err| switch (err) {
                error.EndOfStream => return,
                else => return err,
            };
            const payload = try self.readPayload(4);
            defer self.freePayload(payload);
            if (tag == 'X') {
                if (payload.len != 0) return error.ProtocolViolation;
                return;
            }
            if (self.skip_until_sync and tag != 'S') continue;
            self.setStatementDeadline();
            self.diagnostic = .{};
            self.mutation_ack_pending = false;
            self.dispatch(tag, payload) catch |err| {
                if (err == error.Canceled) return err;
                self.skip_until_sync = tag != 'Q';
                if (self.diagnostic.transaction_status) |status| {
                    self.status = status;
                    if (status == .idle) {
                        if (self.catalog_settings) |*state| state.finish(false);
                        if (self.session_id) |id| self.alloc.free(id);
                        self.session_id = null;
                        if (self.timeout_transaction) |settings| {
                            self.statement_timeout = settings.before;
                            self.namespace_setting = settings.namespace_before;
                            self.application_name = settings.application_before;
                            self.lake_visibility = settings.lake_before;
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
            defer self.freePayload(payload);
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
                            formatting.validateEncoding(value) catch |err| {
                                self.diagnostic.set("22021", "unsupported startup parameter client_encoding; only UTF8 is supported", null, null);
                                return err;
                            };
                        } else if (std.mem.eql(u8, key, "application_name")) {
                            self.application_name = try commands.ApplicationName.init(value);
                        } else if (formatting.lookup(key)) |setting| {
                            setting.validate(value) catch |err| {
                                var buffer: [256]u8 = undefined;
                                const startup_message = std.fmt.bufPrint(&buffer, "unsupported value for startup parameter {s}; effective value is {s}", .{ setting.name(), setting.value() }) catch "unsupported startup parameter value";
                                self.diagnostic.set("0A000", startup_message, null, null);
                                return err;
                            };
                        } else {
                            var buffer: [256]u8 = undefined;
                            const startup_message = std.fmt.bufPrint(&buffer, "unsupported startup parameter: {s}", .{key}) catch "unsupported startup parameter";
                            self.diagnostic.set("0A000", startup_message, null, null);
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
                        self.freePayload(password_payload);
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
                    inline for (std.meta.tags(formatting.Setting)) |setting| try self.parameterStatus(setting.name(), setting.value());
                    try self.parameterStatus("integer_datetimes", "on");
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

    fn freePayload(self: *Session, payload: []u8) void {
        const alloc = if (self.frame_memory) |memory| memory.allocator(false) else self.alloc;
        alloc.free(payload);
    }

    fn wireAllocator(self: *Session) std.mem.Allocator {
        return if (self.frame_memory) |memory| memory.allocator(true) else self.alloc;
    }

    fn protectedControl(self: *Session, statement: []const u8) bool {
        return self.control_allocator != null and if (self.is_protected_control) |classify| classify(statement) else false;
    }

    fn readPayload(self: *Session, minimum: u32) ![]u8 {
        const len = try self.reader.takeInt(u32, .big);
        if (len < minimum or len > self.limits.frame_bytes) return error.ProtocolViolation;
        const alloc = if (self.frame_memory) |memory| memory.allocator(len - 4 <= 8192) else self.alloc;
        return self.reader.readAlloc(alloc, len - 4);
    }

    fn effectiveNamespace(self: *const Session) []const u8 {
        return if (self.namespace_setting) |*value| value.first() else self.namespace orelse "public";
    }

    fn effectiveSearchPath(self: *const Session) ?commands.SearchPath {
        // An explicit Parse/DECLARE scope pins even the absence of an
        // override. Null must not fall through to a later session SET.
        return if (self.request_namespace != null) self.request_search_path else self.namespace_setting;
    }

    fn request(self: *Session, statement: []const u8, parameters: []const std.json.Value, types: []const backend.Type) backend.Request {
        return .{
            .statement = statement,
            .completion_drain = self.completion_drain,
            .scratch_allocator = self.alloc,
            .parameters = parameters,
            .parameter_types = types,
            .database = self.database,
            .namespace = self.request_namespace orelse self.effectiveNamespace(),
            .search_path = self.effectiveSearchPath(),
            .session_namespace = if (self.transaction_namespace) |*value| value.slice() else null,
            .session_id = self.session_id,
            .lake_visibility = if (self.lake_visibility.visibility == .accepted) .accepted else .committed,
            .read_requirements = self.lake_visibility,
            .read_receipt_budget = &self.read_receipt_budget,
            .setting_overlay = if (self.catalog_settings) |*state| state.values() else &.{},
            .limit = self.limits.result_rows,
            .io = self.io,
            .deadline = .{ .clock = .awake, .raw = .{ .nanoseconds = self.statement_deadline_ns orelse self.deadline_ns.load(.acquire) } },
            .cancel_requested = &self.cancel_requested,
            .diagnostics = &self.diagnostic,
        };
    }

    fn describe(self: *Session, alloc: std.mem.Allocator, statement: []const u8, types: []const backend.Type) !backend.Description {
        return self.describeTyped(alloc, statement, types, &.{});
    }

    fn describeTyped(self: *Session, alloc: std.mem.Allocator, statement: []const u8, types: []const backend.Type, descriptors: []const backend.Parameter) !backend.Description {
        if (try commands.settingCommand(alloc, statement)) |setting| {
            if (types.len != 0) return error.InvalidParameter;
            return .{ .columns = switch (setting) {
                .search_path => |value| if (value == .show) &.{.{ .name = "search_path", .type = .string }} else &.{},
                .statement_timeout => |value| if (value == .show) &.{.{ .name = "statement_timeout", .type = .string }} else &.{},
                .lake_visibility => |value| if (value == .show) &.{.{ .name = "antfly.lake_visibility", .type = .string }} else &.{},
                .read_visibility => |value| if (value == .show) &.{.{ .name = "antfly.read_visibility", .type = .string }} else &.{},
                .read_requirements => |value| if (value == .show) &.{.{ .name = "antfly.read_requirements", .type = .string }} else &.{},
                .application_name => |value| if (value == .show) &.{.{ .name = "application_name", .type = .string }} else &.{},
                .formatting => |value| if (value.action == .show) try alloc.dupe(backend.Column, &.{.{ .name = value.kind.name(), .type = .string }}) else &.{},
                .client_encoding => |value| if (value == .show) &.{.{ .name = "client_encoding", .type = .string }} else &.{},
                .catalog => |value| if (value == .show) &.{.{ .name = value.show, .type = .string }} else &.{},
                .reset_all => &.{},
                .discard_all => &.{},
            } };
        }
        self.cancel_requested.store(false, .release);
        self.executing.store(true, .release);
        defer self.executing.store(false, .release);
        var req = self.request(statement, &.{}, types);
        req.parameter_descriptors = descriptors;
        try req.check();
        const result = try self.source.vtable.describe(self.source.context, alloc, self.identity orelse return error.AuthenticationFailed, req);
        if (result.columns.len > self.limits.columns or result.parameter_types.len > self.limits.parameters) return error.ProgramLimitExceeded;
        if (result.parameter_descriptors.len != 0) {
            if (result.parameter_descriptors.len != result.parameter_types.len) return error.InvalidParameter;
            for (result.parameter_descriptors, result.parameter_types) |descriptor, kind| {
                const inferred = try values.parameterFromOid(try values.parameterOid(descriptor));
                if (inferred.kind == null) {
                    if (kind != .unknown) return error.InvalidParameter;
                } else if (!std.mem.eql(u8, @tagName(inferred.kind.?), @tagName(kind))) return error.InvalidParameter;
            }
        }
        return result;
    }

    /// Recompile against the current catalog before an unstarted execution.
    /// Preserve the client's fixed parameter/result contract; retain fresh
    /// schema and settings guards so concurrent changes still fence admission.
    /// Never call this for a suspended portal or an already admitted mutation.
    fn cachedPlanChanged(self: *Session) anyerror {
        self.diagnostic.set("0A000", "cached plan must not change result type", null, null);
        return error.CachedPlanResultTypeChanged;
    }

    fn rebindCached(self: *Session, alloc: std.mem.Allocator, statement: []const u8, original: backend.Description) !backend.Description {
        const current = try self.describeTyped(alloc, statement, original.parameter_types, original.parameter_descriptors);
        if (!cachedColumnsEqual(original.columns, current.columns) or
            original.parameter_types.len != current.parameter_types.len)
            return self.cachedPlanChanged();
        for (original.parameter_types, current.parameter_types, 0..) |before, after, i| {
            const before_oid = if (original.parameter_descriptors.len == original.parameter_types.len)
                try values.parameterOid(original.parameter_descriptors[i])
            else
                try values.oid(before);
            const after_oid = if (current.parameter_descriptors.len == current.parameter_types.len)
                try values.parameterOid(current.parameter_descriptors[i])
            else
                try values.oid(after);
            if (before_oid != after_oid) return self.cachedPlanChanged();
        }
        return current;
    }

    fn execute(self: *Session, alloc: std.mem.Allocator, statement: []const u8, parameters: []const std.json.Value, types: []const backend.Type, binding_guard: ?[]const u8, setting_epoch: ?u64) !backend.Result {
        return self.executeTyped(alloc, statement, parameters, types, &.{}, binding_guard, setting_epoch);
    }

    fn executeTyped(self: *Session, alloc: std.mem.Allocator, statement: []const u8, parameters: []const std.json.Value, types: []const backend.Type, descriptors: []const backend.Parameter, binding_guard: ?[]const u8, setting_epoch: ?u64) !backend.Result {
        return self.executeCells(alloc, statement, parameters, types, descriptors, null, null, binding_guard, setting_epoch);
    }

    fn executeCells(self: *Session, alloc: std.mem.Allocator, statement: []const u8, parameters: []const std.json.Value, types: []const backend.Type, descriptors: []const backend.Parameter, native_cells: ?[]const @import("antfly_local_sources").sql_scalar.Datum, parameter_owner: ?*@import("antfly_local_sources").sql_parameter_frame.Ingress, binding_guard: ?[]const u8, setting_epoch: ?u64) !backend.Result {
        if (try commands.settingCommand(alloc, statement)) |setting| {
            if (parameters.len != 0 or types.len != 0) return error.InvalidParameter;
            return switch (setting) {
                .search_path => |value| self.executeSearchPathSetting(alloc, statement, value),
                .statement_timeout => |value| self.executeTimeoutSetting(alloc, value),
                .lake_visibility => |value| self.executeLakeVisibilitySetting(alloc, value, "antfly.lake_visibility"),
                .read_visibility => |value| self.executeLakeVisibilitySetting(alloc, value, "antfly.read_visibility"),
                .read_requirements => |value| self.executeReadRequirementsSetting(alloc, value),
                .application_name => |value| self.executeApplicationNameSetting(alloc, value),
                .formatting => |value| self.executeFormattingSetting(alloc, value),
                .client_encoding => |value| self.executeEncodingSetting(alloc, value),
                .catalog => |value| self.executeCatalogSetting(alloc, statement, value),
                .reset_all => self.executeResetAll(),
                .discard_all => self.executeDiscardAll(),
            };
        }
        self.cancel_requested.store(false, .release);
        self.executing.store(true, .release);
        defer self.executing.store(false, .release);
        var req = self.request(statement, parameters, types);
        req.parameter_descriptors = descriptors;
        req.parameter_cells = native_cells;
        req.parameter_owner = if (parameter_owner) |owner| owner.lease() else null;
        req.binding_guard = binding_guard;
        req.setting_epoch = setting_epoch;
        try req.check();
        const control = try commands.control(alloc, statement);
        var savepoint_name: ?[]const u8 = null;
        defer if (savepoint_name) |name| self.retainedAlloc().free(name);
        if (control) |value| if (value == .savepoint) {
            if (self.savepoints.items.len >= 64) return error.ProgramLimitExceeded;
            savepoint_name = try (try self.retainedAllocator()).dupe(u8, value.savepoint);
            try self.savepoints.ensureUnusedCapacity(try self.retainedAllocator(), 1);
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
        if (result.rowCount() > self.limits.result_rows or result.columns.len > self.limits.columns) return error.ProgramLimitExceeded;
        if (result.cells) |cells| if (cells.width != result.columns.len or result.rows.len != 0 or result.sql_nulls != null) return error.InvalidResult;
        for (result.rows) |row| if (row.len != result.columns.len) return error.InvalidResult;
        const session = if (result.session_id) |id| try self.alloc.dupe(u8, id) else null;
        if (self.session_id) |old| self.alloc.free(old);
        self.session_id = session;
        self.status = result.transaction_status;
        if (previous_status == .idle and self.status == .in_transaction) {
            try (try self.settingsState()).begin();
            self.timeout_transaction = .{ .before = self.statement_timeout, .committed = self.statement_timeout, .namespace_before = self.namespace_setting, .namespace_committed = self.namespace_setting, .application_before = self.application_name, .application_committed = self.application_name, .lake_before = self.lake_visibility, .lake_committed = self.lake_visibility };
            self.transaction_namespace = try commands.Namespace.init(req.namespace orelse "public");
        }
        if (control) |value| switch (value) {
            .savepoint => {
                self.savepoints.appendAssumeCapacity(.{ .name = savepoint_name.?, .epoch = self.cursor_epoch, .timeout = self.statement_timeout, .committed_timeout = if (self.timeout_transaction) |settings| settings.committed else self.statement_timeout, .namespace = self.namespace_setting, .committed_namespace = if (self.timeout_transaction) |settings| settings.namespace_committed else self.namespace_setting, .application = self.application_name, .committed_application = if (self.timeout_transaction) |settings| settings.application_committed else self.application_name, .lake = self.lake_visibility, .committed_lake = if (self.timeout_transaction) |settings| settings.lake_committed else self.lake_visibility, .catalog = if (self.catalog_settings) |*state| try state.savepoint() else null });
                savepoint_name = null;
            },
            .release, .rollback_to => |name| {
                var index = self.savepoints.items.len;
                while (index != 0) {
                    index -= 1;
                    if (std.mem.eql(u8, name, self.savepoints.items[index].name)) {
                        if (value == .rollback_to) {
                            if (self.catalog_settings) |*state| if (self.savepoints.items[index].catalog) |snapshot| try state.rollbackTo(snapshot);
                            self.discardCursorsAfter(self.savepoints.items[index].epoch);
                            self.statement_timeout = self.savepoints.items[index].timeout;
                            self.namespace_setting = self.savepoints.items[index].namespace;
                            self.application_name = self.savepoints.items[index].application;
                            self.lake_visibility = self.savepoints.items[index].lake;
                            if (self.timeout_transaction) |*settings| {
                                settings.committed = self.savepoints.items[index].committed_timeout;
                                settings.namespace_committed = self.savepoints.items[index].committed_namespace;
                                settings.application_committed = self.savepoints.items[index].committed_application;
                                settings.lake_committed = self.savepoints.items[index].committed_lake;
                            }
                        }
                        const keep = index + @as(usize, if (value == .rollback_to) 1 else 0);
                        for (self.savepoints.items[keep..]) |*point| {
                            self.retainedAlloc().free(point.name);
                            if (point.catalog) |*snapshot| snapshot.deinit(self.retainedAlloc());
                        }
                        self.savepoints.shrinkRetainingCapacity(keep);
                        break;
                    }
                }
            },
        };
        if (self.status == .idle) {
            if (self.catalog_settings) |*state| state.finish(std.ascii.eqlIgnoreCase(result.command_tag, "COMMIT"));
            self.clearCursorSavepoints();
        }
        if (self.status == .idle) if (self.timeout_transaction) |settings| {
            self.statement_timeout = if (std.ascii.eqlIgnoreCase(result.command_tag, "COMMIT")) settings.committed else settings.before;
            self.namespace_setting = if (std.ascii.eqlIgnoreCase(result.command_tag, "COMMIT")) settings.namespace_committed else settings.namespace_before;
            self.application_name = if (std.ascii.eqlIgnoreCase(result.command_tag, "COMMIT")) settings.application_committed else settings.application_before;
            self.lake_visibility = if (std.ascii.eqlIgnoreCase(result.command_tag, "COMMIT")) settings.lake_committed else settings.lake_before;
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
            row[0] = .{ .string = if (self.namespace_setting) |*path| try path.display(alloc) else try alloc.dupe(u8, self.namespace orelse "public") };
            const rows = try alloc.alloc([]const std.json.Value, 1);
            rows[0] = row;
            result.rows = rows;
            return result;
        }
        const update = if (setting == .reset) (commands.SearchPathSetting{ .set = .{ .local = false, .path = null } }).set else setting.set;
        if (update.local and self.status != .in_transaction) return error.NoActiveSqlTransaction;
        const validate = self.source.vtable.validate_namespace orelse return error.UnsupportedSqlExecution;
        var request_value = self.request(statement, &.{}, &.{});
        request_value.namespace = if (update.path) |*value| value.first() else self.namespace orelse "public";
        request_value.search_path = update.path;
        self.cancel_requested.store(false, .release);
        self.executing.store(true, .release);
        defer self.executing.store(false, .release);
        try request_value.check();
        if (update.path) |path| {
            for (path.entries[0..path.len]) |*entry| {
                request_value.namespace = entry.slice();
                try validate(self.source.context, alloc, self.identity orelse return error.AuthenticationFailed, request_value);
            }
        } else try validate(self.source.context, alloc, self.identity orelse return error.AuthenticationFailed, request_value);
        self.namespace_setting = update.path;
        if (!update.local) if (self.timeout_transaction) |*settings| {
            settings.namespace_committed = update.path;
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

    fn executeReadRequirementsSetting(self: *Session, alloc: std.mem.Allocator, setting: commands.ReadRequirementsSetting) !backend.Result {
        if (self.status == .failed) return error.InFailedSqlTransaction;
        var result: backend.Result = .{ .command_tag = if (setting == .show) "SHOW" else if (setting == .reset) "RESET" else "SET", .transaction_status = self.status, .session_id = self.session_id };
        if (setting == .show) {
            result.columns = &.{.{ .name = "antfly.read_requirements", .type = .string }};
            const row = try alloc.alloc(std.json.Value, 1);
            row[0] = .{ .string = try std.json.Stringify.valueAlloc(alloc, self.lake_visibility, .{ .emit_null_optional_fields = false }) };
            const rows = try alloc.alloc([]const std.json.Value, 1);
            rows[0] = row;
            result.rows = rows;
            return result;
        }
        const update = if (setting == .reset) (commands.ReadRequirementsSetting{ .set = .{ .local = false, .value = .{} } }).set else setting.set;
        if (update.local and self.status != .in_transaction) return error.NoActiveSqlTransaction;
        try update.value.validate(.sql);
        self.lake_visibility = update.value;
        if (!update.local) if (self.timeout_transaction) |*settings| {
            settings.lake_committed = update.value;
        };
        return result;
    }

    fn executeLakeVisibilitySetting(self: *Session, alloc: std.mem.Allocator, setting: commands.LakeVisibilitySetting, name: []const u8) !backend.Result {
        if (self.status == .failed) return error.InFailedSqlTransaction;
        var result: backend.Result = .{ .command_tag = if (setting == .show) "SHOW" else if (setting == .reset) "RESET" else "SET", .transaction_status = self.status, .session_id = self.session_id };
        if (setting == .show) {
            const columns = try alloc.alloc(backend.Column, 1);
            columns[0] = .{ .name = name, .type = .string };
            result.columns = columns;
            const row = try alloc.alloc(std.json.Value, 1);
            row[0] = .{ .string = try alloc.dupe(u8, @tagName(self.lake_visibility.visibility orelse .committed)) };
            const rows = try alloc.alloc([]const std.json.Value, 1);
            rows[0] = row;
            result.rows = rows;
            return result;
        }
        const update = if (setting == .reset) (commands.LakeVisibilitySetting{ .set = .{ .local = false, .value = .committed } }).set else setting.set;
        if (update.local and self.status != .in_transaction) return error.NoActiveSqlTransaction;
        self.lake_visibility.visibility = if (update.value == .accepted) .accepted else .committed;
        if (!update.local) if (self.timeout_transaction) |*settings| {
            settings.lake_committed.visibility = self.lake_visibility.visibility;
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

    fn executeFormattingSetting(self: *Session, alloc: std.mem.Allocator, setting: commands.FormattingSetting) !backend.Result {
        if (self.status == .failed) return error.InFailedSqlTransaction;
        if (setting.local and self.status != .in_transaction) return error.NoActiveSqlTransaction;
        var result: backend.Result = .{ .command_tag = switch (setting.action) {
            .show => "SHOW",
            .set => "SET",
            .reset => "RESET",
        }, .transaction_status = self.status, .session_id = self.session_id };
        if (setting.action == .show) {
            result.columns = try alloc.dupe(backend.Column, &.{.{ .name = setting.kind.name(), .type = .string }});
            const row = try alloc.dupe(std.json.Value, &.{.{ .string = setting.kind.value() }});
            result.rows = try alloc.dupe([]const std.json.Value, &.{row});
        }
        return result;
    }

    fn settingsState(self: *Session) !*settings_catalog.OverlayState {
        if (self.catalog_settings == null) self.catalog_settings = settings_catalog.OverlayState.init(try self.retainedAllocator());
        return &self.catalog_settings.?;
    }

    fn executeCatalogSetting(self: *Session, alloc: std.mem.Allocator, statement: []const u8, setting: commands.CatalogSetting) !backend.Result {
        if (self.status == .failed) return error.InFailedSqlTransaction;
        const load = self.source.vtable.load_settings orelse return error.UnsupportedSqlExecution;
        const identity = self.identity orelse return error.AuthenticationFailed;
        const request_ = self.request(statement, &.{}, &.{});
        try request_.check();
        const raw = try load(self.source.context, alloc, identity, request_);
        const snapshot = struct {
            pub fn read(ptr: *anyopaque, _: std.mem.Allocator, _: settings_catalog.Scope) !settings_catalog.RawSnapshot {
                return @as(*settings_catalog.RawSnapshot, @ptrCast(@alignCast(ptr))).*;
            }
        };
        var captured = raw;
        const owner: settings_catalog.Owner = .{ .ptr = &captured, .load = snapshot.read };
        const state = try self.settingsState();
        switch (setting) {
            .set => |value| {
                try state.set(owner, raw.scope, value.name, value.value, value.local);
                return .{ .command_tag = "SET", .transaction_status = self.status, .session_id = self.session_id };
            },
            .reset => |name| {
                try state.reset(owner, raw.scope, name);
                return .{ .command_tag = "RESET", .transaction_status = self.status, .session_id = self.session_id };
            },
            .reset_local => |name| {
                try state.resetLocal(owner, raw.scope, name);
                return .{ .command_tag = "SET", .transaction_status = self.status, .session_id = self.session_id };
            },
            .show => |name| {
                var view = try settings_catalog.View.capture(alloc, owner, raw.scope, state.values());
                defer view.deinit();
                const item = try view.resolve(name);
                const rendered = switch (item.value) {
                    .string => |value| try alloc.dupe(u8, value),
                    .integer => |value| try std.fmt.allocPrint(alloc, "{d}", .{value}),
                    .boolean => |value| try alloc.dupe(u8, if (value) "on" else "off"),
                };
                const row = try alloc.alloc(std.json.Value, 1);
                row[0] = .{ .string = rendered };
                const rows = try alloc.alloc([]const std.json.Value, 1);
                rows[0] = row;
                const columns = try alloc.alloc(backend.Column, 1);
                columns[0] = .{ .name = name, .type = .string };
                return .{ .command_tag = "SHOW", .transaction_status = self.status, .session_id = self.session_id, .columns = columns, .rows = rows };
            },
        }
    }

    fn executeResetAll(self: *Session) !backend.Result {
        if (self.status == .failed) return error.InFailedSqlTransaction;
        self.statement_timeout = null;
        self.namespace_setting = null;
        self.application_name = .{};
        self.lake_visibility = .{};
        if (self.catalog_settings) |*state| state.resetAll();
        // RESET ALL is a session-level change even inside a transaction.
        // Savepoint snapshots can still roll it back locally; a committed
        // transaction publishes the reset values.
        if (self.timeout_transaction) |*settings| {
            settings.committed = null;
            settings.namespace_committed = null;
            settings.application_committed = .{};
            settings.lake_committed = .{};
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
                self.read_receipt_budget = .{};
                const statement = try cursor.string();
                try cursor.finish();
                self.removePortal("");
                self.removePrepared("");
                if (try commands.isEmptyQuery(statement)) {
                    try self.message('I', "");
                } else {
                    const protected = self.protectedControl(statement);
                    var arena = std.heap.ArenaAllocator.init(if (protected) self.control_allocator.? else self.alloc);
                    defer arena.deinit();
                    if (try self.sessionCommand(arena.allocator(), statement)) {
                        if (self.status == .idle) {
                            self.clearPortals();
                            self.finishSqlCursors(false);
                        }
                        try self.ready();
                        return;
                    }
                    if (!protected and try self.simpleStream(statement)) {
                        if (self.status == .idle) self.clearPortals();
                        try self.ready();
                        return;
                    }
                    var result = try self.execute(arena.allocator(), statement, &.{}, &.{}, null, null);
                    defer result.deinit();
                    if (result.columns.len > 0) try self.rowDescription(result.columns, &.{});
                    if (result.sql_nulls) |flags| if (flags.len != result.rows.len) return error.InvalidResult;
                    try self.dataResult(result, &.{}, statement);
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
                const arena = try self.newRetainedArena();
                var transferred = false;
                errdefer if (!transferred) self.destroyRetainedArena(arena);
                const a = arena.allocator();
                const declared = try a.alloc(u32, count);
                const types = try a.alloc(backend.Type, count);
                const descriptors = try a.alloc(backend.Parameter, count);
                for (declared, types, descriptors) |*oid, *kind, *descriptor| {
                    oid.* = try cursor.int(u32);
                    kind.* = try values.fromOid(oid.*);
                    descriptor.* = try values.parameterFromOid(oid.*);
                }
                try cursor.finish();
                const description = try self.describeTyped(a, statement, types, descriptors);
                if (count > description.parameter_types.len) return error.InvalidParameter;
                const oids = try a.alloc(u32, description.parameter_types.len);
                for (oids, description.parameter_types, 0..) |*oid, kind, index| {
                    oid.* = if (index < declared.len and declared[index] != 0) declared[index] else if (description.parameter_descriptors.len == oids.len) try values.parameterOid(description.parameter_descriptors[index]) else try values.oid(kind);
                }
                const owned_statement = try a.dupe(u8, statement);
                const owned_name = try (try self.retainedAllocator()).dupe(u8, name);
                errdefer if (!transferred) self.retainedAlloc().free(owned_name);
                self.removePrepared(name);
                if (name.len == 0) self.removePortal("");
                const namespace = try a.dupe(u8, self.request_namespace orelse self.effectiveNamespace());
                try self.prepared.put(try self.retainedAllocator(), owned_name, .{ .arena = arena, .statement = owned_statement, .parameter_oids = oids, .description = description, .namespace = namespace, .search_path = self.effectiveSearchPath() });
                transferred = true;
                try self.message('1', "");
            },
            'B' => {
                const name = try cursor.string();
                const statement_name = try cursor.string();
                const statement = self.prepared.get(statement_name) orelse return error.InvalidStatementName;
                if (name.len != 0 and self.portals.contains(name)) return error.DuplicatePortal;
                if (!self.portals.contains(name) and self.portals.count() >= self.limits.portals) return error.ProgramLimitExceeded;
                const arena = try self.newRetainedArena();
                var transferred = false;
                errdefer if (!transferred) self.destroyRetainedArena(arena);
                const a = arena.allocator();
                const previous_namespace = self.request_namespace;
                const previous_search_path = self.request_search_path;
                self.request_namespace = statement.namespace;
                defer self.request_namespace = previous_namespace;
                self.request_search_path = statement.search_path;
                defer self.request_search_path = previous_search_path;
                const current = try self.rebindCached(a, statement.statement, statement.description);
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
                const frames = @import("antfly_local_sources").sql_parameter_frame;
                const needs_owner = self.source.vtable.native_parameters and for (statement.parameter_oids) |type_oid| {
                    const descriptor = try values.parameterFromOid(type_oid);
                    if (descriptor.kind == .array or descriptor.kind == .json) break true;
                } else false;
                // Ordinary scalars need no extra region/refcount. Compact
                // arrays and managed JSON containers require a stable owner.
                const parameter_owner = if (needs_owner) try frames.Ingress.create(self.alloc, (frames.Limits{}).bytes) else null;
                errdefer if (!transferred) if (parameter_owner) |owner| owner.release();
                const parameter_alloc = if (parameter_owner) |owner| owner.allocator() else a;
                const native_cells = if (self.source.vtable.native_parameters) try parameter_alloc.alloc(@import("antfly_local_sources").sql_scalar.Datum, count) else null;
                for (parameters, types, statement.parameter_oids, 0..) |*parameter, *kind, oid, index| {
                    kind.* = try values.fromOid(oid);
                    const len = try cursor.int(i32);
                    if (len == -1) {
                        parameter.* = .null;
                        if (native_cells) |cells| cells[index] = .{};
                        continue;
                    }
                    if (len < 0) return error.ProtocolViolation;
                    const parameter_bytes = try cursor.take(@intCast(len));
                    if (native_cells) |cells| {
                        cells[index] = values.decodeNative(parameter_alloc, oid, formatAt(formats, index), parameter_bytes) catch |err| {
                            if (parameter_owner) |owner| if (owner.budget.isExhausted()) return error.ProgramLimitExceeded;
                            return err;
                        };
                        parameter.* = .null;
                    } else parameter.* = try values.decode(a, oid, formatAt(formats, index), parameter_bytes);
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
                    .columns = current.columns,
                    .parameter_types = types,
                    .parameter_descriptors = current.parameter_descriptors,
                    .binding_guard = current.binding_guard,
                    .setting_epoch = current.setting_epoch,
                };
                const owned_name = try (try self.retainedAllocator()).dupe(u8, name);
                errdefer if (!transferred) self.retainedAlloc().free(owned_name);
                self.removePortal(name);
                const namespace = try a.dupe(u8, statement.namespace);
                const portal = try (try self.retainedAllocator()).create(Portal);
                errdefer if (!transferred) self.retainedAlloc().destroy(portal);
                portal.* = .{ .arena = arena, .statement = text, .parameters = parameters, .parameter_cells = native_cells, .parameter_owner = parameter_owner, .types = types, .formats = result_formats, .description = description, .namespace = namespace, .search_path = statement.search_path };
                try self.portals.put(try self.retainedAllocator(), owned_name, portal);
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
                        var arena = std.heap.ArenaAllocator.init(self.alloc);
                        defer arena.deinit();
                        const previous_namespace = self.request_namespace;
                        const previous_search_path = self.request_search_path;
                        self.request_namespace = prepared.namespace;
                        defer self.request_namespace = previous_namespace;
                        self.request_search_path = prepared.search_path;
                        defer self.request_search_path = previous_search_path;
                        const description = try self.rebindCached(arena.allocator(), prepared.statement, prepared.description);
                        var bytes = std.Io.Writer.Allocating.init(self.alloc);
                        defer bytes.deinit();
                        try bytes.writer.writeInt(u16, @intCast(prepared.parameter_oids.len), .big);
                        for (prepared.parameter_oids) |oid| try bytes.writer.writeInt(u32, oid, .big);
                        try self.message('t', bytes.written());
                        try self.rowDescription(description.columns, &.{});
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
                const portal = self.portals.get(name) orelse return error.InvalidPortalName;
                const previous_namespace = self.request_namespace;
                const previous_search_path = self.request_search_path;
                self.request_namespace = portal.namespace;
                defer self.request_namespace = previous_namespace;
                self.request_search_path = portal.search_path;
                defer self.request_search_path = previous_search_path;
                if (portal.failed) return error.PortalExecutionFailed;
                portal.failed = true;
                errdefer if (portal.stream) |stream| {
                    stream.close(stream.context);
                    portal.stream = null;
                };
                if (!portal.stream_opened) {
                    self.read_receipt_budget = .{};
                    portal.description = try self.rebindCached(portal.arena.allocator(), portal.statement, portal.description);
                    portal.stream_opened = true;
                    if (self.source.vtable.open_stream) |open| if (!self.protectedControl(portal.statement) and try commands.settingCommand(portal.arena.allocator(), portal.statement) == null) {
                        self.cancel_requested.store(false, .release);
                        self.executing.store(true, .release);
                        defer self.executing.store(false, .release);
                        var req = self.request(portal.statement, portal.parameters, portal.types);
                        req.parameter_cells = portal.parameter_cells;
                        req.parameter_owner = if (portal.parameter_owner) |owner| owner.lease() else null;
                        req.parameter_descriptors = portal.description.parameter_descriptors;
                        req.binding_guard = portal.description.binding_guard;
                        req.setting_epoch = portal.description.setting_epoch;
                        req = try self.snapshotStreamRequest(portal.arena, req);
                        portal.stream = try open(self.source.context, self.alloc, self.identity orelse return error.AuthenticationFailed, req);
                    };
                }
                if (portal.stream != null or portal.stream_complete) {
                    try self.executeStream(portal, requested);
                    portal.failed = false;
                    return;
                }
                if (portal.result == null) {
                    portal.result = try self.executeCells(if (self.protectedControl(portal.statement)) self.control_allocator.? else portal.arena.allocator(), portal.statement, portal.parameters, portal.types, portal.description.parameter_descriptors, portal.parameter_cells, portal.parameter_owner, portal.description.binding_guard, portal.description.setting_epoch);
                    const result = portal.result.?;
                    if (!columnsEqual(portal.description.columns, result.columns)) return error.ResultShapeChanged;
                }
                const result = portal.result.?;
                if (result.mutation_outcome != null) {
                    self.mutation_ack_pending = true;
                    self.diagnostic.transaction_id = result.transaction_id;
                }
                const count = if (requested == 0) result.rowCount() - portal.offset else @min(@as(usize, @intCast(requested)), result.rowCount() - portal.offset);
                if (result.sql_nulls) |flags| if (flags.len != result.rows.len) return error.InvalidResult;
                try self.dataResult(try result.view(portal.offset, count), portal.formats, portal.statement);
                portal.offset += count;
                if (portal.offset < result.rowCount()) try self.message('s', "") else try self.complete(result);
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
                const arena = try self.newRetainedArena();
                errdefer self.destroyRetainedArena(arena);
                const a = arena.allocator();
                const description = try self.describeTyped(a, prepare.statement, prepare.types, prepare.descriptors);
                if (prepare.types.len > description.parameter_types.len) return error.InvalidParameter;
                const oids = try a.alloc(u32, description.parameter_types.len);
                for (oids, description.parameter_types, 0..) |*oid, kind, index| oid.* = if (description.parameter_descriptors.len == oids.len) try values.parameterOid(description.parameter_descriptors[index]) else try values.oid(kind);
                const sql = try a.dupe(u8, prepare.statement);
                const name = try (try self.retainedAllocator()).dupe(u8, prepare.name);
                errdefer self.retainedAlloc().free(name);
                const namespace = try a.dupe(u8, self.request_namespace orelse self.effectiveNamespace());
                try self.prepared.put(try self.retainedAllocator(), name, .{ .arena = arena, .statement = sql, .parameter_oids = oids, .description = description, .namespace = namespace, .search_path = self.effectiveSearchPath() });
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
                        self.retainedAlloc().free(entry.key_ptr.*);
                        self.destroyRetainedArena(entry.value_ptr.arena);
                    }
                    self.prepared.clearRetainingCapacity();
                }
            },
            .execute => |execute_command| {
                const prepared = self.prepared.get(execute_command.name) orelse return error.InvalidStatementName;
                const previous_namespace = self.request_namespace;
                const previous_search_path = self.request_search_path;
                self.request_namespace = prepared.namespace;
                defer self.request_namespace = previous_namespace;
                self.request_search_path = prepared.search_path;
                defer self.request_search_path = previous_search_path;
                const description = try self.rebindCached(alloc, prepared.statement, prepared.description);
                if (execute_command.expressions.len != description.parameter_types.len) return error.InvalidParameter;
                self.cancel_requested.store(false, .release);
                self.executing.store(true, .release);
                defer self.executing.store(false, .release);
                var req = self.request(prepared.statement, &.{}, description.parameter_types);
                req.parameter_descriptors = description.parameter_descriptors;
                try req.check();
                var native_cells: ?[]const @import("antfly_local_sources").sql_scalar.Datum = null;
                const parameters = if (self.source.vtable.evaluate_parameter_cells) |evaluate| native: {
                    native_cells = try evaluate(self.source.context, alloc, self.identity orelse return error.AuthenticationFailed, req, execute_command.expressions);
                    const placeholders = try alloc.alloc(std.json.Value, native_cells.?.len);
                    @memset(placeholders, .null);
                    break :native placeholders;
                } else try (self.source.vtable.evaluate_parameters orelse return error.UnsupportedSqlExecution)(self.source.context, alloc, self.identity orelse return error.AuthenticationFailed, req, execute_command.expressions);
                if (parameters.len != description.parameter_types.len) return error.InvalidParameter;
                if (try self.simpleStreamCells(prepared.statement, parameters, description.parameter_types, description.parameter_descriptors, native_cells, description.binding_guard, description.setting_epoch)) return true;
                var result = try self.executeCells(alloc, prepared.statement, parameters, description.parameter_types, description.parameter_descriptors, native_cells, null, description.binding_guard, description.setting_epoch);
                defer result.deinit();
                if (result.columns.len > 0) try self.rowDescription(result.columns, &.{});
                if (result.sql_nulls) |flags| if (flags.len != result.rows.len) return error.InvalidResult;
                try self.dataResult(result, &.{}, prepared.statement);
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
        const arena = try self.newRetainedArena();
        var arena_moved = false;
        defer if (!arena_moved) self.destroyRetainedArena(arena);
        const description = try self.describe(arena.allocator(), statement, &.{});
        if (description.parameter_types.len != 0) return error.InvalidParameter;
        self.cancel_requested.store(false, .release);
        self.executing.store(true, .release);
        defer self.executing.store(false, .release);
        var req = self.request(statement, &.{}, &.{});
        req.binding_guard = description.binding_guard;
        req.setting_epoch = description.setting_epoch;
        req = try self.snapshotStreamRequest(arena, req);
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
                budget.* = .{ .child = try self.retainedAllocator(), .limit = @min(self.limits.cursor_bytes, self.limits.retained_bytes) };
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
                result.transaction_status != self.status or !same_session or result.rowCount() > self.limits.result_rows) return error.InvalidResult;
            spool.?.append(result, self.limits.cursor_rows) catch |err| return if (err == error.OutOfMemory) error.ProgramLimitExceeded else err;
        }
        const owned_statement = req.statement;
        const session_id = try arena.allocator().dupe(u8, self.session_id orelse "");
        const key = try (try self.retainedAllocator()).dupe(u8, name);
        var key_moved = false;
        defer if (!key_moved) self.retainedAlloc().free(key);
        const database = if (self.database) |value| try arena.allocator().dupe(u8, value) else null;
        const namespace = try arena.allocator().dupe(u8, self.request_namespace orelse self.effectiveNamespace());
        self.cursor_epoch = std.math.add(u64, self.cursor_epoch, 1) catch return error.ProgramLimitExceeded;
        try self.sql_cursors.put(try self.retainedAllocator(), key, .{ .arena = arena, .statement = owned_statement, .description = description, .stream = stream, .session_id = session_id, .scroll = scroll, .hold = hold, .committed = hold and self.status == .idle, .spool = spool, .exhausted = stream == null, .database = database, .namespace = namespace, .search_path = self.effectiveSearchPath(), .creation_epoch = self.cursor_epoch });
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
        req.setting_epoch = cursor.description.setting_epoch;
        req.database = cursor.database;
        req.namespace = cursor.namespace;
        req.search_path = cursor.search_path;
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
            {
                var page_arena = std.heap.ArenaAllocator.init(self.alloc);
                defer page_arena.deinit();
                const wanted: u32 = @intCast(@min(remaining, @min(self.limits.result_rows, 256)));
                var page = stream.next(stream.context, page_arena.allocator(), req, wanted) catch |err| {
                    cursor.stream = null;
                    stream.close(stream.context);
                    return err;
                };
                defer page.result.deinit();
                if (page.result.mutation_outcome != null or page.result.continuation != null or page.result.session_id != null or page.result.rowCount() > wanted or !columnsEqual(cursor.description.columns, page.result.columns)) return error.InvalidResult;
                if (page.result.rowCount() == 0 and !page.exhausted) return error.InvalidResult;
                if (page.result.sql_nulls) |flags| if (flags.len != page.result.rowCount()) return error.InvalidResult;
                if (!fetch.move) try self.dataResult(page.result, &.{}, cursor.statement);
                cursor.fetched += page.result.rowCount();
                remaining -= page.result.rowCount();
                cursor.exhausted = page.exhausted;
                try self.writer.flush();
            }
            // Native pages borrow the stream's memory admission. Release the
            // page before releasing the exhausted stream and its snapshots.
            if (cursor.exhausted) {
                cursor.stream = null;
                stream.close(stream.context);
            }
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
        req.search_path = cursor.search_path;
        req.session_id = if (cursor.committed or cursor.session_id.len == 0) null else cursor.session_id;
        req.binding_guard = cursor.description.binding_guard;
        req.setting_epoch = cursor.description.setting_epoch;
        return req;
    }

    fn fillCursor(self: *Session, cursor: *SqlCursor, through: usize) !void {
        const spool = &cursor.spool.?;
        const req = self.cursorRequest(cursor);
        while (!cursor.exhausted and spool.row_count < through) {
            try req.check();
            const stream = cursor.stream orelse return error.InvalidResult;
            {
                var arena = std.heap.ArenaAllocator.init(self.alloc);
                defer arena.deinit();
                const wanted: u32 = @intCast(@min(through - spool.row_count, @min(self.limits.result_rows, 256)));
                var page = try stream.next(stream.context, arena.allocator(), req, wanted);
                defer page.result.deinit();
                if (page.result.mutation_outcome != null or page.result.continuation != null or page.result.session_id != null or page.result.rowCount() > wanted or !columnsEqual(cursor.description.columns, page.result.columns)) return error.InvalidResult;
                if (page.result.rowCount() == 0 and !page.exhausted) return error.InvalidResult;
                const RetainPage = struct {
                    spool: *Spool,
                    result: backend.Result,
                    max_rows: usize,
                    fn run(raw: *anyopaque, control: ?backend.ReadControl) !void {
                        const work: *@This() = @ptrCast(@alignCast(raw));
                        try work.spool.appendControlled(work.result, work.max_rows, control);
                    }
                };
                var work: RetainPage = .{ .spool = spool, .result = page.result, .max_rows = self.limits.cursor_rows };
                // The stream still owns this immutable page. Retire a separate
                // consumer execution before advancing its lease or committing.
                if (self.source.vtable.deliver_row) |deliver| {
                    deliver(self.source.context, self.identity orelse return error.AuthenticationFailed, req, .{ .context = &work, .run = RetainPage.run }) catch |err| return if (err == error.OutOfMemory) error.ProgramLimitExceeded else err;
                } else RetainPage.run(&work, null) catch |err| return if (err == error.OutOfMemory) error.ProgramLimitExceeded else err;
                cursor.exhausted = page.exhausted;
            }
            if (cursor.exhausted) stream.detach.?(stream.context);
        }
    }

    fn materializeHeldCursors(self: *Session) !void {
        if (self.status != .in_transaction) return;
        self.completion_drain = true;
        defer self.completion_drain = false;
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
            .absolute => if (fetch.offset < 0) @as(i64, @intCast(spool.row_count)) + 1 +| fetch.offset else fetch.offset,
            .relative => spool.position +| fetch.offset,
        };
        const single = fetch.direction == .absolute or fetch.direction == .relative or fetch.count == 0;
        var remaining: u64 = if (single) 1 else fetch.count;
        if (!fetch.move) try self.rowDescription(cursor.description.columns, &.{});
        var emitted: u64 = 0;
        while (remaining != 0) : (remaining -= 1) {
            try req.check();
            if (target > 0 and @as(u64, @intCast(target)) > spool.row_count and !cursor.exhausted) {
                const wanted: usize = @intCast(@min(@as(u64, @intCast(target)) +| @min(remaining - 1, 255), std.math.maxInt(usize)));
                try self.fillCursor(cursor, wanted);
            }
            const end: i64 = @intCast(spool.row_count + 1);
            spool.position = std.math.clamp(target, 0, end);
            if (target <= 0 or target >= end) break;
            if (emitted != 0 and emitted % 256 == 0) {
                _ = validation.reset(.retain_capacity);
                try self.validateSpooledCursor(cursor, validation.allocator(), req);
            }
            if (!fetch.move) {
                const View = struct {
                    fn cell(raw: *anyopaque, _: std.mem.Allocator, row: usize, column: usize) !backend.Cell {
                        const source: *Spool = @ptrCast(@alignCast(raw));
                        return source.cell(row, column);
                    }
                };
                const view: backend.Result = .{ .columns = cursor.description.columns, .command_tag = "FETCH", .cells = .{ .context = spool, .begin = @intCast(target - 1), .count = 1, .width = cursor.description.columns.len, .read = View.cell } };
                try self.dataResult(view, &.{}, cursor.statement);
            }
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
        for (self.savepoints.items) |*point| {
            self.retainedAlloc().free(point.name);
            if (point.catalog) |*snapshot| snapshot.deinit(self.retainedAlloc());
        }
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
            self.retainedAlloc().free(entry.key);
            var cursor = entry.value;
            if (cursor.stream) |stream| stream.close(stream.context);
            if (cursor.spool) |*spool| spool.deinit();
            self.destroyRetainedArena(cursor.arena);
        }
    }

    fn clearSqlCursors(self: *Session) void {
        var iterator = self.sql_cursors.iterator();
        while (iterator.next()) |entry| {
            self.retainedAlloc().free(entry.key_ptr.*);
            if (entry.value_ptr.stream) |stream| stream.close(stream.context);
            if (entry.value_ptr.spool) |*spool| spool.deinit();
            self.destroyRetainedArena(entry.value_ptr.arena);
        }
        self.sql_cursors.clearRetainingCapacity();
    }

    fn removePrepared(self: *Session, name: []const u8) void {
        if (self.prepared.fetchRemove(name)) |entry| {
            self.retainedAlloc().free(entry.key);
            const value = entry.value;
            self.destroyRetainedArena(value.arena);
        }
    }

    fn clearPrepared(self: *Session) void {
        var it = self.prepared.iterator();
        while (it.next()) |entry| {
            self.retainedAlloc().free(entry.key_ptr.*);
            self.destroyRetainedArena(entry.value_ptr.arena);
        }
        self.prepared.clearRetainingCapacity();
    }

    fn removePortal(self: *Session, name: []const u8) void {
        if (self.portals.fetchRemove(name)) |entry| {
            self.retainedAlloc().free(entry.key);
            const value = entry.value;
            if (value.stream) |stream| stream.close(stream.context);
            if (value.result) |*result| result.deinit();
            if (value.parameter_owner) |owner| owner.release();
            self.destroyRetainedArena(value.arena);
            self.retainedAlloc().destroy(value);
        }
    }

    fn clearPortals(self: *Session) void {
        var it = self.portals.iterator();
        while (it.next()) |entry| {
            self.retainedAlloc().free(entry.key_ptr.*);
            const portal = entry.value_ptr.*;
            if (portal.stream) |stream| stream.close(stream.context);
            if (portal.result) |*result| result.deinit();
            if (portal.parameter_owner) |owner| owner.release();
            self.destroyRetainedArena(portal.arena);
            self.retainedAlloc().destroy(portal);
        }
        self.portals.clearRetainingCapacity();
    }

    fn executeStream(self: *Session, portal: *Portal, requested: i32) !void {
        self.cancel_requested.store(false, .release);
        self.executing.store(true, .release);
        defer self.executing.store(false, .release);
        var req = self.request(portal.statement, portal.parameters, portal.types);
        req.parameter_cells = portal.parameter_cells;
        req.parameter_owner = if (portal.parameter_owner) |owner| owner.lease() else null;
        req.parameter_descriptors = portal.description.parameter_descriptors;
        req.binding_guard = portal.description.binding_guard;
        req.setting_epoch = portal.description.setting_epoch;
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
            if (result.rowCount() > wanted or !columnsEqual(portal.description.columns, result.columns) or
                result.mutation_outcome != null or result.continuation != null or result.session_id != null)
                return error.InvalidResult;
            if (!page.exhausted and result.rowCount() == 0) return error.InvalidResult;
            if (result.sql_nulls) |flags| if (flags.len != result.rowCount()) return error.InvalidResult;
            try self.dataResult(result, portal.formats, portal.statement);
            remaining -= result.rowCount();
            portal.offset += result.rowCount();
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
        return self.simpleStreamParameters(statement, &.{}, &.{}, &.{}, null, null);
    }

    fn simpleStreamParameters(self: *Session, statement: []const u8, parameters: []const std.json.Value, types: []const backend.Type, descriptors: []const backend.Parameter, binding_guard: ?[]const u8, setting_epoch: ?u64) !bool {
        return self.simpleStreamCells(statement, parameters, types, descriptors, null, binding_guard, setting_epoch);
    }

    fn simpleStreamCells(self: *Session, statement: []const u8, parameters: []const std.json.Value, types: []const backend.Type, descriptors: []const backend.Parameter, native_cells: ?[]const @import("antfly_local_sources").sql_scalar.Datum, binding_guard: ?[]const u8, setting_epoch: ?u64) !bool {
        var settings_arena = std.heap.ArenaAllocator.init(self.alloc);
        defer settings_arena.deinit();
        if (try commands.settingCommand(settings_arena.allocator(), statement) != null) return false;
        const open = self.source.vtable.open_stream orelse return false;
        self.cancel_requested.store(false, .release);
        self.executing.store(true, .release);
        defer self.executing.store(false, .release);
        const arena = try self.newRequestArena();
        defer self.destroyRetainedArena(arena);
        var req = self.request(statement, parameters, types);
        req.parameter_descriptors = descriptors;
        req.parameter_cells = native_cells;
        req.binding_guard = binding_guard;
        req.setting_epoch = setting_epoch;
        req = try self.snapshotStreamRequest(arena, req);
        const stream = (try open(self.source.context, self.alloc, self.identity orelse return error.AuthenticationFailed, req)) orelse return false;
        var portal = Portal{
            .arena = arena,
            .statement = req.statement,
            .parameters = req.parameters,
            .parameter_cells = req.parameter_cells,
            .types = req.parameter_types,
            .formats = &.{},
            .description = .{ .columns = stream.columns, .parameter_types = types, .parameter_descriptors = descriptors, .binding_guard = binding_guard, .setting_epoch = setting_epoch },
            .stream = stream,
            .stream_opened = true,
        };
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
        const payload = try std.mem.concat(self.wireAllocator(), u8, &.{ name, &.{0}, value, &.{0} });
        defer self.freePayload(payload);
        try self.message('S', payload);
    }

    pub fn sendError(self: *Session, code: []const u8, message_text: []const u8) !void {
        const payload = try std.mem.concat(self.wireAllocator(), u8, &.{ "SERROR\x00C", code, "\x00M", message_text, "\x00\x00" });
        defer self.freePayload(payload);
        try self.message('E', payload);
    }

    fn sendDiagnostic(self: *Session, tag: u8, diagnostic: backend.Diagnostic, outcome: ?backend.MutationOutcome) !void {
        var bytes = std.Io.Writer.Allocating.init(self.wireAllocator());
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
        try self.message('Z', &.{@backingInt(self.status)});
    }

    fn command(self: *Session, tag: []const u8) !void {
        if (std.mem.indexOfScalar(u8, tag, 0) != null) return error.InvalidResult;
        const payload = try std.mem.concat(self.wireAllocator(), u8, &.{ tag, &.{0} });
        defer self.freePayload(payload);
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
            try bytes.writer.writeInt(u32, try values.columnOid(column), .big);
            try bytes.writer.writeInt(i16, values.columnTypeSize(column), .big);
            try bytes.writer.writeInt(i32, try values.columnModifier(column), .big);
            try bytes.writer.writeInt(u16, formatAt(formats, index), .big);
        }
        try self.message('T', bytes.written());
    }

    fn trimRowBuffer(self: *Session, bytes: *std.Io.Writer.Allocating) void {
        if (bytes.writer.buffer.len > 64 * 1024) {
            bytes.deinit();
            bytes.* = std.Io.Writer.Allocating.init(self.alloc);
        }
    }
    fn encodeDataRow(self: *Session, bytes: *std.Io.Writer.Allocating, columns: []const backend.Column, formats: []const u16, row: []const std.json.Value, null_flags: ?[]const bool) !void {
        if (row.len != columns.len) return error.InvalidResult;
        if (null_flags) |flags| if (flags.len != row.len) return error.InvalidResult;
        try bytes.writer.writeInt(u16, @intCast(row.len), .big);
        for (row, columns, 0..) |value, column, index| {
            const payload_limit = self.limits.frame_bytes -| 4;
            const remaining_headers = (row.len - index) * 4;
            if (bytes.written().len > payload_limit or remaining_headers > payload_limit - bytes.written().len) return error.ProgramLimitExceeded;
            const cell_limit = payload_limit - bytes.written().len - remaining_headers;
            const sql_null = if (null_flags) |flags| flags[index] else value == .null;
            if (sql_null) {
                if (value != .null) return error.InvalidResult;
                try bytes.writer.writeInt(i32, -1, .big);
                continue;
            }
            const position = bytes.written().len;
            try bytes.writer.writeInt(i32, 0, .big);
            try values.encodeColumnInto(self.alloc, &bytes.writer, column, formatAt(formats, index), value, cell_limit);
            const length = bytes.written().len - position - 4;
            if (length > self.limits.frame_bytes -| 4 or length > std.math.maxInt(i32)) return error.ProgramLimitExceeded;
            std.mem.writeInt(i32, bytes.writer.buffer[position..][0..4], @intCast(length), .big);
        }
        if (bytes.written().len > self.limits.frame_bytes -| 4) return error.ProgramLimitExceeded;
    }
    fn appendDataCell(self: *Session, bytes: *std.Io.Writer.Allocating, scratch: std.mem.Allocator, column: backend.Column, format: u16, value: backend.Cell, remaining_columns: usize) !void {
        return self.appendDataCellControlled(bytes, scratch, column, format, value, remaining_columns, null);
    }
    fn appendDataCellControlled(self: *Session, bytes: *std.Io.Writer.Allocating, scratch: std.mem.Allocator, column: backend.Column, format: u16, value: backend.Cell, remaining_columns: usize, control: ?backend.ReadControl) !void {
        const payload_limit = self.limits.frame_bytes -| 4;
        const remaining_headers = std.math.mul(usize, remaining_columns, 4) catch return error.ProgramLimitExceeded;
        if (bytes.written().len > payload_limit or remaining_headers > payload_limit - bytes.written().len) return error.ProgramLimitExceeded;
        const cell_limit = payload_limit - bytes.written().len - remaining_headers;
        if (value.sql_null) {
            if (value.value != .null or value.array != null or value.numeric != null or value.json_bytes != null or value.frozen_json != null) return error.InvalidResult;
            try bytes.writer.writeInt(i32, -1, .big);
            return;
        }
        const position = bytes.written().len;
        try bytes.writer.writeInt(i32, 0, .big);
        try values.encodeCellIntoControlled(scratch, &bytes.writer, column, format, value, cell_limit, control);
        const length = bytes.written().len - position - 4;
        if (length > std.math.maxInt(i32)) return error.ProgramLimitExceeded;
        std.mem.writeInt(i32, bytes.writer.buffer[position..][0..4], @intCast(length), .big);
    }
    fn dataResult(self: *Session, result: backend.Result, formats: []const u16, statement: []const u8) !void {
        if (result.cells) |cells| {
            if (cells.width != result.columns.len or result.rows.len != 0 or result.sql_nulls != null) return error.InvalidResult;
        } else if (result.sql_nulls) |flags| if (flags.len != result.rows.len) return error.InvalidResult;
        var bytes = std.Io.Writer.Allocating.init(self.alloc);
        defer bytes.deinit();
        var scratch = std.heap.ArenaAllocator.init(self.alloc);
        defer scratch.deinit();
        const Row = struct {
            session: *Session,
            result: backend.Result,
            formats: []const u16,
            row: usize,
            bytes: *std.Io.Writer.Allocating,
            scratch: std.mem.Allocator,
            fn encode(raw: *anyopaque, control: ?backend.ReadControl) !void {
                const self_: *@This() = @ptrCast(@alignCast(raw));
                const result_ = self_.result;
                if (result_.cells == null) {
                    if (result_.rows[self_.row].len != result_.columns.len) return error.InvalidResult;
                    if (result_.sql_nulls) |flags| if (flags[self_.row].len != result_.columns.len) return error.InvalidResult;
                }
                try self_.bytes.writer.writeInt(u16, @intCast(result_.columns.len), .big);
                for (result_.columns, 0..) |column, index| {
                    const value = try result_.cellControlled(self_.scratch, self_.row, index, control);
                    try self_.session.appendDataCellControlled(self_.bytes, self_.scratch, column, formatAt(self_.formats, index), value, result_.columns.len - index, control);
                }
                if (self_.bytes.written().len > self_.session.limits.frame_bytes -| 4) return error.ProgramLimitExceeded;
            }
        };
        for (0..result.rowCount()) |row| {
            if (!scratch.reset(.{ .retain_with_limit = 64 * 1024 })) _ = scratch.reset(.free_all);
            bytes.writer.end = 0;
            var work: Row = .{ .session = self, .result = result, .formats = formats, .row = row, .bytes = &bytes, .scratch = scratch.allocator() };
            // Durable acknowledgments keep their existing completion path.
            // Read delivery retires its consumer before any socket write.
            if (result.mutation_outcome == null and self.source.vtable.deliver_row != null) {
                try self.source.vtable.deliver_row.?(self.source.context, self.identity orelse return error.AuthenticationFailed, self.request(statement, &.{}, &.{}), .{ .context = &work, .run = Row.encode });
            } else try Row.encode(&work, null);
            try self.message('D', bytes.written());
            self.trimRowBuffer(&bytes);
        }
    }

    fn dataRows(self: *Session, columns: []const backend.Column, formats: []const u16, rows: []const []const std.json.Value, flags: ?[]const []const bool) !void {
        if (flags) |bits| if (bits.len != rows.len) return error.InvalidResult;
        var bytes = std.Io.Writer.Allocating.init(self.alloc);
        defer bytes.deinit();
        for (rows, 0..) |row, index| {
            bytes.writer.end = 0;
            try self.encodeDataRow(&bytes, columns, formats, row, if (flags) |bits| bits[index] else null);
            try self.message('D', bytes.written());
            self.trimRowBuffer(&bytes);
        }
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
fn cachedColumnsEqual(a: []const backend.Column, b: []const backend.Column) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| if (left.type != right.type or left.element_type != right.element_type or
        !@import("antfly_local_sources").sql_scalar.NumericModifier.eql(left.numeric_modifier, right.numeric_modifier)) return false;
    return true;
}

fn columnsEqual(a: []const backend.Column, b: []const backend.Column) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| if (left.type != right.type or left.element_type != right.element_type or !@import("antfly_local_sources").sql_scalar.NumericModifier.eql(left.numeric_modifier, right.numeric_modifier) or !std.mem.eql(u8, left.name, right.name)) return false;
    return true;
}

test "pgwire result shape fences array element identity across prepared and cursor pages" {
    const int4 = [_]backend.Column{.{ .name = "items", .type = .array, .element_type = .int32 }};
    const int8 = [_]backend.Column{.{ .name = "items", .type = .array, .element_type = .int64 }};
    const missing = [_]backend.Column{.{ .name = "items", .type = .array }};
    const json = [_]backend.Column{.{ .name = "items", .type = .json }};
    try std.testing.expect(columnsEqual(&int4, &int4));
    try std.testing.expect(!columnsEqual(&int4, &int8));
    try std.testing.expect(!columnsEqual(&int4, &missing));
    try std.testing.expect(!columnsEqual(&int4, &json));
    try std.testing.expect(cachedColumnsEqual(&int4, &int4));
    try std.testing.expect(!cachedColumnsEqual(&int4, &int8));
    try std.testing.expect(!cachedColumnsEqual(&int4, &missing));
    var renamed = int4;
    renamed[0].name = "renamed";
    try std.testing.expect(cachedColumnsEqual(&int4, &renamed));
}

test "pgwire NUMERIC result identity fences precision scale and unconstrained results" {
    const scalar = [_]backend.Column{.{ .name = "n", .type = .number, .element_type = .numeric, .numeric_modifier = .{ .precision = 4, .scale = 2 } }};
    var changed = scalar;
    changed[0].numeric_modifier.?.scale = 1;
    try std.testing.expect(columnsEqual(&scalar, &scalar));
    try std.testing.expect(!columnsEqual(&scalar, &changed));
    try std.testing.expect(!cachedColumnsEqual(&scalar, &changed));
    changed[0].numeric_modifier = null;
    try std.testing.expect(!columnsEqual(&scalar, &changed));
    try std.testing.expect(!cachedColumnsEqual(&scalar, &changed));
}

test "pgwire typed parameter failures retain actionable PostgreSQL SQLSTATEs" {
    try std.testing.expectEqualStrings("22P02", sqlstate(error.SqlInvalidTextRepresentation));
    try std.testing.expectEqualStrings("22P03", sqlstate(error.InvalidSqlBinaryRepresentation));
    try std.testing.expectEqualStrings("42804", sqlstate(error.SqlBinaryTypeMismatch));
    try std.testing.expectEqualStrings("22003", sqlstate(error.SqlNumericOutOfRange));
    try std.testing.expectEqualStrings("22021", sqlstate(error.SqlInvalidTextEncoding));
    try std.testing.expectEqualStrings("42P08", sqlstate(error.ConflictingSqlParameterTypes));
}

fn sqlstate(err: anyerror) []const u8 {
    return switch (err) {
        error.AuthenticationFailed, error.Unauthorized => "28000",
        error.InvalidPassword => "28P01",
        error.Forbidden, error.AccessDenied, error.SettingWriteForbidden => "42501",
        error.QueryCanceled, error.Timeout => "57014",
        error.UniqueConstraintViolation => "23505",
        error.ForeignKeyParentMissing, error.ForeignKeyReferenced => "23503",
        error.InvalidParameter => "22P02",
        error.SqlInvalidTextRepresentation, error.InvalidSqlArrayShape => "22P02",
        error.InvalidSqlBinaryRepresentation, error.SqlBinaryTypeMismatch => @import("antfly_local_sources").sql_errors.describe(err).code,
        error.SqlNumericOutOfRange => "22003",
        error.SqlInvalidTextEncoding => "22021",
        error.SqlArraySubscriptError => "2202E",
        error.ConflictingSqlParameterTypes => "42P08",
        error.UnknownSqlParameterType => "42P18",
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
        error.InvalidReadRequirements, error.ReadReceiptGenerationMismatch => "22023",
        error.UnsupportedReadVisibility => "0A000",
        error.ReadRequirementNotReady => "53300",
        error.ReadSnapshotTooOld => "40001",
        error.InFailedSqlTransaction => "25P02",
        error.CursorMustBeInTransaction, error.NoActiveSqlTransaction => "25P01",
        error.CursorNotScrollable => "55000",
        error.InvalidSqlParameters, error.InvalidSqlParameter, error.InvalidSqlNumber, error.SqlTypeMismatch, error.InvalidSqlLimit, error.InvalidSettingValue => "22023",
        error.SqlSubstringError => "22011",
        error.InvalidSqlCharacterCode => "54000",
        error.SqlNotNullViolation => "23502",
        error.SqlProgramLimitExceeded, error.SqlResultTooLarge, error.SqlLimitExceeded, error.SettingLimitExceeded => "54000",
        error.UnknownSetting => "42704",
        error.SettingCatalogChanged => "55000",
        error.UnknownColumn => "42703",
        error.UnknownTable => "42P01",
        error.UnsupportedSqlExecution, error.UnsupportedSqlShape, error.SqlStatementSnapshotRequired => "0A000",
        error.UnsupportedStartupOption, error.UnsupportedFormattingSetting => "0A000",
        error.UnsupportedEncoding => "22021",
        error.PortalExecutionFailed => "55000",
        error.TooManyConnections, error.SqlWriteCapacityUnavailable => "53300",
        error.CachedPlanResultTypeChanged, error.UnsupportedParameterType, error.UnsupportedParameterFormat, error.UnsupportedResultFormat, error.UnsupportedProtocolMessage, error.UnsupportedContinuation, error.UnsupportedResultPrecision, error.UnsupportedStatement, error.ResultShapeChanged => "0A000",
        else => "XX000",
    };
}

test "workload envelope pgwire authenticated startup retires partitioned frames under bulk OOM" {
    const Fixture = struct {
        authenticated: bool = false,
        fn authenticate(ptr: *anyopaque, _: std.mem.Allocator, user: []const u8, password: []const u8) !backend.Identity {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (!std.mem.eql(u8, user, "admin") or !std.mem.eql(u8, password, "test")) return error.AuthenticationFailed;
            self.authenticated = true;
            return .{ .context = self, .release = release };
        }
        fn release(_: *anyopaque, _: std.mem.Allocator) void {}
        fn disconnect(_: *anyopaque, _: backend.Identity, _: ?[]const u8) void {}
    };
    var fixture: Fixture = .{};
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var memory: @import("../common/allocation_budget.zig").Partitioned = .{
        .foreground = .{ .child = failing.allocator(), .limit = 1 << 20 },
        .control = .{ .child = std.testing.allocator, .limit = 32 << 10 },
    };
    var reader = std.Io.Reader.fixed("\x00\x00\x00\x14\x00\x03\x00\x00user\x00admin\x00\x00p\x00\x00\x00\x09test\x00");
    var output = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer output.deinit();
    var session: Session = .{
        .alloc = failing.allocator(),
        .io = std.testing.io,
        .source = .{ .context = &fixture, .vtable = &.{ .authenticate = Fixture.authenticate, .describe = undefined, .execute = undefined, .disconnect = Fixture.disconnect } },
        .reader = &reader,
        .writer = &output.writer,
        .frame_memory = &memory,
    };
    defer session.deinit();
    try std.testing.expect(try session.startup());
    try std.testing.expect(fixture.authenticated);
    var response = Cursor{ .bytes = output.written() };
    var parameters: usize = 0;
    var ready = false;
    while (response.offset < response.bytes.len) {
        const tag = try response.int(u8);
        const length = try response.int(u32);
        const payload = try response.take(length - 4);
        if (tag == 'S') parameters += 1;
        if (tag == 'Z') {
            try std.testing.expectEqualStrings("I", payload);
            ready = true;
        }
    }
    // Driver-compatible startup now includes extra_float_digits and IntervalStyle.
    try std.testing.expectEqual(@as(usize, 9), parameters);
    try std.testing.expect(ready);
    try std.testing.expectEqual(@as(usize, 0), memory.foreground.stats().used_bytes);
    try std.testing.expectEqual(@as(usize, 0), memory.control.stats().used_bytes);
}

test "workload envelope pgwire frame parsing preserves control reserve under bulk OOM" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var memory: @import("../common/allocation_budget.zig").Partitioned = .{
        .foreground = .{ .child = failing.allocator(), .limit = 1 << 20 },
        .control = .{ .child = std.testing.allocator, .limit = 32 << 10 },
    };
    var reader = std.Io.Reader.fixed("\x00\x00\x00\x0bCOMMIT\x00");
    var session: Session = .{ .alloc = failing.allocator(), .io = std.testing.io, .source = undefined, .reader = &reader, .writer = undefined, .frame_memory = &memory };
    const payload = try session.readPayload(4);
    try std.testing.expectEqualStrings("COMMIT\x00", payload);
    try std.testing.expect(memory.control.stats().used_bytes > 0);
    session.freePayload(payload);
    try std.testing.expectEqual(@as(usize, 0), memory.control.stats().used_bytes);
    var oversized: [8197]u8 = undefined;
    std.mem.writeInt(u32, oversized[0..4], oversized.len, .big);
    @memset(oversized[4..], 'x');
    reader = std.Io.Reader.fixed(&oversized);
    try std.testing.expectError(error.OutOfMemory, session.readPayload(4));
    try std.testing.expectEqual(@as(usize, 0), memory.foreground.stats().used_bytes);
    try std.testing.expectEqual(@as(usize, 0), memory.control.stats().used_bytes);
}

test "workload envelope pgwire verified simple controls acknowledge under bulk OOM" {
    const Fixture = struct {
        completed: usize = 0,
        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, _: backend.Identity, req: backend.Request) !backend.Result {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const tag = try alloc.dupe(u8, req.statement);
            self.completed += 1;
            return .{ .command_tag = tag };
        }
        fn release(_: *anyopaque, _: std.mem.Allocator) void {}
        fn disconnect(_: *anyopaque, _: backend.Identity, _: ?[]const u8) void {}
    };
    var fixture: Fixture = .{};
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var memory: @import("../common/allocation_budget.zig").Partitioned = .{
        .foreground = .{ .child = failing.allocator(), .limit = 1 << 20 },
        .control = .{ .child = std.testing.allocator, .limit = 32 << 10 },
    };
    var output = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer output.deinit();
    var session: Session = .{
        .alloc = failing.allocator(),
        .io = std.testing.io,
        .source = .{ .context = &fixture, .vtable = &.{ .authenticate = undefined, .describe = undefined, .execute = Fixture.execute, .disconnect = Fixture.disconnect } },
        .identity = .{ .context = &fixture, .release = Fixture.release },
        .reader = undefined,
        .writer = &output.writer,
        .status = .in_transaction,
        .frame_memory = &memory,
        .control_allocator = std.testing.allocator,
        .is_protected_control = @import("../api/sql_control.zig").classify,
    };
    defer session.deinit();
    // Startup emits ParameterStatus before serving queries. Its allocation
    // must use the same partitioned owner as its free, even when bulk is full.
    try session.parameterStatus("server_encoding", "UTF8");
    try std.testing.expectEqual(@as(u8, 'S'), output.written()[0]);
    try std.testing.expect(std.mem.indexOf(u8, output.written(), "server_encoding\x00UTF8\x00") != null);
    try std.testing.expectEqual(@as(usize, 0), memory.control.stats().used_bytes);
    session.setDeadline(session.limits.statement_timeout_ms);
    try session.dispatch('Q', "COMMIT\x00");
    try std.testing.expectEqual(@as(usize, 1), fixture.completed);
    try std.testing.expectEqual(backend.TransactionStatus.idle, session.status);
    try std.testing.expect(std.mem.indexOf(u8, output.written(), "COMMIT\x00") != null);
    session.status = .failed;
    try session.dispatch('Q', "ROLLBACK\x00");
    try std.testing.expectEqual(@as(usize, 2), fixture.completed);
    try std.testing.expectEqual(backend.TransactionStatus.idle, session.status);
    try std.testing.expectError(error.OutOfMemory, session.dispatch('Q', "SELECT 1\x00"));
    try std.testing.expectEqual(@as(usize, 2), fixture.completed);
    try session.sendError("53300", "ordinary transport capacity unavailable");
    try std.testing.expectEqual(@as(usize, 0), memory.control.stats().used_bytes);
}

test "pgwire retained session idle transaction deadline covers failed owners" {
    var session: Session = .{ .alloc = std.testing.allocator, .io = std.testing.io, .source = undefined, .reader = undefined, .writer = undefined, .limits = .{ .idle_timeout_ms = 1000, .idle_transaction_timeout_ms = 100 } };
    try std.testing.expectEqual(@as(u32, 1000), session.idleTimeoutMs());
    session.status = .in_transaction;
    try std.testing.expectEqual(@as(u32, 100), session.idleTimeoutMs());
    session.status = .failed;
    try std.testing.expectEqual(@as(u32, 100), session.idleTimeoutMs());
    session.limits.idle_transaction_timeout_ms = 2000;
    try std.testing.expectEqual(@as(u32, 1000), session.idleTimeoutMs());
}

test "pgwire retained portal lease fences session teardown after stream failure" {
    const Fixture = struct {
        lease: backend.RetainedLifetime.Lease = .{},
        cancel_flag: ?*const std.atomic.Value(bool) = null,
        closed: bool = false,
        disconnected: bool = false,
        teardown_started: std.Io.Event = .unset,
        teardown_continue: std.Io.Event = .unset,
        teardown_done: std.Io.Event = .unset,

        fn describe(_: *anyopaque, _: std.mem.Allocator, _: backend.Identity, _: backend.Request) !backend.Description {
            // Execute rebinds the unstarted portal before opening its stream.
            return .{ .columns = &.{}, .parameter_types = &.{} };
        }

        fn open(raw: *anyopaque, _: std.mem.Allocator, _: backend.Identity, req: backend.Request) !?backend.ReadStream {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.lease = (req.retained_lifetime orelse return error.MissingRetainedLifetime).retain();
            self.cancel_flag = req.cancel_requested;
            return .{ .context = self, .columns = &.{}, .next = next, .close = close };
        }
        fn next(_: *anyopaque, _: std.mem.Allocator, _: backend.Request, _: u32) !backend.StreamPage {
            return error.QueryCanceled;
        }
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.closed = true; // Physical owner deliberately still holds its lease.
        }
        fn disconnect(raw: *anyopaque, _: backend.Identity, _: ?[]const u8) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.lease.lifetime == null);
            self.disconnected = true;
        }
        fn release(_: *anyopaque, _: std.mem.Allocator) void {}
        fn unregister(raw: *anyopaque, session: *Session) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.teardown_started.set(session.io);
            self.teardown_continue.waitUncancelable(session.io);
        }
        fn teardown(self: *@This(), session: *Session) void {
            session.deinit();
            self.teardown_done.set(session.io);
        }
    };
    var fixture: Fixture = .{};
    var parent: Budget = .{ .child = std.testing.allocator, .limit = 1 << 20 };
    var shared: @import("budget.zig").Shared = .{ .child = std.testing.allocator, .limit = 1 << 20 };
    var reader = std.Io.Reader.fixed("");
    var output = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer output.deinit();
    var session: Session = .{
        .alloc = parent.allocator(),
        .io = std.testing.io,
        .source = .{ .context = &fixture, .vtable = &.{ .authenticate = undefined, .describe = Fixture.describe, .execute = undefined, .disconnect = Fixture.disconnect, .open_stream = Fixture.open } },
        .identity = .{ .context = &fixture, .release = Fixture.release },
        .reader = &reader,
        .writer = &output.writer,
        .retained_shared = &shared,
    };
    var session_owned = true;
    defer if (session_owned) session.deinit();
    const arena = try session.newRetainedArena();
    const statement = try arena.allocator().dupe(u8, "SELECT 1");
    const key = try session.retainedAlloc().dupe(u8, "");
    const portal = try session.retainedAlloc().create(Portal);
    portal.* = .{
        .arena = arena,
        .statement = statement,
        .parameters = &.{},
        .types = &.{},
        .formats = &.{},
        .description = .{ .columns = &.{}, .parameter_types = &.{} },
    };
    try session.portals.put(session.retainedAlloc(), key, portal);
    session.setDeadline(5000);
    try std.testing.expectError(error.QueryCanceled, session.dispatch('E', "\x00\x00\x00\x00\x01"));
    try std.testing.expect(fixture.closed);
    session.removePortal("");
    // Remove map backing before the concurrent teardown so all subsequent
    // allocator mutation is fenced by the outstanding physical lease.
    session.portals.deinit(session.retainedAlloc());
    session.portals = .empty;
    const charged = parent.used.load(.acquire);
    const retained_charge = shared.used.load(.acquire);
    try std.testing.expect(charged > 0 and retained_charge > 0);
    var second = fixture.lease.lifetime.?.retain();
    second.release();
    second.release();
    session.hooks = .{ .context = &fixture, .register = undefined, .unregister = Fixture.unregister, .cancel = undefined };
    var task = try std.testing.io.concurrent(Fixture.teardown, .{ &fixture, &session });
    session_owned = false;
    // Always release before joining, including an assertion failure.
    defer task.await(std.testing.io);
    defer fixture.lease.release();
    defer fixture.teardown_continue.set(std.testing.io);
    try fixture.teardown_started.wait(std.testing.io);
    fixture.teardown_continue.set(std.testing.io);
    try std.testing.io.sleep(.fromMilliseconds(10), .awake);
    try std.testing.expect(!fixture.teardown_done.isSet());
    try std.testing.expectEqual(charged, parent.used.load(.acquire));
    try std.testing.expectEqual(retained_charge, shared.used.load(.acquire));
    try std.testing.expectEqualStrings("SELECT 1", statement);
    try std.testing.expect(!fixture.cancel_flag.?.load(.acquire));
    fixture.lease.release();
    task.await(std.testing.io);
    try std.testing.expect(fixture.disconnected);
    try std.testing.expectEqual(@as(usize, 0), parent.used.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), shared.used.load(.acquire));
}

test "pgwire retained arena header allocation failure retires parent budgets" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 1 });
    var parent: Budget = .{ .child = failing.allocator(), .limit = 1 << 20 };
    var reader = std.Io.Reader.fixed("");
    var output = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer output.deinit();
    var session: Session = .{ .alloc = parent.allocator(), .io = std.testing.io, .source = undefined, .reader = &reader, .writer = &output.writer };
    try std.testing.expectError(error.OutOfMemory, session.newRetainedArena());
    session.deinit();
    try std.testing.expectEqual(@as(usize, 0), parent.used.load(.acquire));
}

test "pgwire simple stream lease owns request after input frame and namespace change" {
    const Fixture = struct {
        lease: backend.RetainedLifetime.Lease = .{},
        request: backend.Request = undefined,
        closed: bool = false,
        fn open(raw: *anyopaque, _: std.mem.Allocator, _: backend.Identity, req: backend.Request) !?backend.ReadStream {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.lease = (req.retained_lifetime orelse return error.MissingRetainedLifetime).retain();
            self.request = req;
            return .{ .context = self, .columns = &.{}, .next = next, .close = close };
        }
        fn next(_: *anyopaque, _: std.mem.Allocator, _: backend.Request, _: u32) !backend.StreamPage {
            return error.QueryCanceled;
        }
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.closed = true;
        }
        fn release(_: *anyopaque, _: std.mem.Allocator) void {}
        fn disconnect(_: *anyopaque, _: backend.Identity, _: ?[]const u8) void {}
    };
    var fixture: Fixture = .{};
    var parent: Budget = .{ .child = std.testing.allocator, .limit = 1 << 20 };
    var reader = std.Io.Reader.fixed("");
    var output = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer output.deinit();
    var session: Session = .{
        .alloc = parent.allocator(),
        .io = std.testing.io,
        .source = .{ .context = &fixture, .vtable = &.{ .authenticate = undefined, .describe = undefined, .execute = undefined, .disconnect = Fixture.disconnect, .open_stream = Fixture.open } },
        .identity = .{ .context = &fixture, .release = Fixture.release },
        .reader = &reader,
        .writer = &output.writer,
    };
    defer session.deinit();
    defer fixture.lease.release();
    session.namespace = try session.alloc.dupe(u8, "public");
    session.setDeadline(5000);
    {
        var input = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer input.deinit();
        const alloc = input.allocator();
        const statement = try alloc.dupe(u8, "SELECT $1, $2");
        const parameters = try alloc.alloc(std.json.Value, 2);
        parameters[0] = try std.json.parseFromSliceLeaky(std.json.Value, alloc, "{\"nested\":[\"original\"]}", .{ .allocate = .alloc_always });
        parameters[1] = .{ .number_string = try alloc.dupe(u8, "12345678901234567890") };
        const types = try alloc.dupe(backend.Type, &.{ .json, .number });
        const guard = try alloc.dupe(u8, "bound-to-schema-1");
        try std.testing.expectError(error.QueryCanceled, session.simpleStreamParameters(statement, parameters, types, &.{}, guard, 17));
        try std.testing.expect(fixture.closed);
        @memset(@constCast(statement), 'x');
        @memset(@constCast(guard), 'x');
        @memset(types, .unknown);
        @memset(@constCast(parameters[0].object.get("nested").?.array.items[0].string), 'x');
        @memset(@constCast(parameters[1].number_string), 'x');
    }
    @memset(session.namespace.?, 'x');
    const retained = fixture.request;
    try std.testing.expectEqualStrings("SELECT $1, $2", retained.statement);
    try std.testing.expectEqualStrings("original", retained.parameters[0].object.get("nested").?.array.items[0].string);
    try std.testing.expectEqualStrings("12345678901234567890", retained.parameters[1].number_string);
    try std.testing.expectEqualSlices(backend.Type, &.{ .json, .number }, retained.parameter_types);
    try std.testing.expectEqualStrings("bound-to-schema-1", retained.binding_guard.?);
    try std.testing.expectEqualStrings("public", retained.namespace.?);
    try std.testing.expectEqual(@as(?u64, 17), retained.setting_epoch);
    try std.testing.expect(session.retired_arenas != null);
    try std.testing.expect(parent.used.load(.acquire) > session.namespace.?.len);
    fixture.lease.release();
    session.reapRetainedArenas();
    try std.testing.expectEqual(session.namespace.?.len, parent.used.load(.acquire));
}

test "pgwire stream request snapshot allocation failures preserve parent accounting" {
    const Fixture = struct {
        fn run(alloc: std.mem.Allocator, request: backend.Request) !void {
            var parent: Budget = .{ .child = alloc, .limit = 1 << 20 };
            defer std.debug.assert(parent.used.load(.acquire) == 0);
            var reader = std.Io.Reader.fixed("");
            var output = std.Io.Writer.Allocating.init(std.testing.allocator);
            defer output.deinit();
            var session: Session = .{ .alloc = parent.allocator(), .io = std.testing.io, .source = undefined, .reader = &reader, .writer = &output.writer };
            defer session.deinit();
            const arena = try session.newRetainedArena();
            defer session.destroyRetainedArena(arena);
            const snapshot = try session.snapshotStreamRequest(arena, request);
            try std.testing.expectEqualStrings("setting before", snapshot.setting_overlay[0].value.string);
            try std.testing.expectEqualStrings("db-before", snapshot.database.?);
            try std.testing.expectEqualStrings("session-before", snapshot.session_id.?);
        }
    };
    var cancel: std.atomic.Value(bool) = .init(false);
    const request: backend.Request = .{
        .statement = "SELECT $1",
        .parameters = &.{.{ .string = "a parameter before" }},
        .parameter_types = &.{.string},
        .database = "db-before",
        .namespace = "public",
        .session_namespace = "public",
        .session_id = "session-before",
        .binding_guard = "guard-before",
        .setting_overlay = &.{.{ .identity = .{ .id = 7, .generation = 9 }, .value = .{ .string = "setting before" } }},
        .limit = 1,
        .io = std.testing.io,
        .deadline = .{ .clock = .awake, .raw = .{ .nanoseconds = std.math.maxInt(i96) } },
        .cancel_requested = &cancel,
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{request});
}

test "pgwire client statement deadline preserves hard transport cutoff and cannot extend authority" {
    var session: Session = .{
        .alloc = std.testing.allocator,
        .io = std.testing.io,
        .source = undefined,
        .reader = undefined,
        .writer = undefined,
        .limits = .{ .statement_timeout_ms = 5000 },
    };
    session.statement_timeout = 100;
    session.setStatementDeadline();
    const hard = session.deadline_ns.load(.acquire);
    try std.testing.expectEqual(hard - 4900 * std.time.ns_per_ms, session.statement_deadline_ns.?);
    try std.testing.expectEqual(session.statement_deadline_ns.?, session.request("SELECT 1", &.{}, &.{}).deadline.raw.nanoseconds);
    // Zero disables only the client hint; larger hints cannot exceed the cap.
    for ([_]?u32{ null, 0, 5000, 6000, std.math.maxInt(u32) }) |timeout| {
        session.statement_timeout = timeout;
        session.setStatementDeadline();
        try std.testing.expectEqual(session.deadline_ns.load(.acquire), session.statement_deadline_ns.?);
    }
    // Idle/startup deadlines do not retain the previous statement's hint.
    session.setDeadline(60000);
    try std.testing.expectEqual(@as(?i64, null), session.statement_deadline_ns);
}

test "pgwire consumer row delivery joins before socket output and cancels before partial frame publication" {
    const Fixture = struct {
        active: bool = false,
        deliveries: usize = 0,
        polls: usize = 0,
        stop: usize = std.math.maxInt(usize),
        text: []const u8,
        fn poll(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            try std.testing.expect(self.active);
            self.polls += 1;
            if (self.polls >= self.stop) return error.OriginalDeliveryCanceled;
        }
        fn delivery(raw: *anyopaque, _: backend.Identity, request_: backend.Request, work: backend.DeliveryWork) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqualStrings("SELECT body", request_.statement);
            self.deliveries += 1;
            self.active = true;
            defer self.active = false;
            try work.run(work.context, .{ .ptr = self, .checkpoint = poll });
        }
        fn oldCell(_: *anyopaque, _: std.mem.Allocator, _: usize, _: usize) !backend.Cell {
            return error.UncontrolledCellRead;
        }
        fn cell(raw: *anyopaque, _: std.mem.Allocator, _: usize, _: usize, control: ?backend.ReadControl) !backend.Cell {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const owner = control orelse return error.MissingConsumer;
            try owner.checkpoint(owner.ptr);
            return .{ .value = .{ .string = self.text }, .sql_null = false };
        }
        fn auth(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8) !backend.Identity {
            return error.UnexpectedAuthentication;
        }
        fn describe(_: *anyopaque, _: std.mem.Allocator, _: backend.Identity, _: backend.Request) !backend.Description {
            return error.UnexpectedDescribe;
        }
        fn execute(_: *anyopaque, _: std.mem.Allocator, _: backend.Identity, _: backend.Request) !backend.Result {
            return error.UnexpectedExecute;
        }
        fn release(_: *anyopaque, _: std.mem.Allocator) void {}
        fn disconnect(_: *anyopaque, _: backend.Identity, _: ?[]const u8) void {}
    };
    const Sink = struct {
        fixture: *Fixture,
        output: std.Io.Writer.Allocating,
        writer: std.Io.Writer = .{ .vtable = &.{ .drain = drain }, .buffer = &.{}, .end = 0 },
        fn drain(raw: *std.Io.Writer, bytes: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
            const self: *@This() = @alignCast(@fieldParentPtr("writer", raw));
            if (self.fixture.active) return error.WriteFailed;
            return self.output.writer.writeSplat(bytes, splat);
        }
    };
    const a = std.testing.allocator;
    const text = try a.alloc(u8, 32 * 1024);
    defer a.free(text);
    @memset(text, 'x');
    var fixture: Fixture = .{ .text = text };
    var sink: Sink = .{ .fixture = &fixture, .output = .init(a) };
    defer sink.output.deinit();
    var input = std.Io.Reader.fixed("");
    var session: Session = .{ .alloc = a, .io = std.testing.io, .reader = &input, .writer = &sink.writer, .source = .{ .context = &fixture, .vtable = &.{ .authenticate = Fixture.auth, .describe = Fixture.describe, .execute = Fixture.execute, .disconnect = Fixture.disconnect, .deliver_row = Fixture.delivery } }, .identity = .{ .context = &fixture, .release = Fixture.release } };
    defer session.deinit();
    const result: backend.Result = .{ .columns = &.{.{ .name = "body", .type = .string }}, .command_tag = "SELECT", .cells = .{ .context = &fixture, .count = 1, .width = 1, .read = Fixture.oldCell, .read_controlled = Fixture.cell } };
    try session.dataResult(result, &.{}, "SELECT body");
    try std.testing.expectEqual(@as(u8, 'D'), sink.output.written()[0]);
    try std.testing.expect(fixture.polls > 8 and !fixture.active);
    sink.output.writer.end = 0;
    fixture.polls = 0;
    fixture.stop = 8;
    try std.testing.expectError(error.OriginalDeliveryCanceled, session.dataResult(result, &.{}, "SELECT body"));
    try std.testing.expectEqual(@as(usize, 0), sink.output.written().len);
    try std.testing.expect(!fixture.active);
    // Existing durable completion has no fresh fallible delivery admission.
    const committed: backend.Result = .{ .columns = result.columns, .rows = &.{&.{.{ .string = "committed" }}}, .command_tag = "INSERT 0 1", .mutation_outcome = .committed };
    const deliveries = fixture.deliveries;
    try session.dataResult(committed, &.{}, "INSERT RETURNING body");
    try std.testing.expectEqual(deliveries, fixture.deliveries);
}
