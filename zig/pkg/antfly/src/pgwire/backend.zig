// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Transport-independent, authenticated SQL boundary. The caller supplies an
//! arena for every describe/execute call; returned slices must use that arena.
//! Parameters remain typed values, never interpolated into SQL text.
const std = @import("std");

pub const Type = enum { string, uuid, integer, number, boolean, datetime, json, unknown };
pub const Column = struct { name: []const u8, type: Type };
pub const TransactionStatus = enum(u8) { idle = 'I', in_transaction = 'T', failed = 'E' };

pub const Identity = struct {
    context: *anyopaque,
    release: *const fn (*anyopaque, std.mem.Allocator) void,
};

pub const Request = struct {
    statement: []const u8,
    parameters: []const std.json.Value = &.{},
    parameter_types: []const Type = &.{},
    database: ?[]const u8 = null,
    namespace: ?[]const u8 = null,
    /// Ordered lookup candidates for unqualified SQL relations. The first
    /// entry remains `namespace` for statement/transaction ownership.
    search_path: ?@import("search_path.zig").Path = null,
    /// Immutable owner scope of an already active transaction; namespace above
    /// is the mutable lookup scope. Never supplied by a remote session id.
    session_namespace: ?[]const u8 = null,
    session_id: ?[]const u8 = null,
    limit: u32,
    io: std.Io,
    deadline: std.Io.Clock.Timestamp,
    cancel_requested: *const std.atomic.Value(bool),
    diagnostics: ?*Diagnostic = null,
    /// Backend-owned opaque Parse-time identity fence, copied into each portal.
    binding_guard: ?[]const u8 = null,
    /// Parse-time setting catalog epoch for prepared current_setting plans.
    setting_epoch: ?u64 = null,
    /// Connection-owned, identity-fenced setting values for this statement.
    lake_visibility: @import("session_commands.zig").LakeVisibility = .committed,
    setting_overlay: []const @import("antfly_local_sources").sql_setting_catalog.OverlayEntry = &.{},

    pub fn check(self: Request) !void {
        try self.io.checkCancel();
        if (self.cancel_requested.load(.acquire)) return error.QueryCanceled;
        if (self.deadline.raw.nanoseconds <= self.deadline.clock.now(self.io).nanoseconds)
            return error.QueryCanceled;
    }
};

/// Fixed-size storage survives error unwinding and per-statement arenas.
pub const Diagnostic = struct {
    code: ?[5]u8 = null,
    message: [256]u8 = undefined,
    message_len: u16 = 0,
    transaction_id: ?[32]u8 = null,
    retryable: ?bool = null,
    /// Authoritative session state after a statement failure (for example a
    /// COMMIT conflict ends the transaction, unlike an ordinary failed query).
    transaction_status: ?TransactionStatus = null,

    pub fn set(self: *Diagnostic, code: []const u8, message: []const u8, transaction_id: ?[32]u8, retryable: ?bool) void {
        std.debug.assert(code.len == 5);
        self.code = code[0..5].*;
        const len = @min(message.len, self.message.len);
        @memcpy(self.message[0..len], message[0..len]);
        self.message_len = @intCast(len);
        self.transaction_id = transaction_id;
        self.retryable = retryable;
    }
};

pub const MutationOutcome = enum { committed, committed_pending, committed_repair_required, committed_graph_metric_materialization_rejected };

pub const Description = struct {
    columns: []const Column = &.{},
    parameter_types: []const Type = &.{},
    binding_guard: ?[]const u8 = null,
    setting_epoch: ?u64 = null,
};

pub const Result = struct {
    columns: []const Column = &.{},
    rows: []const []const std.json.Value = &.{},
    sql_nulls: ?[]const []const bool = null,
    /// Borrowed typed cells owned by this result's lease. A transport reads
    /// cells directly; scroll/hold stores clone only at their ownership edge.
    cells: ?Cells = null,
    rows_affected: u64 = 0,
    command_tag: []const u8,
    session_id: ?[]const u8 = null,
    continuation: ?[]const u8 = null,
    transaction_status: TransactionStatus = .idle,
    mutation_outcome: ?MutationOutcome = null,
    ddl_receipt_json: ?[]const u8 = null,
    transaction_id: ?[32]u8 = null,
    /// Optional zero-copy owner for native result arenas. Released before the
    /// enclosing caller arena; its allocations still use the caller's budget.
    owner: ?struct { context: *anyopaque, release: *const fn (*anyopaque) void } = null,

    pub fn deinit(self: *Result) void {
        if (self.owner) |owner| owner.release(owner.context);
        self.owner = null;
    }

    pub fn rowCount(self: Result) usize {
        return if (self.cells) |cells| cells.count else self.rows.len;
    }

    pub fn cell(self: Result, alloc: std.mem.Allocator, row: usize, column: usize) !Cell {
        if (row >= self.rowCount() or column >= self.columns.len) return error.InvalidResult;
        if (self.cells) |cells| {
            if (self.rows.len != 0 or self.sql_nulls != null or cells.width != self.columns.len) return error.InvalidResult;
            return cells.read(cells.context, alloc, std.math.add(usize, cells.begin, row) catch return error.InvalidResult, column);
        }
        if (self.rows[row].len != self.columns.len) return error.InvalidResult;
        if (self.sql_nulls) |flags| if (flags.len != self.rows.len or flags[row].len != self.columns.len) return error.InvalidResult;
        const value = self.rows[row][column];
        return .{ .value = value, .sql_null = if (self.sql_nulls) |flags| flags[row][column] else value == .null };
    }

    /// A borrowed delivery view; only the original result releases its owner.
    pub fn view(self: Result, begin: usize, count: usize) !Result {
        if (begin > self.rowCount() or count > self.rowCount() - begin) return error.InvalidResult;
        var result = self;
        result.owner = null;
        if (result.cells) |*cells| {
            cells.begin = std.math.add(usize, cells.begin, begin) catch return error.InvalidResult;
            cells.count = count;
        } else {
            result.rows = self.rows[begin..][0..count];
            if (self.sql_nulls) |flags| {
                if (flags.len != self.rows.len) return error.InvalidResult;
                result.sql_nulls = flags[begin..][0..count];
            }
        }
        return result;
    }
};

pub const Cell = struct { value: std.json.Value, sql_null: bool };
pub const Cells = struct {
    context: *anyopaque,
    begin: usize = 0,
    count: usize,
    width: usize,
    read: *const fn (*anyopaque, std.mem.Allocator, usize, usize) anyerror!Cell,
};

pub const Backend = struct {
    context: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        // Called even when a deployment permits anonymous access. The backend
        // alone decides whether credentials authorize a principal; a startup
        // username/database never itself grants access.
        authenticate: *const fn (*anyopaque, std.mem.Allocator, []const u8, []const u8) anyerror!Identity,
        describe: *const fn (*anyopaque, std.mem.Allocator, Identity, Request) anyerror!Description,
        execute: *const fn (*anyopaque, std.mem.Allocator, Identity, Request) anyerror!Result,
        /// Authenticated catalog snapshot for typed dotted-name SET/SHOW/RESET.
        load_settings: ?*const fn (*anyopaque, std.mem.Allocator, Identity, Request) anyerror!@import("antfly_local_sources").sql_setting_catalog.RawSnapshot = null,
        validate_namespace: ?*const fn (*anyopaque, std.mem.Allocator, Identity, Request) anyerror!void = null,
        /// Evaluate SQL EXECUTE arguments as scalar expressions without any
        /// catalog/table access. Values are owned by the supplied allocator.
        evaluate_parameters: ?*const fn (*anyopaque, std.mem.Allocator, Identity, Request, []const []const u8) anyerror![]const std.json.Value = null,
        /// Mark an explicit transaction failed when a SQL cursor fetch or
        /// declaration fails after its statement snapshot was opened.
        fail_transaction: ?*const fn (*anyopaque, Identity, Request) anyerror!void = null,
        /// Optional owned read-only pull execution. Null declines a blocking
        /// shape before execution. Each page is independently owned; release it
        /// before the next pull or closing the stream. Never use for mutations.
        open_stream: ?*const fn (*anyopaque, std.mem.Allocator, Identity, Request) anyerror!?ReadStream = null,
        // Disconnect/termination must abandon any transaction session owned by
        // the connection. This callback must not commit it or perform retries.
        disconnect: *const fn (*anyopaque, Identity, ?[]const u8) void,
    };
};

/// Result pages must be released before close or detach: native page arenas
/// may borrow the stream's shared memory admission and retained column metadata.
pub const ReadStream = struct {
    context: *anyopaque,
    columns: []const Column,
    next: *const fn (*anyopaque, std.mem.Allocator, Request, u32) anyerror!StreamPage,
    close: *const fn (*anyopaque) void,
    /// Release capture, execution admission and plan after bounded spooling,
    /// retaining only the authorization capsule until close.
    detach: ?*const fn (*anyopaque) void = null,
    /// Reauthenticate buffered rows against the original aliases/filter and
    /// namespace, without opening readers or reexecuting the statement.
    validate: ?*const fn (*anyopaque, std.mem.Allocator, Request) anyerror!void = null,
};
pub const StreamPage = struct { result: Result, exhausted: bool };
