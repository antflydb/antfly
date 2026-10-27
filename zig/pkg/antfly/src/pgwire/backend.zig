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

//! Transport-independent, authenticated SQL boundary. The caller supplies an
//! arena for every describe/execute call; returned slices must use that arena.
//! Parameters remain typed values, never interpolated into SQL text.
const std = @import("std");

pub const Type = enum { string, uuid, integer, number, boolean, datetime, json, array, unknown, numrange, daterange, tsrange, tstzrange };
pub const Parameter = @import("antfly_local_sources").sql_scalar.Type;
pub const Column = struct {
    defaulted: bool = false,
    name: []const u8,
    type: Type,
    temporal_kind: ?@import("antfly_local_sources").sql_temporal_value.Kind = null,
    element_type: ?@import("antfly_local_sources").sql_array_value.ElementType = null,
    numeric_modifier: ?@import("antfly_local_sources").sql_scalar.NumericModifier = null,
};
/// Physical output identity shared by prepared plans, stream pages and cursor
/// spools. Names may change under cached-plan rebinding; wire types may not.
pub fn sameColumnType(a: Column, b: Column) bool {
    return a.type == b.type and a.element_type == b.element_type and
        (a.type != .datetime or (a.temporal_kind orelse .timestamp_tz) == (b.temporal_kind orelse .timestamp_tz)) and
        @import("antfly_local_sources").sql_scalar.NumericModifier.eql(a.numeric_modifier, b.numeric_modifier);
}

pub const TransactionStatus = enum(u8) { idle = 'I', in_transaction = 'T', failed = 'E' };

pub const Identity = struct {
    context: *anyopaque,
    release: *const fn (*anyopaque, std.mem.Allocator) void,
};

pub const Request = struct {
    statement: []const u8,
    /// Internal ingress intent: preserve PostgreSQL COPY/RLS admission on every batch.
    copy_input: bool = false,
    statement_instant: ?@import("antfly_local_sources").sql_catalog.Clock.Instant = null,
    /// Reclaiming allocator sharing the connection cap. Result envelopes use
    /// the describe/execute arena; backend-owned execution scratch/results can
    /// release memory independently instead of nesting under that arena.
    scratch_allocator: ?std.mem.Allocator = null,
    parameters: []const std.json.Value = &.{},
    parameter_cells: ?[]const @import("antfly_local_sources").sql_scalar.Datum = null,
    parameter_owner: ?@import("antfly_local_sources").sql_parameter_frame.PayloadLease = null,
    parameter_types: []const Type = &.{},
    parameter_descriptors: []const Parameter = &.{},
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
    read_requirements: @import("session_commands.zig").ReadRequirements = .{},
    /// Borrowed only during admission; streaming owners capture the deadline
    /// before returning so later statements can reuse the connection budget.
    read_receipt_budget: ?*@import("antfly_local_sources").sql_read_requirements.WaitBudget = null,
    setting_overlay: []const @import("antfly_local_sources").sql_setting_catalog.OverlayEntry = &.{},
    sequence_state: ?@import("antfly_local_sources").sql_sequence_session.Borrowed = null,
    setting_baseline: ?[]const @import("antfly_local_sources").sql_setting_catalog.OverlayEntry = null,

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
    /// Authoritative binder approval for DECLARE's query body. Result columns
    /// alone cannot distinguish SELECT from a mutation with RETURNING.
    cursor_readable: bool = false,
    copy_out: ?@import("antfly_local_sources").sql_ast.CopyOptions = null,
    copy_in: ?CopyInput = null,
    columns: []const Column = &.{},
    parameter_types: []const Type = &.{},
    parameter_descriptors: []const Parameter = &.{},
    binding_guard: ?[]const u8 = null,
    setting_epoch: ?u64 = null,
};

pub const CopyInput = struct {
    table: @import("antfly_local_sources").sql_ast.Name,
    columns: []const Column,
    options: @import("antfly_local_sources").sql_ast.CopyOptions,
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
    catalog_receipt_json: ?[]const u8 = null,
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

pub const Cell = struct {
    /// Encoded JSON owned by the result lease; pgwire normalizes SQL JSONB
    /// output rather than assuming API JSON is PostgreSQL canonical text.
    /// JSON null remains a non-NULL value.
    json_bytes: ?[]const u8 = null,
    /// Validated immutable tokens borrow the result lease; pgwire streams them
    /// directly into DataRow frames without an intermediate JSON string.
    frozen_json: ?@import("antfly_local_sources").sql_execution_batch.FrozenJson = null,
    value: std.json.Value = .null,
    sql_null: bool,
    array: ?*const @import("antfly_local_sources").sql_array_value.Value = null,
    numeric: ?*const @import("antfly_local_sources").sql_numeric_value.Value = null,
    range: ?*const @import("antfly_local_sources").sql_range_value.Value = null,
};
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
        native_parameters: bool = false,
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
        evaluate_parameter_cells: ?*const fn (*anyopaque, std.mem.Allocator, Identity, Request, []const []const u8) anyerror![]const @import("antfly_local_sources").sql_scalar.Datum = null,
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
    copy_out: ?@import("antfly_local_sources").sql_ast.CopyOptions = null,
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
