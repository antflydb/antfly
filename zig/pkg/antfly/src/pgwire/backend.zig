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

//! Transport-independent, authenticated SQL boundary. The caller supplies an
//! arena for every describe/execute call; returned slices must use that arena.
//! Parameters remain typed values, never interpolated into SQL text.
const std = @import("std");

pub const Type = enum { string, integer, number, boolean, datetime, json, unknown };
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
    session_id: ?[]const u8 = null,
    limit: u32,
    io: std.Io,
    deadline: std.Io.Clock.Timestamp,
    cancel_requested: *const std.atomic.Value(bool),
    diagnostics: ?*Diagnostic = null,
    /// Backend-owned opaque Parse-time identity fence, copied into each portal.
    binding_guard: ?[]const u8 = null,

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
};

pub const Result = struct {
    columns: []const Column = &.{},
    rows: []const []const std.json.Value = &.{},
    sql_nulls: ?[]const []const bool = null,
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
        /// Optional owned read-only pull execution. Null declines a blocking
        /// shape before execution. Each page is independently owned; release it
        /// before the next pull or closing the stream. Never use for mutations.
        open_stream: ?*const fn (*anyopaque, std.mem.Allocator, Identity, Request) anyerror!?ReadStream = null,
        // Disconnect/termination must abandon any transaction session owned by
        // the connection. This callback must not commit it or perform retries.
        disconnect: *const fn (*anyopaque, Identity, ?[]const u8) void,
    };
};

pub const ReadStream = struct {
    context: *anyopaque,
    columns: []const Column,
    next: *const fn (*anyopaque, std.mem.Allocator, Request, u32) anyerror!StreamPage,
    close: *const fn (*anyopaque) void,
};
pub const StreamPage = struct { result: Result, exhausted: bool };
