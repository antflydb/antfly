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
pub const replication_commands = @import("replication_commands.zig");
pub const pgoutput = @import("pgoutput.zig");

const sql_session = @import("antfly_local_sources").sql_session;
const callback_abi = @import("antfly_local_sources").runtime_callback_abi;
pub const TransactionOptions = struct {
    isolation: ?sql_session.Isolation = null,
    mode: ?sql_session.ReadMode = null,
};

pub const Type = enum { string, uuid, integer, number, boolean, datetime, json, array, unknown, numrange, daterange, tsrange, tstzrange, enumeration, bytea, void };
pub const Parameter = @import("antfly_local_sources").sql_scalar.Type;
pub const Column = struct {
    defaulted: bool = false,
    name: []const u8,
    type: Type,
    temporal_kind: ?@import("antfly_local_sources").sql_temporal_value.Kind = null,
    element_type: ?@import("antfly_local_sources").sql_array_value.ElementType = null,
    numeric_modifier: ?@import("antfly_local_sources").sql_scalar.NumericModifier = null,
    domain_view: ?*const @import("antfly_local_sources").sql_catalog.domain_values.View = null,
    enum_view: ?*const @import("antfly_local_sources").sql_scalar.EnumValues.View = null,
};
/// Metadata survives producer/statement retirement; views are immutable and
/// deduplicated inside the caller's owning arena.
pub fn cloneColumnsLeaky(a: std.mem.Allocator, input: []const Column) ![]Column {
    const output = try a.dupe(Column, input);
    var views: std.AutoHashMapUnmanaged(*const @import("antfly_local_sources").sql_scalar.EnumValues.View, *const @import("antfly_local_sources").sql_scalar.EnumValues.View) = .empty;
    var domains: @import("antfly_local_sources").sql_scalar.DomainViews = .empty;
    for (output) |*column| {
        column.name = try a.dupe(u8, column.name);
        column.domain_view = try @import("antfly_local_sources").sql_scalar.ownDomainView(a, &domains, column.domain_view);
        column.enum_view = try @import("antfly_local_sources").sql_scalar.ownEnumView(a, &views, column.enum_view);
    }
    return output;
}
/// Physical output identity shared by prepared plans, stream pages and cursor
/// spools. Names may change under cached-plan rebinding; wire types may not.
pub fn sameColumnType(a: Column, b: Column) bool {
    return a.type == b.type and a.element_type == b.element_type and
        (a.type != .datetime or (a.temporal_kind orelse .timestamp_tz) == (b.temporal_kind orelse .timestamp_tz)) and
        @import("antfly_local_sources").sql_scalar.NumericModifier.eql(a.numeric_modifier, b.numeric_modifier) and
        @import("antfly_local_sources").sql_scalar.sameEnumDomain(a.enum_view, b.enum_view) and
        @import("antfly_local_sources").sql_scalar.sameDomain(a.domain_view, b.domain_view);
}

pub const TransactionStatus = enum(u8) { idle = 'I', in_transaction = 'T', failed = 'E' };

pub const Identity = struct {
    renewable: bool = false,
    context: *anyopaque,
    release: *const fn (*anyopaque, std.mem.Allocator) void,
};

pub const SelinuxIngress = @import("antfly_local_sources").system_catalog_domain.security_labels.KernelRelabel.Ingress;

pub const CursorCapture = enum { none, if_select, require_select };

pub const Request = struct {
    /// Borrowed from the accepted transport for this request only.
    selinux_ingress: ?SelinuxIngress = null,
    backend_pid: i32 = 0,
    capture_cursor_authority: CursorCapture = .none,
    completion: ?*@import("completion.zig").Completion = null,
    completion_allocator: ?std.mem.Allocator = null,
    statement: []const u8,
    /// Internal ingress intent: preserve PostgreSQL COPY/RLS admission on every batch.
    copy_input: bool = false,
    copy_freeze_admission: bool = false,
    copy_has_active_cursor: bool = false,
    copy_source_statement: ?[]const u8 = null,
    copy_source_inputs: []const @import("antfly_local_sources").sql_parameter_frame.Input = &.{},
    copy_source_descriptors: []const Parameter = &.{},
    statement_instant: ?@import("antfly_local_sources").sql_statement_clock.Instant = null,
    /// Reclaiming allocator sharing the connection cap. Result envelopes use
    /// the describe/execute arena; backend-owned execution scratch/results can
    /// release memory independently instead of nesting under that arena.
    scratch_allocator: ?std.mem.Allocator = null,
    parameters: []const std.json.Value = &.{},
    parameter_cells: ?[]const @import("antfly_local_sources").sql_scalar.Datum = null,
    parameter_owner: ?@import("antfly_local_sources").sql_parameter_frame.PayloadLease = null,
    parameter_types: []const Type = &.{},
    /// Raw PostgreSQL OIDs are resolved by the authenticated backend before binding.
    parameter_oids: []const u32 = &.{},
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
pub const notices = @import("antfly_local_sources").sql_notices;
pub const Notice = notices.Notice;
pub const Diagnostic = struct {
    notice_allocator: ?std.mem.Allocator = null,
    notices_buffer: ?*notices.Buffer = null,
    code: ?[5]u8 = null,
    message: [256]u8 = undefined,
    message_len: u16 = 0,
    transaction_id: ?[32]u8 = null,
    retryable: ?bool = null,
    /// Authoritative session state after a statement failure (for example a
    /// COMMIT conflict ends the transaction, unlike an ordinary failed query).
    transaction_status: ?TransactionStatus = null,

    pub fn clearNotices(self: *Diagnostic) void {
        if (self.notices_buffer) |buffer| self.notice_allocator.?.destroy(buffer);
        self.notices_buffer = null;
    }
    pub fn reserveNotices(raw: *anyopaque, count: usize, bytes: usize) !void {
        const self: *Diagnostic = @ptrCast(@alignCast(raw));
        const a = self.notice_allocator orelse return error.InvalidSqlBackendResponse;
        if (self.notices_buffer == null) {
            self.notices_buffer = try a.create(notices.Buffer);
            self.notices_buffer.?.* = .{};
        }
        try self.notices_buffer.?.reserve(count, bytes);
    }
    pub fn emitNotice(raw: *anyopaque, notice: notices.Notice) !void {
        const self: *Diagnostic = @ptrCast(@alignCast(raw));
        const a = self.notice_allocator orelse return error.InvalidSqlBackendResponse;
        if (self.notices_buffer == null) {
            self.notices_buffer = try a.create(notices.Buffer);
            self.notices_buffer.?.* = .{};
        }
        try self.notices_buffer.?.append(notice);
    }

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

pub const CatalogStamp = struct {
    catalog_revision: u64,
    relation_epoch: @import("antfly_local_sources").system_catalog_relation_reconciliation.Epoch,
};

pub const DescriptionStamp = struct {
    /// Null when the binder consulted no catalog objects.
    catalog: ?CatalogStamp = null,
    authority: u64,
    reusable: bool = true,
};

pub const CursorAuthority = struct {
    ptr: *anyopaque,
    validate: *const fn (*anyopaque, std.mem.Allocator, Identity, Request, bool) anyerror!void,
};
pub const Description = struct {
    /// Authoritative binder approval for DECLARE's query body. Result columns
    /// alone cannot distinguish SELECT from a mutation with RETURNING.
    cursor_readable: bool = false,
    copy_out: ?@import("antfly_local_sources").sql_ast.CopyOptions = null,
    copy_in: ?CopyInput = null,
    is_select: bool = false,
    stamp: ?DescriptionStamp = null,
    columns: []const Column = &.{},
    parameter_types: []const Type = &.{},
    parameter_descriptors: []const Parameter = &.{},
    binding_guard: ?[]const u8 = null,
    setting_epoch: ?u64 = null,
};

pub const CopyInput = struct {
    freeze_admitted: bool = false,
    source_statement: ?[]const u8 = null,
    table: @import("antfly_local_sources").sql_ast.Name,
    columns: []const Column,
    options: @import("antfly_local_sources").sql_ast.CopyOptions,

    pub fn cloneLeaky(self: CopyInput, a: std.mem.Allocator) !CopyInput {
        return .{
            .freeze_admitted = self.freeze_admitted,
            .source_statement = if (self.source_statement) |sql| try a.dupe(u8, sql) else null,
            .table = .{
                .database = if (self.table.database) |name| try a.dupe(u8, name) else null,
                .namespace = if (self.table.namespace) |name| try a.dupe(u8, name) else null,
                .table = try a.dupe(u8, self.table.table),
            },
            .columns = try cloneColumnsLeaky(a, self.columns),
            .options = try self.options.clone(a),
        };
    }
};

pub fn cloneParametersLeaky(a: std.mem.Allocator, input: []const Parameter) ![]Parameter {
    const scalar = @import("antfly_local_sources").sql_scalar;
    const output = try a.dupe(Parameter, input);
    var enums: std.AutoHashMapUnmanaged(*const scalar.EnumValues.View, *const scalar.EnumValues.View) = .empty;
    var domains: scalar.DomainViews = .empty;
    defer enums.deinit(a);
    defer domains.deinit(a);
    for (output) |*descriptor| {
        descriptor.enum_view = try scalar.ownEnumView(a, &enums, descriptor.enum_view);
        if (descriptor.enum_reference) |reference| {
            const owned = try a.create(scalar.EnumReference);
            owned.* = reference.*;
            descriptor.enum_reference = owned;
        }
        if (try scalar.ownDomainView(a, &domains, descriptor.domainView())) |view| {
            descriptor.domain_binding = &view.type_binding;
        } else if (descriptor.domain_binding) |binding| {
            const owned = try a.create(@TypeOf(binding.*));
            owned.* = binding.*;
            descriptor.domain_binding = owned;
        }
    }
    return output;
}

test "pgwire portal metadata survives release of the prepared arena" {
    var prepared = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer prepared.deinit();
    var portal = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer portal.deinit();
    const a = prepared.allocator();
    const scalar = @import("antfly_local_sources").sql_scalar;
    const reference = try a.create(scalar.EnumReference);
    reference.* = .{ .id = 17, .origin_id = @splat(1), .generation = 2 };
    const parameters = try cloneParametersLeaky(portal.allocator(), &.{.{ .kind = .enumeration, .enum_reference = reference }});
    const input: CopyInput = .{
        .source_statement = try a.dupe(u8, "COPY sample FROM STDIN"),
        .table = .{ .table = try a.dupe(u8, "sample") },
        .columns = &.{.{ .name = try a.dupe(u8, "value"), .type = .string }},
        .options = .{ .null_string = try a.dupe(u8, "missing") },
    };
    const copied = try input.cloneLeaky(portal.allocator());
    try std.testing.expect(parameters[0].enum_reference.? != reference);
    _ = prepared.reset(.free_all);
    try std.testing.expectEqual(@as(u64, 17), parameters[0].enum_reference.?.id);
    try std.testing.expectEqualStrings("COPY sample FROM STDIN", copied.source_statement.?);
    try std.testing.expectEqualStrings("sample", copied.table.table);
    try std.testing.expectEqualStrings("value", copied.columns[0].name);
    try std.testing.expectEqualStrings("missing", copied.options.null_string.?);
}

pub const Result = struct {
    /// Captured from the executed binding into the caller's cursor arena.
    cursor_authority: ?CursorAuthority = null,
    columns: []const Column = &.{},
    rows: []const []const std.json.Value = &.{},
    sql_nulls: ?[]const []const bool = null,
    /// Borrowed typed cells owned by this result's lease. A transport reads
    /// cells directly; scroll/hold stores clone only at their ownership edge.
    cells: ?Cells = null,
    rows_affected: u64 = 0,
    notices: []const Notice = &.{},
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
    enumeration: ?*const @import("antfly_local_sources").sql_scalar.EnumValues.Value = null,
    bytea: ?[]const u8 = null,
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
    replication_dispatch: ReplicationBoundary.Dispatch = ReplicationBoundary.local_dispatch,

    const ReplicationBoundary = callback_abi.Boundary(struct {
        open_replication_session: *const fn (*anyopaque, std.mem.Allocator, Identity, Request) anyerror!ReplicationSession,
    });

    pub fn openReplicationSession(self: Backend, a: std.mem.Allocator, identity: Identity, request: Request) !ReplicationSession {
        const open = self.vtable.open_replication_session orelse return error.UnsupportedReplicationMode;
        return ReplicationBoundary.callRemote("open_replication_session", self.replication_dispatch, open, .{ self.context, a, identity, request });
    }

    pub const VTable = struct {
        native_parameters: bool = false,
        open_replication: ?*const fn (*anyopaque, std.mem.Allocator, Identity, Request) anyerror!@import("logical_replication_frontend.zig").Port = null,
        start_connection: ?*const fn (*anyopaque, std.mem.Allocator, Identity, Request) anyerror!void = null,
        discard_connection: ?*const fn (*anyopaque, std.mem.Allocator, Identity, Request) anyerror!void = null,
        has_notifications: ?*const fn (*anyopaque, Identity) bool = null,
        notifications: ?*const fn (*anyopaque, std.mem.Allocator, Identity, Request, @import("antfly_local_sources").sql_ast.notifications.DeliveryControl) anyerror!@import("antfly_local_sources").sql_ast.notifications.Page = null,
        // Called even when a deployment permits anonymous access. The backend
        // alone decides whether credentials authorize a principal; a startup
        // username/database never itself grants access.
        authenticate: *const fn (*anyopaque, std.mem.Allocator, []const u8, []const u8) anyerror!Identity,
        /// Private gateway renewal; never available to native password sessions.
        /// Called serially between protocol operations, after backend work joins.
        refresh: ?*const fn (*anyopaque, std.mem.Allocator, Identity, []const u8) anyerror!void = null,
        describe: *const fn (*anyopaque, std.mem.Allocator, Identity, Request) anyerror!Description,
        /// Validate the exact catalog and principal used to describe a statement.
        description_current: ?*const fn (*anyopaque, std.mem.Allocator, Identity, Request, Description) anyerror!bool = null,
        /// Compare resolved targets across compatible schema versions of a bound portal.
        same_binding_targets: ?*const fn ([]const u8, []const u8) bool = null,
        execute: *const fn (*anyopaque, std.mem.Allocator, Identity, Request) anyerror!Result,
        /// Attach a qualified cut to the existing SQL transaction. The owner
        /// checks isolation, prior transaction work and import eligibility.
        import_snapshot: ?*const fn (*anyopaque, std.mem.Allocator, Identity, Request, []const u8) anyerror!void = null,
        /// Change transaction characteristics before any readings or writes.
        set_transaction_options: ?*const fn (*anyopaque, Identity, Request, TransactionOptions) anyerror!void = null,
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
        // Called after disconnect, while the credential is still owned. False
        // retains external fencing for a durable decision handed to recovery.
        completion_ready: ?*const fn (*anyopaque) bool = null,
        reserve_completion: ?*const fn (*anyopaque, *@import("completion.zig").Completion, usize) anyerror!void = null,
        pin_completion: ?*const fn (*anyopaque, std.mem.Allocator, *@import("completion.zig").Completion) void = null,
        completion_safe: ?*const fn (*anyopaque, Identity) bool = null,
        /// Authenticate explicit replication privilege and runtime capability;
        /// a SQL identity or SELECT grant alone is insufficient. The returned
        /// owner borrows this connection's identity until close. Missing
        /// capability refuses logical startup, before any replication command.
        open_replication_session: ?*const fn (*anyopaque, std.mem.Allocator, Identity, Request) anyerror!ReplicationSession = null,
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

/// Connection-owned replication authority. Each operation must revalidate live
/// credential, immutable database/table scope and publication/slot ownership.
/// authorize runs at startup (null command), before execution and before reply.
/// Commands and results borrow their caller arena. close releases connection
/// leases/exported snapshots; durable recovery owns ambiguous metadata effects.
pub const ReplicationSession = struct {
    context: *anyopaque,
    authorize: *const fn (*anyopaque, std.mem.Allocator, Request, ?replication_commands.Command) anyerror!void,
    command: *const fn (*anyopaque, std.mem.Allocator, Request, replication_commands.Command) anyerror!Result,
    close: *const fn (*anyopaque) void,
    /// Absent until the owner can retain/spool complete projected transactions
    /// and map their serialized positions durably. Never opens a SQL cursor.
    open_stream: ?*const fn (*anyopaque, std.mem.Allocator, Request, replication_commands.Start) anyerror!ReplicationStream = null,
    boundary_dispatch: Boundary.Dispatch = Boundary.local_dispatch,

    const Boundary = callback_abi.Boundary(struct {
        authorize: @FieldType(ReplicationSession, "authorize"),
        command: @FieldType(ReplicationSession, "command"),
        open_stream: @typeInfo(@FieldType(ReplicationSession, "open_stream")).optional.child,
    });

    pub fn authorizeRequest(self: ReplicationSession, a: std.mem.Allocator, request: Request, command: ?replication_commands.Command) !void {
        return Boundary.callRemote("authorize", self.boundary_dispatch, self.authorize, .{ self.context, a, request, command });
    }

    pub fn executeCommand(self: ReplicationSession, a: std.mem.Allocator, request: Request, command: replication_commands.Command) !Result {
        return Boundary.callRemote("command", self.boundary_dispatch, self.command, .{ self.context, a, request, command });
    }

    pub fn openStream(self: ReplicationSession, a: std.mem.Allocator, request: Request, start: replication_commands.Start) !ReplicationStream {
        const open = self.open_stream orelse return error.UnsupportedReplicationMode;
        return Boundary.callRemote("open_stream", self.boundary_dispatch, open, .{ self.context, a, request, start });
    }
};

/// All methods run serially on the connection coordinator. poll returns a
/// bounded batch of whole pgoutput messages from durable committed spooling at
/// the exact requested byte frontier. It must not retain an entire transaction in
/// connection RAM or advance an acknowledgement. No-data returns immediately.
/// feedback validates the slot fence and maps only complete issued intervals
/// to durable acknowledgement. close runs after all reader/writer tasks join.
pub const ReplicationStream = struct {
    context: *anyopaque,
    initial_lsn: pgoutput.Lsn,
    /// Validated reconnect position; it can exceed initial_lsn when replay
    /// rewinds an interior requested LSN to the transaction's Begin.
    resume_lsn: pgoutput.Lsn,
    initial_end_lsn: pgoutput.Lsn,
    poll: *const fn (*anyopaque, std.mem.Allocator, Request, pgoutput.Lsn, u32) anyerror!ReplicationPoll,
    feedback: *const fn (*anyopaque, std.mem.Allocator, Request, pgoutput.Feedback) anyerror!void,
    close: *const fn (*anyopaque) void,
    boundary_dispatch: Boundary.Dispatch = Boundary.local_dispatch,

    const Boundary = callback_abi.Boundary(struct {
        poll: @FieldType(ReplicationStream, "poll"),
        feedback: @FieldType(ReplicationStream, "feedback"),
    });

    pub fn readMessage(self: ReplicationStream, a: std.mem.Allocator, request: Request, after: pgoutput.Lsn, max_bytes: u32) !ReplicationPoll {
        return Boundary.callRemote("poll", self.boundary_dispatch, self.poll, .{ self.context, a, request, after, max_bytes });
    }

    pub fn applyFeedback(self: ReplicationStream, a: std.mem.Allocator, request: Request, value: pgoutput.Feedback) !void {
        return Boundary.callRemote("feedback", self.boundary_dispatch, self.feedback, .{ self.context, a, request, value });
    }
};
pub const ReplicationChunk = struct { start_lsn: pgoutput.Lsn, bytes: []const u8 };
pub const ReplicationPoll = struct {
    end_lsn: pgoutput.Lsn,
    chunk: ?ReplicationChunk = null,
    chunks: []const ReplicationChunk = &.{},
    owner: ?struct { context: *anyopaque, release: *const fn (*anyopaque) void } = null,
    pub fn deinit(self: *@This()) void {
        if (self.owner) |owner| owner.release(owner.context);
        self.* = undefined;
    }
};

test "replication transport preserves deferred errors through boundary dispatch" {
    const Remote = struct {
        fn poll(_: *anyopaque, _: std.mem.Allocator, _: Request, _: pgoutput.Lsn, _: u32) anyerror!ReplicationPoll {
            return error.ReplicationReadDeferred;
        }
        fn feedback(_: *anyopaque, _: std.mem.Allocator, _: Request, _: pgoutput.Feedback) anyerror!void {
            return error.ReplicationFeedbackDeferred;
        }
        fn close(_: *anyopaque) void {}

        fn dispatch(
            contract: *const @import("antfly_runtime_abi").native_abi.CallContract,
            callback: *const anyopaque,
            args: *const anyopaque,
            output: ?*anyopaque,
        ) callconv(.c) @import("antfly_runtime_abi").error_abi.Status {
            return ReplicationStream.Boundary.local_dispatch(contract, callback, args, output);
        }
    };
    var context: u8 = 0;
    const stream: ReplicationStream = .{
        .context = &context,
        .initial_lsn = @fromBackingInt(1),
        .resume_lsn = @fromBackingInt(1),
        .initial_end_lsn = @fromBackingInt(1),
        .poll = Remote.poll,
        .feedback = Remote.feedback,
        .close = Remote.close,
        .boundary_dispatch = Remote.dispatch,
    };
    var canceled: std.atomic.Value(bool) = .init(false);
    const request: Request = .{
        .statement = "",
        .io = std.testing.io,
        .parameters = &.{},
        .parameter_types = &.{},
        .limit = 1,
        .deadline = .{ .raw = std.Io.Clock.awake.now(std.testing.io), .clock = .awake },
        .cancel_requested = &canceled,
    };
    try std.testing.expectError(error.ReplicationReadDeferred, stream.readMessage(std.testing.allocator, request, @fromBackingInt(1), 64));
    try std.testing.expectError(error.ReplicationFeedbackDeferred, stream.applyFeedback(std.testing.allocator, request, .{
        .written = @fromBackingInt(1),
        .flushed = @fromBackingInt(1),
        .applied = @fromBackingInt(1),
        .client_time = 0,
        .request_reply = false,
    }));
}
