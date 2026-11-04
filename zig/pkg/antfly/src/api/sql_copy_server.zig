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

//! Server-side COPY uses the same typed decoder/batcher, native SQL owner,
//! transaction coordinator and read delivery as client COPY. External programs
//! are invoked once; their successful completion precedes SQL acknowledgement.
const std = @import("std");
const local = @import("antfly_local_sources");
const ast = local.sql_ast;
const compiler = local.sql_compiler;
const runtime = local.sql_runtime;
const catalog = local.sql_catalog;
const wire = @import("../pgwire/backend.zig");
const http = @import("http_server.zig");
const Adapter = @import("sql_execution.zig").Adapter;
const Endpoint = @import("sql_copy_io.zig").Endpoint;
const Batch = @import("sql_copy_input_batch.zig").Batch;

pub fn target(statement: ast.Statement) ?ast.CopyTarget {
    return switch (statement) {
        .copy_to => |command| command.target,
        .copy_from => |command| command.target,
        else => null,
    };
}

pub fn execute(adapter: *Adapter, alloc: std.mem.Allocator, compiled: *const compiler.Compiled, parameters: []const std.json.Value, limits: runtime.Limits, backend: ?catalog.Backend) anyerror!runtime.Result {
    try adapter.context.ensureActive();
    // A table grant cannot authorize reading arbitrary server files or running
    // shell commands. Require a fresh global administrative credential grant.
    if (adapter.server.cfg.auth_enabled and adapter.identity.* == null) return error.Forbidden;
    if (!try http.resourcePermissionCurrentlyAllowed(adapter.identity.*, .@"*", "*", .admin)) return error.Forbidden;
    const io = adapter.server.sharedApiFilesystemIo() orelse return error.UnsupportedSqlExecution;
    var state: State = .{ .adapter = adapter, .alloc = alloc, .compiled = compiled, .parameters = parameters, .limits = limits, .backend = backend, .io = io, .context = adapter.context };
    const Event = union(enum) { done: void, canceled: anyerror };
    var events: [2]Event = undefined;
    var selection: std.Io.Select(Event) = .init(io, &events);
    var transferred = false;
    defer {
        selection.cancelDiscard();
        if (!transferred) if (state.result) |*result| result.deinit();
    }
    // Reserve cancellation concurrency before any external producer can run.
    try selection.concurrent(.canceled, State.watch, .{&state});
    try selection.concurrent(.done, State.run, .{&state});
    const event = selection.await() catch |err| {
        selection.cancelDiscard();
        if (state.failure) |failure| return queryError(failure);
        if (state.result) |result| {
            transferred = true;
            return result;
        }
        return queryError(err);
    };
    selection.cancelDiscard();
    if (state.failure) |err| return queryError(err);
    if (state.result) |result| {
        transferred = true;
        return result;
    }
    return switch (event) {
        .canceled => |err| queryError(err),
        .done => error.InvalidSqlBackendResponse,
    };
}

const State = struct {
    adapter: *Adapter,
    alloc: std.mem.Allocator,
    compiled: *const compiler.Compiled,
    parameters: []const std.json.Value,
    limits: runtime.Limits,
    backend: ?catalog.Backend,
    io: std.Io,
    context: local.api_operation.RequestContext,
    failure: ?anyerror = null,
    result: ?runtime.Result = null,
    session: ?[32]u8 = null,
    transaction_started: bool = false,
    transaction_implicit: bool = false,
    external_started: bool = false,

    fn watch(self: *State) anyerror {
        // The callback holds the immutable original context, not the adapter's
        // temporary policy/read scopes installed by inner SQL execution.
        const context = self.context;
        while (true) {
            context.ensureActive() catch |err| return err;
            self.io.sleep(.fromMilliseconds(10), .awake) catch |err| return err;
        }
    }
    fn run(self: *State) void {
        self.result = self.runInner() catch |err| {
            if (!unknown(err) and (self.transaction_started or self.session != null)) self.cleanup(self.transaction_started and self.transaction_implicit);
            self.failure = if (self.external_started and !unknown(err) and (local.sql_errors.describe(err).retryable orelse false)) error.SqlCopyExternalFailure else err;
            return;
        };
    }
    fn runInner(self: *State) anyerror!runtime.Result {
        const original_session = self.adapter.session_id;
        defer self.adapter.session_id = original_session;
        if (original_session) |id| {
            if (id.len != 32) return error.SqlTransactionNotActive;
            self.session = id[0..32].*;
        }
        const original_clock = self.adapter.statement_instant;
        self.adapter.statement_instant = try (catalog.Clock.Source{ .io = self.io }).sample();
        defer self.adapter.statement_instant = original_clock;
        if (self.compiled.statement == .copy_from) return self.input();
        return self.output();
    }
    fn executeCompiled(self: *State, alloc: std.mem.Allocator, compiled: *const compiler.Compiled, parameters: []const std.json.Value, limits: runtime.Limits, descriptors: []const wire.Parameter) anyerror!runtime.Result {
        self.adapter.session_id = if (self.session) |*id| id else null;
        var backend = self.backend orelse self.adapter.backend();
        backend.parameter_descriptor_hints = descriptors;
        backend.copy_filter = self.adapter.copy_filter;
        const result = try self.adapter.execute(alloc, compiled, parameters, limits, backend);
        self.session = self.adapter.result_session_id;
        return result;
    }
    fn control(self: *State, sql: []const u8) anyerror!runtime.Result {
        var compiled = try compiler.compile(self.alloc, sql, .{});
        defer compiled.deinit();
        return self.executeCompiled(self.alloc, &compiled, &.{}, .{}, &.{});
    }
    fn cleanup(self: *State, implicit: bool) void {
        if (self.session == null) return;
        const prior_protection = self.io.swapCancelProtection(.blocked);
        defer _ = self.io.swapCancelProtection(prior_protection);
        const prior_context = self.adapter.context;
        defer self.adapter.context = prior_context;
        self.adapter.context.cancellation = .none;
        self.adapter.context.deadline_io = null;
        self.adapter.context.deadline_ns = @import("antfly_platform").time.monotonicNs() +| 5 * std.time.ns_per_s;
        if (implicit) {
            var result = self.control("ROLLBACK") catch return;
            result.deinit();
        } else {
            var coordinator: @import("sql_session.zig").Coordinator = .{ .server = self.adapter.server, .identity = self.adapter.identity, .context = self.adapter.context };
            @import("sql_session.zig").Coordinator.failed(&coordinator, .{ .principal = http.transactionPrincipal(self.adapter.identity.*) orelse "", .database = self.adapter.database, .namespace = self.adapter.session_namespace orelse self.adapter.namespace }, @import("distributed_txn.zig").parseTxnIdHex(&self.session.?) catch return) catch {};
            self.adapter.transaction_status = .failed;
        }
    }
    fn input(self: *State) anyerror!runtime.Result {
        const command = self.compiled.statement.copy_from;
        var arena: std.heap.ArenaAllocator = .init(self.alloc);
        defer arena.deinit();
        const a = arena.allocator();
        const old_copy = self.adapter.copy_input;
        const old_filter = self.adapter.copy_filter;
        self.adapter.copy_input = true;
        defer self.adapter.copy_input = old_copy;
        defer self.adapter.copy_filter = old_filter;
        const backend = self.backend orelse self.adapter.backend();
        const lowered = try local.sql_describe.copyWrite(a, backend, self.compiled);
        var description = try local.sql_describe.describe(self.alloc, lowered.binding_backend, &lowered.compiled, &.{});
        defer description.deinit();
        const table = description.binding.table orelse return error.InvalidSqlBackendResponse;
        const columns = try a.alloc(wire.Column, lowered.compiled.statement.insert.columns.len);
        for (columns, lowered.compiled.statement.insert.columns) |*out, name| {
            const column = try table.column(name);
            if (column.generated) return error.SqlGeneratedColumnWrite;
            out.* = columnWire(column);
            out.defaulted = column.defaulted;
        }
        if (columns.len == 0 or columns.len > std.math.maxInt(u16)) return error.SqlProgramLimitExceeded;
        const filter_descriptors = if (description.binding.copy_filter) |filter| filter.parameter_descriptors else &.{};
        if (self.parameters.len != filter_descriptors.len) return error.InvalidSqlParameters;
        const inputs = try a.alloc(local.sql_parameter_frame.Input, self.parameters.len);
        for (inputs, self.parameters, 0..) |*out, value, i| out.* = .{ .datum = if (self.limits.parameter_cells) |cells| cells[i] else local.sql_scalar.Datum.fromJson(value) };
        self.adapter.copy_filter = if (command.predicate) |predicate| .{ .predicate = predicate, .parameter_count = self.compiled.parameter_count, .inputs = inputs, .descriptors = filter_descriptors } else null;
        const implicit = self.session == null;
        if (implicit) {
            var begun = try self.control("BEGIN");
            begun.deinit();
        }
        self.transaction_started = true;
        self.transaction_implicit = implicit;
        var source = try Endpoint.open(self.io, command.target.?, true);
        self.external_started = command.target.? == .program;
        defer source.deinit();
        var decoder = try @import("../pgwire/copy_in.zig").Decoder.init(self.alloc, command.options, columns.len, 16 * 1024 * 1024);
        defer decoder.deinit();
        const names = try a.alloc([]const u8, columns.len);
        for (names, columns) |*name, column| name.* = column.name;
        try decoder.setColumns(names);
        var batch: Batch = .{ .alloc = self.alloc, .driver = .{ .ptr = self, .check = checkpoint, .execute = insert, .sqlstate = sqlstate, .notice = notice }, .description = .{ .columns = &.{}, .copy_in = .{ .table = command.table, .columns = columns, .options = command.options, .source_statement = if (command.predicate != null) "COPY" else null } }, .arena = .init(self.alloc), .row_arena = .init(self.alloc), .max_rows = @min(128, std.math.maxInt(u16) / columns.len) };
        defer batch.deinit();
        try batch.initColumns(a);
        self.consume(&source, &decoder, &batch) catch |err| {
            if (!batch.admission_started and std.mem.startsWith(u8, sqlstate(err), "22")) try batch.flush();
            return err;
        };
        try batch.flush();
        try batch.report();
        // A producer which emitted valid rows and then failed cannot commit.
        try source.finish();
        var result = try runtime.Result.empty(self.alloc, "COPY");
        errdefer result.deinit();
        result.output.rows_affected = batch.total;
        if (implicit) {
            // Once commit starts, use the coordinator's exact outcome. Never
            // roll it back or rerun a program after an ambiguous admission.
            var committed = try self.control("COMMIT");
            self.transaction_started = false;
            defer committed.deinit();
            result.output.mutation_outcome = committed.output.mutation_outcome;
        }
        return result;
    }
    fn consume(self: *State, source: *Endpoint, decoder: *@import("../pgwire/copy_in.zig").Decoder, batch: *Batch) !void {
        var buffer: [64 * 1024]u8 = undefined;
        while (true) {
            try checkpoint(self);
            const n = try source.read(&buffer);
            if (n == 0) break;
            var remaining: []const u8 = buffer[0..n];
            while (try decoder.next(&remaining)) |row| try batch.row(row, decoder.defaults.items);
        }
        if (try decoder.finish()) |row| try batch.row(row, decoder.defaults.items);
    }
    fn checkpoint(raw: *anyopaque) anyerror!void {
        const self: *State = @ptrCast(@alignCast(raw));
        try self.io.checkCancel();
        try self.adapter.context.ensureActive();
    }
    fn sqlstate(err: anyerror) []const u8 {
        return local.sql_errors.describe(err).code;
    }
    fn notice(raw: *anyopaque, text: []const u8) anyerror!void {
        const self: *State = @ptrCast(@alignCast(raw));
        if (self.adapter.notice_sink) |sink| try sink.emit(sink.ptr, .{ .message = text });
    }
    fn insert(raw: *anyopaque, a: std.mem.Allocator, sql: []const u8, parameters: []const std.json.Value, _: []const wire.Type, descriptors: []const wire.Parameter, cells: []const local.sql_scalar.Datum, _: ?[]const u8, _: ?u64) anyerror!wire.Result {
        const self: *State = @ptrCast(@alignCast(raw));
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        var limits = self.limits;
        limits.parameter_cells = cells;
        limits.parameter_owner = null;
        var result = try self.executeCompiled(a, &compiled, parameters, limits, descriptors);
        defer result.deinit();
        return .{ .command_tag = "INSERT", .rows_affected = result.output.rows_affected, .transaction_status = @fromBackingInt(@backingInt(self.adapter.transaction_status)) };
    }
    fn output(self: *State) anyerror!runtime.Result {
        // Resolve and authorize before creating a file or spawning a program.
        const backend = self.backend orelse self.adapter.backend();
        var arena: std.heap.ArenaAllocator = .init(self.alloc);
        defer arena.deinit();
        const lowered = try local.sql_describe.copyRead(arena.allocator(), backend, self.compiled);
        {
            var description = try local.sql_describe.describe(self.alloc, lowered.binding_backend, &lowered.compiled, &.{});
            defer description.deinit();
        }
        const implicit = self.session == null;
        if (implicit) {
            var begun = try self.control("BEGIN");
            begun.deinit();
        }
        self.transaction_started = true;
        self.transaction_implicit = implicit;
        var sink: Output = .{ .state = self, .options = self.compiled.statement.copy_to.options };
        defer sink.deinit();
        const prior_delivery = self.adapter.read_delivery;
        self.adapter.read_delivery = .{ .ptr = &sink, .deliver = Output.deliver, .in_transaction = true };
        defer self.adapter.read_delivery = prior_delivery;
        var limits = self.limits;
        limits.result_rows = std.math.maxInt(usize);
        // Preserve COPY's unbounded, spill-backed result contract. Lowering
        // the command to SELECT would choose its bounded constant-result
        // fallback and reject COPY's deliberately unlimited output row count.
        var read = self.compiled.*;
        read.statement.copy_to.target = null;
        var rows = self.executeCompiled(self.alloc, &read, self.parameters, limits, backend.parameter_descriptor_hints) catch |err| {
            if (err == error.WriteFailed) return sink.writeError();
            return err;
        };
        defer rows.deinit();
        if (sink.encoder == null) try sink.write(rows.output);
        try sink.finish();
        var result = try runtime.Result.empty(self.alloc, "COPY");
        errdefer result.deinit();
        result.output.rows_affected = sink.count;
        if (implicit) {
            var committed = try self.control("COMMIT");
            defer committed.deinit();
            self.transaction_started = false;
        }
        return result;
    }
};

fn queryError(err: anyerror) anyerror {
    // The private I/O worker is canceled by a SQL deadline/CancelRequest.
    // Propagating Io.Canceled would close pgwire instead of emitting 57014.
    return switch (err) {
        error.Canceled, error.Cancelled, error.DeadlineExceeded, error.Timeout => error.QueryCanceled,
        else => err,
    };
}
fn unknown(err: anyerror) bool {
    return err == error.SqlMutationOutcomeUnknown or err == error.SqlTransactionOutcomeUnknown or err == error.SessionLeaseLost;
}
fn columnWire(column: anytype) wire.Column {
    return .{ .name = column.name, .type = switch (column.type) {
        inline else => |kind| @field(wire.Type, @tagName(kind)),
    }, .element_type = column.element_type, .numeric_modifier = column.numeric_modifier, .enum_view = column.enum_view, .domain_view = column.domain_view, .temporal_kind = column.temporal_kind };
}
const Output = struct {
    state: *State,
    destination: ?Endpoint = null,
    writer: ?std.Io.File.Writer = null,
    writer_buffer: [64 * 1024]u8 = undefined,
    options: ast.CopyOptions,
    arena: ?std.heap.ArenaAllocator = null,
    encoder: ?@import("../pgwire/copy_out.zig").Encoder = null,
    count: u64 = 0,
    fn deinit(self: *Output) void {
        if (self.encoder) |*encoder| encoder.deinit();
        if (self.arena) |*arena| arena.deinit();
        if (self.destination) |*destination| destination.deinit();
    }
    fn writeError(self: *Output) anyerror {
        return Endpoint.fileError(if (self.writer) |writer| writer.err orelse error.WriteFailed else error.WriteFailed);
    }
    fn finish(self: *Output) anyerror!void {
        try self.encoder.?.finish(&self.writer.?.interface);
        self.writer.?.interface.flush() catch return self.writeError();
        try self.destination.?.finish();
    }
    fn write(self: *Output, output: runtime.Output) anyerror!void {
        if (self.arena == null) self.arena = .init(self.state.alloc);
        const a = self.arena.?.allocator();
        if (self.encoder == null) {
            // Stream.open has acquired the native read/snapshot admission.
            // Failed admission must not truncate a file or launch a program.
            self.destination = try Endpoint.open(self.state.io, self.state.compiled.statement.copy_to.target.?, false);
            self.state.external_started = true;
            self.writer = self.destination.?.file.writerStreaming(self.state.io, &self.writer_buffer);
            const columns = try a.alloc(wire.Column, output.columns.len);
            for (columns, output.columns) |*out, column| out.* = columnWire(column);
            self.encoder = try @import("../pgwire/copy_out.zig").Encoder.init(self.state.alloc, a, try wire.cloneColumnsLeaky(a, columns), self.options, 16 * 1024 * 1024);
            try self.encoder.?.start(&self.writer.?.interface);
        }
        var scratch: std.heap.ArenaAllocator = .init(self.state.alloc);
        defer scratch.deinit();
        var cells: @import("sql_wire_cells.zig").OutputCells = .{ .output = output };
        const result: wire.Result = .{ .columns = self.encoder.?.columns, .rows = if (output.native_rows == null) output.rows else &.{}, .sql_nulls = if (output.native_rows == null) output.sql_nulls else null, .cells = if (output.native_rows != null) .{ .context = &cells, .count = output.rowCount(), .width = output.columns.len, .read = @import("sql_wire_cells.zig").OutputCells.read } else null, .command_tag = "SELECT" };
        for (0..output.rowCount()) |i| {
            try State.checkpoint(self.state);
            _ = scratch.reset(.retain_capacity);
            try self.encoder.?.row(scratch.allocator(), &self.writer.?.interface, result, i);
        }
        self.count = std.math.add(u64, self.count, output.rowCount()) catch return error.SqlProgramLimitExceeded;
    }
    fn deliver(raw: *anyopaque, a: std.mem.Allocator, stream: *local.sql_read_stream.Stream) anyerror!runtime.Result {
        const self: *Output = @ptrCast(@alignCast(raw));
        while (true) {
            var page = try stream.next(256);
            defer page.deinit();
            try self.write(page.output);
            if (page.exhausted) break;
        }
        return runtime.Result.empty(a, "SELECT");
    }
};
