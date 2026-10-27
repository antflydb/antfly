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

//! Transport-independent execution over the native catalog/read/write boundary.
//! No durable state, global apply lock, or protocol-specific types live here.
const std = @import("std");
const ast = @import("ast.zig");
const catalog = @import("catalog.zig");
const compiler = @import("compiler.zig");
const describe = @import("describe.zig");
const MemoryBudget = @import("memory_budget.zig");
const Json = std.json.Value;
const operators = @import("operators.zig");
const scalar = @import("scalar.zig");
const Datum = scalar.Datum;

test {
    _ = @import("explain.zig");
}

pub const resource_limits = @import("resource_limits.zig");

pub const Limits = struct {
    /// Internal transports opt into complete Datums; HTTP retains public JSON.
    native_result: bool = false,
    /// Optional execution-owned measurement sink, inherited by lowered plans.
    profile: ?*@import("execution_profile.zig").Profile = null,
    memory_accounting: ?*MemoryBudget = null,
    /// Prepared transport cells; execution takes one owned typed copy.
    parameter_cells: ?[]const Datum = null,
    parameter_owner: ?@import("parameter_frame.zig").PayloadLease = null,
    error_context: ?*@import("errors.zig").Context = null,
    result_rows: usize = 128,
    mutation_rows: usize = 4096,
    scan_rows: usize = 10_000_000,
    retained_bytes: usize = resource_limits.default_memory_bytes,
    page_rows: u32 = 256,
    /// Native execution batches are independent of response/decision pages.
    execution_batch_rows: u32 = 4096,
    page_bytes: usize = 256 * 1024,
    /// Native scans charge execution-sized logical row batches plus empty
    /// nonterminal pulls. Worker and row-group fragmentation do not add credit.
    scan_pages: usize = 65_536,
    spill_bytes: u64 = 1024 * 1024 * 1024,
    spill_root: []const u8 = "/tmp",
    pub fn workingMemory(self: Limits) !usize {
        const bytes = if (self.parameter_owner) |owner| owner.bytes else 0;
        if (self.parameter_owner != null and self.parameter_cells == null) return error.InvalidSqlParameters;
        if (bytes >= self.retained_bytes) return error.SqlWorkingMemoryLimitExceeded;
        return self.retained_bytes - bytes;
    }
    pub fn executionRows(self: Limits) u32 {
        return @intCast(@min(self.execution_batch_rows, @max(@as(usize, 1), self.retained_bytes / (16 * 1024))));
    }
};
pub const Column = describe.Column;
pub const Output = struct {
    columns: []const Column = &.{},
    rows: []const []const Json = &.{},
    native_rows: ?[]const []const Datum = null,

    /// Mirrors rows when present. JSON null is encoded as a value when false;
    /// true denotes SQL NULL independently of the column's logical type.
    sql_nulls: ?[]const []const bool = null,
    pattern_sources: ?[]const []const ?*@import("scalar.zig").PatternSet = null,
    rows_affected: u64 = 0,
    command_tag: []const u8,
    mutation_outcome: ?catalog.MutationOutcome = null,
    ddl_receipt: ?catalog.DdlReceipt = null,
    catalog_receipt: ?catalog.CatalogOperationReceipt = null,

    pub fn rowCount(self: Output) usize {
        return if (self.native_rows) |rows| rows.len else self.rows.len;
    }

    /// Internal pattern sources contain executable callbacks and have no wire
    /// representation. Keep the public result envelope explicit at every ABI.
    pub fn jsonStringify(self: Output, writer: anytype) !void {
        if (self.native_rows != null) return error.WriteFailed;
        try writer.write(.{ .columns = self.columns, .rows = self.rows, .sql_nulls = self.sql_nulls, .rows_affected = self.rows_affected, .command_tag = self.command_tag, .mutation_outcome = self.mutation_outcome, .ddl_receipt = self.ddl_receipt, .catalog_receipt = self.catalog_receipt });
    }
};
/// An execution-scoped allocator whose control state outlives returned rows or
/// a suspended stream. Owned execution entry points consume this capability on
/// every path; the owner is released after the last execution allocation.
pub const MemoryOwner = struct {
    allocator: std.mem.Allocator,
    pressure: ?MemoryBudget.Pressure = null,
    ptr: *anyopaque,
    release: *const fn (*anyopaque) void,

    pub fn deinit(self: MemoryOwner) void {
        self.release(self.ptr);
    }
    pub fn mapError(self: MemoryOwner, err: anyerror) anyerror {
        if (err == error.OutOfMemory) if (self.pressure) |pressure|
            if (pressure.exhausted(pressure.ptr)) return error.SqlWorkingMemoryLimitExceeded;
        return err;
    }
};

pub const Result = struct {
    state: *State,
    output: Output,

    const State = struct { budget: MemoryBudget, arena: std.heap.ArenaAllocator, regex_execution: @import("regex_execution.zig"), clock_values: ?catalog.Clock.Frame, owner: ?MemoryOwner };

    /// Includes retained result arena capacity and transient native page data.
    pub fn peakMemoryBytes(self: Result) usize {
        return self.state.budget.peak;
    }

    /// Transport adapters may normalize cells in an exclusively owned result
    /// before publishing it. SELECT cells are mutable result-arena allocations;
    /// callers must not mutate shared/published results or column metadata.
    pub fn mutableRow(self: *Result, index: usize) []Json {
        return @constCast(self.output.rows[index]);
    }

    pub fn deinit(self: *Result) void {
        const backing = self.state.budget.backing;
        const owner = self.state.owner;
        self.state.regex_execution.deinit();
        self.state.arena.deinit();
        std.debug.assert(self.state.budget.live == 0);
        backing.destroy(self.state);
        if (owner) |retained| retained.deinit();
        self.* = undefined;
    }

    pub fn empty(alloc: std.mem.Allocator, command_tag: []const u8) !Result {
        const state = try alloc.create(State);
        state.clock_values = null;
        state.owner = null;
        state.budget = .{ .backing = alloc, .limit = (Limits{}).retained_bytes };
        state.regex_execution = .init(state.budget.allocator(), 16 * 1024 * 1024);
        state.arena = std.heap.ArenaAllocator.init(state.budget.allocator());
        var result = Result{ .state = state, .output = .{ .command_tag = "" } };
        errdefer result.deinit();
        result.output.command_tag = try state.arena.allocator().dupe(u8, command_tag);
        return result;
    }

    pub fn singleText(alloc: std.mem.Allocator, command_tag: []const u8, column_name: []const u8, value: []const u8) !Result {
        var result = try empty(alloc, command_tag);
        errdefer result.deinit();
        const arena = result.state.arena.allocator();
        const columns = try arena.alloc(Column, 1);
        columns[0] = .{ .name = try arena.dupe(u8, column_name), .type = .string };
        const row = try arena.alloc(Json, 1);
        row[0] = .{ .string = try arena.dupe(u8, value) };
        const rows = try arena.alloc([]const Json, 1);
        rows[0] = row;
        result.output.columns = columns;
        result.output.rows = rows;
        return result;
    }
};

pub fn executeOwned(owner: MemoryOwner, backend: catalog.Backend, compiled: *const compiler.Compiled, parameters: []const Json, limits: Limits) !Result {
    var adopted = false;
    defer if (!adopted) owner.deinit();
    var result = executeInner(owner.allocator, backend, compiled, parameters, limits, owner) catch |err| return owner.mapError(err);
    result.state.owner = owner;
    adopted = true;
    return result;
}

pub fn execute(alloc: std.mem.Allocator, backend: catalog.Backend, compiled: *const compiler.Compiled, parameters: []const Json, limits: Limits) !Result {
    return executeInner(alloc, backend, compiled, parameters, limits, null);
}

fn executeInner(alloc: std.mem.Allocator, backend: catalog.Backend, compiled: *const compiler.Compiled, parameters: []const Json, limits: Limits, owner: ?MemoryOwner) !Result {
    if (limits.result_rows == 0 or limits.result_rows > 4096 or limits.mutation_rows == 0 or limits.mutation_rows > 4096 or
        limits.execution_batch_rows == 0 or limits.execution_batch_rows > 4096 or limits.page_rows == 0 or limits.page_rows > 4096 or limits.page_bytes == 0 or limits.scan_rows == 0) return error.InvalidSqlLimit;
    if (parameters.len != compiled.parameter_count) return error.InvalidSqlParameters;
    try backend.vtable.checkpoint(backend.ptr);
    var pinned_settings: ?@import("setting_catalog.zig").View = null;
    defer if (pinned_settings) |*view| view.deinit();
    var statement_backend = try backend.pinStatementClock();
    if (limits.error_context) |context| statement_backend.error_context = context;
    if (parameters.len > 1024) return error.InvalidSqlParameters;
    if (backend.setting_capture) |capture| {
        pinned_settings = try @import("setting_catalog.zig").View.capture(alloc, capture.owner, capture.scope, capture.overlay);
        try pinned_settings.?.pinSqlUser(capture.sql_user);
        statement_backend.settings_view = &pinned_settings.?;
    }
    const memory = try limits.workingMemory();
    const state = try alloc.create(Result.State);
    errdefer alloc.destroy(state);
    state.clock_values = if (statement_backend.statement_clock) |view| try catalog.Clock.Frame.init(view) else null;
    state.owner = null;
    state.budget = .{ .backing = alloc, .limit = memory, .pressure = if (owner) |retained| retained.pressure else null };
    state.regex_execution = .init(state.budget.allocator(), @min(16 * 1024 * 1024, limits.retained_bytes));
    statement_backend.regex_execution = &state.regex_execution;
    state.arena = std.heap.ArenaAllocator.init(state.budget.allocator());
    var result = Result{ .state = state, .output = undefined };
    errdefer {
        state.regex_execution.deinit();
        state.arena.deinit();
        std.debug.assert(state.budget.live == 0);
    }
    statement_backend.clock_values = if (state.clock_values) |*frame| frame else null;
    const arena = state.arena.allocator();
    statement_backend.parameter_fallback_types = try parameterFallbackTypesNative(arena, parameters, limits.parameter_cells);
    var execution_limits = limits;
    execution_limits.memory_accounting = &state.budget;
    result.output = runBound(state.budget.allocator(), arena, statement_backend, compiled, parameters, execution_limits, null) catch |err| {
        if (err == error.OutOfMemory and state.budget.isExhausted()) return error.SqlWorkingMemoryLimitExceeded;
        return err;
    };
    return result;
}

/// Execution-only transport hints fill polymorphic holes after SQL constraints
/// converge. Prepare/Describe without values must not invent these types.
/// The returned metadata belongs to the statement or cursor's owned arena.
pub fn parameterFallbackTypesNative(arena: std.mem.Allocator, parameters: []const Json, cells: ?[]const Datum) ![]const ?ast.ColumnType {
    const native = cells orelse return parameterFallbackTypes(arena, parameters);
    if (native.len != parameters.len) return error.InvalidSqlParameters;
    const kinds = try arena.alloc(?ast.ColumnType, native.len);
    for (native, kinds) |cell, *kind| kind.* = if (cell.sql_null) null else if (cell.array != null) .array else if (cell.numeric != null) .number else if (cell.range()) |range| @import("range_value.zig").typeFromKind(ast.ColumnType, range.kind) else parameterFallbackType(cell.value);
    return kinds;
}

pub fn parameterFallbackTypes(arena: std.mem.Allocator, parameters: []const Json) ![]const ?ast.ColumnType {
    if (parameters.len > 1024) return error.InvalidSqlParameters;
    const types = try arena.alloc(?ast.ColumnType, parameters.len);
    for (parameters, types) |value, *kind| kind.* = parameterFallbackType(value);
    return types;
}

fn parameterFallbackType(value: Json) ?ast.ColumnType {
    return switch (value) {
        .null => null,
        .bool => .boolean,
        .integer => .integer,
        .float => .number,
        .string => .string,
        .array, .object => .json,
        // Integer tokens never pass through f64, including overflow tokens
        // which the eventual integer coercion must reject rather than round.
        .number_string => |text| if (std.mem.indexOfAny(u8, text, ".eE") == null) .integer else .number,
    };
}

fn runBound(alloc: std.mem.Allocator, arena: std.mem.Allocator, backend: catalog.Backend, compiled: *const compiler.Compiled, parameters: []const Json, limits: Limits, sink: ?RowSink) !Output {
    // COPY has a streaming protocol contract, not a JSON SELECT result. Only
    // the retained read-stream entry point may execute its underlying query.
    if (compiled.statement == .copy_to or compiled.statement == .copy_from) return error.UnsupportedSqlShape;
    if (compiled.statement == .explain) {
        const explanation = compiled.statement.explain;
        const inner: compiler.Compiled = .{ .arena = undefined, .statement = explanation.statement.*, .parameter_count = compiled.parameter_count, .uses_sequences = compiled.uses_sequences, .uses_current_setting = compiled.uses_current_setting };
        // Analyze is explicitly read-only: reject mutation plans before binding,
        // cursor capture, parameter preparation, or native admission.
        if (explanation.analyze and inner.statement != .select) return error.SqlAnalyzeRequiresReadOnly;
        var profile: @import("execution_profile.zig").Profile = .{ .allocator = arena, .io = backend.execution_io, .timing = explanation.timingEnabled(), .summary = explanation.summaryEnabled(), .buffers = explanation.buffersEnabled(), .wal = explanation.wal };
        const planning_start = if (explanation.summaryEnabled()) profile.wallNow() else null;
        const binding = try describe.bind(arena, backend, &inner, &.{});
        if (backend.execution_binding_validator) |validator| try validator.validate(validator.ptr, alloc, binding);
        if (binding.parameter_invocation) |invocation| try invocation.prepareNativeOrJson(alloc, parameters, limits.parameter_cells, .{ .bytes = limits.retained_bytes, .wire_bytes = limits.retained_bytes, .error_context = backend.error_context, .payload_owner = limits.parameter_owner });
        defer if (binding.parameter_invocation) |invocation| invocation.deinitFrame();
        const prepared_parameters = if (binding.parameter_invocation) |invocation| try invocation.compatibilityValues(arena) else parameters;
        if (inner.statement == .select) try @import("execution_prepare.zig").validate(alloc, backend, binding);
        const planning_ns = profile.wallDuration(planning_start);
        const preparation_peak = if (limits.memory_accounting) |budget| @as(?usize, budget.peak) else null;
        if (explanation.analyze) {
            var scratch = std.heap.ArenaAllocator.init(alloc);
            defer scratch.deinit();
            var drain: AnalyzeDrain = .{ .profile = &profile, .scratch = &scratch, .backend = backend };
            const started = if (explanation.summaryEnabled()) profile.wallNow() else null;
            var measured_limits = limits;
            measured_limits.profile = &profile;
            measured_limits.native_result = false;
            // The response is one plan row. Execution retains the original
            // scan/work/memory/spill bounds, rather than a transport row quota.
            measured_limits.result_rows = std.math.maxInt(i64);
            const measured = try runWithBinding(alloc, arena, backend, &inner, binding, prepared_parameters, measured_limits, .{ .ptr = &drain, .append = AnalyzeDrain.append, .append_batch = AnalyzeDrain.appendBatch }, true);
            profile.result_rows = std.math.add(usize, profile.result_rows, measured.rowCount()) catch return error.SqlProgramLimitExceeded;
            profile.elapsed_ns = profile.wallDuration(started);
            if (limits.memory_accounting) |budget| profile.retained_peak_bytes = budget.peak;
        }
        const rendered = try @import("explain.zig").renderMeasuredWithContext(arena, explanation, binding, if (explanation.analyze) &profile else null, backend, prepared_parameters, .{ .planning_ns = planning_ns, .preparation_peak_bytes = preparation_peak, .memory_bytes = limits.retained_bytes, .scan_rows = limits.scan_rows, .scan_pages = limits.scan_pages, .batch_rows = limits.execution_batch_rows });
        const row = try arena.dupe(Json, &.{.{ .string = rendered }});
        return .{ .columns = try arena.dupe(Column, &.{.{ .name = "QUERY PLAN", .type = .string }}), .rows = try arena.dupe([]const Json, &.{row}), .command_tag = "EXPLAIN" };
    }
    const ddl = @import("ddl_runtime.zig");
    if (ddl.accepts(compiled.statement)) {
        const result = try ddl.execute(.{ .result = arena, .scratch = alloc }, backend, compiled.statement);
        return .{ .command_tag = result.command_tag, .mutation_outcome = result.mutation_outcome, .ddl_receipt = result.receipt, .catalog_receipt = result.catalog_receipt };
    }
    // Describe and Execute share binding, type inference and authorization.
    // Resolve exactly once so validation and execution cannot pin different
    // catalog identities within one statement.
    const binding = try describe.bind(arena, backend, compiled, &.{});
    return runWithBinding(alloc, arena, backend, compiled, binding, parameters, limits, sink, false);
}

fn runWithBinding(alloc: std.mem.Allocator, arena: std.mem.Allocator, backend: catalog.Backend, compiled: *const compiler.Compiled, binding: describe.BoundStatement, parameters: []const Json, limits: Limits, sink: ?RowSink, expressions_prepared: bool) !Output {
    if (compiled.statement == .select) if (backend.execution_binding_validator) |validator| try validator.validate(validator.ptr, alloc, binding);
    var statement_backend = backend;
    statement_backend.execution_binding_validator = null;
    statement_backend.parameter_invocation = binding.parameter_invocation;
    var owns_frame = false;
    if (binding.parameter_invocation) |invocation| if (invocation.phase == .binding) {
        try invocation.prepareNativeOrJson(alloc, parameters, limits.parameter_cells, .{ .bytes = limits.retained_bytes, .wire_bytes = limits.retained_bytes, .error_context = backend.error_context, .payload_owner = limits.parameter_owner });
        owns_frame = true;
    };
    defer if (owns_frame) binding.parameter_invocation.?.deinitFrame();
    if (compiled.statement == .select and !expressions_prepared) try @import("execution_prepare.zig").validate(alloc, backend, binding);
    const prepared_parameters = if (binding.parameter_invocation) |invocation| try invocation.compatibilityValues(arena) else parameters;
    var manager: ?@import("spill.zig").Manager = null;
    defer if (manager) |*owned| owned.deinit();
    if (backend.spill_manager == null and limits.spill_bytes != 0) if (backend.execution_io) |io| {
        manager = .{ .alloc = alloc, .io = io, .context = backend.ptr, .checkpoint = backend.vtable.checkpoint, .root = limits.spill_root, .max_bytes = limits.spill_bytes, .buffer_bytes = @min(4096, @max(128, limits.retained_bytes / 512)), .max_record_bytes = limits.retained_bytes };
    };
    const context = Context{ .alloc = alloc, .arena = arena, .backend = statement_backend, .binding = binding, .parameters = prepared_parameters, .limits = limits, .spill = backend.spill_manager orelse if (manager) |*owned| owned else null, .sink = sink, .typed_output = sink != null };
    const stage = if (limits.profile) |profile| try profile.registerStage(@tagName(compiled.statement), if (binding.table) |table| table.physical_name else null) else null;
    const started = if (limits.profile) |profile| profile.now() else null;
    const rows_before = if (limits.profile) |profile| profile.result_rows else 0;
    const spill_before = if (context.spill) |spill| spill.written_bytes else 0;
    const output = context.run(compiled.statement) catch |err| {
        if (limits.profile) |profile| profile.record(stage, started, 0, false, true);
        return err;
    };
    if (limits.profile) |profile| profile.record(stage, started, output.rowCount() + (profile.result_rows - rows_before), true, false);
    if (limits.profile) |profile| {
        profile.spill_written_bytes = if (context.spill) |spill| spill.written_bytes - spill_before else 0;
    }
    return output;
}

/// A complete execution consumer. Lazy typed batches must be forced before
/// their page owners are released; row counting alone can hide scalar errors.
const AnalyzeDrain = struct {
    profile: *@import("execution_profile.zig").Profile,
    scratch: *std.heap.ArenaAllocator,
    backend: catalog.Backend,
    fn append(raw: *anyopaque, _: []const Datum) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.profile.result_rows = std.math.add(usize, self.profile.result_rows, 1) catch return error.SqlProgramLimitExceeded;
    }
    fn appendBatch(raw: *anyopaque, batch: @import("execution_batch.zig").Batch) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        for (0..batch.len()) |row| {
            if (row % 64 == 0) try self.backend.vtable.checkpoint(self.backend.ptr);
            _ = self.scratch.reset(.retain_capacity);
            for (0..batch.width()) |column| _ = try batch.cell(self.scratch.allocator(), row, column);
            try append(raw, &.{});
        }
    }
};

pub const RowSink = struct {
    ptr: *anyopaque,
    append: *const fn (*anyopaque, []const Datum) anyerror!void,
    append_batch: ?*const fn (*anyopaque, @import("execution_batch.zig").Batch) anyerror!void = null,
    take_sorted: ?*const fn (*anyopaque, *@import("operators.zig").TopK, usize, usize, bool) anyerror!void = null,
};
// A synchronous view over a scan-page lease. Expressions and sort keys use
// scalar semantics; pass-through JSON retains the native producer capability.
const ProjectedBatch = struct {
    rows: []const catalog.Row,
    selection: []const usize,
    positions: []const ?usize,
    fields: []const []const u8,
    columns: []const describe.Column,
    expressions: []const ?[]const Datum,
    coerced: []const ?[]const Datum = &.{},
    fn cell(raw: *anyopaque, a: std.mem.Allocator, lane: usize, column: usize) anyerror!Datum {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (column < self.coerced.len) if (self.coerced[column]) |values| return values[lane];
        const row = self.selection[lane];
        const input = if (column < self.expressions.len and self.expressions[column] != null)
            self.expressions[column].?[self.positions[row].?]
        else
            try self.rows[row].cell(self.fields[column]);
        return describe.coerceDatumTyped(a, input, scalar.columnType(self.columns[column]));
    }
    fn native(self: *@This(), column: usize) bool {
        return self.columns[column].type == .json and (column >= self.expressions.len or self.expressions[column] == null);
    }
    fn frozen(raw: *anyopaque, lane: usize, column: usize) anyerror!?@import("execution_batch.zig").FrozenJson {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return if (self.native(column)) self.rows[self.selection[lane]].frozenJson(self.fields[column]) else null;
    }
    fn json(raw: *anyopaque, lane: usize, column: usize) anyerror!?[]const u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return if (self.native(column)) self.rows[self.selection[lane]].encodedJson(self.fields[column]) else null;
    }
    fn batch(self: *@This()) @import("execution_batch.zig").Batch {
        return .{ .reader = .{ .ptr = self, .read = cell, .read_frozen = frozen, .read_json = json, .count = self.selection.len, .width = self.columns.len } };
    }
};
test "SQL range comparisons bind typed residuals without JSON storage predicates" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const table: catalog.Table = .{ .id = 1, .physical_name = "ranges", .schema_version = 1, .storage_mode = .relational, .columns = &.{.{ .name = "value", .path = "value", .type = .numrange }} };
    const context: Context = .{
        .alloc = a,
        .arena = a,
        .backend = undefined,
        .binding = .{ .table = table, .action = .read, .columns = &.{}, .parameter_types = &.{}, .json_literals = .empty },
        .parameters = &.{},
        .limits = .{},
    };
    const text = try describe.bindLiteral(a, .{ .string = "[1.00,10.000)" }, .numrange);
    try std.testing.expectEqualStrings("[1.00,10.000)", text.string);
    for ([_]ast.Comparison{ .eq, .neq, .lt, .lte, .gt, .gte }) |op| {
        const predicate: ast.Predicate = .{ .comparison = .{ .field = "value", .op = op, .value = .{ .string = "[1,10)" } } };
        const bound = try context.conditions(table, &predicate);
        try std.testing.expectEqual(@as(usize, 0), bound.terms.items.len);
        try std.testing.expect(!bound.complete);
        try std.testing.expect(@import("bound_scalars.zig").needsResidual(table, &predicate));
    }
}

test "SQL range native parameter identity survives unconstrained transport fallback" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const ranges = @import("range_value.zig");
    var context: ranges.Context = .{ .alloc = a };
    for ([_]ranges.Kind{ .numrange, .daterange, .tsrange, .tstzrange }) |kind| {
        const value = try ranges.parseLeaky(&context, kind, "empty");
        const cells = [_]Datum{.typedRange(&value)};
        const hints = try parameterFallbackTypesNative(a, &.{.null}, &cells);
        const invocation = try @import("parameter_binding.zig").Invocation.initLeaky(a, 1);
        invocation.fallbacks = hints;
        try invocation.prepareNativeOrJson(std.testing.allocator, &.{.null}, &cells, .{});
        defer invocation.deinitFrame();
        try std.testing.expectEqual(ranges.typeFromKind(ast.ColumnType, kind), invocation.frame.?.descriptors[0].kind.?);
        try std.testing.expect(invocation.frame.?.descriptors[0].element_type == null);
        try std.testing.expectEqual(kind, invocation.frame.?.values[0].range().?.kind);
        try std.testing.expect(invocation.frame.?.values[0].range().? != &value);
    }
}

test "SQL timestamp parameters preserve native conditions and nonfinite typed residuals" {
    const Run = struct {
        fn run(backing: std.mem.Allocator) !void {
            var arena = std.heap.ArenaAllocator.init(backing);
            defer arena.deinit();
            const a = arena.allocator();
            for ([_][]const u8{ "2024-02-29T13:14:15+01:00", "1969-12-31T23:59:59.999Z", "infinity", "-infinity" }, 0..) |input, index| {
                const invocation = try @import("parameter_binding.zig").Invocation.initLeaky(a, 1);
                invocation.descriptors[0] = .{ .kind = .datetime };
                try invocation.prepareJson(backing, &.{.{ .string = input }}, .{});
                defer invocation.deinitFrame();
                const context: Context = .{
                    .alloc = a,
                    .arena = a,
                    .backend = undefined,
                    .binding = .{ .table = null, .action = .read, .columns = &.{}, .parameter_types = &.{.datetime}, .json_literals = .empty, .parameter_invocation = invocation },
                    .parameters = &.{.null},
                    .limits = .{},
                };
                const table: catalog.Table = .{ .id = 1, .physical_name = "rows", .schema_version = 1, .columns = &.{.{ .name = "ts", .path = "ts", .type = .datetime }} };
                const predicate: ast.Predicate = .{ .comparison = .{ .field = "ts", .op = .eq, .value = .{ .parameter = 1 } } };
                const conditions = try context.conditions(table, &predicate);
                try std.testing.expect(!conditions.empty);
                try std.testing.expectEqual(index == 0, conditions.complete);
                try std.testing.expectEqual(@as(usize, if (index == 0) 1 else 0), conditions.terms.items.len);
                if (index == 0) try std.testing.expectEqualStrings("2024-02-29T12:14:15.000000000Z", conditions.terms.items[0].value.string);
            }
        }
    };
    try Run.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Run.run, .{});
}

test "SQL native document version conditions remain typed residuals not stored properties" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const context: Context = .{
        .alloc = a,
        .arena = a,
        .backend = undefined,
        .binding = .{ .table = null, .action = .read, .columns = &.{}, .parameter_types = &.{}, .json_literals = .empty },
        .parameters = &.{},
        .limits = .{},
    };
    var table: catalog.Table = .{ .id = 1, .physical_name = "docs", .schema_version = 1, .storage_mode = .document, .columns = &.{} };
    for ([_]ast.Comparison{ .eq, .neq, .lt, .lte, .gt, .gte }) |op| {
        const predicate: ast.Predicate = .{ .comparison = .{ .field = "_version", .op = op, .value = .{ .integer = 42 } } };
        const bound = try context.conditions(table, &predicate);
        try std.testing.expectEqual(@as(usize, 0), bound.terms.items.len);
        try std.testing.expect(!bound.complete);
        try std.testing.expect(@import("bound_scalars.zig").needsResidual(table, &predicate));
    }
    for ([_]bool{ false, true }) |negated| {
        const predicate: ast.Predicate = .{ .is_null = .{ .field = "_version", .negated = negated } };
        const bound = try context.conditions(table, &predicate);
        try std.testing.expectEqual(!negated, bound.empty);
        try std.testing.expectEqual(@as(usize, 0), bound.terms.items.len);
    }
    // A genuine relational user column with this name keeps ordinary native
    // pushdown. This metadata rule must not become an unqualified-name hack.
    table.storage_mode = .relational;
    table.columns = &.{.{ .name = "_version", .path = "_version", .type = .integer }};
    const real: ast.Predicate = .{ .comparison = .{ .field = "_version", .op = .eq, .value = .{ .integer = 42 } } };
    const pushed = try context.conditions(table, &real);
    try std.testing.expectEqual(@as(usize, 1), pushed.terms.items.len);
    try std.testing.expect(pushed.complete);
}

pub const Context = struct {
    sink: ?RowSink = null,
    spill: ?*@import("spill.zig").Manager = null,
    alloc: std.mem.Allocator,
    arena: std.mem.Allocator,
    backend: catalog.Backend,
    binding: describe.BoundStatement,
    parameters: []const Json,
    limits: Limits,
    /// Internal relational consumers retain typed values. Decimal strings are
    /// a transport encoding, never the representation of an INSERT source.
    typed_output: bool = false,
    statement_capture: ?*@import("mutation_capture.zig") = null,
    returning_rows: []const catalog.Row = &.{},
    returning_cursor: ?*@import("result_cursor.zig").Cursor = null,
    returning_layout: ?*const catalog.Row.TypedLayout = null,
    /// Typed values borrowed from the active Apply frame, not from input rows.
    /// Their lifetime covers this invocation, including empty global grouping.
    invocation_constants: []const Datum = &.{},

    pub fn emitTop(self: Context, top: *@import("operators.zig").TopK, offset: usize, limit: usize, implicit: bool) !Output {
        if (self.sink.?.take_sorted) |take| try take(self.sink.?.ptr, top, offset, limit, implicit) else try top.drain(self.alloc, offset, limit, implicit, self.sink.?);
        return .{ .columns = self.binding.columns, .command_tag = "SELECT" };
    }

    pub fn evaluate(self: Context, alloc: std.mem.Allocator, program: @import("scalar.zig").Program, cells: []const Datum) !Datum {
        const decision = @import("decision_eval.zig");
        return decision.evaluateWithLimits(alloc, self.backend.decision_provider, &program, cells, self.parameters, decision.limitsFor(self.backend));
    }

    pub fn matchesRow(self: Context, alloc: std.mem.Allocator, cells: []const Datum) !bool {
        return self.binding.scalars.matchesWithLimits(alloc, cells, self.parameters, self.backend.decision_provider, @import("decision_eval.zig").limitsFor(self.backend));
    }

    pub const ScanState = struct {
        profile_id: ?usize = null,
        opened: bool = false,
        cursor: ?catalog.Cursor = null,

        pub fn deinit(self: *ScanState) void {
            if (self.cursor) |cursor| cursor.close(cursor.ptr);
            self.cursor = null;
        }

        fn open(self: *ScanState, context: Context, table: catalog.Table, request: catalog.Scan) !void {
            if (!self.opened) {
                self.opened = true;
                if (context.limits.profile) |profile| self.profile_id = try profile.registerScan(table);
                if (context.backend.vtable.open_scan) |capture|
                    self.cursor = try capture(context.backend.ptr, context.alloc, table, request);
            }
        }
        pub fn page(self: *ScanState, context: Context, alloc: std.mem.Allocator, table: catalog.Table, request: catalog.Scan) !catalog.Page {
            try self.open(context, table, request);
            const profile = context.limits.profile;
            const started = if (profile) |p| p.now() else null;
            const result = (if (self.cursor) |cursor| cursor.next(cursor.ptr, alloc, request.limit) else context.backend.vtable.scan(context.backend.ptr, alloc, table, request)) catch |err| {
                if (profile) |p| p.record(self.profile_id, started, 0, false, true);
                return err;
            };
            if (profile) |p| p.record(self.profile_id, started, result.rows.len, result.after == null, false);
            return result;
        }
        pub fn count(self: *ScanState, context: Context, table: catalog.Table, request: catalog.Scan) !?u64 {
            try self.open(context, table, request);
            if (self.cursor) |cursor| if (cursor.count_rows) |count_rows| {
                const count_rows_result = try count_rows(cursor.ptr);
                if (context.limits.profile) |p| if (self.profile_id) |id| {
                    p.nodes.items[id].metadata_rows = count_rows_result;
                };
                return count_rows_result;
            };
            return null;
        }
        pub fn columns(self: *ScanState, context: Context, alloc: std.mem.Allocator, table: catalog.Table, request: catalog.Scan) !?catalog.ColumnPage {
            try self.open(context, table, request);
            if (self.cursor) |cursor| if (cursor.next_columns) |next| {
                const profile = context.limits.profile;
                const started = if (profile) |p| p.now() else null;
                const result = next(cursor.ptr, alloc, request.limit) catch |err| {
                    if (profile) |p| p.record(self.profile_id, started, 0, false, true);
                    return err;
                };
                if (profile) |p| p.record(self.profile_id, started, result.selection.len, result.after == null, false);
                return result;
            };
            return null;
        }

        pub fn partitions(self: *ScanState, context: Context, alloc: std.mem.Allocator, table: catalog.Table, request: catalog.Scan, maximum: usize) !?[]catalog.Cursor {
            try self.open(context, table, request);
            if (self.cursor) |cursor| if (cursor.split_scan) |split| return try split(cursor.ptr, alloc, maximum);
            return null;
        }

        pub fn retained(self: ScanState, context: Context) bool {
            return self.cursor != null or context.backend.pinned_statement_snapshot;
        }
    };

    fn run(unscoped: Context, input: ast.Statement) !Output {
        var context = unscoped;
        context.backend = @import("decision_eval.zig").scopedBackend(context.backend, context.binding);
        if (input != .select) try @import("decision_eval.zig").validateStatement(context.arena, context.backend.decision_provider, context.binding, context.parameters);
        var capture: ?@import("mutation_capture.zig") = null;
        defer if (capture) |*owner| owner.deinit();
        if (context.binding.returning_query != null) {
            capture = try @import("mutation_capture.zig").open(context.alloc, context.backend, context.binding, context.parameters);
            context.statement_capture = &capture.?;
        }
        if (context.binding.joined_mutation) |joined| return @import("joined_mutation.zig").execute(context, joined.*);
        if (context.binding.merge_mutation) |merge| return @import("merge_mutation.zig").execute(context, merge.*);
        return switch (input) {
            .select => |statement| try context.selectOutput(statement),
            .insert => |input_insert| blk: {
                var statement = input_insert;
                if (context.binding.normalized_insert_columns) |names| {
                    statement.columns = names;
                    statement.implicit_columns = false;
                }
                break :blk try context.insert(statement);
            },
            .update => |statement| try context.change(statement.table, statement.predicate, statement.assignments, statement.returning),
            .delete => |statement| try context.change(statement.table, statement.predicate, null, statement.returning),
            else => return error.UnsupportedSqlExecution,
        };
    }

    pub fn checkpoint(self: Context) !void {
        try self.backend.vtable.checkpoint(self.backend.ptr);
    }

    fn expressionAccessCheckpoint(raw: ?*anyopaque) !void {
        const self: *const Context = @ptrCast(@alignCast(raw.?));
        try self.checkpoint();
    }

    pub fn expressionEqualities(self: Context, table: catalog.Table, eligible: bool) ![]const @import("../storage/relational_index.zig").ExpressionEqualityHint {
        if (!eligible or !self.backend.supports_expression_access_hints) return &.{};
        const program = if (self.binding.scalars.predicate) |*predicate| predicate else return &.{};
        return @import("index_access.zig").prepareEqualities(self.arena, self.alloc, table, program, self.binding.scalars.columns, self.parameters, .{ .ptr = @constCast(&self), .checkpoint = expressionAccessCheckpoint, .clock_values = self.backend.clock_values });
    }

    /// Evaluate one owner-demanded scalar on the active statement cut. The
    /// native backend owns the cut and accumulates every point/range proof in
    /// the same transaction as the eventual mutation. Bound output is typed;
    /// the public bigint string representation must not enter row arithmetic.
    pub fn deferredScalar(self: Context, query: *const ast.Select, bound: *const describe.BoundStatement) !@import("scalar.zig").Datum {
        var nested = self;
        nested.binding = bound.*;
        nested.typed_output = true;
        nested.limits.result_rows = @min(nested.limits.result_rows, 2);
        if (nested.limits.result_rows < 2) return error.SqlProgramLimitExceeded;
        const cursor = nested.typedQuery(query.*) catch |err| switch (err) {
            error.SqlResultTooLarge => return error.SqlCardinalityViolation,
            else => return err,
        };
        defer cursor.close();
        if (cursor.count() > 1) return error.SqlCardinalityViolation;
        const row = (try cursor.next(self.arena)) orelse return .{};
        if (row.len != 1) return error.InvalidSqlBackendResponse;
        return row[0];
    }

    /// One typed blocking-result boundary for nested relations, windows and
    /// scalar owners. Spill and no-I/O execution have identical value semantics.
    pub fn typedQuery(self: Context, statement: ast.Select) !*@import("result_cursor.zig").Cursor {
        const Cursor = @import("result_cursor.zig").Cursor;
        const cursor = if (self.spill) |manager| try Cursor.create(self.alloc, manager, self.binding.columns.len) else try Cursor.createMemory(self.alloc, self.binding.columns.len, self.limits.retained_bytes / 2);
        errdefer cursor.close();
        try self.selectInto(statement, .{ .ptr = cursor, .append = Cursor.append, .append_batch = Cursor.appendBatch, .take_sorted = Cursor.takeSorted });
        return cursor;
    }

    pub const ResultRow = struct { json: []const Json, nulls: []const bool, native: ?[]const Datum };

    pub fn resultRow(self: Context, values: []const Datum, columns: []const Column) !ResultRow {
        if (values.len != columns.len) return error.InvalidSqlBackendResponse;
        if (self.limits.native_result) {
            const cells = try self.arena.alloc(Datum, values.len);
            for (values, columns, cells) |value_, column, *cell| {
                var checked = try describe.coerceDatumTyped(self.arena, value_, scalar.columnType(column));
                // Pattern callbacks belong to execution frames. Transports
                // consume their value representation after those frames close.
                if (checked.patterns() != null) {
                    checked.extension_kind = .none;
                    checked.extension = null;
                }
                cell.* = try operators.cloneDatum(self.arena, checked);
            }
            return .{ .json = &.{}, .nulls = &.{}, .native = cells };
        }
        const cells = try self.arena.alloc(Json, values.len);
        const nulls = try self.arena.alloc(bool, values.len);
        for (values, columns, cells, nulls) |value_, column, *cell, *flag| {
            cell.* = try self.outputDatum(value_, column.type, column.element_type);
            flag.* = value_.sql_null;
        }
        return .{ .json = cells, .nulls = nulls, .native = null };
    }

    /// Materialized transport results use the same typed sink as blocking
    /// operators and pull execution. The result arena owns each retained cell.
    pub fn selectOutput(self: Context, statement: ast.Select) !Output {
        if (!self.limits.native_result) return self.select(statement);
        const Collector = struct {
            context: Context,
            rows: std.ArrayList([]const Datum) = .empty,
            fn append(raw: *anyopaque, cells: []const Datum) !void {
                const owner: *@This() = @ptrCast(@alignCast(raw));
                if (owner.rows.items.len >= owner.context.limits.result_rows) return error.SqlResultTooLarge;
                const row = try owner.context.resultRow(cells, owner.context.binding.columns);
                try owner.rows.append(owner.context.arena, row.native.?);
            }
        };
        var collector: Collector = .{ .context = self };
        try self.selectInto(statement, .{ .ptr = &collector, .append = Collector.append });
        return .{ .columns = self.binding.columns, .native_rows = collector.rows.items, .command_tag = "SELECT" };
    }

    pub fn selectInto(self: Context, statement: ast.Select, sink: RowSink) !void {
        var nested = self;
        nested.typed_output = true;
        nested.sink = sink;
        const output = try nested.select(statement);
        // Metadata/count optimizations may return a tiny scalar JSON result.
        // Never interpret an array placeholder through this compatibility path.
        var scratch = std.heap.ArenaAllocator.init(self.alloc);
        defer scratch.deinit();
        if (output.sql_nulls) |flags| if (flags.len != output.rows.len) return error.InvalidSqlBackendResponse;
        if (output.pattern_sources) |sources| if (sources.len != output.rows.len) return error.InvalidSqlBackendResponse;
        for (output.rows, 0..) |row, index| {
            _ = scratch.reset(.retain_capacity);
            if (row.len != self.binding.columns.len) return error.InvalidSqlBackendResponse;
            if (output.sql_nulls) |flags| if (flags[index].len != row.len) return error.InvalidSqlBackendResponse;
            if (output.pattern_sources) |sources| if (sources[index].len != row.len) return error.InvalidSqlBackendResponse;
            const values = try scratch.allocator().alloc(Datum, row.len);
            for (row, values, self.binding.columns, 0..) |value_, *cell, column, ordinal| {
                if (column.type == .array) return error.InvalidSqlBackendResponse;
                cell.* = try describe.coerceDatumTyped(scratch.allocator(), Datum.fromParts(value_, if (output.sql_nulls) |flags| flags[index][ordinal] else value_ == .null, if (output.pattern_sources) |sources| sources[index][ordinal] else null), scalar.columnType(column));
            }
            try sink.append(sink.ptr, values);
        }
    }

    pub fn outputValue(self: Context, value_: Json) !Json {
        if (!self.typed_output and value_ == .integer) return .{ .string = try std.fmt.allocPrint(self.arena, "{d}", .{value_.integer}) };
        return clone(self.arena, value_);
    }
    pub fn outputCell(self: Context, value_: Json, kind: ?ast.ColumnType) !Json {
        // SQL bigint uses a lossless string wire representation. A JSON
        // numeric scalar is still a JSON number, not a SQL bigint cell.
        if (kind == .json) return clone(self.arena, value_);
        // Native preparation can preserve exact JSON numeric tokens rather
        // than materialize i64 cells. The declared SQL type, not that physical
        // JSON tag, determines the transport encoding. Normalize without f64.
        if (kind == .integer) return self.outputValue(try describe.coerceAlloc(self.arena, value_, .integer));
        return self.outputValue(value_);
    }

    /// Preserve the complete typed value until the public result boundary.
    /// Arrays are owned envelopes and NUMERIC values are exact decimal text,
    /// never their JSON-null placeholders.
    pub fn outputDatum(self: Context, datum: Datum, kind: ?ast.ColumnType, element_type: ?@import("array_value.zig").ElementType) !Json {
        if (datum.range()) |range| {
            try datum.validateTemporal();
            if (kind == null or @import("range_value.zig").kindFromType(kind.?) != range.kind) return error.SqlTypeMismatch;
            var context: @import("range_value.zig").Context = .{ .alloc = self.arena, .max_output_bytes = self.limits.retained_bytes };
            return .{ .string = try @import("range_value.zig").formatLeaky(&context, range.*) };
        }
        if (datum.temporal() != null) {
            var text: [@import("temporal_text.zig").output_capacity]u8 = undefined;
            if (kind != .datetime) return error.SqlTypeMismatch;
            try datum.validateTemporal();
            return .{ .string = try self.arena.dupe(u8, try @import("temporal_text.zig").format(datum.temporal().?.*, &text)) };
        }
        if (kind == .array) {
            const checked = try describe.coerceDatum(self.arena, datum, .array, element_type);
            if (checked.sql_null) return .null;
            return @import("array_wire.zig").toJsonLeaky(self.arena, checked.array.?.*, .{ .values = .{ .bytes = self.limits.retained_bytes }, .wire_bytes = self.limits.retained_bytes });
        }
        if (datum.array != null) return error.SqlTypeMismatch;
        if (datum.numeric != null or (kind == .number and element_type == .numeric)) {
            const checked = try describe.coerceDatum(self.arena, datum, kind orelse .number, element_type);
            if (checked.sql_null) return .null;
            var context: @import("numeric_value.zig").Context = .{ .alloc = self.arena, .max_output_bytes = self.limits.retained_bytes };
            return .{ .string = try @import("numeric_value.zig").format(&context, checked.numeric.?.*) };
        }
        return self.outputCell(datum.value, kind);
    }

    /// The mutation boundary owns one storage representation of each logical
    /// cell. Arrays remain typed until here; their null JSON placeholder must
    /// never be mistaken for either a SQL NULL or a JSONB null write.
    pub fn storageDatum(self: Context, datum: Datum, column: catalog.Column) !Json {
        return self.storageDatumTyped(datum, column, null);
    }

    pub fn storageDatumTyped(self: Context, datum: Datum, column: catalog.Column, source_identity: ?@import("array_value.zig").ElementType) !Json {
        return encodeStorageDatumTyped(self.arena, datum, column, source_identity, self.limits.retained_bytes) catch |err| {
            if (err == error.SqlNotNullViolation) return @import("errors.zig").notNull(self.backend.error_context, column.name);
            return err;
        };
    }

    fn value(self: Context, input: ast.Value, column: catalog.Column) !Json {
        if (input == .string and column.type == .json)
            return self.binding.json_literals.get(input.string) orelse error.InvalidSqlBackendResponse;
        if (input != .parameter) return describe.bindLiteral(self.arena, input, column.type);
        const index = input.parameter;
        if (index == 0 or index > self.parameters.len) return error.InvalidSqlParameters;
        if (self.binding.parameter_invocation) |invocation| if (invocation.frame) |frame| {
            const datum = try describe.coerceDatumTyped(self.arena, frame.values[index - 1], scalar.columnType(column));
            if (datum.sql_null) return .null;
            if (datum.temporal()) |temporal| {
                // Existing native timestamp predicates consume RFC3339. Keep
                // the logical frame typed and encode only at this boundary.
                if (temporal.kind() != .timestamp_tz) return error.UnsupportedSqlShape;
                const nanos = temporal.timestamp_tz.unixNanoseconds() orelse return error.UnsupportedSqlShape;
                return .{ .string = try @import("../datetime.zig").formatDateTimeSignedNsAlloc(self.arena, nanos) };
            }
            if (datum.array != null or datum.numeric != null or datum.range() != null) return error.UnsupportedSqlShape;
            return datum.value;
        };
        return coerce(self.arena, self.parameters[index - 1], column.type);
    }

    fn parameterDatum(self: Context, index: usize) !Datum {
        if (index >= self.parameters.len) return error.InvalidSqlParameters;
        if (self.binding.parameter_invocation) |invocation| if (invocation.frame) |frame| return frame.values[index];
        const raw = Datum.fromJson(self.parameters[index]);
        if (index < self.binding.parameter_descriptors.len) {
            const actual = self.binding.parameter_descriptors[index];
            if (actual.kind != null) return describe.coerceDatumTyped(self.arena, raw, actual);
        }
        return raw;
    }

    pub fn count(self: Context, input: ?ast.Value, default: usize) !usize {
        const node = input orelse return default;
        const parsed = try self.value(node, .{ .name = "limit", .path = "limit", .type = .integer });
        if (parsed == .null) return default;
        if (parsed != .integer) return error.InvalidSqlLimit;
        if (parsed.integer < 0) return error.SqlNegativeLimit;
        return std.math.cast(usize, parsed.integer) orelse error.InvalidSqlLimit;
    }

    pub fn hasRowLimit(self: Context, input: ?ast.Value) !bool {
        const value_ = input orelse return false;
        return (try self.value(value_, .{ .name = "limit", .path = "limit", .type = .integer })) != .null;
    }

    pub fn offsetCount(self: Context, input: ?ast.Value) !usize {
        return self.count(input, 0) catch |err| switch (err) {
            error.SqlNegativeLimit => error.SqlNegativeOffset,
            else => err,
        };
    }

    const BoundPredicates = struct {
        complete: bool = true,
        terms: std.ArrayList(catalog.Condition) = .empty,
        primary_key: ?[]const u8 = null,
        empty: bool = false,
    };

    pub fn conditions(self: Context, table_def: catalog.Table, predicate: ?*const ast.Predicate) !BoundPredicates {
        var conditions_out: BoundPredicates = .{};
        try self.bindConditions(table_def, predicate, &conditions_out);
        // The bound program, including casts and parameter identities, is the
        // authority for exact operands. JSON-only AST lowering cannot prove
        // the comparison domain of NUMERIC versus floating-point values.
        if (table_def.external_base_source != null) if (self.binding.scalars.predicate) |*program| {
            try self.bindNumericConditions(table_def, program, program.root, &conditions_out, 0);
        };
        return conditions_out;
    }

    fn constantOperand(program: *const @import("scalar.zig").Program, index: u32, depth: usize) bool {
        if (depth >= 128 or index >= program.instructions.len) return false;
        return switch (program.instructions[index].operation) {
            .literal, .parameter => true,
            .cast => |cast| constantOperand(program, cast.operand, depth + 1),
            .unary => |unary| (unary.op == .positive or unary.op == .negative) and constantOperand(program, unary.operand, depth + 1),
            else => false,
        };
    }
    fn bindNumericConditions(self: Context, table: catalog.Table, program: *const @import("scalar.zig").Program, index: u32, output: *BoundPredicates, depth: usize) anyerror!void {
        if (depth >= 128 or index >= program.instructions.len) return error.InvalidSqlProgram;
        const instruction = program.instructions[index];
        if (instruction.operation != .binary) return;
        const binary = instruction.operation.binary;
        if (binary.op == .@"and") {
            try self.bindNumericConditions(table, program, binary.left, output, depth + 1);
            return self.bindNumericConditions(table, program, binary.right, output, depth + 1);
        }
        var op: catalog.Condition.Op = switch (binary.op) {
            .eq => .eq,
            .neq => .neq,
            .lt => .lt,
            .lte => .lte,
            .gt => .gt,
            .gte => .gte,
            else => return,
        };
        var col = binary.left;
        var value_index = binary.right;
        if (program.instructions[col].operation != .column) {
            col = binary.right;
            value_index = binary.left;
            op = switch (op) {
                .lt => .gt,
                .lte => .gte,
                .gt => .lt,
                .gte => .lte,
                else => op,
            };
        }
        if (program.instructions[col].operation != .column or !constantOperand(program, value_index, 0)) return;
        const ordinal = program.instructions[col].operation.column;
        if (ordinal >= self.binding.scalars.columns.len) return error.InvalidSqlProgram;
        const column = try table.column(self.binding.scalars.columns[ordinal].name);
        if (column.element_type != .numeric) return;
        const descriptor = program.instructions[value_index].type;
        // Mixed floating comparisons intentionally use SQL's floating domain;
        // an exact NUMERIC tuple seek would not implement that equivalence.
        if (descriptor.kind != .integer and !(descriptor.kind == .number and descriptor.element_type == .numeric)) return;
        const raw = try program.evaluateInstruction(self.arena, value_index, self.parameters);
        if (raw.sql_null) {
            output.empty = true;
            return;
        }
        const value_ = try describe.coerceDatumTyped(self.arena, raw, scalar.columnType(column));
        try output.terms.append(self.arena, .{ .column = column.path, .op = op, .value = try self.outputDatum(value_, column.type, column.element_type), .operand = value_ });
        if (output.terms.items.len > 256) return error.SqlProgramLimitExceeded;
    }

    fn bindConditions(self: Context, table_def: catalog.Table, predicate: ?*const ast.Predicate, output: *BoundPredicates) anyerror!void {
        const node = predicate orelse return;
        switch (node.*) {
            .comparison => |comparison| {
                const column = try table_def.column(comparison.field);
                // Native condition values cannot carry typed arrays or exact
                // NUMERIC limbs. Keep these comparisons in the typed residual
                // instead of interpreting their JSON placeholder as SQL NULL.
                if (column.type == .array or column.element_type == .numeric or @import("range_value.zig").kindFromType(column.type) != null or table_def.isDocumentRoot(column.name) or table_def.isDocumentVersion(column.name)) {
                    output.complete = false;
                    return;
                }
                if (column.type == .datetime and comparison.value == .parameter) {
                    const datum = try self.parameterDatum(comparison.value.parameter - 1);
                    if (datum.temporal()) |temporal| {
                        // Non-finite and other temporal domains cannot be
                        // represented by the legacy native timestamp envelope.
                        const nanos = if (temporal.kind() == .timestamp_tz) temporal.timestamp_tz.unixNanoseconds() else null;
                        if (nanos == null or nanos.? < 0 or nanos.? > std.math.maxInt(u64)) {
                            output.complete = false;
                            return;
                        }
                    }
                }
                // A declared NUMERIC parameter carries exact limbs, not a
                // native condition value. Mixed comparisons use the bound
                // scalar program's floating domain (including REAL/NUMERIC
                // comparisons in double precision), without narrowing the
                // operand to the physical column width.
                var comparison_operand: ?Json = null;
                if (column.type == .number and comparison.value == .parameter) {
                    const datum = try self.parameterDatum(comparison.value.parameter - 1);
                    if (datum.numeric != null or (datum.value == .float and !std.math.isFinite(datum.value.float))) {
                        output.complete = false;
                        return;
                    }
                    // Frame values already carry their declared width. A REAL
                    // stored cell must not narrow a declared float8 operand.
                    comparison_operand = if (datum.sql_null) .null else try describe.coerceAlloc(self.arena, datum.value, column.type);
                }
                // Row identity has a separate native key boundary; never
                // pretend it is a document property in a storage predicate.
                const bound_value = comparison_operand orelse try self.value(comparison.value, column);
                if (column.type == .json) {
                    output.complete = false;
                    return;
                }
                // A JSON-null value is not SQL NULL. The current native
                // condition envelope cannot express that operand, so retain
                // this comparison in the already bound typed residual.
                if (bound_value == .null and column.type == .json and comparison.value == .string) return;
                // In a conjunction, comparison with SQL NULL can never make
                // WHERE true. Bind all remaining terms for diagnostics, but
                // do not scan a relation merely to rediscover UNKNOWN per row.
                if (bound_value == .null) {
                    output.empty = true;
                    return;
                }
                if (std.mem.eql(u8, column.name, "_id")) {
                    if (comparison.op != .eq) {
                        output.complete = false;
                        return;
                    }
                    if (bound_value.string.len == 0) {
                        output.empty = true;
                    } else {
                        if (!std.unicode.utf8ValidateSlice(bound_value.string)) return error.SqlTypeMismatch;
                        if (output.primary_key) |previous| if (!std.mem.eql(u8, previous, bound_value.string)) {
                            output.empty = true;
                        };
                        output.primary_key = bound_value.string;
                    }
                    return;
                }
                const op: @FieldType(catalog.Condition, "op") = switch (comparison.op) {
                    inline else => |tag| @field(@FieldType(catalog.Condition, "op"), @tagName(tag)),
                };
                // Native relational predicates implement SQL three-valued
                // logic, including UNKNOWN for comparisons against NULL.
                try output.terms.append(self.arena, .{ .column = column.path, .op = op, .value = bound_value });
            },
            .is_null => |test_null| {
                const column = try table_def.column(test_null.field);
                if (table_def.isDocumentVersion(column.name)) {
                    if (!test_null.negated) output.empty = true;
                    return;
                }
                if (column.type == .json) {
                    output.complete = false;
                    return;
                }
                if (std.mem.eql(u8, column.name, "_id")) {
                    if (!test_null.negated) output.empty = true;
                    return;
                }
                try output.terms.append(self.arena, .{ .column = column.path, .op = if (test_null.negated) .is_not_null else .is_null });
            },
            .conjunction => |both| {
                try self.bindConditions(table_def, both.left, output);
                try self.bindConditions(table_def, both.right, output);
            },
            // Push down only safe conjuncts. The complete bound residual is
            // evaluated before OFFSET/LIMIT/counting or mutation staging.
            .disjunction, .negation, .scalar => output.complete = false,
        }
        if (output.terms.items.len > 256) return error.SqlProgramLimitExceeded;
    }

    pub fn select(unscoped: Context, requested: ast.Select) anyerror!Output {
        var self = unscoped;
        self.backend = @import("decision_eval.zig").scopedBackend(self.backend, self.binding);
        try @import("decision_eval.zig").validateStatement(self.arena, self.backend.decision_provider, self.binding, self.parameters);
        var statement = requested;
        if (statement.selection_prefix) {
            if (!statement.internal_projection) return error.InvalidSqlBackendResponse;
            const skipped = try self.offsetCount(statement.offset);
            if (skipped > self.limits.scan_rows) return error.SqlProgramLimitExceeded;
            if (try self.hasRowLimit(statement.limit) or statement.scalar_cardinality_limit) {
                const requested_rows = statement.capRows(try self.count(statement.limit, 2));
                const prefix = if (requested_rows == 0) 0 else std.math.add(usize, skipped, requested_rows) catch return error.SqlProgramLimitExceeded;
                if (prefix > self.limits.scan_rows) return error.SqlProgramLimitExceeded;
                statement.limit = .{ .integer = std.math.cast(i64, prefix) orelse return error.SqlProgramLimitExceeded };
            } else statement.limit = null;
            statement.offset = null;
            statement.scalar_cardinality_limit = false;
            statement.selection_prefix = false;
            // This transparent sorted cursor may forward a large skipped
            // prefix. Public output quotas still belong to its outer consumer;
            // storage scan, retained-memory and spill limits remain in force.
            self.limits.result_rows = self.limits.scan_rows;
        }
        if (statement.limit != null and !try self.hasRowLimit(statement.limit))
            statement.limit = if (statement.scalar_cardinality_limit) .{ .integer = 2 } else null;
        // A scalar child's internal two-row bound is physical demand, not an
        // implicit public response quota. Sort/group/window finishers must
        // deliver those two rows to the cardinality check instead of reporting
        // a result-size error for the unused third row.
        if (statement.scalar_cardinality_limit and statement.limit == null)
            statement.limit = .{ .integer = 2 };
        if (self.binding.relation != null) return @import("relation_runtime.zig").execute(self);
        if (self.binding.window != null) return @import("window_runtime.zig").execute(self, statement);
        if (self.binding.aggregate != null) return @import("aggregate_runtime.zig").execute(self, statement);
        const table_def = self.binding.table orelse return self.constantSelect(statement);
        const predicates = try self.conditions(table_def, statement.predicate);
        const limit = statement.capRows(try self.count(statement.limit, self.limits.result_rows));
        const offset = try self.offsetCount(statement.offset);
        if (limit > self.limits.result_rows or offset > self.limits.scan_rows) return error.SqlProgramLimitExceeded;
        var fields: std.ArrayList([]const u8) = .empty;
        const columns = self.binding.columns;
        if (statement.count_all) {
            // An empty native projection avoids materializing unused values.
        } else if (statement.columns.len == 0) {
            for (table_def.columns) |column| {
                try fields.append(self.arena, column.path);
            }
        } else {
            for (statement.columns) |projection| {
                if (projection.expression != null) {
                    try fields.append(self.arena, "");
                    continue;
                }
                const column = try table_def.column(projection.field);
                try fields.append(self.arena, column.path);
            }
        }
        // Native projection is a set; SQL output can repeat/alias a field.
        var native_fields: std.ArrayList([]const u8) = .empty;
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        for (fields.items) |field| {
            if (field.len == 0 or std.mem.eql(u8, field, "_id")) continue;
            const slot = try seen.getOrPut(self.arena, field);
            if (!slot.found_existing) try native_fields.append(self.arena, field);
        }
        for (self.binding.scalars.required) |ordinal| {
            const name = self.binding.scalars.columns[ordinal].name;
            if (std.mem.eql(u8, name, "_id")) continue;
            const slot = try seen.getOrPut(self.arena, name);
            if (!slot.found_existing) try native_fields.append(self.arena, name);
        }
        for (self.binding.order_keys) |key| switch (key.source) {
            .output, .expression => {},
            .column => |column| {
                if (std.mem.eql(u8, column.name, "_id")) continue;
                const slot = try seen.getOrPut(self.arena, column.path);
                if (!slot.found_existing) try native_fields.append(self.arena, column.path);
            },
        };
        var scan_state: ScanState = .{};
        defer scan_state.deinit();
        const requested_order = if (!self.binding.primary_order and !statement.count_all) try @import("describe.zig").scanOrder(self.arena, self.binding, statement) else &.{};
        const expression_equalities = try self.expressionEqualities(table_def, !self.binding.primary_order and predicates.primary_key == null and !predicates.empty and limit != 0);
        const scan_request: catalog.Scan = .{ .row_goal = if (statement.limit != null and predicates.complete and !statement.count_all) offset +| limit else null, .fields = native_fields.items, .primary_order = self.binding.primary_order, .order = requested_order, .primary_key = predicates.primary_key, .conditions = predicates.terms.items, .expression_equalities = expression_equalities, .limit = @intCast(self.limits.page_rows) };
        var ordered_source = false;
        if (self.backend.vtable.supports_scan_order and requested_order.len != 0 and limit != 0 and !predicates.empty) {
            try scan_state.open(self, table_def, scan_request);
            ordered_source = if (scan_state.cursor) |cursor| cursor.order_satisfied else false;
        }
        var top_k: ?operators.TopK = null;
        defer if (top_k) |*operator| operator.deinit();
        if (self.binding.order_keys.len != 0 and !self.binding.primary_order and !ordered_source and !statement.count_all and limit != 0) {
            const orders = try self.arena.alloc(operators.Order, self.binding.order_keys.len);
            for (self.binding.order_keys, orders) |key, *order| order.* = .{ .descending = key.descending, .nulls_first = key.nulls_first };
            top_k = try operators.TopK.initWithSpill(self.alloc, offset + limit + @intFromBool(statement.limit == null), orders, self.limits.retained_bytes, self.spill);
        }
        const deferred = try self.arena.alloc(bool, fields.items.len);
        @memset(deferred, false);
        if (top_k != null) for (self.binding.scalars.projections, 0..) |optional, index| {
            if (optional) |*program| deferred[index] = @import("decision_eval.zig").hasExternal(program);
        };
        for (self.binding.order_keys) |key| if (key.source == .output) {
            deferred[key.source.output] = false;
        };
        const defer_projection = std.mem.indexOfScalar(bool, deferred, true) != null;
        var rows: std.ArrayList([]const Json) = .empty;
        var delivered: usize = 0;
        var null_rows: std.ArrayList([]const bool) = .empty;
        var scanned: usize = 0;
        var visited: usize = 0;
        var page_count: usize = 0;
        var retained: usize = 0;
        var after: ?[]const u8 = null;
        defer if (after) |key| self.alloc.free(key);
        if (limit == 0) return .{ .columns = columns, .command_tag = "SELECT" };
        var metadata_counted = false;
        if (statement.count_all and statement.predicate == null and !predicates.empty) {
            const materialized_count = try @import("aggregate_materialization.zig").countFromProvider(self, table_def);
            const exact: ?u64 = if (materialized_count != null) materialized_count else try scan_state.count(self, table_def, .{ .fields = native_fields.items, .limit = self.limits.page_rows });
            if (exact) |exact_count| {
                _ = std.math.cast(i64, exact_count) orelse return error.SqlNumericOutOfRange;
                scanned = std.math.cast(usize, exact_count) orelse return error.SqlNumericOutOfRange;
                metadata_counted = true;
            }
        }
        while (!predicates.empty and !metadata_counted) {
            try self.checkpoint();
            page_count += 1;
            if (page_count > self.limits.scan_pages) return error.SqlProgramLimitExceeded;
            var page_arena = std.heap.ArenaAllocator.init(self.alloc);
            defer page_arena.deinit();
            // Native pages bound scan work independently of the number of
            // residual matches still needed. LIMIT 1 must not impose a
            // 1024-row scan ceiling on a selective scalar predicate.
            // Unfiltered, unsorted pulls can bound prefetch to output demand
            // without changing residual scan capacity or sort/count inputs.
            const wanted = if (top_k == null and !statement.count_all and self.binding.scalars.predicate == null)
                @min(self.limits.page_rows, (offset -| scanned) +| (limit -| delivered) +| @intFromBool(statement.limit == null))
            else
                self.limits.page_rows;
            var request = scan_request;
            request.after = after;
            request.limit = @intCast(wanted);
            const page = try scan_state.page(self, page_arena.allocator(), table_def, request);
            defer page.deinit();
            if (page.rows.len > wanted) return error.InvalidSqlBackendResponse;
            if (page.rows.len > self.limits.scan_rows -| visited) return error.SqlProgramLimitExceeded;
            var first: usize = 0;
            while (first < page.rows.len) {
                var decision_page = std.heap.ArenaAllocator.init(self.alloc);
                defer decision_page.deinit();
                const scratch = decision_page.allocator();
                const chunk_cells = try @import("decision_eval.zig").rowPage(scratch, self.binding.scalars, page.rows[first..], self.limits.page_rows, self.limits.page_bytes);
                const chunk_rows = page.rows[first..][0..chunk_cells.len];
                const page_cells = chunk_cells;
                const predicate_values = if (self.binding.scalars.predicate) |*predicate|
                    try @import("decision_eval.zig").evaluateBatchWithLimits(scratch, self.backend.decision_provider, predicate, page_cells, self.parameters, @import("decision_eval.zig").limitsFor(self.backend))
                else
                    null;
                var selected_cells: std.ArrayList([]const Datum) = .empty;
                const selected_positions = try scratch.alloc(?usize, chunk_rows.len);
                @memset(selected_positions, null);
                var page_scanned = scanned;
                for (page_cells, 0..) |cells, index| {
                    if (predicate_values) |values| {
                        if (values[index].sql_null) continue;
                        if (values[index].value != .bool) return error.SqlTypeMismatch;
                        if (!values[index].value.bool) continue;
                    }
                    page_scanned += 1;
                    if (statement.count_all or (top_k == null and page_scanned <= offset)) continue;
                    if (top_k == null and selected_cells.items.len >= limit - delivered) continue;
                    selected_positions[index] = selected_cells.items.len;
                    try selected_cells.append(scratch, cells);
                }
                const projection_values = try scratch.alloc(?[]const Datum, self.binding.scalars.projections.len);
                for (self.binding.scalars.projections, projection_values, 0..) |optional, *values, index| values.* = if (!deferred[index] and optional != null)
                    try @import("decision_eval.zig").evaluateBatchWithLimits(scratch, self.backend.decision_provider, &optional.?, selected_cells.items, self.parameters, @import("decision_eval.zig").limitsFor(self.backend))
                else
                    null;
                const order_values = try scratch.alloc(?[]const Datum, self.binding.scalars.orders.len);
                for (self.binding.scalars.orders, order_values) |optional, *values| values.* = if (top_k != null and optional != null)
                    try @import("decision_eval.zig").evaluateBatchWithLimits(scratch, self.backend.decision_provider, &optional.?, selected_cells.items, self.parameters, @import("decision_eval.zig").limitsFor(self.backend))
                else
                    null;
                if (!defer_projection and !statement.count_all and (top_k != null or (self.sink != null and self.sink.?.append_batch != null))) {
                    const selection = try scratch.alloc(usize, selected_cells.items.len);
                    const ordinals = try scratch.alloc(u64, selected_cells.items.len);
                    var selected_count: usize = 0;
                    var complete = false;
                    for (chunk_rows, 0..) |_, row_index| {
                        try self.checkpoint();
                        visited += 1;
                        if (predicate_values) |values| {
                            if (values[row_index].sql_null or !values[row_index].value.bool) continue;
                        }
                        scanned += 1;
                        if (top_k == null and scanned <= offset) continue;
                        if (top_k == null and delivered + selected_count == limit) {
                            if (statement.limit == null) return error.SqlResultTooLarge;
                            complete = true;
                            break;
                        }
                        selection[selected_count] = row_index;
                        ordinals[selected_count] = visited;
                        selected_count += 1;
                        if (top_k == null and delivered + selected_count == limit and statement.limit != null) {
                            complete = true;
                            break;
                        }
                    }
                    if (selected_count != 0) {
                        var view: ProjectedBatch = .{ .rows = chunk_rows, .selection = selection[0..selected_count], .positions = selected_positions, .fields = fields.items, .columns = columns, .expressions = projection_values };
                        // Keep declared coercion errors eager even for heap
                        // losers. Cache scalar columns once; validated native
                        // JSON needs no DOM to cross its declared JSON boundary.
                        if (top_k != null) {
                            const coerced = try scratch.alloc(?[]const Datum, columns.len);
                            @memset(coerced, null);
                            for (columns, 0..) |column, ordinal| {
                                if (column.type == .json) {
                                    for (0..selected_count) |lane| {
                                        if (try ProjectedBatch.frozen(&view, lane, ordinal) != null or try ProjectedBatch.json(&view, lane, ordinal) != null) continue;
                                        _ = try ProjectedBatch.cell(&view, scratch, lane, ordinal);
                                    }
                                    continue;
                                }
                                const values = try scratch.alloc(Datum, selected_count);
                                for (values, 0..) |*cell_, lane| cell_.* = try ProjectedBatch.cell(&view, scratch, lane, ordinal);
                                coerced[ordinal] = values;
                            }
                            view.coerced = coerced;
                        }
                        const batch = view.batch();
                        if (top_k) |*operator| {
                            const keys = try scratch.alloc([]const Datum, selected_count);
                            for (selection[0..selected_count], keys, 0..) |physical, *out, lane| {
                                const row_keys = try scratch.alloc(Datum, self.binding.order_keys.len);
                                for (self.binding.order_keys, row_keys) |key, *sort_key| sort_key.* = switch (key.source) {
                                    .output => |column| try batch.cell(scratch, lane, column),
                                    .expression => |column| order_values[column].?[selected_positions[physical].?],
                                    .column => |column| try describe.coerceDatumTyped(scratch, try chunk_rows[physical].cell(column.path), scalar.columnType(column)),
                                };
                                out.* = row_keys;
                            }
                            try operator.addBatch(scratch, batch, keys, ordinals[0..selected_count]);
                        } else {
                            const sink = self.sink.?;
                            try sink.append_batch.?(sink.ptr, batch);
                            delivered += selected_count;
                        }
                    }
                    if (complete) return .{ .columns = columns, .command_tag = "SELECT" };
                    first += chunk_rows.len;
                    continue;
                }
                for (chunk_rows, 0..) |row, row_index| {
                    try self.checkpoint();
                    visited += 1;
                    if (visited > self.limits.scan_rows) return error.SqlProgramLimitExceeded;
                    if (predicate_values) |values| {
                        if (values[row_index].sql_null) continue;
                        if (values[row_index].value != .bool) return error.SqlTypeMismatch;
                        if (!values[row_index].value.bool) continue;
                    }
                    scanned += 1;
                    if (statement.count_all) continue;
                    if (top_k) |*operator| {
                        const values = try scratch.alloc(Datum, fields.items.len + if (defer_projection) page_cells[row_index].len else @as(usize, 0));
                        if (defer_projection) @memcpy(values[fields.items.len..], page_cells[row_index]);
                        for (fields.items, values[0..fields.items.len], 0..) |field, *projected_value, index| projected_value.* = if (deferred[index]) .{} else if (index < projection_values.len and projection_values[index] != null)
                            projection_values[index].?[selected_positions[row_index].?]
                        else blk: {
                            const cell = try row.cell(field);
                            break :blk try describe.coerceDatumTyped(scratch, cell, scalar.columnType(columns[index]));
                        };
                        const keys = try scratch.alloc(Datum, self.binding.order_keys.len);
                        for (self.binding.order_keys, keys) |key, *out| out.* = switch (key.source) {
                            .output => |index| values[index],
                            .expression => |index| order_values[index].?[selected_positions[row_index].?],
                            .column => |column| blk: {
                                const cell = try row.cell(column.path);
                                break :blk try describe.coerceDatumTyped(scratch, cell, scalar.columnType(column));
                            },
                        };
                        try operator.add(.{ .values = values, .keys = keys, .ordinal = visited });
                        continue;
                    }
                    if (scanned <= offset) continue;
                    if (delivered == limit) {
                        if (statement.limit == null) return error.SqlResultTooLarge;
                        return .{ .columns = columns, .rows = rows.items, .sql_nulls = null_rows.items, .command_tag = "SELECT" };
                    }
                    if (self.sink) |sink| {
                        const values = try scratch.alloc(Datum, fields.items.len);
                        for (fields.items, columns, values, 0..) |field, column, *cell, index| {
                            const input = if (index < projection_values.len and projection_values[index] != null) projection_values[index].?[selected_positions[row_index].?] else try row.cell(field);
                            cell.* = try describe.coerceDatumTyped(scratch, input, scalar.columnType(column));
                        }
                        try sink.append(sink.ptr, values);
                        delivered += 1;
                        if (delivered == limit and statement.limit != null) return .{ .columns = columns, .command_tag = "SELECT" };
                        continue;
                    }
                    const cells = try self.arena.alloc(Json, fields.items.len);
                    const nulls = try self.arena.alloc(bool, fields.items.len);
                    for (fields.items, columns, cells, nulls, 0..) |field, column, *cell, *is_null, index| {
                        const program = if (index < self.binding.scalars.projections.len) self.binding.scalars.projections[index] else null;
                        const input_cell: catalog.Row.Cell = if (program) |expression| blk: {
                            _ = expression;
                            const evaluated = projection_values[index].?[selected_positions[row_index].?];
                            break :blk evaluated;
                        } else try row.cell(field);
                        const typed = try describe.coerceDatumTyped(self.arena, input_cell, scalar.columnType(column));
                        is_null.* = typed.sql_null;
                        // SQL bigint results are lossless even in JS SDKs.
                        cell.* = try self.outputDatum(typed, column.type, column.element_type);
                        retained = std.math.add(usize, retained, jsonSize(cell.*)) catch return error.SqlProgramLimitExceeded;
                        if (retained > self.limits.retained_bytes) return error.SqlProgramLimitExceeded;
                    }
                    try rows.append(self.arena, cells);
                    try null_rows.append(self.arena, nulls);
                    delivered += 1;
                    if (delivered == limit and statement.limit != null) return .{ .columns = columns, .rows = rows.items, .sql_nulls = null_rows.items, .command_tag = "SELECT" };
                }
                first += chunk_rows.len;
            }
            const next = page.after orelse break;
            if (!scan_state.retained(self)) return error.SqlStatementSnapshotRequired;
            if (after) |previous| if (std.mem.eql(u8, previous, next)) return error.InvalidSqlBackendResponse;
            const owned_next = try self.alloc.dupe(u8, next);
            if (after) |previous| self.alloc.free(previous);
            after = owned_next;
        }
        if (top_k) |*operator| {
            if (self.sink != null and !defer_projection) return self.emitTop(operator, offset, limit, statement.limit == null);
            const ordered = try operator.finishPage(self.arena, offset, limit + @intFromBool(statement.limit == null));
            const remaining = ordered.len;
            const start = operator.released;
            if (statement.limit == null and remaining > limit) return error.SqlResultTooLarge;
            const selected = ordered[0..@min(remaining, limit)];
            if (defer_projection) {
                var first: usize = 0;
                while (first < selected.len) {
                    try self.checkpoint();
                    var page = std.heap.ArenaAllocator.init(self.alloc);
                    defer page.deinit();
                    const scratch = page.allocator();
                    var inputs: std.ArrayList([]const Datum) = .empty;
                    var bytes: usize = 0;
                    for (selected[first..]) |row| {
                        const input = row.values[fields.items.len..];
                        try inputs.append(scratch, input);
                        for (input) |cell| bytes +|= try operators.datumBytes(cell);
                        if (inputs.items.len >= self.limits.page_rows or bytes >= self.limits.page_bytes) break;
                    }
                    const output = try scratch.alloc([]Datum, inputs.items.len);
                    for (selected[first..][0..inputs.items.len], output) |row, *cells| cells.* = try scratch.dupe(Datum, row.values[0..fields.items.len]);
                    var stateful = false;
                    for (self.binding.scalars.projections) |optional| if (optional) |*program| {
                        stateful = stateful or program.uses_sequences;
                    };
                    if (stateful) {
                        for (inputs.items, output) |input, cells_| for (deferred, 0..) |needed, column| {
                            if (needed) cells_[column] = try self.evaluate(scratch, self.binding.scalars.projections[column].?, input);
                        };
                    } else for (deferred, 0..) |needed, column| if (needed) {
                        const program = self.binding.scalars.projections[column].?;
                        const values = try @import("decision_eval.zig").evaluateBatchWithLimits(scratch, self.backend.decision_provider, &program, inputs.items, self.parameters, @import("decision_eval.zig").limitsFor(self.backend));
                        for (output, values) |cells, datum| cells[column] = datum;
                    };
                    for (output, first + start..) |values, index| {
                        if (self.sink) |sink| {
                            try sink.append(sink.ptr, values);
                            operator.releaseFinishedRow(index);
                            continue;
                        }
                        const cells = try self.arena.alloc(Json, values.len);
                        const nulls = try self.arena.alloc(bool, values.len);
                        for (values, cells, nulls, columns) |datum, *cell, *flag, column| {
                            cell.* = try self.outputDatum(datum, column.type, column.element_type);
                            flag.* = datum.sql_null;
                        }
                        try rows.append(self.arena, cells);
                        try null_rows.append(self.arena, nulls);
                        operator.releaseFinishedRow(index);
                    }
                    first += inputs.items.len;
                }
            } else for (ordered[0..@min(remaining, limit)], start..) |row, index| {
                try self.checkpoint();
                const cells = try self.arena.alloc(Json, row.values.len);
                const nulls = try self.arena.alloc(bool, row.values.len);
                for (row.values, cells, nulls, columns) |value_, *cell, *is_null, column| {
                    cell.* = try self.outputDatum(value_, column.type, column.element_type);
                    is_null.* = value_.sql_null;
                }
                try rows.append(self.arena, cells);
                try null_rows.append(self.arena, nulls);
                operator.releaseFinishedRow(index);
            }
        }
        if (statement.count_all and offset == 0) {
            const cells = try self.arena.alloc(Json, 1);
            cells[0] = try self.outputValue(.{ .integer = std.math.cast(i64, scanned) orelse return error.SqlNumericOutOfRange });
            try rows.append(self.arena, cells);
            const nulls = try self.arena.alloc(bool, 1);
            nulls[0] = false;
            try null_rows.append(self.arena, nulls);
        }
        return .{ .columns = columns, .rows = rows.items, .sql_nulls = null_rows.items, .command_tag = "SELECT" };
    }

    pub fn projectValues(self: Context, alloc: std.mem.Allocator, row: catalog.Row, fields: []const []const u8, expression_cells: []const Datum) ![]const Datum {
        const values = try alloc.alloc(Datum, fields.len);
        for (fields, self.binding.columns, values, 0..) |field, column, *out, index| {
            const program = if (index < self.binding.scalars.projections.len) self.binding.scalars.projections[index] else null;
            const input = if (program) |expression| try self.evaluate(alloc, expression, expression_cells) else blk: {
                const cell = try row.cell(field);
                break :blk cell;
            };
            out.* = try describe.coerceDatumTyped(alloc, input, scalar.columnType(column));
        }
        return values;
    }

    /// Resolve external projection columns together for a bounded input page.
    /// The caller selects predicate/OFFSET/LIMIT survivors before calling this,
    /// so discarded rows never invoke projection providers.
    pub fn projectValuesBatch(self: Context, alloc: std.mem.Allocator, rows: []const catalog.Row, fields: []const []const u8, cells: []const []const Datum) ![]const []const Datum {
        return self.projectValuesBatchDemand(alloc, rows, fields, cells, null);
    }
    pub fn projectValuesBatchDemand(self: Context, alloc: std.mem.Allocator, rows: []const catalog.Row, fields: []const []const u8, cells: []const []const Datum, needed: ?[]const bool) ![]const []const Datum {
        if (rows.len != cells.len) return error.InvalidSqlBackendResponse;
        var ordered = false;
        for (self.binding.scalars.projections) |optional| if (optional) |*program| {
            ordered = ordered or @import("decision_eval.zig").hasSequences(program);
        };
        if (ordered) {
            const output = try alloc.alloc([]const Datum, rows.len);
            for (rows, cells, output) |row, source, *values| {
                try self.checkpoint();
                const projected = try alloc.alloc(Datum, fields.len);
                for (fields, self.binding.columns, projected, 0..) |field, column, *cell_, index| {
                    if (needed != null and !needed.?[index]) {
                        cell_.* = .{};
                        continue;
                    }
                    const program = if (index < self.binding.scalars.projections.len) self.binding.scalars.projections[index] else null;
                    const input = if (program) |expression| try self.evaluate(alloc, expression, source) else try row.cell(field);
                    cell_.* = try describe.coerceDatumTyped(alloc, input, scalar.columnType(column));
                }
                values.* = projected;
            }
            return output;
        }
        const columns = try alloc.alloc(?[]const Datum, self.binding.scalars.projections.len);
        for (self.binding.scalars.projections, columns, 0..) |optional, *values, index| values.* = if (needed != null and !needed.?[index]) null else if (optional) |*program|
            try @import("decision_eval.zig").evaluateBatchWithLimits(alloc, self.backend.decision_provider, program, cells, self.parameters, @import("decision_eval.zig").limitsFor(self.backend))
        else
            null;
        const output = try alloc.alloc([]const Datum, rows.len);
        for (rows, output, 0..) |row, *values, row_index| {
            const projected = try alloc.alloc(Datum, fields.len);
            for (fields, self.binding.columns, projected, 0..) |field, column, *out, index| {
                if (needed != null and !needed.?[index]) {
                    out.* = .{};
                    continue;
                }
                const input = if (index < columns.len and columns[index] != null) columns[index].?[row_index] else blk: {
                    const cell = try row.cell(field);
                    break :blk cell;
                };
                out.* = try describe.coerceDatumTyped(alloc, input, scalar.columnType(column));
            }
            values.* = projected;
        }
        return output;
    }

    fn constantSelect(self: Context, statement: ast.Select) !Output {
        const limit = statement.capRows(try self.count(statement.limit, self.limits.result_rows));
        const offset = try self.offsetCount(statement.offset);
        if (limit > self.limits.result_rows or offset > self.limits.scan_rows) return error.SqlProgramLimitExceeded;
        const columns = self.binding.columns;
        if (limit == 0 or offset != 0) return .{ .columns = columns, .command_tag = "SELECT" };
        try self.checkpoint();
        var evaluation = std.heap.ArenaAllocator.init(self.alloc);
        defer evaluation.deinit();
        const matches = try self.matchesRow(evaluation.allocator(), &.{});
        if (!evaluation.reset(.retain_capacity)) return error.OutOfMemory;
        if (!matches and !statement.count_all) return .{ .columns = columns, .command_tag = "SELECT" };
        if (self.sink) |sink| {
            const values = try self.arena.alloc(Datum, columns.len);
            if (statement.count_all) {
                values[0] = Datum.json(.{ .integer = @intFromBool(matches) });
            } else for (self.binding.scalars.projections, values) |optional, *out| {
                const program = optional orelse return error.InvalidSqlBackendResponse;
                if (!evaluation.reset(.retain_capacity)) return error.OutOfMemory;
                out.* = try operators.cloneDatum(self.arena, try self.evaluate(evaluation.allocator(), program, &.{}));
            }
            try sink.append(sink.ptr, values);
            return .{ .columns = columns, .command_tag = "SELECT" };
        }
        const rows = try self.arena.alloc([]const Json, 1);
        const cells = try self.arena.alloc(Json, columns.len);
        const null_rows = try self.arena.alloc([]const bool, 1);
        const nulls = try self.arena.alloc(bool, columns.len);
        if (statement.count_all) {
            cells[0] = try self.outputValue(.{ .integer = @intFromBool(matches) });
            nulls[0] = false;
        } else for (self.binding.scalars.projections, cells, nulls) |optional, *cell, *is_null| {
            const program = optional orelse return error.InvalidSqlBackendResponse;
            if (!evaluation.reset(.retain_capacity)) return error.OutOfMemory;
            const evaluated = try self.evaluate(evaluation.allocator(), program, &.{});
            cell.* = try self.outputDatum(evaluated, program.output_type.kind, program.output_type.element_type);
            is_null.* = evaluated.sql_null;
        }
        rows[0] = cells;
        null_rows[0] = nulls;
        return .{ .columns = columns, .rows = rows, .sql_nulls = null_rows, .command_tag = "SELECT" };
    }

    fn validateInsertRequired(self: Context, table: catalog.Table, object: std.json.ObjectMap) !void {
        for (table.columns) |column| {
            if (column.nullable or column.generated or column.defaulted or std.mem.eql(u8, column.name, "_id")) continue;
            const present = if (column.document_path.len != 0) @import("document_path.zig").get(.{ .object = object }, column.document_path) != null else object.contains(column.path);
            if (!present) return @import("errors.zig").notNull(self.backend.error_context, column.name);
        }
    }

    fn insert(self: Context, statement: ast.Insert) !Output {
        if (statement.source) |source| return self.insertSelect(statement, source.*);
        const table_def = self.binding.table orelse return error.InvalidSqlBackendResponse;
        try table_def.validateDocumentWriteColumns(statement.columns);
        if (statement.rows.len > self.limits.mutation_rows) return error.SqlProgramLimitExceeded;
        const columns = try self.arena.alloc(catalog.Column, statement.columns.len);
        var key_index: ?usize = null;
        for (statement.columns, columns, 0..) |name, *column, i| {
            column.* = try table_def.column(name);
            if (std.mem.eql(u8, name, "_id")) key_index = i;
            for (statement.columns[0..i]) |previous| if (std.mem.eql(u8, previous, name)) return error.DuplicateColumn;
        }
        const mutations = try self.arena.alloc(catalog.Mutation, statement.rows.len);
        var keys: std.StringHashMapUnmanaged(void) = .empty;
        var retained: usize = 0;
        var preparation: ?catalog.MutationPreparation = null;
        defer if (preparation) |owner| owner.close(owner.ptr);
        var stateful_defaults = false;
        for (table_def.columns) |column| stateful_defaults = stateful_defaults or column.sequence_default != null;
        var first: usize = 0;
        while (first < statement.rows.len) {
            var page_arena = std.heap.ArenaAllocator.init(self.alloc);
            defer page_arena.deinit();
            const page = try self.insertDecisionPage(page_arena.allocator(), statement, first);
            for (statement.rows[first..page.end], mutations[first..page.end], first..) |row, *mutation, row_index| {
                try self.checkpoint();
                if (row.len != columns.len) return error.InvalidSqlParameters;
                const key_datum: @import("scalar.zig").Datum = if (key_index) |key_at| blk: {
                    if (!statement.isDefault(row_index, key_at)) break :blk try self.insertValue(row[key_at], columns[key_at], row_index, key_at, page.values[row_index - first][key_at]);
                    break :blk .{ .value = .{ .string = try (self.backend.vtable.generate_row_id orelse return error.SqlRowIdentityRequired)(self.backend.ptr, self.arena) }, .sql_null = false };
                } else .{ .value = .{ .string = try (self.backend.vtable.generate_row_id orelse return error.SqlRowIdentityRequired)(self.backend.ptr, self.arena) }, .sql_null = false };
                const key = try clone(self.arena, key_datum.value);
                if (key_datum.sql_null or key != .string or key.string.len == 0) return error.SqlRowIdentityRequired;
                if (!std.unicode.utf8ValidateSlice(key.string)) return error.SqlTypeMismatch;
                if ((try keys.getOrPut(self.arena, key.string)).found_existing and (statement.conflict == null or !@import("conflict.zig").allowsDuplicateKeys(statement.conflict.?))) return @import("conflict.zig").duplicateRowError(statement.conflict);
                var object: std.json.ObjectMap = .empty;
                var json_null_fields: std.ArrayList([]const u8) = .empty;
                for (columns, row, 0..) |column, item, i| {
                    if (key_index != null and i == key_index.?) continue;
                    if (statement.isDefault(row_index, i) or statement.ignoresIdentity(column)) continue;
                    const datum = try self.insertValue(item, column, row_index, i, page.values[row_index - first][i]);
                    const typed = try self.storageDatumTyped(datum, column, column.element_type);
                    if (table_def.isDocumentRoot(column.name)) {
                        if (datum.sql_null or typed != .object) return error.SqlTypeMismatch;
                        object = typed.object;
                        try json_null_fields.appendSlice(self.arena, try table_def.documentRootNulls(self.arena, typed));
                        continue;
                    }
                    const json_null = column.type == .json and typed == .null and !datum.sql_null;
                    if (json_null) try json_null_fields.append(self.arena, column.path);
                    try column.assignStorageValue(self.arena, &object, typed, datum.sql_null);
                }
                const document: Json = .{ .object = object };
                retained = std.math.add(usize, retained, jsonSize(document) + key.string.len) catch return error.SqlProgramLimitExceeded;
                if (retained > self.limits.retained_bytes) return error.SqlProgramLimitExceeded;
                try self.validateInsertRequired(table_def, object);
                mutation.* = .{ .key = key.string, .expected_version = 0, .unique_absence = true, .row = document, .json_null_fields = json_null_fields.items };
                if (stateful_defaults) {
                    if (preparation == null) preparation = try (self.backend.vtable.open_mutation_preparation orelse return error.UnsupportedSqlExecution)(self.backend.ptr, self.arena, table_def, mutation.key, statement.returning != null or statement.conflict != null or table_def.storage_mode == .document);
                    const owner = preparation.?;
                    const normalized = try owner.prepare(owner.ptr, self.arena, mutations[row_index .. row_index + 1]);
                    if (normalized.len != 1) return error.InvalidSqlBackendResponse;
                    mutation.* = normalized[0];
                }
            }
            first = page.end;
            try self.checkpoint();
        }
        if (preparation) |owner| {
            try owner.finish(owner.ptr, self.arena);
            owner.close(owner.ptr);
            preparation = null;
        }
        const resolved = if (statement.conflict) |clause| try @import("conflict.zig").resolve(self, table_def, clause, self.binding.conflict orelse return error.InvalidSqlBackendResponse, mutations, &.{}) else mutations;
        return self.commitMutationsInner(table_def, resolved, "INSERT", statement.returning, stateful_defaults and statement.conflict == null);
    }

    fn insertSelect(self: Context, statement: ast.Insert, source: ast.Select) !Output {
        const table = self.binding.table orelse return error.InvalidSqlBackendResponse;
        try table.validateDocumentWriteColumns(statement.columns);
        const bound = self.binding.insert_source orelse return error.InvalidSqlBackendResponse;
        const target_columns = try self.arena.alloc(catalog.Column, statement.columns.len);
        for (statement.columns, target_columns) |name, *column| column.* = try table.column(name);
        var input = self;
        input.binding = bound.*;
        input.typed_output = true;
        input.limits.result_rows = self.limits.mutation_rows;
        // Materialize a bounded statement result before any mutation. This
        // also releases source cursors before writer admission and prevents
        // self-inserts from reading their own writes (Halloween problem).
        const selected = try input.typedQuery(source);
        defer selected.close();
        const capture_count = if (statement.conflict) |clause| clause.capture_count else 0;
        if (bound.columns.len != statement.columns.len + capture_count or selected.count() > self.limits.mutation_rows) return error.InvalidSqlBackendResponse;
        if (statement.values_source_rows.len != 0 and selected.count() != statement.values_source_rows.len) return error.InvalidSqlBackendResponse;
        const mutations = try self.arena.alloc(catalog.Mutation, selected.count());
        const captured = try self.arena.alloc([]const Datum, selected.count());
        var keys: std.StringHashMapUnmanaged(void) = .empty;
        var scratch = std.heap.ArenaAllocator.init(self.alloc);
        defer scratch.deinit();
        var retained: usize = 0;
        for (mutations, 0..) |*mutation, row_index| {
            try self.checkpoint();
            _ = scratch.reset(.retain_capacity);
            const row = (try selected.next(scratch.allocator())) orelse return error.InvalidSqlBackendResponse;
            if (row.len != statement.columns.len + capture_count) return error.InvalidSqlBackendResponse;
            if (capture_count != 0) {
                const cells = try self.arena.alloc(Datum, capture_count);
                for (cells, row[statement.columns.len..]) |*cell, value_| {
                    cell.* = try operators.cloneDatum(self.arena, value_);
                    retained = std.math.add(usize, retained, try operators.datumBytes(value_)) catch return error.SqlProgramLimitExceeded;
                }
                captured[row_index] = cells;
            } else captured[row_index] = &.{};
            var object: std.json.ObjectMap = .empty;
            var json_null_fields: std.ArrayList([]const u8) = .empty;
            var key: ?[]const u8 = null;
            for (target_columns, bound.columns[0..statement.columns.len], row[0..statement.columns.len], 0..) |target, source_column, value_, cell_index| {
                if (statement.isDefault(row_index, cell_index) or statement.ignoresIdentity(target)) continue;
                if (!value_.sql_null and source_column.type != target.type and target.type != .string and !@import("builtin_cast.zig").numericAssignmentAllowed(source_column.type, target.type) and !(statement.values_source_rows.len != 0 and source_column.type == .string and (target.type == .datetime or target.type == .json or target.type == .uuid))) return error.SqlTypeMismatch;
                const typed = try self.storageDatumTyped(value_, target, @import("scalar.zig").assignmentSourceIdentity(source_column.type, source_column.element_type));
                if (std.mem.eql(u8, target.name, "_id")) {
                    if (value_.sql_null or typed != .string or typed.string.len == 0) return error.SqlRowIdentityRequired;
                    if (!std.unicode.utf8ValidateSlice(typed.string)) return error.SqlTypeMismatch;
                    key = typed.string;
                } else if (table.isDocumentRoot(target.name)) {
                    if (value_.sql_null or typed != .object) return error.SqlTypeMismatch;
                    object = typed.object;
                    try json_null_fields.appendSlice(self.arena, try table.documentRootNulls(self.arena, typed));
                } else {
                    if (target.type == .json and !value_.sql_null and typed == .null) try json_null_fields.append(self.arena, target.path);
                    try target.assignStorageValue(self.arena, &object, typed, value_.sql_null);
                }
            }
            const identity = key orelse try (self.backend.vtable.generate_row_id orelse return error.SqlRowIdentityRequired)(self.backend.ptr, self.arena);
            if (identity.len == 0 or !std.unicode.utf8ValidateSlice(identity)) return error.InvalidSqlBackendResponse;
            if ((try keys.getOrPut(self.arena, identity)).found_existing and (statement.conflict == null or !@import("conflict.zig").allowsDuplicateKeys(statement.conflict.?))) return @import("conflict.zig").duplicateRowError(statement.conflict);
            retained = std.math.add(usize, retained, jsonSize(.{ .object = object }) + identity.len) catch return error.SqlProgramLimitExceeded;
            if (retained > self.limits.retained_bytes) return error.SqlProgramLimitExceeded;
            try self.validateInsertRequired(table, object);
            mutation.* = .{ .key = identity, .expected_version = 0, .unique_absence = true, .row = .{ .object = object }, .json_null_fields = json_null_fields.items };
        }
        try self.checkpoint();
        const resolved = if (statement.conflict) |clause| try @import("conflict.zig").resolve(self, table, clause, self.binding.conflict orelse return error.InvalidSqlBackendResponse, mutations, captured) else mutations;
        return self.commitMutations(table, resolved, "INSERT", statement.returning);
    }

    const InsertDecisionPage = struct { end: usize, values: []const []const ?Datum };

    fn insertDecisionPage(self: Context, scratch: std.mem.Allocator, statement: ast.Insert, first: usize) !InsertDecisionPage {
        const decision = @import("decision_eval.zig");
        var budget: decision.PageBudget = .{ .row_limit = self.limits.page_rows, .byte_limit = self.limits.page_bytes };
        var end = first;
        const table = self.binding.table orelse return error.InvalidSqlBackendResponse;
        var defaults_have_effects = false;
        for (table.columns) |column| defaults_have_effects = defaults_have_effects or column.sequence_default != null;
        while (end < statement.rows.len) {
            var stateful = defaults_have_effects;
            if (self.binding.scalars.insert_rows.len != 0) for (self.binding.scalars.insert_rows[end]) |optional| {
                if (optional) |program| stateful = stateful or program.uses_sequences;
            };
            if (stateful and end != first) break;
            var bytes: usize = 0;
            if (self.binding.scalars.insert_rows.len != 0) {
                for (self.binding.scalars.insert_rows[end], 0..) |optional, column| {
                    if (statement.isDefault(end, column) or statement.ignoresIdentity(try table.column(statement.columns[column]))) continue;
                    if (optional) |program| for (program.instructions) |instruction| {
                        const input_value = switch (instruction.operation) {
                            .literal => |literal_value| Datum.fromJson(literal_value),
                            .parameter => |index| try self.parameterDatum(index),
                            else => continue,
                        };
                        bytes +|= try operators.datumBytes(input_value);
                    };
                }
            }
            end += 1;
            if (budget.addBytes(bytes) or stateful) break;
        }
        const values = try scratch.alloc([]?Datum, end - first);
        for (statement.rows[first..end], values) |row, *output| {
            output.* = try scratch.alloc(?Datum, row.len);
            @memset(output.*, null);
        }
        for (0..statement.columns.len) |column| {
            var programs: std.ArrayList(*const @import("scalar.zig").Program) = .empty;
            var positions: std.ArrayList(usize) = .empty;
            if (self.binding.scalars.insert_rows.len != 0) for (self.binding.scalars.insert_rows[first..end], first..) |row, index| {
                if (statement.isDefault(index, column) or statement.ignoresIdentity(try table.column(statement.columns[column]))) continue;
                if (row[column]) |*program| {
                    try programs.append(scratch, program);
                    try positions.append(scratch, index - first);
                }
            };
            const inputs = try scratch.alloc([]const Datum, programs.items.len);
            @memset(inputs, &.{});
            const evaluated = try decision.evaluateInvocationsWithLimits(scratch, self.backend.decision_provider, programs.items, inputs, self.parameters, @import("decision_eval.zig").limitsFor(self.backend));
            for (positions.items, evaluated) |position, datum| values[position][column] = datum;
        }
        return .{ .end = end, .values = values };
    }

    fn insertValue(self: Context, literal: ast.Value, column: catalog.Column, row: usize, cell: usize, evaluated: ?Datum) !Datum {
        if (self.binding.scalars.insert_rows.len != 0 and self.binding.scalars.insert_rows[row][cell] != null) {
            const result = evaluated orelse return error.InvalidSqlProgram;
            return describe.assignDatum(self.arena, result, self.binding.scalars.insert_rows[row][cell].?.output_type.element_type, column.type, column.element_type, .{ .output_bytes = self.limits.retained_bytes });
        }
        if (literal == .parameter) {
            if (literal.parameter == 0) return error.InvalidSqlParameters;
            return describe.assignDatum(self.arena, try self.parameterDatum(literal.parameter - 1), self.binding.parameter_descriptors[literal.parameter - 1].element_type, column.type, column.element_type, .{ .output_bytes = self.limits.retained_bytes });
        }
        const result = try self.value(literal, column);
        return .{ .value = result, .sql_null = result == .null and !(column.type == .json and literal == .string) };
    }

    const MutationDecisionPage = struct {
        cells: []const []const Datum,
        positions: []const ?usize,
        assignments: []const ?[]const Datum,
    };

    /// Predicate demand is resolved before assignment demand. Scratch belongs
    /// to one row/byte-bounded page; only staged postimages outlive the page.
    fn mutationDecisionPage(self: Context, scratch: std.mem.Allocator, rows: []const catalog.Row, remaining: usize) !MutationDecisionPage {
        const decision = @import("decision_eval.zig");
        var cells: std.ArrayList([]const Datum) = .empty;
        var bytes: usize = 0;
        for (rows) |row| {
            try self.checkpoint();
            const values = try self.binding.scalars.cells(scratch, row);
            try cells.append(scratch, values);
            for (values) |cell| bytes +|= try operators.datumBytes(cell);
            if (cells.items.len >= self.limits.page_rows or bytes >= self.limits.page_bytes) break;
        }
        const predicates = if (self.binding.scalars.predicate) |*program|
            try decision.evaluateBatchWithLimits(scratch, self.backend.decision_provider, program, cells.items, self.parameters, @import("decision_eval.zig").limitsFor(self.backend))
        else
            null;
        const positions = try scratch.alloc(?usize, cells.items.len);
        @memset(positions, null);
        var accepted: std.ArrayList([]const Datum) = .empty;
        for (cells.items, 0..) |row, index| {
            if (predicates) |values| {
                if (values[index].sql_null) continue;
                if (values[index].value != .bool) return error.SqlTypeMismatch;
                if (!values[index].value.bool) continue;
            }
            if (accepted.items.len == remaining) return error.SqlProgramLimitExceeded;
            positions[index] = accepted.items.len;
            try accepted.append(scratch, row);
        }
        const assignments = try scratch.alloc(?[]const Datum, self.binding.scalars.assignments.len);
        for (self.binding.scalars.assignments, assignments) |optional, *values| values.* = if (optional) |*program|
            try decision.evaluateBatchWithLimits(scratch, self.backend.decision_provider, program, accepted.items, self.parameters, @import("decision_eval.zig").limitsFor(self.backend))
        else
            null;
        return .{ .cells = cells.items, .positions = positions, .assignments = assignments };
    }

    fn change(self: Context, _: ast.Name, predicate: ?*const ast.Predicate, assignments: ?[]const ast.Assignment, requested_returning: ?[]const ast.Projection) !Output {
        const returning = if (requested_returning != null) self.binding.returning_projections else null;
        const table_def = self.binding.table orelse return error.InvalidSqlBackendResponse;
        var root_assignment = false;
        if (assignments) |items| {
            for (items) |item| root_assignment = root_assignment or table_def.isDocumentRoot(item.field);
            if (root_assignment and items.len != 1) return error.UnsupportedSqlShape;
        }
        const predicates = try self.conditions(table_def, predicate);
        // Validate assignments before reading or staging any row.
        const Assignment = struct { column: catalog.Column, value: Json, sql_null: bool, program: ?@import("scalar.zig").Program = null };
        const bound_assignments = try self.arena.alloc(Assignment, if (assignments) |items| items.len else 0);
        var replaced: std.StringHashMapUnmanaged(void) = .empty;
        if (assignments) |items| for (items, bound_assignments, 0..) |item, *bound, i| {
            const column = try table_def.column(item.field);
            if (std.mem.eql(u8, column.name, "_id")) return error.UnsupportedSqlExecution;
            for (items[0..i]) |previous| if (std.mem.eql(u8, previous.field, item.field)) return error.DuplicateColumn;
            var program = if (i < self.binding.scalars.assignments.len) self.binding.scalars.assignments[i] else null;
            var typed: Json = .null;
            var sql_null = true;
            if (program != null and item.expression == null) {
                // Prepared literals and direct parameters are statement
                // constants. Retain their typed value once, not per row;
                // exact NUMERIC literals must never use coarse JSON coercion.
                const value_ = try self.evaluate(self.arena, program.?, &.{});
                typed = try self.storageDatumTyped(value_, column, program.?.output_type.element_type);
                sql_null = value_.sql_null;
                program = null;
            } else if (program == null) {
                typed = try self.value(item.value, column);
                sql_null = typed == .null and !(column.type == .json and item.value == .string);
            }
            if (program == null and sql_null and !column.nullable) return @import("errors.zig").notNull(self.backend.error_context, column.name);
            // These constants are immutable for the entire statement. Own
            // them once, then share them across the prepared replacement rows.
            bound.* = .{ .column = column, .value = try clone(self.arena, typed), .sql_null = sql_null, .program = program };
            try replaced.put(self.arena, column.path, {});
        };
        var fields: std.ArrayList([]const u8) = .empty;
        // Document updates preserve untouched fields from the pinned raw
        // postimage below, not from independently coerced SQL projections.
        // Fetch only expression/predicate inputs; cold schema fields may be
        // large or may not satisfy a later SQL view's declared scalar type.
        if (assignments != null and table_def.storage_mode != .document) for (table_def.columns) |column| {
            // The native row version still fences the entire row. Loading an
            // overwritten value adds no conflict protection and can dominate
            // I/O/memory for wide JSON/blob-like columns.
            if (!column.generated and !replaced.contains(column.path)) try fields.append(self.arena, column.path);
        };
        if (assignments == null and returning != null) {
            if (returning.?.len == 0) {
                for (table_def.columns) |column| try fields.append(self.arena, column.path);
            } else for (returning.?) |projection| {
                if (projection.expression == null and !std.mem.eql(u8, projection.field, "_id")) {
                    const path = (try table_def.column(projection.field)).path;
                    if (!contains(fields.items, path)) try fields.append(self.arena, path);
                }
            }
            const projection = self.binding.returning.?;
            for (projection.scalars.required) |ordinal| {
                const name = projection.scalars.columns[ordinal].name;
                if (!std.mem.eql(u8, name, "_id") and !contains(fields.items, name)) try fields.append(self.arena, name);
            }
        }
        for (self.binding.scalars.required) |ordinal| {
            const name = self.binding.scalars.columns[ordinal].name;
            if (std.mem.eql(u8, name, "_id")) continue;
            var present = false;
            for (fields.items) |field| if (std.mem.eql(u8, field, name)) {
                present = true;
                break;
            };
            if (!present) try fields.append(self.arena, name);
        }
        var mutations: std.ArrayList(catalog.Mutation) = .empty;
        var retained: usize = 0;
        var after: ?[]const u8 = null;
        var page_count: usize = 0;
        var visited: usize = 0;
        defer if (after) |key| self.alloc.free(key);
        var scan_state: ScanState = .{};
        defer scan_state.deinit();
        const decision = @import("decision_eval.zig");
        var external = if (self.binding.scalars.predicate) |*program| decision.hasExternal(program) else false;
        for (self.binding.scalars.assignments) |optional| if (optional) |*program| {
            external = external or decision.hasExternal(program);
        };
        while (!predicates.empty) {
            try self.checkpoint();
            page_count += 1;
            if (page_count > self.limits.scan_pages) return error.SqlProgramLimitExceeded;
            var page_arena = std.heap.ArenaAllocator.init(self.alloc);
            defer page_arena.deinit();
            const page = try scan_state.page(self, page_arena.allocator(), table_def, .{ .fields = fields.items, .include_primary_digest = true, .include_document = table_def.storage_mode == .document and assignments != null, .primary_key = predicates.primary_key, .conditions = predicates.terms.items, .after = after, .limit = self.limits.page_rows });
            defer page.deinit();
            if (page.rows.len > self.limits.page_rows) return error.InvalidSqlBackendResponse;
            if (page.rows.len > self.limits.scan_rows -| visited) return error.SqlProgramLimitExceeded;
            var retained_layout: ?*const catalog.Row.TypedLayout = null;
            // Grow once per bounded native page. Arena-backed geometric growth
            // otherwise retains every superseded per-row staging buffer.
            try mutations.ensureUnusedCapacity(self.arena, @min(page.rows.len, self.limits.mutation_rows - mutations.items.len));
            var first: usize = 0;
            while (first < page.rows.len) {
                var decision_arena = std.heap.ArenaAllocator.init(self.alloc);
                defer decision_arena.deinit();
                const scratch = if (external) decision_arena.allocator() else page_arena.allocator();
                const evaluated = if (external) try self.mutationDecisionPage(scratch, page.rows[first..], self.limits.mutation_rows - mutations.items.len) else null;
                const end = first + if (evaluated) |batch| batch.cells.len else page.rows.len;
                for (page.rows[first..end], 0..) |row, row_index| {
                    try self.checkpoint();
                    visited += 1;
                    if (visited > self.limits.scan_rows) return error.SqlProgramLimitExceeded;
                    const expression_cells = if (evaluated) |batch| batch.cells[row_index] else try self.binding.scalars.cells(scratch, row);
                    if (evaluated) |batch| {
                        if (batch.positions[row_index] == null) continue;
                    } else if (!try self.matchesRow(scratch, expression_cells)) continue;
                    if (mutations.items.len >= self.limits.mutation_rows) return error.SqlProgramLimitExceeded;
                    var document: ?Json = null;
                    var json_null_fields: std.ArrayList([]const u8) = .empty;
                    if (assignments != null) {
                        var copy: Json = .{ .object = .empty };
                        if (table_def.storage_mode == .document and !root_assignment) {
                            const original = row.document orelse return error.InvalidSqlBackendResponse;
                            if (original != .object or (row.expected_content_digest == null and row.version != 0)) return error.InvalidSqlBackendResponse;
                            var members = original.object.iterator();
                            while (members.next()) |member| {
                                const declared = table_def.column(member.key_ptr.*) catch null;
                                // Replacing an alias changes its compiled physical
                                // path, never an unrelated raw member with the
                                // same spelling as the SQL column.
                                if (replaced.contains(member.key_ptr.*)) {
                                    if (declared == null or declared.?.document_path.len == 0) continue;
                                }
                                if (declared) |column| if (column.generated) continue;
                                try copy.object.put(self.arena, try self.arena.dupe(u8, member.key_ptr.*), try clone(self.arena, member.value_ptr.*));
                                if (declared) |column| if (column.type == .json and member.value_ptr.* == .null) try json_null_fields.append(self.arena, column.path);
                            }
                        }
                        for (table_def.columns) |column| {
                            if (table_def.storage_mode == .document) continue;
                            if (column.generated or replaced.contains(column.path)) continue;
                            if (try row.hasField(column.path)) {
                                const cell = try row.cell(column.path);
                                if (column.type == .json and cell.value == .null and !cell.sql_null) try json_null_fields.append(self.arena, column.path);
                                try putField(self.arena, &copy.object, column.path, try self.storageDatumTyped(cell, column, column.element_type));
                            }
                        }
                        for (bound_assignments, 0..) |bound, assignment_index| {
                            var assigned_null = bound.sql_null;
                            const assigned_value = if (bound.program) |program| blk: {
                                const assigned = if (evaluated) |batch| batch.assignments[assignment_index].?[batch.positions[row_index].?] else try self.evaluate(scratch, program, expression_cells);
                                assigned_null = assigned.sql_null;
                                if (assigned.sql_null and !bound.column.nullable) return @import("errors.zig").notNull(self.backend.error_context, bound.column.name);
                                if (bound.column.type == .json and !assigned.sql_null and assigned.value == .null) try json_null_fields.append(self.arena, bound.column.path);
                                break :blk try self.storageDatumTyped(assigned, bound.column, program.output_type.element_type);
                            } else blk: {
                                if (bound.column.type == .json and !bound.sql_null and bound.value == .null) try json_null_fields.append(self.arena, bound.column.path);
                                break :blk bound.value;
                            };
                            if (table_def.isDocumentRoot(bound.column.name)) {
                                if (assigned_value != .object) return error.SqlTypeMismatch;
                                copy = assigned_value;
                                try json_null_fields.appendSlice(self.arena, try table_def.documentRootNulls(self.arena, assigned_value));
                            } else try bound.column.assignStorageValue(self.arena, &copy.object, assigned_value, assigned_null);
                        }
                        document = copy;
                    }
                    retained = std.math.add(usize, retained, row.id.len + if (document) |doc| jsonSize(doc) else 0) catch return error.SqlProgramLimitExceeded;
                    if (retained > self.limits.retained_bytes) return error.SqlProgramLimitExceeded;
                    const key = try self.arena.dupe(u8, row.id);
                    if (table_def.storage_mode == .document and row.expected_content_digest == null and row.version != 0) return error.InvalidSqlBackendResponse;
                    const previous = if (assignments == null and returning != null) blk: {
                        const owned = try self.arena.create(catalog.Row);
                        if (row.typed_cells) |cells| if (retained_layout == null) {
                            retained_layout = try cells.layout.clone(self.arena);
                        };
                        owned.* = try row.cloneWithLayout(self.arena, retained_layout);
                        break :blk owned;
                    } else null;
                    try mutations.append(self.arena, .{ .key = key, .expected_version = row.version, .expected_content_digest = row.expected_content_digest, .row = document, .json_null_fields = json_null_fields.items, .previous = previous });
                }
                first = end;
            }
            const next = page.after orelse break;
            if (!scan_state.retained(self)) return error.SqlStatementSnapshotRequired;
            if (after) |previous| if (std.mem.eql(u8, previous, next)) return error.InvalidSqlBackendResponse;
            const owned_next = try self.alloc.dupe(u8, next);
            if (after) |previous| self.alloc.free(previous);
            after = owned_next;
        }
        try self.checkpoint();
        // Prepared rows own their values and version fences. Do not retain a
        // read snapshot while waiting for writer/commit admission.
        scan_state.deinit();
        return self.commitMutations(table_def, mutations.items, if (assignments != null) "UPDATE" else "DELETE", returning);
    }

    pub fn commitMutations(self: Context, table: catalog.Table, all: []const catalog.Mutation, tag: []const u8, returning: ?[]const ast.Projection) !Output {
        return self.commitMutationsInner(table, all, tag, returning, false);
    }

    /// Call only after native preparation has produced all postimages and the
    /// caller has built any source-aware RETURNING result before admission.
    pub fn commitPreparedMutations(self: Context, table: catalog.Table, all: []const catalog.Mutation, tag: []const u8) !Output {
        return self.commitMutationsInner(table, all, tag, null, true);
    }

    fn commitMutationsInner(self: Context, table: catalog.Table, all: []const catalog.Mutation, tag: []const u8, returning: ?[]const ast.Projection, already_prepared: bool) !Output {
        var fences: std.ArrayList(catalog.Mutation) = .empty;
        for (all) |mutation| if (mutation.predicate_only) {
            try fences.append(self.arena, mutation);
        };
        const input: []const catalog.Mutation = if (fences.items.len == 0) all else blk: {
            const effective = try self.arena.alloc(catalog.Mutation, all.len - fences.items.len);
            var index: usize = 0;
            for (all) |mutation| if (!mutation.predicate_only) {
                effective[index] = mutation;
                index += 1;
            };
            break :blk effective;
        };
        var output: Output = .{ .command_tag = tag, .rows_affected = input.len };
        var prepared = input;
        var did_prepare = already_prepared;
        if (!already_prepared and table.storage_mode == .document and input.len != 0) {
            const prepare = self.backend.vtable.prepare_mutations orelse return error.UnsupportedSqlExecution;
            prepared = try prepare(self.backend.ptr, self.arena, table, input);
            did_prepare = true;
            if (prepared.len != input.len) return error.InvalidSqlBackendResponse;
        }
        if (returning != null) {
            const projections = self.binding.returning_projections orelse return error.InvalidSqlBackendResponse;
            if (input.len > self.limits.result_rows) return error.SqlResultTooLarge;
            const binding = self.binding.returning orelse return error.InvalidSqlBackendResponse;
            if (!did_prepare and input.len != 0 and table.storage_mode != .document and !std.mem.eql(u8, tag, "DELETE")) {
                const prepare = self.backend.vtable.prepare_mutations orelse return error.UnsupportedSqlExecution;
                prepared = try prepare(self.backend.ptr, self.arena, table, input);
                did_prepare = true;
                if (prepared.len != input.len) return error.InvalidSqlBackendResponse;
            }
            var context = self;
            context.binding = binding.*;
            if (self.binding.returning_query) |query| {
                const images_adapter = try ReturningImages.initAtVersion(self.arena, table, null, self.backend.write_version);
                const images = try self.arena.alloc(catalog.Row, prepared.len);
                for (prepared, input, images) |mutation, original, *image| image.* = try images_adapter.row(self.arena, mutation, original);
                context.returning_rows = images;
                context.sink = null;
                const projected = if (images.len == 0) Output{ .columns = binding.columns, .rows = &.{}, .sql_nulls = &.{}, .command_tag = "SELECT" } else try context.selectOutput(query);
                if (projected.rowCount() != prepared.len) return error.InvalidSqlBackendResponse;
                output.columns = projected.columns;
                output.rows = projected.rows;
                output.native_rows = projected.native_rows;
                output.sql_nulls = projected.sql_nulls;
            } else {
                var fields: std.ArrayList([]const u8) = .empty;
                if (projections.len == 0) {
                    for (table.columns) |column| try fields.append(self.arena, column.path);
                } else for (projections) |projection| try fields.append(self.arena, if (projection.expression != null) "" else (try table.column(projection.field)).path);
                // Bind just the requested cells and expression inputs. A wide
                // untouched array must not be decoded merely to RETURN a key
                // or scalar counter. Relational RETURNING queries above retain
                // the complete row for their independently bound scan demands.
                var image_fields: std.ArrayList([]const u8) = .empty;
                for (fields.items) |field| if (field.len != 0) try image_fields.append(self.arena, field);
                for (binding.scalars.required) |ordinal| try image_fields.append(self.arena, binding.scalars.columns[ordinal].name);
                const images_adapter = try ReturningImages.initAtVersion(self.arena, table, image_fields.items, self.backend.write_version);
                const rows = try self.arena.alloc([]const Json, prepared.len);
                const flags = try self.arena.alloc([]const bool, prepared.len);
                const native_rows = if (self.limits.native_result) try self.arena.alloc([]const Datum, prepared.len) else null;
                var external = false;
                for (binding.scalars.projections) |optional| if (optional) |*program| {
                    external = external or @import("decision_eval.zig").hasExternal(program);
                };
                if (external) {
                    try context.decisionMutationReturning(images_adapter, fields.items, prepared, input, rows, flags, native_rows);
                } else for (prepared, input, rows, flags, 0..) |mutation, original, *cells, *nulls, row_index| {
                    try self.checkpoint();
                    const row = try images_adapter.row(self.arena, mutation, original);
                    const expressions = try binding.scalars.cells(self.arena, row);
                    const projected = try context.projectValues(self.arena, row, fields.items, expressions);
                    const result_row = try self.resultRow(projected, binding.columns);
                    cells.* = result_row.json;
                    nulls.* = result_row.nulls;
                    if (native_rows) |native_output| native_output[row_index] = result_row.native.?;
                }
                output.columns = binding.columns;
                output.rows = if (native_rows == null) rows else &.{};
                output.native_rows = native_rows;
                output.sql_nulls = if (native_rows == null) flags else null;
            }
        }
        // Projection, quotas and normalization can fail only BEFORE commit.
        // No post-commit lookup or allocation can replace the known outcome.
        for (input, prepared) |original, mutation| {
            if (!std.mem.eql(u8, mutation.key, original.key) or mutation.expected_version != original.expected_version or
                !std.meta.eql(mutation.expected_content_digest, original.expected_content_digest) or
                mutation.unique_absence != original.unique_absence or mutation.predicate_only != original.predicate_only or (mutation.row == null) != (original.row == null)) return error.InvalidSqlBackendResponse;
            if (mutation.conflict_guard != original.conflict_guard) return error.InvalidSqlBackendResponse;
        }
        try self.checkpoint();
        // No iterator may borrow the captured cut past publication. Native
        // proof/version fences have already joined the atomic write read-set.
        if (self.statement_capture) |capture| capture.release();
        const committed_base = if (fences.items.len == 0) prepared else blk: {
            try fences.appendSlice(self.arena, prepared);
            break :blk fences.items;
        };
        // Provenance belongs to each effective mutation, not the enclosing
        // batch: ON CONFLICT and MERGE can mix inserts, updates and deletes.
        const committed = try self.arena.dupe(catalog.Mutation, committed_base);
        for (committed) |*mutation| {
            if (mutation.predicate_only) continue;
            const select_required = if (mutation.unique_absence) returning != null or self.binding.returning != null else table.sql_select_required;
            mutation.row_policy_requirement = if (mutation.row == null)
                (if (select_required) .delete_select else .delete)
            else if (mutation.unique_absence)
                (if (select_required) .insert_select else .insert)
            else
                (if (select_required) .update_select else .update);
        }
        // Commit-only wire buffers have their own lifetime. Growing them in
        // the result arena amplifies retained preimages and cannot reclaim
        // serialization capacity before the statement result is released.
        var commit_arena = std.heap.ArenaAllocator.init(self.alloc);
        defer commit_arena.deinit();
        output.mutation_outcome = if (committed.len == 0) .committed else if (did_prepare)
            try (self.backend.vtable.mutate_prepared orelse return error.UnsupportedSqlExecution)(self.backend.ptr, commit_arena.allocator(), self.alloc, table, committed)
        else
            try self.backend.vtable.mutate(self.backend.ptr, commit_arena.allocator(), self.alloc, table, committed);
        return output;
    }

    pub const ReturningImages = struct {
        table: catalog.Table,
        projection: @import("document_row.zig").Projection,
        layout: ?*const catalog.Row.TypedLayout,
        write_version: ?u64 = null,

        pub fn init(a: std.mem.Allocator, table: catalog.Table, fields: ?[]const []const u8) !ReturningImages {
            return initAtVersion(a, table, fields, null);
        }

        pub fn initAtVersion(a: std.mem.Allocator, table: catalog.Table, fields: ?[]const []const u8, write_version: ?u64) !ReturningImages {
            const names = fields orelse blk: {
                const all = try a.alloc([]const u8, table.columns.len);
                for (table.columns, all) |column, *name| name.* = column.name;
                break :blk all;
            };
            const projection = try @import("document_row.zig").Projection.init(a, table, names);
            return .{ .table = table, .projection = projection, .layout = try projection.pageLayout(a), .write_version = write_version };
        }

        pub fn row(self: ReturningImages, a: std.mem.Allocator, mutation: catalog.Mutation, original: catalog.Mutation) !catalog.Row {
            if (!std.mem.eql(u8, mutation.key, original.key) or mutation.expected_version != original.expected_version or
                !std.meta.eql(mutation.expected_content_digest, original.expected_content_digest) or mutation.unique_absence != original.unique_absence or
                mutation.conflict_guard != original.conflict_guard or mutation.predicate_only != original.predicate_only or
                (mutation.row == null) != (original.row == null)) return error.InvalidSqlBackendResponse;
            const datum = mutation.row orelse return (original.previous orelse return error.InvalidSqlBackendResponse).*;
            if (self.write_version == null) for (self.projection.columns) |column| {
                if (self.table.isDocumentVersion(column.name)) return error.UnsupportedSqlExecution;
            };
            if (datum != .object) return error.InvalidSqlBackendResponse;
            const nulls = try a.alloc(bool, datum.object.count());
            for (datum.object.values(), nulls) |cell, *flag| flag.* = cell == .null;
            for (mutation.json_null_fields) |name| {
                const index = datum.object.getIndex(name) orelse return error.InvalidSqlBackendResponse;
                if (!nulls[index] or (try self.table.column(name)).type != .json) return error.InvalidSqlBackendResponse;
                nulls[index] = false;
            }
            return self.projection.adaptBorrowed(a, self.layout, .{ .id = mutation.key, .version = try self.table.preparedVersion(datum, self.write_version orelse mutation.expected_version), .value = datum, .sql_nulls = nulls, .document = if (self.table.storage_mode == .document) datum else null });
        }
    };

    /// Provider scratch must not accumulate in the retained mutation/output
    /// arena. Resolve RETURNING pages before publishing any native mutation.
    fn decisionMutationReturning(self: Context, images: ReturningImages, fields: []const []const u8, prepared: []const catalog.Mutation, input: []const catalog.Mutation, rows: [][]const Json, flags: [][]const bool, native_rows: ?[][]const Datum) !void {
        var first: usize = 0;
        while (first < prepared.len) {
            var arena = std.heap.ArenaAllocator.init(self.alloc);
            defer arena.deinit();
            const scratch = arena.allocator();
            var page: std.ArrayList(catalog.Row) = .empty;
            var cells: std.ArrayList([]const Datum) = .empty;
            var bytes: usize = 0;
            while (first + page.items.len < prepared.len and page.items.len < self.limits.page_rows) {
                try self.checkpoint();
                const index = first + page.items.len;
                const row = try images.row(scratch, prepared[index], input[index]);
                const values = try self.binding.scalars.cells(scratch, row);
                try page.append(scratch, row);
                try cells.append(scratch, values);
                for (values) |datum| bytes +|= try operators.datumBytes(datum);
                if (bytes >= self.limits.page_bytes) break;
            }
            const projected = try self.projectValuesBatch(scratch, page.items, fields, cells.items);
            for (projected, first..) |values, index| {
                const row = try self.resultRow(values, self.binding.columns);
                rows[index] = row.json;
                flags[index] = row.nulls;
                if (native_rows) |native_output| native_output[index] = row.native.?;
            }
            first += page.items.len;
        }
    }
};

fn coerce(alloc: std.mem.Allocator, raw: Json, kind: ast.ColumnType) !Json {
    if (kind == .uuid and raw != .null) {
        if (raw != .string) return error.SqlTypeMismatch;
        return .{ .string = @import("../common/uuid.zig").canonicalAlloc(alloc, raw.string) catch |err| switch (err) {
            error.InvalidUuid => return error.SqlTypeMismatch,
            else => return err,
        } };
    }
    return describe.coerceAlloc(alloc, raw, kind);
}

fn contains(items: []const []const u8, needle: []const u8) bool {
    for (items) |item| if (std.mem.eql(u8, item, needle)) return true;
    return false;
}

fn fieldValue(root: Json, path: []const u8) Json {
    if (root != .object) return .null;
    // Relational column names are literal properties. A quoted "a.b" must
    // never be reinterpreted as a document-path expression.
    return root.object.get(path) orelse .null;
}

fn putField(arena: std.mem.Allocator, object: *std.json.ObjectMap, path: []const u8, value: Json) !void {
    try object.put(arena, path, value);
}

/// The single owned SQL-to-storage boundary, shared by ordinary mutations and
/// independently prepared MERGE images. Never encode a typed array's JSON
/// placeholder or retain payloads borrowed from an evaluator's scratch arena.
pub fn encodeStorageDatum(arena: std.mem.Allocator, datum: Datum, column: catalog.Column, retained_bytes: usize) !Json {
    return encodeStorageDatumTyped(arena, datum, column, null, retained_bytes);
}

pub fn encodeStorageDatumTyped(arena: std.mem.Allocator, datum: Datum, column: catalog.Column, source_identity: ?@import("array_value.zig").ElementType, retained_bytes: usize) !Json {
    if (datum.range()) |range| {
        try datum.validateTemporal();
        if (@import("range_value.zig").kindFromType(column.type) != range.kind) return error.SqlTypeMismatch;
        var context: @import("range_value.zig").Context = .{ .alloc = arena, .max_output_bytes = retained_bytes };
        return .{ .string = try @import("range_value.zig").formatLeaky(&context, range.*) };
    }
    if (datum.temporal() != null and column.type != .string) {
        try datum.validateTemporal();
        if (column.type == .datetime and column.element_type == null and
            (column.temporal_kind orelse .timestamp_tz) == .timestamp_tz and datum.temporal().?.kind() == .timestamp_tz)
        {
            const nanos = datum.temporal().?.timestamp_tz.unixNanoseconds() orelse return error.UnsupportedRelationalValueType;
            const physical = std.math.cast(u64, nanos) orelse return error.UnsupportedRelationalValueType;
            var buffer: [20]u8 = undefined;
            const bytes = try std.fmt.bufPrint(&buffer, "{d}", .{physical});
            if (bytes.len > retained_bytes) return error.SqlProgramLimitExceeded;
            return .{ .number_string = try arena.dupe(u8, bytes) };
        }
        return error.UnsupportedRelationalValueType; // other temporal types need schema/layout activation
    }
    const checked = try describe.assignDatum(arena, datum, source_identity, column.type, column.element_type, .{ .output_bytes = retained_bytes });
    if (checked.sql_null) {
        if (!column.nullable) return error.SqlNotNullViolation;
        return .null;
    }
    if (checked.array) |array| return @import("array_wire.zig").toJsonLeaky(arena, array.*, .{ .values = .{ .bytes = retained_bytes }, .wire_bytes = retained_bytes });
    if (checked.numeric) |value| {
        var context: @import("numeric_value.zig").Context = .{ .alloc = arena, .max_output_bytes = retained_bytes };
        return .{ .string = try @import("numeric_value.zig").format(&context, value.*) };
    }
    // Scalar floating storage is currently finite-only. Never let JSON
    // serialization turn a valid SQL special value into a persisted SQL NULL.
    if (checked.value == .float and !std.math.isFinite(checked.value.float)) return error.SqlNonFiniteScalarStorage;
    return clone(arena, checked.value);
}

test "SQL scalar floating storage rejects specials without serializing nullable NULLs" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    for ([_]@import("array_value.zig").ElementType{ .float32, .float64 }) |kind| {
        const column: catalog.Column = .{ .name = "n", .path = "n", .type = .number, .element_type = kind, .nullable = true };
        for ([_]f64{ std.math.nan(f64), std.math.inf(f64), -std.math.inf(f64) }) |number| try std.testing.expectError(error.SqlNonFiniteScalarStorage, encodeStorageDatumTyped(scratch, Datum.json(.{ .float = number }), column, kind, 1 << 20));
        for ([_][]const u8{ "NaN", "Infinity", "-Infinity" }) |text| {
            var context: @import("numeric_value.zig").Context = .{ .alloc = scratch };
            var number = try @import("numeric_value.zig").parse(&context, text);
            defer number.deinit();
            try std.testing.expectError(error.SqlNonFiniteScalarStorage, encodeStorageDatumTyped(scratch, Datum.typedNumeric(&number.value), column, .numeric, 1 << 20));
            // NUMERIC has a canonical special-value representation already;
            // the finite-only floating barrier must not erase that capability.
            const exact = try encodeStorageDatumTyped(scratch, Datum.typedNumeric(&number.value), .{ .name = "n", .path = "n", .type = .number, .element_type = .numeric }, .numeric, 1 << 20);
            try std.testing.expectEqualStrings(text, exact.string);
        }
    }
}

pub fn clone(arena: std.mem.Allocator, value: Json) error{ OutOfMemory, SqlProgramLimitExceeded }!Json {
    return cloneDepth(arena, value, 0);
}

fn cloneDepth(arena: std.mem.Allocator, value: Json, depth: usize) error{ OutOfMemory, SqlProgramLimitExceeded }!Json {
    if (depth > 64) return error.SqlProgramLimitExceeded;
    return switch (value) {
        .string => |text| .{ .string = try arena.dupe(u8, text) },
        .number_string => |text| .{ .number_string = try arena.dupe(u8, text) },
        .object => |object| blk: {
            var result: std.json.ObjectMap = .empty;
            for (object.keys(), object.values()) |key, item| try result.put(arena, try arena.dupe(u8, key), try cloneDepth(arena, item, depth + 1));
            break :blk .{ .object = result };
        },
        .array => |array| blk: {
            var result: std.array_list.Managed(Json) = .init(arena);
            try result.ensureTotalCapacity(array.items.len);
            for (array.items) |item| result.appendAssumeCapacity(try cloneDepth(arena, item, depth + 1));
            break :blk .{ .array = result };
        },
        else => value,
    };
}

fn jsonSize(value: Json) usize {
    return switch (value) {
        .string, .number_string => |text| @sizeOf(Json) +| text.len,
        .object => |object| blk: {
            var bytes: usize = @sizeOf(Json);
            for (object.keys(), object.values()) |key, item| bytes +|= key.len +| jsonSize(item);
            break :blk bytes;
        },
        .array => |array| blk: {
            var bytes: usize = @sizeOf(Json);
            for (array.items) |item| bytes +|= jsonSize(item);
            break :blk bytes;
        },
        else => @sizeOf(Json),
    };
}

test "SQL RETURNING write versions require the durable stamp and preserve delete preimages" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const table: catalog.Table = .{ .id = 1, .physical_name = "docs", .schema_version = 1, .storage_mode = .document, .columns = &.{} };
    const mutation: catalog.Mutation = .{ .key = "doc:a", .expected_version = 42, .row = .{ .object = .empty } };
    const unstamped = try Context.ReturningImages.init(a, table, &.{"_version"});
    try std.testing.expectError(error.UnsupportedSqlExecution, unstamped.row(a, mutation, mutation));
    const stamp: u64 = 9007199254740994;
    const stamped = try Context.ReturningImages.initAtVersion(a, table, &.{"_version"}, stamp);
    const image = try stamped.row(a, mutation, mutation);
    try std.testing.expectEqual(stamp, image.version);
    try std.testing.expectEqual(@as(i64, stamp), (try image.cell("_version")).value.integer);
    const previous = try a.create(catalog.Row);
    previous.* = .{ .id = "doc:a", .version = 42, .value = .{ .object = .empty }, .document_envelope = true };
    const deletion: catalog.Mutation = .{ .key = "doc:a", .expected_version = 42, .row = null, .previous = previous };
    const deleted = try stamped.row(a, deletion, deletion);
    try std.testing.expectEqual(@as(i64, 42), (try deleted.cell("_version")).value.integer);
    var custom = table;
    custom.version_field = "expires";
    var root: std.json.ObjectMap = .empty;
    var with_ttl = mutation;
    with_ttl.row = .{ .object = root };
    const custom_images = try Context.ReturningImages.initAtVersion(a, custom, &.{"_version"}, stamp);
    try std.testing.expectEqual(stamp, (try custom_images.row(a, with_ttl, with_ttl)).version);
    for ([_]Json{ .null, .{ .integer = 0 }, .{ .number_string = "9007199254740993" }, .{ .string = "2090-01-01T00:00:00Z" } }, [_]u64{ stamp, 0, 9007199254740993, 3786912000000000000 }) |value, wanted| {
        try root.put(a, "expires", value);
        with_ttl.row = .{ .object = root };
        const result = try custom_images.row(a, with_ttl, with_ttl);
        try std.testing.expectEqual(wanted, result.version);
    }
}

test "SQL RETURNING write stamp demand follows typed expressions without treating strings as columns" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "INSERT INTO docs(_id) VALUES('a') RETURNING _version",
        "INSERT INTO docs(_id) VALUES('a') RETURNING docs._version AS v",
        "UPDATE docs SET n=1 RETURNING coalesce(_version,0)+1",
        "UPDATE docs SET n=1 RETURNING CASE WHEN _version > 0 THEN _version ELSE 0 END",
        "UPDATE docs SET n=1 RETURNING _version IN (1,2)",
    }) |sql| {
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        try std.testing.expect(ast.returningWriteVersion(compiled.statement));
    }
    for ([_][]const u8{
        "INSERT INTO docs(_id) VALUES('a') RETURNING _id",
        "UPDATE docs SET n=1 RETURNING '_version' AS text",
        "DELETE FROM docs RETURNING _version",
        "SELECT _version FROM docs",
    }) |sql| {
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        try std.testing.expect(!ast.returningWriteVersion(compiled.statement));
    }
}

fn numericBoundaryScenario(backing: std.mem.Allocator) !void {
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    var fixture: TestBackend = .{};
    const context: Context = .{ .alloc = backing, .arena = a, .backend = fixture.iface(), .binding = undefined, .parameters = &.{}, .limits = .{} };
    var exact_context: @import("numeric_value.zig").Context = .{ .alloc = backing };
    const output, const stored = blk: {
        var source = try @import("numeric_value.zig").parse(&exact_context, "9007199254740993.1200");
        defer source.deinit();
        break :blk .{
            try context.outputDatum(Datum.typedNumeric(&source.value), .number, .numeric),
            try encodeStorageDatum(a, Datum.typedNumeric(&source.value), .{ .name = "n", .path = "n", .type = .number, .element_type = .numeric }, 1 << 20),
        };
    };
    try std.testing.expectEqualStrings("9007199254740993.1200", output.string);
    try std.testing.expectEqualStrings(output.string, stored.string);
    const table: catalog.Table = .{ .id = 1, .physical_name = "rows", .schema_version = 1, .columns = &.{
        .{ .name = "n", .path = "n", .type = .number, .element_type = .numeric },
    } };
    var object: std.json.ObjectMap = .empty;
    try object.put(a, "n", stored);
    const mutation: catalog.Mutation = .{ .key = "row", .expected_version = 7, .row = .{ .object = object } };
    const adapter = try Context.ReturningImages.init(a, table, null);
    try std.testing.expect(adapter.projection.has_typed_cells);
    const image = try adapter.row(a, mutation, mutation);
    const numeric_cell = try image.cell("n");
    try std.testing.expect(numeric_cell.numeric != null and !numeric_cell.sql_null);
    try std.testing.expectEqualStrings(output.string, (try context.outputDatum(numeric_cell, .number, .numeric)).string);
    for ([_]Json{ .{ .string = "9007199254740993.1200" }, .{ .number_string = "9007199254740993.1200" } }) |token| {
        try std.testing.expectEqualStrings(output.string, (try context.outputDatum(Datum.json(token), .number, .numeric)).string);
    }
    try std.testing.expectEqualStrings("9223372036854775807", (try context.outputDatum(Datum.json(.{ .integer = std.math.maxInt(i64) }), .number, .numeric)).string);
    try std.testing.expect((try context.outputDatum(.{}, .number, .numeric)) == .null);
    try std.testing.expectError(error.SqlTypeMismatch, context.outputDatum(Datum.json(.{ .float = 1.25 }), .number, .numeric));
    var limited = context;
    limited.limits.retained_bytes = 3;
    try std.testing.expectError(error.SqlProgramLimitExceeded, limited.outputDatum(Datum.json(.{ .string = "12345" }), .number, .numeric));
}

test "SQL NUMERIC executes exact scalar projections through public result metadata" {
    const a = std.testing.allocator;
    var fixture: TestBackend = .{};
    var compiled = try compiler.compile(a, "SELECT '9007199254740993.1200'::numeric + 1 AS exact, NULL::numeric AS absent, 'Infinity'::numeric AS special", .{});
    defer compiled.deinit();
    var result = try execute(a, fixture.iface(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
    try std.testing.expectEqualStrings("9007199254740994.1200", result.output.rows[0][0].string);
    try std.testing.expect(result.output.rows[0][1] == .null);
    try std.testing.expectEqualStrings("Infinity", result.output.rows[0][2].string);
    for (result.output.columns) |column| {
        try std.testing.expectEqual(ast.ColumnType.number, column.type);
        try std.testing.expectEqual(@import("array_value.zig").ElementType.numeric, column.element_type.?);
    }
}

test "SQL expression access shares materialized and streaming hints while preserving residual row goals" {
    const Fixture = struct {
        table: catalog.Table,
        hinted: bool,
        scans: usize = 0,
        offset: usize = 0,
        request: catalog.Scan = undefined,
        fn resolve(raw: *anyopaque, _: std.mem.Allocator, _: ast.Name, _: catalog.Action) !catalog.Table {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.table;
        }
        fn scan(raw: *anyopaque, a: std.mem.Allocator, _: catalog.Table, request: catalog.Scan) !catalog.Page {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.scans += 1;
            try std.testing.expectEqual(@as(usize, @intFromBool(self.hinted)), request.expression_equalities.len);
            try std.testing.expect(request.row_goal == null);
            if (self.hinted) try std.testing.expectEqualStrings("owner", request.expression_equalities[0].value.string);
            const rows = try a.alloc(catalog.Row, 2);
            // Simulate the legitimate unavailable-index fallback. SQL must
            // reject its first row before applying LIMIT, not trust the hint.
            for (rows, [_][]const u8{ "Other", "Owner" }) |*row, label| {
                var object: std.json.ObjectMap = .empty;
                try object.put(a, "email", .{ .string = label });
                row.* = .{ .id = label, .version = 1, .value = .{ .object = object } };
            }
            return .{ .rows = rows };
        }
        fn open(raw: *anyopaque, _: std.mem.Allocator, _: catalog.Table, request: catalog.Scan) !?catalog.Cursor {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.request = request;
            self.offset = 0;
            return .{ .ptr = raw, .next = next, .close = close };
        }
        fn next(raw: *anyopaque, a: std.mem.Allocator, limit: u32) !catalog.Page {
            const self: *@This() = @ptrCast(@alignCast(raw));
            var page = try scan(raw, a, self.table, self.request);
            const end = @min(page.rows.len, self.offset + limit);
            const total = page.rows.len;
            page.rows = page.rows[self.offset..end];
            self.offset = end;
            page.after = if (end < total) "Other" else null;
            return page;
        }
        fn close(_: *anyopaque) void {}
        fn mutate(_: *anyopaque, _: std.mem.Allocator, _: std.mem.Allocator, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
            return error.UnexpectedMutation;
        }
        fn checkpoint(_: *anyopaque) !void {}
    };
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const owned = arena.allocator();
    var table: catalog.Table = .{ .id = 1, .physical_name = "items", .schema_version = 1, .columns = &.{.{ .name = "email", .path = "email", .type = .string, .element_type = .text }} };
    var key: catalog.IndexDefinition.Key = .{ .column = "", .expression = try std.json.parseFromSliceLeaky(Json, owned,
        \\{"op":"lower_ascii","args":[{"op":"column","column":"email"}]}
    , .{}), .result_type = .string, .collation = null, .direction = .asc, .nulls = .default };
    key.identity = try key.bindIdentity(owned, try table.expressionSchemaLeaky(owned));
    const keys = [_]catalog.IndexDefinition.Key{key};
    table.index_definitions = &.{.{ .name = "email_lower", .keys = &keys, .include_columns = &.{}, .where = &.{} }};
    var compiled = try compiler.compile(a, "SELECT email FROM items WHERE lower(email)=$1 LIMIT 1", .{});
    defer compiled.deinit();
    for ([_]bool{ false, true }) |enabled| {
        var fixture: Fixture = .{ .table = table, .hinted = enabled };
        const backend: catalog.Backend = .{ .ptr = &fixture, .supports_expression_access_hints = enabled, .vtable = &.{ .resolve = Fixture.resolve, .scan = Fixture.scan, .open_scan = Fixture.open, .mutate = Fixture.mutate, .checkpoint = Fixture.checkpoint } };
        var result = try execute(a, backend, &compiled, &.{.{ .string = "owner" }}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
        try std.testing.expectEqualStrings("Owner", result.output.rows[0][0].string);
        const stream = (try @import("read_stream.zig").Stream.open(a, backend, &compiled, &.{.{ .string = "owner" }}, .{})).?;
        defer stream.close();
        var matched_page = try stream.next(32);
        defer matched_page.deinit();
        try std.testing.expectEqual(@as(usize, 1), matched_page.output.rows.len);
        try std.testing.expectEqualStrings("Owner", matched_page.output.rows[0][0].string);
        try std.testing.expectEqual(@as(usize, 3), fixture.scans);
    }
}

test "SQL NUMERIC predicates preserve inferred and explicit parameters for reads and mutations" {
    const Fixture = struct {
        mutations: usize = 0,
        scan_request: catalog.Scan = undefined,
        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: ast.Name, _: catalog.Action) !catalog.Table {
            return .{ .id = 1, .physical_name = "docs", .schema_version = 1, .columns = &.{.{ .name = "n", .path = "n", .type = .number, .element_type = .numeric }} };
        }
        fn scan(_: *anyopaque, a: std.mem.Allocator, _: catalog.Table, request: catalog.Scan) !catalog.Page {
            try std.testing.expectEqual(@as(usize, 0), request.conditions.len);
            const rows = try a.alloc(catalog.Row, 2);
            for (rows, [_][]const u8{ "9007199254740993.1200", "9007199254740993.1201" }, [_][]const u8{ "match", "other" }) |*row, text, key| {
                var object: std.json.ObjectMap = .empty;
                try object.put(a, "n", .{ .string = text });
                row.* = .{ .id = key, .version = 7, .value = .{ .object = object } };
            }
            return .{ .rows = rows };
        }
        fn mutate(raw: *anyopaque, _: std.mem.Allocator, _: std.mem.Allocator, _: catalog.Table, mutations: []const catalog.Mutation) !catalog.MutationOutcome {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(@as(usize, 1), mutations.len);
            try std.testing.expectEqualStrings("match", mutations[0].key);
            self.mutations += mutations.len;
            return .committed;
        }
        fn prepare(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, mutations: []const catalog.Mutation) ![]const catalog.Mutation {
            return mutations;
        }
        fn open(raw: *anyopaque, _: std.mem.Allocator, _: catalog.Table, request: catalog.Scan) !?catalog.Cursor {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.scan_request = request;
            return .{ .ptr = raw, .next = next, .close = close };
        }
        fn next(raw: *anyopaque, a: std.mem.Allocator, _: u32) !catalog.Page {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return scan(raw, a, undefined, self.scan_request);
        }
        fn close(_: *anyopaque) void {}
        fn checkpoint(_: *anyopaque) !void {}
    };
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |explicit| for ([_][]const u8{
        "SELECT n FROM docs WHERE n = $1",
        "UPDATE docs SET n = n + 1 WHERE n = $1 RETURNING n",
        "DELETE FROM docs WHERE n = $1 RETURNING n",
    }, 0..) |sql, index| {
        var fixture: Fixture = .{};
        const backend: catalog.Backend = .{ .ptr = &fixture, .parameter_descriptor_hints = if (explicit) &.{.{ .kind = .number, .element_type = .numeric }} else &.{}, .vtable = &.{ .resolve = Fixture.resolve, .scan = Fixture.scan, .mutate = Fixture.mutate, .mutate_prepared = Fixture.mutate, .prepare_mutations = Fixture.prepare, .checkpoint = Fixture.checkpoint } };
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        var result = try execute(a, backend, &compiled, &.{.{ .string = "9007199254740993.1200" }}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
        try std.testing.expectEqualStrings(if (index == 1) "9007199254740994.1200" else "9007199254740993.1200", result.output.rows[0][0].string);
        if (index == 0) {
            var token_result = try execute(a, backend, &compiled, &.{.{ .number_string = "9007199254740993.1200" }}, .{});
            defer token_result.deinit();
            try std.testing.expectEqual(@as(usize, 1), token_result.output.rows.len);
            try std.testing.expectEqualStrings("9007199254740993.1200", token_result.output.rows[0][0].string);
            for ([_][]const u8{
                "SELECT q.n FROM docs q WHERE q.n=$1",
                "SELECT q.n FROM (SELECT n FROM docs) q WHERE q.n=$1",
            }) |aliased_sql| {
                var aliased = try compiler.compile(a, aliased_sql, .{});
                defer aliased.deinit();
                var aliased_backend = backend;
                var aliased_callbacks = backend.vtable.*;
                aliased_callbacks.open_scan = Fixture.open;
                aliased_backend.vtable = &aliased_callbacks;
                var aliased_result = try execute(a, aliased_backend, &aliased, &.{.{ .number_string = "9007199254740993.1200" }}, .{});
                defer aliased_result.deinit();
                try std.testing.expectEqual(@as(usize, 1), aliased_result.output.rows.len);
                try std.testing.expectEqualStrings("9007199254740993.1200", aliased_result.output.rows[0][0].string);
                aliased_backend.parameter_descriptor_hints = &.{.{ .kind = .number, .element_type = .float64 }};
                var aliased_floating = try execute(a, aliased_backend, &aliased, &.{.{ .float = 9007199254740994 }}, .{});
                defer aliased_floating.deinit();
                try std.testing.expectEqual(@as(usize, 2), aliased_floating.output.rows.len);
            }
        }
        try std.testing.expectEqual(@as(usize, @intFromBool(index != 0)), fixture.mutations);
        if (index == 0) {
            var floating_backend = backend;
            floating_backend.parameter_descriptor_hints = &.{.{ .kind = .number, .element_type = .float64 }};
            var floating_result = try execute(a, floating_backend, &compiled, &.{.{ .float = 9007199254740994 }}, .{});
            defer floating_result.deinit();
            try std.testing.expectEqual(@as(usize, 2), floating_result.output.rows.len);
            var stream_backend = backend;
            var stream_vtable = backend.vtable.*;
            stream_vtable.open_scan = Fixture.open;
            stream_backend.vtable = &stream_vtable;
            const stream = (try @import("read_stream.zig").Stream.open(a, stream_backend, &compiled, &.{.{ .string = "9007199254740993.1200" }}, .{})).?;
            defer stream.close();
            var page = try stream.next(32);
            defer page.deinit();
            try std.testing.expect(page.exhausted);
            try std.testing.expectEqual(@as(usize, 1), page.output.rows.len);
            try std.testing.expectEqualStrings("9007199254740993.1200", page.output.rows[0][0].string);
        }
        var null_result = try execute(a, backend, &compiled, &.{.null}, .{});
        defer null_result.deinit();
        try std.testing.expectEqual(@as(usize, 0), null_result.output.rows.len);
        try std.testing.expectEqual(@as(usize, @intFromBool(index != 0)), fixture.mutations);
    };
}

test "SQL floating predicates retain declared NUMERIC parameters for reads and mutations" {
    const Fixture = struct {
        element: @import("array_value.zig").ElementType,
        sample: f64 = 1.25,
        mutations: usize = 0,
        fn resolve(raw: *anyopaque, a: std.mem.Allocator, _: ast.Name, _: catalog.Action) !catalog.Table {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return .{ .id = 1, .physical_name = "docs", .schema_version = 1, .columns = try a.dupe(catalog.Column, &.{.{ .name = "x", .path = "x", .type = .number, .element_type = self.element }}) };
        }
        fn scan(raw: *anyopaque, a: std.mem.Allocator, _: catalog.Table, request: catalog.Scan) !catalog.Page {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(@as(usize, 0), request.conditions.len);
            const rows = try a.alloc(catalog.Row, 2);
            for (rows, [_]f64{ self.sample, 2.5 }, [_][]const u8{ "match", "other" }) |*row, number, key| {
                var object: std.json.ObjectMap = .empty;
                try object.put(a, "x", .{ .float = number });
                row.* = .{ .id = key, .version = 7, .value = .{ .object = object } };
            }
            return .{ .rows = rows };
        }
        fn mutate(raw: *anyopaque, _: std.mem.Allocator, _: std.mem.Allocator, _: catalog.Table, mutations: []const catalog.Mutation) !catalog.MutationOutcome {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(@as(usize, 1), mutations.len);
            try std.testing.expectEqualStrings("match", mutations[0].key);
            self.mutations += mutations.len;
            return .committed;
        }
        fn prepare(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, mutations: []const catalog.Mutation) ![]const catalog.Mutation {
            return mutations;
        }
        fn open(raw: *anyopaque, _: std.mem.Allocator, _: catalog.Table, request: catalog.Scan) !?catalog.Cursor {
            try std.testing.expectEqual(@as(usize, 0), request.conditions.len);
            return .{ .ptr = raw, .next = next, .close = close };
        }
        fn next(raw: *anyopaque, a: std.mem.Allocator, _: u32) !catalog.Page {
            return scan(raw, a, undefined, .{ .fields = &.{"x"}, .limit = 32 });
        }
        fn close(_: *anyopaque) void {}
        fn checkpoint(_: *anyopaque) !void {}
    };
    const a = std.testing.allocator;
    for ([_]@import("array_value.zig").ElementType{ .float32, .float64 }) |element| for ([_]bool{ false, true }) |rounded| for ([_][]const u8{
        "SELECT x FROM docs WHERE x = $1",
        "UPDATE docs SET x = x + 1 WHERE x = $1 RETURNING x",
        "DELETE FROM docs WHERE x = $1 RETURNING x",
    }, 0..) |sql, index| {
        var fixture: Fixture = .{ .element = element, .sample = if (rounded) (if (element == .float32) @as(f64, @as(f32, 1.1)) else 1.1) else 1.25 };
        const expected: usize = @intFromBool(!rounded or element == .float64);
        const backend: catalog.Backend = .{ .ptr = &fixture, .parameter_descriptor_hints = &.{.{ .kind = .number, .element_type = .numeric }}, .vtable = &.{ .resolve = Fixture.resolve, .scan = Fixture.scan, .checkpoint = Fixture.checkpoint, .mutate = Fixture.mutate, .mutate_prepared = Fixture.mutate, .prepare_mutations = Fixture.prepare } };
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        const parameters = [_]Json{.{ .string = if (rounded) "1.1" else "1.25000000000000000000" }};
        var result = try execute(a, backend, &compiled, &parameters, .{});
        defer result.deinit();
        try std.testing.expectEqual(expected, result.output.rows.len);
        try std.testing.expectEqual(expected * @intFromBool(index != 0), fixture.mutations);
        if (index == 0) {
            var stream_backend = backend;
            var vtable = backend.vtable.*;
            vtable.open_scan = Fixture.open;
            stream_backend.vtable = &vtable;
            const stream = (try @import("read_stream.zig").Stream.open(a, stream_backend, &compiled, &parameters, .{})).?;
            defer stream.close();
            var page = try stream.next(32);
            defer page.deinit();
            try std.testing.expect(page.exhausted);
            try std.testing.expectEqual(expected, page.output.rows.len);
        }
        var null_result = try execute(a, backend, &compiled, &.{.null}, .{});
        defer null_result.deinit();
        try std.testing.expectEqual(@as(usize, 0), null_result.output.rows.len);
        try std.testing.expectEqual(expected * @intFromBool(index != 0), fixture.mutations);
    };
}

test "SQL numeric selectors coerce arguments to their common domain" {
    const a = std.testing.allocator;
    for ([_]struct { sql: []const u8, json: []const u8 }{
        .{ .sql = "SELECT to_jsonb(coalesce(NULL::real,1.123456789::numeric))", .json = "1.1234568" },
        .{ .sql = "SELECT to_jsonb(coalesce(1.123456789::numeric,NULL::real))", .json = "1.1234568" },
        .{ .sql = "SELECT to_jsonb(greatest(1.1::real,1.1::numeric))", .json = "1.1" },
        .{ .sql = "SELECT to_jsonb(least(1.1::numeric,1.1::real))", .json = "1.1" },
        .{ .sql = "SELECT to_jsonb(greatest(NULL::real,1.123456789::numeric))", .json = "1.1234568" },
        .{ .sql = "SELECT to_jsonb(least(NULL::double precision,1.123456789::numeric))", .json = "1.123456789" },
        .{ .sql = "SELECT to_jsonb(coalesce(NULL::numeric,9007199254740993::bigint))", .json = "9007199254740993" },
        .{ .sql = "SELECT to_jsonb(greatest('NaN'::real,1::numeric))", .json = "\"NaN\"" },
        .{ .sql = "SELECT to_jsonb(least('NaN'::numeric,1::real))", .json = "1" },
        .{ .sql = "SELECT to_jsonb(greatest('-Infinity'::real,'Infinity'::numeric))", .json = "\"Infinity\"" },
        .{ .sql = "SELECT to_jsonb(least('-Infinity'::numeric,'Infinity'::real))", .json = "\"-Infinity\"" },
    }) |case| {
        var fixture: TestBackend = .{ .row_count = 0 };
        var compiled = try compiler.compile(a, case.sql, .{});
        defer compiled.deinit();
        var result = try execute(a, fixture.iface(), &compiled, &.{}, .{});
        defer result.deinit();
        const actual = try std.json.Stringify.valueAlloc(a, result.output.rows[0][0], .{});
        defer a.free(actual);
        try std.testing.expectEqualStrings(case.json, actual);
    }
    var fixture: TestBackend = .{ .row_count = 0 };
    var backend = fixture.iface();
    backend.parameter_descriptor_hints = &.{.{ .kind = .number, .element_type = .numeric }};
    var compiled = try compiler.compile(a, "SELECT coalesce(NULL::real,$1),greatest(1::real,$1),least(2::real,$1)", .{});
    defer compiled.deinit();
    var result = try execute(a, backend, &compiled, &.{.{ .string = "1.123456789" }}, .{});
    defer result.deinit();
    for (result.output.columns, result.output.rows[0]) |column, cell| {
        try std.testing.expectEqual(@as(?@import("array_value.zig").ElementType, .float32), column.element_type);
        try std.testing.expectEqual(@as(f64, @as(f32, 1.123456789)), cell.float);
    }
}

test "SQL floating numeric conversions respect lazy branch demand" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "SELECT CASE WHEN true THEN 1::real ELSE 1e1000 END",
        "SELECT CASE WHEN false THEN 1e1000 ELSE 1::real END",
        "SELECT CASE WHEN true THEN 1::double precision ELSE 1e1000 END",
        "SELECT CASE WHEN true THEN 1::real ELSE (1e1000::numeric)::real END",
        "SELECT COALESCE(1::real,1e1000)",
        "SELECT COALESCE(1::double precision,1e1000)",
    }) |sql| {
        var fixture: TestBackend = .{ .row_count = 0 };
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        var result = try execute(a, fixture.iface(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(f64, 1), result.output.rows[0][0].float);
    }
    for ([_][]const u8{
        "SELECT CASE WHEN false THEN 1::real ELSE 1e1000 END",
        "SELECT CASE WHEN true THEN 1e1000 ELSE 1::double precision END",
        "SELECT COALESCE(NULL::real,1e1000)",
        "SELECT GREATEST(1::real,1e1000)",
        "SELECT LEAST(1::double precision,1e1000)",
    }) |sql| {
        var fixture: TestBackend = .{ .row_count = 0 };
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(error.SqlNumericOutOfRange, execute(a, fixture.iface(), &compiled, &.{}, .{}));
    }
    var fixture: TestBackend = .{ .row_count = 0 };
    var backend = fixture.iface();
    backend.parameter_descriptor_hints = &.{.{ .kind = .integer, .element_type = .int32 }};
    var compiled = try compiler.compile(a, "SELECT COALESCE(1::real,1e1000),$1", .{});
    defer compiled.deinit();
    var result = try execute(a, backend, &compiled, &.{.{ .integer = 0 }}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(f64, 1), result.output.rows[0][0].float);
}

test "SQL numeric selectors unwind lazy conversion allocation failures" {
    const Harness = struct {
        fn run(a: std.mem.Allocator, compiled: *const compiler.Compiled) !void {
            var fixture: TestBackend = .{ .row_count = 0 };
            var backend = fixture.iface();
            backend.parameter_descriptor_hints = &.{.{ .kind = .number, .element_type = .numeric }};
            var result = try execute(a, backend, compiled, &.{.{ .string = "1.123456789" }}, .{});
            defer result.deinit();
            for (result.output.rows[0]) |cell| try std.testing.expectEqual(@as(f64, @as(f32, 1.123456789)), cell.float);
        }
    };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT coalesce($1,1::real,1e1000),greatest($1,1::real),least($1,2::real),CASE WHEN true THEN coalesce($1,1::real) ELSE 1e1000 END", .{});
    defer compiled.deinit();
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{&compiled});
}

test "SQL mixed numeric comparisons preserve prepared ANY ALL probe domains" {
    const a = std.testing.allocator;
    const Case = struct { sql: []const u8, parameter: Json, expected: []const Json };
    const cases = [_]Case{
        .{ .sql = "SELECT $1 = ANY(ARRAY[1.1::real]),$1 <> ALL(ARRAY[1.1::real]),$1 = ANY(ARRAY[1.1::double precision])", .parameter = .{ .string = "1.1" }, .expected = &.{ .{ .bool = false }, .{ .bool = true }, .{ .bool = true } } },
        .{ .sql = "SELECT $1 = ANY(ARRAY[0::real]),$1 <> ALL(ARRAY[0::real])", .parameter = .{ .string = "1e100" }, .expected = &.{ .{ .bool = false }, .{ .bool = true } } },
        .{ .sql = "SELECT $1 = ANY(ARRAY[1.1::real,NULL]),$1 <> ALL(ARRAY[1.1::real,NULL])", .parameter = .{ .string = "1.1" }, .expected = &.{ .null, .null } },
        .{ .sql = "SELECT $1 = ANY(ARRAY[]::real[]),$1 <> ALL(ARRAY[]::real[])", .parameter = .null, .expected = &.{ .{ .bool = false }, .{ .bool = true } } },
        .{ .sql = "SELECT $1 = ANY(NULL::real[]),$1 <> ALL(NULL::real[])", .parameter = .{ .string = "1.1" }, .expected = &.{ .null, .null } },
        .{ .sql = "SELECT 1.1 = ANY(ARRAY[1.1::real]),1.1 <> ALL(ARRAY[1.1::real]),$1::text", .parameter = .{ .string = "1.1" }, .expected = &.{ .{ .bool = false }, .{ .bool = true }, .{ .string = "1.1" } } },
    };
    for (cases) |case| {
        var fixture: TestBackend = .{ .row_count = 0 };
        var backend = fixture.iface();
        backend.parameter_descriptor_hints = &.{.{ .kind = .number, .element_type = .numeric }};
        var compiled = try compiler.compile(a, case.sql, .{});
        defer compiled.deinit();
        var result = try execute(a, backend, &compiled, &.{case.parameter}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
        for (result.output.rows[0], case.expected) |actual, expected| {
            try std.testing.expectEqual(std.meta.activeTag(expected), std.meta.activeTag(actual));
            switch (expected) {
                .bool => |value| try std.testing.expectEqual(value, actual.bool),
                .string => |value| try std.testing.expectEqualStrings(value, actual.string),
                .null => {},
                else => unreachable,
            }
        }
    }
}

test "SQL mixed numeric comparisons preserve NULLIF equality and return domains" {
    const a = std.testing.allocator;
    const Kind = @import("array_value.zig").ElementType;
    const cases = [_]struct { expression: []const u8, kind: Kind, text: ?[]const u8 }{
        .{ .expression = "NULLIF(1.1::real,1.1::numeric)", .kind = .float32, .text = "1.1" },
        .{ .expression = "NULLIF(1.1::numeric,1.1::real)", .kind = .float64, .text = "1.1" },
        .{ .expression = "NULLIF(1::real,1::numeric)", .kind = .float32, .text = null },
        .{ .expression = "NULLIF(1::numeric,1::real)", .kind = .float64, .text = null },
        .{ .expression = "NULLIF(1::int4,2::int8)", .kind = .int32, .text = "1" },
        .{ .expression = "NULLIF(1::int2,2::int8)", .kind = .int16, .text = "1" },
        .{ .expression = "NULLIF(1::int4,2::real)", .kind = .float64, .text = "1" },
        .{ .expression = "NULLIF(1::real,2::int4)", .kind = .float32, .text = "1" },
        .{ .expression = "NULLIF('9007199254740993'::int8,'9007199254740992'::double precision)", .kind = .float64, .text = null },
        .{ .expression = "NULLIF('9007199254740992'::double precision,'9007199254740993'::int8)", .kind = .float64, .text = null },
        .{ .expression = "NULLIF(1::int4,2::numeric)", .kind = .numeric, .text = "1" },
        .{ .expression = "NULLIF(1.20::numeric,2::int4)", .kind = .numeric, .text = "1.20" },
        .{ .expression = "NULLIF('9007199254740993.1200'::numeric,2::int8)", .kind = .numeric, .text = "9007199254740993.1200" },
        .{ .expression = "NULLIF(NULL::numeric,1::real)", .kind = .float64, .text = null },
        .{ .expression = "NULLIF(1.1::real,NULL::numeric)", .kind = .float32, .text = "1.1" },
        .{ .expression = "NULLIF('NaN'::numeric,'NaN'::real)", .kind = .float64, .text = null },
    };
    for (cases) |case| {
        var fixture: TestBackend = .{ .row_count = 0 };
        const sql = try std.fmt.allocPrint(a, "SELECT {s},({s})::text", .{ case.expression, case.expression });
        defer a.free(sql);
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        var result = try execute(a, fixture.iface(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(?Kind, case.kind), result.output.columns[0].element_type);
        const actual = result.output.rows[0][1];
        if (case.text) |text| {
            try std.testing.expectEqual(std.meta.Tag(Json).string, std.meta.activeTag(actual));
            try std.testing.expectEqualStrings(text, actual.string);
        } else try std.testing.expectEqual(std.meta.Tag(Json).null, std.meta.activeTag(actual));
    }
    var overflow = try compiler.compile(a, "SELECT NULLIF(NULL::real,1e1000::numeric)", .{});
    defer overflow.deinit();
    var overflow_fixture: TestBackend = .{ .row_count = 0 };
    try std.testing.expectError(error.SqlNumericOutOfRange, execute(a, overflow_fixture.iface(), &overflow, &.{}, .{}));
    var fixture: TestBackend = .{ .row_count = 0 };
    var backend = fixture.iface();
    backend.parameter_descriptor_hints = &.{.{ .kind = .number, .element_type = .numeric }};
    var compiled = try compiler.compile(a, "SELECT NULLIF($1,1.1::real),NULLIF(1.1::real,$1)", .{});
    defer compiled.deinit();
    var result = try execute(a, backend, &compiled, &.{.{ .string = "1.1" }}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(?Kind, .float64), result.output.columns[0].element_type);
    try std.testing.expectEqual(@as(?Kind, .float32), result.output.columns[1].element_type);
    try std.testing.expectEqual(@as(f64, 1.1), result.output.rows[0][0].float);
    try std.testing.expectEqual(@as(f64, @as(f32, 1.1)), result.output.rows[0][1].float);
}

test "SQL mixed numeric comparisons unwind prepared allocation failures" {
    const Harness = struct {
        fn run(a: std.mem.Allocator, compiled: *const compiler.Compiled) !void {
            var fixture: TestBackend = .{ .row_count = 0 };
            var backend = fixture.iface();
            backend.parameter_descriptor_hints = &.{.{ .kind = .number, .element_type = .numeric }};
            var result = try execute(a, backend, compiled, &.{.{ .string = "1.1" }}, .{});
            defer result.deinit();
            try std.testing.expect(!result.output.rows[0][0].bool);
            try std.testing.expect(result.output.rows[0][1] != .null);
            try std.testing.expect(result.output.rows[0][2] != .null);
        }
    };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT $1 = ANY(ARRAY[1.1::real]),NULLIF($1,1.1::real),NULLIF(1.1::real,$1)", .{});
    defer compiled.deinit();
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{&compiled});
}

test "SQL floating text consumers preserve declared width exponents and signed zero" {
    const a = std.testing.allocator;
    for ([_]struct { sql: []const u8, expected: []const u8 }{
        .{ .sql = "SELECT concat(1.1::real)", .expected = "1.1" },
        .{ .sql = "SELECT concat(1.1::double precision)", .expected = "1.1" },
        .{ .sql = "SELECT concat('0.00001'::real)", .expected = "1e-05" },
        .{ .sql = "SELECT concat('-0'::real)", .expected = "-0" },
        .{ .sql = "SELECT concat(NULL::real)", .expected = "" },
        .{ .sql = "SELECT concat_ws(',',NULL::real)", .expected = "" },
        .{ .sql = "SELECT concat('NaN'::real,'Infinity'::double precision,'-Infinity'::real)", .expected = "NaNInfinity-Infinity" },
        .{ .sql = "SELECT concat_ws(',',NULL,1.1::real,'1e20'::double precision,'-0'::real)", .expected = "1.1,1e+20,-0" },
        .{ .sql = "SELECT concat(1.20::numeric,9007199254740993::bigint)", .expected = "1.209007199254740993" },
        .{ .sql = "SELECT concat_ws(',',1.20::numeric,'1'::jsonb)", .expected = "1.20,1" },
        .{ .sql = "SELECT concat(x) FROM (VALUES (1.1::real)) t(x)", .expected = "1.1" },
    }) |case| {
        var fixture: TestBackend = .{ .row_count = 0 };
        var compiled = try compiler.compile(a, case.sql, .{});
        defer compiled.deinit();
        var result = try execute(a, fixture.iface(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqualStrings(case.expected, result.output.rows[0][0].string);
    }
    for ([_]@import("array_value.zig").ElementType{ .float32, .float64 }) |kind| {
        var fixture: TestBackend = .{ .row_count = 0 };
        var backend = fixture.iface();
        backend.parameter_descriptor_hints = &.{.{ .kind = .number, .element_type = kind }};
        var compiled = try compiler.compile(a, "SELECT concat($1),concat_ws(',',$1),jsonb_build_object($1,1)", .{});
        defer compiled.deinit();
        const number: f64 = @as(f32, 1.1);
        var result = try execute(a, backend, &compiled, &.{.{ .float = number }}, .{});
        defer result.deinit();
        const expected = if (kind == .float32) "1.1" else "1.100000023841858";
        try std.testing.expectEqualStrings(expected, result.output.rows[0][0].string);
        try std.testing.expectEqualStrings(expected, result.output.rows[0][1].string);
        const object = result.output.rows[0][2].object;
        try std.testing.expectEqual(@as(usize, 1), object.count());
        try std.testing.expectEqual(@as(i64, 1), object.get(expected).?.integer);
        try std.testing.expectError(error.InvalidSqlParameters, execute(a, backend, &compiled, &.{.null}, .{}));
    }
}

test "SQL JSONB object keys reject JSON domains and retain ordinary SQL scalar keys" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "SELECT jsonb_build_object('1'::jsonb,2)",
        "SELECT jsonb_build_object('true'::jsonb,2)",
        "SELECT jsonb_build_object('\"key\"'::jsonb,2)",
        "SELECT jsonb_build_object('null'::jsonb,2)",
        "SELECT jsonb_build_object('{}'::jsonb,2)",
        "SELECT jsonb_build_object('[]'::jsonb,2)",
        "SELECT jsonb_build_object('1'::json,2)",
        "SELECT jsonb_build_object(to_jsonb(1),2)",
        "SELECT jsonb_build_object('valid',1,'1'::jsonb,2)",
    }) |sql| {
        var fixture: TestBackend = .{ .row_count = 0 };
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(error.InvalidSqlParameters, execute(a, fixture.iface(), &compiled, &.{}, .{}));
    }
    var fixture: TestBackend = .{ .row_count = 0 };
    var parameter = try compiler.compile(a, "SELECT jsonb_build_object($1,2)", .{});
    defer parameter.deinit();
    var backend = fixture.iface();
    backend.parameter_descriptor_hints = &.{.{ .kind = .json, .element_type = .jsonb }};
    for ([_]Json{ .{ .integer = 1 }, .{ .bool = true }, .{ .string = "key" } }) |key| {
        try std.testing.expectError(error.InvalidSqlParameters, execute(a, backend, &parameter, &.{key}, .{}));
    }
    var ordinary = try compiler.compile(a, "SELECT jsonb_build_object(1,'1'::jsonb,true,'true'::jsonb,'key','\"value\"'::jsonb)", .{});
    defer ordinary.deinit();
    var result = try execute(a, fixture.iface(), &ordinary, &.{}, .{});
    defer result.deinit();
    const json = try std.json.Stringify.valueAlloc(a, result.output.rows[0][0], .{});
    defer a.free(json);
    try std.testing.expectEqualStrings("{\"1\":1,\"true\":true,\"key\":\"value\"}", json);
}

test "SQL JSONB converts declared floating output to exact decimals and special strings" {
    const a = std.testing.allocator;
    const cases = [_]struct { expression: []const u8, expected: []const u8 }{
        .{ .expression = "to_jsonb(1.1::real)", .expected = "1.1" },
        .{ .expression = "to_jsonb(1.1::double precision)", .expected = "1.1" },
        .{ .expression = "to_jsonb('0.00001'::double precision)", .expected = "0.00001" },
        .{ .expression = "to_jsonb('1e20'::double precision)", .expected = "100000000000000000000" },
        .{ .expression = "to_jsonb('-0'::real)", .expected = "0" },
        .{ .expression = "to_jsonb('NaN'::real)", .expected = "\"NaN\"" },
        .{ .expression = "to_jsonb('NaN'::double precision)", .expected = "\"NaN\"" },
        .{ .expression = "to_jsonb('Infinity'::real)", .expected = "\"Infinity\"" },
        .{ .expression = "to_jsonb('-Infinity'::double precision)", .expected = "\"-Infinity\"" },
        .{ .expression = "jsonb_build_object('x',1.1::real)", .expected = "{\"x\":1.1}" },
        .{ .expression = "jsonb_build_object(1.1::real,2.2::real)", .expected = "{\"1.1\":2.2}" },
        .{ .expression = "jsonb_build_object(1.20::numeric,'Infinity'::double precision)", .expected = "{\"1.20\":\"Infinity\"}" },
        .{ .expression = "jsonb_build_object('1e20'::double precision,1)", .expected = "{\"1e+20\":1}" },
        .{ .expression = "jsonb_build_object('0.00001'::real,1)", .expected = "{\"1e-05\":1}" },
        .{ .expression = "jsonb_build_object('-0'::real,1)", .expected = "{\"-0\":1}" },
        .{ .expression = "jsonb_build_object('NaN'::real,1,'Infinity'::double precision,2)", .expected = "{\"NaN\":1,\"Infinity\":2}" },
        .{ .expression = "jsonb_typeof(to_jsonb('NaN'::real))", .expected = "\"string\"" },
        .{ .expression = "to_jsonb(1.1::real) = '1.1'::jsonb", .expected = "true" },
        .{ .expression = "to_jsonb(NULL::real)", .expected = "null" },
        .{ .expression = "jsonb_build_object('x',NULL::real)", .expected = "{\"x\":null}" },
    };
    for (cases) |case| {
        var fixture: TestBackend = .{ .row_count = 0 };
        const sql = try std.fmt.allocPrint(a, "SELECT {s}", .{case.expression});
        defer a.free(sql);
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        var result = try execute(a, fixture.iface(), &compiled, &.{}, .{});
        defer result.deinit();
        const json = try std.json.Stringify.valueAlloc(a, result.output.rows[0][0], .{});
        defer a.free(json);
        try std.testing.expectEqualStrings(case.expected, json);
    }
}

test "SQL floating text and JSONB conversion unwinds every allocation failure" {
    const Harness = struct {
        fn run(a: std.mem.Allocator, compiled: *const compiler.Compiled) !void {
            var fixture: TestBackend = .{ .row_count = 0 };
            var result = try execute(a, fixture.iface(), compiled, &.{}, .{});
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
        }
    };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT jsonb_build_object(1.20::numeric,1.1::real,'1e20'::double precision,1),to_jsonb('NaN'::real),to_jsonb('1e20'::double precision),concat(1.1::real),concat_ws(',','-0'::real,1.20::numeric)", .{});
    defer compiled.deinit();
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{&compiled});
}

test "SQL typed windows preserve NUMERIC and REAL semantics in memory and spill" {
    const a = std.testing.allocator;
    const Case = struct { sql: []const u8, expected: []const ?[]const u8, element: @import("array_value.zig").ElementType };
    const cases = [_]Case{
        .{ .sql = "SELECT SUM(x) OVER () FROM (VALUES (1.20::numeric),(2.30::numeric)) t(x)", .expected = &.{ "3.50", "3.50" }, .element = .numeric },
        .{ .sql = "SELECT AVG(x) OVER () FROM (VALUES (1.20::numeric),(2.30::numeric)) t(x)", .expected = &.{ "1.7500000000000000", "1.7500000000000000" }, .element = .numeric },
        .{ .sql = "SELECT SUM(x) OVER (ORDER BY k ROWS BETWEEN 1 PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) FROM (VALUES (1,1.20::numeric),(2,2.30::numeric),(3,NULL::numeric),(4,4.567::numeric)) t(k,x) ORDER BY k", .expected = &.{ null, "1.20", "2.30", null }, .element = .numeric },
        .{ .sql = "SELECT AVG(x) FILTER (WHERE k <> 2) OVER (ORDER BY k ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM (VALUES (1,1.20::numeric),(2,2.30::numeric),(3,3.40::numeric)) t(k,x) ORDER BY k", .expected = &.{ "1.20000000000000000000", "2.3000000000000000", "3.4000000000000000" }, .element = .numeric },
        .{ .sql = "SELECT SUM(x) OVER () FROM (VALUES ('9007199254740993.1200'::numeric),('0.0001'::numeric)) t(x)", .expected = &.{ "9007199254740993.1201", "9007199254740993.1201" }, .element = .numeric },
        .{ .sql = "SELECT SUM(x) OVER (ORDER BY k ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM (VALUES (1,'Infinity'::numeric),(2,'-Infinity'::numeric),(3,1.20::numeric)) t(k,x) ORDER BY k", .expected = &.{ "NaN", "-Infinity", "1.20" }, .element = .numeric },
        .{ .sql = "SELECT COUNT(*) OVER (ORDER BY x RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM (VALUES (1.20::numeric),(2.00::numeric),(3.50::numeric)) t(x) ORDER BY x", .expected = &.{ "1", "2", "1" }, .element = .int64 },
        .{ .sql = "SELECT COUNT(*) OVER (ORDER BY x DESC RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM (VALUES (1.20::numeric),(2.00::numeric),(3.50::numeric)) t(x) ORDER BY x DESC", .expected = &.{ "1", "1", "2" }, .element = .int64 },
        .{ .sql = "SELECT COUNT(*) OVER (ORDER BY x RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM (VALUES ('9007199254740993.12'::numeric),('9007199254740994.12'::numeric),('9007199254740995.13'::numeric)) t(x) ORDER BY x", .expected = &.{ "1", "2", "1" }, .element = .int64 },
        .{ .sql = "SELECT SUM(x) OVER () FROM (VALUES (16777216::real),(1::real),(-16777216::real)) t(x)", .expected = &.{ "0", "0", "0" }, .element = .float32 },
        .{ .sql = "SELECT SUM(x) OVER (ORDER BY k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM (VALUES (1,16777216::real),(2,1::real),(3,1::real),(4,-16777216::real)) t(k,x) ORDER BY k", .expected = &.{ "16777216", "16777216", "16777216", "0" }, .element = .float32 },
        .{ .sql = "SELECT SUM(x) FILTER (WHERE k <> 2) OVER (ORDER BY k ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW) FROM (VALUES (1,2::real),(2,NULL::real),(3,4::real),(4,8::real)) t(k,x) ORDER BY k", .expected = &.{ null, "6", "8", "4" }, .element = .float32 },
    };
    for (cases) |case| for ([_]bool{ false, true }) |spilled| {
        var fixture: TestBackend = .{ .row_count = 0 };
        var backend = fixture.iface();
        var manager: @import("spill.zig").Manager = .{ .alloc = a, .io = std.testing.io, .context = &fixture, .checkpoint = backend.vtable.checkpoint, .async_writes = false };
        defer manager.deinit();
        if (spilled) backend.spill_manager = &manager;
        var compiled = try compiler.compile(a, case.sql, .{});
        defer compiled.deinit();
        var result = try execute(a, backend, &compiled, &.{}, .{ .page_rows = if (spilled) 1 else 256, .retained_bytes = 256 * 1024 });
        defer result.deinit();
        if (spilled) try std.testing.expect(manager.written_bytes > 0);
        try std.testing.expectEqual(case.element, result.output.columns[0].element_type.?);
        try std.testing.expectEqual(case.expected.len, result.output.rows.len);
        for (result.output.rows, case.expected) |row, expected| {
            if (expected) |text| {
                if (case.element == .float32) {
                    try std.testing.expectEqual(try std.fmt.parseFloat(f64, text), row[0].float);
                } else try std.testing.expectEqualStrings(text, row[0].string);
            } else try std.testing.expect(row[0] == .null);
        }
    };
}

test "SQL real SUM retains float4 transitions and result identity in scalar and grouped plans" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "SELECT SUM(x) FROM (VALUES (16777216::real),(1::real),(-16777216::real)) t(x)",
        "SELECT SUM(DISTINCT x) FROM (VALUES (16777216::real),(1::real),(-16777216::real)) t(x)",
        "SELECT SUM(x) FROM (VALUES (1,16777216::real),(1,1::real),(1,-16777216::real)) t(g,x) GROUP BY g",
    }) |sql| {
        var fixture: TestBackend = .{ .row_count = 0 };
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        var result = try execute(a, fixture.iface(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(f64, 0), result.output.rows[0][0].float);
        try std.testing.expectEqual(@import("array_value.zig").ElementType.float32, result.output.columns[0].element_type.?);
    }
}

test "SQL NUMERIC set projections preserve exact cells across mapped batches" {
    const a = std.testing.allocator;
    var fixture: TestBackend = .{};
    var compiled = try compiler.compile(a, "SELECT '9007199254740993.1200'::numeric UNION ALL SELECT '9007199254740993.3400'::numeric", .{});
    defer compiled.deinit();
    var result = try execute(a, fixture.iface(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), result.output.rows.len);
    try std.testing.expectEqualStrings("9007199254740993.1200", result.output.rows[0][0].string);
    try std.testing.expectEqualStrings("9007199254740993.3400", result.output.rows[1][0].string);
    try std.testing.expectEqual(@import("array_value.zig").ElementType.numeric, result.output.columns[0].element_type.?);
}

test "SQL NUMERIC literals preserve PostgreSQL inference precision scale and mixed casts" {
    const a = std.testing.allocator;
    const cases = [_]struct { sql: []const u8, values: []const []const u8, numeric: bool = true }{
        .{ .sql = "SELECT 0.1 + 0.2, 1.0 / 3.0, 9223372036854775808, 1.2300, 1e3", .values = &.{ "0.3", "0.33333333333333333333", "9223372036854775808", "1.2300", "1000" } },
        .{ .sql = "SELECT -9223372036854775809, 18446744073709551616, -1.2300", .values = &.{ "-9223372036854775809", "18446744073709551616", "-1.2300" } },
        .{ .sql = "SELECT round(1.255, 2), trunc(-1.255, 2), round(1, 2), round(1250, -2)", .values = &.{ "1.26", "-1.25", "1.00", "1300" } },
        .{ .sql = "SELECT ceil(1.200), floor(-1.200), abs(-1.200), sign('Infinity'::numeric)", .values = &.{ "2", "-2", "1.200", "1" } },
        .{ .sql = "SELECT round('NaN'::numeric, 2), trunc('Infinity'::numeric, 2), mod(5.50, 2.0)", .values = &.{ "NaN", "Infinity", "1.50" } },
        .{ .sql = "SELECT 9223372036854775807, -9223372036854775808", .values = &.{ "9223372036854775807", "-9223372036854775808" }, .numeric = false },
        .{ .sql = "SELECT 0.1 + 0.2::DOUBLE PRECISION", .values = &.{"0.30000000000000004"}, .numeric = false },
        .{ .sql = "SELECT trunc(9007199254740993), ceil(1), floor(1::real), sign(-1)", .values = &.{ "9007199254740992", "1", "1", "-1" }, .numeric = false },
        .{ .sql = "SELECT round(2.5::double precision), round(3.5::double precision), round(-2.5::double precision), round(-3.5::double precision)", .values = &.{ "2", "4", "-2", "-4" }, .numeric = false },
        .{ .sql = "SELECT round(2.5), round(3.5), round(-2.5), round(-3.5)", .values = &.{ "3", "4", "-3", "-4" } },
        .{ .sql = "SELECT sqrt(2.0), sqrt(9007199254740993::numeric), sqrt(0.00), sqrt(1.2345678901234567890123456789)", .values = &.{ "1.414213562373095", "94906265.624251558", "0.000000000000000", "1.1111111061111110993611110582" } },
        .{ .sql = "SELECT sqrt('Infinity'::numeric), sqrt('NaN'::numeric)", .values = &.{ "Infinity", "NaN" } },
        .{ .sql = "SELECT sqrt(NULL::numeric)", .values = &.{"null"} },
        .{ .sql = "SELECT sqrt(NULL)", .values = &.{"null"}, .numeric = false },
        .{ .sql = "SELECT sqrt(4), sqrt(4::real), sqrt('4')", .values = &.{ "2", "2", "2" }, .numeric = false },
        .{ .sql = "SELECT CASE WHEN false THEN sqrt(-1.0) ELSE 2.0 END, CASE WHEN true THEN 2.0 ELSE sqrt(-1.0) END, COALESCE(2.0, sqrt(-1.0))", .values = &.{ "2.0", "2.0", "2.0" } },
    };
    for (cases) |case| {
        var fixture: TestBackend = .{ .row_count = 0 };
        var compiled = try compiler.compile(a, case.sql, .{});
        defer compiled.deinit();
        var result = try execute(a, fixture.iface(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
        for (case.values, result.output.rows[0], result.output.columns) |text, actual, column| {
            const rendered = if (actual == .string) actual.string else try std.json.Stringify.valueAlloc(a, actual, .{});
            defer if (actual != .string) a.free(rendered);
            try std.testing.expectEqualStrings(text, rendered);
            try std.testing.expectEqual(case.numeric, column.element_type == .numeric);
        }
    }
}

test "SQL NUMERIC scaled rounding resolves PostgreSQL overloads and strict nulls" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "SELECT round(1.25::double precision, 1)",
        "SELECT trunc(1.25::real, 1)",
        "SELECT round(1.25, 1::bigint)",
    }) |sql| {
        var fixture: TestBackend = .{ .row_count = 0 };
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(error.SqlUndefinedFunction, execute(a, fixture.iface(), &compiled, &.{}, .{}));
    }
    var fixture: TestBackend = .{ .row_count = 0 };
    var compiled = try compiler.compile(a, "SELECT round(NULL::numeric, 2), trunc(1.25, NULL::integer), round('1.255', '2')", .{});
    defer compiled.deinit();
    var result = try execute(a, fixture.iface(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expect(result.output.rows[0][0] == .null);
    try std.testing.expect(result.output.rows[0][1] == .null);
    try std.testing.expectEqualStrings("1.26", result.output.rows[0][2].string);
    for (result.output.columns) |column| try std.testing.expectEqual(@import("array_value.zig").ElementType.numeric, column.element_type.?);
}

test "SQL NUMERIC SUM AVG execute grouped distinct empty and special reductions" {
    const a = std.testing.allocator;
    const cases = [_]struct { sql: []const u8, rows: []const [2]?[]const u8, count: usize = 4 }{
        .{ .sql = "SELECT SUM(1.20), AVG(1.20) FROM things", .rows = &.{.{ "4.80", "1.20000000000000000000" }} },
        .{ .sql = "SELECT SUM(0.1 + 0.2), AVG(0.1 + 0.2) FROM things", .rows = &.{.{ "1.2", "0.30000000000000000000" }} },
        .{ .sql = "SELECT SUM('1.20'::numeric + _id::numeric), AVG('1.20'::numeric + _id::numeric) FROM things", .rows = &.{.{ "10.80", "2.7000000000000000" }} },
        .{ .sql = "SELECT SUM('1.20'::numeric + _id::numeric), AVG('1.20'::numeric + _id::numeric) FROM things GROUP BY _id::bigint % 2 ORDER BY _id::bigint % 2", .rows = &.{ .{ "4.40", "2.2000000000000000" }, .{ "6.40", "3.2000000000000000" } } },
        .{ .sql = "SELECT SUM(DISTINCT '1.20'::numeric), AVG(DISTINCT '1.20'::numeric) FROM things", .rows = &.{.{ "1.20", "1.20000000000000000000" }} },
        .{ .sql = "SELECT SUM(NULL::numeric), AVG(NULL::numeric) FROM things", .rows = &.{.{ null, null }} },
        .{ .sql = "SELECT SUM('1.20'::numeric), AVG('1.20'::numeric) FROM things", .rows = &.{.{ null, null }}, .count = 0 },
        .{ .sql = "SELECT SUM('Infinity'::numeric), AVG('Infinity'::numeric) FROM things", .rows = &.{.{ "Infinity", "Infinity" }} },
    };
    for (cases) |case| {
        var fixture: TestBackend = .{ .row_count = case.count };
        var compiled = try compiler.compile(a, case.sql, .{});
        defer compiled.deinit();
        var result = try execute(a, fixture.iface(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(case.rows.len, result.output.rows.len);
        for (case.rows, result.output.rows) |expected, actual| for (expected, actual) |text, cell| {
            if (text) |value| try std.testing.expectEqualStrings(value, cell.string) else try std.testing.expect(cell == .null);
        };
        for (result.output.columns) |column| try std.testing.expectEqual(@import("array_value.zig").ElementType.numeric, column.element_type.?);
    }
}

test "SQL NUMERIC grouped extrema preserve exact values and result identity" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "SELECT min('9007199254740993.1200'::numeric + _id::numeric), max('9007199254740993.1200'::numeric + _id::numeric) FROM things",
        "SELECT min('9007199254740993.1200'::numeric + _id::numeric), max('9007199254740993.1200'::numeric + _id::numeric) FROM things GROUP BY _id::bigint % 1",
    }) |sql| {
        var fixture: TestBackend = .{ .row_count = 4 };
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        var result = try execute(a, fixture.iface(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
        try std.testing.expectEqualStrings("9007199254740993.1200", result.output.rows[0][0].string);
        try std.testing.expectEqualStrings("9007199254740996.1200", result.output.rows[0][1].string);
        for (result.output.columns) |column| try std.testing.expectEqual(@import("array_value.zig").ElementType.numeric, column.element_type.?);
    }
}

test "SQL NUMERIC result and mutation boundaries own exact decimals through allocation faults" {
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, numericBoundaryScenario, .{});
}

test "SQL integer output canonicalizes exact native tokens without changing JSON numbers" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var fixture: TestBackend = .{};
    const context: Context = .{ .alloc = a, .arena = a, .backend = fixture.iface(), .binding = undefined, .parameters = &.{}, .limits = .{} };
    for ([_][]const u8{ "3", "9007199254740993", "9223372036854775807", "-9223372036854775808" }) |text| {
        const token: Json = .{ .number_string = text };
        const wire = try context.outputDatum(Datum.json(token), .integer, null);
        try std.testing.expectEqualStrings(text, wire.string);
        try std.testing.expectEqualStrings(text, (try context.outputDatum(Datum.json(token), .json, null)).number_string);
        var internal = context;
        internal.typed_output = true;
        try std.testing.expectEqual(try std.fmt.parseInt(i64, text, 10), (try internal.outputDatum(Datum.json(token), .integer, null)).integer);
    }
    try std.testing.expect((try context.outputDatum(.{}, .integer, null)) == .null);
    try std.testing.expectError(error.SqlTypeMismatch, context.outputDatum(Datum.json(.{ .number_string = "9223372036854775808" }), .integer, null));
    try std.testing.expectError(error.SqlTypeMismatch, context.outputDatum(Datum.json(.{ .number_string = "1.5" }), .integer, null));
}

fn mutationArrayOwnershipScenario(backing: std.mem.Allocator) !void {
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    var fixture: TestBackend = .{};
    const a = arena.allocator();
    const context: Context = .{ .alloc = backing, .arena = a, .backend = fixture.iface(), .binding = undefined, .parameters = &.{}, .limits = .{} };
    const column: catalog.Column = .{ .name = "a", .path = "a", .type = .array, .element_type = .jsonb };
    var input = try @import("array_text.zig").decode(backing, .jsonb, "[0:2]={\"null\",NULL,\"{\\\"x\\\":[1,2]}\"}", .{});
    const encoded = context.storageDatum(Datum.typedArray(&input.value), column) catch |err| {
        input.deinit();
        return err;
    };
    input.deinit();
    try std.testing.expect((try context.storageDatum(.{}, column)) == .null);
    try std.testing.expectError(error.SqlAssignmentTypeMismatch, context.storageDatum(Datum.json(.null), column));
    var required = column;
    required.nullable = false;
    try std.testing.expectError(error.SqlNotNullViolation, context.storageDatum(.{}, required));
    const table: catalog.Table = .{ .id = 1, .physical_name = "items", .schema_version = 1, .columns = &.{ column, .{ .name = "j", .path = "j", .type = .json }, .{ .name = "n", .path = "n", .type = .integer } } };
    var object: std.json.ObjectMap = .empty;
    try object.put(a, "a", encoded);
    try object.put(a, "j", .null);
    try object.put(a, "n", .{ .integer = 3 });
    const mutation: catalog.Mutation = .{ .key = "key", .expected_version = 0, .row = .{ .object = object }, .json_null_fields = &.{"j"} };
    const adapter = try Context.ReturningImages.init(a, table, null);
    const row = try adapter.row(a, mutation, mutation);
    const array = (try row.cell("a")).array.?;
    try std.testing.expectEqual(@as(i32, 0), array.dimensions[0].lower);
    try std.testing.expect(array.flatCell(0).value == .null and !array.flatCell(0).sql_null);
    try std.testing.expect(array.flatCell(1).sql_null);
    try std.testing.expectEqualStrings("2", array.flatCell(2).value.object.get("x").?.array.items[1].number_string);
    try std.testing.expect(!(try row.cell("j")).sql_null);
    const narrow = try Context.ReturningImages.init(a, table, &.{"n"});
    try std.testing.expect(!narrow.projection.has_typed_cells);
    const scalar_row = try narrow.row(a, mutation, mutation);
    try std.testing.expect(scalar_row.typed_cells == null);
    try std.testing.expectEqual(@as(i64, 3), (try scalar_row.cell("n")).value.integer);
}

test "SQL mutation array boundary owns payloads and unwinds every allocation failure" {
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, mutationArrayOwnershipScenario, .{});
}

test "SQL result boundary preserves array ownership descriptors and NULL provenance" {
    const a = std.testing.allocator;
    var arena: std.heap.ArenaAllocator = .init(a);
    defer arena.deinit();
    var fixture: TestBackend = .{};
    const context: Context = .{ .alloc = a, .arena = arena.allocator(), .backend = fixture.iface(), .binding = undefined, .parameters = &.{}, .limits = .{} };
    var input = try @import("array_text.zig").decode(a, .jsonb, "[0:2]={\"null\",NULL,\"{\\\"a\\\":[1,2]}\"}", .{});
    const output = try context.outputDatum(Datum.typedArray(&input.value), .array, .jsonb);
    input.deinit();
    const values = output.object.get("values").?.array.items;
    const flags = output.object.get("sql_nulls").?.array.items;
    try std.testing.expect(values[0] == .null and !flags[0].bool);
    try std.testing.expect(values[1] == .null and flags[1].bool);
    try std.testing.expectEqualStrings("2", values[2].object.get("a").?.array.items[1].number_string);
    try std.testing.expectEqual(@as(i64, 0), output.object.get("dimensions").?.array.items[0].object.get("lower_bound").?.integer);
    try std.testing.expect((try context.outputDatum(.{}, .array, .jsonb)) == .null);
    try std.testing.expectError(error.SqlTypeMismatch, context.outputDatum(Datum.json(.null), .array, .jsonb));
    var integers = try @import("array_text.zig").decode(a, .int64, "{9223372036854775807,NULL}", .{});
    defer integers.deinit();
    const big = try context.outputDatum(Datum.typedArray(&integers.value), .array, .int64);
    try std.testing.expectEqualStrings("9223372036854775807", big.object.get("values").?.array.items[0].string);
    try std.testing.expectError(error.SqlTypeMismatch, context.outputDatum(Datum.typedArray(&integers.value), .array, .int32));
    try std.testing.expectError(error.SqlTypeMismatch, context.outputDatum(Datum.typedArray(&integers.value), .json, null));
}

test {
    _ = @import("view_lateral_test.zig");
}

test "SQL UUIDv7 executes through runtime with exact extraction and interval metadata" {
    const a = std.testing.allocator;
    var source: TestBackend = .{};
    var backend = source.iface();
    backend.execution_io = std.testing.io;
    var compiled = try compiler.compile(a, "SELECT uuidv7() AS id, uuid_extract_version(uuidv7()) AS version, uuid_extract_timestamp(uuidv7(INTERVAL '1 day')) AS created, INTERVAL '1 month 2 days 3 hours' AS shift", .{});
    defer compiled.deinit();
    var result = try execute(a, backend, &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
    try std.testing.expectEqual(@as(?u4, 7), @import("../common/uuid.zig").version(try @import("../common/uuid.zig").parse(result.output.rows[0][0].string)));
    try std.testing.expectEqualStrings("7", result.output.rows[0][1].string);
    try std.testing.expectEqual(@as(?@import("array_value.zig").ElementType, .int16), result.output.columns[1].element_type);
    try std.testing.expectEqual(ast.ColumnType.datetime, result.output.columns[2].type);
    try std.testing.expectEqual(@as(?@import("temporal_value.zig").Kind, .interval), result.output.columns[3].temporal_kind);
    try std.testing.expectEqualStrings("1 mon 2 days 03:00:00", result.output.rows[0][3].string);
    for ([_][]const u8{
        "SELECT uuid_extract_version(uuidv7(d.shift)) FROM (SELECT INTERVAL '1 day' AS shift) d",
        "SELECT uuid_extract_version(uuidv7(d.shift)) FROM (VALUES (INTERVAL '1 day')) AS d(shift)",
    }) |sql| {
        var derived = try compiler.compile(a, sql, .{});
        defer derived.deinit();
        var executed = try execute(a, backend, &derived, &.{}, .{});
        defer executed.deinit();
        try std.testing.expectEqualStrings("7", executed.output.rows[0][0].string);
    }
    for ([_][]const u8{ "SELECT uuidv7(INTERVAL 'infinity')", "SELECT uuidv7(INTERVAL '-100000 years')" }) |sql| {
        var invalid = try compiler.compile(a, sql, .{});
        defer invalid.deinit();
        try std.testing.expectError(error.TemporalOutOfRange, execute(a, backend, &invalid, &.{}, .{}));
        try std.testing.expectEqualStrings("22008", @import("errors.zig").describe(error.TemporalOutOfRange).code);
    }
}

const TestBackend = struct {
    const stored_tag_elements = [_]Datum{ Datum.fromJson(.{ .string = "urgent" }), .{}, Datum.fromJson(.{ .string = "vip" }) };
    stored_arrays: bool = false,
    estimate_calls: usize = 0,
    estimated_search: bool = false,
    estimate_scans: bool = false,
    enable_dynamic_filters: bool = false,
    dynamic_filters: usize = 0,
    pages: usize = 0,
    writes: usize = 0,
    row_count: usize = 2,
    group_every: ?usize = null,
    group_modulo: ?usize = null,
    metadata_count: ?u64 = null,
    cancelled: bool = false,
    ambiguous: bool = false,
    outcome: catalog.MutationOutcome = .committed,
    point_reads: usize = 0,
    row_goal: ?u64 = null,
    primary_order: bool = false,
    statement_opens: usize = 0,
    statement_closes: usize = 0,
    statement_scan_count: usize = 0,

    fn iface(self: *TestBackend) catalog.Backend {
        return .{ .ptr = self, .pinned_statement_snapshot = true, .vtable = &.{ .resolve = resolve, .scan = scan, .mutate = mutate, .checkpoint = checkpoint } };
    }
    fn coordinated(self: *TestBackend) catalog.Backend {
        return .{ .ptr = self, .vtable = &.{ .resolve = resolve, .scan = scan, .mutate = mutate, .checkpoint = checkpoint, .open_statement = openStatement, .estimate_scans = estimates } };
    }
    fn estimates(ptr: *anyopaque, alloc: std.mem.Allocator, scans: []const catalog.StatementScan) ![]const catalog.ScanEstimate {
        const self: *TestBackend = @ptrCast(@alignCast(ptr));
        self.estimate_calls += 1;
        const output = try alloc.alloc(catalog.ScanEstimate, scans.len);
        for (scans, output) |scan_request, *estimate| {
            if (scan_request.request.search) |search| {
                try std.testing.expectEqualStrings("{}", search.request_text.?);
                self.estimated_search = true;
            }
            estimate.* = .{ .estimated_rows = if (self.estimate_scans) self.row_count else null };
        }
        return output;
    }
    const Statement = struct {
        backend: *TestBackend,
        alloc: std.mem.Allocator,
        cursors: []catalog.Cursor,
        states: []State,
        const State = struct { owner: *Statement, table: catalog.Table, request: catalog.Scan, after: ?[]const u8 = null, done: bool = false, started: bool = false };
        fn next(ptr: *anyopaque, alloc: std.mem.Allocator, limit: u32) !catalog.Page {
            const state: *State = @ptrCast(@alignCast(ptr));
            if (state.done) return .{ .rows = &.{} };
            state.started = true;
            var request = state.request;
            request.limit = limit;
            request.after = state.after;
            const page = try TestBackend.scan(state.owner.backend, alloc, state.table, request);
            const next_key = if (page.after) |key| try state.owner.alloc.dupe(u8, key) else null;
            if (state.after) |key| state.owner.alloc.free(key);
            state.after = next_key;
            state.done = page.after == null;
            return page;
        }
        fn setFilter(ptr: *anyopaque, filter: *const @import("dynamic_filter.zig").Filter) !bool {
            const state: *State = @ptrCast(@alignCast(ptr));
            try std.testing.expect(!state.started);
            try std.testing.expect(filter.sealed);
            state.owner.backend.dynamic_filters += 1;
            return false; // The fixture keeps row execution to verify exact results.
        }
        fn closeCursor(_: *anyopaque) void {
            @panic("statement-owned cursor closed individually");
        }
        fn close(ptr: *anyopaque) void {
            const self: *Statement = @ptrCast(@alignCast(ptr));
            self.backend.statement_closes += 1;
            for (self.states) |state| if (state.after) |key| self.alloc.free(key);
            self.alloc.free(self.states);
            self.alloc.free(self.cursors);
            self.alloc.destroy(self);
        }
    };
    fn openStatement(ptr: *anyopaque, alloc: std.mem.Allocator, scans: []const catalog.StatementScan) !catalog.StatementRead {
        const backend: *TestBackend = @ptrCast(@alignCast(ptr));
        const owner = try alloc.create(Statement);
        errdefer alloc.destroy(owner);
        const cursors = try alloc.alloc(catalog.Cursor, scans.len);
        errdefer alloc.free(cursors);
        const states = try alloc.alloc(Statement.State, scans.len);
        owner.* = .{ .backend = backend, .alloc = alloc, .cursors = cursors, .states = states };
        for (scans, states, cursors) |request, *state, *cursor| {
            state.* = .{ .owner = owner, .table = request.table, .request = request.request };
            cursor.* = .{ .ptr = state, .next = Statement.next, .close = Statement.closeCursor, .estimated_rows = if (backend.estimate_scans) backend.row_count else null, .set_dynamic_filter = if (backend.enable_dynamic_filters) Statement.setFilter else null };
        }
        backend.statement_opens += 1;
        backend.statement_scan_count = scans.len;
        return .{ .ptr = owner, .cursors = cursors, .close = Statement.close };
    }
    fn resolve(ptr: *anyopaque, _: std.mem.Allocator, _: ast.Name, _: catalog.Action) !catalog.Table {
        const self: *TestBackend = @ptrCast(@alignCast(ptr));
        const columns: []const catalog.Column = &.{
            .{ .name = "id", .path = "id", .type = .integer, .nullable = false },
            .{ .name = "tags", .path = "tags", .type = .array, .element_type = .text },
        };
        return .{ .id = 19, .physical_name = "table:stable", .schema_version = 7, .columns = if (self.stored_arrays) columns else columns[0..1] };
    }
    fn scan(ptr: *anyopaque, alloc: std.mem.Allocator, table_def: catalog.Table, request: catalog.Scan) !catalog.Page {
        const self: *TestBackend = @ptrCast(@alignCast(ptr));
        try std.testing.expectEqual(@as(u64, 19), table_def.id);
        try std.testing.expectEqual(@as(u32, 7), table_def.schema_version);
        self.pages += 1;
        self.primary_order = request.primary_order;
        self.row_goal = request.row_goal;
        const from = if (request.primary_key orelse request.after) |key| try std.fmt.parseInt(usize, key, 10) else 0;
        if (request.primary_key != null) self.point_reads += 1;
        const count_rows = @min(if (request.primary_key != null) @as(u32, 1) else request.limit, self.row_count -| from);
        const rows = try alloc.alloc(catalog.Row, count_rows);
        for (rows, from..) |*row, index| {
            var object: std.json.ObjectMap = .empty;
            try object.put(alloc, "id", if (self.group_modulo) |modulo| .{ .integer = @intCast(index % modulo) } else if (self.group_every) |every| .{ .integer = @intCast(index / every) } else .{ .number_string = "9007199254740993" });
            row.* = .{ .id = try std.fmt.allocPrint(alloc, "{d}", .{index}), .version = 99, .value = .{ .object = object } };
            if (self.stored_arrays) {
                const elements: []const Datum = if (index == 0) &stored_tag_elements else &.{};
                const dimensions = try alloc.alloc(@import("array_value.zig").Dimension, 1);
                dimensions[0] = .{ .length = @intCast(elements.len) };
                const array = try alloc.create(@import("array_value.zig").Value);
                array.* = try @import("array_value.zig").Value.init(.text, dimensions, elements, .{});
                row.* = try catalog.Row.fromDatums(alloc, row.id, try catalog.Row.TypedLayout.init(alloc, &.{ "id", "tags" }), &.{ Datum.fromJson(object.get("id").?), if (index == 2) .{} else Datum.typedArray(array) });
                row.version = 99;
            }
        }
        return .{ .rows = rows, .after = if (request.primary_key == null and from + count_rows < self.row_count) try std.fmt.allocPrint(alloc, "{d}", .{from + count_rows}) else null };
    }
    fn mutate(ptr: *anyopaque, _: std.mem.Allocator, _: std.mem.Allocator, table_def: catalog.Table, mutations: []const catalog.Mutation) !catalog.MutationOutcome {
        const self: *TestBackend = @ptrCast(@alignCast(ptr));
        self.writes += 1;
        try std.testing.expectEqual(@as(u32, 7), table_def.schema_version);
        for (mutations) |mutation| {
            try std.testing.expectEqual(@as(u64, 99), mutation.expected_version);
            if (mutation.row) |row| try std.testing.expectEqual(@as(i64, 9007199254740993), row.object.get("id").?.integer);
        }
        if (self.ambiguous) return error.AmbiguousCommit;
        return self.outcome;
    }
    fn checkpoint(ptr: *anyopaque) !void {
        const self: *TestBackend = @ptrCast(@alignCast(ptr));
        if (self.cancelled) return error.Cancelled;
    }
};

test "SQL INSERT hands exact JSONB display tokens to mutation preparation" {
    const Provider = struct {
        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: ast.Name, _: catalog.Action) !catalog.Table {
            return .{ .id = 19, .physical_name = "table:stable", .schema_version = 7, .columns = &.{
                .{ .name = "payload", .path = "payload", .type = .json, .element_type = .jsonb },
            } };
        }
        fn prepare(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, mutations: []const catalog.Mutation) ![]const catalog.Mutation {
            const payload = mutations[0].row.?.object.get("payload").?;
            try std.testing.expectEqualStrings("1.2300e2", payload.object.get("source").?.number_string);
            return mutations;
        }
        fn mutate(_: *anyopaque, _: std.mem.Allocator, _: std.mem.Allocator, _: catalog.Table, mutations: []const catalog.Mutation) !catalog.MutationOutcome {
            try std.testing.expectEqualStrings("1.2300e2", mutations[0].row.?.object.get("payload").?.object.get("source").?.number_string);
            return .committed;
        }
    };
    var provider: TestBackend = .{};
    const backend: catalog.Backend = .{ .ptr = &provider, .vtable = &.{ .resolve = Provider.resolve, .scan = TestBackend.scan, .mutate = Provider.mutate, .mutate_prepared = Provider.mutate, .prepare_mutations = Provider.prepare, .checkpoint = TestBackend.checkpoint } };
    var compiled = try compiler.compile(std.testing.allocator, "INSERT INTO things (_id,payload) VALUES ('one','{\"source\":1.2300e2}'::jsonb)", .{});
    defer compiled.deinit();
    var result = try execute(std.testing.allocator, backend, &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.output.rows_affected);
}

test "SQL commit serialization is released before the statement result" {
    const Provider = struct {
        fn mutate(_: *anyopaque, alloc: std.mem.Allocator, _: std.mem.Allocator, _: catalog.Table, mutations: []const catalog.Mutation) !catalog.MutationOutcome {
            try std.testing.expect(mutations[0].unique_absence);
            // Model a provider's temporary wire buffer. The callback contract
            // supplies an arena and owns its lifetime even on allocation failure.
            const wire = try alloc.alloc(u8, 256 * 1024);
            @memset(wire, 1);
            return .committed;
        }
        fn run(alloc: std.mem.Allocator) !void {
            var provider: TestBackend = .{};
            const backend: catalog.Backend = .{ .ptr = &provider, .vtable = &.{ .resolve = TestBackend.resolve, .scan = TestBackend.scan, .mutate = mutate, .checkpoint = TestBackend.checkpoint } };
            var compiled = try compiler.compile(alloc, "INSERT INTO things (_id,id) VALUES ('one',1)", .{});
            defer compiled.deinit();
            var budget: MemoryBudget = .{ .backing = alloc, .limit = 1024 * 1024 };
            var result = try execute(budget.allocator(), backend, &compiled, &.{}, .{});
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 1), result.output.rows_affected);
            try std.testing.expect(budget.peak >= 256 * 1024);
            try std.testing.expect(budget.live < 64 * 1024);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Provider.run, .{});
}

test "SQL EXPLAIN binds authorized plans without reading or writing rows" {
    const Authority = struct {
        scans: usize = 0,
        writes: usize = 0,
        write_authorizations: usize = 0,
        last_action: ?catalog.Action = null,
        deny_write: bool = false,
        fn iface(self: *@This()) catalog.Backend {
            return .{ .ptr = self, .vtable = &.{ .resolve = resolve, .scan = scan, .mutate = mutate, .checkpoint = checkpoint, .generate_row_id = generateRowId } };
        }
        fn resolve(ptr: *anyopaque, _: std.mem.Allocator, _: ast.Name, action: catalog.Action) !catalog.Table {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.last_action = action;
            if (action != .read) self.write_authorizations += 1;
            if (self.deny_write and action != .read) return error.Forbidden;
            return .{ .id = 19, .physical_name = "table:stable", .schema_version = 7, .columns = &.{
                .{ .name = "id", .path = "id", .type = .string, .nullable = false },
                .{ .name = "status", .path = "status", .type = .string },
            } };
        }
        fn scan(ptr: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.scans += 1;
            return error.UnexpectedScan;
        }
        fn mutate(ptr: *anyopaque, _: std.mem.Allocator, _: std.mem.Allocator, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.writes += 1;
            return error.UnexpectedMutation;
        }
        fn checkpoint(_: *anyopaque) !void {}
        fn generateRowId(_: *anyopaque, _: std.mem.Allocator) ![]const u8 {
            return error.UnexpectedRowIdGeneration;
        }
    };
    var backend: Authority = .{};
    for ([_]struct { sql: []const u8, contains: []const u8, action: catalog.Action }{
        .{ .sql = "EXPLAIN SELECT id FROM things WHERE id = '1'", .contains = "Select on table:stable", .action = .read },
        .{ .sql = "EXPLAIN INSERT INTO things (_id, id) VALUES ('new', '1')", .contains = "Insert on table:stable", .action = .write },
        .{ .sql = "EXPLAIN (FORMAT JSON, VERBOSE, COSTS OFF) SELECT id FROM things", .contains = "\"schema_version\":7", .action = .read },
        .{ .sql = "EXPLAIN (FORMAT JSON, COSTS OFF) SELECT a.id FROM things a JOIN things b ON a.id = b.id", .contains = "Hash Join", .action = .read },
        .{ .sql = "EXPLAIN WITH c AS MATERIALIZED (SELECT id FROM things) SELECT a.id FROM c a JOIN c b ON a.id = b.id", .contains = "Materialized Reference", .action = .read },
    }) |case| {
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var result = try execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqualStrings("EXPLAIN", result.output.command_tag);
        try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
        try std.testing.expectEqual(@as(usize, 1), result.output.rows[0].len);
        try std.testing.expect(std.mem.indexOf(u8, result.output.rows[0][0].string, case.contains) != null);
        try std.testing.expectEqual(case.action, backend.last_action.?);
    }
    try std.testing.expectEqual(@as(usize, 0), backend.scans);
    try std.testing.expectEqual(@as(usize, 0), backend.writes);
    const corpus = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, @embedFile("fixtures/sql_parity_inventory.json"), .{});
    defer corpus.deinit();
    for ([_]struct { id: []const u8, kind: []const u8 }{
        .{ .id = "sql-0066", .kind = "Insert" },
        .{ .id = "sql-0068", .kind = "Update" },
        .{ .id = "sql-0069", .kind = "Merge" },
    }) |case| {
        backend.write_authorizations = 0;
        const exact_sql = for (corpus.value.object.get("entries").?.array.items) |entry| {
            if (std.mem.eql(u8, entry.object.get("id").?.string, case.id)) break entry.object.get("sql").?.string;
        } else return error.TestMissingCorpusCase;
        var compiled = try compiler.compile(std.testing.allocator, exact_sql, .{});
        defer compiled.deinit();
        var result = try execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expect(std.mem.indexOf(u8, result.output.rows[0][0].string, case.kind) != null);
        try std.testing.expect(backend.write_authorizations > 0);
    }
    try std.testing.expectEqual(@as(usize, 0), backend.scans);
    try std.testing.expectEqual(@as(usize, 0), backend.writes);
    backend.deny_write = true;
    var denied = try compiler.compile(std.testing.allocator, "EXPLAIN INSERT INTO things (_id, id) VALUES ('denied', '1')", .{});
    defer denied.deinit();
    try std.testing.expectError(error.Forbidden, execute(std.testing.allocator, backend.iface(), &denied, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 0), backend.writes);
    for ([_][]const u8{"EXPLAIN DROP TABLE things"}) |sql| {
        try std.testing.expectError(error.UnsupportedSqlShape, compiler.compile(std.testing.allocator, sql, .{}));
    }
}

test "SQL INSERT VALUES scalar subqueries prepare a bounded source before writing" {
    const ValuesBackend = struct {
        writes: usize = 0,
        large: bool = false,
        checkpoints: usize = 0,
        cancel_after: usize = std.math.maxInt(usize),
        fn iface(self: *@This()) catalog.Backend {
            return .{ .ptr = self, .vtable = &.{ .resolve = resolve, .scan = scan, .mutate = mutate, .checkpoint = checkpoint } };
        }
        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: ast.Name, action: catalog.Action) !catalog.Table {
            try std.testing.expectEqual(catalog.Action.write, action);
            return .{ .id = 19, .physical_name = "things", .schema_version = 1, .columns = &.{
                .{ .name = "n", .path = "n", .type = .integer, .nullable = false },
                .{ .name = "created_at", .path = "created_at", .type = .datetime },
                .{ .name = "payload", .path = "payload", .type = .json },
            } };
        }
        fn scan(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
            return error.UnexpectedIndependentScan;
        }
        fn mutate(ptr: *anyopaque, _: std.mem.Allocator, _: std.mem.Allocator, _: catalog.Table, mutations: []const catalog.Mutation) !catalog.MutationOutcome {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.large) {
                try std.testing.expectEqual(@as(usize, 1000), mutations.len);
                for (mutations, 0..) |mutation, index| {
                    var key_buffer: [32]u8 = undefined;
                    const expected_key = try std.fmt.bufPrint(&key_buffer, "r{d}", .{index});
                    try std.testing.expectEqualStrings(expected_key, mutation.key);
                    try std.testing.expectEqual(@as(i64, if (index == 0) 7 else 8), mutation.row.?.object.get("n").?.integer);
                }
                self.writes += mutations.len;
                return .committed;
            }
            try std.testing.expectEqual(@as(usize, 2), mutations.len);
            for (mutations, [_][]const u8{ "a", "b" }, [_]i64{ 7, 8 }) |mutation, key, value| {
                try std.testing.expectEqualStrings(key, mutation.key);
                try std.testing.expectEqual(value, mutation.row.?.object.get("n").?.integer);
                if (mutation.row.?.object.get("payload")) |payload| try std.testing.expectEqualStrings(if (std.mem.eql(u8, key, "a")) "hello" else "world", payload.string);
            }
            self.writes += mutations.len;
            return .committed;
        }
        fn checkpoint(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.checkpoints += 1;
            if (self.checkpoints > self.cancel_after) return error.Canceled;
        }
    };
    for ([_]struct { sql: []const u8, params: []const Json }{
        .{ .sql = "INSERT INTO things (_id,n) VALUES ('a',(SELECT $1::bigint)),('b',(SELECT 8))", .params = &.{.{ .integer = 7 }} },
        .{ .sql = "INSERT INTO things (_id,n) VALUES ('a',(SELECT 7)),('b','8')", .params = &.{} },
        .{ .sql = "INSERT INTO things (_id,n,created_at,payload) VALUES ('a',(SELECT 7),'2026-01-01T00:00:00Z','hello'),('b','8','2026-01-02T00:00:00Z','world')", .params = &.{} },
    }) |case| {
        var backend: ValuesBackend = .{};
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        try std.testing.expect(compiled.statement.insert.source != null);
        var result = try execute(std.testing.allocator, backend.iface(), &compiled, case.params, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(u64, 2), result.output.rows_affected);
        try std.testing.expectEqual(@as(usize, 2), backend.writes);
    }
    var rejected_backend: ValuesBackend = .{};
    var invalid = try compiler.compile(std.testing.allocator, "INSERT INTO things (_id,n) VALUES ('a',(SELECT 7)),('b',TRUE)", .{});
    defer invalid.deinit();
    try std.testing.expectError(error.SqlTypeMismatch, execute(std.testing.allocator, rejected_backend.iface(), &invalid, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 0), rejected_backend.writes);
    var empty_backend: ValuesBackend = .{};
    var empty = try compiler.compile(std.testing.allocator, "INSERT INTO things (_id,n) VALUES ('a',(SELECT 7 WHERE FALSE)),('b',8)", .{});
    defer empty.deinit();
    try std.testing.expectError(error.SqlNotNullViolation, execute(std.testing.allocator, empty_backend.iface(), &empty, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 0), empty_backend.writes);
    var null_backend: ValuesBackend = .{};
    var null_row = try compiler.compile(std.testing.allocator, "INSERT INTO things (_id,n) VALUES ('a',(SELECT 7)),('b',NULL)", .{});
    defer null_row.deinit();
    try std.testing.expectError(error.SqlNotNullViolation, execute(std.testing.allocator, null_backend.iface(), &null_row, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 0), null_backend.writes);
    var large_sql: std.ArrayList(u8) = .empty;
    defer large_sql.deinit(std.testing.allocator);
    try large_sql.appendSlice(std.testing.allocator, "INSERT INTO things (_id,n) VALUES ");
    for (0..1000) |index| {
        const row = try std.fmt.allocPrint(std.testing.allocator, "{s}('r{d}',{s})", .{ if (index == 0) "" else ",", index, if (index == 0) "(SELECT 7)" else "8" });
        defer std.testing.allocator.free(row);
        try large_sql.appendSlice(std.testing.allocator, row);
    }
    var large = try compiler.compile(std.testing.allocator, large_sql.items, .{});
    defer large.deinit();
    var backend: ValuesBackend = .{ .large = true };
    var bound = try describe.describe(std.testing.allocator, backend.iface(), &large, &.{});
    defer bound.deinit();
    try std.testing.expect(bound.binding.insert_source != null);
    switch (bound.binding.insert_source.?.relation.?.root.operation) {
        .values => |arms| {
            try std.testing.expectEqual(@as(usize, 2), arms.len);
            switch (arms[1].operation) {
                .literal_rows => |rows| try std.testing.expectEqual(@as(usize, 999), rows.len),
                else => return error.TestUnexpectedResult,
            }
        },
        else => return error.TestUnexpectedResult,
    }
    try std.testing.expectEqual(@as(usize, 0), backend.writes);
    var limited: ValuesBackend = .{ .large = true };
    try std.testing.expectError(error.SqlWorkingMemoryLimitExceeded, execute(std.testing.allocator, limited.iface(), &large, &.{}, .{ .retained_bytes = 1024 * 1024 }));
    try std.testing.expectEqual(@as(usize, 0), limited.writes);
    var canceled: ValuesBackend = .{ .large = true, .cancel_after = 8 };
    try std.testing.expectError(error.Canceled, execute(std.testing.allocator, canceled.iface(), &large, &.{}, .{ .retained_bytes = 4 * 1024 * 1024 }));
    try std.testing.expectEqual(@as(usize, 0), canceled.writes);
    var large_result = try execute(std.testing.allocator, backend.iface(), &large, &.{}, .{ .retained_bytes = 4 * 1024 * 1024 });
    defer large_result.deinit();
    try std.testing.expectEqual(@as(u64, 1000), large_result.output.rows_affected);
    try std.testing.expectEqual(@as(usize, 1000), backend.writes);
    var late_parameter_sql: std.ArrayList(u8) = .empty;
    defer late_parameter_sql.deinit(std.testing.allocator);
    try late_parameter_sql.appendSlice(std.testing.allocator, "INSERT INTO things (_id,n) VALUES ");
    for (0..128) |index| {
        const row = try std.fmt.allocPrint(std.testing.allocator, "{s}('p{d}',{s})", .{ if (index == 0) "" else ",", index, if (index == 0) "(SELECT 7)" else if (index == 127) "$1" else "8" });
        defer std.testing.allocator.free(row);
        try late_parameter_sql.appendSlice(std.testing.allocator, row);
    }
    var late_parameter = try compiler.compile(std.testing.allocator, late_parameter_sql.items, .{});
    defer late_parameter.deinit();
    var late_bound = try describe.describe(std.testing.allocator, backend.iface(), &late_parameter, &.{});
    defer late_bound.deinit();
    try std.testing.expectEqual(@as(?ast.ColumnType, .integer), late_bound.binding.parameter_types[0]);
}

test "SQL INSERT VALUES self-subquery closes its captured read before commit" {
    const SelfBackend = struct {
        const Self = @This();
        const CursorState = struct {
            owner: *Self,
            done: bool = false,
            fn next(ptr: *anyopaque, alloc: std.mem.Allocator, _: u32) !catalog.Page {
                const state: *@This() = @ptrCast(@alignCast(ptr));
                if (state.done) return .{ .rows = &.{} };
                state.done = true;
                const rows = try alloc.alloc(catalog.Row, state.owner.source_rows);
                for (rows, 0..) |*row, index| {
                    var object: std.json.ObjectMap = .empty;
                    try object.put(alloc, "n", .{ .integer = @intCast(7 + index) });
                    row.* = .{ .id = if (index == 0) "old" else "other", .version = 1, .value = .{ .object = object } };
                }
                return .{ .rows = rows };
            }
            fn close(_: *anyopaque) void {}
        };
        captures: usize = 0,
        source_rows: usize = 1,
        closes: usize = 0,
        writes: usize = 0,
        states: [4]CursorState = undefined,
        cursors: [4]catalog.Cursor = undefined,
        fn iface(self: *@This()) catalog.Backend {
            return .{ .ptr = self, .pinned_statement_snapshot = true, .vtable = &.{ .resolve = resolve, .scan = scan, .open_statement = open, .mutate = mutate, .checkpoint = checkpoint } };
        }
        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: ast.Name, action: catalog.Action) !catalog.Table {
            try std.testing.expect(action == .read or action == .write);
            return .{ .id = 19, .physical_name = "things", .schema_version = 1, .columns = &.{.{ .name = "n", .path = "n", .type = .integer, .nullable = false }} };
        }
        fn open(ptr: *anyopaque, _: std.mem.Allocator, scans: []const catalog.StatementScan) !catalog.StatementRead {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (scans.len == 0 or scans.len > self.states.len) return error.TestUnexpectedScanCount;
            self.captures += 1;
            for (scans, self.states[0..scans.len], self.cursors[0..scans.len]) |request, *state, *cursor| {
                try std.testing.expectEqual(@as(u64, 19), request.table.id);
                state.* = .{ .owner = self };
                cursor.* = .{ .ptr = state, .next = CursorState.next, .close = CursorState.close };
            }
            return .{ .ptr = self, .cursors = self.cursors[0..scans.len], .close = close };
        }
        fn close(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.closes += 1;
        }
        fn scan(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
            return error.UnexpectedIndependentScan;
        }
        fn mutate(ptr: *anyopaque, _: std.mem.Allocator, _: std.mem.Allocator, _: catalog.Table, mutations: []const catalog.Mutation) !catalog.MutationOutcome {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(self.captures, self.closes);
            try std.testing.expectEqual(@as(usize, 1), mutations.len);
            try std.testing.expectEqualStrings("new", mutations[0].key);
            try std.testing.expectEqual(@as(i64, 7), mutations[0].row.?.object.get("n").?.integer);
            self.writes += 1;
            return .committed;
        }
        fn checkpoint(_: *anyopaque) !void {}
    };
    var backend: SelfBackend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "INSERT INTO things (_id,n) VALUES ('new',(SELECT n FROM things WHERE _id='old'))", .{});
    defer compiled.deinit();
    var result = try execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(u64, 1), result.output.rows_affected);
    try std.testing.expectEqual(@as(usize, 1), backend.captures);
    try std.testing.expectEqual(@as(usize, 1), backend.closes);
    try std.testing.expectEqual(@as(usize, 1), backend.writes);
    var multiple: SelfBackend = .{ .source_rows = 2 };
    var invalid = try compiler.compile(std.testing.allocator, "INSERT INTO things (_id,n) VALUES ('new',(SELECT n FROM things))", .{});
    defer invalid.deinit();
    try std.testing.expectError(error.SqlCardinalityViolation, execute(std.testing.allocator, multiple.iface(), &invalid, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 1), multiple.captures);
    try std.testing.expectEqual(@as(usize, 1), multiple.closes);
    try std.testing.expectEqual(@as(usize, 0), multiple.writes);
}

test "SQL grouped aggregation streams pages then applies HAVING ORDER OFFSET and LIMIT" {
    var backend: TestBackend = .{ .row_count = 8 };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT _id::bigint % 2 AS bucket, sum(_id::bigint) AS amount, count(*) AS n FROM things WHERE _id != '0' GROUP BY bucket HAVING sum(_id::bigint) > 5 ORDER BY amount DESC LIMIT 1 OFFSET 1", .{});
    defer compiled.deinit();
    var result = try execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{ .page_rows = 1 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 8), backend.pages);
    try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
    try std.testing.expectEqualStrings("0", result.output.rows[0][0].string);
    try std.testing.expectEqualStrings("12", result.output.rows[0][1].string);
    try std.testing.expectEqualStrings("3", result.output.rows[0][2].string);
}

test "SQL JSON record projections preserve typed arrays paths and NULLs through lateral relations" {
    const a = std.testing.allocator;
    var backend: TestBackend = .{};
    const Case = struct { sql: []const u8, expected: []const u8 };
    for ([_]Case{
        .{
            .sql = "SELECT r.title,r.metadata->>'plan' AS plan,array_to_string(r.tags,'|','<null>') AS tags,cardinality(r.tags) AS n FROM jsonb_to_record('{\"title\":\"alpha\",\"metadata\":{\"plan\":\"pro\"},\"tags\":[\"urgent\",null,\"vip\"]}') AS r(title TEXT,metadata JSONB,tags TEXT[])",
            .expected = "[[\"alpha\",\"pro\",\"urgent|<null>|vip\",\"3\"]]",
        },
        .{ .sql = "SELECT cardinality(r.tags),r.extra FROM jsonb_to_record('{\"tags\":[]}') AS r(tags TEXT[],extra BIGINT)", .expected = "[[\"0\",null]]" },
        .{ .sql = "SELECT r.a,r.b FROM jsonb_to_record(NULL::jsonb) AS r(a JSONB,b TEXT[])", .expected = "[[null,null]]" },
        .{ .sql = "SELECT r.a,r.b FROM jsonb_to_record('{\"a\":null,\"b\":null}') AS r(a JSONB,b TEXT[])", .expected = "[[null,null]]" },
        .{ .sql = "SELECT r.n,cardinality(r.tags) FROM (VALUES('{\"n\":1,\"tags\":[\"x\"]}'::jsonb),('{\"n\":2,\"tags\":[]}'::jsonb)) d(body),jsonb_to_record(d.body) AS r(n BIGINT,tags TEXT[]) ORDER BY r.n", .expected = "[[\"1\",\"1\"],[\"2\",\"0\"]]" },
        .{ .sql = "SELECT t.tag,t.n FROM jsonb_to_record('{\"tags\":[\"urgent\",null,\"vip\"]}') AS r(tags TEXT[]),LATERAL UNNEST(r.tags) WITH ORDINALITY AS t(tag,n) ORDER BY t.n", .expected = "[[\"urgent\",\"1\"],[null,\"2\"],[\"vip\",\"3\"]]" },
        .{ .sql = "SELECT r.n,r.b FROM pg_catalog.jsonb_to_record('{\"n\":\"9007199254740993\",\"b\":\"yes\"}') AS r(n BIGINT,b BOOLEAN)", .expected = "[[\"9007199254740993\",true]]" },
        .{ .sql = "SELECT array_ndims(r.a),array_length(r.a,1),array_length(r.a,2),array_to_string(r.a,',','NULL') FROM jsonb_to_record('{\"a\":[[1,2],[3,null]]}') AS r(a BIGINT[])", .expected = "[[\"2\",\"2\",\"2\",\"1,2,3,NULL\"]]" },
    }) |case| {
        errdefer std.debug.print("JSON record projection: {s}\n", .{case.sql});
        var compiled = try compiler.compile(a, case.sql, .{});
        defer compiled.deinit();
        var result = try execute(a, backend.iface(), &compiled, &.{}, .{});
        defer result.deinit();
        const json = try std.json.Stringify.valueAlloc(a, result.output.rows, .{});
        defer a.free(json);
        try std.testing.expectEqualStrings(case.expected, json);
    }
    for ([_]struct { sql: []const u8, code: []const u8 }{
        .{ .sql = "SELECT COUNT(*) FROM jsonb_to_record('[1]') AS r(id BIGINT)", .code = "22023" },
        .{ .sql = "SELECT COUNT(*) FROM jsonb_to_record('{\"n\":\"bad\"}') AS r(n BIGINT)", .code = "22P02" },
        .{ .sql = "SELECT 1 FROM jsonb_to_record('{\"n\":\"bad\"}') AS r(n BIGINT)", .code = "22P02" },
        .{ .sql = "SELECT * FROM jsonb_to_record('null') AS r(id BIGINT)", .code = "22023" },
        .{ .sql = "SELECT * FROM jsonb_to_record('{\"a\":[[1],[2,3]]}') AS r(a BIGINT[])", .code = "2202E" },
        .{ .sql = "SELECT * FROM jsonb_to_record(7) AS r(a BIGINT)", .code = "42883" },
    }) |case| {
        var compiled = try compiler.compile(a, case.sql, .{});
        defer compiled.deinit();
        var result = execute(a, backend.iface(), &compiled, &.{}, .{}) catch |err| {
            try std.testing.expectEqualStrings(case.code, @import("errors.zig").describe(err).code);
            continue;
        };
        result.deinit();
        return error.ExpectedJsonRecordFailure;
    }
}

test "SQL current_user preserves authenticated SQL identity instead of API key settings scope" {
    const Settings = @import("setting_catalog.zig");
    const Owner = struct {
        fn load(_: *anyopaque, _: std.mem.Allocator, scope: Settings.Scope) !Settings.RawSnapshot {
            return .{ .scope = scope, .epoch = 1, .definitions = &.{} };
        }
    };
    const a = std.testing.allocator;
    var fixture: TestBackend = .{};
    var backend = fixture.iface();
    backend.setting_capture = .{
        .owner = .{ .ptr = &fixture, .load = Owner.load },
        .scope = .{ .principal = "api-key:credential-123", .database = "default" },
        .sql_user = "alice",
    };
    var compiled = try compiler.compile(a, "SELECT CURRENT_USER", .{});
    defer compiled.deinit();
    try std.testing.expect(compiled.uses_current_setting);
    var result = try execute(a, backend, &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqualStrings("alice", result.output.rows[0][0].string);
    try std.testing.expectEqualStrings("current_user", result.output.columns[0].name);
    try std.testing.expectError(error.InvalidSqlSyntax, compiler.compile(a, "SELECT CURRENT_USER()", .{}));
}

test "SQL stored views execute derived plans in isolated scopes and preserve security barriers" {
    const Fixture = struct {
        rows: TestBackend = .{},
        query: *const ast.Select,
        expose_alias: bool = false,
        row_filter: ?catalog.ViewRowFilter = null,
        fn iface(self: *@This()) catalog.Backend {
            return .{ .ptr = self, .vtable = &.{
                .resolve_read = read,
                .resolve = resolve,
                .scan = scan,
                .open_statement = open,
                .mutate = mutate,
                .checkpoint = checkpoint,
            } };
        }
        fn read(raw: *anyopaque, a: std.mem.Allocator, name: ast.Name) !catalog.ReadRelation {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (std.mem.eql(u8, name.table, "stored")) {
                var scoped = self.iface();
                scoped.binding_scope = @intFromPtr(self.query);
                return .{ .view = .{
                    .id = 51,
                    .generation = 1,
                    .query = self.query,
                    .backend = scoped,
                    .security_barrier = true,
                    .columns = &.{.{ .name = "id", .path = "id", .type = .integer, .element_type = .int64 }},
                    .exposure = if (self.expose_alias) &.{.{ .ordinal = 0, .name = "frozen" }} else null,
                    .row_filter = self.row_filter,
                } };
            }
            return .{ .table = try resolve(raw, a, name, .read) };
        }
        fn resolve(raw: *anyopaque, a: std.mem.Allocator, name: ast.Name, action: catalog.Action) !catalog.Table {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!std.mem.eql(u8, name.table, "things")) return error.TableNotFound;
            var table = try TestBackend.resolve(&self.rows, a, name, action);
            table.read_authority = raw;
            return table;
        }
        fn scan(raw: *anyopaque, a: std.mem.Allocator, table: catalog.Table, request: catalog.Scan) !catalog.Page {
            const self: *@This() = @ptrCast(@alignCast(raw));
            // Outer equality demand cannot cross a barrier to become an
            // owner-side source filter without a leakproof-expression proof.
            try std.testing.expectEqual(@as(usize, 0), request.conditions.len);
            return TestBackend.scan(&self.rows, a, table, request);
        }
        fn mutate(_: *anyopaque, _: std.mem.Allocator, _: std.mem.Allocator, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
            return error.Forbidden;
        }
        fn open(raw: *anyopaque, a: std.mem.Allocator, scans: []const catalog.StatementScan) !catalog.StatementRead {
            const self: *@This() = @ptrCast(@alignCast(raw));
            for (scans) |request| {
                try std.testing.expectEqual(@as(usize, 0), request.request.conditions.len);
                try std.testing.expect(request.table.read_authority == raw);
            }
            return TestBackend.openStatement(&self.rows, a, scans);
        }
        fn checkpoint(_: *anyopaque) !void {}
    };
    const a = std.testing.allocator;
    var body = try compiler.compile(a, "SELECT id FROM things", .{});
    defer body.deinit();
    var fixture: Fixture = .{ .query = &body.statement.select };
    for ([_][]const u8{
        "SELECT id FROM stored WHERE id = 9007199254740993",
        "WITH things AS (SELECT 0::BIGINT AS id) SELECT id FROM stored",
        "SELECT s.id FROM stored s JOIN stored other ON s.id=other.id LIMIT 2",
    }) |sql| {
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        var result = try execute(a, fixture.iface(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 2), result.output.rows.len);
        try std.testing.expectEqualStrings("9007199254740993", result.output.rows[0][0].string);
    }
    fixture.expose_alias = true;
    {
        var exposed = try compiler.compile(a, "SELECT frozen FROM stored", .{});
        defer exposed.deinit();
        var result = try execute(a, fixture.iface(), &exposed, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 2), result.output.rows.len);
        try std.testing.expectEqualStrings("9007199254740993", result.output.rows[0][0].string);
    }
    fixture.expose_alias = false;
    {
        const SelectLast = struct {
            fn matches(raw: *const anyopaque, _: std.mem.Allocator, cells: []const Datum) !bool {
                const last: *const i64 = @ptrCast(@alignCast(raw));
                return cells[0].value.integer == last.*;
            }
        };
        var numbered = try compiler.compile(a, "SELECT CAST(_id AS BIGINT) AS id FROM things", .{});
        defer numbered.deinit();
        const last: i64 = 8191;
        fixture.query = &numbered.statement.select;
        fixture.rows.row_count = 8192;
        fixture.row_filter = .{ .ptr = &last, .matches = SelectLast.matches };
        var filtered = try compiler.compile(a, "SELECT id FROM stored LIMIT 1", .{});
        defer filtered.deinit();
        var result = try execute(a, fixture.iface(), &filtered, &.{}, .{ .retained_bytes = 2 * 1024 * 1024 });
        defer result.deinit();
        try std.testing.expectEqualStrings("8191", result.output.rows[0][0].string);
        var counted = try compiler.compile(a, "SELECT count(*) FROM stored", .{});
        defer counted.deinit();
        var count_result = try execute(a, fixture.iface(), &counted, &.{}, .{ .retained_bytes = 2 * 1024 * 1024 });
        defer count_result.deinit();
        try std.testing.expectEqualStrings("1", count_result.output.rows[0][0].string);
    }
    fixture.row_filter = null;
    var recursive = try compiler.compile(a, "SELECT id FROM stored", .{});
    defer recursive.deinit();
    fixture.query = &recursive.statement.select;
    try std.testing.expectError(error.SqlViewCycle, execute(a, fixture.iface(), &recursive, &.{}, .{}));
}

test "SQL high fanout join bounds candidate allocation churn" {
    // Every row shares a hash key; the residual must inspect 65,536 pairs.
    // Aggregation keeps result materialization out of the allocation measurement.
    var backend: TestBackend = .{ .row_count = 256 };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT count(*) FROM things a JOIN things b ON a.id = b.id AND a._id < b._id", .{});
    defer compiled.deinit();
    var counting = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var result = try execute(counting.allocator(), backend.coordinated(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
    try std.testing.expectEqualStrings("32640", result.output.rows[0][0].string);
    // Allow headroom for incidental executor changes, but not one backing
    // allocation per candidate/output row (68,120 before scratch reuse).
    try std.testing.expect(counting.allocations < 4096);
}

test "SQL aggregate admission sizes TopK by observed groups rather than result cap" {
    var backend: TestBackend = .{ .row_count = 8 };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT count(*) FROM things", .{});
    defer compiled.deinit();
    var result = try execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{ .result_rows = 4096, .retained_bytes = 128 * 1024 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
    try std.testing.expectEqualStrings("8", result.output.rows[0][0].string);
}

test "SQL joins use one coordinated cut and preserve typed outer nulls across pages" {
    var backend: TestBackend = .{ .row_count = 3 };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT a._id AS a, b._id AS b FROM things AS a LEFT JOIN (SELECT _id FROM things WHERE _id != '2') AS b ON a._id = b._id ORDER BY a._id", .{});
    defer compiled.deinit();
    var result = try execute(std.testing.allocator, backend.coordinated(), &compiled, &.{}, .{ .page_rows = 1 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 3), result.output.rows.len);
    try std.testing.expectEqualStrings("0", result.output.rows[0][0].string);
    try std.testing.expectEqualStrings("1", result.output.rows[1][1].string);
    try std.testing.expectEqualStrings("2", result.output.rows[2][0].string);
    try std.testing.expect(result.output.sql_nulls.?[2][1]);
    try std.testing.expectEqual(@as(usize, 1), backend.statement_opens);
    try std.testing.expectEqual(@as(usize, 1), backend.statement_closes);
    try std.testing.expectError(error.SqlStatementSnapshotRequired, execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{}));
}

test "SQL CTE aliases feed hash joins and grouped projection without stringifying rows" {
    var backend: TestBackend = .{ .row_count = 4 };
    var compiled = try compiler.compile(std.testing.allocator, "WITH q(k) AS (SELECT _id FROM things WHERE _id != '3') SELECT a.k, count(*) AS n FROM q AS a JOIN q AS b ON a.k = b.k GROUP BY a.k ORDER BY a.k DESC", .{});
    defer compiled.deinit();
    var result = try execute(std.testing.allocator, backend.coordinated(), &compiled, &.{}, .{ .page_rows = 1 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 3), result.output.rows.len);
    try std.testing.expectEqualStrings("2", result.output.rows[0][0].string);
    try std.testing.expectEqualStrings("1", result.output.rows[0][1].string);
    try std.testing.expectEqual(@as(usize, 1), backend.statement_closes);
}

test "SQL window ranks and ntile preserve PostgreSQL builtin widths and admission" {
    var backend: TestBackend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT row_number() OVER (), rank() OVER (), dense_rank() OVER (), ntile(2::smallint) OVER (), ntile(NULL) OVER () FROM (VALUES (1),(2),(3)) v(n)", .{});
    defer compiled.deinit();
    var result = try execute(std.testing.allocator, backend.coordinated(), &compiled, &.{}, .{});
    defer result.deinit();
    for (result.output.columns, [_]@import("array_value.zig").ElementType{ .int64, .int64, .int64, .int32, .int32 }) |column, expected| try std.testing.expectEqual(@as(?@import("array_value.zig").ElementType, expected), column.element_type);
    for (result.output.rows) |row| try std.testing.expect(row[4] == .null);
    for ([_][]const u8{ "SELECT ntile(2147483648) OVER ()", "SELECT ntile(2::bigint) OVER ()" }) |sql| {
        var rejected = try compiler.compile(std.testing.allocator, sql, .{});
        defer rejected.deinit();
        var empty: TestBackend = .{};
        try std.testing.expectError(error.UndefinedSqlFunction, execute(std.testing.allocator, empty.coordinated(), &rejected, &.{}, .{}));
        try std.testing.expectEqual(@as(usize, 0), empty.statement_opens);
    }
    for ([_][]const u8{ "SELECT ntile(0) OVER ()", "SELECT ntile(-1) OVER ()" }) |sql| {
        var rejected = try compiler.compile(std.testing.allocator, sql, .{});
        defer rejected.deinit();
        try std.testing.expectError(error.SqlInvalidNtileArgument, execute(std.testing.allocator, backend.coordinated(), &rejected, &.{}, .{}));
    }
}

test "SQL integer AVG preserves numeric signatures scale filters sliding exclusions and symbolic types" {
    const Case = struct { sql: []const u8, values: []const ?[]const u8 };
    for ([_]Case{
        .{ .sql = "SELECT avg(n) FROM (VALUES (1::bigint),(1),(2)) v(n)", .values = &.{"1.3333333333333333"} },
        .{ .sql = "SELECT avg(n) FROM (VALUES (9223372036854775807::bigint),(9223372036854775807)) v(n)", .values = &.{"9223372036854775807"} },
        .{ .sql = "SELECT avg(n) FROM (VALUES (9007199254740993::bigint),(9007199254740994)) v(n)", .values = &.{"9007199254740993.5000"} },
        .{ .sql = "SELECT avg(n) FROM (VALUES (0::integer),(0)) v(n)", .values = &.{"0.00000000000000000000"} },
        .{ .sql = "SELECT avg(n) FROM (VALUES (-1::smallint),(2::smallint)) v(n)", .values = &.{"0.50000000000000000000"} },
        .{ .sql = "SELECT avg(DISTINCT n) FILTER (WHERE n>0) FROM (VALUES (1),(2),(2),(NULL),(-1)) v(n)", .values = &.{"1.5000000000000000"} },
        .{ .sql = "SELECT avg(n) FILTER (WHERE false) FROM (VALUES (1::bigint)) v(n)", .values = &.{null} },
        .{ .sql = "SELECT avg(n) OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM (VALUES (0,9007199254740993::bigint),(1,9007199254740994::bigint),(2,NULL),(3,0::bigint)) v(i,n) ORDER BY i", .values = &.{ "9007199254740993.0000", "9007199254740993.5000", "9007199254740994.0000", "0.00000000000000000000" } },
        .{ .sql = "SELECT avg(n) OVER (ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) FROM (VALUES (0,1),(1,2),(2,3)) v(i,n) ORDER BY i", .values = &.{ "2.5000000000000000", "2.0000000000000000", "1.5000000000000000" } },
        .{ .sql = "SELECT CASE WHEN true THEN avg(n) ELSE 0 END AS x FROM (VALUES (1),(2)) v(n) UNION SELECT 1.5::numeric", .values = &.{"1.5000000000000000"} },
    }) |case| {
        var backend: TestBackend = .{};
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var result = try execute(std.testing.allocator, backend.coordinated(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(ast.ColumnType.number, result.output.columns[0].type);
        try std.testing.expectEqual(@as(?@import("array_value.zig").ElementType, .numeric), result.output.columns[0].element_type);
        try std.testing.expectEqual(case.values.len, result.output.rows.len);
        for (case.values, result.output.rows) |expected, row| {
            if (expected) |text| try std.testing.expectEqualStrings(text, row[0].string) else try std.testing.expect(row[0] == .null);
        }
    }
}

test "SQL bigint SUM preserves exact numeric outputs through grouped and window plans" {
    const Case = struct { sql: []const u8, values: []const ?[]const u8 };
    for ([_]Case{
        .{ .sql = "SELECT sum(n) FROM (VALUES (9223372036854775807::bigint),(9223372036854775807::bigint)) v(n)", .values = &.{"18446744073709551614"} },
        .{ .sql = "SELECT sum(DISTINCT n) FILTER (WHERE n>0) FROM (VALUES (9223372036854775807::bigint),(9223372036854775807::bigint),(-1::bigint)) v(n)", .values = &.{"9223372036854775807"} },
        .{ .sql = "SELECT sum(n) FILTER (WHERE false) FROM (VALUES (1::bigint)) v(n)", .values = &.{null} },
        .{ .sql = "SELECT sum(n) OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM (VALUES (0,9223372036854775807::bigint),(1,9223372036854775807::bigint),(2,-9223372036854775807::bigint)) v(i,n) ORDER BY i", .values = &.{ "9223372036854775807", "18446744073709551614", "0" } },
        .{ .sql = "SELECT sum(n) OVER (ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) FROM (VALUES (0,9223372036854775807::bigint),(1,9223372036854775807::bigint),(2,-9223372036854775807::bigint)) v(i,n) ORDER BY i", .values = &.{ "0", "0", "18446744073709551614" } },
    }) |case| {
        var backend: TestBackend = .{};
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var result = try execute(std.testing.allocator, backend.coordinated(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(ast.ColumnType.number, result.output.columns[0].type);
        try std.testing.expectEqual(@as(?@import("array_value.zig").ElementType, .numeric), result.output.columns[0].element_type);
        try std.testing.expectEqual(case.values.len, result.output.rows.len);
        for (case.values, result.output.rows) |expected, row| {
            if (expected) |text| try std.testing.expectEqualStrings(text, row[0].string) else try std.testing.expect(row[0] == .null);
        }
    }
}

test "SQL recursive type conflicts fail before physical statement admission" {
    var backend: TestBackend = .{ .row_count = 0 };
    var compiled = try compiler.compile(std.testing.allocator, "WITH RECURSIVE r(n) AS (SELECT id::integer FROM things UNION ALL SELECT n+2147483648 FROM r WHERE false) SELECT n FROM r", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.SqlRecursiveTypeMismatch, execute(std.testing.allocator, backend.coordinated(), &compiled, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 0), backend.statement_opens);
    try std.testing.expectEqual(@as(usize, 0), backend.pages);
    try std.testing.expectEqual(@as(usize, 0), backend.writes);
}

test "SQL set and VALUES preserve PostgreSQL integer literal builtin widths" {
    const Case = struct { sql: []const u8, width: @import("array_value.zig").ElementType, rows: usize = 2 };
    for ([_]Case{
        .{ .sql = "SELECT n FROM (VALUES (1),(2),(NULL)) v(n)", .width = .int32, .rows = 3 },
        .{ .sql = "SELECT n FROM (VALUES (1),(2147483648)) v(n)", .width = .int64 },
        .{ .sql = "SELECT n FROM (VALUES (1),(9223372036854775807)) v(n)", .width = .int64 },
        .{ .sql = "SELECT 1 AS n UNION ALL SELECT 2", .width = .int32 },
        .{ .sql = "SELECT 1::smallint AS n UNION ALL SELECT 2::smallint", .width = .int16 },
        .{ .sql = "SELECT 1::smallint AS n UNION ALL SELECT 2", .width = .int32 },
        .{ .sql = "SELECT 1 AS n UNION ALL SELECT 2147483648", .width = .int64 },
        .{ .sql = "SELECT NULL AS n UNION ALL SELECT 1", .width = .int32 },
    }) |case| {
        var backend: TestBackend = .{};
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var result = try execute(std.testing.allocator, backend.coordinated(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(case.rows, result.output.rows.len);
        try std.testing.expectEqual(@as(usize, 1), result.output.columns.len);
        try std.testing.expectEqualStrings("n", result.output.columns[0].name);
        try std.testing.expectEqual(ast.ColumnType.integer, result.output.columns[0].type);
        try std.testing.expectEqual(@as(?@import("array_value.zig").ElementType, case.width), result.output.columns[0].element_type);
    }
}

test "SQL materialized prefix replay preserves LIMIT errors through nested CTE owners" {
    for ([_][]const u8{
        "WITH q AS MATERIALIZED (SELECT 10 / (1-id) AS v FROM things) SELECT v FROM q LIMIT 1",
        "WITH q AS MATERIALIZED (SELECT 10 / (1-id) AS v FROM things), nested AS MATERIALIZED (SELECT v FROM q) SELECT v FROM nested LIMIT 1",
    }) |sql| {
        var backend: TestBackend = .{ .row_count = 4096, .group_every = 1 };
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var result = try execute(std.testing.allocator, backend.coordinated(), &compiled, &.{}, .{ .page_rows = 16, .scan_pages = 1 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
        try std.testing.expectEqualStrings("10", result.output.rows[0][0].string);
        try std.testing.expectEqual(@as(usize, 1), backend.pages);
        try std.testing.expectEqual(@as(usize, 1), backend.statement_opens);
        try std.testing.expectEqual(backend.statement_opens, backend.statement_closes);
    }
    var backend: TestBackend = .{ .row_count = 4096, .group_every = 1 };
    var compiled = try compiler.compile(std.testing.allocator, "WITH q AS MATERIALIZED (SELECT 10 / (1-id) AS v FROM things) SELECT v FROM q LIMIT 2", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.SqlDivisionByZero, execute(std.testing.allocator, backend.coordinated(), &compiled, &.{}, .{ .page_rows = 16, .scan_pages = 1 }));
    try std.testing.expectEqual(backend.statement_opens, backend.statement_closes);
}

test "SQL scoped materialized prefix replay retires outer epochs without draining invariant storage" {
    const Check = struct {
        fn run(a: std.mem.Allocator, compiled: *const compiler.Compiled) !void {
            var backend: TestBackend = .{ .row_count = 4096, .group_every = 1 };
            defer std.debug.assert(backend.statement_opens == backend.statement_closes);
            // The final relation has two rows, while the invariant native
            // producer supplies only its first row once across both epochs.
            var result = try execute(a, backend.coordinated(), compiled, &.{}, .{ .page_rows = 16, .scan_pages = 1, .scan_rows = 2 });
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 2), result.output.rows.len);
            for (result.output.rows, [_][]const u8{ "1", "2" }) |row, expected| {
                try std.testing.expectEqualStrings(expected, row[0].string);
                try std.testing.expectEqualStrings(expected, row[1].string);
            }
            try std.testing.expectEqual(@as(usize, 1), backend.pages);
            try std.testing.expectEqual(@as(usize, 1), backend.statement_opens);
        }
    };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT p.n, q.v FROM (VALUES (1),(2)) p(n) CROSS JOIN LATERAL (WITH c AS MATERIALIZED (SELECT id+p.n AS v FROM things), nested AS MATERIALIZED (SELECT v FROM c) SELECT v FROM nested LIMIT 1) q", .{});
    defer compiled.deinit();
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Check.run, .{&compiled});
}

test "SQL CTE materialization shares one bounded producer while inline hints retain independent scans" {
    // sql-1221, sql-1258, sql-1262: the mounted cases establish exact SQL
    // output; this producer-work test establishes the hint execution policy.
    const Case = struct { hint: []const u8, scans: usize };
    var materialized_pages: usize = 0;
    for ([_]Case{
        .{ .hint = "MATERIALIZED", .scans = 1 },
        .{ .hint = "", .scans = 1 },
        .{ .hint = "NOT MATERIALIZED", .scans = 2 },
    }) |case| {
        var backend: TestBackend = .{ .row_count = 4 };
        const sql = try std.fmt.allocPrint(std.testing.allocator, "WITH q(k) AS {s} (SELECT _id FROM things WHERE _id != '3') SELECT a.k FROM q AS a JOIN q AS b ON a.k = b.k ORDER BY a.k", .{case.hint});
        defer std.testing.allocator.free(sql);
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var result = try execute(std.testing.allocator, backend.coordinated(), &compiled, &.{}, .{ .page_rows = 1 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 3), result.output.rows.len);
        for (result.output.rows, [_][]const u8{ "0", "1", "2" }) |row, expected| try std.testing.expectEqualStrings(expected, row[0].string);
        try std.testing.expectEqual(case.scans, backend.statement_scan_count);
        try std.testing.expectEqual(@as(usize, 1), backend.statement_opens);
        try std.testing.expectEqual(@as(usize, 1), backend.statement_closes);
        if (case.scans == 1) materialized_pages = backend.pages else try std.testing.expect(backend.pages > materialized_pages);
    }
    {
        var backend: TestBackend = .{};
        var compiled = try compiler.compile(std.testing.allocator, "WITH q(v) AS MATERIALIZED (SELECT $1::bigint) SELECT a.v FROM q AS a JOIN q AS b ON a.v = b.v WHERE a.v = $2", .{});
        defer compiled.deinit();
        var result = try execute(std.testing.allocator, backend.coordinated(), &compiled, &.{ .{ .integer = 7 }, .{ .integer = 7 } }, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
        try std.testing.expectEqualStrings("7", result.output.rows[0][0].string);
    }
    {
        // Each alias consumes different producer columns. The one retained
        // producer must keep both columns even if the first alias uses one.
        var backend: TestBackend = .{ .row_count = 3 };
        var compiled = try compiler.compile(std.testing.allocator, "WITH q AS MATERIALIZED (SELECT _id AS k, id AS v FROM things) SELECT a.k, b.v FROM q AS a JOIN q AS b ON a.k = b.k ORDER BY a.k", .{});
        defer compiled.deinit();
        var result = try execute(std.testing.allocator, backend.coordinated(), &compiled, &.{}, .{ .page_rows = 1 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 3), result.output.rows.len);
        for (result.output.rows, [_][]const u8{ "0", "1", "2" }) |row, expected| {
            try std.testing.expectEqualStrings(expected, row[0].string);
            try std.testing.expectEqualStrings("9007199254740993", row[1].string);
        }
        try std.testing.expectEqual(@as(usize, 1), backend.statement_scan_count);
    }
    {
        // Inlining a multiply referenced downstream CTE must propagate its
        // demand to an automatic upstream producer instead of rescanning it.
        var backend: TestBackend = .{ .row_count = 3 };
        var compiled = try compiler.compile(std.testing.allocator, "WITH q AS (SELECT _id AS k FROM things), r AS NOT MATERIALIZED (SELECT k FROM q) SELECT a.k FROM r AS a JOIN r AS b ON a.k = b.k ORDER BY a.k", .{});
        defer compiled.deinit();
        var result = try execute(std.testing.allocator, backend.coordinated(), &compiled, &.{}, .{ .page_rows = 1 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 3), result.output.rows.len);
        try std.testing.expectEqual(@as(usize, 1), backend.statement_scan_count);
        try std.testing.expectEqual(@as(usize, 1), backend.statement_opens);
    }
}

test "SQL self joins preserve duplicate matches and evaluate complete ON residuals" {
    var backend: TestBackend = .{ .row_count = 3 };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT a._id AS a, b._id AS b FROM things a JOIN things b ON a.id = b.id AND a._id < b._id ORDER BY a._id, b._id", .{});
    defer compiled.deinit();
    var result = try execute(std.testing.allocator, backend.coordinated(), &compiled, &.{}, .{ .page_rows = 1 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 3), result.output.rows.len);
    for (result.output.rows, [_][2][]const u8{ .{ "0", "1" }, .{ "0", "2" }, .{ "1", "2" } }) |row, expected| {
        try std.testing.expectEqualStrings(expected[0], row[0].string);
        try std.testing.expectEqualStrings(expected[1], row[1].string);
    }
    try std.testing.expectEqual(@as(usize, 6), backend.pages);
    try std.testing.expectEqual(@as(usize, 1), backend.statement_closes);
}

test "SQL outer joins retain rejected ON candidates and SQL null keys as unmatched" {
    const cases = [_]struct { kind: []const u8, rows: usize, left_nulls: usize, right_nulls: usize }{
        .{ .kind = "LEFT", .rows = 3, .left_nulls = 1, .right_nulls = 2 },
        .{ .kind = "RIGHT", .rows = 3, .left_nulls = 2, .right_nulls = 1 },
        .{ .kind = "FULL", .rows = 5, .left_nulls = 3, .right_nulls = 3 },
    };
    for (cases) |case| {
        var backend: TestBackend = .{ .row_count = 3 };
        const sql = try std.fmt.allocPrint(std.testing.allocator, "WITH q AS (SELECT CASE WHEN _id = '2' THEN NULL ELSE _id END AS k FROM things) SELECT a.k, b.k FROM q a {s} JOIN q b ON a.k = b.k AND a.k != '1'", .{case.kind});
        defer std.testing.allocator.free(sql);
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var result = try execute(std.testing.allocator, backend.coordinated(), &compiled, &.{}, .{ .page_rows = 1 });
        defer result.deinit();
        try std.testing.expectEqual(case.rows, result.output.rows.len);
        var nulls = [_]usize{ 0, 0 };
        var matches: usize = 0;
        for (result.output.sql_nulls.?) |flags| {
            for (flags, &nulls) |flag, *count| count.* += @intFromBool(flag);
            matches += @intFromBool(!flags[0] and !flags[1]);
        }
        try std.testing.expectEqual(case.left_nulls, nulls[0]);
        try std.testing.expectEqual(case.right_nulls, nulls[1]);
        try std.testing.expectEqual(@as(usize, 1), matches);
        try std.testing.expectEqual(@as(usize, 1), backend.statement_closes);
    }
}

test "SQL derived joins preserve JSON null independently of outer SQL null" {
    var backend: TestBackend = .{ .row_count = 2 };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT a._id, b.j FROM things a LEFT JOIN (SELECT _id, 'null'::json AS j FROM things WHERE _id = '0') b ON a._id = b._id ORDER BY a._id", .{});
    defer compiled.deinit();
    var result = try execute(std.testing.allocator, backend.coordinated(), &compiled, &.{}, .{ .page_rows = 1 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), result.output.rows.len);
    try std.testing.expect(!result.output.sql_nulls.?[0][1]);
    try std.testing.expect(result.output.sql_nulls.?[1][1]);
    try std.testing.expect(result.output.rows[0][1] == .null);
    try std.testing.expect(result.output.rows[1][1] == .null);
}

test "SQL USING joins merge outer keys while preserving qualified input columns" {
    for ([_]struct { kind: []const u8, expected: []const []const u8 }{
        .{ .kind = "INNER", .expected = &.{"1"} },
        .{ .kind = "LEFT", .expected = &.{ "0", "1" } },
        .{ .kind = "RIGHT", .expected = &.{ "1", "2" } },
        .{ .kind = "FULL", .expected = &.{ "0", "1", "2" } },
    }) |case| {
        var backend: TestBackend = .{ .row_count = 3 };
        const sql = try std.fmt.allocPrint(std.testing.allocator, "SELECT k, a.k, b.k, m.k FROM (SELECT _id AS k FROM things WHERE _id != '2') a {s} JOIN (SELECT _id AS k FROM things WHERE _id != '0') b USING (k) AS m ORDER BY 1", .{case.kind});
        defer std.testing.allocator.free(sql);
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var result = try execute(std.testing.allocator, backend.coordinated(), &compiled, &.{}, .{ .page_rows = 1 });
        defer result.deinit();
        try std.testing.expectEqual(case.expected.len, result.output.rows.len);
        for (result.output.rows, result.output.sql_nulls.?, case.expected) |row, flags, expected| {
            try std.testing.expectEqualStrings(expected, row[0].string);
            try std.testing.expectEqualStrings(expected, row[3].string);
            try std.testing.expectEqual(std.mem.eql(u8, expected, "2"), flags[1]);
            try std.testing.expectEqual(std.mem.eql(u8, expected, "0"), flags[2]);
        }
        try std.testing.expectEqual(@as(usize, 1), backend.statement_opens);
        try std.testing.expectEqual(@as(usize, 1), backend.statement_closes);
    }
}

test "SQL USING and NATURAL retain star order composite keys bags and empty key products" {
    for ([_][]const u8{
        "SELECT count(*) FROM (SELECT id AS k, id AS z FROM things) a JOIN (SELECT id AS k, id AS z FROM things) b USING (z, k)",
        "SELECT count(*) FROM things a NATURAL JOIN things b",
        "SELECT count(*) FROM (SELECT _id FROM things) a NATURAL JOIN things b",
    }) |sql| {
        var backend: TestBackend = .{ .row_count = 2 };
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var result = try execute(std.testing.allocator, backend.coordinated(), &compiled, &.{}, .{});
        defer result.deinit();
        // All visible logical cells are equal; hidden physical _id differs.
        try std.testing.expectEqualStrings("4", result.output.rows[0][0].string);
    }
    {
        var backend: TestBackend = .{ .row_count = 2 };
        var compiled = try compiler.compile(std.testing.allocator, "SELECT *, a.*, b.* FROM (SELECT _id AS k, id AS x FROM things) a JOIN (SELECT _id AS k, id AS y FROM things) b USING (k) ORDER BY 1", .{});
        defer compiled.deinit();
        var result = try execute(std.testing.allocator, backend.coordinated(), &compiled, &.{}, .{ .page_rows = 1 });
        defer result.deinit();
        const names = [_][]const u8{ "k", "x", "y", "k", "x", "k", "y" };
        try std.testing.expectEqual(names.len, result.output.columns.len);
        for (result.output.columns, names) |column, name| try std.testing.expectEqualStrings(name, column.name);
        for (result.output.rows) |row| {
            try std.testing.expectEqualStrings(row[0].string, row[3].string);
            try std.testing.expectEqualStrings(row[0].string, row[5].string);
        }
    }
    {
        var backend: TestBackend = .{ .row_count = 2 };
        var compiled = try compiler.compile(std.testing.allocator, "SELECT count(*) FROM (SELECT _id AS x FROM things) a NATURAL FULL JOIN (SELECT _id AS y FROM things) b", .{});
        defer compiled.deinit();
        var result = try execute(std.testing.allocator, backend.coordinated(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqualStrings("4", result.output.rows[0][0].string);
    }
}

test "SQL USING rejects missing ambiguous and duplicate keys before capture" {
    for ([_]struct { sql: []const u8, failure: anyerror }{
        .{ .sql = "SELECT * FROM things a JOIN things b USING (absent)", .failure = error.UndefinedColumn },
        .{ .sql = "SELECT * FROM things a JOIN things b USING (id, id)", .failure = error.DuplicateSqlColumn },
        .{ .sql = "SELECT * FROM things a JOIN things b USING (id) AS a", .failure = error.DuplicateSqlAlias },
        .{ .sql = "SELECT * FROM things a CROSS JOIN things a", .failure = error.DuplicateSqlAlias },
        .{ .sql = "SELECT * FROM things a JOIN things a USING (id)", .failure = error.DuplicateSqlAlias },
        .{ .sql = "SELECT * FROM (SELECT id AS k, id AS k FROM things) a JOIN (SELECT id AS k FROM things) b USING (k)", .failure = error.AmbiguousSqlColumn },
    }) |case| {
        var backend: TestBackend = .{};
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(case.failure, execute(std.testing.allocator, backend.coordinated(), &compiled, &.{}, .{}));
        try std.testing.expectEqual(@as(usize, 0), backend.statement_opens);
    }
}

test "SQL USING common types null keys and chained merged namespaces" {
    const Case = struct { sql: []const u8, expected: []const u8 };
    for ([_]Case{
        .{ .sql = "SELECT k FROM (SELECT 1::integer AS k) a FULL JOIN (SELECT 2::bigint AS k) b USING(k) ORDER BY k", .expected = "[[\"1\"],[\"2\"]]" },
        .{ .sql = "SELECT k, a.k, b.k FROM (SELECT NULL::bigint AS k) a FULL JOIN (SELECT NULL::bigint AS k) b USING(k)", .expected = "[[null,null,null],[null,null,null]]" },
        .{ .sql = "SELECT k, a.k, b.k, c.k FROM (SELECT 1 AS k) a FULL JOIN (SELECT 2 AS k) b USING(k) FULL JOIN (SELECT 2 AS k) c USING(k) ORDER BY 1", .expected = "[[\"1\",\"1\",null,null],[\"2\",null,\"2\",\"2\"]]" },
    }) |case| {
        var backend: TestBackend = .{};
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var result = try execute(std.testing.allocator, backend.coordinated(), &compiled, &.{}, .{ .page_rows = 1 });
        defer result.deinit();
        const json = try std.json.Stringify.valueAlloc(std.testing.allocator, result.output.rows, .{});
        defer std.testing.allocator.free(json);
        try std.testing.expectEqualStrings(case.expected, json);
    }
}

test "SQL USING closes one coordinated capture across every allocation failure" {
    const Check = struct {
        fn run(alloc: std.mem.Allocator, compiled: *const compiler.Compiled) !void {
            var backend: TestBackend = .{ .row_count = 3 };
            defer std.debug.assert(backend.statement_opens == backend.statement_closes);
            var result = try execute(alloc, backend.coordinated(), compiled, &.{}, .{ .page_rows = 1 });
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
            try std.testing.expectEqualStrings("0", result.output.rows[0][0].string);
        }
    };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT k, a.k, b.k FROM (SELECT _id AS k FROM things WHERE _id != '2') a FULL JOIN (SELECT _id AS k FROM things WHERE _id != '0') b USING(k) ORDER BY 1 LIMIT 1", .{});
    defer compiled.deinit();
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Check.run, .{&compiled});
}

test "SQL UNNEST streams typed arrays with storage order zip padding and ordinality" {
    const a = std.testing.allocator;
    const Case = struct { id: []const u8, sql: []const u8, columns: []const []const u8, column_oids: []const u32, rows: []const []const std.json.Value, sql_nulls: []const []const bool };
    var reference = try std.json.parseFromSlice(struct { format: u8, reference: []const u8, entries: []const Case }, a, @embedFile("fixtures/sql_array_expansion_reference.json"), .{});
    defer reference.deinit();
    try std.testing.expectEqual(@as(u8, 1), reference.value.format);
    try std.testing.expectEqualStrings("PostgreSQL exact SQL", reference.value.reference);
    try std.testing.expectEqual(@as(usize, 17), reference.value.entries.len);
    for (reference.value.entries) |case| for ([_]u32{ 1, 3, 64 }) |page_rows| {
        var backend: TestBackend = .{ .row_count = 3, .stored_arrays = true };
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var result = try execute(std.testing.allocator, backend.coordinated(), &compiled, &.{}, .{ .page_rows = page_rows });
        defer result.deinit();
        const expected = try std.json.Stringify.valueAlloc(a, case.rows, .{});
        defer a.free(expected);
        const json = try std.json.Stringify.valueAlloc(std.testing.allocator, result.output.rows, .{});
        defer std.testing.allocator.free(json);
        try std.testing.expectEqualStrings(expected, json);
        try std.testing.expectEqual(case.columns.len, result.output.columns.len);
        for (result.output.columns, case.columns, case.column_oids) |column, name, oid| {
            try std.testing.expectEqualStrings(name, column.name);
            const actual = if (column.element_type) |element| element.oid() else switch (column.type) {
                .integer => @as(u32, 20),
                .number => 701,
                .string => 25,
                .uuid => 2950,
                .boolean => 16,
                .json => 3802,
                else => return error.TestUnexpectedResult,
            };
            try std.testing.expectEqual(oid, actual);
        }
        try std.testing.expectEqual(case.sql_nulls.len, result.output.sql_nulls.?.len);
        for (case.sql_nulls, result.output.sql_nulls.?) |want, actual| try std.testing.expectEqualSlices(bool, want, actual);
        try std.testing.expectEqual(backend.statement_opens, backend.statement_closes);
    };
}

test "SQL UNNEST implicit lateral inputs retain one capture and outer NULL extension" {
    for ([_][]const u8{ ", unnest", " CROSS JOIN LATERAL unnest", " JOIN LATERAL unnest" }, 0..) |syntax, index| {
        var backend: TestBackend = .{ .row_count = 3 };
        const sql = try std.fmt.allocPrint(std.testing.allocator, "SELECT d._id, tag FROM things d{s}(string_to_array(d._id || ' urgent', ' ')) AS tag{s} WHERE tag = 'urgent' ORDER BY d._id", .{ syntax, if (index == 2) " ON true" else "" });
        defer std.testing.allocator.free(sql);
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var result = try execute(std.testing.allocator, backend.coordinated(), &compiled, &.{}, .{ .page_rows = 1 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 3), result.output.rows.len);
        for (result.output.rows, [_][]const u8{ "0", "1", "2" }) |row, expected| {
            try std.testing.expectEqualStrings(expected, row[0].string);
            try std.testing.expectEqualStrings("urgent", row[1].string);
        }
        try std.testing.expectEqual(@as(usize, 1), backend.statement_opens);
        try std.testing.expectEqual(@as(usize, 1), backend.statement_closes);
    }
    {
        var backend: TestBackend = .{ .row_count = 3 };
        var compiled = try compiler.compile(std.testing.allocator, "SELECT d._id, t.v, t.n FROM things d LEFT JOIN unnest(CASE WHEN d._id = '1' THEN ARRAY[]::text[] ELSE ARRAY[NULL]::text[] END) WITH ORDINALITY t(v,n) ON true ORDER BY d._id", .{});
        defer compiled.deinit();
        var result = try execute(std.testing.allocator, backend.coordinated(), &compiled, &.{}, .{ .page_rows = 1 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 3), result.output.rows.len);
        for (result.output.sql_nulls.?, 0..) |flags, index| {
            try std.testing.expect(flags[1]);
            try std.testing.expectEqual(index == 1, flags[2]);
        }
        try std.testing.expectEqualStrings("1", result.output.rows[0][2].string);
    }
}

test "SQL UNNEST validates array inputs and lateral scope before capture" {
    for ([_]struct { sql: []const u8, failure: anyerror }{
        .{ .sql = "SELECT * FROM unnest(1)", .failure = error.SqlUndefinedFunction },
        .{ .sql = "SELECT * FROM pg_catalog.unnest(ARRAY[1],ARRAY[2])", .failure = error.SqlUndefinedFunction },
        .{ .sql = "SELECT * FROM unnest(NULL)", .failure = error.SqlAmbiguousFunction },
        .{ .sql = "SELECT * FROM unnest(ARRAY[1]) t(x,y)", .failure = error.InvalidSqlParameters },
        .{ .sql = "SELECT * FROM things d RIGHT JOIN unnest(ARRAY[d.id]) t ON true", .failure = error.InvalidLateralReference },
        .{ .sql = "SELECT t.unnest FROM unnest(ARRAY[1],ARRAY[2]) t", .failure = error.AmbiguousSqlColumn },
    }) |case| {
        var backend: TestBackend = .{};
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(case.failure, execute(std.testing.allocator, backend.coordinated(), &compiled, &.{}, .{}));
        try std.testing.expectEqual(@as(usize, 0), backend.statement_opens);
    }
}

test "SQL UNNEST demand admission and allocation failures never materialize expanded inputs" {
    const cells = try std.testing.allocator.alloc([]const u8, 10001);
    defer std.testing.allocator.free(cells);
    @memset(cells, "x");
    const joined = try std.mem.join(std.testing.allocator, ",", cells);
    defer std.testing.allocator.free(joined);
    const array_text = try std.fmt.allocPrint(std.testing.allocator, "{{{s}}}", .{joined});
    defer std.testing.allocator.free(array_text);
    for ([_][]const u8{
        "SELECT v FROM unnest($1::text[]) t(v) LIMIT 1",
        "SELECT v FROM things d CROSS JOIN unnest($1::text[]) t(v) LIMIT 1",
        "SELECT q.v FROM things d CROSS JOIN LATERAL (SELECT v FROM unnest($1::text[]) t(v)) q LIMIT 1",
        "SELECT q.v FROM things d CROSS JOIN LATERAL (SELECT v FROM unnest($1::text[]) t(v) WHERE v = 'x') q LIMIT 1",
    }) |sql| {
        var backend: TestBackend = .{ .row_count = 3 };
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var result = try execute(std.testing.allocator, backend.coordinated(), &compiled, &.{.{ .string = array_text }}, .{ .scan_rows = 1 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
        try std.testing.expectEqualStrings("x", result.output.rows[0][0].string);
    }
    {
        var backend: TestBackend = .{};
        var compiled = try compiler.compile(std.testing.allocator, "SELECT count(*) FROM unnest(ARRAY[1,2,3])", .{});
        defer compiled.deinit();
        try std.testing.expectError(error.SqlProgramLimitExceeded, execute(std.testing.allocator, backend.coordinated(), &compiled, &.{}, .{ .scan_rows = 2 }));
    }
    const Check = struct {
        fn run(a: std.mem.Allocator, compiled: *const compiler.Compiled) !void {
            var backend: TestBackend = .{ .row_count = 3 };
            defer std.debug.assert(backend.statement_opens == backend.statement_closes);
            var result = try execute(a, backend.coordinated(), compiled, &.{}, .{ .page_rows = 1 });
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 3), result.output.rows.len);
        }
    };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT d._id, t.v, t.n FROM things d LEFT JOIN unnest(ARRAY[d._id, NULL]) WITH ORDINALITY t(v,n) ON true ORDER BY d._id, t.n LIMIT 3", .{});
    defer compiled.deinit();
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Check.run, .{&compiled});
}

test "SQL captured prefix replay defers later errors without reopening physical storage" {
    const sql = "SELECT p.n, q.v FROM (VALUES (1)) p(n) CROSS JOIN LATERAL (SELECT 10 / (1-id) AS v FROM things) q LIMIT 1";
    var compiled = try compiler.compile(std.testing.allocator, sql, .{});
    defer compiled.deinit();
    var backend: TestBackend = .{ .row_count = 4096, .group_every = 1 };
    var result = try execute(std.testing.allocator, backend.coordinated(), &compiled, &.{}, .{ .page_rows = 16, .scan_pages = 1 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
    try std.testing.expectEqualStrings("10", result.output.rows[0][1].string);
    try std.testing.expectEqual(@as(usize, 1), backend.pages);
    try std.testing.expectEqual(@as(usize, 1), backend.statement_opens);
    try std.testing.expectEqual(backend.statement_opens, backend.statement_closes);

    backend = .{ .row_count = 4096, .group_every = 1 };
    var second = try compiler.compile(std.testing.allocator, "SELECT p.n, q.v FROM (VALUES (1)) p(n) CROSS JOIN LATERAL (SELECT 10 / (1-id) AS v FROM things) q LIMIT 2", .{});
    defer second.deinit();
    try std.testing.expectError(error.SqlDivisionByZero, execute(std.testing.allocator, backend.coordinated(), &second, &.{}, .{ .page_rows = 16, .scan_pages = 1 }));
    try std.testing.expectEqual(@as(usize, 1), backend.pages);
    try std.testing.expectEqual(backend.statement_opens, backend.statement_closes);
}

test "SQL captured prefix replay shares one captured iterator across lateral consumers and allocation faults" {
    const Check = struct {
        fn run(a: std.mem.Allocator, compiled: *const compiler.Compiled) !void {
            var backend: TestBackend = .{ .row_count = 2, .group_every = 1 };
            defer std.debug.assert(backend.statement_opens == backend.statement_closes);
            var result = try execute(a, backend.coordinated(), compiled, &.{}, .{ .page_rows = 16 });
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 4), result.output.rows.len);
            for (result.output.rows, [_][]const u8{ "1", "1", "2", "2" }, [_][]const u8{ "0", "1", "0", "1" }) |row, parent, value| {
                try std.testing.expectEqualStrings(parent, row[0].string);
                try std.testing.expectEqualStrings(value, row[1].string);
            }
            try std.testing.expectEqual(@as(usize, 1), backend.pages);
            try std.testing.expectEqual(@as(usize, 1), backend.statement_opens);
        }
    };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT p.n, q.v FROM (VALUES (1),(2)) p(n) CROSS JOIN LATERAL (SELECT id AS v FROM things) q", .{});
    defer compiled.deinit();
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Check.run, .{&compiled});
}

test "SQL expansion replay policy distinguishes restartable wrappers from capture and volatility" {
    for ([_]struct { sql: []const u8, expected: @import("relation_binding.zig").Node.ReplayPolicy }{
        .{ .sql = "SELECT q.v FROM things d CROSS JOIN LATERAL (SELECT v FROM unnest(ARRAY[1,2]) t(v)) q", .expected = .streaming },
        .{ .sql = "SELECT q.v FROM things d CROSS JOIN LATERAL (SELECT v FROM things s CROSS JOIN unnest(ARRAY[1,2]) t(v)) q", .expected = .captured },
        .{ .sql = "SELECT q.v FROM things d CROSS JOIN LATERAL (SELECT ai_probability(v, 'Refund?', 'local') AS v FROM unnest(ARRAY['input']) t(v)) q", .expected = .captured },
        .{ .sql = "SELECT q.v FROM things d CROSS JOIN LATERAL unnest(ARRAY[ai_probability('input', 'Refund?', 'local')]) q(v)", .expected = .captured },
    }) |case| {
        var backend: TestBackend = .{};
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        var parameters: [0]?ast.ColumnType = .{};
        const bound = try @import("relation_binding.zig").bind(arena.allocator(), backend.coordinated(), compiled.statement.select, &parameters);
        try std.testing.expect(bound.root.operation == .apply);
        try std.testing.expectEqual(case.expected, bound.root.operation.apply.right.replay_policy);
        try std.testing.expectEqual(@import("relation_binding.zig").Node.ReplayPolicy.captured, bound.root.operation.apply.left.replay_policy);
        try std.testing.expectEqual(@as(usize, 0), backend.statement_opens);
    }
}

test "SQL expansion demand preserves residual filtering and errors beyond delivered prefixes" {
    for ([_]struct { sql: []const u8, expected: []const u8, rows: usize, native_rows: usize }{
        .{ .sql = "SELECT q.v FROM things d CROSS JOIN LATERAL (SELECT 10 / v AS v FROM unnest(ARRAY[2,0]) t(v)) q LIMIT 1", .expected = "5", .rows = 1, .native_rows = 1 },
        .{ .sql = "SELECT q.v FROM things d CROSS JOIN LATERAL (SELECT v FROM unnest(ARRAY[0,1]) t(v) WHERE v = 1) q LIMIT 1", .expected = "1", .rows = 2, .native_rows = 3 },
    }) |case| {
        var backend: TestBackend = .{ .row_count = case.native_rows };
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var result = try execute(std.testing.allocator, backend.coordinated(), &compiled, &.{}, .{ .scan_rows = case.rows, .page_rows = 64 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
        try std.testing.expectEqualStrings(case.expected, result.output.rows[0][0].string);
        try std.testing.expectEqual(@as(usize, 1), backend.statement_opens);
        try std.testing.expectEqual(@as(usize, 1), backend.statement_closes);
    }
    // One parent makes requesting the zero-divisor row deterministic even
    // when PostgreSQL changes join orientation for an unordered LIMIT.
    var backend: TestBackend = .{ .row_count = 1 };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT q.v FROM things d CROSS JOIN LATERAL (SELECT 10 / v AS v FROM unnest(ARRAY[2,0]) t(v)) q LIMIT 2", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.SqlDivisionByZero, execute(std.testing.allocator, backend.coordinated(), &compiled, &.{}, .{ .page_rows = 64 }));
    try std.testing.expectEqual(backend.statement_opens, backend.statement_closes);
}

test "SQL USING retains composite hash extraction and bounded fanout allocations" {
    {
        var backend: TestBackend = .{};
        var compiled = try compiler.compile(std.testing.allocator, "SELECT k FROM (SELECT id AS k, id AS z FROM things) a JOIN (SELECT id AS k, id AS z FROM things) b USING(z, k)", .{});
        defer compiled.deinit();
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        var parameters: [0]?ast.ColumnType = .{};
        const bound = try @import("relation_binding.zig").bind(arena.allocator(), backend.coordinated(), compiled.statement.select, &parameters);
        try std.testing.expect(bound.root.operation == .query);
        const join = bound.root.operation.query.source.operation.join;
        try std.testing.expectEqual(@as(usize, 2), join.left_keys.len);
        try std.testing.expectEqual(@as(usize, 2), join.right_keys.len);
    }
    var allocations: [2]usize = undefined;
    for ([_][]const u8{
        "SELECT count(*) FROM things a JOIN things b ON a.id = b.id",
        "SELECT count(*) FROM things a JOIN things b USING(id)",
    }, &allocations) |sql, *count| {
        var backend: TestBackend = .{ .row_count = 256 };
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var counting = std.testing.FailingAllocator.init(std.testing.allocator, .{});
        var result = try execute(counting.allocator(), backend.coordinated(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqualStrings("65536", result.output.rows[0][0].string);
        count.* = counting.allocations;
    }
    // The extra projection is batched, not a per-pair arena allocation.
    try std.testing.expect(allocations[1] < 4096);
    try std.testing.expect(allocations[1] <= allocations[0] * 2 + 512);
}

test "SQL relation cursors close on early limit quota failure and every allocation failure" {
    const Check = struct {
        fn run(alloc: std.mem.Allocator, compiled: *const compiler.Compiled) !void {
            var backend: TestBackend = .{ .row_count = 3 };
            defer std.debug.assert(backend.statement_opens == backend.statement_closes);
            var result = try execute(alloc, backend.coordinated(), compiled, &.{}, .{ .page_rows = 1 });
            defer result.deinit();
        }
    };
    var compiled = try compiler.compile(std.testing.allocator, "WITH q AS (SELECT _id FROM things) SELECT a._id, b._id FROM q a FULL JOIN q b ON a._id = b._id LIMIT 1", .{});
    defer compiled.deinit();
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Check.run, .{&compiled});
    var backend: TestBackend = .{ .row_count = 3 };
    try std.testing.expectError(error.SqlProgramLimitExceeded, execute(std.testing.allocator, backend.coordinated(), &compiled, &.{}, .{ .page_rows = 1, .scan_rows = 2 }));
    try std.testing.expectEqual(@as(usize, 1), backend.statement_opens);
    try std.testing.expectEqual(backend.statement_opens, backend.statement_closes);
}

test "SQL row aggregate and mutation predicates reuse statement regex resources" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "SELECT count(*) FROM things WHERE regexp_like(_id, '[0-9]')",
        "UPDATE things SET id = id WHERE regexp_like(_id, '[0-9]')",
        "DELETE FROM things WHERE regexp_like(_id, '[0-9]')",
    }, 0..) |sql, index| {
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        // Row-only provider: do not accidentally exercise a columnar path.
        var backend: TestBackend = .{ .row_count = 1000 };
        var result = try execute(a, backend.iface(), &compiled, &.{}, .{ .page_rows = 37 });
        defer result.deinit();
        if (index == 0) {
            try std.testing.expectEqualStrings("1000", result.output.rows[0][0].string);
            try std.testing.expectEqual(@as(usize, 0), backend.writes);
        } else {
            try std.testing.expectEqual(@as(u64, 1000), result.output.rows_affected);
            try std.testing.expectEqual(@as(usize, 1), backend.writes);
        }
        const stats = result.state.regex_execution.snapshot();
        try std.testing.expectEqual(@as(usize, 1), stats.lanes);
        try std.testing.expectEqual(@as(u64, 1), stats.compilations);
        try std.testing.expectEqual(@as(u64, 999), stats.hits);
        try std.testing.expectEqual(@as(usize, 0), stats.active);
    }
}

test "SQL row predicate native cancellation prevents mutation and releases resources" {
    const Control = struct {
        fixture: *TestBackend,
        calls: usize = 0,
        fail: bool = true,
        fn checkpoint(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            // Let a complete page warm the cache before failing a later page.
            if (self.fail and self.fixture.pages >= 2) return error.Canceled;
        }
    };
    const a = std.testing.allocator;
    for ([_][]const u8{
        "SELECT count(*) FROM things WHERE regexp_like(_id, '[0-9]')",
        "UPDATE things SET id = id WHERE regexp_like(_id, '[0-9]')",
        "DELETE FROM things WHERE regexp_like(_id, '[0-9]')",
    }) |sql| {
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        var fixture: TestBackend = .{ .row_count = 100 };
        var control: Control = .{ .fixture = &fixture };
        var backend = fixture.iface();
        // Scan checkpoints succeed; only a native regex poll can cancel.
        backend.scalar_control = .{ .ptr = &control, .checkpoint = Control.checkpoint };
        try std.testing.expectError(error.Canceled, execute(a, backend, &compiled, &.{}, .{ .page_rows = 13 }));
        try std.testing.expect(control.calls > 0);
        try std.testing.expectEqual(@as(usize, 2), fixture.pages);
        try std.testing.expectEqual(@as(usize, 0), fixture.writes);
        control.fail = false;
        var retried = try execute(a, backend, &compiled, &.{}, .{ .page_rows = 13 });
        defer retried.deinit();
        const stats = retried.state.regex_execution.snapshot();
        try std.testing.expectEqual(@as(u64, 1), stats.compilations);
        try std.testing.expectEqual(@as(u64, 99), stats.hits);
        try std.testing.expectEqual(@as(usize, 0), stats.active);
    }
}

test "SQL row predicate ownership unwinds every allocation failure" {
    const Harness = struct {
        fn run(a: std.mem.Allocator, compiled: *const compiler.Compiled) !void {
            var backend: TestBackend = .{ .row_count = 8 };
            var result = try execute(a, backend.iface(), compiled, &.{}, .{ .page_rows = 3 });
            defer result.deinit();
            const stats = result.state.regex_execution.snapshot();
            try std.testing.expectEqual(@as(u64, 1), stats.compilations);
            try std.testing.expectEqual(@as(u64, 7), stats.hits);
            try std.testing.expectEqual(@as(usize, 0), stats.active);
        }
    };
    for ([_][]const u8{
        "SELECT count(*) FROM things WHERE regexp_like(_id, '[0-9]')",
        "UPDATE things SET id = id WHERE regexp_like(_id, '[0-9]')",
        "DELETE FROM things WHERE regexp_like(_id, '[0-9]')",
    }) |sql| {
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{&compiled});
    }
}

test "SQL empty global aggregates produce one row and grouped empty input produces none" {
    var backend: TestBackend = .{ .row_count = 0 };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT count(*), sum(id), avg(id), min(id), bool_and(id > 0) FROM things", .{});
    defer compiled.deinit();
    var result = try execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
    try std.testing.expectEqualStrings("0", result.output.rows[0][0].string);
    try std.testing.expectEqualSlices(bool, &.{ false, true, true, true, true }, result.output.sql_nulls.?[0]);
    var grouped = try compiler.compile(std.testing.allocator, "SELECT id, count(*) FROM things GROUP BY id", .{});
    defer grouped.deinit();
    var empty = try execute(std.testing.allocator, backend.iface(), &grouped, &.{}, .{});
    defer empty.deinit();
    try std.testing.expectEqual(@as(usize, 0), empty.output.rows.len);
}

test "SQL tableless aggregate and HAVING preserve SQL null semantics" {
    var backend: TestBackend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT count(*), sum(2), avg(2), max('null'::json) WHERE FALSE", .{});
    defer compiled.deinit();
    var result = try execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 0), backend.pages);
    try std.testing.expectEqualStrings("0", result.output.rows[0][0].string);
    try std.testing.expectEqualSlices(bool, &.{ false, true, true, true }, result.output.sql_nulls.?[0]);
    var accepted = try compiler.compile(std.testing.allocator, "SELECT count(*), max('null'::json) HAVING count(*) = 1", .{});
    defer accepted.deinit();
    var present = try execute(std.testing.allocator, backend.iface(), &accepted, &.{}, .{});
    defer present.deinit();
    try std.testing.expectEqualSlices(bool, &.{ false, false }, present.output.sql_nulls.?[0]);
    try std.testing.expect(present.output.rows[0][1] == .null);
}

test "SQL aggregate parameter inference and allocation failures are statement wide" {
    const Harness = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var backend: TestBackend = .{ .row_count = 3 };
            var compiled = try compiler.compile(alloc, "SELECT $1 + 1 AS shifted, max($1) AS largest FROM things HAVING count(*) > 0", .{});
            defer compiled.deinit();
            var result = try execute(alloc, backend.iface(), &compiled, &.{.{ .integer = 4 }}, .{ .page_rows = 1 });
            defer result.deinit();
            try std.testing.expectEqualStrings("5", result.output.rows[0][0].string);
            try std.testing.expectEqualStrings("4", result.output.rows[0][1].string);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

test "SQL DISTINCT aggregates use semantic values and FILTER skips unused expressions" {
    var backend: TestBackend = .{ .row_count = 6 };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT count(DISTINCT id), sum(DISTINCT _id::bigint % 2), count(*) FILTER (WHERE _id::bigint % 2 = 0), sum(1 / (_id::bigint - _id::bigint)) FILTER (WHERE FALSE) FROM things", .{});
    defer compiled.deinit();
    var result = try execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{ .page_rows = 1 });
    defer result.deinit();
    try std.testing.expectEqualStrings("1", result.output.rows[0][0].string);
    try std.testing.expectEqualStrings("1", result.output.rows[0][1].string);
    try std.testing.expectEqualStrings("3", result.output.rows[0][2].string);
    try std.testing.expect(result.output.sql_nulls.?[0][3]);
}

test "SQL aggregate conjuncts keep native point seek and contradictory empty optimization" {
    var backend: TestBackend = .{ .row_count = 20 };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT sum(id), count(*) FROM things WHERE _id = '2'", .{});
    defer compiled.deinit();
    var result = try execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), backend.point_reads);
    try std.testing.expectEqualStrings("1", result.output.rows[0][1].string);
    var empty = try compiler.compile(std.testing.allocator, "SELECT sum(id), count(*) FROM things WHERE _id = '2' AND _id = '3'", .{});
    defer empty.deinit();
    var empty_result = try execute(std.testing.allocator, backend.iface(), &empty, &.{}, .{});
    defer empty_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), backend.pages);
    try std.testing.expectEqualStrings("0", empty_result.output.rows[0][1].string);
}

test "SQL executor pages projection and preserves exact integers" {
    var backend: TestBackend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT id AS exact, id FROM things", .{});
    defer compiled.deinit();
    var result = try execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{ .page_rows = 1 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), backend.pages);
    try std.testing.expectEqual(@as(usize, 2), result.output.rows.len);
    try std.testing.expectEqualStrings("exact", result.output.columns[0].name);
    try std.testing.expectEqualStrings("9007199254740993", result.output.rows[0][0].string);
}

test "SQL ordered scans carry required primary order to the access path" {
    var backend: TestBackend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT _id FROM things ORDER BY _id LIMIT 1", .{});
    defer compiled.deinit();
    var result = try execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expect(backend.primary_order);
    try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
}

test "SQL result admission is not an implicit LIMIT" {
    var backend: TestBackend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT id FROM things", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.SqlResultTooLarge, execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{ .result_rows = 1, .page_rows = 1 }));
    var limited = try compiler.compile(std.testing.allocator, "SELECT id FROM things LIMIT 1", .{});
    defer limited.deinit();
    var result = try execute(std.testing.allocator, backend.iface(), &limited, &.{}, .{ .result_rows = 1 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
}

test "SQL mutation overflow never commits a partial write set" {
    var backend: TestBackend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "DELETE FROM things", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.SqlProgramLimitExceeded, execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{ .mutation_rows = 1, .page_rows = 1 }));
    try std.testing.expectEqual(@as(usize, 0), backend.writes);
}

test "SQL mutations commit once with exact versions and never retry ambiguity" {
    var backend: TestBackend = .{ .ambiguous = true };
    var compiled = try compiler.compile(std.testing.allocator, "UPDATE things SET id = $1", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.AmbiguousCommit, execute(std.testing.allocator, backend.iface(), &compiled, &.{.{ .string = "9007199254740993" }}, .{ .page_rows = 1 }));
    try std.testing.expectEqual(@as(usize, 1), backend.writes);
}

test "SQL cancellation precedes mutation and disjunction applies a typed residual" {
    var backend: TestBackend = .{ .cancelled = true };
    var compiled = try compiler.compile(std.testing.allocator, "DELETE FROM things WHERE id = 1 OR id = 2", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.Cancelled, execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{}));
    backend.cancelled = false;
    var result = try execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(u64, 0), result.output.rows_affected);
    try std.testing.expectEqual(@as(usize, 1), backend.pages);
    try std.testing.expectEqual(@as(usize, 0), backend.writes);
}

test "SQL expression projection and residual filtering precede offset limit and count" {
    var backend: TestBackend = .{ .row_count = 4 };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT id + 1 AS next, upper(_id) AS key FROM things WHERE _id = '1' OR _id = '3' LIMIT 1 OFFSET 1", .{});
    defer compiled.deinit();
    var result = try execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{ .page_rows = 1 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
    try std.testing.expectEqualStrings("9007199254740994", result.output.rows[0][0].string);
    try std.testing.expectEqualStrings("3", result.output.rows[0][1].string);
    try std.testing.expectEqual(@as(usize, 4), backend.pages);
    try std.testing.expectEqualSlices(bool, &.{ false, false }, result.output.sql_nulls.?[0]);

    var counted = try compiler.compile(std.testing.allocator, "SELECT count(*) FROM things WHERE NOT (_id = '0' OR _id = '2')", .{});
    defer counted.deinit();
    var count_result = try execute(std.testing.allocator, backend.iface(), &counted, &.{}, .{ .page_rows = 1 });
    defer count_result.deinit();
    try std.testing.expectEqualStrings("2", count_result.output.rows[0][0].string);
}

test "SQL projected JSON null remains distinct from SQL NULL" {
    var backend: TestBackend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT CAST('null' AS JSON) AS j, NULL AS n FROM things LIMIT 1", .{});
    defer compiled.deinit();
    var result = try execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expect(result.output.rows[0][0] == .null);
    try std.testing.expect(result.output.rows[0][1] == .null);
    try std.testing.expectEqualSlices(bool, &.{ false, true }, result.output.sql_nulls.?[0]);
}

test "SQL tableless SELECT uses one logical row without catalog or storage access" {
    const NoLookup = struct {
        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: ast.Name, _: catalog.Action) !catalog.Table {
            return error.UnexpectedCatalogLookup;
        }
    };
    const cases = [_]struct { sql: []const u8, expected: ?[]const u8 }{
        .{ .sql = "SELECT 2 + 3 AS sum", .expected = "5" },
        .{ .sql = "SELECT count(*) WHERE FALSE", .expected = "0" },
        .{ .sql = "SELECT count(*) WHERE TRUE", .expected = "1" },
        .{ .sql = "SELECT 1 WHERE FALSE", .expected = null },
        .{ .sql = "SELECT 1 LIMIT 0", .expected = null },
        .{ .sql = "SELECT 1 OFFSET 1", .expected = null },
    };
    var backend: TestBackend = .{};
    var iface = backend.iface();
    var vtable = iface.vtable.*;
    vtable.resolve = NoLookup.resolve;
    iface.vtable = &vtable;
    for (cases) |case| {
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var result = try execute(std.testing.allocator, iface, &compiled, &.{}, .{});
        defer result.deinit();
        if (case.expected) |expected| {
            try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
            try std.testing.expectEqualStrings(expected, result.output.rows[0][0].string);
        } else try std.testing.expectEqual(@as(usize, 0), result.output.rows.len);
    }
    try std.testing.expectEqual(@as(usize, 0), backend.pages);
    var undefined_column = try compiler.compile(std.testing.allocator, "SELECT 1 WHERE missing = 2", .{});
    defer undefined_column.deinit();
    try std.testing.expectError(error.UndefinedColumn, execute(std.testing.allocator, iface, &undefined_column, &.{}, .{}));
}

test "SQL array scalar contracts execute without catalog or storage access" {
    const NoLookup = struct {
        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: ast.Name, _: catalog.Action) !catalog.Table {
            return error.UnexpectedCatalogLookup;
        }
    };
    const Entry = struct { sql: []const u8, value: Json };
    const fixture = try std.json.parseFromSlice(struct { reference: []const u8, entries: []const Entry }, std.testing.allocator, @embedFile("fixtures/sql_array_expression_reference.json"), .{});
    defer fixture.deinit();
    var backend: TestBackend = .{};
    var iface = backend.iface();
    var vtable = iface.vtable.*;
    vtable.resolve = NoLookup.resolve;
    iface.vtable = &vtable;
    for (fixture.value.entries) |entry| {
        const sql = try std.fmt.allocPrint(std.testing.allocator, "SELECT {s} AS value", .{entry.sql});
        defer std.testing.allocator.free(sql);
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var result = try execute(std.testing.allocator, iface, &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
        try std.testing.expectEqual(entry.value == .null, result.output.sql_nulls.?[0][0]);
        const actual = result.output.rows[0][0];
        switch (entry.value) {
            .integer => |value| try std.testing.expectEqual(value, try std.fmt.parseInt(i64, actual.string, 10)),
            .bool => |value| try std.testing.expectEqual(value, actual.bool),
            .null => try std.testing.expect(actual == .null),
            else => return error.InvalidArrayScalarFixture,
        }
    }
    var array_query = try compiler.compile(std.testing.allocator, "SELECT ARRAY[1, NULL]", .{});
    defer array_query.deinit();
    var array_result = try execute(std.testing.allocator, iface, &array_query, &.{}, .{});
    defer array_result.deinit();
    try std.testing.expectEqual(ast.ColumnType.array, array_result.output.columns[0].type);
    try std.testing.expectEqual(@import("array_value.zig").ElementType.int32, array_result.output.columns[0].element_type.?);
    const envelope = array_result.output.rows[0][0];
    try std.testing.expectEqualStrings("1", envelope.object.get("values").?.array.items[0].string);
    try std.testing.expect(envelope.object.get("sql_nulls").?.array.items[1].bool);
    try std.testing.expectEqual(@as(usize, 0), backend.pages);
    try std.testing.expectEqual(@as(usize, 0), backend.writes);
}

test "SQL expression errors never publish a partially prepared mutation" {
    var backend: TestBackend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "UPDATE things SET id = id / CAST(_id AS INTEGER)", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.SqlDivisionByZero, execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 0), backend.writes);
}

test "SQL statement never crosses fresh native page snapshots" {
    var backend: TestBackend = .{};
    var iface = backend.iface();
    iface.pinned_statement_snapshot = false;
    var compiled = try compiler.compile(std.testing.allocator, "DELETE FROM things", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.SqlStatementSnapshotRequired, execute(std.testing.allocator, iface, &compiled, &.{}, .{ .page_rows = 1 }));
    try std.testing.expectEqual(@as(usize, 0), backend.writes);
    try std.testing.expectEqual(@as(usize, 1), backend.pages);
}

test "SQL retained cursors close on completion early limit cancellation and write overflow" {
    const Provider = struct {
        const Self = @This();
        base: TestBackend = .{},
        opens: usize = 0,
        closes: usize = 0,
        cancel_after_page: bool = false,

        const Read = struct {
            alloc: std.mem.Allocator,
            provider: *Self,
            snapshot: TestBackend,
            table: catalog.Table,
            request: catalog.Scan,
            offset: usize = 0,

            fn next(ptr: *anyopaque, alloc: std.mem.Allocator, limit: u32) !catalog.Page {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                var request = self.request;
                request.limit = limit;
                request.after = try std.fmt.allocPrint(alloc, "{d}", .{self.offset});
                const page = try TestBackend.scan(&self.snapshot, alloc, self.table, request);
                self.offset += page.rows.len;
                // Concurrent writes cannot extend a retained statement view.
                self.provider.base.row_count += 10;
                if (self.provider.cancel_after_page) self.provider.base.cancelled = true;
                return page;
            }

            fn close(ptr: *anyopaque) void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                self.provider.closes += 1;
                self.alloc.destroy(self);
            }
        };

        fn open(ptr: *anyopaque, alloc: std.mem.Allocator, table: catalog.Table, request: catalog.Scan) !?catalog.Cursor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const read = try alloc.create(Read);
            read.* = .{ .alloc = alloc, .provider = self, .snapshot = self.base, .table = table, .request = request };
            self.opens += 1;
            return .{ .ptr = read, .next = Read.next, .close = Read.close };
        }

        fn checkpoint(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try TestBackend.checkpoint(&self.base);
        }

        fn scan(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
            return error.TestUnexpectedResult;
        }

        fn mutate(ptr: *anyopaque, alloc: std.mem.Allocator, _: std.mem.Allocator, table: catalog.Table, mutations: []const catalog.Mutation) !catalog.MutationOutcome {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(self.opens, self.closes);
            return TestBackend.mutate(&self.base, alloc, alloc, table, mutations);
        }

        fn iface(self: *@This()) catalog.Backend {
            return .{ .ptr = self, .vtable = &.{ .resolve = TestBackend.resolve, .scan = scan, .open_scan = open, .mutate = mutate, .checkpoint = checkpoint } };
        }
    };
    var count = try compiler.compile(std.testing.allocator, "SELECT count(*) FROM things", .{});
    defer count.deinit();
    var provider: Provider = .{};
    var result = try execute(std.testing.allocator, provider.iface(), &count, &.{}, .{ .page_rows = 1 });
    defer result.deinit();
    try std.testing.expectEqualStrings("2", result.output.rows[0][0].string);
    try std.testing.expectEqual(@as(usize, 1), provider.opens);
    try std.testing.expectEqual(provider.opens, provider.closes);

    var limited = try compiler.compile(std.testing.allocator, "SELECT id FROM things LIMIT 1", .{});
    defer limited.deinit();
    var small = try execute(std.testing.allocator, provider.iface(), &limited, &.{}, .{ .page_rows = 1 });
    defer small.deinit();
    try std.testing.expectEqual(@as(usize, 2), provider.closes);

    var deletion = try compiler.compile(std.testing.allocator, "DELETE FROM things", .{});
    defer deletion.deinit();
    try std.testing.expectError(error.SqlProgramLimitExceeded, execute(std.testing.allocator, provider.iface(), &deletion, &.{}, .{ .page_rows = 1, .mutation_rows = 1 }));
    try std.testing.expectEqual(@as(usize, 0), provider.base.writes);
    try std.testing.expectEqual(@as(usize, 3), provider.closes);

    provider.base.row_count = 2;
    var committed = try execute(std.testing.allocator, provider.iface(), &deletion, &.{}, .{ .page_rows = 1 });
    defer committed.deinit();
    try std.testing.expectEqual(@as(usize, 1), provider.base.writes);
    try std.testing.expectEqual(@as(usize, 4), provider.closes);

    provider.cancel_after_page = true;
    try std.testing.expectError(error.Cancelled, execute(std.testing.allocator, provider.iface(), &count, &.{}, .{ .page_rows = 1 }));
    try std.testing.expectEqual(@as(usize, 5), provider.closes);
}

test "SQL memory quota reports shared parent admission as a working limit" {
    var backend: TestBackend = .{ .row_count = 4096 };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT id FROM things", .{});
    defer compiled.deinit();
    var parent: MemoryBudget = .{ .backing = std.testing.allocator, .limit = 64 * 1024 };
    try std.testing.expectError(error.SqlWorkingMemoryLimitExceeded, execute(parent.allocator(), backend.iface(), &compiled, &.{}, .{ .retained_bytes = 4 * 1024 * 1024 }));
    try std.testing.expect(parent.isExhausted());
    try std.testing.expectEqual(@as(usize, 0), parent.live);
}

test "SQL memory quota rejects allocations before backend work" {
    var backend: TestBackend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT id FROM things", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.SqlWorkingMemoryLimitExceeded, execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{ .retained_bytes = 1 }));
    try std.testing.expectEqual(@as(usize, 0), backend.pages);
}

test "SQL page budget bounds progressing but empty backend pages" {
    const Empty = struct {
        pages: usize = 0,
        fn scan(ptr: *anyopaque, alloc: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.pages += 1;
            return .{ .rows = &.{}, .after = try std.fmt.allocPrint(alloc, "{d}", .{self.pages}) };
        }
        fn checkpoint(_: *anyopaque) !void {}
    };
    var fake: Empty = .{};
    const backend: catalog.Backend = .{ .ptr = &fake, .pinned_statement_snapshot = true, .vtable = &.{ .resolve = TestBackend.resolve, .mutate = TestBackend.mutate, .scan = Empty.scan, .checkpoint = Empty.checkpoint } };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT count(*) FROM things", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.SqlProgramLimitExceeded, execute(std.testing.allocator, backend, &compiled, &.{}, .{ .scan_pages = 3 }));
    try std.testing.expectEqual(@as(usize, 3), fake.pages);
}

test "SQL quoted dotted columns remain literal properties" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var object: std.json.ObjectMap = .empty;
    try putField(arena.allocator(), &object, "a.b", .{ .integer = 7 });
    try std.testing.expectEqual(@as(i64, 7), fieldValue(.{ .object = object }, "a.b").integer);
    try std.testing.expect(object.get("a") == null);
}

test "SQL executor frees every partial allocation on failure" {
    const Check = struct {
        fn run(alloc: std.mem.Allocator, compiled: *const compiler.Compiled) !void {
            var backend: TestBackend = .{};
            var result = try execute(alloc, backend.iface(), compiled, &.{}, .{ .page_rows = 1 });
            defer result.deinit();
        }
    };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT id FROM things", .{});
    defer compiled.deinit();
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Check.run, .{&compiled});
}

test "SQL bounded JSON copy rejects nesting before exhausting the stack" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var root: Json = .null;
    for (0..70) |_| {
        var array: std.json.Array = .init(arena.allocator());
        try array.append(root);
        root = .{ .array = array };
    }
    try std.testing.expectError(error.SqlProgramLimitExceeded, clone(arena.allocator(), root));
}

test "SQL count releases pages instead of retaining the scanned relation" {
    var backend: TestBackend = .{ .row_count = 10_000 };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT count(*) FROM things", .{});
    defer compiled.deinit();
    var result = try execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{ .retained_bytes = 512 * 1024 });
    defer result.deinit();
    try std.testing.expectEqualStrings("10000", result.output.rows[0][0].string);
    try std.testing.expectEqual(@as(usize, 40), backend.pages);
    try std.testing.expect(result.peakMemoryBytes() <= 512 * 1024);
}

test "SQL ORDER BY respects output aliases instead of silently using physical keys" {
    var backend: TestBackend = .{};
    var shadowed = try compiler.compile(std.testing.allocator, "SELECT id AS _id FROM things ORDER BY _id", .{});
    defer shadowed.deinit();
    var sorted = try execute(std.testing.allocator, backend.iface(), &shadowed, &.{}, .{});
    defer sorted.deinit();
    try std.testing.expectEqual(@as(usize, 2), sorted.output.rows.len);
    try std.testing.expect(!backend.primary_order);
    var aggregate = try compiler.compile(std.testing.allocator, "SELECT count(*) FROM things ORDER BY _id", .{});
    defer aggregate.deinit();
    try std.testing.expectError(error.SqlGroupingError, execute(std.testing.allocator, backend.iface(), &aggregate, &.{}, .{}));
    var renamed = try compiler.compile(std.testing.allocator, "SELECT _id AS key FROM things ORDER BY key", .{});
    defer renamed.deinit();
    var result = try execute(std.testing.allocator, backend.iface(), &renamed, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqualStrings("0", result.output.rows[0][0].string);
}

test "SQL bounded top K sorts typed expressions before offset and limits retained rows" {
    var backend: TestBackend = .{ .row_count = 100 };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT CAST(_id AS INTEGER) AS n FROM things ORDER BY n DESC LIMIT 3 OFFSET 2", .{});
    defer compiled.deinit();
    var result = try execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{ .page_rows = 7, .retained_bytes = 256 * 1024 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 3), result.output.rows.len);
    try std.testing.expectEqualStrings("97", result.output.rows[0][0].string);
    try std.testing.expectEqualStrings("96", result.output.rows[1][0].string);
    try std.testing.expectEqualStrings("95", result.output.rows[2][0].string);
    try std.testing.expectEqual(@as(usize, 15), backend.pages);
    try std.testing.expect(result.peakMemoryBytes() <= 256 * 1024);
    try std.testing.expect(!backend.primary_order);
}

test "SQL ordering handles expressions positions and explicit null placement" {
    var backend: TestBackend = .{ .row_count = 4 };
    const statements = [_][]const u8{
        "SELECT CASE WHEN _id = '0' THEN NULL ELSE CAST(_id AS INTEGER) END AS n FROM things ORDER BY 1 DESC NULLS LAST",
        "SELECT CASE WHEN _id = '0' THEN NULL ELSE CAST(_id AS INTEGER) END AS n FROM things ORDER BY CASE WHEN _id = '0' THEN NULL ELSE CAST(_id AS INTEGER) END DESC NULLS LAST",
    };
    for (statements) |statement| {
        var compiled = try compiler.compile(std.testing.allocator, statement, .{});
        defer compiled.deinit();
        var result = try execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{ .page_rows = 1 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 4), result.output.rows.len);
        try std.testing.expectEqualStrings("3", result.output.rows[0][0].string);
        try std.testing.expectEqualStrings("1", result.output.rows[2][0].string);
        try std.testing.expect(result.output.rows[3][0] == .null);
        try std.testing.expect(result.output.sql_nulls.?[3][0]);
    }
}

test "SQL shared invocation arrays cross derived set grouped window and recursive plans" {
    const cases = [_]struct { sql: []const u8, rows: usize = 1 }{
        .{ .sql = "SELECT cardinality($1::bigint[]),array_lower($1,1)" },
        .{ .sql = "SELECT cardinality(q.a),array_lower(q.a,1) FROM (SELECT $1::bigint[] a) q" },
        .{ .sql = "WITH q AS MATERIALIZED (SELECT $1::bigint[] a) SELECT cardinality(l.a),array_lower(r.a,1) FROM q l CROSS JOIN q r" },
        .{ .sql = "SELECT max(cardinality($1::bigint[])),array_lower($1,1) FROM things" },
        .{ .sql = "SELECT cardinality(a),array_lower(a,1) FROM (SELECT $1::bigint[] a UNION SELECT $1::bigint[]) q" },
        .{ .sql = "SELECT cardinality($1::bigint[]),array_lower($1,1) FROM things ORDER BY _id", .rows = 2 },
    };
    for (cases) |case| {
        var fixture: TestBackend = .{};
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var result = try execute(std.testing.allocator, fixture.iface(), &compiled, &.{.{ .string = "[-1:1]={9007199254740993,NULL,2}" }}, .{ .page_rows = 1 });
        defer result.deinit();
        try std.testing.expectEqual(case.rows, result.output.rows.len);
        for (result.output.rows) |row| {
            try std.testing.expectEqualStrings("3", row[0].string);
            try std.testing.expectEqualStrings("-1", row[1].string);
        }
    }
    var fixture: TestBackend = .{};
    var window = try compiler.compile(std.testing.allocator, "SELECT lag($1::bigint[]) OVER (ORDER BY _id) a FROM things", .{});
    defer window.deinit();
    var result = try execute(std.testing.allocator, fixture.iface(), &window, &.{.{ .string = "[-1:1]={9007199254740993,NULL,2}" }}, .{});
    defer result.deinit();
    try std.testing.expect(result.output.sql_nulls.?[0][0]);
    try std.testing.expect(!result.output.sql_nulls.?[1][0]);
    var decoded = try @import("array_wire.zig").decode(std.testing.allocator, .int64, result.output.rows[1][0], .{});
    defer decoded.deinit();
    try std.testing.expectEqual(@as(i64, 9007199254740993), decoded.value.flatCell(0).value.integer);
    try std.testing.expectEqual(@as(i32, -1), decoded.value.dimensions[0].lower);
    try std.testing.expect(decoded.value.flatCell(1).sql_null);
    var recursive = try compiler.compile(std.testing.allocator, "WITH RECURSIVE r(n) AS (SELECT cardinality($1::integer[]) UNION ALL SELECT n-1 FROM r WHERE n>1) SELECT n FROM r ORDER BY n", .{});
    defer recursive.deinit();
    var recursion = try execute(std.testing.allocator, fixture.iface(), &recursive, &.{.{ .string = "{1,2,3}" }}, .{});
    defer recursion.deinit();
    try std.testing.expectEqual(@as(usize, 3), recursion.output.rows.len);
    for (recursion.output.rows, 1..) |row, n| try std.testing.expectEqual(n, try std.fmt.parseInt(usize, row[0].string, 10));
}

test "SQL invocation arrays own public envelopes and reject invalid inputs before reads" {
    const a = std.testing.allocator;
    var fixture: TestBackend = .{};
    var source = try @import("array_text.zig").decode(a, .int64, "[-1:1]={9007199254740993,NULL,2}", .{});
    defer source.deinit();
    var input: std.heap.ArenaAllocator = .init(a);
    const envelope = try @import("array_wire.zig").toJsonLeaky(input.allocator(), source.value, .{});
    var compiled = try compiler.compile(a, "SELECT $1::bigint[] a,$2::integer+1 n", .{});
    defer compiled.deinit();
    var result = try execute(a, fixture.iface(), &compiled, &.{ envelope, .{ .integer = 41 } }, .{});
    defer result.deinit();
    input.deinit();
    try std.testing.expectEqualStrings("42", result.output.rows[0][1].string);
    var decoded = try @import("array_wire.zig").decode(a, .int64, result.output.rows[0][0], .{});
    defer decoded.deinit();
    try std.testing.expectEqual(@as(i64, 9007199254740993), decoded.value.flatCell(0).value.integer);
    try std.testing.expectEqual(@as(i32, -1), decoded.value.dimensions[0].lower);
    try std.testing.expect(decoded.value.flatCell(1).sql_null);
    var checked = try compiler.compile(a, "SELECT id FROM things WHERE id=ANY($1::smallint[])", .{});
    defer checked.deinit();
    try std.testing.expectError(error.SqlNumericOutOfRange, execute(a, fixture.iface(), &checked, &.{.{ .string = "{32768}" }}, .{}));
    try std.testing.expectEqual(@as(usize, 0), fixture.pages);
    var json_array = std.json.Array.init(a);
    defer json_array.deinit();
    try std.testing.expectError(error.SqlTypeMismatch, execute(a, fixture.iface(), &checked, &.{.{ .array = json_array }}, .{}));
    try std.testing.expectEqual(@as(usize, 0), fixture.pages);
}

test "SQL typed invocation ownership unwinds allocation faults across nested results" {
    const Faults = struct {
        fn run(a: std.mem.Allocator) !void {
            var fixture: TestBackend = .{};
            var compiled = try compiler.compile(a, "WITH q AS MATERIALIZED (SELECT $1::bigint[] a) SELECT a,cardinality(a) FROM q", .{});
            defer compiled.deinit();
            var result = try execute(a, fixture.iface(), &compiled, &.{.{ .string = "[0:2]={9007199254740993,NULL,2}" }}, .{});
            defer result.deinit();
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Faults.run, .{});
}

test "SQL typed array navigation promotes defaults without dropping bounds or SQL NULLs" {
    const Faults = struct {
        fn run(a: std.mem.Allocator) !void {
            var fixture: TestBackend = .{};
            var compiled = try compiler.compile(a, "SELECT lag($1::int2[],1,$2::float8[]) OVER (ORDER BY _id) a FROM things", .{});
            defer compiled.deinit();
            var result = try execute(a, fixture.iface(), &compiled, &.{ .{ .string = "[-1:1]={1,NULL,2}" }, .{ .string = "[0:1]={3.5,NULL}" } }, .{});
            defer result.deinit();
            try std.testing.expectEqual(@as(?@import("array_value.zig").ElementType, .float64), result.output.columns[0].element_type);
            try std.testing.expectEqual(@as(usize, 2), result.output.rows.len);
            var first = try @import("array_wire.zig").decode(a, .float64, result.output.rows[0][0], .{});
            defer first.deinit();
            var second = try @import("array_wire.zig").decode(a, .float64, result.output.rows[1][0], .{});
            defer second.deinit();
            try std.testing.expectEqual(@as(i32, 0), first.value.dimensions[0].lower);
            try std.testing.expectEqual(@as(f64, 3.5), first.value.flatCell(0).value.float);
            try std.testing.expect(first.value.flatCell(1).sql_null);
            try std.testing.expectEqual(@as(i32, -1), second.value.dimensions[0].lower);
            try std.testing.expectEqual(@as(f64, 1), second.value.flatCell(0).value.float);
            try std.testing.expect(second.value.flatCell(1).sql_null);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Faults.run, .{});
}

test "SQL target parameters preserve PostgreSQL source order and integer widths" {
    var backend: TestBackend = .{};
    // A bare first target is unknown/text before the later int4 expression
    // constrains the slot. PostgreSQL PREPARE rejects that ambiguity (42P08).
    var ambiguous = try compiler.compile(std.testing.allocator, "SELECT $1, $1 + 1", .{});
    defer ambiguous.deinit();
    try std.testing.expectError(error.ConflictingSqlParameterTypes, describe.describe(std.testing.allocator, backend.iface(), &ambiguous, &.{}));
    try std.testing.expectError(error.ConflictingSqlParameterTypes, execute(std.testing.allocator, backend.iface(), &ambiguous, &.{.{ .integer = 41 }}, .{}));
    var compiled = try compiler.compile(std.testing.allocator, "SELECT $1 + 1, $1", .{});
    defer compiled.deinit();
    var description = try describe.describe(std.testing.allocator, backend.iface(), &compiled, &.{});
    defer description.deinit();
    try std.testing.expectEqual(ast.ColumnType.integer, description.binding.parameter_types[0].?);
    try std.testing.expectEqual(@as(?@import("array_value.zig").ElementType, .int32), description.binding.parameter_descriptors[0].element_type);
    var result = try execute(std.testing.allocator, backend.iface(), &compiled, &.{.{ .integer = 41 }}, .{});
    defer result.deinit();
    try std.testing.expectEqualStrings("42", result.output.rows[0][0].string);
    try std.testing.expectEqualStrings("41", result.output.rows[0][1].string);
    try std.testing.expectError(error.SqlNumericOutOfRange, execute(std.testing.allocator, backend.iface(), &compiled, &.{.{ .integer = 9007199254740993 }}, .{}));
}

test "SQL numeric function parameter contexts use result identity not argument width" {
    var fixture: TestBackend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT sqrt(16)>$1,power(2,3)>$1", .{});
    defer compiled.deinit();
    var description = try describe.describe(std.testing.allocator, fixture.iface(), &compiled, &.{});
    defer description.deinit();
    try std.testing.expectEqual(ast.ColumnType.number, description.binding.parameter_types[0].?);
    try std.testing.expectEqual(@as(?@import("array_value.zig").ElementType, .float64), description.binding.parameter_descriptors[0].element_type);
    var result = try execute(std.testing.allocator, fixture.iface(), &compiled, &.{.{ .integer = 3 }}, .{});
    defer result.deinit();
    try std.testing.expect(result.output.rows[0][0].bool and result.output.rows[0][1].bool);
}

test "SQL mutation success preserves post-commit recovery outcomes" {
    var backend: TestBackend = .{ .outcome = .committed_repair_required };
    var compiled = try compiler.compile(std.testing.allocator, "DELETE FROM things", .{});
    defer compiled.deinit();
    var result = try execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(catalog.MutationOutcome.committed_repair_required, result.output.mutation_outcome.?);
    try std.testing.expectEqual(@as(u64, 2), result.output.rows_affected);
    try std.testing.expectEqual(@as(usize, 1), backend.writes);
}

test "SQL row identity uses a point seek and contradictory keys never scan" {
    var backend: TestBackend = .{ .row_count = 100_000 };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT _id, id FROM things WHERE _id = $1", .{});
    defer compiled.deinit();
    var result = try execute(std.testing.allocator, backend.iface(), &compiled, &.{.{ .string = "99999" }}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), backend.pages);
    try std.testing.expectEqual(@as(usize, 1), backend.point_reads);
    try std.testing.expectEqualStrings("99999", result.output.rows[0][0].string);
    for ([_][]const u8{
        "DELETE FROM things WHERE _id = 'a' AND _id = 'b'",
        "DELETE FROM things WHERE _id = NULL",
        "DELETE FROM things WHERE _id IS NULL",
        "DELETE FROM things WHERE _id = ''",
        "DELETE FROM things WHERE id = NULL",
        "DELETE FROM things WHERE id <> NULL",
    }) |sql| {
        var empty = try compiler.compile(std.testing.allocator, sql, .{});
        defer empty.deinit();
        var output = try execute(std.testing.allocator, backend.iface(), &empty, &.{}, .{});
        defer output.deinit();
        try std.testing.expectEqual(@as(u64, 0), output.output.rows_affected);
    }
    try std.testing.expectEqual(@as(usize, 1), backend.pages);
    try std.testing.expectEqual(@as(usize, 0), backend.writes);
}

test "SQL update reads only preserved columns and shares immutable assignments" {
    const Fixture = struct {
        expected: []const u8,
        reads: usize = 0,
        writes: usize = 0,

        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: ast.Name, action: catalog.Action) !catalog.Table {
            // A constant unconditional UPDATE needs write authority, not
            // SELECT; internal preservation is not a caller-visible read.
            try std.testing.expectEqual(catalog.Action.write, action);
            return .{ .id = 1, .physical_name = "wide", .schema_version = 1, .columns = &.{
                .{ .name = "keep", .path = "keep", .type = .integer },
                .{ .name = "payload", .path = "payload", .type = .string },
            } };
        }
        fn scan(ptr: *anyopaque, alloc: std.mem.Allocator, _: catalog.Table, request: catalog.Scan) !catalog.Page {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.reads += 1;
            try std.testing.expectEqual(@as(usize, 1), request.fields.len);
            try std.testing.expectEqualStrings("keep", request.fields[0]);
            const rows = try alloc.alloc(catalog.Row, 64);
            for (rows, 0..) |*row, index| {
                var object: std.json.ObjectMap = .empty;
                try object.put(alloc, "keep", .{ .integer = @intCast(index) });
                row.* = .{ .id = try std.fmt.allocPrint(alloc, "{d}", .{index}), .version = 27, .value = .{ .object = object } };
            }
            return .{ .rows = rows };
        }
        fn mutate(ptr: *anyopaque, _: std.mem.Allocator, _: std.mem.Allocator, _: catalog.Table, rows: []const catalog.Mutation) !catalog.MutationOutcome {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.writes += 1;
            try std.testing.expectEqual(@as(usize, 64), rows.len);
            var shared: ?[*]const u8 = null;
            for (rows, 0..) |row, index| {
                try std.testing.expectEqual(@as(u64, 27), row.expected_version);
                const object = row.row.?.object;
                try std.testing.expectEqual(@as(usize, 2), object.count());
                try std.testing.expectEqual(@as(i64, @intCast(index)), object.get("keep").?.integer);
                const payload = object.get("payload").?.string;
                try std.testing.expectEqualStrings(self.expected, payload);
                // The constant is independently owned, but not cloned 64 times.
                try std.testing.expect(payload.ptr != self.expected.ptr);
                if (shared) |first| try std.testing.expect(first == payload.ptr);
                shared = payload.ptr;
            }
            return .committed;
        }
        fn checkpoint(_: *anyopaque) !void {}
    };
    const payload = z17RepeatString("v", 2048);
    var fixture: Fixture = .{ .expected = payload };
    const backend: catalog.Backend = .{ .ptr = &fixture, .vtable = &.{ .resolve = Fixture.resolve, .scan = Fixture.scan, .mutate = Fixture.mutate, .checkpoint = Fixture.checkpoint } };
    var compiled = try compiler.compile(std.testing.allocator, "UPDATE wide SET payload = $1", .{});
    defer compiled.deinit();
    var result = try execute(std.testing.allocator, backend, &compiled, &.{.{ .string = payload }}, .{ .retained_bytes = 256 * 1024 });
    defer result.deinit();
    try std.testing.expectEqual(@as(u64, 64), result.output.rows_affected);
    try std.testing.expectEqual(@as(usize, 1), fixture.reads);
    try std.testing.expectEqual(@as(usize, 1), fixture.writes);
    std.debug.print("SQL shared UPDATE: peak_bytes={d} mutation_bytes={d} row_bytes={d}\n", .{ result.peakMemoryBytes(), @sizeOf(catalog.Mutation), @sizeOf(catalog.Row) });
    try std.testing.expect(result.peakMemoryBytes() < 128 * 1024);
}

test "SQL typed mutations preserve JSON null separately from SQL NULL and filter logical values" {
    const Fixture = struct {
        expected_null_fields: []const []const u8 = &.{},
        writes: usize = 0,
        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: ast.Name, _: catalog.Action) !catalog.Table {
            return .{ .id = 1, .physical_name = "typed", .schema_version = 1, .columns = &.{
                .{ .name = "j", .path = "j", .type = .json },
                .{ .name = "k", .path = "k", .type = .json, .nullable = false },
                .{ .name = "n", .path = "n", .type = .integer },
            } };
        }
        fn scan(_: *anyopaque, alloc: std.mem.Allocator, _: catalog.Table, request: catalog.Scan) !catalog.Page {
            var result: std.ArrayList(catalog.Row) = .empty;
            for ([_][]const u8{ "a", "b" }, 0..) |id, index| {
                if (request.primary_key) |key| if (!std.mem.eql(u8, key, id)) continue;
                var object: std.json.ObjectMap = .empty;
                try object.put(alloc, "j", .null);
                try object.put(alloc, "k", .null);
                try object.put(alloc, "n", .{ .integer = 1 });
                try result.append(alloc, .{ .id = id, .version = 1, .value = .{ .object = object }, .sql_nulls = if (index == 0) &.{ false, false, false } else &.{ true, false, false } });
            }
            return .{ .rows = result.items, .after = null };
        }
        fn mutate(ptr: *anyopaque, _: std.mem.Allocator, _: std.mem.Allocator, _: catalog.Table, mutations: []const catalog.Mutation) !catalog.MutationOutcome {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(usize, 1), mutations.len);
            try std.testing.expectEqual(self.expected_null_fields.len, mutations[0].json_null_fields.len);
            for (self.expected_null_fields) |expected| {
                var found = false;
                for (mutations[0].json_null_fields) |actual| if (std.mem.eql(u8, actual, expected)) {
                    found = true;
                    break;
                };
                try std.testing.expect(found);
                try std.testing.expect(mutations[0].row.?.object.get(expected).? == .null);
            }
            self.writes += 1;
            return .committed;
        }
        fn checkpoint(_: *anyopaque) !void {}
    };
    const alloc = std.testing.allocator;
    var fixture: Fixture = .{};
    const backend: catalog.Backend = .{ .ptr = &fixture, .vtable = &.{ .resolve = Fixture.resolve, .scan = Fixture.scan, .mutate = Fixture.mutate, .checkpoint = Fixture.checkpoint } };
    const Case = struct { sql: []const u8, fields: []const []const u8 };
    for ([_]Case{
        .{ .sql = "INSERT INTO typed (_id,j,k,n) VALUES ('c','null','null',1)", .fields = &.{ "j", "k" } },
        .{ .sql = "INSERT INTO typed (_id,j,k,n) VALUES ('c',NULL,'null',1)", .fields = &.{"k"} },
        .{ .sql = "UPDATE typed SET n=2 WHERE _id='a'", .fields = &.{ "j", "k" } },
        .{ .sql = "UPDATE typed SET j=CAST('null' AS JSON) WHERE _id='b'", .fields = &.{ "j", "k" } },
        .{ .sql = "UPDATE typed SET j=NULL WHERE _id='a'", .fields = &.{"k"} },
    }) |case| {
        fixture.expected_null_fields = case.fields;
        var compiled = try compiler.compile(alloc, case.sql, .{});
        defer compiled.deinit();
        var result = try execute(alloc, backend, &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(u64, 1), result.output.rows_affected);
    }
    var invalid = try compiler.compile(alloc, "INSERT INTO typed (_id,j,k,n) VALUES ('c','null',NULL,1)", .{});
    defer invalid.deinit();
    try std.testing.expectError(error.SqlNotNullViolation, execute(alloc, backend, &invalid, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 5), fixture.writes);
    var filtered = try compiler.compile(alloc, "SELECT _id, j FROM typed WHERE j='null'", .{});
    defer filtered.deinit();
    var selected = try execute(alloc, backend, &filtered, &.{}, .{});
    defer selected.deinit();
    try std.testing.expectEqual(@as(usize, 1), selected.output.rows.len);
    try std.testing.expectEqualStrings("a", selected.output.rows[0][0].string);
    try std.testing.expect(!selected.output.sql_nulls.?[0][1]);
}

test "SQL decisions execute batches across projection predicate aggregation and reusable CTE bindings" {
    const d = @import("../functions/decisions.zig");
    const Fake = struct {
        calls: usize = 0,
        max_batch: usize = 0,
        fn validate(_: *anyopaque, _: []const u8, questions: Json, _: []const u8) !void {
            try d.validateQuestions(questions, d.capabilities(.jev));
        }
        fn evaluate(ptr: *anyopaque, a: std.mem.Allocator, requests: []const d.Request) ![]const Json {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += requests.len;
            self.max_batch = @max(self.max_batch, requests.len);
            const output = try a.alloc(Json, requests.len);
            for (output) |*value| value.* = try std.json.parseFromSliceLeaky(Json, a, "{\"model\":\"mock\",\"answers\":[{\"name\":\"answer\",\"type\":\"predicate\",\"decision_method\":\"typed\",\"probability\":0.9}],\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}", .{});
            return output;
        }
    };
    const a = std.testing.allocator;
    for ([_][]const u8{
        "SELECT ai_probability(CAST(id AS TEXT), 'Refund?', 'local') FROM things",
        "SELECT id FROM things WHERE ai_probability(CAST(id AS TEXT), 'Refund?', 'local') > 0.8",
        "SELECT avg(ai_probability(CAST(id AS TEXT), 'Refund?', 'local')) FROM things",
        "WITH classified AS (SELECT ai_probability(CAST(id AS TEXT), 'Refund?', 'local') AS p FROM things) SELECT p,p FROM classified WHERE p > 0.8",
    }, 0..) |sql, index| {
        var fixture: TestBackend = .{};
        var fake: Fake = .{};
        var backend = if (index == 3) fixture.coordinated() else fixture.iface();
        backend.decision_provider = .{ .ptr = &fake, .validate_fn = Fake.validate, .evaluate_batch_fn = Fake.evaluate };
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        var result = try execute(a, backend, &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 2), fake.calls);
        try std.testing.expectEqual(@as(usize, if (index == 2) 1 else 2), result.output.rows.len);
        if (index != 2) try std.testing.expectEqual(@as(usize, 2), fake.max_batch);
    }
    var fixture: TestBackend = .{};
    var planning_backend = fixture.coordinated();
    planning_backend.atomic_statement_read_set = true;
    planning_backend.coordinated_point_reads = true;
    for ([_][]const u8{
        "EXPLAIN SELECT ai_probability(CAST(id AS TEXT), 'Refund?', 'missing') FROM things",
        "EXPLAIN SELECT AVG(ai_probability(CAST(id AS TEXT), 'Refund?', 'missing')) FROM things",
        "EXPLAIN SELECT ai_probability('refund', 'Refund?', 'missing')",
        "EXPLAIN DELETE FROM things WHERE ai_probability(CAST(id AS TEXT), 'Refund?', 'missing') > 0.8",
        "EXPLAIN UPDATE things SET id=CAST(ai_probability('refund', 'Refund?', 'missing') AS BIGINT)",
        "EXPLAIN INSERT INTO things (_id,id) VALUES ('new',CAST(ai_probability('refund','Refund?','missing') AS BIGINT))",
        "EXPLAIN DELETE FROM things RETURNING ai_probability(CAST(id AS TEXT),'Refund?','missing')",
        "EXPLAIN INSERT INTO things (_id,id) VALUES ('new',1) ON CONFLICT (_id) DO UPDATE SET id=CAST(ai_probability('refund','Refund?','missing') AS BIGINT)",
        "EXPLAIN (FORMAT JSON) UPDATE things SET id=1 WHERE ai_probability(CAST(id AS TEXT),'Refund?','missing')>0.8",
        "EXPLAIN INSERT INTO things (_id,id) SELECT _id,CAST(ai_probability(CAST(id AS TEXT),'Refund?','missing') AS BIGINT) FROM things",
        "EXPLAIN UPDATE things t SET id=CAST(ai_probability(CAST(s.id AS TEXT),'Refund?','missing') AS BIGINT) FROM things s WHERE t._id=s._id",
        "EXPLAIN DELETE FROM things t USING things s WHERE t._id=s._id AND ai_probability(CAST(s.id AS TEXT),'Refund?','missing')>0.8",
        "EXPLAIN INSERT INTO things (_id,id) VALUES ('new',1) ON CONFLICT (_id) DO UPDATE SET id=1 WHERE ai_probability(CAST(things.id AS TEXT),'Refund?','missing')>0.8",
    }) |sql| {
        var explain = try compiler.compile(a, sql, .{});
        defer explain.deinit();
        var explained = try execute(a, planning_backend, &explain, &.{}, .{});
        defer explained.deinit();
        try std.testing.expectEqual(@as(usize, 0), fixture.pages);
        try std.testing.expect(std.mem.indexOf(u8, explained.output.rows[0][0].string, "DecisionEval") != null);
    }
    var invalid = try compiler.compile(a, "SELECT ai_decide(CAST(id AS TEXT), '{}', 'local') FROM things", .{});
    defer invalid.deinit();
    try std.testing.expectError(error.InvalidDecisionSpecification, execute(a, fixture.iface(), &invalid, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 0), fixture.pages);
}

test "SQL decision CTE pages preserve projection demand offset and limit" {
    const Provider = @import("decision_eval.zig").testing.Provider;
    const Case = struct { sql: []const u8, rows: usize, calls: usize, max_batch: usize };
    const cases = [_]Case{
        .{ .sql = "WITH c AS (SELECT ai_probability(_id,'Refund?','local') AS p FROM things) SELECT p,p FROM c WHERE p>0.8", .rows = 19, .calls = 19, .max_batch = 4 },
        // The first source page skips three rows and produces one value. The
        // enclosing four-row page then demands only three additional values.
        .{ .sql = "WITH c AS (SELECT ai_probability(_id,'Refund?','local') AS p FROM things LIMIT 5 OFFSET 3) SELECT p FROM c", .rows = 5, .calls = 5, .max_batch = 3 },
        .{ .sql = "WITH c AS (SELECT _id FROM things WHERE ai_probability(_id,'Refund?','local')>0.8) SELECT _id FROM c", .rows = 19, .calls = 19, .max_batch = 4 },
        .{ .sql = "WITH c AS (SELECT CASE WHEN FALSE THEN ai_probability(_id,'Refund?','local') ELSE 0 END AS p FROM things) SELECT p FROM c", .rows = 19, .calls = 0, .max_batch = 0 },
        .{ .sql = "WITH c AS (SELECT ai_probability(_id,'Refund?','local') AS p FROM things LIMIT 0) SELECT p FROM c", .rows = 0, .calls = 0, .max_batch = 0 },
        .{ .sql = "WITH c AS (SELECT _id FROM things WHERE _id<>'0' AND ai_probability(_id,'Refund?','local')>0.8) SELECT _id FROM c", .rows = 18, .calls = 18, .max_batch = 4 },
    };
    for (cases) |case| {
        var fixture: TestBackend = .{ .row_count = 19 };
        var provider: Provider = .{};
        var backend = fixture.coordinated();
        backend.decision_provider = provider.provider();
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var result = try execute(std.testing.allocator, backend, &compiled, &.{}, .{ .page_rows = 4 });
        defer result.deinit();
        try std.testing.expectEqual(case.rows, result.output.rows.len);
        try std.testing.expectEqual(case.calls, provider.calls);
        try std.testing.expectEqual(case.max_batch, provider.max_batch);
        try std.testing.expectEqual(fixture.statement_opens, fixture.statement_closes);
    }
    var fixture: TestBackend = .{ .row_count = 19 };
    var provider: Provider = .{ .fail = true };
    var backend = fixture.coordinated();
    backend.decision_provider = provider.provider();
    var compiled = try compiler.compile(std.testing.allocator, cases[0].sql, .{});
    defer compiled.deinit();
    try std.testing.expectError(error.DecisionProviderUnavailable, execute(std.testing.allocator, backend, &compiled, &.{}, .{ .page_rows = 4 }));
    try std.testing.expectEqual(fixture.statement_opens, fixture.statement_closes);
}

test "SQL LIMIT scan pages are independent of residual selectivity" {
    var backend: TestBackend = .{ .row_count = 3000 };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT _id FROM things WHERE _id LIKE '%2999' LIMIT 1", .{});
    defer compiled.deinit();
    var result = try execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
    try std.testing.expectEqualStrings("2999", result.output.rows[0][0].string);
    try std.testing.expectEqual(@as(usize, 12), backend.pages);
}

test "SQL virtual relation byte pages preserve continuation and release exhausted sources" {
    var backend: TestBackend = .{ .row_count = 5000 };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT count(*) FROM (SELECT _id FROM things) q", .{});
    defer compiled.deinit();
    var result = try execute(std.testing.allocator, backend.coordinated(), &compiled, &.{}, .{ .page_bytes = 1, .retained_bytes = 512 * 1024 });
    defer result.deinit();
    try std.testing.expectEqualStrings("5000", result.output.rows[0][0].string);
    try std.testing.expectEqual(backend.statement_opens, backend.statement_closes);
    try std.testing.expect(result.peakMemoryBytes() < 512 * 1024);
}

fn z17RepeatString(comptime bytes: []const u8, comptime repetitions: usize) *const [bytes.len * repetitions:0]u8 {
    const result = comptime blk: {
        @setEvalBranchQuota(@intCast(@min(std.math.maxInt(u32), 100000 +| (repetitions *| 16))));
        var repeated: [bytes.len * repetitions:0]u8 = undefined;
        for (0..repetitions) |i| @memcpy(repeated[i * bytes.len ..][0..bytes.len], bytes);
        repeated[bytes.len * repetitions] = 0;
        break :blk repeated;
    };
    return &result;
}

test "SQL decision ordering batches qualifying rows and preserves lazy evaluation" {
    const a = std.testing.allocator;
    const cases = [_]struct { sql: []const u8, calls: usize, rows: usize }{
        .{ .sql = "WITH q AS (SELECT id FROM things) SELECT id FROM q ORDER BY ai_probability(CAST(id AS TEXT),'Refund?','local') DESC LIMIT 3", .calls = 19, .rows = 3 },
        .{ .sql = "WITH q AS (SELECT _id FROM things) SELECT _id,count(*) FROM q GROUP BY _id ORDER BY ai_probability(CAST(count(*) AS TEXT),'Refund?','local') DESC LIMIT 3", .calls = 19, .rows = 3 },
        .{ .sql = "WITH q AS (SELECT _id FROM things) SELECT _id,row_number() OVER (ORDER BY _id) AS n FROM q ORDER BY ai_probability(CAST(row_number() OVER (ORDER BY _id) AS TEXT),'Refund?','local') DESC LIMIT 3", .calls = 19, .rows = 3 },
        .{ .sql = "WITH q AS (SELECT _id FROM things) SELECT _id FROM q WHERE _id<>'0' ORDER BY ai_probability(_id,'Refund?','local') DESC LIMIT 3", .calls = 18, .rows = 3 },
        .{ .sql = "WITH q AS (SELECT id FROM things) SELECT id FROM q ORDER BY CASE WHEN FALSE THEN ai_probability(CAST(id AS TEXT),'Refund?','local') ELSE 0 END LIMIT 3", .calls = 0, .rows = 3 },
        .{ .sql = "WITH q AS (SELECT id FROM things) SELECT id FROM q ORDER BY ai_probability(CAST(id AS TEXT),'Refund?','local') LIMIT 0", .calls = 0, .rows = 0 },
    };
    for (cases) |case| {
        var fixture: TestBackend = .{ .row_count = 19 };
        var mock: @import("decision_eval.zig").testing.Provider = .{};
        var backend = fixture.coordinated();
        backend.decision_provider = mock.provider();
        var compiled = try compiler.compile(a, case.sql, .{});
        defer compiled.deinit();
        var result = try execute(a, backend, &compiled, &.{}, .{ .page_rows = 4 });
        defer result.deinit();
        try std.testing.expectEqual(case.calls, mock.calls);
        try std.testing.expectEqual(case.rows, result.output.rows.len);
        try std.testing.expectEqual(@as(usize, if (case.calls == 0) 0 else 4), mock.max_batch);
        try std.testing.expectEqual(fixture.statement_opens, fixture.statement_closes);
    }
}

test "SQL malformed decision specifications report client errors before reads" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "SELECT ai_decide('refund','{','local') FROM things",
        "SELECT ai_choice('refund','Refund?','{','local') FROM things",
        "SELECT ai_score('refund','Urgency','[','local') FROM things",
    }) |sql| {
        var fixture: TestBackend = .{};
        var mock: @import("decision_eval.zig").testing.Provider = .{};
        var backend = fixture.coordinated();
        backend.decision_provider = mock.provider();
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(error.InvalidDecisionSpecification, execute(a, backend, &compiled, &.{}, .{}));
        try std.testing.expectEqual(@as(usize, 0), fixture.pages);
        try std.testing.expectEqual(@as(usize, 0), mock.calls);
    }
    const diagnostic = @import("errors.zig").describe(error.InvalidDecisionSpecification);
    try std.testing.expectEqualStrings("22023", diagnostic.code);
    try std.testing.expectEqual(@as(u16, 400), diagnostic.httpStatus());
}

test "SQL ordinary mutations batch decisions and fail before commit" {
    const Provider = @import("decision_eval.zig").testing.Provider;
    const a = std.testing.allocator;
    for ([_][]const u8{
        "UPDATE things SET id=id WHERE ai_probability(_id,'Refund?','local')>0.8",
        "UPDATE things SET id=CASE WHEN ai_probability(_id,'Refund?','local')>0.8 THEN id ELSE id END",
        "DELETE FROM things WHERE ai_probability(_id,'Refund?','local')>0.8",
        "DELETE FROM things RETURNING ai_probability(_id,'Refund?','local')",
    }) |sql| {
        for ([_]usize{ 1, 65536 }) |page_bytes| {
            var fixture: TestBackend = .{ .row_count = 8 };
            var provider: Provider = .{ .expected_source = "table:stable" };
            var backend = fixture.iface();
            backend.decision_provider = provider.provider();
            var compiled = try compiler.compile(a, sql, .{});
            defer compiled.deinit();
            var result = try execute(a, backend, &compiled, &.{}, .{ .page_rows = 4, .page_bytes = page_bytes });
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 8), provider.calls);
            try std.testing.expectEqual(@as(usize, if (page_bytes == 1) 1 else 4), provider.max_batch);
            try std.testing.expectEqual(@as(usize, 1), fixture.writes);
            try std.testing.expectEqual(@as(u64, 8), result.output.rows_affected);
            if (std.mem.indexOf(u8, sql, "RETURNING") != null) {
                try std.testing.expectEqual(@as(usize, 8), result.output.rows.len);
                for (result.output.rows) |row| try std.testing.expectApproxEqAbs(@as(f64, 0.9), row[0].float, 0.001);
            }
        }
        var fixture: TestBackend = .{ .row_count = 8 };
        var provider: Provider = .{ .fail_after = 4 };
        var backend = fixture.iface();
        backend.decision_provider = provider.provider();
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(error.DecisionProviderUnavailable, execute(a, backend, &compiled, &.{}, .{ .page_rows = 4 }));
        try std.testing.expectEqual(@as(usize, 0), fixture.writes);
    }
    var fixture: TestBackend = .{ .row_count = 8 };
    var provider: Provider = .{};
    var backend = fixture.iface();
    backend.decision_provider = provider.provider();
    var compiled = try compiler.compile(a, "UPDATE things SET id=CASE WHEN ai_probability(_id,'Refund?','local')>0.8 THEN id ELSE id END WHERE _id<>'0'", .{});
    defer compiled.deinit();
    var result = try execute(a, backend, &compiled, &.{}, .{ .page_rows = 4 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 7), provider.calls);
    try std.testing.expectEqual(@as(usize, 4), provider.max_batch);
}

test "SQL decision reads enforce byte pages across projection predicate and aggregate" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "SELECT ai_probability(_id,'Refund?','local') FROM things",
        "SELECT id FROM things WHERE ai_probability(_id,'Refund?','local')>0.8",
        "SELECT avg(ai_probability(_id,'Refund?','local')) FROM things",
    }) |sql| {
        var fixture: TestBackend = .{ .row_count = 8 };
        var provider: @import("decision_eval.zig").testing.Provider = .{};
        var backend = fixture.iface();
        backend.decision_provider = provider.provider();
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        var result = try execute(a, backend, &compiled, &.{}, .{ .page_rows = 4, .page_bytes = 1 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 8), provider.calls);
        try std.testing.expectEqual(@as(usize, 1), provider.max_batch);
    }
}

test "SQL sorted decisions project only selected rows and preserve sort dependencies" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "SELECT ai_probability(_id,'Refund?','local') FROM things ORDER BY id+1 LIMIT 2 OFFSET 1",
        "SELECT ai_probability(_id,'Refund?','local') AS p FROM things ORDER BY p LIMIT 2 OFFSET 1",
    }, 0..) |sql, index| {
        var fixture: TestBackend = .{ .row_count = 8 };
        var provider: @import("decision_eval.zig").testing.Provider = .{};
        var backend = fixture.iface();
        backend.decision_provider = provider.provider();
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        var result = try execute(a, backend, &compiled, &.{}, .{ .page_rows = 4 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 2), result.output.rows.len);
        try std.testing.expectEqual(@as(usize, if (index == 0) 2 else 8), provider.calls);
        try std.testing.expectEqual(@as(usize, if (index == 0) 2 else 4), provider.max_batch);
    }
}

fn deferredDecisionAllocationScenario(a: std.mem.Allocator) !void {
    var fixture: TestBackend = .{ .row_count = 4 };
    var provider: @import("decision_eval.zig").testing.Provider = .{};
    var backend = fixture.iface();
    backend.decision_provider = provider.provider();
    var compiled = try compiler.compile(a, "SELECT ai_probability(_id,'Refund?','local') FROM things ORDER BY id+1 LIMIT 1", .{});
    defer compiled.deinit();
    var result = try execute(a, backend, &compiled, &.{}, .{ .page_rows = 2 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), provider.calls);
}

test "SQL deferred decision projections unwind every allocation failure" {
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, deferredDecisionAllocationScenario, .{});
}

test "SQL grouped and window decisions defer independent projections and reuse sort outputs" {
    const a = std.testing.allocator;
    const cases = [_]struct { sql: []const u8, calls: usize }{
        .{ .sql = "SELECT ai_probability(_id,'Refund?','local') FROM things GROUP BY _id ORDER BY _id LIMIT 2 OFFSET 1", .calls = 2 },
        .{ .sql = "SELECT ai_probability(_id,'Refund?','local'), row_number() OVER (ORDER BY id) FROM things ORDER BY id LIMIT 2 OFFSET 1", .calls = 2 },
        .{ .sql = "SELECT ai_probability(CAST(count(*) AS TEXT),'Refund?','local') FROM things GROUP BY _id LIMIT 2 OFFSET 1", .calls = 2 },
        .{ .sql = "SELECT ai_probability(CAST(row_number() OVER (ORDER BY id) AS TEXT),'Refund?','local') FROM things ORDER BY id LIMIT 2 OFFSET 1", .calls = 2 },
        .{ .sql = "SELECT ai_probability(_id,'Refund?','local') AS p FROM things GROUP BY _id ORDER BY p LIMIT 2 OFFSET 1", .calls = 8 },
        .{ .sql = "SELECT ai_probability(_id,'Refund?','local') AS p, row_number() OVER (ORDER BY id) FROM things ORDER BY p LIMIT 2 OFFSET 1", .calls = 8 },
        .{ .sql = "SELECT ai_probability(_id,'Refund?','local'), row_number() OVER (ORDER BY ai_probability(_id,'Refund?','local')) FROM things LIMIT 2 OFFSET 1", .calls = 10 },
        .{ .sql = "SELECT CASE WHEN FALSE THEN ai_probability(_id,'Refund?','local') ELSE 1.0 END, row_number() OVER (ORDER BY id) FROM things LIMIT 2 OFFSET 1", .calls = 0 },
    };
    for (cases) |case| for ([_]usize{ 1, 65536 }) |page_bytes| {
        var fixture: TestBackend = .{ .row_count = 8 };
        var provider: @import("decision_eval.zig").testing.Provider = .{ .fail_after = if (case.calls == 2) 2 else null };
        var backend = fixture.iface();
        backend.decision_provider = provider.provider();
        var compiled = try compiler.compile(a, case.sql, .{});
        defer compiled.deinit();
        var result = try execute(a, backend, &compiled, &.{}, .{ .page_rows = 4, .page_bytes = page_bytes });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 2), result.output.rows.len);
        try std.testing.expectEqual(case.calls, provider.calls);
        if (case.calls != 0) try std.testing.expectEqual(@as(usize, if (page_bytes == 1) 1 else @min(case.calls, 4)), provider.max_batch);
        if (case.calls == 2) for (result.output.rows) |row| try std.testing.expectApproxEqAbs(@as(f64, 0.9), row[0].float, 0.001);
    };
}
fn groupedWindowAllocationScenario(a: std.mem.Allocator) !void {
    for ([_][]const u8{
        "SELECT ai_probability(_id,'Refund?','local') FROM things GROUP BY _id ORDER BY _id LIMIT 1",
        "SELECT ai_probability(_id,'Refund?','local'),row_number() OVER (ORDER BY id) FROM things ORDER BY id LIMIT 1",
    }) |sql| {
        var fixture: TestBackend = .{ .row_count = 2 };
        var provider: @import("decision_eval.zig").testing.Provider = .{};
        var backend = fixture.iface();
        backend.decision_provider = provider.provider();
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        var result = try execute(a, backend, &compiled, &.{}, .{ .page_rows = 1 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), provider.calls);
    }
}
test "SQL grouped and window deferred projections unwind allocation failures" {
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, groupedWindowAllocationScenario, .{});
}

const InsertDecisionFixture = struct {
    calls: usize = 0,
    max_batch: usize = 0,
    writes: usize = 0,
    metadata_bytes: usize = 65536,
    fail_after: ?usize = null,
    fn backend(self: *@This()) catalog.Backend {
        return .{ .ptr = self, .decision_provider = .{ .ptr = self, .validate_fn = validate, .evaluate_batch_fn = batch }, .vtable = &.{ .resolve = resolve, .scan = scan, .mutate = mutate, .checkpoint = checkpoint } };
    }
    fn resolve(_: *anyopaque, _: std.mem.Allocator, _: ast.Name, _: catalog.Action) !catalog.Table {
        return .{ .id = 1, .physical_name = "things", .schema_version = 1, .columns = &.{
            .{ .name = "p", .path = "p", .type = .number },
            .{ .name = "payload", .path = "payload", .type = .json },
        } };
    }
    fn scan(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
        return error.UnexpectedScan;
    }
    fn checkpoint(_: *anyopaque) !void {}
    fn validate(_: *anyopaque, _: []const u8, questions: Json, _: []const u8) !void {
        try @import("../functions/decisions.zig").validateQuestions(questions, @import("../functions/decisions.zig").capabilities(.antfly));
    }
    fn batch(ptr: *anyopaque, a: std.mem.Allocator, requests: []const @import("../functions/decisions.zig").Request) ![]const Json {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.fail_after) |count_| if (self.calls >= count_) return error.DecisionProviderUnavailable;
        self.calls += requests.len;
        self.max_batch = @max(self.max_batch, requests.len);
        const results = try a.alloc(Json, requests.len);
        for (results) |*result| {
            result.* = try std.json.parseFromSliceLeaky(Json, a, "{\"model\":\"mock\",\"answers\":[{\"name\":\"answer\",\"type\":\"predicate\",\"decision_method\":\"typed\",\"probability\":0.9}],\"usage\":{\"input_tokens\":2,\"output_tokens\":0}}", .{});
            const payload = try a.alloc(u8, self.metadata_bytes);
            @memset(payload, 'x');
            try result.object.put(a, "metadata", .{ .string = payload });
        }
        return results;
    }
    fn mutate(ptr: *anyopaque, _: std.mem.Allocator, _: std.mem.Allocator, _: catalog.Table, mutations: []const catalog.Mutation) !catalog.MutationOutcome {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        for (mutations) |mutation| {
            try std.testing.expect(std.mem.startsWith(u8, mutation.key, "row"));
            try std.testing.expectEqual(@as(u64, 0), mutation.expected_version);
            try std.testing.expect(mutation.unique_absence);
            if (mutation.row.?.object.get("p")) |datum| if (datum != .null) try std.testing.expectApproxEqAbs(@as(f64, 0.9), datum.float, 0.001);
            if (mutation.row.?.object.get("payload")) |payload| try std.testing.expectEqual(self.metadata_bytes, payload.object.get("metadata").?.string.len);
        }
        self.writes += 1;
        return .committed;
    }
};

test "SQL INSERT values release provider payloads and batch heterogeneous conditional programs" {
    const a = std.testing.allocator;
    var sql: std.ArrayList(u8) = .empty;
    defer sql.deinit(a);
    try sql.appendSlice(a, "INSERT INTO things (_id,p) VALUES ");
    for (0..16) |index| {
        const item = try std.fmt.allocPrint(a, "{s}(lower('ROW{d}'),CASE WHEN {s} THEN ai_probability('input-{d}','Question {d}?','local') ELSE NULL END)", .{ if (index == 0) "" else ",", index, if (index % 2 == 0) "TRUE" else "FALSE", index, index });
        defer a.free(item);
        try sql.appendSlice(a, item);
    }
    var compiled = try compiler.compile(a, sql.items, .{});
    defer compiled.deinit();
    for ([_]Limits{ .{ .page_rows = 1, .retained_bytes = 1024 * 1024 }, .{ .page_rows = 4 }, .{ .page_rows = 4, .page_bytes = 1 } }) |limits| {
        var fixture: InsertDecisionFixture = .{};
        var result = try execute(a, fixture.backend(), &compiled, &.{}, limits);
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 8), fixture.calls);
        try std.testing.expectEqual(@as(usize, if (limits.page_bytes == 1 or limits.page_rows == 1) 1 else 2), fixture.max_batch);
        try std.testing.expectEqual(@as(usize, 1), fixture.writes);
        try std.testing.expectEqual(@as(u64, 16), result.output.rows_affected);
        fixture = .{ .fail_after = 2 };
        try std.testing.expectError(error.DecisionProviderUnavailable, execute(a, fixture.backend(), &compiled, &.{}, limits));
        try std.testing.expectEqual(@as(usize, 0), fixture.writes);
    }
    var fixture: InsertDecisionFixture = .{};
    var json = try compiler.compile(a, "INSERT INTO things (_id,payload) VALUES ('row0',ai_decide('input','[{\"name\":\"answer\",\"type\":\"predicate\",\"instructions\":\"Refund?\"}]','local')),('row1',ai_decide('input','[{\"name\":\"answer\",\"type\":\"predicate\",\"instructions\":\"Refund?\"}]','local'))", .{});
    defer json.deinit();
    var result = try execute(a, fixture.backend(), &json, &.{}, .{ .page_rows = 1 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), fixture.calls);
    try std.testing.expectEqual(@as(usize, 1), fixture.writes);
}
fn insertDecisionAllocationScenario(a: std.mem.Allocator) !void {
    var fixture: InsertDecisionFixture = .{ .metadata_bytes = 0 };
    var compiled = try compiler.compile(a, "INSERT INTO things (_id,p) VALUES (lower('ROW0'),ai_probability('input','Refund?','local')),(lower('ROW1'),ai_probability('input','Return?','local'))", .{});
    defer compiled.deinit();
    var result = try execute(a, fixture.backend(), &compiled, &.{}, .{ .page_rows = 1 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), fixture.calls);
    try std.testing.expectEqual(@as(usize, 1), fixture.writes);
}
test "SQL INSERT decision pages unwind allocation failures before commit" {
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, insertDecisionAllocationScenario, .{});
}

fn constantDecisionAllocationScenario(a: std.mem.Allocator) !void {
    var fixture: InsertDecisionFixture = .{ .metadata_bytes = 0 };
    var compiled = try compiler.compile(a, "SELECT ai_probability('input','Refund?','local'),ai_decide('input','[{\"name\":\"answer\",\"type\":\"predicate\",\"instructions\":\"Refund?\"}]','local')", .{});
    defer compiled.deinit();
    var result = try execute(a, fixture.backend(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), fixture.calls);
    try std.testing.expectEqualStrings("mock", result.output.rows[0][1].object.get("model").?.string);
}

test "SQL constant decisions release discarded provider payloads and own final values" {
    const a = std.testing.allocator;
    var sql: std.ArrayList(u8) = .empty;
    defer sql.deinit(a);
    try sql.appendSlice(a, "SELECT ");
    for (0..16) |index| {
        if (index != 0) try sql.appendSlice(a, ",");
        try sql.appendSlice(a, "ai_probability('input','Refund?','local')");
    }
    var compiled = try compiler.compile(a, sql.items, .{});
    defer compiled.deinit();
    var fixture: InsertDecisionFixture = .{};
    var result = try execute(a, fixture.backend(), &compiled, &.{}, .{ .page_rows = 1, .retained_bytes = 1024 * 1024 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 16), fixture.calls);
    for (result.output.rows[0]) |cell| try std.testing.expectApproxEqAbs(@as(f64, 0.9), cell.float, 0.001);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, constantDecisionAllocationScenario, .{});
}

test "SQL decisions retain trusted routing across reads CTEs grouped and window inputs" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "SELECT ai_probability(_id,'Refund?','local') FROM things LIMIT 1",
        "WITH q AS (SELECT _id FROM things) SELECT ai_probability(_id,'Refund?','local') FROM q LIMIT 1",
        "SELECT ai_probability(_id,'Refund?','local') FROM things GROUP BY _id LIMIT 1",
        "SELECT ai_probability(_id,'Refund?','local'),row_number() OVER (ORDER BY id) FROM things LIMIT 1",
    }) |sql| {
        var fixture: TestBackend = .{};
        var provider: @import("decision_eval.zig").testing.Provider = .{ .expected_source = "table:stable" };
        var backend = fixture.coordinated();
        backend.decision_provider = provider.provider();
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        var result = try execute(a, backend, &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), provider.calls);
    }
}

test "SQL runtime executes spilled order group distinct and join under a small statement budget" {
    const a = std.testing.allocator;
    for ([_]struct { sql: []const u8, first: []const u8, second: []const u8 }{
        .{ .sql = "SELECT CAST(_id AS BIGINT) AS k FROM things ORDER BY k DESC LIMIT 2 OFFSET 1499", .first = "500", .second = "499" },
        .{ .sql = "SELECT CAST(_id AS BIGINT) AS k, SUM(CAST(_id AS BIGINT)) AS total, COUNT(DISTINCT _id) AS n FROM things GROUP BY CAST(_id AS BIGINT) ORDER BY k LIMIT 2 OFFSET 1998", .first = "1998", .second = "1999" },
        .{ .sql = "SELECT CAST(a._id AS BIGINT) AS k FROM things a JOIN things b ON a._id = b._id ORDER BY k DESC LIMIT 2", .first = "1999", .second = "1998" },
    }) |case| {
        var fixture: TestBackend = .{ .row_count = 2000 };
        var backend = fixture.coordinated();
        backend.pinned_statement_snapshot = true;
        backend.execution_io = std.testing.io;
        var compiled = try compiler.compile(a, case.sql, .{});
        defer compiled.deinit();
        var quota: MemoryBudget = .{ .backing = std.heap.page_allocator, .limit = 512 * 1024 };
        defer std.debug.assert(quota.live == 0);
        var result = try execute(quota.allocator(), backend, &compiled, &.{}, .{ .retained_bytes = 256 * 1024, .page_rows = 16 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 2), result.output.rows.len);
        try std.testing.expectEqualStrings(case.first, result.output.rows[0][0].string);
        try std.testing.expectEqualStrings(case.second, result.output.rows[1][0].string);
        try std.testing.expect(result.state.budget.peak <= 256 * 1024);
        if (result.output.rows[0].len == 3) {
            try std.testing.expectEqualStrings(case.first, result.output.rows[0][1].string);
            try std.testing.expectEqualStrings("1", result.output.rows[0][2].string);
        }
    }
}

test "SQL NUMERIC aggregates spill grouped partials with PostgreSQL scale and DISTINCT" {
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |distinct| {
        const sql = if (distinct)
            "SELECT CAST(_id AS BIGINT) % 1000 AS k, SUM(DISTINCT '0.10'::NUMERIC + _id::NUMERIC), AVG(DISTINCT '0.10'::NUMERIC + _id::NUMERIC) FROM things GROUP BY CAST(_id AS BIGINT) % 1000 ORDER BY k LIMIT 2 OFFSET 998"
        else
            "SELECT CAST(_id AS BIGINT) % 1000 AS k, SUM('0.10'::NUMERIC + _id::NUMERIC), AVG('0.10'::NUMERIC + _id::NUMERIC) FROM things GROUP BY CAST(_id AS BIGINT) % 1000 ORDER BY k LIMIT 2 OFFSET 998";
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        var fixture: TestBackend = .{ .row_count = 2000 };
        var native = try execute(a, fixture.iface(), &compiled, &.{}, .{ .retained_bytes = 8 * 1024 * 1024 });
        defer native.deinit();
        var backend = fixture.iface();
        backend.execution_io = std.testing.io;
        var quota: MemoryBudget = .{ .backing = std.heap.page_allocator, .limit = 1024 * 1024 };
        defer std.debug.assert(quota.live == 0);
        var spilled = try execute(quota.allocator(), backend, &compiled, &.{}, .{ .retained_bytes = 256 * 1024, .page_rows = 16 });
        defer spilled.deinit();
        try std.testing.expectEqualDeep(native.output.rows, spilled.output.rows);
        try std.testing.expectEqualDeep(native.output.sql_nulls, spilled.output.sql_nulls);
        try std.testing.expectEqualStrings("2996.20", spilled.output.rows[0][1].string);
        try std.testing.expectEqualStrings("1498.1000000000000000", spilled.output.rows[0][2].string);
        try std.testing.expectEqualStrings("2998.20", spilled.output.rows[1][1].string);
        try std.testing.expectEqualStrings("1499.1000000000000000", spilled.output.rows[1][2].string);
    }
}

test "SQL external window partitions match memory frames ranks and navigation under small budget" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "SELECT CAST(_id AS BIGINT) AS k, row_number() OVER (PARTITION BY CAST(_id AS BIGINT) % 7 ORDER BY CAST(_id AS BIGINT)) AS rn, sum(CAST(_id AS BIGINT)) OVER (PARTITION BY CAST(_id AS BIGINT) % 7 ORDER BY CAST(_id AS BIGINT) ROWS BETWEEN 3 PRECEDING AND 2 FOLLOWING) AS s, lag(_id,2,'none') OVER (PARTITION BY CAST(_id AS BIGINT) % 7 ORDER BY CAST(_id AS BIGINT)) AS previous FROM things ORDER BY k LIMIT 3 OFFSET 990",
        "SELECT CAST(_id AS BIGINT) AS k, rank() OVER (ORDER BY CAST(_id AS BIGINT) % 3) AS r, sum(CAST(_id AS BIGINT)) OVER (ORDER BY CAST(_id AS BIGINT) % 3 GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW) AS s, last_value(_id) OVER (ORDER BY CAST(_id AS BIGINT) RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last FROM things ORDER BY k LIMIT 3 OFFSET 990",
    }) |sql| {
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        var fixture: TestBackend = .{ .row_count = 1000 };
        var native = try execute(std.heap.page_allocator, fixture.iface(), &compiled, &.{}, .{ .retained_bytes = 8 * 1024 * 1024, .page_rows = 16 });
        defer native.deinit();
        var backend = fixture.iface();
        backend.execution_io = std.testing.io;
        var quota: MemoryBudget = .{ .backing = std.heap.page_allocator, .limit = 512 * 1024 };
        defer std.debug.assert(quota.live == 0);
        var spilled = try execute(quota.allocator(), backend, &compiled, &.{}, .{ .retained_bytes = 256 * 1024, .page_rows = 16 });
        defer spilled.deinit();
        try std.testing.expectEqualDeep(native.output.rows, spilled.output.rows);
        try std.testing.expectEqualDeep(native.output.sql_nulls, spilled.output.sql_nulls);
        try std.testing.expect(spilled.state.budget.peak <= 256 * 1024);
    }
}

test "SQL quantified subqueries retain spilled pattern sources through relational adapters" {
    for ([_]struct { sql: []const u8, expected: ?bool }{
        .{ .sql = "SELECT 'absent' LIKE ANY (SELECT _id FROM things)", .expected = false },
        .{ .sql = "SELECT 'absent' LIKE ANY (SELECT _id FROM things UNION ALL SELECT NULL)", .expected = null },
        .{ .sql = "SELECT 'absent' NOT LIKE ALL (SELECT _id FROM things)", .expected = true },
    }) |case| {
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var fixture: TestBackend = .{ .row_count = 2000 };
        var backend = fixture.coordinated();
        backend.execution_io = std.testing.io;
        var output = try execute(std.heap.page_allocator, backend, &compiled, &.{}, .{ .retained_bytes = 1024 * 1024, .page_rows = 32 });
        defer output.deinit();
        try std.testing.expectEqual(@as(usize, 1), output.output.rows.len);
        if (case.expected) |value| {
            try std.testing.expect(!output.output.sql_nulls.?[0][0]);
            try std.testing.expectEqual(value, output.output.rows[0][0].bool);
        } else try std.testing.expect(output.output.sql_nulls.?[0][0]);
    }
}

test "SQL snapshot estimates change build side without changing residual outer joins" {
    const a = std.testing.allocator;
    for ([_][]const u8{ "LEFT", "RIGHT", "FULL" }) |kind| {
        const text = try std.fmt.allocPrint(a, "SELECT a.k AS l, CAST(b._id AS BIGINT) AS r FROM (SELECT CAST(_id AS BIGINT) AS k FROM things ORDER BY k LIMIT 3) a {s} JOIN things b ON a.k = CAST(b._id AS BIGINT) AND CAST(b._id AS BIGINT) < 2 ORDER BY r, l", .{kind});
        defer a.free(text);
        var compiled = try compiler.compile(a, text, .{});
        defer compiled.deinit();
        var baseline: TestBackend = .{ .row_count = 10 };
        var planned: TestBackend = .{ .row_count = 10, .estimate_scans = true };
        var before = try execute(a, baseline.coordinated(), &compiled, &.{}, .{});
        defer before.deinit();
        var after = try execute(a, planned.coordinated(), &compiled, &.{}, .{});
        defer after.deinit();
        try std.testing.expectEqualDeep(before.output.rows, after.output.rows);
        try std.testing.expectEqualDeep(before.output.sql_nulls, after.output.sql_nulls);
    }
}

test "SQL nested blocking results drain within the shared statement budget" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "SELECT COUNT(*) FROM (SELECT _id FROM things ORDER BY _id) AS q",
        "WITH q AS (SELECT _id FROM things ORDER BY _id DESC) SELECT COUNT(*) FROM q",
        "SELECT COUNT(*) FROM (SELECT _id, COUNT(*) AS n FROM things GROUP BY _id) AS q",
        "SELECT COUNT(*) FROM (SELECT _id, ROW_NUMBER() OVER (ORDER BY _id) AS n FROM things) AS q",
    }) |sql| {
        var fixture: TestBackend = .{ .row_count = 5000 };
        var backend = fixture.coordinated();
        backend.execution_io = std.testing.io;
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        // Track statement admission without retaining debug-allocator quarantine for thousands of spill frames.
        var result = try execute(std.heap.page_allocator, backend, &compiled, &.{}, .{ .retained_bytes = 256 * 1024, .page_rows = 16 });
        defer result.deinit();
        try std.testing.expectEqualStrings("5000", result.output.rows[0][0].string);
        try std.testing.expect(result.state.budget.peak <= 256 * 1024);
        try std.testing.expectEqual(fixture.statement_opens, fixture.statement_closes);
    }
}

test "SQL metadata counts enforce BIGINT bounds for simple and grouped outputs" {
    const Metadata = struct {
        fn open(ptr: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !?catalog.Cursor {
            return .{ .ptr = ptr, .next = next, .close = close, .count_rows = count };
        }
        fn next(_: *anyopaque, _: std.mem.Allocator, _: u32) !catalog.Page {
            return error.UnexpectedRowScan;
        }
        fn close(_: *anyopaque) void {}
        fn count(ptr: *anyopaque) !?u64 {
            const fixture: *TestBackend = @ptrCast(@alignCast(ptr));
            return fixture.metadata_count;
        }
    };
    const a = std.testing.allocator;
    for ([_][]const u8{ "SELECT COUNT(*) FROM things", "SELECT COUNT(*), COUNT(*) FROM things" }) |sql| {
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        var fixture: TestBackend = .{ .metadata_count = @as(u64, std.math.maxInt(i64)) + 1 };
        var backend = fixture.iface();
        var vtable = backend.vtable.*;
        vtable.open_scan = Metadata.open;
        backend.vtable = &vtable;
        try std.testing.expectError(error.SqlNumericOutOfRange, execute(a, backend, &compiled, &.{}, .{}));
        try std.testing.expectEqual(@as(usize, 0), fixture.pages);
        try std.testing.expectEqual(fixture.statement_opens, fixture.statement_closes);
        fixture.metadata_count = std.math.maxInt(i64);
        var result = try execute(a, backend, &compiled, &.{}, .{});
        defer result.deinit();
        for (result.output.rows[0]) |value| try std.testing.expectEqualStrings("9223372036854775807", value.string);
        try std.testing.expectEqual(@as(usize, 0), fixture.pages);
    }
    const grouped = try operators.Grouped.create(a, &.{.{ .kind = .count }}, .{});
    defer grouped.deinit();
    try grouped.ensureGlobalGroup();
    try std.testing.expectError(error.SqlNumericOutOfRange, grouped.addGlobalCount(@as(u64, std.math.maxInt(i64)) + 1));
    const invalid: operators.Aggregate = .{ .alloc = a, .input_type = .integer, .kind = .count, .count = @as(u64, std.math.maxInt(i64)) + 1 };
    try std.testing.expectError(error.SqlNumericOutOfRange, invalid.finish());
}

test "SQL nested blocking result ownership unwinds allocation failures" {
    const Scenario = struct {
        fn run(a: std.mem.Allocator) !void {
            var fixture: TestBackend = .{ .row_count = 5 };
            var backend = fixture.coordinated();
            backend.execution_io = std.testing.io;
            var compiled = try compiler.compile(a, "SELECT COUNT(*) FROM (SELECT _id FROM things ORDER BY _id) q", .{});
            defer compiled.deinit();
            var result = try execute(a, backend, &compiled, &.{}, .{ .page_rows = 2 });
            defer result.deinit();
            try std.testing.expectEqualStrings("5", result.output.rows[0][0].string);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Scenario.run, .{});
}

test "lake SQL joins negotiate exact key spooling and consume native column batches" {
    const Native = struct {
        var installs: usize = 0;
        var exact: bool = false;
        fn wants(_: *anyopaque, _: []const @import("dynamic_filter.zig").Column) !bool {
            return exact;
        }
        fn install(_: *anyopaque, filter: *const @import("dynamic_filter.zig").Filter) !bool {
            try std.testing.expect(filter.sealed);
            try std.testing.expectEqual(exact, filter.spill_manager != null);
            try std.testing.expectEqual(exact, filter.hasExact());
            installs += 1;
            return true;
        }
        fn rows(_: *anyopaque, _: std.mem.Allocator, _: u32) !catalog.Page {
            return error.UnexpectedJsonCursor;
        }
        fn columns(raw: *anyopaque, a: std.mem.Allocator, limit: u32) !catalog.ColumnPage {
            const state: *TestBackend.Statement.State = @ptrCast(@alignCast(raw));
            const total = state.owner.backend.row_count;
            const first = if (state.after) |key| (try std.fmt.parseInt(usize, key, 10)) + 1 else 0;
            const count = @min(limit, total - first);
            const refs = try a.alloc(@import("../storage/rowsource/types.zig").RowRef, count);
            const values = try a.alloc(i64, count);
            const selection = try a.alloc(usize, count);
            for (refs, values, selection, first..) |*ref, *value, *selected, index| {
                ref.* = .{ .relational_key = try std.fmt.allocPrint(a, "{d}", .{index}) };
                value.* = @intCast(index);
                selected.* = index - first;
            }
            if (state.after) |key| state.owner.alloc.free(key);
            state.after = if (first + count < total) try std.fmt.allocPrint(state.owner.alloc, "{d}", .{first + count - 1}) else null;
            const vectors = try a.alloc(@import("../storage/rowsource/types.zig").ColumnVector, 1);
            vectors[0] = .{ .name = "id", .values = .{ .i64 = values } };
            return .{ .batch = .{ .snapshot = .{ .table_id = "things", .snapshot_id = "pinned" }, .row_refs = refs, .columns = vectors }, .selection = selection, .after = state.after };
        }
        fn open(raw: *anyopaque, a: std.mem.Allocator, scans: []const catalog.StatementScan) !catalog.StatementRead {
            const statement = try TestBackend.openStatement(raw, a, scans);
            const owner: *TestBackend.Statement = @ptrCast(@alignCast(statement.ptr));
            for (owner.cursors) |*cursor| {
                cursor.next = rows;
                cursor.next_columns = columns;
                cursor.set_dynamic_filter = install;
                cursor.wants_exact_filter = wants;
            }
            return statement;
        }
    };
    for ([_]bool{ false, true }) |exact| {
        Native.exact = exact;
        Native.installs = 0;
        var fixture: TestBackend = .{ .row_count = 5000 };
        var backend = fixture.coordinated();
        backend.execution_io = std.testing.io;
        var vtable = backend.vtable.*;
        vtable.open_statement = Native.open;
        backend.vtable = &vtable;
        var compiled = try compiler.compile(std.testing.allocator, "SELECT count(*), sum(l.id) FROM things l JOIN things r ON l.id=r.id", .{});
        defer compiled.deinit();
        var result = try execute(std.testing.allocator, backend, &compiled, &.{}, .{ .page_rows = 7, .execution_batch_rows = 1024 });
        defer result.deinit();
        try std.testing.expectEqualStrings("5000", result.output.rows[0][0].string);
        try std.testing.expectEqualStrings("12497500", result.output.rows[0][1].string);
        try std.testing.expectEqual(@as(usize, 0), fixture.pages);
        try std.testing.expectEqual(@as(usize, 1), Native.installs);
        try std.testing.expectEqual(fixture.statement_opens, fixture.statement_closes);
    }
}

test "SQL batched join defers probe expression errors beyond a satisfied limit" {
    const a = std.testing.allocator;
    var fixture: TestBackend = .{ .row_count = 3 };
    var compiled = try compiler.compile(a, "SELECT a._id FROM things a JOIN things b ON (1 / (1 - CAST(a._id AS BIGINT))) = CAST(b._id AS BIGINT) LIMIT 1", .{});
    defer compiled.deinit();
    var result = try execute(a, fixture.coordinated(), &compiled, &.{}, .{ .page_rows = 1 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
    try std.testing.expectEqualStrings("0", result.output.rows[0][0].string);
    var all = try compiler.compile(a, "SELECT a._id FROM things a JOIN things b ON (1 / (1 - CAST(a._id AS BIGINT))) = CAST(b._id AS BIGINT)", .{});
    defer all.deinit();
    try std.testing.expectError(error.SqlDivisionByZero, execute(a, fixture.coordinated(), &all, &.{}, .{ .page_rows = 1 }));
}

test "SQL review regression nested probe projection preserves satisfied limit" {
    const a = std.testing.allocator;
    var fixture: TestBackend = .{ .row_count = 3 };
    var compiled = try compiler.compile(a, "SELECT a.k FROM (SELECT 1 / (1 - CAST(_id AS BIGINT)) AS k FROM things) a JOIN things b ON a.k = CAST(b._id AS BIGINT) LIMIT 1", .{});
    defer compiled.deinit();
    var result = try execute(a, fixture.coordinated(), &compiled, &.{}, .{ .page_rows = 1 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
    try std.testing.expectEqualStrings("1", result.output.rows[0][0].string);
}

test "external lake SQL native aggregate materialization retains projection and fails closed after selection" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var base: TestBackend = .{};
    const table = try TestBackend.resolve(&base, scratch, .{ .table = "docs" }, .read);
    var reference = try compiler.compile(a, "SELECT SUM(id), COUNT(*) FROM docs", .{});
    defer reference.deinit();
    const bound = try @import("aggregate_binding.zig").bind(scratch, table, reference.statement.select, &.{});
    const recipe = (try @import("aggregate_materialization.zig").fromBound(scratch, table, bound)).?;
    const source = try operators.Grouped.create(a, bound.specs, .{});
    defer source.deinit();
    for (0..2) |_| try source.add(&.{}, &.{ Datum.json(.{ .integer = 9007199254740993 }), Datum.json(.{ .integer = 1 }) });
    const partials = try scratch.alloc(operators.GroupResult, 1);
    partials[0] = (try source.nextPartialResult(scratch)).?;
    const Driver = struct {
        const Owner = @This();
        base: TestBackend = .{},
        recipe: @import("aggregate_materialization.zig").Recipe,
        partials: []const operators.GroupResult,
        opened: usize = 0,
        closed: usize = 0,
        fail: bool = false,
        fn from(raw: *anyopaque) *@This() {
            return @ptrCast(@alignCast(raw));
        }
        fn resolve(raw: *anyopaque, alloc: std.mem.Allocator, name: ast.Name, action: catalog.Action) !catalog.Table {
            return TestBackend.resolve(&from(raw).base, alloc, name, action);
        }
        fn scan(raw: *anyopaque, alloc: std.mem.Allocator, definition: catalog.Table, request: catalog.Scan) !catalog.Page {
            return TestBackend.scan(&from(raw).base, alloc, definition, request);
        }
        fn mutate(_: *anyopaque, _: std.mem.Allocator, _: std.mem.Allocator, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
            return error.TestUnexpectedResult;
        }
        fn checkpoint(raw: *anyopaque) !void {
            try TestBackend.checkpoint(&from(raw).base);
        }
        fn load(raw: *anyopaque, alloc: std.mem.Allocator, _: catalog.Table, requested: @import("aggregate_materialization.zig").Recipe) !?catalog.AggregatePartialCursor {
            const self = from(raw);
            if (!self.recipe.eql(requested)) return null;
            const state = try alloc.create(State);
            state.* = .{ .owner = self, .a = alloc };
            self.opened += 1;
            return .{ .ptr = state, .next = State.next, .close = State.close };
        }
        const State = struct {
            owner: *Owner,
            a: std.mem.Allocator,
            done: bool = false,
            fn next(raw: *anyopaque, _: std.mem.Allocator, _: u32) !?[]const operators.GroupResult {
                const self: *@This() = @ptrCast(@alignCast(raw));
                if (self.owner.fail) return error.ArtifactChecksumMismatch;
                if (self.done) return null;
                self.done = true;
                return self.owner.partials;
            }
            fn close(raw: *anyopaque) void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.owner.closed += 1;
                self.a.destroy(self);
            }
        };
        fn backend(self: *@This()) catalog.Backend {
            return .{ .ptr = self, .pinned_statement_snapshot = true, .vtable = &.{ .resolve = resolve, .scan = scan, .mutate = mutate, .checkpoint = checkpoint, .aggregate_partials = load } };
        }
    };
    var driver: Driver = .{ .recipe = recipe, .partials = partials };
    {
        const repeated: [1024]operators.GroupResult = @splat(partials[0]);
        driver.partials = &repeated;
        var counting = std.testing.FailingAllocator.init(a, .{});
        var result = try execute(counting.allocator(), driver.backend(), &reference, &.{}, .{});
        defer result.deinit();
        const expected = try std.fmt.allocPrint(a, "{d}", .{@as(i128, 9007199254740993) * 2048});
        defer a.free(expected);
        try std.testing.expectEqualStrings(expected, result.output.rows[0][0].string);
        try std.testing.expectEqualStrings("2048", result.output.rows[0][1].string);
        try std.testing.expectEqual(@as(usize, 0), driver.base.pages);
        try std.testing.expectEqual(@as(usize, 1), driver.opened);
        try std.testing.expectEqual(@as(usize, 1), driver.closed);
        try std.testing.expect(counting.allocations < 1300);
        std.debug.print("MATERIALIZED_PARTIAL_SCRATCH partials=1024 allocations={d} bytes_allocated={d}\n", .{ counting.allocations, counting.allocated_bytes });
        driver.partials = partials;
        driver.opened = 0;
        driver.closed = 0;
    }
    const Sweep = struct {
        fn run(allocator: std.mem.Allocator, initial: *const Driver, compiled: *const compiler.Compiled) !void {
            var isolated = initial.*;
            // Three partials cross reset boundaries without turning an OOM
            // sweep into a quadratic stress benchmark over all 1,024 inputs.
            const repeated: [3]operators.GroupResult = @splat(initial.partials[0]);
            isolated.partials = &repeated;
            defer std.debug.assert(isolated.opened == isolated.closed);
            var result = try execute(allocator, isolated.backend(), compiled, &.{}, .{});
            defer result.deinit();
            try std.testing.expectEqualStrings("6", result.output.rows[0][1].string);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Sweep.run, .{ &driver, &reference });
    for ([_][]const u8{
        "SELECT SUM(id) AS total, COUNT(*) AS n FROM docs HAVING COUNT(*) > 0 ORDER BY SUM(id) DESC LIMIT 1",
        "SELECT SUM(id), COUNT(*) FROM docs WHERE id > 0",
        "SELECT SUM(id + 1), COUNT(*) FROM docs",
    }, 0..) |sql, index| {
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        var result = try execute(a, driver.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
        try std.testing.expectEqualStrings(if (index == 2) "18014398509481988" else "18014398509481986", result.output.rows[0][0].string);
        try std.testing.expectEqualStrings("2", result.output.rows[0][1].string);
        try std.testing.expectEqual(@as(usize, 1), driver.opened);
        try std.testing.expectEqual(driver.opened, driver.closed);
        try std.testing.expectEqual(index, driver.base.pages);
    }
    const count_partials = [_]operators.GroupResult{.{ .keys = &.{}, .aggregates = partials[0].aggregates[1..], .ordinal = 0 }};
    driver.recipe = .{ .keys = &.{}, .inputs = recipe.inputs[1..] };
    driver.partials = &count_partials;
    var count_compiled = try compiler.compile(a, "SELECT COUNT(*) AS total FROM docs", .{});
    defer count_compiled.deinit();
    var count_result = try execute(a, driver.backend(), &count_compiled, &.{}, .{});
    defer count_result.deinit();
    try std.testing.expectEqualStrings("2", count_result.output.rows[0][0].string);
    try std.testing.expectEqual(@as(usize, 2), driver.opened);
    try std.testing.expectEqual(@as(usize, 2), driver.base.pages);
    // Stored groups may serve predicates and FILTERs over their direct keys.
    // Other input columns retain the row path. Empty filters preserve groups.
    var keyed = try compiler.compile(a, "SELECT id, SUM(id), COUNT(*) FROM docs GROUP BY id", .{});
    defer keyed.deinit();
    const keyed_bound = try @import("aggregate_binding.zig").bind(scratch, table, keyed.statement.select, &.{});
    driver.recipe = (try @import("aggregate_materialization.zig").fromBound(scratch, table, keyed_bound)).?;
    const keyed_source = try operators.Grouped.create(a, keyed_bound.specs, .{});
    defer keyed_source.deinit();
    for (0..2) |_| try keyed_source.add(&.{Datum.json(.{ .integer = 9007199254740993 })}, &.{ Datum.json(.{ .integer = 9007199254740993 }), Datum.json(.{ .integer = 1 }) });
    const keyed_partials = try scratch.alloc(operators.GroupResult, 1);
    keyed_partials[0] = (try keyed_source.nextPartialResult(scratch)).?;
    driver.partials = keyed_partials;
    for ([_]struct { sql: []const u8, rows: usize, sum: ?[]const u8, count: []const u8 }{
        .{ .sql = "SELECT id, SUM(id), COUNT(*) FROM docs WHERE id >= 9007199254740993 GROUP BY id", .rows = 1, .sum = "18014398509481986", .count = "2" },
        .{ .sql = "SELECT id, SUM(id) FILTER (WHERE id < 0), COUNT(*) FILTER (WHERE id > 0) FROM docs GROUP BY id", .rows = 1, .sum = null, .count = "2" },
        .{ .sql = "SELECT id, SUM(id) FILTER (WHERE NULL), COUNT(*) FILTER (WHERE NULL) FROM docs GROUP BY id", .rows = 1, .sum = null, .count = "0" },
        .{ .sql = "SELECT id, SUM(id), COUNT(*) FROM docs WHERE id < 0 GROUP BY id", .rows = 0, .sum = null, .count = "0" },
    }) |case| {
        var compiled = try compiler.compile(a, case.sql, .{});
        defer compiled.deinit();
        const before = driver.opened;
        var result = try execute(a, driver.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(before + 1, driver.opened);
        try std.testing.expectEqual(driver.opened, driver.closed);
        try std.testing.expectEqual(@as(usize, 2), driver.base.pages);
        try std.testing.expectEqual(case.rows, result.output.rows.len);
        if (case.rows != 0) {
            if (case.sum) |sum| try std.testing.expectEqualStrings(sum, result.output.rows[0][1].string) else try std.testing.expect(result.output.sql_nulls.?[0][1]);
            try std.testing.expectEqualStrings(case.count, result.output.rows[0][2].string);
        }
    }
    const before_failure = driver.opened;

    driver.recipe = recipe;
    driver.partials = partials;
    driver.fail = true;
    try std.testing.expectError(error.ArtifactChecksumMismatch, execute(a, driver.backend(), &reference, &.{}, .{}));
    try std.testing.expectEqual(before_failure + 1, driver.opened);
    try std.testing.expectEqual(driver.opened, driver.closed);
    try std.testing.expectEqual(@as(usize, 2), driver.base.pages);
}

test "SQL finalized aggregate runtime stops unordered limits after filtering and HAVING" {
    const a = std.testing.allocator;
    const Driver = struct {
        base: TestBackend = .{},
        position: usize = 0,
        emitted: usize = 0,
        closes: usize = 0,
        transferred: bool = false,
        request: catalog.FinalGroupRequest = undefined,
        fn from(raw: *anyopaque) *@This() {
            return @ptrCast(@alignCast(raw));
        }
        fn resolve(raw: *anyopaque, alloc: std.mem.Allocator, name: ast.Name, action: catalog.Action) !catalog.Table {
            return TestBackend.resolve(&from(raw).base, alloc, name, action);
        }
        fn scan(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
            return error.TestUnexpectedResult;
        }
        fn mutate(_: *anyopaque, _: std.mem.Allocator, _: std.mem.Allocator, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
            return error.TestUnexpectedResult;
        }
        fn load(raw: *anyopaque, _: std.mem.Allocator, _: catalog.Table, recipe: @import("aggregate_materialization.zig").Recipe) !?catalog.AggregatePartialCursor {
            if (recipe.keys.len != 1 or recipe.inputs.len != 1 or recipe.inputs[0].spec.kind != .count) return error.TestUnexpectedResult;
            return .{ .ptr = raw, .next = nextPartial, .close = closeCursor, .adopt_final_groups = adopt };
        }
        fn nextPartial(_: *anyopaque, _: std.mem.Allocator, _: u32) !?[]const operators.GroupResult {
            return error.TestUnexpectedResult;
        }
        fn checkpoint(raw: *anyopaque) !void {
            try TestBackend.checkpoint(&from(raw).base);
        }
        fn closeCursor(raw: *anyopaque) void {
            if (!from(raw).transferred) close(raw);
        }
        fn close(raw: *anyopaque) void {
            from(raw).closes += 1;
        }
        fn adopt(raw: *anyopaque, grouped: *operators.Grouped, request: catalog.FinalGroupRequest) !bool {
            const self = from(raw);
            self.request = request;
            try grouped.adoptFinalized(.{ .ptr = raw, .count = 10000, .exact_count = request.predicate == null, .next = next, .close = close }, request.page_limit);
            self.transferred = true;
            return true;
        }
        fn next(raw: *anyopaque, out: std.mem.Allocator, maximum: usize, _: bool) !?operators.GroupBatch {
            const self = from(raw);
            var scratch = std.heap.ArenaAllocator.init(out);
            defer scratch.deinit();
            const keys = try out.alloc([]const Datum, maximum);
            const values = try out.alloc([]const Datum, maximum);
            const ordinals = try out.alloc(u64, maximum);
            var count: usize = 0;
            while (count < maximum and self.position < 10000) {
                const row = self.position;
                self.position += 1;
                const key = [_]Datum{Datum.json(.{ .integer = @intCast(row) })};
                _ = scratch.reset(.free_all);
                if (self.request.predicate) |predicate| if (!try predicate.matches(predicate.ptr, scratch.allocator(), &key)) continue;
                keys[count] = try out.dupe(Datum, &key);
                values[count] = try out.dupe(Datum, &.{Datum.json(.{ .integer = 1 })});
                ordinals[count] = row;
                count += 1;
            }
            if (count == 0) return null;
            self.emitted += count;
            return .{ .keys = .{ .rows = keys[0..count] }, .aggregates = .{ .rows = values[0..count] }, .ordinals = ordinals[0..count] };
        }
        fn backend(self: *@This()) catalog.Backend {
            return .{ .ptr = self, .pinned_statement_snapshot = true, .vtable = &.{ .resolve = resolve, .scan = scan, .mutate = mutate, .checkpoint = checkpoint, .aggregate_partials = load } };
        }
    };
    for ([_]struct { sql: []const u8, first: i64, rows: usize, emitted: usize }{
        .{ .sql = "SELECT id, COUNT(*) FROM docs GROUP BY id LIMIT 3 OFFSET 2", .first = 2, .rows = 3, .emitted = 5 },
        .{ .sql = "SELECT id, COUNT(*) FROM docs WHERE id >= 9950 GROUP BY id LIMIT 1", .first = 9950, .rows = 1, .emitted = 1 },
        .{ .sql = "SELECT id, COUNT(*) FROM docs GROUP BY id HAVING id >= 50 LIMIT 2 OFFSET 1", .first = 51, .rows = 2, .emitted = 56 },
        .{ .sql = "SELECT id, COUNT(*) FROM docs GROUP BY id ORDER BY id + 0 DESC LIMIT 1", .first = 9999, .rows = 1, .emitted = 10000 },
        .{ .sql = "SELECT id, COUNT(*) FROM docs WHERE id < 0 GROUP BY id LIMIT 1", .first = 0, .rows = 0, .emitted = 0 },
    }) |case| {
        var driver: Driver = .{};
        var compiled = try compiler.compile(a, case.sql, .{});
        defer compiled.deinit();
        var result = try execute(a, driver.backend(), &compiled, &.{}, .{ .execution_batch_rows = 7 });
        defer result.deinit();
        try std.testing.expectEqual(case.rows, result.output.rows.len);
        for (result.output.rows, 0..) |row, index| try std.testing.expectEqual(case.first + @as(i64, @intCast(index)), try std.fmt.parseInt(i64, row[0].string, 10));
        try std.testing.expectEqual(case.emitted, driver.emitted);
        try std.testing.expectEqual(@as(usize, 1), driver.closes);
    }

    const Sink = struct {
        rows: usize = 0,
        batches: usize = 0,
        fn row(_: *anyopaque, _: []const Datum) !void {
            return error.TestUnexpectedResult;
        }
        fn batch(raw: *anyopaque, cells: @import("execution_batch.zig").Batch) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (cells != .vectors) return error.TestUnexpectedResult;
            for (0..cells.len()) |i| {
                try std.testing.expectEqual(@as(i64, @intCast(51 + self.rows)), (try cells.cell(std.testing.allocator, i, 0)).value.integer);
                self.rows += 1;
            }
            self.batches += 1;
        }
    };
    var driver: Driver = .{};
    var sink: Sink = .{};
    var compiled = try compiler.compile(a, "SELECT id, COUNT(*) FROM docs GROUP BY id HAVING id >= 50 LIMIT 9 OFFSET 1", .{});
    defer compiled.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    _ = try runBound(a, arena.allocator(), driver.backend(), &compiled, &.{}, .{ .execution_batch_rows = 7 }, .{ .ptr = &sink, .append = Sink.row, .append_batch = Sink.batch });
    try std.testing.expectEqual(@as(usize, 9), sink.rows);
    try std.testing.expectEqual(@as(usize, 2), sink.batches);
    try std.testing.expectEqual(@as(usize, 1), driver.closes);
    var overflow: Driver = .{};
    var unbounded = try compiler.compile(a, "SELECT id, COUNT(*) FROM docs GROUP BY id", .{});
    defer unbounded.deinit();
    try std.testing.expectError(error.SqlResultTooLarge, execute(a, overflow.backend(), &unbounded, &.{}, .{ .result_rows = 3 }));
    try std.testing.expectEqual(@as(usize, 4), overflow.emitted);
    try std.testing.expectEqual(@as(usize, 1), overflow.closes);
}

test "SQL filtered LIMIT hints require a complete bound predicate and remain advisory" {
    const cases = [_]struct { sql: []const u8, goal: ?u64 }{
        .{ .sql = "SELECT id FROM things WHERE id >= 9007199254740993 LIMIT 2 OFFSET 1", .goal = 3 },
        .{ .sql = "SELECT id FROM things WHERE id >= 9007199254740993 AND id < 9007199254740994 LIMIT 2 OFFSET 1", .goal = 3 },
        .{ .sql = "SELECT id FROM things WHERE id = 9007199254740993 OR id = 0 LIMIT 2 OFFSET 1", .goal = null },
        .{ .sql = "SELECT id FROM things WHERE _id LIKE '%' LIMIT 2 OFFSET 1", .goal = null },
        .{ .sql = "SELECT id FROM things WHERE id + 1 > 0 LIMIT 2 OFFSET 1", .goal = null },
    };
    for (cases) |case| {
        var backend: TestBackend = .{ .row_count = 19 };
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var result = try execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 2), result.output.rows.len);
        try std.testing.expectEqual(case.goal, backend.row_goal);
    }
}

test "SQL special floats retain PostgreSQL comparison ordering" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "SELECT 'NaN'::real = 'NaN'::real",
        "SELECT 'NaN'::real > 'Infinity'::double precision",
        "SELECT '-Infinity'::real < 1::integer",
        "SELECT 1::bigint < 'NaN'::double precision",
        "SELECT 'NaN'::real IS NOT DISTINCT FROM 'NaN'::double precision",
        "SELECT 'NaN'::numeric IN ('NaN'::real)",
        "SELECT NOT ('NaN'::real NOT IN ('NaN'::real))",
        "SELECT 'NaN'::real = ANY(ARRAY['NaN'::double precision])",
        "SELECT 'Infinity'::real > ALL(ARRAY[1::integer,2::integer])",

        "SELECT 'NaN'::real IN ('NaN'::real)",
        "SELECT 'Infinity'::double precision IN ('Infinity'::double precision)",
    }) |sql| {
        var fixture: TestBackend = .{ .row_count = 0 };
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        var result = try execute(a, fixture.iface(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expect(result.output.rows[0][0].bool);
    }
}

test "SQL special floats survive derived row coercion" {
    const a = std.testing.allocator;
    for ([_]struct { sql: []const u8, expected: []const u8 }{
        .{ .sql = "SELECT to_jsonb(x) FROM (VALUES ('NaN'::real)) t(x)", .expected = "NaN" },
        .{ .sql = "SELECT to_jsonb(x) FROM (VALUES ('Infinity'::double precision)) t(x)", .expected = "Infinity" },
        .{ .sql = "SELECT to_jsonb(x) FROM (VALUES ('-Infinity'::real)) t(x)", .expected = "-Infinity" },
        .{ .sql = "WITH t(x) AS (SELECT 'NaN'::double precision) SELECT to_jsonb(x) FROM t", .expected = "NaN" },
    }) |case| {
        var fixture: TestBackend = .{ .row_count = 0 };
        var compiled = try compiler.compile(a, case.sql, .{});
        defer compiled.deinit();
        var result = try execute(a, fixture.iface(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqualStrings(case.expected, result.output.rows[0][0].string);
    }
}

test "SQL special floats sort group and deduplicate derived rows" {
    const a = std.testing.allocator;
    var fixture: TestBackend = .{ .row_count = 0 };
    var grouped = try compiler.compile(a, "SELECT x::text,count(*) FROM (VALUES ('NaN'::real),('NaN'::real),('Infinity'::real),('-Infinity'::real),(1::real)) t(x) GROUP BY x ORDER BY x", .{});
    defer grouped.deinit();
    var result = try execute(a, fixture.iface(), &grouped, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 4), result.output.rows.len);
    for (result.output.rows, [_][]const u8{ "-Infinity", "1", "Infinity", "NaN" }, [_][]const u8{ "1", "1", "1", "2" }) |row, value, count| {
        try std.testing.expectEqualStrings(value, row[0].string);
        try std.testing.expectEqualStrings(count, row[1].string);
    }
    var distinct = try compiler.compile(a, "SELECT count(DISTINCT x),to_jsonb(min(x)),to_jsonb(max(x)) FROM (VALUES ('NaN'::double precision),('NaN'::double precision),('-Infinity'::double precision),(1::double precision)) t(x)", .{});
    defer distinct.deinit();
    var aggregate = try execute(a, fixture.iface(), &distinct, &.{}, .{});
    defer aggregate.deinit();
    try std.testing.expectEqualStrings("3", aggregate.output.rows[0][0].string);
    try std.testing.expectEqualStrings("-Infinity", aggregate.output.rows[0][1].string);
    try std.testing.expectEqualStrings("NaN", aggregate.output.rows[0][2].string);
    var comparisons = try compiler.compile(a, "SELECT x = 'NaN'::double precision FROM (VALUES ('NaN'::double precision),('Infinity'::double precision),(1::double precision),('-Infinity'::double precision)) t(x)", .{});
    defer comparisons.deinit();
    var compared = try execute(a, fixture.iface(), &comparisons, &.{}, .{});
    defer compared.deinit();
    for (compared.output.rows, [_]bool{ true, false, false, false }) |row, expected| try std.testing.expectEqual(expected, row[0].bool);
}

test "SQL special floats preserve prepared derived values on allocation failures" {
    const Harness = struct {
        fn run(a: std.mem.Allocator, compiled: *const compiler.Compiled) !void {
            var fixture: TestBackend = .{ .row_count = 0 };
            var backend = fixture.iface();
            backend.parameter_descriptor_hints = &.{.{ .kind = .number, .element_type = .float64 }};
            var result = try execute(a, backend, compiled, &.{.{ .float = std.math.nan(f64) }}, .{});
            defer result.deinit();
            try std.testing.expectEqualStrings("NaN", result.output.rows[0][0].string);
            try std.testing.expect(result.output.rows[0][1].bool);
        }
    };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT to_jsonb(x),x IN ('NaN'::double precision) FROM (VALUES ($1)) t(x)", .{});
    defer compiled.deinit();
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{&compiled});
}

test "SQL floating special values survive ordinary and window reducers" {
    const a = std.testing.allocator;
    for ([_]struct { sql: []const u8, expected: []const u8 }{
        .{ .sql = "SELECT to_jsonb(sum(x)) FROM (VALUES ('Infinity'::real),(1::real)) t(x)", .expected = "Infinity" },
        .{ .sql = "SELECT to_jsonb(avg(x)) FROM (VALUES ('Infinity'::real),('Infinity'::real),(1::real)) t(x)", .expected = "Infinity" },
        .{ .sql = "SELECT to_jsonb(sum(x)) FROM (VALUES ('Infinity'::real),('-Infinity'::real)) t(x)", .expected = "NaN" },
        .{ .sql = "SELECT to_jsonb(sum(x)) FROM (VALUES (1::double precision),('Infinity'::double precision),(2::double precision)) t(x)", .expected = "Infinity" },
        .{ .sql = "SELECT to_jsonb(avg(x) OVER ()) FROM (VALUES ('Infinity'::double precision),('Infinity'::double precision)) t(x)", .expected = "Infinity" },
        .{ .sql = "SELECT to_jsonb(sum(x) OVER ()) FROM (VALUES ('Infinity'::real),('-Infinity'::real)) t(x)", .expected = "NaN" },
        .{ .sql = "SELECT to_jsonb(avg(x) OVER ()) FROM (VALUES ('Infinity'::real),('-Infinity'::real)) t(x)", .expected = "NaN" },

        .{ .sql = "SELECT to_jsonb(avg(x)) FROM (VALUES ('NaN'::double precision),(1::double precision)) t(x)", .expected = "NaN" },
        .{ .sql = "SELECT to_jsonb(sum(x)) FROM (VALUES ('Infinity'::double precision),('-Infinity'::double precision)) t(x)", .expected = "NaN" },
        .{ .sql = "SELECT to_jsonb(avg(x) OVER ()) FROM (VALUES ('Infinity'::double precision),(1::double precision)) t(x)", .expected = "Infinity" },
        .{ .sql = "SELECT to_jsonb(sum(x) OVER ()) FROM (VALUES ('NaN'::double precision),(1::double precision)) t(x)", .expected = "NaN" },
    }) |case| {
        var fixture: TestBackend = .{ .row_count = 0 };
        var compiled = try compiler.compile(a, case.sql, .{});
        defer compiled.deinit();
        var result = try execute(a, fixture.iface(), &compiled, &.{}, .{});
        defer result.deinit();
        for (result.output.rows) |row| try std.testing.expectEqualStrings(case.expected, row[0].string);
    }
}

test "SQL floating special values propagate through arithmetic and functions" {
    const a = std.testing.allocator;
    for ([_]struct { sql: []const u8, expected: []const u8 }{
        .{ .sql = "SELECT to_jsonb('Infinity'::real + 1::real)", .expected = "Infinity" },
        .{ .sql = "SELECT to_jsonb('Infinity'::double precision - 'Infinity'::double precision)", .expected = "NaN" },
        .{ .sql = "SELECT to_jsonb(-('Infinity'::real))", .expected = "-Infinity" },
        .{ .sql = "SELECT to_jsonb(abs('NaN'::real))", .expected = "NaN" },
        .{ .sql = "SELECT to_jsonb(floor('Infinity'::double precision))", .expected = "Infinity" },
        .{ .sql = "SELECT to_jsonb('Infinity'::real * 0::real)", .expected = "NaN" },
        .{ .sql = "SELECT to_jsonb('Infinity'::double precision / 'Infinity'::double precision)", .expected = "NaN" },
        .{ .sql = "SELECT to_jsonb('NaN'::double precision / 0::double precision)", .expected = "NaN" },
        .{ .sql = "SELECT to_jsonb(round('NaN'::double precision))", .expected = "NaN" },
        .{ .sql = "SELECT to_jsonb(ceil('-Infinity'::real))", .expected = "-Infinity" },
        .{ .sql = "SELECT to_jsonb(trunc('Infinity'::real))", .expected = "Infinity" },
        .{ .sql = "SELECT to_jsonb(power('Infinity'::double precision,2))", .expected = "Infinity" },
    }) |case| {
        var fixture: TestBackend = .{ .row_count = 0 };
        var compiled = try compiler.compile(a, case.sql, .{});
        defer compiled.deinit();
        var result = try execute(a, fixture.iface(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqualStrings(case.expected, result.output.rows[0][0].string);
    }
}

test "SQL floating special values do not weaken finite overflow or division errors" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "SELECT '1e308'::double precision + '1e308'::double precision",
        "SELECT sum(x) FROM (VALUES ('3e38'::real),('3e38'::real)) t(x)",
        "SELECT sum(x) FROM (VALUES ('1e308'::double precision),('1e308'::double precision)) t(x)",
    }) |sql| {
        var fixture: TestBackend = .{ .row_count = 0 };
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(error.SqlNumericOutOfRange, execute(a, fixture.iface(), &compiled, &.{}, .{}));
    }
    var fixture: TestBackend = .{ .row_count = 0 };
    var compiled = try compiler.compile(a, "SELECT 'Infinity'::double precision / 0::double precision", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.SqlDivisionByZero, execute(a, fixture.iface(), &compiled, &.{}, .{}));
}

test "SQL floating special values leave moving frames when their rows leave" {
    const a = std.testing.allocator;
    var fixture: TestBackend = .{ .row_count = 0 };
    var compiled = try compiler.compile(a, "SELECT to_jsonb(sum(x) OVER (ORDER BY k ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)),to_jsonb(avg(x) OVER (ORDER BY k ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)) FROM (VALUES (1,'Infinity'::double precision),(2,1::double precision),(3,2::double precision)) t(k,x) ORDER BY k", .{});
    defer compiled.deinit();
    var result = try execute(a, fixture.iface(), &compiled, &.{}, .{});
    defer result.deinit();
    for (result.output.rows[0..2]) |row| for (row) |cell| try std.testing.expectEqualStrings("Infinity", cell.string);
    for (result.output.rows[2], [_][]const u8{ "3", "1.5" }) |value, expected| {
        const actual = try std.json.Stringify.valueAlloc(a, value, .{});
        defer a.free(actual);
        try std.testing.expectEqualStrings(expected, actual);
    }
}

test "SQL floating RANGE frames order NaN peers in memory and spill" {
    const a = std.testing.allocator;
    for ([_][]const u8{ "real", "double precision" }) |kind| for ([_]bool{ false, true }) |descending| for ([_]bool{ false, true }) |spilled| {
        const sql = try std.fmt.allocPrint(
            a,
            "SELECT count(*) OVER (ORDER BY x {s} RANGE BETWEEN 1 PRECEDING AND CURRENT ROW), " ++
                "count(*) OVER (ORDER BY x {s} RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) " ++
                "FROM (VALUES ('-Infinity'::{s}),(1),(2),('Infinity'),('NaN'),('NaN'),(NULL)) t(x) ORDER BY x {s}",
            .{ if (descending) "DESC" else "ASC", if (descending) "DESC" else "ASC", kind, if (descending) "DESC" else "ASC" },
        );
        defer a.free(sql);
        var fixture: TestBackend = .{ .row_count = 0 };
        var backend = fixture.iface();
        var manager: @import("spill.zig").Manager = .{ .alloc = a, .io = std.testing.io, .context = &fixture, .checkpoint = backend.vtable.checkpoint, .async_writes = false };
        defer manager.deinit();
        if (spilled) backend.spill_manager = &manager;
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        var result = try execute(a, backend, &compiled, &.{}, .{ .page_rows = if (spilled) 1 else 256, .retained_bytes = 256 * 1024 });
        defer result.deinit();
        if (spilled) try std.testing.expect(manager.written_bytes > 0);
        const preceding: []const []const u8 = if (descending) &.{ "1", "2", "2", "1", "1", "2", "1" } else &.{ "1", "1", "2", "1", "2", "2", "1" };
        const following: []const []const u8 = if (descending) &.{ "1", "2", "2", "1", "2", "1", "1" } else &.{ "1", "2", "1", "1", "2", "2", "1" };
        try std.testing.expectEqual(preceding.len, result.output.rows.len);
        for (result.output.rows, preceding, following) |row, before, after| {
            try std.testing.expectEqualStrings(before, row[0].string);
            try std.testing.expectEqualStrings(after, row[1].string);
        }
    };
}

test "SQL floating special values unwind prepared reducer allocation failures" {
    const Harness = struct {
        fn run(a: std.mem.Allocator, compiled: *const compiler.Compiled) !void {
            var fixture: TestBackend = .{ .row_count = 0 };
            var backend = fixture.iface();
            backend.parameter_descriptor_hints = &.{.{ .kind = .number, .element_type = .float64 }};
            var result = try execute(a, backend, compiled, &.{.{ .float = std.math.nan(f64) }}, .{});
            defer result.deinit();
            for (result.output.rows[0]) |value| try std.testing.expectEqualStrings("NaN", value.string);
        }
    };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT to_jsonb(sum(x)),to_jsonb(avg(x)),to_jsonb(abs(max(x))) FROM (VALUES ($1),(1::double precision)) t(x)", .{});
    defer compiled.deinit();
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{&compiled});
}

test "SQL floating arithmetic rejects underflow in literal and prepared expressions" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "SELECT '1e-300'::double precision * '1e-300'::double precision",
        "SELECT '-1e-300'::double precision * '1e-300'::double precision",
        "SELECT '1e-300'::double precision / '1e300'::double precision",
        "SELECT power('1e-300'::double precision,2::double precision)",
        "SELECT '1e-30'::real * '1e-30'::real",
        "SELECT '1e-30'::real / '1e30'::real",
    }) |sql| {
        var fixture: TestBackend = .{ .row_count = 0 };
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(error.SqlNumericOutOfRange, execute(a, fixture.iface(), &compiled, &.{}, .{}));
    }
    for ([_]struct { sql: []const u8, right: f64 }{
        .{ .sql = "SELECT $1 * $2", .right = 1e-300 },
        .{ .sql = "SELECT $1 / $2", .right = 1e300 },
        .{ .sql = "SELECT power($1,$2)", .right = 2 },
    }) |case| {
        var fixture: TestBackend = .{ .row_count = 0 };
        var backend = fixture.iface();
        backend.parameter_descriptor_hints = &.{ .{ .kind = .number, .element_type = .float64 }, .{ .kind = .number, .element_type = .float64 } };
        var compiled = try compiler.compile(a, case.sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(error.SqlNumericOutOfRange, execute(a, backend, &compiled, &.{ .{ .float = 1e-300 }, .{ .float = case.right } }, .{}));
    }
}

test "SQL floating POWER rejects invalid domains with PostgreSQL SQLSTATE" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "SELECT power(0::double precision,-1::double precision)",
        "SELECT power(0::double precision,'-Infinity'::double precision)",
        "SELECT power(-1::double precision,0.5::double precision)",
        "SELECT power('-Infinity'::double precision,0.5::double precision)",
        "SELECT power('-Infinity'::double precision,-0.5::double precision)",
    }) |sql| {
        var fixture: TestBackend = .{ .row_count = 0 };
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(error.SqlInvalidPowerArgument, execute(a, fixture.iface(), &compiled, &.{}, .{}));
    }
    var fixture: TestBackend = .{ .row_count = 0 };
    var backend = fixture.iface();
    backend.parameter_descriptor_hints = &.{ .{ .kind = .number, .element_type = .float64 }, .{ .kind = .number, .element_type = .float64 } };
    var compiled = try compiler.compile(a, "SELECT power($1,$2)", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.SqlInvalidPowerArgument, execute(a, backend, &compiled, &.{ .{ .float = -std.math.inf(f64) }, .{ .float = 0.5 } }, .{}));
    try std.testing.expectEqualStrings("2201F", @import("errors.zig").describe(error.SqlInvalidPowerArgument).code);
}

test "SQL floating underflow and POWER checks preserve valid zero and special results" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "SELECT (0::double precision * '1e-300'::double precision) = 0",
        "SELECT (0::double precision / '1e300'::double precision) = 0",
        "SELECT (1::double precision / 'Infinity'::double precision) = 0",
        "SELECT ('1e-300'::double precision * '1e-10'::double precision) = '1e-310'::double precision",
        "SELECT power(0::double precision,2::double precision) = 0",
        "SELECT power('NaN'::double precision,0::double precision) = 1",
        "SELECT power(1::double precision,'NaN'::double precision) = 1",
        "SELECT power(-1::double precision,'NaN'::double precision) = 'NaN'::double precision",
        "SELECT power('-Infinity'::double precision,-3::double precision) = 0",
        "SELECT power('-Infinity'::double precision,3::double precision) = '-Infinity'::double precision",
        "SELECT power(2::double precision,'-Infinity'::double precision) = 0",
        "SELECT power(-2::double precision,'Infinity'::double precision) = 'Infinity'::double precision",
    }) |sql| {
        var fixture: TestBackend = .{ .row_count = 0 };
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        var result = try execute(a, fixture.iface(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expect(result.output.rows[0][0].bool);
    }
}

test "SQL captured search scans bind execution parameters without mutating plans" {
    const a = std.testing.allocator;
    const Native = struct {
        fn open(raw: *anyopaque, alloc: std.mem.Allocator, scans: []const catalog.StatementScan) !catalog.StatementRead {
            try std.testing.expectEqual(@as(usize, 1), scans.len);
            const search = scans[0].request.search orelse return error.TestExpectedEqual;
            try std.testing.expectEqualStrings("body:alpha", search.request_text.?);
            try std.testing.expectEqual(@as(u32, 7), search.limit);
            return TestBackend.openStatement(raw, alloc, scans);
        }
    };
    var fixture: TestBackend = .{ .row_count = 0 };
    var backend = fixture.coordinated();
    backend.supports_search_relations = true;
    var vtable = backend.vtable.*;
    vtable.open_statement = Native.open;
    backend.vtable = &vtable;
    var compiled = try compiler.compile(a, "SELECT _id FROM antfly_search('things',$1,$2)", .{});
    defer compiled.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const bound = try describe.bind(arena.allocator(), backend, &compiled, &.{ .string, .integer });
    var capture = try @import("mutation_capture.zig").open(a, backend, bound, &.{ .{ .string = "body:alpha" }, .{ .integer = 7 } });
    defer capture.deinit();
    try std.testing.expectEqual(@as(usize, 1), (try capture.cursors(bound.relation.?)).len);
    try std.testing.expect(bound.relation.?.scans[0].request.search.?.request_text == null);
    capture.release();
    try std.testing.expectEqual(fixture.statement_opens, fixture.statement_closes);
}

test "external lake SQL exact join partial aggregation preserves fanout NULLs filters and fallback" {
    const a = std.testing.allocator;
    const from = " FROM (VALUES (1,2,true),(1,3,false),(1,NULL,NULL),(2,7,true),(NULL,99,true)) l(k,v,b) JOIN (VALUES (1,5,true),(1,6,false),(2,8,NULL),(3,9,true),(NULL,9,true)) r(k,v,b) ON l.k=r.k";
    for ([_][]const u8{
        "SELECT l.k, COUNT(*), COUNT(l.v), SUM(l.v), SUM(r.v), MIN(l.v), MAX(r.v), BOOL_AND(l.b), BOOL_OR(r.b)",
        "SELECT l.k, SUM(l.v) FILTER (WHERE l.k=1), COUNT(*) FILTER (WHERE l.k=2)",
        "SELECT l.k, SUM(l.v), COUNT(*)",
        "SELECT l.k, SUM(CAST(l.v AS DOUBLE PRECISION)), COUNT(DISTINCT r.v)",
        "SELECT l.b, COUNT(*) FILTER (WHERE l.b IS NULL), SUM(l.v) FILTER (WHERE l.b IS NOT NULL)",
    }, 0..) |select, i| {
        const tail = if (i == 2) " WHERE l.k>=2 GROUP BY l.k ORDER BY l.k" else if (i == 4) " GROUP BY l.b ORDER BY l.b" else " GROUP BY l.k ORDER BY l.k";
        const sql = try std.mem.concat(a, u8, &.{ select, from, tail });
        defer a.free(sql);
        const fallback_sql = try std.mem.concat(a, u8, &.{ select, from, " AND (l.v IS NOT DISTINCT FROM l.v)", tail });
        defer a.free(fallback_sql);
        var backend: TestBackend = .{};
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        var baseline = try compiler.compile(a, fallback_sql, .{});
        defer baseline.deinit();
        var actual = try execute(a, backend.iface(), &compiled, &.{}, .{});
        defer actual.deinit();
        var expected = try execute(a, backend.iface(), &baseline, &.{}, .{});
        defer expected.deinit();
        const actual_rows = try std.json.Stringify.valueAlloc(a, actual.output.rows, .{});
        defer a.free(actual_rows);
        const expected_rows = try std.json.Stringify.valueAlloc(a, expected.output.rows, .{});
        defer a.free(expected_rows);
        try std.testing.expectEqualStrings(expected_rows, actual_rows);
        try std.testing.expectEqualDeep(expected.output.sql_nulls, actual.output.sql_nulls);
    }
    // Native signed limbs must preserve an intermediate SUM outside i64,
    // including negative sign extension, before multiplicity is applied.
    {
        var backend: TestBackend = .{};
        var compiled = try compiler.compile(a, "SELECT COUNT(*), SUM(l.v), SUM(r.v), MIN(l.v) FROM (VALUES (1,-9223372036854775807-1),(1,-1),(1,9223372036854775807)) l(k,v) JOIN (VALUES (1,-3),(1,1)) r(k,v) ON l.k=r.k", .{});
        defer compiled.deinit();
        var result = try execute(a, backend.iface(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqualStrings("6", result.output.rows[0][0].string);
        try std.testing.expectEqualStrings("-4", result.output.rows[0][1].string);
        try std.testing.expectEqualStrings("-6", result.output.rows[0][2].string);
        try std.testing.expectEqualStrings("-9223372036854775808", result.output.rows[0][3].string);
    }
    // 24 input rows produce 144 pairs. A 64-row execution budget admits the
    // compact partial path, while expanding the pairs exceeds that budget.
    const repeated = "(VALUES (1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1))";
    const sql = try std.fmt.allocPrint(a, "SELECT COUNT(*), SUM(l.k) FROM {s} l(k) JOIN {s} r(k) ON l.k=r.k", .{ repeated, repeated });
    defer a.free(sql);
    var backend: TestBackend = .{};
    var compiled = try compiler.compile(a, sql, .{});
    defer compiled.deinit();
    var result = try execute(a, backend.iface(), &compiled, &.{}, .{ .scan_rows = 64 });
    defer result.deinit();
    try std.testing.expectEqualStrings("144", result.output.rows[0][0].string);
    try std.testing.expectEqualStrings("144", result.output.rows[0][1].string);
    const fallback = try std.mem.concat(a, u8, &.{ sql, " AND (l.k IS NOT DISTINCT FROM l.k)" });
    defer a.free(fallback);
    var baseline = try compiler.compile(a, fallback, .{});
    defer baseline.deinit();
    try std.testing.expectError(error.SqlProgramLimitExceeded, execute(a, backend.iface(), &baseline, &.{}, .{ .scan_rows = 64 }));
}

test "external lake SQL finalized join state preserves empty globals HAVING and delivery" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "SELECT COUNT(*), SUM(l.v) FROM (VALUES (1,2)) l(k,v) JOIN (VALUES (2)) r(k) ON l.k=r.k",
        "SELECT l.v, COUNT(*), SUM(r.k) FROM (VALUES (1,2),(2,2),(3,4)) l(k,v) JOIN (VALUES (1),(1),(2),(3)) r(k) ON l.k=r.k GROUP BY l.v HAVING COUNT(*)>1 ORDER BY SUM(r.k) DESC LIMIT 1",
        "SELECT l.v, COUNT(*), SUM(r.k) FROM (VALUES (1,2),(2,2),(3,4)) l(k,v) JOIN (VALUES (1),(1),(2),(3)) r(k) ON l.k=r.k WHERE l.v=2 GROUP BY l.v",
        "SELECT l.v, COUNT(*), SUM(r.k) FILTER (WHERE l.v=2) FROM (VALUES (1,2),(2,2),(3,4)) l(k,v) JOIN (VALUES (1),(1),(2),(3)) r(k) ON l.k=r.k GROUP BY l.v ORDER BY l.v",
    }) |sql| {
        const end = (std.mem.indexOf(u8, sql, "l.k=r.k") orelse unreachable) + "l.k=r.k".len;
        const fallback = try std.mem.concat(a, u8, &.{ sql[0..end], " AND (l.k IS NOT DISTINCT FROM l.k)", sql[end..] });
        defer a.free(fallback);
        var backend: TestBackend = .{};
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        var reference = try compiler.compile(a, fallback, .{});
        defer reference.deinit();
        var actual = try execute(a, backend.iface(), &compiled, &.{}, .{});
        defer actual.deinit();
        var expected = try execute(a, backend.iface(), &reference, &.{}, .{});
        defer expected.deinit();
        try std.testing.expectEqualDeep(expected.output.rows, actual.output.rows);
        try std.testing.expectEqualDeep(expected.output.sql_nulls, actual.output.sql_nulls);
    }
}

test "external lake SQL plain count consumes resident and spilled typed join partials" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "SELECT COUNT(*) FROM (VALUES (1),(1)) l(k) JOIN (VALUES (1),(1)) r(k) ON l.k=r.k",
        "SELECT COUNT(*) FROM (VALUES (NULL),(NULL)) l(k) JOIN (VALUES (NULL),(NULL)) r(k) ON l.k=r.k",
    }, 0..) |sql, index| {
        var backend: TestBackend = .{};
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        var result = try execute(a, backend.iface(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqualStrings(if (index == 0) "4" else "0", result.output.rows[0][0].string);
    }
    var source: TestBackend = .{ .row_count = 16384, .group_every = 4 };
    var backend = source.coordinated();
    backend.execution_io = std.testing.io;
    var manager: @import("spill.zig").Manager = .{ .alloc = a, .io = std.testing.io, .context = &source, .checkpoint = backend.vtable.checkpoint, .async_writes = false };
    defer manager.deinit();
    backend.spill_manager = &manager;
    var compiled = try compiler.compile(a, "SELECT COUNT(*) FROM things l JOIN things r ON l.id=r.id", .{});
    defer compiled.deinit();
    var result = try execute(a, backend, &compiled, &.{}, .{ .retained_bytes = 2 * 1024 * 1024, .scan_rows = 50000 });
    defer result.deinit();
    try std.testing.expect(manager.written_bytes > 0);
    try std.testing.expectEqualStrings("65536", result.output.rows[0][0].string);
    // Retaining every join key proves that compact pairs are already distinct
    // final groups. Keep those streaming rather than doing a second reduction.
    var keyed = try compiler.compile(a, "SELECT l.id, COUNT(*) FROM things l JOIN things r ON l.id=r.id GROUP BY l.id ORDER BY l.id LIMIT 3", .{});
    defer keyed.deinit();
    var groups = try execute(a, backend, &keyed, &.{}, .{ .retained_bytes = 2 * 1024 * 1024, .scan_rows = 50000 });
    defer groups.deinit();
    try std.testing.expectEqual(@as(usize, 3), groups.output.rows.len);
    for (groups.output.rows, 0..) |row, index| {
        var expected: [16]u8 = undefined;
        try std.testing.expectEqualStrings(try std.fmt.bufPrint(&expected, "{d}", .{index}), row[0].string);
        try std.testing.expectEqualStrings("16", row[1].string);
    }
}

test "external lake SQL join partial state spills and unwinds allocation failures" {
    var allocator_state = try @import("allocation_test_support.zig").CheckedAllocator.init();
    defer allocator_state.deinit();
    const a = allocator_state.allocator();
    var source: TestBackend = .{ .row_count = 16384, .group_every = 4 };
    var backend = source.coordinated();
    backend.execution_io = std.testing.io;
    var manager: @import("spill.zig").Manager = .{ .alloc = a, .io = std.testing.io, .context = &source, .checkpoint = backend.vtable.checkpoint, .async_writes = false };
    defer manager.deinit();
    backend.spill_manager = &manager;
    var compiled = try compiler.compile(a, "SELECT COUNT(*), MIN(l.id), MAX(r.id) FROM things l JOIN things r ON l.id=r.id", .{});
    defer compiled.deinit();
    var result = try execute(a, backend, &compiled, &.{}, .{ .retained_bytes = 2 * 1024 * 1024, .scan_rows = 50000 });
    defer result.deinit();
    try std.testing.expect(manager.written_bytes > 0);
    try std.testing.expectEqualStrings("65536", result.output.rows[0][0].string);
    try std.testing.expectEqualStrings("0", result.output.rows[0][1].string);
    try std.testing.expectEqualStrings("4095", result.output.rows[0][2].string);
    const Sweep = struct {
        fn run(allocator: std.mem.Allocator) !void {
            var test_backend: TestBackend = .{};
            var query = try compiler.compile(allocator, "SELECT COUNT(*), SUM(l.n) FROM (VALUES (1,2),(1,3)) l(k,n) JOIN (VALUES (1),(1)) r(k) ON l.k=r.k", .{});
            defer query.deinit();
            var output = try execute(allocator, test_backend.iface(), &query, &.{}, .{});
            defer output.deinit();
            try std.testing.expectEqualStrings("4", output.output.rows[0][0].string);
            try std.testing.expectEqualStrings("10", output.output.rows[0][1].string);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Sweep.run, .{});
}

test "external lake SQL join partials install scan filters before either oriented probe" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "SELECT COUNT(*), SUM(l.id) FROM things l JOIN (VALUES (9007199254740993),(9007199254740993)) r(k) ON l.id=r.k",
        "SELECT COUNT(*), SUM(r.id) FROM (VALUES (9007199254740993),(9007199254740993)) l(k) JOIN things r ON l.k=r.id",
    }) |sql| {
        var source: TestBackend = .{ .row_count = 256, .estimate_scans = true, .enable_dynamic_filters = true };
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        var result = try execute(a, source.coordinated(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqualStrings("512", result.output.rows[0][0].string);
        try std.testing.expectEqualStrings("4611686018427388416", result.output.rows[0][1].string);
        try std.testing.expectEqual(@as(usize, 1), source.dynamic_filters);
    }
}

test "external lake SQL unique join keys replay the bounded cost sample exactly once" {
    const a = std.testing.allocator;
    var source: TestBackend = .{ .row_count = 12000 };
    var compiled = try compiler.compile(a, "SELECT COUNT(*), MIN(l._id), MAX(r._id) FROM things l JOIN things r ON l._id=r._id", .{});
    defer compiled.deinit();
    var result = try execute(a, source.coordinated(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqualStrings("12000", result.output.rows[0][0].string);
    try std.testing.expectEqualStrings("0", result.output.rows[0][1].string);
    try std.testing.expectEqualStrings("9999", result.output.rows[0][2].string);
}

test "SQL DDL scratch is reclaimable under the statement cap while receipts survive" {
    const Fake = struct {
        fn ddl(_: *anyopaque, allocators: catalog.DdlAllocators, _: catalog.Ddl) !catalog.DdlOutcome {
            // A migration can validate many owners without retaining each
            // owner's scratch until the final statement result is released.
            for (0..32) |_| {
                const bytes = try allocators.scratch.alloc(u8, 1024 * 1024);
                @memset(bytes, 1);
                allocators.scratch.free(bytes);
            }
            return .{ .mutation_outcome = .committed_pending, .receipt = .{
                .database = try allocators.result.dupe(u8, "default"),
                .namespace = try allocators.result.dupe(u8, "public"),
                .table = try allocators.result.dupe(u8, "child"),
                .table_id = try allocators.result.dupe(u8, "7"),
                .schema_version = 0,
                .state = .pending,
                .fk_generation_publication_id = try allocators.result.dupe(u8, "0123456789abcdef0123456789abcdef"),
            } };
        }
        fn checkpoint(_: *anyopaque) !void {}
    };
    var compiled = try compiler.compile(std.testing.allocator, "CREATE TABLE child (id TEXT PRIMARY KEY)", .{});
    defer compiled.deinit();
    var sentinel: u8 = 0;
    var result = try execute(std.testing.allocator, .{ .ptr = &sentinel, .vtable = &.{ .resolve = undefined, .scan = undefined, .mutate = undefined, .checkpoint = Fake.checkpoint, .ddl = Fake.ddl } }, &compiled, &.{}, .{ .retained_bytes = 2 * 1024 * 1024 });
    defer result.deinit();
    try std.testing.expect(result.peakMemoryBytes() < 2 * 1024 * 1024);
    try std.testing.expectEqualStrings("child", result.output.ddl_receipt.?.table);
    try std.testing.expectEqualStrings("0123456789abcdef0123456789abcdef", result.output.ddl_receipt.?.fk_generation_publication_id.?);
}

test "external lake SQL parallel partition reducers preserve spilled count sum and groups" {
    var allocator_state = try @import("allocation_test_support.zig").CheckedAllocator.init();
    defer allocator_state.deinit();
    const a = allocator_state.allocator();
    for ([_]bool{ false, true }) |parallel| {
        var source: TestBackend = .{ .row_count = 32768, .group_every = 2 };
        var backend = source.coordinated();
        backend.execution_io = if (parallel) std.testing.io else null;
        var manager: @import("spill.zig").Manager = .{ .alloc = a, .io = std.testing.io, .context = &source, .checkpoint = backend.vtable.checkpoint, .async_writes = false };
        defer manager.deinit();
        backend.spill_manager = &manager;
        var compiled = try compiler.compile(a, "SELECT l.id, COUNT(*), SUM(l.id), MIN(l.id), MAX(r.id) FROM things l JOIN things r ON l.id=r.id GROUP BY l.id ORDER BY l.id DESC LIMIT 3", .{});
        defer compiled.deinit();
        var result = try execute(a, backend, &compiled, &.{}, .{ .retained_bytes = 16 * 1024 * 1024, .page_rows = 1024, .scan_rows = 200000 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 3), result.output.rows.len);
        for (result.output.rows, 0..) |row, index| {
            const id = 16383 - index;
            const key = try std.fmt.allocPrint(a, "{d}", .{id});
            defer a.free(key);
            const sum = try std.fmt.allocPrint(a, "{d}", .{id * 4});
            defer a.free(sum);
            try std.testing.expectEqualStrings(key, row[0].string);
            try std.testing.expectEqualStrings("4", row[1].string);
            try std.testing.expectEqualStrings(sum, row[2].string);
            try std.testing.expectEqualStrings(key, row[3].string);
            try std.testing.expectEqualStrings(key, row[4].string);
        }
        try std.testing.expect(result.state.budget.peak <= 16 * 1024 * 1024);
    }
}

// Repeated keys straddle every input page and lane boundary; grouping by a
// deterministic hash must preserve multiplicity for both join orientations.
test "external lake SQL key owned reducers preserve interleaved groups across pages" {
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |parallel| {
        var source: TestBackend = .{ .row_count = 4096, .group_modulo = 8 };
        var backend = source.coordinated();
        backend.execution_io = if (parallel) std.testing.io else null;
        var compiled = try compiler.compile(a, "SELECT l.id, COUNT(*), SUM(r.id) FROM things l JOIN things r ON l.id=r.id GROUP BY l.id ORDER BY l.id", .{});
        defer compiled.deinit();
        var result = try execute(a, backend, &compiled, &.{}, .{ .retained_bytes = 16 * 1024 * 1024, .page_rows = 257, .scan_rows = 10000 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 8), result.output.rows.len);
        for (result.output.rows, 0..) |row, id| {
            const sum = try std.fmt.allocPrint(a, "{d}", .{id * 512 * 512});
            defer a.free(sum);
            try std.testing.expectEqualStrings("262144", row[1].string);
            try std.testing.expectEqualStrings(sum, row[2].string);
        }
    }
}

// The public JSON result mode remains the oracle for scalar/null semantics;
// native transport results must retain typed payloads through blocking plans.
test "SQL native transport results retain typed arrays numeric and null provenance" {
    const a = std.testing.allocator;
    var fixture: TestBackend = .{};
    for ([_][]const u8{
        "SELECT ARRAY[1::bigint,NULL::bigint,9007199254740993::bigint] AS a, '9007199254740993.1200'::numeric AS n, 'null'::jsonb AS j, NULL::numeric AS z",
        "SELECT a,n,j,z FROM (SELECT ARRAY[1::bigint,NULL::bigint,9007199254740993::bigint] AS a, '9007199254740993.1200'::numeric AS n, 'null'::jsonb AS j, NULL::numeric AS z) q ORDER BY n",
    }) |sql| {
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        var result = try execute(a, fixture.iface(), &compiled, &.{}, .{ .native_result = true });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.output.rowCount());
        try std.testing.expectEqual(@as(usize, 0), result.output.rows.len);
        const cells = result.output.native_rows.?[0];
        try std.testing.expectEqual(@as(usize, 3), cells[0].array.?.cardinality());
        try std.testing.expect(cells[0].array.?.flatCell(1).sql_null);
        try std.testing.expectEqual(@as(i64, 9007199254740993), cells[0].array.?.flatCell(2).value.integer);
        var numeric_context: @import("numeric_value.zig").Context = .{ .alloc = a };
        const text = try @import("numeric_value.zig").format(&numeric_context, cells[1].numeric.?.*);
        defer a.free(text);
        try std.testing.expectEqualStrings("9007199254740993.1200", text);
        try std.testing.expect(!cells[2].sql_null and cells[2].value == .null);
        try std.testing.expect(cells[3].sql_null);
    }
}

test "SQL native transport mutation returning retains complete typed rows before commit" {
    const Fixture = struct {
        fn prepare(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, mutations: []const catalog.Mutation) ![]const catalog.Mutation {
            return mutations;
        }
        fn mutate(raw: *anyopaque, _: std.mem.Allocator, _: std.mem.Allocator, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
            const owner: *TestBackend = @ptrCast(@alignCast(raw));
            owner.writes += 1;
            return .committed;
        }
    };
    const a = std.testing.allocator;
    for ([_][]const u8{
        "UPDATE things SET id=id RETURNING ARRAY[id,NULL::bigint], id::numeric, 'null'::jsonb, NULL::numeric",
        "DELETE FROM things RETURNING ARRAY[id,NULL::bigint], id::numeric, 'null'::jsonb, NULL::numeric",
        "INSERT INTO things (_id,id) VALUES ('new',9007199254740993) RETURNING ARRAY[id,NULL::bigint], id::numeric, 'null'::jsonb, NULL::numeric",
        "UPDATE things AS t SET id=t.id FROM things AS s WHERE t._id=s._id RETURNING ARRAY[t.id,NULL::bigint], t.id::numeric, 'null'::jsonb, NULL::numeric",
        "MERGE INTO things t USING things s ON t._id=s._id WHEN MATCHED THEN UPDATE SET id=t.id RETURNING ARRAY[t.id,NULL::bigint], t.id::numeric, 'null'::jsonb, NULL::numeric",
    }) |sql| {
        var fixture: TestBackend = .{};
        var backend = fixture.coordinated();
        var methods = backend.vtable.*;
        methods.prepare_mutations = Fixture.prepare;
        methods.mutate = Fixture.mutate;
        methods.mutate_prepared = Fixture.mutate;
        backend.vtable = &methods;
        backend.atomic_statement_read_set = true;
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        var result = execute(a, backend, &compiled, &.{}, .{ .native_result = true }) catch |err| {
            std.debug.print("native RETURNING failed: {s}: {s}\n", .{ sql, @errorName(err) });
            return err;
        };
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), fixture.writes);
        try std.testing.expectEqual(@as(usize, 0), result.output.rows.len);
        try std.testing.expect(result.output.native_rows.?.len > 0);
        try std.testing.expect(result.output.sql_nulls == null);
        for (result.output.native_rows.?) |row| {
            try std.testing.expectEqual(@as(i64, 9007199254740993), row[0].array.?.flatCell(0).value.integer);
            try std.testing.expect(row[0].array.?.flatCell(1).sql_null);
            try std.testing.expect(row[1].numeric != null and !row[1].sql_null);
            try std.testing.expect(!row[2].sql_null and row[2].value == .null);
            try std.testing.expect(row[3].sql_null);
        }
    }
}

test "SQL native transport materialized results avoid public envelopes" {
    const a = std.testing.allocator;
    const arrays = @import("array_value.zig");
    const cells = try a.alloc(arrays.Element, 32768);
    defer a.free(cells);
    @memset(cells, arrays.Element.json(.{ .integer = 42 }));
    const array = try arrays.Value.init(.int64, &.{.{ .length = 32768, .lower = 1 }}, cells, .{});
    const Fixture = struct {
        array: *const arrays.Value,
        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: ast.Name, _: catalog.Action) !catalog.Table {
            return .{ .id = 1, .physical_name = "rows", .schema_version = 1, .columns = &.{.{ .name = "a", .path = "a", .type = .array, .element_type = .int64 }} };
        }
        fn checkpoint(_: *anyopaque) !void {}
        fn scan(raw: *anyopaque, owned: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const values = try owned.alloc(catalog.Row.Cell, 1);
            values[0] = .typedArray(self.array);
            const rows = try owned.alloc(catalog.Row, 1);
            rows[0] = .{ .id = "row", .version = 1, .value = .null, .typed_cells = .{ .layout = try catalog.Row.TypedLayout.init(owned, &.{"a"}), .values = values } };
            return .{ .rows = rows };
        }
    };
    var fixture: Fixture = .{ .array = &array };
    const backend: catalog.Backend = .{ .ptr = &fixture, .pinned_statement_snapshot = true, .vtable = &.{ .resolve = Fixture.resolve, .checkpoint = Fixture.checkpoint, .scan = Fixture.scan, .mutate = undefined } };
    var compiled = try compiler.compile(a, "SELECT a FROM rows", .{});
    defer compiled.deinit();
    var peaks: [2]usize = undefined;
    for ([_]bool{ false, true }, &peaks) |native, *peak| {
        var nr: @import("../storage/lite/test_allocator.zig").NoResizeAllocator = .{ .backing = a };
        var budget: @import("../storage/lite/test_allocator.zig").BudgetAllocator = .{ .backing = nr.allocator() };
        {
            var result = try execute(budget.allocator(), backend, &compiled, &.{}, .{ .native_result = native });
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 1), result.output.rowCount());
            peak.* = budget.peak;
            std.debug.print("SQL_RESULT native={any} backing_peak={d} retained={d} result_budget_peak={d} calls={d}\n", .{ native, budget.peak, budget.live, result.peakMemoryBytes(), budget.alloc_calls });
        }
        try std.testing.expectEqual(@as(usize, 0), budget.live);
    }
    try std.testing.expect(peaks[1] < peaks[0] / 4 * 3);
}

test "SQL native transport does not retain execution pattern callbacks" {
    const Poison = struct {
        fn next(_: *anyopaque, _: std.mem.Allocator, _: *u64) !?Datum {
            return error.UnexpectedPatternExecution;
        }
        fn close(_: *anyopaque) void {
            @panic("transport closed execution callback");
        }
    };
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var marker: u8 = 0;
    var patterns: @import("scalar.zig").PatternSet = .{ .ptr = &marker, .count = 1, .next = Poison.next, .close = Poison.close };
    const context: Context = .{ .alloc = a, .arena = arena.allocator(), .backend = undefined, .binding = undefined, .parameters = &.{}, .limits = .{ .native_result = true } };
    const row = try context.resultRow(&.{@import("scalar.zig").Datum.fromParts(.{ .string = "retained" }, false, &patterns)}, &.{.{ .name = "p", .type = .string }});
    try std.testing.expect(row.native.?[0].patterns() == null);
    try std.testing.expectEqualStrings("retained", row.native.?[0].value.string);
}

test "SQL document root reads replacements and source inserts share typed preparation" {
    const Fixture = struct {
        const Self = @This();
        const Cursor = struct {
            alloc: std.mem.Allocator,
            fixture: *Self,
            request: catalog.Scan,
            delivered: bool = false,
            fn next(raw: *anyopaque, a: std.mem.Allocator, _: u32) !catalog.Page {
                const self: *@This() = @ptrCast(@alignCast(raw));
                if (self.delivered) return .{ .rows = &.{} };
                self.delivered = true;
                return Self.scan(self.fixture, a, undefined, self.request);
            }
            fn close(raw: *anyopaque) void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.alloc.destroy(self);
            }
        };
        const Captured = struct {
            alloc: std.mem.Allocator,
            cursors: []catalog.Cursor,
            fn close(raw: *anyopaque) void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                for (self.cursors) |cursor| cursor.close(cursor.ptr);
                self.alloc.free(self.cursors);
                self.alloc.destroy(self);
            }
        };
        root: Json,
        require_narrow: bool = false,
        prepared: usize = 0,
        writes: usize = 0,
        last: ?Json = null,
        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: ast.Name, _: catalog.Action) !catalog.Table {
            return .{ .id = 1, .physical_name = "docs", .schema_version = 1, .storage_mode = .document, .columns = &.{ .{ .name = "title", .path = "title", .type = .string }, .{ .name = "metadata", .path = "metadata", .type = .json } } };
        }
        fn checkpoint(_: *anyopaque) !void {}
        fn open(raw: *anyopaque, a: std.mem.Allocator, _: catalog.Table, request: catalog.Scan) !?catalog.Cursor {
            const cursor = try a.create(Cursor);
            cursor.* = .{ .alloc = a, .fixture = @ptrCast(@alignCast(raw)), .request = request };
            return .{ .ptr = cursor, .next = Cursor.next, .close = Cursor.close };
        }
        fn capture(raw: *anyopaque, a: std.mem.Allocator, scans: []const catalog.StatementScan) !catalog.StatementRead {
            const owner = try a.create(Captured);
            errdefer a.destroy(owner);
            const cursors = try a.alloc(catalog.Cursor, scans.len);
            errdefer a.free(cursors);
            var opened: usize = 0;
            errdefer for (cursors[0..opened]) |cursor| cursor.close(cursor.ptr);
            for (scans, cursors) |input, *cursor| {
                cursor.* = (try open(raw, a, input.table, input.request)).?;
                opened += 1;
            }
            owner.* = .{ .alloc = a, .cursors = cursors };
            return .{ .ptr = owner, .cursors = cursors, .close = Captured.close };
        }
        fn scan(raw: *anyopaque, a: std.mem.Allocator, _: catalog.Table, request: catalog.Scan) !catalog.Page {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.require_narrow) for (request.fields) |field| try std.testing.expect(!std.mem.eql(u8, field, "metadata"));
            if (request.after != null) return .{ .rows = &.{} };
            const rows = try a.alloc(catalog.Row, 1);
            rows[0] = .{ .id = "doc:a", .version = 1, .value = self.root, .document = self.root, .expected_content_digest = @splat(1) };
            return .{ .rows = rows };
        }
        fn prepare(raw: *anyopaque, _: std.mem.Allocator, _: catalog.Table, mutations: []const catalog.Mutation) ![]const catalog.Mutation {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.prepared += mutations.len;
            for (mutations) |mutation| if (mutation.row) |root| {
                try std.testing.expect(root == .object);
                try std.testing.expect(!root.object.contains("_doc"));
                if (root.object.get("metadata")) |value| if (value == .null) {
                    const marked = for (mutation.json_null_fields) |field| {
                        if (std.mem.eql(u8, field, "metadata")) break true;
                    } else false;
                    try std.testing.expect(marked);
                };
            };
            return mutations;
        }
        fn mutate(raw: *anyopaque, _: std.mem.Allocator, _: std.mem.Allocator, _: catalog.Table, mutations: []const catalog.Mutation) !catalog.MutationOutcome {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.writes += mutations.len;
            if (mutations.len != 0) self.last = mutations[0].row;
            return .committed;
        }
        fn backend(self: *@This()) catalog.Backend {
            return .{ .ptr = self, .atomic_statement_read_set = true, .vtable = &.{ .resolve = resolve, .scan = scan, .open_scan = open, .open_statement = capture, .prepare_mutations = prepare, .mutate = mutate, .mutate_prepared = mutate, .checkpoint = checkpoint } };
        }
    };
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const root = try std.json.parseFromSliceLeaky(Json, a, "{\"title\":\"before\",\"unknown\":{\"exact\":9007199254740993.1250,\"null\":null}}", .{ .parse_numbers = false });
    {
        var fixture: Fixture = .{ .root = root, .require_narrow = true };
        var compiled = try compiler.compile(a, "UPDATE docs SET title='after' WHERE _id='doc:a' RETURNING title", .{});
        defer compiled.deinit();
        var result = try execute(std.testing.allocator, fixture.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), fixture.writes);
        try std.testing.expectEqualStrings("after", fixture.last.?.object.get("title").?.string);
        try std.testing.expect(fixture.last.?.object.contains("unknown"));
    }
    for ([_][]const u8{
        "SELECT _doc FROM docs WHERE _id='doc:a'",
        "WITH c AS (SELECT _doc FROM docs) SELECT _doc FROM c",
        "SELECT d._doc FROM docs AS d CROSS JOIN (VALUES(1)) AS n(v)",
    }) |sql| {
        var fixture: Fixture = .{ .root = root };
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        var result = try execute(std.testing.allocator, fixture.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
        try std.testing.expect(result.output.rows[0][0] == .object);
        try std.testing.expectEqual(@as(usize, 0), fixture.writes);
    }
    for ([_][]const u8{
        "INSERT INTO docs (_id,_doc) VALUES ('doc:b','{\"title\":\"after\"}') RETURNING _id,_doc",
        "INSERT INTO docs (_id,_doc) SELECT 'doc:b',_doc FROM docs WHERE _id='doc:a' RETURNING _doc",
        "UPDATE docs SET _doc=jsonb_set(_doc,'{title}','\"after\"',true) WHERE _id='doc:a' RETURNING _doc",
        "UPDATE docs d SET _doc='{\"title\":\"joined\"}' FROM (VALUES('doc:a')) s(id) WHERE d._id=s.id RETURNING d._doc",
        "MERGE INTO docs d USING (VALUES('doc:a')) s(id) ON d._id=s.id WHEN MATCHED THEN UPDATE SET _doc='{\"title\":\"merged\"}' RETURNING d._doc",
        "MERGE INTO docs d USING (VALUES('doc:b')) s(id) ON d._id=s.id WHEN NOT MATCHED THEN INSERT (_id,_doc) VALUES(s.id,'{\"title\":\"inserted\"}') RETURNING _doc",
        "MERGE INTO docs d USING (VALUES('doc:a')) s(id) ON d._id=s.id WHEN MATCHED THEN UPDATE SET _doc='{\"title\":\"null-merged\",\"metadata\":null}' RETURNING d._doc",
    }, [_][]const u8{ "after", "before", "after", "joined", "merged", "inserted", "null-merged" }, [_]bool{ false, true, true, false, false, false, false }) |sql, title, preserves_unknown| {
        var fixture: Fixture = .{ .root = root };
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        var result = try execute(std.testing.allocator, fixture.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), fixture.writes);
        try std.testing.expectEqual(@as(usize, 1), fixture.prepared);
        try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
        try std.testing.expect(!fixture.last.?.object.contains("_doc"));
        try std.testing.expectEqualStrings(title, fixture.last.?.object.get("title").?.string);
        const expected_fields: usize = if (preserves_unknown or std.mem.eql(u8, title, "null-merged")) 2 else 1;
        try std.testing.expectEqual(expected_fields, fixture.last.?.object.count());
        const returned = result.output.rows[0][result.output.rows[0].len - 1];
        try std.testing.expectEqualStrings(title, returned.object.get("title").?.string);
        try std.testing.expectEqual(fixture.last.?.object.count(), returned.object.count());
        if (preserves_unknown) try std.testing.expectEqualStrings("9007199254740993.1250", returned.object.get("unknown").?.object.get("exact").?.number_string);
        if (std.mem.eql(u8, title, "null-merged")) try std.testing.expect(returned.object.get("metadata").? == .null);
    }
    {
        var fixture: Fixture = .{ .root = root };
        var compiled = try compiler.compile(a, "MERGE INTO docs d USING docs s ON d._id=s._id WHEN MATCHED THEN UPDATE SET _doc='{\"title\":\"merged\"}' RETURNING d._doc,s._doc", .{});
        defer compiled.deinit();
        var result = try execute(std.testing.allocator, fixture.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), fixture.writes);
        try std.testing.expectEqualStrings("merged", fixture.last.?.object.get("title").?.string);
        try std.testing.expectEqualStrings("merged", result.output.rows[0][0].object.get("title").?.string);
        try std.testing.expectEqualStrings("before", result.output.rows[0][1].object.get("title").?.string);
        try std.testing.expectEqual(@as(usize, 2), result.output.rows[0][1].object.count());
    }
    {
        var fixture: Fixture = .{ .root = root };
        var compiled = try compiler.compile(a, "MERGE INTO docs d USING (VALUES('doc:a')) s(id) ON d._id=s.id WHEN MATCHED THEN DELETE RETURNING d._doc", .{});
        defer compiled.deinit();
        var result = try execute(std.testing.allocator, fixture.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), fixture.writes);
        try std.testing.expect(fixture.last == null);
        try std.testing.expectEqualStrings("before", result.output.rows[0][0].object.get("title").?.string);
        try std.testing.expectEqual(@as(usize, 2), result.output.rows[0][0].object.count());
    }
    for ([_]struct { sql: []const u8, failure: anyerror }{
        .{ .sql = "INSERT INTO docs (_id,_doc,title) VALUES ('doc:b','{}','mixed')", .failure = error.UnsupportedSqlShape },
        .{ .sql = "INSERT INTO docs (_id,_doc) VALUES ('doc:b','[]')", .failure = error.SqlTypeMismatch },
        .{ .sql = "INSERT INTO docs (_id,_doc) VALUES ('doc:b',NULL)", .failure = error.SqlNotNullViolation },
        .{ .sql = "INSERT INTO docs (_id,_doc) VALUES ('doc:b',DEFAULT)", .failure = error.SqlNotNullViolation },
        .{ .sql = "UPDATE docs SET _doc='{}',title='mixed' WHERE _id='doc:a'", .failure = error.UnsupportedSqlShape },
        .{ .sql = "UPDATE docs SET _doc='[]' WHERE _id='doc:a'", .failure = error.SqlTypeMismatch },
        .{ .sql = "INSERT INTO docs (_id,_doc) VALUES ('doc:a','{}') ON CONFLICT(_id) DO UPDATE SET _doc=excluded._doc,title='mixed'", .failure = error.UnsupportedSqlShape },
    }) |case| {
        var fixture: Fixture = .{ .root = root };
        var compiled = try compiler.compile(a, case.sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(case.failure, execute(std.testing.allocator, fixture.backend(), &compiled, &.{}, .{}));
        try std.testing.expectEqual(@as(usize, 0), fixture.writes);
    }
}

test "SQL native transport parameters retain arrays numeric JSON null and source ownership" {
    const Probe = struct {
        fn checkpoint(_: *anyopaque) !void {}
        fn run(a: std.mem.Allocator, leased: bool) !void {
            const inputs = try @import("parameter_frame.zig").Ingress.create(a, 1024 * 1024);
            var live = true;
            defer if (live) inputs.release();
            const owned = inputs.allocator();
            const array = try owned.create(@import("array_value.zig").Value);
            array.* = (try @import("array_text.zig").decodeLeaky(owned, .int64, "[-1:1]={9007199254740993,NULL,-7}", .{ .compact = leased })).value;
            var ctx: @import("numeric_value.zig").Context = .{ .alloc = owned };
            const number = try @import("numeric_value.zig").parse(&ctx, "9007199254740993.1200");
            const cells = [_]Datum{ .typedArray(array), .typedNumeric(&number.value), .json(.null), .{} };
            var marker: u8 = 0;
            const backend: catalog.Backend = .{ .ptr = &marker, .vtable = &.{ .resolve = undefined, .scan = undefined, .mutate = undefined, .checkpoint = checkpoint } };
            var compiled = try compiler.compile(a, "SELECT $1::bigint[], $2::numeric, $3::jsonb, $4::numeric", .{});
            defer compiled.deinit();
            var result = try execute(a, backend, &compiled, &.{ .null, .null, .null, .null }, .{ .native_result = true, .parameter_cells = &cells, .parameter_owner = if (leased) inputs.lease() else null });
            defer result.deinit();
            inputs.release();
            live = false;
            const row = result.output.native_rows.?[0];
            try std.testing.expectEqual(@as(i64, 9007199254740993), row[0].array.?.flatCell(0).value.integer);
            try std.testing.expectEqual(@as(i32, -1), row[0].array.?.dimensions[0].lower);
            try std.testing.expect(row[0].array.?.flatCell(1).sql_null);
            var context: @import("numeric_value.zig").Context = .{ .alloc = a };
            const text = try @import("numeric_value.zig").format(&context, row[1].numeric.?.*);
            defer a.free(text);
            try std.testing.expectEqualStrings("9007199254740993.1200", text);
            try std.testing.expect(row[2].value == .null and !row[2].sql_null);
            try std.testing.expect(row[3].sql_null);
        }
    };
    for ([_]bool{ false, true }) |leased| try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{leased});
}

test "SQL REAL pushed comparisons preserve declared float8 parameter operands" {
    const Fixture = struct {
        expected: f64,
        sample: f64 = @as(f32, 0.1),
        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: ast.Name, _: catalog.Action) !catalog.Table {
            return .{ .id = 1, .physical_name = "docs", .schema_version = 1, .columns = &.{.{ .name = "n", .path = "n", .type = .number, .element_type = .float32 }} };
        }
        fn scan(raw: *anyopaque, a: std.mem.Allocator, _: catalog.Table, request: catalog.Scan) !catalog.Page {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(@as(usize, 1), request.conditions.len);
            try std.testing.expectEqual(self.expected, request.conditions[0].value.float);
            const order = try @import("scalar.zig").compare(.{ .float = self.sample }, request.conditions[0].value);
            if (order != (if (request.conditions[0].op == .gt) std.math.Order.gt else .eq)) return .{ .rows = &.{} };
            const rows = try a.alloc(catalog.Row, 1);
            var object: std.json.ObjectMap = .empty;
            try object.put(a, "n", .{ .float = self.sample });
            rows[0] = .{ .id = "one", .version = 0, .value = .{ .object = object } };
            return .{ .rows = rows };
        }
        fn mutate(_: *anyopaque, _: std.mem.Allocator, _: std.mem.Allocator, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
            return error.UnsupportedSqlExecution;
        }
        fn checkpoint(_: *anyopaque) !void {}
    };
    const a = std.testing.allocator;
    var fixture: Fixture = .{ .expected = 0.10000000148 };
    const backend: catalog.Backend = .{ .ptr = &fixture, .parameter_descriptor_hints = &.{.{ .kind = .number, .element_type = .float64 }}, .vtable = &.{ .resolve = Fixture.resolve, .scan = Fixture.scan, .mutate = Fixture.mutate, .checkpoint = Fixture.checkpoint } };
    var compiled = try compiler.compile(a, "SELECT n FROM docs WHERE n > $1", .{});
    defer compiled.deinit();
    var result = try execute(a, backend, &compiled, &.{.{ .float = 0.10000000148 }}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
    // Declared REAL operands normalize to their own descriptor as well.
    fixture.expected = @as(f32, 0.1);
    var real_backend = backend;
    real_backend.parameter_descriptor_hints = &.{.{ .kind = .number, .element_type = .float32 }};
    var equality = try compiler.compile(a, "SELECT n FROM docs WHERE n = $1", .{});
    defer equality.deinit();
    var real = try execute(a, real_backend, &equality, &.{.{ .float = 0.1 }}, .{});
    defer real.deinit();
    try std.testing.expectEqual(@as(usize, 1), real.output.rows.len);
}

test "SQL finalized materialization page quota stays independent of decision output fragmentation" {
    const a = std.testing.allocator;
    const Driver = struct {
        base: TestBackend = .{},
        position: usize = 0,
        emitted: usize = 0,
        closes: usize = 0,
        transferred: bool = false,
        request: catalog.FinalGroupRequest = undefined,
        fn from(raw: *anyopaque) *@This() {
            return @ptrCast(@alignCast(raw));
        }
        fn resolve(raw: *anyopaque, alloc: std.mem.Allocator, name: ast.Name, action: catalog.Action) !catalog.Table {
            return TestBackend.resolve(&from(raw).base, alloc, name, action);
        }
        fn scan(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
            return error.TestUnexpectedResult;
        }
        fn mutate(_: *anyopaque, _: std.mem.Allocator, _: std.mem.Allocator, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
            return error.TestUnexpectedResult;
        }
        fn load(raw: *anyopaque, _: std.mem.Allocator, _: catalog.Table, recipe: @import("aggregate_materialization.zig").Recipe) !?catalog.AggregatePartialCursor {
            if (recipe.keys.len != 1 or recipe.inputs.len != 1 or recipe.inputs[0].spec.kind != .count) return error.TestUnexpectedResult;
            return .{ .ptr = raw, .next = nextPartial, .close = closeCursor, .adopt_final_groups = adopt };
        }
        fn nextPartial(_: *anyopaque, _: std.mem.Allocator, _: u32) !?[]const operators.GroupResult {
            return error.TestUnexpectedResult;
        }
        fn checkpoint(raw: *anyopaque) !void {
            try TestBackend.checkpoint(&from(raw).base);
        }
        fn closeCursor(raw: *anyopaque) void {
            if (!from(raw).transferred) close(raw);
        }
        fn close(raw: *anyopaque) void {
            from(raw).closes += 1;
        }
        fn adopt(raw: *anyopaque, grouped: *operators.Grouped, request: catalog.FinalGroupRequest) !bool {
            const self = from(raw);
            self.request = request;
            try grouped.adoptFinalized(.{ .ptr = raw, .count = 10, .exact_count = request.predicate == null, .ordered = request.selection.order.len != 0, .batch_rows = request.batch_rows, .next = next, .close = close }, request.page_limit);
            self.transferred = true;
            return true;
        }
        fn next(raw: *anyopaque, out: std.mem.Allocator, maximum: usize, _: bool) !?operators.GroupBatch {
            const self = from(raw);
            var scratch = std.heap.ArenaAllocator.init(out);
            defer scratch.deinit();
            const keys = try out.alloc([]const Datum, maximum);
            const values = try out.alloc([]const Datum, maximum);
            const ordinals = try out.alloc(u64, maximum);
            var count: usize = 0;
            while (count < maximum and self.position < 10) {
                const row = self.position;
                self.position += 1;
                const key = [_]Datum{Datum.json(.{ .integer = @intCast(row) })};
                _ = scratch.reset(.free_all);
                if (self.request.predicate) |predicate| if (!try predicate.matches(predicate.ptr, scratch.allocator(), &key)) continue;
                keys[count] = try out.dupe(Datum, &key);
                values[count] = try out.dupe(Datum, &.{Datum.json(.{ .integer = 1 })});
                ordinals[count] = row;
                count += 1;
            }
            if (count == 0) return null;
            self.emitted += count;
            return .{ .keys = .{ .rows = keys[0..count] }, .aggregates = .{ .rows = values[0..count] }, .ordinals = ordinals[0..count] };
        }
        fn backend(self: *@This()) catalog.Backend {
            return .{ .ptr = self, .pinned_statement_snapshot = true, .vtable = &.{ .resolve = resolve, .scan = scan, .mutate = mutate, .checkpoint = checkpoint, .aggregate_partials = load } };
        }
    };
    for ([_][]const u8{
        "SELECT id, COUNT(*) FROM docs GROUP BY id LIMIT 3",
        "SELECT id, COUNT(*), ai_probability(CAST(id AS TEXT),'Refund?','local') FROM docs GROUP BY id LIMIT 3",
        "SELECT id, COUNT(*), ai_probability(CAST(id AS TEXT),'Refund?','local') FROM docs GROUP BY id",
        "SELECT id, COUNT(*), ai_probability(CAST(id AS TEXT),'Refund?','local') FROM docs GROUP BY id ORDER BY id LIMIT 9",
    }) |query| {
        var driver: Driver = .{};
        var provider: @import("decision_eval.zig").testing.Provider = .{};
        var backend = driver.backend();
        backend.decision_provider = provider.provider();
        var compiled = try compiler.compile(a, query, .{});
        defer compiled.deinit();
        var result = try execute(a, backend, &compiled, &.{}, .{ .execution_batch_rows = 8, .scan_pages = 2 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, if (std.mem.indexOf(u8, query, "LIMIT") == null) 10 else if (std.mem.indexOf(u8, query, "LIMIT 9") != null) 9 else 3), result.output.rows.len);
        try std.testing.expectEqual(@as(usize, 1), driver.closes);
    }
}

test "SQL owned execution releases allocator capability after results and every allocation failure" {
    const Case = struct {
        fn release(raw: *anyopaque) void {
            const calls: *usize = @ptrCast(@alignCast(raw));
            calls.* += 1;
        }
        fn run(a: std.mem.Allocator) !void {
            var fixture: TestBackend = .{};
            var compiled = try compiler.compile(a, "SELECT 42 AS n", .{});
            defer compiled.deinit();
            var calls: usize = 0;
            defer std.debug.assert(calls == 1);
            const owner: MemoryOwner = .{ .allocator = a, .ptr = &calls, .release = release };
            var result = try executeOwned(owner, fixture.iface(), &compiled, &.{}, .{});
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 0), calls);
            try std.testing.expectEqualStrings("42", result.output.rows[0][0].string);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "SQL EXPLAIN roadmap measures one read and retains admission and cancellation" {
    const a = std.testing.allocator;
    var backend: TestBackend = .{ .row_count = 7, .estimate_scans = true };
    var iface = backend.coordinated();
    iface.execution_io = std.testing.io;
    var compiled = try compiler.compile(a, "EXPLAIN (FORMAT JSON, ANALYZE ON) SELECT id FROM things LIMIT 3", .{});
    defer compiled.deinit();
    var result = try execute(a, iface, &compiled, &.{}, .{});
    defer result.deinit();
    var plan = try std.json.parseFromSlice(Json, a, result.output.rows[0][0].string, .{});
    defer plan.deinit();
    const measured = plan.value.object.get("runtime").?.object;
    try std.testing.expectEqual(@as(usize, 1), backend.pages);
    try std.testing.expectEqual(@as(i64, 3), measured.get("result_rows").?.integer);
    try std.testing.expect(measured.get("elapsed_ns").? == .integer);
    try std.testing.expect(measured.get("retained_peak_bytes").?.integer > 0);
    try std.testing.expect(measured.get("nodes").?.array.items.len > 0);
    try std.testing.expect(measured.get("storage_read_bytes").? == .null);
    try std.testing.expectEqual(@as(usize, 0), backend.statement_opens);
    try std.testing.expectEqual(@as(usize, 0), backend.statement_closes);
    try std.testing.expectEqual(@as(usize, 0), backend.writes);
    try std.testing.expect(!plan.value.object.get("planning_only").?.bool);
    var no_limit = try compiler.compile(a, "EXPLAIN (ANALYZE,FORMAT JSON) SELECT id FROM things", .{});
    defer no_limit.deinit();
    var full = try execute(a, iface, &no_limit, &.{}, .{ .result_rows = 2 });
    defer full.deinit();
    var full_plan = try std.json.parseFromSlice(Json, a, full.output.rows[0][0].string, .{});
    defer full_plan.deinit();
    try std.testing.expectEqual(@as(i64, 7), full_plan.value.object.get("runtime").?.object.get("result_rows").?.integer);
    backend.cancelled = true;
    try std.testing.expectError(error.Cancelled, execute(a, iface, &compiled, &.{}, .{}));
    backend.cancelled = false;
    const opens = backend.statement_opens;
    var mutation = try compiler.compile(a, "EXPLAIN ANALYZE DELETE FROM things", .{});
    defer mutation.deinit();
    try std.testing.expectError(error.SqlAnalyzeRequiresReadOnly, execute(a, iface, &mutation, &.{}, .{}));
    try std.testing.expectEqual(opens, backend.statement_opens);
    try std.testing.expectEqual(@as(usize, 0), backend.writes);
}

test "SQL EXPLAIN roadmap costs capture metadata without pulling rows" {
    const a = std.testing.allocator;
    var backend: TestBackend = .{ .row_count = 100, .estimate_scans = true };
    var compiled = try compiler.compile(a, "EXPLAIN (FORMAT JSON, COSTS ON) SELECT id FROM things", .{});
    defer compiled.deinit();
    var result = try execute(a, backend.coordinated(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 0), backend.pages);
    try std.testing.expectEqual(@as(usize, 0), backend.writes);
    try std.testing.expectEqual(@as(usize, 0), backend.statement_opens);
    try std.testing.expectEqual(@as(usize, 0), backend.statement_closes);
    try std.testing.expect(std.mem.indexOf(u8, result.output.rows[0][0].string, "\"local_work_units\":100") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.output.rows[0][0].string, "\"calibrated_time_model\":false") != null);
    try std.testing.expectEqual(@as(usize, 1), backend.estimate_calls);
    var unknown = try execute(a, backend.iface(), &compiled, &.{}, .{});
    defer unknown.deinit();
    try std.testing.expect(std.mem.indexOf(u8, unknown.output.rows[0][0].string, "source statistics unavailable") != null);
    try std.testing.expectEqual(@as(usize, 0), backend.pages);
}

test "SQL EXPLAIN roadmap profiles physical joins groups sorts and recursion" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "SELECT a.id FROM things a JOIN things b ON a.id=b.id LIMIT 3",
        "SELECT id,COUNT(*) FROM things GROUP BY id",
        "SELECT id FROM things ORDER BY id LIMIT 3",
        "SELECT id FROM things UNION SELECT id FROM things",
        "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n<3) SELECT n FROM c",
    }) |query| {
        var backend: TestBackend = .{ .row_count = 4, .estimate_scans = true };
        var iface = backend.coordinated();
        iface.execution_io = std.testing.io;
        var reference_plan = try compiler.compile(a, query, .{});
        defer reference_plan.deinit();
        var reference = try execute(a, iface, &reference_plan, &.{}, .{});
        defer reference.deinit();
        const sql = try std.fmt.allocPrint(a, "EXPLAIN (FORMAT JSON, ANALYZE ON) {s}", .{query});
        defer a.free(sql);
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        var result = try execute(a, iface, &compiled, &.{}, .{});
        defer result.deinit();
        var parsed = try std.json.parseFromSlice(Json, a, result.output.rows[0][0].string, .{});
        defer parsed.deinit();
        const profile = parsed.value.object.get("runtime").?.object;
        try std.testing.expectEqual(reference.output.rowCount(), @as(usize, @intCast(profile.get("result_rows").?.integer)));
        try std.testing.expect(profile.get("nodes").?.array.items.len > 0);
        try std.testing.expectEqual(backend.statement_opens, backend.statement_closes);
        try std.testing.expectEqual(@as(usize, 0), backend.writes);
    }
}

test "SQL EXPLAIN roadmap estimates use native parameter frames without reading search rows" {
    const a = std.testing.allocator;
    var backend: TestBackend = .{ .row_count = 100, .estimate_scans = true };
    var iface = backend.coordinated();
    iface.supports_search_relations = true;
    var compiled = try compiler.compile(a, "EXPLAIN (FORMAT JSON, COSTS ON) SELECT _id FROM antfly_search('things',$1,5)", .{});
    defer compiled.deinit();
    const cells = [_]Datum{Datum.json(.{ .string = "{}" })};
    var result = try execute(a, iface, &compiled, &.{.null}, .{ .parameter_cells = &cells });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 0), backend.pages);
    try std.testing.expectEqual(@as(usize, 0), backend.statement_opens);
    try std.testing.expectEqual(@as(usize, 0), backend.statement_closes);
    try std.testing.expect(std.mem.indexOf(u8, result.output.rows[0][0].string, "\"local_work_units\":100") != null);
    try std.testing.expect(backend.estimated_search);
    try std.testing.expectEqual(@as(usize, 1), backend.estimate_calls);
    var mutation = try compiler.compile(a, "EXPLAIN (COSTS ON) DELETE FROM things", .{});
    defer mutation.deinit();
    var mutation_plan = try execute(a, iface, &mutation, &.{}, .{});
    defer mutation_plan.deinit();
    try std.testing.expectEqual(@as(usize, 0), backend.writes);
}

test "SQL storage reencodes native timestamptz datums without changing their microsecond value" {
    const Check = struct {
        fn run(backing: std.mem.Allocator) !void {
            var arena = std.heap.ArenaAllocator.init(backing);
            defer arena.deinit();
            const a = arena.allocator();
            const T = @import("temporal_value.zig");
            const column: catalog.Column = .{ .name = "created_at", .path = "created_at", .type = .datetime, .temporal_kind = .timestamp_tz };
            for ([_]i128{ 0, 1706704496123456000, std.math.maxInt(u64) - @mod(@as(i128, std.math.maxInt(u64)), 1000) }) |nanos| {
                var value: T.Value = .{ .timestamp_tz = try T.Timestamp.fromUnixNanoseconds(nanos) };
                const encoded = try encodeStorageDatum(a, .typedTemporal(&value), column, 20);
                try std.testing.expectEqual(nanos, try std.fmt.parseInt(i128, encoded.number_string, 10));
                try std.testing.expectError(error.SqlProgramLimitExceeded, encodeStorageDatum(a, .typedTemporal(&value), column, 0));
            }
            for ([_]T.Timestamp{ try T.Timestamp.fromUnixNanoseconds(-1000), try T.Timestamp.init(T.end_timestamp - 1), try T.Timestamp.init(std.math.maxInt(i64)) }) |instant| {
                var value: T.Value = .{ .timestamp_tz = instant };
                try std.testing.expectError(error.UnsupportedRelationalValueType, encodeStorageDatum(a, .typedTemporal(&value), column, 20));
            }
            var date: T.Value = .{ .date = try T.Date.init(0) };
            try std.testing.expectError(error.UnsupportedRelationalValueType, encodeStorageDatum(a, .typedTemporal(&date), column, 20));
        }
    };
    try Check.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}

test "SQL collection aggregates retain ordered tuples filters exact types and empty groups" {
    const a = std.testing.allocator;
    var backend: TestBackend = .{ .row_count = 3 };
    const sql = "SELECT string_agg(_id,',' ORDER BY _id DESC) AS joined,array_agg(_id::integer ORDER BY _id DESC) AS values,sum(_id::integer ORDER BY _id DESC) AS total,count(*) AS n FROM things";
    var compiled = try compiler.compile(a, sql, .{});
    defer compiled.deinit();
    var result = try execute(a, backend.iface(), &compiled, &.{}, .{ .page_rows = 1 });
    defer result.deinit();
    try std.testing.expectEqualStrings("2,1,0", result.output.rows[0][0].string);
    try std.testing.expectEqual(@as(usize, 3), result.output.rows[0][1].object.get("values").?.array.items.len);
    try std.testing.expectEqualStrings("2", result.output.rows[0][1].object.get("values").?.array.items[0].string);
    try std.testing.expectEqualStrings("3", result.output.rows[0][2].string);
    try std.testing.expectEqualStrings("3", result.output.rows[0][3].string);
    try std.testing.expectEqual(@import("array_value.zig").ElementType.int32, result.output.columns[1].element_type.?);
    var filtered = try compiler.compile(a, "SELECT string_agg((1/(_id::int-_id::int))::text,',' ORDER BY _id) FILTER(WHERE FALSE),array_agg(NULL::integer ORDER BY _id) FILTER(WHERE FALSE),count(*) FROM things", .{});
    defer filtered.deinit();
    var absent = try execute(a, backend.iface(), &filtered, &.{}, .{ .page_rows = 1 });
    defer absent.deinit();
    try std.testing.expectEqualSlices(bool, &.{ true, true, false }, absent.output.sql_nulls.?[0]);
    var distinct = try compiler.compile(a, "SELECT string_agg(DISTINCT _id,',' ORDER BY id) FROM things", .{});
    defer distinct.deinit();
    try std.testing.expectError(error.SqlAggregateDistinctOrderMismatch, execute(a, backend.iface(), &distinct, &.{}, .{}));
    backend.row_count = 0;
    var empty = try execute(a, backend.iface(), &compiled, &.{}, .{});
    defer empty.deinit();
    try std.testing.expectEqualSlices(bool, &.{ true, true, true, false }, empty.output.sql_nulls.?[0]);
}

test "SQL collection aggregates retain derived input ordering" {
    const a = std.testing.allocator;
    var backend: TestBackend = .{ .row_count = 3 };
    const queries = [_][]const u8{
        "SELECT string_agg(v,',') FROM (SELECT _id AS v FROM things ORDER BY _id DESC) sorted",
        "SELECT concat_ws(';',string_agg(v,',' ORDER BY v DESC),sum(v::int ORDER BY v)::text) FROM (SELECT _id AS v FROM things ORDER BY _id DESC) sorted",
    };
    for (queries, 0..) |sql, index| {
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        var result = try execute(a, backend.coordinated(), &compiled, &.{}, .{ .page_rows = 1 });
        defer result.deinit();
        try std.testing.expectEqualStrings(if (index == 0) "2,1,0" else "2,1,0;3", result.output.rows[0][0].string);
    }
}

test "SQL EXPLAIN drains lazy columns and preserves errors cancellation and count metadata" {
    const a = std.testing.allocator;
    var backend: TestBackend = .{ .row_count = 7 };
    var iface = backend.iface();
    iface.execution_io = std.testing.io;
    for ([_][]const u8{
        "SELECT _id FROM things",
        "SELECT _id FROM things ORDER BY _id DESC LIMIT 9223372036854775807",
        "SELECT _id FROM things WHERE false",
        "SELECT count(*) FROM things",
        "SELECT _id,count(*) FROM things GROUP BY _id ORDER BY _id",
        "SELECT array_agg(_id ORDER BY _id) FROM things",
    }, [_]i64{ 7, 7, 0, 1, 7, 1 }) |query, expected| {
        const sql = try std.fmt.allocPrint(a, "EXPLAIN (ANALYZE,FORMAT JSON,TIMING OFF,SUMMARY OFF,BUFFERS ON) {s}", .{query});
        defer a.free(sql);
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        var result = try execute(a, iface, &compiled, &.{}, .{ .result_rows = 1, .page_rows = 2 });
        defer result.deinit();
        var parsed = try std.json.parseFromSlice(Json, a, result.output.rows[0][0].string, .{});
        defer parsed.deinit();
        const measured = parsed.value.object.get("runtime").?.object;
        try std.testing.expectEqual(expected, measured.get("result_rows").?.integer);
        try std.testing.expect(measured.get("elapsed_ns").? == .null);
        try std.testing.expect(parsed.value.object.get("planning_time_ns").? == .null);
        for (measured.get("nodes").?.array.items) |node| try std.testing.expect(node.object.get("elapsed_ns").? == .null);
        try std.testing.expectEqual(backend.statement_opens, backend.statement_closes);
    }
    var failing = try compiler.compile(a, "EXPLAIN (ANALYZE,TIMING OFF,SUMMARY OFF) SELECT 1/(_id::int-_id::int) FROM things", .{});
    defer failing.deinit();
    try std.testing.expectError(error.SqlDivisionByZero, execute(a, iface, &failing, &.{}, .{ .result_rows = 1 }));
    backend.cancelled = true;
    try std.testing.expectError(error.Cancelled, execute(a, iface, &failing, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 0), backend.writes);
}

test "SQL EXPLAIN bounded drain reduces retained response memory benchmark" {
    const a = std.testing.allocator;
    var backend: TestBackend = .{ .row_count = 2048 };
    var iface = backend.iface();
    iface.execution_io = std.testing.io;
    var select = try compiler.compile(a, "SELECT repeat(_id,1024) FROM things", .{});
    defer select.deinit();
    var analyze = try compiler.compile(a, "EXPLAIN (ANALYZE,FORMAT JSON,TIMING OFF,SUMMARY OFF) SELECT repeat(_id,1024) FROM things", .{});
    defer analyze.deinit();
    const materialized_peak = blk: {
        var result = try execute(a, iface, &select, &.{}, .{ .result_rows = 2048, .page_rows = 64 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 2048), result.output.rowCount());
        break :blk result.peakMemoryBytes();
    };
    var result = try execute(a, iface, &analyze, &.{}, .{ .result_rows = 1, .page_rows = 64 });
    defer result.deinit();
    var plan = try std.json.parseFromSlice(Json, a, result.output.rows[0][0].string, .{});
    defer plan.deinit();
    try std.testing.expectEqual(@as(i64, 2048), plan.value.object.get("runtime").?.object.get("result_rows").?.integer);
    const drain_peak = result.peakMemoryBytes();
    try std.testing.expect(drain_peak < materialized_peak / 2);
    std.debug.print("EXPLAIN drain component rows=2048 materialized_peak_bytes={d} drain_peak_bytes={d} timing_and_storage_comparison_excluded=true\n", .{ materialized_peak, drain_peak });
}

test "SQL EXPLAIN execution releases every allocation failure" {
    const Check = struct {
        fn run(a: std.mem.Allocator) !void {
            var backend: TestBackend = .{ .row_count = 3 };
            var compiled = try compiler.compile(a, "EXPLAIN (ANALYZE,FORMAT JSON,TIMING OFF,SUMMARY OFF,COSTS OFF) SELECT _id,count(*) FROM things GROUP BY _id ORDER BY _id", .{});
            defer compiled.deinit();
            var result = try execute(a, backend.iface(), &compiled, &.{}, .{ .result_rows = 1, .page_rows = 1 });
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 1), result.output.rowCount());
            try std.testing.expectEqual(backend.statement_opens, backend.statement_closes);
        }
    };
    try Check.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}

test "SQL EXPLAIN PostgreSQL options custom plans and complete execution matrix" {
    const Options = struct { analyze: bool, timing: bool, summary: bool, buffers: bool, settings: bool, wal: bool, costs: bool };
    const Entry = struct { sql: []const u8, parameters: []const Json, sqlstate: ?[]const u8, rows: ?usize, options: ?Options };
    const Reference = struct { entries: []const Entry };
    const a = std.testing.allocator;
    var fixture = try std.json.parseFromSlice(Reference, a, @import("parity_fixtures.zig").explain_reference, .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    var backend: TestBackend = .{ .row_count = 0 };
    var iface = backend.coordinated();
    iface.execution_io = std.testing.io;
    var failures: usize = 0;
    for (fixture.value.entries, 0..) |entry, index| {
        const matches = check: {
            var compiled = compiler.compile(a, entry.sql, .{}) catch |err| break :check if (entry.sqlstate) |expected| std.mem.eql(u8, expected, @import("errors.zig").describe(err).code) else false;
            defer compiled.deinit();
            var result = execute(a, iface, &compiled, entry.parameters, .{ .result_rows = 1 }) catch |err| {
                if (entry.sqlstate) |expected| if (std.mem.eql(u8, expected, @import("errors.zig").describe(err).code)) break :check true;
                if (failures < 20) std.debug.print("EXPLAIN error index={d} err={s}\n", .{ index, @errorName(err) });
                break :check false;
            };
            defer result.deinit();
            if (entry.sqlstate != null or result.output.rows.len != 1) break :check false;
            var parsed = try std.json.parseFromSlice(Json, a, result.output.rows[0][0].string, .{});
            defer parsed.deinit();
            const plan = parsed.value.object;
            const options = plan.get("options").?.object;
            inline for (@typeInfo(Options).@"struct".field_names) |field| {
                if (options.get(field).?.bool != @field(entry.options.?, field)) break :check false;
            }
            if (entry.rows) |count| {
                const profile = plan.get("runtime").?.object;
                if (profile.get("result_rows").?.integer != count) break :check false;
                if (!entry.options.?.summary and profile.get("elapsed_ns").? != .null) break :check false;
                if (!entry.options.?.timing) for (profile.get("nodes").?.array.items) |node| {
                    if (node.object.get("elapsed_ns").? != .null) break :check false;
                };
            } else if (plan.get("runtime").? != .null) break :check false;
            if (failures < 20 and entry.options.?.settings and plan.get("settings").? == .null) break :check false;
            break :check true;
        };
        if (!matches) {
            failures += 1;
            if (failures <= 20) std.debug.print("EXPLAIN mismatch index={d} sql={s} expected_code={?s} expected_options={f}\n", .{ index, entry.sql, entry.sqlstate, std.json.fmt(entry.options, .{}) });
        }
    }
    std.debug.print("EXPLAIN PostgreSQL matrix cases={d} failures={d}\n", .{ fixture.value.entries.len, failures });
    try std.testing.expectEqual(@as(usize, 0), failures);
    try std.testing.expectEqual(backend.statement_opens, backend.statement_closes);
    try std.testing.expectEqual(@as(usize, 0), backend.writes);
}

test "SQL EXPLAIN temporal functions preserve PostgreSQL native values" {
    const Entry = struct { sql: []const u8, value: []const u8 };
    const a = std.testing.allocator;
    var fixture = try std.json.parseFromSlice(struct { truncations: []const Entry }, a, @import("parity_fixtures.zig").explain_reference, .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    var backend: TestBackend = .{};
    for (fixture.value.truncations) |entry| {
        errdefer std.debug.print("timestamp truncation: {s}\n", .{entry.sql});
        var compiled = try compiler.compile(a, entry.sql, .{});
        defer compiled.deinit();
        var result = try execute(a, backend.iface(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqualStrings(entry.value, result.output.rows[0][0].string);
    }
}

test "SQL EXPLAIN volatile sequence effects execute exactly once and planning never invokes them" {
    const sequences = @import("sequence_effects.zig");
    const Owner = struct {
        calls: i64 = 0,
        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: []const u8) !sequences.Reference {
            return .{ .identity = .{ .incarnation = @splat(1), .id = 7 }, .namespace_id = 2, .target = .{ .table = "numbers" } };
        }
        fn invoke(raw: *anyopaque, _: std.mem.Allocator, _: sequences.Function, _: ?sequences.Reference, _: []const Datum) !i64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return self.calls;
        }
        fn flush(_: *anyopaque) !void {}
    };
    const a = std.testing.allocator;
    var backend: TestBackend = .{ .row_count = 0 };
    var owner: Owner = .{};
    var iface = backend.coordinated();
    iface.execution_io = std.testing.io;
    iface.sequence_resolver = .{ .ptr = &owner, .resolve = Owner.resolve };
    iface.sequence_provider = .{ .ptr = &owner, .invoke = Owner.invoke, .flush = Owner.flush };
    for ([_]bool{ false, true }) |analyze| {
        const sql = try std.fmt.allocPrint(a, "EXPLAIN (ANALYZE {s},FORMAT JSON) SELECT nextval('numbers') FROM (VALUES(1),(2),(3)) t(n)", .{if (analyze) "ON" else "OFF"});
        defer a.free(sql);
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        var result = try execute(a, iface, &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(i64, if (analyze) 3 else 0), owner.calls);
    }
    for ([_][]const u8{
        "SELECT keep FROM (SELECT count(*) AS keep,sum(nextval('numbers')) AS dead FROM (VALUES(1),(2)) q(n)) d",
        "SELECT count(*) FROM (SELECT sum(nextval('numbers')) AS dead FROM (VALUES(1),(2)) q(n)) d",
        "SELECT keep FROM (SELECT count(*) AS keep,array_agg(nextval('numbers')) AS dead FROM (VALUES(1),(2)) q(n)) d",
    }) |query| {
        for ([_]bool{ false, true }) |analyze| {
            owner.calls = 0;
            const sql = try std.fmt.allocPrint(a, "EXPLAIN (ANALYZE {s},FORMAT JSON) {s}", .{ if (analyze) "ON" else "OFF", query });
            defer a.free(sql);
            var compiled = try compiler.compile(a, sql, .{});
            defer compiled.deinit();
            var result = try execute(a, iface, &compiled, &.{}, .{});
            defer result.deinit();
            try std.testing.expectEqual(@as(i64, if (analyze) 2 else 0), owner.calls);
        }
    }
}
