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
const Datum = @import("scalar.zig").Datum;

pub const Limits = struct {
    result_rows: usize = 128,
    mutation_rows: usize = 4096,
    scan_rows: usize = 10_000_000,
    retained_bytes: usize = 64 * 1024 * 1024,
    page_rows: u32 = 256,
    /// Native execution batches are independent of response/decision pages.
    execution_batch_rows: u32 = 4096,
    page_bytes: usize = 256 * 1024,
    scan_pages: usize = 65_536,
    spill_bytes: u64 = 1024 * 1024 * 1024,
    spill_root: []const u8 = "/tmp",
    pub fn executionRows(self: Limits) u32 {
        return @intCast(@min(self.execution_batch_rows, @max(@as(usize, 1), self.retained_bytes / (16 * 1024))));
    }
};
pub const Column = describe.Column;
pub const Output = struct {
    columns: []const Column = &.{},
    rows: []const []const Json = &.{},
    /// Mirrors rows when present. JSON null is encoded as a value when false;
    /// true denotes SQL NULL independently of the column's logical type.
    sql_nulls: ?[]const []const bool = null,
    pattern_sources: ?[]const []const ?*@import("scalar.zig").PatternSet = null,
    rows_affected: u64 = 0,
    command_tag: []const u8,
    mutation_outcome: ?catalog.MutationOutcome = null,
    ddl_receipt: ?catalog.DdlReceipt = null,

    /// Internal pattern sources contain executable callbacks and have no wire
    /// representation. Keep the public result envelope explicit at every ABI.
    pub fn jsonStringify(self: Output, writer: anytype) !void {
        try writer.write(.{ .columns = self.columns, .rows = self.rows, .sql_nulls = self.sql_nulls, .rows_affected = self.rows_affected, .command_tag = self.command_tag, .mutation_outcome = self.mutation_outcome, .ddl_receipt = self.ddl_receipt });
    }
};
pub const Result = struct {
    state: *State,
    output: Output,

    const State = struct { budget: MemoryBudget, arena: std.heap.ArenaAllocator, regex_execution: @import("regex_execution.zig") };

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
        self.state.regex_execution.deinit();
        self.state.arena.deinit();
        std.debug.assert(self.state.budget.live == 0);
        backing.destroy(self.state);
        self.* = undefined;
    }

    pub fn empty(alloc: std.mem.Allocator, command_tag: []const u8) !Result {
        const state = try alloc.create(State);
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

pub fn execute(alloc: std.mem.Allocator, backend: catalog.Backend, compiled: *const compiler.Compiled, parameters: []const Json, limits: Limits) !Result {
    if (limits.result_rows == 0 or limits.result_rows > 4096 or limits.mutation_rows == 0 or limits.mutation_rows > 4096 or
        limits.execution_batch_rows == 0 or limits.execution_batch_rows > 4096 or limits.page_rows == 0 or limits.page_rows > 4096 or limits.page_bytes == 0 or limits.scan_rows == 0) return error.InvalidSqlLimit;
    if (parameters.len != compiled.parameter_count) return error.InvalidSqlParameters;
    try backend.vtable.checkpoint(backend.ptr);
    var pinned_settings: ?@import("setting_catalog.zig").View = null;
    defer if (pinned_settings) |*view| view.deinit();
    var statement_backend = backend;
    if (parameters.len > 1024) return error.InvalidSqlParameters;
    if (backend.setting_capture) |capture| {
        pinned_settings = try @import("setting_catalog.zig").View.capture(alloc, capture.owner, capture.scope, capture.overlay);
        statement_backend.settings_view = &pinned_settings.?;
    }
    const state = try alloc.create(Result.State);
    state.budget = .{ .backing = alloc, .limit = limits.retained_bytes };
    state.regex_execution = .init(state.budget.allocator(), @min(16 * 1024 * 1024, limits.retained_bytes));
    statement_backend.regex_execution = &state.regex_execution;
    state.arena = std.heap.ArenaAllocator.init(state.budget.allocator());
    var result = Result{ .state = state, .output = undefined };
    errdefer result.deinit();
    const arena = state.arena.allocator();
    statement_backend.parameter_fallback_types = try parameterFallbackTypes(arena, parameters);
    result.output = runBound(state.budget.allocator(), arena, statement_backend, compiled, parameters, limits, null) catch |err| {
        if (err == error.OutOfMemory and state.budget.exhausted) return error.SqlProgramLimitExceeded;
        return err;
    };
    return result;
}

/// Execution-only transport hints fill polymorphic holes after SQL constraints
/// converge. Prepare/Describe without values must not invent these types.
/// The returned metadata belongs to the statement or cursor's owned arena.
pub fn parameterFallbackTypes(arena: std.mem.Allocator, parameters: []const Json) ![]const ?ast.ColumnType {
    if (parameters.len > 1024) return error.InvalidSqlParameters;
    const types = try arena.alloc(?ast.ColumnType, parameters.len);
    for (parameters, types) |value, *kind| kind.* = switch (value) {
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
    return types;
}

fn runBound(alloc: std.mem.Allocator, arena: std.mem.Allocator, backend: catalog.Backend, compiled: *const compiler.Compiled, parameters: []const Json, limits: Limits, sink: ?RowSink) !Output {
    if (compiled.statement == .explain) {
        const explanation = compiled.statement.explain;
        const inner: compiler.Compiled = .{ .arena = undefined, .statement = explanation.statement.*, .parameter_count = compiled.parameter_count };
        const binding = try describe.bind(arena, backend, &inner, &.{});
        const rendered = try @import("explain.zig").render(arena, explanation, binding);
        const row = try arena.dupe(Json, &.{.{ .string = rendered }});
        return .{ .columns = try arena.dupe(Column, &.{.{ .name = "QUERY PLAN", .type = .string }}), .rows = try arena.dupe([]const Json, &.{row}), .command_tag = "EXPLAIN" };
    }
    const ddl = @import("ddl_runtime.zig");
    if (ddl.accepts(compiled.statement)) {
        const result = try ddl.execute(arena, backend, compiled.statement);
        return .{ .command_tag = result.command_tag, .mutation_outcome = result.mutation_outcome, .ddl_receipt = result.receipt };
    }
    // Describe and Execute share binding, type inference and authorization.
    // Resolve exactly once so validation and execution cannot pin different
    // catalog identities within one statement.
    const binding = try describe.bind(arena, backend, compiled, &.{});
    var statement_backend = backend;
    statement_backend.parameter_invocation = binding.parameter_invocation;
    if (binding.parameter_invocation) |invocation| try invocation.prepareJson(alloc, parameters, .{ .bytes = limits.retained_bytes, .wire_bytes = limits.retained_bytes });
    defer if (binding.parameter_invocation) |invocation| invocation.deinitFrame();
    const prepared_parameters = if (binding.parameter_invocation) |invocation| try invocation.compatibilityValues(arena) else parameters;
    var manager: ?@import("spill.zig").Manager = null;
    defer if (manager) |*owned| owned.deinit();
    if (backend.spill_manager == null and limits.spill_bytes != 0) if (backend.execution_io) |io| {
        manager = .{ .alloc = alloc, .io = io, .context = backend.ptr, .checkpoint = backend.vtable.checkpoint, .root = limits.spill_root, .max_bytes = limits.spill_bytes, .buffer_bytes = @min(4096, @max(128, limits.retained_bytes / 512)), .max_record_bytes = @max(@as(usize, 1024), @min(@as(usize, 4 * 1024 * 1024), limits.retained_bytes / 32)) };
    };
    const context = Context{ .alloc = alloc, .arena = arena, .backend = statement_backend, .binding = binding, .parameters = prepared_parameters, .limits = limits, .spill = backend.spill_manager orelse if (manager) |*owned| owned else null, .sink = sink };
    return context.run(compiled.statement);
}

pub const RowSink = struct {
    ptr: *anyopaque,
    append: *const fn (*anyopaque, []const Datum) anyerror!void,
    take_sorted: ?*const fn (*anyopaque, *@import("operators.zig").TopK, usize, usize, bool) anyerror!void = null,
};
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
    returning_layout: ?catalog.Row.TypedLayout = null,
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
        opened: bool = false,
        cursor: ?catalog.Cursor = null,

        pub fn deinit(self: *ScanState) void {
            if (self.cursor) |cursor| cursor.close(cursor.ptr);
            self.cursor = null;
        }

        fn open(self: *ScanState, context: Context, table: catalog.Table, request: catalog.Scan) !void {
            if (!self.opened) {
                self.opened = true;
                if (context.backend.vtable.open_scan) |capture|
                    self.cursor = try capture(context.backend.ptr, context.alloc, table, request);
            }
        }
        pub fn page(self: *ScanState, context: Context, alloc: std.mem.Allocator, table: catalog.Table, request: catalog.Scan) !catalog.Page {
            try self.open(context, table, request);
            if (self.cursor) |cursor| return cursor.next(cursor.ptr, alloc, request.limit);
            return context.backend.vtable.scan(context.backend.ptr, alloc, table, request);
        }
        pub fn count(self: *ScanState, context: Context, table: catalog.Table, request: catalog.Scan) !?u64 {
            try self.open(context, table, request);
            if (self.cursor) |cursor| if (cursor.count_rows) |count_rows| return try count_rows(cursor.ptr);
            return null;
        }
        pub fn columns(self: *ScanState, context: Context, alloc: std.mem.Allocator, table: catalog.Table, request: catalog.Scan) !?catalog.ColumnPage {
            try self.open(context, table, request);
            if (self.cursor) |cursor| if (cursor.next_columns) |next| return try next(cursor.ptr, alloc, request.limit);
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
            capture = try @import("mutation_capture.zig").open(context.alloc, context.backend, context.binding);
            context.statement_capture = &capture.?;
        }
        if (context.binding.joined_mutation) |joined| return @import("joined_mutation.zig").execute(context, joined.*);
        if (context.binding.merge_mutation) |merge| return @import("merge_mutation.zig").execute(context, merge.*);
        return switch (input) {
            .select => |statement| try context.select(statement),
            .insert => |statement| try context.insert(statement),
            .update => |statement| try context.change(statement.table, statement.predicate, statement.assignments, statement.returning),
            .delete => |statement| try context.change(statement.table, statement.predicate, null, statement.returning),
            else => return error.UnsupportedSqlExecution,
        };
    }

    pub fn checkpoint(self: Context) !void {
        try self.backend.vtable.checkpoint(self.backend.ptr);
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
        try self.selectInto(statement, .{ .ptr = cursor, .append = Cursor.append, .take_sorted = Cursor.takeSorted });
        return cursor;
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
                cell.* = try describe.coerceDatum(scratch.allocator(), .{ .value = value_, .sql_null = if (output.sql_nulls) |flags| flags[index][ordinal] else value_ == .null, .patterns = if (output.pattern_sources) |sources| sources[index][ordinal] else null }, column.type, column.element_type);
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
        return encodeStorageDatum(self.arena, datum, column, self.limits.retained_bytes);
    }

    fn value(self: Context, input: ast.Value, column: catalog.Column) !Json {
        if (input == .string and column.type == .json)
            return self.binding.json_literals.get(input.string) orelse error.InvalidSqlBackendResponse;
        if (input != .parameter) return describe.bindLiteral(self.arena, input, column.type);
        const index = input.parameter;
        if (index == 0 or index > self.parameters.len) return error.InvalidSqlParameters;
        if (self.binding.parameter_invocation) |invocation| if (invocation.frame) |frame| {
            const datum = try describe.coerceDatum(self.arena, frame.values[index - 1], column.type, column.element_type);
            if (datum.array != null) return error.UnsupportedSqlShape;
            return if (datum.sql_null) .null else datum.value;
        };
        return coerce(self.arena, self.parameters[index - 1], column.type);
    }

    fn parameterDatum(self: Context, index: usize) !Datum {
        if (index >= self.parameters.len) return error.InvalidSqlParameters;
        if (self.binding.parameter_invocation) |invocation| if (invocation.frame) |frame| return frame.values[index];
        return Datum.fromJson(self.parameters[index]);
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
        return conditions_out;
    }

    fn bindConditions(self: Context, table_def: catalog.Table, predicate: ?*const ast.Predicate, output: *BoundPredicates) anyerror!void {
        const node = predicate orelse return;
        switch (node.*) {
            .comparison => |comparison| {
                const column = try table_def.column(comparison.field);
                // Native condition values cannot carry typed arrays or exact
                // NUMERIC limbs. Keep these comparisons in the typed residual
                // instead of interpreting their JSON placeholder as SQL NULL.
                if (column.type == .array or column.element_type == .numeric) {
                    output.complete = false;
                    return;
                }
                // A declared NUMERIC parameter carries exact limbs, not a
                // native condition value. Mixed comparisons use the bound
                // scalar program's floating domain (including REAL/NUMERIC
                // comparisons in double precision), without narrowing the
                // operand to the physical column width.
                if (column.type == .number and comparison.value == .parameter) {
                    const datum = try self.parameterDatum(comparison.value.parameter - 1);
                    if (datum.numeric != null) {
                        output.complete = false;
                        return;
                    }
                }
                // Row identity has a separate native key boundary; never
                // pretend it is a document property in a storage predicate.
                const bound_value = try self.value(comparison.value, column);
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
        const scan_request: catalog.Scan = .{ .row_goal = if (statement.limit != null and predicates.complete and !statement.count_all) offset +| limit else null, .fields = native_fields.items, .primary_order = self.binding.primary_order, .order = requested_order, .primary_key = predicates.primary_key, .conditions = predicates.terms.items, .limit = @intCast(self.limits.page_rows) };
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
                        const values = try scratch.alloc(Datum, fields.items.len + if (defer_projection) self.binding.scalars.columns.len else @as(usize, 0));
                        if (defer_projection) @memcpy(values[fields.items.len..], page_cells[row_index]);
                        for (fields.items, values[0..fields.items.len], 0..) |field, *projected_value, index| projected_value.* = if (deferred[index]) .{} else if (index < projection_values.len and projection_values[index] != null)
                            projection_values[index].?[selected_positions[row_index].?]
                        else blk: {
                            const cell = try row.cell(field);
                            break :blk try describe.coerceDatum(scratch, cell, columns[index].type, columns[index].element_type);
                        };
                        const keys = try scratch.alloc(Datum, self.binding.order_keys.len);
                        for (self.binding.order_keys, keys) |key, *out| out.* = switch (key.source) {
                            .output => |index| values[index],
                            .expression => |index| order_values[index].?[selected_positions[row_index].?],
                            .column => |column| blk: {
                                const cell = try row.cell(column.path);
                                break :blk try describe.coerceDatum(scratch, cell, column.type, column.element_type);
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
                            cell.* = try describe.coerceDatum(scratch, input, column.type, column.element_type);
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
                        const typed = try describe.coerceDatum(self.arena, input_cell, column.type, column.element_type);
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
                    for (deferred, 0..) |needed, column| if (needed) {
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
            out.* = try describe.coerceDatum(alloc, input, column.type, column.element_type);
        }
        return values;
    }

    /// Resolve external projection columns together for a bounded input page.
    /// The caller selects predicate/OFFSET/LIMIT survivors before calling this,
    /// so discarded rows never invoke projection providers.
    pub fn projectValuesBatch(self: Context, alloc: std.mem.Allocator, rows: []const catalog.Row, fields: []const []const u8, cells: []const []const Datum) ![]const []const Datum {
        if (rows.len != cells.len) return error.InvalidSqlBackendResponse;
        const columns = try alloc.alloc(?[]const Datum, self.binding.scalars.projections.len);
        for (self.binding.scalars.projections, columns) |optional, *values| values.* = if (optional) |*program|
            try @import("decision_eval.zig").evaluateBatchWithLimits(alloc, self.backend.decision_provider, program, cells, self.parameters, @import("decision_eval.zig").limitsFor(self.backend))
        else
            null;
        const output = try alloc.alloc([]const Datum, rows.len);
        for (rows, output, 0..) |row, *values, row_index| {
            const projected = try alloc.alloc(Datum, fields.len);
            for (fields, self.binding.columns, projected, 0..) |field, column, *out, index| {
                const input = if (index < columns.len and columns[index] != null) columns[index].?[row_index] else blk: {
                    const cell = try row.cell(field);
                    break :blk cell;
                };
                out.* = try describe.coerceDatum(alloc, input, column.type, column.element_type);
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

    fn insert(self: Context, statement: ast.Insert) !Output {
        if (statement.source) |source| return self.insertSelect(statement, source.*);
        const table_def = self.binding.table orelse return error.InvalidSqlBackendResponse;
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
                if ((try keys.getOrPut(self.arena, key.string)).found_existing and (statement.conflict == null or !@import("conflict.zig").allowsDuplicateKeys(statement.conflict.?))) return error.DuplicateSqlRow;
                var object: std.json.ObjectMap = .empty;
                var json_null_fields: std.ArrayList([]const u8) = .empty;
                for (columns, row, 0..) |column, item, i| {
                    if (key_index != null and i == key_index.?) continue;
                    if (statement.isDefault(row_index, i)) continue;
                    const datum = try self.insertValue(item, column, row_index, i, page.values[row_index - first][i]);
                    const typed = try self.storageDatum(datum, column);
                    const json_null = column.type == .json and typed == .null and !datum.sql_null;
                    if (json_null) try json_null_fields.append(self.arena, column.path);
                    try putField(self.arena, &object, column.path, typed);
                }
                const document: Json = .{ .object = object };
                retained = std.math.add(usize, retained, jsonSize(document) + key.string.len) catch return error.SqlProgramLimitExceeded;
                if (retained > self.limits.retained_bytes) return error.SqlProgramLimitExceeded;
                mutation.* = .{ .key = key.string, .expected_version = 0, .unique_absence = true, .row = document, .json_null_fields = json_null_fields.items };
            }
            first = page.end;
            try self.checkpoint();
        }
        const resolved = if (statement.conflict) |clause| try @import("conflict.zig").resolve(self, table_def, clause, self.binding.conflict orelse return error.InvalidSqlBackendResponse, mutations, &.{}) else mutations;
        return self.commitMutations(table_def, resolved, "INSERT", statement.returning);
    }

    fn insertSelect(self: Context, statement: ast.Insert, source: ast.Select) !Output {
        const table = self.binding.table orelse return error.InvalidSqlBackendResponse;
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
                if (statement.isDefault(row_index, cell_index)) continue;
                if (!value_.sql_null and source_column.type != target.type and !(source_column.type == .integer and target.type == .number) and !(statement.values_source_rows.len != 0 and source_column.type == .string and (target.type == .datetime or target.type == .json or target.type == .uuid))) return error.SqlTypeMismatch;
                const typed = try self.storageDatum(value_, target);
                if (std.mem.eql(u8, target.name, "_id")) {
                    if (value_.sql_null or typed != .string or typed.string.len == 0) return error.SqlRowIdentityRequired;
                    if (!std.unicode.utf8ValidateSlice(typed.string)) return error.SqlTypeMismatch;
                    key = typed.string;
                } else {
                    if (target.type == .json and !value_.sql_null and typed == .null) try json_null_fields.append(self.arena, target.path);
                    try putField(self.arena, &object, target.path, typed);
                }
            }
            const identity = key orelse try (self.backend.vtable.generate_row_id orelse return error.SqlRowIdentityRequired)(self.backend.ptr, self.arena);
            if (identity.len == 0 or !std.unicode.utf8ValidateSlice(identity)) return error.InvalidSqlBackendResponse;
            if ((try keys.getOrPut(self.arena, identity)).found_existing and (statement.conflict == null or !@import("conflict.zig").allowsDuplicateKeys(statement.conflict.?))) return error.DuplicateSqlRow;
            retained = std.math.add(usize, retained, jsonSize(.{ .object = object }) + identity.len) catch return error.SqlProgramLimitExceeded;
            if (retained > self.limits.retained_bytes) return error.SqlProgramLimitExceeded;
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
        while (end < statement.rows.len) {
            var bytes: usize = 0;
            if (self.binding.scalars.insert_rows.len != 0) {
                for (self.binding.scalars.insert_rows[end], 0..) |optional, column| {
                    if (statement.isDefault(end, column)) continue;
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
            if (budget.addBytes(bytes)) break;
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
                if (statement.isDefault(index, column)) continue;
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
            return describe.coerceDatum(self.arena, result, column.type, column.element_type);
        }
        if (literal == .parameter) {
            if (literal.parameter == 0) return error.InvalidSqlParameters;
            return describe.coerceDatum(self.arena, try self.parameterDatum(literal.parameter - 1), column.type, column.element_type);
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
            if (program != null and item.expression == null and (item.value == .parameter or (column.type == .array and item.value == .string))) {
                // A direct parameter assignment borrows the invocation frame,
                // not row scratch. Retain it once, as on the literal path;
                // do not clone a wide prepared payload for every target row.
                const value_ = try self.evaluate(self.arena, program.?, &.{});
                typed = try self.storageDatum(value_, column);
                sql_null = value_.sql_null;
                program = null;
            } else if (program == null) {
                typed = try self.value(item.value, column);
                sql_null = typed == .null and !(column.type == .json and item.value == .string);
            }
            if (program == null and sql_null and !column.nullable) return error.SqlNotNullViolation;
            // These constants are immutable for the entire statement. Own
            // them once, then share them across the prepared replacement rows.
            bound.* = .{ .column = column, .value = try clone(self.arena, typed), .sql_null = sql_null, .program = program };
            try replaced.put(self.arena, column.path, {});
        };
        var fields: std.ArrayList([]const u8) = .empty;
        if (assignments != null) for (table_def.columns) |column| {
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
            var retained_layout: ?catalog.Row.TypedLayout = null;
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
                        if (table_def.storage_mode == .document) {
                            const original = row.document orelse return error.InvalidSqlBackendResponse;
                            if (original != .object or (row.expected_content_digest == null and row.version != 0)) return error.InvalidSqlBackendResponse;
                            var members = original.object.iterator();
                            while (members.next()) |member| {
                                if (replaced.contains(member.key_ptr.*)) continue;
                                const declared = table_def.column(member.key_ptr.*) catch null;
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
                                try putField(self.arena, &copy.object, column.path, try self.storageDatum(cell, column));
                            }
                        }
                        for (bound_assignments, 0..) |bound, assignment_index| {
                            const assigned_value = if (bound.program) |program| blk: {
                                const assigned = if (evaluated) |batch| batch.assignments[assignment_index].?[batch.positions[row_index].?] else try self.evaluate(scratch, program, expression_cells);
                                if (assigned.sql_null and !bound.column.nullable) return error.SqlNotNullViolation;
                                if (bound.column.type == .json and !assigned.sql_null and assigned.value == .null) try json_null_fields.append(self.arena, bound.column.path);
                                break :blk try self.storageDatum(assigned, bound.column);
                            } else blk: {
                                if (bound.column.type == .json and !bound.sql_null and bound.value == .null) try json_null_fields.append(self.arena, bound.column.path);
                                break :blk bound.value;
                            };
                            try putField(self.arena, &copy.object, bound.column.path, assigned_value);
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
            if (input.len != 0 and table.storage_mode != .document and !std.mem.eql(u8, tag, "DELETE")) {
                const prepare = self.backend.vtable.prepare_mutations orelse return error.UnsupportedSqlExecution;
                prepared = try prepare(self.backend.ptr, self.arena, table, input);
                did_prepare = true;
                if (prepared.len != input.len) return error.InvalidSqlBackendResponse;
            }
            var context = self;
            context.binding = binding.*;
            if (self.binding.returning_query) |query| {
                const images_adapter = try ReturningImages.init(self.arena, table, null);
                const images = try self.arena.alloc(catalog.Row, prepared.len);
                for (prepared, input, images) |mutation, original, *image| image.* = try images_adapter.row(self.arena, mutation, original);
                context.returning_rows = images;
                context.sink = null;
                const projected = if (images.len == 0) Output{ .columns = binding.columns, .rows = &.{}, .sql_nulls = &.{}, .command_tag = "SELECT" } else try context.select(query);
                if (projected.rows.len != prepared.len) return error.InvalidSqlBackendResponse;
                output.columns = projected.columns;
                output.rows = projected.rows;
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
                const images_adapter = try ReturningImages.init(self.arena, table, image_fields.items);
                const rows = try self.arena.alloc([]const Json, prepared.len);
                const flags = try self.arena.alloc([]const bool, prepared.len);
                var external = false;
                for (binding.scalars.projections) |optional| if (optional) |*program| {
                    external = external or @import("decision_eval.zig").hasExternal(program);
                };
                if (external) {
                    try context.decisionMutationReturning(images_adapter, fields.items, prepared, input, rows, flags);
                } else for (prepared, input, rows, flags) |mutation, original, *cells, *nulls| {
                    try self.checkpoint();
                    const row = try images_adapter.row(self.arena, mutation, original);
                    const expressions = try binding.scalars.cells(self.arena, row);
                    const projected = try context.projectValues(self.arena, row, fields.items, expressions);
                    const values = try self.arena.alloc(Json, projected.len);
                    const sql_nulls = try self.arena.alloc(bool, projected.len);
                    for (projected, values, sql_nulls, binding.columns) |value_, *cell_value, *is_null, column| {
                        cell_value.* = try self.outputDatum(value_, column.type, column.element_type);
                        is_null.* = value_.sql_null;
                    }
                    cells.* = values;
                    nulls.* = sql_nulls;
                }
                output.columns = binding.columns;
                output.rows = rows;
                output.sql_nulls = flags;
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
        const committed = if (fences.items.len == 0) prepared else blk: {
            try fences.appendSlice(self.arena, prepared);
            break :blk fences.items;
        };
        // Commit-only wire buffers have their own lifetime. Growing them in
        // the result arena amplifies retained preimages and cannot reclaim
        // serialization capacity before the statement result is released.
        var commit_arena = std.heap.ArenaAllocator.init(self.alloc);
        defer commit_arena.deinit();
        output.mutation_outcome = if (committed.len == 0) .committed else if (did_prepare)
            try (self.backend.vtable.mutate_prepared orelse return error.UnsupportedSqlExecution)(self.backend.ptr, commit_arena.allocator(), table, committed)
        else
            try self.backend.vtable.mutate(self.backend.ptr, commit_arena.allocator(), table, committed);
        return output;
    }

    pub const ReturningImages = struct {
        table: catalog.Table,
        projection: @import("document_row.zig").Projection,
        layout: ?catalog.Row.TypedLayout,

        pub fn init(a: std.mem.Allocator, table: catalog.Table, fields: ?[]const []const u8) !ReturningImages {
            const names = fields orelse blk: {
                const all = try a.alloc([]const u8, table.columns.len);
                for (table.columns, all) |column, *name| name.* = column.name;
                break :blk all;
            };
            const projection = try @import("document_row.zig").Projection.init(a, table, names);
            return .{ .table = table, .projection = projection, .layout = try projection.pageLayout(a) };
        }

        pub fn row(self: ReturningImages, a: std.mem.Allocator, mutation: catalog.Mutation, original: catalog.Mutation) !catalog.Row {
            if (!std.mem.eql(u8, mutation.key, original.key) or mutation.expected_version != original.expected_version or
                !std.meta.eql(mutation.expected_content_digest, original.expected_content_digest) or mutation.unique_absence != original.unique_absence or
                mutation.conflict_guard != original.conflict_guard or mutation.predicate_only != original.predicate_only or
                (mutation.row == null) != (original.row == null)) return error.InvalidSqlBackendResponse;
            const datum = mutation.row orelse return (original.previous orelse return error.InvalidSqlBackendResponse).*;
            if (datum != .object) return error.InvalidSqlBackendResponse;
            const nulls = try a.alloc(bool, datum.object.count());
            for (datum.object.values(), nulls) |cell, *flag| flag.* = cell == .null;
            for (mutation.json_null_fields) |name| {
                const index = datum.object.getIndex(name) orelse return error.InvalidSqlBackendResponse;
                if (!nulls[index] or (try self.table.column(name)).type != .json) return error.InvalidSqlBackendResponse;
                nulls[index] = false;
            }
            return self.projection.adaptBorrowed(a, self.layout, .{ .id = mutation.key, .version = mutation.expected_version, .value = datum, .sql_nulls = nulls });
        }
    };

    /// Provider scratch must not accumulate in the retained mutation/output
    /// arena. Resolve RETURNING pages before publishing any native mutation.
    fn decisionMutationReturning(self: Context, images: ReturningImages, fields: []const []const u8, prepared: []const catalog.Mutation, input: []const catalog.Mutation, rows: [][]const Json, flags: [][]const bool) !void {
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
                const output = try self.arena.alloc(Json, values.len);
                const nulls = try self.arena.alloc(bool, values.len);
                for (values, output, nulls, self.binding.columns) |datum, *cell, *flag, column| {
                    cell.* = try self.outputDatum(datum, column.type, column.element_type);
                    flag.* = datum.sql_null;
                }
                rows[index] = output;
                flags[index] = nulls;
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
    const assigned = if (column.type == .array) try @import("scalar.zig").assignArray(arena, datum, column.element_type orelse return error.SqlAssignmentTypeMismatch, .{ .output_bytes = retained_bytes }) else datum;
    const checked = try describe.coerceDatum(arena, assigned, column.type, column.element_type);
    if (checked.sql_null) {
        if (!column.nullable) return error.SqlNotNullViolation;
        return .null;
    }
    if (checked.array) |array| return @import("array_wire.zig").toJsonLeaky(arena, array.*, .{ .values = .{ .bytes = retained_bytes }, .wire_bytes = retained_bytes });
    if (checked.numeric) |value| {
        var context: @import("numeric_value.zig").Context = .{ .alloc = arena, .max_output_bytes = retained_bytes };
        return .{ .string = try @import("numeric_value.zig").format(&context, value.*) };
    }
    return clone(arena, checked.value);
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
        fn mutate(raw: *anyopaque, _: std.mem.Allocator, _: catalog.Table, mutations: []const catalog.Mutation) !catalog.MutationOutcome {
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
        fn mutate(raw: *anyopaque, _: std.mem.Allocator, _: catalog.Table, mutations: []const catalog.Mutation) !catalog.MutationOutcome {
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
    try std.testing.expect(array.elements[0].value == .null and !array.elements[0].sql_null);
    try std.testing.expect(array.elements[1].sql_null);
    try std.testing.expectEqualStrings("2", array.elements[2].value.object.get("x").?.array.items[1].number_string);
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

const TestBackend = struct {
    estimate_scans: bool = false,
    pages: usize = 0,
    writes: usize = 0,
    row_count: usize = 2,
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
        return .{ .ptr = self, .vtable = &.{ .resolve = resolve, .scan = scan, .mutate = mutate, .checkpoint = checkpoint, .open_statement = openStatement } };
    }
    const Statement = struct {
        backend: *TestBackend,
        alloc: std.mem.Allocator,
        cursors: []catalog.Cursor,
        states: []State,
        const State = struct { owner: *Statement, table: catalog.Table, request: catalog.Scan, after: ?[]const u8 = null, done: bool = false };
        fn next(ptr: *anyopaque, alloc: std.mem.Allocator, limit: u32) !catalog.Page {
            const state: *State = @ptrCast(@alignCast(ptr));
            if (state.done) return .{ .rows = &.{} };
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
            cursor.* = .{ .ptr = state, .next = Statement.next, .close = Statement.closeCursor, .estimated_rows = if (backend.estimate_scans) backend.row_count else null };
        }
        backend.statement_opens += 1;
        backend.statement_scan_count = scans.len;
        return .{ .ptr = owner, .cursors = cursors, .close = Statement.close };
    }
    fn resolve(_: *anyopaque, _: std.mem.Allocator, _: ast.Name, _: catalog.Action) !catalog.Table {
        return .{ .id = 19, .physical_name = "table:stable", .schema_version = 7, .columns = &.{.{ .name = "id", .path = "id", .type = .integer, .nullable = false }} };
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
            try object.put(alloc, "id", .{ .number_string = "9007199254740993" });
            row.* = .{ .id = try std.fmt.allocPrint(alloc, "{d}", .{index}), .version = 99, .value = .{ .object = object } };
        }
        return .{ .rows = rows, .after = if (request.primary_key == null and from + count_rows < self.row_count) try std.fmt.allocPrint(alloc, "{d}", .{from + count_rows}) else null };
    }
    fn mutate(ptr: *anyopaque, _: std.mem.Allocator, table_def: catalog.Table, mutations: []const catalog.Mutation) !catalog.MutationOutcome {
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

test "SQL commit serialization is released before the statement result" {
    const Provider = struct {
        fn mutate(_: *anyopaque, alloc: std.mem.Allocator, _: catalog.Table, mutations: []const catalog.Mutation) !catalog.MutationOutcome {
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
        fn mutate(ptr: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
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
    for ([_][]const u8{ "EXPLAIN ANALYZE SELECT id FROM things", "EXPLAIN (COSTS ON) SELECT id FROM things", "EXPLAIN DROP TABLE things" }) |sql| {
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
        fn mutate(ptr: *anyopaque, _: std.mem.Allocator, _: catalog.Table, mutations: []const catalog.Mutation) !catalog.MutationOutcome {
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
    try std.testing.expectError(error.SqlProgramLimitExceeded, execute(std.testing.allocator, limited.iface(), &large, &.{}, .{ .retained_bytes = 1024 * 1024 }));
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
        fn mutate(ptr: *anyopaque, _: std.mem.Allocator, _: catalog.Table, mutations: []const catalog.Mutation) !catalog.MutationOutcome {
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
    var compiled = try compiler.compile(std.testing.allocator, "SELECT count(DISTINCT id), sum(DISTINCT _id::bigint % 2), count(*) FILTER (WHERE _id::bigint % 2 = 0), sum(1 / 0) FILTER (WHERE FALSE) FROM things", .{});
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

        fn mutate(ptr: *anyopaque, alloc: std.mem.Allocator, table: catalog.Table, mutations: []const catalog.Mutation) !catalog.MutationOutcome {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(self.opens, self.closes);
            return TestBackend.mutate(&self.base, alloc, table, mutations);
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

test "SQL memory quota rejects allocations before backend work" {
    var backend: TestBackend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT id FROM things", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.SqlProgramLimitExceeded, execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{ .retained_bytes = 1 }));
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
    try std.testing.expectEqual(@as(i64, 9007199254740993), decoded.value.elements[0].value.integer);
    try std.testing.expectEqual(@as(i32, -1), decoded.value.dimensions[0].lower);
    try std.testing.expect(decoded.value.elements[1].sql_null);
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
    try std.testing.expectEqual(@as(i64, 9007199254740993), decoded.value.elements[0].value.integer);
    try std.testing.expectEqual(@as(i32, -1), decoded.value.dimensions[0].lower);
    try std.testing.expect(decoded.value.elements[1].sql_null);
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
            try std.testing.expectEqual(@as(f64, 3.5), first.value.elements[0].value.float);
            try std.testing.expect(first.value.elements[1].sql_null);
            try std.testing.expectEqual(@as(i32, -1), second.value.dimensions[0].lower);
            try std.testing.expectEqual(@as(f64, 1), second.value.elements[0].value.float);
            try std.testing.expect(second.value.elements[1].sql_null);
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
            try std.testing.expectEqual(catalog.Action.read_write, action);
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
        fn mutate(ptr: *anyopaque, _: std.mem.Allocator, _: catalog.Table, rows: []const catalog.Mutation) !catalog.MutationOutcome {
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
        fn mutate(ptr: *anyopaque, _: std.mem.Allocator, _: catalog.Table, mutations: []const catalog.Mutation) !catalog.MutationOutcome {
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
        fn validate(_: *anyopaque, _: []const u8, questions: Json) !void {
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
    fn validate(_: *anyopaque, _: []const u8, questions: Json) !void {
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
    fn mutate(ptr: *anyopaque, _: std.mem.Allocator, _: catalog.Table, mutations: []const catalog.Mutation) !catalog.MutationOutcome {
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

test "SQL joins consume native column batches without invoking the JSON cursor" {
    const Native = struct {
        var installs: usize = 0;
        fn install(_: *anyopaque, filter: *const @import("dynamic_filter.zig").Filter) !bool {
            try std.testing.expect(filter.sealed);
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
            }
            return statement;
        }
    };
    Native.installs = 0;
    var fixture: TestBackend = .{ .row_count = 1537 };
    var backend = fixture.coordinated();
    var vtable = backend.vtable.*;
    vtable.open_statement = Native.open;
    backend.vtable = &vtable;
    var compiled = try compiler.compile(std.testing.allocator, "SELECT count(*), sum(l.id) FROM things l JOIN things r ON l.id=r.id", .{});
    defer compiled.deinit();
    var result = try execute(std.testing.allocator, backend, &compiled, &.{}, .{ .page_rows = 7, .execution_batch_rows = 1024 });
    defer result.deinit();
    try std.testing.expectEqualStrings("1537", result.output.rows[0][0].string);
    try std.testing.expectEqualStrings("1180416", result.output.rows[0][1].string);
    try std.testing.expectEqual(@as(usize, 0), fixture.pages);
    try std.testing.expectEqual(@as(usize, 1), Native.installs);
    try std.testing.expectEqual(fixture.statement_opens, fixture.statement_closes);
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

test "SQL native aggregate materialization retains projection and fails closed after selection" {
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
        fn mutate(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
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
    driver.recipe = recipe;
    driver.partials = partials;
    driver.fail = true;
    try std.testing.expectError(error.ArtifactChecksumMismatch, execute(a, driver.backend(), &reference, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 3), driver.opened);
    try std.testing.expectEqual(driver.opened, driver.closed);
    try std.testing.expectEqual(@as(usize, 2), driver.base.pages);
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
