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

//! Request-owned expression binding. Compiled programs borrow neither a schema
//! cache lease nor row memory. Only required columns enter the native scan;
//! expression evaluation uses resolved ordinals rather than SQL name lookup.
const std = @import("std");
const ast = @import("ast.zig");
const catalog = @import("catalog.zig");
pub const scalar = @import("scalar.zig");
const Allocator = std.mem.Allocator;
const parameter_frame = @import("parameter_frame.zig");

pub const Bound = struct {
    invocation: ?*@import("parameter_binding.zig").Invocation = null,
    parameter_descriptors: []const scalar.Type = &.{},
    typed_parameters: bool = false,
    columns: []const scalar.Column = &.{},
    projections: []const ?scalar.Program = &.{},
    orders: []const ?scalar.Program = &.{},
    assignments: []const ?scalar.Program = &.{},
    insert_rows: []const []const ?scalar.Program = &.{},
    predicate: ?scalar.Program = null,
    required: []const u32 = &.{},

    /// The immutable binding must outlive the execution. All programs borrow
    /// one bounded frame; decoding and descriptor checks never enter row loops.
    pub fn prepareParameters(self: *const Bound, backing: Allocator, inputs: []const parameter_frame.Input, limits: parameter_frame.Limits) !Prepared {
        if (!self.typed_parameters) return error.UnsupportedSqlShape;
        var frame = try parameter_frame.Frame.prepare(backing, self.parameter_descriptors, inputs, limits);
        errdefer frame.deinit();
        return Prepared.init(self, frame) catch |err| {
            if (err == error.OutOfMemory and frame.budget.exhausted) return error.SqlProgramLimitExceeded;
            return err;
        };
    }

    pub fn validateDecisions(self: Bound, alloc: Allocator, parameters: []const std.json.Value, provider: ?@import("../functions/decisions.zig").DecisionProvider) !void {
        if (self.typed_parameters and self.invocation == null) return error.UnsupportedSqlShape;
        const evaluator = @import("decision_eval.zig");
        if (self.predicate) |*program| try evaluator.validate(alloc, provider, program, parameters);
        for (self.projections) |optional| if (optional) |*program| try evaluator.validate(alloc, provider, program, parameters);
        for (self.orders) |optional| if (optional) |*program| try evaluator.validate(alloc, provider, program, parameters);
        for (self.assignments) |optional| if (optional) |*program| try evaluator.validate(alloc, provider, program, parameters);
        for (self.insert_rows) |row| for (row) |optional| if (optional) |*program| try evaluator.validate(alloc, provider, program, parameters);
    }

    /// Page-local cells. Required ordinals only are materialized; generated
    /// expression values use the same page arena and cannot retain prior pages.
    pub fn cells(self: Bound, alloc: Allocator, row: catalog.Row) ![]const scalar.Datum {
        if (self.required.len == 0) return &.{};
        const out = try alloc.alloc(scalar.Datum, self.columns.len);
        @memset(out, .{});
        for (self.required) |ordinal| {
            const cell = try row.cell(self.columns[ordinal].name);
            out[ordinal] = try @import("describe.zig").coerceDatum(alloc, cell, self.columns[ordinal].type, self.columns[ordinal].element_type);
        }
        return out;
    }

    /// Bind borrowed typed vectors directly to scalar ordinals. Primitive
    /// values need no JSON object, serialization or per-cell heap allocation.
    pub fn columnCells(self: Bound, alloc: Allocator, page: catalog.ColumnPage, index: usize) ![]const scalar.Datum {
        if (self.required.len == 0) return &.{};
        const out = try alloc.alloc(scalar.Datum, self.columns.len);
        @memset(out, .{});
        for (self.required) |ordinal| {
            const cell = try page.cell(alloc, index, self.columns[ordinal].name);
            out[ordinal] = try @import("describe.zig").coerceDatum(alloc, cell, self.columns[ordinal].type, self.columns[ordinal].element_type);
        }
        return out;
    }

    pub fn matches(self: Bound, alloc: Allocator, values: []const scalar.Datum, parameters: []const std.json.Value) !bool {
        return self.matchesWithProvider(alloc, values, parameters, null);
    }

    pub fn matchesWithProvider(self: Bound, alloc: Allocator, values: []const scalar.Datum, parameters: []const std.json.Value, provider: ?@import("../functions/decisions.zig").DecisionProvider) !bool {
        return self.matchesWithLimits(alloc, values, parameters, provider, .{});
    }

    /// Predicate evaluation must retain the statement's scalar resource owner
    /// and cancellation controls even on row-based fallback execution paths.
    pub fn matchesWithLimits(self: Bound, alloc: Allocator, values: []const scalar.Datum, parameters: []const std.json.Value, provider: ?@import("../functions/decisions.zig").DecisionProvider, limits: scalar.EvalLimits) !bool {
        if (self.typed_parameters and self.invocation == null) return error.UnsupportedSqlShape;
        const program = self.predicate orelse return true;
        const value = try @import("decision_eval.zig").evaluateWithLimits(alloc, provider, &program, values, parameters, limits);
        if (value.sql_null) return false;
        if (value.value != .bool) return error.SqlTypeMismatch;
        return value.value.bool;
    }
};

/// Execution-owned parameters and prebound program views. Values borrowed from
/// the frame live until deinit; computed values use the caller's page arena.
pub const Prepared = struct {
    frame: parameter_frame.Frame,
    predicate: ?scalar.PreparedEvaluation,
    projections: []const ?scalar.PreparedEvaluation,
    orders: []const ?scalar.PreparedEvaluation,
    assignments: []const ?scalar.PreparedEvaluation,
    insert_rows: []const []const ?scalar.PreparedEvaluation,

    fn init(bound: *const Bound, frame: parameter_frame.Frame) !Prepared {
        const a = frame.arena.allocator();
        const rows = try a.alloc([]const ?scalar.PreparedEvaluation, bound.insert_rows.len);
        for (bound.insert_rows, rows) |programs, *row| row.* = try bindPrograms(a, programs, &frame);
        return .{
            .frame = frame,
            .predicate = if (bound.predicate) |*program| try program.bindParameters(&frame) else null,
            .projections = try bindPrograms(a, bound.projections, &frame),
            .orders = try bindPrograms(a, bound.orders, &frame),
            .assignments = try bindPrograms(a, bound.assignments, &frame),
            .insert_rows = rows,
        };
    }

    fn bindPrograms(a: Allocator, programs: []const ?scalar.Program, frame: *const parameter_frame.Frame) ![]const ?scalar.PreparedEvaluation {
        const result = try a.alloc(?scalar.PreparedEvaluation, programs.len);
        for (programs, result) |*optional, *prepared| prepared.* = if (optional.*) |*program| try program.bindParameters(frame) else null;
        return result;
    }

    pub fn deinit(self: *Prepared) void {
        self.frame.deinit();
        self.* = undefined;
    }

    pub fn validateDecisions(self: Prepared, a: Allocator, provider: ?@import("../functions/decisions.zig").DecisionProvider) !void {
        const evaluator = @import("decision_eval.zig");
        if (self.predicate) |program| try evaluator.validatePrepared(a, provider, program);
        for (self.projections) |optional| if (optional) |program| try evaluator.validatePrepared(a, provider, program);
        for (self.orders) |optional| if (optional) |program| try evaluator.validatePrepared(a, provider, program);
        for (self.assignments) |optional| if (optional) |program| try evaluator.validatePrepared(a, provider, program);
        for (self.insert_rows) |row| for (row) |optional| if (optional) |program| try evaluator.validatePrepared(a, provider, program);
    }

    pub fn matches(self: Prepared, a: Allocator, cells: []const scalar.Datum, provider: ?@import("../functions/decisions.zig").DecisionProvider) !bool {
        const program = self.predicate orelse return true;
        const value = try @import("decision_eval.zig").evaluatePrepared(a, provider, program, cells);
        if (value.sql_null) return false;
        if (value.value != .bool) return error.SqlTypeMismatch;
        return value.value.bool;
    }
};

pub fn needsResidual(table: ?catalog.Table, predicate: ?*const ast.Predicate) bool {
    const node = predicate orelse return false;
    return switch (node.*) {
        .comparison => |comparison| jsonColumn(table, comparison.field) or (std.mem.eql(u8, comparison.field, "_id") and comparison.op != .eq) or
            (comparison.value == .string and std.mem.eql(u8, std.mem.trim(u8, comparison.value.string, " \t\r\n"), "null")),
        .is_null => |condition| jsonColumn(table, condition.field),
        .conjunction => |pair| needsResidual(table, pair.left) or needsResidual(table, pair.right),
        .disjunction, .negation, .scalar => true,
    };
}

fn jsonColumn(table: ?catalog.Table, name: []const u8) bool {
    const definition = table orelse return false;
    const column = definition.column(name) catch return false;
    return column.type == .json;
}

test "SQL bound scalar cells preserve typed array descriptors and reject JSON substitutes" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const table: catalog.Table = .{ .id = 1, .physical_name = "items", .schema_version = 1, .columns = &.{.{ .name = "a", .path = "a", .type = .array, .element_type = .int64 }} };
    var compiled = try @import("compiler.zig").compile(a, "SELECT cardinality(a), array_lower(a, 1), array_upper(a, 1), 9007199254740993 = ANY(a), 9007199254740992 = ANY(a) FROM items", .{});
    defer compiled.deinit();
    const bound = try bind(a, table, compiled.statement, &.{});
    try std.testing.expectEqual(@as(?@import("array_value.zig").ElementType, .int64), bound.columns[0].element_type);
    var array = try @import("array_value.zig").Value.init(.int64, &.{.{ .length = 2, .lower = -3 }}, &.{ scalar.Datum.json(.{ .integer = 9007199254740993 }), .{} }, .{});
    const row = try catalog.Row.fromDatums(a, "", try catalog.Row.TypedLayout.init(a, &.{"a"}), &.{scalar.Datum.typedArray(&array)});
    const values = try bound.cells(a, row);
    for (bound.projections[0..3], [_]i64{ 2, -3, -2 }) |program, integer| try std.testing.expectEqual(integer, (try program.?.evaluate(a, values, &.{}, .{})).value.integer);
    try std.testing.expect((try bound.projections[3].?.evaluate(a, values, &.{}, .{})).value.bool);
    try std.testing.expect((try bound.projections[4].?.evaluate(a, values, &.{}, .{})).sql_null);
    const describe = @import("describe.zig");
    var none = std.heap.FixedBufferAllocator.init(&.{});
    // Borrowing an already-owned typed value does not allocate per element.
    const borrowed = try describe.coerceDatum(none.allocator(), values[0], .array, .int64);
    try std.testing.expect(borrowed.array == values[0].array);
    try std.testing.expectError(error.SqlTypeMismatch, describe.coerceDatum(a, values[0], .json, null));
    try std.testing.expectError(error.SqlTypeMismatch, describe.coerceDatum(a, values[0], .array, .float64));
    try std.testing.expectError(error.SqlTypeMismatch, describe.coerceDatum(a, scalar.Datum.json(.null), .array, .int64));
    try std.testing.expect((try describe.coerceDatum(a, .{}, .array, .int64)).sql_null);
}

test "SQL statements converge typed descriptors before emission and share one owned frame" {
    const a = std.testing.allocator;
    const table: catalog.Table = .{ .id = 1, .physical_name = "items", .schema_version = 1, .columns = &.{.{ .name = "needle", .path = "needle", .type = .integer }} };
    for ([_][]const u8{
        "SELECT $1, cardinality($1::bigint[]) FROM items WHERE needle = ANY($1) ORDER BY cardinality($1)",
        "SELECT cardinality($1::bigint[]), $1 FROM items WHERE needle = ANY($1) ORDER BY cardinality($1)",
    }) |sql| {
        var compiled = try @import("compiler.zig").compile(a, sql, .{});
        defer compiled.deinit();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        var types: [1]scalar.Type = .{.{ .kind = .array, .element_type = .int64 }};
        const bound = try bindTyped(arena.allocator(), table, compiled.statement, &types, null, &.{});
        try std.testing.expectEqual(@as(?ast.ColumnType, .array), types[0].kind);
        try std.testing.expectEqual(@as(?@import("array_value.zig").ElementType, .int64), types[0].element_type);
        var input = [_]u8{ '{', '1', ',', '2', ',', 'N', 'U', 'L', 'L', '}' };
        var prepared = try bound.prepareParameters(a, &.{.{ .text = &input }}, .{});
        defer prepared.deinit();
        @memset(&input, 0);
        // Pointers must refer to programs owned by Bound, not loop temporaries.
        for (prepared.projections, bound.projections) |view, *original| try std.testing.expect(view.?.program == &original.*.?);
        var none = std.heap.FixedBufferAllocator.init(&.{});
        try prepared.validateDecisions(none.allocator(), null);
        const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
        for (0..10000) |_| {
            try std.testing.expect(try prepared.matches(none.allocator(), &.{scalar.Datum.json(.{ .integer = 2 })}, null));
            try std.testing.expectEqual(@as(i64, 3), (try prepared.orders[0].?.evaluate(none.allocator(), &.{}, .{})).value.integer);
            for (prepared.projections) |view| {
                const result = try view.?.evaluate(none.allocator(), &.{}, .{});
                if (result.array) |array| try std.testing.expect(array == prepared.frame.values[0].array.?) else try std.testing.expectEqual(@as(i64, 3), result.value.integer);
            }
        }
        std.debug.print("SQL statement parameter frame: rows=10000 programs=4 decode_count=1 evaluation_scratch_bytes=0 peak_bytes={} elapsed_ns={}\n", .{ prepared.frame.budget.peak, std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start });
        try std.testing.expect(!try prepared.matches(none.allocator(), &.{scalar.Datum.json(.{ .integer = 9 })}, null));
        try std.testing.expectError(error.UnsupportedSqlShape, bound.matches(none.allocator(), &.{}, &.{}));
    }
}

test "SQL typed mutation scalar bindings preserve assignment parameter identity" {
    const a = std.testing.allocator;
    const table: catalog.Table = .{ .id = 1, .physical_name = "items", .schema_version = 1, .columns = &.{.{ .name = "needle", .path = "needle", .type = .integer }} };
    for ([_][]const u8{
        "UPDATE items SET needle = cardinality($1::integer[]) WHERE needle = $2",
        "UPDATE items SET needle = $1 WHERE needle = $2",
        "INSERT INTO items (needle) VALUES (cardinality($1::integer[])), ($2)",
    }, 0..) |sql, index| {
        var compiled = try @import("compiler.zig").compile(a, sql, .{});
        defer compiled.deinit();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        var types: [2]scalar.Type = .{ .{}, .{} };
        const bound = try bindTyped(arena.allocator(), table, compiled.statement, &types, null, &.{});
        var prepared = try bound.prepareParameters(a, &.{ .{ .text = if (index == 1) "3" else "{1,2,3}" }, .{ .text = "7" } }, .{});
        defer prepared.deinit();
        var none = std.heap.FixedBufferAllocator.init(&.{});
        if (index < 2) {
            try std.testing.expect(try prepared.matches(none.allocator(), &.{scalar.Datum.json(.{ .integer = 7 })}, null));
            try std.testing.expectEqual(@as(i64, 3), (try prepared.assignments[0].?.evaluate(none.allocator(), &.{}, .{})).value.integer);
        } else {
            try std.testing.expectEqual(@as(i64, 3), (try prepared.insert_rows[0][0].?.evaluate(none.allocator(), &.{}, .{})).value.integer);
            try std.testing.expectEqual(@as(i64, 7), (try prepared.insert_rows[1][0].?.evaluate(none.allocator(), &.{}, .{})).value.integer);
        }
    }
}

test "SQL typed statement frames reject invalid admission and unwind all allocation faults" {
    const Faults = struct {
        fn run(backing: Allocator) !void {
            var vtable = backing.vtable.*;
            vtable.resize = Allocator.noResize;
            vtable.remap = Allocator.noRemap;
            const a: Allocator = .{ .ptr = backing.ptr, .vtable = &vtable };
            var compiled = try @import("compiler.zig").compile(a, "SELECT cardinality($1::integer[]), $1 ORDER BY cardinality($1)", .{});
            defer compiled.deinit();
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            var types: [1]scalar.Type = .{.{}};
            const bound = try bindTyped(arena.allocator(), null, compiled.statement, &types, null, &.{});
            var prepared = try bound.prepareParameters(a, &.{.{ .text = "{1,2,NULL}" }}, .{});
            defer prepared.deinit();
            try std.testing.expect(prepared.projections[0].?.parameters.ptr == prepared.orders[0].?.parameters.ptr);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Faults.run, .{});
    var compiled = try @import("compiler.zig").compile(std.testing.allocator, "SELECT cardinality($1::integer[]), $1", .{});
    defer compiled.deinit();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var types: [1]scalar.Type = .{.{}};
    const bound = try bindTyped(arena.allocator(), null, compiled.statement, &types, null, &.{});
    try std.testing.expectError(error.SqlProgramLimitExceeded, bound.prepareParameters(std.testing.allocator, &.{.{ .text = "{1,2,NULL}" }}, .{ .bytes = 1 }));
    try std.testing.expectError(error.InvalidSqlParameters, bound.prepareParameters(std.testing.allocator, &.{}, .{}));
}

test "SQL inferred bare target parameters retain PostgreSQL ambiguity diagnostics" {
    for ([_][]const u8{ "SELECT $1, cardinality($1::integer[])", "SELECT $1, $1::smallint" }) |sql| {
        var compiled = try @import("compiler.zig").compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        var types: [1]scalar.Type = .{.{}};
        try std.testing.expectError(error.ConflictingSqlParameterTypes, bindTyped(arena.allocator(), null, compiled.statement, &types, null, &.{}));
    }
}

pub fn predicateScalar(alloc: Allocator, table: ?catalog.Table, predicate: *const ast.Predicate) !*const ast.Scalar {
    var builder: Builder = .{ .alloc = alloc, .table = table, .columns = &.{}, .parameters = &.{} };
    return builder.predicateExpression(predicate);
}

/// allocator is the bounded binding arena. Nested Program arenas allocate from
/// it, so the enclosing binding owns every program and frees them together.
pub fn bind(alloc: Allocator, table: ?catalog.Table, statement: ast.Statement, parameters: []?ast.ColumnType) !Bound {
    return bindWithSettings(alloc, table, statement, parameters, null);
}

pub fn bindWithSettings(alloc: Allocator, table: ?catalog.Table, statement: ast.Statement, parameters: []?ast.ColumnType, settings: ?*const @import("setting_catalog.zig").View) !Bound {
    return bindWithParameterFallback(alloc, table, statement, parameters, settings, &.{});
}

pub fn bindWithParameterFallback(alloc: Allocator, table: ?catalog.Table, statement: ast.Statement, parameters: []?ast.ColumnType, settings: ?*const @import("setting_catalog.zig").View, fallbacks: []const ?ast.ColumnType) !Bound {
    return bindWithInvocation(alloc, table, statement, parameters, settings, fallbacks, null);
}

pub fn bindWithInvocation(alloc: Allocator, table: ?catalog.Table, statement: ast.Statement, parameters: []?ast.ColumnType, settings: ?*const @import("setting_catalog.zig").View, fallbacks: []const ?ast.ColumnType, invocation: ?*@import("parameter_binding.zig").Invocation) !Bound {
    if (parameters.len > 1024 or fallbacks.len > 1024) return error.SqlProgramLimitExceeded;
    const descriptors = try alloc.alloc(scalar.Type, parameters.len);
    for (parameters, descriptors) |kind, *descriptor| descriptor.* = .{ .kind = kind };
    if (invocation) |owner| {
        try owner.mergeCoarse(parameters);
        if (parameters.len != owner.descriptors.len) return error.InvalidSqlParameters;
        @memcpy(descriptors, owner.descriptors);
    }
    const fallback_descriptors = try alloc.alloc(scalar.Type, fallbacks.len);
    for (fallbacks, fallback_descriptors) |kind, *descriptor| descriptor.* = .{ .kind = kind };
    const result = try bindDescriptorsWithInvocation(alloc, table, statement, descriptors, settings, fallback_descriptors, invocation != null, invocation);
    for (parameters, if (invocation) |owner| owner.descriptors else descriptors) |*kind, descriptor| kind.* = descriptor.kind;
    return result;
}

pub fn bindTyped(alloc: Allocator, table: ?catalog.Table, statement: ast.Statement, parameters: []scalar.Type, settings: ?*const @import("setting_catalog.zig").View, fallbacks: []const scalar.Type) !Bound {
    if (parameters.len > 1024 or fallbacks.len > 1024) return error.SqlProgramLimitExceeded;
    for (parameters) |descriptor| try scalar.validateParameterType(descriptor);
    for (fallbacks) |descriptor| try scalar.validateParameterType(descriptor);
    return bindDescriptors(alloc, table, statement, parameters, settings, fallbacks, true);
}

fn bindDescriptors(alloc: Allocator, table: ?catalog.Table, statement: ast.Statement, parameters: []scalar.Type, settings: ?*const @import("setting_catalog.zig").View, fallbacks: []const scalar.Type, typed_parameters: bool) !Bound {
    return bindDescriptorsWithInvocation(alloc, table, statement, parameters, settings, fallbacks, typed_parameters, null);
}

fn bindDescriptorsWithInvocation(alloc: Allocator, table: ?catalog.Table, statement: ast.Statement, parameters: []scalar.Type, settings: ?*const @import("setting_catalog.zig").View, fallbacks: []const scalar.Type, typed_parameters: bool, invocation: ?*@import("parameter_binding.zig").Invocation) !Bound {
    if (statement == .insert) return bindInsert(alloc, table orelse return error.UndefinedTable, statement.insert, parameters, settings, fallbacks, typed_parameters, invocation);
    const needed = switch (statement) {
        .select => |select| blk: {
            if (needsResidual(table, select.predicate)) break :blk true;
            for (select.columns) |projection| if (projection.expression != null) break :blk true;
            for (select.order_by) |order| if (order.expression != null) break :blk true;
            break :blk false;
        },
        .update => |update| blk: {
            if (needsResidual(table, update.predicate)) break :blk true;
            for (update.assignments) |assignment| {
                if (assignment.expression != null) break :blk true;
                if (table) |definition| if ((try definition.column(assignment.field)).type == .array and (assignment.value == .string or assignment.value == .parameter)) break :blk true;
            }
            break :blk false;
        },
        .delete => |delete| needsResidual(table, delete.predicate),
        else => false,
    };
    // Catalog-directed literals/parameters use Context.value's prepared frame.
    // Do not manufacture scalar programs and residual cells for native seeks,
    // direct replacements or row bounds that already have a typed fast path.
    if (table != null and !needed and (!typed_parameters or invocation != null)) return .{ .invocation = invocation };
    const table_columns: []const catalog.Column = if (table) |definition| definition.columns else &.{};
    const relations = @import("relation_binding.zig");
    const qualified_names = switch (statement) {
        .update => |mutation| qualified: {
            if (relations.hasQualifiedPredicate(mutation.predicate)) break :qualified true;
            for (mutation.assignments) |assignment| if (assignment.expression) |expression| if (relations.hasQualifiedScalar(expression)) break :qualified true;
            break :qualified false;
        },
        .delete => |mutation| relations.hasQualifiedPredicate(mutation.predicate),
        .select => |selection| relations.accepts(selection),
        else => false,
    };
    const columns = try alloc.alloc(scalar.Column, table_columns.len + @intFromBool(table != null));
    for (table_columns, columns[0..table_columns.len]) |column, *out| out.* = .{ .name = column.name, .type = column.type, .element_type = column.element_type, .nullable = column.nullable, .aliases = if (qualified_names) try table.?.columnAliases(alloc, column.name) else &.{} };
    if (table != null) columns[table_columns.len] = .{ .name = "_id", .type = .string, .nullable = false, .aliases = if (qualified_names) try table.?.columnAliases(alloc, "_id") else &.{} };
    var builder: Builder = .{ .alloc = alloc, .table = table, .columns = columns, .parameters = parameters, .settings = settings, .fallbacks = fallbacks, .typed_parameters = typed_parameters, .invocation = invocation };
    var out: Bound = .{ .columns = columns, .typed_parameters = typed_parameters, .invocation = invocation };
    const predicate = switch (statement) {
        .select => |select| select.predicate,
        .update => |update| update.predicate,
        .delete => |delete| delete.predicate,
        else => null,
    };
    const predicate_expression = if (predicate) |node| try builder.predicateExpression(node) else null;
    // PostgreSQL resolves SELECT targets in source order. An untyped bare
    // target retains its unknown/text identity even when a later occurrence
    // constrains the parameter. Declared wire types do not have this ambiguity.
    var unresolved_targets: [1024]bool = @splat(false);
    if (typed_parameters and statement == .select) {
        for (statement.select.columns) |projection| if (projection.expression) |expression| {
            if (expression.* == .literal and expression.literal == .parameter) {
                const slot = expression.literal.parameter;
                if (slot == 0 or slot > parameters.len) return error.InvalidSqlParameters;
                if (parameters[slot - 1].kind == null) unresolved_targets[slot - 1] = true;
            }
            _ = try builder.infer(expression, null);
        };
    }
    // Converge shared descriptors before emitting any program. Programs must
    // not freeze different input identities as projections are emitted.
    var pass: usize = 0;
    while (true) : (pass += 1) {
        if (pass > parameters.len + 1) return error.ConflictingSqlParameterTypes;
        var changed = false;
        if (predicate_expression) |expression| changed = try builder.infer(expression, .boolean) or changed;
        switch (statement) {
            .select => |select| {
                for (select.columns) |projection| if (projection.expression) |expression| {
                    changed = try builder.infer(expression, null) or changed;
                };
                for (select.order_by) |order| if (order.expression) |expression| {
                    changed = try builder.infer(expression, null) or changed;
                };
                for ([_]?ast.Value{ select.limit, select.offset }) |optional| if (optional) |node| {
                    if (node == .parameter) {
                        if (node.parameter == 0 or node.parameter > parameters.len) return error.InvalidSqlParameters;
                        const slot = &parameters[node.parameter - 1];
                        if (slot.kind) |kind| {
                            if (kind != .integer) return error.ConflictingSqlParameterTypes;
                        } else {
                            slot.* = .{ .kind = .integer, .element_type = if (typed_parameters) .int64 else null };
                            changed = true;
                        }
                    }
                };
            },
            .update => |update| for (update.assignments) |assignment| {
                const column = try (table orelse return error.UndefinedColumn).column(assignment.field);
                if (assignment.expression == null and assignment.value != .parameter and !(column.type == .array and assignment.value == .string)) continue;
                const expression = try builder.assignmentNode(assignment.expression, assignment.value, column);
                changed = try builder.inferAssignment(expression, column) or changed;
            },
            else => {},
        }
        if (!changed) break;
    }
    try builder.freeze();
    for (parameters, unresolved_targets[0..parameters.len]) |*parameter, unresolved| {
        if (unresolved and parameter.kind == null) parameter.* = .{ .kind = .string, .element_type = .text };
        if (unresolved and parameter.kind != .string) return error.ConflictingSqlParameterTypes;
    }
    if (predicate != null and (typed_parameters or table == null or needsResidual(table, predicate))) {
        out.predicate = try builder.program(predicate_expression.?, .boolean);
        if (out.predicate.?.output_type.kind != null and out.predicate.?.output_type.kind != .boolean) return error.SqlTypeMismatch;
    }
    switch (statement) {
        .select => |select| {
            const programs = try alloc.alloc(?scalar.Program, select.columns.len);
            @memset(programs, null);
            for (select.columns, programs) |projection, *program| if (projection.expression) |expression| {
                program.* = try builder.program(expression, null);
            };
            out.projections = programs;
            const orders = try alloc.alloc(?scalar.Program, select.order_by.len);
            @memset(orders, null);
            for (select.order_by, orders) |order, *program| if (order.expression) |expression| {
                program.* = try builder.program(expression, null);
            };
            out.orders = orders;
        },
        .update => |update| {
            const programs = try alloc.alloc(?scalar.Program, update.assignments.len);
            @memset(programs, null);
            for (update.assignments, programs) |assignment, *program| {
                const column = try (table orelse return error.UndefinedColumn).column(assignment.field);
                if (assignment.expression == null and !(typed_parameters and assignment.value == .parameter) and !(column.type == .array and assignment.value == .string)) continue;
                const expression = try builder.assignmentNode(assignment.expression, assignment.value, column);
                program.* = try builder.assignmentProgram(expression, column);
                const kind = program.*.?.output_type.kind;
                if (kind != null and kind != column.type and !(kind == .integer and column.type == .number)) return error.SqlTypeMismatch;
            }
            out.assignments = programs;
        },
        else => {},
    }
    out.required = try builder.required.toOwnedSlice(alloc);
    out.parameter_descriptors = try alloc.dupe(scalar.Type, parameters);
    return out;
}

fn bindInsert(alloc: Allocator, table: catalog.Table, statement: ast.Insert, parameters: []scalar.Type, settings: ?*const @import("setting_catalog.zig").View, fallbacks: []const scalar.Type, typed_parameters: bool, invocation: ?*@import("parameter_binding.zig").Invocation) !Bound {
    if (statement.expressions.len == 0) return .{ .parameter_descriptors = try alloc.dupe(scalar.Type, parameters), .typed_parameters = typed_parameters, .invocation = invocation };
    if (statement.expressions.len != statement.rows.len) return error.InvalidSqlParameters;
    if (statement.defaults.len != 0) {
        if (statement.defaults.len != statement.rows.len) return error.InvalidSqlParameters;
        for (statement.defaults) |mask| if (mask.len != statement.columns.len) return error.InvalidSqlParameters;
    }
    var builder: Builder = .{ .alloc = alloc, .table = null, .columns = &.{}, .parameters = parameters, .settings = settings, .fallbacks = fallbacks, .typed_parameters = typed_parameters, .invocation = invocation };
    var pass: usize = 0;
    while (true) : (pass += 1) {
        if (pass > parameters.len + 1) return error.ConflictingSqlParameterTypes;
        var changed = false;
        for (statement.rows, statement.expressions, 0..) |row, expressions, row_index| {
            if (row.len != statement.columns.len or expressions.len != row.len) return error.InvalidSqlParameters;
            for (row, expressions, statement.columns, 0..) |literal, expression, name, cell_index| {
                if (statement.isDefault(row_index, cell_index)) continue;
                const column = try table.column(name);
                const node = try builder.assignmentNode(expression, literal, column);
                changed = try builder.inferAssignment(node, column) or changed;
            }
        }
        if (!changed) break;
    }
    try builder.freeze();
    const rows = try alloc.alloc([]const ?scalar.Program, statement.rows.len);
    for (statement.expressions, statement.rows, rows) |expressions, literals, *row| {
        const programs = try alloc.alloc(?scalar.Program, expressions.len);
        @memset(programs, null);
        for (expressions, literals, statement.columns, programs) |expression, literal, name, *program| {
            const column = try table.column(name);
            if (expression == null and !(typed_parameters and literal == .parameter) and !(column.type == .array and literal == .string)) continue;
            const node = try builder.assignmentNode(expression, literal, column);
            program.* = try builder.assignmentProgram(node, column);
            const kind = program.*.?.output_type.kind;
            if (kind != null and kind != column.type and !(kind == .integer and column.type == .number)) return error.SqlTypeMismatch;
        }
        row.* = programs;
    }
    return .{ .insert_rows = rows, .parameter_descriptors = try alloc.dupe(scalar.Type, parameters), .typed_parameters = typed_parameters, .invocation = invocation };
}

fn arrayAssignmentBindingScenario(backing: Allocator) !void {
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    const table: catalog.Table = .{ .id = 1, .physical_name = "items", .schema_version = 1, .columns = &.{
        .{ .name = "a", .path = "a", .type = .array, .element_type = .int64 },
        .{ .name = "b", .path = "b", .type = .array, .element_type = .int16 },
    } };
    var compiled = try @import("compiler.zig").compile(backing, "INSERT INTO items (_id,a,b) VALUES ('x','[-1:1]={1,NULL,2}',$1)", .{});
    defer compiled.deinit();
    var descriptors = [_]scalar.Type{.{}};
    const bound = try bindTyped(a, table, compiled.statement, &descriptors, null, &.{});
    try std.testing.expectEqual(@as(?@import("array_value.zig").ElementType, .int16), descriptors[0].element_type);
    var prepared = try bound.prepareParameters(backing, &.{.{ .text = "{3,NULL}" }}, .{});
    defer prepared.deinit();
    var none = std.heap.FixedBufferAllocator.init(&.{});
    for (0..10000) |_| {
        const literal = try prepared.insert_rows[0][1].?.evaluate(none.allocator(), &.{}, .{});
        try std.testing.expectEqual(@as(i32, -1), literal.array.?.dimensions[0].lower);
        try std.testing.expect(literal.array.?.elements[1].sql_null);
        const parameter = try prepared.insert_rows[0][2].?.evaluate(none.allocator(), &.{}, .{});
        try std.testing.expect(parameter.array == prepared.frame.values[0].array);
    }
}

test "SQL array assignments prepare unknown literals and precise parameters once with no row scratch" {
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, arrayAssignmentBindingScenario, .{});
}

test "SQL undeclared repeated array assignment parameters retain one consistent target identity" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const table: catalog.Table = .{ .id = 1, .physical_name = "items", .schema_version = 1, .columns = &.{
        .{ .name = "a", .path = "a", .type = .array, .element_type = .int64 },
        .{ .name = "b", .path = "b", .type = .array, .element_type = .int16 },
    } };
    var compiled = try @import("compiler.zig").compile(a, "INSERT INTO items (b,a) VALUES ($1,$1)", .{});
    defer compiled.deinit();
    var unknown = [_]scalar.Type{.{}};
    try std.testing.expectError(error.ConflictingSqlParameterTypes, bindTyped(a, table, compiled.statement, &unknown, null, &.{}));
    var declared = [_]scalar.Type{.{ .kind = .array, .element_type = .int16 }};
    _ = try bindTyped(a, table, compiled.statement, &declared, null, &.{});
    try std.testing.expectEqual(@as(?@import("array_value.zig").ElementType, .int16), declared[0].element_type);
}

const Builder = struct {
    invocation: ?*@import("parameter_binding.zig").Invocation = null,
    alloc: Allocator,
    table: ?catalog.Table,
    columns: []const scalar.Column,
    parameters: []scalar.Type,
    typed_parameters: bool = false,
    settings: ?*const @import("setting_catalog.zig").View = null,
    fallbacks: []const scalar.Type = &.{},
    required: std.ArrayList(u32) = .empty,
    seen: std.AutoHashMapUnmanaged(u32, void) = .empty,
    assignment_contexts: std.AutoHashMapUnmanaged(u32, scalar.Type) = .empty,

    fn program(self: *Builder, expression: *const ast.Scalar, expected: ?ast.ColumnType) !scalar.Program {
        return self.programType(expression, if (expected) |kind| .{ .kind = kind } else null, false);
    }

    fn programType(self: *Builder, expression: *const ast.Scalar, expected: ?scalar.Type, assignment: bool) !scalar.Program {
        // The statement-wide constraint pass has converged before emission.
        // Actual input kinds resolve polymorphic holes, not known SQL types.
        var coarse: [1024]?ast.ColumnType = undefined;
        for (self.parameters, coarse[0..self.parameters.len]) |descriptor, *kind| kind.* = descriptor.kind;
        const result = if (self.typed_parameters or (self.parameters.len == 0 and expected != null and expected.?.kind == .array)) try scalar.bindTypedExpectedWithSettings(self.alloc, expression, self.columns, self.parameters, expected, .{ .invocation = self.invocation, .assignment = assignment }, self.settings) else try scalar.bindExpectedWithSettings(self.alloc, expression, self.columns, coarse[0..self.parameters.len], if (expected) |kind| kind.kind else null, .{ .assignment = assignment }, self.settings);
        if (result.parameter_types.len > self.parameters.len) return error.InvalidSqlParameters;
        for (result.parameter_descriptors, self.parameters[0..result.parameter_types.len]) |inferred, *existing| {
            if (inferred.kind) |kind| {
                if (existing.kind) |prior| if (prior != kind or (self.typed_parameters and existing.element_type != null and existing.element_type != inferred.element_type)) return error.ConflictingSqlParameterTypes;
                existing.* = inferred;
            }
        }
        for (result.required_columns) |ordinal| {
            if (!(try self.seen.getOrPut(self.alloc, ordinal)).found_existing) try self.required.append(self.alloc, ordinal);
        }
        return result;
    }

    fn freeze(self: *Builder) !void {
        if (self.invocation) |owner| try owner.mergeInferred(self.parameters);
        for (self.parameters, 0..) |*parameter, index| {
            if (parameter.kind == null and index < self.fallbacks.len) parameter.* = self.fallbacks[index];
            if (!self.typed_parameters) continue;
            if (parameter.kind != null and parameter.kind != .datetime and parameter.element_type == null) parameter.element_type = try scalar.parameterElementType(parameter.*);
        }
    }

    fn infer(self: *Builder, expression: *const ast.Scalar, expected: ?ast.ColumnType) !bool {
        return self.inferType(expression, if (expected) |kind| .{ .kind = kind } else null, false);
    }

    fn inferType(self: *Builder, expression: *const ast.Scalar, expected: ?scalar.Type, assignment: bool) !bool {
        if (self.typed_parameters or (self.parameters.len == 0 and expected != null and expected.?.kind == .array)) return scalar.inferTypedParametersExpected(self.alloc, expression, self.columns, self.parameters, expected, .{ .assignment = assignment });
        var coarse: [1024]?ast.ColumnType = undefined;
        for (self.parameters, coarse[0..self.parameters.len]) |descriptor, *kind| kind.* = descriptor.kind;
        const changed = try scalar.inferParameters(self.alloc, expression, self.columns, coarse[0..self.parameters.len], if (expected) |kind| kind.kind else null, .{ .assignment = assignment });
        for (self.parameters, coarse[0..self.parameters.len]) |*descriptor, kind| descriptor.* = .{ .kind = kind };
        return changed;
    }

    fn assignmentNode(self: *Builder, expression: ?*const ast.Scalar, literal: ast.Value, column: catalog.Column) !*const ast.Scalar {
        const node_ = expression orelse try self.node(.{ .literal = literal });
        return scalar.assignmentExpression(self.alloc, node_, .{ .kind = column.type, .element_type = column.element_type });
    }

    fn inferAssignment(self: *Builder, expression: *const ast.Scalar, column: catalog.Column) !bool {
        var expected: scalar.Type = .{ .kind = column.type, .element_type = column.element_type };
        if (self.typed_parameters and expected.kind != .datetime and expected.element_type == null) expected.element_type = try scalar.parameterElementType(expected);
        if (self.typed_parameters and expression.* == .literal and expression.literal == .parameter) {
            const index = expression.literal.parameter;
            if (index == 0 or index > self.parameters.len) return error.InvalidSqlParameters;
            if (self.assignment_contexts.get(index)) |prior| {
                if (prior.kind != expected.kind or prior.element_type != expected.element_type) return error.ConflictingSqlParameterTypes;
            } else if (self.parameters[index - 1].kind == null) try self.assignment_contexts.put(self.alloc, index, expected);
        }
        return self.inferType(expression, expected, true);
    }

    fn assignmentProgram(self: *Builder, expression: *const ast.Scalar, column: catalog.Column) !scalar.Program {
        const program_ = try self.programType(expression, .{ .kind = column.type, .element_type = column.element_type }, true);
        if (program_.output_type.kind != null and program_.output_type.kind != column.type and !(program_.output_type.kind == .integer and column.type == .number)) return error.SqlAssignmentTypeMismatch;
        return program_;
    }

    fn node(self: *Builder, value: ast.Scalar) !*const ast.Scalar {
        const result = try self.alloc.create(ast.Scalar);
        result.* = value;
        return result;
    }

    fn predicateExpression(self: *Builder, predicate: *const ast.Predicate) anyerror!*const ast.Scalar {
        return switch (predicate.*) {
            .scalar => |expression| expression,
            .comparison => |comparison| blk: {
                const column = try (self.table orelse return error.UndefinedColumn).column(comparison.field);
                const literal = try self.node(.{ .literal = comparison.value });
                const right = if (comparison.value == .null) literal else try self.node(.{ .cast = .{ .operand = literal, .type = column.type } });
                break :blk try self.node(.{ .binary = .{
                    .op = switch (comparison.op) {
                        inline else => |tag| @field(ast.Scalar.Binary, @tagName(tag)),
                    },
                    .left = try self.node(.{ .column = comparison.field }),
                    .right = right,
                } });
            },
            .is_null => |check| try self.node(.{ .unary = .{ .op = if (check.negated) .is_not_null else .is_null, .operand = try self.node(.{ .column = check.field }) } }),
            .conjunction, .disjunction => |pair| try self.node(.{ .binary = .{ .op = if (predicate.* == .conjunction) .@"and" else .@"or", .left = try self.predicateExpression(pair.left), .right = try self.predicateExpression(pair.right) } }),
            .negation => |inner| try self.node(.{ .unary = .{ .op = .not, .operand = try self.predicateExpression(inner) } }),
        };
    }
};
