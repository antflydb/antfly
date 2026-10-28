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

//! Native scalar SQL bodies. Typed programs are prepared outside row loops;
//! declaration text is never interpreted by an execution callback.
const std = @import("std");
const ast = @import("ast.zig");
const scalar = @import("scalar.zig");
pub const records = @import("../system_catalog/routines.zig");
pub const bindings = @import("routine_bindings.zig");
pub const procedures = @import("procedure.zig");
pub const preparation_bytes = @import("resource_limits.zig").preparation_bytes;
const A = std.mem.Allocator;

pub fn storageType(declared: ast.RoutineDefinition.Type) !records.Type {
    if (declared.kind == .datetime) return .{ .scalar = switch (declared.temporal_kind orelse .timestamp_tz) {
        .date => .date,
        .time => .time,
        .time_tz => .time_tz,
        .timestamp => .timestamp,
        .timestamp_tz => .timestamp_tz,
        .interval => .interval,
    } };
    if (declared.kind == .numrange or declared.kind == .daterange or declared.kind == .tsrange or declared.kind == .tstzrange)
        return .{ .scalar = std.meta.stringToEnum(records.Type.Scalar, @tagName(declared.kind)).? };
    const element: @import("array_value.zig").ElementType = declared.element_type orelse switch (declared.kind) {
        .string => .text,
        .uuid => .uuid,
        .integer => .int64,
        .number => .float64,
        .boolean => .boolean,
        .json => .jsonb,
        else => return error.InvalidSqlRoutineRecord,
    };
    const result: records.Type = .{ .scalar = std.meta.stringToEnum(records.Type.Scalar, @tagName(element)).?, .array = declared.kind == .array };
    try result.validate();
    return result;
}

pub fn runtimeType(declared: records.Type) scalar.Type {
    const kind: ast.ColumnType = if (declared.array) .array else switch (declared.scalar) {
        .text => .string,
        .uuid => .uuid,
        .int16, .int32, .int64 => .integer,
        .float32, .float64, .numeric => .number,
        .boolean => .boolean,
        .jsonb => .json,
        .date, .time, .time_tz, .timestamp, .timestamp_tz, .interval => .datetime,
        .numrange => .numrange,
        .daterange => .daterange,
        .tsrange => .tsrange,
        .tstzrange => .tstzrange,
    };
    return .{ .kind = kind, .element_type = switch (declared.scalar) {
        .text, .uuid, .int16, .int32, .int64, .float32, .float64, .boolean, .jsonb, .numeric => std.meta.stringToEnum(@import("array_value.zig").ElementType, @tagName(declared.scalar)).?,
        else => null,
    }, .temporal_kind = if (kind == .datetime) std.meta.stringToEnum(@import("temporal_value.zig").Kind, @tagName(declared.scalar)).? else null };
}

pub fn definition(a: A, intent: ast.RoutineDefinition, owner: @import("../system_catalog/owners.zig").Owner) !records.Definition {
    const arguments = try a.alloc(records.Argument, intent.arguments.len);
    for (intent.arguments, arguments) |arg, *out| out.* = .{ .name = if (arg.name) |name| try a.dupe(u8, name) else null, .type = try storageType(arg.type) };
    const result: records.Definition = .{ .arguments = arguments, .result = if (intent.result) |result| try storageType(result) else null, .language = std.meta.stringToEnum(@FieldType(records.Definition, "language"), @tagName(intent.language)).?, .body = try a.dupe(u8, intent.body), .strict = intent.strict, .volatility = std.meta.stringToEnum(records.Volatility, @tagName(intent.volatility)).?, .owner = owner };
    try result.validate();
    return result;
}

pub fn prepare(a: A, record: records.Record, resolver: ?@import("routine_bindings.zig").Resolver, settings: ?*const @import("setting_catalog.zig").View) !*const @import("routine_bindings.zig").Resolved {
    try record.validate();
    if (record.kind == .operator) {
        const provider = resolver orelse return error.UnsupportedSqlShape;
        const resolve_reference = provider.resolve_reference orelse return error.UnsupportedSqlShape;
        const inputs = try a.alloc(bindings.Argument, record.definition.arguments.len);
        for (record.definition.arguments, inputs) |argument, *input| input.* = .{ .type = runtimeType(argument.type) };
        const function = try resolve_reference(provider.ptr, a, record.definition.function_reference.?, inputs);
        if (function.id != record.definition.function_reference.?.id or !std.meta.eql(function.output, runtimeType(record.definition.result.?))) {
            // Nullability is a runtime inference property; declared domains
            // remain invariant through the referenced function's replacement.
            if (function.id != record.definition.function_reference.?.id or function.output.kind != runtimeType(record.definition.result.?).kind or function.output.element_type != runtimeType(record.definition.result.?).element_type or function.output.temporal_kind != runtimeType(record.definition.result.?).temporal_kind) return error.CatalogGenerationChanged;
        }
        const alias = try a.create(bindings.Resolved);
        alias.* = function.*;
        alias.id = record.id;
        alias.generation = record.generation;
        return alias;
    }
    if (record.kind != .function) return error.SqlWrongObjectType;
    var parsed = try @import("compiler.zig").compile(a, record.definition.body, .{ .max_bytes = records.max_body_bytes });
    defer parsed.deinit();
    if (parsed.statement != .select) return error.SqlInvalidFunctionDefinition;
    if (parsed.parameter_count > record.definition.arguments.len) return error.SqlUndefinedParameter;
    const body = parsed.statement.select;
    if (body.table != null or body.source != null or body.ctes.len != 0 or body.set_operation != null or body.values_arms.len != 0 or
        body.distinct != null or body.windows.len != 0 or body.count_all or body.columns.len != 1 or body.predicate != null or
        body.group_by.len != 0 or body.having != null or body.order_by.len != 0 or body.limit != null or body.offset != null or body.columns[0].wildcard)
        return error.UnsupportedSqlShape;
    const descriptors = try a.alloc(scalar.Type, record.definition.arguments.len);
    const columns = try a.alloc(scalar.Column, descriptors.len);
    for (record.definition.arguments, descriptors, columns, 0..) |arg, *descriptor, *column, i| {
        descriptor.* = runtimeType(arg.type);
        column.* = .{ .name = arg.name orelse try std.fmt.allocPrint(a, "\x00routine_argument:{d}", .{i}), .type = descriptor.kind.?, .element_type = descriptor.element_type, .temporal_kind = descriptor.temporal_kind };
    }
    // Keep unnamed argument ordinals in the row frame, but never invent an
    // arg1/arg2 identifier: PostgreSQL exposes only $n for unnamed SQL inputs.
    const expression = body.columns[0].expression orelse &ast.Scalar{ .column = body.columns[0].field };
    const result = runtimeType(record.definition.result orelse return error.SqlWrongObjectType);
    const cast: ast.Scalar = .{ .cast = .{ .operand = expression, .type = result.kind.?, .element_type = result.element_type, .temporal_kind = result.temporal_kind, .coercion = .function, .context = .assignment } };
    const program = try a.create(scalar.Program);
    program.* = try scalar.bindTypedExpectedWithSettings(a, &cast, columns, descriptors, result, .{ .routines = resolver }, settings);
    // Suspending external effects need their own retained invocation directory;
    // do not let child ordinals alias the caller's sequence/decision receipts.
    if (@import("decision_eval.zig").hasExternal(program)) return error.UnsupportedSqlShape;
    const resolved = try a.create(@import("routine_bindings.zig").Resolved);
    resolved.* = .{ .id = record.id, .generation = record.generation, .arguments = descriptors, .output = result, .program = program, .strict = record.definition.strict };
    return resolved;
}

test "SQL routine prepared bodies preserve positional named null and return domains" {
    const cases = [_]struct { sql: []const u8, args: []const scalar.Datum, expected: []const u8 }{
        .{ .sql = "CREATE FUNCTION f(text,text) RETURNS text LANGUAGE sql AS $$SELECT concat_ws(':',lower($1),coalesce($2,'missing'))$$", .args = &.{ scalar.Datum.json(.{ .string = "READY" }), .{} }, .expected = "ready:missing" },
        .{ .sql = "CREATE FUNCTION f(status_text text) RETURNS text LANGUAGE sql AS $$SELECT lower(status_text)$$", .args = &.{scalar.Datum.json(.{ .string = "READY" })}, .expected = "ready" },
    };
    for (cases) |entry| {
        var region: std.heap.ArenaAllocator = .init(std.testing.allocator);
        defer region.deinit();
        const a = region.allocator();
        var parsed = try @import("compiler.zig").compile(a, entry.sql, .{});
        defer parsed.deinit();
        const declared = try definition(a, parsed.statement.catalog_ddl.routine.?, .auth_disabled);
        const bound = try prepare(a, .{ .id = 7, .generation = 1, .definition = declared }, null, null);
        const Owner = struct {
            bound: *const bindings.Resolved,
            fn resolve(raw: *anyopaque, _: A, _: []const u8, _: []const bindings.Argument) !*const bindings.Resolved {
                return (@as(*@This(), @ptrCast(@alignCast(raw)))).bound;
            }
        };
        var owner: Owner = .{ .bound = bound };
        var call = try @import("compiler.zig").compileScalar(a, if (entry.args.len == 2) "f('READY',NULL)" else "f('READY')", .{});
        defer call.deinit();
        const invocation = try @import("parameter_binding.zig").Invocation.initLeaky(a, 0);
        invocation.routine_resolver = .{ .ptr = &owner, .resolve = Owner.resolve };
        const output = try scalar.inferOutputWithInvocation(a, call.expression, &.{}, &.{}, invocation);
        try std.testing.expectEqual(ast.ColumnType.string, output.kind.?);
        var program = try scalar.bind(a, call.expression, &.{}, &.{}, .{ .invocation = invocation });
        defer program.deinit();
        var selection = try @import("compiler.zig").compile(a, if (entry.args.len == 2) "SELECT f('READY',NULL) AS value" else "SELECT f('READY') AS value", .{});
        defer selection.deinit();
        _ = try @import("bound_scalars.zig").bindWithInvocation(a, null, selection.statement, &.{}, null, &.{}, invocation);
        const value = try program.evaluate(a, &.{}, &.{}, .{});
        try std.testing.expectEqualStrings(entry.expected, value.value.string);
    }
}

test "SQL routine numeric row execution requires no allocator or catalog work after binding" {
    var region: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer region.deinit();
    const a = region.allocator();
    var parsed = try @import("compiler.zig").compile(a, "CREATE FUNCTION successor(integer) RETURNS integer LANGUAGE sql IMMUTABLE AS $$SELECT $1+1$$", .{});
    defer parsed.deinit();
    const declared = try definition(a, parsed.statement.catalog_ddl.routine.?, .auth_disabled);
    const bound = try prepare(a, .{ .id = 7, .generation = 1, .definition = declared }, null, null);
    const Owner = struct {
        bound: *const bindings.Resolved,
        lookups: usize = 0,
        fn resolve(raw: *anyopaque, _: A, _: []const u8, _: []const bindings.Argument) !*const bindings.Resolved {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.lookups += 1;
            return self.bound;
        }
    };
    var owner: Owner = .{ .bound = bound };
    var call = try @import("compiler.zig").compileScalar(a, "successor(n)", .{});
    defer call.deinit();
    var program = try scalar.bind(a, call.expression, &.{.{ .name = "n", .type = .integer, .element_type = .int32 }}, &.{}, .{ .routines = .{ .ptr = &owner, .resolve = Owner.resolve } });
    defer program.deinit();
    const prepared_lookups = owner.lookups;
    var none: std.heap.FixedBufferAllocator = .init(&.{});
    for (0..10000) |row| {
        const result = try program.evaluate(none.allocator(), &.{scalar.Datum.json(.{ .integer = @intCast(row) })}, &.{}, .{});
        try std.testing.expectEqual(@as(i64, @intCast(row + 1)), result.value.integer);
    }
    try std.testing.expectEqual(prepared_lookups, owner.lookups);
}

/// Native publication validates syntax before writing catalog state. Procedural
/// SQL expression binding is deliberately deferred to its statement-owned CALL
/// plan, matching PostgreSQL's first-execution PL/pgSQL resolution boundary.
pub fn aggregateInitial(a: A, definition_: records.Definition) !scalar.Datum {
    const aggregate_definition = definition_.aggregate orelse return error.InvalidSqlRoutineRecord;
    const initial = aggregate_definition.initial orelse return .{};
    const target = runtimeType(aggregate_definition.state);
    const literal: ast.Scalar = .{ .literal = .{ .string = initial } };
    const cast: ast.Scalar = .{ .cast = .{ .operand = &literal, .type = target.kind.?, .element_type = target.element_type, .temporal_kind = target.temporal_kind } };
    const program = try a.create(scalar.Program);
    program.* = try scalar.bind(a, &cast, &.{}, &.{}, .{});
    return program.evaluate(a, &.{}, &.{}, .{});
}

pub fn prepareAggregate(a: A, record: records.Record, resolver: bindings.Resolver) !*const bindings.Aggregate {
    try record.validate();
    if (record.kind != .aggregate) return error.SqlWrongObjectType;
    const lookup = resolver.resolve_reference orelse return error.UnsupportedSqlShape;
    const uses = try record.functionUsesAlloc(a);
    const inputs = try a.alloc(bindings.Argument, uses[0].arguments.len);
    for (uses[0].arguments, inputs) |kind, *input| input.* = .{ .type = runtimeType(kind) };
    const transition = try lookup(resolver.ptr, a, uses[0].reference, inputs);
    const aggregate_definition = record.definition.aggregate.?;
    const initial = try aggregateInitial(a, record.definition);
    if (transition.strict and initial.sql_null and !std.meta.eql(aggregate_definition.state, record.definition.arguments[0].type)) return error.SqlInvalidFunctionDefinition;
    var final: ?*const bindings.Resolved = null;
    if (uses.len == 2) final = try lookup(resolver.ptr, a, uses[1].reference, &.{.{ .type = runtimeType(aggregate_definition.state) }});
    const arguments = try a.alloc(scalar.Type, record.definition.arguments.len);
    for (record.definition.arguments, arguments) |argument, *kind| kind.* = runtimeType(argument.type);
    const result = try a.create(bindings.Aggregate);
    result.* = .{ .id = record.id, .generation = record.generation, .arguments = arguments, .state = runtimeType(aggregate_definition.state), .output = runtimeType(record.definition.result.?), .transition = transition, .final = final, .initial = initial };
    return result;
}

pub fn validateBody(a: A, record: records.Record) !void {
    try record.validate();
    switch (record.kind) {
        .function => _ = try prepare(a, record, null, null),
        .procedure => _ = try @import("procedure.zig").compileBody(a, record),
        .operator => {},
        .aggregate => _ = try aggregateInitial(a, record.definition),
    }
}

test "SQL native operator row execution performs no allocator or catalog work after binding" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const arguments: []const records.Argument = &.{ .{ .type = .{ .scalar = .int32 } }, .{ .type = .{ .scalar = .int32 } } };
    const function = try prepare(a, .{ .id = 7, .generation = 1, .definition = .{ .arguments = arguments, .result = .{ .scalar = .boolean }, .body = "SELECT $1<>$2", .owner = .auth_disabled } }, null, null);
    const Fixture = struct {
        function: *const bindings.Resolved,
        alias: *const bindings.Resolved = undefined,
        references: usize = 0,
        operators: usize = 0,
        fn reference(raw: *anyopaque, _: A, ref: records.FunctionReference, _: []const bindings.Argument) !*const bindings.Resolved {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (ref.id != self.function.id) return error.CatalogGenerationChanged;
            self.references += 1;
            return self.function;
        }
        fn operator(raw: *anyopaque, _: A, _: []const u8, _: []const bindings.Argument) !*const bindings.Resolved {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.operators += 1;
            return self.alias;
        }
    };
    var fixture: Fixture = .{ .function = function };
    fixture.alias = try prepare(a, .{ .kind = .operator, .id = 8, .generation = 1, .definition = .{ .arguments = arguments, .result = .{ .scalar = .boolean }, .body = "", .owner = .auth_disabled, .function_reference = .{ .database = "default", .namespace = "public", .name = "different", .namespace_id = 2, .id = 7 } } }, .{ .ptr = &fixture, .resolve = undefined, .resolve_reference = Fixture.reference }, null);
    var parsed = try @import("compiler.zig").compileScalar(a, "left_value===right_value", .{});
    defer parsed.deinit();
    var program = try scalar.bind(a, parsed.expression, &.{ .{ .name = "left_value", .type = .integer, .element_type = .int32 }, .{ .name = "right_value", .type = .integer, .element_type = .int32 } }, &.{}, .{ .routines = .{ .ptr = &fixture, .resolve = undefined, .resolve_operator = Fixture.operator } });
    defer program.deinit();
    const lookups = fixture.operators;
    var none: std.heap.FixedBufferAllocator = .init(&.{});
    for (0..10000) |ordinal| {
        const value: i64 = @intCast(ordinal);
        const result = try program.evaluate(none.allocator(), &.{ scalar.Datum.json(.{ .integer = value }), scalar.Datum.json(.{ .integer = value + 1 }) }, &.{}, .{});
        try std.testing.expectEqual(true, result.value.bool);
    }
    try std.testing.expectEqual(lookups, fixture.operators);
    try std.testing.expectEqual(@as(usize, 1), fixture.references);
}
