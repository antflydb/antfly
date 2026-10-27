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

//! Bounded procedural bytecode. SQL expressions use the ordinary typed scalar
//! compiler and VM; declarations never install interpreted execution callbacks.
const std = @import("std");
const compiler = @import("compiler.zig");
const scalar = @import("scalar.zig");
const ast = @import("ast.zig");
const routines = @import("routine_definition.zig");
const parser = @import("sql_parser");
const A = std.mem.Allocator;
pub const max_steps = 256;
pub const Notice = @import("notices.zig").Notice;
const SyntaxStep = union(enum) { perform: compiler.CompiledScalar, notice: []const u8, return_void };
pub const Syntax = struct {
    allocator: A,
    steps: []const SyntaxStep,
    pub fn deinit(self: *Syntax) void {
        for (self.steps) |step| switch (step) {
            .perform => |value| {
                var owned = value;
                owned.deinit();
            },
            .notice => |text| self.allocator.free(text),
            .return_void => {},
        };
        self.allocator.free(self.steps);
    }
};
fn word(value: anytype, expected: []const u8) bool {
    return value.kind == .identifier and !value.owned and std.ascii.eqlIgnoreCase(value.text, expected);
}

/// PL/pgSQL validates procedural syntax at CREATE, but resolves PERFORM's SQL
/// names at first execution. Source spans come from the shared lexer; quoted
/// semicolons, dollar strings and comments are never statement boundaries.
pub fn compileBody(a: A, record: routines.records.Record) !Syntax {
    try record.validate();
    if (record.kind != .procedure) return error.SqlWrongObjectType;
    if (record.definition.language != .plpgsql) return error.UnsupportedSqlShape;
    var scratch: std.heap.ArenaAllocator = .init(a);
    defer scratch.deinit();
    const lexed = try parser.lexer.tokenizeBoundedDiagnosticAlloc(scratch.allocator(), record.definition.body, 16384);
    const tokens = switch (lexed) {
        .tokens => |value| value.items,
        .diagnostic => return error.InvalidSqlSyntax,
    };
    if (tokens.len < 2 or !word(tokens[0], "BEGIN")) return error.InvalidSqlSyntax;
    var steps: std.ArrayList(SyntaxStep) = .empty;
    errdefer {
        const owned: Syntax = .{ .allocator = a, .steps = steps.items };
        for (owned.steps) |step| switch (step) {
            .perform => |value| {
                var parsed = value;
                parsed.deinit();
            },
            .notice => |text| a.free(text),
            .return_void => {},
        };
        steps.deinit(a);
    }
    var pos: usize = 1;
    while (pos < tokens.len and !word(tokens[pos], "END")) {
        if (steps.items.len >= max_steps) return error.SqlProgramLimitExceeded;
        if (word(tokens[pos], "NULL")) {
            pos += 1;
        } else if (word(tokens[pos], "RETURN")) {
            pos += 1;
            try steps.append(a, .return_void);
        } else if (word(tokens[pos], "RAISE")) {
            pos += 1;
            if (pos >= tokens.len or !word(tokens[pos], "NOTICE")) return error.UnsupportedSqlShape;
            pos += 1;
            if (pos >= tokens.len or tokens[pos].kind != .string) return error.InvalidSqlSyntax;
            // Format parameters must be retained as typed expressions before
            // admission; do not accept '%' while silently losing its inputs.
            if (std.mem.indexOfScalar(u8, tokens[pos].text, '%') != null) return error.UnsupportedSqlShape;
            const text = try a.dupe(u8, tokens[pos].text);
            errdefer a.free(text);
            try steps.append(a, .{ .notice = text });
            pos += 1;
        } else if (word(tokens[pos], "PERFORM")) {
            pos += 1;
            const start = pos;
            var depth: usize = 0;
            while (pos < tokens.len) : (pos += 1) {
                if (tokens[pos].kind == .semicolon and depth == 0) break;
                if (tokens[pos].kind == .lparen) depth += 1;
                if (tokens[pos].kind == .rparen) {
                    if (depth == 0) return error.InvalidSqlSyntax;
                    depth -= 1;
                }
            }
            if (pos == start or depth != 0) return error.InvalidSqlSyntax;
            var expression = try compiler.compileScalar(a, record.definition.body[tokens[start].source_start..tokens[pos - 1].source_end], .{ .max_bytes = routines.records.max_body_bytes, .max_parameters = @intCast(record.definition.arguments.len) });
            errdefer expression.deinit();
            try steps.append(a, .{ .perform = expression });
        } else return error.UnsupportedSqlShape;
        if (pos >= tokens.len or tokens[pos].kind != .semicolon) return error.InvalidSqlSyntax;
        pos += 1;
    }
    if (pos >= tokens.len or !word(tokens[pos], "END")) return error.InvalidSqlSyntax;
    pos += 1;
    if (pos < tokens.len and tokens[pos].kind == .semicolon) pos += 1;
    if (pos != tokens.len) return error.InvalidSqlSyntax;
    return .{ .allocator = a, .steps = try steps.toOwnedSlice(a) };
}

pub const Step = union(enum) { perform: struct { syntax: usize, program: ?scalar.Program = null }, notice: []const u8, return_void };
pub const Resolved = struct {
    allocator: A,
    id: u64,
    generation: u32,
    arguments: []const scalar.Type,
    columns: []const scalar.Column,
    syntax: Syntax,
    steps: []Step,
    invocation: *@import("parameter_binding.zig").Invocation,
    settings: ?*const @import("setting_catalog.zig").View,
};
pub const Resolver = struct { ptr: *anyopaque, resolve: *const fn (*anyopaque, A, []const u8, []const routines.bindings.Argument) anyerror!*Resolved };

/// Statement-owned syntax and declared domains. SQL names/types are bound only
/// when their PERFORM instruction is demanded, so RETURN cannot execute or
/// validate unreachable SQL. Each active instruction is bound at most once.
pub fn prepare(a: A, record: routines.records.Record, functions: ?routines.bindings.Resolver, sequences: ?@import("sequence_effects.zig").Resolver, settings: ?*const @import("setting_catalog.zig").View) !*Resolved {
    var syntax = try compileBody(a, record);
    errdefer syntax.deinit();
    const types = try a.alloc(scalar.Type, record.definition.arguments.len);
    const columns = try a.alloc(scalar.Column, types.len);
    for (record.definition.arguments, types, columns, 0..) |argument, *kind, *column, ordinal| {
        kind.* = routines.runtimeType(argument.type);
        column.* = .{ .name = argument.name orelse try std.fmt.allocPrint(a, "\x00procedure_argument:{d}", .{ordinal}), .type = kind.kind.?, .element_type = kind.element_type, .temporal_kind = kind.temporal_kind };
    }
    const steps = try a.alloc(Step, syntax.steps.len);
    for (syntax.steps, steps, 0..) |source, *target, ordinal| target.* = switch (source) {
        .notice => |text| .{ .notice = text },
        .return_void => .return_void,
        .perform => .{ .perform = .{ .syntax = ordinal } },
    };
    const invocation = try @import("parameter_binding.zig").Invocation.initLeaky(a, types.len);
    try invocation.mergeInferred(types);
    invocation.routine_resolver = functions;
    invocation.sequence_resolver = sequences;
    const result = try a.create(Resolved);
    result.* = .{ .allocator = a, .id = record.id, .generation = record.generation, .arguments = types, .columns = columns, .syntax = syntax, .steps = steps, .invocation = invocation, .settings = settings };
    return result;
}

pub const Call = struct { procedure: *Resolved, arguments: []const scalar.Program };
fn encodedName(a: A, name: ast.Name) ![]const u8 {
    if (name.database) |database| return std.mem.concat(a, u8, &.{ database, "\x00", name.namespace orelse return error.InvalidSqlSyntax, "\x00", name.table });
    if (name.namespace) |namespace| return std.mem.concat(a, u8, &.{ namespace, "\x00", name.table });
    return name.table;
}
pub fn bind(a: A, backend: @import("catalog.zig").Backend, invocation: ?*@import("parameter_binding.zig").Invocation, statement: @FieldType(ast.Statement, "call"), parameters: []?ast.ColumnType) anyerror!Call {
    const resolver = backend.procedure_resolver orelse return error.UnsupportedSqlExecution;
    const inputs = try a.alloc(routines.bindings.Argument, statement.arguments.len);
    for (statement.arguments, inputs) |expression, *input| input.* = .{ .type = try scalar.inferOutputWithInvocation(a, expression, &.{}, parameters, invocation), .unknown_literal = expression.* == .literal and (expression.literal == .string or expression.literal == .null) };
    const bound = try resolver.resolve(resolver.ptr, a, try encodedName(a, statement.name), inputs);
    if (bound.id == 0 or bound.generation == 0 or bound.arguments.len != inputs.len) return error.InvalidSqlBackendResponse;
    const arguments = try a.alloc(scalar.Program, inputs.len);
    for (statement.arguments, bound.arguments, arguments) |expression, kind, *program| {
        const cast: ast.Scalar = .{ .cast = .{ .operand = expression, .type = kind.kind orelse return error.InvalidSqlProgram, .element_type = kind.element_type, .temporal_kind = kind.temporal_kind, .coercion = .function } };
        program.* = try scalar.bindExpectedWithSettings(a, &cast, &.{}, parameters, kind.kind, .{ .invocation = invocation }, backend.settings_view);
    }
    return .{ .procedure = bound, .arguments = arguments };
}

pub fn execute(context: @import("runtime.zig").Context, call: Call) anyerror!@import("runtime.zig").Output {
    const decision = @import("decision_eval.zig");
    const a = context.arena;
    const arguments = try a.alloc(scalar.Datum, call.arguments.len);
    var work: @import("numeric_value.zig").Context = .{ .alloc = context.alloc, .remaining = 65536, .checkpoint = if (context.backend.scalar_control) |control| control.checkpoint else null, .ptr = if (context.backend.scalar_control) |control| control.ptr else null };
    var bytes: usize = 0;
    var limits = decision.limitsFor(context.backend);
    limits.effect_work = &work;
    limits.effect_bytes = &bytes;
    for (call.arguments, arguments) |*program, *value| value.* = try decision.evaluateWithLimits(a, context.backend.decision_provider, program, &.{}, context.parameters, limits);
    var notices: std.ArrayList(Notice) = .empty;
    for (call.procedure.steps) |*step| {
        try context.checkpoint();
        switch (step.*) {
            .return_void => break,
            .notice => |text| {
                const notice: Notice = .{ .message = text };
                if (context.backend.notice_sink) |sink| try sink.emit(sink.ptr, notice);
                try notices.append(a, .{ .message = try a.dupe(u8, text) });
            },
            .perform => |*instruction| {
                const procedure = call.procedure;
                if (instruction.program == null) instruction.program = try scalar.bindTypedExpectedWithSettings(procedure.allocator, procedure.syntax.steps[instruction.syntax].perform.expression, procedure.columns, procedure.arguments, null, .{ .invocation = procedure.invocation }, procedure.settings);
                const prepared: scalar.PreparedEvaluation = .{ .program = &instruction.program.?, .parameters = arguments };
                _ = try decision.evaluatePreparedWithLimits(a, context.backend.decision_provider, prepared, arguments, limits);
            },
        }
    }
    return .{ .command_tag = "CALL", .notices = try notices.toOwnedSlice(a) };
}

test "SQL procedure syntax retains complete original blocks without executing or resolving PERFORM" {
    const a = std.testing.allocator;
    var corpus = try @import("parity_fixtures.zig").Corpus.init(a);
    defer corpus.deinit();
    for ([_][]const u8{ "sql-0414", "sql-0415", "sql-0416", "sql-0418" }) |id| {
        var parsed = try compiler.compile(a, (try corpus.get(id)).sql, .{});
        defer parsed.deinit();
        try std.testing.expectEqual(.procedure, parsed.statement.catalog_ddl.kind);
        var region: std.heap.ArenaAllocator = .init(a);
        defer region.deinit();
        const definition = try routines.definition(region.allocator(), parsed.statement.catalog_ddl.routine.?, .auth_disabled);
        var body = try compileBody(a, .{ .kind = .procedure, .id = 1, .generation = 1, .definition = definition });
        defer body.deinit();
        try std.testing.expect(body.steps.len <= 1);
    }
}

test "SQL procedure CALL shares typed parameter frames and preserves lazy RETURN and notices" {
    const catalog = @import("catalog.zig");
    const runtime = @import("runtime.zig");
    const Fixture = struct {
        plan: *Resolved,
        lookups: usize = 0,
        fn lookup(raw: *anyopaque, _: A, _: []const u8, _: []const routines.bindings.Argument) !*Resolved {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.lookups += 1;
            return self.plan;
        }
        fn checkpoint(_: *anyopaque) !void {}
        fn backend(self: *@This()) catalog.Backend {
            return .{ .ptr = self, .procedure_resolver = .{ .ptr = self, .resolve = lookup }, .vtable = &.{ .resolve = undefined, .scan = undefined, .mutate = undefined, .checkpoint = checkpoint } };
        }
    };
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const definition: routines.records.Definition = .{ .arguments = &.{.{ .name = "value", .type = .{ .scalar = .numeric } }}, .result = null, .language = .plpgsql, .body = "BEGIN RAISE NOTICE 'first; retained'; PERFORM value + $1; RETURN; PERFORM missing_function(1); RAISE NOTICE 'unreachable'; END", .owner = .auth_disabled };
    const plan = try prepare(a, .{ .kind = .procedure, .id = 1, .generation = 1, .definition = definition }, null, null, null);
    var fixture: Fixture = .{ .plan = plan };
    var parsed = try compiler.compile(a, "CALL p($1)", .{});
    defer parsed.deinit();
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), &parsed, &.{.{ .integer = 7 }}, .{});
    defer result.deinit();
    try std.testing.expectEqualStrings("CALL", result.output.command_tag);
    try std.testing.expectEqual(@as(usize, 0), result.output.columns.len);
    try std.testing.expectEqual(@as(usize, 1), result.output.notices.len);
    try std.testing.expectEqualStrings("first; retained", result.output.notices[0].message);
    try std.testing.expect(plan.steps[1].perform.program != null);
    try std.testing.expect(plan.steps[3].perform.program == null);
    try std.testing.expectEqual(@as(usize, 1), fixture.lookups);
}
