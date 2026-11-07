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
pub const TriggerReturn = enum { new, old, skip };
pub const TriggerImage = enum { new, old };
const Assignment = struct { image: TriggerImage, column: []const u8, expression: compiler.CompiledScalar };
const SyntaxStep = union(enum) { perform: compiler.CompiledScalar, notice: []const u8, assign: Assignment, return_void, return_trigger: TriggerReturn };
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
            .assign => |value| {
                var expression = value.expression;
                expression.deinit();
                self.allocator.free(value.column);
            },
            .return_void, .return_trigger => {},
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
    if (record.kind != .procedure and !(record.kind == .function and record.definition.returns_trigger)) return error.SqlWrongObjectType;
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
            .assign => |value| {
                var expression = value.expression;
                expression.deinit();
                a.free(value.column);
            },
            .return_void, .return_trigger => {},
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
            if (record.definition.returns_trigger) {
                if (pos >= tokens.len) return error.InvalidSqlSyntax;
                if (tokens[pos].kind == .semicolon) return error.SqlTriggerReturnValueRequired;
                const selection: TriggerReturn = if (word(tokens[pos], "NEW")) .new else if (word(tokens[pos], "OLD")) .old else if (word(tokens[pos], "NULL")) .skip else return error.UnsupportedSqlShape;
                pos += 1;
                try steps.append(a, .{ .return_trigger = selection });
            } else try steps.append(a, .return_void);
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
        } else if (record.definition.returns_trigger and (word(tokens[pos], "NEW") or word(tokens[pos], "OLD"))) {
            const image: TriggerImage = if (word(tokens[pos], "NEW")) .new else .old;
            pos += 1;
            if (pos + 3 >= tokens.len or tokens[pos].kind != .dot or tokens[pos + 1].kind != .identifier or tokens[pos + 2].kind != .colon or tokens[pos + 3].kind != .eq) return error.UnsupportedSqlShape;
            const column = if (tokens[pos + 1].owned) try a.dupe(u8, tokens[pos + 1].text) else try std.ascii.allocLowerString(a, tokens[pos + 1].text);
            errdefer a.free(column);
            pos += 4;
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
            var expression = try compiler.compileScalar(a, record.definition.body[tokens[start].source_start..tokens[pos - 1].source_end], .{ .max_bytes = routines.records.max_body_bytes, .max_parameters = 0 });
            errdefer expression.deinit();
            try steps.append(a, .{ .assign = .{ .image = image, .column = column, .expression = expression } });
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

pub const Step = union(enum) { perform: struct { syntax: usize, program: ?scalar.Program = null }, assign: struct { syntax: usize, program: ?scalar.Program = null }, notice: []const u8, return_void, return_trigger: TriggerReturn };
pub const Resolved = struct {
    allocator: A,
    id: u64,
    generation: u32,
    returns_trigger: bool = false,
    arguments: []const scalar.Type,
    columns: []const scalar.Column,
    syntax: Syntax,
    steps: []Step,
    invocation: *@import("parameter_binding.zig").Invocation,
    settings: ?*const @import("setting_catalog.zig").View,
    /// Scalar caches belong to one pinned statement/table environment.
    trigger_columns: ?[]const scalar.Column = null,
};
/// Resolve the immutable callable closure without evaluating scalar effects.
/// Binding failures stay demand-driven in the actual VM; metadata, authority,
/// resource and cancellation failures abort capture rather than publish an
/// incomplete executable authority. This never fills the receiver's caches.
pub fn captureTriggerFunctions(prepared: *Resolved, frame: TriggerFrame) !void {
    for (prepared.syntax.steps) |step| switch (step) {
        .return_void, .return_trigger => break,
        .notice => {},
        .perform => |instruction| {
            _ = scalar.bindTypedExpectedWithSettings(prepared.allocator, instruction.expression, frame.columns, prepared.arguments, null, .{ .invocation = prepared.invocation }, prepared.settings) catch |err| {
                try deferCaptureBindingError(err);
                continue;
            };
        },
        .assign => |instruction| {
            const target = frame.target(instruction.image, instruction.column) catch |err| {
                try deferCaptureBindingError(err);
                continue;
            };
            const column = frame.columns[target];
            const cast: ast.Scalar = .{ .cast = .{ .operand = instruction.expression.expression, .type = column.type, .element_type = column.element_type, .temporal_kind = column.temporal_kind, .enum_view = column.enum_view, .domain_view = column.domain_view, .numeric_modifier = column.numeric_modifier, .context = .assignment } };
            _ = scalar.bindTypedExpectedWithSettings(prepared.allocator, &cast, frame.columns, prepared.arguments, scalar.Type{ .kind = column.type, .element_type = column.element_type, .temporal_kind = column.temporal_kind, .numeric_modifier = column.numeric_modifier, .enum_view = column.enum_view, .domain_binding = scalar.domainBindingOf(column) }, .{ .invocation = prepared.invocation }, prepared.settings) catch |err| {
                try deferCaptureBindingError(err);
                continue;
            };
        },
    };
}
fn deferCaptureBindingError(err: anyerror) !void {
    const diagnostic = @import("errors.zig").describe(err);
    // A denied metadata lookup cannot be forged into a missing declaration.
    if (std.mem.eql(u8, diagnostic.code, "42501")) return err;
    if (std.mem.startsWith(u8, diagnostic.code, "42") or std.mem.startsWith(u8, diagnostic.code, "22") or std.mem.eql(u8, diagnostic.code, "0A000")) return;
    return err;
}

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
        .return_trigger => |selection| .{ .return_trigger = selection },
        .perform => .{ .perform = .{ .syntax = ordinal } },
        .assign => .{ .assign = .{ .syntax = ordinal } },
    };
    const invocation = try @import("parameter_binding.zig").Invocation.initLeaky(a, types.len);
    try invocation.mergeInferred(types);
    invocation.routine_resolver = functions;
    invocation.sequence_resolver = sequences;
    const result = try a.create(Resolved);
    result.* = .{ .allocator = a, .id = record.id, .generation = record.generation, .returns_trigger = record.definition.returns_trigger, .arguments = types, .columns = columns, .syntax = syntax, .steps = steps, .invocation = invocation, .settings = settings };
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
    if (call.procedure.returns_trigger) return error.SqlTriggerContextRequired;
    const decision = @import("decision_eval.zig");
    const a = context.arena;
    const arguments = try a.alloc(scalar.Datum, call.arguments.len);
    var work: @import("numeric_value.zig").Context = .{ .alloc = context.alloc, .remaining = 65536, .checkpoint = if (context.backend.scalar_control) |control| control.checkpoint else null, .ptr = if (context.backend.scalar_control) |control| control.ptr else null };
    var bytes: usize = 0;
    var limits = decision.limitsFor(context.backend);
    limits.effect_work = &work;
    limits.effect_bytes = &bytes;
    for (call.arguments, arguments) |*program, *value| value.* = try decision.evaluateWithLimits(a, context.backend.decision_provider, program, &.{}, context.parameters, limits);
    const result = try run(context, call.procedure, arguments, limits, null);
    return .{ .command_tag = "CALL", .notices = result.notices };
}

/// Mutable invocation cells are owned by the mutation frame, not a shared
/// procedure plan. The owner supplies copy-on-write row assignment, preserving
/// its original MVCC image and typed storage/domain conversion boundary.
pub const TriggerFrame = struct {
    columns: []const scalar.Column,
    values: []scalar.Datum,
    width: usize,
    new_offset: usize,
    old_offset: usize,
    new: ?*const @import("catalog.zig").Row,
    old: ?*const @import("catalog.zig").Row,
    ptr: *anyopaque,
    assign: *const fn (*anyopaque, A, TriggerImage, usize, scalar.Datum) anyerror!*const @import("catalog.zig").Row,

    fn validate(self: TriggerFrame) !void {
        if (self.columns.len != self.values.len or self.new_offset > self.values.len or self.old_offset > self.values.len or self.width > self.values.len - self.new_offset or self.width > self.values.len - self.old_offset or
            (self.width != 0 and self.new_offset < self.old_offset + self.width and self.old_offset < self.new_offset + self.width)) return error.InvalidSqlBackendResponse;
        // Trigger records keep the table's tuple descriptor when their image
        // is NULL (statement level, OLD on INSERT, NEW on DELETE). Their fields
        // are nullable input cells and local assignments materialize a record.
        for ([_]usize{ self.new_offset, self.old_offset }, [_]?*const @import("catalog.zig").Row{ self.new, self.old }) |offset, image| {
            for (self.columns[offset..][0..self.width], self.values[offset..][0..self.width]) |column, value| {
                if (!column.nullable or (image == null and !value.sql_null)) return error.InvalidSqlBackendResponse;
            }
        }
    }
    fn target(self: TriggerFrame, image: TriggerImage, name: []const u8) !usize {
        const offset = if (image == .new) self.new_offset else self.old_offset;
        for (self.columns[offset..][0..self.width], 0..) |column, i| {
            const separator = std.mem.lastIndexOfScalar(u8, column.name, 0) orelse return error.InvalidSqlBackendResponse;
            if (std.mem.eql(u8, column.name[separator + 1 ..], name)) return offset + i;
        }
        return error.UnknownColumn;
    }
};

/// Borrowed row images, pinned by the mutation owner for this invocation. The
/// function never clones an unchanged row or transfers ownership of its image.
pub const TriggerContext = struct {
    event: enum { insert, update, delete, truncate },
    level: enum { row, statement } = .row,
    new: ?*const @import("catalog.zig").Row = null,
    old: ?*const @import("catalog.zig").Row = null,
    frame: ?*TriggerFrame = null,

    fn validate(self: TriggerContext) !void {
        if (self.level == .statement) {
            if (self.new != null or self.old != null) return error.InvalidSqlBackendResponse;
            return;
        }
        const valid = switch (self.event) {
            .insert => self.new != null and self.old == null,
            .update => self.new != null and self.old != null,
            .delete => self.new == null and self.old != null,
            .truncate => false,
        };
        if (!valid) return error.InvalidSqlBackendResponse;
    }
};
pub const TriggerOutput = struct {
    row: ?*const @import("catalog.zig").Row,
    notices: []const Notice,
};

/// Explicit trigger-only ABI. Table trigger attachment and the mutation owner
/// decide when to invoke this program and whether to use its returned image.
pub fn executeTrigger(context: @import("runtime.zig").Context, prepared: *Resolved, images: TriggerContext) anyerror!TriggerOutput {
    if (!prepared.returns_trigger) return error.SqlWrongObjectType;
    try images.validate();
    const decision = @import("decision_eval.zig");
    var work: @import("numeric_value.zig").Context = .{ .alloc = context.alloc, .remaining = 65536, .checkpoint = if (context.backend.scalar_control) |control| control.checkpoint else null, .ptr = if (context.backend.scalar_control) |control| control.ptr else null };
    var bytes: usize = 0;
    var limits = decision.limitsFor(context.backend);
    limits.effect_work = &work;
    limits.effect_bytes = &bytes;
    if (images.frame) |frame| {
        try frame.validate();
        if (frame.new != images.new or frame.old != images.old) return error.InvalidSqlBackendResponse;
        if (prepared.trigger_columns) |columns| {
            if (columns.ptr != frame.columns.ptr or columns.len != frame.columns.len) return error.CatalogGenerationChanged;
        } else prepared.trigger_columns = frame.columns;
    }
    const result = try run(context, prepared, &.{}, limits, images.frame);
    const selection = result.selection orelse return error.SqlTriggerReturnedNoValue;
    return .{ .row = switch (selection) {
        .new => if (images.frame) |frame| frame.new else images.new,
        .old => if (images.frame) |frame| frame.old else images.old,
        .skip => null,
    }, .notices = result.notices };
}

const RunOutput = struct { selection: ?TriggerReturn = null, notices: []const Notice };
fn run(context: @import("runtime.zig").Context, prepared: *Resolved, arguments: []const scalar.Datum, limits: scalar.EvalLimits, frame: ?*TriggerFrame) !RunOutput {
    const a = context.arena;
    var notices: std.ArrayList(Notice) = .empty;
    var selection: ?TriggerReturn = null;
    for (prepared.steps) |*step| {
        try context.checkpoint();
        switch (step.*) {
            .return_void => break,
            .return_trigger => |value| {
                selection = value;
                break;
            },
            .notice => |text| {
                const notice: Notice = .{ .message = text };
                if (context.backend.notice_sink) |sink| try sink.emit(sink.ptr, notice);
                try notices.append(a, .{ .message = try a.dupe(u8, text) });
            },
            .perform => |*instruction| {
                if (instruction.program == null) instruction.program = try scalar.bindTypedExpectedWithSettings(prepared.allocator, prepared.syntax.steps[instruction.syntax].perform.expression, if (frame) |value| value.columns else prepared.columns, prepared.arguments, null, .{ .invocation = prepared.invocation }, prepared.settings);
                const evaluation: scalar.PreparedEvaluation = .{ .program = &instruction.program.?, .parameters = arguments };
                _ = try @import("decision_eval.zig").evaluatePreparedWithLimits(a, context.backend.decision_provider, evaluation, if (frame) |value| value.values else arguments, limits);
            },
            .assign => |*instruction| {
                const owner = frame orelse return error.InvalidSqlBackendResponse;
                const syntax = prepared.syntax.steps[instruction.syntax].assign;
                const target = try owner.target(syntax.image, syntax.column);
                if (instruction.program == null) {
                    const column = owner.columns[target];
                    const cast: ast.Scalar = .{ .cast = .{ .operand = syntax.expression.expression, .type = column.type, .element_type = column.element_type, .temporal_kind = column.temporal_kind, .enum_view = column.enum_view, .domain_view = column.domain_view, .numeric_modifier = column.numeric_modifier, .context = .assignment } };
                    instruction.program = try scalar.bindTypedExpectedWithSettings(prepared.allocator, &cast, owner.columns, prepared.arguments, scalar.Type{ .kind = column.type, .element_type = column.element_type, .temporal_kind = column.temporal_kind, .numeric_modifier = column.numeric_modifier, .enum_view = column.enum_view, .domain_binding = scalar.domainBindingOf(column) }, .{ .invocation = prepared.invocation }, prepared.settings);
                }
                const evaluation: scalar.PreparedEvaluation = .{ .program = &instruction.program.?, .parameters = arguments };
                const value = try @import("decision_eval.zig").evaluatePreparedWithLimits(a, context.backend.decision_provider, evaluation, owner.values, limits);
                const offset = if (syntax.image == .new) owner.new_offset else owner.old_offset;
                const row = try owner.assign(owner.ptr, a, syntax.image, target - offset, value);
                owner.values[target] = value;
                if (syntax.image == .new) owner.new = row else owner.old = row;
            },
        }
    }
    return .{ .selection = selection, .notices = try notices.toOwnedSlice(a) };
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

test "SQL trigger functions preserve original row returns lazy PERFORM and context boundaries" {
    const catalog = @import("catalog.zig");
    const runtime = @import("runtime.zig");
    const Fixture = struct {
        available: bool = false,
        lookups: usize = 0,
        cancelled: bool = false,
        fn lookup(raw: *anyopaque, a: A, name: []const u8, inputs: []const routines.bindings.Argument) !*const routines.bindings.Resolved {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.lookups += 1;
            if (!self.available or !std.mem.eql(u8, name, "audit_log")) return error.SqlUndefinedFunction;
            const args = try a.alloc(routines.records.Argument, inputs.len);
            for (args) |*arg| arg.* = .{ .type = .{ .scalar = .int64 } };
            return routines.prepare(a, .{ .id = 91, .generation = 1, .definition = .{ .arguments = args, .result = .{ .scalar = .int64 }, .body = if (inputs.len == 0) "SELECT 1" else "SELECT $1", .owner = .auth_disabled } }, null, null);
        }
        fn checkpoint(raw: *anyopaque) !void {
            if ((@as(*@This(), @ptrCast(@alignCast(raw)))).cancelled) return error.Cancelled;
        }
        fn backend(self: *@This()) catalog.Backend {
            return .{ .ptr = self, .vtable = &.{ .resolve = undefined, .scan = undefined, .mutate = undefined, .checkpoint = checkpoint } };
        }
    };
    var corpus = try @import("parity_fixtures.zig").Corpus.init(std.testing.allocator);
    defer corpus.deinit();
    const old: catalog.Row = .{ .id = "row", .version = 1, .value = .{ .string = "old" } };
    const new: catalog.Row = .{ .id = "row", .version = 2, .value = .{ .string = "new" } };
    for ([_][]const u8{ "sql-0405", "sql-0406", "sql-0407", "sql-0408", "sql-0409", "sql-0410" }, 0..) |id, index| {
        var region: std.heap.ArenaAllocator = .init(std.testing.allocator);
        defer region.deinit();
        const a = region.allocator();
        var compiled = try compiler.compile(a, (try corpus.get(id)).sql, .{});
        defer compiled.deinit();
        const definition = try routines.definition(a, compiled.statement.catalog_ddl.routine.?, .auth_disabled);
        const record: routines.records.Record = .{ .id = 8, .generation = 1, .definition = definition };
        var fixture: Fixture = .{};
        const prepared = try prepare(a, record, .{ .ptr = &fixture, .resolve = Fixture.lookup }, null, null);
        try std.testing.expectEqual(@as(usize, 0), fixture.lookups);
        const context: runtime.Context = .{ .alloc = a, .arena = a, .backend = fixture.backend(), .binding = undefined, .parameters = &.{}, .limits = .{} };
        try std.testing.expectError(error.SqlTriggerContextRequired, routines.prepare(a, record, null, null));
        try std.testing.expectError(error.SqlTriggerContextRequired, execute(context, .{ .procedure = prepared, .arguments = &.{} }));
        try std.testing.expectError(error.InvalidSqlBackendResponse, executeTrigger(context, prepared, .{ .event = .insert, .old = &old, .new = &new }));
        try std.testing.expectEqual(@as(usize, 0), fixture.lookups);
        if (index >= 4) {
            try std.testing.expectError(error.SqlUndefinedFunction, executeTrigger(context, prepared, .{ .event = .update, .old = &old, .new = &new }));
            fixture.available = true;
        }
        const result = try executeTrigger(context, prepared, .{ .event = .update, .old = &old, .new = &new });
        try std.testing.expectEqual(if (index == 1) &old else if (index == 2) @as(?*const catalog.Row, null) else &new, result.row);
        try std.testing.expectEqual(@as(usize, if (index == 3) 1 else 0), result.notices.len);
        if (index == 3) try std.testing.expectEqualStrings("audit", result.notices[0].message);
        const bound_lookups = fixture.lookups;
        _ = try executeTrigger(context, prepared, .{ .event = .update, .old = &old, .new = &new });
        try std.testing.expectEqual(bound_lookups, fixture.lookups);
        fixture.cancelled = true;
        try std.testing.expectError(error.Cancelled, executeTrigger(context, prepared, .{ .event = .update, .old = &old, .new = &new }));
    }
}

test "SQL trigger row-return fast path allocates no row memory and preserves event nulls" {
    const catalog = @import("catalog.zig");
    const runtime = @import("runtime.zig");
    const Fixture = struct {
        fn checkpoint(_: *anyopaque) !void {}
    };
    var region: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer region.deinit();
    const a = region.allocator();
    var sentinel: u8 = 0;
    const backend: catalog.Backend = .{ .ptr = &sentinel, .vtable = &.{ .resolve = undefined, .scan = undefined, .mutate = undefined, .checkpoint = Fixture.checkpoint } };
    var empty: std.heap.FixedBufferAllocator = .init(&.{});
    const context: runtime.Context = .{ .alloc = empty.allocator(), .arena = empty.allocator(), .backend = backend, .binding = undefined, .parameters = &.{}, .limits = .{} };
    const row: catalog.Row = .{ .id = "row", .version = 1, .value = .null };
    const plan = try prepare(a, .{ .id = 1, .generation = 1, .definition = .{ .returns_trigger = true, .language = .plpgsql, .arguments = &.{}, .body = "BEGIN RETURN NEW; PERFORM nonexistent(); END", .owner = .auth_disabled } }, null, null, null);
    for (0..10000) |_| try std.testing.expectEqual(&row, (try executeTrigger(context, plan, .{ .event = .insert, .new = &row })).row.?);
    try std.testing.expect(plan.steps[1].perform.program == null);
    try std.testing.expect((try executeTrigger(context, plan, .{ .event = .delete, .old = &row })).row == null);
    try std.testing.expect((try executeTrigger(context, plan, .{ .event = .update, .level = .statement })).row == null);
    const fallthrough = try prepare(a, .{ .id = 2, .generation = 1, .definition = .{ .returns_trigger = true, .language = .plpgsql, .arguments = &.{}, .body = "BEGIN NULL; END", .owner = .auth_disabled } }, null, null, null);
    try std.testing.expectError(error.SqlTriggerReturnedNoValue, executeTrigger(context, fallthrough, .{ .event = .insert, .new = &row }));
}

test "SQL trigger assignments share typed scalar programs and preserve original images and statement nulls" {
    const catalog = @import("catalog.zig");
    const runtime = @import("runtime.zig");
    const Fixture = struct {
        modified: catalog.Row = .{ .id = "one", .version = 1, .value = .null },
        writes: usize = 0,
        fn assign(raw: *anyopaque, _: A, image: TriggerImage, ordinal: usize, value: scalar.Datum) !*const catalog.Row {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(TriggerImage.new, image);
            try std.testing.expectEqual(@as(usize, 0), ordinal);
            self.writes += 1;
            self.modified.value = value.value;
            return &self.modified;
        }
        fn checkpoint(_: *anyopaque) !void {}
    };
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var fixture: Fixture = .{};
    const backend: catalog.Backend = .{ .ptr = &fixture, .vtable = &.{ .resolve = undefined, .scan = undefined, .mutate = undefined, .checkpoint = Fixture.checkpoint } };
    const context: runtime.Context = .{ .alloc = a, .arena = a, .backend = backend, .binding = undefined, .parameters = &.{}, .limits = .{} };
    const original: catalog.Row = .{ .id = "one", .version = 1, .value = .{ .integer = 10 } };
    const old: catalog.Row = .{ .id = "one", .version = 1, .value = .{ .integer = 1 } };
    const plan = try prepare(a, .{ .id = 1, .generation = 1, .definition = .{ .returns_trigger = true, .language = .plpgsql, .arguments = &.{}, .body = "BEGIN NEW.n := OLD.n + 2; NEW.n := NEW.n + 3; RETURN NEW; END", .owner = .auth_disabled } }, null, null, null);
    const columns = [_]scalar.Column{ .{ .name = "new\x00n", .type = .integer, .element_type = .int64 }, .{ .name = "old\x00n", .type = .integer, .element_type = .int64 } };
    var values = [_]scalar.Datum{ scalar.Datum.json(.{ .integer = 10 }), scalar.Datum.json(.{ .integer = 1 }) };
    var frame: TriggerFrame = .{ .columns = &columns, .values = &values, .width = 1, .new_offset = 0, .old_offset = 1, .new = &original, .old = &old, .ptr = &fixture, .assign = Fixture.assign };
    const output = try executeTrigger(context, plan, .{ .event = .update, .new = &original, .old = &old, .frame = &frame });
    try std.testing.expectEqual(&fixture.modified, output.row.?);
    try std.testing.expectEqual(@as(i64, 6), output.row.?.value.integer);
    try std.testing.expectEqual(@as(i64, 10), original.value.integer);
    try std.testing.expectEqual(@as(i64, 1), old.value.integer);
    try std.testing.expectEqual(@as(usize, 2), fixture.writes);
    try std.testing.expect(plan.steps[0].assign.program != null);
    try std.testing.expect(plan.steps[1].assign.program != null);
    var incompatible_columns = columns;
    incompatible_columns[0].element_type = .int32;
    var incompatible = frame;
    incompatible.columns = &incompatible_columns;
    try std.testing.expectError(error.CatalogGenerationChanged, executeTrigger(context, plan, .{ .event = .update, .new = frame.new, .old = frame.old, .frame = &incompatible }));
    try std.testing.expectEqual(@as(usize, 2), fixture.writes);
    frame.new = null;
    frame.old = null;
    @memset(&values, scalar.Datum{});
    const statement = try executeTrigger(context, plan, .{ .event = .update, .level = .statement, .frame = &frame });
    try std.testing.expect(statement.row != null);
    try std.testing.expect(statement.row.?.value == .null);
    try std.testing.expectEqual(@as(usize, 4), fixture.writes);
    frame.new = &original;
    frame.old = null;
    values[0] = scalar.Datum.json(.{ .integer = 10 });
    values[1] = .{};
    const inserted = try executeTrigger(context, plan, .{ .event = .insert, .new = &original, .frame = &frame });
    try std.testing.expect(inserted.row.?.value == .null);
    try std.testing.expectEqual(@as(usize, 6), fixture.writes);
    // Even a NULL trigger record has a known tuple descriptor. An undeclared
    // field is 42703, whereas an existing field simply evaluates SQL NULL.
    const null_read = try prepare(a, .{ .id = 2, .generation = 1, .definition = .{ .returns_trigger = true, .language = .plpgsql, .arguments = &.{}, .body = "BEGIN PERFORM OLD.n; RETURN NEW; END", .owner = .auth_disabled } }, null, null, null);
    frame.new = &original;
    try std.testing.expectEqual(&original, (try executeTrigger(context, null_read, .{ .event = .insert, .new = &original, .frame = &frame })).row.?);
    const absent = try prepare(a, .{ .id = 3, .generation = 1, .definition = .{ .returns_trigger = true, .language = .plpgsql, .arguments = &.{}, .body = "BEGIN PERFORM CASE WHEN false THEN OLD.unknown ELSE 0 END; RETURN NEW; END", .owner = .auth_disabled } }, null, null, null);
    try std.testing.expectError(error.UnknownColumn, executeTrigger(context, absent, .{ .event = .insert, .new = &original, .frame = &frame }));
    const division = try prepare(a, .{ .id = 4, .generation = 1, .definition = .{ .returns_trigger = true, .language = .plpgsql, .arguments = &.{}, .body = "BEGIN NEW.n := 1/0; RETURN NEW; END", .owner = .auth_disabled } }, null, null, null);
    frame.new = null;
    values[0] = .{};
    try std.testing.expectError(error.SqlDivisionByZero, executeTrigger(context, division, .{ .event = .insert, .level = .statement, .frame = &frame }));
    try std.testing.expectEqual(@as(usize, 6), fixture.writes);
}
