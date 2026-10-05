// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Demand-driven batch operator. Scalar evaluation only requests missing
//! decision columns; provider I/O happens here, outside the scalar evaluator.
const std = @import("std");
const scalar = @import("scalar.zig");
const decisions = @import("../functions/decisions.zig");
/// Single-source reads route by their bound physical identity. Mutations route
/// by the target. Multi-source reads use the provider's general routing policy;
/// synthetic relation tables inherit the enclosing query's selected scope.
pub fn scopedBackend(backend: @import("catalog.zig").Backend, bound: @import("describe.zig").BoundStatement) @import("catalog.zig").Backend {
    var scoped = backend;
    if (backend.decision_provider) |provider| scoped.decision_provider = provider.withSourceTable(sourceTable(bound, provider.source_table));
    return scoped;
}
fn sourceTable(bound: @import("describe.zig").BoundStatement, inherited: []const u8) []const u8 {
    if (bound.action != .read) if (bound.table) |table| return table.physical_name;
    if (bound.window) |window| return sourceTable(window.input.*, inherited);
    if (bound.relation) |relation| {
        if (relation.scans.len == 0) return "";
        const table = relation.scans[0].table.physical_name;
        for (relation.scans[1..]) |scan| if (!std.mem.eql(u8, table, scan.table.physical_name)) return "";
        return table;
    }
    if (bound.table) |table| {
        if (!std.mem.eql(u8, table.physical_name, @import("relation_binding.zig").virtual_table_name)) return table.physical_name;
        return inherited;
    }
    return "";
}

pub fn validateStatement(a: std.mem.Allocator, provider: ?decisions.DecisionProvider, bound: @import("describe.zig").BoundStatement, parameters: []const std.json.Value) anyerror!void {
    try bound.scalars.validateDecisions(a, parameters, provider);
    if (bound.insert_source) |source| try validateStatement(a, provider, source.*, parameters);
    if (bound.returning) |returning| try validateStatement(a, provider, returning.*, parameters);
    if (bound.conflict) |conflict| {
        if (conflict.predicate) |*program| try validate(a, provider, program, parameters);
        for (conflict.assignments) |optional| if (optional) |*program| try validate(a, provider, program, parameters);
        for (conflict.deferred) |optional| if (optional) |deferred| try validateStatement(a, provider, deferred.binding.*, parameters);
    }
    if (bound.joined_mutation) |mutation| try validateStatement(a, provider, mutation.input.*, parameters);
    if (bound.merge_mutation) |mutation| {
        try validateStatement(a, provider, mutation.input.*, parameters);
        for (mutation.arms) |arm| {
            if (arm.predicate) |*program| try validate(a, provider, program, parameters);
            switch (arm.action) {
                .update, .insert => |assignments| for (assignments) |assignment| {
                    if (assignment.program) |*program| try validate(a, provider, program, parameters);
                },
                .delete, .nothing => {},
            }
        }
        if (mutation.returning_plan) |returning| for (returning.programs) |*program| try validate(a, provider, program, parameters);
    }
    if (bound.relation) |relation| try validateRelation(a, provider, relation.root, parameters);
    if (bound.window) |window| {
        try validateStatement(a, provider, window.input.*, parameters);
        for (window.outputs) |*program| try validate(a, provider, program, parameters);
        for (window.orders) |*program| try validate(a, provider, program, parameters);
    }
    if (bound.aggregate) |aggregate| {
        try aggregate.input.validateDecisions(a, parameters, provider);
        for (aggregate.outputs) |*program| try validate(a, provider, program, parameters);
        for (aggregate.orders) |*program| try validate(a, provider, program, parameters);
        if (aggregate.having) |*program| try validate(a, provider, program, parameters);
    }
}
fn validateRelation(a: std.mem.Allocator, provider: ?decisions.DecisionProvider, node: *const @import("relation_binding.zig").Node, parameters: []const std.json.Value) anyerror!void {
    switch (node.operation) {
        .recursive => |part| {
            try validateRelation(a, provider, part.seed, parameters);
            try validateRelation(a, provider, part.step, parameters);
        },
        .materialized_ref => |source| try validateRelation(a, provider, source, parameters),
        .query => |part| {
            try validateRelation(a, provider, part.source, parameters);
            try validateStatement(a, provider, part.binding, parameters);
        },
        .join => |part| {
            try validateRelation(a, provider, part.left, parameters);
            try validateRelation(a, provider, part.right, parameters);
            if (part.condition) |*program| try validate(a, provider, program, parameters);
            for (part.left_keys) |*program| try validate(a, provider, program, parameters);
            for (part.right_keys) |*program| try validate(a, provider, program, parameters);
        },
        .set => |part| {
            try validateRelation(a, provider, part.left, parameters);
            try validateRelation(a, provider, part.right, parameters);
        },
        .values => |parts| for (parts) |part| try validateRelation(a, provider, part, parameters),
        else => {},
    }
}
pub fn validate(a: std.mem.Allocator, provider: ?decisions.DecisionProvider, program: *const scalar.Program, parameters: []const std.json.Value) !void {
    for (program.instructions) |instruction| {
        if (instruction.operation != .call) continue;
        const call = instruction.operation.call;
        const descriptor = decisions.descriptor(@tagName(call.function)) orelse continue;
        const args = try a.alloc(decisions.Json, call.args.len);
        args[0] = .null;
        var nullable = false;
        for (call.args[1..], args[1..]) |index, *arg| {
            const value = try program.evaluateInstruction(a, index, parameters);
            arg.* = value.value;
            nullable = nullable or value.sql_null;
        }
        if (nullable) continue;
        const questions = try decisions.questionsFor(a, descriptor.function, args);
        try decisions.validateQuestions(questions, decisions.capabilities(.antfly));
        const active = provider orelse return error.DecisionProviderUnavailable;
        try active.validate(try decisions.text(args[args.len - 1]), questions);
    }
}

pub fn evaluateBatch(a: std.mem.Allocator, provider: ?decisions.DecisionProvider, program: *const scalar.Program, rows: []const []const scalar.Datum, parameters: []const std.json.Value) ![]const scalar.Datum {
    if (!hasExternal(program)) {
        if (try @import("vector_eval.zig").evaluate(a, program, rows, parameters)) |output| return output;
        const output = try a.alloc(scalar.Datum, rows.len);
        for (rows, output) |cells, *value| value.* = try program.evaluate(a, cells, parameters, .{});
        return output;
    }
    const programs = try a.alloc(*const scalar.Program, rows.len);
    @memset(programs, program);
    return evaluateInvocations(a, provider, programs, rows, parameters);
}

/// A VALUES page can contain different programs/configurations for every cell.
/// Resolve its conditional decision demands together without retaining provider
/// payloads in the owned mutation arena.
pub fn evaluateInvocations(a: std.mem.Allocator, provider: ?decisions.DecisionProvider, programs: []const *const scalar.Program, rows: []const []const scalar.Datum, parameters: []const std.json.Value) ![]const scalar.Datum {
    if (programs.len != rows.len) return error.InvalidSqlProgram;
    const output = try a.alloc(scalar.Datum, rows.len);
    const ready = try a.alloc(bool, rows.len);
    @memset(ready, false);
    const values = try a.alloc([]?scalar.Datum, rows.len);
    for (values, programs) |*row, program| {
        row.* = try a.alloc(?scalar.Datum, program.instructions.len);
        @memset(row.*, null);
    }
    var remaining = rows.len;
    while (remaining > 0) {
        var requests: std.ArrayList(decisions.Request) = .empty;
        var demands: std.ArrayList(struct { row: usize, demand: scalar.DecisionDemand }) = .empty;
        for (rows, programs, 0..) |cells, program, i| {
            if (ready[i]) continue;
            var demand: ?scalar.DecisionDemand = null;
            const value = program.evaluate(a, cells, parameters, .{ .decision_values = values[i], .decision_demand = &demand }) catch |err| {
                if (err != error.DecisionNotEvaluated) return err;
                const pending = demand orelse return error.InvalidSqlProgram;
                const questions = try decisions.questionsFor(a, pending.function, pending.args);
                const name = try decisions.text(pending.args[pending.args.len - 1]);
                const active = provider orelse return error.DecisionProviderUnavailable;
                try active.validate(name, questions);
                try requests.append(a, .{ .decider = name, .questions = questions, .input = try decisions.text(pending.args[0]) });
                try demands.append(a, .{ .row = i, .demand = pending });
                continue;
            };
            output[i] = value;
            ready[i] = true;
            remaining -= 1;
        }
        if (requests.items.len == 0) {
            if (remaining != 0) return error.InvalidSqlProgram;
            break;
        }
        const results = try provider.?.evaluateBatch(a, requests.items);
        for (demands.items, results) |pending, result| values[pending.row][pending.demand.instruction] = scalar.Datum.json(try decisions.selectResult(pending.demand.function, result));
    }
    return output;
}
/// Evaluate a bounded relation page against several independent scalar programs.
/// Each program retains its own conditional demand and per-occurrence results.
pub fn evaluateProgramsBatch(a: std.mem.Allocator, provider: ?decisions.DecisionProvider, programs: []const scalar.Program, rows: []const []const scalar.Datum, parameters: []const std.json.Value) ![]const []const scalar.Datum {
    const output = try a.alloc([]scalar.Datum, rows.len);
    for (output) |*row| row.* = try a.alloc(scalar.Datum, programs.len);
    for (programs, 0..) |*program, column| {
        const values = try evaluateBatch(a, provider, program, rows, parameters);
        for (output, values) |row, value| row[column] = value;
    }
    return output;
}
pub fn hasExternalPrograms(programs: []const scalar.Program) bool {
    for (programs) |*program| if (hasExternal(program)) return true;
    return false;
}

pub fn evaluate(a: std.mem.Allocator, provider: ?decisions.DecisionProvider, program: *const scalar.Program, cells: []const scalar.Datum, parameters: []const std.json.Value) !scalar.Datum {
    if (!hasExternal(program)) return program.evaluate(a, cells, parameters, .{});
    return (try evaluateBatch(a, provider, program, &.{cells}, parameters))[0];
}

pub fn hasExternal(program: *const scalar.Program) bool {
    for (program.instructions) |instruction| if (instruction.operation == .call and decisions.descriptor(@tagName(instruction.operation.call.function)) != null) return true;
    return false;
}

const Mock = struct {
    expected_source: ?[]const u8 = null,
    calls: usize = 0,
    max_batch: usize = 0,
    fail: bool = false,
    fail_after: ?usize = null,
    fn validate(_: *anyopaque, _: []const u8, questions: decisions.Json) !void {
        try decisions.validateQuestions(questions, decisions.capabilities(.antfly));
    }
    fn batch(ptr: *anyopaque, a: std.mem.Allocator, requests: []const decisions.Request) ![]const decisions.Json {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.fail or (self.fail_after != null and self.calls >= self.fail_after.?)) return error.DecisionProviderUnavailable;
        self.calls += requests.len;
        self.max_batch = @max(self.max_batch, requests.len);
        const results = try a.alloc(decisions.Json, requests.len);
        for (requests, results) |request, *result| {
            if (self.expected_source) |table| try std.testing.expectEqualStrings(table, request.source_table);
            const kind = request.questions.object.get("answer").?.object.get("type").?.string;
            const bytes = if (std.mem.eql(u8, kind, "choice"))
                "{\"model\":\"mock\",\"answers\":{\"answer\":{\"type\":\"choice\",\"choice\":\"yes\",\"probabilities\":{\"yes\":0.8,\"no\":0.2}}},\"usage\":{\"input_tokens\":2,\"output_tokens\":0}}"
            else if (std.mem.eql(u8, kind, "score"))
                "{\"model\":\"mock\",\"answers\":{\"answer\":{\"type\":\"score\",\"score\":99,\"probabilities\":{\"0\":0.2,\"1\":0.8}}},\"usage\":{\"input_tokens\":2,\"output_tokens\":0}}"
            else
                "{\"model\":\"mock\",\"answers\":{\"answer\":{\"type\":\"noul\",\"noul\":0.9}},\"usage\":{\"input_tokens\":2,\"output_tokens\":0}}";
            result.* = try std.json.parseFromSliceLeaky(decisions.Json, a, bytes, .{});
        }
        return results;
    }
    pub fn provider(self: *@This()) decisions.DecisionProvider {
        return .{ .ptr = self, .validate_fn = Mock.validate, .evaluate_batch_fn = batch };
    }
};

pub const testing = if (@import("builtin").is_test) struct {
    pub const Provider = Mock;
} else struct {};

test "SQL decisions batch requested rows and preserve NULL and conditional evaluation" {
    const compiler = @import("compiler.zig");
    const a = std.testing.allocator;
    var compiled = try compiler.compile(a, "SELECT CASE WHEN enabled THEN ai_probability(body, 'Refund?', 'local') ELSE 0 END", .{});
    defer compiled.deinit();
    var program = try scalar.bind(a, compiled.statement.select.columns[0].expression.?, &.{ .{ .name = "enabled", .type = .boolean }, .{ .name = "body", .type = .string } }, &.{}, .{});
    defer program.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var mock: Mock = .{};
    const rows = [_][]const scalar.Datum{
        &.{ scalar.Datum.json(.{ .bool = true }), scalar.Datum.json(.{ .string = "charged twice" }) },
        &.{ scalar.Datum.json(.{ .bool = false }), scalar.Datum.json(.{ .string = "unused" }) },
        &.{ scalar.Datum.json(.{ .bool = true }), .{} },
        &.{ scalar.Datum.json(.{ .bool = true }), scalar.Datum.json(.{ .string = "refund" }) },
    };
    const results = try evaluateBatch(arena.allocator(), mock.provider(), &program, &rows, &.{});
    try std.testing.expectEqual(@as(usize, 2), mock.calls);
    try std.testing.expectEqual(@as(usize, 2), mock.max_batch);
    try std.testing.expectApproxEqAbs(@as(f64, 0.9), results[0].value.float, 0.001);
    try std.testing.expectEqual(@as(f64, 0), results[1].value.float);
    try std.testing.expect(results[2].sql_null);
}

test "SQL decisions bind all builtins and validate prepared question parameters without inference" {
    const compiler = @import("compiler.zig");
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var mock: Mock = .{};
    const cases = [_][]const u8{
        "SELECT ai_choice('refund', 'Classify', '{\"yes\":\"Refund\",\"no\":\"Other\"}', 'local')",
        "SELECT ai_score('refund', 'Severity', '[\"low\",\"high\"]', 'local')",
        "SELECT ai_decide('refund', $1::jsonb, 'local')",
    };
    const questions = try std.json.parseFromSliceLeaky(decisions.Json, arena.allocator(), "{\"answer\":{\"type\":\"noul\",\"instructions\":\"Refund?\"}}", .{});
    for (cases, 0..) |query, i| {
        var compiled = try compiler.compile(a, query, .{});
        defer compiled.deinit();
        var program = try scalar.bind(a, compiled.statement.select.columns[0].expression.?, &.{}, &.{}, .{});
        defer program.deinit();
        const parameters: []const decisions.Json = if (i == 2) &.{questions} else &.{};
        const calls = mock.calls;
        try validate(arena.allocator(), mock.provider(), &program, parameters);
        try std.testing.expectEqual(calls, mock.calls);
        const value = try evaluate(arena.allocator(), mock.provider(), &program, &.{}, parameters);
        switch (i) {
            0 => try std.testing.expectEqualStrings("yes", value.value.string),
            1 => try std.testing.expectApproxEqAbs(@as(f64, 0.8), value.value.float, 0.001),
            else => {
                try std.testing.expectEqualStrings("mock", value.value.object.get("model").?.string);
                try std.testing.expectError(error.DecisionLimitExceeded, validate(arena.allocator(), mock.provider(), &program, &.{decisions.jsonObject()}));
            },
        }
    }
}

/// Native cursor pages and inference pages have independent lifetimes.
pub fn rowPage(a: std.mem.Allocator, bound: @import("bound_scalars.zig").Bound, rows: []const @import("catalog.zig").Row, row_limit: usize, byte_limit: usize) ![]const []const scalar.Datum {
    var cells: std.ArrayList([]const scalar.Datum) = .empty;
    var budget: PageBudget = .{ .row_limit = row_limit, .byte_limit = byte_limit };
    for (rows) |row| {
        const values = try bound.cells(a, row);
        try cells.append(a, values);
        if (try budget.add(values)) break;
    }
    return cells.items;
}

/// Shared row/byte accounting for inference pages. One oversized row makes
/// progress, but no subsequent row shares that page.
pub const PageBudget = struct {
    row_limit: usize,
    byte_limit: usize,
    rows: usize = 0,
    bytes: usize = 0,
    pub fn add(self: *@This(), cells: []const scalar.Datum) !bool {
        var bytes: usize = 0;
        for (cells) |cell| bytes +|= try @import("operators.zig").datumBytes(cell);
        return self.addBytes(bytes);
    }
    pub fn addBytes(self: *@This(), bytes: usize) bool {
        self.rows += 1;
        self.bytes +|= bytes;
        return self.rows >= self.row_limit or self.bytes >= self.byte_limit;
    }
};

/// Preserve sort-dependent outputs once, and retain deferred input cells in
/// Top-K's owned, budgeted rows until final pagination selects their consumers.
pub const SortedProjection = struct {
    outputs: []const scalar.Program,
    orders: []const scalar.Program,
    order_outputs: []const ?usize,
    deferred: []bool,
    has_deferred: bool,
    pub fn init(a: std.mem.Allocator, outputs: []const scalar.Program, orders: []const scalar.Program, order_outputs: []const ?usize) !@This() {
        const deferred = try a.alloc(bool, outputs.len);
        for (outputs, deferred) |*program, *flag| flag.* = hasExternal(program);
        for (order_outputs) |optional| if (optional) |index| {
            deferred[index] = false;
        };
        return .{ .outputs = outputs, .orders = orders, .order_outputs = order_outputs, .deferred = deferred, .has_deferred = std.mem.indexOfScalar(bool, deferred, true) != null };
    }
    pub fn add(self: @This(), context: anytype, a: std.mem.Allocator, top: *@import("operators.zig").TopK, inputs: []const []const scalar.Datum, ordinals: []const u64) !void {
        const values = try a.alloc([]scalar.Datum, inputs.len);
        for (inputs, values) |input, *row| {
            row.* = try a.alloc(scalar.Datum, self.outputs.len + if (self.has_deferred) input.len else @as(usize, 0));
            @memset(row.*, .{});
            if (self.has_deferred) @memcpy(row.*[self.outputs.len..], input);
        }
        for (self.outputs, self.deferred, 0..) |*program, deferred, column| if (!deferred) {
            const output = try evaluateBatch(a, context.backend.decision_provider, program, inputs, context.parameters);
            for (values, output) |row, value| row[column] = value;
        };
        const keys = try a.alloc([]scalar.Datum, inputs.len);
        for (keys) |*row| row.* = try a.alloc(scalar.Datum, self.orders.len);
        for (self.orders, 0..) |*program, column| {
            if (column < self.order_outputs.len and self.order_outputs[column] != null) {
                for (keys, values) |row, value| row[column] = value[self.order_outputs[column].?];
            } else {
                const output = try evaluateBatch(a, context.backend.decision_provider, program, inputs, context.parameters);
                for (keys, output) |row, value| row[column] = value;
            }
        }
        for (values, keys, ordinals) |row, key, ordinal| try top.add(.{ .values = row, .keys = key, .ordinal = ordinal });
    }
    pub fn finish(self: @This(), context: anytype, top: *@import("operators.zig").TopK, offset: usize, limit: usize, implicit_limit: bool) !@import("runtime.zig").Output {
        const ordered = try top.finishPage(context.arena, offset, limit + @intFromBool(implicit_limit));
        const remaining = ordered.len;
        if (implicit_limit and remaining > limit) return error.SqlResultTooLarge;
        const start = top.released;
        const selected = ordered[0..@min(remaining, limit)];
        const rows = try context.arena.alloc([]const std.json.Value, selected.len);
        const flags = try context.arena.alloc([]const bool, selected.len);
        var first: usize = 0;
        while (first < selected.len) {
            try context.checkpoint();
            var arena = std.heap.ArenaAllocator.init(context.alloc);
            defer arena.deinit();
            const a = arena.allocator();
            var inputs: std.ArrayList([]const scalar.Datum) = .empty;
            var budget: PageBudget = .{ .row_limit = context.limits.page_rows, .byte_limit = context.limits.page_bytes };
            for (selected[first..]) |row| {
                const input = row.values[self.outputs.len..];
                try inputs.append(a, input);
                if (try budget.add(input)) break;
            }
            const values = try a.alloc([]scalar.Datum, inputs.items.len);
            for (selected[first..][0..inputs.items.len], values) |row, *out| out.* = try a.dupe(scalar.Datum, row.values[0..self.outputs.len]);
            for (self.outputs, self.deferred, 0..) |*program, deferred, column| if (deferred) {
                const output = try evaluateBatch(a, context.backend.decision_provider, program, inputs.items, context.parameters);
                for (values, output) |row, value| row[column] = value;
            };
            for (values, first..) |row, index| {
                const output = try context.arena.alloc(std.json.Value, self.outputs.len);
                const nulls = try context.arena.alloc(bool, self.outputs.len);
                for (row, output, nulls) |value, *out, *flag| {
                    out.* = try context.outputValue(value.value);
                    flag.* = value.sql_null;
                }
                rows[index] = output;
                flags[index] = nulls;
                top.releaseFinishedRow(start + index);
            }
            first += inputs.items.len;
        }
        return .{ .columns = context.binding.columns, .rows = rows, .sql_nulls = flags, .command_tag = "SELECT" };
    }
};

test "SQL decision routing selects catalog identity and clears ambiguous relation scopes" {
    const catalog = @import("catalog.zig");
    const bound_type = @import("describe.zig").BoundStatement;
    const table: catalog.Table = .{ .id = 7, .physical_name = "private-physical", .schema_version = 1, .columns = &.{} };
    var provider: Mock = .{};
    const backend: catalog.Backend = .{ .ptr = &provider, .decision_provider = provider.provider().withSourceTable("enclosing"), .vtable = undefined };
    var bound: bound_type = .{ .table = table, .action = .read, .columns = &.{}, .parameter_types = &.{}, .json_literals = .empty };
    try std.testing.expectEqualStrings("private-physical", scopedBackend(backend, bound).decision_provider.?.source_table);
    bound.table = .{ .id = 0, .schema_version = 0, .physical_name = @import("relation_binding.zig").virtual_table_name, .columns = &.{} };
    try std.testing.expectEqualStrings("enclosing", scopedBackend(backend, bound).decision_provider.?.source_table);
    bound.table = null;
    try std.testing.expectEqualStrings("", scopedBackend(backend, bound).decision_provider.?.source_table);
    const scans = [_]catalog.StatementScan{
        .{ .table = table, .request = .{ .fields = &.{}, .limit = 1 } },
        .{ .table = .{ .id = 8, .physical_name = "other-physical", .schema_version = 1, .columns = &.{} }, .request = .{ .fields = &.{}, .limit = 1 } },
    };
    const node: @import("relation_binding.zig").Node = .{ .columns = &.{}, .operation = .singleton };
    var relation: @import("relation_binding.zig").Bound = .{ .root = &node, .scans = scans[0..1], .table = table, .statement = .{ .columns = &.{}, .table = null } };
    bound.relation = &relation;
    try std.testing.expectEqualStrings("private-physical", scopedBackend(backend, bound).decision_provider.?.source_table);
    relation.scans = &scans;
    try std.testing.expectEqualStrings("", scopedBackend(backend, bound).decision_provider.?.source_table);
    bound.table = table;
    bound.action = .write;
    try std.testing.expectEqualStrings("private-physical", scopedBackend(backend, bound).decision_provider.?.source_table);
}
