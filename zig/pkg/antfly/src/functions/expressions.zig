// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Closed JSON expression vocabulary and named computation DAG.
const std = @import("std");
const d = @import("decisions.zig");
const Json = d.Json;
pub const Scope = enum { candidates, matches };
pub const Plan = struct {
    scope: Scope,
    graph_query: ?[]const u8,
    candidate_count: u32,
    compute: std.json.ObjectMap,
    where: ?Json,
    order_by: ?Json,
    aggregations: ?Json,
    order: []const usize,
    required_fields: []const []const u8,
    pub fn parse(a: std.mem.Allocator, value: Json) !Plan {
        const root = try d.object(value);
        try allowedKeys(root, &.{ "scope", "candidate_count", "max_rows", "compute", "where", "order_by", "aggregations", "graph_query" });
        const scope = std.meta.stringToEnum(Scope, try d.text(root.get("scope") orelse return error.InvalidFunctionExpression)) orelse return error.InvalidFunctionExpression;
        const limit = root.get(if (scope == .candidates) "candidate_count" else "max_rows") orelse return error.InvalidFunctionExpression;
        if (limit != .integer or limit.integer <= 0 or limit.integer > 10000) return error.DecisionLimitExceeded;
        const compute = try d.object(root.get("compute") orelse return error.InvalidFunctionExpression);
        if (compute.count() == 0 or compute.count() > 64) return error.DecisionLimitExceeded;
        var validator: Validator = .{ .a = a, .bindings = compute, .states = try a.alloc(u8, compute.count()) };
        @memset(validator.states, 0);
        for (0..compute.count()) |i| try validator.binding(i);
        const where = root.get("where");
        if (where) |predicate| try validator.predicate(predicate, 0);
        const order_by = root.get("order_by");
        if (order_by) |orders| {
            if (orders != .array or orders.array.items.len > 16) return error.InvalidFunctionExpression;
            for (orders.array.items) |item| {
                const obj = try d.object(item);
                try allowedKeys(obj, &.{ "expression", "descending" });
                try validator.expression(obj.get("expression") orelse return error.InvalidFunctionExpression, 0);
                if (obj.get("descending")) |desc| if (desc != .bool) return error.InvalidFunctionExpression;
            }
        }
        const aggregations = root.get("aggregations");
        if (aggregations) |aggs| {
            const obj = try d.object(aggs);
            var it = obj.iterator();
            while (it.next()) |entry| {
                const spec = try d.object(entry.value_ptr.*);
                try allowedKeys(spec, &.{ "type", "expression" });
                const kind = try d.text(spec.get("type") orelse return error.InvalidFunctionExpression);
                if (!std.mem.eql(u8, kind, "terms") and !std.mem.eql(u8, kind, "avg") and !std.mem.eql(u8, kind, "sum") and !std.mem.eql(u8, kind, "count")) return error.InvalidFunctionExpression;
                try validator.expression(spec.get("expression") orelse return error.InvalidFunctionExpression, 0);
            }
        }
        return .{ .graph_query = if (root.get("graph_query")) |name| try d.text(name) else null, .scope = scope, .candidate_count = @intCast(limit.integer), .compute = compute, .where = where, .order_by = order_by, .aggregations = aggregations, .order = validator.order.items, .required_fields = validator.fields.items };
    }
};
fn allowedKeys(obj: std.json.ObjectMap, allowed: []const []const u8) !void {
    for (obj.keys()) |key| {
        var found = false;
        for (allowed) |name| if (std.mem.eql(u8, name, key)) {
            found = true;
            break;
        };
        if (!found) return error.InvalidFunctionExpression;
    }
}

/// Validates every specification, including untaken branches, without inference.
pub fn validateProviders(a: std.mem.Allocator, value: Json, provider: d.DecisionProvider) anyerror!void {
    switch (value) {
        .object => |obj| {
            if (obj.get("literal") != null or obj.get("field") != null or obj.get("ref") != null) return;
            if (obj.get("call")) |name| if (name == .string) {
                const function = d.descriptor(try d.text(name)) orelse return error.UnknownQueryFunction;
                const args = try callArgs(a, function.function, obj, .null);
                try provider.validate(try d.text(obj.get("decider").?), try d.questionsFor(a, function.function, args));
                try validateProviders(a, obj.get("input").?, provider);
                return;
            };
            for (obj.values()) |child| try validateProviders(a, child, provider);
        },
        .array => |array| for (array.items) |child| try validateProviders(a, child, provider),
        else => {},
    }
}

const ValueType = enum { dynamic, null_value, text, number, boolean, json };
fn expressionType(value: Json, bindings: std.json.ObjectMap, depth: usize) ValueType {
    if (depth > 64) return .dynamic;
    const obj = value.object;
    if (obj.get("literal")) |literal| return switch (literal) {
        .null => .null_value,
        .string => .text,
        .integer, .float => .number,
        .bool => .boolean,
        else => .json,
    };
    if (obj.get("call")) |name| return switch (d.descriptor(name.string).?.result) {
        .json => .json,
        .string => .text,
        .number => .number,
    };
    if (obj.get("ref")) |ref| {
        if (std.mem.indexOfScalar(u8, ref.string, '.') != null) return .dynamic;
        return expressionType(bindings.get(ref.string) orelse return .dynamic, bindings, depth + 1);
    }
    return .dynamic;
}

const Validator = struct {
    a: std.mem.Allocator,
    bindings: std.json.ObjectMap,
    states: []u8,
    order: std.ArrayList(usize) = .empty,
    fields: std.ArrayList([]const u8) = .empty,
    steps: usize = 0,
    fn check(self: *@This(), depth: usize) !void {
        self.steps += 1;
        if (depth > 64 or self.steps > 8192) return error.DecisionLimitExceeded;
    }
    fn binding(self: *@This(), i: usize) anyerror!void {
        if (self.states[i] == 2) return;
        if (self.states[i] == 1) return error.CyclicFunctionBinding;
        const name = self.bindings.keys()[i];
        _ = try d.text(.{ .string = name });
        if (std.mem.indexOfScalar(u8, name, '.') != null) return error.InvalidFunctionExpression;
        self.states[i] = 1;
        try self.expression(self.bindings.values()[i], 0);
        self.states[i] = 2;
        try self.order.append(self.a, i);
    }
    fn expression(self: *@This(), value: Json, depth: usize) anyerror!void {
        try self.check(depth);
        const obj = try d.object(value);
        if (obj.get("literal") != null) {
            if (obj.count() != 1) return error.InvalidFunctionExpression;
            return;
        }
        if (obj.get("field")) |field| {
            if (obj.count() != 1) return error.InvalidFunctionExpression;
            const path = try d.text(field);
            for (self.fields.items) |existing| if (std.mem.eql(u8, path, existing)) return;
            try self.fields.append(self.a, path);
            return;
        }
        if (obj.get("ref")) |ref| {
            if (obj.count() != 1) return error.InvalidFunctionExpression;
            const name = try d.text(ref);
            const dot = std.mem.indexOfScalar(u8, name, '.') orelse name.len;
            const i = self.bindings.getIndex(name[0..dot]) orelse return error.UnknownFunctionBinding;
            try self.binding(i);
            return;
        }
        const name = try d.text(obj.get("call") orelse return error.InvalidFunctionExpression);
        const desc = d.descriptor(name) orelse return error.UnknownQueryFunction;
        try allowedKeys(obj, switch (desc.function) {
            .ai_decide => &.{ "call", "input", "questions", "decider" },
            .ai_probability => &.{ "call", "input", "statement", "decider" },
            .ai_choice, .ai_score => &.{ "call", "input", "instructions", "criteria", "decider" },
        });
        try self.expression(obj.get("input") orelse return error.InvalidFunctionExpression, depth + 1);
        const input_type = expressionType(obj.get("input").?, self.bindings, 0);
        if (input_type != .text and input_type != .dynamic and input_type != .null_value) return error.FunctionTypeMismatch;
        _ = try d.text(obj.get("decider") orelse return error.InvalidFunctionExpression);
        const args = try callArgs(self.a, desc.function, obj, .null);
        const questions = try d.questionsFor(self.a, desc.function, args);
        try d.validateQuestions(questions, d.capabilities(.antfly));
    }
    fn predicate(self: *@This(), value: Json, depth: usize) anyerror!void {
        try self.check(depth);
        const obj = try d.object(value);
        if (obj.count() != 1) return error.InvalidFunctionExpression;
        const op = obj.keys()[0];
        const args = obj.values()[0];
        if (std.mem.eql(u8, op, "not")) return self.predicate(args, depth + 1);
        if (std.mem.eql(u8, op, "is_null")) return self.expression(args, depth + 1);
        if (args != .array) return error.InvalidFunctionExpression;
        if (std.mem.eql(u8, op, "and") or std.mem.eql(u8, op, "or")) {
            for (args.array.items) |item| try self.predicate(item, depth + 1);
            return;
        }
        if (args.array.items.len != 2) return error.InvalidFunctionExpression;
        for ([_][]const u8{ "eq", "neq", "lt", "lte", "gt", "gte" }) |allowed| if (std.mem.eql(u8, op, allowed)) {
            for (args.array.items) |item| try self.expression(item, depth + 1);
            const left_type = expressionType(args.array.items[0], self.bindings, 0);
            const right_type = expressionType(args.array.items[1], self.bindings, 0);
            if (left_type != .dynamic and right_type != .dynamic and left_type != .null_value and right_type != .null_value and (left_type != right_type or left_type == .json)) return error.FunctionTypeMismatch;
            return;
        };
        return error.InvalidFunctionExpression;
    }
};
pub fn member(value: Json, path: []const u8) Json {
    var current = value;
    var parts = std.mem.splitScalar(u8, path, '.');
    while (parts.next()) |part| {
        if (current != .object) return .null;
        current = current.object.get(part) orelse return .null;
    }
    return current;
}
fn callArgs(a: std.mem.Allocator, function: d.Function, obj: std.json.ObjectMap, input: Json) ![]const Json {
    const args = try a.alloc(Json, d.descriptor(@tagName(function)).?.argument_count);
    args[0] = input;
    args[args.len - 1] = obj.get("decider").?;
    if (function == .ai_decide) args[1] = obj.get("questions") orelse return error.InvalidFunctionExpression else args[1] = obj.get(if (function == .ai_probability) "statement" else "instructions") orelse return error.InvalidFunctionExpression;
    if (function == .ai_choice or function == .ai_score) args[2] = obj.get("criteria") orelse return error.InvalidFunctionExpression;
    return args;
}
pub const Evaluation = struct { computed: Json, accepted: bool };
pub fn evaluateBatch(a: std.mem.Allocator, plan: Plan, provider: d.DecisionProvider, documents: []const Json) ![]Evaluation {
    const rows = try a.alloc(Evaluation, documents.len);
    for (rows) |*row| row.* = .{ .computed = d.jsonObject(), .accepted = true };
    for (plan.order) |i| {
        const expr = plan.compute.values()[i];
        const name = plan.compute.keys()[i];
        const values = try evalExpressions(a, provider, expr, documents, rows, 0);
        for (rows, values) |*row, value| try d.put(a, &row.computed, name, value);
    }
    if (plan.where) |predicate| for (rows, documents) |*row, document| {
        const result = try evalPredicate(a, provider, predicate, document, row.*, 0);
        row.accepted = result == .bool and result.bool;
    };
    return rows;
}
fn evalExpressions(a: std.mem.Allocator, provider: d.DecisionProvider, expr: Json, documents: []const Json, rows: []const Evaluation, depth: usize) anyerror![]const Json {
    if (depth > 64) return error.DecisionLimitExceeded;
    const obj = expr.object;
    const values = try a.alloc(Json, documents.len);
    if (obj.get("literal")) |literal| {
        @memset(values, literal);
        return values;
    }
    if (obj.get("field")) |field| {
        for (documents, values) |doc, *v| v.* = member(doc, field.string);
        return values;
    }
    if (obj.get("ref")) |ref| {
        for (rows, values) |row, *v| v.* = member(row.computed, ref.string);
        return values;
    }
    const function = d.descriptor(obj.get("call").?.string).?.function;
    const inputs = try evalExpressions(a, provider, obj.get("input").?, documents, rows, depth + 1);
    var requests: std.ArrayList(d.Request) = .empty;
    var indexes: std.ArrayList(usize) = .empty;
    const args = try callArgs(a, function, obj, .null);
    const questions = try d.questionsFor(a, function, args);
    const decider = try d.text(args[args.len - 1]);
    try provider.validate(decider, questions);
    for (inputs, values, 0..) |input, *value, i| {
        value.* = .null;
        if (input == .null) continue;
        try requests.append(a, .{ .input = try d.text(input), .questions = questions, .decider = decider });
        try indexes.append(a, i);
    }
    if (requests.items.len > 0) {
        const results = try provider.evaluateBatch(a, requests.items);
        for (indexes.items, results) |i, result| values[i] = try d.selectResult(function, result);
    }
    return values;
}
pub fn evalExpression(a: std.mem.Allocator, provider: d.DecisionProvider, expr: Json, document: Json, row: Evaluation) !Json {
    return (try evalExpressions(a, provider, expr, &.{document}, &.{row}, 0))[0];
}
fn evalPredicate(a: std.mem.Allocator, provider: d.DecisionProvider, predicate: Json, document: Json, row: Evaluation, depth: usize) anyerror!Json {
    if (depth > 64) return error.DecisionLimitExceeded;
    const op = predicate.object.keys()[0];
    const args = predicate.object.values()[0];
    if (std.mem.eql(u8, op, "not")) {
        const v = try evalPredicate(a, provider, args, document, row, depth + 1);
        return if (v == .null) .null else .{ .bool = !v.bool };
    }
    if (std.mem.eql(u8, op, "is_null")) return .{ .bool = (try evalExpression(a, provider, args, document, row)) == .null };
    if (std.mem.eql(u8, op, "and") or std.mem.eql(u8, op, "or")) {
        const is_and = std.mem.eql(u8, op, "and");
        var unknown = false;
        for (args.array.items) |arg| {
            const v = try evalPredicate(a, provider, arg, document, row, depth + 1);
            if (v == .null) {
                unknown = true;
                continue;
            }
            if (v.bool != is_and) return v;
        }
        return if (unknown) .null else .{ .bool = is_and };
    }
    const left = try evalExpression(a, provider, args.array.items[0], document, row);
    const right = try evalExpression(a, provider, args.array.items[1], document, row);
    if (left == .null or right == .null) return .null;
    const cmp = try compare(left, right);
    return .{ .bool = if (std.mem.eql(u8, op, "eq")) cmp == .eq else if (std.mem.eql(u8, op, "neq")) cmp != .eq else if (std.mem.eql(u8, op, "lt")) cmp == .lt else if (std.mem.eql(u8, op, "lte")) cmp != .gt else if (std.mem.eql(u8, op, "gt")) cmp == .gt else cmp != .lt };
}
pub fn compare(left: Json, right: Json) !std.math.Order {
    if (left == .null or right == .null) return if (left == .null and right == .null) .eq else if (left == .null) .gt else .lt;
    if ((left == .integer or left == .float) and (right == .integer or right == .float)) {
        if (left == .integer and right == .integer) return std.math.order(left.integer, right.integer);
        const l: f128 = if (left == .integer) @floatFromInt(left.integer) else @floatCast(left.float);
        const r: f128 = if (right == .integer) @floatFromInt(right.integer) else @floatCast(right.float);
        if (!std.math.isFinite(l) or !std.math.isFinite(r)) return error.InvalidFunctionExpression;
        return std.math.order(l, r);
    }
    if (left == .string and right == .string) return std.mem.order(u8, left.string, right.string);
    if (left == .bool and right == .bool) return std.math.order(@intFromBool(left.bool), @intFromBool(right.bool));
    return error.FunctionTypeMismatch;
}

test "function bindings reject cycles and unknown names and order dependencies" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const good = try std.json.parseFromSliceLeaky(Json, a, "{\"scope\":\"candidates\",\"candidate_count\":20,\"compute\":{\"second\":{\"ref\":\"first\"},\"first\":{\"field\":\"body\"}}}", .{});
    const plan = try Plan.parse(a, good);
    try std.testing.expectEqualSlices(usize, &.{ 1, 0 }, plan.order);
    const cyclic = try std.json.parseFromSliceLeaky(Json, a, "{\"scope\":\"matches\",\"max_rows\":20,\"compute\":{\"a\":{\"ref\":\"b\"},\"b\":{\"ref\":\"a\"}}}", .{});
    try std.testing.expectError(error.CyclicFunctionBinding, Plan.parse(a, cyclic));
}
