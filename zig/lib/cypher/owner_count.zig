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

//! Exact owner-local aggregation. The same evaluator enforces Cypher NULL and
//! numeric semantics, TTL, row policies and label membership at the retained cut.
const std = @import("std");
const b = @import("backend.zig");
const ast = @import("ast.zig");
const catalog = @import("catalog.zig");
pub fn expression(expr: *const ast.Expr, alias: ?[]const u8) bool {
    return switch (expr.value) {
        .literal, .parameter => true,
        .variable => |name| alias != null and std.mem.eql(u8, name, alias.?),
        .property => |p| expression(p.base, alias),
        .unary => |u| expression(u.operand, alias),
        .binary => |op| op.op != .regex and expression(op.left, alias) and expression(op.right, alias),
        .comparison => |c| blk: {
            for (c.operands) |operand| if (!expression(operand, alias)) break :blk false;
            break :blk true;
        },
        .list => |items| blk: {
            for (items) |item| if (!expression(item, alias)) break :blk false;
            break :blk true;
        },
        else => false,
    };
}
/// These arguments are total: moving their evaluation across owners cannot
/// reorder scalar failures. Fallible expressions keep the scalar evaluator.
pub fn argument(expr: *const ast.Expr, alias: ?[]const u8) bool {
    return switch (expr.value) {
        .literal, .parameter => true,
        .variable => |name| alias != null and std.mem.eql(u8, name, alias.?),
        .property => |p| argument(p.base, alias),
        else => false,
    };
}
pub fn eligible(input: b.CountInput) bool {
    if (input.arguments.len > b.max_count_arguments) return false;
    for (input.arguments) |expr| if (expr) |value| if (!argument(value, input.node.alias)) return false;
    if (input.node.labels.len != 1) return false;
    for (input.node.properties) |property| if (!expression(property.value, input.node.alias)) return false;
    if (input.predicate) |predicate| if (!expression(predicate, input.node.alias)) return false;
    return true;
}
pub fn run(a: std.mem.Allocator, backend: b.Backend, graph: catalog.Graph, table: []const u8, input: b.CountInput, work: ?b.Work) !?b.CountResult {
    if (!eligible(input)) return null;
    const label = graph.label(input.node.labels[0]) orelse return error.UnknownGraphLabel;
    if (!std.mem.eql(u8, label.table, table)) return error.Forbidden;
    var callbacks = backend.vtable.*;
    callbacks.count_nodes = null;
    callbacks.startup_work = null;
    const count: ast.Expr = .{ .span = .{}, .value = .{ .call = .{ .name = "count", .args = &.{}, .star = true } } };
    const path: ast.Path = .{ .span = .{}, .alias = null, .nodes = &.{input.node}, .edges = &.{} };
    const calls = try a.alloc(ast.Expr, input.arguments.len + 1);
    defer a.free(calls);
    const items = try a.alloc(ast.Item, calls.len);
    defer a.free(items);
    const args = try a.alloc([1]*const ast.Expr, input.arguments.len);
    defer a.free(args);
    var initialized: usize = 0;
    defer for (items[1..][0..initialized]) |item| a.free(item.alias.?);
    calls[0] = count;
    items[0] = .{ .expression = &calls[0], .alias = "_owner_rows" };
    for (input.arguments, 0..) |expr, i| {
        calls[i + 1] = count;
        if (expr) |value| {
            args[i] = .{value};
            calls[i + 1].value.call.star = false;
            calls[i + 1].value.call.args = &args[i];
        }
        items[i + 1] = .{ .expression = &calls[i + 1], .alias = try std.fmt.allocPrint(a, "_owner_count_{d}", .{i}) };
        initialized += 1;
    }
    const clauses = [_]ast.Clause{
        .{ .span = .{}, .operation = .{ .match = .{ .paths = &.{path}, .predicate = input.predicate } } },
        .{ .span = .{}, .operation = .{ .project = .{ .final = true, .value = .{ .items = items } } } },
    };
    var compiled: @import("compiler.zig").Compiled = .{ .arena = .init(a), .query = .{ .source = input.source, .clauses = &clauses }, .parameters = &.{} };
    defer compiled.deinit();
    var result = try @import("runtime.zig").executeWithWork(a, .{ .ptr = backend.ptr, .vtable = &callbacks }, graph, &compiled, input.parameters, .{ .scanned_anchors = input.max_examined, .intermediate_states = input.max_work, .memory_bytes = input.memory_bytes }, work);
    defer result.deinit();
    if (result.rows.len != 1 or result.rows[0].len != items.len or result.rows[0][0] != .integer or result.rows[0][0].integer < 0) return error.CypherInvalidBackendPage;
    const counts = try a.alloc(usize, input.arguments.len);
    errdefer a.free(counts);
    for (counts, result.rows[0][1..]) |*out, value| {
        if (value != .integer or value.integer < 0) return error.CypherInvalidBackendPage;
        out.* = @intCast(value.integer);
    }
    const out: b.CountResult = .{ .count = @intCast(result.rows[0][0].integer), .counts = counts, .examined = result.stats.scanned_anchors };
    try out.validateFor(input.max_examined, input.arguments.len);
    return out;
}

/// Preserve actual values observed by membership and predicates. Only properties
/// observed exclusively by direct COUNT arguments may use a nullness sentinel.
pub const Projection = struct {
    fields: []const []const u8,
    presence: []const []const u8,
    arguments: []const Argument,
    pub const Argument = union(enum) { field: []const u8, present: bool };
    fn dependencies(a: std.mem.Allocator, expr: *const ast.Expr, values: *std.StringHashMapUnmanaged(void)) !bool {
        return switch (expr.value) {
            .literal, .parameter => true,
            .property => |p| if (p.base.value == .variable) add: {
                try values.put(a, p.name, {});
                break :add true;
            } else try dependencies(a, p.base, values),
            .unary => |u| try dependencies(a, u.operand, values),
            .binary => |op| try dependencies(a, op.left, values) and try dependencies(a, op.right, values),
            .comparison => |chain| all: {
                for (chain.operands) |operand| if (!try dependencies(a, operand, values)) break :all false;
                break :all true;
            },
            .list => |items| all: {
                for (items) |item| if (!try dependencies(a, item, values)) break :all false;
                break :all true;
            },
            // Whole entities and extension expressions keep the ordinary path.
            else => false,
        };
    }
    pub fn init(a: std.mem.Allocator, graph: catalog.Graph, input: b.CountInput) !?Projection {
        if (!eligible(input)) return null;
        var fields: std.StringHashMapUnmanaged(void) = .empty;
        var values: std.StringHashMapUnmanaged(void) = .empty;
        for (graph.labels) |label| for (label.predicate) |entry| {
            try fields.put(a, entry.name, {});
            try values.put(a, entry.name, {});
        };
        for (input.node.properties) |property| {
            try values.put(a, property.name, {});
            if (!try dependencies(a, property.value, &values)) return null;
        }
        if (input.predicate) |predicate| if (!try dependencies(a, predicate, &values)) return null;
        var dependencies_ = values.keyIterator();
        while (dependencies_.next()) |name| try fields.put(a, name.*, {});
        const arguments = try a.alloc(Argument, input.arguments.len);
        for (input.arguments, arguments) |expr, *out| {
            const value = expr orelse {
                out.* = .{ .present = true };
                continue;
            };
            out.* = switch (value.value) {
                .literal => |literal| .{ .present = literal != .null },
                .parameter => |name| parameter: {
                    for (input.parameters) |entry| if (std.mem.eql(u8, name, entry.name)) break :parameter .{ .present = entry.value != .null };
                    // Missing parameters are diagnosed by the scalar evaluator,
                    // even if the source happens to be empty.
                    return null;
                },
                .variable => .{ .present = true },
                .property => |property| property: {
                    if (property.base.value != .variable) return null;
                    try fields.put(a, property.name, {});
                    break :property .{ .field = property.name };
                },
                else => return null,
            };
        }
        const names = try a.alloc([]const u8, fields.count());
        var presence: std.ArrayList([]const u8) = .empty;
        var iterator = fields.keyIterator();
        for (names) |*name| {
            name.* = iterator.next().?.*;
            // Native projection syntax must never reinterpret a literal name.
            if (name.len == 0) return null;
            for (name.*) |byte| if (!std.ascii.isAlphanumeric(byte) and byte != '_') return null;
            if (!values.contains(name.*)) try presence.append(a, name.*);
        }
        return .{ .fields = names, .presence = presence.items, .arguments = arguments };
    }
};
