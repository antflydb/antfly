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
    for (input.node.properties) |property| if (!expression(property.value, input.node.alias)) return false;
    if (input.predicate) |predicate| if (!expression(predicate, input.node.alias)) return false;
    return true;
}
/// A request is table-local even when its logical pattern has no label or
/// several labels. The coordinator visits each physical table only once.
pub fn permitsTable(graph: catalog.Graph, input: b.CountInput, table: []const u8) bool {
    if (input.node.labels.len == 0) {
        for (graph.labels) |label| if (std.mem.eql(u8, label.table, table)) return true;
        return false;
    }
    for (input.node.labels) |name| {
        const label = graph.label(name) orelse return false;
        if (!std.mem.eql(u8, label.table, table)) return false;
    }
    return true;
}
pub fn admits(graph: catalog.Graph, input: b.CountInput, node: *const @import("value.zig").Node, work: *@import("value_work.zig").Work) !bool {
    if (input.node.labels.len == 0) {
        for (graph.labels) |label| if (try graph.admits(node, label.name, work)) return true;
        return false;
    }
    for (input.node.labels) |name| if (!try graph.admits(node, name, work)) return false;
    return true;
}
/// A private count node exposes neither the full entity nor labels(). Keep
/// catalog authorization separate from the membership actually observed here.
pub fn membershipGraph(a: std.mem.Allocator, graph: catalog.Graph, input: b.CountInput, table: []const u8) !catalog.Graph {
    if (!permitsTable(graph, input, table)) return error.Forbidden;
    var local = graph;
    var labels: std.ArrayList(catalog.Label) = .empty;
    if (input.node.labels.len == 0) {
        // One unconditional label certifies the entire physical-table union.
        for (graph.labels) |label| if (std.mem.eql(u8, label.table, table) and label.predicate.len == 0) {
            try labels.append(a, label);
            local.labels = labels.items;
            return local;
        };
        for (graph.labels) |label| if (std.mem.eql(u8, label.table, table)) try labels.append(a, label);
    } else for (input.node.labels) |name| try labels.append(a, graph.label(name).?);
    local.labels = labels.items;
    return local;
}
fn alreadyBound(_: *anyopaque, _: std.mem.Allocator, _: catalog.Graph) !void {}
pub fn run(a: std.mem.Allocator, backend: b.Backend, graph: catalog.Graph, table: []const u8, input: b.CountInput, work: ?b.Work) !?b.CountResult {
    if (!eligible(input)) return null;
    if (!permitsTable(graph, input, table)) return error.Forbidden;
    // The portable owner fallback must never scan another physical owner.
    var local = graph;
    var labels: std.ArrayList(catalog.Label) = .empty;
    defer labels.deinit(a);
    for (graph.labels) |label| if (std.mem.eql(u8, label.table, table)) try labels.append(a, label);
    local.labels = labels.items;
    local.relationships = &.{};
    // Authenticate the original catalog, then keep the backend bound to it.
    // The evaluator's table-local view must not replace a retained adapter's
    // catalog with a temporary slice that dies when this reduction returns.
    try backend.vtable.bind(backend.ptr, a, graph);
    var callbacks = backend.vtable.*;
    callbacks.bind = alreadyBound;
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
    var result = try @import("runtime.zig").executeWithWork(a, .{ .ptr = backend.ptr, .vtable = &callbacks }, local, &compiled, input.parameters, .{ .scanned_anchors = input.max_examined, .intermediate_states = input.max_work, .memory_bytes = input.memory_bytes }, work);
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

/// Preserve actual values observed by membership and value-sensitive predicates.
/// Direct COUNT arguments and nullness predicates may share a checked sentinel.
pub const Projection = struct {
    fields: []const []const u8,
    presence: []const []const u8,
    arguments: []const Argument,
    pub const Argument = union(enum) { field: []const u8, present: bool };
    fn dependencies(a: std.mem.Allocator, expr: *const ast.Expr, values: *std.StringHashMapUnmanaged(void), fields: *std.StringHashMapUnmanaged(void)) !bool {
        return switch (expr.value) {
            .literal, .parameter => true,
            .property => |p| if (p.base.value == .variable) add: {
                try values.put(a, p.name, {});
                break :add true;
            } else try dependencies(a, p.base, values, fields),
            .unary => |u| nullness: {
                if ((u.op == .is_null or u.op == .is_not_null) and u.operand.value == .property and u.operand.value.property.base.value == .variable) {
                    try fields.put(a, u.operand.value.property.name, {});
                    break :nullness true;
                }
                break :nullness try dependencies(a, u.operand, values, fields);
            },
            .binary => |op| try dependencies(a, op.left, values, fields) and try dependencies(a, op.right, values, fields),
            .comparison => |chain| all: {
                for (chain.operands) |operand| if (!try dependencies(a, operand, values, fields)) break :all false;
                break :all true;
            },
            .list => |items| all: {
                for (items) |item| if (!try dependencies(a, item, values, fields)) break :all false;
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
            if (!try dependencies(a, property.value, &values, &fields)) return null;
        }
        if (input.predicate) |predicate| if (!try dependencies(a, predicate, &values, &fields)) return null;
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
