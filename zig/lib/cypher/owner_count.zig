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
pub fn eligible(input: b.CountInput) bool {
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
    const clauses = [_]ast.Clause{
        .{ .span = .{}, .operation = .{ .match = .{ .paths = &.{path}, .predicate = input.predicate } } },
        .{ .span = .{}, .operation = .{ .project = .{ .final = true, .value = .{ .items = &.{.{ .expression = &count, .alias = "_owner_count" }} } } } },
    };
    var compiled: @import("compiler.zig").Compiled = .{ .arena = .init(a), .query = .{ .source = input.source, .clauses = &clauses }, .parameters = &.{} };
    defer compiled.deinit();
    var result = try @import("runtime.zig").executeWithWork(a, .{ .ptr = backend.ptr, .vtable = &callbacks }, graph, &compiled, input.parameters, .{ .scanned_anchors = input.max_examined, .intermediate_states = input.max_work, .memory_bytes = input.memory_bytes }, work);
    defer result.deinit();
    if (result.rows.len != 1 or result.rows[0].len != 1 or result.rows[0][0] != .integer or result.rows[0][0].integer < 0) return error.CypherInvalidBackendPage;
    return .{ .count = @intCast(result.rows[0][0].integer), .examined = result.stats.scanned_anchors };
}
