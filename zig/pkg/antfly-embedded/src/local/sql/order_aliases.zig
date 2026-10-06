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

//! ORDER BY aliases belong to the output domain, not the window input domain.
const std = @import("std");
const ast = @import("ast.zig");

pub fn normalize(alloc: std.mem.Allocator, statement: ast.Select) !ast.Select {
    if (statement.order_aliases_expanded) return statement;
    var builder: Builder = .{ .alloc = alloc };
    defer builder.aliases.deinit(alloc);
    // Build one registry for the whole statement, including implicit function
    // labels. A null entry records ambiguity without rejecting unused labels.
    for (statement.columns) |projection| {
        // Plain source fields continue through ordinary column resolution.
        // Explicit aliases retain precedence over those input names.
        const name = projection.alias orelse if (projection.expression) |expression| (if (expression.* == .call) expression.call.name else continue) else continue;
        const entry = try builder.aliases.getOrPut(alloc, name);
        entry.value_ptr.* = if (entry.found_existing) null else projection;
    }
    var out = statement;
    const orders = try alloc.dupe(ast.Order, statement.order_by);
    for (orders) |*order| {
        if (order.position != null) continue;
        const input = order.expression orelse blk: {
            const node = try alloc.create(ast.Scalar);
            node.* = .{ .column = order.field };
            break :blk node;
        };
        order.expression = try builder.walk(input, 0);
    }
    out.order_by = orders;
    out.order_aliases_expanded = true;
    return out;
}

const Builder = struct {
    alloc: std.mem.Allocator,
    aliases: std.StringHashMapUnmanaged(?ast.Projection) = .empty,
    remaining: usize = 4096,
    fn walk(self: *Builder, input: *const ast.Scalar, depth: usize) anyerror!*const ast.Scalar {
        if (depth >= 128 or self.remaining == 0) return error.SqlProgramLimitExceeded;
        self.remaining -= 1;
        if (input.* == .column) {
            // The parser encodes qualification with NUL, not a literal dot.
            // A quoted output label such as "n.total" remains unqualified.
            if (std.mem.indexOfScalar(u8, input.column, 0) != null) return input;
            if (self.aliases.get(input.column)) |match| {
                const projection = match orelse return error.AmbiguousSqlColumn;
                // Never expand recursively into the projection's source domain.
                if (projection.expression) |expression| return expression;
                const node = try self.alloc.create(ast.Scalar);
                node.* = .{ .column = projection.field };
                return node;
            }
            return input;
        }
        const value: ast.Scalar = switch (input.*) {
            .literal => return input,
            .unary => |part| .{ .unary = .{ .op = part.op, .operand = try self.walk(part.operand, depth + 1) } },
            .binary => |part| .{ .binary = .{ .op = part.op, .left = try self.walk(part.left, depth + 1), .right = try self.walk(part.right, depth + 1) } },
            .cast => |part| .{ .cast = .{ .type = part.type, .operand = try self.walk(part.operand, depth + 1) } },
            .call => |part| blk: {
                // Window arguments, FILTER and sort keys see source columns only.
                if (part.window != null) return input;
                var copy = part;
                const args = try self.alloc.alloc(*const ast.Scalar, part.args.len);
                for (part.args, args) |arg, *out| out.* = try self.walk(arg, depth + 1);
                copy.args = args;
                copy.filter = if (part.filter) |filter| try self.walk(filter, depth + 1) else null;
                break :blk .{ .call = copy };
            },
            .case_when => |part| blk: {
                const branches = try self.alloc.alloc(ast.Scalar.Branch, part.branches.len);
                for (part.branches, branches) |branch, *out| out.* = .{ .condition = try self.walk(branch.condition, depth + 1), .value = try self.walk(branch.value, depth + 1) };
                break :blk .{ .case_when = .{ .branches = branches, .otherwise = if (part.otherwise) |other| try self.walk(other, depth + 1) else null } };
            },
            .in_list => |part| blk: {
                const values = try self.alloc.alloc(*const ast.Scalar, part.values.len);
                for (part.values, values) |item, *out| out.* = try self.walk(item, depth + 1);
                break :blk .{ .in_list = .{ .operand = try self.walk(part.operand, depth + 1), .values = values, .negated = part.negated } };
            },
            .column => unreachable,
        };
        const out = try self.alloc.create(ast.Scalar);
        out.* = value;
        return out;
    }
};
