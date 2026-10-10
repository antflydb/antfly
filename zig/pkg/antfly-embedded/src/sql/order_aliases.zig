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
        order.expression = try builder.resolve(input);
    }
    out.order_by = orders;
    out.order_aliases_expanded = true;
    return out;
}

const Builder = struct {
    alloc: std.mem.Allocator,
    aliases: std.StringHashMapUnmanaged(?ast.Projection) = .empty,
    remaining: usize = 4096,
    fn resolve(self: *Builder, input: *const ast.Scalar) !*const ast.Scalar {
        if (self.remaining == 0) return error.SqlProgramLimitExceeded;
        self.remaining -= 1;
        // PostgreSQL permits an output label only as the complete sort key.
        // Arithmetic, casts, calls and predicates bind in the input domain;
        // never traverse them looking for labels to substitute.
        if (input.* != .column) return input;
        // Qualification uses NUL, so quoted labels containing dots are bare.
        if (std.mem.indexOfScalar(u8, input.column, 0) != null) return input;
        if (self.aliases.get(input.column)) |match| {
            const projection = match orelse return error.AmbiguousSqlColumn;
            if (projection.expression) |expression| return expression;
            const node = try self.alloc.create(ast.Scalar);
            node.* = .{ .column = projection.field };
            return node;
        }
        return input;
    }
};
