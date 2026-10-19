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

//! SQL statement intent. These request-owned values never enter a shared
//! schema/prepared binding: one table may occur in different command roles.
const ast = @import("ast.zig");
const authority = @import("../usermgr/row_policy_authority.zig");

pub fn scalarReadsColumn(value: *const ast.Scalar) bool {
    return switch (value.*) {
        .literal => false,
        .column => true,
        .unary => |node| scalarReadsColumn(node.operand),
        .cast => |node| scalarReadsColumn(node.operand),
        .binary => |node| scalarReadsColumn(node.left) or scalarReadsColumn(node.right),
        .call => |node| blk: {
            for (node.args) |arg| if (scalarReadsColumn(arg)) break :blk true;
            if (node.filter) |filter| if (scalarReadsColumn(filter)) break :blk true;
            // Correlated subqueries and mutation sources need their binding
            // domain retained; conservatively keep read authority until then.
            break :blk node.subquery != null or node.window != null;
        },
        .case_when => |node| blk: {
            for (node.branches) |branch| if (scalarReadsColumn(branch.condition) or scalarReadsColumn(branch.value)) break :blk true;
            break :blk if (node.otherwise) |other| scalarReadsColumn(other) else false;
        },
        .in_list => |node| blk: {
            if (scalarReadsColumn(node.operand)) break :blk true;
            for (node.values) |item| if (scalarReadsColumn(item)) break :blk true;
            break :blk false;
        },
    };
}

fn predicateReadsColumn(value: ?*const ast.Predicate) bool {
    const node = value orelse return false;
    return switch (node.*) {
        .comparison, .is_null => true,
        .scalar => |scalar| scalarReadsColumn(scalar),
        .conjunction, .disjunction => |binary| predicateReadsColumn(binary.left) or predicateReadsColumn(binary.right),
        .negation => |inner| predicateReadsColumn(inner),
    };
}

pub fn selectRequired(statement: ast.Statement) bool {
    return switch (statement) {
        .insert => |insert| insert.returning != null or insert.conflict != null,
        .update => |update| blk: {
            if (update.returning != null or update.source != null or predicateReadsColumn(update.predicate)) break :blk true;
            for (update.assignments) |assignment| if (assignment.expression) |expression| if (scalarReadsColumn(expression)) break :blk true;
            break :blk false;
        },
        .delete => |delete| delete.returning != null or delete.source != null or predicateReadsColumn(delete.predicate),
        else => true,
    };
}

pub fn scanPurpose(statement: ast.Statement) authority.Purpose {
    return switch (statement) {
        .update => if (selectRequired(statement)) .update_select else .update,
        .delete => if (selectRequired(statement)) .delete_select else .delete,
        else => .ordinary,
    };
}

test "constant write-only mutations do not acquire SELECT authority" {
    const std = @import("std");
    const compiler = @import("compiler.zig");
    for ([_]struct { sql: []const u8, selected: bool }{
        .{ .sql = "UPDATE items SET status = 'active'", .selected = false },
        .{ .sql = "UPDATE items SET status = upper('active') WHERE true", .selected = false },
        .{ .sql = "UPDATE items SET status = upper(status)", .selected = true },
        .{ .sql = "UPDATE items SET status = 'active' WHERE id = 'a'", .selected = true },
        .{ .sql = "UPDATE items SET status = 'active' RETURNING status", .selected = true },
        .{ .sql = "DELETE FROM items", .selected = false },
        .{ .sql = "DELETE FROM items WHERE id = 'a'", .selected = true },
    }) |case| {
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        try std.testing.expectEqual(case.selected, selectRequired(compiled.statement));
    }
}
