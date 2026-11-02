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

pub fn returningReadsColumn(projections: ?[]const ast.Projection) bool {
    const items = projections orelse return false;
    // The parser's compact spelling for plain RETURNING * is a present,
    // empty projection list. It reads stored columns, unlike RETURNING 1.
    if (items.len == 0) return true;
    for (items) |projection| {
        if (projection.wildcard or projection.field.len != 0) return true;
        if (projection.expression) |expression| if (scalarReadsColumn(expression)) return true;
    }
    return false;
}

fn sourceSelectRequired(source: ?*const ast.Relation) bool {
    const relation = source orelse return false;
    // DEFAULT/subquery lowering may synthesize the target's preimage source.
    // That scan retains the command's authority; it is not a user FROM/USING
    // reference. Explicit sources still resolve through their read authority.
    return relation.* != .table or !relation.table.mutation_target;
}

pub fn selectRequired(statement: ast.Statement) bool {
    return switch (statement) {
        .insert => |insert| returningReadsColumn(insert.returning) or if (insert.conflict) |conflict| conflict.columns.len != 0 or conflict.constraint_name != null or conflict.expressions.len != 0 or conflict.arbiter_predicate != null or conflict.assignments.len != 0 or conflict.predicate != null else false,
        .update => |update| blk: {
            if (returningReadsColumn(update.returning) or sourceSelectRequired(update.source) or predicateReadsColumn(update.predicate)) break :blk true;
            for (update.assignments) |assignment| if (assignment.expression) |expression| if (scalarReadsColumn(expression)) break :blk true;
            break :blk false;
        },
        .delete => |delete| returningReadsColumn(delete.returning) or sourceSelectRequired(delete.source) or predicateReadsColumn(delete.predicate),
        else => true,
    };
}

pub fn requiredPrivileges(statement: ast.Statement) u16 {
    const access = @import("../system_catalog/privileges.zig").Access;
    const select = if (selectRequired(statement)) access.bit(.select) else @as(u16, 0);
    return switch (statement) {
        .insert => |insert| access.bit(.insert) | select | if (insert.conflict) |conflict| (if (conflict.assignments.len != 0) access.bit(.update) else @as(u16, 0)) else @as(u16, 0),
        .update => access.bit(.update) | select,
        .delete => access.bit(.delete) | select,
        .merge => |merge| blk: {
            var required = access.bit(.select);
            for (merge.arms) |arm| required |= switch (arm.action) {
                .update => access.bit(.update),
                .delete => access.bit(.delete),
                .insert => access.bit(.insert),
                .nothing => 0,
            };
            break :blk required;
        },
        else => access.bit(.select),
    };
}

pub fn bindPrivileges(table: *@import("catalog.zig").Table, statement: ast.Statement) !void {
    table.sql_required_privileges = requiredPrivileges(statement);
    if (table.sql_access) |access| {
        if ((access.rights | table.sql_native_rights) & table.sql_required_privileges != table.sql_required_privileges) return error.Forbidden;
    }
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
        .{ .sql = "INSERT INTO items(id) VALUES('a') RETURNING *", .selected = true },
        .{ .sql = "INSERT INTO items(id) VALUES('a') RETURNING items.*", .selected = true },
        .{ .sql = "UPDATE items SET status = 'active' RETURNING *", .selected = true },
        .{ .sql = "DELETE FROM items RETURNING *", .selected = true },
        .{ .sql = "DELETE FROM items", .selected = false },
        .{ .sql = "DELETE FROM items WHERE id = 'a'", .selected = true },
    }) |case| {
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        try std.testing.expectEqual(case.selected, selectRequired(compiled.statement));
    }
}

test "SQL privileges mutation intent separates insert update delete conflict and literal returning" {
    const std = @import("std");
    const compiler = @import("compiler.zig");
    const bits = @import("../system_catalog/privileges.zig").Access;
    const catalog = @import("catalog.zig");
    for ([_]struct { sql: []const u8, required: u16 }{
        .{ .sql = "INSERT INTO t(id,v) VALUES(2,2) RETURNING 1", .required = bits.bit(.insert) },
        .{ .sql = "INSERT INTO t(id,v) VALUES(1,2) ON CONFLICT DO NOTHING", .required = bits.bit(.insert) },
        .{ .sql = "INSERT INTO t(id,v) VALUES(1,2) ON CONFLICT(id) DO NOTHING", .required = bits.bit(.insert) | bits.bit(.select) },
        .{ .sql = "INSERT INTO t(id,v) VALUES(1,2) ON CONFLICT(id) DO UPDATE SET v=3", .required = bits.bit(.insert) | bits.bit(.select) | bits.bit(.update) },
        .{ .sql = "UPDATE t SET v=4 RETURNING 1", .required = bits.bit(.update) },
        .{ .sql = "DELETE FROM t WHERE false RETURNING 1", .required = bits.bit(.delete) },
        .{ .sql = "DELETE FROM t WHERE id=1", .required = bits.bit(.delete) | bits.bit(.select) },
    }) |example| {
        var compiled = try compiler.compile(std.testing.allocator, example.sql, .{});
        defer compiled.deinit();
        try std.testing.expectEqual(example.required, requiredPrivileges(compiled.statement));
        var table: catalog.Table = .{ .id = 1, .physical_name = "t", .schema_version = 1, .columns = &.{}, .sql_access = .{ .rights = bits.bit(.insert) } };
        if (example.required == bits.bit(.insert)) {
            try bindPrivileges(&table, compiled.statement);
        } else try std.testing.expectError(error.Forbidden, bindPrivileges(&table, compiled.statement));
    }
}

test "SQL privileges lowered target preimages retain command authority without adding SELECT" {
    const std = @import("std");
    const compiler = @import("compiler.zig");
    const access = @import("../system_catalog/privileges.zig").Access;
    for ([_]struct { sql: []const u8, selected: bool }{
        .{ .sql = "UPDATE t SET v=DEFAULT RETURNING 1", .selected = false },
        .{ .sql = "UPDATE t SET v=DEFAULT WHERE id=1 RETURNING 1", .selected = true },
        .{ .sql = "UPDATE t SET v=DEFAULT RETURNING v", .selected = true },
        .{ .sql = "DELETE FROM t WHERE false RETURNING 1", .selected = false },
        .{ .sql = "DELETE FROM t RETURNING id", .selected = true },
    }) |input| {
        var compiled = try compiler.compile(std.testing.allocator, input.sql, .{});
        defer compiled.deinit();
        const before = requiredPrivileges(compiled.statement);
        const purpose = scanPurpose(compiled.statement);
        var target: ast.Relation = .{ .table = .{ .name = .{ .table = "t" }, .mutation_target = true } };
        switch (compiled.statement) {
            .update => |*update| update.source = &target,
            .delete => |*deletion| deletion.source = &target,
            else => unreachable,
        }
        try std.testing.expectEqual(input.selected, selectRequired(compiled.statement));
        try std.testing.expectEqual(before, requiredPrivileges(compiled.statement));
        try std.testing.expectEqual(purpose, scanPurpose(compiled.statement));
        try std.testing.expectEqual(input.selected, before & access.bit(.select) != 0);
        target.table.mutation_target = false;
        try std.testing.expect(selectRequired(compiled.statement));
    }
}
