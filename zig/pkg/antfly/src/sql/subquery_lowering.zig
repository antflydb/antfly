// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Equality decorrelation. Every inner relation is evaluated once and joined
//! through grouped keys; there is no per-outer-row backend execution.
const std = @import("std");
const ast = @import("ast.zig");
const Allocator = std.mem.Allocator;

fn has(node: *const ast.Scalar) bool {
    return switch (node.*) {
        .call => |part| blk: {
            if (part.subquery != null) break :blk true;
            for (part.args) |arg| if (has(arg)) break :blk true;
            break :blk if (part.filter) |filter| has(filter) else false;
        },
        .unary => |part| has(part.operand),
        .binary => |part| has(part.left) or has(part.right),
        .cast => |part| has(part.operand),
        .case_when => |part| blk: {
            for (part.branches) |branch| if (has(branch.condition) or has(branch.value)) break :blk true;
            break :blk if (part.otherwise) |other| has(other) else false;
        },
        .in_list => |part| blk: {
            if (has(part.operand)) break :blk true;
            for (part.values) |value| if (has(value)) break :blk true;
            break :blk false;
        },
        else => false,
    };
}
fn predicateHas(predicate: *const ast.Predicate) bool {
    return switch (predicate.*) {
        .scalar => |value| has(value),
        .conjunction, .disjunction => |part| predicateHas(part.left) or predicateHas(part.right),
        .negation => |part| predicateHas(part),
        else => false,
    };
}
pub fn accepts(statement: ast.Select) bool {
    for (statement.columns) |column| if (column.expression) |value| if (has(value)) return true;
    if (statement.predicate) |predicate| if (predicateHas(predicate)) return true;
    for (statement.group_by) |value| if (has(value)) return true;
    if (statement.having) |value| if (has(value)) return true;
    for (statement.order_by) |order| if (order.expression) |value| if (has(value)) return true;
    return false;
}
const Names = std.StringHashMapUnmanaged(void);
fn aliases(alloc: Allocator, relation: *const ast.Relation, names: *Names) !void {
    switch (relation.*) {
        .table => |table| try names.put(alloc, table.alias orelse table.name.table, {}),
        .derived => |query| try names.put(alloc, query.alias, {}),
        .join => |join| {
            try aliases(alloc, join.left, names);
            try aliases(alloc, join.right, names);
        },
    }
}
const Key = struct { inner: *const ast.Scalar, outer: *const ast.Scalar };
const Builder = struct {
    alloc: Allocator,
    source: *const ast.Relation,
    outer: Names = .empty,
    serial: usize = 0,
    fn scalar(self: *Builder, value: ast.Scalar) !*const ast.Scalar {
        const out = try self.alloc.create(ast.Scalar);
        out.* = value;
        return out;
    }
    fn relation(self: *Builder, value: ast.Relation) !*const ast.Relation {
        const out = try self.alloc.create(ast.Relation);
        out.* = value;
        return out;
    }
    fn field(self: *Builder, qualifier: []const u8, name: []const u8) !*const ast.Scalar {
        return self.scalar(.{ .column = try std.fmt.allocPrint(self.alloc, "{s}\x00{s}", .{ qualifier, name }) });
    }
    fn call(self: *Builder, name: []const u8, args: []const *const ast.Scalar) !*const ast.Scalar {
        return self.scalar(.{ .call = .{ .name = name, .args = try self.alloc.dupe(*const ast.Scalar, args) } });
    }
    fn outerRef(self: *Builder, value: *const ast.Scalar, local: Names) bool {
        if (value.* != .column) return false;
        const separator = std.mem.indexOfScalar(u8, value.column, 0) orelse return false;
        const qualifier = value.column[0..separator];
        return !local.contains(qualifier) and self.outer.contains(qualifier);
    }
    fn referencesOuter(self: *Builder, value: *const ast.Scalar, local: Names) bool {
        if (self.outerRef(value, local)) return true;
        return switch (value.*) {
            .call => |part| blk: {
                if (part.subquery != null) break :blk true;
                for (part.args) |arg| if (self.referencesOuter(arg, local)) break :blk true;
                break :blk if (part.filter) |filter| self.referencesOuter(filter, local) else false;
            },
            .unary => |part| self.referencesOuter(part.operand, local),
            .binary => |part| self.referencesOuter(part.left, local) or self.referencesOuter(part.right, local),
            .cast => |part| self.referencesOuter(part.operand, local),
            .case_when => |part| blk: {
                for (part.branches) |branch| if (self.referencesOuter(branch.condition, local) or self.referencesOuter(branch.value, local)) break :blk true;
                break :blk if (part.otherwise) |other| self.referencesOuter(other, local) else false;
            },
            .in_list => |part| blk: {
                if (self.referencesOuter(part.operand, local)) break :blk true;
                for (part.values) |item| if (self.referencesOuter(item, local)) break :blk true;
                break :blk false;
            },
            else => false,
        };
    }
    fn predicateScalar(self: *Builder, input: *const ast.Predicate) anyerror!*const ast.Scalar {
        return switch (input.*) {
            .scalar => |value| value,
            .comparison => |part| self.scalar(.{ .binary = .{ .op = @enumFromInt(@intFromEnum(ast.Scalar.Binary.eq) + @intFromEnum(part.op)), .left = try self.scalar(.{ .column = part.field }), .right = try self.scalar(.{ .literal = part.value }) } }),
            .is_null => |part| self.scalar(.{ .unary = .{ .op = if (part.negated) .is_not_null else .is_null, .operand = try self.scalar(.{ .column = part.field }) } }),
            .negation => |part| self.scalar(.{ .unary = .{ .op = .not, .operand = try self.predicateScalar(part) } }),
            .conjunction, .disjunction => |part| self.scalar(.{ .binary = .{ .op = if (input.* == .conjunction) .@"and" else .@"or", .left = try self.predicateScalar(part.left), .right = try self.predicateScalar(part.right) } }),
        };
    }
    fn extract(self: *Builder, value: *const ast.Scalar, local: Names, keys: *std.ArrayList(Key)) anyerror!?*const ast.Scalar {
        if (value.* == .binary and value.binary.op == .@"and") {
            const left = try self.extract(value.binary.left, local, keys);
            const right = try self.extract(value.binary.right, local, keys);
            return if (left != null and right != null) try self.scalar(.{ .binary = .{ .op = .@"and", .left = left.?, .right = right.? } }) else left orelse right;
        }
        if (value.* == .binary and value.binary.op == .eq) {
            const part = value.binary;
            if (self.outerRef(part.left, local) and !self.referencesOuter(part.right, local)) {
                try keys.append(self.alloc, .{ .inner = part.right, .outer = part.left });
                return null;
            }
            if (self.outerRef(part.right, local) and !self.referencesOuter(part.left, local)) {
                try keys.append(self.alloc, .{ .inner = part.left, .outer = part.right });
                return null;
            }
        }
        if (self.referencesOuter(value, local)) return error.UnsupportedSqlShape;
        return value;
    }
    fn subquery(self: *Builder, expression: *const ast.Scalar) anyerror!*const ast.Scalar {
        const original = expression.call.subquery.?;
        const exists = std.mem.eql(u8, expression.call.name, "$exists");
        if (exists and (original.count_all or @import("aggregate_binding.zig").accepts(original.*))) return error.UnsupportedSqlShape;
        if (original.set_operation != null or original.ctes.len != 0 or original.order_by.len != 0 or original.limit != null or original.offset != null or original.group_by.len != 0 or original.having != null or @import("window_binding.zig").accepts(original.*)) return error.UnsupportedSqlShape;
        if (!exists and original.columns.len != 1 and !original.count_all) return error.InvalidSqlParameters;
        var local: Names = .empty;
        if (original.source) |source| try aliases(self.alloc, source, &local) else if (original.table) |table| try local.put(self.alloc, table.table, {});
        var keys: std.ArrayList(Key) = .empty;
        const residual = if (original.predicate) |predicate| try self.extract(try self.predicateScalar(predicate), local, &keys) else null;
        var query = original.*;
        query.order_by = &.{};
        query.count_all = false;
        query.count_alias = null;
        query.predicate = if (residual) |value| blk: {
            const out = try self.alloc.create(ast.Predicate);
            out.* = .{ .scalar = value };
            break :blk out;
        } else null;
        // Binding must still reject undefined projection names in EXISTS, but
        // the projection's value is not evaluated. Simple field probes use
        // COUNT(field); expressions needing side-effect-free pruning are not
        // accepted until the binder has a validation-only expression domain.
        const probes = if (exists) original.columns.len else 0;
        const columns = try self.alloc.alloc(ast.Projection, keys.items.len + 2 + probes);
        const groups = try self.alloc.alloc(*const ast.Scalar, keys.items.len);
        const alias = try std.fmt.allocPrint(self.alloc, "$subquery_{d}", .{self.serial});
        self.serial += 1;
        if (self.serial > 64 or self.outer.contains(alias)) return error.SqlProgramLimitExceeded;
        try self.outer.put(self.alloc, alias, {});
        var condition: ?*const ast.Scalar = null;
        for (keys.items, columns[0..keys.items.len], groups, 0..) |key, *column, *group, index| {
            const name = try std.fmt.allocPrint(self.alloc, "$key_{d}", .{index});
            column.* = .{ .alias = name, .expression = key.inner };
            group.* = key.inner;
            const equal = try self.scalar(.{ .binary = .{ .op = .eq, .left = key.outer, .right = try self.field(alias, name) } });
            condition = if (condition) |prior| try self.scalar(.{ .binary = .{ .op = .@"and", .left = prior, .right = equal } }) else equal;
        }
        const count = try self.scalar(.{ .call = .{ .name = "count", .args = &.{}, .star = true } });
        columns[keys.items.len] = .{ .alias = "$count", .expression = count };
        var value: *const ast.Scalar = try self.scalar(.{ .literal = .{ .integer = 1 } });
        var aggregate = false;
        if (!exists) {
            value = if (original.count_all) count else original.columns[0].expression orelse try self.scalar(.{ .column = original.columns[0].field });
            if (self.referencesOuter(value, local)) return error.UnsupportedSqlShape;
            aggregate = @import("aggregate_binding.zig").contains(value);
            if (aggregate and (value.* != .call or @import("aggregate_binding.zig").aggregateKind(value.call.name) == null)) return error.UnsupportedSqlShape;
            if (!aggregate) value = try self.call("min", &.{value});
        }
        columns[keys.items.len + 1] = .{ .alias = "$value", .expression = value };
        for (columns[keys.items.len + 2 ..], 0..) |*column, index| {
            const projection = original.columns[index];
            const expression_ = projection.expression orelse try self.scalar(.{ .column = projection.field });
            if (expression_.* != .literal and expression_.* != .column) return error.UnsupportedSqlShape;
            if (self.referencesOuter(expression_, local)) return error.UnsupportedSqlShape;
            column.* = .{ .alias = try std.fmt.allocPrint(self.alloc, "$probe_{d}", .{index}), .expression = try self.call("count", &.{expression_}) };
        }
        query.columns = columns;
        query.group_by = groups;
        const owned = try self.alloc.create(ast.Select);
        owned.* = query;
        self.source = try self.relation(.{ .join = .{ .kind = .left, .left = self.source, .right = try self.relation(.{ .derived = .{ .query = owned, .alias = alias, .hidden = true } }), .condition = condition } });
        const observed = try self.field(alias, "$count");
        if (exists) return self.scalar(.{ .binary = .{ .op = .gt, .left = try self.call("coalesce", &.{ observed, try self.scalar(.{ .literal = .{ .integer = 0 } }) }), .right = try self.scalar(.{ .literal = .{ .integer = 0 } }) } });
        const result = try self.field(alias, "$value");
        if (aggregate) {
            if (original.count_all or (value.* == .call and std.mem.eql(u8, value.call.name, "count"))) return self.call("coalesce", &.{ result, try self.scalar(.{ .literal = .{ .integer = 0 } }) });
            return result;
        }
        return self.call("$single", &.{ result, observed });
    }
    fn rewrite(self: *Builder, input: *const ast.Scalar) anyerror!*const ast.Scalar {
        if (!has(input)) return input;
        if (input.* == .call and input.call.subquery != null) return self.subquery(input);
        // Hoisting a subquery out of a lazy branch would evaluate its data
        // expressions even when SQL never demands that branch. A future
        // conditional Apply node must own those evaluation masks explicitly.
        if (input.* == .case_when or (input.* == .call and std.mem.eql(u8, input.call.name, "coalesce"))) return error.UnsupportedSqlShape;
        return self.scalar(switch (input.*) {
            .call => |part| blk: {
                var copy = part;
                const args = try self.alloc.alloc(*const ast.Scalar, part.args.len);
                for (part.args, args) |arg, *out| out.* = try self.rewrite(arg);
                copy.args = args;
                copy.filter = if (part.filter) |filter| try self.rewrite(filter) else null;
                break :blk .{ .call = copy };
            },
            .unary => |part| .{ .unary = .{ .op = part.op, .operand = try self.rewrite(part.operand) } },
            .binary => |part| .{ .binary = .{ .op = part.op, .left = try self.rewrite(part.left), .right = try self.rewrite(part.right) } },
            .cast => |part| .{ .cast = .{ .type = part.type, .operand = try self.rewrite(part.operand) } },
            .case_when => |part| blk: {
                const branches = try self.alloc.alloc(ast.Scalar.Branch, part.branches.len);
                for (part.branches, branches) |branch, *out| out.* = .{ .condition = try self.rewrite(branch.condition), .value = try self.rewrite(branch.value) };
                break :blk .{ .case_when = .{ .branches = branches, .otherwise = if (part.otherwise) |other| try self.rewrite(other) else null } };
            },
            .in_list => |part| blk: {
                const values = try self.alloc.alloc(*const ast.Scalar, part.values.len);
                for (part.values, values) |item, *out| out.* = try self.rewrite(item);
                break :blk .{ .in_list = .{ .operand = try self.rewrite(part.operand), .values = values, .negated = part.negated } };
            },
            else => unreachable,
        });
    }
};
pub fn lower(alloc: Allocator, statement: ast.Select) !ast.Select {
    var builder: Builder = .{ .alloc = alloc, .source = undefined };
    if (statement.source) |source| builder.source = source else if (statement.table) |table| builder.source = try builder.relation(.{ .table = .{ .name = table } }) else {
        const singleton = try alloc.create(ast.Select);
        singleton.* = .{ .columns = try alloc.dupe(ast.Projection, &.{.{ .expression = try builder.scalar(.{ .literal = .{ .integer = 1 } }) }}) };
        builder.source = try builder.relation(.{ .derived = .{ .query = singleton, .alias = "$singleton", .hidden = true } });
    }
    try aliases(alloc, builder.source, &builder.outer);
    var result = statement;
    const columns = try alloc.dupe(ast.Projection, statement.columns);
    for (columns) |*column| if (column.expression) |value| {
        if (column.alias == null and has(value)) column.alias = if (value.* == .call and value.call.subquery != null and std.mem.eql(u8, value.call.name, "$exists")) "exists" else "?column?";
        column.expression = try builder.rewrite(value);
    };
    result.columns = columns;
    if (statement.predicate) |predicate| {
        const out = try alloc.create(ast.Predicate);
        out.* = .{ .scalar = try builder.rewrite(try builder.predicateScalar(predicate)) };
        result.predicate = out;
    }
    const groups = try alloc.alloc(*const ast.Scalar, statement.group_by.len);
    for (statement.group_by, groups) |value, *out| out.* = try builder.rewrite(value);
    result.group_by = groups;
    result.having = if (statement.having) |value| try builder.rewrite(value) else null;
    const orders = try alloc.dupe(ast.Order, statement.order_by);
    for (orders) |*order| if (order.expression) |value| {
        order.expression = try builder.rewrite(value);
    };
    result.order_by = orders;
    result.source = builder.source;
    result.table = null;
    return result;
}
