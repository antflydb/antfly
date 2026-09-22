// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Schema-bound logical relations. Physical scans are flattened so execution
//! can pin the complete read set before consuming any row.
const std = @import("std");
const ast = @import("ast.zig");
const catalog = @import("catalog.zig");
const scalar = @import("scalar.zig");
const describe = @import("describe.zig");
const compiler = @import("compiler.zig");
const Allocator = std.mem.Allocator;

pub const Column = struct {
    name: []const u8,
    internal: []const u8,
    qualifier: []const u8,
    type: ast.ColumnType,
    nullable: bool,
    visible: bool = true,
    untyped_null: bool = false,
    /// Symbolic lineage exists only during the pre-emission constraint pass.
    origin: ?*const ast.Scalar = null,
};
pub const Node = struct {
    columns: []const Column,
    operation: union(enum) {
        singleton,
        scan: struct { index: usize, source_columns: []const []const u8 },
        join: struct { kind: ast.JoinKind, left: *const Node, right: *const Node, condition: ?scalar.Program, left_keys: []const scalar.Program, right_keys: []const scalar.Program },
        query: struct { source: *const Node, statement: ast.Select, binding: describe.BoundStatement },
        set: struct { kind: ast.SetKind, all: bool, left: *const Node, right: *const Node },
    },
};
pub const Bound = struct { root: *const Node, scans: []const catalog.StatementScan, table: catalog.Table, statement: ast.Select };

fn qualified(node: *const ast.Scalar) bool {
    return switch (node.*) {
        .column => |name| std.mem.indexOfScalar(u8, name, 0) != null,
        .literal => false,
        .unary => |part| qualified(part.operand),
        .binary => |part| qualified(part.left) or qualified(part.right),
        .cast => |part| qualified(part.operand),
        .call => |part| blk: {
            for (part.args) |arg| if (qualified(arg)) break :blk true;
            break :blk if (part.filter) |filter| qualified(filter) else false;
        },
        .case_when => |part| blk: {
            for (part.branches) |branch| if (qualified(branch.condition) or qualified(branch.value)) break :blk true;
            break :blk if (part.otherwise) |other| qualified(other) else false;
        },
        .in_list => |part| blk: {
            if (qualified(part.operand)) break :blk true;
            for (part.values) |value| if (qualified(value)) break :blk true;
            break :blk false;
        },
    };
}
fn qualifiedPredicate(node: *const ast.Predicate) bool {
    return switch (node.*) {
        .comparison => |part| std.mem.indexOfScalar(u8, part.field, 0) != null,
        .is_null => |part| std.mem.indexOfScalar(u8, part.field, 0) != null,
        .scalar => |part| qualified(part),
        .negation => |part| qualifiedPredicate(part),
        .conjunction, .disjunction => |part| qualifiedPredicate(part.left) or qualifiedPredicate(part.right),
    };
}
pub fn accepts(statement: ast.Select) bool {
    if (statement.source != null or statement.ctes.len != 0 or statement.set_operation != null) return true;
    for (statement.columns) |projection| {
        if (std.mem.indexOfScalar(u8, projection.field, 0) != null) return true;
        if (projection.expression) |node| if (qualified(node)) return true;
    }
    if (statement.predicate) |node| if (qualifiedPredicate(node)) return true;
    for (statement.group_by) |node| if (qualified(node)) return true;
    if (statement.having) |node| if (qualified(node)) return true;
    for (statement.order_by) |order| {
        if (std.mem.indexOfScalar(u8, order.field, 0) != null) return true;
        if (order.expression) |node| if (qualified(node)) return true;
    }
    return false;
}

pub const ResolveAdapter = struct {
    backend: catalog.Backend,
    table: catalog.Table,
    pub fn iface(self: *ResolveAdapter) catalog.Backend {
        return .{ .ptr = self, .vtable = &.{ .resolve = resolve, .scan = scan, .mutate = mutate, .checkpoint = checkpoint } };
    }
    fn resolve(ptr: *anyopaque, _: Allocator, _: ast.Name, action: catalog.Action) !catalog.Table {
        if (action != .read) return error.UnsupportedSqlExecution;
        const self: *ResolveAdapter = @ptrCast(@alignCast(ptr));
        return self.table;
    }
    fn scan(_: *anyopaque, _: Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
        return error.UnsupportedSqlExecution;
    }
    fn mutate(_: *anyopaque, _: Allocator, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
        return error.UnsupportedSqlExecution;
    }
    fn checkpoint(ptr: *anyopaque) !void {
        const self: *ResolveAdapter = @ptrCast(@alignCast(ptr));
        return self.backend.vtable.checkpoint(self.backend.ptr);
    }
};

const Builder = struct {
    alloc: Allocator,
    backend: catalog.Backend,
    parameters: []?ast.ColumnType,
    scans: std.ArrayList(catalog.StatementScan) = .empty,
    identities: std.StringHashMapUnmanaged(catalog.Table) = .empty,
    next_column: usize = 0,
    nodes: usize = 0,
    prepared: std.AutoHashMapUnmanaged(*const ast.Select, Prepared) = .empty,
    inferred_sets: std.AutoHashMapUnmanaged(*const ast.Select, void) = .empty,
    shape_only: bool = false,
    shape_expression_nodes: usize = 0,
    shape_columns: std.ArrayList(scalar.Column) = .empty,
    constraints: std.ArrayList(Constraint) = .empty,
    const Constraint = struct { expression: *const ast.Scalar, expected: ?ast.ColumnType = null };

    const Prepared = struct { source: *const Node, lowered: ast.Select, expressions: []const *const ast.Scalar, types: []?ast.ColumnType };

    fn inferenceExpression(self: *Builder, expression_: *const ast.Scalar, columns: []const Column) anyerror!*const ast.Scalar {
        if (expression_.* == .column) for (columns) |column| {
            if (std.mem.eql(u8, column.internal, expression_.column)) if (column.origin) |origin| return origin;
        };
        return self.scalarNode(switch (expression_.*) {
            .column => |name| blk: {
                for (columns) |column| if (column.untyped_null and std.mem.eql(u8, column.internal, name)) break :blk .{ .literal = .null };
                break :blk expression_.*;
            },
            .call => |call| blk: {
                if (@import("aggregate_binding.zig").aggregateKind(call.name)) |kind| {
                    if (self.shape_only) if (call.filter) |filter| try self.constraints.append(self.alloc, .{ .expression = try self.inferenceExpression(filter, columns), .expected = .boolean });
                    if (kind == .count) break :blk .{ .literal = .{ .integer = 0 } };
                    if (call.args.len != 1) return error.InvalidSqlParameters;
                    const argument = try self.inferenceExpression(call.args[0], columns);
                    break :blk switch (kind) {
                        .avg => .{ .cast = .{ .operand = argument, .type = .number } },
                        .bool_and, .bool_or => .{ .cast = .{ .operand = argument, .type = .boolean } },
                        else => argument.*,
                    };
                }
                var copy = call;
                const args = try self.alloc.alloc(*const ast.Scalar, call.args.len);
                for (call.args, args) |arg, *out| out.* = try self.inferenceExpression(arg, columns);
                copy.args = args;
                break :blk .{ .call = copy };
            },
            .binary => |part| .{ .binary = .{ .op = part.op, .left = try self.inferenceExpression(part.left, columns), .right = try self.inferenceExpression(part.right, columns) } },
            .unary => |part| .{ .unary = .{ .op = part.op, .operand = try self.inferenceExpression(part.operand, columns) } },
            .cast => |part| .{ .cast = .{ .type = part.type, .operand = try self.inferenceExpression(part.operand, columns) } },
            .case_when => |part| blk: {
                const branches = try self.alloc.alloc(ast.Scalar.Branch, part.branches.len);
                for (part.branches, branches) |branch, *out| out.* = .{ .condition = try self.inferenceExpression(branch.condition, columns), .value = try self.inferenceExpression(branch.value, columns) };
                break :blk .{ .case_when = .{ .branches = branches, .otherwise = if (part.otherwise) |other| try self.inferenceExpression(other, columns) else null } };
            },
            .in_list => |part| blk: {
                const values = try self.alloc.alloc(*const ast.Scalar, part.values.len);
                for (part.values, values) |value, *out| out.* = try self.inferenceExpression(value, columns);
                break :blk .{ .in_list = .{ .operand = try self.inferenceExpression(part.operand, columns), .values = values, .negated = part.negated } };
            },
            else => expression_.*,
        });
    }

    fn constrainSelect(self: *Builder, source: *const Node, query: ast.Select) anyerror![]const *const ast.Scalar {
        const expressions = try self.alloc.alloc(*const ast.Scalar, if (query.count_all) 1 else query.columns.len);
        if (query.count_all) {
            expressions[0] = try self.scalarNode(.{ .cast = .{ .operand = try self.scalarNode(.{ .literal = .null }), .type = .integer } });
        } else for (query.columns, expressions) |projection, *out| {
            out.* = try self.inferenceExpression(projection.expression orelse try self.scalarNode(.{ .column = projection.field }), source.columns);
            try self.constraints.append(self.alloc, .{ .expression = out.* });
        }
        if (query.predicate) |predicate_| {
            const expression_ = try @import("bound_scalars.zig").predicateScalar(self.alloc, try self.virtualTable(source.columns), predicate_);
            try self.constraints.append(self.alloc, .{ .expression = try self.inferenceExpression(expression_, source.columns), .expected = .boolean });
        }
        if (query.having) |having| try self.constraints.append(self.alloc, .{ .expression = try self.inferenceExpression(having, source.columns), .expected = .boolean });
        for (query.order_by) |order| if (order.expression) |expression_| try self.constraints.append(self.alloc, .{ .expression = try self.inferenceExpression(expression_, source.columns) });
        for ([_]?ast.Value{ query.limit, query.offset }) |value| if (value) |bound| try self.constraints.append(self.alloc, .{ .expression = try self.scalarNode(.{ .literal = bound }), .expected = .integer });
        return expressions;
    }

    fn inferShape(self: *Builder, statement: ast.Select, expected: []const ast.ColumnType) !void {
        const root = try self.querySource(statement, &.{}, 0);
        const expressions = try self.constrainSelect(root, try self.lower(root, statement));
        if (expected.len != 0) {
            if (expressions.len != expected.len) return error.InvalidSqlParameters;
            for (expressions, expected) |expression_, kind| try self.constraints.append(self.alloc, .{ .expression = expression_, .expected = kind });
        }
        // A slot changes only from unknown to known. This bounds propagation
        // independently of data cardinality and never evaluates a query.
        for (0..self.parameters.len + 1) |_| {
            try self.backend.vtable.checkpoint(self.backend.ptr);
            var changed = false;
            for (self.constraints.items, 0..) |constraint, index| {
                if (index % 64 == 0) try self.backend.vtable.checkpoint(self.backend.ptr);
                changed = try scalar.inferParameters(self.alloc, constraint.expression, self.shape_columns.items, self.parameters, constraint.expected, .{}) or changed;
            }
            if (!changed) return;
        }
        return error.SqlProgramLimitExceeded;
    }

    fn collectSet(self: *Builder, query: *const ast.Select, scope: []const ast.Cte, leaves: *std.ArrayList(*const ast.Select), depth: usize) anyerror!void {
        if (depth > 32) return error.SqlProgramLimitExceeded;
        if (query.set_operation) |set| {
            try self.inferred_sets.put(self.alloc, set.left, {});
            const ctes = try self.alloc.alloc(ast.Cte, scope.len + query.ctes.len);
            @memcpy(ctes[0..scope.len], scope);
            @memcpy(ctes[scope.len..], query.ctes);
            try self.collectSet(set.left, ctes, leaves, depth + 1);
            try self.collectSet(set.right, ctes, leaves, depth + 1);
        } else {
            const source = try self.querySource(query.*, scope, depth + 1);
            const lowered = try self.lower(source, query.*);
            const expressions = try self.alloc.alloc(*const ast.Scalar, if (lowered.count_all) 1 else lowered.columns.len);
            if (lowered.count_all) {
                expressions[0] = try self.scalarNode(.{ .literal = .{ .integer = 0 } });
            } else for (lowered.columns, expressions) |projection, *out| {
                out.* = try self.inferenceExpression(projection.expression orelse try self.scalarNode(.{ .column = projection.field }), source.columns);
            }
            const types = try self.alloc.alloc(?ast.ColumnType, expressions.len);
            @memset(types, null);
            try self.prepared.put(self.alloc, query, .{ .source = source, .lowered = lowered, .expressions = expressions, .types = types });
            try leaves.append(self.alloc, query);
        }
    }

    fn inferSet(self: *Builder, query: ast.Select, scope: []const ast.Cte, depth: usize) anyerror!void {
        if (self.inferred_sets.contains(query.set_operation.?.left)) return;
        var leaves: std.ArrayList(*const ast.Select) = .empty;
        try self.collectSet(&query, scope, &leaves, depth);
        const width = self.prepared.get(leaves.items[0]).?.expressions.len;
        const common = try self.alloc.alloc(?ast.ColumnType, width);
        for (leaves.items) |leaf| if (self.prepared.get(leaf).?.expressions.len != width) return error.SqlTypeMismatch;
        for (0..self.parameters.len + 2) |_| {
            @memset(common, null);
            // All arms contribute before any unknown slot is constrained.
            for (leaves.items) |leaf| {
                const prepared = self.prepared.get(leaf).?;
                const columns = try self.scalarColumns(prepared.source.columns);
                for (prepared.expressions, common) |expression_, *kind| {
                    const inferred = (try scalar.inferOutput(self.alloc, expression_, columns, self.parameters)).kind orelse continue;
                    if (kind.* == null) kind.* = inferred else if (kind.* != inferred) {
                        if ((kind.* == .integer or kind.* == .number) and (inferred == .integer or inferred == .number)) kind.* = .number else return error.SqlTypeMismatch;
                    }
                }
            }
            var changed = false;
            for (leaves.items) |leaf| {
                const prepared = self.prepared.get(leaf).?;
                const columns = try self.scalarColumns(prepared.source.columns);
                if (prepared.lowered.predicate) |predicate_| {
                    const expression_ = try @import("bound_scalars.zig").predicateScalar(self.alloc, try self.virtualTable(prepared.source.columns), predicate_);
                    changed = try scalar.inferParameters(self.alloc, expression_, columns, self.parameters, .boolean, .{}) or changed;
                }
                for (prepared.expressions, common, prepared.types) |expression_, kind, *output| {
                    changed = try scalar.inferParameters(self.alloc, expression_, columns, self.parameters, kind, .{}) or changed;
                    output.* = kind;
                }
            }
            if (!changed) break;
        }
    }

    fn node(self: *Builder, columns: []const Column, operation: @FieldType(Node, "operation")) !*const Node {
        self.nodes += 1;
        if (self.nodes > 256) return error.SqlProgramLimitExceeded;
        const result = try self.alloc.create(Node);
        result.* = .{ .columns = columns, .operation = operation };
        return result;
    }
    fn internal(self: *Builder) ![]const u8 {
        defer self.next_column += 1;
        return std.fmt.allocPrint(self.alloc, "$relation_{d}", .{self.next_column});
    }
    fn virtualTable(self: *Builder, columns: []const Column) !catalog.Table {
        const result = try self.alloc.alloc(catalog.Column, columns.len);
        for (columns, result) |column, *out| out.* = .{ .name = column.internal, .path = column.internal, .type = column.type, .nullable = column.nullable };
        return .{ .id = 0, .physical_name = "$sql_relation", .schema_version = 0, .columns = result };
    }
    fn scalarColumns(self: *Builder, columns: []const Column) ![]const scalar.Column {
        const result = try self.alloc.alloc(scalar.Column, columns.len);
        for (columns, result) |column, *out| out.* = .{ .name = column.internal, .type = column.type, .nullable = column.nullable };
        return result;
    }
    fn field(columns: []const Column, name: []const u8) !Column {
        const separator = std.mem.indexOfScalar(u8, name, 0);
        const unqualified = if (separator) |position| name[position + 1 ..] else name;
        var found: ?Column = null;
        for (columns) |column| {
            if (!std.mem.eql(u8, column.name, unqualified)) continue;
            if (separator) |position| if (!std.mem.eql(u8, column.qualifier, name[0..position])) continue;
            if (found != null) return error.AmbiguousSqlColumn;
            found = column;
        }
        return found orelse error.UndefinedColumn;
    }
    fn scalarNode(self: *Builder, value: ast.Scalar) !*const ast.Scalar {
        if (self.shape_only) {
            if (self.shape_expression_nodes >= 32768) return error.SqlProgramLimitExceeded;
            self.shape_expression_nodes += 1;
        }
        const result = try self.alloc.create(ast.Scalar);
        result.* = value;
        return result;
    }
    fn shapePredicate(self: *Builder, columns: []const Column, input: *const ast.Predicate) anyerror!*const ast.Scalar {
        return switch (input.*) {
            .scalar => |expression_| expression_,
            .comparison => |part| blk: {
                var kind: ?ast.ColumnType = null;
                for (columns) |column| if (std.mem.eql(u8, column.internal, part.field)) {
                    kind = if (column.origin) |origin| (try scalar.inferOutput(self.alloc, origin, self.shape_columns.items, self.parameters)).kind else column.type;
                };
                const literal = try self.scalarNode(.{ .literal = part.value });
                const right = if (kind != null and part.value != .null) try self.scalarNode(.{ .cast = .{ .operand = literal, .type = kind.? } }) else literal;
                break :blk try self.scalarNode(.{ .binary = .{ .op = switch (part.op) {
                    inline else => |tag| @field(ast.Scalar.Binary, @tagName(tag)),
                }, .left = try self.scalarNode(.{ .column = part.field }), .right = right } });
            },
            .is_null => |part| self.scalarNode(.{ .unary = .{ .op = if (part.negated) .is_not_null else .is_null, .operand = try self.scalarNode(.{ .column = part.field }) } }),
            .conjunction, .disjunction => |part| self.scalarNode(.{ .binary = .{ .op = if (input.* == .conjunction) .@"and" else .@"or", .left = try self.shapePredicate(columns, part.left), .right = try self.shapePredicate(columns, part.right) } }),
            .negation => |part| self.scalarNode(.{ .unary = .{ .op = .not, .operand = try self.shapePredicate(columns, part) } }),
        };
    }
    fn expression(self: *Builder, columns: []const Column, input: *const ast.Scalar, aliases: []const ast.Projection) anyerror!*const ast.Scalar {
        return self.scalarNode(switch (input.*) {
            .column => |name| blk: {
                const column = field(columns, name) catch |err| fallback: {
                    if (err == error.UndefinedColumn) for (aliases) |projection| if (projection.alias) |alias| if (std.mem.eql(u8, alias, name)) break :fallback Column{ .name = name, .internal = name, .qualifier = "", .type = .string, .nullable = true };
                    return err;
                };
                break :blk if (column.untyped_null) .{ .literal = .null } else .{ .column = column.internal };
            },
            .literal => input.*,
            .unary => |part| .{ .unary = .{ .op = part.op, .operand = try self.expression(columns, part.operand, aliases) } },
            .binary => |part| .{ .binary = .{ .op = part.op, .left = try self.expression(columns, part.left, aliases), .right = try self.expression(columns, part.right, aliases) } },
            .cast => |part| .{ .cast = .{ .type = part.type, .operand = try self.expression(columns, part.operand, aliases) } },
            .call => |part| blk: {
                const args = try self.alloc.alloc(*const ast.Scalar, part.args.len);
                for (part.args, args) |arg, *out| out.* = try self.expression(columns, arg, aliases);
                break :blk .{ .call = .{ .name = part.name, .args = args, .star = part.star, .distinct = part.distinct, .filter = if (part.filter) |filter| try self.expression(columns, filter, aliases) else null } };
            },
            .case_when => |part| blk: {
                const branches = try self.alloc.alloc(ast.Scalar.Branch, part.branches.len);
                for (part.branches, branches) |branch, *out| out.* = .{ .condition = try self.expression(columns, branch.condition, aliases), .value = try self.expression(columns, branch.value, aliases) };
                break :blk .{ .case_when = .{ .branches = branches, .otherwise = if (part.otherwise) |other| try self.expression(columns, other, aliases) else null } };
            },
            .in_list => |part| blk: {
                const values = try self.alloc.alloc(*const ast.Scalar, part.values.len);
                for (part.values, values) |value, *out| out.* = try self.expression(columns, value, aliases);
                break :blk .{ .in_list = .{ .operand = try self.expression(columns, part.operand, aliases), .values = values, .negated = part.negated } };
            },
        });
    }
    fn predicate(self: *Builder, columns: []const Column, input: *const ast.Predicate) anyerror!*const ast.Predicate {
        const result = try self.alloc.create(ast.Predicate);
        result.* = switch (input.*) {
            .comparison => |part| .{ .comparison = .{ .field = (try field(columns, part.field)).internal, .op = part.op, .value = part.value } },
            .is_null => |part| .{ .is_null = .{ .field = (try field(columns, part.field)).internal, .negated = part.negated } },
            .scalar => |part| .{ .scalar = try self.expression(columns, part, &.{}) },
            .negation => |part| .{ .negation = try self.predicate(columns, part) },
            .conjunction => |part| .{ .conjunction = .{ .left = try self.predicate(columns, part.left), .right = try self.predicate(columns, part.right) } },
            .disjunction => |part| .{ .disjunction = .{ .left = try self.predicate(columns, part.left), .right = try self.predicate(columns, part.right) } },
        };
        return result;
    }
    fn lower(self: *Builder, source: *const Node, statement: ast.Select) !ast.Select {
        var result = statement;
        result.source = null;
        result.ctes = &.{};
        result.set_operation = null;
        result.table = .{ .table = "$sql_relation" };
        var projections: std.ArrayList(ast.Projection) = .empty;
        if (statement.columns.len == 0 and !statement.count_all) {
            for (source.columns) |column| if (column.visible) {
                try projections.append(self.alloc, .{ .field = column.internal, .alias = try self.alloc.dupe(u8, column.name) });
            };
        } else for (statement.columns) |projection| {
            if (projection.expression) |node_| {
                try projections.append(self.alloc, .{ .expression = try self.expression(source.columns, node_, &.{}), .alias = projection.alias });
            } else {
                const column = try field(source.columns, projection.field);
                try projections.append(self.alloc, .{ .field = column.internal, .expression = if (column.untyped_null) try self.scalarNode(.{ .literal = .null }) else null, .alias = projection.alias orelse column.name });
            }
        }
        result.columns = try projections.toOwnedSlice(self.alloc);
        result.predicate = if (statement.predicate) |input| try self.predicate(source.columns, input) else null;
        const groups = try self.alloc.alloc(*const ast.Scalar, statement.group_by.len);
        for (statement.group_by, groups) |input, *out| out.* = try self.expression(source.columns, input, result.columns);
        result.group_by = groups;
        result.having = if (statement.having) |input| try self.expression(source.columns, input, &.{}) else null;
        const orders = try self.alloc.alloc(ast.Order, statement.order_by.len);
        for (statement.order_by, orders) |order, *out| {
            out.* = order;
            if (order.expression) |input| out.expression = try self.expression(source.columns, input, &.{}) else if (order.position == null) {
                var alias = false;
                for (result.columns) |projection| if (projection.alias) |name| if (std.mem.eql(u8, name, order.field)) {
                    alias = true;
                    break;
                };
                if (!alias) out.field = (try field(source.columns, order.field)).internal;
            }
        }
        result.order_by = orders;
        if (result.predicate) |predicate_| {
            const wrapper = try self.alloc.create(ast.Predicate);
            wrapper.* = .{ .scalar = if (self.shape_only) try self.shapePredicate(source.columns, predicate_) else try @import("bound_scalars.zig").predicateScalar(self.alloc, try self.virtualTable(source.columns), predicate_) };
            result.predicate = wrapper;
        }
        return result;
    }

    fn querySource(self: *Builder, statement: ast.Select, scope: []const ast.Cte, depth: usize) anyerror!*const Node {
        if (depth > 32) return error.SqlProgramLimitExceeded;
        const ctes = try self.alloc.alloc(ast.Cte, scope.len + statement.ctes.len);
        @memcpy(ctes[0..scope.len], scope);
        @memcpy(ctes[scope.len..], statement.ctes);
        for (statement.ctes, 0..) |cte, index| for (statement.ctes[0..index]) |prior| if (std.mem.eql(u8, cte.name, prior.name)) return error.DuplicateSqlColumn;
        if (statement.set_operation) |set| {
            if (!self.shape_only) try self.inferSet(statement, scope, depth + 1);
            defer _ = self.inferred_sets.remove(set.left);
            const left = try self.derived(set.left, "", &.{}, ctes, depth + 1);
            const right = try self.derived(set.right, "", &.{}, ctes, depth + 1);
            if (left.columns.len != right.columns.len) return error.SqlTypeMismatch;
            const columns = try self.alloc.dupe(Column, left.columns);
            for (columns, right.columns, 0..) |*column, other, index| {
                if (self.shape_only) {
                    const args = try self.alloc.dupe(*const ast.Scalar, &.{ column.origin.?, other.origin.? });
                    column.origin = try self.scalarNode(.{ .call = .{ .name = "coalesce", .args = args } });
                    try self.constraints.append(self.alloc, .{ .expression = column.origin.? });
                } else if (untypedNull(left, index)) column.type = other.type else if (!untypedNull(right, index) and column.type != other.type) {
                    if ((column.type == .integer and other.type == .number) or (column.type == .number and other.type == .integer)) column.type = .number else return error.SqlTypeMismatch;
                }
                column.internal = try self.internal();
                column.nullable = column.nullable or other.nullable;
                column.untyped_null = column.untyped_null and other.untyped_null;
            }
            return self.node(columns, .{ .set = .{ .kind = set.kind, .all = set.all, .left = left, .right = right } });
        }
        if (statement.source) |relation_node| return self.relation(relation_node, ctes, depth + 1);
        if (statement.table) |table| return self.relation(&.{ .table = .{ .name = table } }, ctes, depth + 1);
        return self.node(&.{}, .singleton);
    }
    fn derived(self: *Builder, query: *const ast.Select, alias: []const u8, names: []const []const u8, scope: []const ast.Cte, depth: usize) anyerror!*const Node {
        const prepared: ?Prepared = if (self.prepared.fetchRemove(query)) |entry| entry.value else null;
        const child = if (prepared) |entry| entry.source else try self.querySource(query.*, scope, depth + 1);
        var lowered = if (prepared) |entry| entry.lowered else try self.lower(child, query.*);
        if (self.shape_only) {
            const expressions = try self.constrainSelect(child, lowered);
            if (names.len != 0 and names.len != expressions.len) return error.InvalidSqlParameters;
            const columns = try self.alloc.alloc(Column, expressions.len);
            for (columns, expressions, 0..) |*column, expression_, index| {
                const output = try scalar.inferOutput(self.alloc, expression_, self.shape_columns.items, self.parameters);
                column.* = .{
                    .name = if (names.len != 0) names[index] else if (lowered.count_all) lowered.count_alias orelse "count" else lowered.columns[index].alias orelse if (@import("aggregate_binding.zig").accepts(lowered) and lowered.columns[index].expression != null and lowered.columns[index].expression.?.* == .call) lowered.columns[index].expression.?.call.name else "?column?",
                    .internal = try self.internal(),
                    .qualifier = alias,
                    .type = output.kind orelse .string,
                    .nullable = true,
                    .origin = expression_,
                };
            }
            return self.node(columns, .singleton);
        }
        if (prepared) |entry| if (!lowered.count_all) {
            const projections = try self.alloc.dupe(ast.Projection, lowered.columns);
            const column_types = try self.scalarColumns(child.columns);
            for (projections, entry.expressions, entry.types) |*projection, expression_, kind| if (kind) |known| {
                if ((try scalar.inferOutput(self.alloc, expression_, column_types, self.parameters)).kind == null and projection.expression != null)
                    projection.expression = try self.scalarNode(.{ .cast = .{ .operand = projection.expression.?, .type = known } });
            };
            lowered.columns = projections;
        };
        const table = try self.virtualTable(child.columns);
        var adapter: ResolveAdapter = .{ .backend = self.backend, .table = table };
        const compiled: compiler.Compiled = .{ .arena = undefined, .statement = .{ .select = lowered }, .parameter_count = @intCast(self.parameters.len) };
        const bound = try describe.bind(self.alloc, adapter.iface(), &compiled, self.parameters);
        for (bound.parameter_types, self.parameters) |hint, *parameter| if (hint != null) {
            parameter.* = hint;
        };
        if (names.len != 0 and names.len != bound.columns.len) return error.InvalidSqlParameters;
        const columns = try self.alloc.alloc(Column, bound.columns.len);
        for (bound.columns, columns, 0..) |column, *out, index| {
            var untyped = column.untyped_null;
            if (lowered.columns.len > index and lowered.columns[index].expression == null) {
                for (child.columns) |source_column| if (std.mem.eql(u8, source_column.internal, lowered.columns[index].field)) {
                    untyped = untyped or source_column.untyped_null;
                };
            }
            out.* = .{ .name = try self.alloc.dupe(u8, if (names.len == 0) column.name else names[index]), .internal = try self.internal(), .qualifier = alias, .type = column.type, .nullable = true, .untyped_null = untyped };
        }
        return self.node(columns, .{ .query = .{ .source = child, .statement = lowered, .binding = bound } });
    }
    fn relation(self: *Builder, input: *const ast.Relation, scope: []const ast.Cte, depth: usize) anyerror!*const Node {
        if (depth > 32) return error.SqlProgramLimitExceeded;
        try self.backend.vtable.checkpoint(self.backend.ptr);
        return switch (input.*) {
            .table => |reference| blk: {
                if (reference.name.database == null and reference.name.namespace == null) {
                    var i = scope.len;
                    while (i != 0) {
                        i -= 1;
                        const cte = scope[i];
                        if (std.mem.eql(u8, cte.name, reference.name.table)) break :blk try self.derived(cte.query, reference.alias orelse cte.name, cte.columns, scope[0..i], depth + 1);
                    }
                }
                const identity = try std.fmt.allocPrint(self.alloc, "{s}\x00{s}\x00{s}", .{ reference.name.database orelse "", reference.name.namespace orelse "", reference.name.table });
                const entry = try self.identities.getOrPut(self.alloc, identity);
                if (!entry.found_existing) entry.value_ptr.* = try self.backend.vtable.resolve(self.backend.ptr, self.alloc, reference.name, .read);
                const table = entry.value_ptr.*;
                const columns = try self.alloc.alloc(Column, table.columns.len + 1);
                const source_columns = try self.alloc.alloc([]const u8, columns.len);
                const fields = try self.alloc.alloc([]const u8, table.columns.len);
                for (table.columns, columns[0..table.columns.len], source_columns[0..table.columns.len], fields) |column, *out, *source_name, *field_name| {
                    out.* = .{ .name = try self.alloc.dupe(u8, column.name), .internal = try self.internal(), .qualifier = reference.alias orelse reference.name.table, .type = column.type, .nullable = column.nullable };
                    source_name.* = column.name;
                    field_name.* = column.path;
                }
                columns[table.columns.len] = .{ .name = "_id", .internal = try self.internal(), .qualifier = reference.alias orelse reference.name.table, .type = .string, .nullable = false, .visible = false };
                if (self.shape_only) for (columns) |column| try self.shape_columns.append(self.alloc, .{ .name = column.internal, .type = column.type, .nullable = column.nullable });
                source_columns[table.columns.len] = "_id";
                const index = self.scans.items.len;
                if (index >= 64) return error.SqlProgramLimitExceeded;
                try self.scans.append(self.alloc, .{ .table = table, .request = .{ .fields = fields, .limit = 256 } });
                break :blk try self.node(columns, .{ .scan = .{ .index = index, .source_columns = source_columns } });
            },
            .derived => |query| self.derived(query.query, query.alias, &.{}, scope, depth + 1),
            .join => |join| blk: {
                const left = try self.relation(join.left, scope, depth + 1);
                const right = try self.relation(join.right, scope, depth + 1);
                for (left.columns) |a| for (right.columns) |b| if (std.mem.eql(u8, a.qualifier, b.qualifier)) return error.AmbiguousSqlColumn;
                const columns = try self.alloc.alloc(Column, left.columns.len + right.columns.len);
                @memcpy(columns[0..left.columns.len], left.columns);
                @memcpy(columns[left.columns.len..], right.columns);
                if (join.kind == .right or join.kind == .full) for (columns[0..left.columns.len]) |*column| {
                    column.nullable = true;
                };
                if (join.kind == .left or join.kind == .full) for (columns[left.columns.len..]) |*column| {
                    column.nullable = true;
                };
                const expression_ = if (join.condition) |condition| try self.expression(columns, condition, &.{}) else null;
                if (self.shape_only) {
                    if (expression_) |condition| try self.constraints.append(self.alloc, .{ .expression = try self.inferenceExpression(condition, columns), .expected = .boolean });
                    break :blk try self.node(columns, .singleton);
                }
                var left_keys: std.ArrayList(scalar.Program) = .empty;
                var right_keys: std.ArrayList(scalar.Program) = .empty;
                if (expression_) |condition| try self.joinKeys(condition, left, right, &left_keys, &right_keys);
                const column_types = try self.scalarColumns(columns);
                if (expression_) |condition| _ = try scalar.inferParameters(self.alloc, condition, column_types, self.parameters, .boolean, .{});
                const program = if (expression_) |condition| try scalar.bindExpected(self.alloc, condition, column_types, self.parameters, .boolean, .{}) else null;
                if (program) |bound| if (bound.output_type.kind != null and bound.output_type.kind != .boolean) return error.SqlTypeMismatch;
                break :blk try self.node(columns, .{ .join = .{ .kind = join.kind, .left = left, .right = right, .condition = program, .left_keys = try left_keys.toOwnedSlice(self.alloc), .right_keys = try right_keys.toOwnedSlice(self.alloc) } });
            },
        };
    }
    fn joinKeys(self: *Builder, input: *const ast.Scalar, left: *const Node, right: *const Node, left_keys: *std.ArrayList(scalar.Program), right_keys: *std.ArrayList(scalar.Program)) anyerror!void {
        if (input.* != .binary) return;
        const binary = input.binary;
        if (binary.op == .@"and") {
            try self.joinKeys(binary.left, left, right, left_keys, right_keys);
            try self.joinKeys(binary.right, left, right, left_keys, right_keys);
            return;
        }
        if (binary.op != .eq or binary.left.* != .column or binary.right.* != .column) return;
        var left_node = binary.left;
        var right_node = binary.right;
        if (!hasInternal(left.columns, left_node.column)) std.mem.swap(*const ast.Scalar, &left_node, &right_node);
        if (!hasInternal(left.columns, left_node.column) or !hasInternal(right.columns, right_node.column)) return;
        try left_keys.append(self.alloc, try scalar.bind(self.alloc, left_node, try self.scalarColumns(left.columns), self.parameters, .{}));
        try right_keys.append(self.alloc, try scalar.bind(self.alloc, right_node, try self.scalarColumns(right.columns), self.parameters, .{}));
    }
};
fn hasInternal(columns: []const Column, name: []const u8) bool {
    for (columns) |column| if (std.mem.eql(u8, column.internal, name)) return true;
    return false;
}

fn untypedNull(node: *const Node, index: usize) bool {
    return index < node.columns.len and node.columns[index].untyped_null;
}

pub fn outputUntypedNull(bound: Bound, index: usize) bool {
    if (index >= bound.statement.columns.len) return false;
    const projection = bound.statement.columns[index];
    if (projection.expression != null) return false;
    for (bound.root.columns) |column| if (std.mem.eql(u8, column.internal, projection.field)) return column.untyped_null;
    return false;
}

fn markExpression(alloc: Allocator, needed: *std.StringHashMapUnmanaged(void), input: *const ast.Scalar) anyerror!void {
    switch (input.*) {
        .column => |name| try needed.put(alloc, name, {}),
        .literal => {},
        .unary => |part| try markExpression(alloc, needed, part.operand),
        .binary => |part| {
            try markExpression(alloc, needed, part.left);
            try markExpression(alloc, needed, part.right);
        },
        .cast => |part| try markExpression(alloc, needed, part.operand),
        .call => |part| {
            for (part.args) |arg| try markExpression(alloc, needed, arg);
            if (part.filter) |filter| try markExpression(alloc, needed, filter);
        },
        .case_when => |part| {
            for (part.branches) |branch| {
                try markExpression(alloc, needed, branch.condition);
                try markExpression(alloc, needed, branch.value);
            }
            if (part.otherwise) |other| try markExpression(alloc, needed, other);
        },
        .in_list => |part| {
            try markExpression(alloc, needed, part.operand);
            for (part.values) |value| try markExpression(alloc, needed, value);
        },
    }
}
fn markPredicate(alloc: Allocator, needed: *std.StringHashMapUnmanaged(void), input: *const ast.Predicate) anyerror!void {
    switch (input.*) {
        .comparison => |part| try needed.put(alloc, part.field, {}),
        .is_null => |part| try needed.put(alloc, part.field, {}),
        .scalar => |part| try markExpression(alloc, needed, part),
        .negation => |part| try markPredicate(alloc, needed, part),
        .conjunction, .disjunction => |part| {
            try markPredicate(alloc, needed, part.left);
            try markPredicate(alloc, needed, part.right);
        },
    }
}
fn markSelect(alloc: Allocator, needed: *std.StringHashMapUnmanaged(void), statement: ast.Select) !void {
    for (statement.columns) |projection| if (projection.expression) |expression| {
        try markExpression(alloc, needed, expression);
    } else {
        try needed.put(alloc, projection.field, {});
    };
    if (statement.predicate) |predicate| try markPredicate(alloc, needed, predicate);
    for (statement.group_by) |expression| try markExpression(alloc, needed, expression);
    if (statement.having) |expression| try markExpression(alloc, needed, expression);
    for (statement.order_by) |order| if (order.expression) |expression| {
        try markExpression(alloc, needed, expression);
    } else if (order.position == null) {
        try needed.put(alloc, order.field, {});
    };
}
fn projectScans(builder: *Builder, node: *const Node, needed: *std.StringHashMapUnmanaged(void)) anyerror!void {
    switch (node.operation) {
        .singleton => {},
        .scan => |scan| {
            var fields: std.ArrayList([]const u8) = .empty;
            const request = &builder.scans.items[scan.index];
            for (node.columns, scan.source_columns) |column, name| {
                if (!needed.contains(column.internal) or std.mem.eql(u8, name, "_id")) continue;
                try fields.append(builder.alloc, (try request.table.column(name)).path);
            }
            request.request.fields = try fields.toOwnedSlice(builder.alloc);
        },
        .join => |join| {
            if (join.condition) |program| for (program.required_columns) |ordinal| try needed.put(builder.alloc, node.columns[ordinal].internal, {});
            try projectScans(builder, join.left, needed);
            try projectScans(builder, join.right, needed);
        },
        .query => |query| {
            try markSelect(builder.alloc, needed, query.statement);
            try projectScans(builder, query.source, needed);
        },
        .set => |set| {
            try projectScans(builder, set.left, needed);
            try projectScans(builder, set.right, needed);
        },
    }
}

pub fn bind(alloc: Allocator, backend: catalog.Backend, statement: ast.Select, parameters: []?ast.ColumnType) anyerror!Bound {
    var builder: Builder = .{ .alloc = alloc, .backend = backend, .parameters = parameters };
    if (std.mem.indexOfScalar(?ast.ColumnType, parameters, null) != null) {
        var shape: Builder = .{ .alloc = alloc, .backend = backend, .parameters = parameters, .shape_only = true };
        try shape.inferShape(statement, &.{});
        builder.identities = shape.identities;
    }
    const root = try builder.querySource(statement, &.{}, 0);
    const lowered = try builder.lower(root, statement);
    var needed: std.StringHashMapUnmanaged(void) = .empty;
    try markSelect(alloc, &needed, lowered);
    try projectScans(&builder, root, &needed);
    return .{ .root = root, .scans = try builder.scans.toOwnedSlice(alloc), .table = try builder.virtualTable(root.columns), .statement = lowered };
}

/// Assignment types cross arbitrary derived/CTE/set boundaries before binding
/// source programs. This is a catalog-only pass; it never opens a row reader.
pub fn inferExpected(alloc: Allocator, backend: catalog.Backend, statement: ast.Select, parameters: []?ast.ColumnType, expected: []const ast.ColumnType) !void {
    if (std.mem.indexOfScalar(?ast.ColumnType, parameters, null) == null) return;
    var shape: Builder = .{ .alloc = alloc, .backend = backend, .parameters = parameters, .shape_only = true };
    try shape.inferShape(statement, expected);
}

/// RETURNING has one authorized target, not a separate relational read. Reuse
/// ordinary qualification validation while retaining native column names for
/// evaluating the already-prepared mutation image.
pub fn normalizeTargetProjection(alloc: Allocator, backend: catalog.Backend, table: catalog.Table, name: ast.Name, projections: []const ast.Projection) ![]const ast.Projection {
    var builder: Builder = .{ .alloc = alloc, .backend = backend, .parameters = &.{} };
    const columns = try alloc.alloc(Column, table.columns.len + 1);
    for (table.columns, columns[0..table.columns.len]) |column, *out| out.* = .{ .name = column.name, .internal = column.name, .qualifier = name.table, .type = column.type, .nullable = column.nullable };
    columns[table.columns.len] = .{ .name = "_id", .internal = "_id", .qualifier = name.table, .type = .string, .nullable = false, .visible = false };
    const source: Node = .{ .columns = columns, .operation = .singleton };
    return (try builder.lower(&source, .{ .table = name, .columns = projections })).columns;
}
