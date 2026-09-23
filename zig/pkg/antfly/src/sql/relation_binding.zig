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
        recursive_ref: usize,
        recursive: struct { id: usize, seed: *const Node, step: *const Node, all: bool },
        materialized_ref: *const Node,
        scan: struct { index: usize, source_columns: []const []const u8 },
        join: struct { kind: ast.JoinKind, left: *const Node, right: *const Node, condition: ?scalar.Program, left_keys: []const scalar.Program, right_keys: []const scalar.Program },
        query: struct { source: *const Node, statement: ast.Select, binding: describe.BoundStatement },
        set: struct { kind: ast.SetKind, all: bool, left: *const Node, right: *const Node },
        /// Compiler-generated INSERT VALUES arms in input order. Each arm
        /// retains its own captured source dependencies, but execution opens
        /// only one arm iterator at a time after statement capture.
        values: []const *const Node,
        /// Adjacent compiler-owned literal rows share one small operator rather
        /// than each allocating a SELECT program and iterator.
        literal_rows: []const []const scalar.Datum,
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
            if (part.window) |spec| {
                for (spec.partition) |item| if (qualified(item)) break :blk true;
                for (spec.order) |item| {
                    if (std.mem.indexOfScalar(u8, item.field, 0) != null) break :blk true;
                    if (item.expression) |expression_| if (qualified(expression_)) break :blk true;
                }
            }
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
    if (statement.source != null or statement.ctes.len != 0 or statement.set_operation != null or statement.values_arms.len != 0) return true;
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

/// Joined mutations pin the read/write-authorized target once, then bind all
/// source relations against that same catalog identity. Non-target relations
/// still resolve through the backend with their requested read authority.
pub const TargetResolveAdapter = struct {
    backend: catalog.Backend,
    table: catalog.Table,
    name: ast.Name,
    /// A mutation statement must bind every occurrence of a source against
    /// the same authorized catalog identity, including an optimized rebind.
    cache_sources: bool = false,
    source_tables: std.StringHashMapUnmanaged(catalog.Table) = .empty,
    pub fn iface(self: *@This()) catalog.Backend {
        return .{ .ptr = self, .vtable = &.{ .resolve = resolve, .scan = scan, .mutate = mutate, .checkpoint = checkpoint } };
    }
    fn resolve(ptr: *anyopaque, alloc: Allocator, name: ast.Name, action: catalog.Action) !catalog.Table {
        // This adapter is only used while binding the read side of a mutation.
        // Never let a future caller turn the pinned target into an implicit
        // authorization for a second write or an administrative operation.
        if (action != .read) return error.UnsupportedSqlExecution;
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (std.mem.eql(u8, name.table, self.name.table) and std.mem.eql(u8, name.database orelse "", self.name.database orelse "") and std.mem.eql(u8, name.namespace orelse "", self.name.namespace orelse "")) return self.table;
        if (!self.cache_sources) return self.backend.vtable.resolve(self.backend.ptr, alloc, name, action);
        const key = try std.fmt.allocPrint(alloc, "{s}\x00{s}\x00{s}", .{ name.database orelse "", name.namespace orelse "", name.table });
        if (self.source_tables.get(key)) |cached| {
            alloc.free(key);
            return cached;
        }
        errdefer alloc.free(key);
        const resolved = try self.backend.vtable.resolve(self.backend.ptr, alloc, name, action);
        try self.source_tables.put(alloc, key, resolved);
        return resolved;
    }
    fn scan(_: *anyopaque, _: Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
        return error.InvalidSqlBackendResponse;
    }
    fn mutate(_: *anyopaque, _: Allocator, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
        return error.InvalidSqlBackendResponse;
    }
    fn checkpoint(ptr: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try self.backend.vtable.checkpoint(self.backend.ptr);
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
    node_limit: usize = 256,
    prepared: std.AutoHashMapUnmanaged(*const ast.Select, Prepared) = .empty,
    inferred_sets: std.AutoHashMapUnmanaged(*const ast.Select, void) = .empty,
    shape_only: bool = false,
    shape_expression_nodes: usize = 0,
    shape_columns: std.ArrayList(scalar.Column) = .empty,
    constraints: std.ArrayList(Constraint) = .empty,
    recursive_nodes: std.AutoHashMapUnmanaged(*const ast.Select, *const Node) = .empty,
    materialized_nodes: std.AutoHashMapUnmanaged(*const ast.Select, *const Node) = .empty,
    auto_materialized: std.AutoHashMapUnmanaged(*const ast.Select, void) = .empty,
    recursive_active: ?*const RecursiveFrame = null,
    recursive_next: usize = 0,
    const RecursiveFrame = struct { id: usize, query: *const ast.Select, columns: []const Column, parent: ?*const RecursiveFrame };
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
                if (call.window) |spec| {
                    if (self.shape_only) {
                        for (spec.partition) |item| try self.constraints.append(self.alloc, .{ .expression = try self.inferenceExpression(item, columns) });
                        for (spec.order) |item| try self.constraints.append(self.alloc, .{ .expression = try self.inferenceExpression(item.expression orelse try self.scalarNode(.{ .column = item.field }), columns) });
                        if (spec.frame) |frame| for ([_]ast.Window.Bound{ frame.start, frame.end }) |bound| switch (bound) {
                            .preceding, .following => |value| try self.constraints.append(self.alloc, .{ .expression = try self.scalarNode(.{ .literal = value }), .expected = .integer }),
                            else => {},
                        };
                    }
                    const kind = std.meta.stringToEnum(@import("window_binding.zig").Kind, call.name) orelse return error.UndefinedSqlFunction;
                    switch (kind) {
                        .row_number, .rank, .dense_rank => break :blk .{ .literal = .{ .integer = 0 } },
                        .ntile => {
                            if (call.args.len != 1) return error.InvalidSqlParameters;
                            if (self.shape_only) try self.constraints.append(self.alloc, .{ .expression = try self.inferenceExpression(call.args[0], columns), .expected = .integer });
                            break :blk .{ .literal = .{ .integer = 0 } };
                        },
                        .percent_rank, .cume_dist => break :blk .{ .literal = .{ .number = 0 } },
                        .lag, .lead, .first_value, .last_value, .nth_value => {
                            if (call.args.len == 0) return error.InvalidSqlParameters;
                            if (self.shape_only and call.args.len > 1) try self.constraints.append(self.alloc, .{ .expression = try self.inferenceExpression(call.args[1], columns), .expected = .integer });
                            if (call.args.len == 3) {
                                const args = try self.alloc.alloc(*const ast.Scalar, 2);
                                args[0] = try self.inferenceExpression(call.args[0], columns);
                                args[1] = try self.inferenceExpression(call.args[2], columns);
                                break :blk .{ .call = .{ .name = "coalesce", .args = args } };
                            }
                            break :blk (try self.inferenceExpression(call.args[0], columns)).*;
                        },
                        else => {},
                    }
                }
                if (@import("aggregate_binding.zig").aggregateKind(call.name)) |kind| {
                    if (self.shape_only) if (call.filter) |filter| try self.constraints.append(self.alloc, .{ .expression = try self.inferenceExpression(filter, columns), .expected = .boolean });
                    if (kind == .count) break :blk .{ .literal = .{ .integer = 0 } };
                    if (call.args.len != 1) return error.InvalidSqlParameters;
                    const argument = try self.inferenceExpression(call.args[0], columns);
                    break :blk switch (kind) {
                        .avg => .{ .cast = .{ .operand = argument, .type = .number } },
                        .bool_and, .bool_or => .{ .cast = .{ .operand = argument, .type = .boolean } },
                        .pattern_set => .{ .cast = .{ .operand = try self.scalarNode(.{ .cast = .{ .operand = argument, .type = .string } }), .type = .json } },
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

    fn mergeInferredType(current: *?ast.ColumnType, inferred: ?ast.ColumnType) !void {
        const kind = inferred orelse return;
        if (current.* == null) {
            current.* = kind;
        } else if (current.* != kind) {
            if ((current.* == .integer or current.* == .number) and (kind == .integer or kind == .number)) current.* = .number else return error.SqlTypeMismatch;
        }
    }

    fn literalType(value: ast.Value) ?ast.ColumnType {
        return switch (value) {
            .integer => .integer,
            .number => .number,
            .boolean => .boolean,
            .string => .string,
            .null, .parameter => null,
        };
    }

    fn inferValues(self: *Builder, statement: ast.Select, scope: []const ast.Cte, depth: usize) anyerror![]const ?ast.ColumnType {
        const width = statement.values_arms[0].columns.len;
        var leaves: std.ArrayList(*const ast.Select) = .empty;
        for (statement.values_arms, 0..) |arm, index| {
            if (index % 64 == 0) try self.backend.vtable.checkpoint(self.backend.ptr);
            if (arm.columns.len != width) return error.SqlTypeMismatch;
            if (!literalValuesArm(arm)) try self.collectSet(arm, scope, &leaves, depth + 1);
        }
        const common = try self.alloc.alloc(?ast.ColumnType, width);
        for (0..self.parameters.len + 2) |_| {
            @memset(common, null);
            for (statement.values_arms, 0..) |arm, index| {
                if (index % 64 == 0) try self.backend.vtable.checkpoint(self.backend.ptr);
                if (literalValuesArm(arm)) {
                    for (arm.columns, common) |projection, *kind| {
                        const expression_ = projection.expression orelse return error.InvalidSqlBackendResponse;
                        try mergeInferredType(kind, literalType(expression_.literal));
                    }
                } else {
                    const prepared = self.prepared.get(arm) orelse return error.InvalidSqlBackendResponse;
                    const columns = try self.scalarColumns(prepared.source.columns);
                    for (prepared.expressions, common) |expression_, *kind| try mergeInferredType(kind, (try scalar.inferOutput(self.alloc, expression_, columns, self.parameters)).kind);
                }
            }
            var changed = false;
            for (leaves.items) |leaf| {
                const prepared = self.prepared.getPtr(leaf) orelse return error.InvalidSqlBackendResponse;
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
        return common;
    }

    fn node(self: *Builder, columns: []const Column, operation: @FieldType(Node, "operation")) !*const Node {
        self.nodes += 1;
        if (self.nodes > self.node_limit) return error.SqlProgramLimitExceeded;
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
                var window = part.window;
                if (window) |*spec| {
                    const partitions = try self.alloc.alloc(*const ast.Scalar, spec.partition.len);
                    for (spec.partition, partitions) |item, *out| out.* = try self.expression(columns, item, &.{});
                    spec.partition = partitions;
                    const order = try self.alloc.dupe(ast.Order, spec.order);
                    for (order) |*item| {
                        if (item.expression) |expression_| item.expression = try self.expression(columns, expression_, &.{}) else item.field = (try field(columns, item.field)).internal;
                    }
                    spec.order = order;
                }
                break :blk .{ .call = .{ .name = part.name, .args = args, .star = part.star, .distinct = part.distinct, .filter = if (part.filter) |filter| try self.expression(columns, filter, aliases) else null, .window = window } };
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
        result.values_arms = &.{};
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
        const windows = try self.alloc.alloc(ast.NamedWindow, statement.windows.len);
        for (statement.windows, windows) |definition, *out| {
            const wrapped = try self.scalarNode(.{ .call = .{ .name = "row_number", .args = &.{}, .window = definition.window } });
            const rewritten = try self.expression(source.columns, wrapped, &.{});
            out.* = .{ .name = definition.name, .window = rewritten.call.window.? };
        }
        result.windows = windows;
        if (result.predicate) |predicate_| {
            const wrapper = try self.alloc.create(ast.Predicate);
            wrapper.* = .{ .scalar = if (self.shape_only) try self.shapePredicate(source.columns, predicate_) else try @import("bound_scalars.zig").predicateScalar(self.alloc, try self.virtualTable(source.columns), predicate_) };
            result.predicate = wrapper;
        }
        return result;
    }

    fn valuesOrigin(self: *Builder, arms: []const *const Node, index: usize) anyerror!*const ast.Scalar {
        if (arms.len == 1) return arms[0].columns[index].origin orelse error.InvalidSqlBackendResponse;
        const middle = arms.len / 2;
        const arguments = try self.alloc.dupe(*const ast.Scalar, &.{ try self.valuesOrigin(arms[0..middle], index), try self.valuesOrigin(arms[middle..], index) });
        return self.scalarNode(.{ .call = .{ .name = "coalesce", .args = arguments } });
    }

    fn literalValuesArm(query: *const ast.Select) bool {
        if (!query.generated_values or query.table != null or query.source != null or query.set_operation != null or query.values_arms.len != 0 or query.ctes.len != 0 or query.predicate != null or query.group_by.len != 0 or query.having != null or query.order_by.len != 0 or query.limit != null or query.offset != null) return false;
        for (query.columns) |projection| {
            const expression_ = projection.expression orelse return false;
            if (expression_.* != .literal or expression_.literal == .parameter) return false;
        }
        return true;
    }

    fn literalDatum(expression_: *const ast.Scalar) !scalar.Datum {
        if (expression_.* != .literal) return error.InvalidSqlBackendResponse;
        const value = expression_.literal;
        return .{ .value = switch (value) {
            .integer => |number| .{ .integer = number },
            .number => |number| .{ .float = number },
            .boolean => |boolean| .{ .bool = boolean },
            .string => |string| .{ .string = string },
            .null => .null,
            .parameter => return error.InvalidSqlBackendResponse,
        }, .sql_null = value == .null };
    }

    fn querySource(self: *Builder, statement: ast.Select, scope: []const ast.Cte, depth: usize) anyerror!*const Node {
        if (depth > 32) return error.SqlProgramLimitExceeded;
        // Work backward through the WITH dependency DAG. An inlined later CTE
        // can multiply demand on its producer, while a materialized later CTE
        // evaluates its producer once. Two is enough to choose sharing, so
        // neither reference counts nor planning work grow with execution size.
        const demand = try self.alloc.alloc(u8, statement.ctes.len);
        var body = statement;
        body.ctes = &.{};
        var remaining = statement.ctes.len;
        while (remaining != 0) {
            remaining -= 1;
            const cte = statement.ctes[remaining];
            var count: usize = @min(2, try @import("recursive_shape.zig").references(body, cte.name, 0));
            for (statement.ctes[remaining + 1 ..], demand[remaining + 1 ..]) |later, later_demand| {
                if (later_demand == 0 or count == 2) continue;
                const direct = @min(2, try @import("recursive_shape.zig").references(later.query.*, cte.name, 0));
                const evaluations: usize = if (later.recursive) 2 else if (later.materialization == .materialized or (later.materialization == .automatic and self.auto_materialized.contains(later.query))) 1 else later_demand;
                count = @min(2, count + direct * evaluations);
            }
            demand[remaining] = @intCast(count);
            if (!cte.recursive and cte.materialization == .automatic and count > 1) try self.auto_materialized.put(self.alloc, cte.query, {});
        }
        for (statement.ctes, 0..) |cte, i| if (cte.recursive) {
            for (statement.ctes[i + 1 ..]) |later| if (try @import("recursive_shape.zig").references(cte.query.*, later.name, 0) != 0) return error.UnsupportedSqlShape;
        };
        const ctes = try self.alloc.alloc(ast.Cte, scope.len + statement.ctes.len);
        @memcpy(ctes[0..scope.len], scope);
        @memcpy(ctes[scope.len..], statement.ctes);
        for (statement.ctes, 0..) |cte, index| for (statement.ctes[0..index]) |prior| if (std.mem.eql(u8, cte.name, prior.name)) return error.DuplicateSqlColumn;
        if (statement.values_arms.len != 0) {
            if (!statement.generated_values or statement.set_operation != null or statement.values_arms.len > 1000) return error.InvalidSqlBackendResponse;
            const common = if (self.shape_only) &.{} else try self.inferValues(statement, scope, depth + 1);
            var grouped: std.ArrayList(*const Node) = .empty;
            var arm_index: usize = 0;
            while (arm_index < statement.values_arms.len) {
                if (arm_index % 64 == 0) try self.backend.vtable.checkpoint(self.backend.ptr);
                const leaf = statement.values_arms[arm_index];
                if (!self.shape_only and literalValuesArm(leaf)) {
                    const first = arm_index;
                    while (arm_index < statement.values_arms.len and literalValuesArm(statement.values_arms[arm_index])) : (arm_index += 1) {
                        if (arm_index % 64 == 0) try self.backend.vtable.checkpoint(self.backend.ptr);
                    }
                    const columns = try self.alloc.alloc(Column, leaf.columns.len);
                    for (leaf.columns, columns, common) |projection, *column, kind| column.* = .{
                        .name = projection.alias orelse return error.InvalidSqlBackendResponse,
                        .internal = try self.internal(),
                        .qualifier = "",
                        .type = kind orelse .string,
                        .nullable = true,
                        .untyped_null = kind == null,
                    };
                    const rows = try self.alloc.alloc([]const scalar.Datum, arm_index - first);
                    for (statement.values_arms[first..arm_index], rows, 0..) |row_query, *row, index| {
                        if (index % 64 == 0) try self.backend.vtable.checkpoint(self.backend.ptr);
                        const values = try self.alloc.alloc(scalar.Datum, row_query.columns.len);
                        for (row_query.columns, values) |projection, *value| value.* = try literalDatum(projection.expression orelse return error.InvalidSqlBackendResponse);
                        row.* = values;
                    }
                    try grouped.append(self.alloc, try self.node(columns, .{ .literal_rows = rows }));
                } else {
                    try grouped.append(self.alloc, try self.derived(leaf, "", &.{}, ctes, depth + 1));
                    arm_index += 1;
                }
            }
            const arms = grouped.items;
            const columns = try self.alloc.dupe(Column, arms[0].columns);
            for (arms[1..]) |arm| {
                if (arm.columns.len != columns.len) return error.SqlTypeMismatch;
                for (columns, arm.columns) |*column, other| {
                    if (self.shape_only) {
                        column.type = .string;
                    } else if (column.untyped_null) column.type = other.type else if (!other.untyped_null and column.type != other.type) {
                        if ((column.type == .integer and other.type == .number) or (column.type == .number and other.type == .integer)) column.type = .number else return error.SqlTypeMismatch;
                    }
                    column.nullable = column.nullable or other.nullable;
                    column.untyped_null = column.untyped_null and other.untyped_null;
                }
            }
            for (columns, 0..) |*column, index| {
                column.internal = try self.internal();
                if (self.shape_only) {
                    column.origin = try self.valuesOrigin(arms, index);
                    try self.constraints.append(self.alloc, .{ .expression = column.origin.? });
                }
            }
            return self.node(columns, .{ .values = arms });
        }
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
        if (@import("subquery_lowering.zig").accepts(query.*)) {
            const rewritten = try self.alloc.create(ast.Select);
            rewritten.* = try @import("subquery_lowering.zig").lower(self.alloc, query.*);
            return self.derived(rewritten, alias, names, scope, depth + 1);
        }
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
    fn recursiveAlias(self: *Builder, source: *const Node, alias: []const u8) !*const Node {
        const columns = try self.alloc.dupe(Column, source.columns);
        for (columns) |*column| {
            column.internal = try self.internal();
            column.qualifier = alias;
            column.nullable = true;
        }
        return self.node(columns, source.operation);
    }

    fn recursiveCte(self: *Builder, cte: ast.Cte, alias: []const u8, scope: []const ast.Cte, depth: usize) anyerror!*const Node {
        var active = self.recursive_active;
        while (active) |frame| : (active = frame.parent) if (frame.query == cte.query) {
            return self.recursiveAlias(try self.node(frame.columns, .{ .recursive_ref = frame.id }), alias);
        };
        if (self.recursive_nodes.get(cte.query)) |prior| return self.recursiveAlias(prior, alias);
        try @import("recursive_shape.zig").validate(cte);
        if (self.recursive_next >= 32) return error.SqlProgramLimitExceeded;
        const id = self.recursive_next;
        self.recursive_next += 1;
        const set = cte.query.set_operation.?;
        const seed_scope = try self.alloc.alloc(ast.Cte, scope.len + cte.query.ctes.len);
        @memcpy(seed_scope[0..scope.len], scope);
        @memcpy(seed_scope[scope.len..], cte.query.ctes);
        const seed = try self.derived(set.left, cte.name, cte.columns, seed_scope, depth + 1);
        if (seed.columns.len == 0) return error.InvalidSqlParameters;
        const frame: RecursiveFrame = .{ .id = id, .query = cte.query, .columns = seed.columns, .parent = self.recursive_active };
        self.recursive_active = &frame;
        defer self.recursive_active = frame.parent;
        const step_scope = try self.alloc.alloc(ast.Cte, scope.len + 1 + cte.query.ctes.len);
        @memcpy(step_scope[0..scope.len], scope);
        step_scope[scope.len] = cte;
        @memcpy(step_scope[scope.len + 1 ..], cte.query.ctes);
        const step = try self.derived(set.right, cte.name, cte.columns, step_scope, depth + 1);
        if (seed.columns.len != step.columns.len) return error.SqlTypeMismatch;
        const columns = try self.alloc.dupe(Column, seed.columns);
        for (columns, step.columns) |*column, other| {
            column.nullable = true;
            if (self.shape_only) {
                const args = try self.alloc.dupe(*const ast.Scalar, &.{ column.origin.?, other.origin.? });
                column.origin = try self.scalarNode(.{ .call = .{ .name = "coalesce", .args = args } });
                try self.constraints.append(self.alloc, .{ .expression = column.origin.? });
            } else if (!other.untyped_null and column.type != other.type and !(column.type == .number and other.type == .integer)) return error.SqlTypeMismatch;
        }
        const result = try self.node(columns, if (self.shape_only) .singleton else .{ .recursive = .{ .id = id, .seed = seed, .step = step, .all = set.all } });
        try self.recursive_nodes.put(self.alloc, cte.query, result);
        return self.recursiveAlias(result, alias);
    }

    fn relation(self: *Builder, input: *const ast.Relation, scope: []const ast.Cte, depth: usize) anyerror!*const Node {
        if (depth > 32) return error.SqlProgramLimitExceeded;
        try self.backend.vtable.checkpoint(self.backend.ptr);
        return switch (input.*) {
            .table => |reference| blk: {
                if (!reference.mutation_target and reference.name.database == null and reference.name.namespace == null) {
                    var i = scope.len;
                    while (i != 0) {
                        i -= 1;
                        const cte = scope[i];
                        if (std.mem.eql(u8, cte.name, reference.name.table)) {
                            if (cte.recursive and try @import("recursive_shape.zig").references(cte.query.*, cte.name, 0) != 0)
                                break :blk try self.recursiveCte(cte, reference.alias orelse cte.name, scope[0..i], depth + 1);
                            if (cte.materialization == .materialized or (cte.materialization == .automatic and self.auto_materialized.contains(cte.query))) {
                                const materialized = self.materialized_nodes.get(cte.query) orelse source: {
                                    const source = try self.derived(cte.query, cte.name, cte.columns, scope[0..i], depth + 1);
                                    try self.materialized_nodes.put(self.alloc, cte.query, source);
                                    break :source source;
                                };
                                const columns = try self.alloc.dupe(Column, materialized.columns);
                                for (columns) |*column| {
                                    column.internal = try self.internal();
                                    column.qualifier = reference.alias orelse cte.name;
                                }
                                break :blk try self.node(columns, .{ .materialized_ref = materialized });
                            }
                            break :blk try self.derived(cte.query, reference.alias orelse cte.name, cte.columns, scope[0..i], depth + 1);
                        }
                    }
                }
                const identity = try std.fmt.allocPrint(self.alloc, "{s}\x00{s}\x00{s}", .{ reference.name.database orelse "", reference.name.namespace orelse "", reference.name.table });
                const entry = try self.identities.getOrPut(self.alloc, identity);
                if (!entry.found_existing) entry.value_ptr.* = try self.backend.vtable.resolve(self.backend.ptr, self.alloc, reference.name, .read);
                const table = entry.value_ptr.*;
                if (reference.mutation_presence and !reference.mutation_target) return error.InvalidSqlBackendResponse;
                const metadata_count: usize = if (!reference.mutation_target) 0 else if (reference.mutation_presence) 4 else 3;
                const columns = try self.alloc.alloc(Column, table.columns.len + 1 + metadata_count);
                const source_columns = try self.alloc.alloc([]const u8, columns.len);
                const fields = try self.alloc.alloc([]const u8, table.columns.len);
                for (table.columns, columns[0..table.columns.len], source_columns[0..table.columns.len], fields) |column, *out, *source_name, *field_name| {
                    out.* = .{ .name = try self.alloc.dupe(u8, column.name), .internal = try self.internal(), .qualifier = reference.alias orelse reference.name.table, .type = column.type, .nullable = column.nullable };
                    source_name.* = column.name;
                    field_name.* = column.path;
                }
                columns[table.columns.len] = .{ .name = "_id", .internal = try self.internal(), .qualifier = reference.alias orelse reference.name.table, .type = .string, .nullable = false, .visible = false };
                for (@import("joined_mutation.zig").metadata_fields[0..metadata_count], 0..) |name, i| {
                    columns[table.columns.len + 1 + i] = .{ .name = name, .internal = try self.internal(), .qualifier = reference.alias orelse reference.name.table, .type = if (i == 2) .json else .string, .nullable = i == 2, .visible = false };
                    source_columns[table.columns.len + 1 + i] = name;
                }
                if (self.shape_only) for (columns) |column| try self.shape_columns.append(self.alloc, .{ .name = column.internal, .type = column.type, .nullable = column.nullable });
                source_columns[table.columns.len] = "_id";
                const index = self.scans.items.len;
                if (index >= 64) return error.SqlProgramLimitExceeded;
                try self.scans.append(self.alloc, .{ .table = table, .request = .{ .fields = fields, .limit = 256, .include_primary_digest = reference.mutation_target, .include_document = reference.mutation_document and table.storage_mode == .document } });
                break :blk try self.node(columns, .{ .scan = .{ .index = index, .source_columns = source_columns } });
            },
            .derived => |query| blk: {
                const result = try self.derived(query.query, query.alias, query.columns, scope, depth + 1);
                if (query.hidden) for (@constCast(result.columns)) |*column| {
                    column.visible = false;
                };
                break :blk result;
            },
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
        if (binary.op != .eq) return;
        var left_node = binary.left;
        var right_node = binary.right;
        if (!sideLocal(left.columns, left_node) or !sideLocal(right.columns, right_node)) std.mem.swap(*const ast.Scalar, &left_node, &right_node);
        if (!sideLocal(left.columns, left_node) or !sideLocal(right.columns, right_node)) return;
        try left_keys.append(self.alloc, try scalar.bind(self.alloc, left_node, try self.scalarColumns(left.columns), self.parameters, .{}));
        try right_keys.append(self.alloc, try scalar.bind(self.alloc, right_node, try self.scalarColumns(right.columns), self.parameters, .{}));
    }
};
/// Expressions whose inputs belong to one side are valid hash keys too.
/// Restricting extraction to bare columns makes computed IN/join operands
/// unexpectedly quadratic despite a perfectly usable equality key.
fn sideLocal(columns: []const Column, input: *const ast.Scalar) bool {
    return switch (input.*) {
        .literal => true,
        .column => |name| hasInternal(columns, name),
        .unary => |part| sideLocal(columns, part.operand),
        .cast => |part| sideLocal(columns, part.operand),
        .binary => |part| sideLocal(columns, part.left) and sideLocal(columns, part.right),
        .call => |part| blk: {
            if (part.subquery != null or part.window != null or part.star or part.distinct or part.filter != null) break :blk false;
            for (part.args) |arg| if (!sideLocal(columns, arg)) break :blk false;
            break :blk true;
        },
        .case_when => |part| blk: {
            for (part.branches) |branch| if (!sideLocal(columns, branch.condition) or !sideLocal(columns, branch.value)) break :blk false;
            break :blk if (part.otherwise) |other| sideLocal(columns, other) else true;
        },
        .in_list => |part| blk: {
            if (!sideLocal(columns, part.operand)) break :blk false;
            for (part.values) |value| if (!sideLocal(columns, value)) break :blk false;
            break :blk true;
        },
    };
}
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
            // Binding already validated the discarded EXISTS expression.
            // It has no runtime dependencies and must not fetch cold payloads.
            if (std.mem.eql(u8, part.name, "$validate")) return;
            for (part.args) |arg| try markExpression(alloc, needed, arg);
            if (part.filter) |filter| try markExpression(alloc, needed, filter);
            if (part.window) |spec| {
                for (spec.partition) |item| try markExpression(alloc, needed, item);
                for (spec.order) |item| if (item.expression) |expression_| try markExpression(alloc, needed, expression_) else try needed.put(alloc, item.field, {});
            }
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
        .singleton, .recursive_ref, .literal_rows => {},
        .materialized_ref => |source| {
            // A materialized CTE stores its complete declared output once;
            // references can project different columns without changing its
            // producer's physical scan contract.
            for (source.columns) |column| try needed.put(builder.alloc, column.internal, {});
            try projectScans(builder, source, needed);
        },
        .recursive => |recursive| {
            // The working row is positional. Every recursive output is needed
            // to seed the next iteration even when the outer SELECT projects less.
            try projectScans(builder, recursive.seed, needed);
            try projectScans(builder, recursive.step, needed);
        },
        .scan => |scan| {
            var fields: std.ArrayList([]const u8) = .empty;
            const request = &builder.scans.items[scan.index];
            for (node.columns, scan.source_columns) |column, name| {
                if (!needed.contains(column.internal) or std.mem.eql(u8, name, "_id") or @import("joined_mutation.zig").isMetadata(name)) continue;
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
        .values => |arms| for (arms) |arm| try projectScans(builder, arm, needed),
    }
}

pub fn bind(alloc: Allocator, backend: catalog.Backend, statement: ast.Select, parameters: []?ast.ColumnType) anyerror!Bound {
    var builder: Builder = .{ .alloc = alloc, .backend = backend, .parameters = parameters, .node_limit = if (statement.generated_values) 8192 else 256 };
    if (std.mem.indexOfScalar(?ast.ColumnType, parameters, null) != null) {
        var shape: Builder = .{ .alloc = alloc, .backend = backend, .parameters = parameters, .shape_only = true, .node_limit = if (statement.generated_values) 8192 else 256 };
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
    var shape: Builder = .{ .alloc = alloc, .backend = backend, .parameters = parameters, .shape_only = true, .node_limit = if (statement.generated_values) 8192 else 256 };
    try shape.inferShape(statement, expected);
}

/// Normalize one expression against an already-bound relation. Mutation arms
/// use the same qualified-name and ambiguity rules as SELECT, then bind typed
/// programs against the projected internal column ordinals.
pub fn lowerBoundExpression(alloc: Allocator, columns: []const Column, expression_: *const ast.Scalar) !*const ast.Scalar {
    var builder: Builder = .{ .alloc = alloc, .backend = undefined, .parameters = &.{} };
    return builder.expression(columns, expression_, &.{});
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
