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

//! DISTINCT is a typed projection boundary, not a storage scan flag. Reuse
//! bounded grouping/window operators so deduplication precedes pagination and
//! retains source authorization, statement snapshots and ordinary spill paths.
const std = @import("std");
const ast = @import("ast.zig");
const compiler = @import("compiler.zig");
const catalog = @import("catalog.zig");
const describe = @import("describe.zig");
const relations = @import("relation_binding.zig");
const same = @import("aggregate_binding.zig").same;
const A = std.mem.Allocator;

fn node(a: A, value: ast.Scalar) !*const ast.Scalar {
    const result = try a.create(ast.Scalar);
    result.* = value;
    return result;
}
fn field(a: A, name: []const u8) !*const ast.Scalar {
    return node(a, .{ .column = name });
}
fn slot(a: A, index: usize) ![]const u8 {
    return std.fmt.allocPrint(a, "$distinct_source\x00$distinct_{d}", .{index});
}

fn expandedOutputs(a: A, original: ast.Select, domain: []const relations.Column) !ast.Select {
    var result = original;
    const input = if (original.columns.len == 0) &[_]ast.Projection{.{ .wildcard = true }} else original.columns;
    result.columns = try a.dupe(ast.Projection, try relations.expandWildcards(a, domain, input, null));
    for (@constCast(result.columns)) |*projection| if (projection.bound_column) |ordinal| {
        if (ordinal >= domain.len) return error.InvalidSqlProgram;
        const column = domain[ordinal];
        projection.expression = try field(a, if (column.qualifier.len == 0) column.name else try std.fmt.allocPrint(a, "{s}\x00{s}", .{ column.qualifier, column.name }));
        projection.bound_column = null;
    };
    return result;
}

fn functionName(name: []const u8) []const u8 {
    // The parser encodes qualification with NUL. A quoted identifier containing
    // a literal dot must remain distinct from a pg_catalog builtin reference.
    const prefix = "pg_catalog\x00";
    return if (std.mem.startsWith(u8, name, prefix)) name[prefix.len..] else name;
}

fn orderExpression(a: A, original: ast.Select, order: ast.Order, columns: []const describe.Column) !*const ast.Scalar {
    if (order.position) |position| {
        if (position == 0 or position > columns.len) return error.SqlDistinctOrderMismatch;
        if (original.columns.len == columns.len) {
            const projection = original.columns[position - 1];
            if (!projection.wildcard) return projection.expression orelse try field(a, projection.field);
        }
        return field(a, columns[position - 1].name);
    }
    return order.expression orelse try field(a, order.field);
}

fn selectedSlot(a: A, original: ast.Select, expression: *const ast.Scalar, columns: []const describe.Column, domain: []const relations.Column) !?usize {
    if (original.columns.len == columns.len) for (original.columns, 0..) |projection, index| {
        if (projection.wildcard) continue;
        if (try sameKey(domain, expression, projection.expression orelse try field(a, projection.field))) return index;
    };
    if (expression.* == .column) {
        var selected: ?usize = null;
        for (columns, 0..) |column, index| if (std.mem.eql(u8, expression.column, column.name)) {
            if (selected != null) return error.AmbiguousSqlColumn;
            selected = index;
        };
        return selected;
    }
    return null;
}

fn boundDomain(a: A, original: ast.Select, shape: describe.BoundStatement) ![]const relations.Column {
    if (shape.relation) |relation| return relation.root.columns;
    const table = shape.table orelse return &.{};
    const virtual = table.virtualColumns();
    const result = try a.alloc(relations.Column, table.columns.len + virtual.len);
    const name = original.table orelse return error.UnsupportedSqlShape;
    const scope = table.binding_scope orelse table.scope orelse if (name.namespace) |namespace| catalog.Table.Scope{ .database = name.database orelse "", .namespace = namespace, .name = name.table, .revision = 0 } else null;
    for (result, 0..) |*output, index| {
        const column = if (index < table.columns.len) table.columns[index] else virtual[index - table.columns.len];
        output.* = .{ .name = column.name, .internal = try std.fmt.allocPrint(a, "$distinct_binding_{d}", .{index}), .qualifier = name.table, .scope = scope, .type = column.type, .element_type = column.element_type, .numeric_modifier = column.numeric_modifier, .nullable = column.nullable, .visible = index < table.columns.len };
    }
    return result;
}

const Liveness = struct {
    domain: []const relations.Column,
    needed: []bool,
    pub fn column(self: *@This(), name: []const u8) !void {
        const resolved = try relations.lookupColumn(self.domain, name);
        for (self.domain, self.needed) |item, *needed| if (std.mem.eql(u8, item.internal, resolved.internal)) {
            needed.* = true;
            return;
        };
        return error.InvalidSqlProgram;
    }
    pub fn subquery(_: *@This(), _: *const ast.Select) !void {
        // Correlated child reads require the existing phase-scope dependency
        // adapter, not a guessed all-column projection or a hidden new read.
        return error.UnsupportedSqlShape;
    }
};

fn sameKey(domain: []const relations.Column, left: *const ast.Scalar, right: *const ast.Scalar) anyerror!bool {
    if (std.meta.activeTag(left.*) != std.meta.activeTag(right.*)) return false;
    return switch (left.*) {
        .column => std.mem.eql(u8, (try relations.lookupColumn(domain, left.column)).internal, (try relations.lookupColumn(domain, right.column)).internal),
        .literal => same(left, right),
        .unary => |part| part.op == right.unary.op and try sameKey(domain, part.operand, right.unary.operand),
        .binary => |part| part.op == right.binary.op and try sameKey(domain, part.left, right.binary.left) and try sameKey(domain, part.right, right.binary.right),
        .cast => |part| part.type == right.cast.type and part.element_type == right.cast.element_type and std.meta.eql(part.numeric_modifier, right.cast.numeric_modifier) and try sameKey(domain, part.operand, right.cast.operand),
        .call => |part| blk: {
            if (!std.mem.eql(u8, functionName(part.name), functionName(right.call.name)) or part.args.len != right.call.args.len) break :blk false;
            if (part.subquery != null or part.window != null or part.within_group != null or part.filter != null or part.star or part.distinct) break :blk same(left, right);
            for (part.args, right.call.args) |l, r| if (!try sameKey(domain, l, r)) break :blk false;
            break :blk true;
        },
        .case_when => |part| blk: {
            if (part.branches.len != right.case_when.branches.len or (part.otherwise == null) != (right.case_when.otherwise == null)) break :blk false;
            for (part.branches, right.case_when.branches) |l, r| if (!try sameKey(domain, l.condition, r.condition) or !try sameKey(domain, l.value, r.value)) break :blk false;
            break :blk if (part.otherwise) |other| try sameKey(domain, other, right.case_when.otherwise.?) else true;
        },
        .in_list => |part| blk: {
            if (part.negated != right.in_list.negated or part.values.len != right.in_list.values.len or !try sameKey(domain, part.operand, right.in_list.operand)) break :blk false;
            for (part.values, right.in_list.values) |l, r| if (!try sameKey(domain, l, r)) break :blk false;
            break :blk true;
        },
    };
}

fn lowerOn(a: A, original: ast.Select, columns: []const describe.Column, domain: []const relations.Column) anyerror!ast.Select {
    // DISTINCT ON follows SQL92 target-list alias/ordinal resolution, just like
    // a complete ORDER BY key. Nested expressions retain the source domain.
    var key_statement = original;
    const key_orders = try a.alloc(ast.Order, original.distinct.?.len);
    for (original.distinct.?, key_orders) |key, *order| order.* = .{ .expression = key };
    key_statement.order_by = key_orders;
    key_statement.order_aliases_expanded = false;
    key_statement = try @import("order_aliases.zig").normalize(a, key_statement);
    const keys = try a.alloc(*const ast.Scalar, key_orders.len);
    for (original.distinct.?, key_statement.order_by, keys) |source_key, order, *key| {
        const expression = order.expression.?;
        // Classify ordinals before alias expansion: an alias may legitimately
        // denote an integer constant rather than a target-list position.
        const integer: ?i64 = if (source_key.* == .literal) blk: {
            if (source_key.literal != .integer) return error.InvalidSqlSyntax;
            break :blk source_key.literal.integer;
        } else if (source_key.* == .unary and (source_key.unary.op == .positive or source_key.unary.op == .negative) and source_key.unary.operand.* == .literal and source_key.unary.operand.literal == .integer) blk: {
            const value = source_key.unary.operand.literal.integer;
            break :blk if (source_key.unary.op == .negative) std.math.negate(value) catch return error.SqlDistinctOrderMismatch else value;
        } else null;
        if (integer) |ordinal| {
            const position = std.math.cast(u32, ordinal) orelse return error.SqlDistinctOrderMismatch;
            key.* = try orderExpression(a, original, .{ .position = position }, columns);
        } else key.* = expression;
    }
    // Group/window phases require their own lexical forwarding domain. Do not
    // move erroring/volatile output expressions ahead of uniqueness merely to
    // accept a shape whose completed phase is not yet forwarded here.
    if (@import("aggregate_binding.zig").accepts(original) or @import("window_binding.zig").accepts(original) or original.count_all) return error.UnsupportedSqlShape;
    var raw = original;
    raw.distinct = null;
    raw.columns = &.{};
    raw.order_by = &.{};
    raw.limit = null;
    raw.offset = null;
    raw.scalar_cardinality_limit = false;
    if (raw.table) |table| {
        const source = try a.create(ast.Relation);
        source.* = .{ .table = .{ .name = table } };
        raw.source = source;
        raw.table = null;
    }
    const needed = try a.alloc(bool, domain.len);
    @memset(needed, false);
    var liveness: Liveness = .{ .domain = domain, .needed = needed };
    const outputs = if (original.columns.len == 0) &[_]ast.Projection{.{ .wildcard = true }} else original.columns;
    for (outputs) |projection| {
        if (projection.wildcard) {
            const expanded = try relations.expandWildcards(a, domain, &.{projection}, null);
            for (expanded) |item| needed[item.bound_column.?] = true;
        } else if (projection.bound_column) |ordinal| {
            if (ordinal >= needed.len) return error.InvalidSqlProgram;
            needed[ordinal] = true;
        } else try @import("phase_projection.zig").visitReferences(projection.expression orelse try field(a, projection.field), &.{}, &liveness);
    }
    for (keys) |key| try @import("phase_projection.zig").visitReferences(key, &.{}, &liveness);
    for (original.order_by) |order| try @import("phase_projection.zig").visitReferences(try orderExpression(a, original, order, columns), &.{}, &liveness);
    var fields: std.ArrayList(ast.Projection) = .empty;
    var refs: std.ArrayList(?[]const u8) = .empty;
    for (domain, needed) |column, live| {
        if (!live) continue;
        const name = if (column.qualifier.len == 0) column.name else try std.fmt.allocPrint(a, "{s}\x00{s}", .{ column.qualifier, column.name });
        try fields.append(a, .{ .field = name, .alias = try std.fmt.allocPrint(a, "$distinct_input_{d}", .{refs.items.len}) });
        try refs.append(a, name);
    }
    const input_orders = try a.alloc(ast.Order, original.order_by.len);
    const orders = try a.alloc(ast.Order, input_orders.len);
    for (original.order_by, input_orders) |order, *input| {
        input.* = order;
        input.position = null;
        input.field = "";
        input.expression = try orderExpression(a, original, order, columns);
    }
    const matched = try a.alloc(bool, keys.len);
    @memset(matched, false);
    var count: usize = 0;
    for (input_orders) |order| {
        var found = false;
        for (keys, matched) |key, *seen| if (try sameKey(domain, key, order.expression.?)) {
            if (!seen.*) count += 1;
            seen.* = true;
            found = true;
        };
        if (!found) {
            if (count != keys.len) return error.SqlDistinctOnOrderMismatch;
            break;
        }
    }
    try fields.append(a, .{ .expression = try node(a, .{ .call = .{ .name = "row_number", .args = &.{}, .window = .{ .partition = keys, .order = input_orders } } }), .alias = "$distinct_rank" });
    try refs.append(a, null);
    for (input_orders, orders, 0..) |input, *output, index| {
        const alias = try std.fmt.allocPrint(a, "$distinct_order_{d}", .{index});
        try fields.append(a, .{ .expression = input.expression, .alias = alias });
        try refs.append(a, null);
        output.* = .{ .field = try std.fmt.allocPrint(a, "$distinct_source\x00{s}", .{alias}), .descending = input.descending, .nulls_first = input.nulls_first };
    }
    const query = try a.create(ast.Select);
    query.* = raw;
    query.columns = try fields.toOwnedSlice(a);
    const phase = try a.create(ast.PhaseScope);
    phase.* = .{ .columns = try refs.toOwnedSlice(a), .grouped = false };
    const source = try a.create(ast.Relation);
    source.* = .{ .derived = .{ .query = query, .alias = "$distinct_source", .phase_scope = phase } };
    const predicate = try a.create(ast.Predicate);
    predicate.* = .{ .scalar = try node(a, .{ .binary = .{ .op = .eq, .left = try field(a, "$distinct_source\x00$distinct_rank"), .right = try node(a, .{ .literal = .{ .integer = 1 } }) } }) };
    var result = original;
    result.distinct = null;
    result.table = null;
    result.source = source;
    result.predicate = predicate;
    result.group_by = &.{};
    result.having = null;
    result.windows = &.{};
    result.ctes = &.{};
    result.order_by = orders;
    result.order_aliases_expanded = true;
    return result;
}

pub fn lower(a: A, backend: catalog.Backend, compiled: *const compiler.Compiled, parameters: []const ?ast.ColumnType) anyerror!ast.Select {
    const original = try @import("order_aliases.zig").normalize(a, compiled.statement.select);
    const keys = original.distinct orelse return error.InvalidSqlProgram;
    var inner = original;
    inner.distinct = null;
    inner.order_by = &.{};
    inner.limit = null;
    inner.offset = null;
    inner.scalar_cardinality_limit = false;
    var inspected = compiled.*;
    inspected.statement = .{ .select = inner };
    const shape = try describe.bindInternal(a, backend, &inspected, parameters);
    const width = shape.columns.len;
    if (width == 0 or width > 1024) return error.SqlProgramLimitExceeded;
    const domain = try boundDomain(a, original, shape);
    const expanded = try expandedOutputs(a, original, domain);
    if (keys.len != 0) return lowerOn(a, expanded, shape.columns, domain);
    const outputs = try a.alloc(ast.Projection, width);
    const groups = try a.alloc(*const ast.Scalar, if (keys.len == 0) width else 0);
    for (shape.columns, outputs, 0..) |column, *projection, index| {
        const name = try slot(a, index);
        projection.* = .{ .field = name, .alias = column.name };
        if (keys.len == 0) groups[index] = try field(a, name);
    }
    const orders = try a.alloc(ast.Order, original.order_by.len);
    const input_orders = try a.alloc(ast.Order, original.order_by.len);
    for (original.order_by, input_orders) |order, *input| {
        input.* = order;
        input.position = null;
        input.field = "";
        input.expression = try orderExpression(a, expanded, order, shape.columns);
    }
    for (input_orders, orders) |input, *output| {
        const index = try selectedSlot(a, expanded, input.expression.?, shape.columns, domain) orelse return error.SqlDistinctOrderMismatch;
        output.* = .{ .field = try slot(a, index), .descending = input.descending, .nulls_first = input.nulls_first };
    }
    const query = try a.create(ast.Select);
    query.* = inner;
    const aliases = try a.alloc([]const u8, width);
    for (aliases, 0..) |*alias, index| alias.* = try std.fmt.allocPrint(a, "$distinct_{d}", .{index});
    const source = try a.create(ast.Relation);
    source.* = .{ .derived = .{ .query = query, .alias = "$distinct_source", .columns = aliases, .hidden = true } };
    return .{
        .source = source,
        .columns = outputs,
        .group_by = groups,
        .order_by = orders,
        .order_aliases_expanded = true,
        .limit = original.limit,
        .offset = original.offset,
        .scalar_cardinality_limit = original.scalar_cardinality_limit,
    };
}

test "SQL DISTINCT ON preparation keeps wide cold schemas out of retained projection and scans" {
    const Fixture = struct {
        width: usize,
        reads: usize = 0,
        fn resolve(ptr: *anyopaque, a: A, _: ast.Name, _: catalog.Action) !catalog.Table {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const columns = try a.alloc(catalog.Column, self.width + 3);
            for ([_][]const u8{ "id", "g", "priority" }, columns[0..3]) |name, *column| column.* = .{ .name = name, .path = name, .type = .integer, .element_type = .int64, .nullable = false };
            for (columns[3..], 0..) |*column, index| {
                const name = try std.fmt.allocPrint(a, "cold_json_{d}", .{index});
                column.* = .{ .name = name, .path = name, .type = .json, .element_type = .jsonb };
            }
            return .{ .id = 7, .physical_name = "wide", .schema_version = 1, .columns = columns };
        }
        fn scan(ptr: *anyopaque, _: A, _: catalog.Table, _: catalog.Scan) !catalog.Page {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.reads += 1;
            return error.PreparationMustNotReadRows;
        }
        fn mutate(_: *anyopaque, _: A, _: A, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
            return error.PreparationMustNotMutate;
        }
        fn checkpoint(_: *anyopaque) !void {}
    };
    const a = std.testing.allocator;
    for ([_]usize{ 8, 512 }) |width| {
        var fixture: Fixture = .{ .width = width };
        const backend: catalog.Backend = .{ .ptr = &fixture, .vtable = &.{ .resolve = Fixture.resolve, .scan = Fixture.scan, .mutate = Fixture.mutate, .checkpoint = Fixture.checkpoint } };
        var compiled = try compiler.compile(a, "SELECT DISTINCT ON (g) id FROM wide ORDER BY g,priority DESC LIMIT 5", .{});
        defer compiled.deinit();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const staged = try lower(arena.allocator(), backend, &compiled, &.{});
        try std.testing.expectEqual(@as(usize, 6), staged.source.?.derived.query.columns.len);
        var prepared = try describe.describe(a, backend, &compiled, &.{});
        defer prepared.deinit();
        const scans = prepared.binding.relation.?.scans;
        try std.testing.expectEqual(@as(usize, 1), scans.len);
        try std.testing.expectEqual(@as(usize, 3), scans[0].request.fields.len);
        for (scans[0].request.fields) |name| try std.testing.expect(!std.mem.startsWith(u8, name, "cold_json_"));
        try std.testing.expectEqual(@as(usize, 0), fixture.reads);
        std.debug.print("SQL DISTINCT ON preparation: catalog_columns={} retained_input_columns=3 scan_fields=3\n", .{width + 3});
    }
}
