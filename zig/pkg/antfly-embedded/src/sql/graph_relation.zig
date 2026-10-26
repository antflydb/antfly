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

//! SQL/PGQ relation types and rows over the shared graph IR and visibility cut.
const std = @import("std");
const cypher = @import("cypher");
const ast = @import("ast.zig");
const catalog = @import("catalog.zig");
const scalar = @import("scalar.zig");
const A = std.mem.Allocator;
pub const Binding = struct {
    definition: ast.GraphTable,
    graph: cypher.catalog.Graph,
    catalog_json: []const u8,
    dependencies: []const catalog.Table,
};
pub fn columns(a: A, binding: Binding) ![]const catalog.Column {
    const clauses = binding.definition.query.clauses;
    if (clauses.len != 2 or clauses[1].operation != .project) return error.InvalidSqlGraphTable;
    const items = clauses[1].operation.project.value.items;
    const result = try a.alloc(catalog.Column, items.len);
    for (items, result, 0..) |item, *out, index| {
        const name = item.alias orelse return error.SqlGraphColumnRequiresAlias;
        for (items[0..index]) |prior| if (std.mem.eql(u8, prior.alias.?, name)) return error.DuplicateSqlColumn;
        out.* = .{ .name = name, .path = name, .type = expressionType(binding, item.expression), .nullable = true };
    }
    return result;
}
fn expressionType(binding: Binding, expr: *const cypher.ast.Expr) ast.ColumnType {
    return switch (expr.value) {
        .literal => |literal| switch (literal) {
            .string => .string,
            .integer => .integer,
            .number => .number,
            .boolean => .boolean,
            .null => .json,
        },
        .property => |property| blk: {
            if (property.base.value != .variable) break :blk .json;
            const variable = property.base.value.variable;
            const match = binding.definition.query.clauses[0].operation.match;
            for (match.paths) |path| {
                for (path.edges) |edge| if (edge.alias) |alias| if (std.mem.eql(u8, alias, variable)) {
                    // Cypher properties, including weight, can have arbitrary data
                    // types. No relationship property schema is declared here.
                    break :blk .json;
                };
                for (path.nodes) |node| if (node.alias) |alias| if (std.mem.eql(u8, alias, variable)) {
                    var kind: ?ast.ColumnType = null;
                    for (binding.graph.labels) |label| {
                        if (node.labels.len != 0) {
                            var found = false;
                            for (node.labels) |name| if (std.mem.eql(u8, name, label.name)) {
                                found = true;
                                break;
                            };
                            if (!found) continue;
                        }
                        for (binding.dependencies) |table| if (std.mem.eql(u8, table.physical_name, label.table)) {
                            const column = table.column(property.name) catch break :blk .json;
                            if (kind) |previous| if (previous != (if (column.type == .array) ast.ColumnType.json else column.type)) break :blk .json;
                            kind = if (column.type == .array) .json else column.type;
                        };
                    }
                    break :blk kind orelse .json;
                };
            }
            break :blk .json;
        },
        .call => |call| callType(binding, call),
        .binary => |binary| switch (binary.op) {
            .eq, .neq, .lt, .lte, .gt, .gte, .@"and", .@"or", .xor, .in, .starts_with, .ends_with, .contains, .regex => .boolean,
            else => blk: {
                const left = expressionType(binding, binary.left);
                const right = expressionType(binding, binary.right);
                if (binary.op == .add and ((left == .string and (right == .string or right == .integer or right == .number or right == .boolean)) or
                    (right == .string and (left == .integer or left == .number or left == .boolean)))) break :blk .string;
                if (left == .integer and right == .integer and binary.op != .power) break :blk .integer;
                if ((left == .integer or left == .number) and (right == .integer or right == .number)) break :blk .number;
                break :blk .json;
            },
        },
        .unary => |unary| switch (unary.op) {
            .not, .is_null, .is_not_null => .boolean,
            else => expressionType(binding, unary.operand),
        },
        .comparison, .list_predicate, .exists => .boolean,
        else => .json,
    };
}
fn callType(binding: Binding, call: cypher.ast.Function) ast.ColumnType {
    const name = call.name;
    for ([_][]const u8{ "count", "size", "length", "toInteger" }) |function| if (std.ascii.eqlIgnoreCase(name, function)) return .integer;
    for ([_][]const u8{ "elementId", "antfly.documentKey", "toString", "toLower", "toUpper", "trim", "ltrim", "rtrim", "substring", "type" }) |function| if (std.ascii.eqlIgnoreCase(name, function)) return .string;
    if (std.ascii.eqlIgnoreCase(name, "toBoolean")) return .boolean;
    if (std.ascii.eqlIgnoreCase(name, "toFloat") or std.ascii.eqlIgnoreCase(name, "avg")) return .number;
    if (call.args.len == 1) {
        const input = expressionType(binding, call.args[0]);
        if (std.ascii.eqlIgnoreCase(name, "min") or std.ascii.eqlIgnoreCase(name, "max")) return input;
        if (std.ascii.eqlIgnoreCase(name, "reverse") and input == .string) return .string;
        if (std.ascii.eqlIgnoreCase(name, "abs") or std.ascii.eqlIgnoreCase(name, "sum")) return if (input == .integer or input == .number) input else .json;
    }
    if (std.ascii.eqlIgnoreCase(name, "coalesce")) {
        var common: ?ast.ColumnType = null;
        for (call.args) |arg| {
            if (arg.value == .literal and arg.value.literal == .null) continue;
            const kind = expressionType(binding, arg);
            if (common) |previous| if (kind != previous) return .json;
            common = kind;
        }
        return common orelse .json;
    }
    return .json;
}
/// Graph entities retain their wire envelope at SQL's JSON boundary; ordinary
/// user maps remain ordinary maps, including keys named type or value.
fn json(a: A, value: cypher.value.Value) anyerror!std.json.Value {
    return switch (value) {
        .null => .null,
        .boolean => |item| .{ .bool = item },
        .integer => |item| .{ .integer = item },
        .number => |item| .{ .float = item },
        .string => |item| .{ .string = try a.dupe(u8, item) },
        .list => |items| blk: {
            var output = std.json.Array.init(a);
            for (items) |item| try output.append(try json(a, item));
            break :blk .{ .array = output };
        },
        .map => |entries| blk: {
            var output: std.json.ObjectMap = .empty;
            for (entries) |entry| try output.put(a, try a.dupe(u8, entry.name), try json(a, entry.value));
            break :blk .{ .object = output };
        },
        .node, .relationship, .path => try std.json.parseFromSliceLeaky(std.json.Value, a, try std.json.Stringify.valueAlloc(a, cypher.wire.Value{ .value = value }, .{}), .{ .allocate = .alloc_always, .parse_numbers = false }),
    };
}
// A scan predicate is SQL-typed. Push only comparisons whose direct scalar
// projection has the same Cypher domain, retaining SQL residual checks below.
// Aggregation, DISTINCT, ordering and path selection remain semantic barriers.
fn graphComparison(a: A, binding: Binding, original: cypher.ast.Projection, condition: catalog.Condition, allow_null: bool) !?*cypher.ast.Expr {
    const item = for (original.items) |candidate| {
        if (std.mem.eql(u8, candidate.alias.?, condition.column)) break candidate;
    } else return null;
    if (item.expression.value != .property) return null;
    const kind = expressionType(binding, item.expression);
    if (kind != .integer and kind != .number and kind != .string and kind != .boolean) return null;
    const datum = condition.datum();
    if (datum.numeric != null or datum.array != null or datum.patterns != null) return null;
    const predicate = try a.create(cypher.ast.Expr);
    const span = item.expression.span;
    if (condition.op == .is_null or condition.op == .is_not_null) {
        predicate.* = .{ .span = span, .depth = item.expression.depth + 1, .value = .{ .unary = .{ .op = if (condition.op == .is_null) .is_null else .is_not_null, .operand = item.expression } } };
    } else {
        if (datum.sql_null and !allow_null) return null;
        const literal: cypher.ast.Value = if (datum.sql_null) .null else switch (datum.value) {
            .integer => |value| if (kind == .integer or kind == .number) .{ .integer = value } else return null,
            .number_string => |value| if (kind == .integer or kind == .number) .{ .integer = std.fmt.parseInt(i64, value, 10) catch return null } else return null,
            .float => |value| if (kind == .number and std.math.isFinite(value)) .{ .number = value } else return null,
            .string => |value| if (kind == .string) .{ .string = try a.dupe(u8, value) } else return null,
            .bool => |value| if (kind == .boolean and (condition.op == .eq or condition.op == .neq)) .{ .boolean = value } else return null,
            else => return null,
        };
        const operand = try a.create(cypher.ast.Expr);
        operand.* = .{ .span = span, .value = .{ .literal = literal } };
        predicate.* = .{ .span = span, .depth = item.expression.depth + 1, .value = .{ .binary = .{ .left = item.expression, .right = operand, .op = switch (condition.op) {
            .eq => .eq,
            .neq => .neq,
            .lt => .lt,
            .lte => .lte,
            .gt => .gt,
            .gte => .gte,
            else => unreachable,
        } } } };
    }
    return predicate;
}
fn combinePredicate(a: A, left: *const cypher.ast.Expr, right: *const cypher.ast.Expr, op: cypher.ast.Binary) !?*cypher.ast.Expr {
    const depth = 1 + @max(left.depth, right.depth);
    if (depth > 64) return null;
    const combined = try a.create(cypher.ast.Expr);
    combined.* = .{ .span = left.span, .depth = depth, .value = .{ .binary = .{ .op = op, .left = left, .right = right } } };
    return combined;
}
fn membershipTerms(a: A, binding: Binding, original: cypher.ast.Projection, filter: catalog.Scan.GraphFilter, property: *?*const cypher.ast.Expr, values: *std.ArrayList(*const cypher.ast.Expr)) anyerror!bool {
    switch (filter) {
        .any => |children| {
            for (children) |child| if (!try membershipTerms(a, binding, original, child, property, values)) return false;
            return true;
        },
        .comparison => |condition| {
            if (condition.op != .eq) return false;
            const expression = (try graphComparison(a, binding, original, condition, true)) orelse return false;
            const binary = expression.value.binary;
            if (property.*) |previous| {
                if (previous != binary.left) return false;
            } else property.* = binary.left;
            try values.append(a, binary.right);
            return true;
        },
        else => return false,
    }
}
fn graphFilter(a: A, binding: Binding, original: cypher.ast.Projection, filter: catalog.Scan.GraphFilter) anyerror!?*cypher.ast.Expr {
    if (filter == .any) {
        var property: ?*const cypher.ast.Expr = null;
        var values: std.ArrayList(*const cypher.ast.Expr) = .empty;
        if (try membershipTerms(a, binding, original, filter, &property, &values) and property != null) {
            const list = try a.create(cypher.ast.Expr);
            list.* = .{ .span = property.?.span, .value = .{ .list = try values.toOwnedSlice(a) } };
            const membership = try a.create(cypher.ast.Expr);
            membership.* = .{ .span = property.?.span, .depth = property.?.depth + 1, .value = .{ .binary = .{ .op = .in, .left = property.?, .right = list } } };
            return membership;
        }
    }
    return switch (filter) {
        .comparison => |condition| graphComparison(a, binding, original, condition, true),
        .all, .any => |children| blk: {
            if (children.len == 0) {
                const literal = try a.create(cypher.ast.Expr);
                literal.* = .{ .span = .{}, .value = .{ .literal = .{ .boolean = filter == .all } } };
                break :blk literal;
            }
            var expression = (try graphFilter(a, binding, original, children[0])) orelse break :blk null;
            for (children[1..]) |child| {
                const right = (try graphFilter(a, binding, original, child)) orelse break :blk null;
                expression = (try combinePredicate(a, expression, right, if (filter == .all) .@"and" else .@"or")) orelse break :blk null;
            }
            break :blk expression;
        },
    };
}
fn filterNeeds(filter: catalog.Scan.GraphFilter, column: []const u8) bool {
    return switch (filter) {
        .comparison => |condition| std.mem.eql(u8, condition.column, column),
        .all, .any => |children| for (children) |child| {
            if (filterNeeds(child, column)) break true;
        } else false,
    };
}
fn scanQuery(a: A, binding: Binding, request: catalog.Scan) !cypher.ast.Query {
    var consumed = false;
    return scanQueryCertified(a, binding, request, &consumed);
}
fn exactFilterDomain(binding: Binding, original: cypher.ast.Projection, filter: catalog.Scan.GraphFilter) bool {
    return switch (filter) {
        .comparison => |condition| for (original.items) |item| {
            if (!std.mem.eql(u8, item.alias.?, condition.column)) continue;
            break switch (expressionType(binding, item.expression)) {
                .integer, .string, .boolean => true,
                else => false,
            };
        } else false,
        .all, .any => |children| for (children) |child| {
            if (!exactFilterDomain(binding, original, child)) break false;
        } else true,
    };
}
fn scanQueryCertified(a: A, binding: Binding, request: catalog.Scan, consumed: *bool) !cypher.ast.Query {
    consumed.* = false;
    var query = binding.definition.query;
    if (query.clauses.len != 2 or query.clauses[0].operation != .match or query.clauses[1].operation != .project) return query;
    const original = query.clauses[1].operation.project.value;
    if (original.distinct or original.wildcard or original.order.len != 0 or original.skip != null or original.limit != null or original.predicate != null) return query;
    const grouped = for (original.items) |item| {
        if (cypher.expressions.aggregates(item.expression)) break true;
    } else false;
    if (grouped) {
        if (request.graph_source_ordinals or request.primary_key != null or request.after != null) return query;
        for (request.fields) |field| if (std.mem.eql(u8, field, "_id")) return query;
        for (request.conditions) |condition| if (std.mem.eql(u8, condition.column, "_id")) return query;
        for (request.graph_filters) |filter| if (filterNeeds(filter, "_id")) return query;
        var order: std.ArrayList(cypher.ast.Order) = .empty;
        for (original.items) |item| if (!cypher.expressions.aggregates(item.expression)) {
            switch (expressionType(binding, item.expression)) {
                .integer, .string, .boolean => {},
                else => return query,
            }
            if (item.expression.value != .property and item.expression.value != .literal and item.expression.value != .parameter) return query;
            const alias = try a.create(cypher.ast.Expr);
            alias.* = .{ .span = item.expression.span, .value = .{ .variable = item.alias.? } };
            try order.append(a, .{ .expression = alias });
        };
        if (order.items.len == 0) return query;
        const clauses = try a.dupe(cypher.ast.Clause, query.clauses);
        clauses[1].operation.project.value.order = order.items;
        if (request.ordered_window) |maximum| bounded: {
            if (maximum == 0 or maximum > std.math.maxInt(i64) or request.order.len != order.items.len or request.conditions.len != 0 or request.graph_filters.len != 0 or request.graph_conditions.len != 0 or request.graph_predicates.len != 0 or request.exact_graph_filter != null) break :bounded;
            const requested = try a.alloc(cypher.ast.Order, request.order.len);
            var seen: std.StringHashMapUnmanaged(void) = .empty;
            for (request.order, requested) |key, *sort| {
                if (key.nulls_first != key.descending or seen.contains(key.column)) break :bounded;
                const item = for (original.items) |item| {
                    if (std.mem.eql(u8, item.alias.?, key.column) and !cypher.expressions.aggregates(item.expression)) break item;
                } else break :bounded;
                try seen.put(a, key.column, {});
                const alias = try a.create(cypher.ast.Expr);
                alias.* = .{ .span = item.expression.span, .value = .{ .variable = item.alias.? } };
                sort.* = .{ .expression = alias, .descending = key.descending };
            }
            const limit = try a.create(cypher.ast.Expr);
            limit.* = .{ .span = .{}, .value = .{ .literal = .{ .integer = @intCast(maximum) } } };
            clauses[1].operation.project.value.order = requested;
            clauses[1].operation.project.value.limit = limit;
        }
        query.clauses = clauses;
        return query;
    }
    const clauses = try a.dupe(cypher.ast.Clause, query.clauses);
    const operation = &clauses[0].operation.match;
    var push = !operation.optional and !request.graph_source_ordinals and request.primary_key == null and request.after == null;
    for (request.fields) |field| if (std.mem.eql(u8, field, "_id")) {
        push = false;
    };
    for (operation.paths) |path| if (path.selector != .all) {
        push = false;
    };
    if (push) if (request.exact_graph_filter) |filter| exact: {
        if (!exactFilterDomain(binding, original, filter)) break :exact;
        const predicate = (try graphFilter(a, binding, original, filter)) orelse break :exact;
        operation.predicate = if (operation.predicate) |previous| (try combinePredicate(a, previous, predicate, .@"and")) orelse break :exact else predicate;
        consumed.* = true;
    };
    if (push and !consumed.*) {
        for (request.conditions) |condition| {
            const predicate = (try graphComparison(a, binding, original, condition, false)) orelse continue;
            operation.predicate = if (operation.predicate) |previous| (try combinePredicate(a, previous, predicate, .@"and")) orelse continue else predicate;
        }
        for (request.graph_filters) |filter| {
            const predicate = (try graphFilter(a, binding, original, filter)) orelse continue;
            operation.predicate = if (operation.predicate) |previous| (try combinePredicate(a, previous, predicate, .@"and")) orelse continue else predicate;
        }
    }
    var items: std.ArrayList(cypher.ast.Item) = .empty;
    for (original.items) |item| {
        const name = item.alias.?;
        var needed = false;
        for (request.fields) |field| if (std.mem.eql(u8, name, field)) {
            needed = true;
        };
        for (request.order) |order| if (std.mem.eql(u8, name, order.column)) {
            needed = true;
        };
        for (request.conditions) |condition| if (std.mem.eql(u8, name, condition.column)) {
            needed = true;
        };
        for (request.graph_filters) |filter| if (filterNeeds(filter, name)) {
            needed = true;
        };
        if (needed) try items.append(a, item);
    }
    if (items.items.len == 0) {
        const expression = try a.create(cypher.ast.Expr);
        expression.* = .{ .span = clauses[1].span, .value = .{ .literal = .{ .integer = 1 } } };
        try items.append(a, .{ .alias = "_antfly_scan_row", .expression = expression });
    }
    clauses[1].operation.project.value.items = try items.toOwnedSlice(a);
    query.clauses = clauses;
    return query;
}
pub const Cursor = struct {
    a: A,
    arena: std.heap.ArenaAllocator,
    result: ?cypher.runtime.Result = null,
    stream: ?*cypher.runtime.NodeCursor = null,
    request: catalog.Scan,
    position: usize = 0,
    resume_position: usize = 0,
    optimized: bool = true,
    ordered: bool = false,
    empty: bool = false,
    weight_passes: []const WeightPass = &.{},
    weight_pass: usize = 0,
    scalar_fields: []const []const u8 = &.{},
    scalar_binding: Binding,
    scalar_admission: ?*const cypher.ast.Expr = null,
    scalar_admission_valid: bool = false,
    groups: ?*GroupState = null,
    const WeightPass = struct { plan: cypher.runtime.NodeCursor.WeightPlan, slots: []const usize };
    pub fn open(a: A, binding: Binding, backend: cypher.backend.Backend, parameters: []const cypher.value.Entry, request: catalog.Scan, limits: cypher.runtime.Limits) !catalog.Cursor {
        const self = try a.create(Cursor);
        errdefer a.destroy(self);
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const owned = arena.allocator();
        // Observer negotiation happens after open returns. Retain its AST and
        // scalar type environment in the cursor, independently of caller arenas.
        const scalar_query = try std.json.parseFromSliceLeaky(cypher.ast.Query, owned, try std.json.Stringify.valueAlloc(owned, binding.definition.query, .{}), .{});
        const scalar_graph = try binding.graph.clone(owned);
        const scalar_dependencies = try owned.alloc(catalog.Table, binding.dependencies.len);
        for (binding.dependencies, scalar_dependencies) |table, *copy| {
            const copied_columns = try owned.alloc(catalog.Column, table.columns.len);
            for (table.columns, copied_columns) |column, *out| {
                out.* = column;
                out.name = try owned.dupe(u8, column.name);
                out.path = try owned.dupe(u8, column.path);
            }
            copy.* = .{ .id = table.id, .physical_name = try owned.dupe(u8, table.physical_name), .schema_version = table.schema_version, .columns = copied_columns };
        }
        var scalar_binding = binding;
        scalar_binding.definition.query = scalar_query;
        scalar_binding.graph = scalar_graph;
        scalar_binding.dependencies = scalar_dependencies;
        const fields = try owned.alloc([]const u8, request.fields.len);
        for (request.fields, fields) |field, *out| out.* = try owned.dupe(u8, field);
        const conditions = try owned.alloc(catalog.Condition, request.conditions.len);
        for (request.conditions, conditions) |condition, *out| out.* = .{ .column = try owned.dupe(u8, condition.column), .op = condition.op, .value = try @import("../storage/typed_json.zig").clone(owned, condition.value), .operand = try @import("operators.zig").cloneDatum(owned, condition.datum()) };
        const ordered_resume = if (request.after) |after| std.mem.startsWith(u8, after, "o:") else false;
        const optimized = if (request.after) |after| std.mem.startsWith(u8, after, "g:") or ordered_resume else true;
        const resume_position = if (request.after) |after| try std.fmt.parseUnsigned(usize, if (optimized) after[2..] else after, 10) else 0;
        var query_request = request;
        // Tagged tokens resume the same pruned/pushed relation. Legacy ordinal
        // tokens retain their original unfiltered source interpretation.
        if (optimized) query_request.after = null;
        var consumed_filter = false;
        var compiled: cypher.compiler.Compiled = .{ .arena = std.heap.ArenaAllocator.init(a), .query = try scanQueryCertified(owned, binding, query_request, &consumed_filter), .parameters = binding.definition.parameters };
        defer compiled.deinit();
        // Every ordinary comparison with SQL NULL is UNKNOWN. A conjunction
        // containing one can emit no rows, even across graph semantic barriers.
        const empty = for (conditions) |condition| {
            if (condition.op != .is_null and condition.op != .is_not_null and condition.datum().sql_null) break true;
        } else false;
        const stream = if (empty) null else try cypher.runtime.NodeCursor.open(a, backend, binding.graph, compiled.query, parameters, limits);
        errdefer if (stream) |cursor| cursor.close();
        var ordered = stream == null and compiled.query.clauses[1].operation.project.value.limit != null and binding.definition.query.clauses[1].operation.project.value.limit == null;
        var source_ordinals = request.graph_source_ordinals;
        for (request.fields) |field| source_ordinals = source_ordinals or std.mem.eql(u8, field, "_id");
        for (request.conditions) |condition| source_ordinals = source_ordinals or std.mem.eql(u8, condition.column, "_id");
        for (request.graph_filters) |filter| source_ordinals = source_ordinals or filterNeeds(filter, "_id");
        if (stream) |cursor| if (request.order.len != 0 and (request.after == null or ordered_resume) and request.primary_key == null and !source_ordinals) {
            var order: std.ArrayList(cypher.backend.PropertyOrder) = .empty;
            var compatible = true;
            for (request.order) |key| {
                if (key.nulls_first != key.descending) {
                    compatible = false;
                    break;
                }
                const item = for (binding.definition.query.clauses[1].operation.project.value.items) |item| {
                    if (std.mem.eql(u8, item.alias.?, key.column)) break item;
                } else {
                    compatible = false;
                    break;
                };
                switch (expressionType(binding, item.expression)) {
                    .integer, .string, .boolean => {},
                    else => {
                        compatible = false;
                        break;
                    },
                }
                try order.append(owned, .{ .property = key.column, .descending = key.descending });
            }
            if (compatible) {
                ordered = try cursor.certifyOrder(order.items);
                if (!ordered and request.primary_key == null and request.after == null and (if (request.exact_graph_filter != null) consumed_filter else (request.conditions.len == 0 and request.graph_conditions.len == 0 and request.graph_predicates.len == 0 and request.graph_filters.len == 0))) if (request.ordered_window) |maximum| {
                    ordered = try cursor.certifyWindow(order.items, maximum);
                };
            }
        };
        if (ordered_resume and !ordered) return error.InvalidSqlBackendResponse;
        var result: ?cypher.runtime.Result = if (!empty and stream == null) try cypher.runtime.execute(a, backend, binding.graph, &compiled, parameters, limits) else null;
        errdefer if (result) |*value| value.deinit();
        var copied = request;
        copied.fields = fields;
        copied.conditions = conditions;
        copied.graph_filters = &.{};
        copied.exact_graph_filter = null;
        copied.after = null;
        copied.primary_key = if (request.primary_key) |key| try owned.dupe(u8, key) else null;
        if ((empty and resume_position != 0) or (result != null and resume_position > result.?.rows.len)) return error.InvalidSqlBackendResponse;
        var scalar_fields: std.ArrayList([]const u8) = .empty;
        for (binding.definition.query.clauses[1].operation.project.value.items) |item| {
            if (item.expression.value != .property) continue;
            switch (expressionType(binding, item.expression)) {
                .integer, .string, .boolean => try scalar_fields.append(owned, try owned.dupe(u8, item.alias.?)),
                else => {},
            }
        }
        var scalar_admission = if (compiled.query.clauses[0].operation.match.predicate) |predicate| try std.json.parseFromSliceLeaky(*const cypher.ast.Expr, owned, try std.json.Stringify.valueAlloc(owned, predicate, .{}), .{}) else null;
        var scalar_admission_valid = request.graph_conditions.len == 0 and request.graph_predicates.len == 0;
        const original = scalar_binding.definition.query.clauses[1].operation.project.value;
        for (request.conditions) |condition| {
            const filter: catalog.Scan.GraphFilter = .{ .comparison = condition };
            if (!exactFilterDomain(scalar_binding, original, filter)) {
                scalar_admission_valid = false;
                break;
            }
            const predicate = (try graphFilter(owned, scalar_binding, original, filter)) orelse {
                scalar_admission_valid = false;
                break;
            };
            scalar_admission = (if (scalar_admission) |previous| try combinePredicate(owned, previous, predicate, .@"and") else predicate) orelse {
                scalar_admission_valid = false;
                break;
            };
        }
        for (request.graph_filters) |filter| {
            if (!exactFilterDomain(scalar_binding, original, filter)) {
                scalar_admission_valid = false;
                break;
            }
            const predicate = (try graphFilter(owned, scalar_binding, original, filter)) orelse {
                scalar_admission_valid = false;
                break;
            };
            scalar_admission = (if (scalar_admission) |previous| try combinePredicate(owned, previous, predicate, .@"and") else predicate) orelse {
                scalar_admission_valid = false;
                break;
            };
        }
        self.* = .{ .scalar_binding = scalar_binding, .scalar_admission = scalar_admission, .scalar_admission_valid = scalar_admission_valid, .scalar_fields = scalar_fields.items, .a = a, .arena = arena, .result = result, .stream = stream, .request = copied, .position = if (stream != null) 0 else resume_position, .resume_position = resume_position, .optimized = optimized, .ordered = ordered, .empty = empty };
        return .{ .order_satisfied = ordered, .ptr = self, .next = next, .close = close, .count_rows = countRows, .enable_weights = enableWeights, .enable_groups = enableGroups, .enable_aggregate_weights = enableAggregateWeights, .estimated_rows = if (empty) 0 else if (result) |value| value.rows.len else null };
    }
    const GroupState = struct {
        source: *cypher.runtime.NodeCursor.ScalarGroups,
        orders: []const cypher.runtime.NodeCursor.GroupOrder,
        fn skip(ptr: *anyopaque, maximum: usize) !usize {
            const self: *GroupState = @ptrCast(@alignCast(ptr));
            return self.source.skip(maximum);
        }
        fn closeGroups(_: *anyopaque) void {} // Parent cursor retains the cut.
        fn nextGroups(ptr: *anyopaque, a: A, maximum: usize, _: bool) !?@import("operators.zig").GroupBatch {
            if (maximum == 0) return error.InvalidSqlBackendResponse;
            const self: *GroupState = @ptrCast(@alignCast(ptr));
            const group = (try self.source.next(a)) orelse return null;
            const keys = try a.alloc(scalar.Datum, group.keys.len);
            for (group.keys, self.orders) |value, order| keys[order.key] = scalar.Datum.fromJson(try json(a, value));
            const values = try a.alloc(scalar.Datum, group.values.len);
            for (group.values, values) |value, *out| out.* = scalar.Datum.fromJson(try json(a, value));
            return .{ .keys = .{ .rows = try a.dupe([]const scalar.Datum, &.{keys}) }, .aggregates = .{ .rows = try a.dupe([]const scalar.Datum, &.{values}) }, .ordinals = try a.dupe(u64, &.{self.source.ordinal - 1}) };
        }
    };
    fn enableGroups(ptr: *anyopaque, requests: []const catalog.AggregateWeight, orders: []const cypher.runtime.NodeCursor.GroupOrder, grouped: *@import("operators.zig").Grouped) !bool {
        const self: *Cursor = @ptrCast(@alignCast(ptr));
        if (self.position != 0 or self.resume_position != 0 or self.request.primary_key != null or self.request.graph_source_ordinals or self.weight_passes.len != 0 or self.groups != null or !self.scalar_admission_valid or requests.len == 0) return false;
        const stream = self.stream orelse return false;
        for (self.request.fields) |field| if (std.mem.eql(u8, field, "_id")) return false;
        const a = self.arena.allocator();
        const keys = requests[0].observer_keys;
        if (keys.len == 0 or keys.len != orders.len) return false;
        for (keys) |key| {
            if (std.mem.eql(u8, key, "_id")) return false;
            const compatible = for (self.scalar_fields) |field| {
                if (std.mem.eql(u8, field, key)) break true;
            } else false;
            if (!compatible) return false;
        }
        const reducers = try a.alloc(cypher.runtime.NodeCursor.GroupReducer, requests.len);
        var source: ?*const cypher.ast.Expr = null;
        const original = self.scalar_binding.definition.query.clauses[1].operation.project.value;
        for (requests, reducers, 0..) |request, *reducer, index| {
            if (request.observer == null or request.observer_field == null or request.multiplicity != .presence or request.observer_keys.len != keys.len) return false;
            for (keys, request.observer_keys) |left, right| if (!std.mem.eql(u8, left, right)) return false;
            const field = request.observer_field.?;
            const compatible = for (self.scalar_fields) |candidate| {
                if (std.mem.eql(u8, candidate, field)) break true;
            } else false;
            if (!compatible) return false;
            var predicate = self.scalar_admission;
            if (request.observer_filter) |filter| {
                if (!exactFilterDomain(self.scalar_binding, original, filter)) return false;
                const admission = (try graphFilter(a, self.scalar_binding, original, filter)) orelse return false;
                predicate = (if (predicate) |prior| try combinePredicate(a, prior, admission, .@"and") else admission) orelse return false;
            }
            if (index == 0) source = predicate else {
                if ((source == null) != (predicate == null)) return false;
                if (source) |prior| if (!cypher.expressions.same(prior, predicate.?)) return false;
            }
            if (request.observer_reducer_filter) |filter| {
                if (!exactFilterDomain(self.scalar_binding, original, filter)) return false;
                const admission = (try graphFilter(a, self.scalar_binding, original, filter)) orelse return false;
                predicate = (if (predicate) |prior| try combinePredicate(a, prior, admission, .@"and") else admission) orelse return false;
            }
            reducer.* = .{ .field = field, .kind = request.observer.?, .predicate = predicate };
        }
        const scalar_groups = (try stream.planScalarGroups(keys, orders, reducers, source)) orelse return false;
        if (requests[0].observer_window) |window| if (try stream.preferScalarGroupScan(window.maximum, window.retained_bytes, keys.len, reducers.len, source)) {
            scalar_groups.deinit();
            return false;
        };
        const state = try a.create(GroupState);
        state.* = .{ .source = scalar_groups, .orders = try a.dupe(cypher.runtime.NodeCursor.GroupOrder, orders) };
        try grouped.adoptFinalized(.{ .ptr = state, .count = std.math.maxInt(usize), .exact_count = false, .ordered = true, .batch_rows = 1, .skip = GroupState.skip, .next = GroupState.nextGroups, .close = GroupState.closeGroups }, std.math.maxInt(usize));
        self.groups = state;
        return true;
    }
    fn enableAggregateWeights(ptr: *anyopaque, requests: []const catalog.AggregateWeight) !bool {
        const self: *Cursor = @ptrCast(@alignCast(ptr));
        if (self.position != 0 or self.resume_position != 0 or self.request.primary_key != null or self.request.graph_source_ordinals or self.weight_passes.len != 0) return false;
        if (requests.len == 0 or requests.len > 256) return false;
        for (self.request.fields) |field| if (std.mem.eql(u8, field, "_id")) return false;
        for (self.request.conditions) |condition| if (std.mem.eql(u8, condition.column, "_id")) return false;
        const stream = self.stream orelse return false;
        const a = self.arena.allocator();
        var fields: [2]std.StringHashMapUnmanaged(void) = .{ .empty, .empty };
        var slots: [2]std.ArrayList(usize) = .{ .empty, .empty };
        var multiplicities: [2]cypher.runtime.Multiplicity = .{ .presence, .presence };
        const Candidate = struct { plan: cypher.runtime.NodeCursor.WeightPlan, slots: std.ArrayList(usize) = .empty, presence_keys: ?[]const []const u8 = null, presence_predicate: ?*const cypher.ast.Expr = null };
        var candidates: std.ArrayList(Candidate) = .empty;
        var window: ?catalog.ObserverWindow = null;
        var window_keys: []const []const u8 = &.{};
        var window_predicate: ?*const cypher.ast.Expr = null;
        for (requests, 0..) |request, i| {
            const multiplicity = request.multiplicity;
            if (request.observer) |observer| scalar: {
                if (!self.scalar_admission_valid or multiplicity != .presence) break :scalar;
                const selected = request.observer_field orelse if (request.fields.len == 1) request.fields[0] else break :scalar;
                var compatible = false;
                for (self.scalar_fields) |field| if (std.mem.eql(u8, field, selected)) {
                    compatible = true;
                    break;
                };
                if (!compatible) break :scalar;
                for (request.observer_keys) |key| {
                    const valid = for (self.scalar_fields) |field| {
                        if (std.mem.eql(u8, field, key)) break true;
                    } else false;
                    if (!valid) break :scalar;
                }
                var predicate = self.scalar_admission;
                if (request.observer_filter) |filter| {
                    const original = self.scalar_binding.definition.query.clauses[1].operation.project.value;
                    if (!exactFilterDomain(self.scalar_binding, original, filter)) break :scalar;
                    const admission = (try graphFilter(a, self.scalar_binding, original, filter)) orelse break :scalar;
                    predicate = (if (predicate) |previous| try combinePredicate(a, previous, admission, .@"and") else admission) orelse break :scalar;
                }
                const group_predicate = predicate;
                if (request.observer_reducer_filter) |filter| {
                    const original = self.scalar_binding.definition.query.clauses[1].operation.project.value;
                    if (!exactFilterDomain(self.scalar_binding, original, filter)) break :scalar;
                    const admission = (try graphFilter(a, self.scalar_binding, original, filter)) orelse break :scalar;
                    predicate = (if (predicate) |previous| try combinePredicate(a, previous, admission, .@"and") else admission) orelse break :scalar;
                }
                const plan = (try stream.planScalarDomainFiltered(selected, request.observer_keys, observer, predicate, group_predicate, if (request.observer_window) |demand| demand.descending else &.{}, request.observer_reducer_filter != null and request.observer_keys.len != 0)) orelse break :scalar;
                // Preserve groups even when every per-reducer FILTER rejects.
                // Only idempotent reducers receive representative contributions.
                if (request.observer_reducer_filter != null and request.observer_keys.len != 0) {
                    const presence = (try stream.planScalarDomainFiltered(request.observer_keys[0], request.observer_keys, .distinct, group_predicate, group_predicate, if (request.observer_window) |demand| demand.descending else &.{}, false)) orelse break :scalar;
                    const shared = for (candidates.items, 0..) |candidate, index| {
                        const keys = candidate.presence_keys orelse continue;
                        if (keys.len != request.observer_keys.len) continue;
                        const same_keys = for (keys, request.observer_keys) |left, right| {
                            if (!std.mem.eql(u8, left, right)) break false;
                        } else true;
                        if (!same_keys or (candidate.presence_predicate == null) != (group_predicate == null)) continue;
                        if (group_predicate) |predicate_| if (!cypher.expressions.same(candidate.presence_predicate.?, predicate_)) continue;
                        break index;
                    } else null;
                    if (shared) |index| {
                        try candidates.items[index].slots.append(a, i);
                    } else {
                        var candidate: Candidate = .{ .plan = presence, .presence_keys = request.observer_keys, .presence_predicate = group_predicate };
                        try candidate.slots.append(a, i);
                        try candidates.append(a, candidate);
                    }
                }
                if (request.observer_window) |demand| {
                    window = demand;
                    window_keys = request.observer_keys;
                    window_predicate = group_predicate;
                }
                var candidate: Candidate = .{ .plan = plan };
                try candidate.slots.append(a, i);
                try candidates.append(a, candidate);
                continue;
            }
            const summary_plan = selected: {
                for (request.summaries) |certificate| {
                    if (certificate.joint) {
                        for (request.fields) |field| if (try stream.planSummaryFieldsChecked(request.fields, multiplicity, field, certificate.invariants, true, certificate.linear)) |plan| break :selected plan;
                    } else if (try stream.planSummaryFields(request.fields, multiplicity, certificate.field, certificate.invariants)) |plan| break :selected plan;
                }
                if (request.summary) |field| break :selected try stream.planSummaryFields(request.fields, multiplicity, field, request.invariants);
                break :selected @as(?cypher.runtime.NodeCursor.WeightPlan, null);
            };
            {
                if (summary_plan) |plan| {
                    var selected: ?usize = null;
                    for (candidates.items, 0..) |*candidate, position| {
                        if (plan.statistic != null and candidate.plan.statistic != null) {
                            if (try stream.mergeStatisticPlans(candidate.plan, plan)) |merged| {
                                candidate.plan = merged;
                                selected = position;
                                break;
                            }
                            continue;
                        }
                        if (plan.statistic != null or candidate.plan.statistic != null or candidate.plan.summary == null or !std.mem.eql(u8, candidate.plan.summary.?, plan.summary.?) or
                            candidate.plan.linear != plan.linear or candidate.plan.total_axis != plan.total_axis or candidate.plan.path.execution_reversed != plan.path.execution_reversed or candidate.plan.projection.items.len != plan.projection.items.len) continue;
                        const same_fields = for (plan.projection.items) |item| {
                            const name = item.alias.?;
                            const present = for (candidate.plan.projection.items) |other| {
                                if (std.mem.eql(u8, other.alias.?, name)) break true;
                            } else false;
                            if (!present) break false;
                        } else true;
                        if (same_fields) {
                            selected = position;
                            break;
                        }
                    }
                    if (selected == null) {
                        selected = candidates.items.len;
                        try candidates.append(a, .{ .plan = plan });
                    }
                    const candidate = &candidates.items[selected.?];
                    try candidate.slots.append(a, i);
                    if (multiplicity == .exact or (multiplicity == .capped and candidate.plan.multiplicity == .presence)) candidate.plan.multiplicity = multiplicity;
                    continue;
                }
            }
            const plan = (try stream.planWeightFields(request.fields, multiplicity)) orelse return false;
            const direction: usize = @intFromBool(plan.path.execution_reversed);
            try slots[direction].append(a, i);
            for (request.fields) |field| try fields[direction].put(a, field, {});
            if (multiplicity == .exact or (multiplicity == .capped and multiplicities[direction] == .presence)) multiplicities[direction] = multiplicity;
        }
        for (0..2) |i| {
            if (slots[i].items.len == 0) continue;
            var names: std.ArrayList([]const u8) = .empty;
            var keys = fields[i].keyIterator();
            while (keys.next()) |name| try names.append(a, name.*);
            try candidates.append(a, .{ .plan = (try stream.planWeightFields(names.items, multiplicities[i])) orelse return false, .slots = slots[i] });
        }
        if (window) |demand| bounded: {
            // A mixed fallback keeps its complete compressed domain. Only an
            // entirely certified scalar family can share the finite prefix.
            for (candidates.items) |candidate| if (candidate.plan.scalar_kind == null) break :bounded;
            const prefix = try stream.scalarGroupWindow(window_keys, demand.descending, demand.maximum, window_predicate);
            for (candidates.items) |*candidate| candidate.plan.scalar_window = prefix;
        }
        const passes = try a.alloc(WeightPass, candidates.items.len);
        for (candidates.items, passes) |candidate, *pass| pass.* = .{ .plan = candidate.plan, .slots = candidate.slots.items };
        try stream.startWeightPlan(passes[0].plan);
        self.weight_passes = passes;
        return true;
    }
    fn enableWeights(ptr: *anyopaque, multiplicity: catalog.Multiplicity) !bool {
        const self: *Cursor = @ptrCast(@alignCast(ptr));
        if (self.position != 0 or self.resume_position != 0 or self.request.primary_key != null or self.request.graph_source_ordinals) return false;
        for (self.request.fields) |field| if (std.mem.eql(u8, field, "_id")) return false;
        for (self.request.conditions) |condition| if (std.mem.eql(u8, condition.column, "_id")) return false;
        if (self.stream) |stream| return stream.enableWeights(multiplicity);
        return false;
    }
    fn countRows(ptr: *anyopaque) anyerror!?u64 {
        const self: *Cursor = @ptrCast(@alignCast(ptr));
        if (self.weight_passes.len != 0) return null;
        if (self.empty) return 0;
        // Storage hints are supersets of SQL truth. Leave any SQL residual to
        // its consumer rather than counting hinted candidates as exact rows.
        if (self.position != 0 or self.resume_position != 0 or self.request.primary_key != null or self.request.conditions.len != 0) return null;
        if (self.stream) |stream| return stream.countRows();
        return if (self.result) |result| result.rows.len else null;
    }
    fn next(ptr: *anyopaque, a: A, limit: u32) !catalog.Page {
        const self: *Cursor = @ptrCast(@alignCast(ptr));
        if (limit == 0 or limit > 4096) return error.InvalidSqlParameters;
        if (self.weight_passes.len != 0 and self.stream.?.done and self.weight_pass + 1 < self.weight_passes.len) {
            self.weight_pass += 1;
            try self.stream.?.startWeightPlan(self.weight_passes[self.weight_pass].plan);
        }
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const owned = arena.allocator();
        // row_goal is advisory: reduce page size, never truncate the source.
        const page_limit: u32 = @intCast(@max(1, @min(limit, self.request.row_goal orelse limit)));
        var output: std.ArrayList(catalog.Row) = .empty;
        var weights: std.ArrayList(u64) = .empty;
        var aggregate_inputs: std.ArrayList(?catalog.AggregateInput) = .empty;
        var counts: std.ArrayList(catalog.Count) = .empty;
        const names = try owned.alloc([]const u8, self.request.fields.len);
        for (self.request.fields, names) |name, *out| out.* = try owned.dupe(u8, name);
        const layout = try catalog.Row.TypedLayout.init(owned, names);
        while (!self.empty and output.items.len < page_limit) {
            var scratch = std.heap.ArenaAllocator.init(cypher.backend.reclaimingAllocator(a));
            defer scratch.deinit();
            const temporary = scratch.allocator();
            const result_columns = if (self.stream) |stream| stream.columns else self.result.?.columns;
            const rows: []const []const cypher.value.Value = if (self.stream) |stream| blk: {
                const entries = try stream.next(temporary, page_limit - output.items.len);
                const projected = try temporary.alloc([]const cypher.value.Value, entries.len);
                for (entries, projected) |row, *out| {
                    const values = try temporary.alloc(cypher.value.Value, row.len);
                    for (row, values) |entry, *value| value.* = entry.value;
                    out.* = values;
                }
                break :blk projected;
            } else blk: {
                if (self.position == self.result.?.rows.len) break;
                break :blk self.result.?.rows[self.position..@min(self.result.?.rows.len, self.position + page_limit - output.items.len)];
            };
            if (rows.len == 0) {
                if (self.position < self.resume_position) return error.InvalidSqlBackendResponse;
                break;
            }
            for (rows, 0..) |values, row_index| {
                const index = self.position;
                self.position += 1;
                if (index < self.resume_position) continue;
                const row_id = try std.fmt.allocPrint(temporary, "{d}", .{index});
                if (self.request.primary_key) |key| if (!std.mem.eql(u8, key, row_id)) continue;
                var all: std.json.ObjectMap = .empty;
                for (result_columns, values) |name, value| try all.put(temporary, name, try json(temporary, value));
                const row: catalog.Row = .{ .id = row_id, .version = 0, .value = .{ .object = all } };
                var accepted = true;
                for (self.request.conditions) |condition| {
                    const cell = try row.cell(condition.column);
                    const operand = condition.datum();
                    const matches = switch (condition.op) {
                        .is_null => cell.sql_null,
                        .is_not_null => !cell.sql_null,
                        else => blk: {
                            if (cell.sql_null or operand.sql_null) break :blk false;
                            const order = try scalar.compareDatums(cell, operand);
                            break :blk switch (condition.op) {
                                .eq => order == .eq,
                                .neq => order != .eq,
                                .lt => order == .lt,
                                .lte => order != .gt,
                                .gt => order == .gt,
                                .gte => order != .lt,
                                else => unreachable,
                            };
                        },
                    };
                    if (!matches) {
                        accepted = false;
                        break;
                    }
                }
                if (!accepted) continue;
                const cells = try owned.alloc(scalar.Datum, self.request.fields.len);
                for (self.request.fields, cells) |field, *out| out.* = try @import("operators.zig").cloneDatum(owned, try row.cell(field));
                try output.append(owned, try catalog.Row.fromDatums(owned, try owned.dupe(u8, row_id), layout, cells));
                if (self.stream) |stream| if (stream.weighted) {
                    try weights.append(owned, stream.last_weights.items[row_index]);
                    try counts.append(owned, try stream.last_counts.items[row_index].clone(owned));
                    if (self.weight_passes.len != 0 and self.weight_passes[self.weight_pass].plan.statistic != null) {
                        const input = stream.last_statistic_inputs.items[row_index];
                        const active_slots = self.weight_passes[self.weight_pass].slots;
                        var mapped = if (input) |argument| try argument.clone(owned) else null;
                        if (mapped) |*argument| try argument.remap(active_slots);
                        try aggregate_inputs.append(owned, mapped);
                    }
                };
            }
        }
        const more = if (self.weight_passes.len != 0) !self.stream.?.done or self.weight_pass + 1 < self.weight_passes.len else if (self.empty) false else if (self.stream) |stream| !stream.done else self.position < self.result.?.rows.len;
        return .{ .rows = try output.toOwnedSlice(owned), .aggregate_inputs = if (self.weight_passes.len != 0 and self.weight_passes[self.weight_pass].plan.statistic != null) aggregate_inputs.items else null, .exact_multiplicities = if (counts.items.len != 0) counts.items else null, .multiplicities = if (self.stream != null and self.stream.?.weighted) try weights.toOwnedSlice(owned) else null, .aggregate_slots = if (self.weight_passes.len != 0) try owned.dupe(usize, self.weight_passes[self.weight_pass].slots) else null, .owned_arena = arena, .after = if (!more) null else if (self.weight_passes.len != 0) try std.fmt.allocPrint(owned, "ag:{d}:{d}", .{ self.weight_pass, self.position }) else try std.fmt.allocPrint(owned, "{s}{d}", .{ if (self.ordered) "o:" else if (self.optimized) "g:" else "", self.position }) };
    }

    fn close(ptr: *anyopaque) void {
        const self: *Cursor = @ptrCast(@alignCast(ptr));
        if (self.groups) |groups| groups.source.deinit();
        if (self.stream) |stream| stream.close();
        if (self.result) |*result| result.deinit();
        self.arena.deinit();
        self.a.destroy(self);
    }
};

test "SQL GRAPH_TABLE dependency partitions share quotas and resume narrow pages" {
    const F = struct {
        const nodes = [_]cypher.value.Node{
            .{ .identity = .{ .table = "docs", .key = "a" }, .labels = &.{"D"}, .properties = &.{.{ .name = "rank", .value = .{ .integer = 1 } }} },
            .{ .identity = .{ .table = "docs", .key = "b" }, .labels = &.{"D"}, .properties = &.{.{ .name = "rank", .value = .{ .integer = 2 } }} },
            .{ .identity = .{ .table = "docs", .key = "c" }, .labels = &.{"D"}, .properties = &.{.{ .name = "rank", .value = .{ .integer = 3 } }} },
        };
        const edges = [_]cypher.value.Relationship{
            .{ .identity = .{ .table = "docs", .key = "ab1" }, .index = "links", .type = "R", .source = nodes[0].identity, .target = nodes[1].identity },
            .{ .identity = .{ .table = "docs", .key = "ab2" }, .index = "links", .type = "R", .source = nodes[0].identity, .target = nodes[1].identity },
            .{ .identity = .{ .table = "docs", .key = "bc" }, .index = "links", .type = "R", .source = nodes[1].identity, .target = nodes[2].identity },
        };
        fn bind(_: *anyopaque, _: A, _: cypher.catalog.Graph) !void {}
        fn checkpoint(_: *anyopaque) !void {}
        fn scan(_: *anyopaque, a: A, _: []const u8, after: ?[]const u8, limit: usize) !cypher.backend.Page(*const cypher.value.Node) {
            const start = if (after) |s| try std.fmt.parseUnsigned(usize, s, 10) else 0;
            const end = @min(nodes.len, start + limit);
            const out = try a.alloc(*const cypher.value.Node, end - start);
            for (out, start..) |*node, i| node.* = &nodes[i];
            return .{ .items = out, .next = if (end < nodes.len) try std.fmt.allocPrint(a, "{d}", .{end}) else null };
        }
        fn ordered(_: *anyopaque, a: A, _: []const u8, _: []const cypher.backend.PropertyFilter, order: []const cypher.backend.PropertyOrder, after: ?[]const u8, limit: usize, _: usize) !?cypher.backend.Page(*const cypher.value.Node) {
            if (order.len != 1 or !std.mem.eql(u8, order[0].property, "rank")) return null;
            if (limit == 0) return .{ .exact_keys = true, .items = &.{}, .examined = 0, .next = "0" };
            const start = if (after) |text| try std.fmt.parseInt(usize, text, 10) else 0;
            const end = @min(nodes.len, start + limit);
            const out = try a.alloc(*const cypher.value.Node, end - start);
            for (out, start..) |*node, index| node.* = &nodes[if (order[0].descending) nodes.len - 1 - index else index];
            return .{ .exact_keys = true, .items = out, .examined = out.len, .next = if (end < nodes.len) try std.fmt.allocPrint(a, "{d}", .{end}) else null };
        }
        fn coarse(ptr: *anyopaque, a: A, table: []const u8, filters: []const cypher.backend.PropertyFilter, order: []const cypher.backend.PropertyOrder, after: ?[]const u8, limit: usize, maximum: usize) !?cypher.backend.Page(*const cypher.value.Node) {
            var page = (try ordered(ptr, a, table, filters, order, after, limit, maximum)) orelse return null;
            page.stable_ties = false;
            return page;
        }
        fn get(_: *anyopaque, _: A, id: cypher.value.Identity) !?*const cypher.value.Node {
            for (&nodes) |*node| if (node.identity.eql(id)) return node;
            return null;
        }
        fn expand(_: *anyopaque, a: A, _: cypher.catalog.Relationship, id: cypher.value.Identity, d: cypher.ast.Direction, after: ?[]const u8, limit: usize) !cypher.backend.Page(*const cypher.value.Relationship) {
            var selected: std.ArrayList(*const cypher.value.Relationship) = .empty;
            for (&edges) |*edge| if ((d != .in and edge.source.eql(id)) or (d != .out and edge.target.eql(id))) try selected.append(a, edge);
            const start = if (after) |s| try std.fmt.parseUnsigned(usize, s, 10) else 0;
            const end = @min(selected.items.len, start + limit);
            return .{ .items = selected.items[start..end], .next = if (end < selected.items.len) try std.fmt.allocPrint(a, "{d}", .{end}) else null };
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var diagnostic: cypher.compiler.Diagnostic = .{};
    const compiled = try cypher.compiler.compileInto(a, "MATCH (a:D)-[:R*0..3]->(b:D) RETURN a.rank AS start_score,b.rank AS end_score", .{}, &diagnostic);
    const binding: Binding = .{ .definition = .{ .name = .{ .table = "g" }, .query = compiled.query, .parameters = compiled.parameters }, .graph = .{ .name = "g", .incarnation = 1, .revision = 1, .labels = &.{.{ .name = "D", .table = "docs", .table_id = 7 }}, .relationships = &.{.{ .name = "R", .index = "links", .source_label = "D", .target_label = "D", .stored_type = "R" }} }, .catalog_json = "{}", .dependencies = &.{} };
    var marker: u8 = 0;
    const backend: cypher.backend.Backend = .{ .ptr = &marker, .vtable = &.{ .bind = F.bind, .checkpoint = F.checkpoint, .scan = F.scan, .get = F.get, .expand = F.expand } };
    for ([_]bool{ false, true }) |bounded| {
        const cursor = try Cursor.open(std.testing.allocator, binding, backend, &.{}, .{ .fields = &.{ "start_score", "end_score" }, .limit = 1 }, .{ .scanned_anchors = if (bounded) 3 else 6 });
        defer cursor.close(cursor.ptr);
        try std.testing.expect(try cursor.enable_aggregate_weights.?(cursor.ptr, &.{
            .{ .fields = &.{"start_score"}, .multiplicity = .exact },
            .{ .fields = &.{"end_score"}, .multiplicity = .exact },
        }));
        var totals = [_]i64{ 0, 0 };
        var failed = false;
        while (true) {
            const page = cursor.next(cursor.ptr, std.testing.allocator, 1) catch |err| {
                if (!bounded) return err;
                try std.testing.expectEqual(error.CypherAnchorBudgetExceeded, err);
                failed = true;
                break;
            };
            defer page.deinit();
            try std.testing.expectEqual(@as(usize, 1), page.aggregate_slots.?.len);
            const slot = page.aggregate_slots.?[0];
            for (page.rows, page.multiplicities.?) |row, weight| {
                const value = try row.cell(if (slot == 0) "start_score" else "end_score");
                const inactive = try row.cell(if (slot == 0) "end_score" else "start_score");
                try std.testing.expect(inactive.sql_null);
                totals[slot] += value.value.integer * @as(i64, @intCast(weight));
            }
            if (page.after == null) break;
        }
        if (bounded) try std.testing.expect(failed) else try std.testing.expectEqualSlices(i64, &.{ 12, 19 }, &totals);
    }
    const simple = try cypher.compiler.compileInto(a, "MATCH (n:D) RETURN n.rank AS rank", .{}, &diagnostic);
    var ordered_binding = binding;
    ordered_binding.definition.query = simple.query;
    ordered_binding.dependencies = &.{.{ .id = 7, .physical_name = "docs", .schema_version = 1, .columns = &.{.{ .name = "rank", .path = "rank", .type = .integer }} }};
    var callbacks = backend.vtable.*;
    callbacks.scan_ordered = F.ordered;
    const indexed: cypher.backend.Backend = .{ .ptr = &marker, .vtable = &callbacks };
    {
        const cursor = try Cursor.open(std.testing.allocator, ordered_binding, indexed, &.{}, .{ .fields = &.{"rank"}, .limit = 1 }, .{ .scanned_anchors = 5 });
        defer cursor.close(cursor.ptr);
        try std.testing.expect(try cursor.enable_aggregate_weights.?(cursor.ptr, &.{
            .{ .observer = .distinct, .fields = &.{"rank"}, .multiplicity = .presence },
            .{ .observer = .min, .fields = &.{"rank"}, .multiplicity = .presence },
            .{ .observer = .max, .fields = &.{"rank"}, .multiplicity = .presence },
        }));
        var counts = [_]usize{ 0, 0, 0 };
        var totals = [_]i64{ 0, 0, 0 };
        while (true) {
            const page = try cursor.next(cursor.ptr, std.testing.allocator, 1);
            defer page.deinit();
            const slot = page.aggregate_slots.?[0];
            for (page.rows, page.multiplicities.?) |row, weight| {
                try std.testing.expectEqual(@as(u64, 1), weight);
                counts[slot] += 1;
                totals[slot] += (try row.cell("rank")).value.integer;
            }
            if (page.after == null) break;
        }
        try std.testing.expectEqualSlices(usize, &.{ 3, 1, 1 }, &counts);
        try std.testing.expectEqualSlices(i64, &.{ 6, 1, 3 }, &totals);
    }
    for ([_]bool{ false, true }) |grouped| {
        const cursor = try Cursor.open(std.testing.allocator, ordered_binding, indexed, &.{}, .{ .fields = &.{"rank"}, .limit = 1, .conditions = if (grouped) &.{} else &.{.{ .column = "rank", .op = .gte, .value = .{ .integer = 2 } }} }, .{ .scanned_anchors = 9 });
        defer cursor.close(cursor.ptr);
        const keys: []const []const u8 = if (grouped) &.{"rank"} else &.{};
        try std.testing.expect(try cursor.enable_aggregate_weights.?(cursor.ptr, &.{
            .{ .observer = .distinct, .observer_field = "rank", .observer_keys = keys, .fields = &.{"rank"}, .multiplicity = .presence },
            .{ .observer = .min, .observer_field = "rank", .observer_keys = keys, .fields = &.{"rank"}, .multiplicity = .presence },
            .{ .observer = .max, .observer_field = "rank", .observer_keys = keys, .fields = &.{"rank"}, .multiplicity = .presence },
        }));
        var counts = [_]usize{ 0, 0, 0 };
        var totals = [_]i64{ 0, 0, 0 };
        while (true) {
            const page = try cursor.next(cursor.ptr, std.testing.allocator, 1);
            defer page.deinit();
            const slot = page.aggregate_slots.?[0];
            for (page.rows, page.multiplicities.?) |row, weight| {
                try std.testing.expectEqual(@as(u64, 1), weight);
                counts[slot] += 1;
                totals[slot] += (try row.cell("rank")).value.integer;
            }
            if (page.after == null) break;
        }
        try std.testing.expectEqualSlices(usize, if (grouped) &.{ 3, 3, 3 } else &.{ 2, 1, 1 }, &counts);
        try std.testing.expectEqualSlices(i64, if (grouped) &.{ 6, 6, 6 } else &.{ 5, 2, 3 }, &totals);
    }
    for ([_]bool{ true, false }) |nulls_first| {
        const cursor = try Cursor.open(std.testing.allocator, ordered_binding, indexed, &.{}, .{ .fields = &.{"rank"}, .order = &.{.{ .column = "rank", .descending = true, .nulls_first = nulls_first }}, .row_goal = 1, .limit = 1 }, .{ .scanned_anchors = 1 });
        defer cursor.close(cursor.ptr);
        try std.testing.expectEqual(nulls_first, cursor.order_satisfied);
        if (nulls_first) {
            const page = try cursor.next(cursor.ptr, std.testing.allocator, 1);
            defer page.deinit();
            try std.testing.expectEqual(@as(i64, 3), (try page.rows[0].cell("rank")).value.integer);
            const resumed = try Cursor.open(std.testing.allocator, ordered_binding, indexed, &.{}, .{ .fields = &.{"rank"}, .order = &.{.{ .column = "rank", .descending = true, .nulls_first = true }}, .after = page.after, .limit = 1 }, .{ .scanned_anchors = 2 });
            defer resumed.close(resumed.ptr);
            try std.testing.expect(resumed.order_satisfied);
            const next_page = try resumed.next(resumed.ptr, std.testing.allocator, 1);
            defer next_page.deinit();
            try std.testing.expectEqual(@as(i64, 2), (try next_page.rows[0].cell("rank")).value.integer);
        }
    }
    callbacks.scan_ordered = F.coarse;
    // Superset hints cannot cap a source. A complete, exactly translated
    // predicate can, while an unsupported OR arm must decline atomically.
    for ([_]bool{ true, false }) |exact| {
        const filter: catalog.Scan.GraphFilter = if (exact) .{ .comparison = .{ .column = "rank", .op = .gte, .value = .{ .integer = 2 } } } else .{ .any = &.{
            .{ .comparison = .{ .column = "rank", .op = .gte, .value = .{ .integer = 2 } } },
            .{ .comparison = .{ .column = "rank", .op = .eq, .value = .{ .float = 1.5 } } },
        } };
        const cursor = try Cursor.open(std.testing.allocator, ordered_binding, indexed, &.{}, .{ .fields = &.{"rank"}, .order = &.{.{ .column = "rank", .descending = true, .nulls_first = true }}, .conditions = if (exact) &.{.{ .column = "rank", .op = .gte, .value = .{ .integer = 2 } }} else &.{}, .exact_graph_filter = filter, .ordered_window = 1, .limit = 1 }, .{ .scanned_anchors = 3 });
        defer cursor.close(cursor.ptr);
        try std.testing.expectEqual(exact, cursor.order_satisfied);
        var emitted: usize = 0;
        while (true) {
            const page = try cursor.next(cursor.ptr, std.testing.allocator, 1);
            defer page.deinit();
            for (page.rows) |row| {
                emitted += 1;
                try std.testing.expectEqual(@as(i64, if (exact) 3 else @intCast(emitted)), (try row.cell("rank")).value.integer);
            }
            if (page.after == null) break;
        }
        try std.testing.expectEqual(@as(usize, if (exact) 1 else 3), emitted);
    }
    for ([_]bool{ false, true }) |licensed| {
        const cursor = try Cursor.open(std.testing.allocator, ordered_binding, indexed, &.{}, .{ .fields = &.{"rank"}, .order = &.{.{ .column = "rank", .descending = true, .nulls_first = true }}, .row_goal = 1, .ordered_window = if (licensed) 1 else null, .limit = 1 }, .{ .scanned_anchors = if (licensed) 2 else 3 });
        defer cursor.close(cursor.ptr);
        try std.testing.expectEqual(licensed, cursor.order_satisfied);
        try std.testing.expectEqual(@as(?u64, null), if (licensed) try cursor.count_rows.?(cursor.ptr) else null);
        var emitted: usize = 0;
        while (true) {
            const page = try cursor.next(cursor.ptr, std.testing.allocator, 1);
            defer page.deinit();
            for (page.rows) |row| {
                emitted += 1;
                try std.testing.expectEqual(@as(i64, if (licensed) 3 else @intCast(emitted)), (try row.cell("rank")).value.integer);
            }
            if (page.after == null) break;
        }
        try std.testing.expectEqual(@as(usize, if (licensed) 1 else 3), emitted);
    }
}

test "SQL GRAPH_TABLE derives columns and exposes typed rows from shared graph IR" {
    const Fixture = struct {
        const node: cypher.value.Node = .{ .identity = .{ .table = "docs", .key = "a" }, .labels = &.{"Document"}, .properties = &.{ .{ .name = "id", .value = .{ .string = "a" } }, .{ .name = "large", .value = .{ .integer = std.math.maxInt(i64) } } } };
        fn bind(_: *anyopaque, _: A, _: cypher.catalog.Graph) !void {}
        fn checkpoint(_: *anyopaque) !void {}
        fn ordered(_: *anyopaque, _: A, _: []const u8, _: []const cypher.backend.PropertyFilter, _: []const cypher.backend.PropertyOrder, _: ?[]const u8, _: usize, _: usize) !?cypher.backend.Page(*const cypher.value.Node) {
            return error.TestUnexpectedResult;
        }
        fn scan(ptr: *anyopaque, a: A, _: []const u8, after: ?[]const u8, _: usize) !cypher.backend.Page(*const cypher.value.Node) {
            const scanned: *u8 = @ptrCast(@alignCast(ptr));
            scanned.* += 1;
            return .{ .items = if (after == null) try a.dupe(*const cypher.value.Node, &.{&node}) else &.{} };
        }
        fn get(_: *anyopaque, _: A, _: cypher.value.Identity) !?*const cypher.value.Node {
            return null;
        }
        fn expand(_: *anyopaque, _: A, _: cypher.catalog.Relationship, _: cypher.value.Identity, _: cypher.ast.Direction, _: ?[]const u8, _: usize) !cypher.backend.Page(*const cypher.value.Relationship) {
            return .{ .items = &.{} };
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var diagnostic: cypher.compiler.Diagnostic = .{};
    const compiled = try cypher.compiler.compileInto(a, "MATCH (n:Document) RETURN n.id AS id,n AS entity,123 AS exact", .{}, &diagnostic);
    const binding: Binding = .{ .definition = .{ .name = .{ .table = "knowledge" }, .query = compiled.query, .parameters = compiled.parameters }, .graph = .{ .name = "knowledge", .incarnation = 1, .revision = 1, .labels = &.{.{ .name = "Document", .table = "docs", .table_id = 7 }}, .relationships = &.{} }, .catalog_json = "{}", .dependencies = &.{.{ .id = 7, .physical_name = "docs", .schema_version = 1, .columns = &.{.{ .name = "id", .path = "id", .type = .string }} }} };
    const shape = try columns(a, binding);
    try std.testing.expectEqual(ast.ColumnType.string, shape[0].type);
    try std.testing.expectEqual(ast.ColumnType.json, shape[1].type);
    try std.testing.expectEqual(ast.ColumnType.integer, shape[2].type);
    var marker: u8 = 0;
    const backend: cypher.backend.Backend = .{ .ptr = &marker, .vtable = &.{ .bind = Fixture.bind, .checkpoint = Fixture.checkpoint, .scan = Fixture.scan, .get = Fixture.get, .expand = Fixture.expand } };
    const cursor = try Cursor.open(std.testing.allocator, binding, backend, &.{}, .{ .fields = &.{ "id", "entity", "exact" }, .limit = 1, .conditions = &.{.{ .column = "id", .op = .eq, .value = .{ .string = "a" } }} }, .{});
    defer cursor.close(cursor.ptr);
    const page = try cursor.next(cursor.ptr, std.testing.allocator, 1);
    defer page.deinit();
    try std.testing.expectEqual(@as(usize, 1), page.rows.len);
    try std.testing.expectEqualStrings("a", (try page.rows[0].cell("id")).value.string);
    try std.testing.expectEqual(@as(i64, 123), (try page.rows[0].cell("exact")).value.integer);
    const entity = (try page.rows[0].cell("entity")).value;
    try std.testing.expectEqualStrings("node", entity.object.get("type").?.string);
    const large = entity.object.get("value").?.object.get("properties").?.object.get("value").?.object.get("large").?;
    try std.testing.expectEqualStrings("9223372036854775807", large.object.get("value").?.string);
    marker = 0;
    const empty = try Cursor.open(std.testing.allocator, binding, backend, &.{}, .{ .fields = &.{"id"}, .limit = 1, .conditions = &.{.{ .column = "id", .op = .eq, .value = .null }} }, .{ .scanned_anchors = 0 });
    defer empty.close(empty.ptr);
    try std.testing.expectEqual(@as(?u64, 0), empty.estimated_rows);
    for (0..2) |_| {
        const empty_page = try empty.next(empty.ptr, std.testing.allocator, 1);
        defer empty_page.deinit();
        try std.testing.expectEqual(@as(usize, 0), empty_page.rows.len);
        try std.testing.expectEqual(@as(?[]const u8, null), empty_page.after);
    }
    try std.testing.expectEqual(@as(u8, 0), marker);
    // A physically ordered backend cannot license SQL JSON/array grouping.
    // Decline before asking it for a scalar-domain certificate.
    const scalar_query = try cypher.compiler.compileInto(a, "MATCH(n:Document) RETURN n.id AS id,n.large AS large", .{}, &diagnostic);
    const guarded_backend: cypher.backend.Backend = .{ .ptr = &marker, .vtable = &.{ .bind = Fixture.bind, .checkpoint = Fixture.checkpoint, .scan = Fixture.scan, .get = Fixture.get, .expand = Fixture.expand, .scan_ordered = Fixture.ordered } };
    for ([_]ast.ColumnType{ .json, .array }) |key_type| {
        var typed_binding = binding;
        typed_binding.definition.query = scalar_query.query;
        typed_binding.dependencies = &.{.{ .id = 7, .physical_name = "docs", .schema_version = 1, .columns = &.{ .{ .name = "id", .path = "id", .type = key_type }, .{ .name = "large", .path = "large", .type = .integer } } }};
        const guarded = try Cursor.open(std.testing.allocator, typed_binding, guarded_backend, &.{}, .{ .fields = &.{ "id", "large" }, .limit = 1 }, .{});
        defer guarded.close(guarded.ptr);
        const grouped = try @import("operators.zig").Grouped.create(std.testing.allocator, &.{.{ .kind = .count }}, .{});
        defer grouped.deinit();
        try std.testing.expect(!try guarded.enable_groups.?(guarded.ptr, &.{.{ .fields = &.{"large"}, .multiplicity = .presence, .observer = .distinct, .observer_field = "large", .observer_keys = &.{"id"} }}, &.{.{ .key = 0, .descending = false, .nulls_first = true }}, grouped));
        try std.testing.expect(grouped.finalized == null);
        try std.testing.expectEqual(@as(u8, 0), marker);
    }
}

test "graph scan pushdown respects typed comparisons and cardinality barriers" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const dependencies = &[_]catalog.Table{.{ .id = 7, .physical_name = "docs", .schema_version = 1, .columns = &.{ .{ .name = "score", .path = "score", .type = .integer }, .{ .name = "payload", .path = "payload", .type = .string } } }};
    const graph: cypher.catalog.Graph = .{ .name = "g", .incarnation = 1, .revision = 1, .labels = &.{.{ .name = "D", .table = "docs", .table_id = 7 }}, .relationships = &.{} };
    var diagnostic: cypher.compiler.Diagnostic = .{};
    const compiled = try cypher.compiler.compileInto(a, "MATCH (n:D) RETURN n.score AS score,n.payload AS unused", .{}, &diagnostic);
    const binding: Binding = .{ .definition = .{ .name = .{ .table = "g" }, .query = compiled.query, .parameters = compiled.parameters }, .graph = graph, .catalog_json = "{}", .dependencies = dependencies };
    const request: catalog.Scan = .{ .fields = &.{"score"}, .conditions = &.{.{ .column = "score", .op = .eq, .value = .{ .integer = 9 } }}, .limit = 1 };
    const planned = try scanQuery(a, binding, request);
    try std.testing.expectEqual(@as(usize, 1), planned.clauses[1].operation.project.value.items.len);
    try std.testing.expect(planned.clauses[0].operation.match.predicate != null);
    var mixed = request;
    mixed.conditions = &.{.{ .column = "score", .op = .eq, .value = .{ .float = 1.5 } }};
    try std.testing.expect((try scanQuery(a, binding, mixed)).clauses[0].operation.match.predicate == null);
    var grouped = binding;
    grouped.definition.query = (try cypher.compiler.compileInto(a, "MATCH (n:D) RETURN n.score AS score,count(*) AS total", .{}, &diagnostic)).query;
    const grouped_plan = try scanQuery(a, grouped, request);
    try std.testing.expect(grouped_plan.clauses[0].operation.match.predicate == null);
    try std.testing.expectEqual(@as(usize, 2), grouped_plan.clauses[1].operation.project.value.items.len);
}

test "graph cursor resumes pushed pages and preserves observable source ordinals" {
    const Fixture = struct {
        const nodes = [_]cypher.value.Node{
            .{ .identity = .{ .table = "docs", .key = "a" }, .labels = &.{"D"}, .properties = &.{ .{ .name = "id", .value = .{ .string = "a" } }, .{ .name = "rank", .value = .{ .integer = 0 } } } },
            .{ .identity = .{ .table = "docs", .key = "b" }, .labels = &.{"D"}, .properties = &.{ .{ .name = "id", .value = .{ .string = "b" } }, .{ .name = "rank", .value = .{ .integer = 1 } } } },
            .{ .identity = .{ .table = "docs", .key = "c" }, .labels = &.{"D"}, .properties = &.{ .{ .name = "id", .value = .{ .string = "c" } }, .{ .name = "rank", .value = .{ .integer = 2 } } } },
        };
        fn bind(_: *anyopaque, _: A, _: cypher.catalog.Graph) !void {}
        fn checkpoint(_: *anyopaque) !void {}
        fn scan(_: *anyopaque, a: A, _: []const u8, after: ?[]const u8, demand: usize) !cypher.backend.Page(*const cypher.value.Node) {
            const start = if (after) |value| try std.fmt.parseUnsigned(usize, value, 10) else 0;
            const end = @min(nodes.len, start + demand);
            const output = try a.alloc(*const cypher.value.Node, end - start);
            for (output, start..) |*out, index| out.* = &nodes[index];
            return .{ .items = output, .next = if (end < nodes.len) try std.fmt.allocPrint(a, "{d}", .{end}) else null };
        }
        fn get(_: *anyopaque, _: A, _: cypher.value.Identity) !?*const cypher.value.Node {
            return null;
        }
        fn expand(_: *anyopaque, _: A, _: cypher.catalog.Relationship, _: cypher.value.Identity, _: cypher.ast.Direction, _: ?[]const u8, _: usize) !cypher.backend.Page(*const cypher.value.Relationship) {
            return .{ .items = &.{} };
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var diagnostic: cypher.compiler.Diagnostic = .{};
    const compiled = try cypher.compiler.compileInto(a, "MATCH (n:D) RETURN n.id AS id,n.rank AS rank", .{}, &diagnostic);
    const binding: Binding = .{ .definition = .{ .name = .{ .table = "g" }, .query = compiled.query, .parameters = compiled.parameters }, .graph = .{ .name = "g", .incarnation = 1, .revision = 1, .labels = &.{.{ .name = "D", .table = "docs", .table_id = 7 }}, .relationships = &.{} }, .catalog_json = "{}", .dependencies = &.{.{ .id = 7, .physical_name = "docs", .schema_version = 1, .columns = &.{ .{ .name = "id", .path = "id", .type = .string }, .{ .name = "rank", .path = "rank", .type = .integer } } }} };
    var marker: u8 = 0;
    const backend: cypher.backend.Backend = .{ .ptr = &marker, .vtable = &.{ .bind = Fixture.bind, .checkpoint = Fixture.checkpoint, .scan = Fixture.scan, .get = Fixture.get, .expand = Fixture.expand } };
    const paths = try cypher.compiler.compileInto(a, "MATCH (n:D)-[:R*0..2]->(m:D) RETURN n.id AS id", .{}, &diagnostic);
    var path_binding = binding;
    path_binding.definition.query = paths.query;
    path_binding.graph.relationships = &.{.{ .name = "R", .index = "links", .source_label = "D", .target_label = "D", .stored_type = "R" }};
    for ([_]bool{ false, true }) |ordinals| {
        const weighted = try Cursor.open(std.testing.allocator, path_binding, backend, &.{}, .{ .fields = &.{"id"}, .graph_source_ordinals = ordinals, .limit = 128 }, .{});
        defer weighted.close(weighted.ptr);
        try std.testing.expectEqual(!ordinals, try weighted.enable_weights.?(weighted.ptr, .exact));
    }
    var request: catalog.Scan = .{ .fields = &.{"id"}, .conditions = &.{.{ .column = "rank", .op = .gte, .value = .{ .integer = 1 } }}, .limit = 1, .row_goal = 1 };
    const first = try Cursor.open(std.testing.allocator, binding, backend, &.{}, request, .{});
    const page = try first.next(first.ptr, std.testing.allocator, 2);
    defer page.deinit();
    const continuing = try first.next(first.ptr, std.testing.allocator, 2);
    defer continuing.deinit();
    first.close(first.ptr);
    try std.testing.expectEqualStrings("c", (try continuing.rows[0].cell("id")).value.string);
    try std.testing.expectEqualStrings("b", (try page.rows[0].cell("id")).value.string);
    try std.testing.expectEqualStrings("g:1", page.after.?);
    request.after = page.after;
    const resumed = try Cursor.open(std.testing.allocator, binding, backend, &.{}, request, .{});
    defer resumed.close(resumed.ptr);
    const last = try resumed.next(resumed.ptr, std.testing.allocator, 2);
    defer last.deinit();
    try std.testing.expectEqual(@as(usize, 1), last.rows.len);
    try std.testing.expectEqualStrings("c", (try last.rows[0].cell("id")).value.string);
    try std.testing.expect(last.after == null);
    request.after = "1";
    request.fields = &.{ "id", "_id" };
    const legacy = try Cursor.open(std.testing.allocator, binding, backend, &.{}, request, .{});
    defer legacy.close(legacy.ptr);
    const ordinal = try legacy.next(legacy.ptr, std.testing.allocator, 1);
    defer ordinal.deinit();
    try std.testing.expectEqualStrings("b", (try ordinal.rows[0].cell("id")).value.string);
    try std.testing.expectEqualStrings("1", (try ordinal.rows[0].cell("_id")).value.string);
}
