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
    var query = binding.definition.query;
    if (query.clauses.len != 2 or query.clauses[0].operation != .match or query.clauses[1].operation != .project) return query;
    const original = query.clauses[1].operation.project.value;
    if (original.distinct or original.wildcard or original.order.len != 0 or original.skip != null or original.limit != null or original.predicate != null) return query;
    for (original.items) |item| if (cypher.expressions.aggregates(item.expression)) return query;
    const clauses = try a.dupe(cypher.ast.Clause, query.clauses);
    const operation = &clauses[0].operation.match;
    var push = !operation.optional and !request.graph_source_ordinals and request.primary_key == null and request.after == null;
    for (request.fields) |field| if (std.mem.eql(u8, field, "_id")) {
        push = false;
    };
    for (operation.paths) |path| if (path.selector != .all) {
        push = false;
    };
    if (push) {
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
    empty: bool = false,
    pub fn open(a: A, binding: Binding, backend: cypher.backend.Backend, parameters: []const cypher.value.Entry, request: catalog.Scan, limits: cypher.runtime.Limits) !catalog.Cursor {
        const self = try a.create(Cursor);
        errdefer a.destroy(self);
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const owned = arena.allocator();
        const fields = try owned.alloc([]const u8, request.fields.len);
        for (request.fields, fields) |field, *out| out.* = try owned.dupe(u8, field);
        const conditions = try owned.alloc(catalog.Condition, request.conditions.len);
        for (request.conditions, conditions) |condition, *out| out.* = .{ .column = try owned.dupe(u8, condition.column), .op = condition.op, .value = try @import("../storage/typed_json.zig").clone(owned, condition.value), .operand = try @import("operators.zig").cloneDatum(owned, condition.datum()) };
        const optimized = if (request.after) |after| std.mem.startsWith(u8, after, "g:") else true;
        const resume_position = if (request.after) |after| try std.fmt.parseUnsigned(usize, if (optimized) after[2..] else after, 10) else 0;
        var query_request = request;
        // Tagged tokens resume the same pruned/pushed relation. Legacy ordinal
        // tokens retain their original unfiltered source interpretation.
        if (optimized) query_request.after = null;
        var compiled: cypher.compiler.Compiled = .{ .arena = std.heap.ArenaAllocator.init(a), .query = try scanQuery(owned, binding, query_request), .parameters = binding.definition.parameters };
        defer compiled.deinit();
        // Every ordinary comparison with SQL NULL is UNKNOWN. A conjunction
        // containing one can emit no rows, even across graph semantic barriers.
        const empty = for (conditions) |condition| {
            if (condition.op != .is_null and condition.op != .is_not_null and condition.datum().sql_null) break true;
        } else false;
        const stream = if (empty) null else try cypher.runtime.NodeCursor.open(a, backend, binding.graph, compiled.query, parameters, limits);
        errdefer if (stream) |cursor| cursor.close();
        var result: ?cypher.runtime.Result = if (!empty and stream == null) try cypher.runtime.execute(a, backend, binding.graph, &compiled, parameters, limits) else null;
        errdefer if (result) |*value| value.deinit();
        var copied = request;
        copied.fields = fields;
        copied.conditions = conditions;
        copied.graph_filters = &.{};
        copied.after = null;
        copied.primary_key = if (request.primary_key) |key| try owned.dupe(u8, key) else null;
        if ((empty and resume_position != 0) or (result != null and resume_position > result.?.rows.len)) return error.InvalidSqlBackendResponse;
        self.* = .{ .a = a, .arena = arena, .result = result, .stream = stream, .request = copied, .position = if (stream != null) 0 else resume_position, .resume_position = resume_position, .optimized = optimized, .empty = empty };
        return .{ .ptr = self, .next = next, .close = close, .count_rows = countRows, .enable_weights = enableWeights, .estimated_rows = if (empty) 0 else if (result) |value| value.rows.len else null };
    }
    fn enableWeights(ptr: *anyopaque) !bool {
        const self: *Cursor = @ptrCast(@alignCast(ptr));
        if (self.position != 0 or self.resume_position != 0 or self.request.primary_key != null or self.request.graph_source_ordinals) return false;
        for (self.request.fields) |field| if (std.mem.eql(u8, field, "_id")) return false;
        for (self.request.conditions) |condition| if (std.mem.eql(u8, condition.column, "_id")) return false;
        if (self.stream) |stream| return stream.enableWeights();
        return false;
    }
    fn countRows(ptr: *anyopaque) anyerror!?u64 {
        const self: *Cursor = @ptrCast(@alignCast(ptr));
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
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const owned = arena.allocator();
        // row_goal is advisory: reduce page size, never truncate the source.
        const page_limit: u32 = @intCast(@max(1, @min(limit, self.request.row_goal orelse limit)));
        var output: std.ArrayList(catalog.Row) = .empty;
        var weights: std.ArrayList(u64) = .empty;
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
                if (self.stream) |stream| if (stream.weighted) try weights.append(owned, stream.last_weights.items[row_index]);
            }
        }
        const more = if (self.empty) false else if (self.stream) |stream| !stream.done else self.position < self.result.?.rows.len;
        return .{ .rows = try output.toOwnedSlice(owned), .multiplicities = if (self.stream != null and self.stream.?.weighted) try weights.toOwnedSlice(owned) else null, .owned_arena = arena, .after = if (more) try std.fmt.allocPrint(owned, "{s}{d}", .{ if (self.optimized) "g:" else "", self.position }) else null };
    }

    fn close(ptr: *anyopaque) void {
        const self: *Cursor = @ptrCast(@alignCast(ptr));
        if (self.stream) |stream| stream.close();
        if (self.result) |*result| result.deinit();
        self.arena.deinit();
        self.a.destroy(self);
    }
};

test "SQL GRAPH_TABLE derives columns and exposes typed rows from shared graph IR" {
    const Fixture = struct {
        const node: cypher.value.Node = .{ .identity = .{ .table = "docs", .key = "a" }, .labels = &.{"Document"}, .properties = &.{ .{ .name = "id", .value = .{ .string = "a" } }, .{ .name = "large", .value = .{ .integer = std.math.maxInt(i64) } } } };
        fn bind(_: *anyopaque, _: A, _: cypher.catalog.Graph) !void {}
        fn checkpoint(_: *anyopaque) !void {}
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
        try std.testing.expectEqual(!ordinals, try weighted.enable_weights.?(weighted.ptr));
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
