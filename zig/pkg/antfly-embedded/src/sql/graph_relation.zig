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
                if (binary.op == .add and left == .string and right == .string) break :blk .string;
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
pub const Cursor = struct {
    a: A,
    arena: std.heap.ArenaAllocator,
    result: cypher.runtime.Result,
    request: catalog.Scan,
    position: usize = 0,
    pub fn open(a: A, binding: Binding, backend: cypher.backend.Backend, parameters: []const cypher.value.Entry, request: catalog.Scan, limits: cypher.runtime.Limits) !catalog.Cursor {
        const self = try a.create(Cursor);
        errdefer a.destroy(self);
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const owned = arena.allocator();
        const fields = try owned.alloc([]const u8, request.fields.len);
        for (request.fields, fields) |field, *out| out.* = try owned.dupe(u8, field);
        const conditions = try owned.alloc(catalog.Condition, request.conditions.len);
        for (request.conditions, conditions) |condition, *out| out.* = .{ .column = try owned.dupe(u8, condition.column), .op = condition.op, .value = try @import("../storage/typed_json.zig").clone(owned, condition.value) };
        var compiled: cypher.compiler.Compiled = .{ .arena = std.heap.ArenaAllocator.init(a), .query = binding.definition.query, .parameters = binding.definition.parameters };
        defer compiled.deinit();
        var result = try cypher.runtime.execute(a, backend, binding.graph, &compiled, parameters, limits);
        errdefer result.deinit();
        var copied = request;
        copied.fields = fields;
        copied.conditions = conditions;
        copied.after = null;
        copied.primary_key = if (request.primary_key) |key| try owned.dupe(u8, key) else null;
        const position = if (request.after) |after| try std.fmt.parseUnsigned(usize, after, 10) else 0;
        if (position > result.rows.len) return error.InvalidSqlBackendResponse;
        self.* = .{ .a = a, .arena = arena, .result = result, .request = copied, .position = position };
        return .{ .ptr = self, .next = next, .close = close, .estimated_rows = result.rows.len };
    }
    fn next(ptr: *anyopaque, a: A, limit: u32) !catalog.Page {
        const self: *Cursor = @ptrCast(@alignCast(ptr));
        if (limit == 0 or limit > 4096) return error.InvalidSqlParameters;
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const owned = arena.allocator();
        var output: std.ArrayList(catalog.Row) = .empty;
        const names = try owned.alloc([]const u8, self.request.fields.len);
        for (self.request.fields, names) |name, *out| out.* = try owned.dupe(u8, name);
        const layout = try catalog.Row.TypedLayout.init(owned, names);
        while (self.position < self.result.rows.len and output.items.len < limit) {
            const index = self.position;
            self.position += 1;
            const values = self.result.rows[index];
            const row_id = try std.fmt.allocPrint(owned, "{d}", .{index});
            if (self.request.primary_key) |key| if (!std.mem.eql(u8, key, row_id)) continue;
            var all: std.json.ObjectMap = .empty;
            for (self.result.columns, values) |name, value| try all.put(owned, name, try json(owned, value));
            const row: catalog.Row = .{ .id = row_id, .version = 0, .value = .{ .object = all } };
            var accepted = true;
            for (self.request.conditions) |condition| {
                const cell = try row.cell(condition.column);
                const matches = switch (condition.op) {
                    .is_null => cell.sql_null,
                    .is_not_null => !cell.sql_null,
                    else => blk: {
                        if (cell.sql_null or condition.value == .null) break :blk false;
                        const order = try scalar.compareDatums(cell, scalar.Datum.fromJson(condition.value));
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
            for (self.request.fields, cells) |field, *out| out.* = try row.cell(field);
            try output.append(owned, try catalog.Row.fromDatums(owned, row_id, layout, cells));
        }
        return .{ .rows = try output.toOwnedSlice(owned), .owned_arena = arena, .after = if (self.position < self.result.rows.len) try std.fmt.allocPrint(owned, "{d}", .{self.position}) else null };
    }
    fn close(ptr: *anyopaque) void {
        const self: *Cursor = @ptrCast(@alignCast(ptr));
        self.result.deinit();
        self.arena.deinit();
        self.a.destroy(self);
    }
};

test "SQL GRAPH_TABLE derives columns and exposes typed rows from shared graph IR" {
    const Fixture = struct {
        const node: cypher.value.Node = .{ .identity = .{ .table = "docs", .key = "a" }, .labels = &.{"Document"}, .properties = &.{ .{ .name = "id", .value = .{ .string = "a" } }, .{ .name = "large", .value = .{ .integer = std.math.maxInt(i64) } } } };
        fn bind(_: *anyopaque, _: A, _: cypher.catalog.Graph) !void {}
        fn checkpoint(_: *anyopaque) !void {}
        fn scan(_: *anyopaque, a: A, _: []const u8, after: ?[]const u8, _: usize) !cypher.backend.Page(*const cypher.value.Node) {
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
}
