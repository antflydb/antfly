// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");

const db_mod = struct {
    pub const DB = @import("../../storage/db/db.zig").DB;
    pub const types = @import("../../storage/db/types.zig");
};
const expr_equal = @import("equal.zig");
const expr_text = @import("text.zig");
const expr_type = @import("type.zig");
const plan_mod = @import("../plan.zig");
const runtime_schema = @import("../../storage/schema.zig");

const freeRelationalChecks = plan_mod.freeRelationalChecks;

pub fn expressionValueJsonAlloc(
    alloc: std.mem.Allocator,
    expression: db_mod.types.RelationalRowsExpression,
    values: anytype,
) ![]u8 {
    return switch (expression.kind) {
        .field => blk: {
            if (expression.field_source != .row) return error.UnsupportedSqlShape;
            const value_json = fieldValueJsonFor(values, expression.field) orelse return error.UnsupportedSqlShape;
            break :blk try alloc.dupe(u8, value_json);
        },
        .value => try alloc.dupe(u8, expression.value_json),
        .lower, .upper, .initcap, .md5, .soundex => blk: {
            if (expression.operands.len != 1) return error.UnsupportedSqlShape;
            const value_json = try expressionValueJsonAlloc(alloc, expression.operands[0], values);
            defer alloc.free(value_json);
            var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.UnsupportedSqlShape;
            defer parsed.deinit();
            switch (parsed.value) {
                .null => break :blk try alloc.dupe(u8, "null"),
                .string => |text| {
                    const transformed = switch (expression.kind) {
                        .lower => try std.ascii.allocLowerString(alloc, text),
                        .upper => try std.ascii.allocUpperString(alloc, text),
                        .initcap => try expr_text.initcapTextAlloc(alloc, text),
                        .md5 => try expr_text.md5HexTextAlloc(alloc, text),
                        .soundex => try expr_text.soundexTextAlloc(alloc, text),
                        else => unreachable,
                    };
                    defer alloc.free(transformed);
                    break :blk try std.json.Stringify.valueAlloc(alloc, std.json.Value{ .string = transformed }, .{});
                },
                else => return error.UnsupportedSqlShape,
            }
        },
        .concat => blk: {
            if (expression.operands.len == 0) return error.UnsupportedSqlShape;
            var joined = std.ArrayListUnmanaged(u8).empty;
            defer joined.deinit(alloc);
            for (expression.operands) |operand| {
                const value_json = try expressionValueJsonAlloc(alloc, operand, values);
                defer alloc.free(value_json);
                var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.UnsupportedSqlShape;
                defer parsed.deinit();
                if (parsed.value == .null) continue;
                const text = try scalarJsonValueTextAlloc(alloc, parsed.value);
                defer alloc.free(text);
                try joined.appendSlice(alloc, text);
            }
            break :blk try std.json.Stringify.valueAlloc(alloc, std.json.Value{ .string = joined.items }, .{});
        },
        .concat_ws => blk: {
            if (expression.operands.len < 2) return error.UnsupportedSqlShape;
            const separator_json = try expressionValueJsonAlloc(alloc, expression.operands[0], values);
            defer alloc.free(separator_json);
            var parsed_separator = std.json.parseFromSlice(std.json.Value, alloc, separator_json, .{}) catch return error.UnsupportedSqlShape;
            defer parsed_separator.deinit();
            if (parsed_separator.value == .null) break :blk try alloc.dupe(u8, "null");
            if (parsed_separator.value != .string) return error.UnsupportedSqlShape;

            var joined = std.ArrayListUnmanaged(u8).empty;
            defer joined.deinit(alloc);
            var emitted = false;
            for (expression.operands[1..]) |operand| {
                const value_json = try expressionValueJsonAlloc(alloc, operand, values);
                defer alloc.free(value_json);
                var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.UnsupportedSqlShape;
                defer parsed.deinit();
                if (parsed.value == .null) continue;
                if (parsed.value != .string) return error.UnsupportedSqlShape;
                if (emitted) try joined.appendSlice(alloc, parsed_separator.value.string);
                try joined.appendSlice(alloc, parsed.value.string);
                emitted = true;
            }
            break :blk try std.json.Stringify.valueAlloc(alloc, std.json.Value{ .string = joined.items }, .{});
        },
        else => error.UnsupportedSqlShape,
    };
}

pub fn fieldValueJsonFor(values: anytype, field: []const u8) ?[]const u8 {
    for (values) |value| {
        if (std.mem.eql(u8, value.field, field)) return value.value_json;
    }
    return null;
}

pub fn fieldValuesContain(values: anytype, field: []const u8) bool {
    return fieldValueJsonFor(values, field) != null;
}

pub fn fieldValuesMatchColumns(values: anytype, columns: []const []const u8) bool {
    if (values.len != columns.len) return false;
    for (columns) |column| {
        if (fieldValueJsonFor(values, column) == null) return false;
    }
    return true;
}

pub fn writeFieldValuesObjectJson(
    writer: *std.Io.Writer,
    values: anytype,
    columns: []const []const u8,
) !void {
    try writer.writeByte('{');
    for (columns, 0..) |column, i| {
        const value_json = fieldValueJsonFor(values, column) orelse return error.UnsupportedSqlShape;
        if (i != 0) try writer.writeByte(',');
        try writer.print("{f}:", .{std.json.fmt(column, .{})});
        try writer.writeAll(value_json);
    }
    try writer.writeByte('}');
}

pub fn writeAllFieldValuesObjectJson(
    writer: *std.Io.Writer,
    values: anytype,
) !void {
    try writer.writeByte('{');
    for (values, 0..) |value, i| {
        if (i != 0) try writer.writeByte(',');
        try writer.print("{f}:", .{std.json.fmt(value.field, .{})});
        try writer.writeAll(value.value_json);
    }
    try writer.writeByte('}');
}

fn scalarJsonValueTextAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]u8 {
    return switch (value) {
        .string => |text| try alloc.dupe(u8, text),
        .integer => |integer| try std.fmt.allocPrint(alloc, "{d}", .{integer}),
        .float => |float| try std.fmt.allocPrint(alloc, "{d}", .{float}),
        .number_string => |text| try alloc.dupe(u8, text),
        .bool => |enabled| try alloc.dupe(u8, if (enabled) "true" else "false"),
        else => error.UnsupportedSqlShape,
    };
}

pub fn jsonValuesEqual(a: std.json.Value, b: std.json.Value) bool {
    if (a == .null or b == .null) return a == .null and b == .null;
    if (a == .bool and b == .bool) return a.bool == b.bool;
    if (a == .string and b == .string) return std.mem.eql(u8, a.string, b.string);
    if (jsonNumber(a)) |left| {
        if (jsonNumber(b)) |right| return left == right;
    }
    return false;
}

pub const JsonOrder = enum { lt, eq, gt };

pub fn compareJsonScalars(a: std.json.Value, b: std.json.Value) ?JsonOrder {
    if (jsonNumber(a)) |left| {
        if (jsonNumber(b)) |right| {
            if (left < right) return .lt;
            if (left > right) return .gt;
            return .eq;
        }
    }
    if (a == .string and b == .string) {
        const order = std.mem.order(u8, a.string, b.string);
        return switch (order) {
            .lt => .lt,
            .eq => .eq,
            .gt => .gt,
        };
    }
    return null;
}

fn jsonNumber(value: std.json.Value) ?f64 {
    return switch (value) {
        .integer => |integer| @floatFromInt(integer),
        .float => |float| float,
        .number_string => |text| std.fmt.parseFloat(f64, text) catch null,
        else => null,
    };
}

pub fn findUniqueConstraintByColumnSet(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    values: anytype,
) ?runtime_schema.UniqueConstraint {
    for (schema.unique_constraints) |constraint| {
        if (constraint.validation_state != .enforced) continue;
        if (constraint.expressions.len != 0) continue;
        if (uniqueConstraintMatchesPointSelector(alloc, constraint, values)) return constraint;
    }
    return null;
}

fn uniqueConstraintMatchesPointSelector(
    alloc: std.mem.Allocator,
    constraint: runtime_schema.UniqueConstraint,
    values: anytype,
) bool {
    if (constraint.where.len == 0 and constraint.where_expressions.len == 0) return fieldValuesMatchColumns(values, constraint.columns);
    for (constraint.columns) |column| {
        if (fieldValueJsonFor(values, column) == null) return false;
    }
    for (values) |value| {
        if (!uniqueConstraintAllowsPointSelectorField(constraint, value.field)) return false;
    }
    for (constraint.where) |predicate| {
        if (!uniquePredicateProvenByFieldValues(predicate, values)) return false;
    }
    for (constraint.where_expressions) |condition| {
        if (!(uniqueExpressionPredicateProvenByFieldValues(alloc, condition, values) catch false)) return false;
    }
    return true;
}

fn uniqueConstraintAllowsPointSelectorField(constraint: runtime_schema.UniqueConstraint, field: []const u8) bool {
    if (stringSlicesContains(constraint.columns, field)) return true;
    for (constraint.where) |predicate| {
        if (std.mem.eql(u8, predicate.field, field)) return true;
    }
    for (constraint.where_expressions) |condition| {
        if (expr_type.expressionConditionReferencesField(condition, field)) return true;
    }
    return false;
}

fn uniquePredicateProvenByFieldValues(predicate: runtime_schema.UniquePredicate, values: anytype) bool {
    const value_json = fieldValueJsonFor(values, predicate.field) orelse return false;
    return switch (predicate.op) {
        .eq => if (predicate.value_json) |expected| std.mem.eql(u8, value_json, expected) else false,
        .ne => if (predicate.value_json) |forbidden|
            !std.mem.eql(u8, value_json, "null") and !std.mem.eql(u8, value_json, forbidden)
        else
            false,
        .is_not_null => !std.mem.eql(u8, value_json, "null"),
        .is_null => std.mem.eql(u8, value_json, "null"),
    };
}

fn uniqueExpressionPredicateProvenByFieldValues(
    alloc: std.mem.Allocator,
    condition: db_mod.types.RelationalRowsExpressionCondition,
    values: anytype,
) !bool {
    const lhs_json = try expressionValueJsonAlloc(alloc, condition.lhs, values);
    defer alloc.free(lhs_json);
    var lhs = std.json.parseFromSlice(std.json.Value, alloc, lhs_json, .{}) catch return false;
    defer lhs.deinit();

    return switch (condition.op) {
        .is_null => lhs.value == .null,
        .is_not_null => lhs.value != .null,
        .eq, .ne, .is_distinct, .is_not_distinct => blk: {
            if (condition.rhs.len != 1) return false;
            const rhs_json = try expressionValueJsonAlloc(alloc, condition.rhs[0], values);
            defer alloc.free(rhs_json);
            var rhs = std.json.parseFromSlice(std.json.Value, alloc, rhs_json, .{}) catch return false;
            defer rhs.deinit();
            const equal = jsonValuesEqual(lhs.value, rhs.value);
            break :blk switch (condition.op) {
                .eq, .is_not_distinct => equal,
                .ne, .is_distinct => !equal,
                else => unreachable,
            };
        },
        .gt, .gte, .lt, .lte => blk: {
            if (condition.rhs.len != 1) return false;
            const rhs_json = try expressionValueJsonAlloc(alloc, condition.rhs[0], values);
            defer alloc.free(rhs_json);
            var rhs = std.json.parseFromSlice(std.json.Value, alloc, rhs_json, .{}) catch return false;
            defer rhs.deinit();
            const comparison = compareJsonScalars(lhs.value, rhs.value) orelse return false;
            break :blk switch (condition.op) {
                .gt => comparison == .gt,
                .gte => comparison == .gt or comparison == .eq,
                .lt => comparison == .lt,
                .lte => comparison == .lt or comparison == .eq,
                else => unreachable,
            };
        },
    };
}

pub fn conflictInsertedValueForColumn(insert_columns: []const []const u8, row: []const []const u8, column: []const u8) ?[]const u8 {
    if (insert_columns.len != row.len) return null;
    for (insert_columns, row) |insert_column, value_json| {
        if (std.mem.eql(u8, insert_column, column)) return value_json;
    }
    return null;
}

pub fn findUniqueConstraintByColumnsAndExpressions(
    schema: runtime_schema.TableSchema,
    columns: []const []const u8,
    expressions: []const runtime_schema.UniqueExpression,
    require_partial: bool,
) ?runtime_schema.UniqueConstraint {
    for (schema.unique_constraints) |constraint| {
        if (constraint.validation_state != .enforced) continue;
        if (require_partial and constraint.where.len == 0) continue;
        if (!require_partial and constraint.where.len != 0) continue;
        if (!stringSlicesEqual(constraint.columns, columns)) continue;
        if (!expr_equal.uniqueExpressionsEqual(constraint.expressions, expressions)) continue;
        return constraint;
    }
    return null;
}

pub fn findUniqueConstraintByColumnsExpressionsAndConflictWhere(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    columns: []const []const u8,
    expressions: []const runtime_schema.UniqueExpression,
    where_json: []const u8,
    where_expressions: []const db_mod.types.RelationalRowsExpressionCondition,
) !?runtime_schema.UniqueConstraint {
    const has_field_where = where_json.len > 0;
    const has_expression_where = where_expressions.len > 0;
    if (has_field_where and has_expression_where) return error.UnsupportedSqlShape;

    for (schema.unique_constraints) |constraint| {
        if (constraint.validation_state != .enforced) continue;
        if (!stringSlicesEqual(constraint.columns, columns)) continue;
        if (!expr_equal.uniqueExpressionsEqual(constraint.expressions, expressions)) continue;

        if (!has_field_where and !has_expression_where) {
            if (constraint.where.len == 0 and constraint.where_expressions.len == 0) return constraint;
            continue;
        }

        if (has_field_where) {
            if (constraint.where.len != 0 and constraint.where_expressions.len == 0) {
                validateUniqueWhereJsonMatches(alloc, where_json, constraint.where) catch continue;
                return constraint;
            }
            if (constraint.where.len == 0 and constraint.where_expressions.len != 0) {
                const predicates = try relationalChecksFromUniqueWhereJsonAlloc(alloc, where_json);
                defer {
                    freeRelationalChecks(alloc, predicates);
                    if (predicates.len > 0) alloc.free(predicates);
                }
                if (try db_mod.DB.relationalRowsExpressionConditionsImpliedByEqualityPredicatesAlloc(
                    alloc,
                    predicates,
                    constraint.where_expressions,
                )) return constraint;
            }
            continue;
        }

        if (constraint.where.len != 0) continue;
        if (expr_equal.relationalRowsExpressionConditionsEqual(constraint.where_expressions, where_expressions)) return constraint;
    }
    return null;
}

fn relationalChecksFromUniqueWhereJsonAlloc(
    alloc: std.mem.Allocator,
    where_json: []const u8,
) ![]runtime_schema.RelationalCheck {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, where_json, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    if (parsed.value != .object) return error.UnsupportedSqlShape;
    const all_value = parsed.value.object.get("all") orelse return error.UnsupportedSqlShape;
    if (all_value != .array) return error.UnsupportedSqlShape;
    const out = try alloc.alloc(runtime_schema.RelationalCheck, all_value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |check| {
            alloc.free(check.field);
            if (check.value_json) |json| alloc.free(json);
        }
        alloc.free(out);
    }
    for (all_value.array.items) |item| {
        if (item != .object) return error.UnsupportedSqlShape;
        const field_value = item.object.get("field") orelse return error.UnsupportedSqlShape;
        const op_value = item.object.get("op") orelse return error.UnsupportedSqlShape;
        if (field_value != .string or op_value != .string) return error.UnsupportedSqlShape;
        const op = expr_type.relationalCheckOpFromUniquePredicateToken(op_value.string) orelse return error.UnsupportedSqlShape;
        const field = try alloc.dupe(u8, field_value.string);
        var field_transferred = false;
        errdefer if (!field_transferred) alloc.free(field);
        const value_json: ?[]const u8 = if (item.object.get("value")) |value| try std.json.Stringify.valueAlloc(alloc, value, .{}) else null;
        var value_transferred = false;
        errdefer if (!value_transferred) if (value_json) |json| alloc.free(json);
        out[initialized] = .{
            .name = "",
            .field = field,
            .op = op,
            .value_json = value_json,
        };
        initialized += 1;
        field_transferred = true;
        value_transferred = true;
    }
    return out;
}

fn validateUniqueWhereJsonMatches(alloc: std.mem.Allocator, where_json: []const u8, predicates: []const runtime_schema.UniquePredicate) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, where_json, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    if (parsed.value != .object) return error.UnsupportedSqlShape;
    const all_value = parsed.value.object.get("all") orelse return error.UnsupportedSqlShape;
    if (all_value != .array or all_value.array.items.len != predicates.len) return error.UnsupportedSqlShape;
    for (all_value.array.items, predicates) |item, predicate| {
        if (item != .object) return error.UnsupportedSqlShape;
        const field_value = item.object.get("field") orelse return error.UnsupportedSqlShape;
        const op_value = item.object.get("op") orelse return error.UnsupportedSqlShape;
        if (field_value != .string or !std.mem.eql(u8, field_value.string, predicate.field)) return error.UnsupportedSqlShape;
        if (op_value != .string or !std.mem.eql(u8, op_value.string, expr_type.uniquePredicateOpToken(predicate.op))) return error.UnsupportedSqlShape;
        const supplied_value = item.object.get("value");
        if (predicate.value_json) |expected_json| {
            const supplied = supplied_value orelse return error.UnsupportedSqlShape;
            const supplied_json = try std.json.Stringify.valueAlloc(alloc, supplied, .{});
            defer alloc.free(supplied_json);
            if (!std.mem.eql(u8, supplied_json, expected_json)) return error.UnsupportedSqlShape;
        } else if (supplied_value != null) {
            return error.UnsupportedSqlShape;
        }
    }
}

fn stringSlicesEqual(a: []const []const u8, b: []const []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| {
        if (!std.mem.eql(u8, left, right)) return false;
    }
    return true;
}

fn stringSlicesContains(values: []const []const u8, value: []const u8) bool {
    for (values) |candidate| {
        if (std.mem.eql(u8, candidate, value)) return true;
    }
    return false;
}

test "sql expr_selector handles selector values and json scalars" {
    const SelectorValue = struct {
        field: []const u8,
        value_json: []const u8,
    };
    const selector_values = [_]SelectorValue{
        .{ .field = "status", .value_json = "\"Active User\"" },
        .{ .field = "tenant_id", .value_json = "\"tenant-a\"" },
        .{ .field = "amount", .value_json = "42" },
    };
    const lower_selector: db_mod.types.RelationalRowsExpression = .{
        .kind = .lower,
        .operands = &.{.{ .kind = .field, .field = "status", .field_source = .row }},
    };
    const lower_json = try expressionValueJsonAlloc(std.testing.allocator, lower_selector, &selector_values);
    defer std.testing.allocator.free(lower_json);
    try std.testing.expectEqualStrings("\"active user\"", lower_json);

    const concat_selector: db_mod.types.RelationalRowsExpression = .{
        .kind = .concat_ws,
        .operands = &.{
            .{ .kind = .value, .value_json = "\"/\"" },
            .{ .kind = .field, .field = "tenant_id", .field_source = .row },
            .{ .kind = .field, .field = "status", .field_source = .row },
        },
    };
    const concat_json = try expressionValueJsonAlloc(std.testing.allocator, concat_selector, &selector_values);
    defer std.testing.allocator.free(concat_json);
    try std.testing.expectEqualStrings("\"tenant-a/Active User\"", concat_json);
    try std.testing.expectEqualStrings("\"Active User\"", fieldValueJsonFor(&selector_values, "status").?);
    try std.testing.expect(fieldValuesContain(&selector_values, "tenant_id"));
    try std.testing.expect(!fieldValuesContain(&selector_values, "missing"));
    try std.testing.expect(fieldValuesMatchColumns(&selector_values, &.{ "status", "tenant_id", "amount" }));
    try std.testing.expect(!fieldValuesMatchColumns(&selector_values, &.{ "status", "tenant_id" }));

    var selected_fields: std.Io.Writer.Allocating = .init(std.testing.allocator);
    errdefer selected_fields.deinit();
    try writeFieldValuesObjectJson(&selected_fields.writer, &selector_values, &.{ "tenant_id", "status" });
    const selected_json = try selected_fields.toOwnedSlice();
    defer std.testing.allocator.free(selected_json);
    try std.testing.expectEqualStrings("{\"tenant_id\":\"tenant-a\",\"status\":\"Active User\"}", selected_json);

    var all_fields: std.Io.Writer.Allocating = .init(std.testing.allocator);
    errdefer all_fields.deinit();
    try writeAllFieldValuesObjectJson(&all_fields.writer, &selector_values);
    const all_json = try all_fields.toOwnedSlice();
    defer std.testing.allocator.free(all_json);
    try std.testing.expectEqualStrings("{\"status\":\"Active User\",\"tenant_id\":\"tenant-a\",\"amount\":42}", all_json);

    var left_number = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, "42", .{});
    defer left_number.deinit();
    var right_number = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, "43", .{});
    defer right_number.deinit();
    try std.testing.expectEqual(JsonOrder.lt, compareJsonScalars(left_number.value, right_number.value) orelse return error.TestUnexpectedResult);
    try std.testing.expect(jsonValuesEqual(left_number.value, left_number.value));
}
