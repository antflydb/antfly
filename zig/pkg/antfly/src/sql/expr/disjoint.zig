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

const db_mod = @import("../../storage/db/mod.zig");
const expr_equal = @import("equal.zig");
const runtime_schema = @import("../../storage/schema.zig");

pub fn relationalChecksProvablyDisjoint(
    lhs: runtime_schema.RelationalCheck,
    rhs: runtime_schema.RelationalCheck,
) bool {
    if (lhs.expression) |left| {
        if (rhs.expression) |right| return expressionConditionsProvablyDisjoint(left, right);
        return relationalCheckAndExpressionConditionProvablyDisjoint(rhs, left);
    }
    if (rhs.expression) |right| return relationalCheckAndExpressionConditionProvablyDisjoint(lhs, right);
    if (!std.mem.eql(u8, lhs.field, rhs.field)) return false;
    return simplePredicateOpsProvablyDisjoint(lhs.op, lhs.value_json, rhs.op, rhs.value_json);
}

pub fn relationalCheckAndExpressionConditionProvablyDisjoint(
    check: runtime_schema.RelationalCheck,
    condition: db_mod.types.RelationalRowsExpressionCondition,
) bool {
    if (check.expression) |check_condition| return expressionConditionsProvablyDisjoint(check_condition, condition);
    if (!expressionIsRowField(condition.lhs, check.field)) return false;
    return simplePredicateOpsProvablyDisjoint(check.op, check.value_json, condition.op, expressionConditionSingleValueJson(condition));
}

pub fn expressionConditionsProvablyDisjoint(
    lhs: db_mod.types.RelationalRowsExpressionCondition,
    rhs: db_mod.types.RelationalRowsExpressionCondition,
) bool {
    if (!expr_equal.relationalRowsExpressionEqual(lhs.lhs, rhs.lhs)) return false;
    return simplePredicateOpsProvablyDisjoint(lhs.op, expressionConditionSingleValueJson(lhs), rhs.op, expressionConditionSingleValueJson(rhs));
}

fn expressionIsRowField(expression: db_mod.types.RelationalRowsExpression, field: []const u8) bool {
    return expression.kind == .field and
        expression.field_source == .row and
        std.mem.eql(u8, expression.field, field);
}

fn expressionConditionSingleValueJson(condition: db_mod.types.RelationalRowsExpressionCondition) ?[]const u8 {
    if (condition.rhs.len != 1) return null;
    if (condition.rhs[0].kind != .value) return null;
    return condition.rhs[0].value_json;
}

fn simplePredicateOpsProvablyDisjoint(
    lhs_op: runtime_schema.RelationalCheckOp,
    lhs_value_json: ?[]const u8,
    rhs_op: runtime_schema.RelationalCheckOp,
    rhs_value_json: ?[]const u8,
) bool {
    if (lhs_op == .eq and rhs_op == .eq) return jsonScalarLiteralsDefinitelyDistinct(lhs_value_json, rhs_value_json);
    if (jsonScalarLiteralsExactlyEqualForComplement(lhs_value_json, rhs_value_json)) {
        if ((lhs_op == .eq and rhs_op == .ne) or (lhs_op == .ne and rhs_op == .eq)) return true;
        if ((lhs_op == .eq and rhs_op == .is_distinct) or (rhs_op == .eq and lhs_op == .is_distinct)) return true;
        if ((lhs_op == .is_not_distinct and rhs_op == .is_distinct) or (lhs_op == .is_distinct and rhs_op == .is_not_distinct)) return true;
        if (jsonScalarLiteralDefinitelyNonNull(lhs_value_json) and
            ((lhs_op == .is_not_distinct and rhs_op == .ne) or (lhs_op == .ne and rhs_op == .is_not_distinct)))
        {
            return true;
        }
    }
    if (lhs_op == .is_null and rhs_op == .is_not_null) return true;
    if (lhs_op == .is_not_null and rhs_op == .is_null) return true;
    if (lhs_op == .is_null and rhs_op == .eq) return jsonScalarLiteralDefinitelyNonNull(rhs_value_json);
    if (lhs_op == .eq and rhs_op == .is_null) return jsonScalarLiteralDefinitelyNonNull(lhs_value_json);
    if (numericRangePredicatesProvablyDisjoint(lhs_op, lhs_value_json, rhs_op, rhs_value_json)) return true;
    return false;
}

const SimpleNumericBound = struct {
    value: f64,
    inclusive: bool,
};

fn numericRangePredicatesProvablyDisjoint(
    lhs_op: runtime_schema.RelationalCheckOp,
    lhs_value_json: ?[]const u8,
    rhs_op: runtime_schema.RelationalCheckOp,
    rhs_value_json: ?[]const u8,
) bool {
    if (upperNumericBound(lhs_op, lhs_value_json)) |left_upper| {
        if (lowerNumericBound(rhs_op, rhs_value_json)) |right_lower| {
            return numericUpperLowerBoundsProvablyDisjoint(left_upper, right_lower);
        }
    }
    if (lowerNumericBound(lhs_op, lhs_value_json)) |left_lower| {
        if (upperNumericBound(rhs_op, rhs_value_json)) |right_upper| {
            return numericUpperLowerBoundsProvablyDisjoint(right_upper, left_lower);
        }
    }
    return false;
}

fn upperNumericBound(op: runtime_schema.RelationalCheckOp, value_json: ?[]const u8) ?SimpleNumericBound {
    return switch (op) {
        .lt => .{ .value = jsonNumberLiteralValue(value_json) orelse return null, .inclusive = false },
        .lte => .{ .value = jsonNumberLiteralValue(value_json) orelse return null, .inclusive = true },
        else => null,
    };
}

fn lowerNumericBound(op: runtime_schema.RelationalCheckOp, value_json: ?[]const u8) ?SimpleNumericBound {
    return switch (op) {
        .gt => .{ .value = jsonNumberLiteralValue(value_json) orelse return null, .inclusive = false },
        .gte => .{ .value = jsonNumberLiteralValue(value_json) orelse return null, .inclusive = true },
        else => null,
    };
}

fn numericUpperLowerBoundsProvablyDisjoint(upper: SimpleNumericBound, lower: SimpleNumericBound) bool {
    if (lower.value > upper.value) return true;
    if (lower.value < upper.value) return false;
    return !(upper.inclusive and lower.inclusive);
}

fn jsonScalarLiteralsDefinitelyDistinct(lhs: ?[]const u8, rhs: ?[]const u8) bool {
    const left = lhs orelse return false;
    const right = rhs orelse return false;
    if (!jsonIsSafeDisjointProofLiteral(left) or !jsonIsSafeDisjointProofLiteral(right)) return false;
    return !std.mem.eql(u8, left, right);
}

fn jsonScalarLiteralsExactlyEqualForComplement(lhs: ?[]const u8, rhs: ?[]const u8) bool {
    const left = lhs orelse return false;
    const right = rhs orelse return false;
    if (!std.mem.eql(u8, left, right)) return false;
    return jsonIsSafeDisjointProofLiteral(left) or jsonIsJsonNumberLiteral(left);
}

pub fn jsonIsSafeDisjointProofLiteral(value: []const u8) bool {
    return (value.len >= 2 and value[0] == '"' and value[value.len - 1] == '"') or
        std.mem.eql(u8, value, "true") or
        std.mem.eql(u8, value, "false") or
        std.mem.eql(u8, value, "null");
}

fn jsonScalarLiteralDefinitelyNonNull(value: ?[]const u8) bool {
    const json = value orelse return false;
    return (jsonIsSafeDisjointProofLiteral(json) and !std.mem.eql(u8, json, "null")) or jsonIsJsonNumberLiteral(json);
}

pub fn jsonIsJsonNumberLiteral(value: []const u8) bool {
    if (value.len == 0) return false;
    var index: usize = 0;
    if (value[index] == '-') {
        index += 1;
        if (index == value.len) return false;
    }

    if (value[index] == '0') {
        index += 1;
    } else if (std.ascii.isDigit(value[index]) and value[index] != '0') {
        while (index < value.len and std.ascii.isDigit(value[index])) index += 1;
    } else {
        return false;
    }

    if (index < value.len and value[index] == '.') {
        index += 1;
        const start = index;
        while (index < value.len and std.ascii.isDigit(value[index])) index += 1;
        if (index == start) return false;
    }

    if (index < value.len and (value[index] == 'e' or value[index] == 'E')) {
        index += 1;
        if (index < value.len and (value[index] == '+' or value[index] == '-')) index += 1;
        const start = index;
        while (index < value.len and std.ascii.isDigit(value[index])) index += 1;
        if (index == start) return false;
    }

    return index == value.len;
}

fn jsonNumberLiteralValue(value: ?[]const u8) ?f64 {
    const json = value orelse return null;
    if (!jsonIsJsonNumberLiteral(json)) return null;
    const parsed = std.fmt.parseFloat(f64, json) catch return null;
    if (!std.math.isFinite(parsed)) return null;
    return parsed;
}

test "sql expr_disjoint proves simple predicate disjointness" {
    const status_field: runtime_schema.RelationalRowsExpression = .{ .kind = .field, .field = "status" };
    const active_literal: runtime_schema.RelationalRowsExpression = .{ .kind = .value, .value_json = "\"active\"" };
    const inactive_literal: runtime_schema.RelationalRowsExpression = .{ .kind = .value, .value_json = "\"inactive\"" };
    const active_condition: runtime_schema.RelationalRowsExpressionCondition = .{
        .lhs = status_field,
        .op = .eq,
        .rhs = &.{active_literal},
    };
    const inactive_condition: runtime_schema.RelationalRowsExpressionCondition = .{
        .lhs = status_field,
        .op = .eq,
        .rhs = &.{inactive_literal},
    };

    try std.testing.expect(relationalChecksProvablyDisjoint(
        .{ .name = "", .field = "status", .op = .eq, .value_json = "\"active\"" },
        .{ .name = "", .field = "status", .op = .eq, .value_json = "\"inactive\"" },
    ));
    try std.testing.expect(relationalCheckAndExpressionConditionProvablyDisjoint(
        .{ .name = "", .field = "status", .op = .eq, .value_json = "\"active\"" },
        inactive_condition,
    ));
    try std.testing.expect(expressionConditionsProvablyDisjoint(active_condition, inactive_condition));
    try std.testing.expect(relationalChecksProvablyDisjoint(
        .{ .name = "", .field = "amount", .op = .lt, .value_json = "10" },
        .{ .name = "", .field = "amount", .op = .gte, .value_json = "10" },
    ));
    try std.testing.expect(jsonIsSafeDisjointProofLiteral("\"active\""));
    try std.testing.expect(jsonIsJsonNumberLiteral("-10.5e2"));
    try std.testing.expect(!jsonIsJsonNumberLiteral("01"));
}
