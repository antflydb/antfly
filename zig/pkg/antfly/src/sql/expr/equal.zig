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
    pub const types = @import("../../storage/db/types.zig");
};
const runtime_schema = @import("../../storage/schema.zig");

fn optionalStringEqual(lhs: ?[]const u8, rhs: ?[]const u8) bool {
    if (lhs == null or rhs == null) return lhs == null and rhs == null;
    return std.mem.eql(u8, lhs.?, rhs.?);
}

fn floatSlicesEqual(lhs: []const f64, rhs: []const f64) bool {
    if (lhs.len != rhs.len) return false;
    for (lhs, rhs) |left, right| {
        if (left != right) return false;
    }
    return true;
}

fn orderByEqual(lhs: []const db_mod.types.RelationalRowsQueryOrder, rhs: []const db_mod.types.RelationalRowsQueryOrder) bool {
    if (lhs.len != rhs.len) return false;
    for (lhs, rhs) |lhs_order, rhs_order| {
        if (lhs_order.direction != rhs_order.direction or
            lhs_order.null_test != rhs_order.null_test or
            !std.mem.eql(u8, lhs_order.field, rhs_order.field) or
            !relationalRowsExpressionOptionalEqual(lhs_order.expression, rhs_order.expression))
        {
            return false;
        }
    }
    return true;
}

fn relationalChecksEqual(lhs: []const runtime_schema.RelationalCheck, rhs: []const runtime_schema.RelationalCheck) bool {
    if (lhs.len != rhs.len) return false;
    for (lhs, rhs) |lhs_check, rhs_check| {
        if (!std.mem.eql(u8, lhs_check.name, rhs_check.name) or
            !std.mem.eql(u8, lhs_check.field, rhs_check.field) or
            lhs_check.op != rhs_check.op or
            !optionalStringEqual(lhs_check.value_json, rhs_check.value_json) or
            !optionalStringEqual(lhs_check.collation, rhs_check.collation))
        {
            return false;
        }
    }
    return true;
}

fn structuredValuePredicatesEqual(lhs: anytype, rhs: @TypeOf(lhs)) bool {
    if (lhs.len != rhs.len) return false;
    for (lhs, rhs) |lhs_predicate, rhs_predicate| {
        if (!std.mem.eql(u8, lhs_predicate.field, rhs_predicate.field) or
            !std.mem.eql(u8, lhs_predicate.value_json, rhs_predicate.value_json))
        {
            return false;
        }
    }
    return true;
}

fn inPredicatesEqual(
    lhs: []const db_mod.types.RelationalRowsInPredicate,
    rhs: []const db_mod.types.RelationalRowsInPredicate,
) bool {
    if (lhs.len != rhs.len) return false;
    for (lhs, rhs) |lhs_predicate, rhs_predicate| {
        if (!std.mem.eql(u8, lhs_predicate.field, rhs_predicate.field) or
            !std.mem.eql(u8, lhs_predicate.values_json, rhs_predicate.values_json) or
            lhs_predicate.negated != rhs_predicate.negated or
            !optionalStringEqual(lhs_predicate.collation, rhs_predicate.collation))
        {
            return false;
        }
    }
    return true;
}

fn jsonContainsEqual(
    lhs: []const db_mod.types.RelationalRowsJsonContainsPredicate,
    rhs: []const db_mod.types.RelationalRowsJsonContainsPredicate,
) bool {
    if (lhs.len != rhs.len) return false;
    for (lhs, rhs) |lhs_predicate, rhs_predicate| {
        if (!std.mem.eql(u8, lhs_predicate.field, rhs_predicate.field) or
            !std.mem.eql(u8, lhs_predicate.value_json, rhs_predicate.value_json))
        {
            return false;
        }
    }
    return true;
}

fn jsonPathEqEqual(
    lhs: []const db_mod.types.RelationalRowsJsonPathEqPredicate,
    rhs: []const db_mod.types.RelationalRowsJsonPathEqPredicate,
) bool {
    if (lhs.len != rhs.len) return false;
    for (lhs, rhs) |lhs_predicate, rhs_predicate| {
        if (!std.mem.eql(u8, lhs_predicate.field, rhs_predicate.field) or
            !std.mem.eql(u8, lhs_predicate.path, rhs_predicate.path) or
            !std.mem.eql(u8, lhs_predicate.value_json, rhs_predicate.value_json))
        {
            return false;
        }
    }
    return true;
}

fn jsonPathExistsEqual(
    lhs: []const db_mod.types.RelationalRowsJsonPathExistsPredicate,
    rhs: []const db_mod.types.RelationalRowsJsonPathExistsPredicate,
) bool {
    if (lhs.len != rhs.len) return false;
    for (lhs, rhs) |lhs_predicate, rhs_predicate| {
        if (!std.mem.eql(u8, lhs_predicate.field, rhs_predicate.field) or
            !std.mem.eql(u8, lhs_predicate.path, rhs_predicate.path))
        {
            return false;
        }
    }
    return true;
}

fn textPatternsEqual(
    lhs: []const db_mod.types.RelationalRowsTextPatternPredicate,
    rhs: []const db_mod.types.RelationalRowsTextPatternPredicate,
) bool {
    if (lhs.len != rhs.len) return false;
    for (lhs, rhs) |lhs_predicate, rhs_predicate| {
        if (!std.mem.eql(u8, lhs_predicate.field, rhs_predicate.field) or
            !std.mem.eql(u8, lhs_predicate.pattern, rhs_predicate.pattern) or
            lhs_predicate.case_insensitive != rhs_predicate.case_insensitive or
            lhs_predicate.negated != rhs_predicate.negated)
        {
            return false;
        }
    }
    return true;
}

pub fn relationalRowsExpressionOptionalEqual(
    lhs: ?runtime_schema.RelationalRowsExpression,
    rhs: ?runtime_schema.RelationalRowsExpression,
) bool {
    if (lhs == null or rhs == null) return lhs == null and rhs == null;
    return relationalRowsExpressionEqual(lhs.?, rhs.?);
}

pub fn relationalRowsExpressionEqual(
    lhs: runtime_schema.RelationalRowsExpression,
    rhs: runtime_schema.RelationalRowsExpression,
) bool {
    if (lhs.kind != rhs.kind or
        lhs.field_source != rhs.field_source or
        lhs.json_as_text != rhs.json_as_text or
        lhs.cast_type != rhs.cast_type or
        !std.mem.eql(u8, lhs.field, rhs.field) or
        !std.mem.eql(u8, lhs.value_json, rhs.value_json) or
        !std.mem.eql(u8, lhs.json_path, rhs.json_path) or
        lhs.operands.len != rhs.operands.len or
        lhs.case_branches.len != rhs.case_branches.len or
        lhs.case_else.len != rhs.case_else.len)
    {
        return false;
    }
    for (lhs.operands, rhs.operands) |lhs_operand, rhs_operand| {
        if (!relationalRowsExpressionEqual(lhs_operand, rhs_operand)) return false;
    }
    for (lhs.case_branches, rhs.case_branches) |lhs_branch, rhs_branch| {
        if (!relationalRowsExpressionConditionEqual(lhs_branch.when, rhs_branch.when)) return false;
        if (!relationalRowsExpressionEqual(lhs_branch.then, rhs_branch.then)) return false;
    }
    for (lhs.case_else, rhs.case_else) |lhs_else, rhs_else| {
        if (!relationalRowsExpressionEqual(lhs_else, rhs_else)) return false;
    }
    return true;
}

pub fn relationalRowsExpressionConditionsEqual(
    lhs: []const runtime_schema.RelationalRowsExpressionCondition,
    rhs: []const runtime_schema.RelationalRowsExpressionCondition,
) bool {
    if (lhs.len != rhs.len) return false;
    for (lhs, rhs) |lhs_condition, rhs_condition| {
        if (!relationalRowsExpressionConditionEqual(lhs_condition, rhs_condition)) return false;
    }
    return true;
}

pub fn relationalRowsExpressionArrayContainsEqual(
    lhs: []const runtime_schema.RelationalRowsExpressionArrayContainsPredicate,
    rhs: []const runtime_schema.RelationalRowsExpressionArrayContainsPredicate,
) bool {
    if (lhs.len != rhs.len) return false;
    for (lhs, rhs) |lhs_predicate, rhs_predicate| {
        if (!relationalRowsExpressionEqual(lhs_predicate.expression, rhs_predicate.expression) or
            !std.mem.eql(u8, lhs_predicate.value_json, rhs_predicate.value_json))
        {
            return false;
        }
    }
    return true;
}

pub fn relationalRowsExpressionPredicateGroupsEqual(
    lhs: []const runtime_schema.RelationalRowsExpressionPredicateGroup,
    rhs: []const runtime_schema.RelationalRowsExpressionPredicateGroup,
) bool {
    if (lhs.len != rhs.len) return false;
    for (lhs, rhs) |lhs_group, rhs_group| {
        if (!relationalRowsExpressionConditionsEqual(lhs_group.conditions, rhs_group.conditions)) return false;
    }
    return true;
}

pub fn relationalRowsExpressionConditionEqual(
    lhs: runtime_schema.RelationalRowsExpressionCondition,
    rhs: runtime_schema.RelationalRowsExpressionCondition,
) bool {
    if (lhs.op != rhs.op or lhs.rhs.len != rhs.rhs.len or !relationalRowsExpressionEqual(lhs.lhs, rhs.lhs)) return false;
    for (lhs.rhs, rhs.rhs) |lhs_rhs, rhs_rhs| {
        if (!relationalRowsExpressionEqual(lhs_rhs, rhs_rhs)) return false;
    }
    return true;
}

pub fn aggregateSpecsEquivalent(
    lhs: db_mod.types.RelationalRowsAggregateSpec,
    rhs: db_mod.types.RelationalRowsAggregateSpec,
) bool {
    if (lhs.op != rhs.op or lhs.distinct != rhs.distinct) return false;
    if (lhs.percentile_max_items != rhs.percentile_max_items) return false;
    if (lhs.percentile_order != rhs.percentile_order) return false;
    if (lhs.percentile == null or rhs.percentile == null) {
        if (lhs.percentile != null or rhs.percentile != null) return false;
    } else if (lhs.percentile.? != rhs.percentile.?) return false;
    if (!floatSlicesEqual(lhs.percentiles, rhs.percentiles)) return false;
    if (!optionalStringEqual(lhs.field, rhs.field)) return false;
    if (!optionalStringEqual(lhs.string_delimiter, rhs.string_delimiter)) return false;
    if (!relationalRowsExpressionOptionalEqual(lhs.expression, rhs.expression)) return false;
    if (!orderByEqual(lhs.array_order_by, rhs.array_order_by)) return false;
    if (!relationalChecksEqual(lhs.filter_predicates, rhs.filter_predicates)) return false;
    if (!structuredValuePredicatesEqual(lhs.filter_array_any, rhs.filter_array_any)) return false;
    if (!structuredValuePredicatesEqual(lhs.filter_array_contains, rhs.filter_array_contains)) return false;
    if (!structuredValuePredicatesEqual(lhs.filter_array_eq, rhs.filter_array_eq)) return false;
    if (!inPredicatesEqual(lhs.filter_in_predicates, rhs.filter_in_predicates)) return false;
    if (!jsonContainsEqual(lhs.filter_json_contains, rhs.filter_json_contains)) return false;
    if (!jsonPathEqEqual(lhs.filter_json_path_eq, rhs.filter_json_path_eq)) return false;
    if (!jsonPathExistsEqual(lhs.filter_json_path_exists, rhs.filter_json_path_exists)) return false;
    if (!textPatternsEqual(lhs.filter_text_patterns, rhs.filter_text_patterns)) return false;
    if (!relationalRowsExpressionConditionsEqual(lhs.filter_expressions, rhs.filter_expressions)) return false;
    if (!relationalRowsExpressionArrayContainsEqual(lhs.filter_expression_array_contains, rhs.filter_expression_array_contains)) return false;
    if (!relationalRowsExpressionPredicateGroupsEqual(lhs.filter_any, rhs.filter_any)) return false;
    if (!relationalRowsExpressionPredicateGroupsEqual(lhs.filter_not, rhs.filter_not)) return false;
    return true;
}

pub fn uniqueExpressionsEqual(a: []const runtime_schema.UniqueExpression, b: []const runtime_schema.UniqueExpression) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| {
        if (left.op != right.op) return false;
        if (!std.mem.eql(u8, left.field, right.field)) return false;
        if (!relationalRowsExpressionOptionalEqual(left.expression, right.expression)) return false;
    }
    return true;
}

pub fn expressionProjectionsEqual(
    lhs: []const db_mod.types.RelationalRowsExpressionProjection,
    rhs: []const db_mod.types.RelationalRowsExpressionProjection,
) bool {
    if (lhs.len != rhs.len) return false;
    for (lhs, rhs) |left, right| {
        if (!std.mem.eql(u8, left.output, right.output)) return false;
        if (!relationalRowsExpressionEqual(left.expression, right.expression)) return false;
    }
    return true;
}

test "sql expr_equal compares row expressions" {
    const lower_status: runtime_schema.RelationalRowsExpression = .{
        .kind = .lower,
        .operands = &.{.{ .kind = .field, .field = "status" }},
    };
    const same_lower_status: runtime_schema.RelationalRowsExpression = .{
        .kind = .lower,
        .operands = &.{.{ .kind = .field, .field = "status" }},
    };
    const upper_status: runtime_schema.RelationalRowsExpression = .{
        .kind = .upper,
        .operands = &.{.{ .kind = .field, .field = "status" }},
    };

    try std.testing.expect(relationalRowsExpressionEqual(lower_status, same_lower_status));
    try std.testing.expect(!relationalRowsExpressionEqual(lower_status, upper_status));
    try std.testing.expect(relationalRowsExpressionOptionalEqual(lower_status, same_lower_status));
    try std.testing.expect(!relationalRowsExpressionOptionalEqual(lower_status, null));
}

test "sql expr_equal compares aggregate specs" {
    const lower_status: runtime_schema.RelationalRowsExpression = .{
        .kind = .lower,
        .operands = &.{.{ .kind = .field, .field = "status" }},
    };
    const order_by = [_]db_mod.types.RelationalRowsQueryOrder{.{
        .field = "status",
        .expression = lower_status,
        .direction = .asc,
    }};
    const filters = [_]runtime_schema.RelationalCheck{.{
        .name = "status_check",
        .field = "status",
        .op = .eq,
        .value_json = "\"open\"",
    }};
    const json_filters = [_]db_mod.types.RelationalRowsJsonContainsPredicate{.{
        .field = "metadata",
        .value_json = "{\"source\":\"sql\"}",
    }};

    const lhs: db_mod.types.RelationalRowsAggregateSpec = .{
        .name = "statuses",
        .op = .array_agg,
        .field = "status",
        .array_order_by = &order_by,
        .filter_predicates = &filters,
        .filter_json_contains = &json_filters,
    };
    const same: db_mod.types.RelationalRowsAggregateSpec = .{
        .name = "different_output_name",
        .op = .array_agg,
        .field = "status",
        .array_order_by = &order_by,
        .filter_predicates = &filters,
        .filter_json_contains = &json_filters,
    };
    const different: db_mod.types.RelationalRowsAggregateSpec = .{
        .name = "statuses",
        .op = .array_agg,
        .field = "status",
        .array_order_by = &.{.{
            .field = "status",
            .expression = lower_status,
            .direction = .desc,
        }},
        .filter_predicates = &filters,
        .filter_json_contains = &json_filters,
    };

    try std.testing.expect(aggregateSpecsEquivalent(lhs, same));
    try std.testing.expect(!aggregateSpecsEquivalent(lhs, different));
}

test "sql expr_equal compares unique expressions and projections" {
    const lower_status: runtime_schema.RelationalRowsExpression = .{
        .kind = .lower,
        .operands = &.{.{ .kind = .field, .field = "status" }},
    };
    const same_unique_expressions = [_]runtime_schema.UniqueExpression{.{ .op = .expression, .expression = lower_status }};
    const equal_unique_expressions = [_]runtime_schema.UniqueExpression{.{ .op = .expression, .expression = lower_status }};
    const different_unique_expressions = [_]runtime_schema.UniqueExpression{.{ .op = .lower, .field = "tenant_id" }};
    try std.testing.expect(uniqueExpressionsEqual(&same_unique_expressions, &equal_unique_expressions));
    try std.testing.expect(!uniqueExpressionsEqual(&same_unique_expressions, &different_unique_expressions));

    const projections = [_]db_mod.types.RelationalRowsExpressionProjection{.{
        .output = "status_lower",
        .expression = lower_status,
    }};
    const equal_projections = [_]db_mod.types.RelationalRowsExpressionProjection{.{
        .output = "status_lower",
        .expression = lower_status,
    }};
    const different_projections = [_]db_mod.types.RelationalRowsExpressionProjection{.{
        .output = "status_upper",
        .expression = .{ .kind = .upper, .operands = &.{.{ .kind = .field, .field = "status" }} },
    }};
    try std.testing.expect(expressionProjectionsEqual(&projections, &equal_projections));
    try std.testing.expect(!expressionProjectionsEqual(&projections, &different_projections));
}
