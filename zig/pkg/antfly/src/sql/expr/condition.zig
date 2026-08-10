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
const plan_mod = @import("../plan.zig");
const runtime_schema = @import("../../storage/schema.zig");

const cloneExpressionAlloc = plan_mod.cloneExpressionAlloc;
const cloneExpressionConditionsConcatAlloc = plan_mod.cloneExpressionConditionsConcatAlloc;
const freeExpression = plan_mod.freeExpression;
const freeExpressionCondition = plan_mod.freeExpressionCondition;
const freeExpressionConditions = plan_mod.freeExpressionConditions;
const freeExpressionPredicateGroups = plan_mod.freeExpressionPredicateGroups;

pub fn booleanExpressionFromPredicateGroupsAlloc(
    alloc: std.mem.Allocator,
    groups: []db_mod.types.RelationalRowsExpressionPredicateGroup,
) !db_mod.types.RelationalRowsExpression {
    if (groups.len == 0) return error.UnsupportedSqlShape;
    if (groups.len == 1) return try booleanExpressionFromPredicateGroupAlloc(alloc, &groups[0]);

    const operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, groups.len);
    var initialized: usize = 0;
    errdefer {
        for (operands[0..initialized]) |operand| freeExpression(alloc, operand);
        alloc.free(operands);
    }
    for (groups) |*group| {
        operands[initialized] = try booleanExpressionFromPredicateGroupAlloc(alloc, group);
        initialized += 1;
    }
    return .{
        .kind = .bool_or,
        .operands = operands,
    };
}

pub fn booleanExpressionFromPredicateGroupAlloc(
    alloc: std.mem.Allocator,
    group: *db_mod.types.RelationalRowsExpressionPredicateGroup,
) !db_mod.types.RelationalRowsExpression {
    if (group.conditions.len == 0) return try booleanLiteralExpressionAlloc(alloc, true);
    if (group.conditions.len == 1) {
        const conditions = group.conditions;
        const condition = conditions[0];
        @constCast(conditions)[0] = emptyExpressionCondition();
        const expression = try booleanExpressionFromConditionAlloc(alloc, condition);
        group.conditions = &.{};
        alloc.free(conditions);
        return expression;
    }

    const operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, group.conditions.len);
    var initialized: usize = 0;
    errdefer {
        for (operands[0..initialized]) |operand| freeExpression(alloc, operand);
        alloc.free(operands);
    }
    const conditions = group.conditions;
    for (conditions, 0..) |condition, i| {
        @constCast(conditions)[i] = emptyExpressionCondition();
        operands[initialized] = try booleanExpressionFromConditionAlloc(alloc, condition);
        initialized += 1;
    }
    group.conditions = &.{};
    alloc.free(conditions);
    return .{
        .kind = .bool_and,
        .operands = operands,
    };
}

fn emptyExpressionCondition() db_mod.types.RelationalRowsExpressionCondition {
    return .{
        .lhs = .{ .kind = .value },
        .op = .eq,
        .rhs = &.{},
    };
}

pub fn booleanNotExpressionAlloc(
    alloc: std.mem.Allocator,
    operand: db_mod.types.RelationalRowsExpression,
) !db_mod.types.RelationalRowsExpression {
    var operand_transferred = false;
    errdefer if (!operand_transferred) freeExpression(alloc, operand);
    const operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    var operands_transferred = false;
    errdefer if (!operands_transferred) alloc.free(operands);
    operands[0] = operand;
    operand_transferred = true;
    operands_transferred = true;
    return .{
        .kind = .bool_not,
        .operands = operands,
    };
}

pub fn booleanExpressionConditionAlloc(
    alloc: std.mem.Allocator,
    expression: db_mod.types.RelationalRowsExpression,
) !db_mod.types.RelationalRowsExpressionCondition {
    var expression_transferred = false;
    errdefer if (!expression_transferred) freeExpression(alloc, expression);
    const rhs = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    var rhs_transferred = false;
    errdefer if (!rhs_transferred) alloc.free(rhs);
    rhs[0] = try booleanLiteralExpressionAlloc(alloc, true);
    var rhs_expression_transferred = false;
    errdefer if (!rhs_expression_transferred) freeExpression(alloc, rhs[0]);
    expression_transferred = true;
    rhs_expression_transferred = true;
    rhs_transferred = true;
    return .{
        .lhs = expression,
        .op = .eq,
        .rhs = rhs,
    };
}

pub fn booleanExpressionFromConditionAlloc(
    alloc: std.mem.Allocator,
    condition: db_mod.types.RelationalRowsExpressionCondition,
) !db_mod.types.RelationalRowsExpression {
    var condition_transferred = false;
    errdefer if (!condition_transferred) freeExpressionCondition(alloc, condition);

    const branches = try alloc.alloc(db_mod.types.RelationalRowsExpressionCaseBranch, 1);
    var branches_transferred = false;
    errdefer if (!branches_transferred) alloc.free(branches);
    const then_expression = try booleanLiteralExpressionAlloc(alloc, true);
    var then_transferred = false;
    errdefer if (!then_transferred) freeExpression(alloc, then_expression);

    const fallback = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    var fallback_transferred = false;
    errdefer if (!fallback_transferred) alloc.free(fallback);
    fallback[0] = try booleanLiteralExpressionAlloc(alloc, false);
    var fallback_expression_transferred = false;
    errdefer if (!fallback_expression_transferred) freeExpression(alloc, fallback[0]);

    branches[0] = .{
        .when = condition,
        .then = then_expression,
    };
    condition_transferred = true;
    then_transferred = true;
    branches_transferred = true;
    fallback_expression_transferred = true;
    fallback_transferred = true;
    return .{
        .kind = .case,
        .case_branches = branches,
        .case_else = fallback,
    };
}

pub fn booleanLiteralExpressionAlloc(
    alloc: std.mem.Allocator,
    value: bool,
) !db_mod.types.RelationalRowsExpression {
    return .{
        .kind = .value,
        .value_json = try alloc.dupe(u8, if (value) "true" else "false"),
    };
}

pub fn appendExpressionConditionGroup(
    alloc: std.mem.Allocator,
    expression_groups: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    condition: db_mod.types.RelationalRowsExpressionCondition,
) !void {
    var condition_transferred = false;
    errdefer if (!condition_transferred) freeExpressionCondition(alloc, condition);
    const conditions = try alloc.alloc(db_mod.types.RelationalRowsExpressionCondition, 1);
    var conditions_transferred = false;
    errdefer if (!conditions_transferred) alloc.free(conditions);
    conditions[0] = condition;
    condition_transferred = true;
    try expression_groups.append(alloc, .{ .conditions = conditions });
    conditions_transferred = true;
}

pub fn andExpressionPredicateAlternatives(
    alloc: std.mem.Allocator,
    groups: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    alternatives: []const db_mod.types.RelationalRowsExpressionPredicateGroup,
) !void {
    if (groups.items.len == 0 or alternatives.len == 0) return error.UnsupportedSqlShape;
    var combined = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup).empty;
    var combined_transferred = false;
    errdefer {
        if (!combined_transferred) freeExpressionPredicateGroups(alloc, combined.items);
        combined.deinit(alloc);
    }

    for (groups.items) |base| {
        for (alternatives) |alternative| {
            const conditions = try cloneExpressionConditionsConcatAlloc(alloc, base.conditions, alternative.conditions);
            var conditions_transferred = false;
            errdefer if (!conditions_transferred) {
                freeExpressionConditions(alloc, conditions);
                if (conditions.len > 0) alloc.free(conditions);
            };
            try combined.append(alloc, .{ .conditions = conditions });
            conditions_transferred = true;
        }
    }

    freeExpressionPredicateGroups(alloc, groups.items);
    groups.deinit(alloc);
    groups.* = combined;
    combined_transferred = true;
}

pub fn appendExpressionBetweenSymmetricGroups(
    alloc: std.mem.Allocator,
    expression_or_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    lhs: db_mod.types.RelationalRowsExpression,
    lower_expression: db_mod.types.RelationalRowsExpression,
    upper_expression: db_mod.types.RelationalRowsExpression,
    negated: bool,
) !void {
    if (!negated) {
        try appendExpressionBetweenGroup(alloc, expression_or_predicates, lhs, .gte, lower_expression, .lte, upper_expression);
        try appendExpressionBetweenGroup(alloc, expression_or_predicates, lhs, .gte, upper_expression, .lte, lower_expression);
    } else {
        try appendExpressionBetweenGroup(alloc, expression_or_predicates, lhs, .lt, lower_expression, .lt, upper_expression);
        try appendExpressionBetweenGroup(alloc, expression_or_predicates, lhs, .gt, lower_expression, .gt, upper_expression);
    }
}

pub fn appendExpressionBetweenGroup(
    alloc: std.mem.Allocator,
    expression_or_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    lhs: db_mod.types.RelationalRowsExpression,
    first_op: runtime_schema.RelationalCheckOp,
    first_rhs: db_mod.types.RelationalRowsExpression,
    second_op: runtime_schema.RelationalCheckOp,
    second_rhs: db_mod.types.RelationalRowsExpression,
) !void {
    const conditions = try alloc.alloc(db_mod.types.RelationalRowsExpressionCondition, 2);
    var conditions_transferred = false;
    var first_initialized = false;
    var second_initialized = false;
    errdefer {
        if (!conditions_transferred) {
            if (first_initialized) freeExpressionCondition(alloc, conditions[0]);
            if (second_initialized) freeExpressionCondition(alloc, conditions[1]);
            alloc.free(conditions);
        }
    }

    conditions[0] = try expressionConditionCloneAlloc(alloc, lhs, first_op, first_rhs);
    first_initialized = true;
    conditions[1] = try expressionConditionCloneAlloc(alloc, lhs, second_op, second_rhs);
    second_initialized = true;

    try expression_or_predicates.append(alloc, .{ .conditions = conditions });
    conditions_transferred = true;
}

pub fn expressionConditionCloneAlloc(
    alloc: std.mem.Allocator,
    lhs: db_mod.types.RelationalRowsExpression,
    op: runtime_schema.RelationalCheckOp,
    rhs: db_mod.types.RelationalRowsExpression,
) !db_mod.types.RelationalRowsExpressionCondition {
    const lhs_clone = try cloneExpressionAlloc(alloc, lhs);
    var lhs_transferred = false;
    errdefer if (!lhs_transferred) freeExpression(alloc, lhs_clone);

    const rhs_slice = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    var rhs_slice_transferred = false;
    var rhs_initialized = false;
    errdefer {
        if (!rhs_slice_transferred) {
            if (rhs_initialized) freeExpression(alloc, rhs_slice[0]);
            alloc.free(rhs_slice);
        }
    }
    rhs_slice[0] = try cloneExpressionAlloc(alloc, rhs);
    rhs_initialized = true;

    lhs_transferred = true;
    rhs_slice_transferred = true;
    return .{
        .lhs = lhs_clone,
        .op = op,
        .rhs = rhs_slice,
    };
}

pub fn appendBooleanConstantExpressionCondition(
    alloc: std.mem.Allocator,
    expression_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionCondition),
    enabled: bool,
) !void {
    const condition = try booleanConstantExpressionConditionAlloc(alloc, enabled);
    var condition_transferred = false;
    errdefer if (!condition_transferred) freeExpressionCondition(alloc, condition);
    try expression_predicates.append(alloc, condition);
    condition_transferred = true;
}

pub fn appendBooleanConstantExpressionGroup(
    alloc: std.mem.Allocator,
    expression_groups: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    enabled: bool,
) !void {
    if (enabled) {
        try expression_groups.append(alloc, .{ .conditions = &.{} });
        return;
    }
    const conditions = try alloc.alloc(db_mod.types.RelationalRowsExpressionCondition, 1);
    var conditions_transferred = false;
    errdefer if (!conditions_transferred) alloc.free(conditions);
    conditions[0] = try booleanConstantExpressionConditionAlloc(alloc, false);
    var condition_transferred = false;
    errdefer if (!condition_transferred) freeExpressionCondition(alloc, conditions[0]);
    try expression_groups.append(alloc, .{ .conditions = conditions });
    condition_transferred = true;
    conditions_transferred = true;
}

pub fn booleanConstantExpressionConditionAlloc(
    alloc: std.mem.Allocator,
    enabled: bool,
) !db_mod.types.RelationalRowsExpressionCondition {
    const lhs_json = try alloc.dupe(u8, "true");
    var lhs_transferred = false;
    errdefer if (!lhs_transferred) alloc.free(lhs_json);
    const rhs_json = try alloc.dupe(u8, if (enabled) "true" else "false");
    var rhs_json_transferred = false;
    errdefer if (!rhs_json_transferred) alloc.free(rhs_json);
    const rhs = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    var rhs_transferred = false;
    errdefer if (!rhs_transferred) alloc.free(rhs);
    rhs[0] = .{
        .kind = .value,
        .value_json = rhs_json,
    };
    lhs_transferred = true;
    rhs_json_transferred = true;
    rhs_transferred = true;
    return .{
        .lhs = .{
            .kind = .value,
            .value_json = lhs_json,
        },
        .op = .eq,
        .rhs = rhs,
    };
}

pub fn expressionBooleanComparisonConditionAlloc(
    alloc: std.mem.Allocator,
    lhs: db_mod.types.RelationalRowsExpression,
    op: runtime_schema.RelationalCheckOp,
    value: bool,
) !db_mod.types.RelationalRowsExpressionCondition {
    const rhs_json = try alloc.dupe(u8, if (value) "true" else "false");
    var rhs_json_transferred = false;
    errdefer if (!rhs_json_transferred) alloc.free(rhs_json);
    const rhs = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    var rhs_transferred = false;
    errdefer if (!rhs_transferred) alloc.free(rhs);
    rhs[0] = .{
        .kind = .value,
        .value_json = rhs_json,
    };
    rhs_json_transferred = true;
    rhs_transferred = true;
    return .{
        .lhs = lhs,
        .op = op,
        .rhs = rhs,
    };
}

pub fn expressionNullTestCondition(
    lhs: db_mod.types.RelationalRowsExpression,
    op: runtime_schema.RelationalCheckOp,
) db_mod.types.RelationalRowsExpressionCondition {
    return .{
        .lhs = lhs,
        .op = op,
        .rhs = &.{},
    };
}

pub fn expressionFieldBooleanConditionAlloc(
    alloc: std.mem.Allocator,
    field: []const u8,
    op: runtime_schema.RelationalCheckOp,
    value: bool,
) !db_mod.types.RelationalRowsExpressionCondition {
    const lhs_field = try alloc.dupe(u8, field);
    var lhs_transferred = false;
    errdefer if (!lhs_transferred) alloc.free(lhs_field);
    const lhs: db_mod.types.RelationalRowsExpression = .{
        .kind = .field,
        .field = lhs_field,
    };
    const condition = try expressionBooleanComparisonConditionAlloc(alloc, lhs, op, value);
    lhs_transferred = true;
    return condition;
}

pub fn expressionFieldNullConditionAlloc(
    alloc: std.mem.Allocator,
    field: []const u8,
    op: runtime_schema.RelationalCheckOp,
) !db_mod.types.RelationalRowsExpressionCondition {
    const lhs_field = try alloc.dupe(u8, field);
    var lhs_transferred = false;
    errdefer if (!lhs_transferred) alloc.free(lhs_field);
    lhs_transferred = true;
    return expressionNullTestCondition(.{
        .kind = .field,
        .field = lhs_field,
    }, op);
}

pub fn appendBooleanIsNotExpressionGroups(
    alloc: std.mem.Allocator,
    expression_groups: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    field: []const u8,
    value: bool,
) !void {
    const ne_condition = try expressionFieldBooleanConditionAlloc(alloc, field, .ne, value);
    try appendExpressionConditionGroup(alloc, expression_groups, ne_condition);
    const null_condition = try expressionFieldNullConditionAlloc(alloc, field, .is_null);
    try appendExpressionConditionGroup(alloc, expression_groups, null_condition);
}

pub fn appendExpressionBooleanIsNotGroups(
    alloc: std.mem.Allocator,
    expression_groups: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    lhs: db_mod.types.RelationalRowsExpression,
    value: bool,
) !void {
    const ne_lhs = try cloneExpressionAlloc(alloc, lhs);
    var ne_lhs_transferred = false;
    errdefer if (!ne_lhs_transferred) freeExpression(alloc, ne_lhs);
    const null_lhs = try cloneExpressionAlloc(alloc, lhs);
    var null_lhs_transferred = false;
    errdefer if (!null_lhs_transferred) freeExpression(alloc, null_lhs);

    const ne_conditions = try alloc.alloc(db_mod.types.RelationalRowsExpressionCondition, 1);
    var ne_conditions_transferred = false;
    errdefer if (!ne_conditions_transferred) alloc.free(ne_conditions);
    ne_conditions[0] = try expressionBooleanComparisonConditionAlloc(alloc, ne_lhs, .ne, value);
    var ne_condition_transferred = false;
    errdefer if (!ne_condition_transferred) freeExpressionCondition(alloc, ne_conditions[0]);
    ne_lhs_transferred = true;

    const null_conditions = try alloc.alloc(db_mod.types.RelationalRowsExpressionCondition, 1);
    var null_conditions_transferred = false;
    errdefer if (!null_conditions_transferred) alloc.free(null_conditions);
    null_conditions[0] = expressionNullTestCondition(null_lhs, .is_null);
    var null_condition_transferred = false;
    errdefer if (!null_condition_transferred) freeExpressionCondition(alloc, null_conditions[0]);
    null_lhs_transferred = true;

    try expression_groups.append(alloc, .{ .conditions = ne_conditions });
    ne_condition_transferred = true;
    ne_conditions_transferred = true;
    try expression_groups.append(alloc, .{ .conditions = null_conditions });
    null_condition_transferred = true;
    null_conditions_transferred = true;
}

fn testOwnedBooleanConditionAlloc(
    alloc: std.mem.Allocator,
    lhs_value: bool,
    rhs_value: bool,
) !db_mod.types.RelationalRowsExpressionCondition {
    const lhs = try booleanLiteralExpressionAlloc(alloc, lhs_value);
    var lhs_transferred = false;
    errdefer if (!lhs_transferred) freeExpression(alloc, lhs);
    const rhs = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    var rhs_transferred = false;
    errdefer if (!rhs_transferred) alloc.free(rhs);
    rhs[0] = try booleanLiteralExpressionAlloc(alloc, rhs_value);
    var rhs_expression_transferred = false;
    errdefer if (!rhs_expression_transferred) freeExpression(alloc, rhs[0]);
    lhs_transferred = true;
    rhs_expression_transferred = true;
    rhs_transferred = true;
    return .{ .lhs = lhs, .op = .eq, .rhs = rhs };
}

test "sql expr_condition builds boolean expression conditions" {
    const alloc = std.testing.allocator;

    var conditions = try alloc.alloc(db_mod.types.RelationalRowsExpressionCondition, 1);
    conditions[0] = try booleanConstantExpressionConditionAlloc(alloc, true);
    var groups = [_]db_mod.types.RelationalRowsExpressionPredicateGroup{.{ .conditions = conditions }};
    const expression = try booleanExpressionFromPredicateGroupsAlloc(alloc, &groups);
    defer freeExpression(alloc, expression);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.case, expression.kind);

    var expression_conditions = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionCondition).empty;
    defer {
        plan_mod.freeExpressionConditions(alloc, expression_conditions.items);
        expression_conditions.deinit(alloc);
    }
    try appendBooleanConstantExpressionCondition(alloc, &expression_conditions, false);
    try std.testing.expectEqual(@as(usize, 1), expression_conditions.items.len);

    var expression_groups = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup).empty;
    defer {
        plan_mod.freeExpressionPredicateGroups(alloc, expression_groups.items);
        expression_groups.deinit(alloc);
    }
    try appendBooleanConstantExpressionGroup(alloc, &expression_groups, true);
    try appendBooleanConstantExpressionGroup(alloc, &expression_groups, false);
    const is_not_lhs = try booleanLiteralExpressionAlloc(alloc, true);
    defer freeExpression(alloc, is_not_lhs);
    try appendExpressionBooleanIsNotGroups(alloc, &expression_groups, is_not_lhs, true);
    try appendBooleanIsNotExpressionGroups(alloc, &expression_groups, "enabled", false);
    try std.testing.expectEqual(@as(usize, 6), expression_groups.items.len);
    try std.testing.expectEqual(@as(usize, 0), expression_groups.items[0].conditions.len);
    try std.testing.expectEqual(@as(usize, 1), expression_groups.items[1].conditions.len);
    try std.testing.expectEqual(@as(usize, 1), expression_groups.items[2].conditions.len);
    try std.testing.expectEqual(@as(usize, 1), expression_groups.items[3].conditions.len);
    try std.testing.expectEqual(@as(usize, 1), expression_groups.items[4].conditions.len);
    try std.testing.expectEqual(@as(usize, 1), expression_groups.items[5].conditions.len);
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.ne, expression_groups.items[4].conditions[0].op);
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.is_null, expression_groups.items[5].conditions[0].op);
    try std.testing.expectEqualStrings("enabled", expression_groups.items[4].conditions[0].lhs.field);
    try std.testing.expectEqualStrings("enabled", expression_groups.items[5].conditions[0].lhs.field);

    var appended_groups = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup).empty;
    defer {
        plan_mod.freeExpressionPredicateGroups(alloc, appended_groups.items);
        appended_groups.deinit(alloc);
    }
    const appended_condition = try testOwnedBooleanConditionAlloc(alloc, true, true);
    try appendExpressionConditionGroup(alloc, &appended_groups, appended_condition);

    const lhs = try booleanLiteralExpressionAlloc(alloc, true);
    defer freeExpression(alloc, lhs);
    const lower = try booleanLiteralExpressionAlloc(alloc, false);
    defer freeExpression(alloc, lower);
    const upper = try booleanLiteralExpressionAlloc(alloc, true);
    defer freeExpression(alloc, upper);
    try appendExpressionBetweenSymmetricGroups(alloc, &appended_groups, lhs, lower, upper, false);
    try std.testing.expectEqual(@as(usize, 3), appended_groups.items.len);
    try std.testing.expectEqual(@as(usize, 1), appended_groups.items[0].conditions.len);
    try std.testing.expectEqual(@as(usize, 2), appended_groups.items[1].conditions.len);
    try std.testing.expectEqual(@as(usize, 2), appended_groups.items[2].conditions.len);

    var left_conditions = try alloc.alloc(db_mod.types.RelationalRowsExpressionCondition, 2);
    left_conditions[0] = try testOwnedBooleanConditionAlloc(alloc, true, true);
    left_conditions[1] = try testOwnedBooleanConditionAlloc(alloc, true, false);

    var right_conditions = try alloc.alloc(db_mod.types.RelationalRowsExpressionCondition, 1);
    right_conditions[0] = try testOwnedBooleanConditionAlloc(alloc, false, false);

    var predicate_groups = [_]db_mod.types.RelationalRowsExpressionPredicateGroup{
        .{ .conditions = left_conditions },
        .{ .conditions = right_conditions },
    };
    const grouped_expression = try booleanExpressionFromPredicateGroupsAlloc(alloc, &predicate_groups);
    defer freeExpression(alloc, grouped_expression);

    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.bool_or, grouped_expression.kind);
    try std.testing.expectEqual(@as(usize, 2), grouped_expression.operands.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.bool_and, grouped_expression.operands[0].kind);
    try std.testing.expectEqual(@as(usize, 2), grouped_expression.operands[0].operands.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.case, grouped_expression.operands[1].kind);
    try std.testing.expectEqual(@as(usize, 0), predicate_groups[0].conditions.len);
    try std.testing.expectEqual(@as(usize, 0), predicate_groups[1].conditions.len);
}
