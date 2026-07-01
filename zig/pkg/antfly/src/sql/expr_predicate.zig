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

const ast = @import("ast.zig");
const db_mod = @import("../storage/db/mod.zig");
const expr_limits = @import("expr_limits.zig");
const expr_type = @import("expr_type.zig");
const plan_mod = @import("plan.zig");
const runtime_schema = @import("../storage/schema.zig");
const token_mod = @import("token.zig");
const value_mod = @import("value.zig");

const Token = token_mod.Token;
const cloneExpressionAlloc = plan_mod.cloneExpressionAlloc;
const freeExpression = plan_mod.freeExpression;
const freeExpressionCondition = plan_mod.freeExpressionCondition;
const max_scalar_or_expanded_branches = expr_limits.max_scalar_or_expanded_branches;

pub fn appendExpressionValuesJsonConjunction(
    alloc: std.mem.Allocator,
    type_context: expr_type.RowExpressionTypeContext,
    expression_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionCondition),
    lhs: db_mod.types.RelationalRowsExpression,
    values_json: []const u8,
) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, values_json, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    if (parsed.value != .array or parsed.value.array.items.len == 0 or parsed.value.array.items.len > max_scalar_or_expanded_branches) return error.UnsupportedSqlShape;
    try expr_type.validateExpressionScalarMembershipValues(type_context, lhs, parsed.value);

    for (parsed.value.array.items) |value| {
        const lhs_clone = try cloneExpressionAlloc(alloc, lhs);
        var lhs_transferred = false;
        errdefer if (!lhs_transferred) freeExpression(alloc, lhs_clone);

        const value_json = try std.json.Stringify.valueAlloc(alloc, value, .{});
        var value_transferred = false;
        errdefer if (!value_transferred) alloc.free(value_json);

        const rhs = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
        var rhs_transferred = false;
        errdefer if (!rhs_transferred) alloc.free(rhs);
        rhs[0] = .{
            .kind = .value,
            .value_json = value_json,
        };
        value_transferred = true;

        try expression_predicates.append(alloc, .{
            .lhs = lhs_clone,
            .op = .eq,
            .rhs = rhs,
        });
        lhs_transferred = true;
        rhs_transferred = true;
    }
}

pub fn appendExpressionValuesJsonOrGroups(
    alloc: std.mem.Allocator,
    type_context: expr_type.RowExpressionTypeContext,
    expression_or_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    lhs: db_mod.types.RelationalRowsExpression,
    values_json: []const u8,
) !void {
    try appendExpressionValuesJsonComparisonGroups(alloc, type_context, expression_or_predicates, lhs, values_json, .eq);
}

pub fn appendExpressionValuesJsonConjunctionGroup(
    alloc: std.mem.Allocator,
    type_context: expr_type.RowExpressionTypeContext,
    expression_or_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    lhs: db_mod.types.RelationalRowsExpression,
    values_json: []const u8,
    op: runtime_schema.RelationalCheckOp,
) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, values_json, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    if (parsed.value != .array or parsed.value.array.items.len == 0 or parsed.value.array.items.len > max_scalar_or_expanded_branches) return error.UnsupportedSqlShape;
    try expr_type.validateExpressionScalarMembershipValues(type_context, lhs, parsed.value);

    const conditions = try alloc.alloc(db_mod.types.RelationalRowsExpressionCondition, parsed.value.array.items.len);
    var condition_count: usize = 0;
    errdefer {
        for (conditions[0..condition_count]) |condition| freeExpressionCondition(alloc, condition);
        alloc.free(conditions);
    }

    for (parsed.value.array.items) |value| {
        if (op == .ne and value == .null) return error.UnsupportedSqlShape;

        const lhs_clone = try cloneExpressionAlloc(alloc, lhs);
        var lhs_transferred = false;
        errdefer if (!lhs_transferred) freeExpression(alloc, lhs_clone);

        const value_json = try std.json.Stringify.valueAlloc(alloc, value, .{});
        var value_transferred = false;
        errdefer if (!value_transferred) alloc.free(value_json);

        const rhs = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
        var rhs_transferred = false;
        errdefer if (!rhs_transferred) alloc.free(rhs);
        rhs[0] = .{
            .kind = .value,
            .value_json = value_json,
        };
        value_transferred = true;

        conditions[condition_count] = .{
            .lhs = lhs_clone,
            .op = op,
            .rhs = rhs,
        };
        condition_count += 1;
        lhs_transferred = true;
        rhs_transferred = true;
    }

    try expression_or_predicates.append(alloc, .{ .conditions = conditions });
}

pub fn appendExpressionValuesJsonComparisonGroups(
    alloc: std.mem.Allocator,
    type_context: expr_type.RowExpressionTypeContext,
    expression_or_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    lhs: db_mod.types.RelationalRowsExpression,
    values_json: []const u8,
    op: runtime_schema.RelationalCheckOp,
) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, values_json, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    if (parsed.value != .array or parsed.value.array.items.len == 0 or parsed.value.array.items.len > max_scalar_or_expanded_branches) return error.UnsupportedSqlShape;
    try expr_type.validateExpressionScalarMembershipValues(type_context, lhs, parsed.value);

    for (parsed.value.array.items) |value| {
        const lhs_clone = try cloneExpressionAlloc(alloc, lhs);
        var lhs_transferred = false;
        errdefer if (!lhs_transferred) freeExpression(alloc, lhs_clone);

        const value_json = try std.json.Stringify.valueAlloc(alloc, value, .{});
        var value_transferred = false;
        errdefer if (!value_transferred) alloc.free(value_json);

        const rhs = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
        var rhs_transferred = false;
        errdefer if (!rhs_transferred) alloc.free(rhs);
        rhs[0] = .{
            .kind = .value,
            .value_json = value_json,
        };
        value_transferred = true;

        const condition: db_mod.types.RelationalRowsExpressionCondition = .{
            .lhs = lhs_clone,
            .op = op,
            .rhs = rhs,
        };
        var condition_transferred = false;
        errdefer if (!condition_transferred) freeExpressionCondition(alloc, condition);
        lhs_transferred = true;
        rhs_transferred = true;

        const conditions = try alloc.alloc(db_mod.types.RelationalRowsExpressionCondition, 1);
        var conditions_transferred = false;
        errdefer if (!conditions_transferred) alloc.free(conditions);
        conditions[0] = condition;
        condition_transferred = true;

        try expression_or_predicates.append(alloc, .{ .conditions = conditions });
        conditions_transferred = true;
    }
}

pub fn wrapBooleanNotExpressionAlloc(
    alloc: std.mem.Allocator,
    expression: db_mod.types.RelationalRowsExpression,
) !db_mod.types.RelationalRowsExpression {
    const operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    var operands_transferred = false;
    errdefer if (!operands_transferred) alloc.free(operands);
    operands[0] = expression;
    operands_transferred = true;
    return .{
        .kind = .bool_not,
        .operands = operands,
    };
}

pub fn expressionLikePatternExpressionAlloc(
    alloc: std.mem.Allocator,
    expression: db_mod.types.RelationalRowsExpression,
    pattern: []const u8,
    case_insensitive: bool,
) !db_mod.types.RelationalRowsExpression {
    const source_expression = try cloneExpressionAlloc(alloc, expression);
    var source_expression_transferred = false;
    errdefer if (!source_expression_transferred) freeExpression(alloc, source_expression);
    const pattern_json = try std.json.Stringify.valueAlloc(alloc, std.json.Value{ .string = pattern }, .{});
    var pattern_json_transferred = false;
    errdefer if (!pattern_json_transferred) alloc.free(pattern_json);
    const operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, 2);
    var operands_transferred = false;
    errdefer if (!operands_transferred) alloc.free(operands);
    operands[0] = source_expression;
    operands[1] = .{
        .kind = .value,
        .value_json = pattern_json,
    };
    source_expression_transferred = true;
    pattern_json_transferred = true;
    operands_transferred = true;
    return .{
        .kind = if (case_insensitive) .ilike else .like,
        .operands = operands,
    };
}

pub fn expressionLikeConditionAlloc(
    alloc: std.mem.Allocator,
    type_context: expr_type.RowExpressionTypeContext,
    expression: db_mod.types.RelationalRowsExpression,
    normalized_pattern: []const u8,
    case_insensitive: bool,
    negated: bool,
) !db_mod.types.RelationalRowsExpressionCondition {
    try type_context.validateTextRowExpression(expression);
    const lhs = try expressionLikePatternExpressionAlloc(alloc, expression, normalized_pattern, case_insensitive);
    var lhs_transferred = false;
    errdefer if (!lhs_transferred) freeExpression(alloc, lhs);

    const rhs_json = try alloc.dupe(u8, value_mod.booleanJson(!negated));
    var rhs_json_transferred = false;
    errdefer if (!rhs_json_transferred) alloc.free(rhs_json);
    const rhs = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    var rhs_transferred = false;
    errdefer if (!rhs_transferred) {
        if (rhs_json_transferred) freeExpression(alloc, rhs[0]);
        alloc.free(rhs);
    };
    rhs[0] = .{
        .kind = .value,
        .value_json = rhs_json,
    };
    rhs_json_transferred = true;
    lhs_transferred = true;
    rhs_transferred = true;

    return .{
        .lhs = lhs,
        .op = .eq,
        .rhs = rhs,
    };
}

pub fn expressionRegexpMatchConditionAlloc(
    alloc: std.mem.Allocator,
    type_context: expr_type.RowExpressionTypeContext,
    expression: db_mod.types.RelationalRowsExpression,
    pattern_expression: db_mod.types.RelationalRowsExpression,
    case_insensitive: bool,
    negated: bool,
) !db_mod.types.RelationalRowsExpressionCondition {
    try type_context.validateTextRowExpression(expression);
    try type_context.validateTextRowExpression(pattern_expression);

    const source_expression = try cloneExpressionAlloc(alloc, expression);
    var source_expression_transferred = false;
    errdefer if (!source_expression_transferred) freeExpression(alloc, source_expression);

    const pattern_expression_clone = try cloneExpressionAlloc(alloc, pattern_expression);
    var pattern_expression_transferred = false;
    errdefer if (!pattern_expression_transferred) freeExpression(alloc, pattern_expression_clone);

    const case_json = try alloc.dupe(u8, value_mod.booleanJson(case_insensitive));
    var case_json_transferred = false;
    errdefer if (!case_json_transferred) alloc.free(case_json);
    const operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, 3);
    var operands_transferred = false;
    errdefer if (!operands_transferred) {
        freeExpression(alloc, operands[0]);
        freeExpression(alloc, operands[1]);
        freeExpression(alloc, operands[2]);
        alloc.free(operands);
    };
    operands[0] = source_expression;
    operands[1] = pattern_expression_clone;
    operands[2] = .{
        .kind = .value,
        .value_json = case_json,
    };
    source_expression_transferred = true;
    pattern_expression_transferred = true;
    case_json_transferred = true;

    const rhs_json = try alloc.dupe(u8, value_mod.booleanJson(!negated));
    var rhs_json_transferred = false;
    errdefer if (!rhs_json_transferred) alloc.free(rhs_json);
    const rhs = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    var rhs_transferred = false;
    errdefer if (!rhs_transferred) {
        if (rhs_json_transferred) freeExpression(alloc, rhs[0]);
        alloc.free(rhs);
    };
    rhs[0] = .{
        .kind = .value,
        .value_json = rhs_json,
    };
    rhs_json_transferred = true;
    rhs_transferred = true;
    operands_transferred = true;

    return .{
        .lhs = .{
            .kind = .regexp_match,
            .operands = operands,
        },
        .op = .eq,
        .rhs = rhs,
    };
}

pub fn expressionLikeSetExpressionAlloc(
    alloc: std.mem.Allocator,
    expression: db_mod.types.RelationalRowsExpression,
    values_json: []const u8,
    case_insensitive: bool,
    negated: bool,
    quantifier: ast.SqlPatternQuantifier,
) !db_mod.types.RelationalRowsExpression {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, values_json, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    if (parsed.value != .array or parsed.value.array.items.len == 0 or parsed.value.array.items.len > max_scalar_or_expanded_branches) return error.UnsupportedSqlShape;

    var operands = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpression).empty;
    var operands_transferred = false;
    errdefer {
        if (!operands_transferred) {
            for (operands.items) |operand| freeExpression(alloc, operand);
            operands.deinit(alloc);
        }
    }

    for (parsed.value.array.items) |value| {
        if (value != .string) return error.UnsupportedSqlShape;
        var operand = try expressionLikePatternExpressionAlloc(alloc, expression, value.string, case_insensitive);
        var operand_transferred = false;
        errdefer if (!operand_transferred) freeExpression(alloc, operand);
        if (negated) {
            operand = try wrapBooleanNotExpressionAlloc(alloc, operand);
        }
        try operands.append(alloc, operand);
        operand_transferred = true;
    }

    if (operands.items.len == 1) {
        const out = operands.items[0];
        operands.clearRetainingCapacity();
        operands.deinit(alloc);
        operands_transferred = true;
        return out;
    }

    const owned_operands = try operands.toOwnedSlice(alloc);
    operands = .empty;
    operands_transferred = true;
    return .{
        .kind = if (quantifier == .all) .bool_and else .bool_or,
        .operands = owned_operands,
    };
}

pub fn expressionLikeSetConditionAlloc(
    alloc: std.mem.Allocator,
    type_context: expr_type.RowExpressionTypeContext,
    expression: db_mod.types.RelationalRowsExpression,
    values_json: []const u8,
    case_insensitive: bool,
    negated: bool,
    quantifier: ast.SqlPatternQuantifier,
) !db_mod.types.RelationalRowsExpressionCondition {
    try type_context.validateTextRowExpression(expression);
    const lhs = try expressionLikeSetExpressionAlloc(alloc, expression, values_json, case_insensitive, negated, quantifier);
    var lhs_transferred = false;
    errdefer if (!lhs_transferred) freeExpression(alloc, lhs);
    const rhs_json = try alloc.dupe(u8, "true");
    var rhs_json_transferred = false;
    errdefer if (!rhs_json_transferred) alloc.free(rhs_json);
    const rhs = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    var rhs_transferred = false;
    errdefer if (!rhs_transferred) {
        if (rhs_json_transferred) freeExpression(alloc, rhs[0]);
        alloc.free(rhs);
    };
    rhs[0] = .{
        .kind = .value,
        .value_json = rhs_json,
    };
    rhs_json_transferred = true;
    lhs_transferred = true;
    rhs_transferred = true;
    return .{
        .lhs = lhs,
        .op = .eq,
        .rhs = rhs,
    };
}

pub fn appendExpressionInPredicateGroupsAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    type_context: expr_type.RowExpressionTypeContext,
    expression_groups: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    lhs: db_mod.types.RelationalRowsExpression,
) !void {
    const values_json = try value_mod.parseSqlInValuesJsonAlloc(alloc, tokens, pos, params);
    defer alloc.free(values_json);
    try appendExpressionValuesJsonOrGroups(alloc, type_context, expression_groups, lhs, values_json);
}

pub fn appendTextPatternPredicateAlloc(
    alloc: std.mem.Allocator,
    text_patterns: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsTextPatternPredicate),
    field: []const u8,
    column: runtime_schema.RelationalColumn,
    pattern: []const u8,
    case_insensitive: bool,
    negated: bool,
) !void {
    if (column.field_type != .keyword and column.field_type != .text and column.field_type != .link) return error.InvalidSqlCatalog;
    const predicate_field = try alloc.dupe(u8, field);
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(predicate_field);
    const predicate_pattern = try alloc.dupe(u8, pattern);
    var pattern_transferred = false;
    errdefer if (!pattern_transferred) alloc.free(predicate_pattern);
    try text_patterns.append(alloc, .{
        .field = predicate_field,
        .pattern = predicate_pattern,
        .case_insensitive = case_insensitive,
        .negated = negated,
    });
    field_transferred = true;
    pattern_transferred = true;
}

fn testTypeContext(alloc: std.mem.Allocator) expr_type.RowExpressionTypeContext {
    const columns = struct {
        const values = [_]runtime_schema.RelationalColumn{
            .{ .name = "body", .path = "body", .field_type = .text },
            .{ .name = "status", .path = "status", .field_type = .keyword },
        };
    }.values;
    return .{
        .alloc = alloc,
        .schema = .{ .storage_mode = .relational, .relational_columns = &columns },
    };
}

test "sql expr_predicate builds like predicate expressions" {
    const alloc = std.testing.allocator;
    const type_context = testTypeContext(alloc);
    const body_expression: db_mod.types.RelationalRowsExpression = .{ .kind = .field, .field = "body" };

    const like_condition = try expressionLikeConditionAlloc(alloc, type_context, body_expression, "ant%", false, false);
    defer freeExpressionCondition(alloc, like_condition);
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.eq, like_condition.op);
    try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionKind.like, like_condition.lhs.kind);
    try std.testing.expectEqual(@as(usize, 2), like_condition.lhs.operands.len);
    try std.testing.expectEqualStrings("body", like_condition.lhs.operands[0].field);
    try std.testing.expectEqualStrings("\"ant%\"", like_condition.lhs.operands[1].value_json);
    try std.testing.expectEqualStrings("true", like_condition.rhs[0].value_json);

    const like_any_condition = try expressionLikeSetConditionAlloc(alloc, type_context, body_expression, "[\"ant%\",\"bee%\"]", true, false, .any);
    defer freeExpressionCondition(alloc, like_any_condition);
    try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionKind.bool_or, like_any_condition.lhs.kind);
    try std.testing.expectEqual(@as(usize, 2), like_any_condition.lhs.operands.len);
    try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionKind.ilike, like_any_condition.lhs.operands[0].kind);
    try std.testing.expectEqualStrings("true", like_any_condition.rhs[0].value_json);
}

test "sql expr_predicate appends scalar and text predicates" {
    const alloc = std.testing.allocator;
    const type_context = testTypeContext(alloc);

    var groups = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup).empty;
    defer {
        plan_mod.freeExpressionPredicateGroups(alloc, groups.items);
        groups.deinit(alloc);
    }
    try appendExpressionValuesJsonOrGroups(
        alloc,
        type_context,
        &groups,
        .{ .kind = .field, .field = "status" },
        "[\"active\",\"paused\"]",
    );
    try std.testing.expectEqual(@as(usize, 2), groups.items.len);
    try std.testing.expectEqual(@as(usize, 1), groups.items[0].conditions.len);
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.eq, groups.items[0].conditions[0].op);
    try std.testing.expectEqualStrings("status", groups.items[0].conditions[0].lhs.field);
    try std.testing.expectEqualStrings("\"active\"", groups.items[0].conditions[0].rhs[0].value_json);

    var text_patterns = std.ArrayListUnmanaged(db_mod.types.RelationalRowsTextPatternPredicate).empty;
    defer {
        plan_mod.freeTextPatterns(alloc, text_patterns.items);
        text_patterns.deinit(alloc);
    }
    try appendTextPatternPredicateAlloc(
        alloc,
        &text_patterns,
        "body",
        .{ .name = "body", .path = "body", .field_type = .text },
        "ant%",
        true,
        false,
    );
    try std.testing.expectEqual(@as(usize, 1), text_patterns.items.len);
    try std.testing.expectEqualStrings("body", text_patterns.items[0].field);
    try std.testing.expectEqualStrings("ant%", text_patterns.items[0].pattern);
    try std.testing.expect(text_patterns.items[0].case_insensitive);
    try std.testing.expect(!text_patterns.items[0].negated);
}
