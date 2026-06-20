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
const binder = @import("binder.zig");
const db_mod = @import("../../storage/db/mod.zig");
const ddl_plan = @import("ddl_plan.zig");
const parser = @import("parser.zig");
const runtime_schema = @import("../../storage/schema.zig");
const token_mod = @import("token.zig");

pub const Token = token_mod.Token;

pub const ExtensionFunctionBinding = struct {
    sql_name: []const u8,
    native_expression_kind: runtime_schema.RelationalRowsExpressionKind,
    arity: u16,
};

pub const RoutineExpressionBinding = struct {
    sql_name: []const u8,
    arity: u16,
    expression: runtime_schema.RelationalRowsExpression,
    null_input: ?ddl_plan.RoutineNullInput = null,
};

pub const SqlFunctionBindings = struct {
    extension_functions: []const ExtensionFunctionBinding = &.{},
    routine_expressions: []const RoutineExpressionBinding = &.{},
};

pub fn sqlKeywordIsAnyOrSome(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "any") or std.ascii.eqlIgnoreCase(text, "some");
}

pub fn sqlKeywordStartsScalarPredicate(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "is") or
        std.ascii.eqlIgnoreCase(text, "isnull") or
        std.ascii.eqlIgnoreCase(text, "in") or
        std.ascii.eqlIgnoreCase(text, "between") or
        std.ascii.eqlIgnoreCase(text, "like") or
        std.ascii.eqlIgnoreCase(text, "ilike") or
        std.ascii.eqlIgnoreCase(text, "notnull") or
        sqlKeywordIsAnyOrSome(text) or
        std.ascii.eqlIgnoreCase(text, "all");
}

pub fn peekExtensionFunctionCall(tokens: []const Token, pos: usize, bindings: []const ExtensionFunctionBinding) bool {
    if (bindings.len == 0) return false;
    if (pos + 1 >= tokens.len or tokens[pos].kind != .identifier or tokens[pos + 1].kind != .lparen) return false;
    for (bindings) |binding| {
        if (std.ascii.eqlIgnoreCase(binding.sql_name, tokens[pos].text)) return true;
    }
    return false;
}

pub fn extensionFunctionBinding(bindings: []const ExtensionFunctionBinding, name: []const u8) !?ExtensionFunctionBinding {
    var found: ?ExtensionFunctionBinding = null;
    for (bindings) |binding| {
        if (!std.ascii.eqlIgnoreCase(binding.sql_name, name)) continue;
        if (found != null) return error.UnsupportedSqlShape;
        found = binding;
    }
    return found;
}

pub fn peekRoutineExpressionCall(tokens: []const Token, pos: usize, bindings: []const RoutineExpressionBinding) bool {
    if (bindings.len == 0) return false;
    if (pos + 1 >= tokens.len or tokens[pos].kind != .identifier or tokens[pos + 1].kind != .lparen) return false;
    for (bindings) |binding| {
        if (std.ascii.eqlIgnoreCase(binding.sql_name, tokens[pos].text)) return true;
    }
    return false;
}

pub fn routineExpressionBinding(
    bindings: []const RoutineExpressionBinding,
    name: []const u8,
    arity: usize,
) !?RoutineExpressionBinding {
    var found: ?RoutineExpressionBinding = null;
    for (bindings) |binding| {
        if (!std.ascii.eqlIgnoreCase(binding.sql_name, name)) continue;
        if (@as(usize, binding.arity) != arity) continue;
        if (found != null) return error.UnsupportedSqlShape;
        found = binding;
    }
    return found;
}

pub fn routineArgumentExpressionIsNullLiteral(value: runtime_schema.RelationalRowsExpression) bool {
    return value.kind == .value and std.mem.eql(u8, value.value_json, "null");
}

fn routineArgIndex(field: []const u8) ?usize {
    if (!std.mem.startsWith(u8, field, "arg") or field.len <= 3) return null;
    var index: usize = 0;
    for (field[3..]) |ch| {
        if (ch < '0' or ch > '9') return null;
        index = index * 10 + @as(usize, ch - '0');
    }
    if (index == 0) return null;
    return index - 1;
}

pub fn cloneExpressionSubstitutingRoutineArgsAlloc(
    alloc: std.mem.Allocator,
    value: runtime_schema.RelationalRowsExpression,
    args: []const runtime_schema.RelationalRowsExpression,
) anyerror!runtime_schema.RelationalRowsExpression {
    if (value.kind == .field) {
        if (routineArgIndex(value.field)) |index| {
            if (index >= args.len) return error.UnsupportedSqlShape;
            return try runtime_schema.cloneRelationalRowsExpressionAlloc(alloc, args[index]);
        }
    }

    var cloned: runtime_schema.RelationalRowsExpression = .{
        .kind = value.kind,
        .field_source = value.field_source,
        .cast_type = value.cast_type,
        .json_as_text = value.json_as_text,
    };
    errdefer runtime_schema.freeRelationalRowsExpression(alloc, cloned);

    if (value.field.len > 0) cloned.field = try alloc.dupe(u8, value.field);
    if (value.value_json.len > 0) cloned.value_json = try alloc.dupe(u8, value.value_json);
    if (value.json_path.len > 0) cloned.json_path = try alloc.dupe(u8, value.json_path);

    if (value.operands.len > 0) {
        const operands = try alloc.alloc(runtime_schema.RelationalRowsExpression, value.operands.len);
        var initialized: usize = 0;
        errdefer {
            for (operands[0..initialized]) |operand| runtime_schema.freeRelationalRowsExpression(alloc, operand);
            alloc.free(operands);
        }
        for (value.operands, 0..) |operand, i| {
            operands[i] = try cloneExpressionSubstitutingRoutineArgsAlloc(alloc, operand, args);
            initialized += 1;
        }
        cloned.operands = operands;
    }

    if (value.case_branches.len > 0) {
        const branches = try alloc.alloc(runtime_schema.RelationalRowsExpressionCaseBranch, value.case_branches.len);
        var initialized: usize = 0;
        errdefer {
            for (branches[0..initialized]) |branch| freeRoutineExpressionCaseBranch(alloc, branch);
            alloc.free(branches);
        }
        for (value.case_branches, 0..) |branch, i| {
            branches[i] = try cloneExpressionCaseBranchSubstitutingRoutineArgsAlloc(alloc, branch, args);
            initialized += 1;
        }
        cloned.case_branches = branches;
    }

    if (value.case_else.len > 0) {
        const fallback = try alloc.alloc(runtime_schema.RelationalRowsExpression, value.case_else.len);
        var initialized: usize = 0;
        errdefer {
            for (fallback[0..initialized]) |expression| runtime_schema.freeRelationalRowsExpression(alloc, expression);
            alloc.free(fallback);
        }
        for (value.case_else, 0..) |expression, i| {
            fallback[i] = try cloneExpressionSubstitutingRoutineArgsAlloc(alloc, expression, args);
            initialized += 1;
        }
        cloned.case_else = fallback;
    }

    return cloned;
}

fn cloneExpressionCaseBranchSubstitutingRoutineArgsAlloc(
    alloc: std.mem.Allocator,
    value: runtime_schema.RelationalRowsExpressionCaseBranch,
    args: []const runtime_schema.RelationalRowsExpression,
) anyerror!runtime_schema.RelationalRowsExpressionCaseBranch {
    const when = try cloneExpressionConditionSubstitutingRoutineArgsAlloc(alloc, value.when, args);
    var when_transferred = false;
    errdefer if (!when_transferred) runtime_schema.freeRelationalRowsExpressionCondition(alloc, when);
    const then_expression = try cloneExpressionSubstitutingRoutineArgsAlloc(alloc, value.then, args);
    when_transferred = true;
    return .{
        .when = when,
        .then = then_expression,
    };
}

fn cloneExpressionConditionSubstitutingRoutineArgsAlloc(
    alloc: std.mem.Allocator,
    value: runtime_schema.RelationalRowsExpressionCondition,
    args: []const runtime_schema.RelationalRowsExpression,
) anyerror!runtime_schema.RelationalRowsExpressionCondition {
    const lhs = try cloneExpressionSubstitutingRoutineArgsAlloc(alloc, value.lhs, args);
    var lhs_transferred = false;
    errdefer if (!lhs_transferred) runtime_schema.freeRelationalRowsExpression(alloc, lhs);

    const rhs = if (value.rhs.len > 0) blk: {
        const out = try alloc.alloc(runtime_schema.RelationalRowsExpression, value.rhs.len);
        var initialized: usize = 0;
        errdefer {
            for (out[0..initialized]) |expression| runtime_schema.freeRelationalRowsExpression(alloc, expression);
            alloc.free(out);
        }
        for (value.rhs, 0..) |expression, i| {
            out[i] = try cloneExpressionSubstitutingRoutineArgsAlloc(alloc, expression, args);
            initialized += 1;
        }
        break :blk out;
    } else &.{};

    lhs_transferred = true;
    return .{
        .lhs = lhs,
        .op = value.op,
        .rhs = rhs,
    };
}

fn freeRoutineExpressionCaseBranch(
    alloc: std.mem.Allocator,
    value: runtime_schema.RelationalRowsExpressionCaseBranch,
) void {
    runtime_schema.freeRelationalRowsExpressionCondition(alloc, value.when);
    runtime_schema.freeRelationalRowsExpression(alloc, value.then);
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
            !optionalStringEqual(lhs_check.value_json, rhs_check.value_json))
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
            lhs_predicate.negated != rhs_predicate.negated)
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

pub fn rowExpressionDeterministic(expression: runtime_schema.RelationalRowsExpression) bool {
    if (expression.field_source != .row) return false;
    if (expression.kind == .now or expression.kind == .uuid_v4) return false;
    for (expression.operands) |operand| {
        if (!rowExpressionDeterministic(operand)) return false;
    }
    for (expression.case_branches) |branch| {
        if (!rowExpressionConditionDeterministic(branch.when)) return false;
        if (!rowExpressionDeterministic(branch.then)) return false;
    }
    for (expression.case_else) |case_else| {
        if (!rowExpressionDeterministic(case_else)) return false;
    }
    return true;
}

pub fn rowExpressionConditionDeterministic(condition: runtime_schema.RelationalRowsExpressionCondition) bool {
    if (!rowExpressionDeterministic(condition.lhs)) return false;
    for (condition.rhs) |rhs| {
        if (!rowExpressionDeterministic(rhs)) return false;
    }
    return true;
}

pub fn validateCheckExpressionConditionForColumns(
    columns: []const runtime_schema.RelationalColumn,
    condition: runtime_schema.RelationalRowsExpressionCondition,
) anyerror!void {
    switch (condition.op) {
        .is_null, .is_not_null => if (condition.rhs.len != 0) return error.InvalidSqlCatalog,
        .eq, .ne, .is_distinct, .is_not_distinct, .gt, .gte, .lt, .lte => if (condition.rhs.len != 1) return error.InvalidSqlCatalog,
    }
    const lhs_type = try checkExpressionTypeForColumns(columns, condition.lhs);
    if (condition.rhs.len == 0) return;
    const rhs_type = try checkExpressionTypeForColumns(columns, condition.rhs[0]);
    if (!checkExpressionTypesComparable(lhs_type, rhs_type)) return error.InvalidSqlCatalog;
    switch (condition.op) {
        .gt, .gte, .lt, .lte => if (!checkExpressionTypeOrderable(lhs_type) or !checkExpressionTypeOrderable(rhs_type)) return error.InvalidSqlCatalog,
        else => {},
    }
}

pub fn validateCheckExpressionForColumns(
    columns: []const runtime_schema.RelationalColumn,
    expression: runtime_schema.RelationalRowsExpression,
) anyerror!void {
    _ = try checkExpressionTypeForColumns(columns, expression);
}

pub const CheckExpressionType = union(enum) {
    type: runtime_schema.AntflyType,
    null,
};

fn checkExpressionContainsInterval(expression: runtime_schema.RelationalRowsExpression) bool {
    if (expression.kind == .interval_ns or expression.kind == .interval_months) return true;
    for (expression.operands) |operand| {
        if (checkExpressionContainsInterval(operand)) return true;
    }
    for (expression.case_branches) |branch| {
        if (checkExpressionContainsInterval(branch.then)) return true;
        if (checkExpressionContainsInterval(branch.when.lhs)) return true;
        for (branch.when.rhs) |rhs| {
            if (checkExpressionContainsInterval(rhs)) return true;
        }
    }
    for (expression.case_else) |case_else| {
        if (checkExpressionContainsInterval(case_else)) return true;
    }
    return false;
}

fn validateDateBinStrideExpressionForColumns(
    columns: []const runtime_schema.RelationalColumn,
    expression: runtime_schema.RelationalRowsExpression,
) anyerror!void {
    switch (expression.kind) {
        .interval_ns => {
            if (expression.operands.len != 1) return error.InvalidSqlCatalog;
            if (checkExpressionContainsInterval(expression.operands[0])) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression.operands[0]), .numeric)) return error.InvalidSqlCatalog;
        },
        .interval_months => return error.InvalidSqlCatalog,
        else => {
            if (checkExpressionContainsInterval(expression)) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression), .numeric)) return error.InvalidSqlCatalog;
        },
    }
}

pub fn checkExpressionTypeForColumns(
    columns: []const runtime_schema.RelationalColumn,
    expression: runtime_schema.RelationalRowsExpression,
) anyerror!CheckExpressionType {
    if (expression.kind == .field) {
        if (expression.field_source != .row) return error.InvalidSqlCatalog;
        const column = binder.relationalColumnForDdl(columns, expression.field) orelse return error.InvalidSqlCatalog;
        return .{ .type = column.field_type };
    }
    if (expression.kind == .value) return checkExpressionLiteralType(expression.value_json);

    for (expression.case_branches) |branch| try validateCheckExpressionConditionForColumns(columns, branch.when);

    switch (expression.kind) {
        .field, .value => unreachable,
        .now => return .{ .type = .datetime },
        .uuid_v4 => return .{ .type = .text },
        .regexp_replace, .regexp_substr => {
            if (expression.kind == .regexp_replace and expression.operands.len != 3 and expression.operands.len != 4) return error.InvalidSqlCatalog;
            if (expression.kind == .regexp_substr and expression.operands.len != 2) return error.InvalidSqlCatalog;
            for (expression.operands) |operand| {
                if (!checkExpressionTypeTextLike(try checkExpressionTypeForColumns(columns, operand))) return error.InvalidSqlCatalog;
            }
            return .{ .type = .text };
        },
        .lower, .upper, .initcap, .trim, .ltrim, .rtrim, .replace, .translate, .substring, .overlay, .split_part, .left, .right, .lpad, .rpad, .repeat, .reverse, .chr, .md5, .concat, .concat_ws => {
            for (expression.operands) |operand| {
                if (!checkExpressionTypeTextLike(try checkExpressionTypeForColumns(columns, operand))) return error.InvalidSqlCatalog;
            }
            return .{ .type = .text };
        },
        .length, .octet_length, .bit_length, .ascii, .strpos => {
            for (expression.operands) |operand| {
                if (!checkExpressionTypeTextLike(try checkExpressionTypeForColumns(columns, operand))) return error.InvalidSqlCatalog;
            }
            return .{ .type = .numeric };
        },
        .regexp_count, .regexp_instr => {
            if (expression.operands.len != 2) return error.InvalidSqlCatalog;
            for (expression.operands) |operand| {
                if (!checkExpressionTypeTextLike(try checkExpressionTypeForColumns(columns, operand))) return error.InvalidSqlCatalog;
            }
            return .{ .type = .numeric };
        },
        .starts_with, .ends_with, .like, .ilike => {
            for (expression.operands) |operand| {
                if (!checkExpressionTypeTextLike(try checkExpressionTypeForColumns(columns, operand))) return error.InvalidSqlCatalog;
            }
            return .{ .type = .boolean };
        },
        .regexp_match => {
            if (expression.operands.len != 2 and expression.operands.len != 3) return error.InvalidSqlCatalog;
            for (expression.operands[0..2]) |operand| {
                if (!checkExpressionTypeTextLike(try checkExpressionTypeForColumns(columns, operand))) return error.InvalidSqlCatalog;
            }
            if (expression.operands.len == 3 and !checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression.operands[2]), .boolean)) return error.InvalidSqlCatalog;
            return .{ .type = .boolean };
        },
        .bool_and, .bool_or, .bool_not => {
            for (expression.operands) |operand| {
                if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, operand), .boolean)) return error.InvalidSqlCatalog;
            }
            return .{ .type = .boolean };
        },
        .abs, .round, .trunc, .floor, .ceil, .sqrt, .sign, .power, .mul, .div, .mod, .interval_ns, .interval_months => {
            for (expression.operands) |operand| {
                if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, operand), .numeric)) return error.InvalidSqlCatalog;
            }
            return .{ .type = .numeric };
        },
        .add, .sub => {
            var saw_datetime = false;
            for (expression.operands) |operand| {
                const operand_type = try checkExpressionTypeForColumns(columns, operand);
                switch (operand_type) {
                    .type => |field_type| {
                        if (field_type == .datetime) saw_datetime = true else if (field_type != .numeric) return error.InvalidSqlCatalog;
                    },
                    .null => return error.InvalidSqlCatalog,
                }
            }
            return .{ .type = if (saw_datetime) .datetime else .numeric };
        },
        .date_trunc => {
            if (expression.operands.len != 2) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeTextLike(try checkExpressionTypeForColumns(columns, expression.operands[0]))) return error.InvalidSqlCatalog;
            const value_type = try checkExpressionTypeForColumns(columns, expression.operands[1]);
            if (!checkExpressionTypeEquals(value_type, .datetime) and !checkExpressionTypeEquals(value_type, .numeric)) return error.InvalidSqlCatalog;
            return .{ .type = .datetime };
        },
        .date_bin => {
            if (expression.operands.len != 3) return error.InvalidSqlCatalog;
            try validateDateBinStrideExpressionForColumns(columns, expression.operands[0]);
            const source_type = try checkExpressionTypeForColumns(columns, expression.operands[1]);
            if (!checkExpressionTypeEquals(source_type, .datetime) and !checkExpressionTypeEquals(source_type, .numeric)) return error.InvalidSqlCatalog;
            const origin_type = try checkExpressionTypeForColumns(columns, expression.operands[2]);
            if (!checkExpressionTypeEquals(origin_type, .datetime) and !checkExpressionTypeEquals(origin_type, .numeric)) return error.InvalidSqlCatalog;
            return .{ .type = .datetime };
        },
        .date_part => {
            if (expression.operands.len != 2) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeTextLike(try checkExpressionTypeForColumns(columns, expression.operands[0]))) return error.InvalidSqlCatalog;
            const value_type = try checkExpressionTypeForColumns(columns, expression.operands[1]);
            if (!checkExpressionTypeEquals(value_type, .datetime) and !checkExpressionTypeEquals(value_type, .numeric)) return error.InvalidSqlCatalog;
            return .{ .type = .numeric };
        },
        .cast => return .{ .type = switch (expression.cast_type orelse return error.InvalidSqlCatalog) {
            .text => .text,
            .numeric => .numeric,
            .bool => .boolean,
            .datetime => .datetime,
        } },
        .json_extract => {
            if (expression.operands.len != 1) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression.operands[0]), .json)) return error.InvalidSqlCatalog;
            return .{ .type = if (expression.json_as_text) .text else .json };
        },
        .json_path_exists => {
            if (expression.operands.len != 1 or expression.json_path.len == 0) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression.operands[0]), .json)) return error.InvalidSqlCatalog;
            return .{ .type = .boolean };
        },
        .json_typeof => {
            if (expression.operands.len != 1) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression.operands[0]), .json)) return error.InvalidSqlCatalog;
            return .{ .type = .text };
        },
        .json_array_length => {
            if (expression.operands.len != 1) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression.operands[0]), .json)) return error.InvalidSqlCatalog;
            return .{ .type = .numeric };
        },
        .json_build_object => {
            if (expression.operands.len % 2 != 0) return error.InvalidSqlCatalog;
            var index: usize = 0;
            while (index < expression.operands.len) : (index += 2) {
                if (!checkExpressionTypeTextLike(try checkExpressionTypeForColumns(columns, expression.operands[index]))) return error.InvalidSqlCatalog;
                _ = try checkExpressionTypeForColumns(columns, expression.operands[index + 1]);
            }
            return .{ .type = .json };
        },
        .to_jsonb => {
            if (expression.operands.len != 1) return error.InvalidSqlCatalog;
            _ = try checkExpressionTypeForColumns(columns, expression.operands[0]);
            return .{ .type = .json };
        },
        .array_length => {
            if (expression.operands.len != 1) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression.operands[0]), .array)) return error.InvalidSqlCatalog;
            return .{ .type = .numeric };
        },
        .array_position => {
            if (expression.operands.len != 2) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression.operands[0]), .array)) return error.InvalidSqlCatalog;
            _ = try checkExpressionTypeForColumns(columns, expression.operands[1]);
            return .{ .type = .numeric };
        },
        .array_positions => {
            if (expression.operands.len != 2) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression.operands[0]), .array)) return error.InvalidSqlCatalog;
            _ = try checkExpressionTypeForColumns(columns, expression.operands[1]);
            return .{ .type = .array };
        },
        .array_append, .array_prepend, .array_remove => {
            if (expression.operands.len != 2) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression.operands[0]), .array)) return error.InvalidSqlCatalog;
            _ = try checkExpressionTypeForColumns(columns, expression.operands[1]);
            return .{ .type = .array };
        },
        .array_replace => {
            if (expression.operands.len != 3) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression.operands[0]), .array)) return error.InvalidSqlCatalog;
            _ = try checkExpressionTypeForColumns(columns, expression.operands[1]);
            _ = try checkExpressionTypeForColumns(columns, expression.operands[2]);
            return .{ .type = .array };
        },
        .array_cat => {
            if (expression.operands.len != 2) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression.operands[0]), .array)) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression.operands[1]), .array)) return error.InvalidSqlCatalog;
            return .{ .type = .array };
        },
        .array_to_string => {
            if (expression.operands.len != 2 and expression.operands.len != 3) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression.operands[0]), .array)) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeTextLike(try checkExpressionTypeForColumns(columns, expression.operands[1]))) return error.InvalidSqlCatalog;
            if (expression.operands.len == 3 and !checkExpressionTypeTextLike(try checkExpressionTypeForColumns(columns, expression.operands[2]))) return error.InvalidSqlCatalog;
            return .{ .type = .text };
        },
        .string_to_array => {
            for (expression.operands) |operand| {
                if (!checkExpressionTypeTextLike(try checkExpressionTypeForColumns(columns, operand))) return error.InvalidSqlCatalog;
            }
            return .{ .type = .array };
        },
        .coalesce, .nullif, .greatest, .least => return try checkExpressionCommonType(columns, expression.operands),
        .case => {
            if (expression.case_else.len != 1) return error.InvalidSqlCatalog;
            var candidate = try checkExpressionTypeForColumns(columns, expression.case_else[0]);
            for (expression.case_branches) |branch| {
                const branch_type = try checkExpressionTypeForColumns(columns, branch.then);
                if (!checkExpressionTypesComparable(candidate, branch_type)) return error.InvalidSqlCatalog;
                if (candidate == .null) candidate = branch_type;
            }
            return candidate;
        },
    }
}

fn checkExpressionCommonType(
    columns: []const runtime_schema.RelationalColumn,
    expressions: []const runtime_schema.RelationalRowsExpression,
) anyerror!CheckExpressionType {
    if (expressions.len == 0) return error.InvalidSqlCatalog;
    var candidate = try checkExpressionTypeForColumns(columns, expressions[0]);
    for (expressions[1..]) |expression| {
        const expression_type = try checkExpressionTypeForColumns(columns, expression);
        if (!checkExpressionTypesComparable(candidate, expression_type)) return error.InvalidSqlCatalog;
        if (candidate == .null) candidate = expression_type;
    }
    return candidate;
}

pub fn checkExpressionLiteralType(value_json: []const u8) !CheckExpressionType {
    if (std.mem.eql(u8, value_json, "null")) return .null;
    if (std.mem.eql(u8, value_json, "true") or std.mem.eql(u8, value_json, "false")) return .{ .type = .boolean };
    if (value_json.len == 0) return error.InvalidSqlCatalog;
    return switch (value_json[0]) {
        '"' => .{ .type = .text },
        '[' => .{ .type = .array },
        '{' => .{ .type = .json },
        '-', '0'...'9' => .{ .type = .numeric },
        else => error.InvalidSqlCatalog,
    };
}

pub fn checkExpressionTypesComparable(lhs: CheckExpressionType, rhs: CheckExpressionType) bool {
    if (lhs == .null or rhs == .null) return true;
    if (checkExpressionTypeTextLike(lhs) and checkExpressionTypeTextLike(rhs)) return true;
    return switch (lhs) {
        .null => true,
        .type => |lhs_type| switch (rhs) {
            .null => true,
            .type => |rhs_type| (lhs_type == .datetime and rhs_type == .numeric) or
                (lhs_type == .numeric and rhs_type == .datetime) or
                lhs_type == rhs_type,
        },
    };
}

pub fn checkExpressionTypeEquals(value: CheckExpressionType, expected: runtime_schema.AntflyType) bool {
    return switch (value) {
        .null => false,
        .type => |actual| actual == expected,
    };
}

pub fn checkExpressionTypeTextLike(value: CheckExpressionType) bool {
    return switch (value) {
        .null => false,
        .type => |field_type| switch (field_type) {
            .text, .keyword, .link, .html, .search_as_you_type, .blob, .geoshape => true,
            else => false,
        },
    };
}

pub fn checkExpressionTypeOrderable(value: CheckExpressionType) bool {
    return checkExpressionTypeTextLike(value) or
        checkExpressionTypeEquals(value, .numeric) or
        checkExpressionTypeEquals(value, .datetime) or
        checkExpressionTypeEquals(value, .boolean);
}

pub fn validateGeneratedColumnExpressionForColumns(
    columns: []const runtime_schema.RelationalColumn,
    generated_column_name: []const u8,
    expression: runtime_schema.RelationalRowsExpression,
) error{InvalidSqlCatalog}!void {
    if (expression.kind == .field) {
        if (std.mem.eql(u8, expression.field, generated_column_name)) return error.InvalidSqlCatalog;
        _ = binder.relationalColumnForDdl(columns, expression.field) orelse return error.InvalidSqlCatalog;
    }
    for (expression.operands) |operand| try validateGeneratedColumnExpressionForColumns(columns, generated_column_name, operand);
    for (expression.case_branches) |branch| {
        try validateGeneratedColumnExpressionConditionForColumns(columns, generated_column_name, branch.when);
        try validateGeneratedColumnExpressionForColumns(columns, generated_column_name, branch.then);
    }
    for (expression.case_else) |case_else| try validateGeneratedColumnExpressionForColumns(columns, generated_column_name, case_else);
}

fn validateGeneratedColumnExpressionConditionForColumns(
    columns: []const runtime_schema.RelationalColumn,
    generated_column_name: []const u8,
    condition: runtime_schema.RelationalRowsExpressionCondition,
) error{InvalidSqlCatalog}!void {
    try validateGeneratedColumnExpressionForColumns(columns, generated_column_name, condition.lhs);
    for (condition.rhs) |rhs| try validateGeneratedColumnExpressionForColumns(columns, generated_column_name, rhs);
}

pub fn validateUniquePredicateExpressionsForColumns(
    columns: []const runtime_schema.RelationalColumn,
    conditions: []const runtime_schema.RelationalRowsExpressionCondition,
) !void {
    for (conditions) |condition| {
        try validateCheckExpressionConditionForColumns(columns, condition);
        if (!rowExpressionDeterministic(condition.lhs)) return error.InvalidSqlCatalog;
        for (condition.rhs) |rhs| {
            if (!rowExpressionDeterministic(rhs)) return error.InvalidSqlCatalog;
        }
    }
}

pub fn validateCheckForColumns(columns: []const runtime_schema.RelationalColumn, check: runtime_schema.RelationalCheck) !void {
    if (check.expression) |condition| {
        if (check.field.len != 0 or check.value_json != null) return error.InvalidSqlCatalog;
        return validateCheckExpressionConditionForColumns(columns, condition);
    }
    const column = binder.relationalColumnForDdl(columns, check.field) orelse return error.InvalidSqlCatalog;
    switch (check.op) {
        .is_null, .is_not_null => if (check.value_json != null) return error.InvalidSqlCatalog,
        .eq, .ne, .is_distinct, .is_not_distinct, .gt, .gte, .lt, .lte => {
            const value_json = check.value_json orelse return error.InvalidSqlCatalog;
            const field_type: CheckExpressionType = .{ .type = column.field_type };
            const value_type = try checkExpressionLiteralType(value_json);
            if (!checkExpressionTypesComparable(field_type, value_type)) return error.InvalidSqlCatalog;
            switch (check.op) {
                .gt, .gte, .lt, .lte => if (!checkExpressionTypeOrderable(field_type) or !checkExpressionTypeOrderable(value_type)) return error.InvalidSqlCatalog,
                else => {},
            }
        },
    }
}

pub fn validateGeneratedColumnForColumns(columns: []const runtime_schema.RelationalColumn, column: runtime_schema.RelationalColumn) !void {
    const generated = column.generated orelse return;
    switch (generated.op) {
        .lower, .upper, .md5 => {
            const field = generated.field orelse return error.InvalidSqlCatalog;
            if (std.mem.eql(u8, field, column.name)) return error.InvalidSqlCatalog;
            const source = binder.relationalColumnForDdl(columns, field) orelse return error.InvalidSqlCatalog;
            if (source.field_type == .json or source.field_type == .array) return error.InvalidSqlCatalog;
        },
        .concat => {
            if (generated.fields.len == 0) return error.InvalidSqlCatalog;
            for (generated.fields) |field| {
                if (std.mem.eql(u8, field, column.name)) return error.InvalidSqlCatalog;
                const source = binder.relationalColumnForDdl(columns, field) orelse return error.InvalidSqlCatalog;
                if (source.field_type == .json or source.field_type == .array) return error.InvalidSqlCatalog;
            }
        },
        .concat_ws => {
            if (generated.fields.len == 0) return error.InvalidSqlCatalog;
            for (generated.fields) |field| {
                if (std.mem.eql(u8, field, column.name)) return error.InvalidSqlCatalog;
                const source = binder.relationalColumnForDdl(columns, field) orelse return error.InvalidSqlCatalog;
                if (!checkExpressionTypeTextLike(.{ .type = source.field_type })) return error.InvalidSqlCatalog;
            }
        },
        .expression => {
            const expression = generated.expression orelse return error.InvalidSqlCatalog;
            try validateGeneratedColumnExpressionForColumns(columns, column.name, expression);
        },
    }
}

pub fn validateCreateIndexIncludeColumns(
    columns: []const runtime_schema.RelationalColumn,
    key_columns: []const []const u8,
    include_columns: []const []const u8,
) !void {
    for (include_columns) |column| {
        if (stringSlicesContains(key_columns, column)) return error.InvalidSqlCatalog;
        const found = binder.relationalColumnForDdl(columns, column) orelse return error.InvalidSqlCatalog;
        if (found.field_type == .json or found.field_type == .array) return error.InvalidSqlCatalog;
    }
}

pub fn validateUniquePredicatesForColumns(columns: []const runtime_schema.RelationalColumn, predicates: []const runtime_schema.UniquePredicate) !void {
    for (predicates) |predicate| {
        const found = binder.relationalColumnForDdl(columns, predicate.field) orelse return error.InvalidSqlCatalog;
        if (found.field_type == .json or found.field_type == .array) return error.InvalidSqlCatalog;
    }
}

pub fn validateUniqueConstraintForColumns(columns: []const runtime_schema.RelationalColumn, periods: []const runtime_schema.RelationalPeriod, constraint: runtime_schema.UniqueConstraint) !void {
    if (constraint.columns.len == 0 and constraint.expressions.len == 0) return error.InvalidSqlCatalog;
    if (constraint.without_overlaps_period) |period| {
        _ = binder.relationalPeriodForDdl(periods, period) orelse return error.InvalidSqlCatalog;
    }
    for (constraint.columns) |column| {
        const found = binder.relationalColumnForDdl(columns, column) orelse return error.InvalidSqlCatalog;
        if (found.field_type == .json or found.field_type == .array) return error.InvalidSqlCatalog;
    }
    for (constraint.expressions) |expression| {
        switch (expression.op) {
            .lower, .upper, .md5 => {
                const found = binder.relationalColumnForDdl(columns, expression.field) orelse return error.InvalidSqlCatalog;
                if (found.field_type == .json or found.field_type == .array) return error.InvalidSqlCatalog;
            },
            .expression => {
                const row_expression = expression.expression orelse return error.InvalidSqlCatalog;
                try validateCheckExpressionForColumns(columns, row_expression);
                if (!rowExpressionDeterministic(row_expression)) return error.InvalidSqlCatalog;
                if (!checkExpressionTypeOrderable(try checkExpressionTypeForColumns(columns, row_expression))) return error.InvalidSqlCatalog;
            },
        }
    }
    try validateCreateIndexIncludeColumns(columns, constraint.columns, constraint.include_columns);
    try validateUniquePredicatesForColumns(columns, constraint.where);
    try validateUniquePredicateExpressionsForColumns(columns, constraint.where_expressions);
}

pub fn validateRelationalColumnCatalog(columns: []const runtime_schema.RelationalColumn) !void {
    for (columns, 0..) |column, i| {
        if (binder.relationalColumnIndex(columns[0..i], column.name) != null) return error.InvalidSqlCatalog;
        if (!std.mem.eql(u8, column.name, column.path)) return error.InvalidSqlCatalog;
        if (column.collation != null and !binder.relationalFieldTypeSupportsCollation(column.field_type)) return error.InvalidSqlCatalog;
        if (column.generated) |_| try validateGeneratedColumnForColumns(columns, column);
        try validateUniquePredicatesForColumns(columns, column.index_where);
        try validateUniquePredicateExpressionsForColumns(columns, column.index_where_expressions);
        try validateRelationalColumnIndexIncludes(columns, column);
        if (column.on_update_value) |_| {
            if (column.field_type != .numeric and column.field_type != .datetime) return error.InvalidSqlCatalog;
        }
    }
}

pub fn validateRelationalColumnIndexIncludes(columns: []const runtime_schema.RelationalColumn, column: runtime_schema.RelationalColumn) !void {
    if (column.index_include_columns.len == 0) return;
    if (!column.indexed or column.index_name == null) return error.InvalidSqlCatalog;
    for (column.index_include_columns) |field| {
        _ = binder.relationalColumnForDdl(columns, field) orelse return error.InvalidSqlCatalog;
    }
    const index_name = column.index_name.?;
    for (columns) |peer| {
        if (!binder.relationalColumnHasDeclaredIndexName(peer, index_name)) continue;
        if (!stringSlicesEqual(peer.index_include_columns, column.index_include_columns)) return error.InvalidSqlCatalog;
        if (stringSlicesContains(column.index_include_columns, peer.name)) return error.InvalidSqlCatalog;
    }
}

pub fn validateUniqueConstraintCatalog(columns: []const runtime_schema.RelationalColumn, periods: []const runtime_schema.RelationalPeriod, constraints: []const runtime_schema.UniqueConstraint) !void {
    for (constraints, 0..) |constraint, i| {
        if (binder.uniqueConstraintNameExists(constraints[0..i], constraint.name)) return error.InvalidSqlCatalog;
        try validateUniqueConstraintForColumns(columns, periods, constraint);
    }
}

pub fn validateForeignKeyCatalog(columns: []const runtime_schema.RelationalColumn, periods: []const runtime_schema.RelationalPeriod, foreign_keys: []const runtime_schema.ForeignKey) !void {
    for (foreign_keys, 0..) |foreign_key, i| {
        if (binder.foreignKeyNameExists(foreign_keys[0..i], foreign_key.name)) return error.InvalidSqlCatalog;
        try binder.validateForeignKeyForColumns(columns, periods, foreign_key);
    }
}

pub fn validateRelationalCheckCatalog(columns: []const runtime_schema.RelationalColumn, checks: []const runtime_schema.RelationalCheck) !void {
    for (checks, 0..) |check, i| {
        if (binder.relationalCheckNameExists(checks[0..i], check.name)) return error.InvalidSqlCatalog;
        try validateCheckForColumns(columns, check);
    }
}

pub fn validatePrimaryKeyColumns(columns: []const runtime_schema.RelationalColumn, primary_key: runtime_schema.PrimaryKey) !void {
    if (primary_key.columns.len == 0) return error.InvalidSqlCatalog;
    for (primary_key.columns) |column| {
        const found = binder.relationalColumnForDdl(columns, column) orelse return error.InvalidSqlCatalog;
        if (found.nullable) return error.InvalidSqlCatalog;
    }
    try validateCreateIndexIncludeColumns(columns, primary_key.columns, primary_key.include_columns);
}

pub fn generatedColumnReferencesAny(column: runtime_schema.RelationalColumn, fields: []const []const u8) bool {
    const generated = column.generated orelse return false;
    if (generated.field) |field| {
        if (stringSlicesContains(fields, field)) return true;
    }
    if (generated.expression) |expression| {
        if (expressionReferencesAny(expression, fields)) return true;
    }
    return stringSlicesIntersect(generated.fields, fields);
}

pub fn expressionReferencesAny(expression: runtime_schema.RelationalRowsExpression, fields: []const []const u8) bool {
    if (expression.kind == .field and stringSlicesContains(fields, expression.field)) return true;
    for (expression.operands) |operand| {
        if (expressionReferencesAny(operand, fields)) return true;
    }
    for (expression.case_branches) |branch| {
        if (expressionConditionReferencesAny(branch.when, fields)) return true;
        if (expressionReferencesAny(branch.then, fields)) return true;
    }
    for (expression.case_else) |case_else| {
        if (expressionReferencesAny(case_else, fields)) return true;
    }
    return false;
}

pub fn expressionConditionReferencesAny(condition: runtime_schema.RelationalRowsExpressionCondition, fields: []const []const u8) bool {
    if (expressionReferencesAny(condition.lhs, fields)) return true;
    for (condition.rhs) |rhs| {
        if (expressionReferencesAny(rhs, fields)) return true;
    }
    return false;
}

pub fn expressionReferencesField(expression: runtime_schema.RelationalRowsExpression, field: []const u8) bool {
    if (expression.kind == .field and expression.field_source == .row and std.mem.eql(u8, expression.field, field)) return true;
    for (expression.operands) |operand| {
        if (expressionReferencesField(operand, field)) return true;
    }
    for (expression.case_branches) |branch| {
        if (expressionConditionReferencesField(branch.when, field)) return true;
        if (expressionReferencesField(branch.then, field)) return true;
    }
    for (expression.case_else) |fallback| {
        if (expressionReferencesField(fallback, field)) return true;
    }
    return false;
}

pub fn expressionConditionReferencesField(condition: runtime_schema.RelationalRowsExpressionCondition, field: []const u8) bool {
    if (expressionReferencesField(condition.lhs, field)) return true;
    for (condition.rhs) |rhs| {
        if (expressionReferencesField(rhs, field)) return true;
    }
    return false;
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

pub fn uniqueConstraintReferencesAny(
    constraint: runtime_schema.UniqueConstraint,
    fields: []const []const u8,
) bool {
    if (stringSlicesIntersect(constraint.columns, fields)) return true;
    for (constraint.expressions) |expression| {
        switch (expression.op) {
            .lower, .upper, .md5 => if (stringSlicesContains(fields, expression.field)) return true,
            .expression => if (expression.expression) |row_expression| {
                if (expressionReferencesAny(row_expression, fields)) return true;
            },
        }
    }
    for (constraint.where) |predicate| {
        if (stringSlicesContains(fields, predicate.field)) return true;
    }
    for (constraint.where_expressions) |condition| {
        if (expressionConditionReferencesAny(condition, fields)) return true;
    }
    return false;
}

pub fn queryHasOnlySimpleUnionPredicateSurface(query: db_mod.types.RelationalRowsQueryRequest) bool {
    if (query.array_any.len != 0 or
        query.array_contains.len != 0 or
        query.array_eq.len != 0 or
        query.json_contains.len != 0 or
        query.json_path_eq.len != 0 or
        query.json_path_exists.len != 0 or
        query.text_patterns.len != 0 or
        query.not_predicates.len != 0 or
        query.access_or_predicates.len != 0 or
        query.access_not_predicates.len != 0 or
        query.expression_not_predicates.len != 0 or
        query.expression_array_contains.len != 0 or
        query.json_extract.len != 0 or
        query.array_length.len != 0 or
        query.coalesce.len != 0 or
        query.field_aliases.len != 0 or
        query.distinct_on.len != 0 or
        query.distinct_on_expressions.len != 0 or
        query.order_by.len != 0 or
        query.row_claim != null or
        query.doc_key_range != null or
        query.limit != null or
        query.offset != 0)
    {
        return false;
    }
    return true;
}

pub fn queryHasOnlySimpleIntersectExceptPredicateSurface(query: db_mod.types.RelationalRowsQueryRequest) bool {
    if (query.array_any.len != 0 or
        query.array_contains.len != 0 or
        query.array_eq.len != 0 or
        query.json_contains.len != 0 or
        query.json_path_eq.len != 0 or
        query.json_path_exists.len != 0 or
        query.text_patterns.len != 0 or
        query.not_predicates.len != 0 or
        query.access_or_predicates.len != 0 or
        query.access_not_predicates.len != 0 or
        query.expression_not_predicates.len != 0 or
        query.expression_array_contains.len != 0 or
        query.json_extract.len != 0 or
        query.array_length.len != 0 or
        query.coalesce.len != 0 or
        query.field_aliases.len != 0 or
        query.distinct_on.len != 0 or
        query.distinct_on_expressions.len != 0 or
        query.order_by.len != 0 or
        query.row_claim != null or
        query.doc_key_range != null or
        query.limit != null or
        query.offset != 0)
    {
        return false;
    }
    return true;
}

pub fn simpleSelectProjectionsEqual(
    lhs: db_mod.types.RelationalRowsQueryRequest,
    rhs: db_mod.types.RelationalRowsQueryRequest,
    lhs_outputs: []const ast.SelectOutputRef,
    rhs_outputs: []const ast.SelectOutputRef,
) bool {
    if (lhs.select_all != rhs.select_all) return false;
    if (!stringSlicesEqual(lhs.select, rhs.select)) return false;
    if (!expressionProjectionsEqual(lhs.expressions, rhs.expressions)) return false;
    if (lhs_outputs.len != rhs_outputs.len) return false;
    for (lhs_outputs, rhs_outputs) |left, right| {
        if (left.kind != right.kind or left.index != right.index) return false;
    }
    return true;
}

pub fn setOperationColumnsCompatible(
    lhs: []const runtime_schema.RelationalColumn,
    rhs: []const runtime_schema.RelationalColumn,
) bool {
    if (lhs.len == 0 or lhs.len != rhs.len) return false;
    for (lhs, rhs) |left, right| {
        if (!std.mem.eql(u8, left.name, right.name)) return false;
        if (left.field_type != right.field_type) return false;
        if (left.array_item_type != right.array_item_type) return false;
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
    if (!relationalRowsExpressionEqual(lhs.lhs, rhs.lhs)) return false;
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
        if ((lhs_op == .eq and rhs_op == .is_distinct) or (lhs_op == .is_distinct and rhs_op == .eq)) return true;
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

fn jsonScalarLiteralsDefinitelyEqual(lhs: ?[]const u8, rhs: ?[]const u8) bool {
    const left = lhs orelse return false;
    const right = rhs orelse return false;
    if (!jsonIsSafeDisjointProofLiteral(left) or !jsonIsSafeDisjointProofLiteral(right)) return false;
    return std.mem.eql(u8, left, right);
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

fn stringSlicesIntersect(a: []const []const u8, b: []const []const u8) bool {
    for (a) |value| {
        if (stringSlicesContains(b, value)) return true;
    }
    return false;
}

pub fn validateSqlUniqueExpressionListUnique(expressions: []const runtime_schema.UniqueExpression) !void {
    for (expressions, 0..) |lhs, i| {
        for (expressions[i + 1 ..]) |rhs| {
            if (lhs.op != rhs.op) continue;
            if (lhs.op == .expression) {
                if (lhs.expression != null and rhs.expression != null and relationalRowsExpressionEqual(lhs.expression.?, rhs.expression.?)) return error.UnsupportedSqlShape;
                continue;
            }
            if (std.ascii.eqlIgnoreCase(lhs.field, rhs.field)) return error.UnsupportedSqlShape;
        }
    }
}

pub fn validateExtensionFunctionArity(
    kind: runtime_schema.RelationalRowsExpressionKind,
    declared_arity: u16,
    operand_count: usize,
) !void {
    if (operand_count != @as(usize, declared_arity)) return error.UnsupportedSqlShape;
    switch (kind) {
        .uuid_v4 => if (operand_count != 0) return error.UnsupportedSqlShape,
        .lower, .upper, .md5 => if (operand_count != 1) return error.UnsupportedSqlShape,
        .concat => if (operand_count == 0) return error.UnsupportedSqlShape,
        .concat_ws => if (operand_count < 2) return error.UnsupportedSqlShape,
        else => return error.UnsupportedSqlShape,
    }
}

pub fn sqlJoinedSourceAliasTerminator(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "where") or
        std.ascii.eqlIgnoreCase(text, "returning") or
        std.ascii.eqlIgnoreCase(text, "order") or
        std.ascii.eqlIgnoreCase(text, "limit") or
        std.ascii.eqlIgnoreCase(text, "offset") or
        std.ascii.eqlIgnoreCase(text, "for");
}

pub fn sqlAssignmentTailKeyword(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "where") or
        std.ascii.eqlIgnoreCase(text, "from") or
        std.ascii.eqlIgnoreCase(text, "returning") or
        std.ascii.eqlIgnoreCase(text, "order") or
        std.ascii.eqlIgnoreCase(text, "limit") or
        std.ascii.eqlIgnoreCase(text, "offset") or
        std.ascii.eqlIgnoreCase(text, "for");
}

pub fn sqlKeywordIsLengthFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "length") or
        std.ascii.eqlIgnoreCase(text, "char_length") or
        std.ascii.eqlIgnoreCase(text, "character_length");
}

pub fn sqlKeywordIsOctetLengthFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "octet_length");
}

pub fn sqlKeywordIsBitLengthFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "bit_length");
}

pub fn sqlKeywordIsJsonArrayLengthFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "json_array_length") or
        std.ascii.eqlIgnoreCase(text, "jsonb_array_length");
}

pub fn sqlKeywordIsCardinalityFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "cardinality");
}

pub fn sqlKeywordIsArrayLengthFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "array_length") or
        sqlKeywordIsCardinalityFunction(text);
}

pub fn sqlKeywordIsArrayPositionFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "array_position") or
        std.ascii.eqlIgnoreCase(text, "array_positions");
}

pub fn sqlKeywordIsArrayToStringFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "array_to_string");
}

pub fn arrayLengthDefaultOutput(keyword: []const u8) []const u8 {
    if (sqlKeywordIsCardinalityFunction(keyword)) return "cardinality";
    return "array_length";
}

pub fn sqlKeywordIsJsonTypeofFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "json_typeof") or
        std.ascii.eqlIgnoreCase(text, "jsonb_typeof");
}

pub fn sqlKeywordIsJsonExtractPathFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "json_extract_path") or
        std.ascii.eqlIgnoreCase(text, "json_extract_path_text") or
        std.ascii.eqlIgnoreCase(text, "jsonb_extract_path") or
        std.ascii.eqlIgnoreCase(text, "jsonb_extract_path_text");
}

pub fn sqlKeywordIsJsonBuildObjectFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "json_build_object") or
        std.ascii.eqlIgnoreCase(text, "jsonb_build_object");
}

pub fn sqlJsonExtractPathFunctionAsText(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "json_extract_path_text") or
        std.ascii.eqlIgnoreCase(text, "jsonb_extract_path_text");
}

pub fn sqlKeywordIsAsciiFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "ascii");
}

pub fn sqlKeywordIsChrFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "chr");
}

pub fn sqlKeywordIsSubstringFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "substring") or
        std.ascii.eqlIgnoreCase(text, "substr");
}

pub fn sqlKeywordIsOverlayFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "overlay");
}

pub fn sqlKeywordIsTranslateFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "translate");
}

pub fn sqlKeywordIsSplitPartFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "split_part");
}

pub fn sqlKeywordIsStrposFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "strpos");
}

pub fn sqlKeywordIsLeftRightFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "left") or
        std.ascii.eqlIgnoreCase(text, "right");
}

pub fn sqlKeywordIsPadFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "lpad") or
        std.ascii.eqlIgnoreCase(text, "rpad");
}

pub fn sqlKeywordIsRepeatFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "repeat");
}

pub fn sqlKeywordIsReverseFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "reverse");
}

pub fn sqlKeywordIsInitcapFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "initcap");
}

pub fn sqlKeywordIsMd5Function(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "md5");
}

pub fn sqlKeywordIsStartsWithFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "starts_with");
}

pub fn sqlKeywordIsEndsWithFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "ends_with");
}

pub fn sqlKeywordIsDateTruncFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "date_trunc");
}

pub fn sqlKeywordIsDateBinFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "date_bin");
}

pub fn sqlKeywordIsDatePartFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "date_part") or
        std.ascii.eqlIgnoreCase(text, "extract");
}

pub fn sqlKeywordIsTrimVariantFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "btrim") or
        std.ascii.eqlIgnoreCase(text, "ltrim") or
        std.ascii.eqlIgnoreCase(text, "rtrim");
}

pub fn sqlKeywordIsUuidV4Function(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "gen_random_uuid") or
        std.ascii.eqlIgnoreCase(text, "uuid_generate_v4");
}

pub fn sqlKeywordIsRegexpMatchFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "regexp_match") or
        std.ascii.eqlIgnoreCase(text, "regexp_like");
}

pub fn sqlKeywordIsRegexpCountFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "regexp_count");
}

pub fn sqlKeywordIsRegexpSubstrFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "regexp_substr");
}

pub fn sqlKeywordIsRegexpInstrFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "regexp_instr");
}

pub fn rowExpressionBoundaryKeyword(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "as") or
        std.ascii.eqlIgnoreCase(text, "from") or
        std.ascii.eqlIgnoreCase(text, "where") or
        std.ascii.eqlIgnoreCase(text, "and") or
        std.ascii.eqlIgnoreCase(text, "or") or
        std.ascii.eqlIgnoreCase(text, "group") or
        std.ascii.eqlIgnoreCase(text, "having") or
        std.ascii.eqlIgnoreCase(text, "order") or
        std.ascii.eqlIgnoreCase(text, "limit") or
        std.ascii.eqlIgnoreCase(text, "offset") or
        std.ascii.eqlIgnoreCase(text, "fetch") or
        std.ascii.eqlIgnoreCase(text, "for") or
        std.ascii.eqlIgnoreCase(text, "asc") or
        std.ascii.eqlIgnoreCase(text, "desc") or
        std.ascii.eqlIgnoreCase(text, "nulls") or
        std.ascii.eqlIgnoreCase(text, "then") or
        std.ascii.eqlIgnoreCase(text, "else") or
        std.ascii.eqlIgnoreCase(text, "end") or
        std.ascii.eqlIgnoreCase(text, "when") or
        std.ascii.eqlIgnoreCase(text, "filter") or
        std.ascii.eqlIgnoreCase(text, "over");
}

pub fn sqlWhereTailClauseKeyword(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "order") or
        std.ascii.eqlIgnoreCase(text, "limit") or
        std.ascii.eqlIgnoreCase(text, "offset") or
        std.ascii.eqlIgnoreCase(text, "for") or
        std.ascii.eqlIgnoreCase(text, "group") or
        std.ascii.eqlIgnoreCase(text, "having") or
        std.ascii.eqlIgnoreCase(text, "returning") or
        std.ascii.eqlIgnoreCase(text, "join") or
        std.ascii.eqlIgnoreCase(text, "left") or
        std.ascii.eqlIgnoreCase(text, "inner") or
        std.ascii.eqlIgnoreCase(text, "with") or
        std.ascii.eqlIgnoreCase(text, "over") or
        std.ascii.eqlIgnoreCase(text, "lateral");
}

pub fn sqlWindowTailClauseKeyword(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "order") or
        std.ascii.eqlIgnoreCase(text, "limit") or
        std.ascii.eqlIgnoreCase(text, "offset") or
        std.ascii.eqlIgnoreCase(text, "fetch") or
        std.ascii.eqlIgnoreCase(text, "for");
}

pub fn parenthesizedPredicateGroupCanStartAt(tokens: []const Token, index: usize) bool {
    if (index >= tokens.len or tokens[index].kind != .lparen) return false;
    const close = parser.findMatchingRParenIndex(tokens, index) orelse return false;
    if (close + 1 >= tokens.len) return true;
    return !tokenContinuesScalarExpressionPredicate(tokens, close + 1);
}

fn tokenContinuesScalarExpressionPredicate(tokens: []const Token, index: usize) bool {
    const token = tokens[index];
    switch (token.kind) {
        .eq, .neq, .gt, .gte, .lt, .lte, .arrow_text, .arrow_json, .path_arrow_text, .path_arrow_json => return true,
        .identifier => {
            if (std.ascii.eqlIgnoreCase(token.text, "is") or
                std.ascii.eqlIgnoreCase(token.text, "like") or
                std.ascii.eqlIgnoreCase(token.text, "ilike") or
                std.ascii.eqlIgnoreCase(token.text, "in") or
                std.ascii.eqlIgnoreCase(token.text, "between"))
            {
                return true;
            }
            if (std.ascii.eqlIgnoreCase(token.text, "not") and index + 1 < tokens.len and tokens[index + 1].kind == .identifier) {
                const after_not = tokens[index + 1].text;
                if (std.ascii.eqlIgnoreCase(after_not, "like") or
                    std.ascii.eqlIgnoreCase(after_not, "ilike") or
                    std.ascii.eqlIgnoreCase(after_not, "in") or
                    std.ascii.eqlIgnoreCase(after_not, "between"))
                {
                    return true;
                }
            }
        },
        else => {},
    }
    return false;
}

test "sql adapter expression keyword predicates classify function and tail tokens" {
    try std.testing.expect(sqlKeywordIsAnyOrSome("SOME"));
    try std.testing.expect(sqlKeywordStartsScalarPredicate("between"));
    try std.testing.expect(sqlKeywordIsArrayLengthFunction("cardinality"));
    try std.testing.expectEqualStrings("cardinality", arrayLengthDefaultOutput("cardinality"));
    try std.testing.expect(sqlKeywordIsJsonExtractPathFunction("jsonb_extract_path_text"));
    try std.testing.expect(sqlJsonExtractPathFunctionAsText("json_extract_path_text"));
    try std.testing.expect(sqlKeywordIsRegexpMatchFunction("regexp_like"));
    try std.testing.expect(rowExpressionBoundaryKeyword("returning") == false);
    try std.testing.expect(sqlWhereTailClauseKeyword("returning"));
    try std.testing.expect(sqlWindowTailClauseKeyword("fetch"));
}

test "sql adapter expression grammar distinguishes grouped predicates from scalar expression predicates" {
    const grouped = [_]Token{
        .{ .kind = .lparen, .text = "(", .source_start = 0, .source_end = 1 },
        .{ .kind = .identifier, .text = "a", .source_start = 1, .source_end = 2 },
        .{ .kind = .eq, .text = "=", .source_start = 3, .source_end = 4 },
        .{ .kind = .number, .text = "1", .source_start = 5, .source_end = 6 },
        .{ .kind = .rparen, .text = ")", .source_start = 6, .source_end = 7 },
        .{ .kind = .identifier, .text = "and", .source_start = 8, .source_end = 11 },
    };
    try std.testing.expect(parenthesizedPredicateGroupCanStartAt(grouped[0..], 0));

    const compared_expression = [_]Token{
        .{ .kind = .lparen, .text = "(", .source_start = 0, .source_end = 1 },
        .{ .kind = .identifier, .text = "a", .source_start = 1, .source_end = 2 },
        .{ .kind = .rparen, .text = ")", .source_start = 2, .source_end = 3 },
        .{ .kind = .eq, .text = "=", .source_start = 4, .source_end = 5 },
        .{ .kind = .number, .text = "1", .source_start = 6, .source_end = 7 },
    };
    try std.testing.expect(!parenthesizedPredicateGroupCanStartAt(compared_expression[0..], 0));

    const not_like_expression = [_]Token{
        .{ .kind = .lparen, .text = "(", .source_start = 0, .source_end = 1 },
        .{ .kind = .identifier, .text = "name", .source_start = 1, .source_end = 5 },
        .{ .kind = .rparen, .text = ")", .source_start = 5, .source_end = 6 },
        .{ .kind = .identifier, .text = "not", .source_start = 7, .source_end = 10 },
        .{ .kind = .identifier, .text = "ilike", .source_start = 11, .source_end = 16 },
        .{ .kind = .string, .text = "%bot%", .source_start = 17, .source_end = 24 },
    };
    try std.testing.expect(!parenthesizedPredicateGroupCanStartAt(not_like_expression[0..], 0));
}

test "sql adapter lower expr compares row expressions" {
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

test "sql adapter lower expr compares aggregate specs" {
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

test "sql adapter lower expr detects catalog expression references" {
    const status_field: runtime_schema.RelationalRowsExpression = .{ .kind = .field, .field = "status" };
    const tenant_field: runtime_schema.RelationalRowsExpression = .{ .kind = .field, .field = "tenant_id" };
    const literal: runtime_schema.RelationalRowsExpression = .{ .kind = .value, .value_json = "\"active\"" };
    const condition: runtime_schema.RelationalRowsExpressionCondition = .{
        .lhs = status_field,
        .op = .eq,
        .rhs = &.{literal},
    };
    const generated_column: runtime_schema.RelationalColumn = .{
        .name = "tenant_status",
        .path = "tenant_status",
        .field_type = .keyword,
        .generated = .{ .op = .concat_ws, .fields = &.{ "tenant_id", "status" }, .separator = ":" },
    };
    const expression_generated: runtime_schema.RelationalColumn = .{
        .name = "status_lower",
        .path = "status_lower",
        .field_type = .keyword,
        .generated = .{ .op = .expression, .expression = .{ .kind = .lower, .operands = &.{status_field} } },
    };
    const unique_constraint: runtime_schema.UniqueConstraint = .{
        .name = "tenant_status_key",
        .columns = &.{"tenant_id"},
        .expressions = &.{.{ .op = .expression, .expression = .{ .kind = .concat, .operands = &.{ tenant_field, status_field } } }},
        .where_expressions = &.{condition},
    };

    try std.testing.expect(generatedColumnReferencesAny(generated_column, &.{"tenant_id"}));
    try std.testing.expect(generatedColumnReferencesAny(expression_generated, &.{"status"}));
    try std.testing.expect(expressionConditionReferencesAny(condition, &.{"status"}));
    try std.testing.expect(uniqueConstraintReferencesAny(unique_constraint, &.{"status"}));
    try std.testing.expect(!uniqueConstraintReferencesAny(unique_constraint, &.{"missing"}));
}

test "sql adapter lower expr compares query projection and set operation surfaces" {
    const status_field: runtime_schema.RelationalRowsExpression = .{ .kind = .field, .field = "status" };
    const lower_status: runtime_schema.RelationalRowsExpression = .{
        .kind = .lower,
        .operands = &.{status_field},
    };
    const projection: db_mod.types.RelationalRowsExpressionProjection = .{
        .output = "status_lower",
        .expression = lower_status,
    };
    const simple_query: db_mod.types.RelationalRowsQueryRequest = .{
        .select = &.{"status"},
        .expressions = &.{projection},
    };
    const same_query: db_mod.types.RelationalRowsQueryRequest = .{
        .select = &.{"status"},
        .expressions = &.{projection},
    };
    const limited_query: db_mod.types.RelationalRowsQueryRequest = .{
        .select = &.{"status"},
        .expressions = &.{projection},
        .limit = 10,
    };
    const left_outputs = [_]ast.SelectOutputRef{.{ .kind = .field, .index = 0 }};
    const right_outputs = [_]ast.SelectOutputRef{.{ .kind = .field, .index = 0 }};
    const mismatched_outputs = [_]ast.SelectOutputRef{.{ .kind = .expression, .index = 0 }};
    const left_columns = [_]runtime_schema.RelationalColumn{.{ .name = "status", .path = "status", .field_type = .keyword }};
    const right_columns = [_]runtime_schema.RelationalColumn{.{ .name = "status", .path = "status", .field_type = .keyword }};
    const incompatible_columns = [_]runtime_schema.RelationalColumn{.{ .name = "status", .path = "status", .field_type = .numeric }};

    try std.testing.expect(queryHasOnlySimpleUnionPredicateSurface(simple_query));
    try std.testing.expect(queryHasOnlySimpleIntersectExceptPredicateSurface(simple_query));
    try std.testing.expect(!queryHasOnlySimpleUnionPredicateSurface(limited_query));
    try std.testing.expect(!queryHasOnlySimpleIntersectExceptPredicateSurface(limited_query));
    try std.testing.expect(expressionProjectionsEqual(&.{projection}, &.{projection}));
    try std.testing.expect(simpleSelectProjectionsEqual(simple_query, same_query, &left_outputs, &right_outputs));
    try std.testing.expect(!simpleSelectProjectionsEqual(simple_query, same_query, &left_outputs, &mismatched_outputs));
    try std.testing.expect(setOperationColumnsCompatible(&left_columns, &right_columns));
    try std.testing.expect(!setOperationColumnsCompatible(&left_columns, &incompatible_columns));
}

test "sql adapter lower expr proves simple predicate disjointness" {
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

test "sql adapter lower expr validates unique expression lists" {
    try validateSqlUniqueExpressionListUnique(&.{
        .{ .op = .lower, .field = "status" },
        .{ .op = .upper, .field = "status" },
    });
    try std.testing.expectError(error.UnsupportedSqlShape, validateSqlUniqueExpressionListUnique(&.{
        .{ .op = .lower, .field = "status" },
        .{ .op = .lower, .field = "STATUS" },
    }));

    const lower_status: runtime_schema.RelationalRowsExpression = .{
        .kind = .lower,
        .operands = &.{.{ .kind = .field, .field = "status" }},
    };
    try std.testing.expectError(error.UnsupportedSqlShape, validateSqlUniqueExpressionListUnique(&.{
        .{ .op = .expression, .expression = lower_status },
        .{ .op = .expression, .expression = lower_status },
    }));
}

test "sql adapter lower expr detects deterministic row expressions" {
    const status_field: runtime_schema.RelationalRowsExpression = .{ .kind = .field, .field = "status" };
    const literal: runtime_schema.RelationalRowsExpression = .{ .kind = .value, .value_json = "\"open\"" };
    const source_field: runtime_schema.RelationalRowsExpression = .{ .kind = .field, .field = "status", .field_source = .source };
    const now_expression: runtime_schema.RelationalRowsExpression = .{ .kind = .now };
    const condition: runtime_schema.RelationalRowsExpressionCondition = .{
        .lhs = status_field,
        .op = .eq,
        .rhs = &.{literal},
    };
    const nondeterministic_condition: runtime_schema.RelationalRowsExpressionCondition = .{
        .lhs = status_field,
        .op = .eq,
        .rhs = &.{now_expression},
    };

    try std.testing.expect(rowExpressionDeterministic(status_field));
    try std.testing.expect(!rowExpressionDeterministic(source_field));
    try std.testing.expect(!rowExpressionDeterministic(now_expression));
    try std.testing.expect(rowExpressionConditionDeterministic(condition));
    try std.testing.expect(!rowExpressionConditionDeterministic(nondeterministic_condition));
}

test "sql adapter lower expr validates catalog check expression types" {
    const columns = [_]runtime_schema.RelationalColumn{
        .{ .name = "status", .path = "status", .field_type = .keyword },
        .{ .name = "amount", .path = "amount", .field_type = .numeric },
        .{ .name = "metadata", .path = "metadata", .field_type = .json },
    };
    const status_field: runtime_schema.RelationalRowsExpression = .{ .kind = .field, .field = "status" };
    const amount_field: runtime_schema.RelationalRowsExpression = .{ .kind = .field, .field = "amount" };
    const metadata_field: runtime_schema.RelationalRowsExpression = .{ .kind = .field, .field = "metadata" };
    const numeric_literal: runtime_schema.RelationalRowsExpression = .{ .kind = .value, .value_json = "10" };
    const now_expression: runtime_schema.RelationalRowsExpression = .{ .kind = .now };
    const lower_status: runtime_schema.RelationalRowsExpression = .{
        .kind = .lower,
        .operands = &.{status_field},
    };
    const valid_condition: runtime_schema.RelationalRowsExpressionCondition = .{
        .lhs = amount_field,
        .op = .gt,
        .rhs = &.{numeric_literal},
    };
    const incomparable_condition: runtime_schema.RelationalRowsExpressionCondition = .{
        .lhs = status_field,
        .op = .eq,
        .rhs = &.{metadata_field},
    };
    const nondeterministic_condition: runtime_schema.RelationalRowsExpressionCondition = .{
        .lhs = amount_field,
        .op = .eq,
        .rhs = &.{now_expression},
    };

    try validateCheckExpressionForColumns(&columns, lower_status);
    try validateCheckExpressionConditionForColumns(&columns, valid_condition);
    try std.testing.expectError(error.InvalidSqlCatalog, validateCheckExpressionConditionForColumns(&columns, incomparable_condition));
    try validateGeneratedColumnExpressionForColumns(&columns, "status_lower", lower_status);
    try std.testing.expectError(error.InvalidSqlCatalog, validateGeneratedColumnExpressionForColumns(&columns, "status", status_field));
    try std.testing.expectError(error.InvalidSqlCatalog, validateUniquePredicateExpressionsForColumns(&columns, &.{nondeterministic_condition}));
}

test "sql adapter lower expr validates DDL expression catalog constraints" {
    const columns = [_]runtime_schema.RelationalColumn{
        .{ .name = "status", .path = "status", .field_type = .keyword },
        .{ .name = "amount", .path = "amount", .field_type = .numeric },
        .{ .name = "created_at", .path = "created_at", .field_type = .datetime },
        .{ .name = "updated_at", .path = "updated_at", .field_type = .datetime },
        .{ .name = "metadata", .path = "metadata", .field_type = .json },
    };
    const pk_columns = [_]runtime_schema.RelationalColumn{
        .{ .name = "id", .path = "id", .field_type = .keyword, .nullable = false },
        .{ .name = "status", .path = "status", .field_type = .keyword },
    };
    const periods = [_]runtime_schema.RelationalPeriod{.{ .name = "valid_at", .start_column = "created_at", .end_column = "updated_at" }};
    const lower_status: runtime_schema.RelationalRowsExpression = .{
        .kind = .lower,
        .operands = &.{.{ .kind = .field, .field = "status" }},
    };

    try validateRelationalColumnCatalog(&columns);
    try std.testing.expectError(error.InvalidSqlCatalog, validateRelationalColumnCatalog(&.{.{
        .name = "amount",
        .path = "amount",
        .field_type = .numeric,
        .collation = "C",
    }}));
    try validatePrimaryKeyColumns(&pk_columns, .{ .columns = &.{"id"}, .include_columns = &.{"status"} });
    try std.testing.expectError(error.InvalidSqlCatalog, validatePrimaryKeyColumns(&columns, .{ .columns = &.{"status"} }));

    try validateCheckForColumns(&columns, .{ .name = "amount_positive", .field = "amount", .op = .gt, .value_json = "0" });
    try std.testing.expectError(error.InvalidSqlCatalog, validateCheckForColumns(&columns, .{ .name = "bad_json_order", .field = "metadata", .op = .gt, .value_json = "{}" }));
    try validateRelationalCheckCatalog(&columns, &.{.{ .name = "amount_positive", .field = "amount", .op = .gt, .value_json = "0" }});
    try std.testing.expectError(error.InvalidSqlCatalog, validateRelationalCheckCatalog(&columns, &.{
        .{ .name = "dup_check", .field = "amount", .op = .gt, .value_json = "0" },
        .{ .name = "dup_check", .field = "amount", .op = .lt, .value_json = "10" },
    }));

    try validateGeneratedColumnForColumns(&columns, .{
        .name = "status_lower",
        .path = "status_lower",
        .field_type = .keyword,
        .generated = .{ .op = .expression, .expression = lower_status },
    });
    try std.testing.expectError(error.InvalidSqlCatalog, validateGeneratedColumnForColumns(&columns, .{
        .name = "status",
        .path = "status",
        .field_type = .keyword,
        .generated = .{ .op = .lower, .field = "status" },
    }));

    try validateCreateIndexIncludeColumns(&columns, &.{"status"}, &.{"amount"});
    try std.testing.expectError(error.InvalidSqlCatalog, validateCreateIndexIncludeColumns(&columns, &.{"status"}, &.{"status"}));
    try std.testing.expectError(error.InvalidSqlCatalog, validateCreateIndexIncludeColumns(&columns, &.{"status"}, &.{"metadata"}));

    try validateUniquePredicatesForColumns(&columns, &.{.{ .field = "status", .op = .eq, .value_json = "\"open\"" }});
    try std.testing.expectError(error.InvalidSqlCatalog, validateUniquePredicatesForColumns(&columns, &.{.{ .field = "metadata", .op = .is_not_null }}));

    try validateUniqueConstraintForColumns(&columns, &periods, .{
        .name = "status_key",
        .columns = &.{"status"},
        .expressions = &.{.{ .op = .expression, .expression = lower_status }},
        .include_columns = &.{"amount"},
        .without_overlaps_period = "valid_at",
    });
    try std.testing.expectError(error.InvalidSqlCatalog, validateUniqueConstraintForColumns(&columns, &periods, .{
        .name = "metadata_key",
        .columns = &.{"metadata"},
    }));
    try std.testing.expectError(error.InvalidSqlCatalog, validateUniqueConstraintForColumns(&columns, &periods, .{
        .name = "empty_key",
    }));
    try validateUniqueConstraintCatalog(&columns, &periods, &.{.{ .name = "status_key", .columns = &.{"status"} }});
    try std.testing.expectError(error.InvalidSqlCatalog, validateUniqueConstraintCatalog(&columns, &periods, &.{
        .{ .name = "dup_key", .columns = &.{"status"} },
        .{ .name = "dup_key", .columns = &.{"amount"} },
    }));

    try validateForeignKeyCatalog(&columns, &periods, &.{.{
        .name = "status_parent_fkey",
        .parent_table = "parent_statuses",
        .child_columns = &.{"status"},
        .parent_columns = &.{"status"},
    }});
    try std.testing.expectError(error.InvalidSqlCatalog, validateForeignKeyCatalog(&columns, &periods, &.{
        .{ .name = "dup_fkey", .parent_table = "parent_statuses", .child_columns = &.{"status"}, .parent_columns = &.{"status"} },
        .{ .name = "dup_fkey", .parent_table = "parent_statuses", .child_columns = &.{"amount"}, .parent_columns = &.{"amount"} },
    }));
}
