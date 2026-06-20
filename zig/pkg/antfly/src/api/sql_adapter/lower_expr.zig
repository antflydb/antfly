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
const plan_mod = @import("plan.zig");
const parser = @import("parser.zig");
const runtime_schema = @import("../../storage/schema.zig");
const token_mod = @import("token.zig");

pub const Token = token_mod.Token;
pub const max_scalar_or_expanded_branches: usize = 32;

const cloneExpressionConditionAlloc = plan_mod.cloneExpressionConditionAlloc;
const cloneExpressionConditionsAlloc = plan_mod.cloneExpressionConditionsAlloc;
const cloneExpressionConditionsConcatAlloc = plan_mod.cloneExpressionConditionsConcatAlloc;
const cloneInPredicatesAlloc = plan_mod.cloneInPredicatesAlloc;
const cloneQueryRelationalChecksAlloc = plan_mod.cloneQueryRelationalChecksAlloc;
const freeAccessPredicateGroup = plan_mod.freeAccessPredicateGroup;
const freeExpression = plan_mod.freeExpression;
const freeExpressionCondition = plan_mod.freeExpressionCondition;
const freeExpressionConditions = plan_mod.freeExpressionConditions;
const freeExpressionPredicateGroup = plan_mod.freeExpressionPredicateGroup;
const freeExpressionPredicateGroups = plan_mod.freeExpressionPredicateGroups;
const freeInPredicates = plan_mod.freeInPredicates;
const freePredicateGroup = plan_mod.freePredicateGroup;
const freeRelationalChecks = plan_mod.freeRelationalChecks;

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

pub fn sqlRowClaimForClause(clause: ast.SqlRowClaimClause) db_mod.types.RowClaimRequest {
    return .{
        .mode = clause.mode,
        .wait_policy = clause.wait_policy,
        .skip_locked = clause.wait_policy == .skip_locked,
    };
}

pub fn setSqlRowClaimClause(claim: *db_mod.types.RowClaimRequest, clause: ast.SqlRowClaimClause) void {
    claim.mode = clause.mode;
    claim.wait_policy = clause.wait_policy;
    claim.skip_locked = clause.wait_policy == .skip_locked;
}

pub fn sqlRowClaimModeName(mode: db_mod.types.RowClaimMode) []const u8 {
    return switch (mode) {
        .for_update => "for_update",
        .for_no_key_update => "for_no_key_update",
        .for_share => "for_share",
        .for_key_share => "for_key_share",
    };
}

pub fn sqlRowClaimWaitPolicyName(wait_policy: db_mod.types.RowClaimWaitPolicy) []const u8 {
    return switch (wait_policy) {
        .wait => "wait",
        .nowait => "nowait",
        .skip_locked => "skip_locked",
    };
}

pub fn sqlRowClaimFingerprintName(claim: db_mod.types.RowClaimRequest) []const u8 {
    return switch (claim.mode) {
        .for_update => switch (claim.effectiveWaitPolicy()) {
            .wait => "locked",
            .nowait => "nowait",
            .skip_locked => "skip_locked",
        },
        .for_no_key_update => switch (claim.effectiveWaitPolicy()) {
            .wait => "no_key_update",
            .nowait => "no_key_update_nowait",
            .skip_locked => "no_key_update_skip_locked",
        },
        .for_share => switch (claim.effectiveWaitPolicy()) {
            .wait => "share",
            .nowait => "share_nowait",
            .skip_locked => "share_skip_locked",
        },
        .for_key_share => switch (claim.effectiveWaitPolicy()) {
            .wait => "key_share",
            .nowait => "key_share_nowait",
            .skip_locked => "key_share_skip_locked",
        },
    };
}

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

pub fn isCaseFoldExpressionOp(op: runtime_schema.UniqueExpressionOp) bool {
    return switch (op) {
        .lower, .upper, .md5 => true,
        .expression => false,
    };
}

pub fn relationalGeneratedOpForUniqueExpressionOp(op: runtime_schema.UniqueExpressionOp) runtime_schema.RelationalGeneratedOp {
    return switch (op) {
        .lower => .lower,
        .upper => .upper,
        .md5 => .md5,
        .expression => unreachable,
    };
}

pub fn uniqueExpressionOpToken(op: runtime_schema.UniqueExpressionOp) []const u8 {
    return switch (op) {
        .lower => "lower",
        .upper => "upper",
        .md5 => "md5",
        .expression => "expression",
    };
}

pub fn uniquePredicateOpToken(op: runtime_schema.UniquePredicateOp) []const u8 {
    return switch (op) {
        .is_null => "is_null",
        .is_not_null => "is_not_null",
        .eq => "eq",
        .ne => "ne",
    };
}

pub fn uniquePredicateAsRelationalCheckOp(op: runtime_schema.UniquePredicateOp) runtime_schema.RelationalCheckOp {
    return switch (op) {
        .is_null => .is_null,
        .is_not_null => .is_not_null,
        .eq => .eq,
        .ne => .ne,
    };
}

pub fn relationalCheckOpFromUniquePredicateToken(token: []const u8) ?runtime_schema.RelationalCheckOp {
    if (std.mem.eql(u8, token, "is_null")) return .is_null;
    if (std.mem.eql(u8, token, "is_not_null")) return .is_not_null;
    if (std.mem.eql(u8, token, "eq")) return .eq;
    if (std.mem.eql(u8, token, "ne")) return .ne;
    return null;
}

pub fn tokenKindIsJsonExtractOperator(kind: token_mod.TokenKind) bool {
    return kind == .arrow_json or kind == .arrow_text or kind == .path_arrow_json or kind == .path_arrow_text;
}

pub fn tokenKindIsJsonExtractTextOperator(kind: token_mod.TokenKind) bool {
    return kind == .arrow_text or kind == .path_arrow_text;
}

pub fn tokenKindIsJsonExtractPathOperator(kind: token_mod.TokenKind) bool {
    return kind == .path_arrow_json or kind == .path_arrow_text;
}

pub fn generatedUnaryTextColumnForField(
    schema: runtime_schema.TableSchema,
    op: runtime_schema.RelationalGeneratedOp,
    field: []const u8,
) ?runtime_schema.RelationalColumn {
    switch (op) {
        .lower, .upper, .md5 => {},
        .concat, .concat_ws, .expression => return null,
    }
    for (schema.relational_columns) |column| {
        const generated = column.generated orelse continue;
        if (generated.op != op) continue;
        const generated_field = generated.field orelse continue;
        if (std.mem.eql(u8, generated_field, field)) return column;
    }
    return null;
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

pub fn aggregateOpForName(name: []const u8) ?db_mod.types.RelationalRowsAggregateOp {
    if (std.ascii.eqlIgnoreCase(name, "count")) return .count;
    if (std.ascii.eqlIgnoreCase(name, "sum")) return .sum;
    if (std.ascii.eqlIgnoreCase(name, "min")) return .min;
    if (std.ascii.eqlIgnoreCase(name, "max")) return .max;
    if (std.ascii.eqlIgnoreCase(name, "avg")) return .avg;
    if (std.ascii.eqlIgnoreCase(name, "percentile_cont")) return .percentile_cont;
    if (std.ascii.eqlIgnoreCase(name, "percentile_disc")) return .percentile_disc;
    if (std.ascii.eqlIgnoreCase(name, "mode")) return .mode;
    if (std.ascii.eqlIgnoreCase(name, "array_agg")) return .array_agg;
    if (std.ascii.eqlIgnoreCase(name, "string_agg")) return .string_agg;
    if (std.ascii.eqlIgnoreCase(name, "bool_or")) return .bool_or;
    if (std.ascii.eqlIgnoreCase(name, "bool_and")) return .bool_and;
    return null;
}

pub fn aggregateOpName(op: db_mod.types.RelationalRowsAggregateOp) []const u8 {
    return switch (op) {
        .count => "count",
        .sum => "sum",
        .min => "min",
        .max => "max",
        .avg => "avg",
        .percentile_cont => "percentile_cont",
        .percentile_disc => "percentile_disc",
        .mode => "mode",
        .array_agg => "array_agg",
        .string_agg => "string_agg",
        .bool_or => "bool_or",
        .bool_and => "bool_and",
    };
}

pub fn isSqlPercentileAggregateOp(op: db_mod.types.RelationalRowsAggregateOp) bool {
    return op == .percentile_cont or op == .percentile_disc;
}

pub fn sqlJsonNumberAsF64(value: std.json.Value) ?f64 {
    return switch (value) {
        .integer => |number| @floatFromInt(number),
        .float => |number| number,
        .number_string => |number| std.fmt.parseFloat(f64, number) catch null,
        else => null,
    };
}

pub fn validateAggregateGroupBy(
    group_fields: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    group_by: []const []const u8,
    parsed_group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
) !void {
    if (!stringSlicesEqual(group_fields, group_by)) return error.UnsupportedSqlShape;
    if (group_expressions.len != parsed_group_expressions.len) return error.UnsupportedSqlShape;
    for (group_expressions, parsed_group_expressions) |selected, parsed| {
        if (!relationalRowsExpressionEqual(selected.expression, parsed.expression)) return error.UnsupportedSqlShape;
    }
}

pub fn aggregateOutputFieldIsUnique(
    group_fields: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
    field: []const u8,
) bool {
    var matches: usize = 0;
    for (group_fields) |group_field| {
        if (std.mem.eql(u8, group_field, field)) matches += 1;
    }
    for (group_expressions) |projection| {
        if (std.mem.eql(u8, projection.output, field)) matches += 1;
    }
    for (aggregations) |aggregation| {
        if (std.mem.eql(u8, aggregation.name, field)) matches += 1;
    }
    return matches == 1;
}

pub fn aggregateOutputColumnExists(columns: []const runtime_schema.RelationalColumn, name: []const u8) bool {
    for (columns) |column| {
        if (std.mem.eql(u8, column.name, name)) return true;
    }
    return false;
}

pub fn expressionOrderCount(order_by: []const db_mod.types.RelationalRowsQueryOrder) usize {
    var count: usize = 0;
    for (order_by) |order| {
        if (order.expression != null) count += 1;
    }
    return count;
}

pub fn aggregateFilterExpressionCount(aggregations: []const db_mod.types.RelationalRowsAggregateSpec) usize {
    var count: usize = 0;
    for (aggregations) |aggregation| {
        count += aggregation.filter_expressions.len;
    }
    return count;
}

pub fn aggregateFilterExpressionArrayCount(aggregations: []const db_mod.types.RelationalRowsAggregateSpec) usize {
    var count: usize = 0;
    for (aggregations) |aggregation| {
        count += aggregation.filter_expression_array_contains.len;
    }
    return count;
}

pub fn aggregateFilterJsonAccessCount(aggregations: []const db_mod.types.RelationalRowsAggregateSpec) usize {
    var count: usize = 0;
    for (aggregations) |aggregation| {
        count += aggregation.filter_json_contains.len +
            aggregation.filter_json_path_eq.len +
            aggregation.filter_json_path_exists.len;
    }
    return count;
}

pub fn aggregateFilterStructuredAccessCount(aggregations: []const db_mod.types.RelationalRowsAggregateSpec) usize {
    var count: usize = 0;
    for (aggregations) |aggregation| {
        count += aggregation.filter_array_any.len +
            aggregation.filter_array_contains.len +
            aggregation.filter_array_eq.len +
            aggregation.filter_in_predicates.len +
            aggregation.filter_text_patterns.len;
    }
    return count;
}

pub fn aggregateFilterGroupCount(aggregations: []const db_mod.types.RelationalRowsAggregateSpec) usize {
    var count: usize = 0;
    for (aggregations) |aggregation| {
        count += aggregation.filter_any.len + aggregation.filter_not.len;
    }
    return count;
}

pub fn aggregateInputExpressionCount(aggregations: []const db_mod.types.RelationalRowsAggregateSpec) usize {
    var count: usize = 0;
    for (aggregations) |aggregation| {
        if (aggregation.expression != null) count += 1;
    }
    return count;
}

pub fn aggregateDescendingPercentileCount(aggregations: []const db_mod.types.RelationalRowsAggregateSpec) usize {
    var count: usize = 0;
    for (aggregations) |aggregation| {
        if (isSqlPercentileAggregateOp(aggregation.op) and aggregation.percentile_order == .desc) count += 1;
    }
    return count;
}

pub fn aggregatePercentileArrayCount(aggregations: []const db_mod.types.RelationalRowsAggregateSpec) usize {
    var count: usize = 0;
    for (aggregations) |aggregation| {
        if (isSqlPercentileAggregateOp(aggregation.op) and aggregation.percentiles.len > 0) count += 1;
    }
    return count;
}

pub fn aggregateModeCount(aggregations: []const db_mod.types.RelationalRowsAggregateSpec) usize {
    var count: usize = 0;
    for (aggregations) |aggregation| {
        if (aggregation.op == .mode) count += 1;
    }
    return count;
}

pub fn windowValueExpressionCount(windows: []const db_mod.types.RelationalRowsWindowSpec) usize {
    var count: usize = 0;
    for (windows) |window| {
        if (window.value_expression != null) count += 1;
    }
    return count;
}

pub fn windowDefaultCount(windows: []const db_mod.types.RelationalRowsWindowSpec) usize {
    var count: usize = 0;
    for (windows) |window| {
        if (window.default_json.len > 0) count += 1;
    }
    return count;
}

pub fn windowFilterPredicateCount(windows: []const db_mod.types.RelationalRowsWindowSpec) usize {
    var count: usize = 0;
    for (windows) |window| {
        count += window.filter_predicates.len;
    }
    return count;
}

pub fn windowFilterExpressionCount(windows: []const db_mod.types.RelationalRowsWindowSpec) usize {
    var count: usize = 0;
    for (windows) |window| {
        count += window.filter_expressions.len;
    }
    return count;
}

pub fn windowFilterAccessCount(windows: []const db_mod.types.RelationalRowsWindowSpec) usize {
    var count: usize = 0;
    for (windows) |window| {
        count += window.filter_array_any.len +
            window.filter_array_contains.len +
            window.filter_array_eq.len +
            window.filter_in_predicates.len +
            window.filter_json_contains.len +
            window.filter_json_path_eq.len +
            window.filter_json_path_exists.len +
            window.filter_text_patterns.len +
            window.filter_expression_array_contains.len;
    }
    return count;
}

pub fn windowFilterGroupCount(windows: []const db_mod.types.RelationalRowsWindowSpec) usize {
    var count: usize = 0;
    for (windows) |window| {
        count += window.filter_any.len + window.filter_not.len;
    }
    return count;
}

pub fn windowFrameSignature(windows: []const db_mod.types.RelationalRowsWindowSpec) u64 {
    var signature: u64 = 0;
    for (windows) |window| {
        if (window.frame) |frame| {
            signature = signature *% 131 +% 17;
            signature = signature *% 131 +% windowFrameUnitCode(frame.unit);
            signature = signature *% 131 +% windowFrameBoundCode(frame.start);
            signature = signature *% 131 +% @as(u64, @intCast(frame.start_offset));
            signature = signature *% 131 +% windowFrameBoundCode(frame.end);
            signature = signature *% 131 +% @as(u64, @intCast(frame.end_offset));
        } else {
            signature = signature *% 131;
        }
    }
    return signature;
}

fn windowFrameUnitCode(unit: db_mod.types.RelationalRowsWindowFrameUnit) u64 {
    return switch (unit) {
        .rows => 1,
        .range => 2,
    };
}

fn windowFrameBoundCode(bound: db_mod.types.RelationalRowsWindowFrameBound) u64 {
    return switch (bound) {
        .unbounded_preceding => 1,
        .offset_preceding => 2,
        .current_row => 3,
        .offset_following => 4,
        .unbounded_following => 5,
    };
}

pub fn joinSideForQualifier(
    qualifier: []const u8,
    left_alias: []const u8,
    right_alias: []const u8,
) !db_mod.types.RelationalRowsJoinProjectionSide {
    if (std.mem.eql(u8, qualifier, left_alias)) return .left;
    if (std.mem.eql(u8, qualifier, right_alias)) return .right;
    return error.UnsupportedSqlShape;
}

pub fn joinProjectionOutputIsUnique(select: []const db_mod.types.RelationalRowsJoinProjection, field: []const u8) bool {
    var matches: usize = 0;
    for (select) |projection| {
        if (std.mem.eql(u8, projection.output, field)) matches += 1;
    }
    return matches == 1;
}

pub fn identifierContainsQualifier(identifier: []const u8) bool {
    const dot = std.mem.indexOfScalar(u8, identifier, '.') orelse return false;
    return dot > 0 and dot + 1 < identifier.len;
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

pub fn writeRowExpressionJson(writer: *std.Io.Writer, expression: db_mod.types.RelationalRowsExpression) !void {
    switch (expression.kind) {
        .field => {
            try writer.print("{{\"field\":{f}", .{std.json.fmt(expression.field, .{})});
            if (expression.field_source != .row) {
                try writer.print(",\"source\":{f}", .{std.json.fmt(rowExpressionFieldSourceName(expression.field_source), .{})});
            }
            try writer.writeByte('}');
        },
        .value => {
            try writer.writeAll("{\"value\":");
            try writer.writeAll(expression.value_json);
            try writer.writeByte('}');
        },
        .now => {
            try writer.writeAll("{\"op\":\"now\",\"args\":[]}");
        },
        .case => {
            try writer.writeAll("{\"op\":\"case\",\"cases\":[");
            for (expression.case_branches, 0..) |branch, i| {
                if (i != 0) try writer.writeByte(',');
                try writer.writeAll("{\"when\":{\"lhs\":");
                try writeRowExpressionJson(writer, branch.when.lhs);
                try writer.print(",\"op\":{f}", .{std.json.fmt(rowExpressionConditionOpName(branch.when.op), .{})});
                if (branch.when.rhs.len == 1) {
                    try writer.writeAll(",\"rhs\":");
                    try writeRowExpressionJson(writer, branch.when.rhs[0]);
                }
                try writer.writeAll("},\"then\":");
                try writeRowExpressionJson(writer, branch.then);
                try writer.writeByte('}');
            }
            try writer.writeAll("],\"else\":");
            if (expression.case_else.len != 1) return error.UnsupportedSqlShape;
            try writeRowExpressionJson(writer, expression.case_else[0]);
            try writer.writeByte('}');
        },
        .cast => {
            if (expression.operands.len != 1) return error.UnsupportedSqlShape;
            const cast_type = expression.cast_type orelse return error.UnsupportedSqlShape;
            try writer.print("{{\"op\":\"cast\",\"to\":{f},\"args\":[", .{std.json.fmt(rowExpressionCastTypeName(cast_type), .{})});
            try writeRowExpressionJson(writer, expression.operands[0]);
            try writer.writeAll("]}");
        },
        .json_extract => {
            if (expression.operands.len != 1 or expression.json_path.len == 0) return error.UnsupportedSqlShape;
            try writer.writeAll("{\"op\":\"json_extract\",\"args\":[");
            try writeRowExpressionJson(writer, expression.operands[0]);
            try writer.print("],\"path\":{f}", .{std.json.fmt(expression.json_path, .{})});
            if (expression.json_as_text) try writer.writeAll(",\"as_text\":true");
            try writer.writeByte('}');
        },
        else => {
            try writer.print("{{\"op\":{f},\"args\":[", .{std.json.fmt(rowExpressionOpName(expression.kind), .{})});
            for (expression.operands, 0..) |operand, i| {
                if (i != 0) try writer.writeByte(',');
                try writeRowExpressionJson(writer, operand);
            }
            try writer.writeAll("]}");
        },
    }
}

pub fn writeRowExpressionConditionJson(writer: *std.Io.Writer, condition: db_mod.types.RelationalRowsExpressionCondition) !void {
    try writer.writeAll("{\"lhs\":");
    try writeRowExpressionJson(writer, condition.lhs);
    try writer.print(",\"op\":{f}", .{std.json.fmt(rowExpressionConditionOpName(condition.op), .{})});
    if (condition.rhs.len == 1) {
        try writer.writeAll(",\"rhs\":");
        try writeRowExpressionJson(writer, condition.rhs[0]);
    }
    try writer.writeByte('}');
}

pub fn writeRowExpressionPredicateGroupsJson(
    writer: *std.Io.Writer,
    field_name: []const u8,
    groups: []const db_mod.types.RelationalRowsExpressionPredicateGroup,
) !void {
    try writer.print("{f}:[", .{std.json.fmt(field_name, .{})});
    for (groups, 0..) |group, group_i| {
        if (group_i != 0) try writer.writeByte(',');
        try writer.writeAll("{\"all\":[");
        for (group.conditions, 0..) |condition, condition_i| {
            if (condition_i != 0) try writer.writeByte(',');
            try writeRowExpressionConditionJson(writer, condition);
        }
        try writer.writeAll("]}");
    }
    try writer.writeByte(']');
}

pub fn rowExpressionOpName(kind: db_mod.types.RelationalRowsExpressionKind) []const u8 {
    return switch (kind) {
        .field, .value => unreachable,
        .now => "now",
        .uuid_v4 => "uuid_v4",
        .coalesce => "coalesce",
        .lower => "lower",
        .upper => "upper",
        .initcap => "initcap",
        .trim => "trim",
        .ltrim => "ltrim",
        .rtrim => "rtrim",
        .replace => "replace",
        .regexp_replace => "regexp_replace",
        .regexp_substr => "regexp_substr",
        .regexp_count => "regexp_count",
        .regexp_instr => "regexp_instr",
        .translate => "translate",
        .substring => "substring",
        .overlay => "overlay",
        .split_part => "split_part",
        .strpos => "strpos",
        .ascii => "ascii",
        .left => "left",
        .right => "right",
        .lpad => "lpad",
        .rpad => "rpad",
        .repeat => "repeat",
        .reverse => "reverse",
        .md5 => "md5",
        .starts_with => "starts_with",
        .ends_with => "ends_with",
        .chr => "chr",
        .like => "like",
        .ilike => "ilike",
        .regexp_match => "regexp_match",
        .bool_and => "and",
        .bool_or => "or",
        .bool_not => "not",
        .date_trunc => "date_trunc",
        .date_bin => "date_bin",
        .date_part => "date_part",
        .concat => "concat",
        .concat_ws => "concat_ws",
        .length => "length",
        .octet_length => "octet_length",
        .bit_length => "bit_length",
        .nullif => "nullif",
        .greatest => "greatest",
        .least => "least",
        .abs => "abs",
        .round => "round",
        .trunc => "trunc",
        .floor => "floor",
        .ceil => "ceil",
        .sqrt => "sqrt",
        .sign => "sign",
        .power => "power",
        .add => "add",
        .sub => "sub",
        .mul => "mul",
        .div => "div",
        .mod => "mod",
        .case => unreachable,
        .cast => unreachable,
        .json_extract => unreachable,
        .json_path_exists => "json_path_exists",
        .json_typeof => "json_typeof",
        .json_array_length => "json_array_length",
        .json_build_object => "jsonb_build_object",
        .to_jsonb => "to_jsonb",
        .array_length => "array_length",
        .array_position => "array_position",
        .array_positions => "array_positions",
        .array_append => "array_append",
        .array_prepend => "array_prepend",
        .array_cat => "array_cat",
        .array_remove => "array_remove",
        .array_replace => "array_replace",
        .array_to_string => "array_to_string",
        .string_to_array => "string_to_array",
        .interval_ns => "interval_ns",
        .interval_months => "interval_months",
    };
}

pub fn rowExpressionDefaultOutputName(kind: db_mod.types.RelationalRowsExpressionKind) []const u8 {
    return switch (kind) {
        .field => "field",
        .value => "value",
        .case => "case",
        .cast => "cast",
        .json_extract => "json_extract",
        .json_typeof => "json_typeof",
        .json_array_length => "json_array_length",
        .json_build_object => "jsonb_build_object",
        .to_jsonb => "to_jsonb",
        else => rowExpressionOpName(kind),
    };
}

pub fn rowExpressionFieldSourceName(source: db_mod.types.RelationalRowsExpressionFieldSource) []const u8 {
    return switch (source) {
        .row => "row",
        .existing => "existing",
        .proposed => "proposed",
        .source => "source",
    };
}

pub fn rowExpressionCastTypeName(cast_type: db_mod.types.RelationalRowsExpressionCastType) []const u8 {
    return switch (cast_type) {
        .text => "text",
        .numeric => "numeric",
        .bool => "bool",
        .datetime => "datetime",
    };
}

pub fn rowExpressionConditionOpName(op: runtime_schema.RelationalCheckOp) []const u8 {
    return switch (op) {
        .eq => "eq",
        .ne => "ne",
        .gt => "gt",
        .gte => "gte",
        .lt => "lt",
        .lte => "lte",
        .is_null => "is_null",
        .is_not_null => "is_not_null",
        .is_distinct => "is_distinct",
        .is_not_distinct => "is_not_distinct",
    };
}

pub fn writeRelationalCheckAtomsJson(
    writer: *std.Io.Writer,
    wrote_atom: *bool,
    predicates: []const runtime_schema.RelationalCheck,
) !void {
    for (predicates) |predicate| {
        if (wrote_atom.*) try writer.writeByte(',');
        try writeRelationalCheckAtomJson(writer, predicate);
        wrote_atom.* = true;
    }
}

pub fn writeRelationalCheckAtomJson(writer: *std.Io.Writer, predicate: runtime_schema.RelationalCheck) !void {
    try writer.print("{{\"field\":{f},\"op\":{f}", .{
        std.json.fmt(predicate.field, .{}),
        std.json.fmt(ddl_plan.relationalCheckOpToken(predicate.op), .{}),
    });
    if (predicate.value_json) |value_json| {
        try writer.writeAll(",\"value\":");
        try writer.writeAll(value_json);
    }
    try writer.writeByte('}');
}

pub fn writeInPredicateAtomsJson(
    writer: *std.Io.Writer,
    wrote_atom: *bool,
    predicates: []const db_mod.types.RelationalRowsInPredicate,
) !void {
    for (predicates) |predicate| {
        if (wrote_atom.*) try writer.writeByte(',');
        try writer.print("{{\"field\":{f},\"op\":{f},\"value\":", .{
            std.json.fmt(predicate.field, .{}),
            std.json.fmt(if (predicate.negated) "not_in" else "in", .{}),
        });
        try writer.writeAll(predicate.values_json);
        try writer.writeByte('}');
        wrote_atom.* = true;
    }
}

pub fn writeTextPatternPredicateAtomsJson(
    writer: *std.Io.Writer,
    wrote_atom: *bool,
    predicates: []const db_mod.types.RelationalRowsTextPatternPredicate,
) !void {
    for (predicates) |predicate| {
        if (wrote_atom.*) try writer.writeByte(',');
        try writer.print("{{\"field\":{f},\"op\":\"text_pattern\",\"pattern\":{f}", .{
            std.json.fmt(predicate.field, .{}),
            std.json.fmt(predicate.pattern, .{}),
        });
        if (predicate.case_insensitive) try writer.writeAll(",\"case_insensitive\":true");
        if (predicate.negated) try writer.writeAll(",\"negated\":true");
        try writer.writeByte('}');
        wrote_atom.* = true;
    }
}

pub fn writeStructuredValuePredicateAtomsJson(
    writer: *std.Io.Writer,
    wrote_atom: *bool,
    op_name: []const u8,
    predicates: anytype,
) !void {
    for (predicates) |predicate| {
        if (wrote_atom.*) try writer.writeByte(',');
        try writer.print("{{\"field\":{f},\"op\":{f},\"value\":", .{
            std.json.fmt(predicate.field, .{}),
            std.json.fmt(op_name, .{}),
        });
        try writer.writeAll(predicate.value_json);
        try writer.writeByte('}');
        wrote_atom.* = true;
    }
}

pub fn writeJsonPathEqPredicateAtomsJson(
    writer: *std.Io.Writer,
    wrote_atom: *bool,
    predicates: []const db_mod.types.RelationalRowsJsonPathEqPredicate,
) !void {
    for (predicates) |predicate| {
        if (wrote_atom.*) try writer.writeByte(',');
        try writer.print("{{\"field\":{f},\"op\":\"json_path_eq\",\"path\":{f},\"value\":", .{
            std.json.fmt(predicate.field, .{}),
            std.json.fmt(predicate.path, .{}),
        });
        try writer.writeAll(predicate.value_json);
        try writer.writeByte('}');
        wrote_atom.* = true;
    }
}

pub fn writeJsonPathExistsPredicateAtomsJson(
    writer: *std.Io.Writer,
    wrote_atom: *bool,
    predicates: []const db_mod.types.RelationalRowsJsonPathExistsPredicate,
) !void {
    for (predicates) |predicate| {
        if (wrote_atom.*) try writer.writeByte(',');
        try writer.print("{{\"field\":{f},\"op\":\"json_path_exists\",\"path\":{f}}}", .{
            std.json.fmt(predicate.field, .{}),
            std.json.fmt(predicate.path, .{}),
        });
        wrote_atom.* = true;
    }
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

pub fn selectorExpressionValueJsonAlloc(
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
        .lower, .upper, .initcap, .md5 => blk: {
            if (expression.operands.len != 1) return error.UnsupportedSqlShape;
            const value_json = try selectorExpressionValueJsonAlloc(alloc, expression.operands[0], values);
            defer alloc.free(value_json);
            var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.UnsupportedSqlShape;
            defer parsed.deinit();
            switch (parsed.value) {
                .null => break :blk try alloc.dupe(u8, "null"),
                .string => |text| {
                    const transformed = switch (expression.kind) {
                        .lower => try std.ascii.allocLowerString(alloc, text),
                        .upper => try std.ascii.allocUpperString(alloc, text),
                        .initcap => try selectorInitcapTextAlloc(alloc, text),
                        .md5 => try selectorMd5HexTextAlloc(alloc, text),
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
                const value_json = try selectorExpressionValueJsonAlloc(alloc, operand, values);
                defer alloc.free(value_json);
                var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.UnsupportedSqlShape;
                defer parsed.deinit();
                if (parsed.value == .null) continue;
                const text = try selectorScalarJsonValueTextAlloc(alloc, parsed.value);
                defer alloc.free(text);
                try joined.appendSlice(alloc, text);
            }
            break :blk try std.json.Stringify.valueAlloc(alloc, std.json.Value{ .string = joined.items }, .{});
        },
        .concat_ws => blk: {
            if (expression.operands.len < 2) return error.UnsupportedSqlShape;
            const separator_json = try selectorExpressionValueJsonAlloc(alloc, expression.operands[0], values);
            defer alloc.free(separator_json);
            var parsed_separator = std.json.parseFromSlice(std.json.Value, alloc, separator_json, .{}) catch return error.UnsupportedSqlShape;
            defer parsed_separator.deinit();
            if (parsed_separator.value == .null) break :blk try alloc.dupe(u8, "null");
            if (parsed_separator.value != .string) return error.UnsupportedSqlShape;

            var joined = std.ArrayListUnmanaged(u8).empty;
            defer joined.deinit(alloc);
            var emitted = false;
            for (expression.operands[1..]) |operand| {
                const value_json = try selectorExpressionValueJsonAlloc(alloc, operand, values);
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

fn selectorScalarJsonValueTextAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]u8 {
    return switch (value) {
        .string => |text| try alloc.dupe(u8, text),
        .integer => |integer| try std.fmt.allocPrint(alloc, "{d}", .{integer}),
        .float => |float| try std.fmt.allocPrint(alloc, "{d}", .{float}),
        .number_string => |text| try alloc.dupe(u8, text),
        .bool => |enabled| try alloc.dupe(u8, if (enabled) "true" else "false"),
        else => error.UnsupportedSqlShape,
    };
}

fn selectorMd5HexTextAlloc(alloc: std.mem.Allocator, text: []const u8) ![]u8 {
    const digest = std.crypto.hash.Md5.hashResult(text);
    const out = try alloc.alloc(u8, 32);
    for (digest, 0..) |byte, i| {
        out[i * 2] = std.fmt.digitToChar(byte >> 4, .lower);
        out[i * 2 + 1] = std.fmt.digitToChar(byte & 0x0f, .lower);
    }
    return out;
}

fn selectorInitcapTextAlloc(alloc: std.mem.Allocator, text: []const u8) ![]u8 {
    const out = try alloc.dupe(u8, text);
    var at_word_start = true;
    for (out) |*byte| {
        if (std.ascii.isAlphanumeric(byte.*)) {
            byte.* = if (at_word_start) std.ascii.toUpper(byte.*) else std.ascii.toLower(byte.*);
            at_word_start = false;
        } else {
            at_word_start = true;
        }
    }
    return out;
}

pub fn selectorJsonValuesEqual(a: std.json.Value, b: std.json.Value) bool {
    if (a == .null or b == .null) return a == .null and b == .null;
    if (a == .bool and b == .bool) return a.bool == b.bool;
    if (a == .string and b == .string) return std.mem.eql(u8, a.string, b.string);
    if (selectorJsonNumber(a)) |left| {
        if (selectorJsonNumber(b)) |right| return left == right;
    }
    return false;
}

pub const SelectorJsonOrder = enum { lt, eq, gt };

pub fn selectorCompareJsonScalars(a: std.json.Value, b: std.json.Value) ?SelectorJsonOrder {
    if (selectorJsonNumber(a)) |left| {
        if (selectorJsonNumber(b)) |right| {
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

fn selectorJsonNumber(value: std.json.Value) ?f64 {
    return switch (value) {
        .integer => |integer| @floatFromInt(integer),
        .float => |float| float,
        .number_string => |text| std.fmt.parseFloat(f64, text) catch null,
        else => null,
    };
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

pub fn findCteByName(ctes: []const db_mod.types.RelationalRowsCte, name: []const u8) ?db_mod.types.RelationalRowsCte {
    for (ctes) |cte| {
        if (std.mem.eql(u8, cte.name, name)) return cte;
    }
    return null;
}

pub fn windowOutputFieldIsUnique(
    fields: []const []const u8,
    windows: []const db_mod.types.RelationalRowsWindowSpec,
    field: []const u8,
) bool {
    var matches: usize = 0;
    for (fields) |candidate| {
        if (std.mem.eql(u8, candidate, field)) matches += 1;
    }
    for (windows) |window| {
        if (std.mem.eql(u8, window.output, field)) matches += 1;
    }
    return matches == 1;
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
        if (expressionConditionReferencesField(condition, field)) return true;
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
    const lhs_json = try selectorExpressionValueJsonAlloc(alloc, condition.lhs, values);
    defer alloc.free(lhs_json);
    var lhs = std.json.parseFromSlice(std.json.Value, alloc, lhs_json, .{}) catch return false;
    defer lhs.deinit();

    return switch (condition.op) {
        .is_null => lhs.value == .null,
        .is_not_null => lhs.value != .null,
        .eq, .ne, .is_distinct, .is_not_distinct => blk: {
            if (condition.rhs.len != 1) return false;
            const rhs_json = try selectorExpressionValueJsonAlloc(alloc, condition.rhs[0], values);
            defer alloc.free(rhs_json);
            var rhs = std.json.parseFromSlice(std.json.Value, alloc, rhs_json, .{}) catch return false;
            defer rhs.deinit();
            const equal = selectorJsonValuesEqual(lhs.value, rhs.value);
            break :blk switch (condition.op) {
                .eq, .is_not_distinct => equal,
                .ne, .is_distinct => !equal,
                else => unreachable,
            };
        },
        .gt, .gte, .lt, .lte => blk: {
            if (condition.rhs.len != 1) return false;
            const rhs_json = try selectorExpressionValueJsonAlloc(alloc, condition.rhs[0], values);
            defer alloc.free(rhs_json);
            var rhs = std.json.parseFromSlice(std.json.Value, alloc, rhs_json, .{}) catch return false;
            defer rhs.deinit();
            const comparison = selectorCompareJsonScalars(lhs.value, rhs.value) orelse return false;
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
        if (!uniqueExpressionsEqual(constraint.expressions, expressions)) continue;
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
        if (!uniqueExpressionsEqual(constraint.expressions, expressions)) continue;

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
        if (relationalRowsExpressionConditionsEqual(constraint.where_expressions, where_expressions)) return constraint;
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
        const op = relationalCheckOpFromUniquePredicateToken(op_value.string) orelse return error.UnsupportedSqlShape;
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
        if (op_value != .string or !std.mem.eql(u8, op_value.string, uniquePredicateOpToken(predicate.op))) return error.UnsupportedSqlShape;
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

pub fn queryHasNoSimpleSetPredicates(query: db_mod.types.RelationalRowsQueryRequest) bool {
    return query.predicates.len == 0 and
        query.or_predicates.len == 0 and
        query.in_predicates.len == 0 and
        query.access_or_predicates.len == 0 and
        query.expression_predicates.len == 0 and
        query.expression_or_predicates.len == 0;
}

pub fn querySupportsSimpleUnionRewrite(query: db_mod.types.RelationalRowsQueryRequest) bool {
    if (!queryHasOnlySimpleUnionPredicateSurface(query)) return false;
    if (query.or_predicates.len > 0 and
        (query.predicates.len > 0 or query.in_predicates.len > 0 or query.expression_predicates.len > 0 or query.expression_or_predicates.len > 0))
    {
        return false;
    }
    if (query.in_predicates.len > 0 and query.or_predicates.len > 0) return false;
    if (query.expression_or_predicates.len != 0) {
        if (query.or_predicates.len != 0) return false;
    }
    return true;
}

pub fn querySupportsSimpleIntersectExceptRewrite(query: db_mod.types.RelationalRowsQueryRequest) bool {
    if (!queryHasOnlySimpleIntersectExceptPredicateSurface(query)) return false;
    if (query.expression_or_predicates.len != 0) {
        if (query.or_predicates.len != 0) return false;
    }
    if (query.or_predicates.len != 0 and
        (query.predicates.len != 0 or query.in_predicates.len != 0 or query.expression_predicates.len != 0))
    {
        return false;
    }
    return true;
}

pub fn simpleSetQueriesProvablyDisjoint(
    alloc: std.mem.Allocator,
    lhs: db_mod.types.RelationalRowsQueryRequest,
    rhs: db_mod.types.RelationalRowsQueryRequest,
) !bool {
    if (lhs.in_predicates.len > 0 or rhs.in_predicates.len > 0) {
        if (!querySupportsSimpleUnionRewrite(lhs) or !querySupportsSimpleUnionRewrite(rhs)) return false;
        if (lhs.expression_predicates.len > 0 or rhs.expression_predicates.len > 0 or
            lhs.expression_or_predicates.len > 0 or rhs.expression_or_predicates.len > 0)
        {
            if (try simpleSetQueriesExpressionBranchesProvablyDisjoint(alloc, lhs, rhs)) return true;
        }
        const lhs_branch_count = simpleAccessSetQueryBranchCount(lhs);
        const rhs_branch_count = simpleAccessSetQueryBranchCount(rhs);
        if (lhs_branch_count == 0 or rhs_branch_count == 0) return false;
        var access_branches_disjoint = true;
        for (0..lhs_branch_count) |left_index| {
            const left = simpleAccessSetQueryBranchAt(lhs, left_index) orelse return false;
            for (0..rhs_branch_count) |right_index| {
                const right = simpleAccessSetQueryBranchAt(rhs, right_index) orelse return false;
                if (!try simpleAccessSetBranchesProvablyDisjoint(alloc, left, right)) {
                    access_branches_disjoint = false;
                    break;
                }
            }
            if (!access_branches_disjoint) break;
        }
        return access_branches_disjoint;
    }

    if (lhs.or_predicates.len > 0 or rhs.or_predicates.len > 0) {
        if (!querySupportsSimpleUnionRewrite(lhs) or !querySupportsSimpleUnionRewrite(rhs)) return false;
        if (lhs.expression_predicates.len > 0 or rhs.expression_predicates.len > 0) return false;
        const lhs_branch_count = simpleScalarSetQueryBranchCount(lhs);
        const rhs_branch_count = simpleScalarSetQueryBranchCount(rhs);
        if (lhs_branch_count == 0 or rhs_branch_count == 0) return false;
        for (0..lhs_branch_count) |left_index| {
            const left = simpleScalarSetQueryBranchAt(lhs, left_index) orelse return false;
            for (0..rhs_branch_count) |right_index| {
                const right = simpleScalarSetQueryBranchAt(rhs, right_index) orelse return false;
                if (!relationalCheckBranchesProvablyDisjoint(left, right)) return false;
            }
        }
        return true;
    }

    if (lhs.expression_or_predicates.len > 0 or rhs.expression_or_predicates.len > 0) {
        if (!querySupportsSimpleUnionRewrite(lhs) or !querySupportsSimpleUnionRewrite(rhs)) return false;
        return try simpleSetQueriesExpressionBranchesProvablyDisjoint(alloc, lhs, rhs);
    }

    for (lhs.predicates) |left| {
        for (rhs.predicates) |right| {
            if (relationalChecksProvablyDisjoint(left, right)) return true;
        }
        for (rhs.expression_predicates) |right| {
            if (relationalCheckAndExpressionConditionProvablyDisjoint(left, right)) return true;
        }
    }
    for (lhs.expression_predicates) |left| {
        for (rhs.predicates) |right| {
            if (relationalCheckAndExpressionConditionProvablyDisjoint(right, left)) return true;
        }
        for (rhs.expression_predicates) |right| {
            if (expressionConditionsProvablyDisjoint(left, right)) return true;
        }
    }
    return false;
}

pub const SimpleAccessSetBranch = struct {
    predicates: []const runtime_schema.RelationalCheck = &.{},
    in_predicates: []const db_mod.types.RelationalRowsInPredicate = &.{},
};

pub fn simpleScalarSetQueryBranchCount(query: db_mod.types.RelationalRowsQueryRequest) usize {
    if (query.or_predicates.len > 0) return query.or_predicates.len;
    if (query.predicates.len > 0) return 1;
    return 0;
}

pub fn simpleScalarSetQueryBranchAt(
    query: db_mod.types.RelationalRowsQueryRequest,
    index: usize,
) ?[]const runtime_schema.RelationalCheck {
    if (query.or_predicates.len > 0) {
        if (index >= query.or_predicates.len) return null;
        return query.or_predicates[index].predicates;
    }
    if (query.predicates.len > 0 and index == 0) return query.predicates;
    return null;
}

pub fn simpleExpressionSetQueryBranchCount(query: db_mod.types.RelationalRowsQueryRequest) usize {
    if (query.expression_or_predicates.len > 0) return query.expression_or_predicates.len;
    if (query.or_predicates.len > 0) return query.or_predicates.len;
    if (query.predicates.len > 0 or query.expression_predicates.len > 0) return 1;
    return 0;
}

pub fn simpleAccessSetQueryBranchCount(query: db_mod.types.RelationalRowsQueryRequest) usize {
    if (query.or_predicates.len > 0) return query.or_predicates.len;
    if (query.predicates.len > 0 or query.in_predicates.len > 0) return 1;
    return 0;
}

pub fn simpleAccessSetQueryBranchAt(
    query: db_mod.types.RelationalRowsQueryRequest,
    index: usize,
) ?SimpleAccessSetBranch {
    if (query.or_predicates.len > 0) {
        if (index >= query.or_predicates.len) return null;
        return .{ .predicates = query.or_predicates[index].predicates };
    }
    if ((query.predicates.len > 0 or query.in_predicates.len > 0) and index == 0) {
        return .{ .predicates = query.predicates, .in_predicates = query.in_predicates };
    }
    return null;
}

fn relationalCheckBranchesProvablyDisjoint(
    lhs: []const runtime_schema.RelationalCheck,
    rhs: []const runtime_schema.RelationalCheck,
) bool {
    if (lhs.len == 0 or rhs.len == 0) return false;
    for (lhs) |left| {
        for (rhs) |right| {
            if (relationalChecksProvablyDisjoint(left, right)) return true;
        }
    }
    return false;
}

fn expressionConditionBranchesProvablyDisjoint(
    lhs: []const db_mod.types.RelationalRowsExpressionCondition,
    rhs: []const db_mod.types.RelationalRowsExpressionCondition,
) bool {
    if (lhs.len == 0 or rhs.len == 0) return false;
    for (lhs) |left| {
        for (rhs) |right| {
            if (expressionConditionsProvablyDisjoint(left, right)) return true;
        }
    }
    return false;
}

fn simpleSetQueriesExpressionBranchesProvablyDisjoint(
    alloc: std.mem.Allocator,
    lhs: db_mod.types.RelationalRowsQueryRequest,
    rhs: db_mod.types.RelationalRowsQueryRequest,
) !bool {
    if (lhs.or_predicates.len > 0 or rhs.or_predicates.len > 0) return false;

    const lhs_groups = try expressionGroupsFromSimpleUnionQueryAlloc(alloc, lhs);
    defer {
        freeExpressionPredicateGroups(alloc, lhs_groups);
        if (lhs_groups.len > 0) alloc.free(lhs_groups);
    }
    const rhs_groups = try expressionGroupsFromSimpleUnionQueryAlloc(alloc, rhs);
    defer {
        freeExpressionPredicateGroups(alloc, rhs_groups);
        if (rhs_groups.len > 0) alloc.free(rhs_groups);
    }

    if (lhs_groups.len == 0 or rhs_groups.len == 0) return false;
    for (lhs_groups) |left| {
        for (rhs_groups) |right| {
            if (!expressionConditionBranchesProvablyDisjoint(left.conditions, right.conditions)) return false;
        }
    }
    return true;
}

const InPredicateContainmentProof = enum {
    contains,
    does_not_contain,
    unknown,
};

fn simpleAccessSetBranchesProvablyDisjoint(
    alloc: std.mem.Allocator,
    lhs: SimpleAccessSetBranch,
    rhs: SimpleAccessSetBranch,
) !bool {
    if (lhs.predicates.len == 0 and lhs.in_predicates.len == 0) return false;
    if (rhs.predicates.len == 0 and rhs.in_predicates.len == 0) return false;

    for (lhs.predicates) |left| {
        for (rhs.predicates) |right| {
            if (relationalChecksProvablyDisjoint(left, right)) return true;
        }
        for (rhs.in_predicates) |right| {
            if (try inPredicateAndRelationalCheckProvablyDisjoint(alloc, right, left)) return true;
        }
    }
    for (lhs.in_predicates) |left| {
        for (rhs.predicates) |right| {
            if (try inPredicateAndRelationalCheckProvablyDisjoint(alloc, left, right)) return true;
        }
        for (rhs.in_predicates) |right| {
            if (try inPredicatesProvablyDisjoint(alloc, left, right)) return true;
        }
    }
    return false;
}

fn inPredicateAndRelationalCheckProvablyDisjoint(
    alloc: std.mem.Allocator,
    in_predicate: db_mod.types.RelationalRowsInPredicate,
    check: runtime_schema.RelationalCheck,
) !bool {
    if (check.expression != null) return false;
    if (!std.mem.eql(u8, in_predicate.field, check.field)) return false;

    switch (check.op) {
        .eq, .is_not_distinct => {
            const value_json = check.value_json orelse return false;
            const containment = try inPredicateValuesContainJsonLiteralProof(alloc, in_predicate, value_json);
            return containment == .does_not_contain;
        },
        .is_null => {
            const containment = try inPredicateValuesContainJsonLiteralProof(alloc, in_predicate, "null");
            return containment == .does_not_contain;
        },
        else => return false,
    }
}

fn inPredicatesProvablyDisjoint(
    alloc: std.mem.Allocator,
    lhs: db_mod.types.RelationalRowsInPredicate,
    rhs: db_mod.types.RelationalRowsInPredicate,
) !bool {
    if (lhs.negated or rhs.negated) return false;
    if (!std.mem.eql(u8, lhs.field, rhs.field)) return false;

    var parsed = std.json.parseFromSlice(std.json.Value, alloc, lhs.values_json, .{}) catch return false;
    defer parsed.deinit();
    if (parsed.value != .array) return false;

    for (parsed.value.array.items) |value| {
        const value_json = (try jsonValueScalarProofLiteralAlloc(alloc, value)) orelse return false;
        defer alloc.free(value_json);
        const containment = try inPredicateValuesContainJsonLiteralProof(alloc, rhs, value_json);
        switch (containment) {
            .contains, .unknown => return false,
            .does_not_contain => {},
        }
    }
    return true;
}

fn inPredicateValuesContainJsonLiteralProof(
    alloc: std.mem.Allocator,
    predicate: db_mod.types.RelationalRowsInPredicate,
    value_json: []const u8,
) !InPredicateContainmentProof {
    if (predicate.negated) return .unknown;
    if (!jsonIsSafeDisjointProofLiteral(value_json) and !jsonIsJsonNumberLiteral(value_json)) return .unknown;

    var parsed = std.json.parseFromSlice(std.json.Value, alloc, predicate.values_json, .{}) catch return .unknown;
    defer parsed.deinit();
    if (parsed.value != .array) return .unknown;

    for (parsed.value.array.items) |value| {
        const item_json = (try jsonValueScalarProofLiteralAlloc(alloc, value)) orelse return .unknown;
        defer alloc.free(item_json);
        if (std.mem.eql(u8, item_json, value_json)) return .contains;
    }
    return .does_not_contain;
}

fn jsonValueScalarProofLiteralAlloc(alloc: std.mem.Allocator, value: std.json.Value) !?[]const u8 {
    const value_json = try std.json.Stringify.valueAlloc(alloc, value, .{});
    if (jsonIsSafeDisjointProofLiteral(value_json) or jsonIsJsonNumberLiteral(value_json)) return value_json;
    alloc.free(value_json);
    return null;
}

pub fn cloneSimpleScalarSetQueryBranchesInto(
    alloc: std.mem.Allocator,
    out: []db_mod.types.RelationalRowsPredicateGroup,
    initialized: *usize,
    query: db_mod.types.RelationalRowsQueryRequest,
) !void {
    const branch_count = simpleScalarSetQueryBranchCount(query);
    if (branch_count == 0 or out.len - initialized.* < branch_count) return error.UnsupportedSqlShape;
    for (0..branch_count) |index| {
        const branch = simpleScalarSetQueryBranchAt(query, index) orelse return error.UnsupportedSqlShape;
        out[initialized.*] = .{ .predicates = try cloneQueryRelationalChecksAlloc(alloc, branch) };
        initialized.* += 1;
    }
}

pub fn cloneSimpleScalarSetQueryBranchesAlloc(
    alloc: std.mem.Allocator,
    query: db_mod.types.RelationalRowsQueryRequest,
) ![]db_mod.types.RelationalRowsPredicateGroup {
    const branch_count = simpleScalarSetQueryBranchCount(query);
    if (branch_count == 0) return error.UnsupportedSqlShape;
    const groups = try alloc.alloc(db_mod.types.RelationalRowsPredicateGroup, branch_count);
    var initialized: usize = 0;
    errdefer {
        for (groups[0..initialized]) |group| freePredicateGroup(alloc, group);
        alloc.free(groups);
    }
    try cloneSimpleScalarSetQueryBranchesInto(alloc, groups, &initialized, query);
    return groups;
}

pub fn cloneSimpleExpressionSetQueryBranchesAlloc(
    alloc: std.mem.Allocator,
    query: db_mod.types.RelationalRowsQueryRequest,
) ![]db_mod.types.RelationalRowsExpressionPredicateGroup {
    const branch_count = simpleExpressionSetQueryBranchCount(query);
    if (branch_count == 0) return error.UnsupportedSqlShape;
    const groups = try alloc.alloc(db_mod.types.RelationalRowsExpressionPredicateGroup, branch_count);
    var initialized: usize = 0;
    errdefer {
        for (groups[0..initialized]) |group| freeExpressionPredicateGroup(alloc, group);
        alloc.free(groups);
    }
    for (0..branch_count) |index| {
        groups[initialized] = .{
            .conditions = try expressionConditionsFromSimpleExpressionSetQueryBranchAlloc(alloc, query, index),
        };
        initialized += 1;
    }
    return groups;
}

pub fn expressionGroupsFromSimpleIntersectQueryAlloc(
    alloc: std.mem.Allocator,
    query: db_mod.types.RelationalRowsQueryRequest,
) ![]db_mod.types.RelationalRowsExpressionPredicateGroup {
    if (queryHasNoSimpleSetPredicates(query)) return &.{};
    if (query.in_predicates.len > 0) return try expressionGroupsFromInSetQueryAlloc(alloc, query);
    return try cloneSimpleExpressionSetQueryBranchesAlloc(alloc, query);
}

pub fn cloneSimpleAccessSetQueryBranchesInto(
    alloc: std.mem.Allocator,
    out: []db_mod.types.RelationalRowsAccessPredicateGroup,
    initialized: *usize,
    query: db_mod.types.RelationalRowsQueryRequest,
) !void {
    const branch_count = simpleAccessSetQueryBranchCount(query);
    if (branch_count == 0 or out.len - initialized.* < branch_count) return error.UnsupportedSqlShape;
    for (0..branch_count) |index| {
        const branch = simpleAccessSetQueryBranchAt(query, index) orelse return error.UnsupportedSqlShape;
        const predicates = try cloneQueryRelationalChecksAlloc(alloc, branch.predicates);
        var predicates_transferred = false;
        errdefer if (!predicates_transferred) {
            freeRelationalChecks(alloc, predicates);
            if (predicates.len > 0) alloc.free(predicates);
        };
        const in_predicates = try cloneInPredicatesAlloc(alloc, branch.in_predicates);
        var in_transferred = false;
        errdefer if (!in_transferred) {
            freeInPredicates(alloc, in_predicates);
            if (in_predicates.len > 0) alloc.free(in_predicates);
        };
        const group = db_mod.types.RelationalRowsAccessPredicateGroup{
            .predicates = predicates,
            .in_predicates = in_predicates,
        };
        var group_transferred = false;
        errdefer if (!group_transferred) freeAccessPredicateGroup(alloc, group);
        out[initialized.*] = group;
        predicates_transferred = true;
        in_transferred = true;
        group_transferred = true;
        initialized.* += 1;
    }
}

const InPredicateExpansion = struct {
    field: []const u8,
    values_json: []const []const u8,
};

fn freeInPredicateExpansions(alloc: std.mem.Allocator, expansions: []const InPredicateExpansion) void {
    for (expansions) |expansion| {
        for (expansion.values_json) |value_json| alloc.free(value_json);
        if (expansion.values_json.len > 0) alloc.free(expansion.values_json);
    }
    if (expansions.len > 0) alloc.free(expansions);
}

fn inPredicateExpansionsAlloc(
    alloc: std.mem.Allocator,
    predicates: []const db_mod.types.RelationalRowsInPredicate,
) ![]InPredicateExpansion {
    if (predicates.len == 0) return error.UnsupportedSqlShape;
    const expansions = try alloc.alloc(InPredicateExpansion, predicates.len);
    var initialized: usize = 0;
    errdefer {
        for (expansions[0..initialized]) |expansion| {
            for (expansion.values_json) |value_json| alloc.free(value_json);
            if (expansion.values_json.len > 0) alloc.free(expansion.values_json);
        }
        alloc.free(expansions);
    }

    for (predicates) |predicate| {
        if (predicate.negated) return error.UnsupportedSqlShape;

        var parsed = std.json.parseFromSlice(std.json.Value, alloc, predicate.values_json, .{}) catch return error.UnsupportedSqlShape;
        defer parsed.deinit();
        if (parsed.value != .array or parsed.value.array.items.len == 0) return error.UnsupportedSqlShape;

        const values = try alloc.alloc([]const u8, parsed.value.array.items.len);
        var values_initialized: usize = 0;
        errdefer {
            for (values[0..values_initialized]) |value_json| alloc.free(value_json);
            alloc.free(values);
        }
        for (parsed.value.array.items) |value| {
            values[values_initialized] = try std.json.Stringify.valueAlloc(alloc, value, .{});
            values_initialized += 1;
        }

        expansions[initialized] = .{
            .field = predicate.field,
            .values_json = values,
        };
        initialized += 1;
    }
    return expansions;
}

fn inPredicateExpansionBranchCount(expansions: []const InPredicateExpansion) !usize {
    var total: usize = 1;
    for (expansions) |expansion| {
        if (expansion.values_json.len == 0) return error.UnsupportedSqlShape;
        if (total > max_scalar_or_expanded_branches / expansion.values_json.len) return error.UnsupportedSqlShape;
        total *= expansion.values_json.len;
    }
    return total;
}

pub fn expressionGroupsFromInSetQueryAlloc(
    alloc: std.mem.Allocator,
    query: db_mod.types.RelationalRowsQueryRequest,
) ![]db_mod.types.RelationalRowsExpressionPredicateGroup {
    if (query.in_predicates.len == 0) return error.UnsupportedSqlShape;

    const expansions = try inPredicateExpansionsAlloc(alloc, query.in_predicates);
    defer freeInPredicateExpansions(alloc, expansions);
    const branch_count = try inPredicateExpansionBranchCount(expansions);
    if (branch_count == 0) return error.UnsupportedSqlShape;

    const base_conditions = try expressionConditionsFromSimpleSetQueryAlloc(alloc, query);
    defer {
        freeExpressionConditions(alloc, base_conditions);
        if (base_conditions.len > 0) alloc.free(base_conditions);
    }

    const groups = try alloc.alloc(db_mod.types.RelationalRowsExpressionPredicateGroup, branch_count);
    var initialized: usize = 0;
    errdefer {
        for (groups[0..initialized]) |group| freeExpressionPredicateGroup(alloc, group);
        alloc.free(groups);
    }

    for (0..branch_count) |branch_index| {
        const conditions = try alloc.alloc(db_mod.types.RelationalRowsExpressionCondition, base_conditions.len + expansions.len);
        var condition_count: usize = 0;
        errdefer {
            for (conditions[0..condition_count]) |condition| freeExpressionCondition(alloc, condition);
            alloc.free(conditions);
        }

        for (base_conditions) |condition| {
            conditions[condition_count] = try cloneExpressionConditionAlloc(alloc, condition);
            condition_count += 1;
        }

        var quotient = branch_index;
        for (expansions) |expansion| {
            const value_index = quotient % expansion.values_json.len;
            quotient /= expansion.values_json.len;
            const predicate: runtime_schema.RelationalCheck = .{
                .name = "",
                .field = expansion.field,
                .op = .eq,
                .value_json = expansion.values_json[value_index],
            };
            conditions[condition_count] = try expressionConditionFromRelationalCheckAlloc(alloc, predicate);
            condition_count += 1;
        }

        groups[initialized] = .{ .conditions = conditions };
        initialized += 1;
    }

    errdefer {
        freeExpressionPredicateGroups(alloc, groups);
        alloc.free(groups);
    }
    if (query.expression_or_predicates.len != 0) {
        if (groups.len > max_scalar_or_expanded_branches / query.expression_or_predicates.len) return error.UnsupportedSqlShape;
        const combined = try alloc.alloc(db_mod.types.RelationalRowsExpressionPredicateGroup, groups.len * query.expression_or_predicates.len);
        var combined_initialized: usize = 0;
        errdefer {
            for (combined[0..combined_initialized]) |group| freeExpressionPredicateGroup(alloc, group);
            alloc.free(combined);
        }
        for (groups) |left| {
            for (query.expression_or_predicates) |right| {
                combined[combined_initialized] = .{ .conditions = try cloneExpressionConditionsConcatAlloc(alloc, left.conditions, right.conditions) };
                combined_initialized += 1;
            }
        }
        freeExpressionPredicateGroups(alloc, groups);
        alloc.free(groups);
        return combined;
    }

    return groups;
}

pub fn expressionGroupsFromSimpleUnionQueryAlloc(
    alloc: std.mem.Allocator,
    query: db_mod.types.RelationalRowsQueryRequest,
) ![]db_mod.types.RelationalRowsExpressionPredicateGroup {
    if (query.in_predicates.len > 0) return try expressionGroupsFromInSetQueryAlloc(alloc, query);
    if (query.expression_or_predicates.len > 0) return try cloneSimpleExpressionSetQueryBranchesAlloc(alloc, query);

    const groups = try alloc.alloc(db_mod.types.RelationalRowsExpressionPredicateGroup, 1);
    var initialized: usize = 0;
    errdefer {
        for (groups[0..initialized]) |group| freeExpressionPredicateGroup(alloc, group);
        alloc.free(groups);
    }
    groups[0] = .{ .conditions = try expressionConditionsFromSimpleSetQueryAlloc(alloc, query) };
    initialized += 1;
    return groups;
}

pub fn expressionConditionFromRelationalCheckAlloc(
    alloc: std.mem.Allocator,
    value: runtime_schema.RelationalCheck,
) !db_mod.types.RelationalRowsExpressionCondition {
    if (value.expression) |expression| return try cloneExpressionConditionAlloc(alloc, expression);

    const lhs: db_mod.types.RelationalRowsExpression = .{
        .kind = .field,
        .field = try alloc.dupe(u8, value.field),
    };
    var lhs_transferred = false;
    errdefer if (!lhs_transferred) freeExpression(alloc, lhs);

    const rhs = if (value.value_json) |json| blk: {
        const out = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
        errdefer alloc.free(out);
        out[0] = .{
            .kind = .value,
            .value_json = try alloc.dupe(u8, json),
        };
        break :blk out;
    } else &.{};

    lhs_transferred = true;
    return .{
        .lhs = lhs,
        .op = value.op,
        .rhs = rhs,
    };
}

pub fn expressionConditionsFromSimpleSetQueryAlloc(
    alloc: std.mem.Allocator,
    query: db_mod.types.RelationalRowsQueryRequest,
) ![]const db_mod.types.RelationalRowsExpressionCondition {
    const len = query.predicates.len + query.expression_predicates.len;
    if (len == 0) return &.{};

    const out = try alloc.alloc(db_mod.types.RelationalRowsExpressionCondition, len);
    var initialized: usize = 0;
    errdefer {
        freeExpressionConditions(alloc, out[0..initialized]);
        alloc.free(out);
    }
    for (query.predicates) |predicate| {
        out[initialized] = try expressionConditionFromRelationalCheckAlloc(alloc, predicate);
        initialized += 1;
    }
    for (query.expression_predicates) |predicate| {
        out[initialized] = try cloneExpressionConditionAlloc(alloc, predicate);
        initialized += 1;
    }
    return out;
}

pub fn expressionConditionsFromRelationalChecksAlloc(
    alloc: std.mem.Allocator,
    checks: []const runtime_schema.RelationalCheck,
) ![]const db_mod.types.RelationalRowsExpressionCondition {
    if (checks.len == 0) return &.{};

    const out = try alloc.alloc(db_mod.types.RelationalRowsExpressionCondition, checks.len);
    var initialized: usize = 0;
    errdefer {
        freeExpressionConditions(alloc, out[0..initialized]);
        alloc.free(out);
    }
    for (checks) |check| {
        out[initialized] = try expressionConditionFromRelationalCheckAlloc(alloc, check);
        initialized += 1;
    }
    return out;
}

pub fn expressionConditionsFromSimpleExpressionSetQueryBranchAlloc(
    alloc: std.mem.Allocator,
    query: db_mod.types.RelationalRowsQueryRequest,
    index: usize,
) ![]const db_mod.types.RelationalRowsExpressionCondition {
    if (query.expression_or_predicates.len > 0) {
        if (index >= query.expression_or_predicates.len) return error.UnsupportedSqlShape;
        const base_conditions = try expressionConditionsFromSimpleSetQueryAlloc(alloc, query);
        defer {
            freeExpressionConditions(alloc, base_conditions);
            if (base_conditions.len > 0) alloc.free(base_conditions);
        }
        return try cloneExpressionConditionsConcatAlloc(alloc, base_conditions, query.expression_or_predicates[index].conditions);
    }
    if (query.or_predicates.len > 0) {
        if (index >= query.or_predicates.len) return error.UnsupportedSqlShape;
        return try expressionConditionsFromRelationalChecksAlloc(alloc, query.or_predicates[index].predicates);
    }
    if ((query.predicates.len > 0 or query.expression_predicates.len > 0) and index == 0) {
        return try expressionConditionsFromSimpleSetQueryAlloc(alloc, query);
    }
    return error.UnsupportedSqlShape;
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
    try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.percentile_cont, aggregateOpForName("PERCENTILE_CONT").?);
    try std.testing.expectEqualStrings("array_agg", aggregateOpName(.array_agg));
    try std.testing.expect(isSqlPercentileAggregateOp(.percentile_disc));
    try std.testing.expect(!isSqlPercentileAggregateOp(.array_agg));
    try std.testing.expectEqual(@as(?f64, 1.5), sqlJsonNumberAsF64(.{ .number_string = "1.5" }));
    try std.testing.expect(sqlJsonNumberAsF64(.{ .string = "1.5" }) == null);

    const group_projection: db_mod.types.RelationalRowsExpressionProjection = .{
        .output = "status_lower",
        .expression = lower_status,
    };
    try validateAggregateGroupBy(&.{"status"}, &.{group_projection}, &.{"status"}, &.{group_projection});
    try std.testing.expectError(error.UnsupportedSqlShape, validateAggregateGroupBy(&.{"status"}, &.{}, &.{"tenant_id"}, &.{}));

    const aggregate_specs = [_]db_mod.types.RelationalRowsAggregateSpec{lhs};
    const duplicate_aggregate_specs = [_]db_mod.types.RelationalRowsAggregateSpec{.{
        .name = "status",
        .op = .count,
    }};
    try std.testing.expect(aggregateOutputFieldIsUnique(&.{"status"}, &.{group_projection}, &aggregate_specs, "statuses"));
    try std.testing.expect(!aggregateOutputFieldIsUnique(&.{"status"}, &.{group_projection}, &duplicate_aggregate_specs, "status"));
    const output_columns = [_]runtime_schema.RelationalColumn{.{ .name = "statuses", .path = "statuses", .field_type = .array }};
    try std.testing.expect(aggregateOutputColumnExists(&output_columns, "statuses"));
    try std.testing.expect(!aggregateOutputColumnExists(&output_columns, "missing"));
    try std.testing.expectEqual(@as(usize, 1), expressionOrderCount(&order_by));
    try std.testing.expectEqual(@as(usize, 0), aggregateFilterGroupCount(&aggregate_specs));
    try std.testing.expectEqual(@as(usize, 0), aggregateFilterExpressionCount(&aggregate_specs));
    try std.testing.expectEqual(@as(usize, 0), aggregateFilterExpressionArrayCount(&aggregate_specs));
    try std.testing.expectEqual(@as(usize, 1), aggregateFilterJsonAccessCount(&aggregate_specs));
    try std.testing.expectEqual(@as(usize, 0), aggregateFilterStructuredAccessCount(&aggregate_specs));
    try std.testing.expectEqual(@as(usize, 0), aggregateInputExpressionCount(&aggregate_specs));

    const percentile_specs = [_]db_mod.types.RelationalRowsAggregateSpec{
        .{ .name = "p", .op = .percentile_cont, .field = "amount", .percentile_order = .desc, .percentiles = &.{ 0.5, 0.9 } },
        .{ .name = "m", .op = .mode, .field = "status" },
    };
    try std.testing.expectEqual(@as(usize, 1), aggregateDescendingPercentileCount(&percentile_specs));
    try std.testing.expectEqual(@as(usize, 1), aggregatePercentileArrayCount(&percentile_specs));
    try std.testing.expectEqual(@as(usize, 1), aggregateModeCount(&percentile_specs));

    const window_specs = [_]db_mod.types.RelationalRowsWindowSpec{.{
        .output = "ranked",
        .function = .lag,
        .value_expression = lower_status,
        .default_json = "\"unknown\"",
        .filter_predicates = &filters,
        .filter_json_contains = &json_filters,
        .filter_expressions = &.{.{
            .lhs = lower_status,
            .op = .eq,
            .rhs = &.{.{ .kind = .value, .value_json = "\"open\"" }},
        }},
        .frame = .{
            .unit = .rows,
            .start = .offset_preceding,
            .start_offset = 1,
            .end = .current_row,
        },
    }};
    try std.testing.expectEqual(@as(usize, 1), windowValueExpressionCount(&window_specs));
    try std.testing.expectEqual(@as(usize, 1), windowDefaultCount(&window_specs));
    try std.testing.expectEqual(@as(usize, 1), windowFilterPredicateCount(&window_specs));
    try std.testing.expectEqual(@as(usize, 1), windowFilterExpressionCount(&window_specs));
    try std.testing.expectEqual(@as(usize, 1), windowFilterAccessCount(&window_specs));
    try std.testing.expectEqual(@as(usize, 0), windowFilterGroupCount(&window_specs));
    try std.testing.expect(windowFrameSignature(&window_specs) != 0);

    try std.testing.expect(identifierContainsQualifier("left.status"));
    try std.testing.expect(!identifierContainsQualifier("status"));
    try std.testing.expectEqual(db_mod.types.RelationalRowsJoinProjectionSide.left, try joinSideForQualifier("l", "l", "r"));
    try std.testing.expectError(error.UnsupportedSqlShape, joinSideForQualifier("x", "l", "r"));

    const join_projections = [_]db_mod.types.RelationalRowsJoinProjection{
        .{ .output = "id", .field = "id", .side = .left },
        .{ .output = "status", .field = "status", .side = .right },
    };
    const duplicate_join_projections = [_]db_mod.types.RelationalRowsJoinProjection{
        .{ .output = "id", .field = "left_id", .side = .left },
        .{ .output = "id", .field = "right_id", .side = .right },
    };
    try std.testing.expect(joinProjectionOutputIsUnique(&join_projections, "status"));
    try std.testing.expect(!joinProjectionOutputIsUnique(&duplicate_join_projections, "id"));
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
    try std.testing.expect(expressionConditionReferencesField(condition, "status"));
    try std.testing.expect(!expressionConditionReferencesField(condition, "tenant_id"));
    try std.testing.expect(uniqueConstraintReferencesAny(unique_constraint, &.{"status"}));
    try std.testing.expect(!uniqueConstraintReferencesAny(unique_constraint, &.{"missing"}));

    const lower_status: runtime_schema.RelationalRowsExpression = .{
        .kind = .lower,
        .operands = &.{status_field},
    };
    const same_unique_expressions = [_]runtime_schema.UniqueExpression{.{ .op = .expression, .expression = lower_status }};
    const equal_unique_expressions = [_]runtime_schema.UniqueExpression{.{ .op = .expression, .expression = lower_status }};
    const different_unique_expressions = [_]runtime_schema.UniqueExpression{.{ .op = .lower, .field = "tenant_id" }};
    try std.testing.expect(uniqueExpressionsEqual(&same_unique_expressions, &equal_unique_expressions));
    try std.testing.expect(!uniqueExpressionsEqual(&same_unique_expressions, &different_unique_expressions));

    const SelectorValue = struct {
        field: []const u8,
        value_json: []const u8,
    };
    const selector_values = [_]SelectorValue{
        .{ .field = "status", .value_json = "\"Active User\"" },
        .{ .field = "tenant_id", .value_json = "\"tenant-a\"" },
        .{ .field = "amount", .value_json = "42" },
    };
    const lower_selector: runtime_schema.RelationalRowsExpression = .{
        .kind = .lower,
        .operands = &.{.{ .kind = .field, .field = "status", .field_source = .row }},
    };
    const lower_json = try selectorExpressionValueJsonAlloc(std.testing.allocator, lower_selector, &selector_values);
    defer std.testing.allocator.free(lower_json);
    try std.testing.expectEqualStrings("\"active user\"", lower_json);

    const concat_selector: runtime_schema.RelationalRowsExpression = .{
        .kind = .concat_ws,
        .operands = &.{
            .{ .kind = .value, .value_json = "\"/\"" },
            .{ .kind = .field, .field = "tenant_id", .field_source = .row },
            .{ .kind = .field, .field = "status", .field_source = .row },
        },
    };
    const concat_json = try selectorExpressionValueJsonAlloc(std.testing.allocator, concat_selector, &selector_values);
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
    try std.testing.expectEqual(SelectorJsonOrder.lt, selectorCompareJsonScalars(left_number.value, right_number.value) orelse return error.TestUnexpectedResult);
    try std.testing.expect(selectorJsonValuesEqual(left_number.value, left_number.value));

    try std.testing.expect(isCaseFoldExpressionOp(.lower));
    try std.testing.expect(isCaseFoldExpressionOp(.upper));
    try std.testing.expect(isCaseFoldExpressionOp(.md5));
    try std.testing.expect(!isCaseFoldExpressionOp(.expression));
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.lower, relationalGeneratedOpForUniqueExpressionOp(.lower));
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.upper, relationalGeneratedOpForUniqueExpressionOp(.upper));
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.md5, relationalGeneratedOpForUniqueExpressionOp(.md5));
    try std.testing.expectEqualStrings("expression", uniqueExpressionOpToken(.expression));
    try std.testing.expectEqualStrings("eq", uniquePredicateOpToken(.eq));
    try std.testing.expectEqualStrings("is_not_null", uniquePredicateOpToken(.is_not_null));
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.ne, uniquePredicateAsRelationalCheckOp(.ne));
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.is_null, uniquePredicateAsRelationalCheckOp(.is_null));
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.eq, relationalCheckOpFromUniquePredicateToken("eq").?);
    try std.testing.expect(relationalCheckOpFromUniquePredicateToken("missing") == null);
    try std.testing.expect(tokenKindIsJsonExtractOperator(.arrow_json));
    try std.testing.expect(tokenKindIsJsonExtractOperator(.path_arrow_text));
    try std.testing.expect(tokenKindIsJsonExtractTextOperator(.arrow_text));
    try std.testing.expect(!tokenKindIsJsonExtractTextOperator(.arrow_json));
    try std.testing.expect(tokenKindIsJsonExtractPathOperator(.path_arrow_json));
    try std.testing.expect(!tokenKindIsJsonExtractPathOperator(.arrow_json));

    const generated_columns = [_]runtime_schema.RelationalColumn{
        .{
            .name = "status_lower",
            .path = "status_lower",
            .field_type = .keyword,
            .generated = .{ .op = .lower, .field = "status" },
        },
    };
    const generated_schema: runtime_schema.TableSchema = .{ .relational_columns = &generated_columns };
    try std.testing.expectEqualStrings("status_lower", generatedUnaryTextColumnForField(generated_schema, .lower, "status").?.name);
    try std.testing.expect(generatedUnaryTextColumnForField(generated_schema, .upper, "status") == null);
    try std.testing.expect(generatedUnaryTextColumnForField(generated_schema, .concat, "status") == null);
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
    const status_predicate: runtime_schema.RelationalCheck = .{
        .name = "status_active",
        .field = "status",
        .op = .eq,
        .value_json = "\"active\"",
    };
    const grouped_predicate: db_mod.types.RelationalRowsPredicateGroup = .{
        .predicates = &.{status_predicate},
    };
    const scalar_or_query: db_mod.types.RelationalRowsQueryRequest = .{
        .or_predicates = &.{grouped_predicate},
    };
    const mixed_or_query: db_mod.types.RelationalRowsQueryRequest = .{
        .predicates = &.{status_predicate},
        .or_predicates = &.{grouped_predicate},
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
    try std.testing.expect(queryHasNoSimpleSetPredicates(simple_query));
    try std.testing.expect(!queryHasNoSimpleSetPredicates(scalar_or_query));
    try std.testing.expect(querySupportsSimpleUnionRewrite(scalar_or_query));
    try std.testing.expect(querySupportsSimpleIntersectExceptRewrite(scalar_or_query));
    try std.testing.expect(!querySupportsSimpleUnionRewrite(mixed_or_query));
    try std.testing.expect(!querySupportsSimpleIntersectExceptRewrite(mixed_or_query));
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
