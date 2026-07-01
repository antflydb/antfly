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
const expr_disjoint = @import("expr_disjoint.zig");
const expr_equal = @import("expr_equal.zig");
const expr_limits = @import("expr_limits.zig");
const expr_type = @import("expr_type.zig");
const plan_mod = @import("plan.zig");
const relational_rows_executor = @import("../storage/db/relational_rows.zig");
const runtime_schema = @import("../storage/schema.zig");

const cloneExpressionConditionAlloc = plan_mod.cloneExpressionConditionAlloc;
const cloneExpressionConditionsAlloc = plan_mod.cloneExpressionConditionsAlloc;
const cloneExpressionConditionsConcatAlloc = plan_mod.cloneExpressionConditionsConcatAlloc;
const cloneExpressionPredicateGroupsAlloc = plan_mod.cloneExpressionPredicateGroupsAlloc;
const cloneInPredicatesAlloc = plan_mod.cloneInPredicatesAlloc;
const cloneInPredicatesConcatAlloc = plan_mod.cloneInPredicatesConcatAlloc;
const cloneQueryRelationalChecksAlloc = plan_mod.cloneQueryRelationalChecksAlloc;
const cloneQueryRelationalChecksConcatAlloc = plan_mod.cloneQueryRelationalChecksConcatAlloc;
const freeAccessPredicateGroup = plan_mod.freeAccessPredicateGroup;
const freeAccessPredicateGroups = plan_mod.freeAccessPredicateGroups;
const freeExpression = plan_mod.freeExpression;
const freeExpressionCondition = plan_mod.freeExpressionCondition;
const freeExpressionConditions = plan_mod.freeExpressionConditions;
const freeExpressionPredicateGroup = plan_mod.freeExpressionPredicateGroup;
const freeExpressionPredicateGroups = plan_mod.freeExpressionPredicateGroups;
const freeInPredicates = plan_mod.freeInPredicates;
const freePredicateGroup = plan_mod.freePredicateGroup;
const freePredicateGroups = plan_mod.freePredicateGroups;
const freeRelationalChecks = plan_mod.freeRelationalChecks;
const max_scalar_or_expanded_branches = expr_limits.max_scalar_or_expanded_branches;

fn optionalStringEqual(lhs: ?[]const u8, rhs: ?[]const u8) bool {
    if (lhs == null or rhs == null) return lhs == null and rhs == null;
    return std.mem.eql(u8, lhs.?, rhs.?);
}

fn stringSlicesEqual(a: []const []const u8, b: []const []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| {
        if (!std.mem.eql(u8, left, right)) return false;
    }
    return true;
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

pub fn sourceQueryUsesExtendedPredicates(query: db_mod.types.RelationalRowsQueryRequest) bool {
    return query.array_any.len > 0 or
        query.in_predicates.len > 0 or
        query.json_contains.len > 0 or
        query.json_path_exists.len > 0 or
        query.array_contains.len > 0 or
        query.array_eq.len > 0 or
        query.access_or_predicates.len > 0 or
        query.access_not_predicates.len > 0 or
        query.expression_predicates.len > 0 or
        query.expression_or_predicates.len > 0 or
        query.expression_not_predicates.len > 0 or
        query.expression_array_contains.len > 0;
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

pub fn validateSimpleSetOperationSelect(
    lhs: db_mod.types.RelationalRowsQueryRequest,
    rhs: db_mod.types.RelationalRowsQueryRequest,
    op: ast.SelectSetOperation,
) !void {
    if (!std.mem.eql(u8, lhs.source_cte, rhs.source_cte)) return error.UnsupportedSqlShape;
    switch (op) {
        .union_distinct, .union_all => {
            if (!querySupportsSimpleUnionRewrite(lhs)) return error.UnsupportedSqlShape;
            if (!querySupportsSimpleUnionRewrite(rhs)) return error.UnsupportedSqlShape;
        },
        .intersect, .except => {
            if (!querySupportsSimpleIntersectExceptRewrite(lhs)) return error.UnsupportedSqlShape;
            if (!querySupportsSimpleIntersectExceptRewrite(rhs)) return error.UnsupportedSqlShape;
        },
    }
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
            if (expr_disjoint.relationalChecksProvablyDisjoint(left, right)) return true;
        }
        for (rhs.expression_predicates) |right| {
            if (expr_disjoint.relationalCheckAndExpressionConditionProvablyDisjoint(left, right)) return true;
        }
    }
    for (lhs.expression_predicates) |left| {
        for (rhs.predicates) |right| {
            if (expr_disjoint.relationalCheckAndExpressionConditionProvablyDisjoint(right, left)) return true;
        }
        for (rhs.expression_predicates) |right| {
            if (expr_disjoint.expressionConditionsProvablyDisjoint(left, right)) return true;
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
            if (expr_disjoint.relationalChecksProvablyDisjoint(left, right)) return true;
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
            if (expr_disjoint.expressionConditionsProvablyDisjoint(left, right)) return true;
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
            if (expr_disjoint.relationalChecksProvablyDisjoint(left, right)) return true;
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
    if (!optionalStringEqual(in_predicate.collation, check.collation)) return false;

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
    if (!optionalStringEqual(lhs.collation, rhs.collation)) return false;

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
    if (!expr_disjoint.jsonIsSafeDisjointProofLiteral(value_json) and !expr_disjoint.jsonIsJsonNumberLiteral(value_json)) return .unknown;
    var wanted = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return .unknown;
    defer wanted.deinit();
    if (!jsonValueIsSafeDisjointProofScalar(wanted.value)) return .unknown;

    var parsed = std.json.parseFromSlice(std.json.Value, alloc, predicate.values_json, .{}) catch return .unknown;
    defer parsed.deinit();
    if (parsed.value != .array) return .unknown;

    for (parsed.value.array.items) |value| {
        if (!jsonValueIsSafeDisjointProofScalar(value)) return .unknown;
        const not_distinct = relational_rows_executor.jsonValuesNotDistinctWithCollation(value, wanted.value, predicate.collation) orelse return .unknown;
        if (not_distinct) return .contains;
    }
    return .does_not_contain;
}

fn jsonValueIsSafeDisjointProofScalar(value: std.json.Value) bool {
    return switch (value) {
        .null, .bool, .string, .integer, .float, .number_string => true,
        else => false,
    };
}

fn jsonValueScalarProofLiteralAlloc(alloc: std.mem.Allocator, value: std.json.Value) !?[]const u8 {
    const value_json = try std.json.Stringify.valueAlloc(alloc, value, .{});
    if (expr_disjoint.jsonIsSafeDisjointProofLiteral(value_json) or expr_disjoint.jsonIsJsonNumberLiteral(value_json)) return value_json;
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

pub fn expressionConditionFromOwnedRelationalCheckAlloc(
    alloc: std.mem.Allocator,
    value: runtime_schema.RelationalCheck,
) !db_mod.types.RelationalRowsExpressionCondition {
    var owned = value;
    var transferred = false;
    errdefer if (!transferred) plan_mod.freeRelationalCheck(alloc, owned);
    std.debug.assert(owned.name.len == 0);
    if (owned.expression != null) return error.UnsupportedSqlShape;

    const rhs = switch (owned.op) {
        .is_null, .is_not_null => &.{},
        else => blk: {
            const value_json = owned.value_json orelse return error.UnsupportedSqlShape;
            const out = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
            errdefer alloc.free(out);
            out[0] = .{
                .kind = .value,
                .value_json = value_json,
            };
            owned.value_json = null;
            break :blk out;
        },
    };

    const lhs: db_mod.types.RelationalRowsExpression = .{
        .kind = .field,
        .field = owned.field,
    };
    owned.field = "";
    transferred = true;
    return .{
        .lhs = lhs,
        .op = owned.op,
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
    if (!expr_equal.expressionProjectionsEqual(lhs.expressions, rhs.expressions)) return false;
    if (lhs_outputs.len != rhs_outputs.len) return false;
    for (lhs_outputs, rhs_outputs) |left, right| {
        if (left.kind != right.kind or left.index != right.index) return false;
    }
    return true;
}

pub fn applySimpleSelectSetOperationAlloc(
    alloc: std.mem.Allocator,
    lhs: *plan_mod.LoweredSelect,
    rhs: plan_mod.LoweredSelect,
    op: ast.SelectSetOperation,
) !void {
    if (!std.mem.eql(u8, lhs.table_name, rhs.table_name)) return error.UnsupportedSqlShape;
    try validateSimpleSetOperationSelect(lhs.query, rhs.query, op);
    if (!simpleSelectProjectionsEqual(lhs.query, rhs.query, lhs.select_outputs, rhs.select_outputs)) return error.UnsupportedSqlShape;

    switch (op) {
        .union_distinct => try applySimpleUnion(alloc, lhs, rhs.query),
        .union_all => try applySimpleUnionAll(alloc, lhs, rhs.query),
        .intersect => try applySimpleIntersect(alloc, lhs, rhs.query),
        .except => try applySimpleExcept(alloc, lhs, rhs.query),
    }
}

fn clearSimpleSetUnionPredicates(alloc: std.mem.Allocator, query: *db_mod.types.RelationalRowsQueryRequest) void {
    freeRelationalChecks(alloc, query.predicates);
    if (query.predicates.len > 0) alloc.free(query.predicates);
    query.predicates = &.{};
    freePredicateGroups(alloc, query.or_predicates);
    if (query.or_predicates.len > 0) alloc.free(query.or_predicates);
    query.or_predicates = &.{};
    freeInPredicates(alloc, query.in_predicates);
    if (query.in_predicates.len > 0) alloc.free(query.in_predicates);
    query.in_predicates = &.{};
    freeAccessPredicateGroups(alloc, query.access_or_predicates);
    if (query.access_or_predicates.len > 0) alloc.free(query.access_or_predicates);
    query.access_or_predicates = &.{};
    freeExpressionConditions(alloc, query.expression_predicates);
    if (query.expression_predicates.len > 0) alloc.free(query.expression_predicates);
    query.expression_predicates = &.{};
    freeExpressionPredicateGroups(alloc, query.expression_or_predicates);
    if (query.expression_or_predicates.len > 0) alloc.free(query.expression_or_predicates);
    query.expression_or_predicates = &.{};
}

fn applySimpleUnion(
    alloc: std.mem.Allocator,
    lhs: *plan_mod.LoweredSelect,
    rhs: db_mod.types.RelationalRowsQueryRequest,
) !void {
    if (queryHasNoSimpleSetPredicates(lhs.query) or queryHasNoSimpleSetPredicates(rhs)) {
        clearSimpleSetUnionPredicates(alloc, &lhs.query);
        return;
    }
    if (lhs.query.expression_or_predicates.len > 0 or rhs.expression_or_predicates.len > 0 or
        ((lhs.query.in_predicates.len > 0 or rhs.in_predicates.len > 0) and
            (lhs.query.expression_predicates.len > 0 or rhs.expression_predicates.len > 0)))
    {
        return try applySimpleExpressionBranchUnion(alloc, lhs, rhs);
    }
    if (lhs.query.in_predicates.len > 0 or rhs.in_predicates.len > 0) {
        return try applySimpleAccessBranchUnion(alloc, lhs, rhs);
    }
    if (lhs.query.or_predicates.len > 0 or rhs.or_predicates.len > 0) {
        return try applySimpleScalarBranchUnion(alloc, lhs, rhs);
    }
    if (lhs.query.expression_predicates.len > 0 or rhs.expression_predicates.len > 0) {
        return try applySimpleExpressionUnion(alloc, lhs, rhs);
    }

    const groups = try alloc.alloc(db_mod.types.RelationalRowsPredicateGroup, 2);
    var initialized: usize = 0;
    errdefer {
        for (groups[0..initialized]) |group| freePredicateGroup(alloc, group);
        alloc.free(groups);
    }
    groups[0] = .{ .predicates = try cloneQueryRelationalChecksAlloc(alloc, lhs.query.predicates) };
    initialized += 1;
    groups[1] = .{ .predicates = try cloneQueryRelationalChecksAlloc(alloc, rhs.predicates) };
    initialized += 1;

    freeRelationalChecks(alloc, lhs.query.predicates);
    if (lhs.query.predicates.len > 0) alloc.free(lhs.query.predicates);
    lhs.query.predicates = &.{};
    freePredicateGroups(alloc, lhs.query.or_predicates);
    if (lhs.query.or_predicates.len > 0) alloc.free(lhs.query.or_predicates);
    lhs.query.or_predicates = groups;
}

fn applySimpleUnionAll(
    alloc: std.mem.Allocator,
    lhs: *plan_mod.LoweredSelect,
    rhs: db_mod.types.RelationalRowsQueryRequest,
) !void {
    if (!try simpleSetQueriesProvablyDisjoint(alloc, lhs.query, rhs)) return error.UnsupportedSqlShape;
    try applySimpleUnion(alloc, lhs, rhs);
}

fn applySimpleScalarBranchUnion(
    alloc: std.mem.Allocator,
    lhs: *plan_mod.LoweredSelect,
    rhs: db_mod.types.RelationalRowsQueryRequest,
) !void {
    const lhs_branch_count = simpleScalarSetQueryBranchCount(lhs.query);
    const rhs_branch_count = simpleScalarSetQueryBranchCount(rhs);
    if (lhs_branch_count == 0 or rhs_branch_count == 0) return error.UnsupportedSqlShape;

    const groups = try alloc.alloc(db_mod.types.RelationalRowsPredicateGroup, lhs_branch_count + rhs_branch_count);
    var initialized: usize = 0;
    errdefer {
        for (groups[0..initialized]) |group| freePredicateGroup(alloc, group);
        alloc.free(groups);
    }
    try cloneSimpleScalarSetQueryBranchesInto(alloc, groups, &initialized, lhs.query);
    try cloneSimpleScalarSetQueryBranchesInto(alloc, groups, &initialized, rhs);

    freeRelationalChecks(alloc, lhs.query.predicates);
    if (lhs.query.predicates.len > 0) alloc.free(lhs.query.predicates);
    lhs.query.predicates = &.{};
    freePredicateGroups(alloc, lhs.query.or_predicates);
    if (lhs.query.or_predicates.len > 0) alloc.free(lhs.query.or_predicates);
    lhs.query.or_predicates = groups;
}

fn applySimpleAccessBranchUnion(
    alloc: std.mem.Allocator,
    lhs: *plan_mod.LoweredSelect,
    rhs: db_mod.types.RelationalRowsQueryRequest,
) !void {
    const lhs_branch_count = simpleAccessSetQueryBranchCount(lhs.query);
    const rhs_branch_count = simpleAccessSetQueryBranchCount(rhs);
    if (lhs_branch_count == 0 or rhs_branch_count == 0) return error.UnsupportedSqlShape;

    const groups = try alloc.alloc(db_mod.types.RelationalRowsAccessPredicateGroup, lhs_branch_count + rhs_branch_count);
    var initialized: usize = 0;
    errdefer {
        for (groups[0..initialized]) |group| freeAccessPredicateGroup(alloc, group);
        alloc.free(groups);
    }
    try cloneSimpleAccessSetQueryBranchesInto(alloc, groups, &initialized, lhs.query);
    try cloneSimpleAccessSetQueryBranchesInto(alloc, groups, &initialized, rhs);

    freeRelationalChecks(alloc, lhs.query.predicates);
    if (lhs.query.predicates.len > 0) alloc.free(lhs.query.predicates);
    lhs.query.predicates = &.{};
    freePredicateGroups(alloc, lhs.query.or_predicates);
    if (lhs.query.or_predicates.len > 0) alloc.free(lhs.query.or_predicates);
    lhs.query.or_predicates = &.{};
    freeInPredicates(alloc, lhs.query.in_predicates);
    if (lhs.query.in_predicates.len > 0) alloc.free(lhs.query.in_predicates);
    lhs.query.in_predicates = &.{};
    freeAccessPredicateGroups(alloc, lhs.query.access_or_predicates);
    if (lhs.query.access_or_predicates.len > 0) alloc.free(lhs.query.access_or_predicates);
    lhs.query.access_or_predicates = groups;
}

fn applySimpleExpressionUnion(
    alloc: std.mem.Allocator,
    lhs: *plan_mod.LoweredSelect,
    rhs: db_mod.types.RelationalRowsQueryRequest,
) !void {
    const groups = try alloc.alloc(db_mod.types.RelationalRowsExpressionPredicateGroup, 2);
    var initialized: usize = 0;
    errdefer {
        for (groups[0..initialized]) |group| freeExpressionPredicateGroup(alloc, group);
        alloc.free(groups);
    }
    groups[0] = .{ .conditions = try expressionConditionsFromSimpleSetQueryAlloc(alloc, lhs.query) };
    initialized += 1;
    groups[1] = .{ .conditions = try expressionConditionsFromSimpleSetQueryAlloc(alloc, rhs) };
    initialized += 1;

    freeRelationalChecks(alloc, lhs.query.predicates);
    if (lhs.query.predicates.len > 0) alloc.free(lhs.query.predicates);
    lhs.query.predicates = &.{};
    freeExpressionConditions(alloc, lhs.query.expression_predicates);
    if (lhs.query.expression_predicates.len > 0) alloc.free(lhs.query.expression_predicates);
    lhs.query.expression_predicates = &.{};
    freeExpressionPredicateGroups(alloc, lhs.query.expression_or_predicates);
    if (lhs.query.expression_or_predicates.len > 0) alloc.free(lhs.query.expression_or_predicates);
    lhs.query.expression_or_predicates = groups;
}

fn applySimpleExpressionBranchUnion(
    alloc: std.mem.Allocator,
    lhs: *plan_mod.LoweredSelect,
    rhs: db_mod.types.RelationalRowsQueryRequest,
) !void {
    const left_groups = try expressionGroupsFromSimpleUnionQueryAlloc(alloc, lhs.query);
    defer {
        freeExpressionPredicateGroups(alloc, left_groups);
        if (left_groups.len > 0) alloc.free(left_groups);
    }
    const right_groups = try expressionGroupsFromSimpleUnionQueryAlloc(alloc, rhs);
    defer {
        freeExpressionPredicateGroups(alloc, right_groups);
        if (right_groups.len > 0) alloc.free(right_groups);
    }

    const groups = try alloc.alloc(db_mod.types.RelationalRowsExpressionPredicateGroup, left_groups.len + right_groups.len);
    var initialized: usize = 0;
    errdefer {
        for (groups[0..initialized]) |group| freeExpressionPredicateGroup(alloc, group);
        alloc.free(groups);
    }
    for (left_groups) |group| {
        groups[initialized] = .{ .conditions = try cloneExpressionConditionsAlloc(alloc, group.conditions) };
        initialized += 1;
    }
    for (right_groups) |group| {
        groups[initialized] = .{ .conditions = try cloneExpressionConditionsAlloc(alloc, group.conditions) };
        initialized += 1;
    }

    replaceQueryWithExpressionOrBranches(alloc, lhs, groups);
}

fn applySimpleIntersect(
    alloc: std.mem.Allocator,
    lhs: *plan_mod.LoweredSelect,
    rhs: db_mod.types.RelationalRowsQueryRequest,
) !void {
    if (lhs.query.expression_or_predicates.len > 0 or rhs.expression_or_predicates.len > 0) {
        return try applySimpleExpressionBranchIntersect(alloc, lhs, rhs);
    }
    if (lhs.query.or_predicates.len > 0 or rhs.or_predicates.len > 0) {
        return try applySimpleScalarBranchIntersect(alloc, lhs, rhs);
    }
    if (rhs.predicates.len > 0) {
        const predicates = try cloneQueryRelationalChecksConcatAlloc(alloc, lhs.query.predicates, rhs.predicates);
        freeRelationalChecks(alloc, lhs.query.predicates);
        if (lhs.query.predicates.len > 0) alloc.free(lhs.query.predicates);
        lhs.query.predicates = predicates;
    }
    if (rhs.in_predicates.len > 0) {
        const in_predicates = try cloneInPredicatesConcatAlloc(alloc, lhs.query.in_predicates, rhs.in_predicates);
        freeInPredicates(alloc, lhs.query.in_predicates);
        if (lhs.query.in_predicates.len > 0) alloc.free(lhs.query.in_predicates);
        lhs.query.in_predicates = in_predicates;
    }
    if (rhs.expression_predicates.len > 0) {
        const expression_predicates = try cloneExpressionConditionsConcatAlloc(alloc, lhs.query.expression_predicates, rhs.expression_predicates);
        freeExpressionConditions(alloc, lhs.query.expression_predicates);
        if (lhs.query.expression_predicates.len > 0) alloc.free(lhs.query.expression_predicates);
        lhs.query.expression_predicates = expression_predicates;
    }
}

fn applySimpleScalarBranchIntersect(
    alloc: std.mem.Allocator,
    lhs: *plan_mod.LoweredSelect,
    rhs: db_mod.types.RelationalRowsQueryRequest,
) !void {
    const lhs_branch_count = simpleScalarSetQueryBranchCount(lhs.query);
    const rhs_branch_count = simpleScalarSetQueryBranchCount(rhs);
    if (rhs_branch_count == 0) return;
    if (lhs_branch_count == 0) {
        const groups = try cloneSimpleScalarSetQueryBranchesAlloc(alloc, rhs);
        freeRelationalChecks(alloc, lhs.query.predicates);
        if (lhs.query.predicates.len > 0) alloc.free(lhs.query.predicates);
        lhs.query.predicates = &.{};
        freePredicateGroups(alloc, lhs.query.or_predicates);
        if (lhs.query.or_predicates.len > 0) alloc.free(lhs.query.or_predicates);
        lhs.query.or_predicates = groups;
        return;
    }
    if (lhs_branch_count > max_scalar_or_expanded_branches / rhs_branch_count) return error.UnsupportedSqlShape;

    const groups = try alloc.alloc(db_mod.types.RelationalRowsPredicateGroup, lhs_branch_count * rhs_branch_count);
    var initialized: usize = 0;
    errdefer {
        for (groups[0..initialized]) |group| freePredicateGroup(alloc, group);
        alloc.free(groups);
    }
    for (0..lhs_branch_count) |left_index| {
        const left = simpleScalarSetQueryBranchAt(lhs.query, left_index) orelse return error.UnsupportedSqlShape;
        for (0..rhs_branch_count) |right_index| {
            const right = simpleScalarSetQueryBranchAt(rhs, right_index) orelse return error.UnsupportedSqlShape;
            groups[initialized] = .{ .predicates = try cloneQueryRelationalChecksConcatAlloc(alloc, left, right) };
            initialized += 1;
        }
    }

    freeRelationalChecks(alloc, lhs.query.predicates);
    if (lhs.query.predicates.len > 0) alloc.free(lhs.query.predicates);
    lhs.query.predicates = &.{};
    freePredicateGroups(alloc, lhs.query.or_predicates);
    if (lhs.query.or_predicates.len > 0) alloc.free(lhs.query.or_predicates);
    lhs.query.or_predicates = groups;
}

fn applySimpleExpressionBranchIntersect(
    alloc: std.mem.Allocator,
    lhs: *plan_mod.LoweredSelect,
    rhs: db_mod.types.RelationalRowsQueryRequest,
) !void {
    const rhs_groups = try expressionGroupsFromSimpleIntersectQueryAlloc(alloc, rhs);
    defer {
        freeExpressionPredicateGroups(alloc, rhs_groups);
        if (rhs_groups.len > 0) alloc.free(rhs_groups);
    }
    if (rhs_groups.len == 0) return;

    const lhs_groups = try expressionGroupsFromSimpleIntersectQueryAlloc(alloc, lhs.query);
    defer {
        freeExpressionPredicateGroups(alloc, lhs_groups);
        if (lhs_groups.len > 0) alloc.free(lhs_groups);
    }
    if (lhs_groups.len == 0) {
        const groups = try cloneExpressionPredicateGroupsAlloc(alloc, rhs_groups);
        replaceQueryWithExpressionOrBranches(alloc, lhs, groups);
        return;
    }
    if (lhs_groups.len > max_scalar_or_expanded_branches / rhs_groups.len) return error.UnsupportedSqlShape;

    const groups = try alloc.alloc(db_mod.types.RelationalRowsExpressionPredicateGroup, lhs_groups.len * rhs_groups.len);
    var initialized: usize = 0;
    errdefer {
        for (groups[0..initialized]) |group| freeExpressionPredicateGroup(alloc, group);
        alloc.free(groups);
    }
    for (lhs_groups) |left| {
        for (rhs_groups) |right| {
            groups[initialized] = .{ .conditions = try cloneExpressionConditionsConcatAlloc(alloc, left.conditions, right.conditions) };
            initialized += 1;
        }
    }

    replaceQueryWithExpressionOrBranches(alloc, lhs, groups);
}

fn replaceQueryWithExpressionOrBranches(
    alloc: std.mem.Allocator,
    lhs: *plan_mod.LoweredSelect,
    groups: []db_mod.types.RelationalRowsExpressionPredicateGroup,
) void {
    freeRelationalChecks(alloc, lhs.query.predicates);
    if (lhs.query.predicates.len > 0) alloc.free(lhs.query.predicates);
    lhs.query.predicates = &.{};
    freePredicateGroups(alloc, lhs.query.or_predicates);
    if (lhs.query.or_predicates.len > 0) alloc.free(lhs.query.or_predicates);
    lhs.query.or_predicates = &.{};
    freeInPredicates(alloc, lhs.query.in_predicates);
    if (lhs.query.in_predicates.len > 0) alloc.free(lhs.query.in_predicates);
    lhs.query.in_predicates = &.{};
    freeAccessPredicateGroups(alloc, lhs.query.access_or_predicates);
    if (lhs.query.access_or_predicates.len > 0) alloc.free(lhs.query.access_or_predicates);
    lhs.query.access_or_predicates = &.{};
    freeExpressionConditions(alloc, lhs.query.expression_predicates);
    if (lhs.query.expression_predicates.len > 0) alloc.free(lhs.query.expression_predicates);
    lhs.query.expression_predicates = &.{};
    freeExpressionPredicateGroups(alloc, lhs.query.expression_or_predicates);
    if (lhs.query.expression_or_predicates.len > 0) alloc.free(lhs.query.expression_or_predicates);
    lhs.query.expression_or_predicates = groups;
}

fn applySimpleExcept(
    alloc: std.mem.Allocator,
    lhs: *plan_mod.LoweredSelect,
    rhs: db_mod.types.RelationalRowsQueryRequest,
) !void {
    if (rhs.predicates.len == 0 and rhs.or_predicates.len == 0 and rhs.in_predicates.len == 0 and rhs.expression_predicates.len == 0 and rhs.expression_or_predicates.len == 0) return error.UnsupportedSqlShape;
    if (rhs.or_predicates.len > 0) {
        const groups = try cloneSimpleScalarSetQueryBranchesAlloc(alloc, rhs);
        freePredicateGroups(alloc, lhs.query.not_predicates);
        if (lhs.query.not_predicates.len > 0) alloc.free(lhs.query.not_predicates);
        lhs.query.not_predicates = groups;
        return;
    }
    if (rhs.in_predicates.len > 0 and rhs.expression_or_predicates.len > 0) {
        const groups = try expressionGroupsFromInSetQueryAlloc(alloc, rhs);
        freeExpressionPredicateGroups(alloc, lhs.query.expression_not_predicates);
        if (lhs.query.expression_not_predicates.len > 0) alloc.free(lhs.query.expression_not_predicates);
        lhs.query.expression_not_predicates = groups;
        return;
    }
    if (rhs.expression_or_predicates.len > 0) {
        const groups = try cloneSimpleExpressionSetQueryBranchesAlloc(alloc, rhs);
        freeExpressionPredicateGroups(alloc, lhs.query.expression_not_predicates);
        if (lhs.query.expression_not_predicates.len > 0) alloc.free(lhs.query.expression_not_predicates);
        lhs.query.expression_not_predicates = groups;
        return;
    }
    if (rhs.in_predicates.len > 0 and rhs.expression_predicates.len > 0) {
        const groups = try expressionGroupsFromInSetQueryAlloc(alloc, rhs);
        freeExpressionPredicateGroups(alloc, lhs.query.expression_not_predicates);
        if (lhs.query.expression_not_predicates.len > 0) alloc.free(lhs.query.expression_not_predicates);
        lhs.query.expression_not_predicates = groups;
        return;
    }
    if (rhs.expression_predicates.len > 0) {
        const groups = try alloc.alloc(db_mod.types.RelationalRowsExpressionPredicateGroup, 1);
        var initialized: usize = 0;
        errdefer {
            for (groups[0..initialized]) |group| freeExpressionPredicateGroup(alloc, group);
            alloc.free(groups);
        }
        groups[0] = .{ .conditions = try expressionConditionsFromSimpleSetQueryAlloc(alloc, rhs) };
        initialized += 1;
        freeExpressionPredicateGroups(alloc, lhs.query.expression_not_predicates);
        if (lhs.query.expression_not_predicates.len > 0) alloc.free(lhs.query.expression_not_predicates);
        lhs.query.expression_not_predicates = groups;
        return;
    }
    if (rhs.in_predicates.len > 0) {
        const groups = try alloc.alloc(db_mod.types.RelationalRowsAccessPredicateGroup, 1);
        var initialized: usize = 0;
        errdefer {
            for (groups[0..initialized]) |group| freeAccessPredicateGroup(alloc, group);
            alloc.free(groups);
        }
        const predicates = try cloneQueryRelationalChecksAlloc(alloc, rhs.predicates);
        var predicates_transferred = false;
        errdefer if (!predicates_transferred) {
            freeRelationalChecks(alloc, predicates);
            if (predicates.len > 0) alloc.free(predicates);
        };
        const in_predicates = try cloneInPredicatesAlloc(alloc, rhs.in_predicates);
        var in_transferred = false;
        errdefer if (!in_transferred) {
            freeInPredicates(alloc, in_predicates);
            if (in_predicates.len > 0) alloc.free(in_predicates);
        };
        groups[0] = .{ .predicates = predicates, .in_predicates = in_predicates };
        predicates_transferred = true;
        in_transferred = true;
        initialized += 1;
        freeAccessPredicateGroups(alloc, lhs.query.access_not_predicates);
        if (lhs.query.access_not_predicates.len > 0) alloc.free(lhs.query.access_not_predicates);
        lhs.query.access_not_predicates = groups;
        return;
    }
    const groups = try alloc.alloc(db_mod.types.RelationalRowsPredicateGroup, 1);
    var initialized: usize = 0;
    errdefer {
        for (groups[0..initialized]) |group| freePredicateGroup(alloc, group);
        alloc.free(groups);
    }
    groups[0] = .{ .predicates = try cloneQueryRelationalChecksAlloc(alloc, rhs.predicates) };
    initialized += 1;
    freePredicateGroups(alloc, lhs.query.not_predicates);
    if (lhs.query.not_predicates.len > 0) alloc.free(lhs.query.not_predicates);
    lhs.query.not_predicates = groups;
}

pub fn setOperationColumnsCompatible(
    lhs: []const runtime_schema.RelationalColumn,
    rhs: []const runtime_schema.RelationalColumn,
) bool {
    if (lhs.len == 0 or lhs.len != rhs.len) return false;
    for (lhs, rhs) |left, right| {
        if (left.field_type == .array or right.field_type == .array) {
            if (left.field_type != .array or right.field_type != .array) return false;
            const left_item = left.array_item_type orelse .json;
            const right_item = right.array_item_type orelse .json;
            if (!expr_type.sqlExpressionTypesComparable(left_item, right_item)) return false;
        } else if (!expr_type.sqlExpressionTypesComparable(left.field_type, right.field_type)) {
            return false;
        }
    }
    return true;
}

test "sql select_set compares query projection and set operation surfaces" {
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
    const extended_query: db_mod.types.RelationalRowsQueryRequest = .{
        .json_contains = &.{.{ .field = "metadata", .value_json = "{\"tier\":\"gold\"}" }},
    };
    const left_outputs = [_]ast.SelectOutputRef{.{ .kind = .field, .index = 0 }};
    const right_outputs = [_]ast.SelectOutputRef{.{ .kind = .field, .index = 0 }};
    const mismatched_outputs = [_]ast.SelectOutputRef{.{ .kind = .expression, .index = 0 }};
    const left_columns = [_]runtime_schema.RelationalColumn{.{ .name = "status", .path = "status", .field_type = .keyword }};
    const right_columns = [_]runtime_schema.RelationalColumn{.{ .name = "status", .path = "status", .field_type = .keyword }};
    const right_text_alias_columns = [_]runtime_schema.RelationalColumn{.{ .name = "body_text", .path = "body_text", .field_type = .text }};
    const left_array_columns = [_]runtime_schema.RelationalColumn{.{ .name = "tags", .path = "tags", .field_type = .array, .array_item_type = .keyword }};
    const right_array_columns = [_]runtime_schema.RelationalColumn{.{ .name = "labels", .path = "labels", .field_type = .array, .array_item_type = .text }};
    const incompatible_columns = [_]runtime_schema.RelationalColumn{.{ .name = "status", .path = "status", .field_type = .numeric }};
    const incompatible_array_columns = [_]runtime_schema.RelationalColumn{.{ .name = "amounts", .path = "amounts", .field_type = .array, .array_item_type = .numeric }};

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
    try std.testing.expect(!sourceQueryUsesExtendedPredicates(simple_query));
    try std.testing.expect(sourceQueryUsesExtendedPredicates(extended_query));
    try std.testing.expect(expr_equal.expressionProjectionsEqual(&.{projection}, &.{projection}));
    try std.testing.expect(simpleSelectProjectionsEqual(simple_query, same_query, &left_outputs, &right_outputs));
    try std.testing.expect(!simpleSelectProjectionsEqual(simple_query, same_query, &left_outputs, &mismatched_outputs));
    try std.testing.expect(setOperationColumnsCompatible(&left_columns, &right_columns));
    try std.testing.expect(setOperationColumnsCompatible(&left_columns, &right_text_alias_columns));
    try std.testing.expect(setOperationColumnsCompatible(&left_array_columns, &right_array_columns));
    try std.testing.expect(!setOperationColumnsCompatible(&left_columns, &incompatible_columns));
    try std.testing.expect(!setOperationColumnsCompatible(&left_array_columns, &incompatible_array_columns));
}
