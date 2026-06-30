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

const corpus = @import("corpus.zig");
const db_mod = @import("../storage/db/mod.zig");
const diagnostics = @import("diagnostics.zig");
const ddl_fingerprint = @import("fingerprint.zig");
const lower_dml = @import("lower_dml.zig");
const lower_expr = @import("lower_expr.zig");
const plan_mod = @import("plan.zig");

const ExplainFormat = plan_mod.ExplainFormat;
const LoweredExplainPlan = plan_mod.LoweredExplainPlan;
const LoweredInsertSource = plan_mod.LoweredInsertSource;
const LoweredJoinedMutationSource = plan_mod.LoweredJoinedMutationSource;
const LoweredMergeMutationPlan = plan_mod.LoweredMergeMutationPlan;
const LoweredMutationSource = plan_mod.LoweredMutationSource;
const LoweredRecursiveInsertSource = plan_mod.LoweredRecursiveInsertSource;
const LoweredRecursiveJoinedMutationSource = plan_mod.LoweredRecursiveJoinedMutationSource;
const LoweredRecursiveMergeMutation = plan_mod.LoweredRecursiveMergeMutation;
const LoweredRelationPopulationPlan = plan_mod.LoweredRelationPopulationPlan;
const LoweredWritePlan = plan_mod.LoweredWritePlan;
const RelationLifetimeKind = plan_mod.RelationLifetimeKind;
const RelationPopulationMode = plan_mod.RelationPopulationMode;

const conflictActionName = lower_dml.conflictActionName;
const expressionAssignmentComputedCount = lower_dml.expressionAssignmentComputedCount;
const transformOperationCount = lower_dml.transformOperationCount;
const sourceQueryUsesExtendedPredicates = lower_expr.sourceQueryUsesExtendedPredicates;
const sqlRowClaimFingerprintName = lower_expr.sqlRowClaimFingerprintName;
const relationLifetimeKindName = ddl_fingerprint.relationLifetimeKindName;
const appParityBoolValue = corpus.appParityBoolValue;
const appParityLimitValue = corpus.appParityLimitValue;
const appendBoolFingerprintAlloc = corpus.appendBoolFingerprintAlloc;
const appendCteAccessPathFingerprintAlloc = corpus.appendCteAccessPathFingerprintAlloc;
const appendNamedNonZeroUsizeFingerprintAlloc = corpus.appendNamedNonZeroUsizeFingerprintAlloc;
const appendNonZeroU32FingerprintAlloc = corpus.appendNonZeroU32FingerprintAlloc;
const appendNonZeroUsizeFingerprintAlloc = corpus.appendNonZeroUsizeFingerprintAlloc;
const appendSideQueryAccessOnlyFingerprintAlloc = corpus.appendSideQueryAccessOnlyFingerprintAlloc;
const appendSourceQueryAccessOnlyFingerprintAlloc = corpus.appendSourceQueryAccessOnlyFingerprintAlloc;
const appendStringFingerprintAlloc = corpus.appendStringFingerprintAlloc;
const appendTransformOpFingerprintAlloc = corpus.appendTransformOpFingerprintAlloc;
const appendTrueBoolFingerprintAlloc = corpus.appendTrueBoolFingerprintAlloc;
const readPlanFingerprintAlloc = corpus.readPlanFingerprintAlloc;
const recursiveCteMemberFingerprintAlloc = corpus.recursiveCteMemberFingerprintAlloc;

const mergeMatchedExpressionNotPredicateCount = plan_mod.mergeMatchedExpressionNotPredicateCount;
const mergeMatchedExpressionOrPredicateCount = plan_mod.mergeMatchedExpressionOrPredicateCount;
const mergeMatchedExpressionPredicateCount = plan_mod.mergeMatchedExpressionPredicateCount;
const mergeMatchedHasDelete = plan_mod.mergeMatchedHasDelete;
const mergeMatchedHasDoNothing = plan_mod.mergeMatchedHasDoNothing;
const mergeMatchedPredicateCount = plan_mod.mergeMatchedPredicateCount;
const mergeMatchedUpdateCount = plan_mod.mergeMatchedUpdateCount;
const mergeMatchedUpdateExpressionCount = plan_mod.mergeMatchedUpdateExpressionCount;
const mergeNotMatchedExpressionNotPredicateCount = plan_mod.mergeNotMatchedExpressionNotPredicateCount;
const mergeNotMatchedExpressionOrPredicateCount = plan_mod.mergeNotMatchedExpressionOrPredicateCount;
const mergeNotMatchedExpressionPredicateCount = plan_mod.mergeNotMatchedExpressionPredicateCount;
const mergeNotMatchedHasDoNothing = plan_mod.mergeNotMatchedHasDoNothing;
const mergeNotMatchedInsertCount = plan_mod.mergeNotMatchedInsertCount;
const mergeNotMatchedInsertExpressionCount = plan_mod.mergeNotMatchedInsertExpressionCount;
const mergeNotMatchedPredicateCount = plan_mod.mergeNotMatchedPredicateCount;

pub fn insertSourceFingerprintAlloc(alloc: std.mem.Allocator, lowered: LoweredInsertSource) ![]u8 {
    const req = lowered.insert_source.req;
    const source = req.source;
    const conflict = req.on_conflict != null;
    const base = if (sourceQueryUsesExtendedPredicates(source))
        try std.fmt.allocPrint(
            alloc,
            "insert_source:table={s}:source_table={s}:source_pred={d}:source_array_any={d}:source_expr_pred={d}:source_expr_or={d}:source_expr_not={d}:source_expr_array={d}:source_order={d}:source_limit={d}:assignments={d}:conflict={d}:returning={d}:returning_expr={d}:returning_all={d}",
            .{
                lowered.table_name,
                req.source_table,
                source.predicates.len,
                source.array_any.len,
                source.expression_predicates.len,
                source.expression_or_predicates.len,
                source.expression_not_predicates.len,
                source.expression_array_contains.len,
                source.order_by.len,
                appParityLimitValue(source.limit),
                req.assignments.len,
                appParityBoolValue(conflict),
                req.returning.len,
                req.returning_expressions.len,
                appParityBoolValue(req.returning_all),
            },
        )
    else
        try std.fmt.allocPrint(
            alloc,
            "insert_source:table={s}:source_table={s}:source_pred={d}:source_order={d}:source_limit={d}:assignments={d}:conflict={d}:returning={d}:returning_expr={d}:returning_all={d}",
            .{
                lowered.table_name,
                req.source_table,
                source.predicates.len,
                source.order_by.len,
                appParityLimitValue(source.limit),
                req.assignments.len,
                appParityBoolValue(conflict),
                req.returning.len,
                req.returning_expressions.len,
                appParityBoolValue(req.returning_all),
            },
        );
    const with_in = try appendNonZeroUsizeFingerprintAlloc(alloc, base, "source_in", source.in_predicates.len);
    const with_or = try appendNonZeroUsizeFingerprintAlloc(alloc, with_in, "source_or", source.or_predicates.len);
    const with_not = try appendNonZeroUsizeFingerprintAlloc(alloc, with_or, "source_not", source.not_predicates.len);
    const with_access = try appendSourceQueryAccessOnlyFingerprintAlloc(alloc, with_not, source);
    const with_assignment_expr = try appendNonZeroUsizeFingerprintAlloc(alloc, with_access, "assignment_expr", expressionAssignmentComputedCount(req.assignments));
    const with_ctes = try appendNonZeroUsizeFingerprintAlloc(alloc, with_assignment_expr, "ctes", lowered.ctes.len);
    const with_source_cte = try appendTrueBoolFingerprintAlloc(alloc, with_ctes, "source_cte", source.source_cte.len != 0);
    const with_cte_access = try appendCteAccessPathFingerprintAlloc(alloc, with_source_cte, lowered.ctes);
    if (req.on_conflict) |on_conflict| {
        const with_action = try std.fmt.allocPrint(alloc, "{s}:conflict_action={s}", .{ with_cte_access, conflictActionName(on_conflict.action) });
        alloc.free(with_cte_access);
        const with_ops = try appendNonZeroUsizeFingerprintAlloc(alloc, with_action, "conflict_ops", on_conflict.operations.len);
        const with_patch = try appendNonZeroUsizeFingerprintAlloc(alloc, with_ops, "conflict_patch_expr", on_conflict.patch_expressions.len);
        const with_increment = try appendNonZeroUsizeFingerprintAlloc(alloc, with_patch, "conflict_increment_expr", on_conflict.increment_expressions.len);
        const with_json_set = try appendNonZeroUsizeFingerprintAlloc(alloc, with_increment, "conflict_json_set_expr", on_conflict.json_set_expressions.len);
        const with_where = try appendTrueBoolFingerprintAlloc(alloc, with_json_set, "conflict_where_expr", on_conflict.where_expression != null);
        const with_where_all = try appendNonZeroUsizeFingerprintAlloc(alloc, with_where, "conflict_where_exprs", on_conflict.where_expressions.len);
        const with_where_any = try appendNonZeroUsizeFingerprintAlloc(alloc, with_where_all, "conflict_where_any", on_conflict.where_any.len);
        return try appendNonZeroUsizeFingerprintAlloc(alloc, with_where_any, "conflict_where_not", on_conflict.where_not.len);
    }
    return with_cte_access;
}

pub fn recursiveInsertSourceFingerprintAlloc(
    alloc: std.mem.Allocator,
    lowered: LoweredRecursiveInsertSource,
) ![]u8 {
    const insert = try insertSourceFingerprintAlloc(alloc, lowered.insert_source);
    defer alloc.free(insert);
    const member = try recursiveCteMemberFingerprintAlloc(alloc, lowered.recursive.recursive_member);
    defer alloc.free(member);
    var fingerprint = try std.fmt.allocPrint(
        alloc,
        "recursive_insert_source:cte={s}:op={s}:anchor_table={s}:outputs={d}:member={s}:insert={s}",
        .{
            lowered.recursive.cte_name,
            @tagName(lowered.recursive.operation),
            lowered.recursive.anchor.table_name,
            lowered.recursive.output_columns.len,
            member,
            insert,
        },
    );
    fingerprint = try appendNonZeroU32FingerprintAlloc(alloc, fingerprint, "source_offset", lowered.insert_source.insert_source.req.source.offset);
    fingerprint = try appendTrueBoolFingerprintAlloc(alloc, fingerprint, "conflict_where", lowered.insert_source.conflict_where);
    return fingerprint;
}

pub fn mergeMutationFingerprintAlloc(
    alloc: std.mem.Allocator,
    merge: LoweredMergeMutationPlan,
) ![]u8 {
    const source_name = if (merge.source.source_cte.len > 0) merge.source.source_cte else merge.source_table_name;
    var fingerprint = try std.fmt.allocPrint(
        alloc,
        "merge_mutation:target={s}:source={s}:ctes={d}:source_cte={d}:match={d}:matched_pred={d}:matched_update={d}:matched_delete={d}:matched_noop={d}:not_matched_pred={d}:not_matched_insert={d}:not_matched_noop={d}:returning={d}:returning_expr={d}:returning_all={d}",
        .{
            merge.target_table_name,
            source_name,
            merge.ctes.len,
            @as(u8, if (merge.source.source_cte.len > 0) 1 else 0),
            merge.match_fields.len,
            mergeMatchedPredicateCount(merge.matched_arms),
            mergeMatchedUpdateCount(merge.matched_arms),
            appParityBoolValue(mergeMatchedHasDelete(merge.matched_arms)),
            appParityBoolValue(mergeMatchedHasDoNothing(merge.matched_arms)),
            mergeNotMatchedPredicateCount(merge.not_matched_arms),
            mergeNotMatchedInsertCount(merge.not_matched_arms),
            appParityBoolValue(mergeNotMatchedHasDoNothing(merge.not_matched_arms)),
            merge.returning.fields.len,
            merge.returning.expressions.len,
            appParityBoolValue(merge.returning.returnsAll()),
        },
    );
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "matched_update_expr", mergeMatchedUpdateExpressionCount(merge.matched_arms));
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "not_matched_insert_expr", mergeNotMatchedInsertExpressionCount(merge.not_matched_arms));
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "matched_expr_pred", mergeMatchedExpressionPredicateCount(merge.matched_arms));
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "matched_expr_or", mergeMatchedExpressionOrPredicateCount(merge.matched_arms));
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "matched_expr_not", mergeMatchedExpressionNotPredicateCount(merge.matched_arms));
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "not_matched_expr_pred", mergeNotMatchedExpressionPredicateCount(merge.not_matched_arms));
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "not_matched_expr_or", mergeNotMatchedExpressionOrPredicateCount(merge.not_matched_arms));
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "not_matched_expr_not", mergeNotMatchedExpressionNotPredicateCount(merge.not_matched_arms));
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "matched_arms", if (merge.matched_arms.len > 1) merge.matched_arms.len else 0);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "not_matched_arms", if (merge.not_matched_arms.len > 1) merge.not_matched_arms.len else 0);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "data_ctes", merge.data_modifying_ctes.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "data_cte_update", mergeDataModifyingCteKindCount(merge.data_modifying_ctes, .update));
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "data_cte_delete", mergeDataModifyingCteKindCount(merge.data_modifying_ctes, .delete));
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "data_cte_returning", mergeDataModifyingCteReturningCount(merge.data_modifying_ctes));
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "data_cte_claim_placeholder", mergeDataModifyingCteClaimPlaceholderCount(merge.data_modifying_ctes));
    return try appendCteAccessPathFingerprintAlloc(alloc, fingerprint, merge.ctes);
}

fn mergeDataModifyingCteKindCount(
    ctes: []const plan_mod.LoweredDataModifyingCte,
    kind: db_mod.types.RelationalRowsMutationKind,
) usize {
    var count: usize = 0;
    for (ctes) |cte| {
        if (cte.mutation.req.kind == kind) count += 1;
    }
    return count;
}

fn mergeDataModifyingCteReturningCount(ctes: []const plan_mod.LoweredDataModifyingCte) usize {
    var count: usize = 0;
    for (ctes) |cte| {
        count += cte.mutation.req.returning.len;
        count += cte.mutation.req.returning_expressions.len;
        if (cte.mutation.req.returning_all) count += 1;
    }
    return count;
}

fn mergeDataModifyingCteClaimPlaceholderCount(ctes: []const plan_mod.LoweredDataModifyingCte) usize {
    var count: usize = 0;
    for (ctes) |cte| {
        if (cte.claim_placeholder) count += 1;
    }
    return count;
}

pub fn recursiveMergeMutationFingerprintAlloc(
    alloc: std.mem.Allocator,
    lowered: LoweredRecursiveMergeMutation,
) ![]u8 {
    const merge = try mergeMutationFingerprintAlloc(alloc, lowered.merge);
    defer alloc.free(merge);
    const member = try recursiveCteMemberFingerprintAlloc(alloc, lowered.recursive.recursive_member);
    defer alloc.free(member);
    return try std.fmt.allocPrint(
        alloc,
        "recursive_merge_mutation:cte={s}:op={s}:anchor_table={s}:outputs={d}:member={s}:merge={s}",
        .{
            lowered.recursive.cte_name,
            @tagName(lowered.recursive.operation),
            lowered.recursive.anchor.table_name,
            lowered.recursive.output_columns.len,
            member,
            merge,
        },
    );
}

pub fn updateSourceFingerprintAlloc(alloc: std.mem.Allocator, lowered: LoweredMutationSource) ![]u8 {
    const source = lowered.mutation.req.source;
    const claim = if (source.row_claim) |claim_value| sqlRowClaimFingerprintName(claim_value) else "none";
    const base = if (sourceQueryUsesExtendedPredicates(source))
        try std.fmt.allocPrint(
            alloc,
            "update_source:table={s}:source_pred={d}:source_array_any={d}:source_expr_pred={d}:source_expr_or={d}:source_expr_not={d}:source_expr_array={d}:source_order={d}:source_limit={d}:claim={s}:rewrite={}:ops={d}:patch_expr={d}:increment_expr={d}:json_set_expr={d}:returning={d}:returning_expr={d}:returning_all={d}",
            .{
                lowered.table_name,
                source.predicates.len,
                source.array_any.len,
                source.expression_predicates.len,
                source.expression_or_predicates.len,
                source.expression_not_predicates.len,
                source.expression_array_contains.len,
                source.order_by.len,
                appParityLimitValue(source.limit),
                claim,
                appParityBoolValue(lowered.mutation.req.rewrite_identity),
                lowered.mutation.req.operations.len,
                lowered.mutation.req.patch_expressions.len,
                lowered.mutation.req.increment_expressions.len,
                lowered.mutation.req.json_set_expressions.len,
                lowered.mutation.req.returning.len,
                lowered.mutation.req.returning_expressions.len,
                appParityBoolValue(lowered.mutation.req.returning_all),
            },
        )
    else
        try std.fmt.allocPrint(
            alloc,
            "update_source:table={s}:source_pred={d}:source_order={d}:source_limit={d}:claim={s}:rewrite={}:ops={d}:patch_expr={d}:increment_expr={d}:json_set_expr={d}:returning={d}:returning_expr={d}:returning_all={d}",
            .{
                lowered.table_name,
                source.predicates.len,
                source.order_by.len,
                appParityLimitValue(source.limit),
                claim,
                appParityBoolValue(lowered.mutation.req.rewrite_identity),
                lowered.mutation.req.operations.len,
                lowered.mutation.req.patch_expressions.len,
                lowered.mutation.req.increment_expressions.len,
                lowered.mutation.req.json_set_expressions.len,
                lowered.mutation.req.returning.len,
                lowered.mutation.req.returning_expressions.len,
                appParityBoolValue(lowered.mutation.req.returning_all),
            },
        );
    const with_in = try appendNonZeroUsizeFingerprintAlloc(alloc, base, "source_in", source.in_predicates.len);
    const with_or = try appendNonZeroUsizeFingerprintAlloc(alloc, with_in, "source_or", source.or_predicates.len);
    const with_not = try appendNonZeroUsizeFingerprintAlloc(alloc, with_or, "source_not", source.not_predicates.len);
    const with_temporal = try appendTrueBoolFingerprintAlloc(alloc, with_not, "temporal", lowered.mutation.req.temporal_portion != null);
    return try appendSourceQueryAccessOnlyFingerprintAlloc(alloc, with_temporal, source);
}

pub fn deleteSourceFingerprintAlloc(alloc: std.mem.Allocator, lowered: LoweredMutationSource) ![]u8 {
    const source = lowered.mutation.req.source;
    const claim = if (source.row_claim) |claim_value| sqlRowClaimFingerprintName(claim_value) else "none";
    const base = if (sourceQueryUsesExtendedPredicates(source))
        try std.fmt.allocPrint(
            alloc,
            "delete_source:table={s}:source_pred={d}:source_array_any={d}:source_expr_pred={d}:source_expr_or={d}:source_expr_not={d}:source_expr_array={d}:source_order={d}:source_limit={d}:claim={s}:returning={d}:returning_expr={d}:returning_all={d}",
            .{
                lowered.table_name,
                source.predicates.len,
                source.array_any.len,
                source.expression_predicates.len,
                source.expression_or_predicates.len,
                source.expression_not_predicates.len,
                source.expression_array_contains.len,
                source.order_by.len,
                appParityLimitValue(source.limit),
                claim,
                lowered.mutation.req.returning.len,
                lowered.mutation.req.returning_expressions.len,
                appParityBoolValue(lowered.mutation.req.returning_all),
            },
        )
    else
        try std.fmt.allocPrint(
            alloc,
            "delete_source:table={s}:source_pred={d}:source_order={d}:source_limit={d}:claim={s}:returning={d}:returning_expr={d}:returning_all={d}",
            .{
                lowered.table_name,
                source.predicates.len,
                source.order_by.len,
                appParityLimitValue(source.limit),
                claim,
                lowered.mutation.req.returning.len,
                lowered.mutation.req.returning_expressions.len,
                appParityBoolValue(lowered.mutation.req.returning_all),
            },
        );
    const with_in = try appendNonZeroUsizeFingerprintAlloc(alloc, base, "source_in", source.in_predicates.len);
    const with_or = try appendNonZeroUsizeFingerprintAlloc(alloc, with_in, "source_or", source.or_predicates.len);
    const with_not = try appendNonZeroUsizeFingerprintAlloc(alloc, with_or, "source_not", source.not_predicates.len);
    const with_temporal = try appendTrueBoolFingerprintAlloc(alloc, with_not, "temporal", lowered.mutation.req.temporal_portion != null);
    const with_restart_identity = try appendTrueBoolFingerprintAlloc(alloc, with_temporal, "restart_identity", lowered.mutation.req.restart_identity);
    return try appendSourceQueryAccessOnlyFingerprintAlloc(alloc, with_restart_identity, source);
}

pub fn truncateSourceFingerprintAlloc(alloc: std.mem.Allocator, lowered: LoweredMutationSource) ![]u8 {
    const delete_fingerprint = try deleteSourceFingerprintAlloc(alloc, lowered);
    defer alloc.free(delete_fingerprint);
    const delete_prefix = "delete_source:";
    if (!std.mem.startsWith(u8, delete_fingerprint, delete_prefix)) return error.TestUnexpectedResult;
    var fingerprint = try std.fmt.allocPrint(alloc, "truncate_source:{s}", .{delete_fingerprint[delete_prefix.len..]});
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "additional_tables", lowered.additional_table_names.len);
    return try appendTrueBoolFingerprintAlloc(alloc, fingerprint, "cascade", lowered.truncate_cascade);
}

pub fn joinedSourceFingerprintAlloc(
    alloc: std.mem.Allocator,
    prefix: []const u8,
    lowered: LoweredJoinedMutationSource,
) ![]u8 {
    const req = lowered.mutation.req;
    const claim = if (req.join.left.row_claim) |claim_value| sqlRowClaimFingerprintName(claim_value) else "none";
    const source_table = if (req.source_table.len > 0) req.source_table else lowered.source_table_name;
    var fingerprint = try std.fmt.allocPrint(
        alloc,
        "{s}:target={s}:source={s}:left_pred={d}:right_pred={d}:on={d}:order={d}:limit={d}:claim={s}:source_assignments={d}:ops={d}:returning={d}:returning_expr={d}:returning_all={d}",
        .{
            prefix,
            lowered.target_table_name,
            source_table,
            req.join.left.predicates.len,
            req.join.right.predicates.len,
            req.join.on.len,
            req.join.order_by.len,
            appParityLimitValue(req.join.limit),
            claim,
            req.source_assignments.len,
            req.operations.len,
            req.returning.len,
            req.returning_expressions.len,
            appParityBoolValue(req.returning_all),
        },
    );
    fingerprint = try appendNonZeroU32FingerprintAlloc(alloc, fingerprint, "offset", req.join.offset);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "patch_expr", req.patch_expressions.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "increment_expr", req.increment_expressions.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "json_set_expr", req.json_set_expressions.len);
    fingerprint = try appendNamedNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "left", "expr_pred", req.join.left.expression_predicates.len);
    fingerprint = try appendNamedNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "left", "expr_or", req.join.left.expression_or_predicates.len);
    fingerprint = try appendNamedNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "left", "expr_not", req.join.left.expression_not_predicates.len);
    fingerprint = try appendNamedNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "left", "expr_array", req.join.left.expression_array_contains.len);
    fingerprint = try appendNamedNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "right", "expr_pred", req.join.right.expression_predicates.len);
    fingerprint = try appendNamedNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "right", "expr_or", req.join.right.expression_or_predicates.len);
    fingerprint = try appendNamedNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "right", "expr_not", req.join.right.expression_not_predicates.len);
    fingerprint = try appendNamedNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "right", "expr_array", req.join.right.expression_array_contains.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "on_expr_pred", req.join.on_expression_predicates.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "on_expr_or", req.join.on_expression_or_predicates.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "on_expr_not", req.join.on_expression_not_predicates.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "on_expr_array", req.join.on_expression_array_contains.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "match_expr_pred", req.match_expression_predicates.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "match_expr_or", req.match_expression_or_predicates.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "match_expr_not", req.match_expression_not_predicates.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "match_expr_array", req.match_expression_array_contains.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "ctes", req.ctes.len);
    fingerprint = try appendTrueBoolFingerprintAlloc(alloc, fingerprint, "right_source_cte", req.join.right.source_cte.len != 0);
    fingerprint = try appendTrueBoolFingerprintAlloc(alloc, fingerprint, "rewrite", req.rewrite_identity);
    fingerprint = try appendSideQueryAccessOnlyFingerprintAlloc(alloc, fingerprint, "left", req.join.left);
    fingerprint = try appendSideQueryAccessOnlyFingerprintAlloc(alloc, fingerprint, "right", req.join.right);
    return try appendCteAccessPathFingerprintAlloc(alloc, fingerprint, req.ctes);
}

pub fn recursiveJoinedSourceFingerprintAlloc(
    alloc: std.mem.Allocator,
    prefix: []const u8,
    lowered: LoweredRecursiveJoinedMutationSource,
) ![]u8 {
    const mutation = try joinedSourceFingerprintAlloc(alloc, prefix, lowered.mutation);
    defer alloc.free(mutation);
    const member = try recursiveCteMemberFingerprintAlloc(alloc, lowered.recursive.recursive_member);
    defer alloc.free(member);
    return try std.fmt.allocPrint(
        alloc,
        "{s}:recursive_cte={s}:op={s}:anchor_table={s}:outputs={d}:member={s}:mutation={s}",
        .{
            prefix,
            lowered.recursive.cte_name,
            @tagName(lowered.recursive.operation),
            lowered.recursive.anchor.table_name,
            lowered.recursive.output_columns.len,
            member,
            mutation,
        },
    );
}

pub fn relationPopulationFingerprintAlloc(
    alloc: std.mem.Allocator,
    lowered: LoweredRelationPopulationPlan,
) ![]u8 {
    const source = try readPlanFingerprintAlloc(alloc, lowered.source);
    defer alloc.free(source);
    return try std.fmt.allocPrint(
        alloc,
        "relation_population:mode={s}:target={s}:lifetime={s}:if_not_exists={}:populate={}:source={s}",
        .{ relationPopulationModeName(lowered.mode), lowered.target_table_name, relationPopulationTargetLifetimeName(lowered.target_lifetime), lowered.if_not_exists, lowered.populate, source },
    );
}

fn relationPopulationModeName(mode: RelationPopulationMode) []const u8 {
    return switch (mode) {
        .create_table_as => "create_table_as",
        .select_into => "select_into",
    };
}

fn relationPopulationTargetLifetimeName(kind: ?RelationLifetimeKind) []const u8 {
    return if (kind) |lifetime| relationLifetimeKindName(lifetime) else "durable";
}

pub fn writePlanFingerprintAlloc(alloc: std.mem.Allocator, lowered: LoweredWritePlan) ![]u8 {
    return switch (lowered) {
        .insert => |insert| blk: {
            const operations = transformOperationCount(insert.batch.transforms);
            var fingerprint = try std.fmt.allocPrint(
                alloc,
                "insert:table={s}:writes={d}:transforms={d}:ops={d}:deletes={d}:returning_rows={d}:returning_expr={d}",
                .{
                    insert.table_name,
                    insert.batch.writes.len,
                    insert.batch.transforms.len,
                    operations,
                    insert.batch.deletes.len,
                    insert.batch.returning_rows.len,
                    insert.returning_expression_count,
                },
            );
            fingerprint = try appendTransformOpFingerprintAlloc(alloc, fingerprint, insert.batch.transforms);
            fingerprint = try appendTrueBoolFingerprintAlloc(alloc, fingerprint, "returning_all", insert.returning_all);
            fingerprint = try appendTrueBoolFingerprintAlloc(alloc, fingerprint, "conflict_where", insert.conflict_where);
            break :blk fingerprint;
        },
        .insert_source => |insert_source| blk: {
            var fingerprint = try insertSourceFingerprintAlloc(alloc, insert_source);
            fingerprint = try appendNonZeroU32FingerprintAlloc(alloc, fingerprint, "source_offset", insert_source.insert_source.req.source.offset);
            fingerprint = try appendTrueBoolFingerprintAlloc(alloc, fingerprint, "conflict_where", insert_source.conflict_where);
            break :blk fingerprint;
        },
        .recursive_insert_source => |recursive_insert_source| blk: {
            break :blk try recursiveInsertSourceFingerprintAlloc(alloc, recursive_insert_source);
        },
        .update => |update| blk: {
            const operations = transformOperationCount(update.batch.transforms);
            var fingerprint = try std.fmt.allocPrint(
                alloc,
                "update:table={s}:transforms={d}:ops={d}:returning_rows={d}:returning_expr={d}",
                .{ update.table_name, update.batch.transforms.len, operations, update.batch.returning_rows.len, update.returning_expression_count },
            );
            fingerprint = try appendTransformOpFingerprintAlloc(alloc, fingerprint, update.batch.transforms);
            fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "writes", update.batch.writes.len);
            fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "deletes", update.batch.deletes.len);
            fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "identity_rewrites", update.batch.relational_identity_rewrites.len);
            fingerprint = try appendTrueBoolFingerprintAlloc(alloc, fingerprint, "returning_all", update.returning_all);
            break :blk fingerprint;
        },
        .delete => |delete| blk: {
            var fingerprint = try std.fmt.allocPrint(
                alloc,
                "delete:table={s}:deletes={d}:returning_rows={d}:returning_expr={d}",
                .{ delete.table_name, delete.batch.deletes.len, delete.batch.returning_rows.len, delete.returning_expression_count },
            );
            fingerprint = try appendTrueBoolFingerprintAlloc(alloc, fingerprint, "returning_all", delete.returning_all);
            break :blk fingerprint;
        },
        .update_source => |update_source| blk: {
            var fingerprint = try updateSourceFingerprintAlloc(alloc, update_source);
            fingerprint = try appendNonZeroU32FingerprintAlloc(alloc, fingerprint, "source_offset", update_source.mutation.req.source.offset);
            break :blk fingerprint;
        },
        .delete_source => |delete_source| blk: {
            var fingerprint = try deleteSourceFingerprintAlloc(alloc, delete_source);
            fingerprint = try appendNonZeroU32FingerprintAlloc(alloc, fingerprint, "source_offset", delete_source.mutation.req.source.offset);
            break :blk fingerprint;
        },
        .truncate_source => |truncate_source| blk: {
            var fingerprint = try truncateSourceFingerprintAlloc(alloc, truncate_source);
            fingerprint = try appendNonZeroU32FingerprintAlloc(alloc, fingerprint, "source_offset", truncate_source.mutation.req.source.offset);
            break :blk fingerprint;
        },
        .update_joined_source => |update_joined_source| try joinedSourceFingerprintAlloc(alloc, "update_joined_source", update_joined_source),
        .delete_joined_source => |delete_joined_source| try joinedSourceFingerprintAlloc(alloc, "delete_joined_source", delete_joined_source),
        .recursive_update_joined_source => |recursive_update_joined_source| try recursiveJoinedSourceFingerprintAlloc(alloc, "update_joined_source", recursive_update_joined_source),
        .recursive_delete_joined_source => |recursive_delete_joined_source| try recursiveJoinedSourceFingerprintAlloc(alloc, "delete_joined_source", recursive_delete_joined_source),
        .merge_mutation => |merge| try mergeMutationFingerprintAlloc(alloc, merge),
        .recursive_merge_mutation => |recursive_merge| try recursiveMergeMutationFingerprintAlloc(alloc, recursive_merge),
    };
}

pub fn explainPlanFingerprintAlloc(alloc: std.mem.Allocator, lowered: LoweredExplainPlan) ![]u8 {
    var subject_kind: []const u8 = undefined;
    const inner = switch (lowered.subject) {
        .read => |read| blk: {
            subject_kind = "read";
            break :blk try readPlanFingerprintAlloc(alloc, read);
        },
        .write => |write| blk: {
            subject_kind = "write";
            break :blk try writePlanFingerprintAlloc(alloc, write);
        },
    };
    defer alloc.free(inner);
    var fingerprint = try std.fmt.allocPrint(
        alloc,
        "explain:kind={s}:analyze={}:inner={s}",
        .{ subject_kind, lowered.analyze, inner },
    );
    if (lowered.format != .text) {
        fingerprint = try appendStringFingerprintAlloc(
            alloc,
            fingerprint,
            "format",
            explainFormatName(lowered.format),
        );
    }
    if (lowered.verbose) {
        fingerprint = try appendBoolFingerprintAlloc(alloc, fingerprint, "verbose", lowered.verbose);
    }
    if (!lowered.costs) {
        fingerprint = try appendBoolFingerprintAlloc(alloc, fingerprint, "costs", lowered.costs);
    }
    if (lowered.buffers) {
        fingerprint = try appendBoolFingerprintAlloc(alloc, fingerprint, "buffers", lowered.buffers);
    }
    if (!lowered.timing) {
        fingerprint = try appendBoolFingerprintAlloc(alloc, fingerprint, "timing", lowered.timing);
    }
    if (!lowered.summary) {
        fingerprint = try appendBoolFingerprintAlloc(alloc, fingerprint, "summary", lowered.summary);
    }
    if (lowered.settings) {
        fingerprint = try appendBoolFingerprintAlloc(alloc, fingerprint, "settings", lowered.settings);
    }
    if (lowered.wal) {
        fingerprint = try appendBoolFingerprintAlloc(alloc, fingerprint, "wal", lowered.wal);
    }
    return fingerprint;
}

fn explainFormatName(format: ExplainFormat) []const u8 {
    return switch (format) {
        .text => "text",
        .json => "json",
    };
}

pub fn invalidPlanFingerprintAlloc(
    alloc: std.mem.Allocator,
    family: []const u8,
    reason: diagnostics.SqlAdapterClassificationReason,
) ![]u8 {
    return try std.fmt.allocPrint(alloc, "invalid:{s}:reason={s}", .{ family, @tagName(reason) });
}
