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
const document_plan = @import("document_plan.zig");
const expr_aggregate = @import("expr/aggregate.zig");
const lower_dml = @import("lower_dml.zig");
const lower_expr = @import("lower_expr.zig");
const plan_mod = @import("plan.zig");
const query_contract = @import("../query/contract.zig");
const query_function = @import("query_function.zig");
const tokenized = @import("tokenized.zig");

pub fn expectOptionalUsize(expected: ?usize, actual: usize) !void {
    if (expected) |value| try std.testing.expectEqual(value, actual);
}

pub fn expectOptionalU32(expected: ?u32, actual: ?u32) !void {
    if (expected) |value| try std.testing.expectEqual(value, actual orelse return error.TestUnexpectedResult);
}

pub fn expectOptionalTableName(expected: ?[]const u8, actual: []const u8) !void {
    if (expected) |value| try std.testing.expectEqualStrings(value, actual);
}

pub fn expectAppParityPlan(expected: []const u8, actual: []const u8) !void {
    if (expected.len == 0) return;
    try std.testing.expectEqualStrings(expected, actual);
}

pub fn expectAppParityReturningRows(expected: []const []const u8, actual: []const []const u8) !void {
    if (expected.len == 0) return;
    try std.testing.expectEqual(expected.len, actual.len);
    for (expected, actual) |expected_row, actual_row| {
        try std.testing.expectEqualStrings(expected_row, actual_row);
    }
}

pub fn expectFailClosedUnsupported(result: anytype) !void {
    if (result) |_| {
        return error.TestExpectedError;
    } else |err| switch (err) {
        error.UnsupportedSqlShape,
        error.InvalidSqlCatalog,
        error.DocumentSqlMergeRequiresNativeProducer,
        error.DocumentSqlLateralRequiresNativeProducer,
        error.DocumentSqlWriteUnsupported,
        error.DocumentSqlUnsupportedJoin,
        error.DocumentSqlBoundedScanIncompleteTopK,
        error.DocumentSqlBoundedScanMissingExactProducer,
        error.DocumentSqlBoundedScanUnboundedSource,
        error.DocumentSqlBoundedScanUnsupportedResidual,
        error.DocumentSqlUnnestUnsupported,
        error.DocumentSqlIndexUnavailable,
        error.DocumentSqlWriteJoinMissingExactProducer,
        error.DocumentSqlWriteJoinMissingCardinalityProof,
        error.DocumentSqlWriteJoinMissingIndexProof,
        error.DocumentSqlWriteJoinOrderedIndexProof,
        error.DocumentSqlWriteJoinPartialIndexProof,
        error.DocumentSqlWriteJoinStaleIndexProof,
        error.DocumentSqlWriteSourceAssignmentAlias,
        error.DocumentSqlWriteSourceAssignmentAmbiguousReference,
        error.DocumentSqlWriteSourceAssignmentGeneratedField,
        error.DocumentSqlWriteSourceAssignmentMissingField,
        error.DocumentSqlWriteSourceAssignmentReservedField,
        error.DocumentSqlWriteSourceAssignmentTargetGeneratedField,
        error.DocumentSqlWriteSourceAssignmentTargetReservedField,
        error.DocumentSqlWriteSourceAssignmentTypeMismatch,
        => return,
        else => return err,
    }
}

pub fn expectTypedInvalid(result: anytype) !void {
    if (result) |_| {
        return error.TestExpectedError;
    } else |err| switch (err) {
        error.InvalidRowsRequest, error.InvalidSqlCatalog, error.UnsupportedSqlShape => return,
        else => return err,
    }
}

pub fn adapterNoopFingerprintAlloc(
    alloc: std.mem.Allocator,
    family: []const u8,
    reason: []const u8,
) ![]u8 {
    const diagnostic_reason = diagnostics.classificationReasonFromToken(reason) orelse return error.TestUnexpectedResult;
    return corpus.adapterNoopFingerprintAlloc(alloc, family, diagnostic_reason) catch |err| switch (err) {
        error.UnsupportedSqlShape => return error.TestUnexpectedResult,
        else => return err,
    };
}

fn appParityResolveDenseQuery(
    ptr: *anyopaque,
    allocator: std.mem.Allocator,
    table_name: []const u8,
    index_name: []const u8,
    semantic_search: []const u8,
    embedding_template: ?[]const u8,
    limit: u32,
) anyerror!db_mod.types.DenseKnnQuery {
    _ = ptr;
    _ = table_name;
    _ = index_name;
    _ = semantic_search;
    _ = embedding_template;
    return .{
        .vector = try allocator.dupe(f32, &[_]f32{ 0.25, 0.5, 0.75 }),
        .k = limit,
    };
}

pub fn expectAppParityQueryFunctionEntry(
    alloc: std.mem.Allocator,
    entry: corpus.AppParityCorpusEntry,
) !void {
    var parsed_sql = try tokenized.ParsedSql.initAlloc(alloc, entry.sql);
    defer parsed_sql.deinit(alloc);
    return try expectAppParityQueryFunctionParsedSqlEntry(alloc, entry, &parsed_sql);
}

pub fn expectAppParityQueryFunctionParsedSqlEntry(
    alloc: std.mem.Allocator,
    entry: corpus.AppParityCorpusEntry,
    parsed_sql: *const tokenized.ParsedSql,
) !void {
    var resolver_state: u8 = 0;
    const semantic_resolver = query_contract.SemanticResolver{
        .ptr = &resolver_state,
        .vtable = &.{ .resolve_dense_query = appParityResolveDenseQuery },
    };
    var lowered = try query_function.lowerAntflyQueryFunctionParsedSqlAlloc(alloc, semantic_resolver, parsed_sql);
    defer lowered.deinit(alloc);
    const fingerprint = try corpus.queryFunctionFingerprintAlloc(alloc, lowered);
    defer alloc.free(fingerprint);
    try expectAppParityPlan(entry.plan, fingerprint);
}

pub fn expectQuerySummary(summary: corpus.AppParityPlanSummary, query: db_mod.types.RelationalRowsQueryRequest) !void {
    try expectQuerySourceSummary(summary, query);
    try expectOptionalUsize(summary.select, query.select.len);
    if (summary.select_all) |expected| try std.testing.expectEqual(expected, query.select_all);
    try expectOptionalUsize(summary.distinct_on, query.distinct_on.len + query.distinct_on_expressions.len);
    try expectOptionalUsize(summary.order_by, query.order_by.len);
    try expectOptionalU32(summary.limit, query.limit);
    if (summary.offset) |expected| try std.testing.expectEqual(expected, query.offset);
    try expectRowClaimSummary(summary, query);
}

fn expectRowClaimSummary(summary: corpus.AppParityPlanSummary, query: db_mod.types.RelationalRowsQueryRequest) !void {
    if (summary.row_claim_skip_locked) |expected| {
        try std.testing.expect(query.row_claim != null);
        try std.testing.expectEqual(expected, query.row_claim.?.skip_locked);
    }
}

pub fn expectQuerySourceSummary(summary: corpus.AppParityPlanSummary, query: db_mod.types.RelationalRowsQueryRequest) !void {
    try expectOptionalUsize(summary.predicates, query.predicates.len);
    try expectOptionalUsize(summary.array_any, query.array_any.len);
    try expectOptionalUsize(summary.in_predicates, query.in_predicates.len);
    try expectOptionalUsize(summary.json_path_eq, query.json_path_eq.len);
    try expectOptionalUsize(summary.json_contains, query.json_contains.len);
    try expectOptionalUsize(summary.json_path_exists, query.json_path_exists.len);
    try expectOptionalUsize(summary.array_contains, query.array_contains.len);
    try expectOptionalUsize(summary.array_eq, query.array_eq.len);
    try expectOptionalUsize(summary.text_patterns, query.text_patterns.len);
    try expectOptionalUsize(summary.access_or_predicates, query.access_or_predicates.len);
    try expectOptionalUsize(summary.access_not_predicates, query.access_not_predicates.len);
    try expectOptionalUsize(summary.expression_predicates, query.expression_predicates.len);
    try expectOptionalUsize(summary.expression_or_predicates, query.expression_or_predicates.len);
    try expectOptionalUsize(summary.expression_not_predicates, query.expression_not_predicates.len);
    try expectOptionalUsize(summary.expression_array_contains, query.expression_array_contains.len);
}

pub fn expectCombinedQuerySourceSummary(
    summary: corpus.AppParityPlanSummary,
    left: db_mod.types.RelationalRowsQueryRequest,
    right: db_mod.types.RelationalRowsQueryRequest,
) !void {
    try expectOptionalUsize(summary.predicates, left.predicates.len + right.predicates.len);
    try expectOptionalUsize(summary.array_any, left.array_any.len + right.array_any.len);
    try expectOptionalUsize(summary.in_predicates, left.in_predicates.len + right.in_predicates.len);
    try expectOptionalUsize(summary.json_path_eq, left.json_path_eq.len + right.json_path_eq.len);
    try expectOptionalUsize(summary.json_contains, left.json_contains.len + right.json_contains.len);
    try expectOptionalUsize(summary.json_path_exists, left.json_path_exists.len + right.json_path_exists.len);
    try expectOptionalUsize(summary.array_contains, left.array_contains.len + right.array_contains.len);
    try expectOptionalUsize(summary.array_eq, left.array_eq.len + right.array_eq.len);
    try expectOptionalUsize(summary.text_patterns, left.text_patterns.len + right.text_patterns.len);
    try expectOptionalUsize(summary.access_or_predicates, left.access_or_predicates.len + right.access_or_predicates.len);
    try expectOptionalUsize(summary.access_not_predicates, left.access_not_predicates.len + right.access_not_predicates.len);
    try expectOptionalUsize(summary.expression_predicates, left.expression_predicates.len + right.expression_predicates.len);
    try expectOptionalUsize(summary.expression_or_predicates, left.expression_or_predicates.len + right.expression_or_predicates.len);
    try expectOptionalUsize(summary.expression_not_predicates, left.expression_not_predicates.len + right.expression_not_predicates.len);
    try expectOptionalUsize(summary.expression_array_contains, left.expression_array_contains.len + right.expression_array_contains.len);
    try expectRowClaimSummary(summary, left);
}

pub fn expectAppParityReadSummary(summary: corpus.AppParityPlanSummary, lowered: plan_mod.LoweredReadPlan) !void {
    switch (lowered) {
        .query => |query| {
            try expectOptionalTableName(summary.table_name, query.table_name);
            try expectOptionalUsize(summary.ctes, query.plan.ctes.len);
            try expectQuerySummary(summary, query.plan.query);
        },
        .document_query => |document| try expectDocumentReadSummary(summary, document),
        .document_aggregate => |aggregate| {
            try expectOptionalTableName(summary.table_name, aggregate.table_name);
            if (corpus.corpusFixtureHasDocumentReadSummary(summary)) return error.TestUnexpectedResult;
        },
        .set_operation => |set_operation| {
            try expectOptionalTableName(summary.table_name, set_operation.left.table_name);
            try expectOptionalUsize(summary.ctes, set_operation.ctes.len + set_operation.left.plan.ctes.len + set_operation.right.plan.ctes.len);
            try expectOptionalUsize(summary.select, set_operation.left.plan.query.select.len);
            try expectOptionalUsize(summary.order_by, set_operation.left.plan.query.order_by.len + set_operation.right.plan.query.order_by.len + set_operation.order_by.len);
            try expectOptionalU32(summary.limit, set_operation.limit orelse set_operation.left.plan.query.limit);
            if (summary.offset) |expected| try std.testing.expectEqual(expected, if (set_operation.offset != 0) set_operation.offset else set_operation.left.plan.query.offset);
            if (summary.right_offset) |expected| try std.testing.expectEqual(expected, set_operation.right.plan.query.offset);
        },
        .recursive_cte => |recursive_cte| {
            try expectOptionalTableName(summary.table_name, recursive_cte.anchor.table_name);
            try expectOptionalUsize(summary.ctes, 1);
            try expectOptionalUsize(summary.select, recursive_cte.final_query.select.len);
            try expectOptionalUsize(summary.order_by, recursive_cte.final_query.order_by.len);
            try expectOptionalU32(summary.limit, recursive_cte.final_query.limit);
        },
        .aggregate => |aggregate| {
            try expectOptionalTableName(summary.table_name, aggregate.table_name);
            try expectOptionalUsize(summary.ctes, aggregate.plan.ctes.len);
            try expectQuerySourceSummary(summary, aggregate.plan.aggregate.source);
            try expectOptionalUsize(summary.group_by, aggregate.plan.aggregate.group_by.len);
            try expectOptionalUsize(summary.group_expressions, aggregate.plan.aggregate.group_expressions.len);
            try expectOptionalUsize(summary.aggregations, aggregate.plan.aggregate.aggregations.len);
            try expectOptionalUsize(summary.filter_groups, expr_aggregate.filterGroupCount(aggregate.plan.aggregate.aggregations));
            try expectOptionalUsize(summary.having, aggregate.plan.aggregate.having_predicates.len);
            try expectOptionalUsize(summary.having_expressions, aggregate.plan.aggregate.having_expressions.len);
            try expectOptionalUsize(summary.having_any, aggregate.plan.aggregate.having_any.len);
            try expectOptionalUsize(summary.having_not, aggregate.plan.aggregate.having_not.len);
            try expectOptionalUsize(summary.order_by, aggregate.plan.aggregate.order_by.len);
            try expectOptionalU32(summary.limit, aggregate.plan.aggregate.limit);
            if (summary.offset) |expected| try std.testing.expectEqual(expected, aggregate.plan.aggregate.offset);
        },
        .join => |join| {
            try expectOptionalTableName(summary.table_name, join.left_table_name);
            try expectCombinedQuerySourceSummary(summary, join.join.left, join.join.right);
            try expectOptionalUsize(summary.join_on, join.join.on.len);
            try expectOptionalUsize(summary.join_select, join.join.select.len);
            try expectOptionalUsize(summary.order_by, join.join.order_by.len);
            try expectOptionalU32(summary.limit, join.join.limit);
            if (summary.offset) |expected| try std.testing.expectEqual(expected, join.join.offset);
        },
        .lateral => |lateral| {
            try expectOptionalTableName(summary.table_name, lateral.left_table_name);
            try expectCombinedQuerySourceSummary(summary, lateral.plan.lateral.left, lateral.plan.lateral.right);
            try expectOptionalUsize(summary.lateral_correlations, lateral.plan.lateral.correlations.len);
            try expectOptionalUsize(summary.join_select, lateral.plan.lateral.select.len);
            try expectOptionalUsize(summary.order_by, lateral.plan.lateral.order_by.len);
            try expectOptionalU32(summary.limit, lateral.plan.lateral.limit);
            if (summary.offset) |expected| try std.testing.expectEqual(expected, lateral.plan.lateral.offset);
            if (summary.right_offset) |expected| try std.testing.expectEqual(expected, lateral.plan.lateral.right.offset);
        },
        .window => |window| {
            try expectOptionalTableName(summary.table_name, window.table_name);
            try expectOptionalUsize(summary.ctes, window.plan.ctes.len);
            try expectQuerySourceSummary(summary, window.plan.window.source);
            try expectOptionalUsize(summary.windows, window.plan.window.windows.len);
            try expectOptionalUsize(summary.select, window.plan.window.select.len);
            try expectOptionalUsize(summary.order_by, window.plan.window.order_by.len);
            try expectOptionalU32(summary.limit, window.plan.window.limit);
            if (summary.offset) |expected| try std.testing.expectEqual(expected, window.plan.window.offset);
        },
    }
}

fn documentProducerNativeRequestName(producer: document_plan.DocumentProducer) []const u8 {
    return switch (producer) {
        .id_lookup => "id_lookup",
        .indexed_query => "indexed_query",
        .bounded_scan => "bounded_scan",
    };
}

fn documentProducerHasResidual(producer: document_plan.DocumentProducer) bool {
    return switch (producer) {
        .id_lookup => false,
        .indexed_query => |query| query.residual_filter_json != null,
        .bounded_scan => |scan| scan.residual_filter_json != null,
    };
}

fn expectDocumentReadSummary(summary: corpus.AppParityPlanSummary, document: document_plan.DocumentReadPlan) !void {
    if (summary.table_name) |table_name| try std.testing.expectEqualStrings(table_name, document.table_name);
    if (summary.document_source_table) |source_table| {
        if (document.view_mapping) |view_mapping| {
            try std.testing.expectEqualStrings(source_table, view_mapping.source_table);
        } else {
            try std.testing.expectEqualStrings(source_table, document.table_name);
        }
    }
    if (summary.document_view_mapping) |mapping_name| {
        const view_mapping = document.view_mapping orelse return error.TestUnexpectedResult;
        try std.testing.expectEqualStrings(mapping_name, view_mapping.name);
    }
    if (summary.document_native_request) |request| {
        try std.testing.expectEqualStrings(request, documentProducerNativeRequestName(document.producer));
    }
    try expectOptionalUsize(summary.document_projections, document.projection.len);
    if (summary.document_residual) |has_residual| {
        try std.testing.expectEqual(has_residual, documentProducerHasResidual(document.producer));
    }
    if (summary.document_order) |has_order| {
        try std.testing.expectEqual(has_order, document.order_by != null);
    }
    if (summary.document_unnest) |has_unnest| {
        try std.testing.expectEqual(has_unnest, document.unnest != null);
    }
    try expectOptionalU32(summary.document_limit, document.limit);
}

pub fn expectAppParityWriteSummary(summary: corpus.AppParityPlanSummary, lowered: plan_mod.LoweredWritePlan) !void {
    switch (lowered) {
        .insert => |insert| {
            try expectOptionalTableName(summary.table_name, insert.table_name);
            try expectOptionalUsize(summary.operations, lower_dml.transformOperationCount(insert.batch.transforms));
            try expectOptionalUsize(summary.returning, insert.batch.returning_rows.len);
            if (summary.returning_all) |expected| try std.testing.expectEqual(expected, insert.returning_all);
            if (summary.conflict_where) |expected| try std.testing.expectEqual(expected, insert.conflict_where);
        },
        .document_write => |document_write| {
            try expectOptionalTableName(summary.table_name, document_write.table_name);
            try expectOptionalUsize(
                summary.operations,
                document_write.batch.writes.len + document_write.batch.transforms.len + document_write.batch.deletes.len,
            );
            try expectOptionalUsize(summary.predicates, document_write.batch.predicates.len);
        },
        .document_conflict_write => |document_conflict| {
            try expectOptionalTableName(summary.table_name, document_conflict.table_name);
            try expectOptionalUsize(summary.operations, document_conflict.operations.len);
            try expectOptionalUsize(summary.source_assignments, document_conflict.source_assignments.len);
            try expectOptionalUsize(summary.returning, document_conflict.returning_fields.len);
        },
        .document_source_insert => |document_source_insert| {
            try expectOptionalTableName(summary.table_name, document_source_insert.table_name);
            if (summary.document_source_table) |expected| {
                try std.testing.expectEqualStrings(expected, document_source_insert.source_table_name);
            }
            try expectOptionalUsize(summary.source_assignments, document_source_insert.assignments.len);
            try expectOptionalUsize(summary.returning, document_source_insert.returning_fields.len);
        },
        .document_producer_mutation => |document_mutation| {
            try expectOptionalTableName(summary.table_name, document_mutation.table_name);
            const operation_count: usize = switch (document_mutation.template) {
                .delete => 0,
                .transform => |operations| operations.len,
            };
            try expectOptionalUsize(summary.operations, operation_count);
            try expectOptionalUsize(summary.predicates, if (document_mutation.expected_version != null) 1 else 0);
        },
        .document_joined_mutation => |document_mutation| {
            try expectOptionalTableName(summary.table_name, document_mutation.table_name);
            const operation_count: usize = switch (document_mutation.template) {
                .delete => 0,
                .transform => |operations| operations.len,
            };
            try expectOptionalUsize(summary.operations, operation_count);
            try expectOptionalUsize(summary.source_assignments, document_mutation.source_assignments.len);
            try expectOptionalUsize(summary.predicates, if (document_mutation.expected_version != null) 1 else 0);
            try expectOptionalUsize(summary.join_on, document_mutation.join_keys.len);
        },
        .document_merge_mutation => |document_merge| {
            try expectOptionalTableName(summary.table_name, document_merge.table_name);
            try expectOptionalUsize(summary.join_on, document_merge.join_keys.len);
        },
        .insert_source => |insert_source| {
            try expectOptionalTableName(summary.table_name, insert_source.table_name);
            try expectOptionalUsize(summary.ctes, insert_source.ctes.len);
            try expectQuerySummary(summary, insert_source.insert_source.req.source);
            try expectOptionalUsize(summary.operations, insert_source.insert_source.req.assignments.len);
            const conflict_patch_expressions = if (insert_source.insert_source.req.on_conflict) |conflict| conflict.patch_expressions.len else 0;
            const conflict_increment_expressions = if (insert_source.insert_source.req.on_conflict) |conflict| conflict.increment_expressions.len else 0;
            const conflict_json_set_expressions = if (insert_source.insert_source.req.on_conflict) |conflict| conflict.json_set_expressions.len else 0;
            try expectOptionalUsize(summary.patch_expressions, conflict_patch_expressions);
            try expectOptionalUsize(summary.increment_expressions, conflict_increment_expressions);
            try expectOptionalUsize(summary.json_set_expressions, conflict_json_set_expressions);
            try expectOptionalUsize(summary.returning, insert_source.insert_source.req.returning.len);
            if (summary.returning_all) |expected| try std.testing.expectEqual(expected, insert_source.insert_source.req.returning_all);
            if (summary.conflict_where) |expected| try std.testing.expectEqual(expected, insert_source.conflict_where);
        },
        .recursive_insert_source => |recursive_insert_source| {
            try expectOptionalTableName(summary.table_name, recursive_insert_source.insert_source.table_name);
            try expectOptionalUsize(summary.ctes, 1);
            try expectQuerySummary(summary, recursive_insert_source.insert_source.insert_source.req.source);
            try expectOptionalUsize(summary.operations, recursive_insert_source.insert_source.insert_source.req.assignments.len);
            const conflict_patch_expressions = if (recursive_insert_source.insert_source.insert_source.req.on_conflict) |conflict| conflict.patch_expressions.len else 0;
            const conflict_increment_expressions = if (recursive_insert_source.insert_source.insert_source.req.on_conflict) |conflict| conflict.increment_expressions.len else 0;
            const conflict_json_set_expressions = if (recursive_insert_source.insert_source.insert_source.req.on_conflict) |conflict| conflict.json_set_expressions.len else 0;
            try expectOptionalUsize(summary.patch_expressions, conflict_patch_expressions);
            try expectOptionalUsize(summary.increment_expressions, conflict_increment_expressions);
            try expectOptionalUsize(summary.json_set_expressions, conflict_json_set_expressions);
            try expectOptionalUsize(summary.returning, recursive_insert_source.insert_source.insert_source.req.returning.len);
            if (summary.returning_all) |expected| try std.testing.expectEqual(expected, recursive_insert_source.insert_source.insert_source.req.returning_all);
            if (summary.conflict_where) |expected| try std.testing.expectEqual(expected, recursive_insert_source.insert_source.conflict_where);
        },
        .update => |update| {
            try expectOptionalTableName(summary.table_name, update.table_name);
            try expectOptionalUsize(summary.operations, lower_dml.transformOperationCount(update.batch.transforms));
            try expectOptionalUsize(summary.returning, update.batch.returning_rows.len);
            if (summary.returning_all) |expected| try std.testing.expectEqual(expected, update.returning_all);
        },
        .delete => |delete| {
            try expectOptionalTableName(summary.table_name, delete.table_name);
            try expectOptionalUsize(summary.returning, delete.batch.returning_rows.len);
            if (summary.returning_all) |expected| try std.testing.expectEqual(expected, delete.returning_all);
        },
        .update_source => |update_source| {
            try expectOptionalTableName(summary.table_name, update_source.table_name);
            try expectQuerySummary(summary, update_source.mutation.req.source);
            try expectOptionalUsize(summary.operations, update_source.mutation.req.operations.len);
            try expectOptionalUsize(summary.patch_expressions, update_source.mutation.req.patch_expressions.len);
            try expectOptionalUsize(summary.increment_expressions, update_source.mutation.req.increment_expressions.len);
            try expectOptionalUsize(summary.json_set_expressions, update_source.mutation.req.json_set_expressions.len);
            try expectOptionalUsize(summary.returning, update_source.mutation.req.returning.len);
            if (summary.returning_all) |expected| try std.testing.expectEqual(expected, update_source.mutation.req.returning_all);
        },
        .delete_source => |delete_source| {
            try expectOptionalTableName(summary.table_name, delete_source.table_name);
            try expectQuerySummary(summary, delete_source.mutation.req.source);
            try expectOptionalUsize(summary.returning, delete_source.mutation.req.returning.len);
            if (summary.returning_all) |expected| try std.testing.expectEqual(expected, delete_source.mutation.req.returning_all);
        },
        .truncate_source => |truncate_source| {
            try expectOptionalTableName(summary.table_name, truncate_source.table_name);
            try expectQuerySummary(summary, truncate_source.mutation.req.source);
            try std.testing.expectEqual(db_mod.types.RelationalRowsMutationKind.delete, truncate_source.mutation.req.kind);
            try std.testing.expectEqual(@as(usize, 0), truncate_source.mutation.req.returning.len);
            try std.testing.expect(!truncate_source.mutation.req.returning_all);
        },
        .update_joined_source => |update_joined_source| {
            try expectOptionalTableName(summary.table_name, update_joined_source.target_table_name);
            try expectOptionalUsize(summary.ctes, update_joined_source.mutation.req.ctes.len);
            try expectCombinedQuerySourceSummary(summary, update_joined_source.mutation.req.join.left, update_joined_source.mutation.req.join.right);
            try expectOptionalUsize(summary.join_on, update_joined_source.mutation.req.join.on.len);
            try expectOptionalUsize(summary.order_by, update_joined_source.mutation.req.join.order_by.len);
            try expectOptionalU32(summary.limit, update_joined_source.mutation.req.join.limit);
            try expectOptionalU32(summary.offset, update_joined_source.mutation.req.join.offset);
            try expectOptionalUsize(summary.operations, update_joined_source.mutation.req.operations.len);
            try expectOptionalUsize(summary.source_assignments, update_joined_source.mutation.req.source_assignments.len);
            try expectOptionalUsize(summary.patch_expressions, update_joined_source.mutation.req.patch_expressions.len);
            try expectOptionalUsize(summary.increment_expressions, update_joined_source.mutation.req.increment_expressions.len);
            try expectOptionalUsize(summary.json_set_expressions, update_joined_source.mutation.req.json_set_expressions.len);
            try expectOptionalUsize(summary.returning, update_joined_source.mutation.req.returning.len);
            if (summary.returning_all) |expected| try std.testing.expectEqual(expected, update_joined_source.mutation.req.returning_all);
        },
        .delete_joined_source => |delete_joined_source| {
            try expectOptionalTableName(summary.table_name, delete_joined_source.target_table_name);
            try expectOptionalUsize(summary.ctes, delete_joined_source.mutation.req.ctes.len);
            try expectCombinedQuerySourceSummary(summary, delete_joined_source.mutation.req.join.left, delete_joined_source.mutation.req.join.right);
            try expectOptionalUsize(summary.join_on, delete_joined_source.mutation.req.join.on.len);
            try expectOptionalUsize(summary.order_by, delete_joined_source.mutation.req.join.order_by.len);
            try expectOptionalU32(summary.limit, delete_joined_source.mutation.req.join.limit);
            try expectOptionalU32(summary.offset, delete_joined_source.mutation.req.join.offset);
            try expectOptionalUsize(summary.returning, delete_joined_source.mutation.req.returning.len);
            if (summary.returning_all) |expected| try std.testing.expectEqual(expected, delete_joined_source.mutation.req.returning_all);
        },
        .recursive_update_joined_source => |recursive_update_joined_source| {
            try expectOptionalTableName(summary.table_name, recursive_update_joined_source.mutation.target_table_name);
            try expectOptionalUsize(summary.ctes, 1);
            try expectCombinedQuerySourceSummary(summary, recursive_update_joined_source.mutation.mutation.req.join.left, recursive_update_joined_source.mutation.mutation.req.join.right);
            try expectOptionalUsize(summary.join_on, recursive_update_joined_source.mutation.mutation.req.join.on.len);
            try expectOptionalUsize(summary.order_by, recursive_update_joined_source.mutation.mutation.req.join.order_by.len);
            try expectOptionalU32(summary.limit, recursive_update_joined_source.mutation.mutation.req.join.limit);
            try expectOptionalU32(summary.offset, recursive_update_joined_source.mutation.mutation.req.join.offset);
            try expectOptionalUsize(summary.operations, recursive_update_joined_source.mutation.mutation.req.operations.len);
            try expectOptionalUsize(summary.source_assignments, recursive_update_joined_source.mutation.mutation.req.source_assignments.len);
            try expectOptionalUsize(summary.patch_expressions, recursive_update_joined_source.mutation.mutation.req.patch_expressions.len);
            try expectOptionalUsize(summary.increment_expressions, recursive_update_joined_source.mutation.mutation.req.increment_expressions.len);
            try expectOptionalUsize(summary.json_set_expressions, recursive_update_joined_source.mutation.mutation.req.json_set_expressions.len);
            try expectOptionalUsize(summary.returning, recursive_update_joined_source.mutation.mutation.req.returning.len);
            if (summary.returning_all) |expected| try std.testing.expectEqual(expected, recursive_update_joined_source.mutation.mutation.req.returning_all);
        },
        .recursive_delete_joined_source => |recursive_delete_joined_source| {
            try expectOptionalTableName(summary.table_name, recursive_delete_joined_source.mutation.target_table_name);
            try expectOptionalUsize(summary.ctes, 1);
            try expectCombinedQuerySourceSummary(summary, recursive_delete_joined_source.mutation.mutation.req.join.left, recursive_delete_joined_source.mutation.mutation.req.join.right);
            try expectOptionalUsize(summary.join_on, recursive_delete_joined_source.mutation.mutation.req.join.on.len);
            try expectOptionalUsize(summary.order_by, recursive_delete_joined_source.mutation.mutation.req.join.order_by.len);
            try expectOptionalU32(summary.limit, recursive_delete_joined_source.mutation.mutation.req.join.limit);
            try expectOptionalU32(summary.offset, recursive_delete_joined_source.mutation.mutation.req.join.offset);
            try expectOptionalUsize(summary.returning, recursive_delete_joined_source.mutation.mutation.req.returning.len);
            if (summary.returning_all) |expected| try std.testing.expectEqual(expected, recursive_delete_joined_source.mutation.mutation.req.returning_all);
        },
        .merge_mutation => |merge_mutation| {
            try expectOptionalTableName(summary.table_name, merge_mutation.target_table_name);
            try expectOptionalUsize(summary.ctes, merge_mutation.ctes.len);
            try expectOptionalUsize(summary.join_on, merge_mutation.match_fields.len);
            try expectOptionalUsize(summary.matched_predicates, plan_mod.mergeMatchedPredicateCount(merge_mutation.matched_arms));
            try expectOptionalUsize(summary.operations, plan_mod.mergeMatchedUpdateCount(merge_mutation.matched_arms));
            if (summary.matched_delete) |expected| try std.testing.expectEqual(expected, plan_mod.mergeMatchedHasDelete(merge_mutation.matched_arms));
            if (summary.matched_do_nothing) |expected| try std.testing.expectEqual(expected, plan_mod.mergeMatchedHasDoNothing(merge_mutation.matched_arms));
            try expectOptionalUsize(summary.not_matched_predicates, plan_mod.mergeNotMatchedPredicateCount(merge_mutation.not_matched_arms));
            try expectOptionalUsize(summary.select, plan_mod.mergeNotMatchedInsertCount(merge_mutation.not_matched_arms));
            if (summary.not_matched_do_nothing) |expected| try std.testing.expectEqual(expected, plan_mod.mergeNotMatchedHasDoNothing(merge_mutation.not_matched_arms));
            try expectOptionalUsize(summary.returning, merge_mutation.returning.fields.len);
            if (summary.returning_all) |expected| try std.testing.expectEqual(expected, merge_mutation.returning.returnsAll());
        },
        .recursive_merge_mutation => |recursive_merge_mutation| {
            const merge_mutation = recursive_merge_mutation.merge;
            try expectOptionalTableName(summary.table_name, merge_mutation.target_table_name);
            try expectOptionalUsize(summary.ctes, 1);
            try expectOptionalUsize(summary.join_on, merge_mutation.match_fields.len);
            try expectOptionalUsize(summary.matched_predicates, plan_mod.mergeMatchedPredicateCount(merge_mutation.matched_arms));
            try expectOptionalUsize(summary.operations, plan_mod.mergeMatchedUpdateCount(merge_mutation.matched_arms));
            if (summary.matched_delete) |expected| try std.testing.expectEqual(expected, plan_mod.mergeMatchedHasDelete(merge_mutation.matched_arms));
            if (summary.matched_do_nothing) |expected| try std.testing.expectEqual(expected, plan_mod.mergeMatchedHasDoNothing(merge_mutation.matched_arms));
            try expectOptionalUsize(summary.not_matched_predicates, plan_mod.mergeNotMatchedPredicateCount(merge_mutation.not_matched_arms));
            try expectOptionalUsize(summary.select, plan_mod.mergeNotMatchedInsertCount(merge_mutation.not_matched_arms));
            if (summary.not_matched_do_nothing) |expected| try std.testing.expectEqual(expected, plan_mod.mergeNotMatchedHasDoNothing(merge_mutation.not_matched_arms));
            try expectOptionalUsize(summary.returning, merge_mutation.returning.fields.len);
            if (summary.returning_all) |expected| try std.testing.expectEqual(expected, merge_mutation.returning.returnsAll());
        },
    }
}

pub fn expectAppParityExplainSummary(summary: corpus.AppParityPlanSummary, lowered: plan_mod.LoweredExplainPlan) !void {
    try expectOptionalBool(summary.explain_options, lowered.format != .text or lowered.verbose or !lowered.costs);
    try expectOptionalBool(summary.explain_analyze, lowered.analyze);
    try expectOptionalBool(summary.explain_buffers, lowered.buffers);
    try expectOptionalBool(summary.explain_timing, lowered.timing);
    try expectOptionalBool(summary.explain_summary, lowered.summary);
    try expectOptionalBool(summary.explain_settings, lowered.settings);
    try expectOptionalBool(summary.explain_wal, lowered.wal);
    switch (lowered.subject) {
        .read => |read| {
            try expectOptionalString(summary.explain_subject, "read");
            try expectOptionalString(summary.explain_inner_kind, @tagName(std.meta.activeTag(read)));
            try expectAppParityReadSummary(summary, read);
        },
        .write => |write| {
            try expectOptionalString(summary.explain_subject, "write");
            try expectOptionalString(summary.explain_inner_kind, @tagName(std.meta.activeTag(write)));
            try expectAppParityWriteSummary(summary, write);
        },
    }
}

fn expectOptionalString(expected: ?[]const u8, actual: []const u8) !void {
    if (expected) |value| try std.testing.expectEqualStrings(value, actual);
}

fn expectOptionalBool(expected: ?bool, actual: bool) !void {
    if (expected) |value| try std.testing.expectEqual(value, actual);
}
