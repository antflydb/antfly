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

pub const document_sql_corpus_json = @embedFile("fixtures/document_sql_corpus.json");
pub const document_sql_corpus_fixture_format: u32 = 1;

pub const DocumentSqlResidualExpectationJson = struct {
    matches: ?bool = null,
    @"error": ?[]const u8 = null,
};

pub const DocumentSqlResidualCaseJson = struct {
    name: []const u8,
    document_json: []const u8,
    filter_json: []const u8,
    expected: DocumentSqlResidualExpectationJson,
};

pub const DocumentSqlUnsupportedExpressionCaseJson = struct {
    name: []const u8,
    sql: []const u8,
    expected_error: []const u8,
};

pub const DocumentSqlReadPlanExpectationJson = struct {
    producer: ?[]const u8 = null,
    aggregate_op: ?[]const u8 = null,
    aggregate_input: ?[]const u8 = null,
    group_by: ?[]const u8 = null,
    having: ?usize = null,
    order_by: ?[]const u8 = null,
    limit: ?u32 = null,
    native_query_json: ?[]const u8 = null,
    residual_filter_json: ?[]const u8 = null,
    filter_query_contains: []const []const u8 = &.{},
    native_query_contains: []const []const u8 = &.{},
    residual_filter_contains: []const []const u8 = &.{},
    max_candidate_rows: ?u32 = null,
    expected_error: ?[]const u8 = null,
};

pub const DocumentSqlReadPlanCaseJson = struct {
    name: []const u8,
    schema: []const u8,
    indexes_json: ?[]const u8 = null,
    bounded_scan_rows: ?u32 = null,
    sql: []const u8,
    expected: DocumentSqlReadPlanExpectationJson,
};

pub const DocumentSqlWritePlanExpectationJson = struct {
    plan: ?[]const u8 = null,
    action: ?[]const u8 = null,
    operation: ?[]const u8 = null,
    table_name: ?[]const u8 = null,
    producer: ?[]const u8 = null,
    target_producer: ?[]const u8 = null,
    source_producer: ?[]const u8 = null,
    template: ?[]const u8 = null,
    writes: ?usize = null,
    transforms: ?usize = null,
    deletes: ?usize = null,
    ops: ?usize = null,
    returning: ?usize = null,
    join_keys: ?usize = null,
    source_assignments: ?usize = null,
    conflict_source_assignments: ?usize = null,
    merge_insert_assignments: ?usize = null,
    generated_target_id: bool = false,
    expression_assignments: ?usize = null,
    where_expression: ?usize = null,
    where_expressions: ?usize = null,
    where_any: ?usize = null,
    where_not: ?usize = null,
    matched_arms: ?usize = null,
    not_matched_arms: ?usize = null,
    max_candidate_rows: ?u32 = null,
    source_limit: ?u32 = null,
    max_scan_rows: ?u32 = null,
    max_scan_bytes: ?u64 = null,
    max_target_rows: ?u32 = null,
    max_source_rows: ?u32 = null,
    filter_query_json: ?[]const u8 = null,
    residual_filter_json: ?[]const u8 = null,
    no_residual_filter: bool = false,
    expected_error: ?[]const u8 = null,
};

pub const DocumentSqlWritePlanCaseJson = struct {
    name: []const u8,
    schema: []const u8,
    sql: []const u8,
    expected: DocumentSqlWritePlanExpectationJson,
};

pub const DocumentSqlCorpusRootJson = struct {
    fixture_format: u32,
    residual_filter_cases: []const DocumentSqlResidualCaseJson = &.{},
    unsupported_residual_expression_cases: []const DocumentSqlUnsupportedExpressionCaseJson = &.{},
    document_write_plan_cases: []const DocumentSqlWritePlanCaseJson = &.{},
    document_read_plan_cases: []const DocumentSqlReadPlanCaseJson = &.{},
};

pub fn parseDocumentSqlCorpusAlloc(alloc: std.mem.Allocator) !std.json.Parsed(DocumentSqlCorpusRootJson) {
    var parsed = try std.json.parseFromSlice(DocumentSqlCorpusRootJson, alloc, document_sql_corpus_json, .{ .allocate = .alloc_always });
    errdefer parsed.deinit();
    if (parsed.value.fixture_format != document_sql_corpus_fixture_format) return error.InvalidSqlCorpusFixture;
    return parsed;
}

pub const ExpectedError = enum {
    DocumentSqlBoundedScanUnsupportedResidual,
    DocumentSqlBoundedScanPolicyRequired,
    DocumentSqlIndexUnavailable,
    DocumentSqlNativeSearchRequiresTableFunction,
    DocumentSqlLateralRequiresNativeProducer,
    DocumentSqlUnnestUnsupported,
    DocumentSqlUnnestRequiresArray,
    UnsupportedSqlShape,
    InvalidRowsRequest,
    InvalidSqlCatalog,
    DocumentSqlAggregateUnsupported,
    DocumentSqlBoundedScanMissingExactProducer,
    DocumentSqlWriteJoinMissingCardinalityProof,
    DocumentSqlWriteJoinMissingExactProducer,
    DocumentSqlWriteJoinMissingIndexProof,
    DocumentSqlWriteJoinOrderedIndexProof,
    DocumentSqlWriteJoinPartialIndexProof,
    DocumentSqlWriteJoinStaleIndexProof,
    DocumentSqlMergeRequiresNativeProducer,
    DocumentSqlWriteReturningAllUnsupported,
    DocumentSqlWriteReturningDuplicateOutput,
    DocumentSqlWriteReturningExpressionUnsupported,
    DocumentSqlWriteReturningGeneratedField,
    DocumentSqlWriteReturningVersionUnsupported,
    DocumentSqlWriteReturningVirtualFieldUnsupported,
    DocumentSqlWriteUnsupported,
};

pub fn errorFromName(name: []const u8) !ExpectedError {
    if (std.mem.eql(u8, name, "DocumentSqlBoundedScanUnsupportedResidual")) return .DocumentSqlBoundedScanUnsupportedResidual;
    if (std.mem.eql(u8, name, "DocumentSqlBoundedScanPolicyRequired")) return .DocumentSqlBoundedScanPolicyRequired;
    if (std.mem.eql(u8, name, "DocumentSqlIndexUnavailable")) return .DocumentSqlIndexUnavailable;
    if (std.mem.eql(u8, name, "DocumentSqlNativeSearchRequiresTableFunction")) return .DocumentSqlNativeSearchRequiresTableFunction;
    if (std.mem.eql(u8, name, "DocumentSqlLateralRequiresNativeProducer")) return .DocumentSqlLateralRequiresNativeProducer;
    if (std.mem.eql(u8, name, "DocumentSqlUnnestUnsupported")) return .DocumentSqlUnnestUnsupported;
    if (std.mem.eql(u8, name, "DocumentSqlUnnestRequiresArray")) return .DocumentSqlUnnestRequiresArray;
    if (std.mem.eql(u8, name, "UnsupportedSqlShape")) return .UnsupportedSqlShape;
    if (std.mem.eql(u8, name, "InvalidRowsRequest")) return .InvalidRowsRequest;
    if (std.mem.eql(u8, name, "InvalidSqlCatalog")) return .InvalidSqlCatalog;
    if (std.mem.eql(u8, name, "DocumentSqlAggregateUnsupported")) return .DocumentSqlAggregateUnsupported;
    if (std.mem.eql(u8, name, "DocumentSqlBoundedScanMissingExactProducer")) return .DocumentSqlBoundedScanMissingExactProducer;
    if (std.mem.eql(u8, name, "DocumentSqlWriteJoinMissingCardinalityProof")) return .DocumentSqlWriteJoinMissingCardinalityProof;
    if (std.mem.eql(u8, name, "DocumentSqlWriteJoinMissingExactProducer")) return .DocumentSqlWriteJoinMissingExactProducer;
    if (std.mem.eql(u8, name, "DocumentSqlWriteJoinMissingIndexProof")) return .DocumentSqlWriteJoinMissingIndexProof;
    if (std.mem.eql(u8, name, "DocumentSqlWriteJoinOrderedIndexProof")) return .DocumentSqlWriteJoinOrderedIndexProof;
    if (std.mem.eql(u8, name, "DocumentSqlWriteJoinPartialIndexProof")) return .DocumentSqlWriteJoinPartialIndexProof;
    if (std.mem.eql(u8, name, "DocumentSqlWriteJoinStaleIndexProof")) return .DocumentSqlWriteJoinStaleIndexProof;
    if (std.mem.eql(u8, name, "DocumentSqlMergeRequiresNativeProducer")) return .DocumentSqlMergeRequiresNativeProducer;
    if (std.mem.eql(u8, name, "DocumentSqlWriteReturningAllUnsupported")) return .DocumentSqlWriteReturningAllUnsupported;
    if (std.mem.eql(u8, name, "DocumentSqlWriteReturningDuplicateOutput")) return .DocumentSqlWriteReturningDuplicateOutput;
    if (std.mem.eql(u8, name, "DocumentSqlWriteReturningExpressionUnsupported")) return .DocumentSqlWriteReturningExpressionUnsupported;
    if (std.mem.eql(u8, name, "DocumentSqlWriteReturningGeneratedField")) return .DocumentSqlWriteReturningGeneratedField;
    if (std.mem.eql(u8, name, "DocumentSqlWriteReturningVersionUnsupported")) return .DocumentSqlWriteReturningVersionUnsupported;
    if (std.mem.eql(u8, name, "DocumentSqlWriteReturningVirtualFieldUnsupported")) return .DocumentSqlWriteReturningVirtualFieldUnsupported;
    if (std.mem.eql(u8, name, "DocumentSqlWriteUnsupported")) return .DocumentSqlWriteUnsupported;
    return error.InvalidSqlCorpusFixture;
}

pub fn errorValue(expected: ExpectedError) anyerror {
    return switch (expected) {
        .DocumentSqlBoundedScanUnsupportedResidual => error.DocumentSqlBoundedScanUnsupportedResidual,
        .DocumentSqlBoundedScanPolicyRequired => error.DocumentSqlBoundedScanPolicyRequired,
        .DocumentSqlIndexUnavailable => error.DocumentSqlIndexUnavailable,
        .DocumentSqlNativeSearchRequiresTableFunction => error.DocumentSqlNativeSearchRequiresTableFunction,
        .DocumentSqlLateralRequiresNativeProducer => error.DocumentSqlLateralRequiresNativeProducer,
        .DocumentSqlUnnestUnsupported => error.DocumentSqlUnnestUnsupported,
        .DocumentSqlUnnestRequiresArray => error.DocumentSqlUnnestRequiresArray,
        .UnsupportedSqlShape => error.UnsupportedSqlShape,
        .InvalidRowsRequest => error.InvalidRowsRequest,
        .InvalidSqlCatalog => error.InvalidSqlCatalog,
        .DocumentSqlAggregateUnsupported => error.DocumentSqlAggregateUnsupported,
        .DocumentSqlBoundedScanMissingExactProducer => error.DocumentSqlBoundedScanMissingExactProducer,
        .DocumentSqlWriteJoinMissingCardinalityProof => error.DocumentSqlWriteJoinMissingCardinalityProof,
        .DocumentSqlWriteJoinMissingExactProducer => error.DocumentSqlWriteJoinMissingExactProducer,
        .DocumentSqlWriteJoinMissingIndexProof => error.DocumentSqlWriteJoinMissingIndexProof,
        .DocumentSqlWriteJoinOrderedIndexProof => error.DocumentSqlWriteJoinOrderedIndexProof,
        .DocumentSqlWriteJoinPartialIndexProof => error.DocumentSqlWriteJoinPartialIndexProof,
        .DocumentSqlWriteJoinStaleIndexProof => error.DocumentSqlWriteJoinStaleIndexProof,
        .DocumentSqlMergeRequiresNativeProducer => error.DocumentSqlMergeRequiresNativeProducer,
        .DocumentSqlWriteReturningAllUnsupported => error.DocumentSqlWriteReturningAllUnsupported,
        .DocumentSqlWriteReturningDuplicateOutput => error.DocumentSqlWriteReturningDuplicateOutput,
        .DocumentSqlWriteReturningExpressionUnsupported => error.DocumentSqlWriteReturningExpressionUnsupported,
        .DocumentSqlWriteReturningGeneratedField => error.DocumentSqlWriteReturningGeneratedField,
        .DocumentSqlWriteReturningVersionUnsupported => error.DocumentSqlWriteReturningVersionUnsupported,
        .DocumentSqlWriteReturningVirtualFieldUnsupported => error.DocumentSqlWriteReturningVirtualFieldUnsupported,
        .DocumentSqlWriteUnsupported => error.DocumentSqlWriteUnsupported,
    };
}

test "document SQL corpus fixture parses" {
    var parsed = try parseDocumentSqlCorpusAlloc(std.testing.allocator);
    defer parsed.deinit();
    try std.testing.expect(parsed.value.residual_filter_cases.len > 0);
    try std.testing.expect(parsed.value.unsupported_residual_expression_cases.len > 0);
    try std.testing.expect(parsed.value.document_write_plan_cases.len > 0);
    try std.testing.expect(parsed.value.document_read_plan_cases.len > 0);
}
