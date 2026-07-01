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

pub const DocumentSqlCorpusRootJson = struct {
    fixture_format: u32,
    residual_filter_cases: []const DocumentSqlResidualCaseJson = &.{},
    unsupported_residual_expression_cases: []const DocumentSqlUnsupportedExpressionCaseJson = &.{},
    document_read_plan_cases: []const DocumentSqlReadPlanCaseJson = &.{},
};

pub fn parseDocumentSqlCorpusAlloc(alloc: std.mem.Allocator) !std.json.Parsed(DocumentSqlCorpusRootJson) {
    var parsed = try std.json.parseFromSlice(DocumentSqlCorpusRootJson, alloc, document_sql_corpus_json, .{ .allocate = .alloc_always });
    errdefer parsed.deinit();
    if (parsed.value.fixture_format != document_sql_corpus_fixture_format) return error.InvalidSqlCorpusFixture;
    return parsed;
}

pub fn errorFromName(name: []const u8) !anyerror {
    if (std.mem.eql(u8, name, "DocumentSqlBoundedScanUnsupportedResidual")) return error.DocumentSqlBoundedScanUnsupportedResidual;
    if (std.mem.eql(u8, name, "DocumentSqlBoundedScanPolicyRequired")) return error.DocumentSqlBoundedScanPolicyRequired;
    if (std.mem.eql(u8, name, "DocumentSqlIndexUnavailable")) return error.DocumentSqlIndexUnavailable;
    if (std.mem.eql(u8, name, "DocumentSqlNativeSearchRequiresTableFunction")) return error.DocumentSqlNativeSearchRequiresTableFunction;
    if (std.mem.eql(u8, name, "DocumentSqlUnnestRequiresArray")) return error.DocumentSqlUnnestRequiresArray;
    if (std.mem.eql(u8, name, "UnsupportedSqlShape")) return error.UnsupportedSqlShape;
    if (std.mem.eql(u8, name, "InvalidRowsRequest")) return error.InvalidRowsRequest;
    if (std.mem.eql(u8, name, "InvalidSqlCatalog")) return error.InvalidSqlCatalog;
    return error.InvalidSqlCorpusFixture;
}

test "document SQL corpus fixture parses" {
    var parsed = try parseDocumentSqlCorpusAlloc(std.testing.allocator);
    defer parsed.deinit();
    try std.testing.expect(parsed.value.residual_filter_cases.len > 0);
    try std.testing.expect(parsed.value.unsupported_residual_expression_cases.len > 0);
    try std.testing.expect(parsed.value.document_read_plan_cases.len > 0);
}
