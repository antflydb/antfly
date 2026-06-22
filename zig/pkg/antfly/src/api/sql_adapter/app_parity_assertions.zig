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
const db_mod = @import("../../storage/db/mod.zig");
const diagnostics = @import("diagnostics.zig");
const query_contract = @import("../query_contract.zig");
const query_function = @import("query_function.zig");

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
        error.UnsupportedSqlShape, error.InvalidSqlCatalog => return,
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
    var resolver_state: u8 = 0;
    const semantic_resolver = query_contract.SemanticResolver{
        .ptr = &resolver_state,
        .vtable = &.{ .resolve_dense_query = appParityResolveDenseQuery },
    };
    var lowered = try query_function.lowerAntflyQueryFunctionSqlAlloc(alloc, semantic_resolver, entry.sql);
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
