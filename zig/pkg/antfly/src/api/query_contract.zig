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
const db_mod = @import("../storage/db/mod.zig");
const query_contract = @import("../query/contract.zig");

pub const QueryResponse = query_contract.QueryResponse;
pub const testing = struct {
    pub fn bodyHasInternalShardFields(alloc: std.mem.Allocator, body: []const u8) !bool {
        return query_contract.testing.bodyHasInternalShardFields(alloc, body);
    }

    pub fn bodyHasForbiddenPublicDocIdentityControls(alloc: std.mem.Allocator, body: []const u8) !bool {
        return query_contract.testing.bodyHasForbiddenPublicDocIdentityControls(alloc, body);
    }

    pub fn bodyHasPublicDocFilterBindings(alloc: std.mem.Allocator, body: []const u8) !bool {
        return query_contract.testing.bodyHasPublicDocFilterBindings(alloc, body);
    }

    pub fn expectPublicExactSortRejectionMapping() !void {
        try expectPublicExactSortRejectionMappingForTest();
    }

    pub fn expectFilterOnlyQueryStringFilterPreserved() !void {
        var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
            \\{"filter_query":{"query":"status:active"},"limit":5}
        );
        defer owned.deinit(std.testing.allocator);

        try std.testing.expect(owned.req.full_text != null);
        try std.testing.expect(owned.req.full_text.? == .match_all);
        try std.testing.expect(std.mem.indexOf(u8, owned.req.filter_query_json, "\"status\":\"active\"") != null);
    }
};
pub const QueryResponseMeta = query_contract.QueryResponseMeta;
pub const NativeDocIdConstraintEnvelope = query_contract.NativeDocIdConstraintEnvelope;
pub const OwnedNativeDocIdConstraintEnvelope = query_contract.OwnedNativeDocIdConstraintEnvelope;
pub const AlgebraicVectorWorkerRequestOptions = query_contract.AlgebraicVectorWorkerRequestOptions;
pub const OwnedAlgebraicVectorWorkerRequestEnvelope = query_contract.OwnedAlgebraicVectorWorkerRequestEnvelope;
pub const AlgebraicVectorWorkerQuery = query_contract.AlgebraicVectorWorkerQuery;
pub const OwnedAlgebraicVectorWorkerQuery = query_contract.OwnedAlgebraicVectorWorkerQuery;
pub const AlgebraicTensorAccessPathEnvelopeInput = query_contract.AlgebraicTensorAccessPathEnvelopeInput;
pub const AlgebraicDictionaryIdentityInput = query_contract.AlgebraicDictionaryIdentityInput;
pub const AlgebraicTensorExprEnvelopeInput = query_contract.AlgebraicTensorExprEnvelopeInput;
pub const AlgebraicTensorProgramRefInput = query_contract.AlgebraicTensorProgramRefInput;
pub const AlgebraicTensorProgramStepEnvelopeInput = query_contract.AlgebraicTensorProgramStepEnvelopeInput;
pub const AlgebraicTensorProgramEnvelopeInput = query_contract.AlgebraicTensorProgramEnvelopeInput;
pub const OwnedAlgebraicTensorAccessPathEnvelope = query_contract.OwnedAlgebraicTensorAccessPathEnvelope;
pub const OwnedAlgebraicDictionaryIdentity = query_contract.OwnedAlgebraicDictionaryIdentity;
pub const OwnedAlgebraicTensorExprEnvelope = query_contract.OwnedAlgebraicTensorExprEnvelope;
pub const OwnedAlgebraicTensorProgramStepEnvelope = query_contract.OwnedAlgebraicTensorProgramStepEnvelope;
pub const OwnedAlgebraicTensorProgramEnvelope = query_contract.OwnedAlgebraicTensorProgramEnvelope;
pub const OwnedAlgebraicTensorProgramView = query_contract.OwnedAlgebraicTensorProgramView;
pub const OwnedQueryRequest = query_contract.OwnedQueryRequest;
pub const QueryPreflightSummary = query_contract.QueryPreflightSummary;
pub const SemanticResolver = query_contract.SemanticResolver;

pub const nativeDocIdConstraintEnvelopeFromSearchRequest = query_contract.nativeDocIdConstraintEnvelopeFromSearchRequest;
pub const encodeAlgebraicTensorAccessPathEnvelopeAlloc = query_contract.encodeAlgebraicTensorAccessPathEnvelopeAlloc;
pub const parseAlgebraicTensorAccessPathEnvelopeAlloc = query_contract.parseAlgebraicTensorAccessPathEnvelopeAlloc;
pub const encodeAlgebraicTensorExprEnvelopeAlloc = query_contract.encodeAlgebraicTensorExprEnvelopeAlloc;
pub const encodeAlgebraicTensorProgramEnvelopeAlloc = query_contract.encodeAlgebraicTensorProgramEnvelopeAlloc;
pub const parseAlgebraicTensorExprEnvelopeAlloc = query_contract.parseAlgebraicTensorExprEnvelopeAlloc;
pub const parseAlgebraicTensorExprEnvelopeInputAlloc = query_contract.parseAlgebraicTensorExprEnvelopeInputAlloc;
pub const parseAlgebraicTensorProgramEnvelopeAlloc = query_contract.parseAlgebraicTensorProgramEnvelopeAlloc;
pub const parseAlgebraicTensorProgramEnvelopeInputAlloc = query_contract.parseAlgebraicTensorProgramEnvelopeInputAlloc;
pub const encodeAlgebraicVectorWorkerRequestEnvelopeAlloc = query_contract.encodeAlgebraicVectorWorkerRequestEnvelopeAlloc;
pub const parseAlgebraicVectorWorkerRequestEnvelopeAlloc = query_contract.parseAlgebraicVectorWorkerRequestEnvelopeAlloc;
pub const parseAlgebraicTensorAccessPathEnvelopeInputAlloc = query_contract.parseAlgebraicTensorAccessPathEnvelopeInputAlloc;
pub const applyNativeDocIdConstraintEnvelope = query_contract.applyNativeDocIdConstraintEnvelope;
pub const encodeNativeDocIdConstraintEnvelopeAlloc = query_contract.encodeNativeDocIdConstraintEnvelopeAlloc;
pub const parseNativeDocIdConstraintEnvelopeAlloc = query_contract.parseNativeDocIdConstraintEnvelopeAlloc;
pub const parseQueryRequest = query_contract.parseQueryRequest;
pub const parsePublicQueryRequest = query_contract.parsePublicQueryRequest;
pub const queryExecutionDeadlineNsFromBody = query_contract.queryExecutionDeadlineNsFromBody;
pub const preflightGraphSearchesAlloc = query_contract.preflightGraphSearchesAlloc;
pub const queryRequestHasScoreBearingTextSourceAlloc = query_contract.queryRequestHasScoreBearingTextSourceAlloc;
pub const queryRequestHasScoreBearingSourceAlloc = query_contract.queryRequestHasScoreBearingSourceAlloc;
pub const preflightQueryRequestAlloc = query_contract.preflightQueryRequestAlloc;
pub const parseTotalHitsRelation = query_contract.parseTotalHitsRelation;
pub const totalHitsRelationString = query_contract.totalHitsRelationString;
pub const encodeQueryResponses = query_contract.encodeQueryResponses;
pub const parseAggregationRequestsJson = query_contract.parseAggregationRequestsJson;
pub const freeAggregationRequests = query_contract.freeAggregationRequests;
pub const encodeSupportedPatternFilterQueryAlloc = query_contract.encodeSupportedPatternFilterQueryAlloc;
pub const normalizePublicFilterQueryAlloc = query_contract.normalizePublicFilterQueryAlloc;

pub const PublicExactSortRejection = struct {
    reason: []const u8,
    detail: []const u8,
};

pub fn queryHitsTotalValueToU32(total: anytype) !u32 {
    if (total.value < 0) return error.InvalidQueryRequest;
    return std.math.cast(u32, total.value) orelse error.InvalidQueryRequest;
}

const public_exact_sort_reasons = [_][]const u8{
    "unmapped_field",
    "non_sortable_field",
    "unsupported_sort_field",
    "mixed_field_type",
    "field_not_sort_ready",
    "filter_not_queryable",
    "invalid_cursor_arity",
    "invalid_cursor_type",
    "invalid_sort_tuple",
    "approximate_candidate_source",
    "candidate_budget_exceeded",
    "missing_null_policy",
    "non_score_bearing_source",
    "invalid_score_value",
    "count_only_ordered_page",
    "stored_json_sort_disabled",
    "unsupported_exact_sort",
    "distributed_merge_unsupported",
};

pub fn publicExactSortRejection(reason: []const u8, detail: []const u8) PublicExactSortRejection {
    const public_reason = publicExactSortReason(reason, detail);
    return .{
        .reason = public_reason,
        .detail = publicExactSortDetail(public_reason, detail),
    };
}

pub fn validatePublicQuerySortTupleContract(alloc: std.mem.Allocator, body: []const u8) !void {
    if (std.mem.indexOf(u8, body, "\"order_by\"") == null) return;

    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{}) catch return error.InvalidQueryRequest;
    defer parsed.deinit();
    const object = switch (parsed.value) {
        .object => |object| object,
        else => return,
    };
    const order_by = object.get("order_by") orelse return;
    if (order_by == .null) return;
    if (order_by != .array) return recordInvalidSortTuple("*");

    for (order_by.array.items) |item| {
        if (item != .object) return recordInvalidSortTuple("*");
        const field = publicSortTupleFieldName(item.object) orelse "*";
        var it = item.object.iterator();
        while (it.next()) |entry| {
            const key = entry.key_ptr.*;
            if (std.mem.eql(u8, key, "field") or std.mem.eql(u8, key, "desc")) continue;
            return recordInvalidSortTuple(field);
        }
    }
}

fn publicSortTupleFieldName(object: std.json.ObjectMap) ?[]const u8 {
    const field = object.get("field") orelse return null;
    return switch (field) {
        .string => |value| value,
        else => null,
    };
}

fn recordInvalidSortTuple(field: []const u8) error{InvalidQueryRequest} {
    db_mod.recordSortRejectionDiagnostic(field, "invalid_sort_tuple", "invalid_sort_tuple");
    return error.InvalidQueryRequest;
}

pub fn publicExactSortReason(reason: []const u8, detail: []const u8) []const u8 {
    if (std.mem.eql(u8, reason, "missing_doc_values_coverage")) return "field_not_sort_ready";
    if (std.mem.eql(u8, reason, "missing_native_filter_coverage")) return "filter_not_queryable";
    if (std.mem.eql(u8, reason, "unmapped_sort_field")) return "unmapped_field";
    if (std.mem.eql(u8, reason, "non_sortable_sort_field")) {
        if (std.mem.eql(u8, detail, "non_scalar_field")) return "unsupported_sort_field";
        if (std.mem.eql(u8, detail, "mixed_field_type")) return "mixed_field_type";
        return "non_sortable_field";
    }
    if (std.mem.eql(u8, reason, "invalid_doc_value_type") and
        std.mem.eql(u8, detail, "mixed_sort_value_domain"))
    {
        return "mixed_field_type";
    }
    if (std.mem.eql(u8, reason, "invalid_doc_value_type") or std.mem.eql(u8, reason, "missing_runtime_mapping")) {
        return "unsupported_sort_field";
    }
    if (std.mem.eql(u8, reason, "unsupported_exact_sort") and publicExactSortReasonIsStable(detail)) return detail;
    if (publicExactSortReasonIsStable(reason)) return reason;
    return "unsupported_exact_sort";
}

fn publicExactSortReasonIsStable(reason: []const u8) bool {
    for (public_exact_sort_reasons) |stable_reason| {
        if (std.mem.eql(u8, reason, stable_reason)) return true;
    }
    return false;
}

fn publicExactSortDetail(public_reason: []const u8, detail: []const u8) []const u8 {
    _ = detail;
    return public_reason;
}

fn expectPublicExactSortRejectionMappingForTest() !void {
    const missing_doc_values = publicExactSortRejection("missing_doc_values_coverage", "missing_doc_values_section");
    try std.testing.expectEqualStrings("field_not_sort_ready", missing_doc_values.reason);
    try std.testing.expectEqualStrings("field_not_sort_ready", missing_doc_values.detail);

    const missing_filter = publicExactSortRejection("missing_native_filter_coverage", "native_filter_doc_nums_missing");
    try std.testing.expectEqualStrings("filter_not_queryable", missing_filter.reason);
    try std.testing.expectEqualStrings("filter_not_queryable", missing_filter.detail);

    const non_sortable = publicExactSortRejection("non_sortable_sort_field", "non_scalar_field");
    try std.testing.expectEqualStrings("unsupported_sort_field", non_sortable.reason);
    try std.testing.expectEqualStrings("unsupported_sort_field", non_sortable.detail);

    const mixed_sort_domain = publicExactSortRejection("invalid_doc_value_type", "mixed_sort_value_domain");
    try std.testing.expectEqualStrings("mixed_field_type", mixed_sort_domain.reason);
    try std.testing.expectEqualStrings("mixed_field_type", mixed_sort_domain.detail);

    const public_reason = publicExactSortRejection("invalid_cursor_arity", "sort_tuple_arity");
    try std.testing.expectEqualStrings("invalid_cursor_arity", public_reason.reason);
    try std.testing.expectEqualStrings("invalid_cursor_arity", public_reason.detail);

    const count_only = publicExactSortRejection("unsupported_exact_sort", "count_only_ordered_page");
    try std.testing.expectEqualStrings("count_only_ordered_page", count_only.reason);
    try std.testing.expectEqualStrings("count_only_ordered_page", count_only.detail);

    for (public_exact_sort_reasons) |stable_reason| {
        const direct = publicExactSortRejection(stable_reason, "internal_detail");
        try std.testing.expectEqualStrings(stable_reason, direct.reason);
        try std.testing.expectEqualStrings(stable_reason, direct.detail);

        const promoted = publicExactSortRejection("unsupported_exact_sort", stable_reason);
        try std.testing.expectEqualStrings(stable_reason, promoted.reason);
        try std.testing.expectEqualStrings(stable_reason, promoted.detail);
    }

    const unknown_internal = publicExactSortRejection("missing_private_planner_state", "private_detail");
    try std.testing.expectEqualStrings("unsupported_exact_sort", unknown_internal.reason);
    try std.testing.expectEqualStrings("unsupported_exact_sort", unknown_internal.detail);
}

test "public query sort tuple contract rejects unknown order_by properties" {
    const alloc = std.testing.allocator;
    db_mod.resetLastSortRejectionDiagnostic();
    try std.testing.expectError(error.InvalidQueryRequest, validatePublicQuerySortTupleContract(
        alloc,
        "{\"order_by\":[{\"field\":\"created_at\",\"descc\":true}]}",
    ));

    const diagnostic = db_mod.takeLastSortRejectionDiagnostic() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("created_at", diagnostic.field);
    try std.testing.expectEqualStrings("invalid_sort_tuple", diagnostic.reason);
    try std.testing.expectEqualStrings("invalid_sort_tuple", diagnostic.detail);
}

test "public query sort tuple contract accepts known order_by properties" {
    const alloc = std.testing.allocator;
    db_mod.resetLastSortRejectionDiagnostic();
    try validatePublicQuerySortTupleContract(
        alloc,
        "{\"order_by\":[{\"field\":\"created_at\",\"desc\":true}]}",
    );
    try std.testing.expect(db_mod.peekLastSortRejectionDiagnostic() == null);
}
