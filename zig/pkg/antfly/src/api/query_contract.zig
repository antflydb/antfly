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
const builtin = @import("builtin");
const ant_json = @import("antfly-json");
const db_mod = @import("../storage/db/mod.zig");
const document_query = @import("../storage/db/document_query.zig");
const hierarchy_navigation = @import("../storage/hierarchy_navigation.zig");
const graph_edge_type = @import("../graph/edge_type.zig");
const graph_edge_weight = @import("../graph/edge_weight.zig");
const graph_pattern_mod = @import("../graph/pattern.zig");
const graph_paths_mod = @import("../graph/paths.zig");
const graph_traversal_mod = @import("../graph/traversal.zig");
const graph_path_weight_diagnostic = @import("../graph/path_weight_diagnostic.zig");
const graph_query_mod = @import("../graph/query.zig");
const graph_mod = @import("../graph/graph.zig");
const graph_node_identity = @import("../graph/node_identity.zig");
const rfc3339 = @import("../common/rfc3339.zig");
const fusion_mod = @import("../search/fusion.zig");
const aggregations_mod = @import("../storage/db/aggregations.zig");
const public_search_request_mod = @import("public_search_request.zig");
const public_text_query_mod = @import("public_text_query.zig");
const public_query_string_mod = @import("public_query_string.zig");
const public_limits = @import("public_limits.zig");
const graph_wire_envelope = @import("graph_wire_envelope.zig");
const indexes_openapi = @import("antfly_indexes_openapi");
const metadata_openapi = @import("antfly_metadata_openapi");
const query_openapi = @import("antfly_query_openapi");
const reranking_mod = @import("antfly_reranking");
const vector_codec = @import("antfly_vector").codec;
const platform_time = @import("antfly_platform").time;
const algebraic_ir = db_mod.algebraic.ir;
const algebraic_law = db_mod.algebraic.law;
const algebraic_lexical = db_mod.algebraic.lexical;
const public_query_max_tree_depth: usize = 64;
const public_query_max_tree_nodes: usize = 16 * 1024;
pub const QueryResponse = @import("query_response.zig").QueryResponse;

pub const testing = if (builtin.is_test) struct {
    pub fn bodyHasInternalShardFields(alloc: std.mem.Allocator, body: []const u8) !bool {
        return queryBodyHasInternalShardFields(alloc, body);
    }

    pub fn bodyHasForbiddenPublicDocIdentityControls(alloc: std.mem.Allocator, body: []const u8) !bool {
        return queryBodyHasForbiddenPublicDocIdentityControlFields(alloc, body);
    }

    pub fn bodyHasPublicDocFilterBindings(alloc: std.mem.Allocator, body: []const u8) !bool {
        return queryBodyHasPublicDocFilterBindings(alloc, body);
    }

    pub fn expectSortProfileDiagnosticsSerialization() !void {
        return expectSortProfileDiagnosticsSerializationForTest();
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
        try std.testing.expect(std.mem.indexOf(u8, owned.req.filter_query_json, "\"path\":\"status\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, owned.req.filter_query_json, "\"text\":\"active\"") != null);
    }
} else struct {};

pub fn totalHitsRelationString(relation: db_mod.types.TotalHitsRelation) []const u8 {
    return switch (relation) {
        .exact => "exact",
        .gte => "gte",
    };
}

pub const PublicExactSortRejection = struct {
    reason: []const u8,
    detail: []const u8,
};

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
    recordUnsupportedExactSortDiagnostic(field, "invalid_sort_tuple", "invalid_sort_tuple");
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

pub fn parseTotalHitsRelation(value: []const u8) !db_mod.types.TotalHitsRelation {
    if (std.mem.eql(u8, value, "exact")) return .exact;
    if (std.mem.eql(u8, value, "gte")) return .gte;
    return error.InvalidQueryRequest;
}

pub fn queryHitsTotalFromSearchResult(result: db_mod.types.SearchResult) metadata_openapi.QueryHitsTotal {
    return .{
        .value = result.total_hits,
        .relation = totalHitsRelationString(result.total_hits_relation),
    };
}

pub fn queryHitsTotalValueToU32(total: metadata_openapi.QueryHitsTotal) !u32 {
    if (total.value < 0) return error.InvalidQueryRequest;
    return std.math.cast(u32, total.value) orelse error.InvalidQueryRequest;
}

pub const QueryResponseMeta = struct {
    pub const RerankerProfile = struct {
        model: []const u8 = "",
        documents_reranked: u32 = 0,
        duration_ms: i64 = 0,
    };

    pub const MergeProfile = struct {
        strategy: ?indexes_openapi.MergeStrategy = null,
        full_text_hits: u32 = 0,
        semantic_hits: u32 = 0,
        duration_ms: i64 = 0,
    };

    pub const DenseSearchProfile = struct {
        pub const DebugHit = struct {
            id: u64 = 0,
            distance: f32 = 0,
            error_bound: f32 = 0,
            lower_bound: f32 = 0,
            upper_bound: f32 = 0,
        };

        pub const DebugPair = struct {
            left: DebugHit = .{},
            right: DebugHit = .{},
            distance_gap: f32 = 0,
            interval_gap: f32 = 0,
            overlaps: bool = false,
        };

        total_ns: u64 = 0,
        index_lookup_ns: u64 = 0,
        hbc_search_ns: u64 = 0,
        hbc_runtime_txn_ns: u64 = 0,
        hbc_scratch_acquire_ns: u64 = 0,
        hbc_node_cache_lookup_ns: u64 = 0,
        hbc_quantized_cache_lookup_ns: u64 = 0,
        hbc_filter_candidates: u64 = 0,
        hbc_filter_rejected: u64 = 0,
        hbc_filter_metadata_batches: u64 = 0,
        hbc_filter_metadata_batch_ns: u64 = 0,
        hbc_traversal_waves: u64 = 0,
        hbc_traversal_initial_wave_leaves: u64 = 0,
        hbc_traversal_max_wave_leaves: u64 = 0,
        hbc_traversal_bound_resolutions: u64 = 0,
        hbc_traversal_bound_fallbacks: u64 = 0,
        hbc_traversal_bound_stops: u64 = 0,
        hbc_traversal_frontier_remaining: u64 = 0,
        hbc_traversal_eligible_vectors: u64 = 0,
        hbc_traversal_stop_lower_bound: f32 = 0,
        hbc_traversal_stop_result_upper_bound: f32 = 0,
        resolved_search_width: u32 = 0,
        resolved_epsilon: f32 = 0,
        native_filter_candidate_count: u64 = 0,
        search_route: []const u8 = "",
        route_reason: []const u8 = "",
        route_estimated_exact_storage_bytes: u64 = 0,
        route_estimated_hbc_storage_bytes: u64 = 0,
        route_estimated_exact_work_ns: u64 = 0,
        route_estimated_hbc_work_ns: u64 = 0,
        exact_candidate_count: u64 = 0,
        exact_batch_count: u64 = 0,
        exact_max_batch_size: u64 = 0,
        exact_workspace_bytes: u64 = 0,
        exact_request_vector_cache_entries: u64 = 0,
        exact_raw_batch_reads: u64 = 0,
        exact_raw_scalar_reads: u64 = 0,
        exact_missing_vectors: u64 = 0,
        exact_candidate_prepare_ns: u64 = 0,
        exact_metadata_lookup_ns: u64 = 0,
        exact_artifact_key_ns: u64 = 0,
        exact_artifact_read_ns: u64 = 0,
        exact_artifact_decode_ns: u64 = 0,
        exact_distance_ns: u64 = 0,
        exact_lsm_cache_hits: u64 = 0,
        exact_lsm_cache_misses: u64 = 0,
        exact_artifact_cache_hits: u64 = 0,
        exact_artifact_vectors_loaded: u64 = 0,
        hbc_nodes_visited: u64 = 0,
        hbc_leaves_explored: u64 = 0,
        hbc_approx_vectors_scored: u64 = 0,
        hbc_exact_vectors_scored: u64 = 0,
        hbc_reranked_vectors: u64 = 0,
        hbc_approx_candidate_count: u64 = 0,
        hbc_rerank_candidate_count: u64 = 0,
        hbc_rerank_batches: u64 = 0,
        hbc_rerank_max_batch_size: u64 = 0,
        hbc_rerank_candidates_skipped_by_bound: u64 = 0,
        hbc_ambiguous_top_k_pairs: u64 = 0,
        hbc_ambiguous_boundary_pairs: u64 = 0,
        hbc_ambiguous_distance_over_hits: u64 = 0,
        hbc_ambiguous_distance_under_hits: u64 = 0,
        hbc_full_rerank_due_to_threshold: bool = false,
        hbc_top_k_count: u64 = 0,
        hbc_min_distance_gap_top_k: f32 = 0,
        hbc_min_interval_gap_top_k: f32 = 0,
        hbc_closest_pair_top_k: ?DebugPair = null,
        hbc_boundary_pair: ?DebugPair = null,
        hbc_boundary_tail_error_avg: f32 = 0,
        hbc_boundary_tail_error_max: f32 = 0,
        hbc_boundary_tail_distance_gap_avg: f32 = 0,
        hbc_boundary_tail_distance_gap_min: f32 = 0,
        hbc_boundary_tail_distance_gap_max: f32 = 0,
        hbc_boundary_tail_interval_gap_avg: f32 = 0,
        hbc_boundary_tail_interval_gap_min: f32 = 0,
        hbc_boundary_tail_interval_gap_max: f32 = 0,
        hbc_approx_top_count: u64 = 0,
        hbc_approx_top: [5]DebugHit = .{ .{}, .{}, .{}, .{}, .{} },
        hbc_rerank_external_score_ns: u64 = 0,
        hbc_rerank_vector_load_ns: u64 = 0,
        hbc_rerank_metadata_lookup_ns: u64 = 0,
        hbc_rerank_metadata_vectors_loaded: u64 = 0,
        hbc_rerank_artifact_key_ns: u64 = 0,
        hbc_rerank_artifact_read_ns: u64 = 0,
        hbc_rerank_artifact_decode_ns: u64 = 0,
        hbc_rerank_artifact_distance_ns: u64 = 0,
        hbc_rerank_lsm_cache_hits: u64 = 0,
        hbc_rerank_lsm_cache_misses: u64 = 0,
        hbc_rerank_artifact_cache_hits: u64 = 0,
        hbc_rerank_artifact_vectors_loaded: u64 = 0,
        hbc_rerank_distance_ns: u64 = 0,
        doc_key_resolve_ns: u64 = 0,
        doc_ordinal_lookup_ns: u64 = 0,
        load_projected_document_ns: u64 = 0,
        postprocess_ns: u64 = 0,
        raw_hit_count: u32 = 0,
        returned_hit_count: u32 = 0,
        inline_metadata_hits: u32 = 0,
        fetched_metadata_hits: u32 = 0,
        lookup_doc_key_hits: u32 = 0,
    };

    took_ms: i64 = 0,
    shard_count: u32 = 1,
    merged: bool = false,
    reranker: ?RerankerProfile = null,
    merge: ?MergeProfile = null,
    dense_search: ?DenseSearchProfile = null,
    aggregation_results: []aggregations_mod.SearchAggregationResult = &.{},

    pub fn deinit(self: *QueryResponseMeta, alloc: std.mem.Allocator) void {
        aggregations_mod.deinitResults(alloc, self.aggregation_results);
        self.* = undefined;
    }
};

/// Transitional public response shaping. This remains API-local; graph
/// executors and storage graph plans always use the canonical IR.
const GraphResponseFormat = graph_wire_envelope.Dialect;

fn graphResponseFormat(req: db_mod.types.SearchRequest) !GraphResponseFormat {
    const transport = req.graph_query_transport orelse return error.InvalidRemoteResponse;
    if (!transport.matchesOperations(req.graph_queries)) return error.InvalidRemoteResponse;
    return transport.dialect;
}

test "graph response format uses admitted metadata and fails closed on plan drift" {
    const queries = [_]db_mod.types.NamedGraphQuery{.{
        .name = "walk",
        .query = .{
            .query_type = .traverse,
            .index_name = "graph_idx",
            .start_nodes = .{ .keys = &.{"doc:a"} },
        },
    }};
    try std.testing.expectEqual(GraphResponseFormat.canonical, try graphResponseFormat(
        .{
            .graph_queries = &queries,
            .graph_query_transport = .{
                .dialect = .canonical,
                .operations_json = "{\"walk\":{}}",
                .admitted_operations_ptr = @ptrCast(queries[0..].ptr),
                .admitted_operations_len = queries.len,
            },
        },
    ));
    try std.testing.expectEqual(GraphResponseFormat.legacy, try graphResponseFormat(
        .{
            .graph_queries = &queries,
            .graph_query_transport = .{
                .dialect = .legacy,
                .operations_json = "{\"walk\":{}}",
                .admitted_operations_ptr = @ptrCast(queries[0..].ptr),
                .admitted_operations_len = queries.len,
            },
        },
    ));
    try std.testing.expectError(error.InvalidRemoteResponse, graphResponseFormat(
        .{
            .graph_queries = &queries,
            .graph_query_transport = .{
                .dialect = .canonical,
                .operations_json = "{}",
                .admitted_operations_ptr = @ptrCast(queries[0..].ptr),
                .admitted_operations_len = 0,
            },
        },
    ));

    var same_name_different_plan = queries;
    same_name_different_plan[0].query.index_name = "other_graph_idx";
    try std.testing.expectError(error.InvalidRemoteResponse, graphResponseFormat(
        .{
            .graph_queries = &same_name_different_plan,
            .graph_query_transport = .{
                .dialect = .canonical,
                .operations_json = "{\"walk\":{}}",
                .admitted_operations_ptr = @ptrCast(queries[0..].ptr),
                .admitted_operations_len = queries.len,
            },
        },
    ));
}

fn appendJsonFieldName(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
) !void {
    if (!first.*) try out.append(alloc, ',');
    first.* = false;
    try appendJsonString(alloc, out, name);
    try out.append(alloc, ':');
}

fn appendJsonFieldString(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    value: []const u8,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try appendJsonString(alloc, out, value);
}

fn appendJsonFieldBool(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    value: bool,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try out.appendSlice(alloc, if (value) "true" else "false");
}

fn appendJsonFieldUsize(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    value: usize,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    const rendered = try std.fmt.allocPrint(alloc, "{d}", .{value});
    defer alloc.free(rendered);
    try out.appendSlice(alloc, rendered);
}

fn appendJsonFieldU64(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    value: u64,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    const rendered = try std.fmt.allocPrint(alloc, "{d}", .{value});
    defer alloc.free(rendered);
    try out.appendSlice(alloc, rendered);
}

fn appendJsonString(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    const escaped = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
    defer alloc.free(escaped);
    try out.appendSlice(alloc, escaped);
}

fn freeOwnedStringItems(alloc: std.mem.Allocator, values: []const []const u8) void {
    for (values) |value| alloc.free(@constCast(value));
}

fn freeOwnedStringSlice(alloc: std.mem.Allocator, values: []const []const u8) void {
    freeOwnedStringItems(alloc, values);
    if (values.len > 0) alloc.free(@constCast(values));
}

pub const NativeDocIdConstraintEnvelope = struct {
    positive_filter: bool = false,
    include_doc_ids: []const []const u8 = &.{},
    exclude_doc_ids: []const []const u8 = &.{},

    pub fn hasConstraints(self: @This()) bool {
        return self.positive_filter or self.include_doc_ids.len > 0 or self.exclude_doc_ids.len > 0;
    }
};

pub const OwnedNativeDocIdConstraintEnvelope = struct {
    constraints: NativeDocIdConstraintEnvelope,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        freeOwnedStringSlice(alloc, self.constraints.include_doc_ids);
        freeOwnedStringSlice(alloc, self.constraints.exclude_doc_ids);
        self.* = undefined;
    }
};

pub const AlgebraicVectorWorkerRequestOptions = struct {
    fields: [][]const u8 = &.{},
    filter_prefix: []const u8 = "",
    filter_query_json: []const u8 = "",
    exclusion_query_json: []const u8 = "",
    filter_ids: []const u64 = &.{},
    exclude_ids: []const u64 = &.{},
    require_algebraic_filter_resolution: bool = false,
    include_all_fields: bool = true,
    defer_stored_projection: bool = false,
    limit: u32 = 10,
    offset: u32 = 0,
    count_only: bool = false,
    profile: bool = false,
    include_stored: bool = true,
    search_effort: ?f32 = null,
    distance_over: ?f32 = null,
    distance_under: ?f32 = null,
    return_mode: db_mod.types.ReturnMode = .parent,
    max_chunks_per_parent: u32 = 0,
    hierarchy_include_source: bool = false,
    hierarchy_include_unit: bool = false,
    hierarchy_omit_implicit_source_ancestor_document: bool = false,
    hierarchy_match_fields: [][]const u8 = &.{},
    hierarchy_match_include_all_fields: bool = true,
    hierarchy_grouped_matches: bool = false,
    hierarchy_group_level: db_mod.types.HierarchyGroupLevel = .source,
    defer_hierarchy_child_hydration: bool = false,
    hierarchy_source_fields: [][]const u8 = &.{},
    hierarchy_source_include_all_fields: bool = true,
    hierarchy_unit_fields: [][]const u8 = &.{},
    hierarchy_unit_include_all_fields: bool = true,
    identity_read_generation: ?u64 = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.fields) |field| alloc.free(field);
        if (self.fields.len > 0) alloc.free(self.fields);
        freeOwnedStringSlice(alloc, self.hierarchy_match_fields);
        freeOwnedStringSlice(alloc, self.hierarchy_source_fields);
        freeOwnedStringSlice(alloc, self.hierarchy_unit_fields);
        if (self.filter_prefix.len > 0) alloc.free(@constCast(self.filter_prefix));
        if (self.filter_query_json.len > 0) alloc.free(@constCast(self.filter_query_json));
        if (self.exclusion_query_json.len > 0) alloc.free(@constCast(self.exclusion_query_json));
        if (self.filter_ids.len > 0) alloc.free(@constCast(self.filter_ids));
        if (self.exclude_ids.len > 0) alloc.free(@constCast(self.exclude_ids));
        self.* = undefined;
    }
};

pub const OwnedAlgebraicVectorWorkerRequestEnvelope = struct {
    index_name: []u8,
    layout: algebraic_ir.PhysicalLayout,
    query: OwnedAlgebraicVectorWorkerQuery,
    options: AlgebraicVectorWorkerRequestOptions = .{},
    native_doc_id_constraints: OwnedNativeDocIdConstraintEnvelope,
    resolved_doc_filter: ?*const anyopaque = null,
    resolved_doc_filter_wire_context: ?db_mod.types.ResolvedDocFilterWireContext = null,
    tensor_access_paths: []OwnedAlgebraicTensorAccessPathEnvelope,
    tensor_program: OwnedAlgebraicTensorProgramEnvelope,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.index_name);
        self.query.deinit(alloc);
        self.options.deinit(alloc);
        self.native_doc_id_constraints.deinit(alloc);
        if (self.resolved_doc_filter) |ptr| db_mod.doc_filter_wire.destroyResolvedDocFilter(alloc, ptr);
        for (self.tensor_access_paths) |*path| path.deinit(alloc);
        if (self.tensor_access_paths.len > 0) alloc.free(self.tensor_access_paths);
        self.tensor_program.deinit(alloc);
        self.* = undefined;
    }

    pub fn proveTensorProgramAlloc(self: *const @This(), alloc: std.mem.Allocator) !algebraic_ir.TensorProgramProof {
        var paths = try alloc.alloc(algebraic_ir.PhysicalAccessPath, self.tensor_access_paths.len);
        defer if (paths.len > 0) alloc.free(paths);
        var found_target_path = false;
        for (self.tensor_access_paths, 0..) |*path, i| {
            paths[i] = path.asAccessPath();
            if (paths[i].layout == self.layout and
                std.mem.eql(u8, paths[i].owner, self.index_name) and
                algebraic_ir.accessPathCanSatisfy(paths[i], .{
                    .fragment = .vector_search,
                    .output_dims = &.{ .doc, .score },
                    .owner = self.index_name,
                    .layout = self.layout,
                }).safe())
            {
                found_target_path = true;
            }
        }
        if (!found_target_path) return error.InvalidQueryRequest;

        var view = try self.tensor_program.asProgramAlloc(alloc);
        defer view.deinit(alloc);
        if (!algebraic_ir.vectorSearchProgramMatchesTarget(
            view.program,
            self.index_name,
            self.layout,
            self.native_doc_id_constraints.constraints.hasConstraints(),
        )) return error.InvalidQueryRequest;
        return try algebraic_ir.tensorProgramProof(alloc, paths, view.program);
    }
};

pub const AlgebraicVectorWorkerQuery = union(enum) {
    dense: db_mod.types.DenseKnnQuery,
    sparse: db_mod.types.SparseKnnQuery,
};

pub const OwnedAlgebraicVectorWorkerQuery = union(enum) {
    dense: db_mod.types.DenseKnnQuery,
    sparse: db_mod.types.SparseKnnQuery,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .dense => |query| if (query.vector.len > 0) alloc.free(@constCast(query.vector)),
            .sparse => |query| {
                if (query.indices.len > 0) alloc.free(@constCast(query.indices));
                if (query.values.len > 0) alloc.free(@constCast(query.values));
            },
        }
        self.* = undefined;
    }
};

pub const AlgebraicTensorAccessPathEnvelopeInput = struct {
    owner: []const u8,
    layout: []const u8,
    dictionary: ?AlgebraicDictionaryIdentityInput = null,
    fragments: []const []const u8 = &.{},
    output_dims: []const []const u8 = &.{},
    law_ids: []const []const u8 = &.{},
};

pub const AlgebraicDictionaryIdentityInput = struct {
    scope: []const u8,
    field_or_path: []const u8,
    label_kind: []const u8,
    analyzer_or_canonicalization: []const u8,
    value_kind: []const u8,
    coercion_policy: []const u8,
};

pub const AlgebraicTensorExprEnvelopeInput = struct {
    expr_id: ?[]const u8 = null,
    fragment: []const u8,
    input_dims: []const []const u8 = &.{},
    output_dims: []const []const u8 = &.{},
    semantic_id: ?[]const u8 = null,
    owner: ?[]const u8 = null,
    layout: ?[]const u8 = null,
    dictionary: ?AlgebraicDictionaryIdentityInput = null,
    law_id: ?[]const u8 = null,
    metadata: ?[]const u8 = null,
};

pub const AlgebraicTensorProgramRefInput = struct {
    kind: []const u8,
    index: usize,
};

pub const AlgebraicTensorProgramStepEnvelopeInput = struct {
    expr: AlgebraicTensorExprEnvelopeInput,
    inputs: []const AlgebraicTensorProgramRefInput = &.{},
};

pub const AlgebraicTensorProgramEnvelopeInput = struct {
    program_id: ?[]const u8 = null,
    inputs: []const AlgebraicTensorExprEnvelopeInput = &.{},
    steps: []const AlgebraicTensorProgramStepEnvelopeInput = &.{},
    output: AlgebraicTensorProgramRefInput,
    outputs: []const AlgebraicTensorProgramRefInput = &.{},
};

pub const OwnedAlgebraicTensorAccessPathEnvelope = struct {
    owner: []u8,
    layout: algebraic_ir.PhysicalLayout,
    dictionary: ?OwnedAlgebraicDictionaryIdentity = null,
    fragments: []algebraic_ir.TensorFragment,
    output_dims: []algebraic_ir.Dimension,
    law_ids: []algebraic_law.Id,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.owner);
        if (self.dictionary) |*dictionary| dictionary.deinit(alloc);
        if (self.fragments.len > 0) alloc.free(self.fragments);
        if (self.output_dims.len > 0) alloc.free(self.output_dims);
        if (self.law_ids.len > 0) alloc.free(self.law_ids);
        self.* = undefined;
    }

    pub fn asAccessPath(self: *const @This()) algebraic_ir.PhysicalAccessPath {
        const dictionary = if (self.dictionary) |*value| value.asIdentity() else null;
        return .{
            .owner = self.owner,
            .layout = self.layout,
            .dictionary = dictionary,
            .fragments = self.fragments,
            .output_dims = self.output_dims,
            .law_ids = self.law_ids,
        };
    }
};

pub const OwnedAlgebraicDictionaryIdentity = struct {
    scope: []u8,
    field_or_path: []u8,
    label_kind: algebraic_lexical.LabelKind,
    analyzer_or_canonicalization: []u8,
    value_kind: []u8,
    coercion_policy: []u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.scope);
        alloc.free(self.field_or_path);
        alloc.free(self.analyzer_or_canonicalization);
        alloc.free(self.value_kind);
        alloc.free(self.coercion_policy);
        self.* = undefined;
    }

    pub fn asIdentity(self: *const @This()) algebraic_lexical.DictionaryIdentity {
        return .{
            .scope = self.scope,
            .field_or_path = self.field_or_path,
            .label_kind = self.label_kind,
            .analyzer_or_canonicalization = self.analyzer_or_canonicalization,
            .value_kind = self.value_kind,
            .coercion_policy = self.coercion_policy,
        };
    }
};

pub const OwnedAlgebraicTensorExprEnvelope = struct {
    expr_id: []u8,
    fragment: algebraic_ir.TensorFragment,
    input_dims: []algebraic_ir.Dimension,
    output_dims: []algebraic_ir.Dimension,
    semantic_id: ?[]u8,
    owner: ?[]u8,
    layout: ?algebraic_ir.PhysicalLayout,
    dictionary: ?OwnedAlgebraicDictionaryIdentity,
    law_id: ?algebraic_law.Id,
    metadata: ?[]u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.expr_id);
        if (self.input_dims.len > 0) alloc.free(self.input_dims);
        if (self.output_dims.len > 0) alloc.free(self.output_dims);
        if (self.semantic_id) |value| alloc.free(value);
        if (self.owner) |value| alloc.free(value);
        if (self.metadata) |value| alloc.free(value);
        if (self.dictionary) |*value| value.deinit(alloc);
        self.* = undefined;
    }

    pub fn asExpr(self: *const @This()) algebraic_ir.TensorExpr {
        const dictionary = if (self.dictionary) |*value| value.asIdentity() else null;
        return .{
            .fragment = self.fragment,
            .input_dims = self.input_dims,
            .output_dims = self.output_dims,
            .semantic_id = self.semantic_id,
            .owner = self.owner,
            .layout = self.layout,
            .dictionary = dictionary,
            .law_id = self.law_id,
            .metadata = self.metadata,
        };
    }
};

pub const OwnedAlgebraicTensorProgramStepEnvelope = struct {
    expr: OwnedAlgebraicTensorExprEnvelope,
    inputs: []algebraic_ir.TensorProgramRef,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        self.expr.deinit(alloc);
        if (self.inputs.len > 0) alloc.free(self.inputs);
        self.* = undefined;
    }
};

pub const OwnedAlgebraicTensorProgramEnvelope = struct {
    program_id: []u8,
    inputs: []OwnedAlgebraicTensorExprEnvelope,
    steps: []OwnedAlgebraicTensorProgramStepEnvelope,
    output: algebraic_ir.TensorProgramRef,
    outputs: []algebraic_ir.TensorProgramRef,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.program_id);
        for (self.inputs) |*input| input.deinit(alloc);
        if (self.inputs.len > 0) alloc.free(self.inputs);
        for (self.steps) |*step| step.deinit(alloc);
        if (self.steps.len > 0) alloc.free(self.steps);
        if (self.outputs.len > 0) alloc.free(self.outputs);
        self.* = undefined;
    }

    pub fn asProgramAlloc(self: *const @This(), alloc: std.mem.Allocator) !OwnedAlgebraicTensorProgramView {
        const inputs = try alloc.alloc(algebraic_ir.TensorExpr, self.inputs.len);
        errdefer if (inputs.len > 0) alloc.free(inputs);
        for (self.inputs, 0..) |*input, i| inputs[i] = input.asExpr();
        const steps = try alloc.alloc(algebraic_ir.TensorProgramStep, self.steps.len);
        errdefer if (steps.len > 0) alloc.free(steps);
        for (self.steps, 0..) |*step, i| {
            steps[i] = .{
                .expr = step.expr.asExpr(),
                .inputs = step.inputs,
            };
        }
        return .{
            .program = .{
                .inputs = inputs,
                .steps = steps,
                .output = self.output,
                .outputs = self.outputs,
            },
            .input_exprs = inputs,
            .steps = steps,
        };
    }
};

pub const OwnedAlgebraicTensorProgramView = struct {
    program: algebraic_ir.TensorProgram,
    input_exprs: []algebraic_ir.TensorExpr,
    steps: []algebraic_ir.TensorProgramStep,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.input_exprs.len > 0) alloc.free(self.input_exprs);
        if (self.steps.len > 0) alloc.free(self.steps);
        self.* = undefined;
    }
};

pub fn nativeDocIdConstraintEnvelopeFromSearchRequest(req: db_mod.types.SearchRequest) NativeDocIdConstraintEnvelope {
    return .{
        .positive_filter = req.filter_doc_ids_positive,
        .include_doc_ids = req.filter_doc_ids,
        .exclude_doc_ids = req.exclude_doc_ids,
    };
}

pub fn encodeAlgebraicTensorAccessPathEnvelopeAlloc(
    alloc: std.mem.Allocator,
    access_path: algebraic_ir.PhysicalAccessPath,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    var first = true;
    try out.append(alloc, '{');
    try appendJsonFieldString(alloc, &out, &first, "owner", access_path.owner);
    try appendJsonFieldString(alloc, &out, &first, "layout", @tagName(access_path.layout));
    if (access_path.dictionary) |dictionary| try appendAlgebraicDictionaryIdentity(alloc, &out, &first, dictionary);
    try appendJsonFieldName(alloc, &out, &first, "fragments");
    try appendEnumNameArray(alloc, &out, algebraic_ir.TensorFragment, access_path.fragments);
    try appendJsonFieldName(alloc, &out, &first, "output_dims");
    try appendEnumNameArray(alloc, &out, algebraic_ir.Dimension, access_path.output_dims);
    try appendJsonFieldName(alloc, &out, &first, "law_ids");
    try appendEnumNameArray(alloc, &out, algebraic_law.Id, access_path.law_ids);
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

fn appendAlgebraicDictionaryIdentity(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    dictionary: algebraic_lexical.DictionaryIdentity,
) !void {
    try appendJsonFieldName(alloc, out, first, "dictionary");
    try out.append(alloc, '{');
    var dictionary_first = true;
    try appendJsonFieldString(alloc, out, &dictionary_first, "scope", dictionary.scope);
    try appendJsonFieldString(alloc, out, &dictionary_first, "field_or_path", dictionary.field_or_path);
    try appendJsonFieldString(alloc, out, &dictionary_first, "label_kind", @tagName(dictionary.label_kind));
    try appendJsonFieldString(alloc, out, &dictionary_first, "analyzer_or_canonicalization", dictionary.analyzer_or_canonicalization);
    try appendJsonFieldString(alloc, out, &dictionary_first, "value_kind", dictionary.value_kind);
    try appendJsonFieldString(alloc, out, &dictionary_first, "coercion_policy", dictionary.coercion_policy);
    try out.append(alloc, '}');
}

pub fn parseAlgebraicTensorAccessPathEnvelopeAlloc(
    alloc: std.mem.Allocator,
    body: []const u8,
) !OwnedAlgebraicTensorAccessPathEnvelope {
    var parsed = std.json.parseFromSlice(AlgebraicTensorAccessPathEnvelopeInput, alloc, body, .{}) catch return error.InvalidQueryRequest;
    defer parsed.deinit();
    return try parseAlgebraicTensorAccessPathEnvelopeInputAlloc(alloc, parsed.value);
}

pub fn encodeAlgebraicTensorExprEnvelopeAlloc(
    alloc: std.mem.Allocator,
    expr: algebraic_ir.TensorExpr,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    var first = true;
    const expr_id = try algebraic_ir.tensorExprIdAlloc(alloc, expr);
    defer alloc.free(expr_id);
    try out.append(alloc, '{');
    try appendJsonFieldString(alloc, &out, &first, "expr_id", expr_id);
    try appendJsonFieldString(alloc, &out, &first, "fragment", @tagName(expr.fragment));
    try appendJsonFieldName(alloc, &out, &first, "input_dims");
    try appendEnumNameArray(alloc, &out, algebraic_ir.Dimension, expr.input_dims);
    try appendJsonFieldName(alloc, &out, &first, "output_dims");
    try appendEnumNameArray(alloc, &out, algebraic_ir.Dimension, expr.output_dims);
    if (expr.semantic_id) |semantic_id| try appendJsonFieldString(alloc, &out, &first, "semantic_id", semantic_id);
    if (expr.owner) |owner| try appendJsonFieldString(alloc, &out, &first, "owner", owner);
    if (expr.layout) |layout| try appendJsonFieldString(alloc, &out, &first, "layout", @tagName(layout));
    if (expr.dictionary) |dictionary| try appendAlgebraicDictionaryIdentity(alloc, &out, &first, dictionary);
    if (expr.law_id) |law_id| try appendJsonFieldString(alloc, &out, &first, "law_id", @tagName(law_id));
    if (expr.metadata) |metadata| try appendJsonFieldString(alloc, &out, &first, "metadata", metadata);
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

pub fn encodeAlgebraicTensorProgramEnvelopeAlloc(
    alloc: std.mem.Allocator,
    program: algebraic_ir.TensorProgram,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    var first = true;
    const program_id = try algebraic_ir.tensorProgramIdAlloc(alloc, program);
    defer alloc.free(program_id);
    try out.append(alloc, '{');
    try appendJsonFieldString(alloc, &out, &first, "program_id", program_id);
    try appendJsonFieldName(alloc, &out, &first, "inputs");
    try out.append(alloc, '[');
    for (program.inputs, 0..) |expr, i| {
        if (i > 0) try out.append(alloc, ',');
        const encoded = try encodeAlgebraicTensorExprEnvelopeAlloc(alloc, expr);
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    try out.append(alloc, ']');
    try appendJsonFieldName(alloc, &out, &first, "steps");
    try out.append(alloc, '[');
    for (program.steps, 0..) |step, i| {
        if (i > 0) try out.append(alloc, ',');
        try appendAlgebraicTensorProgramStep(alloc, &out, step);
    }
    try out.append(alloc, ']');
    try appendJsonFieldName(alloc, &out, &first, "output");
    try appendAlgebraicTensorProgramRef(alloc, &out, program.output);
    try appendJsonFieldName(alloc, &out, &first, "outputs");
    try out.append(alloc, '[');
    for (program.outputs, 0..) |output_ref, i| {
        if (i > 0) try out.append(alloc, ',');
        try appendAlgebraicTensorProgramRef(alloc, &out, output_ref);
    }
    try out.append(alloc, ']');
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

fn appendAlgebraicTensorProgramStep(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    step: algebraic_ir.TensorProgramStep,
) !void {
    try out.append(alloc, '{');
    var first = true;
    try appendJsonFieldName(alloc, out, &first, "expr");
    const encoded = try encodeAlgebraicTensorExprEnvelopeAlloc(alloc, step.expr);
    defer alloc.free(encoded);
    try out.appendSlice(alloc, encoded);
    try appendJsonFieldName(alloc, out, &first, "inputs");
    try out.append(alloc, '[');
    for (step.inputs, 0..) |input_ref, i| {
        if (i > 0) try out.append(alloc, ',');
        try appendAlgebraicTensorProgramRef(alloc, out, input_ref);
    }
    try out.append(alloc, ']');
    try out.append(alloc, '}');
}

fn appendAlgebraicTensorProgramRef(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    input_ref: algebraic_ir.TensorProgramRef,
) !void {
    try out.append(alloc, '{');
    var first = true;
    switch (input_ref) {
        .input => |idx| {
            try appendJsonFieldString(alloc, out, &first, "kind", "input");
            try appendJsonFieldUsize(alloc, out, &first, "index", idx);
        },
        .step => |idx| {
            try appendJsonFieldString(alloc, out, &first, "kind", "step");
            try appendJsonFieldUsize(alloc, out, &first, "index", idx);
        },
    }
    try out.append(alloc, '}');
}

pub fn parseAlgebraicTensorExprEnvelopeAlloc(
    alloc: std.mem.Allocator,
    body: []const u8,
) !OwnedAlgebraicTensorExprEnvelope {
    var parsed = std.json.parseFromSlice(AlgebraicTensorExprEnvelopeInput, alloc, body, .{}) catch return error.InvalidQueryRequest;
    defer parsed.deinit();
    return try parseAlgebraicTensorExprEnvelopeInputAlloc(alloc, parsed.value);
}

pub fn parseAlgebraicTensorExprEnvelopeInputAlloc(
    alloc: std.mem.Allocator,
    input: AlgebraicTensorExprEnvelopeInput,
) !OwnedAlgebraicTensorExprEnvelope {
    const fragment = std.meta.stringToEnum(algebraic_ir.TensorFragment, input.fragment) orelse return error.InvalidQueryRequest;
    const input_dims = try parseAlgebraicDimensionArrayAlloc(alloc, input.input_dims);
    errdefer if (input_dims.len > 0) alloc.free(input_dims);
    const output_dims = try parseAlgebraicDimensionArrayAlloc(alloc, input.output_dims);
    errdefer if (output_dims.len > 0) alloc.free(output_dims);
    const semantic_id = if (input.semantic_id) |value| try alloc.dupe(u8, value) else null;
    errdefer if (semantic_id) |value| alloc.free(value);
    const owner = if (input.owner) |value| try alloc.dupe(u8, value) else null;
    errdefer if (owner) |value| alloc.free(value);
    const layout = if (input.layout) |value| std.meta.stringToEnum(algebraic_ir.PhysicalLayout, value) orelse return error.InvalidQueryRequest else null;
    var dictionary = if (input.dictionary) |value| try parseAlgebraicDictionaryIdentityAlloc(alloc, value) else null;
    errdefer if (dictionary) |*value| value.deinit(alloc);
    const law_id = if (input.law_id) |value| algebraic_law.Id.parse(value) orelse return error.InvalidQueryRequest else null;
    const metadata = if (input.metadata) |value| try alloc.dupe(u8, value) else null;
    errdefer if (metadata) |value| alloc.free(value);
    const expr_for_id = algebraic_ir.TensorExpr{
        .fragment = fragment,
        .input_dims = input_dims,
        .output_dims = output_dims,
        .semantic_id = semantic_id,
        .owner = owner,
        .layout = layout,
        .dictionary = if (dictionary) |*value| value.asIdentity() else null,
        .law_id = law_id,
        .metadata = metadata,
    };
    const expr_id = try algebraic_ir.tensorExprIdAlloc(alloc, expr_for_id);
    errdefer alloc.free(expr_id);
    const claimed_id = input.expr_id orelse return error.InvalidQueryRequest;
    if (!std.mem.eql(u8, expr_id, claimed_id)) return error.InvalidQueryRequest;

    return .{
        .expr_id = expr_id,
        .fragment = fragment,
        .input_dims = input_dims,
        .output_dims = output_dims,
        .semantic_id = semantic_id,
        .owner = owner,
        .layout = layout,
        .dictionary = dictionary,
        .law_id = law_id,
        .metadata = metadata,
    };
}

pub fn parseAlgebraicTensorProgramEnvelopeAlloc(
    alloc: std.mem.Allocator,
    body: []const u8,
) !OwnedAlgebraicTensorProgramEnvelope {
    var parsed = std.json.parseFromSlice(AlgebraicTensorProgramEnvelopeInput, alloc, body, .{}) catch return error.InvalidQueryRequest;
    defer parsed.deinit();
    return try parseAlgebraicTensorProgramEnvelopeInputAlloc(alloc, parsed.value);
}

pub fn parseAlgebraicTensorProgramEnvelopeInputAlloc(
    alloc: std.mem.Allocator,
    input: AlgebraicTensorProgramEnvelopeInput,
) !OwnedAlgebraicTensorProgramEnvelope {
    const inputs = try alloc.alloc(OwnedAlgebraicTensorExprEnvelope, input.inputs.len);
    var inputs_initialized: usize = 0;
    errdefer {
        for (inputs[0..inputs_initialized]) |*owned| owned.deinit(alloc);
        if (inputs.len > 0) alloc.free(inputs);
    }
    for (input.inputs, 0..) |expr_input, i| {
        inputs[i] = try parseAlgebraicTensorExprEnvelopeInputAlloc(alloc, expr_input);
        inputs_initialized += 1;
    }

    const steps = try alloc.alloc(OwnedAlgebraicTensorProgramStepEnvelope, input.steps.len);
    var steps_initialized: usize = 0;
    errdefer {
        for (steps[0..steps_initialized]) |*step| step.deinit(alloc);
        if (steps.len > 0) alloc.free(steps);
    }
    for (input.steps, 0..) |step_input, i| {
        var expr = try parseAlgebraicTensorExprEnvelopeInputAlloc(alloc, step_input.expr);
        var expr_moved = false;
        errdefer if (!expr_moved) expr.deinit(alloc);
        const refs = try parseAlgebraicTensorProgramRefsAlloc(alloc, step_input.inputs);
        var refs_moved = false;
        errdefer if (!refs_moved and refs.len > 0) alloc.free(refs);
        steps[i] = .{
            .expr = expr,
            .inputs = refs,
        };
        expr_moved = true;
        refs_moved = true;
        steps_initialized += 1;
    }

    const output = parseAlgebraicTensorProgramRef(input.output) orelse return error.InvalidQueryRequest;
    const outputs = try parseAlgebraicTensorProgramRefsAlloc(alloc, input.outputs);
    errdefer if (outputs.len > 0) alloc.free(outputs);
    var envelope = OwnedAlgebraicTensorProgramEnvelope{
        .program_id = undefined,
        .inputs = inputs,
        .steps = steps,
        .output = output,
        .outputs = outputs,
    };
    try validateAlgebraicTensorProgramEnvelopeStructure(envelope);
    var view = try envelope.asProgramAlloc(alloc);
    defer view.deinit(alloc);
    const program_id = try algebraic_ir.tensorProgramIdAlloc(alloc, view.program);
    errdefer alloc.free(program_id);
    if (input.program_id) |claimed_id| {
        if (!std.mem.eql(u8, program_id, claimed_id)) return error.InvalidQueryRequest;
    }
    envelope.program_id = program_id;
    return envelope;
}

fn validateAlgebraicTensorProgramEnvelopeStructure(envelope: OwnedAlgebraicTensorProgramEnvelope) !void {
    try validateAlgebraicTensorProgramOutputRef(envelope, envelope.output);
    for (envelope.outputs) |output_ref| try validateAlgebraicTensorProgramOutputRef(envelope, output_ref);
    for (envelope.steps, 0..) |step, step_index| {
        for (step.inputs) |input_ref| {
            switch (input_ref) {
                .input => |idx| if (idx >= envelope.inputs.len) return error.InvalidQueryRequest,
                .step => |idx| if (idx >= step_index) return error.InvalidQueryRequest,
            }
        }
    }
}

fn validateAlgebraicTensorProgramOutputRef(
    envelope: OwnedAlgebraicTensorProgramEnvelope,
    ref: algebraic_ir.TensorProgramRef,
) !void {
    switch (ref) {
        .input => |idx| if (idx >= envelope.inputs.len) return error.InvalidQueryRequest,
        .step => |idx| if (idx >= envelope.steps.len) return error.InvalidQueryRequest,
    }
}

pub fn encodeAlgebraicVectorWorkerRequestEnvelopeAlloc(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    layout: algebraic_ir.PhysicalLayout,
    query: AlgebraicVectorWorkerQuery,
    options: AlgebraicVectorWorkerRequestOptions,
    native_doc_id_constraints: NativeDocIdConstraintEnvelope,
    resolved_doc_filter: ?*const anyopaque,
    resolved_doc_filter_wire_context: ?db_mod.types.ResolvedDocFilterWireContext,
    tensor_access_paths: []const algebraic_ir.PhysicalAccessPath,
    tensor_program: algebraic_ir.TensorProgram,
) ![]u8 {
    if (layout != .dense_vector and layout != .sparse_vector) return error.InvalidQueryRequest;
    switch (query) {
        .dense => if (layout != .dense_vector) return error.InvalidQueryRequest,
        .sparse => |sparse| {
            if (layout != .sparse_vector) return error.InvalidQueryRequest;
            if (sparse.indices.len != sparse.values.len) return error.InvalidQueryRequest;
        },
    }
    if (!algebraic_ir.vectorSearchProgramMatchesTarget(
        tensor_program,
        index_name,
        layout,
        native_doc_id_constraints.hasConstraints(),
    )) return error.InvalidQueryRequest;
    if ((options.filter_query_json.len > 0 or options.exclusion_query_json.len > 0) and !options.require_algebraic_filter_resolution) return error.InvalidQueryRequest;
    const proof = try algebraic_ir.tensorProgramProof(alloc, tensor_access_paths, tensor_program);
    if (!proof.safe()) return error.InvalidQueryRequest;

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    var first = true;
    try out.append(alloc, '{');
    try appendJsonFieldString(alloc, &out, &first, "index_name", index_name);
    try appendJsonFieldString(alloc, &out, &first, "layout", @tagName(layout));
    try appendJsonFieldName(alloc, &out, &first, "query");
    try appendAlgebraicVectorWorkerQuery(alloc, &out, query);
    try appendJsonFieldName(alloc, &out, &first, "options");
    try appendAlgebraicVectorWorkerRequestOptions(alloc, &out, options);
    try appendJsonFieldName(alloc, &out, &first, "native_doc_id_constraints");
    const encoded_constraints = try encodeNativeDocIdConstraintEnvelopeAlloc(alloc, native_doc_id_constraints);
    defer alloc.free(encoded_constraints);
    try out.appendSlice(alloc, encoded_constraints);
    if (resolved_doc_filter) |ptr| {
        try db_mod.doc_filter_wire.appendSearchRequestFieldAlloc(alloc, &out, &first, .{
            .resolved_doc_filter = ptr,
            .resolved_doc_filter_wire_context = resolved_doc_filter_wire_context orelse return error.UnsupportedQueryRequest,
        });
    }
    try appendJsonFieldName(alloc, &out, &first, "tensor_access_paths");
    try out.append(alloc, '[');
    for (tensor_access_paths, 0..) |access_path, i| {
        if (i > 0) try out.append(alloc, ',');
        const encoded = try encodeAlgebraicTensorAccessPathEnvelopeAlloc(alloc, access_path);
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    try out.append(alloc, ']');
    try appendJsonFieldName(alloc, &out, &first, "tensor_program");
    const encoded_program = try encodeAlgebraicTensorProgramEnvelopeAlloc(alloc, tensor_program);
    defer alloc.free(encoded_program);
    try out.appendSlice(alloc, encoded_program);
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

pub fn parseAlgebraicVectorWorkerRequestEnvelopeAlloc(
    alloc: std.mem.Allocator,
    body: []const u8,
) !OwnedAlgebraicVectorWorkerRequestEnvelope {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{}) catch return error.InvalidQueryRequest;
    defer parsed.deinit();
    const root = parsed.value;
    if (root != .object) return error.InvalidQueryRequest;
    const index_name_value = root.object.get("index_name") orelse return error.InvalidQueryRequest;
    if (index_name_value != .string or index_name_value.string.len == 0) return error.InvalidQueryRequest;
    const layout_value = root.object.get("layout") orelse return error.InvalidQueryRequest;
    if (layout_value != .string) return error.InvalidQueryRequest;
    const layout = std.meta.stringToEnum(algebraic_ir.PhysicalLayout, layout_value.string) orelse return error.InvalidQueryRequest;
    if (layout != .dense_vector and layout != .sparse_vector) return error.InvalidQueryRequest;

    const index_name = try alloc.dupe(u8, index_name_value.string);
    errdefer alloc.free(index_name);

    var query = try parseAlgebraicVectorWorkerQueryAlloc(alloc, layout, root.object.get("query") orelse return error.InvalidQueryRequest);
    errdefer query.deinit(alloc);
    var options = if (root.object.get("options")) |value|
        try parseAlgebraicVectorWorkerRequestOptions(alloc, value)
    else
        AlgebraicVectorWorkerRequestOptions{};
    errdefer options.deinit(alloc);

    var native_doc_id_constraints = if (root.object.get("native_doc_id_constraints")) |value|
        try parseNativeDocIdConstraintEnvelopeValueAlloc(alloc, value)
    else
        OwnedNativeDocIdConstraintEnvelope{ .constraints = .{} };
    errdefer native_doc_id_constraints.deinit(alloc);

    var resolved_req = db_mod.types.SearchRequest{};
    if (root.object.get(db_mod.doc_filter_wire.field_name)) |value| {
        try db_mod.doc_filter_wire.parseIntoSearchRequestAlloc(alloc, value, &resolved_req);
    }
    errdefer if (resolved_req.resolved_doc_filter_owned) db_mod.doc_filter_wire.destroyResolvedDocFilter(alloc, resolved_req.resolved_doc_filter.?);
    if (resolved_req.identity_read_generation) |generation| {
        if (options.identity_read_generation) |existing| {
            if (existing != generation) return error.InvalidQueryRequest;
        } else {
            options.identity_read_generation = generation;
        }
    }

    const access_path_value = root.object.get("tensor_access_paths") orelse return error.InvalidQueryRequest;
    if (access_path_value != .array) return error.InvalidQueryRequest;
    const tensor_access_paths = try alloc.alloc(OwnedAlgebraicTensorAccessPathEnvelope, access_path_value.array.items.len);
    var paths_initialized: usize = 0;
    errdefer {
        for (tensor_access_paths[0..paths_initialized]) |*path| path.deinit(alloc);
        if (tensor_access_paths.len > 0) alloc.free(tensor_access_paths);
    }
    for (access_path_value.array.items, 0..) |item, i| {
        const encoded = try jsonStringifyAlloc(alloc, item);
        defer alloc.free(encoded);
        tensor_access_paths[i] = try parseAlgebraicTensorAccessPathEnvelopeAlloc(alloc, encoded);
        paths_initialized += 1;
    }

    const program_value = root.object.get("tensor_program") orelse return error.InvalidQueryRequest;
    const encoded_program = try jsonStringifyAlloc(alloc, program_value);
    defer alloc.free(encoded_program);
    var tensor_program = try parseAlgebraicTensorProgramEnvelopeAlloc(alloc, encoded_program);
    errdefer tensor_program.deinit(alloc);

    const envelope = OwnedAlgebraicVectorWorkerRequestEnvelope{
        .index_name = index_name,
        .layout = layout,
        .query = query,
        .options = options,
        .native_doc_id_constraints = native_doc_id_constraints,
        .resolved_doc_filter = resolved_req.resolved_doc_filter,
        .resolved_doc_filter_wire_context = resolved_req.resolved_doc_filter_wire_context,
        .tensor_access_paths = tensor_access_paths,
        .tensor_program = tensor_program,
    };
    if (!(try envelope.proveTensorProgramAlloc(alloc)).safe()) return error.InvalidQueryRequest;
    resolved_req.resolved_doc_filter = null;
    resolved_req.resolved_doc_filter_owned = false;
    return envelope;
}

fn appendAlgebraicVectorWorkerQuery(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    query: AlgebraicVectorWorkerQuery,
) !void {
    try out.append(alloc, '{');
    var first = true;
    switch (query) {
        .dense => |dense| {
            try appendJsonFieldString(alloc, out, &first, "kind", "dense");
            try appendJsonFieldUsize(alloc, out, &first, "k", dense.k);
            try appendJsonFieldName(alloc, out, &first, "vector");
            try appendF32Array(alloc, out, dense.vector);
        },
        .sparse => |sparse| {
            try appendJsonFieldString(alloc, out, &first, "kind", "sparse");
            try appendJsonFieldUsize(alloc, out, &first, "k", sparse.k);
            try appendJsonFieldName(alloc, out, &first, "indices");
            try appendU32Array(alloc, out, sparse.indices);
            try appendJsonFieldName(alloc, out, &first, "values");
            try appendF32Array(alloc, out, sparse.values);
        },
    }
    try out.append(alloc, '}');
}

fn appendAlgebraicVectorWorkerRequestOptions(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    options: AlgebraicVectorWorkerRequestOptions,
) !void {
    try out.append(alloc, '{');
    var first = true;
    if (options.fields.len > 0) try appendJsonFieldStringArray(alloc, out, &first, "fields", options.fields);
    if (options.filter_prefix.len > 0) try appendJsonFieldString(alloc, out, &first, "filter_prefix", options.filter_prefix);
    if (options.filter_query_json.len > 0) try appendJsonFieldString(alloc, out, &first, "filter_query_json", options.filter_query_json);
    if (options.exclusion_query_json.len > 0) try appendJsonFieldString(alloc, out, &first, "exclusion_query_json", options.exclusion_query_json);
    if (options.filter_ids.len > 0) {
        try appendJsonFieldName(alloc, out, &first, "filter_ids");
        try appendU64Array(alloc, out, options.filter_ids);
    }
    if (options.exclude_ids.len > 0) {
        try appendJsonFieldName(alloc, out, &first, "exclude_ids");
        try appendU64Array(alloc, out, options.exclude_ids);
    }
    if (options.require_algebraic_filter_resolution) try appendJsonFieldBool(alloc, out, &first, "require_algebraic_filter_resolution", true);
    if (!options.include_all_fields) try appendJsonFieldBool(alloc, out, &first, "include_all_fields", false);
    if (options.defer_stored_projection) try appendJsonFieldBool(alloc, out, &first, "defer_stored_projection", true);
    try appendJsonFieldUsize(alloc, out, &first, "limit", options.limit);
    if (options.offset != 0) try appendJsonFieldUsize(alloc, out, &first, "offset", options.offset);
    if (options.count_only) try appendJsonFieldBool(alloc, out, &first, "count_only", true);
    if (options.profile) try appendJsonFieldBool(alloc, out, &first, "profile", true);
    if (!options.include_stored) try appendJsonFieldBool(alloc, out, &first, "include_stored", false);
    if (options.search_effort) |value| try appendJsonFieldF32(alloc, out, &first, "search_effort", value);
    if (options.distance_over) |value| try appendJsonFieldF32(alloc, out, &first, "distance_over", value);
    if (options.distance_under) |value| try appendJsonFieldF32(alloc, out, &first, "distance_under", value);
    if (options.return_mode != .parent) try appendJsonFieldString(
        alloc,
        out,
        &first,
        "return_mode",
        // Internal worker envelopes retain the established spelling so a
        // rolling-upgrade peer can execute the equivalent raw-member shape.
        if (options.return_mode == .member) "chunk" else @tagName(options.return_mode),
    );
    if (options.max_chunks_per_parent != 0) try appendJsonFieldUsize(alloc, out, &first, "max_chunks_per_parent", options.max_chunks_per_parent);
    if (options.hierarchy_include_source) try appendJsonFieldBool(alloc, out, &first, "hierarchy_include_source", true);
    if (options.hierarchy_include_unit) try appendJsonFieldBool(alloc, out, &first, "hierarchy_include_unit", true);
    if (options.hierarchy_omit_implicit_source_ancestor_document) try appendJsonFieldBool(alloc, out, &first, "hierarchy_omit_implicit_source_ancestor_document", true);
    if (options.hierarchy_match_fields.len > 0) try appendJsonFieldStringArray(alloc, out, &first, "hierarchy_match_fields", options.hierarchy_match_fields);
    if (!options.hierarchy_match_include_all_fields) try appendJsonFieldBool(alloc, out, &first, "hierarchy_match_include_all_fields", false);
    if (options.hierarchy_grouped_matches) try appendJsonFieldBool(alloc, out, &first, "hierarchy_grouped_matches", true);
    if (options.hierarchy_group_level != .source) try appendJsonFieldString(alloc, out, &first, "hierarchy_group_level", @tagName(options.hierarchy_group_level));
    if (options.defer_hierarchy_child_hydration) try appendJsonFieldBool(alloc, out, &first, "defer_hierarchy_child_hydration", true);
    if (options.hierarchy_source_fields.len > 0) try appendJsonFieldStringArray(alloc, out, &first, "hierarchy_source_fields", options.hierarchy_source_fields);
    if (!options.hierarchy_source_include_all_fields) try appendJsonFieldBool(alloc, out, &first, "hierarchy_source_include_all_fields", false);
    if (options.hierarchy_unit_fields.len > 0) try appendJsonFieldStringArray(alloc, out, &first, "hierarchy_unit_fields", options.hierarchy_unit_fields);
    if (!options.hierarchy_unit_include_all_fields) try appendJsonFieldBool(alloc, out, &first, "hierarchy_unit_include_all_fields", false);
    if (options.identity_read_generation) |generation| try appendJsonFieldU64(alloc, out, &first, "identity_read_generation", generation);
    try out.append(alloc, '}');
}

fn parseAlgebraicVectorWorkerRequestOptions(alloc: std.mem.Allocator, value: std.json.Value) !AlgebraicVectorWorkerRequestOptions {
    if (value != .object) return error.InvalidQueryRequest;
    const fields = if (value.object.get("fields")) |fields_value|
        try parseStringArrayJsonAlloc(alloc, fields_value)
    else
        @constCast((&[_][]const u8{})[0..]);
    errdefer freeOwnedStringSlice(alloc, fields);
    const filter_prefix = if (value.object.get("filter_prefix")) |prefix_value|
        try parseStringJsonAlloc(alloc, prefix_value)
    else
        "";
    errdefer if (filter_prefix.len > 0) alloc.free(filter_prefix);
    const filter_query_json = if (value.object.get("filter_query_json")) |query_value|
        try parseStringJsonAlloc(alloc, query_value)
    else
        "";
    errdefer if (filter_query_json.len > 0) alloc.free(filter_query_json);
    const exclusion_query_json = if (value.object.get("exclusion_query_json")) |query_value|
        try parseStringJsonAlloc(alloc, query_value)
    else
        "";
    errdefer if (exclusion_query_json.len > 0) alloc.free(exclusion_query_json);
    const filter_ids = if (value.object.get("filter_ids")) |ids_value|
        try parseU64JsonArrayAlloc(alloc, ids_value)
    else
        @constCast((&[_]u64{})[0..]);
    errdefer if (filter_ids.len > 0) alloc.free(filter_ids);
    const exclude_ids = if (value.object.get("exclude_ids")) |ids_value|
        try parseU64JsonArrayAlloc(alloc, ids_value)
    else
        @constCast((&[_]u64{})[0..]);
    errdefer if (exclude_ids.len > 0) alloc.free(exclude_ids);
    const hierarchy_match_fields = if (value.object.get("hierarchy_match_fields")) |fields_value|
        try parseStringArrayJsonAlloc(alloc, fields_value)
    else
        @constCast((&[_][]const u8{})[0..]);
    errdefer freeOwnedStringSlice(alloc, hierarchy_match_fields);
    const hierarchy_source_fields = if (value.object.get("hierarchy_source_fields")) |fields_value|
        try parseStringArrayJsonAlloc(alloc, fields_value)
    else
        @constCast((&[_][]const u8{})[0..]);
    errdefer freeOwnedStringSlice(alloc, hierarchy_source_fields);
    const hierarchy_unit_fields = if (value.object.get("hierarchy_unit_fields")) |fields_value|
        try parseStringArrayJsonAlloc(alloc, fields_value)
    else
        @constCast((&[_][]const u8{})[0..]);
    errdefer freeOwnedStringSlice(alloc, hierarchy_unit_fields);
    if (filter_query_json.len > 0) {
        var parsed_filter = std.json.parseFromSlice(std.json.Value, alloc, filter_query_json, .{}) catch return error.InvalidQueryRequest;
        parsed_filter.deinit();
    }
    if (exclusion_query_json.len > 0) {
        var parsed_exclusion = std.json.parseFromSlice(std.json.Value, alloc, exclusion_query_json, .{}) catch return error.InvalidQueryRequest;
        parsed_exclusion.deinit();
    }
    const require_algebraic_filter_resolution = try parseOptionalBoolJson(value.object.get("require_algebraic_filter_resolution"), false);
    if ((filter_query_json.len > 0 or exclusion_query_json.len > 0) and !require_algebraic_filter_resolution) return error.InvalidQueryRequest;
    const hierarchy_group_level = if (value.object.get("hierarchy_group_level")) |level_value| blk: {
        if (level_value != .string) return error.InvalidQueryRequest;
        break :blk std.meta.stringToEnum(db_mod.types.HierarchyGroupLevel, level_value.string) orelse return error.InvalidQueryRequest;
    } else .source;
    const return_mode = try parseOptionalReturnModeJson(value.object.get("return_mode"));
    const defer_hierarchy_child_hydration = try parseOptionalBoolJson(value.object.get("defer_hierarchy_child_hydration"), false);
    if (defer_hierarchy_child_hydration and
        (hierarchy_group_level != .unit or
            (return_mode != .unit and return_mode != .unit_with_chunks)))
    {
        return error.InvalidQueryRequest;
    }
    return .{
        .fields = fields,
        .filter_prefix = filter_prefix,
        .filter_query_json = filter_query_json,
        .exclusion_query_json = exclusion_query_json,
        .filter_ids = filter_ids,
        .exclude_ids = exclude_ids,
        .require_algebraic_filter_resolution = require_algebraic_filter_resolution,
        .include_all_fields = try parseOptionalBoolJson(value.object.get("include_all_fields"), true),
        .defer_stored_projection = try parseOptionalBoolJson(value.object.get("defer_stored_projection"), false),
        .limit = try parseOptionalU32Json(value.object.get("limit"), 10),
        .offset = try parseOptionalU32Json(value.object.get("offset"), 0),
        .count_only = try parseOptionalBoolJson(value.object.get("count_only"), false),
        .profile = try parseOptionalBoolJson(value.object.get("profile"), false),
        .include_stored = try parseOptionalBoolJson(value.object.get("include_stored"), true),
        .search_effort = try parseOptionalF32Json(value.object.get("search_effort")),
        .distance_over = try parseOptionalF32Json(value.object.get("distance_over")),
        .distance_under = try parseOptionalF32Json(value.object.get("distance_under")),
        .return_mode = return_mode,
        .max_chunks_per_parent = try parseOptionalU32Json(value.object.get("max_chunks_per_parent"), 0),
        .hierarchy_include_source = try parseOptionalBoolJson(value.object.get("hierarchy_include_source"), false),
        .hierarchy_include_unit = try parseOptionalBoolJson(value.object.get("hierarchy_include_unit"), false),
        .hierarchy_omit_implicit_source_ancestor_document = try parseOptionalBoolJson(value.object.get("hierarchy_omit_implicit_source_ancestor_document"), false),
        .hierarchy_match_fields = hierarchy_match_fields,
        .hierarchy_match_include_all_fields = try parseOptionalBoolJson(value.object.get("hierarchy_match_include_all_fields"), true),
        .hierarchy_grouped_matches = try parseOptionalBoolJson(value.object.get("hierarchy_grouped_matches"), false),
        .hierarchy_group_level = hierarchy_group_level,
        .defer_hierarchy_child_hydration = defer_hierarchy_child_hydration,
        .hierarchy_source_fields = hierarchy_source_fields,
        .hierarchy_source_include_all_fields = try parseOptionalBoolJson(value.object.get("hierarchy_source_include_all_fields"), true),
        .hierarchy_unit_fields = hierarchy_unit_fields,
        .hierarchy_unit_include_all_fields = try parseOptionalBoolJson(value.object.get("hierarchy_unit_include_all_fields"), true),
        .identity_read_generation = try parseOptionalU64Json(value.object.get("identity_read_generation")),
    };
}

fn appendJsonFieldF32(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    value: f32,
) !void {
    if (!std.math.isFinite(value)) return error.InvalidQueryRequest;
    try appendJsonFieldName(alloc, out, first, name);
    const rendered = try std.fmt.allocPrint(alloc, "{d}", .{value});
    defer alloc.free(rendered);
    try out.appendSlice(alloc, rendered);
}

fn appendF32Array(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    values: []const f32,
) !void {
    try out.append(alloc, '[');
    for (values, 0..) |value, i| {
        if (!std.math.isFinite(value)) return error.InvalidQueryRequest;
        if (i > 0) try out.append(alloc, ',');
        const rendered = try std.fmt.allocPrint(alloc, "{d}", .{value});
        defer alloc.free(rendered);
        try out.appendSlice(alloc, rendered);
    }
    try out.append(alloc, ']');
}

fn appendU32Array(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    values: []const u32,
) !void {
    try out.append(alloc, '[');
    for (values, 0..) |value, i| {
        if (i > 0) try out.append(alloc, ',');
        const rendered = try std.fmt.allocPrint(alloc, "{d}", .{value});
        defer alloc.free(rendered);
        try out.appendSlice(alloc, rendered);
    }
    try out.append(alloc, ']');
}

fn appendU64Array(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    values: []const u64,
) !void {
    try out.append(alloc, '[');
    for (values, 0..) |value, i| {
        if (i > 0) try out.append(alloc, ',');
        const rendered = try std.fmt.allocPrint(alloc, "{d}", .{value});
        defer alloc.free(rendered);
        try out.appendSlice(alloc, rendered);
    }
    try out.append(alloc, ']');
}

fn parseAlgebraicVectorWorkerQueryAlloc(
    alloc: std.mem.Allocator,
    layout: algebraic_ir.PhysicalLayout,
    value: std.json.Value,
) !OwnedAlgebraicVectorWorkerQuery {
    if (value != .object) return error.InvalidQueryRequest;
    const kind_value = value.object.get("kind") orelse return error.InvalidQueryRequest;
    if (kind_value != .string) return error.InvalidQueryRequest;
    const k = try parseOptionalU32Json(value.object.get("k"), 10);
    if (std.mem.eql(u8, kind_value.string, "dense")) {
        if (layout != .dense_vector) return error.InvalidQueryRequest;
        return .{ .dense = .{
            .vector = try parseF32JsonArrayAlloc(alloc, value.object.get("vector") orelse return error.InvalidQueryRequest),
            .k = k,
        } };
    }
    if (std.mem.eql(u8, kind_value.string, "sparse")) {
        if (layout != .sparse_vector) return error.InvalidQueryRequest;
        const indices = try parseU32JsonArrayAlloc(alloc, value.object.get("indices") orelse return error.InvalidQueryRequest);
        errdefer if (indices.len > 0) alloc.free(indices);
        const values = try parseF32JsonArrayAlloc(alloc, value.object.get("values") orelse return error.InvalidQueryRequest);
        errdefer if (values.len > 0) alloc.free(values);
        if (indices.len != values.len) return error.InvalidQueryRequest;
        return .{ .sparse = .{
            .indices = indices,
            .values = values,
            .k = k,
        } };
    }
    return error.InvalidQueryRequest;
}

fn parseOptionalU32Json(value_opt: ?std.json.Value, default_value: u32) !u32 {
    const value = value_opt orelse return default_value;
    if (value != .integer or value.integer < 0) return error.InvalidQueryRequest;
    return std.math.cast(u32, value.integer) orelse return error.InvalidQueryRequest;
}

fn parseOptionalU64Json(value_opt: ?std.json.Value) !?u64 {
    const value = value_opt orelse return null;
    if (value != .integer or value.integer < 0) return error.InvalidQueryRequest;
    return std.math.cast(u64, value.integer) orelse return error.InvalidQueryRequest;
}

fn parseOptionalBoolJson(value_opt: ?std.json.Value, default_value: bool) !bool {
    const value = value_opt orelse return default_value;
    if (value != .bool) return error.InvalidQueryRequest;
    return value.bool;
}

fn parseOptionalF32Json(value_opt: ?std.json.Value) !?f32 {
    const value = value_opt orelse return null;
    const parsed: f32 = switch (value) {
        .integer => |raw| @floatFromInt(raw),
        .float => |raw| @floatCast(raw),
        .number_string => |raw| std.fmt.parseFloat(f32, raw) catch return error.InvalidQueryRequest,
        else => return error.InvalidQueryRequest,
    };
    if (!std.math.isFinite(parsed)) return error.InvalidQueryRequest;
    return parsed;
}

fn parseOptionalReturnModeJson(value_opt: ?std.json.Value) !db_mod.types.ReturnMode {
    const value = value_opt orelse return .parent;
    if (value != .string) return error.InvalidQueryRequest;
    return std.meta.stringToEnum(db_mod.types.ReturnMode, value.string) orelse return error.InvalidQueryRequest;
}

fn parseStringJsonAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]const u8 {
    if (value != .string) return error.InvalidQueryRequest;
    if (value.string.len == 0) return "";
    return try alloc.dupe(u8, value.string);
}

fn parseStringArrayJsonAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![][]const u8 {
    if (value != .array) return error.InvalidQueryRequest;
    const out = try alloc.alloc([]const u8, value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |item| alloc.free(item);
        if (out.len > 0) alloc.free(out);
    }
    for (value.array.items) |item| {
        if (item != .string) return error.InvalidQueryRequest;
        out[initialized] = try alloc.dupe(u8, item.string);
        initialized += 1;
    }
    return out;
}

fn parseF32JsonArrayAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]const f32 {
    if (value != .array) return error.InvalidQueryRequest;
    const out = try alloc.alloc(f32, value.array.items.len);
    errdefer if (out.len > 0) alloc.free(out);
    for (value.array.items, 0..) |item, i| {
        out[i] = switch (item) {
            .integer => |raw| @floatFromInt(raw),
            .float => |raw| @floatCast(raw),
            .number_string => |raw| std.fmt.parseFloat(f32, raw) catch return error.InvalidQueryRequest,
            else => return error.InvalidQueryRequest,
        };
        if (!std.math.isFinite(out[i])) return error.InvalidQueryRequest;
    }
    return out;
}

fn parseU32JsonArrayAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]const u32 {
    if (value != .array) return error.InvalidQueryRequest;
    const out = try alloc.alloc(u32, value.array.items.len);
    errdefer if (out.len > 0) alloc.free(out);
    for (value.array.items, 0..) |item, i| {
        if (item != .integer or item.integer < 0) return error.InvalidQueryRequest;
        out[i] = std.math.cast(u32, item.integer) orelse return error.InvalidQueryRequest;
    }
    return out;
}

fn parseU64JsonArrayAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]u64 {
    if (value != .array) return error.InvalidQueryRequest;
    const out = try alloc.alloc(u64, value.array.items.len);
    errdefer if (out.len > 0) alloc.free(out);
    for (value.array.items, 0..) |item, i| {
        if (item != .integer or item.integer < 0) return error.InvalidQueryRequest;
        out[i] = std.math.cast(u64, item.integer) orelse return error.InvalidQueryRequest;
    }
    return out;
}

fn parseAlgebraicTensorProgramRefsAlloc(
    alloc: std.mem.Allocator,
    refs: []const AlgebraicTensorProgramRefInput,
) ![]algebraic_ir.TensorProgramRef {
    const out = try alloc.alloc(algebraic_ir.TensorProgramRef, refs.len);
    errdefer if (out.len > 0) alloc.free(out);
    for (refs, 0..) |input, i| {
        out[i] = parseAlgebraicTensorProgramRef(input) orelse return error.InvalidQueryRequest;
    }
    return out;
}

fn parseAlgebraicTensorProgramRef(input: AlgebraicTensorProgramRefInput) ?algebraic_ir.TensorProgramRef {
    if (std.mem.eql(u8, input.kind, "input")) return .{ .input = input.index };
    if (std.mem.eql(u8, input.kind, "step")) return .{ .step = input.index };
    return null;
}

pub fn parseAlgebraicTensorAccessPathEnvelopeInputAlloc(
    alloc: std.mem.Allocator,
    input: AlgebraicTensorAccessPathEnvelopeInput,
) !OwnedAlgebraicTensorAccessPathEnvelope {
    if (input.owner.len == 0) return error.InvalidQueryRequest;
    const layout = std.meta.stringToEnum(algebraic_ir.PhysicalLayout, input.layout) orelse return error.InvalidQueryRequest;
    var dictionary = if (input.dictionary) |value| try parseAlgebraicDictionaryIdentityAlloc(alloc, value) else null;
    errdefer if (dictionary) |*value| value.deinit(alloc);

    const fragments = try alloc.alloc(algebraic_ir.TensorFragment, input.fragments.len);
    errdefer if (fragments.len > 0) alloc.free(fragments);
    for (input.fragments, 0..) |fragment, i| {
        fragments[i] = std.meta.stringToEnum(algebraic_ir.TensorFragment, fragment) orelse return error.InvalidQueryRequest;
    }

    const output_dims = try alloc.alloc(algebraic_ir.Dimension, input.output_dims.len);
    errdefer if (output_dims.len > 0) alloc.free(output_dims);
    for (input.output_dims, 0..) |dim, i| {
        output_dims[i] = std.meta.stringToEnum(algebraic_ir.Dimension, dim) orelse return error.InvalidQueryRequest;
    }

    const law_ids = try alloc.alloc(algebraic_law.Id, input.law_ids.len);
    errdefer if (law_ids.len > 0) alloc.free(law_ids);
    for (input.law_ids, 0..) |law_id, i| {
        law_ids[i] = algebraic_law.Id.parse(law_id) orelse return error.InvalidQueryRequest;
    }
    const owner = try alloc.dupe(u8, input.owner);

    return .{
        .owner = owner,
        .layout = layout,
        .dictionary = dictionary,
        .fragments = fragments,
        .output_dims = output_dims,
        .law_ids = law_ids,
    };
}

fn parseAlgebraicDictionaryIdentityAlloc(
    alloc: std.mem.Allocator,
    input: AlgebraicDictionaryIdentityInput,
) !OwnedAlgebraicDictionaryIdentity {
    const label_kind = std.meta.stringToEnum(algebraic_lexical.LabelKind, input.label_kind) orelse return error.InvalidQueryRequest;
    const scope = try alloc.dupe(u8, input.scope);
    errdefer alloc.free(scope);
    const field_or_path = try alloc.dupe(u8, input.field_or_path);
    errdefer alloc.free(field_or_path);
    const analyzer_or_canonicalization = try alloc.dupe(u8, input.analyzer_or_canonicalization);
    errdefer alloc.free(analyzer_or_canonicalization);
    const value_kind = try alloc.dupe(u8, input.value_kind);
    errdefer alloc.free(value_kind);
    const coercion_policy = try alloc.dupe(u8, input.coercion_policy);
    errdefer alloc.free(coercion_policy);
    return .{
        .scope = scope,
        .field_or_path = field_or_path,
        .label_kind = label_kind,
        .analyzer_or_canonicalization = analyzer_or_canonicalization,
        .value_kind = value_kind,
        .coercion_policy = coercion_policy,
    };
}

fn parseAlgebraicDimensionArrayAlloc(
    alloc: std.mem.Allocator,
    values: []const []const u8,
) ![]algebraic_ir.Dimension {
    const dims = try alloc.alloc(algebraic_ir.Dimension, values.len);
    errdefer if (dims.len > 0) alloc.free(dims);
    for (values, 0..) |value, i| {
        dims[i] = std.meta.stringToEnum(algebraic_ir.Dimension, value) orelse return error.InvalidQueryRequest;
    }
    return dims;
}

pub fn applyNativeDocIdConstraintEnvelope(req: *db_mod.types.SearchRequest, constraints: NativeDocIdConstraintEnvelope) void {
    if (constraints.positive_filter or constraints.include_doc_ids.len > 0) {
        req.filter_doc_ids_positive = true;
        req.filter_doc_ids = constraints.include_doc_ids;
    }
    if (constraints.exclude_doc_ids.len > 0) req.exclude_doc_ids = constraints.exclude_doc_ids;
}

/// Projects the owned internal vector-worker wire envelope into the
/// transport-neutral storage request consumed by the group query operation.
pub fn searchRequestFromVectorWorkerEnvelope(envelope: *const OwnedAlgebraicVectorWorkerRequestEnvelope) db_mod.types.SearchRequest {
    var req = switch (envelope.query) {
        .dense => |dense| db_mod.types.SearchRequest{
            .index_name = envelope.index_name,
            .limit = envelope.options.limit,
            .offset = envelope.options.offset,
            .count_only = envelope.options.count_only,
            .profile = envelope.options.profile,
            .include_stored = envelope.options.include_stored,
            .fields = envelope.options.fields,
            .filter_query_json = envelope.options.filter_query_json,
            .exclusion_query_json = envelope.options.exclusion_query_json,
            .filter_prefix = envelope.options.filter_prefix,
            .filter_ids = envelope.options.filter_ids,
            .exclude_ids = envelope.options.exclude_ids,
            .require_algebraic_filter_resolution = envelope.options.require_algebraic_filter_resolution,
            .include_all_fields = envelope.options.include_all_fields,
            .defer_stored_projection = envelope.options.defer_stored_projection,
            .search_effort = envelope.options.search_effort,
            .distance_over = envelope.options.distance_over,
            .distance_under = envelope.options.distance_under,
            .return_mode = envelope.options.return_mode,
            .max_chunks_per_parent = envelope.options.max_chunks_per_parent,
            .hierarchy_include_source = envelope.options.hierarchy_include_source,
            .hierarchy_include_unit = envelope.options.hierarchy_include_unit,
            .hierarchy_omit_implicit_source_ancestor_document = envelope.options.hierarchy_omit_implicit_source_ancestor_document,
            .hierarchy_match_fields = envelope.options.hierarchy_match_fields,
            .hierarchy_match_include_all_fields = envelope.options.hierarchy_match_include_all_fields,
            .hierarchy_grouped_matches = envelope.options.hierarchy_grouped_matches,
            .hierarchy_group_level = envelope.options.hierarchy_group_level,
            .defer_hierarchy_child_hydration = envelope.options.defer_hierarchy_child_hydration,
            .hierarchy_source_fields = envelope.options.hierarchy_source_fields,
            .hierarchy_source_include_all_fields = envelope.options.hierarchy_source_include_all_fields,
            .hierarchy_unit_fields = envelope.options.hierarchy_unit_fields,
            .hierarchy_unit_include_all_fields = envelope.options.hierarchy_unit_include_all_fields,
            .identity_read_generation = envelope.options.identity_read_generation,
            .resolved_doc_filter = envelope.resolved_doc_filter,
            .resolved_doc_filter_wire_context = envelope.resolved_doc_filter_wire_context,
            .query = .{ .dense_knn = dense },
        },
        .sparse => |sparse| db_mod.types.SearchRequest{
            .index_name = envelope.index_name,
            .limit = envelope.options.limit,
            .offset = envelope.options.offset,
            .count_only = envelope.options.count_only,
            .profile = envelope.options.profile,
            .include_stored = envelope.options.include_stored,
            .fields = envelope.options.fields,
            .filter_query_json = envelope.options.filter_query_json,
            .exclusion_query_json = envelope.options.exclusion_query_json,
            .filter_prefix = envelope.options.filter_prefix,
            .filter_ids = envelope.options.filter_ids,
            .exclude_ids = envelope.options.exclude_ids,
            .require_algebraic_filter_resolution = envelope.options.require_algebraic_filter_resolution,
            .include_all_fields = envelope.options.include_all_fields,
            .defer_stored_projection = envelope.options.defer_stored_projection,
            .search_effort = envelope.options.search_effort,
            .distance_over = envelope.options.distance_over,
            .distance_under = envelope.options.distance_under,
            .return_mode = envelope.options.return_mode,
            .max_chunks_per_parent = envelope.options.max_chunks_per_parent,
            .hierarchy_include_source = envelope.options.hierarchy_include_source,
            .hierarchy_include_unit = envelope.options.hierarchy_include_unit,
            .hierarchy_omit_implicit_source_ancestor_document = envelope.options.hierarchy_omit_implicit_source_ancestor_document,
            .hierarchy_match_fields = envelope.options.hierarchy_match_fields,
            .hierarchy_match_include_all_fields = envelope.options.hierarchy_match_include_all_fields,
            .hierarchy_grouped_matches = envelope.options.hierarchy_grouped_matches,
            .hierarchy_group_level = envelope.options.hierarchy_group_level,
            .defer_hierarchy_child_hydration = envelope.options.defer_hierarchy_child_hydration,
            .hierarchy_source_fields = envelope.options.hierarchy_source_fields,
            .hierarchy_source_include_all_fields = envelope.options.hierarchy_source_include_all_fields,
            .hierarchy_unit_fields = envelope.options.hierarchy_unit_fields,
            .hierarchy_unit_include_all_fields = envelope.options.hierarchy_unit_include_all_fields,
            .identity_read_generation = envelope.options.identity_read_generation,
            .resolved_doc_filter = envelope.resolved_doc_filter,
            .resolved_doc_filter_wire_context = envelope.resolved_doc_filter_wire_context,
            .query = .{ .sparse_knn = sparse },
        },
    };
    applyNativeDocIdConstraintEnvelope(&req, envelope.native_doc_id_constraints.constraints);
    return req;
}

pub fn encodeNativeDocIdConstraintEnvelopeAlloc(
    alloc: std.mem.Allocator,
    constraints: NativeDocIdConstraintEnvelope,
) ![]u8 {
    var include_doc_ids = try normalizedDocIdArrayAlloc(alloc, constraints.include_doc_ids);
    defer freeOwnedStringSlice(alloc, include_doc_ids);
    const exclude_doc_ids = try normalizedDocIdArrayAlloc(alloc, constraints.exclude_doc_ids);
    defer freeOwnedStringSlice(alloc, exclude_doc_ids);
    include_doc_ids = try subtractSortedOwnedStringsAlloc(alloc, include_doc_ids, exclude_doc_ids);
    const positive_filter = constraints.positive_filter or include_doc_ids.len > 0;
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    var first = true;
    try out.append(alloc, '{');
    try appendJsonFieldBool(alloc, &out, &first, "positive_filter", positive_filter);
    try appendJsonFieldStringArray(alloc, &out, &first, "include_doc_ids", include_doc_ids);
    if (exclude_doc_ids.len > 0) {
        try appendJsonFieldStringArray(alloc, &out, &first, "exclude_doc_ids", exclude_doc_ids);
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

fn normalizedDocIdArrayAlloc(
    alloc: std.mem.Allocator,
    values: []const []const u8,
) ![][]const u8 {
    if (values.len == 0) return &.{};
    const out = try alloc.alloc([]const u8, values.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |item| alloc.free(@constCast(item));
        alloc.free(out);
    }
    for (values) |value| {
        out[initialized] = try alloc.dupe(u8, value);
        initialized += 1;
    }
    return try sortAndDedupeOwnedStringArrayAlloc(alloc, out);
}

fn sortAndDedupeOwnedStringArrayAlloc(
    alloc: std.mem.Allocator,
    values: [][]const u8,
) ![][]const u8 {
    if (values.len == 0) return &.{};
    std.mem.sort([]const u8, values, {}, stringLessThan);

    var write: usize = 0;
    for (values, 0..) |value, read| {
        if (read > 0 and std.mem.eql(u8, value, values[read - 1])) {
            alloc.free(@constCast(value));
            continue;
        }
        values[write] = value;
        write += 1;
    }
    if (write == values.len) return values;
    const resized = alloc.realloc(values, write) catch |err| {
        for (values[0..write]) |value| alloc.free(@constCast(value));
        alloc.free(values);
        return err;
    };
    return resized;
}

fn subtractSortedOwnedStringsAlloc(
    alloc: std.mem.Allocator,
    values: [][]const u8,
    excluded: []const []const u8,
) ![][]const u8 {
    if (values.len == 0 or excluded.len == 0) return values;

    var write: usize = 0;
    var exclude_idx: usize = 0;
    for (values) |value| {
        while (exclude_idx < excluded.len and std.mem.lessThan(u8, excluded[exclude_idx], value)) {
            exclude_idx += 1;
        }
        if (exclude_idx < excluded.len and std.mem.eql(u8, excluded[exclude_idx], value)) {
            alloc.free(@constCast(value));
            continue;
        }
        values[write] = value;
        write += 1;
    }
    if (write == values.len) return values;
    const resized = alloc.realloc(values, write) catch |err| {
        for (values[0..write]) |value| alloc.free(@constCast(value));
        alloc.free(values);
        return err;
    };
    return resized;
}

fn stringLessThan(_: void, left: []const u8, right: []const u8) bool {
    return std.mem.lessThan(u8, left, right);
}

fn appendJsonFieldStringArray(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    values: []const []const u8,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try out.append(alloc, '[');
    for (values, 0..) |value, i| {
        if (i > 0) try out.append(alloc, ',');
        try appendJsonString(alloc, out, value);
    }
    try out.append(alloc, ']');
}

fn appendEnumNameArray(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    comptime T: type,
    values: []const T,
) !void {
    try out.append(alloc, '[');
    for (values, 0..) |value, i| {
        if (i > 0) try out.append(alloc, ',');
        try appendJsonString(alloc, out, @tagName(value));
    }
    try out.append(alloc, ']');
}

pub fn parseNativeDocIdConstraintEnvelopeAlloc(
    alloc: std.mem.Allocator,
    body: []const u8,
) !OwnedNativeDocIdConstraintEnvelope {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{}) catch return error.InvalidQueryRequest;
    defer parsed.deinit();
    return parseNativeDocIdConstraintEnvelopeValueAlloc(alloc, parsed.value);
}

fn parseNativeDocIdConstraintEnvelopeValueAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
) !OwnedNativeDocIdConstraintEnvelope {
    if (value != .object) return error.InvalidQueryRequest;

    var out = OwnedNativeDocIdConstraintEnvelope{ .constraints = .{} };
    errdefer out.deinit(alloc);

    var include_doc_ids: [][]const u8 = &.{};
    errdefer freeOwnedStringSlice(alloc, include_doc_ids);
    var exclude_doc_ids: [][]const u8 = &.{};
    errdefer freeOwnedStringSlice(alloc, exclude_doc_ids);

    if (value.object.get("positive_filter")) |positive| {
        if (positive != .bool) return error.InvalidQueryRequest;
        out.constraints.positive_filter = positive.bool;
    }
    if (value.object.get("include_doc_ids")) |include| {
        include_doc_ids = try parseInternalDocIdArrayAlloc(alloc, include);
        if (include_doc_ids.len > 0) out.constraints.positive_filter = true;
    }
    if (value.object.get("exclude_doc_ids")) |exclude| {
        exclude_doc_ids = try parseInternalDocIdArrayAlloc(alloc, exclude);
    }
    include_doc_ids = try subtractSortedOwnedStringsAlloc(alloc, include_doc_ids, exclude_doc_ids);
    out.constraints.include_doc_ids = include_doc_ids;
    out.constraints.exclude_doc_ids = exclude_doc_ids;
    include_doc_ids = &.{};
    exclude_doc_ids = &.{};
    return out;
}

fn freeOwnedMutableStrings(alloc: std.mem.Allocator, values: [][]u8) void {
    for (values) |value| alloc.free(value);
    if (values.len > 0) alloc.free(values);
}

pub const OwnedQueryRequest = struct {
    fields: [][]const u8 = &.{},
    req: db_mod.types.SearchRequest = .{},

    pub fn deinit(self: *OwnedQueryRequest, alloc: std.mem.Allocator) void {
        if (self.fields.len > 0) {
            for (self.fields) |field| alloc.free(field);
            alloc.free(self.fields);
        }
        freeSearchRequest(alloc, &self.req);
        self.* = undefined;
    }
};

pub const QueryPreflightSummary = struct {
    full_text_indexes: []const []const u8 = &.{},
    embedding_indexes: []const []const u8 = &.{},
    graph_indexes: []const []const u8 = &.{},
    result_refs: []const []const u8 = &.{},
    graph_query_order: []const []const u8 = &.{},
    requested_limit: u32 = 10,
    requested_offset: u32 = 0,
    base_result_set_count: u32 = 0,
    graph_query_count: u32 = 0,
    requires_fusion: bool = false,
    count_only: bool = false,
    profile_requested: bool = false,
    include_stored: bool = false,
    reranker_enabled: bool = false,
    aggregation_count: u32 = 0,

    pub fn deinit(self: *const @This(), alloc: std.mem.Allocator) void {
        freeOwnedStringSlice(alloc, self.full_text_indexes);
        freeOwnedStringSlice(alloc, self.embedding_indexes);
        freeOwnedStringSlice(alloc, self.graph_indexes);
        freeOwnedStringSlice(alloc, self.result_refs);
        freeOwnedStringSlice(alloc, self.graph_query_order);
    }
};

const NamedVectorQueries = struct {
    dense: []const db_mod.types.NamedDenseQuery = &.{},
    sparse: []const db_mod.types.NamedSparseQuery = &.{},

    fn deinit(self: *const NamedVectorQueries, alloc: std.mem.Allocator) void {
        freeNamedDenseQueries(alloc, self.dense);
        freeNamedSparseQueries(alloc, self.sparse);
    }
};

pub const SemanticResolver = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        resolve_dense_query: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            index_name: []const u8,
            semantic_search: []const u8,
            embedding_template: ?[]const u8,
            limit: u32,
        ) anyerror!db_mod.types.DenseKnnQuery,
    };

    pub fn resolveDenseQuery(
        self: SemanticResolver,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
        semantic_search: []const u8,
        embedding_template: ?[]const u8,
        limit: u32,
    ) !db_mod.types.DenseKnnQuery {
        return try self.vtable.resolve_dense_query(self.ptr, alloc, table_name, index_name, semantic_search, embedding_template, limit);
    }
};

fn applyCommonSearchRequestOptions(
    alloc: std.mem.Allocator,
    request: anytype,
    req: *db_mod.types.SearchRequest,
) !void {
    if (request.limit) |limit| req.limit = @intCast(limit);
    if (request.offset) |offset| req.offset = @intCast(offset);
    if (request.count) |count| req.count_only = count;
    const has_result_page_options =
        request.order_by != null or
        request.search_after != null or
        request.search_before != null;
    const has_result_page_transforms = request.reranker != null;
    if (req.count_only and (has_result_page_options or has_result_page_transforms)) {
        if (has_result_page_options) return unsupportedExactSort("*", "unsupported_exact_sort", "count_only_ordered_page");
        return error.UnsupportedQueryRequest;
    }
    if (request.profile) |profile| req.profile = profile;
    if (request.aggregations) |aggregations| {
        req.aggregations_json = try jsonStringifyAlloc(alloc, aggregations);
    }
    if (request.filter_prefix) |filter_prefix| req.filter_prefix = try alloc.dupe(u8, filter_prefix);
    if (request.distance_over) |distance_over| req.distance_over = distance_over;
    if (request.distance_under) |distance_under| req.distance_under = distance_under;
    req.search_effort = request.search_effort;
    if (request.order_by) |order_by| {
        req.order_by = try cloneSortFieldsWithStableTiebreaker(alloc, order_by);
    } else if (request.search_after != null or request.search_before != null) {
        req.order_by = try cloneDefaultIdSortField(alloc);
    }
    if (request.search_after) |search_after| req.search_after = try cloneJsonValues(alloc, search_after);
    if (request.search_before) |search_before| req.search_before = try cloneJsonValues(alloc, search_before);
    if (request.merge_config) |merge_config| req.merge_config = try parseMergeConfig(alloc, merge_config);
    if (request.pruner) |pruner| req.pruner = try parsePruner(pruner);
    if (request.reranker) |reranker| {
        const encoded_reranker = try std.json.Stringify.valueAlloc(alloc, reranker, .{});
        defer alloc.free(encoded_reranker);
        req.reranker = reranking_mod.parseConfigFromSlice(alloc, encoded_reranker) catch |err| switch (err) {
            error.InvalidRerankerConfig => return error.InvalidQueryRequest,
            else => return err,
        };
    }

    const has_semantic = request.semantic_search != null or request.embeddings != null;
    if (has_semantic and req.offset > 0) return error.UnsupportedQueryRequest;
    if (has_semantic and req.order_by.len > 0) {
        return unsupportedExactSort(approximateSemanticSortField(req.order_by), "approximate_candidate_source", "approximate_candidate_source");
    }
    if (req.order_by.len > 0 and req.offset > 0 and (req.search_after.len > 0 or req.search_before.len > 0)) {
        return unsupportedExactSort("*", "invalid_cursor_arity", "invalid_cursor_arity");
    }
    if (req.search_after.len > 0 and req.search_before.len > 0) {
        return unsupportedExactSort("*", "invalid_cursor_arity", "invalid_cursor_arity");
    }
    if (req.search_after.len > 0 and req.search_after.len != req.order_by.len) {
        return unsupportedExactSort("*", "invalid_cursor_arity", "invalid_cursor_arity");
    }
    if (req.search_before.len > 0 and req.search_before.len != req.order_by.len) {
        return unsupportedExactSort("*", "invalid_cursor_arity", "invalid_cursor_arity");
    }
    try validatePublicSortCursorTuple(req.*);
    if (request.embedding_template != null and request.semantic_search == null) return error.UnsupportedQueryRequest;
    if (request.embedding_template != null and request.embeddings != null) return error.UnsupportedQueryRequest;
}

fn validatePublicSortCursorTuple(req: db_mod.types.SearchRequest) !void {
    const cursor = if (req.search_after.len > 0) req.search_after else req.search_before;
    if (cursor.len == 0) return;
    for (cursor, 0..) |value, i| {
        const field = req.order_by[i].field;
        if (!openApiSortValueIsCursorReplayable(value)) return invalidExactSortCursor(field, "invalid_cursor_type", "invalid_cursor_type");
        if (std.mem.eql(u8, field, "_score") and !openApiSortValueIsNumeric(value)) return invalidExactSortCursor(field, "invalid_cursor_type", "invalid_cursor_type");
        if (std.mem.eql(u8, field, "_id") and value != .string) return invalidExactSortCursor(field, "invalid_cursor_type", "invalid_cursor_type");
    }
}

fn recordUnsupportedExactSortDiagnostic(field: []const u8, reason: []const u8, detail: []const u8) void {
    db_mod.recordSortRejectionDiagnostic(field, reason, detail);
}

fn unsupportedExactSort(field: []const u8, reason: []const u8, detail: []const u8) error{UnsupportedQueryRequest} {
    recordUnsupportedExactSortDiagnostic(field, reason, detail);
    return error.UnsupportedQueryRequest;
}

fn invalidExactSortCursor(field: []const u8, reason: []const u8, detail: []const u8) error{InvalidQueryRequest} {
    recordUnsupportedExactSortDiagnostic(field, reason, detail);
    return error.InvalidQueryRequest;
}

fn cloneSortFieldsWithStableTiebreaker(
    alloc: std.mem.Allocator,
    fields: []const metadata_openapi.SortField,
) ![]const db_mod.types.SortField {
    if (fields.len == 0) return unsupportedExactSort("*", "invalid_cursor_arity", "invalid_cursor_arity");
    var has_final_id = false;
    for (fields, 0..) |field, i| {
        if (field.field.len == 0) return unsupportedExactSort("*", "invalid_cursor_arity", "invalid_cursor_arity");
        for (fields[0..i]) |prior| {
            if (std.mem.eql(u8, prior.field, field.field)) {
                return unsupportedExactSort(field.field, "invalid_cursor_arity", "invalid_cursor_arity");
            }
        }
        if (std.mem.eql(u8, field.field, "_id")) {
            if (i + 1 != fields.len or (field.desc orelse false)) {
                return unsupportedExactSort("_id", "invalid_cursor_arity", "invalid_cursor_arity");
            }
            has_final_id = true;
        }
    }

    const out_len = fields.len + @as(usize, if (has_final_id) 0 else 1);
    const out = try alloc.alloc(db_mod.types.SortField, out_len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |item| alloc.free(item.field);
        alloc.free(out);
    }
    for (fields, 0..) |field, i| {
        out[i] = .{
            .field = try alloc.dupe(u8, field.field),
            .desc = field.desc orelse false,
        };
        initialized += 1;
    }
    if (!has_final_id) {
        out[fields.len] = .{
            .field = try alloc.dupe(u8, "_id"),
            .desc = false,
        };
        initialized += 1;
    }
    return out;
}

fn cloneDefaultIdSortField(alloc: std.mem.Allocator) ![]const db_mod.types.SortField {
    const out = try alloc.alloc(db_mod.types.SortField, 1);
    errdefer alloc.free(out);
    out[0] = .{
        .field = try alloc.dupe(u8, "_id"),
        .desc = false,
    };
    return out;
}

fn applySearchRequestFields(
    alloc: std.mem.Allocator,
    generated_fields: ?[]const []const u8,
    req: *db_mod.types.SearchRequest,
) ![][]const u8 {
    const include_all_fields = generated_fields == null;
    const fields: [][]const u8 = if (generated_fields) |items|
        try cloneFields(alloc, items)
    else
        &.{};
    req.fields = fields;
    req.include_all_fields = include_all_fields;
    req.include_stored = include_all_fields or fields.len > 0 or req.reranker != null;
    req.defer_stored_projection = canDeferStoredProjection(fields) or
        !req.hierarchy_match_include_all_fields or
        !req.hierarchy_source_include_all_fields or
        !req.hierarchy_unit_include_all_fields;
    return fields;
}

fn freeClonedFields(alloc: std.mem.Allocator, fields: []const []const u8) void {
    for (fields) |field| alloc.free(field);
    if (fields.len > 0) alloc.free(fields);
}

fn cloneJsonValues(alloc: std.mem.Allocator, values: []const std.json.Value) ![]std.json.Value {
    return try db_mod.types.cloneJsonValues(alloc, values);
}

fn freeClonedJsonValues(alloc: std.mem.Allocator, values: []const std.json.Value) void {
    db_mod.types.freeJsonValues(alloc, @constCast(values));
}

fn parseQueryTimeoutMs(alloc: std.mem.Allocator, body: []const u8) !?u64 {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{}) catch return error.InvalidQueryRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidQueryRequest;
    const value = parsed.value.object.get("timeout_ms") orelse return null;
    return switch (value) {
        .null => null,
        .integer => |v| if (v >= 0) @as(u64, @intCast(v)) else error.InvalidQueryRequest,
        .float => error.InvalidQueryRequest,
        .number_string => |v| std.fmt.parseUnsigned(u64, v, 10) catch error.InvalidQueryRequest,
        else => error.InvalidQueryRequest,
    };
}

const QueryBodyContractFields = struct {
    has_internal_shard_fields: bool,
    has_public_doc_filter_bindings: bool,
    has_public_hierarchy_controls: bool,
    has_query_timeout: bool,
};

const RawGraphValueEntry = struct {
    value: std.json.Value,
    depth: usize,
};

/// Validate graph request shape before the generated recursive union decoder
/// runs. This keeps public input recursion and aggregate DFS depth within the
/// same small, deterministic contract budget.
pub fn validateRawGraphQueriesValueAlloc(
    alloc: std.mem.Allocator,
    root: std.json.Value,
) !void {
    if (root != .object) return error.InvalidQueryRequest;
    const graph_queries = root.object.get("graph_queries") orelse return;
    if (graph_queries == .null) return;
    if (graph_queries != .object or graph_queries.object.count() == 0 or graph_queries.object.count() > graph_query_mod.max_named_queries)
        return error.InvalidQueryRequest;

    var pending = std.ArrayListUnmanaged(RawGraphValueEntry).empty;
    defer pending.deinit(alloc);
    var query_it = graph_queries.object.iterator();
    while (query_it.next()) |entry| {
        try pending.append(alloc, .{ .value = entry.value_ptr.*, .depth = 0 });
        if (entry.value_ptr.* == .object) {
            if (entry.value_ptr.object.get("match")) |match_value| {
                try validateRawGraphMatchComplexityAlloc(alloc, match_value);
            }
            if (entry.value_ptr.object.get("return")) |return_value| {
                if (return_value != .object) return error.InvalidQueryRequest;
                if (return_value.object.get("aggregates")) |aggregates| {
                    if (aggregates != .object or
                        aggregates.object.count() == 0 or
                        aggregates.object.count() > graph_pattern_mod.max_count_aggregates)
                    {
                        return error.InvalidQueryRequest;
                    }
                }
            }
        }
    }

    var visited: usize = 0;
    while (pending.pop()) |entry| {
        visited += 1;
        if (visited > public_query_max_tree_nodes or entry.depth > public_query_max_tree_depth)
            return error.InvalidQueryRequest;
        const child_depth = entry.depth + 1;
        switch (entry.value) {
            .array => |array| for (array.items) |child| {
                try pending.append(alloc, .{ .value = child, .depth = child_depth });
            },
            .object => |object| {
                var it = object.iterator();
                while (it.next()) |child| {
                    try pending.append(alloc, .{ .value = child.value_ptr.*, .depth = child_depth });
                }
            },
            else => {},
        }
    }
}

test "public query envelope rejects an explicitly empty graph_queries object" {
    try std.testing.expectError(
        error.InvalidQueryRequest,
        queryBodyContractFields(std.testing.allocator, "{\"graph_queries\":{}}"),
    );
}

/// Validate controls that must be rejected at every public query boundary.
/// Routing layers should parse the request envelope once and borrow that tree
/// here before selecting foreign, join, graph, or ordinary query execution.
pub fn validatePublicQueryEnvelopeValueAlloc(
    alloc: std.mem.Allocator,
    root: std.json.Value,
) !void {
    if (root != .object) return error.InvalidQueryRequest;
    if (objectHasForbiddenPublicQueryField(root.object))
        return error.InvalidQueryRequest;
    try validateRawGraphQueriesValueAlloc(alloc, root);
}

fn addRawGraphComplexity(total: *usize, amount: usize, limit: usize) !void {
    total.* = std.math.add(usize, total.*, amount) catch return error.InvalidQueryRequest;
    if (total.* > limit) return error.InvalidQueryRequest;
}

fn rawGraphObjectCount(value: ?std.json.Value) !usize {
    const item = value orelse return 0;
    if (item != .object) return error.InvalidQueryRequest;
    return item.object.count();
}

fn rawGraphArrayLength(value: ?std.json.Value) !usize {
    const item = value orelse return 0;
    if (item != .array) return error.InvalidQueryRequest;
    return item.array.items.len;
}

fn validateRawGraphMatchComplexityAlloc(
    alloc: std.mem.Allocator,
    match_value: std.json.Value,
) !void {
    if (match_value != .object) return error.InvalidQueryRequest;
    var total_nodes = try rawGraphObjectCount(match_value.object.get("nodes"));
    var total_edges = try rawGraphArrayLength(match_value.object.get("edges"));
    var total_predicates: usize = 0;
    if (total_nodes == 0 or
        total_nodes > graph_pattern_mod.max_conjunctive_nodes or
        total_edges > graph_pattern_mod.max_conjunctive_edges)
        return error.InvalidQueryRequest;

    if (match_value.object.get("where")) |where| {
        try validateRawGraphWhereAlloc(alloc, where, &total_edges, &total_predicates);
    }
    if (match_value.object.get("optional")) |optional_value| {
        if (optional_value != .array or optional_value.array.items.len > graph_pattern_mod.max_optional_patterns)
            return error.InvalidQueryRequest;
        for (optional_value.array.items) |group| {
            if (group != .object) return error.InvalidQueryRequest;
            try addRawGraphComplexity(
                &total_nodes,
                try rawGraphObjectCount(group.object.get("nodes")),
                graph_pattern_mod.max_conjunctive_nodes,
            );
            const group_edges = try rawGraphArrayLength(group.object.get("edges"));
            if (group_edges == 0) return error.InvalidQueryRequest;
            try addRawGraphComplexity(&total_edges, group_edges, graph_pattern_mod.max_conjunctive_edges);
            if (group.object.get("where")) |where| {
                try validateRawGraphWhereAlloc(alloc, where, &total_edges, &total_predicates);
            }
        }
    }
}

fn validateRawGraphWhereAlloc(
    alloc: std.mem.Allocator,
    root: std.json.Value,
    total_edges: *usize,
    total_predicates: *usize,
) !void {
    var pending = std.ArrayListUnmanaged(RawGraphValueEntry).empty;
    defer pending.deinit(alloc);
    try pending.append(alloc, .{ .value = root, .depth = 0 });
    while (pending.pop()) |entry| {
        if (entry.depth >= graph_pattern_mod.max_match_predicate_depth or entry.value != .object)
            return error.InvalidQueryRequest;
        if (entry.value.object.get("and")) |children| {
            if (children != .array or
                children.array.items.len == 0 or
                children.array.items.len > graph_pattern_mod.max_match_predicates)
                return error.InvalidQueryRequest;
            for (children.array.items) |child| {
                try pending.append(alloc, .{ .value = child, .depth = entry.depth + 1 });
            }
            continue;
        }
        try addRawGraphComplexity(total_predicates, 1, graph_pattern_mod.max_match_predicates);
        if (entry.value.object.get("not_exists")) |not_exists| {
            if (not_exists != .object) return error.InvalidQueryRequest;
            const edges = try rawGraphArrayLength(not_exists.object.get("edges"));
            if (edges == 0) return error.InvalidQueryRequest;
            try addRawGraphComplexity(total_edges, edges, graph_pattern_mod.max_conjunctive_edges);
        }
    }
}

fn queryBodyContractFields(alloc: std.mem.Allocator, body: []const u8) !QueryBodyContractFields {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{}) catch return error.InvalidQueryRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidQueryRequest;
    try validateRawGraphQueriesValueAlloc(alloc, parsed.value);
    return .{
        .has_internal_shard_fields = objectHasInternalShardField(parsed.value.object),
        .has_public_doc_filter_bindings = parsed.value.object.get("with") != null,
        .has_public_hierarchy_controls = objectHasNonNullField(parsed.value.object, "hierarchy"),
        .has_query_timeout = parsed.value.object.get("timeout_ms") != null,
    };
}

pub fn queryExecutionDeadlineNsFromBody(alloc: std.mem.Allocator, body: []const u8) !?u64 {
    const started_ns = platform_time.monotonicNs();
    const timeout_ms = (try parseQueryTimeoutMs(alloc, body)) orelse return null;
    return started_ns +| timeout_ms *| std.time.ns_per_ms;
}

fn ensureQueryDeadline(deadline_ns: ?u64) !void {
    if (deadline_ns) |deadline| {
        if (platform_time.monotonicNs() >= deadline) return error.Timeout;
    }
}

const QueryDeadlinePoller = struct {
    deadline_ns: ?u64,
    work_until_check: u8 = 1,

    fn poll(self: *@This()) !void {
        self.work_until_check -|= 1;
        if (self.work_until_check != 0) return;
        self.work_until_check = 64;
        try ensureQueryDeadline(self.deadline_ns);
    }

    fn check(self: *@This()) !void {
        self.work_until_check = 64;
        try ensureQueryDeadline(self.deadline_ns);
    }
};

pub fn parseQueryRequest(
    alloc: std.mem.Allocator,
    semantic_resolver: ?SemanticResolver,
    table_name: []const u8,
    body: []const u8,
) !OwnedQueryRequest {
    const execution_deadline_ns = try queryExecutionDeadlineNsFromBody(alloc, body);
    return try parseQueryRequestWithDeadline(
        alloc,
        semantic_resolver,
        table_name,
        body,
        execution_deadline_ns,
    );
}

/// Parse a query against the caller's absolute deadline. Public HTTP admission
/// computes this once and shares it with normalization, embedding, retries, and
/// execution so no stage silently receives a fresh timeout window.
pub fn parseQueryRequestWithDeadline(
    alloc: std.mem.Allocator,
    semantic_resolver: ?SemanticResolver,
    table_name: []const u8,
    body: []const u8,
    execution_deadline_ns: ?u64,
) !OwnedQueryRequest {
    if (body.len == 0) return error.InvalidQueryRequest;
    try ensureQueryDeadline(execution_deadline_ns);
    if (try queryBodyHasForbiddenDocIdentityControlFields(alloc, body)) return error.InvalidQueryRequest;
    try ensureQueryDeadline(execution_deadline_ns);

    // Structured named filters stay in compact binding form so the algebraic
    // resolver can cache and reuse them. Bindings that require the text index
    // are expanded into their public query positions before contract parsing;
    // this preserves one public AST without teaching the storage-only binding
    // resolver a second text-query representation.
    const expanded_binding_body = try maybeExpandPublicDocFilterBindingsAlloc(
        alloc,
        body,
        execution_deadline_ns,
    );
    defer if (expanded_binding_body) |owned| alloc.free(owned);
    const effective_body = expanded_binding_body orelse body;

    // Packed dense requests are benchmark-oriented and unusual in production.
    // Skip the extra JSON parse unless the request even mentions embeddings.
    if (std.mem.indexOf(u8, effective_body, "\"embeddings\"") != null and fastDensePublicQueryMayApply(effective_body)) {
        if (try tryParseFastDensePublicQueryRequest(alloc, effective_body)) |fast| {
            var owned = fast;
            errdefer owned.deinit(alloc);
            owned.req.execution_deadline_ns = execution_deadline_ns;
            try ensureQueryDeadline(execution_deadline_ns);
            return owned;
        }
    }

    // Inspect the generated-contract extensions in one semantic parse. Besides
    // honoring escaped member names, this avoids repeatedly materializing the
    // same request tree on the query admission hot path.
    const contract_fields = try queryBodyContractFields(alloc, effective_body);
    try ensureQueryDeadline(execution_deadline_ns);
    const contract_body = try queryBodyForGeneratedContractAlloc(alloc, effective_body, .{
        .strip_internal_shard_fields = contract_fields.has_internal_shard_fields,
        .strip_public_doc_filter_bindings = contract_fields.has_public_doc_filter_bindings,
        .strip_public_hierarchy_controls = contract_fields.has_public_hierarchy_controls,
        // Admission interprets this extension semantically, so escaped JSON
        // member names and the canonical spelling have identical behavior.
        .strip_query_timeout = contract_fields.has_query_timeout,
    });
    defer if (contract_body) |owned| alloc.free(owned);
    try ensureQueryDeadline(execution_deadline_ns);
    const body_for_contract = contract_body orelse effective_body;

    var parsed = ant_json.parseFromSlice(
        metadata_openapi.StatefulQueryRequest,
        alloc,
        body_for_contract,
        .{},
    ) catch return classifyPublicFilterContractErrorAlloc(alloc, effective_body);
    defer parsed.deinit();
    try ensureQueryDeadline(execution_deadline_ns);
    const request = parsed.value;

    if (request.analyses != null) return error.UnsupportedQueryRequest;
    if (request.document_renderer != null) return error.UnsupportedQueryRequest;
    if (request.join != null) return error.UnsupportedQueryRequest;
    if (request.foreign_sources != null) return error.UnsupportedQueryRequest;

    var req: db_mod.types.SearchRequest = .{};
    errdefer freeSearchRequest(alloc, &req);

    try applyCommonSearchRequestOptions(alloc, request, &req);
    req.execution_deadline_ns = execution_deadline_ns;
    try applyPublicHierarchyControls(alloc, effective_body, &req);
    req.distributed_text_stats = try parseDistributedTextStatsAlloc(alloc, effective_body);
    try parseInternalDocIdConstraintsAlloc(alloc, effective_body, &req);
    try ensureQueryDeadline(execution_deadline_ns);

    const fields = try applySearchRequestFields(alloc, request.fields, &req);
    errdefer freeClonedFields(alloc, fields);
    // Validate the normalized storage contract only after projection fields
    // have been applied. Child traversal deliberately rejects implicit
    // include-all projections at the DB boundary.
    try validateCanonicalHierarchyExecutionBudget(req);

    var normalized_query = try normalizePublicQueryBucketsAlloc(alloc, request, req.limit);
    errdefer normalized_query.deinit(alloc);
    try ensureQueryDeadline(execution_deadline_ns);

    if (req.reranker != null) {
        req.reranker_query_text = try buildRerankerQueryText(alloc, request);
    }

    if (normalized_query.full_text) |query| {
        if (request.full_text_index) |index_name| {
            req.full_text_queries = try singleNamedFullTextQueryAlloc(alloc, index_name, query);
        } else {
            req.full_text = query;
        }
        normalized_query.full_text = null;
    } else if (request.full_text_index != null) {
        return error.InvalidQueryRequest;
    } else if (normalized_query.filter_text != null or
        normalized_query.exclusion_text != null or
        normalized_query.filter_query_json.len > 0 or
        normalized_query.exclusion_query_json.len > 0)
    {
        req.full_text = .{ .match_all = {} };
    }

    req.filter_text = normalized_query.filter_text;
    normalized_query.filter_text = null;
    req.exclusion_text = normalized_query.exclusion_text;
    normalized_query.exclusion_text = null;
    req.filter_query_json = normalized_query.filter_query_json;
    normalized_query.filter_query_json = "";
    req.exclusion_query_json = normalized_query.exclusion_query_json;
    normalized_query.exclusion_query_json = "";
    try parseInternalFilterQueryJsonAlloc(alloc, effective_body, &req);
    req.doc_filter_bindings = try parsePublicDocFilterBindingsAlloc(
        alloc,
        effective_body,
        req.limit,
        execution_deadline_ns,
    );
    try validatePublicDocFilterRootRefsAlloc(
        alloc,
        req.doc_filter_bindings,
        req.filter_query_json,
        req.exclusion_query_json,
        execution_deadline_ns,
    );
    try ensureQueryDeadline(execution_deadline_ns);

    const vector_queries = try buildSemanticVectorQueries(alloc, semantic_resolver, table_name, request, req.limit);
    errdefer vector_queries.deinit(alloc);
    try ensureQueryDeadline(execution_deadline_ns);
    req.dense_queries = vector_queries.dense;
    req.sparse_queries = vector_queries.sparse;
    req.graph_queries = try buildGraphQueries(alloc, request);
    if (req.graph_queries.len > 0) {
        req.graph_query_transport = try captureGraphQueryTransportAlloc(alloc, effective_body, req.graph_queries);
    }
    if (comptime @hasField(@TypeOf(request), "expand_strategy")) {
        if (request.expand_strategy) |expand_strategy| {
            req.expand_strategy = try parseExpandStrategy(expand_strategy);
        }
    }
    try validateScoreSortHasScoreBearingSource(req);

    return .{
        .fields = fields,
        .req = req,
    };
}

pub fn parsePublicQueryRequest(
    alloc: std.mem.Allocator,
    semantic_resolver: ?SemanticResolver,
    table_name: []const u8,
    body: []const u8,
) !OwnedQueryRequest {
    const execution_deadline_ns = try queryExecutionDeadlineNsFromBody(alloc, body);
    return try parsePublicQueryRequestWithDeadline(
        alloc,
        semantic_resolver,
        table_name,
        body,
        execution_deadline_ns,
    );
}

pub fn parsePublicQueryRequestWithDeadline(
    alloc: std.mem.Allocator,
    semantic_resolver: ?SemanticResolver,
    table_name: []const u8,
    body: []const u8,
    execution_deadline_ns: ?u64,
) !OwnedQueryRequest {
    try ensureQueryDeadline(execution_deadline_ns);
    if (try queryBodyHasForbiddenPublicDocIdentityControlFields(alloc, body)) return error.InvalidQueryRequest;
    try ensureQueryDeadline(execution_deadline_ns);
    return try parseQueryRequestWithDeadline(
        alloc,
        semantic_resolver,
        table_name,
        body,
        execution_deadline_ns,
    );
}

pub const PublicFilterQueryErrorKind = enum {
    invalid,
    unsupported,
};

pub fn isPublicQueryValidationError(err: anyerror) bool {
    return switch (err) {
        error.InvalidQueryRequest,
        error.UnsupportedQueryRequest,
        error.InvalidFilterQueryRequest,
        error.InvalidExclusionQueryRequest,
        error.UnsupportedFilterQueryRequest,
        error.UnsupportedExclusionQueryRequest,
        => true,
        else => false,
    };
}

pub fn publicFilterQueryErrorStatus(kind: PublicFilterQueryErrorKind) u16 {
    return if (kind == .invalid) 400 else 422;
}

pub fn encodePublicFilterQueryErrorBodyAlloc(
    alloc: std.mem.Allocator,
    body: []const u8,
    field: []const u8,
    kind: PublicFilterQueryErrorKind,
) ![]u8 {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{}) catch
        return encodePublicFilterQueryErrorDetailsAlloc(alloc, field, kind, "unknown");
    defer parsed.deinit();
    const request = switch (parsed.value) {
        .object => |object| object,
        else => return encodePublicFilterQueryErrorDetailsAlloc(
            alloc,
            field,
            kind,
            "non_object",
        ),
    };
    const value = request.get(field) orelse
        return encodePublicFilterQueryErrorDetailsAlloc(alloc, field, kind, "unknown");
    const offending_node = try findPublicFilterQueryOffendingNodeAlloc(
        alloc,
        value,
        0,
    );
    return encodePublicFilterQueryErrorDetailsAlloc(
        alloc,
        field,
        kind,
        offending_node,
    );
}

fn encodePublicFilterQueryErrorDetailsAlloc(
    alloc: std.mem.Allocator,
    field: []const u8,
    kind: PublicFilterQueryErrorKind,
    offending_node: []const u8,
) ![]u8 {
    const status = publicFilterQueryErrorStatus(kind);
    return try std.json.Stringify.valueAlloc(alloc, .{
        .status = status,
        .@"error" = if (kind == .invalid)
            "invalid_query_request"
        else
            "unsupported_query_request",
        .message = if (kind == .invalid)
            "invalid query filter"
        else
            "unsupported query filter",
        .field = field,
        .offending_node = offending_node,
        .retryable = false,
    }, .{});
}

fn findPublicFilterQueryOffendingNodeAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    depth: u8,
) error{OutOfMemory}![]const u8 {
    if (depth >= 32) return publicFilterQueryNodeName(value);
    if (value == .array) {
        for (value.array.items) |item| {
            if (!try publicFilterQueryValueNormalizesAlloc(alloc, item)) {
                return findPublicFilterQueryOffendingNodeAlloc(
                    alloc,
                    item,
                    depth + 1,
                );
            }
        }
        return "array";
    }
    if (value != .object) return "non_object";

    inline for ([_][]const u8{ "conjuncts", "disjuncts" }) |compound| {
        if (value.object.get(compound)) |children| {
            if (children != .array or children.array.items.len == 0) return compound;
            for (children.array.items) |child| {
                if (!try publicFilterQueryValueNormalizesAlloc(alloc, child)) {
                    return findPublicFilterQueryOffendingNodeAlloc(
                        alloc,
                        child,
                        depth + 1,
                    );
                }
            }
            return compound;
        }
    }

    if (value.object.get("bool")) |bool_value| {
        if (bool_value != .object) return "bool";
        if (try findInvalidPublicBooleanBranchAlloc(
            alloc,
            bool_value.object,
            depth + 1,
        )) |node| return node;
        return "bool";
    }
    if (try findInvalidPublicBooleanBranchAlloc(
        alloc,
        value.object,
        depth + 1,
    )) |node| return node;
    return publicFilterQueryNodeName(value);
}

fn findInvalidPublicBooleanBranchAlloc(
    alloc: std.mem.Allocator,
    object: std.json.ObjectMap,
    depth: u8,
) error{OutOfMemory}!?[]const u8 {
    for ([_][]const u8{ "filter", "must", "should", "must_not" }) |branch| {
        const branch_value = object.get(branch) orelse continue;
        if (branch_value == .array) {
            if (branch_value.array.items.len == 0) continue;
            for (branch_value.array.items) |child| {
                if (!try publicFilterQueryValueNormalizesAlloc(alloc, child)) {
                    return try findPublicFilterQueryOffendingNodeAlloc(
                        alloc,
                        child,
                        depth + 1,
                    );
                }
            }
        } else if (!try publicFilterQueryValueNormalizesAlloc(alloc, branch_value)) {
            return try findPublicFilterQueryOffendingNodeAlloc(
                alloc,
                branch_value,
                depth + 1,
            );
        }
    }
    return null;
}

fn publicFilterQueryValueNormalizesAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
) error{OutOfMemory}!bool {
    validatePublicFilterOrTextQueryAlloc(alloc, value, 10) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return false,
    };
    return true;
}

fn publicFilterQueryNodeName(value: std.json.Value) []const u8 {
    if (value != .object) return "non_object";
    inline for ([_][]const u8{
        "match_all",
        "match_none",
        "term",
        "terms",
        "exists",
        "match",
        "prefix",
        "wildcard",
        "regexp",
        "fuzzy",
        "range",
        "numeric_range",
        "term_range",
        "date_range",
        "bool_field",
        "ip_range",
        "geo_distance",
        "geo_bbox",
        "geo_shape",
        "ids",
        "doc_id",
        "conjuncts",
        "disjuncts",
        "bool",
    }) |candidate| {
        if (value.object.get(candidate) != null) return candidate;
    }
    if (value.object.get("must") != null or
        value.object.get("should") != null or
        value.object.get("must_not") != null or
        value.object.get("filter") != null)
    {
        return "boolean";
    }
    if (value.object.count() == 1) {
        var iterator = value.object.iterator();
        if (iterator.next()) |entry| return entry.key_ptr.*;
    }
    return "unknown";
}

fn classifyPublicFilterContractErrorAlloc(
    alloc: std.mem.Allocator,
    body: []const u8,
) anyerror {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{}) catch
        return error.InvalidQueryRequest;
    defer parsed.deinit();
    const request = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidQueryRequest,
    };
    const fields = [_]struct {
        name: []const u8,
        invalid: anyerror,
        unsupported: anyerror,
    }{
        .{
            .name = "filter_query",
            .invalid = error.InvalidFilterQueryRequest,
            .unsupported = error.UnsupportedFilterQueryRequest,
        },
        .{
            .name = "exclusion_query",
            .invalid = error.InvalidExclusionQueryRequest,
            .unsupported = error.UnsupportedExclusionQueryRequest,
        },
    };
    for (fields) |field| {
        const value = request.get(field.name) orelse continue;
        validatePublicFilterOrTextQueryAlloc(alloc, value, 10) catch |err| return switch (err) {
            error.OutOfMemory => error.OutOfMemory,
            error.UnsupportedQueryRequest => field.unsupported,
            else => if (isStructuredFilterValue(value))
                field.invalid
            else
                field.unsupported,
        };
    }
    return error.InvalidQueryRequest;
}

pub fn preflightGraphSearchesAlloc(
    alloc: std.mem.Allocator,
    request: anytype,
) !void {
    const graph_queries = try buildGraphQueries(alloc, request);
    defer freeNamedGraphQueries(alloc, graph_queries);
}

pub fn queryRequestHasScoreBearingTextSourceAlloc(
    alloc: std.mem.Allocator,
    request: anytype,
) !bool {
    var preflight_req = try buildPreflightSearchRequestAlloc(alloc, request);
    defer preflight_req.deinit(alloc);
    return db_mod.searchRequestHasScoreBearingTextSource(preflight_req.req);
}

pub fn queryRequestHasScoreBearingSourceAlloc(
    alloc: std.mem.Allocator,
    request: anytype,
) !bool {
    var preflight_req = try buildPreflightSearchRequestAlloc(alloc, request);
    defer preflight_req.deinit(alloc);
    return db_mod.searchRequestHasScoreBearingSource(preflight_req.req);
}

fn searchRequestHasScoreSort(req: db_mod.types.SearchRequest) bool {
    for (req.order_by) |field| {
        if (std.mem.eql(u8, field.field, "_score")) return true;
    }
    return false;
}

fn approximateSemanticSortField(order_by: []const db_mod.types.SortField) []const u8 {
    for (order_by) |field| {
        if (std.mem.eql(u8, field.field, "_score")) continue;
        if (std.mem.eql(u8, field.field, "_id")) continue;
        return field.field;
    }
    return if (order_by.len > 0) order_by[0].field else "*";
}

fn validateScoreSortHasScoreBearingSource(req: db_mod.types.SearchRequest) !void {
    if (searchRequestHasScoreSort(req) and !db_mod.searchRequestHasScoreBearingSource(req)) {
        return unsupportedExactSort("_score", "non_score_bearing_source", "non_score_bearing_source");
    }
}

fn buildPreflightSearchRequestAlloc(
    alloc: std.mem.Allocator,
    request: anytype,
) !OwnedQueryRequest {
    var req: db_mod.types.SearchRequest = .{};
    errdefer freeSearchRequest(alloc, &req);

    try applyCommonSearchRequestOptions(alloc, request, &req);

    if (request.search_after != null and request.search_before != null) return unsupportedExactSort("*", "invalid_cursor_arity", "invalid_cursor_arity");

    const fields = try applySearchRequestFields(alloc, request.fields, &req);
    errdefer freeClonedFields(alloc, fields);

    var normalized_query = try normalizePublicQueryBucketsAlloc(alloc, request, req.limit);
    errdefer normalized_query.deinit(alloc);

    if (normalized_query.full_text) |query| {
        if (request.full_text_index) |index_name| {
            req.full_text_queries = try singleNamedFullTextQueryAlloc(alloc, index_name, query);
        } else {
            req.full_text = query;
        }
        normalized_query.full_text = null;
    } else if (request.full_text_index != null) {
        return error.InvalidQueryRequest;
    } else if (normalized_query.filter_text != null or
        normalized_query.exclusion_text != null or
        normalized_query.filter_query_json.len > 0 or
        normalized_query.exclusion_query_json.len > 0 or
        request.order_by != null or
        request.search_after != null or
        request.search_before != null)
    {
        req.full_text = .{ .match_all = {} };
    }

    if (req.reranker != null) {
        req.reranker_query_text = try buildRerankerQueryText(alloc, request);
    }

    req.filter_text = normalized_query.filter_text;
    normalized_query.filter_text = null;
    req.exclusion_text = normalized_query.exclusion_text;
    normalized_query.exclusion_text = null;
    req.filter_query_json = normalized_query.filter_query_json;
    normalized_query.filter_query_json = "";
    req.exclusion_query_json = normalized_query.exclusion_query_json;
    normalized_query.exclusion_query_json = "";

    const vector_queries = try buildPreflightSemanticVectorQueries(alloc, request, req.limit);
    errdefer vector_queries.deinit(alloc);
    req.dense_queries = vector_queries.dense;
    req.sparse_queries = vector_queries.sparse;
    req.graph_queries = try buildGraphQueries(alloc, request);
    if (comptime @hasField(@TypeOf(request), "expand_strategy")) {
        if (request.expand_strategy) |expand_strategy| {
            req.expand_strategy = try parseExpandStrategy(expand_strategy);
        }
    }
    try validateScoreSortHasScoreBearingSource(req);

    return .{
        .fields = fields,
        .req = req,
    };
}

fn preflightRequestHasFullTextResults(req: db_mod.types.SearchRequest) bool {
    if (req.full_text != null) return true;
    if (req.full_text_queries.len > 0) return true;
    if (req.filter_text != null or req.exclusion_text != null) return true;
    return req.filter_query_json.len > 0 or req.exclusion_query_json.len > 0;
}

fn preflightBaseResultSetCount(req: db_mod.types.SearchRequest) u32 {
    var count: u32 = 0;
    if (req.full_text_queries.len > 0) {
        count += @as(u32, @intCast(req.full_text_queries.len));
    } else if (preflightRequestHasFullTextResults(req)) {
        count += 1;
    }
    count += @as(u32, @intCast(req.dense_queries.len));
    count += @as(u32, @intCast(req.sparse_queries.len));
    if (req.dense_queries.len == 0 and req.sparse_queries.len == 0) {
        if (req.dense != null) count += 1;
        if (req.sparse != null) count += 1;
    }
    return count;
}

fn preflightRequiresFusion(req: db_mod.types.SearchRequest) bool {
    const base_result_sets = preflightBaseResultSetCount(req);
    return base_result_sets > 1;
}

fn countAggregationRequests(aggregations: ?std.json.ArrayHashMap(metadata_openapi.AggregationRequest)) u32 {
    const value = aggregations orelse return 0;
    var total: u32 = 0;
    for (value.map.values()) |aggregation| {
        total += 1;
        total += countAggregationRequests(aggregation.sub_aggregations);
    }
    return total;
}

pub fn preflightQueryRequestAlloc(
    alloc: std.mem.Allocator,
    request: anytype,
) !QueryPreflightSummary {
    var preflight_req = try buildPreflightSearchRequestAlloc(alloc, request);
    defer preflight_req.deinit(alloc);

    var runtime_preflight = try db_mod.preflightSearchRequestAlloc(alloc, preflight_req.req);
    defer runtime_preflight.deinit(alloc);

    var full_text_indexes = std.ArrayListUnmanaged([]const u8).empty;
    errdefer freeOwnedStringItems(alloc, full_text_indexes.items);
    errdefer full_text_indexes.deinit(alloc);
    var embedding_indexes = std.ArrayListUnmanaged([]const u8).empty;
    errdefer freeOwnedStringItems(alloc, embedding_indexes.items);
    errdefer embedding_indexes.deinit(alloc);
    var graph_indexes = std.ArrayListUnmanaged([]const u8).empty;
    errdefer freeOwnedStringItems(alloc, graph_indexes.items);
    errdefer graph_indexes.deinit(alloc);
    var result_refs = std.ArrayListUnmanaged([]const u8).empty;
    errdefer freeOwnedStringItems(alloc, result_refs.items);
    errdefer result_refs.deinit(alloc);
    var graph_query_order = std.ArrayListUnmanaged([]const u8).empty;
    errdefer freeOwnedStringItems(alloc, graph_query_order.items);
    errdefer graph_query_order.deinit(alloc);

    for (preflight_req.req.full_text_queries) |query| {
        try appendUniqueOwnedString(alloc, &full_text_indexes, query.index_name);
    }
    if (preflight_req.req.full_text != null or
        preflight_req.req.filter_text != null or
        preflight_req.req.exclusion_text != null)
    {
        try appendUniqueOwnedString(alloc, &full_text_indexes, "full_text");
    }
    for (preflight_req.req.dense_queries) |dense_query| {
        try appendUniqueOwnedString(alloc, &embedding_indexes, dense_query.index_name);
    }
    for (preflight_req.req.sparse_queries) |sparse_query| {
        try appendUniqueOwnedString(alloc, &embedding_indexes, sparse_query.index_name);
    }
    for (preflight_req.req.graph_queries) |graph_query| {
        try appendUniqueOwnedString(alloc, &graph_indexes, graph_query.query.index_name);
    }
    // The runtime keeps lane-specific result sets for execution planning, but
    // those are not part of the canonical public graph DSL. Present one stable
    // ranked-result reference plus prior graph outputs to callers and agents.
    try appendUniqueOwnedString(alloc, &result_refs, "$query_results");
    for (runtime_preflight.result_refs) |result_ref| {
        if (std.mem.startsWith(u8, result_ref, "$graph_results."))
            try appendUniqueOwnedString(alloc, &result_refs, result_ref);
    }
    for (runtime_preflight.graph_query_order) |name| try appendUniqueOwnedString(alloc, &graph_query_order, name);

    return .{
        .full_text_indexes = if (full_text_indexes.items.len == 0) &.{} else try full_text_indexes.toOwnedSlice(alloc),
        .embedding_indexes = if (embedding_indexes.items.len == 0) &.{} else try embedding_indexes.toOwnedSlice(alloc),
        .graph_indexes = if (graph_indexes.items.len == 0) &.{} else try graph_indexes.toOwnedSlice(alloc),
        .result_refs = if (result_refs.items.len == 0) &.{} else try result_refs.toOwnedSlice(alloc),
        .graph_query_order = if (graph_query_order.items.len == 0) &.{} else try graph_query_order.toOwnedSlice(alloc),
        .requested_limit = preflight_req.req.limit,
        .requested_offset = preflight_req.req.offset,
        .base_result_set_count = preflightBaseResultSetCount(preflight_req.req),
        .graph_query_count = @as(u32, @intCast(preflight_req.req.graph_queries.len)),
        .requires_fusion = preflightRequiresFusion(preflight_req.req),
        .count_only = preflight_req.req.count_only,
        .profile_requested = preflight_req.req.profile,
        .include_stored = preflight_req.req.include_stored,
        .reranker_enabled = preflight_req.req.reranker != null,
        .aggregation_count = countAggregationRequests(request.aggregations),
    };
}

const FastDenseEmbedding = union(enum) {
    @"packed": []const u8,
    dense: []const f32,

    pub fn jsonParse(
        allocator: std.mem.Allocator,
        source: anytype,
        options: std.json.ParseOptions,
    ) !@This() {
        switch (try source.nextAllocMax(allocator, .alloc_if_needed, options.max_value_len.?)) {
            .string => |value| return .{ .@"packed" = value },
            .allocated_string => |value| return .{ .@"packed" = value },
            .array_begin => return .{ .dense = try parseDenseArrayAlloc(allocator, source, options) },
            else => return error.UnexpectedToken,
        }
    }
};

const FastDensePublicQueryRequest = struct {
    embeddings: ?std.json.ArrayHashMap(FastDenseEmbedding) = null,
    indexes: ?[]const []const u8 = null,
    fields: ?[]const []const u8 = null,
    limit: ?u32 = null,
    offset: ?u32 = null,
    count: ?bool = null,
    profile: ?bool = null,
    search_effort: ?f32 = null,
    filter_prefix: ?[]const u8 = null,
    distance_over: ?f32 = null,
    distance_under: ?f32 = null,
};

fn fastDensePublicQueryMayApply(body: []const u8) bool {
    const disallowed = [_][]const u8{
        "\"query\"",
        "\"full_text_search\"",
        "\"full_text_index\"",
        "\"filter_query\"",
        "\"exclusion_query\"",
        "\"merge_config\"",
        "\"reranker\"",
        "\"pruner\"",
        "\"semantic_search\"",
        "\"sparse\"",
        "\"graph\"",
        "\"join\"",
        "\"with\"",
        "\"hierarchy\"",
        "\"_filter_query_json\"",
        "\"_exclusion_query_json\"",
        db_mod.doc_filter_wire.field_name,
    };
    for (disallowed) |needle| {
        if (std.mem.indexOf(u8, body, needle) != null) return false;
    }
    return true;
}

fn tryParseFastDensePublicQueryRequest(
    alloc: std.mem.Allocator,
    body: []const u8,
) !?OwnedQueryRequest {
    var parsed = ant_json.parseFromSlice(FastDensePublicQueryRequest, alloc, body, .{}) catch return null;
    defer parsed.deinit();

    const request = parsed.value;
    const embeddings = request.embeddings orelse return null;
    if (embeddings.map.count() == 0) return error.UnsupportedQueryRequest;
    if ((request.offset orelse 0) > 0) return error.UnsupportedQueryRequest;

    var req: db_mod.types.SearchRequest = .{};
    errdefer freeSearchRequest(alloc, &req);

    req.limit = request.limit orelse req.limit;
    req.count_only = request.count orelse false;
    req.profile = request.profile orelse false;
    req.search_effort = request.search_effort;
    if (request.filter_prefix) |filter_prefix| req.filter_prefix = try alloc.dupe(u8, filter_prefix);
    req.distance_over = request.distance_over;
    req.distance_under = request.distance_under;

    const fields = try applySearchRequestFields(alloc, request.fields, &req);
    errdefer freeClonedFields(alloc, fields);

    const index_names = request.indexes orelse blk: {
        const out = try alloc.alloc([]u8, embeddings.map.count());
        errdefer alloc.free(out);
        var initialized: usize = 0;
        errdefer {
            for (out[0..initialized]) |index_name| alloc.free(index_name);
        }
        var it = embeddings.map.iterator();
        while (it.next()) |entry| {
            out[initialized] = try alloc.dupe(u8, entry.key_ptr.*);
            initialized += 1;
        }
        break :blk out;
    };
    defer if (request.indexes == null) {
        for (index_names) |index_name| alloc.free(index_name);
        alloc.free(index_names);
    };
    if (index_names.len == 0) return error.UnsupportedQueryRequest;

    const dense_queries = try alloc.alloc(db_mod.types.NamedDenseQuery, index_names.len);
    var dense_queries_initialized: usize = 0;
    errdefer freeNamedDenseQueries(alloc, dense_queries[0..dense_queries_initialized]);

    for (index_names, 0..) |index_name, i| {
        const embedding = embeddings.map.get(index_name) orelse return error.UnsupportedQueryRequest;
        dense_queries[i] = .{
            .name = try alloc.dupe(u8, index_name),
            .index_name = try alloc.dupe(u8, index_name),
            .query = .{
                .vector = switch (embedding) {
                    .@"packed" => |encoded| vector_codec.decodePackedF32Base64Alloc(alloc, encoded) catch return error.InvalidQueryRequest,
                    .dense => |dense| try alloc.dupe(f32, dense),
                },
                .k = req.limit,
            },
        };
        dense_queries_initialized += 1;
    }
    req.dense_queries = dense_queries;

    return .{
        .fields = fields,
        .req = req,
    };
}

fn parseDenseArrayAlloc(
    allocator: std.mem.Allocator,
    source: anytype,
    options: std.json.ParseOptions,
) ![]const f32 {
    var values = std.ArrayListUnmanaged(f32).empty;
    errdefer values.deinit(allocator);

    while (true) {
        const token = try source.nextAllocMax(allocator, .alloc_if_needed, options.max_value_len.?);
        switch (token) {
            .array_end => return try values.toOwnedSlice(allocator),
            .number => |value| try values.append(allocator, try parseJsonNumberF32(value)),
            .allocated_number => |value| {
                defer allocator.free(value);
                try values.append(allocator, try parseJsonNumberF32(value));
            },
            else => return error.UnexpectedToken,
        }
    }
}

fn parseJsonNumberF32(value: []const u8) !f32 {
    if (std.mem.indexOfAny(u8, value, ".eE") != null) {
        return try std.fmt.parseFloat(f32, value);
    }
    return @floatFromInt(try std.fmt.parseInt(i64, value, 10));
}

pub fn encodeQueryResponses(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    meta: QueryResponseMeta,
    result: db_mod.types.SearchResult,
) !QueryResponse {
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    const emitted_hits = if (req.count_only) &.{} else result.hits;
    const hits = try arena.alloc(metadata_openapi.QueryHit, emitted_hits.len);
    for (emitted_hits, 0..) |hit, i| {
        hits[i] = try toOpenApiHit(arena, req, hit);
    }

    const graph_dialect = if (req.graph_queries.len > 0)
        try graphResponseFormat(req)
    else
        null;
    const aggregations = if (meta.aggregation_results.len > 0)
        try buildAggregationResults(arena, req, meta.aggregation_results)
    else
        null;

    const profile = if (req.profile) try buildProfileValue(arena, req, meta, result) else null;
    const response_json = switch (graph_dialect orelse .canonical) {
        .canonical => blk: {
            const graph_results = if (graph_dialect != null)
                try buildGraphQueryResults(indexes_openapi.GraphResult, arena, req, meta, result, .canonical)
            else
                null;
            const query_results = try arena.alloc(metadata_openapi.QueryResult, 1);
            query_results[0] = .{
                .hits = .{
                    .total = queryHitsTotalFromSearchResult(result),
                    .hits = hits,
                    .max_score = computeMaxScore(emitted_hits),
                },
                .aggregations = aggregations,
                .graph_results = graph_results,
                .profile = profile,
                .took = meta.took_ms,
                .status = 200,
                .table = table_name,
            };
            break :blk try std.json.Stringify.valueAlloc(
                alloc,
                metadata_openapi.QueryResponses{ .responses = query_results },
                .{ .emit_null_optional_fields = false },
            );
        },
        .legacy => blk: {
            const graph_results = try buildGraphQueryResults(
                indexes_openapi.StatefulGraphResult,
                arena,
                req,
                meta,
                result,
                .legacy,
            );
            const query_results = try arena.alloc(metadata_openapi.StatefulQueryResult, 1);
            query_results[0] = .{
                .hits = .{
                    .total = queryHitsTotalFromSearchResult(result),
                    .hits = hits,
                    .max_score = computeMaxScore(emitted_hits),
                },
                .aggregations = aggregations,
                .graph_results = graph_results,
                .profile = profile,
                .took = meta.took_ms,
                .status = 200,
                .table = table_name,
            };
            break :blk try std.json.Stringify.valueAlloc(
                alloc,
                metadata_openapi.StatefulQueryResponses{ .responses = query_results },
                .{ .emit_null_optional_fields = false },
            );
        },
    };

    return .{
        .identity_read_generation = result.identity_read_generation orelse req.identity_read_generation,
        .graph_dialect = graph_dialect,
        // OpenAPI optional response fields are absent unless populated; they
        // are not nullable. Keeping nulls out also preserves the compact wire
        // shape now that hierarchy is represented by generated response types.
        .json = response_json,
    };
}

fn toOpenApiHit(alloc: std.mem.Allocator, req: db_mod.types.SearchRequest, hit: db_mod.types.SearchHit) !metadata_openapi.QueryHit {
    try validateOpenApiHitSortTuple(req, hit);

    return .{
        ._id = hit.id,
        ._score = if (hit.score) |score| finiteScoreOrZero(score) else 0,
        ._distance = if (hit.distance) |distance| finiteScoreOrZero(distance) else null,
        ._index_scores = try indexScoresJsonValue(alloc, hit.index_scores),
        ._sort = if (hit.sort_values.len > 0) hit.sort_values else null,
        ._source = if (hit.stored_data) |stored_data|
            try takeOpenApiObjectMap(alloc, if (req.defer_hierarchy_child_hydration and req.hierarchy_group_level == .unit)
                // Deferred unit grouping uses `_source` as a private shard-to-
                // coordinator revision envelope. Projecting it by public unit
                // fields would turn it into `{}` and defeat revision binding.
                try parseInternalGroupedUnitRevisionEnvelopeValue(alloc, stored_data)
            else if (req.defer_stored_projection)
                try projectPublicStoredSourceValue(alloc, stored_data, .{
                    .fields = req.fields,
                    .include_all_fields = req.include_all_fields,
                })
            else
                try parseStoredSourceValue(alloc, stored_data))
        else
            null,
        .hierarchy = try searchHitHierarchyOpenApiValue(alloc, req, hit),
    };
}

/// Move an owned dynamic JSON object into the generated OpenAPI object-map
/// representation without cloning its keys or values. Stored documents are
/// contractually objects; fail closed if corrupted internal state violates
/// that invariant instead of emitting a response outside the public schema.
fn takeOpenApiObjectMap(
    alloc: std.mem.Allocator,
    value: std.json.Value,
) !std.json.ArrayHashMap(std.json.Value) {
    if (value == .object) return .{ .map = value.object };
    var invalid = value;
    db_mod.types.deinitJsonValue(alloc, &invalid);
    return error.InvalidRemoteResponse;
}

fn searchHitHierarchyOpenApiValue(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
    hit: db_mod.types.SearchHit,
) !?metadata_openapi.QueryHitHierarchy {
    const value = (try searchHitHierarchyJsonValue(alloc, req, hit)) orelse return null;
    return try parseSearchHitHierarchyOpenApiValue(alloc, value);
}

fn parseSearchHitHierarchyOpenApiValue(
    alloc: std.mem.Allocator,
    value: std.json.Value,
) !metadata_openapi.QueryHitHierarchy {
    // This conversion is also the boundary between the hand-built runtime
    // envelope and its public schema. Keep it strict so a newly emitted field
    // cannot silently disappear from the API and generated SDKs.
    return try std.json.parseFromValueLeaky(metadata_openapi.QueryHitHierarchy, alloc, value, .{});
}

fn validateOpenApiHitSortTuple(req: db_mod.types.SearchRequest, hit: db_mod.types.SearchHit) !void {
    if (!sortProfileRequestHasOrderedPage(req)) return;
    const expected_len = sortProfileEffectiveOrderLen(req);
    if (hit.sort_values.len != expected_len) {
        return invalidOutboundSortTuple("*", "sort_tuple_arity");
    }
    for (0..expected_len) |i| {
        const field = sortProfileEffectiveOrderField(req, i);
        const value = hit.sort_values[i];
        if (!openApiSortValueIsCursorReplayable(value)) return invalidOutboundSortTuple(field.field, "invalid_cursor_type");
        if (std.mem.eql(u8, field.field, "_score") and !openApiSortValueIsNumeric(value)) {
            return invalidOutboundSortTuple(field.field, "non_numeric_score");
        }
        if (std.mem.eql(u8, field.field, "_id")) {
            if (value != .string or !std.mem.eql(u8, value.string, hit.id)) {
                return invalidOutboundSortTuple(field.field, "id_tiebreaker_mismatch");
            }
        }
    }
}

fn invalidOutboundSortTuple(field: []const u8, detail: []const u8) error{InvalidQueryRequest} {
    recordUnsupportedExactSortDiagnostic(field, "invalid_sort_tuple", detail);
    return error.InvalidQueryRequest;
}

fn openApiSortValueIsCursorReplayable(value: std.json.Value) bool {
    return switch (value) {
        .float => |v| std.math.isFinite(v),
        .number_string => |text| openApiSortNumberStringIsCursorReplayable(text),
        .bool, .integer, .string => true,
        .null => false,
        .array, .object => false,
    };
}

fn openApiSortNumberStringIsCursorReplayable(text: []const u8) bool {
    if (std.fmt.parseInt(i64, text, 10)) |_| return true else |_| {}
    if (std.fmt.parseInt(u64, text, 10)) |_| return true else |_| {}
    const parsed = std.fmt.parseFloat(f64, text) catch return false;
    return std.math.isFinite(parsed);
}

fn openApiSortValueIsNumeric(value: std.json.Value) bool {
    return switch (value) {
        .integer => true,
        .float => |v| std.math.isFinite(v),
        .number_string => |text| openApiSortNumberStringIsCursorReplayable(text),
        else => false,
    };
}

fn finiteScoreOrZero(score: f32) f32 {
    return if (std.math.isFinite(score)) score else 0;
}

fn searchHitHierarchyJsonValue(alloc: std.mem.Allocator, req: db_mod.types.SearchRequest, hit: db_mod.types.SearchHit) !?std.json.Value {
    if (hit.artifact_ref == null and hit.chunk_hits.len == 0) return null;
    const grouped_parent_mode = req.return_mode == .parent or req.return_mode == .parent_with_chunks;
    const grouped_parent = grouped_parent_mode and
        (hit.chunk_hits.len > 0 or if (hit.artifact_ref) |artifact_ref|
            std.mem.eql(u8, hit.id, artifact_ref.document_id)
        else
            false);

    var mention_payload = if (hit.stored_data) |raw| try parseMentionEvidencePayload(alloc, raw) else null;
    defer if (mention_payload) |*parsed| parsed.deinit();

    var obj = std.json.ObjectMap.empty;
    errdefer obj.deinit(alloc);

    const level = if (grouped_parent)
        "source"
    else if (hit.artifact_ref) |artifact_ref|
        if (mention_payload != null) "mention" else artifactRefLevel(artifact_ref)
    else
        "source";
    try putJsonString(alloc, &obj, "level", level);

    if (hit.artifact_ref) |artifact_ref| {
        const parent_doc_key = if (grouped_parent)
            hit.id
        else if (mention_payload) |payload|
            jsonObjectString(payload.value.object, "_parent_doc_key") orelse artifact_ref.document_id
        else
            artifact_ref.document_id;
        try putJsonString(alloc, &obj, "parent_doc_key", parent_doc_key);
        try obj.put(
            alloc,
            try alloc.dupe(u8, if (grouped_parent) "matched_artifact" else "artifact"),
            try artifactRefJsonValue(alloc, artifact_ref),
        );
        if (!grouped_parent) {
            if (artifact_ref.unit_id) |unit_id| {
                try putJsonString(alloc, &obj, "parent_unit_id", unit_id);
            }
        }
        if (mention_payload) |payload| {
            try obj.put(alloc, try alloc.dupe(u8, "evidence"), try mentionEvidenceHierarchyJsonValue(alloc, payload.value.object));
        }
    } else {
        try putJsonString(alloc, &obj, "parent_doc_key", hit.id);
    }

    if (try hierarchyAncestorsJsonValue(alloc, req, if (grouped_parent) null else hit.artifact_ref, hit.id, hit.stored_data, hit.ancestor_source_data, hit.ancestor_unit_data)) |ancestors| {
        try obj.put(alloc, try alloc.dupe(u8, "ancestors"), ancestors);
    }

    // Positions and revisions are traversal state, not durable unit fields.
    // Relevance grouping deliberately omits them so re-extraction cannot leak
    // a cursor from an older artifact generation.
    if (req.hierarchy_children != null) try appendHierarchyNavigationMetadata(alloc, &obj, hit);

    if (hit.chunk_hits.len > 0) {
        var chunks = try std.json.Array.initCapacity(alloc, hit.chunk_hits.len);
        errdefer chunks.deinit();
        for (hit.chunk_hits) |chunk_hit| {
            try chunks.append(try chunkHitJsonValue(alloc, req, chunk_hit));
        }
        try obj.put(
            alloc,
            try alloc.dupe(u8, if (req.hierarchy_grouped_matches) "matches" else "chunks"),
            .{ .array = chunks },
        );
    }

    return .{ .object = obj };
}

fn appendHierarchyNavigationMetadata(
    alloc: std.mem.Allocator,
    hierarchy: *std.json.ObjectMap,
    hit: db_mod.types.SearchHit,
) !void {
    if (hit.sort_values.len != 2 or hit.sort_values[0] != .string or hit.sort_values[1] != .string) {
        return error.InvalidQueryRequest;
    }
    const artifact_ref = hit.artifact_ref orelse return error.InvalidQueryRequest;
    const position = hit.sort_values[0].string;
    const parsed = hierarchy_navigation.parsePositionForArtifact(artifact_ref.name, position) catch
        return error.InvalidQueryRequest;
    try putJsonString(alloc, hierarchy, "position", position);
    const revision = try std.fmt.allocPrint(alloc, "source@{s}", .{parsed.source_revision});
    defer alloc.free(revision);
    try putJsonString(alloc, hierarchy, "revision", revision);
}

fn parseMentionEvidencePayload(alloc: std.mem.Allocator, raw: []const u8) !?std.json.Parsed(std.json.Value) {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, raw, .{}) catch return null;
    errdefer parsed.deinit();
    if (parsed.value != .object) {
        parsed.deinit();
        return null;
    }
    const schema = jsonObjectString(parsed.value.object, "_schema") orelse {
        parsed.deinit();
        return null;
    };
    if (!std.mem.eql(u8, schema, "antfly.resolution_mention.v1")) {
        parsed.deinit();
        return null;
    }
    return parsed;
}

fn mentionEvidenceHierarchyJsonValue(alloc: std.mem.Allocator, payload: std.json.ObjectMap) !std.json.Value {
    var evidence = std.json.ObjectMap.empty;
    errdefer evidence.deinit(alloc);

    try copyOptionalJsonField(alloc, &evidence, payload, "local_id", "local_id");
    try copyOptionalJsonField(alloc, &evidence, payload, "decision", "decision");
    try copyOptionalJsonField(alloc, &evidence, payload, "confidence", "confidence");
    try copyOptionalJsonField(alloc, &evidence, payload, "source_artifact", "source_artifact");
    try copyOptionalJsonField(alloc, &evidence, payload, "source_artifact_key", "source_artifact_key");
    try copyOptionalJsonField(alloc, &evidence, payload, "resolution_artifact", "resolution_artifact");
    try copyOptionalJsonField(alloc, &evidence, payload, "resolution_artifact_key", "resolution_artifact_key");
    try copyOptionalJsonField(alloc, &evidence, payload, "resolver", "resolver");
    try copyOptionalJsonField(alloc, &evidence, payload, "resolver_table", "resolver_table");
    try copyOptionalJsonField(alloc, &evidence, payload, "mention", "mention");
    try copyOptionalJsonField(alloc, &evidence, payload, "canonical", "canonical");

    return .{ .object = evidence };
}

fn chunkHitJsonValue(alloc: std.mem.Allocator, req: db_mod.types.SearchRequest, hit: db_mod.types.ChunkHit) !std.json.Value {
    var obj = std.json.ObjectMap.empty;
    errdefer obj.deinit(alloc);

    try putJsonString(alloc, &obj, "_id", hit.id);
    try obj.put(alloc, try alloc.dupe(u8, "_score"), .{ .float = hit.score orelse 0 });
    if (hit.distance) |distance| {
        try obj.put(alloc, try alloc.dupe(u8, "_distance"), .{ .float = finiteScoreOrZero(distance) });
    }
    if (hit.stored_data) |stored_data| {
        const match_projection_configured = !req.hierarchy_match_include_all_fields;
        const source = if (req.defer_stored_projection or match_projection_configured)
            try projectPublicStoredSourceValue(alloc, stored_data, .{
                .fields = if (match_projection_configured) req.hierarchy_match_fields else req.fields,
                .include_all_fields = if (match_projection_configured) false else req.include_all_fields,
            })
        else
            try parseStoredSourceValue(alloc, stored_data);
        try obj.put(alloc, try alloc.dupe(u8, "_source"), source);
    }
    if (hit.artifact_ref) |artifact_ref| {
        var hierarchy = std.json.ObjectMap.empty;
        errdefer hierarchy.deinit(alloc);
        try putJsonString(alloc, &hierarchy, "level", artifactRefLevel(artifact_ref));
        try putJsonString(alloc, &hierarchy, "parent_doc_key", artifact_ref.document_id);
        if (artifact_ref.unit_id) |unit_id| {
            try putJsonString(alloc, &hierarchy, "parent_unit_id", unit_id);
        }
        try hierarchy.put(alloc, try alloc.dupe(u8, "artifact"), try artifactRefJsonValue(alloc, artifact_ref));
        if (try hierarchyAncestorsJsonValue(alloc, req, hit.artifact_ref, hit.id, hit.stored_data, hit.ancestor_source_data, hit.ancestor_unit_data)) |ancestors| {
            try hierarchy.put(alloc, try alloc.dupe(u8, "ancestors"), ancestors);
        }
        try obj.put(alloc, try alloc.dupe(u8, "hierarchy"), .{ .object = hierarchy });
    }

    return .{ .object = obj };
}

fn hierarchyAncestorsJsonValue(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
    artifact_ref_opt: ?db_mod.types.ArtifactRef,
    hit_id: []const u8,
    stored_data: ?[]const u8,
    ancestor_source_data: ?[]const u8,
    ancestor_unit_data: ?[]const u8,
) !?std.json.Value {
    var ancestors = std.json.ObjectMap.empty;
    errdefer ancestors.deinit(alloc);

    var source = std.json.ObjectMap.empty;
    errdefer source.deinit(alloc);
    const source_id = if (artifact_ref_opt) |artifact_ref| artifact_ref.document_id else hit_id;
    try putJsonString(alloc, &source, "id", source_id);
    if (ancestor_source_data) |raw| {
        try source.put(alloc, try alloc.dupe(u8, "document"), try hierarchyStoredSourceJsonValue(
            alloc,
            raw,
            req.hierarchy_source_fields,
            req.hierarchy_source_include_all_fields,
        ));
    } else if (artifact_ref_opt == null and
        (!req.hierarchy_omit_implicit_source_ancestor_document or req.hierarchy_include_source))
    {
        if (stored_data) |raw| {
            try source.put(alloc, try alloc.dupe(u8, "document"), try hierarchyStoredSourceJsonValue(
                alloc,
                raw,
                req.hierarchy_source_fields,
                req.hierarchy_source_include_all_fields,
            ));
        }
    }
    try ancestors.put(alloc, try alloc.dupe(u8, "source"), .{ .object = source });

    const artifact_ref = artifact_ref_opt orelse {
        return .{ .object = ancestors };
    };

    if (try hierarchyUnitAncestorJsonValue(alloc, req, artifact_ref, stored_data, ancestor_unit_data)) |unit| {
        try ancestors.put(alloc, try alloc.dupe(u8, "unit"), unit);
    }

    return .{ .object = ancestors };
}

fn hierarchyUnitAncestorJsonValue(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
    artifact_ref: db_mod.types.ArtifactRef,
    stored_data: ?[]const u8,
    ancestor_unit_data: ?[]const u8,
) !?std.json.Value {
    const unit_id = artifact_ref.unit_id orelse if (artifact_ref.source) |source| source.unit_id else null;
    if (unit_id == null and stored_data == null) return null;

    var unit = std.json.ObjectMap.empty;
    errdefer unit.deinit(alloc);
    if (unit_id) |id| try putJsonString(alloc, &unit, "id", id);

    if (ancestor_unit_data) |raw| {
        try unit.put(alloc, try alloc.dupe(u8, "document"), try hierarchyStoredSourceJsonValue(
            alloc,
            raw,
            req.hierarchy_unit_fields,
            req.hierarchy_unit_include_all_fields,
        ));
        if (artifact_ref.kind == .asset and artifact_ref.unit_id != null) {
            return .{ .object = unit };
        }
    }

    if (stored_data) |raw| {
        var stored = try parseStoredSourceValue(alloc, raw);
        if (artifact_ref.kind == .asset and artifact_ref.unit_id != null) {
            // Unit groups and child-navigation hits already expose their own
            // projected payload through `_source`. Do not silently duplicate
            // the complete unit document in its ancestor envelope: aside from
            // response amplification, that would bypass the explicit top-level
            // field projection. An explicitly requested unit ancestor remains
            // available with its independent projection.
            if ((req.return_mode == .unit or req.return_mode == .unit_with_chunks) and
                !req.hierarchy_include_unit)
            {
                db_mod.types.deinitJsonValue(alloc, &stored);
                return .{ .object = unit };
            }
            const document = if (req.hierarchy_unit_include_all_fields) blk: {
                break :blk stored;
            } else blk: {
                defer db_mod.types.deinitJsonValue(alloc, &stored);
                break :blk try projectPublicStoredSourceValue(alloc, raw, .{
                    .fields = req.hierarchy_unit_fields,
                    .include_all_fields = false,
                });
            };
            try unit.put(alloc, try alloc.dupe(u8, "document"), document);
            return .{ .object = unit };
        }

        defer db_mod.types.deinitJsonValue(alloc, &stored);
        if (stored == .object) {
            const stored_obj = stored.object;
            try copyOptionalJsonField(alloc, &unit, stored_obj, "unit_id", "id");
            try copyOptionalJsonField(alloc, &unit, stored_obj, "_parent_unit_key", "key");
            try copyOptionalJsonField(alloc, &unit, stored_obj, "_source_artifact_name", "artifact_name");
            try copyOptionalJsonField(alloc, &unit, stored_obj, "_source_field", "source_field");
            try copyOptionalJsonField(alloc, &unit, stored_obj, "provenance", "provenance");
        }
    }

    return if (unit.count() > 0) .{ .object = unit } else null;
}

fn hierarchyStoredSourceJsonValue(
    alloc: std.mem.Allocator,
    raw: []const u8,
    fields: []const []const u8,
    include_all_fields: bool,
) !std.json.Value {
    return if (include_all_fields)
        try parseStoredSourceValue(alloc, raw)
    else
        try projectPublicStoredSourceValue(alloc, raw, .{
            .fields = fields,
            .include_all_fields = false,
        });
}

fn copyOptionalJsonField(
    alloc: std.mem.Allocator,
    out: *std.json.ObjectMap,
    source: std.json.ObjectMap,
    source_key: []const u8,
    dest_key: []const u8,
) !void {
    const value = source.get(source_key) orelse return;
    if (std.mem.eql(u8, dest_key, "id") and out.get("id") != null) return;
    try out.put(alloc, try alloc.dupe(u8, dest_key), try cloneJsonValueAlloc(alloc, value));
}

fn jsonObjectString(object: std.json.ObjectMap, key: []const u8) ?[]const u8 {
    const value = object.get(key) orelse return null;
    return if (value == .string) value.string else null;
}

fn cloneJsonValueAlloc(alloc: std.mem.Allocator, value: std.json.Value) !std.json.Value {
    const encoded = try std.json.Stringify.valueAlloc(alloc, value, .{});
    defer alloc.free(encoded);
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, encoded, .{});
    return parsed.value;
}

fn artifactRefJsonValue(alloc: std.mem.Allocator, artifact_ref: db_mod.types.ArtifactRef) !std.json.Value {
    var obj = std.json.ObjectMap.empty;
    errdefer obj.deinit(alloc);

    try putJsonString(alloc, &obj, "name", artifact_ref.name);
    try putJsonString(alloc, &obj, "kind", artifactKindString(artifact_ref.kind));
    if (artifact_ref.chunk_id) |chunk_id| {
        try obj.put(alloc, try alloc.dupe(u8, "chunk_id"), .{ .integer = @intCast(chunk_id) });
    }
    if (artifact_ref.unit_id) |unit_id| {
        try putJsonString(alloc, &obj, "unit_id", unit_id);
    }
    if (artifact_ref.source) |source| {
        var source_obj = std.json.ObjectMap.empty;
        errdefer source_obj.deinit(alloc);
        try putJsonString(alloc, &source_obj, "name", source.name);
        try putJsonString(alloc, &source_obj, "kind", artifactKindString(source.kind));
        if (source.chunk_id) |chunk_id| {
            try source_obj.put(alloc, try alloc.dupe(u8, "chunk_id"), .{ .integer = @intCast(chunk_id) });
        }
        if (source.unit_id) |unit_id| {
            try putJsonString(alloc, &source_obj, "unit_id", unit_id);
        }
        try obj.put(alloc, try alloc.dupe(u8, "source"), .{ .object = source_obj });
    }

    return .{ .object = obj };
}

fn artifactRefLevel(artifact_ref: db_mod.types.ArtifactRef) []const u8 {
    return switch (artifact_ref.kind) {
        .chunk => "chunk",
        .asset => if (artifact_ref.unit_id != null) "unit" else "artifact",
        .embedding => "embedding",
    };
}

fn artifactKindString(kind: db_mod.types.ArtifactKind) []const u8 {
    return switch (kind) {
        .chunk => "chunk",
        .asset => "asset",
        .embedding => "embedding",
    };
}

fn putJsonString(alloc: std.mem.Allocator, obj: *std.json.ObjectMap, key: []const u8, value: []const u8) !void {
    try obj.put(alloc, try alloc.dupe(u8, key), .{ .string = try alloc.dupe(u8, value) });
}

fn indexScoresJsonValue(
    alloc: std.mem.Allocator,
    scores: []const fusion_mod.IndexScore,
) !?std.json.ArrayHashMap(f64) {
    if (scores.len == 0) return null;
    var out = std.json.ArrayHashMap(f64){};
    errdefer out.deinit(alloc);
    for (scores) |score| {
        try out.map.put(alloc, score.index_name, score.score);
    }
    return out;
}

test "api query contract serializes fused index scores" {
    const alloc = std.testing.allocator;
    const scores = [_]fusion_mod.IndexScore{
        .{ .index_name = "text_idx", .score = 0.75 },
        .{ .index_name = "semantic_idx", .score = 0.25 },
    };

    var value = (try indexScoresJsonValue(alloc, &scores)).?;
    defer value.deinit(alloc);

    const object = value.map;
    try std.testing.expectEqual(@as(usize, 2), object.count());
    try std.testing.expectEqual(@as(f64, 0.75), object.get("text_idx").?);
    try std.testing.expectEqual(@as(f64, 0.25), object.get("semantic_idx").?);
}

fn expectSortProfileDiagnosticsSerializationForTest() !void {
    const alloc = std.testing.allocator;

    var result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 2,
        .sort_profile = .{
            .plan = "sorted_segment_seek",
            .exactness = "exact",
            .source = "sorted_segment_scan",
            .candidate_source = "native_filter",
            .cursor_support = "segment_seek",
            .source_load = "projected_source_after_page",
            .distributed_behavior = "shard_local_only",
            .selection_reason = "index_sort_sorted_segment_seek",
            .require_native = true,
            .native_loader = true,
            .sort_lifecycle_state = "accelerated",
            .native_filter_mode = "doc_nums",
            .native_filter_candidate_count = 3,
            .native_filter_exclusion_count = 1,
            .selective_filter_doc_values_preferred = true,
            .cost_model_live_docs = 1000,
            .cost_model_candidate_count = 3,
            .cost_model_selective_limit = 4096,
            .native_doc_values_coverage = "covered",
            .index_sort_coverage = "covered_with_bounds",
            .index_sort_match = true,
            .sorted_segment_executor_available = true,
            .sorted_segment_bounds_available = true,
            .sorted_segment_scanned_count = 13,
            .sorted_segment_scan_budget = 100,
            .candidate_count = 7,
            .cursor_rejected_count = 1,
            .admitted_count = 5,
            .replaced_count = 2,
            .discarded_count = 3,
            .selected_count = 2,
            .decorate_us = 11,
            .native_doc_value_load_us = 13,
            .native_doc_value_hit_count = 17,
            .native_doc_value_miss_count = 19,
            .stored_json_load_us = 23,
            .stored_json_load_count = 29,
            .projected_source_load_us = 31,
            .projected_source_load_count = 37,
            .final_sort_us = 31,
            .total_us = 41,
            .window_capacity = 41,
            .window_len = 2,
            .collector_heap_peak = 5,
            .distributed_shard_count = 3,
            .distributed_shard_window = 11,
            .budget_rejection_reason = "match_all_candidate_collect_limit",
            .sort_rejection_reason = "missing_doc_values_coverage",
            .sort_rejection_detail = "missing_doc_values_section",
            .sort_rejection_field = db_mod.types.SortProfileField.init("created_at"),
        },
    };
    defer result.deinit();

    const order_by = [_]db_mod.types.SortField{.{ .field = "created_at", .desc = true }};
    const cursor = [_]std.json.Value{
        .{ .string = "2026-01-01T00:00:00.000000000Z" },
        .{ .string = "doc:a" },
    };
    var response = try encodeQueryResponses(alloc, "docs", .{
        .profile = true,
        .order_by = &order_by,
        .search_after = &cursor,
    }, .{}, result);
    defer response.deinit(alloc);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, response.json, .{});
    defer parsed.deinit();
    const profile = parsed.value.object.get("responses").?.array.items[0].object.get("profile").?.object;
    const sort = profile.get("sort").?.object;
    try std.testing.expectEqualStrings("sorted_segment_seek", sort.get("plan").?.string);
    const emitted_order = sort.get("order_by").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), emitted_order.len);
    try std.testing.expectEqualStrings("created_at", emitted_order[0].object.get("field").?.string);
    try std.testing.expect(emitted_order[0].object.get("desc").?.bool);
    try std.testing.expectEqualStrings("_id", emitted_order[1].object.get("field").?.string);
    try std.testing.expect(!emitted_order[1].object.get("desc").?.bool);
    try std.testing.expectEqualStrings("after", sort.get("cursor").?.string);
    try std.testing.expectEqualStrings("exact", sort.get("exactness").?.string);
    try std.testing.expectEqualStrings("sorted_segment_scan", sort.get("source").?.string);
    try std.testing.expectEqualStrings("native_filter", sort.get("candidate_source").?.string);
    try std.testing.expectEqualStrings("segment_seek", sort.get("cursor_support").?.string);
    try std.testing.expectEqualStrings("projected_source_after_page", sort.get("source_load").?.string);
    try std.testing.expectEqualStrings("shard_local_only", sort.get("distributed_behavior").?.string);
    try std.testing.expectEqualStrings("index_sort_sorted_segment_seek", sort.get("selection_reason").?.string);
    try std.testing.expect(sort.get("require_native").?.bool);
    try std.testing.expectEqualStrings("accelerated", sort.get("sort_lifecycle_state").?.string);
    try std.testing.expectEqualStrings("covered_with_bounds", sort.get("index_sort_coverage").?.string);
    try std.testing.expectEqual(@as(i64, 7), sort.get("candidate_count").?.integer);
    try std.testing.expectEqual(@as(i64, 1), sort.get("cursor_rejected_count").?.integer);
    try std.testing.expectEqual(@as(i64, 2), sort.get("selected_count").?.integer);
    try std.testing.expectEqual(@as(i64, 41), sort.get("total_us").?.integer);
    try std.testing.expectEqual(@as(i64, 3), sort.get("distributed_shard_count").?.integer);
    try std.testing.expectEqualStrings("match_all_candidate_collect_limit", sort.get("budget_rejection_reason").?.string);
    try std.testing.expectEqualStrings("field_not_sort_ready", sort.get("sort_rejection_reason").?.string);
    try std.testing.expectEqualStrings("missing_doc_values_section", sort.get("sort_rejection_detail").?.string);
    try std.testing.expectEqualStrings("created_at", sort.get("sort_rejection_field").?.string);
    try std.testing.expect(sort.get("native_loader") == null);
    try std.testing.expect(sort.get("native_doc_values_coverage") == null);
    try std.testing.expect(sort.get("distributed_shard_window") == null);
}

test "api query contract serializes sort profile diagnostics" {
    try expectSortProfileDiagnosticsSerializationForTest();
}

test "api query contract maps public exact sort rejection diagnostics" {
    try expectPublicExactSortRejectionMappingForTest();
}

test "api query contract serializes ordered hit sort tuple" {
    const alloc = std.testing.allocator;

    var result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = try alloc.alloc(db_mod.types.SearchHit, 1),
        .total_hits = 1,
    };
    defer result.deinit();

    const sort_values = try alloc.alloc(std.json.Value, 2);
    sort_values[0] = .{ .integer = 42 };
    sort_values[1] = .{ .string = try alloc.dupe(u8, "doc:a") };
    result.hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .score = 0.9,
        .sort_values = sort_values,
    };

    const order_by = [_]db_mod.types.SortField{.{ .field = "created_at", .desc = true }};
    var response = try encodeQueryResponses(alloc, "docs", .{
        .order_by = &order_by,
    }, .{}, result);
    defer response.deinit(alloc);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, response.json, .{});
    defer parsed.deinit();
    const hit = parsed.value.object.get("responses").?.array.items[0].object.get("hits").?.object.get("hits").?.array.items[0].object;
    const sort = hit.get("_sort").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), sort.len);
    try std.testing.expectEqual(@as(i64, 42), sort[0].integer);
    try std.testing.expectEqualStrings("doc:a", sort[1].string);
}

test "api query contract serializes cursor-only id sort tuple" {
    const alloc = std.testing.allocator;

    var result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = try alloc.alloc(db_mod.types.SearchHit, 1),
        .total_hits = 1,
    };
    defer result.deinit();

    const sort_values = try alloc.alloc(std.json.Value, 1);
    sort_values[0] = .{ .string = try alloc.dupe(u8, "doc:a") };
    result.hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .score = 0.9,
        .sort_values = sort_values,
    };
    result.sort_profile = .{
        .plan = "id_seek",
        .exactness = "exact",
        .source = "primary_key_scan",
        .cursor_support = "segment_seek",
        .source_load = "source_free",
        .distributed_behavior = "shard_local_only",
        .sort_lifecycle_state = "queryable",
    };

    const cursor = [_]std.json.Value{.{ .string = "doc:0" }};
    var response = try encodeQueryResponses(alloc, "docs", .{
        .profile = true,
        .search_after = &cursor,
    }, .{}, result);
    defer response.deinit(alloc);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, response.json, .{});
    defer parsed.deinit();
    const hit = parsed.value.object.get("responses").?.array.items[0].object.get("hits").?.object.get("hits").?.array.items[0].object;
    const sort = hit.get("_sort").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), sort.len);
    try std.testing.expectEqualStrings("doc:a", sort[0].string);

    const profile_sort = parsed.value.object.get("responses").?.array.items[0].object.get("profile").?.object.get("sort").?.object;
    const emitted_order = profile_sort.get("order_by").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), emitted_order.len);
    try std.testing.expectEqualStrings("_id", emitted_order[0].object.get("field").?.string);
    try std.testing.expect(!emitted_order[0].object.get("desc").?.bool);
    try std.testing.expectEqualStrings("after", profile_sort.get("cursor").?.string);
    try std.testing.expectEqualStrings("id_seek", profile_sort.get("plan").?.string);

    var before_response = try encodeQueryResponses(alloc, "docs", .{
        .profile = true,
        .search_before = &cursor,
    }, .{}, result);
    defer before_response.deinit(alloc);

    var before_parsed = try std.json.parseFromSlice(std.json.Value, alloc, before_response.json, .{});
    defer before_parsed.deinit();
    const before_profile_sort = before_parsed.value.object.get("responses").?.array.items[0].object.get("profile").?.object.get("sort").?.object;
    const before_emitted_order = before_profile_sort.get("order_by").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), before_emitted_order.len);
    try std.testing.expectEqualStrings("_id", before_emitted_order[0].object.get("field").?.string);
    try std.testing.expect(!before_emitted_order[0].object.get("desc").?.bool);
    try std.testing.expectEqualStrings("before", before_profile_sort.get("cursor").?.string);
    try std.testing.expectEqualStrings("id_seek", before_profile_sort.get("plan").?.string);
}

test "api query contract validates cursor-only implicit id sort tuple" {
    const alloc = std.testing.allocator;

    var result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = try alloc.alloc(db_mod.types.SearchHit, 1),
        .total_hits = 1,
    };
    defer result.deinit();

    const sort_values = try alloc.alloc(std.json.Value, 1);
    sort_values[0] = .{ .string = try alloc.dupe(u8, "doc:a") };
    result.hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .sort_values = sort_values,
    };

    const cursor = [_]std.json.Value{.{ .string = "doc:0" }};
    var response = try encodeQueryResponses(alloc, "docs", .{
        .search_after = &cursor,
    }, .{}, result);
    defer response.deinit(alloc);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, response.json, .{});
    defer parsed.deinit();
    const hit = parsed.value.object.get("responses").?.array.items[0].object.get("hits").?.object.get("hits").?.array.items[0].object;
    const sort = hit.get("_sort").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), sort.len);
    try std.testing.expectEqualStrings("doc:a", sort[0].string);

    var bad_result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = try alloc.alloc(db_mod.types.SearchHit, 1),
        .total_hits = 1,
    };
    defer bad_result.deinit();

    const bad_sort_values = try alloc.alloc(std.json.Value, 1);
    bad_sort_values[0] = .{ .string = try alloc.dupe(u8, "doc:b") };
    bad_result.hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .sort_values = bad_sort_values,
    };

    try std.testing.expectError(error.InvalidQueryRequest, encodeQueryResponses(alloc, "docs", .{
        .search_after = &cursor,
    }, .{}, bad_result));
}

test "api query contract rejects ordered hits without complete sort tuple" {
    const alloc = std.testing.allocator;

    const order_by = [_]db_mod.types.SortField{.{ .field = "created_at", .desc = true }};

    var missing = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = try alloc.alloc(db_mod.types.SearchHit, 1),
        .total_hits = 1,
    };
    defer missing.deinit();
    missing.hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .score = 0.9,
    };
    db_mod.resetLastSortRejectionDiagnostic();
    try std.testing.expectError(error.InvalidQueryRequest, encodeQueryResponses(alloc, "docs", .{
        .order_by = &order_by,
    }, .{}, missing));
    var diagnostic = db_mod.takeLastSortRejectionDiagnostic() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("*", diagnostic.field);
    try std.testing.expectEqualStrings("invalid_sort_tuple", diagnostic.reason);
    try std.testing.expectEqualStrings("sort_tuple_arity", diagnostic.detail);

    var incomplete = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = try alloc.alloc(db_mod.types.SearchHit, 1),
        .total_hits = 1,
    };
    defer incomplete.deinit();
    const sort_values = try alloc.alloc(std.json.Value, 1);
    sort_values[0] = .{ .integer = 42 };
    incomplete.hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .score = 0.9,
        .sort_values = sort_values,
    };
    db_mod.resetLastSortRejectionDiagnostic();
    try std.testing.expectError(error.InvalidQueryRequest, encodeQueryResponses(alloc, "docs", .{
        .order_by = &order_by,
    }, .{}, incomplete));
    diagnostic = db_mod.takeLastSortRejectionDiagnostic() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("*", diagnostic.field);
    try std.testing.expectEqualStrings("invalid_sort_tuple", diagnostic.reason);
    try std.testing.expectEqualStrings("sort_tuple_arity", diagnostic.detail);
}

test "api query contract rejects ordered hits with non replayable sort tuple" {
    const alloc = std.testing.allocator;

    const order_by = [_]db_mod.types.SortField{.{ .field = "created_at", .desc = true }};

    var nested = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = try alloc.alloc(db_mod.types.SearchHit, 1),
        .total_hits = 1,
    };
    defer nested.deinit();
    const nested_sort_values = try alloc.alloc(std.json.Value, 2);
    nested_sort_values[0] = .{ .array = std.json.Array.init(alloc) };
    nested_sort_values[1] = .{ .string = try alloc.dupe(u8, "doc:a") };
    nested.hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .score = 0.9,
        .sort_values = nested_sort_values,
    };
    db_mod.resetLastSortRejectionDiagnostic();
    try std.testing.expectError(error.InvalidQueryRequest, encodeQueryResponses(alloc, "docs", .{
        .order_by = &order_by,
    }, .{}, nested));
    var diagnostic = db_mod.takeLastSortRejectionDiagnostic() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("created_at", diagnostic.field);
    try std.testing.expectEqualStrings("invalid_sort_tuple", diagnostic.reason);
    try std.testing.expectEqualStrings("invalid_cursor_type", diagnostic.detail);

    var null_sort = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = try alloc.alloc(db_mod.types.SearchHit, 1),
        .total_hits = 1,
    };
    defer null_sort.deinit();
    const null_sort_values = try alloc.alloc(std.json.Value, 2);
    null_sort_values[0] = .null;
    null_sort_values[1] = .{ .string = try alloc.dupe(u8, "doc:a") };
    null_sort.hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .score = 0.9,
        .sort_values = null_sort_values,
    };
    db_mod.resetLastSortRejectionDiagnostic();
    try std.testing.expectError(error.InvalidQueryRequest, encodeQueryResponses(alloc, "docs", .{
        .order_by = &order_by,
    }, .{}, null_sort));
    diagnostic = db_mod.takeLastSortRejectionDiagnostic() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("created_at", diagnostic.field);
    try std.testing.expectEqualStrings("invalid_sort_tuple", diagnostic.reason);
    try std.testing.expectEqualStrings("invalid_cursor_type", diagnostic.detail);

    var non_finite = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = try alloc.alloc(db_mod.types.SearchHit, 1),
        .total_hits = 1,
    };
    defer non_finite.deinit();
    const non_finite_sort_values = try alloc.alloc(std.json.Value, 2);
    non_finite_sort_values[0] = .{ .float = std.math.inf(f64) };
    non_finite_sort_values[1] = .{ .string = try alloc.dupe(u8, "doc:a") };
    non_finite.hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .score = 0.9,
        .sort_values = non_finite_sort_values,
    };
    db_mod.resetLastSortRejectionDiagnostic();
    try std.testing.expectError(error.InvalidQueryRequest, encodeQueryResponses(alloc, "docs", .{
        .order_by = &order_by,
    }, .{}, non_finite));
    diagnostic = db_mod.takeLastSortRejectionDiagnostic() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("created_at", diagnostic.field);
    try std.testing.expectEqualStrings("invalid_sort_tuple", diagnostic.reason);
    try std.testing.expectEqualStrings("invalid_cursor_type", diagnostic.detail);

    const score_order_by = [_]db_mod.types.SortField{.{ .field = "_score", .desc = true }};
    var non_numeric_score = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = try alloc.alloc(db_mod.types.SearchHit, 1),
        .total_hits = 1,
    };
    defer non_numeric_score.deinit();
    const non_numeric_score_sort_values = try alloc.alloc(std.json.Value, 2);
    non_numeric_score_sort_values[0] = .{ .string = try alloc.dupe(u8, "high") };
    non_numeric_score_sort_values[1] = .{ .string = try alloc.dupe(u8, "doc:a") };
    non_numeric_score.hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .score = 0.9,
        .sort_values = non_numeric_score_sort_values,
    };
    db_mod.resetLastSortRejectionDiagnostic();
    try std.testing.expectError(error.InvalidQueryRequest, encodeQueryResponses(alloc, "docs", .{
        .order_by = &score_order_by,
    }, .{}, non_numeric_score));
    diagnostic = db_mod.takeLastSortRejectionDiagnostic() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("_score", diagnostic.field);
    try std.testing.expectEqualStrings("invalid_sort_tuple", diagnostic.reason);
    try std.testing.expectEqualStrings("non_numeric_score", diagnostic.detail);

    var id_mismatch = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = try alloc.alloc(db_mod.types.SearchHit, 1),
        .total_hits = 1,
    };
    defer id_mismatch.deinit();
    const mismatched_sort_values = try alloc.alloc(std.json.Value, 2);
    mismatched_sort_values[0] = .{ .integer = 42 };
    mismatched_sort_values[1] = .{ .string = try alloc.dupe(u8, "doc:b") };
    id_mismatch.hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .score = 0.9,
        .sort_values = mismatched_sort_values,
    };
    db_mod.resetLastSortRejectionDiagnostic();
    try std.testing.expectError(error.InvalidQueryRequest, encodeQueryResponses(alloc, "docs", .{
        .order_by = &order_by,
    }, .{}, id_mismatch));
    diagnostic = db_mod.takeLastSortRejectionDiagnostic() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("_id", diagnostic.field);
    try std.testing.expectEqualStrings("invalid_sort_tuple", diagnostic.reason);
    try std.testing.expectEqualStrings("id_tiebreaker_mismatch", diagnostic.detail);
}

test "api query contract serializes derived hierarchy ancestry" {
    const alloc = std.testing.allocator;

    var result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = try alloc.alloc(db_mod.types.SearchHit, 1),
        .total_hits = 1,
    };
    defer result.deinit();

    result.hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .score = 0.8,
        .stored_data = try alloc.dupe(u8, "{\"title\":\"source title\",\"private\":\"omit me\"}"),
        .artifact_ref = .{
            .document_id = try alloc.dupe(u8, "doc:a"),
            .name = try alloc.dupe(u8, "title_dense_v1"),
            .kind = .embedding,
        },
        .chunk_hits = try alloc.alloc(db_mod.types.ChunkHit, 1),
    };
    result.hits[0].chunk_hits[0] = .{
        .id = try alloc.dupe(u8, "af1:chunk:ZG9jOmE:ZG9jdW1lbnRfY2h1bmtzX3Yx:3:unit:cGFnZTowMDAwMDE"),
        .score = 0.7,
        .stored_data = try alloc.dupe(u8,
            \\{"text":"chunk text","_parent_doc_key":"doc:a","_parent_unit_id":"page:000001","_source_artifact_name":"document_units_v1","_source_field":"body","_artifact_unit_fingerprint":"storage-only"}
        ),
        .artifact_ref = .{
            .document_id = try alloc.dupe(u8, "doc:a"),
            .name = try alloc.dupe(u8, "document_chunks_v1"),
            .kind = .chunk,
            .chunk_id = 3,
            .unit_id = try alloc.dupe(u8, "page:000001"),
        },
    };

    var legacy_response = try encodeQueryResponses(alloc, "docs", .{}, .{}, result);
    defer legacy_response.deinit(alloc);
    var parsed_legacy = try std.json.parseFromSlice(std.json.Value, alloc, legacy_response.json, .{});
    defer parsed_legacy.deinit();
    const legacy_hit = parsed_legacy.value.object.get("responses").?.array.items[0].object.get("hits").?.object.get("hits").?.array.items[0];
    const legacy_hierarchy = legacy_hit.object.get("hierarchy") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("source", legacy_hierarchy.object.get("level").?.string);
    try std.testing.expectEqualStrings("title_dense_v1", legacy_hierarchy.object.get("matched_artifact").?.object.get("name").?.string);
    try std.testing.expect(legacy_hierarchy.object.get("artifact") == null);
    try std.testing.expect(legacy_hierarchy.object.get("chunks") != null);
    try std.testing.expect(legacy_hierarchy.object.get("matches") == null);
    const legacy_chunk_source = legacy_hierarchy.object.get("chunks").?.array.items[0].object.get("_source").?.object;
    try std.testing.expect(legacy_chunk_source.get(hierarchy_navigation.unit_fingerprint_field) == null);

    const match_fields = [_][]const u8{"text"};
    const source_hit_fields = [_][]const u8{"title"};
    var response = try encodeQueryResponses(alloc, "docs", .{
        .return_mode = .parent_with_chunks,
        .include_stored = false,
        .defer_stored_projection = true,
        .fields = &source_hit_fields,
        .include_all_fields = false,
        .hierarchy_match_fields = &match_fields,
        .hierarchy_match_include_all_fields = false,
        .hierarchy_grouped_matches = true,
        .hierarchy_omit_implicit_source_ancestor_document = true,
    }, .{}, result);
    defer response.deinit(alloc);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, response.json, .{});
    defer parsed.deinit();
    const hit = parsed.value.object.get("responses").?.array.items[0].object.get("hits").?.object.get("hits").?.array.items[0];
    const hierarchy = hit.object.get("hierarchy") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("source title", hit.object.get("_source").?.object.get("title").?.string);
    try std.testing.expect(hit.object.get("_source").?.object.get("private") == null);
    try std.testing.expectEqualStrings("source", hierarchy.object.get("level").?.string);
    try std.testing.expectEqualStrings("doc:a", hierarchy.object.get("parent_doc_key").?.string);
    try std.testing.expectEqualStrings("title_dense_v1", hierarchy.object.get("matched_artifact").?.object.get("name").?.string);
    const source_ancestor = hierarchy.object.get("ancestors").?.object.get("source").?.object;
    try std.testing.expectEqualStrings("doc:a", source_ancestor.get("id").?.string);
    try std.testing.expect(source_ancestor.get("document") == null);
    try std.testing.expect(hierarchy.object.get("chunks") == null);
    const matches = hierarchy.object.get("matches").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), matches.len);
    try std.testing.expectEqualStrings("chunk text", matches[0].object.get("_source").?.object.get("text").?.string);
    try std.testing.expect(matches[0].object.get("_source").?.object.get("_parent_doc_key") == null);
    const chunk_hierarchy = matches[0].object.get("hierarchy").?.object;
    try std.testing.expectEqualStrings("chunk", chunk_hierarchy.get("level").?.string);
    try std.testing.expectEqualStrings("page:000001", chunk_hierarchy.get("parent_unit_id").?.string);
    try std.testing.expectEqual(@as(i64, 3), chunk_hierarchy.get("artifact").?.object.get("chunk_id").?.integer);
    const unit_ancestor = chunk_hierarchy.get("ancestors").?.object.get("unit").?.object;
    try std.testing.expectEqualStrings("page:000001", unit_ancestor.get("id").?.string);
    try std.testing.expectEqualStrings("document_units_v1", unit_ancestor.get("artifact_name").?.string);
    try std.testing.expectEqualStrings("body", unit_ancestor.get("source_field").?.string);
}

test "api query contract preserves the internal grouped unit revision envelope" {
    const alloc = std.testing.allocator;
    var result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = try alloc.alloc(db_mod.types.SearchHit, 1),
        .total_hits = 1,
    };
    defer result.deinit();
    result.hits[0] = .{
        .id = try alloc.dupe(u8, "unit:0"),
        .stored_data = try alloc.dupe(u8, "{\"_hierarchy_unit_revision_token\":\"unit-revision\"}"),
        .artifact_ref = .{
            .document_id = try alloc.dupe(u8, "doc:a"),
            .name = try alloc.dupe(u8, "document_units_v1"),
            .kind = .asset,
            .unit_id = try alloc.dupe(u8, "page:000001"),
        },
    };

    const fields = [_][]const u8{"text"};
    var response = try encodeQueryResponses(alloc, "docs", .{
        .return_mode = .unit_with_chunks,
        .hierarchy_group_level = .unit,
        .include_stored = true,
        .include_all_fields = false,
        .fields = &fields,
        .defer_stored_projection = true,
        .defer_hierarchy_child_hydration = true,
    }, .{}, result);
    defer response.deinit(alloc);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, response.json, .{});
    defer parsed.deinit();
    const source = parsed.value.object.get("responses").?.array.items[0]
        .object.get("hits").?.object.get("hits").?.array.items[0]
        .object.get("_source").?.object;
    try std.testing.expectEqualStrings(
        "unit-revision",
        source.get(hierarchy_navigation.grouped_unit_revision_envelope_field).?.string,
    );

    var public_response = try encodeQueryResponses(alloc, "docs", .{
        .return_mode = .unit,
        .hierarchy_group_level = .unit,
        .include_stored = false,
        .include_all_fields = false,
        .fields = &.{},
    }, .{}, result);
    defer public_response.deinit(alloc);
    var parsed_public = try std.json.parseFromSlice(std.json.Value, alloc, public_response.json, .{});
    defer parsed_public.deinit();
    const public_source = parsed_public.value.object.get("responses").?.array.items[0]
        .object.get("hits").?.object.get("hits").?.array.items[0]
        .object.get("_source").?.object;
    try std.testing.expect(public_source.get(hierarchy_navigation.grouped_unit_revision_envelope_field) == null);
}

test "api query contract serializes hydrated unit ancestor for direct unit hits" {
    const alloc = std.testing.allocator;
    const navigation_position = try hierarchy_navigation.positionAlloc(
        alloc,
        "0000000000000000000000000000000000000000000000000000000000000000",
        "document_units_v1",
        1,
        0,
        "storage-only",
    );
    defer alloc.free(navigation_position);

    var result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = try alloc.alloc(db_mod.types.SearchHit, 1),
        .total_hits = 1,
    };
    defer result.deinit();

    result.hits[0] = .{
        .id = try alloc.dupe(u8, "af1:asset:ZG9jOmE:ZG9jdW1lbnRfdW5pdHNfdjE:unit:cGFnZTowMDAwMDE"),
        .score = 0.9,
        .stored_data = try alloc.dupe(u8,
            \\{"unit_id":"page:000001","unit_type":"page","text":"unit text","_artifact_unit_fingerprint":"storage-only","_hierarchy":{"position":"stale-position","revision":"document_units_v1@1"},"provenance":{"method":"pdf_text","page_number":1}}
        ),
        .ancestor_unit_data = try alloc.dupe(u8, "{\"text\":\"independent ancestor projection\"}"),
        .artifact_ref = .{
            .document_id = try alloc.dupe(u8, "doc:a"),
            .name = try alloc.dupe(u8, "document_units_v1"),
            .kind = .asset,
            .unit_id = try alloc.dupe(u8, "page:000001"),
        },
        .sort_values = try db_mod.types.cloneJsonValues(alloc, &.{
            .{ .string = navigation_position },
            .{ .string = "af1:asset:ZG9jOmE:ZG9jdW1lbnRfdW5pdHNfdjE:unit:cGFnZTowMDAwMDE" },
        }),
    };

    var response = try encodeQueryResponses(alloc, "docs", .{
        .return_mode = .chunk,
        .include_stored = true,
        .hierarchy_include_unit = true,
    }, .{}, result);
    defer response.deinit(alloc);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, response.json, .{});
    defer parsed.deinit();
    const hit = parsed.value.object.get("responses").?.array.items[0].object.get("hits").?.object.get("hits").?.array.items[0];
    const hierarchy = hit.object.get("hierarchy").?.object;
    try std.testing.expect(hit.object.get("_source").?.object.get(hierarchy_navigation.unit_fingerprint_field) == null);
    try std.testing.expectEqualStrings("unit", hierarchy.get("level").?.string);
    try std.testing.expect(hierarchy.get("position") == null);
    try std.testing.expect(hierarchy.get("revision") == null);
    const ancestors = hierarchy.get("ancestors").?.object;
    try std.testing.expectEqualStrings("doc:a", ancestors.get("source").?.object.get("id").?.string);
    const unit = ancestors.get("unit").?.object;
    try std.testing.expectEqualStrings("page:000001", unit.get("id").?.string);
    try std.testing.expectEqualStrings("independent ancestor projection", unit.get("document").?.object.get("text").?.string);
    try std.testing.expect(unit.get("document").?.object.get(hierarchy_navigation.unit_fingerprint_field) == null);

    alloc.free(result.hits[0].ancestor_unit_data.?);
    result.hits[0].ancestor_unit_data = null;
    const navigation_fields = [_][]const u8{ "text", hierarchy_navigation.unit_fingerprint_field };
    var navigation_response = try encodeQueryResponses(alloc, "docs", .{
        .return_mode = .unit,
        .hierarchy_children = .{ .parent_id = "doc:a" },
        .include_stored = true,
        .include_all_fields = false,
        .defer_stored_projection = true,
        .fields = &navigation_fields,
    }, .{}, result);
    defer navigation_response.deinit(alloc);
    var parsed_navigation = try std.json.parseFromSlice(std.json.Value, alloc, navigation_response.json, .{});
    defer parsed_navigation.deinit();
    const navigation_hit = parsed_navigation.value.object.get("responses").?.array.items[0].object.get("hits").?.object.get("hits").?.array.items[0];
    try std.testing.expectEqualStrings("unit text", navigation_hit.object.get("_source").?.object.get("text").?.string);
    try std.testing.expect(navigation_hit.object.get("_source").?.object.get("unit_id") == null);
    try std.testing.expect(navigation_hit.object.get("_source").?.object.get(hierarchy_navigation.unit_fingerprint_field) == null);
    const navigation_hierarchy = navigation_hit.object.get("hierarchy").?.object;
    try std.testing.expectEqualStrings(navigation_position, navigation_hierarchy.get("position").?.string);
    try std.testing.expectEqualStrings(
        "source@0000000000000000000000000000000000000000000000000000000000000000",
        navigation_hierarchy.get("revision").?.string,
    );
    const navigation_unit = navigation_hierarchy.get("ancestors").?.object.get("unit").?.object;
    try std.testing.expect(navigation_unit.get("document") == null);

    var identity_result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = try alloc.alloc(db_mod.types.SearchHit, 1),
        .total_hits = 1,
    };
    defer identity_result.deinit();
    identity_result.hits[0] = .{
        .id = try alloc.dupe(u8, "af1:asset:ZG9jOmE:ZG9jdW1lbnRfdW5pdHNfdjE:unit:cGFnZTowMDAwMDE"),
        .score = 1,
        .artifact_ref = .{
            .document_id = try alloc.dupe(u8, "doc:a"),
            .name = try alloc.dupe(u8, "document_units_v1"),
            .kind = .asset,
            .unit_id = try alloc.dupe(u8, "page:000001"),
        },
        .sort_values = try db_mod.types.cloneJsonValues(alloc, &.{
            .{ .string = navigation_position },
            .{ .string = "af1:asset:ZG9jOmE:ZG9jdW1lbnRfdW5pdHNfdjE:unit:cGFnZTowMDAwMDE" },
        }),
    };
    var identity_response = try encodeQueryResponses(alloc, "docs", .{
        .return_mode = .unit,
        .hierarchy_children = .{ .parent_id = "doc:a" },
        .include_stored = false,
        .include_all_fields = false,
        .fields = &.{},
    }, .{}, identity_result);
    defer identity_response.deinit(alloc);
    var parsed_identity = try std.json.parseFromSlice(std.json.Value, alloc, identity_response.json, .{});
    defer parsed_identity.deinit();
    const identity_hit = parsed_identity.value.object.get("responses").?.array.items[0].object.get("hits").?.object.get("hits").?.array.items[0];
    try std.testing.expect(identity_hit.object.get("_source") == null);
    try std.testing.expectEqualStrings(
        navigation_position,
        identity_hit.object.get("hierarchy").?.object.get("position").?.string,
    );
}

test "api query contract serializes db-backed ancestors for direct chunk hits" {
    const alloc = std.testing.allocator;

    var result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = try alloc.alloc(db_mod.types.SearchHit, 1),
        .total_hits = 1,
    };
    defer result.deinit();

    result.hits[0] = .{
        .id = try alloc.dupe(u8, "af1:chunk:ZG9jOmE:ZG9jdW1lbnRfY2h1bmtzX3Yx:3:unit:cGFnZTowMDAwMDE"),
        .score = 0.7,
        .stored_data = try alloc.dupe(u8,
            \\{"text":"chunk text","_parent_doc_key":"doc:a","_parent_unit_id":"page:000001","_source_artifact_name":"document_units_v1"}
        ),
        .ancestor_source_data = try alloc.dupe(u8, "{\"title\":\"source doc\",\"private\":\"omit me\"}"),
        .ancestor_unit_data = try alloc.dupe(u8, "{\"unit_id\":\"page:000001\",\"text\":\"unit doc\",\"private\":\"omit me\"}"),
        .artifact_ref = .{
            .document_id = try alloc.dupe(u8, "doc:a"),
            .name = try alloc.dupe(u8, "document_chunks_v1"),
            .kind = .chunk,
            .chunk_id = 3,
            .unit_id = try alloc.dupe(u8, "page:000001"),
        },
    };

    const source_fields = [_][]const u8{"title"};
    const unit_fields = [_][]const u8{"text"};
    var response = try encodeQueryResponses(alloc, "docs", .{
        .return_mode = .chunk,
        .include_stored = true,
        .hierarchy_source_fields = &source_fields,
        .hierarchy_source_include_all_fields = false,
        .hierarchy_unit_fields = &unit_fields,
        .hierarchy_unit_include_all_fields = false,
    }, .{}, result);
    defer response.deinit(alloc);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, response.json, .{});
    defer parsed.deinit();
    const hit = parsed.value.object.get("responses").?.array.items[0].object.get("hits").?.object.get("hits").?.array.items[0];
    const hierarchy = hit.object.get("hierarchy").?.object;
    const ancestors = hierarchy.get("ancestors").?.object;
    const source_document = ancestors.get("source").?.object.get("document").?.object;
    try std.testing.expectEqualStrings("source doc", source_document.get("title").?.string);
    try std.testing.expect(source_document.get("private") == null);
    const unit = ancestors.get("unit").?.object;
    try std.testing.expectEqualStrings("page:000001", unit.get("id").?.string);
    const unit_document = unit.get("document").?.object;
    try std.testing.expectEqualStrings("unit doc", unit_document.get("text").?.string);
    try std.testing.expect(unit_document.get("unit_id") == null);
    try std.testing.expect(unit_document.get("private") == null);
}

test "api query contract serializes mention evidence hierarchy" {
    const alloc = std.testing.allocator;

    var result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = try alloc.alloc(db_mod.types.SearchHit, 1),
        .total_hits = 1,
    };
    defer result.deinit();

    result.hits[0] = .{
        .id = try alloc.dupe(u8, "af1:asset:ZG9jOmE:X3Jlc29sdXRpb25fbWVudGlvbg"),
        .score = 0.95,
        .stored_data = try alloc.dupe(u8,
            \\{
            \\  "_schema":"antfly.resolution_mention.v1",
            \\  "_parent_doc_key":"doc:a",
            \\  "_artifact_kind":"resolution_mention",
            \\  "_artifact_key":"mention-key",
            \\  "source_artifact":"relations_v1",
            \\  "source_artifact_key":"source-key",
            \\  "resolution_artifact":"resolution_v1",
            \\  "resolution_artifact_key":"resolution-key",
            \\  "resolver":"kg",
            \\  "resolver_table":"entities",
            \\  "local_id":"e0",
            \\  "decision":"match",
            \\  "confidence":0.87,
            \\  "canonical":{"table":"entities","key":"person/ada_lovelace","name":"Ada Lovelace","label":"person"},
            \\  "mention":{"text":"Ada Lovelace","label":"person","confidence":0.91}
            \\}
        ),
        .artifact_ref = .{
            .document_id = try alloc.dupe(u8, "doc:a"),
            .name = try alloc.dupe(u8, "_resolution_mention"),
            .kind = .asset,
        },
    };

    var response = try encodeQueryResponses(alloc, "docs", .{
        .return_mode = .chunk,
        .include_stored = true,
    }, .{}, result);
    defer response.deinit(alloc);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, response.json, .{});
    defer parsed.deinit();
    const hit = parsed.value.object.get("responses").?.array.items[0].object.get("hits").?.object.get("hits").?.array.items[0];
    const hierarchy = hit.object.get("hierarchy").?.object;
    try std.testing.expectEqualStrings("mention", hierarchy.get("level").?.string);
    try std.testing.expectEqualStrings("doc:a", hierarchy.get("parent_doc_key").?.string);
    const evidence = hierarchy.get("evidence").?.object;
    try std.testing.expectEqualStrings("e0", evidence.get("local_id").?.string);
    try std.testing.expectEqualStrings("match", evidence.get("decision").?.string);
    try std.testing.expectEqualStrings("source-key", evidence.get("source_artifact_key").?.string);
    try std.testing.expectEqualStrings("resolution-key", evidence.get("resolution_artifact_key").?.string);
    try std.testing.expectEqualStrings("Ada Lovelace", evidence.get("mention").?.object.get("text").?.string);
    try std.testing.expectEqualStrings("person/ada_lovelace", evidence.get("canonical").?.object.get("key").?.string);
}

fn parseStoredSourceValue(alloc: std.mem.Allocator, stored_data: []const u8) !std.json.Value {
    var scratch = std.heap.ArenaAllocator.init(alloc);
    defer scratch.deinit();
    var value = std.json.parseFromSliceLeaky(std.json.Value, scratch.allocator(), stored_data, .{}) catch {
        return .{ .string = try alloc.dupe(u8, stored_data) };
    };
    hierarchy_navigation.stripPublicInternalFieldsValue(scratch.allocator(), &value);
    return try db_mod.types.cloneJsonValue(alloc, value);
}

fn parseInternalGroupedUnitRevisionEnvelopeValue(
    alloc: std.mem.Allocator,
    stored_data: []const u8,
) !std.json.Value {
    var scratch = std.heap.ArenaAllocator.init(alloc);
    defer scratch.deinit();
    const value = try std.json.parseFromSliceLeaky(std.json.Value, scratch.allocator(), stored_data, .{});
    if (value != .object or value.object.count() != 1) return error.InvalidDocumentExtractionState;
    const revision = value.object.get(hierarchy_navigation.grouped_unit_revision_envelope_field) orelse
        return error.InvalidDocumentExtractionState;
    if (revision != .string or revision.string.len == 0) return error.InvalidDocumentExtractionState;
    return try db_mod.types.cloneJsonValue(alloc, value);
}

fn projectPublicStoredSourceValue(
    alloc: std.mem.Allocator,
    stored_data: []const u8,
    options: db_mod.types.LookupOptions,
) !std.json.Value {
    var value = try document_query.projectLookupJsonValue(alloc, stored_data, options);
    hierarchy_navigation.stripPublicInternalFieldsValue(alloc, &value);
    return value;
}

fn computeMaxScore(hits: []const db_mod.types.SearchHit) f32 {
    var max_score: f32 = 0;
    var found = false;
    for (hits) |hit| {
        const score = hit.score orelse continue;
        if (!found or score > max_score) max_score = score;
        found = true;
    }
    return max_score;
}

test "query max score preserves negative relevance scores" {
    const hits = [_]db_mod.types.SearchHit{
        .{ .id = @constCast("doc:a"), .score = -0.75 },
        .{ .id = @constCast("doc:b"), .score = -0.25 },
        .{ .id = @constCast("doc:c") },
    };
    try std.testing.expectEqual(@as(f32, -0.25), computeMaxScore(&hits));
    try std.testing.expectEqual(@as(f32, 0), computeMaxScore(&.{}));
}

test "query hit exposes relevance score and raw vector distance separately" {
    const hit = try toOpenApiHit(std.testing.allocator, .{}, .{
        .id = @constCast("doc:a"),
        .score = 0.8,
        .distance = 0.25,
    });
    try std.testing.expectEqual(@as(f32, 0.8), hit._score);
    try std.testing.expectEqual(@as(f32, 0.25), hit._distance.?);
}

test "query hierarchy response conversion rejects schema drift" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(
        std.json.Value,
        alloc,
        "{\"level\":\"source\",\"unmodeled_field\":true}",
        .{},
    );
    defer parsed.deinit();

    try std.testing.expectError(
        error.UnknownField,
        parseSearchHitHierarchyOpenApiValue(alloc, parsed.value),
    );
}

pub fn parseAggregationRequestsJson(
    alloc: std.mem.Allocator,
    aggregations_json: []const u8,
) ![]aggregations_mod.SearchAggregationRequest {
    if (aggregations_json.len == 0) return &.{};

    var parsed = std.json.parseFromSlice(std.json.ArrayHashMap(metadata_openapi.AggregationRequest), alloc, aggregations_json, .{}) catch {
        return error.InvalidQueryRequest;
    };
    defer parsed.deinit();
    return try parseAggregationRequestsAlloc(alloc, parsed.value);
}

pub fn freeAggregationRequests(
    alloc: std.mem.Allocator,
    requests: []const aggregations_mod.SearchAggregationRequest,
) void {
    for (requests) |request| {
        alloc.free(request.name);
        alloc.free(request.type);
        alloc.free(request.field);
        for (request.fields) |field| alloc.free(@constCast(field));
        if (request.fields.len > 0) alloc.free(@constCast(request.fields));
        if (request.calendar_interval.len > 0) alloc.free(request.calendar_interval);
        if (request.fixed_interval.len > 0) alloc.free(request.fixed_interval);
        if (request.significance_algorithm.len > 0) alloc.free(request.significance_algorithm);
        if (request.bucket_path.len > 0) alloc.free(request.bucket_path);
        if (request.sort_order.len > 0) alloc.free(request.sort_order);
        if (request.gap_policy.len > 0) alloc.free(request.gap_policy);
        if (request.term_prefix.len > 0) alloc.free(request.term_prefix);
        if (request.term_pattern.len > 0) alloc.free(request.term_pattern);
        if (request.distance_unit.len > 0) alloc.free(request.distance_unit);
        if (request.algebraic_join) |join| {
            alloc.free(join.name);
            if (join.group_side) |side| alloc.free(side);
            if (join.measure_side) |side| alloc.free(side);
        }
        if (request.background_query) |background_query| switch (background_query) {
            .match => |match| {
                alloc.free(match.field);
                alloc.free(match.text);
            },
            .term => |term| {
                alloc.free(term.field);
                alloc.free(term.term);
            },
            .match_all => {},
        };
        for (request.ranges) |range| {
            if (range.name.len > 0) alloc.free(range.name);
        }
        if (request.ranges.len > 0) alloc.free(request.ranges);
        for (request.date_ranges) |range| {
            if (range.name.len > 0) alloc.free(range.name);
            if (range.start) |value| alloc.free(value);
            if (range.end) |value| alloc.free(value);
        }
        if (request.date_ranges.len > 0) alloc.free(request.date_ranges);
        for (request.distance_ranges) |range| {
            if (range.name.len > 0) alloc.free(range.name);
        }
        if (request.distance_ranges.len > 0) alloc.free(request.distance_ranges);
        freeAggregationRequests(alloc, request.aggregations);
    }
    if (requests.len > 0) alloc.free(requests);
}

fn parseAggregationRequestsAlloc(
    alloc: std.mem.Allocator,
    aggregations: std.json.ArrayHashMap(metadata_openapi.AggregationRequest),
) anyerror![]aggregations_mod.SearchAggregationRequest {
    const out = try alloc.alloc(aggregations_mod.SearchAggregationRequest, aggregations.map.count());
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |request| {
            alloc.free(request.name);
            alloc.free(request.type);
            alloc.free(request.field);
            for (request.fields) |field| alloc.free(@constCast(field));
            if (request.fields.len > 0) alloc.free(@constCast(request.fields));
            if (request.calendar_interval.len > 0) alloc.free(request.calendar_interval);
            if (request.fixed_interval.len > 0) alloc.free(request.fixed_interval);
            if (request.significance_algorithm.len > 0) alloc.free(request.significance_algorithm);
            if (request.bucket_path.len > 0) alloc.free(request.bucket_path);
            if (request.sort_order.len > 0) alloc.free(request.sort_order);
            if (request.gap_policy.len > 0) alloc.free(request.gap_policy);
            if (request.term_prefix.len > 0) alloc.free(request.term_prefix);
            if (request.term_pattern.len > 0) alloc.free(request.term_pattern);
            if (request.distance_unit.len > 0) alloc.free(request.distance_unit);
            if (request.algebraic_join) |join| {
                alloc.free(join.name);
                if (join.group_side) |side| alloc.free(side);
                if (join.measure_side) |side| alloc.free(side);
            }
            if (request.background_query) |background_query| switch (background_query) {
                .match => |match| {
                    alloc.free(match.field);
                    alloc.free(match.text);
                },
                .term => |term| {
                    alloc.free(term.field);
                    alloc.free(term.term);
                },
                .match_all => {},
            };
            for (request.ranges) |range| {
                if (range.name.len > 0) alloc.free(range.name);
            }
            if (request.ranges.len > 0) alloc.free(request.ranges);
            for (request.date_ranges) |range| {
                if (range.name.len > 0) alloc.free(range.name);
                if (range.start) |value| alloc.free(value);
                if (range.end) |value| alloc.free(value);
            }
            if (request.date_ranges.len > 0) alloc.free(request.date_ranges);
            for (request.distance_ranges) |range| {
                if (range.name.len > 0) alloc.free(range.name);
            }
            if (request.distance_ranges.len > 0) alloc.free(request.distance_ranges);
            freeAggregationRequests(alloc, request.aggregations);
        }
        alloc.free(out);
    }

    for (aggregations.map.keys(), aggregations.map.values()) |name, aggregation| {
        out[initialized] = try parseSingleAggregationRequestAlloc(alloc, name, aggregation);
        initialized += 1;
    }
    return out;
}

fn parseSingleAggregationRequestAlloc(
    alloc: std.mem.Allocator,
    name: []const u8,
    aggregation: metadata_openapi.AggregationRequest,
) anyerror!aggregations_mod.SearchAggregationRequest {
    const ranges = try alloc.alloc(aggregations_mod.NumericRangeRequest, if (aggregation.ranges) |ranges_value| ranges_value.len else 0);
    errdefer alloc.free(ranges);
    if (aggregation.ranges) |ranges_value| {
        for (ranges_value, 0..) |range, i| {
            ranges[i] = .{
                .name = if (range.name.len > 0) try alloc.dupe(u8, range.name) else "",
                .start = if (range.from) |value| @floatCast(value) else null,
                .end = if (range.to) |value| @floatCast(value) else null,
            };
        }
    }

    const date_ranges = try alloc.alloc(aggregations_mod.DateRangeRequest, if (aggregation.date_ranges) |ranges_value| ranges_value.len else 0);
    errdefer {
        for (date_ranges) |range| {
            if (range.name.len > 0) alloc.free(range.name);
            if (range.start) |value| alloc.free(value);
            if (range.end) |value| alloc.free(value);
        }
        alloc.free(date_ranges);
    }
    if (aggregation.date_ranges) |ranges_value| {
        for (ranges_value, 0..) |range, i| {
            date_ranges[i] = .{
                .name = if (range.name.len > 0) try alloc.dupe(u8, range.name) else "",
                .start = if (range.from) |value| try alloc.dupe(u8, value) else null,
                .end = if (range.to) |value| try alloc.dupe(u8, value) else null,
            };
        }
    }

    const distance_ranges = try alloc.alloc(aggregations_mod.DistanceRangeRequest, if (aggregation.distance_ranges) |ranges_value| ranges_value.len else 0);
    errdefer {
        for (distance_ranges) |range| {
            if (range.name.len > 0) alloc.free(range.name);
        }
        alloc.free(distance_ranges);
    }
    if (aggregation.distance_ranges) |ranges_value| {
        for (ranges_value, 0..) |range, i| {
            distance_ranges[i] = .{
                .name = if (range.name.len > 0) try alloc.dupe(u8, range.name) else "",
                .from = if (range.from) |value| @floatCast(value) else null,
                .to = if (range.to) |value| @floatCast(value) else null,
            };
        }
    }

    const nested = if (aggregation.sub_aggregations) |value|
        try parseAggregationRequestsAlloc(alloc, value)
    else
        &.{};
    errdefer freeAggregationRequests(alloc, nested);
    const fields = if (aggregation.fields) |values| blk: {
        const cloned = try alloc.alloc([]const u8, values.len);
        var initialized_fields: usize = 0;
        errdefer {
            for (cloned[0..initialized_fields]) |field| alloc.free(@constCast(field));
            alloc.free(cloned);
        }
        for (values, 0..) |field, i| {
            if (field.len == 0) return error.InvalidQueryRequest;
            cloned[i] = try alloc.dupe(u8, field);
            initialized_fields += 1;
        }
        break :blk cloned;
    } else &.{};
    errdefer {
        for (fields) |field| alloc.free(@constCast(field));
        if (fields.len > 0) alloc.free(@constCast(fields));
    }
    if (fields.len > 0) {
        if (aggregation.type != .terms) return error.InvalidQueryRequest;
        if (aggregation.field) |field| {
            if (!std.mem.eql(u8, field, fields[0])) return error.InvalidQueryRequest;
        }
    }
    const primary_field = if (fields.len > 0) fields[0] else aggregation.field orelse return error.InvalidQueryRequest;
    const algebraic_join = if (aggregation.algebraic_join) |join|
        try parseAlgebraicAggregationJoinAlloc(alloc, join)
    else
        null;
    errdefer if (algebraic_join) |join| {
        alloc.free(join.name);
        if (join.group_side) |side| alloc.free(side);
        if (join.measure_side) |side| alloc.free(side);
    };

    var center_lat: f64 = 0;
    var center_lon: f64 = 0;
    if (aggregation.origin) |origin| {
        var it = std.mem.splitScalar(u8, origin, ',');
        const lat_text = it.next() orelse return error.InvalidQueryRequest;
        const lon_text = it.next() orelse return error.InvalidQueryRequest;
        if (it.next() != null) return error.InvalidQueryRequest;
        center_lat = std.fmt.parseFloat(f64, std.mem.trim(u8, lat_text, &std.ascii.whitespace)) catch return error.InvalidQueryRequest;
        center_lon = std.fmt.parseFloat(f64, std.mem.trim(u8, lon_text, &std.ascii.whitespace)) catch return error.InvalidQueryRequest;
    }

    const cardinality_mode = try parseCardinalityModeJson(alloc, aggregation.mode);

    return .{
        .name = try alloc.dupe(u8, name),
        .type = try alloc.dupe(u8, @tagName(aggregation.type)),
        .field = try alloc.dupe(u8, primary_field),
        .cardinality_mode = cardinality_mode,
        .fields = fields,
        .size = aggregation.size orelse 0,
        .interval = if (aggregation.interval) |value| value else 0,
        .calendar_interval = if (aggregation.calendar_interval) |value|
            try jsonValueToFlatStringAlloc(alloc, value)
        else
            "",
        .min_doc_count = aggregation.min_doc_count orelse 0,
        .significance_algorithm = if (aggregation.algorithm) |value|
            try jsonValueToFlatStringAlloc(alloc, value)
        else
            "",
        .background_query = if (aggregation.background_filter) |value|
            try parseAggregationBackgroundQueryJsonAlloc(alloc, value.bytes)
        else
            null,
        .ranges = ranges,
        .date_ranges = date_ranges,
        .distance_ranges = distance_ranges,
        .center_lat = center_lat,
        .center_lon = center_lon,
        .distance_unit = if (aggregation.unit) |value|
            try jsonValueToFlatStringAlloc(alloc, value)
        else
            "",
        .geohash_precision = if (aggregation.precision) |value|
            std.math.cast(u8, value) orelse return error.InvalidQueryRequest
        else
            0,
        .algebraic_join = algebraic_join,
        .aggregations = nested,
    };
}

fn parseAlgebraicAggregationJoinAlloc(
    alloc: std.mem.Allocator,
    join: metadata_openapi.AlgebraicAggregationJoin,
) !db_mod.algebraic.ir.JoinRef {
    if (join.name.len == 0 or join.group_side.len == 0 or join.measure_side.len == 0) return error.InvalidQueryRequest;
    const kind: db_mod.algebraic.join.TemporalMode = if (join.kind) |value| blk: {
        if (std.mem.eql(u8, value, "none")) break :blk .none;
        if (std.mem.eql(u8, value, "bucket")) break :blk .bucket;
        if (std.mem.eql(u8, value, "window")) break :blk .window;
        if (std.mem.eql(u8, value, "bucket_window")) break :blk .bucket_window;
        return error.InvalidQueryRequest;
    } else .none;
    return .{
        .name = try alloc.dupe(u8, join.name),
        .kind = kind,
        .group_side = try alloc.dupe(u8, join.group_side),
        .measure_side = try alloc.dupe(u8, join.measure_side),
    };
}

fn jsonValueToFlatStringAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]u8 {
    return switch (value) {
        .string => |text| try alloc.dupe(u8, text),
        else => return error.UnsupportedQueryRequest,
    };
}

fn parseCardinalityModeJson(alloc: std.mem.Allocator, value_opt: ?std.json.Value) !aggregations_mod.CardinalityMode {
    const value = value_opt orelse return .auto;
    const text = try jsonValueToFlatStringAlloc(alloc, value);
    defer alloc.free(text);
    return std.meta.stringToEnum(aggregations_mod.CardinalityMode, text) orelse error.InvalidQueryRequest;
}

fn parseAggregationBackgroundQueryAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
) !aggregations_mod.BackgroundQuery {
    if (value == .object) {
        if (value.object.get("match_all") != null) return .{ .match_all = {} };
        if (value.object.get("match")) |match| {
            if (match == .object and match.object.count() == 1) {
                var it = match.object.iterator();
                const entry = it.next() orelse return error.UnsupportedQueryRequest;
                if (entry.value_ptr.* != .string) return error.UnsupportedQueryRequest;
                return .{ .match = .{
                    .field = try alloc.dupe(u8, entry.key_ptr.*),
                    .text = try alloc.dupe(u8, entry.value_ptr.string),
                } };
            }
        }
        if (value.object.get("term")) |term| {
            if (term == .object and term.object.count() == 1) {
                var it = term.object.iterator();
                const entry = it.next() orelse return error.UnsupportedQueryRequest;
                if (entry.value_ptr.* != .string) return error.UnsupportedQueryRequest;
                return .{ .term = .{
                    .field = try alloc.dupe(u8, entry.key_ptr.*),
                    .term = try alloc.dupe(u8, entry.value_ptr.string),
                } };
            }
        }
    }
    return error.UnsupportedQueryRequest;
}

fn parseAggregationBackgroundQueryJsonAlloc(
    alloc: std.mem.Allocator,
    raw: []const u8,
) !aggregations_mod.BackgroundQuery {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();
    return try parseAggregationBackgroundQueryAlloc(alloc, parsed.value);
}

fn buildAggregationResults(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
    results: []const aggregations_mod.SearchAggregationResult,
) !std.json.ArrayHashMap(metadata_openapi.AggregationResult) {
    var out: std.json.ArrayHashMap(metadata_openapi.AggregationResult) = .{};
    errdefer out.deinit(alloc);

    var parsed_request_map: ?std.json.Parsed(std.json.ArrayHashMap(metadata_openapi.AggregationRequest)) = null;
    defer if (parsed_request_map) |*parsed| parsed.deinit();
    if (req.aggregations_json.len > 0) {
        parsed_request_map = std.json.parseFromSlice(std.json.ArrayHashMap(metadata_openapi.AggregationRequest), alloc, req.aggregations_json, .{}) catch {
            return error.InvalidQueryRequest;
        };
    }

    for (results) |result| {
        const request = if (parsed_request_map) |*parsed|
            parsed.value.map.get(result.name)
        else
            null;
        try out.map.put(alloc, result.name, try toOpenApiAggregationResult(alloc, request, result));
    }
    return out;
}

fn toOpenApiAggregationResult(
    alloc: std.mem.Allocator,
    request: ?metadata_openapi.AggregationRequest,
    result: aggregations_mod.SearchAggregationResult,
) anyerror!metadata_openapi.AggregationResult {
    var out: metadata_openapi.AggregationResult = .{};

    if (result.value_json) |value_json| {
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, value_json, .{});
        defer parsed.deinit();
        switch (parsed.value) {
            .float => |value| out.value = @floatCast(value),
            .integer => |value| out.value = @floatFromInt(value),
            .object => |object| {
                if (object.get("value")) |value| out.value = try jsonValueToF32(value);
                if (object.get("approximate")) |value| {
                    if (value == .bool) out.approximate = value.bool;
                }
                if (object.get("relative_error")) |value| out.relative_error = try jsonValueToF32(value);
                if (object.get("count")) |value| out.count = try jsonValueToI64(value);
                if (object.get("min")) |value| out.min = try jsonValueToF32(value);
                if (object.get("max")) |value| out.max = try jsonValueToF32(value);
                if (object.get("sum")) |value| out.sum = try jsonValueToF32(value);
                if (object.get("sum_squares")) |value| out.sum_of_squares = try jsonValueToF32(value);
                if (object.get("avg")) |value| out.avg = try jsonValueToF32(value);
                if (object.get("variance")) |value| out.variance = try jsonValueToF32(value);
                if (object.get("std_dev")) |value| out.std_deviation = try jsonValueToF32(value);
            },
            else => {},
        }
    }

    if (result.buckets.len > 0) {
        const buckets = try alloc.alloc(metadata_openapi.AggregationBucket, result.buckets.len);
        for (result.buckets, 0..) |bucket, idx| {
            buckets[idx] = try toOpenApiAggregationBucket(alloc, request, idx, bucket);
        }
        out.buckets = buckets;
    }

    return out;
}

fn toOpenApiAggregationBucket(
    alloc: std.mem.Allocator,
    request: ?metadata_openapi.AggregationRequest,
    idx: usize,
    bucket: aggregations_mod.SearchAggregationBucket,
) anyerror!metadata_openapi.AggregationBucket {
    var parsed_key = try std.json.parseFromSlice(std.json.Value, alloc, bucket.key_json, .{});
    defer parsed_key.deinit();

    var out: metadata_openapi.AggregationBucket = .{
        .key = try jsonValueToBucketKeyAlloc(alloc, parsed_key.value),
        .doc_count = bucket.count,
        .score = if (bucket.score) |value| @floatCast(value) else null,
        .bg_count = bucket.bg_count,
    };

    switch (parsed_key.value) {
        .float, .integer => {
            out.key_as_string = out.key;
        },
        else => {},
    }

    if (request) |aggregation_request| {
        if (aggregation_request.ranges) |ranges| {
            if (idx < ranges.len) {
                out.from = if (ranges[idx].from) |value| @floatCast(value) else null;
                out.to = if (ranges[idx].to) |value| @floatCast(value) else null;
                if (ranges[idx].from) |value| out.from_as_string = try std.fmt.allocPrint(alloc, "{d}", .{value});
                if (ranges[idx].to) |value| out.to_as_string = try std.fmt.allocPrint(alloc, "{d}", .{value});
            }
        } else if (aggregation_request.date_ranges) |ranges| {
            if (idx < ranges.len) {
                out.from_as_string = if (ranges[idx].from) |value| try alloc.dupe(u8, value) else null;
                out.to_as_string = if (ranges[idx].to) |value| try alloc.dupe(u8, value) else null;
            }
        } else if (aggregation_request.distance_ranges) |ranges| {
            if (idx < ranges.len) {
                out.from = if (ranges[idx].from) |value| @floatCast(value) else null;
                out.to = if (ranges[idx].to) |value| @floatCast(value) else null;
                if (ranges[idx].from) |value| out.from_as_string = try std.fmt.allocPrint(alloc, "{d}", .{value});
                if (ranges[idx].to) |value| out.to_as_string = try std.fmt.allocPrint(alloc, "{d}", .{value});
            }
        }
    }

    if (bucket.aggregations.len > 0) {
        const sub_requests = if (request) |aggregation_request|
            aggregation_request.sub_aggregations
        else
            null;
        var sub_out: std.json.ArrayHashMap(metadata_openapi.AggregationResult) = .{};
        errdefer sub_out.deinit(alloc);
        for (bucket.aggregations) |aggregation| {
            const sub_request = if (sub_requests) |requests| requests.map.get(aggregation.name) else null;
            try sub_out.map.put(alloc, aggregation.name, try toOpenApiAggregationResult(alloc, sub_request, aggregation));
        }
        out.sub_aggregations = sub_out;
    }

    return out;
}

fn jsonValueToBucketKeyAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]u8 {
    return switch (value) {
        .string => |text| try alloc.dupe(u8, text),
        .integer => |number| try std.fmt.allocPrint(alloc, "{d}", .{number}),
        .float => |number| try std.fmt.allocPrint(alloc, "{d}", .{number}),
        else => try jsonStringifyAlloc(alloc, value),
    };
}

fn jsonValueToF32(value: std.json.Value) !?f32 {
    return switch (value) {
        .null => null,
        .integer => @floatFromInt(value.integer),
        .float => @floatCast(value.float),
        else => error.InvalidQueryRequest,
    };
}

fn jsonValueToI64(value: std.json.Value) !?i64 {
    return switch (value) {
        .null => null,
        .integer => value.integer,
        .float => @intFromFloat(value.float),
        else => error.InvalidQueryRequest,
    };
}

fn buildGraphQueryResults(
    comptime Result: type,
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
    meta: QueryResponseMeta,
    result: db_mod.types.SearchResult,
    response_format: GraphResponseFormat,
) !std.json.ArrayHashMap(Result) {
    comptime {
        if (Result != indexes_openapi.GraphResult and Result != indexes_openapi.StatefulGraphResult)
            @compileError("graph result map must use a canonical or stateful transport result");
    }
    var out: std.json.ArrayHashMap(Result) = .{};
    errdefer out.deinit(alloc);

    for (req.graph_queries) |requested| {
        var occurrences: usize = 0;
        for (result.graph_results) |graph_result| {
            if (std.mem.eql(u8, requested.name, graph_result.name)) occurrences += 1;
        }
        if (occurrences != 1) return error.InvalidRemoteResponse;
    }

    for (result.graph_results) |graph_result| {
        const graph_query = findGraphQuery(req.graph_queries, graph_result.name) orelse
            return error.InvalidRemoteResponse;
        const converted: Result = (if (Result == indexes_openapi.GraphResult)
            toOpenApiGraphQueryResult(alloc, graph_query.query, meta, graph_result)
        else
            toOpenApiStatefulGraphResultWithFormat(
                alloc,
                graph_query.query,
                meta,
                graph_result,
                response_format,
            )) catch |err| {
            if (graph_path_weight_diagnostic.isDomainError(err)) {
                graph_path_weight_diagnostic.record(graph_query.name, graph_query.query, err);
            }
            return err;
        };
        try out.map.put(alloc, graph_result.name, converted);
    }
    return out;
}

fn findGraphQueryIncludePaths(
    graph_queries: []const db_mod.types.NamedGraphQuery,
    name: []const u8,
) bool {
    for (graph_queries) |graph_query| {
        if (std.mem.eql(u8, graph_query.name, name))
            return graph_query.query.params.include_paths;
    }
    return false;
}

fn findGraphQuery(
    graph_queries: []const db_mod.types.NamedGraphQuery,
    name: []const u8,
) ?db_mod.types.NamedGraphQuery {
    for (graph_queries) |graph_query| {
        if (std.mem.eql(u8, graph_query.name, name)) return graph_query;
    }
    return null;
}

const GraphDocumentLookup = struct {
    const Entry = struct { document: ?std.json.ArrayHashMap(std.json.Value) };

    enabled: bool,
    remaining_bindings: usize = public_limits.max_graph_hydrated_bindings,
    entries: graph_node_identity.Map(Entry) = .{},

    fn init(
        alloc: std.mem.Allocator,
        hits: []const db_mod.types.SearchHit,
        enabled: bool,
    ) !GraphDocumentLookup {
        var self = GraphDocumentLookup{ .enabled = enabled };
        errdefer self.deinit(alloc);
        if (!enabled) return self;

        for (hits) |hit| {
            const identity = graph_node_identity.Ref{
                .table = hit.source_table,
                .key = hit.id,
            };
            if (self.entries.contains(identity)) continue;
            const parsed_document = if (hit.stored_data) |stored_data|
                ant_json.parseFromSliceLeaky(std.json.ArrayHashMap(std.json.Value), alloc, stored_data, .{}) catch
                    return error.InvalidRemoteResponse
            else
                null;
            _ = try self.entries.putIfAbsent(alloc, identity, .{ .document = parsed_document });
        }
        return self;
    }

    fn document(
        self: *GraphDocumentLookup,
        key: []const u8,
        table: ?[]const u8,
    ) !?std.json.ArrayHashMap(std.json.Value) {
        if (!self.enabled) return null;
        if (self.remaining_bindings == 0) return error.QueryCandidateBudgetExceeded;
        self.remaining_bindings -= 1;
        const entry = self.entries.get(.{ .table = table, .key = key }) orelse return null;
        return entry.document;
    }

    fn deinit(self: *GraphDocumentLookup, alloc: std.mem.Allocator) void {
        self.entries.deinit(alloc);
        self.* = undefined;
    }
};

fn toOpenApiGraphQueryResult(
    alloc: std.mem.Allocator,
    query: graph_query_mod.GraphQuery,
    meta: QueryResponseMeta,
    graph_result: db_mod.types.GraphSearchResult,
) !indexes_openapi.GraphResult {
    const result = try toOpenApiStatefulGraphResultWithFormat(alloc, query, meta, graph_result, .canonical);
    return switch (result) {
        .graph_nodes_result => |value| .{ .graph_nodes_result = value.* },
        .graph_paths_result => |value| .{ .graph_paths_result = value.* },
        .graph_aggregates_result => |value| .{ .graph_aggregates_result = value.* },
        .graph_bindings_result => |value| .{ .graph_bindings_result = value.* },
        .legacy_graph_search_result => unreachable,
    };
}

fn toOpenApiStatefulGraphResultWithFormat(
    alloc: std.mem.Allocator,
    query: graph_query_mod.GraphQuery,
    meta: QueryResponseMeta,
    graph_result: db_mod.types.GraphSearchResult,
    response_format: GraphResponseFormat,
) !indexes_openapi.StatefulGraphResult {
    var document_lookup = try GraphDocumentLookup.init(
        alloc,
        graph_result.hits,
        query.include_documents,
    );
    defer document_lookup.deinit(alloc);

    if (response_format == .legacy) {
        const response = try alloc.create(indexes_openapi.LegacyGraphSearchResult);
        errdefer alloc.destroy(response);
        response.* = .{
            .type = legacyGraphQueryType(query.query_type),
            .nodes = try toOpenApiLegacyGraphNodes(alloc, graph_result, &document_lookup),
            .paths = try toOpenApiPaths(alloc, graph_result.paths),
            .matches = try toOpenApiLegacyPatternMatches(
                alloc,
                graph_result,
                query.params.include_paths,
                &document_lookup,
            ),
            .total = @intCast(graph_result.total_hits),
            .took = meta.took_ms,
        };
        return .{ .legacy_graph_search_result = response };
    }
    if (query.match_pattern != null and query.aggregates.len == 0) {
        const return_limit: usize = @intCast(if (query.return_limit == 0) 100 else query.return_limit);
        if (graph_result.matches.len > return_limit)
            return error.InvalidRemoteResponse;
        const rows = try toOpenApiGraphRows(
            alloc,
            graph_result,
            query.return_aliases,
            &document_lookup,
        );
        const response = try alloc.create(indexes_openapi.GraphBindingsResult);
        errdefer alloc.destroy(response);
        response.* = .{
            .kind = "bindings",
            .rows = rows,
            .stats = .{
                .returned_items = @intCast(rows.len),
                .truncated = graph_result.truncated,
            },
        };
        return .{ .graph_bindings_result = response };
    }
    if (query.aggregates.len > 0) {
        // Canonical aggregate values are exact by contract. A producer that
        // stopped early cannot attach `exact: true` values to a truncated
        // result, even if every individual accumulator claims exactness.
        if (graph_result.truncated) return error.QueryCandidateBudgetExceeded;
        const aggregates = try toOpenApiGraphAggregates(alloc, query, graph_result);
        const response = try alloc.create(indexes_openapi.GraphAggregatesResult);
        errdefer alloc.destroy(response);
        response.* = .{
            .kind = "aggregates",
            .aggregates = aggregates,
            .stats = .{
                .returned_items = @intCast(aggregates.map.count()),
            },
        };
        return .{ .graph_aggregates_result = response };
    }

    const path_operation = switch (query.query_type) {
        .shortest_path, .k_shortest_paths => true,
        .traverse => false,
        .neighbors, .pattern => return error.InvalidRemoteResponse,
    };
    const result_limit: usize = if (path_operation)
        @intCast(if (query.query_type == .shortest_path) 1 else query.k)
    else
        @intCast(query.params.max_results);
    if (graph_result.nodes.len > result_limit or graph_result.paths.len > result_limit)
        return error.InvalidRemoteResponse;
    if (!path_operation and graph_result.paths.len != 0)
        return error.InvalidRemoteResponse;
    if (path_operation and graph_result.truncated)
        return error.QueryCandidateBudgetExceeded;

    if (path_operation) {
        const paths = try toOpenApiGraphPaths(alloc, graph_result.paths, query.params.weight_mode);
        if (graph_result.nodes.len != paths.len) return error.InvalidRemoteResponse;
        const path_results = try alloc.alloc(indexes_openapi.GraphPathResult, paths.len);
        for (graph_result.nodes, paths, 0..) |node, path, i| {
            try validateCanonicalGraphResultNode(node);
            if (node.depth != path.length) return error.InvalidRemoteResponse;
            const terminal = path.nodes[path.nodes.len - 1];
            if (!GraphNodeIdentityContext.eql(.{}, .{ .key = node.key, .table = node.table }, .{ .key = terminal.key, .table = terminal.table }))
                return error.InvalidRemoteResponse;
            path_results[i] = .{
                .path = path,
                .document = try document_lookup.document(node.key, node.table),
            };
        }
        const response = try alloc.create(indexes_openapi.GraphPathsResult);
        errdefer alloc.destroy(response);
        response.* = .{
            .kind = "paths",
            .paths = path_results,
            .stats = .{
                .returned_items = @intCast(path_results.len),
            },
        };
        return .{ .graph_paths_result = response };
    }
    const nodes = try toOpenApiGraphNodes(
        alloc,
        graph_result,
        &document_lookup,
        query.params.include_paths,
    );
    const response = try alloc.create(indexes_openapi.GraphNodesResult);
    errdefer alloc.destroy(response);
    response.* = .{
        .kind = "nodes",
        .nodes = nodes,
        .stats = .{
            .returned_items = @intCast(nodes.len),
            .truncated = graph_result.truncated,
        },
    };
    return .{ .graph_nodes_result = response };
}

fn legacyGraphQueryType(query_type: graph_query_mod.QueryType) indexes_openapi.GraphQueryType {
    return switch (query_type) {
        .traverse => .traverse,
        .neighbors => .neighbors,
        .shortest_path => .shortest_path,
        .k_shortest_paths => .k_shortest_paths,
        .pattern => .pattern,
    };
}

fn toOpenApiLegacyPatternMatches(
    alloc: std.mem.Allocator,
    graph_result: db_mod.types.GraphSearchResult,
    include_paths: bool,
    document_lookup: *GraphDocumentLookup,
) !?[]const indexes_openapi.PatternMatch {
    if (graph_result.matches.len == 0) return null;
    const out = try alloc.alloc(indexes_openapi.PatternMatch, graph_result.matches.len);
    for (graph_result.matches, 0..) |match, i| {
        var bindings: std.json.ArrayHashMap(indexes_openapi.LegacyGraphResultNode) = .{};
        errdefer bindings.deinit(alloc);
        for (match.bindings) |binding| {
            try bindings.map.put(alloc, binding.alias, .{
                .key = binding.node.key,
                .table = binding.node.table,
                .depth = @intCast(binding.node.depth),
                .distance = binding.node.distance,
                .document = try document_lookup.document(
                    binding.node.key,
                    binding.node.table,
                ),
                .path = binding.node.path,
                .path_edges = try toOpenApiOptionalPathEdges(alloc, binding.node.path_edges),
                .provenance = binding.node.provenance,
                .evidence = try graphNodeEvidenceObjectMap(alloc, binding.node),
                .edges = null,
            });
        }
        out[i] = .{
            .bindings = bindings,
            .path = if (include_paths) try toOpenApiPathEdges(alloc, match.path) else null,
        };
    }
    return out;
}

fn toOpenApiGraphRows(
    alloc: std.mem.Allocator,
    graph_result: db_mod.types.GraphSearchResult,
    expected_aliases: []const []const u8,
    document_lookup: *GraphDocumentLookup,
) ![]const indexes_openapi.GraphResultRow {
    if (graph_result.matches.len > public_limits.max_graph_result_items)
        return error.InvalidRemoteResponse;
    if (expected_aliases.len == 0 or expected_aliases.len > graph_pattern_mod.max_pattern_steps)
        return error.InvalidRemoteResponse;

    // Build the projection set once per operation. Matching every cell against
    // this bounded set keeps validation linear in the response size while
    // preventing malformed workers from silently changing the row shape.
    var expected = std.StringHashMapUnmanaged(void).empty;
    defer expected.deinit(alloc);
    try expected.ensureTotalCapacity(alloc, @intCast(expected_aliases.len));
    for (expected_aliases) |alias| {
        if (!graph_query_mod.isValidIdentifier(alias) or expected.contains(alias))
            return error.InvalidRemoteResponse;
        expected.putAssumeCapacity(alias, {});
    }

    const out = try alloc.alloc(indexes_openapi.GraphResultRow, graph_result.matches.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*row| row.deinit(alloc);
        alloc.free(out);
    }
    for (graph_result.matches, 0..) |match, i| {
        var row: indexes_openapi.GraphResultRow = .{};
        errdefer row.deinit(alloc);
        const cell_count = std.math.add(usize, match.bindings.len, match.null_aliases.len) catch
            return error.InvalidRemoteResponse;
        if (cell_count != expected_aliases.len) return error.InvalidRemoteResponse;
        for (match.bindings) |binding| {
            if (!expected.contains(binding.alias) or row.map.contains(binding.alias) or
                binding.node.key.len == 0 or
                (binding.node.table != null and binding.node.table.?.len == 0))
                return error.InvalidRemoteResponse;
            const node = indexes_openapi.GraphBindingNode{
                .key = binding.node.key,
                .table = binding.node.table,
                .document = try document_lookup.document(
                    binding.node.key,
                    binding.node.table,
                ),
            };
            try row.map.put(alloc, binding.alias, node);
        }
        for (match.null_aliases) |alias| {
            if (!expected.contains(alias) or row.map.contains(alias))
                return error.InvalidRemoteResponse;
            try row.map.put(alloc, alias, null);
        }
        out[i] = row;
        initialized += 1;
    }
    return out;
}

fn toOpenApiGraphAggregates(
    alloc: std.mem.Allocator,
    query: graph_query_mod.GraphQuery,
    graph_result: db_mod.types.GraphSearchResult,
) !std.json.ArrayHashMap(indexes_openapi.GraphAggregateValue) {
    var out: std.json.ArrayHashMap(indexes_openapi.GraphAggregateValue) = .{};
    errdefer out.deinit(alloc);
    if (graph_result.aggregates.len != query.aggregates.len) return error.InvalidRemoteResponse;
    for (query.aggregates) |requested| {
        var found: ?db_mod.types.GraphAggregateResult = null;
        for (graph_result.aggregates) |aggregate| {
            if (!std.mem.eql(u8, requested.name, aggregate.name)) continue;
            if (found != null) return error.InvalidRemoteResponse;
            found = aggregate;
        }
        const aggregate = found orelse return error.InvalidRemoteResponse;
        if (!aggregate.exact) return error.QueryCandidateBudgetExceeded;
        try out.map.put(alloc, aggregate.name, .{
            .value = try std.fmt.allocPrint(alloc, "{d}", .{aggregate.value}),
            .exact = true,
        });
    }
    return out;
}

test "pattern response omits paths unless requested" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    const bindings = [_]db_mod.types.GraphPatternBinding{.{
        .alias = @constCast("node"),
        .node = .{
            .key = @constCast("doc:a"),
            .depth = 7,
            .distance = 42,
            .path = null,
            .path_edges = null,
        },
    }};
    const path = [_]graph_query_mod.PathEdgeInfo{.{
        .source = @constCast("doc:a"),
        .target = @constCast("doc:b"),
        .edge_type = @constCast("links"),
        .weight = 1,
        .metadata = "",
    }};
    const matches = [_]db_mod.types.GraphPatternMatch{.{
        .bindings = @constCast(bindings[0..]),
        .path = @constCast(path[0..]),
    }};
    const graph_result = db_mod.types.GraphSearchResult{
        .name = @constCast("pattern"),
        .matches = @constCast(matches[0..]),
        .hits = &.{},
        .total_hits = 1,
    };

    var document_lookup = try GraphDocumentLookup.init(alloc, graph_result.hits, false);
    defer document_lookup.deinit(alloc);
    const rows = try toOpenApiGraphRows(alloc, graph_result, &.{"node"}, &document_lookup);
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expect(rows[0].map.get("node") != null);

    const encoded = try jsonStringifyAlloc(alloc, rows);
    var parsed = try ant_json.parseFromSlice(std.json.Value, alloc, encoded, .{});
    defer parsed.deinit();
    const node = parsed.value.array.items[0].object.get("node").?.object;
    try std.testing.expectEqualStrings("doc:a", node.get("key").?.string);
    try std.testing.expect(node.get("depth") == null);
    try std.testing.expect(node.get("distance") == null);
    try std.testing.expect(node.get("path") == null);
}

fn expectInvalidCanonicalGraphRow(
    alloc: std.mem.Allocator,
    expected_aliases: []const []const u8,
    bindings: []const db_mod.types.GraphPatternBinding,
    null_aliases: []const []u8,
) !void {
    const matches = [_]db_mod.types.GraphPatternMatch{.{
        .bindings = @constCast(bindings),
        .path = &.{},
        .null_aliases = @constCast(null_aliases),
    }};
    const graph_result = db_mod.types.GraphSearchResult{
        .name = @constCast("pattern"),
        .matches = @constCast(matches[0..]),
        .hits = &.{},
        .total_hits = 1,
    };
    var document_lookup = try GraphDocumentLookup.init(alloc, graph_result.hits, false);
    defer document_lookup.deinit(alloc);
    try std.testing.expectError(
        error.InvalidRemoteResponse,
        toOpenApiGraphRows(alloc, graph_result, expected_aliases, &document_lookup),
    );
}

test "canonical graph binding responses require exact projected alias sets" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    const node = graph_query_mod.GraphResultNode{
        .key = @constCast("doc:a"),
        .depth = 0,
        .distance = 0,
    };

    const missing = [_]db_mod.types.GraphPatternBinding{
        .{ .alias = @constCast("a"), .node = node },
    };
    try expectInvalidCanonicalGraphRow(alloc, &.{ "a", "b" }, &missing, &.{});

    const unexpected = [_]db_mod.types.GraphPatternBinding{
        .{ .alias = @constCast("a"), .node = node },
        .{ .alias = @constCast("c"), .node = node },
    };
    try expectInvalidCanonicalGraphRow(alloc, &.{ "a", "b" }, &unexpected, &.{});

    const duplicate = [_]db_mod.types.GraphPatternBinding{
        .{ .alias = @constCast("a"), .node = node },
        .{ .alias = @constCast("a"), .node = node },
    };
    try expectInvalidCanonicalGraphRow(alloc, &.{ "a", "b" }, &duplicate, &.{});

    const overlap = [_]db_mod.types.GraphPatternBinding{
        .{ .alias = @constCast("a"), .node = node },
    };
    const null_aliases = [_][]u8{@constCast("a")};
    try expectInvalidCanonicalGraphRow(alloc, &.{ "a", "b" }, &overlap, &null_aliases);

    try expectInvalidCanonicalGraphRow(alloc, &.{"a"}, &.{}, &.{});
}

test "graph aggregate response preserves exact decimal counts" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    const requested = [_]graph_query_mod.NamedCountAggregate{.{
        .name = "count",
        .of = "*",
    }};
    const computed = [_]db_mod.types.GraphAggregateResult{.{
        .name = @constCast("count"),
        .value = 9_384_729_384_729_384,
        .exact = true,
    }};
    const result = try toOpenApiGraphQueryResult(
        alloc,
        .{
            .query_type = .pattern,
            .index_name = "graph_idx",
            .start_nodes = .{ .keys = &.{} },
            .match_pattern = .{ .nodes = &.{}, .edges = &.{} },
            .aggregates = &requested,
        },
        .{},
        .{
            .name = @constCast("counted"),
            .aggregates = @constCast(computed[0..]),
            .hits = &.{},
            .total_hits = 0,
        },
    );
    try std.testing.expect(result == .graph_aggregates_result);
    const aggregate_result = result.graph_aggregates_result;
    const aggregates = aggregate_result.aggregates;
    const count = aggregates.map.get("count") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("9384729384729384", count.value);
    try std.testing.expect(count.exact);
    try std.testing.expectEqual(@as(i64, 1), aggregate_result.stats.returned_items);
}

test "graph aggregate response fails closed on missing or inexact results" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    const requested = [_]graph_query_mod.NamedCountAggregate{.{ .name = "count", .of = "*" }};
    const query = graph_query_mod.GraphQuery{
        .query_type = .pattern,
        .index_name = "graph_idx",
        .start_nodes = .{ .keys = &.{} },
        .match_pattern = .{ .nodes = &.{}, .edges = &.{} },
        .aggregates = &requested,
    };
    const named_queries = [_]db_mod.types.NamedGraphQuery{.{ .name = "counted", .query = query }};
    try std.testing.expectError(error.InvalidRemoteResponse, buildGraphQueryResults(
        indexes_openapi.GraphResult,
        alloc,
        .{ .graph_queries = &named_queries },
        .{},
        .{ .alloc = alloc, .hits = &.{}, .total_hits = 0, .graph_results = &.{} },
        .canonical,
    ));
    try std.testing.expectError(error.InvalidRemoteResponse, toOpenApiGraphQueryResult(
        alloc,
        query,
        .{},
        .{ .name = @constCast("counted"), .hits = &.{}, .total_hits = 0 },
    ));

    const partial = [_]db_mod.types.GraphAggregateResult{.{
        .name = @constCast("count"),
        .value = 1,
        .exact = false,
    }};
    try std.testing.expectError(error.QueryCandidateBudgetExceeded, toOpenApiGraphQueryResult(
        alloc,
        query,
        .{},
        .{
            .name = @constCast("counted"),
            .aggregates = @constCast(partial[0..]),
            .hits = &.{},
            .total_hits = 0,
        },
    ));
    const complete_aggregate = [_]db_mod.types.GraphAggregateResult{.{
        .name = @constCast("count"),
        .value = 1,
        .exact = true,
    }};
    try std.testing.expectError(error.QueryCandidateBudgetExceeded, toOpenApiGraphQueryResult(
        alloc,
        query,
        .{},
        .{
            .name = @constCast("counted"),
            .aggregates = @constCast(complete_aggregate[0..]),
            .hits = &.{},
            .total_hits = 0,
            .truncated = true,
        },
    ));
}

test "graph response encoding requires exactly one result per traversal operation" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    const named_queries = [_]db_mod.types.NamedGraphQuery{.{
        .name = "walk",
        .query = .{
            .query_type = .traverse,
            .index_name = "graph_idx",
            .start_nodes = .{ .keys = &.{"doc:a"} },
        },
    }};

    try std.testing.expectError(error.InvalidRemoteResponse, encodeQueryResponses(
        alloc,
        "docs",
        .{ .graph_queries = &named_queries },
        .{},
        .{ .alloc = alloc, .hits = &.{}, .total_hits = 0, .graph_results = &.{} },
    ));

    const unknown_results = [_]db_mod.types.GraphSearchResult{.{
        .name = @constCast("other"),
        .hits = &.{},
        .total_hits = 0,
    }};
    try std.testing.expectError(error.InvalidRemoteResponse, buildGraphQueryResults(
        indexes_openapi.GraphResult,
        alloc,
        .{ .graph_queries = &named_queries },
        .{},
        .{
            .alloc = alloc,
            .hits = &.{},
            .total_hits = 0,
            .graph_results = @constCast(unknown_results[0..]),
        },
        .canonical,
    ));
}

test "canonical path responses require one terminal node per path" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    var nodes = [_]graph_query_mod.GraphResultNode{
        .{ .key = "a", .depth = 0, .distance = 0 },
        .{ .key = "extra", .depth = 0, .distance = 0 },
    };
    var path_nodes = [_][]const u8{"a"};
    var paths = [_]graph_paths_mod.Path{.{
        .nodes = &path_nodes,
        .edges = &.{},
        .total_weight = 0,
        .length = 0,
    }};
    try std.testing.expectError(error.InvalidRemoteResponse, toOpenApiGraphQueryResult(
        alloc,
        .{
            .query_type = .shortest_path,
            .index_name = "graph_idx",
            .start_nodes = .{ .keys = &.{"a"} },
            .target_nodes = .{ .keys = &.{"a"} },
        },
        .{},
        .{
            .name = @constCast("path"),
            .nodes = &nodes,
            .paths = &paths,
            .hits = &.{},
            .total_hits = 1,
        },
    ));

    try std.testing.expectError(error.InvalidRemoteResponse, toOpenApiGraphQueryResult(
        alloc,
        .{
            .query_type = .shortest_path,
            .index_name = "graph_idx",
            .start_nodes = .{ .keys = &.{"a"} },
            .target_nodes = .{ .keys = &.{"a"} },
        },
        .{},
        .{
            .name = @constCast("path"),
            .nodes = nodes[0..1],
            .paths = &.{},
            .hits = &.{},
            .total_hits = 1,
        },
    ));
    try std.testing.expectError(error.QueryCandidateBudgetExceeded, toOpenApiGraphQueryResult(
        alloc,
        .{
            .query_type = .shortest_path,
            .index_name = "graph_idx",
            .start_nodes = .{ .keys = &.{"a"} },
            .target_nodes = .{ .keys = &.{"a"} },
        },
        .{},
        .{
            .name = @constCast("path"),
            .nodes = nodes[0..1],
            .paths = &paths,
            .hits = &.{},
            .total_hits = 1,
            .truncated = true,
        },
    ));

    nodes[0].depth = 1;
    try std.testing.expectError(error.InvalidRemoteResponse, toOpenApiGraphQueryResult(
        alloc,
        .{
            .query_type = .shortest_path,
            .index_name = "graph_idx",
            .start_nodes = .{ .keys = &.{"a"} },
            .target_nodes = .{ .keys = &.{"a"} },
        },
        .{},
        .{
            .name = @constCast("path"),
            .nodes = nodes[0..1],
            .paths = &paths,
            .hits = &.{},
            .total_hits = 1,
        },
    ));

    nodes = .{
        .{ .key = "a", .table = "entities", .depth = 0, .distance = 0 },
        .{ .key = "unused", .depth = 0, .distance = 0 },
    };
    try std.testing.expectError(error.InvalidRemoteResponse, toOpenApiGraphQueryResult(
        alloc,
        .{
            .query_type = .shortest_path,
            .index_name = "graph_idx",
            .start_nodes = .{ .keys = &.{"a"} },
            .target_nodes = .{ .keys = &.{"a"} },
        },
        .{},
        .{
            .name = @constCast("path"),
            .nodes = nodes[0..1],
            .paths = &paths,
            .hits = &.{},
            .total_hits = 1,
        },
    ));
}

test "canonical traversal responses keep paths on bounded result nodes" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    var node_path = [_][]const u8{"a"};
    var nodes = [_]graph_query_mod.GraphResultNode{
        .{ .key = "a", .depth = 0, .distance = 0, .path = &node_path },
        .{ .key = "extra", .depth = 0, .distance = 0 },
    };
    const query = graph_query_mod.GraphQuery{
        .query_type = .traverse,
        .index_name = "graph_idx",
        .start_nodes = .{ .keys = &.{"a"} },
        .params = .{ .max_results = 1, .include_paths = false },
    };
    try std.testing.expectError(error.InvalidRemoteResponse, toOpenApiGraphQueryResult(
        alloc,
        query,
        .{},
        .{
            .name = @constCast("walk"),
            .nodes = &nodes,
            .hits = &.{},
            .total_hits = 2,
        },
    ));

    const response = try toOpenApiGraphQueryResult(
        alloc,
        query,
        .{},
        .{
            .name = @constCast("walk"),
            .nodes = nodes[0..1],
            .hits = &.{},
            .total_hits = 1,
        },
    );
    try std.testing.expect(response.graph_nodes_result.nodes[0].path == null);

    var query_with_paths = query;
    query_with_paths.params.include_paths = true;
    const response_with_paths = try toOpenApiGraphQueryResult(
        alloc,
        query_with_paths,
        .{},
        .{
            .name = @constCast("walk"),
            .nodes = nodes[0..1],
            .hits = &.{},
            .total_hits = 1,
        },
    );
    try std.testing.expect(response_with_paths.graph_nodes_result.nodes[0].path != null);
    nodes[0].path = null;
    try std.testing.expectError(error.InvalidRemoteResponse, toOpenApiGraphQueryResult(
        alloc,
        query_with_paths,
        .{},
        .{
            .name = @constCast("walk"),
            .nodes = nodes[0..1],
            .hits = &.{},
            .total_hits = 1,
        },
    ));
    nodes[0].path = &node_path;

    var graph_paths = [_]graph_paths_mod.Path{.{
        .nodes = &node_path,
        .edges = &.{},
        .total_weight = 0,
        .length = 0,
    }};
    try std.testing.expectError(error.InvalidRemoteResponse, toOpenApiGraphQueryResult(
        alloc,
        query,
        .{},
        .{
            .name = @constCast("walk"),
            .nodes = nodes[0..1],
            .paths = &graph_paths,
            .hits = &.{},
            .total_hits = 1,
        },
    ));
}

test "deprecated graph search preserves its response envelope" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    const result = try toOpenApiStatefulGraphResultWithFormat(
        alloc,
        .{
            .query_type = .neighbors,
            .index_name = "graph_idx",
            .start_nodes = .{ .keys = &.{"doc:a"} },
        },
        .{ .took_ms = 3 },
        .{
            .name = @constCast("neighbors"),
            .hits = &.{},
            .total_hits = 12,
        },
        .legacy,
    );

    try std.testing.expect(result == .legacy_graph_search_result);
    const legacy = result.legacy_graph_search_result;
    try std.testing.expect(legacy.kind == null);
    try std.testing.expectEqual(indexes_openapi.GraphQueryType.neighbors, legacy.type);
    try std.testing.expectEqual(@as(i64, 12), legacy.total);
    try std.testing.expectEqual(@as(?i64, 3), legacy.took);

    const named_queries = [_]db_mod.types.NamedGraphQuery{.{
        .name = "neighbors",
        .query = .{
            .query_type = .neighbors,
            .index_name = "graph_idx",
            .start_nodes = .{ .keys = &.{"doc:a"} },
        },
    }};
    const graph_results = [_]db_mod.types.GraphSearchResult{.{
        .name = @constCast("neighbors"),
        .hits = &.{},
        .total_hits = 12,
    }};
    var encoded = try encodeQueryResponses(
        alloc,
        "docs",
        .{
            .graph_queries = &named_queries,
            .graph_query_transport = .{
                .dialect = .legacy,
                .operations_json =
                \\{"neighbors":{"type":"neighbors"}}
                ,
                .admitted_operations_ptr = @ptrCast(named_queries[0..].ptr),
                .admitted_operations_len = named_queries.len,
            },
        },
        .{ .took_ms = 3 },
        .{
            .alloc = alloc,
            .hits = &.{},
            .total_hits = 0,
            .graph_results = @constCast(graph_results[0..]),
        },
    );
    defer encoded.deinit(alloc);
    try std.testing.expectEqual(GraphResponseFormat.legacy, encoded.graph_dialect.?);

    var parsed = try ant_json.parseFromSlice(std.json.Value, alloc, encoded.json, .{});
    defer parsed.deinit();
    const encoded_legacy = parsed.value.object
        .get("responses").?.array.items[0].object
        .get("graph_results").?.object
        .get("neighbors").?;
    try std.testing.expect(encoded_legacy.object.get("kind") == null);

    // Model the strict v0.2 generated response type. Its decoder rejects
    // unknown fields, so this is the compatibility direction that merely
    // accepting pre-discriminator responses in new clients does not cover.
    const LegacyGraphSearchResultV02 = struct {
        type: indexes_openapi.GraphQueryType,
        nodes: ?std.json.Value = null,
        paths: ?std.json.Value = null,
        matches: ?std.json.Value = null,
        total: i64,
        took: ?i64 = null,
    };
    const encoded_legacy_json = try std.json.Stringify.valueAlloc(alloc, encoded_legacy, .{});
    defer alloc.free(encoded_legacy_json);
    var parsed_v02 = try std.json.parseFromSlice(LegacyGraphSearchResultV02, alloc, encoded_legacy_json, .{});
    defer parsed_v02.deinit();
    try std.testing.expectEqual(indexes_openapi.GraphQueryType.neighbors, parsed_v02.value.type);
    try std.testing.expectEqual(@as(i64, 12), parsed_v02.value.total);
}

test "canonical graph paths preserve table-qualified node identities" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    var path_nodes = [_][]const u8{ "doc:a", "shared" };
    var node_tables = [_]?[]const u8{ null, "entities" };
    var path_edges = [_]graph_paths_mod.PathEdge{.{
        .source = "doc:a",
        .target = "shared",
        .edge_type = "mentions",
        .weight = 1,
    }};
    var paths = [_]db_mod.types.GraphPath{.{
        .nodes = &path_nodes,
        .node_tables = &node_tables,
        .edges = &path_edges,
        .total_weight = 1,
        .length = 1,
    }};
    var result_node_path_edges = [_]graph_query_mod.PathEdgeInfo{.{
        .source = "doc:a",
        .target = "shared",
        .edge_type = "mentions",
        .weight = 1,
    }};
    var result_nodes = [_]graph_query_mod.GraphResultNode{.{
        .key = "shared",
        .table = "entities",
        .depth = 1,
        .distance = 1,
        .path = &path_nodes,
        .path_tables = &node_tables,
        .path_edges = &result_node_path_edges,
    }};
    const query = graph_query_mod.GraphQuery{
        .query_type = .shortest_path,
        .index_name = "graph_idx",
        .start_nodes = .{ .keys = &.{"doc:a"} },
        .target_nodes = .{ .identities = &.{.{ .key = "shared", .table = "entities" }} },
    };
    const graph_result = db_mod.types.GraphSearchResult{
        .name = @constCast("path"),
        .nodes = &result_nodes,
        .paths = &paths,
        .hits = &.{},
        .total_hits = 1,
    };

    const canonical = try toOpenApiGraphQueryResult(alloc, query, .{}, graph_result);
    try std.testing.expect(canonical == .graph_paths_result);
    const canonical_path = canonical.graph_paths_result.paths[0].path;
    try std.testing.expectEqualStrings("shared", canonical_path.nodes[1].key);
    try std.testing.expectEqualStrings("entities", canonical_path.nodes[1].table.?);
    try std.testing.expectEqualStrings("doc:a", canonical_path.edges[0].from.key);
    try std.testing.expectEqualStrings("shared", canonical_path.edges[0].to.key);
    try std.testing.expectEqualStrings("entities", canonical_path.edges[0].to.table.?);
    try std.testing.expectEqual(indexes_openapi.GraphPathObjective.min_hops, canonical_path.objective);
    try std.testing.expectEqual(@as(f64, 1), canonical_path.weight_sum);
    try std.testing.expectEqual(@as(f64, 1), canonical_path.objective_value);

    const legacy = try toOpenApiStatefulGraphResultWithFormat(alloc, query, .{}, graph_result, .legacy);
    try std.testing.expect(legacy == .legacy_graph_search_result);
    try std.testing.expectEqualStrings("shared", legacy.legacy_graph_search_result.paths.?[0].nodes.?[1]);
}

test "canonical graph path objective exposes max weight product" {
    const edges = [_]graph_paths_mod.PathEdge{
        .{ .source = "a", .target = "b", .edge_type = "e", .weight = 0.8 },
        .{ .source = "b", .target = "c", .edge_type = "e", .weight = 0.5 },
    };
    var nodes = [_][]const u8{ "a", "b", "c" };
    const path = db_mod.types.GraphPath{
        .nodes = &nodes,
        .edges = @constCast(edges[0..]),
        .total_weight = 1.3,
        .length = 2,
    };
    try std.testing.expectApproxEqAbs(@as(f64, 0.4), graphPathObjectiveValue(path, .max_weight), 0.000001);
}

test "generated stateful graph result union decodes pre-discriminator legacy responses" {
    const raw =
        \\{"type":"neighbors","total":12}
    ;

    var stdlib_parsed = try std.json.parseFromSlice(
        indexes_openapi.StatefulGraphResult,
        std.testing.allocator,
        raw,
        .{},
    );
    defer stdlib_parsed.deinit();
    try std.testing.expect(stdlib_parsed.value == .legacy_graph_search_result);
    try std.testing.expectEqual(@as(i64, 12), stdlib_parsed.value.legacy_graph_search_result.total);

    var simd_parsed = try ant_json.parseFromSlice(
        indexes_openapi.StatefulGraphResult,
        std.testing.allocator,
        raw,
        .{},
    );
    defer simd_parsed.deinit();
    try std.testing.expect(simd_parsed.value == .legacy_graph_search_result);
    try std.testing.expectEqual(@as(i64, 12), simd_parsed.value.legacy_graph_search_result.total);
}

test "generated stateful graph result union rejects an explicit null discriminator" {
    const raw =
        \\{"kind":null,"type":"neighbors","total":12}
    ;

    try std.testing.expectError(error.UnexpectedToken, std.json.parseFromSlice(
        indexes_openapi.StatefulGraphResult,
        std.testing.allocator,
        raw,
        .{},
    ));
    try std.testing.expectError(error.UnexpectedToken, ant_json.parseFromSlice(
        indexes_openapi.StatefulGraphResult,
        std.testing.allocator,
        raw,
        .{},
    ));
}

fn toOpenApiGraphNodes(
    alloc: std.mem.Allocator,
    graph_result: db_mod.types.GraphSearchResult,
    document_lookup: *GraphDocumentLookup,
    include_paths: bool,
) ![]const indexes_openapi.GraphResultNode {
    if (graph_result.nodes.len > public_limits.max_graph_result_items)
        return error.InvalidRemoteResponse;
    const nodes = try alloc.alloc(indexes_openapi.GraphResultNode, graph_result.nodes.len);
    for (graph_result.nodes, 0..) |node, i| {
        try validateCanonicalGraphResultNode(node);
        if (include_paths and node.path == null) return error.InvalidRemoteResponse;
        nodes[i] = .{
            .key = node.key,
            .table = node.table,
            .depth = @intCast(node.depth),
            .document = try document_lookup.document(node.key, node.table),
            .path = if (include_paths) try toOpenApiGraphNodePath(alloc, node) else null,
            .path_edges = if (include_paths) try toOpenApiOptionalGraphPathEdges(alloc, node) else null,
            .provenance = node.provenance,
            .evidence = try graphNodeEvidenceObjectMap(alloc, node),
        };
    }
    return nodes;
}

fn validateCanonicalGraphResultNode(node: graph_query_mod.GraphResultNode) !void {
    if (!graph_query_mod.isCanonicalResultNode(node)) return error.InvalidRemoteResponse;
}

test "canonical graph result nodes fail closed outside the public contract" {
    try std.testing.expectError(error.InvalidRemoteResponse, validateCanonicalGraphResultNode(.{
        .key = @constCast(""),
        .depth = 0,
        .distance = 0,
    }));
    try std.testing.expectError(error.InvalidRemoteResponse, validateCanonicalGraphResultNode(.{
        .key = @constCast("node"),
        .depth = graph_pattern_mod.max_pattern_hops + 1,
        .distance = 0,
    }));
    try std.testing.expectError(error.InvalidRemoteResponse, validateCanonicalGraphResultNode(.{
        .key = @constCast("node"),
        .depth = 0,
        .distance = -0.1,
    }));
    try std.testing.expectError(error.InvalidRemoteResponse, validateCanonicalGraphResultNode(.{
        .key = @constCast("node"),
        .depth = 0,
        .distance = 0,
        .path = @constCast((&[_][]const u8{})[0..]),
    }));
    try std.testing.expectError(error.InvalidRemoteResponse, validateCanonicalGraphResultNode(.{
        .key = @constCast("node"),
        .depth = 0,
        .distance = 0,
        .path = &.{ "start", "node" },
    }));
    try std.testing.expectError(error.InvalidRemoteResponse, validateCanonicalGraphResultNode(.{
        .key = @constCast("wrong"),
        .depth = 1,
        .distance = 1,
        .path = &.{ "start", "node" },
    }));
    try std.testing.expectError(error.InvalidRemoteResponse, validateCanonicalGraphResultNode(.{
        .key = @constCast("node"),
        .depth = 0,
        .distance = 0,
        .path_edges = &.{},
    }));
}

fn toOpenApiLegacyGraphNodes(
    alloc: std.mem.Allocator,
    graph_result: db_mod.types.GraphSearchResult,
    document_lookup: *GraphDocumentLookup,
) ![]const indexes_openapi.LegacyGraphResultNode {
    const nodes = try alloc.alloc(indexes_openapi.LegacyGraphResultNode, graph_result.nodes.len);
    for (graph_result.nodes, 0..) |node, i| {
        nodes[i] = .{
            .key = node.key,
            .table = node.table,
            .depth = @intCast(node.depth),
            .distance = node.distance,
            .document = try document_lookup.document(node.key, node.table),
            .path = node.path,
            .path_edges = try toOpenApiOptionalPathEdges(alloc, node.path_edges),
            .provenance = node.provenance,
            .evidence = try graphNodeEvidenceObjectMap(alloc, node),
            .edges = null,
        };
    }
    return nodes;
}

fn toOpenApiGraphNodePath(
    alloc: std.mem.Allocator,
    node: graph_query_mod.GraphResultNode,
) !?[]const indexes_openapi.GraphPathEndpoint {
    const path = node.path orelse return null;
    const tables = node.path_tables;
    if (tables) |values| {
        if (values.len != path.len) return error.InvalidRemoteResponse;
    }
    const out = try alloc.alloc(indexes_openapi.GraphPathEndpoint, path.len);
    for (path, 0..) |key, i| {
        out[i] = .{
            .key = key,
            .table = if (tables) |values| values[i] else null,
        };
    }
    return out;
}

fn toOpenApiPaths(
    alloc: std.mem.Allocator,
    paths: []const db_mod.types.GraphPath,
) ![]const indexes_openapi.Path {
    const out = try alloc.alloc(indexes_openapi.Path, paths.len);
    for (paths, 0..) |path, i| {
        out[i] = .{
            .nodes = path.nodes,
            .edges = try toOpenApiPathEdges(alloc, path.edges),
            .total_weight = path.total_weight,
            .length = @intCast(path.length),
        };
    }
    return out;
}

fn toOpenApiGraphPaths(
    alloc: std.mem.Allocator,
    paths: []const db_mod.types.GraphPath,
    weight_mode: graph_paths_mod.PathWeightMode,
) ![]const indexes_openapi.GraphPath {
    if (paths.len > public_limits.max_graph_result_items)
        return error.InvalidRemoteResponse;
    const out = try alloc.alloc(indexes_openapi.GraphPath, paths.len);
    for (paths, 0..) |path, i| {
        if (@as(usize, path.length) != path.edges.len) return error.InvalidRemoteResponse;
        if (path.nodes.len == 0 or path.nodes.len > graph_pattern_mod.max_pattern_hops + 1)
            return error.InvalidRemoteResponse;
        if (path.node_tables.len != 0 and path.node_tables.len != path.nodes.len) {
            return error.InvalidRemoteResponse;
        }
        for (path.nodes, 0..) |key, node_index| {
            if (key.len == 0) return error.InvalidRemoteResponse;
            if (path.node_tables.len != 0) {
                if (path.node_tables[node_index]) |table| if (table.len == 0)
                    return error.InvalidRemoteResponse;
            }
        }
        for (path.edges) |edge| {
            graph_edge_weight.validateStored(edge.weight) catch return error.InvalidRemoteResponse;
            switch (weight_mode) {
                .min_hops => {},
                .min_weight => _ = graph_paths_mod.pathEdgeCost(.min_weight, edge.weight) catch
                    return error.InvalidRemoteResponse,
                .max_weight => _ = graph_paths_mod.pathEdgeCost(.max_weight, edge.weight) catch
                    return error.InvalidRemoteResponse,
            }
        }
        const weight_sum = try graph_paths_mod.sumPathEdgeWeights(path.edges);
        const objective_value: f64 = switch (weight_mode) {
            .min_hops => @floatFromInt(path.length),
            .min_weight => weight_sum,
            .max_weight => blk: {
                var product: f64 = 1.0;
                for (path.edges) |edge| {
                    product *= edge.weight;
                    if (!std.math.isFinite(product)) return error.GraphPathWeightOverflow;
                }
                break :blk product;
            },
        };
        const nodes = try alloc.alloc(indexes_openapi.GraphPathEndpoint, path.nodes.len);
        for (path.nodes, 0..) |key, node_index| {
            nodes[node_index] = .{
                .key = key,
                .table = if (path.node_tables.len == 0) null else path.node_tables[node_index],
            };
        }
        out[i] = .{
            .nodes = nodes,
            .edges = try toOpenApiGraphPathEdges(
                alloc,
                path.nodes,
                path.node_tables,
                path.edges,
            ),
            .length = @intCast(path.length),
            .objective = switch (weight_mode) {
                .min_hops => .min_hops,
                .min_weight => .min_weight_sum,
                .max_weight => .max_weight_product,
            },
            .weight_sum = weight_sum,
            .objective_value = objective_value,
        };
    }
    return out;
}

fn graphPathObjectiveValue(
    path: db_mod.types.GraphPath,
    weight_mode: graph_paths_mod.PathWeightMode,
) f64 {
    return switch (weight_mode) {
        .min_hops => @floatFromInt(path.length),
        .min_weight => path.total_weight,
        .max_weight => blk: {
            var product: f64 = 1.0;
            for (path.edges) |edge| product *= edge.weight;
            break :blk product;
        },
    };
}

fn toOpenApiPathEdges(
    alloc: std.mem.Allocator,
    edges: anytype,
) ![]const indexes_openapi.PathEdge {
    const out = try alloc.alloc(indexes_openapi.PathEdge, edges.len);
    for (edges, 0..) |edge, i| {
        out[i] = .{
            .source = edge.source,
            .target = edge.target,
            .type = edge.edge_type,
            .weight = edge.weight,
            .metadata = try pathEdgeMetadataObjectMap(alloc, edge.metadata),
        };
    }
    return out;
}

fn toOpenApiGraphPathEdges(
    alloc: std.mem.Allocator,
    nodes: []const []const u8,
    node_tables: []const ?[]const u8,
    edges: anytype,
) ![]const indexes_openapi.GraphPathEdge {
    if (node_tables.len != 0 and node_tables.len != nodes.len)
        return error.InvalidRemoteResponse;
    if (nodes.len == 0) {
        if (edges.len != 0) return error.InvalidRemoteResponse;
    } else if (edges.len != nodes.len - 1) {
        return error.InvalidRemoteResponse;
    }

    const out = try alloc.alloc(indexes_openapi.GraphPathEdge, edges.len);
    errdefer if (out.len > 0) alloc.free(out);
    for (edges, 0..) |edge, i| {
        graph_edge_type.validateStored(edge.edge_type) catch return error.InvalidRemoteResponse;
        graph_edge_weight.validateStored(edge.weight) catch return error.InvalidRemoteResponse;
        const left_key = nodes[i];
        const right_key = nodes[i + 1];
        const connects_in_order = std.mem.eql(u8, edge.source, left_key) and
            std.mem.eql(u8, edge.target, right_key);
        const connects_in_reverse = std.mem.eql(u8, edge.source, right_key) and
            std.mem.eql(u8, edge.target, left_key);
        if (!connects_in_order and !connects_in_reverse)
            return error.InvalidRemoteResponse;
        const direction: indexes_openapi.GraphPathEdgeDirection = if (edge.traversal_direction) |stored| switch (stored) {
            .out => if (connects_in_order) .out else return error.InvalidRemoteResponse,
            .in => if (connects_in_reverse) .in else return error.InvalidRemoteResponse,
            .both => return error.InvalidRemoteResponse,
        } else if (connects_in_order != connects_in_reverse)
            (if (connects_in_order) .out else .in)
        else
            try graphPathEdgeDirectionForEqualKeys(node_tables, i, edge.metadata);
        out[i] = .{
            .from = .{
                .key = left_key,
                .table = if (node_tables.len == 0) null else node_tables[i],
            },
            .to = .{
                .key = right_key,
                .table = if (node_tables.len == 0) null else node_tables[i + 1],
            },
            .direction = direction,
            .type = edge.edge_type,
            .weight = edge.weight,
            .metadata = try pathEdgeMetadataObjectMap(alloc, edge.metadata),
        };
    }
    return out;
}

fn graphPathEdgeDirectionForEqualKeys(
    node_tables: []const ?[]const u8,
    edge_index: usize,
    metadata: []const u8,
) !indexes_openapi.GraphPathEdgeDirection {
    // Equal keys are still distinct cross-table identities. The durable edge
    // metadata names its physical target table, which lets the public path
    // retain orientation even when comparing source/target keys cannot.
    if (node_tables.len != 0) {
        const left_table = node_tables[edge_index];
        const right_table = node_tables[edge_index + 1];
        if (!optionalStringEqual(left_table, right_table)) {
            const target_table = graph_traversal_mod.metadataTargetTable(metadata) orelse
                return error.InvalidRemoteResponse;
            var left_is_target = optionalStringEqualsValue(left_table, target_table);
            var right_is_target = optionalStringEqualsValue(right_table, target_table);
            // The queried table is encoded as null in public path identities.
            // If a cross-table edge names neither explicit side, its durable
            // target can only be that elided base-table identity.
            if (!left_is_target and !right_is_target and (left_table == null) != (right_table == null)) {
                left_is_target = left_table == null;
                right_is_target = right_table == null;
            }
            if (left_is_target == right_is_target) return error.InvalidRemoteResponse;
            return if (right_is_target) .out else .in;
        }
    }
    // A physical self-loop has no distinguishable reverse orientation and is
    // emitted once under `both`, so `out` is its stable canonical spelling.
    return .out;
}

fn optionalStringEqual(left: ?[]const u8, right: ?[]const u8) bool {
    if (left == null or right == null) return left == null and right == null;
    return std.mem.eql(u8, left.?, right.?);
}

fn optionalStringEqualsValue(optional: ?[]const u8, value: []const u8) bool {
    return optional != null and std.mem.eql(u8, optional.?, value);
}

test "canonical graph path edges enforce durable type policy" {
    const edges: []const graph_query_mod.PathEdgeInfo = &.{.{
        .source = "a",
        .target = "b",
        .edge_type = "x" ** (graph_edge_type.max_bytes + 1),
        .weight = 1,
    }};
    try std.testing.expectError(
        error.InvalidRemoteResponse,
        toOpenApiGraphPathEdges(std.testing.allocator, &.{ "a", "b" }, &.{}, edges),
    );

    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const alloc = arena_state.allocator();
    const nodes = [_][]const u8{ "shared", "shared" };
    const tables = [_]?[]const u8{ "authors", "entities" };
    const forward = [_]graph_paths_mod.PathEdge{.{
        .source = "shared",
        .target = "shared",
        .edge_type = "knows",
        .weight = 1,
        .traversal_direction = .out,
    }};
    const reverse = [_]graph_paths_mod.PathEdge{.{
        .source = "shared",
        .target = "shared",
        .edge_type = "knows",
        .weight = 1,
        .traversal_direction = .in,
    }};

    const encoded_forward = try toOpenApiGraphPathEdges(alloc, &nodes, &tables, &forward);
    const encoded_reverse = try toOpenApiGraphPathEdges(alloc, &nodes, &tables, &reverse);
    try std.testing.expectEqual(indexes_openapi.GraphPathEdgeDirection.out, encoded_forward[0].direction);
    try std.testing.expectEqual(indexes_openapi.GraphPathEdgeDirection.in, encoded_reverse[0].direction);
}

fn pathEdgeMetadataJsonValue(alloc: std.mem.Allocator, metadata: []const u8) !?std.json.Value {
    if (metadata.len == 0) return null;
    return std.json.parseFromSliceLeaky(std.json.Value, alloc, metadata, .{}) catch .{ .string = try alloc.dupe(u8, metadata) };
}

/// Canonical GraphPathEdge metadata is object-shaped. New writes enforce this
/// at ingestion, while old durable records may contain scalar or malformed
/// payloads accepted by earlier releases. Omit only that optional metadata on
/// canonical reads so legacy data cannot break generated response decoders.
fn pathEdgeMetadataObjectMap(
    alloc: std.mem.Allocator,
    metadata: []const u8,
) !?std.json.ArrayHashMap(std.json.Value) {
    if (metadata.len == 0) return null;
    const value = ant_json.parseFromSliceLeaky(std.json.Value, alloc, metadata, .{}) catch return null;
    return if (value == .object) .{ .map = value.object } else null;
}

test "canonical graph path metadata safely reads legacy non-object records" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const alloc = arena_state.allocator();

    const object = (try pathEdgeMetadataObjectMap(alloc, "{\"kind\":\"citation\"}")) orelse
        return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("citation", object.map.get("kind").?.string);
    try std.testing.expect((try pathEdgeMetadataObjectMap(alloc, "\"legacy\"")) == null);
    try std.testing.expect((try pathEdgeMetadataObjectMap(alloc, "{")) == null);

    const legacy = (try pathEdgeMetadataJsonValue(alloc, "\"legacy\"")) orelse
        return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("legacy", legacy.string);
}

fn graphNodeEvidenceObjectMap(
    alloc: std.mem.Allocator,
    node: graph_query_mod.GraphResultNode,
) !?std.json.ArrayHashMap(std.json.Value) {
    const has_provenance = if (node.provenance) |items| items.len > 0 else false;
    var has_edge_metadata = false;
    if (node.path_edges) |edges| {
        for (edges) |edge| {
            if (edge.metadata.len > 0) {
                has_edge_metadata = true;
                break;
            }
        }
    }
    if (!has_provenance and !has_edge_metadata) return null;

    var evidence = std.json.ObjectMap.empty;
    errdefer evidence.deinit(alloc);

    if (node.provenance) |items| {
        if (items.len > 0) {
            var provenance = std.json.Array.init(alloc);
            errdefer provenance.deinit();
            for (items) |item| try provenance.append(.{ .string = try alloc.dupe(u8, item) });
            try evidence.put(alloc, try alloc.dupe(u8, "provenance"), .{ .array = provenance });
        }
    }

    if (node.path_edges) |edges| {
        var edge_values = std.json.Array.init(alloc);
        errdefer edge_values.deinit();
        var mention_artifact_keys = std.json.Array.init(alloc);
        errdefer mention_artifact_keys.deinit();
        var mention_count: i64 = 0;
        var saw_rollup = false;

        for (edges) |edge| {
            const metadata_value = (try pathEdgeMetadataJsonValue(alloc, edge.metadata)) orelse continue;

            var edge_obj = std.json.ObjectMap.empty;
            errdefer edge_obj.deinit(alloc);
            try edge_obj.put(alloc, try alloc.dupe(u8, "source"), .{ .string = try alloc.dupe(u8, edge.source) });
            try edge_obj.put(alloc, try alloc.dupe(u8, "target"), .{ .string = try alloc.dupe(u8, edge.target) });
            try edge_obj.put(alloc, try alloc.dupe(u8, "type"), .{ .string = try alloc.dupe(u8, edge.edge_type) });
            try edge_obj.put(alloc, try alloc.dupe(u8, "weight"), .{ .float = edge.weight });
            try edge_obj.put(alloc, try alloc.dupe(u8, "metadata"), metadata_value);
            try edge_values.append(.{ .object = edge_obj });

            if (metadata_value == .object) {
                if (metadata_value.object.get("mention_count")) |value| {
                    if (value == .integer) {
                        mention_count += value.integer;
                        saw_rollup = true;
                    }
                }
                if (metadata_value.object.get("mention_artifact_keys")) |value| {
                    if (value == .array) {
                        for (value.array.items) |item| {
                            if (item != .string) continue;
                            try mention_artifact_keys.append(.{ .string = try alloc.dupe(u8, item.string) });
                            saw_rollup = true;
                        }
                    }
                }
            }
        }

        if (edge_values.items.len > 0) {
            try evidence.put(alloc, try alloc.dupe(u8, "path_edges"), .{ .array = edge_values });
        } else {
            edge_values.deinit();
        }

        if (saw_rollup) {
            var rollup = std.json.ObjectMap.empty;
            errdefer rollup.deinit(alloc);
            try rollup.put(alloc, try alloc.dupe(u8, "mention_count"), .{ .integer = mention_count });
            if (mention_artifact_keys.items.len > 0) {
                try rollup.put(alloc, try alloc.dupe(u8, "mention_artifact_keys"), .{ .array = mention_artifact_keys });
            } else {
                mention_artifact_keys.deinit();
            }
            try evidence.put(alloc, try alloc.dupe(u8, "mention_rollup"), .{ .object = rollup });
        } else {
            mention_artifact_keys.deinit();
        }
    }

    return .{ .map = evidence };
}

fn toOpenApiOptionalPathEdges(
    alloc: std.mem.Allocator,
    edges: ?[]const graph_query_mod.PathEdgeInfo,
) !?[]const indexes_openapi.PathEdge {
    const value = edges orelse return null;
    return try toOpenApiPathEdges(alloc, value);
}

fn toOpenApiOptionalGraphPathEdges(
    alloc: std.mem.Allocator,
    node: graph_query_mod.GraphResultNode,
) !?[]const indexes_openapi.GraphPathEdge {
    const edges = node.path_edges orelse return null;
    const path = node.path orelse {
        if (edges.len == 0) return null;
        return error.InvalidRemoteResponse;
    };
    const tables = node.path_tables orelse &.{};
    return try toOpenApiGraphPathEdges(alloc, path, tables, edges);
}

test "api query contract preserves algebraic graph path provenance" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const alloc = arena_impl.allocator();
    const path_nodes: []const []const u8 = &.{ "A", "B", "C" };
    const path_tables: []const ?[]const u8 = &.{ null, "entities", "entities" };
    const path_edges: []const graph_query_mod.PathEdgeInfo = &.{
        .{ .source = "A", .target = "B", .edge_type = "e", .weight = 2.0, .metadata = "{\"mention_count\":2,\"mention_artifact_keys\":[\"m1\",\"m2\"]}" },
        .{ .source = "B", .target = "C", .edge_type = "e", .weight = 3.0 },
    };
    const provenance: []const []const u8 = &.{ "A\x1fe\x1fB", "B\x1fe\x1fC" };
    const nodes: []const graph_query_mod.GraphResultNode = &.{.{
        .key = "C",
        .table = "entities",
        .depth = 2,
        .distance = 2.0,
        .path = path_nodes,
        .path_tables = path_tables,
        .path_edges = path_edges,
        .provenance = provenance,
    }};
    const graph_result = db_mod.types.GraphSearchResult{
        .name = @constCast("shortest"),
        .nodes = @constCast(nodes),
        .paths = &.{},
        .matches = &.{},
        .hits = @constCast((&[_]db_mod.types.SearchHit{})[0..]),
        .total_hits = 1,
    };

    var document_lookup = try GraphDocumentLookup.init(alloc, graph_result.hits, false);
    defer document_lookup.deinit(alloc);
    const encoded = try toOpenApiGraphNodes(alloc, graph_result, &document_lookup, true);
    defer {
        if (encoded[0].path) |items| alloc.free(items);
        if (encoded[0].path_edges) |items| alloc.free(items);
        alloc.free(encoded);
    }

    try std.testing.expectEqual(@as(usize, 1), encoded.len);
    try std.testing.expectEqualStrings("C", encoded[0].key);
    try std.testing.expectEqual(@as(i64, 2), encoded[0].depth);
    try std.testing.expectEqualStrings("A", encoded[0].path.?[0].key);
    try std.testing.expect(encoded[0].path.?[0].table == null);
    try std.testing.expectEqualStrings("C", encoded[0].path.?[2].key);
    try std.testing.expectEqualStrings("entities", encoded[0].path.?[2].table.?);
    try std.testing.expectEqual(@as(usize, 2), encoded[0].path_edges.?.len);
    try std.testing.expectEqualStrings("A", encoded[0].path_edges.?[0].from.key);
    try std.testing.expect(encoded[0].path_edges.?[0].from.table == null);
    try std.testing.expectEqualStrings("B", encoded[0].path_edges.?[0].to.key);
    try std.testing.expectEqualStrings("entities", encoded[0].path_edges.?[0].to.table.?);
    try std.testing.expectEqualStrings("e", encoded[0].path_edges.?[0].type);
    try std.testing.expectEqual(@as(f64, 3.0), encoded[0].path_edges.?[1].weight);
    try std.testing.expectEqual(@as(i64, 2), encoded[0].path_edges.?[0].metadata.?.map.get("mention_count").?.integer);
    try std.testing.expectEqual(@as(usize, 2), encoded[0].provenance.?.len);
    try std.testing.expectEqualStrings("A\x1fe\x1fB", encoded[0].provenance.?[0]);
    try std.testing.expectEqualStrings("B\x1fe\x1fC", encoded[0].provenance.?[1]);
    const evidence = encoded[0].evidence.?.map;
    try std.testing.expectEqual(@as(usize, 2), evidence.get("provenance").?.array.items.len);
    try std.testing.expectEqual(@as(usize, 1), evidence.get("path_edges").?.array.items.len);
    const edge_evidence = evidence.get("path_edges").?.array.items[0].object;
    try std.testing.expectEqualStrings("A", edge_evidence.get("source").?.string);
    try std.testing.expectEqual(@as(i64, 2), edge_evidence.get("metadata").?.object.get("mention_count").?.integer);
    const mention_rollup = evidence.get("mention_rollup").?.object;
    try std.testing.expectEqual(@as(i64, 2), mention_rollup.get("mention_count").?.integer);
    try std.testing.expectEqual(@as(usize, 2), mention_rollup.get("mention_artifact_keys").?.array.items.len);
    try std.testing.expectEqualStrings("m2", mention_rollup.get("mention_artifact_keys").?.array.items[1].string);

    const legacy_encoded = try toOpenApiLegacyGraphNodes(alloc, graph_result, &document_lookup);
    defer alloc.free(legacy_encoded);
    try std.testing.expectEqualStrings("A", legacy_encoded[0].path.?[0]);
    try std.testing.expectEqualStrings("C", legacy_encoded[0].path.?[2]);
}

test "api query contract hydrates equal graph keys from their table namespace" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const alloc = arena_impl.allocator();
    const hits = [_]db_mod.types.SearchHit{
        .{
            .id = @constCast("shared"),
            .stored_data = @constCast("{\"origin\":\"docs\"}"),
        },
        .{
            .id = @constCast("shared"),
            .source_table = @constCast("entities"),
            .stored_data = @constCast("{\"origin\":\"entities\"}"),
        },
    };

    var document_lookup = try GraphDocumentLookup.init(alloc, &hits, true);
    defer document_lookup.deinit(alloc);

    const local = (try document_lookup.document("shared", null)) orelse
        return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings(
        "docs",
        local.map.get("origin").?.string,
    );
    const external = (try document_lookup.document("shared", "entities")) orelse
        return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings(
        "entities",
        external.map.get("origin").?.string,
    );
    try std.testing.expect(try document_lookup.document("shared", "missing") == null);
}

test "api query contract bounds hydrated graph binding cells" {
    const body =
        \\{
        \\  "graph_queries": {
        \\    "too_wide": {
        \\      "index": "graph",
        \\      "match": {
        \\        "anchor": "a",
        \\        "nodes": {"a": {}, "b": {}},
        \\        "edges": [{"from": "a", "to": "b"}]
        \\      },
        \\      "return": {
        \\        "bindings": ["a", "b"],
        \\        "limit": 5001,
        \\        "include_documents": true
        \\      }
        \\    }
        \\  }
        \\}
    ;
    try std.testing.expectError(
        error.InvalidQueryRequest,
        parseQueryRequest(std.testing.allocator, null, "docs", body),
    );
}

test "graph document lookup enforces its defensive hydration budget" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const alloc = arena_impl.allocator();
    const hits = [_]db_mod.types.SearchHit{.{
        .id = @constCast("doc:a"),
        .stored_data = @constCast("{\"title\":\"a\"}"),
    }};
    var lookup = try GraphDocumentLookup.init(alloc, &hits, true);
    defer lookup.deinit(alloc);
    lookup.remaining_bindings = 1;
    try std.testing.expect((try lookup.document("doc:a", null)) != null);
    try std.testing.expectError(
        error.QueryCandidateBudgetExceeded,
        lookup.document("doc:a", null),
    );
}

fn buildProfileValue(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
    meta: QueryResponseMeta,
    result: db_mod.types.SearchResult,
) !std.json.Value {
    const profile = metadata_openapi.QueryProfile{
        .shards = .{
            .total = meta.shard_count,
            .successful = meta.shard_count,
            .failed = 0,
        },
        .reranker = if (meta.reranker) |reranker| .{
            .model = if (reranker.model.len > 0) reranker.model else null,
            .documents_reranked = reranker.documents_reranked,
            .duration_ms = reranker.duration_ms,
        } else null,
        .merge = if (meta.merge) |merge| .{
            .strategy = merge.strategy,
            .full_text_hits = merge.full_text_hits,
            .semantic_hits = merge.semantic_hits,
            .duration_ms = merge.duration_ms,
        } else if (req.merge_config != null or meta.merged) .{
            .strategy = if (req.merge_config) |merge_config| switch (merge_config.strategy) {
                .rrf => .rrf,
                .rsf => .rsf,
            } else .rrf,
            .full_text_hits = result.total_hits,
            .semantic_hits = if (req.dense_queries.len > 0 or req.sparse_queries.len > 0) result.total_hits else 0,
            .duration_ms = meta.took_ms,
        } else null,
    };
    const encoded = try jsonStringifyAlloc(alloc, profile);
    defer alloc.free(encoded);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, encoded, .{});
    if (meta.dense_search) |dense_search| {
        if (parsed.value != .object) return error.InvalidQueryRequest;
        const dense_json = try jsonStringifyAlloc(alloc, dense_search);
        defer alloc.free(dense_json);
        const dense_parsed = try std.json.parseFromSlice(std.json.Value, alloc, dense_json, .{});
        try parsed.value.object.put(alloc, "dense_search", dense_parsed.value);
    }
    if (result.sort_profile) |sort_profile| {
        if (parsed.value != .object) return error.InvalidQueryRequest;
        try parsed.value.object.put(alloc, "sort", try buildSortProfileValue(alloc, req, sort_profile));
    }
    return parsed.value;
}

fn buildProfileUnsignedValue(alloc: std.mem.Allocator, value: u64) !std.json.Value {
    if (value <= @as(u64, @intCast(std.math.maxInt(i64)))) {
        return .{ .integer = @intCast(value) };
    }
    return .{ .number_string = try std.fmt.allocPrint(alloc, "{d}", .{value}) };
}

fn buildProfileSizeValue(alloc: std.mem.Allocator, value: usize) !std.json.Value {
    return buildProfileUnsignedValue(alloc, @intCast(value));
}

fn buildSortProfileValue(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
    profile: db_mod.types.SortProfile,
) !std.json.Value {
    var sort = std.json.ObjectMap.empty;
    try sort.put(alloc, "plan", .{ .string = profile.plan });
    try sort.put(alloc, "order_by", try buildSortProfileOrderValue(alloc, req));
    try sort.put(alloc, "cursor", .{ .string = sortProfileCursorMode(req) });
    try sort.put(alloc, "exactness", .{ .string = profile.exactness });
    try sort.put(alloc, "source", .{ .string = profile.source });
    try sort.put(alloc, "candidate_source", .{ .string = profile.candidate_source });
    try sort.put(alloc, "cursor_support", .{ .string = profile.cursor_support });
    try sort.put(alloc, "source_load", .{ .string = profile.source_load });
    try sort.put(alloc, "distributed_behavior", .{ .string = profile.distributed_behavior });
    try sort.put(alloc, "selection_reason", .{ .string = profile.selection_reason });
    try sort.put(alloc, "require_native", .{ .bool = profile.require_native });
    try sort.put(alloc, "sort_lifecycle_state", .{ .string = profile.sort_lifecycle_state });
    try sort.put(alloc, "index_sort_coverage", .{ .string = profile.index_sort_coverage });
    try sort.put(alloc, "candidate_count", try buildProfileUnsignedValue(alloc, profile.candidate_count));
    try sort.put(alloc, "cursor_rejected_count", try buildProfileUnsignedValue(alloc, profile.cursor_rejected_count));
    try sort.put(alloc, "selected_count", try buildProfileUnsignedValue(alloc, profile.selected_count));
    try sort.put(alloc, "total_us", try buildProfileUnsignedValue(alloc, profile.total_us));
    try sort.put(alloc, "distributed_shard_count", try buildProfileSizeValue(alloc, profile.distributed_shard_count));
    try sort.put(alloc, "budget_rejection_reason", .{ .string = profile.budget_rejection_reason });
    const public_sort_rejection_reason = if (profile.sort_rejection_reason.len > 0)
        publicExactSortReason(profile.sort_rejection_reason, profile.sort_rejection_detail)
    else
        "";
    try sort.put(alloc, "sort_rejection_reason", .{ .string = public_sort_rejection_reason });
    try sort.put(alloc, "sort_rejection_detail", .{ .string = profile.sort_rejection_detail });
    try sort.put(alloc, "sort_rejection_field", .{ .string = try alloc.dupe(u8, profile.sort_rejection_field.slice()) });
    return .{ .object = sort };
}

fn sortProfileNeedsImplicitIdTiebreaker(req: db_mod.types.SearchRequest) bool {
    if (req.order_by.len == 0) return false;
    return !std.mem.eql(u8, req.order_by[req.order_by.len - 1].field, "_id");
}

fn sortProfileRequestNeedsDefaultIdOrder(req: db_mod.types.SearchRequest) bool {
    return req.order_by.len == 0 and (req.search_after.len > 0 or req.search_before.len > 0);
}

fn sortProfileRequestHasOrderedPage(req: db_mod.types.SearchRequest) bool {
    return req.order_by.len > 0 or req.search_after.len > 0 or req.search_before.len > 0;
}

fn sortProfileEffectiveOrderLen(req: db_mod.types.SearchRequest) usize {
    if (sortProfileRequestNeedsDefaultIdOrder(req)) return 1;
    return req.order_by.len + @intFromBool(sortProfileNeedsImplicitIdTiebreaker(req));
}

fn sortProfileEffectiveOrderField(req: db_mod.types.SearchRequest, index: usize) db_mod.types.SortField {
    if (sortProfileRequestNeedsDefaultIdOrder(req)) return .{ .field = "_id" };
    if (index < req.order_by.len) return req.order_by[index];
    return .{ .field = "_id" };
}

fn buildSortProfileOrderValue(alloc: std.mem.Allocator, req: db_mod.types.SearchRequest) !std.json.Value {
    var order = std.json.Array.init(alloc);
    errdefer order.deinit();
    const field_count = sortProfileEffectiveOrderLen(req);
    for (0..field_count) |i| {
        const field = sortProfileEffectiveOrderField(req, i);
        var field_obj = std.json.ObjectMap.empty;
        errdefer field_obj.deinit(alloc);
        try field_obj.put(alloc, "field", .{ .string = field.field });
        try field_obj.put(alloc, "desc", .{ .bool = field.desc });
        try order.append(.{ .object = field_obj });
    }
    return .{ .array = order };
}

fn sortProfileCursorMode(req: db_mod.types.SearchRequest) []const u8 {
    if (req.search_after.len > 0) return "after";
    if (req.search_before.len > 0) return "before";
    return "none";
}

fn parseMergeConfig(alloc: std.mem.Allocator, generated: indexes_openapi.MergeConfig) !db_mod.types.MergeConfig {
    var config = db_mod.types.MergeConfig{};
    if (generated.strategy) |strategy| {
        config.strategy = switch (strategy) {
            .rrf => .rrf,
            .rsf => .rsf,
            .failover => return error.UnsupportedQueryRequest,
        };
    }
    if (generated.rank_constant) |rank_constant| config.rank_constant = rank_constant;
    if (generated.window_size) |window_size| {
        config.window_size = std.math.cast(u32, window_size) orelse return error.InvalidQueryRequest;
    }
    if (generated.weights) |weights| {
        var named = try alloc.alloc(fusion_mod.NamedWeight, weights.map.count());
        var initialized: usize = 0;
        errdefer {
            for (named[0..initialized]) |item| alloc.free(item.name);
            alloc.free(named);
        }
        for (weights.map.keys(), weights.map.values()) |name, weight| {
            named[initialized] = .{
                .name = try alloc.dupe(u8, name),
                .weight = weight,
            };
            initialized += 1;
        }
        config.weights = named;
    }
    return config;
}

fn parsePruner(generated: indexes_openapi.Pruner) !fusion_mod.Pruner {
    return .{
        .min_score_ratio = generated.min_score_ratio orelse 0.0,
        .max_score_gap_percent = generated.max_score_gap_percent orelse 0.0,
        .min_absolute_score = generated.min_absolute_score orelse 0.0,
        .require_multi_index = generated.require_multi_index orelse false,
        .std_dev_threshold = generated.std_dev_threshold orelse 0.0,
    };
}

fn buildRerankerQueryText(alloc: std.mem.Allocator, request: anytype) ![]const u8 {
    if (request.semantic_search) |semantic_search| {
        return try alloc.dupe(u8, semantic_search);
    }
    if (request.full_text_search) |full_text_search| {
        return try buildRerankerQueryTextFromValue(alloc, full_text_search);
    }
    if (request.query) |query| {
        return try buildRerankerQueryTextFromValue(alloc, query);
    }
    return error.UnsupportedQueryRequest;
}

fn buildRerankerQueryTextFromValue(alloc: std.mem.Allocator, input: anytype) ![]const u8 {
    const Input = @TypeOf(input);
    if (comptime Input != std.json.Value) {
        if (comptime @hasField(Input, "bytes")) {
            const parsed = try std.json.parseFromSlice(std.json.Value, alloc, input.bytes, .{});
            defer parsed.deinit();
            return buildRerankerQueryTextFromValue(alloc, parsed.value);
        }
        @compileError("reranker query input must be std.json.Value or raw JSON");
    }

    const value: std.json.Value = input;
    if (value == .string) return try alloc.dupe(u8, value.string);
    if (value != .object) return try jsonStringifyAlloc(alloc, value);

    if (value.object.get("query")) |query| {
        if (query == .string) return try alloc.dupe(u8, query.string);
    }
    if (value.object.get("match")) |match| {
        if (match == .string) return try alloc.dupe(u8, match.string);
        if (match == .object) {
            if (match.object.get("text")) |text| {
                if (text == .string) return try alloc.dupe(u8, text.string);
            }
            if (match.object.get("match")) |text| {
                if (text == .string) return try alloc.dupe(u8, text.string);
            }
            if (match.object.count() == 1) {
                var it = match.object.iterator();
                if (it.next()) |entry| {
                    if (entry.value_ptr.* == .string) return try alloc.dupe(u8, entry.value_ptr.string);
                }
            }
        }
    }
    if (value.object.get("term")) |term| {
        if (term == .string) return try alloc.dupe(u8, term.string);
        if (term == .object and term.object.count() == 1) {
            var it = term.object.iterator();
            if (it.next()) |entry| {
                if (entry.value_ptr.* == .string) return try alloc.dupe(u8, entry.value_ptr.string);
            }
        }
    }
    return try jsonStringifyAlloc(alloc, value);
}

fn cloneFields(alloc: std.mem.Allocator, value: []const []const u8) ![][]const u8 {
    const fields = try alloc.alloc([]const u8, value.len);
    var initialized: usize = 0;
    errdefer {
        for (fields[0..initialized]) |field| alloc.free(field);
        alloc.free(fields);
    }
    for (value) |item| {
        fields[initialized] = try alloc.dupe(u8, item);
        initialized += 1;
    }
    return fields;
}

fn cloneTextMatrixAlloc(
    alloc: std.mem.Allocator,
    value: []const []const []const u8,
) ![][]const []const u8 {
    const groups = try alloc.alloc([]const []const u8, value.len);
    var initialized: usize = 0;
    errdefer {
        for (groups[0..initialized]) |group| {
            for (group) |term| alloc.free(term);
            alloc.free(group);
        }
        alloc.free(groups);
    }
    for (value) |group| {
        groups[initialized] = try cloneFields(alloc, group);
        initialized += 1;
    }
    return groups;
}

fn canDeferStoredProjection(fields: []const []const u8) bool {
    if (fields.len == 0) return false;
    for (fields) |field| {
        if (std.mem.eql(u8, field, "_chunks") or std.mem.eql(u8, field, "_chunks.*")) return false;
        if (std.mem.eql(u8, field, "_embeddings") or std.mem.eql(u8, field, "_embeddings.*")) return false;
    }
    return true;
}

const NormalizedPublicQueryBuckets = struct {
    full_text: ?db_mod.types.TextQuery = null,
    filter_text: ?db_mod.types.TextQuery = null,
    exclusion_text: ?db_mod.types.TextQuery = null,
    filter_query_json: []const u8 = "",
    exclusion_query_json: []const u8 = "",

    fn deinit(self: *NormalizedPublicQueryBuckets, alloc: std.mem.Allocator) void {
        if (self.full_text) |query| freeTextQuery(alloc, query);
        if (self.filter_text) |query| freeTextQuery(alloc, query);
        if (self.exclusion_text) |query| freeTextQuery(alloc, query);
        if (self.filter_query_json.len > 0) alloc.free(@constCast(self.filter_query_json));
        if (self.exclusion_query_json.len > 0) alloc.free(@constCast(self.exclusion_query_json));
        self.* = .{};
    }
};

fn normalizePublicQueryBucketsAlloc(
    alloc: std.mem.Allocator,
    request: anytype,
    limit: u32,
) !NormalizedPublicQueryBuckets {
    var scoring_must = std.ArrayListUnmanaged(db_mod.types.TextQuery).empty;
    errdefer deinitTextQueryArrayList(alloc, &scoring_must);
    var scoring_should = std.ArrayListUnmanaged(db_mod.types.TextQuery).empty;
    errdefer deinitTextQueryArrayList(alloc, &scoring_should);
    var filter_clauses = std.ArrayListUnmanaged([]u8).empty;
    errdefer deinitOwnedStringArrayList(alloc, &filter_clauses);
    var exclusion_clauses = std.ArrayListUnmanaged([]u8).empty;
    errdefer deinitOwnedStringArrayList(alloc, &exclusion_clauses);
    var filter_text_queries = std.ArrayListUnmanaged(db_mod.types.TextQuery).empty;
    errdefer deinitTextQueryArrayList(alloc, &filter_text_queries);
    var exclusion_text_queries = std.ArrayListUnmanaged(db_mod.types.TextQuery).empty;
    errdefer deinitTextQueryArrayList(alloc, &exclusion_text_queries);

    if (request.query) |query| {
        // QueryRequest.query intentionally uses std.json.Value so the planner
        // can consume its AST without reparsing raw bytes. Preserve the
        // OpenAPI object contract explicitly at this semantic boundary.
        if (query != .object) return error.InvalidQueryRequest;
        try appendCanonicalPublicQueryAlloc(
            alloc,
            query,
            limit,
            &scoring_must,
            &filter_clauses,
            &filter_text_queries,
            &exclusion_clauses,
            &exclusion_text_queries,
        );
    }
    if (request.full_text_search) |full_text_search| {
        var parsed_full_text_search = try std.json.parseFromSlice(std.json.Value, alloc, full_text_search.bytes, .{});
        defer parsed_full_text_search.deinit();
        try validatePublicQueryTraversalBudgetAlloc(alloc, parsed_full_text_search.value);
        try appendFullTextSearchClausesAlloc(
            alloc,
            &scoring_must,
            &filter_clauses,
            &filter_text_queries,
            parsed_full_text_search.value,
            limit,
        );
    }
    if (request.filter_query) |filter_query| {
        var parsed_filter_query = try std.json.parseFromSlice(std.json.Value, alloc, filter_query.bytes, .{});
        defer parsed_filter_query.deinit();
        appendPublicFilterOrTextClausesAlloc(
            alloc,
            &filter_clauses,
            &filter_text_queries,
            parsed_filter_query.value,
            limit,
        ) catch |err| switch (err) {
            error.InvalidQueryRequest => return error.InvalidFilterQueryRequest,
            error.UnsupportedQueryRequest => return error.UnsupportedFilterQueryRequest,
            else => return err,
        };
    }
    if (request.exclusion_query) |exclusion_query| {
        var parsed_exclusion_query = try std.json.parseFromSlice(std.json.Value, alloc, exclusion_query.bytes, .{});
        defer parsed_exclusion_query.deinit();
        appendPublicFilterOrTextClausesAlloc(
            alloc,
            &exclusion_clauses,
            &exclusion_text_queries,
            parsed_exclusion_query.value,
            limit,
        ) catch |err| switch (err) {
            error.InvalidQueryRequest => return error.InvalidExclusionQueryRequest,
            error.UnsupportedQueryRequest => return error.UnsupportedExclusionQueryRequest,
            else => return err,
        };
    }

    var full_text = try buildScoringTextQueryAlloc(
        alloc,
        &scoring_must,
        &scoring_should,
        false,
        1.0,
    );
    errdefer if (full_text) |query| freeTextQuery(alloc, query);
    deinitTextQueryArrayList(alloc, &scoring_must);
    deinitTextQueryArrayList(alloc, &scoring_should);

    const filter_query_json = try buildStructuredFilterClausesJsonAlloc(alloc, filter_clauses.items, .all);
    errdefer if (filter_query_json.len > 0) alloc.free(filter_query_json);
    const exclusion_query_json = try buildStructuredFilterClausesJsonAlloc(alloc, exclusion_clauses.items, .any);
    errdefer if (exclusion_query_json.len > 0) alloc.free(exclusion_query_json);
    const filter_text = try buildTextFilterQueryAlloc(alloc, &filter_text_queries, .all);
    errdefer if (filter_text) |query| freeTextQuery(alloc, query);
    const exclusion_text = try buildTextFilterQueryAlloc(alloc, &exclusion_text_queries, .any);
    errdefer if (exclusion_text) |query| freeTextQuery(alloc, query);

    deinitOwnedStringArrayList(alloc, &filter_clauses);
    deinitOwnedStringArrayList(alloc, &exclusion_clauses);

    const out = NormalizedPublicQueryBuckets{
        .full_text = full_text,
        .filter_text = filter_text,
        .exclusion_text = exclusion_text,
        .filter_query_json = filter_query_json,
        .exclusion_query_json = exclusion_query_json,
    };
    full_text = null;
    filter_text_queries = .empty;
    exclusion_text_queries = .empty;
    return out;
}

const TextFilterMode = enum { all, any };

fn buildTextFilterQueryAlloc(
    alloc: std.mem.Allocator,
    queries: *std.ArrayListUnmanaged(db_mod.types.TextQuery),
    mode: TextFilterMode,
) !?db_mod.types.TextQuery {
    if (queries.items.len == 0) {
        queries.deinit(alloc);
        queries.* = .empty;
        return null;
    }
    if (queries.items.len == 1) {
        const query = queries.items[0];
        queries.deinit(alloc);
        queries.* = .empty;
        return query;
    }
    const owned = try queries.toOwnedSlice(alloc);
    queries.* = .empty;
    return .{ .bool_query = switch (mode) {
        .all => .{ .must = owned },
        .any => .{ .should = owned, .min_should = 1 },
    } };
}

fn appendPublicFilterOrTextClausesAlloc(
    alloc: std.mem.Allocator,
    structured: *std.ArrayListUnmanaged([]u8),
    text: *std.ArrayListUnmanaged(db_mod.types.TextQuery),
    query_or_queries: std.json.Value,
    limit: u32,
) !void {
    if (query_or_queries == .array) {
        if (query_or_queries.array.items.len == 0) return error.InvalidQueryRequest;
        for (query_or_queries.array.items) |item| {
            try appendPublicFilterOrTextClausesAlloc(alloc, structured, text, item, limit);
        }
        return;
    }
    if (nonScoringBoolFilterValue(query_or_queries)) |filter| {
        try appendPublicFilterOrTextClausesAlloc(alloc, structured, text, filter, limit);
        return;
    }
    if (try appendPositiveMixedFilterConjunctionAlloc(
        alloc,
        structured,
        text,
        query_or_queries,
        limit,
    )) return;

    appendPublicFilterClausesAlloc(alloc, structured, query_or_queries, limit) catch |err| switch (err) {
        error.UnsupportedQueryRequest => {
            const parsed = try parseSupportedFullTextQuery(alloc, query_or_queries, limit);
            errdefer freeTextQuery(alloc, parsed);
            try text.append(alloc, parsed);
        },
        else => return err,
    };
}

/// Split only conjunctions, because the execution contract can preserve a
/// heterogeneous AND as independent structured and text filters. Cross-engine
/// disjunctions remain unsupported rather than being silently reinterpreted.
fn appendPositiveMixedFilterConjunctionAlloc(
    alloc: std.mem.Allocator,
    structured: *std.ArrayListUnmanaged([]u8),
    text: *std.ArrayListUnmanaged(db_mod.types.TextQuery),
    value: std.json.Value,
    limit: u32,
) anyerror!bool {
    if (value != .object) return false;

    var mixed_structured = std.ArrayListUnmanaged([]u8).empty;
    defer deinitOwnedStringArrayList(alloc, &mixed_structured);
    var mixed_text = std.ArrayListUnmanaged(db_mod.types.TextQuery).empty;
    defer deinitTextQueryArrayList(alloc, &mixed_text);

    if (value.object.get("conjuncts")) |children| {
        var recognized: usize = 1;
        if (value.object.get("boost") != null) recognized += 1;
        if (recognized != value.object.count()) return false;
        _ = try parseCanonicalBoolBoost(value.object.get("boost"));
        try appendPublicFilterOrTextClausesAlloc(
            alloc,
            &mixed_structured,
            &mixed_text,
            children,
            limit,
        );
    } else {
        if (value.object.count() != 1) return false;
        const bool_value = value.object.get("bool") orelse return false;
        if (bool_value != .object) return false;
        if (bool_value.object.get("should") != null or
            bool_value.object.get("must_not") != null)
        {
            return false;
        }
        var recognized: usize = 0;
        inline for ([_][]const u8{ "must", "filter", "boost" }) |key| {
            if (bool_value.object.get(key) != null) recognized += 1;
        }
        if (recognized != bool_value.object.count()) return false;
        const must = bool_value.object.get("must");
        const filter = bool_value.object.get("filter");
        if (must == null and filter == null) return false;
        _ = try parseCanonicalBoolBoost(bool_value.object.get("boost"));
        if (must) |children| {
            try appendPublicFilterOrTextClausesAlloc(
                alloc,
                &mixed_structured,
                &mixed_text,
                children,
                limit,
            );
        }
        if (filter) |children| {
            try appendPublicFilterOrTextClausesAlloc(
                alloc,
                &mixed_structured,
                &mixed_text,
                children,
                limit,
            );
        }
    }

    // Preserve canonical pure-domain compounds intact. Only a heterogeneous
    // conjunction needs lowering into independent execution buckets.
    if (mixed_structured.items.len == 0 or mixed_text.items.len == 0) return false;
    try structured.ensureUnusedCapacity(alloc, mixed_structured.items.len);
    try text.ensureUnusedCapacity(alloc, mixed_text.items.len);
    for (mixed_structured.items) |item| structured.appendAssumeCapacity(item);
    for (mixed_text.items) |item| text.appendAssumeCapacity(item);
    mixed_structured.deinit(alloc);
    mixed_structured = .empty;
    mixed_text.deinit(alloc);
    mixed_text = .empty;
    return true;
}

fn validatePublicFilterOrTextQueryAlloc(
    alloc: std.mem.Allocator,
    query: std.json.Value,
    limit: u32,
) !void {
    var structured = std.ArrayListUnmanaged([]u8).empty;
    defer deinitOwnedStringArrayList(alloc, &structured);
    var text = std.ArrayListUnmanaged(db_mod.types.TextQuery).empty;
    defer deinitTextQueryArrayList(alloc, &text);
    try appendPublicFilterOrTextClausesAlloc(
        alloc,
        &structured,
        &text,
        query,
        limit,
    );
}

fn appendCanonicalPublicQueryAlloc(
    alloc: std.mem.Allocator,
    query: std.json.Value,
    limit: u32,
    scoring_must: *std.ArrayListUnmanaged(db_mod.types.TextQuery),
    filter_clauses: *std.ArrayListUnmanaged([]u8),
    filter_text_queries: *std.ArrayListUnmanaged(db_mod.types.TextQuery),
    exclusion_clauses: *std.ArrayListUnmanaged([]u8),
    exclusion_text_queries: *std.ArrayListUnmanaged(db_mod.types.TextQuery),
) !void {
    try validatePublicQueryTraversalBudgetAlloc(alloc, query);
    if (query == .object) {
        if (query.object.get("bool")) |bool_value| {
            if (bool_value != .object) return error.InvalidQueryRequest;
            // `bool` is an operator root, not a discriminator that may coexist
            // with another query operator. Reject ambiguous trees instead of
            // silently selecting bool and dropping its siblings.
            if (query.object.count() != 1) return error.InvalidQueryRequest;
            var recognized: usize = 0;
            inline for ([_][]const u8{ "must", "should", "filter", "must_not", "boost" }) |branch| {
                if (bool_value.object.get(branch) != null) recognized += 1;
            }
            if (recognized != bool_value.object.count()) {
                return error.InvalidQueryRequest;
            }

            var bool_scoring_must = std.ArrayListUnmanaged(db_mod.types.TextQuery).empty;
            defer deinitTextQueryArrayList(alloc, &bool_scoring_must);
            var bool_scoring_should = std.ArrayListUnmanaged(db_mod.types.TextQuery).empty;
            defer deinitTextQueryArrayList(alloc, &bool_scoring_should);
            if (bool_value.object.get("must")) |must_value| {
                try appendBoolMustClausesAlloc(
                    alloc,
                    must_value,
                    limit,
                    &bool_scoring_must,
                    filter_clauses,
                    filter_text_queries,
                );
            }
            if (bool_value.object.get("should")) |should_value| {
                try appendScoringQueryClausesAlloc(
                    alloc,
                    &bool_scoring_should,
                    should_value,
                    limit,
                );
            }
            if (bool_value.object.get("filter")) |filter_value| {
                try appendPublicFilterOrTextClausesAlloc(
                    alloc,
                    filter_clauses,
                    filter_text_queries,
                    filter_value,
                    limit,
                );
            }
            if (bool_value.object.get("must_not")) |must_not_value| {
                try appendPublicFilterOrTextClausesAlloc(
                    alloc,
                    exclusion_clauses,
                    exclusion_text_queries,
                    must_not_value,
                    limit,
                );
            }
            const has_required_non_scoring_clause =
                bool_value.object.get("filter") != null or
                (bool_value.object.get("must") != null and
                    bool_scoring_must.items.len == 0);
            if (try buildScoringTextQueryAlloc(
                alloc,
                &bool_scoring_must,
                &bool_scoring_should,
                has_required_non_scoring_clause,
                try parseCanonicalBoolBoost(bool_value.object.get("boost")),
            )) |scoring_query| {
                errdefer freeTextQuery(alloc, scoring_query);
                try scoring_must.append(alloc, scoring_query);
            }
            return;
        }
    }

    if (isUnambiguousStructuredFilterValue(query)) {
        try appendRawStructuredFilterClausesAlloc(alloc, filter_clauses, query);
        return;
    }

    appendScoringQueryClausesAlloc(alloc, scoring_must, query, limit) catch |err| switch (err) {
        error.UnsupportedQueryRequest, error.InvalidQueryRequest => {
            if (!isCanonicalStructuredFilterValue(query)) return err;
            try appendRawStructuredFilterClausesAlloc(alloc, filter_clauses, query);
        },
        else => return err,
    };
}

fn parseCanonicalBoolBoost(value: ?std.json.Value) !f32 {
    const raw = value orelse return 1.0;
    if (raw == .null) return 1.0;
    const number: f64 = switch (raw) {
        .integer => |item| @floatFromInt(item),
        .float => |item| item,
        .number_string => |item| std.fmt.parseFloat(f64, item) catch
            return error.InvalidQueryRequest,
        else => return error.InvalidQueryRequest,
    };
    return try narrowPublicBoost(number);
}

fn parseGeneratedBoost(value: ?f64) !f32 {
    return try narrowPublicBoost(value orelse 1.0);
}

fn narrowPublicBoost(number: f64) !f32 {
    const max_f32: f64 = std.math.floatMax(f32);
    if (!std.math.isFinite(number) or
        number > max_f32 or number < -max_f32)
    {
        return error.InvalidQueryRequest;
    }
    return @floatCast(number);
}

fn appendScoringQueryClausesAlloc(
    alloc: std.mem.Allocator,
    list: *std.ArrayListUnmanaged(db_mod.types.TextQuery),
    query_or_queries: std.json.Value,
    limit: u32,
) !void {
    if (query_or_queries == .array) {
        if (query_or_queries.array.items.len == 0) return error.InvalidQueryRequest;
        for (query_or_queries.array.items) |item| {
            try appendScoringQueryClausesAlloc(alloc, list, item, limit);
        }
        return;
    }

    const parsed = try parseSupportedFullTextQuery(alloc, query_or_queries, limit);
    errdefer freeTextQuery(alloc, parsed);
    try list.append(alloc, parsed);
}

fn appendPublicFilterClausesAlloc(
    alloc: std.mem.Allocator,
    list: *std.ArrayListUnmanaged([]u8),
    query_or_queries: std.json.Value,
    limit: u32,
) !void {
    try validatePublicQueryTraversalBudgetAlloc(alloc, query_or_queries);
    return appendPublicFilterClausesBoundedAlloc(alloc, list, query_or_queries, limit);
}

fn appendPublicFilterClausesBoundedAlloc(
    alloc: std.mem.Allocator,
    list: *std.ArrayListUnmanaged([]u8),
    query_or_queries: std.json.Value,
    limit: u32,
) !void {
    if (query_or_queries == .array) {
        if (query_or_queries.array.items.len == 0) return error.InvalidQueryRequest;
        for (query_or_queries.array.items) |item| {
            try appendPublicFilterClausesBoundedAlloc(alloc, list, item, limit);
        }
        return;
    }
    if (isExplicitStructuredScalarFilterValue(query_or_queries)) {
        db_mod.validateStructuredFilterValueAlloc(
            alloc,
            query_or_queries,
        ) catch |err| return switch (err) {
            error.OutOfMemory => error.OutOfMemory,
            error.UnsupportedQueryRequest => error.UnsupportedQueryRequest,
            else => error.InvalidQueryRequest,
        };
        try appendRawStructuredFilterClausesAlloc(alloc, list, query_or_queries);
        return;
    }

    // A valid storage-level DSL tree is already canonical. Check it first so
    // compound filters containing typed leaves do not enter the public query
    // parser merely because their bool/conjunction roots overlap Query unions.
    if (isCanonicalStructuredFilterValue(query_or_queries) and
        !publicFilterTreeContainsPublicQuerySyntax(query_or_queries))
    {
        if (db_mod.validateStructuredFilterValueAlloc(
            alloc,
            query_or_queries,
        )) |_| {
            try appendRawStructuredFilterClausesAlloc(
                alloc,
                list,
                query_or_queries,
            );
            return;
        } else |validation_err| switch (validation_err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => {},
        }
    }

    // Public Query unions and the internal structured-filter DSL deliberately
    // overlap at their roots. Prefer parsing the public query tree so
    // conjunction/disjunction children and scalar leaves are canonicalized
    // recursively before they cross the storage boundary. Only fall back to
    // the raw DSL for nodes that cannot be represented as a text query (for
    // example typed boolean/numeric terms).
    const parsed = parseSupportedFullTextQuery(
        alloc,
        query_or_queries,
        limit,
    ) catch |err| {
        if (err == error.OutOfMemory) return err;
        if (publicFilterTreeHasUnsupportedNode(query_or_queries)) {
            return error.UnsupportedQueryRequest;
        }
        if (!isStructuredFilterValue(query_or_queries) and
            err == error.UnsupportedQueryRequest)
        {
            return error.UnsupportedQueryRequest;
        }
        return error.InvalidQueryRequest;
    };
    defer freeTextQuery(alloc, parsed);
    const encoded = try encodePatternFilterQuery(alloc, parsed);
    errdefer alloc.free(encoded);
    try list.append(alloc, encoded);
}

const PublicQueryTraversalEntry = struct {
    value: std.json.Value,
    depth: usize,
};

/// Bounds every recursive public-query parser with one non-recursive pass.
/// Counting the complete JSON tree also caps broad boolean/terms requests
/// whose depth alone would otherwise look harmless.
fn validatePublicQueryTraversalBudgetAlloc(
    alloc: std.mem.Allocator,
    root: std.json.Value,
) !void {
    return validatePublicQueryTraversalBudgetWithDeadlineAlloc(alloc, root, null);
}

fn validatePublicQueryTraversalBudgetWithDeadlineAlloc(
    alloc: std.mem.Allocator,
    root: std.json.Value,
    deadline_ns: ?u64,
) !void {
    var pending = std.ArrayListUnmanaged(PublicQueryTraversalEntry).empty;
    defer pending.deinit(alloc);
    try pending.append(alloc, .{ .value = root, .depth = 0 });

    var visited: usize = 0;
    while (pending.pop()) |entry| {
        visited += 1;
        if (visited & 63 == 0) try ensureQueryDeadline(deadline_ns);
        if (visited > public_query_max_tree_nodes or
            entry.depth > public_query_max_tree_depth)
        {
            return error.InvalidQueryRequest;
        }
        const child_depth = entry.depth + 1;
        switch (entry.value) {
            .array => |array| {
                for (array.items) |child| {
                    try pending.append(alloc, .{
                        .value = child,
                        .depth = child_depth,
                    });
                }
            },
            .object => |object| {
                var it = object.iterator();
                while (it.next()) |child| {
                    try pending.append(alloc, .{
                        .value = child.value_ptr.*,
                        .depth = child_depth,
                    });
                }
            },
            else => {},
        }
    }
}

fn appendRawStructuredFilterClausesAlloc(
    alloc: std.mem.Allocator,
    list: *std.ArrayListUnmanaged([]u8),
    query_or_queries: std.json.Value,
) !void {
    if (query_or_queries == .array) {
        if (query_or_queries.array.items.len == 0) return error.InvalidQueryRequest;
        for (query_or_queries.array.items) |item| {
            try appendRawStructuredFilterClausesAlloc(alloc, list, item);
        }
        return;
    }
    if (!isStructuredFilterValue(query_or_queries)) return error.UnsupportedQueryRequest;
    db_mod.validateStructuredFilterValueAlloc(alloc, query_or_queries) catch |err| {
        return switch (err) {
            error.OutOfMemory => error.OutOfMemory,
            error.UnsupportedQueryRequest => error.UnsupportedQueryRequest,
            else => error.InvalidQueryRequest,
        };
    };
    const encoded = try jsonStringifyAlloc(alloc, query_or_queries);
    errdefer alloc.free(encoded);
    try list.append(alloc, encoded);
}

fn isStructuredFilterValue(value: std.json.Value) bool {
    if (value != .object) return false;
    inline for ([_][]const u8{
        "match_all",
        "match_none",
        "term",
        "terms",
        "exists",
        "match",
        "prefix",
        "wildcard",
        "regexp",
        "fuzzy",
        "range",
        "numeric_range",
        "term_range",
        "date_range",
        "bool_field",
        "ip_range",
        "geo_distance",
        "geo_bbox",
        "geo_shape",
        "ids",
        "doc_id",
        "doc_ids",
        "docids",
        "ref",
        "conjuncts",
        "disjuncts",
        "bool",
    }) |key| {
        if (value.object.get(key) != null) return true;
    }
    return false;
}

fn publicFilterTreeHasUnsupportedNode(value: std.json.Value) bool {
    if (value == .array) {
        for (value.array.items) |item| {
            if (publicFilterTreeHasUnsupportedNode(item)) return true;
        }
        return false;
    }
    if (value != .object) return false;

    inline for ([_][]const u8{ "conjuncts", "disjuncts" }) |compound| {
        if (value.object.get(compound)) |children| {
            if (children != .array) return false;
            for (children.array.items) |child| {
                if (publicFilterTreeHasUnsupportedNode(child)) return true;
            }
            return false;
        }
    }
    if (value.object.get("bool")) |bool_value| {
        if (bool_value != .object) return false;
        return publicFilterBooleanHasUnsupportedNode(bool_value.object);
    }
    if (publicFilterBooleanHasUnsupportedNode(value.object)) return true;
    inline for ([_][]const u8{ "filter", "must", "should", "must_not" }) |branch| {
        if (value.object.get(branch) != null) return false;
    }

    // Public PhraseQuery and MultiPhraseQuery use a top-level terms array plus
    // field, while canonical Zig `terms` filters keep their values inside the
    // terms object. Distinguish the full shape so the overlapping root name
    // does not turn an unsupported public variant into a misleading 400.
    if (value.object.get("terms")) |terms| {
        if (terms == .array and directDslFieldValue(value.object) != null) {
            return true;
        }
    }
    if (publicMatchHasUnsupportedOptions(value)) return true;

    inline for ([_][]const u8{
        "match_all",
        "match_none",
        "term",
        "terms",
        "exists",
        "match",
        "prefix",
        "wildcard",
        "regexp",
        "fuzzy",
        "range",
        "numeric_range",
        "term_range",
        "date_range",
        "bool_field",
        "ip_range",
        "geo_distance",
        "geo_bbox",
        "geo_shape",
        "ids",
        "doc_id",
        "query",
    }) |supported| {
        if (value.object.get(supported) != null) return false;
    }
    return true;
}

fn publicMatchHasUnsupportedOptions(value: std.json.Value) bool {
    if (value != .object) return false;
    const match = value.object.get("match") orelse return false;
    const options = if (match == .object) match.object else value.object;
    inline for ([_][]const u8{ "fuzziness", "prefix_length", "operator" }) |key| {
        if (options.get(key)) |option| {
            if (option != .null) return true;
        }
    }
    return false;
}

fn publicFilterTreeContainsPublicQuerySyntax(value: std.json.Value) bool {
    if (value == .array) {
        for (value.array.items) |item| {
            if (publicFilterTreeContainsPublicQuerySyntax(item)) return true;
        }
        return false;
    }
    if (value != .object) return false;

    inline for ([_][]const u8{ "term", "match", "prefix", "wildcard", "regexp", "fuzzy" }) |operator| {
        if (value.object.get(operator)) |operator_value| {
            if (directDslFieldValue(value.object) != null and operator_value != .object) {
                return true;
            }
        }
    }
    if (value.object.get("bool")) |bool_value| {
        // BoolFieldQuery and the storage bool compound deliberately share the
        // `bool` key. A scalar bool plus a field is the public leaf form.
        if (bool_value != .object) {
            return bool_value == .bool and directDslFieldValue(value.object) != null;
        }
        return publicFilterBooleanContainsPublicQuerySyntax(bool_value.object);
    }
    // Public BooleanQuery wraps positive and negative branches in their
    // conjunction/disjunction objects. Detect those wrappers before the
    // direct DSL's must_not shortcut can discard sibling branches.
    if (value.object.get("must")) |must| {
        if (must == .object and must.object.get("conjuncts") != null) return true;
    }
    inline for ([_][]const u8{ "should", "must_not" }) |branch| {
        if (value.object.get(branch)) |branch_value| {
            if (branch_value == .object and
                branch_value.object.get("disjuncts") != null)
            {
                return true;
            }
        }
    }
    inline for ([_][]const u8{ "conjuncts", "disjuncts" }) |compound| {
        if (value.object.get(compound)) |children| {
            return publicFilterTreeContainsPublicQuerySyntax(children);
        }
    }
    return publicFilterBooleanContainsPublicQuerySyntax(value.object);
}

fn publicFilterBooleanContainsPublicQuerySyntax(object: std.json.ObjectMap) bool {
    for ([_][]const u8{ "filter", "must", "should", "must_not" }) |branch| {
        const branch_value = object.get(branch) orelse continue;
        if (publicFilterTreeContainsPublicQuerySyntax(branch_value)) return true;
    }
    return false;
}

fn publicFilterBooleanHasUnsupportedNode(object: std.json.ObjectMap) bool {
    for ([_][]const u8{ "filter", "must", "should", "must_not" }) |branch| {
        const branch_value = object.get(branch) orelse continue;
        if (publicFilterTreeHasUnsupportedNode(branch_value)) return true;
    }
    return false;
}

fn isExplicitStructuredScalarFilterValue(value: std.json.Value) bool {
    if (value != .object) return false;
    inline for ([_][]const u8{ "term", "match", "prefix", "wildcard", "regexp", "fuzzy" }) |key| {
        if (value.object.get(key)) |operator_value| {
            if (operator_value == .object and
                (operator_value.object.get("path") != null or
                    operator_value.object.get("value") != null or
                    operator_value.object.get("values") != null))
            {
                return true;
            }
        }
    }
    return false;
}

fn isCanonicalStructuredFilterValue(value: std.json.Value) bool {
    if (value != .object) return false;
    inline for ([_][]const u8{
        "match_all",
        "match_none",
        "exists",
        "terms",
        "range",
        "numeric_range",
        "term_range",
        "date_range",
        "bool_field",
        "ip_range",
        "geo_distance",
        "geo_bbox",
        "geo_shape",
        "ids",
        "doc_id",
        "doc_ids",
        "docids",
        "ref",
        "conjuncts",
        "disjuncts",
        "bool",
    }) |key| {
        if (value.object.get(key) != null) return true;
    }
    inline for ([_][]const u8{ "term", "match", "prefix", "wildcard", "regexp", "fuzzy" }) |key| {
        if (value.object.get(key)) |operator_value| {
            if (operator_value != .object) return false;
            if (operator_value.object.get("path") != null) return true;
            if (operator_value.object.get("value") != null) return true;
            if (operator_value.object.get("values") != null) return true;
        }
    }
    return false;
}

fn isUnambiguousStructuredFilterValue(value: std.json.Value) bool {
    if (value != .object) return false;
    inline for ([_][]const u8{
        "match_all",
        "match_none",
        "exists",
        "terms",
        "range",
        "numeric_range",
        "term_range",
        "date_range",
        "bool_field",
        "ip_range",
        "geo_distance",
        "geo_bbox",
        "geo_shape",
        "ids",
        "doc_id",
        "doc_ids",
        "docids",
        "ref",
        "conjuncts",
        "disjuncts",
        "bool",
    }) |key| {
        if (value.object.get(key) != null) return true;
    }
    inline for ([_][]const u8{ "term", "match", "prefix", "wildcard", "regexp", "fuzzy" }) |key| {
        if (value.object.get(key)) |operator_value| {
            if (operator_value != .object) return false;
            if (operator_value.object.get("path") != null) return true;
            if (operator_value.object.get("values") != null) return true;
        }
    }
    return false;
}

fn buildScoringTextQueryAlloc(
    alloc: std.mem.Allocator,
    must: *std.ArrayListUnmanaged(db_mod.types.TextQuery),
    should: *std.ArrayListUnmanaged(db_mod.types.TextQuery),
    pure_should_optional: bool,
    boost: f32,
) !?db_mod.types.TextQuery {
    if (must.items.len == 0 and should.items.len == 0) return null;

    const owned_must = try alloc.dupe(db_mod.types.TextQuery, must.items);
    must.clearRetainingCapacity();
    errdefer {
        freeTextQueryList(alloc, owned_must);
    }
    const owned_should = try alloc.dupe(db_mod.types.TextQuery, should.items);
    should.clearRetainingCapacity();
    errdefer {
        freeTextQueryList(alloc, owned_should);
    }

    if (owned_must.len == 1 and owned_should.len == 0 and boost == 1.0) {
        const out = owned_must[0];
        alloc.free(owned_must);
        return out;
    }

    return .{ .bool_query = .{
        .must = owned_must,
        .should = owned_should,
        .min_should = if (owned_should.len > 0 and
            owned_must.len == 0 and
            !pure_should_optional) 1 else 0,
        .pure_should_optional = pure_should_optional and
            owned_should.len > 0 and
            owned_must.len == 0,
        .boost = boost,
    } };
}

const StructuredClauseMode = enum {
    all,
    any,
};

fn buildStructuredFilterClausesJsonAlloc(
    alloc: std.mem.Allocator,
    clauses: []const []const u8,
    mode: StructuredClauseMode,
) ![]u8 {
    if (clauses.len == 0) return "";
    if (clauses.len == 1) return try alloc.dupe(u8, clauses[0]);

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    switch (mode) {
        .all => try out.appendSlice(alloc, "{\"bool\":{\"must\":["),
        .any => try out.appendSlice(alloc, "{\"bool\":{\"should\":["),
    }
    for (clauses, 0..) |clause, i| {
        if (i > 0) try out.append(alloc, ',');
        try out.appendSlice(alloc, clause);
    }
    switch (mode) {
        .all => try out.appendSlice(alloc, "]}}"),
        .any => try out.appendSlice(alloc, "],\"minimum_should_match\":1}}"),
    }
    return try out.toOwnedSlice(alloc);
}

fn appendBoolMustClausesAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    limit: u32,
    scoring_must: *std.ArrayListUnmanaged(db_mod.types.TextQuery),
    filter_clauses: *std.ArrayListUnmanaged([]u8),
    filter_text_queries: *std.ArrayListUnmanaged(db_mod.types.TextQuery),
) !void {
    if (value == .array) {
        if (value.array.items.len == 0) return error.InvalidQueryRequest;
        for (value.array.items) |item| {
            try appendBoolMustClausesAlloc(
                alloc,
                item,
                limit,
                scoring_must,
                filter_clauses,
                filter_text_queries,
            );
        }
        return;
    }

    if (nonScoringBoolFilterValue(value)) |filter| {
        try appendPublicFilterOrTextClausesAlloc(
            alloc,
            filter_clauses,
            filter_text_queries,
            filter,
            limit,
        );
        return;
    }

    if (isUnambiguousStructuredFilterValue(value)) {
        try appendRawStructuredFilterClausesAlloc(alloc, filter_clauses, value);
        return;
    }

    appendScoringQueryClausesAlloc(alloc, scoring_must, value, limit) catch |err| switch (err) {
        error.UnsupportedQueryRequest, error.InvalidQueryRequest => {
            if (!isCanonicalStructuredFilterValue(value)) return err;
            try appendRawStructuredFilterClausesAlloc(alloc, filter_clauses, value);
        },
        else => return err,
    };
}

fn appendFullTextSearchClausesAlloc(
    alloc: std.mem.Allocator,
    scoring: *std.ArrayListUnmanaged(db_mod.types.TextQuery),
    filter_clauses: *std.ArrayListUnmanaged([]u8),
    filter_text_queries: *std.ArrayListUnmanaged(db_mod.types.TextQuery),
    value: std.json.Value,
    limit: u32,
) !void {
    if (value == .array) {
        if (value.array.items.len == 0) return error.InvalidQueryRequest;
        for (value.array.items) |item| {
            try appendFullTextSearchClausesAlloc(
                alloc,
                scoring,
                filter_clauses,
                filter_text_queries,
                item,
                limit,
            );
        }
        return;
    }
    if (nonScoringBoolFilterValue(value)) |filter| {
        try appendPublicFilterOrTextClausesAlloc(
            alloc,
            filter_clauses,
            filter_text_queries,
            filter,
            limit,
        );
        return;
    }
    try appendScoringQueryClausesAlloc(alloc, scoring, value, limit);
}

fn nonScoringBoolFilterValue(value: std.json.Value) ?std.json.Value {
    if (value != .object or value.object.count() != 1) return null;
    const bool_value = value.object.get("bool") orelse return null;
    if (bool_value != .object or bool_value.object.count() != 2) return null;
    const filter = bool_value.object.get("filter") orelse return null;
    const boost = bool_value.object.get("boost") orelse return null;
    const zero_boost = switch (boost) {
        .integer => |number| number == 0,
        .float => |number| number == 0,
        .number_string => |number| (std.fmt.parseFloat(f64, number) catch return null) == 0,
        else => false,
    };
    return if (zero_boost) filter else null;
}

fn deinitOwnedStringArrayList(alloc: std.mem.Allocator, list: *std.ArrayListUnmanaged([]u8)) void {
    for (list.items) |item| alloc.free(item);
    list.deinit(alloc);
    list.* = .empty;
}

fn parseSupportedFullTextQuery(alloc: std.mem.Allocator, query: std.json.Value, limit: u32) !db_mod.types.TextQuery {
    if (query != .object) return error.InvalidQueryRequest;
    if (query.object.get("dense_knn") != null) {
        return error.UnsupportedQueryRequest;
    } else if (query.object.get("sparse_knn") != null) {
        return error.UnsupportedQueryRequest;
    }
    _ = limit;
    return try parseSupportedTextQueryValue(alloc, query);
}

fn parseSupportedTextQueryValue(
    alloc: std.mem.Allocator,
    query: std.json.Value,
) !db_mod.types.TextQuery {
    // Parse overlapping public shapes explicitly before invoking the generated
    // oneOf decoder. Term and fuzzy queries share enough JSON fields that
    // declaration order alone cannot distinguish them.
    if (try parseDirectDslTextQuery(alloc, query)) |direct| return direct;
    return try parseGeneratedBleveTextQuery(alloc, query);
}

fn validateDirectTextQueryRoot(query: std.json.Value) !void {
    if (query != .object) return;
    var roots: usize = 0;
    inline for ([_][]const u8{
        "match_all",
        "match_none",
        "query",
        "conjuncts",
        "disjuncts",
        "bool",
        "multi_match",
        "term",
        "match",
        "match_phrase",
        "prefix",
        "wildcard",
        "regexp",
        "fuzzy",
        "ids",
        "terms",
        "polygon_points",
        "geometry",
        "cidr",
    }) |key| {
        if (query.object.get(key)) |value| {
            // Preserve the operator's validation error when a malformed term
            // is adjacent to a range; it is not a second viable query root.
            const viable = if (comptime std.mem.eql(u8, key, "term"))
                value == .string or value == .object
            else
                true;
            if (viable) roots += 1;
        }
    }
    var has_direct_bool_root = false;
    inline for ([_][]const u8{ "filter", "must", "should", "must_not" }) |key| {
        has_direct_bool_root = has_direct_bool_root or query.object.get(key) != null;
    }
    if (has_direct_bool_root) roots += 1;
    const has_geo_distance_root =
        query.object.get("location") != null or
        query.object.get("distance") != null;
    if (has_geo_distance_root) roots += 1;
    const has_geo_bbox_root =
        query.object.get("min_lat") != null or
        query.object.get("min_lon") != null or
        query.object.get("max_lat") != null or
        query.object.get("max_lon") != null;
    if (has_geo_bbox_root) roots += 1;
    const has_range_root =
        query.object.get("disjuncts") == null and
        (query.object.get("min") != null or
            query.object.get("max") != null or
            query.object.get("start") != null or
            query.object.get("end") != null);
    if (has_range_root) roots += 1;
    if (roots > 1) return error.InvalidQueryRequest;
}

fn boostedMatchAllTextQueryAlloc(
    alloc: std.mem.Allocator,
    boost: f32,
) !db_mod.types.TextQuery {
    if (boost == 1.0) return .{ .match_all = {} };
    const must = try alloc.alloc(db_mod.types.TextQuery, 1);
    must[0] = .{ .match_all = {} };
    return .{ .bool_query = .{
        .must = must,
        .boost = boost,
    } };
}

fn parseDirectDslTextQuery(alloc: std.mem.Allocator, query: std.json.Value) anyerror!?db_mod.types.TextQuery {
    if (query != .object) return null;
    try validateDirectTextQueryRoot(query);

    if (query.object.get("match_all") != null) {
        return try boostedMatchAllTextQueryAlloc(
            alloc,
            try parseCanonicalBoolBoost(query.object.get("boost")),
        );
    }

    if (query.object.get("conjuncts")) |conjuncts| {
        const boost = try parseCanonicalBoolBoost(query.object.get("boost"));
        if (conjuncts == .array and conjuncts.array.items.len == 0) {
            return try boostedMatchAllTextQueryAlloc(alloc, boost);
        }
        return .{ .bool_query = .{
            .must = try parseDirectDslTextQueryArrayAlloc(alloc, conjuncts),
            .boost = boost,
        } };
    }

    if (query.object.get("disjuncts")) |disjuncts| {
        if (disjuncts == .array and disjuncts.array.items.len == 0) {
            return .{ .match_none = {} };
        }
        const should = try parseDirectDslTextQueryArrayAlloc(alloc, disjuncts);
        errdefer freeTextQueryList(alloc, should);
        const raw_min = query.object.get("min");
        const min_should = try parsePublicMinimumShouldMatchJson(
            raw_min,
            should.len,
        );
        return .{ .bool_query = .{
            .should = should,
            .min_should = min_should,
            .pure_should_optional = raw_min != null and min_should == 0,
            .boost = try parseCanonicalBoolBoost(query.object.get("boost")),
        } };
    }

    if (query.object.get("filter") != null or
        query.object.get("must") != null or
        query.object.get("should") != null or
        query.object.get("must_not") != null)
    {
        return try parseDirectDslBoolTextQuery(alloc, query);
    }

    if (query.object.get("bool")) |bool_query| {
        if (bool_query == .object) {
            return try parseDirectDslBoolTextQuery(alloc, bool_query);
        }
    }

    const boost = try parseCanonicalBoolBoost(query.object.get("boost"));
    if (public_text_query_mod.parseStatefulDirectTextOperatorQueryAlloc(alloc, query, boost)) |maybe_direct| {
        if (maybe_direct) |direct| return direct;
    } else |err| return err;

    if (try parseDirectDslDateRangeQueryAlloc(alloc, query)) |date_range| return date_range;

    if (public_text_query_mod.parseStatefulDirectTextRangeQueryAlloc(alloc, query, boost)) |maybe_direct| {
        if (maybe_direct) |direct| return direct;
    } else |err| switch (err) {
        error.UnsupportedQueryRequest => {},
        else => return err,
    }

    return null;
}

fn directDslFieldValue(object: std.json.ObjectMap) ?std.json.Value {
    return object.get("field") orelse object.get("path");
}

fn parseDirectDslDateRangeQueryAlloc(alloc: std.mem.Allocator, query: std.json.Value) !?db_mod.types.TextQuery {
    if (query != .object) return null;
    const raw_start = query.object.get("start");
    const raw_end = query.object.get("end");
    const start_value = if (raw_start != null and raw_start.? != .null) raw_start else null;
    const end_value = if (raw_end != null and raw_end.? != .null) raw_end else null;
    if (start_value == null and end_value == null) return null;
    if ((query.object.get("min") orelse .null) != .null or (query.object.get("max") orelse .null) != .null) return error.UnsupportedQueryRequest;
    const field = directDslFieldValue(query.object) orelse return error.UnsupportedQueryRequest;
    if (field != .string) return error.UnsupportedQueryRequest;
    const start_ns = if (start_value) |start| blk: {
        if (start != .string) return error.UnsupportedQueryRequest;
        break :blk (try parseDateTimeOptionalToNs(start.string)) orelse return error.UnsupportedQueryRequest;
    } else null;
    const end_ns = if (end_value) |end| blk: {
        if (end != .string) return error.UnsupportedQueryRequest;
        break :blk (try parseDateTimeOptionalToNs(end.string)) orelse return error.UnsupportedQueryRequest;
    } else null;
    const inclusive_start = try optionalBoolOrDefault(query.object.get("inclusive_start"), true);
    const inclusive_end = try optionalBoolOrDefault(query.object.get("inclusive_end"), false);
    return .{ .date_range = .{
        .field = try alloc.dupe(u8, field.string),
        .start_ns = start_ns,
        .end_ns = end_ns,
        .inclusive_start = inclusive_start,
        .inclusive_end = inclusive_end,
        .boost = try parseCanonicalBoolBoost(query.object.get("boost")),
    } };
}

fn optionalBoolOrDefault(value: ?std.json.Value, default_value: bool) !bool {
    const item = value orelse return default_value;
    return switch (item) {
        .null => default_value,
        .bool => item.bool,
        else => error.UnsupportedQueryRequest,
    };
}

fn nonNullObjectMember(object: std.json.ObjectMap, key: []const u8) ?std.json.Value {
    const value = object.get(key) orelse return null;
    return if (value == .null) null else value;
}

fn parseDirectDslBoolTextQuery(alloc: std.mem.Allocator, query: std.json.Value) anyerror!db_mod.types.TextQuery {
    if (query != .object) return error.UnsupportedQueryRequest;

    var must = std.ArrayListUnmanaged(db_mod.types.TextQuery).empty;
    errdefer deinitTextQueryArrayList(alloc, &must);
    var should = std.ArrayListUnmanaged(db_mod.types.TextQuery).empty;
    errdefer deinitTextQueryArrayList(alloc, &should);
    var must_not = std.ArrayListUnmanaged(db_mod.types.TextQuery).empty;
    errdefer deinitTextQueryArrayList(alloc, &must_not);

    if (nonNullObjectMember(query.object, "filter")) |filter| {
        try appendDirectDslTextQueryList(alloc, &must, filter);
    }
    if (nonNullObjectMember(query.object, "must")) |must_value| {
        if (must_value == .object and must_value.object.get("conjuncts") != null) {
            try appendDirectDslTextQueryList(
                alloc,
                &must,
                must_value.object.get("conjuncts").?,
            );
        } else {
            try appendDirectDslTextQueryList(alloc, &must, must_value);
        }
    }
    var min_should: u32 = 0;
    var min_should_explicit = false;
    if (nonNullObjectMember(query.object, "should")) |should_value| {
        if (should_value == .object and should_value.object.get("disjuncts") != null) {
            const disjuncts = should_value.object.get("disjuncts").?;
            try appendDirectDslTextQueryList(alloc, &should, disjuncts);
            min_should_explicit = nonNullObjectMember(should_value.object, "min") != null;
            min_should = try parsePublicMinimumShouldMatchJson(
                should_value.object.get("min"),
                should.items.len,
            );
        } else {
            try appendDirectDslTextQueryList(alloc, &should, should_value);
        }
    }
    if (nonNullObjectMember(query.object, "must_not")) |must_not_value| {
        if (must_not_value == .object and
            must_not_value.object.get("disjuncts") != null)
        {
            try appendDirectDslTextQueryList(
                alloc,
                &must_not,
                must_not_value.object.get("disjuncts").?,
            );
        } else {
            try appendDirectDslTextQueryList(alloc, &must_not, must_not_value);
        }
    }

    if (must.items.len == 0 and should.items.len == 0 and must_not.items.len == 0)
        return .{ .match_all = {} };

    const owned_must = try must.toOwnedSlice(alloc);
    errdefer freeTextQueryList(alloc, owned_must);
    const owned_should = try should.toOwnedSlice(alloc);
    errdefer freeTextQueryList(alloc, owned_should);
    const owned_must_not = try must_not.toOwnedSlice(alloc);
    errdefer freeTextQueryList(alloc, owned_must_not);
    if (nonNullObjectMember(query.object, "minimum_should_match") orelse
        nonNullObjectMember(query.object, "min_should")) |minimum|
    {
        min_should_explicit = true;
        min_should = try parsePublicMinimumShouldMatchJson(
            minimum,
            owned_should.len,
        );
    }

    return .{ .bool_query = .{
        .must = owned_must,
        .should = owned_should,
        .must_not = owned_must_not,
        .min_should = min_should,
        .pure_should_optional = min_should_explicit and
            min_should == 0 and
            owned_must.len == 0 and
            owned_should.len > 0,
        .boost = try parseCanonicalBoolBoost(query.object.get("boost")),
    } };
}

fn parseDirectDslTextQueryArrayAlloc(
    alloc: std.mem.Allocator,
    queries: std.json.Value,
) anyerror![]const db_mod.types.TextQuery {
    if (queries != .array) return error.UnsupportedQueryRequest;
    const out = try alloc.alloc(db_mod.types.TextQuery, queries.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |item| freeTextQuery(alloc, item);
        alloc.free(out);
    }
    for (queries.array.items, 0..) |item, i| {
        out[i] = try parseSupportedTextQueryValue(alloc, item);
        initialized += 1;
    }
    return out;
}

fn parseDirectDslTextQueryListAlloc(
    alloc: std.mem.Allocator,
    query_or_queries: std.json.Value,
) anyerror![]const db_mod.types.TextQuery {
    var list = std.ArrayListUnmanaged(db_mod.types.TextQuery).empty;
    errdefer deinitTextQueryArrayList(alloc, &list);
    try appendDirectDslTextQueryList(alloc, &list, query_or_queries);
    return try list.toOwnedSlice(alloc);
}

fn appendDirectDslTextQueryList(
    alloc: std.mem.Allocator,
    list: *std.ArrayListUnmanaged(db_mod.types.TextQuery),
    query_or_queries: std.json.Value,
) anyerror!void {
    if (query_or_queries == .array) {
        for (query_or_queries.array.items) |item| {
            try appendDirectDslTextQueryList(alloc, list, item);
        }
        return;
    }

    const parsed = try parseSupportedTextQueryValue(alloc, query_or_queries);
    errdefer freeTextQuery(alloc, parsed);
    try list.append(alloc, parsed);
}

fn parseGeneratedBleveTextQuery(alloc: std.mem.Allocator, query: std.json.Value) !db_mod.types.TextQuery {
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    const normalized = try normalizeGeneratedBleveQuery(arena, query);
    const parsed = std.json.parseFromValue(
        query_openapi.Query,
        arena,
        normalized,
        .{},
    ) catch |err| switch (err) {
        // Generated oneOf dispatch uses UnexpectedToken when no supported
        // query discriminator matches. Keep that implementation detail out of
        // the public contract while preserving allocator and parser failures.
        error.UnexpectedToken => return error.UnsupportedQueryRequest,
        else => return err,
    };
    return try parseGeneratedBleveQueryValue(alloc, parsed.value);
}

fn normalizeGeneratedBleveQuery(alloc: std.mem.Allocator, query: std.json.Value) !std.json.Value {
    if (query != .object) return query;

    // Stored-document filters address values with RFC 6901 `path`, while the
    // shared generated text-query schema calls the same selector `field`.
    // Normalize that alias once at the generated-parser boundary. In
    // particular, flat fuzzy queries intentionally bypass the direct parser
    // so fuzziness cannot be mistaken for an exact term.
    if (query.object.get("path")) |path| {
        if (query.object.get("field") != null) return error.InvalidQueryRequest;
        var obj = std.json.ObjectMap.empty;
        var it = query.object.iterator();
        while (it.next()) |entry| {
            if (!std.mem.eql(u8, entry.key_ptr.*, "path")) {
                try obj.put(alloc, entry.key_ptr.*, entry.value_ptr.*);
            }
        }
        try obj.put(alloc, "field", path);
        return .{ .object = obj };
    }

    if (query.object.count() != 1) return query;

    if (query.object.get("match")) |wrapped| {
        if (wrapped == .object) {
            var obj = std.json.ObjectMap.empty;
            try obj.put(alloc, "match", wrapped.object.get("text") orelse wrapped.object.get("match") orelse return error.UnsupportedQueryRequest);
            if (directDslFieldValue(wrapped.object)) |field| try obj.put(alloc, "field", field);
            if (wrapped.object.get("analyzer")) |analyzer| try obj.put(alloc, "analyzer", analyzer);
            if (wrapped.object.get("fuzziness")) |fuzziness| try obj.put(alloc, "fuzziness", fuzziness);
            if (wrapped.object.get("prefix_length")) |prefix_length| try obj.put(alloc, "prefix_length", prefix_length);
            if (wrapped.object.get("operator")) |operator| try obj.put(alloc, "operator", operator);
            if (wrapped.object.get("boost")) |boost| try obj.put(alloc, "boost", boost);
            return .{ .object = obj };
        }
    }

    if (query.object.get("term")) |wrapped| {
        if (wrapped == .object) {
            var obj = std.json.ObjectMap.empty;
            if (wrapped.object.get("term")) |term| {
                try obj.put(alloc, "term", term);
            } else if (wrapped.object.count() == 1) {
                var it = wrapped.object.iterator();
                const entry = it.next() orelse return error.UnsupportedQueryRequest;
                try obj.put(alloc, "term", entry.value_ptr.*);
                try obj.put(alloc, "field", .{ .string = entry.key_ptr.* });
                return .{ .object = obj };
            } else {
                return error.UnsupportedQueryRequest;
            }
            if (directDslFieldValue(wrapped.object)) |field| try obj.put(alloc, "field", field);
            if (wrapped.object.get("fuzziness")) |fuzziness| try obj.put(alloc, "fuzziness", fuzziness);
            if (wrapped.object.get("prefix_length")) |prefix_length| try obj.put(alloc, "prefix_length", prefix_length);
            if (wrapped.object.get("boost")) |boost| try obj.put(alloc, "boost", boost);
            return .{ .object = obj };
        }
    }

    return query;
}

fn encodePatternFilterQuery(alloc: std.mem.Allocator, query: db_mod.types.TextQuery) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try appendPatternFilterQueryValue(alloc, &out, query);
    return try out.toOwnedSlice(alloc);
}

pub fn encodeSupportedPatternFilterQueryAlloc(
    alloc: std.mem.Allocator,
    query: std.json.Value,
) ![]u8 {
    try validatePublicQueryTraversalBudgetAlloc(alloc, query);
    const parsed = try parseSupportedFullTextQuery(alloc, query, 10);
    defer freeTextQuery(alloc, parsed);
    return try encodePatternFilterQuery(alloc, parsed);
}

/// Normalizes the structured subset of the public query AST for storage-only
/// readers such as primary-key scans. Text-index clauses are rejected instead
/// of being accepted and then evaluated with slower or observably different
/// stored-document semantics.
pub fn normalizePublicStoredFilterQueryAlloc(
    alloc: std.mem.Allocator,
    input: anytype,
) ![]u8 {
    const Input = @TypeOf(input);
    if (comptime Input != std.json.Value) {
        if (comptime @hasField(Input, "bytes")) {
            const parsed = std.json.parseFromSlice(std.json.Value, alloc, input.bytes, .{}) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return error.InvalidQueryRequest,
            };
            defer parsed.deinit();
            return normalizePublicStoredFilterQueryAlloc(alloc, parsed.value);
        }
        @compileError("stored filter query must be std.json.Value or raw JSON");
    }

    const query: std.json.Value = input;
    var clauses = std.ArrayListUnmanaged([]u8).empty;
    errdefer deinitOwnedStringArrayList(alloc, &clauses);
    try appendPublicFilterClausesAlloc(alloc, &clauses, query, 10);
    const out = try buildStructuredFilterClausesJsonAlloc(alloc, clauses.items, .all);
    deinitOwnedStringArrayList(alloc, &clauses);
    return out;
}

fn appendPatternFilterQueryValue(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    query: db_mod.types.TextQuery,
) !void {
    switch (query) {
        .match_all => try out.appendSlice(alloc, "{\"match_all\":{}}"),
        .match_none => try out.appendSlice(alloc, "{\"match_none\":{}}"),
        .term => |term| {
            try out.appendSlice(alloc, "{\"term\":{");
            var first = true;
            try appendJsonFieldString(alloc, out, &first, "path", term.field);
            try appendJsonFieldString(alloc, out, &first, "term", term.term);
            try out.appendSlice(alloc, "}}");
        },
        .match => |match| {
            try out.appendSlice(alloc, "{\"match\":{");
            var first = true;
            try appendJsonFieldString(alloc, out, &first, "path", match.field);
            try appendJsonFieldString(alloc, out, &first, "text", match.text);
            if (match.analyzer) |analyzer| {
                try appendJsonFieldString(alloc, out, &first, "analyzer", analyzer);
            }
            try out.appendSlice(alloc, "}}");
        },
        .prefix => |prefix| {
            try out.appendSlice(alloc, "{\"prefix\":{");
            var first = true;
            try appendJsonFieldString(alloc, out, &first, "path", prefix.field);
            try appendJsonFieldString(alloc, out, &first, "prefix", prefix.prefix);
            try out.appendSlice(alloc, "}}");
        },
        .wildcard => |wildcard| {
            try out.appendSlice(alloc, "{\"wildcard\":{");
            var first = true;
            try appendJsonFieldString(alloc, out, &first, "path", wildcard.field);
            try appendJsonFieldString(alloc, out, &first, "pattern", wildcard.pattern);
            try out.appendSlice(alloc, "}}");
        },
        .regexp => |regexp| {
            try out.appendSlice(alloc, "{\"regexp\":{");
            var first = true;
            try appendJsonFieldString(alloc, out, &first, "path", regexp.field);
            try appendJsonFieldString(alloc, out, &first, "pattern", regexp.pattern);
            try out.appendSlice(alloc, "}}");
        },
        .fuzzy => |fuzzy| {
            try out.appendSlice(alloc, "{\"fuzzy\":{");
            var first = true;
            try appendJsonFieldString(alloc, out, &first, "path", fuzzy.field);
            try appendJsonFieldString(alloc, out, &first, "query", fuzzy.term);
            try appendJsonFieldName(alloc, out, &first, "max_edits");
            try out.print(alloc, "{d}", .{fuzzy.max_edits});
            try appendJsonFieldName(alloc, out, &first, "prefix_length");
            try out.print(alloc, "{d}", .{fuzzy.prefix_len});
            if (fuzzy.auto_fuzzy) {
                try appendJsonFieldBool(alloc, out, &first, "auto_fuzzy", true);
            }
            try out.appendSlice(alloc, "}}");
        },
        .numeric_range => |range_query| {
            try out.append(alloc, '{');
            var first = true;
            try appendJsonFieldName(alloc, out, &first, "numeric_range");
            try out.append(alloc, '{');
            var inner_first = true;
            if (range_query.min) |min| {
                try appendJsonFieldName(alloc, out, &inner_first, "min");
                try out.print(alloc, "{d}", .{min});
            }
            if (range_query.max) |max| {
                try appendJsonFieldName(alloc, out, &inner_first, "max");
                try out.print(alloc, "{d}", .{max});
            }
            try appendJsonFieldString(alloc, out, &inner_first, "field", range_query.field);
            if (!range_query.inclusive_min) try appendJsonFieldBool(alloc, out, &inner_first, "inclusive_min", false);
            if (range_query.inclusive_max) try appendJsonFieldBool(alloc, out, &inner_first, "inclusive_max", true);
            try out.appendSlice(alloc, "}}");
        },
        .term_range => |range_query| {
            try out.appendSlice(alloc, "{\"term_range\":{");
            var first = true;
            try appendJsonFieldString(alloc, out, &first, "path", range_query.field);
            if (range_query.min) |min| {
                try appendJsonFieldString(alloc, out, &first, "min", min);
            }
            if (range_query.max) |max| {
                try appendJsonFieldString(alloc, out, &first, "max", max);
            }
            if (!range_query.inclusive_min) {
                try appendJsonFieldBool(alloc, out, &first, "inclusive_min", false);
            }
            if (range_query.inclusive_max) {
                try appendJsonFieldBool(alloc, out, &first, "inclusive_max", true);
            }
            try out.appendSlice(alloc, "}}");
        },
        .date_range => |range_query| {
            try out.appendSlice(alloc, "{\"date_range\":{");
            var first = true;
            try appendJsonFieldString(alloc, out, &first, "path", range_query.field);
            if (range_query.start_ns) |start_ns| {
                try appendJsonFieldName(alloc, out, &first, "start_ns");
                try out.print(alloc, "{d}", .{start_ns});
            }
            if (range_query.end_ns) |end_ns| {
                try appendJsonFieldName(alloc, out, &first, "end_ns");
                try out.print(alloc, "{d}", .{end_ns});
            }
            if (!range_query.inclusive_start) {
                try appendJsonFieldBool(alloc, out, &first, "inclusive_start", false);
            }
            if (range_query.inclusive_end) {
                try appendJsonFieldBool(alloc, out, &first, "inclusive_end", true);
            }
            try out.appendSlice(alloc, "}}");
        },
        .doc_id => |doc_id| {
            try out.appendSlice(alloc, "{\"doc_id\":");
            try out.append(alloc, '[');
            for (doc_id.ids, 0..) |id, i| {
                if (i > 0) try out.append(alloc, ',');
                try appendJsonString(alloc, out, id);
            }
            try out.appendSlice(alloc, "]}");
        },
        .bool_field => |bool_field| {
            try out.appendSlice(alloc, "{\"bool_field\":{");
            var first = true;
            try appendJsonFieldString(alloc, out, &first, "path", bool_field.field);
            try appendJsonFieldBool(alloc, out, &first, "value", bool_field.value);
            try out.appendSlice(alloc, "}}");
        },
        .bool_query => |bool_query| {
            try out.appendSlice(alloc, "{\"bool\":{");
            var first = true;
            if (bool_query.must.len > 0) {
                try appendJsonFieldName(alloc, out, &first, "must");
                try out.append(alloc, '[');
                for (bool_query.must, 0..) |item, i| {
                    if (i > 0) try out.append(alloc, ',');
                    try appendPatternFilterQueryValue(alloc, out, item);
                }
                try out.append(alloc, ']');
            }
            if (bool_query.should.len > 0) {
                try appendJsonFieldName(alloc, out, &first, "should");
                try out.append(alloc, '[');
                for (bool_query.should, 0..) |item, i| {
                    if (i > 0) try out.append(alloc, ',');
                    try appendPatternFilterQueryValue(alloc, out, item);
                }
                try out.append(alloc, ']');
            }
            if (bool_query.must_not.len > 0) {
                try appendJsonFieldName(alloc, out, &first, "must_not");
                try out.append(alloc, '[');
                for (bool_query.must_not, 0..) |item, i| {
                    if (i > 0) try out.append(alloc, ',');
                    try appendPatternFilterQueryValue(alloc, out, item);
                }
                try out.append(alloc, ']');
            }
            if (bool_query.min_should > 0 or bool_query.pure_should_optional) {
                try appendJsonFieldName(
                    alloc,
                    out,
                    &first,
                    "minimum_should_match",
                );
                try out.print(alloc, "{d}", .{bool_query.min_should});
            }
            try out.appendSlice(alloc, "}}");
        },
        else => return error.UnsupportedQueryRequest,
    }
}

fn parseGeneratedBleveBooleanQuery(
    alloc: std.mem.Allocator,
    boolean_query: *const query_openapi.BooleanQuery,
    boost: f32,
) anyerror!db_mod.types.TextBoolQuery {
    var must = std.ArrayListUnmanaged(db_mod.types.TextQuery).empty;
    errdefer deinitTextQueryArrayList(alloc, &must);

    if (boolean_query.filter) |filter| {
        const filter_query = try parseGeneratedBleveQueryValue(alloc, filter);
        errdefer freeTextQuery(alloc, filter_query);
        try must.append(alloc, filter_query);
    }
    if (boolean_query.must) |must_query| {
        const items = try parseGeneratedBleveQuerySlice(alloc, must_query.conjuncts);
        errdefer freeTextQueryList(alloc, items);
        try must.ensureUnusedCapacity(alloc, items.len);
        for (items) |item| must.appendAssumeCapacity(item);
        if (items.len > 0) alloc.free(items);
    }

    const should = if (boolean_query.should) |should_query|
        try parseGeneratedBleveQuerySlice(alloc, should_query.disjuncts)
    else
        &.{};
    errdefer if (should.len > 0) freeTextQueryList(alloc, should);

    const must_not = if (boolean_query.must_not) |must_not_query|
        try parseGeneratedBleveQuerySlice(alloc, must_not_query.disjuncts)
    else
        &.{};
    errdefer if (must_not.len > 0) freeTextQueryList(alloc, must_not);
    const min_should = if (boolean_query.should) |should_query|
        try parsePublicMinimumShouldMatch(should_query.min, should.len)
    else
        0;
    const pure_should_optional = if (boolean_query.should) |should_query|
        should_query.min != null and
            min_should == 0 and
            must.items.len == 0 and
            should.len > 0
    else
        false;

    return .{
        .must = try must.toOwnedSlice(alloc),
        .should = should,
        .must_not = must_not,
        .min_should = min_should,
        .pure_should_optional = pure_should_optional,
        .boost = boost,
    };
}

fn parsePublicMinimumShouldMatch(minimum: anytype, clause_count: usize) !u32 {
    return switch (@typeInfo(@TypeOf(minimum))) {
        .optional => if (minimum) |value|
            parsePublicMinimumShouldMatchValue(value, clause_count)
        else
            0,
        else => parsePublicMinimumShouldMatchValue(minimum, clause_count),
    };
}

fn parsePublicMinimumShouldMatchValue(raw: anytype, clause_count: usize) !u32 {
    // The public schema is integer-shaped. Keep the small float compatibility
    // arm for raw JSON and rolling deployments generated from the old schema;
    // both paths enforce the same exact integer execution contract.
    const value: f64 = switch (@typeInfo(@TypeOf(raw))) {
        .int, .comptime_int => @floatFromInt(raw),
        .float, .comptime_float => @floatCast(raw),
        else => @compileError("minimum_should_match must be numeric"),
    };
    if (!std.math.isFinite(value) or
        value < 0 or
        @floor(value) != value or
        value > @as(f64, @floatFromInt(std.math.maxInt(u32))))
    {
        return error.InvalidQueryRequest;
    }
    const parsed: u32 = @intFromFloat(value);
    if (@as(usize, parsed) > clause_count) return error.InvalidQueryRequest;
    return parsed;
}

fn parsePublicMinimumShouldMatchJson(
    minimum: ?std.json.Value,
    clause_count: usize,
) !u32 {
    const value = minimum orelse return 0;
    const parsed: f64 = switch (value) {
        .integer => |raw| @floatFromInt(raw),
        .float => |raw| raw,
        .number_string => |raw| std.fmt.parseFloat(f64, raw) catch
            return error.InvalidQueryRequest,
        else => return error.InvalidQueryRequest,
    };
    return parsePublicMinimumShouldMatch(parsed, clause_count);
}

fn parseGeneratedBleveQuerySlice(
    alloc: std.mem.Allocator,
    queries: []const query_openapi.Query,
) ![]const db_mod.types.TextQuery {
    if (queries.len == 0) return &.{};
    const out = try alloc.alloc(db_mod.types.TextQuery, queries.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |item| freeTextQuery(alloc, item);
        alloc.free(out);
    }
    for (queries, 0..) |item, i| {
        out[i] = try parseGeneratedBleveQueryValue(alloc, item);
        initialized += 1;
    }
    return out;
}

fn parseGeneratedBleveQueryBoost(query: query_openapi.Query) !f32 {
    return switch (query) {
        .match_all_query => |value| parseGeneratedBoost(value.boost.valueOrNull()),
        .match_none_query => |value| parseGeneratedBoost(value.boost.valueOrNull()),
        .query_string_query => |value| parseGeneratedBoost(value.boost.valueOrNull()),
        .term_query => |value| parseGeneratedBoost(value.boost.valueOrNull()),
        .match_query => |value| parseGeneratedBoost(value.boost.valueOrNull()),
        .multi_match_query => |value| parseGeneratedBoost(value.multi_match.boost.valueOrNull()),
        .match_phrase_query => |value| parseGeneratedBoost(value.boost.valueOrNull()),
        .phrase_query => |value| parseGeneratedBoost(value.boost.valueOrNull()),
        .multi_phrase_query => |value| parseGeneratedBoost(value.boost.valueOrNull()),
        .fuzzy_query => |value| parseGeneratedBoost(value.boost.valueOrNull()),
        .prefix_query => |value| parseGeneratedBoost(value.boost.valueOrNull()),
        .wildcard_query => |value| parseGeneratedBoost(value.boost.valueOrNull()),
        .regexp_query => |value| parseGeneratedBoost(value.boost.valueOrNull()),
        .numeric_range_query => |value| parseGeneratedBoost(value.boost.valueOrNull()),
        .term_range_query => |value| parseGeneratedBoost(value.boost.valueOrNull()),
        .date_range_string_query => |value| parseGeneratedBoost(value.boost.valueOrNull()),
        .doc_id_query => |value| parseGeneratedBoost(value.boost.valueOrNull()),
        .bool_field_query => |value| parseGeneratedBoost(value.boost.valueOrNull()),
        .boolean_query => |value| parseGeneratedBoost(value.boost.valueOrNull()),
        .conjunction_query => |value| parseGeneratedBoost(value.boost.valueOrNull()),
        .disjunction_query => |value| parseGeneratedBoost(value.boost.valueOrNull()),
        .ip_range_query => |value| parseGeneratedBoost(value.boost.valueOrNull()),
        .geo_bounding_box_query => |value| parseGeneratedBoost(value.boost.valueOrNull()),
        .geo_distance_query => |value| parseGeneratedBoost(value.boost.valueOrNull()),
        .geo_bounding_polygon_query => |value| parseGeneratedBoost(value.boost.valueOrNull()),
        .geo_shape_query => |value| parseGeneratedBoost(value.boost.valueOrNull()),
    };
}

fn parseGeneratedBleveQueryValue(alloc: std.mem.Allocator, query: query_openapi.Query) anyerror!db_mod.types.TextQuery {
    const query_string_has_default_operator = comptime blk: {
        const QueryStringType = @TypeOf((@as(query_openapi.Query, undefined)).query_string_query);
        break :blk switch (@typeInfo(QueryStringType)) {
            .pointer => |pointer| @hasField(pointer.child, "default_operator"),
            else => @hasField(QueryStringType, "default_operator"),
        };
    };
    const query_boost = try parseGeneratedBleveQueryBoost(query);

    return switch (query) {
        .match_all_query => try boostedMatchAllTextQueryAlloc(alloc, query_boost),
        .match_none_query => .{ .match_none = {} },
        .query_string_query => |query_string| try parseQueryStringTextQuery(
            alloc,
            query_string.query,
            query_boost,
            if (query_string_has_default_operator)
                try normalizeQueryStringDefaultOperator(query_string.default_operator)
            else
                null,
        ),
        .term_query => |term| .{ .term = .{
            .field = try alloc.dupe(u8, term.field orelse return error.UnsupportedQueryRequest),
            .term = try alloc.dupe(u8, term.term),
            .boost = query_boost,
        } },
        .match_query => |match| .{ .match = .{
            .field = try alloc.dupe(u8, match.field orelse return error.UnsupportedQueryRequest),
            .text = try alloc.dupe(u8, match.match),
            .analyzer = if (match.analyzer) |analyzer| try alloc.dupe(u8, analyzer) else null,
            .boost = query_boost,
        } },
        .multi_match_query => |multi_match| try public_text_query_mod.parseMultiMatchBoolPrefixQueryAlloc(
            alloc,
            multi_match.multi_match.query,
            multi_match.multi_match.type,
            multi_match.multi_match.fields,
            query_boost,
        ),
        .match_phrase_query => |phrase| blk: {
            const fuzziness = try parseBleveFuzziness(phrase.fuzziness, 0);
            break :blk .{ .match_phrase = .{
                .field = try alloc.dupe(u8, phrase.field orelse return error.UnsupportedQueryRequest),
                .text = try alloc.dupe(u8, phrase.match_phrase),
                .analyzer = if (phrase.analyzer) |analyzer| try alloc.dupe(u8, analyzer) else null,
                .max_edits = fuzziness.max_edits,
                .auto_fuzzy = fuzziness.auto_fuzzy,
                .boost = query_boost,
            } };
        },
        .phrase_query => |phrase| blk: {
            if (phrase.terms.len == 0) return error.InvalidQueryRequest;
            const fuzziness = try parseBleveFuzziness(phrase.fuzziness, 0);
            const field = try alloc.dupe(
                u8,
                phrase.field orelse return error.UnsupportedQueryRequest,
            );
            errdefer alloc.free(field);
            const terms = try cloneFields(alloc, phrase.terms);
            break :blk .{ .phrase = .{
                .field = field,
                .terms = terms,
                .max_edits = fuzziness.max_edits,
                .auto_fuzzy = fuzziness.auto_fuzzy,
                .boost = query_boost,
            } };
        },
        .multi_phrase_query => |phrase| blk: {
            if (phrase.terms.len == 0) return error.InvalidQueryRequest;
            for (phrase.terms) |group| {
                if (group.len == 0) return error.InvalidQueryRequest;
            }
            const fuzziness = try parseBleveFuzziness(phrase.fuzziness, 0);
            const field = try alloc.dupe(
                u8,
                phrase.field orelse return error.UnsupportedQueryRequest,
            );
            errdefer alloc.free(field);
            const terms = try cloneTextMatrixAlloc(alloc, phrase.terms);
            break :blk .{ .multi_phrase = .{
                .field = field,
                .terms = terms,
                .max_edits = fuzziness.max_edits,
                .auto_fuzzy = fuzziness.auto_fuzzy,
                .boost = query_boost,
            } };
        },
        .fuzzy_query => |fuzzy| blk: {
            const fuzziness = try parseBleveFuzziness(fuzzy.fuzziness, 1);
            const prefix_len = try parseBlevePrefixLength(fuzzy.prefix_length);
            break :blk .{ .fuzzy = .{
                .field = try alloc.dupe(u8, fuzzy.field orelse return error.UnsupportedQueryRequest),
                .term = try alloc.dupe(u8, fuzzy.term),
                .max_edits = fuzziness.max_edits,
                .prefix_len = prefix_len,
                .auto_fuzzy = fuzziness.auto_fuzzy,
                .boost = query_boost,
            } };
        },
        .prefix_query => |prefix| .{ .prefix = .{
            .field = try alloc.dupe(u8, prefix.field orelse return error.UnsupportedQueryRequest),
            .prefix = try alloc.dupe(u8, prefix.prefix),
            .boost = query_boost,
        } },
        .wildcard_query => |wildcard| .{ .wildcard = .{
            .field = try alloc.dupe(u8, wildcard.field orelse return error.UnsupportedQueryRequest),
            .pattern = try alloc.dupe(u8, wildcard.wildcard),
            .boost = query_boost,
        } },
        .regexp_query => |regexp| .{ .regexp = .{
            .field = try alloc.dupe(u8, regexp.field orelse return error.UnsupportedQueryRequest),
            .pattern = try alloc.dupe(u8, regexp.regexp),
            .boost = query_boost,
        } },
        .numeric_range_query => |range_query| .{ .numeric_range = .{
            .field = try alloc.dupe(u8, range_query.field orelse return error.UnsupportedQueryRequest),
            .min = range_query.min.valueOrNull(),
            .max = range_query.max.valueOrNull(),
            .inclusive_min = range_query.inclusive_min.valueOrNull() orelse true,
            .inclusive_max = range_query.inclusive_max.valueOrNull() orelse false,
            .boost = query_boost,
        } },
        .term_range_query => |range_query| .{ .term_range = .{
            .field = try alloc.dupe(u8, range_query.field orelse return error.UnsupportedQueryRequest),
            .min = if (range_query.min.valueOrNull()) |min| try alloc.dupe(u8, min) else null,
            .max = if (range_query.max.valueOrNull()) |max| try alloc.dupe(u8, max) else null,
            .inclusive_min = range_query.inclusive_min.valueOrNull() orelse true,
            .inclusive_max = range_query.inclusive_max.valueOrNull() orelse false,
            .boost = query_boost,
        } },
        .date_range_string_query => |range_query| blk: {
            if (range_query.datetime_parser != null) return error.UnsupportedQueryRequest;
            const field = try alloc.dupe(u8, range_query.field orelse return error.UnsupportedQueryRequest);
            errdefer alloc.free(field);
            break :blk .{ .date_range = .{
                .field = field,
                .start_ns = if (range_query.start) |start|
                    (try parseDateTimeOptionalToNs(start)) orelse return error.UnsupportedQueryRequest
                else
                    null,
                .end_ns = if (range_query.end) |end|
                    (try parseDateTimeOptionalToNs(end)) orelse return error.UnsupportedQueryRequest
                else
                    null,
                .inclusive_start = range_query.inclusive_start.valueOrNull() orelse true,
                .inclusive_end = range_query.inclusive_end.valueOrNull() orelse false,
                .boost = query_boost,
            } };
        },
        .doc_id_query => |doc_id| .{ .doc_id = .{
            .ids = try cloneFields(alloc, doc_id.ids),
            .boost = query_boost,
        } },
        .bool_field_query => |bool_field| .{ .bool_field = .{
            .field = try alloc.dupe(u8, bool_field.field orelse return error.UnsupportedQueryRequest),
            .value = bool_field.bool,
            .boost = query_boost,
        } },
        .boolean_query => |boolean_query| .{
            .bool_query = try parseGeneratedBleveBooleanQuery(alloc, boolean_query, query_boost),
        },
        .conjunction_query => |conjunction| .{
            .bool_query = .{
                .must = try parseGeneratedBleveQuerySlice(alloc, conjunction.conjuncts),
                .boost = query_boost,
            },
        },
        .disjunction_query => |disjunction| blk: {
            const min_should = try parsePublicMinimumShouldMatch(
                disjunction.min,
                disjunction.disjuncts.len,
            );
            break :blk .{ .bool_query = .{
                .should = try parseGeneratedBleveQuerySlice(alloc, disjunction.disjuncts),
                .min_should = min_should,
                .pure_should_optional = disjunction.min != null and
                    min_should == 0 and
                    disjunction.disjuncts.len > 0,
                .boost = query_boost,
            } };
        },
        .ip_range_query => |range| blk: {
            if (!validPublicIpv4Range(range.cidr)) {
                return error.InvalidQueryRequest;
            }
            const field = try alloc.dupe(
                u8,
                range.field orelse return error.UnsupportedQueryRequest,
            );
            errdefer alloc.free(field);
            const cidr = try alloc.dupe(u8, range.cidr);
            break :blk .{ .ip_range = .{
                .field = field,
                .cidr = cidr,
                .boost = query_boost,
            } };
        },
        .geo_bounding_box_query => |bbox| blk: {
            try validateGeoBoundingBox(
                bbox.min_lat,
                bbox.min_lon,
                bbox.max_lat,
                bbox.max_lon,
            );
            break :blk .{ .geo_bbox = .{
                .field = try alloc.dupe(u8, bbox.field),
                .min_lat = bbox.min_lat,
                .min_lon = bbox.min_lon,
                .max_lat = bbox.max_lat,
                .max_lon = bbox.max_lon,
                .boost = query_boost,
            } };
        },
        .geo_distance_query => |distance| blk: {
            if (distance.location.len != 2) return error.InvalidQueryRequest;
            const lon = distance.location[0];
            const lat = distance.location[1];
            try validateGeoPoint(lat, lon);
            const radius_meters = try parseGeoDistanceMeters(distance.distance);
            break :blk .{ .geo_distance = .{
                .field = try alloc.dupe(u8, distance.field orelse return error.UnsupportedQueryRequest),
                .lon = lon,
                .lat = lat,
                .radius_meters = radius_meters,
                .boost = query_boost,
            } };
        },
        .geo_bounding_polygon_query => |polygon| blk: {
            const polygons = try cloneGeneratedGeoPolygonAlloc(
                alloc,
                polygon.polygon_points,
            );
            errdefer freeGeoPolygons(alloc, polygons);
            break :blk .{ .geo_shape = .{
                .field = try alloc.dupe(
                    u8,
                    polygon.field orelse return error.UnsupportedQueryRequest,
                ),
                .polygons = polygons,
                .boost = query_boost,
            } };
        },
        .geo_shape_query => |shape| blk: {
            const relation = try parseGeoShapeRelation(shape.geometry.relation);
            const polygons = try parseGeneratedGeoShapePolygonsAlloc(
                alloc,
                shape.geometry.shape,
            );
            errdefer freeGeoPolygons(alloc, polygons);
            break :blk .{ .geo_shape = .{
                .field = try alloc.dupe(
                    u8,
                    shape.field orelse return error.UnsupportedQueryRequest,
                ),
                .relation = relation,
                .polygons = polygons,
                .boost = query_boost,
            } };
        },
    };
}

fn validateGeoPoint(lat: f64, lon: f64) !void {
    if (!std.math.isFinite(lat) or !std.math.isFinite(lon) or
        lat < -90 or lat > 90 or lon < -180 or lon > 180)
    {
        return error.InvalidQueryRequest;
    }
}

fn validPublicIpv4Range(text: []const u8) bool {
    const slash = std.mem.indexOfScalar(u8, text, '/');
    const address = if (slash) |index| text[0..index] else text;
    var octets = std.mem.splitScalar(u8, address, '.');
    var count: usize = 0;
    while (octets.next()) |octet| {
        if (count >= 4 or octet.len == 0) return false;
        _ = std.fmt.parseInt(u8, octet, 10) catch return false;
        count += 1;
    }
    if (count != 4) return false;
    if (slash) |index| {
        if (std.mem.indexOfScalar(u8, text[index + 1 ..], '/') != null) {
            return false;
        }
        const prefix = std.fmt.parseInt(u8, text[index + 1 ..], 10) catch
            return false;
        return prefix <= 32;
    }
    return true;
}

fn validateGeoBoundingBox(
    min_lat: f64,
    min_lon: f64,
    max_lat: f64,
    max_lon: f64,
) !void {
    try validateGeoPoint(min_lat, min_lon);
    try validateGeoPoint(max_lat, max_lon);
    if (min_lat > max_lat) return error.InvalidQueryRequest;
}

fn parseGeoDistanceMeters(text: []const u8) !f64 {
    const trimmed = std.mem.trim(u8, text, &std.ascii.whitespace);
    if (trimmed.len == 0) return error.InvalidQueryRequest;
    const units = [_]struct {
        suffix: []const u8,
        meters: f64,
    }{
        .{ .suffix = "mm", .meters = 0.001 },
        .{ .suffix = "cm", .meters = 0.01 },
        .{ .suffix = "km", .meters = 1_000 },
        .{ .suffix = "in", .meters = 0.0254 },
        .{ .suffix = "ft", .meters = 0.3048 },
        .{ .suffix = "yd", .meters = 0.9144 },
        .{ .suffix = "mi", .meters = 1_609.344 },
        .{ .suffix = "nm", .meters = 1_852 },
        .{ .suffix = "m", .meters = 1 },
    };
    for (units) |unit| {
        if (!std.mem.endsWith(u8, trimmed, unit.suffix)) continue;
        const number_text = std.mem.trim(
            u8,
            trimmed[0 .. trimmed.len - unit.suffix.len],
            &std.ascii.whitespace,
        );
        if (number_text.len == 0) return error.InvalidQueryRequest;
        const value = std.fmt.parseFloat(f64, number_text) catch
            return error.InvalidQueryRequest;
        const meters = value * unit.meters;
        if (!std.math.isFinite(meters) or meters < 0) {
            return error.InvalidQueryRequest;
        }
        return meters;
    }
    return error.InvalidQueryRequest;
}

fn parseGeoShapeRelation(text: []const u8) !db_mod.types.GeoShapeRelation {
    if (std.mem.eql(u8, text, "intersects")) return .intersects;
    if (std.mem.eql(u8, text, "within")) return .within;
    if (std.mem.eql(u8, text, "contains")) return .contains;
    return error.InvalidQueryRequest;
}

fn freeGeoPolygons(
    alloc: std.mem.Allocator,
    polygons: []const []const db_mod.types.GeoPoint,
) void {
    for (polygons) |polygon| {
        if (polygon.len > 0) alloc.free(polygon);
    }
    if (polygons.len > 0) alloc.free(polygons);
}

fn cloneGeneratedGeoPolygonAlloc(
    alloc: std.mem.Allocator,
    source: []const query_openapi.GeoPoint,
) ![][]const db_mod.types.GeoPoint {
    if (source.len < 3) return error.InvalidQueryRequest;
    const polygon = try alloc.alloc(db_mod.types.GeoPoint, source.len);
    errdefer alloc.free(polygon);
    for (source, 0..) |point, index| {
        const lat = point.lat orelse return error.InvalidQueryRequest;
        const lon = point.lon orelse return error.InvalidQueryRequest;
        try validateGeoPoint(lat, lon);
        polygon[index] = .{ .lat = lat, .lon = lon };
    }
    const polygons = try alloc.alloc([]const db_mod.types.GeoPoint, 1);
    polygons[0] = polygon;
    return polygons;
}

fn jsonNumberAsF64(value: std.json.Value) !f64 {
    const number: f64 = switch (value) {
        .integer => |item| @floatFromInt(item),
        .float => |item| item,
        .number_string => |item| std.fmt.parseFloat(f64, item) catch
            return error.InvalidQueryRequest,
        else => return error.InvalidQueryRequest,
    };
    if (!std.math.isFinite(number)) return error.InvalidQueryRequest;
    return number;
}

fn parseGeoJsonRingAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
) ![]const db_mod.types.GeoPoint {
    if (value != .array or value.array.items.len < 3) {
        return error.InvalidQueryRequest;
    }
    const points = try alloc.alloc(
        db_mod.types.GeoPoint,
        value.array.items.len,
    );
    errdefer alloc.free(points);
    for (value.array.items, 0..) |position, index| {
        if (position != .array or position.array.items.len != 2) {
            return error.InvalidQueryRequest;
        }
        const lon = try jsonNumberAsF64(position.array.items[0]);
        const lat = try jsonNumberAsF64(position.array.items[1]);
        try validateGeoPoint(lat, lon);
        points[index] = .{ .lat = lat, .lon = lon };
    }
    return points;
}

fn parseGeneratedGeoShapePolygonsAlloc(
    alloc: std.mem.Allocator,
    shape: query_openapi.GeoShape,
) ![][]const db_mod.types.GeoPoint {
    if (std.mem.eql(u8, shape.type, "Polygon")) {
        // The execution model stores a union of polygons and cannot represent
        // GeoJSON interior rings without changing their semantics.
        if (shape.coordinates.len != 1) return error.UnsupportedQueryRequest;
        const polygon = try parseGeoJsonRingAlloc(alloc, shape.coordinates[0]);
        errdefer alloc.free(polygon);
        const polygons = try alloc.alloc([]const db_mod.types.GeoPoint, 1);
        polygons[0] = polygon;
        return polygons;
    }
    if (!std.mem.eql(u8, shape.type, "MultiPolygon") or
        shape.coordinates.len == 0)
    {
        return error.UnsupportedQueryRequest;
    }
    const polygons = try alloc.alloc(
        []const db_mod.types.GeoPoint,
        shape.coordinates.len,
    );
    var initialized: usize = 0;
    errdefer {
        for (polygons[0..initialized]) |polygon| alloc.free(polygon);
        alloc.free(polygons);
    }
    for (shape.coordinates, 0..) |polygon_value, index| {
        if (polygon_value != .array or polygon_value.array.items.len != 1) {
            return error.UnsupportedQueryRequest;
        }
        polygons[index] = try parseGeoJsonRingAlloc(
            alloc,
            polygon_value.array.items[0],
        );
        initialized += 1;
    }
    return polygons;
}

fn parseQueryStringTextQuery(
    alloc: std.mem.Allocator,
    input: []const u8,
    boost: f32,
    default_operator: ?[]const u8,
) !db_mod.types.TextQuery {
    const parsed_default_operator = if (default_operator) |value|
        if (std.ascii.eqlIgnoreCase(value, "or"))
            public_query_string_mod.ParseOptions{ .default_operator = .or_ }
        else if (std.ascii.eqlIgnoreCase(value, "and"))
            public_query_string_mod.ParseOptions{ .default_operator = .and_ }
        else
            return error.UnsupportedQueryRequest
    else
        public_query_string_mod.ParseOptions{};

    var owned = try public_query_string_mod.parseFilterAllocWithOptions(alloc, input, parsed_default_operator);
    defer owned.deinit(alloc);
    return try public_query_string_mod.filterToStatefulTextQueryAlloc(alloc, owned.filter, boost);
}

fn normalizeQueryStringDefaultOperator(value: anytype) !?[]const u8 {
    const T = @TypeOf(value);
    return switch (@typeInfo(T)) {
        .optional => if (value) |inner| try normalizeQueryStringDefaultOperator(inner) else null,
        .pointer => |pointer| switch (pointer.size) {
            .slice => value,
            else => error.UnsupportedQueryRequest,
        },
        .@"enum" => @tagName(value),
        else => error.UnsupportedQueryRequest,
    };
}

const ParsedFuzziness = struct {
    max_edits: u8,
    auto_fuzzy: bool,
};

fn parseBleveFuzziness(value: ?query_openapi.Fuzziness, default_edits: u8) !ParsedFuzziness {
    if (value == null) return .{ .max_edits = default_edits, .auto_fuzzy = false };
    return switch (value.?) {
        .integer => |int_value| {
            if (int_value < 0 or int_value > 2) return error.InvalidQueryRequest;
            return .{ .max_edits = @intCast(int_value), .auto_fuzzy = false };
        },
        .string => |str_value| {
            if (!std.mem.eql(u8, str_value, "auto")) return error.UnsupportedQueryRequest;
            return .{ .max_edits = default_edits, .auto_fuzzy = true };
        },
        else => error.UnsupportedQueryRequest,
    };
}

fn parseBlevePrefixLength(value: ?i32) !u8 {
    const prefix_length = value orelse return 0;
    if (prefix_length < 0 or prefix_length > std.math.maxInt(u8)) {
        return error.InvalidQueryRequest;
    }
    return @intCast(prefix_length);
}

fn parseDateTimeOptionalToNs(text: []const u8) !?u64 {
    if (try parseRfc3339ToNs(text)) |ts| return ts;
    return rfc3339.parseDateToUnixNs(text);
}

fn parseRfc3339ToNs(text: []const u8) !?u64 {
    return rfc3339.parseToUnixNs(text);
}

fn buildSemanticVectorQueries(
    alloc: std.mem.Allocator,
    semantic_resolver: ?SemanticResolver,
    table_name: []const u8,
    request: anytype,
    limit: u32,
) !NamedVectorQueries {
    if (request.semantic_search == null and request.embeddings == null) return .{};

    var parsed_embeddings = try public_search_request_mod.parseEmbeddingsAlloc(alloc, request, limit);
    defer parsed_embeddings.deinit(alloc);
    const index_names = (try public_search_request_mod.cloneRequestedIndexesAlloc(alloc, request, parsed_embeddings)) orelse
        return error.UnsupportedQueryRequest;
    defer {
        for (index_names) |index_name| alloc.free(index_name);
        alloc.free(index_names);
    }

    if (index_names.len == 0) return error.UnsupportedQueryRequest;

    var dense_queries = std.ArrayListUnmanaged(db_mod.types.NamedDenseQuery).empty;
    errdefer freeNamedDenseQueries(alloc, dense_queries.items);
    var sparse_queries = std.ArrayListUnmanaged(db_mod.types.NamedSparseQuery).empty;
    errdefer freeNamedSparseQueries(alloc, sparse_queries.items);

    for (index_names) |index_name| {
        if (parsed_embeddings.find(index_name)) |embedding| {
            switch (embedding.query) {
                .dense => |dense_query| try dense_queries.append(alloc, .{
                    .name = try alloc.dupe(u8, index_name),
                    .index_name = try alloc.dupe(u8, index_name),
                    .query = .{
                        .vector = try alloc.dupe(f32, dense_query.vector),
                        .k = dense_query.k,
                    },
                }),
                .sparse => |sparse_query| try sparse_queries.append(alloc, .{
                    .name = try alloc.dupe(u8, index_name),
                    .index_name = try alloc.dupe(u8, index_name),
                    .query = .{
                        .indices = try alloc.dupe(u32, sparse_query.indices),
                        .values = try alloc.dupe(f32, sparse_query.values),
                        .k = sparse_query.k,
                    },
                }),
            }
            continue;
        }
        if (request.semantic_search) |semantic_search| {
            const resolver = semantic_resolver orelse return error.UnsupportedQueryRequest;
            try dense_queries.append(alloc, .{
                .name = try alloc.dupe(u8, index_name),
                .index_name = try alloc.dupe(u8, index_name),
                .query = try resolver.resolveDenseQuery(alloc, table_name, index_name, semantic_search, request.embedding_template, limit),
            });
            continue;
        }
        return error.UnsupportedQueryRequest;
    }

    return .{
        .dense = try dense_queries.toOwnedSlice(alloc),
        .sparse = try sparse_queries.toOwnedSlice(alloc),
    };
}

fn buildPreflightSemanticVectorQueries(
    alloc: std.mem.Allocator,
    request: anytype,
    limit: u32,
) !NamedVectorQueries {
    if (request.semantic_search == null and request.embeddings == null) return .{};

    var parsed_embeddings = try public_search_request_mod.parseEmbeddingsAlloc(alloc, request, limit);
    defer parsed_embeddings.deinit(alloc);
    const index_names = (try public_search_request_mod.cloneRequestedIndexesAlloc(alloc, request, parsed_embeddings)) orelse
        return error.UnsupportedQueryRequest;
    defer {
        for (index_names) |index_name| alloc.free(index_name);
        alloc.free(index_names);
    }

    if (index_names.len == 0) return error.UnsupportedQueryRequest;

    var dense_queries = std.ArrayListUnmanaged(db_mod.types.NamedDenseQuery).empty;
    errdefer freeNamedDenseQueries(alloc, dense_queries.items);
    var sparse_queries = std.ArrayListUnmanaged(db_mod.types.NamedSparseQuery).empty;
    errdefer freeNamedSparseQueries(alloc, sparse_queries.items);

    for (index_names) |index_name| {
        if (parsed_embeddings.find(index_name)) |embedding| {
            switch (embedding.query) {
                .dense => |dense_query| try dense_queries.append(alloc, .{
                    .name = try alloc.dupe(u8, index_name),
                    .index_name = try alloc.dupe(u8, index_name),
                    .query = .{
                        .vector = try alloc.dupe(f32, dense_query.vector),
                        .k = dense_query.k,
                    },
                }),
                .sparse => |sparse_query| try sparse_queries.append(alloc, .{
                    .name = try alloc.dupe(u8, index_name),
                    .index_name = try alloc.dupe(u8, index_name),
                    .query = .{
                        .indices = try alloc.dupe(u32, sparse_query.indices),
                        .values = try alloc.dupe(f32, sparse_query.values),
                        .k = sparse_query.k,
                    },
                }),
            }
            continue;
        }
        if (request.semantic_search != null) {
            try dense_queries.append(alloc, .{
                .name = try alloc.dupe(u8, index_name),
                .index_name = try alloc.dupe(u8, index_name),
                .query = .{
                    .vector = try alloc.alloc(f32, 0),
                    .k = limit,
                },
            });
            continue;
        }
        return error.UnsupportedQueryRequest;
    }

    return .{
        .dense = try dense_queries.toOwnedSlice(alloc),
        .sparse = try sparse_queries.toOwnedSlice(alloc),
    };
}

fn buildGraphQueries(
    alloc: std.mem.Allocator,
    request: anytype,
) ![]const db_mod.types.NamedGraphQuery {
    if (comptime @hasField(@TypeOf(request), "graph_searches")) {
        if (request.graph_queries != null and request.graph_searches != null)
            return error.InvalidQueryRequest;
        if (request.graph_queries != null and request.expand_strategy != null)
            return error.InvalidQueryRequest;
    }
    if (request.graph_queries) |queries| {
        if (queries.map.count() > graph_query_mod.max_named_queries) return error.InvalidQueryRequest;
    }

    var items = std.ArrayListUnmanaged(db_mod.types.NamedGraphQuery).empty;
    errdefer {
        for (items.items) |item| {
            alloc.free(item.name);
            freeGraphQuery(alloc, item.query);
        }
        items.deinit(alloc);
    }

    if (request.graph_queries) |graph_queries| {
        var it = graph_queries.map.iterator();
        while (it.next()) |entry| {
            if (!graph_query_mod.isValidQueryName(entry.key_ptr.*)) return error.InvalidQueryRequest;
            const name = try alloc.dupe(u8, entry.key_ptr.*);
            var name_owned = true;
            errdefer if (name_owned) alloc.free(name);
            const query = try parseGraphQuery(alloc, entry.value_ptr.*);
            var query_owned = true;
            errdefer if (query_owned) freeGraphQuery(alloc, query);
            try items.append(alloc, .{ .name = name, .query = query });
            name_owned = false;
            query_owned = false;
        }
    } else if (comptime @hasField(@TypeOf(request), "graph_searches")) {
        if (request.graph_searches) |graph_searches| {
            var it = graph_searches.map.iterator();
            while (it.next()) |entry| {
                const name = try alloc.dupe(u8, entry.key_ptr.*);
                var name_owned = true;
                errdefer if (name_owned) alloc.free(name);
                const query = try parseLegacyGraphQuery(alloc, entry.value_ptr.*);
                var query_owned = true;
                errdefer if (query_owned) freeGraphQuery(alloc, query);
                try items.append(alloc, .{ .name = name, .query = query });
                name_owned = false;
                query_owned = false;
            }
        }
    }
    return try items.toOwnedSlice(alloc);
}

pub fn parseGraphQuery(
    alloc: std.mem.Allocator,
    query: indexes_openapi.GraphQuery,
) !graph_query_mod.GraphQuery {
    return switch (query) {
        .graph_match_query => |value| try parseGraphMatchQuery(alloc, value.*),
        .graph_traverse_query => |value| try parseGraphTraverseQuery(alloc, value.*),
        .graph_shortest_path_query => |value| try parseGraphPathQuery(alloc, value.index, value.shortest_path, 1, .shortest_path),
        .graph_k_shortest_paths_query => |value| try parseGraphPathQuery(
            alloc,
            value.index,
            value.k_shortest_paths,
            std.math.cast(u32, value.k_shortest_paths.k) orelse return error.InvalidQueryRequest,
            .k_shortest_paths,
        ),
    };
}

pub fn parseLegacyGraphQuery(
    alloc: std.mem.Allocator,
    query: indexes_openapi.LegacyGraphQuery,
) !graph_query_mod.GraphQuery {
    const params = try parseLegacyGraphQueryParams(alloc, query.params);
    errdefer freeGraphQueryParams(alloc, params);
    const index_name = try alloc.dupe(u8, query.index_name);
    errdefer alloc.free(index_name);
    const start_nodes = try parseLegacyGraphNodeSelector(alloc, query.start_nodes orelse return error.UnsupportedQueryRequest);
    errdefer freeGraphNodeSelector(alloc, start_nodes);
    const target_nodes = if (query.target_nodes) |selector|
        try parseLegacyGraphNodeSelector(alloc, selector)
    else
        null;
    errdefer if (target_nodes) |selector| freeGraphNodeSelector(alloc, selector);
    const pattern = if (query.pattern) |steps|
        try parseLegacyPatternSteps(alloc, steps)
    else
        @constCast((&[_]graph_pattern_mod.PatternStep{})[0..]);
    errdefer freePatternSteps(alloc, pattern);
    const return_aliases = if (query.return_aliases) |aliases|
        try cloneFields(alloc, aliases)
    else
        @constCast((&[_][]const u8{})[0..]);
    errdefer freeOwnedStringSlice(alloc, return_aliases);
    if (query.include_edges == true) return error.UnsupportedQueryRequest;
    const fields = if (query.fields) |requested_fields|
        try cloneFields(alloc, requested_fields)
    else
        @constCast((&[_][]const u8{})[0..]);
    errdefer freeOwnedStringSlice(alloc, fields);

    if (query.type == .pattern) {
        if (pattern.len == 0) return error.UnsupportedQueryRequest;
    } else if (pattern.len > 0 or return_aliases.len > 0) {
        return error.UnsupportedQueryRequest;
    }

    const k: u32 = if (query.params) |graph_params|
        if (graph_params.k) |raw| blk: {
            const value = std.math.cast(u32, raw) orelse return error.InvalidQueryRequest;
            if (value < 1 or value > 100) return error.InvalidQueryRequest;
            break :blk value;
        } else 1
    else
        1;

    return .{
        .query_type = switch (query.type) {
            .traverse => .traverse,
            .neighbors => .neighbors,
            .shortest_path => .shortest_path,
            .k_shortest_paths => .k_shortest_paths,
            .pattern => .pattern,
        },
        .index_name = index_name,
        .start_nodes = start_nodes,
        .params = params,
        .target_nodes = target_nodes,
        .k = k,
        .pattern = pattern,
        .return_aliases = return_aliases,
        .include_documents = query.include_documents orelse false,
        .fields = fields,
        .include_all_fields = query.fields == null,
    };
}

fn parseLegacyPatternSteps(
    alloc: std.mem.Allocator,
    value: []const indexes_openapi.PatternStep,
) ![]const graph_pattern_mod.PatternStep {
    if (value.len == 0 or value.len > graph_pattern_mod.max_pattern_steps)
        return error.InvalidQueryRequest;
    const steps = try alloc.alloc(graph_pattern_mod.PatternStep, value.len);
    var initialized: usize = 0;
    errdefer {
        for (steps[0..initialized]) |step| {
            alloc.free(step.alias);
            freePatternNodeFilter(alloc, step.node_filter);
            freeOwnedStringSlice(alloc, step.edge.types);
        }
        alloc.free(steps);
    }

    for (value, 0..) |step, i| {
        const requested_edge_types = if (step.edge) |edge|
            edge.types orelse &.{}
        else
            &.{};
        graph_query_mod.validateEdgeTypes(requested_edge_types) catch return error.InvalidQueryRequest;
        const edge_types = try cloneFields(alloc, requested_edge_types);
        errdefer freeOwnedStringSlice(alloc, edge_types);
        steps[i] = .{
            .alias = try alloc.dupe(u8, step.alias orelse ""),
            .node_filter = try parseLegacyPatternNodeFilter(alloc, step.node_filter),
            .edge = if (step.edge) |edge| .{
                .direction = if (edge.direction) |direction| switch (direction) {
                    .out => .out,
                    .in => .in,
                    .both => .both,
                } else .out,
                .min_hops = if (edge.min_hops) |raw| std.math.cast(u32, raw) orelse return error.InvalidQueryRequest else 1,
                .max_hops = if (edge.max_hops) |raw| std.math.cast(u32, raw) orelse return error.InvalidQueryRequest else 1,
                .min_weight = legacyWeightBound(edge.min_weight),
                .max_weight = legacyWeightBound(edge.max_weight),
                .types = edge_types,
            } else .{ .types = edge_types },
        };
        if (step.edge != null and (steps[i].edge.min_hops == 0 or
            steps[i].edge.max_hops == 0 or
            steps[i].edge.min_hops > steps[i].edge.max_hops or
            steps[i].edge.max_hops > graph_pattern_mod.max_pattern_hops))
            return error.InvalidQueryRequest;
        initialized += 1;
    }
    return steps;
}

fn parseLegacyPatternNodeFilter(
    alloc: std.mem.Allocator,
    filter: ?indexes_openapi.NodeFilter,
) !graph_pattern_mod.NodeFilter {
    const value = filter orelse return .{};
    var out: graph_pattern_mod.NodeFilter = .{};
    errdefer freePatternNodeFilter(alloc, out);
    if (value.filter_prefix) |prefix| out.filter_prefix = try alloc.dupe(u8, prefix);
    if (value.filter_query) |filter_query| {
        const parsed = try parseSupportedFullTextQuery(alloc, .{ .object = filter_query.map }, 10);
        defer freeTextQuery(alloc, parsed);
        out.filter_query_json = try encodePatternFilterQuery(alloc, parsed);
    }
    return out;
}

fn parseLegacyGraphQueryParams(
    alloc: std.mem.Allocator,
    params: ?indexes_openapi.GraphQueryParams,
) !graph_query_mod.QueryParams {
    const value = params orelse return .{};
    if (value.algorithm != null or value.algorithm_params != null) return error.UnsupportedQueryRequest;
    const max_depth = if (value.max_depth) |raw| std.math.cast(u32, raw) orelse return error.InvalidQueryRequest else 3;
    const max_results = if (value.max_results) |raw| std.math.cast(u32, raw) orelse return error.InvalidQueryRequest else 100;
    if (max_depth < 1 or max_depth > 64 or max_results < 1 or max_results > 10_000)
        return error.InvalidQueryRequest;
    const node_filter = try parseLegacyPatternNodeFilter(alloc, value.node_filter);
    errdefer freePatternNodeFilter(alloc, node_filter);
    const requested_edge_types = value.edge_types orelse &.{};
    graph_query_mod.validateEdgeTypes(requested_edge_types) catch return error.InvalidQueryRequest;
    const edge_types = try cloneFields(alloc, requested_edge_types);
    return .{
        .edge_types = edge_types,
        .direction = if (value.direction) |direction| switch (direction) {
            .out => .out,
            .in => .in,
            .both => .both,
        } else .out,
        .max_depth = max_depth,
        .max_results = max_results,
        .min_weight = legacyWeightBound(value.min_weight),
        .max_weight = legacyWeightBound(value.max_weight),
        .deduplicate = value.deduplicate_nodes orelse true,
        .include_paths = value.include_paths orelse false,
        .weight_mode = if (value.weight_mode) |mode| switch (mode) {
            .min_hops => .min_hops,
            .min_weight => .min_weight,
            .max_weight => .max_weight,
        } else .min_hops,
        .node_filter = node_filter,
    };
}

fn parseGraphTraverseQuery(alloc: std.mem.Allocator, value: indexes_openapi.GraphTraverseQuery) !graph_query_mod.GraphQuery {
    const traversal = value.traverse;
    try validateGraphIndexName(value.index);
    const weight_bounds = try parseGraphEdgeWeightRange(traversal.edge_weight);
    if (traversal.fields != null and traversal.include_documents != true)
        return error.InvalidQueryRequest;
    const index = try alloc.dupe(u8, value.index);
    errdefer alloc.free(index);
    const start = try parseGraphNodeSelector(alloc, traversal.start);
    errdefer freeGraphNodeSelector(alloc, start);
    const requested_edge_types = traversal.edge_types orelse &.{};
    graph_query_mod.validateEdgeTypes(requested_edge_types) catch return error.InvalidQueryRequest;
    const edge_types = try cloneFields(alloc, requested_edge_types);
    errdefer freeOwnedStringSlice(alloc, edge_types);
    const filter = try parseGraphFilterValue(alloc, traversal.filter);
    errdefer freePatternNodeFilter(alloc, filter);
    const fields = if (traversal.fields) |items| try cloneFields(alloc, items) else &.{};
    errdefer freeOwnedStringSlice(alloc, fields);
    return .{
        .query_type = .traverse,
        .index_name = index,
        .start_nodes = start,
        .params = .{
            .edge_types = edge_types,
            .direction = parseGraphDirection(traversal.direction),
            .max_depth = try parseGraphBoundedU32(traversal.max_depth, 1, 1, 64),
            .max_results = try parseGraphBoundedU32(traversal.limit, 100, 1, 10_000),
            .min_weight = weight_bounds.min,
            .max_weight = weight_bounds.max,
            .deduplicate = true,
            .include_paths = traversal.include_paths orelse false,
            .node_filter = filter,
        },
        .include_documents = traversal.include_documents orelse false,
        .fields = fields,
        .include_all_fields = traversal.fields == null,
    };
}

fn parseGraphPathQuery(
    alloc: std.mem.Allocator,
    index_value: []const u8,
    path: anytype,
    k: u32,
    query_type: graph_query_mod.QueryType,
) !graph_query_mod.GraphQuery {
    if (k == 0 or k > 100) return error.InvalidQueryRequest;
    try validateGraphIndexName(index_value);
    const weight_bounds = try parseGraphEdgeWeightRange(path.edge_weight);
    if (path.fields != null and path.include_documents != true)
        return error.InvalidQueryRequest;
    const index = try alloc.dupe(u8, index_value);
    errdefer alloc.free(index);
    const starts = try parseGraphPathEndpointSelector(alloc, path.from);
    errdefer freeGraphNodeSelector(alloc, starts);
    const targets = try parseGraphPathEndpointSelector(alloc, path.to);
    errdefer freeGraphNodeSelector(alloc, targets);
    const requested_edge_types = path.edge_types orelse &.{};
    graph_query_mod.validateEdgeTypes(requested_edge_types) catch return error.InvalidQueryRequest;
    const edge_types = try cloneFields(alloc, requested_edge_types);
    errdefer freeOwnedStringSlice(alloc, edge_types);
    const filter = try parseGraphFilterValue(alloc, path.filter);
    errdefer freePatternNodeFilter(alloc, filter);
    const fields = if (path.fields) |items| try cloneFields(alloc, items) else &.{};
    errdefer freeOwnedStringSlice(alloc, fields);
    return .{
        .query_type = query_type,
        .index_name = index,
        .start_nodes = starts,
        .target_nodes = targets,
        .k = k,
        .params = .{
            .edge_types = edge_types,
            .direction = parseGraphDirection(path.direction),
            .max_depth = try parseGraphBoundedU32(path.max_depth, 10, 1, 64),
            .min_weight = weight_bounds.min,
            .max_weight = weight_bounds.max,
            .weight_mode = if (path.objective) |objective| switch (objective) {
                .min_hops => .min_hops,
                .min_weight_sum => .min_weight,
                .max_weight_product => .max_weight,
            } else .min_hops,
            .node_filter = filter,
        },
        .include_documents = path.include_documents orelse false,
        .fields = fields,
        .include_all_fields = path.fields == null,
    };
}

fn parseGraphPathEndpointSelector(alloc: std.mem.Allocator, endpoint: anytype) !graph_query_mod.NodeSelector {
    if (endpoint.key.len == 0) return error.InvalidQueryRequest;
    if (endpoint.table) |table| if (!graph_pattern_mod.isValidTableQualifier(table)) return error.InvalidQueryRequest;
    const identities = try alloc.alloc(graph_query_mod.NodeIdentity, 1);
    errdefer alloc.free(identities);
    const key = try alloc.dupe(u8, endpoint.key);
    errdefer alloc.free(key);
    const table = if (endpoint.table) |value| try alloc.dupe(u8, value) else null;
    errdefer if (table) |value| alloc.free(value);
    identities[0] = .{ .key = key, .table = table };
    return .{ .identities = identities };
}

fn parseGraphMatchQuery(alloc: std.mem.Allocator, value: indexes_openapi.GraphMatchQuery) !graph_query_mod.GraphQuery {
    try validateGraphIndexName(value.index);
    const index = try alloc.dupe(u8, value.index);
    errdefer alloc.free(index);
    const base_ref = try alloc.dupe(u8, "$query_results");
    errdefer alloc.free(base_ref);
    const nodes = try parseGraphMatchNodes(alloc, value.match.nodes);
    errdefer freeGraphMatchNodes(alloc, nodes);
    const edges = try parseGraphMatchEdges(alloc, value.match.edges);
    errdefer freeGraphMatchEdges(alloc, edges);
    const predicates = try parseGraphWherePredicates(alloc, value.match.where);
    errdefer freeGraphMatchPredicates(alloc, predicates);
    const optional = if (value.match.optional) |groups| try parseGraphOptionalPatterns(alloc, groups) else &.{};
    errdefer freeGraphOptionalPatterns(alloc, optional);

    const bindings_return: ?*const indexes_openapi.GraphBindingsReturn = switch (value.@"return") {
        .graph_bindings_return => |return_value| return_value,
        .graph_aggregates_return => null,
    };
    const aggregates_return: ?*const indexes_openapi.GraphAggregatesReturn = switch (value.@"return") {
        .graph_bindings_return => null,
        .graph_aggregates_return => |return_value| return_value,
    };
    if (bindings_return) |return_value| {
        const items = return_value.bindings;
        if (items.len == 0 or items.len > graph_pattern_mod.max_conjunctive_nodes)
            return error.InvalidQueryRequest;
        for (items, 0..) |alias, i| {
            if (!graph_query_mod.isValidIdentifier(alias)) return error.InvalidQueryRequest;
            if (!graphAliasIsDeclared(nodes, optional, alias)) return error.InvalidQueryRequest;
            for (items[0..i]) |prior| {
                if (std.mem.eql(u8, prior, alias)) return error.InvalidQueryRequest;
            }
        }
    }
    if (aggregates_return) |return_value| {
        const items = return_value.aggregates;
        var aggregate_it = items.map.iterator();
        while (aggregate_it.next()) |entry| {
            const aggregate = try graphCountAggregateParts(entry.value_ptr.*);
            if (!std.mem.eql(u8, aggregate.of, "*") and
                !graphAliasIsDeclared(nodes, optional, aggregate.of))
            {
                return error.InvalidQueryRequest;
            }
        }
    }
    const aliases = if (bindings_return) |return_value| try cloneFields(alloc, return_value.bindings) else &.{};
    errdefer freeOwnedStringSlice(alloc, aliases);
    const aggregates = if (aggregates_return) |return_value| try parseGraphCountAggregates(alloc, return_value.aggregates) else &.{};
    errdefer freeGraphCountAggregates(alloc, aggregates);
    const limit = if (bindings_return) |return_value| try parseGraphBoundedU32(return_value.limit, 100, 1, 10_000) else 0;
    if (bindings_return) |return_value| {
        if (return_value.fields != null and return_value.include_documents != true)
            return error.InvalidQueryRequest;
        if (return_value.include_documents == true) {
            const hydrated_bindings = std.math.mul(
                usize,
                @as(usize, limit),
                return_value.bindings.len,
            ) catch return error.InvalidQueryRequest;
            if (hydrated_bindings > public_limits.max_graph_hydrated_bindings)
                return error.InvalidQueryRequest;
        }
    }
    const fields = if (bindings_return) |return_value|
        if (return_value.fields) |items| try cloneFields(alloc, items) else &.{}
    else
        &.{};
    errdefer freeOwnedStringSlice(alloc, fields);
    const anchor_alias = for (nodes) |node| {
        if (std.mem.eql(u8, node.alias, value.match.anchor)) break node.alias;
    } else return error.InvalidQueryRequest;
    const match_pattern = graph_pattern_mod.ConjunctivePattern{
        // Keep the anchor tied to the cloned required-node storage. The
        // generated request parser may borrow its input or own an unescaped
        // copy, and both lifetimes end before the owned search request does.
        .anchor_alias = anchor_alias,
        .nodes = nodes,
        .edges = edges,
        .predicates = predicates,
        .optional = optional,
    };
    graph_pattern_mod.validateConjunctivePattern(match_pattern) catch return error.InvalidQueryRequest;
    return .{
        .query_type = .pattern,
        .index_name = index,
        .start_nodes = .{ .result_ref = .{ .ref = base_ref, .limit = 0 } },
        .params = .{ .max_results = limit },
        .match_pattern = match_pattern,
        .return_aliases = aliases,
        .return_limit = limit,
        .aggregates = aggregates,
        .include_documents = if (bindings_return) |return_value| return_value.include_documents orelse false else false,
        .fields = fields,
        .include_all_fields = if (bindings_return) |return_value| return_value.fields == null else true,
    };
}

fn validateGraphIndexName(index: []const u8) !void {
    if (std.mem.trim(u8, index, " \t\r\n").len == 0) return error.InvalidQueryRequest;
}

test "canonical graph index names are nonblank" {
    try validateGraphIndexName("social");
    try std.testing.expectError(error.InvalidQueryRequest, validateGraphIndexName(""));
    try std.testing.expectError(error.InvalidQueryRequest, validateGraphIndexName(" \t"));
}

fn graphAliasIsDeclared(
    nodes: []const graph_pattern_mod.MatchNode,
    optional: []const graph_pattern_mod.OptionalPattern,
    alias: []const u8,
) bool {
    for (nodes) |node| if (std.mem.eql(u8, node.alias, alias)) return true;
    for (optional) |group| {
        for (group.nodes) |node| if (std.mem.eql(u8, node.alias, alias)) return true;
    }
    return false;
}

fn parseGraphMatchNodes(alloc: std.mem.Allocator, value: std.json.ArrayHashMap(indexes_openapi.GraphMatchNode)) ![]const graph_pattern_mod.MatchNode {
    if (value.map.count() == 0 or value.map.count() > graph_pattern_mod.max_pattern_steps) return error.InvalidQueryRequest;
    const nodes = try alloc.alloc(graph_pattern_mod.MatchNode, value.map.count());
    var initialized: usize = 0;
    errdefer {
        for (nodes[0..initialized]) |node| {
            alloc.free(node.alias);
            if (node.table) |table| alloc.free(table);
            freePatternNodeFilter(alloc, node.filter);
        }
        alloc.free(nodes);
    }
    var it = value.map.iterator();
    while (it.next()) |entry| {
        if (!graph_query_mod.isValidIdentifier(entry.key_ptr.*)) return error.InvalidQueryRequest;
        const alias = try alloc.dupe(u8, entry.key_ptr.*);
        errdefer alloc.free(alias);
        const table = if (entry.value_ptr.table) |table_name| blk: {
            if (!graph_pattern_mod.isValidTableQualifier(table_name)) return error.InvalidQueryRequest;
            break :blk try alloc.dupe(u8, table_name);
        } else null;
        errdefer if (table) |table_name| alloc.free(table_name);
        const filter = try parseGraphFilterValue(alloc, entry.value_ptr.filter);
        errdefer freePatternNodeFilter(alloc, filter);
        nodes[initialized] = .{
            .alias = alias,
            .table = table,
            .filter = filter,
        };
        initialized += 1;
    }
    return nodes;
}

fn parseGraphMatchEdges(alloc: std.mem.Allocator, value: []const indexes_openapi.GraphMatchEdge) ![]const graph_pattern_mod.MatchEdge {
    if (value.len > graph_pattern_mod.max_conjunctive_edges) return error.InvalidQueryRequest;
    const edges = try alloc.alloc(graph_pattern_mod.MatchEdge, value.len);
    var initialized: usize = 0;
    errdefer {
        for (edges[0..initialized]) |edge| {
            alloc.free(edge.from);
            alloc.free(edge.to);
            freeOwnedStringSlice(alloc, edge.step.types);
        }
        alloc.free(edges);
    }
    for (value, 0..) |edge, i| {
        const weight_bounds = try parseGraphEdgeWeightRange(edge.edge_weight);
        if (!graph_query_mod.isValidIdentifier(edge.from) or
            !graph_query_mod.isValidIdentifier(edge.to))
            return error.InvalidQueryRequest;
        const from = try alloc.dupe(u8, edge.from);
        errdefer alloc.free(from);
        const to = try alloc.dupe(u8, edge.to);
        errdefer alloc.free(to);
        const requested_types = edge.types orelse &.{};
        graph_query_mod.validateEdgeTypes(requested_types) catch return error.InvalidQueryRequest;
        const types = try cloneFields(alloc, requested_types);
        errdefer freeOwnedStringSlice(alloc, types);
        edges[i] = .{
            .from = from,
            .to = to,
            .step = .{
                .types = types,
                .direction = parseGraphDirection(edge.direction),
                .min_hops = try parseGraphBoundedU32(edge.min_hops, 1, 1, graph_pattern_mod.max_pattern_hops),
                .max_hops = try parseGraphBoundedU32(edge.max_hops, 1, 1, graph_pattern_mod.max_pattern_hops),
                .min_weight = weight_bounds.min,
                .max_weight = weight_bounds.max,
            },
        };
        if (edges[i].step.min_hops > edges[i].step.max_hops) return error.InvalidQueryRequest;
        initialized += 1;
    }
    return edges;
}

fn legacyWeightBound(value: ?f64) ?f64 {
    const bound = value orelse return null;
    return if (bound > 0 and std.math.isFinite(bound)) bound else null;
}

fn validateGraphWeightBounds(min_weight: ?f64, max_weight: ?f64) !void {
    if (min_weight) |value| if (!std.math.isFinite(value) or value < 0) return error.InvalidQueryRequest;
    if (max_weight) |value| if (!std.math.isFinite(value) or value < 0) return error.InvalidQueryRequest;
    if (min_weight != null and max_weight != null and min_weight.? > max_weight.?)
        return error.InvalidQueryRequest;
}

const GraphEdgeWeightBounds = struct {
    min: ?f64 = null,
    max: ?f64 = null,
};

fn parseGraphEdgeWeightRange(value: ?indexes_openapi.GraphEdgeWeightRange) !GraphEdgeWeightBounds {
    const range = value orelse return .{};
    if (range.min == null and range.max == null) return error.InvalidQueryRequest;
    try validateGraphWeightBounds(range.min, range.max);
    return .{ .min = range.min, .max = range.max };
}

test "canonical graph admission preserves and validates weight bounds" {
    try validateGraphWeightBounds(0, 0);
    try std.testing.expectError(error.InvalidQueryRequest, validateGraphWeightBounds(-2, -1));
    try std.testing.expectError(error.InvalidQueryRequest, validateGraphWeightBounds(1, 0));
    try std.testing.expectError(error.InvalidQueryRequest, validateGraphWeightBounds(std.math.nan(f64), null));
    try std.testing.expectEqual(@as(?f64, null), legacyWeightBound(0));
    try std.testing.expectEqual(@as(?f64, 0.5), legacyWeightBound(0.5));
}

test "graph date filters accept RFC3339 offsets and reject normalized invalid dates" {
    const utc = (try parseDateTimeOptionalToNs("2026-08-24T19:00:00Z")).?;
    try std.testing.expectEqual(utc, (try parseDateTimeOptionalToNs("2026-08-24T12:00:00-07:00")).?);
    try std.testing.expect((try parseDateTimeOptionalToNs("2026-02-29T00:00:00Z")) == null);
    try std.testing.expect((try parseDateTimeOptionalToNs("2026-04-31")) == null);
    try std.testing.expect((try parseRfc3339ToNs("2026-08-24")) == null);
}

test "canonical graph date filters are operation keyed and require a bound" {
    const alloc = std.testing.allocator;
    var owned = try parseQueryRequest(alloc, null, "docs",
        \\{"graph_queries":{"walk":{"index":"g","traverse":{"start":{"keys":["a"]},"filter":{"date_range":{"path":"/created_at","start":"2026-01-01T00:00:00Z"}}}}}}
    );
    defer owned.deinit(alloc);

    const filter_json = owned.req.graph_queries[0].query.params.node_filter.filter_query_json.?;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, filter_json, .{});
    defer parsed.deinit();
    try std.testing.expect(parsed.value.object.get("date_range") != null);

    try std.testing.expectError(error.InvalidQueryRequest, parseQueryRequest(alloc, null, "docs",
        \\{"graph_queries":{"walk":{"index":"g","traverse":{"start":{"keys":["a"]},"filter":{"path":"/created_at","start":"2026-01-01T00:00:00Z"}}}}}}
    ));
    try std.testing.expectError(error.InvalidQueryRequest, parseQueryRequest(alloc, null, "docs",
        \\{"graph_queries":{"walk":{"index":"g","traverse":{"start":{"keys":["a"]},"filter":{"date_range":{"path":"/created_at"}}}}}}
    ));
    try std.testing.expectError(error.InvalidQueryRequest, parseQueryRequest(alloc, null, "docs",
        \\{"graph_queries":{"walk":{"index":"g","traverse":{"start":{"keys":["a"]},"filter":{"date_range":{"path":"/created_at","start":"2026-01-01"}}}}}}
    ));
}

test "canonical graph document filter variants cross the public storage boundary" {
    const alloc = std.testing.allocator;
    const filters = [_][]const u8{
        "{\"term\":\"active\",\"path\":\"/status\"}",
        "{\"term\":\"gild\",\"path\":\"/tier\",\"fuzziness\":1}",
        "{\"prefix\":\"doc:\",\"path\":\"/id\"}",
        "{\"regexp\":\"go.*\",\"path\":\"/tier\"}",
        "{\"wildcard\":\"go*\",\"path\":\"/tier\"}",
        "{\"numeric_range\":{\"path\":\"/score\",\"min\":0}}",
        "{\"term_range\":{\"path\":\"/status\",\"max\":\"z\"}}",
        "{\"date_range\":{\"path\":\"/created_at\",\"start\":\"2026-01-01T00:00:00Z\"}}",
        "{\"match_all\":{}}",
        "{\"match_none\":{}}",
        "{\"ids\":[\"doc:a\"]}",
        "{\"bool_field\":{\"path\":\"/published\",\"value\":true}}",
        "{\"must\":{\"conjuncts\":[{\"term\":\"active\",\"path\":\"/status\"}]}}",
        "{\"should\":{\"disjuncts\":[{\"term\":\"active\",\"path\":\"/status\"}],\"min\":1}}",
        "{\"must_not\":{\"disjuncts\":[{\"term\":\"deleted\",\"path\":\"/status\"}]}}",
        "{\"filter\":{\"term\":\"active\",\"path\":\"/status\"}}",
        "{\"conjuncts\":[{\"term\":\"active\",\"path\":\"/status\"}]}",
        "{\"disjuncts\":[{\"term\":\"active\",\"path\":\"/status\"}],\"min\":1}",
    };

    for (filters, 0..) |filter, filter_index| {
        const request = try std.mem.concat(alloc, u8, &.{
            "{\"graph_queries\":{\"walk\":{\"index\":\"g\",\"traverse\":{\"start\":{\"keys\":[\"a\"]},\"filter\":",
            filter,
            "}}}}",
        });
        defer alloc.free(request);
        var owned = parseQueryRequest(alloc, null, "docs", request) catch |err| {
            std.debug.print("graph document filter {d} failed admission: {s}\n", .{ filter_index, filter });
            return err;
        };
        defer owned.deinit(alloc);
        const normalized = owned.req.graph_queries[0].query.params.node_filter.filter_query_json orelse
            return error.TestUnexpectedResult;
        try std.testing.expect(normalized.len > 0);
    }

    // Closed empty-object predicates reject misspelled or future fields at
    // the generated-schema boundary instead of silently weakening the filter.
    try std.testing.expectError(error.InvalidQueryRequest, parseQueryRequest(alloc, null, "docs",
        \\{"graph_queries":{"walk":{"index":"g","traverse":{"start":{"keys":["a"]},"filter":{"match_all":{"unexpected":true}}}}}}
    ));
}

test "canonical graph boolean field filter has one unambiguous root" {
    const alloc = std.testing.allocator;
    var owned = try parseQueryRequest(alloc, null, "docs",
        \\{"graph_queries":{"walk":{"index":"g","traverse":{"start":{"keys":["a"]},"filter":{"bool_field":{"path":"/published","value":true}}}}}}
    );
    defer owned.deinit(alloc);

    const normalized = owned.req.graph_queries[0].query.params.node_filter.filter_query_json.?;
    var parsed = try ant_json.parseFromSlice(ant_json.Value, alloc, normalized, .{});
    defer parsed.deinit();
    const bool_field = parsed.value.object.get("bool_field") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("/published", bool_field.object.get("path").?.string);
    try std.testing.expect(bool_field.object.get("value").?.bool);
}

test "canonical graph path endpoints reject empty identities before allocation" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(error.InvalidQueryRequest, parseGraphPathEndpointSelector(alloc, .{
        .key = "",
        .table = @as(?[]const u8, null),
    }));
    try std.testing.expectError(error.InvalidQueryRequest, parseGraphPathEndpointSelector(alloc, .{
        .key = "node",
        .table = @as(?[]const u8, ""),
    }));
    try std.testing.expectError(error.InvalidQueryRequest, parseGraphPathEndpointSelector(alloc, .{
        .key = "node",
        .table = @as(?[]const u8, " \t\r\n"),
    }));
}

fn parseGraphWherePredicates(alloc: std.mem.Allocator, value: ?indexes_openapi.GraphWhereExpression) ![]const graph_pattern_mod.MatchPredicate {
    var predicates = std.ArrayListUnmanaged(graph_pattern_mod.MatchPredicate).empty;
    errdefer {
        freeGraphMatchPredicates(alloc, predicates.items);
        predicates.deinit(alloc);
    }
    if (value) |where| try appendGraphWherePredicates(alloc, &predicates, where, 0);
    return try predicates.toOwnedSlice(alloc);
}

fn appendGraphWherePredicates(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(graph_pattern_mod.MatchPredicate),
    value: indexes_openapi.GraphWhereExpression,
    depth: usize,
) !void {
    if (depth >= graph_pattern_mod.max_match_predicate_depth or
        out.items.len >= graph_pattern_mod.max_match_predicates)
        return error.InvalidQueryRequest;
    switch (value) {
        .graph_where_and => |where_and| {
            if (where_and.@"and".len == 0 or
                where_and.@"and".len > graph_pattern_mod.max_match_predicates)
                return error.InvalidQueryRequest;
            for (where_and.@"and") |child| try appendGraphWherePredicates(alloc, out, child, depth + 1);
        },
        .graph_where_not_equal => |where_neq| {
            if (!graph_query_mod.isValidIdentifier(where_neq.not_equal.left.alias) or
                !graph_query_mod.isValidIdentifier(where_neq.not_equal.right.alias))
                return error.InvalidQueryRequest;
            const left = try alloc.dupe(u8, where_neq.not_equal.left.alias);
            errdefer alloc.free(left);
            const right = try alloc.dupe(u8, where_neq.not_equal.right.alias);
            errdefer alloc.free(right);
            try out.append(alloc, .{ .not_equal = .{ .left = left, .right = right } });
        },
        .graph_where_not_exists => |where_missing| {
            const edges = try parseGraphMatchEdges(alloc, where_missing.not_exists.edges);
            errdefer freeGraphMatchEdges(alloc, edges);
            try out.append(alloc, .{ .not_exists = edges });
        },
    }
}

fn parseGraphOptionalPatterns(alloc: std.mem.Allocator, value: []const indexes_openapi.GraphOptionalMatch) ![]const graph_pattern_mod.OptionalPattern {
    if (value.len > graph_pattern_mod.max_optional_patterns) return error.InvalidQueryRequest;
    const groups = try alloc.alloc(graph_pattern_mod.OptionalPattern, value.len);
    var initialized: usize = 0;
    errdefer {
        for (groups[0..initialized]) |group| {
            freeGraphMatchNodes(alloc, group.nodes);
            freeGraphMatchEdges(alloc, group.edges);
            freeGraphMatchPredicates(alloc, group.predicates);
        }
        alloc.free(groups);
    }
    for (value, 0..) |group, i| {
        const nodes = if (group.nodes) |nodes_value| try parseGraphMatchNodes(alloc, nodes_value) else &.{};
        errdefer freeGraphMatchNodes(alloc, nodes);
        const edges = try parseGraphMatchEdges(alloc, group.edges);
        errdefer freeGraphMatchEdges(alloc, edges);
        const predicates = try parseGraphWherePredicates(alloc, group.where);
        errdefer freeGraphMatchPredicates(alloc, predicates);
        groups[i] = .{
            .nodes = nodes,
            .edges = edges,
            .predicates = predicates,
        };
        initialized += 1;
    }
    return groups;
}

fn parseGraphCountAggregates(alloc: std.mem.Allocator, value: std.json.ArrayHashMap(indexes_openapi.GraphCountAggregate)) ![]const graph_query_mod.NamedCountAggregate {
    if (value.map.count() == 0 or value.map.count() > graph_pattern_mod.max_count_aggregates)
        return error.InvalidQueryRequest;
    const aggregates = try alloc.alloc(graph_query_mod.NamedCountAggregate, value.map.count());
    var initialized: usize = 0;
    errdefer {
        for (aggregates[0..initialized]) |aggregate| {
            alloc.free(aggregate.name);
            alloc.free(aggregate.of);
        }
        alloc.free(aggregates);
    }
    var it = value.map.iterator();
    while (it.next()) |entry| {
        if (!graph_query_mod.isValidIdentifier(entry.key_ptr.*)) return error.InvalidQueryRequest;
        const parsed_aggregate = try graphCountAggregateParts(entry.value_ptr.*);
        if (!std.mem.eql(u8, parsed_aggregate.of, "*") and
            !graph_query_mod.isValidIdentifier(parsed_aggregate.of))
            return error.InvalidQueryRequest;
        const name = try alloc.dupe(u8, entry.key_ptr.*);
        errdefer alloc.free(name);
        const of = try alloc.dupe(u8, parsed_aggregate.of);
        errdefer alloc.free(of);
        aggregates[initialized] = .{
            .name = name,
            .of = of,
            .distinct = parsed_aggregate.distinct,
        };
        initialized += 1;
    }
    return aggregates;
}

const ParsedGraphCountAggregate = struct {
    of: []const u8,
    distinct: bool,
};

fn graphCountAggregateParts(value: indexes_openapi.GraphCountAggregate) !ParsedGraphCountAggregate {
    return switch (value) {
        .graph_row_count_aggregate => .{ .of = "*", .distinct = false },
        .graph_alias_count_aggregate => |aggregate| blk: {
            if (!graph_query_mod.isValidIdentifier(aggregate.count))
                return error.InvalidQueryRequest;
            break :blk .{
                .of = aggregate.count,
                .distinct = aggregate.distinct orelse false,
            };
        },
    };
}

fn parseGraphFilterValue(alloc: std.mem.Allocator, value: ?indexes_openapi.GraphDocumentFilter) !graph_pattern_mod.NodeFilter {
    const filter = value orelse return .{};
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    var budget = GraphFilterLoweringBudget{};
    const canonical = try lowerGraphDocumentFilter(arena.allocator(), filter, 0, &budget);
    db_mod.validateStructuredFilterValueAlloc(arena.allocator(), canonical) catch |err| {
        return switch (err) {
            error.OutOfMemory => error.OutOfMemory,
            error.UnsupportedQueryRequest => error.UnsupportedQueryRequest,
            else => error.InvalidQueryRequest,
        };
    };
    return .{ .filter_query_json = try jsonStringifyAlloc(alloc, canonical) };
}

fn validGraphDocumentJsonPointer(path: []const u8) bool {
    if (!std.mem.startsWith(u8, path, "/")) return false;
    var i: usize = 0;
    while (i < path.len) : (i += 1) {
        if (path[i] != '~') continue;
        if (i + 1 >= path.len or (path[i + 1] != '0' and path[i + 1] != '1')) return false;
        i += 1;
    }
    return true;
}

const GraphFilterLoweringBudget = struct {
    remaining_nodes: usize = public_query_max_tree_nodes,
};

fn lowerGraphDocumentFilter(
    alloc: std.mem.Allocator,
    filter: indexes_openapi.GraphDocumentFilter,
    depth: usize,
    budget: *GraphFilterLoweringBudget,
) anyerror!std.json.Value {
    if (depth > public_query_max_tree_depth or budget.remaining_nodes == 0)
        return error.InvalidQueryRequest;
    budget.remaining_nodes -= 1;

    return switch (filter) {
        .graph_document_term_filter => |value| blk: {
            try validateGraphDocumentPath(value.path);
            break :blk try graphStoredStringPredicate(alloc, "term", "term", value.path, value.term);
        },
        .graph_document_prefix_filter => |value| blk: {
            try validateGraphDocumentPath(value.path);
            break :blk try graphStoredStringPredicate(alloc, "prefix", "prefix", value.path, value.prefix);
        },
        .graph_document_regexp_filter => |value| blk: {
            try validateGraphDocumentPath(value.path);
            break :blk try graphStoredStringPredicate(alloc, "regexp", "pattern", value.path, value.regexp);
        },
        .graph_document_wildcard_filter => |value| blk: {
            try validateGraphDocumentPath(value.path);
            break :blk try graphStoredStringPredicate(alloc, "wildcard", "pattern", value.path, value.wildcard);
        },
        .graph_document_fuzzy_filter => |value| blk: {
            try validateGraphDocumentPath(value.path);
            const fuzziness = try parseBleveFuzziness(value.fuzziness, 1);
            const prefix_length = try parseBlevePrefixLength(value.prefix_length);
            var body = std.json.ObjectMap.empty;
            try body.put(alloc, "path", .{ .string = value.path });
            try body.put(alloc, "query", .{ .string = value.term });
            try body.put(alloc, "max_edits", .{ .integer = fuzziness.max_edits });
            try body.put(alloc, "prefix_length", .{ .integer = prefix_length });
            if (fuzziness.auto_fuzzy) try body.put(alloc, "auto_fuzzy", .{ .bool = true });
            break :blk try graphStoredWrappedPredicate(alloc, "fuzzy", .{ .object = body });
        },
        .graph_document_numeric_range_filter => |value| blk: {
            const range = value.numeric_range;
            try validateGraphDocumentPath(range.path);
            if (range.min == null and range.max == null) return error.InvalidQueryRequest;
            if (range.min) |bound| if (!std.math.isFinite(bound)) return error.InvalidQueryRequest;
            if (range.max) |bound| if (!std.math.isFinite(bound)) return error.InvalidQueryRequest;
            if (range.min != null and range.max != null and range.min.? > range.max.?)
                return error.InvalidQueryRequest;
            var body = std.json.ObjectMap.empty;
            try body.put(alloc, "path", .{ .string = range.path });
            if (range.min) |bound| try body.put(alloc, "min", .{ .float = bound });
            if (range.max) |bound| try body.put(alloc, "max", .{ .float = bound });
            if (range.inclusive_min) |inclusive| try body.put(alloc, "inclusive_min", .{ .bool = inclusive });
            if (range.inclusive_max) |inclusive| try body.put(alloc, "inclusive_max", .{ .bool = inclusive });
            break :blk try graphStoredWrappedPredicate(alloc, "numeric_range", .{ .object = body });
        },
        .graph_document_term_range_filter => |value| blk: {
            const range = value.term_range;
            try validateGraphDocumentPath(range.path);
            if (range.min == null and range.max == null) return error.InvalidQueryRequest;
            var body = std.json.ObjectMap.empty;
            try body.put(alloc, "path", .{ .string = range.path });
            if (range.min) |bound| try body.put(alloc, "min", .{ .string = bound });
            if (range.max) |bound| try body.put(alloc, "max", .{ .string = bound });
            if (range.inclusive_min) |inclusive| try body.put(alloc, "inclusive_min", .{ .bool = inclusive });
            if (range.inclusive_max) |inclusive| try body.put(alloc, "inclusive_max", .{ .bool = inclusive });
            break :blk try graphStoredWrappedPredicate(alloc, "term_range", .{ .object = body });
        },
        .graph_document_date_range_filter => |value| blk: {
            const range = value.date_range;
            try validateGraphDocumentPath(range.path);
            if (range.start == null and range.end == null) return error.InvalidQueryRequest;
            var start_ns: ?u64 = null;
            var end_ns: ?u64 = null;
            var body = std.json.ObjectMap.empty;
            try body.put(alloc, "path", .{ .string = range.path });
            if (range.start) |bound| {
                const timestamp = (try parseRfc3339ToNs(bound)) orelse return error.InvalidQueryRequest;
                start_ns = timestamp;
                try body.put(alloc, "start_ns", try graphJsonU64(alloc, timestamp));
            }
            if (range.end) |bound| {
                const timestamp = (try parseRfc3339ToNs(bound)) orelse return error.InvalidQueryRequest;
                end_ns = timestamp;
                try body.put(alloc, "end_ns", try graphJsonU64(alloc, timestamp));
            }
            if (start_ns != null and end_ns != null and start_ns.? > end_ns.?)
                return error.InvalidQueryRequest;
            if (range.inclusive_start) |inclusive| try body.put(alloc, "inclusive_start", .{ .bool = inclusive });
            if (range.inclusive_end) |inclusive| try body.put(alloc, "inclusive_end", .{ .bool = inclusive });
            break :blk try graphStoredWrappedPredicate(alloc, "date_range", .{ .object = body });
        },
        .graph_document_ids_filter => |value| blk: {
            if (value.ids.len == 0 or value.ids.len > 10_000) return error.InvalidQueryRequest;
            var seen = std.StringHashMapUnmanaged(void).empty;
            var ids = std.json.Array.init(alloc);
            for (value.ids) |id| {
                const result = try seen.getOrPut(alloc, id);
                if (result.found_existing) return error.InvalidQueryRequest;
                try ids.append(.{ .string = id });
            }
            break :blk try graphStoredWrappedPredicate(alloc, "doc_id", .{ .array = ids });
        },
        .graph_document_bool_field_filter => |value| blk: {
            try validateGraphDocumentPath(value.bool_field.path);
            var body = std.json.ObjectMap.empty;
            try body.put(alloc, "path", .{ .string = value.bool_field.path });
            try body.put(alloc, "value", .{ .bool = value.bool_field.value });
            break :blk try graphStoredWrappedPredicate(alloc, "bool_field", .{ .object = body });
        },
        .graph_document_match_all_filter => try graphStoredEmptyPredicate(alloc, "match_all"),
        .graph_document_match_none_filter => try graphStoredEmptyPredicate(alloc, "match_none"),
        .graph_document_filter_conjunction => |value| try lowerGraphConjunction(alloc, value.*, depth, budget),
        .graph_document_filter_disjunction => |value| try lowerGraphDisjunction(alloc, value.*, depth, budget),
        .graph_document_filter_boolean => |value| try lowerGraphBoolean(alloc, value.*, depth, budget),
    };
}

fn validateGraphDocumentPath(path: []const u8) !void {
    if (!validGraphDocumentJsonPointer(path)) return error.InvalidQueryRequest;
}

fn graphStoredWrappedPredicate(
    alloc: std.mem.Allocator,
    name: []const u8,
    body: std.json.Value,
) !std.json.Value {
    var root = std.json.ObjectMap.empty;
    try root.put(alloc, name, body);
    return .{ .object = root };
}

fn graphStoredStringPredicate(
    alloc: std.mem.Allocator,
    name: []const u8,
    value_name: []const u8,
    path: []const u8,
    value: []const u8,
) !std.json.Value {
    var body = std.json.ObjectMap.empty;
    try body.put(alloc, "path", .{ .string = path });
    try body.put(alloc, value_name, .{ .string = value });
    return graphStoredWrappedPredicate(alloc, name, .{ .object = body });
}

fn graphStoredEmptyPredicate(alloc: std.mem.Allocator, name: []const u8) !std.json.Value {
    return graphStoredWrappedPredicate(alloc, name, .{ .object = std.json.ObjectMap.empty });
}

fn graphJsonU64(alloc: std.mem.Allocator, value: u64) !std.json.Value {
    if (value <= std.math.maxInt(i64)) return .{ .integer = @intCast(value) };
    return .{ .number_string = try std.fmt.allocPrint(alloc, "{d}", .{value}) };
}

fn lowerGraphFilterArray(
    alloc: std.mem.Allocator,
    filters: []const indexes_openapi.GraphDocumentFilter,
    depth: usize,
    budget: *GraphFilterLoweringBudget,
) anyerror!std.json.Array {
    if (filters.len == 0 or filters.len > 64) return error.InvalidQueryRequest;
    var out = std.json.Array.init(alloc);
    try out.ensureTotalCapacity(filters.len);
    for (filters) |filter| {
        out.appendAssumeCapacity(try lowerGraphDocumentFilter(alloc, filter, depth + 1, budget));
    }
    return out;
}

fn lowerGraphConjunction(
    alloc: std.mem.Allocator,
    conjunction: indexes_openapi.GraphDocumentFilterConjunction,
    depth: usize,
    budget: *GraphFilterLoweringBudget,
) anyerror!std.json.Value {
    const conjuncts = try lowerGraphFilterArray(alloc, conjunction.conjuncts, depth, budget);
    return graphStoredWrappedPredicate(alloc, "conjuncts", .{ .array = conjuncts });
}

fn lowerGraphDisjunction(
    alloc: std.mem.Allocator,
    disjunction: indexes_openapi.GraphDocumentFilterDisjunction,
    depth: usize,
    budget: *GraphFilterLoweringBudget,
) anyerror!std.json.Value {
    const disjuncts = try lowerGraphFilterArray(alloc, disjunction.disjuncts, depth, budget);
    const minimum = disjunction.min orelse
        return graphStoredWrappedPredicate(alloc, "disjuncts", .{ .array = disjuncts });
    if (minimum < 0 or minimum > 64 or @as(usize, @intCast(minimum)) > disjuncts.items.len)
        return error.InvalidQueryRequest;
    var body = std.json.ObjectMap.empty;
    // The stored-filter contract intentionally gives a pure SHOULD a semantic
    // floor of one. An explicit graph `min: 0` means optional, so make the
    // match-all requirement explicit instead of weakening that shared safety
    // invariant for every stored-filter caller.
    if (minimum == 0) {
        var must = std.json.Array.init(alloc);
        try must.append(try graphStoredEmptyPredicate(alloc, "match_all"));
        try body.put(alloc, "must", .{ .array = must });
    }
    try body.put(alloc, "should", .{ .array = disjuncts });
    try body.put(alloc, "minimum_should_match", .{ .integer = minimum });
    return graphStoredWrappedPredicate(alloc, "bool", .{ .object = body });
}

fn lowerGraphBoolean(
    alloc: std.mem.Allocator,
    boolean: indexes_openapi.GraphDocumentFilterBoolean,
    depth: usize,
    budget: *GraphFilterLoweringBudget,
) anyerror!std.json.Value {
    if (boolean.must == null and boolean.should == null and
        boolean.must_not == null and boolean.filter == null)
        return error.InvalidQueryRequest;

    var body = std.json.ObjectMap.empty;
    var has_required = false;
    if (boolean.must) |must| {
        try body.put(alloc, "must", .{ .array = try lowerGraphFilterArray(alloc, must.conjuncts, depth, budget) });
        has_required = true;
    }
    if (boolean.filter) |filter| {
        var filters = std.json.Array.init(alloc);
        try filters.append(try lowerGraphDocumentFilter(alloc, filter, depth + 1, budget));
        try body.put(alloc, "filter", .{ .array = filters });
        has_required = true;
    }
    if (boolean.should) |should| {
        const items = try lowerGraphFilterArray(alloc, should.disjuncts, depth, budget);
        if (should.min) |minimum| {
            if (minimum < 0 or minimum > 64 or @as(usize, @intCast(minimum)) > items.items.len)
                return error.InvalidQueryRequest;
            if (minimum == 0 and !has_required) {
                var must = std.json.Array.init(alloc);
                try must.append(try graphStoredEmptyPredicate(alloc, "match_all"));
                try body.put(alloc, "must", .{ .array = must });
                has_required = true;
            }
            try body.put(alloc, "minimum_should_match", .{ .integer = minimum });
        }
        try body.put(alloc, "should", .{ .array = items });
    }
    if (boolean.must_not) |must_not| {
        var items = std.json.Array.init(alloc);
        if (must_not.min) |_| {
            try items.append(try lowerGraphDisjunction(alloc, must_not, depth, budget));
        } else {
            items = try lowerGraphFilterArray(alloc, must_not.disjuncts, depth, budget);
        }
        try body.put(alloc, "must_not", .{ .array = items });
    }
    return graphStoredWrappedPredicate(alloc, "bool", .{ .object = body });
}

fn parseGraphDirection(value: ?indexes_openapi.EdgeDirection) graph_mod.EdgeDirection {
    return if (value) |direction| switch (direction) {
        .out => .out,
        .in => .in,
        .both => .both,
    } else .out;
}

fn parseGraphBoundedU32(value: ?i64, default: u32, min: u32, max: u32) !u32 {
    const parsed = if (value) |raw| std.math.cast(u32, raw) orelse return error.InvalidQueryRequest else default;
    if (parsed < min or parsed > max) return error.InvalidQueryRequest;
    return parsed;
}

fn freePatternNodeFilter(alloc: std.mem.Allocator, filter: graph_pattern_mod.NodeFilter) void {
    if (filter.filter_prefix.len > 0) alloc.free(filter.filter_prefix);
    if (filter.filter_query_json) |query_json| alloc.free(query_json);
}

fn freePatternSteps(alloc: std.mem.Allocator, steps: []const graph_pattern_mod.PatternStep) void {
    for (steps) |step| {
        alloc.free(step.alias);
        freePatternNodeFilter(alloc, step.node_filter);
        for (step.edge.types) |edge_type| alloc.free(edge_type);
        if (step.edge.types.len > 0) alloc.free(step.edge.types);
    }
    if (steps.len > 0) alloc.free(steps);
}

fn parseGraphNodeSelector(
    alloc: std.mem.Allocator,
    selector: indexes_openapi.GraphNodeSelector,
) !graph_query_mod.NodeSelector {
    return switch (selector) {
        .graph_key_node_selector => |value| blk: {
            if (value.keys.len == 0 or value.keys.len > 10_000) return error.InvalidQueryRequest;
            break :blk .{ .identities = try cloneGraphKeysAsIdentities(alloc, value.keys) };
        },
        .graph_identity_node_selector => |value| blk: {
            if (value.identities.len == 0 or value.identities.len > 10_000) return error.InvalidQueryRequest;
            const owned = try cloneGraphNodeIdentities(alloc, value.identities);
            break :blk .{ .identities = owned };
        },
        .graph_result_ref_node_selector => |value| blk: {
            try validateGraphResultRef(value.result_ref);
            if (value.binding != null and !std.mem.startsWith(u8, value.result_ref, "$graph_results."))
                return error.InvalidQueryRequest;
            if (value.binding) |binding| if (!graph_query_mod.isValidIdentifier(binding))
                return error.InvalidQueryRequest;
            const owned_ref = try alloc.dupe(u8, value.result_ref);
            errdefer alloc.free(owned_ref);
            const owned_binding = if (value.binding) |binding| try alloc.dupe(u8, binding) else null;
            errdefer if (owned_binding) |binding| alloc.free(binding);
            const limit = if (value.limit) |raw_limit|
                try parseGraphBoundedU32(raw_limit, 0, 1, 10_000)
            else
                0;
            break :blk .{ .result_ref = .{
                .ref = owned_ref,
                .binding = owned_binding,
                .limit = limit,
            } };
        },
    };
}

fn parseLegacyGraphNodeSelector(
    alloc: std.mem.Allocator,
    selector: indexes_openapi.LegacyGraphNodeSelector,
) !graph_query_mod.NodeSelector {
    if (selector.node_filter != null) return error.UnsupportedQueryRequest;
    const selector_count = @as(u8, @intFromBool(selector.keys != null)) +
        @as(u8, @intFromBool(selector.identities != null)) +
        @as(u8, @intFromBool(selector.result_ref != null));
    if (selector_count != 1) return error.InvalidQueryRequest;
    if (selector.keys) |keys| {
        if (selector.limit != null) return error.InvalidQueryRequest;
        const owned_keys = try cloneFields(alloc, keys);
        return .{ .keys = owned_keys };
    }
    if (selector.identities) |identities| {
        if (selector.limit != null or identities.len == 0) return error.InvalidQueryRequest;
        const owned = try alloc.alloc(graph_query_mod.NodeIdentity, identities.len);
        var initialized: usize = 0;
        errdefer {
            for (owned[0..initialized]) |identity| {
                alloc.free(identity.key);
                if (identity.table) |table| alloc.free(table);
            }
            alloc.free(owned);
        }
        for (identities, 0..) |identity, i| {
            owned[i] = .{ .key = try alloc.dupe(u8, identity.key), .table = null };
            initialized += 1;
            if (identity.table) |value| owned[i].table = try alloc.dupe(u8, value);
        }
        return .{ .identities = owned };
    }
    if (selector.result_ref) |result_ref| {
        const canonical_ref = legacyGraphResultRef(result_ref) orelse return error.InvalidQueryRequest;
        return .{ .result_ref = .{
            .ref = try alloc.dupe(u8, canonical_ref),
            .limit = if (selector.limit) |limit|
                std.math.cast(u32, limit) orelse return error.InvalidQueryRequest
            else
                0,
        } };
    }
    return error.UnsupportedQueryRequest;
}

fn cloneGraphNodeIdentities(
    alloc: std.mem.Allocator,
    identities: []const indexes_openapi.GraphPathEndpoint,
) ![]graph_query_mod.NodeIdentity {
    var seen = GraphNodeIdentitySet.empty;
    defer seen.deinit(alloc);
    const owned = try alloc.alloc(graph_query_mod.NodeIdentity, identities.len);
    var initialized: usize = 0;
    errdefer {
        for (owned[0..initialized]) |identity| {
            alloc.free(identity.key);
            if (identity.table) |table| alloc.free(table);
        }
        alloc.free(owned);
    }
    for (identities, 0..) |identity, i| {
        if (identity.key.len == 0 or (identity.table != null and identity.table.?.len == 0))
            return error.InvalidQueryRequest;
        const entry = try seen.getOrPut(alloc, .{ .key = identity.key, .table = identity.table });
        if (entry.found_existing) return error.InvalidQueryRequest;
        entry.value_ptr.* = {};
        owned[i] = .{ .key = try alloc.dupe(u8, identity.key), .table = null };
        initialized += 1;
        if (identity.table) |value| owned[i].table = try alloc.dupe(u8, value);
    }
    return owned;
}

fn cloneGraphKeysAsIdentities(
    alloc: std.mem.Allocator,
    keys: []const []const u8,
) ![]graph_query_mod.NodeIdentity {
    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(alloc);
    const owned = try alloc.alloc(graph_query_mod.NodeIdentity, keys.len);
    var initialized: usize = 0;
    errdefer {
        for (owned[0..initialized]) |identity| alloc.free(identity.key);
        alloc.free(owned);
    }
    for (keys, 0..) |key, i| {
        if (key.len == 0) return error.InvalidQueryRequest;
        const entry = try seen.getOrPut(alloc, key);
        if (entry.found_existing) return error.InvalidQueryRequest;
        entry.value_ptr.* = {};
        owned[i] = .{ .key = try alloc.dupe(u8, key), .table = null };
        initialized += 1;
    }
    return owned;
}

const GraphNodeIdentityKey = struct {
    key: []const u8,
    table: ?[]const u8,
};

const GraphNodeIdentityContext = struct {
    pub fn hash(_: @This(), identity: GraphNodeIdentityKey) u64 {
        var hasher = std.hash.Wyhash.init(0x414E_5446_4C59_4752);
        const table_len: u64 = if (identity.table) |table| @intCast(table.len + 1) else 0;
        hasher.update(std.mem.asBytes(&table_len));
        if (identity.table) |table| hasher.update(table);
        const key_len: u64 = @intCast(identity.key.len);
        hasher.update(std.mem.asBytes(&key_len));
        hasher.update(identity.key);
        return hasher.final();
    }

    pub fn eql(_: @This(), a: GraphNodeIdentityKey, b: GraphNodeIdentityKey) bool {
        if (!std.mem.eql(u8, a.key, b.key)) return false;
        if (a.table == null or b.table == null) return a.table == null and b.table == null;
        return std.mem.eql(u8, a.table.?, b.table.?);
    }
};

const GraphNodeIdentitySet = std.HashMapUnmanaged(GraphNodeIdentityKey, void, GraphNodeIdentityContext, 80);

fn validateGraphResultRef(ref: []const u8) !void {
    if (std.mem.eql(u8, ref, "$query_results")) return;
    const prefix = "$graph_results.";
    if (std.mem.startsWith(u8, ref, prefix) and
        graph_query_mod.isValidIdentifier(ref[prefix.len..])) return;
    return error.InvalidQueryRequest;
}

fn legacyGraphResultRef(ref: []const u8) ?[]const u8 {
    if (std.mem.eql(u8, ref, "$full_text_results") or
        std.mem.eql(u8, ref, "$embeddings_results") or
        std.mem.eql(u8, ref, "$fused_results")) return ref;
    if (std.mem.startsWith(u8, ref, "$graph_results.") and
        ref.len > "$graph_results.".len) return ref;
    return null;
}

fn parseExpandStrategy(text: []const u8) !graph_query_mod.ExpandStrategy {
    if (std.mem.eql(u8, text, "union")) return .@"union";
    if (std.mem.eql(u8, text, "intersection")) return .intersection;
    return error.UnsupportedQueryRequest;
}

fn appendUniqueOwnedString(
    alloc: std.mem.Allocator,
    values: *std.ArrayListUnmanaged([]const u8),
    value: []const u8,
) !void {
    for (values.items) |existing| {
        if (std.mem.eql(u8, existing, value)) return;
    }
    try values.append(alloc, try alloc.dupe(u8, value));
}

fn freeSearchRequest(alloc: std.mem.Allocator, req: *db_mod.types.SearchRequest) void {
    if (req.index_name) |index_name| alloc.free(index_name);
    if (req.primary_text_index_name) |index_name| alloc.free(index_name);
    if (req.aggregations_json.len > 0) alloc.free(req.aggregations_json);
    if (req.filter_prefix.len > 0) alloc.free(req.filter_prefix);
    if (req.reranker) |*reranker| reranker.deinit(alloc);
    if (req.reranker_query_text.len > 0) alloc.free(req.reranker_query_text);
    if (req.merge_config) |merge_config| {
        for (merge_config.weights) |item| alloc.free(item.name);
        if (merge_config.weights.len > 0) alloc.free(merge_config.weights);
    }
    if (req.full_text) |full_text| freeTextQuery(alloc, full_text);
    if (req.filter_text) |filter_text| freeTextQuery(alloc, filter_text);
    if (req.exclusion_text) |exclusion_text| freeTextQuery(alloc, exclusion_text);
    if (req.filter_query_json.len > 0) alloc.free(req.filter_query_json);
    if (req.exclusion_query_json.len > 0) alloc.free(req.exclusion_query_json);
    if (req.authorization_filter_query_json.len > 0) alloc.free(req.authorization_filter_query_json);
    for (req.order_by) |field| alloc.free(field.field);
    if (req.order_by.len > 0) alloc.free(req.order_by);
    freeClonedJsonValues(alloc, req.search_after);
    freeClonedJsonValues(alloc, req.search_before);
    switch (req.query) {
        .term => |term| {
            alloc.free(term.field);
            alloc.free(term.term);
        },
        .match => |match| {
            alloc.free(match.field);
            alloc.free(match.text);
        },
        else => {},
    }
    if (req.dense) |dense| alloc.free(dense.vector);
    freeNamedFullTextQueries(alloc, req.full_text_queries);
    freeNamedDenseQueries(alloc, req.dense_queries);
    freeNamedSparseQueries(alloc, req.sparse_queries);
    freeNamedGraphQueries(alloc, req.graph_queries);
    if (req.graph_query_transport) |*transport| transport.deinit(alloc);
    freeNamedDocFilterBindings(alloc, req.doc_filter_bindings);
    if (req.sparse) |sparse| {
        alloc.free(sparse.indices);
        alloc.free(sparse.values);
    }
    if (req.filter_ids.len > 0) alloc.free(req.filter_ids);
    if (req.exclude_ids.len > 0) alloc.free(req.exclude_ids);
    if (req.filter_doc_ids.len > 0) {
        freeOwnedStringItems(alloc, req.filter_doc_ids);
        alloc.free(@constCast(req.filter_doc_ids));
    }
    if (req.exclude_doc_ids.len > 0) {
        freeOwnedStringItems(alloc, req.exclude_doc_ids);
        alloc.free(@constCast(req.exclude_doc_ids));
    }
    freeOwnedStringSlice(alloc, req.hierarchy_match_fields);
    freeOwnedStringSlice(alloc, req.hierarchy_source_fields);
    freeOwnedStringSlice(alloc, req.hierarchy_unit_fields);
    if (req.hierarchy_children) |children| alloc.free(@constCast(children.parent_id));
    if (req.resolved_doc_filter_owned) {
        if (req.resolved_doc_filter) |ptr| db_mod.doc_filter_wire.destroyResolvedDocFilter(alloc, ptr);
    }
    if (req.distributed_text_stats.len > 0) @import("../search/distributed_stats.zig").deinitTextFieldStats(alloc, req.distributed_text_stats);
    req.* = undefined;
}

/// Capture the admitted dialect, immutable canonical plan identity, and
/// normalized operation object once. This stays independent of generated OpenAPI
/// representation details and avoids a reverse serializer that would have to
/// evolve with the DSL.
fn captureGraphQueryTransportAlloc(
    alloc: std.mem.Allocator,
    body: []const u8,
    operations: []const db_mod.types.NamedGraphQuery,
) !db_mod.types.GraphQueryTransport {
    return graph_wire_envelope.captureRequestTransportAlloc(alloc, body, operations) catch |err| switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        else => error.InvalidQueryRequest,
    };
}

fn singleNamedFullTextQueryAlloc(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    query: db_mod.types.TextQuery,
) ![]const db_mod.types.NamedFullTextQuery {
    if (index_name.len == 0) return error.InvalidQueryRequest;
    const items = try alloc.alloc(db_mod.types.NamedFullTextQuery, 1);
    errdefer alloc.free(items);
    const result_name = try alloc.dupe(u8, "$full_text_results");
    errdefer alloc.free(result_name);
    const owned_index_name = try alloc.dupe(u8, index_name);
    errdefer alloc.free(owned_index_name);
    items[0] = .{
        .name = result_name,
        .index_name = owned_index_name,
        .query = query,
    };
    return items;
}

fn freeNamedFullTextQueries(
    alloc: std.mem.Allocator,
    queries: []const db_mod.types.NamedFullTextQuery,
) void {
    for (queries) |item| {
        alloc.free(@constCast(item.name));
        alloc.free(@constCast(item.index_name));
        freeTextQuery(alloc, item.query);
    }
    if (queries.len > 0) alloc.free(@constCast(queries));
}

fn freeNamedDocFilterBindings(alloc: std.mem.Allocator, bindings: []const db_mod.types.NamedDocFilterBinding) void {
    for (bindings) |binding| {
        alloc.free(@constCast(binding.name));
        alloc.free(@constCast(binding.filter_query_json));
    }
    if (bindings.len > 0) alloc.free(@constCast(bindings));
}

fn queryBodyHasPublicDocFilterBindings(alloc: std.mem.Allocator, body: []const u8) !bool {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{}) catch return error.InvalidQueryRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidQueryRequest;
    return parsed.value.object.get("with") != null;
}

fn putOwnedJsonValue(
    alloc: std.mem.Allocator,
    object: *std.json.ObjectMap,
    key: []const u8,
    value: std.json.Value,
) !void {
    var owned_value = value;
    errdefer db_mod.types.deinitJsonValue(alloc, &owned_value);
    const owned_key = try alloc.dupe(u8, key);
    errdefer alloc.free(owned_key);
    try object.put(alloc, owned_key, owned_value);
}

const PublicBindingExpansionBudget = struct {
    remaining_nodes: usize = public_query_max_tree_nodes,
    remaining_bytes: usize,
    deadline_ns: ?u64 = null,
    nodes_until_deadline_check: u8 = 1,

    fn consumeNode(self: *@This(), depth: usize) !void {
        if (depth > public_query_max_tree_depth or self.remaining_nodes == 0) {
            return error.InvalidQueryRequest;
        }
        self.remaining_nodes -= 1;
        self.nodes_until_deadline_check -|= 1;
        if (self.nodes_until_deadline_check == 0) {
            self.nodes_until_deadline_check = 64;
            if (self.deadline_ns) |deadline_ns| {
                if (platform_time.monotonicNs() >= deadline_ns) return error.Timeout;
            }
        }
    }

    fn consumeBytes(self: *@This(), count: usize) !void {
        if (count > self.remaining_bytes) return error.InvalidQueryRequest;
        self.remaining_bytes -= count;
    }
};

fn jsonStringEncodedLengthUpperBound(value: []const u8) !usize {
    var size: usize = 2;
    for (value) |byte| {
        const increment: usize = switch (byte) {
            '"', '\\' => 2,
            0...0x1f => 6,
            else => 1,
        };
        size = std.math.add(usize, size, increment) catch return error.InvalidQueryRequest;
    }
    return size;
}

fn chargeJsonScalar(
    budget: *PublicBindingExpansionBudget,
    value: std.json.Value,
) !void {
    const bytes: usize = switch (value) {
        .null => 4,
        .bool => |item| if (item) 4 else 5,
        .integer => 32,
        .float => 64,
        .number_string => |item| item.len,
        .string => |item| try jsonStringEncodedLengthUpperBound(item),
        else => return error.InvalidQueryRequest,
    };
    try budget.consumeBytes(bytes);
}

fn chargeJsonValue(
    budget: *PublicBindingExpansionBudget,
    value: std.json.Value,
    depth: usize,
) !void {
    try budget.consumeNode(depth);
    switch (value) {
        .array => |array| {
            try budget.consumeBytes(2 +| if (array.items.len > 0) array.items.len - 1 else 0);
            for (array.items) |item| try chargeJsonValue(budget, item, depth + 1);
        },
        .object => |object| {
            try budget.consumeBytes(2 +| if (object.count() > 0) object.count() - 1 else 0);
            var it = object.iterator();
            while (it.next()) |entry| {
                try budget.consumeBytes(try jsonStringEncodedLengthUpperBound(entry.key_ptr.*));
                try budget.consumeBytes(1);
                try chargeJsonValue(budget, entry.value_ptr.*, depth + 1);
            }
        },
        else => try chargeJsonScalar(budget, value),
    }
}

fn cloneJsonValueBudgeted(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    budget: *PublicBindingExpansionBudget,
    depth: usize,
) !std.json.Value {
    try chargeJsonValue(budget, value, depth);
    return try db_mod.types.cloneJsonValue(alloc, value);
}

fn wrapExpandedDocFilterBindingAlloc(
    alloc: std.mem.Allocator,
    expanded_filter: std.json.Value,
    budget: *PublicBindingExpansionBudget,
    depth: usize,
) !std.json.Value {
    // The wrapper is valid public Zig query syntax. A zero boost makes the
    // document-filter provenance explicit even when a reference appears in a
    // scoring-capable branch. The canonical normalizer extracts direct wrappers
    // into filter_text; nested text bools retain matching semantics with no
    // scoring contribution.
    try budget.consumeNode(depth);
    try budget.consumeNode(depth + 1);
    try budget.consumeNode(depth + 2);
    try budget.consumeBytes(40);

    var owned_filter = expanded_filter;
    var filter_owned = true;
    errdefer if (filter_owned) db_mod.types.deinitJsonValue(alloc, &owned_filter);

    var bool_payload = std.json.ObjectMap.empty;
    var bool_payload_owned = true;
    errdefer if (bool_payload_owned) {
        var value: std.json.Value = .{ .object = bool_payload };
        db_mod.types.deinitJsonValue(alloc, &value);
    };
    filter_owned = false;
    try putOwnedJsonValue(alloc, &bool_payload, "filter", owned_filter);
    try putOwnedJsonValue(alloc, &bool_payload, "boost", .{ .integer = 0 });

    var root = std.json.ObjectMap.empty;
    errdefer {
        var value: std.json.Value = .{ .object = root };
        db_mod.types.deinitJsonValue(alloc, &value);
    }
    bool_payload_owned = false;
    try putOwnedJsonValue(alloc, &root, "bool", .{ .object = bool_payload });
    return .{ .object = root };
}

fn expandPublicDocFilterQueryValueAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    bindings: std.json.ObjectMap,
    text_bindings: *const std.StringHashMapUnmanaged(bool),
    active: *std.StringHashMapUnmanaged(void),
    depth: usize,
    budget: *PublicBindingExpansionBudget,
) !std.json.Value {
    try budget.consumeNode(depth);

    if (value == .array) {
        try budget.consumeBytes(2 +| if (value.array.items.len > 0) value.array.items.len - 1 else 0);
        var out = std.json.Array.init(alloc);
        errdefer {
            for (out.items) |*item| db_mod.types.deinitJsonValue(alloc, item);
            out.deinit();
        }
        try out.ensureTotalCapacity(value.array.items.len);
        for (value.array.items) |item| {
            try out.append(try expandPublicDocFilterQueryValueAlloc(
                alloc,
                item,
                bindings,
                text_bindings,
                active,
                depth + 1,
                budget,
            ));
        }
        return .{ .array = out };
    }
    if (value != .object) {
        try chargeJsonScalar(budget, value);
        return try db_mod.types.cloneJsonValue(alloc, value);
    }

    if (value.object.count() == 1) {
        if (value.object.get("ref")) |reference| {
            if (reference != .string or reference.string.len == 0) return error.InvalidQueryRequest;
            if (!(text_bindings.get(reference.string) orelse false)) {
                // Keep structured references compact. Their definitions remain
                // in the filtered `with` object and the algebraic resolver can
                // cache them across every occurrence.
                try budget.consumeBytes(2);
                try budget.consumeBytes(try jsonStringEncodedLengthUpperBound("ref"));
                try budget.consumeBytes(1);
                try chargeJsonValue(budget, reference, depth + 1);
                return try db_mod.types.cloneJsonValue(alloc, value);
            }
            const binding = bindings.get(reference.string) orelse return error.InvalidQueryRequest;
            const active_entry = try active.getOrPut(alloc, reference.string);
            if (active_entry.found_existing) return error.InvalidQueryRequest;
            defer _ = active.remove(reference.string);
            const expanded = try expandPublicDocFilterQueryValueAlloc(
                alloc,
                binding,
                bindings,
                text_bindings,
                active,
                depth + 1,
                budget,
            );
            return try wrapExpandedDocFilterBindingAlloc(alloc, expanded, budget, depth + 1);
        }
    }

    try budget.consumeBytes(2 +| if (value.object.count() > 0) value.object.count() - 1 else 0);
    var out = std.json.ObjectMap.empty;
    errdefer {
        var out_value: std.json.Value = .{ .object = out };
        db_mod.types.deinitJsonValue(alloc, &out_value);
    }
    var it = value.object.iterator();
    while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        const child = entry.value_ptr.*;
        try budget.consumeBytes(try jsonStringEncodedLengthUpperBound(key));
        try budget.consumeBytes(1);
        const expands_query_children =
            std.mem.eql(u8, key, "conjuncts") or
            std.mem.eql(u8, key, "disjuncts") or
            std.mem.eql(u8, key, "must") or
            std.mem.eql(u8, key, "should") or
            std.mem.eql(u8, key, "filter") or
            std.mem.eql(u8, key, "must_not");
        if (expands_query_children) {
            try putOwnedJsonValue(
                alloc,
                &out,
                key,
                try expandPublicDocFilterQueryValueAlloc(
                    alloc,
                    child,
                    bindings,
                    text_bindings,
                    active,
                    depth + 1,
                    budget,
                ),
            );
        } else if (std.mem.eql(u8, key, "bool") and child == .object) {
            // The bool payload is a branch container, not itself a query node.
            try budget.consumeNode(depth + 1);
            try budget.consumeBytes(2 +| if (child.object.count() > 0) child.object.count() - 1 else 0);
            var bool_out = std.json.ObjectMap.empty;
            errdefer {
                var bool_value: std.json.Value = .{ .object = bool_out };
                db_mod.types.deinitJsonValue(alloc, &bool_value);
            }
            var bool_it = child.object.iterator();
            while (bool_it.next()) |bool_entry| {
                const bool_key = bool_entry.key_ptr.*;
                try budget.consumeBytes(try jsonStringEncodedLengthUpperBound(bool_key));
                try budget.consumeBytes(1);
                const is_branch =
                    std.mem.eql(u8, bool_key, "must") or
                    std.mem.eql(u8, bool_key, "should") or
                    std.mem.eql(u8, bool_key, "filter") or
                    std.mem.eql(u8, bool_key, "must_not");
                const bool_child = if (is_branch)
                    try expandPublicDocFilterQueryValueAlloc(
                        alloc,
                        bool_entry.value_ptr.*,
                        bindings,
                        text_bindings,
                        active,
                        depth + 1,
                        budget,
                    )
                else
                    try cloneJsonValueBudgeted(
                        alloc,
                        bool_entry.value_ptr.*,
                        budget,
                        depth + 2,
                    );
                try putOwnedJsonValue(alloc, &bool_out, bool_key, bool_child);
            }
            try putOwnedJsonValue(alloc, &out, key, .{ .object = bool_out });
        } else {
            try putOwnedJsonValue(
                alloc,
                &out,
                key,
                try cloneJsonValueBudgeted(alloc, child, budget, depth + 1),
            );
        }
    }
    return .{ .object = out };
}

const PublicBindingVisitState = enum { visiting, done };

fn validatePublicBindingGraphValueAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    bindings: std.json.ObjectMap,
    states: *std.StringHashMapUnmanaged(PublicBindingVisitState),
    depth: usize,
    deadline_ns: ?u64,
    visited: *usize,
) anyerror!void {
    visited.* += 1;
    if (visited.* & 63 == 0) try ensureQueryDeadline(deadline_ns);
    if (depth > public_query_max_tree_depth) return error.InvalidQueryRequest;
    if (value == .array) {
        for (value.array.items) |item| {
            try validatePublicBindingGraphValueAlloc(
                alloc,
                item,
                bindings,
                states,
                depth + 1,
                deadline_ns,
                visited,
            );
        }
        return;
    }
    if (value != .object) return;
    if (value.object.count() == 1) {
        if (value.object.get("ref")) |reference| {
            if (reference != .string or reference.string.len == 0) return error.InvalidQueryRequest;
            try validatePublicBindingGraphNameAlloc(
                alloc,
                reference.string,
                bindings,
                states,
                depth + 1,
                deadline_ns,
                visited,
            );
            return;
        }
    }

    var it = value.object.iterator();
    while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        const child = entry.value_ptr.*;
        const traverses_query_children =
            std.mem.eql(u8, key, "conjuncts") or
            std.mem.eql(u8, key, "disjuncts") or
            std.mem.eql(u8, key, "must") or
            std.mem.eql(u8, key, "should") or
            std.mem.eql(u8, key, "filter") or
            std.mem.eql(u8, key, "must_not");
        if (traverses_query_children) {
            try validatePublicBindingGraphValueAlloc(
                alloc,
                child,
                bindings,
                states,
                depth + 1,
                deadline_ns,
                visited,
            );
        } else if (std.mem.eql(u8, key, "bool") and child == .object) {
            var bool_it = child.object.iterator();
            while (bool_it.next()) |bool_entry| {
                const bool_key = bool_entry.key_ptr.*;
                if (std.mem.eql(u8, bool_key, "must") or
                    std.mem.eql(u8, bool_key, "should") or
                    std.mem.eql(u8, bool_key, "filter") or
                    std.mem.eql(u8, bool_key, "must_not"))
                {
                    try validatePublicBindingGraphValueAlloc(
                        alloc,
                        bool_entry.value_ptr.*,
                        bindings,
                        states,
                        depth + 1,
                        deadline_ns,
                        visited,
                    );
                }
            }
        }
    }
}

fn validatePublicBindingGraphNameAlloc(
    alloc: std.mem.Allocator,
    name: []const u8,
    bindings: std.json.ObjectMap,
    states: *std.StringHashMapUnmanaged(PublicBindingVisitState),
    depth: usize,
    deadline_ns: ?u64,
    visited: *usize,
) anyerror!void {
    visited.* += 1;
    if (visited.* & 63 == 0) try ensureQueryDeadline(deadline_ns);
    if (depth > public_query_max_tree_depth) return error.InvalidQueryRequest;
    if (states.get(name)) |state| {
        if (state == .visiting) return error.InvalidQueryRequest;
        return;
    }
    const binding = bindings.get(name) orelse return error.InvalidQueryRequest;
    try states.put(alloc, name, .visiting);
    try validatePublicBindingGraphValueAlloc(
        alloc,
        binding,
        bindings,
        states,
        depth + 1,
        deadline_ns,
        visited,
    );
    try states.put(alloc, name, .done);
}

fn validatePublicBindingGraphAlloc(
    alloc: std.mem.Allocator,
    bindings: std.json.ObjectMap,
    deadline_ns: ?u64,
) !void {
    var states = std.StringHashMapUnmanaged(PublicBindingVisitState).empty;
    defer states.deinit(alloc);
    var visited: usize = 0;
    var it = bindings.iterator();
    while (it.next()) |entry| {
        if (entry.key_ptr.*.len == 0) return error.InvalidQueryRequest;
        try validatePublicBindingGraphNameAlloc(
            alloc,
            entry.key_ptr.*,
            bindings,
            &states,
            0,
            deadline_ns,
            &visited,
        );
    }
}

fn publicBindingValueReferencesTextBindingAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    bindings: std.json.ObjectMap,
    text_bindings: *std.StringHashMapUnmanaged(bool),
    active: *std.StringHashMapUnmanaged(void),
    deadline_ns: ?u64,
    visited: *usize,
) anyerror!bool {
    visited.* += 1;
    if (visited.* & 63 == 0) try ensureQueryDeadline(deadline_ns);
    if (value == .array) {
        for (value.array.items) |item| {
            if (try publicBindingValueReferencesTextBindingAlloc(
                alloc,
                item,
                bindings,
                text_bindings,
                active,
                deadline_ns,
                visited,
            )) return true;
        }
        return false;
    }
    if (value != .object) return false;
    if (value.object.count() == 1) {
        if (value.object.get("ref")) |reference| {
            if (reference != .string) return error.InvalidQueryRequest;
            return try publicBindingRequiresTextAlloc(
                alloc,
                reference.string,
                bindings,
                text_bindings,
                active,
                deadline_ns,
                visited,
            );
        }
    }

    var it = value.object.iterator();
    while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        const child = entry.value_ptr.*;
        const traverses_query_children =
            std.mem.eql(u8, key, "conjuncts") or
            std.mem.eql(u8, key, "disjuncts") or
            std.mem.eql(u8, key, "must") or
            std.mem.eql(u8, key, "should") or
            std.mem.eql(u8, key, "filter") or
            std.mem.eql(u8, key, "must_not");
        if (traverses_query_children) {
            if (try publicBindingValueReferencesTextBindingAlloc(
                alloc,
                child,
                bindings,
                text_bindings,
                active,
                deadline_ns,
                visited,
            )) return true;
        } else if (std.mem.eql(u8, key, "bool") and child == .object) {
            var bool_it = child.object.iterator();
            while (bool_it.next()) |bool_entry| {
                const bool_key = bool_entry.key_ptr.*;
                if (std.mem.eql(u8, bool_key, "must") or
                    std.mem.eql(u8, bool_key, "should") or
                    std.mem.eql(u8, bool_key, "filter") or
                    std.mem.eql(u8, bool_key, "must_not"))
                {
                    if (try publicBindingValueReferencesTextBindingAlloc(
                        alloc,
                        bool_entry.value_ptr.*,
                        bindings,
                        text_bindings,
                        active,
                        deadline_ns,
                        visited,
                    )) return true;
                }
            }
        }
    }
    return false;
}

fn publicBindingRequiresTextAlloc(
    alloc: std.mem.Allocator,
    name: []const u8,
    bindings: std.json.ObjectMap,
    text_bindings: *std.StringHashMapUnmanaged(bool),
    active: *std.StringHashMapUnmanaged(void),
    deadline_ns: ?u64,
    visited: *usize,
) anyerror!bool {
    visited.* += 1;
    if (visited.* & 63 == 0) try ensureQueryDeadline(deadline_ns);
    if (text_bindings.get(name)) |requires_text| return requires_text;
    const binding = bindings.get(name) orelse return error.InvalidQueryRequest;
    const active_entry = try active.getOrPut(alloc, name);
    if (active_entry.found_existing) return error.InvalidQueryRequest;
    defer _ = active.remove(name);

    // Validate every definition, including unused definitions, against one of
    // the actual execution paths. Unsupported structured syntax is not assumed
    // to be text syntax: the text parser must accept it.
    var clauses = std.ArrayListUnmanaged([]u8).empty;
    defer deinitOwnedStringArrayList(alloc, &clauses);
    var text_queries = std.ArrayListUnmanaged(db_mod.types.TextQuery).empty;
    defer deinitTextQueryArrayList(alloc, &text_queries);
    try appendPublicFilterOrTextClausesAlloc(
        alloc,
        &clauses,
        &text_queries,
        binding,
        std.math.maxInt(u32),
    );
    try ensureQueryDeadline(deadline_ns);
    const directly_requires_text = text_queries.items.len > 0;
    const requires_text = directly_requires_text or
        try publicBindingValueReferencesTextBindingAlloc(
            alloc,
            binding,
            bindings,
            text_bindings,
            active,
            deadline_ns,
            visited,
        );
    try text_bindings.put(alloc, name, requires_text);
    return requires_text;
}

fn classifyPublicTextBindingsAlloc(
    alloc: std.mem.Allocator,
    bindings: std.json.ObjectMap,
    deadline_ns: ?u64,
) !std.StringHashMapUnmanaged(bool) {
    var text_bindings = std.StringHashMapUnmanaged(bool).empty;
    errdefer text_bindings.deinit(alloc);
    var active = std.StringHashMapUnmanaged(void).empty;
    defer active.deinit(alloc);
    var visited: usize = 0;
    var it = bindings.iterator();
    while (it.next()) |entry| {
        _ = try publicBindingRequiresTextAlloc(
            alloc,
            entry.key_ptr.*,
            bindings,
            &text_bindings,
            &active,
            deadline_ns,
            &visited,
        );
    }
    return text_bindings;
}

fn countStructuredPublicBindings(
    bindings: std.json.ObjectMap,
    text_bindings: *const std.StringHashMapUnmanaged(bool),
) usize {
    var count: usize = 0;
    var it = bindings.iterator();
    while (it.next()) |entry| {
        if (!(text_bindings.get(entry.key_ptr.*) orelse false)) count += 1;
    }
    return count;
}

fn cloneStructuredPublicBindingsBudgeted(
    alloc: std.mem.Allocator,
    bindings: std.json.ObjectMap,
    text_bindings: *const std.StringHashMapUnmanaged(bool),
    retained_count: usize,
    budget: *PublicBindingExpansionBudget,
    depth: usize,
) !std.json.Value {
    try budget.consumeNode(depth);
    try budget.consumeBytes(2 +| if (retained_count > 0) retained_count - 1 else 0);

    var out = std.json.ObjectMap.empty;
    errdefer {
        var out_value: std.json.Value = .{ .object = out };
        db_mod.types.deinitJsonValue(alloc, &out_value);
    }
    try out.ensureTotalCapacity(alloc, retained_count);
    var it = bindings.iterator();
    while (it.next()) |entry| {
        const name = entry.key_ptr.*;
        if (text_bindings.get(name) orelse false) continue;
        try budget.consumeBytes(try jsonStringEncodedLengthUpperBound(name));
        try budget.consumeBytes(1);
        try putOwnedJsonValue(
            alloc,
            &out,
            name,
            try cloneJsonValueBudgeted(alloc, entry.value_ptr.*, budget, depth + 1),
        );
    }
    return .{ .object = out };
}

fn publicBindingExpansionOutputLimit(input_bytes: usize) !usize {
    return std.math.add(
        usize,
        input_bytes,
        public_limits.max_query_binding_expansion_growth_bytes,
    ) catch error.InvalidQueryRequest;
}

fn maybeExpandPublicDocFilterBindingsAlloc(
    alloc: std.mem.Allocator,
    body: []const u8,
    deadline_ns: ?u64,
) !?[]u8 {
    if (std.mem.indexOf(u8, body, "\"with\"") == null) return null;
    const max_expanded_bytes = try publicBindingExpansionOutputLimit(body.len);
    return try maybeExpandPublicDocFilterBindingsWithLimitAlloc(
        alloc,
        body,
        max_expanded_bytes,
        deadline_ns,
    );
}

fn expandPublicDocFilterBindingsWithLimitAlloc(
    alloc: std.mem.Allocator,
    body: []const u8,
    max_expanded_bytes: usize,
) ![]u8 {
    return (try maybeExpandPublicDocFilterBindingsWithLimitAlloc(
        alloc,
        body,
        max_expanded_bytes,
        null,
    )) orelse return error.InvalidQueryRequest;
}

fn maybeExpandPublicDocFilterBindingsWithLimitAlloc(
    alloc: std.mem.Allocator,
    body: []const u8,
    max_expanded_bytes: usize,
    deadline_ns: ?u64,
) !?[]u8 {
    if (max_expanded_bytes == 0) return error.InvalidQueryRequest;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{}) catch return error.InvalidQueryRequest;
    defer parsed.deinit();
    try ensureQueryDeadline(deadline_ns);
    if (parsed.value != .object) return error.InvalidQueryRequest;
    try validatePublicQueryTraversalBudgetWithDeadlineAlloc(alloc, parsed.value, deadline_ns);
    try ensureQueryDeadline(deadline_ns);
    const bindings_value = parsed.value.object.get("with") orelse return null;
    if (bindings_value != .object) return error.InvalidQueryRequest;
    try validatePublicBindingGraphAlloc(alloc, bindings_value.object, deadline_ns);
    try ensureQueryDeadline(deadline_ns);
    var text_bindings = try classifyPublicTextBindingsAlloc(
        alloc,
        bindings_value.object,
        deadline_ns,
    );
    defer text_bindings.deinit(alloc);
    try ensureQueryDeadline(deadline_ns);
    var requires_expansion = false;
    var text_it = text_bindings.iterator();
    while (text_it.next()) |entry| {
        if (entry.value_ptr.*) {
            requires_expansion = true;
            break;
        }
    }
    if (!requires_expansion) return null;

    var out = std.json.ObjectMap.empty;
    errdefer {
        var out_value: std.json.Value = .{ .object = out };
        db_mod.types.deinitJsonValue(alloc, &out_value);
    }
    var budget = PublicBindingExpansionBudget{
        .remaining_bytes = max_expanded_bytes,
        .deadline_ns = deadline_ns,
    };
    try budget.consumeNode(0);
    const retained_binding_count = countStructuredPublicBindings(
        bindings_value.object,
        &text_bindings,
    );
    const output_field_count = parsed.value.object.count() -
        @intFromBool(retained_binding_count == 0);
    try budget.consumeBytes(2 +| if (output_field_count > 0) output_field_count - 1 else 0);
    var root_it = parsed.value.object.iterator();
    while (root_it.next()) |entry| {
        const key = entry.key_ptr.*;
        if (std.mem.eql(u8, key, "with") and retained_binding_count == 0) continue;
        try budget.consumeBytes(try jsonStringEncodedLengthUpperBound(key));
        try budget.consumeBytes(1);

        const is_query_root =
            std.mem.eql(u8, key, "query") or
            std.mem.eql(u8, key, "full_text_search") or
            std.mem.eql(u8, key, "filter_query") or
            std.mem.eql(u8, key, "exclusion_query");
        var owned_value: std.json.Value = undefined;
        if (std.mem.eql(u8, key, "with")) {
            owned_value = try cloneStructuredPublicBindingsBudgeted(
                alloc,
                bindings_value.object,
                &text_bindings,
                retained_binding_count,
                &budget,
                1,
            );
        } else if (is_query_root) {
            var active = std.StringHashMapUnmanaged(void).empty;
            defer active.deinit(alloc);
            owned_value = try expandPublicDocFilterQueryValueAlloc(
                alloc,
                entry.value_ptr.*,
                bindings_value.object,
                &text_bindings,
                &active,
                0,
                &budget,
            );
        } else if ((std.mem.eql(u8, key, "_filter_query_json") or
            std.mem.eql(u8, key, "_exclusion_query_json")) and
            entry.value_ptr.* == .string)
        {
            var internal = std.json.parseFromSlice(
                std.json.Value,
                alloc,
                entry.value_ptr.*.string,
                .{},
            ) catch return error.InvalidQueryRequest;
            defer internal.deinit();
            try ensureQueryDeadline(deadline_ns);
            var active = std.StringHashMapUnmanaged(void).empty;
            defer active.deinit(alloc);
            var expanded = try expandPublicDocFilterQueryValueAlloc(
                alloc,
                internal.value,
                bindings_value.object,
                &text_bindings,
                &active,
                0,
                &budget,
            );
            defer db_mod.types.deinitJsonValue(alloc, &expanded);
            const expanded_json = try std.json.Stringify.valueAlloc(alloc, expanded, .{});
            budget.consumeNode(1) catch |err| {
                alloc.free(expanded_json);
                return err;
            };
            const encoded_string_len = jsonStringEncodedLengthUpperBound(expanded_json) catch |err| {
                alloc.free(expanded_json);
                return err;
            };
            budget.consumeBytes(encoded_string_len) catch |err| {
                alloc.free(expanded_json);
                return err;
            };
            owned_value = .{
                .string = expanded_json,
            };
        } else {
            owned_value = try cloneJsonValueBudgeted(
                alloc,
                entry.value_ptr.*,
                &budget,
                1,
            );
        }
        try putOwnedJsonValue(alloc, &out, key, owned_value);
    }

    var out_value: std.json.Value = .{ .object = out };
    defer db_mod.types.deinitJsonValue(alloc, &out_value);
    try ensureQueryDeadline(deadline_ns);
    const encoded = try std.json.Stringify.valueAlloc(alloc, out_value, .{});
    errdefer alloc.free(encoded);
    try ensureQueryDeadline(deadline_ns);
    if (encoded.len > max_expanded_bytes) {
        alloc.free(encoded);
        return error.InvalidQueryRequest;
    }
    return encoded;
}

fn queryBodyHasForbiddenDocIdentityControlFields(alloc: std.mem.Allocator, body: []const u8) !bool {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{}) catch return error.InvalidQueryRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidQueryRequest;
    return parsed.value.object.get("identity_read_generation") != null or
        parsed.value.object.get("allow_doc_identity_reassignment") != null;
}

fn queryBodyHasForbiddenPublicDocIdentityControlFields(alloc: std.mem.Allocator, body: []const u8) !bool {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{}) catch return error.InvalidQueryRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidQueryRequest;
    return objectHasForbiddenPublicQueryField(parsed.value.object);
}

fn queryBodyHasInternalShardFields(alloc: std.mem.Allocator, body: []const u8) !bool {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{}) catch return error.InvalidQueryRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidQueryRequest;
    return objectHasInternalShardField(parsed.value.object);
}

const QueryContractStripOptions = struct {
    strip_internal_shard_fields: bool = false,
    strip_public_doc_filter_bindings: bool = false,
    strip_public_hierarchy_controls: bool = false,
    strip_query_timeout: bool = false,
};

fn queryBodyForGeneratedContractAlloc(
    alloc: std.mem.Allocator,
    body: []const u8,
    options: QueryContractStripOptions,
) !?[]u8 {
    if (!options.strip_internal_shard_fields and !options.strip_public_doc_filter_bindings and !options.strip_public_hierarchy_controls and !options.strip_query_timeout) return null;

    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{}) catch return error.InvalidQueryRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidQueryRequest;

    if (options.strip_public_doc_filter_bindings) {
        _ = parsed.value.object.orderedRemove("with");
    }
    if (options.strip_public_hierarchy_controls) {
        _ = parsed.value.object.orderedRemove("hierarchy");
    }
    if (options.strip_internal_shard_fields) {
        removeInternalShardFields(&parsed.value.object);
    }
    if (options.strip_query_timeout) {
        _ = parsed.value.object.orderedRemove("timeout_ms");
    }

    return try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
}

fn isInternalShardFieldName(name: []const u8) bool {
    const internal_fields = [_][]const u8{
        "_distributed_text_stats",
        "native_doc_id_constraints",
        "_filter_query_json",
        "_exclusion_query_json",
        "_identity_read_generation",
        "_defer_hierarchy_child_hydration",
        db_mod.doc_filter_wire.field_name,
        "_filter_doc_ids",
        "_filter_doc_ids_positive",
        "_exclude_doc_ids",
    };
    inline for (internal_fields) |field| {
        if (std.mem.eql(u8, name, field)) return true;
    }
    return false;
}

fn objectHasInternalShardField(object: std.json.ObjectMap) bool {
    var it = object.iterator();
    while (it.next()) |entry| {
        if (isInternalShardFieldName(entry.key_ptr.*)) return true;
    }
    return false;
}

fn objectHasForbiddenPublicQueryField(object: std.json.ObjectMap) bool {
    return object.get("identity_read_generation") != null or
        object.get("allow_doc_identity_reassignment") != null or
        objectHasInternalShardField(object);
}

fn objectHasNonNullField(object: std.json.ObjectMap, name: []const u8) bool {
    const value = object.get(name) orelse return false;
    return value != .null;
}

fn queryBodyHasPublicHierarchyControls(alloc: std.mem.Allocator, body: []const u8) !bool {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{}) catch return error.InvalidQueryRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidQueryRequest;
    return objectHasNonNullField(parsed.value.object, "hierarchy");
}

fn applyPublicHierarchyControls(
    alloc: std.mem.Allocator,
    body: []const u8,
    req: *db_mod.types.SearchRequest,
) !void {
    if (!(try queryBodyHasPublicHierarchyControls(alloc, body))) return;

    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{}) catch return error.InvalidQueryRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidQueryRequest;
    const hierarchy = parsed.value.object.get("hierarchy") orelse return;
    if (hierarchy != .object) return error.InvalidQueryRequest;

    var return_level: ?[]const u8 = null;
    var rollup: ?[]const u8 = null;
    var include_chunk = false;
    var max_children_set = false;
    var group_by_set = false;
    var children_set = false;
    var grouped_matches_set = false;
    // Presence of an empty hierarchy object is the canonical direct-match form.
    // Omitting hierarchy entirely retains the legacy default result shape.
    var has_new_controls = hierarchy.object.count() == 0;
    var has_legacy_controls = false;

    var style_it = hierarchy.object.iterator();
    while (style_it.next()) |entry| {
        const key = entry.key_ptr.*;
        if (std.mem.eql(u8, key, "group_by") or
            std.mem.eql(u8, key, "children") or
            std.mem.eql(u8, key, "ancestors"))
        {
            has_new_controls = true;
            if (std.mem.eql(u8, key, "group_by")) group_by_set = true;
            if (std.mem.eql(u8, key, "children")) children_set = true;
        } else if (std.mem.eql(u8, key, "return_level") or
            std.mem.eql(u8, key, "rollup") or
            std.mem.eql(u8, key, "include") or
            std.mem.eql(u8, key, "max_children_per_parent"))
        {
            has_legacy_controls = true;
        } else {
            return error.InvalidQueryRequest;
        }
    }
    if (has_new_controls and has_legacy_controls) return error.InvalidQueryRequest;

    var it = hierarchy.object.iterator();
    while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        const value = entry.value_ptr.*;
        if (std.mem.eql(u8, key, "return_level")) {
            if (value != .string) return error.InvalidQueryRequest;
            if (std.mem.eql(u8, value.string, "source") or std.mem.eql(u8, value.string, "unit") or std.mem.eql(u8, value.string, "chunk") or std.mem.eql(u8, value.string, "mention")) {
                return_level = value.string;
            } else {
                return error.InvalidQueryRequest;
            }
        } else if (std.mem.eql(u8, key, "rollup")) {
            if (value != .string) return error.InvalidQueryRequest;
            if (std.mem.eql(u8, value.string, "source") or std.mem.eql(u8, value.string, "none")) {
                rollup = value.string;
            } else if (std.mem.eql(u8, value.string, "unit") or std.mem.eql(u8, value.string, "mention")) {
                return error.UnsupportedQueryRequest;
            } else {
                return error.InvalidQueryRequest;
            }
        } else if (std.mem.eql(u8, key, "include")) {
            if (value != .array) return error.InvalidQueryRequest;
            for (value.array.items) |item| {
                if (item != .string) return error.InvalidQueryRequest;
                if (std.mem.eql(u8, item.string, "chunk") or std.mem.eql(u8, item.string, "chunks")) {
                    include_chunk = true;
                } else if (std.mem.eql(u8, item.string, "source")) {
                    req.hierarchy_include_source = true;
                } else if (std.mem.eql(u8, item.string, "unit")) {
                    req.hierarchy_include_unit = true;
                } else if (std.mem.eql(u8, item.string, "mention") or std.mem.eql(u8, item.string, "mentions")) {
                    // Mention evidence is returned as the hit itself for now;
                    // there is no descendant grouping knob to set here.
                } else {
                    return error.InvalidQueryRequest;
                }
            }
        } else if (std.mem.eql(u8, key, "max_children_per_parent")) {
            req.max_chunks_per_parent = try parseOptionalU32Json(value, req.max_chunks_per_parent);
            max_children_set = true;
        } else if (std.mem.eql(u8, key, "group_by")) {
            if (value != .object) return error.InvalidQueryRequest;
            var level_set = false;

            var group_it = value.object.iterator();
            while (group_it.next()) |group_entry| {
                const group_key = group_entry.key_ptr.*;
                const group_value = group_entry.value_ptr.*;
                if (std.mem.eql(u8, group_key, "level")) {
                    if (group_value != .string) return error.InvalidQueryRequest;
                    if (std.mem.eql(u8, group_value.string, "source")) {
                        req.hierarchy_group_level = .source;
                    } else if (std.mem.eql(u8, group_value.string, "unit")) {
                        req.hierarchy_group_level = .unit;
                    } else {
                        return error.UnsupportedQueryRequest;
                    }
                    level_set = true;
                } else if (std.mem.eql(u8, group_key, "matches")) {
                    if (group_value != .object) return error.InvalidQueryRequest;
                    grouped_matches_set = true;
                    include_chunk = true;
                    req.max_chunks_per_parent = 3;
                    req.hierarchy_grouped_matches = true;
                    var fields_set = false;
                    var matches_it = group_value.object.iterator();
                    while (matches_it.next()) |matches_entry| {
                        const matches_key = matches_entry.key_ptr.*;
                        const matches_value = matches_entry.value_ptr.*;
                        if (std.mem.eql(u8, matches_key, "limit")) {
                            req.max_chunks_per_parent = try parseOptionalU32Json(matches_value, req.max_chunks_per_parent);
                            if (req.max_chunks_per_parent == 0 or
                                req.max_chunks_per_parent > db_mod.types.max_canonical_hierarchy_matches_per_group)
                            {
                                return error.InvalidQueryRequest;
                            }
                        } else if (std.mem.eql(u8, matches_key, "fields")) {
                            req.hierarchy_match_fields = try parseStringArrayJsonAlloc(alloc, matches_value);
                            req.hierarchy_match_include_all_fields = false;
                            fields_set = true;
                        } else {
                            return error.InvalidQueryRequest;
                        }
                    }
                    if (!fields_set) return error.InvalidQueryRequest;
                } else {
                    return error.InvalidQueryRequest;
                }
            }
            if (!level_set) return error.InvalidQueryRequest;
            req.hierarchy_omit_implicit_source_ancestor_document = req.hierarchy_group_level == .source;
        } else if (std.mem.eql(u8, key, "children")) {
            if (value != .object or req.hierarchy_children != null) return error.InvalidQueryRequest;
            const parent = value.object.get("parent") orelse return error.InvalidQueryRequest;
            const level = value.object.get("level") orelse return error.InvalidQueryRequest;
            if (value.object.count() != 2 or parent != .object or level != .string) return error.InvalidQueryRequest;
            if (!std.mem.eql(u8, level.string, "unit")) return error.UnsupportedQueryRequest;
            const parent_level = parent.object.get("level") orelse return error.InvalidQueryRequest;
            const parent_id = parent.object.get("id") orelse return error.InvalidQueryRequest;
            if (parent.object.count() != 2 or parent_level != .string or parent_id != .string or parent_id.string.len == 0) {
                return error.InvalidQueryRequest;
            }
            if (!std.mem.eql(u8, parent_level.string, "source")) return error.UnsupportedQueryRequest;
            req.hierarchy_children = .{
                .parent_id = try alloc.dupe(u8, parent_id.string),
                .parent_level = .source,
                .level = .unit,
            };
        } else if (std.mem.eql(u8, key, "ancestors")) {
            if (value != .object or value.object.count() == 0) return error.InvalidQueryRequest;
            var ancestor_it = value.object.iterator();
            while (ancestor_it.next()) |ancestor_entry| {
                const ancestor_key = ancestor_entry.key_ptr.*;
                const ancestor_value = ancestor_entry.value_ptr.*;
                if (ancestor_value != .object) return error.InvalidQueryRequest;

                const is_source = std.mem.eql(u8, ancestor_key, "source");
                const is_unit = std.mem.eql(u8, ancestor_key, "unit");
                if (!is_source and !is_unit) return error.InvalidQueryRequest;
                if (is_source) req.hierarchy_include_source = true;
                if (is_unit) req.hierarchy_include_unit = true;
                var fields_set = false;

                var projection_it = ancestor_value.object.iterator();
                while (projection_it.next()) |projection_entry| {
                    if (!std.mem.eql(u8, projection_entry.key_ptr.*, "fields")) return error.InvalidQueryRequest;
                    const fields = try parseStringArrayJsonAlloc(alloc, projection_entry.value_ptr.*);
                    fields_set = true;
                    if (is_source) {
                        req.hierarchy_source_fields = fields;
                        req.hierarchy_source_include_all_fields = false;
                    } else {
                        req.hierarchy_unit_fields = fields;
                        req.hierarchy_unit_include_all_fields = false;
                    }
                }
                if (!fields_set) return error.InvalidQueryRequest;
            }
        }
    }

    if (has_new_controls) {
        if (children_set) {
            if (group_by_set or hierarchy.object.get("ancestors") != null) return error.InvalidQueryRequest;
            if (!objectHasNonNullField(parsed.value.object, "fields")) return error.InvalidQueryRequest;
            var root_it = parsed.value.object.iterator();
            while (root_it.next()) |entry| {
                if (entry.value_ptr.* == .null) continue;
                const root_key = entry.key_ptr.*;
                if (!std.mem.eql(u8, root_key, "table") and
                    !std.mem.eql(u8, root_key, "fields") and
                    !std.mem.eql(u8, root_key, "hierarchy") and
                    !std.mem.eql(u8, root_key, "limit") and
                    !std.mem.eql(u8, root_key, "timeout_ms") and
                    !std.mem.eql(u8, root_key, "order_by") and
                    !std.mem.eql(u8, root_key, "search_after") and
                    !isInternalShardFieldName(root_key))
                {
                    // Sequential traversal has no relevance/filter execution
                    // phase. Reject ignored knobs instead of silently giving
                    // callers a result set they did not ask for.
                    return error.InvalidQueryRequest;
                }
            }
            if (req.offset != 0 or req.count_only or req.aggregations_json.len > 0) return error.InvalidQueryRequest;
            if (req.limit == 0 or req.limit > 100) return error.InvalidQueryRequest;
            if (req.order_by.len != 2 or
                !std.mem.eql(u8, req.order_by[0].field, "_hierarchy.position") or
                req.order_by[0].desc or
                !std.mem.eql(u8, req.order_by[1].field, "_id") or
                req.order_by[1].desc)
            {
                return error.InvalidQueryRequest;
            }
            if (req.search_after.len > 0 and
                (req.search_after.len != 2 or req.search_after[0] != .string or req.search_after[1] != .string))
            {
                return error.InvalidQueryRequest;
            }
            if (req.search_before.len > 0) return error.InvalidQueryRequest;
            req.return_mode = .unit;
        } else if (group_by_set) {
            // Group hits hydrate source documents, which may contain large
            // stored child materializations. Require an explicit projection
            // so canonical grouping cannot accidentally recreate the
            // unbounded `_chunks.*` response shape it is intended to avoid.
            if (!objectHasNonNullField(parsed.value.object, "fields")) return error.InvalidQueryRequest;
            // A source group already carries its projected source document in
            // the top-level hit. Repeating it on the group and every nested
            // match defeats the bounded hierarchy response contract.
            if (req.hierarchy_group_level == .source and req.hierarchy_include_source) return error.InvalidQueryRequest;
            // Unit grouping is a relevance operation over chunk-backed
            // candidates. The grouped unit does not have a canonical stored
            // field sort tuple, so accepting order/cursor controls would
            // produce non-replayable `_sort` values after collapse.
            if (req.hierarchy_group_level == .unit and
                (req.order_by.len > 0 or req.search_after.len > 0 or req.search_before.len > 0))
            {
                const field = if (req.order_by.len > 0) req.order_by[0].field else "*";
                return unsupportedExactSort(field, "unsupported_exact_sort", "unsupported_exact_sort");
            }
            req.return_mode = switch (req.hierarchy_group_level) {
                .source => if (grouped_matches_set) .parent_with_chunks else .parent,
                .unit => if (grouped_matches_set) .unit_with_chunks else .unit,
            };
        } else {
            req.return_mode = .member;
        }
        return;
    }

    const has_child_limit = max_children_set and req.max_chunks_per_parent > 0;
    const wants_source_rollup = if (rollup) |value| std.mem.eql(u8, value, "source") else false;
    const wants_no_rollup = if (rollup) |value| std.mem.eql(u8, value, "none") else false;

    if (return_level) |level| {
        if (std.mem.eql(u8, level, "chunk") or std.mem.eql(u8, level, "unit") or std.mem.eql(u8, level, "mention")) {
            req.return_mode = if (wants_source_rollup or include_chunk or has_child_limit)
                .parent_with_chunks
            else
                .chunk;
        } else {
            req.return_mode = if (include_chunk or has_child_limit)
                .parent_with_chunks
            else
                .parent;
        }
    } else if (wants_source_rollup) {
        req.return_mode = if (include_chunk or has_child_limit)
            .parent_with_chunks
        else
            .parent;
    } else if (wants_no_rollup) {
        req.return_mode = .chunk;
    } else if (include_chunk or has_child_limit) {
        req.return_mode = .parent_with_chunks;
    }
}

fn validateCanonicalHierarchyExecutionBudget(req: db_mod.types.SearchRequest) !void {
    if (!db_mod.types.canonicalHierarchyExecutionWithinBudget(req)) return error.InvalidQueryRequest;
}

fn removeInternalShardFields(object: *std.json.ObjectMap) void {
    const internal_fields = [_][]const u8{
        "_distributed_text_stats",
        "native_doc_id_constraints",
        "_filter_query_json",
        "_exclusion_query_json",
        "_identity_read_generation",
        "_defer_hierarchy_child_hydration",
        db_mod.doc_filter_wire.field_name,
        "_filter_doc_ids",
        "_filter_doc_ids_positive",
        "_exclude_doc_ids",
    };
    inline for (internal_fields) |field| {
        _ = object.orderedRemove(field);
    }
}

fn parsePublicDocFilterBindingsAlloc(
    alloc: std.mem.Allocator,
    body: []const u8,
    limit: u32,
    deadline_ns: ?u64,
) ![]const db_mod.types.NamedDocFilterBinding {
    if (std.mem.indexOf(u8, body, "\"with\"") == null) return &.{};

    var deadline = QueryDeadlinePoller{ .deadline_ns = deadline_ns };
    try deadline.check();
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{}) catch return error.InvalidQueryRequest;
    defer parsed.deinit();
    try deadline.check();
    if (parsed.value != .object) return error.InvalidQueryRequest;
    try validatePublicQueryTraversalBudgetWithDeadlineAlloc(alloc, parsed.value, deadline_ns);
    try deadline.check();
    const with_value = parsed.value.object.get("with") orelse return &.{};
    if (with_value != .object) return error.InvalidQueryRequest;

    var out = std.ArrayListUnmanaged(db_mod.types.NamedDocFilterBinding).empty;
    errdefer {
        for (out.items) |binding| {
            alloc.free(@constCast(binding.name));
            alloc.free(@constCast(binding.filter_query_json));
        }
        out.deinit(alloc);
    }
    try out.ensureTotalCapacity(alloc, with_value.object.count());
    var it = with_value.object.iterator();
    while (it.next()) |entry| {
        try deadline.poll();
        if (entry.key_ptr.*.len == 0) return error.InvalidQueryRequest;

        var clauses = std.ArrayListUnmanaged([]u8).empty;
        defer deinitOwnedStringArrayList(alloc, &clauses);
        try appendPublicFilterClausesAlloc(alloc, &clauses, entry.value_ptr.*, limit);
        var filter_query_json = try buildStructuredFilterClausesJsonAlloc(alloc, clauses.items, .all);
        errdefer if (filter_query_json.len > 0) alloc.free(filter_query_json);
        try deadline.check();
        if (filter_query_json.len == 0) return error.InvalidQueryRequest;
        var name = try alloc.dupe(u8, entry.key_ptr.*);
        errdefer if (name.len > 0) alloc.free(name);
        try out.append(alloc, .{
            .name = name,
            .filter_query_json = filter_query_json,
        });
        name = &.{};
        filter_query_json = "";
    }

    const order = try sortPublicDocFilterBindingsByDependenciesAlloc(
        alloc,
        out.items,
        deadline_ns,
    );
    defer alloc.free(order);
    try deadline.check();
    const sorted = try alloc.alloc(db_mod.types.NamedDocFilterBinding, out.items.len);
    errdefer alloc.free(sorted);
    for (order, 0..) |source_index, target_index| {
        try deadline.poll();
        sorted[target_index] = out.items[source_index];
    }
    out.deinit(alloc);
    out = .empty;
    return sorted;
}

fn collectPublicDocFilterRefs(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    by_name: *const std.StringHashMapUnmanaged(usize),
    dependencies: ?*std.AutoHashMapUnmanaged(usize, void),
) !void {
    return collectPublicDocFilterRefsWithDeadline(
        alloc,
        value,
        by_name,
        dependencies,
        null,
    );
}

fn collectPublicDocFilterRefsWithDeadline(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    by_name: *const std.StringHashMapUnmanaged(usize),
    dependencies: ?*std.AutoHashMapUnmanaged(usize, void),
    deadline_ns: ?u64,
) !void {
    var remaining_nodes: usize = public_query_max_tree_nodes;
    var deadline = QueryDeadlinePoller{ .deadline_ns = deadline_ns };
    return collectPublicDocFilterRefsBounded(
        alloc,
        value,
        by_name,
        dependencies,
        0,
        &remaining_nodes,
        &deadline,
    );
}

fn collectPublicDocFilterRefsBounded(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    by_name: *const std.StringHashMapUnmanaged(usize),
    dependencies: ?*std.AutoHashMapUnmanaged(usize, void),
    depth: usize,
    remaining_nodes: *usize,
    deadline: *QueryDeadlinePoller,
) !void {
    try deadline.poll();
    if (depth > public_query_max_tree_depth or
        remaining_nodes.* == 0 or
        value != .object or
        value.object.count() != 1)
    {
        return error.InvalidQueryRequest;
    }
    remaining_nodes.* -= 1;
    const object = value.object;
    if (object.get("ref")) |reference| {
        if (reference != .string or reference.string.len == 0) {
            return error.InvalidQueryRequest;
        }
        const dependency = by_name.get(reference.string) orelse {
            return error.InvalidQueryRequest;
        };
        if (dependencies) |items| try items.put(alloc, dependency, {});
        return;
    }
    inline for ([_][]const u8{ "conjuncts", "disjuncts" }) |compound| {
        if (object.get(compound)) |children| {
            if (children != .array or children.array.items.len == 0) {
                return error.InvalidQueryRequest;
            }
            for (children.array.items) |child| {
                try collectPublicDocFilterRefsBounded(
                    alloc,
                    child,
                    by_name,
                    dependencies,
                    depth + 1,
                    remaining_nodes,
                    deadline,
                );
            }
            return;
        }
    }
    if (object.get("bool")) |bool_query| {
        if (bool_query != .object) return error.InvalidQueryRequest;
        inline for ([_][]const u8{ "filter", "must", "should", "must_not" }) |branch| {
            if (bool_query.object.get(branch)) |children| {
                if (children != .array or children.array.items.len == 0) {
                    return error.InvalidQueryRequest;
                }
                for (children.array.items) |child| {
                    try collectPublicDocFilterRefsBounded(
                        alloc,
                        child,
                        by_name,
                        dependencies,
                        depth + 1,
                        remaining_nodes,
                        deadline,
                    );
                }
            }
        }
    }
}

fn sortPublicDocFilterBindingsByDependenciesAlloc(
    alloc: std.mem.Allocator,
    bindings: []const db_mod.types.NamedDocFilterBinding,
    deadline_ns: ?u64,
) ![]usize {
    var deadline = QueryDeadlinePoller{ .deadline_ns = deadline_ns };
    try deadline.check();
    var by_name = std.StringHashMapUnmanaged(usize).empty;
    defer by_name.deinit(alloc);
    for (bindings, 0..) |binding, index| {
        try deadline.poll();
        const result = try by_name.getOrPut(alloc, binding.name);
        if (result.found_existing) return error.InvalidQueryRequest;
        result.value_ptr.* = index;
    }

    const dependents = try alloc.alloc(
        std.ArrayListUnmanaged(usize),
        bindings.len,
    );
    defer {
        for (dependents) |*items| items.deinit(alloc);
        alloc.free(dependents);
    }
    for (dependents) |*items| items.* = .empty;

    const indegrees = try alloc.alloc(usize, bindings.len);
    defer alloc.free(indegrees);
    @memset(indegrees, 0);

    for (bindings, 0..) |binding, binding_index| {
        try deadline.poll();
        var parsed = std.json.parseFromSlice(
            std.json.Value,
            alloc,
            binding.filter_query_json,
            .{},
        ) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return error.InvalidQueryRequest,
        };
        defer parsed.deinit();
        try deadline.check();

        var dependencies = std.AutoHashMapUnmanaged(usize, void).empty;
        defer dependencies.deinit(alloc);
        try collectPublicDocFilterRefsWithDeadline(
            alloc,
            parsed.value,
            &by_name,
            &dependencies,
            deadline_ns,
        );
        var dependency_it = dependencies.keyIterator();
        while (dependency_it.next()) |dependency_index| {
            try deadline.poll();
            try dependents[dependency_index.*].append(alloc, binding_index);
            indegrees[binding_index] += 1;
        }
    }

    const order = try alloc.alloc(usize, bindings.len);
    errdefer alloc.free(order);
    var queue = std.ArrayListUnmanaged(usize).empty;
    defer queue.deinit(alloc);
    for (indegrees, 0..) |indegree, index| {
        try deadline.poll();
        if (indegree == 0) try queue.append(alloc, index);
    }

    var read_index: usize = 0;
    var order_len: usize = 0;
    while (read_index < queue.items.len) : (read_index += 1) {
        try deadline.poll();
        const dependency_index = queue.items[read_index];
        order[order_len] = dependency_index;
        order_len += 1;
        for (dependents[dependency_index].items) |dependent_index| {
            indegrees[dependent_index] -= 1;
            if (indegrees[dependent_index] == 0) {
                try queue.append(alloc, dependent_index);
            }
        }
    }
    if (order_len != bindings.len) return error.InvalidQueryRequest;
    return order;
}

fn validatePublicDocFilterRootRefsAlloc(
    alloc: std.mem.Allocator,
    bindings: []const db_mod.types.NamedDocFilterBinding,
    filter_query_json: []const u8,
    exclusion_query_json: []const u8,
    deadline_ns: ?u64,
) !void {
    var deadline = QueryDeadlinePoller{ .deadline_ns = deadline_ns };
    try deadline.check();
    var by_name = std.StringHashMapUnmanaged(usize).empty;
    defer by_name.deinit(alloc);
    for (bindings, 0..) |binding, index| {
        try deadline.poll();
        try by_name.put(alloc, binding.name, index);
    }
    for ([_][]const u8{ filter_query_json, exclusion_query_json }) |encoded| {
        if (encoded.len == 0) continue;
        var parsed = std.json.parseFromSlice(
            std.json.Value,
            alloc,
            encoded,
            .{},
        ) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return error.InvalidQueryRequest,
        };
        defer parsed.deinit();
        try deadline.check();
        try collectPublicDocFilterRefsWithDeadline(
            alloc,
            parsed.value,
            &by_name,
            null,
            deadline_ns,
        );
    }
}

fn parseInternalFilterQueryJsonAlloc(
    alloc: std.mem.Allocator,
    body: []const u8,
    req: *db_mod.types.SearchRequest,
) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{}) catch return error.InvalidQueryRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidQueryRequest;
    if (parsed.value.object.get("_filter_query_json") == null and
        parsed.value.object.get("_exclusion_query_json") == null) return;

    if (parsed.value.object.get("_filter_query_json")) |value| {
        const query_json = try parseInternalFilterJsonStringAlloc(alloc, value);
        req.filter_query_json = try mergeInternalFilterJsonAlloc(
            alloc,
            req.filter_query_json,
            query_json,
            .all,
        );
    }
    if (parsed.value.object.get("_exclusion_query_json")) |value| {
        const query_json = try parseInternalFilterJsonStringAlloc(alloc, value);
        req.exclusion_query_json = try mergeInternalFilterJsonAlloc(
            alloc,
            req.exclusion_query_json,
            query_json,
            .any,
        );
    }
}

fn mergeInternalFilterJsonAlloc(
    alloc: std.mem.Allocator,
    public_json: []const u8,
    internal_json: []u8,
    mode: StructuredClauseMode,
) ![]const u8 {
    if (public_json.len == 0) return internal_json;

    errdefer alloc.free(internal_json);
    const merged = try buildStructuredFilterClausesJsonAlloc(
        alloc,
        &.{ public_json, internal_json },
        mode,
    );
    alloc.free(@constCast(public_json));
    alloc.free(internal_json);
    return merged;
}

fn parseInternalFilterJsonStringAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]u8 {
    if (value != .string) return error.InvalidQueryRequest;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, value.string, .{}) catch return error.InvalidQueryRequest;
    parsed.deinit();
    return try alloc.dupe(u8, value.string);
}

fn parseInternalDocIdConstraintsAlloc(
    alloc: std.mem.Allocator,
    body: []const u8,
    req: *db_mod.types.SearchRequest,
) !void {
    if (!(try queryBodyHasInternalShardFields(alloc, body))) return;

    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{}) catch return;
    defer parsed.deinit();
    if (parsed.value != .object) return;

    const native_constraints = parsed.value.object.get("native_doc_id_constraints");
    const has_legacy_constraints = parsed.value.object.get("_filter_doc_ids_positive") != null or
        parsed.value.object.get("_filter_doc_ids") != null or
        parsed.value.object.get("_exclude_doc_ids") != null;
    if (has_legacy_constraints) return error.InvalidQueryRequest;

    if (native_constraints) |value| {
        var envelope = try parseNativeDocIdConstraintEnvelopeValueAlloc(alloc, value);
        applyNativeDocIdConstraintEnvelope(req, envelope.constraints);
        envelope.constraints.include_doc_ids = &.{};
        envelope.constraints.exclude_doc_ids = &.{};
        envelope.deinit(alloc);
    }
    if (parsed.value.object.get(db_mod.doc_filter_wire.field_name)) |value| {
        try db_mod.doc_filter_wire.parseIntoSearchRequestAlloc(alloc, value, req);
    }
    if (parsed.value.object.get("_identity_read_generation")) |value| {
        const generation = try parseOptionalU64Json(value);
        if (req.resolved_doc_filter_wire_context) |ctx| {
            if (generation == null or generation.? != ctx.identity_read_generation) return error.InvalidQueryRequest;
        }
        req.identity_read_generation = generation;
    }
    if (parsed.value.object.get("_defer_hierarchy_child_hydration")) |value| {
        const is_unit_group = req.hierarchy_group_level == .unit and
            (req.return_mode == .unit or req.return_mode == .unit_with_chunks);
        if (value != .bool or !value.bool or (req.hierarchy_children == null and !is_unit_group)) {
            return error.InvalidQueryRequest;
        }
        req.defer_hierarchy_child_hydration = true;
    }
}

fn parseInternalDocIdArrayAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![][]const u8 {
    if (value != .array) return error.InvalidQueryRequest;
    if (value.array.items.len == 0) return &.{};

    const out = try alloc.alloc([]const u8, value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |item| alloc.free(@constCast(item));
        alloc.free(out);
    }

    for (value.array.items, 0..) |item, i| {
        if (item != .string) return error.InvalidQueryRequest;
        out[i] = try alloc.dupe(u8, item.string);
        initialized += 1;
    }
    return try sortAndDedupeOwnedStringArrayAlloc(alloc, out);
}

fn parseDistributedTextStatsAlloc(
    alloc: std.mem.Allocator,
    body: []const u8,
) ![]const @import("../search/distributed_stats.zig").TextFieldStats {
    const distributed_stats_mod = @import("../search/distributed_stats.zig");

    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{}) catch return &.{};
    defer parsed.deinit();
    if (parsed.value != .object) return &.{};
    const encoded = parsed.value.object.get("_distributed_text_stats") orelse return &.{};
    if (encoded != .array) return error.InvalidQueryRequest;

    const stats = try alloc.alloc(distributed_stats_mod.TextFieldStats, encoded.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (stats[0..initialized]) |*item| item.deinit(alloc);
        if (stats.len > 0) alloc.free(stats);
    }

    for (encoded.array.items, 0..) |entry, i| {
        if (entry != .object) return error.InvalidQueryRequest;
        const field_value = entry.object.get("field") orelse return error.InvalidQueryRequest;
        const doc_count_value = entry.object.get("global_doc_count") orelse return error.InvalidQueryRequest;
        const total_field_len_value = entry.object.get("global_total_field_len") orelse return error.InvalidQueryRequest;
        const term_doc_freqs_value = entry.object.get("term_doc_freqs") orelse return error.InvalidQueryRequest;
        if (field_value != .string or term_doc_freqs_value != .array) return error.InvalidQueryRequest;

        const term_doc_freqs = try alloc.alloc(distributed_stats_mod.TermDocFreq, term_doc_freqs_value.array.items.len);
        var initialized_terms: usize = 0;
        errdefer {
            for (term_doc_freqs[0..initialized_terms]) |*item| item.deinit(alloc);
            if (term_doc_freqs.len > 0) alloc.free(term_doc_freqs);
        }
        for (term_doc_freqs_value.array.items, 0..) |term_entry, term_idx| {
            if (term_entry != .object) return error.InvalidQueryRequest;
            const term_value = term_entry.object.get("term") orelse return error.InvalidQueryRequest;
            const freq_value = term_entry.object.get("doc_freq") orelse return error.InvalidQueryRequest;
            if (term_value != .string) return error.InvalidQueryRequest;
            term_doc_freqs[term_idx] = .{
                .term = try alloc.dupe(u8, term_value.string),
                .doc_freq = try jsonValueToU32(freq_value),
            };
            initialized_terms += 1;
        }

        stats[i] = .{
            .field = try alloc.dupe(u8, field_value.string),
            .global_doc_count = try jsonValueToU32(doc_count_value),
            .global_total_field_len = try jsonValueToU64(total_field_len_value),
            .term_doc_freqs = term_doc_freqs,
        };
        initialized += 1;
    }

    return stats;
}

fn jsonValueToU32(value: std.json.Value) !u32 {
    return switch (value) {
        .integer => |v| std.math.cast(u32, v) orelse return error.InvalidQueryRequest,
        else => error.InvalidQueryRequest,
    };
}

fn jsonValueToU64(value: std.json.Value) !u64 {
    return switch (value) {
        .integer => |v| std.math.cast(u64, v) orelse return error.InvalidQueryRequest,
        else => error.InvalidQueryRequest,
    };
}

fn freeNamedDenseQueries(alloc: std.mem.Allocator, items: []const db_mod.types.NamedDenseQuery) void {
    for (items) |item| {
        alloc.free(item.name);
        alloc.free(item.index_name);
        alloc.free(item.query.vector);
    }
    if (items.len > 0) alloc.free(items);
}

fn freeNamedSparseQueries(alloc: std.mem.Allocator, items: []const db_mod.types.NamedSparseQuery) void {
    for (items) |item| {
        alloc.free(item.name);
        alloc.free(item.index_name);
        alloc.free(item.query.indices);
        alloc.free(item.query.values);
    }
    if (items.len > 0) alloc.free(items);
}

fn freeNamedGraphQueries(alloc: std.mem.Allocator, items: []const db_mod.types.NamedGraphQuery) void {
    for (items) |item| {
        alloc.free(item.name);
        freeGraphQuery(alloc, item.query);
    }
    if (items.len > 0) alloc.free(items);
}

pub fn freeGraphQuery(alloc: std.mem.Allocator, query: graph_query_mod.GraphQuery) void {
    alloc.free(query.index_name);
    freeGraphNodeSelector(alloc, query.start_nodes);
    if (query.target_nodes) |target_nodes| freeGraphNodeSelector(alloc, target_nodes);
    freeGraphQueryParams(alloc, query.params);
    freePatternSteps(alloc, query.pattern);
    if (query.match_pattern) |pattern| {
        freeGraphMatchNodes(alloc, pattern.nodes);
        freeGraphMatchEdges(alloc, pattern.edges);
        freeGraphMatchPredicates(alloc, pattern.predicates);
        freeGraphOptionalPatterns(alloc, pattern.optional);
    }
    for (query.return_aliases) |alias| alloc.free(alias);
    if (query.return_aliases.len > 0) alloc.free(query.return_aliases);
    freeGraphCountAggregates(alloc, query.aggregates);
    for (query.fields) |field| alloc.free(field);
    if (query.fields.len > 0) alloc.free(query.fields);
}

fn freeGraphMatchNodes(alloc: std.mem.Allocator, nodes: []const graph_pattern_mod.MatchNode) void {
    for (nodes) |node| {
        alloc.free(node.alias);
        if (node.table) |table| alloc.free(table);
        freePatternNodeFilter(alloc, node.filter);
    }
    if (nodes.len > 0) alloc.free(nodes);
}

fn freeGraphMatchEdges(alloc: std.mem.Allocator, edges: []const graph_pattern_mod.MatchEdge) void {
    for (edges) |edge| {
        alloc.free(edge.from);
        alloc.free(edge.to);
        freeOwnedStringSlice(alloc, edge.step.types);
    }
    if (edges.len > 0) alloc.free(edges);
}

fn freeGraphMatchPredicates(alloc: std.mem.Allocator, predicates: []const graph_pattern_mod.MatchPredicate) void {
    for (predicates) |predicate| switch (predicate) {
        .not_equal => |value| {
            alloc.free(value.left);
            alloc.free(value.right);
        },
        .not_exists => |edges| freeGraphMatchEdges(alloc, edges),
    };
    if (predicates.len > 0) alloc.free(predicates);
}

fn freeGraphOptionalPatterns(alloc: std.mem.Allocator, groups: []const graph_pattern_mod.OptionalPattern) void {
    for (groups) |group| {
        freeGraphMatchNodes(alloc, group.nodes);
        freeGraphMatchEdges(alloc, group.edges);
        freeGraphMatchPredicates(alloc, group.predicates);
    }
    if (groups.len > 0) alloc.free(groups);
}

fn freeGraphCountAggregates(alloc: std.mem.Allocator, aggregates: []const graph_query_mod.NamedCountAggregate) void {
    for (aggregates) |aggregate| {
        alloc.free(aggregate.name);
        alloc.free(aggregate.of);
    }
    if (aggregates.len > 0) alloc.free(aggregates);
}

fn freeGraphNodeSelector(alloc: std.mem.Allocator, selector: graph_query_mod.NodeSelector) void {
    switch (selector) {
        .keys => |keys| {
            for (keys) |key| alloc.free(key);
            if (keys.len > 0) alloc.free(keys);
        },
        .identities => |identities| {
            for (identities) |identity| {
                alloc.free(identity.key);
                if (identity.table) |table| alloc.free(table);
            }
            if (identities.len > 0) alloc.free(identities);
        },
        .result_ref => |result_ref| {
            alloc.free(result_ref.ref);
            if (result_ref.binding) |binding| alloc.free(binding);
        },
    }
}

fn freeGraphQueryParams(alloc: std.mem.Allocator, params: graph_query_mod.QueryParams) void {
    for (params.edge_types) |edge_type| alloc.free(edge_type);
    if (params.edge_types.len > 0) alloc.free(params.edge_types);
    freePatternNodeFilter(alloc, params.node_filter);
}

fn freeTextQueryList(alloc: std.mem.Allocator, items: []const db_mod.types.TextQuery) void {
    for (items) |item| freeTextQuery(alloc, item);
    if (items.len > 0) alloc.free(items);
}

fn deinitTextQueryArrayList(alloc: std.mem.Allocator, list: *std.ArrayListUnmanaged(db_mod.types.TextQuery)) void {
    for (list.items) |item| freeTextQuery(alloc, item);
    list.deinit(alloc);
    list.* = .empty;
}

fn freeTextQuery(alloc: std.mem.Allocator, query: db_mod.types.TextQuery) void {
    var owned = query;
    owned.deinit(alloc);
}

fn jsonStringifyAlloc(alloc: std.mem.Allocator, value: anytype) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(alloc);
    defer out.deinit();
    try std.json.Stringify.value(value, .{}, &out.writer);
    return try alloc.dupe(u8, out.written());
}

test "api query contract parses direct structured boolean filters" {
    const alloc = std.testing.allocator;

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"conjuncts":[{"term":{"status":"active"}},{"term":{"tenant":"tenant-a"}}]}
    , .{});
    defer parsed.deinit();

    const encoded = try encodeSupportedPatternFilterQueryAlloc(alloc, parsed.value);
    defer alloc.free(encoded);

    try std.testing.expectEqualStrings(
        "{\"bool\":{\"must\":[{\"term\":{\"path\":\"status\",\"term\":\"active\"}},{\"term\":{\"path\":\"tenant\",\"term\":\"tenant-a\"}}]}}",
        encoded,
    );
}

test "api query contract parses direct JSON-pointer path aliases" {
    const alloc = std.testing.allocator;

    var direct_term_json = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"term":"gold","path":"/tier"}
    , .{});
    defer direct_term_json.deinit();
    const direct_term = try parseSupportedFullTextQuery(alloc, direct_term_json.value, 10);
    defer freeTextQuery(alloc, direct_term);
    try std.testing.expect(direct_term == .term);
    try std.testing.expectEqualStrings("/tier", direct_term.term.field);
    try std.testing.expectEqualStrings("gold", direct_term.term.term);

    var wrapped_term_json = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"term":{"path":"/tier","value":"silver"}}
    , .{});
    defer wrapped_term_json.deinit();
    const wrapped_term = try parseSupportedFullTextQuery(alloc, wrapped_term_json.value, 10);
    defer freeTextQuery(alloc, wrapped_term);
    try std.testing.expect(wrapped_term == .term);
    try std.testing.expectEqualStrings("/tier", wrapped_term.term.field);
    try std.testing.expectEqualStrings("silver", wrapped_term.term.term);

    var match_json = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"match":{"path":"/tier","text":"gold"}}
    , .{});
    defer match_json.deinit();
    const match_query = try parseSupportedFullTextQuery(alloc, match_json.value, 10);
    defer freeTextQuery(alloc, match_query);
    try std.testing.expect(match_query == .match);
    try std.testing.expectEqualStrings("/tier", match_query.match.field);
    try std.testing.expectEqualStrings("gold", match_query.match.text);

    var prefix_json = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"prefix":"go","path":"/tier"}
    , .{});
    defer prefix_json.deinit();
    const prefix_query = try parseSupportedFullTextQuery(alloc, prefix_json.value, 10);
    defer freeTextQuery(alloc, prefix_query);
    try std.testing.expect(prefix_query == .prefix);
    try std.testing.expectEqualStrings("/tier", prefix_query.prefix.field);
    try std.testing.expectEqualStrings("go", prefix_query.prefix.prefix);

    var wrapped_prefix_json = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"prefix":{"path":"/tier","value":"si"}}
    , .{});
    defer wrapped_prefix_json.deinit();
    const wrapped_prefix_query = try parseSupportedFullTextQuery(alloc, wrapped_prefix_json.value, 10);
    defer freeTextQuery(alloc, wrapped_prefix_query);
    try std.testing.expect(wrapped_prefix_query == .prefix);
    try std.testing.expectEqualStrings("/tier", wrapped_prefix_query.prefix.field);
    try std.testing.expectEqualStrings("si", wrapped_prefix_query.prefix.prefix);

    var wildcard_json = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"wildcard":{"path":"/tier","pattern":"go*"}}
    , .{});
    defer wildcard_json.deinit();
    const wildcard_query = try parseSupportedFullTextQuery(alloc, wildcard_json.value, 10);
    defer freeTextQuery(alloc, wildcard_query);
    try std.testing.expect(wildcard_query == .wildcard);
    try std.testing.expectEqualStrings("/tier", wildcard_query.wildcard.field);
    try std.testing.expectEqualStrings("go*", wildcard_query.wildcard.pattern);

    var regexp_json = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"regexp":{"path":"/tier","value":"go.*"}}
    , .{});
    defer regexp_json.deinit();
    const regexp_query = try parseSupportedFullTextQuery(alloc, regexp_json.value, 10);
    defer freeTextQuery(alloc, regexp_query);
    try std.testing.expect(regexp_query == .regexp);
    try std.testing.expectEqualStrings("/tier", regexp_query.regexp.field);
    try std.testing.expectEqualStrings("go.*", regexp_query.regexp.pattern);

    var fuzzy_json = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"fuzzy":{"path":"/tier","query":"gild","prefix_length":1,"max_edits":1}}
    , .{});
    defer fuzzy_json.deinit();
    const fuzzy_query = try parseSupportedFullTextQuery(alloc, fuzzy_json.value, 10);
    defer freeTextQuery(alloc, fuzzy_query);
    try std.testing.expect(fuzzy_query == .fuzzy);
    try std.testing.expectEqualStrings("/tier", fuzzy_query.fuzzy.field);
    try std.testing.expectEqualStrings("gild", fuzzy_query.fuzzy.term);
    try std.testing.expectEqual(@as(u8, 1), fuzzy_query.fuzzy.prefix_len);
    try std.testing.expectEqual(@as(u8, 1), fuzzy_query.fuzzy.max_edits);

    var generated_fuzzy_json = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"term":"gild","field":"/tier","prefix_length":1,"fuzziness":1,"boost":2}
    , .{});
    defer generated_fuzzy_json.deinit();
    const generated_fuzzy_query = try parseSupportedFullTextQuery(alloc, generated_fuzzy_json.value, 10);
    defer freeTextQuery(alloc, generated_fuzzy_query);
    try std.testing.expect(generated_fuzzy_query == .fuzzy);
    try std.testing.expectEqualStrings("/tier", generated_fuzzy_query.fuzzy.field);
    try std.testing.expectEqualStrings("gild", generated_fuzzy_query.fuzzy.term);
    try std.testing.expectEqual(@as(u8, 1), generated_fuzzy_query.fuzzy.prefix_len);
    try std.testing.expectEqual(@as(u8, 1), generated_fuzzy_query.fuzzy.max_edits);
    try std.testing.expectEqual(@as(f32, 2), generated_fuzzy_query.fuzzy.boost);

    var range_json = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"path":"/amount","min":10,"max":20}
    , .{});
    defer range_json.deinit();
    const range_query = try parseSupportedFullTextQuery(alloc, range_json.value, 10);
    defer freeTextQuery(alloc, range_query);
    try std.testing.expect(range_query == .numeric_range);
    try std.testing.expectEqualStrings("/amount", range_query.numeric_range.field);
    try std.testing.expectEqual(@as(?f64, 10), range_query.numeric_range.min);
    try std.testing.expectEqual(@as(?f64, 20), range_query.numeric_range.max);

    var mixed_range_json = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"path":"/created_at","start":"2026-01-01T00:00:00Z","min":10}
    , .{});
    defer mixed_range_json.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, parseSupportedFullTextQuery(alloc, mixed_range_json.value, 10));

    var malformed_operator_with_range_json = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"term":42,"path":"/amount","min":10}
    , .{});
    defer malformed_operator_with_range_json.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, parseSupportedFullTextQuery(alloc, malformed_operator_with_range_json.value, 10));

    var malformed_operator_with_date_range_json = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"term":42,"path":"/created_at","start":"2026-01-01T00:00:00Z"}
    , .{});
    defer malformed_operator_with_date_range_json.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, parseSupportedFullTextQuery(alloc, malformed_operator_with_date_range_json.value, 10));

    var disjuncts_json = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"disjuncts":[{"term":"gold","path":"/tier"},{"term":{"path":"/tier","term":"bronze"}}]}
    , .{});
    defer disjuncts_json.deinit();
    const disjuncts_query = try parseSupportedFullTextQuery(alloc, disjuncts_json.value, 10);
    defer freeTextQuery(alloc, disjuncts_query);
    try std.testing.expect(disjuncts_query == .bool_query);
    try std.testing.expectEqual(@as(usize, 2), disjuncts_query.bool_query.should.len);
    try std.testing.expect(disjuncts_query.bool_query.should[0] == .term);
    try std.testing.expectEqualStrings("/tier", disjuncts_query.bool_query.should[0].term.field);
    try std.testing.expectEqualStrings("gold", disjuncts_query.bool_query.should[0].term.term);
    try std.testing.expect(disjuncts_query.bool_query.should[1] == .term);
    try std.testing.expectEqualStrings("/tier", disjuncts_query.bool_query.should[1].term.field);
    try std.testing.expectEqualStrings("bronze", disjuncts_query.bool_query.should[1].term.term);
}

test "api query contract normalizes canonical query with legacy shorthands" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "query": {
        \\    "bool": {
        \\      "must": [{"match":{"field":"body","text":"raft"}}],
        \\      "filter": [
        \\        {"term":{"path":"/tenant","value":"acme"}},
        \\        {"terms":{"path":"/tier","values":["gold",2,true]}}
        \\      ],
        \\      "must_not": [
        \\        {"exists":{"path":"/deleted_at"}},
        \\        {"term":{"path":"/archived","value":true}}
        \\      ]
        \\    }
        \\  },
        \\  "full_text_search": {"term":{"body":"legacy"}},
        \\  "filter_query": {"term":{"status":"published"}},
        \\  "exclusion_query": {"term":{"status":"deleted"}}
        \\}
    ;

    var parsed = try parseQueryRequest(alloc, null, "docs", body);
    defer parsed.deinit(alloc);

    try std.testing.expect(parsed.req.full_text.? == .bool_query);
    try std.testing.expectEqual(@as(usize, 2), parsed.req.full_text.?.bool_query.must.len);
    try std.testing.expect(parsed.req.full_text.?.bool_query.must[0] == .match);
    try std.testing.expect(parsed.req.full_text.?.bool_query.must[1] == .term);
    try std.testing.expectEqualStrings("raft", parsed.req.full_text.?.bool_query.must[0].match.text);
    try std.testing.expectEqualStrings("legacy", parsed.req.full_text.?.bool_query.must[1].term.term);

    try std.testing.expect(std.mem.indexOf(u8, parsed.req.filter_query_json, "\"must\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, parsed.req.filter_query_json, "\"path\":\"/tenant\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, parsed.req.filter_query_json, "\"values\":[\"gold\",2,true]") != null);
    try std.testing.expect(std.mem.indexOf(u8, parsed.req.filter_query_json, "\"path\":\"status\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, parsed.req.filter_query_json, "\"term\":\"published\"") != null);

    try std.testing.expect(std.mem.indexOf(u8, parsed.req.exclusion_query_json, "\"should\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, parsed.req.exclusion_query_json, "\"minimum_should_match\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, parsed.req.exclusion_query_json, "\"exists\":{\"path\":\"/deleted_at\"}") != null);
    try std.testing.expect(std.mem.indexOf(u8, parsed.req.exclusion_query_json, "\"path\":\"/archived\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, parsed.req.exclusion_query_json, "\"path\":\"status\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, parsed.req.exclusion_query_json, "\"term\":\"deleted\"") != null);
}

test "api query contract parses public with document filter bindings" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "with": {
        \\    "visible": {"term":{"path":"/tenant","value":"acme"}},
        \\    "published": {"bool_field":{"field":"published","value":true}}
        \\  },
        \\  "query": {
        \\    "bool": {
        \\      "must": [
        \\        {"match":{"field":"body","text":"raft"}},
        \\        {"ref":"visible"}
        \\      ],
        \\      "filter": [{"ref":"published"}]
        \\    }
        \\  }
        \\}
    ;

    var parsed = try parseQueryRequest(alloc, null, "docs", body);
    defer parsed.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), parsed.req.doc_filter_bindings.len);
    try std.testing.expectEqualStrings("visible", parsed.req.doc_filter_bindings[0].name);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/tenant\",\"value\":\"acme\"}}", parsed.req.doc_filter_bindings[0].filter_query_json);
    try std.testing.expectEqualStrings("published", parsed.req.doc_filter_bindings[1].name);
    try std.testing.expectEqualStrings("{\"bool_field\":{\"field\":\"published\",\"value\":true}}", parsed.req.doc_filter_bindings[1].filter_query_json);

    try std.testing.expect(parsed.req.full_text.? == .match);
    try std.testing.expectEqualStrings("raft", parsed.req.full_text.?.match.text);
    try std.testing.expect(std.mem.indexOf(u8, parsed.req.filter_query_json, "\"ref\":\"visible\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, parsed.req.filter_query_json, "\"ref\":\"published\"") != null);
}

test "api query contract expands text-index document filter bindings" {
    const alloc = std.testing.allocator;
    var parsed = try parseQueryRequest(
        alloc,
        null,
        "docs",
        \\{
        \\  "with": {
        \\    "receipt": {"match_phrase":"paid receipt","field":"body"}
        \\  },
        \\  "filter_query": {"ref":"receipt"}
        \\}
        ,
    );
    defer parsed.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 0), parsed.req.doc_filter_bindings.len);
    const filter_text = parsed.req.filter_text orelse return error.TestExpectedEqual;
    try std.testing.expect(filter_text == .match_phrase);
    try std.testing.expectEqualStrings("body", filter_text.match_phrase.field);
    try std.testing.expectEqualStrings("paid receipt", filter_text.match_phrase.text);
}

test "api query contract keeps text-index bindings in bool must non-scoring" {
    const alloc = std.testing.allocator;
    var parsed = try parseQueryRequest(
        alloc,
        null,
        "docs",
        \\{
        \\  "with": {
        \\    "receipt": {"match_phrase":"paid receipt","field":"body"}
        \\  },
        \\  "query": {
        \\    "bool": {
        \\      "must": [
        \\        {"match":{"field":"body","text":"raft"}},
        \\        {"ref":"receipt"}
        \\      ]
        \\    }
        \\  }
        \\}
        ,
    );
    defer parsed.deinit(alloc);

    const scoring = parsed.req.full_text orelse return error.TestExpectedEqual;
    try std.testing.expect(scoring == .match);
    try std.testing.expectEqualStrings("raft", scoring.match.text);
    const filter_text = parsed.req.filter_text orelse return error.TestExpectedEqual;
    try std.testing.expect(filter_text == .match_phrase);
    try std.testing.expectEqualStrings("paid receipt", filter_text.match_phrase.text);
}

test "api query contract keeps full text binding references non-scoring" {
    const alloc = std.testing.allocator;
    var parsed = try parseQueryRequest(
        alloc,
        null,
        "docs",
        \\{
        \\  "with": {
        \\    "receipt": {"match_phrase":"paid receipt","field":"body"}
        \\  },
        \\  "full_text_search": {"ref":"receipt"}
        \\}
        ,
    );
    defer parsed.deinit(alloc);

    try std.testing.expect(parsed.req.full_text.? == .match_all);
    const filter_text = parsed.req.filter_text orelse return error.TestExpectedEqual;
    try std.testing.expect(filter_text == .match_phrase);
    try std.testing.expectEqualStrings("paid receipt", filter_text.match_phrase.text);
}

test "api query contract retains structured bindings beside text bindings" {
    const alloc = std.testing.allocator;
    var parsed = try parseQueryRequest(
        alloc,
        null,
        "docs",
        \\{
        \\  "with": {
        \\    "tenant": {"term":{"path":"/tenant","value":"acme"}},
        \\    "receipt": {"match_phrase":"paid receipt","field":"body"}
        \\  },
        \\  "filter_query": [
        \\    {"ref":"tenant"},
        \\    {"ref":"receipt"}
        \\  ]
        \\}
        ,
    );
    defer parsed.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), parsed.req.doc_filter_bindings.len);
    try std.testing.expectEqualStrings("tenant", parsed.req.doc_filter_bindings[0].name);
    try std.testing.expectEqualStrings(
        "{\"term\":{\"path\":\"/tenant\",\"value\":\"acme\"}}",
        parsed.req.doc_filter_bindings[0].filter_query_json,
    );
    try std.testing.expect(std.mem.indexOf(u8, parsed.req.filter_query_json, "\"ref\":\"tenant\"") != null);
    const filter_text = parsed.req.filter_text orelse return error.TestExpectedEqual;
    try std.testing.expect(filter_text == .match_phrase);
    try std.testing.expectEqualStrings("paid receipt", filter_text.match_phrase.text);
}

test "api query contract retains structured dependencies of text bindings" {
    const alloc = std.testing.allocator;
    var parsed = try parseQueryRequest(
        alloc,
        null,
        "docs",
        \\{
        \\  "with": {
        \\    "tenant": {"term":{"path":"/tenant","value":"acme"}},
        \\    "receipt": {
        \\      "bool": {
        \\        "must": [
        \\          {"ref":"tenant"},
        \\          {"match_phrase":"paid receipt","field":"body"}
        \\        ]
        \\      }
        \\    }
        \\  },
        \\  "filter_query": {"ref":"receipt"}
        \\}
        ,
    );
    defer parsed.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), parsed.req.doc_filter_bindings.len);
    try std.testing.expectEqualStrings("tenant", parsed.req.doc_filter_bindings[0].name);
    try std.testing.expect(std.mem.indexOf(u8, parsed.req.filter_query_json, "\"ref\":\"tenant\"") != null);
    const filter_text = parsed.req.filter_text orelse return error.TestExpectedEqual;
    try std.testing.expect(filter_text == .match_phrase);
    try std.testing.expectEqualStrings("paid receipt", filter_text.match_phrase.text);
}

test "api query contract classifies transitive text binding dependencies" {
    const alloc = std.testing.allocator;
    var parsed = try parseQueryRequest(
        alloc,
        null,
        "docs",
        \\{
        \\  "with": {
        \\    "receipt": {"match_phrase":"paid receipt","field":"body"},
        \\    "paid": {"ref":"receipt"}
        \\  },
        \\  "filter_query": {"ref":"paid"}
        \\}
        ,
    );
    defer parsed.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 0), parsed.req.doc_filter_bindings.len);
    const filter_text = parsed.req.filter_text orelse return error.TestExpectedEqual;
    try std.testing.expect(filter_text == .match_phrase);
    try std.testing.expectEqualStrings("paid receipt", filter_text.match_phrase.text);
}

test "api query contract rejects unused bindings with unknown syntax" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(
        error.UnsupportedQueryRequest,
        parseQueryRequest(
            alloc,
            null,
            "docs",
            \\{
            \\  "with": {
            \\    "invalid": {"unknown_operator":{"value":"silently discarded before validation"}}
            \\  },
            \\  "query": {"match_all":{}}
            \\}
            ,
        ),
    );
}

test "api query contract splits mixed structured and text binding conjunctions" {
    const alloc = std.testing.allocator;
    var parsed = try parseQueryRequest(
        alloc,
        null,
        "docs",
        \\{
        \\  "with": {
        \\    "visible_receipt": {
        \\      "bool": {
        \\        "must": [
        \\          {"term":{"path":"/tenant","value":"acme"}},
        \\          {"match_phrase":"paid receipt","field":"body"}
        \\        ]
        \\      }
        \\    }
        \\  },
        \\  "filter_query": {"ref":"visible_receipt"}
        \\}
        ,
    );
    defer parsed.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 0), parsed.req.doc_filter_bindings.len);
    try std.testing.expect(std.mem.indexOf(u8, parsed.req.filter_query_json, "\"path\":\"/tenant\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, parsed.req.filter_query_json, "\"value\":\"acme\"") != null);
    const filter_text = parsed.req.filter_text orelse return error.TestExpectedEqual;
    try std.testing.expect(filter_text == .match_phrase);
    try std.testing.expectEqualStrings("paid receipt", filter_text.match_phrase.text);
}

test "api query contract binding expansion observes zero timeout" {
    try std.testing.expectError(
        error.Timeout,
        parseQueryRequest(
            std.testing.allocator,
            null,
            "docs",
            \\{
            \\  "timeout_ms": 0,
            \\  "with": {
            \\    "receipt": {"match_phrase":"paid receipt","field":"body"}
            \\  },
            \\  "filter_query": {"ref":"receipt"}
            \\}
            ,
        ),
    );
}

test "api query contract honors caller absolute deadline during normalization" {
    try std.testing.expectError(
        error.Timeout,
        parsePublicQueryRequestWithDeadline(
            std.testing.allocator,
            null,
            "docs",
            \\{
            \\  "timeout_ms": 60000,
            \\  "with": {
            \\    "receipt": {"match_phrase":"paid receipt","field":"body"}
            \\  },
            \\  "filter_query": {"ref":"receipt"}
            \\}
        ,
            0,
        ),
    );
}

test "api query contract expansion budget checks its absolute deadline" {
    var budget = PublicBindingExpansionBudget{
        .remaining_bytes = 128,
        .deadline_ns = 0,
    };
    try std.testing.expectError(error.Timeout, budget.consumeNode(0));
}

test "api query contract final binding validation observes caller deadline" {
    try std.testing.expectError(
        error.Timeout,
        parsePublicDocFilterBindingsAlloc(
            std.testing.allocator,
            \\{
            \\  "with": {
            \\    "visible": {"term":{"path":"/tenant","value":"acme"}}
            \\  },
            \\  "filter_query": {"ref":"visible"}
            \\}
        ,
            10,
            0,
        ),
    );
}

test "api query contract limits binding expansion growth not input size" {
    const input_bytes = public_limits.max_request_body_bytes;
    try std.testing.expectEqual(
        input_bytes + public_limits.max_query_binding_expansion_growth_bytes,
        try publicBindingExpansionOutputLimit(input_bytes),
    );
    try std.testing.expectError(
        error.InvalidQueryRequest,
        publicBindingExpansionOutputLimit(std.math.maxInt(usize)),
    );
}

test "api query contract bounds expanded binding output bytes" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(
        error.InvalidQueryRequest,
        expandPublicDocFilterBindingsWithLimitAlloc(
            alloc,
            \\{
            \\  "with": {
            \\    "receipt": {"match_phrase":"paid receipt paid receipt paid receipt","field":"body"}
            \\  },
            \\  "filter_query": [
            \\    {"ref":"receipt"},
            \\    {"ref":"receipt"},
            \\    {"ref":"receipt"},
            \\    {"ref":"receipt"}
            \\  ]
            \\}
        ,
            256,
        ),
    );
}

test "api query contract orders forward document filter dependencies" {
    const alloc = std.testing.allocator;
    var parsed = try parseQueryRequest(
        alloc,
        null,
        "docs",
        \\{
        \\  "with": {
        \\    "visible": {"bool":{"must":[{"ref":"tenant"},{"ref":"published"}]}},
        \\    "published": {"bool_field":{"field":"published","value":true}},
        \\    "tenant": {"term":{"path":"/tenant","value":"acme"}}
        \\  },
        \\  "filter_query": {"ref":"visible"}
        \\}
        ,
    );
    defer parsed.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 3), parsed.req.doc_filter_bindings.len);
    try std.testing.expectEqualStrings("published", parsed.req.doc_filter_bindings[0].name);
    try std.testing.expectEqualStrings("tenant", parsed.req.doc_filter_bindings[1].name);
    try std.testing.expectEqualStrings("visible", parsed.req.doc_filter_bindings[2].name);
}

test "api query contract rejects invalid document filter dependency graphs" {
    const alloc = std.testing.allocator;
    inline for ([_][]const u8{
        \\{"with":{"visible":{"ref":"missing"}},"filter_query":{"ref":"visible"}}
        ,
        \\{"with":{"first":{"ref":"second"},"second":{"ref":"first"}},"filter_query":{"ref":"first"}}
        ,
        \\{"with":{"visible":{"match_all":{}}},"filter_query":{"ref":"missing"}}
        ,
        \\{"filter_query":{"ref":"missing"}}
        ,
    }) |body| {
        try std.testing.expectError(
            error.InvalidQueryRequest,
            parseQueryRequest(alloc, null, "docs", body),
        );
    }
}

test "api query contract applies one inclusive depth limit to filter dependencies" {
    const alloc = std.testing.allocator;
    var by_name = std.StringHashMapUnmanaged(usize).empty;
    defer by_name.deinit(alloc);
    try by_name.put(alloc, "base", 0);

    var body = std.ArrayListUnmanaged(u8).empty;
    defer body.deinit(alloc);
    for (0..public_query_max_tree_depth) |_| {
        try body.appendSlice(alloc, "{\"conjuncts\":[");
    }
    try body.appendSlice(alloc, "{\"ref\":\"base\"}");
    for (0..public_query_max_tree_depth) |_| {
        try body.appendSlice(alloc, "]}");
    }

    var at_limit = try std.json.parseFromSlice(std.json.Value, alloc, body.items, .{});
    defer at_limit.deinit();
    try collectPublicDocFilterRefs(alloc, at_limit.value, &by_name, null);

    try body.insertSlice(alloc, 0, "{\"conjuncts\":[");
    try body.appendSlice(alloc, "]}");
    var beyond_limit = try std.json.parseFromSlice(std.json.Value, alloc, body.items, .{});
    defer beyond_limit.deinit();
    try std.testing.expectError(
        error.InvalidQueryRequest,
        collectPublicDocFilterRefs(alloc, beyond_limit.value, &by_name, null),
    );
}

test "api query contract keeps compact ref fields distinct from binding references" {
    const alloc = std.testing.allocator;
    var parsed = try parseQueryRequest(
        alloc,
        null,
        "docs",
        \\{
        \\  "with": {
        \\    "literal_ref": {"term":{"ref":"published"}}
        \\  },
        \\  "filter_query": {"ref":"literal_ref"}
        \\}
        ,
    );
    defer parsed.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), parsed.req.doc_filter_bindings.len);
    try std.testing.expectEqualStrings("literal_ref", parsed.req.doc_filter_bindings[0].name);
    try std.testing.expectEqualStrings(
        "{\"term\":{\"path\":\"ref\",\"term\":\"published\"}}",
        parsed.req.doc_filter_bindings[0].filter_query_json,
    );
}

test "api query contract keeps ambiguous direct text operators score-bearing" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "query": {
        \\    "bool": {
        \\      "must": [
        \\        {"match":{"field":"body","value":"raft"}}
        \\      ]
        \\    }
        \\  }
        \\}
    ;

    var parsed = try parseQueryRequest(alloc, null, "docs", body);
    defer parsed.deinit(alloc);

    try std.testing.expect(parsed.req.full_text.? == .match);
    try std.testing.expectEqualStrings("body", parsed.req.full_text.?.match.field);
    try std.testing.expectEqualStrings("raft", parsed.req.full_text.?.match.text);
    try std.testing.expectEqualStrings("", parsed.req.filter_query_json);
}

test "api query contract targets named full text retrieval without changing primary filters" {
    const alloc = std.testing.allocator;
    var parsed = try parsePublicQueryRequest(
        alloc,
        null,
        "docs",
        \\{
        \\  "full_text_index": "document_text",
        \\  "full_text_search": {"match":"needle","field":"text"},
        \\  "filter_query": {"term":{"path":"/tenant","value":"acme"}}
        \\}
        ,
    );
    defer parsed.deinit(alloc);

    try std.testing.expect(parsed.req.full_text == null);
    try std.testing.expectEqual(@as(usize, 1), parsed.req.full_text_queries.len);
    try std.testing.expectEqualStrings("$full_text_results", parsed.req.full_text_queries[0].name);
    try std.testing.expectEqualStrings("document_text", parsed.req.full_text_queries[0].index_name);
    try std.testing.expect(parsed.req.full_text_queries[0].query == .match);
    try std.testing.expectEqualStrings("needle", parsed.req.full_text_queries[0].query.match.text);
    try std.testing.expectEqualStrings(
        "{\"term\":{\"path\":\"/tenant\",\"value\":\"acme\"}}",
        parsed.req.filter_query_json,
    );

    try std.testing.expectError(
        error.InvalidQueryRequest,
        parsePublicQueryRequest(alloc, null, "docs", "{\"full_text_index\":\"document_text\",\"limit\":10}"),
    );
}

test "api query contract rejects malformed scoring clauses before filter fallback" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "query": {
        \\    "bool": {
        \\      "must": [
        \\        {"match":{"field":"body"}}
        \\      ]
        \\    }
        \\  }
        \\}
    ;

    try std.testing.expectError(error.UnsupportedQueryRequest, parseQueryRequest(alloc, null, "docs", body));
}

test "api query contract parses public hierarchy controls" {
    const alloc = std.testing.allocator;
    const chunk_body =
        \\{
        \\  "full_text_search": {"match":"needle","field":"content"},
        \\  "hierarchy": {"return_level":"chunk"}
        \\}
    ;
    var chunk = try parseQueryRequest(alloc, null, "docs", chunk_body);
    defer chunk.deinit(alloc);
    try std.testing.expectEqual(db_mod.types.ReturnMode.chunk, chunk.req.return_mode);
    try std.testing.expectEqual(@as(u32, 0), chunk.req.max_chunks_per_parent);

    const unit_body =
        \\{
        \\  "full_text_search": {"match":"needle","field":"content"},
        \\  "hierarchy": {"return_level":"unit"}
        \\}
    ;
    var unit = try parseQueryRequest(alloc, null, "docs", unit_body);
    defer unit.deinit(alloc);
    try std.testing.expectEqual(db_mod.types.ReturnMode.chunk, unit.req.return_mode);
    try std.testing.expectEqual(@as(u32, 0), unit.req.max_chunks_per_parent);

    const grouped_body =
        \\{
        \\  "full_text_search": {"match":"needle","field":"content"},
        \\  "hierarchy": {
        \\    "return_level": "source",
        \\    "rollup": "source",
        \\    "include": ["unit", "chunk"],
        \\    "max_children_per_parent": 2
        \\  }
        \\}
    ;
    var grouped = try parseQueryRequest(alloc, null, "docs", grouped_body);
    defer grouped.deinit(alloc);
    try std.testing.expectEqual(db_mod.types.ReturnMode.parent_with_chunks, grouped.req.return_mode);
    try std.testing.expectEqual(@as(u32, 2), grouped.req.max_chunks_per_parent);
    try std.testing.expect(!grouped.req.hierarchy_include_source);
    try std.testing.expect(grouped.req.hierarchy_include_unit);

    const hydrate_body =
        \\{
        \\  "full_text_search": {"match":"needle","field":"content"},
        \\  "hierarchy": {
        \\    "return_level": "chunk",
        \\    "include": ["source", "unit"]
        \\  }
        \\}
    ;
    var hydrate = try parseQueryRequest(alloc, null, "docs", hydrate_body);
    defer hydrate.deinit(alloc);
    try std.testing.expectEqual(db_mod.types.ReturnMode.chunk, hydrate.req.return_mode);
    try std.testing.expect(hydrate.req.hierarchy_include_source);
    try std.testing.expect(hydrate.req.hierarchy_include_unit);

    const mention_body =
        \\{
        \\  "full_text_search": {"match":"needle","field":"content"},
        \\  "hierarchy": {
        \\    "return_level": "mention",
        \\    "include": ["source", "mention"]
        \\  }
        \\}
    ;
    var mention = try parseQueryRequest(alloc, null, "docs", mention_body);
    defer mention.deinit(alloc);
    try std.testing.expectEqual(db_mod.types.ReturnMode.chunk, mention.req.return_mode);
    try std.testing.expect(mention.req.hierarchy_include_source);

    const projected_matches_body =
        \\{
        \\  "full_text_search": {"match":"needle","field":"content"},
        \\  "fields": ["text"],
        \\  "hierarchy": {
        \\    "ancestors": {
        \\      "source": {"fields":["title","url"]},
        \\      "unit": {"fields":["page"]}
        \\    }
        \\  }
        \\}
    ;
    var projected_matches = try parseQueryRequest(alloc, null, "docs", projected_matches_body);
    defer projected_matches.deinit(alloc);
    try std.testing.expectEqual(db_mod.types.ReturnMode.chunk, projected_matches.req.return_mode);
    try std.testing.expect(projected_matches.req.hierarchy_include_source);
    try std.testing.expect(projected_matches.req.hierarchy_include_unit);
    try std.testing.expect(!projected_matches.req.hierarchy_source_include_all_fields);
    try std.testing.expectEqualStrings("url", projected_matches.req.hierarchy_source_fields[1]);
    try std.testing.expect(!projected_matches.req.hierarchy_unit_include_all_fields);
    try std.testing.expectEqualStrings("page", projected_matches.req.hierarchy_unit_fields[0]);
    try std.testing.expect(projected_matches.req.defer_stored_projection);

    const grouped_matches_body =
        \\{
        \\  "full_text_search": {"match":"needle","field":"content"},
        \\  "fields": ["title"],
        \\  "hierarchy": {
        \\    "group_by": {
        \\      "level": "source",
        \\      "matches": {"fields":["text"]}
        \\    }
        \\  }
        \\}
    ;
    var grouped_matches = try parseQueryRequest(alloc, null, "docs", grouped_matches_body);
    defer grouped_matches.deinit(alloc);
    try std.testing.expectEqual(db_mod.types.ReturnMode.parent_with_chunks, grouped_matches.req.return_mode);
    try std.testing.expectEqual(@as(u32, 3), grouped_matches.req.max_chunks_per_parent);
    try std.testing.expect(grouped_matches.req.hierarchy_omit_implicit_source_ancestor_document);
    try std.testing.expect(grouped_matches.req.hierarchy_grouped_matches);
    try std.testing.expect(!grouped_matches.req.hierarchy_include_source);
    try std.testing.expect(!grouped_matches.req.hierarchy_include_unit);
    try std.testing.expect(!grouped_matches.req.hierarchy_match_include_all_fields);
    try std.testing.expectEqualStrings("text", grouped_matches.req.hierarchy_match_fields[0]);
    try std.testing.expect(grouped_matches.req.defer_stored_projection);

    const grouped_only_body =
        \\{
        \\  "full_text_search": {"match":"needle","field":"content"},
        \\  "fields": [],
        \\  "hierarchy": {"group_by":{"level":"source"}}
        \\}
    ;
    var grouped_only = try parseQueryRequest(alloc, null, "docs", grouped_only_body);
    defer grouped_only.deinit(alloc);
    try std.testing.expectEqual(db_mod.types.ReturnMode.parent, grouped_only.req.return_mode);
    try std.testing.expect(!grouped_only.req.hierarchy_grouped_matches);
}

test "api query contract validates canonical hierarchy controls" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "full_text_search": {"match":"needle","field":"content"},
        \\  "hierarchy": {"rollup":"mention"}
        \\}
    ;
    try std.testing.expectError(error.UnsupportedQueryRequest, parseQueryRequest(alloc, null, "docs", body));

    const unit_group_level =
        \\{
        \\  "full_text_search": {"match":"needle","field":"content"},
        \\  "fields": [],
        \\  "hierarchy": {"group_by":{"level":"unit","matches":{"fields":[]}}}
        \\}
    ;
    var unit_group = try parseQueryRequest(alloc, null, "docs", unit_group_level);
    defer unit_group.deinit(alloc);
    try std.testing.expectEqual(db_mod.types.HierarchyGroupLevel.unit, unit_group.req.hierarchy_group_level);
    try std.testing.expectEqual(db_mod.types.ReturnMode.unit_with_chunks, unit_group.req.return_mode);
    try std.testing.expect(unit_group.req.hierarchy_grouped_matches);

    const sorted_unit_group =
        \\{
        \\  "full_text_search": {"match":"needle","field":"content"},
        \\  "fields": [],
        \\  "order_by": [{"field":"_score","desc":true}],
        \\  "hierarchy": {"group_by":{"level":"unit"}}
        \\}
    ;
    db_mod.resetLastSortRejectionDiagnostic();
    try std.testing.expectError(error.UnsupportedQueryRequest, parseQueryRequest(alloc, null, "docs", sorted_unit_group));
    const unit_sort_diagnostic = db_mod.takeLastSortRejectionDiagnostic() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("_score", unit_sort_diagnostic.field);
    try std.testing.expectEqualStrings("unsupported_exact_sort", unit_sort_diagnostic.reason);
    try std.testing.expectEqualStrings("unsupported_exact_sort", unit_sort_diagnostic.detail);

    const missing_group_level =
        \\{
        \\  "full_text_search": {"match":"needle","field":"content"},
        \\  "fields": [],
        \\  "hierarchy": {"group_by":{"matches":{"fields":["text"]}}}
        \\}
    ;
    try std.testing.expectError(error.InvalidQueryRequest, parseQueryRequest(alloc, null, "docs", missing_group_level));

    const unknown_group_field =
        \\{
        \\  "full_text_search": {"match":"needle","field":"content"},
        \\  "fields": [],
        \\  "hierarchy": {"group_by":{"level":"source","sort":[]}}
        \\}
    ;
    try std.testing.expectError(error.InvalidQueryRequest, parseQueryRequest(alloc, null, "docs", unknown_group_field));

    const composable_group_and_unit_ancestor =
        \\{
        \\  "full_text_search": {"match":"needle","field":"content"},
        \\  "fields": [],
        \\  "hierarchy": {"group_by":{"level":"source","matches":{"fields":["text"]}},"ancestors":{"unit":{"fields":["page"]}}}
        \\}
    ;
    var composable = try parseQueryRequest(alloc, null, "docs", composable_group_and_unit_ancestor);
    defer composable.deinit(alloc);
    try std.testing.expectEqual(db_mod.types.ReturnMode.parent_with_chunks, composable.req.return_mode);
    try std.testing.expect(composable.req.hierarchy_grouped_matches);
    try std.testing.expect(!composable.req.hierarchy_include_source);
    try std.testing.expect(composable.req.hierarchy_include_unit);

    const redundant_group_source_ancestor =
        \\{
        \\  "full_text_search": {"match":"needle","field":"content"},
        \\  "fields": [],
        \\  "hierarchy": {"group_by":{"level":"source","matches":{"fields":["text"]}},"ancestors":{"source":{"fields":["title"]}}}
        \\}
    ;
    try std.testing.expectError(error.InvalidQueryRequest, parseQueryRequest(alloc, null, "docs", redundant_group_source_ancestor));

    const mixed_contract_generations =
        \\{
        \\  "full_text_search": {"match":"needle","field":"content"},
        \\  "fields": [],
        \\  "hierarchy": {"group_by":{"level":"source"},"include":["chunk"]}
        \\}
    ;
    try std.testing.expectError(error.InvalidQueryRequest, parseQueryRequest(alloc, null, "docs", mixed_contract_generations));

    const missing_match_fields =
        \\{
        \\  "full_text_search": {"match":"needle","field":"content"},
        \\  "fields": [],
        \\  "hierarchy": {"group_by":{"level":"source","matches":{"limit":2}}}
        \\}
    ;
    try std.testing.expectError(error.InvalidQueryRequest, parseQueryRequest(alloc, null, "docs", missing_match_fields));

    const missing_group_projection =
        \\{
        \\  "full_text_search": {"match":"needle","field":"content"},
        \\  "hierarchy": {"group_by":{"level":"source","matches":{"fields":["text"]}}}
        \\}
    ;
    try std.testing.expectError(error.InvalidQueryRequest, parseQueryRequest(alloc, null, "docs", missing_group_projection));

    const missing_ancestor_fields =
        \\{
        \\  "full_text_search": {"match":"needle","field":"content"},
        \\  "hierarchy": {"ancestors":{"source":{}}}
        \\}
    ;
    try std.testing.expectError(error.InvalidQueryRequest, parseQueryRequest(alloc, null, "docs", missing_ancestor_fields));

    const children_body =
        \\{
        \\  "fields": ["unit_id","unit_type","text","provenance.page_number"],
        \\  "hierarchy": {"children":{"parent":{"level":"source","id":"doc:a"},"level":"unit"}},
        \\  "order_by": [{"field":"_hierarchy.position"}],
        \\  "limit": 20
        \\}
    ;
    var children = try parseQueryRequest(alloc, null, "docs", children_body);
    defer children.deinit(alloc);
    try std.testing.expectEqual(db_mod.types.ReturnMode.unit, children.req.return_mode);
    try std.testing.expectEqualStrings("doc:a", children.req.hierarchy_children.?.parent_id);
    try std.testing.expectEqualStrings("_hierarchy.position", children.req.order_by[0].field);

    const internal_children_body =
        \\{
        \\  "fields": [],
        \\  "hierarchy": {"children":{"parent":{"level":"source","id":"doc:a"},"level":"unit"}},
        \\  "order_by": [{"field":"_hierarchy.position"}],
        \\  "_identity_read_generation": 42,
        \\  "_filter_query_json": "{\"term\":{\"path\":\"/tenant\",\"value\":\"acme\"}}"
        \\}
    ;
    var internal_children = try parseQueryRequest(alloc, null, "docs", internal_children_body);
    defer internal_children.deinit(alloc);
    try std.testing.expectEqual(@as(?u64, 42), internal_children.req.identity_read_generation);
    try std.testing.expect(internal_children.req.filter_query_json.len > 0);
    try std.testing.expectError(error.InvalidQueryRequest, parsePublicQueryRequest(alloc, null, "docs", internal_children_body));

    const children_with_query =
        \\{
        \\  "query": {"match_all": {}},
        \\  "fields": [],
        \\  "hierarchy": {"children":{"parent":{"level":"source","id":"doc:a"},"level":"unit"}},
        \\  "order_by": [{"field":"_hierarchy.position"}]
        \\}
    ;
    try std.testing.expectError(error.InvalidQueryRequest, parseQueryRequest(alloc, null, "docs", children_with_query));

    const children_with_ignored_filter =
        \\{
        \\  "filter_query": {"term":{"path":"/tenant","value":"acme"}},
        \\  "fields": [],
        \\  "hierarchy": {"children":{"parent":{"level":"source","id":"doc:a"},"level":"unit"}},
        \\  "order_by": [{"field":"_hierarchy.position"}]
        \\}
    ;
    try std.testing.expectError(error.InvalidQueryRequest, parseQueryRequest(alloc, null, "docs", children_with_ignored_filter));

    const children_without_order =
        \\{
        \\  "fields": [],
        \\  "hierarchy": {"children":{"parent":{"level":"source","id":"doc:a"},"level":"unit"}}
        \\}
    ;
    try std.testing.expectError(error.InvalidQueryRequest, parseQueryRequest(alloc, null, "docs", children_without_order));

    const stale_shaped_children_cursor =
        \\{
        \\  "fields": [],
        \\  "hierarchy": {"children":{"parent":{"level":"source","id":"doc:a"},"level":"unit"}},
        \\  "order_by": [{"field":"_hierarchy.position"}],
        \\  "search_after": ["position-only"]
        \\}
    ;
    try std.testing.expectError(error.UnsupportedQueryRequest, parseQueryRequest(alloc, null, "docs", stale_shaped_children_cursor));

    const bounded_grouped_matches =
        \\{
        \\  "full_text_search": {"match":"needle","field":"content"},
        \\  "fields": [],
        \\  "hierarchy": {"group_by":{"level":"source","matches":{"limit":2,"fields":["text"]}}}
        \\}
    ;
    var bounded_grouped = try parseQueryRequest(alloc, null, "docs", bounded_grouped_matches);
    defer bounded_grouped.deinit(alloc);
    try std.testing.expectEqual(db_mod.types.ReturnMode.parent_with_chunks, bounded_grouped.req.return_mode);
    try std.testing.expectEqual(@as(u32, 2), bounded_grouped.req.max_chunks_per_parent);

    const too_many_groups =
        \\{
        \\  "full_text_search": {"match":"needle","field":"content"},
        \\  "fields": [],
        \\  "limit": 101,
        \\  "hierarchy": {"group_by":{"level":"source","matches":{"limit":1,"fields":["text"]}}}
        \\}
    ;
    try std.testing.expectError(error.InvalidQueryRequest, parseQueryRequest(alloc, null, "docs", too_many_groups));

    const too_much_grouped_work =
        \\{
        \\  "full_text_search": {"match":"needle","field":"content"},
        \\  "fields": [],
        \\  "limit": 100,
        \\  "hierarchy": {"group_by":{"level":"source","matches":{"limit":11,"fields":["text"]}}}
        \\}
    ;
    try std.testing.expectError(error.InvalidQueryRequest, parseQueryRequest(alloc, null, "docs", too_much_grouped_work));

    const unbounded_group_page =
        \\{
        \\  "full_text_search": {"match":"needle","field":"content"},
        \\  "fields": [],
        \\  "limit": 0,
        \\  "hierarchy": {"group_by":{"level":"source","matches":{"limit":1,"fields":["text"]}}}
        \\}
    ;
    try std.testing.expectError(error.InvalidQueryRequest, parseQueryRequest(alloc, null, "docs", unbounded_group_page));

    const oversized_grouped_matches =
        \\{
        \\  "full_text_search": {"match":"needle","field":"content"},
        \\  "fields": [],
        \\  "hierarchy": {"group_by":{"level":"source","matches":{"limit":101,"fields":["text"]}}}
        \\}
    ;
    try std.testing.expectError(error.InvalidQueryRequest, parseQueryRequest(alloc, null, "docs", oversized_grouped_matches));

    const direct_matches_with_ancestors =
        \\{
        \\  "full_text_search": {"match":"needle","field":"content"},
        \\  "hierarchy": {"ancestors":{"source":{"fields":[]}}}
        \\}
    ;
    var direct_matches = try parseQueryRequest(alloc, null, "docs", direct_matches_with_ancestors);
    defer direct_matches.deinit(alloc);
    try std.testing.expectEqual(db_mod.types.ReturnMode.member, direct_matches.req.return_mode);
    try std.testing.expect(direct_matches.req.hierarchy_include_source);
    try std.testing.expect(!direct_matches.req.hierarchy_source_include_all_fields);

    const empty_hierarchy =
        \\{
        \\  "full_text_search": {"match":"needle","field":"content"},
        \\  "hierarchy": {}
        \\}
    ;
    var empty_direct = try parseQueryRequest(alloc, null, "docs", empty_hierarchy);
    defer empty_direct.deinit(alloc);
    try std.testing.expectEqual(db_mod.types.ReturnMode.member, empty_direct.req.return_mode);
    try std.testing.expect(!empty_direct.req.hierarchy_include_source);
    try std.testing.expect(!empty_direct.req.hierarchy_include_unit);

    // Canonical optional fields are non-nullable: omission selects the default
    // result shape, while an explicit null is a malformed request.
    const null_hierarchy =
        \\{
        \\  "full_text_search": {"match":"needle","field":"content"},
        \\  "hierarchy": null
        \\}
    ;
    try std.testing.expectError(error.InvalidQueryRequest, parseQueryRequest(alloc, null, "docs", null_hierarchy));
}

test "api query contract keeps generated schema strict when with is present" {
    const alloc = std.testing.allocator;
    const unknown_body =
        \\{
        \\  "with": {"visible": {"match_all": {}}},
        \\  "not_a_query_field": true,
        \\  "query": {"match_all": {}}
        \\}
    ;
    try std.testing.expectError(error.InvalidQueryRequest, parseQueryRequest(alloc, null, "docs", unknown_body));

    const generation_body =
        \\{
        \\  "with": {"visible": {"match_all": {}}},
        \\  "identity_read_generation": 7,
        \\  "query": {"match_all": {}}
        \\}
    ;
    try std.testing.expectError(error.InvalidQueryRequest, parseQueryRequest(alloc, null, "docs", generation_body));

    const reassignment_body =
        \\{
        \\  "with": {"visible": {"match_all": {}}},
        \\  "allow_doc_identity_reassignment": true,
        \\  "query": {"match_all": {}}
        \\}
    ;
    try std.testing.expectError(error.InvalidQueryRequest, parseQueryRequest(alloc, null, "docs", reassignment_body));
}

test "api query contract public parser rejects internal shard doc identity controls" {
    const alloc = std.testing.allocator;
    const internal_body =
        \\{
        \\  "query": {"match_all": {}},
        \\  "native_doc_id_constraints": {
        \\    "positive_filter": true,
        \\    "include_doc_ids": ["doc:a"],
        \\    "exclude_doc_ids": []
        \\  },
        \\  "_identity_read_generation": 7
        \\}
    ;

    try std.testing.expect(try testing.bodyHasForbiddenPublicDocIdentityControls(alloc, internal_body));
    try std.testing.expectError(error.InvalidQueryRequest, parsePublicQueryRequest(alloc, null, "docs", internal_body));

    var internal = try parseQueryRequest(alloc, null, "docs", internal_body);
    defer internal.deinit(alloc);
    try std.testing.expect(internal.req.filter_doc_ids_positive);
    try std.testing.expectEqual(@as(usize, 1), internal.req.filter_doc_ids.len);
    try std.testing.expectEqualStrings("doc:a", internal.req.filter_doc_ids[0]);
    try std.testing.expectEqual(@as(?u64, 7), internal.req.identity_read_generation);

    const internal_unknown_body =
        \\{
        \\  "query": {"match_all": {}},
        \\  "native_doc_id_constraints": {
        \\    "positive_filter": true,
        \\    "include_doc_ids": ["doc:a"],
        \\    "exclude_doc_ids": []
        \\  },
        \\  "not_a_query_field": true
        \\}
    ;
    try std.testing.expectError(error.InvalidQueryRequest, parseQueryRequest(alloc, null, "docs", internal_unknown_body));

    const resolved_filter_body =
        \\{
        \\  "query": {"match_all": {}},
        \\  "_resolved_doc_filter": {
        \\    "namespace": {"table_id": 1, "shard_id": 2, "range_id": 3},
        \\    "identity_read_generation": 9,
        \\    "include": {"kind": "ordinals", "values": [1, 3]},
        \\    "exclude": {"kind": "none"}
        \\  }
        \\}
    ;
    try std.testing.expect(try testing.bodyHasForbiddenPublicDocIdentityControls(alloc, resolved_filter_body));
    try std.testing.expectError(error.InvalidQueryRequest, parsePublicQueryRequest(alloc, null, "docs", resolved_filter_body));
    var resolved_internal = try parseQueryRequest(alloc, null, "docs", resolved_filter_body);
    defer resolved_internal.deinit(alloc);
    try std.testing.expect(resolved_internal.req.resolved_doc_filter != null);
    try std.testing.expectEqual(@as(?u64, 9), resolved_internal.req.identity_read_generation);

    const literal_body =
        \\{"full_text_search":{"query":"mentions native_doc_id_constraints and _identity_read_generation"}}
    ;
    try std.testing.expect(!try testing.bodyHasForbiddenPublicDocIdentityControls(alloc, literal_body));

    const timeout_before_ns = platform_time.monotonicNs();
    var timeout_request = try parsePublicQueryRequest(alloc, null, "docs",
        \\{"query":{"match_all":{}},"timeout_ms":250}
    );
    defer timeout_request.deinit(alloc);
    const timeout_after_ns = platform_time.monotonicNs();
    const deadline_ns = timeout_request.req.execution_deadline_ns orelse return error.TestExpectedDeadline;
    const deadline_origin_ns = deadline_ns - 250 * std.time.ns_per_ms;
    try std.testing.expect(deadline_origin_ns >= timeout_before_ns);
    try std.testing.expect(deadline_origin_ns <= timeout_after_ns);

    try std.testing.expectError(error.InvalidQueryRequest, parsePublicQueryRequest(alloc, null, "docs",
        \\{"query":{"match_all":{}},"timeout_ms":-1}
    ));
    try std.testing.expectError(error.InvalidQueryRequest, parsePublicQueryRequest(alloc, null, "docs",
        \\{"query":{"match_all":{}},"timeout_ms":1.5}
    ));
}

test "api query contract treats canonical typed scalar term as structured filter" {
    const alloc = std.testing.allocator;
    const body =
        \\{"query":{"term":{"path":"/published","value":true}}}
    ;

    var parsed = try parseQueryRequest(alloc, null, "docs", body);
    defer parsed.deinit(alloc);

    try std.testing.expect(parsed.req.full_text.? == .match_all);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/published\",\"value\":true}}", parsed.req.filter_query_json);
    try std.testing.expectEqualStrings("", parsed.req.exclusion_query_json);
}

test "api query contract preserves the canonical query object wire kind" {
    const alloc = std.testing.allocator;
    inline for (.{
        \\{"query":[]}
        ,
        \\{"query":"match all"}
        ,
        \\{"query":true}
        ,
        \\{"query":42}
        ,
    }) |body| {
        try std.testing.expectError(
            error.InvalidQueryRequest,
            parsePublicQueryRequest(alloc, null, "docs", body),
        );
    }
}

test "api query contract treats canonical string path term as structured filter" {
    const alloc = std.testing.allocator;
    const body =
        \\{"query":{"term":{"path":"/tier","value":"gold"}}}
    ;

    var parsed = try parseQueryRequest(alloc, null, "docs", body);
    defer parsed.deinit(alloc);

    try std.testing.expect(parsed.req.full_text.? == .match_all);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/tier\",\"value\":\"gold\"}}", parsed.req.filter_query_json);
    try std.testing.expectEqualStrings("", parsed.req.exclusion_query_json);
}

test "api query contract includes stored source when fields are omitted" {
    const alloc = std.testing.allocator;
    const body =
        \\{"full_text_search":{"match":"needle","field":"content"}}
    ;

    var parsed = try parseQueryRequest(alloc, null, "docs", body);
    defer parsed.deinit(alloc);

    try std.testing.expect(parsed.req.include_all_fields);
    try std.testing.expect(parsed.req.include_stored);
    try std.testing.expect(!parsed.req.defer_stored_projection);
    try std.testing.expectEqual(@as(usize, 0), parsed.req.fields.len);
}

test "api query contract accepts multi_match bool_prefix full text" {
    const alloc = std.testing.allocator;
    const body =
        \\{"full_text_search":{"multi_match":{"query":"quick brown f","type":"bool_prefix","fields":["title"],"boost":2}}}
    ;

    var parsed = try parseQueryRequest(alloc, null, "docs", body);
    defer parsed.deinit(alloc);

    try std.testing.expect(parsed.req.full_text.? == .multi_match_bool_prefix);
    try std.testing.expectEqualStrings("quick brown f", parsed.req.full_text.?.multi_match_bool_prefix.query);
    try std.testing.expectEqual(@as(usize, 1), parsed.req.full_text.?.multi_match_bool_prefix.fields.len);
    try std.testing.expectEqualStrings("title", parsed.req.full_text.?.multi_match_bool_prefix.fields[0].field);
    try std.testing.expectEqual(@as(f32, 2.0), parsed.req.full_text.?.multi_match_bool_prefix.boost);

    try std.testing.expectError(
        error.InvalidQueryRequest,
        parseQueryRequest(
            alloc,
            null,
            "docs",
            \\{"full_text_search":{"multi_match":{"query":"quick brown f","type":"bool_prefix","fields":["title"],"boost":1e100}}}
            ,
        ),
    );
}

test "api query contract projects stored source when explicit fields are supplied" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "full_text_search": {"match": "needle", "field": "content"},
        \\  "fields": ["path", "filename"]
        \\}
    ;

    var parsed = try parseQueryRequest(alloc, null, "docs", body);
    defer parsed.deinit(alloc);

    try std.testing.expect(!parsed.req.include_all_fields);
    try std.testing.expect(parsed.req.include_stored);
    try std.testing.expect(parsed.req.defer_stored_projection);
    try std.testing.expectEqual(@as(usize, 2), parsed.req.fields.len);
    try std.testing.expectEqualStrings("path", parsed.req.fields[0]);
    try std.testing.expectEqualStrings("filename", parsed.req.fields[1]);
}

test "api query contract accepts internal normalized filter json on internal query route" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "full_text_search": {"match": "hello", "field": "body"},
        \\  "_filter_query_json": "{\"term\":{\"path\":\"/status\",\"value\":\"published\"}}",
        \\  "_exclusion_query_json": "{\"term\":{\"path\":\"/deleted\",\"value\":true}}"
        \\}
    ;

    var parsed = try parseQueryRequest(alloc, null, "docs", body);
    defer parsed.deinit(alloc);

    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/status\",\"value\":\"published\"}}", parsed.req.filter_query_json);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/deleted\",\"value\":true}}", parsed.req.exclusion_query_json);
}

test "api query contract combines public and internal filter representations losslessly" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "filter_query": {"term": "published", "field": "status"},
        \\  "exclusion_query": {"term": "draft", "field": "status"},
        \\  "_filter_query_json": "{\"term\":{\"path\":\"/tenant\",\"value\":\"acme\"}}",
        \\  "_exclusion_query_json": "{\"term\":{\"path\":\"/deleted\",\"value\":true}}"
        \\}
    ;

    var parsed = try parseQueryRequest(alloc, null, "docs", body);
    defer parsed.deinit(alloc);

    try std.testing.expect(std.mem.indexOf(u8, parsed.req.filter_query_json, "\"must\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, parsed.req.filter_query_json, "\"path\":\"status\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, parsed.req.filter_query_json, "\"path\":\"/tenant\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, parsed.req.exclusion_query_json, "\"should\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, parsed.req.exclusion_query_json, "\"path\":\"status\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, parsed.req.exclusion_query_json, "\"path\":\"/deleted\"") != null);
}

test "api query contract normalizes public scalar filters before forwarding" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "full_text_search": {"match": "hello", "field": "body"},
        \\  "filter_query": {"term": "published", "field": "status"},
        \\  "exclusion_query": {"term": "gamma", "field": "title"}
        \\}
    ;

    var parsed = try parseQueryRequest(alloc, null, "docs", body);
    defer parsed.deinit(alloc);

    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"status\",\"term\":\"published\"}}", parsed.req.filter_query_json);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"title\",\"term\":\"gamma\"}}", parsed.req.exclusion_query_json);
}

test "api query contract canonicalizes public Query filter roots and compositions" {
    const alloc = std.testing.allocator;
    const cases = [_]struct {
        body: []const u8,
        required_fragments: []const []const u8,
        forbidden_fragments: []const []const u8 = &.{},
    }{
        .{
            .body =
            \\{"filter_query":{"prefix":"tenant/","field":"path"}}
            ,
            .required_fragments = &.{
                "\"prefix\"",
                "\"path\":\"path\"",
                "\"prefix\":\"tenant/\"",
            },
            .forbidden_fragments = &.{"\"field\""},
        },
        .{
            .body =
            \\{"filter_query":{"term":"active","field":"status","fuzziness":1,"prefix_length":2}}
            ,
            .required_fragments = &.{
                "\"fuzzy\"",
                "\"path\":\"status\"",
                "\"query\":\"active\"",
                "\"max_edits\":1",
                "\"prefix_length\":2",
            },
            .forbidden_fragments = &.{"\"field\""},
        },
        .{
            .body =
            \\{"filter_query":{"min":"a","max":"z","field":"name","inclusive_max":true}}
            ,
            .required_fragments = &.{
                "\"term_range\"",
                "\"path\":\"name\"",
                "\"min\":\"a\"",
                "\"max\":\"z\"",
                "\"inclusive_max\":true",
            },
            .forbidden_fragments = &.{"\"field\""},
        },
        .{
            .body =
            \\{"filter_query":{"start":"2026-01-01T00:00:00Z","field":"created_at"}}
            ,
            .required_fragments = &.{
                "\"date_range\"",
                "\"path\":\"created_at\"",
                "\"start_ns\"",
            },
            .forbidden_fragments = &.{"\"field\""},
        },
        .{
            .body =
            \\{"filter_query":{"bool":true,"field":"published"}}
            ,
            .required_fragments = &.{
                "\"bool_field\"",
                "\"path\":\"published\"",
                "\"value\":true",
            },
            .forbidden_fragments = &.{"\"field\""},
        },
        .{
            .body =
            \\{"filter_query":{"disjuncts":[{"term":"active","field":"status"}],"min":1}}
            ,
            .required_fragments = &.{
                "\"bool\"",
                "\"should\"",
                "\"path\":\"status\"",
                "\"term\":\"active\"",
            },
            .forbidden_fragments = &.{"\"field\""},
        },
        .{
            .body =
            \\{"filter_query":{"disjuncts":[{"term":"active","field":"status"},{"term":"gold","field":"tier"}],"min":2}}
            ,
            .required_fragments = &.{
                "\"bool\"",
                "\"should\"",
                "\"minimum_should_match\":2",
            },
            .forbidden_fragments = &.{"\"field\""},
        },
        .{
            .body =
            \\{"filter_query":{"conjuncts":[{"term":"active","field":"status"}]}}
            ,
            .required_fragments = &.{
                "\"bool\"",
                "\"must\"",
                "\"path\":\"status\"",
                "\"term\":\"active\"",
            },
            .forbidden_fragments = &.{"\"field\""},
        },
        .{
            .body =
            \\{"filter_query":{"must_not":{"disjuncts":[{"prefix":"private/","field":"path"}]}}}
            ,
            .required_fragments = &.{
                "\"bool\"",
                "\"must_not\"",
                "\"path\":\"path\"",
                "\"prefix\":\"private/\"",
            },
            .forbidden_fragments = &.{"\"field\""},
        },
        .{
            .body =
            \\{"filter_query":{"must":{"conjuncts":[{"disjuncts":[{"prefix":"tenant/","field":"path"}],"min":1}]}}}
            ,
            .required_fragments = &.{
                "\"bool\"",
                "\"must\"",
                "\"should\"",
                "\"path\":\"path\"",
                "\"prefix\":\"tenant/\"",
            },
            .forbidden_fragments = &.{"\"field\""},
        },
        .{
            .body =
            \\{"filter_query":{"bool":{"should":[{"term":"active","field":"status"},{"term":"gold","field":"tier"}],"minimum_should_match":2}}}
            ,
            .required_fragments = &.{
                "\"bool\"",
                "\"should\"",
                "\"minimum_should_match\":2",
            },
            .forbidden_fragments = &.{"\"field\""},
        },
        .{
            .body =
            \\{"filter_query":{"must":{"conjuncts":[{"term":"active","field":"status"}]},"must_not":{"disjuncts":[{"term":"deleted","field":"status"}],"min":1}}}
            ,
            .required_fragments = &.{
                "\"must\"",
                "\"must_not\"",
                "\"term\":\"active\"",
                "\"term\":\"deleted\"",
            },
            .forbidden_fragments = &.{"\"field\""},
        },
    };

    for (cases) |case| {
        var parsed = try parsePublicQueryRequest(alloc, null, "files", case.body);
        defer parsed.deinit(alloc);
        try std.testing.expect(parsed.req.full_text.? == .match_all);
        for (case.required_fragments) |fragment| {
            try std.testing.expect(std.mem.indexOf(
                u8,
                parsed.req.filter_query_json,
                fragment,
            ) != null);
        }
        for (case.forbidden_fragments) |fragment| {
            try std.testing.expect(std.mem.indexOf(
                u8,
                parsed.req.filter_query_json,
                fragment,
            ) == null);
        }
        var canonical = try std.json.parseFromSlice(
            std.json.Value,
            alloc,
            parsed.req.filter_query_json,
            .{},
        );
        defer canonical.deinit();
        try db_mod.validateStructuredFilterValueAlloc(alloc, canonical.value);
    }

    try std.testing.expectError(
        error.InvalidFilterQueryRequest,
        parsePublicQueryRequest(
            alloc,
            null,
            "files",
            \\{"filter_query":{"disjuncts":[{"term":"active","field":"status"}],"min":1.5}}
            ,
        ),
    );
    try std.testing.expectError(
        error.InvalidFilterQueryRequest,
        parsePublicQueryRequest(
            alloc,
            null,
            "files",
            \\{"filter_query":{"disjuncts":[{"term":"active","field":"status"}],"min":2}}
            ,
        ),
    );
}

test "api query contract bounds public fuzzy integers without narrowing traps" {
    const alloc = std.testing.allocator;
    inline for ([_][]const u8{
        \\{"filter_query":{"term":"active","field":"status","fuzziness":-1}}
        ,
        \\{"filter_query":{"term":"active","field":"status","fuzziness":3}}
        ,
        \\{"filter_query":{"term":"active","field":"status","prefix_length":-1}}
        ,
        \\{"filter_query":{"term":"active","field":"status","prefix_length":256}}
        ,
    }) |body| {
        try std.testing.expectError(
            error.InvalidFilterQueryRequest,
            parsePublicQueryRequest(alloc, null, "files", body),
        );
    }
}

test "api query contract preserves supported match options and rejects semantic loss" {
    const alloc = std.testing.allocator;
    var filtered = try parsePublicQueryRequest(
        alloc,
        null,
        "files",
        \\{"filter_query":{"match":"active user","field":"status","analyzer":"tenant_search"}}
        ,
    );
    defer filtered.deinit(alloc);
    try std.testing.expectEqualStrings(
        "{\"match\":{\"path\":\"status\",\"text\":\"active user\",\"analyzer\":\"tenant_search\"}}",
        filtered.req.filter_query_json,
    );

    var scored = try parsePublicQueryRequest(
        alloc,
        null,
        "files",
        \\{"full_text_search":{"match":"active user","field":"status","analyzer":"tenant_search","boost":2}}
        ,
    );
    defer scored.deinit(alloc);
    try std.testing.expect(scored.req.full_text.? == .match);
    try std.testing.expectEqualStrings("tenant_search", scored.req.full_text.?.match.analyzer.?);
    try std.testing.expectEqual(@as(f32, 2), scored.req.full_text.?.match.boost);

    inline for ([_][]const u8{
        \\{"filter_query":{"match":"active","field":"status","fuzziness":1}}
        ,
        \\{"filter_query":{"match":"active","field":"status","prefix_length":1}}
        ,
        \\{"filter_query":{"match":"active","field":"status","operator":"or"}}
        ,
    }) |body| {
        try std.testing.expectError(
            error.UnsupportedFilterQueryRequest,
            parsePublicQueryRequest(alloc, null, "files", body),
        );
    }
}

test "api query contract preserves nested direct boosts and rejects ambiguous scoring roots" {
    const alloc = std.testing.allocator;
    inline for ([_]struct {
        body: []const u8,
        tag: std.meta.Tag(db_mod.types.TextQuery),
        expected_boost: f32,
    }{
        .{
            .body =
            \\{"full_text_search":{"term":{"field":"body","term":"invoice","boost":2}}}
            ,
            .tag = .term,
            .expected_boost = 2,
        },
        .{
            .body =
            \\{"full_text_search":{"match":{"field":"body","text":"paid invoice","boost":3}}}
            ,
            .tag = .match,
            .expected_boost = 3,
        },
        .{
            .body =
            \\{"full_text_search":{"match_phrase":{"field":"body","text":"paid invoice","boost":4}}}
            ,
            .tag = .match_phrase,
            .expected_boost = 4,
        },
    }) |case| {
        var parsed = try parsePublicQueryRequest(
            alloc,
            null,
            "files",
            case.body,
        );
        defer parsed.deinit(alloc);
        const full_text = parsed.req.full_text orelse
            return error.TestExpectedEqual;
        try std.testing.expectEqual(case.tag, std.meta.activeTag(full_text));
        const actual_boost = switch (full_text) {
            .term => |value| value.boost,
            .match => |value| value.boost,
            .match_phrase => |value| value.boost,
            else => unreachable,
        };
        try std.testing.expectEqual(case.expected_boost, actual_boost);
    }

    inline for ([_][]const u8{
        \\{"full_text_search":{"term":"invoice","match":"paid","field":"body"}}
        ,
        \\{"full_text_search":{"terms":["paid","invoice"],"cidr":"10.0.0.0/8","field":"body"}}
        ,
        \\{"full_text_search":{"location":[-122.4,37.8],"distance":"10km","min_lat":30,"min_lon":-130,"max_lat":40,"max_lon":-120,"field":"location"}}
        ,
    }) |body| {
        try std.testing.expectError(
            error.InvalidQueryRequest,
            parsePublicQueryRequest(alloc, null, "files", body),
        );
    }
}

test "api query contract accepts explicit empty public boolean branches" {
    const alloc = std.testing.allocator;
    const cases = [_]struct {
        body: []const u8,
        required: []const u8,
        forbidden: ?[]const u8 = null,
    }{
        .{
            .body =
            \\{"filter_query":{"must":{"conjuncts":[]},"must_not":{"disjuncts":[{"term":"deleted","field":"status"}]}}}
            ,
            .required = "\"must_not\"",
            .forbidden = "\"must\"",
        },
        .{
            .body =
            \\{"filter_query":{"must":{"conjuncts":[{"term":"active","field":"status"}]},"must_not":{"disjuncts":[]}}}
            ,
            .required = "\"must\"",
            .forbidden = "\"must_not\"",
        },
        .{
            .body =
            \\{"filter_query":{"must":{"conjuncts":[]},"should":{"disjuncts":[]},"must_not":{"disjuncts":[]}}}
            ,
            .required = "\"match_all\"",
        },
    };
    for (cases) |case| {
        var parsed = try parsePublicQueryRequest(alloc, null, "files", case.body);
        defer parsed.deinit(alloc);
        try std.testing.expect(std.mem.indexOf(
            u8,
            parsed.req.filter_query_json,
            case.required,
        ) != null);
        if (case.forbidden) |forbidden| {
            try std.testing.expect(std.mem.indexOf(
                u8,
                parsed.req.filter_query_json,
                forbidden,
            ) == null);
        }
    }

    try std.testing.expectError(
        error.InvalidFilterQueryRequest,
        parsePublicQueryRequest(
            alloc,
            null,
            "files",
            \\{"filter_query":{"should":{"disjuncts":[],"min":1}}}
            ,
        ),
    );
}

test "api query contract preserves schema-dependent canonical ranges" {
    const alloc = std.testing.allocator;
    const body =
        \\{"filter_query":{"range":{"price":{"gte":10,"lt":20}}}}
    ;
    var parsed = try parsePublicQueryRequest(alloc, null, "products", body);
    defer parsed.deinit(alloc);
    try std.testing.expectEqualStrings(
        "{\"range\":{\"price\":{\"gte\":10,\"lt\":20}}}",
        parsed.req.filter_query_json,
    );

    inline for ([_][]const u8{
        \\{"filter_query":{"range":{"price":{"gte":null}}}}
        ,
        \\{"filter_query":{"range":{"price":{"gte":10}},"term":{"path":"status","term":"active"}}}
        ,
    }) |invalid_body| {
        try std.testing.expectError(
            error.InvalidFilterQueryRequest,
            parsePublicQueryRequest(alloc, null, "products", invalid_body),
        );
    }
}

test "api query contract rejects ambiguous canonical query roots" {
    const alloc = std.testing.allocator;
    inline for ([_][]const u8{
        \\{"query":{"bool":{"filter":[{"term":{"path":"status","value":"active"}}]},"term":{"path":"tier","value":"gold"}}}
        ,
        \\{"query":{"bool":{"filter":[{"term":{"path":"status","value":"active"}}],"unknown":true}}}
        ,
    }) |body| {
        try std.testing.expectError(
            error.InvalidQueryRequest,
            parsePublicQueryRequest(alloc, null, "files", body),
        );
    }
}

test "api query contract accepts text-index queries in canonical boolean filters" {
    const alloc = std.testing.allocator;
    var parsed = try parsePublicQueryRequest(
        alloc,
        null,
        "files",
        \\{"query":{"bool":{"filter":[{"terms":["quick","fox"],"field":"body"}],"must_not":[{"match_phrase":"bad wolf","field":"body"}]}}}
        ,
    );
    defer parsed.deinit(alloc);

    const filter_text = parsed.req.filter_text orelse return error.TestExpectedEqual;
    try std.testing.expect(filter_text == .phrase);
    try std.testing.expectEqualStrings("quick", filter_text.phrase.terms[0]);
    try std.testing.expectEqualStrings("fox", filter_text.phrase.terms[1]);
    const exclusion_text = parsed.req.exclusion_text orelse return error.TestExpectedEqual;
    try std.testing.expect(exclusion_text == .match_phrase);
    try std.testing.expectEqualStrings("bad wolf", exclusion_text.match_phrase.text);
    try std.testing.expectEqualStrings("", parsed.req.filter_query_json);
    try std.testing.expectEqualStrings("", parsed.req.exclusion_query_json);
}

test "api query contract preserves canonical boolean boost scope" {
    const alloc = std.testing.allocator;
    var parsed = try parsePublicQueryRequest(
        alloc,
        null,
        "files",
        \\{"query":{"bool":{"must":[{"match":{"field":"body","text":"computer"}}],"boost":2}},"full_text_search":{"match":"storage","field":"body"}}
        ,
    );
    defer parsed.deinit(alloc);

    const full_text = parsed.req.full_text orelse return error.TestExpectedEqual;
    try std.testing.expect(full_text == .bool_query);
    try std.testing.expectEqual(@as(usize, 2), full_text.bool_query.must.len);
    try std.testing.expect(full_text.bool_query.must[0] == .bool_query);
    try std.testing.expectEqual(@as(f32, 2), full_text.bool_query.must[0].bool_query.boost);
    try std.testing.expectEqual(@as(usize, 1), full_text.bool_query.must[0].bool_query.must.len);
    try std.testing.expect(full_text.bool_query.must[0].bool_query.must[0] == .match);
    try std.testing.expect(full_text.bool_query.must[1] == .match);
}

test "api query contract preserves schema-valid canonical boolean boosts" {
    const alloc = std.testing.allocator;
    var null_boost = try parsePublicQueryRequest(
        alloc,
        null,
        "files",
        \\{"query":{"bool":{"must":[{"match":{"field":"body","text":"computer"}}],"boost":null}}}
        ,
    );
    defer null_boost.deinit(alloc);
    try std.testing.expect(null_boost.req.full_text.? == .match);

    inline for ([_]struct {
        body: []const u8,
        expected: f32,
    }{
        .{
            .body =
            \\{"query":{"bool":{"must":[{"match":{"field":"body","text":"computer"}}],"boost":0}}}
            ,
            .expected = 0,
        },
        .{
            .body =
            \\{"query":{"bool":{"must":[{"match":{"field":"body","text":"computer"}}],"boost":-1}}}
            ,
            .expected = -1,
        },
    }) |case| {
        var parsed = try parsePublicQueryRequest(alloc, null, "files", case.body);
        defer parsed.deinit(alloc);
        const full_text = parsed.req.full_text orelse return error.TestExpectedEqual;
        try std.testing.expect(full_text == .bool_query);
        try std.testing.expectEqual(case.expected, full_text.bool_query.boost);
    }
}

test "api query contract rejects unrepresentable canonical boolean boosts" {
    const alloc = std.testing.allocator;
    inline for ([_][]const u8{
        \\{"query":{"bool":{"must":[{"match":{"field":"body","text":"computer"}}],"boost":"bad"}}}
        ,
        \\{"query":{"bool":{"must":[{"match":{"field":"body","text":"computer"}}],"boost":1e100}}}
        ,
    }) |body| {
        try std.testing.expectError(
            error.InvalidQueryRequest,
            parsePublicQueryRequest(alloc, null, "files", body),
        );
    }
}

test "api query contract keeps should optional beside required filters" {
    const alloc = std.testing.allocator;
    var parsed = try parsePublicQueryRequest(
        alloc,
        null,
        "files",
        \\{"query":{"bool":{"filter":[{"term":{"path":"status","value":"active"}}],"should":[{"match":{"field":"body","text":"computer"}}]}}}
        ,
    );
    defer parsed.deinit(alloc);

    const full_text = parsed.req.full_text orelse return error.TestExpectedEqual;
    try std.testing.expect(full_text == .bool_query);
    try std.testing.expectEqual(@as(u32, 0), full_text.bool_query.min_should);
    try std.testing.expect(full_text.bool_query.pure_should_optional);
    try std.testing.expectEqual(@as(usize, 0), full_text.bool_query.must.len);
    try std.testing.expectEqual(@as(usize, 1), full_text.bool_query.should.len);
    try std.testing.expect(full_text.bool_query.should[0] == .match);
    try std.testing.expect(std.mem.indexOf(
        u8,
        parsed.req.filter_query_json,
        "\"status\"",
    ) != null);
}

test "api query contract distinguishes explicit zero from implicit pure should minimum" {
    const alloc = std.testing.allocator;
    inline for ([_]struct {
        body: []const u8,
        optional: bool,
        boost: f32 = 1.0,
    }{
        .{
            .body =
            \\{"full_text_search":{"should":{"disjuncts":[{"match":"computer","field":"body"}],"min":0}}}
            ,
            .optional = true,
        },
        .{
            .body =
            \\{"full_text_search":{"should":{"disjuncts":[{"match":"computer","field":"body"}]}}}
            ,
            .optional = false,
        },
        .{
            .body =
            \\{"full_text_search":{"disjuncts":[{"match":"computer","field":"body"}],"min":0,"boost":2}}
            ,
            .optional = true,
            .boost = 2.0,
        },
    }) |case| {
        var parsed = try parsePublicQueryRequest(alloc, null, "files", case.body);
        defer parsed.deinit(alloc);

        const full_text = parsed.req.full_text orelse return error.TestExpectedEqual;
        try std.testing.expect(full_text == .bool_query);
        try std.testing.expectEqual(@as(u32, 0), full_text.bool_query.min_should);
        try std.testing.expectEqual(case.optional, full_text.bool_query.pure_should_optional);
        try std.testing.expectEqual(case.boost, full_text.bool_query.boost);
        try std.testing.expectEqual(@as(usize, 0), full_text.bool_query.must.len);
        try std.testing.expectEqual(@as(usize, 1), full_text.bool_query.should.len);
    }

    var conjunction = try parsePublicQueryRequest(
        alloc,
        null,
        "files",
        \\{"full_text_search":{"conjuncts":[{"match":"computer","field":"body"}],"boost":3}}
        ,
    );
    defer conjunction.deinit(alloc);
    try std.testing.expectEqual(
        @as(f32, 3.0),
        conjunction.req.full_text.?.bool_query.boost,
    );

    var match_all = try parsePublicQueryRequest(
        alloc,
        null,
        "files",
        \\{"full_text_search":{"match_all":{},"boost":4}}
        ,
    );
    defer match_all.deinit(alloc);
    try std.testing.expect(match_all.req.full_text.? == .bool_query);
    try std.testing.expectEqual(
        @as(f32, 4.0),
        match_all.req.full_text.?.bool_query.boost,
    );
    try std.testing.expectEqual(
        @as(usize, 1),
        match_all.req.full_text.?.bool_query.must.len,
    );
    try std.testing.expect(match_all.req.full_text.?.bool_query.must[0] == .match_all);

    var optional_filter = try std.json.parseFromSlice(
        std.json.Value,
        alloc,
        \\{"disjuncts":[{"term":"active","field":"status"}],"min":0}
    ,
        .{},
    );
    defer optional_filter.deinit();
    const encoded_filter = try encodeSupportedPatternFilterQueryAlloc(
        alloc,
        optional_filter.value,
    );
    defer alloc.free(encoded_filter);
    try std.testing.expect(std.mem.indexOf(
        u8,
        encoded_filter,
        "\"minimum_should_match\":0",
    ) != null);
}

test "api query contract preserves text native public filter variants" {
    const alloc = std.testing.allocator;
    inline for ([_]struct {
        body: []const u8,
        expected: std.meta.Tag(db_mod.types.TextQuery),
        exclusion: bool = false,
    }{
        .{
            .body =
            \\{"filter_query":{"terms":["quick","fox"],"field":"body"}}
            ,
            .expected = .phrase,
        },
        .{
            .body =
            \\{"filter_query":{"terms":[["quick","fast"],["fox"]],"field":"body"}}
            ,
            .expected = .multi_phrase,
        },
        .{
            .body =
            \\{"filter_query":{"match_phrase":"quick fox","field":"body"}}
            ,
            .expected = .match_phrase,
        },
        .{
            .body =
            \\{"filter_query":{"multi_match":{"query":"quick fox","type":"bool_prefix","fields":["title","body^2"]}}}
            ,
            .expected = .multi_match_bool_prefix,
        },
        .{
            .body =
            \\{"filter_query":{"cidr":"10.0.0.0/8","field":"client_ip"}}
            ,
            .expected = .ip_range,
        },
        .{
            .body =
            \\{"filter_query":{"location":[-122.4,37.8],"distance":"10km","field":"location"}}
            ,
            .expected = .geo_distance,
        },
        .{
            .body =
            \\{"filter_query":{"field":"location","min_lat":37,"min_lon":-123,"max_lat":38,"max_lon":-122}}
            ,
            .expected = .geo_bbox,
        },
        .{
            .body =
            \\{"exclusion_query":{"geometry":{"shape":{"type":"Polygon","coordinates":[[[-123,37],[-122,37],[-122,38],[-123,37]]]},"relation":"within"},"field":"location"}}
            ,
            .expected = .geo_shape,
            .exclusion = true,
        },
    }) |case| {
        var parsed = try parsePublicQueryRequest(alloc, null, "files", case.body);
        defer parsed.deinit(alloc);
        const query = if (case.exclusion)
            parsed.req.exclusion_text orelse return error.TestExpectedEqual
        else
            parsed.req.filter_text orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(case.expected, std.meta.activeTag(query));
    }
}

test "api query contract rejects public filters beyond the traversal depth budget" {
    const alloc = std.testing.allocator;
    var body = std.ArrayListUnmanaged(u8).empty;
    defer body.deinit(alloc);
    try body.appendSlice(alloc, "{\"filter_query\":");
    for (0..public_query_max_tree_depth + 1) |_| try body.append(alloc, '[');
    try body.appendSlice(alloc, "{\"match_all\":{}}");
    for (0..public_query_max_tree_depth + 1) |_| try body.append(alloc, ']');
    try body.append(alloc, '}');

    try std.testing.expectError(
        error.InvalidFilterQueryRequest,
        parsePublicQueryRequest(alloc, null, "files", body.items),
    );
}

test "api query contract preserves canonical structured compounds without speculative parsing" {
    const alloc = std.testing.allocator;
    const body =
        \\{"filter_query":{"conjuncts":[{"term":{"status":"active"}},{"bool_field":{"path":"/published","value":true}}]}}
    ;

    var parsed = try parsePublicQueryRequest(alloc, null, "files", body);
    defer parsed.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(
        u8,
        parsed.req.filter_query_json,
        "\"term\":{\"status\":\"active\"}",
    ) != null);
    try std.testing.expect(std.mem.indexOf(
        u8,
        parsed.req.filter_query_json,
        "\"bool_field\":{\"path\":\"/published\",\"value\":true}",
    ) != null);
}

test "api query contract cleans up partially parsed direct query arrays" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc,
        \\[{"term":"active","field":"status"},{"query_string":"status:active"}]
    , .{});
    defer parsed.deinit();

    try std.testing.expectError(
        error.UnsupportedQueryRequest,
        parseDirectDslTextQueryArrayAlloc(alloc, parsed.value),
    );
}

test "api query contract reports the failing nested filter node" {
    const alloc = std.testing.allocator;
    const encoded = try encodePublicFilterQueryErrorBodyAlloc(
        alloc,
        \\{"filter_query":{"disjuncts":[{"term":"active","field":"status"},{"query_string":"status:active"}],"min":1}}
    ,
        "filter_query",
        .unsupported,
    );
    defer alloc.free(encoded);

    const Parsed = struct {
        status: u16,
        field: []const u8,
        offending_node: []const u8,
        retryable: bool,
    };
    var parsed = try std.json.parseFromSlice(Parsed, alloc, encoded, .{
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();
    try std.testing.expectEqual(@as(u16, 422), parsed.value.status);
    try std.testing.expectEqualStrings("filter_query", parsed.value.field);
    try std.testing.expectEqualStrings("query_string", parsed.value.offending_node);
    try std.testing.expect(!parsed.value.retryable);
}

test "api query contract classifies typed filter errors as validation errors" {
    inline for ([_]anyerror{
        error.InvalidQueryRequest,
        error.UnsupportedQueryRequest,
        error.InvalidFilterQueryRequest,
        error.InvalidExclusionQueryRequest,
        error.UnsupportedFilterQueryRequest,
        error.UnsupportedExclusionQueryRequest,
    }) |err| {
        try std.testing.expect(isPublicQueryValidationError(err));
    }
    try std.testing.expect(!isPublicQueryValidationError(error.OutOfMemory));
}

test "api query contract preflight summarizes query lanes and result refs" {
    var parsed = try std.json.parseFromSlice(metadata_openapi.QueryRequest, std.testing.allocator,
        \\{
        \\  "full_text_search": {"match":"raft","field":"body"},
        \\  "embeddings": {"body_embedding": [1.0, 0.0, 0.0]},
        \\  "indexes": ["body_embedding"],
        \\  "limit": 7,
        \\  "count": true,
        \\  "profile": true,
        \\  "fields": ["title"],
        \\  "aggregations": {
        \\    "by_status": {
        \\      "type": "terms",
        \\      "field": "status",
        \\      "sub_aggregations": {
        \\        "doc_count": {
        \\          "type": "count",
        \\          "field": "status"
        \\        }
        \\      }
        \\    }
        \\  },
        \\  "graph_queries": {
        \\    "seeded": {
        \\      "index": "doc_graph",
        \\      "traverse": {
        \\        "start": {"result_ref": "$query_results", "limit": 3},
        \\        "max_depth": 1
        \\      }
        \\    },
        \\    "related": {
        \\      "index": "doc_graph",
        \\      "traverse": {
        \\        "start": {"result_ref": "$graph_results.seeded", "limit": 3},
        \\        "max_depth": 1
        \\      }
        \\    }
        \\  }
        \\}
    , .{});
    defer parsed.deinit();

    var summary = try preflightQueryRequestAlloc(std.testing.allocator, parsed.value);
    defer summary.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), summary.full_text_indexes.len);
    try std.testing.expectEqualStrings("full_text", summary.full_text_indexes[0]);
    try std.testing.expectEqual(@as(usize, 1), summary.embedding_indexes.len);
    try std.testing.expectEqualStrings("body_embedding", summary.embedding_indexes[0]);
    try std.testing.expectEqual(@as(usize, 1), summary.graph_indexes.len);
    try std.testing.expectEqualStrings("doc_graph", summary.graph_indexes[0]);
    try std.testing.expectEqual(@as(usize, 3), summary.result_refs.len);
    var saw_seeded = false;
    var saw_graph = false;
    var saw_query = false;
    for (summary.result_refs) |result_ref| {
        if (std.mem.eql(u8, result_ref, "$graph_results.seeded")) saw_seeded = true;
        if (std.mem.eql(u8, result_ref, "$graph_results.related")) saw_graph = true;
        if (std.mem.eql(u8, result_ref, "$query_results")) saw_query = true;
    }
    try std.testing.expect(saw_seeded);
    try std.testing.expect(saw_graph);
    try std.testing.expect(saw_query);
    try std.testing.expectEqual(@as(usize, 2), summary.graph_query_order.len);
    try std.testing.expectEqualStrings("seeded", summary.graph_query_order[0]);
    try std.testing.expectEqualStrings("related", summary.graph_query_order[1]);
    try std.testing.expectEqual(@as(u32, 7), summary.requested_limit);
    try std.testing.expectEqual(@as(u32, 0), summary.requested_offset);
    try std.testing.expectEqual(@as(u32, 2), summary.base_result_set_count);
    try std.testing.expectEqual(@as(u32, 2), summary.graph_query_count);
    try std.testing.expect(summary.requires_fusion);
    try std.testing.expect(summary.count_only);
    try std.testing.expect(summary.profile_requested);
    try std.testing.expect(summary.include_stored);
    try std.testing.expectEqual(@as(u32, 2), summary.aggregation_count);
}

test "api query contract rejects duplicate graph binding projections" {
    const body =
        \\{
        \\  "graph_queries": {
        \\    "matched": {
        \\      "index": "graph_idx",
        \\      "match": {"anchor":"node","nodes":{"node":{}},"edges":[]},
        \\      "return": {"bindings":["node","node"]}
        \\    }
        \\  }
        \\}
    ;
    try std.testing.expectError(
        error.InvalidQueryRequest,
        parsePublicQueryRequest(std.testing.allocator, null, "docs", body),
    );
}

test "api query contract owns the admitted graph wire for exact proxying" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "graph_queries": {
        \\    "walk": {
        \\      "index": "graph_idx",
        \\      "traverse": {
        \\        "start": {"keys":["doc:a"]},
        \\        "direction": "both",
        \\        "filter": {"term":"active","path":"/status"}
        \\      }
        \\    }
        \\  }
        \\}
    ;
    var owned = try parseQueryRequest(alloc, null, "docs", body);
    defer owned.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), owned.req.graph_queries.len);
    try std.testing.expectEqual(graph_mod.EdgeDirection.both, owned.req.graph_queries[0].query.params.direction);
    const transport = owned.req.graph_query_transport orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(db_mod.types.GraphQueryWireDialect.canonical, transport.dialect);
    var wire = try ant_json.parseFromSlice(std.json.Value, alloc, transport.operations_json, .{});
    defer wire.deinit();
    const walk = wire.value.object.get("walk") orelse return error.TestUnexpectedResult;
    const traverse = walk.object.get("traverse") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("both", traverse.object.get("direction").?.string);
    const filter = traverse.object.get("filter") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("active", filter.object.get("term").?.string);
}

test "api query contract preserves opaque legacy graph operation names" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "graph_searches": {
        \\    "$legacy": {
        \\      "type": "neighbors",
        \\      "index_name": "graph_idx",
        \\      "start_nodes": {"keys":["doc:a"]}
        \\    }
        \\  }
        \\}
    ;
    var owned = try parseQueryRequest(alloc, null, "docs", body);
    defer owned.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), owned.req.graph_queries.len);
    try std.testing.expectEqualStrings("$legacy", owned.req.graph_queries[0].name);
}

test "canonical graph contract rejects modes without exact public execution" {
    const cases = [_][]const u8{
        "{\"graph_queries\":{\"walk\":{\"index\":\"g\",\"traverse\":{\"start\":{\"keys\":[\"a\"]},\"deduplicate_nodes\":false}}}}",
        "{\"graph_queries\":{\"walk\":{\"index\":\"g\",\"traverse\":{\"start\":{\"result_ref\":\"$graph_results.\"}}}}}",
        "{\"graph_queries\":{\"walk\":{\"index\":\"g\",\"traverse\":{\"start\":{\"result_ref\":\"$graph_results.bad\u{200b}name\"}}}}}",
        "{\"graph_queries\":{\"walk\":{\"index\":\"g\",\"traverse\":{\"start\":{\"result_ref\":\"$graph_results.*\"}}}}}",
        "{\"graph_queries\":{\"walk\":{\"index\":\"g\",\"traverse\":{\"start\":{\"result_ref\":\"$graph_results.$reserved\"}}}}}",
    };
    for (cases) |body| {
        try std.testing.expectError(
            error.InvalidQueryRequest,
            parsePublicQueryRequest(std.testing.allocator, null, "docs", body),
        );
    }
}

test "canonical graph traversal and paths preserve requested direction" {
    const cases = [_]struct {
        body: []const u8,
        expected: graph_mod.EdgeDirection,
    }{
        .{
            .body = "{\"graph_queries\":{\"walk\":{\"index\":\"g\",\"traverse\":{\"start\":{\"keys\":[\"a\"]},\"direction\":\"in\"}}}}",
            .expected = .in,
        },
        .{
            .body = "{\"graph_queries\":{\"path\":{\"index\":\"g\",\"shortest_path\":{\"from\":{\"key\":\"a\"},\"to\":{\"key\":\"b\"},\"direction\":\"both\"}}}}",
            .expected = .both,
        },
        .{
            .body = "{\"graph_queries\":{\"paths\":{\"index\":\"g\",\"k_shortest_paths\":{\"from\":{\"key\":\"a\"},\"to\":{\"key\":\"b\"},\"k\":2,\"direction\":\"in\"}}}}",
            .expected = .in,
        },
    };
    for (cases) |case| {
        var owned = try parsePublicQueryRequest(std.testing.allocator, null, "docs", case.body);
        defer owned.deinit(std.testing.allocator);
        try std.testing.expectEqual(case.expected, owned.req.graph_queries[0].query.params.direction);
    }
}

test "api query contract preflight preserves a named full text index" {
    var parsed = try std.json.parseFromSlice(metadata_openapi.QueryRequest, std.testing.allocator,
        \\{
        \\  "full_text_index": "document_text",
        \\  "full_text_search": {"match":"raft","field":"body"}
        \\}
    , .{});
    defer parsed.deinit();

    var summary = try preflightQueryRequestAlloc(std.testing.allocator, parsed.value);
    defer summary.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), summary.full_text_indexes.len);
    try std.testing.expectEqualStrings("document_text", summary.full_text_indexes[0]);
}

test "api query contract preflight rejects count with reranker" {
    var parsed = try std.json.parseFromSlice(metadata_openapi.QueryRequest, std.testing.allocator,
        \\{
        \\  "full_text_search": {"match":"raft","field":"body"},
        \\  "count": true,
        \\  "reranker": {"provider":"cohere","model":"rerank-english-v3.0","field":"body","top_n":5}
        \\}
    , .{});
    defer parsed.deinit();

    try std.testing.expectError(error.UnsupportedQueryRequest, preflightQueryRequestAlloc(std.testing.allocator, parsed.value));
}

test "api query contract rejects count with stored sort" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "full_text_search": {"match":"raft","field":"body"},
        \\  "count": true,
        \\  "order_by": [{"field":"created_at","desc":true}]
        \\}
    ;

    db_mod.resetLastSortRejectionDiagnostic();
    try std.testing.expectError(error.UnsupportedQueryRequest, parseQueryRequest(alloc, null, "docs", body));
    const diagnostic = db_mod.takeLastSortRejectionDiagnostic() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("*", diagnostic.field);
    try std.testing.expectEqualStrings("unsupported_exact_sort", diagnostic.reason);
    try std.testing.expectEqualStrings("count_only_ordered_page", diagnostic.detail);
}

test "api query contract rejects count with search_after cursor" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "full_text_search": {"match":"raft","field":"body"},
        \\  "count": true,
        \\  "order_by": [{"field":"created_at","desc":true}],
        \\  "search_after": ["2026-01-01", "doc-9"]
        \\}
    ;

    db_mod.resetLastSortRejectionDiagnostic();
    try std.testing.expectError(error.UnsupportedQueryRequest, parseQueryRequest(alloc, null, "docs", body));
    const diagnostic = db_mod.takeLastSortRejectionDiagnostic() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("*", diagnostic.field);
    try std.testing.expectEqualStrings("unsupported_exact_sort", diagnostic.reason);
    try std.testing.expectEqualStrings("count_only_ordered_page", diagnostic.detail);
}

test "api query contract rejects count with search_before cursor" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "full_text_search": {"match":"raft","field":"body"},
        \\  "count": true,
        \\  "order_by": [{"field":"created_at","desc":true}],
        \\  "search_before": ["2026-01-01", "doc-9"]
        \\}
    ;

    db_mod.resetLastSortRejectionDiagnostic();
    try std.testing.expectError(error.UnsupportedQueryRequest, parseQueryRequest(alloc, null, "docs", body));
    const diagnostic = db_mod.takeLastSortRejectionDiagnostic() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("*", diagnostic.field);
    try std.testing.expectEqualStrings("unsupported_exact_sort", diagnostic.reason);
    try std.testing.expectEqualStrings("count_only_ordered_page", diagnostic.detail);
}

test "api query contract defaults cursor pagination without sort to id order" {
    const alloc = std.testing.allocator;

    var parsed = try parseQueryRequest(alloc, null, "docs",
        \\{
        \\  "full_text_search": {"match":"raft","field":"body"},
        \\  "search_after": ["doc-9"],
        \\  "limit": 10
        \\}
    );
    defer parsed.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), parsed.req.order_by.len);
    try std.testing.expectEqualStrings("_id", parsed.req.order_by[0].field);
    try std.testing.expect(!parsed.req.order_by[0].desc);
    try std.testing.expectEqual(@as(usize, 1), parsed.req.search_after.len);
    try std.testing.expectEqualStrings("doc-9", parsed.req.search_after[0].string);
}

test "api query contract preflight rejects cursor pagination without sort when cursor is not id arity" {
    var parsed = try std.json.parseFromSlice(metadata_openapi.QueryRequest, std.testing.allocator,
        \\{
        \\  "full_text_search": {"match":"raft","field":"body"},
        \\  "search_after": ["2025-01-01", "doc-9"]
        \\}
    , .{});
    defer parsed.deinit();

    db_mod.resetLastSortRejectionDiagnostic();
    try std.testing.expectError(error.UnsupportedQueryRequest, preflightQueryRequestAlloc(std.testing.allocator, parsed.value));
    const diagnostic = db_mod.takeLastSortRejectionDiagnostic() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("*", diagnostic.field);
    try std.testing.expectEqualStrings("invalid_cursor_arity", diagnostic.reason);
    try std.testing.expectEqualStrings("invalid_cursor_arity", diagnostic.detail);
}

test "api query contract preflight rejects cursor pagination over approximate vector source" {
    var parsed = try std.json.parseFromSlice(metadata_openapi.QueryRequest, std.testing.allocator,
        \\{
        \\  "embeddings": {"dense_idx":"AACAPwAAAEAAAEBA"},
        \\  "indexes": ["dense_idx"],
        \\  "search_after": ["doc-9"],
        \\  "limit": 10
        \\}
    , .{});
    defer parsed.deinit();

    db_mod.resetLastSortRejectionDiagnostic();
    try std.testing.expectError(error.UnsupportedQueryRequest, preflightQueryRequestAlloc(std.testing.allocator, parsed.value));
    const diagnostic = db_mod.takeLastSortRejectionDiagnostic() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("_id", diagnostic.field);
    try std.testing.expectEqualStrings("approximate_candidate_source", diagnostic.reason);
    try std.testing.expectEqualStrings("approximate_candidate_source", diagnostic.detail);
}

test "api query contract preflight rejects search_before pagination over approximate vector source" {
    var parsed = try std.json.parseFromSlice(metadata_openapi.QueryRequest, std.testing.allocator,
        \\{
        \\  "embeddings": {"dense_idx":"AACAPwAAAEAAAEBA"},
        \\  "indexes": ["dense_idx"],
        \\  "search_before": ["doc-9"],
        \\  "limit": 10
        \\}
    , .{});
    defer parsed.deinit();

    db_mod.resetLastSortRejectionDiagnostic();
    try std.testing.expectError(error.UnsupportedQueryRequest, preflightQueryRequestAlloc(std.testing.allocator, parsed.value));
    const diagnostic = db_mod.takeLastSortRejectionDiagnostic() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("_id", diagnostic.field);
    try std.testing.expectEqualStrings("approximate_candidate_source", diagnostic.reason);
    try std.testing.expectEqualStrings("approximate_candidate_source", diagnostic.detail);
}

test "api query contract preflight rejects score sort over approximate vector source" {
    var parsed = try std.json.parseFromSlice(metadata_openapi.QueryRequest, std.testing.allocator,
        \\{
        \\  "embeddings": {"dense_idx":"AACAPwAAAEAAAEBA"},
        \\  "indexes": ["dense_idx"],
        \\  "order_by": [{"field":"_score","desc":true}],
        \\  "limit": 10
        \\}
    , .{});
    defer parsed.deinit();

    db_mod.resetLastSortRejectionDiagnostic();
    try std.testing.expectError(error.UnsupportedQueryRequest, preflightQueryRequestAlloc(std.testing.allocator, parsed.value));
    const diagnostic = db_mod.takeLastSortRejectionDiagnostic() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("_score", diagnostic.field);
    try std.testing.expectEqualStrings("approximate_candidate_source", diagnostic.reason);
    try std.testing.expectEqualStrings("approximate_candidate_source", diagnostic.detail);
}

test "api query contract preflight rejects score sort without score-bearing source" {
    var parsed_match_all = try std.json.parseFromSlice(metadata_openapi.QueryRequest, std.testing.allocator,
        \\{
        \\  "full_text_search": {"match_all": {}},
        \\  "order_by": [{"field":"_score","desc":true}]
        \\}
    , .{});
    defer parsed_match_all.deinit();

    db_mod.resetLastSortRejectionDiagnostic();
    try std.testing.expectError(error.UnsupportedQueryRequest, preflightQueryRequestAlloc(std.testing.allocator, parsed_match_all.value));
    const diagnostic = db_mod.takeLastSortRejectionDiagnostic() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("_score", diagnostic.field);
    try std.testing.expectEqualStrings("non_score_bearing_source", diagnostic.reason);
    try std.testing.expectEqualStrings("non_score_bearing_source", diagnostic.detail);

    var parsed_filter_only = try std.json.parseFromSlice(metadata_openapi.QueryRequest, std.testing.allocator,
        \\{
        \\  "filter_query": {"term":{"field":"status","value":"active"}},
        \\  "order_by": [{"field":"_score","desc":true}]
        \\}
    , .{});
    defer parsed_filter_only.deinit();

    db_mod.resetLastSortRejectionDiagnostic();
    try std.testing.expectError(error.UnsupportedQueryRequest, preflightQueryRequestAlloc(std.testing.allocator, parsed_filter_only.value));
    const filter_diagnostic = db_mod.takeLastSortRejectionDiagnostic() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("_score", filter_diagnostic.field);
    try std.testing.expectEqualStrings("non_score_bearing_source", filter_diagnostic.reason);
    try std.testing.expectEqualStrings("non_score_bearing_source", filter_diagnostic.detail);

    var parsed_match = try std.json.parseFromSlice(metadata_openapi.QueryRequest, std.testing.allocator,
        \\{
        \\  "full_text_search": {"match":"raft","field":"body"},
        \\  "order_by": [{"field":"_score","desc":true}]
        \\}
    , .{});
    defer parsed_match.deinit();

    var summary = try preflightQueryRequestAlloc(std.testing.allocator, parsed_match.value);
    defer summary.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u32, 1), summary.base_result_set_count);
}

test "api query contract appends stable id sort tiebreaker for cursors" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "full_text_search": {"match":"raft","field":"body"},
        \\  "order_by": [{"field":"created_at","desc":true}],
        \\  "search_after": ["2026-01-01", "doc-9"],
        \\  "limit": 10
        \\}
    ;

    var parsed = try parseQueryRequest(alloc, null, "docs", body);
    defer parsed.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), parsed.req.order_by.len);
    try std.testing.expectEqualStrings("created_at", parsed.req.order_by[0].field);
    try std.testing.expect(parsed.req.order_by[0].desc);
    try std.testing.expectEqualStrings("_id", parsed.req.order_by[1].field);
    try std.testing.expect(!parsed.req.order_by[1].desc);
    try std.testing.expectEqual(@as(usize, 2), parsed.req.search_after.len);
    try std.testing.expectEqualStrings("2026-01-01", parsed.req.search_after[0].string);
    try std.testing.expectEqualStrings("doc-9", parsed.req.search_after[1].string);
}

test "api query contract rejects cursor width that omits stable id tiebreaker" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "full_text_search": {"match":"raft","field":"body"},
        \\  "order_by": [{"field":"created_at","desc":true}],
        \\  "search_after": ["2026-01-01"]
        \\}
    ;

    db_mod.resetLastSortRejectionDiagnostic();
    try std.testing.expectError(error.UnsupportedQueryRequest, parseQueryRequest(alloc, null, "docs", body));
    const diagnostic = db_mod.takeLastSortRejectionDiagnostic() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("*", diagnostic.field);
    try std.testing.expectEqualStrings("invalid_cursor_arity", diagnostic.reason);
    try std.testing.expectEqualStrings("invalid_cursor_arity", diagnostic.detail);
}

test "api query contract records cursor arity diagnostic without sort" {
    const alloc = std.testing.allocator;

    db_mod.resetLastSortRejectionDiagnostic();
    try std.testing.expectError(error.UnsupportedQueryRequest, parseQueryRequest(alloc, null, "docs",
        \\{
        \\  "full_text_search": {"match":"raft","field":"body"},
        \\  "search_after": ["2026-01-01", "doc-9"]
        \\}
    ));
    const diagnostic = db_mod.takeLastSortRejectionDiagnostic() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("*", diagnostic.field);
    try std.testing.expectEqualStrings("invalid_cursor_arity", diagnostic.reason);
    try std.testing.expectEqualStrings("invalid_cursor_arity", diagnostic.detail);
}

test "api query contract rejects non replayable search_after cursor values" {
    const alloc = std.testing.allocator;

    db_mod.resetLastSortRejectionDiagnostic();
    try std.testing.expectError(error.InvalidQueryRequest, parseQueryRequest(alloc, null, "docs",
        \\{
        \\  "full_text_search": {"match":"raft","field":"body"},
        \\  "order_by": [{"field":"created_at","desc":true}],
        \\  "search_after": [null, "doc-9"]
        \\}
    ));
    var diagnostic = db_mod.takeLastSortRejectionDiagnostic() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("created_at", diagnostic.field);
    try std.testing.expectEqualStrings("invalid_cursor_type", diagnostic.reason);
    try std.testing.expectEqualStrings("invalid_cursor_type", diagnostic.detail);

    try std.testing.expectError(error.InvalidQueryRequest, parseQueryRequest(alloc, null, "docs",
        \\{
        \\  "full_text_search": {"match":"raft","field":"body"},
        \\  "order_by": [{"field":"created_at","desc":true}],
        \\  "search_after": [{"value":"2026-01-01"}, "doc-9"]
        \\}
    ));

    db_mod.resetLastSortRejectionDiagnostic();
    try std.testing.expectError(error.InvalidQueryRequest, parseQueryRequest(alloc, null, "docs",
        \\{
        \\  "full_text_search": {"match":"raft","field":"body"},
        \\  "order_by": [{"field":"_score","desc":true}],
        \\  "search_after": ["high", "doc-9"]
        \\}
    ));
    diagnostic = db_mod.takeLastSortRejectionDiagnostic() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("_score", diagnostic.field);
    try std.testing.expectEqualStrings("invalid_cursor_type", diagnostic.reason);
    try std.testing.expectEqualStrings("invalid_cursor_type", diagnostic.detail);
}

test "api query contract rejects score sort without score-bearing text source" {
    const alloc = std.testing.allocator;

    db_mod.resetLastSortRejectionDiagnostic();
    try std.testing.expectError(error.UnsupportedQueryRequest, parseQueryRequest(alloc, null, "docs",
        \\{
        \\  "full_text_search": {"match_all": {}},
        \\  "order_by": [{"field":"_score","desc":true}]
        \\}
    ));
    const diagnostic = db_mod.takeLastSortRejectionDiagnostic() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("_score", diagnostic.field);
    try std.testing.expectEqualStrings("non_score_bearing_source", diagnostic.reason);
    try std.testing.expectEqualStrings("non_score_bearing_source", diagnostic.detail);

    var parsed = try parseQueryRequest(alloc, null, "docs",
        \\{
        \\  "full_text_search": {"match":"raft","field":"body"},
        \\  "order_by": [{"field":"_score","desc":true}]
        \\}
    );
    defer parsed.deinit(alloc);
    try std.testing.expect(db_mod.searchRequestHasScoreBearingTextSource(parsed.req));
}

test "api query contract rejects non replayable search_before cursor values" {
    const alloc = std.testing.allocator;

    try std.testing.expectError(error.InvalidQueryRequest, parseQueryRequest(alloc, null, "docs",
        \\{
        \\  "full_text_search": {"match":"raft","field":"body"},
        \\  "order_by": [{"field":"created_at","desc":true}],
        \\  "search_before": [[], "doc-9"]
        \\}
    ));

    try std.testing.expectError(error.InvalidQueryRequest, parseQueryRequest(alloc, null, "docs",
        \\{
        \\  "full_text_search": {"match":"raft","field":"body"},
        \\  "order_by": [{"field":"created_at","desc":true}],
        \\  "search_before": ["2026-01-01", 9]
        \\}
    ));
}

test "api query contract rejects ambiguous explicit id sort tiebreaker" {
    const alloc = std.testing.allocator;

    db_mod.resetLastSortRejectionDiagnostic();
    try std.testing.expectError(error.UnsupportedQueryRequest, parseQueryRequest(alloc, null, "docs",
        \\{
        \\  "full_text_search": {"match":"raft","field":"body"},
        \\  "order_by": [{"field":"_id"},{"field":"created_at"}]
        \\}
    ));
    var diagnostic = db_mod.takeLastSortRejectionDiagnostic() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("_id", diagnostic.field);
    try std.testing.expectEqualStrings("invalid_cursor_arity", diagnostic.reason);
    try std.testing.expectEqualStrings("invalid_cursor_arity", diagnostic.detail);

    db_mod.resetLastSortRejectionDiagnostic();
    try std.testing.expectError(error.UnsupportedQueryRequest, parseQueryRequest(alloc, null, "docs",
        \\{
        \\  "full_text_search": {"match":"raft","field":"body"},
        \\  "order_by": [{"field":"created_at"},{"field":"_id","desc":true}]
        \\}
    ));
    diagnostic = db_mod.takeLastSortRejectionDiagnostic() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("_id", diagnostic.field);
    try std.testing.expectEqualStrings("invalid_cursor_arity", diagnostic.reason);
    try std.testing.expectEqualStrings("invalid_cursor_arity", diagnostic.detail);

    db_mod.resetLastSortRejectionDiagnostic();
    try std.testing.expectError(error.UnsupportedQueryRequest, parseQueryRequest(alloc, null, "docs",
        \\{
        \\  "full_text_search": {"match":"raft","field":"body"},
        \\  "order_by": [{"field":"created_at"},{"field":"created_at","desc":true}]
        \\}
    ));
    diagnostic = db_mod.takeLastSortRejectionDiagnostic() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("created_at", diagnostic.field);
    try std.testing.expectEqualStrings("invalid_cursor_arity", diagnostic.reason);
    try std.testing.expectEqualStrings("invalid_cursor_arity", diagnostic.detail);
}

test "api query contract parses packed dense embeddings via antfly-json" {
    const alloc = std.testing.allocator;
    const body =
        \\{"embeddings":{"dense_idx":"AACAPwAAAEAAAEBA"},"indexes":["dense_idx"],"limit":3}
    ;

    var parsed = try parseQueryRequest(alloc, null, "docs", body);
    defer parsed.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), parsed.req.dense_queries.len);
    try std.testing.expectEqual(@as(usize, 0), parsed.req.sparse_queries.len);
    try std.testing.expectEqual(@as(u32, 3), parsed.req.dense_queries[0].query.k);
    try std.testing.expectEqual(@as(usize, 3), parsed.req.dense_queries[0].query.vector.len);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), parsed.req.dense_queries[0].query.vector[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), parsed.req.dense_queries[0].query.vector[1], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 3.0), parsed.req.dense_queries[0].query.vector[2], 0.0001);
}

test "api query contract does not use dense fast path for composed vector requests" {
    const alloc = std.testing.allocator;
    const body =
        \\{"embeddings":{"dense_idx":"AACAPwAAAEAAAEBA"},"indexes":["dense_idx"],"full_text_search":{"match":"alpha","field":"body"},"filter_query":{"term":{"status":"active"}},"exclusion_query":{"term":{"category":"archived"}},"limit":3}
    ;

    var parsed = try parseQueryRequest(alloc, null, "docs", body);
    defer parsed.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), parsed.req.dense_queries.len);
    try std.testing.expect(parsed.req.full_text != null);
    try std.testing.expect(parsed.req.filter_query_json.len > 0);
    try std.testing.expect(parsed.req.exclusion_query_json.len > 0);
}

test "api query contract parses packed sparse embeddings via antfly-json" {
    const alloc = std.testing.allocator;
    const body =
        \\{"embeddings":{"sparse_idx":{"packed_indices":"AQAAAAUAAAA=","packed_values":"AAAAPwAAQD8=","k":4}},"indexes":["sparse_idx"],"limit":9}
    ;

    var parsed = try parseQueryRequest(alloc, null, "docs", body);
    defer parsed.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 0), parsed.req.dense_queries.len);
    try std.testing.expectEqual(@as(usize, 1), parsed.req.sparse_queries.len);
    try std.testing.expectEqual(@as(u32, 4), parsed.req.sparse_queries[0].query.k);
    try std.testing.expectEqualSlices(u32, &.{ 1, 5 }, parsed.req.sparse_queries[0].query.indices);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), parsed.req.sparse_queries[0].query.values[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.75), parsed.req.sparse_queries[0].query.values[1], 0.0001);
}

test "api query contract parses explicit algebraic aggregation join" {
    const alloc = std.testing.allocator;
    const aggregations_json =
        \\{
        \\  "by_segment": {
        \\    "type": "terms",
        \\    "field": "segment",
        \\    "algebraic_join": {
        \\      "name": "orders_customers",
        \\      "kind": "bucket",
        \\      "group_side": "right",
        \\      "measure_side": "left"
        \\    },
        \\    "sub_aggregations": {
        \\      "amount": {"type": "sum", "field": "amount"}
        \\    }
        \\  }
        \\}
    ;
    const requests = try parseAggregationRequestsJson(alloc, aggregations_json);
    defer freeAggregationRequests(alloc, requests);

    try std.testing.expectEqual(@as(usize, 1), requests.len);
    const join = requests[0].algebraic_join.?;
    try std.testing.expectEqualStrings("orders_customers", join.name);
    try std.testing.expectEqual(db_mod.algebraic.join.TemporalMode.bucket, join.kind);
    try std.testing.expectEqualStrings("right", join.group_side.?);
    try std.testing.expectEqualStrings("left", join.measure_side.?);
    try std.testing.expectEqual(@as(usize, 1), requests[0].aggregations.len);
    try std.testing.expect(requests[0].aggregations[0].algebraic_join == null);
}

test "api query contract parses multi field terms aggregation" {
    const alloc = std.testing.allocator;
    const aggregations_json =
        \\{
        \\  "by_customer_product": {
        \\    "type": "terms",
        \\    "fields": ["customer", "product"],
        \\    "sub_aggregations": {
        \\      "amount": {"type": "sum", "field": "amount"}
        \\    }
        \\  }
        \\}
    ;
    const requests = try parseAggregationRequestsJson(alloc, aggregations_json);
    defer freeAggregationRequests(alloc, requests);

    try std.testing.expectEqual(@as(usize, 1), requests.len);
    try std.testing.expectEqualStrings("customer", requests[0].field);
    try std.testing.expectEqual(@as(usize, 2), requests[0].fields.len);
    try std.testing.expectEqualStrings("customer", requests[0].fields[0]);
    try std.testing.expectEqualStrings("product", requests[0].fields[1]);
    try std.testing.expectEqual(@as(usize, 1), requests[0].aggregations.len);
    try std.testing.expectEqualStrings("amount", requests[0].aggregations[0].field);
}

test "api query contract rejects multi field non terms aggregation" {
    const alloc = std.testing.allocator;
    const aggregations_json =
        \\{
        \\  "amount": {
        \\    "type": "sum",
        \\    "fields": ["amount", "tax"]
        \\  }
        \\}
    ;

    try std.testing.expectError(error.InvalidQueryRequest, parseAggregationRequestsJson(alloc, aggregations_json));
}

test "api query contract rejects conflicting terms field and fields" {
    const alloc = std.testing.allocator;
    const aggregations_json =
        \\{
        \\  "by_customer_product": {
        \\    "type": "terms",
        \\    "field": "tenant",
        \\    "fields": ["customer", "product"]
        \\  }
        \\}
    ;

    try std.testing.expectError(error.InvalidQueryRequest, parseAggregationRequestsJson(alloc, aggregations_json));
}

test "api query contract exposes native doc id constraint envelope for non-query worker protocols" {
    const alloc = std.testing.allocator;
    const source = db_mod.types.SearchRequest{
        .filter_doc_ids_positive = true,
        .filter_doc_ids = &.{},
        .exclude_doc_ids = &.{"doc:c"},
    };
    const envelope = nativeDocIdConstraintEnvelopeFromSearchRequest(source);
    try std.testing.expect(envelope.hasConstraints());

    const encoded = try encodeNativeDocIdConstraintEnvelopeAlloc(alloc, envelope);
    defer alloc.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"positive_filter\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"include_doc_ids\":[]") != null);

    var parsed = try parseNativeDocIdConstraintEnvelopeAlloc(alloc, encoded);
    defer parsed.deinit(alloc);
    try std.testing.expect(parsed.constraints.positive_filter);
    try std.testing.expectEqual(@as(usize, 0), parsed.constraints.include_doc_ids.len);
    try std.testing.expectEqual(@as(usize, 1), parsed.constraints.exclude_doc_ids.len);
    try std.testing.expectEqualStrings("doc:c", parsed.constraints.exclude_doc_ids[0]);
}

test "api query contract normalizes native include doc ids to a positive envelope" {
    const alloc = std.testing.allocator;
    const envelope = NativeDocIdConstraintEnvelope{
        .positive_filter = false,
        .include_doc_ids = &.{ "doc:b", "doc:a", "doc:b", "doc:c" },
        .exclude_doc_ids = &.{ "doc:d", "doc:c", "doc:c" },
    };

    const encoded = try encodeNativeDocIdConstraintEnvelopeAlloc(alloc, envelope);
    defer alloc.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"positive_filter\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"include_doc_ids\":[\"doc:a\",\"doc:b\"]") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"exclude_doc_ids\":[\"doc:c\",\"doc:d\"]") != null);

    var parsed = try parseNativeDocIdConstraintEnvelopeAlloc(alloc, encoded);
    defer parsed.deinit(alloc);
    try std.testing.expect(parsed.constraints.positive_filter);
    try std.testing.expectEqual(@as(usize, 2), parsed.constraints.include_doc_ids.len);
    try std.testing.expectEqualStrings("doc:a", parsed.constraints.include_doc_ids[0]);
    try std.testing.expectEqualStrings("doc:b", parsed.constraints.include_doc_ids[1]);
    try std.testing.expectEqual(@as(usize, 2), parsed.constraints.exclude_doc_ids.len);
    try std.testing.expectEqualStrings("doc:c", parsed.constraints.exclude_doc_ids[0]);
    try std.testing.expectEqualStrings("doc:d", parsed.constraints.exclude_doc_ids[1]);
}

test "api query contract exposes typed tensor access path envelope for worker protocols" {
    const alloc = std.testing.allocator;
    const dictionary = algebraic_lexical.DictionaryIdentity.analyzedText("docs", "body", "default");
    const path = algebraic_ir.PhysicalAccessPath{
        .owner = "body_terms",
        .layout = .full_text_postings,
        .dictionary = dictionary,
        .fragments = &.{ .slice, .automaton_select },
        .output_dims = &.{.doc},
    };

    const encoded = try encodeAlgebraicTensorAccessPathEnvelopeAlloc(alloc, path);
    defer alloc.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"owner\":\"body_terms\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"layout\":\"full_text_postings\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"dictionary\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"label_kind\":\"analyzed_term\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"fragments\":[\"slice\",\"automaton_select\"]") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"output_dims\":[\"doc\"]") != null);

    var parsed = try parseAlgebraicTensorAccessPathEnvelopeAlloc(alloc, encoded);
    defer parsed.deinit(alloc);
    const parsed_path = parsed.asAccessPath();
    try std.testing.expectEqualStrings(path.owner, parsed_path.owner);
    try std.testing.expectEqual(path.layout, parsed_path.layout);
    try std.testing.expect(parsed_path.dictionary != null);
    try std.testing.expect(dictionary.eql(parsed_path.dictionary.?));
    try std.testing.expectEqualSlices(algebraic_ir.TensorFragment, path.fragments, parsed_path.fragments);
    try std.testing.expectEqualSlices(algebraic_ir.Dimension, path.output_dims, parsed_path.output_dims);
    try std.testing.expectEqualSlices(algebraic_law.Id, path.law_ids, parsed_path.law_ids);
}

test "api query contract exposes typed tensor expression envelope for worker protocols" {
    const alloc = std.testing.allocator;
    const dictionary = algebraic_lexical.DictionaryIdentity.canonicalScalar("docs", "/customer", .string, "json-scalar-v1", "kind-qualified");
    const expr = algebraic_ir.TensorExpr{
        .fragment = .reduce,
        .input_dims = &.{ .doc, .scalar },
        .output_dims = &.{.bucket},
        .semantic_id = "sum_by_customer",
        .layout = .materialized_expr,
        .dictionary = dictionary,
        .law_id = .sum,
    };

    const encoded = try encodeAlgebraicTensorExprEnvelopeAlloc(alloc, expr);
    defer alloc.free(encoded);
    const expected_expr_id = try algebraic_ir.tensorExprIdAlloc(alloc, expr);
    defer alloc.free(expected_expr_id);
    const expected_expr_id_json = try std.fmt.allocPrint(alloc, "\"expr_id\":\"{s}\"", .{expected_expr_id});
    defer alloc.free(expected_expr_id_json);
    try std.testing.expect(std.mem.indexOf(u8, encoded, expected_expr_id_json) != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"fragment\":\"reduce\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"input_dims\":[\"doc\",\"scalar\"]") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"output_dims\":[\"bucket\"]") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"semantic_id\":\"sum_by_customer\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"layout\":\"materialized_expr\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"label_kind\":\"canonical_scalar\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"law_id\":\"sum\"") != null);

    var parsed = try parseAlgebraicTensorExprEnvelopeAlloc(alloc, encoded);
    defer parsed.deinit(alloc);
    const parsed_expr = parsed.asExpr();
    try std.testing.expectEqualStrings(expected_expr_id, parsed.expr_id);
    try std.testing.expectEqual(expr.fragment, parsed_expr.fragment);
    try std.testing.expectEqualSlices(algebraic_ir.Dimension, expr.input_dims, parsed_expr.input_dims);
    try std.testing.expectEqualSlices(algebraic_ir.Dimension, expr.output_dims, parsed_expr.output_dims);
    try std.testing.expectEqualStrings(expr.semantic_id.?, parsed_expr.semantic_id.?);
    try std.testing.expect(parsed_expr.owner == null);
    try std.testing.expectEqual(expr.layout.?, parsed_expr.layout.?);
    try std.testing.expect(parsed_expr.dictionary != null);
    try std.testing.expect(dictionary.eql(parsed_expr.dictionary.?));
    try std.testing.expectEqual(expr.law_id.?, parsed_expr.law_id.?);

    var plan = (try algebraic_ir.planMaterializedExpressionAlloc(alloc, parsed_expr)).?;
    defer plan.deinit(alloc);
    try std.testing.expectEqualStrings(parsed.expr_id, plan.expr_id);
    try std.testing.expect(algebraic_ir.accessPathCanSatisfy(plan.access_path, parsed_expr).safe());

    var tampered = try std.ArrayListUnmanaged(u8).initCapacity(alloc, encoded.len + 16);
    defer tampered.deinit(alloc);
    try tampered.appendSlice(alloc, encoded);
    const id_pos = std.mem.indexOf(u8, tampered.items, expected_expr_id) orelse return error.TestUnexpectedResult;
    tampered.items[id_pos] = if (tampered.items[id_pos] == 'x') 'y' else 'x';
    try std.testing.expectError(error.InvalidQueryRequest, parseAlgebraicTensorExprEnvelopeAlloc(alloc, tampered.items));

    const missing_id =
        \\{
        \\  "fragment": "reduce",
        \\  "input_dims": ["doc", "scalar"],
        \\  "output_dims": ["bucket"],
        \\  "semantic_id": "sum_by_customer",
        \\  "owner": "expr:sum_by_customer",
        \\  "layout": "materialized_expr",
        \\  "law_id": "sum"
        \\}
    ;
    try std.testing.expectError(error.InvalidQueryRequest, parseAlgebraicTensorExprEnvelopeAlloc(alloc, missing_id));
}

test "api query contract exposes typed tensor program envelope for worker protocols" {
    const alloc = std.testing.allocator;
    const dictionary = algebraic_lexical.DictionaryIdentity.analyzedText("docs", "body", "default");
    const input_expr = algebraic_ir.TensorExpr{
        .fragment = .automaton_select,
        .output_dims = &.{.doc},
        .dictionary = dictionary,
    };
    const reduce_step = algebraic_ir.TensorProgramStep{
        .expr = .{
            .fragment = .reduce,
            .input_dims = &.{.doc},
            .output_dims = &.{.bucket},
            .law_id = .count,
            .metadata = "fold:v1:bucket-body-count",
        },
        .inputs = &.{.{ .input = 0 }},
    };
    const program = algebraic_ir.TensorProgram{
        .inputs = &.{input_expr},
        .steps = &.{reduce_step},
        .output = .{ .step = 0 },
        .outputs = &.{ .{ .input = 0 }, .{ .step = 0 } },
    };
    const encoded = try encodeAlgebraicTensorProgramEnvelopeAlloc(alloc, program);
    defer alloc.free(encoded);
    const expected_program_id = try algebraic_ir.tensorProgramIdAlloc(alloc, program);
    defer alloc.free(expected_program_id);
    const expected_program_id_json = try std.fmt.allocPrint(alloc, "\"program_id\":\"{s}\"", .{expected_program_id});
    defer alloc.free(expected_program_id_json);
    try std.testing.expect(std.mem.indexOf(u8, encoded, expected_program_id_json) != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"inputs\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"steps\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"kind\":\"input\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"kind\":\"step\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"outputs\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"label_kind\":\"analyzed_term\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"metadata\":\"fold:v1:bucket-body-count\"") != null);

    var parsed = try parseAlgebraicTensorProgramEnvelopeAlloc(alloc, encoded);
    defer parsed.deinit(alloc);
    try std.testing.expectEqualStrings(expected_program_id, parsed.program_id);
    var view = try parsed.asProgramAlloc(alloc);
    defer view.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), view.program.outputs.len);
    try std.testing.expectEqualStrings("fold:v1:bucket-body-count", view.program.steps[0].expr.metadata.?);
    const reparsed_id = try algebraic_ir.tensorProgramIdAlloc(alloc, view.program);
    defer alloc.free(reparsed_id);
    try std.testing.expectEqualStrings(expected_program_id, reparsed_id);

    const paths = [_]algebraic_ir.PhysicalAccessPath{
        algebraic_ir.lexicalAccessPath("body_terms", .full_text_postings, dictionary, true),
    };
    try std.testing.expect((try algebraic_ir.tensorProgramProof(alloc, &paths, view.program)).safe());

    var tampered = try std.ArrayListUnmanaged(u8).initCapacity(alloc, encoded.len + 16);
    defer tampered.deinit(alloc);
    try tampered.appendSlice(alloc, encoded);
    const id_pos = std.mem.indexOf(u8, tampered.items, expected_program_id) orelse return error.TestUnexpectedResult;
    tampered.items[id_pos] = if (tampered.items[id_pos] == 'x') 'y' else 'x';
    try std.testing.expectError(error.InvalidQueryRequest, parseAlgebraicTensorProgramEnvelopeAlloc(alloc, tampered.items));

    var bad_output_ref = try std.ArrayListUnmanaged(u8).initCapacity(alloc, encoded.len + 16);
    defer bad_output_ref.deinit(alloc);
    try bad_output_ref.appendSlice(alloc, encoded);
    const output_ref_pos = std.mem.indexOf(u8, bad_output_ref.items, "\"output\":{\"kind\":\"step\",\"index\":0}") orelse return error.TestUnexpectedResult;
    bad_output_ref.items[output_ref_pos + "\"output\":{\"kind\":\"step\",\"index\":".len] = '9';
    try std.testing.expectError(error.InvalidQueryRequest, parseAlgebraicTensorProgramEnvelopeAlloc(alloc, bad_output_ref.items));
}

test "api query contract carries vector worker tensor program and native constraints together" {
    const alloc = std.testing.allocator;
    const access_path = algebraic_ir.vectorAccessPath("dense_idx", .dense_vector);
    const candidate_input = algebraic_ir.TensorExpr{
        .fragment = .slice,
        .output_dims = &.{.doc},
        .semantic_id = "native_doc_id_constraints",
    };
    const program = algebraic_ir.TensorProgram{
        .inputs = &.{candidate_input},
        .steps = &.{.{
            .expr = .{
                .fragment = .vector_search,
                .input_dims = &.{.doc},
                .output_dims = &.{ .doc, .score },
                .owner = "dense_idx",
                .layout = .dense_vector,
            },
            .inputs = &.{.{ .input = 0 }},
        }},
        .output = .{ .step = 0 },
    };
    const constraints = NativeDocIdConstraintEnvelope{
        .positive_filter = true,
        .include_doc_ids = &.{ "doc:a", "doc:b" },
        .exclude_doc_ids = &.{"doc:c"},
    };
    const encoded = try encodeAlgebraicVectorWorkerRequestEnvelopeAlloc(
        alloc,
        "dense_idx",
        .dense_vector,
        .{ .dense = .{ .vector = &.{ 0.25, 0.5, 1.0 }, .k = 7 } },
        .{
            .fields = @constCast((&[_][]const u8{ "title", "score" })[0..]),
            .filter_query_json = "{\"term\":{\"path\":\"/tenant\",\"value\":\"t1\"}}",
            .exclusion_query_json = "{\"term\":{\"path\":\"/deleted\",\"value\":true}}",
            .filter_prefix = "tenant/a/",
            .filter_ids = &.{ 42, 99 },
            .exclude_ids = &.{7},
            .require_algebraic_filter_resolution = true,
            .include_all_fields = false,
            .defer_stored_projection = true,
            .limit = 9,
            .offset = 2,
            .count_only = true,
            .profile = true,
            .include_stored = false,
            .search_effort = 0.75,
            .distance_over = 0.1,
            .distance_under = 0.9,
            .return_mode = .parent_with_chunks,
            .max_chunks_per_parent = 2,
            .hierarchy_include_source = true,
            .hierarchy_include_unit = true,
            .hierarchy_omit_implicit_source_ancestor_document = true,
            .hierarchy_match_fields = @constCast((&[_][]const u8{"text"})[0..]),
            .hierarchy_match_include_all_fields = false,
            .hierarchy_grouped_matches = true,
            .hierarchy_source_fields = @constCast((&[_][]const u8{ "title", "url" })[0..]),
            .hierarchy_source_include_all_fields = false,
            .hierarchy_unit_fields = @constCast((&[_][]const u8{"page"})[0..]),
            .hierarchy_unit_include_all_fields = false,
            .identity_read_generation = 12345,
        },
        constraints,
        null,
        null,
        &.{access_path},
        program,
    );
    defer alloc.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"index_name\":\"dense_idx\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"layout\":\"dense_vector\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"query\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"kind\":\"dense\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"options\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"native_doc_id_constraints\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"tensor_access_paths\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"tensor_program\"") != null);

    const tampered_target = try alloc.dupe(u8, encoded);
    defer alloc.free(tampered_target);
    const index_name_prefix = "\"index_name\":\"";
    const index_name_pos = std.mem.indexOf(u8, tampered_target, index_name_prefix) orelse return error.TestUnexpectedResult;
    const index_name_start = index_name_pos + index_name_prefix.len;
    tampered_target[index_name_start] = if (tampered_target[index_name_start] == 'd') 'x' else 'd';
    try std.testing.expectError(error.InvalidQueryRequest, parseAlgebraicVectorWorkerRequestEnvelopeAlloc(alloc, tampered_target));

    var parsed = try parseAlgebraicVectorWorkerRequestEnvelopeAlloc(alloc, encoded);
    defer parsed.deinit(alloc);
    try std.testing.expectEqualStrings("dense_idx", parsed.index_name);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.dense_vector, parsed.layout);
    try std.testing.expectEqual(@as(u32, 7), parsed.query.dense.k);
    try std.testing.expectEqual(@as(u32, 9), parsed.options.limit);
    try std.testing.expectEqual(@as(u32, 2), parsed.options.offset);
    try std.testing.expect(parsed.options.count_only);
    try std.testing.expect(parsed.options.profile);
    try std.testing.expect(!parsed.options.include_stored);
    try std.testing.expect(!parsed.options.include_all_fields);
    try std.testing.expect(parsed.options.defer_stored_projection);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/tenant\",\"value\":\"t1\"}}", parsed.options.filter_query_json);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/deleted\",\"value\":true}}", parsed.options.exclusion_query_json);
    try std.testing.expect(parsed.options.require_algebraic_filter_resolution);
    try std.testing.expectEqualStrings("tenant/a/", parsed.options.filter_prefix);
    try std.testing.expectEqual(@as(usize, 2), parsed.options.filter_ids.len);
    try std.testing.expectEqual(@as(u64, 42), parsed.options.filter_ids[0]);
    try std.testing.expectEqual(@as(u64, 99), parsed.options.filter_ids[1]);
    try std.testing.expectEqual(@as(usize, 1), parsed.options.exclude_ids.len);
    try std.testing.expectEqual(@as(u64, 7), parsed.options.exclude_ids[0]);
    try std.testing.expectEqual(@as(usize, 2), parsed.options.fields.len);
    try std.testing.expectEqualStrings("title", parsed.options.fields[0]);
    try std.testing.expectEqualStrings("score", parsed.options.fields[1]);
    try std.testing.expectApproxEqAbs(@as(f32, 0.75), parsed.options.search_effort.?, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.1), parsed.options.distance_over.?, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.9), parsed.options.distance_under.?, 0.0001);
    try std.testing.expectEqual(db_mod.types.ReturnMode.parent_with_chunks, parsed.options.return_mode);
    try std.testing.expectEqual(@as(u32, 2), parsed.options.max_chunks_per_parent);
    try std.testing.expect(parsed.options.hierarchy_include_source);
    try std.testing.expect(parsed.options.hierarchy_include_unit);
    try std.testing.expect(parsed.options.hierarchy_omit_implicit_source_ancestor_document);
    try std.testing.expectEqualStrings("text", parsed.options.hierarchy_match_fields[0]);
    try std.testing.expect(!parsed.options.hierarchy_match_include_all_fields);
    try std.testing.expect(parsed.options.hierarchy_grouped_matches);
    try std.testing.expectEqualStrings("url", parsed.options.hierarchy_source_fields[1]);
    try std.testing.expect(!parsed.options.hierarchy_source_include_all_fields);
    try std.testing.expectEqualStrings("page", parsed.options.hierarchy_unit_fields[0]);
    try std.testing.expect(!parsed.options.hierarchy_unit_include_all_fields);
    try std.testing.expectEqual(@as(?u64, 12345), parsed.options.identity_read_generation);
    try std.testing.expectEqual(@as(usize, 3), parsed.query.dense.vector.len);
    try std.testing.expectApproxEqAbs(@as(f32, 0.25), parsed.query.dense.vector[0], 0.0001);
    try std.testing.expect(parsed.native_doc_id_constraints.constraints.positive_filter);
    try std.testing.expectEqual(@as(usize, 2), parsed.native_doc_id_constraints.constraints.include_doc_ids.len);
    try std.testing.expectEqualStrings("doc:c", parsed.native_doc_id_constraints.constraints.exclude_doc_ids[0]);
    try std.testing.expectEqual(@as(usize, 1), parsed.tensor_access_paths.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.dense_vector, parsed.tensor_access_paths[0].layout);
    try std.testing.expect((try parsed.proveTensorProgramAlloc(alloc)).safe());

    var program_view = try parsed.tensor_program.asProgramAlloc(alloc);
    defer program_view.deinit(alloc);
    const program_id = try algebraic_ir.tensorProgramIdAlloc(alloc, program_view.program);
    defer alloc.free(program_id);
    try std.testing.expectEqualStrings(parsed.tensor_program.program_id, program_id);
}

test "api query contract keeps member mode compatible with rolling upgrade workers" {
    const alloc = std.testing.allocator;
    var encoded = std.ArrayListUnmanaged(u8).empty;
    defer encoded.deinit(alloc);

    try appendAlgebraicVectorWorkerRequestOptions(alloc, &encoded, .{ .return_mode = .member });
    try std.testing.expect(std.mem.indexOf(u8, encoded.items, "\"return_mode\":\"chunk\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.items, "\"return_mode\":\"member\"") == null);
}

test "api query contract carries sparse vector worker payload and proof" {
    const alloc = std.testing.allocator;
    const access_path = algebraic_ir.vectorAccessPath("sparse_idx", .sparse_vector);
    const program = algebraic_ir.TensorProgram{
        .steps = &.{.{
            .expr = .{
                .fragment = .vector_search,
                .output_dims = &.{ .doc, .score },
                .owner = "sparse_idx",
                .layout = .sparse_vector,
            },
        }},
        .output = .{ .step = 0 },
    };
    const encoded = try encodeAlgebraicVectorWorkerRequestEnvelopeAlloc(
        alloc,
        "sparse_idx",
        .sparse_vector,
        .{ .sparse = .{ .indices = &.{ 3, 9, 27 }, .values = &.{ 1.0, 0.5, 0.25 }, .k = 5 } },
        .{},
        .{},
        null,
        null,
        &.{access_path},
        program,
    );
    defer alloc.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"kind\":\"sparse\"") != null);

    var parsed = try parseAlgebraicVectorWorkerRequestEnvelopeAlloc(alloc, encoded);
    defer parsed.deinit(alloc);
    try std.testing.expectEqualStrings("sparse_idx", parsed.index_name);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.sparse_vector, parsed.layout);
    try std.testing.expectEqual(@as(u32, 5), parsed.query.sparse.k);
    try std.testing.expectEqual(@as(usize, 3), parsed.query.sparse.indices.len);
    try std.testing.expectEqual(@as(u32, 9), parsed.query.sparse.indices[1]);
    try std.testing.expectApproxEqAbs(@as(f32, 0.25), parsed.query.sparse.values[2], 0.0001);
    try std.testing.expect((try parsed.proveTensorProgramAlloc(alloc)).safe());
}

test "api query contract rejects sparse vector worker payload with mismatched indices and values" {
    const alloc = std.testing.allocator;
    const access_path = algebraic_ir.vectorAccessPath("sparse_idx", .sparse_vector);
    const program = algebraic_ir.TensorProgram{
        .steps = &.{.{
            .expr = .{
                .fragment = .vector_search,
                .output_dims = &.{ .doc, .score },
                .owner = "sparse_idx",
                .layout = .sparse_vector,
            },
        }},
        .output = .{ .step = 0 },
    };
    try std.testing.expectError(error.InvalidQueryRequest, encodeAlgebraicVectorWorkerRequestEnvelopeAlloc(
        alloc,
        "sparse_idx",
        .sparse_vector,
        .{ .sparse = .{ .indices = &.{ 3, 9 }, .values = &.{1.0}, .k = 5 } },
        .{},
        .{},
        null,
        null,
        &.{access_path},
        program,
    ));

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"kind":"sparse","k":5,"indices":[3,9],"values":[1.0]}
    , .{});
    defer parsed.deinit();
    try std.testing.expectError(
        error.InvalidQueryRequest,
        parseAlgebraicVectorWorkerQueryAlloc(alloc, .sparse_vector, parsed.value),
    );
}

test "api query contract rejects vector worker non-finite numeric payloads" {
    const alloc = std.testing.allocator;
    const dense_access_path = algebraic_ir.vectorAccessPath("dense_idx", .dense_vector);
    const dense_program = algebraic_ir.TensorProgram{
        .steps = &.{.{
            .expr = .{
                .fragment = .vector_search,
                .output_dims = &.{ .doc, .score },
                .owner = "dense_idx",
                .layout = .dense_vector,
            },
        }},
        .output = .{ .step = 0 },
    };
    try std.testing.expectError(error.InvalidQueryRequest, encodeAlgebraicVectorWorkerRequestEnvelopeAlloc(
        alloc,
        "dense_idx",
        .dense_vector,
        .{ .dense = .{ .vector = &.{std.math.inf(f32)}, .k = 1 } },
        .{},
        .{},
        null,
        null,
        &.{dense_access_path},
        dense_program,
    ));
    const sparse_access_path = algebraic_ir.vectorAccessPath("sparse_idx", .sparse_vector);
    const sparse_program = algebraic_ir.TensorProgram{
        .steps = &.{.{
            .expr = .{
                .fragment = .vector_search,
                .output_dims = &.{ .doc, .score },
                .owner = "sparse_idx",
                .layout = .sparse_vector,
            },
        }},
        .output = .{ .step = 0 },
    };
    try std.testing.expectError(error.InvalidQueryRequest, encodeAlgebraicVectorWorkerRequestEnvelopeAlloc(
        alloc,
        "sparse_idx",
        .sparse_vector,
        .{ .sparse = .{ .indices = &.{1}, .values = &.{std.math.nan(f32)}, .k = 1 } },
        .{},
        .{},
        null,
        null,
        &.{sparse_access_path},
        sparse_program,
    ));
    {
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc,
            \\{"kind":"dense","k":1,"vector":[1e9999]}
        , .{});
        defer parsed.deinit();
        try std.testing.expectError(
            error.InvalidQueryRequest,
            parseAlgebraicVectorWorkerQueryAlloc(alloc, .dense_vector, parsed.value),
        );
    }
    {
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc,
            \\{"kind":"sparse","k":1,"indices":[7],"values":[-1e9999]}
        , .{});
        defer parsed.deinit();
        try std.testing.expectError(
            error.InvalidQueryRequest,
            parseAlgebraicVectorWorkerQueryAlloc(alloc, .sparse_vector, parsed.value),
        );
    }
}

test "api query contract rejects vector worker envelope without matching tensor proof" {
    const alloc = std.testing.allocator;
    const access_path = algebraic_ir.vectorAccessPath("sparse_idx", .sparse_vector);
    const program = algebraic_ir.TensorProgram{
        .steps = &.{.{
            .expr = .{
                .fragment = .vector_search,
                .output_dims = &.{ .doc, .score },
                .owner = "dense_idx",
                .layout = .dense_vector,
            },
        }},
        .output = .{ .step = 0 },
    };
    try std.testing.expectError(error.InvalidQueryRequest, encodeAlgebraicVectorWorkerRequestEnvelopeAlloc(
        alloc,
        "sparse_idx",
        .sparse_vector,
        .{ .sparse = .{ .indices = &.{ 1, 5 }, .values = &.{ 1.0, 0.5 }, .k = 3 } },
        .{},
        .{},
        null,
        null,
        &.{access_path},
        program,
    ));
}

test "api query contract rejects vector worker envelope when target does not match access path" {
    const alloc = std.testing.allocator;
    const access_path = algebraic_ir.vectorAccessPath("other_dense_idx", .dense_vector);
    const program = algebraic_ir.TensorProgram{
        .steps = &.{.{
            .expr = .{
                .fragment = .vector_search,
                .output_dims = &.{ .doc, .score },
                .owner = "other_dense_idx",
                .layout = .dense_vector,
            },
        }},
        .output = .{ .step = 0 },
    };
    try std.testing.expectError(error.InvalidQueryRequest, encodeAlgebraicVectorWorkerRequestEnvelopeAlloc(
        alloc,
        "dense_idx",
        .dense_vector,
        .{ .dense = .{ .vector = &.{ 1.0, 0.0 }, .k = 2 } },
        .{},
        .{},
        null,
        null,
        &.{access_path},
        program,
    ));
}

test "api query contract rejects vector worker envelope when primary output is not vector search" {
    const alloc = std.testing.allocator;
    const access_path = algebraic_ir.vectorAccessPath("dense_idx", .dense_vector);
    const candidate_input = algebraic_ir.TensorExpr{
        .fragment = .slice,
        .output_dims = &.{.doc},
        .semantic_id = "native_doc_id_constraints",
    };
    const program = algebraic_ir.TensorProgram{
        .inputs = &.{candidate_input},
        .steps = &.{.{
            .expr = .{
                .fragment = .vector_search,
                .input_dims = &.{.doc},
                .output_dims = &.{ .doc, .score },
                .owner = "dense_idx",
                .layout = .dense_vector,
            },
            .inputs = &.{.{ .input = 0 }},
        }},
        .output = .{ .input = 0 },
    };
    try std.testing.expectError(error.InvalidQueryRequest, encodeAlgebraicVectorWorkerRequestEnvelopeAlloc(
        alloc,
        "dense_idx",
        .dense_vector,
        .{ .dense = .{ .vector = &.{ 1.0, 0.0 }, .k = 2 } },
        .{},
        .{},
        null,
        null,
        &.{access_path},
        program,
    ));
}

test "api query contract rejects vector worker envelope when native constraints are not consumed" {
    const alloc = std.testing.allocator;
    const access_path = algebraic_ir.vectorAccessPath("dense_idx", .dense_vector);
    const program = algebraic_ir.TensorProgram{
        .steps = &.{.{
            .expr = .{
                .fragment = .vector_search,
                .output_dims = &.{ .doc, .score },
                .owner = "dense_idx",
                .layout = .dense_vector,
            },
        }},
        .output = .{ .step = 0 },
    };
    try std.testing.expectError(error.InvalidQueryRequest, encodeAlgebraicVectorWorkerRequestEnvelopeAlloc(
        alloc,
        "dense_idx",
        .dense_vector,
        .{ .dense = .{ .vector = &.{ 1.0, 0.0 }, .k = 2 } },
        .{},
        .{ .positive_filter = true, .include_doc_ids = &.{"doc:a"} },
        null,
        null,
        &.{access_path},
        program,
    ));
}

test "api query contract accepts native doc id constraint envelope on internal query route" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "query": {"match_all": {}},
        \\  "native_doc_id_constraints": {
        \\    "positive_filter": true,
        \\    "include_doc_ids": [],
        \\    "exclude_doc_ids": ["doc:c"]
        \\  },
        \\  "_identity_read_generation": 42
        \\}
    ;

    var parsed = try parseQueryRequest(alloc, null, "docs", body);
    defer parsed.deinit(alloc);

    try std.testing.expect(parsed.req.filter_doc_ids_positive);
    try std.testing.expectEqual(@as(usize, 0), parsed.req.filter_doc_ids.len);
    try std.testing.expectEqual(@as(usize, 1), parsed.req.exclude_doc_ids.len);
    try std.testing.expectEqualStrings("doc:c", parsed.req.exclude_doc_ids[0]);
    try std.testing.expectEqual(@as(?u64, 42), parsed.req.identity_read_generation);
}

test "api query contract maps timeout_ms to execution deadline" {
    const alloc = std.testing.allocator;
    const before_ns = platform_time.monotonicNs();
    var parsed = try parseQueryRequest(alloc, null, "docs",
        \\{"query":{"match_all":{}},"timeout_ms":250}
    );
    defer parsed.deinit(alloc);
    const after_ns = platform_time.monotonicNs();

    const deadline_ns = parsed.req.execution_deadline_ns orelse return error.TestExpectedDeadline;
    const deadline_origin_ns = deadline_ns - 250 * std.time.ns_per_ms;
    try std.testing.expect(deadline_origin_ns >= before_ns);
    try std.testing.expect(deadline_origin_ns <= after_ns);
}

test "api query contract applies timeout_ms with an escaped member name" {
    const alloc = std.testing.allocator;
    const before_ns = platform_time.monotonicNs();
    var parsed = try parseQueryRequest(alloc, null, "docs",
        \\{"query":{"match_all":{}},"t\u0069meout_ms":60000}
    );
    defer parsed.deinit(alloc);
    const after_ns = platform_time.monotonicNs();

    const deadline_ns = parsed.req.execution_deadline_ns orelse return error.TestExpectedDeadline;
    try std.testing.expect(deadline_ns >= before_ns + 60_000 * std.time.ns_per_ms);
    try std.testing.expect(deadline_ns <= after_ns + 60_000 * std.time.ns_per_ms);
}

test "api query contract rejects invalid timeout_ms" {
    try std.testing.expectError(error.InvalidQueryRequest, parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"query":{"match_all":{}},"timeout_ms":-1}
    ));
    try std.testing.expectError(error.InvalidQueryRequest, parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"query":{"match_all":{}},"timeout_ms":"bad"}
    ));
    try std.testing.expectError(error.InvalidQueryRequest, parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"query":{"match_all":{}},"timeout_ms":1.5}
    ));
}

test "api query contract rejects legacy native doc id constraint fields" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "query": {"match_all": {}},
        \\  "_filter_doc_ids_positive": true
        \\}
    ;

    try std.testing.expectError(error.InvalidQueryRequest, parseQueryRequest(alloc, null, "docs", body));

    const old_arrays =
        \\{
        \\  "query": {"match_all": {}},
        \\  "_filter_doc_ids": ["doc:a"],
        \\  "_exclude_doc_ids": ["doc:b"]
        \\}
    ;
    try std.testing.expectError(error.InvalidQueryRequest, parseQueryRequest(alloc, null, "docs", old_arrays));
}
