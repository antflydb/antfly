// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");
const public_openapi_types_source = @embedFile("generated/antfly_public_openapi/types.zig");
const metadata_openapi_types_source = @embedFile("generated/antfly_metadata_openapi/types.zig");
const client_openapi_types_source = @embedFile("generated/antfly_client_openapi/types.zig");

fn expectOpenApiDocumentsToken(token: []const u8) !void {
    try std.testing.expect(std.mem.indexOf(u8, public_openapi_types_source, token) != null);
    try std.testing.expect(std.mem.indexOf(u8, metadata_openapi_types_source, token) != null);
    try std.testing.expect(std.mem.indexOf(u8, client_openapi_types_source, token) != null);
}

pub fn expectPublicOpenApiDocumentsStableExactSortDiagnostics() !void {
    const plan_names = [_][]const u8{
        "`none`",
        "`id_only`",
        "`id_seek`",
        "`sorted_segment_seek`",
        "`native_doc_values_top_n`",
        "`score_top_k`",
        "`distributed_k_way_merge`",
        "`stored_json_debug`",
        "`unsupported_exact_sort`",
    };
    for (plan_names) |plan| try expectOpenApiDocumentsToken(plan);

    const exactness_values = [_][]const u8{
        "`none`",
        "`exact`",
        "`bounded_exact`",
        "`approximate`",
        "`unsupported`",
    };
    for (exactness_values) |value| try expectOpenApiDocumentsToken(value);

    const source_values = [_][]const u8{
        "`candidate_collector`",
        "`primary_key_scan`",
        "`sorted_segment_scan`",
        "`doc_values_collector`",
        "`distributed_merge`",
        "`stored_json_debug`",
        "`unsupported`",
    };
    for (source_values) |value| try expectOpenApiDocumentsToken(value);

    const cursor_support_values = [_][]const u8{
        "`comparator`",
        "`segment_seek`",
        "`distributed_seek`",
        "`unsupported`",
    };
    for (cursor_support_values) |value| try expectOpenApiDocumentsToken(value);

    const source_load_values = [_][]const u8{
        "`source_free`",
        "`projected_source_after_page`",
        "`stored_source_required`",
        "`unsupported`",
    };
    for (source_load_values) |value| try expectOpenApiDocumentsToken(value);

    const distributed_behavior_values = [_][]const u8{
        "`shard_local_only`",
        "`coordinator_merge`",
        "`unsupported`",
    };
    for (distributed_behavior_values) |value| try expectOpenApiDocumentsToken(value);

    const rejection_reasons = [_][]const u8{
        "`unmapped_field`",
        "`non_sortable_field`",
        "`unsupported_sort_field`",
        "`mixed_field_type`",
        "`field_not_sort_ready`",
        "`filter_not_queryable`",
        "`invalid_cursor_arity`",
        "`invalid_cursor_type`",
        "`invalid_sort_tuple`",
        "`approximate_candidate_source`",
        "`candidate_budget_exceeded`",
        "`missing_null_policy`",
        "`non_score_bearing_source`",
        "`invalid_score_value`",
        "`count_only_ordered_page`",
        "`stored_json_sort_disabled`",
        "`unsupported_exact_sort`",
        "`distributed_merge_unsupported`",
    };
    for (rejection_reasons) |reason| try expectOpenApiDocumentsToken(reason);

    const budget_reasons = [_][]const u8{
        "`text_exact_late_visibility_totals`",
        "`text_field_sort_candidate_window`",
        "`match_all_candidate_collect_limit`",
        "`match_all_exact_candidate_window`",
        "`distributed_merge_shard_window`",
    };
    for (budget_reasons) |reason| try expectOpenApiDocumentsToken(reason);

    const selection_reasons = [_][]const u8{
        "`id_candidate_order`",
        "`id_primary_key_seek`",
        "`score_top_k`",
        "`index_sort_sorted_segment_seek`",
        "`sorted_segment_seek`",
        "`doc_values_collector`",
        "`index_sort_unavailable_doc_values_collector`",
        "`caller_selected_doc_values_collector`",
        "`selective_filter_doc_values_collector`",
    };
    for (selection_reasons) |reason| try expectOpenApiDocumentsToken(reason);

    const index_sort_coverage_statuses = [_][]const u8{
        "`request_mismatch`",
        "`no_live_segments`",
        "`missing_segment_index_sort`",
        "`covered_without_bounds`",
        "`covered_with_bounds`",
    };
    for (index_sort_coverage_statuses) |status| try expectOpenApiDocumentsToken(status);

    const rejection_details = [_][]const u8{
        "`unmapped_sort_field`",
        "`unmapped_field`",
        "`non_sortable_sort_field`",
        "`non_scalar_field`",
        "`non_sortable_field`",
        "`mixed_field_type`",
        "`missing_doc_values_coverage`",
        "`missing_doc_values_section`",
        "`malformed_doc_values_section`",
        "`doc_values_kind_mismatch`",
        "`sparse_live_doc_values`",
        "`invalid_doc_value_doc_id`",
        "`duplicate_doc_value_doc_id`",
        "`unsupported_doc_values_type`",
        "`missing_doc_values_capability`",
        "`schema_declared`",
        "`observed_declared`",
        "`not_declared`",
        "`missing_doc_values`",
        "`non_sortable`",
        "`declared`",
        "`text_search_only`",
        "`mixed`",
        "`missing_native_filter_coverage`",
        "`invalid_cursor_arity`",
        "`invalid_cursor_type`",
        "`invalid_sort_tuple`",
        "`sort_tuple_arity`",
        "`invalid_doc_value_type`",
        "`missing_runtime_mapping`",
        "`incomplete_sort_tuple`",
        "`mixed_sort_value_domain`",
        "`unsorted_shard_window`",
        "`unsorted_component_window`",
        "`non_numeric_score`",
        "`missing_score`",
        "`non_finite_score`",
        "`score_sort_tuple_mismatch`",
        "`id_tiebreaker_mismatch`",
        "`native_sort_loader_unavailable`",
        "`sorted_segment_executor_unavailable`",
        "`primary_key_stream_unavailable`",
        "`native_candidate_stream_unavailable`",
        "`candidate_stream_unavailable`",
        "`incompatible_sort_plan`",
        "`sorted_segment_bounds_unavailable`",
        "`filter_query_json_unresolved`",
        "`exclusion_query_json_unresolved`",
        "`text_index_entry_unavailable`",
        "`doc_ordinal_projection_unavailable`",
        "`component_sort_profile_missing`",
        "`unsupported_composed_sort_source`",
        "`distributed_merge_plan_required`",
        "`distributed_shard_window_incomplete`",
        "`distributed_shard_cursor_window_invalid`",
    };
    for (rejection_details) |detail| try expectOpenApiDocumentsToken(detail);
}

test "public openapi documents stable exact sort diagnostics" {
    try expectPublicOpenApiDocumentsStableExactSortDiagnostics();
}
