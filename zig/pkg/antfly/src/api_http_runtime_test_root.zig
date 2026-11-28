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

//! Focused discovery root for API HTTP runtime and linked-boundary contracts.
//! Keeping these tests out of the monolithic library root prevents transport
//! changes from forcing code generation for every storage and search test.

const http_server = @import("api/http_server.zig");
const httpx_handler = @import("api/httpx_handler.zig");
const indexes = @import("api/indexes.zig");
const kernel_abi = @import("api/kernel_abi.zig");
const kernel_bridge = @import("api/kernel_bridge.zig");
const kernel_exports = @import("api/kernel_exports.zig");
const openapi_contract = @import("api/openapi_contract.zig");
const runtime_http_abi = @import("antfly_runtime_abi").http_abi;
const runtime_http_bridge = @import("antfly_runtime_abi").http_bridge;
const table_contract = @import("api/table_contract.zig");
const table_read_source = @import("antfly_local_sources").api_table_read_source;

// Some API storage adapters deliberately resolve these declarations through
// the discovery root to avoid production import cycles.
pub const storage_backend_erased = @import("antfly_local_sources").storage_backend_erased;
pub const lsm_backend = @import("antfly_local_sources").storage_lsm_backend;

test {
    _ = @import("api/resolvers.zig");
    _ = @import("api/managed_sources.zig");
    _ = @import("api/protected_future.zig");
    _ = @import("api/sql_graph_overlay_test.zig");
    _ = @import("api/graph_execution.zig");
    _ = @import("api/graph_statement_source.zig");
    _ = @import("api/retained_read_client.zig").consumer_tests;
    _ = @import("api/sql_connection_record.zig");
    _ = @import("api/sql_connections.zig");
    _ = @import("api/kernel_owner_source.zig");
    _ = @import("api/sql_execution.zig");
    _ = @import("api/lake_row_payloads.zig");
    _ = @import("api/lake_expiring_objects.zig");
    _ = @import("api/sql_pgwire.zig");
    _ = @import("api/table_catalog.zig");
    _ = @import("api/public_table_http.zig");
    _ = @import("api/table_reads.zig");
    _ = @import("api/table_reads.zig").document_hydration_tests;
    _ = @import("api/tables.zig");
    _ = @import("antfly_local_sources").storage_db_resolution_runtime;
    _ = @import("api/distributed_candidate_source.zig");
    _ = @import("api/distributed_entity_sink.zig");
    _ = @import("api/http_client.zig");
    _ = @import("api/internal_group_operations.zig");
    _ = @import("antfly_local_sources").api_batch;
    _ = @import("api/distributed_join.zig");
    _ = @import("api/distributed_graph.zig");
    _ = @import("antfly_inference_query_embedding_cache");
    _ = @import("api/agent_tools.zig");
    _ = @import("antfly_local_sources").generating_mod;
    _ = @import("api/query_builder_agent.zig");
    _ = @import("api/retrieval_agent.zig");
    _ = http_server;
    _ = httpx_handler;
    _ = indexes;
    _ = kernel_abi;
    _ = kernel_bridge;
    _ = kernel_exports;
    _ = openapi_contract;
    _ = runtime_http_abi;
    _ = runtime_http_bridge;
    _ = table_contract;
    _ = table_read_source;
}

test "storage artifact inspection carries the reprocess intent across the owner boundary" {
    const std = @import("std");
    const a = std.testing.allocator;
    var manifest = try @import("api/table_reads.zig").parseStorageKernelDocumentArtifactManifestResponse(a,
        \\{"document_id":"doc","artifact_name":"ocr","artifact_id":"asset","manifest_json":"{}","generation":1,"reprocess":{"requested_generation":2,"status":"published","last_retry":{"reason":"provider_retry_after","stage":"ocr","error_code":"ReadRateLimited","retry_at_ms":10000},"worker":{"scanned_sequence":9,"applied_sequence":8,"target_sequence":10,"retry_queue_full":true,"backlog":{"active_tickets":127,"sleeping_tickets":4096,"oldest_unresolved_sequence":9,"admission_blocked_by":"backlog_ticket_limit"},"retrying":true,"last_error_code":"ArtifactPublicationPending","last_progress_at_ms":9000,"failed":false,"background_running":true}}}
    );
    defer manifest.deinit(a);
    try std.testing.expectEqual(@as(u64, 1), manifest.generation);
    try std.testing.expectEqual(@as(u64, 2), manifest.reprocess.?.requested_generation);
    try std.testing.expectEqual(.published, manifest.reprocess.?.status);
    const retry = manifest.reprocess.?.last_retry.?;
    try std.testing.expectEqual(.ocr, retry.stage.?);
    try std.testing.expectEqualStrings("ReadRateLimited", retry.error_code.?.slice());
    const worker = manifest.reprocess.?.worker.?;
    try std.testing.expectEqualStrings("ArtifactPublicationPending", worker.last_error_code.?.slice());
    try std.testing.expectEqual(@as(?u64, 9000), worker.last_progress_at_ms);
    const backlog = worker.backlog.?;
    try std.testing.expectEqual(@as(u32, 4096), backlog.sleeping_tickets);
    try std.testing.expectEqual(.backlog_ticket_limit, backlog.admission_blocked_by.?);
    try std.testing.expectEqual(@as(u64, 10), manifest.reprocess.?.worker.?.target_sequence);
    try std.testing.expect(manifest.reprocess.?.worker.?.next_retry_at_ms == null);
}

/// Implementation source choices for this compilation root.
pub const antfly_sources = @import("source_owner_physical.zig");

test "system catalog routing and transport discovery" {
    _ = @import("api/table_reads.zig").consumer_tests;
    _ = @import("api/table_catalog.zig");
    _ = @import("api/distributed_candidate_source.zig");
    _ = @import("api/distributed_entity_sink.zig");
}

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
test {
    _ = @import("antfly_local_sources").section_source_capture;
    _ = @import("api/lake_sql_cursor.zig");
    _ = @import("api/native_query_repository.zig");
    _ = @import("api/lake_query_metrics.zig");
    _ = @import("api/lake_index_native_text.zig");
    _ = @import("api/lake_index_text_query.zig");
    _ = @import("api/lake_index_ordered_rows.zig");
    _ = @import("api/lake_index_sort_stream.zig");
    _ = @import("serverless/build/lake_build_limits.zig");
    _ = @import("api/lake_index_coordinator.zig");
    _ = @import("api/lake_index_sql_rows.zig");
    _ = @import("api/sql_memory_owner.zig");
    _ = @import("api/lake_artifact_ranges.zig");
    _ = @import("api/lake_index_text_statistics.zig");
    _ = @import("api/lake_index_page_mutations.zig");
    _ = @import("api/lake_row_identity_delta.zig");
    _ = @import("api/lake_index_text_directory.zig");
    _ = @import("api/lake_index_navigation_warming.zig");
    _ = @import("api/lake_index_artifact_recovery.zig");
    _ = @import("api/lake_index_navigation_pages.zig");
    _ = @import("api/lake_source_navigation.zig");
    _ = @import("api/lake_index_seekable_text.zig");
    _ = @import("api/lake_index_text_opening_pack.zig");
}

test {
    _ = @import("api/lake_sql_integration_test.zig");
}

// Schema admission is part of the public lake SQL binding contract.
test "external lake SQL schema cache discovery" {
    _ = @import("api/sql_schema_cache.zig");
}

// Filtered roots must reach the mounted transport tests through a named anchor.
test "api HTTP runtime discovery" {
    _ = httpx_handler;
    _ = @import("api/backup_cohort_driver.zig");
}

test "httpx inference gateway source discovery" {
    _ = @import("api/inference_gateway.zig");
    _ = @import("api/inference_source.zig");
}

// Keep bounded merge storage contracts reachable in focused test builds.
test "linear merge discovery" {
    _ = @import("api/linear_merge.zig");
}

test "index alias object runtime discovery" {
    _ = @import("api/sql_execution.zig");
    _ = @import("api/object_table_runtime.zig");
}
