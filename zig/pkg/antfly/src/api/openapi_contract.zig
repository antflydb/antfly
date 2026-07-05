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

pub const generated = @import("antfly_public_openapi");
pub const client_generated = @import("antfly_client_openapi");
pub const schema_generated = @import("antfly_schema_openapi");
pub const indexes_generated = @import("antfly_indexes_openapi");
pub const generating_api_generated = @import("antfly_generating_api_openapi");
pub const eval_generated = @import("antfly_eval_openapi");
pub const query_generated = @import("antfly_query_openapi");
pub const admin_generated = @import("antfly_admin_openapi");
pub const admin_facade = @import("../admin/mod.zig");
pub const internal_generated = @import("antfly_internal_openapi");
pub const metadata_generated = @import("antfly_metadata_openapi");
pub const usermgr_generated = @import("antfly_usermgr_openapi");
pub const chunking_generated = @import("antfly_chunking_openapi");
pub const embeddings_generated = @import("antfly_embeddings_openapi");
pub const common_generated = @import("antfly_common_openapi");
pub const generating_generated = @import("antfly_generating_openapi");
pub const reranking_generated = @import("antfly_reranking_openapi");
const httpx_handler = @import("httpx_handler.zig");

test "public openapi contract module is generated and wired" {
    try std.testing.expect(@hasDecl(generated, "CreateTableRequest"));
    try std.testing.expect(@hasDecl(generated, "Table"));
    try std.testing.expect(@hasDecl(generated, "TableStatus"));
    try std.testing.expect(@hasDecl(generated, "IndexStatus"));
    try std.testing.expect(@hasDecl(generated, "TableMigration"));
    try std.testing.expect(@hasDecl(generated, "QueryRequest"));
    try std.testing.expect(@hasDecl(generated, "BackupRequest"));
    try std.testing.expect(@hasDecl(generated, "RestoreRequest"));
    try std.testing.expect(@hasDecl(generated, "ClusterBackupRequest"));
    try std.testing.expect(@hasDecl(generated, "ClusterBackupResponse"));
    try std.testing.expect(@hasDecl(generated, "ClusterRestoreRequest"));
    try std.testing.expect(@hasDecl(generated, "ClusterRestoreResponse"));
    try std.testing.expect(@hasDecl(generated, "BackupListResponse"));
}

test "admin openapi contract module is generated and wired" {
    try std.testing.expect(@hasDecl(admin_generated, "ReplicationSlotCreateRequest"));
    try std.testing.expect(@hasField(admin_generated.ReplicationSlotCreateRequest, "slot_name"));
    try std.testing.expect(@hasField(admin_generated.ReplicationSlotCreateRequest, "initial_lsn"));
    try std.testing.expect(@hasDecl(admin_generated, "BaseBackupStartRequest"));
    try std.testing.expect(@hasField(admin_generated.BaseBackupStartRequest, "manifest_id"));
    try std.testing.expect(@hasDecl(admin_generated, "StandbyBootstrapRequest"));
    try std.testing.expect(@hasField(admin_generated.StandbyBootstrapRequest, "content_root"));
    try std.testing.expect(@hasDecl(admin_generated, "FenceAcquireRequest"));
    try std.testing.expect(@hasField(admin_generated.FenceAcquireRequest, "identity"));
    try std.testing.expect(@hasField(admin_generated.FenceAcquireRequest, "old_primary_id"));
    try std.testing.expect(@hasField(admin_generated.FenceAcquireRequest, "promoted_node_id"));
    try std.testing.expect(@hasDecl(admin_generated, "PromotionAssessRequest"));
    try std.testing.expect(@hasField(admin_generated.PromotionAssessRequest, "use_current_fence"));
    try std.testing.expect(@hasDecl(admin_generated, "HAPromotionResult"));
    try std.testing.expect(@hasField(admin_generated.HAPromotionResult, "node_id"));
    try std.testing.expect(@hasDecl(admin_generated, "HAActionReceipt"));
    try std.testing.expect(@hasField(admin_generated.HAActionReceipt, "action_id"));
    try std.testing.expect(@hasField(admin_generated.HAActionReceipt, "action_kind"));
    try std.testing.expect(@hasField(admin_generated.HAActionReceipt, "target"));
    try std.testing.expect(@hasField(admin_generated.HAActionReceipt, "state"));
    try std.testing.expect(@hasDecl(admin_generated, "HAPromotionResponse"));
    try std.testing.expect(@hasField(admin_generated.HAPromotionResponse, "action"));
    try std.testing.expect(@hasField(admin_generated.HAPromotionResponse, "promotion"));
    try std.testing.expect(@hasDecl(admin_generated, "HAFenceReceipt"));
    try std.testing.expect(@hasField(admin_generated.HAFenceReceipt, "parent_timeline_id"));
    try std.testing.expect(@hasDecl(admin_generated, "RejoinAssessRequest"));
    try std.testing.expect(@hasField(admin_generated.RejoinAssessRequest, "retained_from_lsn"));
    try std.testing.expect(@hasField(admin_generated.RejoinAssessRequest, "receipt"));
    try std.testing.expect(@hasDecl(admin_generated, "HARejoinAssessResponse"));
    try std.testing.expect(@hasField(admin_generated.HARejoinAssessResponse, "action"));
    try std.testing.expect(@hasField(admin_generated.HARejoinAssessResponse, "assessment"));
    try std.testing.expect(@hasField(admin_generated.HARejoinAssessResponse, "rewind"));
    try std.testing.expect(@hasField(admin_generated.HARejoinAssessResponse, "reseed"));
    try std.testing.expect(@hasDecl(admin_generated, "HARejoinRewindResult"));
    try std.testing.expect(@hasField(admin_generated.HARejoinRewindResult, "discarded_lsn_count"));
    try std.testing.expect(@hasDecl(admin_generated, "HARejoinReseedResult"));
    try std.testing.expect(@hasField(admin_generated.HARejoinReseedResult, "base_backup_required"));
    try std.testing.expect(@hasDecl(admin_facade, "HAActionReceipt"));
    try std.testing.expect(@hasDecl(admin_facade, "HARejoinRewindResult"));
    try std.testing.expect(@hasDecl(admin_facade, "HARejoinReseedResult"));
    try std.testing.expect(@hasDecl(admin_generated.server, "PauseHAReplicationSlotPathParams"));
    try std.testing.expect(@hasField(admin_generated.server.PauseHAReplicationSlotPathParams, "slot_name"));
}

test "internal openapi contract module is generated and wired" {
    try std.testing.expect(@hasDecl(internal_generated, "HAIdentifySystemResponse"));
    try std.testing.expect(@hasField(internal_generated.HAIdentifySystemResponse, "identity"));
    try std.testing.expect(@hasField(internal_generated.HAIdentifySystemResponse, "record_format_version"));
    try std.testing.expect(@hasDecl(internal_generated, "HACreateReplicationSlotRequest"));
    try std.testing.expect(@hasField(internal_generated.HACreateReplicationSlotRequest, "slot_name"));
    try std.testing.expect(@hasDecl(internal_generated, "HAStartReplicationRequest"));
    try std.testing.expect(@hasField(internal_generated.HAStartReplicationRequest, "from_lsn"));
    try std.testing.expect(@hasDecl(internal_generated, "HAReplicationFrame"));
    try std.testing.expect(@hasField(internal_generated.HAReplicationFrame, "encoded"));
    try std.testing.expect(@hasDecl(internal_generated, "HAStandbyStatusUpdateRequest"));
    try std.testing.expect(@hasField(internal_generated.HAStandbyStatusUpdateRequest, "safe_read_lsn"));
    try std.testing.expect(@hasDecl(internal_generated.server, "ServerRouter"));
}

test "public table contract exposes migration metadata" {
    try std.testing.expect(@hasField(generated.Table, "migration"));
    try std.testing.expect(@hasField(generated.TableMigration, "state"));
    try std.testing.expect(@hasField(generated.TableMigration, "read_schema"));
}

test "public index contract exposes runtime status metadata" {
    try std.testing.expect(@hasField(generated.IndexStatus, "config"));
    try std.testing.expect(@hasField(generated.IndexStatus, "status"));
    try std.testing.expect(@hasField(generated.IndexStatus, "shard_status"));
    try std.testing.expect(@hasField(generated.TableStatus, "artifact_enrichments"));
    try std.testing.expect(@hasField(indexes_generated.EnrichmentConfig, "full_text_index"));
    try std.testing.expect(@hasField(indexes_generated.FullTextIndexStats, "index_type"));
    try std.testing.expect(@hasField(indexes_generated.FullTextIndexStats, "rebuilding"));
    try std.testing.expect(@hasField(indexes_generated.FullTextIndexStats, "total_indexed"));
    try std.testing.expect(@hasField(indexes_generated.EmbeddingsIndexStats, "index_type"));
    try std.testing.expect(@hasField(indexes_generated.GraphIndexStats, "index_type"));
    try std.testing.expect(@hasDecl(indexes_generated, "GraphMetricRuntimeStats"));
    try std.testing.expect(@hasField(indexes_generated.GraphIndexStats, "graph_metric_runtime"));
    try std.testing.expect(@hasField(indexes_generated.GraphMetricRuntimeStats, "owner_id_hash"));
    try std.testing.expect(@hasField(indexes_generated.GraphMetricRuntimeStats, "worker_count"));
    try std.testing.expect(@hasField(indexes_generated.GraphMetricRuntimeStats, "takeover_count"));
    try std.testing.expect(@hasField(indexes_generated.GraphMetricRuntimeStats, "lost_leases"));
    try std.testing.expect(@hasField(indexes_generated.GraphMetricRuntimeStats, "total_pages_claimed"));
    try std.testing.expect(@hasField(indexes_generated.GraphMetricRuntimeStats, "last_pages_completed"));
    try std.testing.expect(@hasDecl(client_generated, "GraphMetricRuntimeStats"));
    try std.testing.expect(@hasField(client_generated.GraphIndexStats, "graph_metric_runtime"));
    try std.testing.expect(@hasField(client_generated.GraphMetricRuntimeStats, "owner_id_hash"));
    try std.testing.expect(@hasDecl(indexes_generated, "AlgebraicIndexStats"));
    try std.testing.expect(@hasField(indexes_generated.AlgebraicIndexStats, "index_type"));
    try std.testing.expect(@hasField(indexes_generated.AlgebraicIndexStats, "healthy"));
    try std.testing.expect(@hasField(indexes_generated.AlgebraicIndexStats, "capability_lifecycle_status"));
    try std.testing.expect(@hasField(indexes_generated.AlgebraicIndexStats, "planner_last_decision"));
    try std.testing.expect(@hasField(indexes_generated.AlgebraicIndexStats, "planner_last_fallback_reason"));
    try std.testing.expect(@hasField(indexes_generated.AlgebraicIndexStats, "planner_last_estimated_scan_rows"));
    try std.testing.expect(@hasField(indexes_generated.AlgebraicIndexStats, "planner_last_estimated_result_buckets"));
    try std.testing.expect(@hasField(indexes_generated.AlgebraicIndexStats, "planner_lifecycle_ready"));
    try std.testing.expect(@hasField(indexes_generated.AlgebraicIndexStats, "planner_lifecycle_blocking_reason"));
    try std.testing.expect(@hasField(indexes_generated.GraphIndexStats, "algebraic_graph"));
    try std.testing.expect(@hasField(indexes_generated.AlgebraicIndexStats, "recommendation_count"));
    try std.testing.expect(@hasField(indexes_generated.AlgebraicIndexStats, "adaptive_backfilling_count"));
    try std.testing.expect(@hasField(indexes_generated.AlgebraicIndexStats, "adaptive_ready_count"));
    try std.testing.expect(@hasField(indexes_generated.AlgebraicIndexStats, "adaptive_stale_count"));
    try std.testing.expect(@hasField(indexes_generated.AlgebraicIndexStats, "adaptive_cleanup_recommended_count"));
    try std.testing.expect(@hasField(indexes_generated.AlgebraicIndexStats, "active_progress_lifecycle"));
    try std.testing.expect(@hasField(indexes_generated.AlgebraicIndexStats, "active_progress_rows_processed"));
    try std.testing.expect(@hasDecl(indexes_generated, "RelationalIndexStats"));
    try std.testing.expect(@hasField(indexes_generated.RelationalIndexStats, "index_type"));
    try std.testing.expect(@hasField(indexes_generated.RelationalIndexStats, "rebuild"));
    try std.testing.expect(@hasField(indexes_generated.RelationalIndexStats, "repair"));
    try std.testing.expect(@hasDecl(client_generated, "RelationalIndexStats"));
    try std.testing.expect(@hasField(client_generated.RelationalIndexStats, "repair"));
    try std.testing.expect(!@hasDecl(indexes_generated, "AlgebraicRuntimeHealth"));
    try std.testing.expect(!@hasDecl(indexes_generated, "AlgebraicAdaptiveProgressStatus"));
    try std.testing.expect(!@hasDecl(indexes_generated, "AlgebraicAdaptiveCandidateStatus"));
    try std.testing.expect(!@hasDecl(indexes_generated, "AlgebraicAdaptiveCandidateDecisionStatus"));
    try std.testing.expect(!@hasField(indexes_generated.AlgebraicIndexStats, "dictionary_registry"));
    try std.testing.expect(!@hasField(indexes_generated.AlgebraicIndexStats, "candidate_decision_history"));
    try std.testing.expect(!@hasField(indexes_generated.AlgebraicIndexStats, "top_candidate"));
    try std.testing.expect(!@hasField(indexes_generated.AlgebraicIndexStats, "last_recommended_materialization"));
    try std.testing.expect(!@hasField(indexes_generated.AlgebraicIndexStats, "active_progress_target_sequence"));
    try std.testing.expect(!@hasField(indexes_generated.AlgebraicIndexStats, "active_progress_applied_sequence"));
    try std.testing.expect(!@hasField(indexes_generated.AlgebraicIndexStats, "materialization_id"));
    try std.testing.expect(!@hasField(indexes_generated.AlgebraicIndexStats, "engine_state_id"));
}

test "indexes openapi parses graph metric runtime summary" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(indexes_generated.IndexStats, alloc,
        \\{"index_type":"graph","total_edges":4,"graph_metric_runtime":{"enabled":true,"role":"worker_pool","owner_id_hash":17,"worker_id_hash":23,"worker_count":3,"lease_owned":true,"has_lease":true,"takeover_count":2,"lost_leases":1,"ticks_started":9,"ticks_completed":8,"durable_progress_ticks":7,"total_pages_claimed":6,"total_pages_completed":5,"last_pages_claimed":4,"last_pages_completed":3}}
    , .{ .allocate = .alloc_always, .ignore_unknown_fields = true });
    defer parsed.deinit();

    switch (parsed.value) {
        .graph_index_stats => |stats| {
            const runtime = stats.graph_metric_runtime orelse return error.UnexpectedOpenApiVariant;
            try std.testing.expect(runtime.enabled.?);
            try std.testing.expectEqualStrings("worker_pool", runtime.role.?);
            try std.testing.expectEqual(@as(i64, 17), runtime.owner_id_hash.?);
            try std.testing.expectEqual(@as(i64, 23), runtime.worker_id_hash.?);
            try std.testing.expectEqual(@as(i64, 3), runtime.worker_count.?);
            try std.testing.expect(runtime.lease_owned.?);
            try std.testing.expect(runtime.has_lease.?);
            try std.testing.expectEqual(@as(i64, 2), runtime.takeover_count.?);
            try std.testing.expectEqual(@as(i64, 1), runtime.lost_leases.?);
            try std.testing.expectEqual(@as(i64, 6), runtime.total_pages_claimed.?);
            try std.testing.expectEqual(@as(i64, 3), runtime.last_pages_completed.?);
        },
        else => return error.UnexpectedOpenApiVariant,
    }
}

test "client openapi parses graph metric runtime summary" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(client_generated.IndexStats, alloc,
        \\{"index_type":"graph","total_edges":4,"graph_metric_runtime":{"enabled":true,"role":"coordinator","owner_id_hash":99,"worker_count":1,"takeover_count":2,"lost_leases":1,"total_pages_claimed":6,"last_pages_completed":3}}
    , .{ .allocate = .alloc_always, .ignore_unknown_fields = true });
    defer parsed.deinit();

    switch (parsed.value) {
        .graph_index_stats => |stats| {
            const runtime = stats.graph_metric_runtime orelse return error.UnexpectedOpenApiVariant;
            try std.testing.expect(runtime.enabled.?);
            try std.testing.expectEqualStrings("coordinator", runtime.role.?);
            try std.testing.expectEqual(@as(i64, 99), runtime.owner_id_hash.?);
            try std.testing.expectEqual(@as(i64, 1), runtime.worker_count.?);
            try std.testing.expectEqual(@as(i64, 2), runtime.takeover_count.?);
            try std.testing.expectEqual(@as(i64, 1), runtime.lost_leases.?);
            try std.testing.expectEqual(@as(i64, 6), runtime.total_pages_claimed.?);
            try std.testing.expectEqual(@as(i64, 3), runtime.last_pages_completed.?);
        },
        else => return error.UnexpectedOpenApiVariant,
    }
}

test "indexes openapi parses algebraic status as algebraic stats" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(indexes_generated.IndexStats, alloc,
        \\{"index_type":"algebraic","total_indexed":3,"healthy":true,"parse_error_count":0,"planner_last_decision":"fallback","planner_last_fallback_reason":"no_materialization","planner_last_estimated_scan_rows":61,"planner_last_estimated_result_buckets":8,"planner_lifecycle_ready":false,"planner_lifecycle_blocking_reason":"capability_lifecycle_not_ready","capability_lifecycle_status":"stale","recommendation_count":4,"adaptive_backfilling_count":1,"adaptive_ready_count":2,"adaptive_stale_count":0,"adaptive_cleanup_recommended_count":1,"active_progress_lifecycle":"backfilling","active_progress_rows_processed":7,"active_progress_target_rows":14}
    , .{ .allocate = .alloc_always, .ignore_unknown_fields = true });
    defer parsed.deinit();

    switch (parsed.value) {
        .algebraic_index_stats => |stats| {
            try std.testing.expectEqual(indexes_generated.AlgebraicIndexStatsIndexType.algebraic, stats.index_type);
            try std.testing.expectEqual(@as(i64, 3), stats.total_indexed.?);
            try std.testing.expect(stats.healthy.?);
            try std.testing.expectEqualStrings("fallback", stats.planner_last_decision.?);
            try std.testing.expectEqualStrings("no_materialization", stats.planner_last_fallback_reason.?);
            try std.testing.expectEqual(@as(i64, 61), stats.planner_last_estimated_scan_rows.?);
            try std.testing.expectEqual(@as(i64, 8), stats.planner_last_estimated_result_buckets.?);
            try std.testing.expect(!stats.planner_lifecycle_ready.?);
            try std.testing.expectEqualStrings("capability_lifecycle_not_ready", stats.planner_lifecycle_blocking_reason.?);
            try std.testing.expectEqualStrings("stale", stats.capability_lifecycle_status.?);
            try std.testing.expectEqual(@as(i64, 4), stats.recommendation_count.?);
            try std.testing.expectEqual(@as(i64, 1), stats.adaptive_backfilling_count.?);
            try std.testing.expectEqual(@as(i64, 2), stats.adaptive_ready_count.?);
            try std.testing.expectEqual(@as(i64, 1), stats.adaptive_cleanup_recommended_count.?);
            try std.testing.expectEqualStrings("backfilling", stats.active_progress_lifecycle.?);
            try std.testing.expectEqual(@as(i64, 7), stats.active_progress_rows_processed.?);
        },
        else => return error.UnexpectedOpenApiVariant,
    }
}

test "indexes openapi parses relational status as relational stats" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(indexes_generated.IndexStats, alloc,
        \\{"index_type":"relational","access_method":"ordered_tuple","lifecycle":"ready","ready":true,"generation":7,"schema_fingerprint":"fp","rebuild":{"range_count":1,"matching_generation_range_count":1},"repair":{"job_count":1,"active_job_count":0,"completed_job_count":1,"total_ranges_repaired":1,"aggregate_report":{"scanned_rows":3,"indexed_rows":3,"deleted_orphan_entries":0,"written_entries":1},"latest":{"job_id":"repair:1","status":"complete","worker_id":"worker:a","updated_at_ns":10,"last_report":{"scanned_rows":3}}}}
    , .{ .allocate = .alloc_always, .ignore_unknown_fields = true });
    defer parsed.deinit();

    switch (parsed.value) {
        .relational_index_stats => |stats| {
            try std.testing.expectEqual(indexes_generated.RelationalIndexStatsIndexType.relational, stats.index_type);
            try std.testing.expectEqualStrings("ordered_tuple", stats.access_method.?);
            try std.testing.expectEqual(@as(?i64, 1), stats.repair.?.job_count);
            try std.testing.expectEqual(@as(?i64, 1), stats.repair.?.aggregate_report.?.written_entries);
        },
        else => return error.UnexpectedOpenApiVariant,
    }
}

test "indexes openapi concrete stats require discriminator" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(error.MissingField, std.json.parseFromSlice(indexes_generated.FullTextIndexStats, alloc,
        \\{"total_indexed":3}
    , .{ .allocate = .alloc_always, .ignore_unknown_fields = true }));
}

test "indexes openapi concrete stats reject wrong discriminator" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(error.UnexpectedToken, std.json.parseFromSlice(indexes_generated.FullTextIndexStats, alloc,
        \\{"index_type":"graph","total_indexed":3}
    , .{ .allocate = .alloc_always, .ignore_unknown_fields = true }));
}

test "indexes openapi rejects stats without discriminator" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(error.MissingField, std.json.parseFromSlice(indexes_generated.IndexStats, alloc,
        \\{"total_indexed":3,"healthy":true}
    , .{ .allocate = .alloc_always, .ignore_unknown_fields = true }));
    try std.testing.expectError(error.UnexpectedToken, std.json.parseFromSlice(indexes_generated.IndexStats, alloc,
        \\{"index_type":"unknown","total_indexed":3,"healthy":true}
    , .{ .allocate = .alloc_always, .ignore_unknown_fields = true }));
}

test "generated extractors: path param structs exist" {
    const server = generated.server;
    try std.testing.expect(@hasField(server.GetNamespaceTablePathParams, "database_name"));
    try std.testing.expect(@hasField(server.GetNamespaceTablePathParams, "namespace_name"));
    try std.testing.expect(@hasField(server.GetNamespaceTablePathParams, "table_name"));
    try std.testing.expect(@hasField(server.QueryNamespaceTablePathParams, "database_name"));
    try std.testing.expect(@hasField(server.QueryNamespaceTablePathParams, "namespace_name"));
    try std.testing.expect(@hasField(server.QueryNamespaceTablePathParams, "table_name"));
    try std.testing.expect(@hasField(server.LookupNamespaceTableDocumentPathParams, "key"));
    try std.testing.expect(@hasField(server.GetNamespaceTableIndexPathParams, "index_name"));
    try std.testing.expect(@hasField(server.GetTablePathParams, "table_name"));
    try std.testing.expect(@hasField(server.CreateTablePathParams, "table_name"));
    try std.testing.expect(@hasField(server.LookupKeyPathParams, "table_name"));
    try std.testing.expect(@hasField(server.LookupKeyPathParams, "key"));
    try std.testing.expect(@hasField(server.GetIndexPathParams, "table_name"));
    try std.testing.expect(@hasField(server.GetIndexPathParams, "index_name"));
}

test "generated extractors: route table covers public API" {
    const server = generated.server;
    try std.testing.expect(server.routes.len >= 33);
    var found_get_status = false;
    var found_list_databases = false;
    var found_get_namespace_table = false;
    var found_query_namespace_table = false;
    var found_batch_namespace_table = false;
    var found_rows_batch_namespace_table = false;
    var found_repair_namespace_table_relational_column_backed_index = false;
    var found_get_namespace_table_relational_index_repair_job = false;
    var found_backup_namespace_table = false;
    var found_restore_namespace_table = false;
    var found_lookup_namespace_table_document = false;
    var found_list_namespace_table_indexes = false;
    var found_get_namespace_table_index = false;
    var found_create_namespace_table_index = false;
    var found_drop_namespace_table_index = false;
    var found_create_table = false;
    var found_lookup_key = false;
    var found_batch_write = false;
    var found_repair_relational_column_backed_index = false;
    var found_get_relational_index_repair_job = false;
    var found_query_builder = false;
    var found_eval = false;
    var found_list_row_filters = false;
    var found_get_row_filter = false;
    var found_set_row_filter = false;
    var found_remove_row_filter = false;
    for (server.routes) |route| {
        if (std.mem.eql(u8, route.operation_id, "getStatus")) found_get_status = true;
        if (std.mem.eql(u8, route.operation_id, "listDatabases")) found_list_databases = true;
        if (std.mem.eql(u8, route.operation_id, "getNamespaceTable")) found_get_namespace_table = true;
        if (std.mem.eql(u8, route.operation_id, "queryNamespaceTable")) found_query_namespace_table = true;
        if (std.mem.eql(u8, route.operation_id, "batchNamespaceTable")) found_batch_namespace_table = true;
        if (std.mem.eql(u8, route.operation_id, "rowsBatchNamespaceTable")) found_rows_batch_namespace_table = true;
        if (std.mem.eql(u8, route.operation_id, "repairNamespaceTableRelationalColumnBackedIndex")) found_repair_namespace_table_relational_column_backed_index = true;
        if (std.mem.eql(u8, route.operation_id, "getNamespaceTableRelationalIndexRepairJob")) found_get_namespace_table_relational_index_repair_job = true;
        if (std.mem.eql(u8, route.operation_id, "backupNamespaceTable")) found_backup_namespace_table = true;
        if (std.mem.eql(u8, route.operation_id, "restoreNamespaceTable")) found_restore_namespace_table = true;
        if (std.mem.eql(u8, route.operation_id, "lookupNamespaceTableDocument")) found_lookup_namespace_table_document = true;
        if (std.mem.eql(u8, route.operation_id, "listNamespaceTableIndexes")) found_list_namespace_table_indexes = true;
        if (std.mem.eql(u8, route.operation_id, "getNamespaceTableIndex")) found_get_namespace_table_index = true;
        if (std.mem.eql(u8, route.operation_id, "createNamespaceTableIndex")) found_create_namespace_table_index = true;
        if (std.mem.eql(u8, route.operation_id, "dropNamespaceTableIndex")) found_drop_namespace_table_index = true;
        if (std.mem.eql(u8, route.operation_id, "createTable")) found_create_table = true;
        if (std.mem.eql(u8, route.operation_id, "lookupKey")) found_lookup_key = true;
        if (std.mem.eql(u8, route.operation_id, "batchWrite")) found_batch_write = true;
        if (std.mem.eql(u8, route.operation_id, "repairRelationalColumnBackedIndex")) found_repair_relational_column_backed_index = true;
        if (std.mem.eql(u8, route.operation_id, "getRelationalIndexRepairJob")) found_get_relational_index_repair_job = true;
        if (std.mem.eql(u8, route.operation_id, "queryBuilderAgent")) found_query_builder = true;
        if (std.mem.eql(u8, route.operation_id, "evaluate")) found_eval = true;
        if (std.mem.eql(u8, route.operation_id, "listRowFilters")) found_list_row_filters = true;
        if (std.mem.eql(u8, route.operation_id, "getRowFilter")) found_get_row_filter = true;
        if (std.mem.eql(u8, route.operation_id, "setRowFilter")) found_set_row_filter = true;
        if (std.mem.eql(u8, route.operation_id, "removeRowFilter")) found_remove_row_filter = true;
    }
    try std.testing.expect(found_get_status);
    try std.testing.expect(found_list_databases);
    try std.testing.expect(found_get_namespace_table);
    try std.testing.expect(found_query_namespace_table);
    try std.testing.expect(found_batch_namespace_table);
    try std.testing.expect(found_rows_batch_namespace_table);
    try std.testing.expect(found_repair_namespace_table_relational_column_backed_index);
    try std.testing.expect(found_get_namespace_table_relational_index_repair_job);
    try std.testing.expect(found_backup_namespace_table);
    try std.testing.expect(found_restore_namespace_table);
    try std.testing.expect(found_lookup_namespace_table_document);
    try std.testing.expect(found_list_namespace_table_indexes);
    try std.testing.expect(found_get_namespace_table_index);
    try std.testing.expect(found_create_namespace_table_index);
    try std.testing.expect(found_drop_namespace_table_index);
    try std.testing.expect(found_create_table);
    try std.testing.expect(found_lookup_key);
    try std.testing.expect(found_batch_write);
    try std.testing.expect(found_repair_relational_column_backed_index);
    try std.testing.expect(found_get_relational_index_repair_job);
    try std.testing.expect(found_query_builder);
    try std.testing.expect(found_eval);
    try std.testing.expect(found_list_row_filters);
    try std.testing.expect(found_get_row_filter);
    try std.testing.expect(found_set_row_filter);
    try std.testing.expect(found_remove_row_filter);
}

test "metadata generated router accepts AntflyApiHandler implementation" {
    _ = metadata_generated.server.ServerRouter(httpx_handler.AntflyApiHandler);
}

test "bleve and metadata openapi modules are generated and wired" {
    try std.testing.expect(@hasDecl(query_generated, "Query"));
    try std.testing.expect(@hasDecl(query_generated, "BooleanQuery"));
    try std.testing.expect(@hasDecl(query_generated, "TermQuery"));
    try std.testing.expect(@hasDecl(metadata_generated, "TableStatus"));
    try std.testing.expect(@hasDecl(metadata_generated, "IndexStatus"));
    try std.testing.expect(@hasDecl(metadata_generated, "BatchResponse"));
    try std.testing.expect(@hasDecl(metadata_generated, "QueryRequest"));
    try std.testing.expect(@hasDecl(metadata_generated, "QueryResponses"));
    try std.testing.expect(@hasDecl(metadata_generated, "QueryBuilderResult"));
    try std.testing.expect(@hasDecl(metadata_generated, "SecretStoreStatus"));
    try std.testing.expect(@hasDecl(metadata_generated, "SecretEntry"));
    try std.testing.expect(@hasDecl(metadata_generated, "SecretList"));
    try std.testing.expect(@hasDecl(metadata_generated, "ClusterBackupResponse"));
    try std.testing.expect(@hasDecl(metadata_generated, "ClusterRestoreResponse"));
    try std.testing.expect(@hasDecl(metadata_generated, "BackupListResponse"));
}

test "schema and indexes openapi modules are generated and wired" {
    try std.testing.expect(@hasDecl(schema_generated, "TableSchema"));
    try std.testing.expect(@hasDecl(schema_generated, "AntflyType"));
    try std.testing.expect(@hasDecl(indexes_generated, "IndexConfig"));
    try std.testing.expect(@hasDecl(indexes_generated, "EmbeddingsIndexConfig"));
    try std.testing.expect(@hasDecl(indexes_generated, "SortField"));
    try std.testing.expect(@hasDecl(generating_api_generated, "GenerationStepConfig"));
    try std.testing.expect(@hasDecl(generating_api_generated, "ClassificationTransformationResult"));
    try std.testing.expect(@hasDecl(eval_generated, "EvalConfig"));
    try std.testing.expect(@hasDecl(eval_generated, "EvalResult"));
    try std.testing.expect(@hasDecl(eval_generated, "EvaluatorName"));
}

test "usermgr openapi module is generated and wired" {
    try std.testing.expect(@hasDecl(usermgr_generated, "CreateUserRequest"));
    try std.testing.expect(@hasDecl(usermgr_generated, "UpdatePasswordRequest"));
    try std.testing.expect(@hasDecl(usermgr_generated, "ApiKeyWithSecret"));
    try std.testing.expect(@hasDecl(usermgr_generated, "RowFilterEntry"));
    try std.testing.expect(@hasDecl(usermgr_generated, "Permission"));
    try std.testing.expect(@hasDecl(usermgr_generated, "RoleAssignment"));
    try std.testing.expect(@hasDecl(usermgr_generated, "AuthSubject"));
}

test "usermgr openapi module generates extractor surface for routed endpoints" {
    const server = usermgr_generated.server;
    try std.testing.expect(@hasDecl(usermgr_generated, "server"));
    try std.testing.expect(@hasDecl(server, "parseCreateUserBody"));
    try std.testing.expect(@hasDecl(server, "parseUpdateUserPasswordBody"));
    try std.testing.expect(@hasDecl(server, "parseAddPermissionToUserBody"));
    try std.testing.expect(@hasDecl(server, "parseAddRoleToUserBody"));
    try std.testing.expect(@hasDecl(server, "parseCreateApiKeyBody"));
    try std.testing.expect(@hasDecl(server, "parseSetRowFilterBody"));
    try std.testing.expect(@hasDecl(server, "parseSetSubjectRowFilterBody"));
    try std.testing.expect(@hasField(server.CreateUserPathParams, "user_name"));
    try std.testing.expect(@hasField(server.GetUserByNamePathParams, "user_name"));
    try std.testing.expect(@hasField(server.DeleteUserPathParams, "user_name"));
    try std.testing.expect(@hasField(server.UpdateUserPasswordPathParams, "user_name"));
    try std.testing.expect(@hasField(server.GetUserPermissionsPathParams, "user_name"));
    try std.testing.expect(@hasField(server.RemovePermissionFromUserParams, "resource"));
    try std.testing.expect(@hasField(server.RemovePermissionFromUserParams, "resource_type"));
    try std.testing.expect(@hasField(server.ListUserRolesPathParams, "user_name"));
    try std.testing.expect(@hasField(server.RemoveRoleFromUserParams, "role"));
    try std.testing.expect(@hasField(server.ListRowFiltersPathParams, "user_name"));
    try std.testing.expect(@hasField(server.ListSubjectRowFiltersPathParams, "subject"));
    try std.testing.expect(@hasField(server.GetSubjectRowFilterPathParams, "table"));
    try std.testing.expect(@hasField(server.DeleteApiKeyPathParams, "key_id"));
    try std.testing.expect(@hasField(server.ListApiKeysPathParams, "user_name"));
    try std.testing.expect(@hasField(server.GetRowFilterPathParams, "table"));

    var found_get_current_user = false;
    var found_list_users = false;
    var found_create_user = false;
    var found_get_user_by_name = false;
    var found_delete_user = false;
    var found_update_password = false;
    var found_get_permissions = false;
    var found_add_permission = false;
    var found_remove_permission = false;
    var found_list_user_roles = false;
    var found_add_role_to_user = false;
    var found_remove_role_from_user = false;
    var found_list_auth_subjects = false;
    var found_list_row_filters = false;
    var found_get_row_filter = false;
    var found_set_row_filter = false;
    var found_remove_row_filter = false;
    var found_list_subject_row_filters = false;
    var found_get_subject_row_filter = false;
    var found_set_subject_row_filter = false;
    var found_remove_subject_row_filter = false;
    var found_list_api_keys = false;
    var found_create_api_key = false;
    var found_delete_api_key = false;
    for (server.routes) |route| {
        if (std.mem.eql(u8, route.operation_id, "getCurrentUser")) found_get_current_user = true;
        if (std.mem.eql(u8, route.operation_id, "listUsers")) found_list_users = true;
        if (std.mem.eql(u8, route.operation_id, "createUser")) found_create_user = true;
        if (std.mem.eql(u8, route.operation_id, "getUserByName")) found_get_user_by_name = true;
        if (std.mem.eql(u8, route.operation_id, "deleteUser")) found_delete_user = true;
        if (std.mem.eql(u8, route.operation_id, "updateUserPassword")) found_update_password = true;
        if (std.mem.eql(u8, route.operation_id, "getUserPermissions")) found_get_permissions = true;
        if (std.mem.eql(u8, route.operation_id, "addPermissionToUser")) found_add_permission = true;
        if (std.mem.eql(u8, route.operation_id, "removePermissionFromUser")) found_remove_permission = true;
        if (std.mem.eql(u8, route.operation_id, "listUserRoles")) found_list_user_roles = true;
        if (std.mem.eql(u8, route.operation_id, "addRoleToUser")) found_add_role_to_user = true;
        if (std.mem.eql(u8, route.operation_id, "removeRoleFromUser")) found_remove_role_from_user = true;
        if (std.mem.eql(u8, route.operation_id, "listAuthSubjects")) found_list_auth_subjects = true;
        if (std.mem.eql(u8, route.operation_id, "listRowFilters")) found_list_row_filters = true;
        if (std.mem.eql(u8, route.operation_id, "getRowFilter")) found_get_row_filter = true;
        if (std.mem.eql(u8, route.operation_id, "setRowFilter")) found_set_row_filter = true;
        if (std.mem.eql(u8, route.operation_id, "removeRowFilter")) found_remove_row_filter = true;
        if (std.mem.eql(u8, route.operation_id, "listSubjectRowFilters")) found_list_subject_row_filters = true;
        if (std.mem.eql(u8, route.operation_id, "getSubjectRowFilter")) found_get_subject_row_filter = true;
        if (std.mem.eql(u8, route.operation_id, "setSubjectRowFilter")) found_set_subject_row_filter = true;
        if (std.mem.eql(u8, route.operation_id, "removeSubjectRowFilter")) found_remove_subject_row_filter = true;
        if (std.mem.eql(u8, route.operation_id, "listApiKeys")) found_list_api_keys = true;
        if (std.mem.eql(u8, route.operation_id, "createApiKey")) found_create_api_key = true;
        if (std.mem.eql(u8, route.operation_id, "deleteApiKey")) found_delete_api_key = true;
    }
    try std.testing.expect(found_get_current_user);
    try std.testing.expect(found_list_users);
    try std.testing.expect(found_create_user);
    try std.testing.expect(found_get_user_by_name);
    try std.testing.expect(found_delete_user);
    try std.testing.expect(found_update_password);
    try std.testing.expect(found_get_permissions);
    try std.testing.expect(found_add_permission);
    try std.testing.expect(found_remove_permission);
    try std.testing.expect(found_list_user_roles);
    try std.testing.expect(found_add_role_to_user);
    try std.testing.expect(found_remove_role_from_user);
    try std.testing.expect(found_list_auth_subjects);
    try std.testing.expect(found_list_row_filters);
    try std.testing.expect(found_get_row_filter);
    try std.testing.expect(found_set_row_filter);
    try std.testing.expect(found_remove_row_filter);
    try std.testing.expect(found_list_subject_row_filters);
    try std.testing.expect(found_get_subject_row_filter);
    try std.testing.expect(found_set_subject_row_filter);
    try std.testing.expect(found_remove_subject_row_filter);
    try std.testing.expect(found_list_api_keys);
    try std.testing.expect(found_create_api_key);
    try std.testing.expect(found_delete_api_key);
}

test "public openapi contract includes row filter user management types" {
    try std.testing.expect(@hasDecl(generated, "RowFilterEntry"));
    try std.testing.expect(@hasField(generated.ApiKey, "row_filter"));
    try std.testing.expect(@hasField(generated.CreateApiKeyRequest, "row_filter"));
}

test "chunking config openapi module is generated and wired" {
    try std.testing.expect(@hasDecl(chunking_generated, "ChunkerConfig"));
    try std.testing.expect(@hasDecl(chunking_generated, "ChunkerProvider"));
    try std.testing.expect(@hasField(chunking_generated.ChunkerConfig, "provider"));
    try std.testing.expect(@hasField(chunking_generated.ChunkerConfig, "store_chunks"));
    try std.testing.expect(@hasField(chunking_generated.ChunkerConfig, "full_text_index"));
}

test "embeddings openapi module is generated and wired" {
    try std.testing.expect(@hasDecl(embeddings_generated, "EmbedderConfig"));
    try std.testing.expect(@hasDecl(embeddings_generated, "EmbedderProvider"));
}

test "common openapi module is generated and wired" {
    try std.testing.expect(@hasDecl(common_generated, "Config"));
    try std.testing.expect(@hasDecl(common_generated, "NamedChainLink"));
    try std.testing.expect(@hasField(common_generated.Config, "generators"));
    try std.testing.expect(@hasField(common_generated.Config, "embedders"));
    try std.testing.expect(@hasField(common_generated.Config, "rerankers"));
    try std.testing.expect(@hasField(common_generated.Config, "chunkers"));
    try std.testing.expect(@hasField(common_generated.Config, "chains"));
}

test "generating and reranking openapi modules are generated and wired" {
    try std.testing.expect(@hasDecl(generating_generated, "GeneratorConfig"));
    try std.testing.expect(@hasDecl(generating_generated, "RetryConfig"));
    try std.testing.expect(@hasDecl(generating_generated, "ChainLink"));
    try std.testing.expect(@hasDecl(reranking_generated, "RerankerConfig"));
    try std.testing.expect(@hasDecl(reranking_generated, "RerankerProvider"));
}

test "public query contract exposes reranker and pruner fields" {
    try std.testing.expect(@hasField(generated.QueryRequest, "reranker"));
    try std.testing.expect(@hasField(generated.QueryRequest, "pruner"));
}

test "query OpenAPI integration generates a recursive query union" {
    try std.testing.expect(@typeInfo(query_generated.Query) == .@"union");
    try std.testing.expect(@hasField(query_generated.Query, "match_query"));
    try std.testing.expect(@hasField(query_generated.Query, "boolean_query"));
}

test "public and metadata query wrappers still keep raw full_text_search payloads" {
    const field_type = @FieldType(metadata_generated.QueryRequest, "full_text_search");
    try std.testing.expect(field_type == ?std.json.Value);
    const public_field_type = @FieldType(generated.QueryRequest, "full_text_search");
    try std.testing.expect(public_field_type == ?std.json.Value);
}

test "metadata openapi module resolves shared refs through owner modules" {
    try std.testing.expect(@FieldType(metadata_generated.QueryBuilderRequest, "generator") == ?generating_generated.GeneratorConfig);
    try std.testing.expect(@hasField(metadata_generated.QueryBuilderRequest, "mode"));
    try std.testing.expect(@hasField(metadata_generated.QueryBuilderRequest, "output"));
    try std.testing.expect(@hasField(metadata_generated.QueryBuilderRequest, "constraints"));
    try std.testing.expect(@FieldType(metadata_generated.QueryBuilderResult, "query_request") == ?metadata_generated.QueryRequest);
    try std.testing.expect(@FieldType(metadata_generated.QueryBuilderResult, "retrieval_query_request") == ?metadata_generated.RetrievalQueryRequest);
    try std.testing.expect(@hasField(metadata_generated.QueryBuilderResult, "specialist"));
    try std.testing.expect(@hasField(metadata_generated.QueryBuilderResult, "plan"));
    try std.testing.expect(@FieldType(metadata_generated.QueryRequest, "reranker") == ?reranking_generated.RerankerConfig);
    try std.testing.expect(@hasField(metadata_generated.QueryRequest, "fields"));
    try std.testing.expect(@hasField(metadata_generated.QueryRequest, "count"));
    try std.testing.expect(@hasField(metadata_generated.QueryRequest, "join"));
    try std.testing.expect(@hasField(metadata_generated.QueryRequest, "foreign_sources"));
    try std.testing.expect(@hasField(metadata_generated.JoinClause, "on"));
    try std.testing.expect(@hasField(metadata_generated.JoinClause, "right_filters"));
    try std.testing.expect(@hasField(metadata_generated.JoinClause, "nested_join"));
}

test "metadata openapi module generates extractor surface for routed endpoints" {
    const server = metadata_generated.server;
    try std.testing.expect(@hasDecl(metadata_generated, "server"));
    try std.testing.expect(@hasDecl(server, "parseRetrievalAgentBody"));
    try std.testing.expect(@hasDecl(server, "parseEvaluateBody"));
    try std.testing.expect(@hasDecl(server, "parseQueryBuilderAgentBody"));
    try std.testing.expect(@hasDecl(server, "parsePutSecretBody"));
    try std.testing.expect(@hasDecl(server, "parseCreateTableBody"));
    try std.testing.expect(@hasDecl(server, "parseUpdateSchemaBody"));
    try std.testing.expect(@hasDecl(server, "parseScanKeysBody"));
    try std.testing.expect(@hasDecl(server, "parseQueryTableBody"));
    try std.testing.expect(@hasDecl(server, "parseBatchWriteBody"));
    try std.testing.expect(@hasDecl(server, "parseBackupBody"));
    try std.testing.expect(@hasDecl(server, "parseRestoreBody"));
    try std.testing.expect(@hasDecl(server, "parseBackupTableBody"));
    try std.testing.expect(@hasDecl(server, "parseRestoreTableBody"));
    try std.testing.expect(@hasField(server.PutSecretPathParams, "key"));
    try std.testing.expect(@hasField(server.DeleteSecretPathParams, "key"));
    try std.testing.expect(@hasField(server.ListBackupsParams, "location"));
    try std.testing.expect(@hasField(server.ListTablesParams, "prefix"));
    try std.testing.expect(@hasField(server.ListTablesParams, "pattern"));
    try std.testing.expect(@hasField(server.GetTablePathParams, "table_name"));
    try std.testing.expect(@hasField(server.QueryTablePathParams, "table_name"));
    try std.testing.expect(@hasField(server.LookupKeyPathParams, "table_name"));
    try std.testing.expect(@hasField(server.LookupKeyPathParams, "key"));
    try std.testing.expect(@hasField(server.GetIndexPathParams, "table_name"));
    try std.testing.expect(@hasField(server.GetIndexPathParams, "index_name"));
    try std.testing.expect(@hasField(server.DropIndexPathParams, "index_name"));
    try std.testing.expect(@hasField(server.ExecuteGraphMetricActionPathParams, "table_name"));
    try std.testing.expect(@hasField(server.ExecuteGraphMetricActionPathParams, "index_name"));
    try std.testing.expect(@hasField(server.ExecuteGraphMetricActionPathParams, "metric_name"));
    try std.testing.expect(@hasField(server.ExecuteGraphMetricActionPathParams, "action"));

    var found_get_status = false;
    var found_list_secrets = false;
    var found_put_secret = false;
    var found_delete_secret = false;
    var found_backup = false;
    var found_restore = false;
    var found_list_backups = false;
    var found_global_query = false;
    var found_eval = false;
    var found_query_builder = false;
    var found_retrieval = false;
    var found_list_tables = false;
    var found_create_table = false;
    var found_drop_table = false;
    var found_get_table = false;
    var found_query_table = false;
    var found_batch_write = false;
    var found_linear_merge = false;
    var found_backup_table = false;
    var found_restore_table = false;
    var found_update_schema = false;
    var found_scan_keys = false;
    var found_lookup_key = false;
    var found_list_indexes = false;
    var found_create_index = false;
    var found_drop_index = false;
    var found_get_index = false;
    var found_execute_graph_metric_action = false;
    var found_put_artifact_enrichment = false;
    var found_delete_artifact_enrichment = false;
    for (server.routes) |route| {
        if (std.mem.eql(u8, route.operation_id, "getStatus")) found_get_status = true;
        if (std.mem.eql(u8, route.operation_id, "listSecrets")) found_list_secrets = true;
        if (std.mem.eql(u8, route.operation_id, "putSecret")) found_put_secret = true;
        if (std.mem.eql(u8, route.operation_id, "deleteSecret")) found_delete_secret = true;
        if (std.mem.eql(u8, route.operation_id, "backup")) found_backup = true;
        if (std.mem.eql(u8, route.operation_id, "restore")) found_restore = true;
        if (std.mem.eql(u8, route.operation_id, "listBackups")) found_list_backups = true;
        if (std.mem.eql(u8, route.operation_id, "globalQuery")) found_global_query = true;
        if (std.mem.eql(u8, route.operation_id, "evaluate")) found_eval = true;
        if (std.mem.eql(u8, route.operation_id, "queryBuilderAgent")) found_query_builder = true;
        if (std.mem.eql(u8, route.operation_id, "retrievalAgent")) found_retrieval = true;
        if (std.mem.eql(u8, route.operation_id, "listTables")) found_list_tables = true;
        if (std.mem.eql(u8, route.operation_id, "createTable")) found_create_table = true;
        if (std.mem.eql(u8, route.operation_id, "dropTable")) found_drop_table = true;
        if (std.mem.eql(u8, route.operation_id, "getTable")) found_get_table = true;
        if (std.mem.eql(u8, route.operation_id, "queryTable")) found_query_table = true;
        if (std.mem.eql(u8, route.operation_id, "batchWrite")) found_batch_write = true;
        if (std.mem.eql(u8, route.operation_id, "linearMerge")) found_linear_merge = true;
        if (std.mem.eql(u8, route.operation_id, "backupTable")) found_backup_table = true;
        if (std.mem.eql(u8, route.operation_id, "restoreTable")) found_restore_table = true;
        if (std.mem.eql(u8, route.operation_id, "updateSchema")) found_update_schema = true;
        if (std.mem.eql(u8, route.operation_id, "scanKeys")) found_scan_keys = true;
        if (std.mem.eql(u8, route.operation_id, "lookupKey")) found_lookup_key = true;
        if (std.mem.eql(u8, route.operation_id, "listIndexes")) found_list_indexes = true;
        if (std.mem.eql(u8, route.operation_id, "createIndex")) found_create_index = true;
        if (std.mem.eql(u8, route.operation_id, "dropIndex")) found_drop_index = true;
        if (std.mem.eql(u8, route.operation_id, "getIndex")) found_get_index = true;
        if (std.mem.eql(u8, route.operation_id, "executeGraphMetricAction")) found_execute_graph_metric_action = true;
        if (std.mem.eql(u8, route.operation_id, "putArtifactEnrichment")) found_put_artifact_enrichment = true;
        if (std.mem.eql(u8, route.operation_id, "deleteArtifactEnrichment")) found_delete_artifact_enrichment = true;
    }
    try std.testing.expect(found_get_status);
    try std.testing.expect(found_list_secrets);
    try std.testing.expect(found_put_secret);
    try std.testing.expect(found_delete_secret);
    try std.testing.expect(found_backup);
    try std.testing.expect(found_restore);
    try std.testing.expect(found_list_backups);
    try std.testing.expect(found_global_query);
    try std.testing.expect(found_eval);
    try std.testing.expect(found_query_builder);
    try std.testing.expect(found_retrieval);
    try std.testing.expect(found_list_tables);
    try std.testing.expect(found_create_table);
    try std.testing.expect(found_drop_table);
    try std.testing.expect(found_get_table);
    try std.testing.expect(found_query_table);
    try std.testing.expect(found_batch_write);
    try std.testing.expect(found_linear_merge);
    try std.testing.expect(found_backup_table);
    try std.testing.expect(found_restore_table);
    try std.testing.expect(found_update_schema);
    try std.testing.expect(found_scan_keys);
    try std.testing.expect(found_lookup_key);
    try std.testing.expect(found_list_indexes);
    try std.testing.expect(found_create_index);
    try std.testing.expect(found_drop_index);
    try std.testing.expect(found_get_index);
    try std.testing.expect(found_execute_graph_metric_action);
    try std.testing.expect(found_put_artifact_enrichment);
    try std.testing.expect(found_delete_artifact_enrichment);
}

test "public chunker config keeps flattened provider-specific fields" {
    try std.testing.expect(@hasField(chunking_generated.ChunkerConfig, "provider"));
    try std.testing.expect(@hasField(chunking_generated.ChunkerConfig, "max_chunks"));
    try std.testing.expect(@hasField(chunking_generated.ChunkerConfig, "threshold"));
    try std.testing.expect(@hasField(chunking_generated.ChunkerConfig, "text"));
    try std.testing.expect(@hasField(chunking_generated.ChunkerConfig, "audio"));
    try std.testing.expect(@hasField(chunking_generated.ChunkerConfig, "api_url"));
    try std.testing.expect(@hasField(chunking_generated.ChunkerConfig, "model"));
    try std.testing.expect(@hasField(chunking_generated.ChunkerConfig, "store_chunks"));
    try std.testing.expect(@hasField(chunking_generated.ChunkerConfig, "full_text_index"));
}

test "public bundled root still exposes foreign-owned shared contract types" {
    try std.testing.expect(@hasDecl(generated, "IndexConfig"));
    try std.testing.expect(@hasDecl(generated, "TableSchema"));
    try std.testing.expect(@hasDecl(generated, "EmbedderConfig"));
    try std.testing.expect(@hasDecl(generated, "GeneratorConfig"));
    try std.testing.expect(@hasDecl(generated, "RerankerConfig"));
    try std.testing.expect(@hasDecl(generated, "ChatMessage"));
    try std.testing.expect(@hasDecl(generated, "EvalConfig"));
    try std.testing.expect(@hasDecl(generated, "WebSearchConfig"));
    try std.testing.expect(@hasDecl(generated, "ExaSearchConfig"));
    try std.testing.expect(@hasDecl(generated, "VertexSearchConfig"));
    try std.testing.expect(@hasDecl(generated, "schemas_AntflyType"));
}

test "public openapi module resolves shared refs through owner modules" {
    try std.testing.expect(@FieldType(generated.CreateTableRequest, "schema") == ?schema_generated.TableSchema);
    try std.testing.expect(@FieldType(generated.Table, "schema") == ?schema_generated.TableSchema);
    try std.testing.expect(@FieldType(generated.QueryRequest, "pruner") == ?indexes_generated.Pruner);
    try std.testing.expect(@FieldType(generated.QueryRequest, "reranker") == ?reranking_generated.RerankerConfig);
    try std.testing.expect(@FieldType(generated.RetrievalAgentRequest, "steps") == ?generated.RetrievalAgentSteps);
    try std.testing.expect(@FieldType(generated.RetrievalAgentSteps, "generation") == ?generating_api_generated.GenerationStepConfig);
    try std.testing.expect(@FieldType(generated.RetrievalAgentSteps, "eval") == ?eval_generated.EvalConfig);
}

test "client openapi module resolves shared refs through owner modules" {
    try std.testing.expect(@hasDecl(client_generated, "Client"));
    try std.testing.expect(@hasDecl(client_generated.Client, "getStatus"));
    try std.testing.expect(@hasDecl(client_generated.Client, "listSecrets"));
    try std.testing.expect(@hasDecl(client_generated.Client, "putSecret"));
    try std.testing.expect(@hasDecl(client_generated.Client, "deleteSecret"));
    try std.testing.expect(@hasDecl(client_generated.Client, "backup"));
    try std.testing.expect(@hasDecl(client_generated.Client, "restore"));
    try std.testing.expect(@hasDecl(client_generated.Client, "listBackups"));
    try std.testing.expect(@hasDecl(client_generated.Client, "listDatabases"));
    try std.testing.expect(@hasDecl(client_generated.Client, "getNamespaceTable"));
    try std.testing.expect(@hasDecl(client_generated.Client, "queryNamespaceTable"));
    try std.testing.expect(@hasDecl(client_generated.Client, "batchNamespaceTable"));
    try std.testing.expect(@hasDecl(client_generated.Client, "rowsBatchNamespaceTable"));
    try std.testing.expect(@hasDecl(client_generated.Client, "repairNamespaceTableRelationalColumnBackedIndex"));
    try std.testing.expect(@hasDecl(client_generated.Client, "getNamespaceTableRelationalIndexRepairJob"));
    try std.testing.expect(@hasDecl(client_generated.Client, "backupNamespaceTable"));
    try std.testing.expect(@hasDecl(client_generated.Client, "restoreNamespaceTable"));
    try std.testing.expect(@hasDecl(client_generated.Client, "lookupNamespaceTableDocument"));
    try std.testing.expect(@hasDecl(client_generated.Client, "listNamespaceTableIndexes"));
    try std.testing.expect(@hasDecl(client_generated.Client, "getNamespaceTableIndex"));
    try std.testing.expect(@hasDecl(client_generated.Client, "createNamespaceTableIndex"));
    try std.testing.expect(@hasDecl(client_generated.Client, "dropNamespaceTableIndex"));
    try std.testing.expect(@hasDecl(client_generated.Client, "listTables"));
    try std.testing.expect(@hasDecl(client_generated.Client, "createTable"));
    try std.testing.expect(@hasDecl(client_generated.Client, "getTable"));
    try std.testing.expect(@hasDecl(client_generated.Client, "lookupKey"));
    try std.testing.expect(@hasDecl(client_generated.Client, "scanKeys"));
    try std.testing.expect(@hasDecl(client_generated.Client, "queryTable"));
    try std.testing.expect(@hasDecl(client_generated.Client, "batchWrite"));
    try std.testing.expect(@hasDecl(client_generated.Client, "repairRelationalColumnBackedIndex"));
    try std.testing.expect(@hasDecl(client_generated.Client, "getRelationalIndexRepairJob"));
    try std.testing.expect(@hasDecl(client_generated.Client, "backupTable"));
    try std.testing.expect(@hasDecl(client_generated.Client, "restoreTable"));
    try std.testing.expect(@hasDecl(client_generated.Client, "updateSchema"));
    try std.testing.expect(@hasDecl(client_generated.Client, "listIndexes"));
    try std.testing.expect(@hasDecl(client_generated.Client, "createIndex"));
    try std.testing.expect(@hasDecl(client_generated.Client, "dropIndex"));
    try std.testing.expect(@hasDecl(client_generated.Client, "getIndex"));
    try std.testing.expect(@hasDecl(client_generated.Client, "executeGraphMetricAction"));
    try std.testing.expect(@FieldType(client_generated.GraphMetricActionResponse, "status") == client_generated.GraphMetricStatus);
    try std.testing.expect(@FieldType(client_generated.GraphMetricStatus, "phase") == []const u8);
    try std.testing.expect(@FieldType(client_generated.GraphMetricStatus, "metadata_version") == ?i64);
    try std.testing.expect(@FieldType(client_generated.GraphMetricStatus, "target_edge_generation") == i64);
    try std.testing.expect(@FieldType(client_generated.GraphMetricStatus, "build_queued") == bool);
    try std.testing.expect(@FieldType(client_generated.GraphMetricStatus, "queued_generation") == ?i64);
    try std.testing.expect(@FieldType(client_generated.GraphMetricStatus, "building_generation") == ?i64);
    try std.testing.expect(@FieldType(client_generated.GraphMetricStatus, "build_job_id") == ?i64);
    try std.testing.expect(@FieldType(client_generated.GraphMetricStatus, "build_started_at_ms") == ?i64);
    try std.testing.expect(@FieldType(client_generated.GraphMetricStatus, "build_iteration") == ?i64);
    try std.testing.expect(@FieldType(client_generated.GraphMetricStatus, "build_lease_expires_at_ms") == ?i64);
    try std.testing.expect(@FieldType(client_generated.GraphMetricStatus, "build_worker_id") == ?[]const u8);
    try std.testing.expect(@FieldType(client_generated.GraphMetricStatus, "build_pages") == ?[]const client_generated.GraphMetricBuildPageStatus);
    try std.testing.expect(@FieldType(client_generated.GraphMetricStatus, "build_pages_truncated") == ?bool);
    try std.testing.expect(@FieldType(client_generated.GraphMetricBuildPageStatus, "phase") == []const u8);
    try std.testing.expect(@FieldType(client_generated.GraphMetricBuildPageStatus, "page_id") == i64);
    try std.testing.expect(@FieldType(client_generated.GraphMetricBuildPageStatus, "state") == []const u8);
    try std.testing.expect(@FieldType(client_generated.GraphMetricBuildPageStatus, "range_kind") == []const u8);
    try std.testing.expect(@FieldType(client_generated.GraphMetricBuildPageStatus, "worker_id") == ?[]const u8);
    try std.testing.expect(@FieldType(client_generated.GraphMetricStatus, "retry_count") == ?i64);
    try std.testing.expect(@FieldType(client_generated.GraphMetricStatus, "last_error") == ?[]const u8);
    try std.testing.expect(@FieldType(client_generated.GraphMetricStatus, "progress") == f64);
    try std.testing.expect(@FieldType(client_generated.GraphMetricStatus, "last_event") == ?client_generated.GraphMetricEvent);
    try std.testing.expect(@FieldType(client_generated.GraphMetricStatus, "recent_events") == ?[]const client_generated.GraphMetricEvent);
    try std.testing.expect(@hasDecl(client_generated.Client, "putArtifactEnrichment"));
    try std.testing.expect(@hasDecl(client_generated.Client, "deleteArtifactEnrichment"));
    try std.testing.expect(@hasDecl(client_generated.Client, "evaluate"));
    try std.testing.expect(@hasDecl(client_generated.Client, "queryBuilderAgent"));
    try std.testing.expect(@hasDecl(client_generated.Client, "retrievalAgent"));
    try std.testing.expect(@hasField(client_generated.TableStatus, "artifact_enrichments"));
    try std.testing.expect(@hasField(client_generated.EnrichmentConfig, "full_text_index"));
    try std.testing.expect(@FieldType(client_generated.CreateTableRequest, "schema") == ?client_generated.TableSchema);
    try std.testing.expect(@FieldType(client_generated.QueryRequest, "pruner") == ?client_generated.Pruner);
    try std.testing.expect(@FieldType(client_generated.QueryRequest, "reranker") == ?client_generated.RerankerConfig);
    try std.testing.expect(@FieldType(client_generated.RetrievalAgentRequest, "steps") == ?client_generated.RetrievalAgentSteps);
    try std.testing.expect(@FieldType(client_generated.RetrievalAgentSteps, "generation") == ?client_generated.GenerationStepConfig);
    try std.testing.expect(@FieldType(client_generated.RetrievalAgentSteps, "eval") == ?client_generated.EvalConfig);
}
