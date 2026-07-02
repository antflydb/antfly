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
const build_options = @import("build_options");
const platform = @import("antfly_platform");

const algebraic_mod = @import("algebraic/mod.zig");
const artifact_ids = @import("artifact_ids.zig");
const change_journal_mod = @import("derived/change_journal.zig");
const common_secrets = @import("../../common/secrets.zig");
const backend_types = @import("../backend_types.zig");
const asset_producer_mod = @import("enrichment/asset_producer.zig");
const chunk_artifact_mod = @import("../../chunking/chunk.zig");
const chunking_types_mod = @import("../../chunking/types.zig");
const chunker_mod = if (builtin.os.tag == .freestanding or builtin.is_test or build_options.bench_minimal_deps)
    @import("enrichment/chunker_stub.zig")
else
    @import("enrichment/chunker.zig");
const db_internal = @import("internal.zig");
const mapper = @import("document_mapper.zig");
const doc_identity = @import("doc_identity.zig");
const docstore_mod = @import("../docstore.zig");
const derived_types = @import("derived/derived_types.zig");
const document_extraction_mod = @import("enrichment/document_extraction.zig");
const embedder_mod = @import("enrichment/embedder.zig");
const enrichment_artifact_codec = @import("enrichment/artifact_codec.zig");
const enrichment_types = @import("enrichment/enrichment_types.zig");
const enrichment_runtime_mod = @import("enrichment/enrichment_runtime.zig");
const ha_types = @import("ha_types.zig");
const scraping = if (builtin.os.tag == .freestanding or build_options.bench_minimal_deps)
    @import("scraping_stub.zig")
else
    @import("antfly_scraping");
const internal_keys = @import("../internal_keys.zig");
const relational_store_mod = @import("relational_store.zig");
const schema_mod = @import("../schema.zig");
const transform_mod = @import("transform.zig");
const template_mod = if (builtin.os.tag == .freestanding or builtin.is_test or build_options.bench_minimal_deps)
    @import("template_stub.zig")
else
    @import("../../template.zig");
const template_remote = if (builtin.os.tag == .freestanding or builtin.is_test or build_options.bench_minimal_deps)
    @import("template_remote_stub.zig")
else
    @import("../../template_remote.zig");
const transactions_mod = @import("../transactions.zig");
const ttl_mod = @import("../ttl.zig");
const ttl_runtime_mod = @import("maintenance/ttl_runtime.zig");
const types = @import("types.zig");

const Allocator = std.mem.Allocator;
const AtomicU64 = platform.atomic.Value(u64);
const ManagedSyncTargets = db_internal.ManagedSyncTargets;
const BorrowedGraphMaterializationBatch = db_internal.BorrowedGraphMaterializationBatch;
const containsStoreWriteKey = db_internal.containsStoreWriteKey;
const filterChangedGraphMaterializationBatch = db_internal.filterChangedGraphMaterializationBatch;

const TestHelpers = if (builtin.is_test) @import("test_support.zig") else struct {};

pub const BatchProfile = struct {
    total_ns: u64 = 0,
    resolve_transforms_ns: u64 = 0,
    merge_effective_req_ns: u64 = 0,
    predicates_ns: u64 = 0,
    validate_range_ns: u64 = 0,
    extract_writes_ns: u64 = 0,
    extract_vector_field_names_ns: u64 = 0,
    extract_mapper_ns: u64 = 0,
    extract_graph_fields_ns: u64 = 0,
    extract_index_field_embeddings_ns: u64 = 0,
    extract_embedding_artifacts_ns: u64 = 0,
    extract_graph_artifacts_ns: u64 = 0,
    extract_strip_store_value_ns: u64 = 0,
    extract_timestamp_ns: u64 = 0,
    overwrite_probe_ns: u64 = 0,
    delete_artifacts_ns: u64 = 0,
    precompute_generated_ns: u64 = 0,
    identity_capacity_check_ns: u64 = 0,
    identity_metadata_ns: u64 = 0,
    identity_metadata_writes: u64 = 0,
    identity_upsert_keys: u64 = 0,
    identity_delete_keys: u64 = 0,
    store_write_ns: u64 = 0,
    store_write_count: u64 = 0,
    store_delete_count: u64 = 0,
    split_delta_ns: u64 = 0,
    build_derived_ns: u64 = 0,
    apply_shadow_ns: u64 = 0,
    collect_sync_targets_ns: u64 = 0,
    append_replay_journal_ns: u64 = 0,
    wait_sync_ns: u64 = 0,
    backlog_pressure_ns: u64 = 0,
    executor_notify_ns: u64 = 0,
    derived_apply_ns: u64 = 0,
    sync_wait_ns: u64 = 0,
    full_text_apply_ns: u64 = 0,
    dense_apply_ns: u64 = 0,
    dense_delete_ns: u64 = 0,
    dense_doc_index_ns: u64 = 0,
    dense_embedding_apply_ns: u64 = 0,
    sparse_apply_ns: u64 = 0,
    graph_apply_ns: u64 = 0,
    index_sync_ns: u64 = 0,
    applied_sequence_save_ns: u64 = 0,
    replay_journal_truncate_ns: u64 = 0,
    notify_enrichment_ns: u64 = 0,
    hbc_insert_calls: u64 = 0,
    hbc_batch_route_calls: u64 = 0,
    hbc_batch_route_internal_nodes: u64 = 0,
    hbc_batch_route_leaf_groups: u64 = 0,
    hbc_batch_route_items: u64 = 0,
    hbc_batch_route_quantized_nodes: u64 = 0,
    hbc_batch_route_exact_child_scores: u64 = 0,
    hbc_batch_route_fallback_nodes: u64 = 0,
    hbc_grouped_items: u64 = 0,
    hbc_grouped_fallback_items: u64 = 0,
    hbc_grouped_leaf_groups: u64 = 0,
    hbc_grouped_split_candidates: u64 = 0,
    hbc_grouped_recursive_splits: u64 = 0,
    hbc_grouped_split_scan_iterations: u64 = 0,
    hbc_grouped_split_queue_peak_total: u64 = 0,
    hbc_grouped_leaf_range_writes: u64 = 0,
    hbc_grouped_ancestor_range_refreshes: u64 = 0,
    hbc_grouped_ancestor_range_nodes: u64 = 0,
    hbc_grouped_node_body_writes: u64 = 0,
    hbc_grouped_vec_leaf_writes: u64 = 0,
    hbc_split_leaf_input_members_total: u64 = 0,
    hbc_split_leaf_input_overflow_members_total: u64 = 0,
    hbc_save_node_calls: u64 = 0,
    hbc_split_leaf_calls: u64 = 0,
    hbc_split_internal_calls: u64 = 0,
    hbc_range_put_calls: u64 = 0,
    hbc_range_delete_calls: u64 = 0,
    hbc_nodes_put_calls: u64 = 0,
    hbc_nodes_append_calls: u64 = 0,
    hbc_nodes_delete_calls: u64 = 0,
    hbc_meta_put_calls: u64 = 0,
    hbc_meta_append_calls: u64 = 0,
    hbc_meta_delete_calls: u64 = 0,
    hbc_quant_put_calls: u64 = 0,
    hbc_quant_append_calls: u64 = 0,
    hbc_quant_delete_calls: u64 = 0,
    hbc_vecs_put_calls: u64 = 0,
    hbc_vecs_append_calls: u64 = 0,
    hbc_vecs_delete_calls: u64 = 0,
    hbc_insert_transform_ns: u64 = 0,
    hbc_insert_store_vector_ns: u64 = 0,
    hbc_insert_find_leaf_ns: u64 = 0,
    hbc_insert_mutate_leaf_ns: u64 = 0,
    hbc_insert_flush_metadata_ns: u64 = 0,
    hbc_insert_commit_ns: u64 = 0,
    hbc_save_node_ns: u64 = 0,
    hbc_save_split_range_ns: u64 = 0,
    hbc_update_parent_ns: u64 = 0,
    hbc_split_leaf_ns: u64 = 0,
    hbc_split_leaf_vector_load_ns: u64 = 0,
    hbc_split_leaf_partition_ns: u64 = 0,
    hbc_split_leaf_finalize_ns: u64 = 0,
    hbc_split_internal_ns: u64 = 0,
    hbc_refresh_quantized_ns: u64 = 0,
    hbc_quantized_vector_load_ns: u64 = 0,
    hbc_quantized_compute_ns: u64 = 0,
    hbc_quantized_store_ns: u64 = 0,
    hbc_quantized_encode_ns: u64 = 0,
    hbc_quantized_put_ns: u64 = 0,
    hbc_bulk_build_store_ns: u64 = 0,
    hbc_bulk_build_tree_ns: u64 = 0,
    hbc_posting_maintenance_scanned_nodes: u64 = 0,
    hbc_posting_maintenance_scanned_postings: u64 = 0,
    hbc_posting_maintenance_dirty_postings: u64 = 0,
    hbc_posting_maintenance_repaired_postings: u64 = 0,
    hbc_posting_maintenance_centroid_refreshed: u64 = 0,
    hbc_posting_maintenance_payload_refreshed: u64 = 0,
    hbc_posting_maintenance_ancestor_refresh_roots: u64 = 0,
    hbc_posting_maintenance_split_postings: u64 = 0,
    hbc_posting_maintenance_merged_postings: u64 = 0,
    hbc_posting_maintenance_boundary_reassigned_vectors: u64 = 0,
    hbc_posting_lazy_centroid_deferrals: u64 = 0,
    hbc_posting_lazy_payload_deferrals: u64 = 0,
    hbc_posting_lazy_ancestor_deferrals: u64 = 0,
};

pub const DocumentArtifactChildRangeApplyBatch = struct {
    artifact_writes: []const types.BatchWrite = &.{},
    artifact_delete_keys: []const []const u8 = &.{},
    documents: []const derived_types.DerivedDocument = &.{},
    dense_embeddings: []const derived_types.DerivedDenseEmbeddingWrite = &.{},
    sparse_embeddings: []const derived_types.DerivedSparseEmbeddingWrite = &.{},
    generated_enrichment_refs: []const enrichment_types.GeneratedEnrichmentRef = &.{},
    sync_level: types.SyncLevel = .full_index,
};

pub const DocumentArtifactChildRangeDispatch = struct {
    owner_group_id: u64,
    doc_key: []const u8,
    artifact_name: []const u8,
    child_batch: DocumentArtifactChildRangeApplyBatch,
};

pub const DocumentArtifactChildRangeOutboxDrainResult = struct {
    scanned: usize = 0,
    dispatched: usize = 0,
    deleted: usize = 0,
};

pub const DocumentArtifactChildRangeOutboxRecord = struct {
    version: u16 = 1,
    owner_group_id: u64,
    doc_key: []const u8,
    artifact_name: []const u8,
    child_batch: DocumentArtifactChildRangeApplyBatch,
};

pub const DocumentArtifactChildRangeDispatcher = struct {
    ptr: *anyopaque,
    apply: *const fn (ptr: *anyopaque, alloc: Allocator, dispatch: DocumentArtifactChildRangeDispatch) anyerror!void,

    fn applyDispatch(self: DocumentArtifactChildRangeDispatcher, alloc: Allocator, dispatch: DocumentArtifactChildRangeDispatch) !void {
        return try self.apply(self.ptr, alloc, dispatch);
    }
};

pub const BatchExecutionOptions = struct {
    validate_range_ownership: bool = true,
    store_batch_options: backend_types.BatchOptions = .{},
    wait_for_sync_level: bool = true,
    force_generated_artifact_names: []const []const u8 = &.{},
    document_child_range_dispatcher: ?DocumentArtifactChildRangeDispatcher = null,
    admission_prechecked: bool = false,
    bypass_ha_write_gate: bool = false,
    ha_applied_lsn_marker: ?u64 = null,
    suppress_derived_replay_append: bool = false,
};

pub fn logBatchProfile(req: types.BatchRequest, profile: BatchProfile) void {
    std.log.info(
        "antfly_bench_batch writes={d} deletes={d} graph_writes={d} graph_deletes={d} transforms={d} sync={s} total_ms={d} resolve_ms={d} merge_ms={d} predicates_ms={d} range_ms={d} extract_ms={d} delete_artifacts_ms={d} precompute_ms={d} identity_capacity_ms={d} identity_metadata_ms={d} identity_metadata_writes={d} store_ms={d} split_delta_ms={d} build_derived_ms={d} shadow_ms={d} collect_sync_ms={d} append_replay_journal_ms={d} backlog_pressure_ms={d} executor_notify_ms={d} sync_wait_ms={d} wait_sync_ms={d} notify_enrichment_ms={d}",
        .{
            req.writes.len,
            req.deletes.len,
            req.graph_writes.len,
            req.graph_deletes.len,
            req.transforms.len,
            @tagName(req.sync_level),
            nsToMs(profile.total_ns),
            nsToMs(profile.resolve_transforms_ns),
            nsToMs(profile.merge_effective_req_ns),
            nsToMs(profile.predicates_ns),
            nsToMs(profile.validate_range_ns),
            nsToMs(profile.extract_writes_ns),
            nsToMs(profile.delete_artifacts_ns),
            nsToMs(profile.precompute_generated_ns),
            nsToMs(profile.identity_capacity_check_ns),
            nsToMs(profile.identity_metadata_ns),
            profile.identity_metadata_writes,
            nsToMs(profile.store_write_ns),
            nsToMs(profile.split_delta_ns),
            nsToMs(profile.build_derived_ns),
            nsToMs(profile.apply_shadow_ns),
            nsToMs(profile.collect_sync_targets_ns),
            nsToMs(profile.append_replay_journal_ns),
            nsToMs(profile.backlog_pressure_ns),
            nsToMs(profile.executor_notify_ns),
            nsToMs(profile.sync_wait_ns),
            nsToMs(profile.wait_sync_ns),
            nsToMs(profile.notify_enrichment_ns),
        },
    );
    std.log.info(
        "antfly_bench_batch_derived writes={d} total_ms={d} derived_apply_ms={d} full_text_apply_ms={d} dense_apply_ms={d} dense_delete_ms={d} dense_doc_index_ms={d} dense_embedding_apply_ms={d} sparse_apply_ms={d} graph_apply_ms={d} index_sync_ms={d} applied_sequence_save_ms={d} replay_journal_truncate_ms={d}",
        .{
            req.writes.len,
            nsToMs(profile.total_ns),
            nsToMs(profile.derived_apply_ns),
            nsToMs(profile.full_text_apply_ns),
            nsToMs(profile.dense_apply_ns),
            nsToMs(profile.dense_delete_ns),
            nsToMs(profile.dense_doc_index_ns),
            nsToMs(profile.dense_embedding_apply_ns),
            nsToMs(profile.sparse_apply_ns),
            nsToMs(profile.graph_apply_ns),
            nsToMs(profile.index_sync_ns),
            nsToMs(profile.applied_sequence_save_ns),
            nsToMs(profile.replay_journal_truncate_ns),
        },
    );
    std.log.info(
        "antfly_bench_batch_extract writes={d} total_extract_ms={d} vector_field_names_ms={d} mapper_ms={d} graph_fields_ms={d} index_field_embeddings_ms={d} embedding_artifacts_ms={d} graph_artifacts_ms={d} strip_store_value_ms={d} timestamp_ms={d} overwrite_probe_ms={d} identity_upserts={d} identity_deletes={d} store_writes={d} store_deletes={d}",
        .{
            req.writes.len,
            nsToMs(profile.extract_writes_ns),
            nsToMs(profile.extract_vector_field_names_ns),
            nsToMs(profile.extract_mapper_ns),
            nsToMs(profile.extract_graph_fields_ns),
            nsToMs(profile.extract_index_field_embeddings_ns),
            nsToMs(profile.extract_embedding_artifacts_ns),
            nsToMs(profile.extract_graph_artifacts_ns),
            nsToMs(profile.extract_strip_store_value_ns),
            nsToMs(profile.extract_timestamp_ns),
            nsToMs(profile.overwrite_probe_ns),
            profile.identity_upsert_keys,
            profile.identity_delete_keys,
            profile.store_write_count,
            profile.store_delete_count,
        },
    );
    std.log.info(
        "antfly_bench_batch_hbc writes={d} insert_calls={d} batch_route_calls={d} batch_route_internal_nodes={d} batch_route_leaf_groups={d} batch_route_items={d} batch_route_quantized_nodes={d} batch_route_exact_child_scores={d} batch_route_fallback_nodes={d} grouped_items={d} grouped_fallback_items={d} leaf_groups={d} split_candidates={d} recursive_splits={d} split_scan_iterations={d} split_queue_peak_total={d} split_input_members_total={d} split_input_overflow_members_total={d} leaf_range_writes={d} ancestor_range_refreshes={d} ancestor_range_nodes={d} node_body_writes={d} vec_leaf_writes={d} save_node_calls={d} split_leaf_calls={d} split_internal_calls={d} range_put_calls={d} range_delete_calls={d}",
        .{
            req.writes.len,
            profile.hbc_insert_calls,
            profile.hbc_batch_route_calls,
            profile.hbc_batch_route_internal_nodes,
            profile.hbc_batch_route_leaf_groups,
            profile.hbc_batch_route_items,
            profile.hbc_batch_route_quantized_nodes,
            profile.hbc_batch_route_exact_child_scores,
            profile.hbc_batch_route_fallback_nodes,
            profile.hbc_grouped_items,
            profile.hbc_grouped_fallback_items,
            profile.hbc_grouped_leaf_groups,
            profile.hbc_grouped_split_candidates,
            profile.hbc_grouped_recursive_splits,
            profile.hbc_grouped_split_scan_iterations,
            profile.hbc_grouped_split_queue_peak_total,
            profile.hbc_split_leaf_input_members_total,
            profile.hbc_split_leaf_input_overflow_members_total,
            profile.hbc_grouped_leaf_range_writes,
            profile.hbc_grouped_ancestor_range_refreshes,
            profile.hbc_grouped_ancestor_range_nodes,
            profile.hbc_grouped_node_body_writes,
            profile.hbc_grouped_vec_leaf_writes,
            profile.hbc_save_node_calls,
            profile.hbc_split_leaf_calls,
            profile.hbc_split_internal_calls,
            profile.hbc_range_put_calls,
            profile.hbc_range_delete_calls,
        },
    );
    std.log.info(
        "antfly_bench_batch_hbc_storage writes={d} nodes_put_calls={d} nodes_append_calls={d} nodes_delete_calls={d} meta_put_calls={d} meta_append_calls={d} meta_delete_calls={d} quant_put_calls={d} quant_append_calls={d} quant_delete_calls={d} vecs_put_calls={d} vecs_append_calls={d} vecs_delete_calls={d}",
        .{
            req.writes.len,
            profile.hbc_nodes_put_calls,
            profile.hbc_nodes_append_calls,
            profile.hbc_nodes_delete_calls,
            profile.hbc_meta_put_calls,
            profile.hbc_meta_append_calls,
            profile.hbc_meta_delete_calls,
            profile.hbc_quant_put_calls,
            profile.hbc_quant_append_calls,
            profile.hbc_quant_delete_calls,
            profile.hbc_vecs_put_calls,
            profile.hbc_vecs_append_calls,
            profile.hbc_vecs_delete_calls,
        },
    );
    std.log.info(
        "antfly_bench_batch_hbc_timing writes={d} insert_transform_ms={d} insert_store_vector_ms={d} insert_find_leaf_ms={d} insert_mutate_leaf_ms={d} insert_flush_metadata_ms={d} insert_commit_ms={d} save_node_ms={d} save_split_range_ms={d} update_parent_ms={d} split_leaf_ms={d} split_leaf_vector_load_ms={d} split_leaf_partition_ms={d} split_leaf_finalize_ms={d} split_internal_ms={d} refresh_quantized_ms={d} quantized_vector_load_ms={d} quantized_compute_ms={d} quantized_store_ms={d} quantized_encode_ms={d} quantized_put_ms={d} bulk_build_store_ms={d} bulk_build_tree_ms={d}",
        .{
            req.writes.len,
            nsToMs(profile.hbc_insert_transform_ns),
            nsToMs(profile.hbc_insert_store_vector_ns),
            nsToMs(profile.hbc_insert_find_leaf_ns),
            nsToMs(profile.hbc_insert_mutate_leaf_ns),
            nsToMs(profile.hbc_insert_flush_metadata_ns),
            nsToMs(profile.hbc_insert_commit_ns),
            nsToMs(profile.hbc_save_node_ns),
            nsToMs(profile.hbc_save_split_range_ns),
            nsToMs(profile.hbc_update_parent_ns),
            nsToMs(profile.hbc_split_leaf_ns),
            nsToMs(profile.hbc_split_leaf_vector_load_ns),
            nsToMs(profile.hbc_split_leaf_partition_ns),
            nsToMs(profile.hbc_split_leaf_finalize_ns),
            nsToMs(profile.hbc_split_internal_ns),
            nsToMs(profile.hbc_refresh_quantized_ns),
            nsToMs(profile.hbc_quantized_vector_load_ns),
            nsToMs(profile.hbc_quantized_compute_ns),
            nsToMs(profile.hbc_quantized_store_ns),
            nsToMs(profile.hbc_quantized_encode_ns),
            nsToMs(profile.hbc_quantized_put_ns),
            nsToMs(profile.hbc_bulk_build_store_ns),
            nsToMs(profile.hbc_bulk_build_tree_ns),
        },
    );
    std.log.info(
        "antfly_bench_batch_hbc_posting writes={d} maintenance_scanned_nodes={d} maintenance_scanned_postings={d} maintenance_dirty_postings={d} maintenance_repaired_postings={d} maintenance_centroid_refreshed={d} maintenance_payload_refreshed={d} maintenance_ancestor_refresh_roots={d} maintenance_split_postings={d} maintenance_merged_postings={d} maintenance_boundary_reassigned_vectors={d} lazy_centroid_deferrals={d} lazy_payload_deferrals={d} lazy_ancestor_deferrals={d}",
        .{
            req.writes.len,
            profile.hbc_posting_maintenance_scanned_nodes,
            profile.hbc_posting_maintenance_scanned_postings,
            profile.hbc_posting_maintenance_dirty_postings,
            profile.hbc_posting_maintenance_repaired_postings,
            profile.hbc_posting_maintenance_centroid_refreshed,
            profile.hbc_posting_maintenance_payload_refreshed,
            profile.hbc_posting_maintenance_ancestor_refresh_roots,
            profile.hbc_posting_maintenance_split_postings,
            profile.hbc_posting_maintenance_merged_postings,
            profile.hbc_posting_maintenance_boundary_reassigned_vectors,
            profile.hbc_posting_lazy_centroid_deferrals,
            profile.hbc_posting_lazy_payload_deferrals,
            profile.hbc_posting_lazy_ancestor_deferrals,
        },
    );
}

pub fn addBatchProfile(total: *BatchProfile, delta: BatchProfile) void {
    inline for (std.meta.fields(BatchProfile)) |field| {
        @field(total, field.name) += @field(delta, field.name);
    }
}

pub fn recordProfileNs(profile: ?*BatchProfile, field: *u64, start_ns: u64) void {
    if (profile == null) return;
    field.* += platform.time.monotonicNs() - start_ns;
}

fn nsToMs(ns: u64) u64 {
    return ns / std.time.ns_per_ms;
}

pub const GeneratedPrecomputeMode = enum {
    none,
    full_text_only,
    all,
};

pub const PrecomputedGeneratedBatch = struct {
    artifact_writes: []types.BatchWrite = &.{},
    artifact_delete_keys: []const []const u8 = &.{},
    documents: []const derived_types.DerivedDocument = &.{},
    dense_embeddings: []const derived_types.DerivedDenseEmbeddingWrite = &.{},
    sparse_embeddings: []const derived_types.DerivedSparseEmbeddingWrite = &.{},
    generated_enrichment_refs: []const enrichment_types.GeneratedEnrichmentRef = &.{},

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        for (self.artifact_writes) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        if (self.artifact_writes.len > 0) alloc.free(self.artifact_writes);
        for (self.artifact_delete_keys) |key| alloc.free(key);
        if (self.artifact_delete_keys.len > 0) alloc.free(self.artifact_delete_keys);

        var derived_batch = derived_types.DerivedBatch{
            .documents = self.documents,
            .dense_embeddings = self.dense_embeddings,
            .sparse_embeddings = self.sparse_embeddings,
            .generated_enrichment_refs = self.generated_enrichment_refs,
        };
        derived_types.deinitDerivedBatch(alloc, &derived_batch);
        self.* = undefined;
    }
};

const DocumentChildRangeRoutingSnapshot = struct {
    doc_key: []u8,
    manifest_artifact_name: []u8,
    child_ranges: []types.DocumentArtifactChildRange,

    fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.doc_key);
        alloc.free(self.manifest_artifact_name);
        freeDocumentArtifactChildRanges(alloc, self.child_ranges);
        self.* = undefined;
    }
};

pub const DocumentChildRangeDispatchGroup = struct {
    owner_group_id: u64,
    doc_key: []u8,
    artifact_name: []u8,
    artifact_writes: std.ArrayListUnmanaged(types.BatchWrite) = .empty,
    artifact_delete_keys: std.ArrayListUnmanaged([]const u8) = .empty,
    documents: std.ArrayListUnmanaged(derived_types.DerivedDocument) = .empty,
    dense_embeddings: std.ArrayListUnmanaged(derived_types.DerivedDenseEmbeddingWrite) = .empty,
    sparse_embeddings: std.ArrayListUnmanaged(derived_types.DerivedSparseEmbeddingWrite) = .empty,
    generated_enrichment_refs: std.ArrayListUnmanaged(enrichment_types.GeneratedEnrichmentRef) = .empty,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.doc_key);
        alloc.free(self.artifact_name);
        for (self.artifact_writes.items) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        self.artifact_writes.deinit(alloc);
        for (self.artifact_delete_keys.items) |key| alloc.free(@constCast(key));
        self.artifact_delete_keys.deinit(alloc);
        var derived_batch = derived_types.DerivedBatch{
            .documents = self.documents.items,
            .dense_embeddings = self.dense_embeddings.items,
            .sparse_embeddings = self.sparse_embeddings.items,
            .generated_enrichment_refs = self.generated_enrichment_refs.items,
        };
        derived_types.deinitDerivedBatch(alloc, &derived_batch);
        self.documents = .empty;
        self.dense_embeddings = .empty;
        self.sparse_embeddings = .empty;
        self.generated_enrichment_refs = .empty;
        self.* = undefined;
    }

    fn dispatch(self: @This(), sync_level: types.SyncLevel) DocumentArtifactChildRangeDispatch {
        return .{
            .owner_group_id = self.owner_group_id,
            .doc_key = self.doc_key,
            .artifact_name = self.artifact_name,
            .child_batch = .{
                .artifact_writes = self.artifact_writes.items,
                .artifact_delete_keys = self.artifact_delete_keys.items,
                .documents = self.documents.items,
                .dense_embeddings = self.dense_embeddings.items,
                .sparse_embeddings = self.sparse_embeddings.items,
                .generated_enrichment_refs = self.generated_enrichment_refs.items,
                .sync_level = sync_level,
            },
        };
    }
};

const DocumentChildRangeRoute = struct {
    owner_group_id: u64,
    doc_key: []const u8,
    artifact_name: []const u8,
};

pub fn appendDocumentChildRangeOutboxWrites(
    alloc: Allocator,
    sequence: u64,
    groups: []const DocumentChildRangeDispatchGroup,
    sync_level: types.SyncLevel,
    writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    owned_keys: *std.ArrayListUnmanaged([]u8),
    owned_values: *std.ArrayListUnmanaged([]u8),
) !void {
    for (groups, 0..) |group, i| {
        const key = try internal_keys.documentChildRangeOutboxKeyAlloc(alloc, sequence, @intCast(i));
        errdefer alloc.free(key);
        const value = try std.json.Stringify.valueAlloc(alloc, DocumentArtifactChildRangeOutboxRecord{
            .owner_group_id = group.owner_group_id,
            .doc_key = group.doc_key,
            .artifact_name = group.artifact_name,
            .child_batch = group.dispatch(sync_level).child_batch,
        }, .{});
        errdefer alloc.free(value);
        try owned_keys.append(alloc, key);
        try owned_values.append(alloc, value);
        try writes.append(alloc, .{ .key = key, .value = value });
    }
}

pub fn documentArtifactChildRangesFromManifestJsonAlloc(alloc: Allocator, manifest_json: []const u8) ![]types.DocumentArtifactChildRange {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, manifest_json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return try alloc.alloc(types.DocumentArtifactChildRange, 0);
    return try documentArtifactChildRangesFromJsonAlloc(alloc, parsed.value.object);
}

pub fn freeDocumentArtifactChildRanges(alloc: Allocator, child_ranges: []types.DocumentArtifactChildRange) void {
    for (child_ranges) |*child_range| child_range.deinit(alloc);
    if (child_ranges.len > 0) alloc.free(child_ranges);
}

fn freeOwnedConstKeySlice(alloc: Allocator, keys: []const []const u8) void {
    db_internal.freeOwnedConstKeySlice(alloc, keys);
}

fn containsDeleteKey(list: []const []const u8, key: []const u8) bool {
    return db_internal.containsKey(list, key);
}

pub fn validateDocumentExtractionInlineSources(db: anytype, doc_value: []const u8) !void {
    var has_document_extraction_asset = false;
    for (db.core.index_manager.enrichments.items) |entry| {
        if (entry.kind != .asset) continue;
        var producer_cfg = asset_producer_mod.parseProducerConfig(db.alloc, entry.producer_json) catch continue;
        defer producer_cfg.deinit(db.alloc);
        if (producer_cfg.type == .document_extraction) {
            has_document_extraction_asset = true;
            break;
        }
    }
    if (!has_document_extraction_asset) return;

    const parsed = try std.json.parseFromSlice(std.json.Value, db.alloc, doc_value, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return;

    for (db.core.index_manager.enrichments.items) |entry| {
        if (entry.kind != .asset) continue;
        var producer_cfg = asset_producer_mod.parseProducerConfig(db.alloc, entry.producer_json) catch continue;
        defer producer_cfg.deinit(db.alloc);
        if (producer_cfg.type != .document_extraction) continue;

        if (entry.source_template.len > 0) {
            const rendered = renderSourceTemplateText(db.alloc, db.secret_store, db.remote_content, entry.source_template, doc_value) catch |err| switch (err) {
                error.PermanentPromptFailure, error.TransientPromptFailure => return err,
                else => continue,
            };
            defer db.alloc.free(rendered);
            try document_extraction_mod.validateInlineSourceSize(db.remote_content, rendered);
            continue;
        }

        const source = jsonValueAtPath(parsed.value, entry.source_field) orelse continue;
        if (source != .string) continue;
        try document_extraction_mod.validateInlineSourceSize(db.remote_content, source.string);
    }
}

const assetStateKeyAlloc = db_internal.assetStateKeyAlloc;
const batchDocumentValueForGraphSource = db_internal.batchDocumentValueForGraphSource;
const graphArtifactContentType = db_internal.graphArtifactContentType;
const graphAssetStateKeyAlloc = db_internal.graphAssetStateKeyAlloc;
const encodeGraphAssetStateKeysAlloc = db_internal.encodeGraphAssetStateKeysAlloc;
const loadGraphAssetStateKeysAlloc = db_internal.loadGraphAssetStateKeysAlloc;

fn jsonObjectStringDupOrEmpty(alloc: Allocator, object: std.json.ObjectMap, field_name: []const u8) ![]u8 {
    return try jsonObjectOptionalStringDup(alloc, object, field_name) orelse "";
}

fn jsonObjectOptionalU64OrZero(object: std.json.ObjectMap, field_name: []const u8) !u64 {
    return try jsonObjectOptionalU64(object, field_name) orelse 0;
}

fn jsonObjectOptionalUsizeOrZero(object: std.json.ObjectMap, field_name: []const u8) !usize {
    return try jsonObjectOptionalUsize(object, field_name) orelse 0;
}

const putLeakyJsonStringField = db_internal.putLeakyJsonStringField;
const putLeakyJsonU64Field = db_internal.putLeakyJsonU64Field;

fn documentArtifactManifestFromValueAlloc(
    alloc: Allocator,
    doc_key: []const u8,
    artifact_name: []const u8,
    manifest_key: []const u8,
    manifest_json: []u8,
    state_json: ?[]u8,
) !types.DocumentArtifactManifest {
    var manifest_json_owned = true;
    errdefer if (manifest_json_owned) alloc.free(manifest_json);
    var state_json_owned = true;
    errdefer if (state_json_owned) {
        if (state_json) |value| alloc.free(value);
    };

    var artifact_ref = (try artifact_ids.decodeArtifactRefAlloc(alloc, manifest_key)) orelse return error.InvalidDocumentExtractionManifest;
    defer artifact_ref.deinit(alloc);
    const artifact_id = try artifact_ids.artifactPublicIdAlloc(alloc, artifact_ref);
    errdefer alloc.free(artifact_id);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, manifest_json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidDocumentExtractionManifest;
    const object = parsed.value.object;

    const child_ranges = try documentArtifactChildRangesFromManifestJsonAlloc(alloc, manifest_json);
    errdefer freeDocumentArtifactChildRanges(alloc, child_ranges);

    var merge_status: []u8 = "";
    errdefer if (merge_status.len > 0) alloc.free(merge_status);
    var merge_from_generation: u64 = 0;
    var merge_to_generation: u64 = 0;
    var merge_operation_granularity: []u8 = "";
    errdefer if (merge_operation_granularity.len > 0) alloc.free(merge_operation_granularity);
    var merge_operation_count: usize = 0;
    if (object.get("merge_plan")) |merge_value| {
        if (merge_value != .object) return error.InvalidDocumentExtractionManifest;
        merge_status = try jsonObjectStringDupOrEmpty(alloc, merge_value.object, "status");
        merge_from_generation = try jsonObjectOptionalU64OrZero(merge_value.object, "from_generation");
        merge_to_generation = try jsonObjectOptionalU64OrZero(merge_value.object, "to_generation");
        merge_operation_granularity = try jsonObjectStringDupOrEmpty(alloc, merge_value.object, "operation_granularity");
        if (merge_value.object.get("operations")) |operations| {
            if (operations != .array) return error.InvalidDocumentExtractionManifest;
            merge_operation_count = operations.array.items.len;
        }
    }

    var last_error_code: ?[]u8 = null;
    errdefer if (last_error_code) |value| alloc.free(value);
    var last_error_message: ?[]u8 = null;
    errdefer if (last_error_message) |value| alloc.free(value);
    if (object.get("last_error")) |last_error| {
        if (last_error != .object) return error.InvalidDocumentExtractionManifest;
        last_error_code = try jsonObjectOptionalStringDup(alloc, last_error.object, "code");
        last_error_message = try jsonObjectOptionalStringDup(alloc, last_error.object, "message");
    }

    const source_url = try jsonObjectStringDupOrEmpty(alloc, object, "source_url");
    errdefer if (source_url.len > 0) alloc.free(source_url);
    const source_fingerprint = try jsonObjectStringDupOrEmpty(alloc, object, "source_fingerprint");
    errdefer if (source_fingerprint.len > 0) alloc.free(source_fingerprint);
    const content_type = try jsonObjectStringDupOrEmpty(alloc, object, "content_type");
    errdefer if (content_type.len > 0) alloc.free(content_type);
    const route_type = try jsonObjectStringDupOrEmpty(alloc, object, "route_type");
    errdefer if (route_type.len > 0) alloc.free(route_type);
    const unsupported_reason = try jsonObjectOptionalStringDup(alloc, object, "unsupported_reason");
    errdefer if (unsupported_reason) |value| alloc.free(value);

    const document_id = try alloc.dupe(u8, doc_key);
    errdefer alloc.free(document_id);
    const artifact_name_owned = try alloc.dupe(u8, artifact_name);
    errdefer alloc.free(artifact_name_owned);
    const manifest_version = try jsonObjectOptionalU64OrZero(object, "manifest_version");
    const generation = try jsonObjectOptionalU64OrZero(object, "generation");
    const unit_count = try jsonObjectOptionalUsizeOrZero(object, "unit_count");
    const chunk_count = try jsonObjectOptionalUsizeOrZero(object, "chunk_count");

    manifest_json_owned = false;
    state_json_owned = false;
    return .{
        .document_id = document_id,
        .artifact_name = artifact_name_owned,
        .artifact_id = artifact_id,
        .manifest_json = manifest_json,
        .state_json = state_json,
        .manifest_version = manifest_version,
        .generation = generation,
        .source_url = source_url,
        .source_fingerprint = source_fingerprint,
        .content_type = content_type,
        .route_type = route_type,
        .unsupported_reason = unsupported_reason,
        .unit_count = unit_count,
        .chunk_count = chunk_count,
        .child_ranges = child_ranges,
        .child_range_count = child_ranges.len,
        .merge_status = merge_status,
        .merge_from_generation = merge_from_generation,
        .merge_to_generation = merge_to_generation,
        .merge_operation_granularity = merge_operation_granularity,
        .merge_operation_count = merge_operation_count,
        .last_error_code = last_error_code,
        .last_error_message = last_error_message,
    };
}

pub fn computeDocumentExtractionAssetRequestDerived(
    alloc: Allocator,
    db: anytype,
    doc_value: []const u8,
    source_url: []const u8,
    request: enrichment_types.GeneratedEnrichmentRequest,
    config_json: []const u8,
    manifest_key: []const u8,
    artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
    artifact_delete_keys: *std.ArrayListUnmanaged([]const u8),
    documents: *std.ArrayListUnmanaged(derived_types.DerivedDocument),
    dense_embeddings: *std.ArrayListUnmanaged(derived_types.DerivedDenseEmbeddingWrite),
    sparse_embeddings: *std.ArrayListUnmanaged(derived_types.DerivedSparseEmbeddingWrite),
    force_reprocess: bool,
) !void {
    const artifact_name = requestArtifactName(request);
    var config = try document_extraction_mod.parseConfig(alloc, config_json);
    defer config.deinit(alloc);
    try document_extraction_mod.applySourceMetadataFromJson(alloc, &config, doc_value);

    const state_key = try assetStateKeyAlloc(alloc, request.doc_key, artifact_name);
    defer alloc.free(state_key);
    const existing_state = db.core.getStoreValue(alloc, state_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    defer if (existing_state) |value| alloc.free(value);
    const existing_manifest = db.core.getStoreValue(alloc, manifest_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    defer if (existing_manifest) |value| alloc.free(value);
    var previous_child_ranges: []types.DocumentArtifactChildRange = &.{};
    defer freeDocumentArtifactChildRanges(alloc, previous_child_ranges);
    if (existing_manifest) |value| {
        previous_child_ranges = try documentArtifactChildRangesFromManifestJsonAlloc(alloc, value);
    }

    const from_generation = if (existing_manifest) |value| try documentExtractionManifestGeneration(alloc, value) else 0;
    const to_generation = from_generation + 1;

    const metadata_fingerprint = try document_extraction_mod.metadataFingerprintAlloc(alloc, source_url, config_json, config);
    defer if (metadata_fingerprint) |fingerprint| alloc.free(fingerprint);
    if (!force_reprocess) {
        if (metadata_fingerprint) |fingerprint| {
            if (existing_state) |state| {
                if (documentExtractionStateFingerprintMatches(alloc, state, fingerprint)) {
                    if (existing_manifest) |value| {
                        if (!(try documentExtractionManifestHasLastError(alloc, value))) {
                            try artifact_writes.append(alloc, .{
                                .key = try alloc.dupe(u8, manifest_key),
                                .value = try alloc.dupe(u8, value),
                            });
                            return;
                        }
                    }
                }
            }
        }
    }

    const fetched = template_remote.downloadRemoteContentOutcomeAllocWithConfig(
        alloc,
        db.remote_content,
        db.secret_store,
        source_url,
        if (config.credentials.len > 0) config.credentials else null,
    ) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => {
            try appendDocumentExtractionFailureManifest(alloc, request.doc_key, artifact_name, source_url, manifest_key, state_key, existing_state, from_generation, to_generation, @errorName(err), "remote content download failed", artifact_writes, artifact_delete_keys);
            return;
        },
    };
    const downloaded = switch (fetched) {
        .ok => |content| content,
        .http_error => |http_error| {
            const message = try std.fmt.allocPrint(alloc, "{s}: HTTP {d}", .{ http_error.message, http_error.status });
            defer alloc.free(message);
            try appendDocumentExtractionFailureManifest(alloc, request.doc_key, artifact_name, source_url, manifest_key, state_key, existing_state, from_generation, to_generation, "RemoteDocumentFetchFailed", message, artifact_writes, artifact_delete_keys);
            return;
        },
    };
    var downloaded_mut = downloaded;
    defer downloaded_mut.deinit(alloc);

    var extraction = document_extraction_mod.extractDownloadedAlloc(alloc, downloaded_mut, source_url, config) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => {
            try appendDocumentExtractionFailureManifest(alloc, request.doc_key, artifact_name, source_url, manifest_key, state_key, existing_state, from_generation, to_generation, @errorName(err), "document extraction failed", artifact_writes, artifact_delete_keys);
            return;
        },
    };
    defer extraction.deinit(alloc);
    try completeDocumentExtractionGeneratedText(alloc, db.enrichment_runtime, config, source_url, extraction.content_type, &extraction);

    const byte_source_fingerprint = if (metadata_fingerprint == null)
        try documentExtractionFingerprintAlloc(alloc, source_url, config_json, config.content_type, config.filename, downloaded_mut.content_type, downloaded_mut.data)
    else
        null;
    defer if (byte_source_fingerprint) |fingerprint| alloc.free(fingerprint);
    const source_fingerprint = metadata_fingerprint orelse byte_source_fingerprint.?;

    var desired_unit_keys = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (desired_unit_keys.items) |key| alloc.free(@constCast(key));
        desired_unit_keys.deinit(alloc);
    }
    var desired_unit_fingerprints = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (desired_unit_fingerprints.items) |fingerprint| alloc.free(@constCast(fingerprint));
        desired_unit_fingerprints.deinit(alloc);
    }
    var desired_chunk_keys = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (desired_chunk_keys.items) |key| alloc.free(@constCast(key));
        desired_chunk_keys.deinit(alloc);
    }

    try collectDocumentExtractionDesiredKeys(alloc, db, request.doc_key, artifact_name, extraction.units, &desired_unit_keys, &desired_unit_fingerprints, &desired_chunk_keys);

    const desired_unit_descriptors = try documentExtractionUnitDescriptorsFromKeysAlloc(alloc, desired_unit_keys.items, desired_unit_fingerprints.items);
    defer alloc.free(desired_unit_descriptors);

    const new_state = try documentExtractionStateValueAlloc(alloc, source_fingerprint, desired_unit_keys.items, desired_unit_descriptors, desired_chunk_keys.items);
    defer alloc.free(new_state);

    var previous_unit_keys: []const []const u8 = &.{};
    defer freeOwnedConstKeySlice(alloc, previous_unit_keys);
    var previous_unit_descriptors: []DocumentExtractionUnitDescriptor = &.{};
    defer freeDocumentExtractionUnitDescriptors(alloc, previous_unit_descriptors);
    var previous_chunk_keys: []const []const u8 = &.{};
    defer freeOwnedConstKeySlice(alloc, previous_chunk_keys);

    if (existing_state) |state| {
        if (!force_reprocess and std.mem.eql(u8, state, new_state)) {
            if (existing_manifest) |value| {
                if (!(try documentExtractionManifestHasLastError(alloc, value))) {
                    try artifact_writes.append(alloc, .{
                        .key = try alloc.dupe(u8, manifest_key),
                        .value = try alloc.dupe(u8, value),
                    });
                    return;
                }
            }
        }

        previous_unit_keys = try documentExtractionStateUnitKeysAlloc(alloc, state);
        previous_unit_descriptors = try documentExtractionStateUnitDescriptorsAlloc(alloc, state);
        for (previous_unit_keys) |previous_key| {
            if (containsDeleteKey(desired_unit_keys.items, previous_key)) continue;
            try artifact_delete_keys.append(alloc, try alloc.dupe(u8, previous_key));
        }
        previous_chunk_keys = try documentExtractionStateChunkKeysAlloc(alloc, state);
        for (previous_chunk_keys) |previous_key| {
            if (containsDeleteKey(desired_chunk_keys.items, previous_key)) continue;
            try artifact_delete_keys.append(alloc, try alloc.dupe(u8, previous_key));
        }
    }

    const text_indexes = try db.core.index_manager.textIndexesForChunk(alloc, artifact_name, false);
    defer {
        for (text_indexes) |name| alloc.free(name);
        alloc.free(text_indexes);
    }

    const chunk_range_base_index = documentExtractionUnitRangeCount(extraction.units);
    for (extraction.units, desired_unit_descriptors, 0..) |unit, unit_descriptor, unit_index| {
        const unit_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, request.doc_key, artifact_name, unit.unit_id);
        defer alloc.free(unit_key);
        const unit_range_id = try documentExtractionRangeIdAlloc(alloc, documentExtractionUnitRangeIndex(extraction.units, unit_index));
        defer alloc.free(unit_range_id);
        const unit_route = documentExtractionRangeRoute(previous_child_ranges, unit_range_id, "unit", artifact_name);
        const unit_unchanged = std.mem.eql(u8, unit_descriptor.key, unit_key) and
            unitDescriptorFingerprintMatches(previous_unit_descriptors, unit_key, unit_descriptor.fingerprint);
        if (unit_unchanged and
            try documentUnitCanSkipLocalWrites(alloc, db, request.doc_key, artifact_name, unit_key, unit, text_indexes))
        {
            if (force_reprocess) {
                const payload = try documentUnitPayloadAlloc(alloc, request.doc_key, artifact_name, unit, source_url, extraction.content_type, unit_route);
                defer alloc.free(payload);
                try artifact_writes.append(alloc, .{
                    .key = try alloc.dupe(u8, unit_key),
                    .value = try alloc.dupe(u8, payload),
                });
                if (text_indexes.len > 0) {
                    const targets = try alloc.alloc(derived_types.DerivedTargetRef, text_indexes.len);
                    errdefer {
                        for (targets) |target| alloc.free(target.index_name);
                        alloc.free(targets);
                    }
                    for (text_indexes, 0..) |index_name, i| {
                        targets[i] = .{
                            .kind = .full_text,
                            .index_name = try alloc.dupe(u8, index_name),
                        };
                    }
                    try documents.append(alloc, .{
                        .key = try alloc.dupe(u8, unit_key),
                        .action = .upsert,
                        .cleaned_value = try alloc.dupe(u8, payload),
                        .targets = targets,
                    });
                }
                try appendDocumentUnitStoredChunkFullTextDocuments(alloc, db, request.doc_key, artifact_name, unit, documents);
            } else {
                try appendDocumentUnitStoredFullTextDocuments(alloc, db, request.doc_key, artifact_name, unit_key, unit, text_indexes, documents);
            }
            continue;
        }
        const payload = try documentUnitPayloadAlloc(alloc, request.doc_key, artifact_name, unit, source_url, extraction.content_type, unit_route);
        defer alloc.free(payload);
        try artifact_writes.append(alloc, .{
            .key = try alloc.dupe(u8, unit_key),
            .value = try alloc.dupe(u8, payload),
        });
        if (text_indexes.len > 0) {
            const targets = try alloc.alloc(derived_types.DerivedTargetRef, text_indexes.len);
            errdefer {
                for (targets) |target| alloc.free(target.index_name);
                alloc.free(targets);
            }
            for (text_indexes, 0..) |index_name, i| {
                targets[i] = .{
                    .kind = .full_text,
                    .index_name = try alloc.dupe(u8, index_name),
                };
            }
            try documents.append(alloc, .{
                .key = try alloc.dupe(u8, unit_key),
                .action = .upsert,
                .cleaned_value = try alloc.dupe(u8, payload),
                .targets = targets,
            });
        }

        try appendDocumentUnitChunkWrites(alloc, db, request.doc_key, artifact_name, unit_key, unit, desired_chunk_keys.items, chunk_range_base_index, previous_child_ranges, artifact_writes, documents, dense_embeddings, sparse_embeddings);
    }

    const manifest = try documentExtractionManifestPayloadAlloc(
        alloc,
        request.doc_key,
        artifact_name,
        source_url,
        source_fingerprint,
        extraction,
        desired_unit_keys.items,
        desired_unit_descriptors,
        desired_chunk_keys.items,
        previous_child_ranges,
        previous_unit_keys,
        previous_unit_descriptors,
        previous_chunk_keys,
        to_generation,
        from_generation,
        to_generation,
        "converged",
    );
    defer alloc.free(manifest);
    try artifact_writes.append(alloc, .{
        .key = try alloc.dupe(u8, manifest_key),
        .value = try alloc.dupe(u8, manifest),
    });
    try artifact_writes.append(alloc, .{
        .key = try alloc.dupe(u8, state_key),
        .value = try alloc.dupe(u8, new_state),
    });
}

fn completeDocumentExtractionGeneratedText(
    alloc: Allocator,
    runtime: ?*enrichment_runtime_mod.EnrichmentRuntime,
    config: document_extraction_mod.Config,
    source_url: []const u8,
    source_content_type: []const u8,
    extraction: *document_extraction_mod.Result,
) !void {
    const active_runtime = runtime orelse return;
    const producer = active_runtime.config.asset_producer orelse return;
    for (extraction.units) |*unit| {
        if (config.ocr_enabled and unit.extraction_status != null and std.mem.eql(u8, unit.extraction_status.?, "pending_ocr")) {
            const parts_json = try documentGeneratedTextPartsJsonAlloc(alloc, extraction.route_type, source_content_type, unit.*);
            defer alloc.free(parts_json);
            const produced = try producer.produce(alloc, .{
                .producer_type = .reader,
                .config_json = config.ocr_config_json,
                .source_text = source_url,
                .source_parts_json = parts_json,
                .content_type = "text/plain",
            });
            errdefer alloc.free(produced);
            try applyGeneratedUnitText(alloc, unit, produced, "ocr_text", "completed", .ocr);
            continue;
        }
        if (config.transcription_enabled and unit.extraction_status != null and std.mem.eql(u8, unit.extraction_status.?, "pending_transcription")) {
            const parts_json = try documentGeneratedTextPartsJsonAlloc(alloc, extraction.route_type, source_content_type, unit.*);
            defer alloc.free(parts_json);
            const produced = try producer.produce(alloc, .{
                .producer_type = .transcriber,
                .config_json = config.transcription_config_json,
                .source_text = source_url,
                .source_parts_json = parts_json,
                .content_type = "text/plain",
            });
            errdefer alloc.free(produced);
            try applyGeneratedUnitText(alloc, unit, produced, "transcript_text", "completed", .transcript);
        }
    }
}

const GeneratedUnitTextKind = enum { ocr, transcript };

fn applyGeneratedUnitText(
    alloc: Allocator,
    unit: *document_extraction_mod.Unit,
    produced: []u8,
    method: []const u8,
    status: []const u8,
    kind: GeneratedUnitTextKind,
) !void {
    if (produced.len == 0) {
        alloc.free(produced);
        return;
    }
    defer alloc.free(produced);
    var parsed = try parseGeneratedUnitTextOutputAlloc(alloc, produced);
    errdefer parsed.deinit(alloc);
    const owned_method = try alloc.dupe(u8, method);
    errdefer alloc.free(owned_method);
    const owned_status = try alloc.dupe(u8, status);
    errdefer alloc.free(owned_status);

    alloc.free(unit.text);
    alloc.free(unit.method);
    if (unit.extraction_status) |value| alloc.free(value);
    if (unit.extraction_warning) |value| alloc.free(value);
    unit.text = parsed.text;
    parsed.text = &.{};
    unit.method = owned_method;
    unit.extraction_status = owned_status;
    switch (kind) {
        .ocr => {
            unit.ocr_used = true;
            unit.ocr_confidence = parsed.confidence;
            unit.ocr_bbox = parsed.bbox;
        },
        .transcript => {
            unit.transcript_used = true;
            unit.transcript_confidence = parsed.confidence;
        },
    }
    unit.extraction_warning = parsed.warning;
    parsed.warning = null;
    const start = unit.char_start orelse 0;
    unit.char_start = start;
    unit.char_end = std.math.cast(u32, @as(usize, @intCast(start)) + unit.text.len);
}

const ParsedGeneratedUnitText = struct {
    text: []u8,
    confidence: ?f64 = null,
    bbox: ?[4]f64 = null,
    warning: ?[]u8 = null,

    fn deinit(self: *ParsedGeneratedUnitText, alloc: Allocator) void {
        if (self.text.len > 0) alloc.free(self.text);
        if (self.warning) |value| alloc.free(value);
        self.* = undefined;
    }
};

fn parseGeneratedUnitTextOutputAlloc(alloc: Allocator, produced: []const u8) !ParsedGeneratedUnitText {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, produced, .{}) catch {
        return .{ .text = try alloc.dupe(u8, produced) };
    };
    defer parsed.deinit();
    if (parsed.value != .object) return .{ .text = try alloc.dupe(u8, produced) };
    const text_value = parsed.value.object.get("text") orelse return .{ .text = try alloc.dupe(u8, produced) };
    if (text_value != .string) return .{ .text = try alloc.dupe(u8, produced) };

    var out = ParsedGeneratedUnitText{ .text = try alloc.dupe(u8, text_value.string) };
    errdefer out.deinit(alloc);
    out.confidence = generatedTextJsonFloatField(parsed.value.object, "confidence");
    out.bbox = generatedTextJsonBboxField(parsed.value.object, "ocr_bbox") orelse generatedTextJsonBboxField(parsed.value.object, "bbox") orelse generatedTextJsonBboxField(parsed.value.object, "coordinates");
    if (generatedTextJsonStringField(parsed.value.object, "warning") orelse generatedTextJsonStringField(parsed.value.object, "extraction_warning")) |warning| {
        out.warning = try alloc.dupe(u8, warning);
    }
    return out;
}

fn generatedTextJsonStringField(object: std.json.ObjectMap, field: []const u8) ?[]const u8 {
    const value = object.get(field) orelse return null;
    return if (value == .string) value.string else null;
}

fn generatedTextJsonFloatField(object: std.json.ObjectMap, field: []const u8) ?f64 {
    const value = object.get(field) orelse return null;
    return switch (value) {
        .float => |v| v,
        .integer => |v| @floatFromInt(v),
        else => null,
    };
}

fn generatedTextJsonBboxField(object: std.json.ObjectMap, field: []const u8) ?[4]f64 {
    const value = object.get(field) orelse return null;
    if (value != .array or value.array.items.len != 4) return null;
    var out: [4]f64 = undefined;
    for (value.array.items, 0..) |item, i| {
        out[i] = switch (item) {
            .float => |v| v,
            .integer => |v| @floatFromInt(v),
            else => return null,
        };
    }
    return out;
}

fn documentGeneratedTextPartsJsonAlloc(
    alloc: Allocator,
    route_type: []const u8,
    source_content_type: []const u8,
    unit: document_extraction_mod.Unit,
) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, .{
        .schema = "antfly.document_generated_text_request.v1",
        .route_type = route_type,
        .source_content_type = source_content_type,
        .unit_id = unit.unit_id,
        .unit_type = unit.unit_type,
        .method = unit.method,
        .extraction_status = unit.extraction_status,
        .source_path = unit.source_path,
        .page_number = unit.page_number,
        .page_label = unit.page_label,
        .page_bbox = unit.page_bbox,
        .page_rotation = unit.page_rotation,
        .text_regions = unit.text_regions,
        .byte_length = unit.byte_length,
        .source_sha256 = unit.source_sha256,
        .ocr_confidence = unit.ocr_confidence,
        .ocr_bbox = unit.ocr_bbox,
        .transcript_confidence = unit.transcript_confidence,
        .extraction_warning = unit.extraction_warning,
    }, .{});
}

pub fn appendDocumentExtractionDeleteKeys(
    alloc: Allocator,
    db: anytype,
    doc_key: []const u8,
    artifact_name: []const u8,
    manifest_key: []const u8,
    artifact_delete_keys: *std.ArrayListUnmanaged([]const u8),
) !void {
    try artifact_delete_keys.append(alloc, try alloc.dupe(u8, manifest_key));
    const state_key = try assetStateKeyAlloc(alloc, doc_key, artifact_name);
    errdefer alloc.free(state_key);
    const existing_state = db.core.getStoreValue(alloc, state_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    defer if (existing_state) |value| alloc.free(value);
    if (existing_state) |state| {
        const previous_keys = try documentExtractionStateUnitKeysAlloc(alloc, state);
        defer freeOwnedConstKeySlice(alloc, previous_keys);
        for (previous_keys) |previous_key| {
            try artifact_delete_keys.append(alloc, try alloc.dupe(u8, previous_key));
        }
        const previous_chunk_keys = try documentExtractionStateChunkKeysAlloc(alloc, state);
        defer freeOwnedConstKeySlice(alloc, previous_chunk_keys);
        for (previous_chunk_keys) |previous_key| {
            try artifact_delete_keys.append(alloc, try alloc.dupe(u8, previous_key));
        }
    }
    try artifact_delete_keys.append(alloc, state_key);
}

fn collectDocumentExtractionDesiredKeys(
    alloc: Allocator,
    db: anytype,
    doc_key: []const u8,
    artifact_name: []const u8,
    units: []const document_extraction_mod.Unit,
    desired_unit_keys: *std.ArrayListUnmanaged([]const u8),
    desired_unit_fingerprints: *std.ArrayListUnmanaged([]const u8),
    desired_chunk_keys: *std.ArrayListUnmanaged([]const u8),
) !void {
    for (units) |unit| {
        try desired_unit_keys.append(alloc, try internal_keys.documentUnitArtifactKeyAlloc(alloc, doc_key, artifact_name, unit.unit_id));
        try desired_unit_fingerprints.append(alloc, try documentExtractionUnitFingerprintAlloc(alloc, unit));
        for (db.core.index_manager.enrichments.items) |entry| {
            if (entry.kind != .chunk) continue;
            if (!std.mem.eql(u8, entry.source_artifact_name, artifact_name)) continue;
            const chunks = if (entry.chunker_json.len > 0)
                try chunker_mod.chunkTextWithConfigJson(alloc, unit.text, entry.chunker_json)
            else
                try chunker_mod.chunkText(alloc, unit.text, entry.chunk_size, entry.chunk_overlap);
            defer chunker_mod.freeChunks(alloc, chunks);
            for (chunks) |chunk| {
                try desired_chunk_keys.append(alloc, try internal_keys.documentUnitChunkArtifactKeyAlloc(alloc, doc_key, entry.name, unit.unit_id, @intCast(chunk.chunk_id)));
            }
        }
    }
}

pub const DocumentExtractionUnitDescriptor = struct {
    key: []const u8,
    fingerprint: []const u8,
};

pub const DocumentExtractionRangeRoute = struct {
    range_id: []const u8,
    route_status: []const u8 = "local_committed",
    owner_group_id: u64 = 0,
};

fn documentExtractionUnitDescriptorsFromKeysAlloc(
    alloc: Allocator,
    unit_keys: []const []const u8,
    fingerprints: []const []const u8,
) ![]DocumentExtractionUnitDescriptor {
    if (unit_keys.len != fingerprints.len) return error.InvalidDocumentExtractionState;
    const out = try alloc.alloc(DocumentExtractionUnitDescriptor, unit_keys.len);
    for (unit_keys, fingerprints, 0..) |key, fingerprint, i| {
        out[i] = .{
            .key = key,
            .fingerprint = fingerprint,
        };
    }
    return out;
}

fn freeDocumentExtractionUnitDescriptors(alloc: Allocator, descriptors: []DocumentExtractionUnitDescriptor) void {
    for (descriptors) |descriptor| {
        if (descriptor.key.len > 0) alloc.free(@constCast(descriptor.key));
        if (descriptor.fingerprint.len > 0) alloc.free(@constCast(descriptor.fingerprint));
    }
    if (descriptors.len > 0) alloc.free(descriptors);
}

fn appendStoredFullTextDocument(
    alloc: Allocator,
    documents: *std.ArrayListUnmanaged(derived_types.DerivedDocument),
    key: []const u8,
    text_indexes: []const []const u8,
) !void {
    if (text_indexes.len == 0) return;
    const targets = try alloc.alloc(derived_types.DerivedTargetRef, text_indexes.len);
    errdefer {
        for (targets) |target| alloc.free(target.index_name);
        alloc.free(targets);
    }
    for (text_indexes, 0..) |index_name, i| {
        targets[i] = .{
            .kind = .full_text,
            .index_name = try alloc.dupe(u8, index_name),
        };
    }
    try documents.append(alloc, .{
        .key = try alloc.dupe(u8, key),
        .action = .upsert,
        .targets = targets,
    });
}

fn appendDocumentUnitStoredFullTextDocuments(
    alloc: Allocator,
    db: anytype,
    doc_key: []const u8,
    source_artifact_name: []const u8,
    unit_key: []const u8,
    unit: document_extraction_mod.Unit,
    unit_text_indexes: []const []const u8,
    documents: *std.ArrayListUnmanaged(derived_types.DerivedDocument),
) !void {
    try appendStoredFullTextDocument(alloc, documents, unit_key, unit_text_indexes);
    try appendDocumentUnitStoredChunkFullTextDocuments(alloc, db, doc_key, source_artifact_name, unit, documents);
}

fn appendDocumentUnitStoredChunkFullTextDocuments(
    alloc: Allocator,
    db: anytype,
    doc_key: []const u8,
    source_artifact_name: []const u8,
    unit: document_extraction_mod.Unit,
    documents: *std.ArrayListUnmanaged(derived_types.DerivedDocument),
) !void {
    for (db.core.index_manager.enrichments.items) |entry| {
        if (entry.kind != .chunk) continue;
        if (!std.mem.eql(u8, entry.source_artifact_name, source_artifact_name)) continue;

        const include_default_full_text = entry.full_text_index or
            try chunking_types_mod.parseHasFullTextIndexFromSlice(alloc, entry.chunker_json);
        const chunk_text_indexes = try db.core.index_manager.textIndexesForChunk(alloc, entry.name, include_default_full_text);
        defer {
            for (chunk_text_indexes) |name| alloc.free(name);
            alloc.free(chunk_text_indexes);
        }
        if (chunk_text_indexes.len == 0) continue;

        const chunks = if (entry.chunker_json.len > 0)
            try chunker_mod.chunkTextWithConfigJson(alloc, unit.text, entry.chunker_json)
        else
            try chunker_mod.chunkText(alloc, unit.text, entry.chunk_size, entry.chunk_overlap);
        defer chunker_mod.freeChunks(alloc, chunks);

        for (chunks) |chunk| {
            if (!chunk.isText()) continue;
            const chunk_key = try internal_keys.documentUnitChunkArtifactKeyAlloc(alloc, doc_key, entry.name, unit.unit_id, @intCast(chunk.chunk_id));
            defer alloc.free(chunk_key);
            try appendStoredFullTextDocument(alloc, documents, chunk_key, chunk_text_indexes);
        }
    }
}

fn storeKeyExists(alloc: Allocator, db: anytype, key: []const u8) !bool {
    const value = db.core.getStoreValue(alloc, key) catch |err| switch (err) {
        error.NotFound => return false,
        else => return err,
    };
    if (value) |owned| alloc.free(owned);
    return true;
}

fn documentUnitCanSkipLocalWrites(
    alloc: Allocator,
    db: anytype,
    doc_key: []const u8,
    source_artifact_name: []const u8,
    unit_key: []const u8,
    unit: document_extraction_mod.Unit,
    unit_text_indexes: []const []const u8,
) !bool {
    _ = unit_text_indexes;
    if (!(try storeKeyExists(alloc, db, unit_key))) return false;

    for (db.core.index_manager.enrichments.items) |entry| {
        if (entry.kind != .chunk) continue;
        if (!std.mem.eql(u8, entry.source_artifact_name, source_artifact_name)) continue;

        const chunks = if (entry.chunker_json.len > 0)
            try chunker_mod.chunkTextWithConfigJson(alloc, unit.text, entry.chunker_json)
        else
            try chunker_mod.chunkText(alloc, unit.text, entry.chunk_size, entry.chunk_overlap);
        defer chunker_mod.freeChunks(alloc, chunks);

        for (chunks) |chunk| {
            if (!chunk.isText()) continue;
            const chunk_key = try internal_keys.documentUnitChunkArtifactKeyAlloc(alloc, doc_key, entry.name, unit.unit_id, @intCast(chunk.chunk_id));
            defer alloc.free(chunk_key);
            if (!(try storeKeyExists(alloc, db, chunk_key))) return false;
            if (!(try documentUnitChunkEmbeddingArtifactsPresent(alloc, db, chunk_key, entry.name))) return false;
        }
    }
    return true;
}

fn documentUnitChunkEmbeddingArtifactsPresent(
    alloc: Allocator,
    db: anytype,
    chunk_key: []const u8,
    chunk_artifact_name: []const u8,
) !bool {
    const runtime = db.enrichment_runtime;
    for (db.core.index_manager.enrichments.items) |entry| {
        if (entry.kind != .embedding) continue;
        if (!std.mem.eql(u8, entry.source_artifact_name, chunk_artifact_name)) continue;

        if (entry.expected_dims > 0) {
            const consumer_indexes = try db.core.index_manager.denseIndexesForEmbedding(alloc, entry.name, entry.expected_dims);
            defer {
                for (consumer_indexes) |name| alloc.free(name);
                alloc.free(consumer_indexes);
            }
            if (consumer_indexes.len == 0 or runtime == null or runtime.?.config.dense_embedder == null) continue;
        } else {
            const consumer_indexes = try db.core.index_manager.sparseIndexesForEmbedding(alloc, entry.name);
            defer {
                for (consumer_indexes) |name| alloc.free(name);
                alloc.free(consumer_indexes);
            }
            if (consumer_indexes.len == 0 or runtime == null or runtime.?.config.sparse_embedder == null) continue;
        }

        const artifact_key = try internal_keys.derivedEmbeddingArtifactKeyAlloc(alloc, chunk_key, entry.name);
        defer alloc.free(artifact_key);
        if (!(try storeKeyExists(alloc, db, artifact_key))) return false;
    }
    return true;
}

fn appendDocumentUnitChunkWrites(
    alloc: Allocator,
    db: anytype,
    doc_key: []const u8,
    source_artifact_name: []const u8,
    unit_key: []const u8,
    unit: document_extraction_mod.Unit,
    desired_chunk_keys: []const []const u8,
    chunk_range_base_index: usize,
    previous_child_ranges: []const types.DocumentArtifactChildRange,
    artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
    documents: *std.ArrayListUnmanaged(derived_types.DerivedDocument),
    dense_embeddings: *std.ArrayListUnmanaged(derived_types.DerivedDenseEmbeddingWrite),
    sparse_embeddings: *std.ArrayListUnmanaged(derived_types.DerivedSparseEmbeddingWrite),
) !void {
    for (db.core.index_manager.enrichments.items) |entry| {
        if (entry.kind != .chunk) continue;
        if (!std.mem.eql(u8, entry.source_artifact_name, source_artifact_name)) continue;
        const chunks = if (entry.chunker_json.len > 0)
            try chunker_mod.chunkTextWithConfigJson(alloc, unit.text, entry.chunker_json)
        else
            try chunker_mod.chunkText(alloc, unit.text, entry.chunk_size, entry.chunk_overlap);
        defer chunker_mod.freeChunks(alloc, chunks);
        if (chunks.len == 0) continue;

        const include_default_full_text = entry.full_text_index or
            try chunking_types_mod.parseHasFullTextIndexFromSlice(alloc, entry.chunker_json);
        const text_indexes = try db.core.index_manager.textIndexesForChunk(alloc, entry.name, include_default_full_text);
        defer {
            for (text_indexes) |name| alloc.free(name);
            alloc.free(text_indexes);
        }

        var arena_state = std.heap.ArenaAllocator.init(alloc);
        defer arena_state.deinit();
        const scratch = arena_state.allocator();

        for (chunks) |chunk| {
            if (!chunk.isText()) continue;
            const chunk_key = try internal_keys.documentUnitChunkArtifactKeyAlloc(alloc, doc_key, entry.name, unit.unit_id, @intCast(chunk.chunk_id));
            defer alloc.free(chunk_key);
            const chunk_key_index = documentExtractionKeyIndex(desired_chunk_keys, chunk_key) orelse return error.DocumentExtractionChunkRangeMissing;
            const chunk_range_id = try documentExtractionRangeIdAlloc(scratch, chunk_range_base_index + (chunk_key_index / document_extraction_range_target_children));
            const chunk_route = documentExtractionRangeRoute(previous_child_ranges, chunk_range_id, "chunk", "derived_chunks");
            const payload = try buildDocumentUnitChunkPayloadAlloc(scratch, doc_key, unit_key, entry.name, source_artifact_name, entry.source_field, unit, chunk, true, chunk_route);
            try artifact_writes.append(alloc, .{
                .key = try alloc.dupe(u8, chunk_key),
                .value = try alloc.dupe(u8, payload),
            });

            if (text_indexes.len > 0) {
                const targets = try alloc.alloc(derived_types.DerivedTargetRef, text_indexes.len);
                errdefer {
                    for (targets) |target| alloc.free(target.index_name);
                    alloc.free(targets);
                }
                for (text_indexes, 0..) |index_name, i| {
                    targets[i] = .{
                        .kind = .full_text,
                        .index_name = try alloc.dupe(u8, index_name),
                    };
                }
                try documents.append(alloc, .{
                    .key = try alloc.dupe(u8, chunk_key),
                    .action = .upsert,
                    .cleaned_value = try alloc.dupe(u8, payload),
                    .targets = targets,
                });
            }

            try appendDocumentUnitChunkDenseEmbeddingWrites(alloc, db, doc_key, chunk_key, entry.name, entry.source_field, chunk, artifact_writes, dense_embeddings);
            try appendDocumentUnitChunkSparseEmbeddingWrites(alloc, db, chunk_key, entry.name, chunk, artifact_writes, sparse_embeddings);

            _ = arena_state.reset(.retain_capacity);
        }
    }
}

fn appendDocumentUnitChunkDenseEmbeddingWrites(
    alloc: Allocator,
    db: anytype,
    doc_key: []const u8,
    chunk_key: []const u8,
    chunk_artifact_name: []const u8,
    source_field: []const u8,
    chunk: chunker_mod.Chunk,
    artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
    dense_embeddings: *std.ArrayListUnmanaged(derived_types.DerivedDenseEmbeddingWrite),
) !void {
    const chunk_text = chunk.text orelse return;
    const runtime = db.enrichment_runtime orelse return;
    const dense_embedder = runtime.config.dense_embedder orelse return;

    for (db.core.index_manager.enrichments.items) |entry| {
        if (entry.kind != .embedding) continue;
        if (!std.mem.eql(u8, entry.source_artifact_name, chunk_artifact_name)) continue;
        if (entry.expected_dims == 0) continue;

        const consumer_indexes = try db.core.index_manager.denseIndexesForEmbedding(alloc, entry.name, entry.expected_dims);
        defer {
            for (consumer_indexes) |name| alloc.free(name);
            alloc.free(consumer_indexes);
        }
        if (consumer_indexes.len == 0) continue;

        const vector = try dense_embedder.embedDense(alloc, entry.name, chunk_text, entry.expected_dims);
        defer alloc.free(vector);
        const artifact_key = try appendEmbeddingArtifactWrite(
            alloc,
            artifact_writes,
            chunk_key,
            doc_key,
            entry.name,
            source_field,
            chunk_key,
            enrichment_artifact_codec.hashSource(chunk_text),
            vector,
        );
        defer alloc.free(artifact_key);
        try appendDerivedDenseEmbeddingForConsumers(alloc, dense_embeddings, chunk_key, doc_key, artifact_key, vector, consumer_indexes);
    }
}

fn appendDocumentUnitChunkSparseEmbeddingWrites(
    alloc: Allocator,
    db: anytype,
    chunk_key: []const u8,
    chunk_artifact_name: []const u8,
    chunk: chunker_mod.Chunk,
    artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
    sparse_embeddings: *std.ArrayListUnmanaged(derived_types.DerivedSparseEmbeddingWrite),
) !void {
    const chunk_text = chunk.text orelse return;
    const runtime = db.enrichment_runtime orelse return;
    const sparse_embedder = runtime.config.sparse_embedder orelse return;

    for (db.core.index_manager.enrichments.items) |entry| {
        if (entry.kind != .embedding) continue;
        if (!std.mem.eql(u8, entry.source_artifact_name, chunk_artifact_name)) continue;
        if (entry.expected_dims != 0) continue;

        const consumer_indexes = try db.core.index_manager.sparseIndexesForEmbedding(alloc, entry.name);
        defer {
            for (consumer_indexes) |name| alloc.free(name);
            alloc.free(consumer_indexes);
        }
        if (consumer_indexes.len == 0) continue;

        var sparse = try sparse_embedder.embedSparse(alloc, entry.name, chunk_text);
        defer sparse.deinit(alloc);
        const artifact_key = try appendSparseEmbeddingArtifactWrite(
            alloc,
            artifact_writes,
            chunk_key,
            entry.name,
            enrichment_artifact_codec.hashSource(chunk_text),
            sparse.indices,
            sparse.values,
        );
        defer alloc.free(artifact_key);
        try appendDerivedSparseEmbeddingForConsumers(alloc, sparse_embeddings, chunk_key, artifact_key, sparse.indices, sparse.values, consumer_indexes);
    }
}

fn buildDocumentUnitChunkPayloadAlloc(
    alloc: Allocator,
    doc_key: []const u8,
    unit_key: []const u8,
    artifact_name: []const u8,
    source_artifact_name: []const u8,
    source_field: []const u8,
    unit: document_extraction_mod.Unit,
    chunk: chunker_mod.Chunk,
    include_payload: bool,
    route: DocumentExtractionRangeRoute,
) ![]u8 {
    const owner_group_id = std.math.cast(i64, route.owner_group_id) orelse return error.InvalidDocumentExtractionManifest;
    var obj = std.json.ObjectMap.empty;
    try obj.put(alloc, try alloc.dupe(u8, "_parent_doc_key"), .{ .string = try alloc.dupe(u8, doc_key) });
    try obj.put(alloc, try alloc.dupe(u8, "_parent_unit_key"), .{ .string = try alloc.dupe(u8, unit_key) });
    try obj.put(alloc, try alloc.dupe(u8, "_parent_unit_id"), .{ .string = try alloc.dupe(u8, unit.unit_id) });
    try obj.put(alloc, try alloc.dupe(u8, "_artifact_name"), .{ .string = try alloc.dupe(u8, artifact_name) });
    try obj.put(alloc, try alloc.dupe(u8, "_source_artifact_name"), .{ .string = try alloc.dupe(u8, source_artifact_name) });
    try obj.put(alloc, try alloc.dupe(u8, "_source_field"), .{ .string = try alloc.dupe(u8, source_field) });
    try obj.put(alloc, try alloc.dupe(u8, "_artifact_range_id"), .{ .string = try alloc.dupe(u8, route.range_id) });
    try obj.put(alloc, try alloc.dupe(u8, "_artifact_range_kind"), .{ .string = try alloc.dupe(u8, "chunk") });
    try obj.put(alloc, try alloc.dupe(u8, "_artifact_route_status"), .{ .string = try alloc.dupe(u8, route.route_status) });
    try obj.put(alloc, try alloc.dupe(u8, "_artifact_owner_group_id"), .{ .integer = owner_group_id });
    try chunk_artifact_mod.appendArtifactFieldsWithProvenance(alloc, &obj, source_field, chunk, include_payload, .{
        .scope = .unit,
        .parent_doc_key = doc_key,
        .parent_unit_key = unit_key,
        .parent_unit_id = unit.unit_id,
        .source_artifact_name = source_artifact_name,
        .document_char_base = unit.char_start,
        .page_number = unit.page_number,
        .page_label = unit.page_label,
        .page_bbox = unit.page_bbox,
        .page_rotation = unit.page_rotation,
        .extraction_method = unit.method,
        .extraction_status = unit.extraction_status,
        .confidence = documentUnitConfidence(unit),
        .ocr_used = unit.ocr_used,
        .ocr_confidence = unit.ocr_confidence,
        .ocr_bbox = unit.ocr_bbox,
        .transcript_used = unit.transcript_used,
        .transcript_confidence = unit.transcript_confidence,
        .extraction_warning = unit.extraction_warning,
    });
    return try std.json.Stringify.valueAlloc(alloc, std.json.Value{ .object = obj }, .{});
}

fn hexBytesAlloc(alloc: Allocator, bytes: []const u8) ![]u8 {
    const out = try alloc.alloc(u8, bytes.len * 2);
    for (bytes, 0..) |byte, idx| {
        out[idx * 2] = std.fmt.digitToChar(byte >> 4, .lower);
        out[idx * 2 + 1] = std.fmt.digitToChar(byte & 0x0f, .lower);
    }
    return out;
}

pub fn documentExtractionUnitFingerprintAlloc(alloc: Allocator, unit: document_extraction_mod.Unit) ![]u8 {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update(unit.unit_id);
    hasher.update(unit.unit_type);
    hasher.update(unit.text);
    hasher.update(unit.method);
    if (unit.source_path) |source_path| hasher.update(source_path);
    if (unit.extraction_status) |extraction_status| hasher.update(extraction_status);
    if (unit.source_sha256) |source_sha256| hasher.update(source_sha256);
    if (unit.byte_length) |byte_length| {
        var buf: [@sizeOf(u64)]u8 = undefined;
        std.mem.writeInt(u64, &buf, byte_length, .big);
        hasher.update(&buf);
    }
    hasher.update(if (unit.ocr_used) "ocr:1" else "ocr:0");
    if (unit.ocr_confidence) |confidence| {
        const value: u64 = @bitCast(confidence);
        hasher.update(std.mem.asBytes(&value));
    }
    if (unit.ocr_bbox) |bbox| {
        for (bbox) |coord| {
            const value: u64 = @bitCast(coord);
            hasher.update(std.mem.asBytes(&value));
        }
    }
    hasher.update(if (unit.transcript_used) "transcript:1" else "transcript:0");
    if (unit.transcript_confidence) |confidence| {
        const value: u64 = @bitCast(confidence);
        hasher.update(std.mem.asBytes(&value));
    }
    if (unit.extraction_warning) |warning| hasher.update(warning);
    if (unit.page_number) |page_number| {
        var buf: [@sizeOf(u32)]u8 = undefined;
        std.mem.writeInt(u32, &buf, page_number, .big);
        hasher.update(&buf);
    }
    if (unit.page_label) |page_label| hasher.update(page_label);
    if (unit.page_bbox) |bbox| {
        for (bbox) |coord| {
            const value: u64 = @bitCast(coord);
            hasher.update(std.mem.asBytes(&value));
        }
    }
    if (unit.page_rotation) |rotation| {
        var buf: [@sizeOf(i32)]u8 = undefined;
        std.mem.writeInt(i32, &buf, rotation, .big);
        hasher.update(&buf);
    }
    for (unit.text_regions) |region| {
        for (region.span) |span| {
            var buf: [@sizeOf(u32)]u8 = undefined;
            std.mem.writeInt(u32, &buf, span, .big);
            hasher.update(&buf);
        }
        for (region.bbox) |coord| {
            const value: u64 = @bitCast(coord);
            hasher.update(std.mem.asBytes(&value));
        }
    }
    if (unit.char_start) |char_start| {
        var buf: [@sizeOf(u32)]u8 = undefined;
        std.mem.writeInt(u32, &buf, char_start, .big);
        hasher.update(&buf);
    }
    if (unit.char_end) |char_end| {
        var buf: [@sizeOf(u32)]u8 = undefined;
        std.mem.writeInt(u32, &buf, char_end, .big);
        hasher.update(&buf);
    }
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    return try hexBytesAlloc(alloc, &digest);
}

fn documentExtractionFingerprintAlloc(
    alloc: Allocator,
    source_url: []const u8,
    config_json: []const u8,
    configured_content_type: []const u8,
    configured_filename: []const u8,
    downloaded_content_type: []const u8,
    data: []const u8,
) ![]u8 {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update(source_url);
    hasher.update(config_json);
    hasher.update(configured_content_type);
    hasher.update(configured_filename);
    hasher.update(downloaded_content_type);
    hasher.update(data);
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    return try hexBytesAlloc(alloc, &digest);
}

fn documentExtractionStateValueAlloc(
    alloc: Allocator,
    fingerprint: []const u8,
    unit_keys: []const []const u8,
    unit_descriptors: []const DocumentExtractionUnitDescriptor,
    chunk_keys: []const []const u8,
) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, .{
        .kind = "document_extraction_state_v1",
        .fingerprint = fingerprint,
        .unit_keys = unit_keys,
        .unit_descriptors = unit_descriptors,
        .chunk_keys = chunk_keys,
    }, .{});
}

fn documentExtractionStateFingerprintMatches(alloc: Allocator, state: []const u8, fingerprint: []const u8) bool {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, state, .{}) catch return false;
    defer parsed.deinit();
    if (parsed.value != .object) return false;
    const value = parsed.value.object.get("fingerprint") orelse return false;
    return value == .string and std.mem.eql(u8, value.string, fingerprint);
}

fn documentExtractionStateUnitKeysAlloc(alloc: Allocator, state: []const u8) ![]const []const u8 {
    return try documentExtractionStateKeysAlloc(alloc, state, "unit_keys");
}

fn documentExtractionStateChunkKeysAlloc(alloc: Allocator, state: []const u8) ![]const []const u8 {
    return try documentExtractionStateKeysAlloc(alloc, state, "chunk_keys");
}

fn documentExtractionStateUnitDescriptorsAlloc(alloc: Allocator, state: []const u8) ![]DocumentExtractionUnitDescriptor {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, state, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return try alloc.alloc(DocumentExtractionUnitDescriptor, 0);
    const descriptors_value = parsed.value.object.get("unit_descriptors") orelse return documentExtractionStateUnitDescriptorFallbackAlloc(alloc, parsed.value.object);
    if (descriptors_value != .array) return error.InvalidDocumentExtractionState;
    const out = try alloc.alloc(DocumentExtractionUnitDescriptor, descriptors_value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |descriptor| {
            alloc.free(@constCast(descriptor.key));
            alloc.free(@constCast(descriptor.fingerprint));
        }
        alloc.free(out);
    }
    for (descriptors_value.array.items, 0..) |item, i| {
        if (item != .object) return error.InvalidDocumentExtractionState;
        const key_value = item.object.get("key") orelse return error.InvalidDocumentExtractionState;
        const fingerprint_value = item.object.get("fingerprint") orelse return error.InvalidDocumentExtractionState;
        if (key_value != .string or fingerprint_value != .string) return error.InvalidDocumentExtractionState;
        out[i] = .{
            .key = try alloc.dupe(u8, key_value.string),
            .fingerprint = try alloc.dupe(u8, fingerprint_value.string),
        };
        initialized += 1;
    }
    return out;
}

fn documentExtractionStateUnitDescriptorFallbackAlloc(alloc: Allocator, object: std.json.ObjectMap) ![]DocumentExtractionUnitDescriptor {
    const keys_value = object.get("unit_keys") orelse return try alloc.alloc(DocumentExtractionUnitDescriptor, 0);
    if (keys_value != .array) return try alloc.alloc(DocumentExtractionUnitDescriptor, 0);
    const out = try alloc.alloc(DocumentExtractionUnitDescriptor, keys_value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |descriptor| {
            alloc.free(@constCast(descriptor.key));
            if (descriptor.fingerprint.len > 0) alloc.free(@constCast(descriptor.fingerprint));
        }
        alloc.free(out);
    }
    for (keys_value.array.items, 0..) |item, i| {
        if (item != .string) return error.InvalidDocumentExtractionState;
        out[i] = .{
            .key = try alloc.dupe(u8, item.string),
            .fingerprint = "",
        };
        initialized += 1;
    }
    return out;
}

fn documentExtractionStateKeysAlloc(alloc: Allocator, state: []const u8, field_name: []const u8) ![]const []const u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, state, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return try alloc.alloc([]const u8, 0);
    const keys_value = parsed.value.object.get(field_name) orelse return try alloc.alloc([]const u8, 0);
    if (keys_value != .array) return try alloc.alloc([]const u8, 0);
    const out = try alloc.alloc([]const u8, keys_value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |key| alloc.free(@constCast(key));
        alloc.free(out);
    }
    for (keys_value.array.items, 0..) |item, i| {
        if (item != .string) return error.InvalidDocumentExtractionState;
        out[i] = try alloc.dupe(u8, item.string);
        initialized += 1;
    }
    return out;
}

fn documentExtractionUnitKeyStillPresent(
    alloc: Allocator,
    doc_key: []const u8,
    artifact_name: []const u8,
    previous_key: []const u8,
    units: []const document_extraction_mod.Unit,
) !bool {
    for (units) |unit| {
        const key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, doc_key, artifact_name, unit.unit_id);
        defer alloc.free(key);
        if (std.mem.eql(u8, previous_key, key)) return true;
    }
    return false;
}

pub fn documentUnitPayloadAlloc(
    alloc: Allocator,
    doc_key: []const u8,
    artifact_name: []const u8,
    unit: document_extraction_mod.Unit,
    source_url: []const u8,
    content_type: []const u8,
    route: DocumentExtractionRangeRoute,
) ![]u8 {
    const owner_group_id = std.math.cast(i64, route.owner_group_id) orelse return error.InvalidDocumentExtractionManifest;
    return try std.json.Stringify.valueAlloc(alloc, .{
        ._parent_doc_key = doc_key,
        ._artifact_name = artifact_name,
        ._artifact_range_id = route.range_id,
        ._artifact_range_kind = "unit",
        ._artifact_route_status = route.route_status,
        ._artifact_owner_group_id = owner_group_id,
        .unit_id = unit.unit_id,
        .unit_type = unit.unit_type,
        .text = unit.text,
        .content_type = "text/plain",
        .language = "",
        .source_path = unit.source_path,
        .extraction_status = unit.extraction_status,
        .source_sha256 = unit.source_sha256,
        .byte_length = unit.byte_length,
        .confidence = documentUnitConfidence(unit),
        .ocr_confidence = unit.ocr_confidence,
        .ocr_bbox = unit.ocr_bbox,
        .transcript_confidence = unit.transcript_confidence,
        .extraction_warning = unit.extraction_warning,
        .provenance = .{
            .source_url = source_url,
            .source_path = unit.source_path,
            .method = unit.method,
            .extraction_status = unit.extraction_status,
            .source_sha256 = unit.source_sha256,
            .byte_length = unit.byte_length,
            .confidence = documentUnitConfidence(unit),
            .ocr_used = unit.ocr_used,
            .ocr_confidence = unit.ocr_confidence,
            .ocr_bbox = unit.ocr_bbox,
            .transcript_used = unit.transcript_used,
            .transcript_confidence = unit.transcript_confidence,
            .extraction_warning = unit.extraction_warning,
            .page_number = unit.page_number,
            .page_label = unit.page_label,
            .page_bbox = unit.page_bbox,
            .page_rotation = unit.page_rotation,
            .text_regions = unit.text_regions,
            .char_start = unit.char_start,
            .char_end = unit.char_end,
            .source_content_type = content_type,
            .format_provenance = .{
                .schema = "antfly.document_format_provenance.v1",
                .source_content_type = content_type,
                .source_path = unit.source_path,
                .coordinate_system = "source_page_points",
                .extraction_method = unit.method,
                .extraction_status = unit.extraction_status,
                .source_sha256 = unit.source_sha256,
                .byte_length = unit.byte_length,
                .confidence = documentUnitConfidence(unit),
                .ocr_used = unit.ocr_used,
                .ocr_confidence = unit.ocr_confidence,
                .ocr_bbox = unit.ocr_bbox,
                .transcript_used = unit.transcript_used,
                .transcript_confidence = unit.transcript_confidence,
                .extraction_warning = unit.extraction_warning,
                .page_number = unit.page_number,
                .page_label = unit.page_label,
                .page_bbox = unit.page_bbox,
                .page_rotation = unit.page_rotation,
                .text_regions = unit.text_regions,
            },
        },
    }, .{});
}

fn documentUnitConfidence(unit: document_extraction_mod.Unit) ?f64 {
    return unit.ocr_confidence orelse unit.transcript_confidence;
}

const document_extraction_range_target_children = 256;
const document_extraction_range_target_text_bytes = 1024 * 1024;

fn documentExtractionRangeCount(key_count: usize) usize {
    if (key_count == 0) return 0;
    return (key_count + document_extraction_range_target_children - 1) / document_extraction_range_target_children;
}

fn documentExtractionRangeEnd(
    key_count: usize,
    units: []const document_extraction_mod.Unit,
    start: usize,
) usize {
    var end = start;
    var text_bytes: usize = 0;
    const use_text_limit = units.len == key_count;
    while (end < key_count and end - start < document_extraction_range_target_children) {
        if (use_text_limit) {
            const unit_bytes = units[end].text.len;
            if (end > start and text_bytes + unit_bytes > document_extraction_range_target_text_bytes) break;
            text_bytes += unit_bytes;
        }
        end += 1;
    }
    return end;
}

pub fn documentExtractionUnitRangeCount(units: []const document_extraction_mod.Unit) usize {
    var count: usize = 0;
    var start: usize = 0;
    while (start < units.len) {
        count += 1;
        start = documentExtractionRangeEnd(units.len, units, start);
    }
    return count;
}

pub fn documentExtractionUnitRangeIndex(units: []const document_extraction_mod.Unit, unit_index: usize) usize {
    var range_index: usize = 0;
    var start: usize = 0;
    while (start < units.len) : (range_index += 1) {
        const end = documentExtractionRangeEnd(units.len, units, start);
        if (unit_index < end) return range_index;
        start = end;
    }
    return range_index;
}

fn documentExtractionRangeIdAlloc(alloc: Allocator, range_index: usize) ![]u8 {
    return try std.fmt.allocPrint(alloc, "range:{d:0>6}", .{range_index});
}

fn documentExtractionKeyIndex(keys: []const []const u8, key: []const u8) ?usize {
    for (keys, 0..) |candidate, i| {
        if (std.mem.eql(u8, candidate, key)) return i;
    }
    return null;
}

fn documentExtractionManifestGeneration(alloc: Allocator, manifest: []const u8) !u64 {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, manifest, .{}) catch return 0;
    defer parsed.deinit();
    if (parsed.value != .object) return 0;
    const generation = parsed.value.object.get("generation") orelse return 0;
    if (generation != .integer or generation.integer < 0) return error.InvalidDocumentExtractionManifest;
    return std.math.cast(u64, generation.integer) orelse return error.InvalidDocumentExtractionManifest;
}

fn documentExtractionManifestHasLastError(alloc: Allocator, manifest: []const u8) !bool {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, manifest, .{}) catch return false;
    defer parsed.deinit();
    if (parsed.value != .object) return false;
    return parsed.value.object.contains("last_error");
}

fn appendDocumentExtractionRangePolicy(alloc: Allocator, out: *std.ArrayListUnmanaged(u8)) !void {
    var first = true;
    try out.append(alloc, '{');
    try appendJsonFieldU64(alloc, out, &first, "policy_version", 1);
    try appendJsonFieldUsize(alloc, out, &first, "unit_target_children", document_extraction_range_target_children);
    try appendJsonFieldUsize(alloc, out, &first, "unit_target_text_bytes", document_extraction_range_target_text_bytes);
    try appendJsonFieldUsize(alloc, out, &first, "chunk_target_children", document_extraction_range_target_children);
    try appendJsonFieldString(alloc, out, &first, "oversized_unit_policy", "single_unit_range");
    try out.append(alloc, '}');
}

fn appendDocumentExtractionRangeDescriptors(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    artifact_name: []const u8,
    unit_keys: []const []const u8,
    chunk_keys: []const []const u8,
    units: []const document_extraction_mod.Unit,
    previous_child_ranges: []const types.DocumentArtifactChildRange,
) !void {
    var first_range = true;
    var range_index: usize = 0;
    try appendDocumentExtractionKeyRanges(alloc, out, &first_range, &range_index, "unit", artifact_name, unit_keys, units, previous_child_ranges);
    try appendDocumentExtractionKeyRanges(alloc, out, &first_range, &range_index, "chunk", "derived_chunks", chunk_keys, &.{}, previous_child_ranges);
}

fn appendDocumentExtractionExistingRanges(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    ranges: []const types.DocumentArtifactChildRange,
) !void {
    for (ranges, 0..) |range, i| {
        if (i > 0) try out.append(alloc, ',');
        var first = true;
        try out.append(alloc, '{');
        try appendJsonFieldString(alloc, out, &first, "range_id", range.range_id);
        try appendJsonFieldString(alloc, out, &first, "range_kind", range.range_kind);
        try appendJsonFieldString(alloc, out, &first, "artifact_name", range.artifact_name);
        try appendJsonFieldString(alloc, out, &first, "split_boundary", range.split_boundary);
        try appendJsonFieldString(alloc, out, &first, "placement", range.placement);
        if (range.owner_group_id) |value| try appendJsonFieldU64(alloc, out, &first, "owner_group_id", value);
        if (range.placement_generation) |value| try appendJsonFieldU64(alloc, out, &first, "placement_generation", value);
        if (range.route_status) |value| try appendJsonFieldString(alloc, out, &first, "route_status", value);
        if (range.split_eligible) |value| try appendJsonFieldBool(alloc, out, &first, "split_eligible", value);
        try appendJsonFieldString(alloc, out, &first, "start_key", range.start_key);
        try appendJsonFieldString(alloc, out, &first, "end_key_exclusive", range.end_key_exclusive);
        try appendJsonFieldString(alloc, out, &first, "last_key", range.last_key);
        try appendJsonFieldUsize(alloc, out, &first, "child_count", range.child_count);
        if (range.text_bytes) |value| try appendJsonFieldUsize(alloc, out, &first, "text_bytes", value);
        try out.append(alloc, '}');
    }
}

fn appendDocumentExtractionKeyRanges(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first_range: *bool,
    range_index: *usize,
    range_kind: []const u8,
    artifact_name: []const u8,
    keys: []const []const u8,
    units: []const document_extraction_mod.Unit,
    previous_child_ranges: []const types.DocumentArtifactChildRange,
) !void {
    var start: usize = 0;
    while (start < keys.len) {
        const end = documentExtractionRangeEnd(keys.len, units, start);
        if (first_range.*) {
            first_range.* = false;
        } else {
            try out.append(alloc, ',');
        }
        var first = true;
        try out.append(alloc, '{');
        const range_id = try documentExtractionRangeIdAlloc(alloc, range_index.*);
        defer alloc.free(range_id);
        const previous_range = findDocumentArtifactChildRange(previous_child_ranges, range_id, range_kind, artifact_name);
        try appendJsonFieldString(alloc, out, &first, "range_id", range_id);
        try appendJsonFieldString(alloc, out, &first, "range_kind", range_kind);
        try appendJsonFieldString(alloc, out, &first, "artifact_name", artifact_name);
        try appendJsonFieldString(alloc, out, &first, "split_boundary", documentExtractionSplitBoundary(range_kind));
        try appendJsonFieldString(alloc, out, &first, "placement", if (previous_range) |range| range.placement else "parent");
        try appendJsonFieldU64(alloc, out, &first, "owner_group_id", if (previous_range) |range| range.owner_group_id orelse 0 else 0);
        try appendJsonFieldU64(alloc, out, &first, "placement_generation", if (previous_range) |range| range.placement_generation orelse 0 else 0);
        try appendJsonFieldString(alloc, out, &first, "route_status", if (previous_range) |range| range.route_status orelse "local_committed" else "local_committed");
        try appendJsonFieldBool(alloc, out, &first, "split_eligible", if (previous_range) |range| range.split_eligible orelse (end - start > 1) else end - start > 1);
        try appendJsonFieldString(alloc, out, &first, "start_key", keys[start]);
        try appendJsonFieldString(alloc, out, &first, "end_key_exclusive", if (end < keys.len) keys[end] else "");
        try appendJsonFieldString(alloc, out, &first, "last_key", keys[end - 1]);
        try appendJsonFieldUsize(alloc, out, &first, "child_count", end - start);
        if (units.len >= end) {
            var text_bytes: usize = 0;
            for (units[start..end]) |unit| text_bytes += unit.text.len;
            try appendJsonFieldUsize(alloc, out, &first, "text_bytes", text_bytes);
        }
        try out.append(alloc, '}');
        range_index.* += 1;
        start = end;
    }
}

fn documentExtractionSplitBoundary(range_kind: []const u8) []const u8 {
    if (std.mem.eql(u8, range_kind, "chunk")) return "chunk";
    return "unit";
}

fn findDocumentArtifactChildRange(
    ranges: []const types.DocumentArtifactChildRange,
    range_id: []const u8,
    range_kind: []const u8,
    artifact_name: []const u8,
) ?*const types.DocumentArtifactChildRange {
    for (ranges) |*range| {
        if (std.mem.eql(u8, range.range_id, range_id) and
            std.mem.eql(u8, range.range_kind, range_kind) and
            std.mem.eql(u8, range.artifact_name, artifact_name))
        {
            return range;
        }
    }
    return null;
}

fn documentExtractionRangeRoute(
    ranges: []const types.DocumentArtifactChildRange,
    range_id: []const u8,
    range_kind: []const u8,
    artifact_name: []const u8,
) DocumentExtractionRangeRoute {
    const range = findDocumentArtifactChildRange(ranges, range_id, range_kind, artifact_name) orelse return .{ .range_id = range_id };
    return .{
        .range_id = range_id,
        .route_status = range.route_status orelse "local_committed",
        .owner_group_id = range.owner_group_id orelse 0,
    };
}

fn unitDescriptorFingerprintMatches(descriptors: []const DocumentExtractionUnitDescriptor, key: []const u8, fingerprint: []const u8) bool {
    if (fingerprint.len == 0) return false;
    for (descriptors) |descriptor| {
        if (std.mem.eql(u8, descriptor.key, key) and std.mem.eql(u8, descriptor.fingerprint, fingerprint)) return true;
    }
    return false;
}

fn countUnitDescriptorsByFingerprintMatch(
    descriptors: []const DocumentExtractionUnitDescriptor,
    comparison: []const DocumentExtractionUnitDescriptor,
    want_match: bool,
) usize {
    var count: usize = 0;
    for (descriptors) |descriptor| {
        const matched = unitDescriptorFingerprintMatches(comparison, descriptor.key, descriptor.fingerprint);
        if (matched == want_match) count += 1;
    }
    return count;
}

fn appendDocumentExtractionUnitMergeOperation(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first_operation: *bool,
    op: []const u8,
    artifact_name: []const u8,
    descriptors: []const DocumentExtractionUnitDescriptor,
    comparison: []const DocumentExtractionUnitDescriptor,
    want_fingerprint_match: bool,
) !void {
    const count = countUnitDescriptorsByFingerprintMatch(descriptors, comparison, want_fingerprint_match);
    if (count == 0) return;

    var first_key: ?[]const u8 = null;
    var last_key: ?[]const u8 = null;
    for (descriptors) |descriptor| {
        const matched = unitDescriptorFingerprintMatches(comparison, descriptor.key, descriptor.fingerprint);
        if (matched != want_fingerprint_match) continue;
        if (first_key == null) first_key = descriptor.key;
        last_key = descriptor.key;
    }

    if (first_operation.*) {
        first_operation.* = false;
    } else {
        try out.append(alloc, ',');
    }

    var first = true;
    try out.append(alloc, '{');
    try appendJsonFieldString(alloc, out, &first, "op", op);
    try appendJsonFieldString(alloc, out, &first, "range_kind", "unit");
    try appendJsonFieldString(alloc, out, &first, "artifact_name", artifact_name);
    try appendJsonFieldString(alloc, out, &first, "first_key", first_key.?);
    try appendJsonFieldString(alloc, out, &first, "last_key", last_key.?);
    try appendJsonFieldUsize(alloc, out, &first, "key_count", count);
    try appendJsonFieldBool(alloc, out, &first, "fingerprint_match", want_fingerprint_match);
    try out.append(alloc, '}');
}

fn countKeysNotIn(keys: []const []const u8, exclude_keys: []const []const u8) usize {
    var count: usize = 0;
    for (keys) |key| {
        if (!containsDeleteKey(exclude_keys, key)) count += 1;
    }
    return count;
}

fn appendDocumentExtractionMergeOperation(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first_operation: *bool,
    op: []const u8,
    range_kind: []const u8,
    artifact_name: []const u8,
    keys: []const []const u8,
    exclude_keys: []const []const u8,
) !void {
    const count = countKeysNotIn(keys, exclude_keys);
    if (count == 0) return;

    var first_key: ?[]const u8 = null;
    var last_key: ?[]const u8 = null;
    for (keys) |key| {
        if (containsDeleteKey(exclude_keys, key)) continue;
        if (first_key == null) first_key = key;
        last_key = key;
    }

    if (first_operation.*) {
        first_operation.* = false;
    } else {
        try out.append(alloc, ',');
    }

    var first = true;
    try out.append(alloc, '{');
    try appendJsonFieldString(alloc, out, &first, "op", op);
    try appendJsonFieldString(alloc, out, &first, "range_kind", range_kind);
    try appendJsonFieldString(alloc, out, &first, "artifact_name", artifact_name);
    try appendJsonFieldString(alloc, out, &first, "first_key", first_key.?);
    try appendJsonFieldString(alloc, out, &first, "last_key", last_key.?);
    try appendJsonFieldUsize(alloc, out, &first, "key_count", count);
    try out.append(alloc, '}');
}

const DocumentExtractionLastError = struct {
    code: []const u8,
    message: []const u8,
};

fn documentExtractionManifestPayloadAllocWithError(
    alloc: Allocator,
    doc_key: []const u8,
    artifact_name: []const u8,
    source_url: []const u8,
    fingerprint: []const u8,
    extraction: document_extraction_mod.Result,
    unit_keys: []const []const u8,
    unit_descriptors: []const DocumentExtractionUnitDescriptor,
    chunk_keys: []const []const u8,
    previous_child_ranges: []const types.DocumentArtifactChildRange,
    previous_unit_keys: []const []const u8,
    previous_unit_descriptors: []const DocumentExtractionUnitDescriptor,
    previous_chunk_keys: []const []const u8,
    child_ranges_override: []const types.DocumentArtifactChildRange,
    manifest_generation: u64,
    from_generation: u64,
    to_generation: u64,
    merge_status: []const u8,
    last_error: ?DocumentExtractionLastError,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    var first = true;
    try out.append(alloc, '{');
    try appendJsonFieldString(alloc, &out, &first, "_parent_doc_key", doc_key);
    try appendJsonFieldString(alloc, &out, &first, "_artifact_name", artifact_name);
    try appendJsonFieldString(alloc, &out, &first, "artifact_type", "document_units");
    try appendJsonFieldU64(alloc, &out, &first, "manifest_version", 2);
    try appendJsonFieldU64(alloc, &out, &first, "generation", manifest_generation);
    try appendJsonFieldString(alloc, &out, &first, "source_url", source_url);
    try appendJsonFieldString(alloc, &out, &first, "source_fingerprint", fingerprint);
    try appendJsonFieldString(alloc, &out, &first, "content_type", extraction.content_type);
    try appendJsonFieldString(alloc, &out, &first, "route_type", extraction.route_type);
    if (extraction.unsupported_reason.len > 0) {
        try appendJsonFieldString(alloc, &out, &first, "unsupported_reason", extraction.unsupported_reason);
    }
    if (last_error) |value| {
        try appendJsonFieldName(alloc, &out, &first, "last_error");
        try out.append(alloc, '{');
        var error_first = true;
        try appendJsonFieldString(alloc, &out, &error_first, "code", value.code);
        try appendJsonFieldString(alloc, &out, &error_first, "message", value.message);
        try out.append(alloc, '}');
    }
    try appendJsonFieldUsize(alloc, &out, &first, "unit_count", extraction.units.len);
    try appendJsonFieldUsize(alloc, &out, &first, "chunk_count", chunk_keys.len);
    try appendJsonFieldName(alloc, &out, &first, "child_ranges");
    try out.append(alloc, '[');
    if (child_ranges_override.len > 0) {
        try appendDocumentExtractionExistingRanges(alloc, &out, child_ranges_override);
    } else {
        try appendDocumentExtractionRangeDescriptors(alloc, &out, artifact_name, unit_keys, chunk_keys, extraction.units, previous_child_ranges);
    }
    try out.append(alloc, ']');
    try appendJsonFieldName(alloc, &out, &first, "range_policy");
    try appendDocumentExtractionRangePolicy(alloc, &out);
    try appendJsonFieldName(alloc, &out, &first, "merge_plan");
    try out.append(alloc, '{');
    var merge_first = true;
    try appendJsonFieldU64(alloc, &out, &merge_first, "plan_version", 1);
    try appendJsonFieldU64(alloc, &out, &merge_first, "from_generation", from_generation);
    try appendJsonFieldU64(alloc, &out, &merge_first, "to_generation", to_generation);
    try appendJsonFieldString(alloc, &out, &merge_first, "status", merge_status);
    try appendJsonFieldString(alloc, &out, &merge_first, "operation_granularity", "unit_fingerprint");
    try appendJsonFieldName(alloc, &out, &merge_first, "operations");
    try out.append(alloc, '[');
    var first_operation = true;
    try appendDocumentExtractionUnitMergeOperation(alloc, &out, &first_operation, "keep", artifact_name, unit_descriptors, previous_unit_descriptors, true);
    try appendDocumentExtractionUnitMergeOperation(alloc, &out, &first_operation, "upsert", artifact_name, unit_descriptors, previous_unit_descriptors, false);
    try appendDocumentExtractionMergeOperation(alloc, &out, &first_operation, "upsert", "chunk", "derived_chunks", chunk_keys, &.{});
    try appendDocumentExtractionMergeOperation(alloc, &out, &first_operation, "delete", "unit", artifact_name, previous_unit_keys, unit_keys);
    try appendDocumentExtractionMergeOperation(alloc, &out, &first_operation, "delete", "chunk", "derived_chunks", previous_chunk_keys, chunk_keys);
    try out.append(alloc, ']');
    try out.append(alloc, '}');
    try appendJsonFieldName(alloc, &out, &first, "coverage_plan");
    try out.append(alloc, '{');
    var coverage_first = true;
    try appendJsonFieldU64(alloc, &out, &coverage_first, "plan_version", 1);
    try appendJsonFieldString(alloc, &out, &coverage_first, "full_text_replay", "stored_artifact_required");
    try appendJsonFieldBool(alloc, &out, &coverage_first, "full_text_replay_suppressed", false);
    try appendJsonFieldBool(alloc, &out, &coverage_first, "watermark_required_before_suppression", true);
    try out.append(alloc, '}');
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

pub fn documentExtractionManifestPayloadAlloc(
    alloc: Allocator,
    doc_key: []const u8,
    artifact_name: []const u8,
    source_url: []const u8,
    fingerprint: []const u8,
    extraction: document_extraction_mod.Result,
    unit_keys: []const []const u8,
    unit_descriptors: []const DocumentExtractionUnitDescriptor,
    chunk_keys: []const []const u8,
    previous_child_ranges: []const types.DocumentArtifactChildRange,
    previous_unit_keys: []const []const u8,
    previous_unit_descriptors: []const DocumentExtractionUnitDescriptor,
    previous_chunk_keys: []const []const u8,
    manifest_generation: u64,
    from_generation: u64,
    to_generation: u64,
    merge_status: []const u8,
) ![]u8 {
    return try documentExtractionManifestPayloadAllocWithError(
        alloc,
        doc_key,
        artifact_name,
        source_url,
        fingerprint,
        extraction,
        unit_keys,
        unit_descriptors,
        chunk_keys,
        previous_child_ranges,
        previous_unit_keys,
        previous_unit_descriptors,
        previous_chunk_keys,
        &.{},
        manifest_generation,
        from_generation,
        to_generation,
        merge_status,
        null,
    );
}

fn appendDocumentExtractionFailureManifest(
    alloc: Allocator,
    doc_key: []const u8,
    artifact_name: []const u8,
    source_url: []const u8,
    manifest_key: []const u8,
    state_key: []const u8,
    existing_state: ?[]const u8,
    from_generation: u64,
    to_generation: u64,
    error_code: []const u8,
    error_message: []const u8,
    artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
    artifact_delete_keys: *std.ArrayListUnmanaged([]const u8),
) !void {
    var previous_unit_keys: []const []const u8 = &.{};
    defer freeOwnedConstKeySlice(alloc, previous_unit_keys);
    var previous_unit_descriptors: []DocumentExtractionUnitDescriptor = &.{};
    defer freeDocumentExtractionUnitDescriptors(alloc, previous_unit_descriptors);
    var previous_chunk_keys: []const []const u8 = &.{};
    defer freeOwnedConstKeySlice(alloc, previous_chunk_keys);
    if (existing_state) |state| {
        previous_unit_keys = try documentExtractionStateUnitKeysAlloc(alloc, state);
        previous_unit_descriptors = try documentExtractionStateUnitDescriptorsAlloc(alloc, state);
        previous_chunk_keys = try documentExtractionStateChunkKeysAlloc(alloc, state);
    }

    const empty_units: [0]document_extraction_mod.Unit = .{};
    const failed_extraction = document_extraction_mod.Result{
        .content_type = @constCast("application/octet-stream"),
        .route_type = @constCast("error"),
        .units = @constCast(empty_units[0..]),
    };
    const manifest = try documentExtractionManifestPayloadAllocWithError(
        alloc,
        doc_key,
        artifact_name,
        source_url,
        error_code,
        failed_extraction,
        &.{},
        &.{},
        &.{},
        &.{},
        previous_unit_keys,
        previous_unit_descriptors,
        previous_chunk_keys,
        &.{},
        to_generation,
        from_generation,
        to_generation,
        "failed",
        .{ .code = error_code, .message = error_message },
    );
    defer alloc.free(manifest);

    try artifact_writes.append(alloc, .{
        .key = try alloc.dupe(u8, manifest_key),
        .value = try alloc.dupe(u8, manifest),
    });
    try artifact_delete_keys.append(alloc, try alloc.dupe(u8, state_key));
    for (previous_unit_keys) |previous_key| {
        try artifact_delete_keys.append(alloc, try alloc.dupe(u8, previous_key));
    }
    for (previous_chunk_keys) |previous_key| {
        try artifact_delete_keys.append(alloc, try alloc.dupe(u8, previous_key));
    }
}

pub fn appendUniqueOwnedKey(alloc: Allocator, list: *std.ArrayListUnmanaged([]u8), key: []const u8) !void {
    for (list.items) |existing| {
        if (std.mem.eql(u8, existing, key)) return;
    }
    try list.append(alloc, try alloc.dupe(u8, key));
}

pub fn containsOwnedKey(list: []const []u8, key: []const u8) bool {
    for (list) |existing| {
        if (std.mem.eql(u8, existing, key)) return true;
    }
    return false;
}

fn appendUniqueReplayRecordKeyWithSet(
    alloc: Allocator,
    list: *std.ArrayListUnmanaged([]const u8),
    seen: *std.StringHashMapUnmanaged(void),
    key: []const u8,
) !void {
    if (key.len == 0) return;
    if (seen.contains(key)) return;
    const owned = try alloc.dupe(u8, key);
    errdefer alloc.free(owned);
    try seen.put(alloc, owned, {});
    errdefer _ = seen.remove(owned);
    try list.append(alloc, owned);
}

fn appendUniqueReplayRecordHint(
    alloc: Allocator,
    list: *std.ArrayListUnmanaged(change_journal_mod.TargetHint),
    hint: change_journal_mod.TargetHint,
) !void {
    for (list.items) |existing| {
        if (existing == hint) return;
    }
    try list.append(alloc, hint);
}

pub fn encodeThinReplayRecordPayload(
    alloc: Allocator,
    req: types.BatchRequest,
    extracted: []const mapper.ExtractedWrite,
    deleted_artifact_keys: []const []u8,
    changed_artifact_keys: []const []u8,
    overwritten_flags: []const bool,
    sequence: u64,
    include_generated_enrichment_hint: bool,
) ![]u8 {
    var changed_doc_keys = std.ArrayListUnmanaged([]const u8).empty;
    var changed_doc_key_set = std.StringHashMapUnmanaged(void).empty;
    errdefer {
        for (changed_doc_keys.items) |key| alloc.free(@constCast(key));
        changed_doc_keys.deinit(alloc);
    }
    var deleted_doc_keys = std.ArrayListUnmanaged([]const u8).empty;
    var deleted_doc_key_set = std.StringHashMapUnmanaged(void).empty;
    errdefer {
        for (deleted_doc_keys.items) |key| alloc.free(@constCast(key));
        deleted_doc_keys.deinit(alloc);
    }
    var overwritten_doc_keys = std.ArrayListUnmanaged([]const u8).empty;
    var overwritten_doc_key_set = std.StringHashMapUnmanaged(void).empty;
    errdefer {
        for (overwritten_doc_keys.items) |key| alloc.free(@constCast(key));
        overwritten_doc_keys.deinit(alloc);
    }
    var thin_changed_artifact_keys = std.ArrayListUnmanaged([]const u8).empty;
    var thin_changed_artifact_key_set = std.StringHashMapUnmanaged(void).empty;
    errdefer {
        for (thin_changed_artifact_keys.items) |key| alloc.free(@constCast(key));
        thin_changed_artifact_keys.deinit(alloc);
    }
    var target_hints = std.ArrayListUnmanaged(change_journal_mod.TargetHint).empty;
    errdefer target_hints.deinit(alloc);
    defer {
        changed_doc_key_set.deinit(alloc);
        deleted_doc_key_set.deinit(alloc);
        overwritten_doc_key_set.deinit(alloc);
        thin_changed_artifact_key_set.deinit(alloc);
    }

    var saw_overwritten = false;

    for (req.writes, 0..) |write, i| {
        const extracted_write = extracted[i];
        if (extracted_write.cleaned_value != null or include_generated_enrichment_hint) {
            try appendUniqueReplayRecordKeyWithSet(alloc, &changed_doc_keys, &changed_doc_key_set, write.key);
            if (extracted_write.cleaned_value != null) {
                try appendUniqueReplayRecordHint(alloc, &target_hints, .full_text);
                try appendUniqueReplayRecordHint(alloc, &target_hints, .algebraic);
            }
            if (include_generated_enrichment_hint) {
                try appendUniqueReplayRecordHint(alloc, &target_hints, .enrichment);
            }
        }

        for (extracted_write.dense_embeddings) |embedding| {
            if (embedding.artifact_key) |artifact_key| try appendUniqueReplayRecordKeyWithSet(alloc, &thin_changed_artifact_keys, &thin_changed_artifact_key_set, artifact_key);
            try appendUniqueReplayRecordHint(alloc, &target_hints, .dense_vector);
        }
        for (extracted_write.sparse_embeddings) |embedding| {
            if (embedding.artifact_key) |artifact_key| try appendUniqueReplayRecordKeyWithSet(alloc, &thin_changed_artifact_keys, &thin_changed_artifact_key_set, artifact_key);
            try appendUniqueReplayRecordHint(alloc, &target_hints, .sparse_vector);
        }
        if (extracted_write.mentioned_graph_indexes.len > 0 or extracted_write.graph_writes.len > 0) {
            try appendUniqueReplayRecordKeyWithSet(alloc, &changed_doc_keys, &changed_doc_key_set, write.key);
            try appendUniqueReplayRecordHint(alloc, &target_hints, .graph);
        }
        if (overwritten_flags[i] and extracted_write.cleaned_value != null) {
            try appendUniqueReplayRecordKeyWithSet(alloc, &overwritten_doc_keys, &overwritten_doc_key_set, write.key);
            saw_overwritten = true;
        }
    }

    if (saw_overwritten) {
        try appendUniqueReplayRecordHint(alloc, &target_hints, .full_text);
        try appendUniqueReplayRecordHint(alloc, &target_hints, .dense_vector);
        try appendUniqueReplayRecordHint(alloc, &target_hints, .sparse_vector);
        try appendUniqueReplayRecordHint(alloc, &target_hints, .algebraic);
    }

    for (changed_artifact_keys) |key| {
        try appendUniqueReplayRecordKeyWithSet(alloc, &thin_changed_artifact_keys, &thin_changed_artifact_key_set, key);
        if (internal_keys.isChunkArtifactRecordKey(key)) try appendUniqueReplayRecordHint(alloc, &target_hints, .full_text);
        if (internal_keys.isGraphEdgeArtifactKey(key) or internal_keys.isAssetArtifactKey(key) or internal_keys.isChunkArtifactRecordKey(key)) try appendUniqueReplayRecordHint(alloc, &target_hints, .graph);
        if (internal_keys.isAssetArtifactKey(key)) {
            try appendUniqueReplayRecordHint(alloc, &target_hints, .resolution);
        }
    }

    for (req.graph_writes) |write| {
        try appendUniqueReplayRecordKeyWithSet(alloc, &changed_doc_keys, &changed_doc_key_set, write.source);
        const artifact_key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, write.source, write.index_name, write.edge_type, write.target);
        defer alloc.free(artifact_key);
        try appendUniqueReplayRecordKeyWithSet(alloc, &thin_changed_artifact_keys, &thin_changed_artifact_key_set, artifact_key);
        try appendUniqueReplayRecordHint(alloc, &target_hints, .graph);
    }
    for (req.graph_deletes) |delete| {
        try appendUniqueReplayRecordKeyWithSet(alloc, &deleted_doc_keys, &deleted_doc_key_set, delete.source);
        const artifact_key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, delete.source, delete.index_name, delete.edge_type, delete.target);
        defer alloc.free(artifact_key);
        try appendUniqueReplayRecordKeyWithSet(alloc, &thin_changed_artifact_keys, &thin_changed_artifact_key_set, artifact_key);
        try appendUniqueReplayRecordHint(alloc, &target_hints, .graph);
    }

    for (req.deletes) |key| {
        try appendUniqueReplayRecordKeyWithSet(alloc, &deleted_doc_keys, &deleted_doc_key_set, key);
        try appendUniqueReplayRecordHint(alloc, &target_hints, .algebraic);
    }
    for (deleted_artifact_keys) |key| {
        try appendUniqueReplayRecordKeyWithSet(alloc, &deleted_doc_keys, &deleted_doc_key_set, key);
        if (internal_keys.isAssetArtifactKey(key) or internal_keys.isGraphEdgeArtifactKey(key)) {
            try appendUniqueReplayRecordKeyWithSet(alloc, &thin_changed_artifact_keys, &thin_changed_artifact_key_set, key);
            if (internal_keys.isChunkArtifactRecordKey(key)) try appendUniqueReplayRecordHint(alloc, &target_hints, .full_text);
            try appendUniqueReplayRecordHint(alloc, &target_hints, .graph);
            if (internal_keys.isAssetArtifactKey(key)) try appendUniqueReplayRecordHint(alloc, &target_hints, .resolution);
        }
    }

    var record: change_journal_mod.Record = .{
        .sequence = sequence,
        .changed_doc_keys = try changed_doc_keys.toOwnedSlice(alloc),
        .deleted_doc_keys = try deleted_doc_keys.toOwnedSlice(alloc),
        .overwritten_doc_keys = try overwritten_doc_keys.toOwnedSlice(alloc),
        .changed_artifact_keys = try thin_changed_artifact_keys.toOwnedSlice(alloc),
        .target_hints = try target_hints.toOwnedSlice(alloc),
    };
    defer change_journal_mod.deinitRecord(alloc, &record);
    return try change_journal_mod.encodeRecord(alloc, record);
}

pub fn appendEmbeddingArtifactWrite(
    alloc: Allocator,
    artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
    base_key: []const u8,
    parent_doc_key: []const u8,
    artifact_name: []const u8,
    source_field: []const u8,
    source_key: ?[]const u8,
    source_hash: ?u64,
    vector: []const f32,
) ![]u8 {
    _ = parent_doc_key;
    _ = source_field;
    _ = source_key;
    const key = if (internal_keys.isInternalUserKey(base_key))
        try internal_keys.derivedEmbeddingArtifactKeyAlloc(alloc, base_key, artifact_name)
    else
        try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, base_key, artifact_name);
    defer alloc.free(key);
    const payload = try enrichment_artifact_codec.encodeDenseEmbeddingAlloc(alloc, source_hash, vector);
    const owned_key = try alloc.dupe(u8, key);
    try artifact_writes.append(alloc, .{
        .key = owned_key,
        .value = payload,
    });
    return try alloc.dupe(u8, key);
}

pub fn appendSparseEmbeddingArtifactWrite(
    alloc: Allocator,
    artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
    base_key: []const u8,
    artifact_name: []const u8,
    source_hash: ?u64,
    indices: []const u32,
    values: []const f32,
) ![]u8 {
    const key = if (internal_keys.isInternalUserKey(base_key))
        try internal_keys.derivedEmbeddingArtifactKeyAlloc(alloc, base_key, artifact_name)
    else
        try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, base_key, artifact_name);
    defer alloc.free(key);
    const payload = try enrichment_artifact_codec.encodeSparseEmbeddingAlloc(alloc, source_hash, indices, values);
    const owned_key = try alloc.dupe(u8, key);
    try artifact_writes.append(alloc, .{
        .key = owned_key,
        .value = payload,
    });
    return try alloc.dupe(u8, key);
}

pub fn appendGraphEdgeArtifactWrite(
    alloc: Allocator,
    artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
    write: types.GraphEdgeWrite,
) !void {
    const key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, write.source, write.index_name, write.edge_type, write.target);
    defer alloc.free(key);
    const payload = try enrichment_artifact_codec.encodeGraphEdgeAlloc(alloc, null, write.weight, write.created_at, write.updated_at, write.metadata_json);
    const owned_key = try alloc.dupe(u8, key);
    try artifact_writes.append(alloc, .{
        .key = owned_key,
        .value = payload,
    });
}

pub fn containsBatchWriteKey(list: []const types.BatchWrite, key: []const u8) bool {
    for (list) |item| {
        if (std.mem.eql(u8, item.key, key)) return true;
    }
    return false;
}

pub const GraphArtifactClear = struct {
    doc_key: []u8,
    index_name: []u8,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.doc_key);
        alloc.free(self.index_name);
        self.* = undefined;
    }
};

const OverwriteProbeEntry = struct {
    key: []const u8,
    write_index: usize,
};

fn overwriteProbeLessThan(_: void, lhs: OverwriteProbeEntry, rhs: OverwriteProbeEntry) bool {
    return std.mem.order(u8, lhs.key, rhs.key) == .lt;
}

fn strippedStoredDocumentValueAlloc(
    alloc: Allocator,
    cleaned: []const u8,
    vector_store_field_names: []const []const u8,
    owned_values: *std.ArrayListUnmanaged([]u8),
) ![]const u8 {
    if (vector_store_field_names.len == 0) return cleaned;
    const stripped = (try mapper.stripTopLevelFieldsAlloc(alloc, cleaned, vector_store_field_names)) orelse try alloc.dupe(u8, "{}");
    errdefer alloc.free(stripped);
    try owned_values.append(alloc, stripped);
    return stripped;
}

const collectGraphArtifactsForDocIndex = db_internal.collectGraphArtifactsForDocIndex;

pub fn appendAssetArtifactSourceIndexMutations(
    alloc: Allocator,
    store_writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    deleted_artifact_keys: []const []const u8,
    delete_keys: *std.ArrayListUnmanaged([]const u8),
    owned_store_keys: *std.ArrayListUnmanaged([]u8),
    owned_store_values: *std.ArrayListUnmanaged([]u8),
    owned_delete_keys: *std.ArrayListUnmanaged([]u8),
) !void {
    const original_write_count = store_writes.items.len;
    var write_index: usize = 0;
    while (write_index < original_write_count) : (write_index += 1) {
        const write = store_writes.items[write_index];
        try appendAssetArtifactSourceIndexWrite(alloc, write.key, store_writes, owned_store_keys, owned_store_values);
    }

    const original_delete_count = delete_keys.items.len;
    var delete_index: usize = 0;
    while (delete_index < original_delete_count) : (delete_index += 1) {
        const key = delete_keys.items[delete_index];
        try appendAssetArtifactSourceIndexDelete(alloc, key, store_writes.items, delete_keys, owned_delete_keys);
    }
    for (deleted_artifact_keys) |key| {
        try appendAssetArtifactSourceIndexDelete(alloc, key, store_writes.items, delete_keys, owned_delete_keys);
    }
}

fn appendAssetArtifactSourceIndexWrite(
    alloc: Allocator,
    artifact_key: []const u8,
    store_writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    owned_store_keys: *std.ArrayListUnmanaged([]u8),
    owned_store_values: *std.ArrayListUnmanaged([]u8),
) !void {
    const parsed = (try internal_keys.parseAssetArtifactKeyAlloc(alloc, artifact_key)) orelse return;
    defer {
        alloc.free(parsed.doc_key);
        alloc.free(parsed.artifact_name);
    }

    const marker_key = try internal_keys.assetArtifactSourceIndexKeyAlloc(alloc, parsed.artifact_name, parsed.doc_key);
    var marker_key_owned = false;
    errdefer if (!marker_key_owned) alloc.free(marker_key);
    if (containsStoreWriteKey(store_writes.items, marker_key)) return;

    const marker_value = try alloc.dupe(u8, artifact_key);
    var marker_value_owned = false;
    errdefer if (!marker_value_owned) alloc.free(marker_value);

    try owned_store_keys.append(alloc, marker_key);
    marker_key_owned = true;
    try owned_store_values.append(alloc, marker_value);
    marker_value_owned = true;
    try store_writes.append(alloc, .{
        .key = marker_key,
        .value = marker_value,
    });
}

fn appendAssetArtifactSourceIndexDelete(
    alloc: Allocator,
    artifact_key: []const u8,
    store_writes: []const docstore_mod.KVPair,
    delete_keys: *std.ArrayListUnmanaged([]const u8),
    owned_delete_keys: *std.ArrayListUnmanaged([]u8),
) !void {
    const parsed = (try internal_keys.parseAssetArtifactKeyAlloc(alloc, artifact_key)) orelse return;
    defer {
        alloc.free(parsed.doc_key);
        alloc.free(parsed.artifact_name);
    }

    const marker_key = try internal_keys.assetArtifactSourceIndexKeyAlloc(alloc, parsed.artifact_name, parsed.doc_key);
    var marker_key_owned = false;
    errdefer if (!marker_key_owned) alloc.free(marker_key);
    if (containsStoreWriteKey(store_writes, marker_key)) return;
    if (containsOwnedKey(owned_delete_keys.items, marker_key)) return;

    try owned_delete_keys.append(alloc, marker_key);
    marker_key_owned = true;
    try delete_keys.append(alloc, marker_key);
}

pub fn generatedPrecomputeModeForSyncLevel(sync_level: types.SyncLevel) GeneratedPrecomputeMode {
    return switch (sync_level) {
        .enrichments, .aknn, .full_index => .all,
        .full_text => .full_text_only,
        .propose, .write => .none,
    };
}

pub const ChunkCacheEntry = struct {
    key: []u8,
    chunks: []chunker_mod.Chunk,
};

pub fn requestArtifactName(request: enrichment_types.GeneratedEnrichmentRequest) []const u8 {
    return if (request.artifact_name.len > 0) request.artifact_name else request.index_name;
}

fn requestMatchesForcedGeneratedArtifact(request: enrichment_types.GeneratedEnrichmentRequest, artifact_names: []const []const u8) bool {
    if (artifact_names.len == 0) return true;
    const artifact_name = requestArtifactName(request);
    for (artifact_names) |name| {
        if (std.mem.eql(u8, artifact_name, name)) return true;
    }
    return false;
}

fn requestEmbeddingName(request: enrichment_types.GeneratedEnrichmentRequest) []const u8 {
    return if (request.embedding_name.len > 0) request.embedding_name else request.index_name;
}

fn requestHasChunking(request: enrichment_types.GeneratedEnrichmentRequest) bool {
    return request.chunk_size > 0 or request.chunker_json.len > 0;
}

fn remoteRenderConfig(
    secret_store: ?*common_secrets.FileStore,
    remote_content: ?*const scraping.RemoteContentConfig,
) template_remote.RenderConfig {
    var config: template_remote.RenderConfig = .{};
    if (comptime @hasField(template_remote.RenderConfig, "secret_store")) {
        config.secret_store = secret_store;
    }
    if (comptime @hasField(template_remote.RenderConfig, "remote_content")) {
        config.remote_content = remote_content;
    }
    return config;
}

pub fn renderSourceTemplateText(
    alloc: Allocator,
    secret_store: ?*common_secrets.FileStore,
    remote_content: ?*const scraping.RemoteContentConfig,
    template_source: []const u8,
    doc_value: []const u8,
) ![]const u8 {
    if (comptime @hasDecl(template_remote, "renderJsonToValidatedTextWithConfig")) {
        return try template_remote.renderJsonToValidatedTextWithConfig(
            alloc,
            template_source,
            doc_value,
            remoteRenderConfig(secret_store, remote_content),
        );
    }
    return try template_remote.renderJsonToTextWithConfig(
        alloc,
        template_source,
        doc_value,
        remoteRenderConfig(secret_store, remote_content),
    );
}

fn renderSourceTemplateParts(
    alloc: Allocator,
    secret_store: ?*common_secrets.FileStore,
    remote_content: ?*const scraping.RemoteContentConfig,
    template_source: []const u8,
    doc_value: []const u8,
) ![]template_mod.ContentPart {
    if (comptime @hasDecl(template_remote, "renderJsonToPartsWithConfig")) {
        return try template_remote.renderJsonToPartsWithConfig(
            alloc,
            template_source,
            doc_value,
            remoteRenderConfig(secret_store, remote_content),
        );
    }
    return try template_remote.renderJsonToParts(alloc, template_source, doc_value);
}

fn extractStringField(alloc: Allocator, doc_value: []const u8, field_name: []const u8) !?[]u8 {
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, doc_value, .{});
    defer parsed.deinit();
    const field = jsonValueAtPath(parsed.value, field_name) orelse return null;
    if (field != .string) return null;
    return try alloc.dupe(u8, field.string);
}

const jsonValueAtPath = db_internal.jsonValueAtPath;

fn appendGraphFieldTargets(
    alloc: Allocator,
    targets: *std.ArrayListUnmanaged([]u8),
    root: std.json.Value,
    field_name: []const u8,
) !void {
    const field = jsonValueAtPath(root, field_name) orelse return;
    switch (field) {
        .string => try appendUniqueOwnedKey(alloc, targets, field.string),
        .array => {
            for (field.array.items) |item| {
                if (item != .string) return error.InvalidGraphEdges;
                try appendUniqueOwnedKey(alloc, targets, item.string);
            }
        },
        else => return error.InvalidGraphEdges,
    }
}

pub fn augmentExtractedWriteWithGraphFieldEdges(
    self: anytype,
    alloc: Allocator,
    key: []const u8,
    doc_value: []const u8,
    extracted: *mapper.ExtractedWrite,
) !void {
    if (!self.core.hasGraphIndexes() or extracted.cleaned_value == null) return;

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, doc_value, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return;

    var extra_writes = std.ArrayListUnmanaged(types.GraphEdgeWrite).empty;
    errdefer {
        for (extra_writes.items) |*write| {
            alloc.free(@constCast(write.index_name));
            alloc.free(@constCast(write.source));
            alloc.free(@constCast(write.target));
            alloc.free(@constCast(write.edge_type));
            if (write.metadata_json.len > 0) alloc.free(@constCast(write.metadata_json));
        }
        extra_writes.deinit(alloc);
    }

    var extra_indexes = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (extra_indexes.items) |index_name| alloc.free(index_name);
        extra_indexes.deinit(alloc);
    }

    for (self.core.graphIndexes()) |entry| {
        var index_has_field_edges = false;
        for (entry.edge_type_configs) |edge_cfg| {
            const field_name = edge_cfg.field_name orelse continue;
            index_has_field_edges = true;

            var targets = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (targets.items) |target| alloc.free(target);
                targets.deinit(alloc);
            }
            try appendGraphFieldTargets(alloc, &targets, parsed.value, field_name);

            for (targets.items) |target| {
                try extra_writes.append(alloc, .{
                    .index_name = try alloc.dupe(u8, entry.config.name),
                    .source = try alloc.dupe(u8, key),
                    .target = try alloc.dupe(u8, target),
                    .edge_type = try alloc.dupe(u8, edge_cfg.name),
                    .weight = 1.0,
                    .created_at = 0,
                    .updated_at = 0,
                    .metadata_json = "",
                });
            }
        }

        if (index_has_field_edges and
            !containsOwnedKey(extracted.mentioned_graph_indexes, entry.config.name) and
            !containsOwnedKey(extra_indexes.items, entry.config.name))
        {
            try extra_indexes.append(alloc, try alloc.dupe(u8, entry.config.name));
        }
    }

    if (extra_writes.items.len > 0) {
        const merged = try alloc.alloc(types.GraphEdgeWrite, extracted.graph_writes.len + extra_writes.items.len);
        @memcpy(merged[0..extracted.graph_writes.len], extracted.graph_writes);
        @memcpy(merged[extracted.graph_writes.len..], extra_writes.items);
        if (extracted.graph_writes.len > 0) alloc.free(extracted.graph_writes);
        extracted.graph_writes = merged;
    }
    extra_writes.deinit(alloc);

    if (extra_indexes.items.len > 0) {
        const merged = try alloc.alloc([]u8, extracted.mentioned_graph_indexes.len + extra_indexes.items.len);
        @memcpy(merged[0..extracted.mentioned_graph_indexes.len], extracted.mentioned_graph_indexes);
        @memcpy(merged[extracted.mentioned_graph_indexes.len..], extra_indexes.items);
        if (extracted.mentioned_graph_indexes.len > 0) alloc.free(extracted.mentioned_graph_indexes);
        extracted.mentioned_graph_indexes = merged;
    }
    extra_indexes.deinit(alloc);
}

pub fn makeChunkCacheKey(alloc: Allocator, request: enrichment_types.GeneratedEnrichmentRequest) ![]u8 {
    var chunk_size: [@sizeOf(u32)]u8 = undefined;
    var chunk_overlap: [@sizeOf(u32)]u8 = undefined;
    std.mem.writeInt(u32, &chunk_size, request.chunk_size, .big);
    std.mem.writeInt(u32, &chunk_overlap, request.chunk_overlap, .big);
    return try chunkCacheTupleKeyAlloc(alloc, &.{
        request.doc_key,
        requestArtifactName(request),
        request.source_field,
        &chunk_size,
        &chunk_overlap,
        request.chunker_json,
    });
}

fn chunkCacheTupleKeyAlloc(alloc: Allocator, components: []const []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    for (components) |component| {
        if (component.len > std.math.maxInt(u32)) return error.KeyComponentTooLarge;
        var len_buf: [@sizeOf(u32)]u8 = undefined;
        std.mem.writeInt(u32, &len_buf, @intCast(component.len), .big);
        try out.appendSlice(alloc, &len_buf);
        try out.appendSlice(alloc, component);
    }

    return try out.toOwnedSlice(alloc);
}

test "db write path create-only batch rejects existing document key" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .mem = .{} },
    });
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"name\":\"first\"}" }},
        .sync_level = .write,
    });

    try std.testing.expectError(error.Conflict, db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"name\":\"second\"}" }},
        .write_mode = .create_only,
        .sync_level = .write,
    }));

    const value = (try db.get(alloc, "doc:a")) orelse return error.TestExpectedEqual;
    defer alloc.free(value);
    try std.testing.expectEqualStrings("{\"name\":\"first\"}", value);
}

test "db write path create-only batch rejects duplicate request keys before coalescing" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .mem = .{} },
    });
    defer db.close();

    try std.testing.expectError(error.Conflict, db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"name\":\"first\"}" },
            .{ .key = "doc:a", .value = "{\"name\":\"second\"}" },
        },
        .write_mode = .create_only,
        .sync_level = .write,
    }));

    if (try db.get(alloc, "doc:a")) |value| {
        defer alloc.free(value);
        return error.TestExpectedEqual;
    }
}

test "db write path doc identity allocates final document ordinal then rejects new documents" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .mem = .{} },
    });
    defer db.close();

    const last_allocatable = std.math.maxInt(doc_identity.DocOrdinal) - 1;
    var value: [4]u8 = undefined;
    std.mem.writeInt(u32, &value, last_allocatable, .big);
    try db.core.store.put(internal_keys.identity_next_ordinal_key[0..], &value);

    try db.batch(.{
        .writes = &.{.{ .key = "doc:last", .value = "{\"name\":\"last\"}" }},
        .sync_level = .write,
    });

    {
        var txn = try db.core.store.beginProbeTxn();
        defer txn.abort();
        try std.testing.expectEqual(@as(?doc_identity.DocOrdinal, last_allocatable), try doc_identity.lookupOrdinalTxn(alloc, &txn, "doc:last"));
    }

    const exhausted = try db.stats(alloc);
    defer types.freeDBStats(alloc, exhausted);
    try std.testing.expectEqual(std.math.maxInt(doc_identity.DocOrdinal), exhausted.doc_identity.next_ordinal);
    try std.testing.expectEqual(@as(u64, 0), exhausted.doc_identity.ordinal_capacity_remaining);
    try std.testing.expect(exhausted.doc_identity.ordinal_capacity_exhausted);
    try std.testing.expect(exhausted.doc_identity.rebuild_required);

    try db.batch(.{
        .writes = &.{.{ .key = "doc:last", .value = "{\"name\":\"updated-last\"}" }},
        .sync_level = .full_index,
    });
    const updated = (try db.get(alloc, "doc:last")) orelse return error.TestExpectedEqual;
    defer alloc.free(updated);
    try std.testing.expectEqualStrings("{\"name\":\"updated-last\"}", updated);

    try std.testing.expectError(error.DocOrdinalExhausted, db.batch(.{
        .writes = &.{.{ .key = "doc:overflow", .value = "{\"name\":\"overflow\"}" }},
        .sync_level = .write,
    }));
}

test "db write path doc identity allocates final document ordinal with all index families present" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .mem = .{} },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\"}",
    });
    try db.addIndex(.{
        .name = "sp_v1",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse\"}",
    });
    try db.addIndex(.{
        .name = "graph_v1",
        .kind = .graph,
        .config_json = "{}",
    });
    try db.addIndex(.{
        .name = "alg_v1",
        .kind = .algebraic,
        .config_json =
        \\{
        \\  "version": 1,
        \\  "table": "docs",
        \\  "group_fields": [{"name":"category","path":"category","type":"string"}],
        \\  "measure_fields": [{"name":"score","path":"score","type":"number"}],
        \\  "materializations": [{"name":"count_by_category","op":"count","group_by":["category"]}]
        \\}
        ,
    });

    const last_allocatable = std.math.maxInt(doc_identity.DocOrdinal) - 1;
    var value: [4]u8 = undefined;
    std.mem.writeInt(u32, &value, last_allocatable, .big);
    try db.core.store.put(internal_keys.identity_next_ordinal_key[0..], &value);

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:last",
            .value = "{\"body\":\"final ordinal token\",\"category\":\"keep\",\"score\":1.0,\"embedding\":[0.0,1.0],\"sparse\":{\"indices\":[1],\"values\":[1.0]}}",
        }},
        .graph_writes = &.{.{
            .index_name = "graph_v1",
            .source = "doc:last",
            .target = "doc:last",
            .edge_type = "self",
            .weight = 1.0,
        }},
        .sync_level = .full_index,
    });

    {
        var txn = try db.core.store.beginProbeTxn();
        defer txn.abort();
        try std.testing.expectEqual(@as(?doc_identity.DocOrdinal, last_allocatable), try doc_identity.lookupOrdinalTxn(alloc, &txn, "doc:last"));
    }

    const exhausted = try db.stats(alloc);
    defer types.freeDBStats(alloc, exhausted);
    try std.testing.expectEqual(std.math.maxInt(doc_identity.DocOrdinal), exhausted.doc_identity.next_ordinal);
    try std.testing.expect(exhausted.doc_identity.ordinal_capacity_exhausted);
    try std.testing.expect(exhausted.doc_identity.rebuild_required);

    try std.testing.expectError(error.DocOrdinalExhausted, db.batch(.{
        .writes = &.{.{ .key = "doc:overflow", .value = "{\"body\":\"overflow\"}" }},
        .sync_level = .full_index,
    }));
}

test "db write path doc identity rejects new document writes at ordinal exhaustion for every sync level" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .mem = .{} },
    });
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:existing", .value = "{\"name\":\"existing\"}" }},
        .sync_level = .write,
    });

    var value: [4]u8 = undefined;
    std.mem.writeInt(u32, &value, std.math.maxInt(doc_identity.DocOrdinal), .big);
    try db.core.store.put(internal_keys.identity_next_ordinal_key[0..], &value);

    try db.batch(.{
        .writes = &.{.{ .key = "doc:existing", .value = "{\"name\":\"updated\"}" }},
        .sync_level = .full_index,
    });
    const existing = (try db.get(alloc, "doc:existing")) orelse return error.TestExpectedEqual;
    defer alloc.free(existing);
    try std.testing.expectEqualStrings("{\"name\":\"updated\"}", existing);

    const levels = [_]types.SyncLevel{
        .propose,
        .write,
        .full_text,
        .enrichments,
        .aknn,
        .full_index,
    };
    for (levels, 0..) |level, i| {
        var key_buf: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "doc:new:{d}", .{i});
        try std.testing.expectError(error.DocOrdinalExhausted, db.batch(.{
            .writes = &.{.{ .key = key, .value = "{\"name\":\"new\"}" }},
            .sync_level = level,
        }));
        try std.testing.expect((try db.get(alloc, key)) == null);
    }
}

test "db write path caches identity visibility summary after local writes" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .mem = .{} },
    });
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"name\":\"alpha\"}" }},
    });
    try std.testing.expect(db.identity_visibility_summary_cache != null);
    try std.testing.expect(try db.internalAllDocsVisibleAtGeneration(db.core.nextDerivedSequence()));

    try db.batch(.{
        .deletes = &.{"doc:a"},
    });
    try std.testing.expect(db.identity_visibility_summary_cache != null);
    try std.testing.expect(!(try db.internalAllDocsVisibleAtGeneration(db.core.nextDerivedSequence())));
}

test "db write path document extraction templated inline source size is rejected before persistence" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const security = scraping.ContentSecurityConfig{ .max_download_size_bytes = 4 };
    var remote_content = scraping.RemoteContentConfig{ .security = security };
    var db = try DB.open(alloc, std.mem.span(path), .{
        .remote_content = &remote_content,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .template = "{{url}}",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
    });

    try std.testing.expectError(error.StreamTooLong, db.batch(.{
        .writes = &.{.{
            .key = "doc:templated-too-large",
            .value = "{\"url\":\"data:text/plain;base64,aGVsbG8=\"}",
        }},
        .sync_level = .write,
    }));

    const doc_key = try internal_keys.documentKeyAlloc(alloc, "doc:templated-too-large");
    defer alloc.free(doc_key);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, doc_key));
}

test "db write path document extraction template prompt failure is rejected before persistence" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .template = "<<<error:status=413 message=StreamTooLong>>> fallback text",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
    });

    try std.testing.expectError(error.PermanentPromptFailure, db.batch(.{
        .writes = &.{.{
            .key = "doc:templated-prompt-failure",
            .value = "{\"url\":\"data:text/plain;base64,Zm9v\"}",
        }},
        .sync_level = .write,
    }));
}

fn testRemoteTemplateHostRenderJsonToPartsErrorDirective(
    _: ?*anyopaque,
    alloc: Allocator,
    _: []const u8,
    _: []const u8,
    _: template_remote.RenderConfig,
) ![]template_mod.ContentPart {
    const parts = try alloc.alloc(template_mod.ContentPart, 1);
    errdefer alloc.free(parts);
    parts[0] = .{ .text = try alloc.dupe(u8, "<<<error:status=413 message=StreamTooLong>>> fallback text") };
    return parts;
}

test "db write path remote template host-rendered parts preserve prompt failures" {
    const alloc = std.testing.allocator;
    template_remote.setHostRenderer(.{
        .render_json_to_parts = testRemoteTemplateHostRenderJsonToPartsErrorDirective,
    });
    defer template_remote.setHostRenderer(null);

    try std.testing.expectError(
        error.PermanentPromptFailure,
        renderSourceTemplateParts(
            alloc,
            null,
            null,
            "{{remoteMedia url=this}}",
            "\"https://example.com/photo.png\"",
        ),
    );
}

test "db write path transform resolves transforms against pending same-batch writes" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:coalesce", .value = "{\"count\":1,\"tags\":[\"db\"]}" },
        },
        .transforms = &.{
            .{
                .key = "doc:coalesce",
                .operations = &.{
                    .{ .op = .inc, .path = "count", .value_json = "2" },
                    .{ .op = .add_to_set, .path = "tags", .value_json = "\"zig\"" },
                },
            },
        },
    });

    const raw = (try db.get(alloc, "doc:coalesce")) orelse return error.TestExpectedEqual;
    defer alloc.free(raw);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();
    const count_value = parsed.value.object.get("count").?;
    switch (count_value) {
        .integer => |value| try std.testing.expectEqual(@as(i64, 3), value),
        .float => |value| try std.testing.expectEqual(@as(f64, 3), value),
        else => return error.TestExpectedEqual,
    }
    try std.testing.expectEqual(@as(usize, 2), parsed.value.object.get("tags").?.array.items.len);
}

test "db write path transform relational batch transforms read and rewrite base rows only" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"text"},"count":{"type":"numeric"},"active":{"type":"boolean"},"attrs":{"type":"json"}},"required":["title","count"],"additionalProperties":false}}}}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.batch(.{
        .writes = &.{.{
            .key = "row:transform",
            .value = "{\"title\":\"base row\",\"count\":1,\"active\":true,\"attrs\":{\"tier\":\"gold\"}}",
        }},
    });

    const primary_key = try internal_keys.documentKeyAlloc(alloc, "row:transform");
    defer alloc.free(primary_key);
    try db.core.store.put(primary_key, "{\"title\":\"stale primary\",\"count\":999,\"active\":false}");

    try db.batch(.{
        .transforms = &.{.{
            .key = "row:transform",
            .operations = &.{
                .{ .op = .inc, .path = "count", .value_json = "4" },
                .{ .op = .set, .path = "active", .value_json = "false" },
                .{ .op = .set, .path = "attrs", .value_json = "{\"tier\":\"platinum\"}" },
            },
        }},
    });

    const raw = (try db.get(alloc, "row:transform")) orelse return error.TestExpectedEqual;
    defer alloc.free(raw);
    try std.testing.expect(std.mem.indexOf(u8, raw, "\"title\":\"base row\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, raw, "\"count\":5") != null);
    try std.testing.expect(std.mem.indexOf(u8, raw, "\"active\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, raw, "\"attrs\":{\"tier\":\"platinum\"}") != null);
    try std.testing.expect(std.mem.indexOf(u8, raw, "stale primary") == null);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, primary_key));
}

test "db write path transform keeps delete when same-batch transform targets deleted key" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:delete_transform", .value = "{\"status\":\"old\"}" },
        },
    });

    try db.batch(.{
        .deletes = &.{"doc:delete_transform"},
        .transforms = &.{
            .{
                .key = "doc:delete_transform",
                .operations = &.{
                    .{ .op = .set, .path = "status", .value_json = "\"new\"" },
                },
            },
        },
    });

    try std.testing.expect((try db.get(alloc, "doc:delete_transform")) == null);
}

test "db write path transform relational batch keeps delete when same-batch transform targets deleted key" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"text"},"status":{"type":"text"}},"required":["title"],"additionalProperties":false}}}}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.batch(.{
        .writes = &.{.{ .key = "row:delete_transform", .value = "{\"title\":\"delete me\",\"status\":\"old\"}" }},
    });

    try db.batch(.{
        .deletes = &.{"row:delete_transform"},
        .transforms = &.{.{
            .key = "row:delete_transform",
            .operations = &.{.{ .op = .set, .path = "status", .value_json = "\"new\"" }},
        }},
    });

    try std.testing.expect((try db.get(alloc, "row:delete_transform")) == null);
    try std.testing.expect((try relational_store_mod.getRawAlloc(alloc, db.core.store, "row:delete_transform")) == null);
}

test "db write path bulk ingest write commits document writes before finish" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.beginBulkIngestSession();
    errdefer db.abortBulkIngestSession();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:bulk_stage", .value = "{\"count\":1}" },
        },
        .sync_level = .write,
    });

    const visible_before_finish = (try db.get(alloc, "doc:bulk_stage")) orelse return error.TestExpectedEqual;
    alloc.free(visible_before_finish);

    try db.finishBulkIngestSessionWithOptions(.{ .compact = false });

    const raw = (try db.get(alloc, "doc:bulk_stage")) orelse return error.TestExpectedEqual;
    defer alloc.free(raw);
}

test "db write path bulk ingest primary lsm writes use direct sorted ingest batch mode" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .lsm = .{
            .flush_threshold = 1,
            .bulk_ingest_flush_threshold_multiplier = 4,
        } },
    });
    defer db.close();

    try db.beginBulkIngestSession();
    errdefer db.abortBulkIngestSession();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:bulk_lsm_a", .value = "{\"count\":1}" },
            .{ .key = "doc:bulk_lsm_b", .value = "{\"count\":2}" },
            .{ .key = "doc:bulk_lsm_c", .value = "{\"count\":3}" },
            .{ .key = "doc:bulk_lsm_d", .value = "{\"count\":4}" },
        },
        .sync_level = .write,
    });

    const stats = db.snapshotPrimaryLsmWriteStatsForTest() orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, 0), stats.flushes);
    try std.testing.expect(stats.sorted_ingest_runs > 0);

    const visible_before_finish = (try db.get(alloc, "doc:bulk_lsm_d")) orelse return error.TestExpectedEqual;
    alloc.free(visible_before_finish);

    try db.finishBulkIngestSessionWithOptions(.{ .compact = false });
}

test "db write path bulk ingest resolves transforms across direct write batches" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.beginBulkIngestSession();
    errdefer db.abortBulkIngestSession();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:bulk_transform", .value = "{\"count\":1}" },
        },
        .sync_level = .write,
    });
    try db.batch(.{
        .transforms = &.{
            .{
                .key = "doc:bulk_transform",
                .operations = &.{
                    .{ .op = .inc, .path = "count", .value_json = "2" },
                },
            },
        },
        .sync_level = .write,
    });

    try db.finishBulkIngestSessionWithOptions(.{ .compact = false });

    const raw = (try db.get(alloc, "doc:bulk_transform")) orelse return error.TestExpectedEqual;
    defer alloc.free(raw);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();
    const count_value = parsed.value.object.get("count").?;
    switch (count_value) {
        .integer => |value| try std.testing.expectEqual(@as(i64, 3), value),
        .float => |value| try std.testing.expectEqual(@as(f64, 3), value),
        else => return error.TestExpectedEqual,
    }
}

test "db write path bulk ingest applies pure-doc work at requested sync level" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.beginBulkIngestSession();
    errdefer db.abortBulkIngestSession();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:bulk_sync", .value = "{\"count\":1}" },
        },
        .sync_level = .write,
    });

    try db.batch(.{
        .transforms = &.{
            .{
                .key = "doc:bulk_sync",
                .operations = &.{
                    .{ .op = .inc, .path = "count", .value_json = "2" },
                },
            },
        },
        .sync_level = .write,
    });

    const raw = (try db.get(alloc, "doc:bulk_sync")) orelse return error.TestExpectedEqual;
    defer alloc.free(raw);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();
    const count_value = parsed.value.object.get("count").?;
    switch (count_value) {
        .integer => |value| try std.testing.expectEqual(@as(i64, 3), value),
        .float => |value| try std.testing.expectEqual(@as(f64, 3), value),
        else => return error.TestExpectedEqual,
    }

    try db.finishBulkIngestSessionWithOptions(.{ .compact = false });
}

test "db write path bulk ingest keeps direct writes visible before timestamped batch" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.beginBulkIngestSession();
    errdefer db.abortBulkIngestSession();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:staged_before_timestamp", .value = "{\"count\":1}" },
        },
        .sync_level = .write,
    });
    const visible_before_timestamp = (try db.get(alloc, "doc:staged_before_timestamp")) orelse return error.TestExpectedEqual;
    alloc.free(visible_before_timestamp);

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:timestamped", .value = "{\"count\":2}" },
        },
        .timestamp_ns = 1234,
        .sync_level = .write,
    });

    const staged = (try db.get(alloc, "doc:staged_before_timestamp")) orelse return error.TestExpectedEqual;
    defer alloc.free(staged);
    const timestamped = (try db.get(alloc, "doc:timestamped")) orelse return error.TestExpectedEqual;
    defer alloc.free(timestamped);

    try db.finishBulkIngestSessionWithOptions(.{ .compact = false });
}

test "db write path bulk ingest keeps direct writes visible before predicate batch" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:pred_target", .value = "{\"count\":1}" },
        },
        .timestamp_ns = 5_000,
        .sync_level = .write,
    });

    try db.beginBulkIngestSession();
    errdefer db.abortBulkIngestSession();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:staged_before_predicate", .value = "{\"count\":1}" },
        },
        .sync_level = .write,
    });
    const visible_before_predicate = (try db.get(alloc, "doc:staged_before_predicate")) orelse return error.TestExpectedEqual;
    alloc.free(visible_before_predicate);

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:pred_target", .value = "{\"count\":2}" },
        },
        .predicates = &.{
            .{ .key = "doc:pred_target", .expected_version = 5_000 },
        },
        .sync_level = .write,
    });

    const staged = (try db.get(alloc, "doc:staged_before_predicate")) orelse return error.TestExpectedEqual;
    defer alloc.free(staged);
    const pred_target = (try db.get(alloc, "doc:pred_target")) orelse return error.TestExpectedEqual;
    defer alloc.free(pred_target);

    try db.finishBulkIngestSessionWithOptions(.{ .compact = false });
}

test "db write path bulk ingest keeps direct writes visible before graph batch" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "gr_v1",
        .kind = .graph,
        .config_json = "{}",
    });

    try db.beginBulkIngestSession();
    errdefer db.abortBulkIngestSession();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:staged_before_graph", .value = "{\"count\":1}" },
        },
        .sync_level = .write,
    });
    const visible_before_graph = (try db.get(alloc, "doc:staged_before_graph")) orelse return error.TestExpectedEqual;
    alloc.free(visible_before_graph);

    try db.batch(.{
        .graph_writes = &.{
            .{ .index_name = "gr_v1", .source = "doc:a", .target = "doc:b", .edge_type = "links", .weight = 1.0 },
        },
        .sync_level = .write,
    });

    const staged = (try db.get(alloc, "doc:staged_before_graph")) orelse return error.TestExpectedEqual;
    defer alloc.free(staged);

    try db.finishBulkIngestSessionWithOptions(.{ .compact = false });
}

test "db write path bulk ingest query_readonly reopen serves empty dense search instead of index-not-found" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var writer = try DB.open(alloc, std.mem.span(path), .{});
    defer writer.close();

    try writer.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    });

    try writer.beginBulkIngestSession();
    errdefer writer.abortBulkIngestSession();

    try writer.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_embeddings\":{\"dense_idx\":[0,1,0]}}" },
        },
        .sync_level = .write,
    });

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .query_readonly,
    });
    defer reopened.close();

    var result = try reopened.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{
            .vector = &.{ 1.0, 0.0, 0.0 },
            .k = 2,
        } },
        .limit = 2,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 0), result.total_hits);
    try std.testing.expectEqual(@as(usize, 0), result.hits.len);
}

test "db write path bulk ingest query_readonly reopen does not backfill pending external dense artifacts" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var writer = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
        });
        defer writer.close();

        try writer.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
        });

        try writer.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}" },
                .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_embeddings\":{\"dense_idx\":[0,1,0]}}" },
            },
            .sync_level = .write,
        });

        const writer_stats = try writer.stats(alloc);
        defer types.freeDBStats(alloc, writer_stats);
        try std.testing.expectEqual(@as(u64, 0), writer_stats.indexes[0].doc_count);
        try std.testing.expect(writer_stats.indexes[0].replay_target_sequence > writer_stats.indexes[0].replay_applied_sequence);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .query_readonly,
    });
    defer reopened.close();

    const reopened_stats = try reopened.stats(alloc);
    defer types.freeDBStats(alloc, reopened_stats);
    try std.testing.expectEqual(@as(u64, 0), reopened_stats.indexes[0].doc_count);
    try std.testing.expect(reopened_stats.indexes[0].replay_target_sequence > reopened_stats.indexes[0].replay_applied_sequence);
}

test "db write path bulk ingest dense auto finish wakes weak-sync replay and publishes visibility after catch-up" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    });

    const HookCtx = struct {
        publish_calls: u64 = 0,
        invalidate_calls: u64 = 0,

        fn onChange(ptr: *anyopaque, _: []const u8, _: u64, _: ?*DB, change: db_internal.QueryVisibilityChange) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            switch (change) {
                .publish, .publish_consistent => self.publish_calls += 1,
                .invalidate => self.invalidate_calls += 1,
            }
        }
    };
    var hook_ctx = HookCtx{};
    db.setQueryVisibilityHook(.{
        .ptr = &hook_ctx,
        .table_name = "docs",
        .group_id = 7001,
        .db = &db,
        .on_change = HookCtx.onChange,
    });

    try db.beginDenseAutoBulkIngestSession();
    errdefer db.abortDenseAutoBulkIngestSession();

    inline for (.{ "write", "propose", "write", "propose" }, 0..) |level, i| {
        const key = try std.fmt.allocPrint(alloc, "doc:{d}", .{i});
        defer alloc.free(key);
        const value = try std.fmt.allocPrint(
            alloc,
            "{{\"_embeddings\":{{\"dense_idx\":[{d}.0,0.0,0.0]}}}}",
            .{i + 1},
        );
        defer alloc.free(value);
        try db.batch(.{
            .writes = &.{.{ .key = key, .value = value }},
            .sync_level = types.parsePublicSyncLevelText(level).?,
        });
    }

    // Finish the implicit bulk publish, but hold the deferred executor wake so
    // the test can prove the replay catch-up itself publishes fresh visibility.
    try db.finishDenseAutoBulkIngestSessionWithOptionsAndNotifyExecutor(.{ .compact = false }, false);
    hook_ctx = .{};

    db_internal.flushDeferredExternalBulkExecutorNotification(db.async_context, db.executor);
    try db.executor.waitForAll(4);

    try std.testing.expect(hook_ctx.publish_calls > 0);
    try std.testing.expectEqual(@as(u64, 0), hook_ctx.invalidate_calls);
    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(@as(u64, 4), stats.indexes[0].replay_applied_sequence);
    try std.testing.expectEqual(@as(u64, 4), stats.indexes[0].replay_target_sequence);
}

test "db write path bulk ingest dense auto finish wakes current replay target if deferred wake is absent" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    });

    try db.beginDenseAutoBulkIngestSession();
    errdefer db.abortDenseAutoBulkIngestSession();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"_embeddings\":{\"dense_idx\":[1.0,0.0,0.0]}}" },
        },
        .sync_level = .write,
    });

    const target_sequence = db.core.nextDerivedSequence();
    try std.testing.expect(target_sequence > 0);
    db.async_context.deferred_external_bulk_notify_sequence.store(0, .release);

    try db.finishDenseAutoBulkIngestSessionWithOptions(.{ .compact = false });
    try db.executor.waitForAll(target_sequence);

    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(target_sequence, stats.indexes[0].replay_applied_sequence);
    try std.testing.expectEqual(@as(u64, 1), stats.indexes[0].doc_count);
}

test "db write path bulk ingest dense auto replays packed external embedding strings" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    });

    try db.beginDenseAutoBulkIngestSession();
    errdefer db.abortDenseAutoBulkIngestSession();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"_embeddings\":{\"dense_idx\":\"AACAPwAAAAAAAAAA\"}}" },
            .{ .key = "doc:b", .value = "{\"_embeddings\":{\"dense_idx\":\"AAAAAAAAAAAAAIA/\"}}" },
        },
        .sync_level = .write,
    });

    const target_sequence = db.core.nextDerivedSequence();
    try db.finishDenseAutoBulkIngestSessionWithOptions(.{ .compact = false });
    try db.executor.waitForAll(target_sequence);

    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(@as(u64, 2), stats.indexes[0].doc_count);
}

test "db write path bulk ingest full_text sync defers text merge work until finish" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .text_merge = .{
            .enabled = true,
            .idle_interval_ms = 10_000,
            .error_interval_ms = 10_000,
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });

    try db.beginBulkIngestSession();
    errdefer db.abortBulkIngestSession();

    for (0..12) |i| {
        const key = try std.fmt.allocPrint(alloc, "doc:{d}", .{i});
        defer alloc.free(key);
        const value = try std.fmt.allocPrint(alloc, "{{\"body\":\"common token {d}\"}}", .{i});
        defer alloc.free(value);

        try db.batch(.{
            .writes = &.{.{ .key = key, .value = value }},
            .sync_level = .full_text,
        });
    }

    const before = db.pendingWorkStats().text_merge;
    try std.testing.expect(before.pending_segments > 0);
    try std.testing.expect(db.async_context.text_merge_deferred.load(.acquire));

    const runtime = db.text_merge_runtime orelse return error.TestUnexpectedResult;
    try std.testing.expect(!(try runtime.runOnce()));

    try db.finishBulkIngestSessionWithOptions(.{ .compact = false });
    try std.testing.expect(!db.async_context.text_merge_deferred.load(.acquire));

    try db.runUntilIdle();

    const after = db.pendingWorkStats().text_merge;
    try std.testing.expect(after.pending_segments <= before.pending_segments);
}

test "db write path bulk ingest finish publishes primary store before external dense leaf splits" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "vec",
        .kind = .dense_vector,
        .config_json =
        \\{"field":"embedding","dims":2,"metric":"l2_squared","external":true,"leaf_size":4,"branching_factor":8,"use_quantization":false,"max_cached_vectors":2}
        ,
    });

    {
        try db.beginBulkIngestSession();
        errdefer db.abortBulkIngestSession();

        for (0..24) |i| {
            const key = try std.fmt.allocPrint(alloc, "doc:{d}", .{i});
            defer alloc.free(key);
            const value = try std.fmt.allocPrint(
                alloc,
                "{{\"title\":\"dense {d}\",\"_embeddings\":{{\"vec\":[{d}.0,0.0]}}}}",
                .{ i, i },
            );
            defer alloc.free(value);

            try db.batch(.{
                .writes = &.{.{ .key = key, .value = value }},
                .sync_level = .write,
            });
        }

        try db.finishBulkIngestSessionWithOptions(.{ .compact = false });
    }
    try db.runUntilIdle();

    var result = try db.search(alloc, .{
        .index_name = "vec",
        .dense = .{ .vector = &.{ 23.0, 0.0 }, .k = 1 },
    });
    defer result.deinit();
    try std.testing.expect(result.total_hits > 0);
}

test "db write path bulk ingest algebraic survives reopen with durable lsm primary backend" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "group_fields": [{"name":"customer","path":"customer","type":"string"}],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "materializations": [{"name":"sum_by_customer","op":"sum","group_by":["customer"],"measure":"amount"}]
        \\}
    ;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .primary_backend = .{ .lsm = .{ .flush_threshold = 2 } },
            .start_index_workers = false,
        });
        defer db.close();

        try db.addIndex(.{
            .name = "alg",
            .kind = .algebraic,
            .config_json = cfg,
        });

        try db.beginBulkIngestSession();
        errdefer db.abortBulkIngestSession();

        try db.batch(.{
            .writes = &.{
                .{ .key = "o1", .value = "{\"customer\":\"alice\",\"amount\":10}" },
                .{ .key = "o2", .value = "{\"customer\":\"alice\",\"amount\":20}" },
                .{ .key = "o3", .value = "{\"customer\":\"bob\",\"amount\":7}" },
            },
            .sync_level = .write,
        });

        try db.finishBulkIngestSessionWithOptions(.{
            .compact = false,
            .flush = true,
            .max_deferred_l0_runs = 2,
            .max_foreground_compaction_steps = 1,
        });

        const entry = db.core.index_manager.algebraicIndex("alg") orelse return error.TestUnexpectedResult;
        const alice_token = try entry.index.constraintTokenAlloc(alloc, "customer", "alice");
        defer alloc.free(alice_token);
        const alice_group = try algebraic_mod.token.canonicalTupleAlloc(alloc, &.{alice_token});
        defer alloc.free(alice_group);
        try std.testing.expectEqual(@as(f64, 30), (try entry.index.numericValue(db.core.store, "sum_by_customer", alice_group)).?);
    }

    {
        var reopened = try DB.open(alloc, std.mem.span(path), .{
            .primary_backend = .{ .lsm = .{ .flush_threshold = 2 } },
            .start_index_workers = false,
        });
        defer reopened.close();

        try std.testing.expectEqual(@as(u32, 1), reopened.core.index_manager.count());
        const entry = reopened.core.index_manager.algebraicIndex("alg") orelse return error.TestUnexpectedResult;

        const alice_token = try entry.index.constraintTokenAlloc(alloc, "customer", "alice");
        defer alloc.free(alice_token);
        const alice_group = try algebraic_mod.token.canonicalTupleAlloc(alloc, &.{alice_token});
        defer alloc.free(alice_group);
        try std.testing.expectEqual(@as(f64, 30), (try entry.index.numericValue(reopened.core.store, "sum_by_customer", alice_group)).?);

        const bob_token = try entry.index.constraintTokenAlloc(alloc, "customer", "bob");
        defer alloc.free(bob_token);
        const bob_group = try algebraic_mod.token.canonicalTupleAlloc(alloc, &.{bob_token});
        defer alloc.free(bob_group);
        try std.testing.expectEqual(@as(f64, 7), (try entry.index.numericValue(reopened.core.store, "sum_by_customer", bob_group)).?);
    }
}

test "db write path batch load profile benchmark" {
    const DB = @import("mod.zig").DB;
    if (!TestHelpers.profileBenchTestsEnabled()) return error.SkipZigTest;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
    });
    defer db.close();

    const dims: usize = 16;
    const batch_docs: usize = 32;
    const batch_count: usize = 1;
    const total_docs: usize = batch_docs * batch_count;

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":16,\"metric\":\"cosine\",\"external\":true}",
    });

    const vector_buf = try alloc.alloc(f32, dims);
    defer alloc.free(vector_buf);

    var total_profile = BatchProfile{};
    for (0..batch_count) |batch_index| {
        const writes = try alloc.alloc(types.BatchWrite, batch_docs);
        defer {
            for (writes) |write| {
                alloc.free(write.key);
                alloc.free(write.value);
            }
            alloc.free(writes);
        }
        for (writes, 0..) |*write, doc_offset| {
            const doc_id = batch_index * batch_docs + doc_offset;
            var norm_sq: f32 = 0;
            for (vector_buf, 0..) |*slot, dim| {
                const raw: u32 = @intCast((doc_id * 1315423911 + dim * 2654435761 + 17) % 1000);
                const centered = (@as(f32, @floatFromInt(raw)) / 500.0) - 1.0;
                slot.* = centered;
                norm_sq += centered * centered;
            }
            const inv_norm: f32 = 1.0 / @sqrt(norm_sq);
            for (vector_buf) |*slot| slot.* *= inv_norm;

            write.* = .{
                .key = try std.fmt.allocPrint(alloc, "doc:{d}", .{doc_id}),
                .value = try std.fmt.allocPrint(
                    alloc,
                    "{{\"title\":\"doc-{d}\",\"_embeddings\":{{\"dv_v1\":{f}}}}}",
                    .{ doc_id, std.json.fmt(vector_buf, .{}) },
                ),
            };
        }

        var batch_profile = BatchProfile{};
        try db.batchProfiled(.{
            .writes = writes,
            .sync_level = .full_index,
        }, &batch_profile);
        addBatchProfile(&total_profile, batch_profile);
    }

    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(@as(u64, total_docs), stats.indexes[0].doc_count);

    std.debug.print(
        "batch_load_profile docs={d} batch_docs={d} batches={d} dims={d} avg_total_ms={d} avg_resolve_transforms_ms={d} avg_merge_req_ms={d} avg_predicates_ms={d} avg_validate_range_ms={d} avg_extract_writes_ms={d} avg_delete_artifacts_ms={d} avg_precompute_generated_ms={d} avg_store_write_ms={d} avg_split_delta_ms={d} avg_build_derived_ms={d} avg_apply_shadow_ms={d} avg_collect_sync_targets_ms={d} avg_append_replay_journal_ms={d} avg_wait_sync_ms={d} avg_notify_enrichment_ms={d}\n",
        .{
            total_docs,
            batch_docs,
            batch_count,
            dims,
            @divTrunc(@divTrunc(total_profile.total_ns, batch_count), std.time.ns_per_ms),
            @divTrunc(@divTrunc(total_profile.resolve_transforms_ns, batch_count), std.time.ns_per_ms),
            @divTrunc(@divTrunc(total_profile.merge_effective_req_ns, batch_count), std.time.ns_per_ms),
            @divTrunc(@divTrunc(total_profile.predicates_ns, batch_count), std.time.ns_per_ms),
            @divTrunc(@divTrunc(total_profile.validate_range_ns, batch_count), std.time.ns_per_ms),
            @divTrunc(@divTrunc(total_profile.extract_writes_ns, batch_count), std.time.ns_per_ms),
            @divTrunc(@divTrunc(total_profile.delete_artifacts_ns, batch_count), std.time.ns_per_ms),
            @divTrunc(@divTrunc(total_profile.precompute_generated_ns, batch_count), std.time.ns_per_ms),
            @divTrunc(@divTrunc(total_profile.store_write_ns, batch_count), std.time.ns_per_ms),
            @divTrunc(@divTrunc(total_profile.split_delta_ns, batch_count), std.time.ns_per_ms),
            @divTrunc(@divTrunc(total_profile.build_derived_ns, batch_count), std.time.ns_per_ms),
            @divTrunc(@divTrunc(total_profile.apply_shadow_ns, batch_count), std.time.ns_per_ms),
            @divTrunc(@divTrunc(total_profile.collect_sync_targets_ns, batch_count), std.time.ns_per_ms),
            @divTrunc(@divTrunc(total_profile.append_replay_journal_ns, batch_count), std.time.ns_per_ms),
            @divTrunc(@divTrunc(total_profile.wait_sync_ns, batch_count), std.time.ns_per_ms),
            @divTrunc(@divTrunc(total_profile.notify_enrichment_ns, batch_count), std.time.ns_per_ms),
        },
    );
}

test "db write path extract enrichments exposes cleaned writes and special fields" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    var result = try db.extractEnrichments(alloc, &.{
        .{
            .key = "doc:a",
            .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,2,3],\"sparse_idx\":{\"indices\":[1,5],\"values\":[0.5,0.75]}},\"_edges\":{\"graph_v1\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":2.0}]}}}",
        },
    });
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.cleaned_writes.len);
    try std.testing.expectEqualStrings("doc:a", result.cleaned_writes[0].key);
    try std.testing.expect(std.mem.indexOf(u8, result.cleaned_writes[0].value, "\"title\":\"alpha\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.cleaned_writes[0].value, "_embeddings") == null);
    try std.testing.expect(std.mem.indexOf(u8, result.cleaned_writes[0].value, "_summaries") == null);
    try std.testing.expect(std.mem.indexOf(u8, result.cleaned_writes[0].value, "_edges") == null);

    try std.testing.expectEqual(@as(usize, 1), result.dense_embeddings.len);
    try std.testing.expectEqualStrings("dense_idx", result.dense_embeddings[0].index_name);
    try std.testing.expectEqual(@as(usize, 3), result.dense_embeddings[0].vector.len);

    try std.testing.expectEqual(@as(usize, 1), result.sparse_embeddings.len);
    try std.testing.expectEqualStrings("sparse_idx", result.sparse_embeddings[0].index_name);
    try std.testing.expectEqual(@as(usize, 2), result.sparse_embeddings[0].indices.len);

    try std.testing.expectEqual(@as(usize, 1), result.graph_writes.len);
    try std.testing.expectEqualStrings("graph_v1", result.graph_writes[0].index_name);
    try std.testing.expectEqualStrings("doc:b", result.graph_writes[0].target);
}

test "db write path extract enrichments projects configured embedded json vector and graph fields" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "attrs_dense",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"attrs.embedding\",\"dims\":3,\"metric\":\"cosine\"}",
    });
    try db.addIndex(.{
        .name = "attrs_sparse",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"attrs.sparse\"}",
    });
    try db.addIndex(.{
        .name = "attrs_graph",
        .kind = .graph,
        .config_json = "{\"edge_types\":[{\"name\":\"cites\",\"field\":\"attrs.links\"}]}",
    });

    var result = try db.extractEnrichments(alloc, &.{
        .{
            .key = "doc:a",
            .value =
            \\{"title":"alpha","attrs":{"embedding":[1,0,0],"sparse":{"indices":[7,42],"values":[1.5,0.5]},"links":["doc:b","doc:c"]}}
            ,
        },
    });
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.cleaned_writes.len);
    try std.testing.expectEqual(@as(usize, 1), result.dense_embeddings.len);
    try std.testing.expectEqualStrings("attrs_dense", result.dense_embeddings[0].index_name);
    try std.testing.expectEqual(@as(usize, 3), result.dense_embeddings[0].vector.len);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), result.dense_embeddings[0].vector[0], 0.0001);

    try std.testing.expectEqual(@as(usize, 1), result.sparse_embeddings.len);
    try std.testing.expectEqualStrings("attrs_sparse", result.sparse_embeddings[0].index_name);
    try std.testing.expectEqual(@as(usize, 2), result.sparse_embeddings[0].indices.len);
    try std.testing.expectEqual(@as(u32, 7), result.sparse_embeddings[0].indices[0]);

    try std.testing.expectEqual(@as(usize, 2), result.graph_writes.len);
    try std.testing.expectEqualStrings("attrs_graph", result.graph_writes[0].index_name);
    try std.testing.expectEqualStrings("cites", result.graph_writes[0].edge_type);
    try std.testing.expectEqualStrings("doc:b", result.graph_writes[0].target);
    try std.testing.expectEqualStrings("doc:c", result.graph_writes[1].target);
}

test "db write path extract enrichments rejects unsupported legacy summaries field" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try std.testing.expectError(error.UnsupportedReservedField, db.extractEnrichments(alloc, &.{
        .{
            .key = "doc:a",
            .value = "{\"title\":\"alpha\",\"_summaries\":{\"sum_idx\":\"brief\"}}",
        },
    }));
}

test "db write path document artifact child range applies batch without source row write" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    const artifact_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:a", "document_units_v1", "page:000001");
    defer alloc.free(artifact_key);
    const artifact_value =
        "{\"_parent_doc_key\":\"doc:a\",\"_artifact_name\":\"document_units_v1\",\"_artifact_range_id\":\"range:000000\",\"_artifact_range_kind\":\"unit\",\"_artifact_route_status\":\"remote_committed\",\"_artifact_owner_group_id\":7002,\"unit_id\":\"page:000001\",\"text\":\"alpha\"}";
    const writes = [_]types.BatchWrite{.{
        .key = artifact_key,
        .value = artifact_value,
    }};

    const sequence = try db.applyDocumentArtifactChildRangeBatch(.{
        .artifact_writes = writes[0..],
        .sync_level = .write,
    });
    try std.testing.expect(sequence > 0);

    const stored = try db.core.store.get(alloc, artifact_key);
    defer alloc.free(stored);
    try std.testing.expectEqualStrings(artifact_value, stored);

    const source_store_key = try internal_keys.documentKeyAlloc(alloc, "doc:a");
    defer alloc.free(source_store_key);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, source_store_key));

    const deletes = [_][]const u8{artifact_key};
    const delete_sequence = try db.applyDocumentArtifactChildRangeBatch(.{
        .artifact_delete_keys = deletes[0..],
        .sync_level = .write,
    });
    try std.testing.expect(delete_sequence > sequence);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, artifact_key));
}

test "db write path document artifact child range dispatches generated artifacts to remote owner" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,YWxwaGE=\"}",
        }},
        .sync_level = .full_index,
    });

    try std.testing.expect(try db.updateDocumentArtifactChildRangePlacement(alloc, "doc:a", "document_units_v1", .{
        .range_id = "range:000000",
        .placement = "remote",
        .owner_group_id = 7002,
        .placement_generation = 7,
        .route_status = "remote_committed",
        .split_eligible = true,
    }));

    var moved = (try db.getDocumentArtifactManifest(alloc, "doc:a", "document_units_v1")) orelse return error.TestUnexpectedResult;
    defer moved.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), moved.child_ranges.len);
    const remote_unit_key = try alloc.dupe(u8, moved.child_ranges[0].start_key);
    defer alloc.free(remote_unit_key);

    const local_deletes = [_][]const u8{remote_unit_key};
    _ = try db.applyDocumentArtifactChildRangeBatch(.{
        .artifact_delete_keys = local_deletes[0..],
        .sync_level = .write,
    });
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, remote_unit_key));

    const Capture = struct {
        calls: usize = 0,
        owner_group_id: u64 = 0,
        artifact_writes: usize = 0,
        artifact_delete_keys: usize = 0,
        documents: usize = 0,
        dense_embeddings: usize = 0,
        sparse_embeddings: usize = 0,
        first_key: ?[]u8 = null,
        first_value: ?[]u8 = null,

        fn deinit(self: *@This(), allocator: Allocator) void {
            if (self.first_key) |key| allocator.free(key);
            if (self.first_value) |value| allocator.free(value);
        }

        fn dispatcher(self: *@This()) DocumentArtifactChildRangeDispatcher {
            return .{ .ptr = self, .apply = apply };
        }

        fn apply(ptr: *anyopaque, allocator: Allocator, dispatch: DocumentArtifactChildRangeDispatch) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            self.owner_group_id = dispatch.owner_group_id;
            self.artifact_writes += dispatch.child_batch.artifact_writes.len;
            self.artifact_delete_keys += dispatch.child_batch.artifact_delete_keys.len;
            self.documents += dispatch.child_batch.documents.len;
            self.dense_embeddings += dispatch.child_batch.dense_embeddings.len;
            self.sparse_embeddings += dispatch.child_batch.sparse_embeddings.len;
            if (self.first_key == null and dispatch.child_batch.artifact_writes.len > 0) {
                self.first_key = try allocator.dupe(u8, dispatch.child_batch.artifact_writes[0].key);
                errdefer {
                    allocator.free(self.first_key.?);
                    self.first_key = null;
                }
                self.first_value = try allocator.dupe(u8, dispatch.child_batch.artifact_writes[0].value);
            }
        }
    };

    var capture = Capture{};
    defer capture.deinit(alloc);

    try db.batchWithDocumentArtifactChildRangeDispatcher(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,YmV0YQ==\"}",
        }},
        .sync_level = .full_index,
    }, capture.dispatcher());

    try std.testing.expectEqual(@as(usize, 1), capture.calls);
    try std.testing.expectEqual(@as(u64, 7002), capture.owner_group_id);
    try std.testing.expectEqual(@as(usize, 1), capture.artifact_writes);
    try std.testing.expectEqual(@as(usize, 0), capture.artifact_delete_keys);
    try std.testing.expectEqualStrings(remote_unit_key, capture.first_key.?);
    try std.testing.expect(std.mem.indexOf(u8, capture.first_value.?, "\"_artifact_route_status\":\"remote_committed\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, capture.first_value.?, "\"_artifact_owner_group_id\":7002") != null);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, remote_unit_key));
}

test "db write path document artifact child range retries remote dispatch from durable outbox" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,YWxwaGE=\"}",
        }},
        .sync_level = .full_index,
    });

    try std.testing.expect(try db.updateDocumentArtifactChildRangePlacement(alloc, "doc:a", "document_units_v1", .{
        .range_id = "range:000000",
        .placement = "remote",
        .owner_group_id = 7002,
        .placement_generation = 7,
        .route_status = "remote_committed",
        .split_eligible = true,
    }));

    var moved = (try db.getDocumentArtifactManifest(alloc, "doc:a", "document_units_v1")) orelse return error.TestUnexpectedResult;
    defer moved.deinit(alloc);
    const remote_unit_key = try alloc.dupe(u8, moved.child_ranges[0].start_key);
    defer alloc.free(remote_unit_key);

    const local_deletes = [_][]const u8{remote_unit_key};
    _ = try db.applyDocumentArtifactChildRangeBatch(.{
        .artifact_delete_keys = local_deletes[0..],
        .sync_level = .write,
    });

    const FlakyCapture = struct {
        fail_next: bool = true,
        calls: usize = 0,
        owner_group_id: u64 = 0,
        artifact_writes: usize = 0,
        first_key: ?[]u8 = null,

        fn deinit(self: *@This(), allocator: Allocator) void {
            if (self.first_key) |key| allocator.free(key);
        }

        fn dispatcher(self: *@This()) DocumentArtifactChildRangeDispatcher {
            return .{ .ptr = self, .apply = apply };
        }

        fn apply(ptr: *anyopaque, allocator: Allocator, dispatch: DocumentArtifactChildRangeDispatch) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            if (self.fail_next) {
                self.fail_next = false;
                return error.IntentionalDispatchFailure;
            }
            self.owner_group_id = dispatch.owner_group_id;
            self.artifact_writes += dispatch.child_batch.artifact_writes.len;
            if (self.first_key == null and dispatch.child_batch.artifact_writes.len > 0) {
                self.first_key = try allocator.dupe(u8, dispatch.child_batch.artifact_writes[0].key);
            }
        }
    };

    var capture = FlakyCapture{};
    defer capture.deinit(alloc);

    try std.testing.expectError(error.IntentionalDispatchFailure, db.batchWithDocumentArtifactChildRangeDispatcher(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,YmV0YQ==\"}",
        }},
        .sync_level = .full_index,
    }, capture.dispatcher()));

    const outbox_prefix = try internal_keys.documentChildRangeOutboxRootPrefixAlloc(alloc);
    defer alloc.free(outbox_prefix);
    const pending = try db.core.scanStorePrefix(alloc, outbox_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, pending);
    try std.testing.expectEqual(@as(usize, 1), pending.len);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, remote_unit_key));

    const drained = try db.drainDocumentArtifactChildRangeOutbox(capture.dispatcher(), 0);
    try std.testing.expectEqual(@as(usize, 1), drained.scanned);
    try std.testing.expectEqual(@as(usize, 1), drained.dispatched);
    try std.testing.expectEqual(@as(usize, 1), drained.deleted);
    try std.testing.expectEqual(@as(usize, 2), capture.calls);
    try std.testing.expectEqual(@as(u64, 7002), capture.owner_group_id);
    try std.testing.expectEqual(@as(usize, 1), capture.artifact_writes);
    try std.testing.expectEqualStrings(remote_unit_key, capture.first_key.?);

    const after = try db.core.scanStorePrefix(alloc, outbox_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, after);
    try std.testing.expectEqual(@as(usize, 0), after.len);
}

test "db write path replay buildDerivedBatch stores thin document and embedding replay records" {
    const alloc = std.testing.allocator;

    const req = types.BatchRequest{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dv_v1\":[1,0],\"sp_v1\":{\"indices\":[1,5],\"values\":[0.5,0.75]}}}" },
        },
    };

    var extracted = try mapper.extractWrite(alloc, req.writes[0].key, req.writes[0].value);
    defer extracted.deinit(alloc);

    extracted.dense_embeddings[0].artifact_key = try alloc.dupe(u8, "artifact:dense:doc:a");
    extracted.sparse_embeddings[0].artifact_key = try alloc.dupe(u8, "artifact:sparse:doc:a");

    var derived_batch = try db_internal.buildDerivedBatch(alloc, req, &.{extracted}, &.{}, &.{});
    defer derived_types.deinitDerivedBatch(alloc, &derived_batch);

    try std.testing.expectEqual(@as(usize, 1), derived_batch.documents.len);
    try std.testing.expectEqual(derived_types.DerivedAction.upsert, derived_batch.documents[0].action);
    try std.testing.expect(derived_batch.documents[0].cleaned_value == null);

    try std.testing.expectEqual(@as(usize, 1), derived_batch.dense_embeddings.len);
    try std.testing.expectEqualStrings("artifact:dense:doc:a", derived_batch.dense_embeddings[0].artifact_key.?);
    try std.testing.expectEqual(@as(usize, 0), derived_batch.dense_embeddings[0].vector.len);

    try std.testing.expectEqual(@as(usize, 1), derived_batch.sparse_embeddings.len);
    try std.testing.expectEqualStrings("artifact:sparse:doc:a", derived_batch.sparse_embeddings[0].artifact_key.?);
    try std.testing.expectEqual(@as(usize, 2), derived_batch.sparse_embeddings[0].indices.len);
    try std.testing.expectEqual(@as(u32, 1), derived_batch.sparse_embeddings[0].indices[0]);
    try std.testing.expectEqual(@as(u32, 5), derived_batch.sparse_embeddings[0].indices[1]);
    try std.testing.expectEqual(@as(usize, 2), derived_batch.sparse_embeddings[0].values.len);
    try std.testing.expectEqual(@as(f32, 0.5), derived_batch.sparse_embeddings[0].values[0]);
    try std.testing.expectEqual(@as(f32, 0.75), derived_batch.sparse_embeddings[0].values[1]);
}

test "db write path batch appends only thin replay stream records" {
    const DB = @import("mod.zig").DB;
    const replay_stream_mod = @import("derived/replay_stream.zig");
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"machine learning\"}" },
        },
    });

    const entries = try replay_stream_mod.iterateFrom(alloc, db.core.store, 1);
    defer {
        for (entries) |*entry| entry.deinit(alloc);
        alloc.free(entries);
    }
    try std.testing.expectEqual(@as(usize, 1), entries.len);
}

test "db write path batch writes thin change journal record" {
    const DB = @import("mod.zig").DB;
    const replay_stream_mod = @import("derived/replay_stream.zig");
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"machine learning\"}" },
        },
    });

    const entries = try replay_stream_mod.iterateFrom(alloc, db.core.store, 1);
    defer {
        for (entries) |*entry| entry.deinit(alloc);
        alloc.free(entries);
    }

    try std.testing.expectEqual(@as(usize, 1), entries.len);
    var record = try change_journal_mod.decodeRecord(alloc, entries[0].payload);
    defer record.deinit();

    try std.testing.expectEqual(@as(u64, 1), record.record.sequence);
    try std.testing.expectEqual(@as(usize, 1), record.record.changed_doc_keys.len);
    try std.testing.expectEqualStrings("doc:a", record.record.changed_doc_keys[0]);
    try std.testing.expectEqual(@as(usize, 0), record.record.changed_artifact_keys.len);
}

test "db write path batch uses change journal as the replay authority" {
    const DB = @import("mod.zig").DB;
    const replay_stream_mod = @import("derived/replay_stream.zig");
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"machine learning\"}" },
        },
    });

    const entries = try replay_stream_mod.iterateFrom(alloc, db.core.store, 1);
    defer {
        for (entries) |*entry| entry.deinit(alloc);
        alloc.free(entries);
    }

    try std.testing.expectEqual(@as(usize, 1), entries.len);
}

test "db write path direct graph writes record graph artifacts in the replay stream instead of graph payload replay" {
    const DB = @import("mod.zig").DB;
    const replay_stream_mod = @import("derived/replay_stream.zig");
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "gr_v1",
        .kind = .graph,
        .config_json = "{}",
    });

    try db.batch(.{
        .graph_writes = &.{
            .{ .index_name = "gr_v1", .source = "doc:a", .target = "doc:b", .edge_type = "links", .weight = 1.0 },
        },
        .sync_level = .write,
    });

    const journal_entries = try replay_stream_mod.iterateFrom(alloc, db.core.store, 1);
    defer {
        for (journal_entries) |*entry| entry.deinit(alloc);
        alloc.free(journal_entries);
    }

    try std.testing.expectEqual(@as(usize, 1), journal_entries.len);
    var journal_record = try change_journal_mod.decodeRecord(alloc, journal_entries[0].payload);
    defer journal_record.deinit();

    try std.testing.expectEqual(@as(usize, 1), journal_record.record.changed_artifact_keys.len);
    try std.testing.expect(internal_keys.isGraphEdgeArtifactKey(journal_record.record.changed_artifact_keys[0]));
}

test "db write path _edges writes record graph artifacts in the replay stream instead of graph payload replay" {
    const DB = @import("mod.zig").DB;
    const replay_stream_mod = @import("derived/replay_stream.zig");
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "gr_v1",
        .kind = .graph,
        .config_json = "{}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"gr_v1\":{\"links\":[{\"target\":\"doc:b\"},{\"target\":\"doc:c\"}]}}}" },
        },
        .sync_level = .write,
    });

    const journal_entries = try replay_stream_mod.iterateFrom(alloc, db.core.store, 1);
    defer {
        for (journal_entries) |*entry| entry.deinit(alloc);
        alloc.free(journal_entries);
    }

    try std.testing.expectEqual(@as(usize, 1), journal_entries.len);
    var journal_record = try change_journal_mod.decodeRecord(alloc, journal_entries[0].payload);
    defer journal_record.deinit();

    try std.testing.expectEqual(@as(usize, 2), journal_record.record.changed_artifact_keys.len);
    for (journal_record.record.changed_artifact_keys) |artifact_key| {
        try std.testing.expect(internal_keys.isGraphEdgeArtifactKey(artifact_key));
    }
}

test "db write path replay encodeThinReplayRecordPayload preserves async write replay contract" {
    const alloc = std.testing.allocator;

    const req = types.BatchRequest{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dv_v1\":[1,0],\"sp_v1\":{\"indices\":[1,5],\"values\":[0.5,0.75]}}}" },
        },
        .deletes = &.{"doc:gone"},
    };

    var extracted = try mapper.extractWrite(alloc, req.writes[0].key, req.writes[0].value);
    defer extracted.deinit(alloc);

    extracted.dense_embeddings[0].artifact_key = try alloc.dupe(u8, "artifact:dense:doc:a");
    extracted.sparse_embeddings[0].artifact_key = try alloc.dupe(u8, "artifact:sparse:doc:a");

    const payload = try encodeThinReplayRecordPayload(
        alloc,
        req,
        &.{extracted},
        &.{},
        &.{},
        &.{true},
        42,
        false,
    );
    defer alloc.free(payload);

    var decoded = try change_journal_mod.decodeRecord(alloc, payload);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u64, 42), decoded.record.sequence);
    try std.testing.expectEqual(@as(usize, 1), decoded.record.changed_doc_keys.len);
    try std.testing.expectEqualStrings("doc:a", decoded.record.changed_doc_keys[0]);
    try std.testing.expectEqual(@as(usize, 1), decoded.record.deleted_doc_keys.len);
    try std.testing.expectEqualStrings("doc:gone", decoded.record.deleted_doc_keys[0]);
    try std.testing.expectEqual(@as(usize, 1), decoded.record.overwritten_doc_keys.len);
    try std.testing.expectEqualStrings("doc:a", decoded.record.overwritten_doc_keys[0]);
    try std.testing.expectEqual(@as(usize, 2), decoded.record.changed_artifact_keys.len);
    try std.testing.expectEqualStrings("artifact:dense:doc:a", decoded.record.changed_artifact_keys[0]);
    try std.testing.expectEqualStrings("artifact:sparse:doc:a", decoded.record.changed_artifact_keys[1]);
    try std.testing.expect(change_journal_mod.recordHasHint(decoded.record, .full_text));
    try std.testing.expect(change_journal_mod.recordHasHint(decoded.record, .dense_vector));
    try std.testing.expect(change_journal_mod.recordHasHint(decoded.record, .sparse_vector));
    try std.testing.expect(!change_journal_mod.recordHasHint(decoded.record, .enrichment));
}

test "db write path replay encodeThinReplayRecordPayload treats embedding-only writes as artifact replay" {
    const alloc = std.testing.allocator;

    const req = types.BatchRequest{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"_embeddings\":{\"dv_v1\":[1,0]}}" },
        },
    };

    var extracted = try mapper.extractWrite(alloc, req.writes[0].key, req.writes[0].value);
    defer extracted.deinit(alloc);

    extracted.dense_embeddings[0].artifact_key = try alloc.dupe(u8, "artifact:dense:doc:a");

    const payload = try encodeThinReplayRecordPayload(
        alloc,
        req,
        &.{extracted},
        &.{},
        &.{},
        &.{false},
        43,
        false,
    );
    defer alloc.free(payload);

    var decoded = try change_journal_mod.decodeRecord(alloc, payload);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u64, 43), decoded.record.sequence);
    try std.testing.expectEqual(@as(usize, 0), decoded.record.changed_doc_keys.len);
    try std.testing.expectEqual(@as(usize, 1), decoded.record.changed_artifact_keys.len);
    try std.testing.expectEqualStrings("artifact:dense:doc:a", decoded.record.changed_artifact_keys[0]);
    try std.testing.expect(!change_journal_mod.recordHasHint(decoded.record, .full_text));
    try std.testing.expect(change_journal_mod.recordHasHint(decoded.record, .dense_vector));
}

test "db write path replay thin replay marks artifact-derived target hints" {
    const alloc = std.testing.allocator;

    const asset_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "relations_v1");
    defer alloc.free(asset_key);
    const graph_key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:a", "relations_graph", "mentions", "doc:b");
    defer alloc.free(graph_key);

    const payload = try encodeThinReplayRecordPayload(
        alloc,
        .{},
        &.{},
        &.{ asset_key, graph_key },
        &.{},
        &.{},
        44,
        false,
    );
    defer alloc.free(payload);

    var decoded = try change_journal_mod.decodeRecord(alloc, payload);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u64, 44), decoded.record.sequence);
    try std.testing.expectEqual(@as(usize, 2), decoded.record.changed_artifact_keys.len);
    try std.testing.expectEqualStrings(asset_key, decoded.record.changed_artifact_keys[0]);
    try std.testing.expectEqualStrings(graph_key, decoded.record.changed_artifact_keys[1]);
    try std.testing.expect(change_journal_mod.recordHasHint(decoded.record, .resolution));
    try std.testing.expect(change_journal_mod.recordHasHint(decoded.record, .graph));
}

test "db write path replay encodeThinReplayRecordPayload marks generated enrichment replay for async writes" {
    const alloc = std.testing.allocator;

    const req = types.BatchRequest{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"needs generated embedding\"}" },
        },
    };

    var extracted = try mapper.extractWrite(alloc, req.writes[0].key, req.writes[0].value);
    defer extracted.deinit(alloc);

    const payload = try encodeThinReplayRecordPayload(
        alloc,
        req,
        &.{extracted},
        &.{},
        &.{},
        &.{false},
        7,
        true,
    );
    defer alloc.free(payload);

    var decoded = try change_journal_mod.decodeRecord(alloc, payload);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u64, 7), decoded.record.sequence);
    try std.testing.expectEqual(@as(usize, 1), decoded.record.changed_doc_keys.len);
    try std.testing.expectEqualStrings("doc:a", decoded.record.changed_doc_keys[0]);
    try std.testing.expect(change_journal_mod.recordHasHint(decoded.record, .full_text));
    try std.testing.expect(change_journal_mod.recordHasHint(decoded.record, .enrichment));
}

test "db chunk cache keys preserve embedded separators" {
    const alloc = std.testing.allocator;

    const left = try makeChunkCacheKey(alloc, .{
        .kind = .chunk_text,
        .index_name = "idx",
        .artifact_name = "artifact",
        .doc_key = "doc\x1fartifact",
        .source_field = "body",
        .chunk_size = 64,
        .chunk_overlap = 8,
        .chunker_json = "{\"mode\":\"a\"}",
    });
    defer alloc.free(left);

    const right = try makeChunkCacheKey(alloc, .{
        .kind = .chunk_text,
        .index_name = "idx",
        .artifact_name = "artifact\x1fartifact",
        .doc_key = "doc",
        .source_field = "body",
        .chunk_size = 64,
        .chunk_overlap = 8,
        .chunker_json = "{\"mode\":\"a\"}",
    });
    defer alloc.free(right);

    try std.testing.expect(!std.mem.eql(u8, left, right));
}

fn getOrCreateChunks(
    alloc: Allocator,
    db: anytype,
    doc_value: []const u8,
    request: enrichment_types.GeneratedEnrichmentRequest,
    cache: *std.ArrayListUnmanaged(ChunkCacheEntry),
) ![]chunker_mod.Chunk {
    const cache_key = try makeChunkCacheKey(alloc, request);
    errdefer alloc.free(cache_key);

    for (cache.items) |entry| {
        if (std.mem.eql(u8, entry.key, cache_key)) {
            alloc.free(cache_key);
            return entry.chunks;
        }
    }

    const source_text = if (request.source_template.len > 0)
        renderSourceTemplateText(alloc, db.secret_store, db.remote_content, request.source_template, doc_value) catch |err| switch (err) {
            error.PermanentPromptFailure, error.TransientPromptFailure => return err,
            else => null,
        }
    else
        try extractStringField(alloc, doc_value, request.source_field);
    if (source_text == null or source_text.?.len == 0) {
        if (source_text) |s| alloc.free(s);
        const empty = try alloc.alloc(chunker_mod.Chunk, 0);
        try cache.append(alloc, .{
            .key = cache_key,
            .chunks = empty,
        });
        return cache.items[cache.items.len - 1].chunks;
    }
    defer alloc.free(source_text.?);

    const chunks = if (request.chunker_json.len > 0)
        try chunker_mod.chunkTextWithConfigJson(alloc, source_text.?, request.chunker_json)
    else
        try chunker_mod.chunkText(alloc, source_text.?, request.chunk_size, request.chunk_overlap);
    try cache.append(alloc, .{
        .key = cache_key,
        .chunks = chunks,
    });
    return cache.items[cache.items.len - 1].chunks;
}

fn shouldStoreChunkArtifacts(alloc: Allocator, request: enrichment_types.GeneratedEnrichmentRequest) !bool {
    if (request.full_text_index) return true;
    if (request.chunker_json.len == 0) return true;
    if (try chunking_types_mod.parseHasFullTextIndexFromSlice(alloc, request.chunker_json)) return true;
    return try chunking_types_mod.parseStoreChunksFromSlice(alloc, request.chunker_json);
}

fn appendChunkArtifactWrites(
    alloc: Allocator,
    doc_key: []const u8,
    source_field: []const u8,
    artifact_name: []const u8,
    chunks: []const chunker_mod.Chunk,
    artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
    include_payload: bool,
) !void {
    var arena_state = std.heap.ArenaAllocator.init(alloc);
    defer arena_state.deinit();
    const scratch = arena_state.allocator();

    for (chunks) |chunk| {
        const key = try internal_keys.chunkArtifactKeyAlloc(alloc, doc_key, artifact_name, @intCast(chunk.chunk_id));
        defer alloc.free(key);
        const payload = try buildChunkArtifactPayloadAlloc(scratch, doc_key, artifact_name, source_field, chunk, include_payload);

        try artifact_writes.append(alloc, .{
            .key = try alloc.dupe(u8, key),
            .value = try alloc.dupe(u8, payload),
        });

        _ = arena_state.reset(.retain_capacity);
    }
}

fn buildChunkArtifactPayloadAlloc(
    alloc: Allocator,
    doc_key: []const u8,
    artifact_name: []const u8,
    source_field: []const u8,
    chunk: chunker_mod.Chunk,
    include_payload: bool,
) ![]u8 {
    var obj = std.json.ObjectMap.empty;
    try obj.put(alloc, try alloc.dupe(u8, "_parent_doc_key"), .{ .string = try alloc.dupe(u8, doc_key) });
    try obj.put(alloc, try alloc.dupe(u8, "_artifact_name"), .{ .string = try alloc.dupe(u8, artifact_name) });
    try obj.put(alloc, try alloc.dupe(u8, "_source_field"), .{ .string = try alloc.dupe(u8, source_field) });
    try chunk_artifact_mod.appendArtifactFields(alloc, &obj, source_field, chunk, include_payload);
    return try std.json.Stringify.valueAlloc(alloc, std.json.Value{ .object = obj }, .{});
}

pub fn appendDerivedDenseEmbeddingForConsumers(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(derived_types.DerivedDenseEmbeddingWrite),
    doc_key: []const u8,
    parent_doc_key: ?[]const u8,
    artifact_key: []const u8,
    vector: []const f32,
    consumer_indexes: []const []const u8,
) !void {
    _ = vector;
    for (consumer_indexes) |index_name| {
        try out.append(alloc, .{
            .index_name = try alloc.dupe(u8, index_name),
            .parent_doc_key = if (parent_doc_key) |key| try alloc.dupe(u8, key) else null,
            .doc_key = try alloc.dupe(u8, doc_key),
            .artifact_key = try alloc.dupe(u8, artifact_key),
            .vector = &.{},
        });
    }
}

pub fn appendDerivedSparseEmbeddingForConsumers(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(derived_types.DerivedSparseEmbeddingWrite),
    doc_key: []const u8,
    artifact_key: []const u8,
    indices: []const u32,
    values: []const f32,
    consumer_indexes: []const []const u8,
) !void {
    _ = indices;
    _ = values;
    for (consumer_indexes) |index_name| {
        try out.append(alloc, .{
            .index_name = try alloc.dupe(u8, index_name),
            .doc_key = try alloc.dupe(u8, doc_key),
            .artifact_key = try alloc.dupe(u8, artifact_key),
            .indices = &.{},
            .values = &.{},
        });
    }
}

fn computeChunkRequestDerived(
    alloc: Allocator,
    db: anytype,
    doc_value: []const u8,
    request: enrichment_types.GeneratedEnrichmentRequest,
    artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
    documents: *std.ArrayListUnmanaged(derived_types.DerivedDocument),
    cache: *std.ArrayListUnmanaged(ChunkCacheEntry),
) !void {
    if (!requestHasChunking(request)) return;

    const artifact_name = requestArtifactName(request);
    const chunks = try getOrCreateChunks(alloc, db, doc_value, request, cache);
    if (chunks.len == 0) return;

    const persist_chunks = try shouldStoreChunkArtifacts(alloc, request);
    if (persist_chunks) {
        try appendChunkArtifactWrites(alloc, request.doc_key, request.source_field, artifact_name, chunks, artifact_writes, true);
    }

    const include_default_full_text = request.full_text_index or
        try chunking_types_mod.parseHasFullTextIndexFromSlice(alloc, request.chunker_json);
    const text_indexes = try db.core.index_manager.textIndexesForChunk(alloc, artifact_name, include_default_full_text);
    defer {
        for (text_indexes) |name| alloc.free(name);
        alloc.free(text_indexes);
    }
    if (text_indexes.len == 0) return;

    var arena_state = std.heap.ArenaAllocator.init(alloc);
    defer arena_state.deinit();
    const scratch = arena_state.allocator();

    for (chunks) |chunk| {
        if (!chunk.isText()) continue;
        const key = try internal_keys.chunkArtifactKeyAlloc(alloc, request.doc_key, artifact_name, @intCast(chunk.chunk_id));
        defer alloc.free(key);

        const targets = try alloc.alloc(derived_types.DerivedTargetRef, text_indexes.len);
        errdefer {
            for (targets) |target| alloc.free(target.index_name);
            alloc.free(targets);
        }
        for (text_indexes, 0..) |index_name, i| {
            targets[i] = .{
                .kind = .full_text,
                .index_name = try alloc.dupe(u8, index_name),
            };
        }
        const payload = try buildChunkArtifactPayloadAlloc(scratch, request.doc_key, artifact_name, request.source_field, chunk, true);

        try documents.append(alloc, .{
            .key = try alloc.dupe(u8, key),
            .action = .upsert,
            .cleaned_value = try alloc.dupe(u8, payload),
            .targets = targets,
        });

        _ = arena_state.reset(.retain_capacity);
    }
}

fn extractAssetSourceValue(
    alloc: Allocator,
    db: anytype,
    doc_value: []const u8,
    request: enrichment_types.GeneratedEnrichmentRequest,
) !?[]u8 {
    if (request.source_template.len > 0) {
        const rendered = renderSourceTemplateText(alloc, db.secret_store, db.remote_content, request.source_template, doc_value) catch |err| switch (err) {
            error.PermanentPromptFailure, error.TransientPromptFailure => return err,
            else => return null,
        };
        errdefer alloc.free(rendered);
        try document_extraction_mod.validateInlineSourceSize(db.remote_content, rendered);
        return @constCast(rendered);
    }

    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, doc_value, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return null;
    const source = jsonValueAtPath(parsed.value, request.source_field) orelse return null;
    return switch (source) {
        .null => null,
        .string => |value| blk: {
            try document_extraction_mod.validateInlineSourceSize(db.remote_content, value);
            break :blk try alloc.dupe(u8, value);
        },
        else => blk: {
            const rendered = try std.json.Stringify.valueAlloc(alloc, source, .{});
            errdefer alloc.free(rendered);
            try document_extraction_mod.validateInlineSourceSize(db.remote_content, rendered);
            break :blk rendered;
        },
    };
}

fn assetStateValueAlloc(
    alloc: Allocator,
    source_text: []const u8,
    source_parts_json: ?[]const u8,
    producer_json: []const u8,
) ![]u8 {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update(source_text);
    if (source_parts_json) |parts| hasher.update(parts);
    hasher.update(producer_json);
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    return try alloc.dupe(u8, &digest);
}

pub fn computeChunkRequest(
    alloc: Allocator,
    db: anytype,
    doc_value: []const u8,
    request: enrichment_types.GeneratedEnrichmentRequest,
    artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
    documents: *std.ArrayListUnmanaged(types.EnrichmentDocumentWrite),
    cache: *std.ArrayListUnmanaged(ChunkCacheEntry),
) !void {
    if (!requestHasChunking(request)) return;

    const artifact_name = requestArtifactName(request);
    const chunks = try getOrCreateChunks(alloc, db, doc_value, request, cache);
    if (chunks.len == 0) return;

    const persist_chunks = try shouldStoreChunkArtifacts(alloc, request);
    if (persist_chunks) {
        try appendChunkArtifactWrites(alloc, request.doc_key, request.source_field, artifact_name, chunks, artifact_writes, true);
    }

    const include_default_full_text = try chunking_types_mod.parseHasFullTextIndexFromSlice(alloc, request.chunker_json);
    const text_indexes = try db.core.index_manager.textIndexesForChunk(alloc, artifact_name, include_default_full_text);
    defer {
        for (text_indexes) |name| alloc.free(name);
        alloc.free(text_indexes);
    }
    if (text_indexes.len == 0) return;

    var arena_state = std.heap.ArenaAllocator.init(alloc);
    defer arena_state.deinit();
    const scratch = arena_state.allocator();

    for (chunks) |chunk| {
        if (!chunk.isText()) continue;
        const key = try internal_keys.chunkArtifactKeyAlloc(alloc, request.doc_key, artifact_name, @intCast(chunk.chunk_id));
        defer alloc.free(key);

        var doc = types.EnrichmentDocumentWrite{
            .key = try alloc.dupe(u8, key),
            .value = undefined,
            .target_index_names = try alloc.alloc([]u8, text_indexes.len),
        };
        errdefer doc.deinit(alloc);

        for (text_indexes, 0..) |index_name, i| {
            doc.target_index_names[i] = try alloc.dupe(u8, index_name);
        }
        const payload = try buildChunkArtifactPayloadAlloc(scratch, request.doc_key, artifact_name, request.source_field, chunk, true);
        doc.value = try alloc.dupe(u8, payload);

        try documents.append(alloc, doc);

        _ = arena_state.reset(.retain_capacity);
    }
}

fn appendDenseEmbeddingForConsumers(
    alloc: Allocator,
    dense_embeddings: *std.ArrayListUnmanaged(types.EnrichmentDenseEmbeddingWrite),
    doc_key: []const u8,
    parent_doc_key: ?[]const u8,
    artifact_key: []const u8,
    vector: []const f32,
    consumer_indexes: []const []const u8,
) !void {
    _ = parent_doc_key;
    const public_artifact = try artifact_ids.resolvePublicArtifactIdentityAlloc(alloc, artifact_key);
    defer {
        var owned = public_artifact;
        owned.deinit(alloc);
    }

    for (consumer_indexes) |index_name| {
        try dense_embeddings.append(alloc, .{
            .index_name = try alloc.dupe(u8, index_name),
            .doc_key = try alloc.dupe(u8, doc_key),
            .artifact_id = try alloc.dupe(u8, public_artifact.id),
            .artifact_ref = try public_artifact.artifact_ref.?.clone(alloc),
            .vector = try alloc.dupe(f32, vector),
        });
    }
}

pub fn externalizeArtifactWritesAlloc(alloc: Allocator, writes: []types.BatchWrite) ![]types.ArtifactWrite {
    var out = try alloc.alloc(types.ArtifactWrite, writes.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*write| write.deinit(alloc);
        alloc.free(out);
        for (writes[initialized..]) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        alloc.free(writes);
    }

    for (writes, 0..) |write, i| {
        var identity = try artifact_ids.resolvePublicArtifactIdentityAlloc(alloc, write.key);
        defer identity.deinit(alloc);

        out[i] = .{
            .id = try alloc.dupe(u8, identity.id),
            .value = @constCast(write.value),
            .artifact_ref = try identity.artifact_ref.?.clone(alloc),
        };
        initialized += 1;
        alloc.free(@constCast(write.key));
    }

    alloc.free(writes);
    return out;
}

pub fn computeDenseRequest(
    alloc: Allocator,
    db: anytype,
    doc_value: []const u8,
    request: enrichment_types.GeneratedEnrichmentRequest,
    artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
    dense_embeddings: *std.ArrayListUnmanaged(types.EnrichmentDenseEmbeddingWrite),
    cache: *std.ArrayListUnmanaged(ChunkCacheEntry),
) !void {
    return computeDenseRequestImpl(alloc, db, doc_value, request, artifact_writes, dense_embeddings, cache, appendDenseEmbeddingForConsumers);
}

fn computeDenseRequestDerived(
    alloc: Allocator,
    db: anytype,
    doc_value: []const u8,
    request: enrichment_types.GeneratedEnrichmentRequest,
    artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
    dense_embeddings: *std.ArrayListUnmanaged(derived_types.DerivedDenseEmbeddingWrite),
    cache: *std.ArrayListUnmanaged(ChunkCacheEntry),
) !void {
    return computeDenseRequestImpl(alloc, db, doc_value, request, artifact_writes, dense_embeddings, cache, appendDerivedDenseEmbeddingForConsumers);
}

fn computeDenseRequestImpl(
    alloc: Allocator,
    db: anytype,
    doc_value: []const u8,
    request: enrichment_types.GeneratedEnrichmentRequest,
    artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
    dense_embeddings: anytype,
    cache: *std.ArrayListUnmanaged(ChunkCacheEntry),
    comptime appendForConsumers: anytype,
) !void {
    const dense_embedder = if (db.enrichment_runtime) |runtime|
        runtime.config.dense_embedder orelse return error.MissingDenseEmbedder
    else
        return error.MissingDenseEmbedder;

    const embedding_name = requestEmbeddingName(request);
    const consumer_indexes = try db.core.index_manager.denseIndexesForEmbedding(alloc, embedding_name, request.expected_dims);
    defer {
        for (consumer_indexes) |index_name| alloc.free(index_name);
        alloc.free(consumer_indexes);
    }
    if (consumer_indexes.len == 0) return;

    if (requestHasChunking(request) and requestArtifactName(request).len > 0) {
        const chunks = try getOrCreateChunks(alloc, db, doc_value, request, cache);
        var chunk_texts = std.ArrayListUnmanaged([]const u8).empty;
        defer chunk_texts.deinit(alloc);
        var chunk_keys = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (chunk_keys.items) |chunk_key| alloc.free(chunk_key);
            chunk_keys.deinit(alloc);
        }

        for (chunks) |chunk| {
            const chunk_text = chunk.text orelse continue;
            try chunk_texts.append(alloc, chunk_text);
            try chunk_keys.append(alloc, try internal_keys.chunkArtifactKeyAlloc(alloc, request.doc_key, requestArtifactName(request), @intCast(chunk.chunk_id)));
        }
        if (chunk_texts.items.len == 0) return;

        const vectors = try dense_embedder.embedDenseBatch(alloc, embedding_name, chunk_texts.items, request.expected_dims);
        defer embedder_mod.freeDenseEmbeddingBatch(alloc, vectors);
        if (vectors.len != chunk_keys.items.len) return error.InvalidEmbeddingResponse;

        for (chunk_keys.items, chunk_texts.items, vectors) |chunk_key, chunk_text, vector| {
            const artifact_key = try appendEmbeddingArtifactWrite(
                alloc,
                artifact_writes,
                chunk_key,
                request.doc_key,
                embedding_name,
                request.source_field,
                chunk_key,
                enrichment_artifact_codec.hashSource(chunk_text),
                vector,
            );
            defer alloc.free(artifact_key);
            try appendForConsumers(alloc, dense_embeddings, chunk_key, request.doc_key, artifact_key, vector, consumer_indexes);
        }
        return;
    }

    if (request.source_template.len > 0 and dense_embedder.supportsParts()) {
        const source_parts = renderSourceParts(alloc, db, doc_value, request) catch |err| switch (err) {
            error.PermanentPromptFailure, error.TransientPromptFailure => return err,
            else => null,
        };
        if (source_parts) |parts| {
            defer template_mod.freeContentParts(alloc, parts);

            const vector = try dense_embedder.embedDenseParts(alloc, embedding_name, parts, request.expected_dims);
            defer alloc.free(vector);
            const artifact_key = try appendEmbeddingArtifactWrite(
                alloc,
                artifact_writes,
                request.doc_key,
                request.doc_key,
                embedding_name,
                request.source_field,
                null,
                null,
                vector,
            );
            defer alloc.free(artifact_key);
            try appendForConsumers(alloc, dense_embeddings, request.doc_key, null, artifact_key, vector, consumer_indexes);
            return;
        }
    }

    const source_text = if (request.source_template.len > 0)
        renderSourceTemplateText(alloc, db.secret_store, db.remote_content, request.source_template, doc_value) catch |err| switch (err) {
            error.PermanentPromptFailure, error.TransientPromptFailure => return err,
            else => null,
        }
    else
        try extractStringField(alloc, doc_value, request.source_field);
    if (source_text == null or source_text.?.len == 0) {
        if (source_text) |s| alloc.free(s);
        return;
    }
    defer alloc.free(source_text.?);

    const vector = try dense_embedder.embedDense(alloc, embedding_name, source_text.?, request.expected_dims);
    defer alloc.free(vector);
    const artifact_key = try appendEmbeddingArtifactWrite(
        alloc,
        artifact_writes,
        request.doc_key,
        request.doc_key,
        embedding_name,
        request.source_field,
        null,
        enrichment_artifact_codec.hashSource(source_text.?),
        vector,
    );
    defer alloc.free(artifact_key);
    try appendForConsumers(alloc, dense_embeddings, request.doc_key, null, artifact_key, vector, consumer_indexes);
}

fn computeSparseRequestDerived(
    alloc: Allocator,
    db: anytype,
    doc_value: []const u8,
    request: enrichment_types.GeneratedEnrichmentRequest,
    artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
    sparse_embeddings: *std.ArrayListUnmanaged(derived_types.DerivedSparseEmbeddingWrite),
    cache: *std.ArrayListUnmanaged(ChunkCacheEntry),
) !void {
    const sparse_embedder = if (db.enrichment_runtime) |runtime|
        runtime.config.sparse_embedder orelse return error.MissingSparseEmbedder
    else
        return error.MissingSparseEmbedder;

    const embedding_name = requestEmbeddingName(request);
    const consumer_indexes = try db.core.index_manager.sparseIndexesForEmbedding(alloc, embedding_name);
    defer {
        for (consumer_indexes) |index_name| alloc.free(index_name);
        alloc.free(consumer_indexes);
    }
    if (consumer_indexes.len == 0) return;

    if (requestHasChunking(request) and requestArtifactName(request).len > 0) {
        const chunks = try getOrCreateChunks(alloc, db, doc_value, request, cache);
        var chunk_texts = std.ArrayListUnmanaged([]const u8).empty;
        defer chunk_texts.deinit(alloc);
        var chunk_keys = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (chunk_keys.items) |chunk_key| alloc.free(chunk_key);
            chunk_keys.deinit(alloc);
        }

        for (chunks) |chunk| {
            const chunk_text = chunk.text orelse continue;
            try chunk_texts.append(alloc, chunk_text);
            try chunk_keys.append(alloc, try internal_keys.chunkArtifactKeyAlloc(alloc, request.doc_key, requestArtifactName(request), @intCast(chunk.chunk_id)));
        }
        if (chunk_texts.items.len == 0) return;

        const sparse_batch = try sparse_embedder.embedSparseBatch(alloc, embedding_name, chunk_texts.items);
        defer embedder_mod.freeSparseEmbeddingBatch(alloc, sparse_batch);
        if (sparse_batch.len != chunk_keys.items.len) return error.InvalidEmbeddingResponse;

        for (chunk_keys.items, sparse_batch) |chunk_key, sparse| {
            const artifact_key = try appendSparseEmbeddingArtifactWrite(
                alloc,
                artifact_writes,
                chunk_key,
                embedding_name,
                null,
                sparse.indices,
                sparse.values,
            );
            defer alloc.free(artifact_key);
            try appendDerivedSparseEmbeddingForConsumers(alloc, sparse_embeddings, chunk_key, artifact_key, sparse.indices, sparse.values, consumer_indexes);
        }
        return;
    }

    const source_text = if (request.source_template.len > 0)
        renderSourceTemplateText(alloc, db.secret_store, db.remote_content, request.source_template, doc_value) catch |err| switch (err) {
            error.PermanentPromptFailure, error.TransientPromptFailure => return err,
            else => null,
        }
    else
        try extractStringField(alloc, doc_value, request.source_field);
    if (source_text == null or source_text.?.len == 0) {
        if (source_text) |s| alloc.free(s);
        return;
    }
    defer alloc.free(source_text.?);

    var sparse = try sparse_embedder.embedSparse(alloc, embedding_name, source_text.?);
    defer sparse.deinit(alloc);
    const artifact_key = try appendSparseEmbeddingArtifactWrite(
        alloc,
        artifact_writes,
        request.doc_key,
        embedding_name,
        null,
        sparse.indices,
        sparse.values,
    );
    defer alloc.free(artifact_key);
    try appendDerivedSparseEmbeddingForConsumers(alloc, sparse_embeddings, request.doc_key, artifact_key, sparse.indices, sparse.values, consumer_indexes);
}

pub fn takeOwnedSlice(comptime T: type, alloc: Allocator, existing: []const T, incoming: *[]const T) ![]const T {
    if (incoming.*.len == 0) return existing;
    if (existing.len == 0) {
        const out = incoming.*;
        incoming.* = &.{};
        return out;
    }

    const out = try alloc.alloc(T, existing.len + incoming.*.len);
    @memcpy(out[0..existing.len], existing);
    @memcpy(out[existing.len..], incoming.*);
    alloc.free(existing);
    alloc.free(incoming.*);
    incoming.* = &.{};
    return out;
}

fn renderSourceParts(
    alloc: Allocator,
    db: anytype,
    doc_value: []const u8,
    request: enrichment_types.GeneratedEnrichmentRequest,
) !?[]template_mod.ContentPart {
    if (request.source_template.len == 0) return null;
    const parts = renderSourceTemplateParts(alloc, db.secret_store, db.remote_content, request.source_template, doc_value) catch |err| switch (err) {
        error.PermanentPromptFailure, error.TransientPromptFailure => return err,
        else => return null,
    };
    if (parts.len == 0) {
        template_mod.freeContentParts(alloc, parts);
        return null;
    }
    return parts;
}

fn renderSourcePartsJson(
    alloc: Allocator,
    db: anytype,
    doc_value: []const u8,
    request: enrichment_types.GeneratedEnrichmentRequest,
) !?[]u8 {
    const parts = try renderSourceParts(alloc, db, doc_value, request) orelse return null;
    defer template_mod.freeContentParts(alloc, parts);
    return try contentPartsJsonAlloc(alloc, parts);
}

fn contentPartsJsonAlloc(alloc: Allocator, parts: []const template_mod.ContentPart) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.append(alloc, '[');
    for (parts, 0..) |part, i| {
        if (i > 0) try out.append(alloc, ',');
        switch (part) {
            .text => |text| {
                try out.appendSlice(alloc, "{\"type\":\"text\",\"text\":");
                try appendJsonString(alloc, &out, text);
                try out.append(alloc, '}');
            },
            .media_url => |url| {
                try out.appendSlice(alloc, "{\"type\":\"media\",\"url\":");
                try appendJsonString(alloc, &out, url);
                try out.append(alloc, '}');
            },
            .binary => |binary| {
                const encoded_len = std.base64.standard.Encoder.calcSize(binary.data.len);
                const encoded = try alloc.alloc(u8, encoded_len);
                defer alloc.free(encoded);
                _ = std.base64.standard.Encoder.encode(encoded, binary.data);
                try out.appendSlice(alloc, "{\"type\":\"media\",\"mime_type\":");
                try appendJsonString(alloc, &out, binary.mime_type);
                try out.appendSlice(alloc, ",\"data\":");
                try appendJsonString(alloc, &out, encoded);
                try out.append(alloc, '}');
            },
        }
    }
    try out.append(alloc, ']');
    return try out.toOwnedSlice(alloc);
}

fn appendJsonString(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
    defer alloc.free(encoded);
    try out.appendSlice(alloc, encoded);
}

pub fn appendJsonFieldName(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), first: *bool, name: []const u8) !void {
    if (first.*) {
        first.* = false;
    } else {
        try out.append(alloc, ',');
    }
    try appendJsonString(alloc, out, name);
    try out.append(alloc, ':');
}

pub fn appendJsonFieldString(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), first: *bool, name: []const u8, value: []const u8) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try appendJsonString(alloc, out, value);
}

pub fn appendJsonFieldU64(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), first: *bool, name: []const u8, value: u64) !void {
    try appendJsonFieldName(alloc, out, first, name);
    const rendered = try std.fmt.allocPrint(alloc, "{d}", .{value});
    defer alloc.free(rendered);
    try out.appendSlice(alloc, rendered);
}

pub fn appendJsonFieldUsize(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), first: *bool, name: []const u8, value: usize) !void {
    try appendJsonFieldName(alloc, out, first, name);
    const rendered = try std.fmt.allocPrint(alloc, "{d}", .{value});
    defer alloc.free(rendered);
    try out.appendSlice(alloc, rendered);
}

pub fn appendJsonFieldBool(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), first: *bool, name: []const u8, value: bool) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try out.appendSlice(alloc, if (value) "true" else "false");
}

fn computeAssetRequestDerived(
    alloc: Allocator,
    db: anytype,
    doc_value: []const u8,
    request: enrichment_types.GeneratedEnrichmentRequest,
    artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
    artifact_delete_keys: *std.ArrayListUnmanaged([]const u8),
    documents: *std.ArrayListUnmanaged(derived_types.DerivedDocument),
    dense_embeddings: *std.ArrayListUnmanaged(derived_types.DerivedDenseEmbeddingWrite),
    sparse_embeddings: *std.ArrayListUnmanaged(derived_types.DerivedSparseEmbeddingWrite),
) !void {
    var producer_cfg = try asset_producer_mod.parseProducerConfig(alloc, request.producer_json);
    defer producer_cfg.deinit(alloc);

    const artifact_name = requestArtifactName(request);
    const key = try internal_keys.artifactNamedPrefixAlloc(alloc, request.doc_key, "asset", artifact_name);
    defer alloc.free(key);

    const source_text = try extractAssetSourceValue(alloc, db, doc_value, request);
    if (source_text == null or source_text.?.len == 0) {
        if (source_text) |s| alloc.free(s);
        if (producer_cfg.type == .document_extraction) {
            try appendDocumentExtractionDeleteKeys(alloc, db, request.doc_key, artifact_name, key, artifact_delete_keys);
        } else {
            try artifact_delete_keys.append(alloc, try alloc.dupe(u8, key));
            const state_key = try assetStateKeyAlloc(alloc, request.doc_key, artifact_name);
            errdefer alloc.free(state_key);
            try artifact_delete_keys.append(alloc, state_key);
        }
        return;
    }
    defer alloc.free(source_text.?);

    if (producer_cfg.type == .document_extraction) {
        try computeDocumentExtractionAssetRequestDerived(
            alloc,
            db,
            doc_value,
            source_text.?,
            request,
            producer_cfg.config_json,
            key,
            artifact_writes,
            artifact_delete_keys,
            documents,
            dense_embeddings,
            sparse_embeddings,
            false,
        );
        return;
    }

    const source_parts_json = if (producer_cfg.type != .copy and request.source_template.len > 0)
        try renderSourcePartsJson(alloc, db, doc_value, request)
    else
        null;
    defer if (source_parts_json) |value| alloc.free(value);

    const state_key = if (producer_cfg.type != .copy)
        try assetStateKeyAlloc(alloc, request.doc_key, artifact_name)
    else
        null;
    defer if (state_key) |value| alloc.free(value);
    const state_value = if (producer_cfg.type != .copy)
        try assetStateValueAlloc(alloc, source_text.?, source_parts_json, request.producer_json)
    else
        null;
    defer if (state_value) |value| alloc.free(value);
    if (state_key != null and state_value != null) {
        const existing_state = try db.core.getStoreValue(alloc, state_key.?);
        defer if (existing_state) |value| alloc.free(value);
        if (existing_state != null and std.mem.eql(u8, existing_state.?, state_value.?)) {
            const existing_asset = try db.core.getStoreValue(alloc, key);
            defer if (existing_asset) |value| alloc.free(value);
            if (existing_asset) |value| {
                try artifact_writes.append(alloc, .{
                    .key = try alloc.dupe(u8, key),
                    .value = try alloc.dupe(u8, value),
                });
                return;
            }
        }
    }

    const value = if (producer_cfg.type == .copy) source_text.? else blk: {
        const runtime = db.enrichment_runtime orelse return error.MissingAssetProducer;
        const producer = runtime.config.asset_producer orelse return error.MissingAssetProducer;
        break :blk try producer.produce(alloc, .{
            .producer_type = producer_cfg.type,
            .config_json = producer_cfg.config_json,
            .source_text = source_text.?,
            .source_parts_json = source_parts_json,
            .content_type = request.content_type,
        });
    };
    defer if (producer_cfg.type != .copy) alloc.free(value);

    try artifact_writes.append(alloc, .{
        .key = try alloc.dupe(u8, key),
        .value = try alloc.dupe(u8, value),
    });

    if (producer_cfg.type != .copy) {
        try artifact_writes.append(alloc, .{
            .key = try alloc.dupe(u8, state_key.?),
            .value = try alloc.dupe(u8, state_value.?),
        });
    }
}

fn freeOwnedKeySlice(alloc: Allocator, keys: [][]u8) void {
    for (keys) |key| alloc.free(key);
    alloc.free(keys);
}

pub fn collectAndDeleteEnrichmentArtifactsForDocContext(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    doc_key: []const u8,
    deleted: *std.ArrayListUnmanaged([]u8),
) !void {
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(alloc);
    var unrecorded_delete_keys = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (unrecorded_delete_keys.items) |key| alloc.free(key);
        unrecorded_delete_keys.deinit(alloc);
    }

    const artifact_prefix = try internal_keys.artifactRootPrefixAlloc(alloc, doc_key);
    defer alloc.free(artifact_prefix);
    try collectDeleteKeysForPrefix(alloc, store, artifact_prefix, &deletes, deleted, &unrecorded_delete_keys);

    const asset_state_prefix = try internal_keys.assetStateRootPrefixAlloc(alloc, doc_key);
    defer alloc.free(asset_state_prefix);
    try collectDeleteKeysForPrefix(alloc, store, asset_state_prefix, &deletes, null, &unrecorded_delete_keys);

    const graph_asset_state_prefix = try internal_keys.graphAssetStateRootPrefixAlloc(alloc, doc_key);
    defer alloc.free(graph_asset_state_prefix);
    try collectDeleteKeysForPrefix(alloc, store, graph_asset_state_prefix, &deletes, null, &unrecorded_delete_keys);

    if (deletes.items.len > 0) {
        try store.putBatch(&.{}, deletes.items);
    }
}

fn collectDeleteKeysForPrefix(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    prefix: []const u8,
    deletes: *std.ArrayListUnmanaged([]const u8),
    recorded: ?*std.ArrayListUnmanaged([]u8),
    unrecorded: *std.ArrayListUnmanaged([]u8),
) !void {
    const existing = try store.scanPrefix(alloc, prefix);
    defer docstore_mod.DocStore.freeResults(alloc, existing);
    for (existing) |entry| {
        const owned = try alloc.dupe(u8, entry.key);
        errdefer alloc.free(owned);
        try deletes.append(alloc, owned);
        if (recorded) |out| {
            try out.append(alloc, owned);
        } else {
            try unrecorded.append(alloc, owned);
        }
    }
}

pub fn CoalescedKeyValueRequest(comptime T: type) type {
    return struct {
        pub const Entry = struct {
            key: []const u8,
            value: ?[]const u8 = null,
            kind: enum { write, delete },
            owned_key: bool = false,
            owned_value: bool = false,
        };

        entries: []Entry = &.{},
        writes: []T = &.{},
        deletes: [][]const u8 = &.{},

        pub fn deinit(self: *@This(), alloc: Allocator) void {
            for (self.entries) |entry| {
                if (entry.owned_key) alloc.free(@constCast(entry.key));
                if (entry.owned_value) alloc.free(@constCast(entry.value.?));
            }
            if (self.entries.len > 0) alloc.free(self.entries);
            if (self.writes.len > 0) alloc.free(self.writes);
            if (self.deletes.len > 0) alloc.free(self.deletes);
            self.* = .{};
        }
    };
}

const BulkIngestCoalescerStats = struct {
    active_session: std.atomic.Value(u8) = .init(0),
    staged_keys: AtomicU64 = .init(0),
    stage_batches: AtomicU64 = .init(0),
    stage_writes: AtomicU64 = .init(0),
    stage_deletes: AtomicU64 = .init(0),
    stage_transforms: AtomicU64 = .init(0),
    flush_calls: AtomicU64 = .init(0),
    flushed_keys: AtomicU64 = .init(0),

    pub fn snapshot(self: *const @This()) types.BulkCoalescingStats {
        return .{
            .active_session = self.active_session.load(.monotonic) != 0,
            .staged_keys = self.staged_keys.load(.monotonic),
            .stage_batches = self.stage_batches.load(.monotonic),
            .stage_writes = self.stage_writes.load(.monotonic),
            .stage_deletes = self.stage_deletes.load(.monotonic),
            .stage_transforms = self.stage_transforms.load(.monotonic),
            .flush_calls = self.flush_calls.load(.monotonic),
            .flushed_keys = self.flushed_keys.load(.monotonic),
        };
    }
};

const BulkIngestCoalescerEntry = struct {
    key: []u8,
    value: ?[]u8 = null,
    kind: enum { write, delete },
};

const BulkIngestRequestView = struct {
    writes: []types.BatchWrite = &.{},
    deletes: [][]const u8 = &.{},

    fn deinit(self: @This(), alloc: Allocator) void {
        if (self.writes.len > 0) alloc.free(self.writes);
        if (self.deletes.len > 0) alloc.free(self.deletes);
    }
};

pub fn BulkIngestCoalescer(comptime DB: type) type {
    return struct {
        const Self = @This();

        active: bool = false,
        entries: std.ArrayListUnmanaged(BulkIngestCoalescerEntry) = .empty,
        positions: std.StringHashMapUnmanaged(usize) = .empty,
        stats: BulkIngestCoalescerStats = .{},

        pub fn begin(self: *Self) void {
            self.active = true;
            self.stats.active_session.store(1, .monotonic);
        }

        pub fn deinit(self: *Self, alloc: Allocator) void {
            self.clear(alloc);
            self.entries.deinit(alloc);
            self.positions.deinit(alloc);
            self.* = .{};
        }

        pub fn clear(self: *Self, alloc: Allocator) void {
            self.resetPending(alloc);
            self.active = false;
            self.stats.active_session.store(0, .monotonic);
        }

        pub fn resetPending(self: *Self, alloc: Allocator) void {
            for (self.entries.items) |entry| {
                alloc.free(entry.key);
                if (entry.value) |value| alloc.free(value);
            }
            self.entries.clearRetainingCapacity();
            self.positions.clearRetainingCapacity();
            self.stats.staged_keys.store(0, .monotonic);
        }

        pub fn hasPending(self: *const Self) bool {
            return self.entries.items.len > 0;
        }

        pub fn snapshotRequestView(self: *const Self, alloc: Allocator) !BulkIngestRequestView {
            var result = BulkIngestRequestView{};
            errdefer result.deinit(alloc);

            var write_count: usize = 0;
            var delete_count: usize = 0;
            for (self.entries.items) |entry| {
                switch (entry.kind) {
                    .write => write_count += 1,
                    .delete => delete_count += 1,
                }
            }

            if (write_count > 0) result.writes = try alloc.alloc(types.BatchWrite, write_count);
            if (delete_count > 0) result.deletes = try alloc.alloc([]const u8, delete_count);

            var write_index: usize = 0;
            var delete_index: usize = 0;
            for (self.entries.items) |entry| {
                switch (entry.kind) {
                    .write => {
                        result.writes[write_index] = .{
                            .key = entry.key,
                            .value = entry.value.?,
                        };
                        write_index += 1;
                    },
                    .delete => {
                        result.deletes[delete_index] = entry.key;
                        delete_index += 1;
                    },
                }
            }
            return result;
        }

        pub fn stageBatch(self: *Self, db: *DB, req: types.BatchRequest) !void {
            std.debug.assert(self.active);
            _ = self.stats.stage_batches.fetchAdd(1, .monotonic);
            _ = self.stats.stage_writes.fetchAdd(@intCast(req.writes.len), .monotonic);
            _ = self.stats.stage_deletes.fetchAdd(@intCast(req.deletes.len), .monotonic);
            _ = self.stats.stage_transforms.fetchAdd(@intCast(req.transforms.len), .monotonic);

            for (req.writes) |write| {
                try self.stageWrite(db.alloc, write.key, write.value);
            }
            for (req.deletes) |key| {
                try self.stageDelete(db.alloc, key);
            }
            for (req.transforms) |transform| {
                try self.stageTransform(db, transform);
            }
            self.stats.staged_keys.store(@intCast(self.entries.items.len), .monotonic);
        }

        fn stageWrite(self: *Self, alloc: Allocator, key: []const u8, value: []const u8) !void {
            const gop = try self.positions.getOrPut(alloc, key);
            if (!gop.found_existing) {
                const owned_key = try alloc.dupe(u8, key);
                errdefer alloc.free(owned_key);
                const owned_value = try alloc.dupe(u8, value);
                errdefer alloc.free(owned_value);
                gop.key_ptr.* = owned_key;
                gop.value_ptr.* = self.entries.items.len;
                try self.entries.append(alloc, .{
                    .key = owned_key,
                    .value = owned_value,
                    .kind = .write,
                });
                return;
            }

            const entry = &self.entries.items[gop.value_ptr.*];
            if (entry.value) |existing| alloc.free(existing);
            entry.value = try alloc.dupe(u8, value);
            entry.kind = .write;
        }

        fn stageDelete(self: *Self, alloc: Allocator, key: []const u8) !void {
            const gop = try self.positions.getOrPut(alloc, key);
            if (!gop.found_existing) {
                const owned_key = try alloc.dupe(u8, key);
                errdefer alloc.free(owned_key);
                gop.key_ptr.* = owned_key;
                gop.value_ptr.* = self.entries.items.len;
                try self.entries.append(alloc, .{
                    .key = owned_key,
                    .kind = .delete,
                });
                return;
            }

            const entry = &self.entries.items[gop.value_ptr.*];
            if (entry.value) |existing| {
                alloc.free(existing);
                entry.value = null;
            }
            entry.kind = .delete;
        }

        fn stageTransform(self: *Self, db: *DB, transform: types.DocumentTransform) !void {
            const existing = if (self.positions.get(transform.key)) |entry_index|
                switch (self.entries.items[entry_index].kind) {
                    .write => self.entries.items[entry_index].value,
                    .delete => null,
                }
            else
                try db.get(db.alloc, transform.key);
            defer if (self.positions.get(transform.key) == null) {
                if (existing) |body| db.alloc.free(body);
            };

            const resolved = try transform_mod.resolveDocumentTransform(db.alloc, existing, transform) orelse return;
            errdefer db.alloc.free(resolved);
            try self.stageWriteOwned(db.alloc, transform.key, resolved);
        }

        fn stageWriteOwned(self: *Self, alloc: Allocator, key: []const u8, owned_value: []u8) !void {
            const gop = try self.positions.getOrPut(alloc, key);
            if (!gop.found_existing) {
                const owned_key = try alloc.dupe(u8, key);
                errdefer alloc.free(owned_key);
                gop.key_ptr.* = owned_key;
                gop.value_ptr.* = self.entries.items.len;
                try self.entries.append(alloc, .{
                    .key = owned_key,
                    .value = owned_value,
                    .kind = .write,
                });
                return;
            }

            const entry = &self.entries.items[gop.value_ptr.*];
            if (entry.value) |existing| alloc.free(existing);
            entry.value = owned_value;
            entry.kind = .write;
        }
    };
}

fn documentChildRangeRouteForKey(
    alloc: Allocator,
    snapshots: []const DocumentChildRangeRoutingSnapshot,
    key: []const u8,
) !?DocumentChildRangeRoute {
    var artifact_ref = (try db_internal.decodeArtifactRefIfKnownAlloc(alloc, key)) orelse return null;
    defer artifact_ref.deinit(alloc);
    const route_kind, const route_artifact_name = switch (artifact_ref.kind) {
        .asset => blk: {
            if (artifact_ref.unit_id == null) return null;
            break :blk .{ "unit", artifact_ref.name };
        },
        .chunk => .{ "chunk", artifact_ref.name },
        .embedding => blk: {
            const source = artifact_ref.source orelse return null;
            break :blk .{ if (source.kind == .chunk) "chunk" else "unit", source.name };
        },
    };
    for (snapshots) |snapshot| {
        if (!std.mem.eql(u8, snapshot.doc_key, artifact_ref.document_id)) continue;
        for (snapshot.child_ranges) |range| {
            if (!std.mem.eql(u8, range.range_kind, route_kind)) continue;
            if (!std.mem.eql(u8, range.artifact_name, route_artifact_name)) continue;
            const owner_group_id = range.owner_group_id orelse 0;
            if (owner_group_id == 0) continue;
            const route_status = range.route_status orelse "local_committed";
            if (!std.mem.eql(u8, route_status, "remote_committed")) continue;
            if (std.mem.order(u8, key, range.start_key) == .lt) continue;
            if (range.end_key_exclusive.len > 0 and std.mem.order(u8, key, range.end_key_exclusive) != .lt) continue;
            return .{ .owner_group_id = owner_group_id, .doc_key = snapshot.doc_key, .artifact_name = range.artifact_name };
        }
    }
    return null;
}

fn ensureDocumentChildRangeDispatchGroup(
    alloc: Allocator,
    groups: *std.ArrayListUnmanaged(DocumentChildRangeDispatchGroup),
    route: DocumentChildRangeRoute,
) !*DocumentChildRangeDispatchGroup {
    for (groups.items) |*group| {
        if (group.owner_group_id == route.owner_group_id and std.mem.eql(u8, group.doc_key, route.doc_key) and std.mem.eql(u8, group.artifact_name, route.artifact_name)) return group;
    }
    const doc_key = try alloc.dupe(u8, route.doc_key);
    errdefer alloc.free(doc_key);
    const artifact_name = try alloc.dupe(u8, route.artifact_name);
    errdefer alloc.free(artifact_name);
    try groups.append(alloc, .{ .owner_group_id = route.owner_group_id, .doc_key = doc_key, .artifact_name = artifact_name });
    return &groups.items[groups.items.len - 1];
}

fn partitionRemoteDerivedDocuments(
    alloc: Allocator,
    snapshots: []const DocumentChildRangeRoutingSnapshot,
    documents: *[]const derived_types.DerivedDocument,
    groups: *std.ArrayListUnmanaged(DocumentChildRangeDispatchGroup),
) !void {
    var local = std.ArrayListUnmanaged(derived_types.DerivedDocument).empty;
    errdefer local.deinit(alloc);
    for (documents.*) |doc| {
        if (try documentChildRangeRouteForKey(alloc, snapshots, doc.key)) |route| {
            const group = try ensureDocumentChildRangeDispatchGroup(alloc, groups, route);
            try group.documents.append(alloc, doc);
        } else try local.append(alloc, doc);
    }
    if (documents.*.len > 0) alloc.free(documents.*);
    documents.* = try local.toOwnedSlice(alloc);
}

fn partitionRemoteDenseEmbeddings(
    alloc: Allocator,
    snapshots: []const DocumentChildRangeRoutingSnapshot,
    embeddings: *[]const derived_types.DerivedDenseEmbeddingWrite,
    groups: *std.ArrayListUnmanaged(DocumentChildRangeDispatchGroup),
) !void {
    var local = std.ArrayListUnmanaged(derived_types.DerivedDenseEmbeddingWrite).empty;
    errdefer local.deinit(alloc);
    for (embeddings.*) |embedding| {
        const route_key = embedding.artifact_key orelse embedding.doc_key;
        if (try documentChildRangeRouteForKey(alloc, snapshots, route_key)) |route| {
            const group = try ensureDocumentChildRangeDispatchGroup(alloc, groups, route);
            try group.dense_embeddings.append(alloc, embedding);
        } else try local.append(alloc, embedding);
    }
    if (embeddings.*.len > 0) alloc.free(embeddings.*);
    embeddings.* = try local.toOwnedSlice(alloc);
}

fn partitionRemoteSparseEmbeddings(
    alloc: Allocator,
    snapshots: []const DocumentChildRangeRoutingSnapshot,
    embeddings: *[]const derived_types.DerivedSparseEmbeddingWrite,
    groups: *std.ArrayListUnmanaged(DocumentChildRangeDispatchGroup),
) !void {
    var local = std.ArrayListUnmanaged(derived_types.DerivedSparseEmbeddingWrite).empty;
    errdefer local.deinit(alloc);
    for (embeddings.*) |embedding| {
        const route_key = embedding.artifact_key orelse embedding.doc_key;
        if (try documentChildRangeRouteForKey(alloc, snapshots, route_key)) |route| {
            const group = try ensureDocumentChildRangeDispatchGroup(alloc, groups, route);
            try group.sparse_embeddings.append(alloc, embedding);
        } else try local.append(alloc, embedding);
    }
    if (embeddings.*.len > 0) alloc.free(embeddings.*);
    embeddings.* = try local.toOwnedSlice(alloc);
}

fn documentArtifactChildRangesFromJsonAlloc(alloc: Allocator, object: std.json.ObjectMap) ![]types.DocumentArtifactChildRange {
    const value = object.get("child_ranges") orelse return try alloc.alloc(types.DocumentArtifactChildRange, 0);
    if (value != .array) return try alloc.alloc(types.DocumentArtifactChildRange, 0);

    const out = try alloc.alloc(types.DocumentArtifactChildRange, value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*range| range.deinit(alloc);
        if (out.len > 0) alloc.free(out);
    }

    for (value.array.items, 0..) |item, i| {
        if (item != .object) return error.InvalidDocumentExtractionManifest;
        out[i] = .{
            .range_id = try jsonObjectStringDup(alloc, item.object, "range_id"),
            .range_kind = try jsonObjectStringDup(alloc, item.object, "range_kind"),
            .artifact_name = try jsonObjectStringDup(alloc, item.object, "artifact_name"),
            .split_boundary = try jsonObjectStringDup(alloc, item.object, "split_boundary"),
            .placement = try jsonObjectStringDup(alloc, item.object, "placement"),
            .owner_group_id = try jsonObjectOptionalU64(item.object, "owner_group_id"),
            .placement_generation = try jsonObjectOptionalU64(item.object, "placement_generation"),
            .route_status = try jsonObjectOptionalStringDup(alloc, item.object, "route_status"),
            .split_eligible = try jsonObjectOptionalBool(item.object, "split_eligible"),
            .start_key = try jsonObjectStringDup(alloc, item.object, "start_key"),
            .end_key_exclusive = try jsonObjectStringDup(alloc, item.object, "end_key_exclusive"),
            .last_key = try jsonObjectStringDup(alloc, item.object, "last_key"),
            .child_count = try jsonObjectUsize(item.object, "child_count"),
            .text_bytes = try jsonObjectOptionalUsize(item.object, "text_bytes"),
        };
        initialized += 1;
    }

    return out;
}

fn jsonObjectStringDup(alloc: Allocator, object: std.json.ObjectMap, field_name: []const u8) ![]u8 {
    const value = object.get(field_name) orelse return "";
    if (value != .string) return "";
    return try alloc.dupe(u8, value.string);
}

fn jsonObjectOptionalStringDup(alloc: Allocator, object: std.json.ObjectMap, field_name: []const u8) !?[]u8 {
    const value = object.get(field_name) orelse return null;
    if (value != .string) return null;
    return try alloc.dupe(u8, value.string);
}

fn jsonObjectUsize(object: std.json.ObjectMap, field_name: []const u8) !usize {
    const value = object.get(field_name) orelse return 0;
    if (value != .integer or value.integer < 0) return error.InvalidDocumentExtractionManifest;
    const value_u64 = std.math.cast(u64, value.integer) orelse return error.InvalidDocumentExtractionManifest;
    return std.math.cast(usize, value_u64) orelse return error.InvalidDocumentExtractionManifest;
}

fn jsonObjectOptionalUsize(object: std.json.ObjectMap, field_name: []const u8) !?usize {
    const value = object.get(field_name) orelse return null;
    if (value != .integer or value.integer < 0) return error.InvalidDocumentExtractionManifest;
    return std.math.cast(usize, value.integer) orelse return error.InvalidDocumentExtractionManifest;
}

pub fn jsonObjectOptionalU64(object: std.json.ObjectMap, field_name: []const u8) !?u64 {
    const value = object.get(field_name) orelse return null;
    if (value != .integer or value.integer < 0) return error.InvalidDocumentExtractionManifest;
    return std.math.cast(u64, value.integer) orelse return error.InvalidDocumentExtractionManifest;
}

fn jsonObjectOptionalBool(object: std.json.ObjectMap, field_name: []const u8) !?bool {
    const value = object.get(field_name) orelse return null;
    if (value != .bool) return error.InvalidDocumentExtractionManifest;
    return value.bool;
}

pub fn Impl(comptime DB: type) type {
    return struct {
        const BatchExecutionContext = db_internal.BatchExecutionContext(DB);
        const TtlCleanupContext = db_internal.TtlCleanupContext(DB);

        pub fn deleteExpiredDocumentsFromCandidates(ctx_ptr: *anyopaque, candidates: []const ttl_runtime_mod.DeleteCandidate) !u32 {
            const ctx: *TtlCleanupContext = @ptrCast(@alignCast(ctx_ptr));
            const loaded_schema = try schema_mod.loadSchema(ctx.batch.store, ctx.batch.alloc);
            defer if (loaded_schema) |schema| schema_mod.freeSchema(ctx.batch.alloc, schema);

            const duration_ns = if (loaded_schema) |schema|
                schema.ttl_duration_ns
            else
                0;
            if (duration_ns == 0) return 0;

            const now_ns = DB.WritePathCallbacks.current_time_ns();
            var deletes = std.ArrayListUnmanaged([]const u8).empty;
            defer deletes.deinit(ctx.batch.alloc);

            for (candidates) |candidate| {
                const current_ts = (try ttl_mod.readTimestamp(ctx.batch.store, ctx.batch.alloc, candidate.key)) orelse continue;
                if (current_ts != candidate.timestamp_ns) continue;
                if (!ttl_mod.isExpiredWithGrace(current_ts, duration_ns, ctx.grace_period_ns, now_ns)) continue;
                try deletes.append(ctx.batch.alloc, candidate.key);
            }
            if (deletes.items.len == 0) return 0;

            try executeDeleteBatchContext(&ctx.batch, deletes.items, .full_index);
            return @intCast(deletes.items.len);
        }

        fn executeDeleteBatchContext(ctx: *const BatchExecutionContext, keys: []const []const u8, sync_level: types.SyncLevel) !void {
            if (keys.len == 0) return;
            try DB.WritePathCallbacks.enforce_ha_write_gate_optional(ctx.ha_write_gate);

            var store_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
            defer store_writes.deinit(ctx.alloc);
            var owned_store_keys = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (owned_store_keys.items) |key| ctx.alloc.free(key);
                owned_store_keys.deinit(ctx.alloc);
            }
            var owned_store_values = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (owned_store_values.items) |value| ctx.alloc.free(value);
                owned_store_values.deinit(ctx.alloc);
            }
            var identity_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
            defer {
                for (identity_writes.items) |item| {
                    ctx.alloc.free(@constCast(item.key));
                    ctx.alloc.free(@constCast(item.value));
                }
                identity_writes.deinit(ctx.alloc);
            }
            var delete_keys = std.ArrayListUnmanaged([]const u8).empty;
            defer delete_keys.deinit(ctx.alloc);
            var timestamp_delete_keys = std.ArrayListUnmanaged([]const u8).empty;
            defer {
                for (timestamp_delete_keys.items) |key| ctx.alloc.free(@constCast(key));
                timestamp_delete_keys.deinit(ctx.alloc);
            }
            var owned_delete_keys = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (owned_delete_keys.items) |key| ctx.alloc.free(key);
                owned_delete_keys.deinit(ctx.alloc);
            }

            for (keys) |key| {
                if (ctx.relational_base_rows) {
                    try relational_store_mod.appendDelete(ctx.alloc, ctx.store, &delete_keys, &owned_delete_keys, key);
                    const primary_key = try internal_keys.documentKeyAlloc(ctx.alloc, key);
                    var primary_key_owned = true;
                    errdefer if (primary_key_owned) ctx.alloc.free(primary_key);
                    try owned_delete_keys.append(ctx.alloc, primary_key);
                    primary_key_owned = false;
                    try delete_keys.append(ctx.alloc, primary_key);
                } else {
                    const store_key = try internal_keys.documentKeyAlloc(ctx.alloc, key);
                    try owned_delete_keys.append(ctx.alloc, store_key);
                    try delete_keys.append(ctx.alloc, store_key);
                }
                if (!DB.WritePathCallbacks.should_write_timestamp(key)) continue;
                const timestamp_key = try DB.WritePathCallbacks.make_timestamp_key(ctx.alloc, key);
                try timestamp_delete_keys.append(ctx.alloc, timestamp_key);
                try delete_keys.append(ctx.alloc, timestamp_key);
            }

            var deleted_artifact_keys = std.ArrayListUnmanaged([]u8).empty;
            defer freeOwnedKeySlice(ctx.alloc, deleted_artifact_keys.items);
            const should_scan_artifacts = if (ctx.artifact_cleanup_maybe) |artifact_cleanup_maybe|
                artifact_cleanup_maybe.load(.acquire) or ctx.index_manager.hasGeneratedEnrichmentTargets()
            else
                true;
            if (should_scan_artifacts) {
                for (keys) |key| {
                    try collectAndDeleteEnrichmentArtifactsForDocContext(ctx.alloc, ctx.store, key, &deleted_artifact_keys);
                }
            }

            const req = types.BatchRequest{
                .deletes = keys,
                .sync_level = sync_level,
            };
            var derived_batch = try db_internal.buildDerivedBatch(ctx.alloc, req, &.{}, deleted_artifact_keys.items, &.{});
            defer derived_types.deinitDerivedBatch(ctx.alloc, &derived_batch);
            const sequence = ctx.store.reserveNextReplaySequence(1);
            derived_batch.sequence = sequence;
            try doc_identity.appendBatchIdentityMetadataForNamespaceAlloc(
                ctx.alloc,
                ctx.store,
                ctx.identity_namespace,
                sequence,
                &identity_writes,
                &.{},
                keys,
            );
            try store_writes.appendSlice(ctx.alloc, identity_writes.items);
            try appendAssetArtifactSourceIndexMutations(
                ctx.alloc,
                &store_writes,
                deleted_artifact_keys.items,
                &delete_keys,
                &owned_store_keys,
                &owned_store_values,
                &owned_delete_keys,
            );
            const replay_payload = try DB.WritePathCallbacks.encode_change_record_payload_context(ctx, derived_batch, sequence);
            defer ctx.alloc.free(replay_payload);
            try ctx.store.putBatchWithReplay(ctx.io, store_writes.items, delete_keys.items, .{
                .sequence = sequence,
                .payload = replay_payload,
            });
            DB.WritePathCallbacks.mirror_ha_replay_payload_best_effort_context(ctx.log_mutex, ctx.ha_async_effect_mirror, replay_payload);
            var sync_targets = try DB.WritePathCallbacks.collect_managed_sync_targets(ctx.alloc, ctx.index_manager, derived_batch);
            defer sync_targets.deinit(ctx.alloc);
            ctx.executor.trackBacklogBytes(sequence, @intCast(replay_payload.len)) catch {};
            try DB.WritePathCallbacks.mark_precomputed_enrichment_applied_for_sync_context(ctx, sync_level, sequence);
            try DB.WritePathCallbacks.apply_derived_backlog_pressure_context(ctx, sequence, sync_level, sync_targets);
            if (ctx.executor.hasWorkers()) {
                DB.WritePathCallbacks.notify_executor_for_sync_level_with_dense_bulk_deferral(ctx.async_context, ctx.executor, sync_level, sequence, sync_targets);
                try DB.WritePathCallbacks.wait_for_sync_level_context(ctx, sync_level, sequence, sync_targets);
            } else {
                if (DB.WritePathCallbacks.sync_level_requires_derived_visibility(sync_level)) {
                    ctx.apply_mutex.lockExclusive();
                    defer ctx.apply_mutex.unlockExclusive();
                    if (sync_level == .full_text) {
                        try DB.WritePathCallbacks.apply_derived_batch_targets_context(ctx, derived_batch, sync_targets.full_text_indexes);
                    } else {
                        try DB.WritePathCallbacks.apply_derived_batch_context(ctx, derived_batch);
                    }
                }
                try DB.WritePathCallbacks.wait_for_sync_level_context(ctx, sync_level, sequence, sync_targets);
            }
            if (ctx.enrichment_runtime) |runtime| runtime.notifySequence(sequence);
            DB.WritePathCallbacks.notify_resolver_replay_runtimes_for_catalog(ctx.index_manager, ctx.resolution_runtime, ctx.promotion_runtime, sequence);
        }

        pub fn appendPrecomputedGraphSourceArtifacts(
            self: *DB,
            artifact_writes: []const types.BatchWrite,
            artifact_delete_keys: []const []const u8,
            owned_graph_artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
            store_writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
            delete_keys: *std.ArrayListUnmanaged([]const u8),
            owned_delete_keys: *std.ArrayListUnmanaged([]u8),
            changed_artifact_keys: *std.ArrayListUnmanaged([]u8),
        ) !void {
            if (!self.core.hasGraphIndexes()) return;

            for (artifact_writes) |artifact_write| {
                try appendPrecomputedGraphSourceArtifactKey(self, artifact_write.key, artifact_write.value, owned_graph_artifact_writes, store_writes, delete_keys, owned_delete_keys, changed_artifact_keys);
            }
            for (artifact_delete_keys) |artifact_key| {
                try appendPrecomputedGraphSourceArtifactKey(self, artifact_key, null, owned_graph_artifact_writes, store_writes, delete_keys, owned_delete_keys, changed_artifact_keys);
            }
        }

        fn appendPrecomputedGraphSourceArtifactKey(
            self: *DB,
            artifact_key: []const u8,
            artifact_value: ?[]const u8,
            owned_graph_artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
            store_writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
            delete_keys: *std.ArrayListUnmanaged([]const u8),
            owned_delete_keys: *std.ArrayListUnmanaged([]u8),
            changed_artifact_keys: *std.ArrayListUnmanaged([]u8),
        ) !void {
            if (!internal_keys.isAssetArtifactKey(artifact_key)) return;
            var artifact_ref = (try artifact_ids.decodeArtifactRefAlloc(self.alloc, artifact_key)) orelse return;
            defer artifact_ref.deinit(self.alloc);
            if (artifact_ref.kind != .asset) return;

            for (self.core.graphIndexes()) |graph_entry| {
                const source = graph_entry.artifact_source orelse continue;
                if (!std.mem.eql(u8, source.artifact_name, artifact_ref.name)) continue;

                var graph_store_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
                defer graph_store_writes.deinit(self.alloc);

                if (artifact_value) |value| {
                    const raw_doc = try batchDocumentValueForGraphSource(
                        self.alloc,
                        self.core.store,
                        store_writes.items,
                        artifact_ref.document_id,
                        DB.WritePathCallbacks.relational_columns_for_store(self) != null,
                    );
                    defer if (raw_doc) |doc_value| self.alloc.free(doc_value);
                    const graph_writes = try DB.WritePathCallbacks.graph_writes_from_artifact_value_alloc(
                        self.alloc,
                        graph_entry.config.name,
                        artifact_ref.document_id,
                        value,
                        source,
                        graphArtifactContentType(self.core.index_manager, source.artifact_name),
                        raw_doc,
                    );
                    defer DB.WritePathCallbacks.free_graph_writes(self.alloc, graph_writes);
                    for (graph_writes) |write| {
                        const key = try internal_keys.graphEdgeArtifactKeyAlloc(self.alloc, write.source, write.index_name, write.edge_type, write.target);
                        var key_owned = true;
                        errdefer if (key_owned) self.alloc.free(key);
                        const payload = try enrichment_artifact_codec.encodeGraphEdgeAlloc(self.alloc, null, write.weight, write.created_at, write.updated_at, write.metadata_json);
                        var payload_owned = true;
                        errdefer if (payload_owned) self.alloc.free(payload);
                        try owned_graph_artifact_writes.append(self.alloc, .{ .key = key, .value = payload });
                        key_owned = false;
                        payload_owned = false;
                        try graph_store_writes.append(self.alloc, .{ .key = key, .value = payload });
                        try store_writes.append(self.alloc, .{ .key = key, .value = payload });
                        try appendUniqueOwnedKey(self.alloc, changed_artifact_keys, key);
                    }
                }

                const state_key = try graphAssetStateKeyAlloc(self.alloc, artifact_ref.document_id, graph_entry.config.name, artifact_ref.name);
                defer self.alloc.free(state_key);
                if (try loadGraphAssetStateKeysAlloc(self.alloc, self.core.store, state_key)) |previous_keys| {
                    defer freeOwnedConstKeySlice(self.alloc, previous_keys);
                    for (previous_keys) |previous_key| {
                        if (containsStoreWriteKey(graph_store_writes.items, previous_key)) continue;
                        if (containsOwnedKey(owned_delete_keys.items, previous_key)) continue;
                        const owned_key = try self.alloc.dupe(u8, previous_key);
                        try owned_delete_keys.append(self.alloc, owned_key);
                        try delete_keys.append(self.alloc, owned_key);
                        try appendUniqueOwnedKey(self.alloc, changed_artifact_keys, previous_key);
                    }
                } else {
                    const protected_keys = try DB.WritePathCallbacks.resolution_mention_state_keys_for_graph_source_alloc(self.alloc, self.core.store, self.core.index_manager, artifact_ref.document_id, graph_entry.config.name, source);
                    defer freeOwnedConstKeySlice(self.alloc, protected_keys);
                    const existing = try collectGraphArtifactsForDocIndex(self.alloc, self.core.store, artifact_ref.document_id, graph_entry.config.name);
                    defer docstore_mod.DocStore.freeResults(self.alloc, existing);
                    for (existing) |entry| {
                        if (containsStoreWriteKey(graph_store_writes.items, entry.key)) continue;
                        if (containsOwnedKey(owned_delete_keys.items, entry.key)) continue;
                        if (containsDeleteKey(protected_keys, entry.key)) continue;
                        const owned_key = try self.alloc.dupe(u8, entry.key);
                        try owned_delete_keys.append(self.alloc, owned_key);
                        try delete_keys.append(self.alloc, owned_key);
                        try appendUniqueOwnedKey(self.alloc, changed_artifact_keys, entry.key);
                    }
                }

                const state_value = try encodeGraphAssetStateKeysAlloc(self.alloc, graph_store_writes.items);
                var state_value_owned = true;
                errdefer if (state_value_owned) self.alloc.free(state_value);
                const state_key_owned = try self.alloc.dupe(u8, state_key);
                var state_key_owned_flag = true;
                errdefer if (state_key_owned_flag) self.alloc.free(state_key_owned);
                try owned_graph_artifact_writes.append(self.alloc, .{ .key = state_key_owned, .value = state_value });
                state_value_owned = false;
                state_key_owned_flag = false;
                try store_writes.append(self.alloc, .{ .key = state_key_owned, .value = state_value });
            }
        }

        pub fn extractEnrichments(self: *DB, alloc: Allocator, writes: []const types.BatchWrite) !types.ExtractEnrichmentsResult {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();

            var cleaned_writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
            errdefer {
                for (cleaned_writes.items) |write| {
                    alloc.free(@constCast(write.key));
                    alloc.free(@constCast(write.value));
                }
                cleaned_writes.deinit(alloc);
            }
            var dense_embeddings = std.ArrayListUnmanaged(types.EnrichmentDenseEmbeddingWrite).empty;
            errdefer {
                for (dense_embeddings.items) |*embedding| embedding.deinit(alloc);
                dense_embeddings.deinit(alloc);
            }
            var sparse_embeddings = std.ArrayListUnmanaged(types.EnrichmentSparseEmbeddingWrite).empty;
            errdefer {
                for (sparse_embeddings.items) |*embedding| embedding.deinit(alloc);
                sparse_embeddings.deinit(alloc);
            }
            var graph_writes = std.ArrayListUnmanaged(types.GraphEdgeWrite).empty;
            errdefer {
                for (graph_writes.items) |*write| {
                    alloc.free(@constCast(write.index_name));
                    alloc.free(@constCast(write.source));
                    alloc.free(@constCast(write.target));
                    alloc.free(@constCast(write.edge_type));
                    if (write.metadata_json.len > 0) alloc.free(@constCast(write.metadata_json));
                }
                graph_writes.deinit(alloc);
            }

            for (writes) |write| {
                var extracted = try mapper.extractWrite(alloc, write.key, write.value);
                try augmentExtractedWriteWithGraphFieldEdges(self, alloc, write.key, write.value, &extracted);
                try self.core.index_manager.appendIndexFieldEmbeddingsToExtractedWrite(alloc, write.key, write.value, &extracted);
                defer extracted.deinit(alloc);

                if (extracted.cleaned_value) |cleaned| {
                    try cleaned_writes.append(alloc, .{
                        .key = try alloc.dupe(u8, write.key),
                        .value = try alloc.dupe(u8, cleaned),
                    });
                }
                for (extracted.dense_embeddings) |embedding| {
                    const public_artifact = if (embedding.artifact_key) |artifact_key|
                        try artifact_ids.resolvePublicArtifactIdentityAlloc(alloc, artifact_key)
                    else
                        null;
                    defer if (public_artifact) |identity| {
                        var owned = identity;
                        owned.deinit(alloc);
                    };

                    try dense_embeddings.append(alloc, .{
                        .index_name = try alloc.dupe(u8, embedding.index_name),
                        .doc_key = try alloc.dupe(u8, embedding.doc_key),
                        .artifact_id = if (public_artifact) |identity| try alloc.dupe(u8, identity.id) else null,
                        .artifact_ref = if (public_artifact) |identity| try identity.artifact_ref.?.clone(alloc) else null,
                        .vector = try alloc.dupe(f32, embedding.vector),
                    });
                }
                for (extracted.sparse_embeddings) |embedding| {
                    try sparse_embeddings.append(alloc, .{
                        .index_name = try alloc.dupe(u8, embedding.index_name),
                        .doc_key = try alloc.dupe(u8, embedding.doc_key),
                        .indices = try alloc.dupe(u32, embedding.indices),
                        .values = try alloc.dupe(f32, embedding.values),
                    });
                }
                for (extracted.graph_writes) |graph_write| {
                    try graph_writes.append(alloc, .{
                        .index_name = try alloc.dupe(u8, graph_write.index_name),
                        .source = try alloc.dupe(u8, graph_write.source),
                        .target = try alloc.dupe(u8, graph_write.target),
                        .edge_type = try alloc.dupe(u8, graph_write.edge_type),
                        .weight = graph_write.weight,
                        .created_at = graph_write.created_at,
                        .updated_at = graph_write.updated_at,
                        .metadata_json = if (graph_write.metadata_json.len > 0) try alloc.dupe(u8, graph_write.metadata_json) else "",
                    });
                }
            }

            return .{
                .cleaned_writes = try cleaned_writes.toOwnedSlice(alloc),
                .dense_embeddings = try dense_embeddings.toOwnedSlice(alloc),
                .sparse_embeddings = try sparse_embeddings.toOwnedSlice(alloc),
                .graph_writes = try graph_writes.toOwnedSlice(alloc),
            };
        }

        pub fn computeEnrichments(self: *DB, alloc: Allocator, writes: []const types.BatchWrite) !types.ComputeEnrichmentsResult {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();

            var artifact_writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
            errdefer {
                for (artifact_writes.items) |write| {
                    alloc.free(@constCast(write.key));
                    alloc.free(@constCast(write.value));
                }
                artifact_writes.deinit(alloc);
            }
            var documents = std.ArrayListUnmanaged(types.EnrichmentDocumentWrite).empty;
            errdefer {
                for (documents.items) |*doc| doc.deinit(alloc);
                documents.deinit(alloc);
            }
            var dense_embeddings = std.ArrayListUnmanaged(types.EnrichmentDenseEmbeddingWrite).empty;
            errdefer {
                for (dense_embeddings.items) |*embedding| embedding.deinit(alloc);
                dense_embeddings.deinit(alloc);
            }
            var failed_keys = std.ArrayListUnmanaged([]u8).empty;
            errdefer {
                for (failed_keys.items) |key| alloc.free(key);
                failed_keys.deinit(alloc);
            }

            for (writes) |write| {
                var extracted = try mapper.extractWrite(alloc, write.key, write.value);
                try augmentExtractedWriteWithGraphFieldEdges(self, alloc, write.key, write.value, &extracted);
                defer extracted.deinit(alloc);
                const cleaned = extracted.cleaned_value orelse continue;

                var explicit_dense = std.ArrayListUnmanaged(types.EnrichmentDenseEmbeddingWrite).empty;
                defer {
                    for (explicit_dense.items) |*embedding| embedding.deinit(alloc);
                    explicit_dense.deinit(alloc);
                }
                for (extracted.dense_embeddings) |embedding| {
                    try explicit_dense.append(alloc, .{
                        .index_name = try alloc.dupe(u8, embedding.index_name),
                        .doc_key = try alloc.dupe(u8, embedding.doc_key),
                        .vector = try alloc.dupe(f32, embedding.vector),
                    });
                }
                var explicit_sparse = std.ArrayListUnmanaged(types.EnrichmentSparseEmbeddingWrite).empty;
                defer {
                    for (explicit_sparse.items) |*embedding| embedding.deinit(alloc);
                    explicit_sparse.deinit(alloc);
                }
                for (extracted.sparse_embeddings) |embedding| {
                    try explicit_sparse.append(alloc, .{
                        .index_name = try alloc.dupe(u8, embedding.index_name),
                        .doc_key = try alloc.dupe(u8, embedding.doc_key),
                        .indices = try alloc.dupe(u32, embedding.indices),
                        .values = try alloc.dupe(f32, embedding.values),
                    });
                }

                const generated = try self.core.planGeneratedEnrichments(
                    alloc,
                    write.key,
                    cleaned,
                    explicit_dense.items,
                    explicit_sparse.items,
                );
                defer enrichment_types.deinitGeneratedRequests(alloc, generated);

                var chunk_cache = std.ArrayListUnmanaged(ChunkCacheEntry).empty;
                defer {
                    for (chunk_cache.items) |entry| {
                        alloc.free(entry.key);
                        chunker_mod.freeChunks(alloc, entry.chunks);
                    }
                    chunk_cache.deinit(alloc);
                }

                for (generated) |request| {
                    switch (request.kind) {
                        .chunk_text => try computeChunkRequest(alloc, self, cleaned, request, &artifact_writes, &documents, &chunk_cache),
                        .dense_embedding => computeDenseRequest(alloc, self, cleaned, request, &artifact_writes, &dense_embeddings, &chunk_cache) catch |err| switch (err) {
                            error.MissingDenseEmbedder => try appendUniqueOwnedKey(alloc, &failed_keys, write.key),
                            else => return err,
                        },
                        else => {},
                    }
                }
            }

            const public_artifact_writes = try externalizeArtifactWritesAlloc(alloc, try artifact_writes.toOwnedSlice(alloc));
            return .{
                .artifact_writes = public_artifact_writes,
                .documents = try documents.toOwnedSlice(alloc),
                .dense_embeddings = try dense_embeddings.toOwnedSlice(alloc),
                .failed_keys = try failed_keys.toOwnedSlice(alloc),
            };
        }

        pub fn batchAfterGate(self: *DB, req: types.BatchRequest) anyerror!void {
            if (DB.WritePathCallbacks.bench_metrics_enabled()) {
                var profile = DB.WritePathCallbacks.Profile{};
                try DB.WritePathCallbacks.batch_internal(self, req, &profile, .{ .admission_prechecked = true });
                DB.WritePathCallbacks.log_batch_profile(req, profile);
            } else {
                try DB.WritePathCallbacks.batch_internal(self, req, null, .{ .admission_prechecked = true });
            }
        }

        pub fn batchProfiledAfterGate(self: *DB, req: types.BatchRequest, profile: *DB.WritePathCallbacks.Profile) anyerror!void {
            try DB.WritePathCallbacks.batch_internal(self, req, profile, .{ .admission_prechecked = true });
        }

        pub fn batchWithDocumentArtifactChildRangeDispatcherAfterGate(
            self: *DB,
            req: types.BatchRequest,
            dispatcher: DocumentArtifactChildRangeDispatcher,
        ) anyerror!void {
            if (DB.WritePathCallbacks.bench_metrics_enabled()) {
                var profile = DB.WritePathCallbacks.Profile{};
                try DB.WritePathCallbacks.batch_internal(self, req, &profile, .{
                    .document_child_range_dispatcher = dispatcher,
                    .admission_prechecked = true,
                });
                DB.WritePathCallbacks.log_batch_profile(req, profile);
            } else {
                try DB.WritePathCallbacks.batch_internal(self, req, null, .{
                    .document_child_range_dispatcher = dispatcher,
                    .admission_prechecked = true,
                });
            }
        }

        pub fn getDocumentArtifactManifest(
            self: *DB,
            alloc: Allocator,
            doc_key: []const u8,
            artifact_name: []const u8,
        ) anyerror!?types.DocumentArtifactManifest {
            const manifest_key = try internal_keys.artifactNamedPrefixAlloc(alloc, doc_key, "asset", artifact_name);
            defer alloc.free(manifest_key);
            const manifest_json = try self.core.getStoreValue(alloc, manifest_key) orelse return null;
            errdefer alloc.free(manifest_json);

            const state_key = try assetStateKeyAlloc(alloc, doc_key, artifact_name);
            defer alloc.free(state_key);
            const state_json = self.core.getStoreValue(alloc, state_key) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            errdefer if (state_json) |value| alloc.free(value);

            return try documentArtifactManifestFromValueAlloc(alloc, doc_key, artifact_name, manifest_key, manifest_json, state_json);
        }

        pub fn listDocumentArtifactManifests(
            self: *DB,
            alloc: Allocator,
            doc_key: []const u8,
        ) anyerror!types.DocumentArtifactManifestList {
            var list = std.ArrayListUnmanaged(types.DocumentArtifactManifest).empty;
            errdefer {
                for (list.items) |*manifest| manifest.deinit(alloc);
                list.deinit(alloc);
            }

            const prefix = try internal_keys.artifactTypePrefixAlloc(alloc, doc_key, "asset");
            defer alloc.free(prefix);
            const artifacts = try self.core.scanStorePrefix(alloc, prefix);
            defer docstore_mod.DocStore.freeResults(alloc, artifacts);

            for (artifacts) |entry| {
                var artifact_ref = (try artifact_ids.decodeArtifactRefAlloc(alloc, entry.key)) orelse continue;
                defer artifact_ref.deinit(alloc);
                if (artifact_ref.kind != .asset or artifact_ref.unit_id != null) continue;

                const manifest_json = try alloc.dupe(u8, entry.value);
                errdefer alloc.free(manifest_json);
                const state_key = try assetStateKeyAlloc(alloc, doc_key, artifact_ref.name);
                defer alloc.free(state_key);
                const state_json = self.core.getStoreValue(alloc, state_key) catch |err| switch (err) {
                    error.NotFound => null,
                    else => return err,
                };
                errdefer if (state_json) |value| alloc.free(value);

                try list.append(alloc, try documentArtifactManifestFromValueAlloc(alloc, doc_key, artifact_ref.name, entry.key, manifest_json, state_json));
            }

            return .{
                .document_id = try alloc.dupe(u8, doc_key),
                .artifacts = try list.toOwnedSlice(alloc),
            };
        }

        pub fn updateDocumentArtifactChildRangePlacementAfterGate(
            self: *DB,
            alloc: Allocator,
            doc_key: []const u8,
            artifact_name: []const u8,
            update: types.DocumentArtifactChildRangePlacementUpdate,
        ) anyerror!bool {
            try self.executor.failIfUnhealthy();

            self.core.lockApply();
            var apply_mutex_held = true;
            errdefer if (apply_mutex_held) self.core.unlockApply();

            const manifest_key = try internal_keys.artifactNamedPrefixAlloc(alloc, doc_key, "asset", artifact_name);
            defer alloc.free(manifest_key);
            const manifest = try self.core.getStoreValue(alloc, manifest_key) orelse {
                self.core.unlockApply();
                apply_mutex_held = false;
                return false;
            };
            defer alloc.free(manifest);

            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            const arena_alloc = arena.allocator();
            var manifest_value = try std.json.parseFromSliceLeaky(std.json.Value, arena_alloc, manifest, .{ .allocate = .alloc_always });
            if (manifest_value != .object) return error.InvalidArgument;
            const child_ranges = manifest_value.object.getPtr("child_ranges") orelse return error.InvalidArgument;
            if (child_ranges.* != .array) return error.InvalidArgument;

            var changed = false;
            for (child_ranges.array.items) |*item| {
                if (item.* != .object) return error.InvalidArgument;
                const range_id = item.object.get("range_id") orelse return error.InvalidArgument;
                if (range_id != .string) return error.InvalidArgument;
                if (!std.mem.eql(u8, range_id.string, update.range_id)) continue;

                try putLeakyJsonStringField(arena_alloc, &item.object, "placement", update.placement);
                if (update.owner_group_id) |owner_group_id| try putLeakyJsonU64Field(arena_alloc, &item.object, "owner_group_id", owner_group_id);
                if (update.placement_generation) |placement_generation| try putLeakyJsonU64Field(arena_alloc, &item.object, "placement_generation", placement_generation);
                if (update.route_status) |route_status| try putLeakyJsonStringField(arena_alloc, &item.object, "route_status", route_status);
                if (update.split_eligible) |split_eligible| try item.object.put(arena_alloc, "split_eligible", .{ .bool = split_eligible });
                changed = true;
                break;
            }

            if (!changed) {
                self.core.unlockApply();
                apply_mutex_held = false;
                return false;
            }

            const updated_manifest = try std.json.Stringify.valueAlloc(alloc, manifest_value, .{});
            defer alloc.free(updated_manifest);

            const sequence = self.core.reserveDerivedAppendSequence();
            const changed_artifact_keys = [_][]const u8{manifest_key};
            const derived_batch = derived_types.DerivedBatch{
                .sequence = sequence,
                .changed_artifact_keys = changed_artifact_keys[0..],
            };
            const replay_payload = try DB.WritePathCallbacks.encode_change_record_payload(self, derived_batch, sequence);
            defer alloc.free(replay_payload);

            const writes = [_]docstore_mod.KVPair{.{
                .key = manifest_key,
                .value = updated_manifest,
            }};
            try self.core.store.putBatchWithReplay(self.backend_runtime.io(), writes[0..], &.{}, .{
                .sequence = sequence,
                .payload = replay_payload,
            });
            DB.WritePathCallbacks.mirror_ha_replay_payload_best_effort(self, replay_payload);
            self.executor.trackBacklogBytes(sequence, @intCast(replay_payload.len)) catch {};
            self.core.unlockApply();
            apply_mutex_held = false;

            if (self.executor.hasWorkers()) {
                self.executor.forceSequence(sequence);
            } else {
                self.executor.notifySequence(sequence);
            }
            DB.WritePathCallbacks.notify_resolver_replay_runtimes(self, sequence);
            return true;
        }

        pub fn reprocessDocumentArtifactAfterGate(
            self: *DB,
            alloc: Allocator,
            doc_key: []const u8,
            artifact_name: []const u8,
        ) anyerror!bool {
            var cfg = (try self.getEnrichment(alloc, .asset, artifact_name)) orelse return false;
            defer cfg.deinit(alloc);
            var producer_cfg = try asset_producer_mod.parseProducerConfig(alloc, cfg.producer_json);
            defer producer_cfg.deinit(alloc);
            if (producer_cfg.type != .document_extraction) return error.InvalidArgument;

            const value = try self.get(alloc, doc_key) orelse return false;
            defer alloc.free(value);

            const state_key = try assetStateKeyAlloc(alloc, doc_key, artifact_name);
            defer alloc.free(state_key);
            self.core.store.delete(state_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };

            const writes = [_]types.BatchWrite{.{ .key = doc_key, .value = value }};
            const force_artifacts = [_][]const u8{artifact_name};
            try DB.WritePathCallbacks.batch_internal(self, .{
                .writes = &writes,
                .sync_level = .write,
            }, null, .{
                .force_generated_artifact_names = &force_artifacts,
                .admission_prechecked = true,
            });
            const sequence = self.core.nextDerivedSequence();
            try self.runEnrichmentUntil(sequence);
            return true;
        }

        pub fn reprocessDocumentArtifactRangeAfterGate(
            self: *DB,
            alloc: Allocator,
            artifact_name: []const u8,
            req: types.DocumentArtifactTableReprocessRequest,
        ) anyerror!types.DocumentArtifactTableReprocessResult {
            var cfg = (try self.getEnrichment(alloc, .asset, artifact_name)) orelse return error.NotFound;
            defer cfg.deinit(alloc);
            var producer_cfg = try asset_producer_mod.parseProducerConfig(alloc, cfg.producer_json);
            defer producer_cfg.deinit(alloc);
            if (producer_cfg.type != .document_extraction) return error.InvalidArgument;

            var effective_req = req;
            if (req.shard_cursors.len > 0) {
                if (req.shard_cursors.len != 1) return error.InvalidArgument;
                const cursor = req.shard_cursors[0];
                if (cursor.group_id != null) return error.InvalidArgument;
                effective_req = .{
                    .from_key = cursor.next_key,
                    .to_key = req.to_key,
                    .limit = if (cursor.limit != 0) cursor.limit else req.limit,
                };
            }

            const limit = if (effective_req.limit == 0) @as(u32, 100) else effective_req.limit;
            const scan_limit = if (limit == std.math.maxInt(u32)) limit else limit + 1;
            var scanned = try self.scan(alloc, effective_req.from_key, effective_req.to_key, .{
                .include_documents = true,
                .inclusive_from = effective_req.from_key.len == 0,
                .limit = scan_limit,
            });
            defer scanned.deinit(alloc);

            var failures = std.ArrayListUnmanaged(types.DocumentArtifactReprocessFailure).empty;
            errdefer {
                for (failures.items) |*failure| failure.deinit(alloc);
                failures.deinit(alloc);
            }

            var result = types.DocumentArtifactTableReprocessResult{
                .limit = limit,
            };
            errdefer result.deinit(alloc);

            const process_len = @min(scanned.documents.len, @as(usize, @intCast(limit)));
            for (scanned.documents[0..process_len]) |doc| {
                result.scanned += 1;
                const handled = reprocessDocumentArtifactAfterGate(self, alloc, doc.id, artifact_name) catch |err| switch (err) {
                    error.OutOfMemory => return err,
                    else => {
                        result.failed += 1;
                        try failures.append(alloc, .{
                            .key = try alloc.dupe(u8, doc.id),
                            .error_code = try alloc.dupe(u8, @errorName(err)),
                        });
                        continue;
                    },
                };
                if (handled) {
                    result.reprocessed += 1;
                } else {
                    result.skipped += 1;
                }
            }

            if (scanned.documents.len > process_len and process_len > 0) {
                result.next_key = try alloc.dupe(u8, scanned.documents[process_len - 1].id);
            }
            result.failures = try failures.toOwnedSlice(alloc);
            return result;
        }

        pub fn drainDocumentArtifactChildRangeOutboxAfterGate(
            self: *DB,
            dispatcher: DocumentArtifactChildRangeDispatcher,
            limit: usize,
        ) anyerror!DocumentArtifactChildRangeOutboxDrainResult {
            const prefix = try internal_keys.documentChildRangeOutboxRootPrefixAlloc(self.alloc);
            defer self.alloc.free(prefix);

            self.core.lockApply();
            var apply_mutex_held = true;
            errdefer if (apply_mutex_held) self.core.unlockApply();
            const scanned = try self.core.scanStorePrefix(self.alloc, prefix);
            self.core.unlockApply();
            apply_mutex_held = false;
            defer docstore_mod.DocStore.freeResults(self.alloc, scanned);

            var result = DocumentArtifactChildRangeOutboxDrainResult{};
            const max_entries = if (limit == 0 or limit > scanned.len) scanned.len else limit;
            for (scanned[0..max_entries]) |entry| {
                result.scanned += 1;
                var parsed = try std.json.parseFromSlice(DocumentArtifactChildRangeOutboxRecord, self.alloc, entry.value, .{
                    .allocate = .alloc_always,
                });
                defer parsed.deinit();
                if (parsed.value.version != 1) return error.InvalidDocumentChildRangeOutboxRecord;
                try dispatcher.applyDispatch(self.alloc, .{
                    .owner_group_id = parsed.value.owner_group_id,
                    .doc_key = parsed.value.doc_key,
                    .artifact_name = parsed.value.artifact_name,
                    .child_batch = parsed.value.child_batch,
                });
                result.dispatched += 1;
                try deleteDocumentArtifactChildRangeOutboxEntryAfterGate(self, entry.key);
                result.deleted += 1;
            }
            return result;
        }

        pub fn applyDocumentArtifactChildRangeBatchAfterGate(
            self: *DB,
            child_batch: DocumentArtifactChildRangeApplyBatch,
        ) anyerror!u64 {
            if (child_batch.artifact_writes.len == 0 and
                child_batch.artifact_delete_keys.len == 0 and
                child_batch.documents.len == 0 and
                child_batch.dense_embeddings.len == 0 and
                child_batch.sparse_embeddings.len == 0 and
                child_batch.generated_enrichment_refs.len == 0)
            {
                return 0;
            }

            try self.executor.failIfUnhealthy();
            self.core.lockApply();
            var apply_mutex_held = true;
            errdefer if (apply_mutex_held) self.core.unlockApply();

            var store_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
            defer store_writes.deinit(self.alloc);
            var delete_keys = std.ArrayListUnmanaged([]const u8).empty;
            defer delete_keys.deinit(self.alloc);
            var owned_store_keys = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (owned_store_keys.items) |key| self.alloc.free(key);
                owned_store_keys.deinit(self.alloc);
            }
            var owned_store_values = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (owned_store_values.items) |value| self.alloc.free(value);
                owned_store_values.deinit(self.alloc);
            }
            var owned_delete_keys = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (owned_delete_keys.items) |key| self.alloc.free(key);
                owned_delete_keys.deinit(self.alloc);
            }
            var changed_artifact_keys = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (changed_artifact_keys.items) |key| self.alloc.free(key);
                changed_artifact_keys.deinit(self.alloc);
            }
            var materialized_graph_artifact_writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
            defer {
                for (materialized_graph_artifact_writes.items) |write| {
                    self.alloc.free(@constCast(write.key));
                    self.alloc.free(@constCast(write.value));
                }
                materialized_graph_artifact_writes.deinit(self.alloc);
            }

            for (child_batch.artifact_writes) |write| {
                try store_writes.append(self.alloc, .{
                    .key = write.key,
                    .value = write.value,
                });
                try appendUniqueOwnedKey(self.alloc, &changed_artifact_keys, write.key);
            }
            for (child_batch.artifact_delete_keys) |key| {
                try delete_keys.append(self.alloc, key);
                if (internal_keys.isAssetArtifactKey(key) or internal_keys.isGraphEdgeArtifactKey(key)) {
                    try appendUniqueOwnedKey(self.alloc, &changed_artifact_keys, key);
                }
            }

            try DB.WritePathCallbacks.append_precomputed_graph_source_artifacts(
                self,
                child_batch.artifact_writes,
                child_batch.artifact_delete_keys,
                &materialized_graph_artifact_writes,
                &store_writes,
                &delete_keys,
                &owned_delete_keys,
                &changed_artifact_keys,
            );

            if (child_batch.artifact_writes.len > 0 or materialized_graph_artifact_writes.items.len > 0) {
                try self.core.appendArtifactPresenceMarker(&store_writes);
            }
            try appendAssetArtifactSourceIndexMutations(
                self.alloc,
                &store_writes,
                child_batch.artifact_delete_keys,
                &delete_keys,
                &owned_store_keys,
                &owned_store_values,
                &owned_delete_keys,
            );

            const sequence = self.core.reserveDerivedAppendSequence();
            const derived_seed = derived_types.DerivedBatch{
                .sequence = sequence,
                .documents = child_batch.documents,
                .deleted_keys = child_batch.artifact_delete_keys,
                .changed_artifact_keys = changed_artifact_keys.items,
                .dense_embeddings = child_batch.dense_embeddings,
                .sparse_embeddings = child_batch.sparse_embeddings,
                .generated_enrichment_refs = child_batch.generated_enrichment_refs,
            };
            var derived_batch = try derived_types.cloneBatch(self.alloc, derived_seed);
            defer derived_types.deinitDerivedBatch(self.alloc, &derived_batch);
            derived_batch.sequence = sequence;

            var sync_targets = try DB.WritePathCallbacks.collect_managed_sync_targets(self.alloc, self.core.index_manager, derived_batch);
            defer sync_targets.deinit(self.alloc);
            const replay_payload = try DB.WritePathCallbacks.encode_change_record_payload(self, derived_batch, sequence);
            defer self.alloc.free(replay_payload);

            try self.core.store.putBatchWithReplay(
                self.backend_runtime.io(),
                store_writes.items,
                delete_keys.items,
                .{
                    .sequence = sequence,
                    .payload = replay_payload,
                },
            );
            DB.WritePathCallbacks.mirror_ha_replay_payload_best_effort(self, replay_payload);
            if (DB.WritePathCallbacks.should_append_split_delta(self)) {
                try self.core.appendSplitDelta(DB.WritePathCallbacks.current_time_ns(), store_writes.items, delete_keys.items);
            }

            self.executor.trackBacklogBytes(sequence, @intCast(replay_payload.len)) catch {};
            self.core.unlockApply();
            apply_mutex_held = false;

            try DB.WritePathCallbacks.mark_precomputed_enrichment_applied_for_sync(self, child_batch.sync_level, sequence);
            try DB.WritePathCallbacks.apply_derived_backlog_pressure(self, sequence, child_batch.sync_level, sync_targets);
            if (self.executor.hasWorkers()) {
                DB.WritePathCallbacks.notify_executor_for_sync_level_with_dense_bulk_deferral(self.async_context, self.executor, child_batch.sync_level, sequence, sync_targets);
                try DB.WritePathCallbacks.wait_for_sync_level(self, child_batch.sync_level, sequence, sync_targets);
            } else {
                if (DB.WritePathCallbacks.sync_level_requires_derived_visibility(child_batch.sync_level)) {
                    if (child_batch.sync_level == .full_text) {
                        try DB.WritePathCallbacks.apply_derived_batch_targets(self, derived_batch, sync_targets.full_text_indexes);
                    } else {
                        try DB.WritePathCallbacks.apply_derived_batch(self, derived_batch);
                    }
                }
                try DB.WritePathCallbacks.wait_for_sync_level(self, child_batch.sync_level, sequence, sync_targets);
            }
            if (self.enrichment_runtime) |runtime| runtime.notifySequence(sequence);
            DB.WritePathCallbacks.notify_resolver_replay_runtimes(self, sequence);
            return sequence;
        }

        pub fn partitionRemoteDocumentChildRangeGeneratedBatch(
            self: *DB,
            generated: *PrecomputedGeneratedBatch,
            out: *std.ArrayListUnmanaged(DocumentChildRangeDispatchGroup),
        ) !void {
            var snapshots = std.ArrayListUnmanaged(DocumentChildRangeRoutingSnapshot).empty;
            defer {
                for (snapshots.items) |*snapshot| snapshot.deinit(self.alloc);
                snapshots.deinit(self.alloc);
            }
            try collectDocumentChildRangeRoutingSnapshots(self, generated.*, &snapshots);
            if (snapshots.items.len == 0) return;

            var local_writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
            errdefer local_writes.deinit(self.alloc);
            for (generated.artifact_writes) |write| {
                if (try documentChildRangeRouteForKey(self.alloc, snapshots.items, write.key)) |route| {
                    const group = try ensureDocumentChildRangeDispatchGroup(self.alloc, out, route);
                    try group.artifact_writes.append(self.alloc, write);
                } else {
                    try local_writes.append(self.alloc, write);
                }
            }
            if (generated.artifact_writes.len > 0) self.alloc.free(generated.artifact_writes);
            generated.artifact_writes = try local_writes.toOwnedSlice(self.alloc);

            var local_deletes = std.ArrayListUnmanaged([]const u8).empty;
            errdefer local_deletes.deinit(self.alloc);
            for (generated.artifact_delete_keys) |key| {
                if (try documentChildRangeRouteForKey(self.alloc, snapshots.items, key)) |route| {
                    const group = try ensureDocumentChildRangeDispatchGroup(self.alloc, out, route);
                    try group.artifact_delete_keys.append(self.alloc, key);
                } else {
                    try local_deletes.append(self.alloc, key);
                }
            }
            if (generated.artifact_delete_keys.len > 0) self.alloc.free(generated.artifact_delete_keys);
            generated.artifact_delete_keys = try local_deletes.toOwnedSlice(self.alloc);

            try partitionRemoteDerivedDocuments(self.alloc, snapshots.items, &generated.documents, out);
            try partitionRemoteDenseEmbeddings(self.alloc, snapshots.items, &generated.dense_embeddings, out);
            try partitionRemoteSparseEmbeddings(self.alloc, snapshots.items, &generated.sparse_embeddings, out);
        }

        pub fn prepareGeneratedEnrichments(
            self: *DB,
            req: types.BatchRequest,
            extracted: []const mapper.ExtractedWrite,
            precompute_mode: GeneratedPrecomputeMode,
            force_generated_artifact_names: []const []const u8,
        ) !PrecomputedGeneratedBatch {
            if (!self.core.hasGeneratedEnrichmentTargets()) return .{};

            var artifact_writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
            errdefer {
                for (artifact_writes.items) |write| {
                    self.alloc.free(@constCast(write.key));
                    self.alloc.free(@constCast(write.value));
                }
                artifact_writes.deinit(self.alloc);
            }
            var artifact_delete_keys = std.ArrayListUnmanaged([]const u8).empty;
            errdefer {
                for (artifact_delete_keys.items) |key| self.alloc.free(@constCast(key));
                artifact_delete_keys.deinit(self.alloc);
            }
            var documents = std.ArrayListUnmanaged(derived_types.DerivedDocument).empty;
            errdefer {
                var tmp = derived_types.DerivedBatch{ .documents = documents.items };
                derived_types.deinitDerivedBatch(self.alloc, &tmp);
                documents = .empty;
            }
            var dense_embeddings = std.ArrayListUnmanaged(derived_types.DerivedDenseEmbeddingWrite).empty;
            errdefer {
                var tmp = derived_types.DerivedBatch{ .dense_embeddings = dense_embeddings.items };
                derived_types.deinitDerivedBatch(self.alloc, &tmp);
                dense_embeddings = .empty;
            }
            var sparse_embeddings = std.ArrayListUnmanaged(derived_types.DerivedSparseEmbeddingWrite).empty;
            errdefer {
                var tmp = derived_types.DerivedBatch{ .sparse_embeddings = sparse_embeddings.items };
                derived_types.deinitDerivedBatch(self.alloc, &tmp);
                sparse_embeddings = .empty;
            }
            var planned = std.ArrayListUnmanaged(enrichment_types.GeneratedEnrichmentRef).empty;
            errdefer enrichment_types.deinitGeneratedRefs(self.alloc, planned.items);

            for (req.writes, 0..) |write, i| {
                const cleaned = extracted[i].cleaned_value orelse continue;

                var explicit_dense = std.ArrayListUnmanaged(types.EnrichmentDenseEmbeddingWrite).empty;
                defer {
                    for (explicit_dense.items) |*embedding| embedding.deinit(self.alloc);
                    explicit_dense.deinit(self.alloc);
                }
                for (extracted[i].dense_embeddings) |embedding| {
                    try explicit_dense.append(self.alloc, .{
                        .index_name = try self.alloc.dupe(u8, embedding.index_name),
                        .doc_key = try self.alloc.dupe(u8, embedding.doc_key),
                        .vector = try self.alloc.dupe(f32, embedding.vector),
                    });
                }

                var explicit_sparse = std.ArrayListUnmanaged(types.EnrichmentSparseEmbeddingWrite).empty;
                defer {
                    for (explicit_sparse.items) |*embedding| embedding.deinit(self.alloc);
                    explicit_sparse.deinit(self.alloc);
                }
                for (extracted[i].sparse_embeddings) |embedding| {
                    try explicit_sparse.append(self.alloc, .{
                        .index_name = try self.alloc.dupe(u8, embedding.index_name),
                        .doc_key = try self.alloc.dupe(u8, embedding.doc_key),
                        .indices = try self.alloc.dupe(u32, embedding.indices),
                        .values = try self.alloc.dupe(f32, embedding.values),
                    });
                }

                const generated = try self.core.planGeneratedEnrichments(
                    self.alloc,
                    write.key,
                    cleaned,
                    explicit_dense.items,
                    explicit_sparse.items,
                );
                defer enrichment_types.deinitGeneratedRequests(self.alloc, generated);

                var chunk_cache = std.ArrayListUnmanaged(ChunkCacheEntry).empty;
                defer {
                    for (chunk_cache.items) |entry| {
                        self.alloc.free(entry.key);
                        chunker_mod.freeChunks(self.alloc, entry.chunks);
                    }
                    chunk_cache.deinit(self.alloc);
                }

                for (generated) |request| {
                    if (!requestMatchesForcedGeneratedArtifact(request, force_generated_artifact_names)) continue;
                    if (!try @This().shouldPrecomputeGeneratedRequest(self, precompute_mode, request)) {
                        try planned.append(self.alloc, try enrichment_types.requestToRef(self.alloc, request));
                        continue;
                    }

                    switch (request.kind) {
                        .asset => try computeAssetRequestDerived(self.alloc, self, cleaned, request, &artifact_writes, &artifact_delete_keys, &documents, &dense_embeddings, &sparse_embeddings),
                        .chunk_text => try computeChunkRequestDerived(self.alloc, self, cleaned, request, &artifact_writes, &documents, &chunk_cache),
                        .dense_embedding => computeDenseRequestDerived(self.alloc, self, cleaned, request, &artifact_writes, &dense_embeddings, &chunk_cache) catch |err| switch (err) {
                            error.MissingDenseEmbedder => try planned.append(self.alloc, try enrichment_types.requestToRef(self.alloc, request)),
                            else => return err,
                        },
                        .sparse_embedding => computeSparseRequestDerived(self.alloc, self, cleaned, request, &artifact_writes, &sparse_embeddings, &chunk_cache) catch |err| switch (err) {
                            error.MissingSparseEmbedder => try planned.append(self.alloc, try enrichment_types.requestToRef(self.alloc, request)),
                            else => return err,
                        },
                    }
                }
            }

            return .{
                .artifact_writes = try artifact_writes.toOwnedSlice(self.alloc),
                .artifact_delete_keys = try artifact_delete_keys.toOwnedSlice(self.alloc),
                .documents = try documents.toOwnedSlice(self.alloc),
                .dense_embeddings = try dense_embeddings.toOwnedSlice(self.alloc),
                .sparse_embeddings = try sparse_embeddings.toOwnedSlice(self.alloc),
                .generated_enrichment_refs = try planned.toOwnedSlice(self.alloc),
            };
        }

        pub fn shouldPrecomputeGeneratedRequest(
            self: *DB,
            mode: GeneratedPrecomputeMode,
            request: enrichment_types.GeneratedEnrichmentRequest,
        ) !bool {
            return switch (mode) {
                .none => false,
                .all => true,
                .full_text_only => blk: {
                    if (request.kind != .chunk_text) break :blk false;
                    const include_default_full_text = request.full_text_index or
                        try chunking_types_mod.parseHasFullTextIndexFromSlice(self.alloc, request.chunker_json);
                    const text_indexes = try self.core.index_manager.textIndexesForChunk(self.alloc, request.artifact_name, include_default_full_text);
                    defer {
                        for (text_indexes) |name| self.alloc.free(name);
                        self.alloc.free(text_indexes);
                    }
                    break :blk text_indexes.len > 0;
                },
            };
        }

        pub fn appendGeneratedEnrichments(
            self: *DB,
            derived_batch_out: *derived_types.DerivedBatch,
            req: types.BatchRequest,
            extracted: []const mapper.ExtractedWrite,
        ) !void {
            var planned = std.ArrayListUnmanaged(enrichment_types.GeneratedEnrichmentRef).empty;
            errdefer {
                for (planned.items) |request| enrichment_types.freeGeneratedRef(self.alloc, request);
                planned.deinit(self.alloc);
            }

            for (req.writes, 0..) |write, i| {
                const cleaned = extracted[i].cleaned_value orelse continue;
                var explicit_dense = std.ArrayListUnmanaged(types.EnrichmentDenseEmbeddingWrite).empty;
                defer {
                    for (explicit_dense.items) |*embedding| embedding.deinit(self.alloc);
                    explicit_dense.deinit(self.alloc);
                }
                for (extracted[i].dense_embeddings) |embedding| {
                    try explicit_dense.append(self.alloc, .{
                        .index_name = try self.alloc.dupe(u8, embedding.index_name),
                        .doc_key = try self.alloc.dupe(u8, embedding.doc_key),
                        .vector = try self.alloc.dupe(f32, embedding.vector),
                    });
                }
                var explicit_sparse = std.ArrayListUnmanaged(types.EnrichmentSparseEmbeddingWrite).empty;
                defer {
                    for (explicit_sparse.items) |*embedding| embedding.deinit(self.alloc);
                    explicit_sparse.deinit(self.alloc);
                }
                for (extracted[i].sparse_embeddings) |embedding| {
                    try explicit_sparse.append(self.alloc, .{
                        .index_name = try self.alloc.dupe(u8, embedding.index_name),
                        .doc_key = try self.alloc.dupe(u8, embedding.doc_key),
                        .indices = try self.alloc.dupe(u32, embedding.indices),
                        .values = try self.alloc.dupe(f32, embedding.values),
                    });
                }
                const generated = try self.core.planGeneratedEnrichments(
                    self.alloc,
                    write.key,
                    cleaned,
                    explicit_dense.items,
                    explicit_sparse.items,
                );
                defer enrichment_types.deinitGeneratedRequests(self.alloc, generated);
                for (generated) |request| {
                    try planned.append(self.alloc, try enrichment_types.requestToRef(self.alloc, request));
                }
            }

            derived_batch_out.generated_enrichment_refs = try planned.toOwnedSlice(self.alloc);
        }

        pub fn deleteEnrichmentArtifactsForBatch(
            self: *DB,
            req: types.BatchRequest,
            extracted: []const mapper.ExtractedWrite,
        ) ![][]u8 {
            _ = extracted;
            if (!self.core.hasArtifactCleanupMaybe()) return try self.alloc.alloc([]u8, 0);

            var deleted = std.ArrayListUnmanaged([]u8).empty;
            errdefer freeOwnedKeySlice(self.alloc, deleted.items);

            for (req.deletes) |key| {
                try self.core.collectAndDeleteEnrichmentArtifactsForDocContext(self.alloc, key, &deleted);
            }
            return try deleted.toOwnedSlice(self.alloc);
        }

        pub fn batchInternal(self: *DB, req: types.BatchRequest, profile: ?*DB.WritePathCallbacks.Profile, opts: DB.WritePathCallbacks.Options) anyerror!void {
            if (!opts.admission_prechecked) {
                if (DB.WritePathCallbacks.open_mode_requires_read_only_backends(self.open_mode)) return error.ReadOnly;
                if (!opts.bypass_ha_write_gate) try DB.WritePathCallbacks.enforce_ha_write_gate(self);
            }
            if (!opts.bypass_ha_write_gate) try DB.WritePathCallbacks.preflight_ha_batch_sync_commit(self);
            const total_start_ns = DB.WritePathCallbacks.monotonic_time_ns();
            defer {
                if (profile) |active_profile| {
                    active_profile.total_ns += DB.WritePathCallbacks.monotonic_time_ns() - total_start_ns;
                }
            }

            try self.executor.failIfUnhealthy();

            DB.WritePathCallbacks.lock_apply(self);
            var apply_mutex_held = true;
            errdefer if (apply_mutex_held) self.core.unlockApply();

            if (self.bulk_ingest_coalescer.active and !self.flushing_bulk_ingest_coalescer) {
                if (self.bulk_ingest_coalescer.hasPending()) {
                    self.core.unlockApply();
                    apply_mutex_held = false;
                    try flushBulkIngestCoalescerWithSyncLevel(self, req.sync_level, profile);
                    DB.WritePathCallbacks.lock_apply(self);
                    apply_mutex_held = true;
                }
            }

            const resolve_transforms_start_ns = DB.WritePathCallbacks.monotonic_time_ns();
            try validateCreateOnlyBatchWriteRequest(self.alloc, req);
            var effective_ops = try coalesceKeyValueRequest(self, types.BatchWrite, req.writes, req.deletes, req.transforms);
            defer effective_ops.deinit(self.alloc);
            if (profile) |active_profile| DB.WritePathCallbacks.record_profile_ns(profile, &active_profile.resolve_transforms_ns, resolve_transforms_start_ns);

            const merge_effective_req_start_ns = DB.WritePathCallbacks.monotonic_time_ns();

            const effective_req: types.BatchRequest = .{
                .writes = effective_ops.writes,
                .deletes = effective_ops.deletes,
                .relational_identity_rewrites = req.relational_identity_rewrites,
                .graph_writes = req.graph_writes,
                .graph_deletes = req.graph_deletes,
                .transforms = &.{},
                .predicates = req.predicates,
                .timestamp_ns = req.timestamp_ns,
                .sync_level = req.sync_level,
                .write_mode = req.write_mode,
            };
            if (profile) |active_profile| DB.WritePathCallbacks.record_profile_ns(profile, &active_profile.merge_effective_req_ns, merge_effective_req_start_ns);

            var effective_predicates = std.ArrayListUnmanaged(transactions_mod.VersionPredicate).empty;
            defer effective_predicates.deinit(self.alloc);
            var owned_row_claim_predicate_keys = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (owned_row_claim_predicate_keys.items) |key| self.alloc.free(key);
                owned_row_claim_predicate_keys.deinit(self.alloc);
            }
            if (effective_req.predicates.len > 0) {
                for (effective_req.predicates) |predicate| {
                    try effective_predicates.append(self.alloc, .{
                        .key = predicate.key,
                        .expected_version = predicate.expected_version,
                    });
                }
            }
            try DB.WritePathCallbacks.append_row_claim_predicates_for_mutation_keys(
                self.alloc,
                &effective_predicates,
                &owned_row_claim_predicate_keys,
                effective_req.writes,
                effective_req.deletes,
            );
            try DB.WritePathCallbacks.append_row_claim_predicates_for_identity_rewrites(
                self.alloc,
                &effective_predicates,
                &owned_row_claim_predicate_keys,
                effective_req.relational_identity_rewrites,
            );
            _ = try DB.WritePathCallbacks.reclaim_expired_row_claim_intents_for_mutation_keys(
                self,
                effective_req.writes,
                effective_req.deletes,
                null,
                DB.WritePathCallbacks.monotonic_time_ns(),
                true,
            );
            _ = try DB.WritePathCallbacks.reclaim_expired_row_claim_intents_for_identity_rewrites(
                self,
                effective_req.relational_identity_rewrites,
                null,
                DB.WritePathCallbacks.monotonic_time_ns(),
                true,
            );

            if (effective_predicates.items.len > 0) {
                const predicates_start_ns = DB.WritePathCallbacks.monotonic_time_ns();
                try self.core.checkVersionPredicates(effective_predicates.items, null);
                if (profile) |active_profile| DB.WritePathCallbacks.record_profile_ns(profile, &active_profile.predicates_ns, predicates_start_ns);
            }

            if (opts.validate_range_ownership) {
                const validate_range_start_ns = DB.WritePathCallbacks.monotonic_time_ns();
                try self.core.validateBatchRangeOwnership(effective_req);
                if (profile) |active_profile| DB.WritePathCallbacks.record_profile_ns(profile, &active_profile.validate_range_ns, validate_range_start_ns);
            }

            const extract_writes_start_ns = DB.WritePathCallbacks.monotonic_time_ns();
            var extracted = try self.alloc.alloc(mapper.ExtractedWrite, effective_req.writes.len);
            var extracted_initialized: usize = 0;
            defer {
                for (extracted[0..extracted_initialized]) |*item| item.deinit(self.alloc);
                self.alloc.free(extracted);
            }

            var store_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
            defer store_writes.deinit(self.alloc);
            var owned_store_keys = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (owned_store_keys.items) |key| self.alloc.free(key);
                owned_store_keys.deinit(self.alloc);
            }
            var owned_store_values = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (owned_store_values.items) |value| self.alloc.free(value);
                owned_store_values.deinit(self.alloc);
            }
            const vector_field_names_start_ns = DB.WritePathCallbacks.monotonic_time_ns();
            const vector_store_field_names = try self.core.index_manager.vectorStoreFieldNamesAlloc(self.alloc);
            if (profile) |active_profile| DB.WritePathCallbacks.record_profile_ns(profile, &active_profile.extract_vector_field_names_ns, vector_field_names_start_ns);
            defer {
                for (vector_store_field_names) |field| self.alloc.free(field);
                if (vector_store_field_names.len > 0) self.alloc.free(vector_store_field_names);
            }
            var timestamp_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
            defer {
                for (timestamp_writes.items) |item| {
                    self.alloc.free(@constCast(item.key));
                    self.alloc.free(@constCast(item.value));
                }
                timestamp_writes.deinit(self.alloc);
            }
            var explicit_embedding_artifact_writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
            defer {
                for (explicit_embedding_artifact_writes.items) |item| {
                    self.alloc.free(@constCast(item.key));
                    self.alloc.free(@constCast(item.value));
                }
                explicit_embedding_artifact_writes.deinit(self.alloc);
            }
            var explicit_graph_artifact_writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
            defer {
                for (explicit_graph_artifact_writes.items) |item| {
                    self.alloc.free(@constCast(item.key));
                    self.alloc.free(@constCast(item.value));
                }
                explicit_graph_artifact_writes.deinit(self.alloc);
            }
            var delete_keys = std.ArrayListUnmanaged([]const u8).empty;
            defer delete_keys.deinit(self.alloc);
            var owned_delete_keys = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (owned_delete_keys.items) |key| self.alloc.free(key);
                owned_delete_keys.deinit(self.alloc);
            }
            var relational_participant = relational_store_mod.WriteParticipant.initWithColumnIndexPolicy(
                self.alloc,
                self.core.store,
                &store_writes,
                &delete_keys,
                &owned_store_keys,
                &owned_store_values,
                DB.WritePathCallbacks.relational_column_index_policy_for_store(self),
            );
            if (self.core.schema) |runtime_schema| {
                relational_participant.configureForeignKeys(runtime_schema.default_type, runtime_schema.foreign_keys, effective_req.deletes);
                relational_participant.configurePrimaryKey(runtime_schema.primary_key);
                relational_participant.configureUniqueConstraints(runtime_schema.unique_constraints);
                relational_participant.configurePeriods(runtime_schema.periods, runtime_schema.relational_columns);
            }
            var relational_participant_prepared = false;
            var relational_participant_closed = false;
            defer if (relational_participant_prepared and !relational_participant_closed)
                relational_participant.abort(null);
            var graph_artifact_clears = std.ArrayListUnmanaged(GraphArtifactClear).empty;
            defer {
                for (graph_artifact_clears.items) |*item| item.deinit(self.alloc);
                graph_artifact_clears.deinit(self.alloc);
            }
            var timestamp_delete_keys = std.ArrayListUnmanaged([]const u8).empty;
            defer {
                for (timestamp_delete_keys.items) |key| self.alloc.free(@constCast(key));
                timestamp_delete_keys.deinit(self.alloc);
            }
            var overwritten_flags = try self.alloc.alloc(bool, effective_req.writes.len);
            defer self.alloc.free(overwritten_flags);
            @memset(overwritten_flags, false);
            var overwrite_probe_entries = std.ArrayListUnmanaged(OverwriteProbeEntry).empty;
            defer overwrite_probe_entries.deinit(self.alloc);
            var identity_upsert_keys = std.ArrayListUnmanaged([]const u8).empty;
            defer identity_upsert_keys.deinit(self.alloc);
            var identity_upsert_write_indexes = std.ArrayListUnmanaged(usize).empty;
            defer identity_upsert_write_indexes.deinit(self.alloc);
            var identity_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
            defer {
                for (identity_writes.items) |item| {
                    self.alloc.free(@constCast(item.key));
                    self.alloc.free(@constCast(item.value));
                }
                identity_writes.deinit(self.alloc);
            }

            const batch_timestamp_ns = if (effective_req.timestamp_ns != 0) effective_req.timestamp_ns else DB.WritePathCallbacks.current_time_ns();

            for (effective_req.writes, 0..) |write, i| {
                if (DB.WritePathCallbacks.is_metadata_key(write.key)) {
                    extracted[i] = .{
                        .cleaned_value = null,
                        .graph_writes = &.{},
                        .mentioned_graph_indexes = &.{},
                        .dense_embeddings = &.{},
                        .sparse_embeddings = &.{},
                    };
                    extracted_initialized += 1;
                    try store_writes.append(self.alloc, .{
                        .key = write.key,
                        .value = write.value,
                    });
                    continue;
                }
                const mapper_extract_start_ns = DB.WritePathCallbacks.monotonic_time_ns();
                extracted[i] = try mapper.extractWrite(self.alloc, write.key, write.value);
                if (profile) |active_profile| DB.WritePathCallbacks.record_profile_ns(profile, &active_profile.extract_mapper_ns, mapper_extract_start_ns);
                const graph_field_extract_start_ns = DB.WritePathCallbacks.monotonic_time_ns();
                try DB.WritePathCallbacks.augment_extracted_write_with_graph_field_edges(self, self.alloc, write.key, write.value, &extracted[i]);
                if (profile) |active_profile| DB.WritePathCallbacks.record_profile_ns(profile, &active_profile.extract_graph_fields_ns, graph_field_extract_start_ns);
                const index_field_embeddings_start_ns = DB.WritePathCallbacks.monotonic_time_ns();
                try self.core.index_manager.appendIndexFieldEmbeddingsToExtractedWrite(self.alloc, write.key, write.value, &extracted[i]);
                if (profile) |active_profile| DB.WritePathCallbacks.record_profile_ns(profile, &active_profile.extract_index_field_embeddings_ns, index_field_embeddings_start_ns);
                extracted_initialized += 1;
                const embedding_artifacts_start_ns = DB.WritePathCallbacks.monotonic_time_ns();
                for (extracted[i].dense_embeddings) |*embedding| {
                    if (embedding.artifact_key != null) continue;
                    embedding.artifact_key = try appendEmbeddingArtifactWrite(
                        self.alloc,
                        &explicit_embedding_artifact_writes,
                        write.key,
                        write.key,
                        embedding.index_name,
                        "_embeddings",
                        null,
                        null,
                        embedding.vector,
                    );
                }
                for (extracted[i].sparse_embeddings) |*embedding| {
                    if (embedding.artifact_key != null) continue;
                    embedding.artifact_key = try appendSparseEmbeddingArtifactWrite(
                        self.alloc,
                        &explicit_embedding_artifact_writes,
                        write.key,
                        embedding.index_name,
                        null,
                        embedding.indices,
                        embedding.values,
                    );
                }
                if (profile) |active_profile| DB.WritePathCallbacks.record_profile_ns(profile, &active_profile.extract_embedding_artifacts_ns, embedding_artifacts_start_ns);
                const graph_artifacts_start_ns = DB.WritePathCallbacks.monotonic_time_ns();
                for (extracted[i].graph_writes) |graph_write| {
                    try appendGraphEdgeArtifactWrite(self.alloc, &explicit_graph_artifact_writes, graph_write);
                }
                for (extracted[i].mentioned_graph_indexes) |index_name| {
                    try graph_artifact_clears.append(self.alloc, .{
                        .doc_key = try self.alloc.dupe(u8, write.key),
                        .index_name = try self.alloc.dupe(u8, index_name),
                    });
                }
                if (profile) |active_profile| DB.WritePathCallbacks.record_profile_ns(profile, &active_profile.extract_graph_artifacts_ns, graph_artifacts_start_ns);
                if (extracted[i].cleaned_value) |cleaned| {
                    try validateDocumentExtractionInlineSources(self, cleaned);
                    const strip_store_value_start_ns = DB.WritePathCallbacks.monotonic_time_ns();
                    // Relational tables project the document once into a typed row
                    // and store that row as the table's only base document record.
                    // Document-mode tables keep the JSON blob under the primary
                    // document key.
                    const relational_columns = DB.WritePathCallbacks.relational_columns_for_store(self);
                    const store_value = if (relational_columns) |columns|
                        try relational_store_mod.relationalStoreRowValueAlloc(self.alloc, cleaned, columns, &owned_store_values)
                    else
                        try strippedStoredDocumentValueAlloc(
                            self.alloc,
                            cleaned,
                            vector_store_field_names,
                            &owned_store_values,
                        );
                    if (profile) |active_profile| DB.WritePathCallbacks.record_profile_ns(profile, &active_profile.extract_strip_store_value_ns, strip_store_value_start_ns);
                    const store_key = if (relational_columns != null) blk: {
                        const row_write_index = store_writes.items.len;
                        relational_participant.prepareUpsert("", write.key, store_value, null) catch |err| {
                            if (err == error.ForeignKeyViolation) DB.WritePathCallbacks.record_foreign_key_child_write_reject(self);
                            return err;
                        };
                        relational_participant_prepared = true;
                        const primary_key = try internal_keys.documentKeyAlloc(self.alloc, write.key);
                        var primary_key_owned = true;
                        errdefer if (primary_key_owned) self.alloc.free(primary_key);
                        try owned_delete_keys.append(self.alloc, primary_key);
                        primary_key_owned = false;
                        try delete_keys.append(self.alloc, primary_key);
                        break :blk store_writes.items[row_write_index].key;
                    } else blk: {
                        const key = try internal_keys.documentKeyAlloc(self.alloc, write.key);
                        try owned_store_keys.append(self.alloc, key);
                        try store_writes.append(self.alloc, .{
                            .key = key,
                            .value = store_value,
                        });
                        break :blk key;
                    };
                    try overwrite_probe_entries.append(self.alloc, .{
                        .key = store_key,
                        .write_index = i,
                    });
                    try identity_upsert_keys.append(self.alloc, write.key);
                    try identity_upsert_write_indexes.append(self.alloc, i);
                    if (DB.WritePathCallbacks.should_write_timestamp(write.key)) {
                        const timestamp_start_ns = DB.WritePathCallbacks.monotonic_time_ns();
                        const write_timestamp_ns = try DB.WritePathCallbacks.resolve_write_timestamp_ns(self, batch_timestamp_ns, write.value);
                        const timestamp_key = try DB.WritePathCallbacks.make_timestamp_key(self.alloc, write.key);
                        const timestamp_value = try DB.WritePathCallbacks.encode_timestamp_value(self.alloc, write_timestamp_ns);
                        try timestamp_writes.append(self.alloc, .{
                            .key = timestamp_key,
                            .value = timestamp_value,
                        });
                        if (profile) |active_profile| DB.WritePathCallbacks.record_profile_ns(profile, &active_profile.extract_timestamp_ns, timestamp_start_ns);
                    }
                }
            }
            for (effective_req.relational_identity_rewrites) |rewrite| {
                const relational_columns = DB.WritePathCallbacks.relational_columns_for_store(self) orelse return error.UnsupportedOperation;
                const store_value = try relational_store_mod.relationalStoreRowValueAlloc(self.alloc, rewrite.value, relational_columns, &owned_store_values);
                relational_participant.prepareIdentityRewrite("", rewrite.old_key, rewrite.new_key, store_value, null) catch |err| {
                    if (err == error.ForeignKeyViolation) DB.WritePathCallbacks.record_foreign_key_parent_delete_reject(self);
                    return err;
                };
                relational_participant_prepared = true;
                try identity_upsert_keys.append(self.alloc, rewrite.new_key);
                const old_document_key = try internal_keys.documentKeyAlloc(self.alloc, rewrite.old_key);
                var old_document_key_owned = true;
                errdefer if (old_document_key_owned) self.alloc.free(old_document_key);
                try owned_delete_keys.append(self.alloc, old_document_key);
                old_document_key_owned = false;
                try delete_keys.append(self.alloc, old_document_key);
                const new_document_key = try internal_keys.documentKeyAlloc(self.alloc, rewrite.new_key);
                var new_document_key_owned = true;
                errdefer if (new_document_key_owned) self.alloc.free(new_document_key);
                try owned_delete_keys.append(self.alloc, new_document_key);
                new_document_key_owned = false;
                try delete_keys.append(self.alloc, new_document_key);
            }
            if (profile) |active_profile| {
                active_profile.identity_upsert_keys += @intCast(identity_upsert_keys.items.len);
                active_profile.identity_delete_keys += @intCast(effective_req.deletes.len);
            }
            const identity_capacity_start_ns = DB.WritePathCallbacks.monotonic_time_ns();
            if (!self.bulk_ingest_identity_all_new or effective_req.deletes.len != 0) {
                try failIfIdentityOrdinalExhaustedForNewUpserts(self, identity_upsert_keys.items);
            }
            if (profile) |active_profile| DB.WritePathCallbacks.record_profile_ns(profile, &active_profile.identity_capacity_check_ns, identity_capacity_start_ns);

            var assume_all_new_identity_upserts = false;
            if (effective_req.write_mode == .upsert and self.bulk_ingest_identity_all_new and effective_req.deletes.len == 0 and identity_upsert_keys.items.len > 0) {
                assume_all_new_identity_upserts = try rememberBulkIngestAllNewIdentityUpserts(self, identity_upsert_keys.items);
                if (!assume_all_new_identity_upserts) clearBulkIngestIdentityAllNewLocked(self);
            }

            if (!assume_all_new_identity_upserts and overwrite_probe_entries.items.len > 0) {
                const overwrite_probe_start_ns = DB.WritePathCallbacks.monotonic_time_ns();
                std.sort.pdq(OverwriteProbeEntry, overwrite_probe_entries.items, {}, overwriteProbeLessThan);
                const probe_keys = try self.alloc.alloc([]const u8, overwrite_probe_entries.items.len);
                defer self.alloc.free(probe_keys);
                const probe_values = try self.alloc.alloc(?[]const u8, overwrite_probe_entries.items.len);
                defer self.alloc.free(probe_values);
                for (overwrite_probe_entries.items, 0..) |entry, i| {
                    probe_keys[i] = entry.key;
                }
                var overwrite_probe_txn = try self.core.store.beginProbeTxn();
                defer overwrite_probe_txn.abort();
                try overwrite_probe_txn.getManySorted(probe_keys, probe_values);
                for (overwrite_probe_entries.items, 0..) |entry, i| {
                    overwritten_flags[entry.write_index] = probe_values[i] != null;
                }
                if (profile) |active_profile| DB.WritePathCallbacks.record_profile_ns(profile, &active_profile.overwrite_probe_ns, overwrite_probe_start_ns);
            }
            try enforceCreateOnlyNoOverwrite(effective_req.write_mode, overwritten_flags);

            for (effective_req.graph_writes) |graph_write| {
                try appendGraphEdgeArtifactWrite(self.alloc, &explicit_graph_artifact_writes, graph_write);
            }

            var changed_graph_artifact_keys = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (changed_graph_artifact_keys.items) |key| self.alloc.free(key);
                changed_graph_artifact_keys.deinit(self.alloc);
            }
            for (graph_artifact_clears.items) |clear| {
                const existing = try collectGraphArtifactsForDocIndex(self.alloc, self.core.store, clear.doc_key, clear.index_name);
                defer docstore_mod.DocStore.freeResults(self.alloc, existing);
                for (existing) |entry| {
                    if (containsBatchWriteKey(explicit_graph_artifact_writes.items, entry.key)) continue;
                    if (containsOwnedKey(owned_delete_keys.items, entry.key)) continue;
                    const owned_key = try self.alloc.dupe(u8, entry.key);
                    try owned_delete_keys.append(self.alloc, owned_key);
                    try delete_keys.append(self.alloc, owned_key);
                    try appendUniqueOwnedKey(self.alloc, &changed_graph_artifact_keys, entry.key);
                }
            }

            try store_writes.appendSlice(self.alloc, timestamp_writes.items);
            for (explicit_embedding_artifact_writes.items) |write| {
                try store_writes.append(self.alloc, .{
                    .key = write.key,
                    .value = write.value,
                });
            }
            for (explicit_graph_artifact_writes.items) |write| {
                try store_writes.append(self.alloc, .{
                    .key = write.key,
                    .value = write.value,
                });
                try appendUniqueOwnedKey(self.alloc, &changed_graph_artifact_keys, write.key);
            }
            for (effective_req.graph_deletes) |delete| {
                const artifact_key = try internal_keys.graphEdgeArtifactKeyAlloc(self.alloc, delete.source, delete.index_name, delete.edge_type, delete.target);
                defer self.alloc.free(artifact_key);
                if (containsBatchWriteKey(explicit_graph_artifact_writes.items, artifact_key)) continue;
                if (containsOwnedKey(owned_delete_keys.items, artifact_key)) continue;
                const owned_key = try self.alloc.dupe(u8, artifact_key);
                try owned_delete_keys.append(self.alloc, owned_key);
                try delete_keys.append(self.alloc, owned_key);
                try appendUniqueOwnedKey(self.alloc, &changed_graph_artifact_keys, artifact_key);
            }
            for (effective_req.deletes) |key| {
                if (DB.WritePathCallbacks.relational_columns_for_store(self) != null) {
                    relational_participant.prepareDelete("", key, null) catch |err| {
                        if (err == error.ForeignKeyViolation) DB.WritePathCallbacks.record_foreign_key_parent_delete_reject(self);
                        return err;
                    };
                    relational_participant_prepared = true;
                    const primary_key = try internal_keys.documentKeyAlloc(self.alloc, key);
                    var primary_key_owned = true;
                    errdefer if (primary_key_owned) self.alloc.free(primary_key);
                    try owned_delete_keys.append(self.alloc, primary_key);
                    primary_key_owned = false;
                    try delete_keys.append(self.alloc, primary_key);
                } else {
                    const store_key = try internal_keys.documentKeyAlloc(self.alloc, key);
                    try owned_delete_keys.append(self.alloc, store_key);
                    try delete_keys.append(self.alloc, store_key);
                }
                if (DB.WritePathCallbacks.should_write_timestamp(key)) {
                    const timestamp_key = try DB.WritePathCallbacks.make_timestamp_key(self.alloc, key);
                    try timestamp_delete_keys.append(self.alloc, timestamp_key);
                    try delete_keys.append(self.alloc, timestamp_key);
                }
            }
            if (profile) |active_profile| DB.WritePathCallbacks.record_profile_ns(profile, &active_profile.extract_writes_ns, extract_writes_start_ns);

            const delete_artifacts_start_ns = DB.WritePathCallbacks.monotonic_time_ns();
            const deleted_artifact_keys = try deleteEnrichmentArtifactsForBatch(self, effective_req, extracted[0..extracted_initialized]);
            defer freeOwnedKeySlice(self.alloc, deleted_artifact_keys);
            if (profile) |active_profile| DB.WritePathCallbacks.record_profile_ns(profile, &active_profile.delete_artifacts_ns, delete_artifacts_start_ns);

            const use_thin_replay_fast_path =
                effective_req.sync_level != .full_text and
                effective_req.sync_level != .enrichments and
                effective_req.sync_level != .aknn and
                effective_req.sync_level != .full_index and
                opts.force_generated_artifact_names.len == 0 and
                !DB.WritePathCallbacks.split_shadow_requires_materialized_derived_batch(self);
            const include_generated_enrichment_hint = use_thin_replay_fast_path and
                self.core.hasGeneratedEnrichmentTargets();

            var precomputed_generated: PrecomputedGeneratedBatch = .{};
            defer precomputed_generated.deinit(self.alloc);
            var remote_child_range_dispatches = std.ArrayListUnmanaged(DocumentChildRangeDispatchGroup).empty;
            defer {
                for (remote_child_range_dispatches.items) |*dispatch| dispatch.deinit(self.alloc);
                remote_child_range_dispatches.deinit(self.alloc);
            }
            var owned_child_range_outbox_keys = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (owned_child_range_outbox_keys.items) |key| self.alloc.free(key);
                owned_child_range_outbox_keys.deinit(self.alloc);
            }
            var owned_child_range_outbox_values = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (owned_child_range_outbox_values.items) |value| self.alloc.free(value);
                owned_child_range_outbox_values.deinit(self.alloc);
            }
            var materialized_graph_artifact_writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
            defer {
                for (materialized_graph_artifact_writes.items) |write| {
                    self.alloc.free(@constCast(write.key));
                    self.alloc.free(@constCast(write.value));
                }
                materialized_graph_artifact_writes.deinit(self.alloc);
            }
            if (!use_thin_replay_fast_path) {
                const precompute_generated_start_ns = DB.WritePathCallbacks.monotonic_time_ns();
                precomputed_generated = try prepareGeneratedEnrichments(
                    self,
                    effective_req,
                    extracted[0..extracted_initialized],
                    generatedPrecomputeModeForSyncLevel(effective_req.sync_level),
                    opts.force_generated_artifact_names,
                );

                if (opts.document_child_range_dispatcher != null) {
                    try partitionRemoteDocumentChildRangeGeneratedBatch(self, &precomputed_generated, &remote_child_range_dispatches);
                }
                for (precomputed_generated.artifact_writes) |write| {
                    try store_writes.append(self.alloc, .{
                        .key = write.key,
                        .value = write.value,
                    });
                    try appendUniqueOwnedKey(self.alloc, &changed_graph_artifact_keys, write.key);
                }
                for (precomputed_generated.artifact_delete_keys) |key| {
                    try delete_keys.append(self.alloc, key);
                    if (internal_keys.isAssetArtifactKey(key)) {
                        try appendUniqueOwnedKey(self.alloc, &changed_graph_artifact_keys, key);
                    }
                }
                try DB.WritePathCallbacks.append_precomputed_graph_source_artifacts(
                    self,
                    precomputed_generated.artifact_writes,
                    precomputed_generated.artifact_delete_keys,
                    &materialized_graph_artifact_writes,
                    &store_writes,
                    &delete_keys,
                    &owned_delete_keys,
                    &changed_graph_artifact_keys,
                );
                if (profile) |active_profile| DB.WritePathCallbacks.record_profile_ns(profile, &active_profile.precompute_generated_ns, precompute_generated_start_ns);
            }
            if (explicit_embedding_artifact_writes.items.len > 0 or
                explicit_graph_artifact_writes.items.len > 0 or
                materialized_graph_artifact_writes.items.len > 0 or
                precomputed_generated.artifact_writes.len > 0)
            {
                try self.core.appendArtifactPresenceMarker(&store_writes);
            }
            try appendAssetArtifactSourceIndexMutations(
                self.alloc,
                &store_writes,
                deleted_artifact_keys,
                &delete_keys,
                &owned_store_keys,
                &owned_store_values,
                &owned_delete_keys,
            );

            var sync_targets: ManagedSyncTargets = .{};
            defer sync_targets.deinit(self.alloc);
            var materialized_derived_batch: ?derived_types.DerivedBatch = null;
            defer if (materialized_derived_batch) |*materialized_batch| derived_types.deinitDerivedBatch(self.alloc, materialized_batch);
            const sequence = self.core.reserveDerivedAppendSequence();
            const identity_metadata_start_ns = DB.WritePathCallbacks.monotonic_time_ns();
            var used_trusted_identity_path = false;
            if (effective_req.deletes.len != 0) {
                clearBulkIngestIdentityAllNewLocked(self);
            }
            if (self.bulk_ingest_identity_all_new and
                effective_req.deletes.len == 0 and
                identity_upsert_keys.items.len > 0 and
                (assume_all_new_identity_upserts or identityUpsertStoreWritesAreNew(identity_upsert_write_indexes.items, overwritten_flags)))
            {
                used_trusted_identity_path = try doc_identity.appendBatchIdentityMetadataAllNewTrustedStateForNamespaceAlloc(
                    self.alloc,
                    self.core.identity_namespace,
                    sequence,
                    &identity_writes,
                    identity_upsert_keys.items,
                    &self.bulk_ingest_identity_state,
                );
                if (!used_trusted_identity_path) clearBulkIngestIdentityAllNewLocked(self);
            }
            if (!used_trusted_identity_path) {
                try doc_identity.appendBatchIdentityMetadataForNamespaceAlloc(
                    self.alloc,
                    self.core.store,
                    self.core.identity_namespace,
                    sequence,
                    &identity_writes,
                    identity_upsert_keys.items,
                    effective_req.deletes,
                );
            }
            if (profile) |active_profile| {
                DB.WritePathCallbacks.record_profile_ns(profile, &active_profile.identity_metadata_ns, identity_metadata_start_ns);
                active_profile.identity_metadata_writes += @intCast(identity_writes.items.len);
            }
            const pending_identity_visibility_summary = try doc_identity.visibilitySummaryFromWrites(identity_writes.items);
            try store_writes.appendSlice(self.alloc, identity_writes.items);
            try appendDocumentChildRangeOutboxWrites(
                self.alloc,
                sequence,
                remote_child_range_dispatches.items,
                effective_req.sync_level,
                &store_writes,
                &owned_child_range_outbox_keys,
                &owned_child_range_outbox_values,
            );
            if (self.core.schema) |runtime_schema| {
                if (runtime_schema.system_versioned) {
                    try DB.WritePathCallbacks.append_system_versioned_history_for_batch(
                        self,
                        effective_req,
                        sequence,
                        batch_timestamp_ns,
                        &store_writes,
                        &owned_store_keys,
                        &owned_store_values,
                    );
                }
            }
            const build_derived_start_ns = DB.WritePathCallbacks.monotonic_time_ns();
            const replay_payload = if (use_thin_replay_fast_path)
                try DB.WritePathCallbacks.encode_thin_replay_record_payload(
                    self.alloc,
                    effective_req,
                    extracted[0..extracted_initialized],
                    deleted_artifact_keys,
                    changed_graph_artifact_keys.items,
                    overwritten_flags,
                    sequence,
                    include_generated_enrichment_hint,
                )
            else blk: {
                materialized_derived_batch = try db_internal.buildDerivedBatch(self.alloc, effective_req, extracted[0..extracted_initialized], deleted_artifact_keys, changed_graph_artifact_keys.items);
                for (materialized_derived_batch.?.overwritten_doc_keys) |key| self.alloc.free(@constCast(key));
                if (materialized_derived_batch.?.overwritten_doc_keys.len > 0) self.alloc.free(materialized_derived_batch.?.overwritten_doc_keys);
                materialized_derived_batch.?.overwritten_doc_keys = try db_internal.buildOverwrittenDocKeys(self.alloc, effective_req.writes, overwritten_flags);
                materialized_derived_batch.?.documents = try takeOwnedSlice(derived_types.DerivedDocument, self.alloc, materialized_derived_batch.?.documents, &precomputed_generated.documents);
                materialized_derived_batch.?.dense_embeddings = try takeOwnedSlice(derived_types.DerivedDenseEmbeddingWrite, self.alloc, materialized_derived_batch.?.dense_embeddings, &precomputed_generated.dense_embeddings);
                materialized_derived_batch.?.sparse_embeddings = try takeOwnedSlice(derived_types.DerivedSparseEmbeddingWrite, self.alloc, materialized_derived_batch.?.sparse_embeddings, &precomputed_generated.sparse_embeddings);
                materialized_derived_batch.?.generated_enrichment_refs = precomputed_generated.generated_enrichment_refs;
                precomputed_generated.generated_enrichment_refs = &.{};
                materialized_derived_batch.?.sequence = sequence;
                const payload = try DB.WritePathCallbacks.encode_change_record_payload(self, materialized_derived_batch.?, sequence);
                try DB.WritePathCallbacks.attach_inline_upsert_document_values(self.alloc, &materialized_derived_batch.?, effective_req, extracted[0..extracted_initialized]);

                const apply_shadow_start_ns = DB.WritePathCallbacks.monotonic_time_ns();
                try DB.WritePathCallbacks.apply_derived_batch_to_shadow_if_needed(self, materialized_derived_batch.?);
                if (profile) |active_profile| DB.WritePathCallbacks.record_profile_ns(profile, &active_profile.apply_shadow_ns, apply_shadow_start_ns);

                const collect_sync_targets_start_ns = DB.WritePathCallbacks.monotonic_time_ns();
                sync_targets = try DB.WritePathCallbacks.collect_managed_sync_targets(self.alloc, self.core.index_manager, materialized_derived_batch.?);
                if (profile) |active_profile| DB.WritePathCallbacks.record_profile_ns(profile, &active_profile.collect_sync_targets_ns, collect_sync_targets_start_ns);
                break :blk payload;
            };
            defer self.alloc.free(replay_payload);
            if (profile) |active_profile| DB.WritePathCallbacks.record_profile_ns(profile, &active_profile.build_derived_ns, build_derived_start_ns);

            const store_write_start_ns = DB.WritePathCallbacks.monotonic_time_ns();
            const store_batch_options: backend_types.BatchOptions = if (opts.store_batch_options.mode != .default)
                opts.store_batch_options
            else if (self.bulk_ingest_coalescer.active)
                .{ .mode = .bulk_ingest }
            else
                .{};
            if (relational_participant_prepared) {
                relational_participant.commit(null, sequence) catch |err| {
                    if (err == error.ForeignKeyViolation) DB.WritePathCallbacks.record_foreign_key_child_write_reject(self);
                    return err;
                };
                relational_participant_closed = true;
            }
            var ha_applied_lsn_value_buf: [ha_types.applied_lsn_value_len]u8 = undefined;
            if (opts.ha_applied_lsn_marker) |lsn| {
                if (lsn != 0) {
                    try store_writes.append(self.alloc, ha_types.appliedReplicationLsnWrite(lsn, &ha_applied_lsn_value_buf));
                }
            }
            const replay_append: ?docstore_mod.DocStore.ReplayAppend = if (opts.suppress_derived_replay_append)
                null
            else
                .{
                    .sequence = sequence,
                    .payload = replay_payload,
                };
            try self.core.store.putBatchWithReplayWithOptions(
                self.backend_runtime.io(),
                store_writes.items,
                delete_keys.items,
                replay_append,
                store_batch_options,
            );
            if (!opts.bypass_ha_write_gate) {
                try DB.WritePathCallbacks.mirror_ha_batch_mutation_commit(self, effective_req);
                try DB.WritePathCallbacks.mirror_ha_replay_payload_commit(self, replay_payload);
            }
            if (pending_identity_visibility_summary) |summary| {
                self.identity_visibility_summary_cache = summary;
            }
            if (profile) |active_profile| {
                DB.WritePathCallbacks.record_profile_ns(profile, &active_profile.store_write_ns, store_write_start_ns);
                active_profile.store_write_count += @intCast(store_writes.items.len);
                active_profile.store_delete_count += @intCast(delete_keys.items.len);
            }
            if (DB.WritePathCallbacks.should_append_split_delta(self)) {
                const split_delta_start_ns = DB.WritePathCallbacks.monotonic_time_ns();
                try self.core.appendSplitDelta(batch_timestamp_ns, store_writes.items, delete_keys.items);
                if (profile) |active_profile| DB.WritePathCallbacks.record_profile_ns(profile, &active_profile.split_delta_ns, split_delta_start_ns);
            }

            if (!opts.suppress_derived_replay_append) {
                const append_replay_journal_start_ns = DB.WritePathCallbacks.monotonic_time_ns();
                self.executor.trackBacklogBytes(sequence, @intCast(replay_payload.len)) catch {};
                if (profile) |active_profile| DB.WritePathCallbacks.record_profile_ns(profile, &active_profile.append_replay_journal_ns, append_replay_journal_start_ns);
            }
            self.core.unlockApply();
            apply_mutex_held = false;
            if (opts.document_child_range_dispatcher) |dispatcher| {
                _ = try self.drainDocumentArtifactChildRangeOutbox(dispatcher, 0);
            }
            if (!opts.suppress_derived_replay_append) {
                const backlog_pressure_start_ns = DB.WritePathCallbacks.monotonic_time_ns();
                try DB.WritePathCallbacks.mark_precomputed_enrichment_applied_for_sync(self, effective_req.sync_level, sequence);
                try DB.WritePathCallbacks.apply_derived_backlog_pressure(self, sequence, effective_req.sync_level, sync_targets);
                if (profile) |active_profile| DB.WritePathCallbacks.record_profile_ns(profile, &active_profile.backlog_pressure_ns, backlog_pressure_start_ns);
            }
            const wait_sync_start_ns = DB.WritePathCallbacks.monotonic_time_ns();
            if (!opts.suppress_derived_replay_append and self.executor.hasWorkers()) {
                const notify_executor_start_ns = DB.WritePathCallbacks.monotonic_time_ns();
                DB.WritePathCallbacks.notify_executor_for_sync_level_with_dense_bulk_deferral(self.async_context, self.executor, effective_req.sync_level, sequence, sync_targets);
                if (profile) |active_profile| DB.WritePathCallbacks.record_profile_ns(profile, &active_profile.executor_notify_ns, notify_executor_start_ns);
                if (opts.wait_for_sync_level) {
                    const sync_wait_start_ns = DB.WritePathCallbacks.monotonic_time_ns();
                    try DB.WritePathCallbacks.wait_for_sync_level(self, effective_req.sync_level, sequence, sync_targets);
                    if (profile) |active_profile| DB.WritePathCallbacks.record_profile_ns(profile, &active_profile.sync_wait_ns, sync_wait_start_ns);
                }
            } else {
                if (opts.wait_for_sync_level and DB.WritePathCallbacks.sync_level_requires_derived_visibility(effective_req.sync_level)) {
                    const derived_apply_start_ns = DB.WritePathCallbacks.monotonic_time_ns();
                    if (effective_req.sync_level == .full_text) {
                        try DB.WritePathCallbacks.apply_derived_batch_targets_profiled(self, materialized_derived_batch.?, sync_targets.full_text_indexes, profile);
                    } else {
                        try DB.WritePathCallbacks.apply_derived_batch_profiled(self, materialized_derived_batch.?, profile);
                    }
                    if (profile) |active_profile| DB.WritePathCallbacks.record_profile_ns(profile, &active_profile.derived_apply_ns, derived_apply_start_ns);
                }
                if (opts.wait_for_sync_level) {
                    const sync_wait_start_ns = DB.WritePathCallbacks.monotonic_time_ns();
                    try DB.WritePathCallbacks.wait_for_sync_level(self, effective_req.sync_level, sequence, sync_targets);
                    if (profile) |active_profile| DB.WritePathCallbacks.record_profile_ns(profile, &active_profile.sync_wait_ns, sync_wait_start_ns);
                }
            }
            if (opts.wait_for_sync_level and effective_req.sync_level == .full_index and self.text_merge_runtime == null) {
                try self.drainScheduledTextMerges();
            }
            if (profile) |active_profile| DB.WritePathCallbacks.record_profile_ns(profile, &active_profile.wait_sync_ns, wait_sync_start_ns);
            if (!opts.suppress_derived_replay_append) {
                if (self.enrichment_runtime) |runtime| {
                    const notify_enrichment_start_ns = DB.WritePathCallbacks.monotonic_time_ns();
                    runtime.notifySequence(sequence);
                    if (profile) |active_profile| DB.WritePathCallbacks.record_profile_ns(profile, &active_profile.notify_enrichment_ns, notify_enrichment_start_ns);
                }
                DB.WritePathCallbacks.notify_resolver_replay_runtimes(self, sequence);
            }
        }

        pub fn batchWithoutRangeValidationAfterGate(self: *DB, req: types.BatchRequest) anyerror!void {
            try DB.WritePathCallbacks.batch_internal(self, req, null, .{
                .validate_range_ownership = false,
                .admission_prechecked = true,
            });
        }

        pub fn batchReplicatedApply(self: *DB, req: types.BatchRequest) anyerror!void {
            try batchReplicatedApplyWithMarker(self, req, null);
        }

        pub fn batchReplicatedApplyWithMarker(self: *DB, req: types.BatchRequest, applied_lsn_marker: ?u64) anyerror!void {
            var apply_req = req;
            apply_req.sync_level = .write;
            try DB.WritePathCallbacks.batch_internal(self, apply_req, null, .{
                .validate_range_ownership = false,
                .wait_for_sync_level = false,
                .bypass_ha_write_gate = true,
                .ha_applied_lsn_marker = applied_lsn_marker,
            });
        }

        pub fn beginBulkIngestSessionAfterGate(self: *DB) !void {
            self.async_context.text_merge_deferred.store(true, .release);
            errdefer self.async_context.text_merge_deferred.store(false, .release);
            db_internal.beginExternalDenseBulkSessionTracked(self.async_context);
            errdefer db_internal.finishExternalDenseBulkSessionTracked(self.async_context);
            self.core.lockApply();
            defer self.core.unlockApply();
            const resources = self.core.batchExecutionResources();
            try resources.store.beginBulkIngestSession();
            errdefer resources.store.abortBulkIngestSession();
            try resources.index_manager.beginDenseBulkIngestSessions();
            errdefer resources.index_manager.abortDenseBulkIngestSessions();
            try resources.index_manager.beginSparseBulkIngestSessions();
            errdefer resources.index_manager.abortSparseBulkIngestSessions();
            try resources.index_manager.beginAlgebraicBulkIngestSessions();
            configureBulkIngestIdentityAllNewLocked(self) catch |err| {
                resources.index_manager.abortAlgebraicBulkIngestSessions();
                return err;
            };
            errdefer clearBulkIngestIdentityAllNewLocked(self);
            self.bulk_ingest_coalescer.begin();
        }

        pub fn finishBulkIngestSessionWithOptionsAfterGate(self: *DB, options: backend_types.BulkIngestFinishOptions) !void {
            try flushBulkIngestCoalescerWithSyncLevel(self, .write, null);
            var external_session_tracked = true;
            defer if (external_session_tracked) db_internal.finishExternalDenseBulkSessionTracked(self.async_context);
            {
                self.core.lockApply();
                defer self.core.unlockApply();
                const resources = self.core.batchExecutionResources();
                var first_err: ?anyerror = null;
                // External dense indexes may need to reload vectors from the primary
                // store while deferred leaf splits are normalized at finish time. Make
                // the primary-store bulk-ingest state visible before the dense finish
                // path runs so those reloads cannot see a half-published store.
                resources.index_manager.finishAlgebraicBulkIngestSessionsWithOptions(resources.store, options) catch |err| {
                    first_err = err;
                };
                resources.store.finishBulkIngestSessionWithOptions(options) catch |err| {
                    if (first_err == null) first_err = err;
                };
                resources.index_manager.finishSparseBulkIngestSessionsWithOptions(options) catch |err| {
                    if (first_err == null) first_err = err;
                };
                resources.index_manager.finishDenseBulkIngestSessionsWithOptions(options) catch |err| {
                    if (first_err == null) first_err = err;
                };
                self.bulk_ingest_coalescer.clear(self.alloc);
                clearBulkIngestIdentityAllNewLocked(self);
                if (first_err) |err| return err;
            }
            db_internal.finishExternalDenseBulkSessionTracked(self.async_context);
            external_session_tracked = false;
            // External bulk finish is the user-visible publication boundary for the
            // staged batch. Publish the primary LSM session first so derived indexes
            // can read the coalesced documents, then force managed index catch-up
            // before returning.
            try self.waitForCurrentSyncLevel(.full_index);
            db_internal.flushDeferredExternalBulkExecutorNotificationOrTarget(
                self.async_context,
                self.executor,
                self.core.nextDerivedSequence(),
            );
            if (self.async_context.query_visibility_hook) |hook| hook.notify(.publish);
        }

        pub fn beginDenseAutoBulkIngestSessionAfterGate(self: *DB) !void {
            db_internal.beginExternalDenseBulkSessionTracked(self.async_context);
            errdefer db_internal.finishExternalDenseBulkSessionTracked(self.async_context);
            self.core.lockApply();
            defer self.core.unlockApply();
            const resources = self.core.batchExecutionResources();
            try resources.index_manager.beginDenseBulkIngestSessions();
            self.bulk_ingest_coalescer.begin();
        }

        pub fn beginPrimaryStoreAutoBulkIngestSessionAfterGate(self: *DB) !void {
            db_internal.beginExternalDenseBulkSessionTracked(self.async_context);
            errdefer db_internal.finishExternalDenseBulkSessionTracked(self.async_context);
            self.core.lockApply();
            defer self.core.unlockApply();
            const resources = self.core.batchExecutionResources();
            try resources.store.beginBulkIngestSession();
            errdefer resources.store.abortBulkIngestSession();
            try resources.index_manager.beginDenseBulkIngestSessions();
            try configureBulkIngestIdentityAllNewLocked(self);
            errdefer clearBulkIngestIdentityAllNewLocked(self);
            self.bulk_ingest_coalescer.begin();
        }

        pub fn finishPrimaryStoreAutoBulkIngestSessionWithOptionsAfterGate(self: *DB, options: backend_types.BulkIngestFinishOptions) !void {
            try flushBulkIngestCoalescerWithSyncLevel(self, .write, null);
            var external_session_tracked = true;
            defer if (external_session_tracked) db_internal.finishExternalDenseBulkSessionTracked(self.async_context);
            {
                self.core.lockApply();
                defer self.core.unlockApply();
                const resources = self.core.batchExecutionResources();
                var first_err: ?anyerror = null;
                resources.store.finishBulkIngestSessionWithOptions(options) catch |err| {
                    first_err = err;
                };
                resources.index_manager.finishDenseBulkIngestSessionsWithOptions(options) catch |err| {
                    if (first_err == null) first_err = err;
                };
                self.bulk_ingest_coalescer.clear(self.alloc);
                clearBulkIngestIdentityAllNewLocked(self);
                if (first_err) |err| return err;
            }
            db_internal.finishExternalDenseBulkSessionTracked(self.async_context);
            external_session_tracked = false;
            db_internal.flushDeferredExternalBulkExecutorNotificationOrTarget(
                self.async_context,
                self.executor,
                self.core.nextDerivedSequence(),
            );
            if (self.async_context.query_visibility_hook) |hook| hook.notify(.publish);
        }

        pub fn rollPrimaryStoreAutoBulkIngestSessionWithOptionsAfterGate(self: *DB, options: backend_types.BulkIngestFinishOptions) !void {
            try finishPrimaryStoreAutoBulkIngestSessionWithOptionsAfterGate(self, options);
            try beginPrimaryStoreAutoBulkIngestSessionAfterGate(self);
        }

        pub fn finishDenseAutoBulkIngestSessionWithOptionsAfterGate(self: *DB, options: backend_types.BulkIngestFinishOptions, notify_executor: bool) !void {
            try flushBulkIngestCoalescerWithSyncLevel(self, .write, null);
            var external_session_tracked = true;
            defer if (external_session_tracked) db_internal.finishExternalDenseBulkSessionTracked(self.async_context);
            {
                self.core.lockApply();
                defer self.core.unlockApply();
                const resources = self.core.batchExecutionResources();
                resources.store.flushBufferedWritesWithOptions(options) catch |err| {
                    self.bulk_ingest_coalescer.clear(self.alloc);
                    return err;
                };
                resources.index_manager.finishDenseBulkIngestSessionsWithOptions(options) catch |err| {
                    self.bulk_ingest_coalescer.clear(self.alloc);
                    clearBulkIngestIdentityAllNewLocked(self);
                    return err;
                };
                self.bulk_ingest_coalescer.clear(self.alloc);
                clearBulkIngestIdentityAllNewLocked(self);
            }
            db_internal.finishExternalDenseBulkSessionTracked(self.async_context);
            external_session_tracked = false;
            if (notify_executor) {
                db_internal.flushDeferredExternalBulkExecutorNotificationOrTarget(
                    self.async_context,
                    self.executor,
                    self.core.nextDerivedSequence(),
                );
            }
            if (self.async_context.query_visibility_hook) |hook| hook.notify(.publish);
        }

        pub fn rollDenseAutoBulkIngestSessionWithOptionsAfterGate(self: *DB, options: backend_types.BulkIngestFinishOptions) !void {
            try finishDenseAutoBulkIngestSessionWithOptionsAfterGate(self, options, true);
            try beginDenseAutoBulkIngestSessionAfterGate(self);
        }

        pub fn abortDenseAutoBulkIngestSession(self: *DB) void {
            self.core.lockApply();
            {
                defer self.core.unlockApply();
                const resources = self.core.batchExecutionResources();
                self.bulk_ingest_coalescer.clear(self.alloc);
                resources.index_manager.abortDenseBulkIngestSessions();
            }
            db_internal.finishExternalDenseBulkSessionTracked(self.async_context);
            db_internal.flushDeferredExternalBulkExecutorNotification(self.async_context, self.executor);
        }

        pub fn abortPrimaryStoreAutoBulkIngestSession(self: *DB) void {
            self.core.lockApply();
            {
                defer self.core.unlockApply();
                const resources = self.core.batchExecutionResources();
                self.bulk_ingest_coalescer.clear(self.alloc);
                clearBulkIngestIdentityAllNewLocked(self);
                resources.index_manager.abortDenseBulkIngestSessions();
                resources.store.abortBulkIngestSession();
            }
            db_internal.finishExternalDenseBulkSessionTracked(self.async_context);
            db_internal.flushDeferredExternalBulkExecutorNotification(self.async_context, self.executor);
        }

        pub fn abortBulkIngestSession(self: *DB) void {
            self.core.lockApply();
            {
                defer self.core.unlockApply();
                const resources = self.core.batchExecutionResources();
                self.bulk_ingest_coalescer.clear(self.alloc);
                clearBulkIngestIdentityAllNewLocked(self);
                resources.index_manager.abortAlgebraicBulkIngestSessions();
                resources.index_manager.abortDenseBulkIngestSessions();
                resources.store.abortBulkIngestSession();
            }
            db_internal.finishExternalDenseBulkSessionTracked(self.async_context);
            db_internal.flushDeferredExternalBulkExecutorNotification(self.async_context, self.executor);
        }

        fn validateCreateOnlyBatchWriteRequest(alloc: Allocator, req: types.BatchRequest) !void {
            if (req.write_mode != .create_only) return;
            if (req.transforms.len != 0 or req.relational_identity_rewrites.len != 0) return error.UnsupportedOperation;

            var seen = std.StringHashMapUnmanaged(void){};
            defer seen.deinit(alloc);
            for (req.writes) |write| {
                const gop = try seen.getOrPut(alloc, write.key);
                if (gop.found_existing) return error.Conflict;
            }
        }

        fn enforceCreateOnlyNoOverwrite(write_mode: types.BatchWriteMode, overwritten_flags: []const bool) !void {
            if (write_mode != .create_only) return;
            for (overwritten_flags) |overwritten| {
                if (overwritten) return error.Conflict;
            }
        }

        pub fn coalesceKeyValueRequest(
            self: *DB,
            comptime T: type,
            writes: []const T,
            deletes: []const []const u8,
            transforms: []const types.DocumentTransform,
        ) !CoalescedKeyValueRequest(T) {
            var result = CoalescedKeyValueRequest(T){};
            var order = std.ArrayListUnmanaged(CoalescedKeyValueRequest(T).Entry).empty;
            errdefer {
                result.entries = order.items;
                order.items = &.{};
                order.capacity = 0;
                result.deinit(self.alloc);
            }
            defer order.deinit(self.alloc);

            var positions = std.StringHashMapUnmanaged(usize){};
            defer positions.deinit(self.alloc);

            for (writes) |write| {
                const gop = try positions.getOrPut(self.alloc, write.key);
                if (!gop.found_existing) {
                    gop.value_ptr.* = order.items.len;
                    try order.append(self.alloc, .{
                        .key = write.key,
                        .value = write.value,
                        .kind = .write,
                    });
                    continue;
                }
                const entry = &order.items[gop.value_ptr.*];
                if (entry.owned_value) self.alloc.free(@constCast(entry.value.?));
                if (entry.owned_key) self.alloc.free(@constCast(entry.key));
                setCoalescedEntryToBorrowedWrite(T, entry, write);
            }

            for (deletes) |key| {
                const gop = try positions.getOrPut(self.alloc, key);
                if (!gop.found_existing) {
                    gop.value_ptr.* = order.items.len;
                    try order.append(self.alloc, .{
                        .key = key,
                        .kind = .delete,
                    });
                    continue;
                }
                const entry = &order.items[gop.value_ptr.*];
                if (entry.owned_value) self.alloc.free(@constCast(entry.value.?));
                entry.owned_value = false;
                resetCoalescedEntryToDelete(T, entry);
            }

            for (transforms) |transform| {
                const maybe_index = positions.get(transform.key);
                const base_json = blk: {
                    if (maybe_index) |entry_index| {
                        const entry = order.items[entry_index];
                        break :blk if (entry.kind == .write) entry.value.? else null;
                    }
                    break :blk try self.get(self.alloc, transform.key);
                };
                defer if (maybe_index == null) {
                    if (base_json) |body| self.alloc.free(body);
                };

                const resolved = try transform_mod.resolveDocumentTransform(self.alloc, base_json, transform) orelse {
                    if (maybe_index == null) continue;
                    const entry = order.items[maybe_index.?];
                    if (entry.kind == .delete) continue;
                    continue;
                };
                errdefer self.alloc.free(resolved);

                const gop = try positions.getOrPut(self.alloc, transform.key);
                if (!gop.found_existing) {
                    gop.value_ptr.* = order.items.len;
                    try order.append(self.alloc, .{
                        .key = try self.alloc.dupe(u8, transform.key),
                        .value = resolved,
                        .kind = .write,
                        .owned_key = true,
                        .owned_value = true,
                    });
                    continue;
                }

                const entry = &order.items[gop.value_ptr.*];
                if (entry.owned_value) self.alloc.free(@constCast(entry.value.?));
                try setCoalescedEntryToOwnedWrite(T, self.alloc, entry, transform.key, resolved);
            }

            var write_count: usize = 0;
            var delete_count: usize = 0;
            for (order.items) |entry| {
                switch (entry.kind) {
                    .write => write_count += 1,
                    .delete => delete_count += 1,
                }
            }

            const final_entries = try order.toOwnedSlice(self.alloc);
            result.entries = final_entries;
            if (write_count > 0) result.writes = try self.alloc.alloc(T, write_count);
            if (delete_count > 0) result.deletes = try self.alloc.alloc([]const u8, delete_count);

            var write_index: usize = 0;
            var delete_index: usize = 0;
            for (final_entries) |entry| {
                switch (entry.kind) {
                    .write => {
                        result.writes[write_index] = .{
                            .key = entry.key,
                            .value = entry.value.?,
                        };
                        write_index += 1;
                    },
                    .delete => {
                        result.deletes[delete_index] = entry.key;
                        delete_index += 1;
                    },
                }
            }
            return result;
        }

        pub fn configureBulkIngestIdentityAllNewLocked(self: *DB) !void {
            clearBulkIngestIdentityAllNewLocked(self);
            if (!try primaryUserNamespaceIsEmptyLocked(self)) return;
            if (try doc_identity.loadAllNewTrustedStateForNamespace(self.core.store, self.core.identity_namespace)) |state| {
                self.bulk_ingest_identity_state = state;
                self.identity_visibility_summary_cache = state.visibility_summary;
                self.bulk_ingest_identity_all_new = true;
            }
        }

        pub fn clearBulkIngestIdentityAllNewLocked(self: *DB) void {
            self.bulk_ingest_identity_all_new = false;
            self.bulk_ingest_identity_state = .{};
            clearBulkIngestSeenDocKeysLocked(self);
        }

        pub fn clearBulkIngestSeenDocKeysLocked(self: *DB) void {
            var it = self.bulk_ingest_seen_doc_keys.keyIterator();
            while (it.next()) |key_ptr| self.alloc.free(@constCast(key_ptr.*));
            self.bulk_ingest_seen_doc_keys.clearRetainingCapacity();
        }

        pub fn deinitBulkIngestCoalescer(self: *DB) void {
            self.bulk_ingest_coalescer.deinit(self.alloc);
        }

        fn failIfIdentityOrdinalExhaustedForNewUpserts(self: *DB, doc_ids: []const []const u8) !void {
            if (doc_ids.len == 0) return;

            var txn = try self.core.store.beginProbeTxn();
            defer txn.abort();

            const raw_next = txn.get(internal_keys.identity_next_ordinal_key[0..]) catch |err| switch (err) {
                error.NotFound => return,
                else => return err,
            };
            if (raw_next.len != @sizeOf(doc_identity.DocOrdinal)) return error.InvalidDocIdentity;
            const next_ordinal = std.mem.readInt(doc_identity.DocOrdinal, raw_next[0..4], .big);
            if (next_ordinal != 0 and next_ordinal < std.math.maxInt(doc_identity.DocOrdinal)) return;

            var seen = std.StringHashMapUnmanaged(void).empty;
            defer seen.deinit(self.alloc);
            for (doc_ids) |doc_id| {
                if (seen.contains(doc_id)) continue;
                try seen.put(self.alloc, doc_id, {});
                if (try doc_identity.lookupOrdinalTxn(self.alloc, &txn, doc_id) != null) continue;
                return error.DocOrdinalExhausted;
            }
        }

        fn rememberBulkIngestAllNewIdentityUpserts(self: *DB, doc_ids: []const []const u8) !bool {
            var batch_seen = std.StringHashMapUnmanaged(void).empty;
            defer batch_seen.deinit(self.alloc);
            for (doc_ids) |doc_id| {
                if (self.bulk_ingest_seen_doc_keys.contains(doc_id)) return false;
                if (batch_seen.contains(doc_id)) return false;
                try batch_seen.put(self.alloc, doc_id, {});
            }

            for (doc_ids) |doc_id| {
                const owned = try self.alloc.dupe(u8, doc_id);
                errdefer self.alloc.free(owned);
                try self.bulk_ingest_seen_doc_keys.put(self.alloc, owned, {});
            }
            return true;
        }

        fn identityUpsertStoreWritesAreNew(write_indexes: []const usize, overwritten_flags: []const bool) bool {
            for (write_indexes) |write_index| {
                if (write_index >= overwritten_flags.len) return false;
                if (overwritten_flags[write_index]) return false;
            }
            return true;
        }

        pub fn flushBulkIngestCoalescerWithSyncLevel(
            self: *DB,
            sync_level: types.SyncLevel,
            profile: ?*DB.WritePathCallbacks.Profile,
        ) anyerror!void {
            if (!self.bulk_ingest_coalescer.active or !self.bulk_ingest_coalescer.hasPending()) return;
            _ = self.bulk_ingest_coalescer.stats.flush_calls.fetchAdd(1, .monotonic);
            _ = self.bulk_ingest_coalescer.stats.flushed_keys.fetchAdd(@intCast(self.bulk_ingest_coalescer.entries.items.len), .monotonic);

            self.core.lockApply();
            var view = try self.bulk_ingest_coalescer.snapshotRequestView(self.alloc);
            self.core.unlockApply();
            defer view.deinit(self.alloc);

            self.flushing_bulk_ingest_coalescer = true;
            defer self.flushing_bulk_ingest_coalescer = false;

            try DB.WritePathCallbacks.batch_internal(self, .{
                .writes = view.writes,
                .deletes = view.deletes,
                .sync_level = sync_level,
            }, profile, .{
                .store_batch_options = .{ .mode = .bulk_ingest },
                .admission_prechecked = true,
            });

            self.core.lockApply();
            defer self.core.unlockApply();
            self.bulk_ingest_coalescer.resetPending(self.alloc);
            self.bulk_ingest_coalescer.active = true;
        }

        fn primaryUserNamespaceIsEmptyLocked(self: *DB) !bool {
            var txn = try self.core.store.beginCurrentScanTxn();
            defer txn.abort();
            var cur = try txn.openCursor();
            defer cur.close();

            const lower = [_]u8{internal_keys.user_namespace};
            const first = (try cur.seekAtOrAfter(lower[0..])) orelse return true;
            return first.key.len == 0 or first.key[0] != internal_keys.user_namespace;
        }

        fn deleteDocumentArtifactChildRangeOutboxEntryAfterGate(self: *DB, key: []const u8) !void {
            self.core.lockApply();
            defer self.core.unlockApply();
            const deletes = [_][]const u8{key};
            try self.core.store.putBatch(&.{}, deletes[0..]);
        }

        fn collectDocumentChildRangeRoutingSnapshots(
            self: *DB,
            generated: PrecomputedGeneratedBatch,
            out: *std.ArrayListUnmanaged(DocumentChildRangeRoutingSnapshot),
        ) !void {
            for (generated.artifact_writes) |write| try appendDocumentChildRangeRoutingSnapshotFromValue(self, write.key, write.value, out);
            for (generated.artifact_delete_keys) |key| {
                var artifact_ref = (try db_internal.decodeArtifactRefIfKnownAlloc(self.alloc, key)) orelse continue;
                defer artifact_ref.deinit(self.alloc);
                if (artifact_ref.kind != .asset or artifact_ref.unit_id != null) continue;
                const existing = self.core.getStoreValue(self.alloc, key) catch |err| switch (err) {
                    error.NotFound => continue,
                    else => return err,
                };
                defer if (existing) |value| self.alloc.free(value);
                if (existing) |value| try appendDocumentChildRangeRoutingSnapshotFromValue(self, key, value, out);
            }
        }

        fn appendDocumentChildRangeRoutingSnapshotFromValue(
            self: *DB,
            key: []const u8,
            value: []const u8,
            out: *std.ArrayListUnmanaged(DocumentChildRangeRoutingSnapshot),
        ) !void {
            if (std.mem.indexOf(u8, value, "\"child_ranges\"") == null) return;
            var artifact_ref = (try db_internal.decodeArtifactRefIfKnownAlloc(self.alloc, key)) orelse return;
            defer artifact_ref.deinit(self.alloc);
            if (artifact_ref.kind != .asset or artifact_ref.unit_id != null) return;
            const ranges = documentArtifactChildRangesFromManifestJsonAlloc(self.alloc, value) catch |err| switch (err) {
                error.InvalidDocumentExtractionManifest => return,
                else => return err,
            };
            errdefer freeDocumentArtifactChildRanges(self.alloc, ranges);
            if (ranges.len == 0) {
                freeDocumentArtifactChildRanges(self.alloc, ranges);
                return;
            }
            const doc_key = try self.alloc.dupe(u8, artifact_ref.document_id);
            errdefer self.alloc.free(doc_key);
            const manifest_artifact_name = try self.alloc.dupe(u8, artifact_ref.name);
            errdefer self.alloc.free(manifest_artifact_name);
            try out.append(self.alloc, .{ .doc_key = doc_key, .manifest_artifact_name = manifest_artifact_name, .child_ranges = ranges });
        }

        fn resetCoalescedEntryToDelete(comptime T: type, entry: *CoalescedKeyValueRequest(T).Entry) void {
            if (entry.owned_value) {
                // Caller frees previous owned value before switching the entry.
                entry.owned_value = false;
            }
            entry.value = null;
            entry.kind = .delete;
        }

        fn setCoalescedEntryToBorrowedWrite(comptime T: type, entry: *CoalescedKeyValueRequest(T).Entry, write: T) void {
            entry.key = write.key;
            entry.value = write.value;
            entry.kind = .write;
            entry.owned_key = false;
            entry.owned_value = false;
        }

        fn setCoalescedEntryToOwnedWrite(
            comptime T: type,
            alloc: Allocator,
            entry: *CoalescedKeyValueRequest(T).Entry,
            key: []const u8,
            value: []u8,
        ) !void {
            if (!entry.owned_key) {
                entry.key = try alloc.dupe(u8, key);
                entry.owned_key = true;
            }
            entry.value = value;
            entry.kind = .write;
            entry.owned_value = true;
        }
    };
}
