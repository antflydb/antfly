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
const platform = @import("antfly_platform");

const artifact_ids = @import("artifact_ids.zig");
const apply_state = @import("derived/apply_state.zig");
const sparse_mod = if (builtin.os.tag == .freestanding)
    @import("sparse_stub.zig")
else
    @import("../../sparse/sparse.zig");
const relational_store_mod = @import("relational_store.zig");
const artifact_replay = @import("artifact_replay.zig");
const backfill_state_mod = @import("backfill_state.zig");
const backend_types = @import("../backend_types.zig");
const change_journal_mod = @import("derived/change_journal.zig");
const db_internal = @import("internal.zig");
const derived_types = @import("derived/derived_types.zig");
const derived_executor_mod = @import("derived/derived_executor.zig");
const derived_worker = @import("derived/derived_worker.zig");
const doc_identity = @import("doc_identity.zig");
const docstore_mod = @import("../docstore.zig");
const enrichment_artifact_codec = @import("enrichment/artifact_codec.zig");
const embedder_mod = @import("enrichment/embedder.zig");
const enrichment_runtime_mod = @import("enrichment/enrichment_runtime.zig");
const enrichment_state = @import("enrichment/enrichment_state.zig");
const enrichment_worker = @import("enrichment/enrichment_worker.zig");
const ha_effects_mod = @import("../ha/effects.zig");
const ha_replication_record_mod = @import("../ha/replication_record.zig");
const hbc_mod = @import("../hbc_adapter.zig");
const index_manager_mod = @import("catalog/index_manager.zig");
const internal_keys = @import("../internal_keys.zig");
const mapper = @import("document_mapper.zig");
const mem_backend_mod = @import("../mem_backend.zig");
const promotion_runtime_mod = @import("promotion_runtime.zig");
const replay_stream_mod = @import("derived/replay_stream.zig");
const resource_manager_mod = @import("../resource_manager.zig");
const resolution_runtime_mod = @import("resolution_runtime.zig");
const schema_mod = @import("../schema.zig");
const types = @import("types.zig");

const Allocator = std.mem.Allocator;
const AtomicU64 = platform.atomic.Value(u64);

fn replayCollectorTimeNs() u64 {
    return platform.time.monotonicNs();
}

const profileDelta = db_internal.profileDelta;
const readEnvUsize = db_internal.readEnvUsize;
const readEnvU64 = db_internal.readEnvU64;

const TestHelpers = if (builtin.is_test) @import("test_support.zig") else struct {};

pub fn freeOwnedKeySlice(alloc: Allocator, keys: [][]u8) void {
    for (keys) |key| alloc.free(key);
    alloc.free(keys);
}

fn nsToMs(ns: u64) u64 {
    return ns / std.time.ns_per_ms;
}

pub fn logReplayCatchUpProfile(index_ref: index_manager_mod.ManagedIndexRef, applied_sequence: u64, stats: derived_worker.CatchUpStats) void {
    std.log.info(
        "db_replay_catch_up_profile index={s} kind={s} applied_sequence={} last_sequence={} scanned_entries={} applied_entries={} window_collect_ns={} apply_ns={}",
        .{
            index_ref.name,
            @tagName(index_ref.kind),
            applied_sequence,
            stats.last_sequence,
            stats.scanned_entries,
            stats.applied_entries,
            stats.window_collect_ns,
            stats.apply_ns,
        },
    );
}

pub fn logDerivedWorkerProfile(index_ref: index_manager_mod.ManagedIndexRef, batch: derived_types.DerivedBatch, profile: anytype) void {
    std.log.info(
        "antfly_bench_derived_worker index={s} kind={s} sequence={d} documents={d} deletes={d} overwritten={d} dense_embeddings={d} sparse_embeddings={d} graph_writes={d} graph_deletes={d} total_ms={d} full_text_apply_ms={d} dense_apply_ms={d} dense_delete_ms={d} dense_doc_index_ms={d} dense_embedding_apply_ms={d} sparse_apply_ms={d} graph_apply_ms={d} index_sync_ms={d}",
        .{
            index_ref.name,
            @tagName(index_ref.kind),
            batch.sequence,
            batch.documents.len,
            batch.deleted_keys.len,
            batch.overwritten_doc_keys.len,
            batch.dense_embeddings.len,
            batch.sparse_embeddings.len,
            batch.graph_writes.len,
            batch.graph_deletes.len,
            nsToMs(profile.total_ns),
            nsToMs(profile.full_text_apply_ns),
            nsToMs(profile.dense_apply_ns),
            nsToMs(profile.dense_delete_ns),
            nsToMs(profile.dense_doc_index_ns),
            nsToMs(profile.dense_embedding_apply_ns),
            nsToMs(profile.sparse_apply_ns),
            nsToMs(profile.graph_apply_ns),
            nsToMs(profile.index_sync_ns),
        },
    );
    std.log.info(
        "antfly_bench_derived_worker_hbc index={s} sequence={d} insert_calls={d} batch_route_calls={d} batch_route_internal_nodes={d} batch_route_leaf_groups={d} batch_route_items={d} batch_route_quantized_nodes={d} batch_route_exact_child_scores={d} batch_route_fallback_nodes={d} grouped_items={d} grouped_fallback_items={d} leaf_groups={d} split_candidates={d} recursive_splits={d} split_scan_iterations={d} split_queue_peak_total={d} split_input_members_total={d} split_input_overflow_members_total={d} leaf_range_writes={d} ancestor_range_refreshes={d} ancestor_range_nodes={d} node_body_writes={d} vec_leaf_writes={d} save_node_calls={d} split_leaf_calls={d} split_internal_calls={d} range_put_calls={d} range_delete_calls={d}",
        .{
            index_ref.name,
            batch.sequence,
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
        "antfly_bench_derived_worker_hbc_storage index={s} sequence={d} nodes_put_calls={d} nodes_append_calls={d} nodes_delete_calls={d} meta_put_calls={d} meta_append_calls={d} meta_delete_calls={d} quant_put_calls={d} quant_append_calls={d} quant_delete_calls={d} vecs_put_calls={d} vecs_append_calls={d} vecs_delete_calls={d}",
        .{
            index_ref.name,
            batch.sequence,
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
        "antfly_bench_derived_worker_hbc_timing index={s} sequence={d} insert_transform_ms={d} insert_store_vector_ms={d} insert_find_leaf_ms={d} insert_mutate_leaf_ms={d} insert_flush_metadata_ms={d} insert_commit_ms={d} save_node_ms={d} save_split_range_ms={d} update_parent_ms={d} split_leaf_ms={d} split_leaf_vector_load_ms={d} split_leaf_partition_ms={d} split_leaf_finalize_ms={d} split_internal_ms={d} refresh_quantized_ms={d} quantized_vector_load_ms={d} quantized_compute_ms={d} quantized_store_ms={d} quantized_encode_ms={d} quantized_put_ms={d} bulk_build_store_ms={d} bulk_build_tree_ms={d}",
        .{
            index_ref.name,
            batch.sequence,
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
}

fn addHbcWriteProfileDelta(total: anytype, before: hbc_mod.WriteProfile, after: hbc_mod.WriteProfile) void {
    total.hbc_insert_calls += profileDelta(after.insert_calls, before.insert_calls);
    total.hbc_batch_route_calls += profileDelta(after.batch_route_calls, before.batch_route_calls);
    total.hbc_batch_route_internal_nodes += profileDelta(after.batch_route_internal_nodes, before.batch_route_internal_nodes);
    total.hbc_batch_route_leaf_groups += profileDelta(after.batch_route_leaf_groups, before.batch_route_leaf_groups);
    total.hbc_batch_route_items += profileDelta(after.batch_route_items, before.batch_route_items);
    total.hbc_batch_route_quantized_nodes += profileDelta(after.batch_route_quantized_nodes, before.batch_route_quantized_nodes);
    total.hbc_batch_route_exact_child_scores += profileDelta(after.batch_route_exact_child_scores, before.batch_route_exact_child_scores);
    total.hbc_batch_route_fallback_nodes += profileDelta(after.batch_route_fallback_nodes, before.batch_route_fallback_nodes);
    total.hbc_grouped_items += profileDelta(after.grouped_items, before.grouped_items);
    total.hbc_grouped_fallback_items += profileDelta(after.grouped_fallback_items, before.grouped_fallback_items);
    total.hbc_grouped_leaf_groups += profileDelta(after.grouped_leaf_groups, before.grouped_leaf_groups);
    total.hbc_grouped_split_candidates += profileDelta(after.grouped_split_candidates, before.grouped_split_candidates);
    total.hbc_grouped_recursive_splits += profileDelta(after.grouped_recursive_splits, before.grouped_recursive_splits);
    total.hbc_grouped_split_scan_iterations += profileDelta(after.grouped_split_scan_iterations, before.grouped_split_scan_iterations);
    total.hbc_grouped_split_queue_peak_total += profileDelta(after.grouped_split_queue_peak_total, before.grouped_split_queue_peak_total);
    total.hbc_grouped_leaf_range_writes += profileDelta(after.grouped_leaf_range_writes, before.grouped_leaf_range_writes);
    total.hbc_grouped_ancestor_range_refreshes += profileDelta(after.grouped_ancestor_range_refreshes, before.grouped_ancestor_range_refreshes);
    total.hbc_grouped_ancestor_range_nodes += profileDelta(after.grouped_ancestor_range_nodes, before.grouped_ancestor_range_nodes);
    total.hbc_grouped_node_body_writes += profileDelta(after.grouped_node_body_writes, before.grouped_node_body_writes);
    total.hbc_grouped_vec_leaf_writes += profileDelta(after.grouped_vec_leaf_writes, before.grouped_vec_leaf_writes);
    total.hbc_split_leaf_input_members_total += profileDelta(after.split_leaf_input_members_total, before.split_leaf_input_members_total);
    total.hbc_split_leaf_input_overflow_members_total += profileDelta(after.split_leaf_input_overflow_members_total, before.split_leaf_input_overflow_members_total);
    total.hbc_save_node_calls += profileDelta(after.save_node_calls, before.save_node_calls);
    total.hbc_split_leaf_calls += profileDelta(after.split_leaf_calls, before.split_leaf_calls);
    total.hbc_split_internal_calls += profileDelta(after.split_internal_calls, before.split_internal_calls);
    total.hbc_range_put_calls += profileDelta(after.range_put_calls, before.range_put_calls);
    total.hbc_range_delete_calls += profileDelta(after.range_delete_calls, before.range_delete_calls);
    total.hbc_nodes_put_calls += profileDelta(after.ns_nodes_put_calls, before.ns_nodes_put_calls);
    total.hbc_nodes_append_calls += profileDelta(after.ns_nodes_append_calls, before.ns_nodes_append_calls);
    total.hbc_nodes_delete_calls += profileDelta(after.ns_nodes_delete_calls, before.ns_nodes_delete_calls);
    total.hbc_meta_put_calls += profileDelta(after.ns_meta_put_calls, before.ns_meta_put_calls);
    total.hbc_meta_append_calls += profileDelta(after.ns_meta_append_calls, before.ns_meta_append_calls);
    total.hbc_meta_delete_calls += profileDelta(after.ns_meta_delete_calls, before.ns_meta_delete_calls);
    total.hbc_quant_put_calls += profileDelta(after.ns_quant_put_calls, before.ns_quant_put_calls);
    total.hbc_quant_append_calls += profileDelta(after.ns_quant_append_calls, before.ns_quant_append_calls);
    total.hbc_quant_delete_calls += profileDelta(after.ns_quant_delete_calls, before.ns_quant_delete_calls);
    total.hbc_vecs_put_calls += profileDelta(after.ns_vecs_put_calls, before.ns_vecs_put_calls);
    total.hbc_vecs_append_calls += profileDelta(after.ns_vecs_append_calls, before.ns_vecs_append_calls);
    total.hbc_vecs_delete_calls += profileDelta(after.ns_vecs_delete_calls, before.ns_vecs_delete_calls);
    total.hbc_insert_transform_ns += profileDelta(after.insert_transform_ns, before.insert_transform_ns);
    total.hbc_insert_store_vector_ns += profileDelta(after.insert_store_vector_ns, before.insert_store_vector_ns);
    total.hbc_insert_find_leaf_ns += profileDelta(after.insert_find_leaf_ns, before.insert_find_leaf_ns);
    total.hbc_insert_mutate_leaf_ns += profileDelta(after.insert_mutate_leaf_ns, before.insert_mutate_leaf_ns);
    total.hbc_insert_flush_metadata_ns += profileDelta(after.insert_flush_metadata_ns, before.insert_flush_metadata_ns);
    total.hbc_insert_commit_ns += profileDelta(after.insert_commit_ns, before.insert_commit_ns);
    total.hbc_save_node_ns += profileDelta(after.save_node_ns, before.save_node_ns);
    total.hbc_save_split_range_ns += profileDelta(after.save_split_range_ns, before.save_split_range_ns);
    total.hbc_update_parent_ns += profileDelta(after.update_parent_ns, before.update_parent_ns);
    total.hbc_split_leaf_ns += profileDelta(after.split_leaf_ns, before.split_leaf_ns);
    total.hbc_split_leaf_vector_load_ns += profileDelta(after.split_leaf_vector_load_ns, before.split_leaf_vector_load_ns);
    total.hbc_split_leaf_partition_ns += profileDelta(after.split_leaf_partition_ns, before.split_leaf_partition_ns);
    total.hbc_split_leaf_finalize_ns += profileDelta(after.split_leaf_finalize_ns, before.split_leaf_finalize_ns);
    total.hbc_split_internal_ns += profileDelta(after.split_internal_ns, before.split_internal_ns);
    total.hbc_refresh_quantized_ns += profileDelta(after.refresh_quantized_ns, before.refresh_quantized_ns);
    total.hbc_quantized_vector_load_ns += profileDelta(after.quantized_vector_load_ns, before.quantized_vector_load_ns);
    total.hbc_quantized_compute_ns += profileDelta(after.quantized_compute_ns, before.quantized_compute_ns);
    total.hbc_quantized_store_ns += profileDelta(after.quantized_store_ns, before.quantized_store_ns);
    total.hbc_quantized_encode_ns += profileDelta(after.quantized_encode_ns, before.quantized_encode_ns);
    total.hbc_quantized_put_ns += profileDelta(after.quantized_put_ns, before.quantized_put_ns);
    total.hbc_bulk_build_store_ns += profileDelta(after.bulk_build_store_ns, before.bulk_build_store_ns);
    total.hbc_bulk_build_tree_ns += profileDelta(after.bulk_build_tree_ns, before.bulk_build_tree_ns);
    total.hbc_posting_maintenance_scanned_nodes += profileDelta(after.posting_maintenance_scanned_nodes, before.posting_maintenance_scanned_nodes);
    total.hbc_posting_maintenance_scanned_postings += profileDelta(after.posting_maintenance_scanned_postings, before.posting_maintenance_scanned_postings);
    total.hbc_posting_maintenance_dirty_postings += profileDelta(after.posting_maintenance_dirty_postings, before.posting_maintenance_dirty_postings);
    total.hbc_posting_maintenance_repaired_postings += profileDelta(after.posting_maintenance_repaired_postings, before.posting_maintenance_repaired_postings);
    total.hbc_posting_maintenance_centroid_refreshed += profileDelta(after.posting_maintenance_centroid_refreshed, before.posting_maintenance_centroid_refreshed);
    total.hbc_posting_maintenance_payload_refreshed += profileDelta(after.posting_maintenance_payload_refreshed, before.posting_maintenance_payload_refreshed);
    total.hbc_posting_maintenance_ancestor_refresh_roots += profileDelta(after.posting_maintenance_ancestor_refresh_roots, before.posting_maintenance_ancestor_refresh_roots);
    total.hbc_posting_maintenance_split_postings += profileDelta(after.posting_maintenance_split_postings, before.posting_maintenance_split_postings);
    total.hbc_posting_maintenance_merged_postings += profileDelta(after.posting_maintenance_merged_postings, before.posting_maintenance_merged_postings);
    total.hbc_posting_maintenance_boundary_reassigned_vectors += profileDelta(after.posting_maintenance_boundary_reassigned_vectors, before.posting_maintenance_boundary_reassigned_vectors);
    total.hbc_posting_lazy_centroid_deferrals += profileDelta(after.posting_lazy_centroid_deferrals, before.posting_lazy_centroid_deferrals);
    total.hbc_posting_lazy_payload_deferrals += profileDelta(after.posting_lazy_payload_deferrals, before.posting_lazy_payload_deferrals);
    total.hbc_posting_lazy_ancestor_deferrals += profileDelta(after.posting_lazy_ancestor_deferrals, before.posting_lazy_ancestor_deferrals);
}

fn logSparseWriteProfileDelta(index_name: []const u8, delta: sparse_mod.WriteProfile) void {
    std.log.info(
        "antfly_bench_sparse_write_profile index={s} reserve_ms={d} dedupe_ms={d} existence_ms={d} doc_num_ms={d} fwd_rev_put_ms={d} posting_collect_ms={d} posting_sort_ms={d} posting_write_ms={d} chunk_read_ms={d} chunk_encode_ms={d} chunk_put_ms={d} range_meta_encode_ms={d} range_meta_put_ms={d} term_meta_ms={d} commit_ms={d} incremental_delete_ms={d} incremental_insert_ms={d} incremental_refresh_ms={d} incremental_commit_ms={d} writes={d} deletes={d} postings={d} terms={d}",
        .{
            index_name,
            nsToMs(delta.reserve_ns),
            nsToMs(delta.dedupe_ns),
            nsToMs(delta.existence_check_ns),
            nsToMs(delta.doc_num_ns),
            nsToMs(delta.fwd_rev_put_ns),
            nsToMs(delta.posting_collect_ns),
            nsToMs(delta.posting_sort_ns),
            nsToMs(delta.posting_write_ns),
            nsToMs(delta.chunk_read_ns),
            nsToMs(delta.chunk_encode_ns),
            nsToMs(delta.chunk_put_ns),
            nsToMs(delta.range_meta_encode_ns),
            nsToMs(delta.range_meta_put_ns),
            nsToMs(delta.term_meta_ns),
            nsToMs(delta.commit_ns),
            nsToMs(delta.incremental_delete_ns),
            nsToMs(delta.incremental_insert_ns),
            nsToMs(delta.incremental_refresh_ns),
            nsToMs(delta.incremental_commit_ns),
            delta.writes,
            delta.deletes,
            delta.postings,
            delta.terms,
        },
    );
}

pub const OwnedBatchWrites = struct {
    alloc: Allocator,
    items: []types.BatchWrite = &.{},
    missing_required: usize = 0,

    pub fn deinit(self: *@This()) void {
        for (self.items) |item| self.alloc.free(@constCast(item.value));
        if (self.items.len > 0) self.alloc.free(self.items);
        self.* = undefined;
    }
};

pub const CollectDocumentWritesProfile = struct {
    scan_ns: u64 = 0,
    sort_ns: u64 = 0,
    read_ns: u64 = 0,
    materialize_ns: u64 = 0,
    input_documents: usize = 0,
    pending_documents: usize = 0,
    output_writes: usize = 0,
    missing_required: usize = 0,
    inline_hits: usize = 0,
    store_hits: usize = 0,
};

pub const CollectSparseFieldWritesProfile = struct {
    scan_ns: u64 = 0,
    sort_ns: u64 = 0,
    read_ns: u64 = 0,
    extract_ns: u64 = 0,
    input_documents: usize = 0,
    pending_documents: usize = 0,
    output_writes: usize = 0,
    missing_required: usize = 0,
    inline_hits: usize = 0,
    store_hits: usize = 0,
    skipped_without_vector: usize = 0,
};

pub const OwnedDenseEmbeddingWrites = artifact_replay.OwnedDenseEmbeddingWrites;
pub const OwnedSparseEmbeddingWrites = artifact_replay.OwnedSparseEmbeddingWrites;

pub const CollectTextDocumentWritesOptions = struct {
    prefer_inline_when_store_tip_matches_sequence: ?u64 = null,
    relational_base_rows: bool = false,
};

pub const CollectDocumentWritesOptions = struct {
    prefer_inline_when_store_tip_matches_sequence: ?u64 = null,
    prefer_available_inline_values: bool = false,
    skip_doc_keys: ?*const std.StringHashMapUnmanaged(void) = null,
    relational_base_rows: bool = false,
};

pub fn replayDocumentStoreKeyAlloc(alloc: Allocator, key: []const u8, relational_base_rows: bool) ![]u8 {
    return if (internal_keys.isInternalUserKey(key))
        try alloc.dupe(u8, key)
    else if (relational_base_rows)
        try relational_store_mod.rowKeyAlloc(alloc, key)
    else
        try internal_keys.documentKeyAlloc(alloc, key);
}

fn materializeReplayDocumentValueAlloc(alloc: Allocator, value: []const u8, relational_base_rows: bool) ![]u8 {
    return if (relational_base_rows)
        try mapper.materializeRelationalRowValueAlloc(alloc, value)
    else
        try mapper.materializeDocumentValueAlloc(alloc, value);
}

pub const OwnedSparseFieldWrites = struct {
    alloc: Allocator,
    items: []sparse_mod.SparseWrite = &.{},
    missing_required: usize = 0,

    pub fn deinit(self: *@This()) void {
        for (self.items) |item| {
            self.alloc.free(@constCast(item.vec.indices));
            self.alloc.free(@constCast(item.vec.values));
        }
        if (self.items.len > 0) self.alloc.free(self.items);
        self.* = undefined;
    }
};

pub fn collectSparseFieldWritesProfiled(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    documents: []const derived_types.DerivedDocument,
    byte_range: types.ByteRange,
    field_name: []const u8,
    opts: CollectDocumentWritesOptions,
    profile: ?*CollectSparseFieldWritesProfile,
) !OwnedSparseFieldWrites {
    const PendingDocumentWrite = struct {
        doc_key: []const u8,
        store_key: []u8,
        inline_value: ?[]const u8,
    };

    var pending = std.ArrayListUnmanaged(PendingDocumentWrite).empty;
    defer {
        for (pending.items) |item| alloc.free(item.store_key);
        pending.deinit(alloc);
    }

    var writes = std.ArrayListUnmanaged(sparse_mod.SparseWrite).empty;
    errdefer {
        for (writes.items) |item| {
            alloc.free(@constCast(item.vec.indices));
            alloc.free(@constCast(item.vec.values));
        }
        writes.deinit(alloc);
    }

    var txn = try store.beginProbeTxn();
    defer txn.abort();
    var missing_required: usize = 0;
    const trust_inline = opts.prefer_available_inline_values or
        if (opts.prefer_inline_when_store_tip_matches_sequence) |sequence|
            store.nextReplaySequence(sequence + 1) == sequence + 1
        else
            false;

    if (profile) |p| p.input_documents = documents.len;
    const scan_start_ns = if (profile != null) replayCollectorTimeNs() else 0;
    for (documents) |doc| {
        if (doc.action != .upsert) continue;
        if (!byte_range.contains(doc.key)) continue;
        if (opts.skip_doc_keys) |skip_doc_keys| {
            if (skip_doc_keys.contains(doc.key)) continue;
        }
        if (trust_inline and doc.cleaned_value != null) {
            const extract_start_ns = if (profile != null) replayCollectorTimeNs() else 0;
            if (try mapper.extractSparseVectorField(alloc, doc.cleaned_value.?, field_name)) |raw_sparse_vec| {
                var sparse_vec = raw_sparse_vec;
                writes.append(alloc, .{
                    .doc_id = doc.key,
                    .vec = .{
                        .indices = sparse_vec.indices,
                        .values = sparse_vec.values,
                    },
                }) catch |err| {
                    sparse_vec.deinit(alloc);
                    return err;
                };
                if (profile) |p| p.output_writes += 1;
            } else if (profile) |p| {
                p.skipped_without_vector += 1;
            }
            if (profile) |p| {
                p.extract_ns += replayCollectorTimeNs() - extract_start_ns;
                p.inline_hits += 1;
            }
            continue;
        }
        try pending.append(alloc, .{
            .doc_key = doc.key,
            .store_key = try replayDocumentStoreKeyAlloc(alloc, doc.key, opts.relational_base_rows),
            .inline_value = doc.cleaned_value,
        });
    }
    if (profile) |p| {
        p.scan_ns = replayCollectorTimeNs() - scan_start_ns -| p.extract_ns;
        p.pending_documents = pending.items.len;
    }

    if (pending.items.len == 0) {
        return .{
            .alloc = alloc,
            .items = try writes.toOwnedSlice(alloc),
            .missing_required = missing_required,
        };
    }

    const SortContext = struct {};
    const sort_start_ns = if (profile != null) replayCollectorTimeNs() else 0;
    std.mem.sort(PendingDocumentWrite, pending.items, SortContext{}, struct {
        fn lessThan(_: SortContext, lhs: PendingDocumentWrite, rhs: PendingDocumentWrite) bool {
            return std.mem.order(u8, lhs.store_key, rhs.store_key) == .lt;
        }
    }.lessThan);
    if (profile) |p| p.sort_ns = replayCollectorTimeNs() - sort_start_ns;

    const read_keys = try alloc.alloc([]const u8, pending.items.len);
    defer alloc.free(read_keys);
    const read_values = try alloc.alloc(?[]const u8, pending.items.len);
    defer alloc.free(read_values);

    for (pending.items, 0..) |item, i| {
        read_keys[i] = item.store_key;
        read_values[i] = null;
    }
    const read_start_ns = if (profile != null) replayCollectorTimeNs() else 0;
    try txn.getManySorted(read_keys, read_values);
    if (profile) |p| p.read_ns = replayCollectorTimeNs() - read_start_ns;

    for (pending.items, 0..) |item, i| {
        const value = if (read_values[i]) |store_value| blk: {
            if (profile) |p| p.store_hits += 1;
            break :blk store_value;
        } else if (item.inline_value) |inline_value| blk: {
            if (profile) |p| p.inline_hits += 1;
            break :blk inline_value;
        } else {
            missing_required += 1;
            continue;
        };
        const extract_start_ns = if (profile != null) replayCollectorTimeNs() else 0;
        // Store-read relational values must be typed rows; document-mode blobs
        // stay on the generic materialization path.
        const doc_json = try materializeReplayDocumentValueAlloc(alloc, value, opts.relational_base_rows);
        defer alloc.free(doc_json);
        if (try mapper.extractSparseVectorField(alloc, doc_json, field_name)) |raw_sparse_vec| {
            var sparse_vec = raw_sparse_vec;
            writes.append(alloc, .{
                .doc_id = item.doc_key,
                .vec = .{
                    .indices = sparse_vec.indices,
                    .values = sparse_vec.values,
                },
            }) catch |err| {
                sparse_vec.deinit(alloc);
                return err;
            };
            if (profile) |p| p.output_writes += 1;
        } else if (profile) |p| {
            p.skipped_without_vector += 1;
        }
        if (profile) |p| p.extract_ns += replayCollectorTimeNs() - extract_start_ns;
    }
    if (profile) |p| {
        p.missing_required = missing_required;
        p.output_writes = writes.items.len;
    }

    return .{
        .alloc = alloc,
        .items = try writes.toOwnedSlice(alloc),
        .missing_required = missing_required,
    };
}

pub fn collectDocumentWrites(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    documents: []const derived_types.DerivedDocument,
    byte_range: types.ByteRange,
) !OwnedBatchWrites {
    return try collectDocumentWritesProfiled(alloc, store, documents, byte_range, .{}, null);
}

pub fn collectDocumentWritesProfiled(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    documents: []const derived_types.DerivedDocument,
    byte_range: types.ByteRange,
    opts: CollectDocumentWritesOptions,
    profile: ?*CollectDocumentWritesProfile,
) !OwnedBatchWrites {
    const PendingDocumentWrite = struct {
        doc_key: []const u8,
        store_key: []u8,
        inline_value: ?[]const u8,
    };

    var pending = std.ArrayListUnmanaged(PendingDocumentWrite).empty;
    defer {
        for (pending.items) |item| alloc.free(item.store_key);
        pending.deinit(alloc);
    }

    var writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
    errdefer {
        for (writes.items) |item| alloc.free(@constCast(item.value));
        writes.deinit(alloc);
    }

    var txn = try store.beginProbeTxn();
    defer txn.abort();
    var missing_required: usize = 0;
    const trust_inline = opts.prefer_available_inline_values or
        if (opts.prefer_inline_when_store_tip_matches_sequence) |sequence|
            store.nextReplaySequence(sequence + 1) == sequence + 1
        else
            false;

    if (profile) |p| p.input_documents = documents.len;
    const scan_start_ns = if (profile != null) replayCollectorTimeNs() else 0;
    for (documents) |doc| {
        if (doc.action != .upsert) continue;
        if (!byte_range.contains(doc.key)) continue;
        if (opts.skip_doc_keys) |skip_doc_keys| {
            if (skip_doc_keys.contains(doc.key)) continue;
        }
        if (trust_inline and doc.cleaned_value != null) {
            const owned_value = try alloc.dupe(u8, doc.cleaned_value.?);
            try writes.append(alloc, .{
                .key = doc.key,
                .value = owned_value,
            });
            if (profile) |p| p.inline_hits += 1;
            continue;
        }
        try pending.append(alloc, .{
            .doc_key = doc.key,
            .store_key = try replayDocumentStoreKeyAlloc(alloc, doc.key, opts.relational_base_rows),
            .inline_value = doc.cleaned_value,
        });
    }
    if (profile) |p| {
        p.scan_ns = replayCollectorTimeNs() - scan_start_ns;
        p.pending_documents = pending.items.len;
        p.output_writes = writes.items.len;
    }

    if (pending.items.len == 0) {
        return .{
            .alloc = alloc,
            .items = try writes.toOwnedSlice(alloc),
        };
    }

    const SortContext = struct {};
    const sort_start_ns = if (profile != null) replayCollectorTimeNs() else 0;
    std.mem.sort(PendingDocumentWrite, pending.items, SortContext{}, struct {
        fn lessThan(_: SortContext, lhs: PendingDocumentWrite, rhs: PendingDocumentWrite) bool {
            return std.mem.order(u8, lhs.store_key, rhs.store_key) == .lt;
        }
    }.lessThan);
    if (profile) |p| p.sort_ns = replayCollectorTimeNs() - sort_start_ns;

    const read_keys = try alloc.alloc([]const u8, pending.items.len);
    defer alloc.free(read_keys);
    const read_values = try alloc.alloc(?[]const u8, pending.items.len);
    defer alloc.free(read_values);

    for (pending.items, 0..) |item, i| {
        read_keys[i] = item.store_key;
        read_values[i] = null;
    }
    const read_start_ns = if (profile != null) replayCollectorTimeNs() else 0;
    try txn.getManySorted(read_keys, read_values);
    if (profile) |p| p.read_ns = replayCollectorTimeNs() - read_start_ns;

    const materialize_start_ns = if (profile != null) replayCollectorTimeNs() else 0;
    for (pending.items, 0..) |item, i| {
        const value = if (read_values[i]) |store_value| blk: {
            if (profile) |p| p.store_hits += 1;
            break :blk store_value;
        } else if (item.inline_value) |inline_value| blk: {
            if (profile) |p| p.inline_hits += 1;
            break :blk inline_value;
        } else {
            missing_required += 1;
            continue;
        };
        // Relational replay reads the relational row keyspace and requires a
        // typed row there; document mode keeps accepting JSON blobs.
        const owned_value = try materializeReplayDocumentValueAlloc(alloc, value, opts.relational_base_rows);
        try writes.append(alloc, .{
            .key = item.doc_key,
            .value = owned_value,
        });
    }
    if (profile) |p| {
        p.materialize_ns = replayCollectorTimeNs() - materialize_start_ns;
        p.output_writes = writes.items.len;
        p.missing_required = missing_required;
    }

    return .{
        .alloc = alloc,
        .items = try writes.toOwnedSlice(alloc),
        .missing_required = missing_required,
    };
}

fn appendUniqueBorrowedKey(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged([]const u8),
    key: []const u8,
) !void {
    if (key.len == 0) return;
    for (out.items) |existing| {
        if (std.mem.eql(u8, existing, key)) return;
    }
    try out.append(alloc, key);
}

pub fn collectTextReplayDeleteKeys(alloc: Allocator, batch: derived_types.DerivedBatch) ![]const []const u8 {
    var keys = std.ArrayListUnmanaged([]const u8).empty;
    errdefer keys.deinit(alloc);

    for (batch.deleted_keys) |key| try appendUniqueBorrowedKey(alloc, &keys, key);
    for (batch.overwritten_doc_keys) |key| try appendUniqueBorrowedKey(alloc, &keys, key);
    for (batch.documents) |doc| {
        if (doc.action != .upsert) continue;
        try appendUniqueBorrowedKey(alloc, &keys, doc.key);
    }

    return try keys.toOwnedSlice(alloc);
}

pub fn denseEmbeddingDocKeySet(
    alloc: Allocator,
    embeddings: []const mapper.DenseEmbeddingWrite,
) !std.StringHashMapUnmanaged(void) {
    var set = std.StringHashMapUnmanaged(void){};
    errdefer set.deinit(alloc);
    for (embeddings) |embedding| {
        try set.put(alloc, embedding.doc_key, {});
    }
    return set;
}

pub fn sparseEmbeddingDocKeySet(
    alloc: Allocator,
    embeddings: []const mapper.SparseEmbeddingWrite,
) !std.StringHashMapUnmanaged(void) {
    var set = std.StringHashMapUnmanaged(void){};
    errdefer set.deinit(alloc);
    for (embeddings) |embedding| {
        try set.put(alloc, embedding.doc_key, {});
    }
    return set;
}

pub fn attachInlineUpsertDocumentValues(
    alloc: Allocator,
    batch: *derived_types.DerivedBatch,
    req: types.BatchRequest,
    extracted: []const mapper.ExtractedWrite,
) !void {
    var cleaned_by_key = std.StringHashMapUnmanaged([]const u8){};
    defer cleaned_by_key.deinit(alloc);

    for (req.writes, 0..) |write, i| {
        const cleaned = extracted[i].cleaned_value orelse continue;
        try cleaned_by_key.put(alloc, write.key, cleaned);
    }

    for (batch.documents) |*const_doc| {
        const doc: *derived_types.DerivedDocument = @constCast(const_doc);
        if (doc.action != .upsert or doc.cleaned_value != null) continue;
        const cleaned = cleaned_by_key.get(doc.key) orelse continue;
        doc.cleaned_value = try alloc.dupe(u8, cleaned);
    }
}

pub fn applyTextDocumentsForIndex(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    index_manager: *index_manager_mod.IndexManager,
    documents: []const derived_types.DerivedDocument,
    index_name: []const u8,
    byte_range: types.ByteRange,
    delete_keys: []const []const u8,
    opts: CollectTextDocumentWritesOptions,
    index_opts: index_manager_mod.IndexBatchOptions,
) !usize {
    const chunk_name = index_manager.textChunkName(index_name);
    const PendingTextWrite = struct {
        doc_key: []const u8,
        store_key: []u8,
        inline_value: ?[]const u8,
    };

    var pending = std.ArrayListUnmanaged(PendingTextWrite).empty;
    defer {
        for (pending.items) |item| alloc.free(item.store_key);
        pending.deinit(alloc);
    }

    var writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
    defer writes.deinit(alloc);

    // Owned JSON buffers for store-read documents that were materialized from a
    // relational typed row. Inline/cleaned values are borrowed and not tracked
    // here; only freshly reconstructed bytes need freeing.
    var materialized_values = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (materialized_values.items) |buf| alloc.free(buf);
        materialized_values.deinit(alloc);
    }

    const trust_inline = if (opts.prefer_inline_when_store_tip_matches_sequence) |sequence|
        store.nextReplaySequence(sequence + 1) == sequence + 1
    else
        false;

    for (documents) |doc| {
        if (doc.action != .upsert) continue;
        if (!byte_range.contains(doc.key)) continue;
        if (!documentTargetsTextIndex(doc, index_name, chunk_name != null)) continue;
        if (trust_inline and doc.cleaned_value != null) {
            try writes.append(alloc, .{
                .key = doc.key,
                .value = doc.cleaned_value.?,
            });
            continue;
        }
        try pending.append(alloc, .{
            .doc_key = doc.key,
            .store_key = try replayDocumentStoreKeyAlloc(alloc, doc.key, opts.relational_base_rows),
            .inline_value = doc.cleaned_value,
        });
    }

    if (pending.items.len == 0) {
        try index_manager.applyTextBatchByNameWithOptions(store, index_name, delete_keys, writes.items, index_opts);
        return 0;
    }

    var txn = try store.beginProbeTxn();
    defer txn.abort();
    var missing_required: usize = 0;

    const SortContext = struct {};
    std.mem.sort(PendingTextWrite, pending.items, SortContext{}, struct {
        fn lessThan(_: SortContext, lhs: PendingTextWrite, rhs: PendingTextWrite) bool {
            return std.mem.order(u8, lhs.store_key, rhs.store_key) == .lt;
        }
    }.lessThan);

    const read_keys = try alloc.alloc([]const u8, pending.items.len);
    defer alloc.free(read_keys);
    const read_values = try alloc.alloc(?[]const u8, pending.items.len);
    defer alloc.free(read_values);

    for (pending.items, 0..) |item, i| {
        read_keys[i] = item.store_key;
        read_values[i] = null;
    }
    try txn.getManySorted(read_keys, read_values);

    for (pending.items, 0..) |item, i| {
        const raw = read_values[i] orelse item.inline_value orelse {
            missing_required += 1;
            continue;
        };
        // A store-read relational document must be a typed row. Inline values
        // are already cleaned JSON and document-mode store values are borrowed.
        const value = if (read_values[i] != null and opts.relational_base_rows) blk: {
            const json = try mapper.materializeRelationalRowValueAlloc(alloc, raw);
            try materialized_values.append(alloc, json);
            break :blk json;
        } else raw;
        try writes.append(alloc, .{
            .key = item.doc_key,
            .value = value,
        });
    }

    if (missing_required != 0) return missing_required;
    try index_manager.applyTextBatchByNameWithOptions(store, index_name, delete_keys, writes.items, index_opts);
    return missing_required;
}

fn documentTargetsTextIndex(doc: derived_types.DerivedDocument, index_name: []const u8, is_chunk_index: bool) bool {
    for (doc.targets) |target| {
        if (target.kind != .full_text) continue;
        if (std.mem.eql(u8, target.index_name, index_name)) return true;
        if (!is_chunk_index and std.mem.eql(u8, target.index_name, "*")) return true;
    }
    return false;
}

pub fn collectDenseEmbeddingWrites(alloc: Allocator, embeddings: []const derived_types.DerivedDenseEmbeddingWrite, index_name: []const u8) ![]mapper.DenseEmbeddingWrite {
    var filtered = std.ArrayListUnmanaged(mapper.DenseEmbeddingWrite).empty;
    defer filtered.deinit(alloc);

    for (embeddings) |embedding| {
        if (!std.mem.eql(u8, embedding.index_name, index_name)) continue;
        try filtered.append(alloc, .{
            .index_name = @constCast(embedding.index_name),
            .doc_key = @constCast(embedding.doc_key),
            .parent_doc_key = embedding.parent_doc_key,
            .artifact_key = if (embedding.artifact_key) |artifact_key| @constCast(artifact_key) else null,
            .vector = if (embedding.artifact_key != null) &.{} else @constCast(embedding.vector),
        });
    }

    return try filtered.toOwnedSlice(alloc);
}

pub fn collectDenseEmbeddingWritesForBatch(
    alloc: Allocator,
    index_manager: *index_manager_mod.IndexManager,
    embeddings: []const derived_types.DerivedDenseEmbeddingWrite,
    artifact_keys: []const []const u8,
    index_name: []const u8,
) !OwnedDenseEmbeddingWrites {
    var filtered = std.ArrayListUnmanaged(mapper.DenseEmbeddingWrite).empty;
    errdefer {
        for (filtered.items) |write| {
            alloc.free(@constCast(write.doc_key));
            if (write.parent_doc_key) |parent_doc_key| alloc.free(@constCast(parent_doc_key));
        }
        filtered.deinit(alloc);
    }

    for (embeddings) |embedding| {
        if (!std.mem.eql(u8, embedding.index_name, index_name)) continue;
        const doc_key = try alloc.dupe(u8, embedding.doc_key);
        errdefer alloc.free(doc_key);
        var parent_doc_key = if (embedding.parent_doc_key) |parent_key| try alloc.dupe(u8, parent_key) else null;
        errdefer if (parent_doc_key) |owned_parent| alloc.free(owned_parent);
        try filtered.append(alloc, .{
            .index_name = @constCast(embedding.index_name),
            .doc_key = doc_key,
            .parent_doc_key = parent_doc_key,
            .artifact_key = if (embedding.artifact_key) |artifact_key| @constCast(artifact_key) else null,
            .vector = if (embedding.artifact_key != null) &.{} else @constCast(embedding.vector),
        });
        parent_doc_key = null;
    }
    try artifact_replay.appendDenseEmbeddingWritesForArtifacts(alloc, index_manager, &filtered, artifact_keys, index_name);

    return .{
        .alloc = alloc,
        .owns_doc_keys = true,
        .writes = try filtered.toOwnedSlice(alloc),
    };
}

pub fn collectSparseEmbeddingWrites(alloc: Allocator, embeddings: []const derived_types.DerivedSparseEmbeddingWrite, index_name: []const u8) ![]mapper.SparseEmbeddingWrite {
    var filtered = std.ArrayListUnmanaged(mapper.SparseEmbeddingWrite).empty;
    defer filtered.deinit(alloc);

    for (embeddings) |embedding| {
        if (!std.mem.eql(u8, embedding.index_name, index_name)) continue;
        try filtered.append(alloc, .{
            .index_name = @constCast(embedding.index_name),
            .doc_key = @constCast(embedding.doc_key),
            .artifact_key = if (embedding.artifact_key) |artifact_key| @constCast(artifact_key) else null,
            .indices = @constCast(embedding.indices),
            .values = @constCast(embedding.values),
        });
    }

    return try filtered.toOwnedSlice(alloc);
}

pub fn collectSparseEmbeddingWritesForBatch(
    alloc: Allocator,
    index_manager: *index_manager_mod.IndexManager,
    embeddings: []const derived_types.DerivedSparseEmbeddingWrite,
    artifact_keys: []const []const u8,
    index_name: []const u8,
) !OwnedSparseEmbeddingWrites {
    var filtered = std.ArrayListUnmanaged(mapper.SparseEmbeddingWrite).empty;
    var owned_doc_keys = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (owned_doc_keys.items) |doc_key| alloc.free(@constCast(doc_key));
        owned_doc_keys.deinit(alloc);
        filtered.deinit(alloc);
    }

    for (embeddings) |embedding| {
        if (!std.mem.eql(u8, embedding.index_name, index_name)) continue;
        try filtered.append(alloc, .{
            .index_name = @constCast(embedding.index_name),
            .doc_key = @constCast(embedding.doc_key),
            .artifact_key = if (embedding.artifact_key) |artifact_key| @constCast(artifact_key) else null,
            .indices = @constCast(embedding.indices),
            .values = @constCast(embedding.values),
        });
    }
    try artifact_replay.appendSparseEmbeddingWritesForArtifacts(alloc, index_manager, &filtered, &owned_doc_keys, artifact_keys, index_name);

    return .{
        .alloc = alloc,
        .owned_doc_keys = try owned_doc_keys.toOwnedSlice(alloc),
        .writes = try filtered.toOwnedSlice(alloc),
    };
}

pub fn collectGraphWrites(alloc: Allocator, writes: []const types.GraphEdgeWrite, index_name: []const u8) ![]types.GraphEdgeWrite {
    var filtered = std.ArrayListUnmanaged(types.GraphEdgeWrite).empty;
    defer filtered.deinit(alloc);

    for (writes) |write| {
        if (!std.mem.eql(u8, write.index_name, index_name)) continue;
        try filtered.append(alloc, write);
    }

    return try filtered.toOwnedSlice(alloc);
}

pub fn collectGraphDeletes(alloc: Allocator, deletes: []const types.GraphEdgeDelete, index_name: []const u8) ![]types.GraphEdgeDelete {
    var filtered = std.ArrayListUnmanaged(types.GraphEdgeDelete).empty;
    defer filtered.deinit(alloc);

    for (deletes) |delete| {
        if (!std.mem.eql(u8, delete.index_name, index_name)) continue;
        try filtered.append(alloc, delete);
    }

    return try filtered.toOwnedSlice(alloc);
}

pub fn concatArtifactKeyViews(alloc: Allocator, lhs: []const []const u8, rhs: []const []u8) ![][]const u8 {
    const out = try alloc.alloc([]const u8, lhs.len + rhs.len);
    @memcpy(out[0..lhs.len], lhs);
    for (rhs, 0..) |key, i| out[lhs.len + i] = key;
    return out;
}

const dense_catch_up_default_deferred_l0_limit: usize = 4;
const dense_catch_up_default_deferred_hbc_leaf_splits_per_publish: usize = 64;
const dense_catch_up_default_deferred_hbc_leaf_split_members_per_publish: usize = 16 * 1024;
const dense_catch_up_default_maintenance_steps: usize = 8;
const dense_catch_up_default_maintenance_cooldown_ns: u64 = 250 * std.time.ns_per_ms;
const dense_catch_up_default_maintenance_urgent_score: u64 = 1_000_000;
const dense_catch_up_startup_max_records_default: usize = 32;
const dense_catch_up_startup_max_chunk_bytes_default: u64 = 512 * 1024;
const dense_catch_up_startup_cache_nodes_default: usize = 2048;
const dense_catch_up_startup_cache_vectors_default: usize = 2048;
const dense_posting_idle_default_max_postings_per_index: usize = 64;
const dense_posting_idle_default_max_layout_changes_per_index: usize = 8;
const dense_posting_idle_default_max_boundary_reassignments_per_index: usize = 64;

var dense_catch_up_deferred_l0_limit_cache = std.atomic.Value(usize).init(0);
var dense_catch_up_deferred_hbc_leaf_splits_cache = std.atomic.Value(usize).init(0);
var dense_catch_up_deferred_hbc_leaf_split_members_cache = std.atomic.Value(usize).init(0);
var dense_catch_up_bulk_rebuild_hbc_leaf_min_members_cache = std.atomic.Value(usize).init(0);
var dense_catch_up_maintenance_steps_cache = std.atomic.Value(usize).init(0);
var dense_catch_up_maintenance_cooldown_ns_cache = AtomicU64.init(0);
var dense_catch_up_maintenance_urgent_score_cache = AtomicU64.init(0);
var dense_catch_up_startup_max_records_cache = std.atomic.Value(usize).init(0);
var dense_catch_up_startup_max_chunk_bytes_cache = AtomicU64.init(0);
var dense_catch_up_startup_cache_nodes_cache = std.atomic.Value(usize).init(0);
var dense_catch_up_startup_cache_vectors_cache = std.atomic.Value(usize).init(0);
var dense_posting_idle_max_postings_cache = std.atomic.Value(usize).init(0);
var dense_posting_idle_max_layout_changes_cache = std.atomic.Value(usize).init(0);
var dense_posting_idle_max_boundary_reassignments_cache = std.atomic.Value(usize).init(0);

fn cachedEnvUsize(cache: *std.atomic.Value(usize), name: [:0]const u8, default_value: usize) usize {
    const cached = cache.load(.acquire);
    if (cached != 0) return cached - 1;

    const value = readEnvUsize(name, default_value);
    const encoded = value +% 1;
    _ = cache.cmpxchgWeak(0, encoded, .acq_rel, .acquire);
    return cache.load(.acquire) - 1;
}

fn cachedEnvU64(cache: *AtomicU64, name: [:0]const u8, default_value: u64) u64 {
    const cached = cache.load(.acquire);
    if (cached != 0) return cached - 1;

    const value = readEnvU64(name, default_value);
    const encoded = value +% 1;
    _ = cache.cmpxchgWeak(0, encoded, .acq_rel, .acquire);
    return cache.load(.acquire) - 1;
}

fn cachedOptionalEnvUsize(cache: *std.atomic.Value(usize), name: [:0]const u8) ?usize {
    const cached = cache.load(.acquire);
    if (cached != 0) return if (cached == 1) null else cached - 2;

    const encoded = if (db_internal.readOptionalEnvUsize(name)) |value| value +% 2 else 1;
    _ = cache.cmpxchgWeak(0, encoded, .acq_rel, .acquire);

    const loaded = cache.load(.acquire);
    return if (loaded == 1) null else loaded - 2;
}

pub fn denseCatchUpFinishOptions() backend_types.BulkIngestFinishOptions {
    return .{
        .compact = false,
        .flush = true,
        .max_deferred_l0_runs = denseCatchUpDeferredL0Limit(),
        .max_deferred_hbc_leaf_splits_per_publish = denseCatchUpDeferredHbcLeafSplitsPerPublish(),
        .max_deferred_hbc_leaf_split_members_per_publish = denseCatchUpDeferredHbcLeafSplitMembersPerPublish(),
        .bulk_rebuild_hbc_leaf_min_members = denseCatchUpBulkRebuildHbcLeafMinMembers(),
    };
}

fn denseCatchUpDeferredL0Limit() usize {
    return cachedEnvUsize(
        &dense_catch_up_deferred_l0_limit_cache,
        "ANTFLY_DENSE_CATCH_UP_MAX_DEFERRED_L0_RUNS",
        dense_catch_up_default_deferred_l0_limit,
    );
}

fn denseCatchUpDeferredHbcLeafSplitsPerPublish() usize {
    return cachedEnvUsize(
        &dense_catch_up_deferred_hbc_leaf_splits_cache,
        "ANTFLY_DENSE_CATCH_UP_MAX_DEFERRED_HBC_LEAF_SPLITS_PER_PUBLISH",
        dense_catch_up_default_deferred_hbc_leaf_splits_per_publish,
    );
}

fn denseCatchUpDeferredHbcLeafSplitMembersPerPublish() usize {
    return cachedEnvUsize(
        &dense_catch_up_deferred_hbc_leaf_split_members_cache,
        "ANTFLY_DENSE_CATCH_UP_MAX_DEFERRED_HBC_LEAF_SPLIT_MEMBERS_PER_PUBLISH",
        dense_catch_up_default_deferred_hbc_leaf_split_members_per_publish,
    );
}

fn denseCatchUpBulkRebuildHbcLeafMinMembers() ?usize {
    return cachedOptionalEnvUsize(
        &dense_catch_up_bulk_rebuild_hbc_leaf_min_members_cache,
        "ANTFLY_DENSE_CATCH_UP_BULK_REBUILD_HBC_LEAF_MIN_MEMBERS",
    );
}

pub fn denseCatchUpMaintenanceSteps() usize {
    return cachedEnvUsize(
        &dense_catch_up_maintenance_steps_cache,
        "ANTFLY_DENSE_CATCH_UP_MAINTENANCE_STEPS",
        dense_catch_up_default_maintenance_steps,
    );
}

pub fn denseCatchUpMaintenanceCooldownNs() u64 {
    return cachedEnvU64(
        &dense_catch_up_maintenance_cooldown_ns_cache,
        "ANTFLY_DENSE_CATCH_UP_MAINTENANCE_COOLDOWN_NS",
        dense_catch_up_default_maintenance_cooldown_ns,
    );
}

pub fn denseCatchUpMaintenanceUrgentScore() u64 {
    return cachedEnvU64(
        &dense_catch_up_maintenance_urgent_score_cache,
        "ANTFLY_DENSE_CATCH_UP_MAINTENANCE_URGENT_SCORE",
        dense_catch_up_default_maintenance_urgent_score,
    );
}

test "dense catch-up maintenance cooldown skips light repeated maintenance" {
    const TestDB = struct {};
    const TestImpl = Impl(TestDB);
    const TestAsyncContext = db_internal.AsyncContext(TestDB);
    var ctx = TestAsyncContext{
        .alloc = std.testing.allocator,
        .store = undefined,
        .index_manager = undefined,
        .apply_mutex = undefined,
    };
    defer ctx.deinit(std.testing.allocator);

    const now_ns = std.time.ns_per_s;
    try std.testing.expect(TestImpl.shouldRunDenseCatchUpMaintenance(&ctx, "vec", 1, now_ns));
    try TestImpl.noteDenseCatchUpMaintenanceRun(&ctx, "vec", now_ns);
    try std.testing.expect(!TestImpl.shouldRunDenseCatchUpMaintenance(&ctx, "vec", 1, now_ns + denseCatchUpMaintenanceCooldownNs() - 1));
    try std.testing.expect(TestImpl.shouldRunDenseCatchUpMaintenance(&ctx, "vec", denseCatchUpMaintenanceUrgentScore(), now_ns + 1));
    try std.testing.expect(TestImpl.shouldRunDenseCatchUpMaintenance(&ctx, "vec", 1, now_ns + denseCatchUpMaintenanceCooldownNs()));
}

test "db derived async runUntilIdle drains lazy dense posting maintenance" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"use_quantization\":false,\"lazy_posting_maintenance\":true,\"auto_posting_maintenance_max_postings\":0}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"embedding\":[1.0,0.0]}" },
            .{ .key = "doc:b", .value = "{\"embedding\":[3.0,0.0]}" },
        },
        .sync_level = .full_index,
    });

    {
        const entry = db.core.denseIndex("dv_v1") orelse return error.IndexNotFound;
        var txn = try entry.index.beginWriteTxn();
        errdefer txn.abort();
        var root = try entry.index.loadNode(&txn, entry.index.metadata.root_node);
        defer root.deinit(alloc);
        try root.ensureUnbacked(alloc);
        root.posting_state.noteMembersChanged(root.members.len);
        try entry.index.saveNode(&txn, &root);
        try entry.index.finishWriteTxn(&txn);
        entry.index.invalidateNodeCache(root.id);
    }

    {
        const stats = try db.diagnosticStats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(@as(u64, 1), stats.indexes[0].hbc_posting.dirty_postings);
    }

    try db.runUntilIdle();

    {
        const stats = try db.diagnosticStats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(@as(u64, 0), stats.indexes[0].hbc_posting.dirty_postings);
        try std.testing.expect(stats.indexes[0].hbc_posting.maintenance_repaired_postings > 0);
    }
}

test "db derived async collectManagedSyncTargets includes graph index for graph artifact journal changes" {
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

    const artifact_key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:a", "gr_v1", "links", "doc:b");

    var batch = derived_types.DerivedBatch{
        .changed_artifact_keys = try alloc.dupe([]const u8, &.{artifact_key}),
    };
    defer derived_types.deinitDerivedBatch(alloc, &batch);

    var sync_targets = try db.derivedAsyncCollectManagedSyncTargets(alloc, batch);
    defer sync_targets.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), sync_targets.all_indexes.len);
    try std.testing.expectEqualStrings("gr_v1", sync_targets.all_indexes[0]);
}

test "db derived async filters relational text-search sync targets by generation record" {
    const alloc = std.testing.allocator;
    const Case = struct {
        name: []const u8,
        schema: ?schema_mod.TableSchema,
        expected_full_text: []const []const u8,
        expected_all: []const []const u8,
    };

    const ready_index = relationalTextSearchIndexForTest(.ready, .{ .generation = 7, .lifecycle = .ready });
    const building_index = relationalTextSearchIndexForTest(.building, .{ .generation = 7, .lifecycle = .building });
    const catching_up_index = relationalTextSearchIndexForTest(.catching_up, .{ .generation = 7, .lifecycle = .catching_up, .lag = 4 });
    const invalid_index = relationalTextSearchIndexForTest(.invalid, .{ .generation = 7, .lifecycle = .invalid });
    const stale_index = relationalTextSearchIndexForTest(.stale, .{ .generation = 7, .lifecycle = .stale });
    const missing_record_index = schema_mod.RelationalIndex{
        .name = "fts_rel",
        .owner_kind = .table,
        .owner_name = schema_mod.relational_table_index_owner_name,
        .access_method = .text_search,
        .lifecycle = .ready,
        .generation = 7,
    };
    const mismatched_generation_index = relationalTextSearchIndexForTest(.ready, .{ .generation = 6, .lifecycle = .ready });
    const unrelated_indexes = [_]schema_mod.RelationalIndex{
        .{
            .name = "scalar_status",
            .owner_kind = .relational_column,
            .owner_name = "status",
            .access_method = .scalar_column,
        },
        relationalTextSearchIndexNamedForTest("fts_other", .ready, .{ .generation = 7, .lifecycle = .ready }),
    };

    const keep_full_text = [_][]const u8{ "fts_rel", "fts_doc" };
    const keep_all = [_][]const u8{ "fts_rel", "fts_doc", "dense_v1" };
    const filtered_full_text = [_][]const u8{"fts_doc"};
    const filtered_all = [_][]const u8{ "fts_doc", "dense_v1" };
    const cases = [_]Case{
        .{
            .name = "no relational schema keeps document full-text targets",
            .schema = null,
            .expected_full_text = &keep_full_text,
            .expected_all = &keep_all,
        },
        .{
            .name = "no matching relational text-search index keeps targets",
            .schema = .{ .relational_indexes = &unrelated_indexes },
            .expected_full_text = &keep_full_text,
            .expected_all = &keep_all,
        },
        .{
            .name = "ready generation keeps target",
            .schema = .{ .relational_indexes = &[_]schema_mod.RelationalIndex{ready_index} },
            .expected_full_text = &keep_full_text,
            .expected_all = &keep_all,
        },
        .{
            .name = "building generation keeps target for write maintenance",
            .schema = .{ .relational_indexes = &[_]schema_mod.RelationalIndex{building_index} },
            .expected_full_text = &keep_full_text,
            .expected_all = &keep_all,
        },
        .{
            .name = "catching-up generation keeps target for write maintenance",
            .schema = .{ .relational_indexes = &[_]schema_mod.RelationalIndex{catching_up_index} },
            .expected_full_text = &keep_full_text,
            .expected_all = &keep_all,
        },
        .{
            .name = "invalid generation filters relational text-search target",
            .schema = .{ .relational_indexes = &[_]schema_mod.RelationalIndex{invalid_index} },
            .expected_full_text = &filtered_full_text,
            .expected_all = &filtered_all,
        },
        .{
            .name = "stale generation filters relational text-search target",
            .schema = .{ .relational_indexes = &[_]schema_mod.RelationalIndex{stale_index} },
            .expected_full_text = &filtered_full_text,
            .expected_all = &filtered_all,
        },
        .{
            .name = "missing generation record filters relational text-search target",
            .schema = .{ .relational_indexes = &[_]schema_mod.RelationalIndex{missing_record_index} },
            .expected_full_text = &filtered_full_text,
            .expected_all = &filtered_all,
        },
        .{
            .name = "mismatched generation record filters relational text-search target",
            .schema = .{ .relational_indexes = &[_]schema_mod.RelationalIndex{mismatched_generation_index} },
            .expected_full_text = &filtered_full_text,
            .expected_all = &filtered_all,
        },
    };

    for (cases) |case| {
        var targets = try managedSyncTargetsForFilterTest(alloc);
        defer targets.deinit(alloc);

        try filterManagedSyncTargetsForRelationalTextSearchMaintenance(alloc, case.schema, &targets);

        expectManagedSyncTargetNames(case.name, case.expected_full_text, targets.full_text_indexes) catch |err| {
            std.debug.print("case failed: {s}\n", .{case.name});
            return err;
        };
        expectManagedSyncTargetNames(case.name, case.expected_all, targets.all_indexes) catch |err| {
            std.debug.print("case failed: {s}\n", .{case.name});
            return err;
        };
    }
}

fn relationalTextSearchIndexForTest(
    lifecycle: schema_mod.RelationalIndexLifecycle,
    record: schema_mod.RelationalIndexGenerationRecord,
) schema_mod.RelationalIndex {
    return relationalTextSearchIndexNamedForTest("fts_rel", lifecycle, record);
}

fn relationalTextSearchIndexNamedForTest(
    name: []const u8,
    lifecycle: schema_mod.RelationalIndexLifecycle,
    record: schema_mod.RelationalIndexGenerationRecord,
) schema_mod.RelationalIndex {
    return .{
        .name = name,
        .owner_kind = .table,
        .owner_name = schema_mod.relational_table_index_owner_name,
        .access_method = .text_search,
        .lifecycle = lifecycle,
        .generation = 7,
        .generation_record = record,
    };
}

fn managedSyncTargetsForFilterTest(alloc: Allocator) !db_internal.ManagedSyncTargets {
    var full_text_indexes = try alloc.alloc([]const u8, 2);
    var initialized_full_text: usize = 0;
    errdefer {
        for (full_text_indexes[0..initialized_full_text]) |name| alloc.free(@constCast(name));
        alloc.free(full_text_indexes);
    }
    full_text_indexes[0] = try alloc.dupe(u8, "fts_rel");
    initialized_full_text += 1;
    full_text_indexes[1] = try alloc.dupe(u8, "fts_doc");
    initialized_full_text += 1;

    var all_indexes = try alloc.alloc([]const u8, 3);
    var initialized_all: usize = 0;
    errdefer {
        for (all_indexes[0..initialized_all]) |name| alloc.free(@constCast(name));
        alloc.free(all_indexes);
    }
    all_indexes[0] = try alloc.dupe(u8, "fts_rel");
    initialized_all += 1;
    all_indexes[1] = try alloc.dupe(u8, "fts_doc");
    initialized_all += 1;
    all_indexes[2] = try alloc.dupe(u8, "dense_v1");
    initialized_all += 1;

    return .{
        .full_text_indexes = full_text_indexes,
        .all_indexes = all_indexes,
    };
}

fn expectManagedSyncTargetNames(case_name: []const u8, expected: []const []const u8, actual: []const []const u8) !void {
    if (expected.len != actual.len) {
        std.debug.print("case {s}: expected {} targets, got {}\n", .{ case_name, expected.len, actual.len });
        return error.TestExpectedEqual;
    }
    for (expected, actual) |expected_name, actual_name| {
        try std.testing.expectEqualStrings(expected_name, actual_name);
    }
}

pub fn filterManagedSyncTargetsForRelationalTextSearchMaintenance(
    alloc: Allocator,
    schema: ?schema_mod.TableSchema,
    targets: *db_internal.ManagedSyncTargets,
) !void {
    targets.full_text_indexes = try filterRelationalTextSearchMaintenanceTargets(alloc, schema, targets.full_text_indexes);
    targets.all_indexes = try filterRelationalTextSearchMaintenanceTargets(alloc, schema, targets.all_indexes);
}

fn filterRelationalTextSearchMaintenanceTargets(
    alloc: Allocator,
    schema: ?schema_mod.TableSchema,
    source: []const []const u8,
) ![]const []const u8 {
    var kept_len: usize = 0;
    for (source) |name| {
        if (relationalTextSearchWriteMaintenanceAllowed(schema, name)) kept_len += 1;
    }
    var kept: [][]const u8 = if (kept_len == 0) &.{} else try alloc.alloc([]const u8, kept_len);
    var kept_index: usize = 0;
    for (source) |name| {
        if (relationalTextSearchWriteMaintenanceAllowed(schema, name)) {
            kept[kept_index] = name;
            kept_index += 1;
        } else {
            alloc.free(@constCast(name));
        }
    }
    if (source.len > 0) alloc.free(source);
    return kept;
}

fn relationalTextSearchWriteMaintenanceAllowed(schema: ?schema_mod.TableSchema, index_name: []const u8) bool {
    const active_schema = schema orelse return true;
    var matched_relational_text_search = false;
    var allowed = false;
    for (active_schema.relational_indexes) |index| {
        if (index.access_method != .text_search) continue;
        if (!std.mem.eql(u8, index.name, index_name)) continue;
        matched_relational_text_search = true;
        if (relationalTextSearchIndexWriteMaintenanceAllowed(index)) allowed = true;
    }
    return !matched_relational_text_search or allowed;
}

fn relationalTextSearchIndexWriteMaintenanceAllowed(index: schema_mod.RelationalIndex) bool {
    if (!schema_mod.relationalIndexGenerationRecordValid(index)) return false;
    const lifecycle = schema_mod.relationalIndexLifecycle(index) orelse return false;
    return switch (lifecycle) {
        .ready, .building, .catching_up => true,
        .invalid, .dropping, .stale, .rebuild_required, .failed => false,
    };
}

pub fn denseCatchUpStartupMaxRecords() usize {
    return cachedEnvUsize(
        &dense_catch_up_startup_max_records_cache,
        "ANTFLY_DENSE_CATCH_UP_STARTUP_MAX_RECORDS",
        dense_catch_up_startup_max_records_default,
    );
}

pub fn denseCatchUpStartupMaxChunkBytes() u64 {
    return cachedEnvU64(
        &dense_catch_up_startup_max_chunk_bytes_cache,
        "ANTFLY_DENSE_CATCH_UP_STARTUP_MAX_CHUNK_BYTES",
        dense_catch_up_startup_max_chunk_bytes_default,
    );
}

pub fn denseCatchUpStartupCacheNodes() usize {
    return cachedEnvUsize(
        &dense_catch_up_startup_cache_nodes_cache,
        "ANTFLY_DENSE_CATCH_UP_STARTUP_MAX_CACHED_NODES",
        dense_catch_up_startup_cache_nodes_default,
    );
}

pub fn denseCatchUpStartupCacheVectors() usize {
    return cachedEnvUsize(
        &dense_catch_up_startup_cache_vectors_cache,
        "ANTFLY_DENSE_CATCH_UP_STARTUP_MAX_CACHED_VECTORS",
        dense_catch_up_startup_cache_vectors_default,
    );
}

pub fn densePostingIdleMaxPostingsPerIndex() usize {
    return cachedEnvUsize(
        &dense_posting_idle_max_postings_cache,
        "ANTFLY_DENSE_POSTING_IDLE_MAX_POSTINGS_PER_INDEX",
        dense_posting_idle_default_max_postings_per_index,
    );
}

pub fn densePostingIdleMaxLayoutChangesPerIndex() usize {
    return cachedEnvUsize(
        &dense_posting_idle_max_layout_changes_cache,
        "ANTFLY_DENSE_POSTING_IDLE_MAX_LAYOUT_CHANGES_PER_INDEX",
        dense_posting_idle_default_max_layout_changes_per_index,
    );
}

pub fn densePostingIdleMaxBoundaryReassignmentsPerIndex() usize {
    return cachedEnvUsize(
        &dense_posting_idle_max_boundary_reassignments_cache,
        "ANTFLY_DENSE_POSTING_IDLE_MAX_BOUNDARY_REASSIGNMENTS_PER_INDEX",
        dense_posting_idle_default_max_boundary_reassignments_per_index,
    );
}

pub const DenseArtifactRebuildResumeHook = *const fn (ctx: *anyopaque, last_key: []const u8) anyerror!void;

pub const DenseArtifactRebuildTarget = struct {
    dense_index_idx: usize,
    resume_from: ?[]u8 = null,
    artifact_target_count: u64 = 0,
    force_reset: bool = false,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        if (self.resume_from) |buf| alloc.free(buf);
        self.* = .{
            .dense_index_idx = 0,
        };
    }
};

pub fn Impl(comptime DB: type) type {
    return struct {
        const Self = @This();
        const AsyncContext = db_internal.AsyncContext(DB);
        const BatchExecutionContext = db_internal.BatchExecutionContext(DB);
        const EnrichmentAppendContext = db_internal.EnrichmentAppendContext(DB);
        const BatchProfile = DB.WritePathCallbacks.Profile;
        const ManagedSyncTargets = db_internal.ManagedSyncTargets;
        const ReplayApplyContext = db_internal.ReplayApplyContext(DB);
        const ReplayApplyContextBatch = db_internal.ReplayApplyContextBatch(DB);

        const DenseArtifactRebuildPlan = struct {
            targets: []DenseArtifactRebuildTarget = &.{},
            target_sequence: u64 = 0,

            fn deinit(self: *@This(), alloc: Allocator) void {
                for (self.targets) |*target| target.deinit(alloc);
                if (self.targets.len > 0) alloc.free(self.targets);
                self.* = .{};
            }
        };

        const DenseArtifactTargetCounts = struct {
            per_target_index: std.AutoHashMapUnmanaged(usize, u64) = .empty,
            total_target_artifacts: u64 = 0,

            fn deinit(self: *@This(), alloc: Allocator) void {
                self.per_target_index.deinit(alloc);
                self.* = .{};
            }
        };

        pub fn canAdvanceDerivedToTargetAsync(ctx_ptr: *anyopaque, index_ref: index_manager_mod.ManagedIndexRef, from_sequence: u64, target_sequence: u64) !bool {
            _ = from_sequence;
            const ctx = asyncContextFromOpaque(ctx_ptr);
            const persisted_applied = try apply_state.loadAppliedSequence(ctx.alloc, ctx.store, index_ref.name);
            if (persisted_applied >= target_sequence) return true;
            if (index_ref.kind != .dense_vector) return true;

            ctx.apply_mutex.lockExclusive();
            defer ctx.apply_mutex.unlockExclusive();

            const entry = ctx.index_manager.denseIndex(index_ref.name) orelse return true;
            if (!denseIndexIsArtifactBacked(entry)) return true;
            if (db_internal.asyncContextHasActiveDenseBulkWork(ctx)) return false;

            const expected_doc_count = try denseTargetCountForIndexContext(ctx, index_ref.name);
            if (expected_doc_count == 0) return true;
            if (entry.index.stats().active_count >= expected_doc_count) return true;

            std.log.warn(
                "dense replay target advance blocked by coverage gap index={s} indexed={} expected_docs={}",
                .{ index_ref.name, entry.index.stats().active_count, expected_doc_count },
            );

            _ = rebuildDenseIndexForTargetCoverageContext(ctx, index_ref.name, 2048) catch |err| {
                std.log.warn(
                    "dense replay target advance repair deferred index={s} err={s}",
                    .{ index_ref.name, @errorName(err) },
                );
                return false;
            };
            const repaired_entry = ctx.index_manager.denseIndex(index_ref.name) orelse return true;
            return repaired_entry.index.stats().active_count >= expected_doc_count;
        }

        pub fn appendDerivedBatchRecord(self: *DB, batch: derived_types.DerivedBatch) !u64 {
            var ctx = self.batchContext();
            return try appendDerivedBatchRecordContext(&ctx, batch);
        }

        pub fn appendDerivedBatchRecordContext(ctx: *const BatchExecutionContext, batch: derived_types.DerivedBatch) !u64 {
            try DB.DerivedAsyncCallbacks.enforce_ha_write_gate_optional(ctx.ha_write_gate);
            const sequence = ctx.store.reserveNextReplaySequence(1);
            const payload = try encodeChangeRecordPayload(ctx, batch, sequence);
            defer ctx.alloc.free(payload);
            try ctx.store.appendReplayOpaque(ctx.alloc, sequence, payload);
            DB.DerivedAsyncCallbacks.mirror_ha_replay_payload_best_effort_context(ctx.log_mutex, ctx.ha_async_effect_mirror, payload);
            ctx.executor.trackBacklogBytes(sequence, @intCast(payload.len)) catch {};
            return sequence;
        }

        pub fn appendReplicatedHADerivedEffectContext(ctx: *const BatchExecutionContext, record: ha_replication_record_mod.RecordView) !u64 {
            var decoded = try ha_effects_mod.decodeDerivedChangeRecord(ctx.alloc, record);
            defer decoded.deinit();

            const sequence = ctx.store.reserveNextReplaySequence(1);
            decoded.record.sequence = sequence;
            const payload = try change_journal_mod.encodeRecord(ctx.alloc, decoded.record);
            defer ctx.alloc.free(payload);

            try ctx.store.appendReplayOpaque(ctx.alloc, sequence, payload);
            ctx.executor.trackBacklogBytes(sequence, @intCast(payload.len)) catch {};
            return sequence;
        }

        pub fn encodeChangeRecordPayload(ctx: *const BatchExecutionContext, batch: derived_types.DerivedBatch, sequence: u64) ![]u8 {
            var record = try change_journal_mod.recordFromDerivedBatch(ctx.alloc, batch, sequence);
            defer change_journal_mod.deinitRecord(ctx.alloc, &record);
            return try change_journal_mod.encodeRecord(ctx.alloc, record);
        }

        pub fn encodeChangeRecordPayloadForDB(self: *DB, derived_batch: derived_types.DerivedBatch, sequence: u64) ![]u8 {
            var ctx = self.batchContext();
            return try encodeChangeRecordPayload(&ctx, derived_batch, sequence);
        }

        pub fn applyDerivedBacklogPressure(self: *DB, sequence: u64, sync_level: types.SyncLevel, sync_targets: ManagedSyncTargets) !void {
            var ctx = self.batchContext();
            try applyDerivedBacklogPressureContext(&ctx, sequence, sync_level, sync_targets);
        }

        pub fn appendDerivedBatchFromEnrichment(ctx_ptr: *anyopaque, batch: derived_types.DerivedBatch) !u64 {
            const ctx: *EnrichmentAppendContext = @ptrCast(@alignCast(ctx_ptr));
            var batch_ctx = ctx.batchContext();
            const sequence = try appendDerivedBatchRecordContext(&batch_ctx, batch);
            notifyResolverReplayRuntimesForCatalog(ctx.index_manager, ctx.resolution_runtime, ctx.promotion_runtime, sequence);
            if (ctx.executor.hasWorkers()) {
                ctx.executor.forceSequence(sequence);
                return sequence;
            }

            var applied_batch = batch;
            applied_batch.sequence = sequence;
            try applyDerivedBatchContext(&batch_ctx, applied_batch);
            return sequence;
        }

        pub fn notifyDerivedExecutorSequence(ctx_ptr: *anyopaque, sequence: u64) void {
            const executor: *derived_executor_mod.Executor = @ptrCast(@alignCast(ctx_ptr));
            executor.notifySequence(sequence);
        }

        pub fn notifyResolverReplayRuntimesForCatalog(
            index_manager: *const index_manager_mod.IndexManager,
            resolution_runtime: ?*resolution_runtime_mod.ResolutionRuntime,
            promotion_runtime: ?*promotion_runtime_mod.PromotionRuntime,
            sequence: u64,
        ) void {
            if (index_manager.resolvers.items.len == 0) return;
            if (resolution_runtime) |runtime| runtime.notifySequence(sequence);
            if (promotion_runtime) |runtime| runtime.notifySequence(sequence);
        }

        pub fn applyDerivedBacklogPressureContext(ctx: *const BatchExecutionContext, sequence: u64, sync_level: types.SyncLevel, sync_targets: ManagedSyncTargets) !void {
            switch (sync_level) {
                .propose, .write => return,
                .enrichments, .full_text, .aknn, .full_index => {},
            }
            if (!ctx.executor.shouldThrottleBacklog()) return;
            if (sync_level == .full_text) {
                try runDerivedUntilTargetsContext(ctx, sequence, sync_targets.full_text_indexes);
                return;
            }
            if (shouldDeferBacklogPressureForExternalDenseBulk(ctx, sync_level)) return;
            runDerivedUntilContext(ctx, sequence) catch |err| switch (err) {
                error.WriterLocked, error.ReplayDocumentNotVisible => {
                    ctx.executor.notifySequence(sequence);
                    return;
                },
                else => return err,
            };
        }

        pub fn markPrecomputedEnrichmentAppliedForSyncContext(ctx: *const BatchExecutionContext, sync_level: types.SyncLevel, sequence: u64) !void {
            if (sync_level != .enrichments or sequence == 0) return;
            const runtime = ctx.enrichment_runtime orelse return;
            const runtime_stats = runtime.stats();
            if (runtime_stats.applied_sequence >= sequence -| 1 or
                try noPendingEnrichmentReplayThroughContext(ctx, runtime_stats.applied_sequence, sequence))
            {
                try runtime.markAppliedThrough(sequence);
            }
        }

        pub fn waitForSyncLevelContext(ctx: *const BatchExecutionContext, sync_level: types.SyncLevel, sequence: u64, sync_targets: ManagedSyncTargets) !void {
            switch (sync_level) {
                .propose, .write => try ctx.executor.failIfUnhealthy(),
                .enrichments => {
                    try ctx.executor.failIfUnhealthy();
                    try runEnrichmentUntilContext(ctx, sequence);
                },
                .full_text => {
                    try runMaintenanceUntilTargetsContext(ctx, sequence, sync_targets.full_text_indexes);
                },
                .aknn, .full_index => try runMaintenanceUntilContext(ctx, sequence, sync_targets),
            }
        }

        pub fn waitForSyncLevel(self: *DB, sync_level: types.SyncLevel, sequence: u64, sync_targets: ManagedSyncTargets) !void {
            var ctx = self.batchContext();
            return try waitForSyncLevelContext(&ctx, sync_level, sequence, sync_targets);
        }

        pub fn syncLevelRequiresDerivedVisibility(sync_level: types.SyncLevel) bool {
            return switch (sync_level) {
                .propose, .write, .enrichments => false,
                .full_text, .aknn, .full_index => true,
            };
        }

        pub fn shouldRunDenseCatchUpMaintenance(ctx: *AsyncContext, index_name: []const u8, score: u64, now_ns: u64) bool {
            if (score == 0) return false;
            const cooldown_ns = denseCatchUpMaintenanceCooldownNs();
            if (cooldown_ns == 0) return true;
            if (score >= denseCatchUpMaintenanceUrgentScore()) return true;
            const last_ns = ctx.dense_maintenance_last_ns.get(index_name) orelse return true;
            return now_ns -| last_ns >= cooldown_ns;
        }

        pub fn noteDenseCatchUpMaintenanceRun(ctx: *AsyncContext, index_name: []const u8, now_ns: u64) !void {
            const gop = try ctx.dense_maintenance_last_ns.getOrPut(ctx.alloc, index_name);
            if (!gop.found_existing) gop.key_ptr.* = try ctx.alloc.dupe(u8, index_name);
            gop.value_ptr.* = now_ns;
        }

        pub fn collectManagedSyncTargets(
            alloc: Allocator,
            index_manager: *index_manager_mod.IndexManager,
            batch: derived_types.DerivedBatch,
        ) !ManagedSyncTargets {
            const managed_indexes = try index_manager.managedIndexes(alloc);
            defer {
                for (managed_indexes) |index_ref| alloc.free(@constCast(index_ref.name));
                alloc.free(managed_indexes);
            }

            var full_text_indexes = std.ArrayListUnmanaged([]const u8).empty;
            errdefer {
                for (full_text_indexes.items) |name| alloc.free(@constCast(name));
                full_text_indexes.deinit(alloc);
            }
            var all_indexes = std.ArrayListUnmanaged([]const u8).empty;
            errdefer {
                for (all_indexes.items) |name| alloc.free(@constCast(name));
                all_indexes.deinit(alloc);
            }

            for (managed_indexes) |index_ref| {
                if (!batchAffectsManagedIndex(index_manager, batch, index_ref)) continue;
                try appendUniqueOwnedName(alloc, &all_indexes, index_ref.name);
                if (index_ref.kind == .full_text) {
                    try appendUniqueOwnedName(alloc, &full_text_indexes, index_ref.name);
                }
            }

            return .{
                .full_text_indexes = try full_text_indexes.toOwnedSlice(alloc),
                .all_indexes = try all_indexes.toOwnedSlice(alloc),
            };
        }

        pub fn collectManagedSyncTargetsForDB(self: *DB, alloc: Allocator, batch: derived_types.DerivedBatch) !ManagedSyncTargets {
            var targets = try collectManagedSyncTargets(alloc, self.core.index_manager, batch);
            errdefer targets.deinit(alloc);
            try filterManagedSyncTargetsForRelationalTextSearchMaintenance(alloc, self.core.schema, &targets);
            return targets;
        }

        fn appendUniqueOwnedName(
            alloc: Allocator,
            items: *std.ArrayListUnmanaged([]const u8),
            value: []const u8,
        ) !void {
            for (items.items) |existing| {
                if (std.mem.eql(u8, existing, value)) return;
            }
            try items.append(alloc, try alloc.dupe(u8, value));
        }

        fn applyGraphDocClearsForIndex(ctx: *const AsyncContext, clears: []const derived_types.DerivedGraphDocClear, index_name: []const u8) !void {
            for (clears) |clear| {
                for (clear.index_names) |clear_index_name| {
                    if (!std.mem.eql(u8, clear_index_name, index_name)) continue;
                    try ctx.index_manager.deleteGraphDocInIndexes(clear.key, &.{index_name});
                    break;
                }
            }
        }

        pub fn applyDerivedBatchToIndexContext(ctx: *const AsyncContext, batch: derived_types.DerivedBatch, index_ref: index_manager_mod.ManagedIndexRef) !void {
            try applyDerivedBatchToIndexContextProfiled(ctx, batch, index_ref, null);
        }

        pub fn applyDerivedBatchToIndexContextProfiled(ctx: *const AsyncContext, batch: derived_types.DerivedBatch, index_ref: index_manager_mod.ManagedIndexRef, profile: ?*BatchProfile) !void {
            var index_apply_guard = try ctx.index_manager.lockManagedIndexApply(index_ref);
            defer index_apply_guard.unlock();
            switch (index_ref.kind) {
                .full_text => {
                    const apply_start_ns = monotonicTimeNs();
                    const text_replay_options: index_manager_mod.IndexBatchOptions = .{
                        .compact_text = false,
                        .compact_text_segment_threshold = null,
                        .defer_text_compaction = true,
                    };
                    const delete_keys = try collectTextReplayDeleteKeys(ctx.alloc, batch);
                    defer if (delete_keys.len > 0) ctx.alloc.free(delete_keys);

                    const missing_required = try applyTextDocumentsForIndex(
                        ctx.alloc,
                        ctx.store,
                        ctx.index_manager,
                        batch.documents,
                        index_ref.name,
                        ctx.index_manager.byte_range,
                        delete_keys,
                        .{
                            .prefer_inline_when_store_tip_matches_sequence = batch.sequence,
                            .relational_base_rows = ctx.relational_base_rows,
                        },
                        text_replay_options,
                    );
                    if (missing_required != 0) return error.ReplayDocumentNotVisible;
                    if (profile) |active_profile| DB.WritePathCallbacks.record_profile_ns(profile, &active_profile.full_text_apply_ns, apply_start_ns);
                },
                .dense_vector => {
                    const dense_apply_start_ns = monotonicTimeNs();
                    const dense_finish_options = denseCatchUpFinishOptions();
                    const use_local_bulk_session = db_internal.denseApplyUsesLocalBulkSession(ctx, index_ref.name);
                    var dense_bulk_session_open = false;
                    if (use_local_bulk_session) {
                        try ctx.index_manager.beginDenseBulkIngestSessionByName(index_ref.name);
                        dense_bulk_session_open = true;
                        errdefer if (dense_bulk_session_open) ctx.index_manager.abortDenseBulkIngestSessionByName(index_ref.name);
                    }
                    const before_hbc_profile = if (profile != null) ctx.index_manager.denseWriteProfileByName(index_ref.name) else null;
                    const batch_options: backend_types.BatchOptions = .{ .mode = .bulk_ingest };
                    const dense_delete_start_ns = monotonicTimeNs();
                    try ctx.index_manager.deleteDenseBatchByNameWithOptions(ctx.store, index_ref.name, batch.deleted_keys, batch_options);
                    try ctx.index_manager.deleteDenseBatchByNameWithOptions(ctx.store, index_ref.name, batch.overwritten_doc_keys, batch_options);
                    if (profile) |active_profile| DB.WritePathCallbacks.record_profile_ns(profile, &active_profile.dense_delete_ns, dense_delete_start_ns);

                    var dense_embeddings = try collectDenseEmbeddingWritesForBatch(
                        ctx.alloc,
                        ctx.index_manager,
                        batch.dense_embeddings,
                        batch.changed_artifact_keys,
                        index_ref.name,
                    );
                    defer dense_embeddings.deinit();

                    var dense_embedding_doc_keys = try denseEmbeddingDocKeySet(ctx.alloc, dense_embeddings.writes);
                    defer dense_embedding_doc_keys.deinit(ctx.alloc);

                    var index_writes = try collectDocumentWritesProfiled(
                        ctx.alloc,
                        ctx.store,
                        batch.documents,
                        ctx.index_manager.byte_range,
                        .{
                            .skip_doc_keys = &dense_embedding_doc_keys,
                            .relational_base_rows = ctx.relational_base_rows,
                        },
                        null,
                    );
                    defer index_writes.deinit();
                    if (index_writes.missing_required != 0) return error.ReplayDocumentNotVisible;
                    const dense_doc_index_start_ns = monotonicTimeNs();
                    try ctx.index_manager.indexDenseBatchByNameWithOptions(ctx.store, index_ref.name, index_writes.items, batch_options);
                    if (profile) |active_profile| DB.WritePathCallbacks.record_profile_ns(profile, &active_profile.dense_doc_index_ns, dense_doc_index_start_ns);

                    const dense_embedding_start_ns = monotonicTimeNs();
                    try ctx.index_manager.applyDenseEmbeddingWritesByNameWithOptions(ctx.store, index_ref.name, dense_embeddings.writes, batch_options);
                    if (profile) |active_profile| {
                        DB.WritePathCallbacks.record_profile_ns(profile, &active_profile.dense_embedding_apply_ns, dense_embedding_start_ns);
                        if (before_hbc_profile) |before| {
                            if (ctx.index_manager.denseWriteProfileByName(index_ref.name)) |after| {
                                addHbcWriteProfileDelta(active_profile, before, after);
                            }
                        }
                    }
                    if (use_local_bulk_session) {
                        try ctx.index_manager.finishDenseBulkIngestSessionByNameWithOptions(index_ref.name, dense_finish_options);
                        dense_bulk_session_open = false;
                    }
                    if (profile) |active_profile| DB.WritePathCallbacks.record_profile_ns(profile, &active_profile.dense_apply_ns, dense_apply_start_ns);
                },
                .sparse_vector => {
                    const apply_start_ns = monotonicTimeNs();
                    const emit_sparse_write_profile = DB.WritePathCallbacks.bench_metrics_enabled();
                    const before_sparse_profile = if (emit_sparse_write_profile) ctx.index_manager.sparseWriteProfileByName(index_ref.name) else null;
                    const batch_options: backend_types.BatchOptions = .{ .mode = .bulk_ingest };
                    var collect_doc_profile: CollectSparseFieldWritesProfile = .{};
                    var sparse_delete_ns: u64 = 0;
                    var sparse_collect_doc_ns: u64 = 0;
                    var sparse_doc_index_ns: u64 = 0;
                    var sparse_collect_embedding_ns: u64 = 0;
                    var sparse_embedding_apply_ns: u64 = 0;
                    const sparse_delete_start_ns = if (emit_sparse_write_profile) monotonicTimeNs() else 0;
                    try ctx.index_manager.deleteSparseBatchByNameWithOptions(index_ref.name, batch.deleted_keys, batch_options);
                    try ctx.index_manager.deleteSparseBatchByNameWithOptions(index_ref.name, batch.overwritten_doc_keys, batch_options);
                    if (emit_sparse_write_profile) sparse_delete_ns = monotonicTimeNs() - sparse_delete_start_ns;

                    const sparse_collect_embedding_start_ns = if (emit_sparse_write_profile) monotonicTimeNs() else 0;
                    var sparse_embeddings = try collectSparseEmbeddingWritesForBatch(
                        ctx.alloc,
                        ctx.index_manager,
                        batch.sparse_embeddings,
                        batch.changed_artifact_keys,
                        index_ref.name,
                    );
                    defer sparse_embeddings.deinit();
                    if (emit_sparse_write_profile) sparse_collect_embedding_ns = monotonicTimeNs() - sparse_collect_embedding_start_ns;

                    var sparse_embedding_doc_keys = try sparseEmbeddingDocKeySet(ctx.alloc, sparse_embeddings.writes);
                    defer sparse_embedding_doc_keys.deinit(ctx.alloc);

                    const sparse_collect_doc_start_ns = if (emit_sparse_write_profile) monotonicTimeNs() else 0;
                    const sparse_field_name = ctx.index_manager.sparseFieldNameByName(index_ref.name) orelse return error.IndexNotFound;
                    var index_writes = try collectSparseFieldWritesProfiled(
                        ctx.alloc,
                        ctx.store,
                        batch.documents,
                        ctx.index_manager.byte_range,
                        sparse_field_name,
                        .{
                            .prefer_inline_when_store_tip_matches_sequence = batch.sequence,
                            .prefer_available_inline_values = true,
                            .skip_doc_keys = &sparse_embedding_doc_keys,
                            .relational_base_rows = ctx.relational_base_rows,
                        },
                        if (emit_sparse_write_profile) &collect_doc_profile else null,
                    );
                    defer index_writes.deinit();
                    if (emit_sparse_write_profile) sparse_collect_doc_ns = monotonicTimeNs() - sparse_collect_doc_start_ns;
                    if (index_writes.missing_required != 0) return error.ReplayDocumentNotVisible;
                    const sparse_doc_index_start_ns = if (emit_sparse_write_profile) monotonicTimeNs() else 0;
                    try ctx.index_manager.indexSparsePreparedWritesByNameWithOptions(index_ref.name, index_writes.items, batch_options);
                    if (emit_sparse_write_profile) sparse_doc_index_ns = monotonicTimeNs() - sparse_doc_index_start_ns;

                    const sparse_embedding_apply_start_ns = if (emit_sparse_write_profile) monotonicTimeNs() else 0;
                    try ctx.index_manager.applySparseEmbeddingWritesByNameWithOptions(ctx.store, index_ref.name, sparse_embeddings.writes, batch_options);
                    if (emit_sparse_write_profile) sparse_embedding_apply_ns = monotonicTimeNs() - sparse_embedding_apply_start_ns;
                    if (before_sparse_profile) |before| {
                        if (ctx.index_manager.sparseWriteProfileByName(index_ref.name)) |after| {
                            logSparseWriteProfileDelta(index_ref.name, sparse_mod.WriteProfile.delta(after, before));
                        }
                    }
                    if (emit_sparse_write_profile) {
                        std.log.info(
                            "antfly_bench_sparse_doc_replay index={s} sequence={} documents={d} sparse_embeddings={d} total_ms={d} delete_ms={d} collect_doc_ms={d} collect_doc_scan_ms={d} collect_doc_sort_ms={d} collect_doc_read_ms={d} collect_doc_extract_ms={d} collect_doc_pending={d} collect_doc_output={d} collect_doc_store_hits={d} collect_doc_inline_hits={d} collect_doc_missing={d} collect_doc_no_vector={d} doc_index_ms={d} collect_embedding_ms={d} embedding_apply_ms={d}",
                            .{
                                index_ref.name,
                                batch.sequence,
                                batch.documents.len,
                                batch.sparse_embeddings.len,
                                nsToMs(monotonicTimeNs() - apply_start_ns),
                                nsToMs(sparse_delete_ns),
                                nsToMs(sparse_collect_doc_ns),
                                nsToMs(collect_doc_profile.scan_ns),
                                nsToMs(collect_doc_profile.sort_ns),
                                nsToMs(collect_doc_profile.read_ns),
                                nsToMs(collect_doc_profile.extract_ns),
                                collect_doc_profile.pending_documents,
                                collect_doc_profile.output_writes,
                                collect_doc_profile.store_hits,
                                collect_doc_profile.inline_hits,
                                collect_doc_profile.missing_required,
                                collect_doc_profile.skipped_without_vector,
                                nsToMs(sparse_doc_index_ns),
                                nsToMs(sparse_collect_embedding_ns),
                                nsToMs(sparse_embedding_apply_ns),
                            },
                        );
                    }
                    if (profile) |active_profile| DB.WritePathCallbacks.record_profile_ns(profile, &active_profile.sparse_apply_ns, apply_start_ns);
                },
                .graph => {
                    const apply_start_ns = monotonicTimeNs();
                    try ctx.index_manager.deleteGraphDocsByName(index_ref.name, batch.deleted_keys);
                    try applyGraphDocClearsForIndex(ctx, batch.graph_doc_clears, index_ref.name);

                    const materialized_artifact_keys = if (ctx.allow_graph_materialization)
                        try artifact_replay.materializeGraphSourceArtifactsForIndex(
                            ctx.alloc,
                            ctx.store,
                            ctx.index_manager,
                            batch.changed_artifact_keys,
                            index_ref.name,
                            .{
                                .relational_base_rows = ctx.relational_base_rows,
                                .require_resolution_contract = ctx.require_graph_resolution_contract,
                            },
                        )
                    else
                        try ctx.alloc.alloc([]u8, 0);
                    defer freeOwnedKeySlice(ctx.alloc, materialized_artifact_keys);

                    if (batch.graph_writes.len > 0 or batch.graph_deletes.len > 0) {
                        const graph_deletes = try collectGraphDeletes(ctx.alloc, batch.graph_deletes, index_ref.name);
                        defer if (graph_deletes.len > 0) ctx.alloc.free(graph_deletes);

                        const graph_writes = try collectGraphWrites(ctx.alloc, batch.graph_writes, index_ref.name);
                        defer if (graph_writes.len > 0) ctx.alloc.free(graph_writes);
                        try ctx.index_manager.applyGraphMutationsByName(index_ref.name, graph_writes, graph_deletes);
                    }
                    if (batch.changed_artifact_keys.len > 0 or materialized_artifact_keys.len > 0) {
                        const graph_artifact_keys = try concatArtifactKeyViews(ctx.alloc, batch.changed_artifact_keys, materialized_artifact_keys);
                        defer ctx.alloc.free(graph_artifact_keys);
                        var graph_mutations = try artifact_replay.collectGraphMutationsForArtifacts(ctx.alloc, ctx.store, graph_artifact_keys, index_ref.name);
                        defer graph_mutations.deinit();
                        try ctx.index_manager.applyGraphMutationsByName(index_ref.name, graph_mutations.writes, graph_mutations.deletes);
                    }
                    if (profile) |active_profile| DB.WritePathCallbacks.record_profile_ns(profile, &active_profile.graph_apply_ns, apply_start_ns);
                },
                .algebraic => {
                    try ctx.index_manager.applyAlgebraicBatchByNameWithOptions(ctx.store, index_ref.name, batch, .{ .mode = .bulk_ingest });
                },
            }
        }

        pub fn batchAffectsManagedIndexForReplay(
            index_manager: *index_manager_mod.IndexManager,
            batch: derived_types.DerivedBatch,
            index_ref: index_manager_mod.ManagedIndexRef,
        ) !bool {
            return switch (managedIndexBatchApplicability(index_manager, batch, index_ref)) {
                .irrelevant => false,
                .relevant => true,
                .missing_dependency => true,
            };
        }

        pub fn batchAffectsManagedIndexForDB(
            self: *DB,
            batch: derived_types.DerivedBatch,
            index_ref: index_manager_mod.ManagedIndexRef,
        ) bool {
            return batchAffectsManagedIndex(self.core.index_manager, batch, index_ref);
        }

        pub fn batchAffectsManagedIndexForReplayForDB(
            self: *DB,
            batch: derived_types.DerivedBatch,
            index_ref: index_manager_mod.ManagedIndexRef,
        ) !bool {
            return try batchAffectsManagedIndexForReplay(self.core.index_manager, batch, index_ref);
        }

        pub fn batchAdvancesManagedIndexApplyState(
            index_manager: *index_manager_mod.IndexManager,
            batch: derived_types.DerivedBatch,
            index_ref: index_manager_mod.ManagedIndexRef,
        ) !bool {
            if (!batchAffectsManagedIndex(index_manager, batch, index_ref)) return false;

            switch (index_ref.kind) {
                .dense_vector, .sparse_vector => {
                    if (batch.deleted_keys.len > 0 or batch.overwritten_doc_keys.len > 0) return true;
                    if (!try index_manager.requiresEnrichmentReplay(index_ref.name)) return true;
                    if (batch.changed_artifact_keys.len > 0) return true;
                    if (index_ref.kind == .dense_vector) {
                        for (batch.dense_embeddings) |embedding| {
                            if (std.mem.eql(u8, embedding.index_name, index_ref.name)) return true;
                        }
                        return false;
                    }
                    for (batch.sparse_embeddings) |embedding| {
                        if (std.mem.eql(u8, embedding.index_name, index_ref.name)) return true;
                    }
                    return false;
                },
                else => return true,
            }
        }

        pub fn batchAdvancesManagedIndexApplyStateForReplay(
            index_manager: *index_manager_mod.IndexManager,
            batch: derived_types.DerivedBatch,
            index_ref: index_manager_mod.ManagedIndexRef,
        ) !bool {
            return switch (managedIndexBatchApplicability(index_manager, batch, index_ref)) {
                .irrelevant => false,
                .missing_dependency => true,
                .relevant => try batchAdvancesManagedIndexApplyState(index_manager, batch, index_ref),
            };
        }

        pub fn batchAdvancesManagedIndexApplyStateForReplayForDB(
            self: *DB,
            batch: derived_types.DerivedBatch,
            index_ref: index_manager_mod.ManagedIndexRef,
        ) !bool {
            return try batchAdvancesManagedIndexApplyStateForReplay(self.core.index_manager, batch, index_ref);
        }

        pub fn batchAffectsManagedIndex(
            index_manager: *index_manager_mod.IndexManager,
            batch: derived_types.DerivedBatch,
            index_ref: index_manager_mod.ManagedIndexRef,
        ) bool {
            return managedIndexBatchApplicability(index_manager, batch, index_ref) == .relevant;
        }

        pub fn resolverConfigForResolution(
            index_manager: *index_manager_mod.IndexManager,
            source_artifact: []const u8,
            resolution_artifact: []const u8,
        ) ?*const index_manager_mod.ResolverConfig {
            for (index_manager.resolvers.items) |*cfg| {
                if (std.mem.eql(u8, cfg.source_artifact, source_artifact) and
                    std.mem.eql(u8, cfg.resolution_artifact, resolution_artifact))
                {
                    return cfg;
                }
            }
            return null;
        }

        pub fn resolverConfigForResolutionArtifact(
            index_manager: *index_manager_mod.IndexManager,
            resolution_artifact: []const u8,
        ) ?*const index_manager_mod.ResolverConfig {
            for (index_manager.resolvers.items) |*cfg| {
                if (std.mem.eql(u8, cfg.resolution_artifact, resolution_artifact)) return cfg;
            }
            return null;
        }

        pub fn applyDerivedBatchProfiled(self: *DB, batch: derived_types.DerivedBatch, profile: ?*BatchProfile) !void {
            var ctx = self.batchContext();
            try applyDerivedBatchContextProfiled(&ctx, batch, profile);
            if (self.text_merge_runtime) |runtime| {
                if (self.async_context.text_merge_deferred.load(.acquire)) return;
                runtime.notify();
                runtime.applyBackpressure();
            }
            if (self.sparse_compaction_runtime) |runtime| runtime.notify();
            if (self.graph_metric_runtime) |runtime| runtime.notify();
        }

        pub fn applyDerivedBatch(self: *DB, batch: derived_types.DerivedBatch) !void {
            try applyDerivedBatchProfiled(self, batch, null);
        }

        pub fn applyDerivedBatchTargetsProfiled(self: *DB, batch: derived_types.DerivedBatch, index_names: []const []const u8, profile: ?*BatchProfile) !void {
            var ctx = self.batchContext();
            try applyDerivedBatchTargetsContextProfiled(&ctx, batch, index_names, profile);
            if (self.text_merge_runtime) |runtime| {
                if (self.async_context.text_merge_deferred.load(.acquire)) return;
                runtime.notify();
                runtime.applyBackpressure();
            }
            if (self.sparse_compaction_runtime) |runtime| runtime.notify();
            if (self.graph_metric_runtime) |runtime| runtime.notify();
        }

        pub fn applyDerivedBatchTargets(self: *DB, batch: derived_types.DerivedBatch, index_names: []const []const u8) !void {
            try applyDerivedBatchTargetsProfiled(self, batch, index_names, null);
        }

        pub fn applyDerivedBatchContext(ctx: *const BatchExecutionContext, batch: derived_types.DerivedBatch) !void {
            try applyDerivedBatchContextProfiled(ctx, batch, null);
        }

        pub fn applyDerivedBatchTargetsContext(ctx: *const BatchExecutionContext, batch: derived_types.DerivedBatch, index_names: []const []const u8) !void {
            try applyDerivedBatchTargetsContextProfiled(ctx, batch, index_names, null);
        }

        pub fn applyDerivedBatchContextProfiled(ctx: *const BatchExecutionContext, batch: derived_types.DerivedBatch, profile: ?*BatchProfile) !void {
            try applyDerivedBatchTargetsContextProfiled(ctx, batch, &.{}, profile);
        }

        pub fn applyDerivedBatchTargetsContextProfiled(ctx: *const BatchExecutionContext, batch: derived_types.DerivedBatch, index_names: []const []const u8, profile: ?*BatchProfile) !void {
            const managed_indexes = try ctx.index_manager.managedIndexes(ctx.alloc);
            defer {
                for (managed_indexes) |index_ref| ctx.alloc.free(@constCast(index_ref.name));
                ctx.alloc.free(managed_indexes);
            }
            var updates = std.ArrayListUnmanaged(apply_state.AppliedSequenceUpdate).empty;
            defer updates.deinit(ctx.alloc);
            for (managed_indexes) |index_ref| {
                if (index_names.len != 0 and !indexNameInSlice(index_ref.name, index_names)) continue;
                if (try batchAdvancesManagedIndexApplyState(ctx.index_manager, batch, index_ref)) {
                    const async_ctx = AsyncContext{
                        .alloc = ctx.alloc,
                        .store = ctx.store,
                        .applied_sequence_checkpoint_path = ctx.applied_sequence_checkpoint_path,
                        .index_manager = ctx.index_manager,
                        .apply_mutex = ctx.apply_mutex,
                        .dense_bulk_session_scope = ctx.dense_bulk_session_scope,
                        .relational_base_rows = ctx.relational_base_rows,
                    };
                    try DB.DerivedAsyncCallbacks.apply_derived_batch_to_index_context_profiled(&async_ctx, batch, index_ref, profile);
                    const index_sync_start_ns = monotonicTimeNs();
                    try ctx.index_manager.syncReplayStateByName(ctx.store, index_ref.name);
                    if (profile) |active_profile| DB.WritePathCallbacks.record_profile_ns(profile, &active_profile.index_sync_ns, index_sync_start_ns);
                    try updates.append(ctx.alloc, .{
                        .index_name = index_ref.name,
                        .sequence = batch.sequence,
                    });
                }
            }
            const applied_sequence_start_ns = monotonicTimeNs();
            try saveAppliedSequencesBatchContext(ctx, updates.items);
            if (profile) |active_profile| DB.WritePathCallbacks.record_profile_ns(profile, &active_profile.applied_sequence_save_ns, applied_sequence_start_ns);
            const truncate_start_ns = monotonicTimeNs();
            try truncateReplayJournalIfSafeContext(ctx);
            if (profile) |active_profile| DB.WritePathCallbacks.record_profile_ns(profile, &active_profile.replay_journal_truncate_ns, truncate_start_ns);
        }

        pub fn replayPendingDerivedBatchesContext(ctx: *const BatchExecutionContext) !void {
            const managed_indexes = try ctx.index_manager.managedIndexes(ctx.alloc);
            defer {
                for (managed_indexes) |index_ref| ctx.alloc.free(@constCast(index_ref.name));
                ctx.alloc.free(managed_indexes);
            }
            if (managed_indexes.len == 0) return;

            var saw_entries = false;
            var updates = std.ArrayListUnmanaged(apply_state.AppliedSequenceUpdate).empty;
            defer updates.deinit(ctx.alloc);
            var replay_ctx = ReplayApplyContextBatch{
                .batch = ctx,
                .dense_bulk_session_scope = .external,
            };
            for (managed_indexes) |index_ref| {
                const applied = try apply_state.loadAppliedSequenceWithCheckpoint(
                    ctx.alloc,
                    ctx.store,
                    ctx.applied_sequence_checkpoint_path,
                    index_ref.name,
                );
                const target_sequence = try ctx.replay_source.latestMatchingSequence(
                    ctx.alloc,
                    applied,
                    derived_worker.targetHintForManagedIndex(index_ref),
                );
                const use_dense_maintenance = index_ref.kind == .dense_vector;
                const stats = try derived_worker.catchUpIndexWithOptions(
                    ctx.alloc,
                    ctx.replay_source,
                    index_ref,
                    applied,
                    &replay_ctx,
                    applyDerivedBatchToIndexReplayContext,
                    .{
                        .resource_manager = ctx.index_manager.resource_manager,
                        .window_ctx = &replay_ctx,
                        .begin_window_fn = beginDerivedCatchUpWindowContext,
                        .finish_window_fn = finishDerivedCatchUpWindowContext,
                        .target_sequence = target_sequence,
                    },
                );
                if (DB.DerivedAsyncCallbacks.open_profile_enabled()) DB.DerivedAsyncCallbacks.log_replay_catch_up_profile(index_ref, applied, stats);
                if (use_dense_maintenance) {
                    _ = try ctx.index_manager.runDenseLsmMaintenanceByName(index_ref.name, denseCatchUpMaintenanceSteps());
                }
                saw_entries = saw_entries or stats.scanned_entries > 0;
                if (stats.appliedSequenceAdvance(applied)) |sequence| {
                    try ctx.index_manager.checkpointLsmWalForManagedIndex(index_ref);
                    try updates.append(ctx.alloc, .{
                        .index_name = index_ref.name,
                        .sequence = sequence,
                    });
                } else if (stats.shouldTryTargetAdvance(applied, target_sequence) and
                    try canAdvanceDerivedReplayTargetContext(ctx, index_ref, applied, target_sequence))
                {
                    try ctx.index_manager.checkpointLsmWalForManagedIndex(index_ref);
                    try updates.append(ctx.alloc, .{
                        .index_name = index_ref.name,
                        .sequence = target_sequence,
                    });
                }
            }
            try saveAppliedSequencesBatchContext(ctx, updates.items);
            try truncateReplayJournalIfSafeContext(ctx);
        }

        fn canAdvanceDerivedReplayTargetContext(
            ctx: *const BatchExecutionContext,
            index_ref: index_manager_mod.ManagedIndexRef,
            from_sequence: u64,
            target_sequence: u64,
        ) !bool {
            if (target_sequence <= from_sequence) return true;
            if (ctx.async_context) |async_ctx| {
                return try canAdvanceDerivedToTargetAsync(async_ctx, index_ref, from_sequence, target_sequence);
            }
            var async_ctx = AsyncContext{
                .alloc = ctx.alloc,
                .io = ctx.io,
                .store = ctx.store,
                .applied_sequence_checkpoint_path = ctx.applied_sequence_checkpoint_path,
                .index_manager = ctx.index_manager,
                .apply_mutex = ctx.apply_mutex,
                .dense_bulk_session_scope = ctx.dense_bulk_session_scope,
                .resolution_runtime = ctx.resolution_runtime,
                .promotion_runtime = ctx.promotion_runtime,
            };
            defer async_ctx.deinit(ctx.alloc);
            return try canAdvanceDerivedToTargetAsync(&async_ctx, index_ref, from_sequence, target_sequence);
        }

        pub fn replayPendingDerivedBatches(self: *DB, progress_ctx: ?*anyopaque, progress_hook: ?db_internal.ReplayProgressHook) !void {
            if (!self.core.hasManagedIndexes()) return;

            const dense_catch_up_watchdog_interval_ns = 5 * std.time.ns_per_s;

            const PersistReplayProgressContext = struct {
                db: *DB,
                progress_ctx: ?*anyopaque,
                progress_hook: ?db_internal.ReplayProgressHook,
                track_dense: bool = false,
                target_sequence: u64 = 0,
                scanned_entries: u64 = 0,
                applied_entries: u64 = 0,
                replay_scan_batches: u64 = 0,
                replay_hint_filter_skips: u64 = 0,
                active: bool = false,

                fn publish(state: *@This(), index_name: []const u8) !void {
                    const progress: db_internal.ReplayProgress = .{
                        .sequence = try state.db.core.loadAppliedSequence(state.db.alloc, index_name),
                        .target_sequence = state.target_sequence,
                        .scanned_entries = state.scanned_entries,
                        .applied_entries = state.applied_entries,
                        .replay_scan_batches = state.replay_scan_batches,
                        .replay_hint_filter_skips = state.replay_hint_filter_skips,
                        .active = state.active,
                    };
                    if (state.track_dense) setDenseCatchUpProgress(state.db.async_context, progress);
                    if (state.progress_hook) |hook| {
                        try hook(state.progress_ctx.?, index_name, progress);
                    }
                }
            };

            const PersistReplayProgress = struct {
                fn run(ctx: *anyopaque, index_name: []const u8, sequence: u64) !void {
                    const progress: *PersistReplayProgressContext = @ptrCast(@alignCast(ctx));
                    try progress.db.core.saveAppliedSequence(index_name, sequence);
                    try progress.publish(index_name);
                }
            };

            const DenseReplayProgressContext = struct {
                db: *DB,
                persist: *PersistReplayProgressContext,
                index_name: []const u8,
                last_log_ns: u64 = 0,
                last_applied_entries: u64 = 0,

                fn maybeLogStall(replay: *@This(), progress: derived_worker.CatchUpProgress) void {
                    if (!replay.persist.active) return;
                    const now_ns = monotonicTimeNs();
                    if (replay.last_log_ns != 0 and now_ns - replay.last_log_ns < dense_catch_up_watchdog_interval_ns) return;
                    replay.last_log_ns = now_ns;

                    const dense_entry = replay.db.core.index_manager.denseIndex(replay.index_name) orelse return;
                    const profile = dense_entry.index.getWriteProfile();
                    const hbc_cache = dense_entry.index.hbcCacheStats();
                    const resource_snapshot = if (replay.db.core.index_manager.resource_manager) |manager| manager.snapshot() else null;

                    std.log.warn(
                        "dense catch-up watchdog index={s} sequence={} target={} scanned={} applied={} delta_applied={} cache_caps={{nodes={},vectors={}}} hbc={{total={},accounted={},vector_used={},metadata_used={}}} rm={{lsm_cache={},hbc_cache={},dense_search={},dense_apply={},replay_window={},full_text_pending={},full_text_build={}}} profile={{find_leaf_ns={},mutate_leaf_ns={},store_vector_ns={},quantized_vector_load_ns={}}}",
                        .{
                            replay.index_name,
                            progress.sequence,
                            replay.persist.target_sequence,
                            progress.scanned_entries,
                            progress.applied_entries,
                            progress.applied_entries -| replay.last_applied_entries,
                            dense_entry.index.config.max_cached_nodes,
                            dense_entry.index.config.max_cached_vectors,
                            hbc_cache.total_bytes,
                            hbc_cache.accounted_bytes,
                            hbc_cache.vector.used_bytes,
                            hbc_cache.metadata.used_bytes,
                            if (resource_snapshot) |stats| stats.slices[@intFromEnum(resource_manager_mod.Slice.lsm_block_table_cache)].used_bytes else 0,
                            if (resource_snapshot) |stats| stats.slices[@intFromEnum(resource_manager_mod.Slice.hbc_node_metadata_cache)].used_bytes else 0,
                            if (resource_snapshot) |stats| stats.slices[@intFromEnum(resource_manager_mod.Slice.dense_search_working_set)].used_bytes else 0,
                            if (resource_snapshot) |stats| stats.slices[@intFromEnum(resource_manager_mod.Slice.dense_apply_working_set)].used_bytes else 0,
                            if (resource_snapshot) |stats| stats.slices[@intFromEnum(resource_manager_mod.Slice.derived_replay_window)].used_bytes else 0,
                            if (resource_snapshot) |stats| stats.slices[@intFromEnum(resource_manager_mod.Slice.full_text_pending_segments)].used_bytes else 0,
                            if (resource_snapshot) |stats| stats.slices[@intFromEnum(resource_manager_mod.Slice.full_text_build_working_set)].used_bytes else 0,
                            profile.insert_find_leaf_ns,
                            profile.insert_mutate_leaf_ns,
                            profile.insert_store_vector_ns,
                            profile.quantized_vector_load_ns,
                        },
                    );
                    replay.last_applied_entries = progress.applied_entries;
                }

                fn run(ctx: *anyopaque, index_name: []const u8, progress: derived_worker.CatchUpProgress) !void {
                    const replay: *@This() = @ptrCast(@alignCast(ctx));
                    replay.persist.scanned_entries = progress.scanned_entries;
                    replay.persist.applied_entries = progress.applied_entries;
                    replay.persist.replay_scan_batches = progress.replay_scan_batches;
                    replay.persist.replay_hint_filter_skips = progress.replay_hint_filter_skips;
                    const snapshot: db_internal.ReplayProgress = .{
                        .sequence = progress.sequence,
                        .target_sequence = replay.persist.target_sequence,
                        .scanned_entries = progress.scanned_entries,
                        .applied_entries = progress.applied_entries,
                        .replay_scan_batches = progress.replay_scan_batches,
                        .replay_hint_filter_skips = progress.replay_hint_filter_skips,
                        .active = replay.persist.active,
                    };
                    setDenseCatchUpProgress(replay.db.async_context, snapshot);
                    replay.maybeLogStall(progress);
                    if (replay.persist.progress_hook) |hook| {
                        try hook(replay.persist.progress_ctx.?, index_name, snapshot);
                    }
                }
            };

            var persist_progress_ctx = PersistReplayProgressContext{
                .db = self,
                .progress_ctx = progress_ctx,
                .progress_hook = progress_hook,
            };

            const managed_indexes = try self.core.managedIndexes(self.alloc);
            defer {
                for (managed_indexes) |index_ref| self.alloc.free(@constCast(index_ref.name));
                self.alloc.free(managed_indexes);
            }
            if (managed_indexes.len == 0) return;

            var replay_ctx = ReplayApplyContext{
                .db = self,
                .dense_bulk_session_scope = .external,
            };
            for (managed_indexes) |index_ref| {
                const applied = try self.core.loadAppliedSequence(self.alloc, index_ref.name);
                const use_dense_catch_up = index_ref.kind == .dense_vector;
                const use_startup_dense_caps = use_dense_catch_up and progress_hook != null;
                const resources = self.core.batchExecutionResources();
                const target_sequence = try DB.LifecycleCallbacks.probe_derived_replay_target_sequence(
                    self,
                    self.alloc,
                    resources.replay_source,
                    index_ref,
                    applied,
                );
                var dense_cache_caps_before: ?struct { nodes: usize, vectors: usize } = null;
                if (use_startup_dense_caps) {
                    const dense_entry = resources.index_manager.denseIndex(index_ref.name) orelse return error.IndexNotFound;
                    dense_cache_caps_before = .{
                        .nodes = dense_entry.index.config.max_cached_nodes,
                        .vectors = dense_entry.index.config.max_cached_vectors,
                    };
                    dense_entry.index.setCacheCaps(
                        @min(dense_cache_caps_before.?.nodes, denseCatchUpStartupCacheNodes()),
                        @min(dense_cache_caps_before.?.vectors, denseCatchUpStartupCacheVectors()),
                    );
                }
                defer if (dense_cache_caps_before) |caps| {
                    if (resources.index_manager.denseIndex(index_ref.name)) |dense_entry| {
                        dense_entry.index.setCacheCaps(caps.nodes, caps.vectors);
                    }
                };
                persist_progress_ctx.target_sequence = target_sequence;
                persist_progress_ctx.scanned_entries = 0;
                persist_progress_ctx.applied_entries = 0;
                persist_progress_ctx.track_dense = use_dense_catch_up;
                persist_progress_ctx.active = use_dense_catch_up and target_sequence > applied;
                if (use_dense_catch_up) {
                    setDenseCatchUpProgress(self.async_context, .{
                        .sequence = applied,
                        .target_sequence = target_sequence,
                        .scanned_entries = 0,
                        .applied_entries = 0,
                        .active = persist_progress_ctx.active,
                    });
                    if (progress_hook) |hook| {
                        try hook(progress_ctx.?, index_ref.name, .{
                            .sequence = applied,
                            .target_sequence = target_sequence,
                            .scanned_entries = 0,
                            .applied_entries = 0,
                            .active = persist_progress_ctx.active,
                        });
                    }
                }
                var dense_replay_progress_ctx = DenseReplayProgressContext{
                    .db = self,
                    .persist = &persist_progress_ctx,
                    .index_name = index_ref.name,
                    .last_log_ns = monotonicTimeNs(),
                };
                const stats = try derived_worker.catchUpIndexWithOptions(
                    self.alloc,
                    resources.replay_source,
                    index_ref,
                    applied,
                    &replay_ctx,
                    applyDerivedBatchToIndexReplay,
                    .{
                        .resource_manager = resources.index_manager.resource_manager,
                        .window_ctx = &replay_ctx,
                        .begin_window_fn = beginDerivedCatchUpWindow,
                        .finish_window_fn = finishDerivedCatchUpWindow,
                        .progress_ctx = if (use_dense_catch_up) &dense_replay_progress_ctx else null,
                        .progress_fn = if (use_dense_catch_up) DenseReplayProgressContext.run else null,
                        .persist_ctx = &persist_progress_ctx,
                        .persist_progress_fn = PersistReplayProgress.run,
                        .max_records_per_window = if (progress_hook != null and use_dense_catch_up) denseCatchUpStartupMaxRecords() else derived_worker.catch_up_max_records_per_window_default,
                        .max_chunk_bytes = if (progress_hook != null and use_dense_catch_up) denseCatchUpStartupMaxChunkBytes() else derived_worker.catch_up_max_chunk_bytes_default,
                        .target_sequence = target_sequence,
                    },
                );
                if (DB.DerivedAsyncCallbacks.open_profile_enabled()) DB.DerivedAsyncCallbacks.log_replay_catch_up_profile(index_ref, applied, stats);
                if (use_dense_catch_up) {
                    _ = try resources.index_manager.runDenseLsmMaintenanceByName(index_ref.name, denseCatchUpMaintenanceSteps());
                    persist_progress_ctx.active = false;
                    const final_progress: db_internal.ReplayProgress = .{
                        .sequence = @max(stats.last_sequence, applied),
                        .target_sequence = target_sequence,
                        .scanned_entries = @intCast(stats.scanned_entries),
                        .applied_entries = @intCast(stats.applied_entries),
                        .active = false,
                    };
                    setDenseCatchUpProgress(self.async_context, final_progress);
                    if (progress_hook) |hook| {
                        try hook(progress_ctx.?, index_ref.name, final_progress);
                    }
                }
                if (stats.appliedSequenceAdvance(applied)) |sequence| {
                    try self.core.saveAppliedSequence(index_ref.name, sequence);
                    try resources.index_manager.checkpointLsmWalForManagedIndex(index_ref);
                } else if (stats.shouldTryTargetAdvance(applied, target_sequence) and
                    try canAdvanceDerivedToTargetAsync(self.async_context, index_ref, applied, target_sequence))
                {
                    try self.core.saveAppliedSequence(index_ref.name, target_sequence);
                    try resources.index_manager.checkpointLsmWalForManagedIndex(index_ref);
                }
            }
            var truncate_ctx = self.batchContext();
            try truncateReplayJournalIfSafeContext(&truncate_ctx);
        }

        pub fn truncateReplayJournalIfSafe(self: *DB) !void {
            var ctx = self.batchContext();
            return try truncateReplayJournalIfSafeContext(&ctx);
        }

        pub fn applyDerivedBatchToShadowIfNeeded(self: *DB, batch: derived_types.DerivedBatch) !void {
            const shadow = self.shadow orelse return;
            const state = self.core.splitState() orelse return;
            if (state.phase != .splitting) return;

            const managed_indexes = try shadow.manager.managedIndexes(self.alloc);
            defer {
                for (managed_indexes) |index_ref| self.alloc.free(@constCast(index_ref.name));
                self.alloc.free(managed_indexes);
            }

            const async_resources = self.core.asyncResources();
            const ctx = AsyncContext{
                .alloc = self.alloc,
                .store = async_resources.store,
                .applied_sequence_checkpoint_path = async_resources.applied_sequence_checkpoint_path,
                .index_manager = shadow.manager,
                .apply_mutex = async_resources.apply_mutex,
                .allow_graph_materialization = false,
                .relational_base_rows = self.relationalColumnsForStore() != null,
            };

            for (managed_indexes) |index_ref| {
                if (!batchAffectsManagedIndex(shadow.manager, batch, index_ref)) continue;
                try applyDerivedBatchToIndexContext(&ctx, batch, index_ref);
            }
        }

        pub fn applyDerivedBatchToIndexReplay(ctx_ptr: *anyopaque, batch: derived_types.DerivedBatch, index_ref: index_manager_mod.ManagedIndexRef) !bool {
            const replay_ctx: *ReplayApplyContext = @ptrCast(@alignCast(ctx_ptr));
            const self = replay_ctx.db;
            const resources = self.core.asyncResources();
            if (!try batchAdvancesManagedIndexApplyStateForReplay(resources.index_manager, batch, index_ref)) return false;
            const batch_ctx = self.batchContext();
            const ctx = AsyncContext{
                .alloc = self.alloc,
                .store = resources.store,
                .applied_sequence_checkpoint_path = resources.applied_sequence_checkpoint_path,
                .index_manager = resources.index_manager,
                .apply_mutex = resources.apply_mutex,
                .dense_bulk_session_scope = replay_ctx.dense_bulk_session_scope,
                .relational_base_rows = batch_ctx.relational_base_rows,
                .require_graph_resolution_contract = true,
            };
            try DB.DerivedAsyncCallbacks.apply_derived_batch_to_index_context(&ctx, batch, index_ref);
            return true;
        }

        fn beginDerivedCatchUpWindow(ctx_ptr: *anyopaque, index_ref: index_manager_mod.ManagedIndexRef) !void {
            if (index_ref.kind != .dense_vector) return;
            const replay_ctx: *ReplayApplyContext = @ptrCast(@alignCast(ctx_ptr));
            try replay_ctx.db.core.batchExecutionResources().index_manager.beginDenseBulkIngestSessionByName(index_ref.name);
        }

        fn finishDerivedCatchUpWindow(ctx_ptr: *anyopaque, index_ref: index_manager_mod.ManagedIndexRef, success: bool) !void {
            if (index_ref.kind != .dense_vector) return;
            const replay_ctx: *ReplayApplyContext = @ptrCast(@alignCast(ctx_ptr));
            const resources = replay_ctx.db.core.batchExecutionResources();
            if (!success) {
                resources.index_manager.abortDenseBulkIngestSessionByName(index_ref.name);
                return;
            }
            errdefer resources.index_manager.abortDenseBulkIngestSessionByName(index_ref.name);
            const finish_start_ns = monotonicTimeNs();
            try resources.index_manager.finishDenseBulkIngestSessionByNameWithOptions(index_ref.name, DB.DerivedAsyncCallbacks.dense_catch_up_finish_options());
            try resources.index_manager.checkpointLsmWalForManagedIndex(index_ref);
            if (resources.index_manager.resource_manager) |manager| {
                manager.noteDenseReplayWindowResult(.{ .finish_ns = elapsedSince(finish_start_ns) });
            }
        }

        fn applyDerivedBatchToIndexReplayContext(
            ctx_ptr: *anyopaque,
            batch: derived_types.DerivedBatch,
            index_ref: index_manager_mod.ManagedIndexRef,
        ) !bool {
            const replay_ctx: *const ReplayApplyContextBatch = @ptrCast(@alignCast(ctx_ptr));
            const ctx = replay_ctx.batch;
            if (!try batchAdvancesManagedIndexApplyStateForReplay(ctx.index_manager, batch, index_ref)) return false;

            const async_ctx = AsyncContext{
                .alloc = ctx.alloc,
                .store = ctx.store,
                .applied_sequence_checkpoint_path = ctx.applied_sequence_checkpoint_path,
                .index_manager = ctx.index_manager,
                .apply_mutex = ctx.apply_mutex,
                .dense_bulk_session_scope = replay_ctx.dense_bulk_session_scope,
                .relational_base_rows = ctx.relational_base_rows,
                .require_graph_resolution_contract = true,
            };
            if (DB.WritePathCallbacks.bench_metrics_enabled()) {
                var profile = BatchProfile{};
                const total_start_ns = monotonicTimeNs();
                try DB.DerivedAsyncCallbacks.apply_derived_batch_to_index_context_profiled(&async_ctx, batch, index_ref, &profile);
                const index_sync_start_ns = monotonicTimeNs();
                try ctx.index_manager.syncReplayStateByName(ctx.store, index_ref.name);
                DB.WritePathCallbacks.record_profile_ns(&profile, &profile.index_sync_ns, index_sync_start_ns);
                profile.total_ns += monotonicTimeNs() - total_start_ns;
                DB.DerivedAsyncCallbacks.log_derived_worker_profile(index_ref, batch, profile);
            } else {
                try DB.DerivedAsyncCallbacks.apply_derived_batch_to_index_context(&async_ctx, batch, index_ref);
                try ctx.index_manager.syncReplayStateByName(ctx.store, index_ref.name);
            }
            return true;
        }

        fn beginDerivedCatchUpWindowContext(ctx_ptr: *anyopaque, index_ref: index_manager_mod.ManagedIndexRef) !void {
            if (index_ref.kind != .dense_vector) return;
            const replay_ctx: *ReplayApplyContextBatch = @ptrCast(@alignCast(ctx_ptr));
            try replay_ctx.batch.index_manager.beginDenseBulkIngestSessionByName(index_ref.name);
        }

        fn finishDerivedCatchUpWindowContext(ctx_ptr: *anyopaque, index_ref: index_manager_mod.ManagedIndexRef, success: bool) !void {
            if (index_ref.kind != .dense_vector) return;
            const replay_ctx: *ReplayApplyContextBatch = @ptrCast(@alignCast(ctx_ptr));
            if (!success) {
                replay_ctx.batch.index_manager.abortDenseBulkIngestSessionByName(index_ref.name);
                return;
            }
            errdefer replay_ctx.batch.index_manager.abortDenseBulkIngestSessionByName(index_ref.name);
            const finish_start_ns = monotonicTimeNs();
            try replay_ctx.batch.index_manager.finishDenseBulkIngestSessionByNameWithOptions(index_ref.name, DB.DerivedAsyncCallbacks.dense_catch_up_finish_options());
            try replay_ctx.batch.index_manager.checkpointLsmWalForManagedIndex(index_ref);
            if (replay_ctx.batch.index_manager.resource_manager) |manager| {
                manager.noteDenseReplayWindowResult(.{ .finish_ns = elapsedSince(finish_start_ns) });
            }
        }

        fn shouldDeferBacklogPressureForExternalDenseBulk(ctx: *const BatchExecutionContext, sync_level: types.SyncLevel) bool {
            switch (sync_level) {
                .propose, .write, .enrichments => {},
                .full_text, .aknn, .full_index => return false,
            }
            const async_context = ctx.async_context orelse return false;
            return async_context.active_external_dense_bulk_sessions.load(.acquire) != 0;
        }

        const ManagedIndexBatchApplicability = enum {
            irrelevant,
            relevant,
            missing_dependency,
        };

        fn managedIndexBatchApplicability(
            index_manager: *index_manager_mod.IndexManager,
            batch: derived_types.DerivedBatch,
            index_ref: index_manager_mod.ManagedIndexRef,
        ) ManagedIndexBatchApplicability {
            switch (index_ref.kind) {
                .full_text, .algebraic => {
                    if (batch.deleted_keys.len > 0 or batch.overwritten_doc_keys.len > 0) return .relevant;
                    for (batch.documents) |doc| {
                        if (doc.action == .upsert) return .relevant;
                    }
                    return .irrelevant;
                },
                .dense_vector => {
                    if (batch.deleted_keys.len > 0 or batch.overwritten_doc_keys.len > 0) return .relevant;
                    for (batch.documents) |doc| {
                        if (doc.action == .upsert) return .relevant;
                    }
                    for (batch.dense_embeddings) |embedding| {
                        if (std.mem.eql(u8, embedding.index_name, index_ref.name)) return .relevant;
                    }
                    if (batchHasEmbeddingArtifactForManagedIndex(index_manager, index_ref, batch.changed_artifact_keys)) return .relevant;
                    return .irrelevant;
                },
                .sparse_vector => {
                    if (batch.deleted_keys.len > 0 or batch.overwritten_doc_keys.len > 0) return .relevant;
                    for (batch.documents) |doc| {
                        if (doc.action == .upsert) return .relevant;
                    }
                    for (batch.sparse_embeddings) |embedding| {
                        if (std.mem.eql(u8, embedding.index_name, index_ref.name)) return .relevant;
                    }
                    if (batchHasEmbeddingArtifactForManagedIndex(index_manager, index_ref, batch.changed_artifact_keys)) return .relevant;
                    return .irrelevant;
                },
                .graph => {
                    if (batch.deleted_keys.len > 0) return .relevant;
                    for (batch.graph_doc_clears) |clear| {
                        for (clear.index_names) |index_name| {
                            if (std.mem.eql(u8, index_name, index_ref.name)) return .relevant;
                        }
                    }
                    for (batch.graph_writes) |write| {
                        if (std.mem.eql(u8, write.index_name, index_ref.name)) return .relevant;
                    }
                    for (batch.graph_deletes) |delete| {
                        if (std.mem.eql(u8, delete.index_name, index_ref.name)) return .relevant;
                    }
                    for (batch.changed_artifact_keys) |artifact_key| {
                        if (internal_keys.isAssetArtifactKey(artifact_key)) {
                            var artifact_ref = (artifact_ids.decodeArtifactRefAlloc(index_manager.alloc, artifact_key) catch continue) orelse continue;
                            defer artifact_ref.deinit(index_manager.alloc);
                            if (artifact_ref.kind == .asset and index_manager.graphIndexConsumesAssetArtifact(index_ref.name, artifact_ref.name)) return .relevant;
                            continue;
                        }
                        if (internal_keys.isResolutionArtifactKey(artifact_key)) {
                            const source = index_manager.graphArtifactSource(index_ref.name) orelse continue;
                            if (source.mention_edge_type.len == 0) continue;
                            const parsed = (internal_keys.parseResolutionArtifactKeyAlloc(index_manager.alloc, artifact_key) catch continue) orelse continue;
                            defer {
                                index_manager.alloc.free(parsed.doc_key);
                                index_manager.alloc.free(parsed.artifact_name);
                            }
                            if (resolverConfigForResolution(index_manager, source.artifact_name, parsed.artifact_name) != null) return .relevant;
                            if (resolverConfigForResolutionArtifact(index_manager, parsed.artifact_name) != null) continue;
                            return .missing_dependency;
                        }
                        if (internal_keys.isGraphEdgeArtifactKey(artifact_key)) {
                            const parsed = (internal_keys.parseGraphEdgeArtifactKeyAlloc(index_manager.alloc, artifact_key) catch continue) orelse continue;
                            defer {
                                index_manager.alloc.free(parsed.doc_key);
                                index_manager.alloc.free(parsed.index_name);
                                index_manager.alloc.free(parsed.edge_type);
                                index_manager.alloc.free(parsed.target_doc_key);
                            }
                            if (std.mem.eql(u8, parsed.index_name, index_ref.name)) return .relevant;
                        }
                    }
                    return .irrelevant;
                },
            }
        }

        fn batchHasEmbeddingArtifactForManagedIndex(
            index_manager: *index_manager_mod.IndexManager,
            index_ref: index_manager_mod.ManagedIndexRef,
            artifact_keys: []const []const u8,
        ) bool {
            const alloc = index_manager.alloc;
            const expected_embedding_name = switch (index_ref.kind) {
                .dense_vector => index_manager.denseEmbeddingName(index_ref.name) orelse index_ref.name,
                .sparse_vector => index_manager.sparseEmbeddingName(index_ref.name) orelse index_ref.name,
                else => return false,
            };

            for (artifact_keys) |artifact_key| {
                var identity = artifact_ids.decodeEmbeddingArtifactIdentityAlloc(alloc, artifact_key) catch |err| switch (err) {
                    error.InvalidInternalUserKey => continue,
                    else => continue,
                } orelse continue;
                defer identity.deinit(alloc);
                if (std.mem.eql(u8, identity.embedding_name, expected_embedding_name)) return true;
            }

            return false;
        }

        fn indexNameInSlice(name: []const u8, index_names: []const []const u8) bool {
            for (index_names) |candidate| {
                if (std.mem.eql(u8, name, candidate)) return true;
            }
            return false;
        }

        pub fn saveAppliedSequencesBatchContext(
            ctx: *const BatchExecutionContext,
            updates: []const apply_state.AppliedSequenceUpdate,
        ) !void {
            if (updates.len == 0) return;
            if (ctx.async_context) |async_ctx| {
                var seq_lock = lockAtomicWithBackoffProfiled(
                    &async_ctx.applied_sequence_mutex,
                    &async_ctx.stats.applied_sequence_mutex,
                );
                defer seq_lock.unlock();
                try apply_state.saveAppliedSequencesWithCheckpoint(
                    ctx.alloc,
                    ctx.store,
                    ctx.applied_sequence_checkpoint_path,
                    updates,
                );
                try DB.DerivedAsyncCallbacks.save_index_status_snapshots(ctx.alloc, ctx.store, ctx.index_manager, updates);
                return;
            }
            try apply_state.saveAppliedSequencesWithCheckpoint(
                ctx.alloc,
                ctx.store,
                ctx.applied_sequence_checkpoint_path,
                updates,
            );
            try DB.DerivedAsyncCallbacks.save_index_status_snapshots(ctx.alloc, ctx.store, ctx.index_manager, updates);
        }

        fn runDerivedUntilContext(ctx: *const BatchExecutionContext, sequence: u64) !void {
            if (sequence == 0) return;
            if (!ctx.executor.hasWorkers()) {
                try DB.DerivedAsyncCallbacks.replay_pending_derived_batches_context(ctx);
                return;
            }
            ctx.executor.notifySequence(sequence);
            try ctx.executor.waitForAll(sequence);
        }

        fn runDerivedUntilTargetsContext(ctx: *const BatchExecutionContext, sequence: u64, index_names: []const []const u8) !void {
            if (sequence == 0 or index_names.len == 0) return;
            if (!ctx.executor.hasWorkers()) return;
            ctx.executor.notifyIndexes(sequence, index_names);
            try ctx.executor.waitForIndexes(sequence, index_names);
        }

        fn runEnrichmentUntilContext(ctx: *const BatchExecutionContext, sequence: u64) !void {
            if (sequence == 0) return;
            if (ctx.enrichment_runtime) |runtime| {
                runtime.notifySequence(sequence);
                try runtime.waitForApplied(sequence);
            }
        }

        fn noPendingEnrichmentReplayThroughContext(ctx: *const BatchExecutionContext, applied_sequence: u64, sequence: u64) !bool {
            const pending = try enrichment_worker.collectPendingDocumentGroups(ctx.alloc, ctx.replay_source, applied_sequence);
            defer enrichment_worker.freePendingDocumentGroups(ctx.alloc, pending);
            for (pending) |group| {
                if (group.sequence <= sequence) return false;
            }
            return true;
        }

        fn runMaintenanceUntilContext(ctx: *const BatchExecutionContext, sequence: u64, sync_targets: ManagedSyncTargets) !void {
            var stable_target = sequence;
            while (true) {
                try runEnrichmentUntilContext(ctx, stable_target);
                const enriched_target = currentReplayTargetSequenceContext(ctx);
                if (enriched_target > stable_target) {
                    stable_target = enriched_target;
                    continue;
                }
                try runDerivedUntilContext(ctx, stable_target);

                const next_target = currentReplayTargetSequenceContext(ctx);
                if (next_target <= stable_target) {
                    try waitForManagedIndexesAppliedContext(ctx, sequence, sync_targets.all_indexes);
                    return;
                }
                stable_target = next_target;
            }
        }

        fn runMaintenanceUntilTargetsContext(ctx: *const BatchExecutionContext, sequence: u64, index_names: []const []const u8) !void {
            if (index_names.len == 0) return;
            var stable_target = sequence;
            while (true) {
                try runEnrichmentUntilContext(ctx, stable_target);
                const enriched_target = currentReplayTargetSequenceContext(ctx);
                if (enriched_target > stable_target) {
                    stable_target = enriched_target;
                    continue;
                }
                try runDerivedUntilTargetsContext(ctx, stable_target, index_names);

                const next_target = currentReplayTargetSequenceContext(ctx);
                if (next_target <= stable_target) {
                    try waitForManagedIndexesAppliedContext(ctx, sequence, index_names);
                    return;
                }
                stable_target = next_target;
            }
        }

        fn currentReplayTargetSequenceContext(ctx: *const BatchExecutionContext) u64 {
            return ctx.store.lastReplaySequence(0);
        }

        fn waitForManagedIndexesAppliedContext(
            ctx: *const BatchExecutionContext,
            sequence: u64,
            index_names: []const []const u8,
        ) !void {
            if (sequence == 0 or index_names.len == 0) return;
            _ = ctx;
        }

        pub fn applyDerivedBatchToIndexAsync(ctx_ptr: *anyopaque, batch: derived_types.DerivedBatch, index_ref: index_manager_mod.ManagedIndexRef) !bool {
            const ctx = asyncContextFromOpaque(ctx_ptr);
            if (!try batchAffectsManagedIndexForReplay(ctx.index_manager, batch, index_ref)) return false;
            if (index_ref.kind == .dense_vector and ctx.active_external_dense_bulk_sessions.load(.acquire) != 0) {
                return error.ReplayDocumentNotVisible;
            }

            try DB.DerivedAsyncCallbacks.apply_derived_batch_to_index_context(ctx, batch, index_ref);

            if (index_ref.kind == .dense_vector) {
                setDenseCatchUpProgress(ctx, .{
                    .sequence = batch.sequence,
                    .target_sequence = ctx.store.lastReplaySequence(0),
                    .scanned_entries = 0,
                    .applied_entries = 1,
                    .active = true,
                });
            }
            if (index_ref.kind == .full_text) if (ctx.text_merge_runtime) |runtime| {
                if (ctx.text_merge_deferred.load(.acquire)) return true;
                runtime.notify();
                runtime.applyBackpressure();
            };
            if (index_ref.kind == .sparse_vector) if (ctx.sparse_compaction_runtime) |runtime| {
                runtime.notify();
            };
            return true;
        }

        pub fn beginDerivedCatchUpSessionAsync(ctx_ptr: *anyopaque, index_ref: index_manager_mod.ManagedIndexRef) !void {
            if (index_ref.kind != .dense_vector) return;
            const ctx = asyncContextFromOpaque(ctx_ptr);
            if (ctx.active_external_dense_bulk_sessions.load(.acquire) != 0) return error.ReplayDocumentNotVisible;

            try db_internal.beginDenseCatchUpSessionTracked(ctx, index_ref.name);
            errdefer db_internal.finishDenseCatchUpSessionTracked(ctx, index_ref.name);
            try ctx.index_manager.beginDenseBulkIngestSessionByName(index_ref.name);
            errdefer ctx.index_manager.abortDenseBulkIngestSessionByName(index_ref.name);
            _ = ctx.stats.dense_catch_up.begin_calls.fetchAdd(1, .monotonic);
        }

        pub fn finishDerivedCatchUpSessionAsync(ctx_ptr: *anyopaque, index_ref: index_manager_mod.ManagedIndexRef, success: bool) !void {
            if (index_ref.kind != .dense_vector) return;
            const ctx = asyncContextFromOpaque(ctx_ptr);

            if (!success) {
                _ = ctx.stats.dense_catch_up.abort_calls.fetchAdd(1, .monotonic);
                var seq_lock = lockAtomicWithBackoffProfiled(&ctx.applied_sequence_mutex, &ctx.stats.applied_sequence_mutex);
                ctx.applied_sequence_coalescer.removePending(ctx.alloc, index_ref.name);
                seq_lock.unlock();
                ctx.index_manager.abortDenseBulkIngestSessionByName(index_ref.name);
                db_internal.finishDenseCatchUpSessionTracked(ctx, index_ref.name);
                return;
            }
            errdefer db_internal.finishDenseCatchUpSessionTracked(ctx, index_ref.name);
            errdefer ctx.index_manager.abortDenseBulkIngestSessionByName(index_ref.name);
            const finish_start_ns = monotonicTimeNs();
            const before_lsm_stats = denseLsmWriteStatsSnapshot(ctx, index_ref.name);

            const maintenance_steps: usize = 0;
            const maintenance_ns: u64 = 0;
            setDenseCatchUpPhase(ctx, .bulk_finish);
            var finish_options = DB.DerivedAsyncCallbacks.dense_catch_up_finish_options();
            finish_options.progress_ctx = ctx;
            finish_options.progress_fn = noteDenseBulkFinishProgress;
            const finalize_start_ns = monotonicTimeNs();
            try ctx.index_manager.finishDenseBulkIngestSessionByNameWithOptions(index_ref.name, finish_options);
            const finalize_ns = elapsedSince(finalize_start_ns);
            setDenseCatchUpPhase(ctx, .applied_sequence_flush);
            var published_visibility = false;
            if (ctx.applied_sequence_mutex.tryLock()) {
                defer ctx.applied_sequence_mutex.unlock();
                published_visibility = try flushFinishedDenseAppliedSequenceLocked(ctx, index_ref.name);
            } else {
                _ = ctx.stats.applied_sequence.skipped_flush_calls.fetchAdd(1, .monotonic);
            }
            if (!published_visibility) {
                if (ctx.query_visibility_hook) |hook| hook.notify(.publish_consistent);
            }
            try ctx.index_manager.checkpointLsmWalForManagedIndex(index_ref);
            db_internal.finishDenseCatchUpSessionTracked(ctx, index_ref.name);
            const after_lsm_stats = denseLsmWriteStatsSnapshot(ctx, index_ref.name);
            const finish_ns = elapsedSince(finish_start_ns);

            _ = ctx.stats.dense_catch_up.finish_calls.fetchAdd(1, .monotonic);
            _ = ctx.stats.dense_catch_up.finish_ns.fetchAdd(finish_ns, .monotonic);
            db_internal.atomicMaxU64(&ctx.stats.dense_catch_up.max_finish_ns, finish_ns);
            _ = ctx.stats.dense_catch_up.finalize_ns.fetchAdd(finalize_ns, .monotonic);
            db_internal.atomicMaxU64(&ctx.stats.dense_catch_up.max_finalize_ns, finalize_ns);
            _ = ctx.stats.dense_catch_up.maintenance_calls.fetchAdd(1, .monotonic);
            _ = ctx.stats.dense_catch_up.maintenance_steps.fetchAdd(@intCast(maintenance_steps), .monotonic);
            _ = ctx.stats.dense_catch_up.maintenance_ns.fetchAdd(maintenance_ns, .monotonic);
            db_internal.atomicMaxU64(&ctx.stats.dense_catch_up.max_maintenance_ns, maintenance_ns);
            var dense_window_result = resource_manager_mod.DenseReplayWindowResult{ .finish_ns = finish_ns };
            if (before_lsm_stats) |before| if (after_lsm_stats) |after| {
                const delta = denseLsmWriteStatsDelta(after, before);
                _ = ctx.stats.dense_catch_up.manifest_writes.fetchAdd(delta.manifest_writes, .monotonic);
                _ = ctx.stats.dense_catch_up.manifest_ns.fetchAdd(delta.manifest_ns, .monotonic);
                _ = ctx.stats.dense_catch_up.write_pressure_compactions.fetchAdd(delta.write_pressure_compactions, .monotonic);
                _ = ctx.stats.dense_catch_up.write_pressure_ns.fetchAdd(delta.write_pressure_ns, .monotonic);
                dense_window_result.write_pressure_compactions = delta.write_pressure_compactions;
                dense_window_result.write_pressure_ns = delta.write_pressure_ns;
            };
            if (ctx.index_manager.resource_manager) |manager| {
                manager.noteDenseReplayWindowResult(dense_window_result);
            }
        }

        pub fn persistAppliedSequenceAsync(ctx_ptr: *anyopaque, index_name: []const u8, sequence: u64, force: bool) !bool {
            const ctx = asyncContextFromOpaque(ctx_ptr);
            if (force) {
                var seq_lock = lockAtomicWithBackoffProfiled(&ctx.applied_sequence_mutex, &ctx.stats.applied_sequence_mutex);
                defer seq_lock.unlock();
                _ = ctx.stats.applied_sequence.note_calls.fetchAdd(1, .monotonic);
                _ = ctx.stats.applied_sequence.forced_flush_calls.fetchAdd(1, .monotonic);
                try ctx.applied_sequence_coalescer.note(ctx.alloc, index_name, sequence);
                return try flushPendingAppliedSequencesLocked(ctx, true);
            }
            if (!ctx.applied_sequence_mutex.tryLock()) {
                _ = ctx.stats.applied_sequence.skipped_flush_calls.fetchAdd(1, .monotonic);
                return false;
            }
            defer ctx.applied_sequence_mutex.unlock();
            _ = ctx.stats.applied_sequence.note_calls.fetchAdd(1, .monotonic);
            try ctx.applied_sequence_coalescer.note(ctx.alloc, index_name, sequence);
            if (db_internal.shouldDeferAppliedSequenceFlush(ctx, false)) {
                _ = ctx.stats.applied_sequence.skipped_flush_calls.fetchAdd(1, .monotonic);
                return false;
            }
            if (!ctx.applied_sequence_coalescer.shouldFlush(monotonicTimeNs())) {
                _ = ctx.stats.applied_sequence.skipped_flush_calls.fetchAdd(1, .monotonic);
                return false;
            }
            return try flushPendingAppliedSequencesLocked(ctx, false);
        }

        pub fn truncateReplaySequenceAsync(ctx_ptr: *anyopaque, sequence: u64) !void {
            const ctx = asyncContextFromOpaque(ctx_ptr);
            var effective = sequence;
            if (ctx.resolution_runtime) |runtime| {
                effective = clampReplayTruncationForReplayStage(effective, ctx.index_manager, runtime.stats());
            }
            if (ctx.promotion_runtime) |runtime| {
                effective = clampReplayTruncationForReplayStage(effective, ctx.index_manager, runtime.stats());
            }
            try ctx.store.truncateReplayUpTo(ctx.alloc, effective);
        }

        pub fn truncateReplayJournalIfSafeContext(ctx: *const BatchExecutionContext) !void {
            if (!ctx.index_manager.hasManagedIndexes()) return;

            const managed_indexes = try ctx.index_manager.managedIndexes(ctx.alloc);
            defer {
                for (managed_indexes) |index_ref| ctx.alloc.free(@constCast(index_ref.name));
                ctx.alloc.free(managed_indexes);
            }
            if (managed_indexes.len == 0) return;

            var min_applied: u64 = std.math.maxInt(u64);
            for (managed_indexes) |index_ref| {
                const applied = try apply_state.loadAppliedSequenceWithCheckpoint(
                    ctx.alloc,
                    ctx.store,
                    ctx.applied_sequence_checkpoint_path,
                    index_ref.name,
                );
                min_applied = @min(min_applied, applied);
            }
            if (ctx.index_manager.hasGeneratedEnrichmentTargets()) {
                const enrichment_applied = try enrichment_state.loadAppliedSequence(
                    ctx.alloc,
                    ctx.store,
                    enrichment_runtime_mod.scope_name,
                );
                min_applied = @min(min_applied, enrichment_applied);
            }
            if (ctx.resolution_runtime) |runtime| {
                const stats = runtime.stats();
                if (resolverReplayRetentionRequired(ctx.index_manager, stats)) {
                    min_applied = @min(min_applied, stats.applied_sequence);
                }
            }
            if (ctx.promotion_runtime) |runtime| {
                const stats = runtime.stats();
                if (resolverReplayRetentionRequired(ctx.index_manager, stats)) {
                    min_applied = @min(min_applied, stats.applied_sequence);
                }
            }
            if (min_applied == 0 or min_applied == std.math.maxInt(u64)) return;
            try truncateReplayLogs(ctx, min_applied);
        }

        pub fn rebuildDenseIndexForTargetCoverageContext(
            ctx: anytype,
            index_name: []const u8,
            rebuild_chunk_size: usize,
        ) !usize {
            const inline_count = try densePrimaryVectorTargetCountForIndexContext(ctx, index_name);
            const artifact_count = try denseArtifactTargetCountForIndexContext(ctx, index_name);
            if (inline_count >= artifact_count and inline_count > 0) {
                return try rebuildDenseIndexFromPrimaryVectorsContext(ctx, index_name, rebuild_chunk_size);
            }
            return try rebuildDenseIndexFromStoredEmbeddingArtifactsContext(ctx, index_name, rebuild_chunk_size);
        }

        pub fn rebuildDenseIndexesForTargetCoverage(self: *DB, alloc: Allocator) !usize {
            var names = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (names.items) |name| alloc.free(name);
                names.deinit(alloc);
            }

            for (self.core.index_manager.dense_indexes.items) |*entry| {
                try names.append(alloc, try alloc.dupe(u8, entry.config.name));
            }

            var rebuilt: usize = 0;
            for (names.items) |name| {
                rebuilt += try rebuildDenseIndexForTargetCoverageContext(self.async_context, name, 2048);
            }
            return rebuilt;
        }

        pub fn rebuildSparseIndexesForTargetCoverage(self: *DB, alloc: Allocator) !usize {
            var names = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (names.items) |name| alloc.free(name);
                names.deinit(alloc);
            }

            for (self.core.index_manager.sparse_indexes.items) |*entry| {
                try names.append(alloc, try alloc.dupe(u8, entry.config.name));
            }

            var rebuilt: usize = 0;
            for (names.items) |name| {
                rebuilt += try rebuildSparseIndexFromStoredEmbeddingArtifactsContext(self.async_context, name, 2048);
            }
            return rebuilt;
        }

        pub fn runDensePostingMaintenanceForIdle(self: *DB) !usize {
            DB.LifecycleCallbacks.lock_apply(self);
            defer self.core.unlockApply();
            return try self.core.index_manager.runDensePostingMaintenance(.{
                .max_postings_per_index = densePostingIdleMaxPostingsPerIndex(),
                .max_layout_changes_per_index = densePostingIdleMaxLayoutChangesPerIndex(),
                .max_boundary_reassignments_per_index = densePostingIdleMaxBoundaryReassignmentsPerIndex(),
            });
        }

        pub fn runDensePostingMaintenanceForIdleBestEffort(self: *DB) !usize {
            if (!self.core.tryLockApplyExclusive()) return 0;
            defer self.core.unlockApply();
            return try self.core.index_manager.runDensePostingMaintenance(.{
                .max_postings_per_index = densePostingIdleMaxPostingsPerIndex(),
                .max_layout_changes_per_index = densePostingIdleMaxLayoutChangesPerIndex(),
                .max_boundary_reassignments_per_index = densePostingIdleMaxBoundaryReassignmentsPerIndex(),
            });
        }

        pub fn flushAppliedSequencesForIdle(self: *DB) !void {
            var seq_lock = lockAtomicWithBackoffProfiled(
                &self.async_context.applied_sequence_mutex,
                &self.async_context.stats.applied_sequence_mutex,
            );
            defer seq_lock.unlock();
            _ = try flushPendingAppliedSequencesLocked(self.async_context, true);
        }

        pub fn denseIndexRebuildStatePathAlloc(self: *DB, alloc: Allocator, index_name: []const u8) ![]u8 {
            return try std.fmt.allocPrint(alloc, "{s}/indexes/{s}", .{ self.core.path, index_name });
        }

        fn collectDenseArtifactTargetCounts(
            self: *DB,
            alloc: Allocator,
            rebuild_targets: ?[]const DenseArtifactRebuildTarget,
        ) !DenseArtifactTargetCounts {
            var counts: DenseArtifactTargetCounts = .{};
            errdefer counts.deinit(alloc);

            var tracked_indices = std.ArrayListUnmanaged(usize).empty;
            defer tracked_indices.deinit(alloc);

            if (rebuild_targets) |targets| {
                for (targets) |target| {
                    try tracked_indices.append(alloc, target.dense_index_idx);
                    try counts.per_target_index.put(alloc, target.dense_index_idx, 0);
                }
            } else {
                for (self.core.index_manager.dense_indexes.items, 0..) |*entry, dense_index_idx| {
                    if (!denseIndexIsArtifactBacked(entry)) continue;
                    try tracked_indices.append(alloc, dense_index_idx);
                    try counts.per_target_index.put(alloc, dense_index_idx, 0);
                }
            }

            if (tracked_indices.items.len == 0) return counts;

            const lower = try self.core.documentRangeLowerAlloc("");
            defer self.core.alloc.free(lower);

            const ScanState = struct {
                alloc: Allocator,
                db: *DB,
                counts: *DenseArtifactTargetCounts,
                tracked_indices: []const usize,

                fn scanEntry(scan_ctx: ?*anyopaque, key: []const u8, value: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
                    const state: *@This() = @ptrCast(@alignCast(scan_ctx orelse return error.InvalidArgument));
                    if (!internal_keys.isInternalUserKey(key)) return .@"continue";

                    var artifact_ref = (try artifact_ids.decodeArtifactRefAlloc(state.alloc, key)) orelse return .@"continue";
                    defer artifact_ref.deinit(state.alloc);
                    if (artifact_ref.kind != .embedding) return .@"continue";

                    const dims = enrichment_artifact_codec.decodeDenseEmbeddingDims(value) catch |err| {
                        if (Self.isRecoverableEmbeddingArtifactError(err)) return .@"continue";
                        return err;
                    };
                    if (dims == 0) return .@"continue";

                    var matched = false;
                    for (state.tracked_indices) |dense_index_idx| {
                        const entry = &state.db.core.index_manager.dense_indexes.items[dense_index_idx];
                        if (!std.mem.eql(u8, denseArtifactNameForEntry(entry), artifact_ref.name)) continue;
                        if (entry.dims != dims) continue;
                        const count = state.counts.per_target_index.getPtr(dense_index_idx).?;
                        count.* += 1;
                        matched = true;
                    }
                    if (matched) state.counts.total_target_artifacts += 1;
                    return .@"continue";
                }
            };

            var state = ScanState{
                .alloc = alloc,
                .db = self,
                .counts = &counts,
                .tracked_indices = tracked_indices.items,
            };
            try self.core.store.scanWithContext(lower, "", .{}, &state, ScanState.scanEntry);
            return counts;
        }

        fn probeDerivedReplayTargetSequence(
            self: *DB,
            alloc: Allocator,
            replay_source: anytype,
            index_ref: index_manager_mod.ManagedIndexRef,
            from_sequence: u64,
        ) !u64 {
            return try DB.LifecycleCallbacks.probe_derived_replay_target_sequence(self, alloc, replay_source, index_ref, from_sequence);
        }

        fn collectDenseArtifactRebuildPlan(self: *DB, alloc: Allocator) !DenseArtifactRebuildPlan {
            const Candidate = struct {
                dense_index_idx: usize,
                persisted_resume: ?[]u8 = null,
                applied_sequence: u64,
                target_sequence: u64,
                force_reset: bool = false,

                fn deinit(candidate: *@This(), local_alloc: Allocator) void {
                    if (candidate.persisted_resume) |buf| local_alloc.free(buf);
                    candidate.* = .{
                        .dense_index_idx = 0,
                        .applied_sequence = 0,
                        .target_sequence = 0,
                    };
                }
            };

            const recoverable_dense_integrity_errors = struct {
                fn check(err: anyerror) bool {
                    return err == error.NotFound or err == error.FileNotFound or err == error.Corrupted;
                }
            };

            var targets = std.ArrayListUnmanaged(DenseArtifactRebuildTarget).empty;
            errdefer targets.deinit(alloc);

            var candidates = std.ArrayListUnmanaged(Candidate).empty;
            defer {
                for (candidates.items) |*candidate| candidate.deinit(alloc);
                candidates.deinit(alloc);
            }

            for (self.core.index_manager.dense_indexes.items, 0..) |*entry, dense_index_idx| {
                if (!denseIndexIsArtifactBacked(entry)) continue;

                const rebuild_root_path = try denseIndexRebuildStatePathAlloc(self, alloc, entry.config.name);
                defer alloc.free(rebuild_root_path);
                const rebuild_state = backfill_state_mod.RebuildState.init(rebuild_root_path);
                const persisted_resume = try rebuild_state.check(alloc);
                errdefer if (persisted_resume) |buf| alloc.free(buf);
                const applied_sequence = try self.core.loadAppliedSequence(alloc, entry.config.name);
                const target_sequence = try probeDerivedReplayTargetSequence(
                    self,
                    alloc,
                    self.core.replaySource(),
                    .{
                        .name = entry.config.name,
                        .kind = .dense_vector,
                    },
                    applied_sequence,
                );
                if (persisted_resume == null and applied_sequence < target_sequence) continue;
                const force_reset = blk: {
                    if (entry.index.stats().active_count == 0) break :blk false;
                    if (@hasDecl(@TypeOf(entry.index), "validateStoredStructure")) {
                        entry.index.validateStoredStructure(alloc) catch |err| {
                            if (recoverable_dense_integrity_errors.check(err)) break :blk true;
                            return err;
                        };
                    }
                    break :blk false;
                };
                try candidates.append(alloc, .{
                    .dense_index_idx = dense_index_idx,
                    .persisted_resume = persisted_resume,
                    .applied_sequence = applied_sequence,
                    .target_sequence = target_sequence,
                    .force_reset = force_reset,
                });
            }

            var candidate_targets = std.ArrayListUnmanaged(DenseArtifactRebuildTarget).empty;
            defer candidate_targets.deinit(alloc);
            for (candidates.items) |candidate| {
                try candidate_targets.append(alloc, .{ .dense_index_idx = candidate.dense_index_idx });
            }

            var target_counts = try collectDenseArtifactTargetCounts(self, alloc, candidate_targets.items);
            defer target_counts.deinit(alloc);

            for (candidates.items) |*candidate| {
                const dense_index_idx = candidate.dense_index_idx;
                const entry = &self.core.index_manager.dense_indexes.items[dense_index_idx];
                const artifact_target_count = target_counts.per_target_index.get(dense_index_idx) orelse 0;
                const rebuild_root_path = try denseIndexRebuildStatePathAlloc(self, alloc, entry.config.name);
                defer alloc.free(rebuild_root_path);
                const rebuild_state = backfill_state_mod.RebuildState.init(rebuild_root_path);
                const already_repaired = !candidate.force_reset and
                    entry.index.stats().active_count >= artifact_target_count and
                    candidate.applied_sequence >= candidate.target_sequence;

                if (candidate.persisted_resume) |buf| {
                    if (artifact_target_count == 0 and !candidate.force_reset) {
                        try rebuild_state.clear();
                        continue;
                    }
                    if (already_repaired) {
                        try rebuild_state.clear();
                        continue;
                    }
                    try targets.append(alloc, .{
                        .dense_index_idx = dense_index_idx,
                        .resume_from = try alloc.dupe(u8, buf),
                        .artifact_target_count = artifact_target_count,
                        .force_reset = candidate.force_reset,
                    });
                    continue;
                }

                if (artifact_target_count == 0 and !candidate.force_reset) continue;
                if (!candidate.force_reset and entry.index.stats().active_count >= artifact_target_count) continue;

                try targets.append(alloc, .{
                    .dense_index_idx = dense_index_idx,
                    .artifact_target_count = artifact_target_count,
                    .force_reset = candidate.force_reset,
                });
            }

            return .{
                .targets = try targets.toOwnedSlice(alloc),
                .target_sequence = target_counts.total_target_artifacts,
            };
        }

        fn prepareDenseArtifactRebuildPlan(self: *DB, plan: DenseArtifactRebuildPlan) !void {
            for (plan.targets) |target| {
                const entry = &self.core.index_manager.dense_indexes.items[target.dense_index_idx];
                if (target.force_reset) {
                    try self.core.index_manager.resetDenseIndexForArtifactRebuild(entry.config.name);
                }
                const rebuild_root_path = try denseIndexRebuildStatePathAlloc(self, self.alloc, entry.config.name);
                defer self.alloc.free(rebuild_root_path);
                const rebuild_state = backfill_state_mod.RebuildState.init(rebuild_root_path);
                try rebuild_state.update(target.resume_from orelse "");
            }
        }

        fn finalizeDenseArtifactRebuildPlan(self: *DB, alloc: Allocator, plan: DenseArtifactRebuildPlan) !void {
            for (plan.targets) |target| {
                const entry = &self.core.index_manager.dense_indexes.items[target.dense_index_idx];
                const rebuild_root_path = try denseIndexRebuildStatePathAlloc(self, alloc, entry.config.name);
                defer alloc.free(rebuild_root_path);
                const rebuild_state = backfill_state_mod.RebuildState.init(rebuild_root_path);
                const applied_sequence = try self.core.loadAppliedSequence(alloc, entry.config.name);
                const target_sequence = try probeDerivedReplayTargetSequence(
                    self,
                    alloc,
                    self.core.replaySource(),
                    .{
                        .name = entry.config.name,
                        .kind = .dense_vector,
                    },
                    applied_sequence,
                );
                const repaired = entry.index.stats().active_count >= target.artifact_target_count and applied_sequence >= target_sequence;
                if (repaired) {
                    try rebuild_state.clear();
                }
            }
        }

        fn denseArtifactWatermarkRepairNeeded(self: *DB, alloc: Allocator) !bool {
            return (try repairDenseArtifactAppliedSequencesFromCoverage(self, alloc, false)) > 0;
        }

        fn repairDenseArtifactAppliedSequencesIfCovered(self: *DB, alloc: Allocator) !usize {
            return try repairDenseArtifactAppliedSequencesFromCoverage(self, alloc, true);
        }

        fn repairDenseArtifactAppliedSequencesFromCoverage(self: *DB, alloc: Allocator, repair: bool) !usize {
            var target_counts = try collectDenseArtifactTargetCounts(self, alloc, null);
            defer target_counts.deinit(alloc);

            var repaired: usize = 0;
            for (self.core.index_manager.dense_indexes.items, 0..) |*entry, dense_index_idx| {
                if (!denseIndexIsArtifactBacked(entry)) continue;

                const artifact_target_count = target_counts.per_target_index.get(dense_index_idx) orelse 0;
                if (artifact_target_count == 0) continue;
                if (entry.index.stats().active_count < artifact_target_count) continue;

                const applied_sequence = try self.core.loadAppliedSequence(alloc, entry.config.name);
                const target_sequence = try probeDerivedReplayTargetSequence(
                    self,
                    alloc,
                    self.core.replaySource(),
                    .{
                        .name = entry.config.name,
                        .kind = .dense_vector,
                    },
                    applied_sequence,
                );
                if (applied_sequence >= target_sequence) continue;

                repaired += 1;
                if (repair) try self.core.saveAppliedSequence(entry.config.name, target_sequence);
            }
            return repaired;
        }

        pub fn rebuildDenseIndexesFromStoredEmbeddingArtifacts(self: *DB, alloc: Allocator) !usize {
            return try rebuildDenseIndexesFromStoredEmbeddingArtifactsWithProgress(self, alloc, null, null);
        }

        pub fn rebuildDenseIndexesFromStoredEmbeddingArtifactsResumeWithProgress(
            self: *DB,
            alloc: Allocator,
            resume_from: ?[]const u8,
            rebuild_targets: ?[]const DenseArtifactRebuildTarget,
            target_sequence_override: ?u64,
            progress_ctx: ?*anyopaque,
            progress_hook: ?db_internal.ReplayProgressHook,
            resume_ctx: ?*anyopaque,
            resume_hook: ?DenseArtifactRebuildResumeHook,
            rebuild_chunk_size: usize,
            rebuild_progress_interval: usize,
        ) !usize {
            const lower = try self.core.documentRangeLowerAlloc("");
            defer self.core.alloc.free(lower);

            const cache_nodes_before = try alloc.alloc(usize, self.core.index_manager.dense_indexes.items.len);
            defer alloc.free(cache_nodes_before);
            const cache_vectors_before = try alloc.alloc(usize, self.core.index_manager.dense_indexes.items.len);
            defer alloc.free(cache_vectors_before);
            for (self.core.index_manager.dense_indexes.items, 0..) |*entry, i| {
                cache_nodes_before[i] = entry.index.config.max_cached_nodes;
                cache_vectors_before[i] = entry.index.config.max_cached_vectors;
                entry.index.setCacheCaps(
                    if (entry.index.config.max_cached_nodes > 0) @min(entry.index.config.max_cached_nodes, denseCatchUpStartupCacheNodes()) else 0,
                    if (entry.index.config.max_cached_vectors > 0) @min(entry.index.config.max_cached_vectors, denseCatchUpStartupCacheVectors()) else 0,
                );
            }
            defer {
                for (self.core.index_manager.dense_indexes.items, 0..) |*entry, i| {
                    entry.index.setCacheCaps(cache_nodes_before[i], cache_vectors_before[i]);
                }
            }

            const target_sequence = if (target_sequence_override) |value| value else blk: {
                var target_counts = try collectDenseArtifactTargetCounts(self, alloc, rebuild_targets);
                defer target_counts.deinit(alloc);
                break :blk target_counts.total_target_artifacts;
            };
            if (progress_hook != null) {
                const initial_progress: db_internal.ReplayProgress = .{
                    .sequence = 0,
                    .target_sequence = target_sequence,
                    .scanned_entries = 0,
                    .applied_entries = 0,
                    .active = true,
                };
                DB.LifecycleCallbacks.set_dense_catch_up_progress(self.async_context, initial_progress);
                try progress_hook.?(progress_ctx.?, "", initial_progress);
            }

            const ScanState = struct {
                db: *DB,
                alloc: Allocator,
                resume_from: ?[]const u8,
                target_sequence: u64,
                progress_ctx: ?*anyopaque,
                progress_hook: ?db_internal.ReplayProgressHook,
                resume_ctx: ?*anyopaque,
                resume_hook: ?DenseArtifactRebuildResumeHook,
                rebuild_chunk_size: usize,
                rebuild_progress_interval: usize,
                rebuild_targets: ?[]const DenseArtifactRebuildTarget = null,
                rebuild_target_lookup: std.StringHashMapUnmanaged(usize) = .empty,
                writes: std.ArrayListUnmanaged(mapper.DenseEmbeddingWrite) = .empty,
                rebuilt: usize = 0,
                scanned_entries: usize = 0,
                last_reported_scanned_entries: usize = 0,
                last_reported_rebuilt: usize = 0,
                last_matching_key: ?[]u8 = null,
                last_flushed_key: ?[]u8 = null,

                fn deinit(state: *@This()) void {
                    Self.freeDenseArtifactRebuildWrites(state.alloc, &state.writes);
                    state.writes.deinit(state.alloc);
                    state.rebuild_target_lookup.deinit(state.alloc);
                    if (state.last_matching_key) |buf| state.alloc.free(buf);
                    if (state.last_flushed_key) |buf| state.alloc.free(buf);
                }

                fn updateOwnedKey(state: *@This(), slot: *?[]u8, key: []const u8) !void {
                    if (slot.*) |buf| state.alloc.free(buf);
                    slot.* = try state.alloc.dupe(u8, key);
                }

                fn rebuildTargetForIndex(
                    state: *const @This(),
                    index_name: []const u8,
                ) ?*const DenseArtifactRebuildTarget {
                    const targets = state.rebuild_targets orelse return null;
                    const target_idx = state.rebuild_target_lookup.get(index_name) orelse return null;
                    return &targets[target_idx];
                }

                fn publishProgress(state: *@This(), active: bool) !void {
                    const hook = state.progress_hook orelse return;
                    const progress: db_internal.ReplayProgress = .{
                        .sequence = @intCast(state.rebuilt),
                        .target_sequence = state.target_sequence,
                        .scanned_entries = @intCast(state.scanned_entries),
                        .applied_entries = @intCast(state.rebuilt),
                        .active = active,
                    };
                    state.last_reported_scanned_entries = state.scanned_entries;
                    state.last_reported_rebuilt = state.rebuilt;
                    DB.LifecycleCallbacks.set_dense_catch_up_progress(state.db.async_context, progress);
                    try hook(state.progress_ctx.?, "", progress);
                }

                fn flushChunk(state: *@This(), key: []const u8) !void {
                    try Self.flushDenseArtifactRebuildChunk(state.db, state.alloc, &state.writes);
                    try state.updateOwnedKey(&state.last_flushed_key, key);
                    if (state.resume_hook) |hook| try hook(state.resume_ctx.?, key);
                    try state.publishProgress(true);
                }

                fn scanEntry(scan_ctx: ?*anyopaque, key: []const u8, value: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
                    const state: *@This() = @ptrCast(@alignCast(scan_ctx orelse return error.InvalidArgument));
                    if (!internal_keys.isInternalUserKey(key)) return .@"continue";
                    if (state.resume_from) |resume_key| {
                        if (resume_key.len > 0 and std.mem.order(u8, key, resume_key) != .gt) return .@"continue";
                    }

                    var artifact_ref = (try artifact_ids.decodeArtifactRefAlloc(state.alloc, key)) orelse return .@"continue";
                    defer artifact_ref.deinit(state.alloc);
                    if (artifact_ref.kind != .embedding) return .@"continue";
                    state.scanned_entries += 1;
                    try state.updateOwnedKey(&state.last_matching_key, key);

                    const vector = enrichment_artifact_codec.decodeDenseEmbeddingAlloc(state.alloc, value) catch |err| {
                        if (Self.isRecoverableEmbeddingArtifactError(err)) {
                            try state.db.core.store.putBatch(&.{}, &.{key});
                            return .@"continue";
                        }
                        return err;
                    };
                    defer state.alloc.free(vector);
                    const dims: u32 = @intCast(vector.len);
                    if (dims == 0) return .@"continue";

                    const consumer_indexes = state.db.core.index_manager.denseIndexesForEmbedding(state.alloc, artifact_ref.name, dims) catch |err| switch (err) {
                        error.ConflictingEnrichmentConfig => return .@"continue",
                        else => return err,
                    };
                    defer {
                        for (consumer_indexes) |index_name| state.alloc.free(index_name);
                        state.alloc.free(consumer_indexes);
                    }
                    if (consumer_indexes.len == 0) return .@"continue";

                    const source_key = if (artifact_ref.source) |source| blk: {
                        var source_ref = types.ArtifactRef{
                            .document_id = try state.alloc.dupe(u8, artifact_ref.document_id),
                            .name = try state.alloc.dupe(u8, source.name),
                            .kind = source.kind,
                            .chunk_id = source.chunk_id,
                        };
                        defer source_ref.deinit(state.alloc);
                        break :blk try artifact_ids.internalKeyForArtifactRefAlloc(state.alloc, source_ref);
                    } else try state.alloc.dupe(u8, artifact_ref.document_id);
                    defer state.alloc.free(source_key);

                    var emitted_any_target = false;
                    for (consumer_indexes) |index_name| {
                        if (state.rebuild_targets) |_| {
                            const target = state.rebuildTargetForIndex(index_name) orelse continue;
                            if (target.resume_from) |target_resume| {
                                if (target_resume.len > 0 and std.mem.order(u8, key, target_resume) != .gt) continue;
                            }
                        }
                        try state.writes.append(state.alloc, .{
                            .index_name = try state.alloc.dupe(u8, index_name),
                            .doc_key = try state.alloc.dupe(u8, source_key),
                            .artifact_key = try state.alloc.dupe(u8, key),
                            .vector = &.{},
                        });
                        emitted_any_target = true;
                    }
                    if (emitted_any_target) {
                        state.rebuilt += 1;
                    }

                    if (state.progress_hook != null and
                        (state.scanned_entries - state.last_reported_scanned_entries >= state.rebuild_progress_interval or
                            state.rebuilt - state.last_reported_rebuilt >= state.rebuild_progress_interval))
                    {
                        try state.publishProgress(true);
                    }

                    if (state.writes.items.len >= state.rebuild_chunk_size) {
                        try state.flushChunk(key);
                    }
                    return .@"continue";
                }
            };

            var state = ScanState{
                .db = self,
                .alloc = alloc,
                .resume_from = resume_from,
                .target_sequence = target_sequence,
                .progress_ctx = progress_ctx,
                .progress_hook = progress_hook,
                .resume_ctx = resume_ctx,
                .resume_hook = resume_hook,
                .rebuild_chunk_size = rebuild_chunk_size,
                .rebuild_progress_interval = rebuild_progress_interval,
                .rebuild_targets = rebuild_targets,
            };
            defer state.deinit();

            if (rebuild_targets) |targets| {
                for (targets, 0..) |target, target_idx| {
                    const entry = &self.core.index_manager.dense_indexes.items[target.dense_index_idx];
                    try state.rebuild_target_lookup.put(state.alloc, entry.config.name, target_idx);
                }
            }

            try self.core.store.scanWithContext(lower, "", .{}, &state, ScanState.scanEntry);

            if (state.writes.items.len > 0) {
                try flushDenseArtifactRebuildChunk(self, alloc, &state.writes);
            }
            if (state.last_flushed_key == null and state.last_matching_key != null) {
                try state.updateOwnedKey(&state.last_flushed_key, state.last_matching_key.?);
            }
            if (resume_hook) |hook| {
                if (state.last_flushed_key) |last_key| try hook(resume_ctx.?, last_key);
            }
            if (progress_hook != null) {
                const final_progress: db_internal.ReplayProgress = .{
                    .sequence = @intCast(state.rebuilt),
                    .target_sequence = target_sequence,
                    .scanned_entries = @intCast(state.scanned_entries),
                    .applied_entries = @intCast(state.rebuilt),
                    .active = false,
                };
                DB.LifecycleCallbacks.set_dense_catch_up_progress(self.async_context, final_progress);
                try progress_hook.?(progress_ctx.?, "", final_progress);
            }
            if (state.rebuilt == 0) return 0;
            return state.rebuilt;
        }

        pub fn rebuildDenseIndexesFromStoredEmbeddingArtifactsWithProgress(
            self: *DB,
            alloc: Allocator,
            progress_ctx: ?*anyopaque,
            progress_hook: ?db_internal.ReplayProgressHook,
        ) !usize {
            return try rebuildDenseIndexesFromStoredEmbeddingArtifactsResumeWithProgress(
                self,
                alloc,
                null,
                null,
                null,
                progress_ctx,
                progress_hook,
                null,
                null,
                2048,
                128,
            );
        }

        pub fn rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(self: *DB, alloc: Allocator) !usize {
            return try rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeededWithProgress(self, alloc, null, null);
        }

        pub fn hasPendingDenseArtifactRebuild(self: *DB, alloc: Allocator) !bool {
            var plan = try collectDenseArtifactRebuildPlan(self, alloc);
            defer plan.deinit(alloc);
            if (plan.targets.len > 0) return true;
            return try denseArtifactWatermarkRepairNeeded(self, alloc);
        }

        pub fn rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeededWithProgress(
            self: *DB,
            alloc: Allocator,
            progress_ctx: ?*anyopaque,
            progress_hook: ?db_internal.ReplayProgressHook,
        ) !usize {
            var plan = try collectDenseArtifactRebuildPlan(self, alloc);
            defer plan.deinit(alloc);
            if (plan.targets.len == 0) return try repairDenseArtifactAppliedSequencesIfCovered(self, alloc);

            try prepareDenseArtifactRebuildPlan(self, plan);
            const ResumePersistCtx = struct {
                db: *DB,
                targets: []DenseArtifactRebuildTarget,

                fn run(ctx: *anyopaque, last_key: []const u8) !void {
                    const persist: *@This() = @ptrCast(@alignCast(ctx));
                    for (persist.targets) |*target| {
                        if (target.resume_from) |resume_from| {
                            if (std.mem.order(u8, last_key, resume_from) != .gt) continue;
                        }
                        const entry = &persist.db.core.index_manager.dense_indexes.items[target.dense_index_idx];
                        const rebuild_root_path = try denseIndexRebuildStatePathAlloc(persist.db, persist.db.alloc, entry.config.name);
                        defer persist.db.alloc.free(rebuild_root_path);
                        const rebuild_state = backfill_state_mod.RebuildState.init(rebuild_root_path);
                        try rebuild_state.update(last_key);
                        const owned_key = try persist.db.alloc.dupe(u8, last_key);
                        errdefer persist.db.alloc.free(owned_key);
                        if (target.resume_from) |resume_from| persist.db.alloc.free(resume_from);
                        target.resume_from = owned_key;
                    }
                }
            };
            var persist_ctx = ResumePersistCtx{
                .db = self,
                .targets = plan.targets,
            };
            const rebuilt = try rebuildDenseIndexesFromStoredEmbeddingArtifactsResumeWithProgress(
                self,
                alloc,
                null,
                plan.targets,
                plan.target_sequence,
                progress_ctx,
                progress_hook,
                &persist_ctx,
                ResumePersistCtx.run,
                2048,
                128,
            );
            try finalizeDenseArtifactRebuildPlan(self, alloc, plan);
            _ = try repairDenseArtifactAppliedSequencesIfCovered(self, alloc);
            return rebuilt;
        }

        pub fn rebuildSparseIndexFromStoredEmbeddingArtifactsContext(
            ctx: anytype,
            index_name: []const u8,
            rebuild_chunk_size: usize,
        ) !usize {
            const expected_name = ctx.index_manager.sparseEmbeddingName(index_name) orelse index_name;

            const lower = try internal_keys.documentRangeLowerAlloc(ctx.alloc, "");
            defer ctx.alloc.free(lower);

            const ScanState = struct {
                ctx: @TypeOf(ctx),
                index_name: []const u8,
                expected_name: []const u8,
                rebuild_chunk_size: usize,
                writes: std.ArrayListUnmanaged(mapper.SparseEmbeddingWrite) = .empty,
                rebuilt: usize = 0,

                fn deinit(state: *@This()) void {
                    Self.freeSparseArtifactRebuildWrites(state.ctx.alloc, &state.writes);
                    state.writes.deinit(state.ctx.alloc);
                }

                fn flush(state: *@This()) !void {
                    try Self.flushSparseArtifactRebuildChunkContext(state.ctx, state.index_name, &state.writes);
                }

                fn scanEntry(scan_ctx: ?*anyopaque, key: []const u8, value: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
                    const state: *@This() = @ptrCast(@alignCast(scan_ctx orelse return error.InvalidArgument));
                    if (!internal_keys.isInternalUserKey(key)) return .@"continue";

                    const identity = (try internal_keys.parseEmbeddingArtifactKeyView(key)) orelse return .@"continue";
                    if (!std.mem.eql(u8, identity.artifact_name, state.expected_name)) return .@"continue";

                    var sparse = enrichment_artifact_codec.decodeSparseEmbeddingAlloc(state.ctx.alloc, value) catch |err| {
                        if (Self.isRecoverableEmbeddingArtifactError(err)) return .@"continue";
                        return err;
                    };
                    sparse.deinit(state.ctx.alloc);

                    try state.writes.append(state.ctx.alloc, .{
                        .index_name = @constCast(state.index_name),
                        .doc_key = try state.ctx.alloc.dupe(u8, identity.doc_key),
                        .artifact_key = try state.ctx.alloc.dupe(u8, key),
                        .indices = &.{},
                        .values = &.{},
                    });
                    state.rebuilt += 1;

                    if (state.writes.items.len >= state.rebuild_chunk_size) try state.flush();
                    return .@"continue";
                }
            };

            var state = ScanState{
                .ctx = ctx,
                .index_name = index_name,
                .expected_name = expected_name,
                .rebuild_chunk_size = rebuild_chunk_size,
            };
            defer state.deinit();

            try ctx.store.scanWithContext(lower, "", .{}, &state, ScanState.scanEntry);
            if (state.writes.items.len > 0) try state.flush();
            return state.rebuilt;
        }

        fn asyncContextFromOpaque(ctx_ptr: *anyopaque) *db_internal.AsyncContext(DB) {
            return @ptrCast(@alignCast(ctx_ptr));
        }

        pub fn setDenseCatchUpProgress(ctx: *AsyncContext, progress: db_internal.ReplayProgress) void {
            ctx.stats.dense_catch_up.active.store(if (progress.active) 1 else 0, .monotonic);
            ctx.stats.dense_catch_up.phase.store(@intFromEnum(if (progress.active) types.DenseCatchUpStats.Phase.replay else types.DenseCatchUpStats.Phase.idle), .monotonic);
            ctx.stats.dense_catch_up.current_sequence.store(progress.sequence, .monotonic);
            ctx.stats.dense_catch_up.current_target_sequence.store(progress.target_sequence, .monotonic);
            ctx.stats.dense_catch_up.current_scanned_entries.store(progress.scanned_entries, .monotonic);
            ctx.stats.dense_catch_up.current_applied_entries.store(progress.applied_entries, .monotonic);
            ctx.stats.dense_catch_up.replay_scan_batches.store(progress.replay_scan_batches, .monotonic);
            ctx.stats.dense_catch_up.replay_hint_filter_skips.store(progress.replay_hint_filter_skips, .monotonic);
            _ = ctx.stats.dense_catch_up.progress_updates.fetchAdd(1, .monotonic);
        }

        fn setDenseCatchUpPhase(ctx: *AsyncContext, phase: types.DenseCatchUpStats.Phase) void {
            ctx.stats.dense_catch_up.phase.store(@intFromEnum(phase), .monotonic);
            _ = ctx.stats.dense_catch_up.progress_updates.fetchAdd(1, .monotonic);
        }

        fn noteDenseBulkFinishProgress(ctx_ptr: *anyopaque, progress: backend_types.BulkIngestFinishOptions.Progress) void {
            const ctx = asyncContextFromOpaque(ctx_ptr);
            const phase: types.DenseCatchUpStats.Phase = switch (progress.phase) {
                .begin => .bulk_finish,
                .split => .bulk_split,
                .publish => .bulk_publish,
                .complete => .bulk_finish,
            };
            ctx.stats.dense_catch_up.phase.store(@intFromEnum(phase), .monotonic);
            ctx.stats.dense_catch_up.bulk_finish_current_window.store(progress.publish_window, .monotonic);
            ctx.stats.dense_catch_up.bulk_finish_current_window_split_steps.store(progress.split_steps, .monotonic);
            ctx.stats.dense_catch_up.bulk_finish_deferred_leaf_splits.store(progress.deferred_leaf_splits, .monotonic);
            ctx.stats.dense_catch_up.bulk_finish_current_window_ns.store(progress.elapsed_ns, .monotonic);
            db_internal.atomicMaxU64(&ctx.stats.dense_catch_up.bulk_finish_max_window_ns, progress.elapsed_ns);
            if (progress.phase == .publish) {
                _ = ctx.stats.dense_catch_up.bulk_finish_windows.fetchAdd(1, .monotonic);
                _ = ctx.stats.dense_catch_up.bulk_finish_split_steps.fetchAdd(progress.split_steps, .monotonic);
                if (progress.elapsed_ns > std.time.ns_per_s) {
                    std.log.info(
                        "dense catch-up bulk finish publish window window={} split_steps={} deferred_leaf_splits={} elapsed_ms={}",
                        .{ progress.publish_window, progress.split_steps, progress.deferred_leaf_splits, progress.elapsed_ns / std.time.ns_per_ms },
                    );
                }
            }
            _ = ctx.stats.dense_catch_up.progress_updates.fetchAdd(1, .monotonic);
        }

        fn lockAtomicWithBackoffProfiled(mutex: *std.atomic.Mutex, stats: *db_internal.MutexContentionStats) db_internal.ProfiledLock {
            return db_internal.lockAtomicWithBackoffProfiled(mutex, stats, DB.DerivedAsyncCallbacks.async_index_profile_enabled());
        }

        fn denseLsmWriteStatsSnapshot(ctx: *AsyncContext, index_name: []const u8) ?hbc_mod.LsmWriteStats {
            const entry = ctx.index_manager.denseIndex(index_name) orelse return null;
            return entry.index.snapshotLsmWriteStats();
        }

        fn denseLsmWriteStatsDelta(after: hbc_mod.LsmWriteStats, before: hbc_mod.LsmWriteStats) hbc_mod.LsmWriteStats {
            var delta = after;
            inline for (std.meta.fields(hbc_mod.LsmWriteStats)) |field| {
                if (field.type == u64) {
                    @field(delta, field.name) = @field(after, field.name) -| @field(before, field.name);
                }
            }
            return delta;
        }

        fn flushFinishedDenseAppliedSequenceLocked(ctx: *AsyncContext, index_name: []const u8) !bool {
            const pending = ctx.applied_sequence_coalescer.takePending(index_name) orelse return false;
            defer ctx.alloc.free(pending.owned_name);

            const flush_start_ns = monotonicTimeNs();
            const save_start_ns = monotonicTimeNs();
            try apply_state.saveAppliedSequencesWithCheckpoint(ctx.alloc, ctx.store, ctx.applied_sequence_checkpoint_path, &[_]apply_state.AppliedSequenceUpdate{.{
                .index_name = pending.owned_name,
                .sequence = pending.sequence,
            }});
            try DB.DerivedAsyncCallbacks.save_index_status_snapshots(ctx.alloc, ctx.store, ctx.index_manager, &[_]apply_state.AppliedSequenceUpdate{.{
                .index_name = pending.owned_name,
                .sequence = pending.sequence,
            }});
            const save_ns = elapsedSince(save_start_ns);

            ctx.applied_sequence_coalescer.last_flush_ns = monotonicTimeNs();
            const flush_ns = elapsedSince(flush_start_ns);
            _ = ctx.stats.applied_sequence.flush_calls.fetchAdd(1, .monotonic);
            _ = ctx.stats.applied_sequence.flushed_indexes.fetchAdd(1, .monotonic);
            _ = ctx.stats.applied_sequence.save_ns.fetchAdd(save_ns, .monotonic);
            _ = ctx.stats.applied_sequence.flush_ns.fetchAdd(flush_ns, .monotonic);
            db_internal.atomicMaxU64(&ctx.stats.applied_sequence.max_flush_ns, flush_ns);
            if (ctx.query_visibility_hook) |hook| hook.notify(.publish_consistent);
            return true;
        }

        fn flushPendingAppliedSequencesLocked(ctx: *AsyncContext, force: bool) !bool {
            if (ctx.applied_sequence_coalescer.pending.count() == 0) return false;

            const flush_start_ns = monotonicTimeNs();
            var updates = std.ArrayListUnmanaged(apply_state.AppliedSequenceUpdate).empty;
            defer updates.deinit(ctx.alloc);

            var it = ctx.applied_sequence_coalescer.pending.iterator();
            while (it.next()) |entry| {
                if (!force and db_internal.shouldDeferAppliedSequenceFlush(ctx, false)) continue;
                try updates.append(ctx.alloc, .{
                    .index_name = entry.key_ptr.*,
                    .sequence = entry.value_ptr.*,
                });
            }
            if (updates.items.len == 0) return false;
            const sync_ns: u64 = 0;

            const save_start_ns = monotonicTimeNs();
            try apply_state.saveAppliedSequencesWithCheckpoint(
                ctx.alloc,
                ctx.store,
                ctx.applied_sequence_checkpoint_path,
                updates.items,
            );
            try DB.DerivedAsyncCallbacks.save_index_status_snapshots(ctx.alloc, ctx.store, ctx.index_manager, updates.items);
            const save_ns = elapsedSince(save_start_ns);
            ctx.applied_sequence_coalescer.clearPending(ctx.alloc);
            ctx.applied_sequence_coalescer.last_flush_ns = monotonicTimeNs();
            const flush_ns = elapsedSince(flush_start_ns);
            _ = ctx.stats.applied_sequence.flush_calls.fetchAdd(1, .monotonic);
            _ = ctx.stats.applied_sequence.flushed_indexes.fetchAdd(@intCast(updates.items.len), .monotonic);
            _ = ctx.stats.applied_sequence.sync_ns.fetchAdd(sync_ns, .monotonic);
            _ = ctx.stats.applied_sequence.save_ns.fetchAdd(save_ns, .monotonic);
            _ = ctx.stats.applied_sequence.flush_ns.fetchAdd(flush_ns, .monotonic);
            db_internal.atomicMaxU64(&ctx.stats.applied_sequence.max_flush_ns, flush_ns);
            if (ctx.query_visibility_hook) |hook| hook.notify(.publish);
            return true;
        }

        fn resolverReplayRetentionRequired(index_manager: *const index_manager_mod.IndexManager, stats: types.ReplayStageStats) bool {
            if (stats.blocked) return true;
            return index_manager.resolvers.items.len > 0;
        }

        fn clampReplayTruncationForReplayStage(
            effective: u64,
            index_manager: *const index_manager_mod.IndexManager,
            stats: types.ReplayStageStats,
        ) u64 {
            if (!resolverReplayRetentionRequired(index_manager, stats)) return effective;
            return @min(effective, stats.applied_sequence);
        }

        fn truncateReplayLogs(ctx: *const BatchExecutionContext, up_to_sequence: u64) !void {
            try ctx.store.truncateReplayUpTo(ctx.alloc, up_to_sequence);
            ctx.executor.releaseBacklogThrough(up_to_sequence);
        }

        fn monotonicTimeNs() u64 {
            return platform.time.monotonicNs();
        }

        fn elapsedSince(start_ns: u64) u64 {
            return monotonicTimeNs() - start_ns;
        }

        fn denseIndexIsArtifactBacked(entry: anytype) bool {
            return entry.external or entry.chunk_name != null or entry.embedding_name != null;
        }

        fn denseTargetCountForIndexContext(ctx: anytype, index_name: []const u8) !u64 {
            const artifact_count = try denseArtifactTargetCountForIndexContext(ctx, index_name);
            const inline_count = try densePrimaryVectorTargetCountForIndexContext(ctx, index_name);
            return @max(artifact_count, inline_count);
        }

        fn denseArtifactTargetCountForIndexContext(ctx: anytype, index_name: []const u8) !u64 {
            const entry = ctx.index_manager.denseIndex(index_name) orelse return 0;
            const expected_name = denseArtifactNameForEntry(entry);
            const expected_dims = entry.dims;

            const lower = try internal_keys.documentRangeLowerAlloc(ctx.alloc, "");
            defer ctx.alloc.free(lower);

            const ScanState = struct {
                alloc: Allocator,
                expected_name: []const u8,
                expected_dims: u32,
                count: u64 = 0,

                fn scanEntry(scan_ctx: ?*anyopaque, key: []const u8, value: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
                    const state: *@This() = @ptrCast(@alignCast(scan_ctx orelse return error.InvalidArgument));
                    if (!internal_keys.isInternalUserKey(key)) return .@"continue";

                    var artifact_ref = (artifact_ids.decodeArtifactRefAlloc(state.alloc, key) catch |err| switch (err) {
                        error.InvalidInternalUserKey => return .@"continue",
                        else => return err,
                    }) orelse return .@"continue";
                    defer artifact_ref.deinit(state.alloc);
                    if (artifact_ref.kind != .embedding) return .@"continue";
                    if (!std.mem.eql(u8, artifact_ref.name, state.expected_name)) return .@"continue";

                    const dims = enrichment_artifact_codec.decodeDenseEmbeddingDims(value) catch |err| {
                        if (Self.isRecoverableEmbeddingArtifactError(err)) return .@"continue";
                        return err;
                    };
                    if (dims != state.expected_dims) return .@"continue";

                    state.count += 1;
                    return .@"continue";
                }
            };

            var state = ScanState{
                .alloc = ctx.alloc,
                .expected_name = expected_name,
                .expected_dims = expected_dims,
            };
            try ctx.store.scanWithContext(lower, "", .{}, &state, ScanState.scanEntry);
            return state.count;
        }

        fn densePrimaryVectorTargetCountForIndexContext(ctx: anytype, index_name: []const u8) !u64 {
            const entry = ctx.index_manager.denseIndex(index_name) orelse return 0;
            const field_name = entry.field_name;
            const dims = entry.dims;

            const lower = try internal_keys.documentRangeLowerAlloc(ctx.alloc, "");
            defer ctx.alloc.free(lower);

            const ScanState = struct {
                alloc: Allocator,
                relational_base_rows: bool,
                field_name: []const u8,
                dims: u32,
                count: u64 = 0,

                fn scanEntry(scan_ctx: ?*anyopaque, key: []const u8, value: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
                    const state: *@This() = @ptrCast(@alignCast(scan_ctx orelse return error.InvalidArgument));
                    if (!db_internal.isBaseDocumentStoreKeyForMode(state.relational_base_rows, key)) return .@"continue";
                    const doc_value = if (state.relational_base_rows)
                        try mapper.materializeRelationalRowValueAlloc(state.alloc, value)
                    else
                        value;
                    defer if (doc_value.ptr != value.ptr) state.alloc.free(doc_value);
                    if (try mapper.extractDenseVectorField(state.alloc, doc_value, state.field_name, state.dims)) |vector| {
                        state.alloc.free(vector);
                        state.count += 1;
                    }
                    return .@"continue";
                }
            };

            var state = ScanState{
                .alloc = ctx.alloc,
                .relational_base_rows = ctx.relational_base_rows,
                .field_name = field_name,
                .dims = dims,
            };
            try ctx.store.scanWithContext(lower, "", .{}, &state, ScanState.scanEntry);
            return state.count;
        }

        fn freePrimaryVectorRebuildWrites(alloc: Allocator, writes: *std.ArrayListUnmanaged(types.BatchWrite)) void {
            for (writes.items) |write| {
                alloc.free(@constCast(write.key));
                alloc.free(@constCast(write.value));
            }
            writes.clearRetainingCapacity();
        }

        fn flushDensePrimaryVectorRebuildChunkContext(
            ctx: anytype,
            index_name: []const u8,
            writes: *std.ArrayListUnmanaged(types.BatchWrite),
        ) !void {
            defer freePrimaryVectorRebuildWrites(ctx.alloc, writes);
            if (writes.items.len == 0) return;
            try ctx.index_manager.indexDenseBatchByNameWithOptions(
                ctx.store,
                index_name,
                writes.items,
                .{ .mode = .bulk_ingest },
            );
        }

        fn rebuildDenseIndexFromPrimaryVectorsContext(
            ctx: anytype,
            index_name: []const u8,
            rebuild_chunk_size: usize,
        ) !usize {
            const entry = ctx.index_manager.denseIndex(index_name) orelse return 0;
            const field_name = entry.field_name;
            const dims = entry.dims;

            const lower = try internal_keys.documentRangeLowerAlloc(ctx.alloc, "");
            defer ctx.alloc.free(lower);

            try ctx.index_manager.beginDenseBulkIngestSessionByName(index_name);
            var bulk_session_open = true;
            errdefer if (bulk_session_open) ctx.index_manager.abortDenseBulkIngestSessionByName(index_name);

            const ScanState = struct {
                ctx: @TypeOf(ctx),
                index_name: []const u8,
                field_name: []const u8,
                dims: u32,
                rebuild_chunk_size: usize,
                writes: std.ArrayListUnmanaged(types.BatchWrite) = .empty,
                rebuilt: usize = 0,

                fn deinit(state: *@This()) void {
                    Self.freePrimaryVectorRebuildWrites(state.ctx.alloc, &state.writes);
                    state.writes.deinit(state.ctx.alloc);
                }

                fn flush(state: *@This()) !void {
                    try Self.flushDensePrimaryVectorRebuildChunkContext(state.ctx, state.index_name, &state.writes);
                }

                fn scanEntry(scan_ctx: ?*anyopaque, key: []const u8, value: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
                    const state: *@This() = @ptrCast(@alignCast(scan_ctx orelse return error.InvalidArgument));
                    if (!db_internal.isBaseDocumentStoreKeyForMode(state.ctx.relational_base_rows, key)) return .@"continue";
                    const doc_value = if (state.ctx.relational_base_rows)
                        try mapper.materializeRelationalRowValueAlloc(state.ctx.alloc, value)
                    else
                        try mapper.materializeDocumentValueAlloc(state.ctx.alloc, value);
                    errdefer state.ctx.alloc.free(doc_value);
                    if (try mapper.extractDenseVectorField(state.ctx.alloc, doc_value, state.field_name, state.dims)) |vector| {
                        state.ctx.alloc.free(vector);
                    } else {
                        state.ctx.alloc.free(doc_value);
                        return .@"continue";
                    }
                    const doc_key = (try internal_keys.decodeStoredDocumentRowKeyAlloc(state.ctx.alloc, key)) orelse {
                        state.ctx.alloc.free(doc_value);
                        return .@"continue";
                    };
                    errdefer state.ctx.alloc.free(doc_key);
                    try state.writes.append(state.ctx.alloc, .{
                        .key = doc_key,
                        .value = doc_value,
                    });
                    state.rebuilt += 1;

                    if (state.writes.items.len >= state.rebuild_chunk_size) try state.flush();
                    return .@"continue";
                }
            };

            var state = ScanState{
                .ctx = ctx,
                .index_name = index_name,
                .field_name = field_name,
                .dims = dims,
                .rebuild_chunk_size = rebuild_chunk_size,
            };
            defer state.deinit();

            try ctx.store.scanWithContext(lower, "", .{}, &state, ScanState.scanEntry);
            if (state.writes.items.len > 0) try state.flush();

            try ctx.index_manager.finishDenseBulkIngestSessionByNameWithOptions(index_name, DB.DerivedAsyncCallbacks.dense_catch_up_finish_options());
            bulk_session_open = false;
            return state.rebuilt;
        }

        fn flushDenseArtifactRebuildChunkContext(
            ctx: anytype,
            index_name: []const u8,
            writes: *std.ArrayListUnmanaged(mapper.DenseEmbeddingWrite),
        ) !void {
            defer Self.freeDenseArtifactRebuildWrites(ctx.alloc, writes);
            if (writes.items.len == 0) return;
            try ctx.index_manager.applyDenseEmbeddingWritesByNameWithOptions(
                ctx.store,
                index_name,
                writes.items,
                .{ .mode = .bulk_ingest },
            );
        }

        fn rebuildDenseIndexFromStoredEmbeddingArtifactsContext(
            ctx: anytype,
            index_name: []const u8,
            rebuild_chunk_size: usize,
        ) !usize {
            const entry = ctx.index_manager.denseIndex(index_name) orelse return 0;
            const expected_name = denseArtifactNameForEntry(entry);
            const expected_dims = entry.dims;

            const lower = try internal_keys.documentRangeLowerAlloc(ctx.alloc, "");
            defer ctx.alloc.free(lower);

            try ctx.index_manager.beginDenseBulkIngestSessionByName(index_name);
            var bulk_session_open = true;
            errdefer if (bulk_session_open) ctx.index_manager.abortDenseBulkIngestSessionByName(index_name);

            const ScanState = struct {
                ctx: @TypeOf(ctx),
                index_name: []const u8,
                expected_name: []const u8,
                expected_dims: u32,
                rebuild_chunk_size: usize,
                writes: std.ArrayListUnmanaged(mapper.DenseEmbeddingWrite) = .empty,
                rebuilt: usize = 0,

                fn deinit(state: *@This()) void {
                    Self.freeDenseArtifactRebuildWrites(state.ctx.alloc, &state.writes);
                    state.writes.deinit(state.ctx.alloc);
                }

                fn flush(state: *@This()) !void {
                    try Self.flushDenseArtifactRebuildChunkContext(state.ctx, state.index_name, &state.writes);
                }

                fn scanEntry(scan_ctx: ?*anyopaque, key: []const u8, value: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
                    const state: *@This() = @ptrCast(@alignCast(scan_ctx orelse return error.InvalidArgument));
                    if (!internal_keys.isInternalUserKey(key)) return .@"continue";

                    var identity = (artifact_ids.decodeEmbeddingArtifactIdentityAlloc(state.ctx.alloc, key) catch |err| switch (err) {
                        error.InvalidInternalUserKey => return .@"continue",
                        else => return err,
                    }) orelse return .@"continue";
                    defer identity.deinit(state.ctx.alloc);
                    if (!std.mem.eql(u8, identity.embedding_name, state.expected_name)) return .@"continue";

                    const dims = enrichment_artifact_codec.decodeDenseEmbeddingDims(value) catch |err| {
                        if (Self.isRecoverableEmbeddingArtifactError(err)) return .@"continue";
                        return err;
                    };
                    if (dims != state.expected_dims) return .@"continue";

                    try state.writes.append(state.ctx.alloc, .{
                        .index_name = try state.ctx.alloc.dupe(u8, state.index_name),
                        .doc_key = try state.ctx.alloc.dupe(u8, identity.doc_key),
                        .artifact_key = try state.ctx.alloc.dupe(u8, key),
                        .vector = &.{},
                    });
                    state.rebuilt += 1;

                    if (state.writes.items.len >= state.rebuild_chunk_size) try state.flush();
                    return .@"continue";
                }
            };

            var state = ScanState{
                .ctx = ctx,
                .index_name = index_name,
                .expected_name = expected_name,
                .expected_dims = expected_dims,
                .rebuild_chunk_size = rebuild_chunk_size,
            };
            defer state.deinit();

            try ctx.store.scanWithContext(lower, "", .{}, &state, ScanState.scanEntry);
            if (state.writes.items.len > 0) try state.flush();

            try ctx.index_manager.finishDenseBulkIngestSessionByNameWithOptions(index_name, DB.DerivedAsyncCallbacks.dense_catch_up_finish_options());
            bulk_session_open = false;
            return state.rebuilt;
        }

        fn flushDenseArtifactRebuildChunk(
            self: *DB,
            alloc: Allocator,
            writes: *std.ArrayListUnmanaged(mapper.DenseEmbeddingWrite),
        ) !void {
            defer freeDenseArtifactRebuildWrites(alloc, writes);
            if (writes.items.len == 0) return;

            for (self.core.index_manager.dense_indexes.items) |entry| {
                try self.core.index_manager.applyDenseEmbeddingWritesByNameWithOptions(
                    self.core.store,
                    entry.config.name,
                    writes.items,
                    .{ .mode = .bulk_ingest },
                );
            }
        }

        fn denseArtifactNameForEntry(entry: anytype) []const u8 {
            return entry.embedding_name orelse entry.config.name;
        }

        fn isRecoverableEmbeddingArtifactError(err: anyerror) bool {
            return switch (err) {
                error.InvalidArtifactHeader,
                error.InvalidArtifactMagic,
                error.UnsupportedArtifactCodecVersion,
                error.InvalidArtifactKind,
                error.InvalidArtifactPayload,
                error.InvalidVectorDimensions,
                error.InvalidSparseEmbedding,
                => true,
                else => false,
            };
        }

        pub fn freeDenseArtifactRebuildWrites(alloc: Allocator, writes: *std.ArrayListUnmanaged(mapper.DenseEmbeddingWrite)) void {
            for (writes.items) |write| {
                alloc.free(write.index_name);
                alloc.free(write.doc_key);
                if (write.parent_doc_key) |parent_doc_key| alloc.free(@constCast(parent_doc_key));
                if (write.artifact_key) |artifact_key| alloc.free(artifact_key);
                if (write.vector.len > 0) alloc.free(write.vector);
            }
            writes.clearRetainingCapacity();
        }

        fn freeSparseArtifactRebuildWrites(alloc: Allocator, writes: *std.ArrayListUnmanaged(mapper.SparseEmbeddingWrite)) void {
            for (writes.items) |write| {
                alloc.free(write.doc_key);
                if (write.artifact_key) |artifact_key| alloc.free(artifact_key);
            }
            writes.clearRetainingCapacity();
        }

        fn flushSparseArtifactRebuildChunkContext(
            ctx: anytype,
            index_name: []const u8,
            writes: *std.ArrayListUnmanaged(mapper.SparseEmbeddingWrite),
        ) !void {
            defer Self.freeSparseArtifactRebuildWrites(ctx.alloc, writes);
            if (writes.items.len == 0) return;

            try ctx.index_manager.applySparseEmbeddingWritesByNameWithOptions(
                ctx.store,
                index_name,
                writes.items,
                .{ .mode = .bulk_ingest },
            );
        }
    };
}

test "db derived async replay reopen preserves applied watermark above retained replay floor" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2}",
        });
        try db.core.saveAppliedSequence("dv_v1", 7);
    }

    {
        var reopened = try DB.open(alloc, std.mem.span(path), .{
            .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer reopened.close();

        const applied = try reopened.core.loadAppliedSequence(alloc, "dv_v1");
        try std.testing.expectEqual(@as(u64, 7), applied);
        try std.testing.expect(reopened.core.nextDerivedAppendSequence() >= 8);
    }
}

test "db derived async replay writer open resumes generated enrichment replay from journal" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense_v1\"}}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"generated vector text\"}" },
            },
            .sync_level = .write,
        });

        const replay_entries = try replay_stream_mod.iterateFrom(alloc, db.core.store, 1);
        defer {
            for (replay_entries) |*entry| entry.deinit(alloc);
            alloc.free(replay_entries);
        }
        try std.testing.expectEqual(@as(usize, 1), replay_entries.len);
    }

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer reopened.close();

    const stats_before = try reopened.stats(alloc);
    defer types.freeDBStats(alloc, stats_before);
    try std.testing.expect(stats_before.enrichment.enabled);
    try std.testing.expectEqual(@as(u64, 1), stats_before.enrichment.target_sequence);

    try reopened.runUntilIdle();

    const stats_after = try reopened.stats(alloc);
    defer types.freeDBStats(alloc, stats_after);
    try std.testing.expectEqual(@as(u64, 2), stats_after.enrichment.applied_sequence);
    try std.testing.expectEqual(@as(u64, 2), stats_after.enrichment.target_sequence);

    const query_vec = try deterministic.interface().embedDense(alloc, "", "generated vector text", 3);
    defer alloc.free(query_vec);

    var result = try reopened.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = query_vec,
            .k = 1,
        },
        .limit = 1,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db derived async replay collectDocumentWrites batches sorted document reads and falls back to inline values" {
    const alloc = std.testing.allocator;

    var backend = mem_backend_mod.Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const stored_a = try replayDocumentStoreKeyAlloc(alloc, "a", false);
    defer alloc.free(stored_a);
    const stored_c = try replayDocumentStoreKeyAlloc(alloc, "c", false);
    defer alloc.free(stored_c);
    try store.putBatch(&.{
        .{ .key = stored_a, .value = "{\"title\":\"alpha\"}" },
        .{ .key = stored_c, .value = "{\"title\":\"charlie\"}" },
    }, &.{});

    const docs = [_]derived_types.DerivedDocument{
        .{ .key = "a", .action = .upsert, .cleaned_value = null },
        .{ .key = "b", .action = .upsert, .cleaned_value = "{\"title\":\"bravo\"}" },
        .{ .key = "c", .action = .upsert, .cleaned_value = null },
    };

    var writes = try collectDocumentWrites(alloc, &store, &docs, .{ .start = "", .end = "" });
    defer writes.deinit();

    try std.testing.expectEqual(@as(usize, 3), writes.items.len);
    try std.testing.expectEqualStrings("a", writes.items[0].key);
    try std.testing.expectEqualStrings("{\"title\":\"alpha\"}", writes.items[0].value);
    try std.testing.expectEqualStrings("b", writes.items[1].key);
    try std.testing.expectEqualStrings("{\"title\":\"bravo\"}", writes.items[1].value);
    try std.testing.expectEqualStrings("c", writes.items[2].key);
    try std.testing.expectEqualStrings("{\"title\":\"charlie\"}", writes.items[2].value);
}

test "db derived async replay collectDocumentWrites skips missing out-of-range replay docs" {
    const alloc = std.testing.allocator;

    var backend = mem_backend_mod.Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const stored_z = try replayDocumentStoreKeyAlloc(alloc, "doc:z", false);
    defer alloc.free(stored_z);
    try store.putBatch(&.{
        .{ .key = stored_z, .value = "{\"title\":\"zeta\"}" },
    }, &.{});

    const docs = [_]derived_types.DerivedDocument{
        .{ .key = "doc:a", .action = .upsert, .cleaned_value = null },
        .{ .key = "doc:z", .action = .upsert, .cleaned_value = null },
    };

    var writes = try collectDocumentWrites(alloc, &store, &docs, .{ .start = "doc:m", .end = "" });
    defer writes.deinit();

    try std.testing.expectEqual(@as(usize, 1), writes.items.len);
    try std.testing.expectEqual(@as(usize, 0), writes.missing_required);
    try std.testing.expectEqualStrings("doc:z", writes.items[0].key);
    try std.testing.expectEqualStrings("{\"title\":\"zeta\"}", writes.items[0].value);
}

test "db derived async replay text replay delete keys include upserted derived document keys" {
    const alloc = std.testing.allocator;

    const docs = [_]derived_types.DerivedDocument{
        .{ .key = "chunk:1", .action = .upsert, .cleaned_value = "{\"text\":\"new\"}" },
        .{ .key = "ignored", .action = .delete },
        .{ .key = "chunk:1", .action = .upsert, .cleaned_value = "{\"text\":\"newer\"}" },
    };
    const deleted = [_][]const u8{"deleted:1"};
    const overwritten = [_][]const u8{ "chunk:1", "overwritten:1" };
    const batch = derived_types.DerivedBatch{
        .documents = &docs,
        .deleted_keys = &deleted,
        .overwritten_doc_keys = &overwritten,
    };

    const keys = try collectTextReplayDeleteKeys(alloc, batch);
    defer alloc.free(keys);

    try std.testing.expectEqual(@as(usize, 3), keys.len);
    try std.testing.expectEqualStrings("deleted:1", keys[0]);
    try std.testing.expectEqualStrings("chunk:1", keys[1]);
    try std.testing.expectEqualStrings("overwritten:1", keys[2]);
}

test "db derived async replay skips dense embedding writes when artifact payload is missing" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var appended_sequence: u64 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2}",
        });

        const stored_key = try internal_keys.documentKeyAlloc(alloc, "doc:a");
        defer alloc.free(stored_key);
        try db.core.store.put(stored_key, "{\"title\":\"alpha\"}");

        const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "dv_v1");
        defer alloc.free(artifact_key);

        const derived_batch = derived_types.DerivedBatch{
            .dense_embeddings = &.{
                .{
                    .index_name = "dv_v1",
                    .doc_key = "doc:a",
                    .artifact_key = artifact_key,
                    .vector = &[_]f32{ 1, 0 },
                },
            },
        };

        appended_sequence = try db.derivedAsyncAppendDerivedBatchRecord(derived_batch);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{});
    defer reopened.close();

    var result = try reopened.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = &[_]f32{ 1, 0 },
            .k = 1,
        },
        .limit = 1,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 0), result.total_hits);

    const dense_applied = try reopened.core.loadAppliedSequence(alloc, "dv_v1");
    try std.testing.expectEqual(appended_sequence, dense_applied);
}

test "db derived async replay skips and deletes corrupt dense embedding artifacts" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var appended_sequence: u64 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2}",
        });

        const stored_key = try internal_keys.documentKeyAlloc(alloc, "doc:a");
        defer alloc.free(stored_key);
        try db.core.store.put(stored_key, "{\"title\":\"alpha\"}");

        const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "dv_v1");
        defer alloc.free(artifact_key);
        try db.core.store.put(artifact_key, "bad-artifact");

        const derived_batch = derived_types.DerivedBatch{
            .dense_embeddings = &.{
                .{
                    .index_name = "dv_v1",
                    .doc_key = "doc:a",
                    .artifact_key = artifact_key,
                    .vector = &[_]f32{ 1, 0 },
                },
            },
        };

        appended_sequence = try db.derivedAsyncAppendDerivedBatchRecord(derived_batch);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{});
    defer reopened.close();

    var result = try reopened.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = &[_]f32{ 1, 0 },
            .k = 1,
        },
        .limit = 1,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 0), result.total_hits);

    const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "dv_v1");
    defer alloc.free(artifact_key);
    try std.testing.expectError(error.NotFound, reopened.core.store.get(alloc, artifact_key));

    const dense_applied = try reopened.core.loadAppliedSequence(alloc, "dv_v1");
    try std.testing.expectEqual(appended_sequence, dense_applied);
}

test "db derived async dense artifact rebuild deletes corrupt stored embedding artifacts" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2}",
    });

    const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "dv_v1");
    defer alloc.free(artifact_key);
    try db.core.store.put(artifact_key, "bad-artifact");

    const rebuilt = try db.rebuildDenseIndexesFromStoredEmbeddingArtifacts(alloc);
    try std.testing.expectEqual(@as(usize, 0), rebuilt);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, artifact_key));
}

test "db derived async dense artifact rebuild write cleanup tolerates artifact-backed empty vectors" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var writes = std.ArrayListUnmanaged(mapper.DenseEmbeddingWrite).empty;
    defer writes.deinit(alloc);

    try writes.append(alloc, .{
        .index_name = try alloc.dupe(u8, "dv_v1"),
        .doc_key = try alloc.dupe(u8, "doc:a"),
        .artifact_key = try alloc.dupe(u8, "artifact:dense:doc:a"),
        .vector = &.{},
    });

    DB.derivedAsyncFreeDenseArtifactRebuildWrites(alloc, &writes);
    try std.testing.expectEqual(@as(usize, 0), writes.items.len);
}

test "db derived async replay applies sparse embeddings from artifact payloads" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.addIndex(.{
            .name = "sp_v1",
            .kind = .sparse_vector,
            .config_json = "{\"field\":\"sparse_embedding\"}",
        });

        const stored_key = try internal_keys.documentKeyAlloc(alloc, "doc:a");
        defer alloc.free(stored_key);
        try db.core.store.put(stored_key, "{\"title\":\"alpha\"}");

        const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "sp_v1");
        defer alloc.free(artifact_key);
        try TestHelpers.putSparseEmbeddingArtifactForTest(&db, alloc, artifact_key, null, &.{ 1, 5 }, &.{ 0.5, 0.75 });

        const derived_batch = derived_types.DerivedBatch{
            .sparse_embeddings = &.{
                .{
                    .index_name = "sp_v1",
                    .doc_key = "doc:a",
                    .artifact_key = artifact_key,
                    .indices = &.{9},
                    .values = &.{9.0},
                },
            },
        };

        _ = try db.derivedAsyncAppendDerivedBatchRecord(derived_batch);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{});
    defer reopened.close();

    var sparse_index = reopened.core.index_manager.sparseIndex("sp_v1").?.index;
    const stats = sparse_index.stats();
    try std.testing.expectEqual(@as(u64, 1), stats.doc_count);
}

test "db derived async replay skips and deletes corrupt sparse embedding artifacts" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var appended_sequence: u64 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.addIndex(.{
            .name = "sp_v1",
            .kind = .sparse_vector,
            .config_json = "{\"field\":\"sparse_embedding\"}",
        });

        const stored_key = try internal_keys.documentKeyAlloc(alloc, "doc:a");
        defer alloc.free(stored_key);
        try db.core.store.put(stored_key, "{\"title\":\"alpha\"}");

        const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "sp_v1");
        defer alloc.free(artifact_key);
        try db.core.store.put(artifact_key, "bad-artifact");

        const derived_batch = derived_types.DerivedBatch{
            .sparse_embeddings = &.{
                .{
                    .index_name = "sp_v1",
                    .doc_key = "doc:a",
                    .artifact_key = artifact_key,
                    .indices = &.{9},
                    .values = &.{9.0},
                },
            },
        };

        appended_sequence = try db.derivedAsyncAppendDerivedBatchRecord(derived_batch);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{});
    defer reopened.close();

    var sparse_index = reopened.core.index_manager.sparseIndex("sp_v1").?.index;
    const stats = sparse_index.stats();
    try std.testing.expectEqual(@as(u64, 0), stats.doc_count);

    const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "sp_v1");
    defer alloc.free(artifact_key);
    try std.testing.expectError(error.NotFound, reopened.core.store.get(alloc, artifact_key));

    const sparse_applied = try reopened.core.loadAppliedSequence(alloc, "sp_v1");
    try std.testing.expectEqual(appended_sequence, sparse_applied);
}

test "db derived async replay batch persists per-index applied sequence watermark" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" },
        },
        .sync_level = .full_index,
    });
    const applied = try db.core.loadAppliedSequence(alloc, "ft_v1");
    try std.testing.expect(applied > 0);
}

test "db derived async replay batch truncates replay logs after managed indexes catch up" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" },
        },
    });

    _ = try TestHelpers.waitForAppliedSequenceAdvance(alloc, &db, "ft_v1", 0);

    var remaining_journal_entries: usize = std.math.maxInt(usize);
    var attempts: usize = 0;
    while (attempts < TestHelpers.default_test_wait_attempts) : (attempts += 1) {
        const journal_entries = try db.core.store.iterateReplayFrom(alloc, 1);
        defer {
            for (journal_entries) |*entry| entry.deinit(alloc);
            alloc.free(journal_entries);
        }
        remaining_journal_entries = journal_entries.len;
        if (remaining_journal_entries == 0) break;
        platform.time.sleepMs(10);
    }
    try std.testing.expectEqual(@as(usize, 0), remaining_journal_entries);
}

test "db derived async replay io_threaded executor processes indexed writes" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .executor = .{ .backend = .io_threaded },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"machine\"}" },
        },
        .sync_level = .full_index,
    });

    var result = try TestHelpers.waitForSearchResult(alloc, &db, .{
        .index_name = "ft_v1",
        .query = .{ .match_all = {} },
    }, 1);
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db derived async replay reopen replays pending derived embeddings from durable log" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var appended_sequence: u64 = 0;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2}",
        });

        const req = types.BatchRequest{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dv_v1\":[1,0]}}" },
            },
        };

        var extracted = try mapper.extractWrite(alloc, req.writes[0].key, req.writes[0].value);
        defer extracted.deinit(alloc);

        const stored_key = try internal_keys.documentKeyAlloc(alloc, req.writes[0].key);
        defer alloc.free(stored_key);
        try db.core.store.putBatch(&.{
            .{ .key = stored_key, .value = extracted.cleaned_value.? },
        }, &.{});
        const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "dv_v1");
        defer alloc.free(artifact_key);
        try TestHelpers.putDenseEmbeddingArtifactForTest(&db, alloc, artifact_key, null, &[_]f32{ 1, 0 });
        extracted.dense_embeddings[0].artifact_key = try alloc.dupe(u8, artifact_key);

        var derived_batch = try db_internal.buildDerivedBatch(alloc, req, &.{extracted}, &.{}, &.{});
        defer derived_types.deinitDerivedBatch(alloc, &derived_batch);

        appended_sequence = try db.derivedAsyncAppendDerivedBatchRecord(derived_batch);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{});
    defer reopened.close();

    var result = try reopened.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = &[_]f32{ 1, 0 },
            .k = 1,
        },
        .limit = 1,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);

    const applied = try reopened.core.loadAppliedSequence(alloc, "dv_v1");
    try std.testing.expectEqual(appended_sequence, applied);
}

test "db derived async replay reopen replays pending derived embeddings with durable lsm primary backend" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var appended_sequence: u64 = 0;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2}",
        });

        const req = types.BatchRequest{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dv_v1\":[1,0]}}" },
            },
        };

        var extracted = try mapper.extractWrite(alloc, req.writes[0].key, req.writes[0].value);
        defer extracted.deinit(alloc);

        const stored_key = try internal_keys.documentKeyAlloc(alloc, req.writes[0].key);
        defer alloc.free(stored_key);
        try db.core.store.putBatch(&.{
            .{ .key = stored_key, .value = extracted.cleaned_value.? },
        }, &.{});
        const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "dv_v1");
        defer alloc.free(artifact_key);
        try TestHelpers.putDenseEmbeddingArtifactForTest(&db, alloc, artifact_key, null, &[_]f32{ 1, 0 });
        extracted.dense_embeddings[0].artifact_key = try alloc.dupe(u8, artifact_key);

        var derived_batch = try db_internal.buildDerivedBatch(alloc, req, &.{extracted}, &.{}, &.{});
        defer derived_types.deinitDerivedBatch(alloc, &derived_batch);

        appended_sequence = try db.derivedAsyncAppendDerivedBatchRecord(derived_batch);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
    });
    defer reopened.close();

    var result = try reopened.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = &[_]f32{ 1, 0 },
            .k = 1,
        },
        .limit = 1,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);

    const applied = try reopened.core.loadAppliedSequence(alloc, "dv_v1");
    try std.testing.expectEqual(appended_sequence, applied);
}

test "db derived async replay replay respects per-index applied watermarks" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var appended_sequence: u64 = 0;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2}",
        });
        try db.addIndex(.{
            .name = "gr_v1",
            .kind = .graph,
            .config_json = "{\"edge_types\":[{\"name\":\"related\"}]}",
        });

        const req = types.BatchRequest{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dv_v1\":[1,0]}}" },
            },
        };

        var extracted = try mapper.extractWrite(alloc, req.writes[0].key, req.writes[0].value);
        defer extracted.deinit(alloc);

        const stored_key = try internal_keys.documentKeyAlloc(alloc, req.writes[0].key);
        defer alloc.free(stored_key);
        try db.core.store.putBatch(&.{
            .{ .key = stored_key, .value = extracted.cleaned_value.? },
        }, &.{});
        const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "dv_v1");
        defer alloc.free(artifact_key);
        try TestHelpers.putDenseEmbeddingArtifactForTest(&db, alloc, artifact_key, null, &[_]f32{ 1, 0 });
        extracted.dense_embeddings[0].artifact_key = try alloc.dupe(u8, artifact_key);

        var derived_batch = try db_internal.buildDerivedBatch(alloc, req, &.{extracted}, &.{}, &.{});
        defer derived_types.deinitDerivedBatch(alloc, &derived_batch);

        appended_sequence = try db.derivedAsyncAppendDerivedBatchRecord(derived_batch);

        try db.core.saveAppliedSequence("dv_v1", appended_sequence);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer reopened.close();

    var result = try reopened.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = &[_]f32{ 1, 0 },
            .k = 1,
        },
        .limit = 1,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 0), result.total_hits);

    const dense_applied = try reopened.core.loadAppliedSequence(alloc, "dv_v1");
    try std.testing.expectEqual(appended_sequence, dense_applied);

    const graph_applied = try reopened.core.loadAppliedSequence(alloc, "gr_v1");
    try std.testing.expectEqual(@as(u64, 0), graph_applied);
}

test "db derived async replay replay applies dense embeddings from artifact payloads" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2}",
        });

        const stored_key = try internal_keys.documentKeyAlloc(alloc, "doc:a");
        defer alloc.free(stored_key);
        try db.core.store.put(stored_key, "{\"title\":\"alpha\"}");

        const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "dv_v1");
        defer alloc.free(artifact_key);
        try TestHelpers.putDenseEmbeddingArtifactForTest(&db, alloc, artifact_key, null, &[_]f32{ 0, 1 });

        const derived_batch = derived_types.DerivedBatch{
            .dense_embeddings = &.{
                .{
                    .index_name = "dv_v1",
                    .doc_key = "doc:a",
                    .artifact_key = artifact_key,
                    .vector = &[_]f32{ 1, 0 },
                },
            },
        };

        _ = try db.derivedAsyncAppendDerivedBatchRecord(derived_batch);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{});
    defer reopened.close();

    var result = try reopened.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = &[_]f32{ 0, 1 },
            .k = 1,
        },
        .limit = 1,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db derived async dense artifact rebuild rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded repairs external dense doc gaps on reopen" {
    const DB = @import("mod.zig").DB;
    const threadedIo = db_internal.threadedIo;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
        });
        try db.addIndex(.{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}" },
                .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_embeddings\":{\"dense_idx\":[0,1,0]}}" },
                .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_embeddings\":{\"dense_idx\":[0.9,0.1,0]}}" },
            },
            .sync_level = .full_index,
        });
    }

    const dense_index_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dense_idx", .{std.mem.span(path)});
    defer alloc.free(dense_index_path);
    var io_impl = threadedIo();
    defer io_impl.deinit();
    try std.Io.Dir.cwd().deleteTree(io_impl.io(), dense_index_path);

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
    });
    defer reopened.close();

    {
        const stats = try reopened.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(@as(u64, 3), stats.doc_count);
        var dense_doc_count: ?u64 = null;
        for (stats.indexes) |index| {
            if (!std.mem.eql(u8, index.name, "dense_idx")) continue;
            dense_doc_count = index.doc_count;
        }
        try std.testing.expectEqual(@as(?u64, 0), dense_doc_count);
    }

    const rebuilt = try reopened.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc);
    try std.testing.expect(rebuilt > 0);
    try reopened.runUntilIdle();

    const stats = try reopened.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    var dense_doc_count: ?u64 = null;
    for (stats.indexes) |index| {
        if (!std.mem.eql(u8, index.name, "dense_idx")) continue;
        dense_doc_count = index.doc_count;
    }
    try std.testing.expectEqual(@as(?u64, 3), dense_doc_count);

    var result = try reopened.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{
            .vector = &.{ 1.0, 0.0, 0.0 },
            .k = 3,
        } },
        .limit = 3,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 3), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);

    try std.testing.expectEqual(@as(usize, 0), try reopened.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc));
}

test "db derived async dense artifact rebuild dense artifact rebuild preserves stable vector ids distinct from ordinals" {
    const DB = @import("mod.zig").DB;
    const threadedIo = db_internal.threadedIo;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}" },
                .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_embeddings\":{\"dense_idx\":[0,1,0]}}" },
            },
            .sync_level = .full_index,
        });

        var txn = try db.core.store.beginProbeTxn();
        defer txn.abort();
        const ordinal = (try doc_identity.lookupOrdinalTxn(alloc, &txn, "doc:b")) orelse return error.TestUnexpectedResult;
        try std.testing.expectEqual(@as(doc_identity.DocOrdinal, 2), ordinal);
        const stable_vector_id = index_manager_mod.deterministicDenseVectorId("doc:b");
        try std.testing.expect(stable_vector_id != @as(u64, ordinal));
        try std.testing.expectEqual(@as(?u64, stable_vector_id), try db.core.index_manager.lookupDenseVectorId(db.core.store, "dense_idx", "doc:b"));
    }

    const dense_index_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dense_idx", .{std.mem.span(path)});
    defer alloc.free(dense_index_path);
    var io_impl = threadedIo();
    defer io_impl.deinit();
    try std.Io.Dir.cwd().deleteTree(io_impl.io(), dense_index_path);

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
    });
    defer reopened.close();

    try std.testing.expect(try reopened.hasPendingDenseArtifactRebuild(alloc));
    try std.testing.expectEqual(@as(usize, 2), try reopened.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc));
    try std.testing.expect(!(try reopened.hasPendingDenseArtifactRebuild(alloc)));

    var txn = try reopened.core.store.beginProbeTxn();
    defer txn.abort();
    const ordinal = (try doc_identity.lookupOrdinalTxn(alloc, &txn, "doc:b")) orelse return error.TestUnexpectedResult;
    const stable_vector_id = index_manager_mod.deterministicDenseVectorId("doc:b");
    try std.testing.expect(stable_vector_id != @as(u64, ordinal));
    try std.testing.expectEqual(@as(?u64, stable_vector_id), try reopened.core.index_manager.lookupDenseVectorId(reopened.core.store, "dense_idx", "doc:b"));

    const vector_ids = try reopened.core.index_manager.lookupDenseVectorIdsForOrdinalsAlloc(alloc, reopened.core.store, "dense_idx", &.{ordinal});
    defer alloc.free(vector_ids);
    try std.testing.expectEqual(@as(usize, 1), vector_ids.len);
    try std.testing.expectEqual(stable_vector_id, vector_ids[0]);

    const dense_entry = reopened.core.index_manager.denseIndex("dense_idx") orelse return error.TestUnexpectedResult;
    const stable_metadata = (try dense_entry.index.getMetadata(stable_vector_id)) orelse return error.TestUnexpectedResult;
    defer alloc.free(stable_metadata);
    try std.testing.expectEqualStrings("doc:b", stable_metadata);
    try std.testing.expectEqual(@as(?[]u8, null), try dense_entry.index.getMetadata(@as(u64, ordinal)));
}

test "db derived async dense artifact rebuild dense artifact rebuild force-resets corrupt external dense structure" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true,\"leaf_size\":2}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}" },
                .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_embeddings\":{\"dense_idx\":[0,1,0]}}" },
                .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_embeddings\":{\"dense_idx\":[0.9,0.1,0]}}" },
                .{ .key = "doc:d", .value = "{\"title\":\"delta\",\"_embeddings\":{\"dense_idx\":[0,0,1]}}" },
                .{ .key = "doc:e", .value = "{\"title\":\"epsilon\",\"_embeddings\":{\"dense_idx\":[0.8,0.2,0]}}" },
                .{ .key = "doc:f", .value = "{\"title\":\"zeta\",\"_embeddings\":{\"dense_idx\":[0.7,0.3,0]}}" },
            },
            .sync_level = .full_index,
        });

        const dense_entry = db.core.index_manager.denseIndex("dense_idx").?;
        try std.testing.expect(dense_entry.index.stats().node_count > 1);

        var read_txn = try dense_entry.index.beginReadTxn();
        defer read_txn.abort();
        var root = try dense_entry.index.loadNode(&read_txn, dense_entry.index.metadata.root_node);
        defer root.deinit(alloc);
        try std.testing.expect(!root.is_leaf);
        try dense_entry.index.deleteNodeHeaderForTest(root.children[0]);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();

    try std.testing.expect(try reopened.hasPendingDenseArtifactRebuild(alloc));

    const rebuilt = try reopened.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc);
    try std.testing.expectEqual(@as(usize, 6), rebuilt);
    try std.testing.expect(!(try reopened.hasPendingDenseArtifactRebuild(alloc)));

    const stats = try reopened.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    var dense_doc_count: ?u64 = null;
    for (stats.indexes) |index| {
        if (std.mem.eql(u8, index.name, "dense_idx")) dense_doc_count = index.doc_count;
    }
    try std.testing.expectEqual(@as(?u64, 6), dense_doc_count);

    var result = try reopened.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{
            .vector = &.{ 1.0, 0.0, 0.0 },
            .k = 3,
        } },
        .limit = 3,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 3), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db derived async dense artifact rebuild dense artifact rebuild resumes from persisted state" {
    const DB = @import("mod.zig").DB;
    const threadedIo = db_internal.threadedIo;
    const alloc = std.testing.allocator;
    const doc_count: usize = 8;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
        });

        for (0..doc_count) |i| {
            const doc_id = try std.fmt.allocPrint(alloc, "doc:{d:0>5}", .{i});
            defer alloc.free(doc_id);
            const stored_key = try internal_keys.documentKeyAlloc(alloc, doc_id);
            defer alloc.free(stored_key);
            const stored_value = try std.fmt.allocPrint(alloc, "{{\"title\":\"doc-{d}\"}}", .{i});
            defer alloc.free(stored_value);
            try db.core.store.putBatch(&.{
                .{ .key = stored_key, .value = stored_value },
            }, &.{});

            const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, doc_id, "dense_idx");
            defer alloc.free(artifact_key);
            try TestHelpers.putDenseEmbeddingArtifactForTest(&db, alloc, artifact_key, null, &[_]f32{
                @floatFromInt(i + 1),
                0,
                0,
            });
        }
    }

    const dense_index_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dense_idx", .{std.mem.span(path)});
    defer alloc.free(dense_index_path);
    var io_impl = threadedIo();
    defer io_impl.deinit();
    try std.Io.Dir.cwd().deleteTree(io_impl.io(), dense_index_path);
    const rebuild_state = backfill_state_mod.RebuildState.init(dense_index_path);

    {
        var interrupted = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer interrupted.close();

        const ResumeFailureCtx = struct {
            rebuild_state: backfill_state_mod.RebuildState,
            persisted_calls: usize = 0,

            fn persistAndFail(ctx: *anyopaque, last_key: []const u8) !void {
                const failure: *@This() = @ptrCast(@alignCast(ctx));
                try failure.rebuild_state.update(last_key);
                failure.persisted_calls += 1;
                return error.TestInjectedFailure;
            }
        };
        var failure_ctx = ResumeFailureCtx{
            .rebuild_state = rebuild_state,
        };

        try std.testing.expectError(
            error.TestInjectedFailure,
            interrupted.derivedAsyncRebuildDenseIndexesFromStoredEmbeddingArtifactsResumeWithProgress(
                alloc,
                null,
                null,
                null,
                null,
                null,
                &failure_ctx,
                ResumeFailureCtx.persistAndFail,
                4,
                1,
            ),
        );
        try std.testing.expectEqual(@as(usize, 1), failure_ctx.persisted_calls);

        const partial_stats = try interrupted.stats(alloc);
        defer types.freeDBStats(alloc, partial_stats);
        var partial_doc_count: ?u64 = null;
        for (partial_stats.indexes) |index| {
            if (!std.mem.eql(u8, index.name, "dense_idx")) continue;
            partial_doc_count = index.doc_count;
        }
        try std.testing.expectEqual(@as(?u64, 4), partial_doc_count);
        const persisted_resume = try rebuild_state.check(alloc);
        defer if (persisted_resume) |buf| alloc.free(buf);
        try std.testing.expect(persisted_resume != null);
    }

    {
        var resumed = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer resumed.close();

        const rebuilt = try resumed.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc);
        try std.testing.expectEqual(@as(usize, 4), rebuilt);

        try std.testing.expect((try rebuild_state.check(alloc)) == null);

        const stats = try resumed.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        var dense_doc_count: ?u64 = null;
        for (stats.indexes) |index| {
            if (!std.mem.eql(u8, index.name, "dense_idx")) continue;
            dense_doc_count = index.doc_count;
        }
        try std.testing.expectEqual(@as(?u64, doc_count), dense_doc_count);
    }
}

test "db derived async dense artifact rebuild dense artifact rebuild progress counts source artifacts across multiple consumer indexes" {
    const DB = @import("mod.zig").DB;
    const threadedIo = db_internal.threadedIo;
    const alloc = std.testing.allocator;
    const doc_count: usize = 3;
    const shared_cfg = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true,\"embedding_name\":\"shared_dense_v1\"}";

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "dense_a",
            .kind = .dense_vector,
            .config_json = shared_cfg,
        });
        try db.addIndex(.{
            .name = "dense_b",
            .kind = .dense_vector,
            .config_json = shared_cfg,
        });

        for (0..doc_count) |i| {
            const doc_id = try std.fmt.allocPrint(alloc, "doc:{d:0>5}", .{i});
            defer alloc.free(doc_id);
            const stored_key = try internal_keys.documentKeyAlloc(alloc, doc_id);
            defer alloc.free(stored_key);
            const stored_value = try std.fmt.allocPrint(alloc, "{{\"title\":\"doc-{d}\"}}", .{i});
            defer alloc.free(stored_value);
            try db.core.store.putBatch(&.{
                .{ .key = stored_key, .value = stored_value },
            }, &.{});

            const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, doc_id, "shared_dense_v1");
            defer alloc.free(artifact_key);
            try TestHelpers.putDenseEmbeddingArtifactForTest(&db, alloc, artifact_key, null, &[_]f32{
                @floatFromInt(i + 1),
                0,
                0,
            });
        }
    }

    var io_impl = threadedIo();
    defer io_impl.deinit();
    const dense_a_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dense_a", .{std.mem.span(path)});
    defer alloc.free(dense_a_path);
    try std.Io.Dir.cwd().deleteTree(io_impl.io(), dense_a_path);
    const dense_b_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dense_b", .{std.mem.span(path)});
    defer alloc.free(dense_b_path);
    try std.Io.Dir.cwd().deleteTree(io_impl.io(), dense_b_path);

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();

    const Capture = struct {
        seen: usize = 0,
        last: db_internal.ReplayProgress = .{},

        fn run(ptr: *anyopaque, _: []const u8, progress: db_internal.ReplayProgress) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.seen += 1;
            self.last = progress;
        }
    };
    var capture = Capture{};

    const rebuilt = try reopened.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeededWithProgress(alloc, &capture, Capture.run);
    try std.testing.expectEqual(doc_count, rebuilt);
    try std.testing.expect(capture.seen > 0);
    try std.testing.expectEqual(@as(u64, doc_count), capture.last.target_sequence);
    try std.testing.expectEqual(@as(u64, doc_count), capture.last.applied_entries);
    try std.testing.expectEqual(@as(u64, doc_count), capture.last.sequence);
    try reopened.runUntilIdle();

    const stats = try reopened.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    var dense_a_count: ?u64 = null;
    var dense_b_count: ?u64 = null;
    for (stats.indexes) |index| {
        if (std.mem.eql(u8, index.name, "dense_a")) dense_a_count = index.doc_count;
        if (std.mem.eql(u8, index.name, "dense_b")) dense_b_count = index.doc_count;
    }
    try std.testing.expectEqual(@as(?u64, doc_count), dense_a_count);
    try std.testing.expectEqual(@as(?u64, doc_count), dense_b_count);
}

test "db derived async dense artifact rebuild chunk-backed dense artifact rebuild stays pending until all chunk artifacts are rebuilt" {
    const DB = @import("mod.zig").DB;
    const threadedIo = db_internal.threadedIo;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .enrichment = .{
                .owner_id = "worker-a",
                .dense_embedder = deterministic.interface(),
            },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2,\"embedding_name\":\"chunk_dense_v1\"}}",
        });

        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"abcdefghijklmno\"}" }},
            .sync_level = .write,
        });
        try db.runUntilIdle();
        try std.testing.expectEqual(@as(u64, 3), db.core.index_manager.denseIndex("dv_v1").?.index.metadata.active_count);
    }

    const dense_index_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dv_v1", .{std.mem.span(path)});
    defer alloc.free(dense_index_path);
    var io_impl = threadedIo();
    defer io_impl.deinit();
    try std.Io.Dir.cwd().deleteTree(io_impl.io(), dense_index_path);
    const rebuild_state = backfill_state_mod.RebuildState.init(dense_index_path);

    {
        var interrupted = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer interrupted.close();

        const ResumeFailureCtx = struct {
            rebuild_state: backfill_state_mod.RebuildState,
            persisted_calls: usize = 0,

            fn persistAndFail(ctx: *anyopaque, last_key: []const u8) !void {
                const failure: *@This() = @ptrCast(@alignCast(ctx));
                try failure.rebuild_state.update(last_key);
                failure.persisted_calls += 1;
                return error.TestInjectedFailure;
            }
        };
        var failure_ctx = ResumeFailureCtx{
            .rebuild_state = rebuild_state,
        };

        try std.testing.expectError(
            error.TestInjectedFailure,
            interrupted.derivedAsyncRebuildDenseIndexesFromStoredEmbeddingArtifactsResumeWithProgress(
                alloc,
                null,
                null,
                null,
                null,
                null,
                &failure_ctx,
                ResumeFailureCtx.persistAndFail,
                1,
                1,
            ),
        );
        try std.testing.expectEqual(@as(usize, 1), failure_ctx.persisted_calls);
        try std.testing.expectEqual(@as(u64, 1), interrupted.core.index_manager.denseIndex("dv_v1").?.index.metadata.active_count);
        const persisted_resume = try rebuild_state.check(alloc);
        defer if (persisted_resume) |buf| alloc.free(buf);
        try std.testing.expect(persisted_resume != null);
    }

    {
        var resumed = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer resumed.close();

        try std.testing.expect(try resumed.hasPendingDenseArtifactRebuild(alloc));

        const Capture = struct {
            seen: usize = 0,
            last: db_internal.ReplayProgress = .{},

            fn run(ptr: *anyopaque, _: []const u8, progress: db_internal.ReplayProgress) !void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                self.seen += 1;
                self.last = progress;
            }
        };
        var capture = Capture{};

        const rebuilt = try resumed.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeededWithProgress(alloc, &capture, Capture.run);
        try std.testing.expectEqual(@as(usize, 2), rebuilt);
        try std.testing.expect(capture.seen > 0);
        try std.testing.expectEqual(@as(u64, 3), capture.last.target_sequence);
        try std.testing.expectEqual(@as(?u64, 3), resumed.core.index_manager.denseIndex("dv_v1").?.index.metadata.active_count);
        try std.testing.expect((try rebuild_state.check(alloc)) == null);
        try std.testing.expect(!(try resumed.hasPendingDenseArtifactRebuild(alloc)));
    }
}

test "db derived async dense artifact rebuild dense artifact rebuild does not let resumed targets skip fresh targets" {
    const DB = @import("mod.zig").DB;
    const threadedIo = db_internal.threadedIo;
    const alloc = std.testing.allocator;
    const doc_count: usize = 4;
    const shared_cfg = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true,\"embedding_name\":\"shared_dense_v1\"}";

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "dense_a",
            .kind = .dense_vector,
            .config_json = shared_cfg,
        });
        try db.addIndex(.{
            .name = "dense_b",
            .kind = .dense_vector,
            .config_json = shared_cfg,
        });

        for (0..doc_count) |i| {
            const doc_id = try std.fmt.allocPrint(alloc, "doc:{d:0>5}", .{i});
            defer alloc.free(doc_id);
            const stored_key = try internal_keys.documentKeyAlloc(alloc, doc_id);
            defer alloc.free(stored_key);
            const stored_value = try std.fmt.allocPrint(alloc, "{{\"title\":\"doc-{d}\"}}", .{i});
            defer alloc.free(stored_value);
            try db.core.store.putBatch(&.{
                .{ .key = stored_key, .value = stored_value },
            }, &.{});

            const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, doc_id, "shared_dense_v1");
            defer alloc.free(artifact_key);
            try TestHelpers.putDenseEmbeddingArtifactForTest(&db, alloc, artifact_key, null, &[_]f32{
                @floatFromInt(i + 1),
                0,
                0,
            });
        }
    }

    var io_impl = threadedIo();
    defer io_impl.deinit();
    const dense_a_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dense_a", .{std.mem.span(path)});
    defer alloc.free(dense_a_path);
    try std.Io.Dir.cwd().deleteTree(io_impl.io(), dense_a_path);
    const dense_b_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dense_b", .{std.mem.span(path)});
    defer alloc.free(dense_b_path);
    try std.Io.Dir.cwd().deleteTree(io_impl.io(), dense_b_path);
    const rebuild_state = backfill_state_mod.RebuildState.init(dense_a_path);

    {
        var interrupted = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer interrupted.close();

        const ResumeFailureCtx = struct {
            rebuild_state: backfill_state_mod.RebuildState,
            persisted_calls: usize = 0,

            fn persistAndFail(ctx: *anyopaque, last_key: []const u8) !void {
                const failure: *@This() = @ptrCast(@alignCast(ctx));
                try failure.rebuild_state.update(last_key);
                failure.persisted_calls += 1;
                return error.TestInjectedFailure;
            }
        };
        var failure_ctx = ResumeFailureCtx{
            .rebuild_state = rebuild_state,
        };

        const targets = [_]DenseArtifactRebuildTarget{
            .{ .dense_index_idx = 0 },
        };

        try std.testing.expectError(
            error.TestInjectedFailure,
            interrupted.derivedAsyncRebuildDenseIndexesFromStoredEmbeddingArtifactsResumeWithProgress(
                alloc,
                null,
                &targets,
                null,
                null,
                null,
                &failure_ctx,
                ResumeFailureCtx.persistAndFail,
                2,
                1,
            ),
        );
        try std.testing.expectEqual(@as(usize, 1), failure_ctx.persisted_calls);
    }

    {
        var resumed = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer resumed.close();

        const rebuilt = try resumed.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc);
        try std.testing.expectEqual(doc_count, rebuilt);

        const stats = try resumed.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        var dense_a_count: ?u64 = null;
        var dense_b_count: ?u64 = null;
        for (stats.indexes) |index| {
            if (std.mem.eql(u8, index.name, "dense_a")) dense_a_count = index.doc_count;
            if (std.mem.eql(u8, index.name, "dense_b")) dense_b_count = index.doc_count;
        }
        try std.testing.expectEqual(@as(?u64, doc_count), dense_a_count);
        try std.testing.expectEqual(@as(?u64, doc_count), dense_b_count);
    }
}

test "db derived async dense artifact rebuild dense artifact rebuild ignores stale wrong-dimension artifacts when counting progress" {
    const DB = @import("mod.zig").DB;
    const threadedIo = db_internal.threadedIo;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true,\"embedding_name\":\"shared_dense_v1\"}",
        });

        const stored_key = try internal_keys.documentKeyAlloc(alloc, "doc:a");
        defer alloc.free(stored_key);
        try db.core.store.putBatch(&.{
            .{ .key = stored_key, .value = "{\"title\":\"alpha\"}" },
        }, &.{});

        const valid_artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "shared_dense_v1");
        defer alloc.free(valid_artifact_key);
        try TestHelpers.putDenseEmbeddingArtifactForTest(&db, alloc, valid_artifact_key, null, &[_]f32{ 1, 0, 0 });

        const stale_artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:stale", "shared_dense_v1");
        defer alloc.free(stale_artifact_key);
        try TestHelpers.putDenseEmbeddingArtifactForTest(&db, alloc, stale_artifact_key, null, &[_]f32{ 1, 0 });
    }

    var io_impl = threadedIo();
    defer io_impl.deinit();
    const dense_index_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dense_idx", .{std.mem.span(path)});
    defer alloc.free(dense_index_path);
    try std.Io.Dir.cwd().deleteTree(io_impl.io(), dense_index_path);

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();

    const Capture = struct {
        seen: usize = 0,
        last: db_internal.ReplayProgress = .{},

        fn run(ptr: *anyopaque, _: []const u8, progress: db_internal.ReplayProgress) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.seen += 1;
            self.last = progress;
        }
    };
    var capture = Capture{};

    const rebuilt = try reopened.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeededWithProgress(alloc, &capture, Capture.run);
    try std.testing.expectEqual(@as(usize, 1), rebuilt);
    try std.testing.expect(capture.seen > 0);
    try std.testing.expectEqual(@as(u64, 1), capture.last.target_sequence);
    try std.testing.expect(!(try reopened.hasPendingDenseArtifactRebuild(alloc)));

    const stats = try reopened.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    var dense_doc_count: ?u64 = null;
    for (stats.indexes) |index| {
        if (std.mem.eql(u8, index.name, "dense_idx")) dense_doc_count = index.doc_count;
    }
    try std.testing.expectEqual(@as(?u64, 1), dense_doc_count);
}

test "db derived async dense artifact rebuild dense artifact rebuild clears stale persisted state when no valid artifacts remain" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true,\"embedding_name\":\"shared_dense_v1\"}",
        });

        const rebuild_root_path = try db.derivedAsyncDenseIndexRebuildStatePathAlloc(alloc, "dense_idx");
        defer alloc.free(rebuild_root_path);
        const rebuild_state = backfill_state_mod.RebuildState.init(rebuild_root_path);
        try rebuild_state.update("doc:z");
    }

    {
        var reopened = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer reopened.close();

        try std.testing.expect(!(try reopened.hasPendingDenseArtifactRebuild(alloc)));
        try std.testing.expectEqual(@as(usize, 0), try reopened.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc));

        const rebuild_root_path = try reopened.derivedAsyncDenseIndexRebuildStatePathAlloc(alloc, "dense_idx");
        defer alloc.free(rebuild_root_path);
        const rebuild_state = backfill_state_mod.RebuildState.init(rebuild_root_path);
        try std.testing.expect((try rebuild_state.check(alloc)) == null);
    }
}

test "db derived async dense artifact rebuild dense artifact rebuild waits for replay debt instead of raw doc count alone" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var appended_sequence: u64 = 0;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2,\"external\":true}",
        });

        const stored_key = try internal_keys.documentKeyAlloc(alloc, "doc:a");
        defer alloc.free(stored_key);
        try db.core.store.putBatch(&.{
            .{ .key = stored_key, .value = "{\"title\":\"alpha\"}" },
        }, &.{});

        const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "dv_v1");
        defer alloc.free(artifact_key);
        try TestHelpers.putDenseEmbeddingArtifactForTest(&db, alloc, artifact_key, null, &[_]f32{ 1, 0 });

        var dense_embeddings = try alloc.alloc(derived_types.DerivedDenseEmbeddingWrite, 1);
        var batch = derived_types.DerivedBatch{
            .dense_embeddings = dense_embeddings,
        };
        defer derived_types.deinitDerivedBatch(alloc, &batch);
        dense_embeddings[0] = .{
            .index_name = try alloc.dupe(u8, "dv_v1"),
            .doc_key = try alloc.dupe(u8, "doc:a"),
            .artifact_key = try alloc.dupe(u8, artifact_key),
            .vector = try alloc.dupe(f32, &[_]f32{ 1, 0 }),
        };

        appended_sequence = db.core.store.reserveNextReplaySequence(1);
        var record = try change_journal_mod.recordFromDerivedBatch(alloc, batch, appended_sequence);
        defer change_journal_mod.deinitRecord(alloc, &record);
        const encoded = try change_journal_mod.encodeRecord(alloc, record);
        defer alloc.free(encoded);
        try replay_stream_mod.appendOpaque(alloc, db.core.store, appended_sequence, encoded);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();

    const rebuilt = try reopened.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc);
    try std.testing.expectEqual(@as(usize, 0), rebuilt);

    const rebuild_state_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dv_v1", .{std.mem.span(path)});
    defer alloc.free(rebuild_state_path);
    const rebuild_state = backfill_state_mod.RebuildState.init(rebuild_state_path);
    try std.testing.expect((try rebuild_state.check(alloc)) == null);

    const replay_debt = try reopened.listDerivedReplayDebt(alloc);
    defer {
        for (replay_debt) |*status| status.deinit(alloc);
        alloc.free(replay_debt);
    }
    try std.testing.expectEqual(@as(usize, 1), replay_debt.len);
    try std.testing.expectEqual(@as(u64, 0), replay_debt[0].applied_sequence);
    try std.testing.expectEqual(appended_sequence, replay_debt[0].target_sequence);
    try std.testing.expect(replay_debt[0].catch_up_required);
}

test "db derived async dense startup catch-up defaults keep more than one cache entry" {
    try std.testing.expect(denseCatchUpStartupCacheNodes() > 1);
    try std.testing.expect(denseCatchUpStartupCacheVectors() > 1);
}

test "db derived async io_threaded executor stress applies explicit dense embeddings on lsm backend" {
    const DB = @import("mod.zig").DB;
    if (db_internal.getenv("ANTFLY_STRESS_DB_DENSE_REPRO") == null) return error.SkipZigTest;

    const alloc = std.testing.allocator;
    const dims = index_manager_mod.stressEnvUsize("ANTFLY_STRESS_DENSE_DIMS", 256);
    const total_docs = index_manager_mod.stressEnvUsize("ANTFLY_STRESS_DENSE_DOCS", 4096);
    const batch_size = @max(@as(usize, 1), index_manager_mod.stressEnvUsize("ANTFLY_STRESS_DENSE_BATCH", 256));
    const progress_interval = @max(batch_size, index_manager_mod.stressEnvUsize("ANTFLY_STRESS_DENSE_PROGRESS", batch_size * 8));
    const dense_backend = TestHelpers.stressDenseBackend();

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
        .executor = .{ .backend = .io_threaded },
        .index_backends = .{
            .dense_storage_backend = dense_backend,
        },
    });
    defer db.close();

    const config_json = try std.fmt.allocPrint(alloc, "{{\"field\":\"embedding\",\"dims\":{d},\"metric\":\"l2_squared\"}}", .{dims});
    defer alloc.free(config_json);
    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = config_json,
    });

    var queued_docs: usize = 0;
    while (queued_docs < total_docs) {
        const end = @min(queued_docs + batch_size, total_docs);

        var writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
        defer {
            for (writes.items) |write| {
                alloc.free(@constCast(write.key));
                alloc.free(@constCast(write.value));
            }
            writes.deinit(alloc);
        }

        for (queued_docs..end) |doc_index| {
            const doc_key = try std.fmt.allocPrint(alloc, "doc:{d:0>8}", .{doc_index});
            const doc_json = try TestHelpers.allocStressDenseDocJson(alloc, dims, doc_index);
            try writes.append(alloc, .{
                .key = doc_key,
                .value = doc_json,
            });
        }

        try db.batch(.{
            .writes = writes.items,
            .sync_level = .write,
        });
        queued_docs = end;

        if (queued_docs % progress_interval == 0 or queued_docs == total_docs) {
            try db.runDerivedUntil(db.core.nextDerivedSequence());
            const entry = db.core.index_manager.denseIndex("dv_v1") orelse return error.IndexNotFound;
            try std.testing.expectEqual(@as(u64, @intCast(queued_docs)), entry.index.stats().active_count);
        }
    }

    try db.runDerivedUntil(db.core.nextDerivedSequence());
    try db.runUntilIdle();

    const entry = db.core.index_manager.denseIndex("dv_v1") orelse return error.IndexNotFound;
    try std.testing.expectEqual(@as(u64, @intCast(total_docs)), entry.index.stats().active_count);

    const first_vector_id = (try db.core.index_manager.lookupDenseVectorId(db.core.store, "dv_v1", "doc:00000000")) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(index_manager_mod.deterministicDenseVectorId("doc:00000000"), first_vector_id);
    const first_doc = (try db.core.index_manager.lookupDenseDocKey(db.core.store, "dv_v1", first_vector_id)) orelse return error.TestUnexpectedResult;
    defer alloc.free(first_doc);
    try std.testing.expectEqualStrings("doc:00000000", first_doc);

    const expected_last_doc = try std.fmt.allocPrint(alloc, "doc:{d:0>8}", .{total_docs - 1});
    defer alloc.free(expected_last_doc);
    const last_vector_id = (try db.core.index_manager.lookupDenseVectorId(db.core.store, "dv_v1", expected_last_doc)) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(index_manager_mod.deterministicDenseVectorId(expected_last_doc), last_vector_id);
    const last_doc = (try db.core.index_manager.lookupDenseDocKey(db.core.store, "dv_v1", last_vector_id)) orelse return error.TestUnexpectedResult;
    defer alloc.free(last_doc);
    try std.testing.expectEqualStrings(expected_last_doc, last_doc);

    var read_txn = try entry.index.beginReadTxn();
    defer read_txn.abort();
    const last_vector = try entry.index.getVector(&read_txn, last_vector_id);
    defer alloc.free(last_vector);

    const expected_last_vector = try alloc.alloc(f32, dims);
    defer alloc.free(expected_last_vector);
    index_manager_mod.fillStressDenseVector(expected_last_vector, total_docs - 1);
    try std.testing.expectEqualSlices(f32, expected_last_vector, last_vector);
}

test "db derived async hbc posting lazy versus eager profile benchmark" {
    const DB = @import("mod.zig").DB;
    const BatchProfile = @import("mod.zig").BatchProfile;
    const monotonicTimeNs = platform.time.monotonicNs;
    if (!TestHelpers.profileBenchTestsEnabled()) return error.SkipZigTest;
    const alloc = std.testing.allocator;

    const dims: usize = @max(@as(usize, 1), readEnvUsize("ANTFLY_HBC_POSTING_BENCH_DIMS", 16));
    const doc_count: usize = @max(@as(usize, 1), readEnvUsize("ANTFLY_HBC_POSTING_BENCH_DOCS", 1024));
    const Mode = struct {
        name: []const u8,
        lazy: bool,
    };
    const modes = [_]Mode{
        .{
            .name = "eager",
            .lazy = false,
        },
        .{
            .name = "lazy",
            .lazy = true,
        },
    };

    const BenchWrites = struct {
        fn fill(
            allocator: Allocator,
            writes: []types.BatchWrite,
            vector_buf: []f32,
            salt: usize,
        ) !void {
            for (writes, 0..) |*write, doc_id| {
                var norm_sq: f32 = 0;
                for (vector_buf, 0..) |*slot, dim| {
                    const raw: u32 = @intCast(((doc_id + salt) * 1103515245 + dim * 2654435761 + 19) % 1000);
                    const centered = (@as(f32, @floatFromInt(raw)) / 500.0) - 1.0;
                    slot.* = centered;
                    norm_sq += centered * centered;
                }
                const inv_norm: f32 = 1.0 / @sqrt(norm_sq);
                for (vector_buf) |*slot| slot.* *= inv_norm;

                const key = try std.fmt.allocPrint(allocator, "doc:{d}", .{doc_id});
                errdefer allocator.free(key);
                const value = try std.fmt.allocPrint(
                    allocator,
                    "{{\"embedding\":{f}}}",
                    .{std.json.fmt(vector_buf, .{})},
                );
                write.* = .{
                    .key = key,
                    .value = value,
                };
            }
        }

        fn free(allocator: Allocator, writes: []types.BatchWrite) void {
            for (writes) |*write| {
                allocator.free(write.key);
                allocator.free(write.value);
                write.* = .{ .key = &.{}, .value = &.{} };
            }
        }
    };

    for (modes) |mode| {
        var path_buf: [256]u8 = undefined;
        const path = TestHelpers.tempPath(&path_buf);
        defer TestHelpers.cleanupTempDir(path);

        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        const config_json = try std.fmt.allocPrint(
            alloc,
            "{{\"field\":\"embedding\",\"dims\":{d},\"metric\":\"cosine\",\"use_quantization\":true,\"lazy_posting_maintenance\":{s},\"auto_posting_maintenance_max_postings\":0}}",
            .{ dims, if (mode.lazy) "true" else "false" },
        );
        defer alloc.free(config_json);

        try db.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = config_json,
        });

        const writes = try alloc.alloc(types.BatchWrite, doc_count);
        @memset(writes, .{ .key = &.{}, .value = &.{} });
        defer {
            BenchWrites.free(alloc, writes);
            alloc.free(writes);
        }

        const vector_buf = try alloc.alloc(f32, dims);
        defer alloc.free(vector_buf);

        try BenchWrites.fill(alloc, writes, vector_buf, 0);
        const seed_start = monotonicTimeNs();
        try db.batch(.{
            .writes = writes,
            .sync_level = .full_index,
        });
        const seed_ns = monotonicTimeNs() - seed_start;

        const seed_stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, seed_stats);

        BenchWrites.free(alloc, writes);
        try BenchWrites.fill(alloc, writes, vector_buf, 17);

        var batch_profile = BatchProfile{};
        const write_start = monotonicTimeNs();
        try db.batchProfiled(.{
            .writes = writes,
            .sync_level = .full_index,
        }, &batch_profile);
        const write_ns = monotonicTimeNs() - write_start;

        const before_idle_stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, before_idle_stats);

        const idle_start = monotonicTimeNs();
        const idle_steps = try db.runDensePostingMaintenanceForIdle();
        const idle_ns = monotonicTimeNs() - idle_start;

        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);

        std.debug.print(
            "hbc_posting_lazy_vs_eager mode={s} docs={d} seed_ms={d} update_ms={d} idle_ms={d} idle_steps={d} dirty_before_idle={d} dirty_after_idle={d} update_lazy_centroid_deferrals={d} update_repaired_postings={d} total_lazy_centroid_deferrals={d} total_repaired_postings={d} split_postings={d} merged_postings={d} boundary_reassigned={d}\n",
            .{
                mode.name,
                doc_count,
                @divTrunc(seed_ns, std.time.ns_per_ms),
                @divTrunc(write_ns, std.time.ns_per_ms),
                @divTrunc(idle_ns, std.time.ns_per_ms),
                idle_steps,
                before_idle_stats.indexes[0].hbc_posting.dirty_postings,
                stats.indexes[0].hbc_posting.dirty_postings,
                profileDelta(stats.indexes[0].hbc_posting.lazy_centroid_deferrals, seed_stats.indexes[0].hbc_posting.lazy_centroid_deferrals),
                profileDelta(stats.indexes[0].hbc_posting.maintenance_repaired_postings, seed_stats.indexes[0].hbc_posting.maintenance_repaired_postings),
                stats.indexes[0].hbc_posting.lazy_centroid_deferrals,
                stats.indexes[0].hbc_posting.maintenance_repaired_postings,
                stats.indexes[0].hbc_posting.maintenance_split_postings,
                stats.indexes[0].hbc_posting.maintenance_merged_postings,
                stats.indexes[0].hbc_posting.maintenance_boundary_reassigned_vectors,
            },
        );
    }
}
