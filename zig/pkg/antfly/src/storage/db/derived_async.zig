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
const apply_rw_lock_mod = @import("apply_rw_lock.zig");
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
const artifact_repair = @import("artifact_repair.zig");
const backfill_state_mod = @import("backfill_state.zig");
const backend_types = @import("../backend_types.zig");
const change_journal_mod = @import("derived/change_journal.zig");
const db_internal = @import("internal.zig");
const derived_types = @import("derived/derived_types.zig");
const derived_executor_mod = @import("derived/derived_executor.zig");
const derived_worker = @import("derived/derived_worker.zig");
const index_repair_state = @import("derived/index_repair_state.zig");
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
const lsm_backend_mod = @import("../lsm_backend/mod.zig");
const mapper = @import("document_mapper.zig");
const mem_backend_mod = @import("../mem_backend.zig");
const text_merge_runtime_mod = @import("maintenance/text_merge_runtime.zig");
const promotion_runtime_mod = @import("promotion_runtime.zig");
const range_cardinality = @import("range_cardinality.zig");
const replay_source_mod = @import("derived/replay_source.zig");
const replay_stream_mod = @import("derived/replay_stream.zig");
const resource_manager_mod = @import("../resource_manager.zig");
const resolution_runtime_mod = @import("resolution_runtime.zig");
const schema_mod = @import("../schema.zig");
const types = @import("types.zig");

const Allocator = std.mem.Allocator;
const AtomicU64 = platform.atomic.Value(u64);
const process_memory_mod = platform.process_memory;

fn replayCollectorTimeNs() u64 {
    return platform.time.monotonicNs();
}

const profileDelta = db_internal.profileDelta;
const readEnvUsize = db_internal.readEnvUsize;
const readEnvU64 = db_internal.readEnvU64;

const TestHelpers = if (builtin.is_test) @import("test_support.zig") else struct {};

test "db derived async external dense bulk waiter owns admission across catch-up handoff" {
    const DB = @import("mod.zig").DB;
    const DerivedAsync = Impl(DB);
    const AsyncContext = db_internal.AsyncContext(DB);
    var apply_mutex: @import("apply_rw_lock.zig").ApplyRwLock = .{};
    var ctx = AsyncContext{
        .alloc = std.testing.allocator,
        .store = undefined,
        .index_manager = undefined,
        .apply_mutex = &apply_mutex,
    };
    defer ctx.deinit(std.testing.allocator);

    try db_internal.beginDenseCatchUpSessionTracked(&ctx, "vec");

    const Waiter = struct {
        ctx: *AsyncContext,
        result: ?anyerror = null,

        fn run(waiter: *@This()) void {
            DerivedAsync.beginExternalDenseBulkSessionTrackedWait(waiter.ctx, null) catch |err| {
                waiter.result = err;
            };
        }
    };
    var waiter = Waiter{ .ctx = &ctx };
    const waiter_thread = try std.Thread.spawn(.{}, Waiter.run, .{&waiter});
    defer waiter_thread.join();

    const wait_deadline = platform.time.monotonicNs() + 5 * std.time.ns_per_s;
    while (ctx.waiting_external_dense_bulk_sessions.load(.acquire) == 0) {
        if (platform.time.monotonicNs() >= wait_deadline) return error.TestUnexpectedResult;
        platform.time.sleepMs(1);
    }

    try std.testing.expect(db_internal.asyncContextHasActiveDenseBulkWork(&ctx));
    try std.testing.expectError(error.ReplayDocumentNotVisible, db_internal.beginDenseCatchUpSessionTracked(&ctx, "late"));
    db_internal.finishDenseCatchUpSessionTracked(&ctx, "vec");

    const admission_deadline = platform.time.monotonicNs() + 5 * std.time.ns_per_s;
    while (ctx.active_external_dense_bulk_sessions.load(.acquire) == 0) {
        if (platform.time.monotonicNs() >= admission_deadline) return error.TestUnexpectedResult;
        platform.time.sleepMs(1);
    }
    try std.testing.expect(waiter.result == null);
    try std.testing.expectEqual(@as(u32, 0), ctx.waiting_external_dense_bulk_sessions.load(.acquire));
    try std.testing.expect(ctx.text_merge_deferred.load(.acquire));

    db_internal.finishExternalDenseBulkSessionTracked(&ctx);
    try std.testing.expect(!db_internal.asyncContextHasActiveDenseBulkWork(&ctx));
    try std.testing.expect(!ctx.text_merge_deferred.load(.acquire));
}

fn expectedDocumentEmbeddingArtifactKeyAlloc(alloc: Allocator, doc_key: []const u8, embedding_name: []const u8) ![]u8 {
    return try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, doc_key, embedding_name);
}

pub fn freeOwnedKeySlice(alloc: Allocator, keys: [][]u8) void {
    for (keys) |key| alloc.free(key);
    alloc.free(keys);
}

pub const DerivedCoverageOutcome = enum { produced, skipped, terminal_failed };

const DerivedCoverageDocOutcome = struct {
    doc_key: []const u8,
    outcome: DerivedCoverageOutcome,
};

fn loadDerivedCoverageOutcomeCounterFromStore(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    index_name: []const u8,
    generation: u64,
    outcome: []const u8,
) !?u64 {
    const counter_key = try internal_keys.derivedCoverageOutcomeCountKeyAlloc(alloc, index_name, generation, outcome);
    defer alloc.free(counter_key);
    const raw = store.get(alloc, counter_key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    defer alloc.free(raw);
    return try internal_keys.decodeDerivedCoverageOutcomeCount(raw);
}

fn scanDerivedCoverageOutcomeFromStore(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    index_name: []const u8,
    generation: u64,
    outcome: []const u8,
) !u64 {
    const lower = try internal_keys.derivedCoverageOutcomeMarkerPrefixAlloc(alloc, index_name, generation);
    defer alloc.free(lower);
    const upper = try internal_keys.nextPrefixAlloc(alloc, lower);
    defer if (upper) |key| alloc.free(key);
    const upper_bound = if (upper) |key| key else "";

    var count: u64 = 0;
    const CountState = struct {
        count: *u64,
        outcome_name: []const u8,

        fn scanEntry(ctx: ?*anyopaque, key: []const u8, value: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
            _ = key;
            const state: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            if (std.mem.eql(u8, value, state.outcome_name)) state.count.* += 1;
            return .@"continue";
        }
    };

    var state = CountState{ .count = &count, .outcome_name = outcome };
    try store.scanWithContext(lower, upper_bound, .{}, &state, CountState.scanEntry);
    return count;
}

fn derivedCoverageOutcomeCounterValueForStore(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    index_name: []const u8,
    generation: u64,
    outcome: []const u8,
) !u64 {
    return (try loadDerivedCoverageOutcomeCounterFromStore(alloc, store, index_name, generation, outcome)) orelse
        try scanDerivedCoverageOutcomeFromStore(alloc, store, index_name, generation, outcome);
}

fn setDerivedCoverageOutcomes(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    index_manager: *index_manager_mod.IndexManager,
    index_name: []const u8,
    outcomes: []const DerivedCoverageDocOutcome,
) !void {
    if (outcomes.len == 0) return;
    const generation = index_manager.coverageGenerationForIndex(index_name) orelse return;
    const tags = std.meta.tags(DerivedCoverageOutcome);

    var counter_counts: [tags.len]u64 = undefined;
    var counter_keys: [tags.len][]u8 = undefined;
    var initialized_counters: usize = 0;
    defer for (counter_keys[0..initialized_counters]) |key| alloc.free(key);
    inline for (tags, 0..) |outcome, i| {
        counter_counts[i] = try derivedCoverageOutcomeCounterValueForStore(alloc, store, index_name, generation, @tagName(outcome));
        counter_keys[i] = try internal_keys.derivedCoverageOutcomeCountKeyAlloc(alloc, index_name, generation, @tagName(outcome));
        initialized_counters += 1;
    }

    var owned_marker_keys = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_marker_keys.items) |key| alloc.free(key);
        owned_marker_keys.deinit(alloc);
    }
    var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer writes.deinit(alloc);
    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(alloc);
    var changed = false;

    for (outcomes) |transition| {
        if (seen.contains(transition.doc_key)) continue;
        try seen.put(alloc, transition.doc_key, {});
        const target_index = @intFromEnum(transition.outcome);
        const marker_key = try internal_keys.derivedCoverageOutcomeKeyAlloc(alloc, index_name, generation, transition.doc_key);
        owned_marker_keys.append(alloc, marker_key) catch |err| {
            alloc.free(marker_key);
            return err;
        };
        const existing = store.get(alloc, marker_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        const existing_outcome: ?DerivedCoverageOutcome = if (existing) |value| blk: {
            defer alloc.free(value);
            break :blk std.meta.stringToEnum(DerivedCoverageOutcome, value) orelse return error.InvalidDerivedCoverageOutcome;
        } else null;
        if (existing_outcome == null or existing_outcome.? != transition.outcome) {
            if (existing_outcome) |previous| {
                const previous_index = @intFromEnum(previous);
                if (counter_counts[previous_index] == 0) return error.InvalidDerivedCoverageCounter;
                counter_counts[previous_index] -= 1;
            }
            counter_counts[target_index] +|= 1;
            try writes.append(alloc, .{ .key = marker_key, .value = @tagName(transition.outcome) });
            changed = true;
        }
    }
    if (!changed) return;

    var counter_values: [tags.len][8]u8 = undefined;
    inline for (tags, 0..) |_, i| {
        try writes.append(alloc, .{
            .key = counter_keys[i],
            .value = internal_keys.encodeDerivedCoverageOutcomeCount(&counter_values[i], counter_counts[i]),
        });
    }
    try store.putBatch(writes.items, &.{});
}

fn deleteDerivedCoverageForDocKeys(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    index_manager: *index_manager_mod.IndexManager,
    index_name: []const u8,
    doc_keys: []const []const u8,
) !void {
    if (doc_keys.len == 0) return;
    const generation = index_manager.coverageGenerationForIndex(index_name) orelse return;

    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (deletes.items) |key| alloc.free(@constCast(key));
        deletes.deinit(alloc);
    }

    var unique_deletes = std.StringHashMapUnmanaged(void).empty;
    defer unique_deletes.deinit(alloc);

    const outcomes = std.meta.tags(DerivedCoverageOutcome);
    var removed_counts = [_]u64{0} ** outcomes.len;
    for (doc_keys) |doc_key| {
        const marker_key = try internal_keys.derivedCoverageOutcomeKeyAlloc(alloc, index_name, generation, doc_key);
        errdefer alloc.free(marker_key);
        if (unique_deletes.contains(marker_key)) {
            alloc.free(marker_key);
            continue;
        }
        const existing = store.get(alloc, marker_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (existing) |value| {
            defer alloc.free(value);
            const outcome = std.meta.stringToEnum(DerivedCoverageOutcome, value) orelse return error.InvalidDerivedCoverageOutcome;
            removed_counts[@intFromEnum(outcome)] +|= 1;
        }
        try deletes.append(alloc, marker_key);
        errdefer _ = deletes.pop();
        try unique_deletes.put(alloc, marker_key, {});
    }

    if (deletes.items.len == 0) return;
    var total_removed: u64 = 0;
    for (removed_counts) |count| total_removed +|= count;
    if (total_removed == 0) {
        try store.putBatch(&.{}, deletes.items);
        return;
    }

    var counter_keys: [outcomes.len]?[]u8 = .{null} ** outcomes.len;
    defer for (counter_keys) |key| if (key) |value| alloc.free(value);
    var counter_values: [outcomes.len][8]u8 = undefined;
    var counter_writes: [outcomes.len]docstore_mod.KVPair = undefined;
    var counter_write_count: usize = 0;
    for (outcomes, removed_counts, 0..) |outcome, removed_count, outcome_index| {
        if (removed_count == 0) continue;
        const current_count = try derivedCoverageOutcomeCounterValueForStore(alloc, store, index_name, generation, @tagName(outcome));
        if (current_count < removed_count) return error.InvalidDerivedCoverageCounter;
        counter_keys[outcome_index] = try internal_keys.derivedCoverageOutcomeCountKeyAlloc(alloc, index_name, generation, @tagName(outcome));
        counter_writes[counter_write_count] = .{
            .key = counter_keys[outcome_index].?,
            .value = internal_keys.encodeDerivedCoverageOutcomeCount(&counter_values[outcome_index], current_count - removed_count),
        };
        counter_write_count += 1;
    }
    try store.putBatch(counter_writes[0..counter_write_count], deletes.items);
}

test "db derived async deletes coverage outcome markers for replay deletes once" {
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
    const generation = db.core.index_manager.coverageGenerationForIndex("dv_v1") orelse return error.TestUnexpectedResult;

    const delete_key = try internal_keys.derivedCoverageOutcomeKeyAlloc(alloc, "dv_v1", generation, "doc:a");
    defer alloc.free(delete_key);
    const keep_key = try internal_keys.derivedCoverageOutcomeKeyAlloc(alloc, "dv_v1", generation, "doc:b");
    defer alloc.free(keep_key);
    const counter_key = try internal_keys.derivedCoverageOutcomeCountKeyAlloc(alloc, "dv_v1", generation, "skipped");
    defer alloc.free(counter_key);

    var counter_value: [8]u8 = undefined;
    try db.core.store.putBatch(&.{
        .{ .key = delete_key, .value = "skipped" },
        .{ .key = keep_key, .value = "skipped" },
        .{ .key = counter_key, .value = internal_keys.encodeDerivedCoverageOutcomeCount(&counter_value, 2) },
    }, &.{});

    try deleteDerivedCoverageForDocKeys(alloc, db.core.store, db.core.index_manager, "dv_v1", &.{ "doc:a", "doc:a" });

    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, delete_key));
    const kept = try db.core.store.get(alloc, keep_key);
    defer alloc.free(kept);
    try std.testing.expectEqualStrings("skipped", kept);

    const raw_counter = try db.core.store.get(alloc, counter_key);
    defer alloc.free(raw_counter);
    try std.testing.expectEqual(@as(u64, 1), try internal_keys.decodeDerivedCoverageOutcomeCount(raw_counter));
}

fn putDenseEmbeddingArtifactWithCounterForTest(db: anytype, alloc: Allocator, artifact_key: []const u8, source_hash: ?u64, vector: []const f32) !void {
    const DB = @TypeOf(db.*);
    const payload = try enrichment_artifact_codec.encodeDenseEmbeddingAlloc(alloc, source_hash, vector);
    defer alloc.free(payload);

    var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer writes.deinit(alloc);
    try writes.append(alloc, .{ .key = artifact_key, .value = payload });

    var owned_keys = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_keys.items) |key| alloc.free(key);
        owned_keys.deinit(alloc);
    }
    var owned_values = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_values.items) |value| alloc.free(value);
        owned_values.deinit(alloc);
    }

    try DB.appendDenseArtifactCounterMutations(
        alloc,
        db.core.store,
        db.core.index_manager,
        &writes,
        &.{},
        &owned_keys,
        &owned_values,
    );
    try db.core.store.putBatch(writes.items, &.{});
    try db.core.putArtifactPresenceMarker();
}

fn concatKeyViews(alloc: Allocator, left: []const []const u8, right: []const []const u8) ![][]const u8 {
    const out = try alloc.alloc([]const u8, left.len + right.len);
    @memcpy(out[0..left.len], left);
    @memcpy(out[left.len..], right);
    return out;
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

fn replayDocumentKeyInRange(byte_range: types.ByteRange, key: []const u8) bool {
    return internal_keys.isInternalUserKey(key) or byte_range.contains(key);
}

test "db derived async replay internal user keys bypass document byte ranges" {
    const range: types.ByteRange = .{ .start = "doc:m", .end = "" };
    try std.testing.expect(replayDocumentKeyInRange(range, "\x01derived:child"));
    try std.testing.expect(!replayDocumentKeyInRange(range, "doc:a"));
    try std.testing.expect(replayDocumentKeyInRange(range, "doc:z"));
}

fn replayDocumentIsDurablyDeleted(alloc: Allocator, txn: anytype, doc_key: []const u8) !bool {
    const ordinal = (try doc_identity.lookupOrdinalTxn(alloc, txn, doc_key)) orelse return false;
    const state = (try doc_identity.lookupStateTxn(txn, ordinal)) orelse return false;
    return !state.isLive();
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
        if (!replayDocumentKeyInRange(byte_range, doc.key)) continue;
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
            if (try replayDocumentIsDurablyDeleted(alloc, &txn, item.doc_key)) continue;
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
        if (!replayDocumentKeyInRange(byte_range, doc.key)) continue;
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
            if (try replayDocumentIsDurablyDeleted(alloc, &txn, item.doc_key)) continue;
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

const CollectedTextDocumentWrites = struct {
    alloc: Allocator,
    writes: std.ArrayListUnmanaged(types.BatchWrite) = .empty,
    owned_values: std.ArrayListUnmanaged([]u8) = .empty,
    missing_required: usize = 0,

    fn deinit(self: *@This()) void {
        for (self.owned_values.items) |value| self.alloc.free(value);
        self.owned_values.deinit(self.alloc);
        self.writes.deinit(self.alloc);
        self.* = undefined;
    }
};

fn collectTextDocumentWritesForIndex(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    documents: []const derived_types.DerivedDocument,
    index_name: []const u8,
    chunk_backed: bool,
    byte_range: types.ByteRange,
    opts: CollectTextDocumentWritesOptions,
) !CollectedTextDocumentWrites {
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

    var result = CollectedTextDocumentWrites{ .alloc = alloc };
    errdefer result.deinit();

    const trust_inline = if (opts.prefer_inline_when_store_tip_matches_sequence) |sequence|
        store.nextReplaySequence(sequence + 1) == sequence + 1
    else
        false;

    for (documents) |doc| {
        if (doc.action != .upsert) continue;
        if (!replayDocumentKeyInRange(byte_range, doc.key)) continue;
        if (!documentTargetsTextIndex(doc, index_name, chunk_backed)) continue;
        if (trust_inline and doc.cleaned_value != null) {
            try result.writes.append(alloc, .{
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

    if (pending.items.len == 0) return result;

    var txn = try store.beginProbeTxn();
    defer txn.abort();
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
            if (try replayDocumentIsDurablyDeleted(alloc, &txn, item.doc_key)) continue;
            result.missing_required += 1;
            continue;
        };
        const stable_value = if (read_values[i] != null) blk: {
            const owned = if (opts.relational_base_rows)
                try mapper.materializeRelationalRowValueAlloc(alloc, raw)
            else
                try alloc.dupe(u8, raw);
            result.owned_values.append(alloc, owned) catch |err| {
                alloc.free(owned);
                return err;
            };
            break :blk owned;
        } else raw;
        result.writes.append(alloc, .{
            .key = item.doc_key,
            .value = stable_value,
        }) catch |err| {
            if (read_values[i] != null) {
                const owned = result.owned_values.pop().?;
                alloc.free(owned);
            }
            return err;
        };
    }

    return result;
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

    const writes = try filtered.toOwnedSlice(alloc);
    return .{
        .alloc = alloc,
        .owns_doc_keys = true,
        .writes = writes,
        .allocation_len = writes.len,
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

    const writes = try filtered.toOwnedSlice(alloc);
    errdefer if (writes.len > 0) alloc.free(writes);
    const owned_keys = try owned_doc_keys.toOwnedSlice(alloc);
    return .{
        .alloc = alloc,
        .owned_doc_keys = owned_keys,
        .writes = writes,
        .allocation_len = writes.len,
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

test "target advance repair cooldown skips repeated repair attempts" {
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
    try std.testing.expect(TestImpl.shouldRunTargetAdvanceRepair(&ctx, "idx", now_ns));
    try TestImpl.noteTargetAdvanceRepairRun(&ctx, "idx", now_ns);
    try std.testing.expect(!TestImpl.shouldRunTargetAdvanceRepair(&ctx, "idx", now_ns + denseCatchUpMaintenanceCooldownNs() - 1));
    try std.testing.expect(TestImpl.shouldRunTargetAdvanceRepair(&ctx, "idx", now_ns + denseCatchUpMaintenanceCooldownNs()));
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

test "db derived async graph replay ignores document extraction parent asset but tracks units" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
    });
    try db.addIndex(.{
        .name = "unit_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"document_units_v1","path":"$.relations[*]","format":"extraction_relation"}
        \\}
        ,
    });

    const graph_ref = index_manager_mod.ManagedIndexRef{ .name = "unit_graph", .kind = .graph };

    const parent_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "document_units_v1");
    defer alloc.free(parent_key);
    const parent_batch = derived_types.DerivedBatch{
        .changed_artifact_keys = &.{parent_key},
    };

    try std.testing.expect(!db.derivedAsyncBatchAffectsManagedIndex(parent_batch, graph_ref));
    try std.testing.expect(!try db.derivedAsyncBatchAffectsManagedIndexForReplay(parent_batch, graph_ref));

    const unit_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:a", "document_units_v1", "document:000001");
    defer alloc.free(unit_key);
    const unit_batch = derived_types.DerivedBatch{
        .changed_artifact_keys = &.{unit_key},
    };

    try std.testing.expect(db.derivedAsyncBatchAffectsManagedIndex(unit_batch, graph_ref));
    try std.testing.expect(try db.derivedAsyncBatchAffectsManagedIndexForReplay(unit_batch, graph_ref));
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

test "db derived async filters relational algebraic sync targets by generation record" {
    const alloc = std.testing.allocator;
    const Case = struct {
        name: []const u8,
        schema: ?schema_mod.TableSchema,
        expected_all: []const []const u8,
    };

    const ready_index = relationalAlgebraicIndexForTest(.ready, .{ .generation = 7, .lifecycle = .ready });
    const building_index = relationalAlgebraicIndexForTest(.building, .{ .generation = 7, .lifecycle = .building });
    const catching_up_index = relationalAlgebraicIndexForTest(.catching_up, .{ .generation = 7, .lifecycle = .catching_up, .lag = 9 });
    const invalid_index = relationalAlgebraicIndexForTest(.invalid, .{ .generation = 7, .lifecycle = .invalid });
    const missing_record_index = schema_mod.RelationalIndex{
        .name = "alg_rel",
        .owner_kind = .table,
        .owner_name = schema_mod.relational_table_index_owner_name,
        .access_method = .algebraic_filter,
        .lifecycle = .ready,
        .generation = 7,
    };
    const mismatched_generation_index = relationalAlgebraicIndexForTest(.ready, .{ .generation = 6, .lifecycle = .ready });
    const unrelated_indexes = [_]schema_mod.RelationalIndex{
        relationalTextSearchIndexNamedForTest("fts_doc", .ready, .{ .generation = 7, .lifecycle = .ready }),
    };

    const keep_all = [_][]const u8{ "alg_rel", "fts_doc", "dense_v1" };
    const filtered_all = [_][]const u8{ "fts_doc", "dense_v1" };
    const cases = [_]Case{
        .{
            .name = "no relational schema keeps document algebraic target",
            .schema = null,
            .expected_all = &keep_all,
        },
        .{
            .name = "no matching relational algebraic index keeps target",
            .schema = .{ .relational_indexes = &unrelated_indexes },
            .expected_all = &keep_all,
        },
        .{
            .name = "ready generation keeps algebraic target",
            .schema = .{ .relational_indexes = &[_]schema_mod.RelationalIndex{ready_index} },
            .expected_all = &keep_all,
        },
        .{
            .name = "building generation keeps algebraic target for write maintenance",
            .schema = .{ .relational_indexes = &[_]schema_mod.RelationalIndex{building_index} },
            .expected_all = &keep_all,
        },
        .{
            .name = "catching-up generation keeps algebraic target for write maintenance",
            .schema = .{ .relational_indexes = &[_]schema_mod.RelationalIndex{catching_up_index} },
            .expected_all = &keep_all,
        },
        .{
            .name = "invalid generation filters algebraic target",
            .schema = .{ .relational_indexes = &[_]schema_mod.RelationalIndex{invalid_index} },
            .expected_all = &filtered_all,
        },
        .{
            .name = "missing generation record filters algebraic target",
            .schema = .{ .relational_indexes = &[_]schema_mod.RelationalIndex{missing_record_index} },
            .expected_all = &filtered_all,
        },
        .{
            .name = "mismatched generation record filters algebraic target",
            .schema = .{ .relational_indexes = &[_]schema_mod.RelationalIndex{mismatched_generation_index} },
            .expected_all = &filtered_all,
        },
    };

    for (cases) |case| {
        var targets = try managedAlgebraicSyncTargetsForFilterTest(alloc);
        defer targets.deinit(alloc);

        try filterManagedSyncTargetsForRelationalDerivedMaintenance(alloc, case.schema, &targets);

        expectManagedSyncTargetNames(case.name, &[_][]const u8{"fts_doc"}, targets.full_text_indexes) catch |err| {
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

fn relationalAlgebraicIndexForTest(
    lifecycle: schema_mod.RelationalIndexLifecycle,
    record: schema_mod.RelationalIndexGenerationRecord,
) schema_mod.RelationalIndex {
    return .{
        .name = "alg_rel",
        .owner_kind = .table,
        .owner_name = schema_mod.relational_table_index_owner_name,
        .access_method = .algebraic_filter,
        .lifecycle = lifecycle,
        .generation = 7,
        .generation_record = record,
    };
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

fn managedAlgebraicSyncTargetsForFilterTest(alloc: Allocator) !db_internal.ManagedSyncTargets {
    var full_text_indexes = try alloc.alloc([]const u8, 1);
    errdefer alloc.free(full_text_indexes);
    full_text_indexes[0] = try alloc.dupe(u8, "fts_doc");
    errdefer alloc.free(@constCast(full_text_indexes[0]));

    var all_indexes = try alloc.alloc([]const u8, 3);
    var initialized_all: usize = 0;
    errdefer {
        for (all_indexes[0..initialized_all]) |name| alloc.free(@constCast(name));
        alloc.free(all_indexes);
    }
    all_indexes[0] = try alloc.dupe(u8, "alg_rel");
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
    return try filterManagedSyncTargetsForRelationalDerivedMaintenance(alloc, schema, targets);
}

pub fn filterManagedSyncTargetsForRelationalDerivedMaintenance(
    alloc: Allocator,
    schema: ?schema_mod.TableSchema,
    targets: *db_internal.ManagedSyncTargets,
) !void {
    targets.full_text_indexes = try filterRelationalDerivedMaintenanceTargets(alloc, schema, targets.full_text_indexes, &.{.text_search});
    targets.all_indexes = try filterRelationalDerivedMaintenanceTargets(alloc, schema, targets.all_indexes, &.{ .text_search, .algebraic_filter });
}

fn filterRelationalDerivedMaintenanceTargets(
    alloc: Allocator,
    schema: ?schema_mod.TableSchema,
    source: []const []const u8,
    access_methods: []const schema_mod.RelationalIndexAccessMethod,
) ![]const []const u8 {
    var kept_len: usize = 0;
    for (source) |name| {
        if (relationalDerivedWriteMaintenanceAllowed(schema, name, access_methods)) kept_len += 1;
    }
    var kept: [][]const u8 = if (kept_len == 0) &.{} else try alloc.alloc([]const u8, kept_len);
    var kept_index: usize = 0;
    for (source) |name| {
        if (relationalDerivedWriteMaintenanceAllowed(schema, name, access_methods)) {
            kept[kept_index] = name;
            kept_index += 1;
        } else {
            alloc.free(@constCast(name));
        }
    }
    if (source.len > 0) alloc.free(source);
    return kept;
}

fn relationalDerivedWriteMaintenanceAllowed(
    schema: ?schema_mod.TableSchema,
    index_name: []const u8,
    access_methods: []const schema_mod.RelationalIndexAccessMethod,
) bool {
    for (access_methods) |access_method| {
        if (!schema_mod.relationalAccessMethodWriteMaintenanceAllowed(schema, access_method, index_name)) return false;
    }
    return true;
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

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        if (self.resume_from) |buf| alloc.free(buf);
        self.* = .{
            .dense_index_idx = 0,
        };
    }
};

test "db derived async dense checkpoint persistence serializes with index apply" {
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
    try db.addIndex(.{
        .name = "dense_checkpoint_lock",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3}",
    });

    var apply_guard = try db.core.index_manager.lockManagedIndexApply(.{
        .name = "dense_checkpoint_lock",
        .kind = .dense_vector,
    });
    var apply_locked = true;
    defer if (apply_locked) apply_guard.unlock();

    const Persist = struct {
        db: *DB,
        started: std.atomic.Value(bool) = .init(false),
        done: std.atomic.Value(bool) = .init(false),
        err: ?anyerror = null,

        fn run(state: *@This()) void {
            state.started.store(true, .release);
            state.db.core.index_manager.saveDenseProjectionCheckpointMetadata(
                "dense_checkpoint_lock",
                .{ .applied_sequence = 1, .status = .clean },
            ) catch |err| {
                state.err = err;
            };
            state.done.store(true, .release);
        }
    };
    var persist = Persist{ .db = &db };
    const thread = try std.Thread.spawn(.{}, Persist.run, .{&persist});
    while (!persist.started.load(.acquire)) std.atomic.spinLoopHint();
    platform.time.sleepMs(25);
    const completed_while_apply_active = persist.done.load(.acquire);

    apply_guard.unlock();
    apply_locked = false;
    thread.join();

    try std.testing.expect(!completed_while_apply_active);
    if (persist.err) |err| return err;
    try std.testing.expect(persist.done.load(.acquire));
}

test "db derived async recreated managed dense index converges replay and irrelevant resolver stages" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;
    const Derived = Impl(DB);

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var counting = TestHelpers.CountingDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = counting.interface(),
        },
    });
    defer db.close();

    const config: types.IndexConfig = .{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2,\"embedding_name\":\"chunk_dense_v1\"}}",
    };
    try db.addIndex(config);
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"abcdefghijklmno\"}" }},
        .sync_level = .write,
    });
    try db.runUntilIdle();

    const calls_before_recreate = counting.calls;
    try std.testing.expect(calls_before_recreate > 0);
    try std.testing.expectEqual(@as(u64, 3), db.core.index_manager.denseIndex("dv_v1").?.index.metadata.active_count);

    try Derived.noteTargetAdvanceRepairRun(db.async_context, "dv_v1", platform.time.monotonicNs());
    try std.testing.expect(try db.deleteIndex("dv_v1"));
    db.backend_runtime.durable_jobs.drainOwner(db.repair_cleanup_owner_id);
    try db.addIndex(config);
    try db.runUntilIdle();

    try std.testing.expect(counting.calls >= calls_before_recreate);
    try std.testing.expectEqual(@as(u64, 3), db.core.index_manager.denseIndex("dv_v1").?.index.metadata.active_count);
    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    const replay = for (stats.indexes) |item| {
        if (std.mem.eql(u8, item.name, "dv_v1")) break item;
    } else return error.TestUnexpectedResult;
    try std.testing.expect(replay.replay_applied_sequence >= replay.replay_target_sequence);
    try std.testing.expect(!stats.resolution.enabled);
    try std.testing.expect(!stats.resolution.catch_up_required);
    try std.testing.expect(!stats.promotion.enabled);
    try std.testing.expect(!stats.promotion.catch_up_required);
}

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

        const DenseGenerationRepairTarget = struct {
            dense_index_idx: usize,
            trigger: index_repair_state.Trigger,
            last_error: []const u8,
        };

        const DenseArtifactRebuildPlan = struct {
            targets: []DenseArtifactRebuildTarget = &.{},
            generation_repairs: []DenseGenerationRepairTarget = &.{},
            target_sequence: u64 = 0,

            fn deinit(self: *@This(), alloc: Allocator) void {
                for (self.targets) |*target| target.deinit(alloc);
                if (self.targets.len > 0) alloc.free(self.targets);
                if (self.generation_repairs.len > 0) alloc.free(self.generation_repairs);
                self.* = .{};
            }
        };

        const DenseArtifactTargetCounts = struct {
            per_target_index: std.AutoHashMapUnmanaged(usize, u64) = .empty,
            authoritative_targets: std.AutoHashMapUnmanaged(usize, void) = .empty,
            total_target_artifacts: u64 = 0,

            fn deinit(self: *@This(), alloc: Allocator) void {
                self.per_target_index.deinit(alloc);
                self.authoritative_targets.deinit(alloc);
                self.* = .{};
            }
        };

        const dense_artifact_target_counter_prefix = "\x00\x00__metadata__:dense_artifact_target_count:";
        const dense_artifact_counter_bootstrap_prefix = "\x00\x00__metadata__:dense_artifact_counter_bootstrap:";
        const dense_artifact_counter_bootstrap_magic = "AFDCB001";
        pub const dense_artifact_counter_bootstrap_encoded_len = dense_artifact_counter_bootstrap_magic.len + 2 * @sizeOf(u128) + @sizeOf(i64);

        pub const DenseArtifactCounterBootstrap = struct {
            repair_id: u128,
            attempt_id: u128,
            delta: i64 = 0,
        };

        fn storeHasReplayRecordForHintAfter(
            store: *docstore_mod.DocStore,
            hint: change_journal_mod.TargetHint,
            from_sequence: u64,
        ) !bool {
            const Context = struct {
                found: bool = false,

                fn handle(self: *@This(), sequence: u64, payload: []const u8) !void {
                    _ = sequence;
                    _ = payload;
                    self.found = true;
                }
            };

            var scan_ctx = Context{};
            const stats = store.forEachReplayLaneFrom(
                @intCast(@intFromEnum(hint)),
                from_sequence + 1,
                1,
                &scan_ctx,
                Context.handle,
            ) catch |err| switch (err) {
                error.ReplayIndexUnavailable => return false,
                else => return err,
            };
            return scan_ctx.found or stats.matched_entries != 0;
        }

        fn managedIndexReplayHint(kind: types.IndexKind) change_journal_mod.TargetHint {
            return switch (kind) {
                .full_text => .full_text,
                .dense_vector => .dense_vector,
                .sparse_vector => .sparse_vector,
                .graph => .graph,
                .algebraic => .algebraic,
            };
        }

        fn replayRangeHasManagedIndexApplicableRecord(
            ctx: *AsyncContext,
            index_ref: index_manager_mod.ManagedIndexRef,
            from_sequence: u64,
            target_sequence: u64,
        ) !bool {
            const Context = struct {
                alloc: Allocator,
                index_manager: *index_manager_mod.IndexManager,
                index_ref: index_manager_mod.ManagedIndexRef,
                target_sequence: u64,
                found: bool = false,

                fn handle(self: *@This(), sequence: u64, payload: []const u8) !void {
                    if (sequence > self.target_sequence) return replay_source_mod.StopReplayChunk.StopReplayChunk;
                    var decoded = try change_journal_mod.decodeRecord(self.alloc, payload);
                    defer decoded.deinit();
                    switch (managedIndexRecordApplicability(self.index_manager, decoded.record, self.index_ref)) {
                        .irrelevant => {},
                        .relevant, .missing_dependency => {
                            self.found = true;
                            return replay_source_mod.StopReplayChunk.StopReplayChunk;
                        },
                    }
                }
            };

            var scan_ctx = Context{
                .alloc = ctx.alloc,
                .index_manager = ctx.index_manager,
                .index_ref = index_ref,
                .target_sequence = target_sequence,
            };
            _ = ctx.store.forEachReplayLaneFrom(
                @intCast(@intFromEnum(managedIndexReplayHint(index_ref.kind))),
                from_sequence + 1,
                0,
                &scan_ctx,
                Context.handle,
            ) catch |err| switch (err) {
                error.ReplayIndexUnavailable => return try storeHasReplayRecordForHintAfter(ctx.store, managedIndexReplayHint(index_ref.kind), from_sequence),
                replay_source_mod.StopReplayChunk.StopReplayChunk => return scan_ctx.found,
                else => return err,
            };
            return scan_ctx.found;
        }

        pub fn canAdvanceDerivedToTargetAsync(ctx_ptr: *anyopaque, index_ref: index_manager_mod.ManagedIndexRef, from_sequence: u64, target_sequence: u64) !bool {
            const ctx = asyncContextFromOpaque(ctx_ptr);
            const persisted_applied = try apply_state.loadAppliedSequenceWithCheckpoint(
                ctx.alloc,
                ctx.index_manager.checkpointIo(),
                ctx.store,
                ctx.applied_sequence_checkpoint_path,
                index_ref.name,
            );
            if (persisted_applied >= target_sequence) return true;

            if (try replayRangeHasManagedIndexApplicableRecord(ctx, index_ref, from_sequence, target_sequence)) return false;
            if (!ctx.index_manager.indexLoadComplete(index_ref.name)) return false;
            if (index_ref.kind != .dense_vector) return true;

            ctx.apply_mutex.lockExclusive();
            defer ctx.apply_mutex.unlockExclusive();

            const entry = ctx.index_manager.denseIndex(index_ref.name) orelse return true;
            if (!denseIndexIsArtifactBacked(entry)) return true;
            if (ctx.active_external_dense_bulk_sessions.load(.acquire) != 0) return false;
            if (ctx.active_dense_catch_up_sessions.load(.acquire) != 0) return true;

            const expected_doc_count = (try denseTargetCountForIndexContext(ctx, index_ref.name)) orelse {
                std.log.warn(
                    "dense replay target advance deferred by missing durable artifact counter index={s}",
                    .{index_ref.name},
                );
                return false;
            };
            const active_count = entry.index.stats().active_count;
            if (denseCoverageMatchesTarget(active_count, expected_doc_count)) return true;

            const now_ns = monotonicTimeNs();
            if (!Self.shouldRunTargetAdvanceRepair(ctx, index_ref.name, now_ns)) return false;
            try Self.noteTargetAdvanceRepairRun(ctx, index_ref.name, now_ns);

            std.log.warn(
                "dense replay target advance blocked by coverage mismatch index={s} indexed={} expected_docs={}",
                .{ index_ref.name, active_count, expected_doc_count },
            );

            // Replay can fill a deficit, but it cannot remove a vector whose
            // source artifact no longer exists. The rebuild planner owns surplus
            // reconstruction and shadow-generation activation.
            if (active_count > expected_doc_count) return false;

            _ = rebuildDenseIndexForTargetCoverageContext(ctx, index_ref.name, 2048) catch |err| {
                std.log.warn(
                    "dense replay target advance repair deferred index={s} err={s}",
                    .{ index_ref.name, @errorName(err) },
                );
                return false;
            };
            const repaired_entry = ctx.index_manager.denseIndex(index_ref.name) orelse return true;
            return denseCoverageMatchesTarget(repaired_entry.index.stats().active_count, expected_doc_count);
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
            DB.DerivedAsyncCallbacks.mirror_ha_replay_payload_best_effort_context(ctx.log_mutex, ctx.identity_namespace, ctx.ha_async_effect_mirror, payload);
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

        pub fn appendGeneratedBatchFromEnrichment(ctx_ptr: *anyopaque, batch: derived_types.DerivedBatch, artifact_delete_keys: []const []const u8) !u64 {
            if (artifact_delete_keys.len == 0) return appendDerivedBatchFromEnrichment(ctx_ptr, batch);

            const ctx: *EnrichmentAppendContext = @ptrCast(@alignCast(ctx_ptr));
            var batch_ctx = ctx.batchContext();
            try DB.DerivedAsyncCallbacks.enforce_ha_write_gate_optional(batch_ctx.ha_write_gate);
            const replay_deleted_keys = try concatKeyViews(batch_ctx.alloc, batch.deleted_keys, artifact_delete_keys);
            defer batch_ctx.alloc.free(replay_deleted_keys);
            var replay_batch = batch;
            replay_batch.deleted_keys = replay_deleted_keys;

            batch_ctx.apply_mutex.lockExclusive();
            const sequence = blk: {
                defer batch_ctx.apply_mutex.unlockExclusive();
                const reserved_sequence = batch_ctx.store.reserveNextReplaySequence(1);
                replay_batch.sequence = reserved_sequence;
                const payload = try encodeChangeRecordPayload(&batch_ctx, replay_batch, reserved_sequence);
                defer batch_ctx.alloc.free(payload);
                var counter_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
                defer counter_writes.deinit(batch_ctx.alloc);
                var owned_counter_keys = std.ArrayListUnmanaged([]u8).empty;
                defer {
                    for (owned_counter_keys.items) |key| batch_ctx.alloc.free(key);
                    owned_counter_keys.deinit(batch_ctx.alloc);
                }
                var owned_counter_values = std.ArrayListUnmanaged([]u8).empty;
                defer {
                    for (owned_counter_values.items) |value| batch_ctx.alloc.free(value);
                    owned_counter_values.deinit(batch_ctx.alloc);
                }
                try appendDenseArtifactCounterMutations(
                    batch_ctx.alloc,
                    batch_ctx.store,
                    batch_ctx.index_manager,
                    &counter_writes,
                    artifact_delete_keys,
                    &owned_counter_keys,
                    &owned_counter_values,
                );
                try batch_ctx.store.putBatchWithReplay(batch_ctx.io, counter_writes.items, artifact_delete_keys, .{
                    .sequence = reserved_sequence,
                    .payload = payload,
                });
                const split_state = batch_ctx.shard_manager.getSplitState();
                if (split_state != null and split_state.?.phase == .splitting) {
                    batch_ctx.shard_manager.appendSplitDelta(DB.WritePathCallbacks.current_time_ns(), &.{}, artifact_delete_keys) catch |err| switch (err) {
                        error.SplitInProgress => {},
                        else => return err,
                    };
                }
                DB.DerivedAsyncCallbacks.mirror_ha_replay_payload_best_effort_context(batch_ctx.log_mutex, batch_ctx.identity_namespace, batch_ctx.ha_async_effect_mirror, payload);
                batch_ctx.executor.trackBacklogBytes(reserved_sequence, @intCast(payload.len)) catch {};
                break :blk reserved_sequence;
            };

            notifyResolverReplayRuntimesForCatalog(ctx.index_manager, ctx.resolution_runtime, ctx.promotion_runtime, sequence);
            if (ctx.executor.hasWorkers()) {
                ctx.executor.forceSequence(sequence);
                return sequence;
            }

            var applied_batch = replay_batch;
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
            if (!syncLevelParticipatesInDerivedBacklogPressure(sync_level)) return;
            const throttle_target = ctx.executor.backlogThrottleTargetSequence() orelse return;
            if (sync_level == .full_text) {
                try runDerivedUntilTargetsContext(ctx, sequence, sync_targets.full_text_indexes);
                return;
            }
            if (shouldDeferBacklogPressureForExternalDenseBulk(ctx, sync_level)) return;
            runDerivedUntilContext(ctx, throttle_target) catch |err| switch (err) {
                error.WriterLocked, error.ReplayDocumentNotVisible, error.ArtifactRepairRequired => {
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
                .full_index => try runMaintenanceUntilContext(ctx, sequence, sync_targets),
            }
        }

        pub fn waitForSyncLevel(self: *DB, sync_level: types.SyncLevel, sequence: u64, sync_targets: ManagedSyncTargets) !void {
            var ctx = self.batchContext();
            return try waitForSyncLevelContext(&ctx, sync_level, sequence, sync_targets);
        }

        pub fn syncLevelRequiresDerivedVisibility(sync_level: types.SyncLevel) bool {
            return switch (sync_level) {
                .propose, .write, .enrichments => false,
                .full_text, .full_index => true,
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

        pub fn shouldRunTargetAdvanceRepair(ctx: *AsyncContext, index_name: []const u8, now_ns: u64) bool {
            const cooldown_ns = denseCatchUpMaintenanceCooldownNs();
            if (cooldown_ns == 0) return true;
            const last_ns = ctx.target_advance_repair_last_ns.get(index_name) orelse return true;
            return now_ns -| last_ns >= cooldown_ns;
        }

        pub fn noteTargetAdvanceRepairRun(ctx: *AsyncContext, index_name: []const u8, now_ns: u64) !void {
            const gop = try ctx.target_advance_repair_last_ns.getOrPut(ctx.alloc, index_name);
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
            try filterManagedSyncTargetsForRelationalDerivedMaintenance(alloc, self.core.schema, &targets);
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

        fn applyDerivedBatchToIndex(self: *DB, batch: derived_types.DerivedBatch, index_ref: index_manager_mod.ManagedIndexRef) !void {
            const resources = self.core.asyncResources();
            const ctx = AsyncContext{
                .alloc = self.alloc,
                .store = resources.store,
                .applied_sequence_checkpoint_path = resources.applied_sequence_checkpoint_path,
                .index_manager = resources.index_manager,
                .apply_mutex = resources.apply_mutex,
                .text_merge_runtime = self.text_merge_runtime,
            };
            try applyDerivedBatchToIndexContext(&ctx, batch, index_ref);
        }

        pub fn applyDerivedBatchToIndexContext(ctx: *const AsyncContext, batch: derived_types.DerivedBatch, index_ref: index_manager_mod.ManagedIndexRef) !void {
            try applyDerivedBatchToIndexContextProfiled(ctx, batch, index_ref, null);
        }

        fn replaySourceDocumentExists(
            ctx: *const AsyncContext,
            source_doc_key: []const u8,
        ) !bool {
            const store_key = try replayDocumentStoreKeyAlloc(ctx.alloc, source_doc_key, ctx.relational_base_rows);
            defer ctx.alloc.free(store_key);
            const raw = ctx.store.get(ctx.alloc, store_key) catch |err| switch (err) {
                error.NotFound => return false,
                else => return err,
            };
            ctx.alloc.free(raw);
            return true;
        }

        fn denseEmbeddingWriteSourceDocumentExists(
            ctx: *const AsyncContext,
            write: mapper.DenseEmbeddingWrite,
        ) !bool {
            return try replaySourceDocumentExists(ctx, write.parent_doc_key orelse write.doc_key);
        }

        fn recordEmbeddingArtifactRepairIssueContext(
            ctx: *const AsyncContext,
            index_name: []const u8,
            artifact_key: []const u8,
            sequence: u64,
            reason: types.ArtifactRepairReason,
        ) !void {
            try artifact_repair.recordEmbeddingArtifactRepairIssueForReplay(
                ctx.alloc,
                ctx.store,
                index_name,
                artifact_key,
                sequence,
                reason,
            );
            if (ctx.repair_issue_counter) |counter| _ = counter.fetchAdd(1, .monotonic);
        }

        fn filterAndRecordDenseEmbeddingArtifactRepairIssuesForReplay(
            ctx: *const AsyncContext,
            index_name: []const u8,
            dims: u32,
            owned: *OwnedDenseEmbeddingWrites,
            sequence: u64,
        ) !void {
            var removed_indices = std.ArrayListUnmanaged(usize).empty;
            defer removed_indices.deinit(ctx.alloc);
            for (owned.writes, 0..) |write, write_idx| {
                const artifact_key = write.artifact_key orelse continue;
                if (try denseEmbeddingArtifactRepairReason(ctx, dims, artifact_key)) |reason| {
                    // A delete can commit after this journal record but before replay
                    // reads its artifact. That is an ordinary supersession, not
                    // corruption: advance past the stale upsert and let the following
                    // delete record remove any indexed value. This also prevents TTL
                    // cleanup from wedging replay on an artifact it correctly removed.
                    if (reason == .missing_artifact and !try denseEmbeddingWriteSourceDocumentExists(ctx, write)) {
                        try removed_indices.append(ctx.alloc, write_idx);
                        continue;
                    }
                    try recordEmbeddingArtifactRepairIssueContext(ctx, index_name, artifact_key, sequence, reason);
                }
            }
            if (removed_indices.items.len == 0) return;

            const kept_len = owned.writes.len - removed_indices.items.len;
            const kept = try owned.alloc.alloc(mapper.DenseEmbeddingWrite, kept_len);
            var kept_idx: usize = 0;
            var removed_idx: usize = 0;
            for (owned.writes, 0..) |write, write_idx| {
                if (removed_idx < removed_indices.items.len and removed_indices.items[removed_idx] == write_idx) {
                    removed_idx += 1;
                    if (owned.owns_doc_keys) {
                        owned.alloc.free(@constCast(write.doc_key));
                        if (write.parent_doc_key) |parent_doc_key| owned.alloc.free(@constCast(parent_doc_key));
                    }
                    continue;
                }
                kept[kept_idx] = write;
                kept_idx += 1;
            }
            if (owned.allocation_len > 0) owned.alloc.free(owned.writes.ptr[0..owned.allocation_len]);
            owned.writes = kept;
            owned.allocation_len = kept.len;
        }

        fn filterAndRecordSparseEmbeddingArtifactRepairIssuesForReplay(
            ctx: *const AsyncContext,
            index_name: []const u8,
            owned: *OwnedSparseEmbeddingWrites,
            sequence: u64,
        ) !void {
            var removed_indices = std.ArrayListUnmanaged(usize).empty;
            defer removed_indices.deinit(ctx.alloc);
            for (owned.writes, 0..) |write, write_idx| {
                const artifact_key = write.artifact_key orelse continue;
                if (try sparseEmbeddingArtifactRepairReason(ctx, artifact_key)) |reason| {
                    // Sparse artifact cleanup follows the same primary/journal
                    // ordering as dense cleanup. If a later delete already removed
                    // both the source and artifact, this stale upsert is superseded;
                    // the later delete record remains responsible for index removal.
                    if (reason == .missing_artifact and !try replaySourceDocumentExists(ctx, write.doc_key)) {
                        try removed_indices.append(ctx.alloc, write_idx);
                        continue;
                    }
                    try recordEmbeddingArtifactRepairIssueContext(ctx, index_name, artifact_key, sequence, reason);
                }
            }
            if (removed_indices.items.len == 0) return;

            const kept_len = owned.writes.len - removed_indices.items.len;
            const kept = try owned.alloc.alloc(mapper.SparseEmbeddingWrite, kept_len);
            var kept_idx: usize = 0;
            var removed_idx: usize = 0;
            for (owned.writes, 0..) |write, write_idx| {
                if (removed_idx < removed_indices.items.len and removed_indices.items[removed_idx] == write_idx) {
                    removed_idx += 1;
                    continue;
                }
                kept[kept_idx] = write;
                kept_idx += 1;
            }
            if (owned.allocation_len > 0) owned.alloc.free(owned.writes.ptr[0..owned.allocation_len]);
            owned.writes = kept;
            owned.allocation_len = kept.len;
        }

        fn accountDenseCoverage(
            ctx: *const AsyncContext,
            index_name: []const u8,
            batch: derived_types.DerivedBatch,
            writes: []const mapper.DenseEmbeddingWrite,
        ) !void {
            const external = ctx.index_manager.denseIndexUsesExternalCoverage(index_name);
            var produced = std.StringHashMapUnmanaged(void).empty;
            defer produced.deinit(ctx.alloc);
            for (writes) |write| {
                try produced.put(ctx.alloc, write.parent_doc_key orelse write.doc_key, {});
            }
            var outcomes = std.ArrayListUnmanaged(DerivedCoverageDocOutcome).empty;
            defer outcomes.deinit(ctx.alloc);
            if (!external) {
                for (batch.documents) |doc| {
                    if (doc.action == .delete or internal_keys.isInternalUserKey(doc.key) or !ctx.index_manager.byte_range.contains(doc.key)) continue;
                    try outcomes.append(ctx.alloc, .{
                        .doc_key = doc.key,
                        .outcome = if (produced.contains(doc.key)) .produced else .skipped,
                    });
                }
            } else {
                var iter = produced.keyIterator();
                while (iter.next()) |doc_key| {
                    if (internal_keys.isInternalUserKey(doc_key.*) or !ctx.index_manager.byte_range.contains(doc_key.*)) continue;
                    try outcomes.append(ctx.alloc, .{ .doc_key = doc_key.*, .outcome = .produced });
                }
            }
            try setDerivedCoverageOutcomes(ctx.alloc, ctx.store, ctx.index_manager, index_name, outcomes.items);
        }

        fn accountSparseCoverage(
            ctx: *const AsyncContext,
            index_name: []const u8,
            batch: derived_types.DerivedBatch,
            writes: []const mapper.SparseEmbeddingWrite,
        ) !void {
            const external = ctx.index_manager.sparseIndexUsesExternalCoverage(index_name);
            var produced = std.StringHashMapUnmanaged(void).empty;
            defer produced.deinit(ctx.alloc);
            for (writes) |write| {
                try produced.put(ctx.alloc, write.doc_key, {});
            }
            var outcomes = std.ArrayListUnmanaged(DerivedCoverageDocOutcome).empty;
            defer outcomes.deinit(ctx.alloc);
            if (!external) {
                for (batch.documents) |doc| {
                    if (doc.action == .delete or internal_keys.isInternalUserKey(doc.key) or !ctx.index_manager.byte_range.contains(doc.key)) continue;
                    try outcomes.append(ctx.alloc, .{
                        .doc_key = doc.key,
                        .outcome = if (produced.contains(doc.key)) .produced else .skipped,
                    });
                }
            } else {
                var iter = produced.keyIterator();
                while (iter.next()) |doc_key| {
                    if (internal_keys.isInternalUserKey(doc_key.*) or !ctx.index_manager.byte_range.contains(doc_key.*)) continue;
                    try outcomes.append(ctx.alloc, .{ .doc_key = doc_key.*, .outcome = .produced });
                }
            }
            try setDerivedCoverageOutcomes(ctx.alloc, ctx.store, ctx.index_manager, index_name, outcomes.items);
        }

        pub fn applyDerivedBatchToIndexContextProfiled(ctx: *const AsyncContext, batch: derived_types.DerivedBatch, index_ref: index_manager_mod.ManagedIndexRef, profile: ?*BatchProfile) !void {
            if (index_ref.kind == .full_text) {
                const apply_start_ns = monotonicTimeNs();
                const text_replay_options: index_manager_mod.IndexBatchOptions = .{
                    .compact_text = false,
                    .compact_text_segment_threshold = null,
                    .defer_text_compaction = true,
                };
                const delete_keys = try collectTextReplayDeleteKeys(ctx.alloc, batch);
                defer if (delete_keys.len > 0) ctx.alloc.free(delete_keys);
                var publication_context = try ctx.index_manager.acquireTextPublicationContext(ctx.alloc, index_ref.name);
                defer publication_context.deinit();
                var collected = try collectTextDocumentWritesForIndex(
                    ctx.alloc,
                    ctx.store,
                    batch.documents,
                    index_ref.name,
                    publication_context.chunk_backed,
                    ctx.index_manager.byte_range,
                    .{
                        .prefer_inline_when_store_tip_matches_sequence = batch.sequence,
                        .relational_base_rows = ctx.relational_base_rows,
                    },
                );
                defer collected.deinit();
                if (collected.missing_required != 0) return error.ReplayDocumentNotVisible;

                const reservation_limit = if (ctx.text_merge_runtime) |runtime| runtime.producerSegmentReservationLimit() else std.math.maxInt(usize);
                var write_start: usize = 0;
                var applied_first_chunk = false;
                while (!applied_first_chunk or write_start < collected.writes.items.len) {
                    const plan_base = write_start;
                    var publication_plan = try ctx.index_manager.planTextBatchPublication(
                        index_ref.name,
                        publication_context,
                        collected.writes.items[plan_base..],
                        reservation_limit,
                    );
                    defer publication_plan.deinit();

                    var relative_start: usize = 0;
                    var replan_suffix = false;
                    for (publication_plan.chunks) |planned| {
                        const write_end = plan_base + planned.end;
                        const write_chunk = collected.writes.items[plan_base + relative_start .. write_end];
                        var applied_chunk = false;
                        var merge_permit: ?text_merge_runtime_mod.TextMergeRuntime.ProducerPermit = null;
                        if (ctx.text_merge_runtime) |runtime| {
                            merge_permit = try runtime.acquireProducerPermit(
                                index_ref.name,
                                planned.estimate.segment_count,
                                planned.estimate.byte_count,
                            );
                        }
                        defer if (merge_permit) |*permit| permit.release();

                        var index_apply_guard = try ctx.index_manager.lockManagedIndexApply(index_ref);
                        defer index_apply_guard.unlock();
                        const before_apply = try ctx.index_manager.refreshTextPublicationContextAssumeCatalogLocked(
                            index_ref.name,
                            &publication_context,
                        );
                        if (before_apply == .projection_changed) {
                            replan_suffix = true;
                        } else {
                            try ctx.index_manager.applyTextBatchByNameWithOptions(
                                ctx.store,
                                index_ref.name,
                                if (applied_first_chunk) &.{} else delete_keys,
                                write_chunk,
                                text_replay_options,
                            );
                            applied_chunk = true;
                            const after_apply = try ctx.index_manager.refreshTextPublicationContextAssumeCatalogLocked(
                                index_ref.name,
                                &publication_context,
                            );
                            replan_suffix = after_apply == .projection_changed;
                            if (ctx.text_merge_runtime) |runtime| runtime.notify();
                        }

                        if (!applied_chunk) break;
                        applied_first_chunk = true;
                        write_start = write_end;
                        relative_start = planned.end;
                        if (replan_suffix and write_start < collected.writes.items.len) break;
                    }

                    if (write_start == collected.writes.items.len) break;
                    std.debug.assert(replan_suffix);
                }
                std.debug.assert(applied_first_chunk and write_start == collected.writes.items.len);
                if (profile) |active_profile| DB.WritePathCallbacks.record_profile_ns(profile, &active_profile.full_text_apply_ns, apply_start_ns);
                return;
            }

            var index_apply_guard = try ctx.index_manager.lockManagedIndexApply(index_ref);
            defer index_apply_guard.unlock();
            switch (index_ref.kind) {
                .full_text => unreachable,
                .dense_vector => {
                    const dense_apply_start_ns = monotonicTimeNs();
                    const dense_finish_options = denseCatchUpFinishOptions();
                    const use_local_streaming_session = db_internal.denseApplyUsesLocalStreamingSession(ctx, index_ref.name);
                    var dense_streaming_session_open = false;
                    if (use_local_streaming_session) {
                        try ctx.index_manager.beginDenseStreamingReplaySessionByName(index_ref.name);
                        dense_streaming_session_open = true;
                        errdefer if (dense_streaming_session_open) ctx.index_manager.abortDenseStreamingReplaySessionByName(index_ref.name);
                    }
                    const before_hbc_profile = if (profile != null) ctx.index_manager.denseWriteProfileByName(index_ref.name) else null;
                    const batch_options: backend_types.BatchOptions = .{ .mode = .bulk_ingest };
                    var dense_embeddings = try collectDenseEmbeddingWritesForBatch(
                        ctx.alloc,
                        ctx.index_manager,
                        batch.dense_embeddings,
                        batch.changed_artifact_keys,
                        index_ref.name,
                    );
                    defer dense_embeddings.deinit();
                    if (ctx.index_manager.denseIndex(index_ref.name)) |entry| {
                        try filterAndRecordDenseEmbeddingArtifactRepairIssuesForReplay(ctx, index_ref.name, entry.dims, &dense_embeddings, batch.sequence);
                    }
                    try ctx.index_manager.validateDenseEmbeddingArtifactsByName(ctx.store, index_ref.name, dense_embeddings.writes);

                    const dense_delete_start_ns = monotonicTimeNs();
                    try ctx.index_manager.deleteDenseBatchByNameWithOptions(ctx.store, index_ref.name, batch.deleted_keys, batch_options);
                    try ctx.index_manager.deleteDenseBatchByNameWithOptions(ctx.store, index_ref.name, batch.overwritten_doc_keys, batch_options);
                    try deleteDerivedCoverageForDocKeys(ctx.alloc, ctx.store, ctx.index_manager, index_ref.name, batch.deleted_keys);
                    try deleteDerivedCoverageForDocKeys(ctx.alloc, ctx.store, ctx.index_manager, index_ref.name, batch.overwritten_doc_keys);
                    if (profile) |active_profile| DB.WritePathCallbacks.record_profile_ns(profile, &active_profile.dense_delete_ns, dense_delete_start_ns);

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
                    try accountDenseCoverage(ctx, index_ref.name, batch, dense_embeddings.writes);
                    if (profile) |active_profile| {
                        DB.WritePathCallbacks.record_profile_ns(profile, &active_profile.dense_embedding_apply_ns, dense_embedding_start_ns);
                        if (before_hbc_profile) |before| {
                            if (ctx.index_manager.denseWriteProfileByName(index_ref.name)) |after| {
                                addHbcWriteProfileDelta(active_profile, before, after);
                            }
                        }
                    }
                    if (use_local_streaming_session) {
                        try ctx.index_manager.finishDenseStreamingReplaySessionByNameWithOptions(index_ref.name, dense_finish_options);
                        dense_streaming_session_open = false;
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
                    try filterAndRecordSparseEmbeddingArtifactRepairIssuesForReplay(ctx, index_ref.name, &sparse_embeddings, batch.sequence);
                    try ctx.index_manager.validateSparseEmbeddingArtifactsByName(ctx.store, index_ref.name, sparse_embeddings.writes);

                    const sparse_delete_start_ns = if (emit_sparse_write_profile) monotonicTimeNs() else 0;
                    try ctx.index_manager.deleteSparseBatchByNameWithOptions(index_ref.name, batch.deleted_keys, batch_options);
                    try ctx.index_manager.deleteSparseBatchByNameWithOptions(index_ref.name, batch.overwritten_doc_keys, batch_options);
                    try deleteDerivedCoverageForDocKeys(ctx.alloc, ctx.store, ctx.index_manager, index_ref.name, batch.deleted_keys);
                    try deleteDerivedCoverageForDocKeys(ctx.alloc, ctx.store, ctx.index_manager, index_ref.name, batch.overwritten_doc_keys);
                    if (emit_sparse_write_profile) sparse_delete_ns = monotonicTimeNs() - sparse_delete_start_ns;

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
                    try accountSparseCoverage(ctx, index_ref.name, batch, sparse_embeddings.writes);
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
                                .repair = .{
                                    .enabled = true,
                                    .sequence = batch.sequence,
                                },
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
                        var graph_mutations = try artifact_replay.collectGraphMutationsForArtifacts(ctx.alloc, ctx.store, graph_artifact_keys, index_ref.name, .{
                            .repair = .{
                                .enabled = true,
                                .sequence = batch.sequence,
                            },
                        });
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

        fn graphArtifactSourceConsumesArtifactKey(
            index_manager: *index_manager_mod.IndexManager,
            source: index_manager_mod.GraphArtifactSource,
            artifact_key: []const u8,
        ) bool {
            return artifact_replay.graphArtifactSourceConsumesArtifactKey(index_manager, source, artifact_key);
        }

        fn managedIndexRecordApplicability(
            index_manager: *index_manager_mod.IndexManager,
            record: change_journal_mod.Record,
            index_ref: index_manager_mod.ManagedIndexRef,
        ) ManagedIndexBatchApplicability {
            switch (index_ref.kind) {
                .full_text, .algebraic => {
                    if (record.changed_doc_keys.len > 0 or
                        record.deleted_doc_keys.len > 0 or
                        record.overwritten_doc_keys.len > 0) return .relevant;
                    return .irrelevant;
                },
                .dense_vector => {
                    if (record.deleted_doc_keys.len > 0 or record.overwritten_doc_keys.len > 0) return .relevant;
                    // Artifact-backed indexes consume generated artifact records, not
                    // the source document record that scheduled enrichment. Treating
                    // that source record as perpetually applicable prevents a clean
                    // target advance after an idempotent index recreate reuses already
                    // durable artifacts. The artifact target counter and active index
                    // cardinality below remain the completeness gate.
                    if (record.changed_doc_keys.len > 0) {
                        const entry = index_manager.denseIndex(index_ref.name);
                        if (entry == null or !denseIndexIsArtifactBacked(entry.?)) return .relevant;
                    }
                    if (batchHasEmbeddingArtifactForManagedIndex(index_manager, index_ref, record.changed_artifact_keys)) return .relevant;
                    return .irrelevant;
                },
                .sparse_vector => {
                    if (record.changed_doc_keys.len > 0 or
                        record.deleted_doc_keys.len > 0 or
                        record.overwritten_doc_keys.len > 0) return .relevant;
                    if (batchHasEmbeddingArtifactForManagedIndex(index_manager, index_ref, record.changed_artifact_keys)) return .relevant;
                    return .irrelevant;
                },
                .graph => {
                    if (record.deleted_doc_keys.len > 0) return .relevant;
                    for (record.changed_artifact_keys) |artifact_key| {
                        if (!internal_keys.isResolutionArtifactKey(artifact_key) and !internal_keys.isGraphEdgeArtifactKey(artifact_key)) {
                            const source = index_manager.graphArtifactSource(index_ref.name) orelse continue;
                            if (graphArtifactSourceConsumesArtifactKey(index_manager, source, artifact_key)) return .relevant;
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

        pub fn batchAdvancesManagedIndexApplyState(
            index_manager: *index_manager_mod.IndexManager,
            batch: derived_types.DerivedBatch,
            index_ref: index_manager_mod.ManagedIndexRef,
        ) !bool {
            if (!batchAffectsManagedIndex(index_manager, batch, index_ref)) return false;

            switch (index_ref.kind) {
                .dense_vector, .sparse_vector => {
                    if (batch.deleted_keys.len > 0 or batch.overwritten_doc_keys.len > 0) return true;
                    const artifact_backed_dense = if (index_ref.kind == .dense_vector)
                        if (index_manager.denseIndex(index_ref.name)) |entry| denseIndexIsArtifactBacked(entry) else false
                    else
                        false;
                    if (!try index_manager.requiresEnrichmentReplay(index_ref.name) and !artifact_backed_dense) return true;
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
                runtime.notify();
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
                runtime.notify();
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
                        .text_merge_runtime = if (ctx.async_context) |active| active.text_merge_runtime else null,
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
                    ctx.index_manager.checkpointIo(),
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

        pub fn catchUpManagedIndexWithBatchContext(
            ctx: *const BatchExecutionContext,
            index_ref: index_manager_mod.ManagedIndexRef,
            applied: u64,
            target_sequence: u64,
        ) !derived_worker.CatchUpStats {
            var replay_ctx = ReplayApplyContextBatch{
                .batch = ctx,
                .dense_bulk_session_scope = .external,
            };
            return try derived_worker.catchUpIndexWithOptions(
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
        }

        pub fn catchUpManagedIndexWithBatchContextOptions(
            ctx: *const BatchExecutionContext,
            index_ref: index_manager_mod.ManagedIndexRef,
            applied: u64,
            target_sequence: u64,
            max_records_per_window: usize,
            max_items_per_window: usize,
            max_windows_per_call: usize,
            deadline_ns: ?u64,
        ) !derived_worker.CatchUpStats {
            var replay_ctx = ReplayApplyContextBatch{
                .batch = ctx,
                .dense_bulk_session_scope = .external,
            };
            return try derived_worker.catchUpIndexWithOptions(
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
                    .max_records_per_window = max_records_per_window,
                    .max_items_per_window = max_items_per_window,
                    .max_windows_per_call = max_windows_per_call,
                    .deadline_ns = deadline_ns,
                },
            );
        }

        pub fn canAdvanceDerivedReplayTargetForBatchContext(
            ctx: *const BatchExecutionContext,
            index_ref: index_manager_mod.ManagedIndexRef,
            from_sequence: u64,
            target_sequence: u64,
        ) !bool {
            return try canAdvanceDerivedReplayTargetContext(ctx, index_ref, from_sequence, target_sequence);
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

        pub fn replayPendingDerivedBatches(
            self: *DB,
            progress_ctx: ?*anyopaque,
            progress_hook: ?db_internal.ReplayProgressHook,
        ) !void {
            try replayPendingDerivedBatchesWithOptions(
                self,
                progress_ctx,
                progress_hook,
                true,
            );
        }

        pub fn replayPendingDerivedBatchesWithOptions(
            self: *DB,
            progress_ctx: ?*anyopaque,
            progress_hook: ?db_internal.ReplayProgressHook,
            truncate_replay: bool,
        ) !void {
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
            if (truncate_replay) {
                var truncate_ctx = self.batchContext();
                try truncateReplayJournalIfSafeContext(&truncate_ctx);
            }
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
            try replay_ctx.db.core.batchExecutionResources().index_manager.beginDenseStreamingReplaySessionByName(index_ref.name);
        }

        fn finishDerivedCatchUpWindow(ctx_ptr: *anyopaque, index_ref: index_manager_mod.ManagedIndexRef, success: bool) !void {
            if (index_ref.kind != .dense_vector) return;
            const replay_ctx: *ReplayApplyContext = @ptrCast(@alignCast(ctx_ptr));
            const resources = replay_ctx.db.core.batchExecutionResources();
            if (!success) {
                resources.index_manager.abortDenseStreamingReplaySessionByName(index_ref.name);
                return;
            }
            errdefer resources.index_manager.abortDenseStreamingReplaySessionByName(index_ref.name);
            const finish_start_ns = monotonicTimeNs();
            try resources.index_manager.finishDenseStreamingReplaySessionByNameWithOptions(index_ref.name, DB.DerivedAsyncCallbacks.dense_catch_up_finish_options());
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
                .text_merge_runtime = if (ctx.async_context) |active| active.text_merge_runtime else null,
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
            try replay_ctx.batch.index_manager.beginDenseStreamingReplaySessionByName(index_ref.name);
        }

        fn finishDerivedCatchUpWindowContext(ctx_ptr: *anyopaque, index_ref: index_manager_mod.ManagedIndexRef, success: bool) !void {
            if (index_ref.kind != .dense_vector) return;
            const replay_ctx: *ReplayApplyContextBatch = @ptrCast(@alignCast(ctx_ptr));
            if (!success) {
                replay_ctx.batch.index_manager.abortDenseStreamingReplaySessionByName(index_ref.name);
                return;
            }
            errdefer replay_ctx.batch.index_manager.abortDenseStreamingReplaySessionByName(index_ref.name);
            const finish_start_ns = monotonicTimeNs();
            try replay_ctx.batch.index_manager.finishDenseStreamingReplaySessionByNameWithOptions(index_ref.name, DB.DerivedAsyncCallbacks.dense_catch_up_finish_options());
            try replay_ctx.batch.index_manager.checkpointLsmWalForManagedIndex(index_ref);
            if (replay_ctx.batch.index_manager.resource_manager) |manager| {
                manager.noteDenseReplayWindowResult(.{ .finish_ns = elapsedSince(finish_start_ns) });
            }
        }

        fn syncLevelParticipatesInDerivedBacklogPressure(sync_level: types.SyncLevel) bool {
            return switch (sync_level) {
                .propose => false,
                .write, .enrichments, .full_text, .full_index => true,
            };
        }

        test "durable writes participate in derived backlog admission control" {
            // `.write` controls visibility/durability, not admission. Once asynchronous
            // derived payloads cross their memory budget, a normal durable writer must
            // help the worker catch up instead of admitting an unbounded replay queue.
            try std.testing.expect(syncLevelParticipatesInDerivedBacklogPressure(.write));
            try std.testing.expect(!syncLevelParticipatesInDerivedBacklogPressure(.propose));
        }

        fn shouldDeferBacklogPressureForExternalDenseBulk(ctx: *const BatchExecutionContext, sync_level: types.SyncLevel) bool {
            switch (sync_level) {
                .propose, .write, .enrichments => {},
                .full_text, .full_index => return false,
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
                        if (!internal_keys.isResolutionArtifactKey(artifact_key) and !internal_keys.isGraphEdgeArtifactKey(artifact_key)) {
                            const source = index_manager.graphArtifactSource(index_ref.name) orelse continue;
                            if (graphArtifactSourceConsumesArtifactKey(index_manager, source, artifact_key)) return .relevant;
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
            const enriched_updates = try appliedSequenceUpdatesWithConfigHashes(ctx.alloc, ctx.index_manager, updates);
            defer ctx.alloc.free(enriched_updates);
            if (ctx.async_context) |async_ctx| {
                var seq_lock = lockAtomicWithBackoffProfiled(
                    &async_ctx.applied_sequence_mutex,
                    &async_ctx.stats.applied_sequence_mutex,
                );
                defer seq_lock.unlock();
                try saveDenseProjectionMetadataForAppliedSequenceUpdates(ctx.index_manager, enriched_updates);
                try checkpointManagedProjectionEffectsForAppliedSequenceUpdates(ctx.index_manager, enriched_updates);
                try apply_state.saveAppliedSequencesWithCheckpoint(
                    ctx.alloc,
                    ctx.index_manager.checkpointIo(),
                    ctx.store,
                    ctx.applied_sequence_checkpoint_path,
                    enriched_updates,
                );
                for (enriched_updates) |update| {
                    _ = try finalizeCoveredDenseProjectionCheckpoint(async_ctx, update.index_name, update.sequence);
                }
                try DB.DerivedAsyncCallbacks.save_index_status_snapshots(ctx.alloc, ctx.store, ctx.index_manager, enriched_updates);
                return;
            }
            try saveDenseProjectionMetadataForAppliedSequenceUpdates(ctx.index_manager, enriched_updates);
            try checkpointManagedProjectionEffectsForAppliedSequenceUpdates(ctx.index_manager, enriched_updates);
            try apply_state.saveAppliedSequencesWithCheckpoint(
                ctx.alloc,
                ctx.index_manager.checkpointIo(),
                ctx.store,
                ctx.applied_sequence_checkpoint_path,
                enriched_updates,
            );
            try DB.DerivedAsyncCallbacks.save_index_status_snapshots(ctx.alloc, ctx.store, ctx.index_manager, enriched_updates);
        }

        fn appliedSequenceUpdatesWithConfigHashes(
            alloc: Allocator,
            index_manager: *const index_manager_mod.IndexManager,
            updates: []const apply_state.AppliedSequenceUpdate,
        ) ![]apply_state.AppliedSequenceUpdate {
            const enriched = try alloc.alloc(apply_state.AppliedSequenceUpdate, updates.len);
            for (updates, 0..) |update, i| {
                enriched[i] = update;
                if (enriched[i].config_hash == 0) {
                    if (index_manager.get(update.index_name)) |cfg| {
                        enriched[i].config_hash = types.indexConfigHash(cfg.*);
                    }
                }
            }
            return enriched;
        }

        pub fn saveDenseProjectionMetadataForAppliedSequenceUpdates(
            index_manager: *index_manager_mod.IndexManager,
            updates: []const apply_state.AppliedSequenceUpdate,
        ) !void {
            for (updates) |update| {
                const current = index_manager.denseProjectionCheckpointMetadata(update.index_name) orelse continue;
                try index_manager.saveDenseProjectionCheckpointMetadata(update.index_name, .{
                    .applied_sequence = update.sequence,
                    .status = current.status,
                    .generation = if (update.generation != 0) update.generation else current.generation,
                    .config_hash = if (update.config_hash != 0) update.config_hash else current.config_hash,
                });
            }
        }

        pub fn checkpointManagedProjectionEffectsForAppliedSequenceUpdates(
            index_manager: *index_manager_mod.IndexManager,
            updates: []const apply_state.AppliedSequenceUpdate,
        ) !void {
            for (updates) |update| {
                const cfg = index_manager.get(update.index_name) orelse continue;
                try index_manager.checkpointLsmWalForManagedIndex(.{
                    .name = update.index_name,
                    .kind = cfg.kind,
                });
            }
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
                runtime.notify();
            };
            if (index_ref.kind == .sparse_vector) if (ctx.sparse_compaction_runtime) |runtime| {
                runtime.notify();
            };
            return true;
        }

        fn beginDenseStreamingReplaySessionForAsyncCatchUp(ctx: *AsyncContext, index_ref: index_manager_mod.ManagedIndexRef) !void {
            var index_apply_guard = try ctx.index_manager.lockManagedIndexApply(index_ref);
            defer index_apply_guard.unlock();
            try ctx.index_manager.beginDenseStreamingReplaySessionByName(index_ref.name);
        }

        fn finishDenseStreamingReplaySessionForAsyncCatchUp(
            ctx: *AsyncContext,
            index_ref: index_manager_mod.ManagedIndexRef,
            options: backend_types.BulkIngestFinishOptions,
        ) !void {
            var index_apply_guard = try ctx.index_manager.lockManagedIndexApply(index_ref);
            defer index_apply_guard.unlock();
            try ctx.index_manager.finishDenseStreamingReplaySessionByNameWithOptions(index_ref.name, options);
        }

        fn abortDenseStreamingReplaySessionForAsyncCatchUp(ctx: *AsyncContext, index_ref: index_manager_mod.ManagedIndexRef) void {
            var index_apply_guard = ctx.index_manager.lockManagedIndexApply(index_ref) catch return;
            defer index_apply_guard.unlock();
            ctx.index_manager.abortDenseStreamingReplaySessionByName(index_ref.name);
        }

        pub fn beginDerivedCatchUpSessionAsync(ctx_ptr: *anyopaque, index_ref: index_manager_mod.ManagedIndexRef) !void {
            if (index_ref.kind != .dense_vector) return;
            const ctx = asyncContextFromOpaque(ctx_ptr);

            try db_internal.beginDenseCatchUpSessionTracked(ctx, index_ref.name);
            errdefer finishDenseCatchUpSessionTrackedBestEffort(ctx, index_ref.name);
            try beginDenseStreamingReplaySessionForAsyncCatchUp(ctx, index_ref);
            errdefer abortDenseStreamingReplaySessionForAsyncCatchUp(ctx, index_ref);
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
                abortDenseStreamingReplaySessionForAsyncCatchUp(ctx, index_ref);
                const lifecycle_completed = try finishDenseCatchUpSessionTrackedAndFinalize(ctx, index_ref.name);
                if (lifecycle_completed) DB.notifyQueryVisibilityHook(ctx, .publish_blocking);
                return;
            }
            var catch_up_tracked = true;
            errdefer if (catch_up_tracked) finishDenseCatchUpSessionTrackedBestEffort(ctx, index_ref.name);
            var streaming_session_finished = false;
            errdefer if (!streaming_session_finished) abortDenseStreamingReplaySessionForAsyncCatchUp(ctx, index_ref);
            const finish_start_ns = monotonicTimeNs();
            const before_lsm_stats = denseLsmWriteStatsSnapshot(ctx, index_ref.name);

            const maintenance_steps: usize = 0;
            const maintenance_ns: u64 = 0;
            setDenseCatchUpPhase(ctx, .bulk_finish);
            var finish_options = DB.DerivedAsyncCallbacks.dense_catch_up_finish_options();
            finish_options.progress_ctx = ctx;
            finish_options.progress_fn = noteDenseBulkFinishProgress;
            const finalize_start_ns = monotonicTimeNs();
            try finishDenseStreamingReplaySessionForAsyncCatchUp(ctx, index_ref, finish_options);
            streaming_session_finished = true;
            const finalize_ns = elapsedSince(finalize_start_ns);
            setDenseCatchUpPhase(ctx, .applied_sequence_flush);
            var lifecycle_completed = false;
            _ = blk: {
                var seq_lock = lockAtomicWithBackoffProfiled(&ctx.applied_sequence_mutex, &ctx.stats.applied_sequence_mutex);
                defer seq_lock.unlock();
                break :blk try flushFinishedDenseAppliedSequenceLocked(ctx, index_ref.name, &lifecycle_completed);
            };
            try ctx.index_manager.checkpointLsmWalForManagedIndex(index_ref);
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
            catch_up_tracked = false;
            lifecycle_completed = try finishDenseCatchUpSessionTrackedAndFinalize(ctx, index_ref.name) or lifecycle_completed;
            if (lifecycle_completed) DB.notifyQueryVisibilityHook(ctx, .publish_blocking);
        }

        pub fn persistAppliedSequenceAsync(ctx_ptr: *anyopaque, index_name: []const u8, sequence: u64, force: bool) !bool {
            const ctx = asyncContextFromOpaque(ctx_ptr);
            if (force) {
                var lifecycle_completed = false;
                const published = blk: {
                    var seq_lock = lockAtomicWithBackoffProfiled(&ctx.applied_sequence_mutex, &ctx.stats.applied_sequence_mutex);
                    defer seq_lock.unlock();
                    _ = ctx.stats.applied_sequence.note_calls.fetchAdd(1, .monotonic);
                    _ = ctx.stats.applied_sequence.forced_flush_calls.fetchAdd(1, .monotonic);
                    try ctx.applied_sequence_coalescer.note(ctx.alloc, index_name, sequence);
                    break :blk try flushPendingAppliedSequencesLocked(ctx, true, &lifecycle_completed);
                };
                const dense_index = ctx.index_manager.denseProjectionCheckpointMetadata(index_name) != null;
                if (published and !dense_index) {
                    DB.notifyQueryVisibilityHook(ctx, .publish_consistent);
                }
                return published;
            }
            if (!ctx.applied_sequence_mutex.tryLock()) {
                _ = ctx.stats.applied_sequence.skipped_flush_calls.fetchAdd(1, .monotonic);
                return false;
            }
            errdefer ctx.applied_sequence_mutex.unlock();
            var lifecycle_completed = false;
            _ = ctx.stats.applied_sequence.note_calls.fetchAdd(1, .monotonic);
            try ctx.applied_sequence_coalescer.note(ctx.alloc, index_name, sequence);
            if (db_internal.shouldDeferAppliedSequenceFlush(ctx, false)) {
                _ = ctx.stats.applied_sequence.skipped_flush_calls.fetchAdd(1, .monotonic);
                ctx.applied_sequence_mutex.unlock();
                return false;
            }
            if (!ctx.applied_sequence_coalescer.shouldFlush(monotonicTimeNs())) {
                _ = ctx.stats.applied_sequence.skipped_flush_calls.fetchAdd(1, .monotonic);
                ctx.applied_sequence_mutex.unlock();
                return false;
            }
            const published = try flushPendingAppliedSequencesLocked(ctx, false, &lifecycle_completed);
            ctx.applied_sequence_mutex.unlock();
            const dense_index = ctx.index_manager.denseProjectionCheckpointMetadata(index_name) != null;
            if (published and !dense_index) {
                DB.notifyQueryVisibilityHook(ctx, .publish_consistent);
            }
            return published;
        }

        fn clampReplayTruncationForRepairPins(
            alloc: Allocator,
            checkpoint: ?index_repair_state.Location,
            effective: u64,
        ) !u64 {
            const location = checkpoint orelse return effective;
            var state = index_repair_state.loadAt(alloc, location) catch |err| switch (err) {
                error.FileNotFound => return effective,
                // A malformed local repair checkpoint may have contained a zero or
                // finalized replay pin. Retain everything until an operator repairs
                // the checkpoint; never convert corruption into replay loss.
                error.InvalidIndexRepairState => return 0,
                else => return err,
            };
            defer state.deinit(alloc);
            const pin = state.minimumRetainAfterSequence() orelse return effective;
            return @min(effective, pin);
        }

        pub fn truncateReplaySequenceAsync(ctx_ptr: *anyopaque, sequence: u64) !void {
            const ctx = asyncContextFromOpaque(ctx_ptr);
            if (ctx.repair_replay_mutex) |mutex| db_internal.lockAtomicWithBackoff(mutex);
            defer if (ctx.repair_replay_mutex) |mutex| mutex.unlock();
            var effective = sequence;
            // Generated enrichment consumes the same durable replay journal as
            // managed-index workers, but advances independently during provider
            // outages. Clamp to its persisted checkpoint so restart can replay
            // every artifact that has not been generated yet.
            if (ctx.index_manager.hasGeneratedEnrichmentTargets()) {
                const enrichment_applied = try enrichment_state.loadAppliedSequence(
                    ctx.alloc,
                    ctx.store,
                    enrichment_runtime_mod.scope_name,
                );
                effective = @min(effective, enrichment_applied);
            }
            if (ctx.resolution_runtime) |runtime| {
                effective = clampReplayTruncationForReplayStage(effective, ctx.index_manager, runtime.stats());
            }
            if (ctx.promotion_runtime) |runtime| {
                effective = clampReplayTruncationForReplayStage(effective, ctx.index_manager, runtime.stats());
            }
            effective = try clampReplayTruncationForRepairPins(ctx.alloc, ctx.index_repair_checkpoint, effective);
            try ctx.store.truncateReplayUpTo(ctx.alloc, effective);
        }

        pub fn truncateReplayJournalIfSafeContext(ctx: *const BatchExecutionContext) !void {
            if (ctx.repair_replay_mutex) |mutex| db_internal.lockAtomicWithBackoff(mutex);
            defer if (ctx.repair_replay_mutex) |mutex| mutex.unlock();
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
                    ctx.index_manager.checkpointIo(),
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
            min_applied = try clampReplayTruncationForRepairPins(ctx.alloc, ctx.index_repair_checkpoint, min_applied);
            if (min_applied == 0 or min_applied == std.math.maxInt(u64)) return;
            try truncateReplayLogs(ctx, min_applied);
        }

        pub fn rebuildDenseIndexForTargetCoverageContext(
            ctx: anytype,
            index_name: []const u8,
            rebuild_chunk_size: usize,
        ) !usize {
            var result = try rebuildDenseIndexForTargetCoverageSliceContext(ctx, index_name, rebuild_chunk_size, null);
            defer result.deinit(ctx.alloc);
            std.debug.assert(result.complete());
            return result.rebuilt;
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
            var lifecycle_completed = false;
            const published = try flushPendingAppliedSequencesLocked(self.async_context, true, &lifecycle_completed);
            if (lifecycle_completed) {
                DB.notifyQueryVisibilityHook(self.async_context, .publish_blocking);
            } else if (published) {
                DB.notifyQueryVisibilityHook(self.async_context, .publish_consistent);
            }
        }

        pub fn denseIndexRebuildStatePathAlloc(self: *DB, alloc: Allocator, index_name: []const u8) ![]u8 {
            return try std.fmt.allocPrint(
                alloc,
                "{s}/indexes/{s}",
                .{ self.core.index_manager.base_path, index_name },
            );
        }

        const DenseArtifactTargetKey = struct {
            artifact_name: []const u8,
            dims: u32,
        };

        const DenseArtifactTargetKeyContext = struct {
            pub fn hash(_: @This(), key: DenseArtifactTargetKey) u64 {
                var hasher = std.hash.Wyhash.init(0);
                const artifact_name_len: u64 = @intCast(key.artifact_name.len);
                hasher.update(std.mem.asBytes(&artifact_name_len));
                hasher.update(key.artifact_name);
                hasher.update(std.mem.asBytes(&key.dims));
                return hasher.final();
            }

            pub fn eql(_: @This(), lhs: DenseArtifactTargetKey, rhs: DenseArtifactTargetKey) bool {
                return lhs.dims == rhs.dims and std.mem.eql(u8, lhs.artifact_name, rhs.artifact_name);
            }
        };

        const DenseArtifactTargetLookup = struct {
            by_artifact: std.HashMapUnmanaged(DenseArtifactTargetKey, std.ArrayListUnmanaged(usize), DenseArtifactTargetKeyContext, 80) = .empty,

            fn deinit(self: *@This(), alloc: Allocator) void {
                var values = self.by_artifact.valueIterator();
                while (values.next()) |indices| indices.deinit(alloc);
                self.by_artifact.deinit(alloc);
                self.* = .{};
            }

            fn add(self: *@This(), alloc: Allocator, artifact_name: []const u8, dims: u32, dense_index_idx: usize) !void {
                const key: DenseArtifactTargetKey = .{
                    .artifact_name = artifact_name,
                    .dims = dims,
                };
                var entry = try self.by_artifact.getOrPut(alloc, key);
                if (!entry.found_existing) entry.value_ptr.* = .empty;
                try entry.value_ptr.append(alloc, dense_index_idx);
            }
        };

        const DenseArtifactCounterTarget = struct {
            index_name: []u8,
            artifact_name: []u8,
            dims: u32,

            fn deinit(self: *@This(), alloc: Allocator) void {
                alloc.free(self.index_name);
                alloc.free(self.artifact_name);
                self.* = undefined;
            }
        };

        /// Includes status-only dense configs so quarantined generations keep
        /// observing concurrent artifact mutations during counter bootstrap.
        const DenseArtifactCounterCatalog = struct {
            targets: std.ArrayListUnmanaged(DenseArtifactCounterTarget) = .empty,
            by_artifact: std.HashMapUnmanaged(DenseArtifactTargetKey, std.ArrayListUnmanaged(usize), DenseArtifactTargetKeyContext, 80) = .empty,

            fn deinit(self: *@This(), alloc: Allocator) void {
                var values = self.by_artifact.valueIterator();
                while (values.next()) |indices| indices.deinit(alloc);
                self.by_artifact.deinit(alloc);
                for (self.targets.items) |*target| target.deinit(alloc);
                self.targets.deinit(alloc);
                self.* = .{};
            }

            fn containsIndex(self: *const @This(), index_name: []const u8) bool {
                for (self.targets.items) |target| {
                    if (std.mem.eql(u8, target.index_name, index_name)) return true;
                }
                return false;
            }

            fn add(
                self: *@This(),
                alloc: Allocator,
                index_name: []const u8,
                artifact_name: []const u8,
                dims: u32,
            ) !void {
                if (self.containsIndex(index_name)) return;
                var owned_index_name: ?[]u8 = try alloc.dupe(u8, index_name);
                errdefer if (owned_index_name) |value| alloc.free(value);
                var owned_artifact_name: ?[]u8 = try alloc.dupe(u8, artifact_name);
                errdefer if (owned_artifact_name) |value| alloc.free(value);
                try self.targets.append(alloc, .{
                    .index_name = owned_index_name.?,
                    .artifact_name = owned_artifact_name.?,
                    .dims = dims,
                });
                owned_index_name = null;
                owned_artifact_name = null;
                var keep_target = false;
                errdefer if (!keep_target) {
                    var removed = self.targets.pop().?;
                    removed.deinit(alloc);
                };

                const target_idx = self.targets.items.len - 1;
                const target = &self.targets.items[target_idx];
                const key: DenseArtifactTargetKey = .{
                    .artifact_name = target.artifact_name,
                    .dims = target.dims,
                };
                var entry = try self.by_artifact.getOrPut(alloc, key);
                if (!entry.found_existing) entry.value_ptr.* = .empty;
                errdefer if (!entry.found_existing) {
                    entry.value_ptr.deinit(alloc);
                    _ = self.by_artifact.remove(key);
                };
                try entry.value_ptr.append(alloc, target_idx);
                keep_target = true;
            }

            fn init(
                alloc: Allocator,
                index_manager: *const index_manager_mod.IndexManager,
            ) !DenseArtifactCounterCatalog {
                var catalog: DenseArtifactCounterCatalog = .{};
                errdefer catalog.deinit(alloc);

                for (index_manager.dense_indexes.items) |*entry| {
                    const artifact_backed = entry.external or entry.chunk_name != null or entry.embedding_name != null;
                    if (!artifact_backed) continue;
                    try catalog.add(
                        alloc,
                        entry.config.name,
                        denseArtifactNameForEntry(entry),
                        entry.dims,
                    );
                }
                for (index_manager.status_only_index_configs) |cfg| {
                    if (cfg.kind != .dense_vector or catalog.containsIndex(cfg.name)) continue;
                    const requires_coverage = index_manager_mod.denseConfigRequiresArtifactCoverage(alloc, cfg) catch |err| switch (err) {
                        error.OutOfMemory => return err,
                        else => continue,
                    };
                    if (!requires_coverage) continue;
                    const artifact_name = index_manager_mod.denseConfigArtifactNameAlloc(alloc, cfg) catch |err| switch (err) {
                        error.OutOfMemory => return err,
                        else => continue,
                    };
                    defer alloc.free(artifact_name);
                    const dims = index_manager_mod.denseConfigDimensions(alloc, cfg) catch |err| switch (err) {
                        error.OutOfMemory => return err,
                        else => continue,
                    };
                    try catalog.add(
                        alloc,
                        cfg.name,
                        artifact_name,
                        dims,
                    );
                }
                return catalog;
            }

            fn targetsFor(
                self: *const @This(),
                artifact_name: []const u8,
                dims: u32,
            ) []const usize {
                const indices = self.by_artifact.get(.{
                    .artifact_name = artifact_name,
                    .dims = dims,
                }) orelse return &.{};
                return indices.items;
            }
        };

        pub fn denseArtifactTargetCounterKeyAlloc(alloc: Allocator, index_name: []const u8) ![]u8 {
            return try std.fmt.allocPrint(alloc, "{s}{s}", .{ dense_artifact_target_counter_prefix, index_name });
        }

        pub fn denseArtifactCounterBootstrapKeyAlloc(alloc: Allocator, index_name: []const u8) ![]u8 {
            return try std.fmt.allocPrint(alloc, "{s}{s}", .{ dense_artifact_counter_bootstrap_prefix, index_name });
        }

        pub fn encodeDenseArtifactCounterBootstrap(value: DenseArtifactCounterBootstrap, out: *[dense_artifact_counter_bootstrap_encoded_len]u8) void {
            @memcpy(out[0..dense_artifact_counter_bootstrap_magic.len], dense_artifact_counter_bootstrap_magic);
            var offset = dense_artifact_counter_bootstrap_magic.len;
            std.mem.writeInt(u128, out[offset..][0..@sizeOf(u128)], value.repair_id, .little);
            offset += @sizeOf(u128);
            std.mem.writeInt(u128, out[offset..][0..@sizeOf(u128)], value.attempt_id, .little);
            offset += @sizeOf(u128);
            std.mem.writeInt(i64, out[offset..][0..@sizeOf(i64)], value.delta, .little);
        }

        fn decodeDenseArtifactCounterBootstrap(raw: []const u8) !DenseArtifactCounterBootstrap {
            if (raw.len != dense_artifact_counter_bootstrap_encoded_len or
                !std.mem.eql(u8, raw[0..dense_artifact_counter_bootstrap_magic.len], dense_artifact_counter_bootstrap_magic))
            {
                return error.InvalidDenseArtifactCounterBootstrap;
            }
            var offset = dense_artifact_counter_bootstrap_magic.len;
            const repair_id = std.mem.readInt(u128, raw[offset..][0..@sizeOf(u128)], .little);
            offset += @sizeOf(u128);
            const attempt_id = std.mem.readInt(u128, raw[offset..][0..@sizeOf(u128)], .little);
            offset += @sizeOf(u128);
            return .{
                .repair_id = repair_id,
                .attempt_id = attempt_id,
                .delta = std.mem.readInt(i64, raw[offset..][0..@sizeOf(i64)], .little),
            };
        }

        pub fn loadDenseArtifactCounterBootstrap(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            index_name: []const u8,
        ) !?DenseArtifactCounterBootstrap {
            const key = try denseArtifactCounterBootstrapKeyAlloc(alloc, index_name);
            defer alloc.free(key);
            const raw = store.get(alloc, key) catch |err| switch (err) {
                error.NotFound => return null,
                else => return err,
            };
            defer alloc.free(raw);
            return try decodeDenseArtifactCounterBootstrap(raw);
        }

        pub fn loadDenseArtifactTargetCounter(alloc: Allocator, store: *docstore_mod.DocStore, index_name: []const u8) !?u64 {
            const key = try denseArtifactTargetCounterKeyAlloc(alloc, index_name);
            defer alloc.free(key);
            const raw = store.get(alloc, key) catch |err| switch (err) {
                error.NotFound => return null,
                else => return err,
            };
            defer alloc.free(raw);
            if (raw.len != 8) return error.InvalidDenseArtifactTargetCounter;
            return std.mem.readInt(u64, raw[0..8], .little);
        }

        pub fn initializeDenseArtifactTargetCounterIfNeeded(self: *DB, cfg: types.IndexConfig) !void {
            if (!try index_manager_mod.denseConfigRequiresArtifactCoverage(self.alloc, cfg)) return;

            // A newly admitted catalog generation must not inherit coverage
            // evidence from an interrupted deletion of the same index name.
            const key = try Self.denseArtifactTargetCounterKeyAlloc(self.alloc, cfg.name);
            defer self.alloc.free(key);
            var value: [8]u8 = undefined;
            std.mem.writeInt(u64, &value, 0, .little);
            try self.core.store.putBatch(&.{.{ .key = key, .value = &value }}, &.{});
        }

        pub fn deleteDenseArtifactCounterMetadataContext(alloc: Allocator, store: *docstore_mod.DocStore, index_name: []const u8) !void {
            const counter_key = try Self.denseArtifactTargetCounterKeyAlloc(alloc, index_name);
            defer alloc.free(counter_key);
            const bootstrap_key = try Self.denseArtifactCounterBootstrapKeyAlloc(alloc, index_name);
            defer alloc.free(bootstrap_key);
            try store.putBatch(&.{}, &.{ counter_key, bootstrap_key });
        }

        fn appendDenseArtifactTargetCounterWrite(
            alloc: Allocator,
            store_writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
            owned_keys: *std.ArrayListUnmanaged([]u8),
            owned_values: *std.ArrayListUnmanaged([]u8),
            index_name: []const u8,
            count: u64,
        ) !void {
            const key = try denseArtifactTargetCounterKeyAlloc(alloc, index_name);
            errdefer alloc.free(key);
            const value = try alloc.alloc(u8, 8);
            errdefer alloc.free(value);
            std.mem.writeInt(u64, value[0..8], count, .little);
            try owned_keys.append(alloc, key);
            try owned_values.append(alloc, value);
            try store_writes.append(alloc, .{
                .key = key,
                .value = value,
            });
        }

        fn appendDenseArtifactCounterBootstrapWrite(
            alloc: Allocator,
            store_writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
            owned_keys: *std.ArrayListUnmanaged([]u8),
            owned_values: *std.ArrayListUnmanaged([]u8),
            index_name: []const u8,
            bootstrap: DenseArtifactCounterBootstrap,
        ) !void {
            const key = try denseArtifactCounterBootstrapKeyAlloc(alloc, index_name);
            errdefer alloc.free(key);
            const value = try alloc.alloc(u8, dense_artifact_counter_bootstrap_encoded_len);
            errdefer alloc.free(value);
            encodeDenseArtifactCounterBootstrap(bootstrap, value[0..dense_artifact_counter_bootstrap_encoded_len]);
            try owned_keys.append(alloc, key);
            try owned_values.append(alloc, value);
            try store_writes.append(alloc, .{ .key = key, .value = value });
        }

        const PendingDenseArtifactCounterMutation = union(enum) {
            counter: u64,
            bootstrap: DenseArtifactCounterBootstrap,
            unavailable,
        };

        fn applyDenseArtifactCounterDelta(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            catalog: *const DenseArtifactCounterCatalog,
            mutations: *std.AutoHashMapUnmanaged(usize, PendingDenseArtifactCounterMutation),
            artifact_key: []const u8,
            artifact_value: ?[]const u8,
            delta: i64,
        ) !void {
            if (delta == 0) return;
            var identity = (artifact_ids.decodeEmbeddingArtifactIdentityAlloc(alloc, artifact_key) catch |err| switch (err) {
                error.InvalidInternalUserKey => return,
                else => return err,
            }) orelse return;
            defer identity.deinit(alloc);
            const value = artifact_value orelse return;
            const dims = enrichment_artifact_codec.decodeDenseEmbeddingDims(value) catch return;
            if (dims == 0) return;

            for (catalog.targetsFor(identity.embedding_name, dims)) |target_idx| {
                const target = &catalog.targets.items[target_idx];
                const gop = try mutations.getOrPut(alloc, target_idx);
                if (!gop.found_existing) {
                    if (try loadDenseArtifactTargetCounter(alloc, store, target.index_name)) |count| {
                        gop.value_ptr.* = .{ .counter = count };
                    } else if (try loadDenseArtifactCounterBootstrap(alloc, store, target.index_name)) |bootstrap| {
                        gop.value_ptr.* = .{ .bootstrap = bootstrap };
                    } else {
                        // A missing counter without an active bootstrap is metadata
                        // debt. Do not manufacture a partial counter from the
                        // next mutation; repair will establish an authoritative
                        // snapshot plus concurrent signed delta.
                        gop.value_ptr.* = .unavailable;
                    }
                }
                switch (gop.value_ptr.*) {
                    .counter => |*count| {
                        if (delta > 0) {
                            count.* +|= @as(u64, @intCast(delta));
                        } else {
                            count.* -|= @as(u64, @intCast(-delta));
                        }
                    },
                    .bootstrap => |*bootstrap| {
                        bootstrap.delta = std.math.add(i64, bootstrap.delta, delta) catch
                            return error.DenseArtifactCounterBootstrapOverflow;
                    },
                    .unavailable => {},
                }
            }
        }

        pub fn appendDenseArtifactCounterMutations(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            index_manager: *const index_manager_mod.IndexManager,
            store_writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
            delete_keys: []const []const u8,
            owned_keys: *std.ArrayListUnmanaged([]u8),
            owned_values: *std.ArrayListUnmanaged([]u8),
        ) !void {
            var may_mutate_embedding_artifact = false;
            for (delete_keys) |key| {
                if (internal_keys.isEmbeddingArtifactKey(key) or internal_keys.isDerivedEmbeddingArtifactKey(key)) {
                    may_mutate_embedding_artifact = true;
                    break;
                }
            }
            if (!may_mutate_embedding_artifact) {
                for (store_writes.items) |write| {
                    if (internal_keys.isEmbeddingArtifactKey(write.key) or internal_keys.isDerivedEmbeddingArtifactKey(write.key)) {
                        may_mutate_embedding_artifact = true;
                        break;
                    }
                }
            }
            // Ordinary document-only writes stay allocation-free in counter
            // routing. Build the catalog lookup only for commits that can actually
            // change embedding-artifact coverage.
            if (!may_mutate_embedding_artifact) return;

            var catalog = try DenseArtifactCounterCatalog.init(alloc, index_manager);
            defer catalog.deinit(alloc);
            if (catalog.targets.items.len == 0) return;
            var mutations = std.AutoHashMapUnmanaged(usize, PendingDenseArtifactCounterMutation){};
            defer mutations.deinit(alloc);
            const deleted_sentinel = std.math.maxInt(usize);
            var final_artifact_mutations = std.StringHashMapUnmanaged(usize).empty;
            defer final_artifact_mutations.deinit(alloc);

            for (delete_keys) |key| {
                if (!internal_keys.isEmbeddingArtifactKey(key) and !internal_keys.isDerivedEmbeddingArtifactKey(key)) continue;
                const gop = try final_artifact_mutations.getOrPut(alloc, key);
                if (!gop.found_existing) gop.value_ptr.* = deleted_sentinel;
            }
            for (store_writes.items, 0..) |write, write_idx| {
                if (!internal_keys.isEmbeddingArtifactKey(write.key) and !internal_keys.isDerivedEmbeddingArtifactKey(write.key)) continue;
                try final_artifact_mutations.put(alloc, write.key, write_idx);
            }

            var final_it = final_artifact_mutations.iterator();
            while (final_it.next()) |entry| {
                const artifact_key = entry.key_ptr.*;
                const old_value = store.get(alloc, artifact_key) catch |err| switch (err) {
                    error.NotFound => null,
                    else => return err,
                };
                defer if (old_value) |value| alloc.free(value);
                if (old_value) |value| {
                    try applyDenseArtifactCounterDelta(alloc, store, &catalog, &mutations, artifact_key, value, -1);
                }
                if (entry.value_ptr.* != deleted_sentinel) {
                    const write = store_writes.items[entry.value_ptr.*];
                    try applyDenseArtifactCounterDelta(alloc, store, &catalog, &mutations, write.key, write.value, 1);
                }
            }

            var it = mutations.iterator();
            while (it.next()) |entry| {
                const target = &catalog.targets.items[entry.key_ptr.*];
                switch (entry.value_ptr.*) {
                    .counter => |count| try appendDenseArtifactTargetCounterWrite(
                        alloc,
                        store_writes,
                        owned_keys,
                        owned_values,
                        target.index_name,
                        count,
                    ),
                    .bootstrap => |bootstrap| try appendDenseArtifactCounterBootstrapWrite(
                        alloc,
                        store_writes,
                        owned_keys,
                        owned_values,
                        target.index_name,
                        bootstrap,
                    ),
                    .unavailable => {},
                }
            }
        }

        fn collectDenseArtifactTargetCounts(
            self: *DB,
            alloc: Allocator,
            rebuild_targets: ?[]const DenseArtifactRebuildTarget,
        ) !DenseArtifactTargetCounts {
            var counts: DenseArtifactTargetCounts = .{};
            errdefer counts.deinit(alloc);

            var target_lookup: DenseArtifactTargetLookup = .{};
            defer target_lookup.deinit(alloc);

            if (rebuild_targets) |targets| {
                for (targets) |target| {
                    const entry = &self.core.index_manager.dense_indexes.items[target.dense_index_idx];
                    try counts.per_target_index.put(alloc, target.dense_index_idx, 0);
                    try target_lookup.add(alloc, entry.config.name, entry.dims, target.dense_index_idx);
                    if (entry.embedding_name) |embedding_name| {
                        if (!std.mem.eql(u8, embedding_name, entry.config.name)) {
                            try target_lookup.add(alloc, embedding_name, entry.dims, target.dense_index_idx);
                        }
                    }
                }
            } else {
                for (self.core.index_manager.dense_indexes.items, 0..) |*entry, dense_index_idx| {
                    if (!denseIndexIsArtifactBacked(entry)) continue;
                    try counts.per_target_index.put(alloc, dense_index_idx, 0);
                    try target_lookup.add(alloc, entry.config.name, entry.dims, dense_index_idx);
                    if (entry.embedding_name) |embedding_name| {
                        if (!std.mem.eql(u8, embedding_name, entry.config.name)) {
                            try target_lookup.add(alloc, embedding_name, entry.dims, dense_index_idx);
                        }
                    }
                }
            }

            if (target_lookup.by_artifact.count() == 0) return counts;

            const lower = try self.core.documentRangeLowerAlloc("");
            defer self.core.alloc.free(lower);

            const ScanState = struct {
                alloc: Allocator,
                counts: *DenseArtifactTargetCounts,
                target_lookup: *const DenseArtifactTargetLookup,

                fn scanEntry(scan_ctx: ?*anyopaque, key: []const u8, value: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
                    const state: *@This() = @ptrCast(@alignCast(scan_ctx orelse return error.InvalidArgument));
                    if (!internal_keys.isInternalUserKey(key)) return .@"continue";

                    var artifact_ref = (try db_internal.decodeArtifactRefIfKnownAlloc(state.alloc, key)) orelse return .@"continue";
                    defer artifact_ref.deinit(state.alloc);
                    if (artifact_ref.kind != .embedding) return .@"continue";

                    const dims = enrichment_artifact_codec.decodeDenseEmbeddingDims(value) catch |err| {
                        if (Self.isRecoverableEmbeddingArtifactError(err)) return .@"continue";
                        return err;
                    };
                    if (dims == 0) return .@"continue";

                    const lookup_key: DenseArtifactTargetKey = .{
                        .artifact_name = artifact_ref.name,
                        .dims = dims,
                    };
                    const indices = state.target_lookup.by_artifact.get(lookup_key) orelse return .@"continue";
                    for (indices.items) |dense_index_idx| {
                        const count = state.counts.per_target_index.getPtr(dense_index_idx).?;
                        count.* += 1;
                    }
                    state.counts.total_target_artifacts += 1;
                    return .@"continue";
                }
            };

            var state = ScanState{
                .alloc = alloc,
                .counts = &counts,
                .target_lookup = &target_lookup,
            };
            try self.core.store.scanWithContext(lower, "", .{}, &state, ScanState.scanEntry);
            return counts;
        }

        fn collectDenseArtifactTargetCountsFromCounters(
            self: *DB,
            alloc: Allocator,
            rebuild_targets: []const DenseArtifactRebuildTarget,
        ) !DenseArtifactTargetCounts {
            var counts: DenseArtifactTargetCounts = .{};
            errdefer counts.deinit(alloc);

            var source_counts = std.HashMapUnmanaged(DenseArtifactTargetKey, u64, DenseArtifactTargetKeyContext, 80).empty;
            defer source_counts.deinit(alloc);

            for (rebuild_targets) |target| {
                const entry = &self.core.index_manager.dense_indexes.items[target.dense_index_idx];
                // Startup planning and shadow activation must use the same
                // generation-fenced target after same-name recreation.
                const maybe_count = try denseTargetCountForIndexContext(self.async_context, entry.config.name);
                const count = maybe_count orelse 0;
                try counts.per_target_index.put(alloc, target.dense_index_idx, count);
                if (maybe_count != null) try counts.authoritative_targets.put(alloc, target.dense_index_idx, {});

                const source_key: DenseArtifactTargetKey = .{
                    .artifact_name = denseArtifactNameForEntry(entry),
                    .dims = entry.dims,
                };
                const gop = try source_counts.getOrPut(alloc, source_key);
                if (!gop.found_existing or count > gop.value_ptr.*) gop.value_ptr.* = count;
            }

            var source_it = source_counts.valueIterator();
            while (source_it.next()) |count| counts.total_target_artifacts +|= count.*;

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
                artifact_counter_required: bool,
                fallback_target_count: u64 = 0,
                invalid_generation_error: ?[]const u8 = null,

                fn deinit(candidate: *@This(), local_alloc: Allocator) void {
                    if (candidate.persisted_resume) |buf| local_alloc.free(buf);
                    candidate.* = .{
                        .dense_index_idx = 0,
                        .applied_sequence = 0,
                        .target_sequence = 0,
                        .artifact_counter_required = false,
                    };
                }
            };

            const recoverable_dense_integrity_errors = struct {
                fn check(err: anyerror) bool {
                    return err == error.NotFound or err == error.FileNotFound or err == error.Corrupted;
                }
            };

            var targets = std.ArrayListUnmanaged(DenseArtifactRebuildTarget).empty;
            errdefer {
                for (targets.items) |*target| target.deinit(alloc);
                targets.deinit(alloc);
            }

            var generation_repairs = std.ArrayListUnmanaged(DenseGenerationRepairTarget).empty;
            errdefer generation_repairs.deinit(alloc);

            var candidates = std.ArrayListUnmanaged(Candidate).empty;
            defer {
                for (candidates.items) |*candidate| candidate.deinit(alloc);
                candidates.deinit(alloc);
            }

            for (self.core.index_manager.dense_indexes.items, 0..) |*entry, dense_index_idx| {
                const artifact_backed = entry.external or entry.chunk_name != null or entry.embedding_name != null;
                const artifact_counter_required = try index_manager_mod.denseConfigRequiresArtifactCoverage(alloc, entry.config);
                const status_snapshot = try db_internal.loadIndexStatusSnapshot(alloc, self.core.store, entry.config.name);
                const watermark_count = if (status_snapshot) |status_value|
                    if (status_value.kind == .dense_vector) status_value.doc_count else 0
                else
                    0;
                const watermark_regressed = watermark_count > entry.index.stats().active_count;
                if (!artifact_backed and !watermark_regressed) continue;

                const rebuild_root_path = try denseIndexRebuildStatePathAlloc(self, alloc, entry.config.name);
                defer alloc.free(rebuild_root_path);
                const rebuild_state = self.core.index_manager.rebuildState(.dense_vector, rebuild_root_path, entry.config);
                const persisted_resume = try rebuild_state.checkWithIo(alloc, self.core.index_manager.checkpointIo());
                errdefer if (persisted_resume) |buf| alloc.free(buf);
                const projection_checkpoint = try self.core.loadProjectionCheckpoint(alloc, entry.config.name);
                const config_hash = types.indexConfigHash(entry.config);
                const checkpoint_config_mismatch = projection_checkpoint.config_hash != config_hash;
                const applied_sequence = projection_checkpoint.applied_sequence;
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
                if (persisted_resume == null and
                    !checkpoint_config_mismatch and
                    applied_sequence < target_sequence and
                    projection_checkpoint.status != .rebuilding and
                    projection_checkpoint.status != .repair_required)
                {
                    continue;
                }
                const invalid_generation_error: ?[]const u8 = blk: {
                    if (checkpoint_config_mismatch) break :blk "dense_projection_config_mismatch";
                    if (projection_checkpoint.status == .repair_required) break :blk "dense_projection_checkpoint_repair_required";
                    if (entry.index.stats().active_count == 0) break :blk null;
                    if (@hasDecl(@TypeOf(entry.index), "validateStoredStructure")) {
                        entry.index.validateStoredStructure(alloc) catch |err| {
                            if (recoverable_dense_integrity_errors.check(err)) break :blk "dense_projection_structure_invalid";
                            return err;
                        };
                    }
                    break :blk null;
                };
                try candidates.append(alloc, .{
                    .dense_index_idx = dense_index_idx,
                    .persisted_resume = persisted_resume,
                    .applied_sequence = applied_sequence,
                    .target_sequence = target_sequence,
                    .artifact_counter_required = artifact_counter_required,
                    .fallback_target_count = watermark_count,
                    .invalid_generation_error = invalid_generation_error,
                });
            }

            var candidate_targets = std.ArrayListUnmanaged(DenseArtifactRebuildTarget).empty;
            defer candidate_targets.deinit(alloc);
            for (candidates.items) |candidate| {
                try candidate_targets.append(alloc, .{ .dense_index_idx = candidate.dense_index_idx });
            }

            var target_counts = try collectDenseArtifactTargetCountsFromCounters(self, alloc, candidate_targets.items);
            defer target_counts.deinit(alloc);

            for (candidates.items) |*candidate| {
                const dense_index_idx = candidate.dense_index_idx;
                const entry = &self.core.index_manager.dense_indexes.items[dense_index_idx];
                const rebuild_root_path = try denseIndexRebuildStatePathAlloc(self, alloc, entry.config.name);
                defer alloc.free(rebuild_root_path);
                const rebuild_state = self.core.index_manager.rebuildState(.dense_vector, rebuild_root_path, entry.config);
                const active_count = entry.index.stats().active_count;
                if (candidate.invalid_generation_error) |last_error| {
                    try generation_repairs.append(alloc, .{
                        .dense_index_idx = dense_index_idx,
                        .trigger = .projection_generation_invalid,
                        .last_error = last_error,
                    });
                    if (candidate.persisted_resume != null) try rebuild_state.clearWithIo(self.core.index_manager.checkpointIo());
                    continue;
                }
                if (candidate.artifact_counter_required and
                    !target_counts.authoritative_targets.contains(dense_index_idx))
                {
                    try generation_repairs.append(alloc, .{
                        .dense_index_idx = dense_index_idx,
                        .trigger = .artifact_counter_missing,
                        .last_error = "dense_artifact_counter_missing",
                    });
                    if (candidate.persisted_resume != null) try rebuild_state.clearWithIo(self.core.index_manager.checkpointIo());
                    continue;
                }
                const artifact_target_count = if (target_counts.authoritative_targets.contains(dense_index_idx))
                    target_counts.per_target_index.get(dense_index_idx).?
                else
                    candidate.fallback_target_count;
                // Replay only upserts live keys, so surplus vectors and invalid
                // generations must be replaced through a separately published
                // shadow generation rather than reset in place.
                if (active_count > artifact_target_count) {
                    try generation_repairs.append(alloc, .{
                        .dense_index_idx = dense_index_idx,
                        .trigger = .artifact_coverage_mismatch,
                        .last_error = "dense_artifact_coverage_surplus",
                    });
                    if (candidate.persisted_resume != null) try rebuild_state.clearWithIo(self.core.index_manager.checkpointIo());
                    continue;
                }
                const already_repaired = denseCoverageMatchesTarget(active_count, artifact_target_count) and
                    candidate.applied_sequence >= candidate.target_sequence;

                if (candidate.persisted_resume) |buf| {
                    if (artifact_target_count == 0) {
                        try rebuild_state.clearWithIo(self.core.index_manager.checkpointIo());
                        continue;
                    }
                    if (already_repaired) {
                        try rebuild_state.clearWithIo(self.core.index_manager.checkpointIo());
                        continue;
                    }
                    try targets.append(alloc, .{
                        .dense_index_idx = dense_index_idx,
                        .resume_from = try alloc.dupe(u8, buf),
                        .artifact_target_count = artifact_target_count,
                    });
                    continue;
                }

                if (artifact_target_count == 0) continue;
                if (denseCoverageMatchesTarget(active_count, artifact_target_count)) continue;

                try targets.append(alloc, .{
                    .dense_index_idx = dense_index_idx,
                    .artifact_target_count = artifact_target_count,
                });
            }

            return .{
                .targets = try targets.toOwnedSlice(alloc),
                .generation_repairs = try generation_repairs.toOwnedSlice(alloc),
                .target_sequence = target_counts.total_target_artifacts,
            };
        }

        fn prepareDenseArtifactRebuildPlan(self: *DB, plan: DenseArtifactRebuildPlan) !void {
            for (plan.generation_repairs) |repair| {
                const entry = &self.core.index_manager.dense_indexes.items[repair.dense_index_idx];
                _ = DB.ArtifactRepairCallbacks.ensure_automatic_dense_generation_repair_intent(
                    self,
                    self.alloc,
                    entry.config,
                    repair.trigger,
                    repair.last_error,
                ) catch |err| switch (err) {
                    error.DurableIndexRepairStateUnavailable => fallback: {
                        // Embedded/Lite runtimes have no durable maintenance
                        // owner, but still use the same fenced shadow generation.
                        var cfg = try types.IndexConfig.clone(self.alloc, entry.config);
                        defer cfg.deinit(self.alloc);
                        if (repair.trigger == .artifact_counter_missing) {
                            const bootstrap_id = try index_repair_state.newRepairId(self.alloc);
                            try DB.ArtifactRepairCallbacks.ensure_dense_artifact_target_counter_for_repair(
                                self,
                                self.alloc,
                                cfg,
                                bootstrap_id,
                                null,
                            );
                        }
                        const rebuilt = try DB.ArtifactRepairCallbacks.rebuild_index_with_shadow_replacement(
                            self,
                            self.alloc,
                            cfg,
                            .{},
                            null,
                        );
                        if (rebuilt.yielded) return error.ShadowIndexCatchUpIncomplete;
                        break :fallback 0;
                    },
                    else => return err,
                };
            }
            for (plan.targets) |target| {
                const entry = &self.core.index_manager.dense_indexes.items[target.dense_index_idx];
                const checkpoint = try self.core.loadProjectionCheckpoint(self.alloc, entry.config.name);
                try self.core.saveProjectionCheckpoint(entry.config.name, .{
                    .applied_sequence = checkpoint.applied_sequence,
                    .status = .rebuilding,
                    .generation = checkpoint.generation,
                    .config_hash = types.indexConfigHash(entry.config),
                });
                const rebuild_root_path = try denseIndexRebuildStatePathAlloc(self, self.alloc, entry.config.name);
                defer self.alloc.free(rebuild_root_path);
                const rebuild_state = self.core.index_manager.rebuildState(.dense_vector, rebuild_root_path, entry.config);
                try rebuild_state.updateWithIo(self.core.index_manager.checkpointIo(), target.resume_from orelse "");
            }
        }

        fn finalizeDenseArtifactRebuildPlan(self: *DB, alloc: Allocator, plan: DenseArtifactRebuildPlan) !void {
            for (plan.targets) |target| {
                const entry = &self.core.index_manager.dense_indexes.items[target.dense_index_idx];
                const rebuild_root_path = try denseIndexRebuildStatePathAlloc(self, alloc, entry.config.name);
                defer alloc.free(rebuild_root_path);
                const rebuild_state = self.core.index_manager.rebuildState(.dense_vector, rebuild_root_path, entry.config);
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
                const repaired = denseCoverageMatchesTarget(entry.index.stats().active_count, target.artifact_target_count) and
                    applied_sequence >= target_sequence;
                if (repaired) {
                    try rebuild_state.clearWithIo(self.core.index_manager.checkpointIo());
                    const checkpoint = try self.core.loadProjectionCheckpoint(alloc, entry.config.name);
                    try self.core.saveProjectionCheckpoint(entry.config.name, .{
                        .applied_sequence = applied_sequence,
                        .status = .clean,
                        .generation = checkpoint.generation +| 1,
                        .config_hash = types.indexConfigHash(entry.config),
                    });
                }
            }
        }

        pub fn denseArtifactWatermarkRepairNeeded(self: *DB, alloc: Allocator) !bool {
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
                if (entry.index.stats().active_count != artifact_target_count) continue;

                const applied_sequence = try self.core.loadAppliedSequence(alloc, entry.config.name);
                const checkpoint = try self.core.loadProjectionCheckpoint(alloc, entry.config.name);
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
                const checkpoint_needs_finalization = checkpoint.status == .rebuilding or
                    checkpoint.applied_sequence < target_sequence;
                if (applied_sequence >= target_sequence and !checkpoint_needs_finalization) continue;

                repaired += 1;
                if (repair) {
                    if (applied_sequence < target_sequence) try self.core.saveAppliedSequence(entry.config.name, target_sequence);
                    const rebuild_root_path = try denseIndexRebuildStatePathAlloc(self, alloc, entry.config.name);
                    defer alloc.free(rebuild_root_path);
                    const rebuild_state = self.core.index_manager.rebuildState(.dense_vector, rebuild_root_path, entry.config);
                    try rebuild_state.clearWithIo(self.core.index_manager.checkpointIo());
                    try self.core.saveProjectionCheckpoint(entry.config.name, .{
                        .applied_sequence = target_sequence,
                        .status = .clean,
                        .generation = checkpoint.generation + @intFromBool(checkpoint.status == .rebuilding),
                        .config_hash = types.indexConfigHash(entry.config),
                    });
                }
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
            if (plan.targets.len > 0 or plan.generation_repairs.len > 0) return true;
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
            if (plan.targets.len == 0 and plan.generation_repairs.len == 0) {
                return try repairDenseArtifactAppliedSequencesIfCovered(self, alloc);
            }

            try prepareDenseArtifactRebuildPlan(self, plan);
            if (plan.targets.len == 0) return plan.generation_repairs.len;
            const ResumePersistCtx = struct {
                db: *DB,
                alloc: Allocator,
                targets: []DenseArtifactRebuildTarget,

                fn run(ctx: *anyopaque, last_key: []const u8) !void {
                    const persist: *@This() = @ptrCast(@alignCast(ctx));
                    for (persist.targets) |*target| {
                        if (target.resume_from) |resume_from| {
                            if (std.mem.order(u8, last_key, resume_from) != .gt) continue;
                        }
                        const entry = &persist.db.core.index_manager.dense_indexes.items[target.dense_index_idx];
                        const rebuild_root_path = try denseIndexRebuildStatePathAlloc(persist.db, persist.alloc, entry.config.name);
                        defer persist.alloc.free(rebuild_root_path);
                        const rebuild_state = persist.db.core.index_manager.rebuildState(.dense_vector, rebuild_root_path, entry.config);
                        try rebuild_state.updateWithIo(persist.db.core.index_manager.checkpointIo(), last_key);
                        const owned_key = try persist.alloc.dupe(u8, last_key);
                        errdefer persist.alloc.free(owned_key);
                        if (target.resume_from) |resume_from| persist.alloc.free(resume_from);
                        target.resume_from = owned_key;
                    }
                }
            };
            var persist_ctx = ResumePersistCtx{
                .db = self,
                .alloc = alloc,
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

        fn tryClaimDenseProjectionFinalizationLocked(ctx: *AsyncContext) bool {
            if (db_internal.asyncContextHasDenseSessionsOrWaiters(ctx) or
                ctx.dense_projection_finalizing.load(.acquire)) return false;
            ctx.dense_projection_finalization_requested = false;
            ctx.dense_projection_finalizing.store(true, .release);
            return true;
        }

        fn finishDenseProjectionFinalization(ctx: *AsyncContext) void {
            var session_lock = lockAtomicWithBackoffProfiled(&ctx.dense_finish_mutex, &ctx.stats.dense_finish_mutex);
            ctx.dense_projection_finalizing.store(false, .release);
            session_lock.unlock();
        }

        pub fn finishDenseCatchUpSessionTrackedAndFinalize(ctx: *AsyncContext, index_name: []const u8) !bool {
            var finished = false;
            errdefer if (finished) db_internal.resumeDeferredBackgroundMaintenanceIfIdle(ctx);
            const completed = blk: {
                ctx.apply_mutex.lockShared();
                defer ctx.apply_mutex.unlockShared();
                var session_lock = lockAtomicWithBackoffProfiled(&ctx.dense_finish_mutex, &ctx.stats.dense_finish_mutex);
                finished = db_internal.finishDenseCatchUpSessionLocked(ctx, index_name);
                const claimed = finished and tryClaimDenseProjectionFinalizationLocked(ctx);
                session_lock.unlock();
                if (!claimed) break :blk false;
                errdefer finishDenseProjectionFinalization(ctx);
                break :blk try drainClaimedDenseProjectionFinalizations(ctx);
            };
            if (finished) db_internal.resumeDeferredBackgroundMaintenanceIfIdle(ctx);
            return completed;
        }

        pub fn finishDenseCatchUpSessionTrackedBestEffort(ctx: *AsyncContext, index_name: []const u8) void {
            const completed = finishDenseCatchUpSessionTrackedAndFinalize(ctx, index_name) catch |err| {
                std.log.err("dense catch-up idle finalization failed index={s} error={s}", .{ index_name, @errorName(err) });
                DB.notifyQueryVisibilityHook(ctx, .index_repair_pending);
                return;
            };
            if (completed) DB.notifyQueryVisibilityHook(ctx, .publish_blocking);
        }

        pub fn beginExternalDenseBulkSessionTrackedWait(ctx: *AsyncContext, io: ?std.Io) !void {
            const wait_start_ns = monotonicTimeNs();
            const wait_timeout_ns = 30 * std.time.ns_per_s;
            {
                var session_lock = lockAtomicWithBackoffProfiled(&ctx.dense_finish_mutex, &ctx.stats.dense_finish_mutex);
                defer session_lock.unlock();
                _ = ctx.waiting_external_dense_bulk_sessions.fetchAdd(1, .release);
            }
            var admitted = false;
            defer {
                if (!admitted) {
                    var session_lock = lockAtomicWithBackoffProfiled(&ctx.dense_finish_mutex, &ctx.stats.dense_finish_mutex);
                    _ = ctx.waiting_external_dense_bulk_sessions.fetchSub(1, .release);
                    session_lock.unlock();

                    const completed = finalizeCoveredDenseProjectionCheckpointsIfIdle(ctx) catch |err| blk: {
                        std.log.err("dense external bulk admission cleanup failed error={s}", .{@errorName(err)});
                        DB.notifyQueryVisibilityHook(ctx, .index_repair_pending);
                        break :blk false;
                    };
                    if (completed) DB.notifyQueryVisibilityHook(ctx, .publish_blocking);
                    db_internal.resumeDeferredBackgroundMaintenanceIfIdle(ctx);
                }
            }

            var wait_ms: u64 = 1;
            while (true) {
                var session_lock = lockAtomicWithBackoffProfiled(&ctx.dense_finish_mutex, &ctx.stats.dense_finish_mutex);
                if (ctx.active_dense_catch_up_sessions.load(.acquire) == 0 and
                    !ctx.dense_projection_finalizing.load(.acquire))
                {
                    _ = ctx.waiting_external_dense_bulk_sessions.fetchSub(1, .release);
                    ctx.text_merge_deferred.store(true, .release);
                    _ = ctx.active_external_dense_bulk_sessions.fetchAdd(1, .release);
                    admitted = true;
                    session_lock.unlock();
                    return;
                }
                session_lock.unlock();
                if (elapsedSince(wait_start_ns) >= wait_timeout_ns) return error.WriterLocked;
                if (io) |runtime_io| {
                    runtime_io.sleep(std.Io.Duration.fromMilliseconds(@intCast(wait_ms)), .awake) catch return error.WriterLocked;
                } else if (comptime builtin.os.tag == .freestanding) {
                    return error.WriterLocked;
                } else {
                    platform.time.sleepNs(wait_ms * std.time.ns_per_ms);
                }
                wait_ms = @min(wait_ms * 2, 16);
            }
        }

        pub fn finishExternalDenseBulkSessionTrackedAndFinalize(ctx: *AsyncContext) !bool {
            var finished = false;
            errdefer if (finished) db_internal.resumeDeferredBackgroundMaintenanceIfIdle(ctx);
            const completed = blk: {
                ctx.apply_mutex.lockShared();
                defer ctx.apply_mutex.unlockShared();
                var session_lock = lockAtomicWithBackoffProfiled(&ctx.dense_finish_mutex, &ctx.stats.dense_finish_mutex);
                finished = db_internal.finishExternalDenseBulkSessionLocked(ctx);
                const claimed = finished and tryClaimDenseProjectionFinalizationLocked(ctx);
                session_lock.unlock();
                if (!claimed) break :blk false;
                errdefer finishDenseProjectionFinalization(ctx);
                break :blk try drainClaimedDenseProjectionFinalizations(ctx);
            };
            if (finished) db_internal.resumeDeferredBackgroundMaintenanceIfIdle(ctx);
            return completed;
        }

        pub fn finishExternalDenseBulkSessionTrackedBestEffort(ctx: *AsyncContext) void {
            const completed = finishExternalDenseBulkSessionTrackedAndFinalize(ctx) catch |err| {
                std.log.err("dense external bulk idle finalization failed error={s}", .{@errorName(err)});
                DB.notifyQueryVisibilityHook(ctx, .index_repair_pending);
                return;
            };
            if (completed) DB.notifyQueryVisibilityHook(ctx, .publish_blocking);
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

        pub fn finalizeCoveredDenseProjectionCheckpoint(
            ctx: *AsyncContext,
            index_name: []const u8,
            applied_sequence: u64,
        ) !bool {
            var session_lock = lockAtomicWithBackoffProfiled(&ctx.dense_finish_mutex, &ctx.stats.dense_finish_mutex);
            const checkpoint = ctx.index_manager.denseProjectionCheckpointMetadata(index_name) orelse {
                session_lock.unlock();
                return false;
            };
            if (checkpoint.status != .rebuilding) {
                session_lock.unlock();
                return false;
            }
            if (!tryClaimDenseProjectionFinalizationLocked(ctx)) {
                queueDenseProjectionFinalizationLocked(ctx, index_name, applied_sequence) catch |err| {
                    session_lock.unlock();
                    return err;
                };
                session_lock.unlock();
                return false;
            }
            session_lock.unlock();
            errdefer finishDenseProjectionFinalization(ctx);

            const finalized = try finalizeCoveredDenseProjectionCheckpointClaimed(ctx, index_name, applied_sequence);
            if (finalized) clearPendingDenseProjectionFinalization(ctx, index_name);
            return try drainClaimedDenseProjectionFinalizations(ctx) or finalized;
        }

        fn queueDenseProjectionFinalizationLocked(
            ctx: *AsyncContext,
            index_name: []const u8,
            applied_sequence: u64,
        ) !void {
            ctx.dense_projection_finalization_requested = true;
            if (ctx.pending_dense_projection_finalizations.getPtr(index_name)) |pending_sequence| {
                pending_sequence.* = @max(pending_sequence.*, applied_sequence);
                return;
            }
            const owned_name = try ctx.alloc.dupe(u8, index_name);
            errdefer ctx.alloc.free(owned_name);
            try ctx.pending_dense_projection_finalizations.putNoClobber(ctx.alloc, owned_name, applied_sequence);
        }

        fn pendingDenseProjectionFinalizationSequence(ctx: *AsyncContext, index_name: []const u8) u64 {
            var session_lock = lockAtomicWithBackoffProfiled(&ctx.dense_finish_mutex, &ctx.stats.dense_finish_mutex);
            defer session_lock.unlock();
            return ctx.pending_dense_projection_finalizations.get(index_name) orelse 0;
        }

        fn clearPendingDenseProjectionFinalization(ctx: *AsyncContext, index_name: []const u8) void {
            var session_lock = lockAtomicWithBackoffProfiled(&ctx.dense_finish_mutex, &ctx.stats.dense_finish_mutex);
            defer session_lock.unlock();
            if (ctx.pending_dense_projection_finalizations.fetchRemove(index_name)) |removed|
                ctx.alloc.free(@constCast(removed.key));
        }

        fn finalizeCoveredDenseProjectionCheckpointClaimed(
            ctx: *AsyncContext,
            index_name: []const u8,
            applied_sequence: u64,
        ) !bool {
            const checkpoint = ctx.index_manager.denseProjectionCheckpointMetadata(index_name) orelse return false;
            if (checkpoint.status != .rebuilding) return false;

            const expected_count = (try denseTargetCountForIndexContext(ctx, index_name)) orelse return false;
            const entry = ctx.index_manager.denseIndex(index_name) orelse return false;
            if (entry.index.stats().active_count != expected_count) return false;

            const clean_checkpoint: apply_state.ProjectionCheckpoint = .{
                .applied_sequence = applied_sequence,
                .status = .clean,
                .generation = checkpoint.generation +| 1,
                .config_hash = checkpoint.config_hash,
            };
            try ctx.index_manager.saveDenseProjectionCheckpointMetadata(index_name, clean_checkpoint);
            try apply_state.saveProjectionCheckpointWithSidecar(
                ctx.alloc,
                ctx.index_manager.checkpointIo(),
                ctx.store,
                ctx.applied_sequence_checkpoint_path,
                index_name,
                clean_checkpoint,
            );
            return true;
        }

        pub fn finalizeCoveredDenseProjectionCheckpointsIfIdle(ctx: *AsyncContext) !bool {
            ctx.apply_mutex.lockShared();
            defer ctx.apply_mutex.unlockShared();

            var session_lock = lockAtomicWithBackoffProfiled(&ctx.dense_finish_mutex, &ctx.stats.dense_finish_mutex);
            const claimed = tryClaimDenseProjectionFinalizationLocked(ctx);
            session_lock.unlock();
            if (!claimed) return false;
            errdefer finishDenseProjectionFinalization(ctx);

            return drainClaimedDenseProjectionFinalizations(ctx);
        }

        pub fn drainClaimedDenseProjectionFinalizations(ctx: *AsyncContext) !bool {
            var completed = false;
            while (true) {
                completed = try finalizeCoveredDenseProjectionCheckpointsClaimed(ctx) or completed;

                var session_lock = lockAtomicWithBackoffProfiled(&ctx.dense_finish_mutex, &ctx.stats.dense_finish_mutex);
                if (!ctx.dense_projection_finalization_requested) {
                    ctx.dense_projection_finalizing.store(false, .release);
                    session_lock.unlock();
                    return completed;
                }
                ctx.dense_projection_finalization_requested = false;
                session_lock.unlock();
            }
        }

        fn finalizeCoveredDenseProjectionCheckpointsClaimed(ctx: *AsyncContext) !bool {
            // Pending work is only a wake-up optimization. Remove entries whose
            // catalog generation disappeared or completed through another path so a
            // long-running process cannot retain obsolete index names indefinitely.
            {
                var session_lock = lockAtomicWithBackoffProfiled(&ctx.dense_finish_mutex, &ctx.stats.dense_finish_mutex);
                defer session_lock.unlock();
                if (ctx.pending_dense_projection_finalizations.count() != 0) {
                    const names = try ctx.alloc.alloc([]const u8, ctx.pending_dense_projection_finalizations.count());
                    defer ctx.alloc.free(names);
                    var count: usize = 0;
                    var it = ctx.pending_dense_projection_finalizations.keyIterator();
                    while (it.next()) |name| : (count += 1) names[count] = name.*;
                    for (names[0..count]) |index_name| {
                        const checkpoint = ctx.index_manager.denseProjectionCheckpointMetadata(index_name);
                        if (checkpoint != null and checkpoint.?.status == .rebuilding) continue;
                        const removed = ctx.pending_dense_projection_finalizations.fetchRemove(index_name) orelse continue;
                        ctx.alloc.free(@constCast(removed.key));
                    }
                }
            }

            var completed = false;
            for (ctx.index_manager.dense_indexes.items) |*entry| {
                const index_name = entry.config.name;
                const checkpoint = ctx.index_manager.denseProjectionCheckpointMetadata(index_name) orelse {
                    continue;
                };
                if (checkpoint.status != .rebuilding) continue;
                const finalized = try finalizeCoveredDenseProjectionCheckpointClaimed(
                    ctx,
                    index_name,
                    @max(checkpoint.applied_sequence, pendingDenseProjectionFinalizationSequence(ctx, index_name)),
                );
                completed = finalized or completed;
                if (finalized) clearPendingDenseProjectionFinalization(ctx, index_name);
            }
            return completed;
        }

        fn flushFinishedDenseAppliedSequenceLocked(
            ctx: *AsyncContext,
            index_name: []const u8,
            lifecycle_completed: *bool,
        ) !bool {
            const pending = ctx.applied_sequence_coalescer.takePending(index_name) orelse return false;
            defer ctx.alloc.free(pending.owned_name);

            const flush_start_ns = monotonicTimeNs();
            const save_start_ns = monotonicTimeNs();
            ctx.apply_mutex.lockShared();
            defer ctx.apply_mutex.unlockShared();
            const raw_update = [_]apply_state.AppliedSequenceUpdate{.{
                .index_name = pending.owned_name,
                .sequence = pending.sequence,
            }};
            const updates = try appliedSequenceUpdatesWithConfigHashes(ctx.alloc, ctx.index_manager, &raw_update);
            defer ctx.alloc.free(updates);
            try saveDenseProjectionMetadataForAppliedSequenceUpdates(ctx.index_manager, updates);
            try checkpointManagedProjectionEffectsForAppliedSequenceUpdates(ctx.index_manager, updates);
            try apply_state.saveAppliedSequencesWithCheckpoint(
                ctx.alloc,
                ctx.index_manager.checkpointIo(),
                ctx.store,
                ctx.applied_sequence_checkpoint_path,
                updates,
            );
            lifecycle_completed.* = try finalizeCoveredDenseProjectionCheckpoint(ctx, pending.owned_name, pending.sequence) or lifecycle_completed.*;
            try DB.DerivedAsyncCallbacks.save_index_status_snapshots(ctx.alloc, ctx.store, ctx.index_manager, updates);
            const save_ns = elapsedSince(save_start_ns);

            ctx.applied_sequence_coalescer.last_flush_ns = monotonicTimeNs();
            const flush_ns = elapsedSince(flush_start_ns);
            _ = ctx.stats.applied_sequence.flush_calls.fetchAdd(1, .monotonic);
            _ = ctx.stats.applied_sequence.flushed_indexes.fetchAdd(1, .monotonic);
            _ = ctx.stats.applied_sequence.save_ns.fetchAdd(save_ns, .monotonic);
            _ = ctx.stats.applied_sequence.flush_ns.fetchAdd(flush_ns, .monotonic);
            db_internal.atomicMaxU64(&ctx.stats.applied_sequence.max_flush_ns, flush_ns);
            return true;
        }

        fn flushPendingAppliedSequencesLocked(
            ctx: *AsyncContext,
            force: bool,
            lifecycle_completed: *bool,
        ) !bool {
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
            ctx.apply_mutex.lockShared();
            defer ctx.apply_mutex.unlockShared();
            const enriched_updates = try appliedSequenceUpdatesWithConfigHashes(ctx.alloc, ctx.index_manager, updates.items);
            defer ctx.alloc.free(enriched_updates);
            try saveDenseProjectionMetadataForAppliedSequenceUpdates(ctx.index_manager, enriched_updates);
            try checkpointManagedProjectionEffectsForAppliedSequenceUpdates(ctx.index_manager, enriched_updates);
            try apply_state.saveAppliedSequencesWithCheckpoint(
                ctx.alloc,
                ctx.index_manager.checkpointIo(),
                ctx.store,
                ctx.applied_sequence_checkpoint_path,
                enriched_updates,
            );
            for (enriched_updates) |update| {
                lifecycle_completed.* = try finalizeCoveredDenseProjectionCheckpoint(ctx, update.index_name, update.sequence) or lifecycle_completed.*;
            }
            try DB.DerivedAsyncCallbacks.save_index_status_snapshots(ctx.alloc, ctx.store, ctx.index_manager, enriched_updates);
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

        pub fn denseIndexIsArtifactBacked(entry: anytype) bool {
            return entry.external or entry.chunk_name != null or entry.embedding_name != null;
        }

        pub fn denseCoverageMatchesTarget(active_count: u64, expected_count: u64) bool {
            return active_count == expected_count;
        }

        pub fn denseTargetCountForIndexContext(ctx: anytype, index_name: []const u8) !?u64 {
            // Inline external vectors have no generated-enrichment incarnation,
            // while one source document can produce multiple chunk-backed vectors.
            // Their durable artifact counter remains authoritative.
            if (ctx.index_manager.denseIndex(index_name)) |entry| {
                if (entry.external or entry.chunk_name != null) {
                    return try loadDenseArtifactTargetCounter(ctx.alloc, ctx.store, index_name);
                }
            }
            const generation = ctx.index_manager.coverageGenerationForIndex(index_name) orelse return null;
            const produced = try loadDerivedCoverageOutcomeCounterFromStore(ctx.alloc, ctx.store, index_name, generation, "produced");
            const skipped = try loadDerivedCoverageOutcomeCounterFromStore(ctx.alloc, ctx.store, index_name, generation, "skipped");
            const terminal_failed = try loadDerivedCoverageOutcomeCounterFromStore(ctx.alloc, ctx.store, index_name, generation, "terminal_failed");
            const present_count: u2 = @as(u2, @intFromBool(produced != null)) +
                @as(u2, @intFromBool(skipped != null)) +
                @as(u2, @intFromBool(terminal_failed != null));
            if (present_count == 0) {
                // Name-scoped artifact counters predate generation-scoped outcomes
                // and remain a bootstrap fallback before a fresh incarnation
                // processes its first source. Once any outcome exists, it is the
                // only counter fenced to the current catalog generation.
                const requires_artifact_coverage = if (ctx.index_manager.get(index_name)) |cfg|
                    cfg.kind == .dense_vector and try index_manager_mod.denseConfigRequiresArtifactCoverage(ctx.alloc, cfg.*)
                else if (ctx.index_manager.denseIndex(index_name)) |entry|
                    denseIndexIsArtifactBacked(entry)
                else
                    false;
                if (requires_artifact_coverage) {
                    return try loadDenseArtifactTargetCounter(ctx.alloc, ctx.store, index_name);
                }
                // A fresh generation on an empty table has no outcome rows to
                // create the counter tuple. Range cardinality distinguishes that
                // valid zero target from missing accounting on a non-empty range.
                const source_count = try range_cardinality.loadOrCount(
                    ctx.alloc,
                    ctx.store,
                    ctx.index_manager.byte_range,
                );
                return if (source_count == 0) 0 else null;
            }
            if (present_count != 3) return error.InvalidDerivedCoverageCounter;

            // Publishing is fail-closed until every source document owned by this
            // range has exactly one terminal outcome for this catalog generation.
            const accounted_without_failures = std.math.add(u64, produced.?, skipped.?) catch
                return error.InvalidDerivedCoverageCounter;
            const accounted = std.math.add(u64, accounted_without_failures, terminal_failed.?) catch
                return error.InvalidDerivedCoverageCounter;
            const source_count = try range_cardinality.loadOrCount(
                ctx.alloc,
                ctx.store,
                ctx.index_manager.byte_range,
            );
            if (accounted != source_count) return null;
            return produced.?;
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

        fn freePrimaryVectorRebuildWrites(alloc: Allocator, writes: *std.ArrayListUnmanaged(types.BatchWrite)) void {
            for (writes.items) |write| {
                alloc.free(@constCast(write.key));
                alloc.free(@constCast(write.value));
            }
            writes.clearRetainingCapacity();
        }

        fn scanStoreForRebuildContext(
            ctx: *AsyncContext,
            lower: []const u8,
            upper: []const u8,
            options: docstore_mod.DocStore.ScanOptions,
            scan_ctx: ?*anyopaque,
            callback: docstore_mod.DocStore.ScanWithContextCallback,
        ) !void {
            if (ctx.snapshot_read_txn) |txn| {
                return try ctx.store.scanReadTxnWithContext(txn, lower, upper, options, scan_ctx, callback);
            }
            return try ctx.store.scanWithContext(lower, upper, options, scan_ctx, callback);
        }

        fn denseRepairFinishAdmission(ptr: *anyopaque) !void {
            const ctx: *AsyncContext = @ptrCast(@alignCast(ptr));
            try db_internal.checkAsyncRepairCapacityBoundary(ctx);
            try db_internal.checkAsyncRepairCancelled(ctx);
        }

        fn denseRepairFinishOptions(ctx: *AsyncContext) backend_types.BulkIngestFinishOptions {
            var options = denseCatchUpFinishOptions();
            options.admission_ctx = ctx;
            options.admission_fn = denseRepairFinishAdmission;
            return options;
        }

        fn flushDensePrimaryVectorRebuildChunkContext(
            ctx: anytype,
            index_name: []const u8,
            writes: *std.ArrayListUnmanaged(types.BatchWrite),
        ) !void {
            defer freePrimaryVectorRebuildWrites(ctx.alloc, writes);
            if (writes.items.len == 0) return;
            try db_internal.checkAsyncRepairCancelled(ctx);
            try db_internal.checkAsyncRepairCapacityBoundary(ctx);
            try ctx.index_manager.indexDenseBatchByNameWithOptions(
                ctx.store,
                index_name,
                writes.items,
                .{ .mode = .bulk_ingest },
            );
        }

        pub const DenseRebuildSliceResult = struct {
            rebuilt: usize = 0,
            /// Owned source-store key. A non-null value means the candidate was
            /// durably closed at a cooperative yield boundary and the next pass
            /// must resume strictly after this key.
            resume_key: ?[]u8 = null,

            pub fn deinit(self: *@This(), alloc: Allocator) void {
                if (self.resume_key) |key| alloc.free(key);
                self.* = undefined;
            }

            pub fn complete(self: @This()) bool {
                return self.resume_key == null;
            }
        };

        fn denseRepairYieldRequested(ctx: *AsyncContext) bool {
            if (ctx.repair_options.yield_check) |check| return check.requested();
            return false;
        }

        fn rebuildDenseIndexFromPrimaryVectorsContext(
            ctx: anytype,
            index_name: []const u8,
            rebuild_chunk_size: usize,
        ) !usize {
            var result = try rebuildDenseIndexFromPrimaryVectorsSliceContext(ctx, index_name, rebuild_chunk_size, null);
            defer result.deinit(ctx.alloc);
            std.debug.assert(result.complete());
            return result.rebuilt;
        }

        fn rebuildDenseIndexFromPrimaryVectorsSliceContext(
            ctx: *AsyncContext,
            index_name: []const u8,
            rebuild_chunk_size: usize,
            resume_key: ?[]const u8,
        ) !DenseRebuildSliceResult {
            const entry = ctx.index_manager.denseIndex(index_name) orelse return .{};
            const field_name = entry.field_name;
            const dims = entry.dims;

            const lower = if (resume_key) |key|
                try ctx.alloc.dupe(u8, key)
            else
                try internal_keys.documentRangeLowerAlloc(ctx.alloc, "");
            defer ctx.alloc.free(lower);

            const resumable = ctx.repair_options.yield_check != null;
            if (resumable) {
                try ctx.index_manager.beginDenseStreamingReplaySessionByName(index_name);
            } else {
                try ctx.index_manager.beginDenseBulkIngestSessionByName(index_name);
            }
            var session_open = true;
            errdefer if (session_open) {
                if (resumable) {
                    ctx.index_manager.abortDenseStreamingReplaySessionByName(index_name);
                } else {
                    ctx.index_manager.abortDenseBulkIngestSessionByName(index_name);
                }
            };

            const ScanState = struct {
                ctx: *AsyncContext,
                index_name: []const u8,
                field_name: []const u8,
                dims: u32,
                rebuild_chunk_size: usize,
                writes: std.ArrayListUnmanaged(types.BatchWrite) = .empty,
                rebuilt: usize = 0,
                scanned_since_yield_check: usize = 0,
                resume_key: ?[]u8 = null,

                fn deinit(state: *@This()) void {
                    Self.freePrimaryVectorRebuildWrites(state.ctx.alloc, &state.writes);
                    state.writes.deinit(state.ctx.alloc);
                    if (state.resume_key) |key| state.ctx.alloc.free(key);
                }

                fn flush(state: *@This()) !void {
                    try Self.flushDensePrimaryVectorRebuildChunkContext(state.ctx, state.index_name, &state.writes);
                }

                fn yieldAfter(state: *@This(), key: []const u8) !docstore_mod.DocStore.ScanAction {
                    if (!Self.denseRepairYieldRequested(state.ctx)) return .@"continue";
                    try state.flush();
                    state.resume_key = try state.ctx.alloc.dupe(u8, key);
                    return .stop;
                }

                fn scanEntry(scan_ctx: ?*anyopaque, key: []const u8, value: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
                    const state: *@This() = @ptrCast(@alignCast(scan_ctx orelse return error.InvalidArgument));
                    state.scanned_since_yield_check += 1;
                    if (!db_internal.isBaseDocumentStoreKeyForMode(state.ctx.relational_base_rows, key)) {
                        if (state.scanned_since_yield_check >= 1024) {
                            state.scanned_since_yield_check = 0;
                            return try state.yieldAfter(key);
                        }
                        return .@"continue";
                    }
                    try db_internal.checkAsyncRepairCancelled(state.ctx);
                    const doc_value = if (state.ctx.relational_base_rows)
                        try mapper.materializeRelationalRowValueAlloc(state.ctx.alloc, value)
                    else
                        try mapper.materializeDocumentValueAlloc(state.ctx.alloc, value);
                    errdefer state.ctx.alloc.free(doc_value);
                    if (try mapper.extractDenseVectorField(state.ctx.alloc, doc_value, state.field_name, state.dims)) |vector| {
                        state.ctx.alloc.free(vector);
                    } else {
                        state.ctx.alloc.free(doc_value);
                        if (state.scanned_since_yield_check >= 1024) {
                            state.scanned_since_yield_check = 0;
                            return try state.yieldAfter(key);
                        }
                        return .@"continue";
                    }
                    const doc_key = (try internal_keys.decodeStoredDocumentRowKeyAlloc(state.ctx.alloc, key)) orelse {
                        state.ctx.alloc.free(doc_value);
                        if (state.scanned_since_yield_check >= 1024) {
                            state.scanned_since_yield_check = 0;
                            return try state.yieldAfter(key);
                        }
                        return .@"continue";
                    };
                    errdefer state.ctx.alloc.free(doc_key);
                    try state.writes.append(state.ctx.alloc, .{
                        .key = doc_key,
                        .value = doc_value,
                    });
                    state.rebuilt += 1;

                    if (state.writes.items.len >= state.rebuild_chunk_size) {
                        try state.flush();
                        state.scanned_since_yield_check = 0;
                        return try state.yieldAfter(key);
                    }
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

            try scanStoreForRebuildContext(ctx, lower, "", .{ .lower_exclusive = resume_key != null }, &state, ScanState.scanEntry);
            if (state.writes.items.len > 0) try state.flush();

            if (resumable) {
                try ctx.index_manager.finishDenseStreamingReplaySessionByNameWithOptions(index_name, denseRepairFinishOptions(ctx));
            } else {
                try ctx.index_manager.finishDenseBulkIngestSessionByNameWithOptions(index_name, denseRepairFinishOptions(ctx));
            }
            session_open = false;
            const owned_resume_key = state.resume_key;
            state.resume_key = null;
            return .{ .rebuilt = state.rebuilt, .resume_key = owned_resume_key };
        }

        fn flushDenseArtifactRebuildChunkContext(
            ctx: anytype,
            index_name: []const u8,
            writes: *std.ArrayListUnmanaged(mapper.DenseEmbeddingWrite),
        ) !void {
            defer Self.freeDenseArtifactRebuildWrites(ctx.alloc, writes);
            if (writes.items.len == 0) return;
            try db_internal.checkAsyncRepairCancelled(ctx);
            try db_internal.checkAsyncRepairCapacityBoundary(ctx);
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
            var result = try rebuildDenseIndexFromStoredEmbeddingArtifactsSliceContext(ctx, index_name, rebuild_chunk_size, null);
            defer result.deinit(ctx.alloc);
            std.debug.assert(result.complete());
            return result.rebuilt;
        }

        fn rebuildDenseIndexFromStoredEmbeddingArtifactsSliceContext(
            ctx: *AsyncContext,
            index_name: []const u8,
            rebuild_chunk_size: usize,
            resume_key: ?[]const u8,
        ) !DenseRebuildSliceResult {
            const entry = ctx.index_manager.denseIndex(index_name) orelse return .{};
            const expected_name = denseArtifactNameForEntry(entry);
            const expected_dims = entry.dims;

            const lower = if (resume_key) |key|
                try ctx.alloc.dupe(u8, key)
            else
                try internal_keys.documentRangeLowerAlloc(ctx.alloc, "");
            defer ctx.alloc.free(lower);

            const resumable = ctx.repair_options.yield_check != null;
            if (resumable) {
                try ctx.index_manager.beginDenseStreamingReplaySessionByName(index_name);
            } else {
                try ctx.index_manager.beginDenseBulkIngestSessionByName(index_name);
            }
            var session_open = true;
            errdefer if (session_open) {
                if (resumable) {
                    ctx.index_manager.abortDenseStreamingReplaySessionByName(index_name);
                } else {
                    ctx.index_manager.abortDenseBulkIngestSessionByName(index_name);
                }
            };

            const ScanState = struct {
                ctx: *AsyncContext,
                index_name: []const u8,
                expected_name: []const u8,
                expected_dims: u32,
                rebuild_chunk_size: usize,
                writes: std.ArrayListUnmanaged(mapper.DenseEmbeddingWrite) = .empty,
                rebuilt: usize = 0,
                scanned_since_yield_check: usize = 0,
                resume_key: ?[]u8 = null,

                fn deinit(state: *@This()) void {
                    Self.freeDenseArtifactRebuildWrites(state.ctx.alloc, &state.writes);
                    state.writes.deinit(state.ctx.alloc);
                    if (state.resume_key) |key| state.ctx.alloc.free(key);
                }

                fn flush(state: *@This()) !void {
                    try Self.flushDenseArtifactRebuildChunkContext(state.ctx, state.index_name, &state.writes);
                }

                fn yieldAfter(state: *@This(), key: []const u8) !docstore_mod.DocStore.ScanAction {
                    if (!Self.denseRepairYieldRequested(state.ctx)) return .@"continue";
                    try state.flush();
                    state.resume_key = try state.ctx.alloc.dupe(u8, key);
                    return .stop;
                }

                fn scanEntry(scan_ctx: ?*anyopaque, key: []const u8, value: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
                    const state: *@This() = @ptrCast(@alignCast(scan_ctx orelse return error.InvalidArgument));
                    state.scanned_since_yield_check += 1;
                    if (!internal_keys.isInternalUserKey(key)) {
                        if (state.scanned_since_yield_check >= 1024) {
                            state.scanned_since_yield_check = 0;
                            return try state.yieldAfter(key);
                        }
                        return .@"continue";
                    }
                    try db_internal.checkAsyncRepairCancelled(state.ctx);

                    const dims = enrichment_artifact_codec.decodeDenseEmbeddingDims(value) catch |err| {
                        if (Self.isRecoverableEmbeddingArtifactError(err)) {
                            if (state.scanned_since_yield_check >= 1024) {
                                state.scanned_since_yield_check = 0;
                                return try state.yieldAfter(key);
                            }
                            return .@"continue";
                        }
                        return err;
                    };
                    if (dims != state.expected_dims) {
                        if (state.scanned_since_yield_check >= 1024) {
                            state.scanned_since_yield_check = 0;
                            return try state.yieldAfter(key);
                        }
                        return .@"continue";
                    }

                    var identity = (try artifact_replay.decodeEmbeddingArtifactWriteIdentityAlloc(state.ctx.alloc, key, state.expected_name)) orelse {
                        if (state.scanned_since_yield_check >= 1024) {
                            state.scanned_since_yield_check = 0;
                            return try state.yieldAfter(key);
                        }
                        return .@"continue";
                    };
                    var write_transferred = false;
                    errdefer if (!write_transferred) identity.deinit(state.ctx.alloc);
                    const owned_index_name = try state.ctx.alloc.dupe(u8, state.index_name);
                    errdefer if (!write_transferred) state.ctx.alloc.free(owned_index_name);
                    const artifact_key = try state.ctx.alloc.dupe(u8, key);
                    errdefer if (!write_transferred) state.ctx.alloc.free(artifact_key);
                    try state.writes.append(state.ctx.alloc, .{
                        .index_name = owned_index_name,
                        .doc_key = identity.doc_key,
                        .parent_doc_key = identity.parent_doc_key,
                        .artifact_key = artifact_key,
                        .vector = &.{},
                    });
                    write_transferred = true;
                    state.rebuilt += 1;

                    if (state.writes.items.len >= state.rebuild_chunk_size) {
                        try state.flush();
                        state.scanned_since_yield_check = 0;
                        return try state.yieldAfter(key);
                    }
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

            try scanStoreForRebuildContext(ctx, lower, "", .{ .lower_exclusive = resume_key != null }, &state, ScanState.scanEntry);
            if (state.writes.items.len > 0) try state.flush();

            if (resumable) {
                try ctx.index_manager.finishDenseStreamingReplaySessionByNameWithOptions(index_name, denseRepairFinishOptions(ctx));
            } else {
                try ctx.index_manager.finishDenseBulkIngestSessionByNameWithOptions(index_name, denseRepairFinishOptions(ctx));
            }
            session_open = false;
            const owned_resume_key = state.resume_key;
            state.resume_key = null;
            return .{ .rebuilt = state.rebuilt, .resume_key = owned_resume_key };
        }

        pub fn rebuildDenseIndexForTargetCoverageSliceContext(
            ctx: *AsyncContext,
            index_name: []const u8,
            rebuild_chunk_size: usize,
            resume_key: ?[]const u8,
        ) !DenseRebuildSliceResult {
            const entry = ctx.index_manager.denseIndex(index_name) orelse return .{};
            if (!denseIndexIsArtifactBacked(entry)) {
                return try rebuildDenseIndexFromPrimaryVectorsSliceContext(ctx, index_name, rebuild_chunk_size, resume_key);
            }
            return try rebuildDenseIndexFromStoredEmbeddingArtifactsSliceContext(ctx, index_name, rebuild_chunk_size, resume_key);
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

        fn denseEmbeddingArtifactRepairReason(
            ctx: anytype,
            dims: u32,
            artifact_key: []const u8,
        ) !?types.ArtifactRepairReason {
            const raw = ctx.store.get(ctx.alloc, artifact_key) catch |err| switch (err) {
                error.NotFound => return .missing_artifact,
                else => return err,
            };
            defer ctx.alloc.free(raw);

            const expected_dims: usize = @intCast(dims);
            if (enrichment_artifact_codec.denseEmbeddingVectorView(raw)) |maybe_view| {
                if (maybe_view) |view| {
                    return if (view.len == expected_dims) null else .corrupt_artifact;
                }
            } else |err| {
                if (Self.isRecoverableEmbeddingArtifactError(err)) return .corrupt_artifact;
                return err;
            }

            const decoded = enrichment_artifact_codec.decodeDenseEmbeddingAlloc(ctx.alloc, raw) catch |err| {
                if (Self.isRecoverableEmbeddingArtifactError(err)) return .corrupt_artifact;
                return err;
            };
            defer ctx.alloc.free(decoded);
            return if (decoded.len == expected_dims) null else .corrupt_artifact;
        }

        fn sparseEmbeddingArtifactRepairReason(ctx: anytype, artifact_key: []const u8) !?types.ArtifactRepairReason {
            const raw = ctx.store.get(ctx.alloc, artifact_key) catch |err| switch (err) {
                error.NotFound => return .missing_artifact,
                else => return err,
            };
            defer ctx.alloc.free(raw);

            if (enrichment_artifact_codec.sparseEmbeddingVectorView(raw)) |maybe_view| {
                if (maybe_view != null) return null;
            } else |err| {
                if (Self.isRecoverableEmbeddingArtifactError(err)) return .corrupt_artifact;
                return err;
            }

            var decoded = enrichment_artifact_codec.decodeSparseEmbeddingAlloc(ctx.alloc, raw) catch |err| {
                if (Self.isRecoverableEmbeddingArtifactError(err)) return .corrupt_artifact;
                return err;
            };
            decoded.deinit(ctx.alloc);
            return null;
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
            try db_internal.checkAsyncRepairCancelled(ctx);
            try db_internal.checkAsyncRepairCapacityBoundary(ctx);
            try ctx.index_manager.applySparseEmbeddingWritesByNameWithOptions(
                ctx.store,
                index_name,
                writes.items,
                .{ .mode = .bulk_ingest },
            );
        }
    };
}

test "dense projection finalization releases admission mutex and blocks catch-up" {
    const TestDB = struct {
        pub const DerivedAsyncCallbacks = struct {
            pub fn async_index_profile_enabled() bool {
                return false;
            }
        };
    };
    const TestAsyncContext = db_internal.AsyncContext(TestDB);
    const Runtime = Impl(TestDB);
    var apply_mutex: apply_rw_lock_mod.ApplyRwLock = .{};
    var ctx = TestAsyncContext{
        .alloc = std.testing.allocator,
        .store = undefined,
        .index_manager = undefined,
        .apply_mutex = &apply_mutex,
    };
    defer ctx.deinit(std.testing.allocator);

    var finalization_lock = Runtime.lockAtomicWithBackoffProfiled(&ctx.dense_finish_mutex, &ctx.stats.dense_finish_mutex);
    try std.testing.expect(Runtime.tryClaimDenseProjectionFinalizationLocked(&ctx));
    finalization_lock.unlock();
    try std.testing.expect(ctx.dense_finish_mutex.tryLock());
    ctx.dense_finish_mutex.unlock();
    try std.testing.expectError(error.ReplayDocumentNotVisible, db_internal.beginDenseCatchUpSessionTracked(&ctx, "vec"));
    Runtime.finishDenseProjectionFinalization(&ctx);
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

test "db replay blocks dense embedding writes when artifact payload is missing" {
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

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();

    const issues = try reopened.listEmbeddingArtifactRepairIssues(alloc, "dv_v1", 0);
    defer types.freeEmbeddingArtifactRepairIssues(alloc, issues);
    try std.testing.expectEqual(@as(usize, 1), issues.len);
    try std.testing.expectEqual(.missing_embedding_artifact, issues[0].reason);
    try std.testing.expectEqual(.embedding, issues[0].artifact_kind);
    try std.testing.expectEqualStrings("doc:a", issues[0].doc_key);
    try std.testing.expectEqualStrings("dv_v1", issues[0].artifact_name);

    const degraded_stats = try reopened.stats(alloc);
    defer types.freeDBStats(alloc, degraded_stats);
    try std.testing.expect(degraded_stats.repair_degraded);
    try std.testing.expectEqual(@as(u64, 1), degraded_stats.repair_issue_count);
    try std.testing.expect(degraded_stats.indexes[0].repair_degraded);
    try std.testing.expectEqual(@as(u64, 1), degraded_stats.indexes[0].repair_issue_count);

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
    try std.testing.expectEqual(@as(u64, 0), dense_applied);
    try std.testing.expect(appended_sequence > dense_applied);
}

test "db replay skips a missing dense artifact after its source document was deleted" {
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

        const artifact_key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:deleted", "dv_v1");
        defer alloc.free(artifact_key);
        appended_sequence = try db.derivedAsyncAppendDerivedBatchRecord(.{
            .dense_embeddings = &.{.{
                .index_name = "dv_v1",
                .doc_key = "doc:deleted",
                .artifact_key = artifact_key,
                .vector = &[_]f32{ 1, 0 },
            }},
        });
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();

    const issues = try reopened.listEmbeddingArtifactRepairIssues(alloc, "dv_v1", 0);
    defer types.freeEmbeddingArtifactRepairIssues(alloc, issues);
    try std.testing.expectEqual(@as(usize, 0), issues.len);
    try std.testing.expectEqual(appended_sequence, try reopened.core.loadAppliedSequence(alloc, "dv_v1"));
}

test "db replay blocks and preserves corrupt dense embedding artifacts" {
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

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();

    const issues = try reopened.listEmbeddingArtifactRepairIssues(alloc, "dv_v1", 0);
    defer types.freeEmbeddingArtifactRepairIssues(alloc, issues);
    try std.testing.expectEqual(@as(usize, 1), issues.len);
    try std.testing.expectEqual(.corrupt_embedding_artifact, issues[0].reason);
    try std.testing.expectEqual(.embedding, issues[0].artifact_kind);
    try std.testing.expectEqualStrings("doc:a", issues[0].doc_key);
    try std.testing.expectEqualStrings("dv_v1", issues[0].artifact_name);

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
    const artifact_value = try reopened.core.store.get(alloc, artifact_key);
    defer alloc.free(artifact_value);
    try std.testing.expectEqualStrings("bad-artifact", artifact_value);

    const dense_applied = try reopened.core.loadAppliedSequence(alloc, "dv_v1");
    try std.testing.expectEqual(@as(u64, 0), dense_applied);
    try std.testing.expect(appended_sequence > dense_applied);

    reopened.runDerivedUntil(appended_sequence) catch |err| switch (err) {
        error.ArtifactRepairRequired => {},
        else => return err,
    };
    const issues_after_replay = try reopened.listEmbeddingArtifactRepairIssues(alloc, "dv_v1", 0);
    defer types.freeEmbeddingArtifactRepairIssues(alloc, issues_after_replay);
    try std.testing.expectEqual(@as(usize, 1), issues_after_replay.len);
    try std.testing.expectEqual(issues[0].first_seen_ns, issues_after_replay[0].first_seen_ns);
    try std.testing.expectEqual(issues[0].attempts, issues_after_replay[0].attempts);
    try std.testing.expectEqualStrings(issues[0].last_error, issues_after_replay[0].last_error);

    const repair = try reopened.repairEmbeddingArtifactIssues(alloc, 10);
    try std.testing.expectEqual(@as(u64, 1), repair.scanned);
    try std.testing.expectEqual(@as(u64, 0), repair.repaired);
    try std.testing.expectEqual(@as(u64, 1), repair.failed);
    const issues_after_repair = try reopened.listEmbeddingArtifactRepairIssues(alloc, "dv_v1", 0);
    defer types.freeEmbeddingArtifactRepairIssues(alloc, issues_after_repair);
    try std.testing.expectEqual(@as(usize, 1), issues_after_repair.len);
    try std.testing.expectEqual(@as(u64, 1), issues_after_repair[0].attempts);
    try std.testing.expectEqualStrings("embedding_enrichment_unavailable", issues_after_repair[0].last_error);
}

test "db replay records wrong-dimension dense embedding artifact as repair debt" {
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
        try TestHelpers.putDenseEmbeddingArtifactForTest(&db, alloc, artifact_key, null, &[_]f32{ 1, 0, 0 });

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

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();

    const issues = try reopened.listEmbeddingArtifactRepairIssues(alloc, "dv_v1", 0);
    defer types.freeEmbeddingArtifactRepairIssues(alloc, issues);
    try std.testing.expectEqual(@as(usize, 1), issues.len);
    try std.testing.expectEqual(.corrupt_embedding_artifact, issues[0].reason);
    try std.testing.expectEqualStrings("doc:a", issues[0].doc_key);
    try std.testing.expectEqualStrings("dv_v1", issues[0].artifact_name);

    const degraded_stats = try reopened.stats(alloc);
    defer types.freeDBStats(alloc, degraded_stats);
    try std.testing.expect(degraded_stats.repair_degraded);
    try std.testing.expectEqual(@as(u64, 1), degraded_stats.repair_issue_count);
    try std.testing.expect(degraded_stats.indexes[0].repair_degraded);
    try std.testing.expectEqual(@as(u64, 1), degraded_stats.indexes[0].repair_issue_count);

    const dense_applied = try reopened.core.loadAppliedSequence(alloc, "dv_v1");
    try std.testing.expectEqual(@as(u64, 0), dense_applied);
    try std.testing.expect(appended_sequence > dense_applied);
}

test "db rebuild dense indexes preserves corrupt stored embedding artifacts" {
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
    const artifact_value = try db.core.store.get(alloc, artifact_key);
    defer alloc.free(artifact_value);
    try std.testing.expectEqualStrings("bad-artifact", artifact_value);
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

test "db replay skips a missing sparse artifact after its source document was deleted" {
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

        const artifact_key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:deleted", "sp_v1");
        defer alloc.free(artifact_key);
        appended_sequence = try db.derivedAsyncAppendDerivedBatchRecord(.{
            .sparse_embeddings = &.{.{
                .index_name = "sp_v1",
                .doc_key = "doc:deleted",
                .artifact_key = artifact_key,
                .indices = &.{1},
                .values = &.{1.0},
            }},
        });
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();

    const issues = try reopened.listEmbeddingArtifactRepairIssues(alloc, "sp_v1", 0);
    defer types.freeEmbeddingArtifactRepairIssues(alloc, issues);
    try std.testing.expectEqual(@as(usize, 0), issues.len);
    try std.testing.expectEqual(appended_sequence, try reopened.core.loadAppliedSequence(alloc, "sp_v1"));
    try std.testing.expectEqual(@as(u64, 0), reopened.core.index_manager.sparseIndex("sp_v1").?.index.stats().doc_count);
}

test "db replay blocks and preserves corrupt sparse embedding artifacts" {
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

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();

    const issues = try reopened.listEmbeddingArtifactRepairIssues(alloc, "sp_v1", 0);
    defer types.freeEmbeddingArtifactRepairIssues(alloc, issues);
    try std.testing.expectEqual(@as(usize, 1), issues.len);
    try std.testing.expectEqual(.corrupt_embedding_artifact, issues[0].reason);
    try std.testing.expectEqual(.embedding, issues[0].artifact_kind);
    try std.testing.expectEqualStrings("doc:a", issues[0].doc_key);
    try std.testing.expectEqualStrings("sp_v1", issues[0].artifact_name);

    var sparse_index = reopened.core.index_manager.sparseIndex("sp_v1").?.index;
    const stats = sparse_index.stats();
    try std.testing.expectEqual(@as(u64, 0), stats.doc_count);

    const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "sp_v1");
    defer alloc.free(artifact_key);
    const artifact_value = try reopened.core.store.get(alloc, artifact_key);
    defer alloc.free(artifact_value);
    try std.testing.expectEqualStrings("bad-artifact", artifact_value);

    const sparse_applied = try reopened.core.loadAppliedSequence(alloc, "sp_v1");
    try std.testing.expectEqual(@as(u64, 0), sparse_applied);
    try std.testing.expect(appended_sequence > sparse_applied);
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

test "db derived async replay truncation retains durable enrichment debt" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;
    const DerivedAsync = Impl(DB);

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

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
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }},
        .sync_level = .write,
    });
    const first_sequence = db.core.nextDerivedSequence();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:b", .value = "{\"body\":\"beta\"}" }},
        .sync_level = .write,
    });
    const target_sequence = db.core.nextDerivedSequence();
    try std.testing.expect(target_sequence > first_sequence);

    try enrichment_state.saveAppliedSequence(
        db.core.store,
        enrichment_runtime_mod.scope_name,
        first_sequence,
    );
    try DerivedAsync.truncateReplaySequenceAsync(db.async_context, target_sequence);

    const retained = try replay_stream_mod.iterateFrom(alloc, db.core.store, 1);
    defer {
        for (retained) |*entry| entry.deinit(alloc);
        alloc.free(retained);
    }
    try std.testing.expectEqual(@as(usize, 1), retained.len);
    try std.testing.expectEqual(target_sequence, retained[0].sequence);
}

test "db derived async replay truncation retains journal behind generated enrichment" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var gated = TestHelpers.GateDenseEmbedder{};
    gated.allowed_successes.store(0, .release);
    var db = try DB.open(alloc, std.mem.span(path), .{
        .executor = .{ .backend = .io_threaded },
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = gated.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.addIndex(.{
        .name = "semantic_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"cosine\",\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"semantic_idx\"}}",
    });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"alpha concept overview\"}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"body\":\"beta architecture notes\"}" },
        },
        .sync_level = .write,
    });

    _ = try TestHelpers.waitForAppliedSequenceAdvance(alloc, &db, "ft_v1", 0);
    _ = try TestHelpers.waitForAppliedSequenceAdvance(alloc, &db, "semantic_idx", 0);
    var attempts: usize = 0;
    while (attempts < TestHelpers.default_test_wait_attempts and gated.snapshot().rate_limited_requests == 0) : (attempts += 1) {
        platform.time.sleepMs(10);
    }
    try std.testing.expect(gated.snapshot().rate_limited_requests > 0);
    try std.testing.expectEqual(
        @as(u64, 0),
        try enrichment_state.loadAppliedSequence(alloc, db.core.store, enrichment_runtime_mod.scope_name),
    );

    var retained_count: usize = 0;
    var retention_attempts: usize = 0;
    while (retention_attempts < 50) : (retention_attempts += 1) {
        const retained = try db.core.store.iterateReplayFrom(alloc, 1);
        defer {
            for (retained) |*entry| entry.deinit(alloc);
            alloc.free(retained);
        }
        retained_count = retained.len;
        if (retained_count == 0) break;
        platform.time.sleepMs(10);
    }
    try std.testing.expect(retained_count > 0);
    gated.allowAll();
}

test "db derived async restart after provider failure resumes enrichment from retained async replay" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var first_applied_sequence: u64 = 0;
    var failed_target_sequence: u64 = 0;
    {
        // Establish one durable watermark, then leave the next replay entry
        // behind a retryable provider failure while managed indexes catch up.
        var gated = TestHelpers.GateDenseEmbedder{};
        var db = try DB.open(alloc, std.mem.span(path), .{
            .enrichment = .{
                .owner_id = "worker-a",
                .dense_embedder = gated.interface(),
            },
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense_v1\"}}",
        });
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }},
            .sync_level = .write,
        });
        first_applied_sequence = db.core.nextEnrichmentSequence();
        try db.enrichment_runtime.?.waitForApplied(first_applied_sequence);

        try db.batch(.{
            .writes = &.{.{ .key = "doc:b", .value = "{\"body\":\"beta\"}" }},
            .sync_level = .write,
        });
        failed_target_sequence = db.core.nextEnrichmentSequence();
        try std.testing.expect(failed_target_sequence > first_applied_sequence);

        var attempts: usize = 0;
        while (attempts < TestHelpers.default_test_wait_attempts and gated.snapshot().rate_limited_requests == 0) : (attempts += 1) {
            platform.time.sleepMs(10);
        }
        try std.testing.expect(gated.snapshot().rate_limited_requests > 0);

        db.enrichment_runtime.?.stop();
        try db.executor.waitForAll(failed_target_sequence);

        const failed_stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, failed_stats);
        try std.testing.expectEqual(first_applied_sequence, failed_stats.enrichment.applied_sequence);
        try std.testing.expectEqual(failed_target_sequence, failed_stats.enrichment.target_sequence);

        const retained = try replay_stream_mod.iterateFrom(alloc, db.core.store, first_applied_sequence + 1);
        defer {
            for (retained) |*entry| entry.deinit(alloc);
            alloc.free(retained);
        }
        try std.testing.expect(retained.len > 0);
        var retained_failed_target = false;
        for (retained) |entry| {
            if (entry.sequence == failed_target_sequence) retained_failed_target = true;
        }
        try std.testing.expect(retained_failed_target);
    }

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();

    try reopened.runUntilIdle();

    const recovered_stats = try reopened.stats(alloc);
    defer types.freeDBStats(alloc, recovered_stats);
    try std.testing.expect(recovered_stats.enrichment.applied_sequence >= failed_target_sequence);
    try std.testing.expectEqual(recovered_stats.enrichment.applied_sequence, recovered_stats.enrichment.target_sequence);

    const artifact_key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:b", "body_dense_v1");
    defer alloc.free(artifact_key);
    const artifacts = try reopened.core.store.scanPrefix(alloc, artifact_key);
    defer docstore_mod.DocStore.freeResults(alloc, artifacts);
    try std.testing.expectEqual(@as(usize, 1), artifacts.len);

    const query_vec = try deterministic.interface().embedDense(alloc, "", "beta", 3);
    defer alloc.free(query_vec);
    var result = try reopened.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{ .vector = query_vec, .k = 2 },
        .limit = 2,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 2), result.total_hits);
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
    var io_impl = db_internal.threadedIo();
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
    var io_impl = db_internal.threadedIo();
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

test "db dense artifact counters include derived chunk embedding artifacts" {
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

    try db.addIndex(.{
        .name = "semantic_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2,\"embedding_name\":\"chunk_dense_v1\"}}",
    });

    const chunk_key = try internal_keys.chunkArtifactKeyAlloc(alloc, "doc:a", "body_chunks_v1", 0);
    defer alloc.free(chunk_key);
    const artifact_key = try internal_keys.derivedEmbeddingArtifactKeyAlloc(alloc, chunk_key, "chunk_dense_v1");
    defer alloc.free(artifact_key);
    try putDenseEmbeddingArtifactWithCounterForTest(&db, alloc, artifact_key, null, &[_]f32{ 1, 0, 0 });

    try std.testing.expectEqual(
        @as(?u64, 1),
        try DB.loadDenseArtifactTargetCounter(alloc, db.core.store, "semantic_idx"),
    );
}

test "db dense artifact rebuild trusts clean projection checkpoint without artifact recount" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const dense_cfg: types.IndexConfig = .{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    };

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(dense_cfg);
        const stored_dense_cfg = &(db.core.index_manager.denseIndex("dense_idx") orelse return error.TestUnexpectedResult).config;
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}" }},
            .sync_level = .full_index,
        });

        const target_sequence = db.core.nextDerivedSequence() -| 1;
        try db.core.saveProjectionCheckpoint("dense_idx", .{
            .applied_sequence = target_sequence,
            .status = .clean,
            .generation = 3,
            .config_hash = types.indexConfigHash(stored_dense_cfg.*),
        });
        const checkpoint_path = db.core.applied_sequence_checkpoint_path orelse return error.TestUnexpectedResult;
        const mirrored_ahead_sequence: u64 = 1000;
        try apply_state.saveProjectionCheckpointWithSidecar(
            alloc,
            db.core.index_manager.checkpointIo(),
            db.core.store,
            checkpoint_path,
            "dense_idx",
            .{
                .applied_sequence = mirrored_ahead_sequence,
                .status = .clean,
                .generation = 99,
                .config_hash = types.indexConfigHash(stored_dense_cfg.*),
            },
        );

        const stale_artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:stale", "dense_idx");
        defer alloc.free(stale_artifact_key);
        try TestHelpers.putDenseEmbeddingArtifactForTest(&db, alloc, stale_artifact_key, null, &[_]f32{ 0, 1, 0 });
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();

    try std.testing.expect(!(try reopened.hasPendingDenseArtifactRebuild(alloc)));

    const checkpoint = try reopened.core.loadProjectionCheckpoint(alloc, "dense_idx");
    try std.testing.expectEqual(apply_state.ProjectionStatus.clean, checkpoint.status);
    try std.testing.expect(checkpoint.applied_sequence < 1000);
    try std.testing.expectEqual(@as(u64, 3), checkpoint.generation);
    try std.testing.expectEqual(types.indexConfigHash(dense_cfg), checkpoint.config_hash);

    const stats = try reopened.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(@as(usize, 1), stats.indexes.len);
    try std.testing.expectEqualStrings("clean", stats.indexes[0].projection_checkpoint_status);
    try std.testing.expectEqual(checkpoint.applied_sequence, stats.indexes[0].projection_checkpoint_applied_sequence);
    try std.testing.expectEqual(@as(u64, 3), stats.indexes[0].projection_checkpoint_generation);
    try std.testing.expectEqual(types.indexConfigHash(dense_cfg), stats.indexes[0].projection_checkpoint_config_hash);
    try std.testing.expectEqual(
        stats.indexes[0].replay_target_sequence -| checkpoint.applied_sequence,
        stats.indexes[0].checkpoint_replay_tail_sequence_count,
    );
}

test "db dense artifact rebuild rejects clean checkpoint for stale config identity" {
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
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
        });
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}" }},
            .sync_level = .full_index,
        });

        const target_sequence = db.core.nextDerivedSequence() -| 1;
        try db.core.saveProjectionCheckpoint("dense_idx", .{
            .applied_sequence = target_sequence,
            .status = .clean,
            .generation = 4,
            .config_hash = types.indexConfigHash(.{
                .name = "dense_idx",
                .kind = .dense_vector,
                .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"cosine\",\"external\":true}",
            }),
        });

        const stale_artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:stale", "dense_idx");
        defer alloc.free(stale_artifact_key);
        try TestHelpers.putDenseEmbeddingArtifactForTest(&db, alloc, stale_artifact_key, null, &[_]f32{ 0, 1, 0 });
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();

    try std.testing.expect(try reopened.hasPendingDenseArtifactRebuild(alloc));
    try std.testing.expectEqual(@as(usize, 1), try reopened.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc));
    const repair_id = (try reopened.indexRepairIdForIndex(alloc, "dense_idx")) orelse return error.TestUnexpectedResult;
    var intent = try DB.ArtifactRepairCallbacks.load_index_repair_entry_by_id(&reopened, alloc, repair_id);
    try std.testing.expectEqual(index_repair_state.Trigger.projection_generation_invalid, intent.intent.trigger);
    intent.deinit(alloc);
    try std.testing.expect(reopened.core.index_manager.repairUnavailable("dense_idx"));
    try std.testing.expectError(error.IndexRebuilding, reopened.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0, 0 }, .k = 1 } },
        .limit = 1,
    }));
}

test "db dense artifact rebuild checks artifact counters before clean checkpoint skip" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const dense_cfg: types.IndexConfig = .{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    };

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(dense_cfg);
        const stored_dense_cfg = &(db.core.index_manager.denseIndex("dense_idx") orelse return error.TestUnexpectedResult).config;
        const stored_key = try internal_keys.documentKeyAlloc(alloc, "doc:a");
        defer alloc.free(stored_key);
        try db.core.store.putBatch(&.{
            .{ .key = stored_key, .value = "{\"title\":\"alpha\"}" },
        }, &.{});
        const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "dense_idx");
        defer alloc.free(artifact_key);
        try putDenseEmbeddingArtifactWithCounterForTest(&db, alloc, artifact_key, null, &[_]f32{ 1, 0, 0 });

        const target_sequence = db.core.nextDerivedSequence() -| 1;
        try db.core.saveProjectionCheckpoint("dense_idx", .{
            .applied_sequence = target_sequence,
            .status = .clean,
            .generation = 7,
            .config_hash = types.indexConfigHash(stored_dense_cfg.*),
        });
    }

    var io_impl = db_internal.threadedIo();
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

    try std.testing.expect(try reopened.hasPendingDenseArtifactRebuild(alloc));
    try std.testing.expectEqual(@as(usize, 1), try reopened.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc));
    try std.testing.expect(!(try reopened.hasPendingDenseArtifactRebuild(alloc)));
}

test "db dense artifact rebuild does not recount counterless artifacts during startup" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const dense_cfg: types.IndexConfig = .{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    };

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(dense_cfg);
        const stored_dense_cfg = &(db.core.index_manager.denseIndex("dense_idx") orelse return error.TestUnexpectedResult).config;
        const stored_key = try internal_keys.documentKeyAlloc(alloc, "doc:a");
        defer alloc.free(stored_key);
        try db.core.store.putBatch(&.{
            .{ .key = stored_key, .value = "{\"title\":\"alpha\"}" },
        }, &.{});
        const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "dense_idx");
        defer alloc.free(artifact_key);
        try TestHelpers.putDenseEmbeddingArtifactForTest(&db, alloc, artifact_key, null, &[_]f32{ 1, 0, 0 });

        const target_sequence = db.core.nextDerivedSequence() -| 1;
        try db.core.saveProjectionCheckpoint("dense_idx", .{
            .applied_sequence = target_sequence,
            .status = .clean,
            .generation = 9,
            .config_hash = types.indexConfigHash(stored_dense_cfg.*),
        });
    }

    var io_impl = db_internal.threadedIo();
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

    try std.testing.expect(!(try reopened.hasPendingDenseArtifactRebuild(alloc)));
    try std.testing.expectEqual(@as(usize, 0), try reopened.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc));
}

test "db dense artifact planner does not let stale status override authoritative counter" {
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
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    });
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"_embeddings\":{\"dense_idx\":[1,0,0]}}",
        }},
        .sync_level = .full_index,
    });
    try std.testing.expectEqual(
        @as(?u64, 1),
        try DB.loadDenseArtifactTargetCounter(alloc, db.core.store, "dense_idx"),
    );

    const status_key = try db_internal.indexStatusKeyAlloc(alloc, "dense_idx");
    defer alloc.free(status_key);
    var stale_status: [db_internal.index_status_encoded_len]u8 = undefined;
    db_internal.encodeIndexStatusSnapshot(.{
        .kind = .dense_vector,
        .doc_count = 99,
        .node_count = 99,
        .root_node = 99,
    }, &stale_status);
    try db.core.store.put(status_key, &stale_status);

    try std.testing.expect(!(try db.hasPendingDenseArtifactRebuild(alloc)));
    try std.testing.expect(!(try DB.ArtifactRepairCallbacks.index_generation_repair_required(&db, alloc, "dense_idx")));
    try std.testing.expectEqual(
        @as(usize, 0),
        try db.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc),
    );
}

test "db dense artifact rebuild bootstraps missing counter metadata" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const dense_cfg: types.IndexConfig = .{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    };

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(dense_cfg);
        const counter_key = try DB.DerivedAsyncCallbacks.dense_artifact_target_counter_key_alloc(alloc, "dense_idx");
        defer alloc.free(counter_key);
        try db.core.store.putBatch(&.{}, &.{counter_key});
        const stored_key = try internal_keys.documentKeyAlloc(alloc, "doc:a");
        defer alloc.free(stored_key);
        try db.core.store.putBatch(&.{
            .{ .key = stored_key, .value = "{\"title\":\"alpha\"}" },
        }, &.{});
        const artifact_key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:a", "dense_idx");
        defer alloc.free(artifact_key);
        try TestHelpers.putDenseEmbeddingArtifactForTest(&db, alloc, artifact_key, null, &[_]f32{ 1, 0, 0 });

        const target_sequence = db.core.nextDerivedSequence() -| 1;
        try db.core.saveProjectionCheckpoint("dense_idx", .{
            .applied_sequence = target_sequence,
            .status = .clean,
            .generation = 9,
            .config_hash = types.indexConfigHash(dense_cfg),
        });
    }

    var io_impl = db_internal.threadedIo();
    defer io_impl.deinit();
    const dense_index_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dense_idx", .{std.mem.span(path)});
    defer alloc.free(dense_index_path);
    try std.Io.Dir.cwd().deleteTree(io_impl.io(), dense_index_path);

    var repair_id: u128 = 0;
    {
        var reopened = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer reopened.close();

        try std.testing.expect(try reopened.hasPendingDenseArtifactRebuild(alloc));
        try std.testing.expectEqual(@as(usize, 1), try reopened.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc));
        repair_id = (try reopened.indexRepairIdForIndex(alloc, "dense_idx")) orelse return error.TestUnexpectedResult;
        var intent = try DB.ArtifactRepairCallbacks.load_index_repair_entry_by_id(&reopened, alloc, repair_id);
        try std.testing.expectEqual(index_repair_state.Trigger.artifact_counter_missing, intent.intent.trigger);
        intent.deinit(alloc);
        try std.testing.expectError(error.IndexRebuilding, reopened.search(alloc, .{
            .index_name = "dense_idx",
            .query = .{ .dense_knn = .{ .vector = &.{ 1, 0, 0 }, .k = 1 } },
            .limit = 1,
        }));
    }

    // The missing-proof gate is reconstructed from the durable intent before
    // any repair owner resumes work after process restart.
    {
        var resumed = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer resumed.close();
        try std.testing.expectEqual(
            repair_id,
            (try resumed.indexRepairIdForIndex(alloc, "dense_idx")) orelse return error.TestUnexpectedResult,
        );
        try std.testing.expectError(error.IndexRebuilding, resumed.search(alloc, .{
            .index_name = "dense_idx",
            .query = .{ .dense_knn = .{ .vector = &.{ 1, 0, 0 }, .k = 1 } },
            .limit = 1,
        }));
        const repaired = try resumed.advanceIndexRepairIntent(alloc, repair_id, .{});
        try std.testing.expect(repaired.repaired);
        try std.testing.expectEqual(
            @as(?u64, 1),
            try DB.loadDenseArtifactTargetCounter(alloc, resumed.core.store, "dense_idx"),
        );
        try std.testing.expect(!(try resumed.hasPendingDenseArtifactRebuild(alloc)));
    }
}

test "db dense artifact counter bootstrap combines snapshot with concurrent write delta" {
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

    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"external\":true}",
    });
    const counter_key = try DB.DerivedAsyncCallbacks.dense_artifact_target_counter_key_alloc(alloc, "dense_idx");
    defer alloc.free(counter_key);
    try db.core.store.putBatch(&.{}, &.{counter_key});

    const artifact_a = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:a", "dense_idx");
    defer alloc.free(artifact_a);
    try TestHelpers.putDenseEmbeddingArtifactForTest(&db, alloc, artifact_a, null, &[_]f32{ 1, 0, 0 });

    const repair_id: u128 = 0x1234;
    var bootstrap_snapshot = try DB.ArtifactRepairCallbacks.begin_dense_artifact_counter_bootstrap_snapshot(&db, alloc, "dense_idx", repair_id);
    defer bootstrap_snapshot.deinit();

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:b",
            .value = "{\"title\":\"beta\",\"_embeddings\":{\"dense_idx\":[0,1,0]}}",
        }},
        .sync_level = .write,
    });
    const marker = (try DB.loadDenseArtifactCounterBootstrap(alloc, db.core.store, "dense_idx")).?;
    try std.testing.expectEqual(repair_id, marker.repair_id);
    try std.testing.expectEqual(@as(i64, 1), marker.delta);

    const snapshot_count = try DB.ArtifactRepairCallbacks.count_dense_artifacts_for_config_from_read_txn(
        &db,
        alloc,
        .{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"external\":true}",
        },
        &bootstrap_snapshot.txn,
        null,
    );
    try std.testing.expectEqual(@as(u64, 1), snapshot_count);
    try DB.ArtifactRepairCallbacks.finish_dense_artifact_counter_bootstrap(
        &db,
        alloc,
        "dense_idx",
        repair_id,
        bootstrap_snapshot.attempt_id,
        snapshot_count,
    );

    try std.testing.expectEqual(
        @as(?u64, 2),
        try DB.loadDenseArtifactTargetCounter(alloc, db.core.store, "dense_idx"),
    );
    try std.testing.expectEqual(
        @as(?DB.DenseArtifactCounterBootstrap, null),
        try DB.loadDenseArtifactCounterBootstrap(alloc, db.core.store, "dense_idx"),
    );
}

test "db dense artifact counter bootstrap restarts from a fresh snapshot" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);
    const cfg: types.IndexConfig = .{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"external\":true}",
    };
    const repair_id: u128 = 0x5678;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.addIndex(cfg);
        const counter_key = try DB.DerivedAsyncCallbacks.dense_artifact_target_counter_key_alloc(alloc, cfg.name);
        defer alloc.free(counter_key);
        try db.core.store.putBatch(&.{}, &.{counter_key});

        const artifact_a = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:a", cfg.name);
        defer alloc.free(artifact_a);
        try TestHelpers.putDenseEmbeddingArtifactForTest(&db, alloc, artifact_a, null, &[_]f32{ 1, 0, 0 });
        var abandoned_snapshot = try DB.ArtifactRepairCallbacks.begin_dense_artifact_counter_bootstrap_snapshot(&db, alloc, cfg.name, repair_id);
        try db.batch(.{
            .writes = &.{.{
                .key = "doc:b",
                .value = "{\"title\":\"beta\",\"_embeddings\":{\"dense_idx\":[0,1,0]}}",
            }},
            .sync_level = .write,
        });
        abandoned_snapshot.deinit();
        try db.core.syncStore(true);
    }

    {
        var reopened = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer reopened.close();
        var fresh_snapshot = try DB.ArtifactRepairCallbacks.begin_dense_artifact_counter_bootstrap_snapshot(&reopened, alloc, cfg.name, repair_id);
        defer fresh_snapshot.deinit();
        const count = try DB.ArtifactRepairCallbacks.count_dense_artifacts_for_config_from_read_txn(
            &reopened,
            alloc,
            cfg,
            &fresh_snapshot.txn,
            null,
        );
        try std.testing.expectEqual(@as(u64, 2), count);
        try DB.ArtifactRepairCallbacks.finish_dense_artifact_counter_bootstrap(
            &reopened,
            alloc,
            cfg.name,
            repair_id,
            fresh_snapshot.attempt_id,
            count,
        );
        try std.testing.expectEqual(
            @as(?u64, 2),
            try DB.loadDenseArtifactTargetCounter(alloc, reopened.core.store, cfg.name),
        );
        try std.testing.expect(try DB.loadDenseArtifactCounterBootstrap(alloc, reopened.core.store, cfg.name) == null);
    }
}

test "db dense artifact counter bootstrap fences stale concurrent attempt" {
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
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"external\":true}",
    });
    const counter_key = try DB.DerivedAsyncCallbacks.dense_artifact_target_counter_key_alloc(alloc, "dense_idx");
    defer alloc.free(counter_key);
    try db.core.store.putBatch(&.{}, &.{counter_key});

    const repair_id: u128 = 0x9abc;
    var stale_snapshot = try DB.ArtifactRepairCallbacks.begin_dense_artifact_counter_bootstrap_snapshot(&db, alloc, "dense_idx", repair_id);
    defer stale_snapshot.deinit();
    var current_snapshot = try DB.ArtifactRepairCallbacks.begin_dense_artifact_counter_bootstrap_snapshot(&db, alloc, "dense_idx", repair_id);
    defer current_snapshot.deinit();
    try std.testing.expect(stale_snapshot.attempt_id != current_snapshot.attempt_id);
    try std.testing.expectError(
        error.StaleDenseArtifactCounterBootstrap,
        DB.ArtifactRepairCallbacks.finish_dense_artifact_counter_bootstrap(
            &db,
            alloc,
            "dense_idx",
            repair_id,
            stale_snapshot.attempt_id,
            0,
        ),
    );
    try DB.ArtifactRepairCallbacks.finish_dense_artifact_counter_bootstrap(
        &db,
        alloc,
        "dense_idx",
        repair_id,
        current_snapshot.attempt_id,
        0,
    );
}

test "db malformed quarantined dense config does not block healthy artifact counters" {
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
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"external\":true}",
    });

    const quarantined = try alloc.alloc(types.IndexConfig, 1);
    quarantined[0] = .{
        .name = try alloc.dupe(u8, "bad_dense"),
        .kind = .dense_vector,
        .config_json = try alloc.dupe(u8, "{"),
    };
    db.core.index_manager.status_only_index_configs = quarantined;

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"_embeddings\":{\"dense_idx\":[1,0,0]}}" }},
        .sync_level = .write,
    });
    try std.testing.expectEqual(
        @as(?u64, 1),
        try DB.loadDenseArtifactTargetCounter(alloc, db.core.store, "dense_idx"),
    );
}

test "db dense repair working set scales batch to resource budget" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@intFromEnum(resource_manager_mod.Slice.dense_repair_working_set)] = .{
        .soft_limit_bytes = 12 * 1024 * 1024,
        .hard_limit_bytes = 16 * 1024 * 1024,
    };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    defer manager.deinit(alloc);

    const cfg: types.IndexConfig = .{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3072,\"external\":true}",
    };
    const plan = try DB.ArtifactRepairCallbacks.repair_working_set_plan(alloc, &manager, cfg);
    try std.testing.expect(plan.dense_rebuild_batch_items > 0);
    try std.testing.expect(plan.dense_rebuild_batch_items < 2048);
    try std.testing.expect(plan.reservation_bytes <= 12 * 1024 * 1024);
    var reservation = try manager.reserve(.dense_repair_working_set, plan.reservation_bytes);
    defer reservation.release();
}

test "db dense counter bootstrap admission respects soft background budget" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@intFromEnum(resource_manager_mod.Slice.dense_repair_working_set)] = .{
        .soft_limit_bytes = 12 * 1024 * 1024,
        .hard_limit_bytes = 16 * 1024 * 1024,
    };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    defer manager.deinit(alloc);

    var existing = try manager.reserve(.dense_repair_working_set, 5 * 1024 * 1024);
    defer existing.release();
    try std.testing.expectError(
        error.RepairResourceUnavailable,
        DB.ArtifactRepairCallbacks.reserve_dense_counter_bootstrap_working_set(&manager),
    );
    const under_soft_pressure = manager.sliceStats(.dense_repair_working_set);
    try std.testing.expectEqual(@as(u64, 5 * 1024 * 1024), under_soft_pressure.used_bytes);
    try std.testing.expectEqual(@as(u64, 0), under_soft_pressure.hard_limit_rejections);

    existing.release();
    var admitted = try DB.ArtifactRepairCallbacks.reserve_dense_counter_bootstrap_working_set(&manager);
    defer admitted.release();
    try std.testing.expectEqual(
        @as(u64, DB.ArtifactRepairCallbacks.dense_counter_bootstrap_working_set_bytes),
        manager.sliceStats(.dense_repair_working_set).used_bytes,
    );
}

test "db dense artifact rebuild uses durable artifact counters instead of recount" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const dense_cfg: types.IndexConfig = .{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    };

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(dense_cfg);
        const stored_dense_cfg = &(db.core.index_manager.denseIndex("dense_idx") orelse return error.TestUnexpectedResult).config;
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}" }},
            .sync_level = .full_index,
        });

        const target_sequence = db.core.nextDerivedSequence() -| 1;
        try db.core.saveProjectionCheckpoint("dense_idx", .{
            .applied_sequence = target_sequence,
            .status = .rebuilding,
            .generation = 5,
            .config_hash = types.indexConfigHash(stored_dense_cfg.*),
        });

        const stale_artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:stale", "dense_idx");
        defer alloc.free(stale_artifact_key);
        try TestHelpers.putDenseEmbeddingArtifactForTest(&db, alloc, stale_artifact_key, null, &[_]f32{ 0, 1, 0 });
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();

    try std.testing.expect(!(try reopened.hasPendingDenseArtifactRebuild(alloc)));
}

test "db dense artifact rebuild persists state through external index storage" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/table.aflite", .{tmp.sub_path});
    defer alloc.free(path);
    const index_base_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/index-namespace", .{tmp.sub_path});
    defer alloc.free(index_base_path);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{
        .sub_path = path,
        .data = "single-file-container",
    });

    var index_storage = lsm_backend_mod.MemoryStorage.init(alloc);
    defer index_storage.deinit();
    const external_storage = index_storage.storage();

    var db = try DB.open(alloc, path, .{
        .primary_backend = .{ .mem = .{} },
        .index_backends = .{
            .dense_storage_backend = .lsm,
            .dense_lsm_storage = external_storage,
        },
        .physical_root_mode = .external_backend,
        .index_base_path = index_base_path,
        .external_derived_checkpoints = false,
        .start_index_workers = false,
        .start_optional_runtimes = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"title\":\"alpha\"}",
        }},
        .sync_level = .write,
    });
    const artifact_key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:a", "dense_idx");
    defer alloc.free(artifact_key);
    try putDenseEmbeddingArtifactWithCounterForTest(&db, alloc, artifact_key, null, &[_]f32{ 1, 0, 0 });

    const rebuild_root_path = try DB.LifecycleCallbacks.dense_index_rebuild_state_path_alloc(&db, alloc, "dense_idx");
    defer alloc.free(rebuild_root_path);
    const expected_rebuild_root_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dense_idx", .{index_base_path});
    defer alloc.free(expected_rebuild_root_path);
    try std.testing.expectEqualStrings(expected_rebuild_root_path, rebuild_root_path);
    const rebuild_state = db.core.index_manager.rebuildState(
        .dense_vector,
        rebuild_root_path,
        db.core.index_manager.denseIndex("dense_idx").?.config,
    );
    try rebuild_state.updateWithIo(db.core.index_manager.checkpointIo(), "");
    const persisted_cursor = (try rebuild_state.checkWithIo(alloc, db.core.index_manager.checkpointIo())) orelse
        return error.MissingExternalRebuildState;
    defer alloc.free(persisted_cursor);
    const rebuilding_stats = try db.diagnosticStats(alloc);
    defer types.freeDBStats(alloc, rebuilding_stats);
    var saw_dense_index = false;
    var saw_active_backfill = false;
    for (rebuilding_stats.indexes) |index| {
        if (!std.mem.eql(u8, index.name, "dense_idx")) continue;
        saw_dense_index = true;
        saw_active_backfill = index.backfill_active;
        try std.testing.expectEqual(@as(u64, 0), index.doc_count);
    }
    try std.testing.expect(saw_dense_index);
    try std.testing.expect(saw_active_backfill);

    try std.testing.expectEqual(
        @as(usize, 1),
        try db.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc),
    );
    try std.testing.expect((try rebuild_state.checkWithIo(alloc, db.core.index_manager.checkpointIo())) == null);

    var result = try db.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0, 0 }, .k = 1 } },
        .limit = 1,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db dense artifact rebuild resume keys are owned by plan allocator" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const db_alloc = std.heap.page_allocator;
    const doc_count: usize = 3;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(db_alloc, std.mem.span(path), .{
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
            try putDenseEmbeddingArtifactWithCounterForTest(&db, alloc, artifact_key, null, &[_]f32{
                @floatFromInt(i + 1),
                0,
                0,
            });
        }
    }

    var io_impl = db_internal.threadedIo();
    defer io_impl.deinit();
    const dense_index_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dense_idx", .{std.mem.span(path)});
    defer alloc.free(dense_index_path);
    try std.Io.Dir.cwd().deleteTree(io_impl.io(), dense_index_path);

    var reopened = try DB.open(db_alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();

    const rebuilt = try reopened.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc);
    try std.testing.expectEqual(doc_count, rebuilt);
    try std.testing.expectEqual(@as(u64, doc_count), reopened.core.index_manager.denseIndex("dense_idx").?.index.metadata.active_count);
}

test "db dense artifact rebuild keeps resume keys owned by caller allocator" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const rebuild_alloc = std.heap.page_allocator;

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

        const stored_key = try internal_keys.documentKeyAlloc(alloc, "doc:a");
        defer alloc.free(stored_key);
        try db.core.store.putBatch(&.{
            .{ .key = stored_key, .value = "{\"title\":\"alpha\"}" },
        }, &.{});

        const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "dense_idx");
        defer alloc.free(artifact_key);
        try putDenseEmbeddingArtifactWithCounterForTest(&db, alloc, artifact_key, null, &[_]f32{ 1, 0, 0 });
    }

    var io_impl = db_internal.threadedIo();
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

    const rebuilt = try reopened.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(rebuild_alloc);
    try std.testing.expectEqual(@as(usize, 1), rebuilt);
    try std.testing.expect(!(try reopened.hasPendingDenseArtifactRebuild(alloc)));
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
    // Discovery reports one scheduled generation repair, not the number of
    // source documents that the BackendRuntime-owned state machine will
    // reconstruct inside that generation.
    try std.testing.expectEqual(@as(usize, 1), rebuilt);
    try std.testing.expect(try reopened.hasPendingDenseArtifactRebuild(alloc));
    const repair_id = (try reopened.indexRepairIdForIndex(alloc, "dense_idx")) orelse return error.TestUnexpectedResult;
    const advanced = try reopened.advanceIndexRepairIntent(alloc, repair_id, .{});
    try std.testing.expect(advanced.repaired);
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

test "db derived async dense artifact rebuild dense artifact surplus uses quarantined generation replacement" {
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
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    });
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}",
        }},
        .sync_level = .full_index,
    });
    try std.testing.expectEqual(
        @as(?u64, 1),
        try DB.loadDenseArtifactTargetCounter(alloc, db.core.store, "dense_idx"),
    );

    // Model an interrupted replay that left a vector without a corresponding
    // source artifact or target-counter increment.
    var ghost_vector = [_]f32{ 0, 1, 0 };
    try db.core.index_manager.denseIndex("dense_idx").?.index.insert(0xdead_beef, &ghost_vector);
    try std.testing.expectEqual(
        @as(u64, 2),
        db.core.index_manager.denseIndex("dense_idx").?.index.stats().active_count,
    );
    try std.testing.expect(try db.hasPendingDenseArtifactRebuild(alloc));

    try std.testing.expect((try db.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc)) > 0);
    const first_repair_id = (try db.indexRepairIdForIndex(alloc, "dense_idx")) orelse
        return error.TestUnexpectedResult;
    var first_state = try db.loadIndexRepairState(alloc);
    try std.testing.expectEqual(
        index_repair_state.Trigger.artifact_coverage_mismatch,
        first_state.entries.items[0].intent.trigger,
    );
    first_state.deinit(alloc);
    try std.testing.expectEqual(
        @as(u64, 2),
        db.core.index_manager.denseIndex("dense_idx").?.index.stats().active_count,
    );
    try std.testing.expectError(error.IndexRebuilding, db.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0, 0 }, .k = 1 } },
        .limit = 1,
    }));
    const QuarantinedGenerationHook = struct {
        fn afterSnapshot(_: *anyopaque, hooked_db: *DB, _: []const u8, _: u64) !void {
            try std.testing.expectEqual(
                @as(u64, 2),
                hooked_db.core.index_manager.denseIndex("dense_idx").?.index.stats().active_count,
            );
            try std.testing.expectError(error.IndexRebuilding, hooked_db.search(hooked_db.alloc, .{
                .index_name = "dense_idx",
                .query = .{ .dense_knn = .{ .vector = &.{ 1, 0, 0 }, .k = 1 } },
                .limit = 1,
            }));
        }
    };
    var hook_context: u8 = 0;
    db.shadow_index_repair_hook = .{
        .ptr = &hook_context,
        .after_snapshot_build = QuarantinedGenerationHook.afterSnapshot,
    };
    const first_repair = try db.advanceIndexRepairIntent(alloc, first_repair_id, .{});
    db.shadow_index_repair_hook = null;
    try std.testing.expect(first_repair.repaired);
    try std.testing.expectEqual(
        @as(u64, 1),
        db.core.index_manager.denseIndex("dense_idx").?.index.stats().active_count,
    );
    try std.testing.expect(!try db.hasPendingIndexRepairIntents(alloc));

    var result = try db.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 0, 1, 0 }, .k = 2 } },
        .limit = 2,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);

    // A zero-target generation must also replace a surplus, including when a
    // stale rebuild cursor belongs to the prior generation.
    try db.batch(.{
        .deletes = &.{"doc:a"},
        .sync_level = .full_index,
    });
    try std.testing.expectEqual(
        @as(?u64, 0),
        try DB.loadDenseArtifactTargetCounter(alloc, db.core.store, "dense_idx"),
    );
    try db.core.index_manager.denseIndex("dense_idx").?.index.insert(0xbeef_dead, &ghost_vector);
    const rebuild_state_path = try db.derivedAsyncDenseIndexRebuildStatePathAlloc(alloc, "dense_idx");
    defer alloc.free(rebuild_state_path);
    const rebuild_state = backfill_state_mod.RebuildState.init(rebuild_state_path);
    try rebuild_state.update("stale-generation-cursor");
    try std.testing.expect(try db.hasPendingDenseArtifactRebuild(alloc));
    _ = try db.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc);
    const zero_repair_id = (try db.indexRepairIdForIndex(alloc, "dense_idx")) orelse
        return error.TestUnexpectedResult;
    try std.testing.expectEqual(
        @as(u64, 1),
        db.core.index_manager.denseIndex("dense_idx").?.index.stats().active_count,
    );
    const zero_repair = try db.advanceIndexRepairIntent(alloc, zero_repair_id, .{});
    try std.testing.expect(zero_repair.repaired);
    try std.testing.expectEqual(
        @as(u64, 0),
        db.core.index_manager.denseIndex("dense_idx").?.index.stats().active_count,
    );
    try std.testing.expect(!try db.hasPendingIndexRepairIntents(alloc));
}

test "db derived async dense artifact rebuild dense artifact rebuild resumes from persisted state" {
    const DB = @import("mod.zig").DB;
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
    var io_impl = db_internal.threadedIo();
    defer io_impl.deinit();
    try std.Io.Dir.cwd().deleteTree(io_impl.io(), dense_index_path);

    {
        var interrupted = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer interrupted.close();

        const dense_entry = interrupted.core.index_manager.denseIndex("dense_idx") orelse return error.IndexNotFound;
        const rebuild_state = interrupted.core.index_manager.rebuildState(.dense_vector, dense_index_path, dense_entry.config);
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

        const dense_entry = resumed.core.index_manager.denseIndex("dense_idx") orelse return error.IndexNotFound;
        const rebuild_state = resumed.core.index_manager.rebuildState(.dense_vector, dense_index_path, dense_entry.config);
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

    var io_impl = db_internal.threadedIo();
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
    var io_impl = db_internal.threadedIo();
    defer io_impl.deinit();
    try std.Io.Dir.cwd().deleteTree(io_impl.io(), dense_index_path);

    {
        var interrupted = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer interrupted.close();

        const dense_entry = interrupted.core.index_manager.denseIndex("dv_v1") orelse return error.IndexNotFound;
        const rebuild_state = interrupted.core.index_manager.rebuildState(.dense_vector, dense_index_path, dense_entry.config);
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

        const dense_entry = resumed.core.index_manager.denseIndex("dv_v1") orelse return error.IndexNotFound;
        const rebuild_state = resumed.core.index_manager.rebuildState(.dense_vector, dense_index_path, dense_entry.config);
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

    var io_impl = db_internal.threadedIo();
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

    var io_impl = db_internal.threadedIo();
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

fn printDenseStreamingQualificationDiagnostics(db: anytype, docs_written: usize, memory: process_memory_mod.Stats) void {
    const print_lsm = struct {
        fn run(label: []const u8, stats: lsm_backend_mod.Backend.MaintenanceStats) void {
            std.debug.print(
                "dense_streaming_qualification_lsm owner={s} mutable_bytes={d} immutable_bytes={d} immutable_memtables={d} active_readers={d} active_bulk_sessions={d} snapshot_clone_calls={d} snapshot_clone_bytes_total={d} snapshot_clone_peak_bytes={d} current_scan_clone_bytes={d} current_scan_clone_peak_bytes={d}\n",
                .{
                    label,
                    stats.mutable_bytes,
                    stats.immutable_bytes,
                    stats.immutable_memtables,
                    stats.active_readers,
                    stats.active_bulk_ingest_batches,
                    stats.mutable_snapshot_clone_calls,
                    stats.mutable_snapshot_clone_bytes_total,
                    stats.mutable_snapshot_clone_peak_bytes,
                    stats.bulk_ingest_current_scan_clone_active_bytes,
                    stats.bulk_ingest_current_scan_clone_peak_active_bytes,
                },
            );
            for (stats.active_readers_by_kind, 0..) |count, kind_index| {
                if (count == 0) continue;
                const kind: lsm_backend_mod.ReaderPinKind = @enumFromInt(kind_index);
                std.debug.print(
                    "dense_streaming_qualification_lsm_reader owner={s} kind={s} count={d}\n",
                    .{ label, lsm_backend_mod.readerPinKindName(kind), count },
                );
            }
        }
    }.run;
    const target_sequence = db.core.nextDerivedSequence();
    const applied_sequence = db.executor.appliedSequence("dense_idx") orelse 0;
    std.debug.print(
        "dense_streaming_qualification_diagnostics docs_written={d} target_sequence={d} applied_sequence={d} lag_sequences={d} rss_bytes={d} working_set_bytes={d}\n",
        .{
            docs_written,
            target_sequence,
            applied_sequence,
            target_sequence -| applied_sequence,
            memory.resident_bytes,
            process_memory_mod.pressureWorkingSetBytes(memory),
        },
    );
    if (db.core.index_manager.denseIndex("dense_idx")) |dense| {
        const index_stats = dense.index.stats();
        const cache_stats = dense.index.hbcCacheStats();
        std.debug.print(
            "dense_streaming_qualification_index active={d} nodes={d} ordinal_cache={d} reverse_ordinal_cache={d} hbc_total_bytes={d} hbc_accounted_bytes={d} hbc_node_bytes={d} hbc_quantized_bytes={d} hbc_vector_bytes={d} hbc_metadata_bytes={d}\n",
            .{
                index_stats.active_count,
                index_stats.node_count,
                dense.ordinal_vector_ids.count(),
                dense.vector_ordinals.count(),
                cache_stats.total_bytes,
                cache_stats.accounted_bytes,
                cache_stats.node.used_bytes,
                cache_stats.quantized.used_bytes,
                cache_stats.vector.used_bytes,
                cache_stats.metadata.used_bytes,
            },
        );
        if (dense.index.snapshotLsmMaintenanceStats()) |stats| print_lsm("dense_idx", stats);
    }
    if (db.core.primary_store_owner.snapshotLsmMaintenanceStats()) |stats| print_lsm("primary", stats);
    if (db.core.index_manager.resource_manager) |manager| {
        const snapshot = manager.snapshot();
        for (snapshot.slices) |slice| {
            if (slice.used_bytes == 0 and slice.peak_bytes == 0) continue;
            std.debug.print(
                "dense_streaming_qualification_resource slice={s} used_bytes={d} peak_bytes={d} soft_limit_bytes={d} hard_limit_bytes={d} pressure={s}\n",
                .{ slice.name, slice.used_bytes, slice.peak_bytes, slice.soft_limit_bytes, slice.hard_limit_bytes, @tagName(slice.pressure) },
            );
        }
    }
}

test "db derived async dense streaming replay qualification survives full ingest and reopen" {
    const DB = @import("mod.zig").DB;
    if (platform.env.getenv("ANTFLY_RUN_DENSE_STREAMING_QUALIFICATION") == null) return error.SkipZigTest;
    const alloc = if (comptime builtin.link_libc) std.heap.c_allocator else std.testing.allocator;
    const doc_count = @max(@as(usize, 1), readEnvUsize("ANTFLY_DENSE_STREAMING_QUALIFICATION_DOCS", 100_000));
    const batch_docs = @max(@as(usize, 1), readEnvUsize("ANTFLY_DENSE_STREAMING_QUALIFICATION_BATCH_DOCS", 1_000));
    const max_rss_bytes = readEnvU64("ANTFLY_DENSE_STREAMING_QUALIFICATION_MAX_RSS_BYTES", 0);
    const max_working_set_bytes = readEnvU64("ANTFLY_DENSE_STREAMING_QUALIFICATION_MAX_WORKING_SET_BYTES", 2 * 1024 * 1024 * 1024);
    var peak_rss_bytes: u64 = 0;
    var peak_working_set_bytes: u64 = 0;
    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const started_ns = platform.time.monotonicNs();
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":8,\"metric\":\"l2_squared\",\"external\":true}",
        });

        var first: usize = 0;
        while (first < doc_count) : (first += batch_docs) {
            const count = @min(batch_docs, doc_count - first);
            const writes = try alloc.alloc(types.BatchWrite, count);
            var initialized: usize = 0;
            errdefer {
                for (writes[0..initialized]) |write| {
                    alloc.free(write.key);
                    alloc.free(write.value);
                }
                alloc.free(writes);
            }
            for (writes, 0..) |*write, offset| {
                const doc_id = first + offset;
                const key = try std.fmt.allocPrint(alloc, "doc:{d}", .{doc_id});
                errdefer alloc.free(key);
                const value = try std.fmt.allocPrint(
                    alloc,
                    "{{\"title\":\"doc-{d}\",\"_embeddings\":{{\"dense_idx\":[{d},{d},{d},{d},{d},{d},{d},{d}]}}}}",
                    .{
                        doc_id,
                        (doc_id *% 17 +% 3) % 997,
                        (doc_id *% 31 +% 5) % 991,
                        (doc_id *% 43 +% 7) % 983,
                        (doc_id *% 59 +% 11) % 977,
                        (doc_id *% 71 +% 13) % 971,
                        (doc_id *% 83 +% 17) % 967,
                        (doc_id *% 97 +% 19) % 953,
                        (doc_id *% 109 +% 23) % 947,
                    },
                );
                write.* = .{ .key = key, .value = value };
                initialized += 1;
            }
            try db.batch(.{ .writes = writes, .sync_level = .write });
            for (writes) |write| {
                alloc.free(write.key);
                alloc.free(write.value);
            }
            alloc.free(writes);

            const memory = process_memory_mod.pressureSnapshot();
            peak_rss_bytes = @max(peak_rss_bytes, memory.resident_bytes);
            const working_set_bytes = process_memory_mod.pressureWorkingSetBytes(memory);
            peak_working_set_bytes = @max(peak_working_set_bytes, working_set_bytes);
            if (memory.available and max_rss_bytes > 0 and memory.resident_bytes > max_rss_bytes) {
                printDenseStreamingQualificationDiagnostics(&db, first + count, memory);
                return error.ResourceBudgetExceeded;
            }
            if (memory.available and max_working_set_bytes > 0 and working_set_bytes > max_working_set_bytes) {
                printDenseStreamingQualificationDiagnostics(&db, first + count, memory);
                return error.ResourceBudgetExceeded;
            }
        }

        const target_sequence = db.core.nextDerivedSequence();
        try db.executor.waitForAll(target_sequence);
        const replay_memory = process_memory_mod.pressureSnapshot();
        peak_rss_bytes = @max(peak_rss_bytes, replay_memory.resident_bytes);
        const replay_working_set_bytes = process_memory_mod.pressureWorkingSetBytes(replay_memory);
        peak_working_set_bytes = @max(peak_working_set_bytes, replay_working_set_bytes);
        if (replay_memory.available and max_rss_bytes > 0 and replay_memory.resident_bytes > max_rss_bytes) {
            printDenseStreamingQualificationDiagnostics(&db, doc_count, replay_memory);
            return error.ResourceBudgetExceeded;
        }
        if (replay_memory.available and max_working_set_bytes > 0 and replay_working_set_bytes > max_working_set_bytes) {
            printDenseStreamingQualificationDiagnostics(&db, doc_count, replay_memory);
            return error.ResourceBudgetExceeded;
        }
        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(@as(u64, @intCast(doc_count)), stats.indexes[0].doc_count);
        try std.testing.expectEqual(target_sequence, stats.indexes[0].replay_applied_sequence);
        try db.core.index_manager.syncAll(true);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();
    try std.testing.expect(reopened.core.index_manager.loadFailure("dense_idx") == null);
    const reopened_stats = try reopened.stats(alloc);
    defer types.freeDBStats(alloc, reopened_stats);
    try std.testing.expectEqual(@as(u64, @intCast(doc_count)), reopened_stats.indexes[0].doc_count);
    var result = try reopened.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{
            .vector = &.{ 3, 5, 7, 11, 13, 17, 19, 23 },
            .k = 1,
        } },
        .limit = 1,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:0", result.hits[0].id);
    std.debug.print(
        "dense_streaming_qualification docs={d} batch_docs={d} elapsed_ms={d} peak_rss_bytes={d} peak_working_set_bytes={d}\n",
        .{ doc_count, batch_docs, @divTrunc(platform.time.monotonicNs() - started_ns, std.time.ns_per_ms), peak_rss_bytes, peak_working_set_bytes },
    );
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
