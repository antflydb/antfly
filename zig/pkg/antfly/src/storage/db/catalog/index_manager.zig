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
const Allocator = std.mem.Allocator;
const fs_paths = @import("../../../common/fs_paths.zig");
const platform_time = @import("../../../platform/time.zig");
const apply_rw_lock_mod = @import("../apply_rw_lock.zig");
const backend_types = @import("../../backend_types.zig");
const backend_erased = @import("../../backend_erased.zig");
const backend_scan = @import("../../backend_scan.zig");
const types = @import("../types.zig");
const doc_identity = @import("../doc_identity.zig");
const apply_state = @import("../derived/apply_state.zig");
const change_journal_mod = @import("../derived/change_journal.zig");
const derived_types = @import("../derived/derived_types.zig");
const internal_keys = @import("../../internal_keys.zig");
const enrichment_catalog = @import("enrichment_catalog.zig");
const enrichment_types = @import("../enrichment/enrichment_types.zig");
const enrichment_artifact_codec = @import("../enrichment/artifact_codec.zig");
const backfill_state_mod = @import("../backfill_state.zig");
const db_config = @import("../config.zig");
const persistent_mod = @import("../../persistent.zig");
const lsm_backend_mod = @import("../../lsm_backend/mod.zig");
const resource_manager_mod = @import("../../resource_manager.zig");
const background_runtime_mod = @import("../../background_runtime.zig");
const docstore_mod = @import("../../docstore.zig");
const schema_mod = @import("../../schema.zig");
const ttl_mod = @import("../../ttl.zig");
const lmdb = @import("../../lmdb.zig");
const mapper = @import("../document_mapper.zig");
const merger_mod = @import("../../../merger.zig");
const index_mod = @import("../../../index.zig");
const text_index_maintenance = @import("text_index_maintenance.zig");
const hbc_mod = @import("../../hbc_adapter.zig");
const sparse_mod = if (builtin.os.tag == .freestanding)
    @import("../sparse_stub.zig")
else
    @import("../../../sparse/sparse.zig");
const algebraic_mod = @import("../algebraic/mod.zig");
const vector_mod = @import("antfly_vector").vector;
const graph_mod = @import("../../../graph/graph.zig");
const segment_mod = @import("../../../segment.zig");
const chunking_types = @import("../../../chunking/types.zig");
const roaring = @import("../../../encoding/roaring.zig");
const introducer_mod = @import("../../../introducer.zig");
const sim_fixture = @import("../../sim_fixture.zig");
const storage_sim = @import("../../sim_runtime.zig");
const index_manager_sim_fixture = @import("index_manager_sim_fixture.zig");
const zig_lmdb = if (builtin.is_test) @import("lmdb_engine") else struct {
    pub const is_zig_backend = false;
    pub const sim = struct {};
};

fn getenv(name: [*:0]const u8) ?[*:0]u8 {
    if (!builtin.link_libc) return null;
    return std.c.getenv(name);
}

const index_catalog_key = "\x00\x00__metadata__:indexes";
const enrichment_catalog_key = "\x00\x00__metadata__:enrichments";
const text_field_analyzers_prefix = "\x00\x00__metadata__:text_field_analyzers:";
var bench_hbc_tree_counter: platform.atomic.Value(u64) = .init(0);
var hbc_coalesce_bulk_writes_cache: std.atomic.Value(u8) = .init(0);
var hbc_bulk_ingest_bulk_build_min_items_cache: std.atomic.Value(usize) = .init(0);
var hbc_bulk_rebuild_leaf_min_members_cache: std.atomic.Value(usize) = .init(0);
var hbc_defer_bulk_leaf_splits_cache: std.atomic.Value(u8) = .init(0);
var hbc_defer_bulk_quantized_rebuild_cache: std.atomic.Value(u8) = .init(0);
var bench_hbc_metrics_cache: std.atomic.Value(u8) = .init(0);
var sparse_replay_profile_enabled_cache: std.atomic.Value(u8) = .init(0);
const default_merge_policy = merger_mod.MergePolicy{
    .max_segments_per_tier = 10,
    .max_segment_size = 256 * 1024 * 1024,
    .floor_segment_size = 16 * 1024,
};
const force_merge_max_segments_at_once = 16;

const text_backfill_batch_size: usize = 1024;
const text_merge_scheduler_default_steps: usize = 1;
const text_merge_quarantine_backoff_ns: u64 = 30 * std.time.ns_per_s;
pub var test_abort_text_backfill_after_batches: ?usize = null;
const sparse_backfill_batch_size: usize = 1024;
pub var test_sparse_backfill_batch_size: ?usize = null;
pub var test_abort_sparse_backfill_after_batches: ?usize = null;

pub const ManagedIndexRef = struct {
    name: []const u8,
    kind: types.IndexKind,
};

pub const IndexBatchOptions = struct {
    compact_text: bool = true,
    compact_text_segment_threshold: ?usize = null,
    defer_text_compaction: bool = false,
};

const max_text_projection_docs_per_segment_build: usize = 32 * 1024;

const TextBatchMutationStats = struct {
    indexed_any: bool = false,
    deleted_any: bool = false,

    fn noteIndex(self: *TextBatchMutationStats, indexed_any: bool) void {
        self.indexed_any = self.indexed_any or indexed_any;
    }

    fn noteDelete(self: *TextBatchMutationStats, deleted_any: bool) void {
        self.deleted_any = self.deleted_any or deleted_any;
    }

    fn touched(self: TextBatchMutationStats) bool {
        return self.indexed_any or self.deleted_any;
    }
};

const ForceTextCompactMode = enum {
    force,
    best_effort,
};

const ForceTextCompactOptions = struct {
    mode: ForceTextCompactMode = .force,
};

const TextMergeScheduler = struct {
    const InFlightMerge = struct {
        index_name: []u8,
        segment_ids: []u64,

        fn deinit(self: *InFlightMerge, alloc: Allocator) void {
            alloc.free(self.index_name);
            alloc.free(self.segment_ids);
            self.* = undefined;
        }
    };

    const QuarantinedMerge = struct {
        index_name: []u8,
        segment_ids: []u64,
        error_name: []u8,
        failures: u64 = 1,
        quarantined_at_ns: u64,
        retry_after_ns: u64,

        fn deinit(self: *QuarantinedMerge, alloc: Allocator) void {
            alloc.free(self.index_name);
            alloc.free(self.segment_ids);
            alloc.free(self.error_name);
            self.* = undefined;
        }
    };

    next_index: usize = 0,
    in_flight: std.ArrayListUnmanaged(InFlightMerge) = .empty,
    quarantined: std.ArrayListUnmanaged(QuarantinedMerge) = .empty,
    completed_merges: u64 = 0,
    skipped_stale_merges: u64 = 0,
    failed_merges: u64 = 0,
    deferred_for_pressure: u64 = 0,

    fn deinit(self: *TextMergeScheduler, alloc: Allocator) void {
        for (self.in_flight.items) |*merge| merge.deinit(alloc);
        self.in_flight.deinit(alloc);
        for (self.quarantined.items) |*merge| merge.deinit(alloc);
        self.quarantined.deinit(alloc);
        self.* = undefined;
    }

    fn schedule(entry: *IndexManager.TextIndex) void {
        entry.compaction_pending = true;
    }

    fn noteComplete(entry: *IndexManager.TextIndex) void {
        entry.compaction_pending = false;
    }

    fn select(self: *TextMergeScheduler, entries: []IndexManager.TextIndex) ?usize {
        if (entries.len == 0) return null;
        const start = if (self.next_index < entries.len) self.next_index else 0;
        for (0..entries.len) |offset| {
            const idx = (start + offset) % entries.len;
            if (entries[idx].compaction_pending) {
                self.next_index = (idx + 1) % entries.len;
                return idx;
            }
        }
        self.next_index = start;
        return null;
    }

    fn indexHasInFlight(self: *const TextMergeScheduler, index_name: []const u8) bool {
        for (self.in_flight.items) |merge| {
            if (std.mem.eql(u8, merge.index_name, index_name)) return true;
        }
        return false;
    }

    fn segmentInFlight(self: *const TextMergeScheduler, index_name: []const u8, segment_id: u64) bool {
        for (self.in_flight.items) |merge| {
            if (!std.mem.eql(u8, merge.index_name, index_name)) continue;
            for (merge.segment_ids) |id| {
                if (id == segment_id) return true;
            }
        }
        return false;
    }

    fn segmentQuarantined(self: *const TextMergeScheduler, index_name: []const u8, segment_id: u64, now_ns: u64) bool {
        for (self.quarantined.items) |merge| {
            if (merge.retry_after_ns <= now_ns) continue;
            if (!std.mem.eql(u8, merge.index_name, index_name)) continue;
            for (merge.segment_ids) |id| {
                if (id == segment_id) return true;
            }
        }
        return false;
    }

    fn indexHasActiveQuarantine(self: *const TextMergeScheduler, index_name: []const u8, now_ns: u64) bool {
        for (self.quarantined.items) |merge| {
            if (merge.retry_after_ns <= now_ns) continue;
            if (std.mem.eql(u8, merge.index_name, index_name)) return true;
        }
        return false;
    }

    fn register(self: *TextMergeScheduler, alloc: Allocator, index_name: []const u8, segment_ids: []const u64) !void {
        const owned_name = try alloc.dupe(u8, index_name);
        errdefer alloc.free(owned_name);
        const owned_ids = try alloc.dupe(u64, segment_ids);
        errdefer alloc.free(owned_ids);
        try self.in_flight.append(alloc, .{
            .index_name = owned_name,
            .segment_ids = owned_ids,
        });
    }

    fn registerSource(self: *TextMergeScheduler, alloc: Allocator, index_name: []const u8, source: []const IndexManager.TextMergeSourceSegment) !void {
        var ids = try alloc.alloc(u64, source.len);
        defer alloc.free(ids);
        for (source, 0..) |segment, i| ids[i] = segment.id;
        try self.register(alloc, index_name, ids);
    }

    fn complete(self: *TextMergeScheduler, alloc: Allocator, index_name: []const u8, segment_ids: []const u64) void {
        var i: usize = 0;
        while (i < self.in_flight.items.len) : (i += 1) {
            const merge = &self.in_flight.items[i];
            if (std.mem.eql(u8, merge.index_name, index_name) and std.mem.eql(u64, merge.segment_ids, segment_ids)) {
                merge.deinit(alloc);
                _ = self.in_flight.orderedRemove(i);
                return;
            }
        }
    }

    fn completeSource(self: *TextMergeScheduler, alloc: Allocator, index_name: []const u8, source: []const IndexManager.TextMergeSourceSegment) void {
        var i: usize = 0;
        while (i < self.in_flight.items.len) : (i += 1) {
            const merge = &self.in_flight.items[i];
            if (!std.mem.eql(u8, merge.index_name, index_name) or merge.segment_ids.len != source.len) continue;
            if (!sourceMatchesSegmentIds(source, merge.segment_ids)) continue;
            merge.deinit(alloc);
            _ = self.in_flight.orderedRemove(i);
            return;
        }
    }

    fn pruneExpiredQuarantines(self: *TextMergeScheduler, alloc: Allocator, now_ns: u64) void {
        var i: usize = 0;
        while (i < self.quarantined.items.len) {
            if (self.quarantined.items[i].retry_after_ns > now_ns) {
                i += 1;
                continue;
            }
            self.quarantined.items[i].deinit(alloc);
            _ = self.quarantined.orderedRemove(i);
        }
    }

    fn quarantineSource(
        self: *TextMergeScheduler,
        alloc: Allocator,
        index_name: []const u8,
        source: []const IndexManager.TextMergeSourceSegment,
        err: anyerror,
        now_ns: u64,
    ) !void {
        for (self.quarantined.items) |*merge| {
            if (!std.mem.eql(u8, merge.index_name, index_name) or merge.segment_ids.len != source.len) continue;
            if (!sourceMatchesSegmentIds(source, merge.segment_ids)) continue;
            alloc.free(merge.error_name);
            merge.error_name = try alloc.dupe(u8, @errorName(err));
            merge.failures += 1;
            merge.quarantined_at_ns = now_ns;
            merge.retry_after_ns = now_ns + text_merge_quarantine_backoff_ns;
            return;
        }

        const owned_name = try alloc.dupe(u8, index_name);
        errdefer alloc.free(owned_name);
        const owned_error = try alloc.dupe(u8, @errorName(err));
        errdefer alloc.free(owned_error);
        const segment_ids = try alloc.alloc(u64, source.len);
        errdefer alloc.free(segment_ids);
        for (source, 0..) |segment, i| segment_ids[i] = segment.id;

        try self.quarantined.append(alloc, .{
            .index_name = owned_name,
            .segment_ids = segment_ids,
            .error_name = owned_error,
            .quarantined_at_ns = now_ns,
            .retry_after_ns = now_ns + text_merge_quarantine_backoff_ns,
        });
    }

    fn inFlightSegmentCount(self: *const TextMergeScheduler) u64 {
        var total: u64 = 0;
        for (self.in_flight.items) |merge| total += merge.segment_ids.len;
        return total;
    }

    fn inFlightMergeCountForIndex(self: *const TextMergeScheduler, index_name: []const u8) u64 {
        var total: u64 = 0;
        for (self.in_flight.items) |merge| {
            if (std.mem.eql(u8, merge.index_name, index_name)) total += 1;
        }
        return total;
    }

    fn inFlightSegmentCountForIndex(self: *const TextMergeScheduler, index_name: []const u8) u64 {
        var total: u64 = 0;
        for (self.in_flight.items) |merge| {
            if (std.mem.eql(u8, merge.index_name, index_name)) total += merge.segment_ids.len;
        }
        return total;
    }

    fn quarantinedSegmentCount(self: *const TextMergeScheduler, now_ns: u64) u64 {
        var total: u64 = 0;
        for (self.quarantined.items) |merge| {
            if (merge.retry_after_ns <= now_ns) continue;
            total += merge.segment_ids.len;
        }
        return total;
    }

    fn quarantinedSegmentCountForIndex(self: *const TextMergeScheduler, index_name: []const u8, now_ns: u64) u64 {
        var total: u64 = 0;
        for (self.quarantined.items) |merge| {
            if (merge.retry_after_ns <= now_ns or !std.mem.eql(u8, merge.index_name, index_name)) continue;
            total += merge.segment_ids.len;
        }
        return total;
    }

    fn activeQuarantineCount(self: *const TextMergeScheduler, now_ns: u64) u64 {
        var total: u64 = 0;
        for (self.quarantined.items) |merge| {
            if (merge.retry_after_ns > now_ns) total += 1;
        }
        return total;
    }

    fn activeQuarantineCountForIndex(self: *const TextMergeScheduler, index_name: []const u8, now_ns: u64) u64 {
        var total: u64 = 0;
        for (self.quarantined.items) |merge| {
            if (merge.retry_after_ns > now_ns and std.mem.eql(u8, merge.index_name, index_name)) total += 1;
        }
        return total;
    }

    fn lastMergeError(self: *const TextMergeScheduler, now_ns: u64) []const u8 {
        var last_error: []const u8 = "";
        var last_time: u64 = 0;
        for (self.quarantined.items) |merge| {
            if (merge.retry_after_ns <= now_ns) continue;
            if (merge.quarantined_at_ns >= last_time) {
                last_time = merge.quarantined_at_ns;
                last_error = merge.error_name;
            }
        }
        return last_error;
    }

    fn lastMergeErrorForIndex(self: *const TextMergeScheduler, index_name: []const u8, now_ns: u64) []const u8 {
        var last_error: []const u8 = "";
        var last_time: u64 = 0;
        for (self.quarantined.items) |merge| {
            if (merge.retry_after_ns <= now_ns or !std.mem.eql(u8, merge.index_name, index_name)) continue;
            if (merge.quarantined_at_ns >= last_time) {
                last_time = merge.quarantined_at_ns;
                last_error = merge.error_name;
            }
        }
        return last_error;
    }

    fn retryAfterNs(self: *const TextMergeScheduler, now_ns: u64) u64 {
        var retry_after_ns: u64 = 0;
        for (self.quarantined.items) |merge| {
            if (merge.retry_after_ns <= now_ns) continue;
            if (retry_after_ns == 0 or merge.retry_after_ns < retry_after_ns) retry_after_ns = merge.retry_after_ns;
        }
        return retry_after_ns;
    }

    fn retryAfterNsForIndex(self: *const TextMergeScheduler, index_name: []const u8, now_ns: u64) u64 {
        var retry_after_ns: u64 = 0;
        for (self.quarantined.items) |merge| {
            if (merge.retry_after_ns <= now_ns or !std.mem.eql(u8, merge.index_name, index_name)) continue;
            if (retry_after_ns == 0 or merge.retry_after_ns < retry_after_ns) retry_after_ns = merge.retry_after_ns;
        }
        return retry_after_ns;
    }
};

fn sourceMatchesSegmentIds(source: []const IndexManager.TextMergeSourceSegment, segment_ids: []const u64) bool {
    if (source.len != segment_ids.len) return false;
    for (source, 0..) |segment, i| {
        if (segment.id != segment_ids[i]) return false;
    }
    return true;
}

pub const SyncProfile = struct {
    text_ns: u64 = 0,
    dense_ns: u64 = 0,
    sparse_ns: u64 = 0,
    graph_ns: u64 = 0,
    algebraic_ns: u64 = 0,

    pub fn totalNs(self: SyncProfile) u64 {
        return self.text_ns + self.dense_ns + self.sparse_ns + self.graph_ns + self.algebraic_ns;
    }
};

pub const OpenIndexProfile = struct {
    kind: types.IndexKind,
    name: []const u8,
    open_ns: u64 = 0,
    backfill_ns: u64 = 0,

    pub fn totalNs(self: OpenIndexProfile) u64 {
        return self.open_ns + self.backfill_ns;
    }
};

pub const TextSplitHandoff = struct {
    index_name: []u8,
    skip_doc_keys: std.StringHashMapUnmanaged(void),
    transferred_segments: usize,

    pub fn deinit(self: *TextSplitHandoff, alloc: Allocator) void {
        var it = self.skip_doc_keys.keyIterator();
        while (it.next()) |key| alloc.free(key.*);
        self.skip_doc_keys.deinit(alloc);
        alloc.free(self.index_name);
        self.* = undefined;
    }

    pub fn shouldSkip(self: *const TextSplitHandoff, key: []const u8) bool {
        return self.skip_doc_keys.contains(key);
    }
};

pub const SparseSplitHandoff = struct {
    index_name: []u8,
    skip_doc_keys: std.StringHashMapUnmanaged(void),
    transferred_docs: usize,
    select_docs_ns: u64 = 0,
    terms_ns: u64 = 0,
    commit_ns: u64 = 0,

    pub fn deinit(self: *SparseSplitHandoff, alloc: Allocator) void {
        var it = self.skip_doc_keys.keyIterator();
        while (it.next()) |key| alloc.free(key.*);
        self.skip_doc_keys.deinit(alloc);
        alloc.free(self.index_name);
        self.* = undefined;
    }

    pub fn shouldSkip(self: *const SparseSplitHandoff, key: []const u8) bool {
        return self.skip_doc_keys.contains(key);
    }
};

pub const DenseSplitHandoff = struct {
    index_name: []u8,
    skip_doc_keys: std.StringHashMapUnmanaged(void),
    transferred_docs: usize,
    stream_ns: u64 = 0,
    insert_ns: u64 = 0,
    insert_store_ns: u64 = 0,
    insert_tree_ns: u64 = 0,
    mapping_ns: u64 = 0,
    finalize_ns: u64 = 0,
    finalize_quantized_ns: u64 = 0,
    finalize_flush_ns: u64 = 0,
    finalize_commit_ns: u64 = 0,

    pub fn deinit(self: *DenseSplitHandoff, alloc: Allocator) void {
        var it = self.skip_doc_keys.keyIterator();
        while (it.next()) |key| alloc.free(key.*);
        self.skip_doc_keys.deinit(alloc);
        alloc.free(self.index_name);
        self.* = undefined;
    }

    pub fn shouldSkip(self: *const DenseSplitHandoff, key: []const u8) bool {
        return self.skip_doc_keys.contains(key);
    }
};

const SplitSide = enum {
    left,
    right,
};

pub const IndexManager = struct {
    alloc: Allocator,
    base_path: []u8,
    byte_range: docstore_mod.ByteRange,
    relaxed_split_durability: bool,
    text_main_backend: persistent_mod.MainBackend,
    text_lsm_storage: ?lsm_backend_mod.Storage,
    text_main_lsm_options: lsm_backend_mod.Options,
    text_wal_lsm_options: lsm_backend_mod.Options,
    dense_storage_backend: hbc_mod.StorageBackend,
    dense_lsm_storage: ?lsm_backend_mod.Storage,
    dense_lsm_options: lsm_backend_mod.Options,
    sparse_backend: sparse_mod.SparseBackend,
    sparse_lsm_storage: ?lsm_backend_mod.Storage,
    sparse_lsm_options: lsm_backend_mod.Options,
    graph_reverse_backend: graph_mod.ReverseBackend,
    graph_lsm_storage: ?lsm_backend_mod.Storage,
    graph_reverse_lsm_options: lsm_backend_mod.Options,
    lsm_cache: ?*lsm_backend_mod.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    // Background lane used by algebraic indexes to run HLL cardinality
    // maintenance off the foreground write path. Attached after construction
    // via attachHllMaintenance(); when null, maintenance runs inline.
    hll_maintenance_lane: ?background_runtime_mod.DurableJobLane = null,
    hll_maintenance_owner_id: u64 = 0,
    primary_store: ?*docstore_mod.DocStore,
    applied_sequence_checkpoint_path: ?[]const u8,
    catalog_mutex: apply_rw_lock_mod.ApplyRwLock = .{},
    load_parallelism: ?usize = null,
    full_text_pending_bytes_accounted: u64 = 0,
    text_indexes: std.ArrayListUnmanaged(TextIndex),
    text_merge_scheduler: TextMergeScheduler,
    dense_indexes: std.ArrayListUnmanaged(DenseIndex),
    sparse_indexes: std.ArrayListUnmanaged(SparseIndex),
    graph_indexes: std.ArrayListUnmanaged(GraphIndex),
    algebraic_indexes: std.ArrayListUnmanaged(AlgebraicIndex),
    enrichments: std.ArrayListUnmanaged(enrichment_catalog.EnrichmentConfig),
    cached_has_generated_enrichment_targets: std.atomic.Value(bool),
    status_only_index_configs: []types.IndexConfig,

    pub const TextIndex = struct {
        apply_mutex: *std.atomic.Mutex,
        config: types.IndexConfig,
        chunk_name: ?[]u8,
        text_analysis: introducer_mod.TextAnalysisConfig,
        observed_field_analyzers: []mapper.ObservedFieldAnalyzer = &.{},
        runtime_schema: ?schema_mod.TableSchema,
        rebuild_root_path: []u8,
        persistent: persistent_mod.PersistentIndex,
        compaction_pending: bool = false,
    };

    pub const AlgebraicIndex = struct {
        apply_mutex: *std.atomic.Mutex,
        config: types.IndexConfig,
        index: algebraic_mod.index.Index,
    };

    pub const TextMergeSourceSegment = struct {
        id: u64,
        deleted: ?roaring.RoaringBitmap = null,

        fn deinit(self: *TextMergeSourceSegment, alloc: Allocator) void {
            _ = alloc;
            if (self.deleted) |*deleted| deleted.deinit();
            self.* = undefined;
        }
    };

    pub const TextMergeTask = struct {
        index_name: []u8,
        source: []TextMergeSourceSegment,
        merge_indices: []usize,
        segments: []index_mod.SegmentEntry,
        buffer_reservation: ?resource_manager_mod.Reservation = null,

        pub fn deinit(self: *TextMergeTask, alloc: Allocator) void {
            if (self.buffer_reservation) |*reservation| reservation.release();
            for (self.segments) |*seg| {
                seg.reader.deinit();
                if (seg.deleted) |*deleted| deleted.deinit();
                seg.data.deinit(alloc);
            }
            alloc.free(self.segments);
            alloc.free(self.merge_indices);
            for (self.source) |*source| source.deinit(alloc);
            alloc.free(self.source);
            alloc.free(self.index_name);
            self.* = undefined;
        }
    };

    pub const TextMergeResult = struct {
        segments: [][]u8 = &.{},

        pub fn deinit(self: *TextMergeResult, alloc: Allocator) void {
            merger_mod.freeMergedSegments(alloc, self.segments);
            self.* = undefined;
        }
    };

    pub const DenseIndex = struct {
        apply_mutex: *std.atomic.Mutex,
        config: types.IndexConfig,
        field_name: []u8,
        dims: u32,
        metric: vector_mod.DistanceMetric,
        external: bool,
        chunk_name: ?[]u8,
        embedding_name: ?[]u8,
        index: hbc_mod.HBCIndex,
        vector_loader_context: ?*DenseVectorLoadContext = null,
        ordinal_vector_ids: std.AutoHashMapUnmanaged(doc_identity.DocOrdinal, u64) = .empty,
        vector_ordinals: std.AutoHashMapUnmanaged(u64, doc_identity.DocOrdinal) = .empty,
    };

    const DenseVectorLoadContext = struct {
        manager: *IndexManager,
        index_name: []u8,
        max_cached_vectors: usize,

        fn deinit(self: *DenseVectorLoadContext, alloc: Allocator) void {
            alloc.free(self.index_name);
            alloc.destroy(self);
        }
    };

    const DenseVectorLoadSession = struct {
        const ReadTxnKind = enum {
            probe,
            snapshot,
        };

        context: *DenseVectorLoadContext,
        read_txn: ?docstore_mod.DocStore.Txn = null,
        read_txn_kind: ReadTxnKind = .probe,
        txn_override: ?docstore_mod.DocStore.Batch.BatchTxn = null,
        raw_cache: std.StringHashMapUnmanaged([]const u8) = .empty,
        vector_cache: std.AutoHashMapUnmanaged(u64, []f32) = .empty,
        raw_cache_hits: u64 = 0,
        raw_cache_misses: u64 = 0,
        raw_cache_key_bytes: u64 = 0,
        raw_read_value_bytes: u64 = 0,
        vector_cache_hits: u64 = 0,
        vector_cache_misses: u64 = 0,
        vector_cache_bytes: u64 = 0,
        working_bytes_current: u64 = 0,
        working_slice: resource_manager_mod.Slice = .dense_apply_working_set,
        recycle_raw_reads: bool = true,
        cache_raw_values: bool = true,
        cache_vectors: bool = true,

        const DefaultRawReadLimitBytes: u64 = 32 * 1024 * 1024;
        const MaxRawReadLimitBytes: u64 = 64 * 1024 * 1024;

        fn deinit(self: *@This()) void {
            self.context.manager.observeDenseWorkingBytes(self.working_slice, &self.working_bytes_current, 0);
            if (getenv("ANTFLY_DEBUG_DENSE_VECTOR_LOAD_SESSION") != null and self.raw_cache_hits + self.raw_cache_misses + self.vector_cache_hits + self.vector_cache_misses >= 128) {
                std.log.debug(
                    "dense vector load session raw_hits={} raw_misses={} vector_hits={} vector_misses={} cached_keys={} cached_vectors={} raw_key_bytes={} raw_value_bytes={} cached_vector_bytes={} index={s}",
                    .{
                        self.raw_cache_hits,
                        self.raw_cache_misses,
                        self.vector_cache_hits,
                        self.vector_cache_misses,
                        self.raw_cache.count(),
                        self.vector_cache.count(),
                        self.raw_cache_key_bytes,
                        self.raw_read_value_bytes,
                        self.vector_cache_bytes,
                        self.context.index_name,
                    },
                );
            }
            self.clearRawCacheKeys();
            self.raw_cache.deinit(self.context.manager.alloc);
            var vector_it = self.vector_cache.iterator();
            while (vector_it.next()) |entry| self.context.manager.alloc.free(entry.value_ptr.*);
            self.vector_cache.deinit(self.context.manager.alloc);
            if (self.read_txn) |*txn| txn.abort();
            self.* = undefined;
        }

        fn rawReadLimitBytes(self: *const @This()) u64 {
            const manager = self.context.manager.resource_manager orelse return DefaultRawReadLimitBytes;
            const stats = manager.sliceStats(self.working_slice);
            if (stats.hard_limit_bytes == 0) return DefaultRawReadLimitBytes;
            return @min(MaxRawReadLimitBytes, @max(@as(u64, 1), stats.hard_limit_bytes / 4));
        }

        fn rawWorkingBytes(self: *const @This()) u64 {
            return self.vector_cache_bytes +| self.raw_cache_key_bytes +| self.raw_read_value_bytes;
        }

        fn observeWorkingBytes(self: *@This()) void {
            self.context.manager.observeDenseWorkingBytes(self.working_slice, &self.working_bytes_current, self.rawWorkingBytes());
        }

        fn clearRawCacheKeys(self: *@This()) void {
            var it = self.raw_cache.keyIterator();
            while (it.next()) |key| self.context.manager.alloc.free(key.*);
            self.raw_cache.clearRetainingCapacity();
            self.raw_cache_key_bytes = 0;
        }

        fn recycleRawReadState(self: *@This()) void {
            self.clearRawCacheKeys();
            if (self.read_txn) |*txn| {
                txn.abort();
                self.read_txn = null;
            }
            self.raw_read_value_bytes = 0;
            self.observeWorkingBytes();
        }

        fn recycleRawReadStateIfNeeded(self: *@This()) void {
            if (!self.recycle_raw_reads) return;
            if (self.raw_cache_key_bytes +| self.raw_read_value_bytes >= self.rawReadLimitBytes()) {
                self.recycleRawReadState();
            }
        }

        fn getTxn(self: *@This(), store: *docstore_mod.DocStore) !*docstore_mod.DocStore.Txn {
            if (self.read_txn == null) {
                self.read_txn = switch (self.read_txn_kind) {
                    .probe => try store.beginProbeTxn(),
                    .snapshot => try store.beginReadTxn(),
                };
            }
            return &self.read_txn.?;
        }

        fn noteRawValueLoaded(self: *@This(), value: []const u8) void {
            if (self.txn_override == null) {
                self.raw_read_value_bytes +|= @intCast(value.len);
                self.observeWorkingBytes();
            }
        }

        fn maybeCacheRawValue(self: *@This(), key: []const u8, value: []const u8) !void {
            if (!self.cache_raw_values) return;
            if (self.raw_cache.contains(key)) return;
            const next_key_bytes = self.raw_cache_key_bytes +| @as(u64, @intCast(key.len));
            if (next_key_bytes +| self.raw_read_value_bytes > self.rawReadLimitBytes()) return;
            const owned_key = try self.context.manager.alloc.dupe(u8, key);
            errdefer self.context.manager.alloc.free(owned_key);
            try self.raw_cache.put(self.context.manager.alloc, owned_key, value);
            self.raw_cache_key_bytes = next_key_bytes;
            self.observeWorkingBytes();
        }

        fn get(self: *@This(), store: *docstore_mod.DocStore, key: []const u8) ![]const u8 {
            self.recycleRawReadStateIfNeeded();
            if (self.cache_raw_values) {
                if (self.raw_cache.get(key)) |cached| {
                    self.raw_cache_hits += 1;
                    return cached;
                }
            }
            self.raw_cache_misses += 1;
            const value = if (self.txn_override) |txn|
                try txn.get(key)
            else
                try (try self.getTxn(store)).get(key);
            self.noteRawValueLoaded(value);
            try self.maybeCacheRawValue(key, value);
            return value;
        }

        fn getManySorted(self: *@This(), store: *docstore_mod.DocStore, keys: []const []const u8, values: []?[]const u8) !void {
            if (keys.len != values.len) return error.InvalidArgument;
            if (keys.len == 0) return;
            self.recycleRawReadStateIfNeeded();

            const miss_keys = try self.context.manager.alloc.alloc([]const u8, keys.len);
            defer self.context.manager.alloc.free(miss_keys);
            const miss_indexes = try self.context.manager.alloc.alloc(usize, keys.len);
            defer self.context.manager.alloc.free(miss_indexes);

            var miss_count: usize = 0;
            for (keys, 0..) |key, i| {
                if (self.cache_raw_values) {
                    if (self.raw_cache.get(key)) |cached| {
                        self.raw_cache_hits += 1;
                        values[i] = cached;
                        continue;
                    }
                }
                self.raw_cache_misses += 1;
                miss_keys[miss_count] = key;
                miss_indexes[miss_count] = i;
                values[i] = null;
                miss_count += 1;
            }
            if (miss_count == 0) return;

            const miss_values = try self.context.manager.alloc.alloc(?[]const u8, miss_count);
            defer self.context.manager.alloc.free(miss_values);
            const debug_timing = getenv("ANTFLY_DEBUG_DENSE_VECTOR_LOAD_SESSION") != null;
            const txn_start_ns = if (debug_timing) platform_time.monotonicNs() else 0;
            var txn_opened = false;
            if (self.txn_override) |txn| {
                if (debug_timing) txn_opened = false;
                const read_start_ns = if (debug_timing) platform_time.monotonicNs() else 0;
                try txn.getManySorted(miss_keys[0..miss_count], miss_values);
                if (debug_timing and miss_count >= 32) {
                    std.log.debug(
                        "dense vector load batch index={s} keys={} txn_opened={} txn_us={} read_us={} kind=batch",
                        .{ self.context.index_name, miss_count, txn_opened, (read_start_ns - txn_start_ns) / 1000, (platform_time.monotonicNs() - read_start_ns) / 1000 },
                    );
                }
            } else {
                const had_txn = self.read_txn != null;
                const txn = try self.getTxn(store);
                txn_opened = !had_txn;
                const read_start_ns = if (debug_timing) platform_time.monotonicNs() else 0;
                try txn.getManySorted(miss_keys[0..miss_count], miss_values);
                if (debug_timing and miss_count >= 32) {
                    std.log.debug(
                        "dense vector load batch index={s} keys={} txn_opened={} txn_us={} read_us={} kind={s}",
                        .{
                            self.context.index_name,
                            miss_count,
                            txn_opened,
                            (read_start_ns - txn_start_ns) / 1000,
                            (platform_time.monotonicNs() - read_start_ns) / 1000,
                            switch (self.read_txn_kind) {
                                .probe => "probe",
                                .snapshot => "snapshot",
                            },
                        },
                    );
                }
            }
            for (miss_values[0..miss_count], 0..) |maybe_value, i| {
                const out_index = miss_indexes[i];
                values[out_index] = maybe_value;
                const value = maybe_value orelse continue;
                self.noteRawValueLoaded(value);
                try self.maybeCacheRawValue(miss_keys[i], value);
            }
        }

        fn getVector(self: *@This(), vector_id: u64) ?[]const f32 {
            if (self.vector_cache.get(vector_id)) |cached| {
                self.vector_cache_hits += 1;
                return cached;
            }
            self.vector_cache_misses += 1;
            return null;
        }

        fn cacheVector(self: *@This(), vector_id: u64, vector: []const f32) !void {
            if (self.vector_cache.contains(vector_id)) return;
            const max_cached_vectors = self.maxCachedVectors();
            if (max_cached_vectors == 0 or self.vector_cache.count() >= max_cached_vectors) return;
            const byte_len: u64 = @intCast(vector.len * @sizeOf(f32));
            const next_bytes = std.math.add(u64, self.vector_cache_bytes, byte_len) catch return;
            const next_working_bytes = next_bytes +| self.raw_cache_key_bytes +| self.raw_read_value_bytes;
            if (!self.context.manager.tryObserveDenseWorkingBytes(self.working_slice, &self.working_bytes_current, next_working_bytes)) return;
            errdefer self.observeWorkingBytes();

            const owned = try self.context.manager.alloc.dupe(f32, vector);
            errdefer self.context.manager.alloc.free(owned);
            try self.vector_cache.put(self.context.manager.alloc, vector_id, owned);
            self.vector_cache_bytes = next_bytes;
        }

        fn maxCachedVectors(self: *const @This()) usize {
            if (!self.cache_vectors) return 0;
            return self.context.max_cached_vectors;
        }
    };

    const DenseVectorMetadataPresenceMemo = struct {
        values: std.AutoHashMapUnmanaged(u64, bool) = .empty,
        metadata: std.AutoHashMapUnmanaged(u64, []const u8) = .empty,

        fn deinit(self: *@This(), alloc: Allocator) void {
            var it = self.metadata.valueIterator();
            while (it.next()) |value| alloc.free(value.*);
            self.values.deinit(alloc);
            self.metadata.deinit(alloc);
            self.* = undefined;
        }

        fn get(self: *const @This(), vector_id: u64) ?bool {
            return self.values.get(vector_id);
        }

        fn put(self: *@This(), alloc: Allocator, vector_id: u64, present: bool) !void {
            try self.values.put(alloc, vector_id, present);
        }

        fn getMetadata(self: *const @This(), vector_id: u64) ?[]const u8 {
            return self.metadata.get(vector_id);
        }

        fn notePresent(self: *@This(), alloc: Allocator, vector_id: u64, metadata: []const u8) !void {
            const owned = try alloc.alloc(u8, metadata.len);
            std.mem.copyForwards(u8, owned, metadata);
            errdefer alloc.free(owned);
            try self.values.put(alloc, vector_id, true);
            if (try self.metadata.fetchPut(alloc, vector_id, owned)) |existing| {
                if (existing.value.ptr != owned.ptr or existing.value.len != owned.len) {
                    alloc.free(existing.value);
                }
            }
        }

        fn noteAbsent(self: *@This(), alloc: Allocator, vector_id: u64) !void {
            try self.values.put(alloc, vector_id, false);
            if (self.metadata.fetchRemove(vector_id)) |existing| {
                alloc.free(existing.value);
            }
        }
    };

    threadlocal var active_dense_vector_load_session: ?*DenseVectorLoadSession = null;

    pub const SparseIndex = struct {
        apply_mutex: *std.atomic.Mutex,
        config: types.IndexConfig,
        field_name: []u8,
        chunk_name: ?[]u8,
        embedding_name: ?[]u8,
        rebuild_root_path: []u8,
        index: sparse_mod.SparseIndex,
    };

    pub const SparseCompactionTask = struct {
        index_name: []u8,
        chunk_size: u32,
        task: sparse_mod.SparseIndex.SegmentCompactionTask,

        pub fn deinit(self: *SparseCompactionTask, alloc: Allocator) void {
            self.task.deinit(alloc);
            alloc.free(self.index_name);
            self.* = undefined;
        }
    };

    pub const SparseCompactionResult = sparse_mod.SparseIndex.SegmentCompactionResult;

    pub const GraphIndex = struct {
        apply_mutex: *std.atomic.Mutex,
        config: types.IndexConfig,
        edge_type_configs: []graph_mod.EdgeTypeConfig,
        artifact_source: ?GraphArtifactSource = null,
        rebuild_root_path: []u8,
        index: graph_mod.GraphIndex,
    };

    const OpenedIndex = union(types.IndexKind) {
        full_text: TextIndex,
        dense_vector: DenseIndex,
        sparse_vector: SparseIndex,
        graph: GraphIndex,
        algebraic: AlgebraicIndex,

        fn deinit(self: *OpenedIndex, manager: *IndexManager) void {
            switch (self.*) {
                .full_text => |*entry| manager.freeTextIndexEntry(entry),
                .dense_vector => |*entry| manager.freeDenseIndexEntry(entry),
                .sparse_vector => |*entry| manager.freeSparseIndexEntry(entry),
                .graph => |*entry| manager.freeGraphIndexEntry(entry),
                .algebraic => |*entry| manager.freeAlgebraicIndexEntry(entry),
            }
            self.* = undefined;
        }
    };

    const OpenResult = struct {
        opened: ?OpenedIndex = null,
        err: ?anyerror = null,

        fn deinit(self: *OpenResult, manager: *IndexManager) void {
            if (self.opened) |*opened| opened.deinit(manager);
            self.* = .{};
        }
    };

    pub fn init(alloc: Allocator, base_path: []const u8) !IndexManager {
        return try initWithOptions(alloc, base_path, .{});
    }

    pub const ManagedIndexApplyGuard = struct {
        manager: *IndexManager,
        mutex: *std.atomic.Mutex,

        pub fn unlock(self: *ManagedIndexApplyGuard) void {
            self.mutex.unlock();
            self.manager.catalog_mutex.unlockShared();
            self.* = undefined;
        }
    };

    pub fn initWithOptions(alloc: Allocator, base_path: []const u8, opts: db_config.IndexBackendOptions) !IndexManager {
        return .{
            .alloc = alloc,
            .base_path = try alloc.dupe(u8, base_path),
            .byte_range = .{ .start = "", .end = "" },
            .relaxed_split_durability = false,
            .text_main_backend = opts.text_main_backend,
            .text_lsm_storage = opts.text_lsm_storage,
            .text_main_lsm_options = opts.text_main_lsm_options,
            .text_wal_lsm_options = opts.text_wal_lsm_options,
            .dense_storage_backend = opts.dense_storage_backend,
            .dense_lsm_storage = opts.dense_lsm_storage,
            .dense_lsm_options = opts.dense_lsm_options,
            .sparse_backend = opts.sparse_backend,
            .sparse_lsm_storage = opts.sparse_lsm_storage,
            .sparse_lsm_options = opts.sparse_lsm_options,
            .graph_reverse_backend = opts.graph_reverse_backend,
            .graph_lsm_storage = opts.graph_lsm_storage,
            .graph_reverse_lsm_options = opts.graph_reverse_lsm_options,
            .lsm_cache = opts.lsm_cache,
            .hbc_cache = opts.hbc_cache,
            .lsm_root_generation = opts.lsm_root_generation,
            .resource_manager = opts.resource_manager,
            .primary_store = null,
            .applied_sequence_checkpoint_path = null,
            .load_parallelism = null,
            .full_text_pending_bytes_accounted = 0,
            .text_indexes = .empty,
            .text_merge_scheduler = .{},
            .dense_indexes = .empty,
            .sparse_indexes = .empty,
            .graph_indexes = .empty,
            .algebraic_indexes = .empty,
            .enrichments = .empty,
            .cached_has_generated_enrichment_targets = .init(false),
            .status_only_index_configs = &.{},
        };
    }

    pub fn setRelaxedSplitDurability(self: *IndexManager, enabled: bool) void {
        self.relaxed_split_durability = enabled;
    }

    pub fn setAppliedSequenceCheckpointPath(self: *IndexManager, path: ?[]const u8) void {
        self.applied_sequence_checkpoint_path = path;
    }

    fn allocIndexApplyMutex(self: *IndexManager) !*std.atomic.Mutex {
        const mutex = try self.alloc.create(std.atomic.Mutex);
        mutex.* = .unlocked;
        return mutex;
    }

    fn destroyIndexApplyMutex(self: *IndexManager, mutex: *std.atomic.Mutex) void {
        mutex.* = undefined;
        self.alloc.destroy(mutex);
    }

    fn lockAtomicWithBackoff(mutex: *std.atomic.Mutex) void {
        var attempts: usize = 0;
        while (!mutex.tryLock()) : (attempts += 1) {
            if (builtin.os.tag == .freestanding or builtin.single_threaded) {
                std.atomic.spinLoopHint();
                continue;
            }
            if (attempts < 64) {
                std.atomic.spinLoopHint();
                continue;
            }
            std.Thread.yield() catch {};
        }
    }

    pub fn lockManagedIndexApply(self: *IndexManager, index_ref: ManagedIndexRef) !ManagedIndexApplyGuard {
        self.catalog_mutex.lockShared();
        errdefer self.catalog_mutex.unlockShared();
        const mutex = switch (index_ref.kind) {
            .full_text => blk: {
                const entry = self.textIndexEntry(index_ref.name) orelse return error.IndexNotFound;
                break :blk entry.apply_mutex;
            },
            .dense_vector => blk: {
                const entry = self.denseIndex(index_ref.name) orelse return error.IndexNotFound;
                break :blk entry.apply_mutex;
            },
            .sparse_vector => blk: {
                const entry = self.sparseIndex(index_ref.name) orelse return error.IndexNotFound;
                break :blk entry.apply_mutex;
            },
            .graph => blk: {
                const entry = self.graphIndex(index_ref.name) orelse return error.IndexNotFound;
                break :blk entry.apply_mutex;
            },
            .algebraic => blk: {
                const entry = self.algebraicIndex(index_ref.name) orelse return error.IndexNotFound;
                break :blk entry.apply_mutex;
            },
        };
        lockAtomicWithBackoff(mutex);
        return .{
            .manager = self,
            .mutex = mutex,
        };
    }

    pub fn setTextMainBackend(self: *IndexManager, backend: persistent_mod.MainBackend) void {
        self.text_main_backend = backend;
    }

    pub fn setTextLsmStorage(self: *IndexManager, storage: ?lsm_backend_mod.Storage) void {
        self.text_lsm_storage = storage;
    }

    pub fn setTextLsmOptions(self: *IndexManager, main_options: lsm_backend_mod.Options, wal_options: lsm_backend_mod.Options) void {
        self.text_main_lsm_options = main_options;
        self.text_wal_lsm_options = wal_options;
    }

    pub fn setDenseStorageBackend(self: *IndexManager, backend: hbc_mod.StorageBackend) void {
        self.dense_storage_backend = backend;
    }

    pub fn setDenseLsmStorage(self: *IndexManager, storage: ?lsm_backend_mod.Storage) void {
        self.dense_lsm_storage = storage;
    }

    pub fn setDenseLsmOptions(self: *IndexManager, options: lsm_backend_mod.Options) void {
        self.dense_lsm_options = options;
    }

    pub fn setGraphReverseBackend(self: *IndexManager, backend: graph_mod.ReverseBackend) void {
        self.graph_reverse_backend = backend;
    }

    pub fn setGraphLsmStorage(self: *IndexManager, storage: ?lsm_backend_mod.Storage) void {
        self.graph_lsm_storage = storage;
    }

    pub fn setGraphReverseLsmOptions(self: *IndexManager, options: lsm_backend_mod.Options) void {
        self.graph_reverse_lsm_options = options;
    }

    pub fn setLsmCache(self: *IndexManager, cache: ?*lsm_backend_mod.Cache) void {
        self.lsm_cache = cache;
    }

    pub fn setLoadParallelism(self: *IndexManager, parallelism: ?usize) void {
        self.load_parallelism = if (parallelism) |value| @max(value, 1) else null;
    }

    // Provide the background lane that algebraic indexes use for HLL cardinality
    // maintenance. Call before loading indexes so newly opened algebraic indexes
    // pick up the lane; already-open indexes are (re)attached here too.
    pub fn attachHllMaintenance(self: *IndexManager, lane: background_runtime_mod.DurableJobLane, owner_id: u64) void {
        self.hll_maintenance_lane = lane;
        self.hll_maintenance_owner_id = owner_id;
        for (self.algebraic_indexes.items) |*entry| {
            entry.index.attachHllMaintenanceLane(lane, owner_id);
        }
    }

    fn bindPrimaryStore(self: *IndexManager, store: anytype) void {
        const Store = @TypeOf(store);
        if (comptime Store == *docstore_mod.DocStore) {
            self.primary_store = store;
        }
    }

    fn freeTextIndexEntry(self: *IndexManager, entry: *TextIndex) void {
        entry.persistent.close();
        self.destroyIndexApplyMutex(entry.apply_mutex);
        if (entry.chunk_name) |chunk_name| self.alloc.free(chunk_name);
        introducer_mod.freeTextAnalysisConfig(self.alloc, entry.text_analysis);
        for (entry.observed_field_analyzers) |item| {
            self.alloc.free(item.field_name);
            self.alloc.free(item.analyzer_name);
        }
        if (entry.observed_field_analyzers.len > 0) self.alloc.free(entry.observed_field_analyzers);
        if (entry.runtime_schema) |schema| schema_mod.freeSchema(self.alloc, schema);
        self.alloc.free(entry.rebuild_root_path);
        entry.config.deinit(self.alloc);
    }

    /// Regenerate every algebraic index's schema-derived config from `schema_json`
    /// and apply it in place when the schema-derived capability actually changed.
    /// Carries forward user-tunable runtime knobs, and sets
    /// dynamic_rules_backfill_pending on a real change so query-time resolution of
    /// dynamic-template fields is withheld (those aggregations fall back to a
    /// complete scan instead of reading facts that only cover post-change docs)
    /// until the index is rebuilt. No-op when there are no algebraic indexes, no
    /// schema, or the capability fingerprint is unchanged.
    pub fn reloadAlgebraicSchemaConfigs(self: *IndexManager, schema_json: []const u8) !void {
        if (schema_json.len == 0) return;
        for (self.algebraic_indexes.items) |*entry| {
            const cur = entry.index.config();
            const new_config_json = try algebraic_mod.schema_capability.configJsonFromSchemaJsonAlloc(self.alloc, cur.table, schema_json);
            defer self.alloc.free(new_config_json);

            var new_parsed = try std.json.parseFromSlice(algebraic_mod.index.Config, self.alloc, new_config_json, .{ .allocate = .alloc_always });
            defer new_parsed.deinit();

            // Skip when the schema-derived capability is unchanged. Compare the
            // capability fingerprint (which encodes schema_version, fields, and
            // dynamic-template rules) rather than raw bytes: the live and durable
            // serializations differ in shape and tunable knobs, so a byte compare
            // would never short-circuit and every reconcile would churn the
            // live config.
            if (cur.capability_fingerprint.len > 0 and
                std.mem.eql(u8, new_parsed.value.capability_fingerprint, cur.capability_fingerprint)) continue;

            // Carry forward user-tunable runtime knobs (the durable regeneration
            // in api/tables.zig preserves the same set) so a schema/template
            // change does not silently reset planner/adaptive tuning in place.
            new_parsed.value.adaptive = cur.adaptive;
            new_parsed.value.pathfact_policy = cur.pathfact_policy;
            new_parsed.value.max_result_buckets = cur.max_result_buckets;
            new_parsed.value.max_planner_scan_rows = cur.max_planner_scan_rows;
            new_parsed.value.max_batch_accumulator_entries = cur.max_batch_accumulator_entries;
            new_parsed.value.min_max_candidate_cache_size = cur.min_max_candidate_cache_size;
            new_parsed.value.enable_temporal_range_pruning = cur.enable_temporal_range_pruning;

            // The capability changed against an already-open table, so existing
            // documents have not been re-projected through the new dynamic rules.
            // Mark dynamic-field resolution as backfill-pending so aggregations
            // over template-promoted fields fall back to a complete scan (correct
            // results) instead of reading facts that only cover docs written after
            // the change. Static fields keep accelerating. The durable
            // regeneration in api/tables.zig sets the same flag so it survives
            // reopen. (Table create takes the fingerprint-equality skip above, so
            // a freshly-built index is never flagged.)
            new_parsed.value.dynamic_rules_backfill_pending = true;

            const merged_json = try std.json.Stringify.valueAlloc(self.alloc, new_parsed.value, .{ .emit_null_optional_fields = false });
            defer self.alloc.free(merged_json);

            const owned = try self.alloc.dupe(u8, merged_json);
            errdefer self.alloc.free(owned);
            try entry.index.reloadConfigJson(merged_json);
            self.alloc.free(entry.config.config_json);
            entry.config.config_json = owned;
        }
    }

    fn freeAlgebraicIndexEntry(self: *IndexManager, entry: *AlgebraicIndex) void {
        entry.index.close();
        self.destroyIndexApplyMutex(entry.apply_mutex);
        entry.config.deinit(self.alloc);
        entry.* = undefined;
    }

    fn freeDenseIndexEntry(self: *IndexManager, entry: *DenseIndex) void {
        entry.index.close();
        self.destroyIndexApplyMutex(entry.apply_mutex);
        if (entry.vector_loader_context) |ctx| ctx.deinit(self.alloc);
        entry.ordinal_vector_ids.deinit(self.alloc);
        entry.vector_ordinals.deinit(self.alloc);
        self.alloc.free(entry.field_name);
        if (entry.chunk_name) |chunk_name| self.alloc.free(chunk_name);
        if (entry.embedding_name) |embedding_name| self.alloc.free(embedding_name);
        entry.config.deinit(self.alloc);
    }

    fn reopenDenseIndexStorage(self: *IndexManager, entry: *DenseIndex, path: []const u8) !void {
        const zpath = try self.alloc.dupeZ(u8, path);
        defer self.alloc.free(zpath);

        const dense_cfg = try parseDenseConfig(self.alloc, entry.config.config_json);
        defer dense_cfg.deinit(self.alloc);

        var index = try hbc_mod.HBCIndex.openWithLsmOptions(self.alloc, zpath, .{
            .storage_backend = self.dense_storage_backend,
            .dims = dense_cfg.dims,
            .metric = dense_cfg.metric,
            .split_algo = dense_cfg.split_algo,
            .search_width = dense_cfg.search_width,
            .epsilon = dense_cfg.epsilon,
            .branching_factor = dense_cfg.branching_factor,
            .leaf_size = dense_cfg.leaf_size,
            .bulk_build_algo = dense_cfg.bulk_build_algo,
            .kmeans_backend = dense_cfg.kmeans_backend,
            .kmeans_update_strategy = dense_cfg.kmeans_update_strategy,
            .use_quantization = dense_cfg.use_quantization,
            .rerank_policy = dense_cfg.rerank_policy,
            .quantizer_seed = dense_cfg.quantizer_seed,
            .use_random_ortho_trans = dense_cfg.use_random_ortho_trans,
            .max_cached_nodes = dense_cfg.max_cached_nodes,
            .max_cached_vectors = dense_cfg.max_cached_vectors,
            .max_cached_metadata = dense_cfg.max_cached_metadata,
            .lazy_posting_maintenance = dense_cfg.lazy_posting_maintenance,
            .auto_posting_maintenance_max_postings = dense_cfg.auto_posting_maintenance_max_postings,
            .centroid_directory_mode = dense_cfg.centroid_directory_mode,
            .flat_centroid_block_size = dense_cfg.flat_centroid_block_size,
            .flat_centroid_probe_count = dense_cfg.flat_centroid_probe_count,
            .no_sync = self.relaxed_split_durability,
            .no_meta_sync = self.relaxed_split_durability,
        }, .{
            .backend_options = self.dense_lsm_options,
            .storage = self.dense_lsm_storage,
            .cache = self.lsm_cache,
            .root_generation = self.lsm_root_generation,
        });
        errdefer index.close();

        if (self.hbc_cache) |cache| index.attachSharedCache(cache);
        if (self.resource_manager) |manager| index.attachResourceManager(manager);

        const vector_loader_context = try self.alloc.create(DenseVectorLoadContext);
        errdefer self.alloc.destroy(vector_loader_context);
        vector_loader_context.* = .{
            .manager = self,
            .index_name = try self.alloc.dupe(u8, entry.config.name),
            .max_cached_vectors = dense_cfg.max_cached_vectors,
        };
        errdefer {
            self.alloc.free(vector_loader_context.index_name);
            self.alloc.destroy(vector_loader_context);
        }
        index.setExternalVectorLoader(vector_loader_context, loadDenseVectorForHbc);
        index.setExternalVectorScratchLoader(vector_loader_context, loadDenseVectorForHbcIntoScratch);
        index.setExternalVectorBatchScratchLoader(vector_loader_context, loadDenseVectorsForHbcBatch);
        index.setExternalVectorBatchTransformedMatrixLoader(vector_loader_context, loadDenseVectorsForHbcBatchIntoTransformedMatrix);
        index.setExternalVectorBatchDistanceLoader(vector_loader_context, scoreDenseVectorsForHbcBatch);

        entry.index = index;
        entry.vector_loader_context = vector_loader_context;
    }

    pub fn resetDenseIndexForArtifactRebuild(self: *IndexManager, index_name: []const u8) !void {
        const entry = self.denseIndex(index_name) orelse return error.IndexNotFound;
        const path = try self.indexPath(index_name);
        defer self.alloc.free(path);

        entry.index.close();
        if (entry.vector_loader_context) |ctx| {
            ctx.deinit(self.alloc);
            entry.vector_loader_context = null;
        }

        deleteIndexDirIfPresent(path);
        entry.ordinal_vector_ids.clearRetainingCapacity();
        entry.vector_ordinals.clearRetainingCapacity();
        try self.reopenDenseIndexStorage(entry, path);
    }

    pub fn clearDenseHbcCaches(self: *IndexManager) void {
        for (self.dense_indexes.items) |*entry| entry.index.clearAllCaches();
    }

    fn freeSparseIndexEntry(self: *IndexManager, entry: *SparseIndex) void {
        entry.index.close();
        self.destroyIndexApplyMutex(entry.apply_mutex);
        self.alloc.free(entry.field_name);
        if (entry.chunk_name) |chunk_name| self.alloc.free(chunk_name);
        if (entry.embedding_name) |embedding_name| self.alloc.free(embedding_name);
        self.alloc.free(entry.rebuild_root_path);
        entry.config.deinit(self.alloc);
    }

    fn freeGraphIndexEntry(self: *IndexManager, entry: *GraphIndex) void {
        entry.index.close();
        self.destroyIndexApplyMutex(entry.apply_mutex);
        for (entry.edge_type_configs) |cfg| {
            self.alloc.free(cfg.name);
            if (cfg.field_name) |field_name| self.alloc.free(field_name);
        }
        self.alloc.free(entry.edge_type_configs);
        if (entry.artifact_source) |*source| source.deinit(self.alloc);
        self.alloc.free(entry.rebuild_root_path);
        entry.config.deinit(self.alloc);
    }

    pub fn deinit(self: *IndexManager) void {
        self.releaseFullTextPendingBytes();
        self.text_merge_scheduler.deinit(self.alloc);
        for (self.text_indexes.items) |*entry| {
            self.freeTextIndexEntry(entry);
        }
        for (self.dense_indexes.items) |*entry| {
            self.freeDenseIndexEntry(entry);
        }
        for (self.sparse_indexes.items) |*entry| {
            self.freeSparseIndexEntry(entry);
        }
        for (self.graph_indexes.items) |*entry| {
            self.freeGraphIndexEntry(entry);
        }
        for (self.algebraic_indexes.items) |*entry| {
            self.freeAlgebraicIndexEntry(entry);
        }
        self.clearStatusOnlyIndexConfigs();
        for (self.enrichments.items) |*entry| entry.deinit(self.alloc);
        self.text_indexes.deinit(self.alloc);
        self.dense_indexes.deinit(self.alloc);
        self.sparse_indexes.deinit(self.alloc);
        self.graph_indexes.deinit(self.alloc);
        self.algebraic_indexes.deinit(self.alloc);
        self.enrichments.deinit(self.alloc);
        self.alloc.free(self.base_path);
        self.* = undefined;
    }

    fn clearStatusOnlyIndexConfigs(self: *IndexManager) void {
        for (self.status_only_index_configs) |*cfg| cfg.deinit(self.alloc);
        if (self.status_only_index_configs.len > 0) self.alloc.free(self.status_only_index_configs);
        self.status_only_index_configs = &.{};
    }

    fn accountFullTextPendingBytes(self: *IndexManager, pending_bytes: u64) !void {
        const manager = self.resource_manager orelse return;
        try manager.adjustUsage(.full_text_pending_segments, &self.full_text_pending_bytes_accounted, pending_bytes);
    }

    fn releaseFullTextPendingBytes(self: *IndexManager) void {
        if (self.full_text_pending_bytes_accounted == 0) return;
        if (self.resource_manager) |manager| {
            manager.releaseBytes(.full_text_pending_segments, self.full_text_pending_bytes_accounted);
        }
        self.full_text_pending_bytes_accounted = 0;
    }

    fn observeDenseWorkingBytes(self: *IndexManager, slice: resource_manager_mod.Slice, current: *u64, next: u64) void {
        if (self.resource_manager) |manager| {
            manager.observeUsage(slice, current, next);
        } else {
            current.* = next;
        }
    }

    fn tryObserveDenseWorkingBytes(self: *IndexManager, slice: resource_manager_mod.Slice, current: *u64, next: u64) bool {
        if (self.resource_manager) |manager| {
            manager.adjustUsage(slice, current, next) catch return false;
        } else {
            current.* = next;
        }
        return true;
    }

    fn observeDenseApplyWorkingBytes(self: *IndexManager, current: *u64, next: u64) void {
        self.observeDenseWorkingBytes(.dense_apply_working_set, current, next);
    }

    pub fn updateRange(self: *IndexManager, byte_range: docstore_mod.ByteRange) void {
        self.byte_range = byte_range;
    }

    pub fn syncAll(self: *IndexManager, force: bool) !void {
        for (self.text_indexes.items) |*entry| try entry.persistent.sync(force);
        for (self.dense_indexes.items) |*entry| try entry.index.sync(force);
        for (self.sparse_indexes.items) |*entry| try entry.index.sync(force);
        for (self.graph_indexes.items) |*entry| try entry.index.sync(force);
        for (self.algebraic_indexes.items) |*entry| try entry.index.sync(force);
    }

    pub fn syncIndexByName(self: *IndexManager, name: []const u8, force: bool) !void {
        for (self.text_indexes.items) |*entry| {
            if (std.mem.eql(u8, entry.config.name, name)) {
                try entry.persistent.sync(force);
                return;
            }
        }
        for (self.dense_indexes.items) |*entry| {
            if (std.mem.eql(u8, entry.config.name, name)) {
                try entry.index.sync(force);
                return;
            }
        }
        for (self.sparse_indexes.items) |*entry| {
            if (std.mem.eql(u8, entry.config.name, name)) {
                try entry.index.sync(force);
                return;
            }
        }
        for (self.graph_indexes.items) |*entry| {
            if (std.mem.eql(u8, entry.config.name, name)) {
                try entry.index.sync(force);
                return;
            }
        }
        for (self.algebraic_indexes.items) |*entry| {
            if (std.mem.eql(u8, entry.config.name, name)) {
                try entry.index.sync(force);
                return;
            }
        }
        return error.IndexNotFound;
    }

    pub fn syncReplayStateByName(self: *IndexManager, store: *docstore_mod.DocStore, name: []const u8) !void {
        for (self.text_indexes.items) |*entry| {
            if (std.mem.eql(u8, entry.config.name, name)) {
                try entry.persistent.sync(false);
                return;
            }
        }
        for (self.dense_indexes.items) |*entry| {
            if (std.mem.eql(u8, entry.config.name, name)) {
                try entry.index.syncReplayState();
                return;
            }
        }
        for (self.sparse_indexes.items) |*entry| {
            if (std.mem.eql(u8, entry.config.name, name)) {
                try entry.index.syncReplayState();
                return;
            }
        }
        for (self.graph_indexes.items) |*entry| {
            if (std.mem.eql(u8, entry.config.name, name)) {
                try store.syncReplayState();
                try entry.index.syncReplayState();
                return;
            }
        }
        for (self.algebraic_indexes.items) |*entry| {
            if (std.mem.eql(u8, entry.config.name, name)) {
                try entry.index.sync(false);
                return;
            }
        }
        return error.IndexNotFound;
    }

    pub fn lsmMaintenanceScore(self: *const IndexManager) u64 {
        var score: u64 = 0;
        for (self.text_indexes.items) |*entry| {
            score = @max(score, entry.persistent.lsmMaintenanceScore());
        }
        for (self.dense_indexes.items) |*entry| {
            score = @max(score, entry.index.lsmMaintenanceScore());
        }
        return score;
    }

    fn lsmMaintenanceDebtHintUnlocked(self: *const IndexManager) u64 {
        var score: u64 = 0;
        for (self.text_indexes.items) |*entry| {
            score = @max(score, entry.persistent.lsmMaintenanceDebtHint());
        }
        for (self.dense_indexes.items) |*entry| {
            score = @max(score, entry.index.lsmMaintenanceDebtHint());
        }
        return score;
    }

    pub fn lsmMaintenanceDebtHint(self: *IndexManager) u64 {
        if (!self.catalog_mutex.tryLockShared()) return 1;
        defer self.catalog_mutex.unlockShared();
        return self.lsmMaintenanceDebtHintUnlocked();
    }

    pub fn refreshLsmMaintenanceDebtHint(self: *IndexManager) void {
        for (self.text_indexes.items) |*entry| {
            entry.persistent.refreshLsmMaintenanceDebtHint();
        }
        for (self.dense_indexes.items) |*entry| {
            entry.index.refreshLsmMaintenanceDebtHint();
        }
    }

    pub fn snapshotLsmMaintenanceStats(self: *const IndexManager) lsm_backend_mod.Backend.MaintenanceStats {
        var stats = lsm_backend_mod.Backend.MaintenanceStats{};
        for (self.text_indexes.items) |*entry| {
            if (entry.persistent.snapshotLsmMaintenanceStats()) |entry_stats| {
                lsm_backend_mod.Backend.accumulateMaintenanceStats(&stats, entry_stats);
            }
        }
        for (self.dense_indexes.items) |*entry| {
            if (entry.index.snapshotLsmMaintenanceStats()) |entry_stats| {
                lsm_backend_mod.Backend.accumulateMaintenanceStats(&stats, entry_stats);
            }
        }
        return stats;
    }

    pub fn snapshotLsmNativeStorageStats(self: *const IndexManager) lsm_backend_mod.NativeStorageStats {
        var stats = lsm_backend_mod.NativeStorageStats{};
        for (self.text_indexes.items) |*entry| {
            if (entry.persistent.snapshotLsmNativeStorageStats()) |entry_stats| {
                stats.fd_cache_entries +|= entry_stats.fd_cache_entries;
                stats.fd_cache_capacity +|= entry_stats.fd_cache_capacity;
            }
        }
        for (self.dense_indexes.items) |*entry| {
            if (entry.index.snapshotLsmNativeStorageStats()) |entry_stats| {
                stats.fd_cache_entries +|= entry_stats.fd_cache_entries;
                stats.fd_cache_capacity +|= entry_stats.fd_cache_capacity;
            }
        }
        return stats;
    }

    pub fn runLsmMaintenanceStep(self: *IndexManager) !bool {
        var best_kind: enum { none, text, dense } = .none;
        var best_index: usize = 0;
        var best_score: u64 = 0;

        for (self.text_indexes.items, 0..) |*entry, i| {
            const score = entry.persistent.lsmMaintenanceScore();
            if (score > best_score) {
                best_score = score;
                best_kind = .text;
                best_index = i;
            }
        }
        for (self.dense_indexes.items, 0..) |*entry, i| {
            const score = entry.index.lsmMaintenanceScore();
            if (score > best_score) {
                best_score = score;
                best_kind = .dense;
                best_index = i;
            }
        }

        if (best_score == 0) return false;
        return switch (best_kind) {
            .none => false,
            .text => try self.text_indexes.items[best_index].persistent.runLsmMaintenanceStep(),
            .dense => try self.dense_indexes.items[best_index].index.runLsmMaintenanceStep(),
        };
    }

    pub fn runLsmMaintenanceStepBestEffort(self: *IndexManager) !bool {
        if (!self.catalog_mutex.tryLockShared()) return false;
        defer self.catalog_mutex.unlockShared();

        var best_kind: enum { none, text, dense } = .none;
        var best_index: usize = 0;
        var best_score: u64 = 0;

        for (self.text_indexes.items, 0..) |*entry, i| {
            const score = entry.persistent.lsmMaintenanceDebtHint();
            if (score > best_score) {
                best_score = score;
                best_kind = .text;
                best_index = i;
            }
        }
        for (self.dense_indexes.items, 0..) |*entry, i| {
            const score = entry.index.lsmMaintenanceDebtHint();
            if (score > best_score) {
                best_score = score;
                best_kind = .dense;
                best_index = i;
            }
        }

        if (best_score == 0) return false;
        return switch (best_kind) {
            .none => false,
            .text => try self.text_indexes.items[best_index].persistent.runLsmMaintenanceStepBestEffort(),
            .dense => try self.dense_indexes.items[best_index].index.runLsmMaintenanceStepBestEffort(),
        };
    }

    pub fn runDenseLsmMaintenanceByName(self: *IndexManager, name: []const u8, max_steps: usize) !usize {
        const entry = self.denseIndex(name) orelse return error.IndexNotFound;
        var steps: usize = 0;
        while (steps < max_steps) {
            if (!try entry.index.runLsmMaintenanceStep()) break;
            steps += 1;
        }
        return steps;
    }

    pub const DensePostingMaintenanceOptions = struct {
        max_postings_per_index: usize = 64,
        max_layout_changes_per_index: usize = 8,
        max_boundary_reassignments_per_index: usize = 64,
        boundary_reassignment_min_improvement: f32 = 0.0,
    };

    pub fn runDensePostingMaintenance(self: *IndexManager, options: DensePostingMaintenanceOptions) !usize {
        var total_steps: usize = 0;
        for (self.dense_indexes.items) |*entry| {
            const backlog = try entry.index.postingBacklogStats();
            if (!backlog.needsRepair()) continue;

            const result = try entry.index.repairDirtyPostingsWithOptions(.{
                .max_postings = options.max_postings_per_index,
                .rebalance_layout = true,
                .max_layout_changes = options.max_layout_changes_per_index,
                .max_boundary_reassignments = options.max_boundary_reassignments_per_index,
                .boundary_reassignment_min_improvement = options.boundary_reassignment_min_improvement,
            });
            total_steps += @intCast(result.repaired_postings + result.split_postings + result.merged_postings + result.boundary_reassigned_vectors);
        }
        return total_steps;
    }

    pub fn denseLsmMaintenanceScoreByName(self: *IndexManager, name: []const u8) !u64 {
        const entry = self.denseIndex(name) orelse return error.IndexNotFound;
        return entry.index.lsmMaintenanceScore();
    }

    pub fn syncAllProfiled(self: *IndexManager, force: bool) !SyncProfile {
        var profile = SyncProfile{};

        const text_started = nowNs();
        for (self.text_indexes.items) |*entry| try entry.persistent.sync(force);
        profile.text_ns = elapsedSince(text_started);

        const dense_started = nowNs();
        for (self.dense_indexes.items) |*entry| try entry.index.sync(force);
        profile.dense_ns = elapsedSince(dense_started);

        const sparse_started = nowNs();
        for (self.sparse_indexes.items) |*entry| try entry.index.sync(force);
        profile.sparse_ns = elapsedSince(sparse_started);

        const graph_started = nowNs();
        for (self.graph_indexes.items) |*entry| try entry.index.sync(force);
        profile.graph_ns = elapsedSince(graph_started);

        return profile;
    }

    pub fn load(self: *IndexManager, store: anytype) !void {
        try self.loadWithBackfill(store, true, false);
    }

    pub fn loadNoBackfill(self: *IndexManager, store: anytype) !void {
        try self.loadWithBackfill(store, false, true);
    }

    fn loadWithBackfill(self: *IndexManager, store: anytype, allow_backfill: bool, read_only: bool) !void {
        const load_started_ns = nowNs();
        self.bindPrimaryStore(store);
        self.clearStatusOnlyIndexConfigs();
        try self.loadEnrichmentCatalog(store);

        var runtime_store = try initRuntimeStore(self.alloc, store);
        defer runtime_store.deinit();
        var txn = try runtime_store.store.beginProbe();
        defer txn.abort();
        const data = txn.get(index_catalog_key) catch |err| switch (err) {
            error.NotFound => {
                self.storeGeneratedEnrichmentTargetCache(try self.computeGeneratedEnrichmentTargetCache());
                return;
            },
            else => return err,
        };

        const configs = try deserializeCatalog(self.alloc, data);
        defer {
            for (configs) |*cfg| cfg.deinit(self.alloc);
            self.alloc.free(configs);
        }

        var parallelism: usize = 1;
        if (comptime builtin.single_threaded or builtin.os.tag == .freestanding) {
            for (configs) |cfg| {
                try self.openConfiguredIndex(store, cfg, allow_backfill, read_only);
            }
        } else {
            parallelism = self.resolvedLoadParallelism(configs.len);
            if (parallelism <= 1) {
                for (configs) |cfg| {
                    try self.openConfiguredIndex(store, cfg, allow_backfill, read_only);
                }
            } else {
                for (configs) |cfg| {
                    try self.ensureConfiguredIndexDir(cfg);
                }
                try self.loadConfiguredIndexesParallel(store, configs, parallelism, allow_backfill, read_only);
            }
        }
        try self.refreshGeneratedEnrichmentTargetCache();
        if (openProfileEnabled()) {
            std.log.info("index_manager_load_profile index_count={} parallelism={} total_ns={}", .{
                configs.len,
                parallelism,
                elapsedSince(load_started_ns),
            });
        }
    }

    pub fn loadCatalogOnly(self: *IndexManager, store: anytype) !void {
        self.bindPrimaryStore(store);
        try self.loadEnrichmentCatalog(store);

        var runtime_store = try initRuntimeStore(self.alloc, store);
        defer runtime_store.deinit();
        var txn = try runtime_store.store.beginRead();
        defer txn.abort();
        const data = txn.get(index_catalog_key) catch |err| switch (err) {
            error.NotFound => {
                self.clearStatusOnlyIndexConfigs();
                return;
            },
            else => return err,
        };

        const configs = try deserializeCatalog(self.alloc, data);
        errdefer {
            for (configs) |*cfg| cfg.deinit(self.alloc);
            self.alloc.free(configs);
        }
        self.clearStatusOnlyIndexConfigs();
        self.status_only_index_configs = configs;
    }

    fn resolvedLoadParallelism(self: *const IndexManager, config_count: usize) usize {
        if (config_count <= 1 or builtin.single_threaded) return 1;
        if (self.load_parallelism) |value| return @min(@max(value, 1), config_count);

        var parallelism = std.Thread.getCpuCount() catch 1;
        parallelism /= 2;
        if (parallelism == 0) parallelism = 1;
        parallelism = @min(parallelism, 4);
        return @min(parallelism, config_count);
    }

    fn loadConfiguredIndexesParallel(
        self: *IndexManager,
        store: anytype,
        configs: []const types.IndexConfig,
        parallelism: usize,
        allow_backfill: bool,
        read_only: bool,
    ) !void {
        const Store = @TypeOf(store);
        const WorkerState = struct {
            manager: *IndexManager,
            store: Store,
            configs: []const types.IndexConfig,
            results: []OpenResult,
            allow_backfill: bool,
            read_only: bool,
            next_index: std.atomic.Value(usize) = .init(0),

            fn run(state: *@This()) void {
                while (true) {
                    const index = state.next_index.fetchAdd(1, .monotonic);
                    if (index >= state.configs.len) return;
                    const opened = state.manager.openConfiguredIndexDetached(state.store, state.configs[index], state.allow_backfill, state.read_only) catch |err| {
                        std.log.warn("load configured index failed name={s} kind={s} err={s}", .{
                            state.configs[index].name,
                            @tagName(state.configs[index].kind),
                            @errorName(err),
                        });
                        state.results[index] = .{ .err = err };
                        continue;
                    };
                    state.results[index] = .{ .opened = opened };
                }
            }
        };

        const results = try self.alloc.alloc(OpenResult, configs.len);
        defer {
            for (results) |*result| result.deinit(self);
            self.alloc.free(results);
        }
        for (results) |*result| result.* = .{};

        var state = WorkerState{
            .manager = self,
            .store = store,
            .configs = configs,
            .results = results,
            .allow_backfill = allow_backfill,
            .read_only = read_only,
        };

        const spawned_count = parallelism - 1;
        var threads = try self.alloc.alloc(std.Thread, spawned_count);
        defer self.alloc.free(threads);

        var spawned: usize = 0;
        var threads_joined = false;
        errdefer {
            if (!threads_joined) {
                for (threads[0..spawned]) |*thread| thread.join();
            }
        }
        for (threads) |*thread| {
            thread.* = try std.Thread.spawn(.{}, WorkerState.run, .{&state});
            spawned += 1;
        }

        WorkerState.run(&state);

        for (threads[0..spawned]) |*thread| thread.join();
        threads_joined = true;

        for (results) |*result| {
            if (result.err) |err| return err;
        }

        for (results) |*result| {
            if (result.opened) |*opened| {
                try self.appendOpenedIndex(opened.*);
                result.opened = null;
            }
        }
    }

    pub fn add(self: *IndexManager, store: anytype, cfg: types.IndexConfig) !void {
        self.catalog_mutex.lockExclusive();
        defer self.catalog_mutex.unlockExclusive();
        self.bindPrimaryStore(store);
        if (self.has(cfg.name)) return error.IndexAlreadyExists;

        const enrichment_checkpoint = self.enrichments.items.len;
        var enrichment_catalog_committed = false;
        errdefer if (!enrichment_catalog_committed) self.truncateEnrichments(enrichment_checkpoint);

        const enrichments_changed = try self.ensureShorthandEnrichments(cfg);
        const has_generated_after_enrichments = if (enrichments_changed)
            try self.computeGeneratedEnrichmentTargetCache()
        else
            false;

        try self.openConfiguredIndex(store, cfg, true, false);
        errdefer {
            self.removeInMemory(cfg.name);
        }
        const has_generated_enrichment_targets = try self.computeGeneratedEnrichmentTargetCache();
        if (enrichments_changed) {
            try self.persistEnrichmentCatalog(store);
            enrichment_catalog_committed = true;
        }
        self.persistCatalog(store) catch |err| {
            self.removeInMemory(cfg.name);
            if (enrichment_catalog_committed) {
                self.storeGeneratedEnrichmentTargetCache(has_generated_after_enrichments);
            }
            return err;
        };
        self.storeGeneratedEnrichmentTargetCache(has_generated_enrichment_targets);
    }

    pub fn addAllNoBackfill(self: *IndexManager, store: anytype, configs: []const types.IndexConfig) !void {
        self.catalog_mutex.lockExclusive();
        defer self.catalog_mutex.unlockExclusive();
        self.bindPrimaryStore(store);
        if (configs.len == 0) return;

        var opened = std.ArrayListUnmanaged([]const u8).empty;
        defer opened.deinit(self.alloc);
        errdefer {
            for (opened.items) |name| self.removeInMemory(name);
        }

        const enrichment_checkpoint = self.enrichments.items.len;
        var enrichment_catalog_committed = false;
        errdefer if (!enrichment_catalog_committed) self.truncateEnrichments(enrichment_checkpoint);

        var enrichments_changed = false;
        for (configs, 0..) |cfg, i| {
            if (self.has(cfg.name)) return error.IndexAlreadyExists;
            for (configs[0..i]) |prior| {
                if (std.mem.eql(u8, prior.name, cfg.name)) return error.IndexAlreadyExists;
            }
            enrichments_changed = (try self.ensureShorthandEnrichments(cfg)) or enrichments_changed;
        }
        const has_generated_after_enrichments = if (enrichments_changed)
            try self.computeGeneratedEnrichmentTargetCache()
        else
            false;

        for (configs) |cfg| {
            try self.openConfiguredIndex(store, cfg, false, false);
            try opened.append(self.alloc, cfg.name);
        }

        const has_generated_enrichment_targets = try self.computeGeneratedEnrichmentTargetCache();
        if (enrichments_changed) {
            try self.persistEnrichmentCatalog(store);
            enrichment_catalog_committed = true;
        }
        self.persistCatalog(store) catch |err| {
            for (opened.items) |name| self.removeInMemory(name);
            if (enrichment_catalog_committed) {
                self.storeGeneratedEnrichmentTargetCache(has_generated_after_enrichments);
            }
            return err;
        };
        self.storeGeneratedEnrichmentTargetCache(has_generated_enrichment_targets);
    }

    pub fn registerShadowIndex(self: *IndexManager, store: anytype, cfg: types.IndexConfig) !void {
        self.catalog_mutex.lockExclusive();
        defer self.catalog_mutex.unlockExclusive();
        if (self.has(cfg.name)) return error.IndexAlreadyExists;
        try self.openConfiguredIndex(store, cfg, true, false);
        errdefer {
            self.removeInMemory(cfg.name);
        }
        try self.refreshGeneratedEnrichmentTargetCache();
    }

    pub fn addEnrichment(self: *IndexManager, store: anytype, cfg: types.EnrichmentConfig) !void {
        self.catalog_mutex.lockExclusive();
        defer self.catalog_mutex.unlockExclusive();
        var internal = try enrichmentFromPublic(self.alloc, cfg);
        defer internal.deinit(self.alloc);

        try validateEnrichmentConfig(self, internal);
        if (self.getEnrichment(internal.kind, internal.name) != null) return error.EnrichmentAlreadyExists;

        const enrichment_checkpoint = self.enrichments.items.len;
        errdefer self.truncateEnrichments(enrichment_checkpoint);

        try self.enrichments.append(self.alloc, try enrichment_catalog.EnrichmentConfig.clone(self.alloc, internal));
        const has_generated_enrichment_targets = try self.computeGeneratedEnrichmentTargetCache();
        try self.persistEnrichmentCatalog(store);
        self.storeGeneratedEnrichmentTargetCache(has_generated_enrichment_targets);
    }

    pub fn removeEnrichment(self: *IndexManager, store: anytype, kind: types.EnrichmentKind, name: []const u8) !bool {
        self.catalog_mutex.lockExclusive();
        defer self.catalog_mutex.unlockExclusive();
        const internal_kind = publicEnrichmentKindToInternal(kind);
        if (enrichmentInUse(self, internal_kind, name)) return error.EnrichmentInUse;

        for (self.enrichments.items, 0..) |*entry, i| {
            if (entry.kind != internal_kind) continue;
            if (!std.mem.eql(u8, entry.name, name)) continue;
            const has_generated_enrichment_targets = try self.computeGeneratedEnrichmentTargetCacheExcluding(
                null,
                .{ .kind = internal_kind, .name = name },
            );
            entry.deinit(self.alloc);
            _ = self.enrichments.orderedRemove(i);
            try self.persistEnrichmentCatalog(store);
            self.storeGeneratedEnrichmentTargetCache(has_generated_enrichment_targets);
            return true;
        }
        return false;
    }

    pub fn remove(self: *IndexManager, store: anytype, name: []const u8) !bool {
        self.catalog_mutex.lockExclusive();
        defer self.catalog_mutex.unlockExclusive();
        if (try self.removeStatusOnlyConfig(store, name)) return true;
        for (self.text_indexes.items, 0..) |*entry, i| {
            if (std.mem.eql(u8, entry.config.name, name)) {
                const index_path = try self.indexPath(name);
                defer self.alloc.free(index_path);
                const has_generated_enrichment_targets = try self.computeGeneratedEnrichmentTargetCacheExcluding(name, null);
                self.freeTextIndexEntry(entry);
                _ = self.text_indexes.orderedRemove(i);
                try self.persistCatalog(store);
                self.storeGeneratedEnrichmentTargetCache(has_generated_enrichment_targets);
                try apply_state.clearAppliedSequenceWithCheckpoint(self.alloc, store, self.applied_sequence_checkpoint_path, name);
                deleteIndexDirIfPresent(index_path);
                return true;
            }
        }
        for (self.dense_indexes.items, 0..) |*entry, i| {
            if (std.mem.eql(u8, entry.config.name, name)) {
                const index_path = try self.indexPath(name);
                defer self.alloc.free(index_path);
                const owned_chunk_name = if (entry.chunk_name) |chunk_name|
                    if (self.ownsGeneratedChunkArtifacts(name, chunk_name)) try self.alloc.dupe(u8, chunk_name) else null
                else
                    null;
                defer if (owned_chunk_name) |chunk_name| self.alloc.free(chunk_name);
                const owned_embedding_name = if (entry.embedding_name) |embedding_name|
                    if (self.ownsGeneratedEmbeddingArtifacts(name, embedding_name)) try self.alloc.dupe(u8, embedding_name) else null
                else
                    null;
                defer if (owned_embedding_name) |embedding_name| self.alloc.free(embedding_name);
                const has_generated_enrichment_targets = try self.computeGeneratedEnrichmentTargetCacheExcluding(name, null);
                self.freeDenseIndexEntry(entry);
                _ = self.dense_indexes.orderedRemove(i);
                try self.persistCatalog(store);
                self.storeGeneratedEnrichmentTargetCache(has_generated_enrichment_targets);
                try apply_state.clearAppliedSequenceWithCheckpoint(self.alloc, store, self.applied_sequence_checkpoint_path, name);
                try self.deleteDenseIndexMetadata(store, name);
                try self.deleteOwnedGeneratedArtifacts(store, owned_chunk_name, owned_embedding_name);
                deleteIndexDirIfPresent(index_path);
                return true;
            }
        }
        for (self.sparse_indexes.items, 0..) |*entry, i| {
            if (std.mem.eql(u8, entry.config.name, name)) {
                const index_path = try self.indexPath(name);
                defer self.alloc.free(index_path);
                const owned_chunk_name = if (entry.chunk_name) |chunk_name|
                    if (self.ownsGeneratedChunkArtifacts(name, chunk_name)) try self.alloc.dupe(u8, chunk_name) else null
                else
                    null;
                defer if (owned_chunk_name) |chunk_name| self.alloc.free(chunk_name);
                const owned_embedding_name = if (entry.embedding_name) |embedding_name|
                    if (self.ownsGeneratedEmbeddingArtifacts(name, embedding_name)) try self.alloc.dupe(u8, embedding_name) else null
                else
                    null;
                defer if (owned_embedding_name) |embedding_name| self.alloc.free(embedding_name);
                const has_generated_enrichment_targets = try self.computeGeneratedEnrichmentTargetCacheExcluding(name, null);
                self.freeSparseIndexEntry(entry);
                _ = self.sparse_indexes.orderedRemove(i);
                try self.persistCatalog(store);
                self.storeGeneratedEnrichmentTargetCache(has_generated_enrichment_targets);
                try apply_state.clearAppliedSequenceWithCheckpoint(self.alloc, store, self.applied_sequence_checkpoint_path, name);
                try self.deleteOwnedGeneratedArtifacts(store, owned_chunk_name, owned_embedding_name);
                deleteIndexDirIfPresent(index_path);
                return true;
            }
        }
        for (self.graph_indexes.items, 0..) |*entry, i| {
            if (std.mem.eql(u8, entry.config.name, name)) {
                const index_path = try self.indexPath(name);
                defer self.alloc.free(index_path);
                const has_generated_enrichment_targets = try self.computeGeneratedEnrichmentTargetCacheExcluding(name, null);
                self.freeGraphIndexEntry(entry);
                _ = self.graph_indexes.orderedRemove(i);
                try self.persistCatalog(store);
                self.storeGeneratedEnrichmentTargetCache(has_generated_enrichment_targets);
                try apply_state.clearAppliedSequenceWithCheckpoint(self.alloc, store, self.applied_sequence_checkpoint_path, name);
                deleteIndexDirIfPresent(index_path);
                return true;
            }
        }
        return false;
    }

    fn removeStatusOnlyConfig(self: *IndexManager, store: anytype, name: []const u8) !bool {
        for (self.status_only_index_configs, 0..) |cfg, i| {
            if (!std.mem.eql(u8, cfg.name, name)) continue;

            const index_path = try self.indexPath(name);
            defer self.alloc.free(index_path);
            var artifact_refs = try self.artifactRefsFromConfig(cfg);
            defer artifact_refs.deinit(self.alloc);
            const cleanup_artifacts = cfg.kind == .dense_vector or cfg.kind == .sparse_vector;
            const owned_chunk_name = if (cleanup_artifacts) blk: {
                const chunk_name = artifact_refs.chunk_name orelse break :blk null;
                if (try self.chunkArtifactsReferencedElsewhereIncludingStatusOnly(name, chunk_name)) break :blk null;
                break :blk try self.alloc.dupe(u8, chunk_name);
            } else null;
            defer if (owned_chunk_name) |chunk_name| self.alloc.free(chunk_name);
            const owned_embedding_name = if (cleanup_artifacts) blk: {
                const embedding_name = artifact_refs.embedding_name orelse break :blk null;
                if (try self.embeddingArtifactsReferencedElsewhereIncludingStatusOnly(name, embedding_name)) break :blk null;
                break :blk try self.alloc.dupe(u8, embedding_name);
            } else null;
            defer if (owned_embedding_name) |embedding_name| self.alloc.free(embedding_name);
            const replacement: []types.IndexConfig = if (self.status_only_index_configs.len > 1)
                try self.alloc.alloc(types.IndexConfig, self.status_only_index_configs.len - 1)
            else
                &.{};
            var out: usize = 0;
            var removed = cfg;
            for (self.status_only_index_configs, 0..) |existing, existing_i| {
                if (existing_i == i) continue;
                replacement[out] = existing;
                out += 1;
            }
            self.alloc.free(self.status_only_index_configs);
            self.status_only_index_configs = replacement;

            const has_generated_enrichment_targets = try self.computeGeneratedEnrichmentTargetCache();
            try self.persistCatalog(store);
            self.storeGeneratedEnrichmentTargetCache(has_generated_enrichment_targets);
            try apply_state.clearAppliedSequenceWithCheckpoint(self.alloc, store, self.applied_sequence_checkpoint_path, name);
            if (removed.kind == .dense_vector) try self.deleteDenseIndexMetadata(store, name);
            try self.deleteOwnedGeneratedArtifacts(store, owned_chunk_name, owned_embedding_name);
            deleteIndexDirIfPresent(index_path);
            removed.deinit(self.alloc);
            return true;
        }
        return false;
    }

    pub fn requiresEnrichmentReplay(self: *const IndexManager, name: []const u8) !bool {
        for (self.dense_indexes.items) |entry| {
            if (!std.mem.eql(u8, entry.config.name, name)) continue;
            return try self.configRequiresEnrichmentReplay(entry.config);
        }
        for (self.sparse_indexes.items) |entry| {
            if (!std.mem.eql(u8, entry.config.name, name)) continue;
            return try self.configRequiresEnrichmentReplay(entry.config);
        }
        return false;
    }

    const ExcludedEnrichment = struct {
        kind: enrichment_catalog.EnrichmentType,
        name: []const u8,
    };

    fn computeGeneratedEnrichmentTargetCache(self: *const IndexManager) !bool {
        return try self.computeGeneratedEnrichmentTargetCacheExcluding(null, null);
    }

    fn computeGeneratedEnrichmentTargetCacheExcluding(
        self: *const IndexManager,
        excluded_index_name: ?[]const u8,
        excluded_enrichment: ?ExcludedEnrichment,
    ) !bool {
        for (self.dense_indexes.items) |entry| {
            if (excluded_index_name) |index_name| {
                if (std.mem.eql(u8, entry.config.name, index_name)) continue;
            }
            if (try parseDenseGeneratorConfig(self.alloc, entry.config.config_json)) |generator| {
                generator.deinit(self.alloc);
                return true;
            }
            if (entry.embedding_name) |embedding_name| {
                if (self.getEnrichmentExcluding(.embedding, embedding_name, excluded_enrichment) != null) return true;
            }
        }
        for (self.sparse_indexes.items) |entry| {
            if (excluded_index_name) |index_name| {
                if (std.mem.eql(u8, entry.config.name, index_name)) continue;
            }
            if (try parseSparseGeneratorConfig(self.alloc, entry.config.config_json)) |generator| {
                generator.deinit(self.alloc);
                return true;
            }
        }
        for (self.enrichments.items) |entry| {
            if (excluded_enrichment) |skip| {
                if (entry.kind == skip.kind and std.mem.eql(u8, entry.name, skip.name)) continue;
            }
            if (entry.kind == .asset) return true;
        }
        return false;
    }

    fn refreshGeneratedEnrichmentTargetCache(self: *IndexManager) !void {
        self.storeGeneratedEnrichmentTargetCache(try self.computeGeneratedEnrichmentTargetCache());
    }

    fn storeGeneratedEnrichmentTargetCache(self: *IndexManager, value: bool) void {
        self.cached_has_generated_enrichment_targets.store(value, .release);
    }

    pub fn hasGeneratedEnrichmentTargets(self: *const IndexManager) bool {
        return self.cached_has_generated_enrichment_targets.load(.acquire);
    }

    pub fn has(self: *const IndexManager, name: []const u8) bool {
        return self.get(name) != null;
    }

    pub fn get(self: *const IndexManager, name: []const u8) ?*const types.IndexConfig {
        for (self.text_indexes.items) |*entry| {
            if (std.mem.eql(u8, entry.config.name, name)) return &entry.config;
        }
        for (self.dense_indexes.items) |*entry| {
            if (std.mem.eql(u8, entry.config.name, name)) return &entry.config;
        }
        for (self.sparse_indexes.items) |*entry| {
            if (std.mem.eql(u8, entry.config.name, name)) return &entry.config;
        }
        for (self.graph_indexes.items) |*entry| {
            if (std.mem.eql(u8, entry.config.name, name)) return &entry.config;
        }
        for (self.algebraic_indexes.items) |*entry| {
            if (std.mem.eql(u8, entry.config.name, name)) return &entry.config;
        }
        for (self.status_only_index_configs) |*cfg| {
            if (std.mem.eql(u8, cfg.name, name)) return cfg;
        }
        return null;
    }

    pub fn count(self: *const IndexManager) u32 {
        return @intCast(self.text_indexes.items.len + self.dense_indexes.items.len + self.sparse_indexes.items.len + self.graph_indexes.items.len + self.algebraic_indexes.items.len + self.status_only_index_configs.len);
    }

    pub fn listIndexesPublic(self: *const IndexManager, alloc: Allocator) ![]types.IndexConfig {
        const total = self.text_indexes.items.len + self.dense_indexes.items.len + self.sparse_indexes.items.len + self.graph_indexes.items.len + self.algebraic_indexes.items.len + self.status_only_index_configs.len;
        const out = try alloc.alloc(types.IndexConfig, total);
        var initialized: usize = 0;
        errdefer {
            for (out[0..initialized]) |*cfg| cfg.deinit(alloc);
            alloc.free(out);
        }

        for (self.text_indexes.items) |entry| {
            out[initialized] = try types.IndexConfig.clone(alloc, entry.config);
            initialized += 1;
        }
        for (self.dense_indexes.items) |entry| {
            out[initialized] = try types.IndexConfig.clone(alloc, entry.config);
            initialized += 1;
        }
        for (self.sparse_indexes.items) |entry| {
            out[initialized] = try types.IndexConfig.clone(alloc, entry.config);
            initialized += 1;
        }
        for (self.graph_indexes.items) |entry| {
            out[initialized] = try types.IndexConfig.clone(alloc, entry.config);
            initialized += 1;
        }
        for (self.algebraic_indexes.items) |entry| {
            out[initialized] = try types.IndexConfig.clone(alloc, entry.config);
            initialized += 1;
        }
        for (self.status_only_index_configs) |cfg| {
            out[initialized] = try types.IndexConfig.clone(alloc, cfg);
            initialized += 1;
        }

        return out;
    }

    pub fn hasManagedIndexes(self: *const IndexManager) bool {
        return self.text_indexes.items.len > 0 or self.dense_indexes.items.len > 0 or self.sparse_indexes.items.len > 0 or self.graph_indexes.items.len > 0 or self.algebraic_indexes.items.len > 0 or self.status_only_index_configs.len > 0;
    }

    pub fn managedIndexNames(self: *const IndexManager, alloc: Allocator) ![][]u8 {
        const total = self.text_indexes.items.len + self.dense_indexes.items.len + self.sparse_indexes.items.len + self.graph_indexes.items.len + self.algebraic_indexes.items.len + self.status_only_index_configs.len;
        var names = try alloc.alloc([]u8, total);
        var initialized: usize = 0;
        errdefer {
            for (names[0..initialized]) |name| alloc.free(name);
            alloc.free(names);
        }

        for (self.text_indexes.items) |entry| {
            names[initialized] = try alloc.dupe(u8, entry.config.name);
            initialized += 1;
        }
        for (self.dense_indexes.items) |entry| {
            names[initialized] = try alloc.dupe(u8, entry.config.name);
            initialized += 1;
        }
        for (self.sparse_indexes.items) |entry| {
            names[initialized] = try alloc.dupe(u8, entry.config.name);
            initialized += 1;
        }
        for (self.graph_indexes.items) |entry| {
            names[initialized] = try alloc.dupe(u8, entry.config.name);
            initialized += 1;
        }
        for (self.algebraic_indexes.items) |entry| {
            names[initialized] = try alloc.dupe(u8, entry.config.name);
            initialized += 1;
        }
        for (self.status_only_index_configs) |cfg| {
            names[initialized] = try alloc.dupe(u8, cfg.name);
            initialized += 1;
        }

        return names;
    }

    pub fn managedIndexes(self: *const IndexManager, alloc: Allocator) ![]ManagedIndexRef {
        const total = self.text_indexes.items.len + self.dense_indexes.items.len + self.sparse_indexes.items.len + self.graph_indexes.items.len + self.algebraic_indexes.items.len + self.status_only_index_configs.len;
        var refs = try alloc.alloc(ManagedIndexRef, total);
        var initialized: usize = 0;
        errdefer {
            for (refs[0..initialized]) |ref| alloc.free(@constCast(ref.name));
            alloc.free(refs);
        }

        for (self.text_indexes.items) |entry| {
            refs[initialized] = .{
                .name = try alloc.dupe(u8, entry.config.name),
                .kind = .full_text,
            };
            initialized += 1;
        }
        for (self.dense_indexes.items) |entry| {
            refs[initialized] = .{
                .name = try alloc.dupe(u8, entry.config.name),
                .kind = .dense_vector,
            };
            initialized += 1;
        }
        for (self.sparse_indexes.items) |entry| {
            refs[initialized] = .{
                .name = try alloc.dupe(u8, entry.config.name),
                .kind = .sparse_vector,
            };
            initialized += 1;
        }
        for (self.graph_indexes.items) |entry| {
            refs[initialized] = .{
                .name = try alloc.dupe(u8, entry.config.name),
                .kind = .graph,
            };
            initialized += 1;
        }
        for (self.algebraic_indexes.items) |entry| {
            refs[initialized] = .{
                .name = try alloc.dupe(u8, entry.config.name),
                .kind = .algebraic,
            };
            initialized += 1;
        }
        for (self.status_only_index_configs) |cfg| {
            refs[initialized] = .{
                .name = try alloc.dupe(u8, cfg.name),
                .kind = cfg.kind,
            };
            initialized += 1;
        }

        return refs;
    }

    pub fn getEnrichment(self: *const IndexManager, kind: enrichment_catalog.EnrichmentType, name: []const u8) ?*const enrichment_catalog.EnrichmentConfig {
        return self.getEnrichmentExcluding(kind, name, null);
    }

    fn getEnrichmentExcluding(
        self: *const IndexManager,
        kind: enrichment_catalog.EnrichmentType,
        name: []const u8,
        excluded: ?ExcludedEnrichment,
    ) ?*const enrichment_catalog.EnrichmentConfig {
        for (self.enrichments.items) |*entry| {
            if (excluded) |skip| {
                if (entry.kind == skip.kind and std.mem.eql(u8, entry.name, skip.name)) continue;
            }
            if (entry.kind == kind and std.mem.eql(u8, entry.name, name)) return entry;
        }
        return null;
    }

    pub fn getEnrichmentPublic(self: *const IndexManager, alloc: Allocator, kind: types.EnrichmentKind, name: []const u8) !?types.EnrichmentConfig {
        const internal_kind = publicEnrichmentKindToInternal(kind);
        const cfg = self.getEnrichment(internal_kind, name) orelse return null;
        return try enrichmentToPublic(alloc, cfg.*);
    }

    pub fn listEnrichmentsPublic(self: *const IndexManager, alloc: Allocator) ![]types.EnrichmentConfig {
        const out = try alloc.alloc(types.EnrichmentConfig, self.enrichments.items.len);
        var initialized: usize = 0;
        errdefer {
            for (out[0..initialized]) |*cfg| cfg.deinit(alloc);
            if (out.len > 0) alloc.free(out);
        }

        for (self.enrichments.items, 0..) |cfg, i| {
            out[i] = try enrichmentToPublic(alloc, cfg);
            initialized += 1;
        }
        return out;
    }

    pub fn planGeneratedEnrichments(
        self: *const IndexManager,
        alloc: Allocator,
        doc_key: []const u8,
        doc_value: []const u8,
        explicit_dense: []const mapper.DenseEmbeddingWrite,
        explicit_sparse: []const mapper.SparseEmbeddingWrite,
    ) ![]enrichment_types.GeneratedEnrichmentRequest {
        var requests = std.ArrayListUnmanaged(enrichment_types.GeneratedEnrichmentRequest).empty;
        errdefer enrichment_types.deinitGeneratedRequests(alloc, requests.items);

        for (self.dense_indexes.items) |entry| {
            if (hasExplicitDenseEmbedding(explicit_dense, entry.config.name)) continue;
            if (try mapper.extractDenseVectorField(alloc, doc_value, entry.field_name, entry.dims)) |vector| {
                alloc.free(vector);
                continue;
            }

            if (try parseDenseGeneratorConfig(alloc, entry.config.config_json)) |generator| {
                defer generator.deinit(alloc);
                const chunk_cfg = resolveChunkGenerator(self, generator);
                const embedding_name = entry.embedding_name orelse entry.config.name;
                if (generatorHasChunking(chunk_cfg) and !hasGeneratedChunkRequest(requests.items, doc_key, chunk_cfg.source_field, chunk_cfg.source_template, chunk_cfg.artifact_name)) {
                    try requests.append(alloc, .{
                        .kind = .chunk_text,
                        .index_name = try alloc.dupe(u8, entry.config.name),
                        .artifact_name = try alloc.dupe(u8, chunk_cfg.artifact_name),
                        .doc_key = try alloc.dupe(u8, doc_key),
                        .source_field = try alloc.dupe(u8, chunk_cfg.source_field),
                        .source_template = if (chunk_cfg.source_template.len > 0) try alloc.dupe(u8, chunk_cfg.source_template) else "",
                        .chunk_size = chunk_cfg.chunk_size,
                        .chunk_overlap = chunk_cfg.chunk_overlap,
                        .chunker_json = if (chunk_cfg.chunker_json.len > 0) try alloc.dupe(u8, chunk_cfg.chunker_json) else "",
                    });
                }
                if (!hasGeneratedDenseEmbeddingRequest(requests.items, doc_key, chunk_cfg.source_field, chunk_cfg.source_template, chunk_cfg.artifact_name, embedding_name)) {
                    try requests.append(alloc, .{
                        .kind = .dense_embedding,
                        .index_name = try alloc.dupe(u8, entry.config.name),
                        .artifact_name = try alloc.dupe(u8, chunk_cfg.artifact_name),
                        .embedding_name = try alloc.dupe(u8, embedding_name),
                        .doc_key = try alloc.dupe(u8, doc_key),
                        .source_field = try alloc.dupe(u8, chunk_cfg.source_field),
                        .source_template = if (chunk_cfg.source_template.len > 0) try alloc.dupe(u8, chunk_cfg.source_template) else "",
                        .expected_dims = entry.dims,
                        .chunk_size = chunk_cfg.chunk_size,
                        .chunk_overlap = chunk_cfg.chunk_overlap,
                        .chunker_json = if (chunk_cfg.chunker_json.len > 0) try alloc.dupe(u8, chunk_cfg.chunker_json) else "",
                    });
                }
            } else if (entry.embedding_name) |embedding_name| {
                const embedding_cfg = self.getEnrichment(.embedding, embedding_name) orelse continue;
                if (embedding_cfg.expected_dims > 0 and embedding_cfg.expected_dims != entry.dims) continue;
                if (embedding_cfg.source_artifact_name.len > 0) {
                    const chunk_cfg = self.getEnrichment(.chunk, embedding_cfg.source_artifact_name) orelse return error.InvalidIndexConfig;
                    if (!hasGeneratedChunkRequest(requests.items, doc_key, chunk_cfg.source_field, chunk_cfg.source_template, chunk_cfg.name)) {
                        try requests.append(alloc, .{
                            .kind = .chunk_text,
                            .index_name = try alloc.dupe(u8, entry.config.name),
                            .artifact_name = try alloc.dupe(u8, chunk_cfg.name),
                            .doc_key = try alloc.dupe(u8, doc_key),
                            .source_field = try alloc.dupe(u8, chunk_cfg.source_field),
                            .source_template = if (chunk_cfg.source_template.len > 0) try alloc.dupe(u8, chunk_cfg.source_template) else "",
                            .chunk_size = chunk_cfg.chunk_size,
                            .chunk_overlap = chunk_cfg.chunk_overlap,
                            .chunker_json = if (chunk_cfg.chunker_json.len > 0) try alloc.dupe(u8, chunk_cfg.chunker_json) else "",
                        });
                    }
                    if (!hasGeneratedDenseEmbeddingRequest(requests.items, doc_key, embedding_cfg.source_field, embedding_cfg.source_template, chunk_cfg.name, embedding_name)) {
                        try requests.append(alloc, .{
                            .kind = .dense_embedding,
                            .index_name = try alloc.dupe(u8, entry.config.name),
                            .artifact_name = try alloc.dupe(u8, chunk_cfg.name),
                            .embedding_name = try alloc.dupe(u8, embedding_name),
                            .doc_key = try alloc.dupe(u8, doc_key),
                            .source_field = try alloc.dupe(u8, embedding_cfg.source_field),
                            .source_template = if (embedding_cfg.source_template.len > 0) try alloc.dupe(u8, embedding_cfg.source_template) else "",
                            .expected_dims = entry.dims,
                            .chunk_size = chunk_cfg.chunk_size,
                            .chunk_overlap = chunk_cfg.chunk_overlap,
                            .chunker_json = if (chunk_cfg.chunker_json.len > 0) try alloc.dupe(u8, chunk_cfg.chunker_json) else "",
                        });
                    }
                } else {
                    if (!hasGeneratedDenseEmbeddingRequest(requests.items, doc_key, embedding_cfg.source_field, embedding_cfg.source_template, "", embedding_name)) {
                        try requests.append(alloc, .{
                            .kind = .dense_embedding,
                            .index_name = try alloc.dupe(u8, entry.config.name),
                            .artifact_name = "",
                            .embedding_name = try alloc.dupe(u8, embedding_name),
                            .doc_key = try alloc.dupe(u8, doc_key),
                            .source_field = try alloc.dupe(u8, embedding_cfg.source_field),
                            .source_template = if (embedding_cfg.source_template.len > 0) try alloc.dupe(u8, embedding_cfg.source_template) else "",
                            .expected_dims = entry.dims,
                        });
                    }
                }
            }
        }

        for (self.sparse_indexes.items) |entry| {
            if (hasExplicitSparseEmbedding(explicit_sparse, entry.config.name)) continue;
            if (try mapper.extractSparseVectorField(alloc, doc_value, entry.field_name)) |sparse_vec| {
                var vec = sparse_vec;
                vec.deinit(alloc);
                continue;
            }

            if (try parseSparseGeneratorConfig(alloc, entry.config.config_json)) |generator| {
                defer generator.deinit(alloc);
                const chunk_cfg = resolveChunkGenerator(self, generator);
                if (generatorHasChunking(chunk_cfg) and !hasGeneratedChunkRequest(requests.items, doc_key, chunk_cfg.source_field, chunk_cfg.source_template, chunk_cfg.artifact_name)) {
                    try requests.append(alloc, .{
                        .kind = .chunk_text,
                        .index_name = try alloc.dupe(u8, entry.config.name),
                        .artifact_name = try alloc.dupe(u8, chunk_cfg.artifact_name),
                        .doc_key = try alloc.dupe(u8, doc_key),
                        .source_field = try alloc.dupe(u8, chunk_cfg.source_field),
                        .source_template = if (chunk_cfg.source_template.len > 0) try alloc.dupe(u8, chunk_cfg.source_template) else "",
                        .chunk_size = chunk_cfg.chunk_size,
                        .chunk_overlap = chunk_cfg.chunk_overlap,
                        .chunker_json = if (chunk_cfg.chunker_json.len > 0) try alloc.dupe(u8, chunk_cfg.chunker_json) else "",
                    });
                }
                try requests.append(alloc, .{
                    .kind = .sparse_embedding,
                    .index_name = try alloc.dupe(u8, entry.config.name),
                    .artifact_name = try alloc.dupe(u8, chunk_cfg.artifact_name),
                    .embedding_name = if (chunk_cfg.embedding_name) |embedding_name| try alloc.dupe(u8, embedding_name) else try alloc.dupe(u8, entry.config.name),
                    .doc_key = try alloc.dupe(u8, doc_key),
                    .source_field = try alloc.dupe(u8, chunk_cfg.source_field),
                    .source_template = if (chunk_cfg.source_template.len > 0) try alloc.dupe(u8, chunk_cfg.source_template) else "",
                    .chunk_size = chunk_cfg.chunk_size,
                    .chunk_overlap = chunk_cfg.chunk_overlap,
                    .chunker_json = if (chunk_cfg.chunker_json.len > 0) try alloc.dupe(u8, chunk_cfg.chunker_json) else "",
                });
            }
        }

        for (self.enrichments.items) |entry| {
            if (entry.kind != .asset) continue;
            try requests.append(alloc, .{
                .kind = .asset,
                .index_name = try alloc.dupe(u8, entry.name),
                .artifact_name = try alloc.dupe(u8, entry.name),
                .doc_key = try alloc.dupe(u8, doc_key),
                .source_field = try alloc.dupe(u8, entry.source_field),
                .source_template = if (entry.source_template.len > 0) try alloc.dupe(u8, entry.source_template) else "",
                .content_type = if (entry.content_type.len > 0) try alloc.dupe(u8, entry.content_type) else "",
                .producer_json = if (entry.producer_json.len > 0) try alloc.dupe(u8, entry.producer_json) else "",
            });
        }

        return try requests.toOwnedSlice(alloc);
    }

    pub fn appendIndexFieldEmbeddingsToExtractedWrite(
        self: *const IndexManager,
        alloc: Allocator,
        doc_key: []const u8,
        doc_value: []const u8,
        extracted: *mapper.ExtractedWrite,
    ) !void {
        for (self.dense_indexes.items) |entry| {
            if (hasExplicitDenseEmbedding(extracted.dense_embeddings, entry.config.name)) continue;
            const vector = (try mapper.extractDenseVectorField(alloc, doc_value, entry.field_name, entry.dims)) orelse continue;
            var vector_owned = true;
            errdefer if (vector_owned) alloc.free(vector);
            var index_name = try alloc.dupe(u8, entry.config.name);
            errdefer if (index_name.len > 0) alloc.free(index_name);
            var owned_doc_key = try alloc.dupe(u8, doc_key);
            errdefer if (owned_doc_key.len > 0) alloc.free(owned_doc_key);
            try appendDenseEmbeddingToExtractedWrite(alloc, extracted, .{
                .index_name = index_name,
                .doc_key = owned_doc_key,
                .vector = vector,
            });
            index_name = &.{};
            owned_doc_key = &.{};
            vector_owned = false;
        }

        for (self.sparse_indexes.items) |entry| {
            if (hasExplicitSparseEmbedding(extracted.sparse_embeddings, entry.config.name)) continue;
            var sparse_vec = (try mapper.extractSparseVectorField(alloc, doc_value, entry.field_name)) orelse continue;
            errdefer sparse_vec.deinit(alloc);
            var index_name = try alloc.dupe(u8, entry.config.name);
            errdefer if (index_name.len > 0) alloc.free(index_name);
            var owned_doc_key = try alloc.dupe(u8, doc_key);
            errdefer if (owned_doc_key.len > 0) alloc.free(owned_doc_key);
            try appendSparseEmbeddingToExtractedWrite(alloc, extracted, .{
                .index_name = index_name,
                .doc_key = owned_doc_key,
                .indices = sparse_vec.indices,
                .values = sparse_vec.values,
            });
            index_name = &.{};
            owned_doc_key = &.{};
            sparse_vec.indices = &.{};
            sparse_vec.values = &.{};
        }
    }

    pub fn vectorStoreFieldNamesAlloc(self: *const IndexManager, alloc: Allocator) ![][]u8 {
        var fields = std.ArrayListUnmanaged([]u8).empty;
        errdefer {
            for (fields.items) |field| alloc.free(field);
            fields.deinit(alloc);
        }

        for (self.dense_indexes.items) |entry| {
            if (entry.external or entry.chunk_name != null or entry.embedding_name != null) continue;
            if (containsOwnedString(fields.items, entry.field_name)) continue;
            try fields.append(alloc, try alloc.dupe(u8, entry.field_name));
        }
        for (self.sparse_indexes.items) |entry| {
            if (try parseSparseGeneratorConfig(alloc, entry.config.config_json)) |generator| {
                generator.deinit(alloc);
                continue;
            }
            if (containsOwnedString(fields.items, entry.field_name)) continue;
            try fields.append(alloc, try alloc.dupe(u8, entry.field_name));
        }
        return try fields.toOwnedSlice(alloc);
    }

    pub fn textIndex(self: *IndexManager, name: ?[]const u8) ?*persistent_mod.PersistentIndex {
        if (name) |index_name| {
            for (self.text_indexes.items) |*entry| {
                if (std.mem.eql(u8, entry.config.name, index_name)) return &entry.persistent;
            }
            return null;
        }

        if (self.text_indexes.items.len == 1) return &self.text_indexes.items[0].persistent;
        return null;
    }

    pub fn textIndexEntry(self: *IndexManager, name: ?[]const u8) ?*TextIndex {
        if (name) |index_name| {
            for (self.text_indexes.items) |*entry| {
                if (std.mem.eql(u8, entry.config.name, index_name)) return entry;
            }
            return null;
        }

        if (self.text_indexes.items.len == 1) return &self.text_indexes.items[0];
        return null;
    }

    pub fn fullTextLexicalAccessPath(self: *IndexManager, name: ?[]const u8, field: []const u8, analyzer: []const u8) ?algebraic_mod.ir.PhysicalAccessPath {
        const entry = self.textIndexEntry(name) orelse return null;
        if (!textFieldAnalyzerMatches(entry, field, analyzer)) return null;
        const identity = algebraic_mod.lexical.DictionaryIdentity.analyzedText(entry.config.name, field, analyzer);
        return algebraic_mod.ir.lexicalAccessPath(entry.config.name, .full_text_postings, identity, true);
    }

    pub fn fullTextLexicalAccessPathForField(self: *IndexManager, name: ?[]const u8, field: []const u8) ?algebraic_mod.ir.PhysicalAccessPath {
        const entry = self.textIndexEntry(name) orelse return null;
        const analyzer = textFieldAnalyzerName(entry, field) orelse return null;
        return self.fullTextLexicalAccessPath(entry.config.name, field, analyzer);
    }

    pub fn planFullTextLexicalAccessPathAlloc(
        self: *IndexManager,
        alloc: Allocator,
        name: ?[]const u8,
        field: []const u8,
        analyzer: ?[]const u8,
        fragment: algebraic_mod.ir.TensorFragment,
    ) !?algebraic_mod.ir.AccessPathPlan {
        const entry = self.textIndexEntry(name) orelse return null;
        const resolved_analyzer = if (analyzer) |explicit| blk: {
            if (!textFieldAnalyzerMatches(entry, field, explicit)) return null;
            break :blk explicit;
        } else textFieldAnalyzerName(entry, field) orelse return null;
        return try self.planTypedAccessPathAlloc(alloc, .{
            .fragment = fragment,
            .layout = .full_text_postings,
            .output_dims = &.{.doc},
            .owner = entry.config.name,
            .dictionary = algebraic_mod.lexical.DictionaryIdentity.analyzedText(entry.config.name, field, resolved_analyzer),
        });
    }

    fn textFieldAnalyzerName(entry: *const TextIndex, field: []const u8) ?[]const u8 {
        var resolved: ?[]const u8 = null;
        for (entry.text_analysis.field_analyzers) |item| {
            if (!std.mem.eql(u8, item.field_name, field)) continue;
            if (resolved == null) {
                resolved = item.analyzer_name;
            } else if (!std.mem.eql(u8, resolved.?, item.analyzer_name)) {
                return null;
            }
        }
        return resolved orelse "standard";
    }

    fn textFieldAnalyzerMatches(entry: *const TextIndex, field: []const u8, analyzer: []const u8) bool {
        var saw_field = false;
        for (entry.text_analysis.field_analyzers) |item| {
            if (!std.mem.eql(u8, item.field_name, field)) continue;
            saw_field = true;
            if (std.mem.eql(u8, item.analyzer_name, analyzer)) return true;
        }
        return !saw_field and std.mem.eql(u8, analyzer, "standard");
    }

    pub fn handoffRightOnlyTextSegmentsFrom(self: *IndexManager, src: *IndexManager, split_key: []const u8, collect_skip_doc_keys: bool) ![]TextSplitHandoff {
        var handoffs = try self.alloc.alloc(TextSplitHandoff, self.text_indexes.items.len);
        errdefer {
            for (handoffs) |*handoff| {
                if (handoff.index_name.len > 0) handoff.deinit(self.alloc);
            }
            self.alloc.free(handoffs);
        }
        for (handoffs) |*handoff| {
            handoff.* = .{
                .index_name = &.{},
                .skip_doc_keys = .empty,
                .transferred_segments = 0,
            };
        }

        for (self.text_indexes.items, 0..) |*dest_entry, i| {
            handoffs[i].index_name = try self.alloc.dupe(u8, dest_entry.config.name);
            const src_entry = src.findTextIndexEntry(dest_entry.config.name) orelse continue;
            const result = try src_entry.persistent.handoffRightOnlySegmentsToChildDetailed(&dest_entry.persistent, split_key, self.alloc, collect_skip_doc_keys);
            defer self.alloc.free(result.doc_keys);

            handoffs[i].transferred_segments += result.transferred_segments;
            if (collect_skip_doc_keys) {
                try mergeHandoffDocKeys(self.alloc, &handoffs[i], result.doc_keys);
            }

            const plan = try src_entry.persistent.classifyActiveSegmentsForSplit(self.alloc, split_key);
            defer {
                for (plan) |*entry| entry.deinit(self.alloc);
                self.alloc.free(plan);
            }

            for (plan) |entry| {
                if (entry.class != .mixed) continue;

                var stored_segment = try src_entry.persistent.readStoredSegment(self.alloc, entry.seg_id);
                defer stored_segment.deinit(self.alloc);

                var rebuilt = try buildSplitSegment(
                    self.alloc,
                    stored_segment.segment_bytes,
                    stored_segment.deletion_bitmap_bytes,
                    split_key,
                    .right,
                    dest_entry.config.config_json,
                    dest_entry.runtime_schema,
                    collect_skip_doc_keys,
                );
                defer if (rebuilt.segment_bytes) |segment_bytes| self.alloc.free(segment_bytes);
                defer self.alloc.free(rebuilt.doc_keys);
                if (rebuilt.segment_bytes) |segment_bytes| {
                    rebuilt.segment_bytes = null;
                    try dest_entry.persistent.indexSegmentOwned(segment_bytes);
                    handoffs[i].transferred_segments += 1;
                    if (collect_skip_doc_keys) {
                        try mergeHandoffDocKeys(self.alloc, &handoffs[i], rebuilt.doc_keys);
                    }
                }
            }
        }

        return handoffs;
    }

    pub fn handoffSparseFrom(self: *IndexManager, src: *IndexManager, lower: []const u8, upper: []const u8, collect_skip_doc_keys: bool) ![]SparseSplitHandoff {
        var handoffs = try self.alloc.alloc(SparseSplitHandoff, self.sparse_indexes.items.len);
        errdefer {
            for (handoffs) |*handoff| {
                if (handoff.index_name.len > 0) handoff.deinit(self.alloc);
            }
            self.alloc.free(handoffs);
        }
        for (handoffs) |*handoff| {
            handoff.* = .{
                .index_name = &.{},
                .skip_doc_keys = .empty,
                .transferred_docs = 0,
            };
        }

        for (self.sparse_indexes.items, 0..) |*dest_entry, i| {
            handoffs[i].index_name = try self.alloc.dupe(u8, dest_entry.config.name);
            const src_entry = src.findSparseIndexEntry(dest_entry.config.name) orelse continue;
            const rebuilt = try src_entry.index.handoffRangeInto(&dest_entry.index, self.alloc, lower, upper, collect_skip_doc_keys);
            defer self.alloc.free(rebuilt.doc_ids);

            handoffs[i].transferred_docs += rebuilt.doc_ids.len;
            handoffs[i].select_docs_ns += rebuilt.select_docs_ns;
            handoffs[i].terms_ns += rebuilt.terms_ns;
            handoffs[i].commit_ns += rebuilt.commit_ns;
            if (collect_skip_doc_keys) {
                try mergeSkipDocKeys(self.alloc, &handoffs[i].skip_doc_keys, rebuilt.doc_ids);
            }
        }

        return handoffs;
    }

    pub fn handoffSparseFromPreparedDocIds(self: *IndexManager, src: *IndexManager, doc_ids: []const []const u8, lower: []const u8, upper: []const u8, collect_skip_doc_keys: bool) ![]SparseSplitHandoff {
        var handoffs = try self.alloc.alloc(SparseSplitHandoff, self.sparse_indexes.items.len);
        errdefer {
            for (handoffs) |*handoff| {
                if (handoff.index_name.len > 0) handoff.deinit(self.alloc);
            }
            self.alloc.free(handoffs);
        }
        for (handoffs) |*handoff| {
            handoff.* = .{
                .index_name = &.{},
                .skip_doc_keys = .empty,
                .transferred_docs = 0,
            };
        }

        for (self.sparse_indexes.items, 0..) |*dest_entry, i| {
            handoffs[i].index_name = try self.alloc.dupe(u8, dest_entry.config.name);
            const src_entry = src.findSparseIndexEntry(dest_entry.config.name) orelse continue;
            const rebuilt = try src_entry.index.handoffPreparedDocIdsInto(&dest_entry.index, self.alloc, doc_ids, lower, upper, collect_skip_doc_keys);
            defer self.alloc.free(rebuilt.doc_ids);

            handoffs[i].transferred_docs += rebuilt.doc_ids.len;
            handoffs[i].select_docs_ns += rebuilt.select_docs_ns;
            handoffs[i].terms_ns += rebuilt.terms_ns;
            handoffs[i].commit_ns += rebuilt.commit_ns;
            if (collect_skip_doc_keys) {
                try mergeSkipDocKeys(self.alloc, &handoffs[i].skip_doc_keys, rebuilt.doc_ids);
            }
        }

        return handoffs;
    }

    pub fn handoffDenseFrom(self: *IndexManager, src: *IndexManager, dest_store: *docstore_mod.DocStore, split_key: []const u8, collect_skip_doc_keys: bool) ![]DenseSplitHandoff {
        const dense_split_batch_size = 1024;
        var handoffs = try self.alloc.alloc(DenseSplitHandoff, self.dense_indexes.items.len);
        errdefer {
            for (handoffs) |*handoff| {
                if (handoff.index_name.len > 0) handoff.deinit(self.alloc);
            }
            self.alloc.free(handoffs);
        }
        for (handoffs) |*handoff| {
            handoff.* = .{
                .index_name = &.{},
                .skip_doc_keys = .empty,
                .transferred_docs = 0,
            };
        }

        for (self.dense_indexes.items, 0..) |*dest_entry, i| {
            const src_entry = src.denseIndex(dest_entry.config.name) orelse continue;
            const split_work = src_entry.index.estimateSplitRebuildWork(split_key) catch |err| switch (err) {
                error.MissingSplitRange => continue,
                else => return err,
            };
            handoffs[i].index_name = try self.alloc.dupe(u8, dest_entry.config.name);
            const total_members = split_work.totalRightMembers();
            var dest_txn = try dest_entry.index.beginWriteTxn();
            errdefer dest_txn.abort();
            var store_batch = try dest_store.beginWriteBatch();
            errdefer store_batch.abort();
            const store_txn = store_batch.asTxn();
            const Context = struct {
                alloc: Allocator,
                handoff: *DenseSplitHandoff,
                collect_skip_doc_keys: bool,
                collected: []hbc_mod.PreparedBulkBuildInput,
                vectors_storage: []f32,
                transformed_storage: []f32,
                collected_count: usize = 0,
                max_vector_id: u64 = 0,

                fn consume(ctx: *@This(), items: []const hbc_mod.BatchInsertItem) !void {
                    for (items) |item| {
                        const slot = ctx.collected_count;
                        const dims = item.vector.len;
                        const vector_slot = ctx.vectors_storage[slot * dims ..][0..dims];
                        @memcpy(vector_slot, item.vector);
                        const transformed_slot = ctx.transformed_storage[slot * dims ..][0..dims];
                        @memcpy(transformed_slot, item.transformed orelse unreachable);
                        const owned_metadata = try ctx.alloc.dupe(u8, item.metadata);
                        errdefer ctx.alloc.free(owned_metadata);

                        ctx.collected[slot] = .{
                            .vector_id = item.vector_id,
                            .vector = vector_slot,
                            .transformed = transformed_slot,
                            .metadata = owned_metadata,
                        };
                        ctx.collected_count += 1;

                        if (ctx.collect_skip_doc_keys) {
                            const owned_key = try ctx.alloc.dupe(u8, owned_metadata);
                            errdefer ctx.alloc.free(owned_key);
                            const gop = try ctx.handoff.skip_doc_keys.getOrPut(ctx.alloc, owned_key);
                            if (gop.found_existing) {
                                ctx.alloc.free(owned_key);
                            } else {
                                gop.value_ptr.* = {};
                            }
                        }
                        ctx.handoff.transferred_docs += 1;
                        ctx.max_vector_id = @max(ctx.max_vector_id, item.vector_id);
                    }
                }
            };

            const dims: usize = @intCast(dest_entry.index.config.dims);
            const collected = try self.alloc.alloc(hbc_mod.PreparedBulkBuildInput, total_members);
            defer self.alloc.free(collected);
            const vectors_storage = try self.alloc.alloc(f32, total_members * dims);
            defer self.alloc.free(vectors_storage);
            const transformed_storage = try self.alloc.alloc(f32, total_members * dims);
            defer self.alloc.free(transformed_storage);
            var ctx = Context{
                .alloc = self.alloc,
                .handoff = &handoffs[i],
                .collect_skip_doc_keys = collect_skip_doc_keys,
                .collected = collected,
                .vectors_storage = vectors_storage,
                .transformed_storage = transformed_storage,
            };
            defer {
                for (ctx.collected[0..ctx.collected_count]) |item| {
                    self.alloc.free(@constCast(item.metadata));
                }
            }
            const stream_started = nowNs();
            _ = try src_entry.index.streamSplitMembers(split_key, dense_split_batch_size, &ctx, Context.consume);
            handoffs[i].stream_ns += elapsedSince(stream_started);

            const before_insert = dest_entry.index.getWriteProfile();
            const insert_started = nowNs();
            try dest_entry.index.bulkBuildPreparedInputsTxn(&dest_txn, ctx.collected[0..ctx.collected_count]);
            handoffs[i].insert_ns += elapsedSince(insert_started);
            const after_insert = dest_entry.index.getWriteProfile();
            handoffs[i].insert_store_ns += after_insert.bulk_build_store_ns - before_insert.bulk_build_store_ns;
            handoffs[i].insert_tree_ns += after_insert.bulk_build_tree_ns - before_insert.bulk_build_tree_ns;

            const mapping_started = nowNs();
            try self.writeDenseVectorMappingsForItemsTxn(dest_store, store_txn, dest_entry.config.name, ctx.collected[0..ctx.collected_count]);
            handoffs[i].mapping_ns += elapsedSince(mapping_started);

            const before_finalize = dest_entry.index.getWriteProfile();
            const finalize_started = nowNs();
            try dest_entry.index.finishWriteTxn(&dest_txn);
            handoffs[i].finalize_ns += elapsedSince(finalize_started);
            const after_finalize = dest_entry.index.getWriteProfile();
            handoffs[i].finalize_quantized_ns += after_finalize.refresh_quantized_ns - before_finalize.refresh_quantized_ns;
            handoffs[i].finalize_flush_ns += after_finalize.insert_flush_metadata_ns - before_finalize.insert_flush_metadata_ns;
            handoffs[i].finalize_commit_ns += after_finalize.insert_commit_ns - before_finalize.insert_commit_ns;

            if (ctx.max_vector_id > 0) {
                try self.setDenseNextIdAtLeastTxn(store_txn, dest_entry.config.name, ctx.max_vector_id + 1);
            }
            try store_batch.commit();
        }

        return handoffs;
    }

    fn writeDenseVectorMappingsForItemsTxn(
        self: *IndexManager,
        _: *docstore_mod.DocStore,
        txn: anytype,
        index_name: []const u8,
        items: []const hbc_mod.PreparedBulkBuildInput,
    ) !void {
        if (items.len == 0) return;

        for (items) |item| {
            const doc_map_key = try denseDocMappingKey(self.alloc, index_name, item.metadata);
            defer self.alloc.free(doc_map_key);

            var buf: [8]u8 = undefined;
            std.mem.writeInt(u64, &buf, item.vector_id, .little);
            try txn.put(doc_map_key, &buf);
        }
    }

    pub fn splitDestinationNeedsDocumentIndexing(
        self: *const IndexManager,
        dense_handoffs: []const DenseSplitHandoff,
        text_handoffs: []const TextSplitHandoff,
        sparse_handoffs: []const SparseSplitHandoff,
    ) bool {
        if (self.dense_indexes.items.len != dense_handoffs.len) return true;
        for (self.dense_indexes.items) |entry| {
            _ = findDenseSplitHandoff(dense_handoffs, entry.config.name) orelse return true;
        }
        if (self.text_indexes.items.len > 0) {
            if (text_handoffs.len != self.text_indexes.items.len) return true;
            for (self.text_indexes.items) |entry| {
                _ = findTextSplitHandoff(text_handoffs, entry.config.name) orelse return true;
            }
        }

        if (self.sparse_indexes.items.len != sparse_handoffs.len) return true;
        for (self.sparse_indexes.items) |entry| {
            _ = findSparseSplitHandoff(sparse_handoffs, entry.config.name) orelse return true;
        }

        // When the destination only has text indexes, the split handoff path
        // already transferred or rebuilt the full child-side text coverage.
        // Sparse indexes hand off their child-side forward/reverse coverage and
        // rebuild postings directly from source chunks.
        // Graph indexes are handled separately from edge keys.
        return false;
    }

    fn ownsGeneratedChunkArtifacts(self: *const IndexManager, exclude_index_name: []const u8, chunk_name: []const u8) bool {
        // Chunk artifacts can still be valid reusable inputs for retained
        // embedding enrichment config, so chunk ownership intentionally keeps
        // following enrichment references in addition to live indexes.
        return !self.chunkArtifactsReferencedElsewhere(exclude_index_name, chunk_name);
    }

    fn ownsGeneratedEmbeddingArtifacts(self: *const IndexManager, exclude_index_name: []const u8, embedding_name: []const u8) bool {
        // Embedding artifact lifetime follows the live index graph for the same
        // reason as generated chunk artifacts: shorthand enrichment catalog
        // entries are retained config, not proof that stale artifacts are still
        // query-visible.
        return !self.embeddingArtifactsReferencedElsewhere(exclude_index_name, embedding_name);
    }

    fn chunkArtifactsReferencedElsewhere(self: *const IndexManager, exclude_index_name: []const u8, chunk_name: []const u8) bool {
        for (self.text_indexes.items) |entry| {
            if (std.mem.eql(u8, entry.config.name, exclude_index_name)) continue;
            if (entry.chunk_name) |configured| {
                if (std.mem.eql(u8, configured, chunk_name)) return true;
            }
        }
        for (self.dense_indexes.items) |entry| {
            if (std.mem.eql(u8, entry.config.name, exclude_index_name)) continue;
            if (entry.chunk_name) |configured| {
                if (std.mem.eql(u8, configured, chunk_name)) return true;
            }
        }
        for (self.sparse_indexes.items) |entry| {
            if (std.mem.eql(u8, entry.config.name, exclude_index_name)) continue;
            if (entry.chunk_name) |configured| {
                if (std.mem.eql(u8, configured, chunk_name)) return true;
            }
        }
        for (self.enrichments.items) |entry| {
            if (entry.kind == .embedding and std.mem.eql(u8, entry.source_artifact_name, chunk_name)) return true;
        }
        return false;
    }

    fn embeddingArtifactsReferencedElsewhere(self: *const IndexManager, exclude_index_name: []const u8, embedding_name: []const u8) bool {
        for (self.dense_indexes.items) |entry| {
            if (std.mem.eql(u8, entry.config.name, exclude_index_name)) continue;
            if (entry.embedding_name) |configured| {
                if (std.mem.eql(u8, configured, embedding_name)) return true;
            }
        }
        for (self.sparse_indexes.items) |entry| {
            if (std.mem.eql(u8, entry.config.name, exclude_index_name)) continue;
            if (entry.embedding_name) |configured| {
                if (std.mem.eql(u8, configured, embedding_name)) return true;
            }
        }
        return false;
    }

    const ArtifactRefs = struct {
        chunk_name: ?[]u8 = null,
        embedding_name: ?[]u8 = null,
        asset_name: ?[]u8 = null,

        fn deinit(self: *@This(), alloc: Allocator) void {
            if (self.chunk_name) |value| alloc.free(value);
            if (self.embedding_name) |value| alloc.free(value);
            if (self.asset_name) |value| alloc.free(value);
            self.* = undefined;
        }
    };

    fn artifactRefsFromConfig(self: *IndexManager, cfg: types.IndexConfig) !ArtifactRefs {
        switch (cfg.kind) {
            .full_text => {
                const text_cfg = try parseTextConfig(self.alloc, cfg.config_json);
                defer text_cfg.deinit(self.alloc);
                return .{
                    .chunk_name = if (text_cfg.source_artifact_name) |name| try self.alloc.dupe(u8, name) else null,
                };
            },
            .dense_vector => {
                const dense_cfg = try parseDenseConfig(self.alloc, cfg.config_json);
                defer dense_cfg.deinit(self.alloc);
                const dense_generator = try parseDenseGeneratorConfig(self.alloc, cfg.config_json);
                defer if (dense_generator) |generator| generator.deinit(self.alloc);
                const referenced_embedding = if (dense_generator == null and dense_cfg.embedding_name != null and !dense_cfg.external)
                    self.getEnrichment(.embedding, dense_cfg.embedding_name.?)
                else
                    null;
                return .{
                    .chunk_name = if (dense_generator) |generator|
                        if (generatorHasChunking(generator)) try self.alloc.dupe(u8, generator.artifact_name) else null
                    else if (referenced_embedding) |embedding_cfg|
                        if (embedding_cfg.source_artifact_name.len > 0) try self.alloc.dupe(u8, embedding_cfg.source_artifact_name) else null
                    else
                        null,
                    .embedding_name = if (dense_generator) |generator|
                        if (generator.embedding_name) |embedding_name| try self.alloc.dupe(u8, embedding_name) else try self.alloc.dupe(u8, cfg.name)
                    else if (dense_cfg.embedding_name) |embedding_name|
                        try self.alloc.dupe(u8, embedding_name)
                    else
                        null,
                };
            },
            .sparse_vector => {
                const sparse_generator = try parseSparseGeneratorConfig(self.alloc, cfg.config_json);
                defer if (sparse_generator) |generator| generator.deinit(self.alloc);
                return .{
                    .chunk_name = if (sparse_generator) |generator| blk: {
                        const chunk_cfg = resolveChunkGenerator(self, generator);
                        break :blk if (generatorHasChunking(chunk_cfg)) try self.alloc.dupe(u8, chunk_cfg.artifact_name) else null;
                    } else null,
                    .embedding_name = if (sparse_generator) |generator|
                        if (generator.embedding_name) |embedding_name| try self.alloc.dupe(u8, embedding_name) else try self.alloc.dupe(u8, cfg.name)
                    else
                        try self.alloc.dupe(u8, cfg.name),
                };
            },
            .graph => {
                var graph_cfg = try parseGraphConfig(self.alloc, cfg.config_json);
                defer graph_cfg.deinit(self.alloc);
                return .{
                    .asset_name = if (graph_cfg.artifact_source) |source| try self.alloc.dupe(u8, source.artifact_name) else null,
                };
            },
            .algebraic => return .{},
        }
    }

    fn chunkArtifactsReferencedElsewhereIncludingStatusOnly(self: *IndexManager, exclude_index_name: []const u8, chunk_name: []const u8) !bool {
        if (self.chunkArtifactsReferencedElsewhere(exclude_index_name, chunk_name)) return true;
        for (self.status_only_index_configs) |cfg| {
            if (std.mem.eql(u8, cfg.name, exclude_index_name)) continue;
            var refs = try self.artifactRefsFromConfig(cfg);
            defer refs.deinit(self.alloc);
            if (refs.chunk_name) |configured| {
                if (std.mem.eql(u8, configured, chunk_name)) return true;
            }
        }
        return false;
    }

    fn embeddingArtifactsReferencedElsewhereIncludingStatusOnly(self: *IndexManager, exclude_index_name: []const u8, embedding_name: []const u8) !bool {
        if (self.embeddingArtifactsReferencedElsewhere(exclude_index_name, embedding_name)) return true;
        for (self.status_only_index_configs) |cfg| {
            if (std.mem.eql(u8, cfg.name, exclude_index_name)) continue;
            var refs = try self.artifactRefsFromConfig(cfg);
            defer refs.deinit(self.alloc);
            if (refs.embedding_name) |configured| {
                if (std.mem.eql(u8, configured, embedding_name)) return true;
            }
        }
        return false;
    }

    fn deleteDenseIndexMetadata(self: *IndexManager, store: anytype, index_name: []const u8) !void {
        const prefix = try denseIndexMetadataPrefixAlloc(self.alloc, index_name);
        defer self.alloc.free(prefix);
        try self.deleteKeysWithPrefix(store, prefix);
        const legacy_prefix = try legacyDenseIndexMetadataPrefixAlloc(self.alloc, index_name);
        defer self.alloc.free(legacy_prefix);
        try self.deleteKeysWithPrefix(store, legacy_prefix);
    }

    fn deleteOwnedGeneratedArtifacts(
        self: *IndexManager,
        store: anytype,
        owned_chunk_name: ?[]const u8,
        owned_embedding_name: ?[]const u8,
    ) !void {
        if (owned_chunk_name == null and owned_embedding_name == null) return;

        const lower = try internal_keys.documentRangeLowerAlloc(self.alloc, "");
        defer self.alloc.free(lower);
        const entries = try store.scanRange(self.alloc, lower, "");
        defer docstore_mod.DocStore.freeResults(self.alloc, entries);
        if (entries.len == 0) return;

        var deletes = std.ArrayListUnmanaged([]const u8).empty;
        defer deletes.deinit(self.alloc);

        for (entries) |entry| {
            if (owned_chunk_name) |chunk_name| {
                if (internal_keys.isChunkArtifactRecordKey(entry.key) and internal_keys.matchesChunkArtifactName(entry.key, chunk_name)) {
                    try deletes.append(self.alloc, entry.key);
                    continue;
                }
                if (internal_keys.isDerivedEmbeddingArtifactKey(entry.key) and try self.derivedEmbeddingBaseMatchesChunk(entry.key, chunk_name)) {
                    try deletes.append(self.alloc, entry.key);
                    continue;
                }
            }
            if (owned_embedding_name) |embedding_name| {
                if (internal_keys.isEmbeddingArtifactKey(entry.key) and internal_keys.matchesEmbeddingArtifactName(entry.key, embedding_name)) {
                    try deletes.append(self.alloc, entry.key);
                    continue;
                }
                if (internal_keys.isDerivedEmbeddingArtifactKey(entry.key) and internal_keys.matchesDerivedEmbeddingArtifactName(entry.key, embedding_name)) {
                    try deletes.append(self.alloc, entry.key);
                    continue;
                }
            }
        }

        if (deletes.items.len > 0) try store.putBatch(&.{}, deletes.items);
    }

    fn derivedEmbeddingBaseMatchesChunk(self: *IndexManager, key: []const u8, chunk_name: []const u8) !bool {
        const base_key = (try internal_keys.derivedEmbeddingBaseKeyAlloc(self.alloc, key)) orelse return false;
        defer self.alloc.free(base_key);
        return internal_keys.matchesChunkArtifactName(base_key, chunk_name);
    }

    fn deleteKeysWithPrefix(self: *IndexManager, store: anytype, prefix: []const u8) !void {
        const entries = try store.scanPrefix(self.alloc, prefix);
        defer docstore_mod.DocStore.freeResults(self.alloc, entries);
        if (entries.len == 0) return;

        var deletes = std.ArrayListUnmanaged([]const u8).empty;
        defer deletes.deinit(self.alloc);
        for (entries) |entry| {
            try deletes.append(self.alloc, entry.key);
        }
        try store.putBatch(&.{}, deletes.items);
    }

    pub fn rebuildGraphSplitDestination(self: *IndexManager, lower: []const u8, upper: []const u8) !usize {
        var rebuilt: usize = 0;
        for (self.graph_indexes.items) |*entry| {
            rebuilt += try entry.index.rebuildReverseFromOwnedOutgoingEdges(self.alloc, lower, upper);
        }
        return rebuilt;
    }

    pub fn copyGraphSplitDestinationFrom(self: *IndexManager, src: *IndexManager, lower: []const u8, upper: []const u8) !usize {
        var copied: usize = 0;
        for (src.graph_indexes.items) |*src_entry| {
            const dest_entry = self.graphIndex(src_entry.config.name) orelse return error.IndexNotFound;
            copied += try src_entry.index.copyOwnedOutgoingEdgesTo(&dest_entry.index, self.alloc, lower, upper);
        }
        return copied;
    }

    pub fn pruneTextSplitRange(self: *IndexManager, split_key: []const u8) !void {
        for (self.text_indexes.items) |*entry| {
            const plan = try entry.persistent.classifyActiveSegmentsForSplit(self.alloc, split_key);
            defer {
                for (plan) |*plan_entry| plan_entry.deinit(self.alloc);
                self.alloc.free(plan);
            }

            var remove_ids = std.ArrayListUnmanaged(u64).empty;
            defer remove_ids.deinit(self.alloc);

            for (plan) |plan_entry| {
                switch (plan_entry.class) {
                    .left_only => {},
                    .right_only => try remove_ids.append(self.alloc, plan_entry.seg_id),
                    .mixed => {
                        var stored_segment = try entry.persistent.readStoredSegment(self.alloc, plan_entry.seg_id);
                        defer stored_segment.deinit(self.alloc);

                        var rebuilt = try buildSplitSegment(
                            self.alloc,
                            stored_segment.segment_bytes,
                            stored_segment.deletion_bitmap_bytes,
                            split_key,
                            .left,
                            entry.config.config_json,
                            entry.runtime_schema,
                            false,
                        );
                        defer if (rebuilt.segment_bytes) |segment_bytes| self.alloc.free(segment_bytes);
                        defer {
                            for (rebuilt.doc_keys) |key| self.alloc.free(key);
                            self.alloc.free(rebuilt.doc_keys);
                        }

                        if (rebuilt.segment_bytes) |segment_bytes| {
                            try entry.persistent.replaceSegmentsOwned(&.{plan_entry.seg_id}, segment_bytes);
                            rebuilt.segment_bytes = null;
                        } else {
                            try remove_ids.append(self.alloc, plan_entry.seg_id);
                        }
                    },
                }
            }

            try entry.persistent.removeSegments(remove_ids.items);
        }
    }

    pub fn pruneDenseSplitRange(self: *IndexManager, store: *docstore_mod.DocStore, split_key: []const u8) !void {
        for (self.dense_indexes.items) |*entry| {
            var members = try entry.index.collectSplitMembers(split_key);
            defer members.deinit(self.alloc);

            for ([_][]const u64{ members.right_only_members, members.mixed_right_members }) |member_ids| {
                for (member_ids) |member_id| {
                    const metadata = (try entry.index.getMetadata(member_id)) orelse continue;
                    defer self.alloc.free(metadata);

                    entry.index.delete(member_id) catch |err| switch (err) {
                        error.NotFound => {},
                        else => return err,
                    };
                    try self.clearDenseVectorMapping(store, entry.config.name, metadata, member_id);
                }
            }
        }
    }

    pub fn pruneSparseSplitRange(self: *IndexManager, split_key: []const u8, original_range_end: []const u8) !void {
        for (self.sparse_indexes.items) |*entry| {
            try entry.index.pruneRange(self.alloc, split_key, original_range_end);
        }
    }

    pub fn pruneGraphSplitRange(self: *IndexManager, split_key: []const u8, original_range_end: []const u8) !void {
        for (self.graph_indexes.items) |*entry| {
            _ = try entry.index.pruneOwnedRange(self.alloc, split_key, original_range_end);
        }
    }

    pub fn textChunkName(self: *const IndexManager, name: []const u8) ?[]const u8 {
        for (self.text_indexes.items) |*entry| {
            if (std.mem.eql(u8, entry.config.name, name)) return entry.chunk_name;
        }
        return null;
    }

    pub fn selectedTextChunkName(self: *const IndexManager, name: ?[]const u8) ?[]const u8 {
        if (name) |index_name| return self.textChunkName(index_name);
        if (self.text_indexes.items.len == 1) return self.text_indexes.items[0].chunk_name;
        return null;
    }

    pub fn textIndexIsChunkBacked(self: *const IndexManager, alloc: Allocator, name: ?[]const u8) !bool {
        const entry = if (name) |index_name| blk: {
            for (self.text_indexes.items) |*text_entry| {
                if (std.mem.eql(u8, text_entry.config.name, index_name)) break :blk text_entry;
            }
            break :blk null;
        } else if (self.text_indexes.items.len == 1)
            &self.text_indexes.items[0]
        else
            null;
        const resolved = entry orelse return false;
        if (resolved.chunk_name != null) return true;

        for (self.enrichments.items) |cfg| {
            if (cfg.kind != .chunk or cfg.chunker_json.len == 0) continue;
            if (try chunking_types.parseHasFullTextIndexFromSlice(alloc, cfg.chunker_json)) return true;
        }
        return false;
    }

    pub fn denseIndex(self: *IndexManager, name: ?[]const u8) ?*DenseIndex {
        if (name) |index_name| {
            for (self.dense_indexes.items) |*entry| {
                if (std.mem.eql(u8, entry.config.name, index_name)) return entry;
            }
            return null;
        }
        if (self.dense_indexes.items.len == 1) return &self.dense_indexes.items[0];
        return null;
    }

    pub fn denseVectorAccessPath(self: *IndexManager, name: ?[]const u8) ?algebraic_mod.ir.PhysicalAccessPath {
        const entry = self.denseIndex(name) orelse return null;
        return algebraic_mod.ir.vectorAccessPath(entry.config.name, .dense_vector);
    }

    pub fn denseWriteProfileByName(self: *IndexManager, name: []const u8) ?hbc_mod.WriteProfile {
        const entry = self.denseIndex(name) orelse return null;
        return entry.index.getWriteProfile();
    }

    pub fn sparseWriteProfileByName(self: *IndexManager, name: []const u8) ?sparse_mod.WriteProfile {
        const entry = self.sparseIndex(name) orelse return null;
        return entry.index.getWriteProfile();
    }

    pub fn beginDenseBulkIngestSessions(self: *IndexManager) !void {
        var opened: usize = 0;
        errdefer {
            for (self.dense_indexes.items[0..opened]) |*entry| {
                entry.index.abortBulkIngestSession();
            }
        }
        for (self.dense_indexes.items) |*entry| {
            try entry.index.beginBulkIngestSession();
            opened += 1;
        }
    }

    pub fn finishDenseBulkIngestSessionsWithOptions(self: *IndexManager, options: backend_types.BulkIngestFinishOptions) !void {
        var first_err: ?anyerror = null;
        for (self.dense_indexes.items) |*entry| {
            self.finishDenseBulkIngestEntryWithOptions(entry, options) catch |err| {
                if (first_err == null) first_err = err;
            };
        }
        if (first_err) |err| return err;
    }

    pub fn abortDenseBulkIngestSessions(self: *IndexManager) void {
        for (self.dense_indexes.items) |*entry| {
            entry.index.abortBulkIngestSession();
        }
    }

    pub fn beginDenseBulkIngestSessionByName(self: *IndexManager, name: []const u8) !void {
        const entry = self.denseIndex(name) orelse return error.IndexNotFound;
        try entry.index.beginBulkIngestSession();
    }

    pub fn finishDenseBulkIngestSessionByNameWithOptions(
        self: *IndexManager,
        name: []const u8,
        options: backend_types.BulkIngestFinishOptions,
    ) !void {
        const entry = self.denseIndex(name) orelse return error.IndexNotFound;
        try self.finishDenseBulkIngestEntryWithOptions(entry, options);
    }

    pub fn abortDenseBulkIngestSessionByName(self: *IndexManager, name: []const u8) void {
        const entry = self.denseIndex(name) orelse return;
        entry.index.abortBulkIngestSession();
    }

    pub fn beginSparseBulkIngestSessions(self: *IndexManager) !void {
        var opened: usize = 0;
        errdefer {
            for (self.sparse_indexes.items[0..opened]) |*entry| {
                entry.index.abortBulkIngestSession();
            }
        }
        for (self.sparse_indexes.items) |*entry| {
            try entry.index.beginBulkIngestSession();
            opened += 1;
        }
    }

    pub fn finishSparseBulkIngestSessionsWithOptions(self: *IndexManager, options: backend_types.BulkIngestFinishOptions) !void {
        var first_err: ?anyerror = null;
        for (self.sparse_indexes.items) |*entry| {
            entry.index.finishBulkIngestSessionWithOptions(options) catch |err| {
                if (first_err == null) first_err = err;
            };
        }
        if (first_err) |err| return err;
    }

    pub fn abortSparseBulkIngestSessions(self: *IndexManager) void {
        for (self.sparse_indexes.items) |*entry| {
            entry.index.abortBulkIngestSession();
        }
    }

    pub fn beginSparseBulkIngestSessionByName(self: *IndexManager, name: []const u8) !void {
        const entry = self.sparseIndex(name) orelse return error.IndexNotFound;
        try entry.index.beginBulkIngestSession();
    }

    pub fn finishSparseBulkIngestSessionByNameWithOptions(
        self: *IndexManager,
        name: []const u8,
        options: backend_types.BulkIngestFinishOptions,
    ) !void {
        const entry = self.sparseIndex(name) orelse return error.IndexNotFound;
        try entry.index.finishBulkIngestSessionWithOptions(options);
    }

    pub fn abortSparseBulkIngestSessionByName(self: *IndexManager, name: []const u8) void {
        const entry = self.sparseIndex(name) orelse return;
        entry.index.abortBulkIngestSession();
    }

    pub fn beginAlgebraicBulkIngestSessions(self: *IndexManager) !void {
        var opened: usize = 0;
        errdefer {
            for (self.algebraic_indexes.items[0..opened]) |*entry| {
                entry.index.abortBulkIngestSession();
            }
        }
        for (self.algebraic_indexes.items) |*entry| {
            try entry.index.beginBulkIngestSession();
            opened += 1;
        }
    }

    pub fn finishAlgebraicBulkIngestSessionsWithOptions(
        self: *IndexManager,
        store: *docstore_mod.DocStore,
        options: backend_types.BulkIngestFinishOptions,
    ) !void {
        var first_err: ?anyerror = null;
        for (self.algebraic_indexes.items) |*entry| {
            entry.index.finishBulkIngestSessionWithOptions(store, options) catch |err| {
                if (first_err == null) first_err = err;
            };
        }
        if (first_err) |err| return err;
    }

    pub fn abortAlgebraicBulkIngestSessions(self: *IndexManager) void {
        for (self.algebraic_indexes.items) |*entry| {
            entry.index.abortBulkIngestSession();
        }
    }

    fn finishDenseBulkIngestEntryWithOptions(
        self: *IndexManager,
        entry: *DenseIndex,
        options: backend_types.BulkIngestFinishOptions,
    ) !void {
        const previous_load_session = active_dense_vector_load_session;
        var vector_load_session: ?DenseVectorLoadSession = null;
        defer {
            if (vector_load_session != null) {
                active_dense_vector_load_session = previous_load_session;
                entry.index.setBypassExternalVectorCache(false);
            }
            if (vector_load_session) |*session| session.deinit();
        }

        const reuse_existing_session = blk: {
            if (active_dense_vector_load_session == null) break :blk false;
            if (entry.vector_loader_context == null) break :blk false;
            break :blk active_dense_vector_load_session.?.context == entry.vector_loader_context.?;
        };

        if (!reuse_existing_session and self.primary_store != null and entry.vector_loader_context != null) {
            vector_load_session = .{
                .context = entry.vector_loader_context.?,
            };
            active_dense_vector_load_session = &vector_load_session.?;
            entry.index.setBypassExternalVectorCache(true);
        }

        try entry.index.finishBulkIngestSessionWithOptions(options);
    }

    pub fn searchDenseEntryWithRequest(
        self: *IndexManager,
        entry: *DenseIndex,
        req: hbc_mod.SearchRequest,
    ) !hbc_mod.SearchResults {
        const previous_load_session = active_dense_vector_load_session;
        var vector_load_session: ?DenseVectorLoadSession = null;
        defer {
            if (vector_load_session != null) active_dense_vector_load_session = previous_load_session;
            if (vector_load_session) |*session| session.deinit();
        }

        if (active_dense_vector_load_session == null and self.primary_store != null and entry.vector_loader_context != null) {
            vector_load_session = .{
                .context = entry.vector_loader_context.?,
                .working_slice = .dense_search_working_set,
                .recycle_raw_reads = false,
                .cache_raw_values = false,
            };
            active_dense_vector_load_session = &vector_load_session.?;
        }

        return try entry.index.searchWithRequest(req);
    }

    pub fn searchDenseEntryProfiledWithRequest(
        self: *IndexManager,
        entry: *DenseIndex,
        req: hbc_mod.SearchRequest,
    ) !hbc_mod.ProfiledSearchResults {
        const previous_load_session = active_dense_vector_load_session;
        var vector_load_session: ?DenseVectorLoadSession = null;
        defer {
            if (vector_load_session != null) active_dense_vector_load_session = previous_load_session;
            if (vector_load_session) |*session| session.deinit();
        }

        if (active_dense_vector_load_session == null and self.primary_store != null and entry.vector_loader_context != null) {
            vector_load_session = .{
                .context = entry.vector_loader_context.?,
                .working_slice = .dense_search_working_set,
                .recycle_raw_reads = false,
                .cache_raw_values = false,
            };
            active_dense_vector_load_session = &vector_load_session.?;
        }

        return try entry.index.searchProfiledRequest(req);
    }

    pub fn textIndexesForChunk(
        self: *const IndexManager,
        alloc: Allocator,
        chunk_name: []const u8,
        include_default_full_text: bool,
    ) ![][]u8 {
        var names = std.ArrayListUnmanaged([]u8).empty;
        errdefer {
            for (names.items) |name| alloc.free(name);
            names.deinit(alloc);
        }
        for (self.text_indexes.items) |entry| {
            if (entry.chunk_name) |configured| {
                if (std.mem.eql(u8, configured, chunk_name)) {
                    try names.append(alloc, try alloc.dupe(u8, entry.config.name));
                }
            } else if (include_default_full_text) {
                try names.append(alloc, try alloc.dupe(u8, entry.config.name));
            }
        }
        return try names.toOwnedSlice(alloc);
    }

    pub fn denseChunkName(self: *const IndexManager, name: []const u8) ?[]const u8 {
        for (self.dense_indexes.items) |*entry| {
            if (std.mem.eql(u8, entry.config.name, name)) return entry.chunk_name;
        }
        return null;
    }

    pub fn selectedDenseChunkName(self: *const IndexManager, name: ?[]const u8) ?[]const u8 {
        if (name) |index_name| return self.denseChunkName(index_name);
        if (self.dense_indexes.items.len == 1) return self.dense_indexes.items[0].chunk_name;
        return null;
    }

    pub fn denseEmbeddingName(self: *const IndexManager, name: []const u8) ?[]const u8 {
        for (self.dense_indexes.items) |*entry| {
            if (std.mem.eql(u8, entry.config.name, name)) return entry.embedding_name;
        }
        return null;
    }

    pub fn selectedDenseEmbeddingName(self: *const IndexManager, name: ?[]const u8) ?[]const u8 {
        if (name) |index_name| return self.denseEmbeddingName(index_name);
        if (self.dense_indexes.items.len == 1) return self.dense_indexes.items[0].embedding_name;
        return null;
    }

    pub fn denseIndexesForEmbedding(self: *const IndexManager, alloc: Allocator, embedding_name: []const u8, dims: u32) ![][]u8 {
        var names = std.ArrayListUnmanaged([]u8).empty;
        errdefer {
            for (names.items) |name| alloc.free(name);
            names.deinit(alloc);
        }
        for (self.dense_indexes.items) |entry| {
            const configured = entry.embedding_name orelse if (entry.external) entry.config.name else continue;
            if (!std.mem.eql(u8, configured, embedding_name)) continue;
            if (entry.dims != dims) return error.ConflictingEnrichmentConfig;
            try names.append(alloc, try alloc.dupe(u8, entry.config.name));
        }
        return try names.toOwnedSlice(alloc);
    }

    pub fn sparseIndex(self: *IndexManager, name: ?[]const u8) ?*SparseIndex {
        if (name) |index_name| {
            for (self.sparse_indexes.items) |*entry| {
                if (std.mem.eql(u8, entry.config.name, index_name)) return entry;
            }
            return null;
        }
        if (self.sparse_indexes.items.len == 1) return &self.sparse_indexes.items[0];
        return null;
    }

    pub fn sparseVectorAccessPath(self: *IndexManager, name: ?[]const u8) ?algebraic_mod.ir.PhysicalAccessPath {
        const entry = self.sparseIndex(name) orelse return null;
        return algebraic_mod.ir.vectorAccessPath(entry.config.name, .sparse_vector);
    }

    pub fn sparseTokenAccessPath(self: *IndexManager, name: ?[]const u8) ?algebraic_mod.ir.PhysicalAccessPath {
        const entry = self.sparseIndex(name) orelse return null;
        const token_space = entry.embedding_name orelse entry.config.name;
        return algebraic_mod.ir.sparseTokenAccessPath(entry.config.name, entry.config.name, entry.field_name, token_space, "u32", false);
    }

    pub fn sparseEmbeddingName(self: *const IndexManager, name: []const u8) ?[]const u8 {
        for (self.sparse_indexes.items) |*entry| {
            if (std.mem.eql(u8, entry.config.name, name)) return entry.embedding_name;
        }
        return null;
    }

    pub fn sparseIndexesForEmbedding(self: *const IndexManager, alloc: Allocator, embedding_name: []const u8) ![][]u8 {
        var names = std.ArrayListUnmanaged([]u8).empty;
        errdefer {
            for (names.items) |name| alloc.free(name);
            names.deinit(alloc);
        }
        for (self.sparse_indexes.items) |entry| {
            const configured = entry.embedding_name orelse continue;
            if (!std.mem.eql(u8, configured, embedding_name)) continue;
            try names.append(alloc, try alloc.dupe(u8, entry.config.name));
        }
        return try names.toOwnedSlice(alloc);
    }

    pub fn graphIndex(self: *IndexManager, name: ?[]const u8) ?*GraphIndex {
        if (name) |index_name| {
            for (self.graph_indexes.items) |*entry| {
                if (std.mem.eql(u8, entry.config.name, index_name)) return entry;
            }
            return null;
        }
        if (self.graph_indexes.items.len == 1) return &self.graph_indexes.items[0];
        return null;
    }

    pub fn graphArtifactSource(self: *const IndexManager, name: []const u8) ?GraphArtifactSource {
        for (self.graph_indexes.items) |entry| {
            if (!std.mem.eql(u8, entry.config.name, name)) continue;
            return entry.artifact_source;
        }
        return null;
    }

    pub fn graphIndexConsumesAssetArtifact(self: *const IndexManager, index_name: []const u8, artifact_name: []const u8) bool {
        const source = self.graphArtifactSource(index_name) orelse return false;
        return std.mem.eql(u8, source.artifact_name, artifact_name);
    }

    pub fn graphTraversalAccessPath(self: *IndexManager, name: ?[]const u8) ?algebraic_mod.ir.PhysicalAccessPath {
        const entry = self.graphIndex(name) orelse return null;
        if (!entry.index.algebraic_semiring_traversal) return null;
        return algebraic_mod.ir.graphReachabilityAccessPath(entry.config.name);
    }

    pub fn planTypedAccessPathAlloc(self: *IndexManager, alloc: Allocator, expr: algebraic_mod.ir.TensorExpr) !?algebraic_mod.ir.AccessPathPlan {
        var paths = std.ArrayListUnmanaged(algebraic_mod.ir.PhysicalAccessPath).empty;
        defer paths.deinit(alloc);

        if (expr.dictionary) |dictionary| {
            if (dictionary.label_kind == .analyzed_term) {
                if (self.fullTextLexicalAccessPath(dictionary.scope, dictionary.field_or_path, dictionary.analyzer_or_canonicalization)) |path| {
                    try paths.append(alloc, path);
                }
            } else if (dictionary.label_kind == .sparse_token) {
                if (self.sparseTokenAccessPath(dictionary.scope)) |path| {
                    try paths.append(alloc, path);
                }
            }
        }
        if (expr.fragment == .vector_search) {
            for (self.dense_indexes.items) |entry| {
                try paths.append(alloc, algebraic_mod.ir.vectorAccessPath(entry.config.name, .dense_vector));
            }
            for (self.sparse_indexes.items) |entry| {
                try paths.append(alloc, algebraic_mod.ir.vectorAccessPath(entry.config.name, .sparse_vector));
            }
        }
        if (expr.fragment == .graph_traverse) {
            for (self.graph_indexes.items) |entry| {
                if (!entry.index.algebraic_semiring_traversal) continue;
                try paths.append(alloc, algebraic_mod.ir.graphReachabilityAccessPath(entry.config.name));
            }
        }
        return algebraic_mod.ir.selectUniqueAccessPath(paths.items, expr);
    }

    pub fn hasGraphIndexes(self: *const IndexManager) bool {
        return self.graph_indexes.items.len > 0;
    }

    pub fn graphIndexes(self: *const IndexManager) []const GraphIndex {
        return self.graph_indexes.items;
    }

    pub fn algebraicIndex(self: *IndexManager, name: ?[]const u8) ?*AlgebraicIndex {
        if (name) |index_name| {
            for (self.algebraic_indexes.items) |*entry| {
                if (std.mem.eql(u8, entry.config.name, index_name)) return entry;
            }
            return null;
        }
        if (self.algebraic_indexes.items.len == 1) return &self.algebraic_indexes.items[0];
        return null;
    }

    /// Resolve the algebraic index that should serve an aggregation. `preferred`
    /// is the query's index_name, which usually names the *text* index, not an
    /// algebraic one: use it only when it actually names an algebraic index,
    /// otherwise fall back to the table's default algebraic index (the first
    /// one). Returns null when the table has no algebraic index.
    pub fn aggregationAlgebraicIndex(self: *IndexManager, preferred: ?[]const u8) ?*AlgebraicIndex {
        if (preferred) |name| {
            if (self.algebraicIndex(name)) |entry| return entry;
        }
        if (self.algebraic_indexes.items.len > 0) return &self.algebraic_indexes.items[0];
        return null;
    }

    fn textProjectionOptions(self: *const IndexManager, arena: Allocator) !mapper.TextProjectionOptions {
        return try self.textProjectionOptionsForSchema(arena, false);
    }

    fn textProjectionOptionsForSchema(self: *const IndexManager, arena: Allocator, schema_less_fast_projection: bool) !mapper.TextProjectionOptions {
        var vector_paths = std.ArrayListUnmanaged([]const u8).empty;
        defer vector_paths.deinit(arena);

        for (self.dense_indexes.items) |entry| {
            try appendUniqueProjectionPath(arena, &vector_paths, entry.field_name);
        }
        for (self.sparse_indexes.items) |entry| {
            try appendUniqueProjectionPath(arena, &vector_paths, entry.field_name);
        }

        const paths = try arena.dupe([]const u8, vector_paths.items);
        return .{
            .vector_field_paths = paths,
            .strip_numeric_array_heuristic = false,
            .schema_less_fast_projection = schema_less_fast_projection,
        };
    }

    fn allTextIndexesSchemaLess(self: *const IndexManager) bool {
        for (self.text_indexes.items) |entry| {
            if (entry.runtime_schema != null) return false;
        }
        return true;
    }

    pub fn indexBatch(self: *IndexManager, store: *docstore_mod.DocStore, writes: []const types.BatchWrite) !void {
        return self.indexBatchWithOptions(store, writes, .{});
    }

    pub fn indexBatchWithOptions(self: *IndexManager, store: *docstore_mod.DocStore, writes: []const types.BatchWrite, opts: IndexBatchOptions) !void {
        if (writes.len == 0) return;

        if (self.text_indexes.items.len > 0) {
            var arena_state = std.heap.ArenaAllocator.init(self.alloc);
            defer arena_state.deinit();
            const arena = arena_state.allocator();
            const source_batch = try mapper.buildTextProjectionSourceBatchFromWritesWithOptions(
                arena,
                writes,
                try self.textProjectionOptionsForSchema(arena, self.allTextIndexesSchemaLess()),
            );

            for (self.text_indexes.items) |*entry| {
                try self.indexTextProjectionSourceDocs(store, entry, source_batch.docs, opts, null);
            }
        }

        for (self.dense_indexes.items) |*entry| {
            try self.indexDenseBatchEntry(store, entry, writes, .{});
        }

        for (self.sparse_indexes.items) |*entry| {
            try self.indexSparseBatchEntryWithOptions(store, entry, writes, .{});
        }
    }

    pub fn indexSplitBatch(
        self: *IndexManager,
        store: *docstore_mod.DocStore,
        writes: []const types.BatchWrite,
        dense_handoffs: []const DenseSplitHandoff,
        text_handoffs: []const TextSplitHandoff,
        sparse_handoffs: []const SparseSplitHandoff,
    ) !void {
        if (writes.len == 0) return;

        if (self.text_indexes.items.len > 0) {
            var arena_state = std.heap.ArenaAllocator.init(self.alloc);
            defer arena_state.deinit();
            const arena = arena_state.allocator();
            const source_batch = try mapper.buildTextProjectionSourceBatchFromWritesWithOptions(
                arena,
                writes,
                try self.textProjectionOptions(arena),
            );

            for (self.text_indexes.items) |*entry| {
                const handoff = findTextSplitHandoff(text_handoffs, entry.config.name);
                try self.indexTextProjectionSourceDocs(store, entry, source_batch.docs, .{ .compact_text = false }, handoff);
            }
        }

        for (self.dense_indexes.items) |*entry| {
            const handoff = findDenseSplitHandoff(dense_handoffs, entry.config.name);
            try self.indexDenseBatchEntryWithSkip(store, entry, writes, handoff, .{});
        }

        for (self.sparse_indexes.items) |*entry| {
            const handoff = findSparseSplitHandoff(sparse_handoffs, entry.config.name);
            try self.indexSparseBatchEntryWithSkip(store, entry, writes, handoff);
        }
    }

    pub fn compactAllTextIndexes(self: *IndexManager) !void {
        for (self.text_indexes.items) |*entry| {
            if (!try self.textIndexNeedsMerge(&entry.persistent, default_merge_policy)) {
                TextMergeScheduler.noteComplete(entry);
                continue;
            }
            try self.compactTextIndex(&entry.persistent, default_merge_policy);
            TextMergeScheduler.noteComplete(entry);
        }
    }

    pub fn drainScheduledTextMerges(self: *IndexManager) !void {
        while (try self.runTextMergeScheduler(text_merge_scheduler_default_steps) > 0) {}
    }

    pub fn textMergeStats(self: *IndexManager) types.TextMergeStats {
        return self.textMergeStatsWithPrune(true);
    }

    pub fn textMergeStatsSnapshot(self: *IndexManager) types.TextMergeStats {
        return self.textMergeStatsWithPrune(false);
    }

    fn textMergeStatsWithPrune(self: *IndexManager, prune_expired: bool) types.TextMergeStats {
        const now_ns = platform_time.monotonicNs();
        if (prune_expired) self.text_merge_scheduler.pruneExpiredQuarantines(self.alloc, now_ns);
        var stats = types.TextMergeStats{
            .pending_indexes = 0,
            .in_flight_merges = @intCast(self.text_merge_scheduler.in_flight.items.len),
            .in_flight_segments = self.text_merge_scheduler.inFlightSegmentCount(),
            .completed_merges = self.text_merge_scheduler.completed_merges,
            .skipped_stale_merges = self.text_merge_scheduler.skipped_stale_merges,
            .failed_merges = self.text_merge_scheduler.failed_merges,
            .quarantined_merges = self.text_merge_scheduler.activeQuarantineCount(now_ns),
            .quarantined_segments = self.text_merge_scheduler.quarantinedSegmentCount(now_ns),
            .last_merge_error = self.text_merge_scheduler.lastMergeError(now_ns),
            .retry_after_ns = self.text_merge_scheduler.retryAfterNs(now_ns),
            .deferred_for_pressure = self.text_merge_scheduler.deferred_for_pressure,
        };

        for (self.text_indexes.items) |*entry| {
            if (!entry.compaction_pending) continue;
            stats.pending_indexes += 1;
            const snap = entry.persistent.snapshot();
            stats.pending_segments += snap.segments.len;
            for (snap.segments) |seg| stats.pending_bytes += seg.data.bytes().len;
        }
        self.accountFullTextPendingBytes(stats.pending_bytes) catch {};
        return stats;
    }

    pub fn textMergeStatsForIndex(self: *IndexManager, index_name: []const u8) types.TextMergeStats {
        return self.textMergeStatsForIndexWithPrune(index_name, true);
    }

    pub fn textMergeStatsSnapshotForIndex(self: *IndexManager, index_name: []const u8) types.TextMergeStats {
        return self.textMergeStatsForIndexWithPrune(index_name, false);
    }

    fn textMergeStatsForIndexWithPrune(self: *IndexManager, index_name: []const u8, prune_expired: bool) types.TextMergeStats {
        const now_ns = platform_time.monotonicNs();
        if (prune_expired) self.text_merge_scheduler.pruneExpiredQuarantines(self.alloc, now_ns);
        var stats = types.TextMergeStats{
            .pending_indexes = 0,
            .in_flight_merges = self.text_merge_scheduler.inFlightMergeCountForIndex(index_name),
            .in_flight_segments = self.text_merge_scheduler.inFlightSegmentCountForIndex(index_name),
            .completed_merges = self.text_merge_scheduler.completed_merges,
            .skipped_stale_merges = self.text_merge_scheduler.skipped_stale_merges,
            .failed_merges = self.text_merge_scheduler.failed_merges,
            .quarantined_merges = self.text_merge_scheduler.activeQuarantineCountForIndex(index_name, now_ns),
            .quarantined_segments = self.text_merge_scheduler.quarantinedSegmentCountForIndex(index_name, now_ns),
            .last_merge_error = self.text_merge_scheduler.lastMergeErrorForIndex(index_name, now_ns),
            .retry_after_ns = self.text_merge_scheduler.retryAfterNsForIndex(index_name, now_ns),
            .deferred_for_pressure = self.text_merge_scheduler.deferred_for_pressure,
        };

        const entry = self.textIndexEntry(index_name) orelse return stats;
        if (entry.compaction_pending) {
            stats.pending_indexes = 1;
            const snap = entry.persistent.snapshot();
            stats.pending_segments = snap.segments.len;
            for (snap.segments) |seg| stats.pending_bytes += seg.data.bytes().len;
        }
        return stats;
    }

    pub fn runTextMergeScheduler(self: *IndexManager, max_steps: usize) !usize {
        var completed: usize = 0;
        while (completed < max_steps) {
            var task = (try self.beginTextMergeTask()) orelse break;
            defer task.deinit(self.alloc);
            errdefer self.cancelTextMergeTask(&task);
            var result = executeTextMergeTask(self.alloc, &task) catch |err| switch (err) {
                error.ResourceBudgetExceeded => {
                    self.cancelTextMergeTask(&task);
                    break;
                },
                else => return err,
            };
            defer result.deinit(self.alloc);
            _ = try self.finishTextMergeTask(&task, &result);
            completed += 1;
        }
        return completed;
    }

    pub fn beginSparseCompactionTask(self: *IndexManager) !?SparseCompactionTask {
        for (self.sparse_indexes.items) |*entry| {
            var task = (try entry.index.beginSegmentCompactionTask(self.alloc, .{})) orelse continue;
            errdefer task.deinit(self.alloc);
            return .{
                .index_name = try self.alloc.dupe(u8, entry.config.name),
                .chunk_size = entry.index.chunk_size,
                .task = task,
            };
        }
        return null;
    }

    pub fn executeSparseCompactionTask(alloc: Allocator, task: *const SparseCompactionTask) !SparseCompactionResult {
        return try sparse_mod.SparseIndex.executeSegmentCompactionTask(alloc, &task.task, task.chunk_size);
    }

    pub fn finishSparseCompactionTask(self: *IndexManager, task: *const SparseCompactionTask, result: *SparseCompactionResult) !bool {
        const entry = self.findSparseIndexEntry(task.index_name) orelse return false;
        return try entry.index.finishSegmentCompactionTask(&task.task, result);
    }

    pub fn runSparseCompactionScheduler(self: *IndexManager, max_steps: usize) !usize {
        var completed: usize = 0;
        while (completed < max_steps) {
            var task = (try self.beginSparseCompactionTask()) orelse break;
            defer task.deinit(self.alloc);
            var result = try executeSparseCompactionTask(self.alloc, &task);
            defer result.deinit(self.alloc);
            _ = try self.finishSparseCompactionTask(&task, &result);
            completed += 1;
        }
        return completed;
    }

    pub fn forceCompactAllTextIndexes(self: *IndexManager) !void {
        try self.forceCompactAllTextIndexesWithOptions(.{});
    }

    pub fn bestEffortForceCompactAllTextIndexes(self: *IndexManager) !void {
        try self.forceCompactAllTextIndexesWithOptions(.{ .mode = .best_effort });
    }

    fn forceCompactAllTextIndexesWithOptions(self: *IndexManager, options: ForceTextCompactOptions) !void {
        for (self.text_indexes.items) |*entry| {
            if (!try self.textIndexNeedsMerge(&entry.persistent, default_merge_policy)) {
                TextMergeScheduler.noteComplete(entry);
                continue;
            }
            const fully_compacted = try self.forceCompactTextIndexWithOptions(entry, options);
            if (fully_compacted) {
                TextMergeScheduler.noteComplete(entry);
            } else {
                TextMergeScheduler.schedule(entry);
                if (options.mode == .best_effort) return;
            }
        }
    }

    pub fn deleteBatch(self: *IndexManager, store: *docstore_mod.DocStore, keys: []const []const u8) !void {
        if (keys.len == 0) return;

        for (self.text_indexes.items) |*entry| {
            const stats = try self.deleteTextBatchEntry(entry, keys);
            try self.finalizeTextBatchMutations(entry, .{}, stats);
        }

        for (self.dense_indexes.items) |*entry| {
            try self.deleteDenseBatchEntry(store, entry, keys, .{});
        }

        for (self.sparse_indexes.items) |*entry| {
            try self.deleteSparseBatchEntry(entry, keys);
        }
    }

    pub fn deleteBatchWithoutText(self: *IndexManager, store: *docstore_mod.DocStore, keys: []const []const u8) !void {
        if (keys.len == 0) return;

        for (self.dense_indexes.items) |*entry| {
            try self.deleteDenseBatchEntry(store, entry, keys, .{});
        }

        for (self.sparse_indexes.items) |*entry| {
            try self.deleteSparseBatchEntry(entry, keys);
        }
    }

    pub fn deleteGraphDocs(self: *IndexManager, keys: []const []const u8) !void {
        if (keys.len == 0 or self.graph_indexes.items.len == 0) return;

        for (self.graph_indexes.items) |*entry| {
            try self.deleteGraphDocsEntry(entry, keys);
        }
    }

    pub fn deleteGraphDocInIndexes(self: *IndexManager, key: []const u8, index_names: []const []const u8) !void {
        if (index_names.len == 0) return;

        for (index_names) |index_name| {
            const entry = self.graphIndex(index_name) orelse return error.IndexNotFound;
            try self.deleteGraphDocsEntry(entry, &.{key});
        }
    }

    pub fn applyGraphWrites(self: *IndexManager, writes: []const types.GraphEdgeWrite) !void {
        if (writes.len == 0) return;

        for (self.graph_indexes.items) |*entry| {
            try self.applyGraphWritesEntry(entry, writes);
        }
    }

    pub fn applyGraphDeletes(self: *IndexManager, deletes: []const types.GraphEdgeDelete) !void {
        if (deletes.len == 0) return;

        for (self.graph_indexes.items) |*entry| {
            try self.applyGraphDeletesEntry(entry, deletes);
        }
    }

    pub fn applyGraphMutations(self: *IndexManager, writes: []const types.GraphEdgeWrite, deletes: []const types.GraphEdgeDelete) !void {
        if (writes.len == 0 and deletes.len == 0) return;

        for (self.graph_indexes.items) |*entry| {
            try self.applyGraphMutationsEntry(entry, writes, deletes);
        }
    }

    const OwnedDenseInsertItems = struct {
        items: std.ArrayListUnmanaged(hbc_mod.BatchInsertItem) = .empty,
        owned_vectors: std.ArrayListUnmanaged([]f32) = .empty,
        owned_metadata: std.ArrayListUnmanaged([]u8) = .empty,
        arena_owner: ?std.heap.ArenaAllocator = null,

        fn ensureArena(self: *@This(), alloc: Allocator) !Allocator {
            if (self.arena_owner == null) {
                self.arena_owner = std.heap.ArenaAllocator.init(alloc);
            }
            return self.arena_owner.?.allocator();
        }

        fn deinit(self: *@This(), alloc: Allocator) void {
            if (self.arena_owner) |*arena| arena.deinit();
            for (self.owned_vectors.items) |vector| alloc.free(vector);
            for (self.owned_metadata.items) |metadata| alloc.free(metadata);
            self.owned_vectors.deinit(alloc);
            self.owned_metadata.deinit(alloc);
            self.items.deinit(alloc);
            self.* = .{};
        }

        fn appendBorrowed(
            self: *@This(),
            alloc: Allocator,
            vector_id: u64,
            vector: []const f32,
            metadata: []const u8,
        ) !void {
            if (self.arena_owner != null) {
                const arena = try self.ensureArena(alloc);
                const owned_vector = try arena.dupe(f32, vector);
                const owned_metadata = try arena.dupe(u8, metadata);
                try self.items.append(alloc, .{
                    .vector_id = vector_id,
                    .vector = owned_vector,
                    .metadata = owned_metadata,
                });
                return;
            }
            const owned_vector = try alloc.dupe(f32, vector);
            errdefer alloc.free(owned_vector);
            try self.owned_vectors.append(alloc, owned_vector);
            errdefer _ = self.owned_vectors.pop();

            const owned_metadata = try alloc.dupe(u8, metadata);
            errdefer alloc.free(owned_metadata);
            try self.owned_metadata.append(alloc, owned_metadata);
            errdefer _ = self.owned_metadata.pop();

            try self.items.append(alloc, .{
                .vector_id = vector_id,
                .vector = owned_vector,
                .metadata = owned_metadata,
            });
        }

        fn appendOwnedVector(
            self: *@This(),
            alloc: Allocator,
            vector_id: u64,
            vector: []f32,
            metadata: []const u8,
        ) !void {
            if (self.arena_owner != null) {
                const arena = try self.ensureArena(alloc);
                const owned_vector = try arena.dupe(f32, vector);
                errdefer alloc.free(vector);
                const owned_metadata = try arena.dupe(u8, metadata);
                alloc.free(vector);
                try self.items.append(alloc, .{
                    .vector_id = vector_id,
                    .vector = owned_vector,
                    .metadata = owned_metadata,
                });
                return;
            }
            try self.owned_vectors.append(alloc, vector);
            errdefer _ = self.owned_vectors.pop();

            const owned_metadata = try alloc.dupe(u8, metadata);
            errdefer alloc.free(owned_metadata);
            try self.owned_metadata.append(alloc, owned_metadata);
            errdefer _ = self.owned_metadata.pop();

            try self.items.append(alloc, .{
                .vector_id = vector_id,
                .vector = vector,
                .metadata = owned_metadata,
            });
        }

        fn appendBorrowedVectorOwnedMetadata(
            self: *@This(),
            alloc: Allocator,
            vector_id: u64,
            vector: []const f32,
            metadata: []const u8,
        ) !void {
            const owned_metadata = if (self.arena_owner != null) blk: {
                const arena = try self.ensureArena(alloc);
                break :blk try arena.dupe(u8, metadata);
            } else blk: {
                const owned = try alloc.dupe(u8, metadata);
                errdefer alloc.free(owned);
                try self.owned_metadata.append(alloc, owned);
                errdefer _ = self.owned_metadata.pop();
                break :blk owned;
            };

            try self.items.append(alloc, .{
                .vector_id = vector_id,
                .vector = vector,
                .metadata = owned_metadata,
            });
        }
    };

    const DenseVectorIdAssignment = struct {
        vector_id: u64,
        needs_mapping: bool,
        can_assume_absent: bool,
    };

    const DenseVectorMetadataState = enum {
        absent,
        matches,
        conflicts,
    };

    const PendingDenseVectorMapping = struct {
        doc_key: []const u8,
        parent_doc_key: ?[]const u8 = null,
        vector_id: u64,
    };

    const DenseOrdinalVectorCacheUpdate = struct {
        ordinal: doc_identity.DocOrdinal,
        vector_id: u64,
    };

    pub fn applyDenseEmbeddingWrites(self: *IndexManager, store: *docstore_mod.DocStore, writes: []const mapper.DenseEmbeddingWrite) !void {
        return try self.applyDenseEmbeddingWritesWithOptions(store, writes, .{});
    }

    pub fn applyDenseEmbeddingWritesWithOptions(
        self: *IndexManager,
        store: *docstore_mod.DocStore,
        writes: []const mapper.DenseEmbeddingWrite,
        batch_options: StoreBatchOptions,
    ) !void {
        if (writes.len == 0) return;

        for (self.dense_indexes.items) |*entry| {
            var store_batch = try store.beginWriteBatchWithOptions(batch_options);
            errdefer store_batch.abort();
            const store_txn = store_batch.asTxn();

            var items: OwnedDenseInsertItems = .{};
            defer items.deinit(self.alloc);
            _ = try items.ensureArena(self.alloc);
            var pending_mappings = std.ArrayListUnmanaged(PendingDenseVectorMapping).empty;
            defer pending_mappings.deinit(self.alloc);
            var all_vector_ids_new = true;

            for (writes) |write| {
                if (!self.keyInRange(write.doc_key)) continue;
                if (!std.mem.eql(u8, write.index_name, entry.config.name)) continue;
                if (entry.dims != write.vector.len) return error.InvalidVectorDimensions;

                if (write.artifact_key == null) {
                    const artifact_name = entry.embedding_name orelse entry.config.name;
                    try self.writeDenseEmbeddingArtifactTxn(store_txn, write.doc_key, write.doc_key, artifact_name, "_embeddings", null, write.vector);
                }
                const assignment = try self.ensureDenseVectorIdTxn(store_txn, write.index_name, write.doc_key, write.parent_doc_key);
                all_vector_ids_new = all_vector_ids_new and assignment.can_assume_absent;
                try items.appendBorrowed(self.alloc, assignment.vector_id, write.vector, write.doc_key);
                try pending_mappings.append(self.alloc, .{
                    .doc_key = items.items.items[items.items.items.len - 1].metadata,
                    .parent_doc_key = write.parent_doc_key,
                    .vector_id = assignment.vector_id,
                });
            }

            if (items.items.items.len == 0) continue;
            try entry.index.batchInsertWithMetadataOptions(
                items.items.items,
                denseHbcBatchOptions(batch_options, all_vector_ids_new, entry.index.hasExternalVectorLoader()),
            );
            try self.commitDenseVectorMappingsWithRollback(&store_batch, store_txn, entry, entry.config.name, pending_mappings.items);
        }
    }

    pub fn applySparseEmbeddingWrites(self: *IndexManager, store: *docstore_mod.DocStore, writes: []const mapper.SparseEmbeddingWrite) !void {
        if (writes.len == 0) return;

        for (self.sparse_indexes.items) |*entry| {
            try self.applySparseEmbeddingWritesEntry(store, entry, writes, .{});
        }
    }

    pub fn indexTextBatchByName(self: *IndexManager, store: *docstore_mod.DocStore, index_name: []const u8, writes: []const types.BatchWrite) !void {
        return try self.indexTextBatchByNameWithOptions(store, index_name, writes, .{});
    }

    pub fn indexTextBatchByNameWithOptions(self: *IndexManager, store: *docstore_mod.DocStore, index_name: []const u8, writes: []const types.BatchWrite, opts: IndexBatchOptions) !void {
        if (writes.len == 0) return;
        for (self.text_indexes.items) |*entry| {
            if (std.mem.eql(u8, entry.config.name, index_name)) {
                const stats = try self.indexTextBatchForConfig(store, entry, writes);
                try self.finalizeTextBatchMutations(entry, opts, stats);
                return;
            }
        }
        return error.IndexNotFound;
    }

    pub fn deleteTextBatchByName(self: *IndexManager, index_name: []const u8, keys: []const []const u8) !void {
        return try self.deleteTextBatchByNameWithOptions(index_name, keys, .{});
    }

    pub fn deleteTextBatchByNameWithOptions(self: *IndexManager, index_name: []const u8, keys: []const []const u8, opts: IndexBatchOptions) !void {
        if (keys.len == 0) return;
        for (self.text_indexes.items) |*entry| {
            if (std.mem.eql(u8, entry.config.name, index_name)) {
                const stats = try self.deleteTextBatchEntry(entry, keys);
                try self.finalizeTextBatchMutations(entry, opts, stats);
                return;
            }
        }
        return error.IndexNotFound;
    }

    pub fn applyTextBatchByNameWithOptions(
        self: *IndexManager,
        store: *docstore_mod.DocStore,
        index_name: []const u8,
        delete_keys: []const []const u8,
        writes: []const types.BatchWrite,
        opts: IndexBatchOptions,
    ) !void {
        if (delete_keys.len == 0 and writes.len == 0) return;
        for (self.text_indexes.items) |*entry| {
            if (std.mem.eql(u8, entry.config.name, index_name)) {
                var stats = TextBatchMutationStats{};
                if (delete_keys.len > 0) {
                    stats.noteDelete((try self.deleteTextBatchEntry(entry, delete_keys)).deleted_any);
                }
                if (writes.len > 0) {
                    const index_stats = try self.indexTextBatchForConfig(store, entry, writes);
                    stats.noteIndex(index_stats.indexed_any);
                }
                try self.finalizeTextBatchMutations(entry, opts, stats);
                return;
            }
        }
        return error.IndexNotFound;
    }

    pub fn indexDenseBatchByName(self: *IndexManager, store: *docstore_mod.DocStore, index_name: []const u8, writes: []const types.BatchWrite) !void {
        return try self.indexDenseBatchByNameWithOptions(store, index_name, writes, .{});
    }

    pub fn indexDenseBatchByNameWithOptions(
        self: *IndexManager,
        store: *docstore_mod.DocStore,
        index_name: []const u8,
        writes: []const types.BatchWrite,
        batch_options: StoreBatchOptions,
    ) !void {
        if (writes.len == 0) return;
        const entry = self.denseIndex(index_name) orelse return error.IndexNotFound;
        try self.indexDenseBatchEntry(store, entry, writes, batch_options);
    }

    pub fn deleteDenseBatchByName(self: *IndexManager, store: *docstore_mod.DocStore, index_name: []const u8, keys: []const []const u8) !void {
        return try self.deleteDenseBatchByNameWithOptions(store, index_name, keys, .{});
    }

    pub fn deleteDenseBatchByNameWithOptions(
        self: *IndexManager,
        store: *docstore_mod.DocStore,
        index_name: []const u8,
        keys: []const []const u8,
        batch_options: StoreBatchOptions,
    ) !void {
        if (keys.len == 0) return;
        const entry = self.denseIndex(index_name) orelse return error.IndexNotFound;
        try self.deleteDenseBatchEntry(store, entry, keys, batch_options);
    }

    pub fn indexSparseBatchByName(self: *IndexManager, store: *docstore_mod.DocStore, index_name: []const u8, writes: []const types.BatchWrite) !void {
        return try self.indexSparseBatchByNameWithOptions(store, index_name, writes, .{});
    }

    pub fn indexSparseBatchByNameWithOptions(self: *IndexManager, store: *docstore_mod.DocStore, index_name: []const u8, writes: []const types.BatchWrite, batch_options: StoreBatchOptions) !void {
        if (writes.len == 0) return;
        const entry = self.sparseIndex(index_name) orelse return error.IndexNotFound;
        try self.indexSparseBatchEntryWithOptions(store, entry, writes, batch_options);
    }

    pub fn sparseFieldNameByName(self: *IndexManager, index_name: []const u8) ?[]const u8 {
        const entry = self.sparseIndex(index_name) orelse return null;
        return entry.field_name;
    }

    pub fn indexSparsePreparedWritesByNameWithOptions(self: *IndexManager, index_name: []const u8, writes: []const sparse_mod.SparseWrite, batch_options: StoreBatchOptions) !void {
        if (writes.len == 0) return;
        const entry = self.sparseIndex(index_name) orelse return error.IndexNotFound;
        try self.indexSparsePreparedWritesEntryWithOptions(entry, writes, batch_options);
    }

    pub fn deleteSparseBatchByName(self: *IndexManager, index_name: []const u8, keys: []const []const u8) !void {
        return try self.deleteSparseBatchByNameWithOptions(index_name, keys, .{});
    }

    pub fn deleteSparseBatchByNameWithOptions(self: *IndexManager, index_name: []const u8, keys: []const []const u8, batch_options: StoreBatchOptions) !void {
        if (keys.len == 0) return;
        const entry = self.sparseIndex(index_name) orelse return error.IndexNotFound;
        try self.deleteSparseBatchEntryWithOptions(entry, keys, batch_options);
    }

    pub fn deleteGraphDocsByName(self: *IndexManager, index_name: []const u8, keys: []const []const u8) !void {
        if (keys.len == 0) return;
        const entry = self.graphIndex(index_name) orelse return error.IndexNotFound;
        try self.deleteGraphDocsEntry(entry, keys);
    }

    pub fn applyAlgebraicBatchByName(
        self: *IndexManager,
        store: *docstore_mod.DocStore,
        index_name: []const u8,
        batch: derived_types.DerivedBatch,
    ) !void {
        return try self.applyAlgebraicBatchByNameWithOptions(store, index_name, batch, .{});
    }

    pub fn applyAlgebraicBatchByNameWithOptions(
        self: *IndexManager,
        store: *docstore_mod.DocStore,
        index_name: []const u8,
        batch: derived_types.DerivedBatch,
        batch_options: StoreBatchOptions,
    ) !void {
        const entry = self.algebraicIndex(index_name) orelse return error.IndexNotFound;
        try entry.index.applyBatchWithOptions(store, batch, .{ .batch_options = batch_options });
    }

    pub fn applyGraphWritesByName(self: *IndexManager, index_name: []const u8, writes: []const types.GraphEdgeWrite) !void {
        if (writes.len == 0) return;
        const entry = self.graphIndex(index_name) orelse return error.IndexNotFound;
        try self.applyGraphWritesEntry(entry, writes);
    }

    pub fn applyGraphDeletesByName(self: *IndexManager, index_name: []const u8, deletes: []const types.GraphEdgeDelete) !void {
        if (deletes.len == 0) return;
        const entry = self.graphIndex(index_name) orelse return error.IndexNotFound;
        try self.applyGraphDeletesEntry(entry, deletes);
    }

    pub fn applyGraphMutationsByName(
        self: *IndexManager,
        index_name: []const u8,
        writes: []const types.GraphEdgeWrite,
        deletes: []const types.GraphEdgeDelete,
    ) !void {
        if (writes.len == 0 and deletes.len == 0) return;
        const entry = self.graphIndex(index_name) orelse return error.IndexNotFound;
        try self.applyGraphMutationsEntry(entry, writes, deletes);
    }

    pub fn applyDenseEmbeddingWritesByName(self: *IndexManager, store: *docstore_mod.DocStore, index_name: []const u8, writes: []const mapper.DenseEmbeddingWrite) !void {
        return try self.applyDenseEmbeddingWritesByNameWithOptions(store, index_name, writes, .{});
    }

    pub fn applyDenseEmbeddingWritesByNameWithOptions(
        self: *IndexManager,
        store: *docstore_mod.DocStore,
        index_name: []const u8,
        writes: []const mapper.DenseEmbeddingWrite,
        batch_options: StoreBatchOptions,
    ) !void {
        if (writes.len == 0) return;
        const entry = self.denseIndex(index_name) orelse return error.IndexNotFound;
        try self.applyDenseEmbeddingWritesEntry(store, entry, writes, batch_options);
    }

    pub fn applySparseEmbeddingWritesByName(self: *IndexManager, store: *docstore_mod.DocStore, index_name: []const u8, writes: []const mapper.SparseEmbeddingWrite) !void {
        return try self.applySparseEmbeddingWritesByNameWithOptions(store, index_name, writes, .{});
    }

    pub fn applySparseEmbeddingWritesByNameWithOptions(
        self: *IndexManager,
        store: *docstore_mod.DocStore,
        index_name: []const u8,
        writes: []const mapper.SparseEmbeddingWrite,
        batch_options: StoreBatchOptions,
    ) !void {
        if (writes.len == 0) return;
        const entry = self.sparseIndex(index_name) orelse return error.IndexNotFound;
        try self.applySparseEmbeddingWritesEntry(store, entry, writes, batch_options);
    }

    /// Reconstruct any relational typed-row values in a freshly scanned set of
    /// document rows into canonical JSON, in place. Backfill scans read the raw
    /// stored value (a typed row for relational tables); rewriting each row's
    /// value to JSON here means every downstream consumer (text segment build,
    /// dense/sparse vector field extraction) sees a document exactly as in
    /// document mode, with no further per-consumer changes. Non-document and
    /// JSON-blob rows are left untouched. The pair's `value` is owned, so the old
    /// buffer is freed and replaced with the reconstructed bytes.
    fn materializeScannedDocumentRows(self: *IndexManager, rows: []backend_scan.OwnedKVPair) !void {
        for (rows) |*row| {
            if (!mapper.isRelationalRowValue(row.value)) continue;
            row.value = try mapper.materializeOwnedDocumentValueAlloc(self.alloc, row.value);
        }
    }

    fn backfillTextIndex(self: *IndexManager, store: *docstore_mod.DocStore, entry: *TextIndex, resume_from: ?[]const u8) !void {
        const rebuild_state = backfill_state_mod.RebuildState.init(entry.rebuild_root_path);
        var runtime_store = try initRuntimeStore(self.alloc, store);
        defer runtime_store.deinit();

        const lower = try internal_keys.documentRangeLowerAlloc(self.alloc, self.byte_range.start);
        defer self.alloc.free(lower);
        const upper = try internal_keys.documentRangeUpperAlloc(self.alloc, if (self.byte_range.end.len > 0) self.byte_range.end else "");
        defer if (upper) |buf| self.alloc.free(buf);

        const docs = try backend_scan.scanRange(self.alloc, &runtime_store.store, lower, if (upper) |buf| buf else "");
        defer backend_scan.freeResults(self.alloc, docs);
        try self.materializeScannedDocumentRows(docs);
        var identity_txn = try runtime_store.store.beginRead();
        defer identity_txn.abort();

        var mapped_docs = std.ArrayListUnmanaged(mapper.MapperDoc).empty;
        defer mapped_docs.deinit(self.alloc);
        var owned_doc_ids = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (owned_doc_ids.items) |key| self.alloc.free(key);
            owned_doc_ids.deinit(self.alloc);
        }

        var flushed_batches: usize = 0;
        var saw_visible_doc = false;
        var max_flushed_key: ?[]const u8 = null;

        const flush_batch = struct {
            fn run(
                manager: *IndexManager,
                doc_store: *docstore_mod.DocStore,
                text_entry: *TextIndex,
                rebuild: backfill_state_mod.RebuildState,
                docs_buf: *std.ArrayListUnmanaged(mapper.MapperDoc),
                last_doc_key: []const u8,
                flush_count: *usize,
            ) !void {
                var built = try mapper.buildTextSegmentsFromDocumentsWithMetadata(manager.alloc, docs_buf.items, text_entry.text_analysis, text_entry.runtime_schema, .{
                    .target_segment_bytes = @intCast(default_merge_policy.max_segment_size),
                });
                defer built.deinit(manager.alloc);
                if (built.observed_field_analyzers.len > 0) {
                    try mergeObservedTextFieldAnalyzers(manager, doc_store, text_entry, built.observed_field_analyzers);
                }
                for (built.segments) |*seg| {
                    const owned = seg.*;
                    seg.* = &.{};
                    try text_entry.persistent.indexSegmentOwned(owned);
                }
                try rebuild.update(last_doc_key);
                docs_buf.clearRetainingCapacity();
                flush_count.* += 1;
                if (@import("builtin").is_test) {
                    if (test_abort_text_backfill_after_batches) |limit| {
                        if (flush_count.* >= limit) return error.TestInjectedBackfillFailure;
                    }
                }
            }
        }.run;

        for (docs) |doc| {
            if (isMetadataKey(doc.key)) continue;
            if (!self.keyInRange(doc.key)) continue;
            if (!try textIndexShouldConsumeDoc(self, entry, doc.key)) continue;
            if (resume_from) |resume_key| {
                if (resume_key.len > 0 and std.mem.order(u8, doc.key, resume_key) != .gt) continue;
            }

            saw_visible_doc = true;
            const doc_id = if (internal_keys.isPrimaryDocumentKey(doc.key))
                (try internal_keys.decodePrimaryDocumentKeyAlloc(self.alloc, doc.key)) orelse continue
            else
                try self.alloc.dupe(u8, doc.key);
            try owned_doc_ids.append(self.alloc, doc_id);
            try mapped_docs.append(self.alloc, .{
                .key = doc_id,
                .value = doc.value,
                .doc_ordinal = try doc_identity.lookupOrdinalTxn(self.alloc, &identity_txn, doc_id),
            });
            if (max_flushed_key == null or std.mem.order(u8, doc.key, max_flushed_key.?) == .gt) {
                max_flushed_key = doc.key;
            }

            if (mapped_docs.items.len >= text_backfill_batch_size) {
                try flush_batch(self, store, entry, rebuild_state, &mapped_docs, max_flushed_key.?, &flushed_batches);
            }
        }

        if (mapped_docs.items.len > 0) {
            try flush_batch(self, store, entry, rebuild_state, &mapped_docs, max_flushed_key.?, &flushed_batches);
        }

        if (!saw_visible_doc or flushed_batches > 0) try rebuild_state.clear();
    }

    fn indexPath(self: *const IndexManager, name: []const u8) ![]u8 {
        return std.fmt.allocPrint(self.alloc, "{s}/indexes/{s}", .{ self.base_path, name });
    }

    fn configRequiresEnrichmentReplay(self: *const IndexManager, cfg: types.IndexConfig) !bool {
        switch (cfg.kind) {
            .dense_vector => {
                if (try parseDenseGeneratorConfig(self.alloc, cfg.config_json)) |generator| {
                    defer generator.deinit(self.alloc);
                    return true;
                }
                const dense_cfg = try parseDenseConfig(self.alloc, cfg.config_json);
                defer dense_cfg.deinit(self.alloc);
                return dense_cfg.embedding_name != null and !dense_cfg.external and self.getEnrichment(.embedding, dense_cfg.embedding_name.?) != null;
            },
            .sparse_vector => {
                if (try parseSparseGeneratorConfig(self.alloc, cfg.config_json)) |generator| {
                    defer generator.deinit(self.alloc);
                    return true;
                }
                return false;
            },
            else => return false,
        }
    }

    fn saveBackfilledAppliedSequence(self: *IndexManager, store: anytype, cfg: types.IndexConfig) !void {
        if (try self.configRequiresEnrichmentReplay(cfg)) return;
        if (try self.configHasPendingSparseEmbeddingArtifactReplay(store, cfg)) return;
        try apply_state.saveAppliedSequenceWithCheckpoint(
            self.alloc,
            store,
            self.applied_sequence_checkpoint_path,
            cfg.name,
            store.lastReplaySequence(0),
        );
    }

    fn configHasPendingSparseEmbeddingArtifactReplay(self: *IndexManager, store: anytype, cfg: types.IndexConfig) !bool {
        if (cfg.kind != .sparse_vector) return false;
        const expected_embedding_name = try self.sparseConfigEmbeddingNameAlloc(cfg);
        defer self.alloc.free(expected_embedding_name);

        const Context = struct {
            alloc: Allocator,
            expected_embedding_name: []const u8,
            found: bool = false,

            fn handle(ctx: *@This(), _: u64, payload: []const u8) !void {
                if (ctx.found) return;
                var decoded = try change_journal_mod.decodeRecord(ctx.alloc, payload);
                defer decoded.deinit();
                for (decoded.record.changed_artifact_keys) |artifact_key| {
                    if (internal_keys.isEmbeddingArtifactKey(artifact_key) and internal_keys.matchesEmbeddingArtifactName(artifact_key, ctx.expected_embedding_name)) {
                        ctx.found = true;
                        return;
                    }
                    if (internal_keys.isDerivedEmbeddingArtifactKey(artifact_key) and internal_keys.matchesDerivedEmbeddingArtifactName(artifact_key, ctx.expected_embedding_name)) {
                        ctx.found = true;
                        return;
                    }
                }
            }
        };

        var ctx = Context{
            .alloc = self.alloc,
            .expected_embedding_name = expected_embedding_name,
        };
        store.forEachReplayEntryFromHint(0, .sparse_vector, &ctx, Context.handle) catch |err| switch (err) {
            error.ReplayIndexUnavailable => return false,
            else => return err,
        };
        return ctx.found;
    }

    fn sparseConfigEmbeddingNameAlloc(self: *IndexManager, cfg: types.IndexConfig) ![]u8 {
        if (try parseSparseGeneratorConfig(self.alloc, cfg.config_json)) |generator| {
            defer generator.deinit(self.alloc);
            if (generator.embedding_name) |embedding_name| return try self.alloc.dupe(u8, embedding_name);
        }
        return try self.alloc.dupe(u8, cfg.name);
    }

    fn appendOpenedIndex(self: *IndexManager, opened: OpenedIndex) !void {
        switch (opened) {
            .full_text => |entry| {
                try self.text_indexes.append(self.alloc, entry);
                if (entry.compaction_pending) {
                    TextMergeScheduler.schedule(&self.text_indexes.items[self.text_indexes.items.len - 1]);
                }
            },
            .dense_vector => |entry| try self.dense_indexes.append(self.alloc, entry),
            .sparse_vector => |entry| try self.sparse_indexes.append(self.alloc, entry),
            .graph => |entry| try self.graph_indexes.append(self.alloc, entry),
            .algebraic => |entry| try self.algebraic_indexes.append(self.alloc, entry),
        }
    }

    fn ensureConfiguredIndexDir(self: *IndexManager, cfg: types.IndexConfig) !void {
        const path = try self.indexPath(cfg.name);
        defer self.alloc.free(path);

        switch (cfg.kind) {
            .full_text => {
                if (self.text_lsm_storage == null) {
                    try ensureIndexDir(self.alloc, self.base_path, path);
                }
            },
            .dense_vector, .sparse_vector, .graph => try ensureIndexDir(self.alloc, self.base_path, path),
            .algebraic => {},
        }
    }

    fn openConfiguredIndex(self: *IndexManager, store: anytype, cfg: types.IndexConfig, allow_backfill: bool, read_only: bool) !void {
        try self.ensureConfiguredIndexDir(cfg);
        var opened = try self.openConfiguredIndexDetached(store, cfg, allow_backfill, read_only);
        errdefer opened.deinit(self);
        try self.appendOpenedIndex(opened);
    }

    fn openConfiguredIndexDetached(self: *IndexManager, store: anytype, cfg: types.IndexConfig, allow_backfill: bool, read_only: bool) !OpenedIndex {
        try self.ensureConfiguredIndexDir(cfg);
        switch (cfg.kind) {
            .full_text => {
                const started_ns = nowNs();
                var backfill_ns: u64 = 0;
                const text_cfg = try parseTextConfig(self.alloc, cfg.config_json);
                defer text_cfg.deinit(self.alloc);

                const path = try self.indexPath(cfg.name);
                defer self.alloc.free(path);

                const zpath = try self.alloc.dupeZ(u8, path);
                defer self.alloc.free(zpath);

                const persistent_opts = persistent_mod.PersistentIndexOptions{
                    .path = zpath,
                    .main_backend = self.text_main_backend,
                    .main_lsm_storage = self.text_lsm_storage,
                    .wal_storage = self.text_lsm_storage,
                    .main_lsm_options = self.text_main_lsm_options,
                    .wal_lsm_options = self.text_wal_lsm_options,
                    .lsm_cache = self.lsm_cache,
                    .lsm_root_generation = self.lsm_root_generation,
                    .read_only = read_only,
                    .main_no_sync = self.relaxed_split_durability,
                    .main_no_meta_sync = self.relaxed_split_durability,
                    .wal_no_sync = self.relaxed_split_durability,
                };
                var persistent = openTextPersistentIndexWithRetry(self.alloc, persistent_opts) catch |err| {
                    std.log.warn("full_text open failed step=persistent_open name={s} err={s}", .{
                        cfg.name,
                        @errorName(err),
                    });
                    return err;
                };
                var persistent_moved = false;
                errdefer if (!persistent_moved) persistent.close();
                const runtime_schema = loadRuntimeSchemaForTextIndex(self.alloc, store, cfg.name) catch |err| {
                    std.log.warn("full_text open failed step=runtime_schema name={s} err={s}", .{
                        cfg.name,
                        @errorName(err),
                    });
                    return err;
                };
                var runtime_schema_moved = false;
                errdefer if (!runtime_schema_moved) {
                    if (runtime_schema) |schema| schema_mod.freeSchema(self.alloc, schema);
                };
                var text_analysis = parseTextAnalysisForTextIndex(self.alloc, cfg.config_json, runtime_schema) catch |err| {
                    std.log.warn("full_text open failed step=text_analysis name={s} err={s}", .{
                        cfg.name,
                        @errorName(err),
                    });
                    return err;
                };
                var text_analysis_moved = false;
                errdefer if (!text_analysis_moved) introducer_mod.freeTextAnalysisConfig(self.alloc, text_analysis);
                const observed_field_analyzers = loadObservedTextFieldAnalyzers(self.alloc, store, cfg.name) catch |err| {
                    std.log.warn("full_text open failed step=observed_field_analyzers name={s} err={s}", .{
                        cfg.name,
                        @errorName(err),
                    });
                    return err;
                };
                var observed_field_analyzers_moved = false;
                errdefer if (!observed_field_analyzers_moved) freeObservedTextFieldAnalyzers(self.alloc, observed_field_analyzers);
                try appendObservedFieldAnalyzers(self.alloc, &text_analysis, observed_field_analyzers);
                if (!read_only) try publishFullTextDictionaryRegistry(store, self.alloc, cfg.name, text_analysis);
                const apply_mutex = try self.allocIndexApplyMutex();
                var apply_mutex_owned = true;
                errdefer if (apply_mutex_owned) self.destroyIndexApplyMutex(apply_mutex);

                var entry = TextIndex{
                    .apply_mutex = apply_mutex,
                    .config = try types.IndexConfig.clone(self.alloc, cfg),
                    .chunk_name = if (text_cfg.source_artifact_name) |name| try self.alloc.dupe(u8, name) else null,
                    .text_analysis = text_analysis,
                    .observed_field_analyzers = observed_field_analyzers,
                    .runtime_schema = runtime_schema,
                    .rebuild_root_path = try self.alloc.dupe(u8, path),
                    .persistent = persistent,
                };
                apply_mutex_owned = false;
                persistent_moved = true;
                runtime_schema_moved = true;
                text_analysis_moved = true;
                observed_field_analyzers_moved = true;
                errdefer {
                    self.freeTextIndexEntry(&entry);
                }

                const persisted_ranges = entry.persistent.activeSegmentRanges(self.alloc) catch |err| {
                    std.log.warn("full_text open failed step=active_segment_ranges name={s} err={s}", .{
                        cfg.name,
                        @errorName(err),
                    });
                    return err;
                };
                defer {
                    for (persisted_ranges) |*range| range.deinit(self.alloc);
                    self.alloc.free(persisted_ranges);
                }

                const rebuild_state = backfill_state_mod.RebuildState.init(entry.rebuild_root_path);
                const resume_from = rebuild_state.check(self.alloc) catch |err| {
                    std.log.warn("full_text open failed step=rebuild_state_check name={s} err={s}", .{
                        cfg.name,
                        @errorName(err),
                    });
                    return err;
                };
                defer if (resume_from) |buf| self.alloc.free(buf);

                if (allow_backfill and (resume_from != null or (entry.persistent.snapshot().global_doc_count == 0 and persisted_ranges.len == 0))) {
                    const backfill_started_ns = nowNs();
                    try rebuild_state.update(if (resume_from) |buf| buf else "");
                    try self.backfillTextIndex(store, &entry, resume_from);
                    try self.saveBackfilledAppliedSequence(store, cfg);
                    backfill_ns += elapsedSince(backfill_started_ns);
                }

                entry.compaction_pending = try self.textIndexNeedsMerge(&entry.persistent, default_merge_policy);
                if (openProfileEnabled()) {
                    logOpenIndexProfile(.{
                        .kind = cfg.kind,
                        .name = cfg.name,
                        .open_ns = elapsedSince(started_ns) -| backfill_ns,
                        .backfill_ns = backfill_ns,
                    });
                }
                return .{ .full_text = entry };
            },
            .dense_vector => {
                const started_ns = nowNs();
                var backfill_ns: u64 = 0;
                const dense_cfg = try parseDenseConfig(self.alloc, cfg.config_json);
                defer dense_cfg.deinit(self.alloc);
                const dense_generator = try parseDenseGeneratorConfig(self.alloc, cfg.config_json);
                defer if (dense_generator) |generator| generator.deinit(self.alloc);
                const referenced_embedding = if (dense_generator == null and dense_cfg.embedding_name != null and !dense_cfg.external)
                    self.getEnrichment(.embedding, dense_cfg.embedding_name.?)
                else
                    null;
                if (dense_generator == null and dense_cfg.embedding_name != null and !dense_cfg.external and referenced_embedding == null) {
                    return error.InvalidIndexConfig;
                }
                if (referenced_embedding) |embedding_cfg| {
                    if (embedding_cfg.expected_dims > 0 and embedding_cfg.expected_dims != dense_cfg.dims) {
                        return error.InvalidIndexConfig;
                    }
                }

                const path = try self.indexPath(cfg.name);
                defer self.alloc.free(path);

                const zpath = try self.alloc.dupeZ(u8, path);
                defer self.alloc.free(zpath);

                var index = try hbc_mod.HBCIndex.openWithLsmOptions(self.alloc, zpath, .{
                    .storage_backend = self.dense_storage_backend,
                    .dims = dense_cfg.dims,
                    .metric = dense_cfg.metric,
                    .split_algo = dense_cfg.split_algo,
                    .search_width = dense_cfg.search_width,
                    .epsilon = dense_cfg.epsilon,
                    .branching_factor = dense_cfg.branching_factor,
                    .leaf_size = dense_cfg.leaf_size,
                    .bulk_build_algo = dense_cfg.bulk_build_algo,
                    .kmeans_backend = dense_cfg.kmeans_backend,
                    .kmeans_update_strategy = dense_cfg.kmeans_update_strategy,
                    .use_quantization = dense_cfg.use_quantization,
                    .rerank_policy = dense_cfg.rerank_policy,
                    .quantizer_seed = dense_cfg.quantizer_seed,
                    .use_random_ortho_trans = dense_cfg.use_random_ortho_trans,
                    .max_cached_nodes = dense_cfg.max_cached_nodes,
                    .max_cached_vectors = dense_cfg.max_cached_vectors,
                    .max_cached_metadata = dense_cfg.max_cached_metadata,
                    .lazy_posting_maintenance = dense_cfg.lazy_posting_maintenance,
                    .auto_posting_maintenance_max_postings = dense_cfg.auto_posting_maintenance_max_postings,
                    .centroid_directory_mode = dense_cfg.centroid_directory_mode,
                    .flat_centroid_block_size = dense_cfg.flat_centroid_block_size,
                    .flat_centroid_probe_count = dense_cfg.flat_centroid_probe_count,
                    .no_sync = self.relaxed_split_durability,
                    .no_meta_sync = self.relaxed_split_durability,
                }, .{
                    .backend_options = self.dense_lsm_options,
                    .storage = self.dense_lsm_storage,
                    .cache = self.lsm_cache,
                    .root_generation = self.lsm_root_generation,
                });
                if (self.hbc_cache) |cache| index.attachSharedCache(cache);
                if (self.resource_manager) |manager| index.attachResourceManager(manager);
                var index_moved = false;
                errdefer if (!index_moved) index.close();
                const vector_loader_context = try self.alloc.create(DenseVectorLoadContext);
                var vector_loader_context_initialized = false;
                errdefer if (!index_moved) {
                    if (vector_loader_context_initialized) {
                        vector_loader_context.deinit(self.alloc);
                    } else {
                        self.alloc.destroy(vector_loader_context);
                    }
                };
                vector_loader_context.* = .{
                    .manager = self,
                    .index_name = try self.alloc.dupe(u8, cfg.name),
                    .max_cached_vectors = dense_cfg.max_cached_vectors,
                };
                vector_loader_context_initialized = true;
                index.setExternalVectorLoader(vector_loader_context, loadDenseVectorForHbc);
                index.setExternalVectorScratchLoader(vector_loader_context, loadDenseVectorForHbcIntoScratch);
                index.setExternalVectorBatchScratchLoader(vector_loader_context, loadDenseVectorsForHbcBatch);
                index.setExternalVectorBatchTransformedMatrixLoader(vector_loader_context, loadDenseVectorsForHbcBatchIntoTransformedMatrix);
                index.setExternalVectorBatchDistanceLoader(vector_loader_context, scoreDenseVectorsForHbcBatch);
                const apply_mutex = try self.allocIndexApplyMutex();
                var apply_mutex_owned = true;
                errdefer if (apply_mutex_owned) self.destroyIndexApplyMutex(apply_mutex);

                var entry = DenseIndex{
                    .apply_mutex = apply_mutex,
                    .config = try types.IndexConfig.clone(self.alloc, cfg),
                    .field_name = try self.alloc.dupe(u8, dense_cfg.field_name),
                    .dims = dense_cfg.dims,
                    .metric = dense_cfg.metric,
                    .external = dense_cfg.external,
                    .chunk_name = if (dense_generator) |generator|
                        if (generatorHasChunking(generator)) try self.alloc.dupe(u8, generator.artifact_name) else null
                    else if (referenced_embedding) |embedding_cfg|
                        if (embedding_cfg.source_artifact_name.len > 0) try self.alloc.dupe(u8, embedding_cfg.source_artifact_name) else null
                    else
                        null,
                    .embedding_name = if (dense_generator) |generator|
                        if (generator.embedding_name) |embedding_name| try self.alloc.dupe(u8, embedding_name) else try self.alloc.dupe(u8, cfg.name)
                    else if (dense_cfg.embedding_name) |embedding_name|
                        try self.alloc.dupe(u8, embedding_name)
                    else
                        null,
                    .index = index,
                    .vector_loader_context = vector_loader_context,
                };
                apply_mutex_owned = false;
                index_moved = true;
                errdefer self.freeDenseIndexEntry(&entry);

                if (allow_backfill and entry.index.metadata.active_count == 0) {
                    const backfill_started_ns = nowNs();
                    try self.backfillDenseIndex(store, &entry);
                    backfill_ns += elapsedSince(backfill_started_ns);
                }

                if (openProfileEnabled()) {
                    logOpenIndexProfile(.{
                        .kind = cfg.kind,
                        .name = cfg.name,
                        .open_ns = elapsedSince(started_ns) -| backfill_ns,
                        .backfill_ns = backfill_ns,
                    });
                }
                return .{ .dense_vector = entry };
            },
            .sparse_vector => {
                const started_ns = nowNs();
                var backfill_ns: u64 = 0;
                const sparse_cfg = try parseSparseConfig(self.alloc, cfg.config_json);
                defer sparse_cfg.deinit(self.alloc);

                const path = try self.indexPath(cfg.name);
                defer self.alloc.free(path);

                const zpath = try self.alloc.dupeZ(u8, path);
                defer self.alloc.free(zpath);

                var index = try sparse_mod.SparseIndex.open(self.alloc, zpath, .{
                    .no_sync = self.relaxed_split_durability,
                    .no_meta_sync = self.relaxed_split_durability,
                    .backend = self.sparse_backend,
                    .lsm_storage = self.sparse_lsm_storage,
                    .lsm_cache = self.lsm_cache,
                    .lsm_options = self.sparse_lsm_options,
                    .lsm_root_generation = self.lsm_root_generation,
                });
                var index_moved = false;
                errdefer if (!index_moved) index.close();
                const apply_mutex = try self.allocIndexApplyMutex();
                var apply_mutex_owned = true;
                errdefer if (apply_mutex_owned) self.destroyIndexApplyMutex(apply_mutex);

                var entry = SparseIndex{
                    .apply_mutex = apply_mutex,
                    .config = try types.IndexConfig.clone(self.alloc, cfg),
                    .field_name = try self.alloc.dupe(u8, sparse_cfg.field_name),
                    .chunk_name = if (try parseSparseGeneratorConfig(self.alloc, cfg.config_json)) |generator| blk: {
                        defer generator.deinit(self.alloc);
                        const chunk_cfg = resolveChunkGenerator(self, generator);
                        break :blk if (generatorHasChunking(chunk_cfg)) try self.alloc.dupe(u8, chunk_cfg.artifact_name) else null;
                    } else null,
                    .embedding_name = if (try parseSparseGeneratorConfig(self.alloc, cfg.config_json)) |generator| blk: {
                        defer generator.deinit(self.alloc);
                        break :blk if (generator.embedding_name) |embedding_name|
                            try self.alloc.dupe(u8, embedding_name)
                        else
                            try self.alloc.dupe(u8, cfg.name);
                    } else try self.alloc.dupe(u8, cfg.name),
                    .rebuild_root_path = try self.alloc.dupe(u8, path),
                    .index = index,
                };
                apply_mutex_owned = false;
                index_moved = true;
                errdefer self.freeSparseIndexEntry(&entry);

                const rebuild_state = backfill_state_mod.RebuildState.init(entry.rebuild_root_path);
                const resume_from = try rebuild_state.check(self.alloc);
                defer if (resume_from) |buf| self.alloc.free(buf);

                if (allow_backfill and (resume_from != null or entry.index.next_doc_num == 0)) {
                    const backfill_started_ns = nowNs();
                    try rebuild_state.update(if (resume_from) |buf| buf else "");
                    try self.backfillSparseIndex(store, &entry, resume_from);
                    try self.saveBackfilledAppliedSequence(store, cfg);
                    backfill_ns += elapsedSince(backfill_started_ns);
                }

                if (openProfileEnabled()) {
                    logOpenIndexProfile(.{
                        .kind = cfg.kind,
                        .name = cfg.name,
                        .open_ns = elapsedSince(started_ns) -| backfill_ns,
                        .backfill_ns = backfill_ns,
                    });
                }
                return .{ .sparse_vector = entry };
            },
            .graph => {
                const started_ns = nowNs();
                var backfill_ns: u64 = 0;
                var graph_cfg = try parseGraphConfig(self.alloc, cfg.config_json);
                var graph_cfg_moved = false;
                errdefer if (!graph_cfg_moved) graph_cfg.deinit(self.alloc);
                if (graph_cfg.artifact_source) |source| {
                    if (self.getEnrichment(.asset, source.artifact_name) == null) return error.InvalidIndexConfig;
                }
                if (graph_cfg.shorthand_asset) |*asset| {
                    asset.deinit(self.alloc);
                    graph_cfg.shorthand_asset = null;
                }

                const path = try self.indexPath(cfg.name);
                defer self.alloc.free(path);

                const forward_path = try std.fmt.allocPrint(self.alloc, "{s}/forward", .{path});
                defer self.alloc.free(forward_path);
                const reverse_path = try std.fmt.allocPrint(self.alloc, "{s}/reverse", .{path});
                defer self.alloc.free(reverse_path);
                const reverse_store_missing = if (comptime builtin.os.tag == .freestanding) true else blk: {
                    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
                    defer io_impl.deinit();
                    var reverse_dir = std.Io.Dir.cwd().openDir(io_impl.io(), reverse_path, .{}) catch |err| switch (err) {
                        error.FileNotFound => break :blk true,
                        else => return err,
                    };
                    reverse_dir.close(io_impl.io());
                    break :blk false;
                };
                const zforward = try self.alloc.dupeZ(u8, forward_path);
                defer self.alloc.free(zforward);
                const zreverse = try self.alloc.dupeZ(u8, reverse_path);
                defer self.alloc.free(zreverse);

                var cloned_cfg = try types.IndexConfig.clone(self.alloc, cfg);
                var cloned_cfg_moved = false;
                errdefer if (!cloned_cfg_moved) cloned_cfg.deinit(self.alloc);

                var index = try graph_mod.GraphIndex.openWithPrivateStores(self.alloc, zforward, zreverse, cloned_cfg.name, .{
                    .no_sync = self.relaxed_split_durability,
                    .no_meta_sync = self.relaxed_split_durability,
                    .reverse_backend = self.graph_reverse_backend,
                    .reverse_lsm_storage = self.graph_lsm_storage,
                    .reverse_lsm_cache = self.lsm_cache,
                    .reverse_lsm_options = self.graph_reverse_lsm_options,
                    .reverse_lsm_root_generation = self.lsm_root_generation,
                    .edge_type_configs = graph_cfg.edge_type_configs,
                    .rebuild_root_path = path,
                    .algebraic_semiring_traversal = graph_cfg.algebraic_semiring_traversal,
                });
                var index_moved = false;
                errdefer if (!index_moved) index.close();
                const apply_mutex = try self.allocIndexApplyMutex();
                var apply_mutex_owned = true;
                errdefer if (apply_mutex_owned) self.destroyIndexApplyMutex(apply_mutex);

                var entry = GraphIndex{
                    .apply_mutex = apply_mutex,
                    .config = cloned_cfg,
                    .edge_type_configs = graph_cfg.edge_type_configs,
                    .artifact_source = graph_cfg.artifact_source,
                    .rebuild_root_path = try self.alloc.dupe(u8, path),
                    .index = index,
                };
                apply_mutex_owned = false;
                cloned_cfg_moved = true;
                index_moved = true;
                graph_cfg_moved = true;
                errdefer self.freeGraphIndexEntry(&entry);

                const rebuild_state = backfill_state_mod.RebuildState.init(entry.rebuild_root_path);
                const resume_from = try rebuild_state.check(self.alloc);
                defer if (resume_from) |buf| self.alloc.free(buf);
                const reverse_edges = (try entry.index.stats(self.alloc)).edge_count;

                const applied_sequence = try apply_state.loadAppliedSequenceWithCheckpoint(
                    self.alloc,
                    store,
                    self.applied_sequence_checkpoint_path,
                    cfg.name,
                );
                const latest_replay_sequence = store.nextReplaySequence(1) -| 1;
                const graph_replay_pending = applied_sequence < latest_replay_sequence;
                if (allow_backfill and (resume_from != null or ((reverse_store_missing or reverse_edges == 0) and !graph_replay_pending))) {
                    const backfill_started_ns = nowNs();
                    try rebuild_state.update(if (resume_from) |buf| buf else "");
                    _ = try entry.index.rebuildReverseFromOwnedOutgoingEdgesResume(self.alloc, self.byte_range.start, self.byte_range.end, resume_from);
                    backfill_ns += elapsedSince(backfill_started_ns);
                }

                if (openProfileEnabled()) {
                    logOpenIndexProfile(.{
                        .kind = cfg.kind,
                        .name = cfg.name,
                        .open_ns = elapsedSince(started_ns) -| backfill_ns,
                        .backfill_ns = backfill_ns,
                    });
                }
                return .{ .graph = entry };
            },
            .algebraic => {
                var index = try algebraic_mod.index.Index.open(self.alloc, cfg.name, cfg.config_json);
                var index_moved = false;
                errdefer if (!index_moved) {
                    var doomed = index;
                    doomed.close();
                };
                if (self.resource_manager) |manager| index.attachResourceManager(manager);
                if (self.hll_maintenance_lane) |lane| {
                    index.attachHllMaintenanceLane(lane, self.hll_maintenance_owner_id);
                    if (comptime @TypeOf(store) == *docstore_mod.DocStore) {
                        index.loadAdaptiveHllCardinalities(store) catch {};
                    }
                }
                const apply_mutex = try self.allocIndexApplyMutex();
                var apply_mutex_owned = true;
                errdefer if (apply_mutex_owned) self.destroyIndexApplyMutex(apply_mutex);

                var entry = AlgebraicIndex{
                    .apply_mutex = apply_mutex,
                    .config = try types.IndexConfig.clone(self.alloc, cfg),
                    .index = index,
                };
                apply_mutex_owned = false;
                index_moved = true;
                errdefer self.freeAlgebraicIndexEntry(&entry);
                return .{ .algebraic = entry };
            },
        }
    }

    fn persistCatalog(self: *IndexManager, store: anytype) !void {
        const data = try serializeCatalog(self.alloc, self);
        defer self.alloc.free(data);
        var runtime_store = try initRuntimeStore(self.alloc, store);
        defer runtime_store.deinit();
        var txn = try runtime_store.store.beginWrite();
        errdefer txn.abort();
        try txn.put(index_catalog_key, data);
        try txn.commit();
    }

    fn loadEnrichmentCatalog(self: *IndexManager, store: anytype) !void {
        var runtime_store = try initRuntimeStore(self.alloc, store);
        defer runtime_store.deinit();
        var txn = try runtime_store.store.beginRead();
        defer txn.abort();
        const data = txn.get(enrichment_catalog_key) catch |err| switch (err) {
            error.NotFound => return,
            else => return err,
        };

        const configs = try enrichment_catalog.deserializeCatalog(self.alloc, data);
        defer {
            for (configs) |*cfg| cfg.deinit(self.alloc);
            self.alloc.free(configs);
        }
        for (configs) |cfg| {
            try self.enrichments.append(self.alloc, try enrichment_catalog.EnrichmentConfig.clone(self.alloc, cfg));
        }
    }

    fn persistEnrichmentCatalog(self: *IndexManager, store: anytype) !void {
        const data = try enrichment_catalog.serializeCatalog(self.alloc, self.enrichments.items);
        defer self.alloc.free(data);
        var runtime_store = try initRuntimeStore(self.alloc, store);
        defer runtime_store.deinit();
        var txn = try runtime_store.store.beginWrite();
        errdefer txn.abort();
        try txn.put(enrichment_catalog_key, data);
        try txn.commit();
    }

    fn ensureShorthandEnrichments(self: *IndexManager, cfg: types.IndexConfig) !bool {
        var changed = false;
        switch (cfg.kind) {
            .dense_vector => {
                const dense_cfg = try parseDenseConfig(self.alloc, cfg.config_json);
                defer dense_cfg.deinit(self.alloc);
                if (try parseDenseGeneratorConfig(self.alloc, cfg.config_json)) |generator| {
                    defer generator.deinit(self.alloc);
                    const chunk_cfg = resolveChunkGenerator(self, generator);
                    if (generatorHasChunking(chunk_cfg)) {
                        changed = (try self.ensureChunkEnrichment(.{
                            .name = chunk_cfg.artifact_name,
                            .kind = .chunk,
                            .source_field = chunk_cfg.source_field,
                            .source_template = if (chunk_cfg.source_template.len > 0) chunk_cfg.source_template else "",
                            .chunk_size = chunk_cfg.chunk_size,
                            .chunk_overlap = chunk_cfg.chunk_overlap,
                            .chunker_json = if (chunk_cfg.chunker_json.len > 0) chunk_cfg.chunker_json else "",
                        })) or changed;
                    }
                    changed = (try self.ensureEmbeddingEnrichment(.{
                        .name = if (chunk_cfg.embedding_name) |embedding_name| embedding_name else cfg.name,
                        .kind = .embedding,
                        .source_field = chunk_cfg.source_field,
                        .source_template = if (chunk_cfg.source_template.len > 0) chunk_cfg.source_template else "",
                        .source_artifact_name = if (generatorHasChunking(chunk_cfg)) chunk_cfg.artifact_name else "",
                        .expected_dims = dense_cfg.dims,
                        .chunk_size = chunk_cfg.chunk_size,
                        .chunk_overlap = chunk_cfg.chunk_overlap,
                    })) or changed;
                }
            },
            .sparse_vector => {
                if (try parseSparseGeneratorConfig(self.alloc, cfg.config_json)) |generator| {
                    defer generator.deinit(self.alloc);
                    const chunk_cfg = resolveChunkGenerator(self, generator);
                    if (generatorHasChunking(chunk_cfg)) {
                        changed = (try self.ensureChunkEnrichment(.{
                            .name = chunk_cfg.artifact_name,
                            .kind = .chunk,
                            .source_field = chunk_cfg.source_field,
                            .source_template = if (chunk_cfg.source_template.len > 0) chunk_cfg.source_template else "",
                            .chunk_size = chunk_cfg.chunk_size,
                            .chunk_overlap = chunk_cfg.chunk_overlap,
                            .chunker_json = if (chunk_cfg.chunker_json.len > 0) chunk_cfg.chunker_json else "",
                        })) or changed;
                    }
                }
            },
            .graph => {
                var graph_cfg = try parseGraphConfig(self.alloc, cfg.config_json);
                defer graph_cfg.deinit(self.alloc);
                if (graph_cfg.shorthand_asset) |asset| {
                    changed = (try self.ensureAssetEnrichment(asset)) or changed;
                }
                if (graph_cfg.artifact_source) |source| {
                    if (graph_cfg.shorthand_asset) |asset| {
                        if (!std.mem.eql(u8, asset.name, source.artifact_name)) return error.InvalidIndexConfig;
                    }
                    if (self.getEnrichment(.asset, source.artifact_name) == null) return error.InvalidIndexConfig;
                }
            },
            else => {},
        }
        return changed;
    }

    fn ensureChunkEnrichment(self: *IndexManager, cfg: enrichment_catalog.EnrichmentConfig) !bool {
        if (self.getEnrichment(.chunk, cfg.name)) |existing| {
            if (!std.mem.eql(u8, existing.source_field, cfg.source_field) or
                !std.mem.eql(u8, existing.source_template, cfg.source_template) or
                existing.chunk_size != cfg.chunk_size or
                existing.chunk_overlap != cfg.chunk_overlap or
                !std.mem.eql(u8, existing.chunker_json, cfg.chunker_json))
            {
                return error.ConflictingEnrichmentConfig;
            }
            return false;
        }

        try self.enrichments.append(self.alloc, try enrichment_catalog.EnrichmentConfig.clone(self.alloc, cfg));
        return true;
    }

    fn ensureEmbeddingEnrichment(self: *IndexManager, cfg: enrichment_catalog.EnrichmentConfig) !bool {
        if (self.getEnrichment(.embedding, cfg.name)) |existing| {
            if (!std.mem.eql(u8, existing.source_field, cfg.source_field) or
                !std.mem.eql(u8, existing.source_template, cfg.source_template) or
                !std.mem.eql(u8, existing.source_artifact_name, cfg.source_artifact_name) or
                existing.expected_dims != cfg.expected_dims)
            {
                return error.ConflictingEnrichmentConfig;
            }
            return false;
        }

        try self.enrichments.append(self.alloc, try enrichment_catalog.EnrichmentConfig.clone(self.alloc, cfg));
        return true;
    }

    fn ensureAssetEnrichment(self: *IndexManager, cfg: enrichment_catalog.EnrichmentConfig) !bool {
        if (self.getEnrichment(.asset, cfg.name)) |existing| {
            if (!std.mem.eql(u8, existing.source_field, cfg.source_field) or
                !std.mem.eql(u8, existing.source_template, cfg.source_template) or
                !std.mem.eql(u8, existing.content_type, cfg.content_type) or
                !std.mem.eql(u8, existing.producer_json, cfg.producer_json))
            {
                return error.ConflictingEnrichmentConfig;
            }
            return false;
        }

        try self.enrichments.append(self.alloc, try enrichment_catalog.EnrichmentConfig.clone(self.alloc, cfg));
        return true;
    }

    fn validateEnrichmentConfig(self: *const IndexManager, cfg: enrichment_catalog.EnrichmentConfig) !void {
        if (cfg.name.len == 0 or (cfg.source_field.len == 0 and cfg.source_template.len == 0)) return error.InvalidEnrichmentConfig;
        switch (cfg.kind) {
            .chunk => {
                if (cfg.chunk_size == 0 and cfg.chunker_json.len == 0) return error.InvalidEnrichmentConfig;
            },
            .embedding => {
                if (cfg.expected_dims == 0) return error.InvalidEnrichmentConfig;
                if (cfg.source_artifact_name.len > 0 and self.getEnrichment(.chunk, cfg.source_artifact_name) == null) {
                    return error.InvalidEnrichmentConfig;
                }
            },
            .asset => {},
        }
    }

    fn enrichmentInUse(self: *const IndexManager, kind: enrichment_catalog.EnrichmentType, name: []const u8) bool {
        switch (kind) {
            .chunk => {
                for (self.text_indexes.items) |entry| {
                    if (entry.chunk_name) |chunk_name| {
                        if (std.mem.eql(u8, chunk_name, name)) return true;
                    }
                }
                for (self.dense_indexes.items) |entry| {
                    if (entry.chunk_name) |chunk_name| {
                        if (std.mem.eql(u8, chunk_name, name)) return true;
                    }
                }
                for (self.sparse_indexes.items) |entry| {
                    const maybe_generator = parseSparseGeneratorConfig(self.alloc, entry.config.config_json) catch continue;
                    if (maybe_generator) |generator| {
                        defer generator.deinit(self.alloc);
                        const chunk_cfg = resolveChunkGenerator(self, generator);
                        if (generatorHasChunking(chunk_cfg) and std.mem.eql(u8, chunk_cfg.artifact_name, name)) return true;
                    }
                }
                for (self.enrichments.items) |entry| {
                    if (entry.kind == .embedding and std.mem.eql(u8, entry.source_artifact_name, name)) return true;
                }
            },
            .embedding => {
                for (self.dense_indexes.items) |entry| {
                    if (entry.embedding_name) |embedding_name| {
                        if (std.mem.eql(u8, embedding_name, name)) return true;
                    }
                }
            },
            .asset => {
                for (self.graph_indexes.items) |entry| {
                    if (entry.artifact_source) |source| {
                        if (std.mem.eql(u8, source.artifact_name, name)) return true;
                    }
                }
            },
        }
        return false;
    }

    fn compactTextIndex(self: *IndexManager, index: *persistent_mod.PersistentIndex, policy: merger_mod.MergePolicy) !void {
        while (true) {
            const snap = index.snapshot();
            const planned = (try text_index_maintenance.planPolicyMergeAlloc(self.alloc, snap, policy)) orelse return;
            defer self.alloc.free(planned);
            var reservation = try self.reserveTextMergeBuffers(snap, planned);
            defer if (reservation) |*active| active.release();

            try text_index_maintenance.applyPlannedMerge(
                self.alloc,
                index,
                snap,
                planned,
                policy.max_segment_size,
                "compact text index merge failed",
                "compact text index apply merge failed",
            );
        }
    }

    pub fn beginTextMergeTask(self: *IndexManager) !?TextMergeTask {
        const now_ns = platform_time.monotonicNs();
        self.text_merge_scheduler.pruneExpiredQuarantines(self.alloc, now_ns);
        _ = self.textMergeStats();
        if (self.shouldDeferTextMergeForResourcePressure()) {
            self.text_merge_scheduler.deferred_for_pressure += 1;
            return null;
        }
        var attempts: usize = 0;
        while (attempts < self.text_indexes.items.len) : (attempts += 1) {
            const idx = self.text_merge_scheduler.select(self.text_indexes.items) orelse return null;
            const entry = &self.text_indexes.items[idx];
            const has_in_flight = self.text_merge_scheduler.indexHasInFlight(entry.config.name);
            const maybe_task = self.beginTextMergeTaskForEntry(entry) catch |err| switch (err) {
                error.ResourceBudgetExceeded => {
                    self.text_merge_scheduler.deferred_for_pressure += 1;
                    return null;
                },
                else => return err,
            };
            if (maybe_task) |task| return task;
            if (!has_in_flight and !self.text_merge_scheduler.indexHasActiveQuarantine(entry.config.name, now_ns)) TextMergeScheduler.noteComplete(entry);
        }
        return null;
    }

    pub fn executeTextMergeTask(alloc: Allocator, task: *const TextMergeTask) !TextMergeResult {
        var synthetic = index_mod.IndexSnapshot{
            .alloc = alloc,
            .ref_count = 1,
            .epoch = 0,
            .segments = task.segments,
            .global_doc_count = 0,
            .global_total_field_len = .empty,
            .term_doc_freq_cache_mu = .unlocked,
            .term_doc_freq_cache = .empty,
            .term_doc_freq_cache_hits = 0,
            .term_doc_freq_cache_misses = 0,
            .retired_segments = &.{},
        };

        const merged = merger_mod.mergeSegmentsBounded(alloc, &synthetic, task.merge_indices, .{
            .target_segment_bytes = @intCast(default_merge_policy.max_segment_size),
        }) catch |err| {
            if (builtin.os.tag != .freestanding) {
                std.log.err("scheduled text merge failed index={s}: {s}", .{ task.index_name, @errorName(err) });
            }
            return err;
        };
        return .{ .segments = merged };
    }

    pub fn finishTextMergeTask(self: *IndexManager, task: *const TextMergeTask, result: *TextMergeResult) !bool {
        defer self.text_merge_scheduler.completeSource(self.alloc, task.index_name, task.source);

        const entry = self.textIndexEntry(task.index_name) orelse return false;
        if (!try self.textMergeSourceStillCurrent(entry, task)) {
            self.text_merge_scheduler.skipped_stale_merges += 1;
            TextMergeScheduler.schedule(entry);
            return false;
        }

        const old_ids = try textMergeSourceIds(self.alloc, task.source);
        defer self.alloc.free(old_ids);
        const applied = entry.persistent.replaceSegmentsIfActiveManyOwned(old_ids, result.segments) catch |err| switch (err) {
            error.EmptySegment => try entry.persistent.removeSegmentsIfActive(old_ids),
            else => {
                if (builtin.os.tag != .freestanding) {
                    std.log.err("scheduled text merge apply failed index={s}: {s}", .{ task.index_name, @errorName(err) });
                }
                return err;
            },
        };
        result.segments = &.{};

        if (applied and !try self.textIndexNeedsMerge(&entry.persistent, default_merge_policy)) {
            TextMergeScheduler.noteComplete(entry);
        } else {
            TextMergeScheduler.schedule(entry);
        }
        if (applied) self.text_merge_scheduler.completed_merges += 1;
        return applied;
    }

    pub fn cancelTextMergeTask(self: *IndexManager, task: *const TextMergeTask) void {
        self.text_merge_scheduler.completeSource(self.alloc, task.index_name, task.source);
        if (self.textIndexEntry(task.index_name)) |entry| TextMergeScheduler.schedule(entry);
    }

    pub fn noteTextMergeFailure(self: *IndexManager, task: *const TextMergeTask, err: anyerror) void {
        self.text_merge_scheduler.failed_merges += 1;
        const now_ns = platform_time.monotonicNs();
        self.text_merge_scheduler.completeSource(self.alloc, task.index_name, task.source);
        self.text_merge_scheduler.quarantineSource(self.alloc, task.index_name, task.source, err, now_ns) catch |quarantine_err| {
            if (builtin.os.tag != .freestanding) {
                std.log.err("text merge quarantine failed index={s}: {s}", .{ task.index_name, @errorName(quarantine_err) });
            }
        };
        if (self.textIndexEntry(task.index_name)) |entry| TextMergeScheduler.schedule(entry);
    }

    fn beginTextMergeTaskForEntry(self: *IndexManager, entry: *TextIndex) !?TextMergeTask {
        const snap = entry.persistent.acquireSnapshot();
        defer snap.release();
        if (snap.segments.len < 2) return null;
        const now_ns = platform_time.monotonicNs();

        var infos = std.ArrayListUnmanaged(merger_mod.SegmentInfo).empty;
        defer infos.deinit(self.alloc);
        for (snap.segments, 0..) |seg, i| {
            if (self.text_merge_scheduler.segmentInFlight(entry.config.name, seg.id)) continue;
            if (self.text_merge_scheduler.segmentQuarantined(entry.config.name, seg.id, now_ns)) continue;
            try infos.append(self.alloc, .{
                .index = i,
                .size = seg.data.bytes().len,
                .doc_count = seg.reader.doc_count,
                .deleted_count = if (seg.deleted) |deleted| @intCast(deleted.cardinality()) else 0,
                .has_deletions = seg.deleted != null,
            });
        }
        if (infos.items.len < 2) return null;

        const planned = (try default_merge_policy.plan(self.alloc, infos.items)) orelse return null;
        defer self.alloc.free(planned);
        if (planned.len < 2) return null;

        var task = try self.copyTextMergeTask(entry.config.name, snap, planned);
        errdefer task.deinit(self.alloc);
        try self.text_merge_scheduler.registerSource(self.alloc, task.index_name, task.source);
        return task;
    }

    fn copyTextMergeTask(self: *IndexManager, index_name: []const u8, snap: *const index_mod.IndexSnapshot, planned: []const usize) !TextMergeTask {
        const owned_index_name = try self.alloc.dupe(u8, index_name);
        errdefer self.alloc.free(owned_index_name);

        var buffer_reservation = try self.reserveTextMergeBuffers(snap, planned);
        errdefer if (buffer_reservation) |*reservation| reservation.release();

        const source = try self.alloc.alloc(TextMergeSourceSegment, planned.len);
        var source_initialized: usize = 0;
        errdefer {
            for (source[0..source_initialized]) |*item| item.deinit(self.alloc);
            self.alloc.free(source);
        }

        const merge_indices = try self.alloc.alloc(usize, planned.len);
        errdefer self.alloc.free(merge_indices);

        const segments = try self.alloc.alloc(index_mod.SegmentEntry, planned.len);
        var segments_initialized: usize = 0;
        errdefer {
            for (segments[0..segments_initialized]) |*seg| {
                seg.reader.deinit();
                if (seg.deleted) |*deleted| deleted.deinit();
                seg.data.deinit(self.alloc);
            }
            self.alloc.free(segments);
        }

        for (planned, 0..) |seg_idx, i| {
            const source_seg = &snap.segments[seg_idx];
            const data_copy = try self.alloc.dupe(u8, source_seg.data.bytes());
            errdefer self.alloc.free(data_copy);

            var reader = try segment_mod.SegmentReader.init(self.alloc, data_copy);
            errdefer reader.deinit();

            var deletion_clone = if (source_seg.deleted) |*deleted|
                try deleted.clone(self.alloc)
            else
                null;
            errdefer if (deletion_clone) |*deleted| deleted.deinit();

            source[i] = .{
                .id = source_seg.id,
                .deleted = if (source_seg.deleted) |*deleted| try deleted.clone(self.alloc) else null,
            };
            source_initialized += 1;
            merge_indices[i] = i;
            segments[i] = .{
                .id = source_seg.id,
                .data = index_mod.SegmentData.fromOwnedHeap(data_copy),
                .reader = reader,
                .deleted = deletion_clone,
            };
            segments_initialized += 1;
        }

        return .{
            .index_name = owned_index_name,
            .source = source,
            .merge_indices = merge_indices,
            .segments = segments,
            .buffer_reservation = buffer_reservation,
        };
    }

    fn reserveTextMergeBuffers(self: *IndexManager, snap: *const index_mod.IndexSnapshot, planned: []const usize) !?resource_manager_mod.Reservation {
        const manager = self.resource_manager orelse return null;

        var source_bytes: u64 = 0;
        for (planned) |seg_idx| {
            const seg = &snap.segments[seg_idx];
            source_bytes = std.math.add(u64, source_bytes, @as(u64, @intCast(seg.data.bytes().len))) catch return error.ResourceBudgetExceeded;
        }

        const merged_output_bytes = source_bytes;
        const segment_overhead = std.math.mul(u64, @as(u64, @intCast(planned.len)), 1024) catch return error.ResourceBudgetExceeded;
        const source_and_output = std.math.add(u64, source_bytes, merged_output_bytes) catch return error.ResourceBudgetExceeded;
        const reservation_bytes = std.math.add(u64, source_and_output, segment_overhead) catch return error.ResourceBudgetExceeded;
        return try manager.reserve(.text_merge_buffers, reservation_bytes);
    }

    fn shouldDeferTextMergeForResourcePressure(self: *IndexManager) bool {
        const manager = self.resource_manager orelse return false;
        if (sliceDefersBackgroundWork(manager.sliceStats(.full_text_pending_segments))) return true;
        return sliceDefersBackgroundWork(manager.sliceStats(.text_merge_buffers));
    }

    fn sliceDefersBackgroundWork(stats: resource_manager_mod.SliceStats) bool {
        return switch (stats.pressure) {
            .normal => false,
            .soft => stats.soft_action == .defer_background_work or stats.soft_action == .reject_work,
            .hard => stats.hard_action == .defer_background_work or stats.hard_action == .reject_work,
        };
    }

    fn textMergeSourceStillCurrent(_: *IndexManager, entry: *TextIndex, task: *const TextMergeTask) !bool {
        const snap = entry.persistent.acquireSnapshot();
        defer snap.release();
        for (task.source) |source| {
            const seg = findSegmentById(snap, source.id) orelse return false;
            if (source.deleted) |expected| {
                const current_deleted = seg.deleted orelse return false;
                if (!current_deleted.eql(&expected)) return false;
            } else if (seg.deleted != null) {
                return false;
            }
        }
        return true;
    }

    fn findSegmentById(snap: *const index_mod.IndexSnapshot, id: u64) ?*const index_mod.SegmentEntry {
        for (snap.segments) |*seg| {
            if (seg.id == id) return seg;
        }
        return null;
    }

    fn textMergeSourceIds(alloc: Allocator, source: []const TextMergeSourceSegment) ![]u64 {
        const ids = try alloc.alloc(u64, source.len);
        for (source, 0..) |segment, i| ids[i] = segment.id;
        return ids;
    }

    fn textIndexNeedsMerge(self: *IndexManager, index: *persistent_mod.PersistentIndex, policy: merger_mod.MergePolicy) !bool {
        return try text_index_maintenance.needsMerge(self.alloc, index, policy);
    }

    fn forceCompactTextIndexWithOptions(
        self: *IndexManager,
        entry: *TextIndex,
        options: ForceTextCompactOptions,
    ) !bool {
        while (true) {
            const snap = entry.persistent.snapshot();
            if (snap.segments.len < 2) return true;
            if (options.mode == .best_effort and self.shouldDeferTextMergeForResourcePressure()) return false;

            const planned = try text_index_maintenance.planForceCompactAlloc(self.alloc, snap, force_merge_max_segments_at_once);
            defer self.alloc.free(planned);
            var reservation = self.reserveTextMergeBuffers(snap, planned) catch |err| switch (err) {
                error.ResourceBudgetExceeded => if (options.mode == .best_effort) return false else return err,
            };
            defer if (reservation) |*active| active.release();

            text_index_maintenance.applyPlannedMerge(
                self.alloc,
                &entry.persistent,
                snap,
                planned,
                default_merge_policy.max_segment_size,
                "force compact text index merge failed",
                "force compact text index apply merge failed",
            ) catch |err| switch (err) {
                error.ResourceBudgetExceeded => if (options.mode == .best_effort) return false else return err,
                else => return err,
            };
        }
    }

    fn removeInMemory(self: *IndexManager, name: []const u8) void {
        for (self.text_indexes.items, 0..) |*entry, i| {
            if (std.mem.eql(u8, entry.config.name, name)) {
                self.freeTextIndexEntry(entry);
                _ = self.text_indexes.orderedRemove(i);
                return;
            }
        }
        for (self.dense_indexes.items, 0..) |*entry, i| {
            if (std.mem.eql(u8, entry.config.name, name)) {
                self.freeDenseIndexEntry(entry);
                _ = self.dense_indexes.orderedRemove(i);
                return;
            }
        }
        for (self.sparse_indexes.items, 0..) |*entry, i| {
            if (std.mem.eql(u8, entry.config.name, name)) {
                self.freeSparseIndexEntry(entry);
                _ = self.sparse_indexes.orderedRemove(i);
                return;
            }
        }
        for (self.graph_indexes.items, 0..) |*entry, i| {
            if (std.mem.eql(u8, entry.config.name, name)) {
                self.freeGraphIndexEntry(entry);
                _ = self.graph_indexes.orderedRemove(i);
                return;
            }
        }
    }

    fn truncateEnrichments(self: *IndexManager, len: usize) void {
        while (self.enrichments.items.len > len) {
            self.enrichments.items[self.enrichments.items.len - 1].deinit(self.alloc);
            _ = self.enrichments.pop();
        }
    }

    pub fn lookupDenseDocKey(self: *IndexManager, store: *docstore_mod.DocStore, index_name: []const u8, vector_id: u64) !?[]u8 {
        const entry = self.denseIndex(index_name) orelse return null;
        if (try entry.index.getMetadata(vector_id)) |metadata| return metadata;

        var runtime_store = try initRuntimeStore(self.alloc, store);
        defer runtime_store.deinit();

        var txn = try runtime_store.store.beginRead();
        defer txn.abort();
        return try self.lookupDenseDocKeyByVectorIdTxn(&txn, index_name, vector_id);
    }

    fn backfillDenseIndex(self: *IndexManager, store: *docstore_mod.DocStore, entry: *DenseIndex) !void {
        var runtime_store = try initRuntimeStore(self.alloc, store);
        defer runtime_store.deinit();

        const lower = try internal_keys.documentRangeLowerAlloc(self.alloc, self.byte_range.start);
        defer self.alloc.free(lower);
        const upper = try internal_keys.documentRangeUpperAlloc(self.alloc, if (self.byte_range.end.len > 0) self.byte_range.end else "");
        defer if (upper) |buf| self.alloc.free(buf);

        const docs = try backend_scan.scanRange(self.alloc, &runtime_store.store, lower, if (upper) |buf| buf else "");
        defer backend_scan.freeResults(self.alloc, docs);
        try self.materializeScannedDocumentRows(docs);

        var items = std.ArrayListUnmanaged(hbc_mod.BatchInsertItem).empty;
        defer {
            for (items.items) |item| {
                self.alloc.free(@constCast(item.vector));
                if (item.metadata.len > 0) self.alloc.free(@constCast(item.metadata));
            }
            items.deinit(self.alloc);
        }
        var pending_mappings = std.ArrayListUnmanaged(PendingDenseVectorMapping).empty;
        defer pending_mappings.deinit(self.alloc);
        var mapping_batch = try runtime_store.store.beginBatch();
        errdefer mapping_batch.abort();

        for (docs) |doc| {
            if (!internal_keys.isPrimaryDocumentKey(doc.key)) continue;
            const raw_key = (try internal_keys.decodePrimaryDocumentKeyAlloc(self.alloc, doc.key)) orelse continue;
            defer self.alloc.free(raw_key);
            if (!self.keyInRange(raw_key)) continue;
            const vector_values = (try mapper.extractDenseVectorField(self.alloc, doc.value, entry.field_name, entry.dims)) orelse continue;
            errdefer self.alloc.free(vector_values);

            const assignment = try self.ensureDenseVectorIdTxn(&mapping_batch, entry.config.name, raw_key, null);
            try items.append(self.alloc, .{
                .vector_id = assignment.vector_id,
                .vector = vector_values,
                .metadata = try self.alloc.dupe(u8, raw_key),
            });
            try pending_mappings.append(self.alloc, .{
                .doc_key = items.items[items.items.len - 1].metadata,
                .parent_doc_key = null,
                .vector_id = assignment.vector_id,
            });
        }

        try self.insertDenseItems(entry, items.items);
        try self.commitDenseVectorMappingsWithRollback(&mapping_batch, &mapping_batch, entry, entry.config.name, pending_mappings.items);
    }

    fn backfillSparseIndex(self: *IndexManager, store: *docstore_mod.DocStore, entry: *SparseIndex, resume_from: ?[]const u8) !void {
        const rebuild_state = backfill_state_mod.RebuildState.init(entry.rebuild_root_path);
        var runtime_store = try initRuntimeStore(self.alloc, store);
        defer runtime_store.deinit();

        const lower = try internal_keys.documentRangeLowerAlloc(self.alloc, self.byte_range.start);
        defer self.alloc.free(lower);
        const upper = try internal_keys.documentRangeUpperAlloc(self.alloc, if (self.byte_range.end.len > 0) self.byte_range.end else "");
        defer if (upper) |buf| self.alloc.free(buf);

        const docs = try backend_scan.scanRange(self.alloc, &runtime_store.store, lower, if (upper) |buf| buf else "");
        defer backend_scan.freeResults(self.alloc, docs);
        try self.materializeScannedDocumentRows(docs);

        var writes = std.ArrayListUnmanaged(sparse_mod.SparseWrite).empty;
        defer {
            for (writes.items) |item| {
                self.alloc.free(@constCast(item.doc_id));
                self.alloc.free(@constCast(item.vec.indices));
                self.alloc.free(@constCast(item.vec.values));
            }
            writes.deinit(self.alloc);
        }

        var flushed_batches: usize = 0;
        var backfilled_doc_count: u64 = entry.index.doc_count;
        var saw_visible_doc = false;
        var max_flushed_key: ?[]const u8 = null;

        const flush_batch = struct {
            fn run(
                manager: *IndexManager,
                doc_store: *docstore_mod.DocStore,
                sparse_entry: *SparseIndex,
                rebuild: backfill_state_mod.RebuildState,
                writes_buf: *std.ArrayListUnmanaged(sparse_mod.SparseWrite),
                last_doc_key: []const u8,
                flush_count: *usize,
                doc_count: *u64,
            ) !void {
                if (writes_buf.items.len == 0) return;
                try manager.assignSparseWriteDocNumsFromIdentity(doc_store, sparse_entry, writes_buf.items);
                try sparse_entry.index.batchWithOptions(writes_buf.items, &.{}, .{
                    .defer_term_range_updates = true,
                });
                doc_count.* += writes_buf.items.len;
                try sparse_entry.index.persistBackfillDocCount(doc_count.*);
                for (writes_buf.items) |item| {
                    manager.alloc.free(@constCast(item.doc_id));
                    manager.alloc.free(@constCast(item.vec.indices));
                    manager.alloc.free(@constCast(item.vec.values));
                }
                writes_buf.clearRetainingCapacity();
                try rebuild.update(last_doc_key);
                flush_count.* += 1;
                if (@import("builtin").is_test) {
                    if (test_abort_sparse_backfill_after_batches) |limit| {
                        if (flush_count.* >= limit) return error.TestInjectedBackfillFailure;
                    }
                }
            }
        }.run;

        const backfill_batch_size = if (builtin.is_test) test_sparse_backfill_batch_size orelse sparse_backfill_batch_size else sparse_backfill_batch_size;
        for (docs) |doc| {
            if (!internal_keys.isPrimaryDocumentKey(doc.key)) continue;
            if (resume_from) |resume_key| {
                if (resume_key.len > 0 and std.mem.order(u8, doc.key, resume_key) != .gt) continue;
            }
            const raw_key = (try internal_keys.decodePrimaryDocumentKeyAlloc(self.alloc, doc.key)) orelse continue;
            defer self.alloc.free(raw_key);
            if (!self.keyInRange(raw_key)) continue;
            var sparse_vec = (try mapper.extractSparseVectorField(self.alloc, doc.value, entry.field_name)) orelse continue;
            var sparse_vec_owned = true;
            errdefer if (sparse_vec_owned) sparse_vec.deinit(self.alloc);
            saw_visible_doc = true;
            if (max_flushed_key == null or std.mem.order(u8, doc.key, max_flushed_key.?) == .gt) {
                max_flushed_key = doc.key;
            }
            try writes.append(self.alloc, .{
                .doc_id = try self.alloc.dupe(u8, raw_key),
                .vec = .{
                    .indices = sparse_vec.indices,
                    .values = sparse_vec.values,
                },
            });
            sparse_vec_owned = false;
            if (writes.items.len >= backfill_batch_size) {
                try flush_batch(self, store, entry, rebuild_state, &writes, max_flushed_key.?, &flushed_batches, &backfilled_doc_count);
            }
        }

        if (writes.items.len > 0) {
            try flush_batch(self, store, entry, rebuild_state, &writes, max_flushed_key.?, &flushed_batches, &backfilled_doc_count);
        }

        if (!saw_visible_doc or flushed_batches > 0) try rebuild_state.clear();
    }

    fn denseVectorIdHasExistingMetadata(
        self: *IndexManager,
        entry: *DenseIndex,
        vector_id: u64,
        memo: ?*DenseVectorMetadataPresenceMemo,
    ) !bool {
        if (memo) |cache| {
            if (cache.get(vector_id)) |present| return present;
        }
        const existing_metadata = entry.index.getMetadata(vector_id) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        const present = existing_metadata != null;
        if (memo) |cache| {
            if (existing_metadata) |metadata| {
                defer self.alloc.free(metadata);
                try cache.notePresent(self.alloc, vector_id, metadata);
            } else {
                try cache.noteAbsent(self.alloc, vector_id);
            }
        } else if (existing_metadata) |metadata| {
            self.alloc.free(metadata);
        }
        return present;
    }

    fn denseVectorIdMetadataState(
        self: *IndexManager,
        entry: *DenseIndex,
        vector_id: u64,
        doc_key: []const u8,
        memo: ?*DenseVectorMetadataPresenceMemo,
    ) !DenseVectorMetadataState {
        if (memo) |cache| {
            if (cache.getMetadata(vector_id)) |metadata| {
                return if (std.mem.eql(u8, metadata, doc_key)) .matches else .conflicts;
            }
            if (cache.get(vector_id) == false) return .absent;
        }
        const existing_metadata = entry.index.getMetadata(vector_id) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (existing_metadata) |metadata| {
            defer self.alloc.free(metadata);
            if (memo) |cache| try cache.notePresent(self.alloc, vector_id, metadata);
            return if (std.mem.eql(u8, metadata, doc_key)) .matches else .conflicts;
        }
        if (memo) |cache| try cache.noteAbsent(self.alloc, vector_id);
        return .absent;
    }

    fn legacyOrdinalDenseVectorIdAssignmentTxn(
        self: *IndexManager,
        txn: anytype,
        entry: *DenseIndex,
        doc_key: []const u8,
        parent_doc_key: ?[]const u8,
        metadata_presence_memo: ?*DenseVectorMetadataPresenceMemo,
    ) !?DenseVectorIdAssignment {
        if (parent_doc_key != null) return null;
        const ordinal = (try doc_identity.lookupOrdinalTxn(self.alloc, txn, doc_key)) orelse return null;
        const vector_id: u64 = ordinal;
        // Compatibility only: pre-DOCID dense vectors may have used the document
        // ordinal as the HBC vector ID. New assignments use deterministic IDs.
        return switch (try self.denseVectorIdMetadataState(entry, vector_id, doc_key, metadata_presence_memo)) {
            .matches => .{
                .vector_id = vector_id,
                .needs_mapping = false,
                .can_assume_absent = false,
            },
            .absent, .conflicts => null,
        };
    }

    fn prefetchDenseExistingMetadataTxn(
        self: *IndexManager,
        entry: *DenseIndex,
        identity_txn: anytype,
        index_txn: anytype,
        writes: []const mapper.DenseEmbeddingWrite,
        keep_write: []const bool,
        memo: *DenseVectorMetadataPresenceMemo,
    ) !void {
        var candidate_count: usize = 0;
        for (writes, 0..) |write, write_index| {
            if (!keep_write[write_index]) continue;
            if (!std.mem.eql(u8, write.index_name, entry.config.name)) continue;
            if (write.vector.len == 0 and write.artifact_key == null) continue;
            candidate_count += 1;
        }
        if (candidate_count == 0) return;

        var arena_state = std.heap.ArenaAllocator.init(self.alloc);
        defer arena_state.deinit();
        const arena = arena_state.allocator();

        const max_candidate_ids = candidate_count * 2;
        const vector_ids_storage = try arena.alloc(u64, max_candidate_ids);
        const out_metadata = try arena.alloc(?[]const u8, max_candidate_ids);
        const lookups = try arena.alloc(hbc_mod.FixedKeyLookup, max_candidate_ids);
        const key_views = try arena.alloc([]const u8, max_candidate_ids);
        const values = try arena.alloc(?[]const u8, max_candidate_ids);

        var filled: usize = 0;
        for (writes, 0..) |write, write_index| {
            if (!keep_write[write_index]) continue;
            if (!std.mem.eql(u8, write.index_name, entry.config.name)) continue;
            if (write.vector.len == 0 and write.artifact_key == null) continue;
            vector_ids_storage[filled] = deterministicDenseVectorId(write.doc_key);
            filled += 1;
            if (write.parent_doc_key == null) {
                if (try doc_identity.lookupOrdinalTxn(self.alloc, identity_txn, write.doc_key)) |ordinal| {
                    vector_ids_storage[filled] = ordinal;
                    filled += 1;
                }
            }
        }

        const candidate_vector_ids = vector_ids_storage[0..filled];
        std.mem.sort(u64, candidate_vector_ids, {}, std.sort.asc(u64));
        var unique_count: usize = 0;
        var previous: ?u64 = null;
        for (candidate_vector_ids) |vector_id| {
            if (previous != null and previous.? == vector_id) continue;
            vector_ids_storage[unique_count] = vector_id;
            unique_count += 1;
            previous = vector_id;
        }
        const vector_ids = vector_ids_storage[0..unique_count];

        if (comptime @hasDecl(@TypeOf(index_txn.*), "getManySorted")) {
            try entry.index.getMetadataManySortedInTxnWithScratch(
                index_txn,
                vector_ids,
                out_metadata[0..unique_count],
                lookups,
                key_views,
                values,
            );
        } else {
            for (vector_ids, 0..) |vector_id, i| {
                out_metadata[i] = try entry.index.getMetadataInTxn(index_txn, vector_id);
            }
        }
        for (vector_ids, out_metadata[0..unique_count]) |vector_id, maybe_metadata| {
            if (maybe_metadata) |metadata| {
                try memo.notePresent(self.alloc, vector_id, metadata);
            } else {
                try memo.noteAbsent(self.alloc, vector_id);
            }
        }
    }

    fn ensureDenseVectorIdTxn(self: *IndexManager, txn: anytype, index_name: []const u8, doc_key: []const u8, parent_doc_key: ?[]const u8) !DenseVectorIdAssignment {
        return try self.ensureDenseVectorIdTxnWithMemo(txn, index_name, doc_key, parent_doc_key, null);
    }

    fn ensureDenseVectorIdTxnWithMemo(
        self: *IndexManager,
        txn: anytype,
        index_name: []const u8,
        doc_key: []const u8,
        parent_doc_key: ?[]const u8,
        metadata_presence_memo: ?*DenseVectorMetadataPresenceMemo,
    ) !DenseVectorIdAssignment {
        const mutable_txn = txn;
        if (try self.lookupDenseVectorIdTxn(mutable_txn, index_name, doc_key)) |existing| {
            return .{
                .vector_id = existing,
                .needs_mapping = false,
                .can_assume_absent = false,
            };
        }
        if (self.denseIndex(index_name)) |entry| {
            if (try self.legacyOrdinalDenseVectorIdAssignmentTxn(mutable_txn, entry, doc_key, parent_doc_key, metadata_presence_memo)) |assignment| {
                return assignment;
            }
            const vector_id = deterministicDenseVectorId(doc_key);
            if (try self.denseVectorIdHasExistingMetadata(entry, vector_id, metadata_presence_memo)) {
                return .{
                    .vector_id = vector_id,
                    .needs_mapping = false,
                    .can_assume_absent = false,
                };
            }
            return .{
                .vector_id = vector_id,
                .needs_mapping = false,
                .can_assume_absent = true,
            };
        }
        const vector_id = deterministicDenseVectorId(doc_key);
        return .{
            .vector_id = vector_id,
            .needs_mapping = false,
            .can_assume_absent = true,
        };
    }

    fn replaceDenseVectorIdTxn(
        self: *IndexManager,
        txn: anytype,
        entry: *DenseIndex,
        index_name: []const u8,
        doc_key: []const u8,
        parent_doc_key: ?[]const u8,
        replacement_deletes: *std.ArrayListUnmanaged(u64),
    ) !DenseVectorIdAssignment {
        return try self.replaceDenseVectorIdTxnWithMemo(
            txn,
            entry,
            index_name,
            doc_key,
            parent_doc_key,
            replacement_deletes,
            null,
        );
    }

    fn replaceDenseVectorIdTxnWithMemo(
        self: *IndexManager,
        txn: anytype,
        entry: *DenseIndex,
        index_name: []const u8,
        doc_key: []const u8,
        parent_doc_key: ?[]const u8,
        replacement_deletes: *std.ArrayListUnmanaged(u64),
        metadata_presence_memo: ?*DenseVectorMetadataPresenceMemo,
    ) !DenseVectorIdAssignment {
        _ = replacement_deletes;
        const mutable_txn = txn;
        if (try self.lookupDenseVectorIdTxn(mutable_txn, index_name, doc_key)) |mapped| {
            return .{
                .vector_id = mapped,
                .needs_mapping = false,
                .can_assume_absent = false,
            };
        }

        if (try self.legacyOrdinalDenseVectorIdAssignmentTxn(mutable_txn, entry, doc_key, parent_doc_key, metadata_presence_memo)) |assignment| {
            return assignment;
        }

        const vector_id = deterministicDenseVectorId(doc_key);
        if (try self.denseVectorIdHasExistingMetadata(entry, vector_id, metadata_presence_memo)) {
            return .{
                .vector_id = vector_id,
                .needs_mapping = false,
                .can_assume_absent = false,
            };
        }
        return .{
            .vector_id = vector_id,
            .needs_mapping = false,
            .can_assume_absent = true,
        };
    }

    fn reserveDenseVectorIdTxn(self: *IndexManager, txn: anytype, index_name: []const u8) !u64 {
        const mutable_txn = txn;
        const next_key = try denseNextIdKey(self.alloc, index_name);
        defer self.alloc.free(next_key);
        const legacy_next_key = try legacyDenseNextIdKey(self.alloc, index_name);
        defer self.alloc.free(legacy_next_key);

        var next_id: u64 = 1;
        const next_raw = mutable_txn.get(next_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (next_raw) |raw| {
            if (raw.len != 8) return error.InvalidDenseVectorMetadata;
            next_id = std.mem.readInt(u64, raw[0..8], .little);
        }
        const legacy_next_raw = mutable_txn.get(legacy_next_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (legacy_next_raw) |raw| {
            if (raw.len != 8) return error.InvalidDenseVectorMetadata;
            next_id = @max(next_id, std.mem.readInt(u64, raw[0..8], .little));
        }

        var next_buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &next_buf, next_id + 1, .little);
        try mutable_txn.put(next_key, &next_buf);
        return next_id;
    }

    fn setDenseNextIdAtLeast(self: *IndexManager, store: anytype, index_name: []const u8, next_id: u64) !void {
        var runtime_store = try initRuntimeStore(self.alloc, store);
        defer runtime_store.deinit();

        var txn = try runtime_store.store.beginWrite();
        errdefer txn.abort();
        try self.setDenseNextIdAtLeastTxn(&txn, index_name, next_id);
        try txn.commit();
    }

    fn setDenseNextIdAtLeastTxn(self: *IndexManager, txn: anytype, index_name: []const u8, next_id: u64) !void {
        const mutable_txn = txn;
        const next_key = try denseNextIdKey(self.alloc, index_name);
        defer self.alloc.free(next_key);
        const legacy_next_key = try legacyDenseNextIdKey(self.alloc, index_name);
        defer self.alloc.free(legacy_next_key);

        const next_raw = mutable_txn.get(next_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        var current_next_id: u64 = 1;
        if (next_raw) |raw| {
            if (raw.len != 8) return error.InvalidDenseVectorMetadata;
            current_next_id = std.mem.readInt(u64, raw[0..8], .little);
        }
        const legacy_next_raw = mutable_txn.get(legacy_next_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (legacy_next_raw) |raw| {
            if (raw.len != 8) return error.InvalidDenseVectorMetadata;
            current_next_id = @max(current_next_id, std.mem.readInt(u64, raw[0..8], .little));
        }
        if (current_next_id >= next_id) return;

        var next_buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &next_buf, next_id, .little);
        try mutable_txn.put(next_key, &next_buf);
    }

    pub fn lookupDenseVectorId(self: *IndexManager, store: anytype, index_name: []const u8, doc_key: []const u8) !?u64 {
        var runtime_store = try initRuntimeStore(self.alloc, store);
        defer runtime_store.deinit();

        var txn = try runtime_store.store.beginRead();
        defer txn.abort();
        if (try self.lookupDenseVectorIdTxn(&txn, index_name, doc_key)) |mapped| return mapped;

        const entry = self.denseIndex(index_name) orelse return null;
        if (try doc_identity.lookupOrdinalTxn(self.alloc, &txn, doc_key)) |ordinal| {
            const vector_id: u64 = ordinal;
            if ((try self.denseVectorIdMetadataState(entry, vector_id, doc_key, null)) == .matches) return vector_id;
        }
        const vector_id = deterministicDenseVectorId(doc_key);
        const metadata = (try entry.index.getMetadata(vector_id)) orelse return null;
        self.alloc.free(metadata);
        return vector_id;
    }

    fn lookupDenseVectorIdTxn(self: *IndexManager, txn: anytype, index_name: []const u8, doc_key: []const u8) !?u64 {
        var mutable_txn = txn;
        const key = try denseDocMappingKey(self.alloc, index_name, doc_key);
        defer self.alloc.free(key);
        const legacy_key = try legacyDenseDocMappingKey(self.alloc, index_name, doc_key);
        defer self.alloc.free(legacy_key);

        const raw = mutable_txn.get(key) catch |err| switch (err) {
            error.NotFound => legacy: {
                const legacy_raw = mutable_txn.get(legacy_key) catch |legacy_err| switch (legacy_err) {
                    error.NotFound => return null,
                    else => return legacy_err,
                };
                break :legacy legacy_raw;
            },
            else => return err,
        };
        if (raw.len != 8) return error.InvalidDenseVectorMetadata;
        return std.mem.readInt(u64, raw[0..8], .little);
    }

    fn resolveDenseVectorIdForDeleteTxn(self: *IndexManager, txn: anytype, index_name: []const u8, doc_key: []const u8) !?u64 {
        if (try self.lookupDenseVectorIdTxn(txn, index_name, doc_key)) |mapped| return mapped;
        const entry = self.denseIndex(index_name) orelse return null;
        if (try doc_identity.lookupOrdinalTxn(self.alloc, txn, doc_key)) |ordinal| {
            const vector_id: u64 = ordinal;
            if ((try self.denseVectorIdMetadataState(entry, vector_id, doc_key, null)) == .matches) return vector_id;
        }
        return deterministicDenseVectorId(doc_key);
    }

    fn indexTextBatchForConfig(self: *IndexManager, store: *docstore_mod.DocStore, entry: *TextIndex, writes: []const types.BatchWrite) !TextBatchMutationStats {
        if (writes.len == 0) return .{};

        var filtered = std.ArrayListUnmanaged(types.BatchWrite).empty;
        defer filtered.deinit(self.alloc);

        for (writes) |write| {
            if (!self.keyInRange(write.key)) continue;
            if (!try textIndexShouldConsumeDoc(self, entry, write.key)) continue;
            try filtered.append(self.alloc, write);
        }

        if (filtered.items.len == 0) return .{};

        var doc_ids = try self.alloc.alloc([]const u8, filtered.items.len);
        defer self.alloc.free(doc_ids);
        for (filtered.items, 0..) |write, i| doc_ids[i] = write.key;

        var identity_txn = try store.beginProbeTxn();
        defer identity_txn.abort();
        const ordinals = try doc_identity.lookupOrdinalsTxnAlloc(self.alloc, &identity_txn, doc_ids);
        defer self.alloc.free(ordinals);

        var docs = try self.alloc.alloc(mapper.MapperDoc, filtered.items.len);
        defer self.alloc.free(docs);
        for (filtered.items, 0..) |write, i| {
            docs[i] = .{
                .key = write.key,
                .value = write.value,
                .doc_ordinal = ordinals[i],
            };
        }
        return try self.indexTextProjectionDocsMaybeChunked(store, entry, docs);
    }

    fn indexTextProjectionDocs(
        self: *IndexManager,
        store: *docstore_mod.DocStore,
        entry: *TextIndex,
        docs: []const mapper.MapperDoc,
    ) !TextBatchMutationStats {
        var arena_state = std.heap.ArenaAllocator.init(self.alloc);
        defer arena_state.deinit();
        const arena = arena_state.allocator();
        const source_batch = try mapper.buildTextProjectionSourceBatchWithOptions(
            arena,
            docs,
            try self.textProjectionOptionsForSchema(arena, entry.runtime_schema == null),
        );
        return try self.indexPreparedTextProjectionSourceDocsWithArena(arena, store, entry, source_batch.docs);
    }

    fn indexTextProjectionDocsMaybeChunked(
        self: *IndexManager,
        store: *docstore_mod.DocStore,
        entry: *TextIndex,
        docs: []const mapper.MapperDoc,
    ) !TextBatchMutationStats {
        if (docs.len <= max_text_projection_docs_per_segment_build) {
            return try self.indexTextProjectionDocs(store, entry, docs);
        }

        var stats = TextBatchMutationStats{};
        var start: usize = 0;
        while (start < docs.len) {
            const end = @min(start + max_text_projection_docs_per_segment_build, docs.len);
            const chunk_stats = try self.indexTextProjectionDocs(store, entry, docs[start..end]);
            stats.noteIndex(chunk_stats.indexed_any);
            stats.noteDelete(chunk_stats.deleted_any);
            start = end;
        }
        return stats;
    }

    fn indexTextProjectionSourceDocs(
        self: *IndexManager,
        store: *docstore_mod.DocStore,
        entry: *TextIndex,
        source_docs: []const mapper.TextProjectionSourceDoc,
        opts: IndexBatchOptions,
        skip: ?*const TextSplitHandoff,
    ) !void {
        var arena_state = std.heap.ArenaAllocator.init(self.alloc);
        defer arena_state.deinit();
        const arena = arena_state.allocator();
        const stats = try self.indexFilteredTextProjectionSourceDocsWithArena(arena, store, entry, source_docs, skip);
        try self.finalizeTextBatchMutations(entry, opts, stats);
    }

    fn indexFilteredTextProjectionSourceDocsWithArena(
        self: *IndexManager,
        arena: std.mem.Allocator,
        store: *docstore_mod.DocStore,
        entry: *TextIndex,
        source_docs: []const mapper.TextProjectionSourceDoc,
        skip: ?*const TextSplitHandoff,
    ) !TextBatchMutationStats {
        var filtered = std.ArrayListUnmanaged(mapper.TextProjectionSourceDoc).empty;
        defer filtered.deinit(arena);
        for (source_docs) |doc| {
            if (!self.keyInRange(doc.key)) continue;
            if (!try textIndexShouldConsumeDoc(self, entry, doc.key)) continue;
            if (skip) |handoff| {
                if (handoff.shouldSkip(doc.key)) continue;
            }
            try filtered.append(arena, doc);
        }
        if (filtered.items.len == 0) return .{};

        return try self.indexPreparedTextProjectionSourceDocsMaybeChunked(arena, store, entry, filtered.items);
    }

    fn indexPreparedTextProjectionSourceDocsMaybeChunked(
        self: *IndexManager,
        arena: std.mem.Allocator,
        store: *docstore_mod.DocStore,
        entry: *TextIndex,
        source_docs: []const mapper.TextProjectionSourceDoc,
    ) !TextBatchMutationStats {
        if (source_docs.len <= max_text_projection_docs_per_segment_build) {
            return try self.indexPreparedTextProjectionSourceDocsWithArena(arena, store, entry, source_docs);
        }

        var stats = TextBatchMutationStats{};
        var start: usize = 0;
        while (start < source_docs.len) {
            const end = @min(start + max_text_projection_docs_per_segment_build, source_docs.len);
            var chunk_arena_state = std.heap.ArenaAllocator.init(self.alloc);
            defer chunk_arena_state.deinit();
            const chunk_stats = try self.indexPreparedTextProjectionSourceDocsWithArena(
                chunk_arena_state.allocator(),
                store,
                entry,
                source_docs[start..end],
            );
            stats.noteIndex(chunk_stats.indexed_any);
            stats.noteDelete(chunk_stats.deleted_any);
            start = end;
        }
        return stats;
    }

    fn indexPreparedTextProjectionSourceDocsWithArena(
        self: *IndexManager,
        arena: std.mem.Allocator,
        store: *docstore_mod.DocStore,
        entry: *TextIndex,
        source_docs: []const mapper.TextProjectionSourceDoc,
    ) !TextBatchMutationStats {
        if (source_docs.len == 0) return .{};

        const profile_enabled = benchMetricsEnabled();
        const total_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
        var ordinals_ns: u64 = 0;
        var projection_ns: u64 = 0;
        var analyzer_merge_ns: u64 = 0;
        var segment_build_ns: u64 = 0;
        var index_segment_ns: u64 = 0;

        var observed_field_analyzers = std.ArrayListUnmanaged(mapper.ObservedFieldAnalyzer).empty;
        const ordinals_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
        const source_docs_with_ordinals = try self.textProjectionSourceDocsWithOrdinals(arena, store, source_docs);
        if (profile_enabled) ordinals_ns = platform_time.monotonicNs() - ordinals_start_ns;

        const projection_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
        const projection_batch = try mapper.buildTextProjectionBatchFromSource(arena, source_docs_with_ordinals, entry.text_analysis, entry.runtime_schema, &observed_field_analyzers);
        if (profile_enabled) projection_ns = platform_time.monotonicNs() - projection_start_ns;
        if (projection_batch.observed_field_analyzers.len > 0) {
            const analyzer_merge_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
            try mergeObservedTextFieldAnalyzers(self, store, entry, projection_batch.observed_field_analyzers);
            if (profile_enabled) analyzer_merge_ns = platform_time.monotonicNs() - analyzer_merge_start_ns;
        }

        var text_build_profile = introducer_mod.BuildTextProfile{};
        const segment_build_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
        const segments = try mapper.buildTextSegmentsFromProjectionBatch(self.alloc, projection_batch, entry.text_analysis, .{
            .target_segment_bytes = @intCast(default_merge_policy.max_segment_size),
            .profile = if (profile_enabled) &text_build_profile else null,
        });
        var segments_owned = true;
        defer self.alloc.free(segments);
        errdefer if (segments_owned) {
            for (segments) |segment| {
                if (segment.len > 0) self.alloc.free(segment);
            }
        };
        if (profile_enabled) segment_build_ns = platform_time.monotonicNs() - segment_build_start_ns;

        var indexed_any = false;
        var segment_bytes: usize = 0;
        const index_segment_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
        for (segments) |*seg| {
            const owned_segment = seg.*;
            segment_bytes += owned_segment.len;
            seg.* = &.{};
            entry.persistent.indexSegmentOwned(owned_segment) catch |err| {
                if (builtin.os.tag != .freestanding) {
                    std.log.err("index text batch indexSegment failed: {s}", .{@errorName(err)});
                }
                return err;
            };
            indexed_any = true;
        }
        segments_owned = false;
        if (profile_enabled) {
            index_segment_ns = platform_time.monotonicNs() - index_segment_start_ns;
            std.log.info(
                "antfly_bench_text_index index={s} source_docs={d} projection_docs={d} observed_analyzers={d} segments={d} segment_bytes={d} total_ms={d} ordinals_ms={d} projection_ms={d} analyzer_merge_ms={d} segment_build_ms={d} index_segment_ms={d} text_docs={d} text_fields={d} tokens={d} term_hits={d} typed_values={d} analyzer_ms={d} term_accum_ms={d} hit_materialize_ms={d} typed_collect_ms={d} typed_build_ms={d} stored_attach_ms={d} section_attach_ms={d} stored_compress_ms={d} segment_assembly_ms={d} segment_encode_ms={d}",
                .{
                    entry.config.name,
                    source_docs.len,
                    projection_batch.docs.len,
                    projection_batch.observed_field_analyzers.len,
                    segments.len,
                    segment_bytes,
                    nsToMs(platform_time.monotonicNs() - total_start_ns),
                    nsToMs(ordinals_ns),
                    nsToMs(projection_ns),
                    nsToMs(analyzer_merge_ns),
                    nsToMs(segment_build_ns),
                    nsToMs(index_segment_ns),
                    text_build_profile.doc_count,
                    text_build_profile.text_field_count,
                    text_build_profile.token_count,
                    text_build_profile.term_hit_count,
                    text_build_profile.typed_value_count,
                    nsToMs(text_build_profile.analyzer_ns),
                    nsToMs(text_build_profile.term_accum_ns),
                    nsToMs(text_build_profile.hit_materialize_ns),
                    nsToMs(text_build_profile.typed_collect_ns),
                    nsToMs(text_build_profile.typed_build_ns),
                    nsToMs(text_build_profile.stored_doc_attach_ns),
                    nsToMs(text_build_profile.section_attach_ns),
                    nsToMs(text_build_profile.stored_compress_ns),
                    nsToMs(text_build_profile.segment_assembly_ns),
                    nsToMs(text_build_profile.segment_encode_ns),
                },
            );
        }
        return .{ .indexed_any = indexed_any };
    }

    fn textProjectionSourceDocsWithOrdinals(
        _: *IndexManager,
        arena: std.mem.Allocator,
        store: *docstore_mod.DocStore,
        source_docs: []const mapper.TextProjectionSourceDoc,
    ) ![]mapper.TextProjectionSourceDoc {
        var docs = try arena.dupe(mapper.TextProjectionSourceDoc, source_docs);

        const PendingOrdinalLookup = struct {
            source_index: usize,
            store_key: []u8,
        };
        var pending = std.ArrayListUnmanaged(PendingOrdinalLookup).empty;
        defer pending.deinit(arena);

        for (docs, 0..) |doc, i| {
            if (doc.doc_ordinal != null) continue;
            try pending.append(arena, .{
                .source_index = i,
                .store_key = try internal_keys.identityDocToOrdinalKeyAlloc(arena, doc.key),
            });
        }
        if (pending.items.len == 0) return docs;

        std.mem.sort(PendingOrdinalLookup, pending.items, {}, struct {
            fn lessThan(_: void, lhs: PendingOrdinalLookup, rhs: PendingOrdinalLookup) bool {
                return std.mem.order(u8, lhs.store_key, rhs.store_key) == .lt;
            }
        }.lessThan);

        const read_keys = try arena.alloc([]const u8, pending.items.len);
        const read_values = try arena.alloc(?[]const u8, pending.items.len);
        for (pending.items, 0..) |item, i| {
            read_keys[i] = item.store_key;
            read_values[i] = null;
        }

        var identity_txn = try store.beginProbeTxn();
        defer identity_txn.abort();
        try identity_txn.getManySorted(read_keys, read_values);

        for (pending.items, 0..) |item, i| {
            const raw = read_values[i] orelse continue;
            if (raw.len != @sizeOf(doc_identity.DocOrdinal)) return error.InvalidDocIdentity;
            docs[item.source_index].doc_ordinal = std.mem.readInt(doc_identity.DocOrdinal, raw[0..4], .big);
        }

        return docs;
    }

    fn textCompactionDue(index: *persistent_mod.PersistentIndex, opts: IndexBatchOptions) bool {
        if (opts.compact_text) return true;
        const threshold = opts.compact_text_segment_threshold orelse return false;
        return index.snapshot().segments.len >= threshold;
    }

    fn deleteTextBatchEntry(_: *IndexManager, entry: *TextIndex, keys: []const []const u8) !TextBatchMutationStats {
        var deleted_any = false;
        for (keys) |key| {
            deleted_any = (try entry.persistent.deleteById(key)) or deleted_any;
        }
        return .{ .deleted_any = deleted_any };
    }

    fn finalizeTextBatchMutations(
        self: *IndexManager,
        entry: *TextIndex,
        opts: IndexBatchOptions,
        stats: TextBatchMutationStats,
    ) !void {
        if (!stats.touched()) return;
        const compaction_due = stats.deleted_any or textCompactionDue(&entry.persistent, opts);
        if (!compaction_due) return;
        if (opts.defer_text_compaction and entry.compaction_pending) return;
        if (!try self.textIndexNeedsMerge(&entry.persistent, default_merge_policy)) {
            TextMergeScheduler.noteComplete(entry);
            return;
        }
        if (opts.defer_text_compaction) {
            TextMergeScheduler.schedule(entry);
            return;
        }
        try self.compactTextIndex(&entry.persistent, default_merge_policy);
        TextMergeScheduler.noteComplete(entry);
    }

    fn indexDenseBatchEntry(
        self: *IndexManager,
        store: *docstore_mod.DocStore,
        entry: *DenseIndex,
        writes: []const types.BatchWrite,
        batch_options: StoreBatchOptions,
    ) !void {
        return self.indexDenseBatchEntryWithSkip(store, entry, writes, null, batch_options);
    }

    fn indexDenseBatchEntryWithSkip(
        self: *IndexManager,
        store: *docstore_mod.DocStore,
        entry: *DenseIndex,
        writes: []const types.BatchWrite,
        skip: ?*const DenseSplitHandoff,
        batch_options: StoreBatchOptions,
    ) !void {
        var store_batch = try store.beginWriteBatchWithOptions(batch_options);
        errdefer store_batch.abort();
        const store_txn = store_batch.asTxn();

        var items = std.ArrayListUnmanaged(hbc_mod.BatchInsertItem).empty;
        defer {
            for (items.items) |item| self.alloc.free(@constCast(item.vector));
            items.deinit(self.alloc);
        }
        var pending_mappings = std.ArrayListUnmanaged(PendingDenseVectorMapping).empty;
        defer pending_mappings.deinit(self.alloc);
        var replacement_deletes = std.ArrayListUnmanaged(u64).empty;
        defer replacement_deletes.deinit(self.alloc);
        var metadata_presence_memo: DenseVectorMetadataPresenceMemo = .{};
        defer metadata_presence_memo.deinit(self.alloc);
        var all_vector_ids_new = true;

        for (writes) |write| {
            if (!self.keyInRange(write.key)) continue;
            if (!isPrimaryDocumentCandidate(write.key)) continue;
            if (skip) |handoff| {
                if (handoff.shouldSkip(write.key)) continue;
            }
            const vector_values = (try mapper.extractDenseVectorField(self.alloc, write.value, entry.field_name, entry.dims)) orelse continue;

            const assignment = try self.replaceDenseVectorIdTxnWithMemo(
                store_txn,
                entry,
                entry.config.name,
                write.key,
                null,
                &replacement_deletes,
                &metadata_presence_memo,
            );
            all_vector_ids_new = all_vector_ids_new and assignment.can_assume_absent;
            try items.append(self.alloc, .{
                .vector_id = assignment.vector_id,
                .vector = vector_values,
                .metadata = write.key,
            });
            try pending_mappings.append(self.alloc, .{
                .doc_key = write.key,
                .parent_doc_key = null,
                .vector_id = assignment.vector_id,
            });
        }

        try self.applyDenseItemsWithOptions(entry, items.items, replacement_deletes.items, batch_options, all_vector_ids_new, store_txn);
        try self.commitDenseVectorMappingsWithRollback(&store_batch, store_txn, entry, entry.config.name, pending_mappings.items);
    }

    fn deleteDenseBatchEntry(
        self: *IndexManager,
        store: *docstore_mod.DocStore,
        entry: *DenseIndex,
        keys: []const []const u8,
        batch_options: StoreBatchOptions,
    ) !void {
        var store_batch = try store.beginWriteBatchWithOptions(batch_options);
        errdefer store_batch.abort();
        const store_txn = store_batch.asTxn();
        var vector_ids = std.ArrayListUnmanaged(u64).empty;
        defer vector_ids.deinit(self.alloc);
        var removed_ordinal_vectors = std.ArrayListUnmanaged(DenseOrdinalVectorCacheUpdate).empty;
        defer removed_ordinal_vectors.deinit(self.alloc);

        for (keys) |key| {
            const vector_id = (try self.resolveDenseVectorIdForDeleteTxn(store_txn, entry.config.name, key)) orelse continue;
            try vector_ids.append(self.alloc, vector_id);
            if (entry.chunk_name == null) {
                if (try doc_identity.lookupOrdinalTxn(self.alloc, store_txn, key)) |ordinal| {
                    try removed_ordinal_vectors.append(self.alloc, .{
                        .ordinal = ordinal,
                        .vector_id = vector_id,
                    });
                }
            }
            try self.clearDenseVectorMappingTxn(store_txn, entry.config.name, key, vector_id);
        }

        entry.index.batchApply(&.{}, vector_ids.items) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
        try store_batch.commit();
        for (removed_ordinal_vectors.items) |removed| {
            _ = entry.ordinal_vector_ids.remove(removed.ordinal);
            _ = entry.vector_ordinals.remove(removed.vector_id);
        }
    }

    fn indexSparseBatchEntry(self: *IndexManager, store: ?*docstore_mod.DocStore, entry: *SparseIndex, writes: []const types.BatchWrite) !void {
        return self.indexSparseBatchEntryWithOptions(store, entry, writes, .{});
    }

    fn indexSparseBatchEntryWithOptions(self: *IndexManager, store: ?*docstore_mod.DocStore, entry: *SparseIndex, writes: []const types.BatchWrite, batch_options: StoreBatchOptions) !void {
        return self.indexSparseBatchEntryWithSkipAndOptions(store, entry, writes, null, batch_options);
    }

    fn indexSparseBatchEntryWithSkip(
        self: *IndexManager,
        store: ?*docstore_mod.DocStore,
        entry: *SparseIndex,
        writes: []const types.BatchWrite,
        skip: ?*const SparseSplitHandoff,
    ) !void {
        return self.indexSparseBatchEntryWithSkipAndOptions(store, entry, writes, skip, .{});
    }

    fn indexSparseBatchEntryWithSkipAndOptions(
        self: *IndexManager,
        store: ?*docstore_mod.DocStore,
        entry: *SparseIndex,
        writes: []const types.BatchWrite,
        skip: ?*const SparseSplitHandoff,
        batch_options: StoreBatchOptions,
    ) !void {
        const ReplayProfile = struct {
            scan_ns: u64 = 0,
            extract_ns: u64 = 0,
            append_ns: u64 = 0,
            batch_ns: u64 = 0,
            in_range: usize = 0,
            primary_candidates: usize = 0,
            skipped_by_handoff: usize = 0,
            extracted: usize = 0,
        };
        const profile_enabled = sparseReplayProfileEnabled();
        const total_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
        var profile: ReplayProfile = .{};
        var sparse_writes = std.ArrayListUnmanaged(sparse_mod.SparseWrite).empty;
        defer {
            for (sparse_writes.items) |item| {
                self.alloc.free(@constCast(item.vec.indices));
                self.alloc.free(@constCast(item.vec.values));
            }
            sparse_writes.deinit(self.alloc);
        }

        const scan_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
        for (writes) |write| {
            if (!self.keyInRange(write.key)) continue;
            if (profile_enabled) profile.in_range += 1;
            if (!isPrimaryDocumentCandidate(write.key)) continue;
            if (profile_enabled) profile.primary_candidates += 1;
            if (skip) |handoff| {
                if (handoff.shouldSkip(write.key)) {
                    if (profile_enabled) profile.skipped_by_handoff += 1;
                    continue;
                }
            }
            const extract_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
            var sparse_vec = (try mapper.extractSparseVectorField(self.alloc, write.value, entry.field_name)) orelse continue;
            var sparse_vec_owned = true;
            errdefer if (sparse_vec_owned) sparse_vec.deinit(self.alloc);
            if (profile_enabled) {
                profile.extract_ns += platform_time.monotonicNs() - extract_start_ns;
                profile.extracted += 1;
            }

            const append_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
            try sparse_writes.append(self.alloc, .{
                .doc_id = write.key,
                .vec = .{
                    .indices = sparse_vec.indices,
                    .values = sparse_vec.values,
                },
            });
            sparse_vec_owned = false;
            if (profile_enabled) profile.append_ns += platform_time.monotonicNs() - append_start_ns;
        }
        if (profile_enabled) profile.scan_ns = platform_time.monotonicNs() - scan_start_ns -| profile.extract_ns -| profile.append_ns;

        const sparse_batch_options: sparse_mod.BatchOptions = .{
            .defer_term_range_updates = true,
            .backend_batch_options = batch_options,
            .prefer_bulk_build = batch_options.mode == .bulk_ingest,
            .assume_new_doc_ids = batch_options.mode == .bulk_ingest,
        };
        if (store) |doc_store| {
            try self.assignSparseWriteDocNumsFromIdentity(doc_store, entry, sparse_writes.items);
        }
        const max_sparse_writes_per_txn: usize = if (batch_options.mode == .bulk_ingest) 16 * 1024 else 256;
        var start: usize = 0;
        while (start < sparse_writes.items.len) {
            const end = @min(start + max_sparse_writes_per_txn, sparse_writes.items.len);
            const batch_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
            try entry.index.batchWithOptions(sparse_writes.items[start..end], &.{}, sparse_batch_options);
            if (profile_enabled) profile.batch_ns += platform_time.monotonicNs() - batch_start_ns;
            start = end;
        }
        if (profile_enabled and (writes.len > 0 or sparse_writes.items.len > 0)) {
            std.log.info(
                "antfly_bench_sparse_doc_index index={s} mode={s} input_writes={d} in_range={d} primary_candidates={d} skipped_by_handoff={d} extracted={d} sparse_writes={d} total_ms={d} scan_filter_ms={d} extract_ms={d} append_ms={d} sparse_batch_ms={d}",
                .{
                    entry.config.name,
                    @tagName(batch_options.mode),
                    writes.len,
                    profile.in_range,
                    profile.primary_candidates,
                    profile.skipped_by_handoff,
                    profile.extracted,
                    sparse_writes.items.len,
                    nsToMs(platform_time.monotonicNs() - total_start_ns),
                    nsToMs(profile.scan_ns),
                    nsToMs(profile.extract_ns),
                    nsToMs(profile.append_ns),
                    nsToMs(profile.batch_ns),
                },
            );
        }
    }

    fn assignSparseWriteDocNumsFromIdentity(
        self: *IndexManager,
        store: *docstore_mod.DocStore,
        entry: *SparseIndex,
        writes: []sparse_mod.SparseWrite,
    ) !void {
        if (entry.chunk_name != null or writes.len == 0) return;

        var txn = try store.beginProbeTxn();
        defer txn.abort();

        const doc_ids = try self.alloc.alloc([]const u8, writes.len);
        defer self.alloc.free(doc_ids);
        for (writes, 0..) |write, i| doc_ids[i] = write.doc_id;

        const ordinals = try doc_identity.lookupOrdinalsTxnAlloc(self.alloc, &txn, doc_ids);
        defer self.alloc.free(ordinals);
        for (ordinals, 0..) |maybe_ordinal, i| {
            const ordinal = maybe_ordinal orelse continue;
            writes[i].doc_num = ordinal;
        }
    }

    fn indexSparsePreparedWritesEntryWithOptions(
        self: *IndexManager,
        entry: *SparseIndex,
        writes: []const sparse_mod.SparseWrite,
        batch_options: StoreBatchOptions,
    ) !void {
        _ = self;
        const profile_enabled = sparseReplayProfileEnabled();
        const total_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
        var batch_ns: u64 = 0;
        const sparse_batch_options: sparse_mod.BatchOptions = .{
            .defer_term_range_updates = true,
            .backend_batch_options = batch_options,
            .prefer_bulk_build = batch_options.mode == .bulk_ingest,
            .assume_new_doc_ids = batch_options.mode == .bulk_ingest,
        };
        const max_sparse_writes_per_txn: usize = if (batch_options.mode == .bulk_ingest) 16 * 1024 else 256;
        var start: usize = 0;
        while (start < writes.len) {
            const end = @min(start + max_sparse_writes_per_txn, writes.len);
            const batch_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
            try entry.index.batchWithOptions(writes[start..end], &.{}, sparse_batch_options);
            if (profile_enabled) batch_ns += platform_time.monotonicNs() - batch_start_ns;
            start = end;
        }
        if (profile_enabled) {
            std.log.info(
                "antfly_bench_sparse_prepared_index index={s} mode={s} sparse_writes={d} total_ms={d} sparse_batch_ms={d}",
                .{
                    entry.config.name,
                    @tagName(batch_options.mode),
                    writes.len,
                    nsToMs(platform_time.monotonicNs() - total_start_ns),
                    nsToMs(batch_ns),
                },
            );
        }
    }

    fn deleteSparseBatchEntry(self: *IndexManager, entry: *SparseIndex, keys: []const []const u8) !void {
        return self.deleteSparseBatchEntryWithOptions(entry, keys, .{});
    }

    fn deleteSparseBatchEntryWithOptions(self: *IndexManager, entry: *SparseIndex, keys: []const []const u8, batch_options: StoreBatchOptions) !void {
        var filtered = std.ArrayListUnmanaged([]const u8).empty;
        defer filtered.deinit(self.alloc);
        for (keys) |key| {
            try filtered.append(self.alloc, key);
        }
        try entry.index.batchWithOptions(&.{}, filtered.items, .{
            .defer_term_range_updates = true,
            .backend_batch_options = batch_options,
        });
    }

    pub fn lookupSparseDocNumsForOrdinalsAlloc(
        self: *IndexManager,
        alloc: Allocator,
        store: *docstore_mod.DocStore,
        index_name: []const u8,
        ordinals: []const doc_identity.DocOrdinal,
    ) ![]const u32 {
        const bench_profile = getenv("ANTFLY_BENCH_QUERY_PROFILE") != null;
        const total_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
        var runtime_store_ns: u64 = 0;
        var primary_txn_ns: u64 = 0;
        var sparse_txn_ns: u64 = 0;
        var doc_id_ns: u64 = 0;
        var doc_num_ns: u64 = 0;
        const entry = self.findSparseIndexEntry(index_name) orelse return error.IndexNotFound;

        const sparse_txn_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
        var sparse_txn = try entry.index.beginReadTxn();
        defer sparse_txn.abort();
        if (bench_profile) sparse_txn_ns = platform_time.monotonicNs() - sparse_txn_start_ns;

        if (entry.chunk_name == null) {
            var fast_lookup = try entry.index.docNumsForOrdinalDocNumsAlloc(alloc, &sparse_txn, ordinals);
            defer fast_lookup.deinit(alloc);
            if (fast_lookup.missing_ordinals.len == 0) {
                const out = try alloc.dupe(u32, fast_lookup.doc_nums);
                if (bench_profile) {
                    std.log.info(
                        "antfly_bench_sparse_ordinal_projection total_us={d} runtime_store_us={d} primary_txn_us={d} sparse_txn_us={d} doc_id_us={d} doc_num_us={d} ordinals={d} doc_nums={d}",
                        .{
                            (platform_time.monotonicNs() - total_start_ns) / 1000,
                            runtime_store_ns / 1000,
                            primary_txn_ns / 1000,
                            sparse_txn_ns / 1000,
                            doc_id_ns / 1000,
                            doc_num_ns / 1000,
                            ordinals.len,
                            out.len,
                        },
                    );
                }
                return out;
            }

            const primary_txn_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
            var txn = try store.beginProbeTxn();
            defer txn.abort();
            if (bench_profile) primary_txn_ns = platform_time.monotonicNs() - primary_txn_start_ns;
            const doc_id_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
            const parent_doc_ids = try lookupSparseProjectionDocIdsForOrdinalsAlloc(alloc, &txn, fast_lookup.missing_ordinals);
            defer alloc.free(parent_doc_ids);
            if (bench_profile) doc_id_ns = platform_time.monotonicNs() - doc_id_start_ns;
            const doc_num_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
            const fallback_doc_nums = try entry.index.docNumsForDocIdsAlloc(alloc, &sparse_txn, parent_doc_ids);
            defer alloc.free(fallback_doc_nums);
            var out = std.ArrayListUnmanaged(u32).empty;
            errdefer out.deinit(alloc);
            try out.appendSlice(alloc, fast_lookup.doc_nums);
            try out.appendSlice(alloc, fallback_doc_nums);
            const out_slice = try out.toOwnedSlice(alloc);
            if (bench_profile) {
                doc_num_ns = platform_time.monotonicNs() - doc_num_start_ns;
                std.log.info(
                    "antfly_bench_sparse_ordinal_projection total_us={d} runtime_store_us={d} primary_txn_us={d} sparse_txn_us={d} doc_id_us={d} doc_num_us={d} ordinals={d} doc_nums={d}",
                    .{
                        (platform_time.monotonicNs() - total_start_ns) / 1000,
                        runtime_store_ns / 1000,
                        primary_txn_ns / 1000,
                        sparse_txn_ns / 1000,
                        doc_id_ns / 1000,
                        doc_num_ns / 1000,
                        ordinals.len,
                        out_slice.len,
                    },
                );
            }
            return out_slice;
        }

        const primary_txn_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
        var txn = try store.beginProbeTxn();
        defer txn.abort();
        if (bench_profile) primary_txn_ns = platform_time.monotonicNs() - primary_txn_start_ns;

        const runtime_store_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
        var runtime_store = try initRuntimeStore(self.alloc, store);
        defer runtime_store.deinit();
        if (bench_profile) runtime_store_ns = platform_time.monotonicNs() - runtime_store_start_ns;

        var out = std.ArrayListUnmanaged(u32).empty;
        errdefer out.deinit(alloc);
        var seen = std.AutoHashMapUnmanaged(u32, void).empty;
        defer seen.deinit(alloc);
        for (ordinals) |ordinal| {
            const parent_doc_id = (try doc_identity.lookupDocIdTxn(self.alloc, &txn, ordinal)) orelse continue;
            defer self.alloc.free(parent_doc_id);
            const prefix = try internal_keys.artifactNamedPrefixAlloc(self.alloc, parent_doc_id, "chunk", entry.chunk_name.?);
            defer self.alloc.free(prefix);
            const upper = try internal_keys.nextPrefixAlloc(self.alloc, prefix);
            defer if (upper) |buf| self.alloc.free(buf);
            const chunk_rows = try backend_scan.scanRange(alloc, &runtime_store.store, prefix, if (upper) |buf| buf else "");
            defer backend_scan.freeResults(alloc, chunk_rows);
            for (chunk_rows) |row| {
                if (!internal_keys.isChunkArtifactRecordKey(row.key)) continue;
                const doc_num = (entry.index.debugDocNumForDocId(row.key) catch |err| switch (err) {
                    error.DocNumOverflow => continue,
                    else => return err,
                }) orelse continue;
                const gop = try seen.getOrPut(alloc, doc_num);
                if (!gop.found_existing) try out.append(alloc, doc_num);
            }
        }
        return try out.toOwnedSlice(alloc);
    }

    fn lookupSparseProjectionDocIdsForOrdinalsAlloc(alloc: Allocator, txn: anytype, ordinals: []const doc_identity.DocOrdinal) ![]const []const u8 {
        if (ordinals.len == 0) return try alloc.alloc([]const u8, 0);

        const sorted_ordinals = try alloc.dupe(doc_identity.DocOrdinal, ordinals);
        defer alloc.free(sorted_ordinals);
        std.mem.sort(doc_identity.DocOrdinal, sorted_ordinals, {}, docOrdinalLessThan);

        const IdentityOrdinalKey = @TypeOf(internal_keys.identityOrdinalToDocKey(0));
        var key_storage = try alloc.alloc(IdentityOrdinalKey, sorted_ordinals.len);
        defer alloc.free(key_storage);
        var keys = try alloc.alloc([]const u8, sorted_ordinals.len);
        defer alloc.free(keys);
        var values = try alloc.alloc(?[]const u8, sorted_ordinals.len);
        defer alloc.free(values);

        var key_count: usize = 0;
        var previous: ?doc_identity.DocOrdinal = null;
        for (sorted_ordinals) |ordinal| {
            if (previous != null and previous.? == ordinal) continue;
            key_storage[key_count] = internal_keys.identityOrdinalToDocKey(ordinal);
            keys[key_count] = key_storage[key_count][0..];
            values[key_count] = null;
            key_count += 1;
            previous = ordinal;
        }

        try txn.getManySorted(keys[0..key_count], values[0..key_count]);

        var doc_ids = std.ArrayListUnmanaged([]const u8).empty;
        errdefer doc_ids.deinit(alloc);
        try doc_ids.ensureTotalCapacity(alloc, key_count);
        for (values[0..key_count]) |maybe_raw| {
            if (maybe_raw) |raw| doc_ids.appendAssumeCapacity(raw);
        }
        return try doc_ids.toOwnedSlice(alloc);
    }

    fn deleteGraphDocsEntry(self: *IndexManager, entry: *GraphIndex, keys: []const []const u8) !void {
        var deletes = std.ArrayListUnmanaged(graph_mod.BatchDelete).empty;
        defer {
            for (deletes.items) |delete| {
                self.alloc.free(@constCast(delete.source));
                self.alloc.free(@constCast(delete.target));
                self.alloc.free(@constCast(delete.edge_type));
            }
            deletes.deinit(self.alloc);
        }

        for (keys) |key| {
            const edges = try entry.index.getEdges(self.alloc, key, "", .both);
            defer graph_mod.GraphIndex.freeEdges(self.alloc, edges);

            for (edges) |edge| {
                try deletes.append(self.alloc, .{
                    .source = try self.alloc.dupe(u8, edge.source),
                    .target = try self.alloc.dupe(u8, edge.target),
                    .edge_type = try self.alloc.dupe(u8, edge.edge_type),
                });
            }
        }

        try entry.index.batchApply(&.{}, deletes.items);
    }

    fn applyGraphWritesEntry(self: *IndexManager, entry: *GraphIndex, writes: []const types.GraphEdgeWrite) !void {
        try self.applyGraphMutationsEntry(entry, writes, &.{});
    }

    fn applyGraphDeletesEntry(self: *IndexManager, entry: *GraphIndex, deletes: []const types.GraphEdgeDelete) !void {
        try self.applyGraphMutationsEntry(entry, &.{}, deletes);
    }

    fn applyGraphMutationsEntry(
        self: *IndexManager,
        entry: *GraphIndex,
        writes: []const types.GraphEdgeWrite,
        deletes: []const types.GraphEdgeDelete,
    ) !void {
        var batch_writes = std.ArrayListUnmanaged(graph_mod.BatchWrite).empty;
        defer batch_writes.deinit(self.alloc);
        var batch_deletes = std.ArrayListUnmanaged(graph_mod.BatchDelete).empty;
        defer batch_deletes.deinit(self.alloc);

        for (writes) |write| {
            if (!self.keyInRange(write.source)) continue;
            if (!std.mem.eql(u8, write.index_name, entry.config.name)) continue;
            try batch_writes.append(self.alloc, .{
                .source = write.source,
                .target = write.target,
                .edge_type = write.edge_type,
                .weight = write.weight,
                .created_at = write.created_at,
                .updated_at = write.updated_at,
                .metadata_json = write.metadata_json,
            });
        }

        for (deletes) |delete| {
            if (!self.keyInRange(delete.source)) continue;
            if (!std.mem.eql(u8, delete.index_name, entry.config.name)) continue;
            try batch_deletes.append(self.alloc, .{
                .source = delete.source,
                .target = delete.target,
                .edge_type = delete.edge_type,
            });
        }

        try entry.index.batchApply(batch_writes.items, batch_deletes.items);
    }

    fn applyDenseEmbeddingWritesEntry(
        self: *IndexManager,
        store: *docstore_mod.DocStore,
        entry: *DenseIndex,
        writes: []const mapper.DenseEmbeddingWrite,
        batch_options: StoreBatchOptions,
    ) !void {
        const keep_write = try self.alloc.alloc(bool, writes.len);
        defer self.alloc.free(keep_write);
        try computeDenseReplayKeepMask(self.alloc, writes, keep_write);

        var dense_apply_working_bytes: u64 = 0;
        defer self.observeDenseApplyWorkingBytes(&dense_apply_working_bytes, 0);
        var store_batch = try store.beginWriteBatchWithOptions(batch_options);
        errdefer store_batch.abort();
        const store_txn = store_batch.asTxn();
        const previous_load_session = active_dense_vector_load_session;
        var vector_load_session: ?DenseVectorLoadSession = null;
        defer {
            active_dense_vector_load_session = previous_load_session;
            if (vector_load_session != null) entry.index.setBypassExternalVectorCache(false);
            if (vector_load_session) |*session| session.deinit();
        }
        if (self.primary_store != null and entry.vector_loader_context != null) {
            const existing_session = active_dense_vector_load_session;
            const reuse_existing_session = existing_session != null and existing_session.?.context == entry.vector_loader_context.?;
            if (!reuse_existing_session) {
                vector_load_session = .{
                    .context = entry.vector_loader_context.?,
                    .txn_override = store_txn,
                };
                active_dense_vector_load_session = &vector_load_session.?;
                entry.index.setBypassExternalVectorCache(true);
            }
        }
        const existing_vector_scratch = try self.alloc.alloc(f32, entry.dims);
        defer self.alloc.free(existing_vector_scratch);
        const preloaded_artifact_vectors = try self.alloc.alloc(?[]const f32, writes.len);
        defer {
            self.alloc.free(preloaded_artifact_vectors);
        }
        for (preloaded_artifact_vectors) |*slot| slot.* = null;
        var preloaded_artifact_vector_scratch = std.ArrayListUnmanaged(f32).empty;
        defer preloaded_artifact_vector_scratch.deinit(self.alloc);
        var corrupt_artifact_deletes = std.ArrayListUnmanaged([]const u8).empty;
        defer corrupt_artifact_deletes.deinit(self.alloc);

        var items: OwnedDenseInsertItems = .{};
        defer items.deinit(self.alloc);
        _ = try items.ensureArena(self.alloc);
        var new_items = std.ArrayListUnmanaged(hbc_mod.BatchInsertItem).empty;
        defer new_items.deinit(self.alloc);
        var existing_items = std.ArrayListUnmanaged(hbc_mod.BatchInsertItem).empty;
        defer existing_items.deinit(self.alloc);
        var pending_mappings = std.ArrayListUnmanaged(PendingDenseVectorMapping).empty;
        defer pending_mappings.deinit(self.alloc);
        var replacement_deletes = std.ArrayListUnmanaged(u64).empty;
        defer replacement_deletes.deinit(self.alloc);
        var metadata_presence_memo: DenseVectorMetadataPresenceMemo = .{};
        defer metadata_presence_memo.deinit(self.alloc);
        var all_vector_ids_new = true;
        var preloaded_vector_bytes: u64 = 0;
        var item_vector_bytes: u64 = 0;
        var item_metadata_bytes: u64 = 0;

        if (try self.preloadDenseEmbeddingArtifactVectorsTxn(
            entry.config.name,
            entry.dims,
            writes,
            keep_write,
            &store_txn,
            preloaded_artifact_vectors,
            &preloaded_artifact_vector_scratch,
            &corrupt_artifact_deletes,
        )) {
            for (corrupt_artifact_deletes.items) |artifact_key| {
                try store_txn.delete(artifact_key);
            }
        }
        for (preloaded_artifact_vectors) |maybe_vector| {
            if (maybe_vector) |vector| preloaded_vector_bytes += @as(u64, @intCast(vector.len * @sizeOf(f32)));
        }
        self.observeDenseApplyWorkingBytes(&dense_apply_working_bytes, preloaded_vector_bytes);
        {
            var existing_index_write_txn = try entry.index.beginRuntimeWriteTxn();
            defer existing_index_write_txn.abort();
            try self.prefetchDenseExistingMetadataTxn(entry, store_txn, &existing_index_write_txn, writes, keep_write, &metadata_presence_memo);

            for (writes, 0..) |write, write_index| {
                if (!keep_write[write_index]) continue;
                if (!std.mem.eql(u8, write.index_name, entry.config.name)) continue;

                if (write.vector.len > 0) {
                    if (entry.dims != write.vector.len) return error.InvalidVectorDimensions;
                    const assignment = try self.replaceDenseVectorIdTxnWithMemo(
                        store_txn,
                        entry,
                        write.index_name,
                        write.doc_key,
                        write.parent_doc_key,
                        &replacement_deletes,
                        &metadata_presence_memo,
                    );
                    const artifact_name = entry.embedding_name orelse entry.config.name;
                    try self.writeDenseEmbeddingArtifactTxn(store_txn, write.doc_key, write.doc_key, artifact_name, "_embeddings", null, write.vector);
                    if (try self.denseVectorWriteIsNoOp(entry, &existing_index_write_txn, assignment.vector_id, write.doc_key, write.vector, existing_vector_scratch, &metadata_presence_memo)) continue;
                    all_vector_ids_new = all_vector_ids_new and assignment.can_assume_absent;
                    try items.appendBorrowed(self.alloc, assignment.vector_id, write.vector, write.doc_key);
                    if (assignment.can_assume_absent) {
                        try new_items.append(self.alloc, items.items.items[items.items.items.len - 1]);
                    } else {
                        try existing_items.append(self.alloc, items.items.items[items.items.items.len - 1]);
                    }
                    item_vector_bytes += @as(u64, @intCast(write.vector.len * @sizeOf(f32)));
                    item_metadata_bytes += @intCast(write.doc_key.len);
                    self.observeDenseApplyWorkingBytes(&dense_apply_working_bytes, preloaded_vector_bytes + item_vector_bytes + item_metadata_bytes);
                    try pending_mappings.append(self.alloc, .{
                        .doc_key = items.items.items[items.items.items.len - 1].metadata,
                        .parent_doc_key = write.parent_doc_key,
                        .vector_id = assignment.vector_id,
                    });
                } else if (write.artifact_key != null) {
                    const vector = preloaded_artifact_vectors[write_index] orelse continue;
                    const assignment = try self.replaceDenseVectorIdTxnWithMemo(
                        store_txn,
                        entry,
                        write.index_name,
                        write.doc_key,
                        write.parent_doc_key,
                        &replacement_deletes,
                        &metadata_presence_memo,
                    );
                    if (try self.denseVectorWriteIsNoOp(entry, &existing_index_write_txn, assignment.vector_id, write.doc_key, vector, existing_vector_scratch, &metadata_presence_memo)) continue;
                    all_vector_ids_new = all_vector_ids_new and assignment.can_assume_absent;
                    try items.appendBorrowedVectorOwnedMetadata(self.alloc, assignment.vector_id, vector, write.doc_key);
                    if (assignment.can_assume_absent) {
                        try new_items.append(self.alloc, items.items.items[items.items.items.len - 1]);
                    } else {
                        try existing_items.append(self.alloc, items.items.items[items.items.items.len - 1]);
                    }
                    item_metadata_bytes += @intCast(write.doc_key.len);
                    self.observeDenseApplyWorkingBytes(&dense_apply_working_bytes, preloaded_vector_bytes + item_vector_bytes + item_metadata_bytes);
                    try pending_mappings.append(self.alloc, .{
                        .doc_key = items.items.items[items.items.items.len - 1].metadata,
                        .parent_doc_key = write.parent_doc_key,
                        .vector_id = assignment.vector_id,
                    });
                } else {
                    continue;
                }
            }
        }

        if (items.items.items.len == 0) {
            if (replacement_deletes.items.len > 0) {
                try entry.index.batchApplyOptions(
                    &.{},
                    replacement_deletes.items,
                    denseHbcBatchOptions(batch_options, true, entry.index.hasExternalVectorLoader()),
                );
            }
            try store_batch.commit();
            return;
        }

        // Explicit embedding writes can be the first population path for an
        // otherwise empty external dense index. Use the direct HBC insert path
        // there for default live writes; replay/bulk-ingest can take the HBC
        // bulk builder because the input is already a derived batch.
        if (entry.index.stats().active_count == 0 and batch_options.mode == .default) {
            const before_stats = entry.index.stats();
            const before_profile = entry.index.getWriteProfile();
            const before_lsm_stats = entry.index.snapshotLsmWriteStats();
            const started = platform_time.monotonicNs();
            const skip_vector_store = entry.index.hasExternalVectorLoader();
            if (skip_vector_store) entry.index.setBypassExternalVectorCache(true);
            defer if (skip_vector_store) entry.index.setBypassExternalVectorCache(false);
            try entry.index.batchInsertWithMetadataOptions(
                items.items.items,
                denseHbcBatchOptions(batch_options, all_vector_ids_new, skip_vector_store),
            );
            logBenchHbcWrite(self.alloc, "explicit_empty_batch_insert", entry, items.items.items.len, batch_options, all_vector_ids_new, before_stats, before_profile, before_lsm_stats, started);
        } else {
            if (new_items.items.len > 0) {
                try self.applyDenseItemsWithOptions(entry, new_items.items, &.{}, batch_options, true, store_txn);
            }
            if (existing_items.items.len > 0 or replacement_deletes.items.len > 0) {
                try self.applyDenseItemsWithOptions(entry, existing_items.items, replacement_deletes.items, batch_options, false, store_txn);
            }
        }
        try self.commitDenseVectorMappingsWithRollback(&store_batch, store_txn, entry, entry.config.name, pending_mappings.items);
    }

    const PendingDenseArtifactLoad = struct {
        write_index: usize,
        artifact_key: []const u8,
    };

    fn preloadDenseEmbeddingArtifactVectorsTxn(
        self: *IndexManager,
        index_name: []const u8,
        dims: u32,
        writes: []const mapper.DenseEmbeddingWrite,
        keep_write: []const bool,
        store_txn: anytype,
        out_vectors: []?[]const f32,
        fallback_scratch: *std.ArrayListUnmanaged(f32),
        corrupt_artifact_deletes: *std.ArrayListUnmanaged([]const u8),
    ) !bool {
        return try self.preloadDenseEmbeddingArtifactVectorsFromTxn(
            index_name,
            dims,
            writes,
            keep_write,
            store_txn,
            out_vectors,
            fallback_scratch,
            corrupt_artifact_deletes,
        );
    }

    fn preloadDenseEmbeddingArtifactVectorsFromSession(
        self: *IndexManager,
        index_name: []const u8,
        dims: u32,
        writes: []const mapper.DenseEmbeddingWrite,
        keep_write: []const bool,
        store: *docstore_mod.DocStore,
        session: *DenseVectorLoadSession,
        out_vectors: []?[]const f32,
        fallback_scratch: *std.ArrayListUnmanaged(f32),
        corrupt_artifact_deletes: *std.ArrayListUnmanaged([]const u8),
    ) !bool {
        const dims_usize: usize = @intCast(dims);
        var pending = std.ArrayListUnmanaged(PendingDenseArtifactLoad).empty;
        defer pending.deinit(self.alloc);

        for (writes, 0..) |write, write_index| {
            if (!keep_write[write_index]) continue;
            if (!std.mem.eql(u8, write.index_name, index_name)) continue;
            if (write.vector.len > 0) continue;
            const artifact_key = write.artifact_key orelse continue;
            try pending.append(self.alloc, .{
                .write_index = write_index,
                .artifact_key = artifact_key,
            });
        }
        if (pending.items.len == 0) return false;

        std.mem.sort(PendingDenseArtifactLoad, pending.items, {}, struct {
            fn lessThan(_: void, a: PendingDenseArtifactLoad, b: PendingDenseArtifactLoad) bool {
                return std.mem.order(u8, a.artifact_key, b.artifact_key) == .lt;
            }
        }.lessThan);

        const read_keys = try self.alloc.alloc([]const u8, pending.items.len);
        defer self.alloc.free(read_keys);
        const read_values = try self.alloc.alloc(?[]const u8, pending.items.len);
        defer self.alloc.free(read_values);

        for (pending.items, 0..) |item, i| read_keys[i] = item.artifact_key;
        try session.getManySorted(store, read_keys, read_values);

        var fallback_count: usize = 0;
        for (pending.items, 0..) |item, i| {
            const raw = read_values[i] orelse continue;
            if (enrichment_artifact_codec.denseEmbeddingVectorView(raw)) |maybe_view| {
                if (maybe_view) |view| {
                    if (view.len != dims_usize) return error.InvalidVectorDimensions;
                    continue;
                }
                fallback_count += 1;
            } else |err| {
                if (isRecoverableEmbeddingArtifactError(err)) {
                    try corrupt_artifact_deletes.append(self.alloc, item.artifact_key);
                    continue;
                }
                return err;
            }
        }

        const fallback_start = fallback_scratch.items.len;
        if (fallback_count > 0) try fallback_scratch.resize(self.alloc, fallback_start + fallback_count * dims_usize);
        var fallback_index: usize = 0;
        for (pending.items, 0..) |item, i| {
            const raw = read_values[i] orelse continue;
            if (enrichment_artifact_codec.denseEmbeddingVectorView(raw)) |maybe_view| {
                if (maybe_view) |view| {
                    out_vectors[item.write_index] = view;
                    continue;
                }
            } else |err| {
                if (isRecoverableEmbeddingArtifactError(err)) {
                    try corrupt_artifact_deletes.append(self.alloc, item.artifact_key);
                    continue;
                }
                return err;
            }
            const vector = fallback_scratch.items[fallback_start + fallback_index * dims_usize ..][0..dims_usize];
            fallback_index += 1;
            _ = enrichment_artifact_codec.decodeDenseEmbeddingInto(raw, vector) catch |err| {
                if (isRecoverableEmbeddingArtifactError(err)) {
                    try corrupt_artifact_deletes.append(self.alloc, item.artifact_key);
                    continue;
                }
                return err;
            };
            if (vector.len != dims_usize) {
                return error.InvalidVectorDimensions;
            }
            out_vectors[item.write_index] = vector;
        }
        return true;
    }

    fn preloadDenseEmbeddingArtifactVectorsFromTxn(
        self: *IndexManager,
        index_name: []const u8,
        dims: u32,
        writes: []const mapper.DenseEmbeddingWrite,
        keep_write: []const bool,
        txn: anytype,
        out_vectors: []?[]const f32,
        fallback_scratch: *std.ArrayListUnmanaged(f32),
        corrupt_artifact_deletes: *std.ArrayListUnmanaged([]const u8),
    ) !bool {
        const dims_usize: usize = @intCast(dims);
        var pending = std.ArrayListUnmanaged(PendingDenseArtifactLoad).empty;
        defer pending.deinit(self.alloc);

        for (writes, 0..) |write, write_index| {
            if (!keep_write[write_index]) continue;
            if (!std.mem.eql(u8, write.index_name, index_name)) continue;
            if (write.vector.len > 0) continue;
            const artifact_key = write.artifact_key orelse continue;
            try pending.append(self.alloc, .{
                .write_index = write_index,
                .artifact_key = artifact_key,
            });
        }
        if (pending.items.len == 0) return false;

        std.mem.sort(PendingDenseArtifactLoad, pending.items, {}, struct {
            fn lessThan(_: void, a: PendingDenseArtifactLoad, b: PendingDenseArtifactLoad) bool {
                return std.mem.order(u8, a.artifact_key, b.artifact_key) == .lt;
            }
        }.lessThan);

        const read_keys = try self.alloc.alloc([]const u8, pending.items.len);
        defer self.alloc.free(read_keys);
        const read_values = try self.alloc.alloc(?[]const u8, pending.items.len);
        defer self.alloc.free(read_values);

        for (pending.items, 0..) |item, i| read_keys[i] = item.artifact_key;
        try txn.getManySorted(read_keys, read_values);

        var fallback_count: usize = 0;
        for (pending.items, 0..) |item, i| {
            const raw = read_values[i] orelse continue;
            if (enrichment_artifact_codec.denseEmbeddingVectorView(raw)) |maybe_view| {
                if (maybe_view) |view| {
                    if (view.len != dims_usize) return error.InvalidVectorDimensions;
                    continue;
                }
                fallback_count += 1;
            } else |err| {
                if (isRecoverableEmbeddingArtifactError(err)) {
                    try corrupt_artifact_deletes.append(self.alloc, item.artifact_key);
                    continue;
                }
                return err;
            }
        }

        const fallback_start = fallback_scratch.items.len;
        if (fallback_count > 0) try fallback_scratch.resize(self.alloc, fallback_start + fallback_count * dims_usize);
        var fallback_index: usize = 0;
        for (pending.items, 0..) |item, i| {
            const raw = read_values[i] orelse continue;
            if (enrichment_artifact_codec.denseEmbeddingVectorView(raw)) |maybe_view| {
                if (maybe_view) |view| {
                    out_vectors[item.write_index] = view;
                    continue;
                }
            } else |err| {
                if (isRecoverableEmbeddingArtifactError(err)) {
                    try corrupt_artifact_deletes.append(self.alloc, item.artifact_key);
                    continue;
                }
                return err;
            }
            const vector = fallback_scratch.items[fallback_start + fallback_index * dims_usize ..][0..dims_usize];
            fallback_index += 1;
            _ = enrichment_artifact_codec.decodeDenseEmbeddingInto(raw, vector) catch |err| {
                if (isRecoverableEmbeddingArtifactError(err)) {
                    try corrupt_artifact_deletes.append(self.alloc, item.artifact_key);
                    continue;
                }
                return err;
            };
            if (vector.len != dims_usize) {
                return error.InvalidVectorDimensions;
            }
            out_vectors[item.write_index] = vector;
        }
        return true;
    }

    fn persistDenseVectorMappings(self: *IndexManager, store: anytype, index_name: []const u8, pending: []const PendingDenseVectorMapping) !void {
        if (pending.len == 0) return;

        var runtime_store = try initRuntimeStore(self.alloc, store);
        defer runtime_store.deinit();

        var batch = try runtime_store.store.beginBatch();
        errdefer batch.abort();
        for (pending) |mapping| {
            try self.writeDenseVectorMappingTxn(&batch, index_name, mapping.doc_key, mapping.parent_doc_key, mapping.vector_id);
        }
        try batch.commit();
    }

    fn commitDenseVectorMappingsTxn(self: *IndexManager, txn: anytype, index_name: []const u8, pending: []const PendingDenseVectorMapping) !void {
        for (pending) |mapping| {
            try self.writeDenseVectorMappingTxn(txn, index_name, mapping.doc_key, mapping.parent_doc_key, mapping.vector_id);
        }
    }

    const DenseReplayKey = struct {
        index_name: []const u8,
        doc_key: []const u8,
    };

    const DenseReplayKeyContext = struct {
        pub fn hash(_: @This(), key: DenseReplayKey) u64 {
            var hasher = std.hash.Wyhash.init(0);
            const index_name_len: u64 = @intCast(key.index_name.len);
            const doc_key_len: u64 = @intCast(key.doc_key.len);
            hasher.update(std.mem.asBytes(&index_name_len));
            hasher.update(key.index_name);
            hasher.update(std.mem.asBytes(&doc_key_len));
            hasher.update(key.doc_key);
            return hasher.final();
        }

        pub fn eql(_: @This(), a: DenseReplayKey, b: DenseReplayKey) bool {
            return std.mem.eql(u8, a.index_name, b.index_name) and
                std.mem.eql(u8, a.doc_key, b.doc_key);
        }
    };

    fn computeDenseReplayKeepMask(alloc: Allocator, writes: []const mapper.DenseEmbeddingWrite, keep_write: []bool) !void {
        @memset(keep_write, true);
        if (writes.len <= 1) return;

        var seen = std.HashMapUnmanaged(DenseReplayKey, void, DenseReplayKeyContext, 80).empty;
        defer seen.deinit(alloc);

        var i: usize = writes.len;
        while (i > 0) {
            i -= 1;
            const key: DenseReplayKey = .{
                .index_name = writes[i].index_name,
                .doc_key = writes[i].doc_key,
            };
            if (seen.contains(key)) {
                keep_write[i] = false;
                continue;
            }
            try seen.put(alloc, key, {});
        }
    }

    fn denseVectorWriteIsNoOp(
        self: *IndexManager,
        entry: *DenseIndex,
        txn: anytype,
        vector_id: u64,
        doc_key: []const u8,
        vector: []const f32,
        scratch: []f32,
        metadata_memo: ?*DenseVectorMetadataPresenceMemo,
    ) !bool {
        _ = self;
        const existing_metadata = blk: {
            if (metadata_memo) |memo| {
                if (memo.get(vector_id)) |present| {
                    if (!present) return false;
                    if (memo.getMetadata(vector_id)) |metadata| break :blk metadata;
                }
            }
            break :blk (try entry.index.getMetadataInTxn(txn, vector_id)) orelse return false;
        };
        if (!std.mem.eql(u8, existing_metadata, doc_key)) return false;
        const existing_vector = entry.index.getVectorScratch(txn, vector_id, scratch) catch |err| switch (err) {
            error.NotFound => return false,
            else => return err,
        };
        if (existing_vector.len != vector.len) return false;
        return std.mem.eql(u8, std.mem.sliceAsBytes(existing_vector), std.mem.sliceAsBytes(vector));
    }

    fn rollbackPendingDenseVectors(self: *IndexManager, entry: *DenseIndex, pending: []const PendingDenseVectorMapping) void {
        if (pending.len == 0) return;
        const vector_ids = self.alloc.alloc(u64, pending.len) catch return;
        defer self.alloc.free(vector_ids);
        for (pending, 0..) |mapping, i| vector_ids[i] = mapping.vector_id;
        entry.index.batchDelete(vector_ids) catch {};
        entry.index.clearMetadataCache();
    }

    fn commitDenseVectorMappingsWithRollback(
        self: *IndexManager,
        batch: anytype,
        txn: anytype,
        entry: *DenseIndex,
        index_name: []const u8,
        pending: []const PendingDenseVectorMapping,
    ) !void {
        const cache_updates = try self.collectDenseOrdinalVectorCacheUpdatesTxn(self.alloc, txn, pending);
        defer self.alloc.free(cache_updates);
        self.commitDenseVectorMappingsTxn(txn, index_name, pending) catch |err| {
            self.rollbackPendingDenseVectors(entry, pending);
            return err;
        };
        batch.commit() catch |err| {
            self.rollbackPendingDenseVectors(entry, pending);
            return err;
        };
        try self.applyDenseOrdinalVectorCacheUpdates(entry, cache_updates);
    }

    fn persistDenseVectorMappingsWithRollback(
        self: *IndexManager,
        store: anytype,
        entry: *DenseIndex,
        index_name: []const u8,
        pending: []const PendingDenseVectorMapping,
    ) !void {
        self.persistDenseVectorMappings(store, index_name, pending) catch |err| {
            if (pending.len > 0) {
                const vector_ids = self.alloc.alloc(u64, pending.len) catch return err;
                defer self.alloc.free(vector_ids);
                for (pending, 0..) |mapping, i| vector_ids[i] = mapping.vector_id;
                entry.index.batchDelete(vector_ids) catch {};
                entry.index.clearMetadataCache();
            }
            return err;
        };
        try self.refreshDenseOrdinalVectorCacheFromStoreAlloc(store, entry, index_name, pending);
    }

    fn collectDenseOrdinalVectorCacheUpdatesTxn(
        self: *IndexManager,
        alloc: Allocator,
        txn: anytype,
        pending: []const PendingDenseVectorMapping,
    ) ![]DenseOrdinalVectorCacheUpdate {
        const mutable_txn = txn;
        var updates = std.ArrayListUnmanaged(DenseOrdinalVectorCacheUpdate).empty;
        errdefer updates.deinit(alloc);
        for (pending) |mapping| {
            const ordinal_doc_key = mapping.parent_doc_key orelse mapping.doc_key;
            const ordinal = (try doc_identity.lookupOrdinalTxn(self.alloc, mutable_txn, ordinal_doc_key)) orelse continue;
            try updates.append(alloc, .{
                .ordinal = ordinal,
                .vector_id = mapping.vector_id,
            });
        }
        return try updates.toOwnedSlice(alloc);
    }

    fn applyDenseOrdinalVectorCacheUpdates(
        self: *IndexManager,
        entry: *DenseIndex,
        updates: []const DenseOrdinalVectorCacheUpdate,
    ) !void {
        if (entry.chunk_name != null) return;
        for (updates) |update| {
            try entry.ordinal_vector_ids.put(self.alloc, update.ordinal, update.vector_id);
            try entry.vector_ordinals.put(self.alloc, update.vector_id, update.ordinal);
        }
        if (benchMetricsEnabled()) {
            std.log.info(
                "antfly_bench_dense_ordinal_cache index={s} updates={d} ordinal_cache={d} vector_cache={d}",
                .{ entry.config.name, updates.len, entry.ordinal_vector_ids.count(), entry.vector_ordinals.count() },
            );
        }
    }

    fn refreshDenseOrdinalVectorCacheFromStoreAlloc(
        self: *IndexManager,
        store: anytype,
        entry: *DenseIndex,
        index_name: []const u8,
        pending: []const PendingDenseVectorMapping,
    ) !void {
        _ = index_name;
        if (entry.chunk_name != null or pending.len == 0) return;
        var runtime_store = try initRuntimeStore(self.alloc, store);
        defer runtime_store.deinit();
        var txn = try runtime_store.store.beginRead();
        defer txn.abort();
        const updates = try self.collectDenseOrdinalVectorCacheUpdatesTxn(self.alloc, &txn, pending);
        defer self.alloc.free(updates);
        try self.applyDenseOrdinalVectorCacheUpdates(entry, updates);
    }

    fn insertDenseItems(self: *IndexManager, entry: *DenseIndex, items: []const hbc_mod.BatchInsertItem) !void {
        try self.insertDenseItemsWithOptions(entry, items, .{}, false);
    }

    fn insertDenseItemsWithOptions(
        self: *IndexManager,
        entry: *DenseIndex,
        items: []const hbc_mod.BatchInsertItem,
        batch_options: StoreBatchOptions,
        all_vector_ids_new: bool,
    ) !void {
        try self.applyDenseItemsWithOptions(entry, items, &.{}, batch_options, all_vector_ids_new, null);
    }

    fn applyDenseItemsWithOptions(
        self: *IndexManager,
        entry: *DenseIndex,
        items: []const hbc_mod.BatchInsertItem,
        deletes: []const u64,
        batch_options: StoreBatchOptions,
        all_vector_ids_new: bool,
        primary_store_txn: ?docstore_mod.DocStore.Batch.BatchTxn,
    ) !void {
        if (items.len == 0 and deletes.len == 0) return;
        const before_stats = entry.index.stats();
        const before_profile = entry.index.getWriteProfile();
        const before_lsm_stats = entry.index.snapshotLsmWriteStats();
        const started = platform_time.monotonicNs();
        const previous_load_session = active_dense_vector_load_session;
        var vector_load_session: ?DenseVectorLoadSession = null;
        const reuse_existing_session = blk: {
            if (active_dense_vector_load_session == null) break :blk false;
            if (entry.vector_loader_context == null) break :blk false;
            break :blk active_dense_vector_load_session.?.context == entry.vector_loader_context.?;
        };
        defer {
            if (vector_load_session != null) {
                active_dense_vector_load_session = previous_load_session;
                entry.index.setBypassExternalVectorCache(false);
            }
            if (vector_load_session) |*session| session.deinit();
        }
        if (!reuse_existing_session and self.primary_store != null and entry.vector_loader_context != null) {
            vector_load_session = .{
                .context = entry.vector_loader_context.?,
                .txn_override = primary_store_txn,
            };
            active_dense_vector_load_session = &vector_load_session.?;
            entry.index.setBypassExternalVectorCache(true);
        }
        if (deletes.len == 0 and entry.index.stats().active_count == 0 and shouldBulkBuildEmptyDenseIndex(batch_options, items.len)) {
            const skip_vector_store = entry.index.hasExternalVectorLoader();
            const bulk_options: hbc_mod.BulkBuildOptions = .{
                .skip_vector_store = skip_vector_store,
                .algo = if (batch_options.mode == .bulk_ingest) .recursive else null,
            };
            entry.index.bulkBuildWithMetadataOptions(items, bulk_options) catch |err| switch (err) {
                error.IndexNotEmpty => {
                    try entry.index.batchApplyOptions(items, deletes, denseHbcBatchOptions(batch_options, false, skip_vector_store));
                    logBenchHbcWrite(self.alloc, "bulk_build_fallback_batch_apply", entry, items.len, batch_options, false, before_stats, before_profile, before_lsm_stats, started);
                    return;
                },
                else => return err,
            };
            logBenchHbcWrite(self.alloc, "bulk_build", entry, items.len, batch_options, all_vector_ids_new, before_stats, before_profile, before_lsm_stats, started);
            return;
        }
        try entry.index.batchApplyOptions(
            items,
            deletes,
            denseHbcBatchOptions(batch_options, all_vector_ids_new, entry.index.hasExternalVectorLoader()),
        );
        logBenchHbcWrite(self.alloc, "batch_apply", entry, items.len, batch_options, all_vector_ids_new, before_stats, before_profile, before_lsm_stats, started);
    }

    fn denseHbcBatchOptions(batch_options: StoreBatchOptions, all_vector_ids_new: bool, skip_vector_store: bool) hbc_mod.BatchInsertOptions {
        const bulk_ingest = batch_options.mode == .bulk_ingest;
        const bulk_new_vectors = bulk_ingest and all_vector_ids_new;
        return .{
            .assume_absent_ids = bulk_new_vectors,
            .centroid_only_routing = bulk_ingest,
            .allow_quantized_routing = bulk_ingest,
            .coalesce_leaf_writes = bulk_ingest and hbcCoalesceBulkWritesEnabled(),
            .defer_quantized_rebuild = bulk_ingest,
            .defer_quantized_rebuild_to_bulk_finish = bulk_ingest and hbcDeferBulkQuantizedRebuildEnabled(),
            .skip_vector_store = skip_vector_store,
            .bulk_ingest = bulk_ingest,
            .defer_leaf_splits_to_batch_finish = bulk_ingest and hbcDeferBulkLeafSplitsEnabled(),
            .defer_leaf_splits_to_bulk_finish = false,
            .bulk_rebuild_leaf_min_members = if (bulk_ingest) hbcBulkRebuildLeafMinMembers() else 0,
        };
    }

    fn shouldBulkBuildEmptyDenseIndex(batch_options: StoreBatchOptions, item_count: usize) bool {
        return switch (batch_options.mode) {
            .default => true,
            .bulk_ingest => item_count >= hbcBulkIngestBulkBuildMinItems(),
        };
    }

    fn hbcBulkIngestBulkBuildMinItems() usize {
        const cached = hbc_bulk_ingest_bulk_build_min_items_cache.load(.monotonic);
        if (cached != 0) return cached - 1;
        const value = stressEnvUsize("ANTFLY_HBC_BULK_INGEST_BULK_BUILD_MIN_ITEMS", 1024);
        hbc_bulk_ingest_bulk_build_min_items_cache.store(value +% 1, .monotonic);
        return value;
    }

    fn hbcBulkRebuildLeafMinMembers() usize {
        const cached = hbc_bulk_rebuild_leaf_min_members_cache.load(.monotonic);
        if (cached != 0) return cached - 1;
        const value = stressEnvUsize("ANTFLY_HBC_BULK_REBUILD_LEAF_MIN_MEMBERS", 0);
        hbc_bulk_rebuild_leaf_min_members_cache.store(value +% 1, .monotonic);
        return value;
    }

    fn hbcDeferBulkLeafSplitsEnabled() bool {
        const cached = hbc_defer_bulk_leaf_splits_cache.load(.monotonic);
        if (cached != 0) return cached == 2;
        if (comptime builtin.os.tag == .freestanding) {
            hbc_defer_bulk_leaf_splits_cache.store(2, .monotonic);
            return true;
        }
        const raw_z = getenv("ANTFLY_HBC_DEFER_BULK_LEAF_SPLITS") orelse {
            hbc_defer_bulk_leaf_splits_cache.store(2, .monotonic);
            return true;
        };
        const raw = std.mem.span(raw_z);
        const enabled = !(std.mem.eql(u8, raw, "0") or
            std.ascii.eqlIgnoreCase(raw, "false") or
            std.ascii.eqlIgnoreCase(raw, "no"));
        hbc_defer_bulk_leaf_splits_cache.store(if (enabled) 2 else 1, .monotonic);
        return enabled;
    }

    fn hbcDeferBulkQuantizedRebuildEnabled() bool {
        const cached = hbc_defer_bulk_quantized_rebuild_cache.load(.monotonic);
        if (cached != 0) return cached == 2;
        if (comptime builtin.os.tag == .freestanding) {
            hbc_defer_bulk_quantized_rebuild_cache.store(1, .monotonic);
            return false;
        }
        const raw_z = getenv("ANTFLY_HBC_DEFER_BULK_QUANTIZED_REBUILD") orelse {
            hbc_defer_bulk_quantized_rebuild_cache.store(1, .monotonic);
            return false;
        };
        const raw = std.mem.span(raw_z);
        const enabled = !(std.mem.eql(u8, raw, "0") or
            std.ascii.eqlIgnoreCase(raw, "false") or
            std.ascii.eqlIgnoreCase(raw, "no"));
        hbc_defer_bulk_quantized_rebuild_cache.store(if (enabled) 2 else 1, .monotonic);
        return enabled;
    }

    fn hbcCoalesceBulkWritesEnabled() bool {
        const cached = hbc_coalesce_bulk_writes_cache.load(.monotonic);
        if (cached != 0) return cached == 2;
        if (comptime builtin.os.tag == .freestanding) {
            hbc_coalesce_bulk_writes_cache.store(2, .monotonic);
            return true;
        }
        const raw_z = getenv("ANTFLY_HBC_COALESCE_BULK_WRITES") orelse {
            hbc_coalesce_bulk_writes_cache.store(2, .monotonic);
            return true;
        };
        const raw = std.mem.span(raw_z);
        const enabled = !(std.mem.eql(u8, raw, "0") or
            std.ascii.eqlIgnoreCase(raw, "false") or
            std.ascii.eqlIgnoreCase(raw, "no"));
        hbc_coalesce_bulk_writes_cache.store(if (enabled) 2 else 1, .monotonic);
        return enabled;
    }

    fn benchMetricsEnabled() bool {
        const cached = bench_hbc_metrics_cache.load(.monotonic);
        if (cached != 0) return cached == 2;
        if (comptime builtin.os.tag == .freestanding) {
            bench_hbc_metrics_cache.store(1, .monotonic);
            return false;
        }
        const raw_z = getenv("ANTFLY_BENCH_METRICS") orelse
            getenv("ANTFLY_BENCH_HBC_WRITE_PROFILE") orelse {
            bench_hbc_metrics_cache.store(1, .monotonic);
            return false;
        };
        const raw = std.mem.span(raw_z);
        const enabled = !(std.mem.eql(u8, raw, "0") or
            std.ascii.eqlIgnoreCase(raw, "false") or
            std.ascii.eqlIgnoreCase(raw, "no"));
        bench_hbc_metrics_cache.store(if (enabled) 2 else 1, .monotonic);
        return enabled;
    }

    fn sparseReplayProfileEnabled() bool {
        const cached = sparse_replay_profile_enabled_cache.load(.monotonic);
        if (cached != 0) return cached == 2;
        if (comptime builtin.os.tag == .freestanding) {
            sparse_replay_profile_enabled_cache.store(1, .monotonic);
            return false;
        }
        const raw_z = getenv("ANTFLY_BENCH_SPARSE_REPLAY_PROFILE") orelse
            getenv("ANTFLY_BENCH_METRICS") orelse {
            sparse_replay_profile_enabled_cache.store(1, .monotonic);
            return false;
        };
        const raw = std.mem.span(raw_z);
        const enabled = !(std.mem.eql(u8, raw, "0") or
            std.ascii.eqlIgnoreCase(raw, "false") or
            std.ascii.eqlIgnoreCase(raw, "no"));
        sparse_replay_profile_enabled_cache.store(if (enabled) 2 else 1, .monotonic);
        return enabled;
    }

    fn openProfileEnabled() bool {
        const cached = open_profile_enabled_cache.load(.monotonic);
        if (cached != 0) return cached == 2;
        if (comptime builtin.os.tag == .freestanding) {
            open_profile_enabled_cache.store(1, .monotonic);
            return false;
        }
        const raw_z = getenv("ANTFLY_DB_OPEN_PROFILE") orelse
            getenv("ANTFLY_BENCH_METRICS") orelse {
            open_profile_enabled_cache.store(1, .monotonic);
            return false;
        };
        const raw = std.mem.span(raw_z);
        const enabled = !(std.mem.eql(u8, raw, "0") or
            std.ascii.eqlIgnoreCase(raw, "false") or
            std.ascii.eqlIgnoreCase(raw, "no"));
        open_profile_enabled_cache.store(if (enabled) 2 else 1, .monotonic);
        return enabled;
    }

    fn logOpenIndexProfile(profile: OpenIndexProfile) void {
        std.log.info("index_open_profile kind={s} name={s} open_ns={} backfill_ns={} total_ns={}", .{
            @tagName(profile.kind),
            profile.name,
            profile.open_ns,
            profile.backfill_ns,
            profile.totalNs(),
        });
    }

    fn nsToMs(ns: u64) u64 {
        return ns / std.time.ns_per_ms;
    }

    fn deltaU64(after: u64, before: u64) u64 {
        return after -| before;
    }

    const HbcTreeProfile = struct {
        total_nodes: usize = 0,
        leaf_nodes: usize = 0,
        internal_nodes: usize = 0,
        max_level: u16 = 0,
        leaf_members_total: usize = 0,
        leaf_members_min: usize = 0,
        leaf_members_p50: usize = 0,
        leaf_members_p95: usize = 0,
        leaf_members_p99: usize = 0,
        leaf_members_max: usize = 0,
    };

    fn deinitDebugNodes(alloc: Allocator, nodes: []hbc_mod.HBCDebugNode) void {
        for (nodes) |*node| node.deinit(alloc);
        alloc.free(nodes);
    }

    fn percentileSorted(values: []const usize, pct: usize) usize {
        if (values.len == 0) return 0;
        const idx = ((values.len - 1) * pct) / 100;
        return values[idx];
    }

    fn collectHbcTreeProfile(alloc: Allocator, entry: *DenseIndex) !HbcTreeProfile {
        const nodes = try entry.index.debugDumpNodes(alloc);
        defer deinitDebugNodes(alloc, nodes);

        var leaf_sizes = std.ArrayListUnmanaged(usize).empty;
        defer leaf_sizes.deinit(alloc);

        var profile: HbcTreeProfile = .{ .total_nodes = nodes.len };
        for (nodes) |node| {
            profile.max_level = @max(profile.max_level, node.level);
            if (node.is_leaf) {
                profile.leaf_nodes += 1;
                profile.leaf_members_total += node.members.len;
                try leaf_sizes.append(alloc, node.members.len);
            } else {
                profile.internal_nodes += 1;
            }
        }

        if (leaf_sizes.items.len > 0) {
            std.mem.sort(usize, leaf_sizes.items, {}, struct {
                fn lessThan(_: void, a: usize, b: usize) bool {
                    return a < b;
                }
            }.lessThan);
            profile.leaf_members_min = leaf_sizes.items[0];
            profile.leaf_members_p50 = percentileSorted(leaf_sizes.items, 50);
            profile.leaf_members_p95 = percentileSorted(leaf_sizes.items, 95);
            profile.leaf_members_p99 = percentileSorted(leaf_sizes.items, 99);
            profile.leaf_members_max = leaf_sizes.items[leaf_sizes.items.len - 1];
        }

        return profile;
    }

    fn benchHbcTreeEvery() ?u64 {
        if (comptime builtin.os.tag == .freestanding) return null;

        const raw_z = getenv("ANTFLY_BENCH_HBC_TREE_EVERY") orelse {
            const enabled_z = getenv("ANTFLY_BENCH_HBC_TREE") orelse return null;
            const enabled = std.mem.span(enabled_z);
            if (std.mem.eql(u8, enabled, "0") or
                std.ascii.eqlIgnoreCase(enabled, "false") or
                std.ascii.eqlIgnoreCase(enabled, "no"))
            {
                return null;
            }
            return 100;
        };
        const raw = std.mem.span(raw_z);
        if (raw.len == 0) return null;
        return std.fmt.parseUnsigned(u64, raw, 10) catch null;
    }

    fn shouldLogBenchHbcTree() bool {
        const every = benchHbcTreeEvery() orelse return false;
        if (every == 0) return false;
        const current = bench_hbc_tree_counter.fetchAdd(1, .monotonic) + 1;
        return current % every == 0;
    }

    fn logBenchHbcTree(alloc: Allocator, phase: []const u8, entry: *DenseIndex) void {
        if (!shouldLogBenchHbcTree()) return;
        const profile = collectHbcTreeProfile(alloc, entry) catch |err| {
            std.log.warn("antfly_bench_hbc_tree_failed phase={s} index={s} err={s}", .{ phase, entry.config.name, @errorName(err) });
            return;
        };
        const avg_leaf_members = if (profile.leaf_nodes == 0) 0 else profile.leaf_members_total / profile.leaf_nodes;
        std.log.info(
            "antfly_bench_hbc_tree phase={s} index={s} total_nodes={d} leaf_nodes={d} internal_nodes={d} max_level={d} leaf_members_total={d} leaf_members_avg={d} leaf_members_min={d} leaf_members_p50={d} leaf_members_p95={d} leaf_members_p99={d} leaf_members_max={d}",
            .{
                phase,
                entry.config.name,
                profile.total_nodes,
                profile.leaf_nodes,
                profile.internal_nodes,
                profile.max_level,
                profile.leaf_members_total,
                avg_leaf_members,
                profile.leaf_members_min,
                profile.leaf_members_p50,
                profile.leaf_members_p95,
                profile.leaf_members_p99,
                profile.leaf_members_max,
            },
        );
    }

    fn logBenchHbcWrite(
        alloc: Allocator,
        phase: []const u8,
        entry: *DenseIndex,
        item_count: usize,
        batch_options: StoreBatchOptions,
        all_vector_ids_new: bool,
        before_stats: hbc_mod.IndexStats,
        before_profile: hbc_mod.WriteProfile,
        before_lsm_stats: ?hbc_mod.LsmWriteStats,
        started_ns: u64,
    ) void {
        if (!benchMetricsEnabled()) return;
        const after_stats = entry.index.stats();
        const after_profile = entry.index.getWriteProfile();
        const after_lsm_stats = entry.index.snapshotLsmWriteStats();
        const after_lsm_maintenance = entry.index.snapshotLsmMaintenanceStats();
        const hbc_options = denseHbcBatchOptions(batch_options, all_vector_ids_new, entry.index.hasExternalVectorLoader());
        std.log.info(
            "antfly_bench_hbc_write phase={s} index={s} items={d} mode={s} all_new={any} assume_absent={any} coalesce={any} defer_quantized={any} wall_ms={d} active_before={d} active_after={d} nodes_before={d} nodes_after={d}",
            .{
                phase,
                entry.config.name,
                item_count,
                @tagName(batch_options.mode),
                all_vector_ids_new,
                hbc_options.assume_absent_ids,
                hbc_options.coalesce_leaf_writes,
                hbc_options.defer_quantized_rebuild,
                nsToMs(platform_time.monotonicNs() - started_ns),
                before_stats.active_count,
                after_stats.active_count,
                before_stats.node_count,
                after_stats.node_count,
            },
        );
        std.log.info(
            "antfly_bench_hbc_write_counts phase={s} index={s} insert_calls={d} noop_existing_skips={d} grouped_items={d} grouped_fallback_items={d} grouped_leaf_groups={d} grouped_split_candidates={d} grouped_recursive_splits={d} grouped_leaf_range_writes={d} grouped_ancestor_refreshes={d} grouped_ancestor_nodes={d} grouped_node_body_writes={d} grouped_vec_leaf_writes={d} save_node_calls={d} update_parent_calls={d} split_leaf_calls={d} split_internal_calls={d} range_put_calls={d} range_delete_calls={d} vecs_put={d} vecs_append={d} nodes_put={d} nodes_append={d} quant_put={d} quant_append={d}",
            .{
                phase,
                entry.config.name,
                deltaU64(after_profile.insert_calls, before_profile.insert_calls),
                deltaU64(after_profile.noop_existing_skips, before_profile.noop_existing_skips),
                deltaU64(after_profile.grouped_items, before_profile.grouped_items),
                deltaU64(after_profile.grouped_fallback_items, before_profile.grouped_fallback_items),
                deltaU64(after_profile.grouped_leaf_groups, before_profile.grouped_leaf_groups),
                deltaU64(after_profile.grouped_split_candidates, before_profile.grouped_split_candidates),
                deltaU64(after_profile.grouped_recursive_splits, before_profile.grouped_recursive_splits),
                deltaU64(after_profile.grouped_leaf_range_writes, before_profile.grouped_leaf_range_writes),
                deltaU64(after_profile.grouped_ancestor_range_refreshes, before_profile.grouped_ancestor_range_refreshes),
                deltaU64(after_profile.grouped_ancestor_range_nodes, before_profile.grouped_ancestor_range_nodes),
                deltaU64(after_profile.grouped_node_body_writes, before_profile.grouped_node_body_writes),
                deltaU64(after_profile.grouped_vec_leaf_writes, before_profile.grouped_vec_leaf_writes),
                deltaU64(after_profile.save_node_calls, before_profile.save_node_calls),
                deltaU64(after_profile.update_parent_calls, before_profile.update_parent_calls),
                deltaU64(after_profile.split_leaf_calls, before_profile.split_leaf_calls),
                deltaU64(after_profile.split_internal_calls, before_profile.split_internal_calls),
                deltaU64(after_profile.range_put_calls, before_profile.range_put_calls),
                deltaU64(after_profile.range_delete_calls, before_profile.range_delete_calls),
                deltaU64(after_profile.ns_vecs_put_calls, before_profile.ns_vecs_put_calls),
                deltaU64(after_profile.ns_vecs_append_calls, before_profile.ns_vecs_append_calls),
                deltaU64(after_profile.ns_nodes_put_calls, before_profile.ns_nodes_put_calls),
                deltaU64(after_profile.ns_nodes_append_calls, before_profile.ns_nodes_append_calls),
                deltaU64(after_profile.ns_quant_put_calls, before_profile.ns_quant_put_calls),
                deltaU64(after_profile.ns_quant_append_calls, before_profile.ns_quant_append_calls),
            },
        );
        std.log.info(
            "antfly_bench_hbc_write_route phase={s} index={s} batch_route_calls={d} batch_route_internal_nodes={d} batch_route_leaf_groups={d} batch_route_items={d} batch_route_quantized_nodes={d} batch_route_exact_child_scores={d} batch_route_fallback_nodes={d}",
            .{
                phase,
                entry.config.name,
                deltaU64(after_profile.batch_route_calls, before_profile.batch_route_calls),
                deltaU64(after_profile.batch_route_internal_nodes, before_profile.batch_route_internal_nodes),
                deltaU64(after_profile.batch_route_leaf_groups, before_profile.batch_route_leaf_groups),
                deltaU64(after_profile.batch_route_items, before_profile.batch_route_items),
                deltaU64(after_profile.batch_route_quantized_nodes, before_profile.batch_route_quantized_nodes),
                deltaU64(after_profile.batch_route_exact_child_scores, before_profile.batch_route_exact_child_scores),
                deltaU64(after_profile.batch_route_fallback_nodes, before_profile.batch_route_fallback_nodes),
            },
        );
        std.log.info(
            "antfly_bench_hbc_write_timing phase={s} index={s} transform_ms={d} find_leaf_ms={d} store_vector_ms={d} mutate_leaf_ms={d} refresh_quantized_ms={d} quantized_vector_load_ms={d} quantized_compute_ms={d} quantized_store_ms={d} quantized_encode_ms={d} quantized_put_ms={d} save_node_ms={d} save_split_range_ms={d} update_parent_ms={d} split_leaf_ms={d} split_internal_ms={d} commit_ms={d} flush_metadata_ms={d} bulk_build_store_ms={d} bulk_build_tree_ms={d}",
            .{
                phase,
                entry.config.name,
                nsToMs(deltaU64(after_profile.insert_transform_ns, before_profile.insert_transform_ns)),
                nsToMs(deltaU64(after_profile.insert_find_leaf_ns, before_profile.insert_find_leaf_ns)),
                nsToMs(deltaU64(after_profile.insert_store_vector_ns, before_profile.insert_store_vector_ns)),
                nsToMs(deltaU64(after_profile.insert_mutate_leaf_ns, before_profile.insert_mutate_leaf_ns)),
                nsToMs(deltaU64(after_profile.refresh_quantized_ns, before_profile.refresh_quantized_ns)),
                nsToMs(deltaU64(after_profile.quantized_vector_load_ns, before_profile.quantized_vector_load_ns)),
                nsToMs(deltaU64(after_profile.quantized_compute_ns, before_profile.quantized_compute_ns)),
                nsToMs(deltaU64(after_profile.quantized_store_ns, before_profile.quantized_store_ns)),
                nsToMs(deltaU64(after_profile.quantized_encode_ns, before_profile.quantized_encode_ns)),
                nsToMs(deltaU64(after_profile.quantized_put_ns, before_profile.quantized_put_ns)),
                nsToMs(deltaU64(after_profile.save_node_ns, before_profile.save_node_ns)),
                nsToMs(deltaU64(after_profile.save_split_range_ns, before_profile.save_split_range_ns)),
                nsToMs(deltaU64(after_profile.update_parent_ns, before_profile.update_parent_ns)),
                nsToMs(deltaU64(after_profile.split_leaf_ns, before_profile.split_leaf_ns)),
                nsToMs(deltaU64(after_profile.split_internal_ns, before_profile.split_internal_ns)),
                nsToMs(deltaU64(after_profile.insert_commit_ns, before_profile.insert_commit_ns)),
                nsToMs(deltaU64(after_profile.insert_flush_metadata_ns, before_profile.insert_flush_metadata_ns)),
                nsToMs(deltaU64(after_profile.bulk_build_store_ns, before_profile.bulk_build_store_ns)),
                nsToMs(deltaU64(after_profile.bulk_build_tree_ns, before_profile.bulk_build_tree_ns)),
            },
        );
        if (before_lsm_stats) |before_lsm| {
            if (after_lsm_stats) |after_lsm| {
                std.log.info(
                    "antfly_bench_hbc_lsm_write phase={s} index={s} flushes={d} flush_entries={d} flush_runs={d} flush_bytes={d} flush_ms={d} sorted_ingest_runs={d} sorted_ingest_bytes={d} sorted_ingest_ms={d} compaction_ms={d} write_pressure_compactions={d} write_pressure_ms={d} manifest_writes={d} manifest_bytes={d} manifest_ms={d} table_writes={d} table_bytes={d} table_logical_entry_bytes={d} table_physical_entry_bytes={d} table_compressed_blocks={d} table_raw_blocks={d} table_compression_codec_mask={d}",
                    .{
                        phase,
                        entry.config.name,
                        deltaU64(after_lsm.flushes, before_lsm.flushes),
                        deltaU64(after_lsm.flush_input_entries, before_lsm.flush_input_entries),
                        deltaU64(after_lsm.flush_output_runs, before_lsm.flush_output_runs),
                        deltaU64(after_lsm.flush_output_bytes, before_lsm.flush_output_bytes),
                        nsToMs(deltaU64(after_lsm.flush_ns, before_lsm.flush_ns)),
                        deltaU64(after_lsm.sorted_ingest_runs, before_lsm.sorted_ingest_runs),
                        deltaU64(after_lsm.sorted_ingest_bytes, before_lsm.sorted_ingest_bytes),
                        nsToMs(deltaU64(after_lsm.sorted_ingest_ns, before_lsm.sorted_ingest_ns)),
                        nsToMs(deltaU64(after_lsm.compaction_ns, before_lsm.compaction_ns)),
                        deltaU64(after_lsm.write_pressure_compactions, before_lsm.write_pressure_compactions),
                        nsToMs(deltaU64(after_lsm.write_pressure_ns, before_lsm.write_pressure_ns)),
                        deltaU64(after_lsm.manifest_writes, before_lsm.manifest_writes),
                        deltaU64(after_lsm.manifest_bytes, before_lsm.manifest_bytes),
                        nsToMs(deltaU64(after_lsm.manifest_ns, before_lsm.manifest_ns)),
                        deltaU64(after_lsm.table_file_writes, before_lsm.table_file_writes),
                        deltaU64(after_lsm.table_file_bytes, before_lsm.table_file_bytes),
                        deltaU64(after_lsm.table_file_logical_entry_bytes, before_lsm.table_file_logical_entry_bytes),
                        deltaU64(after_lsm.table_file_physical_entry_bytes, before_lsm.table_file_physical_entry_bytes),
                        deltaU64(after_lsm.table_file_compressed_blocks, before_lsm.table_file_compressed_blocks),
                        deltaU64(after_lsm.table_file_raw_blocks, before_lsm.table_file_raw_blocks),
                        after_lsm.table_file_compression_codec_mask,
                    },
                );
            }
        }
        if (after_lsm_maintenance) |maintenance| {
            std.log.info(
                "antfly_bench_hbc_lsm_maintenance phase={s} index={s} mutable_entries={d} mutable_bytes={d} total_runs={d} total_run_bytes={d} total_run_logical_entry_bytes={d} total_run_physical_entry_bytes={d} total_run_compressed_blocks={d} total_run_raw_blocks={d} total_run_compression_codec_mask={d} l0_runs={d} l0_bytes={d} overlapping_l0_runs={d} lower_level_runs={d} lower_level_bytes={d} max_level={d} compactable_l0_runs={d} soft_limit_l0_runs={d} hard_limit_l0_runs={d} soft_limit_l0_bytes={d} hard_limit_l0_bytes={d} level_overflow_runs={d} level_overflow_bytes={d} obsolete_paths={d} active_readers={d} active_bulk_ingest_batches={d} manifest_dirty={any} obsolete_manifest_dirty={any}",
                .{
                    phase,
                    entry.config.name,
                    maintenance.mutable_entries,
                    maintenance.mutable_bytes,
                    maintenance.total_runs,
                    maintenance.total_run_bytes,
                    maintenance.total_run_logical_entry_bytes,
                    maintenance.total_run_physical_entry_bytes,
                    maintenance.total_run_compressed_blocks,
                    maintenance.total_run_raw_blocks,
                    maintenance.total_run_compression_codec_mask,
                    maintenance.l0_runs,
                    maintenance.l0_bytes,
                    maintenance.overlapping_l0_runs,
                    maintenance.lower_level_runs,
                    maintenance.lower_level_bytes,
                    maintenance.max_level,
                    maintenance.compactable_l0_runs,
                    maintenance.soft_limit_l0_runs,
                    maintenance.hard_limit_l0_runs,
                    maintenance.soft_limit_l0_bytes,
                    maintenance.hard_limit_l0_bytes,
                    maintenance.level_overflow_runs,
                    maintenance.level_overflow_bytes,
                    maintenance.obsolete_paths,
                    maintenance.active_readers,
                    maintenance.active_bulk_ingest_batches,
                    maintenance.manifest_dirty,
                    maintenance.obsolete_manifest_dirty,
                },
            );
            std.log.info(
                "antfly_bench_hbc_lsm_scheduler phase={s} index={s} active_jobs={d} in_flight_input_bytes={d} grants={d} completions={d} denied_capacity={d} denied_resource_pressure={d} oversized_grants={d} remembered_pending={d} remembered_candidates={d} remembered_retries={d} remembered_hits={d} remembered_stale={d} conflict_denials={d}",
                .{
                    phase,
                    entry.config.name,
                    maintenance.compaction_scheduler_active_jobs,
                    maintenance.compaction_scheduler_in_flight_input_bytes,
                    maintenance.compaction_scheduler_grants,
                    maintenance.compaction_scheduler_completions,
                    maintenance.compaction_scheduler_denied_capacity,
                    maintenance.compaction_scheduler_denied_resource_pressure,
                    maintenance.compaction_scheduler_oversized_grants,
                    maintenance.compaction_scheduler_remembered_pending,
                    maintenance.compaction_scheduler_remembered_candidates,
                    maintenance.compaction_scheduler_remembered_retries,
                    maintenance.compaction_scheduler_remembered_hits,
                    maintenance.compaction_scheduler_remembered_stale,
                    maintenance.compaction_scheduler_conflict_denials,
                },
            );
        }
        logBenchHbcTree(alloc, phase, entry);
    }

    fn loadDenseEmbeddingArtifactVector(self: *IndexManager, store: anytype, artifact_key: []const u8) ![]f32 {
        var runtime_store = try initRuntimeStore(self.alloc, store);
        defer runtime_store.deinit();
        var txn = try runtime_store.store.beginProbe();
        defer txn.abort();
        return try self.loadDenseEmbeddingArtifactVectorTxn(&txn, artifact_key);
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

    fn loadDenseEmbeddingArtifactVectorTxn(self: *IndexManager, txn: anytype, artifact_key: []const u8) ![]f32 {
        const raw = try txn.get(artifact_key);
        return try enrichment_artifact_codec.decodeDenseEmbeddingAlloc(self.alloc, raw);
    }

    fn loadSparseEmbeddingArtifactTxn(self: *IndexManager, txn: anytype, artifact_key: []const u8) !enrichment_artifact_codec.SparseEmbedding {
        const raw = try txn.get(artifact_key);
        return try enrichment_artifact_codec.decodeSparseEmbeddingAlloc(self.alloc, raw);
    }

    fn writeDenseEmbeddingArtifactTxn(
        self: *IndexManager,
        txn: anytype,
        base_key: []const u8,
        parent_doc_key: []const u8,
        artifact_name: []const u8,
        source_field: []const u8,
        source_key: ?[]const u8,
        vector: []const f32,
    ) !void {
        _ = parent_doc_key;
        _ = source_field;
        _ = source_key;
        const artifact_key = if (internal_keys.isInternalUserKey(base_key))
            try internal_keys.derivedEmbeddingArtifactKeyAlloc(self.alloc, base_key, artifact_name)
        else
            try internal_keys.embeddingArtifactKeyForDocumentAlloc(self.alloc, base_key, artifact_name);
        defer self.alloc.free(artifact_key);
        const payload = try enrichment_artifact_codec.encodeDenseEmbeddingAlloc(self.alloc, null, vector);
        defer self.alloc.free(payload);
        try txn.put(artifact_key, payload);
    }

    fn loadDenseVectorForHbc(ctx: *anyopaque, alloc: Allocator, vector_id: u64, metadata: []const u8) ![]f32 {
        const loader: *DenseVectorLoadContext = @ptrCast(@alignCast(ctx));
        const manager = loader.manager;
        const store = manager.primary_store orelse return error.NotFound;
        const entry = manager.denseIndex(loader.index_name) orelse return error.IndexNotFound;
        const load_session = blk: {
            const session = active_dense_vector_load_session orelse break :blk null;
            if (session.context != loader) break :blk null;
            break :blk session;
        };
        if (load_session) |session| {
            if (session.getVector(vector_id)) |cached| return try alloc.dupe(f32, cached);
        }

        const vector = blk: {
            if (entry.embedding_name) |embedding_name| {
                break :blk try manager.loadDenseVectorArtifactForHbc(alloc, store, metadata, embedding_name, load_session);
            }
            if (entry.external) {
                break :blk try manager.loadDenseVectorArtifactForHbc(alloc, store, metadata, entry.config.name, load_session);
            }

            const doc_store_key = try internal_keys.documentKeyAlloc(alloc, metadata);
            defer alloc.free(doc_store_key);
            const raw = manager.loadPrimaryDocumentRawForHbc(store, doc_store_key, load_session, alloc) catch |err| switch (err) {
                error.NotFound => break :blk try manager.loadDenseVectorArtifactForHbc(alloc, store, metadata, entry.config.name, load_session),
                else => return err,
            };
            defer if (load_session == null) alloc.free(raw);
            break :blk (try mapper.extractDenseVectorField(alloc, raw, entry.field_name, entry.dims)) orelse
                try manager.loadDenseVectorArtifactForHbc(alloc, store, metadata, entry.config.name, load_session);
        };
        errdefer alloc.free(vector);
        if (load_session) |session| try session.cacheVector(vector_id, vector);
        return vector;
    }

    fn loadDenseVectorForHbcIntoScratch(ctx: *anyopaque, vector_id: u64, metadata: []const u8, scratch: []f32) ![]const f32 {
        const loader: *DenseVectorLoadContext = @ptrCast(@alignCast(ctx));
        const manager = loader.manager;
        const store = manager.primary_store orelse return error.NotFound;
        const entry = manager.denseIndex(loader.index_name) orelse return error.IndexNotFound;
        const load_session = blk: {
            const session = active_dense_vector_load_session orelse break :blk null;
            if (session.context != loader) break :blk null;
            break :blk session;
        };
        if (load_session) |session| {
            if (session.getVector(vector_id)) |cached| return cached;
        }

        if (entry.embedding_name) |embedding_name| {
            const vector = try manager.loadDenseVectorArtifactForHbcIntoScratch(store, metadata, embedding_name, load_session, scratch);
            if (load_session) |session| try session.cacheVector(vector_id, vector);
            return vector;
        }
        if (entry.external) {
            const vector = try manager.loadDenseVectorArtifactForHbcIntoScratch(store, metadata, entry.config.name, load_session, scratch);
            if (load_session) |session| try session.cacheVector(vector_id, vector);
            return vector;
        }

        const vector = try loadDenseVectorForHbc(ctx, manager.alloc, vector_id, metadata);
        defer manager.alloc.free(vector);
        if (vector.len > scratch.len) return error.BufferTooSmall;
        @memcpy(scratch[0..vector.len], vector);
        return scratch[0..vector.len];
    }

    const DenseArtifactReadKey = struct {
        key: []const u8,
        position: usize,

        fn lessThan(_: void, lhs: @This(), rhs: @This()) bool {
            return std.mem.order(u8, lhs.key, rhs.key) == .lt;
        }
    };

    fn loadDenseVectorsForHbcBatch(
        ctx: *anyopaque,
        vector_ids: []const u64,
        metadata: []const ?[]const u8,
        vector_views: [][]const f32,
        batch_scratch: []f32,
        dims: usize,
    ) !void {
        const loader: *DenseVectorLoadContext = @ptrCast(@alignCast(ctx));
        const manager = loader.manager;
        const store = manager.primary_store orelse return error.NotFound;
        const entry = manager.denseIndex(loader.index_name) orelse return error.IndexNotFound;
        if (vector_ids.len != metadata.len or vector_ids.len != vector_views.len) return error.InvalidArgument;
        if (dims == 0) return error.InvalidVectorDimensions;
        const scratch_floats = std.math.mul(usize, dims, vector_ids.len) catch return error.BufferTooSmall;
        if (batch_scratch.len < scratch_floats) return error.BufferTooSmall;

        const artifact_name = entry.embedding_name orelse blk: {
            if (!entry.external) return error.Unsupported;
            break :blk entry.config.name;
        };
        const load_session = blk: {
            const session = active_dense_vector_load_session orelse break :blk null;
            if (session.context != loader) break :blk null;
            break :blk session;
        };

        const artifact_reads = try manager.alloc.alloc(DenseArtifactReadKey, vector_ids.len);
        var key_count: usize = 0;
        defer {
            for (artifact_reads[0..key_count]) |artifact_read| manager.alloc.free(artifact_read.key);
            manager.alloc.free(artifact_reads);
        }

        for (metadata, 0..) |maybe_doc_key, i| {
            if (load_session) |session| {
                if (session.getVector(vector_ids[i])) |cached| {
                    if (cached.len != dims) return error.InvalidVectorDimensions;
                    vector_views[i] = cached;
                    continue;
                }
            }
            if (entry.index.borrowCachedVector(vector_ids[i])) |cached_handle| {
                var handle = cached_handle;
                defer handle.deinit();
                const cached = handle.view();
                if (cached.len != dims) return error.InvalidVectorDimensions;
                const scratch = batch_scratch[i * dims ..][0..dims];
                @memcpy(scratch, cached);
                vector_views[i] = scratch;
                continue;
            }
            const doc_key = maybe_doc_key orelse continue;
            const artifact_key = if (internal_keys.isInternalUserKey(doc_key))
                try internal_keys.derivedEmbeddingArtifactKeyAlloc(manager.alloc, doc_key, artifact_name)
            else
                try internal_keys.embeddingArtifactKeyForDocumentAlloc(manager.alloc, doc_key, artifact_name);
            artifact_reads[key_count] = .{ .key = artifact_key, .position = i };
            key_count += 1;
        }
        if (key_count == 0) return;
        std.mem.sort(DenseArtifactReadKey, artifact_reads[0..key_count], {}, DenseArtifactReadKey.lessThan);

        const artifact_keys = try manager.alloc.alloc([]const u8, key_count);
        defer manager.alloc.free(artifact_keys);
        for (artifact_reads[0..key_count], 0..) |artifact_read, i| artifact_keys[i] = artifact_read.key;

        const raw_values = try manager.alloc.alloc(?[]const u8, key_count);
        defer manager.alloc.free(raw_values);
        if (load_session) |session| {
            try session.getManySorted(store, artifact_keys, raw_values);
        } else {
            var runtime_store = try initRuntimeStore(manager.alloc, store);
            defer runtime_store.deinit();
            var txn = try runtime_store.store.beginRead();
            defer txn.abort();
            try txn.getManySorted(artifact_keys, raw_values);
        }

        for (raw_values, 0..) |maybe_raw, key_index| {
            const slot = artifact_reads[key_index].position;
            const raw = maybe_raw orelse continue;
            const scratch = batch_scratch[slot * dims ..][0..dims];
            const vector = enrichment_artifact_codec.decodeDenseEmbeddingViewOrInto(raw, scratch) catch |err| {
                if (isRecoverableEmbeddingArtifactError(err)) continue;
                return err;
            };
            if (vector.len != dims) return error.InvalidVectorDimensions;
            vector_views[slot] = vector;
            if (load_session) |session| try session.cacheVector(vector_ids[slot], vector);
        }
    }

    fn loadDenseVectorsForHbcBatchIntoTransformedMatrix(
        ctx: *anyopaque,
        vector_ids: []const u64,
        metadata: []const ?[]const u8,
        matrix_positions: []const usize,
        matrix: []f32,
        scratch: []f32,
        dims: usize,
        index: *hbc_mod.HBCIndex,
        transform: hbc_mod.HBCIndex.ExternalVectorTransformFn,
    ) !void {
        const loader: *DenseVectorLoadContext = @ptrCast(@alignCast(ctx));
        const manager = loader.manager;
        const store = manager.primary_store orelse return error.NotFound;
        const entry = manager.denseIndex(loader.index_name) orelse return error.IndexNotFound;
        if (vector_ids.len != metadata.len or vector_ids.len != matrix_positions.len) return error.InvalidArgument;
        if (dims == 0) return error.InvalidVectorDimensions;
        if (scratch.len < dims) return error.BufferTooSmall;

        const artifact_name = entry.embedding_name orelse blk: {
            if (!entry.external) return error.Unsupported;
            break :blk entry.config.name;
        };
        const load_session = blk: {
            const session = active_dense_vector_load_session orelse break :blk null;
            if (session.context != loader) break :blk null;
            break :blk session;
        };

        const artifact_reads = try manager.alloc.alloc(DenseArtifactReadKey, vector_ids.len);
        var key_count: usize = 0;
        defer {
            for (artifact_reads[0..key_count]) |artifact_read| manager.alloc.free(artifact_read.key);
            manager.alloc.free(artifact_reads);
        }

        for (metadata, 0..) |maybe_doc_key, i| {
            const matrix_pos = matrix_positions[i];
            const matrix_start = std.math.mul(usize, matrix_pos, dims) catch return error.BufferTooSmall;
            const matrix_end = std.math.add(usize, matrix_start, dims) catch return error.BufferTooSmall;
            if (matrix_end > matrix.len) return error.BufferTooSmall;
            if (load_session) |session| {
                if (session.getVector(vector_ids[i])) |cached| {
                    if (cached.len != dims) return error.InvalidVectorDimensions;
                    _ = transform(index, cached, matrix[matrix_start..matrix_end]);
                    continue;
                }
            }
            if (entry.index.borrowCachedVector(vector_ids[i])) |cached_handle| {
                var handle = cached_handle;
                defer handle.deinit();
                const cached = handle.view();
                if (cached.len != dims) return error.InvalidVectorDimensions;
                _ = transform(index, cached, matrix[matrix_start..matrix_end]);
                continue;
            }
            const doc_key = maybe_doc_key orelse return error.NotFound;
            const artifact_key = if (internal_keys.isInternalUserKey(doc_key))
                try internal_keys.derivedEmbeddingArtifactKeyAlloc(manager.alloc, doc_key, artifact_name)
            else
                try internal_keys.embeddingArtifactKeyForDocumentAlloc(manager.alloc, doc_key, artifact_name);
            artifact_reads[key_count] = .{ .key = artifact_key, .position = i };
            key_count += 1;
        }
        if (key_count == 0) return;
        std.mem.sort(DenseArtifactReadKey, artifact_reads[0..key_count], {}, DenseArtifactReadKey.lessThan);

        const artifact_keys = try manager.alloc.alloc([]const u8, key_count);
        defer manager.alloc.free(artifact_keys);
        for (artifact_reads[0..key_count], 0..) |artifact_read, i| artifact_keys[i] = artifact_read.key;

        const raw_values = try manager.alloc.alloc(?[]const u8, key_count);
        defer manager.alloc.free(raw_values);
        if (load_session) |session| {
            try session.getManySorted(store, artifact_keys, raw_values);
        } else {
            var runtime_store = try initRuntimeStore(manager.alloc, store);
            defer runtime_store.deinit();
            var txn = try runtime_store.store.beginRead();
            defer txn.abort();
            try txn.getManySorted(artifact_keys, raw_values);
        }

        for (raw_values, 0..) |maybe_raw, key_index| {
            const slot = artifact_reads[key_index].position;
            const raw = maybe_raw orelse return error.NotFound;
            const vector = enrichment_artifact_codec.decodeDenseEmbeddingViewOrInto(raw, scratch) catch |err| {
                if (isRecoverableEmbeddingArtifactError(err)) return error.NotFound;
                return err;
            };
            if (vector.len != dims) return error.InvalidVectorDimensions;
            const matrix_pos = matrix_positions[slot];
            const matrix_start = std.math.mul(usize, matrix_pos, dims) catch return error.BufferTooSmall;
            const matrix_end = std.math.add(usize, matrix_start, dims) catch return error.BufferTooSmall;
            if (matrix_end > matrix.len) return error.BufferTooSmall;
            _ = transform(index, vector, matrix[matrix_start..matrix_end]);
            if (load_session) |session| try session.cacheVector(vector_ids[slot], vector);
        }
    }

    fn scoreDenseVectorsForHbcBatch(
        ctx: *anyopaque,
        vector_ids: []const u64,
        metadata: []const ?[]const u8,
        query: []const f32,
        query_measure: f32,
        metric: vector_mod.DistanceMetric,
        distances: []f32,
        batch_scratch: []f32,
        dims: usize,
        scratch: hbc_mod.HBCIndex.ExternalVectorBatchDistanceScratch,
        profile: ?*hbc_mod.SearchProfile,
    ) !void {
        const loader: *DenseVectorLoadContext = @ptrCast(@alignCast(ctx));
        const manager = loader.manager;
        const store = manager.primary_store orelse return error.NotFound;
        const entry = manager.denseIndex(loader.index_name) orelse return error.IndexNotFound;
        if (vector_ids.len != metadata.len or vector_ids.len != distances.len) return error.InvalidArgument;
        if (dims == 0 or query.len != dims) return error.InvalidVectorDimensions;
        if (batch_scratch.len < dims) return error.BufferTooSmall;
        if (scratch.artifact_keys.len < vector_ids.len) return error.InvalidArgument;
        if (scratch.raw_values.len < vector_ids.len) return error.InvalidArgument;

        const artifact_name = entry.embedding_name orelse blk: {
            if (!entry.external) return error.Unsupported;
            break :blk entry.config.name;
        };
        const load_session = blk: {
            const session = active_dense_vector_load_session orelse break :blk null;
            if (session.context != loader) break :blk null;
            break :blk session;
        };

        const key_start = platform_time.monotonicNs();
        var key_arena = std.heap.ArenaAllocator.init(manager.alloc);
        defer key_arena.deinit();
        const key_alloc = key_arena.allocator();
        const artifact_reads = try key_alloc.alloc(DenseArtifactReadKey, vector_ids.len);
        var key_count: usize = 0;
        for (metadata, 0..) |maybe_doc_key, i| {
            if (load_session) |session| {
                if (session.getVector(vector_ids[i])) |cached| {
                    if (cached.len != dims) return error.InvalidVectorDimensions;
                    const distance_start = platform_time.monotonicNs();
                    distances[i] = exactStoredVectorDistance(query, query_measure, cached, metric);
                    if (profile) |p| {
                        const elapsed = platform_time.monotonicNs() - distance_start;
                        p.rerank_artifact_distance_ns += elapsed;
                        p.rerank_distance_ns += elapsed;
                    }
                    continue;
                }
            }
            if (entry.index.borrowCachedVector(vector_ids[i])) |cached_handle| {
                var handle = cached_handle;
                defer handle.deinit();
                const cached = handle.view();
                if (cached.len != dims) return error.InvalidVectorDimensions;
                const distance_start = platform_time.monotonicNs();
                distances[i] = exactStoredVectorDistance(query, query_measure, cached, metric);
                if (profile) |p| {
                    const elapsed = platform_time.monotonicNs() - distance_start;
                    p.rerank_artifact_distance_ns += elapsed;
                    p.rerank_distance_ns += elapsed;
                }
                continue;
            }
            const doc_key = maybe_doc_key orelse continue;
            const artifact_key = if (internal_keys.isInternalUserKey(doc_key))
                try internal_keys.derivedEmbeddingArtifactKeyAlloc(key_alloc, doc_key, artifact_name)
            else
                try internal_keys.embeddingArtifactKeyForDocumentAlloc(key_alloc, doc_key, artifact_name);
            artifact_reads[key_count] = .{ .key = artifact_key, .position = i };
            key_count += 1;
        }
        if (key_count == 0) return;
        std.mem.sort(DenseArtifactReadKey, artifact_reads[0..key_count], {}, DenseArtifactReadKey.lessThan);
        if (profile) |p| p.rerank_artifact_key_ns += platform_time.monotonicNs() - key_start;

        const artifact_keys = scratch.artifact_keys[0..key_count];
        for (artifact_reads[0..key_count], 0..) |artifact_read, i| artifact_keys[i] = artifact_read.key;

        const raw_values = scratch.raw_values[0..key_count];
        const cache_before = if (manager.lsm_cache) |cache| cache.snapshotStats() else null;
        const read_start = platform_time.monotonicNs();
        if (load_session) |session| {
            try session.getManySorted(store, artifact_keys, raw_values);
        } else {
            var runtime_store = try initRuntimeStore(manager.alloc, store);
            defer runtime_store.deinit();
            var txn = try runtime_store.store.beginRead();
            defer txn.abort();
            try txn.getManySorted(artifact_keys, raw_values);
        }
        if (profile) |p| {
            p.rerank_artifact_read_ns += platform_time.monotonicNs() - read_start;
            if (manager.lsm_cache) |cache| {
                if (cache_before) |before| {
                    const after = cache.snapshotStats();
                    const before_hits = before.run_table_index.hits + before.run_table_block.hits;
                    const after_hits = after.run_table_index.hits + after.run_table_block.hits;
                    const before_misses = before.run_table_index.misses + before.run_table_block.misses;
                    const after_misses = after.run_table_index.misses + after.run_table_block.misses;
                    p.rerank_lsm_cache_hits += after_hits -| before_hits;
                    p.rerank_lsm_cache_misses += after_misses -| before_misses;
                }
            }
        }

        for (raw_values, 0..) |maybe_raw, key_index| {
            const slot = artifact_reads[key_index].position;
            const raw = maybe_raw orelse {
                distances[slot] = std.math.inf(f32);
                continue;
            };
            const vector_scratch = batch_scratch[0..dims];
            const decode_start = platform_time.monotonicNs();
            const vector = enrichment_artifact_codec.decodeDenseEmbeddingViewOrInto(raw, vector_scratch) catch |err| {
                if (isRecoverableEmbeddingArtifactError(err)) {
                    distances[slot] = std.math.inf(f32);
                    continue;
                }
                return err;
            };
            if (profile) |p| p.rerank_artifact_decode_ns += platform_time.monotonicNs() - decode_start;
            if (vector.len != dims) return error.InvalidVectorDimensions;
            if (load_session) |session| try session.cacheVector(vector_ids[slot], vector);
            _ = entry.index.cacheVector(vector_ids[slot], vector) catch {};
            const distance_start = platform_time.monotonicNs();
            distances[slot] = exactStoredVectorDistance(query, query_measure, vector, metric);
            if (profile) |p| {
                const elapsed = platform_time.monotonicNs() - distance_start;
                p.rerank_artifact_distance_ns += elapsed;
                p.rerank_distance_ns += elapsed;
            }
        }
    }

    fn exactStoredVectorDistance(
        query: []const f32,
        query_measure: f32,
        candidate: []const f32,
        metric: vector_mod.DistanceMetric,
    ) f32 {
        return switch (metric) {
            .cosine => if (query_measure == 0) 1.0 else 1.0 - (vector_mod.dot(query, candidate) / query_measure),
            else => vector_mod.distanceToQuery(query, query_measure, candidate, metric),
        };
    }

    fn loadDenseVectorArtifactForHbc(
        self: *IndexManager,
        alloc: Allocator,
        store: *docstore_mod.DocStore,
        doc_key: []const u8,
        artifact_name: []const u8,
        load_session: ?*DenseVectorLoadSession,
    ) ![]f32 {
        const artifact_key = if (internal_keys.isInternalUserKey(doc_key))
            try internal_keys.derivedEmbeddingArtifactKeyAlloc(alloc, doc_key, artifact_name)
        else
            try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, doc_key, artifact_name);
        defer alloc.free(artifact_key);
        return self.loadDenseEmbeddingArtifactVectorWithSession(store, artifact_key, load_session) catch |err| {
            if (isRecoverableEmbeddingArtifactError(err)) return error.NotFound;
            return err;
        };
    }

    fn loadDenseVectorArtifactForHbcIntoScratch(
        self: *IndexManager,
        store: *docstore_mod.DocStore,
        doc_key: []const u8,
        artifact_name: []const u8,
        load_session: ?*DenseVectorLoadSession,
        scratch: []f32,
    ) ![]const f32 {
        const artifact_key = if (internal_keys.isInternalUserKey(doc_key))
            try internal_keys.derivedEmbeddingArtifactKeyAlloc(self.alloc, doc_key, artifact_name)
        else
            try internal_keys.embeddingArtifactKeyForDocumentAlloc(self.alloc, doc_key, artifact_name);
        defer self.alloc.free(artifact_key);
        return self.loadDenseEmbeddingArtifactVectorWithSessionIntoScratch(store, artifact_key, load_session, scratch) catch |err| {
            if (isRecoverableEmbeddingArtifactError(err)) return error.NotFound;
            return err;
        };
    }

    fn loadPrimaryDocumentRawForHbc(
        self: *IndexManager,
        store: *docstore_mod.DocStore,
        doc_store_key: []const u8,
        load_session: ?*DenseVectorLoadSession,
        alloc: Allocator,
    ) ![]const u8 {
        _ = self;
        if (load_session) |session| return try session.get(store, doc_store_key);
        return try store.get(alloc, doc_store_key);
    }

    fn loadDenseEmbeddingArtifactVectorWithSession(
        self: *IndexManager,
        store: *docstore_mod.DocStore,
        artifact_key: []const u8,
        load_session: ?*DenseVectorLoadSession,
    ) ![]f32 {
        if (load_session) |session| {
            const raw = try session.get(store, artifact_key);
            return try enrichment_artifact_codec.decodeDenseEmbeddingAlloc(self.alloc, raw);
        }
        return try self.loadDenseEmbeddingArtifactVector(store, artifact_key);
    }

    fn loadDenseEmbeddingArtifactVectorWithSessionIntoScratch(
        self: *IndexManager,
        store: *docstore_mod.DocStore,
        artifact_key: []const u8,
        load_session: ?*DenseVectorLoadSession,
        scratch: []f32,
    ) ![]const f32 {
        if (load_session) |session| {
            const raw = try session.get(store, artifact_key);
            return try enrichment_artifact_codec.decodeDenseEmbeddingViewOrInto(raw, scratch);
        }

        var runtime_store = try initRuntimeStore(self.alloc, store);
        defer runtime_store.deinit();
        var txn = try runtime_store.store.beginProbe();
        defer txn.abort();
        const raw = try txn.get(artifact_key);
        return try enrichment_artifact_codec.decodeDenseEmbeddingInto(raw, scratch);
    }

    fn applySparseEmbeddingWritesEntry(self: *IndexManager, store: *docstore_mod.DocStore, entry: *SparseIndex, writes: []const mapper.SparseEmbeddingWrite, batch_options: StoreBatchOptions) !void {
        const PendingSparseArtifactLoad = struct {
            doc_key: []const u8,
            artifact_key: []const u8,
        };
        const ReplayProfile = struct {
            scan_ns: u64 = 0,
            sort_ns: u64 = 0,
            artifact_read_ns: u64 = 0,
            artifact_decode_ns: u64 = 0,
            artifact_view_count: u64 = 0,
            artifact_copy_count: u64 = 0,
            delete_batch_ns: u64 = 0,
            sparse_batch_ns: u64 = 0,
            corrupt_delete_ns: u64 = 0,
        };
        const profile_enabled = sparseReplayProfileEnabled();
        const total_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
        var profile: ReplayProfile = .{};
        var sparse_writes = std.ArrayListUnmanaged(sparse_mod.SparseWrite).empty;
        var owned_indices = std.ArrayListUnmanaged([]u32).empty;
        var owned_values = std.ArrayListUnmanaged([]f32).empty;
        var pending_artifact_loads = std.ArrayListUnmanaged(PendingSparseArtifactLoad).empty;
        defer {
            for (owned_indices.items) |indices| self.alloc.free(indices);
            for (owned_values.items) |values| self.alloc.free(values);
            owned_indices.deinit(self.alloc);
            owned_values.deinit(self.alloc);
            sparse_writes.deinit(self.alloc);
            pending_artifact_loads.deinit(self.alloc);
        }
        var delete_keys = std.ArrayListUnmanaged([]const u8).empty;
        defer delete_keys.deinit(self.alloc);
        var corrupt_artifact_deletes = std.ArrayListUnmanaged([]const u8).empty;
        defer corrupt_artifact_deletes.deinit(self.alloc);

        const scan_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
        for (writes) |write| {
            if (!std.mem.eql(u8, write.index_name, entry.config.name)) continue;
            const indices = write.indices;
            const values = write.values;
            if (write.artifact_key != null and indices.len == 0) {
                try pending_artifact_loads.append(self.alloc, .{
                    .doc_key = write.doc_key,
                    .artifact_key = write.artifact_key.?,
                });
                continue;
            }
            try delete_keys.append(self.alloc, write.doc_key);
            try sparse_writes.append(self.alloc, .{
                .doc_id = write.doc_key,
                .vec = .{
                    .indices = indices,
                    .values = values,
                },
            });
        }
        if (profile_enabled) profile.scan_ns = platform_time.monotonicNs() - scan_start_ns;

        if (pending_artifact_loads.items.len > 0) {
            const sort_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
            std.mem.sort(PendingSparseArtifactLoad, pending_artifact_loads.items, {}, struct {
                fn lessThan(_: void, a: PendingSparseArtifactLoad, b: PendingSparseArtifactLoad) bool {
                    return std.mem.order(u8, a.artifact_key, b.artifact_key) == .lt;
                }
            }.lessThan);
            if (profile_enabled) profile.sort_ns = platform_time.monotonicNs() - sort_start_ns;

            const read_keys = try self.alloc.alloc([]const u8, pending_artifact_loads.items.len);
            defer self.alloc.free(read_keys);
            const read_values = try self.alloc.alloc(?[]const u8, pending_artifact_loads.items.len);
            defer self.alloc.free(read_values);
            for (pending_artifact_loads.items, 0..) |item, i| read_keys[i] = item.artifact_key;

            const read_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
            var artifact_read_txn = try store.beginProbeTxn();
            defer artifact_read_txn.abort();
            try artifact_read_txn.getManySorted(read_keys, read_values);
            if (profile_enabled) profile.artifact_read_ns = platform_time.monotonicNs() - read_start_ns;

            const decode_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
            for (pending_artifact_loads.items, 0..) |item, i| {
                const raw = read_values[i] orelse continue;
                const maybe_view = enrichment_artifact_codec.sparseEmbeddingVectorView(raw) catch |err| {
                    if (isRecoverableEmbeddingArtifactError(err)) {
                        try corrupt_artifact_deletes.append(self.alloc, item.artifact_key);
                        continue;
                    }
                    return err;
                };
                if (maybe_view) |view| {
                    if (profile_enabled) profile.artifact_view_count += 1;
                    try delete_keys.append(self.alloc, item.doc_key);
                    try sparse_writes.append(self.alloc, .{
                        .doc_id = item.doc_key,
                        .vec = .{
                            .indices = view.indices,
                            .values = view.values,
                        },
                    });
                    continue;
                }

                if (profile_enabled) profile.artifact_copy_count += 1;
                var decoded = enrichment_artifact_codec.decodeSparseEmbeddingAlloc(self.alloc, raw) catch |err| {
                    if (isRecoverableEmbeddingArtifactError(err)) {
                        try corrupt_artifact_deletes.append(self.alloc, item.artifact_key);
                        continue;
                    }
                    return err;
                };
                errdefer decoded.deinit(self.alloc);

                const owned_indices_slice = decoded.indices;
                try owned_indices.append(self.alloc, owned_indices_slice);
                decoded.indices = &.{};

                const owned_values_slice = decoded.values;
                try owned_values.append(self.alloc, owned_values_slice);
                decoded.values = &.{};

                try delete_keys.append(self.alloc, item.doc_key);
                try sparse_writes.append(self.alloc, .{
                    .doc_id = item.doc_key,
                    .vec = .{
                        .indices = owned_indices_slice,
                        .values = owned_values_slice,
                    },
                });
            }
            if (profile_enabled) profile.artifact_decode_ns = platform_time.monotonicNs() - decode_start_ns;

            if (delete_keys.items.len > 0) {
                const delete_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
                try entry.index.batchWithOptions(&.{}, delete_keys.items, .{
                    .defer_term_range_updates = true,
                    .backend_batch_options = batch_options,
                });
                if (profile_enabled) profile.delete_batch_ns = platform_time.monotonicNs() - delete_start_ns;
            }
            if (sparse_writes.items.len > 0) {
                const sparse_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
                try self.assignSparseWriteDocNumsFromIdentity(store, entry, sparse_writes.items);
                try entry.index.batchWithOptions(sparse_writes.items, &.{}, .{
                    .defer_term_range_updates = true,
                    .backend_batch_options = batch_options,
                    .prefer_bulk_build = batch_options.mode == .bulk_ingest,
                    .assume_new_doc_ids = batch_options.mode == .bulk_ingest,
                });
                if (profile_enabled) profile.sparse_batch_ns = platform_time.monotonicNs() - sparse_start_ns;
            }
        }

        if (pending_artifact_loads.items.len == 0 and delete_keys.items.len > 0) {
            const delete_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
            try entry.index.batchWithOptions(&.{}, delete_keys.items, .{
                .defer_term_range_updates = true,
                .backend_batch_options = batch_options,
            });
            if (profile_enabled) profile.delete_batch_ns = platform_time.monotonicNs() - delete_start_ns;
        }
        if (pending_artifact_loads.items.len == 0 and sparse_writes.items.len > 0) {
            const sparse_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
            try self.assignSparseWriteDocNumsFromIdentity(store, entry, sparse_writes.items);
            try entry.index.batchWithOptions(sparse_writes.items, &.{}, .{
                .defer_term_range_updates = true,
                .backend_batch_options = batch_options,
                .prefer_bulk_build = batch_options.mode == .bulk_ingest,
                .assume_new_doc_ids = batch_options.mode == .bulk_ingest,
            });
            if (profile_enabled) profile.sparse_batch_ns = platform_time.monotonicNs() - sparse_start_ns;
        }
        if (corrupt_artifact_deletes.items.len > 0) {
            const corrupt_delete_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
            try store.putBatch(&.{}, corrupt_artifact_deletes.items);
            if (profile_enabled) profile.corrupt_delete_ns = platform_time.monotonicNs() - corrupt_delete_start_ns;
        }
        if (profile_enabled and (pending_artifact_loads.items.len > 0 or sparse_writes.items.len > 0 or delete_keys.items.len > 0)) {
            std.log.info(
                "antfly_bench_sparse_replay index={s} mode={s} input_writes={d} pending_artifacts={d} sparse_writes={d} delete_keys={d} corrupt_artifacts={d} total_ms={d} scan_ms={d} sort_ms={d} artifact_read_ms={d} artifact_decode_ms={d} artifact_views={d} artifact_copies={d} delete_batch_ms={d} sparse_batch_ms={d} corrupt_delete_ms={d}",
                .{
                    entry.config.name,
                    @tagName(batch_options.mode),
                    writes.len,
                    pending_artifact_loads.items.len,
                    sparse_writes.items.len,
                    delete_keys.items.len,
                    corrupt_artifact_deletes.items.len,
                    nsToMs(platform_time.monotonicNs() - total_start_ns),
                    nsToMs(profile.scan_ns),
                    nsToMs(profile.sort_ns),
                    nsToMs(profile.artifact_read_ns),
                    nsToMs(profile.artifact_decode_ns),
                    profile.artifact_view_count,
                    profile.artifact_copy_count,
                    nsToMs(profile.delete_batch_ns),
                    nsToMs(profile.sparse_batch_ns),
                    nsToMs(profile.corrupt_delete_ns),
                },
            );
        }
    }

    fn writeDenseVectorMapping(self: *IndexManager, store: *docstore_mod.DocStore, index_name: []const u8, doc_key: []const u8, vector_id: u64) !void {
        var batch = try store.beginWriteBatch();
        errdefer batch.abort();
        const txn = batch.asTxn();
        try self.writeDenseVectorMappingTxn(txn, index_name, doc_key, null, vector_id);
        try batch.commit();
    }

    fn writeDenseVectorMappingTxn(
        self: *IndexManager,
        txn: anytype,
        index_name: []const u8,
        doc_key: []const u8,
        parent_doc_key: ?[]const u8,
        vector_id: u64,
    ) !void {
        var mutable_txn = txn;
        const doc_map_key = try denseDocMappingKey(self.alloc, index_name, doc_key);
        defer self.alloc.free(doc_map_key);
        const vector_map_key = try denseVectorIdMappingKey(self.alloc, index_name, vector_id);
        defer self.alloc.free(vector_map_key);

        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, vector_id, .little);
        try mutable_txn.put(doc_map_key, &buf);
        try mutable_txn.put(vector_map_key, doc_key);

        const ordinal_doc_key = parent_doc_key orelse doc_key;
        if (try doc_identity.lookupOrdinalTxn(self.alloc, mutable_txn, ordinal_doc_key)) |ordinal| {
            const ordinal_map_key = try denseOrdinalMappingKey(self.alloc, index_name, ordinal);
            defer self.alloc.free(ordinal_map_key);
            const vector_ordinal_map_key = try denseVectorOrdinalMappingKey(self.alloc, index_name, vector_id);
            defer self.alloc.free(vector_ordinal_map_key);
            const ordinal_member_key = try denseOrdinalMemberKey(self.alloc, index_name, ordinal, vector_id);
            defer self.alloc.free(ordinal_member_key);

            var ordinal_buf: [4]u8 = undefined;
            std.mem.writeInt(u32, &ordinal_buf, ordinal, .little);
            try mutable_txn.put(ordinal_map_key, &buf);
            try mutable_txn.put(ordinal_member_key, &buf);
            try mutable_txn.put(vector_ordinal_map_key, &ordinal_buf);
        }
    }

    fn clearDenseVectorMapping(self: *IndexManager, store: *docstore_mod.DocStore, index_name: []const u8, doc_key: []const u8, vector_id: u64) !void {
        var batch = try store.beginWriteBatch();
        errdefer batch.abort();
        const txn = batch.asTxn();
        const ordinal = if (self.denseIndex(index_name)) |entry|
            if (entry.chunk_name == null) try doc_identity.lookupOrdinalTxn(self.alloc, txn, doc_key) else null
        else
            null;
        try self.clearDenseVectorMappingTxn(txn, index_name, doc_key, vector_id);
        try batch.commit();
        if (ordinal) |doc_ordinal| {
            if (self.denseIndex(index_name)) |entry| {
                _ = entry.ordinal_vector_ids.remove(doc_ordinal);
                _ = entry.vector_ordinals.remove(vector_id);
            }
        }
    }

    fn clearDenseVectorMappingTxn(self: *IndexManager, txn: anytype, index_name: []const u8, doc_key: []const u8, vector_id: u64) !void {
        var mutable_txn = txn;
        const doc_map_key = try denseDocMappingKey(self.alloc, index_name, doc_key);
        defer self.alloc.free(doc_map_key);
        const vector_map_key = try denseVectorIdMappingKey(self.alloc, index_name, vector_id);
        defer self.alloc.free(vector_map_key);
        const legacy_doc_map_key = try legacyDenseDocMappingKey(self.alloc, index_name, doc_key);
        defer self.alloc.free(legacy_doc_map_key);
        const legacy_vector_map_key = try legacyDenseVectorIdMappingKey(self.alloc, index_name, vector_id);
        defer self.alloc.free(legacy_vector_map_key);
        const ordinal = (try doc_identity.lookupOrdinalTxn(self.alloc, mutable_txn, doc_key)) orelse
            try self.lookupDenseVectorOrdinalTxn(mutable_txn, index_name, vector_id);

        mutable_txn.delete(doc_map_key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
        mutable_txn.delete(vector_map_key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
        mutable_txn.delete(legacy_doc_map_key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
        mutable_txn.delete(legacy_vector_map_key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
        if (ordinal) |doc_ordinal| {
            const ordinal_map_key = try denseOrdinalMappingKey(self.alloc, index_name, doc_ordinal);
            defer self.alloc.free(ordinal_map_key);
            const vector_ordinal_map_key = try denseVectorOrdinalMappingKey(self.alloc, index_name, vector_id);
            defer self.alloc.free(vector_ordinal_map_key);
            const ordinal_member_key = try denseOrdinalMemberKey(self.alloc, index_name, doc_ordinal, vector_id);
            defer self.alloc.free(ordinal_member_key);
            const legacy_ordinal_map_key = try legacyDenseOrdinalMappingKey(self.alloc, index_name, doc_ordinal);
            defer self.alloc.free(legacy_ordinal_map_key);
            const legacy_vector_ordinal_map_key = try legacyDenseVectorOrdinalMappingKey(self.alloc, index_name, vector_id);
            defer self.alloc.free(legacy_vector_ordinal_map_key);
            const legacy_ordinal_member_key = try legacyDenseOrdinalMemberKey(self.alloc, index_name, doc_ordinal, vector_id);
            defer self.alloc.free(legacy_ordinal_member_key);
            mutable_txn.delete(ordinal_map_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            mutable_txn.delete(ordinal_member_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            mutable_txn.delete(vector_ordinal_map_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            mutable_txn.delete(legacy_ordinal_map_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            mutable_txn.delete(legacy_ordinal_member_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            mutable_txn.delete(legacy_vector_ordinal_map_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
        }
    }

    fn lookupDenseDocKeyByVectorIdTxn(self: *IndexManager, txn: anytype, index_name: []const u8, vector_id: u64) !?[]u8 {
        var mutable_txn = txn;
        const key = try denseVectorIdMappingKey(self.alloc, index_name, vector_id);
        defer self.alloc.free(key);
        const legacy_key = try legacyDenseVectorIdMappingKey(self.alloc, index_name, vector_id);
        defer self.alloc.free(legacy_key);

        const raw = mutable_txn.get(key) catch |err| switch (err) {
            error.NotFound => legacy: {
                const legacy_raw = mutable_txn.get(legacy_key) catch |legacy_err| switch (legacy_err) {
                    error.NotFound => return null,
                    else => return legacy_err,
                };
                break :legacy legacy_raw;
            },
            else => return err,
        };
        return try self.alloc.dupe(u8, raw);
    }

    pub fn lookupDenseVectorIdsForOrdinalsAlloc(
        self: *IndexManager,
        alloc: Allocator,
        store: anytype,
        index_name: []const u8,
        ordinals: []const doc_identity.DocOrdinal,
    ) ![]u64 {
        const prefer_primary_mapping = if (self.denseIndex(index_name)) |entry| entry.chunk_name == null else false;
        if (prefer_primary_mapping) {
            const entry = self.denseIndex(index_name) orelse return try alloc.alloc(u64, 0);
            if (try self.lookupPrimaryDenseCachedVectorIdsForOrdinalsAlloc(alloc, entry, ordinals)) |cached| {
                return cached;
            }
        }

        var runtime_store = try initRuntimeStore(self.alloc, store);
        defer runtime_store.deinit();

        var txn = try runtime_store.store.beginRead();
        defer txn.abort();

        var out = std.ArrayListUnmanaged(u64).empty;
        errdefer out.deinit(alloc);
        if (prefer_primary_mapping) {
            const entry = self.denseIndex(index_name) orelse return try alloc.alloc(u64, 0);
            return try self.lookupPrimaryDenseVectorIdsForOrdinalsAlloc(alloc, &txn, entry, index_name, ordinals);
        }
        for (ordinals) |ordinal| {
            const before_len = out.items.len;
            try self.appendDenseVectorIdsForOrdinalAlloc(alloc, &out, &runtime_store.store, index_name, ordinal);
            if (out.items.len != before_len) continue;
            const vector_id = (try self.lookupDenseVectorIdByOrdinalTxn(&txn, index_name, ordinal)) orelse fallback: {
                const doc_key = (try doc_identity.lookupDocIdTxn(self.alloc, &txn, ordinal)) orelse continue;
                defer self.alloc.free(doc_key);
                break :fallback (try self.lookupDenseVectorIdForDocKeyTxn(&txn, index_name, doc_key)) orelse continue;
            };
            if (!containsU64(out.items, vector_id)) try out.append(alloc, vector_id);
        }
        return try out.toOwnedSlice(alloc);
    }

    pub fn lookupDenseOrdinalsForVectorIdsAlloc(
        self: *IndexManager,
        alloc: Allocator,
        store: anytype,
        index_name: []const u8,
        vector_ids: []const u64,
    ) ![]?doc_identity.DocOrdinal {
        const out = try alloc.alloc(?doc_identity.DocOrdinal, vector_ids.len);
        errdefer alloc.free(out);
        @memset(out, null);
        if (vector_ids.len == 0) return out;

        const entry = self.denseIndex(index_name) orelse return error.IndexNotFound;
        var missing = std.ArrayListUnmanaged(struct {
            source_index: usize,
            vector_id: u64,
        }).empty;
        defer missing.deinit(alloc);

        for (vector_ids, 0..) |vector_id, i| {
            if (entry.vector_ordinals.get(vector_id)) |ordinal| {
                out[i] = ordinal;
            } else {
                try missing.append(alloc, .{ .source_index = i, .vector_id = vector_id });
            }
        }
        if (missing.items.len == 0) return out;

        var runtime_store = try initRuntimeStore(self.alloc, store);
        defer runtime_store.deinit();
        var txn = try runtime_store.store.beginRead();
        defer txn.abort();
        for (missing.items) |item| {
            const ordinal = (try self.lookupDenseVectorOrdinalTxn(&txn, index_name, item.vector_id)) orelse continue;
            out[item.source_index] = ordinal;
            if (entry.chunk_name == null) {
                try entry.vector_ordinals.put(self.alloc, item.vector_id, ordinal);
                try entry.ordinal_vector_ids.put(self.alloc, ordinal, item.vector_id);
            }
        }
        return out;
    }

    fn lookupPrimaryDenseCachedVectorIdsForOrdinalsAlloc(
        self: *IndexManager,
        alloc: Allocator,
        entry: *DenseIndex,
        ordinals: []const doc_identity.DocOrdinal,
    ) !?[]u64 {
        _ = self;
        if (ordinals.len == 0) return try alloc.alloc(u64, 0);

        var sorted_ordinals = try alloc.dupe(doc_identity.DocOrdinal, ordinals);
        defer alloc.free(sorted_ordinals);
        std.mem.sort(doc_identity.DocOrdinal, sorted_ordinals, {}, docOrdinalLessThan);
        const unique_ordinals = sorted_ordinals[0..uniqueSortedDocOrdinals(sorted_ordinals)];

        var out = try std.ArrayListUnmanaged(u64).initCapacity(alloc, unique_ordinals.len);
        errdefer out.deinit(alloc);
        for (unique_ordinals) |ordinal| {
            const vector_id = entry.ordinal_vector_ids.get(ordinal) orelse {
                out.deinit(alloc);
                return null;
            };
            out.appendAssumeCapacity(vector_id);
        }
        return try out.toOwnedSlice(alloc);
    }

    fn lookupPrimaryDenseVectorIdsForOrdinalsAlloc(
        self: *IndexManager,
        alloc: Allocator,
        txn: anytype,
        entry: *DenseIndex,
        index_name: []const u8,
        ordinals: []const doc_identity.DocOrdinal,
    ) ![]u64 {
        if (ordinals.len == 0) return try alloc.alloc(u64, 0);

        var sorted_ordinals = try alloc.dupe(doc_identity.DocOrdinal, ordinals);
        defer alloc.free(sorted_ordinals);
        std.mem.sort(doc_identity.DocOrdinal, sorted_ordinals, {}, docOrdinalLessThan);
        const unique_ordinals = sorted_ordinals[0..uniqueSortedDocOrdinals(sorted_ordinals)];

        var cached_out = std.ArrayListUnmanaged(u64).empty;
        errdefer cached_out.deinit(alloc);
        var missing = std.ArrayListUnmanaged(doc_identity.DocOrdinal).empty;
        defer missing.deinit(alloc);
        for (unique_ordinals) |ordinal| {
            if (entry.ordinal_vector_ids.get(ordinal)) |vector_id| {
                try cached_out.append(alloc, vector_id);
            } else {
                try missing.append(alloc, ordinal);
            }
        }
        if (missing.items.len == 0) return try cached_out.toOwnedSlice(alloc);

        const lookup_ordinals = missing.items;
        const keys = try alloc.alloc([]const u8, lookup_ordinals.len);
        defer alloc.free(keys);
        const values = try alloc.alloc(?[]const u8, lookup_ordinals.len);
        defer alloc.free(values);
        var key_count: usize = 0;
        errdefer {
            for (keys[0..key_count]) |key| self.alloc.free(@constCast(key));
        }
        for (lookup_ordinals, 0..) |ordinal, i| {
            keys[i] = try denseOrdinalMappingKey(self.alloc, index_name, ordinal);
            key_count += 1;
            values[i] = null;
        }
        key_count = 0;
        defer {
            for (keys) |key| self.alloc.free(@constCast(key));
        }

        var mutable_txn = txn;
        try mutable_txn.getManySorted(keys, values);

        var out = cached_out;
        cached_out = .empty;
        errdefer out.deinit(alloc);
        var missing_count: usize = 0;
        for (values, lookup_ordinals) |maybe_raw, ordinal| {
            const raw = maybe_raw orelse {
                missing_count += 1;
                continue;
            };
            if (raw.len != 8) return error.InvalidDenseVectorMetadata;
            const vector_id = std.mem.readInt(u64, raw[0..8], .little);
            try entry.ordinal_vector_ids.put(self.alloc, ordinal, vector_id);
            try out.append(alloc, vector_id);
        }
        if (missing_count == 0) return try out.toOwnedSlice(alloc);

        var legacy_keys = try alloc.alloc([]const u8, missing_count);
        defer alloc.free(legacy_keys);
        var legacy_values = try alloc.alloc(?[]const u8, missing_count);
        defer alloc.free(legacy_values);
        var missing_ordinals = try alloc.alloc(doc_identity.DocOrdinal, missing_count);
        defer alloc.free(missing_ordinals);
        var missing_index: usize = 0;
        errdefer {
            for (legacy_keys[0..missing_index]) |key| self.alloc.free(@constCast(key));
        }
        for (lookup_ordinals, values) |ordinal, maybe_raw| {
            if (maybe_raw != null) continue;
            legacy_keys[missing_index] = try legacyDenseOrdinalMappingKey(self.alloc, index_name, ordinal);
            legacy_values[missing_index] = null;
            missing_ordinals[missing_index] = ordinal;
            missing_index += 1;
        }
        missing_index = 0;
        defer {
            for (legacy_keys) |key| self.alloc.free(@constCast(key));
        }

        try mutable_txn.getManySorted(legacy_keys, legacy_values);
        for (legacy_values, missing_ordinals) |maybe_raw, ordinal| {
            if (maybe_raw) |raw| {
                if (raw.len != 8) return error.InvalidDenseVectorMetadata;
                const vector_id = std.mem.readInt(u64, raw[0..8], .little);
                try entry.ordinal_vector_ids.put(self.alloc, ordinal, vector_id);
                if (!containsU64(out.items, vector_id)) try out.append(alloc, vector_id);
                continue;
            }
            const doc_key = (try doc_identity.lookupDocIdTxn(self.alloc, mutable_txn, ordinal)) orelse continue;
            defer self.alloc.free(doc_key);
            const vector_id = (try self.lookupDenseVectorIdForDocKeyTxn(mutable_txn, index_name, doc_key)) orelse continue;
            try entry.ordinal_vector_ids.put(self.alloc, ordinal, vector_id);
            if (!containsU64(out.items, vector_id)) try out.append(alloc, vector_id);
        }

        return try out.toOwnedSlice(alloc);
    }

    fn appendDenseVectorIdsForOrdinalAlloc(
        self: *IndexManager,
        alloc: Allocator,
        out: *std.ArrayListUnmanaged(u64),
        store: anytype,
        index_name: []const u8,
        ordinal: doc_identity.DocOrdinal,
    ) !void {
        const prefix = try denseOrdinalMemberPrefix(self.alloc, index_name, ordinal);
        defer self.alloc.free(prefix);
        var scan_txn = try store.beginCurrentScan();
        defer scan_txn.abort();
        var cursor = try scan_txn.openCursor();
        defer cursor.close();
        var maybe_entry = try cursor.seekAtOrAfter(prefix);
        while (maybe_entry) |row| : (maybe_entry = try cursor.next()) {
            if (!std.mem.startsWith(u8, row.key, prefix)) break;
            if (row.value.len != 8) return error.InvalidDenseVectorMetadata;
            const vector_id = std.mem.readInt(u64, row.value[0..8], .little);
            if (!containsU64(out.items, vector_id)) try out.append(alloc, vector_id);
        }

        const legacy_prefix = try legacyDenseOrdinalMemberPrefix(self.alloc, index_name, ordinal);
        defer self.alloc.free(legacy_prefix);
        maybe_entry = try cursor.seekAtOrAfter(legacy_prefix);
        while (maybe_entry) |row| : (maybe_entry = try cursor.next()) {
            if (!std.mem.startsWith(u8, row.key, legacy_prefix)) break;
            if (row.value.len != 8) return error.InvalidDenseVectorMetadata;
            const vector_id = std.mem.readInt(u64, row.value[0..8], .little);
            if (!containsU64(out.items, vector_id)) try out.append(alloc, vector_id);
        }
    }

    fn lookupDenseVectorIdByOrdinalTxn(self: *IndexManager, txn: anytype, index_name: []const u8, ordinal: doc_identity.DocOrdinal) !?u64 {
        var mutable_txn = txn;
        const key = try denseOrdinalMappingKey(self.alloc, index_name, ordinal);
        defer self.alloc.free(key);
        const legacy_key = try legacyDenseOrdinalMappingKey(self.alloc, index_name, ordinal);
        defer self.alloc.free(legacy_key);

        const raw = mutable_txn.get(key) catch |err| switch (err) {
            error.NotFound => legacy: {
                const legacy_raw = mutable_txn.get(legacy_key) catch |legacy_err| switch (legacy_err) {
                    error.NotFound => return null,
                    else => return legacy_err,
                };
                break :legacy legacy_raw;
            },
            else => return err,
        };
        if (raw.len != 8) return error.InvalidDenseVectorMetadata;
        return std.mem.readInt(u64, raw[0..8], .little);
    }

    fn lookupDenseVectorOrdinalTxn(self: *IndexManager, txn: anytype, index_name: []const u8, vector_id: u64) !?doc_identity.DocOrdinal {
        var mutable_txn = txn;
        const key = try denseVectorOrdinalMappingKey(self.alloc, index_name, vector_id);
        defer self.alloc.free(key);
        const legacy_key = try legacyDenseVectorOrdinalMappingKey(self.alloc, index_name, vector_id);
        defer self.alloc.free(legacy_key);

        const raw = mutable_txn.get(key) catch |err| switch (err) {
            error.NotFound => legacy: {
                const legacy_raw = mutable_txn.get(legacy_key) catch |legacy_err| switch (legacy_err) {
                    error.NotFound => return null,
                    else => return legacy_err,
                };
                break :legacy legacy_raw;
            },
            else => return err,
        };
        if (raw.len != 4) return error.InvalidDenseVectorMetadata;
        return std.mem.readInt(u32, raw[0..4], .little);
    }

    fn lookupDenseVectorIdForDocKeyTxn(self: *IndexManager, txn: anytype, index_name: []const u8, doc_key: []const u8) !?u64 {
        if (try self.lookupDenseVectorIdTxn(txn, index_name, doc_key)) |mapped| return mapped;
        const entry = self.denseIndex(index_name) orelse return null;
        if (try doc_identity.lookupOrdinalTxn(self.alloc, txn, doc_key)) |ordinal| {
            const vector_id: u64 = ordinal;
            if ((try self.denseVectorIdMetadataState(entry, vector_id, doc_key, null)) == .matches) return vector_id;
        }
        const vector_id = deterministicDenseVectorId(doc_key);
        const metadata = (try entry.index.getMetadata(vector_id)) orelse return null;
        self.alloc.free(metadata);
        return vector_id;
    }

    fn keyInRange(self: *const IndexManager, key: []const u8) bool {
        if (internal_keys.isInternalUserKey(key)) {
            const raw = (internal_keys.decodeDocumentComponentAlloc(self.alloc, key) catch return false) orelse return false;
            defer self.alloc.free(raw);
            return self.byte_range.contains(raw);
        }
        return self.byte_range.contains(key);
    }

    fn findTextIndexEntry(self: *IndexManager, name: []const u8) ?*TextIndex {
        for (self.text_indexes.items) |*entry| {
            if (std.mem.eql(u8, entry.config.name, name)) return entry;
        }
        return null;
    }

    fn findSparseIndexEntry(self: *IndexManager, name: []const u8) ?*SparseIndex {
        for (self.sparse_indexes.items) |*entry| {
            if (std.mem.eql(u8, entry.config.name, name)) return entry;
        }
        return null;
    }
};

const SplitRebuiltSegment = struct {
    segment_bytes: ?[]u8,
    doc_keys: [][]u8,
};

fn mergeHandoffDocKeys(alloc: Allocator, handoff: *TextSplitHandoff, doc_keys: []const []u8) !void {
    try mergeSkipDocKeys(alloc, &handoff.skip_doc_keys, doc_keys);
}

fn mergeSkipDocKeys(alloc: Allocator, skip_doc_keys: *std.StringHashMapUnmanaged(void), doc_keys: []const []u8) !void {
    for (doc_keys) |key| {
        const gop = try skip_doc_keys.getOrPut(alloc, key);
        if (gop.found_existing) {
            alloc.free(key);
        } else {
            gop.value_ptr.* = {};
        }
    }
}

fn buildSplitSegment(
    alloc: Allocator,
    segment_bytes: []const u8,
    deletion_bitmap_bytes: ?[]const u8,
    split_key: []const u8,
    side: SplitSide,
    config_json: ?[]const u8,
    runtime_schema: ?schema_mod.TableSchema,
    collect_doc_keys: bool,
) !SplitRebuiltSegment {
    var reader = try segment_mod.SegmentReader.init(alloc, segment_bytes);
    defer reader.deinit();

    var deleted: ?roaring.RoaringBitmap = null;
    defer if (deleted) |*bitmap| {
        var owned = bitmap.*;
        owned.deinit();
    };
    if (deletion_bitmap_bytes) |bytes| {
        deleted = try roaring.RoaringBitmap.fromBytes(alloc, bytes);
    }

    var docs = std.ArrayListUnmanaged(mapper.MapperDoc).empty;
    defer {
        for (docs.items) |doc| {
            alloc.free(@constCast(doc.key));
            alloc.free(@constCast(doc.value));
        }
        docs.deinit(alloc);
    }

    var doc_keys = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (doc_keys.items) |key| alloc.free(key);
        doc_keys.deinit(alloc);
    }

    for (0..reader.doc_count) |doc_idx_usize| {
        const doc_idx: u32 = @intCast(doc_idx_usize);
        if (deleted) |bitmap| {
            if (bitmap.contains(doc_idx)) continue;
        }

        const stored = (try reader.storedDocDecompressed(doc_idx)) orelse continue;
        errdefer alloc.free(stored.data);

        const keep = switch (side) {
            .left => std.mem.order(u8, stored.id, split_key) == .lt,
            .right => std.mem.order(u8, stored.id, split_key) != .lt,
        };
        if (!keep) {
            alloc.free(stored.data);
            continue;
        }

        const key = try alloc.dupe(u8, stored.id);
        try docs.append(alloc, .{
            .key = key,
            .value = stored.data,
            .doc_ordinal = try reader.docOrdinal(doc_idx),
        });
        if (collect_doc_keys) {
            try doc_keys.append(alloc, try alloc.dupe(u8, stored.id));
        }
    }

    if (docs.items.len == 0) {
        return .{
            .segment_bytes = null,
            .doc_keys = try doc_keys.toOwnedSlice(alloc),
        };
    }

    const split_text_analysis = try introducer_mod.parseTextAnalysisConfig(alloc, config_json);
    defer introducer_mod.freeTextAnalysisConfig(alloc, split_text_analysis);
    // Pass the runtime schema so a relational table's split segment re-derives
    // its typed columns + manifest from the reconstructed documents (rather than
    // degrading to a document-mode segment that has lost columnar pushdown).
    const rebuilt = try mapper.buildTextSegmentFromDocuments(alloc, docs.items, split_text_analysis, runtime_schema);
    return .{
        .segment_bytes = rebuilt,
        .doc_keys = try doc_keys.toOwnedSlice(alloc),
    };
}

fn findTextSplitHandoff(handoffs: []const TextSplitHandoff, index_name: []const u8) ?*const TextSplitHandoff {
    for (handoffs) |*handoff| {
        if (std.mem.eql(u8, handoff.index_name, index_name)) return handoff;
    }
    return null;
}

fn findDenseSplitHandoff(handoffs: []const DenseSplitHandoff, index_name: []const u8) ?*const DenseSplitHandoff {
    for (handoffs) |*handoff| {
        if (std.mem.eql(u8, handoff.index_name, index_name)) return handoff;
    }
    return null;
}

fn findSparseSplitHandoff(handoffs: []const SparseSplitHandoff, index_name: []const u8) ?*const SparseSplitHandoff {
    for (handoffs) |*handoff| {
        if (std.mem.eql(u8, handoff.index_name, index_name)) return handoff;
    }
    return null;
}

fn isMetadataKey(key: []const u8) bool {
    return std.mem.startsWith(u8, key, "\x00\x00__metadata__:") or
        std.mem.startsWith(u8, key, "splitstate:") or
        std.mem.startsWith(u8, key, "splitdelta:") or
        internal_keys.isTtlKey(key);
}

fn textIndexShouldConsumeDoc(self: *const IndexManager, entry: *const IndexManager.TextIndex, key: []const u8) !bool {
    if (entry.chunk_name) |chunk_name| {
        return internal_keys.matchesChunkArtifactName(key, chunk_name);
    }
    if (isPrimaryDocumentCandidate(key)) return true;
    if (!internal_keys.isChunkArtifactRecordKey(key)) return false;
    return try self.textIndexIsChunkBacked(self.alloc, entry.config.name);
}

fn isPrimaryDocumentCandidate(key: []const u8) bool {
    if (internal_keys.isPrimaryDocumentKey(key)) return true;
    if (internal_keys.isInternalUserKey(key)) return false;
    if (docstore_mod.KeyEncoder.parseEdgeKey(key) != null) return false;
    return true;
}

fn serializeCatalog(alloc: Allocator, manager: *const IndexManager) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    try out.appendSlice(alloc, "AIDX");
    try appendU32(&out, alloc, 1);
    const count = manager.text_indexes.items.len + manager.dense_indexes.items.len + manager.sparse_indexes.items.len + manager.algebraic_indexes.items.len + manager.status_only_index_configs.len;
    const graph_count = manager.graph_indexes.items.len;
    try appendU32(&out, alloc, @intCast(count + graph_count));

    for (manager.text_indexes.items) |entry| {
        try appendStr(&out, alloc, entry.config.name);
        try out.append(alloc, @intFromEnum(entry.config.kind));
        try appendStr(&out, alloc, entry.config.config_json);
    }
    for (manager.dense_indexes.items) |entry| {
        try appendStr(&out, alloc, entry.config.name);
        try out.append(alloc, @intFromEnum(entry.config.kind));
        try appendStr(&out, alloc, entry.config.config_json);
    }
    for (manager.sparse_indexes.items) |entry| {
        try appendStr(&out, alloc, entry.config.name);
        try out.append(alloc, @intFromEnum(entry.config.kind));
        try appendStr(&out, alloc, entry.config.config_json);
    }
    for (manager.graph_indexes.items) |entry| {
        try appendStr(&out, alloc, entry.config.name);
        try out.append(alloc, @intFromEnum(entry.config.kind));
        try appendStr(&out, alloc, entry.config.config_json);
    }
    for (manager.algebraic_indexes.items) |entry| {
        try appendStr(&out, alloc, entry.config.name);
        try out.append(alloc, @intFromEnum(entry.config.kind));
        try appendStr(&out, alloc, entry.config.config_json);
    }
    for (manager.status_only_index_configs) |cfg| {
        try appendStr(&out, alloc, cfg.name);
        try out.append(alloc, @intFromEnum(cfg.kind));
        try appendStr(&out, alloc, cfg.config_json);
    }

    const owned = try alloc.dupe(u8, out.items);
    out.deinit(alloc);
    return owned;
}

fn deserializeCatalog(alloc: Allocator, data: []const u8) ![]types.IndexConfig {
    if (data.len < 12 or !std.mem.eql(u8, data[0..4], "AIDX")) return error.InvalidIndexCatalog;

    var pos: usize = 4;
    const version = try readU32(data, &pos);
    if (version != 1) return error.UnsupportedIndexCatalogVersion;

    const count = try readU32(data, &pos);
    var configs = try alloc.alloc(types.IndexConfig, count);
    var initialized: usize = 0;
    errdefer {
        for (configs[0..initialized]) |*cfg| cfg.deinit(alloc);
        alloc.free(configs);
    }

    for (0..count) |i| {
        const name = try alloc.dupe(u8, try readStr(data, &pos));
        errdefer alloc.free(name);

        if (pos >= data.len) return error.InvalidIndexCatalog;
        const kind_value = data[pos];
        pos += 1;
        const kind: types.IndexKind = switch (kind_value) {
            @intFromEnum(types.IndexKind.full_text) => .full_text,
            @intFromEnum(types.IndexKind.dense_vector) => .dense_vector,
            @intFromEnum(types.IndexKind.sparse_vector) => .sparse_vector,
            @intFromEnum(types.IndexKind.graph) => .graph,
            @intFromEnum(types.IndexKind.algebraic) => .algebraic,
            else => return error.InvalidIndexCatalog,
        };

        const config_json = try alloc.dupe(u8, try readStr(data, &pos));
        configs[i] = .{
            .name = name,
            .kind = kind,
            .config_json = config_json,
        };
        initialized += 1;
    }

    return configs;
}

fn appendU32(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, value: u32) !void {
    var bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &bytes, value, .little);
    try out.appendSlice(alloc, &bytes);
}

fn appendStr(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, value: []const u8) !void {
    try appendU32(out, alloc, @intCast(value.len));
    try out.appendSlice(alloc, value);
}

fn readU32(data: []const u8, pos: *usize) !u32 {
    if (pos.* + 4 > data.len) return error.InvalidIndexCatalog;
    const value = std.mem.readInt(u32, data[pos.*..][0..4], .little);
    pos.* += 4;
    return value;
}

fn readStr(data: []const u8, pos: *usize) ![]const u8 {
    const len = try readU32(data, pos);
    if (pos.* + len > data.len) return error.InvalidIndexCatalog;
    const value = data[pos.* .. pos.* + len];
    pos.* += len;
    return value;
}

const DenseConfig = struct {
    field_name: []u8,
    dims: u32,
    metric: vector_mod.DistanceMetric,
    split_algo: vector_mod.ClustAlgorithm = .kmeans,
    embedding_name: ?[]u8 = null,
    external: bool = false,
    search_width: u32 = 2 * 3 * 7 * 24,
    epsilon: f32 = 7,
    branching_factor: u32 = 7 * 24,
    leaf_size: u32 = 7 * 24,
    bulk_build_algo: hbc_mod.BulkBuildAlgo = .hilbert_seeded,
    kmeans_backend: hbc_mod.HBCConfig.KmeansBackend = .auto,
    kmeans_update_strategy: hbc_mod.HBCConfig.KmeansUpdateStrategy = .auto,
    use_quantization: bool = true,
    rerank_policy: hbc_mod.HBCConfig.RerankPolicy = .boundary,
    quantizer_seed: u64 = 42,
    use_random_ortho_trans: bool = false,
    max_cached_nodes: usize = 100_000,
    max_cached_vectors: usize = 100_000,
    max_cached_metadata: usize = 100_000,
    lazy_posting_maintenance: bool = false,
    auto_posting_maintenance_max_postings: usize = 0,
    centroid_directory_mode: hbc_mod.HBCConfig.CentroidDirectoryMode = .hbc,
    flat_centroid_block_size: usize = 8192,
    flat_centroid_probe_count: usize = 0,

    fn deinit(self: *const DenseConfig, alloc: Allocator) void {
        alloc.free(self.field_name);
        if (self.embedding_name) |embedding_name| alloc.free(embedding_name);
    }
};

const TextConfig = struct {
    source_artifact_name: ?[]u8 = null,

    fn deinit(self: *const TextConfig, alloc: Allocator) void {
        if (self.source_artifact_name) |source_artifact_name| alloc.free(source_artifact_name);
    }
};

const GeneratorConfig = struct {
    source_field: []u8,
    source_template: []u8 = &.{},
    artifact_name: []u8,
    embedding_name: ?[]u8 = null,
    chunk_size: u32 = 0,
    chunk_overlap: u32 = 0,
    chunker_json: []u8 = &.{},

    fn deinit(self: *const GeneratorConfig, alloc: Allocator) void {
        alloc.free(self.source_field);
        if (self.source_template.len > 0) alloc.free(self.source_template);
        alloc.free(self.artifact_name);
        if (self.embedding_name) |embedding_name| alloc.free(embedding_name);
        if (self.chunker_json.len > 0) alloc.free(self.chunker_json);
    }
};

const SparseConfig = struct {
    field_name: []u8,

    fn deinit(self: *const SparseConfig, alloc: Allocator) void {
        alloc.free(self.field_name);
    }
};

pub const GraphArtifactFormat = enum {
    extraction_relation,
    extraction_graph,
};

pub const GraphNodeModel = enum {
    document,
    external,
};

pub const GraphArtifactMapping = struct {
    node_model: GraphNodeModel = .document,
    source_template: []u8 = "",
    target_template: []u8 = "",
    edge_type_template: []u8 = "",
    weight_template: []u8 = "",
    metadata_template_json: []u8 = "",
    context_doc_fields: []const []u8 = &.{},

    pub fn clone(alloc: Allocator, mapping: GraphArtifactMapping) !GraphArtifactMapping {
        const context_doc_fields = if (mapping.context_doc_fields.len > 0)
            try alloc.alloc([]u8, mapping.context_doc_fields.len)
        else
            &.{};
        var initialized: usize = 0;
        errdefer {
            for (context_doc_fields[0..initialized]) |field| alloc.free(field);
            if (context_doc_fields.len > 0) alloc.free(context_doc_fields);
        }
        for (mapping.context_doc_fields, 0..) |field, i| {
            context_doc_fields[i] = try alloc.dupe(u8, field);
            initialized += 1;
        }
        return .{
            .node_model = mapping.node_model,
            .source_template = if (mapping.source_template.len > 0) try alloc.dupe(u8, mapping.source_template) else "",
            .target_template = if (mapping.target_template.len > 0) try alloc.dupe(u8, mapping.target_template) else "",
            .edge_type_template = if (mapping.edge_type_template.len > 0) try alloc.dupe(u8, mapping.edge_type_template) else "",
            .weight_template = if (mapping.weight_template.len > 0) try alloc.dupe(u8, mapping.weight_template) else "",
            .metadata_template_json = if (mapping.metadata_template_json.len > 0) try alloc.dupe(u8, mapping.metadata_template_json) else "",
            .context_doc_fields = context_doc_fields,
        };
    }

    pub fn deinit(self: *GraphArtifactMapping, alloc: Allocator) void {
        if (self.source_template.len > 0) alloc.free(self.source_template);
        if (self.target_template.len > 0) alloc.free(self.target_template);
        if (self.edge_type_template.len > 0) alloc.free(self.edge_type_template);
        if (self.weight_template.len > 0) alloc.free(self.weight_template);
        if (self.metadata_template_json.len > 0) alloc.free(self.metadata_template_json);
        for (self.context_doc_fields) |field| alloc.free(field);
        if (self.context_doc_fields.len > 0) alloc.free(self.context_doc_fields);
        self.* = undefined;
    }
};

pub const GraphArtifactSource = struct {
    artifact_name: []u8,
    path: []u8 = "",
    format: GraphArtifactFormat = .extraction_relation,
    mapping: GraphArtifactMapping = .{},

    pub fn clone(alloc: Allocator, source: GraphArtifactSource) !GraphArtifactSource {
        return .{
            .artifact_name = try alloc.dupe(u8, source.artifact_name),
            .path = if (source.path.len > 0) try alloc.dupe(u8, source.path) else "",
            .format = source.format,
            .mapping = try GraphArtifactMapping.clone(alloc, source.mapping),
        };
    }

    pub fn deinit(self: *GraphArtifactSource, alloc: Allocator) void {
        alloc.free(self.artifact_name);
        if (self.path.len > 0) alloc.free(self.path);
        self.mapping.deinit(alloc);
        self.* = undefined;
    }
};

const GraphConfig = struct {
    edge_type_configs: []graph_mod.EdgeTypeConfig,
    artifact_source: ?GraphArtifactSource = null,
    shorthand_asset: ?enrichment_catalog.EnrichmentConfig = null,
    algebraic_semiring_traversal: bool = false,

    fn deinit(self: *GraphConfig, alloc: Allocator) void {
        for (self.edge_type_configs) |cfg| {
            alloc.free(cfg.name);
            if (cfg.field_name) |field_name| alloc.free(field_name);
        }
        alloc.free(self.edge_type_configs);
        if (self.artifact_source) |*source| source.deinit(alloc);
        if (self.shorthand_asset) |*asset| asset.deinit(alloc);
    }
};

fn publicEnrichmentKindToInternal(kind: types.EnrichmentKind) enrichment_catalog.EnrichmentType {
    return switch (kind) {
        .chunk => .chunk,
        .asset => .asset,
        .embedding => .embedding,
    };
}

fn enrichmentFromPublic(alloc: Allocator, cfg: types.EnrichmentConfig) !enrichment_catalog.EnrichmentConfig {
    return .{
        .name = try alloc.dupe(u8, cfg.name),
        .kind = publicEnrichmentKindToInternal(cfg.kind),
        .source_field = if (cfg.field.len > 0) try alloc.dupe(u8, cfg.field) else "",
        .source_template = if (cfg.template.len > 0) try alloc.dupe(u8, cfg.template) else "",
        .source_artifact_name = if (cfg.source_artifact_name.len > 0) try alloc.dupe(u8, cfg.source_artifact_name) else "",
        .expected_dims = cfg.expected_dims,
        .chunk_size = cfg.chunk_size,
        .chunk_overlap = cfg.chunk_overlap,
        .chunker_json = if (cfg.chunker_json.len > 0) try alloc.dupe(u8, cfg.chunker_json) else "",
        .content_type = if (cfg.content_type.len > 0) try alloc.dupe(u8, cfg.content_type) else "",
        .producer_json = if (cfg.producer_json.len > 0) try alloc.dupe(u8, cfg.producer_json) else "",
    };
}

fn internalEnrichmentKindToPublic(kind: enrichment_catalog.EnrichmentType) types.EnrichmentKind {
    return switch (kind) {
        .chunk => .chunk,
        .asset => .asset,
        .embedding => .embedding,
    };
}

fn enrichmentToPublic(alloc: Allocator, cfg: enrichment_catalog.EnrichmentConfig) !types.EnrichmentConfig {
    const out = types.EnrichmentConfig{
        .name = try alloc.dupe(u8, cfg.name),
        .kind = internalEnrichmentKindToPublic(cfg.kind),
        .field = if (cfg.source_field.len > 0) try alloc.dupe(u8, cfg.source_field) else "",
        .template = if (cfg.source_template.len > 0) try alloc.dupe(u8, cfg.source_template) else "",
        .source_artifact_name = if (cfg.source_artifact_name.len > 0) try alloc.dupe(u8, cfg.source_artifact_name) else "",
        .expected_dims = cfg.expected_dims,
        .chunk_size = cfg.chunk_size,
        .chunk_overlap = cfg.chunk_overlap,
        .chunker_json = if (cfg.chunker_json.len > 0) try alloc.dupe(u8, cfg.chunker_json) else "",
        .content_type = if (cfg.content_type.len > 0) try alloc.dupe(u8, cfg.content_type) else "",
        .producer_json = if (cfg.producer_json.len > 0) try alloc.dupe(u8, cfg.producer_json) else "",
    };
    return out;
}

fn parseDenseConfig(alloc: Allocator, raw: []const u8) !DenseConfig {
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();
    const root = parsed.value;
    if (root != .object) return error.InvalidIndexConfig;

    const field = root.object.get("field") orelse return error.InvalidIndexConfig;
    const dims = root.object.get("dims") orelse return error.InvalidIndexConfig;
    const metric = root.object.get("metric");

    return .{
        .field_name = try alloc.dupe(u8, field.string),
        .dims = std.math.cast(u32, dims.integer) orelse return error.InvalidIndexConfig,
        .metric = if (metric) |value| try parseMetric(value.string) else .l2_squared,
        .split_algo = if (root.object.get("split_algo")) |value| try parseClustAlgorithm(value.string) else .kmeans,
        .embedding_name = if (root.object.get("embedding_name")) |value|
            try alloc.dupe(u8, value.string)
        else
            null,
        .external = if (root.object.get("external")) |value|
            switch (value) {
                .bool => value.bool,
                else => return error.InvalidIndexConfig,
            }
        else
            false,
        .search_width = if (root.object.get("search_width")) |value|
            std.math.cast(u32, value.integer) orelse return error.InvalidIndexConfig
        else
            2 * 3 * 7 * 24,
        .epsilon = if (root.object.get("epsilon")) |value|
            switch (value) {
                .float => @floatCast(value.float),
                .integer => @floatFromInt(value.integer),
                else => return error.InvalidIndexConfig,
            }
        else
            7,
        .branching_factor = if (root.object.get("branching_factor")) |value|
            std.math.cast(u32, value.integer) orelse return error.InvalidIndexConfig
        else
            7 * 24,
        .leaf_size = if (root.object.get("leaf_size")) |value|
            std.math.cast(u32, value.integer) orelse return error.InvalidIndexConfig
        else
            7 * 24,
        .bulk_build_algo = if (root.object.get("bulk_build_algo")) |value|
            try parseBulkBuildAlgo(value.string)
        else
            .hilbert_seeded,
        .kmeans_backend = if (root.object.get("kmeans_backend")) |value|
            try parseKmeansBackend(value.string)
        else
            .auto,
        .kmeans_update_strategy = if (root.object.get("kmeans_update_strategy")) |value|
            try parseKmeansUpdateStrategy(value.string)
        else
            .auto,
        .use_quantization = if (root.object.get("use_quantization")) |value|
            value.bool
        else
            true,
        .rerank_policy = if (root.object.get("rerank_policy")) |value|
            try parseDenseRerankPolicy(value.string)
        else if (root.object.get("disable_reranking")) |value|
            if (value.bool) .never else .boundary
        else
            .boundary,
        .quantizer_seed = if (root.object.get("quantizer_seed")) |value|
            std.math.cast(u64, value.integer) orelse return error.InvalidIndexConfig
        else
            42,
        .use_random_ortho_trans = if (root.object.get("use_random_ortho_trans")) |value|
            value.bool
        else
            false,
        .max_cached_nodes = if (root.object.get("max_cached_nodes")) |value|
            std.math.cast(usize, value.integer) orelse return error.InvalidIndexConfig
        else
            100_000,
        .max_cached_vectors = if (root.object.get("max_cached_vectors")) |value|
            std.math.cast(usize, value.integer) orelse return error.InvalidIndexConfig
        else
            100_000,
        .max_cached_metadata = if (root.object.get("max_cached_metadata")) |value|
            std.math.cast(usize, value.integer) orelse return error.InvalidIndexConfig
        else
            100_000,
        .lazy_posting_maintenance = if (root.object.get("lazy_posting_maintenance")) |value|
            value.bool
        else
            false,
        .auto_posting_maintenance_max_postings = if (root.object.get("auto_posting_maintenance_max_postings")) |value|
            std.math.cast(usize, value.integer) orelse return error.InvalidIndexConfig
        else
            0,
        .centroid_directory_mode = if (root.object.get("centroid_directory_mode")) |value|
            try parseCentroidDirectoryMode(value.string)
        else
            .hbc,
        .flat_centroid_block_size = if (root.object.get("flat_centroid_block_size")) |value|
            std.math.cast(usize, value.integer) orelse return error.InvalidIndexConfig
        else
            8192,
        .flat_centroid_probe_count = if (root.object.get("flat_centroid_probe_count")) |value|
            std.math.cast(usize, value.integer) orelse return error.InvalidIndexConfig
        else
            0,
    };
}

fn parseTextConfig(alloc: Allocator, raw: []const u8) !TextConfig {
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();
    const root = parsed.value;
    if (root != .object) return error.InvalidIndexConfig;

    return .{
        .source_artifact_name = if (root.object.get("artifact_name")) |value|
            try alloc.dupe(u8, value.string)
        else if (root.object.get("chunk_name")) |value|
            try alloc.dupe(u8, value.string)
        else
            null,
    };
}

fn parseTextAnalysisForTextIndex(
    alloc: Allocator,
    raw: []const u8,
    runtime_schema: ?schema_mod.TableSchema,
) !introducer_mod.TextAnalysisConfig {
    var cfg = try introducer_mod.parseTextAnalysisConfig(alloc, raw);
    errdefer introducer_mod.freeTextAnalysisConfig(alloc, cfg);

    if (runtime_schema) |schema| {
        try appendSchemaFieldAnalyzers(alloc, &cfg, schema);
    }
    return cfg;
}

fn openTextPersistentIndexWithRetry(
    alloc: Allocator,
    opts: persistent_mod.PersistentIndexOptions,
) !persistent_mod.PersistentIndex {
    const max_attempts: usize = 6;
    const debug_open = std.c.getenv("ANTFLY_LSM_OPEN_DEBUG") != null;
    var attempt: usize = 0;
    while (true) : (attempt += 1) {
        if (debug_open) {
            std.log.info(
                "full_text persistent open begin attempt={d} path={s} main_backend={s} wal_backend={s} main_lsm_storage={any} wal_storage={any} read_only={any} main_read_only={any} wal_read_only={any}",
                .{
                    attempt + 1,
                    std.mem.span(opts.path),
                    @tagName(opts.main_backend),
                    @tagName(opts.resolvedWalBackend()),
                    opts.main_lsm_storage != null,
                    opts.wal_storage != null,
                    opts.read_only,
                    opts.main_lsm_options.backend.read_only,
                    opts.wal_lsm_options.backend.read_only,
                },
            );
        }
        return persistent_mod.PersistentIndex.open(alloc, opts) catch |err| {
            std.log.warn("full_text persistent open attempt failed attempt={d} path={s} err={s}", .{
                attempt + 1,
                std.mem.span(opts.path),
                @errorName(err),
            });
            if (!isTransientTextPersistentOpenError(err) or attempt + 1 >= max_attempts) return err;
            sleepBeforeTextPersistentOpenRetry(attempt);
            continue;
        };
    }
}

fn isTransientTextPersistentOpenError(err: anyerror) bool {
    return switch (err) {
        error.CorruptInput,
        error.InvalidTableFile,
        error.NotFound,
        => true,
        else => false,
    };
}

fn sleepBeforeTextPersistentOpenRetry(attempt: usize) void {
    const capped = @min(attempt, 5);
    const delay_ns: u64 = (@as(u64, 5) << @intCast(capped)) * std.time.ns_per_ms;
    if (comptime builtin.os.tag != .freestanding) {
        var req = std.posix.timespec{
            .sec = @intCast(delay_ns / std.time.ns_per_s),
            .nsec = @intCast(delay_ns % std.time.ns_per_s),
        };
        while (true) switch (std.posix.errno(std.posix.system.nanosleep(&req, &req))) {
            .SUCCESS => return,
            .INTR => continue,
            else => return,
        };
    } else {
        const spins = 64 * (capped + 1);
        for (0..spins) |_| std.atomic.spinLoopHint();
    }
}

fn appendSchemaFieldAnalyzers(
    alloc: Allocator,
    cfg: *introducer_mod.TextAnalysisConfig,
    schema: schema_mod.TableSchema,
) !void {
    const FieldAnalyzer = std.meta.Child(@TypeOf(cfg.field_analyzers));
    var extra_count: usize = 0;
    for (schema.full_text_documents) |doc| {
        for (doc.fields) |field| {
            if (std.mem.eql(u8, field.emitted_name, "_all")) continue;
            extra_count += 1;
        }
    }
    if (extra_count == 0) return;

    const original_len = cfg.field_analyzers.len;
    const combined = try alloc.alloc(FieldAnalyzer, original_len + extra_count);
    var initialized: usize = 0;
    errdefer {
        for (combined[original_len..initialized]) |item| {
            alloc.free(item.field_name);
            alloc.free(item.analyzer_name);
        }
        alloc.free(combined);
    }

    for (cfg.field_analyzers, 0..) |item, i| combined[i] = item;
    initialized = original_len;
    for (schema.full_text_documents) |doc| {
        for (doc.fields) |field| {
            if (std.mem.eql(u8, field.emitted_name, "_all")) continue;
            combined[initialized] = .{
                .field_name = try alloc.dupe(u8, field.emitted_name),
                .analyzer_name = try alloc.dupe(u8, field.analyzer),
            };
            initialized += 1;
        }
    }

    if (cfg.field_analyzers.len > 0) alloc.free(cfg.field_analyzers);
    cfg.field_analyzers = combined;
}

fn appendObservedFieldAnalyzers(
    alloc: Allocator,
    cfg: *introducer_mod.TextAnalysisConfig,
    observed: []const mapper.ObservedFieldAnalyzer,
) !void {
    if (observed.len == 0) return;

    const FieldAnalyzer = std.meta.Child(@TypeOf(cfg.field_analyzers));
    const original_len = cfg.field_analyzers.len;
    const combined = try alloc.alloc(FieldAnalyzer, original_len + observed.len);
    var initialized: usize = 0;
    errdefer {
        for (combined[original_len..initialized]) |item| {
            alloc.free(item.field_name);
            alloc.free(item.analyzer_name);
        }
        alloc.free(combined);
    }

    for (cfg.field_analyzers, 0..) |item, i| combined[i] = item;
    initialized = original_len;
    for (observed) |item| {
        combined[initialized] = .{
            .field_name = try alloc.dupe(u8, item.field_name),
            .analyzer_name = try alloc.dupe(u8, item.analyzer_name),
        };
        initialized += 1;
    }

    if (cfg.field_analyzers.len > 0) alloc.free(cfg.field_analyzers);
    cfg.field_analyzers = combined;
}

fn mergeObservedTextFieldAnalyzers(
    self: *IndexManager,
    store: *docstore_mod.DocStore,
    entry: *IndexManager.TextIndex,
    observed: []const mapper.ObservedFieldAnalyzer,
) !void {
    if (observed.len == 0) return;

    var additions = std.ArrayListUnmanaged(mapper.ObservedFieldAnalyzer).empty;
    defer {
        for (additions.items) |item| {
            self.alloc.free(item.field_name);
            self.alloc.free(item.analyzer_name);
        }
        additions.deinit(self.alloc);
    }

    for (observed) |item| {
        if (containsObservedFieldAnalyzer(entry.observed_field_analyzers, item.field_name, item.analyzer_name)) continue;
        try additions.append(self.alloc, .{
            .field_name = try self.alloc.dupe(u8, item.field_name),
            .analyzer_name = try self.alloc.dupe(u8, item.analyzer_name),
        });
    }
    if (additions.items.len == 0) return;

    const original_len = entry.observed_field_analyzers.len;
    const expanded = try self.alloc.realloc(entry.observed_field_analyzers, original_len + additions.items.len);
    for (additions.items, 0..) |item, i| {
        expanded[original_len + i] = .{
            .field_name = try self.alloc.dupe(u8, item.field_name),
            .analyzer_name = try self.alloc.dupe(u8, item.analyzer_name),
        };
    }
    entry.observed_field_analyzers = expanded;

    try appendObservedFieldAnalyzers(self.alloc, &entry.text_analysis, additions.items);
    try saveObservedTextFieldAnalyzers(store, self.alloc, entry.config.name, entry.observed_field_analyzers);
    try publishFullTextDictionaryRegistry(store, self.alloc, entry.config.name, entry.text_analysis);
}

fn containsObservedFieldAnalyzer(
    observed: []const mapper.ObservedFieldAnalyzer,
    field_name: []const u8,
    analyzer_name: []const u8,
) bool {
    for (observed) |item| {
        if (std.mem.eql(u8, item.field_name, field_name) and std.mem.eql(u8, item.analyzer_name, analyzer_name)) return true;
    }
    return false;
}

fn appendUniqueProjectionPath(alloc: Allocator, items: *std.ArrayListUnmanaged([]const u8), path: []const u8) !void {
    for (items.items) |existing| {
        if (std.mem.eql(u8, existing, path)) return;
    }
    try items.append(alloc, path);
}

fn loadRuntimeSchemaForTextIndex(alloc: Allocator, store: anytype, index_name: []const u8) !?schema_mod.TableSchema {
    const active_schema = (try schema_mod.loadSchema(store, alloc)) orelse return null;
    const explicit_version = textIndexSchemaVersion(index_name);
    if (explicit_version == null or explicit_version.? == active_schema.version) return active_schema;

    defer schema_mod.freeSchema(alloc, active_schema);
    return try schema_mod.loadSchemaVersion(store, alloc, explicit_version.?);
}

fn textIndexSchemaVersion(index_name: []const u8) ?u32 {
    if (std.mem.eql(u8, index_name, "default") or std.mem.eql(u8, index_name, "full_text_index")) return 0;
    const prefix = "full_text_index_v";
    if (!std.mem.startsWith(u8, index_name, prefix)) return null;
    return std.fmt.parseInt(u32, index_name[prefix.len..], 10) catch null;
}

fn parseSparseConfig(alloc: Allocator, raw: []const u8) !SparseConfig {
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();
    const root = parsed.value;
    if (root != .object) return error.InvalidIndexConfig;

    const field = root.object.get("field") orelse return error.InvalidIndexConfig;
    return .{
        .field_name = try alloc.dupe(u8, field.string),
    };
}

fn parseDenseGeneratorConfig(alloc: Allocator, raw: []const u8) !?GeneratorConfig {
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();
    const root = parsed.value;
    if (root != .object) return error.InvalidIndexConfig;

    const generator = root.object.get("generator") orelse return null;
    if (generator != .object) return error.InvalidIndexConfig;

    if (generator.object.get("kind")) |kind| {
        if (kind != .string) return error.InvalidIndexConfig;
        if (!std.mem.eql(u8, kind.string, "dense_embedding") and !std.mem.eql(u8, kind.string, "embedding")) {
            return null;
        }
    }

    const source_field = generator.object.get("source_field") orelse return error.InvalidIndexConfig;
    if (source_field != .string) return error.InvalidIndexConfig;
    const artifact_value = generator.object.get("artifact_name");
    const chunk_name_value = generator.object.get("chunk_name");

    return .{
        .source_field = try alloc.dupe(u8, source_field.string),
        .source_template = if (generator.object.get("source_template")) |value|
            if (value == .string and value.string.len > 0) try alloc.dupe(u8, value.string) else &.{}
        else
            &.{},
        .artifact_name = if (chunk_name_value) |value|
            try alloc.dupe(u8, value.string)
        else if (artifact_value) |value|
            try alloc.dupe(u8, value.string)
        else
            try alloc.dupe(u8, source_field.string),
        .embedding_name = if (generator.object.get("embedding_name")) |value|
            try alloc.dupe(u8, value.string)
        else
            null,
        .chunk_size = if (generator.object.get("chunk_size")) |value|
            std.math.cast(u32, value.integer) orelse return error.InvalidIndexConfig
        else
            0,
        .chunk_overlap = if (generator.object.get("chunk_overlap")) |value|
            std.math.cast(u32, value.integer) orelse return error.InvalidIndexConfig
        else
            0,
        .chunker_json = try parseGeneratorChunkerJson(alloc, generator.object),
    };
}

fn parseSparseGeneratorConfig(alloc: Allocator, raw: []const u8) !?GeneratorConfig {
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();
    const root = parsed.value;
    if (root != .object) return error.InvalidIndexConfig;

    const generator = root.object.get("generator") orelse return null;
    if (generator != .object) return error.InvalidIndexConfig;

    if (generator.object.get("kind")) |kind| {
        if (kind != .string) return error.InvalidIndexConfig;
        if (!std.mem.eql(u8, kind.string, "sparse_embedding")) {
            return null;
        }
    }

    const source_field = generator.object.get("source_field") orelse return error.InvalidIndexConfig;
    if (source_field != .string) return error.InvalidIndexConfig;
    const artifact_value = generator.object.get("artifact_name");
    const chunk_name_value = generator.object.get("chunk_name");

    return .{
        .source_field = try alloc.dupe(u8, source_field.string),
        .source_template = if (generator.object.get("source_template")) |value|
            if (value == .string and value.string.len > 0) try alloc.dupe(u8, value.string) else &.{}
        else
            &.{},
        .artifact_name = if (chunk_name_value) |value|
            try alloc.dupe(u8, value.string)
        else if (artifact_value) |value|
            try alloc.dupe(u8, value.string)
        else
            try alloc.dupe(u8, source_field.string),
        .embedding_name = if (generator.object.get("embedding_name")) |value|
            try alloc.dupe(u8, value.string)
        else
            null,
        .chunk_size = if (generator.object.get("chunk_size")) |value|
            std.math.cast(u32, value.integer) orelse return error.InvalidIndexConfig
        else
            0,
        .chunk_overlap = if (generator.object.get("chunk_overlap")) |value|
            std.math.cast(u32, value.integer) orelse return error.InvalidIndexConfig
        else
            0,
        .chunker_json = try parseGeneratorChunkerJson(alloc, generator.object),
    };
}

fn hasExplicitDenseEmbedding(embeddings: []const mapper.DenseEmbeddingWrite, index_name: []const u8) bool {
    for (embeddings) |embedding| {
        if (std.mem.eql(u8, embedding.index_name, index_name)) return true;
    }
    return false;
}

fn hasExplicitSparseEmbedding(embeddings: []const mapper.SparseEmbeddingWrite, index_name: []const u8) bool {
    for (embeddings) |embedding| {
        if (std.mem.eql(u8, embedding.index_name, index_name)) return true;
    }
    return false;
}

fn appendDenseEmbeddingToExtractedWrite(
    alloc: Allocator,
    extracted: *mapper.ExtractedWrite,
    embedding: mapper.DenseEmbeddingWrite,
) !void {
    const old = extracted.dense_embeddings;
    const next = try alloc.alloc(mapper.DenseEmbeddingWrite, old.len + 1);
    @memcpy(next[0..old.len], old);
    next[old.len] = embedding;
    if (old.len > 0) alloc.free(old);
    extracted.dense_embeddings = next;
}

fn appendSparseEmbeddingToExtractedWrite(
    alloc: Allocator,
    extracted: *mapper.ExtractedWrite,
    embedding: mapper.SparseEmbeddingWrite,
) !void {
    const old = extracted.sparse_embeddings;
    const next = try alloc.alloc(mapper.SparseEmbeddingWrite, old.len + 1);
    @memcpy(next[0..old.len], old);
    next[old.len] = embedding;
    if (old.len > 0) alloc.free(old);
    extracted.sparse_embeddings = next;
}

fn containsOwnedString(items: []const []const u8, value: []const u8) bool {
    for (items) |item| {
        if (std.mem.eql(u8, item, value)) return true;
    }
    return false;
}

fn hasGeneratedChunkRequest(
    requests: []const enrichment_types.GeneratedEnrichmentRequest,
    doc_key: []const u8,
    source_field: []const u8,
    source_template: []const u8,
    artifact_name: []const u8,
) bool {
    for (requests) |request| {
        if (request.kind != .chunk_text) continue;
        if (!std.mem.eql(u8, request.doc_key, doc_key)) continue;
        if (!std.mem.eql(u8, request.source_field, source_field)) continue;
        if (!std.mem.eql(u8, request.source_template, source_template)) continue;
        if (!std.mem.eql(u8, request.artifact_name, artifact_name)) continue;
        return true;
    }
    return false;
}

fn hasGeneratedDenseEmbeddingRequest(
    requests: []const enrichment_types.GeneratedEnrichmentRequest,
    doc_key: []const u8,
    source_field: []const u8,
    source_template: []const u8,
    artifact_name: []const u8,
    embedding_name: []const u8,
) bool {
    for (requests) |request| {
        if (request.kind != .dense_embedding) continue;
        if (!std.mem.eql(u8, request.doc_key, doc_key)) continue;
        if (!std.mem.eql(u8, request.source_field, source_field)) continue;
        if (!std.mem.eql(u8, request.source_template, source_template)) continue;
        if (!std.mem.eql(u8, request.artifact_name, artifact_name)) continue;
        if (!std.mem.eql(u8, request.embedding_name, embedding_name)) continue;
        return true;
    }
    return false;
}

fn resolveChunkGenerator(self: *const IndexManager, generator: GeneratorConfig) GeneratorConfig {
    if (self.getEnrichment(.chunk, generator.artifact_name)) |cfg| {
        return .{
            .source_field = @constCast(cfg.source_field),
            .source_template = if (cfg.source_template.len > 0) @constCast(cfg.source_template) else &.{},
            .artifact_name = @constCast(cfg.name),
            .embedding_name = generator.embedding_name,
            .chunk_size = cfg.chunk_size,
            .chunk_overlap = cfg.chunk_overlap,
            .chunker_json = if (cfg.chunker_json.len > 0) @constCast(cfg.chunker_json) else &.{},
        };
    }
    return generator;
}

fn generatorHasChunking(generator: GeneratorConfig) bool {
    return generator.chunk_size > 0 or generator.chunker_json.len > 0;
}

fn parseGeneratorChunkerJson(alloc: Allocator, object: std.json.ObjectMap) ![]u8 {
    const chunker_value = object.get("chunker") orelse return &.{};
    var cfg = try chunking_types.parseConfigFromValue(alloc, chunker_value);
    defer cfg.deinit(alloc);
    return try chunking_types.stringifyAlloc(alloc, cfg);
}

fn parseGraphConfig(alloc: Allocator, raw: []const u8) !GraphConfig {
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();
    const root = parsed.value;
    if (root != .object) return error.InvalidIndexConfig;
    const algebraic_semiring_traversal = try parseGraphAlgebraicSemiringTraversal(root);
    var artifact_source = try parseGraphArtifactSource(alloc, root);
    errdefer if (artifact_source) |*source| {
        source.deinit(alloc);
    };
    var shorthand_asset = try parseGraphShorthandAsset(alloc, root);
    errdefer if (shorthand_asset) |*asset| {
        asset.deinit(alloc);
    };

    const edge_types = root.object.get("edge_types") orelse {
        return .{
            .edge_type_configs = try alloc.alloc(graph_mod.EdgeTypeConfig, 0),
            .artifact_source = artifact_source,
            .shorthand_asset = shorthand_asset,
            .algebraic_semiring_traversal = algebraic_semiring_traversal,
        };
    };
    if (edge_types != .array) return error.InvalidIndexConfig;

    const configs = try alloc.alloc(graph_mod.EdgeTypeConfig, edge_types.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (configs[0..initialized]) |cfg| {
            alloc.free(cfg.name);
            if (cfg.field_name) |field_name| alloc.free(field_name);
        }
        alloc.free(configs);
    }

    for (edge_types.array.items, 0..) |item, i| {
        if (item != .object) return error.InvalidIndexConfig;
        const name = item.object.get("name") orelse return error.InvalidIndexConfig;
        if (name != .string) return error.InvalidIndexConfig;

        const topology = if (item.object.get("topology")) |value| blk: {
            if (value != .string) return error.InvalidIndexConfig;
            if (std.mem.eql(u8, value.string, "graph")) break :blk graph_mod.TopologyMode.graph;
            if (std.mem.eql(u8, value.string, "tree")) break :blk graph_mod.TopologyMode.tree;
            return error.InvalidIndexConfig;
        } else graph_mod.TopologyMode.graph;

        const field_name = if (item.object.get("field")) |value| blk: {
            if (artifact_source != null) return error.InvalidIndexConfig;
            if (value != .string) return error.InvalidIndexConfig;
            break :blk try alloc.dupe(u8, value.string);
        } else null;

        configs[i] = .{
            .name = try alloc.dupe(u8, name.string),
            .field_name = field_name,
            .topology = topology,
        };
        initialized += 1;
    }

    return .{
        .edge_type_configs = configs,
        .artifact_source = artifact_source,
        .shorthand_asset = shorthand_asset,
        .algebraic_semiring_traversal = algebraic_semiring_traversal,
    };
}

fn parseGraphArtifactSource(alloc: Allocator, root: std.json.Value) !?GraphArtifactSource {
    const source = root.object.get("source") orelse return null;
    if (source != .object) return error.InvalidIndexConfig;
    const kind = source.object.get("kind") orelse return error.InvalidIndexConfig;
    if (kind != .string) return error.InvalidIndexConfig;
    if (std.mem.eql(u8, kind.string, "document_field")) {
        const field = source.object.get("field") orelse return error.InvalidIndexConfig;
        if (field != .string or field.string.len == 0) return error.InvalidIndexConfig;
        if (!std.mem.eql(u8, field.string, "_edges")) return error.InvalidIndexConfig;
        return null;
    }
    if (!std.mem.eql(u8, kind.string, "artifact")) return error.InvalidIndexConfig;

    const artifact = source.object.get("artifact") orelse return error.InvalidIndexConfig;
    if (artifact != .string or artifact.string.len == 0) return error.InvalidIndexConfig;
    const path = if (source.object.get("path")) |value| blk: {
        if (value != .string) return error.InvalidIndexConfig;
        try validateGraphArtifactPath(value.string);
        break :blk value.string;
    } else "";
    const format = if (source.object.get("format")) |value| blk: {
        if (value != .string) return error.InvalidIndexConfig;
        if (std.mem.eql(u8, value.string, "extraction_relation")) break :blk GraphArtifactFormat.extraction_relation;
        if (std.mem.eql(u8, value.string, "extraction_graph")) break :blk GraphArtifactFormat.extraction_graph;
        return error.InvalidIndexConfig;
    } else GraphArtifactFormat.extraction_relation;

    var out = GraphArtifactSource{
        .artifact_name = try alloc.dupe(u8, artifact.string),
        .path = if (path.len > 0) try alloc.dupe(u8, path) else "",
        .format = format,
    };
    errdefer out.deinit(alloc);
    out.mapping = try parseGraphArtifactMapping(alloc, root);
    return out;
}

fn validateGraphArtifactPath(path: []const u8) !void {
    if (path.len == 0 or std.mem.eql(u8, path, "$")) return;
    if (!std.mem.startsWith(u8, path, "$.")) return error.InvalidIndexConfig;
    var trimmed = path[2..];
    if (std.mem.endsWith(u8, trimmed, "[*]")) trimmed = trimmed[0 .. trimmed.len - 3];
    if (trimmed.len == 0) return error.InvalidIndexConfig;
    var parts = std.mem.splitScalar(u8, trimmed, '.');
    while (parts.next()) |part| {
        if (part.len == 0) return error.InvalidIndexConfig;
        for (part) |ch| {
            if (!(std.ascii.isAlphanumeric(ch) or ch == '_')) return error.InvalidIndexConfig;
        }
    }
}

fn parseGraphArtifactMapping(alloc: Allocator, root: std.json.Value) !GraphArtifactMapping {
    var mapping = GraphArtifactMapping{};
    errdefer mapping.deinit(alloc);

    if (root.object.get("nodes")) |nodes| {
        if (nodes != .object) return error.InvalidIndexConfig;
        if (nodes.object.get("model")) |model| {
            if (model != .string) return error.InvalidIndexConfig;
            if (std.mem.eql(u8, model.string, "document")) {
                mapping.node_model = .document;
            } else if (std.mem.eql(u8, model.string, "external")) {
                mapping.node_model = .external;
            } else {
                return error.InvalidIndexConfig;
            }
        }
        mapping.source_template = try parseOptionalGraphTemplate(alloc, nodes, "source");
        mapping.target_template = try parseOptionalGraphTemplate(alloc, nodes, "target");
    }

    if (root.object.get("edge")) |edge| {
        if (edge != .object) return error.InvalidIndexConfig;
        mapping.edge_type_template = try parseOptionalGraphTemplate(alloc, edge, "type");
        mapping.weight_template = try parseOptionalGraphTemplate(alloc, edge, "weight");
        if (edge.object.get("metadata")) |metadata| {
            mapping.metadata_template_json = try std.json.Stringify.valueAlloc(alloc, metadata, .{});
        }
    }

    if (root.object.get("context")) |context| {
        if (context != .object) return error.InvalidIndexConfig;
        mapping.context_doc_fields = try parseGraphContextDocFields(alloc, context);
    }

    try validateGraphMappingTemplates(mapping);
    return mapping;
}

fn parseOptionalGraphTemplate(alloc: Allocator, parent: std.json.Value, name: []const u8) ![]u8 {
    const value = parent.object.get(name) orelse return "";
    return switch (value) {
        .string => |text| if (text.len > 0) try alloc.dupe(u8, text) else "",
        .integer, .float, .number_string => try std.json.Stringify.valueAlloc(alloc, value, .{}),
        else => error.InvalidIndexConfig,
    };
}

fn parseGraphContextDocFields(alloc: Allocator, context: std.json.Value) ![]const []u8 {
    const value = context.object.get("doc_fields") orelse return &.{};
    if (value != .array) return error.InvalidIndexConfig;
    const fields = try alloc.alloc([]u8, value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (fields[0..initialized]) |field| alloc.free(field);
        alloc.free(fields);
    }
    for (value.array.items, 0..) |item, i| {
        if (item != .string or item.string.len == 0) return error.InvalidIndexConfig;
        fields[i] = try alloc.dupe(u8, item.string);
        initialized += 1;
    }
    return fields;
}

fn validateGraphMappingTemplates(mapping: GraphArtifactMapping) !void {
    try validateGraphMaterializedSourceTemplate(mapping.source_template);
    try validateGraphTemplateDocFields(mapping.source_template, mapping.context_doc_fields);
    try validateGraphTemplateDocFields(mapping.target_template, mapping.context_doc_fields);
    try validateGraphTemplateDocFields(mapping.edge_type_template, mapping.context_doc_fields);
    try validateGraphTemplateDocFields(mapping.weight_template, mapping.context_doc_fields);
    try validateGraphTemplateDocFields(mapping.metadata_template_json, mapping.context_doc_fields);
}

fn validateGraphMaterializedSourceTemplate(template_source: []const u8) !void {
    const trimmed = std.mem.trim(u8, template_source, &std.ascii.whitespace);
    if (trimmed.len == 0) return;
    if (!std.mem.startsWith(u8, trimmed, "{{") or !std.mem.endsWith(u8, trimmed, "}}")) return error.InvalidIndexConfig;
    const expr = std.mem.trim(u8, trimmed[2 .. trimmed.len - 2], &std.ascii.whitespace);
    if (!std.mem.eql(u8, expr, "_doc.key")) return error.InvalidIndexConfig;
}

fn validateGraphTemplateDocFields(template_source: []const u8, declared_fields: []const []u8) !void {
    var pos: usize = 0;
    while (std.mem.indexOfPos(u8, template_source, pos, "_doc.value.")) |start| {
        const field_start = start + "_doc.value.".len;
        var field_end = field_start;
        while (field_end < template_source.len) : (field_end += 1) {
            const ch = template_source[field_end];
            if (!(std.ascii.isAlphanumeric(ch) or ch == '_')) break;
        }
        if (field_end == field_start) return error.InvalidIndexConfig;
        const field = template_source[field_start..field_end];
        if (!graphContextFieldDeclared(field, declared_fields)) return error.InvalidIndexConfig;
        pos = field_end;
    }
}

fn graphContextFieldDeclared(field: []const u8, declared_fields: []const []u8) bool {
    for (declared_fields) |declared| {
        if (std.mem.eql(u8, declared, field)) return true;
    }
    return false;
}

fn parseGraphShorthandAsset(alloc: Allocator, root: std.json.Value) !?enrichment_catalog.EnrichmentConfig {
    const artifact = root.object.get("artifact") orelse return null;
    if (artifact != .object) return error.InvalidIndexConfig;

    const name = artifact.object.get("name") orelse return error.InvalidIndexConfig;
    if (name != .string or name.string.len == 0) return error.InvalidIndexConfig;
    const kind = artifact.object.get("kind") orelse return error.InvalidIndexConfig;
    if (kind != .string or !std.mem.eql(u8, kind.string, "asset")) return error.InvalidIndexConfig;

    const field = if (artifact.object.get("field")) |value| blk: {
        if (value != .string) return error.InvalidIndexConfig;
        break :blk value.string;
    } else "";
    const template = if (artifact.object.get("template")) |value| blk: {
        if (value != .string) return error.InvalidIndexConfig;
        break :blk value.string;
    } else "";
    if (field.len == 0 and template.len == 0) return error.InvalidIndexConfig;
    const content_type = if (artifact.object.get("content_type")) |value| blk: {
        if (value != .string) return error.InvalidIndexConfig;
        break :blk value.string;
    } else "";
    const producer_json = if (artifact.object.get("producer_json")) |value|
        try std.json.Stringify.valueAlloc(alloc, value, .{})
    else
        "";
    errdefer if (producer_json.len > 0) alloc.free(producer_json);

    return .{
        .name = try alloc.dupe(u8, name.string),
        .kind = .asset,
        .source_field = if (field.len > 0) try alloc.dupe(u8, field) else "",
        .source_template = if (template.len > 0) try alloc.dupe(u8, template) else "",
        .content_type = if (content_type.len > 0) try alloc.dupe(u8, content_type) else "",
        .producer_json = producer_json,
    };
}

fn parseGraphAlgebraicSemiringTraversal(root: std.json.Value) !bool {
    const planning = root.object.get("algebraic_planning") orelse return false;
    if (planning != .object) return error.InvalidIndexConfig;
    const bounded = planning.object.get("bounded_traversal") orelse return false;
    if (bounded != .object) return error.InvalidIndexConfig;
    const law = bounded.object.get("law") orelse return error.InvalidIndexConfig;
    if (law != .string or !std.mem.eql(u8, law.string, "provenance_semiring")) return error.InvalidIndexConfig;
    if (bounded.object.get("enabled")) |enabled| {
        if (enabled != .bool) return error.InvalidIndexConfig;
        return enabled.bool;
    }
    return true;
}

test "graph config declares algebraic provenance semiring traversal law" {
    const alloc = std.testing.allocator;
    var cfg = try parseGraphConfig(alloc,
        \\{"algebraic_planning":{"bounded_traversal":{"law":"provenance_semiring"}}}
    );
    defer cfg.deinit(alloc);
    try std.testing.expect(cfg.algebraic_semiring_traversal);
    try std.testing.expectEqual(@as(usize, 0), cfg.edge_type_configs.len);

    var disabled = try parseGraphConfig(alloc,
        \\{"algebraic_planning":{"bounded_traversal":{"law":"provenance_semiring","enabled":false}}}
    );
    defer disabled.deinit(alloc);
    try std.testing.expect(!disabled.algebraic_semiring_traversal);

    try std.testing.expectError(error.InvalidIndexConfig, parseGraphConfig(alloc,
        \\{"algebraic_planning":{"bounded_traversal":{"law":"min_plus_semiring"}}}
    ));
}

test "graph config parses artifact source and shorthand asset enrichment" {
    const alloc = std.testing.allocator;
    var cfg = try parseGraphConfig(alloc,
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","field":"body","content_type":"application/json","producer_json":{"type":"extractor","config":{"provider":"antfly"}}}
        \\}
    );
    defer cfg.deinit(alloc);

    try std.testing.expect(cfg.artifact_source != null);
    try std.testing.expectEqualStrings("relations_v1", cfg.artifact_source.?.artifact_name);
    try std.testing.expectEqualStrings("$.relations[*]", cfg.artifact_source.?.path);
    try std.testing.expectEqual(GraphArtifactFormat.extraction_relation, cfg.artifact_source.?.format);
    try std.testing.expect(cfg.shorthand_asset != null);
    try std.testing.expectEqualStrings("relations_v1", cfg.shorthand_asset.?.name);
    try std.testing.expectEqual(enrichment_catalog.EnrichmentType.asset, cfg.shorthand_asset.?.kind);
    try std.testing.expectEqualStrings("body", cfg.shorthand_asset.?.source_field);
    try std.testing.expectEqualStrings("application/json", cfg.shorthand_asset.?.content_type);
    try std.testing.expect(std.mem.indexOf(u8, cfg.shorthand_asset.?.producer_json, "\"type\":\"extractor\"") != null);
}

test "graph config parses artifact mapping templates and context fields" {
    const alloc = std.testing.allocator;
    var cfg = try parseGraphConfig(alloc,
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_v1","path":"$.items[*]","format":"extraction_relation"},
        \\  "nodes":{"model":"document","source":"{{ _doc.key }}","target":"{{ _item.to }}"},
        \\  "edge":{"type":"{{ _item.rel }}","weight":"{{ default _item.score 1.0 }}","metadata":{"evidence":"{{ _item.evidence }}","tenant":"{{ _doc.value.tenant_id }}"}},
        \\  "context":{"doc_fields":["tenant_id"]}
        \\}
    );
    defer cfg.deinit(alloc);

    const mapping = cfg.artifact_source.?.mapping;
    try std.testing.expectEqual(GraphNodeModel.document, mapping.node_model);
    try std.testing.expectEqualStrings("{{ _doc.key }}", mapping.source_template);
    try std.testing.expectEqualStrings("{{ _item.to }}", mapping.target_template);
    try std.testing.expectEqualStrings("{{ _item.rel }}", mapping.edge_type_template);
    try std.testing.expectEqualStrings("{{ default _item.score 1.0 }}", mapping.weight_template);
    try std.testing.expectEqual(@as(usize, 1), mapping.context_doc_fields.len);
    try std.testing.expectEqualStrings("tenant_id", mapping.context_doc_fields[0]);
    try std.testing.expect(std.mem.indexOf(u8, mapping.metadata_template_json, "_item.evidence") != null);
}

test "graph config rejects undeclared doc value template fields and unsupported paths" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(error.InvalidIndexConfig, parseGraphConfig(alloc,
        \\{"source":{"kind":"artifact","artifact":"relations_v1"},"edge":{"type":"{{ _doc.value.tenant_id }}"}}
    ));
    try std.testing.expectError(error.InvalidIndexConfig, parseGraphConfig(alloc,
        \\{"source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[0]"}}
    ));
    try std.testing.expectError(error.InvalidIndexConfig, parseGraphConfig(alloc,
        \\{"source":{"kind":"artifact","artifact":"relations_v1"},"nodes":{"source":"{{ _item.source.document_id }}"}}
    ));
}

test "graph config rejects artifact source combined with document field edge types" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(error.InvalidIndexConfig, parseGraphConfig(alloc,
        \\{"source":{"kind":"artifact","artifact":"relations_v1"},"edge_types":[{"name":"mentions","field":"edges"}]}
    ));
}

test "graph config validates document field source shape" {
    const alloc = std.testing.allocator;
    var cfg = try parseGraphConfig(alloc,
        \\{"source":{"kind":"document_field","field":"_edges"}}
    );
    defer cfg.deinit(alloc);
    try std.testing.expect(cfg.artifact_source == null);

    try std.testing.expectError(error.InvalidIndexConfig, parseGraphConfig(alloc,
        \\{"source":{"kind":"document_field"}}
    ));
    try std.testing.expectError(error.InvalidIndexConfig, parseGraphConfig(alloc,
        \\{"source":{"kind":"document_field","field":"links"}}
    ));
}

fn parseMetric(raw: []const u8) !vector_mod.DistanceMetric {
    if (std.mem.eql(u8, raw, "l2_squared")) return .l2_squared;
    if (std.mem.eql(u8, raw, "cosine")) return .cosine;
    if (std.mem.eql(u8, raw, "inner_product")) return .inner_product;
    return error.InvalidIndexConfig;
}

fn parseClustAlgorithm(raw: []const u8) !vector_mod.ClustAlgorithm {
    if (std.mem.eql(u8, raw, "kmeans")) return .kmeans;
    if (std.mem.eql(u8, raw, "hilbert")) return .hilbert;
    return error.InvalidIndexConfig;
}

fn parseBulkBuildAlgo(raw: []const u8) !hbc_mod.BulkBuildAlgo {
    if (std.mem.eql(u8, raw, "recursive")) return .recursive;
    if (std.mem.eql(u8, raw, "hilbert_seeded")) return .hilbert_seeded;
    if (std.mem.eql(u8, raw, "doc_key_seeded")) return .doc_key_seeded;
    if (std.mem.eql(u8, raw, "kmeans")) return .kmeans;
    return error.InvalidIndexConfig;
}

fn parseKmeansBackend(raw: []const u8) !hbc_mod.HBCConfig.KmeansBackend {
    if (std.mem.eql(u8, raw, "auto")) return .auto;
    if (std.mem.eql(u8, raw, "cpu")) return .cpu;
    if (std.mem.eql(u8, raw, "metal")) return .metal;
    return error.InvalidIndexConfig;
}

fn parseKmeansUpdateStrategy(raw: []const u8) !hbc_mod.HBCConfig.KmeansUpdateStrategy {
    if (std.mem.eql(u8, raw, "auto")) return .auto;
    if (std.mem.eql(u8, raw, "scatter")) return .scatter;
    if (std.mem.eql(u8, raw, "segmented")) return .segmented;
    if (std.mem.eql(u8, raw, "metal")) return .metal;
    return error.InvalidIndexConfig;
}

fn parseDenseRerankPolicy(raw: []const u8) !hbc_mod.HBCConfig.RerankPolicy {
    if (std.mem.eql(u8, raw, "always")) return .always;
    if (std.mem.eql(u8, raw, "boundary")) return .boundary;
    if (std.mem.eql(u8, raw, "never")) return .never;
    return error.InvalidIndexConfig;
}

fn parseCentroidDirectoryMode(raw: []const u8) !hbc_mod.HBCConfig.CentroidDirectoryMode {
    if (std.mem.eql(u8, raw, "hbc")) return .hbc;
    if (std.mem.eql(u8, raw, "flat_rabitq")) return .flat_rabitq;
    return error.InvalidIndexConfig;
}

fn ensureIndexDir(alloc: Allocator, base_path: []const u8, path: []const u8) !void {
    const parent_path = try std.fmt.allocPrint(alloc, "{s}/indexes", .{base_path});
    defer alloc.free(parent_path);

    if (builtin.os.tag != .freestanding) {
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        try fs_paths.createDirPathPortable(io_impl.io(), parent_path);
        try fs_paths.createDirPathPortable(io_impl.io(), path);
        const wal_path = try std.fmt.allocPrint(alloc, "{s}/wal", .{path});
        defer alloc.free(wal_path);
        try fs_paths.createDirPathPortable(io_impl.io(), wal_path);
    }
}

fn deleteIndexDirIfPresent(path: []const u8) void {
    if (builtin.os.tag == .freestanding) return;

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
}

fn denseDocMappingKey(alloc: Allocator, index_name: []const u8, doc_key: []const u8) ![]u8 {
    return try denseMetadataKeyAlloc(alloc, index_name, "doc", &.{doc_key}, &.{});
}

fn denseVectorIdMappingKey(alloc: Allocator, index_name: []const u8, vector_id: u64) ![]u8 {
    var id_buf: [@sizeOf(u64)]u8 = undefined;
    std.mem.writeInt(u64, &id_buf, vector_id, .big);
    return try denseMetadataKeyAlloc(alloc, index_name, "vector", &.{}, &.{&id_buf});
}

fn denseOrdinalMappingKey(alloc: Allocator, index_name: []const u8, ordinal: doc_identity.DocOrdinal) ![]u8 {
    var ordinal_buf: [@sizeOf(doc_identity.DocOrdinal)]u8 = undefined;
    std.mem.writeInt(doc_identity.DocOrdinal, &ordinal_buf, ordinal, .big);
    return try denseMetadataKeyAlloc(alloc, index_name, "ordinal", &.{}, &.{&ordinal_buf});
}

fn denseOrdinalMemberPrefix(alloc: Allocator, index_name: []const u8, ordinal: doc_identity.DocOrdinal) ![]u8 {
    var ordinal_buf: [@sizeOf(doc_identity.DocOrdinal)]u8 = undefined;
    std.mem.writeInt(doc_identity.DocOrdinal, &ordinal_buf, ordinal, .big);
    return try denseMetadataKeyAlloc(alloc, index_name, "ordinal_member", &.{}, &.{&ordinal_buf});
}

fn denseOrdinalMemberKey(alloc: Allocator, index_name: []const u8, ordinal: doc_identity.DocOrdinal, vector_id: u64) ![]u8 {
    var ordinal_buf: [@sizeOf(doc_identity.DocOrdinal)]u8 = undefined;
    var id_buf: [@sizeOf(u64)]u8 = undefined;
    std.mem.writeInt(doc_identity.DocOrdinal, &ordinal_buf, ordinal, .big);
    std.mem.writeInt(u64, &id_buf, vector_id, .big);
    return try denseMetadataKeyAlloc(alloc, index_name, "ordinal_member", &.{}, &.{ &ordinal_buf, &id_buf });
}

fn denseVectorOrdinalMappingKey(alloc: Allocator, index_name: []const u8, vector_id: u64) ![]u8 {
    var id_buf: [@sizeOf(u64)]u8 = undefined;
    std.mem.writeInt(u64, &id_buf, vector_id, .big);
    return try denseMetadataKeyAlloc(alloc, index_name, "vector_ordinal", &.{}, &.{&id_buf});
}

fn denseNextIdKey(alloc: Allocator, index_name: []const u8) ![]u8 {
    return try denseMetadataKeyAlloc(alloc, index_name, "next_id", &.{}, &.{});
}

fn denseIndexMetadataPrefixAlloc(alloc: Allocator, index_name: []const u8) ![]u8 {
    var out = try denseMetadataPrefixAlloc(alloc, index_name, null);
    errdefer out.deinit(alloc);
    return try out.toOwnedSlice(alloc);
}

fn denseMetadataKeyAlloc(
    alloc: Allocator,
    index_name: []const u8,
    kind: []const u8,
    encoded_components: []const []const u8,
    fixed_components: []const []const u8,
) ![]u8 {
    var out = try denseMetadataPrefixAlloc(alloc, index_name, kind);
    errdefer out.deinit(alloc);
    for (encoded_components) |component| {
        try internal_keys.appendEncodedComponent(&out, alloc, component);
    }
    for (fixed_components) |component| {
        try out.appendSlice(alloc, component);
    }
    return try out.toOwnedSlice(alloc);
}

fn denseMetadataPrefixAlloc(alloc: Allocator, index_name: []const u8, kind: ?[]const u8) !std.ArrayListUnmanaged(u8) {
    const prefix = "\x00\x00__metadata__:dense2:";
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, prefix);
    try internal_keys.appendEncodedComponent(&out, alloc, index_name);
    if (kind) |kind_name| try internal_keys.appendEncodedComponent(&out, alloc, kind_name);
    return out;
}

fn legacyDenseIndexMetadataPrefixAlloc(alloc: Allocator, index_name: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "\x00\x00__metadata__:dense:{s}:", .{index_name});
}

fn legacyDenseDocMappingKey(alloc: Allocator, index_name: []const u8, doc_key: []const u8) ![]u8 {
    const prefix = "\x00\x00__metadata__:dense:";
    const infix = ":doc:";
    const total_len = prefix.len + index_name.len + infix.len + internal_keys.encodedComponentLen(doc_key);
    const out = try alloc.alloc(u8, total_len);
    var pos: usize = 0;
    @memcpy(out[pos..][0..prefix.len], prefix);
    pos += prefix.len;
    @memcpy(out[pos..][0..index_name.len], index_name);
    pos += index_name.len;
    @memcpy(out[pos..][0..infix.len], infix);
    pos += infix.len;
    _ = internal_keys.encodeComponent(out[pos..], doc_key);
    return out;
}

fn legacyDenseVectorIdMappingKey(alloc: Allocator, index_name: []const u8, vector_id: u64) ![]u8 {
    return std.fmt.allocPrint(alloc, "\x00\x00__metadata__:dense:{s}:vector:{d}", .{ index_name, vector_id });
}

fn legacyDenseOrdinalMappingKey(alloc: Allocator, index_name: []const u8, ordinal: doc_identity.DocOrdinal) ![]u8 {
    return std.fmt.allocPrint(alloc, "\x00\x00__metadata__:dense:{s}:ordinal:{d}", .{ index_name, ordinal });
}

fn legacyDenseOrdinalMemberPrefix(alloc: Allocator, index_name: []const u8, ordinal: doc_identity.DocOrdinal) ![]u8 {
    return std.fmt.allocPrint(alloc, "\x00\x00__metadata__:dense:{s}:ordinal_member:{d}:", .{ index_name, ordinal });
}

fn legacyDenseOrdinalMemberKey(alloc: Allocator, index_name: []const u8, ordinal: doc_identity.DocOrdinal, vector_id: u64) ![]u8 {
    return std.fmt.allocPrint(alloc, "\x00\x00__metadata__:dense:{s}:ordinal_member:{d}:{d}", .{ index_name, ordinal, vector_id });
}

fn legacyDenseVectorOrdinalMappingKey(alloc: Allocator, index_name: []const u8, vector_id: u64) ![]u8 {
    return std.fmt.allocPrint(alloc, "\x00\x00__metadata__:dense:{s}:vector_ordinal:{d}", .{ index_name, vector_id });
}

fn legacyDenseNextIdKey(alloc: Allocator, index_name: []const u8) ![]u8 {
    return std.fmt.allocPrint(alloc, "\x00\x00__metadata__:dense:{s}:next_id", .{index_name});
}

test "dense metadata keys preserve embedded index separators" {
    const alloc = std.testing.allocator;

    const parent_prefix = try denseOrdinalMemberPrefix(alloc, "idx", 7);
    defer alloc.free(parent_prefix);
    const child_key = try denseOrdinalMemberKey(alloc, "idx:ordinal_member", 7, 11);
    defer alloc.free(child_key);
    try std.testing.expect(!std.mem.startsWith(u8, child_key, parent_prefix));

    const parent_delete_prefix = try denseIndexMetadataPrefixAlloc(alloc, "idx");
    defer alloc.free(parent_delete_prefix);
    const child_next = try denseNextIdKey(alloc, "idx:next_id");
    defer alloc.free(child_next);
    try std.testing.expect(!std.mem.startsWith(u8, child_next, parent_delete_prefix));
}

test "dense metadata lookups read legacy textual rows" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);

    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();

    var manager = try IndexManager.init(alloc, path);
    defer manager.deinit();

    const index_name = "semantic_idx";
    const doc_key = "doc:legacy";
    const vector_id: u64 = 77;
    const ordinal: doc_identity.DocOrdinal = 5;

    const doc_map_key = try legacyDenseDocMappingKey(alloc, index_name, doc_key);
    defer alloc.free(doc_map_key);
    const vector_map_key = try legacyDenseVectorIdMappingKey(alloc, index_name, vector_id);
    defer alloc.free(vector_map_key);
    const ordinal_map_key = try legacyDenseOrdinalMappingKey(alloc, index_name, ordinal);
    defer alloc.free(ordinal_map_key);
    const ordinal_member_key = try legacyDenseOrdinalMemberKey(alloc, index_name, ordinal, vector_id);
    defer alloc.free(ordinal_member_key);
    const vector_ordinal_key = try legacyDenseVectorOrdinalMappingKey(alloc, index_name, vector_id);
    defer alloc.free(vector_ordinal_key);
    const next_id_key = try legacyDenseNextIdKey(alloc, index_name);
    defer alloc.free(next_id_key);

    var vector_buf: [@sizeOf(u64)]u8 = undefined;
    std.mem.writeInt(u64, &vector_buf, vector_id, .little);
    var ordinal_buf: [@sizeOf(doc_identity.DocOrdinal)]u8 = undefined;
    std.mem.writeInt(doc_identity.DocOrdinal, &ordinal_buf, ordinal, .little);
    var next_buf: [@sizeOf(u64)]u8 = undefined;
    std.mem.writeInt(u64, &next_buf, 99, .little);
    try store.putBatch(&.{
        .{ .key = doc_map_key, .value = &vector_buf },
        .{ .key = vector_map_key, .value = doc_key },
        .{ .key = ordinal_map_key, .value = &vector_buf },
        .{ .key = ordinal_member_key, .value = &vector_buf },
        .{ .key = vector_ordinal_key, .value = &ordinal_buf },
        .{ .key = next_id_key, .value = &next_buf },
    }, &.{});

    var read_txn = try store.beginProbeTxn();
    defer read_txn.abort();
    try std.testing.expectEqual(@as(?u64, vector_id), try manager.lookupDenseVectorIdTxn(&read_txn, index_name, doc_key));
    const mapped_doc = (try manager.lookupDenseDocKeyByVectorIdTxn(&read_txn, index_name, vector_id)) orelse return error.TestUnexpectedResult;
    defer alloc.free(mapped_doc);
    try std.testing.expectEqualStrings(doc_key, mapped_doc);
    try std.testing.expectEqual(@as(?u64, vector_id), try manager.lookupDenseVectorIdByOrdinalTxn(&read_txn, index_name, ordinal));
    try std.testing.expectEqual(@as(?doc_identity.DocOrdinal, ordinal), try manager.lookupDenseVectorOrdinalTxn(&read_txn, index_name, vector_id));

    var vector_ids = std.ArrayListUnmanaged(u64).empty;
    defer vector_ids.deinit(alloc);
    var runtime_store = try initRuntimeStore(alloc, &store);
    defer runtime_store.deinit();
    try manager.appendDenseVectorIdsForOrdinalAlloc(alloc, &vector_ids, &runtime_store.store, index_name, ordinal);
    try std.testing.expectEqual(@as(usize, 1), vector_ids.items.len);
    try std.testing.expectEqual(vector_id, vector_ids.items[0]);

    var batch = try store.beginWriteBatch();
    errdefer batch.abort();
    const write_txn = batch.asTxn();
    try std.testing.expectEqual(@as(u64, 99), try manager.reserveDenseVectorIdTxn(write_txn, index_name));
    try batch.commit();
}

fn deterministicDenseVectorId(doc_key: []const u8) u64 {
    const id = std.hash.XxHash64.hash(0, doc_key);
    return if (id == 0) 1 else id;
}

fn docOrdinalLessThan(_: void, lhs: doc_identity.DocOrdinal, rhs: doc_identity.DocOrdinal) bool {
    return lhs < rhs;
}

fn uniqueSortedDocOrdinals(items: []doc_identity.DocOrdinal) usize {
    if (items.len == 0) return 0;
    var write: usize = 1;
    var previous = items[0];
    for (items[1..]) |item| {
        if (item == previous) continue;
        items[write] = item;
        write += 1;
        previous = item;
    }
    return write;
}

fn containsU64(items: []const u64, id: u64) bool {
    for (items) |item| {
        if (item == id) return true;
    }
    return false;
}

fn containsU32(items: []const u32, id: u32) bool {
    for (items) |item| {
        if (item == id) return true;
    }
    return false;
}

fn textFieldAnalyzersKey(alloc: Allocator, index_name: []const u8) ![]u8 {
    return std.fmt.allocPrint(alloc, "{s}{s}", .{ text_field_analyzers_prefix, index_name });
}

fn saveObservedTextFieldAnalyzers(store: anytype, alloc: Allocator, index_name: []const u8, observed: []const mapper.ObservedFieldAnalyzer) !void {
    const key = try textFieldAnalyzersKey(alloc, index_name);
    defer alloc.free(key);
    const data = try serializeObservedTextFieldAnalyzers(alloc, observed);
    defer alloc.free(data);

    var runtime = try initRuntimeStore(alloc, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginWrite();
    errdefer txn.abort();
    try txn.put(key, data);
    try txn.commit();
}

fn publishFullTextDictionaryRegistry(store: anytype, alloc: Allocator, index_name: []const u8, text_analysis: introducer_mod.TextAnalysisConfig) !void {
    if (text_analysis.field_analyzers.len == 0) return;

    var runtime = try initRuntimeStore(alloc, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginWrite();
    errdefer txn.abort();

    for (text_analysis.field_analyzers) |item| {
        if (item.field_name.len == 0 or item.analyzer_name.len == 0) continue;
        const identity = algebraic_mod.lexical.DictionaryIdentity.analyzedText(index_name, item.field_name, item.analyzer_name);
        switch (try algebraic_mod.lexical.claimRegistryOwnerTxn(alloc, &txn, identity, index_name, .fst_postings, "ready")) {
            .claimed, .already_owned => {},
            .owned_by_other => return error.InvalidIndexConfig,
        }
    }

    try txn.commit();
}

fn loadObservedTextFieldAnalyzers(alloc: Allocator, store: anytype, index_name: []const u8) ![]mapper.ObservedFieldAnalyzer {
    const key = try textFieldAnalyzersKey(alloc, index_name);
    defer alloc.free(key);

    var runtime = try initRuntimeStore(alloc, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginRead();
    defer txn.abort();
    const raw = txn.get(key) catch |err| switch (err) {
        error.NotFound => return &.{},
        else => return err,
    };
    return try deserializeObservedTextFieldAnalyzers(alloc, raw);
}

fn serializeObservedTextFieldAnalyzers(alloc: Allocator, observed: []const mapper.ObservedFieldAnalyzer) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    try out.appendSlice(alloc, "ATFA");
    try appendU32(&out, alloc, 1);
    try appendU32(&out, alloc, @intCast(observed.len));
    for (observed) |item| {
        try appendStr(&out, alloc, item.field_name);
        try appendStr(&out, alloc, item.analyzer_name);
    }

    const owned = try alloc.dupe(u8, out.items);
    out.deinit(alloc);
    return owned;
}

fn deserializeObservedTextFieldAnalyzers(alloc: Allocator, data: []const u8) ![]mapper.ObservedFieldAnalyzer {
    if (data.len < 12 or !std.mem.eql(u8, data[0..4], "ATFA")) return error.InvalidIndexCatalog;

    var pos: usize = 4;
    const version = try readU32(data, &pos);
    if (version != 1) return error.UnsupportedIndexCatalogVersion;

    const count = try readU32(data, &pos);
    const observed = try alloc.alloc(mapper.ObservedFieldAnalyzer, count);
    var initialized: usize = 0;
    errdefer {
        for (observed[0..initialized]) |item| {
            alloc.free(item.field_name);
            alloc.free(item.analyzer_name);
        }
        alloc.free(observed);
    }

    for (0..count) |i| {
        observed[i] = .{
            .field_name = try alloc.dupe(u8, try readStr(data, &pos)),
            .analyzer_name = try alloc.dupe(u8, try readStr(data, &pos)),
        };
        initialized += 1;
    }
    return observed;
}

fn freeObservedTextFieldAnalyzers(alloc: Allocator, observed: []const mapper.ObservedFieldAnalyzer) void {
    for (observed) |item| {
        alloc.free(item.field_name);
        alloc.free(item.analyzer_name);
    }
    if (observed.len > 0) alloc.free(observed);
}

const RuntimeStoreHandle = struct {
    store: backend_erased.Store,
    owned: bool,

    fn deinit(self: *@This()) void {
        if (self.owned) self.store.deinit();
    }
};

fn initRuntimeStore(alloc: Allocator, store: anytype) !RuntimeStoreHandle {
    const T = @TypeOf(store);
    if (T == backend_erased.Store) return .{ .store = store, .owned = false };
    if (T == *backend_erased.Store) return .{ .store = store.*, .owned = false };

    switch (@typeInfo(T)) {
        .pointer => |ptr| {
            if (@typeInfo(ptr.child) == .@"struct" and @hasDecl(ptr.child, "backendStore")) {
                return .{
                    .store = try backend_erased.storeFrom(alloc, store.backendStore()),
                    .owned = true,
                };
            }
        },
        .@"struct" => {
            if (@hasDecl(T, "backendStore")) {
                return .{
                    .store = try backend_erased.storeFrom(alloc, store.backendStore()),
                    .owned = true,
                };
            }
        },
        else => {},
    }

    return .{
        .store = try backend_erased.storeFrom(alloc, store),
        .owned = true,
    };
}

fn nowNs() u64 {
    return platform_time.monotonicNs();
}

fn elapsedSince(start_ns: u64) u64 {
    return nowNs() - start_ns;
}

var open_profile_enabled_cache: std.atomic.Value(u8) = .init(0);

const IndexManagerSimAction = index_manager_sim_fixture.Action;
const IndexManagerSimDocSpec = index_manager_sim_fixture.DocSpec;
const IndexManagerSimCrashOutcome = index_manager_sim_fixture.CrashOutcome;
var index_manager_tmp_nonce: u64 = 0;

fn nextIndexManagerTmpNonce() u64 {
    return @atomicRmw(u64, &index_manager_tmp_nonce, .Add, 1, .seq_cst);
}

const index_manager_sim_index_name = "ft_v1";
const index_manager_sim_split_key = "doc:m";

const IndexManagerSimSummary = struct {
    source_doc_count: u32 = 0,
    dest_doc_count: u32 = 0,
    source_alpha_hits: u32 = 0,
    source_beta_hits: u32 = 0,
    source_gamma_hits: u32 = 0,
    dest_alpha_hits: u32 = 0,
    dest_beta_hits: u32 = 0,
    dest_gamma_hits: u32 = 0,
};

const IndexManagerTerm = enum {
    alpha,
    beta,
    gamma,
};

const IndexManagerExpectedDoc = struct {
    side: enum { source, dest },
    term: IndexManagerTerm,
};

const IndexManagerOwnedWrite = struct {
    key: []u8,
    value: []u8,
    term: IndexManagerTerm,

    fn deinit(self: *IndexManagerOwnedWrite, alloc: Allocator) void {
        alloc.free(self.key);
        alloc.free(self.value);
        self.* = undefined;
    }
};

const IndexManagerSimRuntime = struct {
    alloc: Allocator,
    source_path: [*:0]const u8,
    dest_path: [*:0]const u8,
    source_store: docstore_mod.DocStore,
    dest_store: docstore_mod.DocStore,
    source_store_open: bool,
    dest_store_open: bool,
    source_manager: IndexManager,
    dest_manager: IndexManager,
    source_manager_open: bool,
    dest_manager_open: bool,
    backend_options: db_config.IndexBackendOptions,
    split_active: bool,

    fn init(alloc: Allocator, source_path: [*:0]const u8, dest_path: [*:0]const u8) !IndexManagerSimRuntime {
        return try initWithOptions(alloc, source_path, dest_path, .{
            .text_main_backend = .lmdb,
            .dense_storage_backend = .lmdb,
            .graph_reverse_backend = .lmdb,
        });
    }

    fn initWithOptions(
        alloc: Allocator,
        source_path: [*:0]const u8,
        dest_path: [*:0]const u8,
        backend_options: db_config.IndexBackendOptions,
    ) !IndexManagerSimRuntime {
        var runtime = IndexManagerSimRuntime{
            .alloc = alloc,
            .source_path = source_path,
            .dest_path = dest_path,
            .source_store = undefined,
            .dest_store = undefined,
            .source_store_open = false,
            .dest_store_open = false,
            .source_manager = undefined,
            .dest_manager = undefined,
            .source_manager_open = false,
            .dest_manager_open = false,
            .backend_options = backend_options,
            .split_active = false,
        };
        runtime.source_store = try docstore_mod.DocStore.open(alloc, source_path, .{});
        runtime.source_store_open = true;
        errdefer if (runtime.source_store_open) runtime.source_store.close();

        runtime.dest_store = try docstore_mod.DocStore.open(alloc, dest_path, .{});
        runtime.dest_store_open = true;
        errdefer if (runtime.dest_store_open) runtime.dest_store.close();

        runtime.source_manager = try IndexManager.initWithOptions(alloc, std.mem.span(source_path), backend_options);
        runtime.source_manager_open = true;
        errdefer if (runtime.source_manager_open) runtime.source_manager.deinit();
        runtime.dest_manager = try IndexManager.initWithOptions(alloc, std.mem.span(dest_path), backend_options);
        runtime.dest_manager_open = true;
        errdefer if (runtime.dest_manager_open) runtime.dest_manager.deinit();

        try runtime.source_manager.addAllNoBackfill(&runtime.source_store, &.{indexManagerSimTextConfig()});
        try runtime.dest_manager.addAllNoBackfill(&runtime.dest_store, &.{indexManagerSimTextConfig()});
        runtime.updateRanges();
        return runtime;
    }

    fn deinit(self: *IndexManagerSimRuntime) void {
        if (self.source_manager_open) {
            self.source_manager.deinit();
            self.source_manager_open = false;
        }
        if (self.dest_manager_open) {
            self.dest_manager.deinit();
            self.dest_manager_open = false;
        }
        if (self.source_store_open) {
            self.source_store.close();
            self.source_store_open = false;
        }
        if (self.dest_store_open) {
            self.dest_store.close();
            self.dest_store_open = false;
        }
        self.* = undefined;
    }

    fn reopen(self: *IndexManagerSimRuntime) !void {
        if (self.source_manager_open) {
            self.source_manager.deinit();
            self.source_manager_open = false;
        }
        if (self.dest_manager_open) {
            self.dest_manager.deinit();
            self.dest_manager_open = false;
        }
        if (self.source_store_open) {
            self.source_store.close();
            self.source_store_open = false;
        }
        if (self.dest_store_open) {
            self.dest_store.close();
            self.dest_store_open = false;
        }

        self.source_store = try docstore_mod.DocStore.open(self.alloc, self.source_path, .{});
        self.source_store_open = true;
        errdefer {
            if (self.source_store_open) {
                self.source_store.close();
                self.source_store_open = false;
            }
        }
        self.dest_store = try docstore_mod.DocStore.open(self.alloc, self.dest_path, .{});
        self.dest_store_open = true;
        errdefer {
            if (self.dest_store_open) {
                self.dest_store.close();
                self.dest_store_open = false;
            }
        }

        self.source_manager = try IndexManager.initWithOptions(self.alloc, std.mem.span(self.source_path), self.backend_options);
        self.source_manager_open = true;
        errdefer {
            if (self.source_manager_open) {
                self.source_manager.deinit();
                self.source_manager_open = false;
            }
        }
        self.dest_manager = try IndexManager.initWithOptions(self.alloc, std.mem.span(self.dest_path), self.backend_options);
        self.dest_manager_open = true;
        errdefer {
            if (self.dest_manager_open) {
                self.dest_manager.deinit();
                self.dest_manager_open = false;
            }
        }

        try self.source_manager.load(&self.source_store);
        try self.dest_manager.load(&self.dest_store);
        self.updateRanges();
    }

    fn updateRanges(self: *IndexManagerSimRuntime) void {
        if (self.split_active) {
            self.source_manager.updateRange(.{ .start = "", .end = index_manager_sim_split_key });
            self.dest_manager.updateRange(.{ .start = index_manager_sim_split_key, .end = "" });
        } else {
            self.source_manager.updateRange(.{ .start = "", .end = "" });
            self.dest_manager.updateRange(.{ .start = index_manager_sim_split_key, .end = "" });
        }
    }

    fn applyReplayAction(self: *IndexManagerSimRuntime, action: IndexManagerSimAction, step: usize) !void {
        switch (action) {
            .reopen => try self.reopen(),
            .split_handoff => try self.applySplitHandoff(),
            .add_doc => |spec| try self.applyWrite(spec, step),
        }
    }

    fn applyCrashAction(self: *IndexManagerSimRuntime, action: IndexManagerSimAction, step: usize, phase: lmdb.CommitPublishPhase) !void {
        switch (action) {
            .add_doc => |spec| try self.applyWriteAtPhase(spec, step, phase),
            else => return error.InvalidFixture,
        }
    }

    fn applySplitHandoff(self: *IndexManagerSimRuntime) !void {
        if (self.split_active) return error.InvalidFixture;

        const text_handoffs = try self.dest_manager.handoffRightOnlyTextSegmentsFrom(&self.source_manager, index_manager_sim_split_key, false);
        defer {
            for (text_handoffs) |*handoff| handoff.deinit(self.alloc);
            self.alloc.free(text_handoffs);
        }

        try self.source_manager.pruneTextSplitRange(index_manager_sim_split_key);
        self.split_active = true;
        self.updateRanges();
    }

    fn applyWrite(self: *IndexManagerSimRuntime, spec: IndexManagerSimDocSpec, step: usize) !void {
        const writes = try buildIndexManagerWrites(self.alloc, spec, step);
        defer {
            for (writes) |*write| write.deinit(self.alloc);
            self.alloc.free(writes);
        }
        try self.routeAndApplyWrites(writes, false, null);
    }

    fn applyWriteAtPhase(self: *IndexManagerSimRuntime, spec: IndexManagerSimDocSpec, step: usize, phase: lmdb.CommitPublishPhase) !void {
        const writes = try buildIndexManagerWrites(self.alloc, spec, step);
        defer {
            for (writes) |*write| write.deinit(self.alloc);
            self.alloc.free(writes);
        }
        try self.routeAndApplyWrites(writes, true, phase);
    }

    fn routeAndApplyWrites(
        self: *IndexManagerSimRuntime,
        writes: []const IndexManagerOwnedWrite,
        crash_phase: bool,
        phase: ?lmdb.CommitPublishPhase,
    ) !void {
        var source_store_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
        defer source_store_writes.deinit(self.alloc);
        var source_index_writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
        defer source_index_writes.deinit(self.alloc);
        var dest_store_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
        defer dest_store_writes.deinit(self.alloc);
        var dest_index_writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
        defer dest_index_writes.deinit(self.alloc);

        for (writes) |write| {
            if (self.split_active and std.mem.order(u8, write.key, index_manager_sim_split_key) != .lt) {
                try dest_store_writes.append(self.alloc, .{ .key = write.key, .value = write.value });
                try dest_index_writes.append(self.alloc, .{ .key = write.key, .value = write.value });
            } else {
                try source_store_writes.append(self.alloc, .{ .key = write.key, .value = write.value });
                try source_index_writes.append(self.alloc, .{ .key = write.key, .value = write.value });
            }
        }

        try self.source_store.putBatch(source_store_writes.items, &.{});
        try self.dest_store.putBatch(dest_store_writes.items, &.{});

        if (crash_phase) {
            const crash_publish_phase = phase orelse return error.InvalidFixture;
            if (source_index_writes.items.len != 0 and dest_index_writes.items.len != 0) return error.InvalidFixture;
            if (source_index_writes.items.len != 0) {
                const entry = self.source_manager.textIndexEntry(index_manager_sim_index_name) orelse return error.IndexNotFound;
                try self.indexTextBatchEntryAtPhaseForTest(&self.source_manager, entry, source_index_writes.items, crash_publish_phase);
                return;
            }
            if (dest_index_writes.items.len != 0) {
                const entry = self.dest_manager.textIndexEntry(index_manager_sim_index_name) orelse return error.IndexNotFound;
                try self.indexTextBatchEntryAtPhaseForTest(&self.dest_manager, entry, dest_index_writes.items, crash_publish_phase);
            }
            return;
        }

        try self.source_manager.indexTextBatchByName(&self.source_store, index_manager_sim_index_name, source_index_writes.items);
        try self.dest_manager.indexTextBatchByName(&self.dest_store, index_manager_sim_index_name, dest_index_writes.items);
    }

    fn indexTextBatchEntryAtPhaseForTest(
        self: *IndexManagerSimRuntime,
        manager: *IndexManager,
        entry: *IndexManager.TextIndex,
        writes: []const types.BatchWrite,
        phase: lmdb.CommitPublishPhase,
    ) !void {
        var filtered_count: usize = 0;
        for (writes) |write| {
            if (!try textIndexShouldConsumeDoc(manager, entry, write.key)) continue;
            filtered_count += 1;
        }
        if (filtered_count == 0) return;

        const docs = try self.alloc.alloc(mapper.MapperDoc, filtered_count);
        defer self.alloc.free(docs);

        var mapped_idx: usize = 0;
        for (writes) |write| {
            if (!try textIndexShouldConsumeDoc(manager, entry, write.key)) continue;
            docs[mapped_idx] = .{
                .key = write.key,
                .value = write.value,
            };
            mapped_idx += 1;
        }

        const segment = try mapper.buildTextSegmentFromDocuments(self.alloc, docs, entry.text_analysis, entry.runtime_schema);
        if (segment) |seg| {
            defer self.alloc.free(seg);
            try persistent_mod.indexSegmentPublishPhaseForTest(&entry.persistent, seg, phase);
        }
    }

    fn summary(self: *IndexManagerSimRuntime, alloc: Allocator) !IndexManagerSimSummary {
        return .{
            .source_doc_count = self.source_manager.textIndex(index_manager_sim_index_name).?.snapshot().global_doc_count,
            .dest_doc_count = self.dest_manager.textIndex(index_manager_sim_index_name).?.snapshot().global_doc_count,
            .source_alpha_hits = try self.source_manager.textIndex(index_manager_sim_index_name).?.snapshot().termDocFreq(alloc, "title", "alpha"),
            .source_beta_hits = try self.source_manager.textIndex(index_manager_sim_index_name).?.snapshot().termDocFreq(alloc, "title", "beta"),
            .source_gamma_hits = try self.source_manager.textIndex(index_manager_sim_index_name).?.snapshot().termDocFreq(alloc, "title", "gamma"),
            .dest_alpha_hits = try self.dest_manager.textIndex(index_manager_sim_index_name).?.snapshot().termDocFreq(alloc, "title", "alpha"),
            .dest_beta_hits = try self.dest_manager.textIndex(index_manager_sim_index_name).?.snapshot().termDocFreq(alloc, "title", "beta"),
            .dest_gamma_hits = try self.dest_manager.textIndex(index_manager_sim_index_name).?.snapshot().termDocFreq(alloc, "title", "gamma"),
        };
    }
};

fn indexManagerSimTextConfig() types.IndexConfig {
    return .{
        .name = index_manager_sim_index_name,
        .kind = .full_text,
        .config_json = "{}",
    };
}

fn buildIndexManagerWrites(alloc: Allocator, spec: IndexManagerSimDocSpec, step: usize) ![]IndexManagerOwnedWrite {
    var writes = std.ArrayListUnmanaged(IndexManagerOwnedWrite).empty;
    errdefer {
        for (writes.items) |*write| write.deinit(alloc);
        writes.deinit(alloc);
    }

    switch (spec) {
        .left_alpha => try writes.append(alloc, try indexManagerWrite(alloc, "doc:b", step, "alpha")),
        .left_gamma => try writes.append(alloc, try indexManagerWrite(alloc, "doc:c", step, "gamma")),
        .right_beta => try writes.append(alloc, try indexManagerWrite(alloc, "doc:z", step, "beta")),
        .mixed_alpha_beta => {
            try writes.append(alloc, try indexManagerWrite(alloc, "doc:b", step, "alpha"));
            try writes.append(alloc, try indexManagerWrite(alloc, "doc:z", step, "beta"));
        },
    }
    return writes.toOwnedSlice(alloc);
}

fn indexManagerWrite(alloc: Allocator, prefix: []const u8, step: usize, term: []const u8) !IndexManagerOwnedWrite {
    return .{
        .key = try std.fmt.allocPrint(alloc, "{s}:{d}", .{ prefix, step }),
        .value = try std.fmt.allocPrint(alloc, "{{\"title\":\"{s}\"}}", .{term}),
        .term = std.meta.stringToEnum(IndexManagerTerm, term) orelse unreachable,
    };
}

fn expectedIndexManagerSummary(actions: []const IndexManagerSimAction) !IndexManagerSimSummary {
    var docs = std.StringHashMapUnmanaged(IndexManagerExpectedDoc).empty;
    defer docs.deinit(std.testing.allocator);

    var split_active = false;
    for (actions, 0..) |action, step| {
        switch (action) {
            .reopen => {},
            .split_handoff => {
                if (split_active) return error.InvalidFixture;
                split_active = true;

                var it = docs.iterator();
                while (it.next()) |entry| {
                    entry.value_ptr.side = if (std.mem.order(u8, entry.key_ptr.*, index_manager_sim_split_key) == .lt) .source else .dest;
                }
            },
            .add_doc => |spec| {
                const writes = try buildIndexManagerWrites(std.testing.allocator, spec, step);
                defer {
                    for (writes) |*write| write.deinit(std.testing.allocator);
                    std.testing.allocator.free(writes);
                }

                for (writes) |write| {
                    const gop = try docs.getOrPut(std.testing.allocator, write.key);
                    if (!gop.found_existing) {
                        gop.key_ptr.* = try std.testing.allocator.dupe(u8, write.key);
                    }
                    gop.value_ptr.* = .{
                        .side = if (split_active and std.mem.order(u8, write.key, index_manager_sim_split_key) != .lt) .dest else .source,
                        .term = write.term,
                    };
                }
            },
        }
    }

    var summary = IndexManagerSimSummary{};
    var it = docs.iterator();
    while (it.next()) |entry| {
        switch (entry.value_ptr.side) {
            .source => {
                summary.source_doc_count += 1;
                switch (entry.value_ptr.term) {
                    .alpha => summary.source_alpha_hits += 1,
                    .beta => summary.source_beta_hits += 1,
                    .gamma => summary.source_gamma_hits += 1,
                }
            },
            .dest => {
                summary.dest_doc_count += 1;
                switch (entry.value_ptr.term) {
                    .alpha => summary.dest_alpha_hits += 1,
                    .beta => summary.dest_beta_hits += 1,
                    .gamma => summary.dest_gamma_hits += 1,
                }
            },
        }
    }

    var cleanup_it = docs.keyIterator();
    while (cleanup_it.next()) |key| std.testing.allocator.free(key.*);
    return summary;
}

fn expectIndexManagerSummaryEqual(case_label: []const u8, expected: IndexManagerSimSummary, actual: IndexManagerSimSummary) !void {
    try sim_fixture.expectFieldEqual(case_label, "source_doc_count", expected.source_doc_count, actual.source_doc_count);
    try sim_fixture.expectFieldEqual(case_label, "dest_doc_count", expected.dest_doc_count, actual.dest_doc_count);
    try sim_fixture.expectFieldEqual(case_label, "source_alpha_hits", expected.source_alpha_hits, actual.source_alpha_hits);
    try sim_fixture.expectFieldEqual(case_label, "source_beta_hits", expected.source_beta_hits, actual.source_beta_hits);
    try sim_fixture.expectFieldEqual(case_label, "source_gamma_hits", expected.source_gamma_hits, actual.source_gamma_hits);
    try sim_fixture.expectFieldEqual(case_label, "dest_alpha_hits", expected.dest_alpha_hits, actual.dest_alpha_hits);
    try sim_fixture.expectFieldEqual(case_label, "dest_beta_hits", expected.dest_beta_hits, actual.dest_beta_hits);
    try sim_fixture.expectFieldEqual(case_label, "dest_gamma_hits", expected.dest_gamma_hits, actual.dest_gamma_hits);
}

fn fixtureOptionsFromIndexManagerSummary(summary: IndexManagerSimSummary) index_manager_sim_fixture.Options {
    return .{
        .expected_source_doc_count = summary.source_doc_count,
        .expected_dest_doc_count = summary.dest_doc_count,
        .expected_source_alpha_hits = summary.source_alpha_hits,
        .expected_source_beta_hits = summary.source_beta_hits,
        .expected_source_gamma_hits = summary.source_gamma_hits,
        .expected_dest_alpha_hits = summary.dest_alpha_hits,
        .expected_dest_beta_hits = summary.dest_beta_hits,
        .expected_dest_gamma_hits = summary.dest_gamma_hits,
    };
}

fn indexManagerTmpPathWithSuffix(buf: []u8, suffix: []const u8) [*:0]const u8 {
    const base = "/tmp/antfly-index-manager-test-";
    const ts = platform_time.monotonicNs();
    const nonce = nextIndexManagerTmpNonce();
    const path = std.fmt.bufPrint(buf, "{s}{d}-{d}-{s}\x00", .{ base, ts, nonce, suffix }) catch unreachable;
    return @ptrCast(path.ptr);
}

fn indexManagerReplayArtifactPath(buf: []u8, suffix: []const u8) []const u8 {
    const base = "/tmp/antfly-index-manager-replay-";
    const ts = platform_time.monotonicNs();
    const nonce = nextIndexManagerTmpNonce();
    return std.fmt.bufPrint(buf, "{s}{d}-{d}-{s}.fixture", .{ base, ts, nonce, suffix }) catch unreachable;
}

fn cleanupIndexManagerDir(path: [*:0]const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}

fn writeIndexManagerReplayArtifactFile(path: []const u8, contents: []const u8) !void {
    var file = try std.Io.Dir.createFileAbsolute(std.testing.io, path, .{});
    defer file.close(std.testing.io);

    var file_buf: [4096]u8 = undefined;
    var writer = file.writer(std.testing.io, &file_buf);
    try writer.interface.writeAll(contents);
    try writer.end();
}

fn writeIndexManagerReplayFixtureArtifact(
    alloc: Allocator,
    case_label: []const u8,
    seed: u64,
    expectation_note: []const u8,
    summary: IndexManagerSimSummary,
    actions: []const IndexManagerSimAction,
) !?[]u8 {
    var path_buf: [256]u8 = undefined;
    const artifact_path = indexManagerReplayArtifactPath(&path_buf, case_label);
    const path = try alloc.dupe(u8, artifact_path);
    errdefer alloc.free(path);

    const normalized = try index_manager_sim_fixture.renderReplayArtifact(
        alloc,
        fixtureOptionsFromIndexManagerSummary(summary),
        case_label,
        seed,
        expectation_note,
        actions,
    );
    defer alloc.free(normalized);

    try writeIndexManagerReplayArtifactFile(path, normalized);
    return path;
}

fn writeIndexManagerCrashFixtureArtifact(
    alloc: Allocator,
    case_label: []const u8,
    seed: u64,
    phase: lmdb.CommitPublishPhase,
    expectation_note: []const u8,
    summary: IndexManagerSimSummary,
    prelude_actions: []const IndexManagerSimAction,
    crash_action: IndexManagerSimAction,
) !?[]u8 {
    var path_buf: [256]u8 = undefined;
    const artifact_path = indexManagerReplayArtifactPath(&path_buf, case_label);
    const path = try alloc.dupe(u8, artifact_path);
    errdefer alloc.free(path);

    var opts = fixtureOptionsFromIndexManagerSummary(summary);
    opts.expected_outcome = .committed;
    const normalized = try index_manager_sim_fixture.renderCrashArtifact(
        alloc,
        opts,
        case_label,
        seed,
        @tagName(phase),
        expectation_note,
        prelude_actions,
        crash_action,
    );
    defer alloc.free(normalized);

    try writeIndexManagerReplayArtifactFile(path, normalized);
    return path;
}

fn printIndexManagerAction(action: IndexManagerSimAction) !void {
    const line = try index_manager_sim_fixture.renderAction(std.testing.allocator, action);
    defer std.testing.allocator.free(line);
    std.debug.print("    {s}\n", .{line});
}

fn replayIndexManagerActionsAtPaths(
    alloc: Allocator,
    source_path: [*:0]const u8,
    dest_path: [*:0]const u8,
    actions: []const IndexManagerSimAction,
) !IndexManagerSimSummary {
    return try replayIndexManagerActionsAtPathsWithOptions(alloc, source_path, dest_path, .{
        .text_main_backend = .lmdb,
        .dense_storage_backend = .lmdb,
        .graph_reverse_backend = .lmdb,
    }, actions);
}

fn replayIndexManagerActionsAtPathsWithOptions(
    alloc: Allocator,
    source_path: [*:0]const u8,
    dest_path: [*:0]const u8,
    backend_options: db_config.IndexBackendOptions,
    actions: []const IndexManagerSimAction,
) !IndexManagerSimSummary {
    var runtime = try IndexManagerSimRuntime.initWithOptions(alloc, source_path, dest_path, backend_options);
    defer runtime.deinit();

    for (actions, 0..) |action, step| {
        try runtime.applyReplayAction(action, step);
    }
    try runtime.reopen();
    return try runtime.summary(alloc);
}

fn replayIndexManagerCrashWorkload(
    alloc: Allocator,
    case_label: []const u8,
    prelude_actions: []const IndexManagerSimAction,
    crash_action: IndexManagerSimAction,
    phase: lmdb.CommitPublishPhase,
) !IndexManagerSimCrashOutcome {
    _ = case_label;
    var source_expected_buf: [256]u8 = undefined;
    var dest_expected_buf: [256]u8 = undefined;
    const source_expected_path = indexManagerTmpPathWithSuffix(&source_expected_buf, "expected-src");
    const dest_expected_path = indexManagerTmpPathWithSuffix(&dest_expected_buf, "expected-dst");
    defer cleanupIndexManagerDir(source_expected_path);
    defer cleanupIndexManagerDir(dest_expected_path);

    var source_actual_buf: [256]u8 = undefined;
    var dest_actual_buf: [256]u8 = undefined;
    const source_actual_path = indexManagerTmpPathWithSuffix(&source_actual_buf, "actual-src");
    const dest_actual_path = indexManagerTmpPathWithSuffix(&dest_actual_buf, "actual-dst");
    defer cleanupIndexManagerDir(source_actual_path);
    defer cleanupIndexManagerDir(dest_actual_path);

    var expected_runtime = try IndexManagerSimRuntime.init(alloc, source_expected_path, dest_expected_path);
    defer expected_runtime.deinit();
    for (prelude_actions, 0..) |action, step| {
        try expected_runtime.applyReplayAction(action, step);
    }
    try expected_runtime.applyReplayAction(crash_action, prelude_actions.len);
    try expected_runtime.reopen();
    const expected = try expected_runtime.summary(alloc);

    var actual_runtime = try IndexManagerSimRuntime.init(alloc, source_actual_path, dest_actual_path);
    defer actual_runtime.deinit();
    for (prelude_actions, 0..) |action, step| {
        try actual_runtime.applyReplayAction(action, step);
    }
    try actual_runtime.applyCrashAction(crash_action, prelude_actions.len, phase);
    try actual_runtime.reopen();
    const actual = try actual_runtime.summary(alloc);

    try expectIndexManagerSummaryEqual("index-manager-crash", expected, actual);
    return .committed;
}

fn reportReducedIndexManagerSchedule(
    alloc: Allocator,
    case_label: []const u8,
    seed: u64,
    actions: []const IndexManagerSimAction,
) !void {
    const Replayer = struct {
        alloc: Allocator,
        case_label: []const u8,

        pub fn replay(self: @This(), candidate: []const IndexManagerSimAction) !void {
            var source_path_buf: [256]u8 = undefined;
            var dest_path_buf: [256]u8 = undefined;
            const source_path = indexManagerTmpPathWithSuffix(&source_path_buf, "reduce-src");
            const dest_path = indexManagerTmpPathWithSuffix(&dest_path_buf, "reduce-dst");
            defer cleanupIndexManagerDir(source_path);
            defer cleanupIndexManagerDir(dest_path);
            const actual = try replayIndexManagerActionsAtPaths(self.alloc, source_path, dest_path, candidate);
            try expectIndexManagerSummaryEqual(self.case_label, try expectedIndexManagerSummary(candidate), actual);
        }
    };

    const reduced = try zig_lmdb.sim.reduceFailingSequence(IndexManagerSimAction, alloc, actions, Replayer{
        .alloc = alloc,
        .case_label = case_label,
    });
    defer alloc.free(reduced);

    const summary = try expectedIndexManagerSummary(reduced);
    const artifact_path = writeIndexManagerReplayFixtureArtifact(
        alloc,
        case_label,
        seed,
        "expected index-manager replay to preserve text split routing and reopen semantics across source and destination managers",
        summary,
        reduced,
    ) catch |err| blk: {
        std.debug.print("failed to write index-manager replay artifact for {s}: {s}\n", .{ case_label, @errorName(err) });
        break :blk null;
    };
    defer if (artifact_path) |path| alloc.free(path);

    std.debug.print("reduced failing index-manager schedule ({d} actions):\n", .{reduced.len});
    if (artifact_path) |path| std.debug.print("replay fixture: {s}\n", .{path});
    for (reduced) |action| try printIndexManagerAction(action);
}

fn reportReducedIndexManagerCrashSchedule(
    alloc: Allocator,
    case_label: []const u8,
    seed: u64,
    phase: lmdb.CommitPublishPhase,
    prelude_actions: []const IndexManagerSimAction,
    crash_action: IndexManagerSimAction,
) !void {
    const Replayer = struct {
        alloc: Allocator,
        case_label: []const u8,
        phase: lmdb.CommitPublishPhase,
        crash_action: IndexManagerSimAction,

        pub fn replay(self: @This(), candidate: []const IndexManagerSimAction) !void {
            _ = try replayIndexManagerCrashWorkload(self.alloc, self.case_label, candidate, self.crash_action, self.phase);
        }
    };

    const reduced = try zig_lmdb.sim.reduceFailingSequence(IndexManagerSimAction, alloc, prelude_actions, Replayer{
        .alloc = alloc,
        .case_label = case_label,
        .phase = phase,
        .crash_action = crash_action,
    });
    defer alloc.free(reduced);

    const full_actions = try alloc.alloc(IndexManagerSimAction, reduced.len + 1);
    defer alloc.free(full_actions);
    @memcpy(full_actions[0..reduced.len], reduced);
    full_actions[reduced.len] = crash_action;
    const summary = try expectedIndexManagerSummary(full_actions);

    const artifact_path = writeIndexManagerCrashFixtureArtifact(
        alloc,
        case_label,
        seed,
        phase,
        "expected index-manager reopen to preserve the committed text batch once the underlying persistent WAL append has completed",
        summary,
        reduced,
        crash_action,
    ) catch |err| blk: {
        std.debug.print("failed to write index-manager crash artifact for {s}: {s}\n", .{ case_label, @errorName(err) });
        break :blk null;
    };
    defer if (artifact_path) |path| alloc.free(path);

    std.debug.print("reduced failing index-manager crash prelude ({d} actions):\n", .{reduced.len});
    if (artifact_path) |path| std.debug.print("replay fixture: {s}\n", .{path});
}

fn randomIndexManagerWriteSpec(random: std.Random) IndexManagerSimDocSpec {
    return @enumFromInt(random.uintLessThan(u8, 4));
}

fn runIndexManagerReplayCase(
    alloc: Allocator,
    case_label: []const u8,
    seed: u64,
    steps: usize,
) !void {
    var prng = std.Random.DefaultPrng.init(seed);
    const random = prng.random();

    var actions = std.ArrayListUnmanaged(IndexManagerSimAction).empty;
    defer actions.deinit(alloc);

    var split_active = false;
    var saw_write = false;
    for (0..steps) |_| {
        if (!split_active and saw_write and random.uintLessThan(u8, 5) == 0) {
            try actions.append(alloc, .split_handoff);
            split_active = true;
            continue;
        }
        if (random.uintLessThan(u8, 4) == 0) {
            try actions.append(alloc, .reopen);
            continue;
        }
        try actions.append(alloc, .{ .add_doc = randomIndexManagerWriteSpec(random) });
        saw_write = true;
    }

    var source_path_buf: [256]u8 = undefined;
    var dest_path_buf: [256]u8 = undefined;
    const source_path = indexManagerTmpPathWithSuffix(&source_path_buf, "sim-src");
    const dest_path = indexManagerTmpPathWithSuffix(&dest_path_buf, "sim-dst");
    defer cleanupIndexManagerDir(source_path);
    defer cleanupIndexManagerDir(dest_path);

    var modeled_device = storage_sim.ModeledDevice.init(alloc);
    defer modeled_device.deinit();
    const backend_options = db_config.IndexBackendOptions{
        .text_main_backend = .lsm,
        .text_lsm_storage = modeled_device.storage(),
        .dense_storage_backend = .lsm,
        .dense_lsm_storage = modeled_device.storage(),
        .graph_reverse_backend = .lsm,
        .graph_lsm_storage = modeled_device.storage(),
    };

    const actual = replayIndexManagerActionsAtPathsWithOptions(
        alloc,
        source_path,
        dest_path,
        backend_options,
        actions.items,
    ) catch |err| {
        reportReducedIndexManagerSchedule(alloc, case_label, seed, actions.items) catch {};
        return err;
    };
    try modeled_device.device().crash();
    const reopened = try replayIndexManagerActionsAtPathsWithOptions(
        alloc,
        source_path,
        dest_path,
        backend_options,
        &.{},
    );
    try expectIndexManagerSummaryEqual(case_label, try expectedIndexManagerSummary(actions.items), actual);
    try expectIndexManagerSummaryEqual(case_label, actual, reopened);
}

fn randomIndexManagerCrashAction(random: std.Random) IndexManagerSimAction {
    return .{ .add_doc = switch (random.uintLessThan(u8, 3)) {
        0 => .left_alpha,
        1 => .left_gamma,
        else => .right_beta,
    } };
}

fn runIndexManagerCrashCase(
    alloc: Allocator,
    case_label: []const u8,
    seed: u64,
    steps: usize,
) !void {
    var modeled_device = storage_sim.ModeledDevice.init(alloc);
    defer modeled_device.deinit();
    const backend_options = db_config.IndexBackendOptions{
        .text_main_backend = .lsm,
        .text_lsm_storage = modeled_device.storage(),
        .dense_storage_backend = .lsm,
        .dense_lsm_storage = modeled_device.storage(),
        .graph_reverse_backend = .lsm,
        .graph_lsm_storage = modeled_device.storage(),
    };

    var prng = std.Random.DefaultPrng.init(seed);
    const random = prng.random();

    var prelude = std.ArrayListUnmanaged(IndexManagerSimAction).empty;
    defer prelude.deinit(alloc);

    var split_active = false;
    var saw_write = false;
    for (0..steps) |_| {
        if (!split_active and saw_write and random.uintLessThan(u8, 6) == 0) {
            try prelude.append(alloc, .split_handoff);
            split_active = true;
            continue;
        }
        if (random.uintLessThan(u8, 4) == 0) {
            try prelude.append(alloc, .reopen);
            continue;
        }
        try prelude.append(alloc, .{ .add_doc = randomIndexManagerWriteSpec(random) });
        saw_write = true;
    }

    const crash_action = randomIndexManagerCrashAction(random);
    var source_path_buf: [256]u8 = undefined;
    var dest_path_buf: [256]u8 = undefined;
    const source_path = indexManagerTmpPathWithSuffix(&source_path_buf, "modeled-sim-crash-src");
    const dest_path = indexManagerTmpPathWithSuffix(&dest_path_buf, "modeled-sim-crash-dst");
    defer cleanupIndexManagerDir(source_path);
    defer cleanupIndexManagerDir(dest_path);

    const outcome = try replayModeledIndexManagerCrashFixture(
        alloc,
        source_path,
        dest_path,
        backend_options,
        case_label,
        prelude.items,
        crash_action,
        &modeled_device,
    );
    try sim_fixture.expectFieldEqual(case_label, "expected_outcome", IndexManagerSimCrashOutcome.committed, outcome);
}

fn runIndexManagerReplayFixtures(alloc: Allocator) !void {
    const root_dir = "pkg/antfly/src/storage/db/catalog/index_manager_sim_fixtures";
    var fixture_dir = try std.Io.Dir.cwd().openDir(std.testing.io, root_dir, .{ .iterate = true });
    defer fixture_dir.close(std.testing.io);

    var walker = try fixture_dir.walk(alloc);
    defer walker.deinit();

    var fixture_paths = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (fixture_paths.items) |path| alloc.free(path);
        fixture_paths.deinit(alloc);
    }

    while (try walker.next(std.testing.io)) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.path, ".fixture")) continue;
        try fixture_paths.append(alloc, try alloc.dupe(u8, entry.path));
    }

    std.mem.sort([]u8, fixture_paths.items, {}, struct {
        fn lessThan(_: void, lhs: []const u8, rhs: []const u8) bool {
            return std.mem.order(u8, lhs, rhs) == .lt;
        }
    }.lessThan);

    for (fixture_paths.items) |fixture_rel_path| {
        const fixture_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ root_dir, fixture_rel_path });
        defer alloc.free(fixture_path);

        const raw = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, fixture_path, alloc, .limited(64 * 1024));
        defer alloc.free(raw);

        var fixture = try index_manager_sim_fixture.parseFixture(alloc, raw);
        defer fixture.deinit(alloc);

        const fixture_name = fixture.case_label orelse fixture.label orelse fixture_rel_path;
        switch (fixture.mode) {
            .replay => {
                var source_path_buf: [256]u8 = undefined;
                var dest_path_buf: [256]u8 = undefined;
                const source_path = indexManagerTmpPathWithSuffix(&source_path_buf, "fixture-src");
                const dest_path = indexManagerTmpPathWithSuffix(&dest_path_buf, "fixture-dst");
                defer cleanupIndexManagerDir(source_path);
                defer cleanupIndexManagerDir(dest_path);

                const actual = try replayIndexManagerActionsAtPaths(alloc, source_path, dest_path, fixture.actions);
                try expectIndexManagerFixtureExpectation(fixture_name, fixture.opts, actual);
            },
            .crash => {
                if (!zig_lmdb.is_zig_backend) continue;
                const phase = std.meta.stringToEnum(lmdb.CommitPublishPhase, fixture.phase orelse return error.InvalidFixture) orelse return error.InvalidFixture;
                const crash_action = fixture.crash_action orelse return error.InvalidFixture;
                const outcome = try replayIndexManagerCrashWorkload(alloc, fixture_name, fixture.prelude_actions, crash_action, phase);
                try sim_fixture.expectFieldEqual(fixture_name, "expected_outcome", fixture.opts.expected_outcome orelse .committed, outcome);

                const full_actions = try alloc.alloc(IndexManagerSimAction, fixture.prelude_actions.len + 1);
                defer alloc.free(full_actions);
                @memcpy(full_actions[0..fixture.prelude_actions.len], fixture.prelude_actions);
                full_actions[fixture.prelude_actions.len] = crash_action;
                try expectIndexManagerFixtureExpectation(fixture_name, fixture.opts, try expectedIndexManagerSummary(full_actions));
            },
        }
    }
}

fn runModeledIndexManagerReplayFixtures(alloc: Allocator) !void {
    const root_dir = "pkg/antfly/src/storage/db/catalog/index_manager_sim_fixtures/replay";
    var fixture_dir = try std.Io.Dir.cwd().openDir(std.testing.io, root_dir, .{ .iterate = true });
    defer fixture_dir.close(std.testing.io);

    var walker = try fixture_dir.walk(alloc);
    defer walker.deinit();

    var fixture_paths = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (fixture_paths.items) |path| alloc.free(path);
        fixture_paths.deinit(alloc);
    }

    while (try walker.next(std.testing.io)) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.path, ".fixture")) continue;
        try fixture_paths.append(alloc, try alloc.dupe(u8, entry.path));
    }

    std.mem.sort([]u8, fixture_paths.items, {}, struct {
        fn lessThan(_: void, lhs: []const u8, rhs: []const u8) bool {
            return std.mem.order(u8, lhs, rhs) == .lt;
        }
    }.lessThan);

    for (fixture_paths.items) |fixture_rel_path| {
        const fixture_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ root_dir, fixture_rel_path });
        defer alloc.free(fixture_path);

        const raw = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, fixture_path, alloc, .limited(64 * 1024));
        defer alloc.free(raw);

        var fixture = try index_manager_sim_fixture.parseFixture(alloc, raw);
        defer fixture.deinit(alloc);
        if (fixture.mode != .replay) continue;

        var modeled_device = storage_sim.ModeledDevice.init(alloc);
        defer modeled_device.deinit();
        const backend_options = db_config.IndexBackendOptions{
            .text_main_backend = .lsm,
            .text_lsm_storage = modeled_device.storage(),
            .dense_storage_backend = .lsm,
            .dense_lsm_storage = modeled_device.storage(),
            .graph_reverse_backend = .lsm,
            .graph_lsm_storage = modeled_device.storage(),
        };

        var source_path_buf: [256]u8 = undefined;
        var dest_path_buf: [256]u8 = undefined;
        const source_path = indexManagerTmpPathWithSuffix(&source_path_buf, "modeled-fixture-src");
        const dest_path = indexManagerTmpPathWithSuffix(&dest_path_buf, "modeled-fixture-dst");
        defer cleanupIndexManagerDir(source_path);
        defer cleanupIndexManagerDir(dest_path);

        const actual = try replayIndexManagerActionsAtPathsWithOptions(
            alloc,
            source_path,
            dest_path,
            backend_options,
            fixture.actions,
        );
        try modeled_device.device().crash();
        const reopened = try replayIndexManagerActionsAtPathsWithOptions(
            alloc,
            source_path,
            dest_path,
            backend_options,
            &.{},
        );

        const fixture_name = fixture.case_label orelse fixture.label orelse fixture_rel_path;
        try expectIndexManagerFixtureExpectation(fixture_name, fixture.opts, actual);
        try expectIndexManagerSummaryEqual(fixture_name, actual, reopened);
    }
}

fn replayModeledIndexManagerCrashFixture(
    alloc: Allocator,
    source_path: [*:0]const u8,
    dest_path: [*:0]const u8,
    backend_options: db_config.IndexBackendOptions,
    fixture_name: []const u8,
    prelude_actions: []const IndexManagerSimAction,
    crash_action: IndexManagerSimAction,
    modeled_device: *storage_sim.ModeledDevice,
) !IndexManagerSimCrashOutcome {
    var runtime = try IndexManagerSimRuntime.initWithOptions(alloc, source_path, dest_path, backend_options);
    defer runtime.deinit();

    for (prelude_actions, 0..) |action, step| {
        try runtime.applyReplayAction(action, step);
    }
    try runtime.applyReplayAction(crash_action, prelude_actions.len);
    try modeled_device.device().crash();
    try runtime.reopen();

    const full_actions = try alloc.alloc(IndexManagerSimAction, prelude_actions.len + 1);
    defer alloc.free(full_actions);
    @memcpy(full_actions[0..prelude_actions.len], prelude_actions);
    full_actions[prelude_actions.len] = crash_action;
    try expectIndexManagerSummaryEqual(fixture_name, try expectedIndexManagerSummary(full_actions), try runtime.summary(alloc));
    return .committed;
}

fn runModeledIndexManagerCrashFixtures(alloc: Allocator) !void {
    const root_dir = "pkg/antfly/src/storage/db/catalog/index_manager_sim_fixtures/crash";
    var fixture_dir = try std.Io.Dir.cwd().openDir(std.testing.io, root_dir, .{ .iterate = true });
    defer fixture_dir.close(std.testing.io);

    var walker = try fixture_dir.walk(alloc);
    defer walker.deinit();

    var fixture_paths = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (fixture_paths.items) |path| alloc.free(path);
        fixture_paths.deinit(alloc);
    }

    while (try walker.next(std.testing.io)) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.path, ".fixture")) continue;
        try fixture_paths.append(alloc, try alloc.dupe(u8, entry.path));
    }

    std.mem.sort([]u8, fixture_paths.items, {}, struct {
        fn lessThan(_: void, lhs: []const u8, rhs: []const u8) bool {
            return std.mem.order(u8, lhs, rhs) == .lt;
        }
    }.lessThan);

    for (fixture_paths.items) |fixture_rel_path| {
        const fixture_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ root_dir, fixture_rel_path });
        defer alloc.free(fixture_path);

        const raw = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, fixture_path, alloc, .limited(64 * 1024));
        defer alloc.free(raw);

        var fixture = try index_manager_sim_fixture.parseFixture(alloc, raw);
        defer fixture.deinit(alloc);
        if (fixture.mode != .crash) continue;

        var modeled_device = storage_sim.ModeledDevice.init(alloc);
        defer modeled_device.deinit();
        const backend_options = db_config.IndexBackendOptions{
            .text_main_backend = .lsm,
            .text_lsm_storage = modeled_device.storage(),
            .dense_storage_backend = .lsm,
            .dense_lsm_storage = modeled_device.storage(),
            .graph_reverse_backend = .lsm,
            .graph_lsm_storage = modeled_device.storage(),
        };

        var source_path_buf: [256]u8 = undefined;
        var dest_path_buf: [256]u8 = undefined;
        const source_path = indexManagerTmpPathWithSuffix(&source_path_buf, "modeled-crash-src");
        const dest_path = indexManagerTmpPathWithSuffix(&dest_path_buf, "modeled-crash-dst");
        defer cleanupIndexManagerDir(source_path);
        defer cleanupIndexManagerDir(dest_path);

        const fixture_name = fixture.case_label orelse fixture.label orelse fixture_rel_path;
        const outcome = try replayModeledIndexManagerCrashFixture(
            alloc,
            source_path,
            dest_path,
            backend_options,
            fixture_name,
            fixture.prelude_actions,
            fixture.crash_action orelse return error.InvalidFixture,
            &modeled_device,
        );
        try sim_fixture.expectFieldEqual(fixture_name, "expected_outcome", fixture.opts.expected_outcome orelse .committed, outcome);
    }
}

fn expectIndexManagerFixtureExpectation(
    fixture_name: []const u8,
    opts: index_manager_sim_fixture.Options,
    actual: IndexManagerSimSummary,
) !void {
    if (opts.expected_source_doc_count) |expected| try sim_fixture.expectFieldEqual(fixture_name, "expected_source_doc_count", expected, actual.source_doc_count);
    if (opts.expected_dest_doc_count) |expected| try sim_fixture.expectFieldEqual(fixture_name, "expected_dest_doc_count", expected, actual.dest_doc_count);
    if (opts.expected_source_alpha_hits) |expected| try sim_fixture.expectFieldEqual(fixture_name, "expected_source_alpha_hits", expected, actual.source_alpha_hits);
    if (opts.expected_source_beta_hits) |expected| try sim_fixture.expectFieldEqual(fixture_name, "expected_source_beta_hits", expected, actual.source_beta_hits);
    if (opts.expected_source_gamma_hits) |expected| try sim_fixture.expectFieldEqual(fixture_name, "expected_source_gamma_hits", expected, actual.source_gamma_hits);
    if (opts.expected_dest_alpha_hits) |expected| try sim_fixture.expectFieldEqual(fixture_name, "expected_dest_alpha_hits", expected, actual.dest_alpha_hits);
    if (opts.expected_dest_beta_hits) |expected| try sim_fixture.expectFieldEqual(fixture_name, "expected_dest_beta_hits", expected, actual.dest_beta_hits);
    if (opts.expected_dest_gamma_hits) |expected| try sim_fixture.expectFieldEqual(fixture_name, "expected_dest_gamma_hits", expected, actual.dest_gamma_hits);
}

test "index manager sim workloads stay green" {
    const alloc = std.testing.allocator;
    try runIndexManagerReplayCase(alloc, "index-manager-default", 0xA17F_D101, 8);
    try runIndexManagerReplayCase(alloc, "index-manager-split-heavy", 0xA17F_D102, 10);
    try runIndexManagerCrashCase(alloc, "index-manager-crash-default", 0xA17F_D201, 6);
}

test "index manager split handoff preserves interleaved write and query summaries" {
    const alloc = std.testing.allocator;
    const actions = [_]IndexManagerSimAction{
        .{ .add_doc = .mixed_alpha_beta },
        .{ .add_doc = .left_gamma },
        .split_handoff,
        .{ .add_doc = .mixed_alpha_beta },
        .reopen,
        .{ .add_doc = .right_beta },
        .{ .add_doc = .left_alpha },
    };

    var source_path_buf: [256]u8 = undefined;
    var dest_path_buf: [256]u8 = undefined;
    const source_path = indexManagerTmpPathWithSuffix(&source_path_buf, "deterministic-split-src");
    const dest_path = indexManagerTmpPathWithSuffix(&dest_path_buf, "deterministic-split-dst");
    defer cleanupIndexManagerDir(source_path);
    defer cleanupIndexManagerDir(dest_path);

    var modeled_device = storage_sim.ModeledDevice.init(alloc);
    defer modeled_device.deinit();
    const backend_options = db_config.IndexBackendOptions{
        .text_main_backend = .lsm,
        .text_lsm_storage = modeled_device.storage(),
        .dense_storage_backend = .lsm,
        .dense_lsm_storage = modeled_device.storage(),
        .graph_reverse_backend = .lsm,
        .graph_lsm_storage = modeled_device.storage(),
    };

    var runtime = try IndexManagerSimRuntime.initWithOptions(alloc, source_path, dest_path, backend_options);
    defer runtime.deinit();
    for (actions, 0..) |action, step| {
        try runtime.applyReplayAction(action, step);
        const actual = try runtime.summary(alloc);
        try expectIndexManagerSummaryEqual("deterministic-split-step", try expectedIndexManagerSummary(actions[0 .. step + 1]), actual);
    }

    try modeled_device.device().crash();
    try runtime.reopen();
    try expectIndexManagerSummaryEqual("deterministic-split-reopen", try expectedIndexManagerSummary(&actions), try runtime.summary(alloc));
}

test "index manager replay fixtures stay green" {
    try runIndexManagerReplayFixtures(std.testing.allocator);
}

test "index manager modeled replay fixtures stay green" {
    try runModeledIndexManagerReplayFixtures(std.testing.allocator);
}

test "index manager modeled crash fixtures stay green" {
    try runModeledIndexManagerCrashFixtures(std.testing.allocator);
}

test "parseDenseConfig accepts HBC tuning knobs" {
    const alloc = std.testing.allocator;
    const raw =
        \\{
        \\  "field": "embedding",
        \\  "dims": 128,
        \\  "metric": "cosine",
        \\  "split_algo": "kmeans",
        \\  "search_width": 256,
        \\  "epsilon": 3,
        \\  "branching_factor": 64,
        \\  "leaf_size": 48,
        \\  "bulk_build_algo": "kmeans",
        \\  "kmeans_backend": "metal",
        \\  "kmeans_update_strategy": "segmented",
        \\  "use_quantization": true,
        \\  "rerank_policy": "never",
        \\  "quantizer_seed": 99,
        \\  "use_random_ortho_trans": true,
        \\  "max_cached_nodes": 4096,
        \\  "max_cached_vectors": 2048,
        \\  "max_cached_metadata": 8192,
        \\  "lazy_posting_maintenance": true,
        \\  "auto_posting_maintenance_max_postings": 17,
        \\  "centroid_directory_mode": "flat_rabitq",
        \\  "flat_centroid_block_size": 1024,
        \\  "flat_centroid_probe_count": 33
        \\}
    ;

    const cfg = try parseDenseConfig(alloc, raw);
    defer cfg.deinit(alloc);

    try std.testing.expectEqualStrings("embedding", cfg.field_name);
    try std.testing.expectEqual(@as(u32, 128), cfg.dims);
    try std.testing.expectEqual(vector_mod.DistanceMetric.cosine, cfg.metric);
    try std.testing.expectEqual(vector_mod.ClustAlgorithm.kmeans, cfg.split_algo);
    try std.testing.expectEqual(@as(u32, 256), cfg.search_width);
    try std.testing.expectEqual(@as(f32, 3), cfg.epsilon);
    try std.testing.expectEqual(@as(u32, 64), cfg.branching_factor);
    try std.testing.expectEqual(@as(u32, 48), cfg.leaf_size);
    try std.testing.expectEqual(hbc_mod.BulkBuildAlgo.kmeans, cfg.bulk_build_algo);
    try std.testing.expectEqual(hbc_mod.HBCConfig.KmeansBackend.metal, cfg.kmeans_backend);
    try std.testing.expectEqual(hbc_mod.HBCConfig.KmeansUpdateStrategy.segmented, cfg.kmeans_update_strategy);
    try std.testing.expectEqual(true, cfg.use_quantization);
    try std.testing.expectEqual(hbc_mod.HBCConfig.RerankPolicy.never, cfg.rerank_policy);
    try std.testing.expectEqual(@as(u64, 99), cfg.quantizer_seed);
    try std.testing.expectEqual(true, cfg.use_random_ortho_trans);
    try std.testing.expectEqual(@as(usize, 4096), cfg.max_cached_nodes);
    try std.testing.expectEqual(@as(usize, 2048), cfg.max_cached_vectors);
    try std.testing.expectEqual(@as(usize, 8192), cfg.max_cached_metadata);
    try std.testing.expectEqual(true, cfg.lazy_posting_maintenance);
    try std.testing.expectEqual(@as(usize, 17), cfg.auto_posting_maintenance_max_postings);
    try std.testing.expectEqual(hbc_mod.HBCConfig.CentroidDirectoryMode.flat_rabitq, cfg.centroid_directory_mode);
    try std.testing.expectEqual(@as(usize, 1024), cfg.flat_centroid_block_size);
    try std.testing.expectEqual(@as(usize, 33), cfg.flat_centroid_probe_count);
}

test "parseDenseConfig accepts external embedding indexes" {
    const alloc = std.testing.allocator;
    const raw =
        \\{
        \\  "field": "embedding",
        \\  "dims": 384,
        \\  "metric": "cosine",
        \\  "embedding_name": "semantic_idx",
        \\  "external": true
        \\}
    ;

    const cfg = try parseDenseConfig(alloc, raw);
    defer cfg.deinit(alloc);

    try std.testing.expectEqualStrings("embedding", cfg.field_name);
    try std.testing.expectEqual(@as(u32, 384), cfg.dims);
    try std.testing.expectEqual(vector_mod.DistanceMetric.cosine, cfg.metric);
    try std.testing.expectEqualStrings("semantic_idx", cfg.embedding_name.?);
    try std.testing.expect(cfg.external);
}

test "parseDenseGeneratorConfig parses source_template" {
    const alloc = std.testing.allocator;
    const json =
        \\{"field":"embedding","dims":384,"generator":{"kind":"dense_embedding","source_field":"body","source_template":"{{title}} {{body}}","artifact_name":"body_chunks","chunk_size":512,"chunk_overlap":64}}
    ;
    const generator = try parseDenseGeneratorConfig(alloc, json) orelse return error.TestUnexpectedResult;
    defer generator.deinit(alloc);

    try std.testing.expectEqualStrings("body", generator.source_field);
    try std.testing.expectEqualStrings("{{title}} {{body}}", generator.source_template);
    try std.testing.expectEqualStrings("body_chunks", generator.artifact_name);
    try std.testing.expectEqual(@as(u32, 512), generator.chunk_size);
    try std.testing.expectEqual(@as(u32, 64), generator.chunk_overlap);
}

test "parseDenseGeneratorConfig without source_template" {
    const alloc = std.testing.allocator;
    const json =
        \\{"field":"embedding","dims":384,"generator":{"kind":"dense_embedding","source_field":"body","artifact_name":"body_chunks","chunk_size":256}}
    ;
    const generator = try parseDenseGeneratorConfig(alloc, json) orelse return error.TestUnexpectedResult;
    defer generator.deinit(alloc);

    try std.testing.expectEqualStrings("body", generator.source_field);
    try std.testing.expectEqual(@as(usize, 0), generator.source_template.len);
}

test "parseSparseGeneratorConfig parses source_template" {
    const alloc = std.testing.allocator;
    const json =
        \\{"field":"sparse","generator":{"kind":"sparse_embedding","source_field":"body","source_template":"{{title}} {{body}}","artifact_name":"body_chunks","chunk_size":512}}
    ;
    const generator = try parseSparseGeneratorConfig(alloc, json) orelse return error.TestUnexpectedResult;
    defer generator.deinit(alloc);

    try std.testing.expectEqualStrings("body", generator.source_field);
    try std.testing.expectEqualStrings("{{title}} {{body}}", generator.source_template);
    try std.testing.expectEqualStrings("body_chunks", generator.artifact_name);
}

test "shorthand chunk and embedding enrichment compatibility includes source_template" {
    const alloc = std.testing.allocator;

    var manager = try IndexManager.init(alloc, ".");
    defer manager.deinit();

    try std.testing.expect(try manager.ensureChunkEnrichment(.{
        .name = "body_chunks",
        .kind = .chunk,
        .source_field = "body",
        .source_template = "{{title}} {{body}}",
        .chunk_size = 256,
    }));
    try std.testing.expect(!try manager.ensureChunkEnrichment(.{
        .name = "body_chunks",
        .kind = .chunk,
        .source_field = "body",
        .source_template = "{{title}} {{body}}",
        .chunk_size = 256,
    }));
    try std.testing.expectError(error.ConflictingEnrichmentConfig, manager.ensureChunkEnrichment(.{
        .name = "body_chunks",
        .kind = .chunk,
        .source_field = "body",
        .source_template = "{{body}}",
        .chunk_size = 256,
    }));

    try std.testing.expect(try manager.ensureEmbeddingEnrichment(.{
        .name = "body_embedding",
        .kind = .embedding,
        .source_field = "body",
        .source_template = "{{title}} {{body}}",
        .source_artifact_name = "body_chunks",
        .expected_dims = 384,
    }));
    try std.testing.expect(!try manager.ensureEmbeddingEnrichment(.{
        .name = "body_embedding",
        .kind = .embedding,
        .source_field = "body",
        .source_template = "{{title}} {{body}}",
        .source_artifact_name = "body_chunks",
        .expected_dims = 384,
    }));
    try std.testing.expectError(error.ConflictingEnrichmentConfig, manager.ensureEmbeddingEnrichment(.{
        .name = "body_embedding",
        .kind = .embedding,
        .source_field = "body",
        .source_template = "{{body}}",
        .source_artifact_name = "body_chunks",
        .expected_dims = 384,
    }));
}

test "generated enrichment request identity includes source_template" {
    const requests = [_]enrichment_types.GeneratedEnrichmentRequest{
        .{
            .kind = .chunk_text,
            .index_name = "semantic",
            .artifact_name = "body_chunks",
            .doc_key = "doc:1",
            .source_field = "body",
            .source_template = "{{title}} {{body}}",
            .chunk_size = 256,
        },
        .{
            .kind = .dense_embedding,
            .index_name = "semantic",
            .artifact_name = "body_chunks",
            .embedding_name = "body_embedding",
            .doc_key = "doc:1",
            .source_field = "body",
            .source_template = "{{title}} {{body}}",
            .expected_dims = 384,
        },
    };

    try std.testing.expect(hasGeneratedChunkRequest(requests[0..], "doc:1", "body", "{{title}} {{body}}", "body_chunks"));
    try std.testing.expect(!hasGeneratedChunkRequest(requests[0..], "doc:1", "body", "{{body}}", "body_chunks"));

    try std.testing.expect(hasGeneratedDenseEmbeddingRequest(requests[0..], "doc:1", "body", "{{title}} {{body}}", "body_chunks", "body_embedding"));
    try std.testing.expect(!hasGeneratedDenseEmbeddingRequest(requests[0..], "doc:1", "body", "{{body}}", "body_chunks", "body_embedding"));
}

test "parseTextConfig prefers source artifact name and accepts legacy chunk name" {
    const alloc = std.testing.allocator;

    var cfg = try parseTextConfig(alloc, "{\"artifact_name\":\"body_chunks_v1\"}");
    defer cfg.deinit(alloc);
    try std.testing.expectEqualStrings("body_chunks_v1", cfg.source_artifact_name.?);

    var legacy = try parseTextConfig(alloc, "{\"chunk_name\":\"legacy_chunks_v1\"}");
    defer legacy.deinit(alloc);
    try std.testing.expectEqualStrings("legacy_chunks_v1", legacy.source_artifact_name.?);
}

test "dense embedding writes own vectors and metadata past caller lifetime" {
    const alloc = std.testing.allocator;

    var owned: IndexManager.OwnedDenseInsertItems = .{};
    defer owned.deinit(alloc);

    const source_key = try alloc.dupe(u8, "doc:owned");
    defer alloc.free(source_key);
    const source_vector = try alloc.dupe(f32, &[_]f32{ 1.0, 2.0, 3.0 });
    defer alloc.free(source_vector);

    try owned.appendBorrowed(alloc, 7, source_vector, source_key);
    try std.testing.expect(owned.items.items[0].vector.ptr != source_vector.ptr);
    try std.testing.expect(owned.items.items[0].metadata.ptr != source_key.ptr);

    @memset(source_key, 'x');
    @memset(source_vector, 0.0);

    try std.testing.expectEqual(@as(u64, 7), owned.items.items[0].vector_id);
    try std.testing.expectEqualStrings("doc:owned", owned.items.items[0].metadata);
    try std.testing.expectEqual(@as(f32, 1.0), owned.items.items[0].vector[0]);
    try std.testing.expectEqual(@as(f32, 2.0), owned.items.items[0].vector[1]);
    try std.testing.expectEqual(@as(f32, 3.0), owned.items.items[0].vector[2]);
}

test "dense embedding writes can materialize owned payloads in an arena" {
    const alloc = std.testing.allocator;

    var owned: IndexManager.OwnedDenseInsertItems = .{};
    defer owned.deinit(alloc);
    _ = try owned.ensureArena(alloc);

    const source_key = try alloc.dupe(u8, "doc:arena");
    defer alloc.free(source_key);
    const source_vector = try alloc.dupe(f32, &[_]f32{ 4.0, 5.0, 6.0 });

    try owned.appendOwnedVector(alloc, 9, source_vector, source_key);
    try std.testing.expect(owned.items.items[0].vector.ptr != source_vector.ptr);
    try std.testing.expect(owned.items.items[0].metadata.ptr != source_key.ptr);

    @memset(source_key, 'y');

    try std.testing.expectEqual(@as(u64, 9), owned.items.items[0].vector_id);
    try std.testing.expectEqualStrings("doc:arena", owned.items.items[0].metadata);
    try std.testing.expectEqual(@as(f32, 4.0), owned.items.items[0].vector[0]);
    try std.testing.expectEqual(@as(f32, 5.0), owned.items.items[0].vector[1]);
    try std.testing.expectEqual(@as(f32, 6.0), owned.items.items[0].vector[2]);
}

test "initRuntimeStore borrows backend_erased.Store values" {
    var backend = lsm_backend_mod.Backend.init(std.testing.allocator, .{});
    defer backend.close();

    var store = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer store.deinit();

    var runtime = try initRuntimeStore(std.testing.allocator, store);
    try std.testing.expect(!runtime.owned);
    runtime.deinit();

    var txn = try store.beginWrite();
    try txn.put("doc:a", "A");
    try txn.commit();
}

test "dense vector id uses deterministic key hash with legacy mapping fallback" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const cwd = try std.process.currentPathAlloc(io_impl.io(), alloc);
    defer alloc.free(cwd);
    const absolute_path = try std.fs.path.resolve(alloc, &.{ cwd, path });
    defer alloc.free(absolute_path);
    const path_z = try alloc.dupeZ(u8, absolute_path);
    defer alloc.free(path_z);

    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();

    var manager = try IndexManager.init(alloc, absolute_path);
    defer manager.deinit();

    var batch = try store.beginWriteBatch();
    errdefer batch.abort();
    const assignment = try manager.ensureDenseVectorIdTxn(batch.asTxn(), "dv_v1", "doc:a", null);
    try std.testing.expect(!assignment.needs_mapping);
    try std.testing.expectEqual(deterministicDenseVectorId("doc:a"), assignment.vector_id);
    try batch.commit();

    try std.testing.expectEqual(@as(?u64, null), try manager.lookupDenseVectorId(&store, "dv_v1", "doc:a"));

    try manager.writeDenseVectorMapping(&store, "dv_v1", "doc:a", 42);
    try std.testing.expectEqual(@as(?u64, 42), try manager.lookupDenseVectorId(&store, "dv_v1", "doc:a"));

    var second_batch = try store.beginWriteBatch();
    errdefer second_batch.abort();
    const second = try manager.ensureDenseVectorIdTxn(second_batch.asTxn(), "dv_v1", "doc:a", null);
    try std.testing.expect(!second.needs_mapping);
    try std.testing.expectEqual(@as(u64, 42), second.vector_id);
    second_batch.abort();
}

test "dense vector id ignores ordinal metadata for a different doc" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const cwd = try std.process.currentPathAlloc(io_impl.io(), alloc);
    defer alloc.free(cwd);
    const absolute_path = try std.fs.path.resolve(alloc, &.{ cwd, path });
    defer alloc.free(absolute_path);
    const path_z = try alloc.dupeZ(u8, absolute_path);
    defer alloc.free(path_z);

    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();

    var manager = try IndexManager.init(alloc, absolute_path);
    defer manager.deinit();
    manager.updateRange(.{ .start = "", .end = "" });

    try manager.addAllNoBackfill(&store, &.{
        .{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"embedding_name\":\"dv_v1\",\"external\":true}",
        },
    });

    var identity_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer {
        for (identity_writes.items) |item| {
            alloc.free(@constCast(item.key));
            alloc.free(@constCast(item.value));
        }
        identity_writes.deinit(alloc);
    }
    try doc_identity.appendBatchIdentityMetadataAlloc(
        alloc,
        &store,
        0,
        0,
        1,
        &identity_writes,
        &.{"doc:target"},
        &.{},
    );
    try store.putBatchWithReplay(null, identity_writes.items, &.{}, null);

    const entry = manager.denseIndex("dv_v1") orelse return error.IndexNotFound;
    try entry.index.batchInsertWithMetadata(&.{
        .{
            .vector_id = 1,
            .vector = &[_]f32{ 1.0, 0.0, 0.0 },
            .metadata = "doc:other",
        },
    });

    const stable_vector_id = deterministicDenseVectorId("doc:target");
    try std.testing.expect(stable_vector_id != 1);

    var batch = try store.beginWriteBatch();
    errdefer batch.abort();
    const assignment = try manager.ensureDenseVectorIdTxn(batch.asTxn(), "dv_v1", "doc:target", null);
    try std.testing.expectEqual(stable_vector_id, assignment.vector_id);
    try std.testing.expect(assignment.can_assume_absent);
    try batch.commit();

    const dense_index_name = try alloc.dupe(u8, "dv_v1");
    defer alloc.free(dense_index_name);
    const dense_doc_key = try alloc.dupe(u8, "doc:target");
    defer alloc.free(dense_doc_key);
    const dense_vector = try alloc.dupe(f32, &[_]f32{ 0.0, 1.0, 0.0 });
    defer alloc.free(dense_vector);
    const writes = [_]mapper.DenseEmbeddingWrite{.{
        .index_name = dense_index_name,
        .doc_key = dense_doc_key,
        .vector = dense_vector,
        .artifact_key = null,
    }};
    try manager.applyDenseEmbeddingWritesByName(&store, "dv_v1", &writes);

    try std.testing.expectEqual(@as(?u64, stable_vector_id), try manager.lookupDenseVectorId(&store, "dv_v1", "doc:target"));
    const vector_ids = try manager.lookupDenseVectorIdsForOrdinalsAlloc(alloc, &store, "dv_v1", &.{1});
    defer alloc.free(vector_ids);
    try std.testing.expectEqual(@as(usize, 1), vector_ids.len);
    try std.testing.expectEqual(stable_vector_id, vector_ids[0]);
}

test "dense metadata prefetch includes legacy ordinal vector ids" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const cwd = try std.process.currentPathAlloc(io_impl.io(), alloc);
    defer alloc.free(cwd);
    const absolute_path = try std.fs.path.resolve(alloc, &.{ cwd, path });
    defer alloc.free(absolute_path);
    const path_z = try alloc.dupeZ(u8, absolute_path);
    defer alloc.free(path_z);

    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();

    var manager = try IndexManager.init(alloc, absolute_path);
    defer manager.deinit();
    manager.updateRange(.{ .start = "", .end = "" });

    try manager.addAllNoBackfill(&store, &.{
        .{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"embedding_name\":\"dv_v1\",\"external\":true}",
        },
    });

    var identity_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer {
        for (identity_writes.items) |item| {
            alloc.free(@constCast(item.key));
            alloc.free(@constCast(item.value));
        }
        identity_writes.deinit(alloc);
    }
    try doc_identity.appendBatchIdentityMetadataAlloc(
        alloc,
        &store,
        0,
        0,
        1,
        &identity_writes,
        &.{"doc:legacy"},
        &.{},
    );
    try store.putBatchWithReplay(null, identity_writes.items, &.{}, null);

    const entry = manager.denseIndex("dv_v1") orelse return error.IndexNotFound;
    try entry.index.batchInsertWithMetadata(&.{
        .{
            .vector_id = 1,
            .vector = &[_]f32{ 1.0, 0.0, 0.0 },
            .metadata = "doc:legacy",
        },
    });

    const stable_vector_id = deterministicDenseVectorId("doc:legacy");
    try std.testing.expect(stable_vector_id != 1);

    const dense_index_name = try alloc.dupe(u8, "dv_v1");
    defer alloc.free(dense_index_name);
    const dense_doc_key = try alloc.dupe(u8, "doc:legacy");
    defer alloc.free(dense_doc_key);
    const dense_vector = try alloc.dupe(f32, &[_]f32{ 0.0, 1.0, 0.0 });
    defer alloc.free(dense_vector);
    const writes = [_]mapper.DenseEmbeddingWrite{.{
        .index_name = dense_index_name,
        .doc_key = dense_doc_key,
        .vector = dense_vector,
        .artifact_key = null,
    }};
    const keep_write = [_]bool{true};

    var identity_txn = try store.beginWriteBatch();
    defer identity_txn.abort();
    var index_txn = try entry.index.beginRuntimeWriteTxn();
    defer index_txn.abort();

    var memo: IndexManager.DenseVectorMetadataPresenceMemo = .{};
    defer memo.deinit(alloc);
    try manager.prefetchDenseExistingMetadataTxn(entry, identity_txn.asTxn(), &index_txn, &writes, &keep_write, &memo);

    try std.testing.expectEqualStrings("doc:legacy", memo.getMetadata(1).?);
    try std.testing.expectEqual(@as(?bool, false), memo.get(stable_vector_id));
}

test "dense vector metadata presence memo stores present and absent ids" {
    var memo: IndexManager.DenseVectorMetadataPresenceMemo = .{};
    defer memo.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(?bool, null), memo.get(7));
    const metadata = try std.testing.allocator.dupe(u8, "doc:7");
    defer std.testing.allocator.free(metadata);
    try memo.notePresent(std.testing.allocator, 7, metadata);
    try memo.noteAbsent(std.testing.allocator, 9);
    try std.testing.expectEqual(@as(?bool, true), memo.get(7));
    try std.testing.expectEqual(@as(?bool, false), memo.get(9));
    metadata[0] = 'x';
    try std.testing.expectEqualStrings("doc:7", memo.getMetadata(7).?);
    try std.testing.expectEqual(@as(?[]const u8, null), memo.getMetadata(9));
}

test "dense vector metadata presence memo accepts memo-owned metadata input" {
    var memo: IndexManager.DenseVectorMetadataPresenceMemo = .{};
    defer memo.deinit(std.testing.allocator);

    try memo.notePresent(std.testing.allocator, 7, "doc:7");
    const cached = memo.getMetadata(7).?;
    try memo.notePresent(std.testing.allocator, 7, cached);

    try std.testing.expectEqual(@as(?bool, true), memo.get(7));
    try std.testing.expectEqualStrings("doc:7", memo.getMetadata(7).?);
}

test "dense index manager accepts explicit embedding writes after addAllNoBackfill" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const cwd = try std.process.currentPathAlloc(io_impl.io(), alloc);
    defer alloc.free(cwd);
    const absolute_path = try std.fs.path.resolve(alloc, &.{ cwd, path });
    defer alloc.free(absolute_path);
    const path_z = try alloc.dupeZ(u8, absolute_path);
    defer alloc.free(path_z);

    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();

    var manager = try IndexManager.init(alloc, absolute_path);
    defer manager.deinit();
    manager.updateRange(.{ .start = "", .end = "" });

    try manager.addAllNoBackfill(&store, &.{
        .{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\"}",
        },
    });

    const stored_key = try internal_keys.documentKeyAlloc(alloc, "doc:00000000");
    defer alloc.free(stored_key);
    try store.put(stored_key, "{\"title\":\"dense\"}");

    const dense_index_name = try alloc.dupe(u8, "dv_v1");
    defer alloc.free(dense_index_name);
    const dense_doc_key = try alloc.dupe(u8, "doc:00000000");
    defer alloc.free(dense_doc_key);
    const dense_vector = try alloc.dupe(f32, &[_]f32{ 1.0, 2.0, 3.0 });
    defer alloc.free(dense_vector);
    const writes = [_]mapper.DenseEmbeddingWrite{
        .{
            .index_name = dense_index_name,
            .doc_key = dense_doc_key,
            .vector = dense_vector,
            .artifact_key = null,
        },
    };
    try manager.applyDenseEmbeddingWritesByName(&store, "dv_v1", &writes);

    const entry = manager.denseIndex("dv_v1") orelse return error.IndexNotFound;
    try std.testing.expectEqual(@as(u64, 1), entry.index.stats().active_count);
}

test "index manager advertises typed tensor access paths for vector and graph indexes" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);

    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();

    var manager = try IndexManager.init(alloc, path);
    defer manager.deinit();
    manager.updateRange(.{ .start = "", .end = "" });

    try manager.addAllNoBackfill(&store, &.{
        .{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{\"analysis_config\":{\"field_analyzers\":{\"title\":\"standard\"}}}",
        },
        .{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\"}",
        },
        .{
            .name = "sv_v1",
            .kind = .sparse_vector,
            .config_json = "{\"field\":\"sparse\"}",
        },
        .{
            .name = "graph_v1",
            .kind = .graph,
            .config_json = "{\"algebraic_planning\":{\"bounded_traversal\":{\"law\":\"provenance_semiring\"}}}",
        },
    });

    const text_path = manager.fullTextLexicalAccessPath("ft_v1", "title", "standard").?;
    try std.testing.expectEqual(algebraic_mod.ir.PhysicalLayout.full_text_postings, text_path.layout);
    const text_identity = algebraic_mod.lexical.DictionaryIdentity.analyzedText("ft_v1", "title", "standard");
    const text_registry_key = try text_identity.registryKeyAlloc(alloc);
    defer alloc.free(text_registry_key);
    const canonical_scalar_identity = algebraic_mod.lexical.DictionaryIdentity.canonicalScalar("ft_v1", "title", .string, "json-scalar-v1", "kind-qualified");
    const canonical_scalar_registry_key = try canonical_scalar_identity.registryKeyAlloc(alloc);
    defer alloc.free(canonical_scalar_registry_key);
    {
        var read_txn = try store.beginReadTxn();
        defer read_txn.abort();
        const registry_payload = try read_txn.get(text_registry_key);
        var registry_entry = try algebraic_mod.lexical.RegistryEntry.decodeAlloc(alloc, registry_payload);
        defer registry_entry.deinit(alloc);
        try std.testing.expectEqualStrings("ft_v1", registry_entry.owner);
        try std.testing.expectEqual(algebraic_mod.lexical.DictionaryLayoutKind.fst_postings, registry_entry.layout);
        try std.testing.expectEqualStrings("ready", registry_entry.state);
        _ = read_txn.get(canonical_scalar_registry_key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
    }
    try std.testing.expect(algebraic_mod.ir.selectUniqueAccessPath(&.{text_path}, .{
        .fragment = .automaton_select,
        .output_dims = &.{.doc},
        .dictionary = text_identity,
    }) != null);
    try std.testing.expect(try manager.planTypedAccessPathAlloc(alloc, .{
        .fragment = .automaton_select,
        .layout = .full_text_postings,
        .output_dims = &.{.doc},
        .dictionary = canonical_scalar_identity,
    }) == null);
    try std.testing.expect(manager.fullTextLexicalAccessPath("ft_v1", "title", "keyword") == null);

    const dense_path = manager.denseVectorAccessPath("dv_v1").?;
    try std.testing.expectEqual(algebraic_mod.ir.PhysicalLayout.dense_vector, dense_path.layout);
    try std.testing.expect(algebraic_mod.ir.selectUniqueAccessPath(&.{dense_path}, .{
        .fragment = .vector_search,
        .output_dims = &.{ .doc, .score },
    }) != null);

    const sparse_path = manager.sparseVectorAccessPath("sv_v1").?;
    try std.testing.expectEqual(algebraic_mod.ir.PhysicalLayout.sparse_vector, sparse_path.layout);
    try std.testing.expect(algebraic_mod.ir.selectUniqueAccessPath(&.{sparse_path}, .{
        .fragment = .vector_search,
        .output_dims = &.{ .doc, .score },
    }) != null);
    const sparse_token_path = manager.sparseTokenAccessPath("sv_v1").?;
    try std.testing.expectEqual(algebraic_mod.ir.PhysicalLayout.sparse_token_postings, sparse_token_path.layout);
    const sparse_token_identity = algebraic_mod.lexical.DictionaryIdentity.sparseToken("sv_v1", "sparse", "sv_v1", "u32");
    try std.testing.expect(algebraic_mod.ir.selectUniqueAccessPath(&.{sparse_token_path}, .{
        .fragment = .slice,
        .layout = .sparse_token_postings,
        .output_dims = &.{ .doc, .term, .score },
        .dictionary = sparse_token_identity,
    }) != null);
    try std.testing.expect(try manager.planTypedAccessPathAlloc(alloc, .{
        .fragment = .slice,
        .layout = .sparse_token_postings,
        .output_dims = &.{ .doc, .term, .score },
        .dictionary = sparse_token_identity,
    }) != null);

    const graph_path = manager.graphTraversalAccessPath("graph_v1").?;
    try std.testing.expectEqual(algebraic_mod.ir.PhysicalLayout.graph_edges, graph_path.layout);
    try std.testing.expect(algebraic_mod.ir.selectUniqueAccessPath(&.{graph_path}, .{
        .fragment = .graph_traverse,
        .output_dims = &.{.doc},
        .law_id = .provenance_semiring,
    }) != null);
    try std.testing.expect(manager.graphTraversalAccessPath("missing") == null);

    const text_plan = (try manager.planTypedAccessPathAlloc(alloc, .{
        .fragment = .automaton_select,
        .layout = .full_text_postings,
        .output_dims = &.{.doc},
        .dictionary = algebraic_mod.lexical.DictionaryIdentity.analyzedText("ft_v1", "title", "standard"),
    })).?;
    try std.testing.expectEqualStrings("ft_v1", text_plan.access_path.owner);
    const text_helper_plan = (try manager.planFullTextLexicalAccessPathAlloc(alloc, "ft_v1", "title", null, .automaton_select)).?;
    try std.testing.expectEqualStrings("ft_v1", text_helper_plan.access_path.owner);
    try std.testing.expect(try manager.planFullTextLexicalAccessPathAlloc(alloc, "ft_v1", "title", "keyword", .automaton_select) == null);

    const graph_plan = (try manager.planTypedAccessPathAlloc(alloc, .{
        .fragment = .graph_traverse,
        .layout = .graph_edges,
        .output_dims = &.{.doc},
        .law_id = .provenance_semiring,
    })).?;
    try std.testing.expectEqualStrings("graph_v1", graph_plan.access_path.owner);

    try std.testing.expect(try manager.planTypedAccessPathAlloc(alloc, .{
        .fragment = .vector_search,
        .output_dims = &.{ .doc, .score },
    }) == null);
    const dense_plan = (try manager.planTypedAccessPathAlloc(alloc, .{
        .fragment = .vector_search,
        .layout = .dense_vector,
        .output_dims = &.{ .doc, .score },
    })).?;
    try std.testing.expectEqualStrings("dv_v1", dense_plan.access_path.owner);
    var dense_program = (try algebraic_mod.planner.planVectorSearchTensorProgramAlloc(alloc, dense_plan.access_path.owner, .dense_vector, false)).?;
    defer dense_program.deinit(alloc);
    try std.testing.expectEqualStrings("dv_v1", dense_program.access_paths[0].owner);
    try std.testing.expectEqualStrings("dv_v1", dense_program.steps[0].expr.owner.?);
    try std.testing.expectEqual(algebraic_mod.ir.TensorFragment.vector_search, dense_program.steps[0].expr.fragment);
    try std.testing.expect((try algebraic_mod.ir.tensorProgramProof(alloc, dense_program.access_paths, dense_program.asProgram())).safe());
    try std.testing.expect(algebraic_mod.ir.vectorSearchProgramMatchesTarget(dense_program.asProgram(), "dv_v1", .dense_vector, false));
    const dense_expected_id = try algebraic_mod.ir.tensorProgramIdAlloc(alloc, dense_program.asProgram());
    defer alloc.free(dense_expected_id);
    try std.testing.expectEqualStrings(dense_expected_id, dense_program.program_id);
    var constrained_dense_program = (try algebraic_mod.planner.planVectorSearchTensorProgramAlloc(alloc, dense_plan.access_path.owner, .dense_vector, true)).?;
    defer constrained_dense_program.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), constrained_dense_program.inputs.len);
    try std.testing.expectEqual(algebraic_mod.ir.TensorProgramRef{ .input = 0 }, constrained_dense_program.steps[0].inputs[0]);
    try std.testing.expectEqualStrings("dv_v1", constrained_dense_program.steps[0].expr.owner.?);
    try std.testing.expect((try algebraic_mod.ir.tensorProgramProof(alloc, constrained_dense_program.access_paths, constrained_dense_program.asProgram())).safe());
    try std.testing.expect(algebraic_mod.ir.vectorSearchProgramMatchesTarget(constrained_dense_program.asProgram(), "dv_v1", .dense_vector, true));

    const sparse_plan = (try manager.planTypedAccessPathAlloc(alloc, .{
        .fragment = .vector_search,
        .layout = .sparse_vector,
        .output_dims = &.{ .doc, .score },
    })).?;
    try std.testing.expectEqualStrings("sv_v1", sparse_plan.access_path.owner);
    var sparse_program = (try algebraic_mod.planner.planVectorSearchTensorProgramAlloc(alloc, sparse_plan.access_path.owner, .sparse_vector, false)).?;
    defer sparse_program.deinit(alloc);
    try std.testing.expectEqualStrings("sv_v1", sparse_program.access_paths[0].owner);
    try std.testing.expectEqualStrings("sv_v1", sparse_program.steps[0].expr.owner.?);
    try std.testing.expectEqual(algebraic_mod.ir.TensorFragment.vector_search, sparse_program.steps[0].expr.fragment);

    var graph_program = (try algebraic_mod.planner.planGraphTraversalTensorProgramAlloc(alloc, graph_plan.access_path.owner, false)).?;
    defer graph_program.deinit(alloc);
    try std.testing.expectEqualStrings("graph_v1", graph_program.access_paths[0].owner);
    try std.testing.expectEqualStrings("graph_v1", graph_program.steps[0].expr.owner.?);
    try std.testing.expectEqual(algebraic_mod.ir.TensorFragment.graph_traverse, graph_program.steps[0].expr.fragment);
    try std.testing.expectEqual(algebraic_mod.law.Id.provenance_semiring, graph_program.steps[0].expr.law_id.?);
    try std.testing.expect((try algebraic_mod.ir.tensorProgramProof(alloc, graph_program.access_paths, graph_program.asProgram())).safe());
    try std.testing.expect(algebraic_mod.ir.graphTraversalProgramMatchesTarget(graph_program.asProgram(), "graph_v1", false));
    var constrained_graph_program = (try algebraic_mod.planner.planGraphTraversalTensorProgramAlloc(alloc, graph_plan.access_path.owner, true)).?;
    defer constrained_graph_program.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), constrained_graph_program.inputs.len);
    try std.testing.expectEqual(algebraic_mod.ir.TensorProgramRef{ .input = 0 }, constrained_graph_program.steps[0].inputs[0]);
    try std.testing.expectEqualStrings("graph_v1", constrained_graph_program.steps[0].expr.owner.?);
    try std.testing.expectEqual(algebraic_mod.law.Id.provenance_semiring, constrained_graph_program.steps[0].expr.law_id.?);
    try std.testing.expect((try algebraic_mod.ir.tensorProgramProof(alloc, constrained_graph_program.access_paths, constrained_graph_program.asProgram())).safe());
    try std.testing.expect(algebraic_mod.ir.graphTraversalProgramMatchesTarget(constrained_graph_program.asProgram(), "graph_v1", true));
}

test "full text dictionary publication rejects duplicate semantic owners" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);

    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();

    const identity = algebraic_mod.lexical.DictionaryIdentity.analyzedText("ft_v1", "title", "standard");
    {
        var runtime = try initRuntimeStore(alloc, &store);
        defer runtime.deinit();
        var txn = try runtime.store.beginWrite();
        errdefer txn.abort();
        try std.testing.expectEqual(
            algebraic_mod.lexical.RegistryClaim.claimed,
            try algebraic_mod.lexical.claimRegistryOwnerTxn(alloc, &txn, identity, "algebraic:path-promotion", .lexicon_postings_rows, "ready"),
        );
        try txn.commit();
    }

    var manager = try IndexManager.init(alloc, path);
    defer manager.deinit();
    manager.updateRange(.{ .start = "", .end = "" });

    try std.testing.expectError(error.InvalidIndexConfig, manager.addAllNoBackfill(&store, &.{
        .{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{\"analysis_config\":{\"field_analyzers\":{\"title\":\"standard\"}}}",
        },
    }));
    try std.testing.expect(!manager.has("ft_v1"));

    const registry_key = try identity.registryKeyAlloc(alloc);
    defer alloc.free(registry_key);
    var read_txn = try store.beginReadTxn();
    defer read_txn.abort();
    var registry_entry = try algebraic_mod.lexical.RegistryEntry.decodeAlloc(alloc, try read_txn.get(registry_key));
    defer registry_entry.deinit(alloc);
    try std.testing.expectEqualStrings("algebraic:path-promotion", registry_entry.owner);
    try std.testing.expectEqual(algebraic_mod.lexical.DictionaryLayoutKind.lexicon_postings_rows, registry_entry.layout);
}

test "observed full text analyzers publish shared dictionary ownership" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);

    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();

    var manager = try IndexManager.init(alloc, path);
    defer manager.deinit();
    manager.updateRange(.{ .start = "", .end = "" });

    try manager.addAllNoBackfill(&store, &.{
        .{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{}",
        },
    });

    const observed_field = try alloc.dupe(u8, "meta.body");
    defer alloc.free(observed_field);
    const observed_analyzer = try alloc.dupe(u8, "french");
    defer alloc.free(observed_analyzer);
    const observed = [_]mapper.ObservedFieldAnalyzer{
        .{ .field_name = observed_field, .analyzer_name = observed_analyzer },
    };
    const entry = manager.textIndexEntry("ft_v1").?;
    try mergeObservedTextFieldAnalyzers(&manager, &store, entry, observed[0..]);

    const identity = algebraic_mod.lexical.DictionaryIdentity.analyzedText("ft_v1", "meta.body", "french");
    const registry_key = try identity.registryKeyAlloc(alloc);
    defer alloc.free(registry_key);
    {
        var read_txn = try store.beginReadTxn();
        defer read_txn.abort();
        var registry_entry = try algebraic_mod.lexical.RegistryEntry.decodeAlloc(alloc, try read_txn.get(registry_key));
        defer registry_entry.deinit(alloc);
        try std.testing.expectEqualStrings("ft_v1", registry_entry.owner);
        try std.testing.expectEqual(algebraic_mod.lexical.DictionaryLayoutKind.fst_postings, registry_entry.layout);
        try std.testing.expectEqualStrings("ready", registry_entry.state);
    }

    const plan = (try manager.planFullTextLexicalAccessPathAlloc(alloc, "ft_v1", "meta.body", "french", .automaton_select)).?;
    try std.testing.expectEqualStrings("ft_v1", plan.access_path.owner);
    try std.testing.expect(algebraic_mod.lexical.DictionaryIdentity.eql(identity, plan.access_path.dictionary.?));

    var runtime = try initRuntimeStore(alloc, &store);
    defer runtime.deinit();
    var txn = try runtime.store.beginWrite();
    defer txn.abort();
    try std.testing.expectEqual(
        algebraic_mod.lexical.RegistryClaim.owned_by_other,
        try algebraic_mod.lexical.claimRegistryOwnerTxn(alloc, &txn, identity, "algebraic:path-promotion", .lexicon_postings_rows, "ready"),
    );
}

test "dense bulk-ingest uses recursive bulk build for large empty index batch" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);

    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();

    var manager = try IndexManager.init(alloc, path);
    defer manager.deinit();
    manager.updateRange(.{ .start = "", .end = "" });

    try manager.addAllNoBackfill(&store, &.{
        .{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"leaf_size\":16,\"branching_factor\":8}",
        },
    });
    const entry = manager.denseIndex("dv_v1") orelse return error.IndexNotFound;
    const before_profile = entry.index.getWriteProfile();

    const previous_threshold_cache = hbc_bulk_ingest_bulk_build_min_items_cache.load(.monotonic);
    hbc_bulk_ingest_bulk_build_min_items_cache.store(33, .monotonic);
    defer hbc_bulk_ingest_bulk_build_min_items_cache.store(previous_threshold_cache, .monotonic);
    const count = IndexManager.hbcBulkIngestBulkBuildMinItems();
    const index_name = try alloc.dupe(u8, "dv_v1");
    defer alloc.free(index_name);
    const writes = try alloc.alloc(mapper.DenseEmbeddingWrite, count);
    defer alloc.free(writes);
    const doc_keys = try alloc.alloc([]u8, count);
    defer {
        for (doc_keys) |key| alloc.free(key);
        alloc.free(doc_keys);
    }
    const vectors = try alloc.alloc(f32, count * 2);
    defer alloc.free(vectors);

    for (writes, 0..) |*write, i| {
        doc_keys[i] = try std.fmt.allocPrint(alloc, "doc:{d:0>6}", .{i});
        const vector = vectors[i * 2 ..][0..2];
        vector[0] = @as(f32, @floatFromInt(i % 257)) / 257.0;
        vector[1] = @as(f32, @floatFromInt((i * 17) % 263)) / 263.0;
        write.* = .{
            .index_name = index_name,
            .doc_key = doc_keys[i],
            .vector = vector,
            .artifact_key = null,
        };
    }

    try manager.applyDenseEmbeddingWritesByNameWithOptions(&store, "dv_v1", writes, .{ .mode = .bulk_ingest });

    const after_profile = entry.index.getWriteProfile();
    try std.testing.expectEqual(@as(u64, @intCast(count)), entry.index.stats().active_count);
    try std.testing.expect(after_profile.bulk_build_tree_ns > before_profile.bulk_build_tree_ns);
    try std.testing.expectEqual(before_profile.insert_calls, after_profile.insert_calls);
}

test "dense bulk-ingest populates primary ordinal vector cache before first lookup" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);

    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();

    var manager = try IndexManager.init(alloc, path);
    defer manager.deinit();
    manager.updateRange(.{ .start = "", .end = "" });

    try manager.addAllNoBackfill(&store, &.{
        .{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"embedding_name\":\"dv_v1\",\"external\":true}",
        },
    });

    const doc_ids = [_][]const u8{ "doc:a", "doc:b", "doc:c" };
    var identity_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer {
        for (identity_writes.items) |item| {
            alloc.free(@constCast(item.key));
            alloc.free(@constCast(item.value));
        }
        identity_writes.deinit(alloc);
    }
    try doc_identity.appendBatchIdentityMetadataAlloc(
        alloc,
        &store,
        0,
        0,
        1,
        &identity_writes,
        doc_ids[0..],
        &.{},
    );
    try store.putBatchWithReplay(null, identity_writes.items, &.{}, null);

    const dense_index_name = try alloc.dupe(u8, "dv_v1");
    defer alloc.free(dense_index_name);
    const doc_a = try alloc.dupe(u8, "doc:a");
    defer alloc.free(doc_a);
    const doc_b = try alloc.dupe(u8, "doc:b");
    defer alloc.free(doc_b);
    const doc_c = try alloc.dupe(u8, "doc:c");
    defer alloc.free(doc_c);
    var vector_a = [_]f32{ 1.0, 0.0 };
    var vector_b = [_]f32{ 0.0, 1.0 };
    var vector_c = [_]f32{ 1.0, 1.0 };

    const writes = [_]mapper.DenseEmbeddingWrite{
        .{
            .index_name = dense_index_name,
            .doc_key = doc_a,
            .vector = vector_a[0..],
            .artifact_key = null,
        },
        .{
            .index_name = dense_index_name,
            .doc_key = doc_b,
            .vector = vector_b[0..],
            .artifact_key = null,
        },
        .{
            .index_name = dense_index_name,
            .doc_key = doc_c,
            .vector = vector_c[0..],
            .artifact_key = null,
        },
    };

    try manager.beginDenseBulkIngestSessionByName("dv_v1");
    var session_open = true;
    errdefer if (session_open) manager.abortDenseBulkIngestSessionByName("dv_v1");
    try manager.applyDenseEmbeddingWritesByNameWithOptions(&store, "dv_v1", &writes, .{ .mode = .bulk_ingest });
    try manager.finishDenseBulkIngestSessionByNameWithOptions("dv_v1", .{});
    session_open = false;

    const entry = manager.denseIndex("dv_v1") orelse return error.IndexNotFound;
    try std.testing.expectEqual(@as(usize, doc_ids.len), entry.ordinal_vector_ids.count());
    try std.testing.expectEqual(@as(usize, doc_ids.len), entry.vector_ordinals.count());
    try std.testing.expectEqual(@as(u64, 0), entry.index.hbcCacheStats().vector.used_bytes);
    try std.testing.expectEqual(@as(?u64, deterministicDenseVectorId("doc:a")), entry.ordinal_vector_ids.get(1));
    try std.testing.expectEqual(@as(?u64, deterministicDenseVectorId("doc:b")), entry.ordinal_vector_ids.get(2));
    try std.testing.expectEqual(@as(?u64, deterministicDenseVectorId("doc:c")), entry.ordinal_vector_ids.get(3));
}

test "dense embedding writes prefer inline vectors over artifact reloads" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);

    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();

    var manager = try IndexManager.init(alloc, path);
    defer manager.deinit();
    manager.updateRange(.{ .start = "", .end = "" });

    try manager.addAllNoBackfill(&store, &.{
        .{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
        },
    });

    const stored_key = try internal_keys.documentKeyAlloc(alloc, "doc:inline");
    defer alloc.free(stored_key);
    try store.put(stored_key, "{\"title\":\"dense\"}");

    const dense_index_name = try alloc.dupe(u8, "dv_v1");
    defer alloc.free(dense_index_name);
    const dense_doc_key = try alloc.dupe(u8, "doc:inline");
    defer alloc.free(dense_doc_key);
    const dense_vector = try alloc.dupe(f32, &[_]f32{ 1.0, 2.0, 3.0 });
    defer alloc.free(dense_vector);
    const bogus_artifact_key = try alloc.dupe(u8, "artifact:missing:inline");
    defer alloc.free(bogus_artifact_key);
    const writes = [_]mapper.DenseEmbeddingWrite{
        .{
            .index_name = dense_index_name,
            .doc_key = dense_doc_key,
            .artifact_key = bogus_artifact_key,
            .vector = dense_vector,
        },
    };
    try manager.applyDenseEmbeddingWritesByName(&store, "dv_v1", &writes);

    const entry = manager.denseIndex("dv_v1") orelse return error.IndexNotFound;
    try std.testing.expectEqual(@as(u64, 1), entry.index.stats().active_count);
    try std.testing.expectEqual(@as(u64, 0), entry.index.hbcCacheStats().vector.used_bytes);

    const vector_id = deterministicDenseVectorId("doc:inline");
    const metadata = (try entry.index.getMetadata(vector_id)) orelse return error.TestUnexpectedResult;
    defer alloc.free(metadata);
    try std.testing.expectEqualStrings("doc:inline", metadata);
}

test "loadConfiguredIndexesParallel returns worker errors without double-joining threads" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);

    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();

    var manager = try IndexManager.init(alloc, path);
    defer manager.deinit();
    manager.updateRange(.{ .start = "", .end = "" });

    const configs = [_]types.IndexConfig{
        .{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{\"field\":\"title\"}",
        },
        .{
            .name = "dv_bad",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"embedding_name\":\"missing\"}",
        },
    };

    for (configs) |cfg| {
        try manager.ensureConfiguredIndexDir(cfg);
    }

    try std.testing.expectError(error.InvalidIndexConfig, manager.loadConfiguredIndexesParallel(&store, &configs, 2, true, false));
}

test "dense apply resource manager accounts working bytes and releases them" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);

    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();

    var resource_manager = resource_manager_mod.ResourceManager.init(.{});
    var manager = try IndexManager.initWithOptions(alloc, path, .{
        .resource_manager = &resource_manager,
    });
    defer manager.deinit();
    manager.updateRange(.{ .start = "", .end = "" });

    try manager.addAllNoBackfill(&store, &.{
        .{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\"}",
        },
    });

    const stored_key = try internal_keys.documentKeyAlloc(alloc, "doc:tracked");
    defer alloc.free(stored_key);
    try store.put(stored_key, "{\"title\":\"dense\"}");

    const dense_index_name = try alloc.dupe(u8, "dv_v1");
    defer alloc.free(dense_index_name);
    const dense_doc_key = try alloc.dupe(u8, "doc:tracked");
    defer alloc.free(dense_doc_key);
    const dense_vector = try alloc.dupe(f32, &[_]f32{ 1.0, 2.0, 3.0 });
    defer alloc.free(dense_vector);
    const writes = [_]mapper.DenseEmbeddingWrite{
        .{
            .index_name = dense_index_name,
            .doc_key = dense_doc_key,
            .vector = dense_vector,
            .artifact_key = null,
        },
    };
    try manager.applyDenseEmbeddingWritesByName(&store, "dv_v1", &writes);

    const stats = resource_manager.snapshot().slices[@intFromEnum(resource_manager_mod.Slice.dense_apply_working_set)];
    try std.testing.expectEqual(@as(u64, 0), stats.used_bytes);
    try std.testing.expect(stats.peak_bytes >= (@as(u64, 3 * @sizeOf(f32)) + @as(u64, "doc:tracked".len)));
}

test "dense replay-shaped bulk apply skips identical already indexed vector" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);

    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();

    var manager = try IndexManager.init(alloc, path);
    defer manager.deinit();
    manager.updateRange(.{ .start = "", .end = "" });

    try manager.addAllNoBackfill(&store, &.{
        .{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\"}",
        },
    });

    const dense_index_name = try alloc.dupe(u8, "dv_v1");
    defer alloc.free(dense_index_name);
    const dense_doc_key = try alloc.dupe(u8, "doc:same");
    defer alloc.free(dense_doc_key);
    const first_vector = try alloc.dupe(f32, &[_]f32{ 1.0, 2.0, 3.0 });
    defer alloc.free(first_vector);
    const writes = [_]mapper.DenseEmbeddingWrite{
        .{
            .index_name = dense_index_name,
            .doc_key = dense_doc_key,
            .vector = first_vector,
            .artifact_key = null,
        },
    };

    try manager.applyDenseEmbeddingWritesByName(&store, "dv_v1", &writes);

    const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, dense_doc_key, "dv_v1");
    defer alloc.free(artifact_key);
    try store.delete(artifact_key);

    const entry = manager.denseIndex("dv_v1") orelse return error.IndexNotFound;
    const before_profile = entry.index.getWriteProfile();
    try manager.applyDenseEmbeddingWritesByNameWithOptions(&store, "dv_v1", &writes, .{ .mode = .bulk_ingest });
    const after_profile = entry.index.getWriteProfile();

    try std.testing.expectEqual(@as(u64, 1), entry.index.stats().active_count);
    try std.testing.expectEqual(before_profile.insert_calls, after_profile.insert_calls);
    try std.testing.expectEqual(before_profile.ns_vecs_put_calls, after_profile.ns_vecs_put_calls);
    const artifact_payload = try store.get(alloc, artifact_key);
    defer alloc.free(artifact_payload);
    try std.testing.expect(artifact_payload.len > 0);
}

test "dense artifact-only replay apply remains searchable after incremental catch-up" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = indexManagerTmpPathWithSuffix(&path_buf, "dense-artifact-replay-search");
    defer cleanupIndexManagerDir(path);

    var store = try docstore_mod.DocStore.open(alloc, path, .{});
    defer store.close();

    var manager = try IndexManager.init(alloc, std.mem.span(path));
    defer manager.deinit();
    manager.updateRange(.{ .start = "", .end = "" });
    manager.primary_store = &store;

    try manager.addAllNoBackfill(&store, &.{
        .{
            .name = "semantic_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"cosine\",\"embedding_name\":\"semantic_idx\",\"external\":true}",
        },
    });
    const entry = manager.denseIndex("semantic_idx") orelse return error.IndexNotFound;
    const before_replay_profile = entry.index.getWriteProfile();

    const docs = [_][]const u8{ "doc:a", "doc:b", "doc:c" };
    for (docs) |doc_key| {
        const stored_key = try internal_keys.documentKeyAlloc(alloc, doc_key);
        defer alloc.free(stored_key);
        try store.put(stored_key, "{\"title\":\"dense\"}");
    }

    const artifact_a = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "semantic_idx");
    defer alloc.free(artifact_a);
    const artifact_b = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:b", "semantic_idx");
    defer alloc.free(artifact_b);
    const artifact_c = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:c", "semantic_idx");
    defer alloc.free(artifact_c);

    const payload_a = try enrichment_artifact_codec.encodeDenseEmbeddingAlloc(alloc, null, &[_]f32{ 1.0, 0.0, 0.0 });
    defer alloc.free(payload_a);
    const payload_b = try enrichment_artifact_codec.encodeDenseEmbeddingAlloc(alloc, null, &[_]f32{ 0.0, 1.0, 0.0 });
    defer alloc.free(payload_b);
    const payload_c = try enrichment_artifact_codec.encodeDenseEmbeddingAlloc(alloc, null, &[_]f32{ 0.0, 0.0, 1.0 });
    defer alloc.free(payload_c);
    try store.put(artifact_a, payload_a);
    try store.put(artifact_b, payload_b);
    try store.put(artifact_c, payload_c);

    var first_write = [_]mapper.DenseEmbeddingWrite{
        .{
            .index_name = try alloc.dupe(u8, "semantic_idx"),
            .doc_key = try alloc.dupe(u8, "doc:a"),
            .artifact_key = try alloc.dupe(u8, artifact_a),
            .vector = &.{},
        },
    };
    defer {
        alloc.free(first_write[0].index_name);
        alloc.free(first_write[0].doc_key);
        alloc.free(@constCast(first_write[0].artifact_key.?));
    }
    try manager.beginDenseBulkIngestSessionByName("semantic_idx");
    var first_session_open = true;
    errdefer if (first_session_open) manager.abortDenseBulkIngestSessionByName("semantic_idx");
    try manager.applyDenseEmbeddingWritesByNameWithOptions(&store, "semantic_idx", &first_write, .{ .mode = .bulk_ingest });
    try manager.finishDenseBulkIngestSessionByNameWithOptions("semantic_idx", .{});
    first_session_open = false;
    const after_first_replay_profile = entry.index.getWriteProfile();
    try std.testing.expectEqual(before_replay_profile.bulk_build_tree_ns, after_first_replay_profile.bulk_build_tree_ns);

    var second_writes = [_]mapper.DenseEmbeddingWrite{
        .{
            .index_name = try alloc.dupe(u8, "semantic_idx"),
            .doc_key = try alloc.dupe(u8, "doc:a"),
            .artifact_key = try alloc.dupe(u8, artifact_a),
            .vector = &.{},
        },
        .{
            .index_name = try alloc.dupe(u8, "semantic_idx"),
            .doc_key = try alloc.dupe(u8, "doc:b"),
            .artifact_key = try alloc.dupe(u8, artifact_b),
            .vector = &.{},
        },
        .{
            .index_name = try alloc.dupe(u8, "semantic_idx"),
            .doc_key = try alloc.dupe(u8, "doc:c"),
            .artifact_key = try alloc.dupe(u8, artifact_c),
            .vector = &.{},
        },
    };
    defer {
        for (second_writes) |write| {
            alloc.free(write.index_name);
            alloc.free(write.doc_key);
            alloc.free(@constCast(write.artifact_key.?));
        }
    }
    try manager.beginDenseBulkIngestSessionByName("semantic_idx");
    var second_session_open = true;
    errdefer if (second_session_open) manager.abortDenseBulkIngestSessionByName("semantic_idx");
    try manager.applyDenseEmbeddingWritesByNameWithOptions(&store, "semantic_idx", &second_writes, .{ .mode = .bulk_ingest });
    try manager.finishDenseBulkIngestSessionByNameWithOptions("semantic_idx", .{});
    second_session_open = false;
    const after_second_replay_profile = entry.index.getWriteProfile();
    try std.testing.expectEqual(before_replay_profile.bulk_build_tree_ns, after_second_replay_profile.bulk_build_tree_ns);

    try std.testing.expectEqual(@as(u64, 3), entry.index.stats().active_count);
    entry.index.setCacheEnabled(false);
    entry.index.setCacheEnabled(true);

    var results = try entry.index.searchWithRequest(.{
        .query = &[_]f32{ 1.0, 0.0, 0.0 },
        .k = 3,
    });
    defer results.deinit();

    const raw_hits = results.getHits();
    try std.testing.expect(raw_hits.len > 0);
    try std.testing.expect(raw_hits[0].metadata != null);
    try std.testing.expectEqualStrings("doc:a", raw_hits[0].metadata.?);
}

test "dense vector load session caches decoded vectors and tracks bytes" {
    const alloc = std.testing.allocator;

    var manager = try IndexManager.init(alloc, ".");
    defer manager.deinit();

    const context = try alloc.create(IndexManager.DenseVectorLoadContext);
    defer {
        context.deinit(alloc);
    }
    context.* = .{
        .manager = &manager,
        .index_name = try alloc.dupe(u8, "dv_v1"),
        .max_cached_vectors = 100_000,
    };

    var session: IndexManager.DenseVectorLoadSession = .{
        .context = context,
    };
    defer session.deinit();

    try session.cacheVector(7, &[_]f32{ 7.0, 8.0, 9.0 });
    try std.testing.expectEqual(@as(u64, 3 * @sizeOf(f32)), session.vector_cache_bytes);
    try std.testing.expectEqual(@as(u64, 3 * @sizeOf(f32)), session.working_bytes_current);

    const cached = session.getVector(7) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualSlices(f32, &[_]f32{ 7.0, 8.0, 9.0 }, cached);
    try std.testing.expectEqual(@as(u64, 1), session.vector_cache_hits);
    try std.testing.expectEqual(@as(u64, 0), session.vector_cache_misses);
}

test "dense vector load session bounds cache by vector cap and apply working-set budget" {
    const alloc = std.testing.allocator;

    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@intFromEnum(resource_manager_mod.Slice.dense_apply_working_set)] = .{
        .soft_limit_bytes = 8,
        .hard_limit_bytes = 16,
    };
    var resource_manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });

    var manager = try IndexManager.init(alloc, ".");
    defer manager.deinit();
    manager.resource_manager = &resource_manager;

    const capped_context = try alloc.create(IndexManager.DenseVectorLoadContext);
    defer capped_context.deinit(alloc);
    capped_context.* = .{
        .manager = &manager,
        .index_name = try alloc.dupe(u8, "dv_v1"),
        .max_cached_vectors = 1,
    };

    var capped_session: IndexManager.DenseVectorLoadSession = .{
        .context = capped_context,
    };
    try capped_session.cacheVector(1, &[_]f32{ 1.0, 2.0 });
    try capped_session.cacheVector(2, &[_]f32{ 3.0, 4.0 });
    try std.testing.expectEqual(@as(usize, 1), capped_session.vector_cache.count());
    capped_session.deinit();

    const budget_context = try alloc.create(IndexManager.DenseVectorLoadContext);
    defer budget_context.deinit(alloc);
    budget_context.* = .{
        .manager = &manager,
        .index_name = try alloc.dupe(u8, "dv_v1"),
        .max_cached_vectors = 100,
    };

    var budget_session: IndexManager.DenseVectorLoadSession = .{
        .context = budget_context,
    };
    try budget_session.cacheVector(1, &[_]f32{ 1.0, 2.0, 3.0 });
    try budget_session.cacheVector(2, &[_]f32{ 4.0, 5.0, 6.0 });
    try std.testing.expectEqual(@as(usize, 1), budget_session.vector_cache.count());
    budget_session.deinit();

    const stats = resource_manager.snapshot().slices[@intFromEnum(resource_manager_mod.Slice.dense_apply_working_set)];
    try std.testing.expectEqual(@as(u64, 0), stats.used_bytes);
    try std.testing.expect(stats.hard_limit_rejections > 0);
}

test "dense vector scratch loader populates session vector cache" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = indexManagerTmpPathWithSuffix(&path_buf, "dense-vector-scratch-cache");
    defer cleanupIndexManagerDir(path);

    var store = try docstore_mod.DocStore.open(alloc, path, .{});
    defer store.close();

    var manager = try IndexManager.init(alloc, std.mem.span(path));
    defer manager.deinit();
    manager.updateRange(.{ .start = "", .end = "" });
    manager.primary_store = &store;

    try manager.addAllNoBackfill(&store, &.{
        .{
            .name = "semantic_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}",
        },
    });

    const doc_key = "doc:a";
    const stored_key = try internal_keys.documentKeyAlloc(alloc, doc_key);
    defer alloc.free(stored_key);
    try store.put(stored_key, "{\"title\":\"dense\"}");

    const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, doc_key, "semantic_idx");
    defer alloc.free(artifact_key);
    const payload = try enrichment_artifact_codec.encodeDenseEmbeddingAlloc(alloc, null, &[_]f32{ 1, 2 });
    defer alloc.free(payload);
    try store.put(artifact_key, payload);

    const entry = manager.denseIndex("semantic_idx") orelse return error.IndexNotFound;
    const context = entry.vector_loader_context orelse return error.TestUnexpectedResult;

    var session: IndexManager.DenseVectorLoadSession = .{
        .context = context,
    };
    defer session.deinit();
    const previous_session = IndexManager.active_dense_vector_load_session;
    defer IndexManager.active_dense_vector_load_session = previous_session;
    IndexManager.active_dense_vector_load_session = &session;

    var scratch_a: [2]f32 = undefined;
    var scratch_b: [2]f32 = undefined;

    const first = try IndexManager.loadDenseVectorForHbcIntoScratch(context, 7, doc_key, &scratch_a);
    const second = try IndexManager.loadDenseVectorForHbcIntoScratch(context, 7, doc_key, &scratch_b);

    try std.testing.expectEqualSlices(f32, &[_]f32{ 1, 2 }, first);
    try std.testing.expectEqualSlices(f32, &[_]f32{ 1, 2 }, second);
    try std.testing.expectEqual(@as(u64, 1), session.vector_cache_hits);
    try std.testing.expectEqual(@as(u64, 1), session.vector_cache_misses);
    try std.testing.expectEqual(@as(u64, 2 * @sizeOf(f32)), session.vector_cache_bytes);
    try std.testing.expectEqual(@as(usize, 1), session.vector_cache.count());
}

test "remove drops generated embedding artifacts while retaining reusable chunk artifacts and shorthand enrichments" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = indexManagerTmpPathWithSuffix(&path_buf, "managed-artifact-remove");
    defer cleanupIndexManagerDir(path);

    var store = try docstore_mod.DocStore.open(alloc, path, .{});
    defer store.close();

    var manager = try IndexManager.init(alloc, std.mem.span(path));
    defer manager.deinit();
    manager.updateRange(.{ .start = "", .end = "" });

    try manager.addAllNoBackfill(&store, &.{
        .{
            .name = "semantic_idx",
            .kind = .dense_vector,
            .config_json =
            \\{"field":"embedding","dims":3,"metric":"cosine","generator":{"kind":"dense_embedding","source_field":"body","artifact_name":"body_chunks","embedding_name":"semantic_idx","chunk_size":256}}
            ,
        },
    });

    try std.testing.expect(manager.getEnrichment(.chunk, "body_chunks") != null);
    try std.testing.expect(manager.getEnrichment(.embedding, "semantic_idx") != null);

    const doc_internal_key = try internal_keys.documentKeyAlloc(alloc, "doc:a");
    defer alloc.free(doc_internal_key);
    try store.put(doc_internal_key, "{\"body\":\"alpha concept overview\"}");

    const chunk_artifact_key = try internal_keys.chunkArtifactKeyAlloc(alloc, "doc:a", "body_chunks", 0);
    defer alloc.free(chunk_artifact_key);
    try store.put(chunk_artifact_key, "chunk-payload");

    const embedding_artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "semantic_idx");
    defer alloc.free(embedding_artifact_key);
    try store.put(embedding_artifact_key, "bad-artifact");

    try std.testing.expect(try manager.remove(&store, "semantic_idx"));

    try std.testing.expect(manager.getEnrichment(.chunk, "body_chunks") != null);
    try std.testing.expect(manager.getEnrichment(.embedding, "semantic_idx") != null);

    const stored_doc = try store.get(alloc, doc_internal_key);
    defer alloc.free(stored_doc);
    try std.testing.expectEqualStrings("{\"body\":\"alpha concept overview\"}", stored_doc);

    const stored_chunk = try store.get(alloc, chunk_artifact_key);
    defer alloc.free(stored_chunk);
    try std.testing.expectEqualStrings("chunk-payload", stored_chunk);
    try std.testing.expectError(error.NotFound, store.get(alloc, embedding_artifact_key));
}

test "remove status-only dense config drops owned generated artifacts" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = indexManagerTmpPathWithSuffix(&path_buf, "status-only-managed-artifact-remove");
    defer cleanupIndexManagerDir(path);

    var store = try docstore_mod.DocStore.open(alloc, path, .{});
    defer store.close();

    var manager = try IndexManager.init(alloc, std.mem.span(path));
    defer manager.deinit();
    manager.updateRange(.{ .start = "", .end = "" });

    manager.status_only_index_configs = try alloc.alloc(types.IndexConfig, 1);
    manager.status_only_index_configs[0] = try types.IndexConfig.clone(alloc, .{
        .name = "semantic_idx",
        .kind = .dense_vector,
        .config_json =
        \\{"field":"embedding","dims":3,"metric":"cosine","generator":{"kind":"dense_embedding","source_field":"body","artifact_name":"body_chunks","embedding_name":"semantic_idx","chunk_size":256}}
        ,
    });

    const doc_internal_key = try internal_keys.documentKeyAlloc(alloc, "doc:a");
    defer alloc.free(doc_internal_key);
    try store.put(doc_internal_key, "{\"body\":\"alpha concept overview\"}");

    const chunk_artifact_key = try internal_keys.chunkArtifactKeyAlloc(alloc, "doc:a", "body_chunks", 0);
    defer alloc.free(chunk_artifact_key);
    try store.put(chunk_artifact_key, "chunk-payload");

    const embedding_artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "semantic_idx");
    defer alloc.free(embedding_artifact_key);
    try store.put(embedding_artifact_key, "embedding-payload");

    try std.testing.expect(try manager.remove(&store, "semantic_idx"));
    try std.testing.expectError(error.NotFound, store.get(alloc, chunk_artifact_key));
    try std.testing.expectError(error.NotFound, store.get(alloc, embedding_artifact_key));

    const stored_doc = try store.get(alloc, doc_internal_key);
    defer alloc.free(stored_doc);
    try std.testing.expectEqualStrings("{\"body\":\"alpha concept overview\"}", stored_doc);
}

test "dense artifact preload session reuses cached raw values across calls" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);

    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();

    var manager = try IndexManager.init(alloc, path);
    defer manager.deinit();

    const context = try alloc.create(IndexManager.DenseVectorLoadContext);
    defer context.deinit(alloc);
    context.* = .{
        .manager = &manager,
        .index_name = try alloc.dupe(u8, "dv_v1"),
        .max_cached_vectors = 100_000,
    };

    const artifact_key = try alloc.dupe(u8, "artifact:a");
    defer alloc.free(artifact_key);
    const payload = try enrichment_artifact_codec.encodeDenseEmbeddingAlloc(alloc, null, &[_]f32{ 1, 2 });
    defer alloc.free(payload);
    try store.put(artifact_key, payload);

    const index_name = try alloc.dupe(u8, "dv_v1");
    defer alloc.free(index_name);
    const doc_key = try alloc.dupe(u8, "doc:a");
    defer alloc.free(doc_key);
    const empty_vector = try alloc.alloc(f32, 0);
    defer alloc.free(empty_vector);
    const writes = [_]mapper.DenseEmbeddingWrite{
        .{
            .index_name = index_name,
            .doc_key = doc_key,
            .artifact_key = artifact_key,
            .vector = empty_vector,
        },
    };
    const keep_write = try alloc.alloc(bool, writes.len);
    defer alloc.free(keep_write);
    @memset(keep_write, true);

    var session: IndexManager.DenseVectorLoadSession = .{
        .context = context,
    };
    defer session.deinit();

    var corrupt_artifact_deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer corrupt_artifact_deletes.deinit(alloc);

    var out_vectors = try alloc.alloc(?[]const f32, writes.len);
    defer {
        alloc.free(out_vectors);
    }
    for (out_vectors) |*slot| slot.* = null;
    var fallback_scratch = std.ArrayListUnmanaged(f32).empty;
    defer fallback_scratch.deinit(alloc);

    try std.testing.expect(try manager.preloadDenseEmbeddingArtifactVectorsFromSession(
        "dv_v1",
        2,
        &writes,
        keep_write,
        &store,
        &session,
        out_vectors,
        &fallback_scratch,
        &corrupt_artifact_deletes,
    ));
    try std.testing.expectEqualSlices(f32, &[_]f32{ 1, 2 }, out_vectors[0].?);
    try std.testing.expectEqual(@as(u64, 1), session.raw_cache_misses);
    try std.testing.expectEqual(@as(u64, 0), session.raw_cache_hits);

    out_vectors[0] = null;

    try std.testing.expect(try manager.preloadDenseEmbeddingArtifactVectorsFromSession(
        "dv_v1",
        2,
        &writes,
        keep_write,
        &store,
        &session,
        out_vectors,
        &fallback_scratch,
        &corrupt_artifact_deletes,
    ));
    try std.testing.expectEqualSlices(f32, &[_]f32{ 1, 2 }, out_vectors[0].?);
    try std.testing.expectEqual(@as(u64, 1), session.raw_cache_misses);
    try std.testing.expectEqual(@as(u64, 1), session.raw_cache_hits);
}

test "dense mapping commit failure rolls back inserted HBC vectors" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);

    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();

    var manager = try IndexManager.init(alloc, path);
    defer manager.deinit();
    manager.updateRange(.{ .start = "", .end = "" });

    try manager.addAllNoBackfill(&store, &.{
        .{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\"}",
        },
    });

    const entry = manager.denseIndex("dv_v1") orelse return error.IndexNotFound;
    try entry.index.batchInsertWithMetadata(&.{
        .{
            .vector_id = 1,
            .vector = &[_]f32{ 1.0, 2.0, 3.0 },
            .metadata = "doc:rollback",
        },
    });
    try std.testing.expectEqual(@as(u64, 1), entry.index.stats().active_count);

    const DummyCursor = struct {
        pub fn close(_: *@This()) void {}
        pub fn first(_: *@This()) !?backend_erased.Entry {
            return null;
        }
        pub fn last(_: *@This()) !?backend_erased.Entry {
            return null;
        }
        pub fn next(_: *@This()) !?backend_erased.Entry {
            return null;
        }
        pub fn prev(_: *@This()) !?backend_erased.Entry {
            return null;
        }
        pub fn seekAtOrAfter(_: *@This(), _: []const u8) !?backend_erased.Entry {
            return null;
        }
        pub fn seekAtOrBefore(_: *@This(), _: []const u8) !?backend_erased.Entry {
            return null;
        }
    };

    const Shared = struct {
        commits: usize = 0,
        puts: usize = 0,
        aborted: bool = false,
    };

    const MockRead = struct {
        pub fn abort(_: *@This()) void {}
        pub fn get(_: *@This(), _: []const u8) ![]const u8 {
            return error.NotFound;
        }
        pub fn openCursor(_: *@This()) !DummyCursor {
            return .{};
        }
    };

    const MockWrite = struct {
        pub fn abort(_: *@This()) void {}
        pub fn commit(_: *@This()) !void {}
        pub fn get(_: *@This(), _: []const u8) ![]const u8 {
            return error.NotFound;
        }
        pub fn put(_: *@This(), _: []const u8, _: []const u8) !void {}
        pub fn delete(_: *@This(), _: []const u8) !void {}
        pub fn openCursor(_: *@This()) !DummyCursor {
            return .{};
        }
    };

    const MockBatch = struct {
        shared: *Shared,

        pub fn abort(self: *@This()) void {
            self.shared.aborted = true;
        }
        pub fn commit(self: *@This()) !void {
            self.shared.commits += 1;
            return error.CommitFailed;
        }
        pub fn get(_: *@This(), _: []const u8) ![]const u8 {
            return error.NotFound;
        }
        pub fn put(self: *@This(), _: []const u8, _: []const u8) !void {
            self.shared.puts += 1;
        }
        pub fn delete(_: *@This(), _: []const u8) !void {}
    };

    const MockStore = struct {
        shared: *Shared,

        pub fn capabilities(_: *@This()) backend_erased.types.Capabilities {
            return .{};
        }

        pub fn beginRead(_: *@This()) !MockRead {
            return .{};
        }

        pub fn beginWrite(_: *@This()) !MockWrite {
            return .{};
        }

        pub fn beginBatch(self: *@This()) !MockBatch {
            return .{ .shared = self.shared };
        }
    };

    var shared = Shared{};

    try std.testing.expectError(
        error.CommitFailed,
        manager.persistDenseVectorMappingsWithRollback(
            MockStore{ .shared = &shared },
            entry,
            "dv_v1",
            &.{
                .{
                    .doc_key = "doc:rollback",
                    .vector_id = 1,
                },
            },
        ),
    );

    try std.testing.expectEqual(@as(usize, 1), shared.commits);
    try std.testing.expectEqual(@as(usize, 2), shared.puts);
    try std.testing.expect(shared.aborted);
    try std.testing.expectEqual(@as(u64, 0), entry.index.stats().active_count);

    const metadata = try entry.index.getMetadata(1);
    defer if (metadata) |value| alloc.free(value);
    try std.testing.expect(metadata == null);
}

test "dense index manager accepts external embedding indexes without enrichments" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);

    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();

    var manager = try IndexManager.init(alloc, path);
    defer manager.deinit();
    manager.updateRange(.{ .start = "", .end = "" });

    try manager.addAllNoBackfill(&store, &.{
        .{
            .name = "semantic_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"cosine\",\"embedding_name\":\"semantic_idx\",\"external\":true}",
        },
    });

    const stored_key = try internal_keys.documentKeyAlloc(alloc, "doc:00000000");
    defer alloc.free(stored_key);
    try store.put(stored_key, "{\"title\":\"dense\"}");

    const dense_index_name = try alloc.dupe(u8, "semantic_idx");
    defer alloc.free(dense_index_name);
    const dense_doc_key = try alloc.dupe(u8, "doc:00000000");
    defer alloc.free(dense_doc_key);
    const dense_vector = try alloc.dupe(f32, &[_]f32{ 1.0, 2.0, 3.0 });
    defer alloc.free(dense_vector);
    const writes = [_]mapper.DenseEmbeddingWrite{
        .{
            .index_name = dense_index_name,
            .doc_key = dense_doc_key,
            .vector = dense_vector,
            .artifact_key = null,
        },
    };
    try manager.applyDenseEmbeddingWritesByName(&store, "semantic_idx", &writes);

    const entry = manager.denseIndex("semantic_idx") orelse return error.IndexNotFound;
    try std.testing.expectEqual(@as(u64, 1), entry.index.stats().active_count);
}

test "external dense embedding writes persist deterministic vector mappings" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);

    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();

    var manager = try IndexManager.init(alloc, path);
    defer manager.deinit();
    manager.updateRange(.{ .start = "", .end = "" });

    try manager.addAllNoBackfill(&store, &.{
        .{
            .name = "semantic_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"cosine\",\"embedding_name\":\"semantic_idx\",\"external\":true}",
        },
    });

    const stored_key = try internal_keys.documentKeyAlloc(alloc, "doc:00000000");
    defer alloc.free(stored_key);
    try store.put(stored_key, "{\"title\":\"dense\"}");

    const writes = [_]mapper.DenseEmbeddingWrite{
        .{
            .index_name = try alloc.dupe(u8, "semantic_idx"),
            .doc_key = try alloc.dupe(u8, "doc:00000000"),
            .vector = try alloc.dupe(f32, &[_]f32{ 1.0, 2.0, 3.0 }),
            .artifact_key = null,
        },
    };
    defer {
        alloc.free(writes[0].index_name);
        alloc.free(writes[0].doc_key);
        alloc.free(writes[0].vector);
    }

    try manager.applyDenseEmbeddingWritesByName(&store, "semantic_idx", &writes);

    const vector_id = deterministicDenseVectorId("doc:00000000");
    const mapping_key = try denseVectorIdMappingKey(alloc, "semantic_idx", vector_id);
    defer alloc.free(mapping_key);
    const mapped_doc = try store.get(alloc, mapping_key);
    defer alloc.free(mapped_doc);
    try std.testing.expectEqualStrings("doc:00000000", mapped_doc);

    const mapped_vector_id = (try manager.lookupDenseVectorId(&store, "semantic_idx", "doc:00000000")) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(vector_id, mapped_vector_id);
}

test "external dense embedding writes use stable vector ids and ordinal member rows" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);

    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();

    var manager = try IndexManager.init(alloc, path);
    defer manager.deinit();
    manager.updateRange(.{ .start = "", .end = "" });

    try manager.addAllNoBackfill(&store, &.{
        .{
            .name = "semantic_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"cosine\",\"embedding_name\":\"semantic_idx\",\"external\":true}",
        },
    });

    const primary_key = try internal_keys.documentKeyAlloc(alloc, "doc:primary");
    defer alloc.free(primary_key);
    try store.put(primary_key, "{\"title\":\"dense\"}");
    const chunk_key = try internal_keys.documentKeyAlloc(alloc, "doc:chunked");
    defer alloc.free(chunk_key);
    try store.put(chunk_key, "{\"title\":\"chunked\"}");

    const doc_ids = [_][]const u8{ "doc:primary", "doc:chunked" };
    var identity_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer {
        for (identity_writes.items) |item| {
            alloc.free(@constCast(item.key));
            alloc.free(@constCast(item.value));
        }
        identity_writes.deinit(alloc);
    }
    try doc_identity.appendBatchIdentityMetadataAlloc(
        alloc,
        &store,
        0,
        0,
        1,
        &identity_writes,
        doc_ids[0..],
        &.{},
    );
    try store.putBatchWithReplay(null, identity_writes.items, &.{}, null);

    const writes = [_]mapper.DenseEmbeddingWrite{
        .{
            .index_name = try alloc.dupe(u8, "semantic_idx"),
            .doc_key = try alloc.dupe(u8, "doc:primary"),
            .vector = try alloc.dupe(f32, &[_]f32{ 1.0, 0.0, 0.0 }),
            .artifact_key = null,
        },
        .{
            .index_name = try alloc.dupe(u8, "semantic_idx"),
            .doc_key = try alloc.dupe(u8, "chunk:doc:chunked:0"),
            .parent_doc_key = "doc:chunked",
            .vector = try alloc.dupe(f32, &[_]f32{ 0.0, 1.0, 0.0 }),
            .artifact_key = null,
        },
    };
    defer {
        for (writes) |write| {
            alloc.free(write.index_name);
            alloc.free(write.doc_key);
            alloc.free(write.vector);
        }
    }

    try manager.applyDenseEmbeddingWritesByName(&store, "semantic_idx", &writes);

    const primary_vector_id = deterministicDenseVectorId("doc:primary");
    try std.testing.expectEqual(@as(?u64, primary_vector_id), try manager.lookupDenseVectorId(&store, "semantic_idx", "doc:primary"));
    const primary_doc = try manager.lookupDenseDocKey(&store, "semantic_idx", primary_vector_id);
    defer if (primary_doc) |doc_key| alloc.free(doc_key);
    try std.testing.expectEqualStrings("doc:primary", primary_doc.?);

    const chunk_vector_id = deterministicDenseVectorId("chunk:doc:chunked:0");
    try std.testing.expectEqual(@as(?u64, chunk_vector_id), try manager.lookupDenseVectorId(&store, "semantic_idx", "chunk:doc:chunked:0"));

    const primary_vectors = try manager.lookupDenseVectorIdsForOrdinalsAlloc(alloc, &store, "semantic_idx", &.{1});
    defer alloc.free(primary_vectors);
    try std.testing.expectEqual(@as(usize, 1), primary_vectors.len);
    try std.testing.expectEqual(primary_vector_id, primary_vectors[0]);

    const chunk_vectors = try manager.lookupDenseVectorIdsForOrdinalsAlloc(alloc, &store, "semantic_idx", &.{2});
    defer alloc.free(chunk_vectors);
    try std.testing.expectEqual(@as(usize, 1), chunk_vectors.len);
    try std.testing.expectEqual(chunk_vector_id, chunk_vectors[0]);
}

test "primary dense stable vector ids survive identity namespace reassignment" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);

    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();

    var manager = try IndexManager.init(alloc, path);
    defer manager.deinit();
    manager.updateRange(.{ .start = "", .end = "" });

    try manager.addAllNoBackfill(&store, &.{
        .{
            .name = "semantic_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"cosine\",\"embedding_name\":\"semantic_idx\",\"external\":true}",
        },
    });

    const doc_key = try internal_keys.documentKeyAlloc(alloc, "doc:primary");
    defer alloc.free(doc_key);
    try store.put(doc_key, "{\"title\":\"dense\"}");

    const old_namespace = doc_identity.Namespace{ .table_id = 9, .shard_id = 901, .range_id = 9001 };
    const new_namespace = doc_identity.Namespace{ .table_id = 9, .shard_id = 902, .range_id = 9002 };
    var identity_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer {
        for (identity_writes.items) |item| {
            alloc.free(@constCast(item.key));
            alloc.free(@constCast(item.value));
        }
        identity_writes.deinit(alloc);
    }
    try doc_identity.appendBatchIdentityMetadataForNamespaceAlloc(
        alloc,
        &store,
        old_namespace,
        1,
        &identity_writes,
        &.{"doc:primary"},
        &.{},
    );
    try store.putBatchWithReplay(null, identity_writes.items, &.{}, null);

    const writes = [_]mapper.DenseEmbeddingWrite{.{
        .index_name = try alloc.dupe(u8, "semantic_idx"),
        .doc_key = try alloc.dupe(u8, "doc:primary"),
        .vector = try alloc.dupe(f32, &[_]f32{ 1.0, 0.0, 0.0 }),
        .artifact_key = null,
    }};
    defer {
        alloc.free(writes[0].index_name);
        alloc.free(writes[0].doc_key);
        alloc.free(writes[0].vector);
    }

    try manager.applyDenseEmbeddingWritesByName(&store, "semantic_idx", &writes);

    {
        var txn = try store.beginProbeTxn();
        defer txn.abort();
        const ordinal = (try doc_identity.lookupOrdinalTxn(alloc, &txn, "doc:primary")).?;
        try std.testing.expectEqual(@as(doc_identity.DocOrdinal, 1), ordinal);
        const state = (try doc_identity.lookupStateTxn(&txn, ordinal)).?;
        try std.testing.expectEqual(doc_identity.canonicalDocIdForNamespace(old_namespace, "doc:primary"), state.canonical_doc_id);
    }
    const primary_vector_id = deterministicDenseVectorId("doc:primary");
    try std.testing.expectEqual(@as(?u64, primary_vector_id), try manager.lookupDenseVectorId(&store, "semantic_idx", "doc:primary"));

    try doc_identity.reassignNamespaceAlloc(alloc, &store, new_namespace);

    {
        var txn = try store.beginProbeTxn();
        defer txn.abort();
        const ordinal = (try doc_identity.lookupOrdinalTxn(alloc, &txn, "doc:primary")).?;
        try std.testing.expectEqual(@as(doc_identity.DocOrdinal, 1), ordinal);
        const state = (try doc_identity.lookupStateTxn(&txn, ordinal)).?;
        try std.testing.expectEqual(doc_identity.canonicalDocIdForNamespace(new_namespace, "doc:primary"), state.canonical_doc_id);
    }
    try std.testing.expectEqual(@as(?u64, primary_vector_id), try manager.lookupDenseVectorId(&store, "semantic_idx", "doc:primary"));

    const vectors = try manager.lookupDenseVectorIdsForOrdinalsAlloc(alloc, &store, "semantic_idx", &.{1});
    defer alloc.free(vectors);
    try std.testing.expectEqual(@as(usize, 1), vectors.len);
    try std.testing.expectEqual(primary_vector_id, vectors[0]);
}

test "external dense embedding writes keep search working after incremental replay-style applies" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);

    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();

    var manager = try IndexManager.init(alloc, path);
    defer manager.deinit();
    manager.updateRange(.{ .start = "", .end = "" });

    try manager.addAllNoBackfill(&store, &.{
        .{
            .name = "semantic_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"cosine\",\"embedding_name\":\"semantic_idx\",\"external\":true}",
        },
    });

    const docs = [_][]const u8{ "doc:a", "doc:b", "doc:c" };
    for (docs) |doc_key| {
        const stored_key = try internal_keys.documentKeyAlloc(alloc, doc_key);
        defer alloc.free(stored_key);
        try store.put(stored_key, "{\"title\":\"dense\"}");
    }

    var first_write = [_]mapper.DenseEmbeddingWrite{
        .{
            .index_name = try alloc.dupe(u8, "semantic_idx"),
            .doc_key = try alloc.dupe(u8, "doc:a"),
            .vector = try alloc.dupe(f32, &[_]f32{ 1.0, 0.0, 0.0 }),
            .artifact_key = null,
        },
    };
    defer {
        alloc.free(first_write[0].index_name);
        alloc.free(first_write[0].doc_key);
        alloc.free(first_write[0].vector);
    }
    try manager.applyDenseEmbeddingWritesByName(&store, "semantic_idx", &first_write);

    var second_writes = [_]mapper.DenseEmbeddingWrite{
        .{
            .index_name = try alloc.dupe(u8, "semantic_idx"),
            .doc_key = try alloc.dupe(u8, "doc:a"),
            .vector = try alloc.dupe(f32, &[_]f32{ 1.0, 0.0, 0.0 }),
            .artifact_key = null,
        },
        .{
            .index_name = try alloc.dupe(u8, "semantic_idx"),
            .doc_key = try alloc.dupe(u8, "doc:b"),
            .vector = try alloc.dupe(f32, &[_]f32{ 0.0, 1.0, 0.0 }),
            .artifact_key = null,
        },
        .{
            .index_name = try alloc.dupe(u8, "semantic_idx"),
            .doc_key = try alloc.dupe(u8, "doc:c"),
            .vector = try alloc.dupe(f32, &[_]f32{ 0.0, 0.0, 1.0 }),
            .artifact_key = null,
        },
    };
    defer {
        for (second_writes) |write| {
            alloc.free(write.index_name);
            alloc.free(write.doc_key);
            alloc.free(write.vector);
        }
    }
    try manager.applyDenseEmbeddingWritesByName(&store, "semantic_idx", &second_writes);

    const entry = manager.denseIndex("semantic_idx") orelse return error.IndexNotFound;
    try std.testing.expectEqual(@as(u64, 3), entry.index.stats().active_count);

    var results = try entry.index.searchWithRequest(.{
        .query = &[_]f32{ 1.0, 0.0, 0.0 },
        .k = 3,
    });
    defer results.deinit();

    const raw_hits = results.getHits();
    try std.testing.expect(raw_hits.len > 0);

    const mapped_doc = try manager.lookupDenseDocKey(&store, "semantic_idx", raw_hits[0].vector_id);
    defer if (mapped_doc) |doc_key| alloc.free(doc_key);
    try std.testing.expect(mapped_doc != null);
}

test "dense artifact preload batches sorted reads through getManySorted" {
    const alloc = std.testing.allocator;

    var manager = try IndexManager.init(alloc, ".");
    defer manager.deinit();

    const payload_a = try enrichment_artifact_codec.encodeDenseEmbeddingAlloc(alloc, null, &[_]f32{ 1, 2 });
    defer alloc.free(payload_a);
    const payload_b = try enrichment_artifact_codec.encodeDenseEmbeddingAlloc(alloc, null, &[_]f32{ 3, 4 });
    defer alloc.free(payload_b);

    const Shared = struct {
        get_calls: usize = 0,
        get_many_calls: usize = 0,
        saw_sorted_keys: bool = true,

        fn lookup(_: *@This(), key: []const u8, payload_a_inner: []const u8, payload_b_inner: []const u8) ?[]const u8 {
            if (std.mem.eql(u8, key, "artifact:a")) return payload_a_inner;
            if (std.mem.eql(u8, key, "artifact:b")) return payload_b_inner;
            return null;
        }
    };

    const MockTxn = struct {
        shared: *Shared,
        payload_a: []const u8,
        payload_b: []const u8,

        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            self.shared.get_calls += 1;
            return self.shared.lookup(key, self.payload_a, self.payload_b) orelse error.NotFound;
        }

        pub fn getManySorted(self: *@This(), keys: []const []const u8, values: []?[]const u8) !void {
            self.shared.get_many_calls += 1;
            for (keys, 0..) |key, i| {
                if (i > 0 and std.mem.order(u8, keys[i - 1], key) == .gt) self.shared.saw_sorted_keys = false;
                values[i] = self.shared.lookup(key, self.payload_a, self.payload_b);
            }
        }
    };

    var shared = Shared{};
    var txn = MockTxn{
        .shared = &shared,
        .payload_a = payload_a,
        .payload_b = payload_b,
    };

    const index_name_a = try alloc.dupe(u8, "dv_v1");
    defer alloc.free(index_name_a);
    const doc_key_b = try alloc.dupe(u8, "doc:b");
    defer alloc.free(doc_key_b);
    const artifact_key_b = try alloc.dupe(u8, "artifact:b");
    defer alloc.free(artifact_key_b);
    const empty_vector_b = try alloc.alloc(f32, 0);
    defer alloc.free(empty_vector_b);

    const index_name_b = try alloc.dupe(u8, "dv_v1");
    defer alloc.free(index_name_b);
    const doc_key_a = try alloc.dupe(u8, "doc:a");
    defer alloc.free(doc_key_a);
    const artifact_key_a = try alloc.dupe(u8, "artifact:a");
    defer alloc.free(artifact_key_a);
    const empty_vector_a = try alloc.alloc(f32, 0);
    defer alloc.free(empty_vector_a);

    const writes = [_]mapper.DenseEmbeddingWrite{
        .{
            .index_name = index_name_a,
            .doc_key = doc_key_b,
            .artifact_key = artifact_key_b,
            .vector = empty_vector_b,
        },
        .{
            .index_name = index_name_b,
            .doc_key = doc_key_a,
            .artifact_key = artifact_key_a,
            .vector = empty_vector_a,
        },
    };
    const out_vectors = try alloc.alloc(?[]const f32, writes.len);
    defer {
        alloc.free(out_vectors);
    }
    for (out_vectors) |*slot| slot.* = null;
    var fallback_scratch = std.ArrayListUnmanaged(f32).empty;
    defer fallback_scratch.deinit(alloc);
    const keep_write = try alloc.alloc(bool, writes.len);
    defer alloc.free(keep_write);
    @memset(keep_write, true);
    var corrupt_artifact_deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer corrupt_artifact_deletes.deinit(alloc);

    try std.testing.expect(try manager.preloadDenseEmbeddingArtifactVectorsFromTxn(
        "dv_v1",
        2,
        &writes,
        keep_write,
        &txn,
        out_vectors,
        &fallback_scratch,
        &corrupt_artifact_deletes,
    ));

    try std.testing.expectEqual(@as(usize, 0), shared.get_calls);
    try std.testing.expectEqual(@as(usize, 1), shared.get_many_calls);
    try std.testing.expect(shared.saw_sorted_keys);
    try std.testing.expectEqualSlices(f32, &[_]f32{ 3, 4 }, out_vectors[0].?);
    try std.testing.expectEqualSlices(f32, &[_]f32{ 1, 2 }, out_vectors[1].?);
}

fn stressEnvUsize(name: [*:0]const u8, default_value: usize) usize {
    const raw = getenv(name) orelse return default_value;
    return std.fmt.parseInt(usize, std.mem.span(raw), 10) catch default_value;
}

fn fillStressDenseVector(vector: []f32, doc_index: usize) void {
    for (vector, 0..) |*slot, dim_index| {
        const raw = (doc_index * 131 + dim_index * 17) % 2048;
        slot.* = @as(f32, @floatFromInt(raw)) / 1024.0 - 1.0;
    }
}

test "dense index manager stress applies explicit embedding writes on lsm backend" {
    if (getenv("ANTFLY_STRESS_DENSE_EMBED_REPRO") == null) return error.SkipZigTest;

    const alloc = std.testing.allocator;
    const dims = stressEnvUsize("ANTFLY_STRESS_DENSE_DIMS", 256);
    const total_docs = stressEnvUsize("ANTFLY_STRESS_DENSE_DOCS", 4096);
    const batch_size = @max(@as(usize, 1), stressEnvUsize("ANTFLY_STRESS_DENSE_BATCH", 256));
    const progress_interval = @max(batch_size, stressEnvUsize("ANTFLY_STRESS_DENSE_PROGRESS", batch_size * 8));

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);

    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();

    var manager = try IndexManager.initWithOptions(alloc, path, .{
        .dense_storage_backend = .lsm,
    });
    defer manager.deinit();
    manager.updateRange(.{ .start = "", .end = "" });

    const config_json = try std.fmt.allocPrint(alloc, "{{\"field\":\"embedding\",\"dims\":{d},\"metric\":\"l2_squared\"}}", .{dims});
    defer alloc.free(config_json);
    try manager.addAllNoBackfill(&store, &.{
        .{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = config_json,
        },
    });

    var start: usize = 0;
    while (start < total_docs) : (start += batch_size) {
        const end = @min(start + batch_size, total_docs);

        var store_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
        defer {
            for (store_writes.items) |write| {
                alloc.free(@constCast(write.key));
                alloc.free(@constCast(write.value));
            }
            store_writes.deinit(alloc);
        }
        var writes = std.ArrayListUnmanaged(mapper.DenseEmbeddingWrite).empty;
        defer {
            for (writes.items) |write| {
                alloc.free(write.index_name);
                alloc.free(write.doc_key);
                alloc.free(write.vector);
            }
            writes.deinit(alloc);
        }

        for (start..end) |doc_index| {
            const doc_key = try std.fmt.allocPrint(alloc, "doc:{d:0>8}", .{doc_index});
            const store_key = try internal_keys.documentKeyAlloc(alloc, doc_key);
            const store_value = try alloc.dupe(u8, "{\"title\":\"dense\"}");
            try store_writes.append(alloc, .{
                .key = store_key,
                .value = store_value,
            });

            const vector = try alloc.alloc(f32, dims);
            fillStressDenseVector(vector, doc_index);
            try writes.append(alloc, .{
                .index_name = try alloc.dupe(u8, "dv_v1"),
                .doc_key = doc_key,
                .artifact_key = null,
                .vector = vector,
            });
        }

        try store.putBatch(store_writes.items, &.{});
        try manager.applyDenseEmbeddingWritesByName(&store, "dv_v1", writes.items);

        const inserted = end;
        if (inserted % progress_interval == 0 or inserted == total_docs) {
            const entry = manager.denseIndex("dv_v1") orelse return error.IndexNotFound;
            try std.testing.expectEqual(@as(u64, @intCast(inserted)), entry.index.stats().active_count);
        }
    }

    const entry = manager.denseIndex("dv_v1") orelse return error.IndexNotFound;
    try std.testing.expectEqual(@as(u64, @intCast(total_docs)), entry.index.stats().active_count);

    const first_id = (try manager.lookupDenseVectorId(&store, "dv_v1", "doc:00000000")) orelse return error.TestUnexpectedResult;
    const last_doc_key = try std.fmt.allocPrint(alloc, "doc:{d:0>8}", .{total_docs - 1});
    defer alloc.free(last_doc_key);
    const last_id = (try manager.lookupDenseVectorId(&store, "dv_v1", last_doc_key)) orelse return error.TestUnexpectedResult;

    const first_metadata = (try entry.index.getMetadata(first_id)) orelse return error.TestUnexpectedResult;
    defer alloc.free(first_metadata);
    try std.testing.expectEqualStrings("doc:00000000", first_metadata);

    const last_metadata = (try entry.index.getMetadata(last_id)) orelse return error.TestUnexpectedResult;
    defer alloc.free(last_metadata);
    try std.testing.expectEqualStrings(last_doc_key, last_metadata);

    var read_txn = try entry.index.beginReadTxn();
    defer read_txn.abort();
    const last_vector = try entry.index.getVector(&read_txn, last_id);
    defer alloc.free(last_vector);
    const expected_last_vector = try alloc.alloc(f32, dims);
    defer alloc.free(expected_last_vector);
    fillStressDenseVector(expected_last_vector, total_docs - 1);
    try std.testing.expectEqualSlices(f32, expected_last_vector, last_vector);
}

test "dense HBC batchInsertWithMetadata works after addAllNoBackfill" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);

    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();

    var manager = try IndexManager.init(alloc, path);
    defer manager.deinit();
    manager.updateRange(.{ .start = "", .end = "" });

    try manager.addAllNoBackfill(&store, &.{
        .{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\"}",
        },
    });

    const entry = manager.denseIndex("dv_v1") orelse return error.IndexNotFound;
    const items = [_]hbc_mod.BatchInsertItem{
        .{
            .vector_id = 1,
            .vector = &[_]f32{ 1.0, 2.0, 3.0 },
            .metadata = "doc:00000000",
        },
    };
    try entry.index.batchInsertWithMetadata(&items);

    try std.testing.expectEqual(@as(u64, 1), entry.index.stats().active_count);
}

test "dense HBC batchInsertWithMetadata works after text batch setup" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);

    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();

    var manager = try IndexManager.init(alloc, path);
    defer manager.deinit();
    manager.updateRange(.{ .start = "", .end = "" });

    try manager.addAllNoBackfill(&store, &.{
        .{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{\"field\":\"title\"}",
        },
        .{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\"}",
        },
    });

    var store_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer {
        for (store_writes.items) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        store_writes.deinit(alloc);
    }
    var text_writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
    defer {
        for (text_writes.items) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        text_writes.deinit(alloc);
    }
    var dense_vecs = std.ArrayListUnmanaged([]f32).empty;
    defer {
        for (dense_vecs.items) |vec| alloc.free(vec);
        dense_vecs.deinit(alloc);
    }
    var dense_items = std.ArrayListUnmanaged(hbc_mod.BatchInsertItem).empty;
    defer dense_items.deinit(alloc);

    var key_buf: [64]u8 = undefined;
    for (0..16) |i| {
        const key = try alloc.dupe(u8, std.fmt.bufPrint(&key_buf, "doc:{d:0>8}", .{i}) catch unreachable);
        const value = try std.fmt.allocPrint(alloc, "{{\"title\":\"title {d:0>8}\"}}", .{i});
        try store_writes.append(alloc, .{
            .key = try alloc.dupe(u8, key),
            .value = try alloc.dupe(u8, value),
        });
        try text_writes.append(alloc, .{
            .key = key,
            .value = value,
        });

        const vec = try alloc.alloc(f32, 3);
        vec[0] = @floatFromInt((i % 7) + 1);
        vec[1] = @floatFromInt((i % 5) + 2);
        vec[2] = @floatFromInt((i % 3) + 3);
        try dense_vecs.append(alloc, vec);
        try dense_items.append(alloc, .{
            .vector_id = i + 1,
            .vector = vec,
            .metadata = key,
        });
    }

    try store.putBatch(store_writes.items, &.{});
    try manager.indexTextBatchByName(&store, "ft_v1", text_writes.items);

    const entry = manager.denseIndex("dv_v1") orelse return error.IndexNotFound;
    try entry.index.batchInsertWithMetadata(dense_items.items);
    try std.testing.expectEqual(@as(u64, 16), entry.index.stats().active_count);
}

test "text merge task skips stale source after concurrent delete" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);

    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();

    var manager = try IndexManager.init(alloc, path);
    defer manager.deinit();
    manager.updateRange(.{ .start = "", .end = "" });

    try manager.addAllNoBackfill(&store, &.{
        .{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{\"field\":\"title\"}",
        },
    });

    var key_buf: [64]u8 = undefined;
    const opts: IndexBatchOptions = .{
        .compact_text = false,
        .compact_text_segment_threshold = 2,
        .defer_text_compaction = true,
    };
    for (0..12) |i| {
        const key = try alloc.dupe(u8, std.fmt.bufPrint(&key_buf, "doc:{d:0>8}", .{i}) catch unreachable);
        defer alloc.free(key);
        const value = try std.fmt.allocPrint(alloc, "{{\"title\":\"merge stale {d}\"}}", .{i});
        defer alloc.free(value);

        try store.putBatch(&.{.{ .key = key, .value = value }}, &.{});
        try manager.indexTextBatchByNameWithOptions(&store, "ft_v1", &.{.{ .key = key, .value = value }}, opts);
    }

    const pending_stats = manager.textMergeStats();
    try std.testing.expectEqual(@as(u64, 1), pending_stats.pending_indexes);
    try std.testing.expect(pending_stats.pending_segments >= 12);
    try std.testing.expect(pending_stats.pending_bytes > 0);

    var task = (try manager.beginTextMergeTask()) orelse return error.TestUnexpectedResult;
    defer task.deinit(alloc);
    const in_flight_stats = manager.textMergeStats();
    try std.testing.expectEqual(@as(u64, 1), in_flight_stats.in_flight_merges);
    try std.testing.expect(in_flight_stats.in_flight_segments >= 2);

    var second_task = (try manager.beginTextMergeTask()) orelse return error.TestUnexpectedResult;
    defer second_task.deinit(alloc);
    const parallel_stats = manager.textMergeStats();
    try std.testing.expectEqual(@as(u64, 2), parallel_stats.in_flight_merges);
    try std.testing.expect(parallel_stats.in_flight_segments > in_flight_stats.in_flight_segments);
    manager.cancelTextMergeTask(&second_task);

    const stale_doc = task.segments[0].reader.storedDoc(0) orelse return error.TestUnexpectedResult;
    try manager.deleteTextBatchByNameWithOptions("ft_v1", &.{stale_doc.id}, opts);

    var result = try IndexManager.executeTextMergeTask(alloc, &task);
    defer result.deinit(alloc);
    const applied = try manager.finishTextMergeTask(&task, &result);
    try std.testing.expect(!applied);
    const stale_stats = manager.textMergeStats();
    try std.testing.expectEqual(@as(u64, 0), stale_stats.in_flight_merges);
    try std.testing.expectEqual(@as(u64, 0), stale_stats.in_flight_segments);
    try std.testing.expectEqual(@as(u64, 1), stale_stats.skipped_stale_merges);

    const entry = manager.textIndexEntry("ft_v1") orelse return error.IndexNotFound;
    try std.testing.expect(entry.compaction_pending);
    try std.testing.expect(entry.persistent.snapshot().segments.len >= 12);
}

test "text delete clears handed-off stale docs outside current range" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);

    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();

    var manager = try IndexManager.init(alloc, path);
    defer manager.deinit();
    manager.updateRange(.{ .start = "", .end = "" });

    try manager.addAllNoBackfill(&store, &.{
        .{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{\"field\":\"title\"}",
        },
    });

    const writes = [_]types.BatchWrite{
        .{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" },
        .{ .key = "doc:m", .value = "{\"title\":\"middle\"}" },
        .{ .key = "doc:z", .value = "{\"title\":\"zeta\"}" },
    };
    try manager.indexTextBatchByName(&store, "ft_v1", &writes);

    manager.updateRange(.{ .start = "", .end = "doc:m" });
    try manager.deleteTextBatchByName("ft_v1", &.{ "doc:m", "doc:z" });

    const snapshot = manager.textIndex("ft_v1").?.snapshot();
    try std.testing.expectEqual(@as(u64, 1), snapshot.global_doc_count);
    try std.testing.expectEqual(@as(u32, 1), try snapshot.termDocFreq(alloc, "title", "alpha"));
    try std.testing.expectEqual(@as(u32, 0), try snapshot.termDocFreq(alloc, "title", "middle"));
    try std.testing.expectEqual(@as(u32, 0), try snapshot.termDocFreq(alloc, "title", "zeta"));
}

test "text merge failure quarantines source segments" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);

    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();

    var manager = try IndexManager.init(alloc, path);
    defer manager.deinit();
    manager.updateRange(.{ .start = "", .end = "" });

    try manager.addAllNoBackfill(&store, &.{
        .{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{\"field\":\"title\"}",
        },
    });

    const opts: IndexBatchOptions = .{
        .compact_text = false,
        .compact_text_segment_threshold = 2,
        .defer_text_compaction = true,
    };
    for (0..2) |i| {
        var key_buf: [64]u8 = undefined;
        const key = try alloc.dupe(u8, std.fmt.bufPrint(&key_buf, "doc:{d:0>8}", .{i}) catch unreachable);
        defer alloc.free(key);
        const value = try std.fmt.allocPrint(alloc, "{{\"title\":\"quarantine {d}\"}}", .{i});
        defer alloc.free(value);

        try store.putBatch(&.{.{ .key = key, .value = value }}, &.{});
        try manager.indexTextBatchByNameWithOptions(&store, "ft_v1", &.{.{ .key = key, .value = value }}, opts);
    }

    var task = (try manager.beginTextMergeTask()) orelse return error.TestUnexpectedResult;
    defer task.deinit(alloc);
    manager.noteTextMergeFailure(&task, error.InvalidChunk);

    const stats = manager.textMergeStats();
    try std.testing.expectEqual(@as(u64, 1), stats.failed_merges);
    try std.testing.expectEqual(@as(u64, 1), stats.quarantined_merges);
    try std.testing.expectEqual(@as(u64, @intCast(task.source.len)), stats.quarantined_segments);
    try std.testing.expectEqualStrings("InvalidChunk", stats.last_merge_error);
    var blocked_task = try manager.beginTextMergeTask();
    if (blocked_task) |*unexpected| {
        unexpected.deinit(alloc);
        return error.TestUnexpectedResult;
    }

    const entry = manager.textIndexEntry("ft_v1") orelse return error.IndexNotFound;
    try std.testing.expect(entry.compaction_pending);
    try std.testing.expect(entry.persistent.snapshot().segments.len >= 2);
}

test "text merge resource manager accounts pending bytes and active buffers" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@intFromEnum(resource_manager_mod.Slice.full_text_pending_segments)] = .{
        .soft_limit_bytes = 1,
        .hard_limit_bytes = 1024 * 1024,
    };
    budgets[@intFromEnum(resource_manager_mod.Slice.text_merge_buffers)] = .{
        .soft_limit_bytes = 1,
        .hard_limit_bytes = 1024 * 1024,
    };
    var policies = resource_manager_mod.Options.defaultPolicies();
    policies[@intFromEnum(resource_manager_mod.Slice.full_text_pending_segments)] = .{
        .soft_action = .report,
        .hard_action = .report,
    };
    var resource_manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets, .policies = policies });

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);

    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();

    var manager = try IndexManager.initWithOptions(alloc, path, .{
        .resource_manager = &resource_manager,
    });
    defer manager.deinit();
    manager.updateRange(.{ .start = "", .end = "" });

    try manager.addAllNoBackfill(&store, &.{
        .{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{\"field\":\"title\"}",
        },
    });

    const opts: IndexBatchOptions = .{
        .compact_text = false,
        .compact_text_segment_threshold = 2,
        .defer_text_compaction = true,
    };
    for (0..3) |i| {
        var key_buf: [64]u8 = undefined;
        const key = try alloc.dupe(u8, std.fmt.bufPrint(&key_buf, "doc:{d:0>8}", .{i}) catch unreachable);
        defer alloc.free(key);
        const value = try std.fmt.allocPrint(alloc, "{{\"title\":\"resource accounting {d}\"}}", .{i});
        defer alloc.free(value);

        try store.putBatch(&.{.{ .key = key, .value = value }}, &.{});
        try manager.indexTextBatchByNameWithOptions(&store, "ft_v1", &.{.{ .key = key, .value = value }}, opts);
    }

    const merge_stats = manager.textMergeStats();
    try std.testing.expect(merge_stats.pending_bytes > 0);
    var resource_stats = resource_manager.snapshot();
    try std.testing.expectEqual(merge_stats.pending_bytes, resource_stats.slices[@intFromEnum(resource_manager_mod.Slice.full_text_pending_segments)].used_bytes);
    try std.testing.expect(resource_stats.slices[@intFromEnum(resource_manager_mod.Slice.full_text_pending_segments)].soft_limit_events > 0);

    {
        var task = (try manager.beginTextMergeTask()) orelse return error.TestUnexpectedResult;
        defer task.deinit(alloc);

        resource_stats = resource_manager.snapshot();
        try std.testing.expect(resource_stats.slices[@intFromEnum(resource_manager_mod.Slice.text_merge_buffers)].used_bytes > 0);
        try std.testing.expect(resource_stats.slices[@intFromEnum(resource_manager_mod.Slice.text_merge_buffers)].soft_limit_events > 0);
    }

    resource_stats = resource_manager.snapshot();
    try std.testing.expectEqual(@as(u64, 0), resource_stats.slices[@intFromEnum(resource_manager_mod.Slice.text_merge_buffers)].used_bytes);
}

test "text merge resource pressure defers background merges" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@intFromEnum(resource_manager_mod.Slice.full_text_pending_segments)] = .{
        .soft_limit_bytes = 1,
        .hard_limit_bytes = 1024 * 1024,
    };
    var resource_manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);

    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();

    var manager = try IndexManager.initWithOptions(alloc, path, .{
        .resource_manager = &resource_manager,
    });
    defer manager.deinit();
    manager.updateRange(.{ .start = "", .end = "" });

    try manager.addAllNoBackfill(&store, &.{
        .{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{\"field\":\"title\"}",
        },
    });

    const opts: IndexBatchOptions = .{
        .compact_text = false,
        .compact_text_segment_threshold = 2,
        .defer_text_compaction = true,
    };
    for (0..3) |i| {
        var key_buf: [64]u8 = undefined;
        const key = try alloc.dupe(u8, std.fmt.bufPrint(&key_buf, "doc:{d:0>8}", .{i}) catch unreachable);
        defer alloc.free(key);
        const value = try std.fmt.allocPrint(alloc, "{{\"title\":\"pressure defer {d}\"}}", .{i});
        defer alloc.free(value);

        try store.putBatch(&.{.{ .key = key, .value = value }}, &.{});
        try manager.indexTextBatchByNameWithOptions(&store, "ft_v1", &.{.{ .key = key, .value = value }}, opts);
    }

    const pending_stats = manager.textMergeStats();
    try std.testing.expect(pending_stats.pending_bytes > 1);

    var maybe_task = try manager.beginTextMergeTask();
    if (maybe_task) |*task| {
        task.deinit(alloc);
        return error.TestUnexpectedResult;
    }

    const deferred_stats = manager.textMergeStats();
    try std.testing.expectEqual(@as(u64, 1), deferred_stats.deferred_for_pressure);
    const per_index_stats = manager.textMergeStatsForIndex("ft_v1");
    try std.testing.expectEqual(@as(u64, 1), per_index_stats.deferred_for_pressure);
}

test "force compact skips clean text indexes" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);

    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();

    var manager = try IndexManager.init(alloc, path);
    defer manager.deinit();
    manager.updateRange(.{ .start = "", .end = "" });

    try manager.addAllNoBackfill(&store, &.{
        .{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{\"field\":\"title\"}",
        },
    });

    const key = try alloc.dupe(u8, "doc:00000000");
    defer alloc.free(key);
    const value = try alloc.dupe(u8, "{\"title\":\"clean index\"}");
    defer alloc.free(value);

    try store.putBatch(&.{.{ .key = key, .value = value }}, &.{});
    try manager.indexTextBatchByNameWithOptions(&store, "ft_v1", &.{.{ .key = key, .value = value }}, .{
        .compact_text = false,
        .defer_text_compaction = false,
    });

    const entry_before = manager.textIndexEntry("ft_v1") orelse return error.IndexNotFound;
    try std.testing.expectEqual(@as(usize, 1), entry_before.persistent.snapshot().segments.len);
    try std.testing.expect(!entry_before.compaction_pending);

    try manager.compactAllTextIndexes();

    const entry_after_compact = manager.textIndexEntry("ft_v1") orelse return error.IndexNotFound;
    try std.testing.expectEqual(@as(usize, 1), entry_after_compact.persistent.snapshot().segments.len);
    try std.testing.expect(!entry_after_compact.compaction_pending);

    try manager.forceCompactAllTextIndexes();

    const entry_after = manager.textIndexEntry("ft_v1") orelse return error.IndexNotFound;
    try std.testing.expectEqual(@as(usize, 1), entry_after.persistent.snapshot().segments.len);
    try std.testing.expect(!entry_after.compaction_pending);
}

test "force compact accounts text merge buffers via resource manager" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@intFromEnum(resource_manager_mod.Slice.text_merge_buffers)] = .{
        .soft_limit_bytes = 1,
        .hard_limit_bytes = 1024 * 1024,
    };
    var policies = resource_manager_mod.Options.defaultPolicies();
    policies[@intFromEnum(resource_manager_mod.Slice.text_merge_buffers)] = .{
        .soft_action = .report,
        .hard_action = .report,
    };
    var resource_manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets, .policies = policies });

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);

    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();

    var manager = try IndexManager.initWithOptions(alloc, path, .{
        .resource_manager = &resource_manager,
    });
    defer manager.deinit();
    manager.updateRange(.{ .start = "", .end = "" });

    try manager.addAllNoBackfill(&store, &.{
        .{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{\"field\":\"title\"}",
        },
    });

    const opts: IndexBatchOptions = .{
        .compact_text = false,
        .compact_text_segment_threshold = 2,
        .defer_text_compaction = true,
    };
    for (0..3) |i| {
        var key_buf: [64]u8 = undefined;
        const key = try alloc.dupe(u8, std.fmt.bufPrint(&key_buf, "doc:{d:0>8}", .{i}) catch unreachable);
        defer alloc.free(key);
        const value = try std.fmt.allocPrint(alloc, "{{\"title\":\"force compact resource {d}\"}}", .{i});
        defer alloc.free(value);

        try store.putBatch(&.{.{ .key = key, .value = value }}, &.{});
        try manager.indexTextBatchByNameWithOptions(&store, "ft_v1", &.{.{ .key = key, .value = value }}, opts);
    }

    try manager.forceCompactAllTextIndexes();

    const resource_stats = resource_manager.snapshot();
    try std.testing.expectEqual(
        @as(u64, 0),
        resource_stats.slices[@intFromEnum(resource_manager_mod.Slice.text_merge_buffers)].used_bytes,
    );
    try std.testing.expect(
        resource_stats.slices[@intFromEnum(resource_manager_mod.Slice.text_merge_buffers)].soft_limit_events > 0,
    );
}

test "best effort force compact defers under text merge pressure" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@intFromEnum(resource_manager_mod.Slice.text_merge_buffers)] = .{
        .soft_limit_bytes = 1,
        .hard_limit_bytes = 1024 * 1024,
    };
    var policies = resource_manager_mod.Options.defaultPolicies();
    policies[@intFromEnum(resource_manager_mod.Slice.text_merge_buffers)] = .{
        .soft_action = .defer_background_work,
        .hard_action = .defer_background_work,
    };
    var resource_manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets, .policies = policies });
    var tracked_usage: u64 = 0;
    resource_manager.observeUsage(.text_merge_buffers, &tracked_usage, 2);
    defer resource_manager.observeUsage(.text_merge_buffers, &tracked_usage, 0);

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);

    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();

    var manager = try IndexManager.initWithOptions(alloc, path, .{
        .resource_manager = &resource_manager,
    });
    defer manager.deinit();
    manager.updateRange(.{ .start = "", .end = "" });

    try manager.addAllNoBackfill(&store, &.{
        .{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{\"field\":\"title\"}",
        },
    });

    const opts: IndexBatchOptions = .{
        .compact_text = false,
        .compact_text_segment_threshold = 2,
        .defer_text_compaction = true,
    };
    for (0..3) |i| {
        var key_buf: [64]u8 = undefined;
        const key = try alloc.dupe(u8, std.fmt.bufPrint(&key_buf, "doc:{d:0>8}", .{i}) catch unreachable);
        defer alloc.free(key);
        const value = try std.fmt.allocPrint(alloc, "{{\"title\":\"best effort defer {d}\"}}", .{i});
        defer alloc.free(value);

        try store.putBatch(&.{.{ .key = key, .value = value }}, &.{});
        try manager.indexTextBatchByNameWithOptions(&store, "ft_v1", &.{.{ .key = key, .value = value }}, opts);
    }

    const entry_before = manager.textIndexEntry("ft_v1") orelse return error.IndexNotFound;
    try std.testing.expect(entry_before.persistent.snapshot().segments.len >= 2);

    try manager.bestEffortForceCompactAllTextIndexes();

    const entry_after = manager.textIndexEntry("ft_v1") orelse return error.IndexNotFound;
    try std.testing.expect(entry_after.persistent.snapshot().segments.len >= 2);
    try std.testing.expect(entry_after.compaction_pending);
}

test "best effort force compact stops on resource budget rejection" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@intFromEnum(resource_manager_mod.Slice.text_merge_buffers)] = .{
        .soft_limit_bytes = 1,
        .hard_limit_bytes = 1,
    };
    var policies = resource_manager_mod.Options.defaultPolicies();
    policies[@intFromEnum(resource_manager_mod.Slice.text_merge_buffers)] = .{
        .soft_action = .defer_background_work,
        .hard_action = .reject_work,
    };
    var resource_manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets, .policies = policies });

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);

    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();

    var manager = try IndexManager.initWithOptions(alloc, path, .{
        .resource_manager = &resource_manager,
    });
    defer manager.deinit();
    manager.updateRange(.{ .start = "", .end = "" });

    try manager.addAllNoBackfill(&store, &.{
        .{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{\"field\":\"title\"}",
        },
    });

    const opts: IndexBatchOptions = .{
        .compact_text = false,
        .compact_text_segment_threshold = 2,
        .defer_text_compaction = true,
    };
    for (0..3) |i| {
        var key_buf: [64]u8 = undefined;
        const key = try alloc.dupe(u8, std.fmt.bufPrint(&key_buf, "doc:{d:0>8}", .{i}) catch unreachable);
        defer alloc.free(key);
        const value = try std.fmt.allocPrint(alloc, "{{\"title\":\"best effort reject {d}\"}}", .{i});
        defer alloc.free(value);

        try store.putBatch(&.{.{ .key = key, .value = value }}, &.{});
        try manager.indexTextBatchByNameWithOptions(&store, "ft_v1", &.{.{ .key = key, .value = value }}, opts);
    }

    try manager.bestEffortForceCompactAllTextIndexes();

    const entry_after = manager.textIndexEntry("ft_v1") orelse return error.IndexNotFound;
    try std.testing.expect(entry_after.persistent.snapshot().segments.len >= 2);
    try std.testing.expect(entry_after.compaction_pending);

    const resource_stats = resource_manager.snapshot();
    try std.testing.expect(
        resource_stats.slices[@intFromEnum(resource_manager_mod.Slice.text_merge_buffers)].hard_limit_rejections > 0,
    );
}

test "best effort force compact resumes after modeled reopen under relaxed pressure" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var modeled_device = storage_sim.ModeledDevice.init(alloc);
    defer modeled_device.deinit();

    var pressured_budgets = resource_manager_mod.Options.defaultBudgets();
    pressured_budgets[@intFromEnum(resource_manager_mod.Slice.text_merge_buffers)] = .{
        .soft_limit_bytes = 1,
        .hard_limit_bytes = 1024 * 1024,
    };
    var pressured_policies = resource_manager_mod.Options.defaultPolicies();
    pressured_policies[@intFromEnum(resource_manager_mod.Slice.text_merge_buffers)] = .{
        .soft_action = .defer_background_work,
        .hard_action = .defer_background_work,
    };
    var pressured_manager = resource_manager_mod.ResourceManager.init(.{
        .budgets = pressured_budgets,
        .policies = pressured_policies,
    });
    var tracked_usage: u64 = 0;
    pressured_manager.observeUsage(.text_merge_buffers, &tracked_usage, 2);
    defer pressured_manager.observeUsage(.text_merge_buffers, &tracked_usage, 0);

    const backend_options = db_config.IndexBackendOptions{
        .text_main_backend = .lsm,
        .text_lsm_storage = modeled_device.storage(),
        .resource_manager = &pressured_manager,
    };

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);

    {
        var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
        defer store.close();

        var manager = try IndexManager.initWithOptions(alloc, path, backend_options);
        defer manager.deinit();
        manager.updateRange(.{ .start = "", .end = "" });

        try manager.addAllNoBackfill(&store, &.{
            .{
                .name = "ft_v1",
                .kind = .full_text,
                .config_json = "{\"field\":\"title\"}",
            },
        });

        const opts: IndexBatchOptions = .{
            .compact_text = false,
            .compact_text_segment_threshold = 2,
            .defer_text_compaction = true,
        };
        for (0..3) |i| {
            var key_buf: [64]u8 = undefined;
            const key = try alloc.dupe(u8, std.fmt.bufPrint(&key_buf, "doc:{d:0>8}", .{i}) catch unreachable);
            defer alloc.free(key);
            const value = try std.fmt.allocPrint(alloc, "{{\"title\":\"modeled reopen {d}\"}}", .{i});
            defer alloc.free(value);

            try store.putBatch(&.{.{ .key = key, .value = value }}, &.{});
            try manager.indexTextBatchByNameWithOptions(&store, "ft_v1", &.{.{ .key = key, .value = value }}, opts);
        }

        const entry_before = manager.textIndexEntry("ft_v1") orelse return error.IndexNotFound;
        try std.testing.expect(entry_before.compaction_pending);
        try std.testing.expect(entry_before.persistent.snapshot().segments.len >= 2);

        try manager.bestEffortForceCompactAllTextIndexes();

        const entry_after = manager.textIndexEntry("ft_v1") orelse return error.IndexNotFound;
        try std.testing.expect(entry_after.compaction_pending);
        try std.testing.expect(entry_after.persistent.snapshot().segments.len >= 2);
    }

    try modeled_device.device().crash();

    var relaxed_manager = resource_manager_mod.ResourceManager.init(.{});
    const reopen_backend_options = db_config.IndexBackendOptions{
        .text_main_backend = .lsm,
        .text_lsm_storage = modeled_device.storage(),
        .resource_manager = &relaxed_manager,
    };

    var store_reopened = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store_reopened.close();

    var manager_reopened = try IndexManager.initWithOptions(alloc, path, reopen_backend_options);
    defer manager_reopened.deinit();
    manager_reopened.updateRange(.{ .start = "", .end = "" });
    try manager_reopened.load(&store_reopened);

    const reopened_entry = manager_reopened.textIndexEntry("ft_v1") orelse return error.IndexNotFound;
    try std.testing.expect(reopened_entry.compaction_pending);
    try std.testing.expect(reopened_entry.persistent.snapshot().segments.len >= 2);

    try manager_reopened.drainScheduledTextMerges();

    const drained_entry = manager_reopened.textIndexEntry("ft_v1") orelse return error.IndexNotFound;
    try std.testing.expect(!drained_entry.compaction_pending);
    try std.testing.expectEqual(@as(usize, 1), drained_entry.persistent.snapshot().segments.len);
}

test "dense hbc batch options keep startup replay in bulk-ingest mode without assuming absent ids" {
    const opts = IndexManager.denseHbcBatchOptions(.{ .mode = .bulk_ingest }, false, true);
    try std.testing.expect(!opts.assume_absent_ids);
    try std.testing.expect(opts.centroid_only_routing);
    try std.testing.expect(opts.allow_quantized_routing);
    try std.testing.expect(opts.bulk_ingest);
    try std.testing.expect(opts.defer_quantized_rebuild);
    try std.testing.expect(!opts.defer_quantized_rebuild_to_bulk_finish);
    try std.testing.expect(opts.coalesce_leaf_writes);
    try std.testing.expect(opts.defer_leaf_splits_to_batch_finish);
    try std.testing.expect(!opts.defer_leaf_splits_to_bulk_finish);
    try std.testing.expectEqual(@as(usize, 0), opts.bulk_rebuild_leaf_min_members);
    try std.testing.expect(opts.skip_vector_store);
}

test "dense hbc batch options store vectors when no external loader can serve skipped vectors" {
    const opts = IndexManager.denseHbcBatchOptions(.{ .mode = .bulk_ingest }, true, false);
    try std.testing.expect(opts.assume_absent_ids);
    try std.testing.expect(opts.bulk_ingest);
    try std.testing.expect(opts.defer_quantized_rebuild);
    try std.testing.expect(!opts.defer_quantized_rebuild_to_bulk_finish);
    try std.testing.expect(opts.defer_leaf_splits_to_batch_finish);
    try std.testing.expect(!opts.defer_leaf_splits_to_bulk_finish);
    try std.testing.expectEqual(@as(usize, 0), opts.bulk_rebuild_leaf_min_members);
    try std.testing.expect(!opts.skip_vector_store);
}

test "dense replay keep mask keeps only the last write per doc and index" {
    var v0 = [_]f32{1.0};
    var v1 = [_]f32{2.0};
    var v2 = [_]f32{3.0};
    var v3 = [_]f32{4.0};
    const writes = [_]mapper.DenseEmbeddingWrite{
        .{
            .index_name = @constCast("dv_v1"),
            .doc_key = @constCast("doc:a"),
            .vector = v0[0..],
        },
        .{
            .index_name = @constCast("dv_v1"),
            .doc_key = @constCast("doc:b"),
            .vector = v1[0..],
        },
        .{
            .index_name = @constCast("dv_v1"),
            .doc_key = @constCast("doc:a"),
            .vector = v2[0..],
        },
        .{
            .index_name = @constCast("other"),
            .doc_key = @constCast("doc:a"),
            .vector = v3[0..],
        },
    };
    var keep_write: [writes.len]bool = undefined;
    try IndexManager.computeDenseReplayKeepMask(std.testing.allocator, &writes, &keep_write);
    try std.testing.expectEqualSlices(bool, &.{ false, true, true, true }, &keep_write);
}

test "dense replay keep mask does not collide on embedded nul bytes" {
    var v0 = [_]f32{1.0};
    var v1 = [_]f32{2.0};
    const writes = [_]mapper.DenseEmbeddingWrite{
        .{
            .index_name = @constCast("a\x00b"),
            .doc_key = @constCast("c"),
            .vector = v0[0..],
        },
        .{
            .index_name = @constCast("a"),
            .doc_key = @constCast("b\x00c"),
            .vector = v1[0..],
        },
    };
    var keep_write: [writes.len]bool = undefined;
    try IndexManager.computeDenseReplayKeepMask(std.testing.allocator, &writes, &keep_write);
    try std.testing.expectEqualSlices(bool, &.{ true, true }, &keep_write);
}
const StoreBatchOptions = backend_types.BatchOptions;
