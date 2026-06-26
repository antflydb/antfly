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
const platform_sync = @import("antfly_platform").sync;
const scraping = @import("antfly_scraping");

const common_secrets = @import("../../common/secrets.zig");
const metadata_api = @import("../../metadata/api.zig");
const metadata_mod = @import("../../metadata/mod.zig");
const metadata_table_manager = @import("../../metadata/table_manager.zig");
const metadata_transition_state = @import("../../metadata/transition_state.zig");
const raft_reconciler = @import("../../raft/reconciler.zig");
const backend_types = @import("../../storage/backend_types.zig");
const db_mod = @import("../../storage/db/mod.zig");
const doc_identity = @import("../../storage/db/doc_identity.zig");
const ha_primary_mod = @import("../../storage/ha/primary.zig");
const hbc_mod = @import("../../storage/hbc_adapter.zig");
const lsm_backend = @import("../../storage/lsm_backend/mod.zig");
const managed_embedder = @import("../../inference/managed_embedder.zig");
const platform_time = @import("../../platform/time.zig");
const resource_manager_mod = @import("../../storage/resource_manager.zig");
const runtime_status = @import("../runtime_status.zig");
const table_catalog = @import("../table_catalog.zig");
const tables_api = @import("../tables.zig");
const table_write_bulk_ingest = @import("bulk_ingest.zig");
const table_write_core = @import("core.zig");
const table_write_index_config = @import("index_config.zig");
const table_write_managed_db = @import("managed_db.zig");

const max_cached_write_tables = 64;
const backend_current_root_generation = table_write_core.backend_current_root_generation;
const auto_bulk_ingest_max_window_ops = table_write_bulk_ingest.max_window_ops;
const auto_bulk_ingest_max_idle_ns = table_write_bulk_ingest.max_idle_ns;
const auto_bulk_ingest_finish_options = table_write_bulk_ingest.finish_options;
const ManagedDbOpenMode = table_write_managed_db.ManagedDbOpenMode;
const StartupConfiguredIndexes = table_write_index_config.StartupConfiguredIndexes;
const haMirrorForManagedDbOpenMode = table_write_managed_db.haMirrorForManagedDbOpenMode;
const loadTableIdentityNamespaceForGroup = table_write_managed_db.loadTableIdentityNamespaceForGroup;
const openManagedDbForStatusWithCache = table_write_managed_db.openManagedDbForStatusWithCache;
const openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntflyAndIdentityWithOptions = table_write_managed_db.openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntflyAndIdentityWithOptions;
const validateProvisionedDbIdentityNamespaceExpected = table_write_managed_db.validateProvisionedDbIdentityNamespaceExpected;

pub const VisibleRootGenerationSource = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        visible_root_generation_for_group: *const fn (ptr: *anyopaque, group_id: u64) u64,
    };

    pub fn visibleRootGenerationForGroup(self: VisibleRootGenerationSource, group_id: u64) u64 {
        return self.vtable.visible_root_generation_for_group(self.ptr, group_id);
    }
};

pub const HostedManagedDbCacheDiagnostics = struct {
    present: bool = false,
    cached_roots: u64 = 0,
    cached_entries: u64 = 0,
    retired_entries: u64 = 0,
    table_metadata_entries: u64 = 0,
    active_bulk_sessions: u64 = 0,
    active_leases: u64 = 0,
    retired_active_leases: u64 = 0,
    bulk_ingest_open_entries: u64 = 0,
    auto_bulk_ingest_open_entries: u64 = 0,
    auto_bulk_ingest_finish_requested_entries: u64 = 0,
    lsm_mutable_bytes: u64 = 0,
    lsm_immutable_bytes: u64 = 0,
    lsm_total_run_bytes: u64 = 0,
    lsm_wal_retained_bytes: u64 = 0,
    lsm_wal_retained_segments: u64 = 0,
    lsm_active_readers: u64 = 0,
    lsm_obsolete_paths: u64 = 0,
    lsm_bulk_ingest_current_scan_clone_active_bytes: u64 = 0,
};

pub const HostedManagedDbCache = struct {
    replica_root_dir: []u8,
    mutex: std.atomic.Mutex = .unlocked,
    write_cache: ProvisionedTableWriteCache,

    fn init(alloc: std.mem.Allocator, replica_root_dir: []const u8) !*HostedManagedDbCache {
        const cache = try alloc.create(HostedManagedDbCache);
        errdefer alloc.destroy(cache);
        cache.* = .{
            .replica_root_dir = try alloc.dupe(u8, replica_root_dir),
            .write_cache = ProvisionedTableWriteCache.init(alloc),
        };
        return cache;
    }
};

var hosted_managed_db_cache_registry_mutex: std.atomic.Mutex = .unlocked;
var hosted_managed_db_cache_registry: std.ArrayListUnmanaged(*HostedManagedDbCache) = .empty;

pub fn accumulateTextMemoryAttributionStats(dst: *db_mod.TextMemoryAttributionStats, src: db_mod.TextMemoryAttributionStats) void {
    dst.text_indexes +|= src.text_indexes;
    dst.text_segments +|= src.text_segments;
    dst.text_segment_bytes +|= src.text_segment_bytes;
    dst.text_mmap_segment_bytes +|= src.text_mmap_segment_bytes;
    dst.text_heap_segment_bytes +|= src.text_heap_segment_bytes;
    dst.text_max_segment_bytes = @max(dst.text_max_segment_bytes, src.text_max_segment_bytes);
    dst.stored_fields_bytes +|= src.stored_fields_bytes;
    dst.inverted_text_bytes +|= src.inverted_text_bytes;
    dst.inverted_header_bytes +|= src.inverted_header_bytes;
    dst.inverted_norm_bytes +|= src.inverted_norm_bytes;
    dst.inverted_term_dict_bytes +|= src.inverted_term_dict_bytes;
    dst.inverted_term_block_bytes +|= src.inverted_term_block_bytes;
    dst.inverted_term_index_bytes +|= src.inverted_term_index_bytes;
    dst.inverted_fst_bytes +|= src.inverted_fst_bytes;
    dst.inverted_bloom_bytes +|= src.inverted_bloom_bytes;
    dst.inverted_postings_bytes +|= src.inverted_postings_bytes;
    dst.inverted_postings_header_bytes +|= src.inverted_postings_header_bytes;
    dst.inverted_block_max_bytes +|= src.inverted_block_max_bytes;
    dst.inverted_chunk_meta_bytes +|= src.inverted_chunk_meta_bytes;
    dst.inverted_postings_payload_bytes +|= src.inverted_postings_payload_bytes;
    dst.inverted_positions_bytes +|= src.inverted_positions_bytes;
    dst.inverted_skip_bytes +|= src.inverted_skip_bytes;
    dst.inverted_one_hit_terms +|= src.inverted_one_hit_terms;
    dst.inverted_postings_terms +|= src.inverted_postings_terms;
    dst.typed_doc_values_bytes +|= src.typed_doc_values_bytes;
    dst.doc_ordinals_bytes +|= src.doc_ordinals_bytes;
    dst.section_index_bytes +|= src.section_index_bytes;
    dst.configured_lmdb_main_map_bytes +|= src.configured_lmdb_main_map_bytes;
    dst.configured_lmdb_wal_map_bytes +|= src.configured_lmdb_wal_map_bytes;
}

fn dbHbcCacheKindStatsFromIndex(cache_stats: hbc_mod.HbcCacheKindStats) db_mod.types.HbcCacheKindStats {
    return .{
        .used_bytes = cache_stats.used_bytes,
        .peak_bytes = cache_stats.peak_bytes,
        .insertions = cache_stats.insertions,
        .admission_skips = cache_stats.admission_skips,
        .evictions = cache_stats.evictions,
    };
}

fn dbHbcCacheStatsFromIndex(cache_stats: hbc_mod.HbcCacheStats) db_mod.types.HbcCacheStats {
    return .{
        .total_bytes = cache_stats.total_bytes,
        .accounted_bytes = cache_stats.accounted_bytes,
        .node = dbHbcCacheKindStatsFromIndex(cache_stats.node),
        .quantized = dbHbcCacheKindStatsFromIndex(cache_stats.quantized),
        .vector = dbHbcCacheKindStatsFromIndex(cache_stats.vector),
        .metadata = dbHbcCacheKindStatsFromIndex(cache_stats.metadata),
    };
}

fn dbHbcPostingStatsFromIndex(backlog: hbc_mod.PostingBacklogStats, profile: hbc_mod.WriteProfile) db_mod.types.HbcPostingStats {
    return .{
        .scanned_nodes = backlog.scanned_nodes,
        .scanned_postings = backlog.scanned_postings,
        .dirty_postings = backlog.dirty_postings,
        .centroid_dirty_postings = backlog.centroid_dirty_postings,
        .payload_dirty_postings = backlog.payload_dirty_postings,
        .max_centroid_version_lag = backlog.max_centroid_version_lag,
        .max_payload_version_lag = backlog.max_payload_version_lag,
        .max_mutation_version = backlog.max_mutation_version,
        .skipped_missing = backlog.skipped_missing,
        .maintenance_scanned_nodes = profile.posting_maintenance_scanned_nodes,
        .maintenance_scanned_postings = profile.posting_maintenance_scanned_postings,
        .maintenance_dirty_postings = profile.posting_maintenance_dirty_postings,
        .maintenance_repaired_postings = profile.posting_maintenance_repaired_postings,
        .maintenance_centroid_refreshed = profile.posting_maintenance_centroid_refreshed,
        .maintenance_payload_refreshed = profile.posting_maintenance_payload_refreshed,
        .maintenance_ancestor_refresh_roots = profile.posting_maintenance_ancestor_refresh_roots,
        .maintenance_split_postings = profile.posting_maintenance_split_postings,
        .maintenance_merged_postings = profile.posting_maintenance_merged_postings,
        .maintenance_boundary_reassigned_vectors = profile.posting_maintenance_boundary_reassigned_vectors,
        .lazy_centroid_deferrals = profile.posting_lazy_centroid_deferrals,
        .lazy_payload_deferrals = profile.posting_lazy_payload_deferrals,
        .lazy_ancestor_deferrals = profile.posting_lazy_ancestor_deferrals,
    };
}

pub fn overlayDenseHbcCacheStatsFromDb(stats: *db_mod.types.DBStats, db: *db_mod.DB) void {
    if (!db.core.tryLockApplyShared()) return;
    defer db.core.unlockApplyShared();

    for (stats.indexes) |*item| {
        if (item.kind != .dense_vector) continue;
        if (db.core.denseIndex(item.name)) |entry| {
            item.hbc_cache = dbHbcCacheStatsFromIndex(entry.index.hbcCacheStats());
        }
    }
}

pub fn overlayRuntimeStatusReplayTargetFromDb(status: *runtime_status.LocalTableRuntimeStatus, db: *db_mod.DB) void {
    const target_sequence = db.core.nextDerivedSequence();
    const async_stats = db.snapshotAsyncIndexingStats();
    status.stats.async_indexing = async_stats;
    for (status.stats.indexes) |*item| {
        if (target_sequence > item.replay_target_sequence) {
            item.replay_target_sequence = target_sequence;
            item.catch_up_target_sequence = target_sequence;
        }
        if (item.catch_up_target_sequence < item.replay_target_sequence) {
            item.catch_up_target_sequence = item.replay_target_sequence;
        }
        item.replay_catch_up_required = item.replay_applied_sequence < item.replay_target_sequence;
        item.catch_up_applied_sequence = item.replay_applied_sequence;
        item.catch_up_active = item.kind == .dense_vector and async_stats.dense_catch_up.active;
        item.catch_up_phase = if (item.kind == .dense_vector) async_stats.dense_catch_up.phase else .idle;
    }
}

pub fn startupCatchUpStatsForPhase(
    phase: db_mod.types.StartupCatchUpPhase,
    db: ?*db_mod.DB,
) db_mod.types.StartupCatchUpStats {
    var stats: db_mod.types.StartupCatchUpStats = if (db) |managed_db|
        managed_db.snapshotAsyncIndexingStats().startup
    else
        .{};
    stats.active = phase != .idle;
    stats.phase = phase;
    if (db) |managed_db| {
        const maintenance = managed_db.snapshotLsmMaintenanceStats();
        stats.wal_retention_known = true;
        stats.wal_retained_segments = maintenance.wal_retained_segments;
        stats.wal_retained_bytes = maintenance.wal_retained_bytes;
    }
    return stats;
}

pub fn startupCatchUpStatsForPath(
    path: []const u8,
    phase: db_mod.types.StartupCatchUpPhase,
    configured_indexes: ?*const StartupConfiguredIndexes,
) !db_mod.types.StartupCatchUpStats {
    var stats: db_mod.types.StartupCatchUpStats = .{
        .active = phase != .idle,
        .phase = phase,
        .wal_retention_known = phase != .idle,
    };
    if (phase == .idle) return stats;

    var native = try lsm_backend.storage_io.NativeStorage.init(std.heap.page_allocator, .threaded);
    defer native.deinit();

    const main_retention = try lsm_backend.wal.snapshotRetention(native.storage(), std.heap.page_allocator, path);
    const replay_retention = try lsm_backend.wal.snapshotReplayRetention(native.storage(), std.heap.page_allocator, path);
    stats.wal_retained_segments = main_retention.segments + replay_retention.segments;
    stats.wal_retained_bytes = main_retention.bytes + replay_retention.bytes;
    if (configured_indexes) |summary| {
        summary.populateConfiguredCounts(&stats);
        try summary.accumulateRetention(native.storage(), std.heap.page_allocator, path, &stats);
    }
    return stats;
}

pub fn applyStartupCatchUpAsyncOverlay(
    status: *runtime_status.LocalTableRuntimeStatus,
    async_stats: db_mod.types.AsyncIndexingStats,
    startup: db_mod.types.StartupCatchUpStats,
) void {
    status.stats.async_indexing = async_stats;
    var merged_startup = status.stats.async_indexing.startup;
    db_mod.types.accumulateStartupCatchUpStats(&merged_startup, startup);
    status.stats.async_indexing.startup = merged_startup;
}

pub fn syntheticStartupRuntimeStatusFromConfiguredIndexes(
    alloc: std.mem.Allocator,
    group_id: u64,
    configured_indexes: *const StartupConfiguredIndexes,
    startup: db_mod.types.StartupCatchUpStats,
) !runtime_status.LocalTableRuntimeStatus {
    const indexes = try alloc.alloc(db_mod.types.DBIndexStats, configured_indexes.items.len);
    var initialized: usize = 0;
    errdefer {
        for (indexes[0..initialized]) |index| freeSyntheticStartupIndexStatsItem(alloc, index);
        alloc.free(indexes);
    }

    for (configured_indexes.items) |item| {
        var stats = db_mod.types.DBIndexStats{
            .name = try alloc.dupe(u8, item.name),
            .kind = item.kind,
        };
        errdefer freeSyntheticStartupIndexStatsItem(alloc, stats);
        try item.populateStats(alloc, &stats);
        indexes[initialized] = stats;
        initialized += 1;
    }

    return .{
        .group_id = group_id,
        .stats = .{
            .index_count = @intCast(indexes.len),
            .indexes = indexes,
            .async_indexing = .{ .startup = startup },
        },
    };
}

pub fn snapshotLocalTableRuntimeStatusesUncached(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    replica_root_dir: []const u8,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    table_name: []const u8,
) !?runtime_status.LocalTableRuntimeStatuses {
    const group_ids = try table_catalog.resolveGroupsForSpanEventually(
        alloc,
        catalog,
        table_name,
        "",
        "",
        5 * std.time.ns_per_s,
        10,
    );
    defer alloc.free(group_ids);
    if (group_ids.len == 0) return null;

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, group_ids.len);
    var initialized: usize = 0;
    errdefer {
        for (items[0..initialized]) |*item| item.deinit(alloc);
        alloc.free(items);
    }

    for (group_ids) |group_id| {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
        defer alloc.free(path);

        var db = try openManagedDbForStatusWithCache(alloc, path, catalog, table_name, group_id, null, null, backend_current_root_generation, null, backend_runtime);
        errdefer db.close();
        items[initialized] = .{
            .group_id = group_id,
            .stats = try db.runtimeStatusStatsConsistent(alloc),
        };
        initialized += 1;
        db.close();
    }

    return .{ .items = items };
}

pub const RuntimeStatusSnapshotMode = enum {
    best_effort,
    consistent,
    try_consistent,
};

pub fn publishRuntimeStatusSnapshot(
    snapshot_cache: ?*runtime_status.TableRuntimeSnapshotCache,
    startup_catch_up_active: bool,
    alloc: std.mem.Allocator,
    table_name: []const u8,
    group_id: u64,
    db: *db_mod.DB,
) !void {
    _ = try publishRuntimeStatusSnapshotWithStartupPhaseMode(
        snapshot_cache,
        startup_catch_up_active,
        alloc,
        table_name,
        group_id,
        if (startup_catch_up_active) .startup_catch_up else .idle,
        .best_effort,
        db,
    );
}

pub fn publishRuntimeStatusSnapshotConsistent(
    snapshot_cache: ?*runtime_status.TableRuntimeSnapshotCache,
    startup_catch_up_active: bool,
    alloc: std.mem.Allocator,
    table_name: []const u8,
    group_id: u64,
    db: *db_mod.DB,
) !void {
    _ = try publishRuntimeStatusSnapshotWithStartupPhaseMode(
        snapshot_cache,
        startup_catch_up_active,
        alloc,
        table_name,
        group_id,
        if (startup_catch_up_active) .startup_catch_up else .idle,
        .consistent,
        db,
    );
}

pub fn tryPublishRuntimeStatusSnapshotConsistent(
    snapshot_cache: ?*runtime_status.TableRuntimeSnapshotCache,
    startup_catch_up_active: bool,
    alloc: std.mem.Allocator,
    table_name: []const u8,
    group_id: u64,
    db: *db_mod.DB,
) !bool {
    return try publishRuntimeStatusSnapshotWithStartupPhaseMode(
        snapshot_cache,
        startup_catch_up_active,
        alloc,
        table_name,
        group_id,
        if (startup_catch_up_active) .startup_catch_up else .idle,
        .try_consistent,
        db,
    );
}

pub fn publishRuntimeStatusSnapshotWithStartupPhase(
    snapshot_cache: ?*runtime_status.TableRuntimeSnapshotCache,
    startup_catch_up_active: bool,
    alloc: std.mem.Allocator,
    table_name: []const u8,
    group_id: u64,
    phase: db_mod.types.StartupCatchUpPhase,
    db: *db_mod.DB,
) !void {
    _ = try publishRuntimeStatusSnapshotWithStartupPhaseMode(
        snapshot_cache,
        startup_catch_up_active,
        alloc,
        table_name,
        group_id,
        phase,
        .best_effort,
        db,
    );
}

pub fn publishRuntimeStatusSnapshotWithStartupPhaseMode(
    snapshot_cache_opt: ?*runtime_status.TableRuntimeSnapshotCache,
    startup_catch_up_active: bool,
    alloc: std.mem.Allocator,
    table_name: []const u8,
    group_id: u64,
    phase: db_mod.types.StartupCatchUpPhase,
    mode: RuntimeStatusSnapshotMode,
    db: *db_mod.DB,
) !bool {
    const snapshot_cache = snapshot_cache_opt orelse return true;
    const async_stats = db.snapshotAsyncIndexingStats();
    var cached_startup: db_mod.types.StartupCatchUpStats = .{};
    var status = runtime_status.LocalTableRuntimeStatus{
        .group_id = group_id,
        .stats = .{},
    };
    var status_initialized = false;
    defer {
        if (status_initialized) {
            var owned = status;
            owned.deinit(alloc);
        }
    }
    if (phase != .idle) {
        cached_startup = try cachedStartupCatchUpStats(snapshot_cache, alloc, table_name, group_id);
    }
    if (try snapshot_cache.snapshotGroupStatus(alloc, table_name, group_id)) |cached_status| {
        switch (mode) {
            .best_effort => {
                status = cached_status;
                status_initialized = true;
                db.overlayRuntimeStatusBestEffort(alloc, &status.stats);
            },
            .consistent => {
                const disk_bytes = cached_status.disk_bytes;
                const created_at_millis = cached_status.created_at_millis;
                var discard = cached_status;
                discard.deinit(alloc);
                status = .{
                    .group_id = group_id,
                    .disk_bytes = disk_bytes,
                    .created_at_millis = created_at_millis,
                    .stats = try db.runtimeStatusStatsConsistent(alloc),
                };
                status_initialized = true;
            },
            .try_consistent => {
                const disk_bytes = cached_status.disk_bytes;
                const created_at_millis = cached_status.created_at_millis;
                var discard = cached_status;
                discard.deinit(alloc);
                const stats = (try db.tryRuntimeStatusStatsConsistent(alloc)) orelse return false;
                status = .{
                    .group_id = group_id,
                    .disk_bytes = disk_bytes,
                    .created_at_millis = created_at_millis,
                    .stats = stats,
                };
                status_initialized = true;
            },
        }
        markRuntimeStatusFromDb(&status, phase, startup_catch_up_active);
    }
    if (!status_initialized) {
        status = .{
            .group_id = group_id,
            .stats = switch (mode) {
                .best_effort => try db.stats(alloc),
                .consistent => try db.runtimeStatusStatsConsistent(alloc),
                .try_consistent => (try db.tryRuntimeStatusStatsConsistent(alloc)) orelse return false,
            },
        };
        status_initialized = true;
        markRuntimeStatusFromDb(&status, phase, startup_catch_up_active);
    }
    var startup = startupCatchUpStatsForPhase(phase, db);
    if (!startup.wal_retention_known and cached_startup.wal_retention_known) {
        startup.wal_retention_known = true;
        startup.wal_retained_segments = cached_startup.wal_retained_segments;
        startup.wal_retained_bytes = cached_startup.wal_retained_bytes;
    }
    applyStartupCatchUpAsyncOverlay(&status, async_stats, startup);
    try snapshot_cache.upsertGroupStatus(table_name, status);
    return true;
}

fn markRuntimeStatusFromDb(
    status: *runtime_status.LocalTableRuntimeStatus,
    phase: db_mod.types.StartupCatchUpPhase,
    startup_catch_up_active: bool,
) void {
    status.metadata = .{
        .updated_at_ns = platform_time.monotonicNs(),
        .source = if (phase != .idle or startup_catch_up_active)
            .startup_catch_up
        else
            .live_writer_publish,
        .freshness = .fresh,
    };
}

fn cachedStartupCatchUpStats(
    snapshot_cache: *runtime_status.TableRuntimeSnapshotCache,
    alloc: std.mem.Allocator,
    table_name: []const u8,
    group_id: u64,
) !db_mod.types.StartupCatchUpStats {
    if (try snapshot_cache.snapshotGroupStatus(alloc, table_name, group_id)) |owned_status| {
        defer {
            var to_free = owned_status;
            to_free.deinit(alloc);
        }
        return owned_status.stats.async_indexing.startup;
    }
    return .{};
}

pub fn publishStartupCatchUpRuntimeStatusSnapshot(
    snapshot_cache_opt: ?*runtime_status.TableRuntimeSnapshotCache,
    alloc: std.mem.Allocator,
    table_name: []const u8,
    group_id: u64,
    startup: db_mod.types.StartupCatchUpStats,
    db: ?*db_mod.DB,
    configured_indexes: ?*const StartupConfiguredIndexes,
) !void {
    const snapshot_cache = snapshot_cache_opt orelse return;
    var status = runtime_status.LocalTableRuntimeStatus{
        .group_id = group_id,
        .stats = .{},
    };
    var status_initialized = false;
    defer {
        if (status_initialized) {
            var owned = status;
            owned.deinit(alloc);
        }
    }

    if (startup.active) {
        if (db) |managed_db| {
            status = .{
                .group_id = group_id,
                .stats = try managed_db.runtimeStatusStatsConsistent(alloc),
            };
            status_initialized = true;
            var merged_startup = startup;
            if (!merged_startup.wal_retention_known) {
                const cached_startup = try cachedStartupCatchUpStats(snapshot_cache, alloc, table_name, group_id);
                merged_startup.wal_retention_known = cached_startup.wal_retention_known;
                merged_startup.wal_retained_segments = cached_startup.wal_retained_segments;
                merged_startup.wal_retained_bytes = cached_startup.wal_retained_bytes;
            }
            applyStartupCatchUpAsyncOverlay(&status, managed_db.snapshotAsyncIndexingStats(), merged_startup);
        } else if (try snapshot_cache.snapshotGroupStatus(alloc, table_name, group_id)) |owned_status| {
            status = owned_status;
            status_initialized = true;
            var merged_startup = startup;
            if (!merged_startup.wal_retention_known) {
                const cached_startup = status.stats.async_indexing.startup;
                merged_startup.wal_retention_known = cached_startup.wal_retention_known;
                merged_startup.wal_retained_segments = cached_startup.wal_retained_segments;
                merged_startup.wal_retained_bytes = cached_startup.wal_retained_bytes;
            }
            var merged_existing = status.stats.async_indexing.startup;
            db_mod.types.accumulateStartupCatchUpStats(&merged_existing, merged_startup);
            status.stats.async_indexing.startup = merged_existing;
        } else if (configured_indexes) |summary| {
            status = try syntheticStartupRuntimeStatusFromConfiguredIndexes(alloc, group_id, summary, startup);
            status_initialized = true;
        }
    } else if (db) |managed_db| {
        status = .{
            .group_id = group_id,
            .stats = try managed_db.runtimeStatusStatsConsistent(alloc),
        };
        applyStartupCatchUpAsyncOverlay(&status, managed_db.snapshotAsyncIndexingStats(), startup);
        status_initialized = true;
    } else if (try snapshot_cache.snapshot(alloc, table_name)) |owned_statuses| {
        var statuses = owned_statuses;
        defer statuses.deinit(alloc);
        for (statuses.items) |item| {
            if (item.group_id != group_id) continue;
            status = try item.clone(alloc);
            status_initialized = true;
            break;
        }
    }

    if (!status_initialized) return;

    status.group_id = group_id;
    if (!startup.active and db == null) {
        var merged_startup = status.stats.async_indexing.startup;
        db_mod.types.accumulateStartupCatchUpStats(&merged_startup, startup);
        status.stats.async_indexing.startup = merged_startup;
    }
    try snapshot_cache.upsertGroupStatus(table_name, status);
}

test "startup catch-up stats for path include table and index-local wal retention" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/startup-path-retention/table-db", .{tmp.sub_path});
    defer alloc.free(db_path);

    var native = try lsm_backend.storage_io.NativeStorage.init(alloc, .threaded);
    defer native.deinit();
    try native.storage().createDirPath(db_path);

    const index_path = try std.fmt.allocPrint(alloc, "{s}/indexes/vec", .{db_path});
    defer alloc.free(index_path);
    try native.storage().createDirPath(index_path);

    _ = try lsm_backend.wal.appendReplay(
        native.storage(),
        alloc,
        db_path,
        1,
        "first",
        false,
        .{},
    );

    var index_state: lsm_backend.state.State = .{};
    defer index_state.deinit(alloc);
    try index_state.appendUpsert(alloc, .{ .name = "docs" }, "doc:a", "A", false);
    _ = try lsm_backend.wal.appendState(native.storage(), alloc, index_path, index_state, false);

    var configured_indexes = try table_write_index_config.parseStartupConfiguredIndexes(
        alloc,
        "{\"indexes\":[{\"name\":\"vec\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":3}}]}",
    );
    defer configured_indexes.deinit(alloc);
    const stats = try startupCatchUpStatsForPath(db_path, .opening_db, &configured_indexes);
    try std.testing.expect(stats.active);
    try std.testing.expectEqual(db_mod.types.StartupCatchUpPhase.opening_db, stats.phase);
    try std.testing.expectEqual(@as(u64, 2), stats.wal_retained_segments);
    try std.testing.expect(stats.wal_retained_bytes > 0);
    try std.testing.expectEqual(@as(u64, 1), stats.configured_indexes);
    try std.testing.expectEqual(@as(u64, 1), stats.configured_dense_indexes);
}

test "runtime status startup snapshot builds synthetic status from object-form indexes json" {
    const alloc = std.testing.allocator;

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var configured_indexes = try table_write_index_config.parseStartupConfiguredIndexes(
        alloc,
        "{\"vec\":{\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":768}},\"fts\":{\"type\":\"full_text\"},\"alg\":{\"type\":\"algebraic\",\"version\":2,\"schema_version\":42,\"capability_fingerprint\":\"cap:v1\",\"capability_lifecycle_status\":\"rebuild_required\",\"capability_change_added_fields\":1,\"capability_change_removed_fields\":2,\"capability_change_changed_type_fields\":3,\"skipped_dynamic_fields\":4,\"skipped_complex_fields\":5,\"skipped_unbounded_fields\":6,\"materializations\":[]}}",
    );
    defer configured_indexes.deinit(alloc);

    try publishStartupCatchUpRuntimeStatusSnapshot(&snapshot_cache, alloc, "docs", 7001, .{
        .active = true,
        .phase = .opening_db,
        .configured_indexes = 3,
        .configured_dense_indexes = 1,
        .configured_full_text_indexes = 1,
        .wal_retained_segments = 5,
        .wal_retained_bytes = 123,
    }, null, &configured_indexes);
    try publishStartupCatchUpRuntimeStatusSnapshot(&snapshot_cache, alloc, "docs", 7001, .{}, null, null);

    var statuses = (try snapshot_cache.snapshot(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(usize, 3), statuses.items[0].stats.indexes.len);
    try std.testing.expectEqualStrings("vec", statuses.items[0].stats.indexes[0].name);
    try std.testing.expectEqual(db_mod.types.IndexKind.dense_vector, statuses.items[0].stats.indexes[0].kind);
    try std.testing.expectEqualStrings("fts", statuses.items[0].stats.indexes[1].name);
    try std.testing.expectEqual(db_mod.types.IndexKind.full_text, statuses.items[0].stats.indexes[1].kind);
    try std.testing.expectEqualStrings("alg", statuses.items[0].stats.indexes[2].name);
    try std.testing.expectEqual(db_mod.types.IndexKind.algebraic, statuses.items[0].stats.indexes[2].kind);
    try std.testing.expectEqual(@as(u32, 42), statuses.items[0].stats.indexes[2].algebraic_schema_version);
    try std.testing.expectEqualStrings("cap:v1", statuses.items[0].stats.indexes[2].algebraic_capability_fingerprint.?);
    try std.testing.expectEqualStrings("rebuild_required", statuses.items[0].stats.indexes[2].algebraic_capability_lifecycle_status.?);
    try std.testing.expectEqual(@as(u32, 1), statuses.items[0].stats.indexes[2].algebraic_capability_change_added_fields);
    try std.testing.expectEqual(@as(u32, 2), statuses.items[0].stats.indexes[2].algebraic_capability_change_removed_fields);
    try std.testing.expectEqual(@as(u32, 3), statuses.items[0].stats.indexes[2].algebraic_capability_change_changed_type_fields);
    try std.testing.expectEqual(@as(u32, 4), statuses.items[0].stats.indexes[2].algebraic_skipped_dynamic_fields);
    try std.testing.expectEqual(@as(u32, 5), statuses.items[0].stats.indexes[2].algebraic_skipped_complex_fields);
    try std.testing.expectEqual(@as(u32, 6), statuses.items[0].stats.indexes[2].algebraic_skipped_unbounded_fields);
    try std.testing.expect(statuses.items[0].stats.async_indexing.startup.active);
    try std.testing.expectEqual(db_mod.types.StartupCatchUpPhase.opening_db, statuses.items[0].stats.async_indexing.startup.phase);
    try std.testing.expectEqual(@as(u64, 3), statuses.items[0].stats.async_indexing.startup.configured_indexes);
    try std.testing.expectEqual(@as(u64, 1), statuses.items[0].stats.async_indexing.startup.configured_dense_indexes);
    try std.testing.expectEqual(@as(u64, 1), statuses.items[0].stats.async_indexing.startup.configured_full_text_indexes);
}

test "runtime status startup snapshot builds synthetic status from array-form indexes json" {
    const alloc = std.testing.allocator;

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var configured_indexes = try table_write_index_config.parseStartupConfiguredIndexes(
        alloc,
        "{\"indexes\":[{\"name\":\"vec\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":768}},{\"name\":\"fts\",\"type\":\"full_text\",\"config\":{}}]}",
    );
    defer configured_indexes.deinit(alloc);

    try publishStartupCatchUpRuntimeStatusSnapshot(&snapshot_cache, alloc, "docs", 7001, .{
        .active = true,
        .phase = .opening_db,
        .configured_indexes = 2,
        .configured_dense_indexes = 1,
        .configured_full_text_indexes = 1,
        .wal_retained_segments = 7,
        .wal_retained_bytes = 321,
    }, null, &configured_indexes);
    try publishStartupCatchUpRuntimeStatusSnapshot(&snapshot_cache, alloc, "docs", 7001, .{}, null, null);

    var statuses = (try snapshot_cache.snapshot(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(usize, 2), statuses.items[0].stats.indexes.len);
    try std.testing.expectEqualStrings("vec", statuses.items[0].stats.indexes[0].name);
    try std.testing.expectEqual(db_mod.types.IndexKind.dense_vector, statuses.items[0].stats.indexes[0].kind);
    try std.testing.expectEqualStrings("fts", statuses.items[0].stats.indexes[1].name);
    try std.testing.expectEqual(db_mod.types.IndexKind.full_text, statuses.items[0].stats.indexes[1].kind);
    try std.testing.expect(statuses.items[0].stats.async_indexing.startup.active);
    try std.testing.expectEqual(db_mod.types.StartupCatchUpPhase.opening_db, statuses.items[0].stats.async_indexing.startup.phase);
    try std.testing.expectEqual(@as(u64, 2), statuses.items[0].stats.async_indexing.startup.configured_indexes);
}

fn freeSyntheticStartupIndexStatsItem(alloc: std.mem.Allocator, item: db_mod.types.DBIndexStats) void {
    alloc.free(item.name);
    if (item.algebraic_capability_fingerprint) |value| alloc.free(value);
    if (item.algebraic_capability_lifecycle_status) |value| alloc.free(value);
}

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    platform_sync.lockYielding(mutex);
}

pub fn closeHostedManagedDbCacheForRoot(replica_root_dir: []const u8) void {
    const alloc = std.heap.page_allocator;
    var removed: ?*HostedManagedDbCache = null;
    lockAtomic(&hosted_managed_db_cache_registry_mutex);
    {
        defer hosted_managed_db_cache_registry_mutex.unlock();
        for (hosted_managed_db_cache_registry.items, 0..) |cache, idx| {
            if (!std.mem.eql(u8, cache.replica_root_dir, replica_root_dir)) continue;
            removed = hosted_managed_db_cache_registry.orderedRemove(idx);
            break;
        }
    }

    const cache = removed orelse return;
    lockAtomic(&cache.mutex);
    cache.write_cache.deinit();
    cache.mutex.unlock();
    alloc.free(cache.replica_root_dir);
    alloc.destroy(cache);
}

pub fn hostedManagedDbCacheForRoot(replica_root_dir: []const u8) !*HostedManagedDbCache {
    const alloc = std.heap.page_allocator;
    lockAtomic(&hosted_managed_db_cache_registry_mutex);
    defer hosted_managed_db_cache_registry_mutex.unlock();

    for (hosted_managed_db_cache_registry.items) |cache| {
        if (std.mem.eql(u8, cache.replica_root_dir, replica_root_dir)) return cache;
    }

    const cache = try HostedManagedDbCache.init(alloc, replica_root_dir);
    errdefer {
        alloc.free(cache.replica_root_dir);
        alloc.destroy(cache);
    }
    try hosted_managed_db_cache_registry.append(alloc, cache);
    return cache;
}

pub fn hostedManagedDbCacheForRootIfPresent(replica_root_dir: []const u8) ?*HostedManagedDbCache {
    lockAtomic(&hosted_managed_db_cache_registry_mutex);
    defer hosted_managed_db_cache_registry_mutex.unlock();

    for (hosted_managed_db_cache_registry.items) |cache| {
        if (std.mem.eql(u8, cache.replica_root_dir, replica_root_dir)) return cache;
    }
    return null;
}

pub fn hostedManagedDbCacheDiagnosticsForRoot(replica_root_dir: []const u8) HostedManagedDbCacheDiagnostics {
    lockAtomic(&hosted_managed_db_cache_registry_mutex);
    defer hosted_managed_db_cache_registry_mutex.unlock();
    const cached_roots: u64 = @intCast(hosted_managed_db_cache_registry.items.len);
    for (hosted_managed_db_cache_registry.items) |selected| {
        if (!std.mem.eql(u8, selected.replica_root_dir, replica_root_dir)) continue;
        lockAtomic(&selected.mutex);
        defer selected.mutex.unlock();
        const write_cache = selected.write_cache.diagnosticsLocked();
        return .{
            .present = true,
            .cached_roots = cached_roots,
            .cached_entries = write_cache.cached_entries,
            .retired_entries = write_cache.retired_entries,
            .table_metadata_entries = write_cache.table_metadata_entries,
            .active_bulk_sessions = write_cache.active_bulk_sessions,
            .active_leases = write_cache.active_leases,
            .retired_active_leases = write_cache.retired_active_leases,
            .bulk_ingest_open_entries = write_cache.bulk_ingest_open_entries,
            .auto_bulk_ingest_open_entries = write_cache.auto_bulk_ingest_open_entries,
            .auto_bulk_ingest_finish_requested_entries = write_cache.auto_bulk_ingest_finish_requested_entries,
            .lsm_mutable_bytes = write_cache.lsm_mutable_bytes,
            .lsm_immutable_bytes = write_cache.lsm_immutable_bytes,
            .lsm_total_run_bytes = write_cache.lsm_total_run_bytes,
            .lsm_wal_retained_bytes = write_cache.lsm_wal_retained_bytes,
            .lsm_wal_retained_segments = write_cache.lsm_wal_retained_segments,
            .lsm_active_readers = write_cache.lsm_active_readers,
            .lsm_obsolete_paths = write_cache.lsm_obsolete_paths,
            .lsm_bulk_ingest_current_scan_clone_active_bytes = write_cache.lsm_bulk_ingest_current_scan_clone_active_bytes,
        };
    }
    return .{ .cached_roots = cached_roots };
}

pub const ProvisionedTableWriteCache = struct {
    alloc: std.mem.Allocator,
    lsm_cache: ?*lsm_backend.Cache = null,
    hbc_cache: ?*hbc_mod.Cache = null,
    resource_manager: ?*resource_manager_mod.ResourceManager = null,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime = null,
    antfly_provider: ?managed_embedder.AntflyProvider = null,
    secret_store: ?*common_secrets.FileStore = null,
    remote_content: ?*const scraping.RemoteContentConfig = null,
    /// Cross-shard entity-resolution candidate source applied to every managed
    /// DB this cache opens (set after open; see `setResolutionCandidateSource`).
    resolution_candidate_source: ?db_mod.CandidateSource = null,
    /// Cross-shard entity sink for the promoter, applied to every managed DB
    /// this cache opens (set after open; see `setEntitySink`).
    entity_sink: ?db_mod.EntitySink = null,
    /// Source-group leadership predicate for promotion ownership. When set, each
    /// managed DB gets a group-specific `PromotionOwner` so only the local leader
    /// promotes resolution replay into cross-shard entity writes.
    promotion_leadership_source: ?PromotionLeadershipSource = null,
    /// Optional HA ownership gate applied when this cache opens managed writer
    /// DBs. Changing the gate retires live cached DBs so the next operation
    /// reopens with the correct primary/standby role and background runtimes.
    ha_write_gate: ?db_mod.HAWriteGate = null,
    /// Optional HA primary mirror applied when this cache opens managed writer
    /// DBs. Changing the mirror retires live cached DBs so already-open tables
    /// cannot silently continue without primary-side replication.
    ha_async_mirror: ?db_mod.HAAsyncEffectMirror = null,
    open_mutex: std.atomic.Mutex = .unlocked,
    entry_lifecycle_mutex: std.atomic.Mutex = .unlocked,
    hit_count: std.atomic.Value(u64) = .init(0),
    miss_count: std.atomic.Value(u64) = .init(0),
    entries: std.ArrayListUnmanaged(*Entry) = .empty,
    retired_entries: std.ArrayListUnmanaged(*Entry) = .empty,
    table_metadata: std.ArrayListUnmanaged(TableMetadata) = .empty,
    active_bulk_ingest_sessions: std.ArrayListUnmanaged(ActiveBulkIngestSession) = .empty,

    pub const CacheStats = struct {
        hit_count: u64 = 0,
        miss_count: u64 = 0,
    };

    pub const AutoBulkIngestStats = struct {
        cached_entries: u64 = 0,
        open_entries: u64 = 0,
        active_leases: u64 = 0,
        finish_requested_entries: u64 = 0,
        idle_expired_entries: u64 = 0,
        active_bulk_sessions: u64 = 0,
        total_ops: u64 = 0,
        oldest_idle_ns: u64 = 0,

        pub fn merge(self: *AutoBulkIngestStats, other: AutoBulkIngestStats) void {
            self.cached_entries += other.cached_entries;
            self.open_entries += other.open_entries;
            self.active_leases += other.active_leases;
            self.finish_requested_entries += other.finish_requested_entries;
            self.idle_expired_entries += other.idle_expired_entries;
            self.active_bulk_sessions += other.active_bulk_sessions;
            self.total_ops += other.total_ops;
            self.oldest_idle_ns = @max(self.oldest_idle_ns, other.oldest_idle_ns);
        }
    };

    pub const Diagnostics = struct {
        cached_entries: u64 = 0,
        retired_entries: u64 = 0,
        table_metadata_entries: u64 = 0,
        active_bulk_sessions: u64 = 0,
        active_leases: u64 = 0,
        retired_active_leases: u64 = 0,
        bulk_ingest_open_entries: u64 = 0,
        auto_bulk_ingest_open_entries: u64 = 0,
        auto_bulk_ingest_finish_requested_entries: u64 = 0,
        lsm_mutable_bytes: u64 = 0,
        lsm_immutable_bytes: u64 = 0,
        lsm_total_run_bytes: u64 = 0,
        lsm_wal_retained_bytes: u64 = 0,
        lsm_wal_retained_segments: u64 = 0,
        lsm_active_readers: u64 = 0,
        lsm_obsolete_paths: u64 = 0,
        lsm_bulk_ingest_current_scan_clone_active_bytes: u64 = 0,
    };

    pub const CachedDb = struct {
        cache: ?*ProvisionedTableWriteCache = null,
        entry: ?*Entry = null,
        db: *db_mod.DB,
        schema_json: ?[]const u8,
        owned_db: ?*db_mod.DB = null,

        pub fn deinit(self: *CachedDb, alloc: std.mem.Allocator) void {
            if (self.entry) |entry| {
                if (self.cache) |cache| cache.releaseEntry(entry);
                self.entry = null;
            }
            if (self.owned_db) |owned| {
                owned.close();
                alloc.destroy(owned);
            }
            self.* = undefined;
        }
    };

    pub const PreparedOpen = struct {
        indexes_json: ?[]u8 = null,
        schema_json: ?[]u8 = null,

        pub fn deinit(self: *PreparedOpen, alloc: std.mem.Allocator) void {
            if (self.indexes_json) |value| alloc.free(value);
            if (self.schema_json) |value| alloc.free(value);
            self.* = undefined;
        }
    };

    pub const GetOrPrepareOpen = union(enum) {
        cached: CachedDb,
        prepared: PreparedOpen,
    };

    pub const Entry = struct {
        group_id: u64,
        lsm_root_generation: u64,
        table_name: []u8,
        promotion_owner_state: PromotionOwnerState = .{},
        db: db_mod.DB,
        schema_json: ?[]u8 = null,
        active_leases: usize = 0,
        retired: bool = false,
        allow_generation_adoption: bool = false,
        bulk_ingest_session_open: bool = false,
        auto_bulk_ingest_session_open: bool = false,
        auto_bulk_ingest_ops: usize = 0,
        auto_bulk_ingest_started_ns: u64 = 0,
        auto_bulk_ingest_last_ns: u64 = 0,
        auto_bulk_ingest_finish_requested: bool = false,

        fn deinit(self: *Entry, alloc: std.mem.Allocator) void {
            if (self.bulk_ingest_session_open) {
                if (self.auto_bulk_ingest_session_open) {
                    self.db.finishDenseAutoBulkIngestSessionWithOptions(auto_bulk_ingest_finish_options) catch |err| {
                        std.log.warn("auto bulk ingest finish failed before cached db close table={s} err={s}", .{
                            self.table_name,
                            @errorName(err),
                        });
                        self.db.abortDenseAutoBulkIngestSession();
                    };
                } else {
                    self.db.abortBulkIngestSession();
                }
                self.bulk_ingest_session_open = false;
                self.auto_bulk_ingest_session_open = false;
                self.auto_bulk_ingest_ops = 0;
                self.auto_bulk_ingest_started_ns = 0;
                self.auto_bulk_ingest_last_ns = 0;
                self.auto_bulk_ingest_finish_requested = false;
            }
            // Read-side cache invalidation must not turn the first query after
            // a large weak-sync load into a full derived-index drain. DB.close()
            // tears down async workers and flushes owned storage; callers that
            // require full-index visibility must request it through sync_level
            // or the index status path before reading.
            const pending = self.db.pendingWorkStats();
            if (pending.enrichment.error_count == 0) {
                self.db.core.index_manager.syncAll(false) catch |err| {
                    std.log.warn("provisioned write cache sync failed before close: {}", .{err});
                };
            }
            self.db.close();
            alloc.free(self.table_name);
            if (self.schema_json) |value| alloc.free(value);
            self.* = undefined;
        }
    };

    pub const PromotionLeadershipSource = struct {
        ptr: *anyopaque,
        vtable: *const VTable,

        pub const VTable = struct {
            is_local_leader: *const fn (ptr: *anyopaque, group_id: u64) bool,
        };

        pub fn isLocalLeader(self: PromotionLeadershipSource, group_id: u64) bool {
            return self.vtable.is_local_leader(self.ptr, group_id);
        }
    };

    pub const PromotionOwnerState = struct {
        group_id: u64 = 0,
        leadership_source: ?PromotionLeadershipSource = null,

        pub fn owner(self: *PromotionOwnerState) ?db_mod.PromotionOwner {
            if (self.leadership_source == null) return null;
            return .{ .ptr = self, .vtable = &owner_vtable };
        }

        const owner_vtable = db_mod.PromotionOwner.VTable{ .is_local_owner = isLocalOwner };

        fn isLocalOwner(ptr: *anyopaque) bool {
            const self: *PromotionOwnerState = @ptrCast(@alignCast(ptr));
            const source = self.leadership_source orelse return true;
            return source.isLocalLeader(self.group_id);
        }
    };

    fn applyRuntimeHooksToDb(self: *ProvisionedTableWriteCache, db: *db_mod.DB, group_id: u64, owner_state: ?*PromotionOwnerState) void {
        db.setResolutionCandidateSource(self.resolution_candidate_source);
        db.setEntitySink(self.entity_sink);
        if (owner_state) |state| {
            state.* = .{
                .group_id = group_id,
                .leadership_source = self.promotion_leadership_source,
            };
            db.setPromotionOwner(state.owner());
        } else {
            db.setPromotionOwner(null);
        }
    }

    pub fn retireFailedOpenLocked(self: *ProvisionedTableWriteCache, cached: *CachedDb) void {
        const entry = cached.entry orelse {
            cached.deinit(self.alloc);
            return;
        };

        var i: usize = 0;
        while (i < self.entries.items.len) : (i += 1) {
            if (self.entries.items[i] != entry) continue;
            _ = self.entries.orderedRemove(i);
            break;
        }

        if (!entry.retired) self.retireEntryLocked(entry);
        cached.deinit(self.alloc);
    }

    fn refreshRuntimeHooksLocked(self: *ProvisionedTableWriteCache) void {
        for (self.entries.items) |entry| {
            self.applyRuntimeHooksToDb(&entry.db, entry.group_id, &entry.promotion_owner_state);
        }
    }

    fn candidateSourcesEqual(a: ?db_mod.CandidateSource, b: ?db_mod.CandidateSource) bool {
        if (a == null or b == null) return a == null and b == null;
        return a.?.ptr == b.?.ptr and a.?.vtable == b.?.vtable;
    }

    fn entitySinksEqual(a: ?db_mod.EntitySink, b: ?db_mod.EntitySink) bool {
        if (a == null or b == null) return a == null and b == null;
        return a.?.ptr == b.?.ptr and a.?.vtable == b.?.vtable;
    }

    fn promotionLeadershipSourcesEqual(a: ?PromotionLeadershipSource, b: ?PromotionLeadershipSource) bool {
        if (a == null or b == null) return a == null and b == null;
        return a.?.ptr == b.?.ptr and a.?.vtable == b.?.vtable;
    }

    fn haWriteGatesEqual(a: ?db_mod.HAWriteGate, b: ?db_mod.HAWriteGate) bool {
        if (a == null or b == null) return a == null and b == null;
        return switch (a.?) {
            .primary => |left| switch (b.?) {
                .primary => |right| left == right,
                .fenced_primary => false,
                .standby => false,
            },
            .fenced_primary => |left| switch (b.?) {
                .primary => false,
                .fenced_primary => |right| left.primary == right.primary and
                    left.fence_store == right.fence_store and
                    std.mem.eql(u8, left.node_id, right.node_id),
                .standby => false,
            },
            .standby => |left| switch (b.?) {
                .primary => false,
                .fenced_primary => false,
                .standby => |right| left == right,
            },
        };
    }

    fn syncPoliciesEqual(a: ha_primary_mod.SyncPolicy, b: ha_primary_mod.SyncPolicy) bool {
        if (a.mode != b.mode or
            a.selection != b.selection or
            a.required != b.required or
            a.failure_policy != b.failure_policy or
            a.standby_names.len != b.standby_names.len)
        {
            return false;
        }
        for (a.standby_names, b.standby_names) |left, right| {
            if (!std.mem.eql(u8, left, right)) return false;
        }
        return true;
    }

    fn haAsyncMirrorsEqual(a: ?db_mod.HAAsyncEffectMirror, b: ?db_mod.HAAsyncEffectMirror) bool {
        if (a == null or b == null) return a == null and b == null;
        const left = a.?;
        const right = b.?;
        return left.primary == right.primary and
            left.last_lsn == right.last_lsn and
            left.failure_count == right.failure_count and
            syncPoliciesEqual(left.sync_policy, right.sync_policy) and
            left.sync_wait_ctx == right.sync_wait_ctx and
            left.sync_wait_fn == right.sync_wait_fn and
            left.last_gate_lsn == right.last_gate_lsn and
            left.last_gate_action == right.last_gate_action and
            left.sync_reject_count == right.sync_reject_count and
            left.sync_wait_count == right.sync_wait_count and
            left.sync_degraded_count == right.sync_degraded_count;
    }

    fn runtimeHooksEqual(
        self: *const ProvisionedTableWriteCache,
        candidate_source: ?db_mod.CandidateSource,
        entity_sink_value: ?db_mod.EntitySink,
        leadership_source: ?PromotionLeadershipSource,
    ) bool {
        return candidateSourcesEqual(self.resolution_candidate_source, candidate_source) and
            entitySinksEqual(self.entity_sink, entity_sink_value) and
            promotionLeadershipSourcesEqual(self.promotion_leadership_source, leadership_source);
    }

    pub fn setRuntimeHooksLocked(
        self: *ProvisionedTableWriteCache,
        candidate_source: ?db_mod.CandidateSource,
        entity_sink_value: ?db_mod.EntitySink,
        leadership_source: ?PromotionLeadershipSource,
    ) void {
        if (self.runtimeHooksEqual(candidate_source, entity_sink_value, leadership_source)) return;
        self.resolution_candidate_source = candidate_source;
        self.entity_sink = entity_sink_value;
        self.promotion_leadership_source = leadership_source;
        self.refreshRuntimeHooksLocked();
    }

    pub fn setResolutionCandidateSource(self: *ProvisionedTableWriteCache, source: ?db_mod.CandidateSource) void {
        lockAtomic(&self.open_mutex);
        defer self.open_mutex.unlock();
        if (candidateSourcesEqual(self.resolution_candidate_source, source)) return;
        self.resolution_candidate_source = source;
        self.refreshRuntimeHooksLocked();
    }

    pub fn setEntitySink(self: *ProvisionedTableWriteCache, sink: ?db_mod.EntitySink) void {
        lockAtomic(&self.open_mutex);
        defer self.open_mutex.unlock();
        if (entitySinksEqual(self.entity_sink, sink)) return;
        self.entity_sink = sink;
        self.refreshRuntimeHooksLocked();
    }

    pub fn setPromotionLeadershipSource(self: *ProvisionedTableWriteCache, source: ?PromotionLeadershipSource) void {
        lockAtomic(&self.open_mutex);
        defer self.open_mutex.unlock();
        if (promotionLeadershipSourcesEqual(self.promotion_leadership_source, source)) return;
        self.promotion_leadership_source = source;
        self.refreshRuntimeHooksLocked();
    }

    pub fn setHAWriteGate(self: *ProvisionedTableWriteCache, gate: ?db_mod.HAWriteGate) void {
        lockAtomic(&self.open_mutex);
        defer self.open_mutex.unlock();
        if (haWriteGatesEqual(self.ha_write_gate, gate)) return;
        self.ha_write_gate = gate;
        self.clear();
    }

    pub fn setHAMirror(self: *ProvisionedTableWriteCache, mirror: ?db_mod.HAAsyncEffectMirror) void {
        lockAtomic(&self.open_mutex);
        defer self.open_mutex.unlock();
        if (haAsyncMirrorsEqual(self.ha_async_mirror, mirror)) return;
        self.ha_async_mirror = mirror;
        self.clear();
    }

    const ActiveBulkIngestSession = struct {
        table_name: []u8,
        depth: usize = 1,

        pub fn deinit(self: *ActiveBulkIngestSession, alloc: std.mem.Allocator) void {
            alloc.free(self.table_name);
            self.* = undefined;
        }
    };

    pub const TableMetadata = struct {
        table_name: []u8,
        indexes_json: ?[]u8,
        schema_json: ?[]u8,

        fn deinit(self: *TableMetadata, alloc: std.mem.Allocator) void {
            alloc.free(self.table_name);
            if (self.indexes_json) |value| alloc.free(value);
            if (self.schema_json) |value| alloc.free(value);
            self.* = undefined;
        }
    };

    fn cloneTableMetadataAlloc(
        self: *ProvisionedTableWriteCache,
        table_name: []const u8,
        indexes_json: ?[]const u8,
        schema_json: ?[]const u8,
    ) !TableMetadata {
        const owned_table_name = try self.alloc.dupe(u8, table_name);
        errdefer self.alloc.free(owned_table_name);
        const owned_indexes_json = if (indexes_json) |value| try self.alloc.dupe(u8, value) else null;
        errdefer if (owned_indexes_json) |value| self.alloc.free(value);
        const owned_schema_json = if (schema_json) |value| try self.alloc.dupe(u8, value) else null;
        errdefer if (owned_schema_json) |value| self.alloc.free(value);
        return .{
            .table_name = owned_table_name,
            .indexes_json = owned_indexes_json,
            .schema_json = owned_schema_json,
        };
    }

    pub fn init(alloc: std.mem.Allocator) ProvisionedTableWriteCache {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *ProvisionedTableWriteCache) void {
        for (self.entries.items) |entry| {
            entry.deinit(self.alloc);
            self.alloc.destroy(entry);
        }
        self.entries.deinit(self.alloc);
        for (self.retired_entries.items) |entry| {
            entry.deinit(self.alloc);
            self.alloc.destroy(entry);
        }
        self.retired_entries.deinit(self.alloc);
        for (self.table_metadata.items) |*metadata| metadata.deinit(self.alloc);
        self.table_metadata.deinit(self.alloc);
        for (self.active_bulk_ingest_sessions.items) |*session| session.deinit(self.alloc);
        self.active_bulk_ingest_sessions.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn clear(self: *ProvisionedTableWriteCache) void {
        var leased_retirements: usize = 0;
        for (self.entries.items) |entry| {
            if (entry.active_leases > 0) leased_retirements += 1;
        }
        self.retired_entries.ensureUnusedCapacity(self.alloc, leased_retirements) catch return;

        for (self.entries.items) |entry| self.retireEntryLocked(entry);
        self.entries.clearRetainingCapacity();
        for (self.table_metadata.items) |*metadata| metadata.deinit(self.alloc);
        self.table_metadata.clearRetainingCapacity();
        for (self.active_bulk_ingest_sessions.items) |*session| session.deinit(self.alloc);
        self.active_bulk_ingest_sessions.clearRetainingCapacity();
    }

    pub fn cacheStats(self: *const ProvisionedTableWriteCache) CacheStats {
        return .{
            .hit_count = self.hit_count.load(.monotonic),
            .miss_count = self.miss_count.load(.monotonic),
        };
    }

    pub fn autoBulkIngestStatsLocked(self: *const ProvisionedTableWriteCache, now_ns: u64) AutoBulkIngestStats {
        var stats = AutoBulkIngestStats{
            .cached_entries = @intCast(self.entries.items.len),
            .active_bulk_sessions = @intCast(self.active_bulk_ingest_sessions.items.len),
        };
        for (self.entries.items) |entry| {
            stats.active_leases += entry.active_leases;
            if (!entry.auto_bulk_ingest_session_open) continue;
            stats.open_entries += 1;
            stats.total_ops += entry.auto_bulk_ingest_ops;
            if (entry.auto_bulk_ingest_finish_requested) stats.finish_requested_entries += 1;
            if (entry.auto_bulk_ingest_last_ns > 0) {
                const idle_ns = now_ns -| entry.auto_bulk_ingest_last_ns;
                stats.oldest_idle_ns = @max(stats.oldest_idle_ns, idle_ns);
                if (idle_ns >= auto_bulk_ingest_max_idle_ns) stats.idle_expired_entries += 1;
            }
        }
        return stats;
    }

    pub fn diagnosticsLocked(self: *const ProvisionedTableWriteCache) Diagnostics {
        var stats = Diagnostics{
            .cached_entries = @intCast(self.entries.items.len),
            .retired_entries = @intCast(self.retired_entries.items.len),
            .table_metadata_entries = @intCast(self.table_metadata.items.len),
            .active_bulk_sessions = @intCast(self.active_bulk_ingest_sessions.items.len),
        };
        for (self.entries.items) |entry| {
            stats.active_leases += entry.active_leases;
            if (entry.bulk_ingest_session_open) stats.bulk_ingest_open_entries += 1;
            if (entry.auto_bulk_ingest_session_open) stats.auto_bulk_ingest_open_entries += 1;
            if (entry.auto_bulk_ingest_finish_requested) stats.auto_bulk_ingest_finish_requested_entries += 1;
            accumulateDiagnosticsLsmStats(&stats, entry.db.trySnapshotLsmMaintenanceStats());
        }
        for (self.retired_entries.items) |entry| {
            stats.retired_active_leases += entry.active_leases;
            if (entry.bulk_ingest_session_open) stats.bulk_ingest_open_entries += 1;
            if (entry.auto_bulk_ingest_session_open) stats.auto_bulk_ingest_open_entries += 1;
            if (entry.auto_bulk_ingest_finish_requested) stats.auto_bulk_ingest_finish_requested_entries += 1;
            accumulateDiagnosticsLsmStats(&stats, entry.db.trySnapshotLsmMaintenanceStats());
        }
        return stats;
    }

    fn accumulateDiagnosticsLsmStats(stats: *Diagnostics, maintenance: ?lsm_backend.Backend.MaintenanceStats) void {
        const value = maintenance orelse return;
        stats.lsm_mutable_bytes +|= value.mutable_bytes;
        stats.lsm_immutable_bytes +|= value.immutable_bytes;
        stats.lsm_total_run_bytes +|= value.total_run_bytes;
        stats.lsm_wal_retained_bytes +|= value.wal_retained_bytes;
        stats.lsm_wal_retained_segments +|= value.wal_retained_segments;
        stats.lsm_active_readers +|= value.active_readers;
        stats.lsm_obsolete_paths +|= value.obsolete_paths;
        stats.lsm_bulk_ingest_current_scan_clone_active_bytes +|= value.bulk_ingest_current_scan_clone_active_bytes;
    }

    pub fn autoBulkIngestMaxIdleNs() u64 {
        return auto_bulk_ingest_max_idle_ns;
    }

    pub fn getOrOpenLocked(
        self: *ProvisionedTableWriteCache,
        path: []const u8,
        catalog: table_catalog.CatalogSource,
        group_id: u64,
        lsm_root_generation: u64,
        table_name: []const u8,
    ) !CachedDb {
        return try self.getOrOpenLockedMode(path, catalog, group_id, lsm_root_generation, table_name, .default);
    }

    pub fn getOrOpenLockedMode(
        self: *ProvisionedTableWriteCache,
        path: []const u8,
        catalog: table_catalog.CatalogSource,
        group_id: u64,
        lsm_root_generation: u64,
        table_name: []const u8,
        mode: ManagedDbOpenMode,
    ) !CachedDb {
        const OpenedDb = struct {
            db: db_mod.DB,
            start_bulk_session: bool,
        };

        const openDbForMode = struct {
            fn run(
                allocator: std.mem.Allocator,
                db_path: []const u8,
                indexes_json: ?[]const u8,
                cache: ?*lsm_backend.Cache,
                vector_cache: ?*hbc_mod.Cache,
                root_generation: u64,
                manager: ?*resource_manager_mod.ResourceManager,
                open_mode: ManagedDbOpenMode,
                runtime: ?*db_mod.background_runtime.BackendRuntime,
                antfly_provider: ?managed_embedder.AntflyProvider,
                secret_store: ?*common_secrets.FileStore,
                identity_namespace: ?doc_identity.Namespace,
                ha_write_gate: ?db_mod.HAWriteGate,
                ha_async_mirror: ?db_mod.HAAsyncEffectMirror,
            ) !OpenedDb {
                const effective_ha_mirror = haMirrorForManagedDbOpenMode(open_mode, ha_async_mirror);
                var db = if (indexes_json) |managed_indexes_json|
                    try openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntflyAndIdentityWithOptions(
                        allocator,
                        db_path,
                        managed_indexes_json,
                        cache,
                        vector_cache,
                        root_generation,
                        manager,
                        open_mode,
                        runtime,
                        antfly_provider,
                        secret_store,
                        null,
                        identity_namespace,
                        .{
                            .drain_resolver_backfill = false,
                            .ha_write_gate = ha_write_gate,
                            .ha_async_effect_mirror = effective_ha_mirror,
                            .ha_async_batch_mirror = effective_ha_mirror,
                            .ha_async_metadata_mirror = effective_ha_mirror,
                        },
                    )
                else
                    try db_mod.DB.open(allocator, db_path, .{
                        .lsm_cache = cache,
                        .hbc_cache = vector_cache,
                        .lsm_root_generation = root_generation,
                        .resource_manager = manager,
                        .backend_runtime = runtime,
                        .identity_namespace = identity_namespace,
                        .prefer_existing_identity_namespace = identity_namespace != null,
                        .ha_write_gate = ha_write_gate,
                        .ha_async_effect_mirror = effective_ha_mirror,
                        .ha_async_batch_mirror = effective_ha_mirror,
                        .ha_async_metadata_mirror = effective_ha_mirror,
                        .open_mode = switch (open_mode) {
                            .default => .writer,
                            .default_async, .writer_no_replay => .writer_no_replay,
                            .startup_catch_up, .restore_repair => .writer_no_replay,
                            .query_readonly => .query_readonly,
                            .status_only => .status_only,
                        },
                        .start_optional_runtimes = open_mode != .startup_catch_up,
                        .index_open_parallelism = if (open_mode == .default_async or open_mode == .writer_no_replay) 1 else null,
                    });
                errdefer db.close();
                try validateProvisionedDbIdentityNamespaceExpected(identity_namespace, &db);
                return .{
                    .db = db,
                    .start_bulk_session = switch (open_mode) {
                        .default, .default_async, .writer_no_replay => true,
                        .startup_catch_up, .restore_repair, .query_readonly, .status_only => false,
                    },
                };
            }
        }.run;

        const metadata = try self.getOrLoadMetadataLocked(catalog, table_name);
        const identity_namespace = try loadTableIdentityNamespaceForGroup(self.alloc, catalog, table_name, group_id);
        try self.pruneStaleEntriesForGroupTableLocked(group_id, lsm_root_generation, table_name);
        if (mode == .status_only) {
            const opened = try openDbForMode(
                self.alloc,
                path,
                metadata.indexes_json,
                self.lsm_cache,
                self.hbc_cache,
                lsm_root_generation,
                self.resource_manager,
                mode,
                self.backend_runtime,
                self.antfly_provider,
                self.secret_store,
                identity_namespace,
                self.ha_write_gate,
                self.ha_async_mirror,
            );
            const owned_db = try self.alloc.create(db_mod.DB);
            errdefer self.alloc.destroy(owned_db);
            owned_db.* = opened.db;
            return .{
                .cache = self,
                .db = owned_db,
                .schema_json = metadata.schema_json,
                .owned_db = owned_db,
            };
        }
        for (self.entries.items) |entry| {
            if (entry.group_id == group_id and entry.lsm_root_generation == lsm_root_generation and std.mem.eql(u8, entry.table_name, table_name)) {
                _ = self.hit_count.fetchAdd(1, .monotonic);
                lockAtomic(&self.entry_lifecycle_mutex);
                defer self.entry_lifecycle_mutex.unlock();
                entry.active_leases += 1;
                return .{
                    .cache = self,
                    .entry = entry,
                    .db = &entry.db,
                    .schema_json = entry.schema_json,
                };
            }
        }

        _ = self.miss_count.fetchAdd(1, .monotonic);
        const owned_table_name = try self.alloc.dupe(u8, table_name);
        errdefer self.alloc.free(owned_table_name);
        var opened = try openDbForMode(
            self.alloc,
            path,
            metadata.indexes_json,
            self.lsm_cache,
            self.hbc_cache,
            lsm_root_generation,
            self.resource_manager,
            mode,
            self.backend_runtime,
            self.antfly_provider,
            self.secret_store,
            identity_namespace,
            self.ha_write_gate,
            self.ha_async_mirror,
        );
        errdefer opened.db.close();
        const start_bulk_session = opened.start_bulk_session and self.bulkIngestSessionActiveForTable(table_name);
        if (start_bulk_session) {
            try opened.db.beginBulkIngestSession();
            errdefer opened.db.abortBulkIngestSession();
        }
        try self.retired_entries.ensureUnusedCapacity(self.alloc, 1);
        const owned_entry = try self.alloc.create(Entry);
        errdefer self.alloc.destroy(owned_entry);
        owned_entry.* = .{
            .group_id = group_id,
            .lsm_root_generation = lsm_root_generation,
            .table_name = owned_table_name,
            .db = opened.db,
            .schema_json = if (metadata.schema_json) |value| try self.alloc.dupe(u8, value) else null,
            .active_leases = 1,
            .bulk_ingest_session_open = start_bulk_session,
        };
        self.applyRuntimeHooksToDb(&owned_entry.db, group_id, &owned_entry.promotion_owner_state);
        try self.entries.append(self.alloc, owned_entry);
        var cached = CachedDb{
            .cache = self,
            .entry = owned_entry,
            .db = &owned_entry.db,
            .schema_json = metadata.schema_json,
        };
        errdefer self.retireFailedOpenLocked(&cached);
        try owned_entry.db.drainResolverBackfill();
        return cached;
    }

    pub fn adoptSeededEntryGenerationLocked(self: *ProvisionedTableWriteCache, entry: *Entry, lsm_root_generation: u64) bool {
        _ = self;
        if (!entry.allow_generation_adoption) return false;
        if (entry.active_leases != 0) return false;
        if (entry.bulk_ingest_session_open) return false;
        entry.lsm_root_generation = lsm_root_generation;
        entry.allow_generation_adoption = false;
        return true;
    }

    pub fn getOrPrepareOpenLocked(
        self: *ProvisionedTableWriteCache,
        group_id: u64,
        lsm_root_generation: u64,
        table_name: []const u8,
    ) !GetOrPrepareOpen {
        try self.pruneStaleEntriesForGroupTableLocked(group_id, lsm_root_generation, table_name);
        for (self.entries.items) |entry| {
            if (entry.group_id == group_id and entry.lsm_root_generation == lsm_root_generation and std.mem.eql(u8, entry.table_name, table_name)) {
                _ = self.hit_count.fetchAdd(1, .monotonic);
                lockAtomic(&self.entry_lifecycle_mutex);
                defer self.entry_lifecycle_mutex.unlock();
                entry.active_leases += 1;
                return .{
                    .cached = .{
                        .cache = self,
                        .entry = entry,
                        .db = &entry.db,
                        .schema_json = entry.schema_json,
                    },
                };
            }
        }
        for (self.entries.items) |entry| {
            if (entry.group_id != group_id) continue;
            if (!std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (!self.adoptSeededEntryGenerationLocked(entry, lsm_root_generation)) continue;
            _ = self.hit_count.fetchAdd(1, .monotonic);
            lockAtomic(&self.entry_lifecycle_mutex);
            defer self.entry_lifecycle_mutex.unlock();
            entry.active_leases += 1;
            return .{
                .cached = .{
                    .cache = self,
                    .entry = entry,
                    .db = &entry.db,
                    .schema_json = entry.schema_json,
                },
            };
        }

        _ = self.miss_count.fetchAdd(1, .monotonic);
        return .{ .prepared = .{} };
    }

    pub fn snapshotLeaseLocked(
        self: *ProvisionedTableWriteCache,
        group_id: u64,
        lsm_root_generation: u64,
        table_name: []const u8,
    ) ?CachedDb {
        for (self.entries.items) |entry| {
            if (entry.group_id != group_id) continue;
            if (entry.lsm_root_generation != lsm_root_generation) continue;
            if (!std.mem.eql(u8, entry.table_name, table_name)) continue;
            return self.leaseEntryLocked(entry);
        }
        return null;
    }

    pub fn snapshotLeaseOrAdoptSeededLocked(
        self: *ProvisionedTableWriteCache,
        group_id: u64,
        lsm_root_generation: u64,
        table_name: []const u8,
    ) ?CachedDb {
        if (self.snapshotLeaseLocked(group_id, lsm_root_generation, table_name)) |cached| return cached;
        for (self.entries.items) |entry| {
            if (entry.group_id != group_id) continue;
            if (!std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (!self.adoptSeededEntryGenerationLocked(entry, lsm_root_generation)) continue;
            return self.leaseEntryLocked(entry);
        }
        return null;
    }

    fn leaseEntryLocked(self: *ProvisionedTableWriteCache, entry: *Entry) CachedDb {
        lockAtomic(&self.entry_lifecycle_mutex);
        defer self.entry_lifecycle_mutex.unlock();
        entry.active_leases += 1;
        return .{
            .cache = self,
            .entry = entry,
            .db = &entry.db,
            .schema_json = entry.schema_json,
        };
    }

    pub fn adoptPreparedOpenLocked(
        self: *ProvisionedTableWriteCache,
        opened: *?db_mod.DB,
        group_id: u64,
        lsm_root_generation: u64,
        table_name: []const u8,
        mode: ManagedDbOpenMode,
        prepared: *PreparedOpen,
    ) !CachedDb {
        try self.pruneStaleEntriesForGroupTableLocked(group_id, lsm_root_generation, table_name);
        for (self.entries.items) |entry| {
            if (entry.group_id == group_id and entry.lsm_root_generation == lsm_root_generation and std.mem.eql(u8, entry.table_name, table_name)) {
                if (opened.*) |*db| db.close();
                opened.* = null;
                _ = self.hit_count.fetchAdd(1, .monotonic);
                lockAtomic(&self.entry_lifecycle_mutex);
                defer self.entry_lifecycle_mutex.unlock();
                entry.active_leases += 1;
                return .{
                    .cache = self,
                    .entry = entry,
                    .db = &entry.db,
                    .schema_json = entry.schema_json,
                };
            }
        }

        var db = opened.* orelse unreachable;
        opened.* = null;
        errdefer db.close();

        const start_bulk_session = switch (mode) {
            .default, .default_async, .writer_no_replay => self.bulkIngestSessionActiveForTable(table_name),
            .startup_catch_up, .restore_repair, .query_readonly, .status_only => false,
        };
        if (start_bulk_session) {
            try db.beginBulkIngestSession();
            errdefer db.abortBulkIngestSession();
        }

        const owned_table_name = try self.alloc.dupe(u8, table_name);
        errdefer self.alloc.free(owned_table_name);
        try self.retired_entries.ensureUnusedCapacity(self.alloc, 1);
        const owned_entry = try self.alloc.create(Entry);
        errdefer self.alloc.destroy(owned_entry);
        owned_entry.* = .{
            .group_id = group_id,
            .lsm_root_generation = lsm_root_generation,
            .table_name = owned_table_name,
            .db = db,
            .schema_json = prepared.schema_json,
            .active_leases = 1,
            .bulk_ingest_session_open = start_bulk_session,
        };
        self.applyRuntimeHooksToDb(&owned_entry.db, group_id, &owned_entry.promotion_owner_state);
        prepared.schema_json = null;
        errdefer owned_entry.deinit(self.alloc);
        try self.entries.append(self.alloc, owned_entry);
        opened.* = null;
        return .{
            .cache = self,
            .entry = owned_entry,
            .db = &owned_entry.db,
            .schema_json = owned_entry.schema_json,
        };
    }

    pub fn seedCreatedDbLocked(
        self: *ProvisionedTableWriteCache,
        opened: *?db_mod.DB,
        group_id: u64,
        lsm_root_generation: u64,
        table_name: []const u8,
        indexes_json: []const u8,
        schema_json: []const u8,
    ) !void {
        try self.pruneStaleEntriesForGroupTableLocked(group_id, lsm_root_generation, table_name);
        for (self.entries.items) |entry| {
            if (entry.group_id != group_id) continue;
            if (entry.lsm_root_generation != lsm_root_generation) continue;
            if (!std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (opened.*) |*db| db.close();
            opened.* = null;
            try self.replaceTableMetadataLocked(table_name, indexes_json, schema_json);
            return;
        }

        var db = opened.* orelse unreachable;
        opened.* = null;
        errdefer db.close();

        const owned_table_name = try self.alloc.dupe(u8, table_name);
        errdefer self.alloc.free(owned_table_name);
        const owned_schema_json = try self.alloc.dupe(u8, schema_json);
        errdefer self.alloc.free(owned_schema_json);
        try self.retired_entries.ensureUnusedCapacity(self.alloc, 1);
        const owned_entry = try self.alloc.create(Entry);
        errdefer self.alloc.destroy(owned_entry);
        owned_entry.* = .{
            .group_id = group_id,
            .lsm_root_generation = lsm_root_generation,
            .table_name = owned_table_name,
            .db = db,
            .schema_json = owned_schema_json,
            .active_leases = 0,
            .allow_generation_adoption = true,
        };
        self.applyRuntimeHooksToDb(&owned_entry.db, group_id, &owned_entry.promotion_owner_state);
        errdefer owned_entry.deinit(self.alloc);

        try self.replaceTableMetadataLocked(table_name, indexes_json, schema_json);
        try self.entries.append(self.alloc, owned_entry);
    }

    pub fn getLocked(
        self: *ProvisionedTableWriteCache,
        group_id: u64,
        lsm_root_generation: u64,
        table_name: []const u8,
    ) ?*db_mod.DB {
        for (self.entries.items) |entry| {
            if (entry.group_id == group_id and entry.lsm_root_generation == lsm_root_generation and std.mem.eql(u8, entry.table_name, table_name)) return &entry.db;
        }
        return null;
    }

    pub fn snapshotRuntimeStatusesLocked(
        self: *ProvisionedTableWriteCache,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        var count: usize = 0;
        for (self.entries.items) |entry| {
            if (std.mem.eql(u8, entry.table_name, table_name)) count += 1;
        }
        if (count == 0) return null;

        const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, count);
        var initialized: usize = 0;
        errdefer {
            for (items[0..initialized]) |*item| item.deinit(alloc);
            alloc.free(items);
        }

        for (self.entries.items) |entry| {
            if (!std.mem.eql(u8, entry.table_name, table_name)) continue;
            items[initialized] = .{
                .group_id = entry.group_id,
                .stats = try entry.db.runtimeStatusStatsConsistent(alloc),
            };
            initialized += 1;
        }
        return .{ .items = items };
    }

    fn pruneStaleEntriesForGroupTableLocked(
        self: *ProvisionedTableWriteCache,
        group_id: u64,
        lsm_root_generation: u64,
        table_name: []const u8,
    ) !void {
        var leased_retirements: usize = 0;
        for (self.entries.items) |entry| {
            if (entry.group_id != group_id or !std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (entry.lsm_root_generation == lsm_root_generation) continue;
            if (entry.active_leases > 0) leased_retirements += 1;
        }
        try self.retired_entries.ensureUnusedCapacity(self.alloc, leased_retirements);

        var i: usize = 0;
        while (i < self.entries.items.len) {
            const entry = self.entries.items[i];
            if (entry.group_id != group_id or !std.mem.eql(u8, entry.table_name, table_name)) {
                i += 1;
                continue;
            }
            if (entry.lsm_root_generation == lsm_root_generation) {
                i += 1;
                continue;
            }
            if (self.adoptSeededEntryGenerationLocked(entry, lsm_root_generation)) {
                i += 1;
                continue;
            }
            _ = self.entries.orderedRemove(i);
            self.retireEntryLocked(entry);
        }
    }

    pub fn invalidateTable(self: *ProvisionedTableWriteCache, table_name: []const u8) void {
        self.removeDbEntriesForTable(table_name);

        var i: usize = 0;
        while (i < self.table_metadata.items.len) {
            if (!std.mem.eql(u8, self.table_metadata.items[i].table_name, table_name)) {
                i += 1;
                continue;
            }
            var removed = self.table_metadata.orderedRemove(i);
            removed.deinit(self.alloc);
        }
    }

    fn retireDbEntriesForTableLocked(self: *ProvisionedTableWriteCache, table_name: []const u8) !void {
        var leased_retirements: usize = 0;
        for (self.entries.items) |entry| {
            if (!std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (entry.active_leases > 0) leased_retirements += 1;
        }
        try self.retired_entries.ensureUnusedCapacity(self.alloc, leased_retirements);

        var i: usize = 0;
        while (i < self.entries.items.len) {
            if (!std.mem.eql(u8, self.entries.items[i].table_name, table_name)) {
                i += 1;
                continue;
            }
            const removed = self.entries.orderedRemove(i);
            self.retireEntryLocked(removed);
        }
    }

    pub fn removeDbEntriesForTable(self: *ProvisionedTableWriteCache, table_name: []const u8) void {
        self.retireDbEntriesForTableLocked(table_name) catch return;
    }

    fn hasLiveEntryForGroupTableLocked(
        self: *const ProvisionedTableWriteCache,
        group_id: u64,
        table_name: []const u8,
    ) bool {
        for (self.entries.items) |entry| {
            if (entry.group_id != group_id) continue;
            if (!std.mem.eql(u8, entry.table_name, table_name)) continue;
            return true;
        }
        return false;
    }

    pub fn hasLiveEntryForTableLocked(
        self: *const ProvisionedTableWriteCache,
        table_name: []const u8,
    ) bool {
        for (self.entries.items) |entry| {
            if (!std.mem.eql(u8, entry.table_name, table_name)) continue;
            return true;
        }
        return false;
    }

    pub fn hasForegroundStateForGroupTableLocked(
        self: *const ProvisionedTableWriteCache,
        group_id: u64,
        table_name: []const u8,
    ) bool {
        if (self.bulkIngestSessionActiveForTable(table_name)) return true;
        return self.hasLiveEntryForGroupTableLocked(group_id, table_name);
    }

    pub fn beginBulkIngestLocked(self: *ProvisionedTableWriteCache, table_name: []const u8) !void {
        for (self.active_bulk_ingest_sessions.items) |*session| {
            if (!std.mem.eql(u8, session.table_name, table_name)) continue;
            session.depth += 1;
            return;
        }

        const owned_table_name = try self.alloc.dupe(u8, table_name);
        errdefer self.alloc.free(owned_table_name);
        try self.active_bulk_ingest_sessions.ensureUnusedCapacity(self.alloc, 1);
        var started_any = false;
        errdefer if (started_any) {
            for (self.entries.items) |entry| {
                if (!std.mem.eql(u8, entry.table_name, table_name) or !entry.bulk_ingest_session_open) continue;
                entry.db.abortBulkIngestSession();
                entry.bulk_ingest_session_open = false;
            }
        };
        for (self.entries.items) |entry| {
            if (!std.mem.eql(u8, entry.table_name, table_name) or entry.bulk_ingest_session_open) continue;
            try entry.db.beginBulkIngestSession();
            entry.bulk_ingest_session_open = true;
            entry.auto_bulk_ingest_session_open = false;
            entry.auto_bulk_ingest_ops = 0;
            entry.auto_bulk_ingest_started_ns = 0;
            entry.auto_bulk_ingest_last_ns = 0;
            entry.auto_bulk_ingest_finish_requested = false;
            started_any = true;
        }
        self.active_bulk_ingest_sessions.appendAssumeCapacity(.{ .table_name = owned_table_name });
    }

    pub fn finishBulkIngestLocked(
        self: *ProvisionedTableWriteCache,
        table_name: []const u8,
        options: backend_types.BulkIngestFinishOptions,
    ) !void {
        const idx = self.findActiveBulkIngestSession(table_name) orelse return;
        if (self.active_bulk_ingest_sessions.items[idx].depth > 1) {
            self.active_bulk_ingest_sessions.items[idx].depth -= 1;
            return;
        }

        for (self.entries.items) |entry| {
            if (!std.mem.eql(u8, entry.table_name, table_name) or !entry.bulk_ingest_session_open) continue;
            if (entry.auto_bulk_ingest_session_open) {
                try entry.db.finishDenseAutoBulkIngestSessionWithOptions(options);
            } else {
                try entry.db.finishBulkIngestSessionWithOptions(options);
            }
            entry.bulk_ingest_session_open = false;
            entry.auto_bulk_ingest_session_open = false;
            entry.auto_bulk_ingest_ops = 0;
            entry.auto_bulk_ingest_started_ns = 0;
            entry.auto_bulk_ingest_last_ns = 0;
            entry.auto_bulk_ingest_finish_requested = false;
        }
        var removed = self.active_bulk_ingest_sessions.orderedRemove(idx);
        removed.deinit(self.alloc);
    }

    pub fn abortBulkIngestLocked(self: *ProvisionedTableWriteCache, table_name: []const u8) void {
        const idx = self.findActiveBulkIngestSession(table_name) orelse return;
        for (self.entries.items) |entry| {
            if (!std.mem.eql(u8, entry.table_name, table_name) or !entry.bulk_ingest_session_open) continue;
            if (entry.auto_bulk_ingest_session_open) {
                entry.db.abortDenseAutoBulkIngestSession();
            } else {
                entry.db.abortBulkIngestSession();
            }
            entry.bulk_ingest_session_open = false;
            entry.auto_bulk_ingest_session_open = false;
            entry.auto_bulk_ingest_ops = 0;
            entry.auto_bulk_ingest_started_ns = 0;
            entry.auto_bulk_ingest_last_ns = 0;
            entry.auto_bulk_ingest_finish_requested = false;
        }
        var removed = self.active_bulk_ingest_sessions.orderedRemove(idx);
        removed.deinit(self.alloc);
    }

    pub fn ensureAutoBulkIngestLocked(self: *ProvisionedTableWriteCache, group_id: u64, table_name: []const u8, now_ns: u64) !void {
        if (self.bulkIngestSessionActiveForTable(table_name)) return;
        for (self.entries.items) |entry| {
            if (entry.group_id != group_id or !std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (!entry.bulk_ingest_session_open) {
                try entry.db.beginDenseAutoBulkIngestSession();
                entry.bulk_ingest_session_open = true;
            }
            entry.auto_bulk_ingest_session_open = true;
            if (entry.auto_bulk_ingest_started_ns == 0) entry.auto_bulk_ingest_started_ns = now_ns;
            entry.auto_bulk_ingest_last_ns = now_ns;
            return;
        }
    }

    pub fn recordAutoBulkIngestOpsLocked(self: *ProvisionedTableWriteCache, group_id: u64, table_name: []const u8, ops: usize, now_ns: u64) !void {
        if (ops == 0) return;
        var should_finish = false;
        for (self.entries.items) |entry| {
            if (entry.group_id != group_id or !std.mem.eql(u8, entry.table_name, table_name) or !entry.auto_bulk_ingest_session_open) continue;
            entry.auto_bulk_ingest_ops +|= ops;
            entry.auto_bulk_ingest_last_ns = now_ns;
            should_finish = should_finish or entry.auto_bulk_ingest_ops >= auto_bulk_ingest_max_window_ops;
            if (should_finish) entry.auto_bulk_ingest_finish_requested = true;
        }
    }

    pub fn rollRequestedAutoBulkIngestLocked(self: *ProvisionedTableWriteCache, group_id: u64, table_name: []const u8, now_ns: u64) !bool {
        for (self.entries.items) |entry| {
            if (entry.group_id != group_id or !std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (!entry.auto_bulk_ingest_session_open or !entry.auto_bulk_ingest_finish_requested) return false;
            if (entry.active_leases > 1) return false;
            try entry.db.rollDenseAutoBulkIngestSessionWithOptions(auto_bulk_ingest_finish_options);
            entry.auto_bulk_ingest_session_open = true;
            entry.auto_bulk_ingest_ops = 0;
            entry.auto_bulk_ingest_started_ns = now_ns;
            entry.auto_bulk_ingest_last_ns = now_ns;
            entry.auto_bulk_ingest_finish_requested = false;
            return true;
        }
        return false;
    }

    pub fn finishAutoBulkIngestLocked(self: *ProvisionedTableWriteCache, group_id: u64, table_name: []const u8) !void {
        for (self.entries.items) |entry| {
            if (entry.group_id != group_id or !std.mem.eql(u8, entry.table_name, table_name) or !entry.auto_bulk_ingest_session_open) continue;
            try entry.db.finishDenseAutoBulkIngestSessionWithOptions(auto_bulk_ingest_finish_options);
            entry.bulk_ingest_session_open = false;
            entry.auto_bulk_ingest_session_open = false;
            entry.auto_bulk_ingest_ops = 0;
            entry.auto_bulk_ingest_started_ns = 0;
            entry.auto_bulk_ingest_last_ns = 0;
            entry.auto_bulk_ingest_finish_requested = false;
        }
        self.removeInactiveBulkIngestSessionLocked(table_name);
    }

    pub fn finishExpiredAutoBulkIngestLocked(self: *ProvisionedTableWriteCache, now_ns: u64) !bool {
        return self.finishExpiredAutoBulkIngestLockedWithStatusLeases(now_ns, std.heap.page_allocator, null);
    }

    pub fn finishExpiredAutoBulkIngestLockedWithStatusLeases(
        self: *ProvisionedTableWriteCache,
        now_ns: u64,
        lease_alloc: std.mem.Allocator,
        finished_leases: ?*std.ArrayListUnmanaged(CachedDb),
    ) !bool {
        var first_err: ?anyerror = null;
        var finished_any = false;
        for (self.entries.items) |entry| {
            if (!entry.auto_bulk_ingest_session_open) continue;
            if (entry.active_leases != 0) continue;
            const idle_expired = entry.auto_bulk_ingest_last_ns > 0 and now_ns -| entry.auto_bulk_ingest_last_ns >= auto_bulk_ingest_max_idle_ns;
            if (!entry.auto_bulk_ingest_finish_requested and !idle_expired) continue;
            if (!idle_expired and entry.auto_bulk_ingest_finish_requested) {
                continue;
            } else {
                entry.db.finishDenseAutoBulkIngestSessionWithOptions(auto_bulk_ingest_finish_options) catch |err| {
                    if (first_err == null) first_err = err;
                    continue;
                };
                entry.bulk_ingest_session_open = false;
                entry.auto_bulk_ingest_session_open = false;
                entry.auto_bulk_ingest_ops = 0;
                entry.auto_bulk_ingest_started_ns = 0;
                entry.auto_bulk_ingest_last_ns = 0;
                entry.auto_bulk_ingest_finish_requested = false;
                self.removeInactiveBulkIngestSessionLocked(entry.table_name);
            }
            if (finished_leases) |leases| {
                self.appendRuntimeStatusLeaseForEntryLocked(lease_alloc, entry, leases) catch |err| {
                    if (first_err == null) first_err = err;
                    continue;
                };
            }
            finished_any = true;
        }
        if (first_err) |err| return err;
        return finished_any;
    }

    pub fn appendRuntimeStatusLeaseForEntryLocked(
        self: *ProvisionedTableWriteCache,
        alloc: std.mem.Allocator,
        entry: *Entry,
        out: *std.ArrayListUnmanaged(CachedDb),
    ) !void {
        lockAtomic(&self.entry_lifecycle_mutex);
        entry.active_leases += 1;
        self.entry_lifecycle_mutex.unlock();
        errdefer self.releaseEntry(entry);

        try out.append(alloc, .{
            .cache = self,
            .entry = entry,
            .db = &entry.db,
            .schema_json = entry.schema_json,
        });
    }

    fn leaseLsmMaintenanceEntryLocked(self: *ProvisionedTableWriteCache, comptime best_effort: bool) ?CachedDb {
        var best_entry: ?*Entry = null;
        var best_score: u64 = 0;
        for (self.entries.items) |entry| {
            if (entry.bulk_ingest_session_open) continue;
            if (entry.db.hasActiveDenseBulkWork()) continue;
            const score = if (best_effort) entry.db.lsmMaintenanceDebtHint() else entry.db.lsmMaintenanceScore();
            if (score > best_score) {
                best_score = score;
                best_entry = entry;
            }
        }
        if (best_score == 0) return null;
        return self.leaseEntryLocked(best_entry.?);
    }

    pub fn leaseLsmMaintenanceRoundLocked(self: *ProvisionedTableWriteCache) ?CachedDb {
        return self.leaseLsmMaintenanceEntryLocked(false);
    }

    pub fn leaseLsmMaintenanceRoundBestEffortLocked(self: *ProvisionedTableWriteCache) ?CachedDb {
        return self.leaseLsmMaintenanceEntryLocked(true);
    }

    pub fn maxLsmMaintenanceScoreLocked(self: *const ProvisionedTableWriteCache) u64 {
        var score: u64 = 0;
        for (self.entries.items) |entry| {
            if (entry.bulk_ingest_session_open) continue;
            if (entry.db.hasActiveDenseBulkWork()) continue;
            score = @max(score, entry.db.lsmMaintenanceDebtHint());
        }
        return score;
    }

    pub fn bulkIngestSessionActiveForTable(self: *const ProvisionedTableWriteCache, table_name: []const u8) bool {
        return self.findActiveBulkIngestSession(table_name) != null;
    }

    pub fn bulkIngestSessionOpenForTable(self: *const ProvisionedTableWriteCache, table_name: []const u8) bool {
        if (self.bulkIngestSessionActiveForTable(table_name)) return true;
        for (self.entries.items) |entry| {
            if (!std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (entry.bulk_ingest_session_open or entry.auto_bulk_ingest_session_open) return true;
        }
        return false;
    }

    pub fn removeInactiveBulkIngestSessionLocked(self: *ProvisionedTableWriteCache, table_name: []const u8) void {
        for (self.entries.items) |entry| {
            if (!std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (entry.bulk_ingest_session_open or entry.auto_bulk_ingest_session_open) return;
        }
        const idx = self.findActiveBulkIngestSession(table_name) orelse return;
        var removed = self.active_bulk_ingest_sessions.orderedRemove(idx);
        removed.deinit(self.alloc);
    }

    fn findActiveBulkIngestSession(self: *const ProvisionedTableWriteCache, table_name: []const u8) ?usize {
        for (self.active_bulk_ingest_sessions.items, 0..) |session, i| {
            if (std.mem.eql(u8, session.table_name, table_name)) return i;
        }
        return null;
    }

    fn evictOldestTable(self: *ProvisionedTableWriteCache) void {
        if (self.table_metadata.items.len == 0) return;
        const table_name = self.table_metadata.items[0].table_name;
        self.removeDbEntriesForTable(table_name);
        var removed = self.table_metadata.orderedRemove(0);
        removed.deinit(self.alloc);
    }

    fn reserveDbEntryRetirementsForTableLocked(self: *ProvisionedTableWriteCache, table_name: []const u8) !void {
        var leased_retirements: usize = 0;
        for (self.entries.items) |entry| {
            if (!std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (entry.active_leases > 0) leased_retirements += 1;
        }
        try self.retired_entries.ensureUnusedCapacity(self.alloc, leased_retirements);
    }

    pub fn replaceTableMetadataLocked(
        self: *ProvisionedTableWriteCache,
        table_name: []const u8,
        indexes_json: []const u8,
        schema_json: []const u8,
    ) !void {
        const replace_index = blk: {
            for (self.table_metadata.items, 0..) |metadata, i| {
                if (std.mem.eql(u8, metadata.table_name, table_name)) break :blk i;
            }
            break :blk null;
        };
        if (replace_index == null) try self.table_metadata.ensureUnusedCapacity(self.alloc, 1);
        var replacement = try self.cloneTableMetadataAlloc(table_name, indexes_json, schema_json);
        errdefer replacement.deinit(self.alloc);

        if (replace_index) |i| {
            self.table_metadata.items[i].deinit(self.alloc);
            self.table_metadata.items[i] = replacement;
            return;
        }

        if (self.table_metadata.items.len >= max_cached_write_tables) self.evictOldestTable();
        self.table_metadata.appendAssumeCapacity(replacement);
    }

    fn installLoadedTableMetadataLocked(
        self: *ProvisionedTableWriteCache,
        cached_index: ?usize,
        replacement: TableMetadata,
    ) !*const TableMetadata {
        if (cached_index) |i| {
            try self.retireDbEntriesForTableLocked(replacement.table_name);
            self.table_metadata.items[i].deinit(self.alloc);
            self.table_metadata.items[i] = replacement;
            return &self.table_metadata.items[i];
        }

        if (self.table_metadata.items.len >= max_cached_write_tables) self.evictOldestTable();
        self.table_metadata.appendAssumeCapacity(replacement);
        return &self.table_metadata.items[self.table_metadata.items.len - 1];
    }

    fn tableMetadataLocked(self: *const ProvisionedTableWriteCache, table_name: []const u8) ?TableMetadata {
        for (self.table_metadata.items) |metadata| {
            if (std.mem.eql(u8, metadata.table_name, table_name)) return metadata;
        }
        return null;
    }

    pub fn transferAdoptableEntriesForTableLocked(
        self: *ProvisionedTableWriteCache,
        dest: *ProvisionedTableWriteCache,
        table_name: []const u8,
        generation_source: ?VisibleRootGenerationSource,
    ) !usize {
        if (self.alloc.ptr != dest.alloc.ptr or self.alloc.vtable != dest.alloc.vtable) {
            return self.retireAdoptableEntriesForTableLocked(table_name);
        }

        const metadata = self.tableMetadataLocked(table_name);
        if (metadata) |value| {
            if (value.indexes_json) |indexes_json| {
                try dest.replaceTableMetadataLocked(table_name, indexes_json, value.schema_json orelse "");
            }
        }

        var moved: usize = 0;
        var i: usize = 0;
        while (i < self.entries.items.len) {
            const entry = self.entries.items[i];
            if (!std.mem.eql(u8, entry.table_name, table_name)) {
                i += 1;
                continue;
            }
            if (!entry.allow_generation_adoption or entry.active_leases != 0 or entry.bulk_ingest_session_open) {
                i += 1;
                continue;
            }

            const dest_generation = if (generation_source) |source| source.visibleRootGenerationForGroup(entry.group_id) else backend_current_root_generation;
            try dest.pruneStaleEntriesForGroupTableLocked(entry.group_id, dest_generation, table_name);
            var existing_index: ?usize = null;
            for (dest.entries.items, 0..) |dest_entry, dest_i| {
                if (dest_entry.group_id != entry.group_id) continue;
                if (!std.mem.eql(u8, dest_entry.table_name, table_name)) continue;
                existing_index = dest_i;
                break;
            }
            if (existing_index) |dest_i| {
                if (dest.entries.items[dest_i].active_leases > 0) {
                    try dest.retired_entries.ensureUnusedCapacity(dest.alloc, 1);
                }
                const removed = dest.entries.orderedRemove(dest_i);
                dest.retireEntryLocked(removed);
            }

            _ = self.entries.orderedRemove(i);
            entry.lsm_root_generation = dest_generation;
            entry.allow_generation_adoption = false;
            try dest.entries.append(dest.alloc, entry);
            moved += 1;
        }
        return moved;
    }

    fn retireAdoptableEntriesForTableLocked(
        self: *ProvisionedTableWriteCache,
        table_name: []const u8,
    ) usize {
        var retired: usize = 0;
        var i: usize = 0;
        while (i < self.entries.items.len) {
            const entry = self.entries.items[i];
            if (!std.mem.eql(u8, entry.table_name, table_name) or
                !entry.allow_generation_adoption or
                entry.active_leases != 0 or
                entry.bulk_ingest_session_open)
            {
                i += 1;
                continue;
            }

            const removed = self.entries.orderedRemove(i);
            self.retireEntryLocked(removed);
            retired += 1;
        }
        return retired;
    }

    fn getOrLoadMetadataLocked(
        self: *ProvisionedTableWriteCache,
        catalog: table_catalog.CatalogSource,
        table_name: []const u8,
    ) !*const TableMetadata {
        var snapshot = try catalog.adminSnapshot();
        defer catalog.freeAdminSnapshot(&snapshot);
        const table = tables_api.findTableByName(&snapshot, table_name);

        var cached_index: ?usize = null;
        for (self.table_metadata.items, 0..) |*metadata, i| {
            if (std.mem.eql(u8, metadata.table_name, table_name)) {
                cached_index = i;
                if (table) |current| {
                    const indexes_match = if (metadata.indexes_json) |cached|
                        std.mem.eql(u8, cached, current.indexes_json)
                    else
                        current.indexes_json.len == 0;
                    const schema_match = if (metadata.schema_json) |cached|
                        std.mem.eql(u8, cached, current.schema_json)
                    else
                        current.schema_json.len == 0;
                    if (indexes_match and schema_match) return metadata;
                } else if (metadata.indexes_json == null and metadata.schema_json == null) {
                    return metadata;
                }
                break;
            }
        }

        if (cached_index != null) {
            try self.reserveDbEntryRetirementsForTableLocked(table_name);
        } else {
            try self.table_metadata.ensureUnusedCapacity(self.alloc, 1);
        }

        const indexes_json = if (table) |current| current.indexes_json else null;
        const schema_json = if (table) |current| current.schema_json else null;
        var replacement = try self.cloneTableMetadataAlloc(table_name, indexes_json, schema_json);
        errdefer replacement.deinit(self.alloc);

        const installed = try self.installLoadedTableMetadataLocked(cached_index, replacement);
        replacement = undefined;
        return installed;
    }

    fn releaseEntry(self: *ProvisionedTableWriteCache, entry: *Entry) void {
        lockAtomic(&self.entry_lifecycle_mutex);
        defer self.entry_lifecycle_mutex.unlock();
        std.debug.assert(entry.active_leases > 0);
        entry.active_leases -= 1;
        if (entry.active_leases == 0 and entry.retired) {
            self.destroyRetiredEntryLocked(entry);
        }
    }

    pub fn retireEntryLocked(self: *ProvisionedTableWriteCache, entry: *Entry) void {
        lockAtomic(&self.entry_lifecycle_mutex);
        defer self.entry_lifecycle_mutex.unlock();
        if (entry.retired) return;
        entry.retired = true;
        if (entry.active_leases == 0) {
            entry.deinit(self.alloc);
            self.alloc.destroy(entry);
            return;
        }
        self.retired_entries.appendAssumeCapacity(entry);
    }

    fn destroyRetiredEntryLocked(self: *ProvisionedTableWriteCache, entry: *Entry) void {
        var i: usize = 0;
        while (i < self.retired_entries.items.len) : (i += 1) {
            if (self.retired_entries.items[i] != entry) continue;
            _ = self.retired_entries.orderedRemove(i);
            entry.deinit(self.alloc);
            self.alloc.destroy(entry);
            return;
        }
        unreachable;
    }
};

const testing_empty_indexes_table_records = [_]metadata_table_manager.TableRecord{.{
    .table_id = 7,
    .name = "docs",
    .placement_role = "data",
    .indexes_json = "{\"indexes\":[]}",
}};

const testing_empty_indexes_range_records = [_]metadata_table_manager.RangeRecord{.{
    .group_id = 7001,
    .table_id = 7,
    .start_key = "",
    .end_key = null,
}};

fn testingEmptyIndexesCatalog() table_catalog.CatalogSource {
    const Catalog = struct {
        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast(testing_empty_indexes_table_records[0..]),
                .ranges = @constCast(testing_empty_indexes_range_records[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    return .{
        .ptr = undefined,
        .vtable = &.{
            .admin_snapshot = Catalog.adminSnapshot,
            .free_admin_snapshot = Catalog.freeAdminSnapshot,
        },
    };
}

test "write cache invalidation retires leased entry until release" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/write-cache-lease-retire", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const catalog = testingEmptyIndexesCatalog();
    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();

    var cached = try write_cache.getOrOpenLocked(path, catalog, 7001, 0, "docs");
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expectEqual(@as(usize, 0), write_cache.retired_entries.items.len);

    write_cache.removeDbEntriesForTable("docs");
    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items.len);
    try std.testing.expectEqual(@as(usize, 1), write_cache.retired_entries.items.len);

    cached.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), write_cache.retired_entries.items.len);
}

test "provisioned table write cache retires stale db when index metadata changes" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/write-cache-metadata-refresh", .{tmp.sub_path});
    defer alloc.free(path);

    const Catalog = struct {
        var indexes_json_buf: []const u8 = "";
        var table_records = [_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .placement_role = "data",
        }};
        var range_records = [_]metadata_table_manager.RangeRecord{
            .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
        };

        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            table_records[0].indexes_json = indexes_json_buf;
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = table_records[0..],
                .ranges = range_records[0..],
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    Catalog.indexes_json_buf = "{\"first_idx\":{\"type\":\"full_text\",\"field\":\"body\"}}";

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();

    var first = try write_cache.getOrOpenLocked(path, Catalog.iface(), 7001, 0, "docs");
    var first_released = false;
    defer if (!first_released) first.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expectEqual(@as(usize, 1), write_cache.table_metadata.items.len);
    try std.testing.expect(first.db.core.index_manager.textIndex("first_idx") != null);

    Catalog.indexes_json_buf = "{\"second_idx\":{\"type\":\"full_text\",\"field\":\"body\"}}";

    var second = try write_cache.getOrOpenLocked(path, Catalog.iface(), 7001, 0, "docs");
    defer second.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expectEqual(@as(usize, 1), write_cache.retired_entries.items.len);
    try std.testing.expectEqual(@as(usize, 1), write_cache.table_metadata.items.len);
    try std.testing.expect(first.entry.?.retired);
    try std.testing.expect(second.entry.? != first.entry.?);
    try std.testing.expect(second.db.core.index_manager.textIndex("second_idx") != null);
    try std.testing.expect(second.db.core.index_manager.textIndex("first_idx") == null);

    first.deinit(alloc);
    first_released = true;
    try std.testing.expectEqual(@as(usize, 0), write_cache.retired_entries.items.len);
}

test "managed status-only cache open skips shared bulk ingest session state" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/managed-status-only-bulk", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const catalog = testingEmptyIndexesCatalog();
    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();

    try write_cache.beginBulkIngestLocked("docs");
    try std.testing.expectEqual(@as(usize, 1), write_cache.active_bulk_ingest_sessions.items.len);
    var cached_seed = try write_cache.getOrOpenLocked(path, catalog, 7001, 0, "docs");
    defer cached_seed.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(write_cache.entries.items[0].bulk_ingest_session_open);

    var cached = try write_cache.getOrOpenLockedMode(path, catalog, 7001, 0, "docs", .status_only);
    defer cached.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(write_cache.entries.items[0].bulk_ingest_session_open);
    try std.testing.expectEqual(@as(usize, 1), write_cache.active_bulk_ingest_sessions.items.len);
}

test "full text memory attribution aggregation includes norm bytes" {
    var dst = db_mod.TextMemoryAttributionStats{
        .inverted_header_bytes = 3,
        .inverted_norm_bytes = 5,
        .inverted_term_dict_bytes = 7,
    };
    accumulateTextMemoryAttributionStats(&dst, .{
        .inverted_header_bytes = 11,
        .inverted_norm_bytes = 13,
        .inverted_term_dict_bytes = 17,
    });

    try std.testing.expectEqual(@as(u64, 14), dst.inverted_header_bytes);
    try std.testing.expectEqual(@as(u64, 18), dst.inverted_norm_bytes);
    try std.testing.expectEqual(@as(u64, 24), dst.inverted_term_dict_bytes);
}

test "write cache reserves retirement slots when pruning multiple leased generations" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/write-cache-retire-multiple-stale", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const catalog = testingEmptyIndexesCatalog();
    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();

    var gen0 = try write_cache.getOrOpenLocked(path, catalog, 7001, 0, "docs");
    var gen1 = try write_cache.getOrOpenLocked(path, catalog, 7001, 1, "docs");
    var gen2 = try write_cache.getOrOpenLocked(path, catalog, 7001, 2, "docs");

    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expectEqual(@as(usize, 2), write_cache.retired_entries.items.len);

    gen0.deinit(alloc);
    gen1.deinit(alloc);
    gen2.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), write_cache.retired_entries.items.len);
}

test "write cache keeps leased entry cleanup reachable when retirement bookkeeping allocation fails" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const alloc = failing.allocator();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/write-cache-retire-oom", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const catalog = testingEmptyIndexesCatalog();
    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();

    var cached = try write_cache.getOrOpenLocked(path, catalog, 7001, 0, "docs");
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expectEqual(@as(usize, 0), write_cache.retired_entries.items.len);

    failing.fail_index = failing.alloc_index;
    failing.resize_fail_index = failing.resize_index;
    write_cache.removeDbEntriesForTable("docs");

    failing.fail_index = std.math.maxInt(usize);
    failing.resize_fail_index = std.math.maxInt(usize);
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len + write_cache.retired_entries.items.len);

    cached.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items.len);
    try std.testing.expectEqual(@as(usize, 0), write_cache.retired_entries.items.len);
}
