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
const bloom = @import("bloom");
const backend_adapter = @import("backend_adapter.zig");
const backend_erased = @import("backend_erased.zig");
const backend_scan = @import("backend_scan.zig");
const backend_types = @import("backend_types.zig");
const lsm_manifest = @import("lsm/manifest.zig");
const lsm_table_file = @import("lsm/table_file.zig");
const state_mod = @import("lsm_backend/state.zig");
const repository_mod = @import("lsm_backend/repository.zig");
const runtime_mod = @import("lsm_backend/runtime.zig");
const compaction_mod = @import("lsm_backend/compaction.zig");
const compaction_scheduler_mod = @import("lsm_backend/compaction_scheduler.zig");
const background_runtime_mod = @import("background_runtime.zig");
const lsm_background_mod = @import("lsm_backend/background.zig");
const recovery_mod = @import("lsm_backend/recovery.zig");
const storage_io = @import("lsm_backend/storage_io.zig");
const cache_mod = @import("lsm_backend/cache.zig");
const wal_mod = @import("lsm_backend/wal.zig");
const internal_keys = @import("internal_keys.zig");
const resource_manager_mod = @import("resource_manager.zig");
const platform_time = @import("../platform/time.zig");

const State = state_mod.State;
const ActiveMemTable = state_mod.ActiveMemTable;
const SplitStates = state_mod.SplitStates;
const Run = repository_mod.Run;
const ObsoletePath = repository_mod.ObsoletePath;
const namespaceOf = state_mod.namespaceOf;
const compareNamespace = state_mod.compareNamespace;
const compareEntryTo = state_mod.compareEntryTo;
const CounterU64 = platform.atomic.Value(u64);

pub const MutableSnapshotReason = enum(u8) {
    bound_read_txn,
    namespace_read_txn,
    other,
};

pub const mutable_snapshot_reason_count = @typeInfo(MutableSnapshotReason).@"enum".fields.len;

pub const MutableSnapshotCloneReasonStats = struct {
    calls: u64 = 0,
    bytes_total: u64 = 0,
    peak_bytes: u64 = 0,
};

pub fn mutableSnapshotReasonName(reason: MutableSnapshotReason) []const u8 {
    return switch (reason) {
        .bound_read_txn => "bound_read_txn",
        .namespace_read_txn => "namespace_read_txn",
        .other => "other",
    };
}

fn mutableSnapshotReasonIndex(reason: MutableSnapshotReason) usize {
    return @intFromEnum(reason);
}

fn atomicMaxCounter(counter: *CounterU64, candidate: u64) void {
    var current = counter.load(.monotonic);
    while (candidate > current) {
        if (counter.cmpxchgWeak(current, candidate, .monotonic, .monotonic)) |observed| {
            current = observed;
        } else {
            return;
        }
    }
}

pub const Options = struct {
    backend: backend_types.OpenOptions = .{},
    flush_threshold: usize = 8,
    flush_threshold_bytes: u64 = 0,
    bulk_ingest_flush_threshold_multiplier: usize = 8,
    bulk_ingest_flush_threshold_bytes_multiplier: usize = 8,
    compact_threshold_runs: usize = 16,
    l0_overlap_compact_threshold_runs: usize = 4,
    l0_soft_limit_runs: usize = 0,
    l0_hard_limit_runs: usize = 0,
    l0_soft_limit_bytes: u64 = 0,
    l0_hard_limit_bytes: u64 = 0,
    foreground_soft_compaction: bool = false,
    defer_flush_on_commit: bool = false,
    max_deferred_immutable_memtables: usize = 8,
    direct_bulk_ingest: bool = true,
    level_target_runs_base: usize = 4,
    level_target_runs_multiplier: usize = 4,
    level_target_bytes_base: usize = 128 * 1024,
    level_target_bytes_multiplier: usize = 8,
    max_compaction_input_bytes: u64 = 0,
    max_run_file_bytes: usize = 512 * 1024 * 1024,
    bloom: bloom.Config = lsm_table_file.default_filter_config,
    table_block_compression: lsm_table_file.CompressionPolicy = .snappy_adaptive,
    io_runtime: storage_io.RuntimeKind = .threaded,
    storage: ?storage_io.Storage = null,
    cache: ?*cache_mod.Cache = null,
    local_block_cache_enabled: bool = true,
    resource_manager: ?*resource_manager_mod.ResourceManager = null,
    background_executor: ?*const BackgroundExecutor = null,
    compaction_scheduler: compaction_scheduler_mod.Options = .{},
    background_io_budget_bytes: u64 = 0,
    background_io_allow_oversized_single_job: bool = true,
    wal_enabled: bool = true,
    wal_sync_on_commit: bool = false,
    wal_segment_bytes: u64 = 64 * 1024 * 1024,
    wal_soft_limit_segments: u64 = 0,
    wal_hard_limit_segments: u64 = 0,
    wal_soft_limit_bytes: u64 = 0,
    wal_hard_limit_bytes: u64 = 0,
    root_generation: u64 = 0,
    obsolete_retention_ns: u64 = 5 * std.time.ns_per_min,
    read_snapshot_rotate_mutable_bytes: u64 = 256 * 1024,
};

pub const IoRuntime = storage_io.RuntimeKind;
pub const Storage = storage_io.Storage;
pub const HostStorage = storage_io.HostStorage;
pub const NativeStorageStats = storage_io.NativeStorageStats;
pub const Cache = cache_mod.Cache;
pub const CacheStats = cache_mod.Stats;
pub const CacheKindStats = cache_mod.KindStats;
pub const DefaultCacheSizeBytes = cache_mod.DefaultCacheSizeBytes;
pub const TableEntry = lsm_table_file.Entry;
pub const BackgroundExecutor = lsm_background_mod.Executor;
const max_local_cached_run_blocks: usize = 64;

pub const Backend = struct {
    pub const CompactionStats = struct {
        compactions: usize = 0,
        input_runs: usize = 0,
        input_bytes: u64 = 0,
        output_bytes: u64 = 0,
    };

    pub const WriteStats = struct {
        flushes: u64 = 0,
        flush_input_entries: u64 = 0,
        flush_output_runs: u64 = 0,
        flush_output_bytes: u64 = 0,
        flush_ns: u64 = 0,
        table_file_writes: u64 = 0,
        table_file_bytes: u64 = 0,
        table_file_logical_entry_bytes: u64 = 0,
        table_file_physical_entry_bytes: u64 = 0,
        table_file_raw_blocks: u64 = 0,
        table_file_compressed_blocks: u64 = 0,
        table_file_compression_codec_mask: u64 = 0,
        sorted_ingest_runs: u64 = 0,
        sorted_ingest_bytes: u64 = 0,
        sorted_ingest_ns: u64 = 0,
        compaction_ns: u64 = 0,
        manifest_writes: u64 = 0,
        manifest_bytes: u64 = 0,
        manifest_ns: u64 = 0,
        write_pressure_compactions: u64 = 0,
        write_pressure_ns: u64 = 0,
        wal_pressure_flushes: u64 = 0,
        wal_pressure_ns: u64 = 0,
        wal_append_records: u64 = 0,
        wal_append_entries: u64 = 0,
        wal_append_bytes: u64 = 0,
        wal_append_ns: u64 = 0,
        wal_sync_records: u64 = 0,
        wal_sync_ns: u64 = 0,
        wal_replay_records: u64 = 0,
        wal_replay_entries: u64 = 0,
        wal_replay_bytes: u64 = 0,
        wal_replay_ns: u64 = 0,
        wal_replay_truncated_tail_bytes: u64 = 0,
        wal_resets: u64 = 0,
        wal_reset_ns: u64 = 0,
        immutable_rotations: u64 = 0,
        immutable_flushes: u64 = 0,
        immutable_flush_entries: u64 = 0,
        immutable_flush_ns: u64 = 0,
        bulk_append_attempts: u64 = 0,
        bulk_append_entries: u64 = 0,
        bulk_append_direct_successes: u64 = 0,
        bulk_append_direct_entries: u64 = 0,
        bulk_append_fallback_non_bulk: u64 = 0,
        bulk_append_fallback_unsupported: u64 = 0,
        bulk_append_fallback_backend_pending: u64 = 0,
        bulk_append_fallback_duplicate_keys: u64 = 0,
        bulk_append_fallback_below_threshold: u64 = 0,
        bulk_append_fallback_to_mutable_entries: u64 = 0,
        bulk_append_sort_ns: u64 = 0,
        direct_bulk_ingest_attempts: u64 = 0,
        direct_bulk_ingest_entries: u64 = 0,
        direct_bulk_ingest_successes: u64 = 0,
        direct_bulk_ingest_entries_direct: u64 = 0,
        direct_bulk_ingest_fallback_unsupported: u64 = 0,
        direct_bulk_ingest_fallback_backend_mutable: u64 = 0,
        direct_bulk_ingest_fallback_below_threshold: u64 = 0,
        direct_bulk_ingest_sort_ns: u64 = 0,
    };

    pub const MaintenanceStats = struct {
        mutable_entries: u64 = 0,
        mutable_bytes: u64 = 0,
        mutable_snapshot_clone_calls: u64 = 0,
        mutable_snapshot_clone_bytes_total: u64 = 0,
        mutable_snapshot_clone_peak_bytes: u64 = 0,
        mutable_snapshot_clone_by_reason: [mutable_snapshot_reason_count]MutableSnapshotCloneReasonStats = [_]MutableSnapshotCloneReasonStats{.{}} ** mutable_snapshot_reason_count,
        read_snapshot_mutable_rotations: u64 = 0,
        read_snapshot_mutable_rotation_bytes_total: u64 = 0,
        read_snapshot_mutable_rotation_peak_bytes: u64 = 0,
        immutable_memtables: u64 = 0,
        immutable_entries: u64 = 0,
        immutable_bytes: u64 = 0,
        total_runs: u64 = 0,
        total_run_bytes: u64 = 0,
        total_run_logical_entry_bytes: u64 = 0,
        total_run_physical_entry_bytes: u64 = 0,
        total_run_compressed_blocks: u64 = 0,
        total_run_raw_blocks: u64 = 0,
        total_run_compression_codec_mask: u64 = 0,
        l0_runs: u64 = 0,
        l0_bytes: u64 = 0,
        lower_level_runs: u64 = 0,
        lower_level_bytes: u64 = 0,
        max_level: u32 = 0,
        compactable_l0_runs: u64 = 0,
        overlapping_l0_runs: u64 = 0,
        soft_limit_l0_runs: u64 = 0,
        hard_limit_l0_runs: u64 = 0,
        soft_limit_l0_bytes: u64 = 0,
        hard_limit_l0_bytes: u64 = 0,
        level_overflow_runs: u64 = 0,
        level_overflow_bytes: u64 = 0,
        obsolete_paths: u64 = 0,
        active_readers: u64 = 0,
        active_bulk_ingest_batches: u64 = 0,
        wal_retained_segments: u64 = 0,
        wal_retained_bytes: u64 = 0,
        wal_checkpoint_oldest_retained_segment: u64 = 0,
        wal_checkpoint_current_segment: u64 = 0,
        wal_checkpoint_lag_segments: u64 = 0,
        wal_replay_retained_segments: u64 = 0,
        wal_replay_retained_bytes: u64 = 0,
        wal_replay_current_segment: u64 = 0,
        manifest_dirty: bool = false,
        obsolete_manifest_dirty: bool = false,
        compaction_scheduler_active_jobs: u64 = 0,
        compaction_scheduler_in_flight_input_bytes: u64 = 0,
        compaction_scheduler_grants: u64 = 0,
        compaction_scheduler_completions: u64 = 0,
        compaction_scheduler_denied_capacity: u64 = 0,
        compaction_scheduler_denied_resource_pressure: u64 = 0,
        compaction_scheduler_oversized_grants: u64 = 0,
        compaction_scheduler_remembered_candidates: u64 = 0,
        compaction_scheduler_remembered_retries: u64 = 0,
        compaction_scheduler_remembered_hits: u64 = 0,
        compaction_scheduler_remembered_stale: u64 = 0,
        compaction_scheduler_conflict_denials: u64 = 0,
        compaction_scheduler_remembered_pending: u64 = 0,
        background_io_budget_bytes: u64 = 0,
        background_io_reserved_bytes: u64 = 0,
        background_io_denied_jobs: u64 = 0,
        background_io_oversized_jobs: u64 = 0,
        backend_lock_waits: u64 = 0,
        backend_lock_wait_ns: u64 = 0,
        backend_lock_max_wait_ns: u64 = 0,
    };

    pub fn accumulateMaintenanceStats(dst: *MaintenanceStats, src: MaintenanceStats) void {
        dst.mutable_entries +|= src.mutable_entries;
        dst.mutable_bytes +|= src.mutable_bytes;
        dst.mutable_snapshot_clone_calls +|= src.mutable_snapshot_clone_calls;
        dst.mutable_snapshot_clone_bytes_total +|= src.mutable_snapshot_clone_bytes_total;
        dst.mutable_snapshot_clone_peak_bytes = @max(dst.mutable_snapshot_clone_peak_bytes, src.mutable_snapshot_clone_peak_bytes);
        for (&dst.mutable_snapshot_clone_by_reason, src.mutable_snapshot_clone_by_reason) |*dst_reason, src_reason| {
            dst_reason.calls +|= src_reason.calls;
            dst_reason.bytes_total +|= src_reason.bytes_total;
            dst_reason.peak_bytes = @max(dst_reason.peak_bytes, src_reason.peak_bytes);
        }
        dst.read_snapshot_mutable_rotations +|= src.read_snapshot_mutable_rotations;
        dst.read_snapshot_mutable_rotation_bytes_total +|= src.read_snapshot_mutable_rotation_bytes_total;
        dst.read_snapshot_mutable_rotation_peak_bytes = @max(dst.read_snapshot_mutable_rotation_peak_bytes, src.read_snapshot_mutable_rotation_peak_bytes);
        dst.immutable_memtables +|= src.immutable_memtables;
        dst.immutable_entries +|= src.immutable_entries;
        dst.immutable_bytes +|= src.immutable_bytes;
        dst.total_runs +|= src.total_runs;
        dst.total_run_bytes +|= src.total_run_bytes;
        dst.total_run_logical_entry_bytes +|= src.total_run_logical_entry_bytes;
        dst.total_run_physical_entry_bytes +|= src.total_run_physical_entry_bytes;
        dst.total_run_compressed_blocks +|= src.total_run_compressed_blocks;
        dst.total_run_raw_blocks +|= src.total_run_raw_blocks;
        dst.total_run_compression_codec_mask |= src.total_run_compression_codec_mask;
        dst.l0_runs +|= src.l0_runs;
        dst.l0_bytes +|= src.l0_bytes;
        dst.lower_level_runs +|= src.lower_level_runs;
        dst.lower_level_bytes +|= src.lower_level_bytes;
        dst.max_level = @max(dst.max_level, src.max_level);
        dst.compactable_l0_runs +|= src.compactable_l0_runs;
        dst.overlapping_l0_runs +|= src.overlapping_l0_runs;
        dst.soft_limit_l0_runs +|= src.soft_limit_l0_runs;
        dst.hard_limit_l0_runs +|= src.hard_limit_l0_runs;
        dst.soft_limit_l0_bytes +|= src.soft_limit_l0_bytes;
        dst.hard_limit_l0_bytes +|= src.hard_limit_l0_bytes;
        dst.level_overflow_runs +|= src.level_overflow_runs;
        dst.level_overflow_bytes +|= src.level_overflow_bytes;
        dst.obsolete_paths +|= src.obsolete_paths;
        dst.active_readers +|= src.active_readers;
        dst.active_bulk_ingest_batches +|= src.active_bulk_ingest_batches;
        dst.wal_retained_segments +|= src.wal_retained_segments;
        dst.wal_retained_bytes +|= src.wal_retained_bytes;
        dst.wal_checkpoint_oldest_retained_segment = if (dst.wal_checkpoint_oldest_retained_segment == 0)
            src.wal_checkpoint_oldest_retained_segment
        else if (src.wal_checkpoint_oldest_retained_segment == 0)
            dst.wal_checkpoint_oldest_retained_segment
        else
            @min(dst.wal_checkpoint_oldest_retained_segment, src.wal_checkpoint_oldest_retained_segment);
        dst.wal_checkpoint_current_segment = @max(dst.wal_checkpoint_current_segment, src.wal_checkpoint_current_segment);
        dst.wal_checkpoint_lag_segments +|= src.wal_checkpoint_lag_segments;
        dst.wal_replay_retained_segments +|= src.wal_replay_retained_segments;
        dst.wal_replay_retained_bytes +|= src.wal_replay_retained_bytes;
        dst.wal_replay_current_segment = @max(dst.wal_replay_current_segment, src.wal_replay_current_segment);
        dst.manifest_dirty = dst.manifest_dirty or src.manifest_dirty;
        dst.obsolete_manifest_dirty = dst.obsolete_manifest_dirty or src.obsolete_manifest_dirty;
        dst.compaction_scheduler_active_jobs +|= src.compaction_scheduler_active_jobs;
        dst.compaction_scheduler_in_flight_input_bytes +|= src.compaction_scheduler_in_flight_input_bytes;
        dst.compaction_scheduler_grants +|= src.compaction_scheduler_grants;
        dst.compaction_scheduler_completions +|= src.compaction_scheduler_completions;
        dst.compaction_scheduler_denied_capacity +|= src.compaction_scheduler_denied_capacity;
        dst.compaction_scheduler_denied_resource_pressure +|= src.compaction_scheduler_denied_resource_pressure;
        dst.compaction_scheduler_oversized_grants +|= src.compaction_scheduler_oversized_grants;
        dst.compaction_scheduler_remembered_candidates +|= src.compaction_scheduler_remembered_candidates;
        dst.compaction_scheduler_remembered_retries +|= src.compaction_scheduler_remembered_retries;
        dst.compaction_scheduler_remembered_hits +|= src.compaction_scheduler_remembered_hits;
        dst.compaction_scheduler_remembered_stale +|= src.compaction_scheduler_remembered_stale;
        dst.compaction_scheduler_conflict_denials +|= src.compaction_scheduler_conflict_denials;
        dst.compaction_scheduler_remembered_pending +|= src.compaction_scheduler_remembered_pending;
        dst.background_io_budget_bytes +|= src.background_io_budget_bytes;
        dst.background_io_reserved_bytes +|= src.background_io_reserved_bytes;
        dst.background_io_denied_jobs +|= src.background_io_denied_jobs;
        dst.background_io_oversized_jobs +|= src.background_io_oversized_jobs;
        dst.backend_lock_waits +|= src.backend_lock_waits;
        dst.backend_lock_wait_ns +|= src.backend_lock_wait_ns;
        dst.backend_lock_max_wait_ns = @max(dst.backend_lock_max_wait_ns, src.backend_lock_max_wait_ns);
    }

    pub const ReadStats = struct {
        point_gets: u64 = 0,
        get_many_sorted_calls: u64 = 0,
        get_many_sorted_keys: u64 = 0,
        get_many_sorted_hits: u64 = 0,
        get_many_sorted_misses: u64 = 0,
        get_many_sorted_plan_point: u64 = 0,
        get_many_sorted_plan_sorted_by_run: u64 = 0,
        get_many_sorted_plan_cursor: u64 = 0,
        get_many_sorted_monotonic_pairs: u64 = 0,
        get_many_sorted_duplicate_pairs: u64 = 0,
        get_many_sorted_out_of_order_pairs: u64 = 0,
        mutable_hits: u64 = 0,
        l0_hits: u64 = 0,
        level_hits: u64 = 0,
        run_probes: u64 = 0,
        bloom_negatives: u64 = 0,
        read_hint_attempts: u64 = 0,
        read_hint_hits: u64 = 0,
        read_hint_misses: u64 = 0,
        table_entry_parses: u64 = 0,
        table_entry_parse_ns: u64 = 0,
        table_index_loads: u64 = 0,
        table_index_load_ns: u64 = 0,
        table_index_decodes: u64 = 0,
        table_index_decode_ns: u64 = 0,
        table_block_loads: u64 = 0,
        table_block_bytes: u64 = 0,
        table_block_load_ns: u64 = 0,
        shared_block_cache_hits: u64 = 0,
        shared_block_cache_misses: u64 = 0,
        local_block_cache_hits: u64 = 0,
        local_block_cache_misses: u64 = 0,
        cursor_block_reuses: u64 = 0,
        cursor_block_loads: u64 = 0,
        cursor_block_readaheads: u64 = 0,
        cursor_table_index_hits: u64 = 0,
        cursor_table_index_misses: u64 = 0,
        cursor_value_borrows: u64 = 0,
        cursor_value_copies: u64 = 0,
        point_value_borrows: u64 = 0,
        point_value_copies: u64 = 0,
        run_group_builds: u64 = 0,
        run_group_build_ns: u64 = 0,
        run_group_total_runs: u64 = 0,
        run_group_l0_runs: u64 = 0,
    };

    const AtomicReadStats = struct {
        point_gets: CounterU64 = .init(0),
        get_many_sorted_calls: CounterU64 = .init(0),
        get_many_sorted_keys: CounterU64 = .init(0),
        get_many_sorted_hits: CounterU64 = .init(0),
        get_many_sorted_misses: CounterU64 = .init(0),
        get_many_sorted_plan_point: CounterU64 = .init(0),
        get_many_sorted_plan_sorted_by_run: CounterU64 = .init(0),
        get_many_sorted_plan_cursor: CounterU64 = .init(0),
        get_many_sorted_monotonic_pairs: CounterU64 = .init(0),
        get_many_sorted_duplicate_pairs: CounterU64 = .init(0),
        get_many_sorted_out_of_order_pairs: CounterU64 = .init(0),
        mutable_hits: CounterU64 = .init(0),
        l0_hits: CounterU64 = .init(0),
        level_hits: CounterU64 = .init(0),
        run_probes: CounterU64 = .init(0),
        bloom_negatives: CounterU64 = .init(0),
        read_hint_attempts: CounterU64 = .init(0),
        read_hint_hits: CounterU64 = .init(0),
        read_hint_misses: CounterU64 = .init(0),
        table_entry_parses: CounterU64 = .init(0),
        table_entry_parse_ns: CounterU64 = .init(0),
        table_index_loads: CounterU64 = .init(0),
        table_index_load_ns: CounterU64 = .init(0),
        table_index_decodes: CounterU64 = .init(0),
        table_index_decode_ns: CounterU64 = .init(0),
        table_block_loads: CounterU64 = .init(0),
        table_block_bytes: CounterU64 = .init(0),
        table_block_load_ns: CounterU64 = .init(0),
        shared_block_cache_hits: CounterU64 = .init(0),
        shared_block_cache_misses: CounterU64 = .init(0),
        local_block_cache_hits: CounterU64 = .init(0),
        local_block_cache_misses: CounterU64 = .init(0),
        cursor_block_reuses: CounterU64 = .init(0),
        cursor_block_loads: CounterU64 = .init(0),
        cursor_block_readaheads: CounterU64 = .init(0),
        cursor_table_index_hits: CounterU64 = .init(0),
        cursor_table_index_misses: CounterU64 = .init(0),
        cursor_value_borrows: CounterU64 = .init(0),
        cursor_value_copies: CounterU64 = .init(0),
        point_value_borrows: CounterU64 = .init(0),
        point_value_copies: CounterU64 = .init(0),
        run_group_builds: CounterU64 = .init(0),
        run_group_build_ns: CounterU64 = .init(0),
        run_group_total_runs: CounterU64 = .init(0),
        run_group_l0_runs: CounterU64 = .init(0),

        fn snapshot(self: *const AtomicReadStats) ReadStats {
            return .{
                .point_gets = self.point_gets.load(.monotonic),
                .get_many_sorted_calls = self.get_many_sorted_calls.load(.monotonic),
                .get_many_sorted_keys = self.get_many_sorted_keys.load(.monotonic),
                .get_many_sorted_hits = self.get_many_sorted_hits.load(.monotonic),
                .get_many_sorted_misses = self.get_many_sorted_misses.load(.monotonic),
                .get_many_sorted_plan_point = self.get_many_sorted_plan_point.load(.monotonic),
                .get_many_sorted_plan_sorted_by_run = self.get_many_sorted_plan_sorted_by_run.load(.monotonic),
                .get_many_sorted_plan_cursor = self.get_many_sorted_plan_cursor.load(.monotonic),
                .get_many_sorted_monotonic_pairs = self.get_many_sorted_monotonic_pairs.load(.monotonic),
                .get_many_sorted_duplicate_pairs = self.get_many_sorted_duplicate_pairs.load(.monotonic),
                .get_many_sorted_out_of_order_pairs = self.get_many_sorted_out_of_order_pairs.load(.monotonic),
                .mutable_hits = self.mutable_hits.load(.monotonic),
                .l0_hits = self.l0_hits.load(.monotonic),
                .level_hits = self.level_hits.load(.monotonic),
                .run_probes = self.run_probes.load(.monotonic),
                .bloom_negatives = self.bloom_negatives.load(.monotonic),
                .read_hint_attempts = self.read_hint_attempts.load(.monotonic),
                .read_hint_hits = self.read_hint_hits.load(.monotonic),
                .read_hint_misses = self.read_hint_misses.load(.monotonic),
                .table_entry_parses = self.table_entry_parses.load(.monotonic),
                .table_entry_parse_ns = self.table_entry_parse_ns.load(.monotonic),
                .table_index_loads = self.table_index_loads.load(.monotonic),
                .table_index_load_ns = self.table_index_load_ns.load(.monotonic),
                .table_index_decodes = self.table_index_decodes.load(.monotonic),
                .table_index_decode_ns = self.table_index_decode_ns.load(.monotonic),
                .table_block_loads = self.table_block_loads.load(.monotonic),
                .table_block_bytes = self.table_block_bytes.load(.monotonic),
                .table_block_load_ns = self.table_block_load_ns.load(.monotonic),
                .shared_block_cache_hits = self.shared_block_cache_hits.load(.monotonic),
                .shared_block_cache_misses = self.shared_block_cache_misses.load(.monotonic),
                .local_block_cache_hits = self.local_block_cache_hits.load(.monotonic),
                .local_block_cache_misses = self.local_block_cache_misses.load(.monotonic),
                .cursor_block_reuses = self.cursor_block_reuses.load(.monotonic),
                .cursor_block_loads = self.cursor_block_loads.load(.monotonic),
                .cursor_block_readaheads = self.cursor_block_readaheads.load(.monotonic),
                .cursor_table_index_hits = self.cursor_table_index_hits.load(.monotonic),
                .cursor_table_index_misses = self.cursor_table_index_misses.load(.monotonic),
                .cursor_value_borrows = self.cursor_value_borrows.load(.monotonic),
                .cursor_value_copies = self.cursor_value_copies.load(.monotonic),
                .point_value_borrows = self.point_value_borrows.load(.monotonic),
                .point_value_copies = self.point_value_copies.load(.monotonic),
                .run_group_builds = self.run_group_builds.load(.monotonic),
                .run_group_build_ns = self.run_group_build_ns.load(.monotonic),
                .run_group_total_runs = self.run_group_total_runs.load(.monotonic),
                .run_group_l0_runs = self.run_group_l0_runs.load(.monotonic),
            };
        }
    };

    const CachedRunState = struct {
        const Value = union(enum) {
            owned: State,
            shared: cache_mod.Handle,
        };

        run_id: u64,
        path: []u8,
        value: Value,

        pub fn deinit(self: *CachedRunState, allocator: Allocator) void {
            allocator.free(self.path);
            switch (self.value) {
                .owned => |*owned_state| owned_state.deinit(allocator),
                .shared => |*handle| handle.release(),
            }
            self.* = undefined;
        }

        pub fn state(self: *const CachedRunState) *const State {
            return switch (self.value) {
                .owned => |*owned| owned,
                .shared => |*handle| handle.runState(),
            };
        }
    };

    const CachedRunTable = struct {
        const SharedValue = struct {
            raw: cache_mod.Handle,
            index: cache_mod.Handle,
            table: lsm_table_file.BorrowedDecoded,

            fn deinit(self: *SharedValue) void {
                self.raw.release();
                self.index.release();
                self.* = undefined;
            }
        };

        const Value = union(enum) {
            owned: lsm_table_file.BorrowedDecoded,
            shared: SharedValue,
        };

        run_id: u64,
        path: []u8,
        value: Value,

        pub fn deinit(self: *CachedRunTable, allocator: Allocator) void {
            allocator.free(self.path);
            switch (self.value) {
                .owned => |*owned_table| owned_table.deinit(allocator),
                .shared => |*shared| shared.deinit(),
            }
            self.* = undefined;
        }

        pub fn table(self: *const CachedRunTable) *const lsm_table_file.BorrowedDecoded {
            return switch (self.value) {
                .owned => |*owned| owned,
                .shared => |*shared| &shared.table,
            };
        }
    };

    const CachedRunIndex = struct {
        run_id: u64,
        path: []u8,
        index: *lsm_table_file.TableIndex,

        pub fn deinit(self: *CachedRunIndex, allocator: Allocator) void {
            allocator.free(self.path);
            self.index.deinit(allocator);
            allocator.destroy(self.index);
            self.* = undefined;
        }
    };

    const CachedRunBlock = struct {
        run_id: u64,
        path: []u8,
        block_offset: u64,
        block_len: u32,
        bytes: []u8,
        last_access: u64,

        pub fn deinit(self: *CachedRunBlock, allocator: Allocator) void {
            allocator.free(self.path);
            allocator.free(self.bytes);
            self.* = undefined;
        }
    };

    const WalSegmentRange = struct {
        first: u64 = 0,
        last: u64 = 0,

        fn isSet(self: @This()) bool {
            return self.first != 0 and self.last != 0;
        }
    };

    allocator: Allocator,
    mu: std.atomic.Mutex = .unlocked,
    cached_maintenance_hint: CounterU64 = .init(1),
    options: Options,
    root_generation: u64 = 0,
    root_dir: ?[]u8 = null,
    storage_owner: ?*storage_io.NativeStorage = null,
    storage: ?storage_io.Storage = null,
    manifest_backing: ?[]u8 = null,
    next_run_id: u64 = 1,
    active_readers: usize = 0,
    manifest_dirty: bool = false,
    obsolete_paths: std.ArrayListUnmanaged(ObsoletePath) = .empty,
    obsolete_manifest_dirty: bool = false,
    obsolete_runs: std.ArrayListUnmanaged(std.ArrayListUnmanaged(Run)) = .empty,
    run_state_cache: std.ArrayListUnmanaged(CachedRunState) = .empty,
    run_index_cache: std.ArrayListUnmanaged(CachedRunIndex) = .empty,
    run_block_cache: std.ArrayListUnmanaged(CachedRunBlock) = .empty,
    run_table_cache: std.ArrayListUnmanaged(CachedRunTable) = .empty,
    local_cache_access_clock: u64 = 0,
    compaction_stats: CompactionStats = .{},
    compaction_scheduler: compaction_scheduler_mod.Scheduler = .{},
    background_executor: BackgroundExecutor = BackgroundExecutor.initInline(0),
    immutable_flush_job_in_flight: bool = false,
    immutable_flush_build_in_flight: bool = false,
    maintenance_io_budget_remaining: ?u64 = null,
    background_io_reserved_bytes: u64 = 0,
    background_io_denied_jobs: u64 = 0,
    background_io_oversized_jobs: u64 = 0,
    write_pressure_enforcing: bool = false,
    remembered_compaction: ?compaction_mod.RememberedCompaction = null,
    write_stats: WriteStats = .{},
    read_stats: AtomicReadStats = .{},
    tracked_in_memory_state_bytes: u64 = 0,
    tracked_wal_retention_bytes: u64 = 0,
    backend_lock_waits: CounterU64 = .init(0),
    backend_lock_wait_ns: CounterU64 = .init(0),
    backend_lock_max_wait_ns: CounterU64 = .init(0),
    mutable_snapshot_clone_calls: u64 = 0,
    mutable_snapshot_clone_bytes_total: u64 = 0,
    mutable_snapshot_clone_peak_bytes: u64 = 0,
    mutable_snapshot_clone_by_reason: [mutable_snapshot_reason_count]MutableSnapshotCloneReasonStats = [_]MutableSnapshotCloneReasonStats{.{}} ** mutable_snapshot_reason_count,
    read_snapshot_mutable_rotations: u64 = 0,
    read_snapshot_mutable_rotation_bytes_total: u64 = 0,
    read_snapshot_mutable_rotation_peak_bytes: u64 = 0,
    active_bulk_ingest_batches: usize = 0,
    mutable: ActiveMemTable = .{},
    mutable_wal_range: WalSegmentRange = .{},
    empty_mutable_snapshot: State = .{},
    mutable_read_snapshot: ?*State = null,
    immutable_memtables: std.ArrayListUnmanaged(*State) = .empty,
    immutable_wal_ranges: std.ArrayListUnmanaged(WalSegmentRange) = .empty,
    immutable_head: usize = 0,
    retired_immutable_memtables: std.ArrayListUnmanaged(*State) = .empty,
    retired_mutable_snapshots: std.ArrayListUnmanaged(*State) = .empty,
    recovery_replaying_wal: bool = false,
    runs: std.ArrayListUnmanaged(repository_mod.Run) = .empty,

    const BoundStore = runtime_mod.BoundStore(Backend);
    const BoundReadTxn = runtime_mod.BoundReadTxn(Backend);
    const BoundWriteTxn = runtime_mod.BoundWriteTxn(Backend);
    const NamespaceReadTxn = runtime_mod.NamespaceReadTxn(Backend);
    const NamespaceWriteTxn = runtime_mod.NamespaceWriteTxn(Backend);
    pub const BulkIngestFinishOptions = backend_types.BulkIngestFinishOptions;

    pub fn init(allocator: Allocator, options: Options) Backend {
        var backend: Backend = undefined;
        initInPlace(&backend, allocator, options);
        return backend;
    }

    pub fn initInPlace(self: *Backend, allocator: Allocator, options: Options) void {
        self.* = .{
            .allocator = allocator,
            .options = options,
            .root_generation = options.root_generation,
            .compaction_scheduler = compaction_scheduler_mod.Scheduler.init(options.compaction_scheduler),
            .background_executor = resolveBackgroundExecutor(options.background_executor),
            .storage_owner = null,
            .storage = options.storage,
        };
    }

    pub fn open(allocator: Allocator, root_dir: []const u8, options: Options) !Backend {
        return try recovery_mod.open(Backend, allocator, root_dir, options.backend, options);
    }

    pub fn openInto(self: *Backend, allocator: Allocator, root_dir: []const u8, options: Options) !void {
        try recovery_mod.openInto(Backend, self, allocator, root_dir, options.backend, options);
    }

    pub fn close(self: *Backend) void {
        self.background_executor.drain();
        self.releaseTrackedResourceUsage();
        recovery_mod.close(Backend, self);
    }

    fn resolveBackgroundExecutor(configured: ?*const BackgroundExecutor) BackgroundExecutor {
        return if (configured) |executor| executor.* else BackgroundExecutor.initInline(0);
    }

    pub fn sync(self: *Backend, force: bool) !void {
        _ = force;
        if (self.root_dir == null) return;
        const locked = runtime_mod.lockBackend(Backend, self);
        defer runtime_mod.unlockBackend(Backend, self, locked);
        try self.finalizeDeferredStorageWorkLocked();
    }

    pub fn syncReplayState(self: *Backend) !void {
        if (self.root_dir == null) return;
        const locked = runtime_mod.lockBackend(Backend, self);
        defer runtime_mod.unlockBackend(Backend, self, locked);
        if (!self.options.backend.read_only and self.options.wal_enabled) {
            try wal_mod.syncCurrentState(self.storage.?, self.allocator, self.root_dir.?);
            return;
        }
        try self.finalizeDeferredStorageWorkLocked();
    }

    pub fn snapshotReadStats(self: *const Backend) ReadStats {
        return self.read_stats.snapshot();
    }

    pub fn snapshotWriteStats(self: *const Backend) WriteStats {
        return self.write_stats;
    }

    pub fn snapshotMaintenanceStats(self: *const Backend) MaintenanceStats {
        const mutable: *Backend = @constCast(self);
        const locked = runtime_mod.lockBackend(Backend, mutable);
        defer runtime_mod.unlockBackend(Backend, mutable, locked);
        return mutable.snapshotMaintenanceStatsLocked();
    }

    fn snapshotMaintenanceStatsLocked(self: *Backend) MaintenanceStats {
        return self.snapshotMaintenanceStatsLockedWithOptions(true);
    }

    fn snapshotMaintenanceStatsLockedWithOptions(self: *Backend, include_retention: bool) MaintenanceStats {
        var stats = MaintenanceStats{
            .mutable_entries = @intCast(self.mutable.entries.items.len),
            .mutable_bytes = estimateStateBytes(&self.mutable),
            .mutable_snapshot_clone_calls = self.mutable_snapshot_clone_calls,
            .mutable_snapshot_clone_bytes_total = self.mutable_snapshot_clone_bytes_total,
            .mutable_snapshot_clone_peak_bytes = self.mutable_snapshot_clone_peak_bytes,
            .mutable_snapshot_clone_by_reason = self.mutable_snapshot_clone_by_reason,
            .read_snapshot_mutable_rotations = self.read_snapshot_mutable_rotations,
            .read_snapshot_mutable_rotation_bytes_total = self.read_snapshot_mutable_rotation_bytes_total,
            .read_snapshot_mutable_rotation_peak_bytes = self.read_snapshot_mutable_rotation_peak_bytes,
            .immutable_memtables = @intCast(self.activeImmutableMemtableCount()),
            .total_runs = @intCast(self.runs.items.len),
            .obsolete_paths = @intCast(self.obsolete_paths.items.len),
            .active_readers = @intCast(self.active_readers),
            .active_bulk_ingest_batches = @intCast(self.active_bulk_ingest_batches),
            .soft_limit_l0_runs = @intCast(self.effectiveL0SoftLimitRuns()),
            .hard_limit_l0_runs = @intCast(self.effectiveL0HardLimitRuns()),
            .soft_limit_l0_bytes = self.options.l0_soft_limit_bytes,
            .hard_limit_l0_bytes = self.options.l0_hard_limit_bytes,
            .manifest_dirty = self.manifest_dirty,
            .obsolete_manifest_dirty = self.obsolete_manifest_dirty,
        };
        for (self.activeImmutableMemtables()) |state| {
            stats.immutable_entries += @intCast(state.entries.items.len);
            stats.immutable_bytes += estimateStateBytes(state);
        }

        var i: usize = 0;
        while (i < self.runs.items.len) {
            const level = self.runs.items[i].level;
            const start = i;
            var level_bytes: u64 = 0;
            while (i < self.runs.items.len and self.runs.items[i].level == level) : (i += 1) {
                const run = self.runs.items[i];
                level_bytes += run.size_bytes;
                stats.total_run_logical_entry_bytes +|= run.compression_stats.logical_entry_bytes;
                stats.total_run_physical_entry_bytes +|= run.compression_stats.physical_entry_bytes;
                stats.total_run_compressed_blocks +|= run.compression_stats.compressed_blocks;
                stats.total_run_raw_blocks +|= run.compression_stats.raw_blocks;
                stats.total_run_compression_codec_mask |= run.compression_stats.compression_codec_mask;
            }

            const level_len = i - start;
            stats.total_run_bytes += level_bytes;
            stats.max_level = @max(stats.max_level, level);
            if (level == 0) {
                stats.l0_runs = @intCast(level_len);
                stats.l0_bytes = level_bytes;
                if (level_len > self.options.compact_threshold_runs) {
                    stats.compactable_l0_runs = @intCast(level_len - self.options.compact_threshold_runs);
                }
                stats.overlapping_l0_runs = @intCast(compaction_mod.largestL0OverlapRunCount(self.runs.items, self.options.l0_overlap_compact_threshold_runs));
            } else {
                stats.lower_level_runs += @intCast(level_len);
                stats.lower_level_bytes += level_bytes;
                const target_runs = maintenanceLevelRunTarget(level, self.options.level_target_runs_base, self.options.level_target_runs_multiplier);
                if (level_len > target_runs) {
                    stats.level_overflow_runs += @intCast(level_len - target_runs);
                }
                const target_bytes = maintenanceLevelByteTarget(level, self.options.level_target_bytes_base, self.options.level_target_bytes_multiplier);
                if (target_bytes > 0 and level_bytes > target_bytes) {
                    stats.level_overflow_bytes += level_bytes - target_bytes;
                }
            }
        }

        const scheduler_stats = self.compaction_scheduler.snapshot();
        stats.compaction_scheduler_active_jobs = scheduler_stats.active_jobs;
        stats.compaction_scheduler_in_flight_input_bytes = scheduler_stats.in_flight_input_bytes;
        stats.compaction_scheduler_grants = scheduler_stats.grants;
        stats.compaction_scheduler_completions = scheduler_stats.completions;
        stats.compaction_scheduler_denied_capacity = scheduler_stats.denied_capacity;
        stats.compaction_scheduler_denied_resource_pressure = scheduler_stats.denied_resource_pressure;
        stats.compaction_scheduler_oversized_grants = scheduler_stats.oversized_grants;
        stats.compaction_scheduler_remembered_candidates = scheduler_stats.remembered_candidates;
        stats.compaction_scheduler_remembered_retries = scheduler_stats.remembered_retries;
        stats.compaction_scheduler_remembered_hits = scheduler_stats.remembered_hits;
        stats.compaction_scheduler_remembered_stale = scheduler_stats.remembered_stale;
        stats.compaction_scheduler_conflict_denials = scheduler_stats.conflict_denials;
        stats.compaction_scheduler_remembered_pending = if (self.remembered_compaction != null) 1 else 0;
        stats.background_io_budget_bytes = self.options.background_io_budget_bytes;
        stats.background_io_reserved_bytes = self.background_io_reserved_bytes;
        stats.background_io_denied_jobs = self.background_io_denied_jobs;
        stats.background_io_oversized_jobs = self.background_io_oversized_jobs;
        stats.backend_lock_waits = self.backend_lock_waits.load(.monotonic);
        stats.backend_lock_wait_ns = self.backend_lock_wait_ns.load(.monotonic);
        stats.backend_lock_max_wait_ns = self.backend_lock_max_wait_ns.load(.monotonic);
        if (include_retention and self.options.wal_enabled and self.root_dir != null) {
            const wal_retention = wal_mod.snapshotRetention(self.storage.?, self.allocator, self.root_dir.?) catch wal_mod.RetentionStats{};
            stats.wal_retained_segments = wal_retention.segments;
            stats.wal_retained_bytes = wal_retention.bytes;
            stats.wal_checkpoint_oldest_retained_segment = wal_retention.oldest_retained_segment;
            stats.wal_checkpoint_current_segment = wal_retention.current_segment;
            if (wal_retention.current_segment > wal_retention.oldest_retained_segment) {
                stats.wal_checkpoint_lag_segments = wal_retention.current_segment - wal_retention.oldest_retained_segment;
            }
            const replay_retention = wal_mod.snapshotReplayRetention(self.storage.?, self.allocator, self.root_dir.?) catch wal_mod.RetentionStats{};
            stats.wal_retained_segments += replay_retention.segments;
            stats.wal_retained_bytes += replay_retention.bytes;
            stats.wal_replay_retained_segments = replay_retention.segments;
            stats.wal_replay_retained_bytes = replay_retention.bytes;
            stats.wal_replay_current_segment = replay_retention.current_segment;
            self.syncTrackedWalRetentionUsageLocked(stats.wal_retained_bytes);
        }
        self.syncTrackedInMemoryStateUsageLocked(stats);
        return stats;
    }

    pub fn recordBackendLockWait(self: *Backend, wait_ns: u64) void {
        _ = self.backend_lock_waits.fetchAdd(1, .monotonic);
        _ = self.backend_lock_wait_ns.fetchAdd(wait_ns, .monotonic);
        atomicMaxCounter(&self.backend_lock_max_wait_ns, wait_ns);
    }

    pub fn snapshotCacheStats(self: *const Backend) ?cache_mod.Stats {
        const cache = self.options.cache orelse return null;
        return cache.snapshotStats();
    }

    pub fn snapshotNativeStorageStats(self: *const Backend) ?storage_io.NativeStorageStats {
        const owner = self.storage_owner orelse return null;
        return owner.snapshotStats();
    }

    pub fn maintenanceDebtHint(self: *const Backend) u64 {
        return self.cached_maintenance_hint.load(.monotonic);
    }

    pub fn notePotentialMaintenanceDebt(self: *Backend) void {
        self.cached_maintenance_hint.store(1, .release);
    }

    pub fn refreshMaintenanceDebtHint(self: *Backend) void {
        const locked = runtime_mod.lockBackend(Backend, self);
        defer runtime_mod.unlockBackend(Backend, self, locked);
        _ = self.refreshCachedMaintenanceHintLocked();
    }

    pub fn maintenanceScore(self: *const Backend) u64 {
        const mutable: *Backend = @constCast(self);
        const locked = runtime_mod.lockBackend(Backend, mutable);
        defer runtime_mod.unlockBackend(Backend, mutable, locked);
        return mutable.maintenanceScoreLocked();
    }

    pub fn maintenanceScoreBestEffort(self: *Backend) ?u64 {
        if (!self.mu.tryLock()) return null;
        defer self.mu.unlock();
        return self.maintenanceScoreLocked();
    }

    fn maintenanceScoreLocked(self: *Backend) u64 {
        var score: u64 = 0;

        const soft_limit_l0_runs = self.effectiveL0SoftLimitRuns();
        const hard_limit_l0_runs = self.effectiveL0HardLimitRuns();
        if (hard_limit_l0_runs > 0) {
            const l0_runs = countLevelRuns(self.runs.items, 0);
            if (l0_runs > hard_limit_l0_runs) {
                score +|= (l0_runs - hard_limit_l0_runs) * 1_000_000;
            } else if (l0_runs > soft_limit_l0_runs) {
                score +|= (l0_runs - soft_limit_l0_runs) * 10_000;
            }
            if (l0_runs > self.options.compact_threshold_runs) {
                score +|= (l0_runs - self.options.compact_threshold_runs) * 1_000;
            }
            score +|= compaction_mod.largestL0OverlapRunCount(self.runs.items, self.options.l0_overlap_compact_threshold_runs) * 2_000;
        }

        var l0_bytes: u64 = 0;
        var level_overflow_runs: u64 = 0;
        var level_overflow_bytes: u64 = 0;
        var i: usize = 0;
        while (i < self.runs.items.len) {
            const level = self.runs.items[i].level;
            const start = i;
            var level_bytes: u64 = 0;
            while (i < self.runs.items.len and self.runs.items[i].level == level) : (i += 1) {
                level_bytes += self.runs.items[i].size_bytes;
            }

            const level_len = i - start;
            if (level == 0) {
                l0_bytes = level_bytes;
                continue;
            }

            const target_runs = maintenanceLevelRunTarget(level, self.options.level_target_runs_base, self.options.level_target_runs_multiplier);
            if (level_len > target_runs) {
                level_overflow_runs +|= @intCast(level_len - target_runs);
            }
            const target_bytes = maintenanceLevelByteTarget(level, self.options.level_target_bytes_base, self.options.level_target_bytes_multiplier);
            if (target_bytes > 0 and level_bytes > target_bytes) {
                level_overflow_bytes +|= level_bytes - target_bytes;
            }
        }

        if (self.options.l0_hard_limit_bytes > 0 and l0_bytes > self.options.l0_hard_limit_bytes) {
            score +|= (l0_bytes - self.options.l0_hard_limit_bytes) / 1024;
        } else if (self.options.l0_soft_limit_bytes > 0 and l0_bytes > self.options.l0_soft_limit_bytes) {
            score +|= (l0_bytes - self.options.l0_soft_limit_bytes) / (16 * 1024);
        }

        const immutable_memtables = self.activeImmutableMemtableCount();
        score +|= @as(u64, @intCast(immutable_memtables)) * 5_000;
        for (self.activeImmutableMemtables()) |state| {
            score +|= estimateStateBytes(state) / (16 * 1024);
        }

        score +|= level_overflow_runs * 500;
        score +|= level_overflow_bytes / (64 * 1024);
        score +|= self.walRetentionPressureScoreLocked();
        if (self.manifest_dirty or self.obsolete_manifest_dirty or self.hasReclaimableObsoletePathsLocked()) score +|= 1;
        return score;
    }

    fn refreshCachedMaintenanceHintLocked(self: *Backend) u64 {
        const score = self.maintenanceScoreLocked();
        self.cached_maintenance_hint.store(if (score > 0) 1 else 0, .release);
        return score;
    }

    fn syncTrackedInMemoryStateUsageLocked(self: *Backend, stats: MaintenanceStats) void {
        const manager = self.options.resource_manager orelse return;
        manager.observeUsage(
            .lsm_in_memory_state,
            &self.tracked_in_memory_state_bytes,
            stats.mutable_bytes + stats.immutable_bytes,
        );
    }

    pub fn syncTrackedInMemoryStateUsageCurrentLocked(self: *Backend) void {
        const manager = self.options.resource_manager orelse return;
        manager.observeUsage(
            .lsm_in_memory_state,
            &self.tracked_in_memory_state_bytes,
            self.estimateInMemoryStateBytesLocked(),
        );
    }

    fn syncTrackedWalRetentionUsageCurrentLocked(self: *Backend) void {
        if (!self.options.wal_enabled or self.root_dir == null) return;
        const retention = wal_mod.snapshotRetention(self.storage.?, self.allocator, self.root_dir.?) catch wal_mod.RetentionStats{};
        const replay_retention = wal_mod.snapshotReplayRetention(self.storage.?, self.allocator, self.root_dir.?) catch wal_mod.RetentionStats{};
        self.syncTrackedWalRetentionUsageLocked(retention.bytes +| replay_retention.bytes);
    }

    fn syncTrackedWalRetentionUsageLocked(self: *Backend, bytes: u64) void {
        const manager = self.options.resource_manager orelse return;
        manager.observeUsage(.lsm_wal_retention, &self.tracked_wal_retention_bytes, bytes);
    }

    fn estimateInMemoryStateBytesLocked(self: *const Backend) u64 {
        var bytes = estimateStateBytes(&self.mutable);
        for (self.activeImmutableMemtables()) |state| {
            bytes +|= estimateStateBytes(state);
        }
        return bytes;
    }

    fn releaseTrackedResourceUsage(self: *Backend) void {
        const manager = self.options.resource_manager orelse return;
        manager.observeUsage(.lsm_in_memory_state, &self.tracked_in_memory_state_bytes, 0);
        manager.observeUsage(.lsm_wal_retention, &self.tracked_wal_retention_bytes, 0);
    }

    pub fn acquireCompactionGrant(self: *Backend, work: anytype) ?compaction_scheduler_mod.Grant {
        const io_bytes = if (@hasField(@TypeOf(work), "io_bytes")) work.io_bytes else work.input_bytes;
        if (!self.canReserveMaintenanceIoBudget(io_bytes)) return null;
        const grant = self.compaction_scheduler.tryAcquire(.{
            .score = work.score,
            .input_runs = work.input_runs,
            .input_bytes = work.input_bytes,
        }, self.options.resource_manager) orelse return null;
        self.reserveMaintenanceIoBudgetAssumeAdmitted(io_bytes);
        return grant;
    }

    fn canReserveMaintenanceIoBudget(self: *Backend, io_bytes: u64) bool {
        const remaining = self.maintenance_io_budget_remaining orelse return true;
        if (io_bytes == 0 or io_bytes <= remaining) return true;
        const budget = self.options.background_io_budget_bytes;
        if (self.options.background_io_allow_oversized_single_job and remaining == budget) return true;
        self.background_io_denied_jobs +|= 1;
        return false;
    }

    fn reserveMaintenanceIoBudgetAssumeAdmitted(self: *Backend, io_bytes: u64) void {
        if (io_bytes == 0) return;
        const remaining = self.maintenance_io_budget_remaining orelse return;
        if (io_bytes <= remaining) {
            self.maintenance_io_budget_remaining = remaining - io_bytes;
            self.background_io_reserved_bytes +|= io_bytes;
            return;
        }
        self.maintenance_io_budget_remaining = 0;
        self.background_io_reserved_bytes +|= io_bytes;
        self.background_io_oversized_jobs +|= 1;
    }

    fn tryReserveMaintenanceIoBudget(self: *Backend, io_bytes: u64) bool {
        if (!self.canReserveMaintenanceIoBudget(io_bytes)) return false;
        self.reserveMaintenanceIoBudgetAssumeAdmitted(io_bytes);
        return true;
    }

    pub fn runMaintenanceStep(self: *Backend) !bool {
        const locked = runtime_mod.lockBackend(Backend, self);
        defer runtime_mod.unlockBackend(Backend, self, locked);
        return try self.runMaintenanceStepLocked();
    }

    pub fn runMaintenanceStepBestEffort(self: *Backend) !bool {
        if (self.options.backend.read_only or self.bulkIngestActive()) return false;
        if (!self.mu.tryLock()) return false;
        defer self.mu.unlock();
        return try self.runMaintenanceStepLocked();
    }

    fn runMaintenanceStepLocked(self: *Backend) !bool {
        if (self.options.backend.read_only or self.bulkIngestActive()) return false;
        self.maintenance_io_budget_remaining = if (self.options.background_io_budget_bytes > 0)
            self.options.background_io_budget_bytes
        else
            null;
        defer self.maintenance_io_budget_remaining = null;
        const before_compactions = self.compaction_stats.compactions;
        const before_manifest_writes = self.write_stats.manifest_writes;

        if (self.shouldFlushMutable() or try self.shouldFlushMutableForWalPressureLocked()) {
            try self.rotateMutableToImmutable();
        }
        if (self.activeImmutableMemtableCount() > 0) {
            _ = try self.flushOldestImmutableMemtable();
        } else {
            const soft_l0_runs = self.effectiveL0SoftLimitRuns();
            if (soft_l0_runs > 0 and countLevelRuns(self.runs.items, 0) > soft_l0_runs) {
                const score = self.maintenanceScoreLocked();
                _ = try compaction_mod.compactL0ToLimitScheduled(Backend, self, soft_l0_runs, score);
            } else {
                const score = self.maintenanceScoreLocked();
                _ = try compaction_mod.maybeCompactRunsScheduled(Backend, self, score);
            }
        }
        try self.enforceWritePressure();
        if (self.root_dir != null and (self.manifest_dirty or self.obsolete_manifest_dirty or self.hasReclaimableObsoletePathsLocked())) {
            try self.persistManifest();
        }

        _ = self.refreshCachedMaintenanceHintLocked();

        return self.compaction_stats.compactions != before_compactions or
            self.write_stats.manifest_writes != before_manifest_writes;
    }

    pub fn activeImmutableMemtableCount(self: *const Backend) usize {
        std.debug.assert(self.immutable_head <= self.immutable_memtables.items.len);
        return self.immutable_memtables.items.len - self.immutable_head;
    }

    fn activeImmutableMemtables(self: *const Backend) []const *State {
        std.debug.assert(self.immutable_head <= self.immutable_memtables.items.len);
        return self.immutable_memtables.items[self.immutable_head..];
    }

    pub fn snapshotImmutableMemtables(self: *Backend) ![]*const State {
        const active = self.activeImmutableMemtables();
        const snapshot = try self.allocator.alloc(*const State, active.len);
        for (active, 0..) |state, i| {
            snapshot[active.len - 1 - i] = state;
        }
        return snapshot;
    }

    pub fn prepareReadSnapshot(self: *Backend) !void {
        if (self.mutable_read_snapshot != null) return;
        if (self.options.read_snapshot_rotate_mutable_bytes == 0) return;
        if (self.mutable.entries.items.len == 0) return;
        const mutable_bytes = estimateStateBytes(&self.mutable);
        if (mutable_bytes < self.options.read_snapshot_rotate_mutable_bytes) return;

        try self.rotateMutableToImmutable();
        self.read_snapshot_mutable_rotations +|= 1;
        self.read_snapshot_mutable_rotation_bytes_total +|= mutable_bytes;
        self.read_snapshot_mutable_rotation_peak_bytes = @max(self.read_snapshot_mutable_rotation_peak_bytes, mutable_bytes);
        if (self.shouldDeferCommitFlush()) self.scheduleImmutableFlushJob();
        self.notePotentialMaintenanceDebt();
    }

    pub fn snapshotMutableState(self: *Backend) !*const State {
        return try self.snapshotMutableStateWithReason(.other);
    }

    pub fn snapshotMutableStateWithReason(self: *Backend, reason: MutableSnapshotReason) !*const State {
        if (self.mutable_read_snapshot) |snapshot| return snapshot;
        if (self.mutable.entries.items.len == 0) return &self.empty_mutable_snapshot;
        const snapshot = try self.allocator.create(State);
        errdefer self.allocator.destroy(snapshot);
        snapshot.* = try self.mutable.clone(self.allocator);
        errdefer snapshot.deinit(self.allocator);
        const snapshot_bytes = estimateStateBytes(snapshot);
        self.mutable_snapshot_clone_calls +|= 1;
        self.mutable_snapshot_clone_bytes_total +|= snapshot_bytes;
        self.mutable_snapshot_clone_peak_bytes = @max(self.mutable_snapshot_clone_peak_bytes, snapshot_bytes);
        const reason_index = mutableSnapshotReasonIndex(reason);
        self.mutable_snapshot_clone_by_reason[reason_index].calls +|= 1;
        self.mutable_snapshot_clone_by_reason[reason_index].bytes_total +|= snapshot_bytes;
        self.mutable_snapshot_clone_by_reason[reason_index].peak_bytes = @max(self.mutable_snapshot_clone_by_reason[reason_index].peak_bytes, snapshot_bytes);
        try self.retired_mutable_snapshots.ensureUnusedCapacity(self.allocator, 1);
        self.mutable_read_snapshot = snapshot;
        return snapshot;
    }

    pub fn invalidateMutableReadSnapshot(self: *Backend) void {
        const snapshot = self.mutable_read_snapshot orelse return;
        self.mutable_read_snapshot = null;
        self.retireMutableSnapshot(snapshot);
    }

    fn destroyImmutableMemtable(self: *Backend, state: *State) void {
        state.deinit(self.allocator);
        self.allocator.destroy(state);
    }

    fn destroyMutableSnapshot(self: *Backend, state: *State) void {
        state.deinit(self.allocator);
        self.allocator.destroy(state);
    }

    fn retireImmutableMemtable(self: *Backend, state: *State) !void {
        if (self.active_readers == 0) {
            self.destroyImmutableMemtable(state);
            return;
        }
        try self.retired_immutable_memtables.append(self.allocator, state);
    }

    fn retireMutableSnapshot(self: *Backend, state: *State) void {
        if (self.active_readers == 0) {
            self.destroyMutableSnapshot(state);
            return;
        }
        self.retired_mutable_snapshots.append(self.allocator, state) catch {
            // Keep the old snapshot alive rather than risk invalidating
            // active readers when we cannot queue it for retirement.
        };
    }

    fn drainRetiredImmutableMemtables(self: *Backend) void {
        for (self.retired_immutable_memtables.items) |state| {
            self.destroyImmutableMemtable(state);
        }
        self.retired_immutable_memtables.clearRetainingCapacity();
    }

    fn drainRetiredMutableSnapshots(self: *Backend) void {
        for (self.retired_mutable_snapshots.items) |state| {
            self.destroyMutableSnapshot(state);
        }
        self.retired_mutable_snapshots.clearRetainingCapacity();
    }

    fn compactImmutableMemtableQueue(self: *Backend) void {
        if (self.immutable_head == 0) return;
        const active_count = self.activeImmutableMemtableCount();
        if (active_count > 0) {
            std.mem.copyForwards(*State, self.immutable_memtables.items[0..active_count], self.immutable_memtables.items[self.immutable_head..]);
            std.mem.copyForwards(WalSegmentRange, self.immutable_wal_ranges.items[0..active_count], self.immutable_wal_ranges.items[self.immutable_head..]);
        }
        self.immutable_memtables.items.len = active_count;
        self.immutable_wal_ranges.items.len = active_count;
        self.immutable_head = 0;
    }

    fn noteMutableWalSegment(self: *Backend, segment: u64) void {
        if (segment == 0) return;
        if (!self.mutable_wal_range.isSet()) {
            self.mutable_wal_range = .{ .first = segment, .last = segment };
            return;
        }
        self.mutable_wal_range.first = @min(self.mutable_wal_range.first, segment);
        self.mutable_wal_range.last = @max(self.mutable_wal_range.last, segment);
    }

    fn oldestActiveWalSegment(self: *const Backend) ?u64 {
        var oldest: ?u64 = if (self.mutable_wal_range.isSet()) self.mutable_wal_range.first else null;
        for (self.immutable_wal_ranges.items[self.immutable_head..]) |range| {
            if (!range.isSet()) continue;
            oldest = if (oldest) |current| @min(current, range.first) else range.first;
        }
        return oldest;
    }

    fn maybeCheckpointWalAfterManifestPublish(self: *Backend) !void {
        if (!self.options.wal_enabled or self.root_dir == null or self.options.backend.read_only) return;
        if (self.recovery_replaying_wal) return;
        if (self.mutable.entries.items.len == 0 and self.activeImmutableMemtableCount() == 0) {
            try self.resetWalAfterManifestCheckpoint();
            return;
        }
        const oldest_active = self.oldestActiveWalSegment() orelse return;
        if (oldest_active <= 1) return;
        try wal_mod.retireCoveredSegments(self.storage.?, self.allocator, self.root_dir.?, oldest_active - 1);
        self.syncTrackedWalRetentionUsageCurrentLocked();
    }

    fn estimateStateBytes(state: anytype) u64 {
        var total: u64 = 0;
        for (state.entries.items) |entry| {
            if (entry.namespace_name) |name| total += name.len;
            total += entry.key.len;
            total += entry.value.len;
            total += @sizeOf(state_mod.OwnedEntry);
        }
        return total;
    }

    fn estimatedFlushIoBytes(state: *const State) u64 {
        const bytes = estimateStateBytes(state);
        return bytes +| bytes;
    }

    fn maintenanceLevelRunTarget(level: u32, base: usize, multiplier: usize) usize {
        if (level == 0) return 0;
        var target = @max(@as(usize, 1), base);
        var remaining = level - 1;
        while (remaining > 0) : (remaining -= 1) {
            target = std.math.mul(usize, target, @max(@as(usize, 1), multiplier)) catch return std.math.maxInt(usize);
        }
        return target;
    }

    fn maintenanceLevelByteTarget(level: u32, base: usize, multiplier: usize) u64 {
        if (level == 0 or base == 0) return 0;
        var target: u64 = @intCast(base);
        var remaining = level - 1;
        while (remaining > 0) : (remaining -= 1) {
            const multiplier_u64: u64 = @intCast(@max(@as(usize, 1), multiplier));
            target = std.math.mul(u64, target, multiplier_u64) catch return std.math.maxInt(u64);
        }
        return target;
    }

    pub fn capabilities(_: *Backend) backend_types.Capabilities {
        return .{
            .ordered_ranges = true,
            .reverse_ranges = true,
            .cursors = true,
            .ordered_append_puts = true,
            .native_namespaces = false,
            .write_batches = .atomic,
            .single_writer = true,
            .read_snapshots = .snapshot,
        };
    }

    pub fn beginRead(self: *Backend) !NamespaceReadTxn {
        return try NamespaceReadTxn.open(self);
    }

    pub fn beginWrite(self: *Backend) !NamespaceWriteTxn {
        if (self.options.backend.read_only) return error.ReadOnly;
        return try NamespaceWriteTxn.open(self);
    }

    pub fn beginBatch(self: *Backend) !NamespaceWriteTxn {
        if (self.options.backend.read_only) return error.ReadOnly;
        return try NamespaceWriteTxn.open(self);
    }

    pub fn beginBatchWithOptions(self: *Backend, options: backend_types.BatchOptions) !NamespaceWriteTxn {
        if (self.options.backend.read_only) return error.ReadOnly;
        return try NamespaceWriteTxn.openWithOptions(self, options);
    }

    pub fn runtimeNamespaceStore(self: *Backend, allocator: Allocator) !backend_erased.NamespaceStore {
        return try backend_erased.namespaceStoreFrom(allocator, self, backend_types.Namespace, identityNamespace);
    }

    pub fn runtimeStore(
        self: *Backend,
        allocator: Allocator,
        namespace: backend_types.Namespace,
    ) !backend_erased.Store {
        return try backend_erased.storeFrom(allocator, BoundStore{
            .backend = self,
            .namespace = namespace,
        });
    }

    pub fn maybeFlushMutable(self: *Backend) !void {
        if (self.shouldFlushMutable()) {
            if (self.shouldDeferCommitFlush()) {
                try self.rotateMutableToImmutable();
                self.scheduleImmutableFlushJob();
                try self.enforceDeferredImmutableBackpressure();
            } else {
                try self.flushMutable();
            }
        }
        try self.enforceWalRetentionHardPressureGuarded();
    }

    fn shouldDeferCommitFlush(self: *const Backend) bool {
        if (self.root_dir == null) return false;
        if (!self.options.wal_enabled) return false;
        if (self.options.foreground_soft_compaction) return false;
        if (self.effectiveFlushThresholdBytes() > 0) return true;
        return self.options.defer_flush_on_commit;
    }

    fn enforceDeferredImmutableBackpressure(self: *Backend) !void {
        const limit = self.options.max_deferred_immutable_memtables;
        if (limit == 0) return;
        while (self.activeImmutableMemtableCount() > limit) {
            if (!try self.flushOldestImmutableMemtable()) {
                self.scheduleImmutableFlushJob();
                return;
            }
        }
    }

    pub fn prepareSplitRightToDir(self: *Backend, split_key: []const u8, dest_dir: []const u8, options: Options) !bool {
        if (self.root_dir == null) return false;
        if (self.options.backend.read_only) return error.ReadOnly;

        const locked = runtime_mod.lockBackend(Backend, self);
        defer runtime_mod.unlockBackend(Backend, self, locked);

        try self.flushMutable();

        var dest_options = options;
        dest_options.background_executor = null;
        var dest = try Backend.open(self.allocator, dest_dir, dest_options);
        defer dest.close();

        try clearRunsAndFiles(&dest);

        var wrote_any = false;
        for (self.runs.items) |*run| {
            switch (classifyRun(run.*, split_key)) {
                .left => {},
                .right => {
                    try dest.runs.append(self.allocator, try cloneRunForBackend(&dest, run.*));
                    wrote_any = true;
                },
                .overlap => {
                    const run_state = try self.resolveRunState(run);
                    var split = try run_state.splitAtKey(self.allocator, split_key);
                    defer split.deinit(self.allocator);
                    if (split.right.entries.items.len == 0) continue;
                    try dest.runs.append(self.allocator, try dest.makeRun(split.right));
                    split.right = .{};
                    wrote_any = true;
                },
            }
        }

        compaction_mod.sortRuns(dest.runs.items);
        try dest.persistManifest();
        return wrote_any;
    }

    pub fn rewriteLeftInPlace(self: *Backend, split_key: []const u8) !bool {
        if (self.root_dir == null) return false;
        if (self.options.backend.read_only) return error.ReadOnly;

        const locked = runtime_mod.lockBackend(Backend, self);
        defer runtime_mod.unlockBackend(Backend, self, locked);

        try self.flushMutable();

        const allocator = self.allocator;
        var old_runs = self.runs;
        self.runs = .empty;
        errdefer self.runs = old_runs;

        const RunAction = union(enum) {
            keep,
            drop,
            replace: Run,
        };

        var actions = try allocator.alloc(RunAction, old_runs.items.len);
        defer allocator.free(actions);
        var actions_initialized: usize = 0;
        errdefer {
            for (actions[0..actions_initialized]) |*action| {
                switch (action.*) {
                    .replace => |*run| run.deinit(allocator),
                    .keep, .drop => {},
                }
            }
        }

        var changed = false;
        for (old_runs.items, 0..) |*run, i| {
            switch (classifyRun(run.*, split_key)) {
                .left => actions[i] = .keep,
                .right => {
                    actions[i] = .drop;
                    changed = true;
                },
                .overlap => {
                    const run_state = try self.resolveRunStateWithAllocator(run, allocator);
                    var split = try run_state.splitAtKey(allocator, split_key);
                    defer split.deinit(allocator);
                    if (split.left.entries.items.len == 0) {
                        actions[i] = .drop;
                    } else {
                        actions[i] = .{ .replace = try self.makeRun(split.left) };
                        split.left = .{};
                    }
                    changed = true;
                },
            }
            actions_initialized = i + 1;
        }

        if (!changed) {
            self.runs = old_runs;
            return false;
        }

        var rewritten = std.ArrayListUnmanaged(Run).empty;
        errdefer {
            for (rewritten.items) |*run| run.deinit(allocator);
            rewritten.deinit(allocator);
        }
        try rewritten.ensureTotalCapacity(allocator, old_runs.items.len);

        var obsolete_runs = std.ArrayListUnmanaged(Run).empty;
        errdefer {
            for (obsolete_runs.items) |*run| run.deinit(allocator);
            obsolete_runs.deinit(allocator);
        }

        for (old_runs.items, 0..) |*run, i| {
            switch (actions[i]) {
                .keep => {
                    rewritten.appendAssumeCapacity(run.*);
                    run.* = undefined;
                },
                .drop => {
                    if (run.path) |path| try self.queueObsoleteFilePath(try allocator.dupe(u8, path));
                    try obsolete_runs.append(allocator, run.*);
                    run.* = undefined;
                },
                .replace => |replacement| {
                    rewritten.appendAssumeCapacity(replacement);
                    if (run.path) |path| try self.queueObsoleteFilePath(try allocator.dupe(u8, path));
                    try obsolete_runs.append(allocator, run.*);
                    run.* = undefined;
                },
            }
        }

        old_runs.deinit(allocator);
        compaction_mod.sortRuns(rewritten.items);
        self.runs = rewritten;
        try self.queueObsoleteRuns(obsolete_runs);
        try self.persistManifest();
        return true;
    }

    fn flushMutable(self: *Backend) !void {
        if (self.mutable.entries.items.len > 0) {
            try self.rotateMutableToImmutable();
        }
        try self.flushAllImmutableMemtables();
    }

    fn directIngestMutableAtBulkFinishIfPossible(self: *Backend) !bool {
        if (!self.options.direct_bulk_ingest) return false;
        if (self.mutable.entries.items.len == 0) return false;
        if (self.activeImmutableMemtableCount() != 0) return false;
        self.invalidateMutableReadSnapshot();
        var sorted = try self.mutable.toStateMove(self.allocator);
        errdefer sorted.deinit(self.allocator);
        self.mutable_wal_range = .{};
        try self.ingestOwnedSortedState(&sorted);
        sorted.deinit(self.allocator);
        self.syncTrackedInMemoryStateUsageCurrentLocked();
        return true;
    }

    pub fn drainMutableBeforeBulkAppendDirectIngest(self: *Backend) !bool {
        if (!self.options.direct_bulk_ingest) return false;
        if (self.mutable.entries.items.len == 0) return true;
        if (self.activeImmutableMemtableCount() != 0) return false;
        self.invalidateMutableReadSnapshot();
        var sorted = try self.mutable.toStateMove(self.allocator);
        errdefer sorted.deinit(self.allocator);
        self.mutable_wal_range = .{};
        try self.ingestSortedState(&sorted);
        sorted.deinit(self.allocator);
        self.syncTrackedInMemoryStateUsageCurrentLocked();
        return true;
    }

    fn rotateMutableToImmutable(self: *Backend) !void {
        if (self.mutable.entries.items.len == 0) return;
        self.invalidateMutableReadSnapshot();
        const rotated = try self.allocator.create(State);
        errdefer self.allocator.destroy(rotated);
        try self.immutable_memtables.ensureUnusedCapacity(self.allocator, 1);
        try self.immutable_wal_ranges.ensureUnusedCapacity(self.allocator, 1);
        rotated.* = try self.mutable.toStateMove(self.allocator);
        self.immutable_memtables.appendAssumeCapacity(rotated);
        self.immutable_wal_ranges.appendAssumeCapacity(self.mutable_wal_range);
        self.mutable_wal_range = .{};
        self.write_stats.immutable_rotations += 1;
        self.syncTrackedInMemoryStateUsageCurrentLocked();
    }

    fn flushAllImmutableMemtables(self: *Backend) !void {
        while (try self.flushOldestImmutableMemtable()) {}
    }

    fn scheduleImmutableFlushJob(self: *Backend) void {
        if (self.activeImmutableMemtableCount() == 0) return;
        if (self.immutable_flush_job_in_flight) return;
        if (!self.background_executor.canRunDetached()) return;

        self.immutable_flush_job_in_flight = true;
        self.background_executor.submit(.commit_durable, self, runImmutableFlushJob, deinitImmutableFlushJob) catch |err| {
            self.immutable_flush_job_in_flight = false;
            std.log.warn("lsm immutable flush background scheduling failed root={?s} err={}", .{ self.root_dir, err });
        };
    }

    fn runImmutableFlushJob(ptr: *anyopaque) !void {
        const self: *Backend = @ptrCast(@alignCast(ptr));
        const locked = runtime_mod.lockBackend(Backend, self);
        defer runtime_mod.unlockBackend(Backend, self, locked);
        defer self.immutable_flush_job_in_flight = false;
        if (self.options.backend.read_only) return;
        try self.flushAllImmutableMemtables();
        _ = self.refreshCachedMaintenanceHintLocked();
    }

    fn deinitImmutableFlushJob(_: *anyopaque) void {}

    fn flushOldestImmutableMemtable(self: *Backend) !bool {
        if (self.activeImmutableMemtableCount() == 0) return false;
        const state = self.immutable_memtables.items[self.immutable_head];
        if (!self.tryReserveMaintenanceIoBudget(estimatedFlushIoBytes(state))) return false;
        if (self.root_dir != null and self.storage != null) {
            return try self.flushOldestImmutableMemtableUnlockedBuild();
        }
        const start_ns = self.writeStatsNowNs();
        const input_entries = state.entries.items.len;

        var new_runs = try compaction_mod.makeRunsFromStateBorrowed(Backend, self, state);
        errdefer {
            for (new_runs.items) |*run| run.deinit(self.allocator);
            new_runs.deinit(self.allocator);
        }
        const elapsed_ns = self.writeStatsElapsedNs(start_ns);
        self.recordFlushWriteStats(input_entries, new_runs.items, elapsed_ns);
        self.write_stats.immutable_flushes += 1;
        self.write_stats.immutable_flush_entries += @intCast(input_entries);
        self.write_stats.immutable_flush_ns += elapsed_ns;
        try compaction_mod.appendOwnedRuns(&self.runs, self.allocator, &new_runs);
        self.immutable_head += 1;
        try self.retireImmutableMemtable(state);
        self.compactImmutableMemtableQueue();
        self.syncTrackedInMemoryStateUsageCurrentLocked();

        compaction_mod.sortRuns(self.runs.items);
        if (self.bulkIngestActive()) {
            self.markManifestDirty();
            return true;
        }
        try self.enforceWritePressure();
        if (self.options.foreground_soft_compaction) {
            try self.maybeCompactRuns();
        }
        if (self.root_dir != null) {
            try self.persistManifest();
        }
        return true;
    }

    fn flushOldestImmutableMemtableUnlockedBuild(self: *Backend) !bool {
        if (self.activeImmutableMemtableCount() == 0) return false;
        if (self.immutable_flush_build_in_flight) return false;

        const start_ns = self.writeStatsNowNs();
        const publish_head = self.immutable_head;
        const state = self.immutable_memtables.items[publish_head];
        const input_entries = state.entries.items.len;
        const reserved_run_ids = @max(@as(u64, 1), @as(u64, @intCast(input_entries)));
        const reserved_run_id_start = self.next_run_id;
        self.next_run_id +|= reserved_run_ids;
        const reserved_run_id_end = self.next_run_id;
        self.immutable_flush_build_in_flight = true;

        runtime_mod.unlockBackend(Backend, self, true);

        var build_result: std.ArrayListUnmanaged(Run) = .empty;
        var build_result_valid = false;
        var build_err: ?anyerror = null;
        build_result = compaction_mod.buildRunsFromStateBorrowedWithReservedIds(
            Backend,
            self,
            state,
            reserved_run_id_start,
            reserved_run_id_end,
        ) catch |err| blk: {
            build_err = err;
            break :blk .empty;
        };
        if (build_err == null) build_result_valid = true;

        const relocked = runtime_mod.lockBackend(Backend, self);
        std.debug.assert(relocked);
        defer self.immutable_flush_build_in_flight = false;
        errdefer if (build_result_valid) compaction_mod.discardOutputRuns(Backend, self, &build_result);
        if (build_err) |err| return err;

        if (publish_head != self.immutable_head or
            self.activeImmutableMemtableCount() == 0 or
            self.immutable_memtables.items[publish_head] != state)
        {
            compaction_mod.discardOutputRuns(Backend, self, &build_result);
            return false;
        }

        const elapsed_ns = self.writeStatsElapsedNs(start_ns);
        self.recordFlushWriteStats(input_entries, build_result.items, elapsed_ns);
        self.write_stats.immutable_flushes += 1;
        self.write_stats.immutable_flush_entries += @intCast(input_entries);
        self.write_stats.immutable_flush_ns += elapsed_ns;
        try compaction_mod.appendOwnedRuns(&self.runs, self.allocator, &build_result);
        self.immutable_head += 1;
        try self.retireImmutableMemtable(state);
        self.compactImmutableMemtableQueue();
        self.syncTrackedInMemoryStateUsageCurrentLocked();

        compaction_mod.sortRuns(self.runs.items);
        if (self.bulkIngestActive()) {
            self.markManifestDirty();
            return true;
        }
        try self.enforceWritePressure();
        if (self.options.foreground_soft_compaction) {
            try self.maybeCompactRuns();
        }
        if (self.root_dir != null) {
            try self.persistManifest();
        }
        return true;
    }

    fn maybeCompactRuns(self: *Backend) !void {
        try compaction_mod.maybeCompactRuns(Backend, self);
    }

    fn compactOldestPair(self: *Backend) !void {
        try compaction_mod.compactOldestPair(Backend, self);
    }

    pub fn getMergedWithMutable(
        self: *Backend,
        mutable: anytype,
        namespace: backend_types.Namespace,
        key: []const u8,
    ) ![]const u8 {
        if (mutable.findIndex(namespace, key)) |idx| {
            const entry = mutable.entries.items[idx];
            if (entry.tombstone) return error.NotFound;
            return entry.value;
        }
        var immutable_index = self.immutable_memtables.items.len;
        while (immutable_index > self.immutable_head) {
            immutable_index -= 1;
            const immutable = self.immutable_memtables.items[immutable_index];
            if (immutable.findIndex(namespace, key)) |idx| {
                const entry = immutable.entries.items[idx];
                if (entry.tombstone) return error.NotFound;
                return entry.value;
            }
        }

        var run_index: usize = 0;
        while (run_index < self.runs.items.len and self.runs.items[run_index].level == 0) : (run_index += 1) {
            if (try self.getFromRunForPoint(&self.runs.items[run_index], namespace, key)) |value| return value;
        }

        while (run_index < self.runs.items.len) {
            const level = self.runs.items[run_index].level;
            const level_start = run_index;
            while (run_index < self.runs.items.len and self.runs.items[run_index].level == level) : (run_index += 1) {}
            const candidate = findRunIndexInSortedLevel(self.runs.items[level_start..run_index], namespace, key) orelse continue;
            if (try self.getFromRunForPoint(&self.runs.items[level_start + candidate], namespace, key)) |value| return value;
        }
        return error.NotFound;
    }

    fn getFromRunForPoint(self: *Backend, run: *Run, namespace: backend_types.Namespace, key: []const u8) !?[]const u8 {
        if (!runMayContain(run.*, namespace, key)) return null;
        if (!lsm_table_file.maybeContains(try run.ensureBloomFilter(self.allocator), namespace.name, key)) {
            return null;
        }
        const state = try self.resolveRunState(run);
        if (state.findIndex(namespace, key)) |idx| {
            const entry = state.entries.items[idx];
            if (entry.tombstone) return error.NotFound;
            return entry.value;
        }
        return null;
    }

    pub fn getMergedWithOverlay(
        self: *Backend,
        base_mutable: anytype,
        overlay: anytype,
        namespace: backend_types.Namespace,
        key: []const u8,
    ) ![]const u8 {
        if (overlay.findIndex(namespace, key)) |idx| {
            const entry = overlay.entries.items[idx];
            if (entry.tombstone) return error.NotFound;
            return entry.value;
        }
        return try self.getMergedWithMutable(base_mutable, namespace, key);
    }

    pub fn materializeVisibleState(self: *Backend) !State {
        return try self.materializeVisibleStateWithMutable(&self.mutable);
    }

    pub fn materializeVisibleStateWithMutable(self: *Backend, mutable: anytype) !State {
        var out: State = .{};
        errdefer out.deinit(self.allocator);

        var run_index = self.runs.items.len;
        while (run_index > 0) {
            run_index -= 1;
            const state = try self.resolveRunState(&self.runs.items[run_index]);
            try state_mod.applyState(&out, self.allocator, state);
        }
        for (self.activeImmutableMemtables()) |immutable| {
            try state_mod.applyState(&out, self.allocator, immutable);
        }
        try state_mod.applyState(&out, self.allocator, mutable);
        try state_mod.stripTombstones(&out, self.allocator);
        return out;
    }

    pub fn cloneVisibleMutableState(self: *Backend) !State {
        var out: State = .{};
        errdefer out.deinit(self.allocator);
        for (self.activeImmutableMemtables()) |immutable| {
            try state_mod.applyState(&out, self.allocator, immutable);
        }
        try state_mod.applyState(&out, self.allocator, &self.mutable);
        return out;
    }

    pub fn materializeVisibleStateWithOverlay(
        self: *Backend,
        base_mutable: anytype,
        overlay: anytype,
    ) !State {
        var out = try self.materializeVisibleStateWithMutable(base_mutable);
        errdefer out.deinit(self.allocator);
        try state_mod.applyState(&out, self.allocator, overlay);
        try state_mod.stripTombstones(&out, self.allocator);
        return out;
    }

    fn makeRun(self: *Backend, state: State) !Run {
        return try compaction_mod.makeRun(Backend, self, state);
    }

    pub fn ingestSortedTableEntries(self: *Backend, entries: []const TableEntry) !void {
        if (self.options.backend.read_only) return error.ReadOnly;
        if (entries.len == 0) return;

        if (self.mutable.entries.items.len > 0) {
            try self.flushMutable();
        }

        const start_ns = self.writeStatsNowNs();
        var new_runs = try compaction_mod.makeRunsFromSortedTableEntries(Backend, self, entries);
        errdefer {
            for (new_runs.items) |*run| run.deinit(self.allocator);
            new_runs.deinit(self.allocator);
        }

        self.recordSortedIngestWriteStats(new_runs.items, self.writeStatsElapsedNs(start_ns));
        try compaction_mod.appendOwnedRuns(&self.runs, self.allocator, &new_runs);

        compaction_mod.sortRuns(self.runs.items);
        if (self.root_dir != null) {
            if (self.bulkIngestActive()) {
                self.markManifestDirty();
            } else {
                try self.persistManifest();
            }
        }
    }

    pub fn ingestSortedState(self: *Backend, state: *const State) !void {
        if (state.entries.items.len == 0) return;
        var entries = try self.allocator.alloc(TableEntry, state.entries.items.len);
        defer self.allocator.free(entries);
        for (state.entries.items, 0..) |entry, i| {
            entries[i] = .{
                .namespace_name = entry.namespace_name,
                .key = entry.key,
                .value = entry.value,
                .tombstone = entry.tombstone,
            };
        }
        try self.ingestSortedTableEntries(entries);
    }

    pub fn ingestOwnedSortedState(self: *Backend, state: *State) !void {
        if (self.options.backend.read_only) return error.ReadOnly;
        if (state.entries.items.len == 0) return;
        if (self.root_dir != null) {
            try self.ingestSortedState(state);
            return;
        }

        if (self.mutable.entries.items.len > 0) {
            try self.flushMutable();
        }

        const start_ns = self.writeStatsNowNs();
        const target_bytes = @max(@as(usize, 1), @min(self.options.max_run_file_bytes, lsm_table_file.max_entry_data_len));
        var new_runs: std.ArrayListUnmanaged(Run) = .empty;
        if (state.arena_owner != null and estimateStateBytes(state) <= target_bytes) {
            var moved = state.*;
            state.* = .{};
            errdefer moved.deinit(self.allocator);
            try new_runs.ensureUnusedCapacity(self.allocator, 1);
            const run = try compaction_mod.makeRun(Backend, self, moved);
            new_runs.appendAssumeCapacity(run);
        } else if (state.arena_owner != null) {
            try self.ingestSortedState(state);
            return;
        } else {
            new_runs = try compaction_mod.makeRuns(Backend, self, state);
        }
        errdefer {
            for (new_runs.items) |*run| run.deinit(self.allocator);
            new_runs.deinit(self.allocator);
        }

        self.recordSortedIngestWriteStats(new_runs.items, self.writeStatsElapsedNs(start_ns));
        try compaction_mod.appendOwnedRuns(&self.runs, self.allocator, &new_runs);

        compaction_mod.sortRuns(self.runs.items);
    }

    pub fn shouldDirectIngestBulkState(self: *const Backend, state: *const State) bool {
        if (!self.options.direct_bulk_ingest) return false;
        if (self.activeImmutableMemtableCount() != 0) return false;
        return self.stateMeetsBulkFlushThreshold(state);
    }

    pub fn shouldDirectIngestBulkMutable(self: *const Backend, mutable: *const ActiveMemTable) bool {
        if (!self.options.direct_bulk_ingest) return false;
        if (self.activeImmutableMemtableCount() != 0) return false;
        const byte_threshold = self.effectiveFlushThresholdBytes();
        if (byte_threshold > 0) return estimateStateBytes(mutable) >= byte_threshold;
        return mutable.entries.items.len >= self.effectiveFlushThreshold();
    }

    pub fn persistManifest(self: *Backend) !void {
        const root_dir = self.root_dir orelse return;
        const start_ns = self.writeStatsNowNs();
        self.obsolete_manifest_dirty = try self.reconcileObsoletePathsForManifest();
        try validateRunLayoutForManifest(self.runs.items);
        const bytes = try repository_mod.persistManifestWithStorageCount(
            self.storage.?,
            self.allocator,
            root_dir,
            self.next_run_id,
            self.runs.items,
            self.obsolete_paths.items,
        );
        self.write_stats.manifest_writes += 1;
        self.write_stats.manifest_bytes += bytes;
        self.write_stats.manifest_ns += self.writeStatsElapsedNs(start_ns);
        try self.maybeCheckpointWalAfterManifestPublish();
        self.manifest_dirty = false;
    }

    pub fn appendWalForState(self: *Backend, state: *const State) !void {
        try self.appendWalForMutable(state);
    }

    pub fn appendWalForMutable(self: *Backend, state: anytype) !void {
        if (!self.options.wal_enabled or
            self.root_dir == null or
            self.options.backend.read_only or
            state.entries.items.len == 0) return;

        const start_ns = self.writeStatsNowNs();
        var wal_write_bytes: u64 = 0;
        if (self.options.resource_manager) |manager| {
            manager.observeUsage(
                .lsm_wal_write_working_set,
                &wal_write_bytes,
                @intCast(wal_mod.encodedStateRecordLen(state)),
            );
        }
        defer if (self.options.resource_manager) |manager| {
            manager.observeUsage(.lsm_wal_write_working_set, &wal_write_bytes, 0);
        };
        const bytes = try wal_mod.appendStateWithOptions(
            self.storage.?,
            self.allocator,
            self.root_dir.?,
            state,
            self.options.wal_sync_on_commit,
            .{ .segment_bytes = self.options.wal_segment_bytes },
        );
        self.noteMutableWalSegment(try wal_mod.currentSegment(self.storage.?, self.allocator, self.root_dir.?));
        self.syncTrackedWalRetentionUsageCurrentLocked();
        self.write_stats.wal_append_records += 1;
        self.write_stats.wal_append_entries += @intCast(state.entries.items.len);
        self.write_stats.wal_append_bytes += bytes;
        const append_ns = self.writeStatsElapsedNs(start_ns);
        self.write_stats.wal_append_ns += append_ns;
        if (self.options.wal_sync_on_commit) {
            self.write_stats.wal_sync_records += 1;
            self.write_stats.wal_sync_ns += append_ns;
        }
    }

    pub fn replayWalIntoMutable(self: *Backend) !void {
        if (!self.options.wal_enabled or self.root_dir == null) return;
        const start_ns = self.writeStatsNowNs();
        const before_manifest_writes = self.write_stats.manifest_writes;
        self.recovery_replaying_wal = true;
        errdefer self.recovery_replaying_wal = false;
        const replay_hooks: ?wal_mod.ReplayHooks = if (!self.options.backend.read_only)
            .{
                .ctx = @ptrCast(self),
                .entry_allocator = replayWalEntryAllocatorHook,
                .on_applied_record = replayWalAppliedRecordHook,
            }
        else
            null;
        const stats = try wal_mod.replayIntoMutableWithHooks(
            self.storage.?,
            self.allocator,
            self.root_dir.?,
            &self.mutable,
            replay_hooks,
        );
        self.recovery_replaying_wal = false;
        if (!self.options.backend.read_only and self.write_stats.manifest_writes != before_manifest_writes) {
            try self.maybeCheckpointWalAfterManifestPublish();
        }
        const retention = try wal_mod.snapshotRetention(self.storage.?, self.allocator, self.root_dir.?);
        self.mutable_wal_range = if (retention.segments == 0 or self.mutable.entries.items.len == 0)
            .{}
        else
            .{
                .first = retention.oldest_retained_segment,
                .last = retention.current_segment,
            };
        self.syncTrackedWalRetentionUsageCurrentLocked();
        self.write_stats.wal_replay_records += stats.records;
        self.write_stats.wal_replay_entries += stats.entries;
        self.write_stats.wal_replay_bytes += stats.bytes;
        self.write_stats.wal_replay_ns += self.writeStatsElapsedNs(start_ns);
        self.write_stats.wal_replay_truncated_tail_bytes += stats.truncated_tail_bytes;
    }

    fn replayWalAppliedRecordHook(ctx: *anyopaque, segment: u64, _: u64) anyerror!void {
        const self: *Backend = @ptrCast(@alignCast(ctx));
        if (segment != 0) self.noteMutableWalSegment(segment);
        if (!self.shouldFlushMutable()) return;
        try self.flushMutable();
    }

    fn replayWalEntryAllocatorHook(ctx: *anyopaque, default_allocator: Allocator) anyerror!Allocator {
        const self: *Backend = @ptrCast(@alignCast(ctx));
        _ = try self.mutable.ensureRecoveryAllocator(default_allocator);
        return default_allocator;
    }

    fn resetWalAfterManifestCheckpoint(self: *Backend) !void {
        if (!self.options.wal_enabled or self.root_dir == null or self.options.backend.read_only) return;
        const start_ns = self.writeStatsNowNs();
        try wal_mod.reset(self.storage.?, self.allocator, self.root_dir.?);
        self.syncTrackedWalRetentionUsageCurrentLocked();
        self.write_stats.wal_resets += 1;
        self.write_stats.wal_reset_ns += self.writeStatsElapsedNs(start_ns);
    }

    pub fn writeStatsNowNs(_: *Backend) u64 {
        return platform_time.monotonicNs();
    }

    fn writeStatsElapsedNs(self: *Backend, start_ns: u64) u64 {
        const end_ns = self.writeStatsNowNs();
        return if (end_ns >= start_ns) end_ns - start_ns else 0;
    }

    pub fn recordFlushWriteStats(self: *Backend, input_entries: usize, output_runs: []const Run, elapsed_ns: u64) void {
        self.write_stats.flushes += 1;
        self.write_stats.flush_input_entries += input_entries;
        self.write_stats.flush_ns += elapsed_ns;
        for (output_runs) |run| {
            self.write_stats.flush_output_runs += 1;
            self.write_stats.flush_output_bytes += run.size_bytes;
            if (run.path != null) {
                self.write_stats.table_file_writes += 1;
                self.write_stats.table_file_bytes += run.size_bytes;
                self.recordTableCompressionWriteStats(run.compression_stats);
            }
        }
    }

    pub fn recordCompactionWriteStats(self: *Backend, output_runs: []const Run, elapsed_ns: u64) void {
        self.write_stats.compaction_ns += elapsed_ns;
        for (output_runs) |run| {
            if (run.path != null) {
                self.write_stats.table_file_writes += 1;
                self.write_stats.table_file_bytes += run.size_bytes;
                self.recordTableCompressionWriteStats(run.compression_stats);
            }
        }
    }

    fn recordSortedIngestWriteStats(self: *Backend, output_runs: []const Run, elapsed_ns: u64) void {
        self.write_stats.sorted_ingest_ns += elapsed_ns;
        for (output_runs) |run| {
            self.write_stats.sorted_ingest_runs += 1;
            self.write_stats.sorted_ingest_bytes += run.size_bytes;
            if (run.path != null) {
                self.write_stats.table_file_writes += 1;
                self.write_stats.table_file_bytes += run.size_bytes;
                self.recordTableCompressionWriteStats(run.compression_stats);
            }
        }
    }

    pub fn recordBulkAppendAttempt(self: *Backend, entries: usize) void {
        self.write_stats.bulk_append_attempts +|= 1;
        self.write_stats.bulk_append_entries +|= @intCast(entries);
    }

    pub fn recordBulkAppendFallbackNonBulk(self: *Backend, entries: usize) void {
        self.write_stats.bulk_append_fallback_non_bulk +|= 1;
        self.write_stats.bulk_append_fallback_to_mutable_entries +|= @intCast(entries);
    }

    pub fn recordBulkAppendFallbackUnsupported(self: *Backend, entries: usize) void {
        self.write_stats.bulk_append_fallback_unsupported +|= 1;
        self.write_stats.bulk_append_fallback_to_mutable_entries +|= @intCast(entries);
    }

    pub fn recordBulkAppendFallbackBackendPending(self: *Backend, entries: usize) void {
        self.write_stats.bulk_append_fallback_backend_pending +|= 1;
        self.write_stats.bulk_append_fallback_to_mutable_entries +|= @intCast(entries);
    }

    pub fn recordBulkAppendFallbackDuplicateKeys(self: *Backend, entries: usize, sort_ns: u64) void {
        self.write_stats.bulk_append_fallback_duplicate_keys +|= 1;
        self.write_stats.bulk_append_fallback_to_mutable_entries +|= @intCast(entries);
        self.write_stats.bulk_append_sort_ns +|= sort_ns;
    }

    pub fn recordBulkAppendFallbackBelowThreshold(self: *Backend, entries: usize, sort_ns: u64) void {
        self.write_stats.bulk_append_fallback_below_threshold +|= 1;
        self.write_stats.bulk_append_fallback_to_mutable_entries +|= @intCast(entries);
        self.write_stats.bulk_append_sort_ns +|= sort_ns;
    }

    pub fn recordBulkAppendSuccess(self: *Backend, entries: usize, sort_ns: u64) void {
        self.write_stats.bulk_append_direct_successes +|= 1;
        self.write_stats.bulk_append_direct_entries +|= @intCast(entries);
        self.write_stats.bulk_append_sort_ns +|= sort_ns;
    }

    pub fn recordDirectBulkIngestAttempt(self: *Backend, entries: usize) void {
        self.write_stats.direct_bulk_ingest_attempts +|= 1;
        self.write_stats.direct_bulk_ingest_entries +|= @intCast(entries);
    }

    pub fn recordDirectBulkIngestFallbackUnsupported(self: *Backend) void {
        self.write_stats.direct_bulk_ingest_fallback_unsupported +|= 1;
    }

    pub fn recordDirectBulkIngestFallbackBackendMutable(self: *Backend) void {
        self.write_stats.direct_bulk_ingest_fallback_backend_mutable +|= 1;
    }

    pub fn recordDirectBulkIngestFallbackBelowThreshold(self: *Backend) void {
        self.write_stats.direct_bulk_ingest_fallback_below_threshold +|= 1;
    }

    pub fn recordDirectBulkIngestSuccess(self: *Backend, entries: usize, sort_ns: u64) void {
        self.write_stats.direct_bulk_ingest_successes +|= 1;
        self.write_stats.direct_bulk_ingest_entries_direct +|= @intCast(entries);
        self.write_stats.direct_bulk_ingest_sort_ns +|= sort_ns;
    }

    fn recordTableCompressionWriteStats(self: *Backend, stats: lsm_table_file.CompressionStats) void {
        self.write_stats.table_file_logical_entry_bytes +|= stats.logical_entry_bytes;
        self.write_stats.table_file_physical_entry_bytes +|= stats.physical_entry_bytes;
        self.write_stats.table_file_raw_blocks +|= stats.raw_blocks;
        self.write_stats.table_file_compressed_blocks +|= stats.compressed_blocks;
        self.write_stats.table_file_compression_codec_mask |= stats.compression_codec_mask;
    }

    pub fn resolveRunState(self: *Backend, run: *Run) !*const State {
        return try self.resolveRunStateWithAllocator(run, self.allocator);
    }

    fn resolveRunStateWithAllocator(self: *Backend, run: *Run, allocator: Allocator) !*const State {
        if (self.storage != null) {
            if (run.path) |path| {
                if (run.cached_state_index) |index| {
                    if (!self.cachedRunStateIndexMatches(index, path, run.id)) {
                        run.cached_state_index = null;
                    }
                }
                if (run.cached_state_index == null) {
                    run.cached_state_index = try self.getCachedRunStateIndex(path, run.id);
                }
                return self.getCachedRunStateByIndex(run.cached_state_index.?);
            }
        }
        if (self.storage) |storage| return try run.ensureStateWithStorage(allocator, storage);
        return try run.ensureState(allocator);
    }

    pub fn cachedRunStateIndexMatches(self: *const Backend, index: usize, path: []const u8, run_id: u64) bool {
        return index < self.run_state_cache.items.len and
            self.run_state_cache.items[index].run_id == run_id and
            std.mem.eql(u8, self.run_state_cache.items[index].path, path);
    }

    pub fn retainReader(self: *Backend) void {
        self.active_readers += 1;
    }

    pub fn recordPointGet(self: *Backend) void {
        _ = self.read_stats.point_gets.fetchAdd(1, .monotonic);
    }

    pub fn recordPointGets(self: *Backend, count: usize) void {
        if (count == 0) return;
        _ = self.read_stats.point_gets.fetchAdd(@intCast(count), .monotonic);
    }

    pub fn recordGetManySorted(self: *Backend, key_count: usize) void {
        _ = self.read_stats.get_many_sorted_calls.fetchAdd(1, .monotonic);
        _ = self.read_stats.get_many_sorted_keys.fetchAdd(@intCast(key_count), .monotonic);
    }

    pub fn recordGetManySortedResults(self: *Backend, hits: usize, misses: usize) void {
        _ = self.read_stats.get_many_sorted_hits.fetchAdd(@intCast(hits), .monotonic);
        _ = self.read_stats.get_many_sorted_misses.fetchAdd(@intCast(misses), .monotonic);
    }

    pub const GetManySortedPlan = enum {
        point,
        sorted_by_run,
        cursor,
    };

    pub fn recordGetManySortedPlan(self: *Backend, plan: GetManySortedPlan) void {
        const counter = switch (plan) {
            .point => &self.read_stats.get_many_sorted_plan_point,
            .sorted_by_run => &self.read_stats.get_many_sorted_plan_sorted_by_run,
            .cursor => &self.read_stats.get_many_sorted_plan_cursor,
        };
        _ = counter.fetchAdd(1, .monotonic);
    }

    pub fn recordGetManySortedLocality(self: *Backend, keys: []const []const u8) void {
        if (keys.len < 2) return;
        var monotonic_pairs: u64 = 0;
        var duplicate_pairs: u64 = 0;
        var out_of_order_pairs: u64 = 0;
        for (keys[1..], 0..) |key, i| {
            const prev = keys[i];
            switch (std.mem.order(u8, prev, key)) {
                .lt => monotonic_pairs += 1,
                .eq => {
                    monotonic_pairs += 1;
                    duplicate_pairs += 1;
                },
                .gt => out_of_order_pairs += 1,
            }
        }
        _ = self.read_stats.get_many_sorted_monotonic_pairs.fetchAdd(monotonic_pairs, .monotonic);
        _ = self.read_stats.get_many_sorted_duplicate_pairs.fetchAdd(duplicate_pairs, .monotonic);
        _ = self.read_stats.get_many_sorted_out_of_order_pairs.fetchAdd(out_of_order_pairs, .monotonic);
    }

    pub fn recordMutableHit(self: *Backend) void {
        _ = self.read_stats.mutable_hits.fetchAdd(1, .monotonic);
    }

    pub fn recordL0Hit(self: *Backend) void {
        _ = self.read_stats.l0_hits.fetchAdd(1, .monotonic);
    }

    pub fn recordLevelHit(self: *Backend) void {
        _ = self.read_stats.level_hits.fetchAdd(1, .monotonic);
    }

    pub fn recordRunProbe(self: *Backend) void {
        _ = self.read_stats.run_probes.fetchAdd(1, .monotonic);
    }

    pub fn recordBloomNegative(self: *Backend) void {
        _ = self.read_stats.bloom_negatives.fetchAdd(1, .monotonic);
    }

    pub fn recordReadHintAttempt(self: *Backend) void {
        _ = self.read_stats.read_hint_attempts.fetchAdd(1, .monotonic);
    }

    pub fn recordReadHintHit(self: *Backend) void {
        _ = self.read_stats.read_hint_hits.fetchAdd(1, .monotonic);
    }

    pub fn recordReadHintMiss(self: *Backend) void {
        _ = self.read_stats.read_hint_misses.fetchAdd(1, .monotonic);
    }

    pub fn readStatsNowNs(_: *Backend) u64 {
        return platform_time.monotonicNs();
    }

    pub fn readStatsElapsedNs(self: *Backend, start_ns: u64) u64 {
        const end_ns = self.readStatsNowNs();
        return if (end_ns >= start_ns) end_ns - start_ns else 0;
    }

    pub fn recordTableEntryParse(self: *Backend, elapsed_ns: u64) void {
        _ = self.read_stats.table_entry_parses.fetchAdd(1, .monotonic);
        _ = self.read_stats.table_entry_parse_ns.fetchAdd(elapsed_ns, .monotonic);
    }

    pub fn recordTableIndexLoad(self: *Backend, elapsed_ns: u64) void {
        _ = self.read_stats.table_index_loads.fetchAdd(1, .monotonic);
        _ = self.read_stats.table_index_load_ns.fetchAdd(elapsed_ns, .monotonic);
    }

    pub fn recordTableIndexDecode(self: *Backend, elapsed_ns: u64) void {
        _ = self.read_stats.table_index_decodes.fetchAdd(1, .monotonic);
        _ = self.read_stats.table_index_decode_ns.fetchAdd(elapsed_ns, .monotonic);
    }

    pub fn recordTableBlockLoad(self: *Backend, bytes: usize, elapsed_ns: u64) void {
        _ = self.read_stats.table_block_loads.fetchAdd(1, .monotonic);
        _ = self.read_stats.table_block_bytes.fetchAdd(@intCast(bytes), .monotonic);
        _ = self.read_stats.table_block_load_ns.fetchAdd(elapsed_ns, .monotonic);
    }

    pub fn recordSharedBlockCacheHit(self: *Backend) void {
        _ = self.read_stats.shared_block_cache_hits.fetchAdd(1, .monotonic);
    }

    pub fn recordSharedBlockCacheMiss(self: *Backend) void {
        _ = self.read_stats.shared_block_cache_misses.fetchAdd(1, .monotonic);
    }

    pub fn recordLocalBlockCacheHit(self: *Backend) void {
        _ = self.read_stats.local_block_cache_hits.fetchAdd(1, .monotonic);
    }

    pub fn recordLocalBlockCacheMiss(self: *Backend) void {
        _ = self.read_stats.local_block_cache_misses.fetchAdd(1, .monotonic);
    }

    pub fn localBlockCacheEnabled(self: *const Backend) bool {
        return self.options.local_block_cache_enabled;
    }

    pub fn recordCursorBlockReuse(self: *Backend) void {
        _ = self.read_stats.cursor_block_reuses.fetchAdd(1, .monotonic);
    }

    pub fn recordCursorBlockLoad(self: *Backend) void {
        _ = self.read_stats.cursor_block_loads.fetchAdd(1, .monotonic);
    }

    pub fn recordCursorBlockReadahead(self: *Backend) void {
        _ = self.read_stats.cursor_block_readaheads.fetchAdd(1, .monotonic);
    }

    pub fn recordCursorTableIndexHit(self: *Backend) void {
        _ = self.read_stats.cursor_table_index_hits.fetchAdd(1, .monotonic);
    }

    pub fn recordCursorTableIndexMiss(self: *Backend) void {
        _ = self.read_stats.cursor_table_index_misses.fetchAdd(1, .monotonic);
    }

    pub fn recordCursorValueBorrow(self: *Backend) void {
        _ = self.read_stats.cursor_value_borrows.fetchAdd(1, .monotonic);
    }

    pub fn recordCursorValueCopy(self: *Backend) void {
        _ = self.read_stats.cursor_value_copies.fetchAdd(1, .monotonic);
    }

    pub fn recordPointValueBorrow(self: *Backend) void {
        _ = self.read_stats.point_value_borrows.fetchAdd(1, .monotonic);
    }

    pub fn recordPointValueCopy(self: *Backend) void {
        _ = self.read_stats.point_value_copies.fetchAdd(1, .monotonic);
    }

    pub fn recordRunGroupBuild(self: *Backend, total_runs: usize, l0_runs: usize, elapsed_ns: u64) void {
        _ = self.read_stats.run_group_builds.fetchAdd(1, .monotonic);
        _ = self.read_stats.run_group_build_ns.fetchAdd(elapsed_ns, .monotonic);
        _ = self.read_stats.run_group_total_runs.fetchAdd(@intCast(total_runs), .monotonic);
        _ = self.read_stats.run_group_l0_runs.fetchAdd(@intCast(l0_runs), .monotonic);
    }

    pub fn releaseReader(self: *Backend) void {
        std.debug.assert(self.active_readers > 0);
        self.active_readers -= 1;
        if (self.active_readers == 0) {
            self.drainObsoleteRuns();
            self.drainRetiredImmutableMemtables();
            self.drainRetiredMutableSnapshots();
        }
    }

    pub fn queueObsoleteFilePath(self: *Backend, path: []u8) !void {
        const delete_after_ns = self.nowNs() +| self.options.obsolete_retention_ns;
        for (self.obsolete_paths.items) |*obsolete| {
            if (!std.mem.eql(u8, obsolete.path, path)) continue;
            if (obsolete.delete_after_ns < delete_after_ns) obsolete.delete_after_ns = delete_after_ns;
            self.allocator.free(path);
            self.obsolete_manifest_dirty = true;
            return;
        }

        try self.obsolete_paths.append(self.allocator, .{
            .path = path,
            .delete_after_ns = delete_after_ns,
        });
        self.obsolete_manifest_dirty = true;
    }

    pub fn queueObsoleteRuns(self: *Backend, runs: std.ArrayListUnmanaged(Run)) !void {
        if (runs.items.len == 0) {
            var empty = runs;
            empty.deinit(self.allocator);
            return;
        }
        try self.obsolete_runs.append(self.allocator, runs);
        if (self.active_readers == 0) self.drainObsoleteRuns();
    }

    pub fn getCachedRunState(self: *Backend, path: []const u8, run_id: u64) !*const State {
        const index = try self.getCachedRunStateIndex(path, run_id);
        return self.run_state_cache.items[index].state();
    }

    pub fn getCachedRunStateIndex(self: *Backend, path: []const u8, run_id: u64) !usize {
        for (self.run_state_cache.items, 0..) |*cached, i| {
            if (cached.run_id == run_id and std.mem.eql(u8, cached.path, path)) return i;
        }

        const cached_path = try self.allocator.dupe(u8, path);
        errdefer self.allocator.free(cached_path);

        if (self.options.cache) |cache| {
            const generation = self.root_generation;
            var handle = while (true) {
                if (cache.retainRunState(path, run_id, generation)) |retained| break retained;
                try cache.beginLoad(path, run_id, generation, .run_state);
                defer cache.finishLoad(path, run_id, generation, .run_state);
                if (cache.retainRunState(path, run_id, generation)) |retained| break retained;
                const loaded = try repository_mod.loadRunStateAllocWithStorage(self.storage.?, cache.valueAllocator(), path);
                break try cache.putRunState(path, run_id, generation, loaded);
            };
            errdefer handle.release();
            try self.run_state_cache.append(self.allocator, .{
                .run_id = run_id,
                .path = cached_path,
                .value = .{ .shared = handle },
            });
            return self.run_state_cache.items.len - 1;
        }

        const loaded = try repository_mod.loadRunStateAllocWithStorage(self.storage.?, self.allocator, path);
        errdefer {
            var state = loaded;
            state.deinit(self.allocator);
        }
        try self.run_state_cache.append(self.allocator, .{
            .run_id = run_id,
            .path = cached_path,
            .value = .{ .owned = loaded },
        });
        return self.run_state_cache.items.len - 1;
    }

    pub fn getCachedRunStateByIndex(self: *Backend, index: usize) *const State {
        return self.run_state_cache.items[index].state();
    }

    pub fn getCachedRunTable(self: *Backend, path: []const u8, run_id: u64) !*const lsm_table_file.BorrowedDecoded {
        const index = try self.getCachedRunTableIndex(path, run_id);
        return self.run_table_cache.items[index].table();
    }

    pub fn getCachedRunIndex(self: *Backend, path: []const u8, run_id: u64) !*const lsm_table_file.TableIndex {
        const index = try self.getCachedRunIndexIndex(path, run_id);
        return self.getCachedRunIndexByIndex(index);
    }

    pub fn getCachedRunIndexIndex(self: *Backend, path: []const u8, run_id: u64) !usize {
        for (self.run_index_cache.items, 0..) |*cached, i| {
            if (cached.run_id == run_id and std.mem.eql(u8, cached.path, path)) return i;
        }

        const cached_path = try self.allocator.dupe(u8, path);
        errdefer self.allocator.free(cached_path);

        const loaded = try self.allocator.create(lsm_table_file.TableIndex);
        errdefer self.allocator.destroy(loaded);
        const start_ns = self.readStatsNowNs();
        const loaded_index = repository_mod.loadRunTableIndexAllocWithStorage(self.storage.?, self.allocator, path);
        self.recordTableIndexLoad(self.readStatsElapsedNs(start_ns));
        loaded.* = try loaded_index;
        errdefer loaded.deinit(self.allocator);
        try self.run_index_cache.append(self.allocator, .{
            .run_id = run_id,
            .path = cached_path,
            .index = loaded,
        });
        return self.run_index_cache.items.len - 1;
    }

    pub fn getCachedRunTableIndex(self: *Backend, path: []const u8, run_id: u64) !usize {
        for (self.run_table_cache.items, 0..) |*cached, i| {
            if (cached.run_id == run_id and std.mem.eql(u8, cached.path, path)) return i;
        }

        const cached_path = try self.allocator.dupe(u8, path);
        errdefer self.allocator.free(cached_path);

        if (self.options.cache) |cache| {
            const generation = self.root_generation;
            var raw_handle = while (true) {
                if (cache.retainRunTableRaw(path, run_id, generation)) |retained| break retained;
                try cache.beginLoad(path, run_id, generation, .run_table_raw);
                defer cache.finishLoad(path, run_id, generation, .run_table_raw);
                if (cache.retainRunTableRaw(path, run_id, generation)) |retained| break retained;
                const max_read_bytes = repository_mod.maxRunFileReadBytes();
                const raw = self.storage.?.readFileAlloc(cache.valueAllocator(), path, max_read_bytes) catch |err| {
                    logStreamTooLongForPath(self.storage.?, path, max_read_bytes, "Backend.getCachedRunTableIndex.raw", err);
                    return err;
                };
                break try cache.putRunTableRaw(path, run_id, generation, raw);
            };
            errdefer raw_handle.release();

            var index_handle = while (true) {
                if (cache.retainRunTableIndex(path, run_id, generation)) |retained| break retained;
                try cache.beginLoad(path, run_id, generation, .run_table_index);
                defer cache.finishLoad(path, run_id, generation, .run_table_index);
                if (cache.retainRunTableIndex(path, run_id, generation)) |retained| break retained;
                const start_ns = self.readStatsNowNs();
                const decoded = lsm_table_file.decodeIndexAlloc(cache.valueAllocator(), raw_handle.runTableRaw());
                self.recordTableIndexDecode(self.readStatsElapsedNs(start_ns));
                const index = try decoded;
                break try cache.putRunTableIndex(path, run_id, generation, index);
            };
            errdefer index_handle.release();

            if (lsm_table_file.indexHasCompressedBlocks(index_handle.runTableIndex())) {
                const raw_copy = try self.allocator.dupe(u8, raw_handle.runTableRaw());
                var loaded = lsm_table_file.decodeBorrowedOwnedAlloc(self.allocator, raw_copy) catch |err| {
                    self.allocator.free(raw_copy);
                    return err;
                };
                errdefer loaded.deinit(self.allocator);
                try self.run_table_cache.append(self.allocator, .{
                    .run_id = run_id,
                    .path = cached_path,
                    .value = .{ .owned = loaded },
                });
                raw_handle.release();
                index_handle.release();
                return self.run_table_cache.items.len - 1;
            }

            try self.run_table_cache.append(self.allocator, .{
                .run_id = run_id,
                .path = cached_path,
                .value = .{ .shared = .{
                    .raw = raw_handle,
                    .index = index_handle,
                    .table = lsm_table_file.borrowDecoded(raw_handle.runTableRaw(), index_handle.runTableIndex()),
                } },
            });
            return self.run_table_cache.items.len - 1;
        }

        var loaded = try repository_mod.loadRunTableBorrowedAllocWithStorage(self.storage.?, self.allocator, path);
        errdefer loaded.deinit(self.allocator);
        try self.run_table_cache.append(self.allocator, .{
            .run_id = run_id,
            .path = cached_path,
            .value = .{ .owned = loaded },
        });
        return self.run_table_cache.items.len - 1;
    }

    pub fn getCachedRunTableByIndex(self: *Backend, index: usize) *const lsm_table_file.BorrowedDecoded {
        return self.run_table_cache.items[index].table();
    }

    pub fn getCachedRunIndexByIndex(self: *Backend, index: usize) *const lsm_table_file.TableIndex {
        return self.run_index_cache.items[index].index;
    }

    pub fn getCachedRunBlock(
        self: *Backend,
        path: []const u8,
        run_id: u64,
        block_offset: u64,
        block_len: u32,
    ) ?[]const u8 {
        if (!self.options.local_block_cache_enabled) return null;
        for (self.run_block_cache.items) |*cached| {
            if (cached.run_id != run_id or
                cached.block_offset != block_offset or
                cached.block_len != block_len or
                !std.mem.eql(u8, cached.path, path)) continue;
            cached.last_access = self.nextLocalCacheAccess();
            return cached.bytes;
        }
        return null;
    }

    pub fn putCachedRunBlock(
        self: *Backend,
        path: []const u8,
        run_id: u64,
        block_offset: u64,
        block_len: u32,
        block: []u8,
    ) ![]const u8 {
        if (!self.options.local_block_cache_enabled) {
            self.allocator.free(block);
            return &.{};
        }
        errdefer self.allocator.free(block);
        const cached_path = try self.allocator.dupe(u8, path);
        errdefer self.allocator.free(cached_path);
        try self.run_block_cache.append(self.allocator, .{
            .run_id = run_id,
            .path = cached_path,
            .block_offset = block_offset,
            .block_len = block_len,
            .bytes = block,
            .last_access = self.nextLocalCacheAccess(),
        });
        self.evictCachedRunBlocksToBudget();
        return self.run_block_cache.items[self.run_block_cache.items.len - 1].bytes;
    }

    fn drainObsoleteRuns(self: *Backend) void {
        for (self.obsolete_runs.items) |*runs| {
            for (runs.items) |*run| {
                // File retention is tracked separately by obsolete_paths; this only releases local cache handles.
                if (run.path) |path| self.evictLocalCachesForRun(path, run.id);
                run.deinit(self.allocator);
            }
            runs.deinit(self.allocator);
        }
        self.obsolete_runs.clearRetainingCapacity();
    }

    pub fn finalizeWriteReaderRelease(self: *Backend) !void {
        self.releaseReader();
        const reclaimable_obsolete_paths = self.hasReclaimableObsoletePathsLocked();
        if ((!self.manifest_dirty and !self.obsolete_manifest_dirty and !reclaimable_obsolete_paths) or
            self.active_readers != 0 or
            self.bulkIngestActive() or
            self.root_dir == null or
            self.options.backend.read_only) return;
        try self.persistManifest();
    }

    pub fn finalizeReadReaderRelease(self: *Backend) void {
        self.releaseReader();
        const reclaimable_obsolete_paths = self.hasReclaimableObsoletePathsLocked();
        if ((!self.manifest_dirty and !self.obsolete_manifest_dirty and !reclaimable_obsolete_paths) or
            self.active_readers != 0 or
            self.bulkIngestActive() or
            self.root_dir == null or
            self.options.backend.read_only) return;
        self.persistManifest() catch {};
    }

    pub fn beginBatchMode(self: *Backend, options: backend_types.BatchOptions) void {
        if (options.mode != .bulk_ingest) return;
        self.active_bulk_ingest_batches += 1;
    }

    pub fn finishBatchMode(self: *Backend, options: backend_types.BatchOptions) void {
        if (options.mode != .bulk_ingest) return;
        std.debug.assert(self.active_bulk_ingest_batches > 0);
        self.active_bulk_ingest_batches -= 1;
    }

    pub fn finalizeExitedBatchMode(self: *Backend, options: backend_types.BatchOptions) !void {
        if (options.mode != .bulk_ingest or self.active_bulk_ingest_batches != 0) return;
        try self.finalizeDeferredRunWork(.{});
    }

    fn effectiveFlushThreshold(self: *const Backend) usize {
        const base_threshold = @max(@as(usize, 1), self.options.flush_threshold);
        if (self.active_bulk_ingest_batches == 0) return base_threshold;
        const multiplier = @max(@as(usize, 1), self.options.bulk_ingest_flush_threshold_multiplier);
        return std.math.mul(usize, base_threshold, multiplier) catch std.math.maxInt(usize);
    }

    fn effectiveFlushThresholdBytes(self: *const Backend) u64 {
        if (self.options.flush_threshold_bytes == 0) return 0;
        if (self.active_bulk_ingest_batches == 0) return self.options.flush_threshold_bytes;
        const multiplier: u64 = @intCast(@max(@as(usize, 1), self.options.bulk_ingest_flush_threshold_bytes_multiplier));
        return std.math.mul(u64, self.options.flush_threshold_bytes, multiplier) catch std.math.maxInt(u64);
    }

    fn stateMeetsBulkFlushThreshold(self: *const Backend, state: *const State) bool {
        const byte_threshold = self.effectiveFlushThresholdBytes();
        if (byte_threshold > 0) return estimateStateBytes(state) >= byte_threshold;
        return state.entries.items.len >= self.effectiveFlushThreshold();
    }

    fn shouldFlushMutable(self: *const Backend) bool {
        if (self.mutable.entries.items.len == 0) return false;
        const byte_threshold = self.effectiveFlushThresholdBytes();
        if (byte_threshold > 0) return estimateStateBytes(&self.mutable) >= byte_threshold;
        return self.mutable.entries.items.len >= self.effectiveFlushThreshold();
    }

    fn shouldFlushMutableForWalPressureLocked(self: *Backend) !bool {
        if (self.mutable.entries.items.len == 0) return false;
        const retention = try self.snapshotWalRetentionForPressureLocked() orelse return false;
        return self.walRetentionOverSoftLimit(retention);
    }

    fn walRetentionPressureEnabled(self: *const Backend) bool {
        return self.options.wal_soft_limit_segments > 0 or
            self.options.wal_hard_limit_segments > 0 or
            self.options.wal_soft_limit_bytes > 0 or
            self.options.wal_hard_limit_bytes > 0;
    }

    fn snapshotWalRetentionForPressureLocked(self: *Backend) !?wal_mod.RetentionStats {
        if (!self.walRetentionPressureEnabled()) return null;
        if (!self.options.wal_enabled or self.root_dir == null or self.options.backend.read_only) return null;
        return try wal_mod.snapshotRetention(self.storage.?, self.allocator, self.root_dir.?);
    }

    fn walRetentionOverSoftLimit(self: *const Backend, retention: wal_mod.RetentionStats) bool {
        if (self.options.wal_soft_limit_segments > 0 and retention.segments > self.options.wal_soft_limit_segments) return true;
        if (self.options.wal_soft_limit_bytes > 0 and retention.bytes > self.options.wal_soft_limit_bytes) return true;
        return self.walRetentionOverHardLimit(retention);
    }

    fn walRetentionOverHardLimit(self: *const Backend, retention: wal_mod.RetentionStats) bool {
        if (self.options.wal_hard_limit_segments > 0 and retention.segments > self.options.wal_hard_limit_segments) return true;
        if (self.options.wal_hard_limit_bytes > 0 and retention.bytes > self.options.wal_hard_limit_bytes) return true;
        return false;
    }

    fn walRetentionPressureScoreLocked(self: *Backend) u64 {
        const retention = self.snapshotWalRetentionForPressureLocked() catch return 0;
        const stats = retention orelse return 0;
        var score: u64 = 0;
        if (self.options.wal_hard_limit_segments > 0 and stats.segments > self.options.wal_hard_limit_segments) {
            score +|= (stats.segments - self.options.wal_hard_limit_segments) * 1_000_000;
        } else if (self.options.wal_soft_limit_segments > 0 and stats.segments > self.options.wal_soft_limit_segments) {
            score +|= (stats.segments - self.options.wal_soft_limit_segments) * 10_000;
        }
        if (self.options.wal_hard_limit_bytes > 0 and stats.bytes > self.options.wal_hard_limit_bytes) {
            score +|= (stats.bytes - self.options.wal_hard_limit_bytes) / 1024;
        } else if (self.options.wal_soft_limit_bytes > 0 and stats.bytes > self.options.wal_soft_limit_bytes) {
            score +|= (stats.bytes - self.options.wal_soft_limit_bytes) / (16 * 1024);
        }
        return score;
    }

    fn effectiveL0SoftLimitRuns(self: *const Backend) usize {
        if (self.options.l0_soft_limit_runs != 0) return self.options.l0_soft_limit_runs;
        return self.options.compact_threshold_runs;
    }

    fn effectiveL0HardLimitRuns(self: *const Backend) usize {
        if (self.options.l0_hard_limit_runs != 0) return self.options.l0_hard_limit_runs;
        const soft = self.effectiveL0SoftLimitRuns();
        return std.math.mul(usize, @max(@as(usize, 1), soft), 4) catch std.math.maxInt(usize);
    }

    fn enforceWritePressure(self: *Backend) anyerror!void {
        if (self.bulkIngestActive()) return;
        if (self.write_pressure_enforcing) return;
        self.write_pressure_enforcing = true;
        defer self.write_pressure_enforcing = false;

        try self.enforceWalRetentionHardPressure();

        const hard_runs = self.effectiveL0HardLimitRuns();
        const hard_bytes = self.options.l0_hard_limit_bytes;
        if (hard_runs == 0 and hard_bytes == 0) return;

        var l0_runs: usize = 0;
        var l0_bytes: u64 = 0;
        while (l0_runs < self.runs.items.len and self.runs.items[l0_runs].level == 0) : (l0_runs += 1) {
            l0_bytes += self.runs.items[l0_runs].size_bytes;
        }
        const over_runs = hard_runs > 0 and l0_runs > hard_runs;
        const over_bytes = hard_bytes > 0 and l0_bytes > hard_bytes;
        if (!over_runs and !over_bytes) return;

        const start_ns = self.writeStatsNowNs();
        const target_runs = if (self.options.l0_soft_limit_runs != 0) self.options.l0_soft_limit_runs else self.options.compact_threshold_runs;
        const before_compactions = self.compaction_stats.compactions;
        try compaction_mod.compactL0ToLimit(Backend, self, target_runs);
        if (self.compaction_stats.compactions != before_compactions) {
            self.write_stats.write_pressure_compactions += 1;
            self.write_stats.write_pressure_ns += self.writeStatsElapsedNs(start_ns);
        }
    }

    fn enforceWalRetentionHardPressureGuarded(self: *Backend) anyerror!void {
        if (self.bulkIngestActive()) return;
        if (self.write_pressure_enforcing) return;
        self.write_pressure_enforcing = true;
        defer self.write_pressure_enforcing = false;
        try self.enforceWalRetentionHardPressure();
    }

    fn enforceWalRetentionHardPressure(self: *Backend) anyerror!void {
        var retention = try self.snapshotWalRetentionForPressureLocked() orelse return;
        if (!self.walRetentionOverHardLimit(retention)) return;

        const start_ns = self.writeStatsNowNs();
        var flushes: u64 = 0;
        if (self.mutable.entries.items.len > 0) {
            try self.rotateMutableToImmutable();
        }

        const saved_budget = self.maintenance_io_budget_remaining;
        self.maintenance_io_budget_remaining = null;
        defer self.maintenance_io_budget_remaining = saved_budget;

        while (self.activeImmutableMemtableCount() > 0 and self.walRetentionOverHardLimit(retention)) {
            if (!try self.flushOldestImmutableMemtable()) break;
            flushes += 1;
            retention = try self.snapshotWalRetentionForPressureLocked() orelse break;
        }

        if (self.activeImmutableMemtableCount() == 0 and self.mutable.entries.items.len == 0 and self.walRetentionOverHardLimit(retention)) {
            try self.resetWalAfterManifestCheckpoint();
            retention = try self.snapshotWalRetentionForPressureLocked() orelse retention;
        }

        if (flushes > 0 or !self.walRetentionOverHardLimit(retention)) {
            self.write_stats.wal_pressure_flushes += flushes;
            self.write_stats.wal_pressure_ns += self.writeStatsElapsedNs(start_ns);
        }
    }

    pub fn bulkIngestActive(self: *const Backend) bool {
        return self.active_bulk_ingest_batches != 0;
    }

    pub fn beginBulkIngestSession(self: *Backend) !void {
        const locked = runtime_mod.lockBackend(Backend, self);
        defer runtime_mod.unlockBackend(Backend, self, locked);
        try self.beginBulkIngestSessionLocked();
    }

    fn beginBulkIngestSessionLocked(self: *Backend) !void {
        if (self.options.backend.read_only) return error.ReadOnly;
        self.active_bulk_ingest_batches += 1;
    }

    pub fn finishBulkIngestSession(self: *Backend) !void {
        try self.finishBulkIngestSessionWithOptions(.{});
    }

    pub fn finishBulkIngestSessionWithOptions(self: *Backend, options: BulkIngestFinishOptions) !void {
        const locked = runtime_mod.lockBackend(Backend, self);
        defer runtime_mod.unlockBackend(Backend, self, locked);
        try self.finishBulkIngestSessionWithOptionsLocked(options);
    }

    pub fn flushBufferedWritesWithOptions(self: *Backend, options: BulkIngestFinishOptions) !void {
        const locked = runtime_mod.lockBackend(Backend, self);
        defer runtime_mod.unlockBackend(Backend, self, locked);
        try self.flushBufferedWritesWithOptionsLocked(options);
    }

    fn flushBufferedWritesWithOptionsLocked(self: *Backend, options: BulkIngestFinishOptions) !void {
        if (self.mutable.entries.items.len > 0 or self.activeImmutableMemtableCount() > 0) {
            try self.flushMutable();
        }
        try self.runForegroundCompactionBudget(options);
        if (options.compact) {
            try self.finalizeDeferredRunWork(.{ .force_soft_compaction = true });
        } else if (self.root_dir != null and (self.manifest_dirty or self.obsolete_manifest_dirty or self.hasReclaimableObsoletePathsLocked())) {
            try self.persistManifest();
        } else {
            _ = self.refreshCachedMaintenanceHintLocked();
        }
    }

    fn finishBulkIngestSessionWithOptionsLocked(self: *Backend, options: BulkIngestFinishOptions) !void {
        std.debug.assert(self.active_bulk_ingest_batches > 0);
        if (!options.compact and self.active_bulk_ingest_batches == 1) {
            if ((options.flush or self.shouldFlushMemtablesOnLastBulkIngestFinish()) and
                (self.mutable.entries.items.len > 0 or self.activeImmutableMemtableCount() > 0))
            {
                if (!try self.directIngestMutableAtBulkFinishIfPossible()) {
                    try self.flushMutable();
                }
            }
            try self.runForegroundCompactionBudget(options);
            if (self.root_dir != null and (self.manifest_dirty or self.obsolete_manifest_dirty or self.hasReclaimableObsoletePathsLocked())) {
                try self.persistManifest();
            } else {
                _ = self.refreshCachedMaintenanceHintLocked();
            }
            self.active_bulk_ingest_batches -= 1;
            return;
        }
        self.active_bulk_ingest_batches -= 1;
        if (self.active_bulk_ingest_batches == 0) {
            if (self.mutable.entries.items.len > 0 or self.activeImmutableMemtableCount() > 0) {
                if (!try self.directIngestMutableAtBulkFinishIfPossible()) {
                    try self.flushMutable();
                }
            }
            try self.finalizeDeferredRunWork(.{ .force_soft_compaction = options.compact });
        }
    }

    fn shouldFlushMemtablesOnLastBulkIngestFinish(self: *const Backend) bool {
        if (self.root_dir == null) return true;
        if (!self.options.wal_enabled) return true;
        return false;
    }

    pub fn abortBulkIngestSession(self: *Backend) void {
        const locked = runtime_mod.lockBackend(Backend, self);
        defer runtime_mod.unlockBackend(Backend, self, locked);
        self.abortBulkIngestSessionLocked();
    }

    fn abortBulkIngestSessionLocked(self: *Backend) void {
        std.debug.assert(self.active_bulk_ingest_batches > 0);
        self.active_bulk_ingest_batches -= 1;
    }

    pub fn markManifestDirty(self: *Backend) void {
        self.manifest_dirty = true;
    }

    pub fn finalizeDeferredStorageWork(self: *Backend) !void {
        const locked = runtime_mod.lockBackend(Backend, self);
        defer runtime_mod.unlockBackend(Backend, self, locked);
        try self.finalizeDeferredStorageWorkLocked();
    }

    fn finalizeDeferredStorageWorkLocked(self: *Backend) !void {
        if (self.options.backend.read_only) return;
        if (self.mutable.entries.items.len > 0 or self.activeImmutableMemtableCount() > 0) {
            try self.flushMutable();
        }
        try self.finalizeDeferredRunWork(.{});
    }

    const DeferredRunWorkOptions = struct {
        force_soft_compaction: bool = false,
    };

    fn finalizeDeferredRunWork(self: *Backend, options: DeferredRunWorkOptions) !void {
        if (self.options.backend.read_only) return;
        try self.enforceWritePressure();
        if (!self.bulkIngestActive() and (options.force_soft_compaction or self.options.foreground_soft_compaction)) {
            try self.maybeCompactRuns();
        }
        if (self.root_dir != null and (self.manifest_dirty or self.obsolete_manifest_dirty or self.hasReclaimableObsoletePathsLocked())) {
            try self.persistManifest();
        }
    }

    fn compactDeferredL0RunsToLimit(self: *Backend, limit: usize) !void {
        if (limit == 0) {
            try self.maybeCompactRuns();
            return;
        }
        while (countLevelRuns(self.runs.items, 0) > limit) {
            try compaction_mod.compactL0ToLimit(Backend, self, limit);
        }
    }

    fn runForegroundCompactionBudget(self: *Backend, options: BulkIngestFinishOptions) !void {
        const max_steps = options.max_foreground_compaction_steps;
        if (max_steps == 0) {
            _ = self.refreshCachedMaintenanceHintLocked();
            return;
        }

        const limit = options.max_deferred_l0_runs orelse self.effectiveL0SoftLimitRuns();
        const start_ns = self.writeStatsNowNs();
        var steps: usize = 0;
        while (steps < max_steps) : (steps += 1) {
            if (options.max_foreground_compaction_ns) |budget_ns| {
                if (budget_ns == 0) break;
                if (self.writeStatsElapsedNs(start_ns) >= budget_ns) break;
            }
            if (limit > 0 and countLevelRuns(self.runs.items, 0) <= limit) break;
            const score = self.maintenanceScoreLocked();
            const compacted = try compaction_mod.compactL0ToLimitScheduledWithinBudget(
                Backend,
                self,
                limit,
                score,
                options.max_foreground_compaction_input_bytes,
            );
            if (!compacted) break;
        }
        _ = self.refreshCachedMaintenanceHintLocked();
    }

    fn reconcileObsoletePathsForManifest(self: *Backend) !bool {
        const now_ns = self.nowNs();
        var needs_follow_up = false;
        var i: usize = 0;
        while (i < self.obsolete_paths.items.len) {
            const obsolete = self.obsolete_paths.items[i];
            if (self.active_readers != 0) {
                needs_follow_up = true;
                i += 1;
                continue;
            }

            if (obsolete.delete_after_ns > now_ns) {
                i += 1;
                continue;
            }

            repository_mod.deleteFileAbsoluteWithStorage(self.storage.?, obsolete.path) catch |err| switch (err) {
                error.FileNotFound => {},
                else => return err,
            };
            var removed = self.obsolete_paths.orderedRemove(i);
            removed.deinit(self.allocator);
        }
        return needs_follow_up;
    }

    fn hasReclaimableObsoletePathsLocked(self: *Backend) bool {
        if (self.active_readers != 0 or self.obsolete_paths.items.len == 0) return false;
        if (self.root_dir == null or self.storage == null or self.options.backend.read_only) return false;
        const now_ns = self.nowNs();
        for (self.obsolete_paths.items) |obsolete| {
            if (obsolete.delete_after_ns <= now_ns) return true;
        }
        return false;
    }

    fn nowNs(self: *Backend) u64 {
        if (self.storage) |storage| return storage.nowNs();
        return 0;
    }

    fn nextLocalCacheAccess(self: *Backend) u64 {
        self.local_cache_access_clock += 1;
        return self.local_cache_access_clock;
    }

    fn evictLocalCachesForRun(self: *Backend, path: []const u8, run_id: u64) void {
        self.evictCachedRunStateForRun(path, run_id);
        self.evictCachedRunIndexForRun(path, run_id);
        self.evictCachedRunBlocksForRun(path, run_id);
        self.evictCachedRunTableForRun(path, run_id);
    }

    fn evictCachedRunStateForRun(self: *Backend, path: []const u8, run_id: u64) void {
        var i: usize = 0;
        while (i < self.run_state_cache.items.len) : (i += 1) {
            const cached = &self.run_state_cache.items[i];
            if (cached.run_id != run_id or !std.mem.eql(u8, cached.path, path)) continue;
            var removed = self.run_state_cache.orderedRemove(i);
            removed.deinit(self.allocator);
            return;
        }
    }

    fn evictCachedRunIndexForRun(self: *Backend, path: []const u8, run_id: u64) void {
        var i: usize = 0;
        while (i < self.run_index_cache.items.len) {
            const cached = &self.run_index_cache.items[i];
            if (cached.run_id != run_id or !std.mem.eql(u8, cached.path, path)) {
                i += 1;
                continue;
            }
            var removed = self.run_index_cache.orderedRemove(i);
            removed.deinit(self.allocator);
        }
    }

    fn evictCachedRunBlocksForRun(self: *Backend, path: []const u8, run_id: u64) void {
        var i: usize = 0;
        while (i < self.run_block_cache.items.len) {
            const cached = &self.run_block_cache.items[i];
            if (cached.run_id != run_id or !std.mem.eql(u8, cached.path, path)) {
                i += 1;
                continue;
            }
            var removed = self.run_block_cache.orderedRemove(i);
            removed.deinit(self.allocator);
        }
    }

    fn evictCachedRunBlocksToBudget(self: *Backend) void {
        while (self.run_block_cache.items.len > max_local_cached_run_blocks) {
            var victim_index: usize = 0;
            var victim_access = self.run_block_cache.items[0].last_access;
            for (self.run_block_cache.items[1..], 1..) |cached, i| {
                if (cached.last_access < victim_access) {
                    victim_access = cached.last_access;
                    victim_index = i;
                }
            }
            var victim = self.run_block_cache.orderedRemove(victim_index);
            victim.deinit(self.allocator);
        }
    }

    fn evictCachedRunTableForRun(self: *Backend, path: []const u8, run_id: u64) void {
        var i: usize = 0;
        while (i < self.run_table_cache.items.len) : (i += 1) {
            const cached = &self.run_table_cache.items[i];
            if (cached.run_id != run_id or !std.mem.eql(u8, cached.path, path)) continue;
            var removed = self.run_table_cache.orderedRemove(i);
            removed.deinit(self.allocator);
            return;
        }
    }
};

pub const BackendHandle = struct {
    allocator: Allocator,
    backend: *Backend,

    pub fn init(allocator: Allocator, options: Options) !BackendHandle {
        const backend = try allocator.create(Backend);
        errdefer allocator.destroy(backend);
        backend.* = Backend.init(allocator, options);
        return .{
            .allocator = allocator,
            .backend = backend,
        };
    }

    pub fn open(allocator: Allocator, root_dir: []const u8, options: Options) !BackendHandle {
        const backend = try allocator.create(Backend);
        errdefer allocator.destroy(backend);
        try backend.openInto(allocator, root_dir, options);
        return .{
            .allocator = allocator,
            .backend = backend,
        };
    }

    pub fn close(self: *BackendHandle) void {
        self.backend.close();
        self.allocator.destroy(self.backend);
        self.* = undefined;
    }

    pub fn ptr(self: *BackendHandle) *Backend {
        return self.backend;
    }
};

fn logStreamTooLongForPath(storage: Storage, path: []const u8, max_bytes: usize, site: []const u8, err: anyerror) void {
    if (err != error.StreamTooLong) return;
    const size = storage.fileSize(path) catch |size_err| {
        std.log.err("lsm readFileAlloc StreamTooLong site={s} path={s} max_bytes={d} file_size_err={}", .{ site, path, max_bytes, size_err });
        return;
    };
    std.log.err("lsm readFileAlloc StreamTooLong site={s} path={s} max_bytes={d} file_size={d}", .{ site, path, max_bytes, size });
}

fn runMayContain(run: Run, namespace: backend_types.Namespace, key: []const u8) bool {
    return compareRunBound(namespace.name, key, run.smallest_namespace_name, run.smallest_key) != .lt and
        compareRunBound(namespace.name, key, run.largest_namespace_name, run.largest_key) != .gt;
}

fn findRunIndexInSortedLevel(runs: []const Run, namespace: backend_types.Namespace, key: []const u8) ?usize {
    if (runs.len == 0) return null;
    var lo: usize = 0;
    var hi: usize = runs.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (compareRunBound(runs[mid].largest_namespace_name, runs[mid].largest_key, namespace.name, key) == .lt) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if (lo >= runs.len) return null;
    if (!runMayContain(runs[lo], namespace, key)) return null;
    return lo;
}

fn compareRunBound(lhs_namespace_name: ?[]const u8, lhs_key: []const u8, rhs_namespace_name: ?[]const u8, rhs_key: []const u8) std.math.Order {
    const namespace_order = compareNamespace(.{ .name = lhs_namespace_name }, .{ .name = rhs_namespace_name });
    if (namespace_order != .eq) return namespace_order;
    return std.mem.order(u8, lhs_key, rhs_key);
}

fn logInvalidRunLayout(reason: []const u8, prior: ?Run, run: Run) void {
    std.log.warn(
        "lsm manifest run layout invalid reason={s} run_id={} run_level={} run_path={s} run_smallest_len={} run_largest_len={}",
        .{
            reason,
            run.id,
            run.level,
            run.path orelse "(memory)",
            run.smallest_key.len,
            run.largest_key.len,
        },
    );
    if (prior) |prev| {
        std.log.warn(
            "lsm manifest prior run prior_id={} prior_level={} prior_path={s} prior_smallest_len={} prior_largest_len={}",
            .{
                prev.id,
                prev.level,
                prev.path orelse "(memory)",
                prev.smallest_key.len,
                prev.largest_key.len,
            },
        );
    }
}

fn validateRunLayoutForManifest(runs: []const Run) !void {
    var prior: ?Run = null;
    for (runs) |run| {
        if (run.path == null) return error.RunStateUnavailable;
        if (run.entry_count == 0) {
            logInvalidRunLayout("empty_run", prior, run);
            return error.InvalidTableFile;
        }
        if (compareRunBound(run.smallest_namespace_name, run.smallest_key, run.largest_namespace_name, run.largest_key) == .gt) {
            logInvalidRunLayout("inverted_bounds", prior, run);
            return error.InvalidTableFile;
        }
        if (prior) |prev| {
            if (prev.level > run.level) {
                logInvalidRunLayout("level_order", prior, run);
                return error.InvalidTableFile;
            }
            if (prev.level == run.level) {
                if (prev.level == 0) {
                    if (prev.id <= run.id) {
                        logInvalidRunLayout("l0_id_order", prior, run);
                        return error.InvalidTableFile;
                    }
                } else {
                    if (compareRunBound(prev.smallest_namespace_name, prev.smallest_key, run.smallest_namespace_name, run.smallest_key) == .gt) {
                        logInvalidRunLayout("level_bound_order", prior, run);
                        return error.InvalidTableFile;
                    }
                    if (compareRunBound(prev.largest_namespace_name, prev.largest_key, run.smallest_namespace_name, run.smallest_key) != .lt) {
                        logInvalidRunLayout("level_overlap", prior, run);
                        return error.InvalidTableFile;
                    }
                }
            }
        }
        prior = run;
    }
}

const SplitSide = enum {
    left,
    right,
    overlap,
};

fn classifyRun(run: Run, split_key: []const u8) SplitSide {
    if (std.mem.order(u8, run.largest_key, split_key) == .lt) return .left;
    if (std.mem.order(u8, run.smallest_key, split_key) != .lt) return .right;
    return .overlap;
}

fn cloneRunForBackend(dest: *Backend, source: Run) !Run {
    const run_id = dest.next_run_id;
    dest.next_run_id += 1;

    const smallest_namespace_name = if (source.smallest_namespace_name) |name| try dest.allocator.dupe(u8, name) else null;
    errdefer if (smallest_namespace_name) |name| dest.allocator.free(name);
    const smallest_key = try dest.allocator.dupe(u8, source.smallest_key);
    errdefer dest.allocator.free(smallest_key);
    const largest_namespace_name = if (source.largest_namespace_name) |name| try dest.allocator.dupe(u8, name) else null;
    errdefer if (largest_namespace_name) |name| dest.allocator.free(name);
    const largest_key = try dest.allocator.dupe(u8, source.largest_key);
    errdefer dest.allocator.free(largest_key);

    var run = Run{
        .id = run_id,
        .level = source.level,
        .size_bytes = source.size_bytes,
        .compression_stats = source.compression_stats,
        .path = null,
        .smallest_namespace_name = smallest_namespace_name,
        .smallest_key = smallest_key,
        .largest_namespace_name = largest_namespace_name,
        .largest_key = largest_key,
        .entry_count = source.entry_count,
        .bloom_filter = if (source.bloom_filter) |filter| try filter.clone(dest.allocator) else null,
        .encoded_bloom_filter = if (source.encoded_bloom_filter) |encoded| try dest.allocator.dupe(u8, encoded) else null,
        .state = null,
    };
    errdefer run.deinit(dest.allocator);

    if (dest.root_dir) |root_dir| {
        const run_path = try repository_mod.runPath(dest.allocator, root_dir, run_id);
        errdefer dest.allocator.free(run_path);
        if (source.path) |src_path| {
            _ = try repository_mod.copyFileAbsoluteWithStorage(dest.storage.?, dest.allocator, src_path, run_path);
            run.path = run_path;
        } else {
            const source_state = source.state orelse return error.RunStateUnavailable;
            run.state = try source_state.clone(dest.allocator);
            run.path = try repository_mod.persistRunFileWithStorage(dest.storage.?, dest.allocator, root_dir, &run, dest.options.table_block_compression);
        }
    } else {
        const source_state = source.state orelse return error.RunStateUnavailable;
        run.state = try source_state.clone(dest.allocator);
    }

    return run;
}

fn clearRunsAndFiles(backend: *Backend) !void {
    for (backend.runs.items) |*run| {
        if (run.path) |path| repository_mod.deleteFileAbsoluteWithStorage(backend.storage.?, path) catch {};
        run.deinit(backend.allocator);
    }
    backend.runs.deinit(backend.allocator);
    backend.runs = .empty;
    backend.invalidateMutableReadSnapshot();
    backend.mutable.deinit(backend.allocator);
    backend.mutable = .{};
    backend.mutable_wal_range = .{};
    for (backend.immutable_memtables.items) |state| backend.destroyImmutableMemtable(state);
    backend.immutable_memtables.clearRetainingCapacity();
    backend.immutable_wal_ranges.clearRetainingCapacity();
    backend.immutable_head = 0;
    backend.drainRetiredImmutableMemtables();
    backend.drainRetiredMutableSnapshots();
    backend.next_run_id = 1;
    try backend.persistManifest();
    try backend.resetWalAfterManifestCheckpoint();
}

fn identityNamespace(namespace: backend_types.Namespace) !backend_types.Namespace {
    return namespace;
}

fn compareRunBoundForTest(lhs_namespace_name: ?[]const u8, lhs_key: []const u8, rhs_namespace_name: ?[]const u8, rhs_key: []const u8) std.math.Order {
    const namespace_order = compareNamespace(.{ .name = lhs_namespace_name }, .{ .name = rhs_namespace_name });
    if (namespace_order != .eq) return namespace_order;
    return std.mem.order(u8, lhs_key, rhs_key);
}

fn rangesOverlapForTest(lhs: Run, rhs: Run) bool {
    return compareRunBoundForTest(lhs.smallest_namespace_name, lhs.smallest_key, rhs.largest_namespace_name, rhs.largest_key) != .gt and
        compareRunBoundForTest(lhs.largest_namespace_name, lhs.largest_key, rhs.smallest_namespace_name, rhs.smallest_key) != .lt;
}

fn expectLowerLevelsNonOverlapping(runs: []const Run) !void {
    var previous_level: ?u32 = null;
    var previous: ?Run = null;
    for (runs) |run| {
        if (previous_level) |level| {
            try std.testing.expect(level <= run.level);
        }
        if (previous) |prior| {
            if (prior.level == run.level and run.level > 0) {
                try std.testing.expect(!rangesOverlapForTest(prior, run));
            }
        }
        previous_level = run.level;
        previous = run;
    }
}

fn levelRunTargetForTest(level: u32, base: usize, multiplier: usize) usize {
    if (level == 0) return 0;
    var target = @max(@as(usize, 1), base);
    var remaining = level - 1;
    const factor = @max(@as(usize, 1), multiplier);
    while (remaining > 0) : (remaining -= 1) {
        target = std.math.mul(usize, target, factor) catch std.math.maxInt(usize);
    }
    return target;
}

fn expectLevelTargetsSatisfied(runs: []const Run, base: usize, multiplier: usize) !void {
    var i: usize = 0;
    while (i < runs.len) {
        const level = runs[i].level;
        const start = i;
        while (i < runs.len and runs[i].level == level) : (i += 1) {}
        if (level == 0) continue;
        try std.testing.expect(i - start <= levelRunTargetForTest(level, base, multiplier));
    }
}

fn countLevelRuns(runs: []const Run, level: u32) usize {
    var count: usize = 0;
    for (runs) |run| {
        if (run.level == level) count += 1;
    }
    return count;
}

fn countRunEntriesForTest(backend: *Backend) !usize {
    var count: usize = 0;
    for (backend.runs.items) |*run| {
        const state = try backend.resolveRunState(run);
        count += state.entries.items.len;
    }
    return count;
}

test "lsm backend runtime erases namespace store handles" {
    var backend = Backend.init(std.testing.allocator, .{ .flush_threshold = 2 });
    defer backend.close();

    var runtime = try backend.runtimeNamespaceStore(std.testing.allocator);
    defer runtime.deinit();
    try std.testing.expect(runtime.capabilities().ordered_append_puts);

    {
        var txn = try runtime.beginWrite();
        try txn.appendPut(.{}, "meta:lsn", "1");
        try txn.appendPut(.{ .name = "docs" }, "doc:a", "A");
        try txn.appendPut(.{ .name = "docs" }, "doc:b", "B");
        try txn.commit();
    }

    {
        var txn = try runtime.beginRead();
        defer txn.abort();
        try std.testing.expectEqualStrings("1", try txn.get(.{}, "meta:lsn"));
        try std.testing.expectEqualStrings("A", try txn.get(.{ .name = "docs" }, "doc:a"));
        try std.testing.expectEqualStrings("B", try txn.get(.{ .name = "docs" }, "doc:b"));
        var cur = try txn.openCursor(.{ .name = "docs" });
        defer cur.close();
        try std.testing.expectEqualStrings("doc:a", (try cur.first()).?.key);
        try std.testing.expectEqualStrings("doc:b", (try cur.next()).?.key);
        try std.testing.expectEqualStrings("doc:b", (try cur.last()).?.key);
        try std.testing.expectEqualStrings("doc:a", (try cur.prev()).?.key);
    }
}

test "lsm backend heap handle owns a stable backend pointer" {
    var handle = try BackendHandle.init(std.testing.allocator, .{ .flush_threshold = 2 });
    defer handle.close();

    const backend = handle.ptr();
    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    var write = try runtime.beginWrite();
    try write.put("doc:1", "value");
    try write.commit();

    try std.testing.expectEqual(backend, handle.ptr());
    var read = try runtime.beginRead();
    defer read.abort();
    try std.testing.expectEqualStrings("value", try read.get("doc:1"));
}

test "lsm backend defaults background executor to inline mode" {
    const Ctx = struct {
        ran: bool = false,
        deinit_called: bool = false,
    };
    const Fns = struct {
        fn run(ptr: *anyopaque) !void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            ctx.ran = true;
        }

        fn deinit(ptr: *anyopaque) void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            ctx.deinit_called = true;
        }
    };

    var backend = Backend.init(std.testing.allocator, .{});
    defer backend.close();

    var ctx = Ctx{};
    try backend.background_executor.submit(.maintenance, &ctx, Fns.run, Fns.deinit);
    try std.testing.expect(ctx.ran);
    try std.testing.expect(ctx.deinit_called);
}

test "lsm backend copies configured background executor" {
    const FakeLane = struct {
        submitted_owner: ?u64 = null,
        submitted_class: ?lsm_background_mod.JobClass = null,
        drained_owner: ?u64 = null,

        fn lane(self: *@This()) background_runtime_mod.DurableJobLane {
            return .{
                .ptr = self,
                .vtable = &vtable,
            };
        }

        fn submit(ptr: *anyopaque, job: background_runtime_mod.Job) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.submitted_owner = job.owner_id;
            self.submitted_class = job.class;
            job.deinit(job.ptr);
        }

        fn drainOwner(ptr: *anyopaque, owner_id: u64) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.drained_owner = owner_id;
        }

        fn poll(_: *anyopaque, _: usize) !usize {
            return 0;
        }

        const vtable = background_runtime_mod.DurableJobLane.VTable{
            .submit = submit,
            .drain_owner = drainOwner,
            .poll = poll,
        };
    };
    const Fns = struct {
        fn run(_: *anyopaque) !void {}
        fn deinit(_: *anyopaque) void {}
    };

    var lane = FakeLane{};
    const executor = BackgroundExecutor.initLane(lane.lane(), 123);
    var backend = Backend.init(std.testing.allocator, .{
        .background_executor = &executor,
    });
    defer backend.close();

    var byte: u8 = 0;
    try backend.background_executor.submit(.commit_durable, &byte, Fns.run, Fns.deinit);
    try std.testing.expectEqual(@as(?u64, 123), lane.submitted_owner);
    try std.testing.expectEqual(@as(?lsm_background_mod.JobClass, .commit_durable), lane.submitted_class);

    backend.background_executor.drain();
    try std.testing.expectEqual(@as(?u64, 123), lane.drained_owner);
}

test "lsm backend schedules deferred immutable flush on configured background executor" {
    const FakeLane = struct {
        submitted_job: ?background_runtime_mod.Job = null,

        fn lane(self: *@This()) background_runtime_mod.DurableJobLane {
            return .{
                .ptr = self,
                .vtable = &vtable,
            };
        }

        fn submit(ptr: *anyopaque, job: background_runtime_mod.Job) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expect(self.submitted_job == null);
            self.submitted_job = job;
        }

        fn drainOwner(_: *anyopaque, _: u64) void {}

        fn poll(_: *anyopaque, _: usize) !usize {
            return 0;
        }

        const vtable = background_runtime_mod.DurableJobLane.VTable{
            .submit = submit,
            .drain_owner = drainOwner,
            .poll = poll,
        };
    };

    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var lane = FakeLane{};
    const executor = BackgroundExecutor.initLane(lane.lane(), 777);
    var backend = try Backend.open(std.testing.allocator, "/lsm-background-flush-test", .{
        .storage = storage.storage(),
        .flush_threshold = 1,
        .defer_flush_on_commit = true,
        .background_executor = &executor,
    });
    defer backend.close();

    {
        var txn = try backend.beginWrite();
        try txn.put(.{}, "key", "value");
        try txn.commit();
    }

    try std.testing.expect(lane.submitted_job != null);
    try std.testing.expectEqual(@as(usize, 1), backend.activeImmutableMemtableCount());
    try std.testing.expectEqual(@as(usize, 0), backend.runs.items.len);
    try std.testing.expect(backend.immutable_flush_job_in_flight);

    var job = lane.submitted_job.?;
    lane.submitted_job = null;
    try job.run(job.ptr);
    job.deinit(job.ptr);

    try std.testing.expect(!backend.immutable_flush_job_in_flight);
    try std.testing.expectEqual(@as(usize, 0), backend.activeImmutableMemtableCount());
    try std.testing.expectEqual(@as(usize, 1), backend.runs.items.len);
}

test "lsm backend close drains scheduled immutable flush before destroying backend" {
    const FakeLane = struct {
        submitted_job: ?background_runtime_mod.Job = null,
        drained_owner: ?u64 = null,
        ran_on_drain: bool = false,
        deinit_called: bool = false,

        fn lane(self: *@This()) background_runtime_mod.DurableJobLane {
            return .{
                .ptr = self,
                .vtable = &vtable,
            };
        }

        fn submit(ptr: *anyopaque, job: background_runtime_mod.Job) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expect(self.submitted_job == null);
            self.submitted_job = job;
        }

        fn drainOwner(ptr: *anyopaque, owner_id: u64) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.drained_owner = owner_id;
            if (self.submitted_job) |job| {
                if (job.owner_id != owner_id) return;
                self.submitted_job = null;
                job.run(job.ptr) catch unreachable;
                self.ran_on_drain = true;
                job.deinit(job.ptr);
                self.deinit_called = true;
            }
        }

        fn poll(_: *anyopaque, _: usize) !usize {
            return 0;
        }

        const vtable = background_runtime_mod.DurableJobLane.VTable{
            .submit = submit,
            .drain_owner = drainOwner,
            .poll = poll,
        };
    };

    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var lane = FakeLane{};
    const executor = BackgroundExecutor.initLane(lane.lane(), 778);
    var backend = try Backend.open(std.testing.allocator, "/lsm-background-close-drain-test", .{
        .storage = storage.storage(),
        .flush_threshold = 1,
        .defer_flush_on_commit = true,
        .background_executor = &executor,
    });

    {
        var txn = try backend.beginWrite();
        try txn.put(.{}, "key", "value");
        try txn.commit();
    }

    try std.testing.expect(lane.submitted_job != null);
    try std.testing.expect(backend.immutable_flush_job_in_flight);

    backend.close();

    try std.testing.expectEqual(@as(?u64, 778), lane.drained_owner);
    try std.testing.expect(lane.submitted_job == null);
    try std.testing.expect(lane.ran_on_drain);
    try std.testing.expect(lane.deinit_called);
}

test "lsm backend deferred immutable queue enforces per-backend limit" {
    const FakeLane = struct {
        submitted_job: ?background_runtime_mod.Job = null,

        fn lane(self: *@This()) background_runtime_mod.DurableJobLane {
            return .{
                .ptr = self,
                .vtable = &vtable,
            };
        }

        fn submit(ptr: *anyopaque, job: background_runtime_mod.Job) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expect(self.submitted_job == null);
            self.submitted_job = job;
        }

        fn drainOwner(_: *anyopaque, _: u64) void {}

        fn poll(_: *anyopaque, _: usize) !usize {
            return 0;
        }

        const vtable = background_runtime_mod.DurableJobLane.VTable{
            .submit = submit,
            .drain_owner = drainOwner,
            .poll = poll,
        };
    };

    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var lane = FakeLane{};
    const executor = BackgroundExecutor.initLane(lane.lane(), 779);
    var backend = try Backend.open(std.testing.allocator, "/lsm-background-queue-limit-test", .{
        .storage = storage.storage(),
        .flush_threshold = 1,
        .defer_flush_on_commit = true,
        .max_deferred_immutable_memtables = 1,
        .background_executor = &executor,
    });
    defer backend.close();

    {
        var txn = try backend.beginWrite();
        try txn.put(.{}, "key:a", "a");
        try txn.commit();
    }

    try std.testing.expect(lane.submitted_job != null);
    try std.testing.expect(backend.immutable_flush_job_in_flight);
    try std.testing.expectEqual(@as(usize, 1), backend.activeImmutableMemtableCount());
    try std.testing.expectEqual(@as(usize, 0), backend.runs.items.len);

    {
        var txn = try backend.beginWrite();
        try txn.put(.{}, "key:b", "b");
        try txn.commit();
    }

    try std.testing.expect(lane.submitted_job != null);
    try std.testing.expect(backend.immutable_flush_job_in_flight);
    try std.testing.expectEqual(@as(usize, 1), backend.activeImmutableMemtableCount());
    try std.testing.expectEqual(@as(usize, 1), backend.runs.items.len);

    var job = lane.submitted_job.?;
    lane.submitted_job = null;
    try job.run(job.ptr);
    job.deinit(job.ptr);

    try std.testing.expect(!backend.immutable_flush_job_in_flight);
    try std.testing.expectEqual(@as(usize, 0), backend.activeImmutableMemtableCount());
    try std.testing.expectEqual(@as(usize, 2), backend.runs.items.len);
}

test "lsm backend deferred immutable backpressure does not spin behind in-flight build" {
    const FakeLane = struct {
        submitted_job: ?background_runtime_mod.Job = null,

        fn lane(self: *@This()) background_runtime_mod.DurableJobLane {
            return .{
                .ptr = self,
                .vtable = &vtable,
            };
        }

        fn submit(ptr: *anyopaque, job: background_runtime_mod.Job) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expect(self.submitted_job == null);
            self.submitted_job = job;
        }

        fn drainOwner(_: *anyopaque, _: u64) void {}

        fn poll(_: *anyopaque, _: usize) !usize {
            return 0;
        }

        const vtable = background_runtime_mod.DurableJobLane.VTable{
            .submit = submit,
            .drain_owner = drainOwner,
            .poll = poll,
        };
    };

    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var lane = FakeLane{};
    const executor = BackgroundExecutor.initLane(lane.lane(), 781);
    var backend = try Backend.open(std.testing.allocator, "/lsm-background-inflight-backpressure-test", .{
        .storage = storage.storage(),
        .flush_threshold = 1,
        .defer_flush_on_commit = true,
        .max_deferred_immutable_memtables = 1,
        .background_executor = &executor,
    });
    defer backend.close();

    {
        var txn = try backend.beginWrite();
        try txn.put(.{}, "key:a", "a");
        try txn.commit();
    }

    try std.testing.expect(lane.submitted_job != null);
    try std.testing.expect(backend.immutable_flush_job_in_flight);
    try std.testing.expectEqual(@as(usize, 1), backend.activeImmutableMemtableCount());

    backend.immutable_flush_build_in_flight = true;
    {
        var txn = try backend.beginWrite();
        try txn.put(.{}, "key:b", "b");
        try txn.commit();
    }
    try std.testing.expectEqual(@as(usize, 2), backend.activeImmutableMemtableCount());
    try std.testing.expectEqual(@as(usize, 0), backend.runs.items.len);

    backend.immutable_flush_build_in_flight = false;
    var job = lane.submitted_job.?;
    lane.submitted_job = null;
    try job.run(job.ptr);
    job.deinit(job.ptr);

    try std.testing.expect(!backend.immutable_flush_job_in_flight);
    try std.testing.expectEqual(@as(usize, 0), backend.activeImmutableMemtableCount());
    try std.testing.expectEqual(@as(usize, 2), backend.runs.items.len);
}

test "lsm backend manual runtime flush progress does not require threads" {
    var runtime = try background_runtime_mod.BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .manual });
    defer runtime.deinit();

    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    const executor = BackgroundExecutor.init(runtime.ptr(), 780);
    var backend = try Backend.open(std.testing.allocator, "/lsm-background-manual-progress-test", .{
        .storage = storage.storage(),
        .flush_threshold = 1,
        .defer_flush_on_commit = true,
        .background_executor = &executor,
    });
    defer backend.close();

    try std.testing.expect(!backend.background_executor.canRunDetached());

    {
        var txn = try backend.beginWrite();
        try txn.put(.{}, "key", "value");
        try txn.commit();
    }

    try std.testing.expectEqual(@as(usize, 1), backend.activeImmutableMemtableCount());
    try std.testing.expectEqual(@as(usize, 0), backend.runs.items.len);
    try std.testing.expectEqual(@as(usize, 0), try backend.background_executor.poll(1));

    try std.testing.expect(try backend.runMaintenanceStep());

    try std.testing.expectEqual(@as(usize, 0), backend.activeImmutableMemtableCount());
    try std.testing.expectEqual(@as(usize, 1), backend.runs.items.len);
    try std.testing.expectEqualStrings("value", try backend.getMergedWithMutable(&backend.mutable, .{}, "key"));
}

test "lsm backends share one threaded runtime durable lane" {
    if (builtin.os.tag == .freestanding) return;

    var runtime = try background_runtime_mod.BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    defer runtime.deinit();

    var first_storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer first_storage.deinit();
    var second_storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer second_storage.deinit();

    const first_executor = BackgroundExecutor.init(runtime.ptr(), runtime.ptr().allocOwnerId());
    const second_executor = BackgroundExecutor.init(runtime.ptr(), runtime.ptr().allocOwnerId());
    var first = try Backend.open(std.testing.allocator, "/lsm-background-shared-runtime-first", .{
        .storage = first_storage.storage(),
        .background_executor = &first_executor,
    });
    defer first.close();
    var second = try Backend.open(std.testing.allocator, "/lsm-background-shared-runtime-second", .{
        .storage = second_storage.storage(),
        .background_executor = &second_executor,
    });
    defer second.close();

    try std.testing.expect(first.background_executor.canRunDetached());
    try std.testing.expect(second.background_executor.canRunDetached());
    try std.testing.expect(first.background_executor.jobs != null);
    try std.testing.expect(second.background_executor.jobs != null);
    try std.testing.expectEqual(runtime.ptr().durable_jobs.ptr, first.background_executor.jobs.?.ptr);
    try std.testing.expectEqual(runtime.ptr().durable_jobs.ptr, second.background_executor.jobs.?.ptr);
    try std.testing.expect(first.background_executor.owner_id != second.background_executor.owner_id);
}

test "lsm backend replays committed mutable writes from wal after crash reopen" {
    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    const root_dir = "/lsm-wal-crash-reopen";
    const options = Options{
        .flush_threshold = 1024,
        .storage = storage.storage(),
    };

    var backend = try Backend.open(std.testing.allocator, root_dir, options);
    {
        var txn = try backend.beginWrite();
        defer txn.abort();
        try txn.put(.{ .name = "docs" }, "doc:a", "alpha");
        try txn.commit();
    }
    try std.testing.expectEqual(@as(usize, 1), backend.mutable.entries.items.len);

    backend.options.backend.read_only = true;
    backend.close();

    backend = try Backend.open(std.testing.allocator, root_dir, options);
    defer backend.close();
    var read = try backend.beginRead();
    defer read.abort();
    try std.testing.expectEqualStrings("alpha", try read.get(.{ .name = "docs" }, "doc:a"));
    try std.testing.expect(backend.write_stats.wal_replay_records > 0);
}

test "lsm backend maintenance stats report retained wal debt across reopen and reset" {
    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    const root_dir = "/lsm-wal-retained-stats";
    const options = Options{
        .flush_threshold = 1024,
        .storage = storage.storage(),
    };

    var backend = try Backend.open(std.testing.allocator, root_dir, options);
    {
        var txn = try backend.beginWrite();
        defer txn.abort();
        try txn.put(.{ .name = "docs" }, "doc:a", "alpha");
        try txn.commit();
    }

    var maintenance = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(@as(u64, 1), maintenance.wal_retained_segments);
    try std.testing.expect(maintenance.wal_retained_bytes > 0);
    try std.testing.expectEqual(@as(u64, 1), maintenance.wal_checkpoint_oldest_retained_segment);
    try std.testing.expectEqual(@as(u64, 1), maintenance.wal_checkpoint_current_segment);
    try std.testing.expectEqual(@as(u64, 0), maintenance.wal_checkpoint_lag_segments);

    backend.options.backend.read_only = true;
    backend.close();

    backend = try Backend.open(std.testing.allocator, root_dir, options);
    defer backend.close();
    maintenance = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(@as(u64, 1), maintenance.wal_retained_segments);
    try std.testing.expect(maintenance.wal_retained_bytes > 0);
    try std.testing.expectEqual(@as(u64, 1), maintenance.wal_checkpoint_oldest_retained_segment);
    try std.testing.expectEqual(@as(u64, 1), maintenance.wal_checkpoint_current_segment);
    try std.testing.expectEqual(@as(u64, 0), maintenance.wal_checkpoint_lag_segments);
    try std.testing.expect(backend.write_stats.wal_replay_records > 0);

    try backend.resetWalAfterManifestCheckpoint();
    maintenance = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(@as(u64, 0), maintenance.wal_retained_segments);
    try std.testing.expectEqual(@as(u64, 0), maintenance.wal_retained_bytes);
    try std.testing.expectEqual(@as(u64, 1), maintenance.wal_checkpoint_oldest_retained_segment);
    try std.testing.expectEqual(@as(u64, 1), maintenance.wal_checkpoint_current_segment);
    try std.testing.expectEqual(@as(u64, 0), maintenance.wal_checkpoint_lag_segments);
}

test "lsm backend accounts in-memory recovery state in the resource manager and releases it on close" {
    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var manager = resource_manager_mod.ResourceManager.init(.{});
    const root_dir = "/lsm-in-memory-resource-accounting";
    const options = Options{
        .flush_threshold = 1024,
        .storage = storage.storage(),
        .resource_manager = &manager,
    };

    var backend = try Backend.open(std.testing.allocator, root_dir, options);
    {
        var txn = try backend.beginWrite();
        defer txn.abort();
        try txn.put(.{ .name = "docs" }, "doc:a", "alpha");
        try txn.commit();
    }

    const maintenance = backend.snapshotMaintenanceStats();
    const expected_in_memory_bytes = maintenance.mutable_bytes + maintenance.immutable_bytes;
    try std.testing.expect(expected_in_memory_bytes > 0);
    try std.testing.expectEqual(expected_in_memory_bytes, manager.sliceStats(.lsm_in_memory_state).used_bytes);

    backend.close();
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_in_memory_state).used_bytes);
}

test "lsm backend eagerly accounts mutable state and wal write working set" {
    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var manager = resource_manager_mod.ResourceManager.init(.{});
    const root_dir = "/lsm-eager-resource-accounting";
    const options = Options{
        .flush_threshold = 1024,
        .storage = storage.storage(),
        .resource_manager = &manager,
    };

    var backend = try Backend.open(std.testing.allocator, root_dir, options);
    defer backend.close();

    {
        var txn = try backend.beginWrite();
        defer txn.abort();
        try txn.put(.{ .name = "docs" }, "doc:a", "alpha");
        try txn.commit();
    }

    try std.testing.expect(manager.sliceStats(.lsm_in_memory_state).used_bytes > 0);
    const wal_stats = manager.sliceStats(.lsm_wal_write_working_set);
    try std.testing.expectEqual(@as(u64, 0), wal_stats.used_bytes);
    try std.testing.expect(wal_stats.peak_bytes > 0);
}

test "lsm backend accounts retained wal bytes in the resource manager" {
    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var manager = resource_manager_mod.ResourceManager.init(.{});
    const root_dir = "/lsm-wal-retention-resource-accounting";
    const options = Options{
        .flush_threshold = 1024,
        .storage = storage.storage(),
        .resource_manager = &manager,
    };

    var backend = try Backend.open(std.testing.allocator, root_dir, options);
    {
        var txn = try backend.beginWrite();
        defer txn.abort();
        try txn.put(.{ .name = "docs" }, "doc:a", "alpha");
        try txn.commit();
    }

    var maintenance = backend.snapshotMaintenanceStats();
    try std.testing.expect(maintenance.wal_retained_bytes > 0);
    try std.testing.expectEqual(maintenance.wal_retained_bytes, manager.sliceStats(.lsm_wal_retention).used_bytes);

    try backend.finalizeDeferredStorageWork();
    maintenance = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(@as(u64, 0), maintenance.wal_retained_bytes);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_wal_retention).used_bytes);

    {
        var txn = try backend.beginWrite();
        defer txn.abort();
        try txn.put(.{ .name = "docs" }, "doc:b", "beta");
        try txn.commit();
    }
    try std.testing.expect(manager.sliceStats(.lsm_wal_retention).used_bytes > 0);

    backend.close();
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_wal_retention).used_bytes);
}

test "lsm backend retires covered wal segments after durable manifest publish" {
    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    const root_dir = "/lsm-wal-partial-checkpoint";
    const options = Options{
        .flush_threshold = 1024,
        .storage = storage.storage(),
        .wal_segment_bytes = 32,
    };

    var backend = try Backend.open(std.testing.allocator, root_dir, options);

    {
        var txn = try backend.beginWrite();
        defer txn.abort();
        try txn.put(.{ .name = "docs" }, "doc:a", "alpha");
        try txn.commit();
    }
    {
        var txn = try backend.beginWrite();
        defer txn.abort();
        try txn.put(.{ .name = "docs" }, "doc:b", "beta");
        try txn.commit();
    }

    try std.testing.expectEqual(@as(u64, 2), backend.snapshotMaintenanceStats().wal_retained_segments);

    try backend.rotateMutableToImmutable();

    {
        var txn = try backend.beginWrite();
        defer txn.abort();
        try txn.put(.{ .name = "docs" }, "doc:c", "gamma");
        try txn.commit();
    }

    try std.testing.expectEqual(@as(u64, 3), backend.snapshotMaintenanceStats().wal_retained_segments);
    {
        const locked = runtime_mod.lockBackend(Backend, &backend);
        defer runtime_mod.unlockBackend(Backend, &backend, locked);
        try std.testing.expect(try backend.flushOldestImmutableMemtable());
    }

    var maintenance = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(@as(u64, 1), maintenance.wal_retained_segments);
    try std.testing.expect(maintenance.wal_retained_bytes > 0);
    try std.testing.expectEqual(@as(u64, 3), maintenance.wal_checkpoint_oldest_retained_segment);
    try std.testing.expectEqual(@as(u64, 3), maintenance.wal_checkpoint_current_segment);
    try std.testing.expectEqual(@as(u64, 0), maintenance.wal_checkpoint_lag_segments);

    backend.options.backend.read_only = true;
    backend.close();

    backend = try Backend.open(std.testing.allocator, root_dir, options);
    defer backend.close();
    maintenance = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(@as(u64, 1), maintenance.wal_retained_segments);
    try std.testing.expectEqual(@as(u64, 3), maintenance.wal_checkpoint_oldest_retained_segment);
    try std.testing.expectEqual(@as(u64, 3), maintenance.wal_checkpoint_current_segment);
    try std.testing.expectEqual(@as(u64, 0), maintenance.wal_checkpoint_lag_segments);
    try std.testing.expect(backend.write_stats.wal_replay_records <= 1);

    var read = try backend.beginRead();
    defer read.abort();
    try std.testing.expectEqualStrings("alpha", try read.get(.{ .name = "docs" }, "doc:a"));
    try std.testing.expectEqualStrings("beta", try read.get(.{ .name = "docs" }, "doc:b"));
    try std.testing.expectEqualStrings("gamma", try read.get(.{ .name = "docs" }, "doc:c"));
}

test "lsm backend wal pressure maintenance flushes and checkpoints retained segments" {
    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    const root_dir = "/lsm-wal-pressure-maintenance";
    const options = Options{
        .flush_threshold = 1024,
        .storage = storage.storage(),
        .wal_segment_bytes = 32,
        .wal_soft_limit_segments = 1,
        .compact_threshold_runs = 100,
    };

    var backend = try Backend.open(std.testing.allocator, root_dir, options);
    defer backend.close();

    {
        var txn = try backend.beginWrite();
        defer txn.abort();
        try txn.put(.{ .name = "docs" }, "doc:a", "alpha");
        try txn.commit();
    }
    {
        var txn = try backend.beginWrite();
        defer txn.abort();
        try txn.put(.{ .name = "docs" }, "doc:b", "beta");
        try txn.commit();
    }

    var maintenance = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(@as(u64, 2), maintenance.wal_retained_segments);
    try std.testing.expectEqual(@as(u64, 1), maintenance.wal_checkpoint_oldest_retained_segment);
    try std.testing.expectEqual(@as(u64, 2), maintenance.wal_checkpoint_current_segment);
    try std.testing.expectEqual(@as(u64, 1), maintenance.wal_checkpoint_lag_segments);
    try std.testing.expect(backend.maintenanceScore() > 0);

    try std.testing.expect(try backend.runMaintenanceStep());

    maintenance = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(@as(u64, 0), maintenance.mutable_entries);
    try std.testing.expectEqual(@as(u64, 0), maintenance.immutable_memtables);
    try std.testing.expectEqual(@as(u64, 0), maintenance.wal_retained_segments);
    try std.testing.expectEqual(@as(u64, 0), maintenance.wal_retained_bytes);
    try std.testing.expectEqual(@as(u64, 1), maintenance.wal_checkpoint_oldest_retained_segment);
    try std.testing.expectEqual(@as(u64, 1), maintenance.wal_checkpoint_current_segment);
    try std.testing.expectEqual(@as(u64, 0), maintenance.wal_checkpoint_lag_segments);

    var read = try backend.beginRead();
    defer read.abort();
    try std.testing.expectEqualStrings("alpha", try read.get(.{ .name = "docs" }, "doc:a"));
    try std.testing.expectEqualStrings("beta", try read.get(.{ .name = "docs" }, "doc:b"));
}

test "lsm backend hard wal pressure forces foreground checkpoint on commit" {
    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    const root_dir = "/lsm-hard-wal-pressure-commit";
    const options = Options{
        .flush_threshold = 1024,
        .storage = storage.storage(),
        .wal_segment_bytes = 32,
        .wal_hard_limit_segments = 1,
        .compact_threshold_runs = 100,
    };

    var backend = try Backend.open(std.testing.allocator, root_dir, options);
    defer backend.close();

    {
        var txn = try backend.beginWrite();
        defer txn.abort();
        try txn.put(.{ .name = "docs" }, "doc:a", "alpha");
        try txn.commit();
    }
    {
        var txn = try backend.beginWrite();
        defer txn.abort();
        try txn.put(.{ .name = "docs" }, "doc:b", "beta");
        try txn.commit();
    }

    const write_stats = backend.snapshotWriteStats();
    try std.testing.expect(write_stats.wal_pressure_flushes > 0);
    try std.testing.expect(write_stats.wal_pressure_ns > 0);

    const maintenance = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(@as(u64, 0), maintenance.mutable_entries);
    try std.testing.expectEqual(@as(u64, 0), maintenance.immutable_memtables);
    try std.testing.expectEqual(@as(u64, 0), maintenance.wal_retained_segments);
    try std.testing.expectEqual(@as(u64, 0), maintenance.wal_retained_bytes);
    try std.testing.expectEqual(@as(u64, 1), maintenance.wal_checkpoint_oldest_retained_segment);
    try std.testing.expectEqual(@as(u64, 1), maintenance.wal_checkpoint_current_segment);
    try std.testing.expectEqual(@as(u64, 0), maintenance.wal_checkpoint_lag_segments);

    var read = try backend.beginRead();
    defer read.abort();
    try std.testing.expectEqualStrings("alpha", try read.get(.{ .name = "docs" }, "doc:a"));
    try std.testing.expectEqualStrings("beta", try read.get(.{ .name = "docs" }, "doc:b"));
}

test "lsm backend byte flush window coalesces hot overwrites before run publication" {
    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var backend = try Backend.open(std.testing.allocator, "/lsm-hot-overwrite-byte-window", .{
        .backend = .{ .create_if_missing = true },
        .storage = storage.storage(),
        .flush_threshold = 1,
        .flush_threshold_bytes = 8 * 1024 * 1024,
        .compact_threshold_runs = 100,
    });
    defer backend.close();

    const value = try std.testing.allocator.alloc(u8, 512);
    defer std.testing.allocator.free(value);
    const update = try std.testing.allocator.alloc(u8, 512);
    defer std.testing.allocator.free(update);
    @memset(value, 'a');
    @memset(update, 'b');

    var i: usize = 0;
    while (i < 2000) : (i += 500) {
        var txn = try backend.beginWrite();
        errdefer txn.abort();
        var j = i;
        while (j < i + 500) : (j += 1) {
            var key_buf: [32]u8 = undefined;
            const key = try std.fmt.bufPrint(&key_buf, "doc:{d:0>8}", .{j});
            try txn.put(.{ .name = "docs" }, key, value);
        }
        try txn.commit();
    }
    try backend.finalizeDeferredStorageWork();

    try std.testing.expectEqual(@as(usize, 1), countLevelRuns(backend.runs.items, 0));
    try std.testing.expectEqual(@as(usize, 2000), try countRunEntriesForTest(&backend));

    var round: usize = 0;
    while (round < 2) : (round += 1) {
        var start: usize = 0;
        while (start < 500) : (start += 250) {
            var txn = try backend.beginWrite();
            errdefer txn.abort();
            var j = start;
            while (j < start + 250) : (j += 1) {
                var key_buf: [32]u8 = undefined;
                const key = try std.fmt.bufPrint(&key_buf, "doc:{d:0>8}", .{j});
                try txn.put(.{ .name = "docs" }, key, update);
            }
            try txn.commit();
        }
    }
    try std.testing.expectEqual(@as(usize, 500), backend.mutable.entries.items.len);
    try backend.finalizeDeferredStorageWork();

    try std.testing.expectEqual(@as(usize, 2), countLevelRuns(backend.runs.items, 0));
    try std.testing.expectEqual(@as(usize, 2500), try countRunEntriesForTest(&backend));
    try std.testing.expectEqualStrings(update, try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "doc:00000000"));
    try std.testing.expectEqualStrings(update, try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "doc:00000499"));
}

test "lsm backend runtime erases bound store handles with cursor access across runs" {
    var backend = Backend.init(std.testing.allocator, .{ .flush_threshold = 2 });
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    {
        var txn = try runtime.beginWrite();
        try txn.put("doc:a", "A1");
        try txn.put("doc:b", "B1");
        try txn.commit();
    }

    {
        var txn = try runtime.beginWrite();
        try txn.put("doc:a", "A2");
        try txn.put("doc:c", "C1");
        try txn.commit();
    }

    {
        var txn = try runtime.beginRead();
        defer txn.abort();
        try std.testing.expectEqualStrings("A2", try txn.get("doc:a"));
        try std.testing.expectEqualStrings("B1", try txn.get("doc:b"));
        try std.testing.expectEqualStrings("C1", try txn.get("doc:c"));

        var cur = try txn.openCursor();
        defer cur.close();
        try std.testing.expectEqualStrings("doc:a", (try cur.first()).?.key);
        try std.testing.expectEqualStrings("doc:b", (try cur.next()).?.key);
        try std.testing.expectEqualStrings("doc:c", (try cur.next()).?.key);
        try std.testing.expectEqualStrings("doc:c", (try cur.last()).?.key);
        try std.testing.expectEqualStrings("doc:b", (try cur.prev()).?.key);
    }
}

test "lsm backend runtime cursor seeks internal graph artifact prefix before replay keys" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});

    var backend = try Backend.open(alloc, path, .{
        .flush_threshold = 1,
    });
    defer backend.close();

    var runtime = try backend.runtimeStore(alloc, .{});
    defer runtime.deinit();

    const prefix = try internal_keys.graphArtifactIndexPrefixAlloc(alloc, "doc:0000", "gr_v1");
    defer alloc.free(prefix);
    const exact_key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:0000", "gr_v1", "links", "doc:0001");
    defer alloc.free(exact_key);

    {
        var txn = try runtime.beginWrite();
        var i: usize = 0;
        while (i < 1500) : (i += 1) {
            const target = try std.fmt.allocPrint(alloc, "doc:{d:0>4}", .{i});
            defer alloc.free(target);
            const key = try internal_keys.graphEdgeArtifactKeyAlloc(
                alloc,
                "doc:0000",
                "gr_v1",
                "links",
                target,
            );
            defer alloc.free(key);
            try txn.put(key, "{}");
        }
        const replay_key = internal_keys.replayEntryKey(4, 1);
        try txn.put(replay_key[0..], "replay");
        try txn.commit();
    }

    var read = try runtime.beginRead();
    defer read.abort();
    try std.testing.expectEqualStrings("{}", try read.get(exact_key));

    try std.testing.expect(backend.runs.items.len > 0);
    const run = &backend.runs.items[0];
    const path_str = run.path orelse return error.TestUnexpectedResult;
    const table = try backend.getCachedRunTable(path_str, run.id);
    const positioned = (try table.lowerBoundPosition(null, prefix, true)) orelse return error.TestUnexpectedResult;
    try std.testing.expect(std.mem.startsWith(u8, positioned.entry.key, prefix));

    var cur = try read.openCursor();
    defer cur.close();
    const found = (try cur.seekAtOrAfter(prefix)) orelse return error.TestUnexpectedResult;
    try std.testing.expect(std.mem.startsWith(u8, found.key, prefix));
}

test "lsm backend bulk ingest batches use an elevated flush threshold" {
    var backend = Backend.init(std.testing.allocator, .{
        .flush_threshold = 1,
        .bulk_ingest_flush_threshold_multiplier = 4,
    });
    defer backend.close();

    {
        var txn = try backend.beginBatchWithOptions(.{ .mode = .bulk_ingest });
        try txn.put(.{ .name = "docs" }, "doc:a", "A");
        try txn.commit();
    }

    try std.testing.expectEqual(@as(usize, 0), backend.runs.items.len);
    try std.testing.expectEqual(@as(usize, 1), backend.mutable.entries.items.len);

    {
        var txn = try backend.beginBatchWithOptions(.{ .mode = .bulk_ingest });
        try txn.put(.{ .name = "docs" }, "doc:b", "B");
        try txn.commit();
    }

    try std.testing.expectEqual(@as(usize, 0), backend.runs.items.len);
    try std.testing.expectEqual(@as(usize, 2), backend.mutable.entries.items.len);

    {
        var txn = try backend.beginWrite();
        try txn.put(.{ .name = "docs" }, "doc:c", "C");
        try txn.commit();
    }

    try std.testing.expectEqual(@as(usize, 1), backend.runs.items.len);
    try std.testing.expectEqual(@as(usize, 0), backend.mutable.entries.items.len);
    try std.testing.expectEqualStrings("C", try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "doc:c"));
}

test "lsm backend direct-ingests threshold-sized bulk batches" {
    var backend = Backend.init(std.testing.allocator, .{
        .flush_threshold = 1,
        .bulk_ingest_flush_threshold_multiplier = 4,
    });
    defer backend.close();

    {
        var txn = try backend.beginBatchWithOptions(.{ .mode = .bulk_ingest });
        try txn.appendPut(.{ .name = "docs" }, "doc:a", "A");
        try txn.appendPut(.{ .name = "docs" }, "doc:b", "B");
        try txn.appendPut(.{ .name = "docs" }, "doc:c", "C");
        try txn.appendPut(.{ .name = "docs" }, "doc:d", "D");
        try txn.commit();
    }

    const stats = backend.snapshotWriteStats();
    try std.testing.expectEqual(@as(usize, 1), backend.runs.items.len);
    try std.testing.expectEqual(@as(usize, 0), backend.mutable.entries.items.len);
    try std.testing.expectEqual(@as(u64, 0), stats.flushes);
    try std.testing.expectEqual(@as(u64, 1), stats.sorted_ingest_runs);
    try std.testing.expectEqualStrings("A", try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "doc:a"));
    try std.testing.expectEqualStrings("D", try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "doc:d"));
}

test "lsm backend direct bulk ingest cursor hides older overlapping l0 values" {
    var backend = Backend.init(std.testing.allocator, .{
        .flush_threshold = 1,
        .bulk_ingest_flush_threshold_multiplier = 1,
    });
    defer backend.close();

    {
        var txn = try backend.beginBatchWithOptions(.{ .mode = .bulk_ingest });
        try txn.put(.{ .name = "docs" }, "doc:a", "A1");
        try txn.put(.{ .name = "docs" }, "doc:b", "B1");
        try txn.commit();
    }
    {
        var txn = try backend.beginBatchWithOptions(.{ .mode = .bulk_ingest });
        try txn.put(.{ .name = "docs" }, "doc:a", "A2");
        try txn.put(.{ .name = "docs" }, "doc:c", "C2");
        try txn.commit();
    }

    const stats = backend.snapshotWriteStats();
    try std.testing.expectEqual(@as(u64, 2), stats.sorted_ingest_runs);
    try std.testing.expectEqualStrings("A2", try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "doc:a"));

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();
    var read = try runtime.beginRead();
    defer read.abort();
    var cur = try read.openCursor();
    defer cur.close();

    const first = (try cur.first()).?;
    try std.testing.expectEqualStrings("doc:a", first.key);
    try std.testing.expectEqualStrings("A2", first.value);
    const second = (try cur.next()).?;
    try std.testing.expectEqualStrings("doc:b", second.key);
    try std.testing.expectEqualStrings("B1", second.value);
    const third = (try cur.next()).?;
    try std.testing.expectEqualStrings("doc:c", third.key);
    try std.testing.expectEqualStrings("C2", third.value);
    try std.testing.expect((try cur.next()) == null);

    var visible = try backend.materializeVisibleState();
    defer visible.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("A2", try visible.get(.{ .name = "docs" }, "doc:a"));
}

test "lsm backend can disable direct bulk ingest for overwrite-heavy stores" {
    var backend = Backend.init(std.testing.allocator, .{
        .flush_threshold = 1,
        .bulk_ingest_flush_threshold_multiplier = 4,
        .direct_bulk_ingest = false,
    });
    defer backend.close();

    {
        var txn = try backend.beginBatchWithOptions(.{ .mode = .bulk_ingest });
        try txn.appendPut(.{ .name = "docs" }, "doc:a", "A");
        try txn.appendPut(.{ .name = "docs" }, "doc:b", "B");
        try txn.appendPut(.{ .name = "docs" }, "doc:c", "C");
        try txn.appendPut(.{ .name = "docs" }, "doc:d", "D");
        try txn.commit();
    }

    const stats = backend.snapshotWriteStats();
    try std.testing.expectEqual(@as(usize, 1), backend.runs.items.len);
    try std.testing.expectEqual(@as(usize, 0), backend.mutable.entries.items.len);
    try std.testing.expectEqual(@as(u64, 0), stats.sorted_ingest_runs);
    try std.testing.expectEqual(@as(u64, 1), stats.flushes);
    try std.testing.expectEqualStrings("A", try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "doc:a"));
    try std.testing.expectEqualStrings("D", try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "doc:d"));
}

test "lsm backend byte flush threshold controls mutable flushes" {
    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();
    var backend = try Backend.open(std.testing.allocator, "/lsm-byte-flush-immutable", .{
        .flush_threshold = 1000,
        .flush_threshold_bytes = 256,
        .storage = storage.storage(),
    });
    defer backend.close();

    const value = [_]u8{'x'} ** 300;
    var txn = try backend.beginWrite();
    try txn.put(.{ .name = "docs" }, "doc:a", value[0..]);
    try txn.commit();

    try std.testing.expectEqual(@as(usize, 0), backend.runs.items.len);
    try std.testing.expectEqual(@as(usize, 0), backend.mutable.entries.items.len);
    try std.testing.expectEqual(@as(usize, 1), backend.immutable_memtables.items.len);
    try std.testing.expectEqualStrings(value[0..], try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "doc:a"));
}

test "lsm backend wal backed entry threshold defers commit flush to maintenance" {
    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();
    var backend = try Backend.open(std.testing.allocator, "/lsm-entry-flush-immutable", .{
        .flush_threshold = 1,
        .defer_flush_on_commit = true,
        .storage = storage.storage(),
    });
    defer backend.close();

    {
        var txn = try backend.beginWrite();
        try txn.put(.{ .name = "docs" }, "doc:a", "A");
        try txn.commit();
    }

    var stats = backend.snapshotWriteStats();
    try std.testing.expectEqual(@as(u64, 0), stats.flushes);
    try std.testing.expectEqual(@as(usize, 0), backend.runs.items.len);
    try std.testing.expectEqual(@as(usize, 0), backend.mutable.entries.items.len);
    try std.testing.expectEqual(@as(usize, 1), backend.activeImmutableMemtableCount());
    try std.testing.expectEqualStrings("A", try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "doc:a"));

    try std.testing.expect(try backend.runMaintenanceStep());
    stats = backend.snapshotWriteStats();
    try std.testing.expectEqual(@as(u64, 1), stats.flushes);
    try std.testing.expectEqual(@as(usize, 0), backend.activeImmutableMemtableCount());
    try std.testing.expect(backend.runs.items.len > 0);
}

test "lsm backend write stats separate wal sync latency from append latency" {
    {
        var storage = storage_io.MemoryStorage.init(std.testing.allocator);
        defer storage.deinit();
        var backend = try Backend.open(std.testing.allocator, "/lsm-wal-async-stats", .{
            .storage = storage.storage(),
            .flush_threshold = 1024,
            .wal_sync_on_commit = false,
        });
        defer backend.close();

        var txn = try backend.beginWrite();
        try txn.put(.{ .name = "docs" }, "doc:a", "A");
        try txn.commit();

        const stats = backend.snapshotWriteStats();
        try std.testing.expectEqual(@as(u64, 1), stats.wal_append_records);
        try std.testing.expectEqual(@as(u64, 0), stats.wal_sync_records);
        try std.testing.expectEqual(@as(u64, 0), stats.wal_sync_ns);
    }

    {
        var storage = storage_io.MemoryStorage.init(std.testing.allocator);
        defer storage.deinit();
        var backend = try Backend.open(std.testing.allocator, "/lsm-wal-sync-stats", .{
            .storage = storage.storage(),
            .flush_threshold = 1024,
            .wal_sync_on_commit = true,
        });
        defer backend.close();

        var txn = try backend.beginWrite();
        try txn.put(.{ .name = "docs" }, "doc:a", "A");
        try txn.commit();

        const stats = backend.snapshotWriteStats();
        try std.testing.expectEqual(@as(u64, 1), stats.wal_append_records);
        try std.testing.expectEqual(@as(u64, 1), stats.wal_sync_records);
        try std.testing.expectEqual(stats.wal_append_ns, stats.wal_sync_ns);
    }
}

test "lsm backend read snapshot keeps immutable data visible after flush" {
    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();
    var backend = try Backend.open(std.testing.allocator, "/lsm-immutable-read-snapshot", .{
        .flush_threshold = 1000,
        .flush_threshold_bytes = 256,
        .storage = storage.storage(),
    });
    defer backend.close();

    const value = [_]u8{'x'} ** 300;
    {
        var txn = try backend.beginWrite();
        try txn.put(.{ .name = "docs" }, "doc:a", value[0..]);
        try txn.commit();
    }
    try std.testing.expectEqual(@as(usize, 1), backend.immutable_memtables.items.len);

    var read = try backend.beginRead();
    defer read.abort();
    try std.testing.expectEqualStrings(value[0..], try read.get(.{ .name = "docs" }, "doc:a"));
    var cursor = try read.openCursor(.{ .name = "docs" });
    defer cursor.close();
    try std.testing.expectEqualStrings("doc:a", (try cursor.first()).?.key);

    try std.testing.expect(try backend.runMaintenanceStep());
    try std.testing.expectEqual(@as(usize, 0), backend.immutable_memtables.items.len);
    try std.testing.expect(backend.runs.items.len > 0);
    try std.testing.expectEqualStrings(value[0..], try read.get(.{ .name = "docs" }, "doc:a"));
    try std.testing.expectEqualStrings("doc:a", (try cursor.seekAtOrAfter("doc:a")).?.key);
}

test "lsm backend bulk ingest byte threshold uses byte multiplier" {
    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();
    var backend = try Backend.open(std.testing.allocator, "/lsm-bulk-byte-flush-immutable", .{
        .flush_threshold = 1000,
        .flush_threshold_bytes = 256,
        .bulk_ingest_flush_threshold_bytes_multiplier = 4,
        .storage = storage.storage(),
    });
    defer backend.close();

    const value = [_]u8{'x'} ** 300;
    {
        var txn = try backend.beginBatchWithOptions(.{ .mode = .bulk_ingest });
        try txn.put(.{ .name = "docs" }, "doc:a", value[0..]);
        try txn.commit();
    }

    try std.testing.expectEqual(@as(usize, 0), backend.runs.items.len);
    try std.testing.expectEqual(@as(usize, 1), backend.mutable.entries.items.len);

    {
        var txn = try backend.beginBatchWithOptions(.{ .mode = .bulk_ingest });
        try txn.put(.{ .name = "docs" }, "doc:b", value[0..]);
        try txn.put(.{ .name = "docs" }, "doc:c", value[0..]);
        try txn.commit();
    }

    try std.testing.expectEqual(@as(usize, 0), backend.runs.items.len);
    try std.testing.expectEqual(@as(usize, 0), backend.mutable.entries.items.len);
    try std.testing.expectEqual(@as(usize, 1), backend.immutable_memtables.items.len);
}

test "lsm backend write pressure compacts hard L0 debt" {
    var backend = Backend.init(std.testing.allocator, .{
        .flush_threshold = 1,
        .compact_threshold_runs = 100,
        .l0_soft_limit_runs = 1,
        .l0_hard_limit_runs = 2,
    });
    defer backend.close();

    var i: usize = 0;
    while (i < 3) : (i += 1) {
        var key_buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "doc:{d}", .{i});
        var txn = try backend.beginWrite();
        try txn.put(.{ .name = "docs" }, key, "value");
        try txn.commit();
    }

    try backend.finalizeDeferredStorageWork();
    const stats = backend.snapshotWriteStats();
    try std.testing.expect(countLevelRuns(backend.runs.items, 0) <= 1);
    try std.testing.expect(stats.write_pressure_compactions > 0);
}

test "lsm backend maintenance step compacts soft L0 debt" {
    var backend = Backend.init(std.testing.allocator, .{
        .flush_threshold = 1,
        .compact_threshold_runs = 100,
        .l0_soft_limit_runs = 1,
        .l0_hard_limit_runs = 100,
    });
    defer backend.close();

    var i: usize = 0;
    while (i < 3) : (i += 1) {
        var key_buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "doc:{d}", .{i});
        var txn = try backend.beginWrite();
        try txn.put(.{ .name = "docs" }, key, "value");
        try txn.commit();
    }

    try std.testing.expect(backend.maintenanceScore() > 0);
    while (backend.activeImmutableMemtableCount() > 0) {
        try std.testing.expect(try backend.runMaintenanceStep());
    }
    try std.testing.expect(backend.maintenanceScore() > 0);
    try std.testing.expect(try backend.runMaintenanceStep());
    try std.testing.expect(countLevelRuns(backend.runs.items, 0) <= 1);
}

test "lsm backend public maintenance mutators serialize on backend mutex" {
    if (builtin.single_threaded) return error.SkipZigTest;

    const Worker = struct {
        const Action = enum {
            finalize,
            finish_bulk_ingest,
        };

        const Context = struct {
            backend: *Backend,
            stage: *std.atomic.Value(u8),
            action: Action,
        };

        fn run(ctx: *Context) void {
            ctx.stage.store(1, .release);
            switch (ctx.action) {
                .finalize => ctx.backend.finalizeDeferredStorageWork() catch @panic("finalizeDeferredStorageWork failed"),
                .finish_bulk_ingest => ctx.backend.finishBulkIngestSessionWithOptions(.{ .compact = false }) catch @panic("finishBulkIngestSessionWithOptions failed"),
            }
            ctx.stage.store(2, .release);
        }
    };

    const Harness = struct {
        fn expectBlocked(backend: *Backend, action: Worker.Action) !void {
            var stage = std.atomic.Value(u8).init(0);
            var ctx = Worker.Context{
                .backend = backend,
                .stage = &stage,
                .action = action,
            };

            const locked = runtime_mod.lockBackend(Backend, backend);
            const thread = try std.Thread.spawn(.{}, Worker.run, .{&ctx});

            while (stage.load(.acquire) == 0) {
                std.Thread.yield() catch {};
            }
            var spin: usize = 0;
            while (spin < 128) : (spin += 1) {
                std.Thread.yield() catch {};
            }
            const blocked_stage = stage.load(.acquire);

            if (locked) runtime_mod.unlockBackend(Backend, backend, locked);
            thread.join();
            try std.testing.expectEqual(@as(u8, 1), blocked_stage);
            try std.testing.expectEqual(@as(u8, 2), stage.load(.acquire));
        }
    };

    var backend = Backend.init(std.testing.allocator, .{
        .flush_threshold = 1,
        .bulk_ingest_flush_threshold_multiplier = 1,
        .compact_threshold_runs = 4,
    });
    defer backend.close();

    {
        var txn = try backend.beginWrite();
        try txn.put(.{ .name = "docs" }, "doc:seed", "seed");
        try txn.commit();
    }

    try backend.beginBulkIngestSession();
    {
        var txn = try backend.beginBatchWithOptions(.{ .mode = .bulk_ingest });
        try txn.appendPut(.{ .name = "docs" }, "doc:bulk", "bulk");
        try txn.commit();
    }

    try Harness.expectBlocked(&backend, .finalize);
    try Harness.expectBlocked(&backend, .finish_bulk_ingest);
    try std.testing.expectEqual(@as(usize, 0), backend.active_bulk_ingest_batches);
}

test "lsm backend default writes defer soft compaction to maintenance" {
    var backend = Backend.init(std.testing.allocator, .{
        .flush_threshold = 1,
        .compact_threshold_runs = 2,
        .l0_hard_limit_runs = 100,
    });
    defer backend.close();

    var i: usize = 0;
    while (i < 3) : (i += 1) {
        var key_buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "doc:{d}", .{i});
        var txn = try backend.beginWrite();
        try txn.put(.{ .name = "docs" }, key, "value");
        try txn.commit();
    }

    try backend.finalizeDeferredStorageWork();
    try std.testing.expectEqual(@as(usize, 0), backend.compaction_stats.compactions);
    try std.testing.expectEqual(@as(usize, 3), countLevelRuns(backend.runs.items, 0));
    try std.testing.expect(backend.maintenanceScore() > 0);

    try std.testing.expect(try backend.runMaintenanceStep());
    try std.testing.expect(backend.compaction_stats.compactions > 0);
}

test "lsm backend persisted compaction streams run blocks without full run loads" {
    const alloc = std.testing.allocator;
    var backing = storage_io.MemoryStorage.init(alloc);
    defer backing.deinit();
    const root_dir = "/lsm-streaming-compaction";

    var source_paths = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (source_paths.items) |path| alloc.free(path);
        source_paths.deinit(alloc);
    }

    {
        var backend = try Backend.open(alloc, root_dir, .{
            .storage = backing.storage(),
            .flush_threshold = 64,
            .compact_threshold_runs = 2,
            .l0_hard_limit_runs = 100,
            .table_block_compression = .none,
        });
        defer backend.close();

        var batch: usize = 0;
        while (batch < 3) : (batch += 1) {
            var txn = try backend.beginWrite();
            var key_buf: [32]u8 = undefined;
            var value_buf: [256]u8 = undefined;
            @memset(&value_buf, 'v');
            for (0..64) |i| {
                const key = try std.fmt.bufPrint(&key_buf, "doc:{d:0>4}", .{i});
                try txn.put(.{ .name = "docs" }, key, &value_buf);
            }
            try txn.commit();
        }

        try backend.finalizeDeferredStorageWork();
        try std.testing.expectEqual(@as(usize, 3), countLevelRuns(backend.runs.items, 0));
        try source_paths.ensureTotalCapacity(alloc, backend.runs.items.len);
        for (backend.runs.items) |run| {
            source_paths.appendAssumeCapacity(try alloc.dupe(u8, run.path.?));
        }
    }

    const CountingStorage = struct {
        backing: *storage_io.MemoryStorage,
        source_paths: []const []u8,
        source_full_reads: usize = 0,
        source_range_reads: usize = 0,
        source_trailer_reads: usize = 0,

        fn isSourceRunPath(self: *@This(), path: []const u8) bool {
            for (self.source_paths) |source_path| {
                if (std.mem.eql(u8, path, source_path)) return true;
            }
            return false;
        }

        fn createDirPath(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().createDirPath(path);
        }

        fn readFileAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, max_bytes: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.isSourceRunPath(path)) self.source_full_reads += 1;
            return self.backing.storage().readFileAlloc(allocator, path, max_bytes);
        }

        fn readFileRangeAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, offset: u64, len: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.isSourceRunPath(path)) self.source_range_reads += 1;
            return self.backing.storage().readFileRangeAlloc(allocator, path, offset, len);
        }

        fn fileSize(ptr: *anyopaque, path: []const u8) !u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().fileSize(path);
        }

        fn readFileTrailerAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, len: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.isSourceRunPath(path)) self.source_trailer_reads += 1;
            return self.backing.storage().readFileTrailerAlloc(allocator, path, len);
        }

        fn writeFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().writeFileAbsolute(path, contents);
        }

        fn appendFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8, sync: bool) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().appendFileAbsolute(self.backing.allocator, path, contents, sync);
        }

        fn renameAbsolute(ptr: *anyopaque, old_path: []const u8, new_path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().renameAbsolute(old_path, new_path);
        }

        fn deleteFileAbsolute(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().deleteFileAbsolute(path);
        }

        fn deleteTree(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().deleteTree(path);
        }

        fn nowNs(ptr: *anyopaque) u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().nowNs();
        }
    };

    const counting_vtable: storage_io.Storage.VTable = .{
        .create_dir_path = CountingStorage.createDirPath,
        .read_file_alloc = CountingStorage.readFileAlloc,
        .read_file_range_alloc = CountingStorage.readFileRangeAlloc,
        .file_size = CountingStorage.fileSize,
        .read_file_trailer_alloc = CountingStorage.readFileTrailerAlloc,
        .write_file_absolute = CountingStorage.writeFileAbsolute,
        .append_file_absolute = CountingStorage.appendFileAbsolute,
        .rename_absolute = CountingStorage.renameAbsolute,
        .delete_file_absolute = CountingStorage.deleteFileAbsolute,
        .delete_tree = CountingStorage.deleteTree,
        .now_ns = CountingStorage.nowNs,
    };
    var counting = CountingStorage{
        .backing = &backing,
        .source_paths = source_paths.items,
    };
    const storage = storage_io.HostStorage.init(&counting, &counting_vtable).storage();

    {
        var backend = try Backend.open(alloc, root_dir, .{
            .storage = storage,
            .flush_threshold = 64,
            .compact_threshold_runs = 2,
            .l0_hard_limit_runs = 100,
            .table_block_compression = .none,
        });
        defer backend.close();

        try std.testing.expect(try backend.runMaintenanceStep());
        try std.testing.expect(backend.compaction_stats.compactions > 0);
        try std.testing.expect(counting.source_trailer_reads > 0);
        try std.testing.expect(counting.source_range_reads > 0);
        try std.testing.expectEqual(@as(usize, 0), counting.source_full_reads);
        for (backend.runs.items) |run| {
            try std.testing.expect(run.path != null);
            try std.testing.expect(run.state == null);
        }
    }
}

test "lsm backend compaction scheduler denies and later grants capacity" {
    var backend = Backend.init(std.testing.allocator, .{
        .flush_threshold = 1,
        .compact_threshold_runs = 2,
        .l0_hard_limit_runs = 100,
        .compaction_scheduler = .{
            .max_in_flight_input_bytes = 1,
            .allow_oversized_single_job = false,
        },
    });
    defer backend.close();

    const value = [_]u8{'x'} ** 64;
    var i: usize = 0;
    while (i < 3) : (i += 1) {
        var key_buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "doc:{d}", .{i});
        var txn = try backend.beginWrite();
        try txn.put(.{ .name = "docs" }, key, value[0..]);
        try txn.commit();
    }

    try backend.finalizeDeferredStorageWork();
    try std.testing.expectEqual(@as(usize, 0), backend.compaction_stats.compactions);
    try std.testing.expect(!try backend.runMaintenanceStep());
    var maintenance = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(@as(u64, 0), maintenance.compaction_scheduler_grants);
    try std.testing.expect(maintenance.compaction_scheduler_denied_capacity > 0);
    try std.testing.expectEqual(@as(u64, 1), maintenance.compaction_scheduler_remembered_pending);
    try std.testing.expect(maintenance.compaction_scheduler_remembered_candidates > 0);

    backend.compaction_scheduler.options.max_in_flight_input_bytes = 1024 * 1024;
    try std.testing.expect(try backend.runMaintenanceStep());
    maintenance = backend.snapshotMaintenanceStats();
    try std.testing.expect(maintenance.compaction_scheduler_grants > 0);
    try std.testing.expectEqual(maintenance.compaction_scheduler_grants, maintenance.compaction_scheduler_completions);
    try std.testing.expectEqual(@as(u64, 0), maintenance.compaction_scheduler_remembered_pending);
    try std.testing.expect(maintenance.compaction_scheduler_remembered_hits > 0);
    try std.testing.expect(backend.compaction_stats.compactions > 0);
}

test "lsm backend background io budget defers immutable flush" {
    var path_buf: [256]u8 = undefined;
    const path = repository_mod.tmpPath(&path_buf, "background-io-flush-budget");
    defer repository_mod.cleanupTmp(path);
    const root_dir = std.mem.span(path);

    var backend = try Backend.open(std.testing.allocator, root_dir, .{
        .flush_threshold = 1,
        .defer_flush_on_commit = true,
        .background_io_budget_bytes = 1,
        .background_io_allow_oversized_single_job = false,
    });
    defer backend.close();

    var txn = try backend.beginWrite();
    try txn.put(.{ .name = "docs" }, "doc:1", "value large enough for a non-zero flush estimate");
    try txn.commit();

    try std.testing.expectEqual(@as(usize, 1), backend.activeImmutableMemtableCount());
    try std.testing.expect(!try backend.runMaintenanceStep());
    var maintenance = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(@as(u64, 1), maintenance.background_io_budget_bytes);
    try std.testing.expectEqual(@as(u64, 0), maintenance.background_io_reserved_bytes);
    try std.testing.expect(maintenance.background_io_denied_jobs > 0);
    try std.testing.expectEqual(@as(usize, 1), backend.activeImmutableMemtableCount());
    try std.testing.expectEqual(@as(usize, 0), backend.runs.items.len);

    backend.options.background_io_budget_bytes = 1024 * 1024;
    try std.testing.expect(try backend.runMaintenanceStep());
    maintenance = backend.snapshotMaintenanceStats();
    try std.testing.expect(maintenance.background_io_reserved_bytes > 0);
    try std.testing.expectEqual(@as(usize, 0), backend.activeImmutableMemtableCount());
    try std.testing.expectEqual(@as(usize, 1), backend.runs.items.len);
}

test "lsm backend background io budget defers scheduled compaction" {
    var backend = Backend.init(std.testing.allocator, .{
        .flush_threshold = 1,
        .compact_threshold_runs = 2,
        .l0_hard_limit_runs = 100,
        .background_io_budget_bytes = 1,
        .background_io_allow_oversized_single_job = false,
    });
    defer backend.close();

    const value = [_]u8{'x'} ** 64;
    var i: usize = 0;
    while (i < 3) : (i += 1) {
        var key_buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "doc:{d}", .{i});
        var txn = try backend.beginWrite();
        try txn.put(.{ .name = "docs" }, key, value[0..]);
        try txn.commit();
    }

    try backend.finalizeDeferredStorageWork();
    try std.testing.expectEqual(@as(usize, 3), countLevelRuns(backend.runs.items, 0));
    try std.testing.expect(!try backend.runMaintenanceStep());
    var maintenance = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(@as(u64, 0), maintenance.compaction_scheduler_grants);
    try std.testing.expect(maintenance.background_io_denied_jobs > 0);
    try std.testing.expectEqual(@as(u64, 1), maintenance.compaction_scheduler_remembered_pending);
    try std.testing.expectEqual(@as(usize, 3), countLevelRuns(backend.runs.items, 0));

    backend.options.background_io_budget_bytes = 1024 * 1024;
    try std.testing.expect(try backend.runMaintenanceStep());
    maintenance = backend.snapshotMaintenanceStats();
    try std.testing.expect(maintenance.background_io_reserved_bytes > 0);
    try std.testing.expect(maintenance.compaction_scheduler_grants > 0);
    try std.testing.expect(backend.compaction_stats.compactions > 0);
}

test "lsm backend max compaction input bytes skips oversized scheduled plan" {
    var backend = Backend.init(std.testing.allocator, .{
        .flush_threshold = 1,
        .compact_threshold_runs = 1,
        .l0_hard_limit_runs = 100,
        .max_compaction_input_bytes = 1,
    });
    defer backend.close();

    const value = [_]u8{'x'} ** 64;
    var i: usize = 0;
    while (i < 3) : (i += 1) {
        var key_buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "doc:{d}", .{i});
        var txn = try backend.beginWrite();
        try txn.put(.{ .name = "docs" }, key, value[0..]);
        try txn.commit();
    }

    try backend.finalizeDeferredStorageWork();
    try std.testing.expectEqual(@as(usize, 3), countLevelRuns(backend.runs.items, 0));
    try std.testing.expect(!try backend.runMaintenanceStep());
    var maintenance = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(@as(u64, 0), maintenance.compaction_scheduler_grants);
    try std.testing.expectEqual(@as(u64, 0), maintenance.compaction_scheduler_remembered_pending);
    try std.testing.expectEqual(@as(usize, 3), countLevelRuns(backend.runs.items, 0));

    backend.options.max_compaction_input_bytes = 1024 * 1024;
    try std.testing.expect(try backend.runMaintenanceStep());
    maintenance = backend.snapshotMaintenanceStats();
    try std.testing.expect(maintenance.compaction_scheduler_grants > 0);
    try std.testing.expect(backend.compaction_stats.compactions > 0);
    try std.testing.expect(countLevelRuns(backend.runs.items, 0) < 3);
}

test "lsm backend compaction scheduler reserves resource-manager work budget" {
    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@intFromEnum(resource_manager_mod.Slice.lsm_compaction_work)] = .{
        .soft_limit_bytes = 1,
        .hard_limit_bytes = 1,
    };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    var backend = Backend.init(std.testing.allocator, .{
        .flush_threshold = 1,
        .compact_threshold_runs = 2,
        .l0_hard_limit_runs = 100,
        .resource_manager = &manager,
        .compaction_scheduler = .{
            .resource_reservation_bytes = 2,
        },
    });
    defer backend.close();

    var i: usize = 0;
    while (i < 3) : (i += 1) {
        var key_buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "doc:{d}", .{i});
        var txn = try backend.beginWrite();
        try txn.put(.{ .name = "docs" }, key, "value");
        try txn.commit();
    }

    try backend.finalizeDeferredStorageWork();
    try std.testing.expect(!try backend.runMaintenanceStep());
    const maintenance = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(@as(u64, 0), maintenance.compaction_scheduler_grants);
    try std.testing.expect(maintenance.compaction_scheduler_denied_resource_pressure > 0);
}

test "lsm backend write stats include table compression bytes and blocks" {
    var path_buf: [256]u8 = undefined;
    const path = repository_mod.tmpPath(&path_buf, "compression-stats");
    defer repository_mod.cleanupTmp(path);
    const root_dir = std.mem.span(path);

    const value = try std.testing.allocator.alloc(u8, 8192);
    defer std.testing.allocator.free(value);
    @memset(value, 'c');

    var logical_entry_bytes: u64 = 0;
    var physical_entry_bytes: u64 = 0;

    {
        var backend = try Backend.open(std.testing.allocator, root_dir, .{
            .flush_threshold = 1,
            .wal_enabled = false,
            .table_block_compression = .snappy_adaptive,
        });
        defer backend.close();

        {
            var txn = try backend.beginWrite();
            try txn.put(.{ .name = "docs" }, "doc:a", value);
            try txn.commit();
        }

        try backend.sync(true);
        const write_stats = backend.snapshotWriteStats();
        try std.testing.expectEqual(@as(u64, 1), write_stats.table_file_writes);
        try std.testing.expect(write_stats.table_file_logical_entry_bytes > 0);
        try std.testing.expect(write_stats.table_file_physical_entry_bytes > 0);
        try std.testing.expect(write_stats.table_file_physical_entry_bytes < write_stats.table_file_logical_entry_bytes);
        try std.testing.expect(write_stats.table_file_compressed_blocks > 0);
        try std.testing.expect((write_stats.table_file_compression_codec_mask & lsm_table_file.blockCompressionCodecMask(.snappy)) != 0);

        const maintenance = backend.snapshotMaintenanceStats();
        try std.testing.expectEqual(write_stats.table_file_logical_entry_bytes, maintenance.total_run_logical_entry_bytes);
        try std.testing.expectEqual(write_stats.table_file_physical_entry_bytes, maintenance.total_run_physical_entry_bytes);
        logical_entry_bytes = maintenance.total_run_logical_entry_bytes;
        physical_entry_bytes = maintenance.total_run_physical_entry_bytes;
    }

    {
        var reopened = try Backend.open(std.testing.allocator, root_dir, .{
            .flush_threshold = 1,
            .wal_enabled = false,
            .table_block_compression = .snappy_adaptive,
        });
        defer reopened.close();

        const maintenance = reopened.snapshotMaintenanceStats();
        try std.testing.expectEqual(logical_entry_bytes, maintenance.total_run_logical_entry_bytes);
        try std.testing.expectEqual(physical_entry_bytes, maintenance.total_run_physical_entry_bytes);
        try std.testing.expect((maintenance.total_run_compression_codec_mask & lsm_table_file.blockCompressionCodecMask(.snappy)) != 0);
    }
}

test "lsm backend runtime namespace store forwards bulk ingest batch options" {
    var backend = Backend.init(std.testing.allocator, .{
        .flush_threshold = 1,
        .bulk_ingest_flush_threshold_multiplier = 4,
    });
    defer backend.close();

    var store = try backend.runtimeNamespaceStore(std.testing.allocator);
    defer store.deinit();

    {
        var txn = try store.beginBatchWithOptions(.{ .mode = .bulk_ingest });
        try txn.appendPut(.{ .name = "docs" }, "doc:a", "A");
        try txn.appendPut(.{ .name = "docs" }, "doc:b", "B");
        try txn.appendPut(.{ .name = "docs" }, "doc:c", "C");
        try txn.appendPut(.{ .name = "docs" }, "doc:d", "D");
        try txn.commit();
    }

    const stats = backend.snapshotWriteStats();
    try std.testing.expectEqual(@as(u64, 1), stats.sorted_ingest_runs);
    try std.testing.expectEqual(@as(u64, 0), stats.flushes);
    try std.testing.expectEqualStrings("A", try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "doc:a"));
    try std.testing.expectEqualStrings("D", try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "doc:d"));
}

test "lsm backend bulk ingest session defers batch finalization" {
    var backend = Backend.init(std.testing.allocator, .{
        .flush_threshold = 1,
        .bulk_ingest_flush_threshold_multiplier = 1,
        .compact_threshold_runs = 1,
        .foreground_soft_compaction = true,
    });
    defer backend.close();

    try backend.beginBulkIngestSession();
    errdefer backend.abortBulkIngestSession();

    {
        var txn = try backend.beginBatchWithOptions(.{ .mode = .bulk_ingest });
        try txn.appendPut(.{ .name = "docs" }, "doc:a", "A");
        try txn.commit();
    }
    {
        var txn = try backend.beginBatchWithOptions(.{ .mode = .bulk_ingest });
        try txn.appendPut(.{ .name = "docs" }, "doc:b", "B");
        try txn.commit();
    }

    try std.testing.expect(backend.bulkIngestActive());
    try std.testing.expectEqual(@as(usize, 2), backend.runs.items.len);
    try std.testing.expectEqual(@as(usize, 2), countLevelRuns(backend.runs.items, 0));
    try std.testing.expectEqual(@as(usize, 0), backend.mutable.entries.items.len);
    try std.testing.expectEqual(@as(u64, 0), backend.compaction_stats.compactions);

    try backend.finishBulkIngestSession();

    try std.testing.expect(!backend.bulkIngestActive());
    try std.testing.expect(backend.compaction_stats.compactions > 0);
    try std.testing.expectEqualStrings("A", try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "doc:a"));
    try std.testing.expectEqualStrings("B", try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "doc:b"));
}

test "lsm backend bulk ingest session can finish without compaction" {
    var backend = Backend.init(std.testing.allocator, .{
        .flush_threshold = 1,
        .bulk_ingest_flush_threshold_multiplier = 1,
        .compact_threshold_runs = 1,
        .foreground_soft_compaction = true,
    });
    defer backend.close();

    try backend.beginBulkIngestSession();
    errdefer backend.abortBulkIngestSession();

    {
        var txn = try backend.beginBatchWithOptions(.{ .mode = .bulk_ingest });
        try txn.appendPut(.{ .name = "docs" }, "doc:a", "A");
        try txn.commit();
    }
    {
        var txn = try backend.beginBatchWithOptions(.{ .mode = .bulk_ingest });
        try txn.appendPut(.{ .name = "docs" }, "doc:b", "B");
        try txn.commit();
    }

    try backend.finishBulkIngestSessionWithOptions(.{ .compact = false });

    try std.testing.expect(!backend.bulkIngestActive());
    try std.testing.expectEqual(@as(usize, 2), backend.runs.items.len);
    try std.testing.expectEqual(@as(usize, 2), countLevelRuns(backend.runs.items, 0));
    try std.testing.expectEqual(@as(u64, 0), backend.compaction_stats.compactions);
    try std.testing.expectEqualStrings("A", try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "doc:a"));
    try std.testing.expectEqualStrings("B", try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "doc:b"));
}

test "lsm backend bulk ingest finish can flush without compaction for wal-backed stores" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});

    var backend = try Backend.open(alloc, path, .{
        .flush_threshold = 100,
        .bulk_ingest_flush_threshold_multiplier = 100,
        .compact_threshold_runs = 100,
    });
    defer backend.close();

    try backend.beginBulkIngestSession();
    var bulk_active = true;
    errdefer if (bulk_active) backend.abortBulkIngestSession();

    {
        var txn = try backend.beginBatchWithOptions(.{ .mode = .bulk_ingest });
        try txn.appendPut(.{ .name = "docs" }, "doc:a", "A");
        try txn.appendPut(.{ .name = "docs" }, "doc:b", "B");
        try txn.commit();
    }

    try std.testing.expectEqual(@as(usize, 2), backend.mutable.entries.items.len);
    try backend.finishBulkIngestSessionWithOptions(.{ .compact = false, .flush = true });
    bulk_active = false;

    try std.testing.expect(!backend.bulkIngestActive());
    try std.testing.expectEqual(@as(usize, 0), backend.mutable.entries.items.len);
    try std.testing.expectEqual(@as(usize, 1), backend.runs.items.len);
    const stats = backend.snapshotWriteStats();
    try std.testing.expectEqual(@as(u64, 0), stats.flushes);
    try std.testing.expectEqual(@as(u64, 1), stats.sorted_ingest_runs);
    try std.testing.expectEqual(@as(u64, 0), backend.compaction_stats.compactions);
    try std.testing.expectEqualStrings("A", try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "doc:a"));
    try std.testing.expectEqualStrings("B", try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "doc:b"));
}

test "lsm backend flushes buffered writes outside bulk ingest" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});

    var backend = try Backend.open(alloc, path, .{
        .flush_threshold = 100,
        .compact_threshold_runs = 100,
    });
    defer backend.close();

    {
        var txn = try backend.beginBatch();
        try txn.appendPut(.{ .name = "docs" }, "doc:a", "A");
        try txn.appendPut(.{ .name = "docs" }, "doc:b", "B");
        try txn.commit();
    }

    try std.testing.expectEqual(@as(usize, 2), backend.mutable.entries.items.len);
    try backend.flushBufferedWritesWithOptions(.{ .compact = false });

    try std.testing.expectEqual(@as(usize, 0), backend.mutable.entries.items.len);
    try std.testing.expectEqual(@as(usize, 1), backend.runs.items.len);
    try std.testing.expectEqual(@as(u64, 0), backend.compaction_stats.compactions);
    try std.testing.expectEqualStrings("A", try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "doc:a"));
    try std.testing.expectEqualStrings("B", try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "doc:b"));
}

test "lsm backend bulk ingest session coalesces repeated overwrites before flush" {
    var backend = Backend.init(std.testing.allocator, .{
        .flush_threshold = 100,
        .bulk_ingest_flush_threshold_multiplier = 100,
        .compact_threshold_runs = 100,
    });
    defer backend.close();

    try backend.beginBulkIngestSession();
    var bulk_active = true;
    errdefer if (bulk_active) backend.abortBulkIngestSession();

    var round: usize = 0;
    while (round < 10) : (round += 1) {
        var value_a: [16]u8 = undefined;
        var value_b: [16]u8 = undefined;
        const a = try std.fmt.bufPrint(&value_a, "A-{d}", .{round});
        const b = try std.fmt.bufPrint(&value_b, "B-{d}", .{round});

        var txn = try backend.beginBatchWithOptions(.{ .mode = .bulk_ingest });
        try txn.put(.{ .name = "docs" }, "doc:a", a);
        try txn.put(.{ .name = "docs" }, "doc:b", b);
        try txn.commit();
    }

    try std.testing.expectEqual(@as(usize, 2), backend.mutable.entries.items.len);
    try backend.finishBulkIngestSessionWithOptions(.{ .compact = false });
    bulk_active = false;

    const stats = backend.snapshotWriteStats();
    try std.testing.expectEqual(@as(u64, 0), stats.flushes);
    try std.testing.expectEqual(@as(u64, 1), stats.sorted_ingest_runs);
    try std.testing.expectEqual(@as(usize, 1), backend.runs.items.len);
    try std.testing.expectEqual(@as(u32, 2), backend.runs.items[0].entry_count);
    try std.testing.expectEqualStrings("A-9", try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "doc:a"));
    try std.testing.expectEqualStrings("B-9", try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "doc:b"));
}

test "lsm backend bulk ingest session publishes one manifest" {
    var path_buf: [256]u8 = undefined;
    const path = repository_mod.tmpPath(&path_buf, "bulk-session-manifest");
    defer repository_mod.cleanupTmp(path);
    const root_dir = std.mem.span(path);

    {
        var backend = try Backend.open(std.testing.allocator, root_dir, .{
            .flush_threshold = 1,
            .bulk_ingest_flush_threshold_multiplier = 1,
            .compact_threshold_runs = 1,
        });
        defer backend.close();

        try backend.beginBulkIngestSession();
        errdefer backend.abortBulkIngestSession();

        {
            var txn = try backend.beginBatchWithOptions(.{ .mode = .bulk_ingest });
            try txn.appendPut(.{ .name = "docs" }, "doc:a", "A");
            try txn.commit();
        }
        {
            var txn = try backend.beginBatchWithOptions(.{ .mode = .bulk_ingest });
            try txn.appendPut(.{ .name = "docs" }, "doc:b", "B");
            try txn.commit();
        }

        var stats = backend.snapshotWriteStats();
        try std.testing.expectEqual(@as(u64, 0), stats.manifest_writes);
        try std.testing.expectEqual(@as(u64, 0), stats.flushes);
        try std.testing.expectEqual(@as(usize, 2), backend.runs.items.len);

        try backend.finishBulkIngestSessionWithOptions(.{ .compact = false });

        stats = backend.snapshotWriteStats();
        try std.testing.expectEqual(@as(u64, 1), stats.manifest_writes);
        try std.testing.expectEqual(@as(u64, 0), stats.flushes);
        try std.testing.expectEqual(@as(usize, 2), countLevelRuns(backend.runs.items, 0));
        try std.testing.expectEqual(@as(u64, 0), backend.compaction_stats.compactions);
    }

    {
        var reopened = try Backend.open(std.testing.allocator, root_dir, .{});
        defer reopened.close();

        try std.testing.expectEqualStrings("A", try reopened.getMergedWithMutable(&reopened.mutable, .{ .name = "docs" }, "doc:a"));
        try std.testing.expectEqualStrings("B", try reopened.getMergedWithMutable(&reopened.mutable, .{ .name = "docs" }, "doc:b"));
    }
}

test "lsm backend bulk ingest session can reopen wal-backed mutable state without finish flush" {
    var path_buf: [256]u8 = undefined;
    const path = repository_mod.tmpPath(&path_buf, "bulk-session-wal-reopen");
    defer repository_mod.cleanupTmp(path);
    const root_dir = std.mem.span(path);

    {
        var backend = try Backend.open(std.testing.allocator, root_dir, .{
            .flush_threshold = 128,
            .bulk_ingest_flush_threshold_multiplier = 8,
            .compact_threshold_runs = 8,
        });
        defer backend.close();

        try backend.beginBulkIngestSession();
        errdefer backend.abortBulkIngestSession();

        {
            var txn = try backend.beginBatchWithOptions(.{ .mode = .bulk_ingest });
            try txn.put(.{ .name = "docs" }, "doc:a", "A");
            try txn.commit();
        }

        try std.testing.expectEqual(@as(usize, 0), backend.runs.items.len);
        try std.testing.expectEqual(@as(usize, 1), backend.mutable.entries.items.len);
        try std.testing.expectEqual(@as(usize, 0), backend.activeImmutableMemtableCount());

        try backend.finishBulkIngestSessionWithOptions(.{ .compact = false });

        const stats = backend.snapshotWriteStats();
        try std.testing.expectEqual(@as(u64, 0), stats.flushes);
        try std.testing.expectEqual(@as(u64, 0), stats.manifest_writes);
        try std.testing.expectEqual(@as(usize, 0), backend.runs.items.len);
        try std.testing.expectEqual(@as(usize, 1), backend.mutable.entries.items.len);
        try std.testing.expect(!backend.bulkIngestActive());
    }

    {
        var reopened = try Backend.open(std.testing.allocator, root_dir, .{});
        defer reopened.close();

        try std.testing.expectEqualStrings("A", try reopened.getMergedWithMutable(&reopened.mutable, .{ .name = "docs" }, "doc:a"));
    }
}

test "lsm backend bulk ingest finish leaves L0 debt without foreground budget" {
    var backend = Backend.init(std.testing.allocator, .{
        .flush_threshold = 1,
        .bulk_ingest_flush_threshold_multiplier = 1,
        .compact_threshold_runs = 1,
    });
    defer backend.close();

    try backend.beginBulkIngestSession();
    var bulk_active = true;
    errdefer if (bulk_active) backend.abortBulkIngestSession();

    var i: usize = 0;
    while (i < 5) : (i += 1) {
        var key_buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "doc:{d}", .{i});
        var txn = try backend.beginBatchWithOptions(.{ .mode = .bulk_ingest });
        try txn.appendPut(.{ .name = "docs" }, key, "value");
        try txn.commit();
    }

    try std.testing.expectEqual(@as(usize, 5), countLevelRuns(backend.runs.items, 0));
    try backend.finishBulkIngestSessionWithOptions(.{
        .compact = false,
        .max_deferred_l0_runs = 2,
    });
    bulk_active = false;

    try std.testing.expectEqual(@as(usize, 5), countLevelRuns(backend.runs.items, 0));
    try std.testing.expectEqual(@as(u64, 0), backend.compaction_stats.compactions);
    try std.testing.expectEqual(@as(u64, 1), backend.maintenanceDebtHint());
    try std.testing.expectEqualStrings("value", try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "doc:0"));
    try std.testing.expectEqualStrings("value", try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "doc:4"));
}

test "lsm backend bulk ingest finish applies bounded foreground L0 budget" {
    var backend = Backend.init(std.testing.allocator, .{
        .flush_threshold = 1,
        .bulk_ingest_flush_threshold_multiplier = 1,
        .compact_threshold_runs = 1,
    });
    defer backend.close();

    try backend.beginBulkIngestSession();
    var bulk_active = true;
    errdefer if (bulk_active) backend.abortBulkIngestSession();

    var i: usize = 0;
    while (i < 5) : (i += 1) {
        var key_buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "doc:{d}", .{i});
        var txn = try backend.beginBatchWithOptions(.{ .mode = .bulk_ingest });
        try txn.appendPut(.{ .name = "docs" }, key, "value");
        try txn.commit();
    }

    try std.testing.expectEqual(@as(usize, 5), countLevelRuns(backend.runs.items, 0));
    try backend.finishBulkIngestSessionWithOptions(.{
        .compact = false,
        .max_deferred_l0_runs = 2,
        .max_foreground_compaction_steps = 1,
    });
    bulk_active = false;

    try std.testing.expectEqual(@as(usize, 3), countLevelRuns(backend.runs.items, 0));
    try std.testing.expectEqual(@as(usize, 1), backend.compaction_stats.compactions);
    try std.testing.expectEqualStrings("value", try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "doc:0"));
    try std.testing.expectEqualStrings("value", try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "doc:4"));
}

test "lsm backend hard L0 pressure applies bounded step after publish not inside compact false finish" {
    var backend = Backend.init(std.testing.allocator, .{
        .flush_threshold = 1,
        .bulk_ingest_flush_threshold_multiplier = 1,
        .compact_threshold_runs = 1,
        .l0_hard_limit_runs = 2,
    });
    defer backend.close();

    try backend.beginBulkIngestSession();
    var bulk_active = true;
    errdefer if (bulk_active) backend.abortBulkIngestSession();

    var i: usize = 0;
    while (i < 5) : (i += 1) {
        var key_buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "bulk:{d}", .{i});
        var txn = try backend.beginBatchWithOptions(.{ .mode = .bulk_ingest });
        try txn.appendPut(.{ .name = "docs" }, key, "value");
        try txn.commit();
    }

    try backend.finishBulkIngestSessionWithOptions(.{
        .compact = false,
        .max_deferred_l0_runs = 1,
    });
    bulk_active = false;
    try std.testing.expectEqual(@as(usize, 5), countLevelRuns(backend.runs.items, 0));
    try std.testing.expectEqual(@as(usize, 0), backend.compaction_stats.compactions);
    try std.testing.expectEqual(@as(u64, 0), backend.snapshotWriteStats().write_pressure_compactions);

    var txn = try backend.beginBatch();
    try txn.appendPut(.{ .name = "docs" }, "normal:0", "value");
    try txn.commit();

    try std.testing.expectEqual(@as(usize, 4), countLevelRuns(backend.runs.items, 0));
    try std.testing.expect(backend.compaction_stats.compactions > 0);
    try std.testing.expect(backend.snapshotWriteStats().write_pressure_compactions > 0);
    try std.testing.expectEqualStrings("value", try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "bulk:0"));
    try std.testing.expectEqualStrings("value", try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "normal:0"));
}

test "lsm backend bulk publish checkpoints wal without requiring compaction" {
    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    const root_dir = "/lsm-bulk-publish-wal-checkpoint-no-compaction";
    var backend = try Backend.open(std.testing.allocator, root_dir, .{
        .storage = storage.storage(),
        .flush_threshold = 1024,
        .bulk_ingest_flush_threshold_multiplier = 1024,
        .compact_threshold_runs = 1,
        .wal_segment_bytes = 32,
    });
    defer backend.close();

    try backend.beginBulkIngestSession();
    var bulk_active = true;
    errdefer if (bulk_active) backend.abortBulkIngestSession();

    {
        var txn = try backend.beginBatchWithOptions(.{ .mode = .bulk_ingest });
        try txn.appendPut(.{ .name = "docs" }, "doc:a", "alpha");
        try txn.commit();
    }
    {
        var txn = try backend.beginBatchWithOptions(.{ .mode = .bulk_ingest });
        try txn.appendPut(.{ .name = "docs" }, "doc:b", "beta");
        try txn.commit();
    }

    try std.testing.expect(backend.snapshotMaintenanceStats().wal_retained_segments > 0);
    try backend.finishBulkIngestSessionWithOptions(.{
        .compact = false,
        .flush = true,
        .max_deferred_l0_runs = 0,
    });
    bulk_active = false;

    const stats = backend.snapshotWriteStats();
    try std.testing.expectEqual(@as(usize, 0), backend.compaction_stats.compactions);
    try std.testing.expectEqual(@as(u64, 0), stats.flushes);
    try std.testing.expectEqual(@as(u64, 1), stats.sorted_ingest_runs);
    try std.testing.expect(stats.wal_resets > 0);
    try std.testing.expectEqual(@as(u64, 0), backend.snapshotMaintenanceStats().wal_retained_segments);
    try std.testing.expectEqualStrings("alpha", try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "doc:a"));
    try std.testing.expectEqualStrings("beta", try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "doc:b"));
}

test "lsm backend defers bulk ingest compaction until the last batch exits" {
    var backend = Backend.init(std.testing.allocator, .{
        .flush_threshold = 1,
        .bulk_ingest_flush_threshold_multiplier = 1,
        .compact_threshold_runs = 1,
        .foreground_soft_compaction = true,
    });
    defer backend.close();

    var first = try backend.beginBatchWithOptions(.{ .mode = .bulk_ingest });
    var second = try backend.beginBatchWithOptions(.{ .mode = .bulk_ingest });

    try first.put(.{ .name = "docs" }, "doc:a", "A");
    try first.commit();

    try std.testing.expect(backend.bulkIngestActive());
    try std.testing.expectEqual(@as(usize, 1), backend.runs.items.len);
    try std.testing.expectEqual(@as(u32, 0), backend.runs.items[0].level);

    try second.put(.{ .name = "docs" }, "doc:b", "B");
    try second.commit();

    try std.testing.expect(!backend.bulkIngestActive());
    try std.testing.expectEqual(@as(usize, 2), backend.runs.items.len);
    try std.testing.expectEqual(@as(u32, 0), backend.runs.items[0].level);
    try std.testing.expectEqual(@as(u32, 1), backend.runs.items[1].level);
}

test "lsm backend tombstones hide older run values" {
    var backend = Backend.init(std.testing.allocator, .{ .flush_threshold = 1 });
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    {
        var txn = try runtime.beginWrite();
        try txn.put("doc:a", "A");
        try txn.commit();
    }

    {
        var txn = try runtime.beginWrite();
        try txn.delete("doc:a");
        try txn.commit();
    }

    {
        var txn = try runtime.beginRead();
        defer txn.abort();
        try std.testing.expectError(error.NotFound, txn.get("doc:a"));
        var cur = try txn.openCursor();
        defer cur.close();
        try std.testing.expect((try cur.first()) == null);
    }
}

test "lsm backend cache reuses run tables across backend handles" {
    const alloc = std.testing.allocator;
    var backing = storage_io.MemoryStorage.init(alloc);
    defer backing.deinit();
    var cache = Cache.init(alloc, DefaultCacheSizeBytes);
    defer cache.deinit();

    const root_dir = "/lsm-cache-reuse";

    {
        var backend = try Backend.open(alloc, root_dir, .{
            .storage = backing.storage(),
            .cache = &cache,
            .flush_threshold = 1,
        });
        defer backend.close();

        var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginWrite();
        try txn.put("doc:a", "A");
        try txn.commit();
    }

    {
        var backend = try Backend.open(alloc, root_dir, .{
            .storage = backing.storage(),
            .cache = &cache,
            .flush_threshold = 1,
        });
        defer backend.close();

        var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginRead();
        defer txn.abort();
        try std.testing.expectEqualStrings("A", try txn.get("doc:a"));
    }

    const stats_before = cache.snapshotStats();
    const hits_before = stats_before.run_table_index.hits + stats_before.run_table_block.hits;
    {
        var backend = try Backend.open(alloc, root_dir, .{
            .storage = backing.storage(),
            .cache = &cache,
            .flush_threshold = 1,
        });
        defer backend.close();

        var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginRead();
        defer txn.abort();
        try std.testing.expectEqualStrings("A", try txn.get("doc:a"));
    }

    const stats_after = cache.snapshotStats();
    const hits_after = stats_after.run_table_index.hits + stats_after.run_table_block.hits;
    try std.testing.expect(hits_after > hits_before);
}

test "lsm backend shared cache owns loaded table allocations" {
    var cache_gpa: std.heap.DebugAllocator(.{}) = .init;
    defer std.debug.assert(cache_gpa.deinit() == .ok);
    const cache_alloc = cache_gpa.allocator();

    var backend_gpa: std.heap.DebugAllocator(.{}) = .init;
    defer std.debug.assert(backend_gpa.deinit() == .ok);
    const backend_alloc = backend_gpa.allocator();

    const test_alloc = std.testing.allocator;
    var backing = storage_io.MemoryStorage.init(test_alloc);
    defer backing.deinit();
    var cache = Cache.init(cache_alloc, DefaultCacheSizeBytes);
    defer cache.deinit();

    const root_dir = "/lsm-cache-allocator-boundary";
    var run_path: ?[]u8 = null;
    defer if (run_path) |path| test_alloc.free(path);

    const large_value = try test_alloc.alloc(u8, cache_mod.DefaultTableBlockSize / 4);
    defer test_alloc.free(large_value);
    @memset(large_value, 'v');

    {
        var backend = try Backend.open(backend_alloc, root_dir, .{
            .storage = backing.storage(),
            .flush_threshold = 6,
        });
        defer backend.close();

        var runtime = try backend.runtimeStore(backend_alloc, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginWrite();
        var key_buf: [32]u8 = undefined;
        for (0..6) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "doc:{d:0>3}", .{i});
            try txn.put(key, large_value);
        }
        try txn.commit();

        try std.testing.expectEqual(@as(usize, 1), backend.runs.items.len);
        run_path = try test_alloc.dupe(u8, backend.runs.items[0].path.?);
    }

    const Context = struct {
        backing: *storage_io.MemoryStorage,
        run_path: []const u8,
        expected_allocator: Allocator,
        checked_run_allocations: usize = 0,

        fn sameAllocator(a: Allocator, b: Allocator) bool {
            return a.ptr == b.ptr and a.vtable == b.vtable;
        }

        fn expectCacheAllocator(self: *@This(), allocator: Allocator) !void {
            if (!sameAllocator(allocator, self.expected_allocator)) return error.CacheValueAllocatorMismatch;
            self.checked_run_allocations += 1;
        }

        fn createDirPath(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().createDirPath(path);
        }

        fn readFileAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, max_bytes: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (std.mem.eql(u8, path, self.run_path)) try self.expectCacheAllocator(allocator);
            return self.backing.storage().readFileAlloc(allocator, path, max_bytes);
        }

        fn readFileRangeAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, offset: u64, len: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (std.mem.eql(u8, path, self.run_path)) try self.expectCacheAllocator(allocator);
            return self.backing.storage().readFileRangeAlloc(allocator, path, offset, len);
        }

        fn readFileTrailerAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, len: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (std.mem.eql(u8, path, self.run_path)) try self.expectCacheAllocator(allocator);
            return self.backing.storage().readFileTrailerAlloc(allocator, path, len);
        }

        fn fileSize(ptr: *anyopaque, path: []const u8) !u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().fileSize(path);
        }

        fn writeFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().writeFileAbsolute(path, contents);
        }

        fn renameAbsolute(ptr: *anyopaque, old_path: []const u8, new_path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().renameAbsolute(old_path, new_path);
        }

        fn deleteFileAbsolute(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().deleteFileAbsolute(path);
        }

        fn deleteTree(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().deleteTree(path);
        }

        fn nowNs(ptr: *anyopaque) u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().nowNs();
        }
    };

    var ctx = Context{
        .backing = &backing,
        .run_path = run_path.?,
        .expected_allocator = cache.valueAllocator(),
    };
    const host = storage_io.HostStorage.init(&ctx, &.{
        .create_dir_path = Context.createDirPath,
        .read_file_alloc = Context.readFileAlloc,
        .read_file_range_alloc = Context.readFileRangeAlloc,
        .file_size = Context.fileSize,
        .read_file_trailer_alloc = Context.readFileTrailerAlloc,
        .write_file_absolute = Context.writeFileAbsolute,
        .rename_absolute = Context.renameAbsolute,
        .delete_file_absolute = Context.deleteFileAbsolute,
        .delete_tree = Context.deleteTree,
        .now_ns = Context.nowNs,
    });

    {
        var backend = try Backend.open(backend_alloc, root_dir, .{
            .storage = host.storage(),
            .cache = &cache,
            .flush_threshold = 6,
        });
        defer backend.close();

        var runtime = try backend.runtimeStore(backend_alloc, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginRead();
        defer txn.abort();
        try std.testing.expectEqualStrings(large_value, try txn.get("doc:003"));
    }

    const stats = cache.snapshotStats();
    try std.testing.expect(stats.run_table_index.inserts > 0);
    try std.testing.expect(stats.run_table_block.inserts > 0);
    try std.testing.expect(ctx.checked_run_allocations > 0);

    cache.invalidatePath(run_path.?);
    try std.testing.expectEqual(@as(usize, 0), cache.entryCount());
}

test "lsm backend reuses local run table indexes across read snapshots" {
    const alloc = std.testing.allocator;
    var backing = storage_io.MemoryStorage.init(alloc);
    defer backing.deinit();

    var backend = try Backend.open(alloc, "/lsm-local-index-cache-reuse", .{
        .storage = backing.storage(),
        .flush_threshold = 1,
    });
    defer backend.close();

    var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime.deinit();

    {
        var txn = try runtime.beginWrite();
        try txn.put("doc:a", "A");
        try txn.commit();
    }

    try std.testing.expectEqual(@as(usize, 0), backend.run_index_cache.items.len);

    {
        var txn = try runtime.beginRead();
        defer txn.abort();
        try std.testing.expectEqualStrings("A", try txn.get("doc:a"));
    }

    try std.testing.expectEqual(@as(usize, 1), backend.run_index_cache.items.len);

    {
        var txn = try runtime.beginRead();
        defer txn.abort();
        try std.testing.expectEqualStrings("A", try txn.get("doc:a"));
    }

    try std.testing.expectEqual(@as(usize, 1), backend.run_index_cache.items.len);
}

test "lsm backend refreshes stale cached run state indexes after eviction" {
    const alloc = std.testing.allocator;
    var backing = storage_io.MemoryStorage.init(alloc);
    defer backing.deinit();

    var backend = try Backend.open(alloc, "/lsm-stale-run-state-index", .{
        .storage = backing.storage(),
        .flush_threshold = 1,
        .compact_threshold_runs = 999,
        .foreground_soft_compaction = false,
    });
    defer backend.close();

    var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime.deinit();

    {
        var txn = try runtime.beginWrite();
        try txn.put("doc:a", "A");
        try txn.commit();
    }
    {
        var txn = try runtime.beginWrite();
        try txn.put("doc:b", "B");
        try txn.commit();
    }

    try std.testing.expect(backend.runs.items.len >= 2);
    _ = try backend.resolveRunState(&backend.runs.items[0]);
    _ = try backend.resolveRunState(&backend.runs.items[1]);
    try std.testing.expectEqual(@as(usize, 2), backend.run_state_cache.items.len);
    try std.testing.expectEqual(@as(?usize, 1), backend.runs.items[1].cached_state_index);

    const first_path = backend.runs.items[0].path orelse return error.TestUnexpectedResult;
    backend.evictCachedRunStateForRun(first_path, backend.runs.items[0].id);
    try std.testing.expectEqual(@as(usize, 1), backend.run_state_cache.items.len);

    const second_state = try backend.resolveRunState(&backend.runs.items[1]);
    try std.testing.expect(second_state.entries.items.len > 0);
    try std.testing.expectEqual(@as(?usize, 0), backend.runs.items[1].cached_state_index);
}

test "lsm backend obsolete run cleanup does not invalidate shared cache by path" {
    const alloc = std.testing.allocator;
    var backing = storage_io.MemoryStorage.init(alloc);
    defer backing.deinit();
    var cache = Cache.init(alloc, DefaultCacheSizeBytes);
    defer cache.deinit();

    var backend = try Backend.open(alloc, "/lsm-obsolete-cache-cleanup", .{
        .storage = backing.storage(),
        .cache = &cache,
        .flush_threshold = 1,
        .compact_threshold_runs = 1,
        .foreground_soft_compaction = true,
    });
    defer backend.close();

    var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime.deinit();

    {
        var txn = try runtime.beginWrite();
        try txn.put("doc:a", "A");
        try txn.commit();
    }

    {
        var txn = try runtime.beginRead();
        defer txn.abort();
        try std.testing.expectEqualStrings("A", try txn.get("doc:a"));
    }

    const before = cache.snapshotStats();
    const invalidations_before = before.run_state.invalidations +
        before.run_table_raw.invalidations +
        before.run_table_index.invalidations +
        before.run_table_block.invalidations;
    try std.testing.expect(before.entry_count > 0);

    {
        var txn = try runtime.beginWrite();
        try txn.delete("doc:a");
        try txn.put("doc:b", "B");
        try txn.commit();
    }

    {
        var txn = try runtime.beginRead();
        defer txn.abort();
        try std.testing.expectError(error.NotFound, txn.get("doc:a"));
        try std.testing.expectEqualStrings("B", try txn.get("doc:b"));
    }

    const after = cache.snapshotStats();
    const invalidations_after = after.run_state.invalidations +
        after.run_table_raw.invalidations +
        after.run_table_index.invalidations +
        after.run_table_block.invalidations;
    try std.testing.expectEqual(invalidations_before, invalidations_after);
    try std.testing.expectEqual(@as(usize, 0), backend.obsolete_runs.items.len);
    try std.testing.expect(backend.obsolete_paths.items.len > 0);
}

test "lsm backend cache namespaces entries by root generation" {
    const alloc = std.testing.allocator;
    var backing = storage_io.MemoryStorage.init(alloc);
    defer backing.deinit();
    var cache = Cache.init(alloc, DefaultCacheSizeBytes);
    defer cache.deinit();

    const root_dir = "/lsm-root-generation";

    {
        var backend = try Backend.open(alloc, root_dir, .{
            .storage = backing.storage(),
            .cache = &cache,
            .flush_threshold = 1,
            .root_generation = 1,
        });
        defer backend.close();

        var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginWrite();
        try txn.put("doc:a", "A");
        try txn.commit();
    }

    {
        var backend = try Backend.open(alloc, root_dir, .{
            .storage = backing.storage(),
            .cache = &cache,
            .flush_threshold = 1,
            .root_generation = 1,
        });
        defer backend.close();

        var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginRead();
        defer txn.abort();
        try std.testing.expectEqualStrings("A", try txn.get("doc:a"));
    }

    try backing.storage().deleteTree(root_dir);

    {
        var backend = try Backend.open(alloc, root_dir, .{
            .storage = backing.storage(),
            .cache = &cache,
            .flush_threshold = 1,
            .root_generation = 2,
        });
        defer backend.close();

        var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginWrite();
        try txn.put("doc:a", "B");
        try txn.commit();
    }

    {
        var backend = try Backend.open(alloc, root_dir, .{
            .storage = backing.storage(),
            .cache = &cache,
            .flush_threshold = 1,
            .root_generation = 2,
        });
        defer backend.close();

        var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginRead();
        defer txn.abort();
        try std.testing.expectEqualStrings("B", try txn.get("doc:a"));
    }
}

test "lsm backend reuses cached raw table bytes to avoid fragmented index reads" {
    const alloc = std.testing.allocator;
    var backing = storage_io.MemoryStorage.init(alloc);
    defer backing.deinit();
    var cache = Cache.init(alloc, DefaultCacheSizeBytes);
    defer cache.deinit();

    const root_dir = "/lsm-index-from-raw";
    var run_path: ?[]u8 = null;
    defer if (run_path) |path| alloc.free(path);
    var run_id: u64 = 0;

    {
        var backend = try Backend.open(alloc, root_dir, .{
            .storage = backing.storage(),
            .cache = &cache,
            .flush_threshold = 1,
        });
        defer backend.close();

        var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginWrite();
        try txn.put("doc:a", "A");
        try txn.commit();

        try std.testing.expectEqual(@as(usize, 1), backend.runs.items.len);
        run_path = try alloc.dupe(u8, backend.runs.items[0].path.?);
        run_id = backend.runs.items[0].id;
    }

    {
        const raw = try backing.storage().readFileAlloc(alloc, run_path.?, repository_mod.maxRunFileReadBytes());
        var raw_handle = try cache.putRunTableRaw(run_path.?, run_id, run_id, raw);
        raw_handle.release();
    }

    const CountingHostContext = struct {
        backing: *storage_io.MemoryStorage,
        run_path: []const u8,
        run_file_reads: usize = 0,
        run_range_reads: usize = 0,

        fn createDirPath(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().createDirPath(path);
        }

        fn readFileAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, max_bytes: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (std.mem.eql(u8, path, self.run_path)) self.run_file_reads += 1;
            return self.backing.storage().readFileAlloc(allocator, path, max_bytes);
        }

        fn readFileRangeAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, offset: u64, len: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (std.mem.eql(u8, path, self.run_path)) self.run_range_reads += 1;
            return self.backing.storage().readFileRangeAlloc(allocator, path, offset, len);
        }

        fn fileSize(ptr: *anyopaque, path: []const u8) !u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().fileSize(path);
        }

        fn writeFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().writeFileAbsolute(path, contents);
        }

        fn renameAbsolute(ptr: *anyopaque, old_path: []const u8, new_path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().renameAbsolute(old_path, new_path);
        }

        fn deleteFileAbsolute(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().deleteFileAbsolute(path);
        }

        fn deleteTree(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().deleteTree(path);
        }

        fn nowNs(ptr: *anyopaque) u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().nowNs();
        }
    };

    const counting_host_vtable: storage_io.Storage.VTable = .{
        .create_dir_path = CountingHostContext.createDirPath,
        .read_file_alloc = CountingHostContext.readFileAlloc,
        .read_file_range_alloc = CountingHostContext.readFileRangeAlloc,
        .file_size = CountingHostContext.fileSize,
        .write_file_absolute = CountingHostContext.writeFileAbsolute,
        .rename_absolute = CountingHostContext.renameAbsolute,
        .delete_file_absolute = CountingHostContext.deleteFileAbsolute,
        .delete_tree = CountingHostContext.deleteTree,
        .now_ns = CountingHostContext.nowNs,
    };

    var host_ctx = CountingHostContext{
        .backing = &backing,
        .run_path = run_path.?,
    };
    const host_storage = storage_io.HostStorage.init(&host_ctx, &counting_host_vtable);

    {
        var backend = try Backend.open(alloc, root_dir, .{
            .storage = host_storage.storage(),
            .cache = &cache,
            .flush_threshold = 1,
        });
        defer backend.close();

        var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginRead();
        defer txn.abort();
        try std.testing.expectEqualStrings("A", try txn.get("doc:a"));
    }

    try std.testing.expectEqual(@as(usize, 0), host_ctx.run_file_reads);
    try std.testing.expect(host_ctx.run_range_reads <= 3);
}

test "lsm backend avoids full run table load on bloom negative" {
    const alloc = std.testing.allocator;
    var backing = storage_io.MemoryStorage.init(alloc);
    defer backing.deinit();
    var tracked_run_path: ?[]u8 = null;
    defer if (tracked_run_path) |path| alloc.free(path);
    var bloom_negative_key: ?[]u8 = null;
    defer if (bloom_negative_key) |key| alloc.free(key);

    const root_dir = "/lsm-manifest-bloom-negative";

    {
        var backend = try Backend.open(alloc, root_dir, .{
            .storage = backing.storage(),
            .flush_threshold = 2,
            .bloom = .{ .bits_per_key = 64, .min_bits = 1024 },
        });
        defer backend.close();

        var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginWrite();
        try txn.put("doc:a", "A");
        try txn.put("doc:c", "C");
        try txn.commit();
    }

    const Context = struct {
        backing: *storage_io.MemoryStorage,
        run_path: ?[]const u8 = null,
        run_file_reads: usize = 0,
        run_range_reads: usize = 0,
        run_trailer_reads: usize = 0,

        fn createDirPath(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().createDirPath(path);
        }

        fn readFileAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, max_bytes: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.run_path) |run_path| {
                if (std.mem.eql(u8, path, run_path)) self.run_file_reads += 1;
            }
            return self.backing.storage().readFileAlloc(allocator, path, max_bytes);
        }

        fn readFileRangeAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, offset: u64, len: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.run_path) |run_path| {
                if (std.mem.eql(u8, path, run_path)) self.run_range_reads += 1;
            }
            return self.backing.storage().readFileRangeAlloc(allocator, path, offset, len);
        }

        fn fileSize(ptr: *anyopaque, path: []const u8) !u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().fileSize(path);
        }

        fn readFileTrailerAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, len: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.run_path) |run_path| {
                if (std.mem.eql(u8, path, run_path)) self.run_trailer_reads += 1;
            }
            return self.backing.storage().readFileTrailerAlloc(allocator, path, len);
        }

        fn writeFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().writeFileAbsolute(path, contents);
        }

        fn renameAbsolute(ptr: *anyopaque, old_path: []const u8, new_path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().renameAbsolute(old_path, new_path);
        }

        fn deleteFileAbsolute(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().deleteFileAbsolute(path);
        }

        fn deleteTree(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().deleteTree(path);
        }

        fn nowNs(ptr: *anyopaque) u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().nowNs();
        }
    };

    var ctx = Context{ .backing = &backing };
    const host = storage_io.HostStorage.init(&ctx, &.{
        .create_dir_path = Context.createDirPath,
        .read_file_alloc = Context.readFileAlloc,
        .read_file_range_alloc = Context.readFileRangeAlloc,
        .file_size = Context.fileSize,
        .read_file_trailer_alloc = Context.readFileTrailerAlloc,
        .write_file_absolute = Context.writeFileAbsolute,
        .rename_absolute = Context.renameAbsolute,
        .delete_file_absolute = Context.deleteFileAbsolute,
        .delete_tree = Context.deleteTree,
        .now_ns = Context.nowNs,
    });

    {
        var manifest_backing: ?[]u8 = null;
        defer if (manifest_backing) |raw| alloc.free(raw);
        var next_run_id: u64 = 0;
        var runs = std.ArrayListUnmanaged(repository_mod.Run).empty;
        var obsolete_paths = std.ArrayListUnmanaged(repository_mod.ObsoletePath).empty;
        defer {
            for (runs.items) |*run| run.deinit(alloc);
            runs.deinit(alloc);
            for (obsolete_paths.items) |*obsolete| obsolete.deinit(alloc);
            obsolete_paths.deinit(alloc);
        }

        try std.testing.expect(try repository_mod.loadManifestIfPresentWithStorage(
            host.storage(),
            alloc,
            root_dir,
            &manifest_backing,
            &next_run_id,
            &runs,
            &obsolete_paths,
        ));
        try std.testing.expectEqual(@as(usize, 1), runs.items.len);
        tracked_run_path = try alloc.dupe(u8, runs.items[0].path.?);
        ctx.run_path = tracked_run_path.?;

        const filter = try runs.items[0].ensureBloomFilter(alloc);
        var key_buf: [64]u8 = undefined;
        var i: usize = 0;
        while (i < 10_000) : (i += 1) {
            const candidate = try std.fmt.bufPrint(&key_buf, "doc:b-{d}", .{i});
            if (!lsm_table_file.maybeContains(filter, "docs", candidate)) {
                bloom_negative_key = try alloc.dupe(u8, candidate);
                break;
            }
        }
        try std.testing.expect(bloom_negative_key != null);
    }

    {
        var backend = try Backend.open(alloc, root_dir, .{
            .storage = host.storage(),
            .flush_threshold = 2,
            .bloom = .{ .bits_per_key = 64, .min_bits = 1024 },
        });
        defer backend.close();

        ctx.run_file_reads = 0;
        ctx.run_range_reads = 0;
        ctx.run_trailer_reads = 0;

        var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginRead();
        defer txn.abort();
        try std.testing.expectError(error.NotFound, txn.get(bloom_negative_key.?));
    }

    try std.testing.expectEqual(@as(usize, 0), ctx.run_file_reads);
    try std.testing.expect(ctx.run_range_reads <= 1);
    try std.testing.expect(ctx.run_trailer_reads <= 1);
}

test "lsm backend no-cache point reads reuse local index and block cache" {
    const alloc = std.testing.allocator;
    var backing = storage_io.MemoryStorage.init(alloc);
    defer backing.deinit();
    var tracked_run_path: ?[]u8 = null;
    defer if (tracked_run_path) |path| alloc.free(path);

    const root_dir = "/lsm-no-cache-point-read";

    {
        var backend = try Backend.open(alloc, root_dir, .{
            .storage = backing.storage(),
            .flush_threshold = 1,
        });
        defer backend.close();

        var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginWrite();
        try txn.put("doc:a", "A");
        try txn.commit();
    }

    const Context = struct {
        backing: *storage_io.MemoryStorage,
        run_path: ?[]const u8 = null,
        run_file_reads: usize = 0,
        run_range_reads: usize = 0,
        run_trailer_reads: usize = 0,
        run_file_size_reads: usize = 0,

        fn createDirPath(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().createDirPath(path);
        }

        fn readFileAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, max_bytes: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.run_path) |run_path| {
                if (std.mem.eql(u8, path, run_path)) self.run_file_reads += 1;
            }
            return self.backing.storage().readFileAlloc(allocator, path, max_bytes);
        }

        fn readFileRangeAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, offset: u64, len: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.run_path) |run_path| {
                if (std.mem.eql(u8, path, run_path)) self.run_range_reads += 1;
            }
            return self.backing.storage().readFileRangeAlloc(allocator, path, offset, len);
        }

        fn fileSize(ptr: *anyopaque, path: []const u8) !u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.run_path) |run_path| {
                if (std.mem.eql(u8, path, run_path)) self.run_file_size_reads += 1;
            }
            return self.backing.storage().fileSize(path);
        }

        fn readFileTrailerAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, len: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.run_path) |run_path| {
                if (std.mem.eql(u8, path, run_path)) self.run_trailer_reads += 1;
            }
            return self.backing.storage().readFileTrailerAlloc(allocator, path, len);
        }

        fn writeFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().writeFileAbsolute(path, contents);
        }

        fn renameAbsolute(ptr: *anyopaque, old_path: []const u8, new_path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().renameAbsolute(old_path, new_path);
        }

        fn deleteFileAbsolute(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().deleteFileAbsolute(path);
        }

        fn deleteTree(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().deleteTree(path);
        }

        fn nowNs(ptr: *anyopaque) u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().nowNs();
        }
    };

    var ctx = Context{ .backing = &backing };
    const host = storage_io.HostStorage.init(&ctx, &.{
        .create_dir_path = Context.createDirPath,
        .read_file_alloc = Context.readFileAlloc,
        .read_file_range_alloc = Context.readFileRangeAlloc,
        .file_size = Context.fileSize,
        .read_file_trailer_alloc = Context.readFileTrailerAlloc,
        .write_file_absolute = Context.writeFileAbsolute,
        .rename_absolute = Context.renameAbsolute,
        .delete_file_absolute = Context.deleteFileAbsolute,
        .delete_tree = Context.deleteTree,
        .now_ns = Context.nowNs,
    });

    {
        var manifest_backing: ?[]u8 = null;
        defer if (manifest_backing) |raw| alloc.free(raw);
        var next_run_id: u64 = 0;
        var runs = std.ArrayListUnmanaged(repository_mod.Run).empty;
        var obsolete_paths = std.ArrayListUnmanaged(repository_mod.ObsoletePath).empty;
        defer {
            for (runs.items) |*run| run.deinit(alloc);
            runs.deinit(alloc);
            for (obsolete_paths.items) |*obsolete| obsolete.deinit(alloc);
            obsolete_paths.deinit(alloc);
        }

        try std.testing.expect(try repository_mod.loadManifestIfPresentWithStorage(
            host.storage(),
            alloc,
            root_dir,
            &manifest_backing,
            &next_run_id,
            &runs,
            &obsolete_paths,
        ));
        try std.testing.expectEqual(@as(usize, 1), runs.items.len);
        tracked_run_path = try alloc.dupe(u8, runs.items[0].path.?);
        ctx.run_path = tracked_run_path.?;
    }

    {
        var backend = try Backend.open(alloc, root_dir, .{
            .storage = host.storage(),
            .flush_threshold = 1,
        });
        defer backend.close();

        var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer runtime.deinit();

        {
            var txn = try runtime.beginRead();
            defer txn.abort();
            try std.testing.expectEqualStrings("A", try txn.get("doc:a"));
        }

        {
            var txn = try runtime.beginRead();
            defer txn.abort();
            try std.testing.expectEqualStrings("A", try txn.get("doc:a"));
        }
    }

    try std.testing.expectEqual(@as(usize, 0), ctx.run_file_reads);
    try std.testing.expectEqual(@as(usize, 2), ctx.run_range_reads);
    try std.testing.expectEqual(@as(usize, 1), ctx.run_trailer_reads);
    try std.testing.expectEqual(@as(usize, 0), ctx.run_file_size_reads);
}

test "lsm backend multi-block point read skips directly to one candidate block" {
    const alloc = std.testing.allocator;
    var backing = storage_io.MemoryStorage.init(alloc);
    defer backing.deinit();
    var tracked_run_path: ?[]u8 = null;
    defer if (tracked_run_path) |path| alloc.free(path);

    const root_dir = "/lsm-block-skip-point-read";
    const large_value = try alloc.alloc(u8, cache_mod.DefaultTableBlockSize / 4);
    defer alloc.free(large_value);
    @memset(large_value, 'v');

    {
        var backend = try Backend.open(alloc, root_dir, .{
            .storage = backing.storage(),
            .flush_threshold = 6,
        });
        defer backend.close();

        var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginWrite();
        var key_buf: [32]u8 = undefined;
        for (0..6) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "doc:{d:0>3}", .{i});
            try txn.put(key, large_value);
        }
        try txn.commit();
    }

    const Context = struct {
        backing: *storage_io.MemoryStorage,
        run_path: ?[]const u8 = null,
        run_file_reads: usize = 0,
        run_range_reads: usize = 0,
        run_trailer_reads: usize = 0,
        run_file_size_reads: usize = 0,

        fn createDirPath(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().createDirPath(path);
        }

        fn readFileAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, max_bytes: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.run_path) |run_path| {
                if (std.mem.eql(u8, path, run_path)) self.run_file_reads += 1;
            }
            return self.backing.storage().readFileAlloc(allocator, path, max_bytes);
        }

        fn readFileRangeAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, offset: u64, len: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.run_path) |run_path| {
                if (std.mem.eql(u8, path, run_path)) self.run_range_reads += 1;
            }
            return self.backing.storage().readFileRangeAlloc(allocator, path, offset, len);
        }

        fn fileSize(ptr: *anyopaque, path: []const u8) !u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.run_path) |run_path| {
                if (std.mem.eql(u8, path, run_path)) self.run_file_size_reads += 1;
            }
            return self.backing.storage().fileSize(path);
        }

        fn readFileTrailerAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, len: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.run_path) |run_path| {
                if (std.mem.eql(u8, path, run_path)) self.run_trailer_reads += 1;
            }
            return self.backing.storage().readFileTrailerAlloc(allocator, path, len);
        }

        fn writeFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().writeFileAbsolute(path, contents);
        }

        fn renameAbsolute(ptr: *anyopaque, old_path: []const u8, new_path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().renameAbsolute(old_path, new_path);
        }

        fn deleteFileAbsolute(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().deleteFileAbsolute(path);
        }

        fn deleteTree(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().deleteTree(path);
        }

        fn nowNs(ptr: *anyopaque) u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().nowNs();
        }
    };

    var ctx = Context{ .backing = &backing };
    const host = storage_io.HostStorage.init(&ctx, &.{
        .create_dir_path = Context.createDirPath,
        .read_file_alloc = Context.readFileAlloc,
        .read_file_range_alloc = Context.readFileRangeAlloc,
        .file_size = Context.fileSize,
        .read_file_trailer_alloc = Context.readFileTrailerAlloc,
        .write_file_absolute = Context.writeFileAbsolute,
        .rename_absolute = Context.renameAbsolute,
        .delete_file_absolute = Context.deleteFileAbsolute,
        .delete_tree = Context.deleteTree,
        .now_ns = Context.nowNs,
    });

    {
        var manifest_backing: ?[]u8 = null;
        defer if (manifest_backing) |raw| alloc.free(raw);
        var next_run_id: u64 = 0;
        var runs = std.ArrayListUnmanaged(repository_mod.Run).empty;
        var obsolete_paths = std.ArrayListUnmanaged(repository_mod.ObsoletePath).empty;
        defer {
            for (runs.items) |*run| run.deinit(alloc);
            runs.deinit(alloc);
            for (obsolete_paths.items) |*obsolete| obsolete.deinit(alloc);
            obsolete_paths.deinit(alloc);
        }

        try std.testing.expect(try repository_mod.loadManifestIfPresentWithStorage(
            host.storage(),
            alloc,
            root_dir,
            &manifest_backing,
            &next_run_id,
            &runs,
            &obsolete_paths,
        ));
        try std.testing.expectEqual(@as(usize, 1), runs.items.len);
        tracked_run_path = try alloc.dupe(u8, runs.items[0].path.?);
        ctx.run_path = tracked_run_path.?;
    }

    {
        var index = try repository_mod.loadRunTableIndexAllocWithStorage(backing.storage(), alloc, tracked_run_path.?);
        defer index.deinit(alloc);

        try std.testing.expect(index.blockCount() > 1);
        const target_block = index.findBlockIndex("docs", "doc:005") orelse return error.TestUnexpectedResult;
        try std.testing.expect(target_block > 0);
    }

    {
        var backend = try Backend.open(alloc, root_dir, .{
            .storage = host.storage(),
            .flush_threshold = 6,
        });
        defer backend.close();

        var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginRead();
        defer txn.abort();
        try std.testing.expectEqualStrings(large_value, try txn.get("doc:005"));
    }

    try std.testing.expectEqual(@as(usize, 0), ctx.run_file_reads);
    try std.testing.expectEqual(@as(usize, 2), ctx.run_range_reads);
    try std.testing.expectEqual(@as(usize, 1), ctx.run_trailer_reads);
    try std.testing.expectEqual(@as(usize, 0), ctx.run_file_size_reads);
}

test "lsm backend cached cursor scan avoids whole-run table reads" {
    const alloc = std.testing.allocator;
    var backing = storage_io.MemoryStorage.init(alloc);
    defer backing.deinit();
    var cache = Cache.init(alloc, DefaultCacheSizeBytes);
    defer cache.deinit();
    var tracked_run_path: ?[]u8 = null;
    defer if (tracked_run_path) |path| alloc.free(path);

    const root_dir = "/lsm-cursor-block-scan";
    const large_value = try alloc.alloc(u8, cache_mod.DefaultTableBlockSize / 4);
    defer alloc.free(large_value);
    @memset(large_value, 'v');

    {
        var backend = try Backend.open(alloc, root_dir, .{
            .storage = backing.storage(),
            .flush_threshold = 6,
        });
        defer backend.close();

        var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginWrite();
        var key_buf: [32]u8 = undefined;
        for (0..6) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "doc:{d:0>3}", .{i});
            try txn.put(key, large_value);
        }
        try txn.commit();
    }

    const Context = struct {
        backing: *storage_io.MemoryStorage,
        run_path: ?[]const u8 = null,
        run_file_reads: usize = 0,
        run_range_reads: usize = 0,
        run_trailer_reads: usize = 0,
        run_file_size_reads: usize = 0,

        fn createDirPath(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().createDirPath(path);
        }

        fn readFileAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, max_bytes: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.run_path) |run_path| {
                if (std.mem.eql(u8, path, run_path)) {
                    self.run_file_reads += 1;
                    return error.StreamTooLong;
                }
            }
            return self.backing.storage().readFileAlloc(allocator, path, max_bytes);
        }

        fn readFileRangeAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, offset: u64, len: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.run_path) |run_path| {
                if (std.mem.eql(u8, path, run_path)) self.run_range_reads += 1;
            }
            return self.backing.storage().readFileRangeAlloc(allocator, path, offset, len);
        }

        fn fileSize(ptr: *anyopaque, path: []const u8) !u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.run_path) |run_path| {
                if (std.mem.eql(u8, path, run_path)) self.run_file_size_reads += 1;
            }
            return self.backing.storage().fileSize(path);
        }

        fn readFileTrailerAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, len: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.run_path) |run_path| {
                if (std.mem.eql(u8, path, run_path)) self.run_trailer_reads += 1;
            }
            return self.backing.storage().readFileTrailerAlloc(allocator, path, len);
        }

        fn writeFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().writeFileAbsolute(path, contents);
        }

        fn renameAbsolute(ptr: *anyopaque, old_path: []const u8, new_path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().renameAbsolute(old_path, new_path);
        }

        fn deleteFileAbsolute(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().deleteFileAbsolute(path);
        }

        fn deleteTree(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().deleteTree(path);
        }

        fn nowNs(ptr: *anyopaque) u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().nowNs();
        }
    };

    var ctx = Context{ .backing = &backing };
    const host = storage_io.HostStorage.init(&ctx, &.{
        .create_dir_path = Context.createDirPath,
        .read_file_alloc = Context.readFileAlloc,
        .read_file_range_alloc = Context.readFileRangeAlloc,
        .file_size = Context.fileSize,
        .read_file_trailer_alloc = Context.readFileTrailerAlloc,
        .write_file_absolute = Context.writeFileAbsolute,
        .rename_absolute = Context.renameAbsolute,
        .delete_file_absolute = Context.deleteFileAbsolute,
        .delete_tree = Context.deleteTree,
        .now_ns = Context.nowNs,
    });

    {
        var manifest_backing: ?[]u8 = null;
        defer if (manifest_backing) |raw| alloc.free(raw);
        var next_run_id: u64 = 0;
        var runs = std.ArrayListUnmanaged(repository_mod.Run).empty;
        var obsolete_paths = std.ArrayListUnmanaged(repository_mod.ObsoletePath).empty;
        defer {
            for (runs.items) |*run| run.deinit(alloc);
            runs.deinit(alloc);
            for (obsolete_paths.items) |*obsolete| obsolete.deinit(alloc);
            obsolete_paths.deinit(alloc);
        }

        try std.testing.expect(try repository_mod.loadManifestIfPresentWithStorage(
            host.storage(),
            alloc,
            root_dir,
            &manifest_backing,
            &next_run_id,
            &runs,
            &obsolete_paths,
        ));
        try std.testing.expectEqual(@as(usize, 1), runs.items.len);
        tracked_run_path = try alloc.dupe(u8, runs.items[0].path.?);
        ctx.run_path = tracked_run_path.?;
    }

    {
        var index = try repository_mod.loadRunTableIndexAllocWithStorage(backing.storage(), alloc, tracked_run_path.?);
        defer index.deinit(alloc);
        try std.testing.expect(index.blockCount() > 1);
    }

    {
        var backend = try Backend.open(alloc, root_dir, .{
            .storage = host.storage(),
            .flush_threshold = 6,
            .cache = &cache,
        });
        defer backend.close();

        var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginRead();
        defer txn.abort();
        var cur = try txn.openCursor();
        defer cur.close();

        try std.testing.expectEqualStrings("doc:002", (try cur.seekAtOrAfter("doc:002")).?.key);
        try std.testing.expectEqualStrings("doc:003", (try cur.next()).?.key);
        try std.testing.expectEqualStrings("doc:004", (try cur.next()).?.key);
        try std.testing.expectEqualStrings("doc:005", (try cur.next()).?.key);
        try std.testing.expect((try cur.next()) == null);

        const read_stats = backend.snapshotReadStats();
        try std.testing.expect(read_stats.table_entry_parses > 0);
        try std.testing.expect(read_stats.table_block_loads > 0);
        try std.testing.expect(read_stats.table_block_bytes > 0);
        try std.testing.expect(read_stats.cursor_block_loads > 0);
        try std.testing.expect(read_stats.cursor_block_reuses > 0);
        try std.testing.expect(read_stats.cursor_block_readaheads > 0);
        try std.testing.expectEqual(@as(u64, 1), read_stats.cursor_table_index_misses);
        try std.testing.expect(read_stats.cursor_table_index_hits > read_stats.cursor_table_index_misses);
    }

    {
        var bounded_cache = Cache.init(alloc, DefaultCacheSizeBytes);
        defer bounded_cache.deinit();

        var backend = try Backend.open(alloc, root_dir, .{
            .storage = host.storage(),
            .flush_threshold = 6,
            .cache = &bounded_cache,
        });
        defer backend.close();

        var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginRead();
        defer txn.abort();
        var cur = try txn.openCursor();
        defer cur.close();
        cur.setUpperBound("doc:003");

        try std.testing.expectEqualStrings("doc:002", (try cur.seekAtOrAfter("doc:002")).?.key);
        try std.testing.expect((try cur.next()) == null);

        const read_stats = backend.snapshotReadStats();
        try std.testing.expectEqual(@as(u64, 1), read_stats.cursor_block_loads);
        try std.testing.expectEqual(@as(u64, 0), read_stats.cursor_block_readaheads);
    }

    {
        var batch_cache = Cache.init(alloc, DefaultCacheSizeBytes);
        defer batch_cache.deinit();

        var backend = try Backend.open(alloc, root_dir, .{
            .storage = host.storage(),
            .flush_threshold = 6,
            .cache = &batch_cache,
        });
        defer backend.close();

        var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginRead();
        defer txn.abort();

        const keys = [_][]const u8{ "doc:002", "doc:003", "doc:004", "doc:005" };
        var values = [_]?[]const u8{ null, null, null, null };
        try txn.getManySorted(&keys, &values);
        for (values) |maybe_value| try std.testing.expectEqualStrings(large_value, maybe_value.?);

        const read_stats = backend.snapshotReadStats();
        try std.testing.expectEqual(@as(u64, 1), read_stats.get_many_sorted_calls);
        try std.testing.expectEqual(@as(u64, keys.len), read_stats.get_many_sorted_keys);
        try std.testing.expectEqual(@as(u64, keys.len), read_stats.get_many_sorted_hits);
        try std.testing.expectEqual(@as(u64, 0), read_stats.get_many_sorted_misses);
        try std.testing.expectEqual(@as(u64, 0), read_stats.get_many_sorted_plan_point);
        try std.testing.expectEqual(@as(u64, 1), read_stats.get_many_sorted_plan_cursor);
        try std.testing.expect(read_stats.table_entry_parses > 0);
        try std.testing.expect(read_stats.table_block_loads > 0);
        try std.testing.expect(read_stats.table_block_bytes > 0);
        try std.testing.expect(read_stats.cursor_block_loads > 0);
        try std.testing.expect(read_stats.cursor_block_reuses > 0);
        try std.testing.expectEqual(@as(u64, keys.len), read_stats.cursor_value_borrows);
        try std.testing.expectEqual(@as(u64, 0), read_stats.cursor_value_copies);
    }

    {
        var point_cache = Cache.init(alloc, DefaultCacheSizeBytes);
        defer point_cache.deinit();

        var backend = try Backend.open(alloc, root_dir, .{
            .storage = host.storage(),
            .flush_threshold = 100,
            .cache = &point_cache,
        });
        defer backend.close();

        var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer runtime.deinit();

        var writer = try runtime.beginWrite();
        try writer.put("doc:mutable", "mutable");
        try writer.commit();

        var probe = try runtime.beginProbe();
        defer probe.abort();
        try std.testing.expectEqualStrings(large_value, try probe.get("doc:005"));

        const read_stats = backend.snapshotReadStats();
        try std.testing.expectEqual(@as(u64, 1), read_stats.point_gets);
        try std.testing.expectEqual(@as(u64, 1), read_stats.point_value_borrows);
        try std.testing.expectEqual(@as(u64, 0), read_stats.point_value_copies);
    }

    try std.testing.expectEqual(@as(usize, 0), ctx.run_file_reads);
    try std.testing.expect(ctx.run_range_reads <= 12);
    try std.testing.expect(ctx.run_trailer_reads <= 6);
    try std.testing.expectEqual(@as(usize, 0), ctx.run_file_size_reads);
}

test "lsm backend prefix bloom skips bounded scan blocks" {
    const alloc = std.testing.allocator;
    var backing = storage_io.MemoryStorage.init(alloc);
    defer backing.deinit();
    var cache = Cache.init(alloc, DefaultCacheSizeBytes);
    defer cache.deinit();

    const root_dir = "/lsm-prefix-bloom-scan-skip";
    {
        var backend = try Backend.open(alloc, root_dir, .{
            .storage = backing.storage(),
            .flush_threshold = 2,
        });
        defer backend.close();

        var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginWrite();
        try txn.put("tenant-a:001", "a");
        try txn.put("tenant-c:001", "c");
        try txn.commit();
    }

    var backend = try Backend.open(alloc, root_dir, .{
        .storage = backing.storage(),
        .flush_threshold = 2,
        .cache = &cache,
    });
    defer backend.close();

    var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime.deinit();

    var txn = try runtime.beginRead();
    defer txn.abort();
    var cur = try txn.openCursor();
    defer cur.close();
    cur.setUpperBound("tenant-b;");

    try std.testing.expect((try cur.seekAtOrAfter("tenant-b:")) == null);

    const read_stats = backend.snapshotReadStats();
    try std.testing.expect(read_stats.bloom_negatives > 0);
    try std.testing.expectEqual(@as(u64, 0), read_stats.cursor_block_loads);
    try std.testing.expectEqual(@as(u64, 0), read_stats.table_block_loads);
}

test "lsm backend block filter avoids candidate block read on run-bloom false positive" {
    const alloc = std.testing.allocator;
    var backing = storage_io.MemoryStorage.init(alloc);
    defer backing.deinit();
    var tracked_run_path: ?[]u8 = null;
    defer if (tracked_run_path) |path| alloc.free(path);

    const root_dir = "/lsm-block-filter-negative";
    const weak_bloom: bloom.Config = .{
        .bits_per_key = 1,
        .min_bits = 8,
        .max_hash_count = 1,
    };
    const large_value = try alloc.alloc(u8, cache_mod.DefaultTableBlockSize / 4);
    defer alloc.free(large_value);
    @memset(large_value, 'v');

    {
        var backend = try Backend.open(alloc, root_dir, .{
            .storage = backing.storage(),
            .flush_threshold = 6,
            .bloom = weak_bloom,
        });
        defer backend.close();

        var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginWrite();
        var key_buf: [32]u8 = undefined;
        for (0..6) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "doc:{d:0>3}", .{i});
            try txn.put(key, large_value);
        }
        try txn.commit();
    }

    const Context = struct {
        backing: *storage_io.MemoryStorage,
        run_path: ?[]const u8 = null,
        run_file_reads: usize = 0,
        run_range_reads: usize = 0,
        run_trailer_reads: usize = 0,
        run_file_size_reads: usize = 0,

        fn createDirPath(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().createDirPath(path);
        }

        fn readFileAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, max_bytes: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.run_path) |run_path| {
                if (std.mem.eql(u8, path, run_path)) self.run_file_reads += 1;
            }
            return self.backing.storage().readFileAlloc(allocator, path, max_bytes);
        }

        fn readFileRangeAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, offset: u64, len: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.run_path) |run_path| {
                if (std.mem.eql(u8, path, run_path)) self.run_range_reads += 1;
            }
            return self.backing.storage().readFileRangeAlloc(allocator, path, offset, len);
        }

        fn fileSize(ptr: *anyopaque, path: []const u8) !u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.run_path) |run_path| {
                if (std.mem.eql(u8, path, run_path)) self.run_file_size_reads += 1;
            }
            return self.backing.storage().fileSize(path);
        }

        fn readFileTrailerAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, len: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.run_path) |run_path| {
                if (std.mem.eql(u8, path, run_path)) self.run_trailer_reads += 1;
            }
            return self.backing.storage().readFileTrailerAlloc(allocator, path, len);
        }

        fn writeFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().writeFileAbsolute(path, contents);
        }

        fn renameAbsolute(ptr: *anyopaque, old_path: []const u8, new_path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().renameAbsolute(old_path, new_path);
        }

        fn deleteFileAbsolute(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().deleteFileAbsolute(path);
        }

        fn deleteTree(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().deleteTree(path);
        }

        fn nowNs(ptr: *anyopaque) u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().nowNs();
        }
    };

    var ctx = Context{ .backing = &backing };
    const host = storage_io.HostStorage.init(&ctx, &.{
        .create_dir_path = Context.createDirPath,
        .read_file_alloc = Context.readFileAlloc,
        .read_file_range_alloc = Context.readFileRangeAlloc,
        .file_size = Context.fileSize,
        .read_file_trailer_alloc = Context.readFileTrailerAlloc,
        .write_file_absolute = Context.writeFileAbsolute,
        .rename_absolute = Context.renameAbsolute,
        .delete_file_absolute = Context.deleteFileAbsolute,
        .delete_tree = Context.deleteTree,
        .now_ns = Context.nowNs,
    });

    var false_positive_buf: [64]u8 = undefined;
    var false_positive_key: ?[]const u8 = null;
    {
        var manifest_backing: ?[]u8 = null;
        defer if (manifest_backing) |raw| alloc.free(raw);
        var next_run_id: u64 = 0;
        var runs = std.ArrayListUnmanaged(repository_mod.Run).empty;
        var obsolete_paths = std.ArrayListUnmanaged(repository_mod.ObsoletePath).empty;
        defer {
            for (runs.items) |*run| run.deinit(alloc);
            runs.deinit(alloc);
            for (obsolete_paths.items) |*obsolete| obsolete.deinit(alloc);
            obsolete_paths.deinit(alloc);
        }

        try std.testing.expect(try repository_mod.loadManifestIfPresentWithStorage(
            host.storage(),
            alloc,
            root_dir,
            &manifest_backing,
            &next_run_id,
            &runs,
            &obsolete_paths,
        ));
        try std.testing.expectEqual(@as(usize, 1), runs.items.len);
        tracked_run_path = try alloc.dupe(u8, runs.items[0].path.?);
        ctx.run_path = tracked_run_path.?;
    }

    {
        var index = try repository_mod.loadRunTableIndexAllocWithStorage(backing.storage(), alloc, tracked_run_path.?);
        defer index.deinit(alloc);

        try std.testing.expect(index.blockCount() > 1);
        const target_block = index.blockCount() - 1;
        try std.testing.expect(index.blocks[target_block].filter != null);

        for (0..10_000) |i| {
            const candidate = try std.fmt.bufPrint(&false_positive_buf, "doc:003-miss-{d}", .{i});
            if (index.findBlockIndex("docs", candidate) != target_block) continue;
            if (!lsm_table_file.maybeContains(index.borrowFilter(), "docs", candidate)) continue;
            if (index.blocks[target_block].maybeContains("docs", candidate)) continue;
            false_positive_key = candidate;
            break;
        }
    }

    const missing_key = false_positive_key orelse return error.TestUnexpectedResult;

    {
        var backend = try Backend.open(alloc, root_dir, .{
            .storage = host.storage(),
            .flush_threshold = 6,
            .bloom = weak_bloom,
        });
        defer backend.close();

        var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginRead();
        defer txn.abort();
        try std.testing.expectError(error.NotFound, txn.get(missing_key));
    }

    try std.testing.expectEqual(@as(usize, 0), ctx.run_file_reads);
    try std.testing.expectEqual(@as(usize, 1), ctx.run_range_reads);
    try std.testing.expectEqual(@as(usize, 1), ctx.run_trailer_reads);
    try std.testing.expectEqual(@as(usize, 0), ctx.run_file_size_reads);
}

test "lsm backend persists next run id across reopen" {
    var path_buf: [256]u8 = undefined;
    const path = repository_mod.tmpPath(&path_buf, "cache-run-id");
    defer repository_mod.cleanupTmp(path);
    const root_dir = std.mem.span(path);

    var next_run_id_after_first_write: u64 = 0;
    {
        var backend = try Backend.open(std.testing.allocator, root_dir, .{ .flush_threshold = 1 });
        defer backend.close();

        var txn = try backend.beginWrite();
        try txn.put(.{ .name = "docs" }, "doc:a", "A");
        try txn.commit();
        try backend.sync(true);
        next_run_id_after_first_write = backend.next_run_id;
        try std.testing.expect(next_run_id_after_first_write > 1);
    }

    {
        var reopened = try Backend.open(std.testing.allocator, root_dir, .{ .flush_threshold = 1 });
        defer reopened.close();

        try std.testing.expectEqual(next_run_id_after_first_write, reopened.next_run_id);

        var txn = try reopened.beginWrite();
        try txn.put(.{ .name = "docs" }, "doc:b", "B");
        try txn.commit();
        try reopened.sync(true);
        try std.testing.expect(reopened.next_run_id > next_run_id_after_first_write);
    }
}

test "lsm backend read stats count point gets and sorted batches" {
    var backend = Backend.init(std.testing.allocator, .{ .flush_threshold = 1 });
    defer backend.close();

    {
        var txn = try backend.beginWrite();
        try txn.put(.{ .name = "docs" }, "doc:a", "A");
        try txn.put(.{ .name = "docs" }, "doc:b", "B");
        try txn.commit();
    }

    var txn = try backend.beginRead();
    defer txn.abort();
    try std.testing.expectEqualStrings("A", try txn.get(.{ .name = "docs" }, "doc:a"));
    const keys = [_][]const u8{ "doc:a", "doc:b", "doc:missing" };
    var values = [_]?[]const u8{ null, null, null };
    try txn.getManySorted(.{ .name = "docs" }, &keys, &values);
    try std.testing.expectEqualStrings("A", values[0].?);
    try std.testing.expectEqualStrings("B", values[1].?);
    try std.testing.expectEqual(@as(?[]const u8, null), values[2]);

    const stats = backend.snapshotReadStats();
    try std.testing.expect(stats.point_gets >= 4);
    try std.testing.expectEqual(@as(u64, 1), stats.get_many_sorted_calls);
    try std.testing.expectEqual(@as(u64, keys.len), stats.get_many_sorted_keys);
    try std.testing.expectEqual(@as(u64, 2), stats.get_many_sorted_hits);
    try std.testing.expectEqual(@as(u64, 1), stats.get_many_sorted_misses);
    try std.testing.expectEqual(@as(u64, 2), stats.get_many_sorted_monotonic_pairs);
    try std.testing.expectEqual(@as(u64, 0), stats.get_many_sorted_duplicate_pairs);
    try std.testing.expectEqual(@as(u64, 0), stats.get_many_sorted_out_of_order_pairs);
    try std.testing.expect(stats.run_probes > 0);
}

test "lsm backend current scan reuses run grouping across cursor movement" {
    var backend = Backend.init(std.testing.allocator, .{ .flush_threshold = 1 });
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    var key_buf: [32]u8 = undefined;
    var value_buf: [32]u8 = undefined;
    for (0..16) |i| {
        var write = try runtime.beginWrite();
        const key = try std.fmt.bufPrint(&key_buf, "doc:{d:0>3}", .{i});
        const value = try std.fmt.bufPrint(&value_buf, "value-{d}", .{i});
        try write.put(key, value);
        try write.commit();
    }

    const before_scan = backend.snapshotReadStats();
    var scan = try runtime.beginCurrentScan();
    defer scan.abort();
    const after_open = backend.snapshotReadStats();
    try std.testing.expectEqual(@as(u64, 1), after_open.run_group_builds - before_scan.run_group_builds);

    var cur = try scan.openCursor();
    defer cur.close();
    var maybe_entry = try cur.seekAtOrAfter("doc:000");
    var count: usize = 0;
    while (maybe_entry) |entry| {
        if (!std.mem.startsWith(u8, entry.key, "doc:")) break;
        count += 1;
        maybe_entry = try cur.next();
    }
    try std.testing.expectEqual(@as(usize, 16), count);

    const after_scan = backend.snapshotReadStats();
    try std.testing.expectEqual(after_open.run_group_builds, after_scan.run_group_builds);
    try std.testing.expect(after_open.run_group_total_runs > before_scan.run_group_total_runs);
    try std.testing.expectEqual(after_open.run_group_total_runs, after_scan.run_group_total_runs);
    try std.testing.expectEqual(after_open.run_group_l0_runs, after_scan.run_group_l0_runs);
}

test "lsm backend current probe getManySorted reuses source layout across chunks" {
    var backend = Backend.init(std.testing.allocator, .{
        .flush_threshold = 1,
        .compact_threshold_runs = 1024,
        .l0_overlap_compact_threshold_runs = 1024,
        .wal_enabled = false,
    });
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    var key_buf: [32]u8 = undefined;
    var value_buf: [32]u8 = undefined;
    for (0..160) |i| {
        var write = try runtime.beginWrite();
        const key = try std.fmt.bufPrint(&key_buf, "doc:{d:0>3}", .{i});
        const value = try std.fmt.bufPrint(&value_buf, "value-{d}", .{i});
        try write.put(key, value);
        try write.commit();
    }

    try backend.mutable.upsert(std.testing.allocator, .{ .name = "docs" }, "doc:live", "value-live", false);

    var key_storage: [160][16]u8 = undefined;
    var keys: [160][]const u8 = undefined;
    var values: [160]?[]const u8 = undefined;
    for (&keys, 0..) |*key, i| {
        key.* = try std.fmt.bufPrint(&key_storage[i], "doc:{d:0>3}", .{i});
    }
    @memset(&values, null);

    const before_read = backend.snapshotReadStats();
    var probe = try runtime.beginProbe();
    defer probe.abort();
    try probe.getManySorted(&keys, &values);
    const after_read = backend.snapshotReadStats();

    try std.testing.expectEqual(@as(u64, 1), after_read.run_group_builds - before_read.run_group_builds);
    try std.testing.expectEqualStrings("value-0", values[0].?);
    try std.testing.expectEqualStrings("value-159", values[159].?);
}

test "lsm backend current scan does not rotate or clone mutable writer generation" {
    var backend = Backend.init(std.testing.allocator, .{ .flush_threshold = 1024 });
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    {
        var write = try runtime.beginWrite();
        try write.put("doc:a", "A");
        try write.commit();
    }

    const before_maintenance = backend.snapshotMaintenanceStats();
    const before_writes = backend.snapshotWriteStats();
    try std.testing.expectEqual(@as(usize, 0), backend.activeImmutableMemtableCount());

    var scan = try runtime.beginCurrentScan();
    defer scan.abort();

    const after_open = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(before_writes.immutable_rotations, backend.snapshotWriteStats().immutable_rotations);
    try std.testing.expectEqual(@as(usize, 0), backend.activeImmutableMemtableCount());
    try std.testing.expectEqual(before_maintenance.mutable_snapshot_clone_calls, after_open.mutable_snapshot_clone_calls);

    var cursor = try scan.openCursor();
    defer cursor.close();

    var maybe_entry = try cursor.seekAtOrAfter("doc:");
    var saw_a = false;
    while (maybe_entry) |entry| {
        if (!std.mem.startsWith(u8, entry.key, "doc:")) break;
        if (std.mem.eql(u8, entry.key, "doc:a")) saw_a = true;
        maybe_entry = try cursor.next();
    }

    try std.testing.expect(saw_a);
}

test "lsm backend current scan helpers do not clone mutable writer generation" {
    var backend = Backend.init(std.testing.allocator, .{ .flush_threshold = 1024 });
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    {
        var write = try runtime.beginWrite();
        try write.put("doc:a", "A");
        try write.put("doc:b", "B");
        try write.commit();
    }

    const before = backend.snapshotMaintenanceStats();

    const ScanState = struct {
        count: usize = 0,

        threadlocal var active: ?*@This() = null;

        fn cb(key: []const u8, value: []const u8) anyerror!backend_scan.ScanAction {
            const self = active.?;
            if (std.mem.startsWith(u8, key, "doc:")) {
                try std.testing.expect(value.len > 0);
                self.count += 1;
            }
            return .@"continue";
        }
    };

    var state = ScanState{};
    ScanState.active = &state;
    defer ScanState.active = null;

    try backend_scan.scanCurrent(&runtime, "doc:", "doc;", .{}, &ScanState.cb);
    try std.testing.expectEqual(@as(usize, 2), state.count);

    const prefix = try backend_scan.scanPrefixCurrent(std.testing.allocator, &runtime, "doc:");
    defer backend_scan.freeResults(std.testing.allocator, prefix);
    try std.testing.expectEqual(@as(usize, 2), prefix.len);

    const range = try backend_scan.scanRangeCurrent(std.testing.allocator, &runtime, "doc:", "doc;");
    defer backend_scan.freeResults(std.testing.allocator, range);
    try std.testing.expectEqual(@as(usize, 2), range.len);

    const after = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(before.mutable_snapshot_clone_calls, after.mutable_snapshot_clone_calls);
}

test "lsm backend current scan survives mutable rotation after positioning active source" {
    var backend = Backend.init(std.testing.allocator, .{ .flush_threshold = 1024 });
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    {
        var write = try runtime.beginWrite();
        try write.put("doc:a", "A");
        try write.put("doc:c", "C");
        try write.commit();
    }

    var scan = try runtime.beginCurrentScan();
    defer scan.abort();

    var cursor = try scan.openCursor();
    defer cursor.close();

    const first = try cursor.seekAtOrAfter("doc:a") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("doc:a", first.key);

    try backend.rotateMutableToImmutable();

    _ = try cursor.next();
}

test "lsm backend read txn getManySorted uses sorted-by-run path for leaf-sized sparse batches" {
    var backend = Backend.init(std.testing.allocator, .{ .flush_threshold = 1 });
    defer backend.close();

    const count = 512;
    {
        var txn = try backend.beginWrite();
        var key_buf: [64]u8 = undefined;
        var value_buf: [32]u8 = undefined;
        for (0..count) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "artifact:{d:0>8}:dense", .{i * 10});
            const value = try std.fmt.bufPrint(&value_buf, "value-{d}", .{i});
            try txn.put(.{ .name = "docs" }, key, value);
        }
        try txn.commit();
    }

    const keys = try std.testing.allocator.alloc([]const u8, count);
    defer {
        for (keys) |key| std.testing.allocator.free(key);
        std.testing.allocator.free(keys);
    }
    const values = try std.testing.allocator.alloc(?[]const u8, count);
    defer std.testing.allocator.free(values);
    for (keys, 0..) |*key, i| {
        key.* = try std.fmt.allocPrint(std.testing.allocator, "artifact:{d:0>8}:dense", .{i * 10});
    }

    var read = try backend.beginRead();
    defer read.abort();
    try read.getManySorted(.{ .name = "docs" }, keys, values);
    try std.testing.expectEqualStrings("value-0", values[0].?);
    try std.testing.expectEqualStrings("value-511", values[count - 1].?);

    const stats = backend.snapshotReadStats();
    try std.testing.expectEqual(@as(u64, 1), stats.get_many_sorted_calls);
    try std.testing.expectEqual(@as(u64, count), stats.get_many_sorted_keys);
    try std.testing.expectEqual(@as(u64, count), stats.get_many_sorted_hits);
    try std.testing.expectEqual(@as(u64, 0), stats.get_many_sorted_plan_point);
    try std.testing.expectEqual(@as(u64, 1), stats.get_many_sorted_plan_sorted_by_run);
    try std.testing.expectEqual(@as(u64, 0), stats.get_many_sorted_plan_cursor);
    try std.testing.expectEqual(@as(u64, 0), stats.cursor_block_loads);
}

test "lsm backend write batch getManySorted merges overlay and committed cursor reads" {
    var backend = Backend.init(std.testing.allocator, .{ .flush_threshold = 1 });
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    {
        var txn = try runtime.beginWrite();
        try txn.put("doc:a", "A");
        try txn.put("doc:c", "C");
        try txn.commit();
    }

    var batch = try runtime.beginBatch();
    defer batch.abort();
    try batch.put("doc:b", "B-overlay");
    try batch.delete("doc:c");

    const keys = [_][]const u8{ "doc:a", "doc:b", "doc:c", "doc:d" };
    var values = [_]?[]const u8{ null, null, null, null };
    try batch.getManySorted(&keys, &values);
    try std.testing.expectEqualStrings("A", values[0].?);
    try std.testing.expectEqualStrings("B-overlay", values[1].?);
    try std.testing.expectEqual(@as(?[]const u8, null), values[2]);
    try std.testing.expectEqual(@as(?[]const u8, null), values[3]);

    const stats = backend.snapshotReadStats();
    try std.testing.expectEqual(@as(u64, 1), stats.get_many_sorted_calls);
    try std.testing.expectEqual(@as(u64, keys.len), stats.get_many_sorted_keys);
    try std.testing.expectEqual(@as(u64, 2), stats.get_many_sorted_hits);
    try std.testing.expectEqual(@as(u64, 2), stats.get_many_sorted_misses);
}

test "lsm backend bound read txn getManySorted uses sorted-by-run path for leaf-sized sparse batches" {
    var backend = Backend.init(std.testing.allocator, .{ .flush_threshold = 1 });
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    const count = 512;
    {
        var txn = try runtime.beginWrite();
        var key_buf: [64]u8 = undefined;
        var value_buf: [32]u8 = undefined;
        for (0..count) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "artifact:{d:0>8}:dense", .{i * 10});
            const value = try std.fmt.bufPrint(&value_buf, "value-{d}", .{i});
            try txn.put(key, value);
        }
        try txn.commit();
    }

    const keys = try std.testing.allocator.alloc([]const u8, count);
    defer {
        for (keys) |key| std.testing.allocator.free(key);
        std.testing.allocator.free(keys);
    }
    const values = try std.testing.allocator.alloc(?[]const u8, count);
    defer std.testing.allocator.free(values);
    for (keys, 0..) |*key, i| {
        key.* = try std.fmt.allocPrint(std.testing.allocator, "artifact:{d:0>8}:dense", .{i * 10});
    }

    var read = try runtime.beginRead();
    defer read.abort();
    try read.getManySorted(keys, values);
    try std.testing.expectEqualStrings("value-0", values[0].?);
    try std.testing.expectEqualStrings("value-511", values[count - 1].?);

    const stats = backend.snapshotReadStats();
    try std.testing.expectEqual(@as(u64, 1), stats.get_many_sorted_calls);
    try std.testing.expectEqual(@as(u64, count), stats.get_many_sorted_keys);
    try std.testing.expectEqual(@as(u64, count), stats.get_many_sorted_hits);
    try std.testing.expectEqual(@as(u64, 0), stats.get_many_sorted_plan_point);
    try std.testing.expectEqual(@as(u64, 1), stats.get_many_sorted_plan_sorted_by_run);
    try std.testing.expectEqual(@as(u64, 0), stats.get_many_sorted_plan_cursor);
    try std.testing.expectEqual(@as(u64, 0), stats.cursor_block_loads);
}

test "lsm backend sorted-by-run getManySorted advances within cached run blocks" {
    const alloc = std.testing.allocator;
    var storage = storage_io.MemoryStorage.init(alloc);
    defer storage.deinit();
    var cache = Cache.init(alloc, DefaultCacheSizeBytes);
    defer cache.deinit();

    const root_dir = "/lsm-sorted-by-run-forward-cache";
    const count = 128;
    {
        var backend = try Backend.open(alloc, root_dir, .{
            .storage = storage.storage(),
            .flush_threshold = 1,
            .cache = &cache,
        });
        defer backend.close();

        var txn = try backend.beginWrite();
        var key_buf: [64]u8 = undefined;
        var value_buf: [32]u8 = undefined;
        for (0..count) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "artifact:{d:0>8}:dense", .{i * 10});
            const value = try std.fmt.bufPrint(&value_buf, "value-{d}", .{i});
            try txn.put(.{ .name = "docs" }, key, value);
        }
        try txn.commit();
    }

    var backend = try Backend.open(alloc, root_dir, .{
        .storage = storage.storage(),
        .flush_threshold = 1,
        .cache = &cache,
    });
    defer backend.close();

    const keys = try alloc.alloc([]const u8, count);
    defer {
        for (keys) |key| alloc.free(key);
        alloc.free(keys);
    }
    const values = try alloc.alloc(?[]const u8, count);
    defer alloc.free(values);
    for (keys, 0..) |*key, i| {
        key.* = try std.fmt.allocPrint(alloc, "artifact:{d:0>8}:dense", .{i * 10});
    }

    var read = try backend.beginRead();
    defer read.abort();
    try read.getManySorted(.{ .name = "docs" }, keys, values);
    try std.testing.expectEqualStrings("value-0", values[0].?);
    try std.testing.expectEqualStrings("value-127", values[count - 1].?);

    const stats = backend.snapshotReadStats();
    try std.testing.expectEqual(@as(u64, 1), stats.get_many_sorted_plan_sorted_by_run);
    try std.testing.expect(stats.read_hint_attempts > 0);
    try std.testing.expect(stats.read_hint_hits > 0);
    try std.testing.expect(stats.read_hint_hits > stats.read_hint_misses);
    try std.testing.expectEqual(@as(u64, 0), stats.cursor_block_loads);
}

test "lsm backend probe getManySorted uses point path for large sparse batches" {
    var backend = Backend.init(std.testing.allocator, .{ .flush_threshold = 1 });
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    const count = 1500;
    {
        var txn = try runtime.beginWrite();
        var key_buf: [64]u8 = undefined;
        var value_buf: [32]u8 = undefined;
        for (0..count) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "artifact:{d:0>8}:dense", .{i * 10});
            const value = try std.fmt.bufPrint(&value_buf, "value-{d}", .{i});
            try txn.put(key, value);
        }
        try txn.commit();
    }

    const keys = try std.testing.allocator.alloc([]const u8, count);
    defer {
        for (keys) |key| std.testing.allocator.free(key);
        std.testing.allocator.free(keys);
    }
    const values = try std.testing.allocator.alloc(?[]const u8, count);
    defer std.testing.allocator.free(values);
    for (keys, 0..) |*key, i| {
        key.* = try std.fmt.allocPrint(std.testing.allocator, "artifact:{d:0>8}:dense", .{i * 10});
    }

    var probe = try runtime.beginProbe();
    defer probe.abort();
    try probe.getManySorted(keys, values);
    try std.testing.expectEqualStrings("value-0", values[0].?);
    try std.testing.expectEqualStrings("value-1499", values[count - 1].?);

    const stats = backend.snapshotReadStats();
    try std.testing.expectEqual(@as(u64, 1), stats.get_many_sorted_calls);
    try std.testing.expectEqual(@as(u64, count), stats.get_many_sorted_keys);
    try std.testing.expectEqual(@as(u64, count), stats.get_many_sorted_hits);
    try std.testing.expectEqual(@as(u64, 0), stats.get_many_sorted_misses);
    try std.testing.expect(stats.get_many_sorted_plan_point > 0);
    try std.testing.expectEqual(@as(u64, 0), stats.get_many_sorted_plan_cursor);
    try std.testing.expectEqual(@as(u64, 0), stats.cursor_block_loads);
}

test "lsm backend probe getManySorted uses point path for leaf-sized sparse batches" {
    var backend = Backend.init(std.testing.allocator, .{ .flush_threshold = 1 });
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    const count = 512;
    {
        var txn = try runtime.beginWrite();
        var key_buf: [64]u8 = undefined;
        var value_buf: [32]u8 = undefined;
        for (0..count) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "artifact:{d:0>8}:dense", .{i * 10});
            const value = try std.fmt.bufPrint(&value_buf, "value-{d}", .{i});
            try txn.put(key, value);
        }
        try txn.commit();
    }

    const keys = try std.testing.allocator.alloc([]const u8, count);
    defer {
        for (keys) |key| std.testing.allocator.free(key);
        std.testing.allocator.free(keys);
    }
    const values = try std.testing.allocator.alloc(?[]const u8, count);
    defer std.testing.allocator.free(values);
    for (keys, 0..) |*key, i| {
        key.* = try std.fmt.allocPrint(std.testing.allocator, "artifact:{d:0>8}:dense", .{i * 10});
    }

    var probe = try runtime.beginProbe();
    defer probe.abort();
    try probe.getManySorted(keys, values);
    try std.testing.expectEqualStrings("value-0", values[0].?);
    try std.testing.expectEqualStrings("value-511", values[count - 1].?);

    const stats = backend.snapshotReadStats();
    try std.testing.expectEqual(@as(u64, 1), stats.get_many_sorted_calls);
    try std.testing.expectEqual(@as(u64, count), stats.get_many_sorted_keys);
    try std.testing.expectEqual(@as(u64, count), stats.get_many_sorted_hits);
    try std.testing.expect(stats.get_many_sorted_plan_point > 0);
    try std.testing.expectEqual(@as(u64, 0), stats.get_many_sorted_plan_cursor);
    try std.testing.expectEqual(@as(u64, 0), stats.cursor_block_loads);
}

test "lsm backend probe getManySorted keeps artifact-style exact batches on point path" {
    var backend = Backend.init(std.testing.allocator, .{ .flush_threshold = 1 });
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    const count = 64;
    {
        var txn = try runtime.beginWrite();
        var key_buf: [64]u8 = undefined;
        var value_buf: [32]u8 = undefined;
        for (0..count) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "artifact:{d:0>8}:dense", .{i});
            const value = try std.fmt.bufPrint(&value_buf, "value-{d}", .{i});
            try txn.put(key, value);
        }
        try txn.commit();
    }

    const keys = try std.testing.allocator.alloc([]const u8, count);
    defer {
        for (keys) |key| std.testing.allocator.free(key);
        std.testing.allocator.free(keys);
    }
    const values = try std.testing.allocator.alloc(?[]const u8, count);
    defer std.testing.allocator.free(values);
    for (keys, 0..) |*key, i| {
        key.* = try std.fmt.allocPrint(std.testing.allocator, "artifact:{d:0>8}:dense", .{i});
    }

    var probe = try runtime.beginProbe();
    defer probe.abort();
    try probe.getManySorted(keys, values);
    try std.testing.expectEqualStrings("value-0", values[0].?);
    try std.testing.expectEqualStrings("value-63", values[count - 1].?);

    const stats = backend.snapshotReadStats();
    try std.testing.expectEqual(@as(u64, 1), stats.get_many_sorted_calls);
    try std.testing.expectEqual(@as(u64, count), stats.get_many_sorted_keys);
    try std.testing.expectEqual(@as(u64, count), stats.get_many_sorted_hits);
    try std.testing.expect(stats.get_many_sorted_plan_point > 0);
    try std.testing.expectEqual(@as(u64, 0), stats.get_many_sorted_plan_cursor);
    try std.testing.expectEqual(@as(u64, 0), stats.cursor_block_loads);
}

test "lsm backend reuses mutable read snapshot until writes invalidate it" {
    var backend = Backend.init(std.testing.allocator, .{});
    defer backend.close();

    {
        var txn = try backend.beginWrite();
        try txn.put(.{ .name = "docs" }, "doc:a", "A");
        try txn.commit();
    }

    var read_a = try backend.beginRead();
    defer read_a.abort();
    try std.testing.expectEqualStrings("A", try read_a.get(.{ .name = "docs" }, "doc:a"));
    const first_snapshot = backend.mutable_read_snapshot orelse return error.TestUnexpectedResult;

    var read_b = try backend.beginRead();
    defer read_b.abort();
    try std.testing.expectEqualStrings("A", try read_b.get(.{ .name = "docs" }, "doc:a"));
    try std.testing.expect(first_snapshot == backend.mutable_read_snapshot.?);

    {
        var txn = try backend.beginWrite();
        try txn.put(.{ .name = "docs" }, "doc:b", "B");
        try txn.commit();
    }

    try std.testing.expectEqual(@as(?*State, null), backend.mutable_read_snapshot);
    try std.testing.expectEqual(@as(usize, 1), backend.retired_mutable_snapshots.items.len);

    var read_c = try backend.beginRead();
    defer read_c.abort();
    try std.testing.expectEqualStrings("B", try read_c.get(.{ .name = "docs" }, "doc:b"));
    try std.testing.expect(backend.mutable_read_snapshot != null);
    try std.testing.expect(first_snapshot != backend.mutable_read_snapshot.?);
}

test "lsm backend rotates large mutable state for read snapshots instead of cloning" {
    var backend = Backend.init(std.testing.allocator, .{
        .read_snapshot_rotate_mutable_bytes = 1,
    });
    defer backend.close();

    {
        var txn = try backend.beginWrite();
        try txn.put(.{ .name = "docs" }, "doc:a", "A");
        try txn.commit();
    }

    try std.testing.expect(backend.mutable.entries.items.len > 0);
    try std.testing.expectEqual(@as(u64, 0), backend.mutable_snapshot_clone_calls);

    var read = try backend.beginRead();
    defer read.abort();
    try std.testing.expectEqualStrings("A", try read.get(.{ .name = "docs" }, "doc:a"));
    try std.testing.expectEqual(@as(usize, 0), backend.mutable.entries.items.len);
    try std.testing.expectEqual(@as(?*State, null), backend.mutable_read_snapshot);
    try std.testing.expectEqual(@as(usize, 1), backend.activeImmutableMemtableCount());
    try std.testing.expectEqual(@as(u64, 0), backend.mutable_snapshot_clone_calls);
    const maintenance = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(@as(u64, 1), maintenance.read_snapshot_mutable_rotations);
    try std.testing.expect(maintenance.read_snapshot_mutable_rotation_bytes_total > 0);
    try std.testing.expectEqual(maintenance.read_snapshot_mutable_rotation_bytes_total, maintenance.read_snapshot_mutable_rotation_peak_bytes);
}

test "lsm backend attributes mutable snapshot clones by reader class" {
    var backend = Backend.init(std.testing.allocator, .{});
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    {
        var txn = try runtime.beginWrite();
        try txn.put("doc:a", "A");
        try txn.commit();
    }

    {
        var read = try runtime.beginRead();
        defer read.abort();
        try std.testing.expectEqualStrings("A", try read.get("doc:a"));
    }

    var maintenance = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(@as(u64, 1), maintenance.mutable_snapshot_clone_calls);
    try std.testing.expectEqual(@as(u64, 1), maintenance.mutable_snapshot_clone_by_reason[mutableSnapshotReasonIndex(.bound_read_txn)].calls);
    try std.testing.expectEqual(@as(u64, 0), maintenance.mutable_snapshot_clone_by_reason[mutableSnapshotReasonIndex(.namespace_read_txn)].calls);
    try std.testing.expect(maintenance.mutable_snapshot_clone_by_reason[mutableSnapshotReasonIndex(.bound_read_txn)].bytes_total > 0);

    {
        var txn = try backend.beginWrite();
        try txn.put(.{ .name = "docs" }, "doc:b", "B");
        try txn.commit();
    }

    {
        var read = try backend.beginRead();
        defer read.abort();
        try std.testing.expectEqualStrings("B", try read.get(.{ .name = "docs" }, "doc:b"));
    }

    maintenance = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(@as(u64, 2), maintenance.mutable_snapshot_clone_calls);
    try std.testing.expectEqual(@as(u64, 1), maintenance.mutable_snapshot_clone_by_reason[mutableSnapshotReasonIndex(.bound_read_txn)].calls);
    try std.testing.expectEqual(@as(u64, 1), maintenance.mutable_snapshot_clone_by_reason[mutableSnapshotReasonIndex(.namespace_read_txn)].calls);
    try std.testing.expectEqual(@as(u64, 0), maintenance.mutable_snapshot_clone_by_reason[mutableSnapshotReasonIndex(.other)].calls);
}

test "lsm backend write txns retain reader guards until completion" {
    var backend = Backend.init(std.testing.allocator, .{ .flush_threshold = 1 });
    defer backend.close();

    {
        var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginWrite();
        try std.testing.expectEqual(@as(usize, 1), backend.active_readers);
        try txn.put("doc:a", "A");
        try std.testing.expectEqualStrings("A", try txn.get("doc:a"));
        try txn.commit();
        try std.testing.expectEqual(@as(usize, 0), backend.active_readers);
    }

    {
        var txn = try backend.beginWrite();
        try std.testing.expectEqual(@as(usize, 1), backend.active_readers);
        try txn.put(.{ .name = "docs" }, "doc:b", "B");
        var cur = try txn.openCursor(.{ .name = "docs" });
        defer cur.close();
        try std.testing.expectEqualStrings("doc:a", (try cur.first()).?.key);
        txn.abort();
        try std.testing.expectEqual(@as(usize, 0), backend.active_readers);
    }

    {
        var txn = try backend.beginWrite();
        try std.testing.expectEqual(@as(usize, 1), backend.active_readers);
        try txn.put(.{ .name = "docs" }, "doc:c", "C");
        try txn.commit();
        try std.testing.expectEqual(@as(usize, 0), backend.active_readers);
        txn.abort();
        try std.testing.expectEqual(@as(usize, 0), backend.active_readers);
    }
}

test "lsm backend close leaves queued obsolete files on disk" {
    var path_buf: [256]u8 = undefined;
    const path = repository_mod.tmpPath(&path_buf, "obsolete-retain");
    defer repository_mod.cleanupTmp(path);

    var backend = try Backend.open(std.testing.allocator, std.mem.span(path), .{});
    const obsolete_path = try repository_mod.runPath(std.testing.allocator, std.mem.span(path), 9999);
    defer std.testing.allocator.free(obsolete_path);
    try repository_mod.writeFileAbsoluteWithStorage(backend.storage.?, obsolete_path, "obsolete");
    try backend.queueObsoleteFilePath(try std.testing.allocator.dupe(u8, obsolete_path));
    backend.close();

    var native = try storage_io.NativeStorage.init(std.heap.page_allocator, .threaded);
    defer native.deinit();
    const bytes = try native.storage().readFileAlloc(std.testing.allocator, obsolete_path, 1024);
    defer std.testing.allocator.free(bytes);
    try std.testing.expectEqualStrings("obsolete", bytes);
}

test "lsm repository run readers request cap above 64 MiB" {
    const alloc = std.testing.allocator;
    var backing = storage_io.MemoryStorage.init(alloc);
    defer backing.deinit();

    const root_dir = "/host/lsm-cap";
    const run_path = try repository_mod.runPath(alloc, root_dir, 1);
    defer alloc.free(run_path);
    const manifest_path = try repository_mod.manifestPath(alloc, root_dir);
    defer alloc.free(manifest_path);

    const entries = [_]lsm_table_file.Entry{
        .{ .namespace_name = "docs", .key = "doc:a", .value = "A", .tombstone = false },
    };
    var filter = try lsm_table_file.buildFilterAlloc(alloc, &entries, .{});
    defer filter.deinit(alloc);
    const run_bytes = try lsm_table_file.encodeWithFilterAlloc(alloc, &entries, filter);
    defer alloc.free(run_bytes);
    const encoded_filter = try filter.encodeAlloc(alloc);
    defer alloc.free(encoded_filter);

    try backing.storage().writeFileAbsolute(run_path, run_bytes);

    const manifest_bytes = try lsm_manifest.encodeAlloc(alloc, .{
        .next_run_id = 2,
        .runs = &[_]lsm_manifest.RunMeta{
            .{
                .id = 1,
                .level = 0,
                .size_bytes = run_bytes.len,
                .path = run_path,
                .smallest_namespace_name = "docs",
                .smallest_key = "doc:a",
                .largest_namespace_name = "docs",
                .largest_key = "doc:a",
                .entry_count = 1,
                .bloom_filter = encoded_filter,
            },
        },
    });
    defer alloc.free(manifest_bytes);
    try backing.storage().writeFileAbsolute(manifest_path, manifest_bytes);

    const Context = struct {
        backing: *storage_io.MemoryStorage,
        run_path: []const u8,
        run_reads: usize = 0,
        min_required: usize = 70 * 1024 * 1024,

        fn createDirPath(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().createDirPath(path);
        }

        fn readFileAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, max_bytes: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (std.mem.eql(u8, path, self.run_path)) {
                self.run_reads += 1;
                if (max_bytes < self.min_required) return error.StreamTooLong;
            }
            return self.backing.storage().readFileAlloc(allocator, path, max_bytes);
        }

        fn readFileRangeAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, offset: u64, len: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (std.mem.eql(u8, path, self.run_path)) self.run_reads += 1;
            return self.backing.storage().readFileRangeAlloc(allocator, path, offset, len);
        }

        fn fileSize(ptr: *anyopaque, path: []const u8) !u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().fileSize(path);
        }

        fn writeFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().writeFileAbsolute(path, contents);
        }

        fn renameAbsolute(ptr: *anyopaque, old_path: []const u8, new_path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().renameAbsolute(old_path, new_path);
        }

        fn deleteFileAbsolute(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().deleteFileAbsolute(path);
        }

        fn deleteTree(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().deleteTree(path);
        }

        fn nowNs(ptr: *anyopaque) u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().nowNs();
        }
    };

    var ctx = Context{
        .backing = &backing,
        .run_path = run_path,
    };
    const host = storage_io.HostStorage.init(&ctx, &.{
        .create_dir_path = Context.createDirPath,
        .read_file_alloc = Context.readFileAlloc,
        .read_file_range_alloc = Context.readFileRangeAlloc,
        .file_size = Context.fileSize,
        .write_file_absolute = Context.writeFileAbsolute,
        .rename_absolute = Context.renameAbsolute,
        .delete_file_absolute = Context.deleteFileAbsolute,
        .delete_tree = Context.deleteTree,
        .now_ns = Context.nowNs,
    });

    {
        var state = try repository_mod.loadRunStateAllocWithStorage(host.storage(), alloc, run_path);
        defer state.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 1), state.entries.items.len);
        try std.testing.expectEqualStrings("A", state.entries.items[0].value);
    }

    {
        var table = try repository_mod.loadRunTableBorrowedAllocWithStorage(host.storage(), alloc, run_path);
        defer table.deinit(alloc);
        const entry = try table.entryAt(0);
        try std.testing.expectEqualStrings("A", entry.value);
    }

    {
        var manifest_backing: ?[]u8 = null;
        defer if (manifest_backing) |raw| alloc.free(raw);
        var next_run_id: u64 = 0;
        var runs = std.ArrayListUnmanaged(repository_mod.Run).empty;
        var obsolete_paths = std.ArrayListUnmanaged(repository_mod.ObsoletePath).empty;
        defer {
            for (runs.items) |*run| run.deinit(alloc);
            runs.deinit(alloc);
            for (obsolete_paths.items) |*obsolete| obsolete.deinit(alloc);
            obsolete_paths.deinit(alloc);
        }

        try std.testing.expect(try repository_mod.loadManifestIfPresentWithStorage(
            host.storage(),
            alloc,
            root_dir,
            &manifest_backing,
            &next_run_id,
            &runs,
            &obsolete_paths,
        ));
        try std.testing.expectEqual(@as(u64, 2), next_run_id);
        try std.testing.expectEqual(@as(usize, 1), runs.items.len);
        try std.testing.expectEqual(@as(usize, 0), obsolete_paths.items.len);
    }

    try std.testing.expectEqual(@as(usize, 4), ctx.run_reads);
}

test "lsm repository rejects manifests without run bloom filters" {
    const alloc = std.testing.allocator;
    var backing = storage_io.MemoryStorage.init(alloc);
    defer backing.deinit();

    const root_dir = "/host/lsm-missing-bloom";
    const run_path = try repository_mod.runPath(alloc, root_dir, 1);
    defer alloc.free(run_path);
    const manifest_path = try repository_mod.manifestPath(alloc, root_dir);
    defer alloc.free(manifest_path);

    const manifest_bytes = try lsm_manifest.encodeAlloc(alloc, .{
        .next_run_id = 2,
        .runs = &[_]lsm_manifest.RunMeta{
            .{
                .id = 1,
                .level = 0,
                .size_bytes = 128,
                .path = run_path,
                .smallest_namespace_name = "docs",
                .smallest_key = "doc:a",
                .largest_namespace_name = "docs",
                .largest_key = "doc:a",
                .entry_count = 1,
                .bloom_filter = "",
            },
        },
    });
    defer alloc.free(manifest_bytes);
    try backing.storage().writeFileAbsolute(manifest_path, manifest_bytes);

    var manifest_backing: ?[]u8 = null;
    defer if (manifest_backing) |raw| alloc.free(raw);
    var next_run_id: u64 = 0;
    var runs = std.ArrayListUnmanaged(repository_mod.Run).empty;
    var obsolete_paths = std.ArrayListUnmanaged(repository_mod.ObsoletePath).empty;
    defer {
        for (runs.items) |*run| run.deinit(alloc);
        runs.deinit(alloc);
        for (obsolete_paths.items) |*obsolete| obsolete.deinit(alloc);
        obsolete_paths.deinit(alloc);
    }

    try std.testing.expectError(error.MissingRunBloomFilter, repository_mod.loadManifestIfPresentWithStorage(
        backing.storage(),
        alloc,
        root_dir,
        &manifest_backing,
        &next_run_id,
        &runs,
        &obsolete_paths,
    ));
}

test "lsm repository loads v4 table index from trailer plus metadata read" {
    const alloc = std.testing.allocator;
    var backing = storage_io.MemoryStorage.init(alloc);
    defer backing.deinit();

    const root_dir = "/host/lsm-index-reads";
    const run_path = try repository_mod.runPath(alloc, root_dir, 1);
    defer alloc.free(run_path);

    const entries = [_]lsm_table_file.Entry{
        .{ .namespace_name = "docs", .key = "doc:a", .value = "A", .tombstone = false },
        .{ .namespace_name = "docs", .key = "doc:b", .value = "B", .tombstone = false },
    };
    var filter = try lsm_table_file.buildFilterAlloc(alloc, &entries, .{});
    defer filter.deinit(alloc);
    const run_bytes = try lsm_table_file.encodeWithFilterAlloc(alloc, &entries, filter);
    defer alloc.free(run_bytes);
    try backing.storage().writeFileAbsolute(run_path, run_bytes);

    const Context = struct {
        backing: *storage_io.MemoryStorage,
        run_path: []const u8,
        run_range_reads: usize = 0,
        run_trailer_reads: usize = 0,
        run_file_size_reads: usize = 0,
        run_file_reads: usize = 0,

        fn createDirPath(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().createDirPath(path);
        }

        fn readFileAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, max_bytes: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (std.mem.eql(u8, path, self.run_path)) self.run_file_reads += 1;
            return self.backing.storage().readFileAlloc(allocator, path, max_bytes);
        }

        fn readFileRangeAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, offset: u64, len: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (std.mem.eql(u8, path, self.run_path)) self.run_range_reads += 1;
            return self.backing.storage().readFileRangeAlloc(allocator, path, offset, len);
        }

        fn fileSize(ptr: *anyopaque, path: []const u8) !u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (std.mem.eql(u8, path, self.run_path)) self.run_file_size_reads += 1;
            return self.backing.storage().fileSize(path);
        }

        fn readFileTrailerAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, len: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (std.mem.eql(u8, path, self.run_path)) self.run_trailer_reads += 1;
            return self.backing.storage().readFileTrailerAlloc(allocator, path, len);
        }

        fn writeFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().writeFileAbsolute(path, contents);
        }

        fn renameAbsolute(ptr: *anyopaque, old_path: []const u8, new_path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().renameAbsolute(old_path, new_path);
        }

        fn deleteFileAbsolute(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().deleteFileAbsolute(path);
        }

        fn deleteTree(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().deleteTree(path);
        }

        fn nowNs(ptr: *anyopaque) u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().nowNs();
        }
    };

    var ctx = Context{
        .backing = &backing,
        .run_path = run_path,
    };
    const host = storage_io.HostStorage.init(&ctx, &.{
        .create_dir_path = Context.createDirPath,
        .read_file_alloc = Context.readFileAlloc,
        .read_file_range_alloc = Context.readFileRangeAlloc,
        .file_size = Context.fileSize,
        .read_file_trailer_alloc = Context.readFileTrailerAlloc,
        .write_file_absolute = Context.writeFileAbsolute,
        .rename_absolute = Context.renameAbsolute,
        .delete_file_absolute = Context.deleteFileAbsolute,
        .delete_tree = Context.deleteTree,
        .now_ns = Context.nowNs,
    });

    var index = try repository_mod.loadRunTableIndexAllocWithStorage(host.storage(), alloc, run_path);
    defer index.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), index.entry_offsets.len);
    try std.testing.expectEqual(@as(usize, 0), ctx.run_file_reads);
    try std.testing.expectEqual(@as(usize, 1), ctx.run_range_reads);
    try std.testing.expectEqual(@as(usize, 1), ctx.run_trailer_reads);
    try std.testing.expectEqual(@as(usize, 0), ctx.run_file_size_reads);
}

test "lsm backend stale instance can still read after newer instance compacts and closes" {
    var path_buf: [256]u8 = undefined;
    const path = repository_mod.tmpPath(&path_buf, "stale-reader");
    defer repository_mod.cleanupTmp(path);

    {
        var writer = try Backend.open(std.testing.allocator, std.mem.span(path), .{
            .flush_threshold = 1,
            .compact_threshold_runs = 1,
            .foreground_soft_compaction = true,
        });
        defer writer.close();

        var runtime = try writer.runtimeStore(std.testing.allocator, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginWrite();
        try txn.put("doc:a", "A");
        try txn.commit();
    }

    var stale = try Backend.open(std.testing.allocator, std.mem.span(path), .{
        .flush_threshold = 1,
        .compact_threshold_runs = 1,
    });
    defer stale.close();

    {
        var writer = try Backend.open(std.testing.allocator, std.mem.span(path), .{
            .flush_threshold = 1,
            .compact_threshold_runs = 1,
            .foreground_soft_compaction = true,
        });

        var runtime = try writer.runtimeStore(std.testing.allocator, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginWrite();
        try txn.delete("doc:a");
        try txn.put("doc:b", "B");
        try txn.commit();

        try std.testing.expect(writer.obsolete_paths.items.len > 0);
        writer.close();
    }

    var stale_runtime = try stale.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer stale_runtime.deinit();
    var stale_txn = try stale_runtime.beginRead();
    defer stale_txn.abort();
    try std.testing.expectEqualStrings("A", try stale_txn.get("doc:a"));
}

test "lsm backend active reader survives obsolete cache eviction after writer compaction" {
    var path_buf: [256]u8 = undefined;
    const path = repository_mod.tmpPath(&path_buf, "active-reader-compaction");
    defer repository_mod.cleanupTmp(path);

    var backend = try Backend.open(std.testing.allocator, std.mem.span(path), .{
        .flush_threshold = 1,
        .compact_threshold_runs = 1,
        .foreground_soft_compaction = true,
    });
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    {
        var txn = try runtime.beginWrite();
        try txn.put("doc:a", "A");
        try txn.commit();
    }

    var read_txn = try runtime.beginRead();
    defer read_txn.abort();
    try std.testing.expectEqualStrings("A", try read_txn.get("doc:a"));
    var cur = try read_txn.openCursor();
    defer cur.close();
    try std.testing.expectEqualStrings("doc:a", (try cur.seekAtOrAfter("doc:a")).?.key);

    {
        var txn = try runtime.beginWrite();
        try txn.delete("doc:a");
        try txn.put("doc:b", "B");
        try txn.commit();
    }

    try std.testing.expect(backend.obsolete_paths.items.len > 0);
    try std.testing.expectEqualStrings("A", try read_txn.get("doc:a"));
    try std.testing.expectEqualStrings("doc:a", (try cur.seekAtOrAfter("doc:a")).?.key);
}

test "lsm backend reclaims obsolete run files after retention on a later writer commit" {
    const alloc = std.testing.allocator;

    const Context = struct {
        backing: *storage_io.MemoryStorage,
        now_ns: u64 = 0,

        fn createDirPath(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().createDirPath(path);
        }

        fn readFileAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, max_bytes: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().readFileAlloc(allocator, path, max_bytes);
        }

        fn readFileRangeAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, offset: u64, len: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().readFileRangeAlloc(allocator, path, offset, len);
        }

        fn fileSize(ptr: *anyopaque, path: []const u8) !u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().fileSize(path);
        }

        fn writeFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().writeFileAbsolute(path, contents);
        }

        fn renameAbsolute(ptr: *anyopaque, old_path: []const u8, new_path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().renameAbsolute(old_path, new_path);
        }

        fn deleteFileAbsolute(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().deleteFileAbsolute(path);
        }

        fn deleteTree(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().deleteTree(path);
        }

        fn nowNs(ptr: *anyopaque) u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.now_ns;
        }
    };

    var backing = storage_io.MemoryStorage.init(alloc);
    defer backing.deinit();

    var ctx = Context{ .backing = &backing };
    const host = storage_io.HostStorage.init(&ctx, &.{
        .create_dir_path = Context.createDirPath,
        .read_file_alloc = Context.readFileAlloc,
        .read_file_range_alloc = Context.readFileRangeAlloc,
        .file_size = Context.fileSize,
        .write_file_absolute = Context.writeFileAbsolute,
        .rename_absolute = Context.renameAbsolute,
        .delete_file_absolute = Context.deleteFileAbsolute,
        .delete_tree = Context.deleteTree,
        .now_ns = Context.nowNs,
    });

    const root_dir = "/host/lsm-obsolete-gc";
    const obsolete_path = try repository_mod.runPath(alloc, root_dir, 1);
    defer alloc.free(obsolete_path);

    {
        var backend = try Backend.open(alloc, root_dir, .{
            .storage = host.storage(),
            .flush_threshold = 1,
            .compact_threshold_runs = 1,
            .foreground_soft_compaction = true,
            .obsolete_retention_ns = 10,
        });
        defer backend.close();

        var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginWrite();
        try txn.put("doc:a", "A");
        try txn.commit();

        var next_txn = try runtime.beginWrite();
        try next_txn.delete("doc:a");
        try next_txn.put("doc:b", "B");
        try next_txn.commit();
    }

    {
        const bytes = try backing.storage().readFileAlloc(alloc, obsolete_path, 1024);
        defer alloc.free(bytes);
        try std.testing.expect(bytes.len > 0);
    }

    {
        var manifest_backing: ?[]u8 = null;
        defer if (manifest_backing) |raw| alloc.free(raw);
        var next_run_id: u64 = 0;
        var runs = std.ArrayListUnmanaged(repository_mod.Run).empty;
        var obsolete_paths = std.ArrayListUnmanaged(repository_mod.ObsoletePath).empty;
        defer {
            for (runs.items) |*run| run.deinit(alloc);
            runs.deinit(alloc);
            for (obsolete_paths.items) |*obsolete| obsolete.deinit(alloc);
            obsolete_paths.deinit(alloc);
        }

        try std.testing.expect(try repository_mod.loadManifestIfPresentWithStorage(
            host.storage(),
            alloc,
            root_dir,
            &manifest_backing,
            &next_run_id,
            &runs,
            &obsolete_paths,
        ));
        try std.testing.expectEqual(@as(usize, 1), obsolete_paths.items.len);
        if (manifest_backing) |raw| {
            alloc.free(raw);
            manifest_backing = null;
        }
        try std.testing.expectEqualStrings(obsolete_path, obsolete_paths.items[0].path);
        try std.testing.expectEqual(@as(u64, 10), obsolete_paths.items[0].delete_after_ns);
    }

    ctx.now_ns = 11;

    {
        var backend = try Backend.open(alloc, root_dir, .{
            .storage = host.storage(),
            .flush_threshold = 1,
            .compact_threshold_runs = 32,
            .obsolete_retention_ns = 10,
        });
        defer backend.close();

        var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginWrite();
        try txn.put("doc:c", "C");
        try txn.commit();
    }

    try std.testing.expectError(error.FileNotFound, backing.storage().readFileAlloc(alloc, obsolete_path, 1024));

    {
        var manifest_backing: ?[]u8 = null;
        defer if (manifest_backing) |raw| alloc.free(raw);
        var next_run_id: u64 = 0;
        var runs = std.ArrayListUnmanaged(repository_mod.Run).empty;
        var obsolete_paths = std.ArrayListUnmanaged(repository_mod.ObsoletePath).empty;
        defer {
            for (runs.items) |*run| run.deinit(alloc);
            runs.deinit(alloc);
            for (obsolete_paths.items) |*obsolete| obsolete.deinit(alloc);
            obsolete_paths.deinit(alloc);
        }

        try std.testing.expect(try repository_mod.loadManifestIfPresentWithStorage(
            host.storage(),
            alloc,
            root_dir,
            &manifest_backing,
            &next_run_id,
            &runs,
            &obsolete_paths,
        ));
        try std.testing.expectEqual(@as(usize, 0), obsolete_paths.items.len);
    }
}

test "lsm backend reclaims obsolete run files when last reader releases" {
    const alloc = std.testing.allocator;
    var backing = storage_io.MemoryStorage.init(alloc);
    defer backing.deinit();

    const root_dir = "/memory/lsm-reader-release-obsolete-gc";
    const obsolete_path = try repository_mod.runPath(alloc, root_dir, 1);
    defer alloc.free(obsolete_path);

    var backend = try Backend.open(alloc, root_dir, .{
        .storage = backing.storage(),
        .flush_threshold = 1,
        .compact_threshold_runs = 1,
        .foreground_soft_compaction = true,
        .obsolete_retention_ns = 0,
    });
    defer backend.close();

    var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime.deinit();

    {
        var txn = try runtime.beginWrite();
        try txn.put("doc:a", "A");
        try txn.commit();
    }

    var read_txn = try runtime.beginRead();
    try std.testing.expectEqualStrings("A", try read_txn.get("doc:a"));

    {
        var txn = try runtime.beginWrite();
        try txn.delete("doc:a");
        try txn.put("doc:b", "B");
        try txn.commit();
    }

    try std.testing.expect(backend.obsolete_paths.items.len > 0);
    {
        const bytes = try backing.storage().readFileAlloc(alloc, obsolete_path, 1024);
        defer alloc.free(bytes);
        try std.testing.expect(bytes.len > 0);
    }

    read_txn.abort();
    try std.testing.expectEqual(@as(usize, 0), backend.active_readers);
    try std.testing.expectEqual(@as(usize, 0), backend.obsolete_paths.items.len);
    try std.testing.expectError(error.FileNotFound, backing.storage().readFileAlloc(alloc, obsolete_path, 1024));

    var manifest_backing: ?[]u8 = null;
    defer if (manifest_backing) |raw| alloc.free(raw);
    var next_run_id: u64 = 0;
    var runs = std.ArrayListUnmanaged(repository_mod.Run).empty;
    var obsolete_paths = std.ArrayListUnmanaged(repository_mod.ObsoletePath).empty;
    defer {
        for (runs.items) |*run| run.deinit(alloc);
        runs.deinit(alloc);
        for (obsolete_paths.items) |*obsolete| obsolete.deinit(alloc);
        obsolete_paths.deinit(alloc);
    }

    try std.testing.expect(try repository_mod.loadManifestIfPresentWithStorage(
        backing.storage(),
        alloc,
        root_dir,
        &manifest_backing,
        &next_run_id,
        &runs,
        &obsolete_paths,
    ));
    try std.testing.expectEqual(@as(usize, 0), obsolete_paths.items.len);
}

test "lsm backend reader release reclaims expired clean obsolete paths" {
    const alloc = std.testing.allocator;
    var backing = storage_io.MemoryStorage.init(alloc);
    defer backing.deinit();

    const root_dir = "/memory/lsm-reader-release-clean-obsolete-gc";
    const obsolete_path = try repository_mod.runPath(alloc, root_dir, 999);
    defer alloc.free(obsolete_path);

    var backend = try Backend.open(alloc, root_dir, .{
        .storage = backing.storage(),
        .obsolete_retention_ns = 0,
    });
    defer backend.close();

    try repository_mod.writeFileAbsoluteWithStorage(backend.storage.?, obsolete_path, "obsolete");
    try backend.obsolete_paths.append(alloc, .{
        .path = try alloc.dupe(u8, obsolete_path),
        .delete_after_ns = 0,
    });
    backend.manifest_dirty = false;
    backend.obsolete_manifest_dirty = false;

    var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime.deinit();

    var read_txn = try runtime.beginRead();
    try std.testing.expectEqual(@as(usize, 1), backend.active_readers);
    read_txn.abort();

    try std.testing.expectEqual(@as(usize, 0), backend.active_readers);
    try std.testing.expectEqual(@as(usize, 0), backend.obsolete_paths.items.len);
    try std.testing.expectError(error.FileNotFound, backing.storage().readFileAlloc(alloc, obsolete_path, 1024));
}

test "lsm backend reloads persisted manifest and run files" {
    var path_buf: [256]u8 = undefined;
    const path = repository_mod.tmpPath(&path_buf, "reload");
    defer repository_mod.cleanupTmp(path);

    {
        var backend = try Backend.open(std.testing.allocator, std.mem.span(path), .{ .flush_threshold = 1 });
        defer backend.close();

        var runtime = try backend.runtimeNamespaceStore(std.testing.allocator);
        defer runtime.deinit();

        var txn = try runtime.beginWrite();
        try txn.put(.{ .name = "docs" }, "doc:a", "A");
        try txn.put(.{ .name = "docs" }, "doc:b", "B");
        try txn.commit();

        var delete_txn = try runtime.beginWrite();
        try delete_txn.delete(.{ .name = "docs" }, "doc:b");
        try delete_txn.put(.{}, "meta:lsn", "7");
        try delete_txn.commit();
    }

    {
        var reopened = try Backend.open(std.testing.allocator, std.mem.span(path), .{ .flush_threshold = 1 });
        defer reopened.close();

        var runtime = try reopened.runtimeNamespaceStore(std.testing.allocator);
        defer runtime.deinit();

        var txn = try runtime.beginRead();
        defer txn.abort();
        try std.testing.expectEqualStrings("A", try txn.get(.{ .name = "docs" }, "doc:a"));
        try std.testing.expectError(error.NotFound, txn.get(.{ .name = "docs" }, "doc:b"));
        try std.testing.expectEqualStrings("7", try txn.get(.{}, "meta:lsn"));
    }
}

test "lsm backend reloads persisted manifest and run files over memory storage" {
    var memory_storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer memory_storage.deinit();

    const root_dir = "/memory/reload";

    {
        var backend = try Backend.open(std.testing.allocator, root_dir, .{
            .flush_threshold = 1,
            .storage = memory_storage.storage(),
        });
        defer backend.close();

        var runtime = try backend.runtimeNamespaceStore(std.testing.allocator);
        defer runtime.deinit();

        var txn = try runtime.beginWrite();
        try txn.put(.{ .name = "docs" }, "doc:a", "A");
        try txn.put(.{ .name = "docs" }, "doc:b", "B");
        try txn.commit();

        var delete_txn = try runtime.beginWrite();
        try delete_txn.delete(.{ .name = "docs" }, "doc:b");
        try delete_txn.put(.{}, "meta:lsn", "7");
        try delete_txn.commit();
    }

    {
        var reopened = try Backend.open(std.testing.allocator, root_dir, .{
            .flush_threshold = 1,
            .storage = memory_storage.storage(),
        });
        defer reopened.close();

        var runtime = try reopened.runtimeNamespaceStore(std.testing.allocator);
        defer runtime.deinit();

        var txn = try runtime.beginRead();
        defer txn.abort();
        try std.testing.expectEqualStrings("A", try txn.get(.{ .name = "docs" }, "doc:a"));
        try std.testing.expectError(error.NotFound, txn.get(.{ .name = "docs" }, "doc:b"));
        try std.testing.expectEqualStrings("7", try txn.get(.{}, "meta:lsn"));
    }
}

test "lsm backend splits oversized flushes into persisted run segments" {
    const alloc = std.testing.allocator;
    var memory_storage = storage_io.MemoryStorage.init(alloc);
    defer memory_storage.deinit();

    const root_dir = "/memory/split-flush";
    const value = try alloc.alloc(u8, 70);
    defer alloc.free(value);
    @memset(value, 'v');

    {
        var backend = try Backend.open(alloc, root_dir, .{
            .flush_threshold = 8,
            .compact_threshold_runs = 100,
            .max_run_file_bytes = 180,
            .storage = memory_storage.storage(),
        });
        defer backend.close();

        var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginWrite();
        var key_buf: [32]u8 = undefined;
        for (0..8) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "doc:{d:0>3}", .{i});
            try txn.put(key, value);
        }
        try txn.commit();

        try std.testing.expect(backend.runs.items.len > 1);
        for (backend.runs.items) |run| {
            try std.testing.expect(run.entry_count <= 2);
            try std.testing.expect(run.state == null);
        }
    }

    {
        var reopened = try Backend.open(alloc, root_dir, .{
            .flush_threshold = 8,
            .compact_threshold_runs = 100,
            .max_run_file_bytes = 180,
            .storage = memory_storage.storage(),
        });
        defer reopened.close();

        var runtime = try reopened.runtimeStore(alloc, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginRead();
        defer txn.abort();
        var key_buf: [32]u8 = undefined;
        for (0..8) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "doc:{d:0>3}", .{i});
            try std.testing.expectEqualStrings(value, try txn.get(key));
        }
    }
}

test "lsm backend splits oversized compaction output into persisted run segments" {
    const alloc = std.testing.allocator;
    var memory_storage = storage_io.MemoryStorage.init(alloc);
    defer memory_storage.deinit();

    const root_dir = "/memory/split-compaction";
    const value = try alloc.alloc(u8, 70);
    defer alloc.free(value);
    @memset(value, 'c');

    {
        var backend = try Backend.open(alloc, root_dir, .{
            .flush_threshold = 1,
            .compact_threshold_runs = 2,
            .foreground_soft_compaction = true,
            .level_target_runs_base = 100,
            .level_target_bytes_base = 0,
            .max_run_file_bytes = 120,
            .storage = memory_storage.storage(),
        });
        defer backend.close();

        var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer runtime.deinit();

        var key_buf: [32]u8 = undefined;
        for (0..3) |i| {
            var txn = try runtime.beginWrite();
            const key = try std.fmt.bufPrint(&key_buf, "doc:{d:0>3}", .{i});
            try txn.put(key, value);
            try txn.commit();
        }

        var level_one_runs: usize = 0;
        for (backend.runs.items) |run| {
            if (run.level == 1) level_one_runs += 1;
            try std.testing.expect(run.state == null);
        }
        try std.testing.expect(level_one_runs >= 2);
    }

    {
        var reopened = try Backend.open(alloc, root_dir, .{
            .flush_threshold = 1,
            .compact_threshold_runs = 2,
            .level_target_runs_base = 100,
            .level_target_bytes_base = 0,
            .max_run_file_bytes = 120,
            .storage = memory_storage.storage(),
        });
        defer reopened.close();

        var runtime = try reopened.runtimeStore(alloc, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginRead();
        defer txn.abort();
        var key_buf: [32]u8 = undefined;
        for (0..3) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "doc:{d:0>3}", .{i});
            try std.testing.expectEqualStrings(value, try txn.get(key));
        }
    }
}

test "lsm backend manifest layout validation keeps WAL and bulk session when run metadata is inconsistent" {
    const alloc = std.testing.allocator;
    var memory_storage = storage_io.MemoryStorage.init(alloc);
    defer memory_storage.deinit();

    const root_dir = "/memory/durable-manifest-validation";
    var backend = try Backend.open(alloc, root_dir, .{
        .flush_threshold = 1,
        .bulk_ingest_flush_threshold_multiplier = 1,
        .storage = memory_storage.storage(),
    });
    defer backend.close();

    try backend.beginBulkIngestSession();
    errdefer if (backend.bulkIngestActive()) backend.abortBulkIngestSession();

    {
        var txn = try backend.beginBatchWithOptions(.{ .mode = .bulk_ingest });
        try txn.put(.{ .name = "docs" }, "doc:a", "A");
        try txn.commit();
    }

    try std.testing.expect(backend.bulkIngestActive());
    try std.testing.expectEqual(@as(usize, 1), backend.runs.items.len);
    try std.testing.expect(backend.manifest_dirty);
    const before = backend.snapshotMaintenanceStats();
    try std.testing.expect(before.wal_retained_bytes > 0);

    const original_entry_count = backend.runs.items[0].entry_count;
    backend.runs.items[0].entry_count = 0;
    try std.testing.expectError(error.InvalidTableFile, backend.finishBulkIngestSessionWithOptions(.{ .compact = false }));
    try std.testing.expect(backend.bulkIngestActive());
    try std.testing.expect(backend.manifest_dirty);
    const after_failed_publish = backend.snapshotMaintenanceStats();
    try std.testing.expect(after_failed_publish.wal_retained_bytes > 0);

    backend.runs.items[0].entry_count = original_entry_count;
    try backend.finishBulkIngestSessionWithOptions(.{ .compact = false });
    try std.testing.expect(!backend.bulkIngestActive());
    const after_repair = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(@as(u64, 0), after_repair.wal_retained_bytes);
}

test "lsm backend deferred byte-threshold WAL flush preserves DB-style artifacts across reopen" {
    const alloc = std.testing.allocator;
    var memory_storage = storage_io.MemoryStorage.init(alloc);
    defer memory_storage.deinit();

    const root_dir = "/memory/deferred-byte-threshold-db-style";
    const primary_value = try alloc.alloc(u8, 64);
    defer alloc.free(primary_value);
    @memset(primary_value, 'p');
    const ttl_value = try alloc.alloc(u8, 8);
    defer alloc.free(ttl_value);
    @memset(ttl_value, 't');
    const embedding_value = try alloc.alloc(u8, 512);
    defer alloc.free(embedding_value);
    @memset(embedding_value, 'e');

    const opts = Options{
        .flush_threshold = 100_000,
        .flush_threshold_bytes = 32 * 1024,
        .compact_threshold_runs = 4,
        .l0_overlap_compact_threshold_runs = 2,
        .wal_segment_bytes = 64 * 1024,
        .storage = memory_storage.storage(),
    };

    {
        var backend = try Backend.open(alloc, root_dir, opts);
        defer backend.close();

        var raw_key_buf: [32]u8 = undefined;
        var written: usize = 0;
        while (written < 500) {
            var txn = try backend.beginWrite();
            errdefer txn.abort();
            for (0..20) |offset| {
                const i = written + offset;
                const raw_key = try std.fmt.bufPrint(&raw_key_buf, "key:{d}", .{i});
                const doc_key = try internal_keys.documentKeyAlloc(alloc, raw_key);
                defer alloc.free(doc_key);
                const ttl_key = try internal_keys.ttlKeyAlloc(alloc, raw_key);
                defer alloc.free(ttl_key);
                const embedding_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, raw_key, "vec");
                defer alloc.free(embedding_key);

                try txn.put(.{ .name = "docs" }, doc_key, primary_value);
                try txn.put(.{ .name = "docs" }, ttl_key, ttl_value);
                try txn.put(.{ .name = "docs" }, embedding_key, embedding_value);
            }
            try txn.commit();
            written += 20;
        }
        try backend.sync(true);
        const maintenance = backend.snapshotMaintenanceStats();
        try std.testing.expect(maintenance.total_runs > 0);
        try std.testing.expectEqual(@as(u64, 0), maintenance.wal_retained_bytes);
    }

    {
        var reopened = try Backend.open(alloc, root_dir, opts);
        defer reopened.close();

        var runtime = try reopened.runtimeStore(alloc, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginRead();
        defer txn.abort();
        var raw_key_buf: [32]u8 = undefined;
        for (0..500) |i| {
            const raw_key = try std.fmt.bufPrint(&raw_key_buf, "key:{d}", .{i});
            const doc_key = try internal_keys.documentKeyAlloc(alloc, raw_key);
            defer alloc.free(doc_key);
            const ttl_key = try internal_keys.ttlKeyAlloc(alloc, raw_key);
            defer alloc.free(ttl_key);
            const embedding_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, raw_key, "vec");
            defer alloc.free(embedding_key);

            try std.testing.expectEqualStrings(primary_value, try txn.get(doc_key));
            try std.testing.expectEqualStrings(ttl_value, try txn.get(ttl_key));
            try std.testing.expectEqualStrings(embedding_value, try txn.get(embedding_key));
        }
    }
}

test "lsm backend recovery replay flushes incrementally and retires covered wal on reopen" {
    var memory_storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer memory_storage.deinit();

    const root_dir = "/memory/recovery-incremental-replay";
    try memory_storage.storage().createDirPath(root_dir);

    var i: usize = 0;
    while (i < 6) : (i += 1) {
        var state: State = .{};
        defer state.deinit(std.testing.allocator);

        var key_buf: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "doc:{d}", .{i});
        try state.upsert(std.testing.allocator, .{ .name = "docs" }, key, "value", false);
        _ = try wal_mod.appendStateWithOptions(
            memory_storage.storage(),
            std.testing.allocator,
            root_dir,
            &state,
            false,
            .{ .segment_bytes = 96 },
        );
    }

    const before = try wal_mod.snapshotRetention(memory_storage.storage(), std.testing.allocator, root_dir);
    try std.testing.expect(before.segments > 1);
    try std.testing.expect(before.bytes > 0);

    {
        var reopened = try Backend.open(std.testing.allocator, root_dir, .{
            .flush_threshold = 2,
            .storage = memory_storage.storage(),
        });
        defer reopened.close();

        const stats = reopened.snapshotMaintenanceStats();
        try std.testing.expectEqual(@as(u64, 0), stats.mutable_entries);
        try std.testing.expectEqual(@as(u64, 0), stats.immutable_memtables);
        try std.testing.expect(reopened.runs.items.len > 0);

        var runtime = try reopened.runtimeNamespaceStore(std.testing.allocator);
        defer runtime.deinit();
        var txn = try runtime.beginRead();
        defer txn.abort();
        i = 0;
        while (i < 6) : (i += 1) {
            var key_buf: [32]u8 = undefined;
            const key = try std.fmt.bufPrint(&key_buf, "doc:{d}", .{i});
            try std.testing.expectEqualStrings("value", try txn.get(.{ .name = "docs" }, key));
        }
    }

    {
        var reopened = try Backend.open(std.testing.allocator, root_dir, .{
            .flush_threshold = 2,
            .storage = memory_storage.storage(),
        });
        defer reopened.close();

        const write_stats = reopened.snapshotWriteStats();
        try std.testing.expectEqual(@as(u64, 0), write_stats.wal_replay_bytes);
        try std.testing.expectEqual(@as(u64, 0), write_stats.wal_replay_records);
    }
}

test "lsm backend reloads persisted manifest and run files over host storage" {
    var memory_storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer memory_storage.deinit();

    const HostContext = struct {
        backing: *storage_io.MemoryStorage,

        fn createDirPath(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().createDirPath(path);
        }

        fn readFileAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, max_bytes: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().readFileAlloc(allocator, path, max_bytes);
        }

        fn readFileRangeAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, offset: u64, len: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().readFileRangeAlloc(allocator, path, offset, len);
        }

        fn fileSize(ptr: *anyopaque, path: []const u8) !u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().fileSize(path);
        }

        fn writeFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().writeFileAbsolute(path, contents);
        }

        fn renameAbsolute(ptr: *anyopaque, old_path: []const u8, new_path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().renameAbsolute(old_path, new_path);
        }

        fn deleteFileAbsolute(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().deleteFileAbsolute(path);
        }

        fn deleteTree(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().deleteTree(path);
        }

        fn nowNs(ptr: *anyopaque) u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().nowNs();
        }
    };

    const host_vtable: storage_io.Storage.VTable = .{
        .create_dir_path = HostContext.createDirPath,
        .read_file_alloc = HostContext.readFileAlloc,
        .read_file_range_alloc = HostContext.readFileRangeAlloc,
        .file_size = HostContext.fileSize,
        .write_file_absolute = HostContext.writeFileAbsolute,
        .rename_absolute = HostContext.renameAbsolute,
        .delete_file_absolute = HostContext.deleteFileAbsolute,
        .delete_tree = HostContext.deleteTree,
        .now_ns = HostContext.nowNs,
    };

    var host_ctx = HostContext{ .backing = &memory_storage };
    const host_storage = storage_io.HostStorage.init(&host_ctx, &host_vtable);

    const root_dir = "/host/reload";

    {
        var backend = try Backend.open(std.testing.allocator, root_dir, .{
            .flush_threshold = 1,
            .storage = host_storage.storage(),
        });
        defer backend.close();

        var runtime = try backend.runtimeNamespaceStore(std.testing.allocator);
        defer runtime.deinit();

        var txn = try runtime.beginWrite();
        try txn.put(.{ .name = "docs" }, "doc:a", "A");
        try txn.put(.{}, "meta:epoch", "9");
        try txn.commit();
    }

    {
        var reopened = try Backend.open(std.testing.allocator, root_dir, .{
            .flush_threshold = 1,
            .storage = host_storage.storage(),
        });
        defer reopened.close();

        var runtime = try reopened.runtimeNamespaceStore(std.testing.allocator);
        defer runtime.deinit();

        var txn = try runtime.beginRead();
        defer txn.abort();
        try std.testing.expectEqualStrings("A", try txn.get(.{ .name = "docs" }, "doc:a"));
        try std.testing.expectEqualStrings("9", try txn.get(.{}, "meta:epoch"));
    }
}

test "lsm backend compacts oldest persisted runs and reopens" {
    var path_buf: [256]u8 = undefined;
    const path = repository_mod.tmpPath(&path_buf, "compact");
    defer repository_mod.cleanupTmp(path);

    {
        var backend = try Backend.open(std.testing.allocator, std.mem.span(path), .{
            .flush_threshold = 1,
            .compact_threshold_runs = 2,
            .foreground_soft_compaction = true,
        });
        defer backend.close();

        var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
        defer runtime.deinit();

        {
            var txn = try runtime.beginWrite();
            try txn.put("doc:a", "A1");
            try txn.commit();
        }
        {
            var txn = try runtime.beginWrite();
            try txn.put("doc:b", "B1");
            try txn.commit();
        }
        {
            var txn = try runtime.beginWrite();
            try txn.put("doc:a", "A2");
            try txn.commit();
        }

        try std.testing.expect(backend.runs.items.len <= 2);
    }

    {
        var reopened = try Backend.open(std.testing.allocator, std.mem.span(path), .{
            .flush_threshold = 1,
            .compact_threshold_runs = 2,
            .foreground_soft_compaction = true,
        });
        defer reopened.close();

        try std.testing.expect(reopened.runs.items.len <= 2);

        var runtime = try reopened.runtimeStore(std.testing.allocator, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginRead();
        defer txn.abort();
        try std.testing.expectEqualStrings("A2", try txn.get("doc:a"));
        try std.testing.expectEqualStrings("B1", try txn.get("doc:b"));
    }
}

test "lsm backend persists run levels across reopen" {
    var path_buf: [256]u8 = undefined;
    const path = repository_mod.tmpPath(&path_buf, "levels");
    defer repository_mod.cleanupTmp(path);

    var expected_levels = std.ArrayListUnmanaged(u32).empty;
    defer expected_levels.deinit(std.testing.allocator);

    {
        var backend = try Backend.open(std.testing.allocator, std.mem.span(path), .{
            .flush_threshold = 1,
            .compact_threshold_runs = 2,
            .foreground_soft_compaction = true,
        });
        defer backend.close();

        var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
        defer runtime.deinit();

        {
            var txn = try runtime.beginWrite();
            try txn.put("doc:a", "A1");
            try txn.commit();
        }
        {
            var txn = try runtime.beginWrite();
            try txn.put("doc:b", "B1");
            try txn.commit();
        }
        {
            var txn = try runtime.beginWrite();
            try txn.put("doc:a", "A2");
            try txn.commit();
        }

        try std.testing.expectEqual(@as(usize, 2), backend.runs.items.len);
        try expectLowerLevelsNonOverlapping(backend.runs.items);
        for (backend.runs.items) |run| {
            try std.testing.expect(run.size_bytes > 0);
            try expected_levels.append(std.testing.allocator, run.level);
        }
        try std.testing.expectEqualSlices(u32, &.{ 0, 1 }, expected_levels.items);
    }

    {
        var reopened = try Backend.open(std.testing.allocator, std.mem.span(path), .{
            .flush_threshold = 1,
            .compact_threshold_runs = 2,
            .foreground_soft_compaction = true,
        });
        defer reopened.close();

        try std.testing.expectEqual(expected_levels.items.len, reopened.runs.items.len);
        try expectLowerLevelsNonOverlapping(reopened.runs.items);
        for (reopened.runs.items, expected_levels.items) |run, expected_level| {
            try std.testing.expectEqual(expected_level, run.level);
            try std.testing.expect(run.size_bytes > 0);
        }
    }
}

test "lsm backend pressure compacts lower levels" {
    var path_buf: [256]u8 = undefined;
    const path = repository_mod.tmpPath(&path_buf, "level-pressure");
    defer repository_mod.cleanupTmp(path);

    const level_target_runs_base = 1;
    const level_target_runs_multiplier = 1;

    {
        var backend = try Backend.open(std.testing.allocator, std.mem.span(path), .{
            .flush_threshold = 1,
            .compact_threshold_runs = 2,
            .foreground_soft_compaction = true,
            .level_target_runs_base = level_target_runs_base,
            .level_target_runs_multiplier = level_target_runs_multiplier,
        });
        defer backend.close();

        var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
        defer runtime.deinit();

        const keys = [_][]const u8{ "doc:a", "doc:b", "doc:c", "doc:d", "doc:e", "doc:f", "doc:g" };
        for (keys, 0..) |key, idx| {
            var txn = try runtime.beginWrite();
            const value = try std.fmt.allocPrint(std.testing.allocator, "V{d}", .{idx});
            defer std.testing.allocator.free(value);
            try txn.put(key, value);
            try txn.commit();
        }

        try expectLowerLevelsNonOverlapping(backend.runs.items);
        try expectLevelTargetsSatisfied(backend.runs.items, level_target_runs_base, level_target_runs_multiplier);
        for (backend.runs.items) |run| try std.testing.expect(run.size_bytes > 0);
    }

    {
        var reopened = try Backend.open(std.testing.allocator, std.mem.span(path), .{
            .flush_threshold = 1,
            .compact_threshold_runs = 2,
            .foreground_soft_compaction = true,
            .level_target_runs_base = level_target_runs_base,
            .level_target_runs_multiplier = level_target_runs_multiplier,
        });
        defer reopened.close();

        try expectLowerLevelsNonOverlapping(reopened.runs.items);
        try expectLevelTargetsSatisfied(reopened.runs.items, level_target_runs_base, level_target_runs_multiplier);
        for (reopened.runs.items) |run| try std.testing.expect(run.size_bytes > 0);
    }
}

test "lsm backend fast split prepares child and rewrites left in place" {
    var parent_buf: [256]u8 = undefined;
    const parent_path = repository_mod.tmpPath(&parent_buf, "split-parent");
    defer repository_mod.cleanupTmp(parent_path);

    var child_buf: [256]u8 = undefined;
    const child_path = repository_mod.tmpPath(&child_buf, "split-child");
    defer repository_mod.cleanupTmp(child_path);

    var backend = try Backend.open(std.testing.allocator, std.mem.span(parent_path), .{
        .flush_threshold = 1,
    });
    defer backend.close();

    {
        var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginWrite();
        try txn.put("doc:a", "A");
        try txn.put("doc:z", "Z");
        try txn.commit();
    }

    try std.testing.expect(try backend.prepareSplitRightToDir("doc:m", std.mem.span(child_path), .{
        .backend = .{
            .durability = .none,
        },
        .flush_threshold = 1,
    }));

    {
        var child = try Backend.open(std.testing.allocator, std.mem.span(child_path), .{
            .flush_threshold = 1,
        });
        defer child.close();
        var runtime = try child.runtimeStore(std.testing.allocator, .{ .name = "docs" });
        defer runtime.deinit();
        var txn = try runtime.beginRead();
        defer txn.abort();
        try std.testing.expectEqualStrings("Z", try txn.get("doc:z"));
        try std.testing.expectError(error.NotFound, txn.get("doc:a"));
    }

    try std.testing.expect(try backend.rewriteLeftInPlace("doc:m"));

    {
        var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
        defer runtime.deinit();
        var txn = try runtime.beginRead();
        defer txn.abort();
        try std.testing.expectEqualStrings("A", try txn.get("doc:a"));
        try std.testing.expectError(error.NotFound, txn.get("doc:z"));
    }
}

test "lsm backend ignores stray temp files on reopen" {
    var path_buf: [256]u8 = undefined;
    const path = repository_mod.tmpPath(&path_buf, "temp-artifacts");
    defer repository_mod.cleanupTmp(path);

    {
        var backend = try Backend.open(std.testing.allocator, std.mem.span(path), .{
            .flush_threshold = 1,
            .compact_threshold_runs = 2,
        });
        defer backend.close();

        var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginWrite();
        try txn.put("doc:a", "A");
        try txn.commit();

        const manifest_path = try repository_mod.manifestPath(std.testing.allocator, std.mem.span(path));
        defer std.testing.allocator.free(manifest_path);
        const manifest_tmp = try repository_mod.tempSiblingPath(std.testing.allocator, manifest_path);
        defer std.testing.allocator.free(manifest_tmp);
        try repository_mod.writeFileAbsolute(manifest_tmp, "corrupt-temp-manifest");

        const run_path = try repository_mod.runPath(std.testing.allocator, std.mem.span(path), 9999);
        defer std.testing.allocator.free(run_path);
        const run_tmp = try repository_mod.tempSiblingPath(std.testing.allocator, run_path);
        defer std.testing.allocator.free(run_tmp);
        try repository_mod.writeFileAbsolute(run_tmp, "corrupt-temp-run");
    }

    {
        var reopened = try Backend.open(std.testing.allocator, std.mem.span(path), .{
            .flush_threshold = 1,
            .compact_threshold_runs = 2,
        });
        defer reopened.close();

        var runtime = try reopened.runtimeStore(std.testing.allocator, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginRead();
        defer txn.abort();
        try std.testing.expectEqualStrings("A", try txn.get("doc:a"));
        try std.testing.expectEqual(@as(usize, 1), reopened.runs.items.len);
    }
}

test "lsm backend ignores orphaned committed run files not referenced by manifest" {
    var path_buf: [256]u8 = undefined;
    const path = repository_mod.tmpPath(&path_buf, "orphan-run");
    defer repository_mod.cleanupTmp(path);

    {
        var backend = try Backend.open(std.testing.allocator, std.mem.span(path), .{
            .flush_threshold = 1,
            .compact_threshold_runs = 2,
        });
        defer backend.close();

        var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginWrite();
        try txn.put("doc:a", "A");
        try txn.commit();

        const orphan_path = try repository_mod.runPath(std.testing.allocator, std.mem.span(path), 9999);
        defer std.testing.allocator.free(orphan_path);
        const orphan_entries = [_]lsm_table_file.Entry{
            .{ .namespace_name = "docs", .key = "doc:orphan", .value = "O", .tombstone = false },
        };
        const encoded = try lsm_table_file.encodeAlloc(std.testing.allocator, &orphan_entries);
        defer std.testing.allocator.free(encoded);
        try repository_mod.writeFileAbsolute(orphan_path, encoded);
    }

    {
        var reopened = try Backend.open(std.testing.allocator, std.mem.span(path), .{
            .flush_threshold = 1,
            .compact_threshold_runs = 2,
        });
        defer reopened.close();

        try std.testing.expectEqual(@as(usize, 1), reopened.runs.items.len);

        var runtime = try reopened.runtimeStore(std.testing.allocator, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginRead();
        defer txn.abort();
        try std.testing.expectEqualStrings("A", try txn.get("doc:a"));
        try std.testing.expectError(error.NotFound, txn.get("doc:orphan"));
    }
}
test "lsm backend mutable read snapshot retirement allocation failure keeps snapshot cleanup reachable" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const alloc = failing.allocator();
    var backend = Backend.init(alloc, .{});

    {
        var txn = try backend.beginWrite();
        try txn.put(.{ .name = "docs" }, "doc:a", "A");
        try txn.commit();
    }

    var read_a = try backend.beginRead();
    var read_a_active = true;
    defer if (read_a_active) read_a.abort();
    try std.testing.expectEqualStrings("A", try read_a.get(.{ .name = "docs" }, "doc:a"));
    const first_snapshot = backend.mutable_read_snapshot orelse return error.TestUnexpectedResult;

    failing.fail_index = failing.alloc_index;
    failing.resize_fail_index = failing.resize_index;
    backend.invalidateMutableReadSnapshot();
    failing.fail_index = std.math.maxInt(usize);
    failing.resize_fail_index = std.math.maxInt(usize);

    try std.testing.expectEqual(@as(?*State, null), backend.mutable_read_snapshot);
    try std.testing.expectEqual(@as(usize, 1), backend.retired_mutable_snapshots.items.len);
    try std.testing.expect(backend.retired_mutable_snapshots.items[0] == first_snapshot);

    read_a.abort();
    read_a_active = false;
    try std.testing.expectEqual(@as(usize, 0), backend.retired_mutable_snapshots.items.len);
    backend.close();
    try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
}
