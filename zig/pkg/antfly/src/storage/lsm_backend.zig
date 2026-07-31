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
const platform_time = @import("antfly_platform").time;

const State = state_mod.State;
const ActiveMemTable = state_mod.ActiveMemTable;
const SplitStates = state_mod.SplitStates;
const Run = repository_mod.Run;
const ObsoletePath = repository_mod.ObsoletePath;
const namespaceOf = state_mod.namespaceOf;
const compareNamespace = state_mod.compareNamespace;
const compareEntryTo = state_mod.compareEntryTo;
const CounterU64 = platform.atomic.Value(u64);
// Keep flush_threshold_bytes as a logical run-sizing control. Actual resident
// mutable memory can be higher because arenas retain overwritten values and
// hash tables reserve capacity, so enforce a separate bounded safety limit.
const mutable_memory_guard_multiplier: u64 = 2;

const supports_waitable_immutable_flush = builtin.os.tag != .freestanding and
    builtin.link_libc and
    @hasDecl(std.c, "pthread_cond_wait");

/// A writer may reach the immutable byte limit while another thread is
/// building the oldest immutable table without the backend mutex held. The
/// completion epoch lets that writer release the backend mutex and sleep until
/// the builder publishes, rather than either spinning or admitting another
/// unbounded generation.
const ImmutableFlushCompletion = if (supports_waitable_immutable_flush)
    struct {
        mutex: std.c.pthread_mutex_t = std.c.PTHREAD_MUTEX_INITIALIZER,
        cond: std.c.pthread_cond_t = std.c.PTHREAD_COND_INITIALIZER,
        epoch: CounterU64 = .init(0),

        fn snapshot(self: *@This()) u64 {
            return self.epoch.load(.acquire);
        }

        fn waitForChange(self: *@This(), observed: u64) void {
            if (std.c.pthread_mutex_lock(&self.mutex) != .SUCCESS) unreachable;
            defer if (std.c.pthread_mutex_unlock(&self.mutex) != .SUCCESS) unreachable;
            while (self.epoch.load(.acquire) == observed) {
                if (std.c.pthread_cond_wait(&self.cond, &self.mutex) != .SUCCESS) unreachable;
            }
        }

        fn advance(self: *@This()) void {
            if (std.c.pthread_mutex_lock(&self.mutex) != .SUCCESS) unreachable;
            _ = self.epoch.fetchAdd(1, .release);
            if (std.c.pthread_cond_broadcast(&self.cond) != .SUCCESS) unreachable;
            if (std.c.pthread_mutex_unlock(&self.mutex) != .SUCCESS) unreachable;
        }
    }
else
    struct {
        epoch: CounterU64 = .init(0),

        fn snapshot(self: *@This()) u64 {
            return self.epoch.load(.acquire);
        }

        fn waitForChange(_: *@This(), _: u64) void {}

        fn advance(self: *@This()) void {
            _ = self.epoch.fetchAdd(1, .release);
        }
    };

const ObsoletePathRef = struct {
    path: []u8,
    count: u64,
};

const ObsoletePathRefRegistry = struct {
    mutex: std.atomic.Mutex = .unlocked,
    refs: std.ArrayListUnmanaged(ObsoletePathRef) = .empty,

    fn retain(self: *ObsoletePathRefRegistry, path: []const u8) !void {
        lockWorkerMutex(&self.mutex);
        defer self.mutex.unlock();
        for (self.refs.items) |*ref| {
            if (!std.mem.eql(u8, ref.path, path)) continue;
            ref.count +|= 1;
            return;
        }
        const owned = try std.heap.page_allocator.dupe(u8, path);
        errdefer std.heap.page_allocator.free(owned);
        try self.refs.append(std.heap.page_allocator, .{ .path = owned, .count = 1 });
    }

    fn release(self: *ObsoletePathRefRegistry, path: []const u8) void {
        lockWorkerMutex(&self.mutex);
        defer self.mutex.unlock();
        var i: usize = 0;
        while (i < self.refs.items.len) : (i += 1) {
            const ref = &self.refs.items[i];
            if (!std.mem.eql(u8, ref.path, path)) continue;
            if (ref.count > 1) {
                ref.count -= 1;
                return;
            }
            std.heap.page_allocator.free(ref.path);
            _ = self.refs.swapRemove(i);
            return;
        }
    }

    fn isRetained(self: *ObsoletePathRefRegistry, path: []const u8) bool {
        lockWorkerMutex(&self.mutex);
        defer self.mutex.unlock();
        for (self.refs.items) |ref| {
            if (std.mem.eql(u8, ref.path, path) and ref.count > 0) return true;
        }
        return false;
    }
};

var obsolete_path_refs = ObsoletePathRefRegistry{};
// Read transactions borrow run metadata instead of cloning it. Track those
// borrows separately from open-manifest ownership so compaction can retire
// each generation as soon as its own last reader exits, without waiting for a
// backend-wide reader-free instant.
const RunSnapshotRefRegistry = struct {
    mutex: std.atomic.Mutex = .unlocked,
    refs: std.StringHashMapUnmanaged(u64) = .empty,

    fn retain(self: *RunSnapshotRefRegistry, path: []const u8) !void {
        lockWorkerMutex(&self.mutex);
        defer self.mutex.unlock();
        if (self.refs.getPtr(path)) |count| {
            count.* +|= 1;
            return;
        }
        // Cache one stable path allocation for the run generation rather than
        // allocating a path for every read transaction.
        const owned = try std.heap.page_allocator.dupe(u8, path);
        errdefer std.heap.page_allocator.free(owned);
        try self.refs.put(std.heap.page_allocator, owned, 1);
    }

    fn release(self: *RunSnapshotRefRegistry, path: []const u8) void {
        lockWorkerMutex(&self.mutex);
        defer self.mutex.unlock();
        const count = self.refs.getPtr(path) orelse return;
        if (count.* > 0) count.* -= 1;
    }

    fn isRetained(self: *RunSnapshotRefRegistry, path: []const u8) bool {
        lockWorkerMutex(&self.mutex);
        defer self.mutex.unlock();
        return if (self.refs.get(path)) |count| count > 0 else false;
    }

    fn forget(self: *RunSnapshotRefRegistry, path: []const u8) void {
        lockWorkerMutex(&self.mutex);
        defer self.mutex.unlock();
        const count = self.refs.get(path) orelse return;
        if (count != 0) return;
        const removed = self.refs.fetchRemove(path) orelse return;
        std.heap.page_allocator.free(removed.key);
    }
};

var run_snapshot_refs = RunSnapshotRefRegistry{};

pub const MutableSnapshotReason = enum(u8) {
    bound_read_txn,
    namespace_read_txn,
    current_scan,
    other,
};

pub const mutable_snapshot_reason_count = @typeInfo(MutableSnapshotReason).@"enum".fields.len;

pub const ReaderPinKind = enum(u8) {
    bound_read_txn,
    namespace_read_txn,
    probe_txn,
    current_scan,
    write_txn,
    compaction,
    other,
};

pub const reader_pin_kind_count = @typeInfo(ReaderPinKind).@"enum".fields.len;

pub fn readerPinKindName(kind: ReaderPinKind) []const u8 {
    return switch (kind) {
        .bound_read_txn => "bound_read_txn",
        .namespace_read_txn => "namespace_read_txn",
        .probe_txn => "probe_txn",
        .current_scan => "current_scan",
        .write_txn => "write_txn",
        .compaction => "compaction",
        .other => "other",
    };
}

fn readerPinKindIndex(kind: ReaderPinKind) usize {
    return @intFromEnum(kind);
}

pub const MutableSnapshotCloneReasonStats = struct {
    calls: u64 = 0,
    bytes_total: u64 = 0,
    peak_bytes: u64 = 0,
};

const MutableSnapshotReaderRef = struct {
    state: *State,
    readers: usize,
};

pub const MaintenanceWaker = struct {
    ptr: *anyopaque,
    wake_fn: *const fn (ptr: *anyopaque) void,

    pub fn wake(self: MaintenanceWaker) void {
        self.wake_fn(self.ptr);
    }
};

pub fn mutableSnapshotReasonName(reason: MutableSnapshotReason) []const u8 {
    return switch (reason) {
        .bound_read_txn => "bound_read_txn",
        .namespace_read_txn => "namespace_read_txn",
        .current_scan => "current_scan",
        .other => "other",
    };
}

fn mutableSnapshotReasonIndex(reason: MutableSnapshotReason) usize {
    return @intFromEnum(reason);
}

fn activeReaderKindStats(active_readers_by_kind: [reader_pin_kind_count]usize) [reader_pin_kind_count]u64 {
    var stats: [reader_pin_kind_count]u64 = [_]u64{0} ** reader_pin_kind_count;
    for (active_readers_by_kind, 0..) |active, i| stats[i] = @intCast(active);
    return stats;
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
    // Rotate and flush a non-empty mutable table after this much write-idle
    // time once it reaches mutable_idle_flush_min_bytes. Zero disables idle
    // flushing. The deadline is renewed by each mutation, so sustained write
    // throughput still uses the normal size threshold.
    mutable_idle_flush_after_ns: u64 = 0,
    // Avoid turning low-rate writes into one tiny L0 run per idle interval.
    // Zero makes every non-empty mutable table eligible for the idle deadline.
    mutable_idle_flush_min_bytes: u64 = 0,
    // Upper bound on how long any non-empty mutable table remains WAL-only,
    // even when it never reaches mutable_idle_flush_min_bytes. Zero leaves the
    // maximum age unbounded.
    mutable_idle_flush_max_age_ns: u64 = 0,
    recovery_replay_flush_threshold: usize = 64 * 1024,
    bulk_ingest_flush_threshold_multiplier: usize = 8,
    bulk_ingest_flush_threshold_bytes_multiplier: usize = 8,
    compact_threshold_runs: usize = 4,
    l0_overlap_compact_threshold_runs: usize = 4,
    l0_soft_limit_runs: usize = 0,
    l0_hard_limit_runs: usize = 0,
    l0_soft_limit_bytes: u64 = 0,
    l0_hard_limit_bytes: u64 = 0,
    write_pressure_max_compaction_steps: usize = 8,
    write_pressure_reject_on_overload: bool = false,
    write_pressure_during_bulk_ingest: bool = false,
    foreground_soft_compaction: bool = false,
    defer_flush_on_commit: bool = false,
    max_deferred_immutable_memtables: usize = 8,
    // Bound the aggregate allocator-backed immutable queue independently of
    // its count. A count-only limit permits several memory-guard-sized
    // generations to accumulate when background flush falls behind.
    max_deferred_immutable_bytes: u64 = 0,
    background_maintenance_max_steps: usize = 64,
    direct_bulk_ingest: bool = true,
    level_target_runs_base: usize = 32,
    level_target_runs_multiplier: usize = 4,
    level_target_bytes_base: usize = 1024 * 1024,
    level_target_bytes_multiplier: usize = 8,
    max_compaction_input_bytes: u64 = 0,
    max_compaction_input_allow_oversized_single_job: bool = true,
    // Preferred logical payload target used to shape runs.
    max_run_file_bytes: usize = 512 * 1024 * 1024,
    // Hard physical publication bound. Keep this no larger than the reader
    // allocation cap; tests and embedded users may lower it independently of
    // the logical target to exercise encoded-size admission.
    max_run_file_physical_bytes: usize = 512 * 1024 * 1024,
    /// Finish a run when the namespace or the first N key bytes change. This
    /// only controls run layout; the persisted table-file format is unchanged.
    run_partition_prefix_bytes: usize = 0,
    bloom: bloom.Config = lsm_table_file.default_filter_config,
    table_block_compression: lsm_table_file.CompressionPolicy = .snappy_adaptive,
    table_prefix_extractor: lsm_table_file.PrefixExtractor = lsm_table_file.default_prefix_extractor,
    recovery_scratch_retained_cap_bytes: usize = wal_mod.default_replay_scratch_retained_cap_bytes,
    cursor_scratch_retained_cap_bytes: usize = 1 * 1024 * 1024,
    compaction_scratch_retained_cap_bytes: usize = 4 * 1024 * 1024,
    io_runtime: storage_io.RuntimeKind = .threaded,
    read_runtime: ?storage_io.ReadRuntime = null,
    storage: ?storage_io.Storage = null,
    cache: ?*cache_mod.Cache = null,
    local_block_cache_enabled: bool = true,
    max_concurrent_point_block_reads: usize = 4,
    resource_manager: ?*resource_manager_mod.ResourceManager = null,
    background_executor: ?*const BackgroundExecutor = null,
    maintenance_waker: ?MaintenanceWaker = null,
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
    // When background flush is unable to keep up, perform at most one
    // checkpoint-producing immutable flush per enforcement interval after the
    // soft WAL bound is crossed. This avoids accumulating to the hard bound
    // and then charging one large recovery-log drain to a foreground commit.
    foreground_soft_wal_checkpoint: bool = false,
    root_generation: u64 = 0,
    obsolete_retention_ns: u64 = 250 * std.time.ns_per_ms,
    obsolete_delete_retry_ns: u64 = 250 * std.time.ns_per_ms,
    read_snapshot_rotate_mutable_bytes: u64 = 256 * 1024,
    // Per-scan cap; 0 disables bulk current-scan mutable cloning.
    bulk_ingest_current_scan_clone_max_bytes: u64 = 256 * 1024 * 1024,
    // Aggregate active clone cap; 0 leaves aggregate admission uncapped.
    bulk_ingest_current_scan_clone_total_max_bytes: u64 = 256 * 1024 * 1024,
};

fn normalizeOptionsForDurability(options: Options) Options {
    var normalized = options;
    normalized.wal_sync_on_commit = normalized.wal_sync_on_commit or
        (!normalized.backend.read_only and normalized.backend.durability == .full);
    return normalized;
}

fn writePressureDuringBulkIngestEnvEnabled() bool {
    const raw = platform.env.getenv("ANTFLY_LSM_WRITE_PRESSURE_DURING_BULK") orelse return false;
    if (raw.len == 0) return true;
    if (std.ascii.eqlIgnoreCase(raw, "0")) return false;
    if (std.ascii.eqlIgnoreCase(raw, "false")) return false;
    if (std.ascii.eqlIgnoreCase(raw, "no")) return false;
    if (std.ascii.eqlIgnoreCase(raw, "off")) return false;
    return true;
}

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
pub const BackendHandleConfig = struct {
    background_runtime: ?background_runtime_mod.Config = null,
    internal_flush_worker: bool = false,
};
const max_local_cached_run_blocks: usize = 64;
const root_writer_lock_file_name = "writer.lock";
const wal_operation_lock_file_name = "wal.lock";

const RootLockState = struct {
    key: []u8,
    ref_count: usize = 1,
    writer_open: bool = false,
    wal_rwlock: ProcessRwLock = .{},
};

const ProcessRwLock = struct {
    reader_gate: std.atomic.Mutex = .unlocked,
    reader_mutex: std.atomic.Mutex = .unlocked,
    resource_mutex: std.atomic.Mutex = .unlocked,
    reader_count: usize = 0,

    fn lockShared(self: *ProcessRwLock) void {
        platform.sync.lockYielding(&self.reader_gate);
        defer self.reader_gate.unlock();

        platform.sync.lockYielding(&self.reader_mutex);
        defer self.reader_mutex.unlock();

        self.reader_count += 1;
        if (self.reader_count == 1) platform.sync.lockYielding(&self.resource_mutex);
    }

    fn tryLockShared(self: *ProcessRwLock) bool {
        if (!self.reader_gate.tryLock()) return false;
        defer self.reader_gate.unlock();

        if (!self.reader_mutex.tryLock()) return false;
        defer self.reader_mutex.unlock();

        if (self.reader_count == 0 and !self.resource_mutex.tryLock()) return false;
        self.reader_count += 1;
        return true;
    }

    fn unlockShared(self: *ProcessRwLock) void {
        platform.sync.lockYielding(&self.reader_mutex);
        defer self.reader_mutex.unlock();

        std.debug.assert(self.reader_count > 0);
        self.reader_count -= 1;
        if (self.reader_count == 0) self.resource_mutex.unlock();
    }

    fn lockExclusive(self: *ProcessRwLock) void {
        platform.sync.lockYielding(&self.reader_gate);
        platform.sync.lockYielding(&self.resource_mutex);
    }

    fn tryLockExclusive(self: *ProcessRwLock) bool {
        if (!self.reader_gate.tryLock()) return false;
        if (!self.resource_mutex.tryLock()) {
            self.reader_gate.unlock();
            return false;
        }
        return true;
    }

    fn unlockExclusive(self: *ProcessRwLock) void {
        self.resource_mutex.unlock();
        self.reader_gate.unlock();
    }
};

var root_lock_registry_mutex: std.atomic.Mutex = .unlocked;
var root_lock_registry: std.StringHashMapUnmanaged(*RootLockState) = .empty;

fn retainRootLockState(root_identity: []const u8) !*RootLockState {
    lockWorkerMutex(&root_lock_registry_mutex);
    defer root_lock_registry_mutex.unlock();

    if (root_lock_registry.get(root_identity)) |state| {
        state.ref_count += 1;
        return state;
    }

    const key = try std.heap.page_allocator.dupe(u8, root_identity);
    errdefer std.heap.page_allocator.free(key);
    const state = try std.heap.page_allocator.create(RootLockState);
    errdefer std.heap.page_allocator.destroy(state);
    state.* = .{
        .key = key,
    };
    try root_lock_registry.put(std.heap.page_allocator, state.key, state);
    return state;
}

fn releaseProcessRootLockState(state: *RootLockState) void {
    lockWorkerMutex(&root_lock_registry_mutex);
    defer root_lock_registry_mutex.unlock();

    std.debug.assert(state.ref_count > 0);
    state.ref_count -= 1;
    if (state.ref_count != 0) return;

    std.debug.assert(!state.writer_open);
    if (root_lock_registry.fetchRemove(state.key)) |entry| {
        std.heap.page_allocator.free(entry.key);
        std.heap.page_allocator.destroy(entry.value);
        if (root_lock_registry.count() == 0) {
            root_lock_registry.deinit(std.heap.page_allocator);
            root_lock_registry = .empty;
        }
    }
}

fn acquireProcessRootWriter(state: *RootLockState) !void {
    lockWorkerMutex(&root_lock_registry_mutex);
    defer root_lock_registry_mutex.unlock();

    if (state.writer_open) return error.LsmRootWriterAlreadyOpen;
    state.writer_open = true;
}

fn releaseProcessRootWriter(state: *RootLockState) void {
    lockWorkerMutex(&root_lock_registry_mutex);
    defer root_lock_registry_mutex.unlock();

    std.debug.assert(state.writer_open);
    state.writer_open = false;
}

fn rootLockPathAlloc(allocator: Allocator, root_dir: []const u8) ![]u8 {
    return try std.fs.path.join(allocator, &.{ root_dir, root_writer_lock_file_name });
}

fn walOperationLockPathAlloc(allocator: Allocator, root_dir: []const u8) ![]u8 {
    return try std.fs.path.join(allocator, &.{ root_dir, wal_operation_lock_file_name });
}

pub const Backend = struct {
    pub const OpenPhase = enum {
        idle,
        initializing_storage,
        cleaning_recovered_run_temps,
        opening_manifest,
        ensuring_dirs,
        replaying_wal,
        mounting_runs,
        ready,
        failed,
    };

    pub const OpenStats = struct {
        phase: OpenPhase = .idle,
        started: u64 = 0,
        completed: u64 = 0,
        failed: u64 = 0,
        total_ns: u64 = 0,
        initializing_storage_ns: u64 = 0,
        cleaning_recovered_run_temps_ns: u64 = 0,
        opening_manifest_ns: u64 = 0,
        ensuring_dirs_ns: u64 = 0,
        replaying_wal_ns: u64 = 0,
        mounting_runs_ns: u64 = 0,
        loaded_manifest: bool = false,
        loaded_runs: u64 = 0,
        obsolete_paths: u64 = 0,
        mutable_entries_after_replay: u64 = 0,
        immutable_memtables_after_replay: u64 = 0,
        wal_replay_records: u64 = 0,
        wal_replay_entries: u64 = 0,
        wal_replay_bytes: u64 = 0,
        wal_replay_ns: u64 = 0,
        wal_replay_truncated_tail_bytes: u64 = 0,
        recovered_table_temp_files_deleted: u64 = 0,
        recovered_table_temp_bytes_deleted: u64 = 0,
        recovered_table_temp_files_deleted_before_replay: u64 = 0,
        recovered_table_temp_bytes_deleted_before_replay: u64 = 0,
    };

    pub fn accumulateOpenStats(dst: *OpenStats, src: OpenStats) void {
        if (@intFromEnum(src.phase) > @intFromEnum(dst.phase)) dst.phase = src.phase;
        dst.started +|= src.started;
        dst.completed +|= src.completed;
        dst.failed +|= src.failed;
        dst.total_ns +|= src.total_ns;
        dst.initializing_storage_ns +|= src.initializing_storage_ns;
        dst.cleaning_recovered_run_temps_ns +|= src.cleaning_recovered_run_temps_ns;
        dst.opening_manifest_ns +|= src.opening_manifest_ns;
        dst.ensuring_dirs_ns +|= src.ensuring_dirs_ns;
        dst.replaying_wal_ns +|= src.replaying_wal_ns;
        dst.mounting_runs_ns +|= src.mounting_runs_ns;
        dst.loaded_manifest = dst.loaded_manifest or src.loaded_manifest;
        dst.loaded_runs +|= src.loaded_runs;
        dst.obsolete_paths +|= src.obsolete_paths;
        dst.mutable_entries_after_replay +|= src.mutable_entries_after_replay;
        dst.immutable_memtables_after_replay +|= src.immutable_memtables_after_replay;
        dst.wal_replay_records +|= src.wal_replay_records;
        dst.wal_replay_entries +|= src.wal_replay_entries;
        dst.wal_replay_bytes +|= src.wal_replay_bytes;
        dst.wal_replay_ns +|= src.wal_replay_ns;
        dst.wal_replay_truncated_tail_bytes +|= src.wal_replay_truncated_tail_bytes;
        dst.recovered_table_temp_files_deleted +|= src.recovered_table_temp_files_deleted;
        dst.recovered_table_temp_bytes_deleted +|= src.recovered_table_temp_bytes_deleted;
        dst.recovered_table_temp_files_deleted_before_replay +|= src.recovered_table_temp_files_deleted_before_replay;
        dst.recovered_table_temp_bytes_deleted_before_replay +|= src.recovered_table_temp_bytes_deleted_before_replay;
    }

    pub const RecoveredRunFileCleanupStats = struct {
        files_deleted: u64 = 0,
        bytes_deleted: u64 = 0,

        pub fn cleaned(self: @This()) bool {
            return self.files_deleted > 0;
        }
    };

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
        write_pressure_events: u64 = 0,
        write_pressure_compactions: u64 = 0,
        write_pressure_compaction_steps: u64 = 0,
        write_pressure_l0_run_debt: u64 = 0,
        write_pressure_l0_byte_debt: u64 = 0,
        write_pressure_overload_l0_run_debt: u64 = 0,
        write_pressure_overload_l0_byte_debt: u64 = 0,
        write_pressure_overloads: u64 = 0,
        write_pressure_rejections: u64 = 0,
        write_pressure_ns: u64 = 0,
        wal_pressure_flushes: u64 = 0,
        wal_pressure_ns: u64 = 0,
        wal_append_records: u64 = 0,
        wal_append_entries: u64 = 0,
        wal_append_bytes: u64 = 0,
        wal_append_ns: u64 = 0,
        wal_sync_records: u64 = 0,
        wal_sync_ns: u64 = 0,
        wal_segment_syncs: u64 = 0,
        wal_index_syncs: u64 = 0,
        wal_replay_records: u64 = 0,
        wal_replay_entries: u64 = 0,
        wal_replay_bytes: u64 = 0,
        wal_replay_ns: u64 = 0,
        wal_replay_truncated_tail_bytes: u64 = 0,
        wal_replay_recovery_flushes: u64 = 0,
        wal_replay_recovery_entry_bytes: u64 = 0,
        wal_replay_recovery_window_peak_bytes: u64 = 0,
        wal_replay_recovery_records_applied: u64 = 0,
        wal_replay_recovery_entries_applied: u64 = 0,
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
        bulk_ingest_current_scan_clone_active_bytes: u64 = 0,
        bulk_ingest_current_scan_clone_peak_active_bytes: u64 = 0,
        bulk_ingest_current_scan_clone_budget_denials: u64 = 0,
        bulk_ingest_current_scan_clone_oom_fallbacks: u64 = 0,
        read_snapshot_mutable_rotations: u64 = 0,
        read_snapshot_mutable_rotation_bytes_total: u64 = 0,
        read_snapshot_mutable_rotation_peak_bytes: u64 = 0,
        immutable_memtables: u64 = 0,
        immutable_entries: u64 = 0,
        immutable_bytes: u64 = 0,
        retired_immutable_memtables: u64 = 0,
        retired_immutable_entries: u64 = 0,
        retired_immutable_bytes: u64 = 0,
        immutable_pinned_generations: u64 = 0,
        immutable_pin_refs: u64 = 0,
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
        write_stall_l0_run_debt: u64 = 0,
        soft_limit_l0_bytes: u64 = 0,
        hard_limit_l0_bytes: u64 = 0,
        write_stall_l0_byte_debt: u64 = 0,
        level_overflow_runs: u64 = 0,
        level_overflow_bytes: u64 = 0,
        obsolete_paths: u64 = 0,
        current_manifest_bytes: u64 = 0,
        obsolete_paths_pinned_by_readers: u64 = 0,
        obsolete_paths_pinned_by_versions: u64 = 0,
        obsolete_paths_waiting_for_retry: u64 = 0,
        obsolete_paths_reclaimable: u64 = 0,
        obsolete_delete_failures: u64 = 0,
        obsolete_delete_retries: u64 = 0,
        active_readers: u64 = 0,
        active_readers_by_kind: [reader_pin_kind_count]u64 = [_]u64{0} ** reader_pin_kind_count,
        obsolete_paths_pinned_by_reader_kind: [reader_pin_kind_count]u64 = [_]u64{0} ** reader_pin_kind_count,
        active_bulk_ingest_batches: u64 = 0,
        wal_retained_segments: u64 = 0,
        wal_retained_bytes: u64 = 0,
        wal_checkpoint_oldest_retained_segment: u64 = 0,
        wal_checkpoint_covered_through_segment: u64 = 0,
        wal_checkpoint_current_segment: u64 = 0,
        wal_checkpoint_lag_segments: u64 = 0,
        wal_replay_retained_segments: u64 = 0,
        wal_replay_retained_bytes: u64 = 0,
        wal_replay_current_segment: u64 = 0,
        manifest_dirty: bool = false,
        obsolete_manifest_dirty: bool = false,
        compaction_scheduler_active_jobs: u64 = 0,
        compaction_scheduler_in_flight_input_bytes: u64 = 0,
        compaction_scheduler_active_oldest_age_ns: u64 = 0,
        compaction_scheduler_grants: u64 = 0,
        compaction_scheduler_completions: u64 = 0,
        compaction_scheduler_denied_capacity: u64 = 0,
        compaction_scheduler_denied_resource_pressure: u64 = 0,
        compaction_scheduler_oversized_grants: u64 = 0,
        compaction_scheduler_oversized_skips: u64 = 0,
        compaction_scheduler_remembered_candidates: u64 = 0,
        compaction_scheduler_remembered_retries: u64 = 0,
        compaction_scheduler_remembered_hits: u64 = 0,
        compaction_scheduler_remembered_stale: u64 = 0,
        compaction_scheduler_conflict_denials: u64 = 0,
        compaction_scheduler_remembered_pending: u64 = 0,
        compaction_scheduler_remembered_pending_runs: u64 = 0,
        compaction_scheduler_remembered_pending_bytes: u64 = 0,
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
        dst.bulk_ingest_current_scan_clone_active_bytes +|= src.bulk_ingest_current_scan_clone_active_bytes;
        dst.bulk_ingest_current_scan_clone_peak_active_bytes = @max(dst.bulk_ingest_current_scan_clone_peak_active_bytes, src.bulk_ingest_current_scan_clone_peak_active_bytes);
        dst.bulk_ingest_current_scan_clone_budget_denials +|= src.bulk_ingest_current_scan_clone_budget_denials;
        dst.bulk_ingest_current_scan_clone_oom_fallbacks +|= src.bulk_ingest_current_scan_clone_oom_fallbacks;
        dst.read_snapshot_mutable_rotations +|= src.read_snapshot_mutable_rotations;
        dst.read_snapshot_mutable_rotation_bytes_total +|= src.read_snapshot_mutable_rotation_bytes_total;
        dst.read_snapshot_mutable_rotation_peak_bytes = @max(dst.read_snapshot_mutable_rotation_peak_bytes, src.read_snapshot_mutable_rotation_peak_bytes);
        dst.immutable_memtables +|= src.immutable_memtables;
        dst.immutable_entries +|= src.immutable_entries;
        dst.immutable_bytes +|= src.immutable_bytes;
        dst.retired_immutable_memtables +|= src.retired_immutable_memtables;
        dst.retired_immutable_entries +|= src.retired_immutable_entries;
        dst.retired_immutable_bytes +|= src.retired_immutable_bytes;
        dst.immutable_pinned_generations +|= src.immutable_pinned_generations;
        dst.immutable_pin_refs +|= src.immutable_pin_refs;
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
        dst.write_stall_l0_run_debt +|= src.write_stall_l0_run_debt;
        dst.soft_limit_l0_bytes +|= src.soft_limit_l0_bytes;
        dst.hard_limit_l0_bytes +|= src.hard_limit_l0_bytes;
        dst.write_stall_l0_byte_debt +|= src.write_stall_l0_byte_debt;
        dst.level_overflow_runs +|= src.level_overflow_runs;
        dst.level_overflow_bytes +|= src.level_overflow_bytes;
        dst.obsolete_paths +|= src.obsolete_paths;
        dst.obsolete_paths_pinned_by_readers +|= src.obsolete_paths_pinned_by_readers;
        dst.obsolete_paths_pinned_by_versions +|= src.obsolete_paths_pinned_by_versions;
        dst.obsolete_paths_waiting_for_retry +|= src.obsolete_paths_waiting_for_retry;
        dst.obsolete_paths_reclaimable +|= src.obsolete_paths_reclaimable;
        dst.obsolete_delete_failures +|= src.obsolete_delete_failures;
        dst.obsolete_delete_retries +|= src.obsolete_delete_retries;
        dst.current_manifest_bytes +|= src.current_manifest_bytes;
        dst.active_readers +|= src.active_readers;
        for (&dst.active_readers_by_kind, src.active_readers_by_kind) |*dst_count, src_count| dst_count.* +|= src_count;
        for (&dst.obsolete_paths_pinned_by_reader_kind, src.obsolete_paths_pinned_by_reader_kind) |*dst_count, src_count| dst_count.* +|= src_count;
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
        dst.wal_checkpoint_covered_through_segment = @max(dst.wal_checkpoint_covered_through_segment, src.wal_checkpoint_covered_through_segment);
        dst.wal_checkpoint_lag_segments +|= src.wal_checkpoint_lag_segments;
        dst.wal_replay_retained_segments +|= src.wal_replay_retained_segments;
        dst.wal_replay_retained_bytes +|= src.wal_replay_retained_bytes;
        dst.wal_replay_current_segment = @max(dst.wal_replay_current_segment, src.wal_replay_current_segment);
        dst.manifest_dirty = dst.manifest_dirty or src.manifest_dirty;
        dst.obsolete_manifest_dirty = dst.obsolete_manifest_dirty or src.obsolete_manifest_dirty;
        dst.compaction_scheduler_active_jobs +|= src.compaction_scheduler_active_jobs;
        dst.compaction_scheduler_in_flight_input_bytes +|= src.compaction_scheduler_in_flight_input_bytes;
        dst.compaction_scheduler_active_oldest_age_ns = @max(dst.compaction_scheduler_active_oldest_age_ns, src.compaction_scheduler_active_oldest_age_ns);
        dst.compaction_scheduler_grants +|= src.compaction_scheduler_grants;
        dst.compaction_scheduler_completions +|= src.compaction_scheduler_completions;
        dst.compaction_scheduler_denied_capacity +|= src.compaction_scheduler_denied_capacity;
        dst.compaction_scheduler_denied_resource_pressure +|= src.compaction_scheduler_denied_resource_pressure;
        dst.compaction_scheduler_oversized_grants +|= src.compaction_scheduler_oversized_grants;
        dst.compaction_scheduler_oversized_skips +|= src.compaction_scheduler_oversized_skips;
        dst.compaction_scheduler_remembered_candidates +|= src.compaction_scheduler_remembered_candidates;
        dst.compaction_scheduler_remembered_retries +|= src.compaction_scheduler_remembered_retries;
        dst.compaction_scheduler_remembered_hits +|= src.compaction_scheduler_remembered_hits;
        dst.compaction_scheduler_remembered_stale +|= src.compaction_scheduler_remembered_stale;
        dst.compaction_scheduler_conflict_denials +|= src.compaction_scheduler_conflict_denials;
        dst.compaction_scheduler_remembered_pending +|= src.compaction_scheduler_remembered_pending;
        dst.compaction_scheduler_remembered_pending_runs +|= src.compaction_scheduler_remembered_pending_runs;
        dst.compaction_scheduler_remembered_pending_bytes +|= src.compaction_scheduler_remembered_pending_bytes;
        dst.background_io_budget_bytes +|= src.background_io_budget_bytes;
        dst.background_io_reserved_bytes +|= src.background_io_reserved_bytes;
        dst.background_io_denied_jobs +|= src.background_io_denied_jobs;
        dst.background_io_oversized_jobs +|= src.background_io_oversized_jobs;
        dst.backend_lock_waits +|= src.backend_lock_waits;
        dst.backend_lock_wait_ns +|= src.backend_lock_wait_ns;
        dst.backend_lock_max_wait_ns = @max(dst.backend_lock_max_wait_ns, src.backend_lock_max_wait_ns);
    }

    pub fn accumulateWriteStats(dst: *WriteStats, src: WriteStats) void {
        inline for (@typeInfo(WriteStats).@"struct".fields) |field| {
            if (comptime std.mem.eql(u8, field.name, "table_file_compression_codec_mask")) {
                @field(dst, field.name) |= @field(src, field.name);
            } else {
                @field(dst, field.name) +|= @field(src, field.name);
            }
        }
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
        point_run_prechecks: u64 = 0,
        point_run_precheck_survivors: u64 = 0,
        point_run_survivor_reads: u64 = 0,
        point_run_survivor_hits: u64 = 0,
        point_run_survivor_misses: u64 = 0,
        point_run_survivor_tombstones: u64 = 0,
        point_run_async_batches: u64 = 0,
        point_run_async_reads_issued: u64 = 0,
        point_run_async_reads_canceled: u64 = 0,
        point_run_async_wait_ns: u64 = 0,
        bloom_negatives: u64 = 0,
        prefix_bloom_negatives: u64 = 0,
        block_prefix_bloom_negatives: u64 = 0,
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
        point_run_prechecks: CounterU64 = .init(0),
        point_run_precheck_survivors: CounterU64 = .init(0),
        point_run_survivor_reads: CounterU64 = .init(0),
        point_run_survivor_hits: CounterU64 = .init(0),
        point_run_survivor_misses: CounterU64 = .init(0),
        point_run_survivor_tombstones: CounterU64 = .init(0),
        point_run_async_batches: CounterU64 = .init(0),
        point_run_async_reads_issued: CounterU64 = .init(0),
        point_run_async_reads_canceled: CounterU64 = .init(0),
        point_run_async_wait_ns: CounterU64 = .init(0),
        bloom_negatives: CounterU64 = .init(0),
        prefix_bloom_negatives: CounterU64 = .init(0),
        block_prefix_bloom_negatives: CounterU64 = .init(0),
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
                .point_run_prechecks = self.point_run_prechecks.load(.monotonic),
                .point_run_precheck_survivors = self.point_run_precheck_survivors.load(.monotonic),
                .point_run_survivor_reads = self.point_run_survivor_reads.load(.monotonic),
                .point_run_survivor_hits = self.point_run_survivor_hits.load(.monotonic),
                .point_run_survivor_misses = self.point_run_survivor_misses.load(.monotonic),
                .point_run_survivor_tombstones = self.point_run_survivor_tombstones.load(.monotonic),
                .point_run_async_batches = self.point_run_async_batches.load(.monotonic),
                .point_run_async_reads_issued = self.point_run_async_reads_issued.load(.monotonic),
                .point_run_async_reads_canceled = self.point_run_async_reads_canceled.load(.monotonic),
                .point_run_async_wait_ns = self.point_run_async_wait_ns.load(.monotonic),
                .bloom_negatives = self.bloom_negatives.load(.monotonic),
                .prefix_bloom_negatives = self.prefix_bloom_negatives.load(.monotonic),
                .block_prefix_bloom_negatives = self.block_prefix_bloom_negatives.load(.monotonic),
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

    // Keep all derived WAL accounting in one owner so mutation paths cannot
    // accidentally update only half of the cached state. All Backend calls
    // that mutate retained WAL belong on this type (rather than calling
    // wal_mod directly). Appends advance the primary snapshot in O(1);
    // destructive operations invalidate or replace the affected snapshot
    // before exposing their result. The cache-coherence test below is the
    // oracle for any new mutation method.
    const WalRetentionState = struct {
        primary: ?wal_mod.RetentionStats = null,
        primary_ns: u64 = 0,
        replay: ?wal_mod.RetentionStats = null,
        replay_ns: u64 = 0,

        fn append(
            self: *@This(),
            storage: storage_io.Storage,
            allocator: Allocator,
            root_dir: []const u8,
            state: anytype,
            sync_on_commit: bool,
            options: wal_mod.AppendOptions,
            now_ns: u64,
        ) !wal_mod.AppendResult {
            errdefer self.invalidatePrimary();
            const result = try wal_mod.appendStateWithOptionsResult(
                storage,
                allocator,
                root_dir,
                state,
                sync_on_commit,
                options,
            );
            self.noteAppend(result, now_ns);
            return result;
        }

        fn retireCoveredSegments(
            self: *@This(),
            storage: storage_io.Storage,
            allocator: Allocator,
            root_dir: []const u8,
            covered_through: u64,
        ) !void {
            // The operation updates both segment files and the checkpoint
            // index. Invalidate first so a partial failure cannot leave a
            // seemingly authoritative pre-retirement snapshot.
            self.invalidatePrimary();
            try wal_mod.retireCoveredSegments(storage, allocator, root_dir, covered_through);
        }

        fn reset(
            self: *@This(),
            storage: storage_io.Storage,
            allocator: Allocator,
            root_dir: []const u8,
            now_ns: u64,
        ) !void {
            // reset rewrites primary and replay indexes and removes segments.
            // Publish the known empty state only after every write succeeds.
            self.invalidateAll();
            try wal_mod.reset(storage, allocator, root_dir);
            self.installReset(now_ns);
        }

        fn noteAppend(self: *@This(), result: wal_mod.AppendResult, now_ns: u64) void {
            if (result.bytes == 0 or result.segment == 0) return;
            if (self.primary) |*stats| {
                if (stats.current_segment == 0) {
                    stats.oldest_retained_segment = 1;
                }
                stats.current_segment = result.segment;
                stats.bytes +|= @intCast(result.bytes);
                if (result.segment_became_nonempty) stats.segments +|= 1;
                self.primary_ns = now_ns;
            }
        }

        fn invalidatePrimary(self: *@This()) void {
            self.primary = null;
            self.primary_ns = 0;
        }

        fn invalidateAll(self: *@This()) void {
            self.* = .{};
        }

        fn installReset(self: *@This(), now_ns: u64) void {
            self.primary = .{
                .oldest_retained_segment = 1,
                .current_segment = 1,
            };
            self.primary_ns = now_ns;
            self.replay = .{ .current_segment = 1 };
            self.replay_ns = now_ns;
        }
    };

    const L0OverlapCache = struct {
        const max_run_ids = compaction_mod.max_exact_l0_overlap_runs;

        valid: bool = false,
        threshold: usize = 0,
        run_count: usize = 0,
        run_ids: [max_run_ids]u64 = @splat(0),
        result: usize = 0,

        fn lookup(self: *const @This(), runs: []const Run, threshold: usize) ?usize {
            if (!self.valid or self.threshold != threshold or self.run_count != runs.len) return null;
            if (runs.len > max_run_ids) return null;
            for (runs, self.run_ids[0..runs.len]) |run, cached_id| {
                if (run.id != cached_id) return null;
            }
            return self.result;
        }

        fn store(self: *@This(), runs: []const Run, threshold: usize, result: usize) void {
            if (runs.len > max_run_ids) {
                self.valid = false;
                return;
            }
            self.valid = true;
            self.threshold = threshold;
            self.run_count = runs.len;
            for (runs, self.run_ids[0..runs.len]) |run, *cached_id| cached_id.* = run.id;
            self.result = result;
        }
    };

    allocator: Allocator,
    mu: std.atomic.Mutex = .unlocked,
    // Cached score used by best-effort maintenance scheduling and metrics.
    // A value of 1 can still mean "known debt, exact score not refreshed yet".
    cached_maintenance_hint: CounterU64 = .init(1),
    // Exact L0 overlap scoring is quadratic in the number of L0 runs. Runtime
    // status and ResourceManager metrics can ask for the same score many times
    // while the immutable run set is unchanged, so retain the exact result for
    // the normal bounded-L0 case. Run IDs uniquely identify immutable metadata;
    // comparing the complete sequence avoids stale heuristic scheduling.
    l0_overlap_cache: L0OverlapCache = .{},
    options: Options,
    root_generation: u64 = 0,
    root_dir: ?[]u8 = null,
    root_lock_state: ?*RootLockState = null,
    root_writer_registered: bool = false,
    root_writer_lock: ?storage_io.NativePathLock = null,
    wal_operation_lock_file: ?storage_io.NativePathLockFile = null,
    storage_owner: ?*storage_io.NativeStorage = null,
    storage: ?storage_io.Storage = null,
    manifest_backing: ?[]u8 = null,
    current_manifest_bytes: u64 = 0,
    next_run_id: u64 = 1,
    active_readers: usize = 0,
    active_readers_by_kind: [reader_pin_kind_count]usize = [_]usize{0} ** reader_pin_kind_count,
    manifest_dirty: bool = false,
    obsolete_paths: std.ArrayListUnmanaged(ObsoletePath) = .empty,
    obsolete_manifest_dirty: bool = false,
    obsolete_delete_failures: u64 = 0,
    obsolete_delete_retries: u64 = 0,
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
    immutable_flush_completion: ImmutableFlushCompletion = .{},
    maintenance_job_in_flight: bool = false,
    maintenance_io_budget_remaining: ?u64 = null,
    background_io_reserved_bytes: u64 = 0,
    background_io_denied_jobs: u64 = 0,
    background_io_oversized_jobs: u64 = 0,
    write_pressure_enforcing: bool = false,
    last_wal_retention_enforce_ns: u64 = 0,
    wal_retention: WalRetentionState = .{},
    remembered_compaction: ?compaction_mod.RememberedCompaction = null,
    open_stats: OpenStats = .{},
    write_stats: WriteStats = .{},
    read_stats: AtomicReadStats = .{},
    tracked_in_memory_state_bytes: u64 = 0,
    tracked_wal_retention_bytes: u64 = 0,
    tracked_recovery_working_set_bytes: u64 = 0,
    backend_lock_waits: CounterU64 = .init(0),
    backend_lock_wait_ns: CounterU64 = .init(0),
    backend_lock_max_wait_ns: CounterU64 = .init(0),
    mutable_snapshot_clone_calls: u64 = 0,
    mutable_snapshot_clone_bytes_total: u64 = 0,
    mutable_snapshot_clone_peak_bytes: u64 = 0,
    mutable_snapshot_clone_by_reason: [mutable_snapshot_reason_count]MutableSnapshotCloneReasonStats = [_]MutableSnapshotCloneReasonStats{.{}} ** mutable_snapshot_reason_count,
    bulk_ingest_current_scan_clone_active_bytes: u64 = 0,
    bulk_ingest_current_scan_clone_peak_active_bytes: u64 = 0,
    bulk_ingest_current_scan_clone_budget_denials: u64 = 0,
    bulk_ingest_current_scan_clone_oom_fallbacks: u64 = 0,
    read_snapshot_mutable_rotations: u64 = 0,
    read_snapshot_mutable_rotation_bytes_total: u64 = 0,
    read_snapshot_mutable_rotation_peak_bytes: u64 = 0,
    active_bulk_ingest_batches: usize = 0,
    mutable: ActiveMemTable = .{},
    mutable_idle_flush_deadline_ns: u64 = 0,
    mutable_idle_flush_max_deadline_ns: u64 = 0,
    mutable_wal_range: WalSegmentRange = .{},
    empty_mutable_snapshot: State = .{},
    mutable_read_snapshot: ?*State = null,
    mutable_snapshot_reader_refs: std.ArrayListUnmanaged(MutableSnapshotReaderRef) = .empty,
    mutable_snapshot_reader_ref_by_state: std.AutoHashMapUnmanaged(*const State, usize) = .empty,
    immutable_memtables: std.ArrayListUnmanaged(*State) = .empty,
    immutable_wal_ranges: std.ArrayListUnmanaged(WalSegmentRange) = .empty,
    immutable_head: usize = 0,
    /// Read transactions pin the exact immutable generations they borrowed.
    /// A backend-wide reader count is too coarse: overlapping scans can keep
    /// that count non-zero forever and retain every flushed generation.
    immutable_memtable_pins: std.ArrayListUnmanaged(ImmutableMemtablePin) = .empty,
    retired_immutable_memtables: std.ArrayListUnmanaged(*State) = .empty,
    retired_mutable_snapshots: std.ArrayListUnmanaged(*State) = .empty,
    retired_mutable_snapshot_by_state: std.AutoHashMapUnmanaged(*const State, usize) = .empty,
    closing: std.atomic.Value(bool) = .init(false),
    recovery_replaying_wal: bool = false,
    runs: std.ArrayListUnmanaged(repository_mod.Run) = .empty,

    const BoundStore = runtime_mod.BoundStore(Backend);
    const BoundReadTxn = runtime_mod.BoundReadTxn(Backend);
    const BoundWriteTxn = runtime_mod.BoundWriteTxn(Backend);
    const NamespaceReadTxn = runtime_mod.NamespaceReadTxn(Backend);
    const NamespaceWriteTxn = runtime_mod.NamespaceWriteTxn(Backend);
    pub const BulkIngestFinishOptions = backend_types.BulkIngestFinishOptions;

    const ImmutableMemtablePin = struct {
        state: *const State,
        readers: usize,
    };

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
        const normalized_options = normalizeOptionsForDurability(options);
        return try recovery_mod.open(Backend, allocator, root_dir, normalized_options.backend, normalized_options);
    }

    pub fn openInto(self: *Backend, allocator: Allocator, root_dir: []const u8, options: Options) !void {
        const normalized_options = normalizeOptionsForDurability(options);
        try recovery_mod.openInto(Backend, self, allocator, root_dir, normalized_options.backend, normalized_options);
    }

    pub fn close(self: *Backend) void {
        self.closing.store(true, .release);
        self.background_executor.drain();
        self.waitForGenerationReadersToDrain();
        self.releaseTrackedResourceUsage();
        recovery_mod.close(Backend, self);
    }

    pub fn abandonAfterCrash(self: *Backend) void {
        self.closing.store(true, .release);
        self.background_executor.drain();
        self.releaseTrackedResourceUsage();
        recovery_mod.abandon(Backend, self);
    }

    fn waitForGenerationReadersToDrain(self: *Backend) void {
        while (true) {
            const locked = runtime_mod.lockBackend(Backend, self);
            const active_readers = self.active_readers;
            runtime_mod.unlockBackend(Backend, self, locked);
            if (active_readers == 0) return;
            platform.time.yieldBriefly();
        }
    }

    pub fn acquireRootLockState(self: *Backend, create_if_missing: bool) !void {
        if (self.root_dir == null or self.root_lock_state != null) return;

        const root_dir = self.root_dir.?;
        if (create_if_missing) try self.storage.?.createDirPath(root_dir);
        const identity = try self.storage.?.rootIdentityAlloc(self.allocator, root_dir);
        defer self.allocator.free(identity);
        self.root_lock_state = try retainRootLockState(identity);
    }

    pub fn releaseRootLockState(self: *Backend) void {
        if (self.root_lock_state) |state| {
            releaseProcessRootLockState(state);
            self.root_lock_state = null;
        }
    }

    pub fn acquireRootWriterLock(self: *Backend) !void {
        if (self.options.backend.read_only or self.root_dir == null) return;
        if (self.root_writer_registered) return;

        const root_dir = self.root_dir.?;
        const root_state = self.root_lock_state orelse return error.LsmRootWriterLockStateMissing;
        try acquireProcessRootWriter(root_state);
        errdefer releaseProcessRootWriter(root_state);

        var native_lock: ?storage_io.NativePathLock = null;
        errdefer if (native_lock) |*lock| lock.release();

        if (self.storage.?.supportsNativePathLocks()) {
            const lock_path = try rootLockPathAlloc(self.allocator, root_dir);
            defer self.allocator.free(lock_path);
            native_lock = storage_io.acquireNativePathLock(
                self.allocator,
                lock_path,
                .exclusive,
                .{ .nonblocking = true },
            ) catch |err| switch (err) {
                error.WouldBlock => return error.LsmRootWriterAlreadyOpen,
                else => return err,
            };
        }

        self.root_writer_registered = true;
        self.root_writer_lock = native_lock;
        native_lock = null;
    }

    pub fn releaseRootWriterLock(self: *Backend) void {
        if (self.root_writer_lock) |*lock| {
            lock.release();
            self.root_writer_lock = null;
        }
        if (self.root_writer_registered) {
            if (self.root_lock_state) |state| releaseProcessRootWriter(state);
            self.root_writer_registered = false;
        }
    }

    pub fn prepareWalOperationLockFile(self: *Backend) !void {
        if (!self.options.wal_enabled or self.root_dir == null or !self.storage.?.supportsNativePathLocks()) return;
        if (self.wal_operation_lock_file != null) return;

        const lock_path = try walOperationLockPathAlloc(self.allocator, self.root_dir.?);
        defer self.allocator.free(lock_path);
        self.wal_operation_lock_file = storage_io.openNativePathLockFile(self.allocator, lock_path, .{
            .create_if_missing = true,
        }) catch |err| switch (err) {
            error.FileNotFound => if (self.options.backend.read_only) return else return err,
            else => return err,
        };
    }

    pub fn closeWalOperationLockFile(self: *Backend) void {
        if (self.wal_operation_lock_file) |*lock_file| {
            lock_file.close();
            self.wal_operation_lock_file = null;
        }
    }

    const WalOperationLock = struct {
        backend: *Backend,
        process_lock_mode: ?storage_io.NativePathLockMode = null,
        native_locked: bool = false,

        fn release(self: *WalOperationLock) void {
            if (self.native_locked) {
                self.backend.wal_operation_lock_file.?.unlock();
                self.native_locked = false;
            }
            if (self.process_lock_mode) |mode| {
                const state = self.backend.root_lock_state.?;
                switch (mode) {
                    .shared => state.wal_rwlock.unlockShared(),
                    .exclusive => state.wal_rwlock.unlockExclusive(),
                }
                self.process_lock_mode = null;
            }
        }
    };

    fn acquireWalOperationLock(self: *Backend, mode: storage_io.NativePathLockMode) !WalOperationLock {
        var guard = WalOperationLock{ .backend = self };
        if (!self.options.wal_enabled or self.root_dir == null) return guard;

        if (self.root_lock_state) |state| {
            switch (mode) {
                .shared => state.wal_rwlock.lockShared(),
                .exclusive => state.wal_rwlock.lockExclusive(),
            }
            guard.process_lock_mode = mode;
        }
        errdefer guard.release();

        if (self.storage.?.supportsNativePathLocks()) {
            if (self.wal_operation_lock_file == null) try self.prepareWalOperationLockFile();
            if (self.wal_operation_lock_file) |*lock_file| {
                try lock_file.lock(mode);
                guard.native_locked = true;
            }
        }
        return guard;
    }

    fn tryAcquireWalOperationLock(self: *Backend, mode: storage_io.NativePathLockMode) !?WalOperationLock {
        var guard = WalOperationLock{ .backend = self };
        if (!self.options.wal_enabled or self.root_dir == null) return guard;

        if (self.root_lock_state) |state| {
            const process_locked = switch (mode) {
                .shared => state.wal_rwlock.tryLockShared(),
                .exclusive => state.wal_rwlock.tryLockExclusive(),
            };
            if (!process_locked) return null;
            guard.process_lock_mode = mode;
        }
        errdefer guard.release();

        if (self.storage.?.supportsNativePathLocks()) {
            if (self.wal_operation_lock_file == null) try self.prepareWalOperationLockFile();
            if (self.wal_operation_lock_file) |*lock_file| {
                if (!try lock_file.tryLock(mode)) {
                    guard.release();
                    return null;
                }
                guard.native_locked = true;
            }
        }
        return guard;
    }

    fn resolveBackgroundExecutor(configured: ?*const BackgroundExecutor) BackgroundExecutor {
        return if (configured) |executor| executor.* else BackgroundExecutor.initInline(0);
    }

    pub fn sync(self: *Backend, force: bool) !void {
        if (self.root_dir == null) return;
        const locked = runtime_mod.lockBackend(Backend, self);
        defer runtime_mod.unlockBackend(Backend, self, locked);
        if (force and !self.options.backend.read_only and self.options.wal_enabled) {
            var wal_lock = try self.acquireWalOperationLock(.exclusive);
            defer wal_lock.release();
            _ = try wal_mod.syncCurrentState(self.storage.?, self.allocator, self.root_dir.?);
        }
        try self.finalizeDeferredStorageWorkLocked();
    }

    pub fn syncReplayState(self: *Backend) !void {
        _ = try self.syncReplayStateWithStats();
    }

    pub fn syncReplayStateWithStats(self: *Backend) !wal_mod.SyncResult {
        if (self.root_dir == null) return .{};
        const locked = runtime_mod.lockBackend(Backend, self);
        defer runtime_mod.unlockBackend(Backend, self, locked);
        if (!self.options.backend.read_only and self.options.wal_enabled) {
            var wal_lock = try self.acquireWalOperationLock(.exclusive);
            defer wal_lock.release();
            return try wal_mod.syncCurrentState(self.storage.?, self.allocator, self.root_dir.?);
        }
        try self.finalizeDeferredStorageWorkLocked();
        return .{};
    }

    pub fn commitProvidesDurability(self: *const Backend) bool {
        return self.root_dir != null and
            !self.options.backend.read_only and
            self.options.wal_enabled and
            self.options.wal_sync_on_commit;
    }

    pub fn snapshotReadStats(self: *const Backend) ReadStats {
        return self.read_stats.snapshot();
    }

    pub fn snapshotWriteStats(self: *const Backend) WriteStats {
        const mutable = @constCast(self);
        const locked = runtime_mod.lockBackend(Backend, mutable);
        defer runtime_mod.unlockBackend(Backend, mutable, locked);
        return self.write_stats;
    }

    pub fn snapshotOpenStats(self: *const Backend) OpenStats {
        return self.open_stats;
    }

    pub fn openStatsNowNs(_: *Backend) u64 {
        return platform_time.monotonicNs();
    }

    pub fn beginOpenPhase(self: *Backend, phase: OpenPhase) u64 {
        const now = self.openStatsNowNs();
        if (self.open_stats.started == 0) {
            self.open_stats.started = 1;
            self.open_stats.total_ns = now;
        }
        self.open_stats.phase = phase;
        return now;
    }

    pub fn finishOpenPhase(self: *Backend, phase: OpenPhase, start_ns: u64) void {
        const elapsed = self.openStatsElapsedNs(start_ns);
        switch (phase) {
            .initializing_storage => self.open_stats.initializing_storage_ns +|= elapsed,
            .cleaning_recovered_run_temps => self.open_stats.cleaning_recovered_run_temps_ns +|= elapsed,
            .opening_manifest => self.open_stats.opening_manifest_ns +|= elapsed,
            .ensuring_dirs => self.open_stats.ensuring_dirs_ns +|= elapsed,
            .replaying_wal => self.open_stats.replaying_wal_ns +|= elapsed,
            .mounting_runs => self.open_stats.mounting_runs_ns +|= elapsed,
            .idle, .ready, .failed => {},
        }
    }

    pub fn recordRecoveredRunFileCleanup(self: *Backend, stats: RecoveredRunFileCleanupStats, before_wal_replay: bool) void {
        self.open_stats.recovered_table_temp_files_deleted +|= stats.files_deleted;
        self.open_stats.recovered_table_temp_bytes_deleted +|= stats.bytes_deleted;
        if (before_wal_replay) {
            self.open_stats.recovered_table_temp_files_deleted_before_replay +|= stats.files_deleted;
            self.open_stats.recovered_table_temp_bytes_deleted_before_replay +|= stats.bytes_deleted;
        }
    }

    pub fn recordOpenManifestLoaded(self: *Backend, loaded_manifest: bool) void {
        self.open_stats.loaded_manifest = loaded_manifest;
        self.open_stats.loaded_runs = @intCast(self.runs.items.len);
        self.open_stats.obsolete_paths = @intCast(self.obsolete_paths.items.len);
        self.current_manifest_bytes = if (self.manifest_backing) |raw| @intCast(raw.len) else 0;
    }

    pub fn recordOpenReplayComplete(self: *Backend) void {
        self.open_stats.mutable_entries_after_replay = @intCast(self.mutable.entries.items.len);
        self.open_stats.immutable_memtables_after_replay = @intCast(self.activeImmutableMemtableCount());
        self.open_stats.wal_replay_records = self.write_stats.wal_replay_records;
        self.open_stats.wal_replay_entries = self.write_stats.wal_replay_entries;
        self.open_stats.wal_replay_bytes = self.write_stats.wal_replay_bytes;
        self.open_stats.wal_replay_ns = self.write_stats.wal_replay_ns;
        self.open_stats.wal_replay_truncated_tail_bytes = self.write_stats.wal_replay_truncated_tail_bytes;
    }

    pub fn finishOpenSuccess(self: *Backend) void {
        self.open_stats.phase = .ready;
        self.open_stats.completed = 1;
        self.open_stats.failed = 0;
        self.open_stats.total_ns = self.openStatsElapsedNs(self.open_stats.total_ns);
    }

    pub fn finishOpenFailure(self: *Backend) void {
        self.open_stats.phase = .failed;
        self.open_stats.failed = 1;
        if (self.open_stats.total_ns != 0) {
            self.open_stats.total_ns = self.openStatsElapsedNs(self.open_stats.total_ns);
        }
    }

    fn openStatsElapsedNs(self: *Backend, start_ns: u64) u64 {
        const end_ns = self.openStatsNowNs();
        return if (end_ns >= start_ns) end_ns - start_ns else 0;
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
            .bulk_ingest_current_scan_clone_active_bytes = self.bulk_ingest_current_scan_clone_active_bytes,
            .bulk_ingest_current_scan_clone_peak_active_bytes = self.bulk_ingest_current_scan_clone_peak_active_bytes,
            .bulk_ingest_current_scan_clone_budget_denials = self.bulk_ingest_current_scan_clone_budget_denials,
            .bulk_ingest_current_scan_clone_oom_fallbacks = self.bulk_ingest_current_scan_clone_oom_fallbacks,
            .read_snapshot_mutable_rotations = self.read_snapshot_mutable_rotations,
            .read_snapshot_mutable_rotation_bytes_total = self.read_snapshot_mutable_rotation_bytes_total,
            .read_snapshot_mutable_rotation_peak_bytes = self.read_snapshot_mutable_rotation_peak_bytes,
            .immutable_memtables = @intCast(self.activeImmutableMemtableCount()),
            .retired_immutable_memtables = @intCast(self.retired_immutable_memtables.items.len),
            .immutable_pinned_generations = @intCast(self.immutable_memtable_pins.items.len),
            .total_runs = @intCast(self.runs.items.len),
            .obsolete_paths = @intCast(self.obsolete_paths.items.len),
            .current_manifest_bytes = self.current_manifest_bytes,
            .active_readers = (self.active_readers),
            .active_readers_by_kind = activeReaderKindStats(self.active_readers_by_kind),
            .active_bulk_ingest_batches = @intCast(self.active_bulk_ingest_batches),
            .soft_limit_l0_runs = @intCast(self.effectiveL0SoftLimitRuns()),
            .hard_limit_l0_runs = @intCast(self.effectiveL0HardLimitRuns()),
            .soft_limit_l0_bytes = self.options.l0_soft_limit_bytes,
            .hard_limit_l0_bytes = self.options.l0_hard_limit_bytes,
            .manifest_dirty = self.manifest_dirty,
            .obsolete_manifest_dirty = self.obsolete_manifest_dirty,
            .obsolete_delete_failures = self.obsolete_delete_failures,
            .obsolete_delete_retries = self.obsolete_delete_retries,
        };
        const obsolete_now_ns = self.nowNs();
        for (self.obsolete_paths.items) |obsolete| {
            if (run_snapshot_refs.isRetained(obsolete.path)) {
                stats.obsolete_paths_pinned_by_readers +|= 1;
                for (self.active_readers_by_kind, 0..) |active, kind_index| {
                    if (active != 0) stats.obsolete_paths_pinned_by_reader_kind[kind_index] +|= 1;
                }
            } else if (self.pathTrackedByActiveRunsLocked(obsolete.path) or self.obsoletePathPinnedByOpenVersion(obsolete.path)) {
                stats.obsolete_paths_pinned_by_versions +|= 1;
            } else if (obsolete.delete_after_ns > obsolete_now_ns) {
                stats.obsolete_paths_waiting_for_retry +|= 1;
            } else {
                stats.obsolete_paths_reclaimable +|= 1;
            }
        }
        for (self.activeImmutableMemtables()) |state| {
            stats.immutable_entries += @intCast(state.entries.items.len);
            stats.immutable_bytes += estimateStateBytes(state);
        }
        for (self.retired_immutable_memtables.items) |state| {
            stats.retired_immutable_entries +|= @intCast(state.entries.items.len);
            stats.retired_immutable_bytes +|= estimateStateBytes(state);
        }
        for (self.immutable_memtable_pins.items) |pin| {
            stats.immutable_pin_refs +|= @intCast(pin.readers);
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
                stats.write_stall_l0_run_debt = self.l0RunDebtForHardLimit(level_len, self.effectiveL0HardLimitRuns());
                stats.write_stall_l0_byte_debt = self.l0ByteDebtForHardLimit(level_bytes, self.options.l0_hard_limit_bytes);
                if (level_len > self.options.compact_threshold_runs) {
                    stats.compactable_l0_runs = @intCast(level_len - self.options.compact_threshold_runs);
                }
                stats.overlapping_l0_runs = @intCast(self.largestL0OverlapRunCountLocked());
            } else {
                stats.lower_level_runs += @intCast(level_len);
                stats.lower_level_bytes += level_bytes;
                const target_runs = maintenanceLevelRunTarget(level, self.options.level_target_runs_base, self.options.level_target_runs_multiplier);
                if (level_len > target_runs) {
                    stats.level_overflow_runs += @intCast(level_len - target_runs);
                }
                const target_bytes = compaction_mod.levelByteTargetForRuns(self.runs.items, level, self.options.level_target_bytes_base, self.options.level_target_bytes_multiplier);
                if (target_bytes > 0 and level_bytes > target_bytes) {
                    stats.level_overflow_bytes += level_bytes - target_bytes;
                }
            }
        }

        const scheduler_stats = self.compaction_scheduler.snapshotAt(platform_time.monotonicNs());
        stats.compaction_scheduler_active_jobs = scheduler_stats.active_jobs;
        stats.compaction_scheduler_in_flight_input_bytes = scheduler_stats.in_flight_input_bytes;
        stats.compaction_scheduler_active_oldest_age_ns = scheduler_stats.active_oldest_age_ns;
        stats.compaction_scheduler_grants = scheduler_stats.grants;
        stats.compaction_scheduler_completions = scheduler_stats.completions;
        stats.compaction_scheduler_denied_capacity = scheduler_stats.denied_capacity;
        stats.compaction_scheduler_denied_resource_pressure = scheduler_stats.denied_resource_pressure;
        stats.compaction_scheduler_oversized_grants = scheduler_stats.oversized_grants;
        stats.compaction_scheduler_oversized_skips = scheduler_stats.oversized_skips;
        stats.compaction_scheduler_remembered_candidates = scheduler_stats.remembered_candidates;
        stats.compaction_scheduler_remembered_retries = scheduler_stats.remembered_retries;
        stats.compaction_scheduler_remembered_hits = scheduler_stats.remembered_hits;
        stats.compaction_scheduler_remembered_stale = scheduler_stats.remembered_stale;
        stats.compaction_scheduler_conflict_denials = scheduler_stats.conflict_denials;
        if (self.remembered_compaction) |remembered| {
            stats.compaction_scheduler_remembered_pending = 1;
            stats.compaction_scheduler_remembered_pending_runs = @intCast(remembered.input_runs);
            stats.compaction_scheduler_remembered_pending_bytes = remembered.input_bytes;
        }
        stats.background_io_budget_bytes = self.options.background_io_budget_bytes;
        stats.background_io_reserved_bytes = self.background_io_reserved_bytes;
        stats.background_io_denied_jobs = self.background_io_denied_jobs;
        stats.background_io_oversized_jobs = self.background_io_oversized_jobs;
        stats.backend_lock_waits = self.backend_lock_waits.load(.monotonic);
        stats.backend_lock_wait_ns = self.backend_lock_wait_ns.load(.monotonic);
        stats.backend_lock_max_wait_ns = self.backend_lock_max_wait_ns.load(.monotonic);
        if (include_retention and self.options.wal_enabled and self.root_dir != null) {
            const wal_retention = self.cachedWalRetentionLocked() catch wal_mod.RetentionStats{};
            stats.wal_retained_segments = wal_retention.segments;
            stats.wal_retained_bytes = wal_retention.bytes;
            stats.wal_checkpoint_oldest_retained_segment = wal_retention.oldest_retained_segment;
            stats.wal_checkpoint_covered_through_segment = wal_retention.checkpoint_covered_through_segment;
            stats.wal_checkpoint_current_segment = wal_retention.current_segment;
            if (wal_retention.current_segment > wal_retention.oldest_retained_segment) {
                stats.wal_checkpoint_lag_segments = wal_retention.current_segment - wal_retention.oldest_retained_segment;
            }
            const replay_retention = self.cachedWalReplayRetentionLocked() catch wal_mod.RetentionStats{};
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
        if (self.options.maintenance_waker != null) {
            self.wakeMaintenanceWorker();
            return;
        }
        if (!self.mu.tryLock()) return;
        defer self.mu.unlock();
        self.scheduleMaintenanceJobIfNeededLocked();
    }

    pub fn notePotentialMaintenanceDebtLocked(self: *Backend) void {
        self.cached_maintenance_hint.store(1, .release);
        if (self.options.maintenance_waker != null) {
            self.wakeMaintenanceWorker();
            return;
        }
        self.scheduleMaintenanceJobIfNeededLocked();
    }

    pub fn noteWriteMutationLocked(self: *Backend) void {
        if (self.options.mutable_idle_flush_after_ns > 0 and
            self.mutable.entries.items.len > 0 and
            !self.options.backend.read_only)
        {
            const now_ns = self.nowNs();
            if (self.mutable_idle_flush_max_deadline_ns == 0 and
                self.options.mutable_idle_flush_max_age_ns > 0)
            {
                self.mutable_idle_flush_max_deadline_ns = now_ns +| self.options.mutable_idle_flush_max_age_ns;
            }

            const idle_eligible = self.options.mutable_idle_flush_min_bytes == 0 or
                self.mutable.estimatedLogicalBytes() >= self.options.mutable_idle_flush_min_bytes;
            const idle_deadline = if (idle_eligible)
                now_ns +| self.options.mutable_idle_flush_after_ns
            else
                std.math.maxInt(u64);
            self.mutable_idle_flush_deadline_ns = if (self.mutable_idle_flush_max_deadline_ns > 0)
                @min(idle_deadline, self.mutable_idle_flush_max_deadline_ns)
            else if (idle_eligible)
                idle_deadline
            else
                0;
        } else {
            self.mutable_idle_flush_deadline_ns = 0;
            self.mutable_idle_flush_max_deadline_ns = 0;
        }
        self.notePotentialMaintenanceDebtLocked();
    }

    fn wakeMaintenanceWorker(self: *Backend) void {
        if (self.options.maintenance_waker) |waker| waker.wake();
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

    fn largestL0OverlapRunCountLocked(self: *Backend) usize {
        const threshold = self.options.l0_overlap_compact_threshold_runs;
        if (threshold == 0) return 0;
        var l0_count: usize = 0;
        while (l0_count < self.runs.items.len and self.runs.items[l0_count].level == 0) : (l0_count += 1) {}
        // Above the soft limit, normal L0 pressure already schedules
        // compaction. Exact overlap scoring is O(n^2) and runs under `mu`, so
        // performing it during overload can block point and batch reads for
        // seconds. Zero here means "not independently scored"; L0 run/byte
        // pressure remains fully represented by the surrounding metrics.
        const soft_limit = self.effectiveL0SoftLimitRuns();
        if (soft_limit == 0 or l0_count > @min(soft_limit, compaction_mod.max_exact_l0_overlap_runs)) return 0;
        const l0_runs = self.runs.items[0..l0_count];
        if (self.l0_overlap_cache.lookup(l0_runs, threshold)) |cached| return cached;
        const result = compaction_mod.largestL0OverlapRunCount(l0_runs, threshold);
        self.l0_overlap_cache.store(l0_runs, threshold, result);
        return result;
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
            score +|= self.largestL0OverlapRunCountLocked() * 2_000;
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
            const target_bytes = compaction_mod.levelByteTargetForRuns(self.runs.items, level, self.options.level_target_bytes_base, self.options.level_target_bytes_multiplier);
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
        self.cached_maintenance_hint.store(score, .release);
        return score;
    }

    fn syncTrackedInMemoryStateUsageLocked(self: *Backend, stats: MaintenanceStats) void {
        _ = stats;
        const manager = self.options.resource_manager orelse return;
        manager.observeUsage(
            .lsm_in_memory_state,
            &self.tracked_in_memory_state_bytes,
            self.estimateInMemoryStateBytesLocked(),
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
        const manager = self.options.resource_manager orelse return;
        if (!self.options.wal_enabled or self.root_dir == null) return;
        const retention = self.cachedWalRetentionLocked() catch wal_mod.RetentionStats{};
        const replay_retention = self.cachedWalReplayRetentionLocked() catch wal_mod.RetentionStats{};
        manager.observeUsage(
            .lsm_wal_retention,
            &self.tracked_wal_retention_bytes,
            retention.bytes +| replay_retention.bytes,
        );
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
        for (self.retired_immutable_memtables.items) |state| {
            bytes +|= estimateStateBytes(state);
        }
        bytes +|= self.bulk_ingest_current_scan_clone_active_bytes;
        bytes +|= self.mutableReadSnapshotBytesLocked();
        return bytes;
    }

    fn mutableReadSnapshotBytesLocked(self: *const Backend) u64 {
        var bytes: u64 = 0;
        if (self.mutable_read_snapshot) |snapshot| {
            bytes +|= estimateStateBytes(snapshot);
        }
        for (self.retired_mutable_snapshots.items) |snapshot| {
            bytes +|= estimateStateBytes(snapshot);
        }
        return bytes;
    }

    fn releaseTrackedResourceUsage(self: *Backend) void {
        const manager = self.options.resource_manager orelse return;
        manager.observeUsage(.lsm_in_memory_state, &self.tracked_in_memory_state_bytes, 0);
        manager.observeUsage(.lsm_wal_retention, &self.tracked_wal_retention_bytes, 0);
        manager.observeUsage(.lsm_recovery_working_set, &self.tracked_recovery_working_set_bytes, 0);
    }

    pub fn acquireCompactionGrant(self: *Backend, work: anytype) ?compaction_scheduler_mod.Grant {
        const io_bytes = if (@hasField(@TypeOf(work), "io_bytes")) work.io_bytes else work.input_bytes;
        if (!self.canReserveMaintenanceIoBudget(io_bytes)) return null;
        var scheduler_work = compaction_scheduler_mod.Work{
            .score = work.score,
            .input_runs = work.input_runs,
            .input_bytes = work.input_bytes,
        };
        if (@hasField(@TypeOf(work), "run_ids")) {
            scheduler_work.run_ids = work.run_ids;
        }
        if (@hasField(@TypeOf(work), "key_range")) {
            scheduler_work.key_range = work.key_range;
        }
        const grant = self.compaction_scheduler.tryAcquireAt(scheduler_work, self.options.resource_manager, platform_time.monotonicNs()) orelse return null;
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
        const before_obsolete_paths = self.obsolete_paths.items.len;

        if (self.root_dir != null and self.hasReclaimableObsoletePathsLocked()) {
            try self.persistManifest();
            _ = self.refreshCachedMaintenanceHintLocked();
            return self.write_stats.manifest_writes != before_manifest_writes or
                self.obsolete_paths.items.len != before_obsolete_paths;
        }

        if (self.shouldFlushMutableForIdleLocked() or
            self.shouldFlushMutable() or
            try self.shouldFlushMutableForWalPressureLocked())
        {
            try self.rotateMutableToImmutable();
        }
        if (self.activeImmutableMemtableCount() > 0) {
            _ = try self.flushOldestImmutableMemtable();
        } else {
            const soft_l0_runs = self.effectiveL0SoftLimitRuns();
            const score = self.maintenanceScoreLocked();
            // Compare L0 pressure against every lower level even when L0 is
            // above its soft bound. The old L0-only branch bypassed normalized
            // pressure selection, so a 3.2x L0 backlog rewrote an 8.5x-overfull
            // L1 before L1 could be promoted. Use the soft L0 bound as the
            // pressure denominator while retaining overlap-triggered L0 work.
            _ = try compaction_mod.maybeCompactRunsScheduledWithL0Limit(
                Backend,
                self,
                if (soft_l0_runs > 0) soft_l0_runs else self.options.compact_threshold_runs,
                score,
            );
        }
        try self.enforceWritePressure();
        if (self.root_dir != null and (self.manifest_dirty or self.obsolete_manifest_dirty or self.hasReclaimableObsoletePathsLocked())) {
            try self.persistManifest();
        }

        _ = self.refreshCachedMaintenanceHintLocked();

        return self.compaction_stats.compactions != before_compactions or
            self.write_stats.manifest_writes != before_manifest_writes or
            self.obsolete_paths.items.len != before_obsolete_paths;
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
        errdefer self.allocator.free(snapshot);
        // Reserve for the worst case before changing any counts, making the
        // retain operation atomic with respect to allocation failure.
        try self.immutable_memtable_pins.ensureUnusedCapacity(self.allocator, active.len);
        for (active, 0..) |state, i| {
            snapshot[active.len - 1 - i] = state;
            self.retainImmutableMemtable(state);
        }
        return snapshot;
    }

    pub fn releaseImmutableMemtableSnapshot(self: *Backend, snapshot: []const *const State) void {
        self.releaseImmutableMemtablePins(snapshot);
        if (snapshot.len > 0) self.allocator.free(snapshot);
    }

    pub fn releaseImmutableMemtablePins(self: *Backend, snapshot: []const *const State) void {
        for (snapshot) |state| self.releaseImmutableMemtable(state);
        self.syncTrackedInMemoryStateUsageCurrentLocked();
    }

    fn retainImmutableMemtable(self: *Backend, state: *const State) void {
        for (self.immutable_memtable_pins.items) |*pin| {
            if (pin.state != state) continue;
            pin.readers += 1;
            return;
        }
        self.immutable_memtable_pins.appendAssumeCapacity(.{ .state = state, .readers = 1 });
    }

    fn releaseImmutableMemtable(self: *Backend, state: *const State) void {
        for (self.immutable_memtable_pins.items, 0..) |*pin, i| {
            if (pin.state != state) continue;
            std.debug.assert(pin.readers > 0);
            pin.readers -= 1;
            if (pin.readers != 0) return;
            _ = self.immutable_memtable_pins.swapRemove(i);
            self.reclaimRetiredImmutableMemtable(state);
            return;
        }
        std.debug.assert(false);
    }

    fn immutableMemtableIsPinned(self: *const Backend, state: *const State) bool {
        for (self.immutable_memtable_pins.items) |pin| {
            if (pin.state == state) return pin.readers > 0;
        }
        return false;
    }

    fn reclaimRetiredImmutableMemtable(self: *Backend, state: *const State) void {
        for (self.retired_immutable_memtables.items, 0..) |retired, i| {
            if (retired != state) continue;
            const removed = self.retired_immutable_memtables.swapRemove(i);
            self.destroyImmutableMemtable(removed);
            return;
        }
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
        self.notePotentialMaintenanceDebtLocked();
    }

    pub fn prepareCurrentScanSnapshot(self: *Backend) !void {
        if (self.mutable.entries.items.len == 0) return;
        const mutable_bytes = estimateStateBytes(&self.mutable);
        try self.rotateMutableToImmutable();
        self.read_snapshot_mutable_rotations +|= 1;
        self.read_snapshot_mutable_rotation_bytes_total +|= mutable_bytes;
        self.read_snapshot_mutable_rotation_peak_bytes = @max(self.read_snapshot_mutable_rotation_peak_bytes, mutable_bytes);
        if (self.shouldDeferCommitFlush()) self.scheduleImmutableFlushJob();
        self.notePotentialMaintenanceDebtLocked();
    }

    pub fn snapshotMutableState(self: *Backend) !*const State {
        return try self.snapshotMutableStateWithReason(.other);
    }

    fn cloneMutableStateWithReason(self: *Backend, reason: MutableSnapshotReason) !State {
        var snapshot = try self.mutable.cloneArena(self.allocator);
        errdefer snapshot.deinit(self.allocator);
        const snapshot_bytes = estimateStateBytes(&snapshot);
        self.mutable_snapshot_clone_calls +|= 1;
        self.mutable_snapshot_clone_bytes_total +|= snapshot_bytes;
        self.mutable_snapshot_clone_peak_bytes = @max(self.mutable_snapshot_clone_peak_bytes, snapshot_bytes);
        const reason_index = mutableSnapshotReasonIndex(reason);
        self.mutable_snapshot_clone_by_reason[reason_index].calls +|= 1;
        self.mutable_snapshot_clone_by_reason[reason_index].bytes_total +|= snapshot_bytes;
        self.mutable_snapshot_clone_by_reason[reason_index].peak_bytes = @max(self.mutable_snapshot_clone_by_reason[reason_index].peak_bytes, snapshot_bytes);
        return snapshot;
    }

    pub fn snapshotMutableStateWithReason(self: *Backend, reason: MutableSnapshotReason) !*const State {
        if (self.mutable_read_snapshot) |snapshot| {
            self.retainMutableSnapshotReader(snapshot);
            return snapshot;
        }
        if (self.mutable.entries.items.len == 0) return &self.empty_mutable_snapshot;
        const snapshot = try self.allocator.create(State);
        errdefer self.allocator.destroy(snapshot);
        snapshot.* = try self.cloneMutableStateWithReason(reason);
        errdefer snapshot.deinit(self.allocator);
        try self.retired_mutable_snapshots.ensureUnusedCapacity(self.allocator, 1);
        try self.retired_mutable_snapshot_by_state.ensureUnusedCapacity(self.allocator, 1);
        try self.mutable_snapshot_reader_refs.ensureUnusedCapacity(self.allocator, 1);
        try self.mutable_snapshot_reader_ref_by_state.ensureUnusedCapacity(self.allocator, 1);
        const ref_index = self.mutable_snapshot_reader_refs.items.len;
        self.mutable_snapshot_reader_refs.appendAssumeCapacity(.{
            .state = snapshot,
            .readers = 1,
        });
        self.mutable_snapshot_reader_ref_by_state.putAssumeCapacity(snapshot, ref_index);
        self.mutable_read_snapshot = snapshot;
        self.syncTrackedInMemoryStateUsageCurrentLocked();
        return snapshot;
    }

    fn mutableSnapshotReaderRefIndex(self: *const Backend, state: *const State) ?usize {
        return self.mutable_snapshot_reader_ref_by_state.get(state);
    }

    fn removeMutableSnapshotReaderRef(self: *Backend, index: usize) MutableSnapshotReaderRef {
        const removed = self.mutable_snapshot_reader_refs.items[index];
        _ = self.mutable_snapshot_reader_ref_by_state.remove(removed.state);
        _ = self.mutable_snapshot_reader_refs.swapRemove(index);
        if (index < self.mutable_snapshot_reader_refs.items.len) {
            const moved = self.mutable_snapshot_reader_refs.items[index];
            self.mutable_snapshot_reader_ref_by_state.getPtr(moved.state).?.* = index;
        }
        return removed;
    }

    fn retainMutableSnapshotReader(self: *Backend, state: *State) void {
        const index = self.mutableSnapshotReaderRefIndex(state) orelse unreachable;
        self.mutable_snapshot_reader_refs.items[index].readers += 1;
    }

    pub fn releaseMutableReadSnapshot(self: *Backend, state: *const State) void {
        const index = self.mutableSnapshotReaderRefIndex(state) orelse {
            std.debug.assert(state == &self.empty_mutable_snapshot);
            return;
        };
        const ref = &self.mutable_snapshot_reader_refs.items[index];
        std.debug.assert(ref.readers > 0);
        ref.readers -= 1;
        if (ref.readers != 0 or self.mutable_read_snapshot == state) return;

        const remove_index = self.retired_mutable_snapshot_by_state.get(state) orelse unreachable;
        self.removeRetiredMutableSnapshot(remove_index);
        const owned = self.removeMutableSnapshotReaderRef(index).state;
        self.destroyMutableSnapshot(owned);
        self.syncTrackedInMemoryStateUsageCurrentLocked();
    }

    pub fn cloneCurrentScanMutableStateForBulkIngest(self: *Backend) !?State {
        if (!self.bulkIngestActive()) return null;
        if (self.options.bulk_ingest_current_scan_clone_max_bytes == 0) return null;
        if (self.mutable.entries.items.len == 0) return State{};
        const mutable_bytes = estimateStateBytes(&self.mutable);
        if (mutable_bytes > self.options.bulk_ingest_current_scan_clone_max_bytes) return null;
        if (!self.canAdmitBulkIngestCurrentScanClone(mutable_bytes)) {
            self.bulk_ingest_current_scan_clone_budget_denials +|= 1;
            return null;
        }
        var snapshot = self.cloneMutableStateWithReason(.other) catch |err| switch (err) {
            error.OutOfMemory => {
                self.bulk_ingest_current_scan_clone_oom_fallbacks +|= 1;
                return null;
            },
        };
        errdefer snapshot.deinit(self.allocator);
        const snapshot_bytes = estimateStateBytes(&snapshot);
        if (!self.canAdmitBulkIngestCurrentScanClone(snapshot_bytes)) {
            self.bulk_ingest_current_scan_clone_budget_denials +|= 1;
            return null;
        }
        self.bulk_ingest_current_scan_clone_active_bytes +|= snapshot_bytes;
        self.bulk_ingest_current_scan_clone_peak_active_bytes = @max(
            self.bulk_ingest_current_scan_clone_peak_active_bytes,
            self.bulk_ingest_current_scan_clone_active_bytes,
        );
        self.syncTrackedInMemoryStateUsageCurrentLocked();
        return snapshot;
    }

    fn canAdmitBulkIngestCurrentScanClone(self: *const Backend, bytes: u64) bool {
        const total_max = self.options.bulk_ingest_current_scan_clone_total_max_bytes;
        if (total_max == 0) return true;
        if (bytes > total_max) return false;
        return self.bulk_ingest_current_scan_clone_active_bytes <= total_max - bytes;
    }

    pub fn releaseCurrentScanMutableStateForBulkIngest(self: *Backend, snapshot: *const State) void {
        const snapshot_bytes = estimateStateBytes(snapshot);
        if (snapshot_bytes >= self.bulk_ingest_current_scan_clone_active_bytes) {
            self.bulk_ingest_current_scan_clone_active_bytes = 0;
        } else {
            self.bulk_ingest_current_scan_clone_active_bytes -= snapshot_bytes;
        }
        self.syncTrackedInMemoryStateUsageCurrentLocked();
    }

    pub fn invalidateMutableReadSnapshot(self: *Backend) void {
        const snapshot = self.mutable_read_snapshot orelse return;
        self.mutable_read_snapshot = null;
        self.retireMutableSnapshot(snapshot);
        self.syncTrackedInMemoryStateUsageCurrentLocked();
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
        if (!self.immutableMemtableIsPinned(state)) {
            self.destroyImmutableMemtable(state);
            return;
        }
        try self.retired_immutable_memtables.append(self.allocator, state);
        self.syncTrackedInMemoryStateUsageCurrentLocked();
    }

    fn retireMutableSnapshot(self: *Backend, state: *State) void {
        const ref_index = self.mutableSnapshotReaderRefIndex(state) orelse unreachable;
        if (self.mutable_snapshot_reader_refs.items[ref_index].readers == 0) {
            _ = self.removeMutableSnapshotReaderRef(ref_index);
            self.destroyMutableSnapshot(state);
            return;
        }
        // Capacity is reserved when the snapshot is published, before any
        // reader can observe it. Retirement therefore cannot fail while a
        // reader still owns the generation.
        const retired_index = self.retired_mutable_snapshots.items.len;
        self.retired_mutable_snapshots.appendAssumeCapacity(state);
        self.retired_mutable_snapshot_by_state.putAssumeCapacity(state, retired_index);
    }

    fn removeRetiredMutableSnapshot(self: *Backend, index: usize) void {
        const removed = self.retired_mutable_snapshots.items[index];
        _ = self.retired_mutable_snapshot_by_state.remove(removed);
        _ = self.retired_mutable_snapshots.swapRemove(index);
        if (index < self.retired_mutable_snapshots.items.len) {
            const moved = self.retired_mutable_snapshots.items[index];
            self.retired_mutable_snapshot_by_state.getPtr(moved).?.* = index;
        }
    }

    fn drainRetiredImmutableMemtables(self: *Backend) void {
        var i: usize = 0;
        while (i < self.retired_immutable_memtables.items.len) {
            const state = self.retired_immutable_memtables.items[i];
            if (self.immutableMemtableIsPinned(state)) {
                i += 1;
                continue;
            }
            _ = self.retired_immutable_memtables.swapRemove(i);
            self.destroyImmutableMemtable(state);
        }
        self.syncTrackedInMemoryStateUsageCurrentLocked();
    }

    fn drainRetiredMutableSnapshots(self: *Backend) void {
        for (self.retired_mutable_snapshots.items) |state| {
            if (self.mutableSnapshotReaderRefIndex(state)) |index| {
                std.debug.assert(self.mutable_snapshot_reader_refs.items[index].readers == 0);
                _ = self.removeMutableSnapshotReaderRef(index);
            }
            self.destroyMutableSnapshot(state);
        }
        self.retired_mutable_snapshots.clearRetainingCapacity();
        self.retired_mutable_snapshot_by_state.clearRetainingCapacity();
        self.syncTrackedInMemoryStateUsageCurrentLocked();
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
        var wal_lock = try self.acquireWalOperationLock(.exclusive);
        defer wal_lock.release();
        try self.wal_retention.retireCoveredSegments(
            self.storage.?,
            self.allocator,
            self.root_dir.?,
            oldest_active - 1,
        );
        self.syncTrackedWalRetentionUsageCurrentLocked();
    }

    fn estimateStateBytes(state: anytype) u64 {
        var total: u64 = @as(u64, @intCast(state.entries.capacity)) * @sizeOf(state_mod.OwnedEntry);
        if (state.arena_owner) |arena| {
            // Arena frees are intentionally deferred until the whole memtable
            // is released. Query its actual capacity so hot overwrites and
            // allocator growth slack participate in flush/admission limits.
            total +|= arena.queryCapacity();
        } else {
            for (state.entries.items) |entry| {
                if (entry.namespace_name) |name| total += name.len;
                total += entry.key.len;
                total += entry.value.len;
            }
        }
        if (comptime @hasDecl(@TypeOf(state.*), "estimatedIndexMemoryBytes")) {
            total +|= state.estimatedIndexMemoryBytes();
        }
        return total;
    }

    fn estimateStateLogicalBytes(state: anytype) u64 {
        if (comptime @hasDecl(@TypeOf(state.*), "estimatedLogicalBytes")) {
            return state.estimatedLogicalBytes();
        }
        var total: u64 = 0;
        for (state.entries.items) |entry| {
            if (entry.namespace_name) |name| total += name.len;
            total += entry.key.len;
            total += entry.value.len;
            total += @sizeOf(state_mod.OwnedEntry);
        }
        return total;
    }

    fn stateMeetsByteFlushThreshold(state: anytype, logical_threshold: u64) bool {
        if (logical_threshold == 0) return false;
        if (estimateStateLogicalBytes(state) >= logical_threshold) return true;
        const memory_threshold = std.math.mul(
            u64,
            logical_threshold,
            mutable_memory_guard_multiplier,
        ) catch std.math.maxInt(u64);
        return estimateStateBytes(state) >= memory_threshold;
    }

    fn estimatedFlushIoBytes(state: *const State) u64 {
        const bytes = estimateStateLogicalBytes(state);
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
        try self.enforceWalRetentionSoftPressureGuarded();
        try self.enforceWalRetentionHardPressureGuarded();
        self.scheduleMaintenanceJobIfNeededLocked();
    }

    /// Admission happens before WAL append and before the transaction-owned
    /// memtable is merged into allocator-backed LSM state. This is the only
    /// point where a ResourceManager reject policy can fail a write without
    /// reporting failure after it has become durable.
    ///
    /// This boundary can be reached while the caller holds a higher-level DB
    /// apply lock. It must therefore never sleep waiting for aggregate
    /// ResourceManager usage owned by another backend: that backend may need
    /// the same apply lock to flush or publish its work. Soft throttling is a
    /// local reclamation request here. If local state has been drained and the
    /// aggregate slice is still only soft-pressured, admit the write and let
    /// the post-commit/background paths make progress. Hard pressure remains
    /// a non-blocking pre-WAL rejection.
    pub fn enforceMutableWriteAdmission(self: *Backend, incoming: *const ActiveMemTable) !void {
        const manager = self.options.resource_manager orelse return;
        const incoming_bytes = estimateStateBytes(incoming);

        while (true) {
            const decision = manager.admissionDecision(.lsm_in_memory_state, incoming_bytes);
            switch (decision.action) {
                .report, .shrink_cache, .defer_background_work => return,
                .reject_work => return error.ResourceBudgetExceeded,
                .throttle_writes => {},
            }

            if (self.activeImmutableMemtableCount() > 0) {
                if (try self.flushOldestImmutableMemtable()) continue;
                self.scheduleImmutableFlushJob();
                if (self.waitForImmutableFlushBuildLocked()) continue;
            } else if (self.mutable.entries.items.len > 0) {
                try self.flushMutable();
                continue;
            }

            // If this batch cannot fit even in an otherwise empty slice,
            // waiting cannot make progress. Surface the budget error instead.
            if (decision.hard_limit_bytes > 0 and incoming_bytes > decision.hard_limit_bytes) {
                return error.ResourceBudgetExceeded;
            }
            if (decision.pressure == .soft) return;
            return error.ResourceBudgetExceeded;
        }
    }

    fn shouldDeferCommitFlush(self: *const Backend) bool {
        if (self.root_dir == null) return false;
        if (!self.options.wal_enabled) return false;
        if (self.options.foreground_soft_compaction) return false;
        if (self.effectiveFlushThresholdBytes() > 0) return true;
        return self.options.defer_flush_on_commit;
    }

    fn enforceDeferredImmutableBackpressure(self: *Backend) !void {
        const count_limit = self.options.max_deferred_immutable_memtables;
        const byte_limit = self.options.max_deferred_immutable_bytes;
        while (true) {
            const local_pressure = (count_limit > 0 and self.activeImmutableMemtableCount() > count_limit) or
                (byte_limit > 0 and self.activeImmutableMemtableBytes() > byte_limit);
            const resource_decision = if (self.options.resource_manager) |manager|
                manager.pressureDecision(.lsm_in_memory_state)
            else
                resource_manager_mod.PressureDecision{};

            // This boundary is after the write was admitted and may already be
            // durable. A newly observed reject action therefore drains like a
            // throttle; rejection itself is handled before WAL append above.
            const global_throttle = (resource_decision.action == .throttle_writes or
                resource_decision.action == .reject_work) and
                self.activeImmutableMemtableCount() > 0;
            if (!local_pressure and !global_throttle) return;

            if (!try self.flushOldestImmutableMemtable()) {
                self.scheduleImmutableFlushJob();
                if (!self.waitForImmutableFlushBuildLocked()) return;
            }
        }
    }

    fn waitForImmutableFlushBuildLocked(self: *Backend) bool {
        if (!self.immutable_flush_build_in_flight or !supports_waitable_immutable_flush) return false;
        const observed = self.immutable_flush_completion.snapshot();
        runtime_mod.unlockBackend(Backend, self, true);
        self.immutable_flush_completion.waitForChange(observed);
        const relocked = runtime_mod.lockBackend(Backend, self);
        std.debug.assert(relocked);
        return !self.closing.load(.acquire);
    }

    fn finishImmutableFlushBuildLocked(self: *Backend) void {
        std.debug.assert(self.immutable_flush_build_in_flight);
        self.immutable_flush_build_in_flight = false;
        self.immutable_flush_completion.advance();
    }

    fn activeImmutableMemtableBytes(self: *const Backend) u64 {
        var bytes: u64 = 0;
        for (self.activeImmutableMemtables()) |state| bytes +|= estimateStateBytes(state);
        return bytes;
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
        self.mutable_idle_flush_deadline_ns = 0;
        self.mutable_idle_flush_max_deadline_ns = 0;
        self.write_stats.immutable_rotations += 1;
        self.syncTrackedInMemoryStateUsageCurrentLocked();
    }

    fn flushAllImmutableMemtables(self: *Backend) !void {
        while (try self.flushOldestImmutableMemtable()) {}
    }

    fn scheduleImmutableFlushJob(self: *Backend) void {
        if (self.activeImmutableMemtableCount() == 0) return;
        if (self.closing.load(.acquire)) return;
        if (self.options.maintenance_waker != null) {
            self.wakeMaintenanceWorker();
            return;
        }
        if (self.immutable_flush_job_in_flight) return;
        if (!self.background_executor.canRunDetached()) return;

        self.immutable_flush_job_in_flight = true;
        self.background_executor.submit(.commit_durable, self, runImmutableFlushJob, deinitImmutableFlushJob) catch |err| {
            self.immutable_flush_job_in_flight = false;
            if (err == error.BackgroundOwnerClosing or err == error.BackgroundOwnerClosed) return;
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

    fn scheduleMaintenanceJobIfNeededLocked(self: *Backend) void {
        if (self.closing.load(.acquire)) return;
        if (self.options.backend.read_only or self.bulkIngestActive()) return;
        if (self.options.maintenance_waker != null) {
            if (self.maintenanceScoreLocked() != 0) self.wakeMaintenanceWorker();
            return;
        }
        if (self.maintenance_job_in_flight) return;
        if (self.immutable_flush_job_in_flight or self.immutable_flush_build_in_flight) return;
        if (self.activeImmutableMemtableCount() > 0) return;
        if (!self.background_executor.canRunDetached()) return;
        if (self.maintenanceScoreLocked() == 0 and !self.hasReclaimableObsoletePathsLocked()) return;

        self.maintenance_job_in_flight = true;
        self.background_executor.submit(.maintenance, self, runMaintenanceJob, deinitMaintenanceJob) catch |err| {
            self.maintenance_job_in_flight = false;
            if (err == error.BackgroundOwnerClosing or err == error.BackgroundOwnerClosed) return;
            std.log.warn("lsm maintenance background scheduling failed root={?s} err={}", .{ self.root_dir, err });
        };
    }

    fn runMaintenanceJob(ptr: *anyopaque) !void {
        const self: *Backend = @ptrCast(@alignCast(ptr));
        errdefer self.clearMaintenanceJobInFlight(false);

        const max_steps = @max(@as(usize, 1), self.options.background_maintenance_max_steps);
        var steps: usize = 0;
        var made_progress = false;
        while (steps < max_steps and !self.closing.load(.acquire)) : (steps += 1) {
            const progressed = try self.runMaintenanceStep();
            if (!progressed) break;
            made_progress = true;
        }

        // A positive maintenance score is only a hint: overlap or level
        // pressure can remain non-zero when no valid compaction plan exists.
        // Do not immediately resubmit that same no-op job forever. A later
        // write, flush, or a job that actually made progress will schedule the
        // next pass.
        self.clearMaintenanceJobInFlight(made_progress);
    }

    fn clearMaintenanceJobInFlight(self: *Backend, maybe_reschedule: bool) void {
        const locked = runtime_mod.lockBackend(Backend, self);
        defer runtime_mod.unlockBackend(Backend, self, locked);
        self.maintenance_job_in_flight = false;
        if (maybe_reschedule and !self.closing.load(.acquire)) self.scheduleMaintenanceJobIfNeededLocked();
    }

    fn deinitMaintenanceJob(_: *anyopaque) void {}

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
            if (self.writePressureDuringBulkIngestEnabled()) {
                try self.enforceWritePressure();
            }
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
        defer self.finishImmutableFlushBuildLocked();
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
            if (self.writePressureDuringBulkIngestEnabled()) {
                try self.enforceWritePressure();
            }
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
        if (!lsm_table_file.maybeContains(try run.ensureBloomFilterWithOptionalStorage(self.allocator, self.storage), namespace.name, key)) {
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
                if (self.writePressureDuringBulkIngestEnabled()) {
                    try self.enforceWritePressure();
                }
            } else {
                try self.persistManifest();
            }
        }
    }

    pub fn ingestSortedState(self: *Backend, state: *const State) !void {
        if (self.options.backend.read_only) return error.ReadOnly;
        if (state.entries.items.len == 0) return;

        if (self.mutable.entries.items.len > 0) {
            try self.flushMutable();
        }

        const start_ns = self.writeStatsNowNs();
        var new_runs = try compaction_mod.makeRunsFromStateBorrowed(Backend, self, state);
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
                if (self.writePressureDuringBulkIngestEnabled()) {
                    try self.enforceWritePressure();
                }
            } else {
                try self.persistManifest();
            }
        }
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
        if (state.arena_owner != null and estimateStateLogicalBytes(state) <= target_bytes) {
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
        if (self.bulkIngestActive() and self.writePressureDuringBulkIngestEnabled()) {
            try self.enforceWritePressure();
        }
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
        if (byte_threshold > 0) return stateMeetsByteFlushThreshold(mutable, byte_threshold);
        return mutable.entries.items.len >= self.effectiveFlushThreshold();
    }

    pub fn persistManifest(self: *Backend) !void {
        const root_dir = self.root_dir orelse return;
        const start_ns = self.writeStatsNowNs();

        const reclaimable_obsolete_paths = self.hasReclaimableObsoletePathsLocked();
        const clean_publish = !self.manifest_dirty and !self.obsolete_manifest_dirty and !reclaimable_obsolete_paths;
        if (clean_publish and self.durableManifestHasNewerRunIdLocked(root_dir)) return;

        try self.writeManifestSnapshotLocked(root_dir, start_ns);

        if (reclaimable_obsolete_paths) {
            try self.reconcileObsoletePathsForManifest();
            const removed_or_retried = self.obsolete_manifest_dirty;
            if (removed_or_retried) try self.writeManifestSnapshotLocked(root_dir, start_ns);
        }
    }

    fn durableManifestHasNewerRunIdLocked(self: *Backend, root_dir: []const u8) bool {
        const storage = self.storage orelse return false;
        var manifest_backing: ?[]u8 = null;
        defer if (manifest_backing) |backing| self.allocator.free(backing);

        var durable_next_run_id: u64 = 0;
        var durable_runs = std.ArrayListUnmanaged(Run).empty;
        defer {
            for (durable_runs.items) |*run| run.deinit(self.allocator);
            durable_runs.deinit(self.allocator);
        }
        var durable_obsolete = std.ArrayListUnmanaged(ObsoletePath).empty;
        defer {
            for (durable_obsolete.items) |*obsolete| obsolete.deinit(self.allocator);
            durable_obsolete.deinit(self.allocator);
        }

        const found = repository_mod.loadManifestIfPresentWithStorage(
            storage,
            self.allocator,
            root_dir,
            &manifest_backing,
            &durable_next_run_id,
            &durable_runs,
            &durable_obsolete,
        ) catch return false;
        return found and durable_next_run_id > self.next_run_id;
    }

    fn writeManifestSnapshotLocked(self: *Backend, root_dir: []const u8, start_ns: u64) !void {
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
        self.current_manifest_bytes = bytes;
        try self.maybeCheckpointWalAfterManifestPublish();
        self.manifest_dirty = false;
        self.obsolete_manifest_dirty = false;
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
        var wal_lock = try self.acquireWalOperationLock(.exclusive);
        defer wal_lock.release();
        const append_result = try self.wal_retention.append(
            self.storage.?,
            self.allocator,
            self.root_dir.?,
            state,
            self.options.wal_sync_on_commit,
            .{ .segment_bytes = self.options.wal_segment_bytes },
            self.writeStatsNowNs(),
        );
        self.noteMutableWalSegment(append_result.segment);
        self.syncTrackedWalRetentionUsageCurrentLocked();
        self.write_stats.wal_append_records += 1;
        self.write_stats.wal_append_entries += @intCast(state.entries.items.len);
        self.write_stats.wal_append_bytes += append_result.bytes;
        self.write_stats.wal_segment_syncs += append_result.segment_syncs;
        self.write_stats.wal_index_syncs += append_result.index_syncs;
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
        // Replay may truncate a corrupt primary tail and advances replay
        // bookkeeping. Treat both derived snapshots as unknown until it has
        // completed and the primary snapshot is rebuilt below.
        self.wal_retention.invalidateAll();
        self.recovery_replaying_wal = true;
        errdefer self.recovery_replaying_wal = false;
        var recovery_session = RecoveryReplaySession{ .backend = self };
        const replay_hooks: ?wal_mod.ReplayHooks = if (!self.options.backend.read_only)
            .{
                .ctx = @ptrCast(&recovery_session),
                .entry_allocator = replayWalEntryAllocatorHook,
                .on_applied_entry = replayWalAppliedEntryHook,
                .on_applied_record = replayWalAppliedRecordHook,
            }
        else
            null;
        const stats = blk: {
            var wal_lock = try self.acquireWalOperationLock(if (self.options.backend.read_only) .shared else .exclusive);
            defer wal_lock.release();
            break :blk try wal_mod.replayIntoMutableWithHooksAndOptions(
                self.storage.?,
                self.allocator,
                self.root_dir.?,
                &self.mutable,
                replay_hooks,
                .{
                    .resource_manager = self.options.resource_manager,
                    .tracked_working_set_bytes = &self.tracked_recovery_working_set_bytes,
                    .retained_cap_bytes = self.options.recovery_scratch_retained_cap_bytes,
                },
            );
        };
        if (!self.options.backend.read_only and
            recovery_session.flushes > 0 and
            (self.mutable.entries.items.len > 0 or self.activeImmutableMemtableCount() > 0))
        {
            try self.flushMutable();
            recovery_session.flushes += 1;
            recovery_session.active_window_bytes = 0;
        }
        self.recovery_replaying_wal = false;
        if (!self.options.backend.read_only and self.write_stats.manifest_writes != before_manifest_writes) {
            try self.maybeCheckpointWalAfterManifestPublish();
        }
        const retention = try self.cachedWalRetentionLocked();
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
        self.write_stats.wal_replay_recovery_flushes += recovery_session.flushes;
        self.write_stats.wal_replay_recovery_entry_bytes += recovery_session.total_entry_bytes;
        self.write_stats.wal_replay_recovery_window_peak_bytes = @max(
            self.write_stats.wal_replay_recovery_window_peak_bytes,
            recovery_session.peak_window_bytes,
        );
        self.write_stats.wal_replay_recovery_records_applied += recovery_session.records_applied;
        self.write_stats.wal_replay_recovery_entries_applied += recovery_session.entries_applied;
    }

    const RecoveryReplaySession = struct {
        backend: *Backend,
        active_window_bytes: u64 = 0,
        peak_window_bytes: u64 = 0,
        total_entry_bytes: u64 = 0,
        flushes: u64 = 0,
        records_applied: u64 = 0,
        entries_applied: u64 = 0,

        fn noteEntry(self: *@This(), segment: u64, entry_bytes: u64) !void {
            if (segment != 0) self.backend.noteMutableWalSegment(segment);
            self.active_window_bytes +|= entry_bytes;
            self.total_entry_bytes +|= entry_bytes;
            self.peak_window_bytes = @max(self.peak_window_bytes, self.active_window_bytes);
            self.entries_applied += 1;
            if (!self.backend.shouldFlushMutableDuringRecoveryReplay()) return;
            try self.backend.flushMutable();
            self.flushes += 1;
            self.active_window_bytes = 0;
        }

        fn noteRecord(self: *@This(), segment: u64, _: u64) !void {
            if (segment != 0) self.backend.noteMutableWalSegment(segment);
            self.records_applied += 1;
            if (!self.backend.shouldFlushMutableDuringRecoveryReplay()) return;
            try self.backend.flushMutable();
            self.flushes += 1;
            self.active_window_bytes = 0;
        }

        fn entryAllocator(self: *@This(), default_allocator: Allocator) !Allocator {
            // ActiveMemTable owns structural containers on the backend allocator.
            // During recovery, entry byte copies live in the current mutable
            // arena, which is transferred to the immutable flush window and
            // released wholesale when that flush retires.
            _ = try self.backend.mutable.ensureRecoveryAllocator(default_allocator);
            return default_allocator;
        }
    };

    fn replayWalAppliedEntryHook(ctx: *anyopaque, segment: u64, entry_bytes: u64) anyerror!void {
        const session: *RecoveryReplaySession = @ptrCast(@alignCast(ctx));
        try session.noteEntry(segment, entry_bytes);
    }

    fn replayWalAppliedRecordHook(ctx: *anyopaque, segment: u64, applied: u64) anyerror!void {
        const session: *RecoveryReplaySession = @ptrCast(@alignCast(ctx));
        try session.noteRecord(segment, applied);
    }

    fn replayWalEntryAllocatorHook(ctx: *anyopaque, default_allocator: Allocator) anyerror!Allocator {
        const session: *RecoveryReplaySession = @ptrCast(@alignCast(ctx));
        return try session.entryAllocator(default_allocator);
    }

    fn resetWalAfterManifestCheckpoint(self: *Backend) !void {
        if (!self.options.wal_enabled or self.root_dir == null or self.options.backend.read_only) return;
        const start_ns = self.writeStatsNowNs();
        var wal_lock = try self.acquireWalOperationLock(.exclusive);
        defer wal_lock.release();
        try self.wal_retention.reset(
            self.storage.?,
            self.allocator,
            self.root_dir.?,
            self.writeStatsNowNs(),
        );
        self.syncTrackedWalRetentionUsageCurrentLocked();
        self.write_stats.wal_resets += 1;
        self.write_stats.wal_reset_ns += self.writeStatsElapsedNs(start_ns);
    }

    pub fn checkpointWalAfterDurableBoundary(self: *Backend) !void {
        if (!self.options.wal_enabled or self.root_dir == null or self.options.backend.read_only) return;
        const locked = runtime_mod.lockBackend(Backend, self);
        defer runtime_mod.unlockBackend(Backend, self, locked);

        const saved_budget = self.maintenance_io_budget_remaining;
        self.maintenance_io_budget_remaining = null;
        defer self.maintenance_io_budget_remaining = saved_budget;

        if (self.mutable.entries.items.len > 0) {
            try self.rotateMutableToImmutable();
        }
        while (self.activeImmutableMemtableCount() > 0) {
            if (!try self.flushOldestImmutableMemtable()) break;
        }
        try self.maybeCheckpointWalAfterManifestPublish();
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

    pub fn registerOpenManifestRunRefs(self: *Backend) !void {
        for (self.runs.items) |*run| {
            if (run.version_ref_pinned) continue;
            const path = run.path orelse continue;
            try obsolete_path_refs.retain(path);
            run.version_ref_pinned = true;
        }
    }

    pub fn retainRunSnapshotRef(_: *Backend, run: *Run) !void {
        const path = run.path orelse return;
        try run_snapshot_refs.retain(path);
        run.version_ref_pinned = true;
    }

    pub fn releaseRunSnapshotRef(_: *Backend, run: *Run) void {
        if (!run.version_ref_pinned) return;
        if (run.path) |path| run_snapshot_refs.release(path);
        run.version_ref_pinned = false;
    }

    pub fn forgetRunSnapshotRef(_: *Backend, run: *Run) void {
        if (run.path) |path| run_snapshot_refs.forget(path);
    }

    pub fn releaseRunVersionRef(self: *Backend, run: *Run) void {
        _ = self;
        if (!run.version_ref_pinned) return;
        if (run.path) |path| obsolete_path_refs.release(path);
        run.version_ref_pinned = false;
    }

    fn obsoletePathPinnedByOpenVersion(self: *Backend, path: []const u8) bool {
        _ = self;
        return obsolete_path_refs.isRetained(path) or run_snapshot_refs.isRetained(path);
    }

    pub fn retainReader(self: *Backend) void {
        self.retainReaderKind(.other);
    }

    pub fn retainReaderKind(self: *Backend, kind: ReaderPinKind) void {
        self.active_readers += 1;
        self.active_readers_by_kind[readerPinKindIndex(kind)] += 1;
    }

    pub fn retainActiveMutableValueReader(self: *Backend) void {
        self.active_mutable_value_readers += 1;
    }

    pub fn releaseActiveMutableValueReader(self: *Backend) void {
        std.debug.assert(self.active_mutable_value_readers > 0);
        self.active_mutable_value_readers -= 1;
    }

    pub fn canBorrowActiveMutableValues(self: *const Backend) bool {
        return self.active_mutable_value_readers > 0;
    }

    /// Write transactions are counted for backend-close fencing, but point
    /// reads execute under the backend lock and do not retain an LSM version.
    /// A write cursor takes an additional `.current_scan` pin.
    fn activeVersionReaders(self: *const Backend) usize {
        return self.active_readers -| self.active_readers_by_kind[readerPinKindIndex(.write_txn)];
    }

    pub fn hasVersionReaderPins(self: *const Backend) bool {
        return self.activeVersionReaders() != 0;
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

    pub fn recordPointRunPrecheck(self: *Backend) void {
        _ = self.read_stats.point_run_prechecks.fetchAdd(1, .monotonic);
    }

    pub fn recordPointRunPrecheckSurvivor(self: *Backend) void {
        _ = self.read_stats.point_run_precheck_survivors.fetchAdd(1, .monotonic);
    }

    pub fn recordPointRunSurvivorRead(self: *Backend) void {
        _ = self.read_stats.point_run_survivor_reads.fetchAdd(1, .monotonic);
    }

    pub fn recordPointRunSurvivorHit(self: *Backend) void {
        _ = self.read_stats.point_run_survivor_hits.fetchAdd(1, .monotonic);
    }

    pub fn recordPointRunSurvivorMiss(self: *Backend) void {
        _ = self.read_stats.point_run_survivor_misses.fetchAdd(1, .monotonic);
    }

    pub fn recordPointRunSurvivorTombstone(self: *Backend) void {
        _ = self.read_stats.point_run_survivor_tombstones.fetchAdd(1, .monotonic);
    }

    pub fn recordPointRunAsyncBatch(self: *Backend, reads_issued: usize) void {
        _ = self.read_stats.point_run_async_batches.fetchAdd(1, .monotonic);
        _ = self.read_stats.point_run_async_reads_issued.fetchAdd(@intCast(reads_issued), .monotonic);
    }

    pub fn recordPointRunAsyncCancel(self: *Backend) void {
        _ = self.read_stats.point_run_async_reads_canceled.fetchAdd(1, .monotonic);
    }

    pub fn recordPointRunAsyncWait(self: *Backend, elapsed_ns: u64) void {
        _ = self.read_stats.point_run_async_wait_ns.fetchAdd(elapsed_ns, .monotonic);
    }

    pub fn recordBloomNegative(self: *Backend) void {
        _ = self.read_stats.bloom_negatives.fetchAdd(1, .monotonic);
    }

    pub fn recordPrefixBloomNegative(self: *Backend) void {
        _ = self.read_stats.bloom_negatives.fetchAdd(1, .monotonic);
        _ = self.read_stats.prefix_bloom_negatives.fetchAdd(1, .monotonic);
    }

    pub fn recordBlockPrefixBloomNegative(self: *Backend) void {
        _ = self.read_stats.bloom_negatives.fetchAdd(1, .monotonic);
        _ = self.read_stats.block_prefix_bloom_negatives.fetchAdd(1, .monotonic);
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
        self.releaseReaderKind(.other);
    }

    pub fn releaseReaderKind(self: *Backend, kind: ReaderPinKind) void {
        const index = readerPinKindIndex(kind);
        std.debug.assert(self.active_readers > 0);
        std.debug.assert(self.active_readers_by_kind[index] > 0);
        self.active_readers -= 1;
        self.active_readers_by_kind[index] -= 1;
        self.drainUnpinnedObsoleteRuns();
        if (self.activeVersionReaders() == 0) {
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
            self.notePotentialMaintenanceDebtLocked();
            return;
        }

        try self.obsolete_paths.append(self.allocator, .{
            .path = path,
            .delete_after_ns = delete_after_ns,
        });
        self.obsolete_manifest_dirty = true;
        self.notePotentialMaintenanceDebtLocked();
    }

    pub fn queueObsoleteRuns(self: *Backend, runs: std.ArrayListUnmanaged(Run)) !void {
        if (runs.items.len == 0) {
            var empty = runs;
            empty.deinit(self.allocator);
            return;
        }
        const retired = runs;
        // The backend's active-version ownership ends at compaction publish.
        // Snapshot readers now carry their own per-run pins.
        for (retired.items) |*run| self.releaseRunVersionRef(run);
        try self.obsolete_runs.append(self.allocator, retired);
        self.drainUnpinnedObsoleteRuns();
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
        self.drainUnpinnedObsoleteRuns();
    }

    fn drainUnpinnedObsoleteRuns(self: *Backend) void {
        var list_index: usize = 0;
        while (list_index < self.obsolete_runs.items.len) {
            var runs = &self.obsolete_runs.items[list_index];
            var run_index: usize = 0;
            while (run_index < runs.items.len) {
                const run = &runs.items[run_index];
                if (run.path) |path| {
                    if (run_snapshot_refs.isRetained(path)) {
                        run_index += 1;
                        continue;
                    }
                    // File retention is tracked separately by obsolete_paths;
                    // this releases only metadata and local cache handles.
                    self.evictLocalCachesForRun(path, run.id);
                }
                var removed = runs.orderedRemove(run_index);
                removed.deinit(self.allocator);
            }
            if (runs.items.len != 0) {
                list_index += 1;
                continue;
            }
            var removed_list = self.obsolete_runs.orderedRemove(list_index);
            removed_list.deinit(self.allocator);
        }
    }

    pub fn finalizeWriteReaderRelease(self: *Backend) !void {
        try self.finalizeWriteReaderReleaseKind(.other);
    }

    pub fn finalizeWriteReaderReleaseKind(self: *Backend, kind: ReaderPinKind) !void {
        self.releaseReaderKind(kind);
        const reclaimable_obsolete_paths = self.hasReclaimableObsoletePathsLocked();
        if ((!self.manifest_dirty and !self.obsolete_manifest_dirty and !reclaimable_obsolete_paths) or
            self.bulkIngestActive() or
            self.root_dir == null or
            self.options.backend.read_only) return;
        try self.persistManifest();
    }

    pub fn finalizeReadReaderRelease(self: *Backend) void {
        self.finalizeReadReaderReleaseKind(.other);
    }

    pub fn finalizeReadReaderReleaseKind(self: *Backend, kind: ReaderPinKind) void {
        self.releaseReaderKind(kind);
        const reclaimable_obsolete_paths = self.hasReclaimableObsoletePathsLocked();
        if ((!self.manifest_dirty and !self.obsolete_manifest_dirty and !reclaimable_obsolete_paths) or
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
        if (byte_threshold > 0) return stateMeetsByteFlushThreshold(state, byte_threshold);
        return state.entries.items.len >= self.effectiveFlushThreshold();
    }

    fn shouldFlushMutable(self: *const Backend) bool {
        if (self.mutable.entries.items.len == 0) return false;
        const byte_threshold = self.effectiveFlushThresholdBytes();
        if (byte_threshold > 0) return stateMeetsByteFlushThreshold(&self.mutable, byte_threshold);
        return self.mutable.entries.items.len >= self.effectiveFlushThreshold();
    }

    fn shouldFlushMutableForIdleLocked(self: *Backend) bool {
        return if (self.nextMutableIdleFlushDelayNsLocked()) |delay_ns| delay_ns == 0 else false;
    }

    fn shouldFlushMutableDuringRecoveryReplay(self: *const Backend) bool {
        if (self.mutable.entries.items.len == 0) return false;
        const byte_threshold = self.effectiveFlushThresholdBytes();
        if (byte_threshold > 0) return stateMeetsByteFlushThreshold(&self.mutable, byte_threshold);
        const recovery_threshold = @max(
            self.effectiveFlushThreshold(),
            self.options.recovery_replay_flush_threshold,
        );
        return self.mutable.entries.items.len >= recovery_threshold;
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
        return try self.cachedWalRetentionLocked();
    }

    // wal.snapshotRetention re-reads the checkpoint index and current segment
    // and fstats every retained segment. Serve read-only/background callers
    // from a short-lived cache, while every in-process mutation updates or
    // invalidates WalRetentionState. The cache is therefore exact with respect
    // to this Backend; the TTL only bounds visibility of out-of-process changes.
    const wal_retention_cache_ttl_ns: u64 = 250 * std.time.ns_per_ms;

    fn cachedWalRetentionLocked(self: *Backend) !wal_mod.RetentionStats {
        const now_ns = self.writeStatsNowNs();
        if (self.wal_retention.primary) |cached| {
            if (now_ns -| self.wal_retention.primary_ns < wal_retention_cache_ttl_ns) return cached;
        }
        const fresh = try wal_mod.snapshotRetention(self.storage.?, self.allocator, self.root_dir.?);
        self.wal_retention.primary = fresh;
        self.wal_retention.primary_ns = now_ns;
        return fresh;
    }

    fn cachedWalReplayRetentionLocked(self: *Backend) !wal_mod.RetentionStats {
        const now_ns = self.writeStatsNowNs();
        if (self.wal_retention.replay) |cached| {
            if (now_ns -| self.wal_retention.replay_ns < wal_retention_cache_ttl_ns) return cached;
        }
        const fresh = try wal_mod.snapshotReplayRetention(self.storage.?, self.allocator, self.root_dir.?);
        self.wal_retention.replay = fresh;
        self.wal_retention.replay_ns = now_ns;
        return fresh;
    }

    fn invalidatePrimaryWalRetentionCacheLocked(self: *Backend) void {
        self.wal_retention.invalidatePrimary();
    }

    // Retention enforcement rotates memtables and loops over flushes; even
    // with cached retention stats it is too heavy to run on every
    // maintenance step. Once per interval is plenty for an approximate
    // limit.
    fn walRetentionEnforceDue(self: *Backend) bool {
        const interval_ns: u64 = 250 * std.time.ns_per_ms;
        const now_ns = self.writeStatsNowNs();
        if (now_ns -| self.last_wal_retention_enforce_ns < interval_ns) return false;
        self.last_wal_retention_enforce_ns = now_ns;
        return true;
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
        return std.math.mul(usize, @max(@as(usize, 1), soft), 2) catch std.math.maxInt(usize);
    }

    const L0Pressure = struct {
        runs: usize = 0,
        bytes: u64 = 0,

        fn overHardLimit(self: @This(), hard_runs: usize, hard_bytes: u64) bool {
            return (hard_runs > 0 and self.runs > hard_runs) or
                (hard_bytes > 0 and self.bytes > hard_bytes);
        }

        fn runDebt(self: @This(), hard_runs: usize) u64 {
            if (hard_runs == 0 or self.runs <= hard_runs) return 0;
            return @intCast(self.runs - hard_runs);
        }

        fn byteDebt(self: @This(), hard_bytes: u64) u64 {
            if (hard_bytes == 0 or self.bytes <= hard_bytes) return 0;
            return self.bytes - hard_bytes;
        }
    };

    fn l0RunDebtForHardLimit(_: *const Backend, l0_runs: usize, hard_runs: usize) u64 {
        if (hard_runs == 0 or l0_runs <= hard_runs) return 0;
        return @intCast(l0_runs - hard_runs);
    }

    fn l0ByteDebtForHardLimit(_: *const Backend, l0_bytes: u64, hard_bytes: u64) u64 {
        if (hard_bytes == 0 or l0_bytes <= hard_bytes) return 0;
        return l0_bytes - hard_bytes;
    }

    fn snapshotL0PressureLocked(self: *const Backend) L0Pressure {
        var pressure = L0Pressure{};
        while (pressure.runs < self.runs.items.len and self.runs.items[pressure.runs].level == 0) : (pressure.runs += 1) {
            pressure.bytes += self.runs.items[pressure.runs].size_bytes;
        }
        return pressure;
    }

    fn enforceWritePressure(self: *Backend) anyerror!void {
        if (self.bulkIngestActive() and !self.writePressureDuringBulkIngestEnabled()) return;
        if (self.write_pressure_enforcing) return;
        self.write_pressure_enforcing = true;
        defer self.write_pressure_enforcing = false;

        try self.enforceWalRetentionHardPressure(true);

        const hard_runs = self.effectiveL0HardLimitRuns();
        const hard_bytes = self.options.l0_hard_limit_bytes;
        if (hard_runs == 0 and hard_bytes == 0) return;

        var pressure = self.snapshotL0PressureLocked();
        if (!pressure.overHardLimit(hard_runs, hard_bytes)) return;

        const start_ns = self.writeStatsNowNs();
        self.write_stats.write_pressure_events += 1;
        self.write_stats.write_pressure_l0_run_debt +|= pressure.runDebt(hard_runs);
        self.write_stats.write_pressure_l0_byte_debt +|= pressure.byteDebt(hard_bytes);
        const target_runs = if (self.options.l0_soft_limit_runs != 0) self.options.l0_soft_limit_runs else self.options.compact_threshold_runs;
        const before_compactions = self.compaction_stats.compactions;
        const max_steps = @max(@as(usize, 1), self.options.write_pressure_max_compaction_steps);
        var steps: usize = 0;
        while (pressure.overHardLimit(hard_runs, hard_bytes) and steps < max_steps) {
            const before_step_compactions = self.compaction_stats.compactions;
            try compaction_mod.compactL0ToLimit(Backend, self, target_runs);
            if (self.compaction_stats.compactions == before_step_compactions) break;
            steps += self.compaction_stats.compactions - before_step_compactions;
            pressure = self.snapshotL0PressureLocked();
        }

        const compaction_delta = self.compaction_stats.compactions - before_compactions;
        self.write_stats.write_pressure_compactions += @intCast(compaction_delta);
        self.write_stats.write_pressure_compaction_steps += @intCast(steps);
        self.write_stats.write_pressure_ns += self.writeStatsElapsedNs(start_ns);

        if (pressure.overHardLimit(hard_runs, hard_bytes)) {
            self.write_stats.write_pressure_overloads += 1;
            self.write_stats.write_pressure_overload_l0_run_debt +|= pressure.runDebt(hard_runs);
            self.write_stats.write_pressure_overload_l0_byte_debt +|= pressure.byteDebt(hard_bytes);
            if (self.options.write_pressure_reject_on_overload) {
                self.write_stats.write_pressure_rejections += 1;
                return error.WritePressureExceeded;
            }
        }
    }

    fn enforceWalRetentionHardPressureGuarded(self: *Backend) anyerror!void {
        if (self.bulkIngestActive()) return;
        if (self.write_pressure_enforcing) return;
        self.write_pressure_enforcing = true;
        defer self.write_pressure_enforcing = false;
        try self.enforceWalRetentionHardPressure(false);
    }

    fn enforceWalRetentionSoftPressureGuarded(self: *Backend) anyerror!void {
        if (!self.options.foreground_soft_wal_checkpoint) return;
        if (self.bulkIngestActive() or self.write_pressure_enforcing) return;
        if (!self.walRetentionEnforceDue()) return;
        const retention = try self.snapshotWalRetentionForPressureLocked() orelse return;
        if (!self.walRetentionOverSoftLimit(retention)) return;

        self.write_pressure_enforcing = true;
        defer self.write_pressure_enforcing = false;
        const start_ns = self.writeStatsNowNs();

        if (self.activeImmutableMemtableCount() == 0 and self.mutable.entries.items.len > 0) {
            try self.rotateMutableToImmutable();
        }

        const saved_budget = self.maintenance_io_budget_remaining;
        self.maintenance_io_budget_remaining = null;
        defer self.maintenance_io_budget_remaining = saved_budget;

        var flushes: u64 = 0;
        if (self.activeImmutableMemtableCount() > 0 and try self.flushOldestImmutableMemtable()) {
            flushes = 1;
        }
        if (flushes > 0) {
            self.write_stats.wal_pressure_flushes += flushes;
            self.write_stats.wal_pressure_ns += self.writeStatsElapsedNs(start_ns);
        }
    }

    fn enforceWalRetentionHardPressure(self: *Backend, throttle_background: bool) anyerror!void {
        // Best-effort maintenance is throttled, but foreground commit pressure
        // must always check the configured hard bound. Otherwise a fast second
        // commit can cross it inside the interval without forcing a checkpoint.
        if (throttle_background and !self.walRetentionEnforceDue()) return;
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
            // Flushes advance the checkpoint; re-read fresh so the loop sees
            // its own progress instead of the cached snapshot.
            self.invalidatePrimaryWalRetentionCacheLocked();
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

    pub fn shouldDrainMutableBeforeDirectBulkIngest(self: *const Backend, incoming: *const ActiveMemTable) bool {
        if (self.mutable.entries.items.len == 0) return false;
        if (self.active_bulk_ingest_batches <= 1) return false;
        if (self.activeImmutableMemtableCount() != 0) return false;
        const byte_threshold = self.effectiveFlushThresholdBytes();
        if (byte_threshold > 0) {
            const logical_bytes = estimateStateLogicalBytes(&self.mutable) +| estimateStateLogicalBytes(incoming);
            if (logical_bytes >= byte_threshold) return true;
            const memory_threshold = std.math.mul(
                u64,
                byte_threshold,
                mutable_memory_guard_multiplier,
            ) catch std.math.maxInt(u64);
            return estimateStateBytes(&self.mutable) +| estimateStateBytes(incoming) >= memory_threshold;
        }
        return self.mutable.entries.items.len + incoming.entries.items.len >= self.effectiveFlushThreshold();
    }

    fn writePressureDuringBulkIngestEnabled(self: *const Backend) bool {
        return self.options.write_pressure_during_bulk_ingest or writePressureDuringBulkIngestEnvEnabled();
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
            if (self.active_bulk_ingest_batches == 0 and self.root_dir != null and (self.manifest_dirty or self.obsolete_manifest_dirty or self.hasReclaimableObsoletePathsLocked())) {
                try self.persistManifest();
            }
            self.scheduleMaintenanceAfterBulkIngestLocked();
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
            self.scheduleMaintenanceAfterBulkIngestLocked();
        }
    }

    fn scheduleMaintenanceAfterBulkIngestLocked(self: *Backend) void {
        if (self.active_bulk_ingest_batches == 0) {
            self.scheduleMaintenanceJobIfNeededLocked();
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
        self.scheduleMaintenanceAfterBulkIngestLocked();
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
        if (self.root_dir != null and self.hasReclaimableObsoletePathsLocked()) {
            try self.persistManifest();
            _ = self.refreshCachedMaintenanceHintLocked();
            if (!options.force_soft_compaction and !self.options.foreground_soft_compaction) return;
        }
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

    fn parseRunIdFromTableFileName(name: []const u8) ?u64 {
        if (!std.mem.endsWith(u8, name, ".tbl")) return null;
        const stem = name[0 .. name.len - ".tbl".len];
        if (stem.len == 0) return null;
        return std.fmt.parseUnsigned(u64, stem, 10) catch null;
    }

    fn parseRunIdFromRecoveredTableTempFileName(name: []const u8) ?u64 {
        const marker = ".tbl.tmp-";
        const marker_index = std.mem.indexOf(u8, name, marker) orelse return null;
        if (marker_index == 0) return null;
        const nonce = name[marker_index + marker.len ..];
        if (nonce.len == 0) return null;
        _ = std.fmt.parseUnsigned(u64, nonce, 10) catch return null;
        return std.fmt.parseUnsigned(u64, name[0..marker_index], 10) catch null;
    }

    fn runIdTrackedByManifestLocked(self: *Backend, run_id: u64) bool {
        for (self.runs.items) |run| {
            if (run.id == run_id) return true;
        }
        return false;
    }

    fn pathTrackedByActiveRunsLocked(self: *Backend, path: []const u8) bool {
        for (self.runs.items) |run| {
            if (run.path) |active| {
                if (std.mem.eql(u8, active, path)) return true;
            }
        }
        return false;
    }

    fn pathTrackedByManifestLocked(self: *Backend, path: []const u8) bool {
        if (self.pathTrackedByActiveRunsLocked(path)) return true;
        for (self.obsolete_paths.items) |obsolete| {
            if (std.mem.eql(u8, obsolete.path, path)) return true;
        }
        return false;
    }

    pub fn cleanupRecoveredRunFilesForManifest(self: *Backend) !RecoveredRunFileCleanupStats {
        const root_dir = self.root_dir orelse return .{};
        if (self.storage == null or self.options.backend.read_only) return .{};
        if (!std.fs.path.isAbsolute(root_dir)) return .{};

        const runs_dir = try std.fs.path.join(self.allocator, &.{ root_dir, "runs" });
        defer self.allocator.free(runs_dir);

        var io_impl = std.Io.Threaded.init(self.allocator, .{});
        defer io_impl.deinit();

        var dir = std.Io.Dir.cwd().openDir(io_impl.io(), runs_dir, .{ .iterate = true }) catch |err| switch (err) {
            error.FileNotFound => return .{},
            else => return err,
        };
        defer dir.close(io_impl.io());

        var stats = RecoveredRunFileCleanupStats{};
        var it = dir.iterate();
        while (try it.next(io_impl.io())) |entry| {
            if (entry.kind != .file) continue;
            _ = parseRunIdFromRecoveredTableTempFileName(entry.name) orelse continue;

            const path = try std.fs.path.join(self.allocator, &.{ runs_dir, entry.name });
            defer self.allocator.free(path);
            const size = self.storage.?.fileSize(path) catch 0;
            repository_mod.deleteFileAbsoluteWithStorage(self.storage.?, path) catch |err| switch (err) {
                error.FileNotFound => {},
                else => return err,
            };
            stats.files_deleted += 1;
            stats.bytes_deleted += size;
        }
        return stats;
    }

    pub fn cleanupOrphanedRunFilesForManifest(self: *Backend) !RecoveredRunFileCleanupStats {
        const root_dir = self.root_dir orelse return .{};
        if (self.storage == null or self.options.backend.read_only) return .{};
        if (!std.fs.path.isAbsolute(root_dir)) return .{};

        const runs_dir = try std.fs.path.join(self.allocator, &.{ root_dir, "runs" });
        defer self.allocator.free(runs_dir);

        var io_impl = std.Io.Threaded.init(self.allocator, .{});
        defer io_impl.deinit();

        var dir = std.Io.Dir.cwd().openDir(io_impl.io(), runs_dir, .{ .iterate = true }) catch |err| switch (err) {
            error.FileNotFound => return .{},
            else => return err,
        };
        defer dir.close(io_impl.io());

        var stats: RecoveredRunFileCleanupStats = .{};
        var it = dir.iterate();
        while (try it.next(io_impl.io())) |entry| {
            if (entry.kind != .file) continue;
            const run_id = parseRunIdFromTableFileName(entry.name) orelse continue;
            if (self.runIdTrackedByManifestLocked(run_id)) continue;

            const path = try std.fs.path.join(self.allocator, &.{ runs_dir, entry.name });
            defer self.allocator.free(path);
            if (self.pathTrackedByManifestLocked(path) or self.obsoletePathPinnedByOpenVersion(path)) continue;

            const size = self.storage.?.fileSize(path) catch 0;
            repository_mod.deleteFileAbsoluteWithStorage(self.storage.?, path) catch |err| switch (err) {
                error.FileNotFound => {},
                else => return err,
            };
            stats.files_deleted +|= 1;
            stats.bytes_deleted +|= size;
        }
        return stats;
    }

    fn reconcileObsoletePathsForManifest(self: *Backend) !void {
        const now_ns = self.nowNs();
        var i: usize = 0;
        while (i < self.obsolete_paths.items.len) {
            const obsolete = &self.obsolete_paths.items[i];
            if (self.pathTrackedByActiveRunsLocked(obsolete.path) or self.obsoletePathPinnedByOpenVersion(obsolete.path)) {
                i += 1;
                continue;
            }
            if (obsolete.delete_after_ns > now_ns) {
                i += 1;
                continue;
            }

            repository_mod.deleteFileAbsoluteWithStorage(self.storage.?, obsolete.path) catch |err| switch (err) {
                error.FileNotFound => {},
                else => {
                    self.obsolete_delete_failures +|= 1;
                    self.obsolete_delete_retries +|= 1;
                    obsolete.delete_after_ns = now_ns +| self.options.obsolete_delete_retry_ns;
                    self.obsolete_manifest_dirty = true;
                    i += 1;
                    continue;
                },
            };
            run_snapshot_refs.forget(obsolete.path);
            var removed = self.obsolete_paths.orderedRemove(i);
            removed.deinit(self.allocator);
            self.obsolete_manifest_dirty = true;
        }
    }

    fn hasReclaimableObsoletePathsLocked(self: *Backend) bool {
        if (self.bulkIngestActive() or self.obsolete_paths.items.len == 0) return false;
        if (self.root_dir == null or self.storage == null or self.options.backend.read_only) return false;
        const now_ns = self.nowNs();
        for (self.obsolete_paths.items) |obsolete| {
            if (self.pathTrackedByActiveRunsLocked(obsolete.path) or self.obsoletePathPinnedByOpenVersion(obsolete.path)) continue;
            if (obsolete.delete_after_ns <= now_ns) return true;
        }
        return false;
    }

    pub fn nextObsoleteReclaimDelayNsBestEffort(self: *Backend) ?u64 {
        if (!self.mu.tryLock()) return null;
        defer self.mu.unlock();
        return self.nextObsoleteReclaimDelayNsLocked();
    }

    pub fn nextMaintenanceWakeDelayNsBestEffort(self: *Backend) ?u64 {
        if (!self.mu.tryLock()) return null;
        defer self.mu.unlock();

        var delay_ns = self.nextObsoleteReclaimDelayNsLocked();
        if (self.nextMutableIdleFlushDelayNsLocked()) |candidate| {
            delay_ns = if (delay_ns) |current| @min(current, candidate) else candidate;
        }
        return delay_ns;
    }

    fn nextMutableIdleFlushDelayNsLocked(self: *Backend) ?u64 {
        if (self.options.mutable_idle_flush_after_ns == 0 or
            self.mutable_idle_flush_deadline_ns == 0 or
            self.mutable.entries.items.len == 0 or
            self.root_dir == null or
            self.storage == null or
            self.options.backend.read_only or
            self.bulkIngestActive()) return null;

        const now_ns = self.nowNs();
        const delay_ns = if (self.mutable_idle_flush_deadline_ns <= now_ns)
            0
        else
            self.mutable_idle_flush_deadline_ns - now_ns;
        if (delay_ns == 0) self.cached_maintenance_hint.store(1, .release);
        return delay_ns;
    }

    fn nextObsoleteReclaimDelayNsLocked(self: *Backend) ?u64 {
        if (self.obsolete_paths.items.len == 0) return null;
        if (self.root_dir == null or self.storage == null or self.options.backend.read_only) return null;
        if (self.bulkIngestActive()) return null;

        const now_ns = self.nowNs();
        var delay_ns: ?u64 = null;
        var due_now = false;
        for (self.obsolete_paths.items) |obsolete| {
            if (self.pathTrackedByActiveRunsLocked(obsolete.path) or self.obsoletePathPinnedByOpenVersion(obsolete.path)) continue;
            const candidate = if (obsolete.delete_after_ns <= now_ns) 0 else obsolete.delete_after_ns - now_ns;
            if (candidate == 0) due_now = true;
            delay_ns = if (delay_ns) |current| @min(current, candidate) else candidate;
        }
        if (due_now) self.cached_maintenance_hint.store(1, .release);
        return delay_ns;
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

const InternalFlushWorker = if (builtin.os.tag == .freestanding or builtin.single_threaded) struct {
    backend: *Backend,

    const Stats = struct {
        wakeups: u64 = 0,
        maintenance_steps: u64 = 0,
        errors: u64 = 0,
        joined: bool = false,
    };

    fn init(backend: *Backend) InternalFlushWorker {
        return .{ .backend = backend };
    }

    fn start(_: *InternalFlushWorker) !void {
        return error.UnsupportedPlatform;
    }

    fn stopAndJoin(_: *InternalFlushWorker, _: bool) void {}

    fn waker(self: *InternalFlushWorker) MaintenanceWaker {
        return .{ .ptr = self, .wake_fn = wake };
    }

    fn wake(_: *anyopaque) void {}

    fn snapshotStats(_: *InternalFlushWorker) Stats {
        return .{};
    }
} else struct {
    backend: *Backend,
    mutex: std.atomic.Mutex = .unlocked,
    thread: ?std.Thread = null,
    stop_requested: bool = false,
    drain_on_stop: bool = false,
    wake_requested: bool = false,
    wakeups: u64 = 0,
    maintenance_steps: u64 = 0,
    errors: u64 = 0,
    joined: bool = false,

    const idle_obsolete_reclaim_poll_ns = 250 * std.time.ns_per_ms;
    const idle_wake_poll_ns = 2 * std.time.ns_per_ms;

    const Stats = struct {
        wakeups: u64 = 0,
        maintenance_steps: u64 = 0,
        errors: u64 = 0,
        joined: bool = false,
    };

    fn init(backend: *Backend) InternalFlushWorker {
        return .{ .backend = backend };
    }

    fn start(self: *InternalFlushWorker) !void {
        self.thread = try std.Thread.spawn(.{}, run, .{self});
    }

    fn stopAndJoin(self: *InternalFlushWorker, drain: bool) void {
        lockWorkerMutex(&self.mutex);
        self.stop_requested = true;
        self.drain_on_stop = self.drain_on_stop or drain;
        self.wake_requested = true;
        self.mutex.unlock();

        if (self.thread) |thread| {
            thread.join();
            self.thread = null;
            self.joined = true;
        }
    }

    fn waker(self: *InternalFlushWorker) MaintenanceWaker {
        return .{ .ptr = self, .wake_fn = wake };
    }

    fn wake(ptr: *anyopaque) void {
        const self: *InternalFlushWorker = @ptrCast(@alignCast(ptr));
        lockWorkerMutex(&self.mutex);
        self.wake_requested = true;
        self.wakeups +|= 1;
        self.mutex.unlock();
    }

    fn snapshotStats(self: *InternalFlushWorker) Stats {
        lockWorkerMutex(&self.mutex);
        defer self.mutex.unlock();
        return .{
            .wakeups = self.wakeups,
            .maintenance_steps = self.maintenance_steps,
            .errors = self.errors,
            .joined = self.joined,
        };
    }

    fn run(self: *InternalFlushWorker) void {
        var next_reclaim_delay_ns: ?u64 = null;
        while (true) {
            const drain_then_stop = self.waitForWork(next_reclaim_delay_ns);
            if (drain_then_stop) {
                _ = self.drainMaintenance();
                return;
            }
            next_reclaim_delay_ns = self.drainMaintenance();
        }
    }

    fn waitForWork(self: *InternalFlushWorker, initial_reclaim_delay_ns: ?u64) bool {
        var reclaim_delay_ns = initial_reclaim_delay_ns;
        while (true) {
            lockWorkerMutex(&self.mutex);
            if (self.stop_requested or self.wake_requested) break;
            self.mutex.unlock();

            if (reclaim_delay_ns) |delay_ns| {
                if (delay_ns == 0) return false;
                const sleep_ns = @min(delay_ns, idle_obsolete_reclaim_poll_ns);
                sleepForTest(sleep_ns);
                reclaim_delay_ns = if (delay_ns <= sleep_ns) 0 else delay_ns - sleep_ns;
            } else {
                sleepForTest(idle_wake_poll_ns);
            }
        }
        defer self.mutex.unlock();
        const drain_then_stop = self.stop_requested and self.drain_on_stop;
        if (self.stop_requested and !drain_then_stop) return true;
        self.wake_requested = false;
        return drain_then_stop;
    }

    fn drainMaintenance(self: *InternalFlushWorker) ?u64 {
        var steps: usize = 0;
        while (steps < 64) : (steps += 1) {
            const progressed = self.backend.runMaintenanceStep() catch |err| {
                self.recordError(err);
                return null;
            };
            if (!progressed) return self.backend.nextMaintenanceWakeDelayNsBestEffort();
            self.recordStep();
        }
        lockWorkerMutex(&self.mutex);
        self.wake_requested = true;
        self.mutex.unlock();
        return null;
    }

    fn recordStep(self: *InternalFlushWorker) void {
        lockWorkerMutex(&self.mutex);
        self.maintenance_steps +|= 1;
        self.mutex.unlock();
    }

    fn recordError(self: *InternalFlushWorker, err: anyerror) void {
        lockWorkerMutex(&self.mutex);
        self.errors +|= 1;
        self.mutex.unlock();
        std.log.warn("lsm internal flush worker failed root={?s} err={}", .{ self.backend.root_dir, err });
    }
};

fn lockWorkerMutex(mutex: *std.atomic.Mutex) void {
    platform.sync.lockYielding(mutex);
}

pub const InternalFlushWorkerStats = InternalFlushWorker.Stats;

pub const BackendHandle = struct {
    allocator: Allocator,
    backend: *Backend,
    background_runtime: ?background_runtime_mod.BackendRuntimeHandle = null,
    internal_flush_worker: ?*InternalFlushWorker = null,

    pub fn init(allocator: Allocator, options: Options) !BackendHandle {
        return try initWithConfig(allocator, options, .{});
    }

    pub fn initWithConfig(allocator: Allocator, options: Options, config: BackendHandleConfig) !BackendHandle {
        const backend = try allocator.create(Backend);
        errdefer allocator.destroy(backend);

        var owned_runtime: ?background_runtime_mod.BackendRuntimeHandle = null;
        errdefer if (owned_runtime) |*runtime| runtime.deinit();
        var internal_flush_worker: ?*InternalFlushWorker = null;
        errdefer if (internal_flush_worker) |worker| {
            worker.stopAndJoin(true);
            allocator.destroy(worker);
        };

        var resolved_options = options;
        if (config.background_runtime) |runtime_config| {
            if (resolved_options.background_executor != null) return error.BackgroundExecutorAlreadyConfigured;
            owned_runtime = try background_runtime_mod.BackendRuntimeHandle.init(allocator, runtime_config);
            const runtime = owned_runtime.?.ptr();
            const executor = BackgroundExecutor.init(runtime, try runtime.allocOwnerId());
            resolved_options.background_executor = &executor;
            if (resolved_options.read_runtime == null) {
                if (runtime.io()) |io| resolved_options.read_runtime = storage_io.ReadRuntime.init(io);
            }
        }

        backend.* = Backend.init(allocator, resolved_options);
        if (config.internal_flush_worker) {
            internal_flush_worker = try startInternalFlushWorker(allocator, backend);
        }
        return .{
            .allocator = allocator,
            .backend = backend,
            .background_runtime = owned_runtime,
            .internal_flush_worker = internal_flush_worker,
        };
    }

    pub fn open(allocator: Allocator, root_dir: []const u8, options: Options) !BackendHandle {
        return try openWithConfig(allocator, root_dir, options, .{});
    }

    pub fn openWithConfig(allocator: Allocator, root_dir: []const u8, options: Options, config: BackendHandleConfig) !BackendHandle {
        const backend = try allocator.create(Backend);
        errdefer allocator.destroy(backend);

        var owned_runtime: ?background_runtime_mod.BackendRuntimeHandle = null;
        errdefer if (owned_runtime) |*runtime| runtime.deinit();
        var internal_flush_worker: ?*InternalFlushWorker = null;
        errdefer if (internal_flush_worker) |worker| {
            worker.stopAndJoin(true);
            allocator.destroy(worker);
        };

        var resolved_options = options;
        if (config.background_runtime) |runtime_config| {
            if (resolved_options.background_executor != null) return error.BackgroundExecutorAlreadyConfigured;
            owned_runtime = try background_runtime_mod.BackendRuntimeHandle.init(allocator, runtime_config);
            const runtime = owned_runtime.?.ptr();
            const executor = BackgroundExecutor.init(runtime, try runtime.allocOwnerId());
            resolved_options.background_executor = &executor;
            if (resolved_options.read_runtime == null) {
                if (runtime.io()) |io| resolved_options.read_runtime = storage_io.ReadRuntime.init(io);
            }
        }

        try backend.openInto(allocator, root_dir, resolved_options);
        if (config.internal_flush_worker) {
            internal_flush_worker = try startInternalFlushWorker(allocator, backend);
        }
        return .{
            .allocator = allocator,
            .backend = backend,
            .background_runtime = owned_runtime,
            .internal_flush_worker = internal_flush_worker,
        };
    }

    pub fn close(self: *BackendHandle) void {
        if (self.internal_flush_worker) |worker| {
            worker.stopAndJoin(true);
            self.backend.options.maintenance_waker = null;
            self.allocator.destroy(worker);
            self.internal_flush_worker = null;
        }
        self.backend.close();
        self.allocator.destroy(self.backend);
        if (self.background_runtime) |*runtime| runtime.deinit();
        self.* = undefined;
    }

    pub fn abandonAfterCrash(self: *BackendHandle) void {
        if (self.internal_flush_worker) |worker| {
            worker.stopAndJoin(false);
            self.backend.options.maintenance_waker = null;
            self.allocator.destroy(worker);
            self.internal_flush_worker = null;
        }
        self.backend.abandonAfterCrash();
        self.allocator.destroy(self.backend);
        if (self.background_runtime) |*runtime| runtime.deinit();
        self.* = undefined;
    }

    pub fn ptr(self: *BackendHandle) *Backend {
        return self.backend;
    }

    pub fn snapshotMaintenanceStats(self: *const BackendHandle) Backend.MaintenanceStats {
        return self.backend.snapshotMaintenanceStats();
    }

    pub fn ownedBackgroundRuntime(self: *BackendHandle) ?*background_runtime_mod.BackendRuntime {
        return if (self.background_runtime) |*runtime| runtime.ptr() else null;
    }

    pub fn stopInternalFlushWorkerForTest(self: *BackendHandle) ?InternalFlushWorkerStats {
        const worker = self.internal_flush_worker orelse return null;
        worker.stopAndJoin(true);
        self.backend.options.maintenance_waker = null;
        return worker.snapshotStats();
    }

    pub fn internalFlushWorkerStats(self: *BackendHandle) ?InternalFlushWorkerStats {
        const worker = self.internal_flush_worker orelse return null;
        return worker.snapshotStats();
    }

    fn startInternalFlushWorker(allocator: Allocator, backend: *Backend) !*InternalFlushWorker {
        if (backend.options.maintenance_waker != null) return error.MaintenanceWakerAlreadyConfigured;
        const worker = try allocator.create(InternalFlushWorker);
        errdefer allocator.destroy(worker);
        worker.* = InternalFlushWorker.init(backend);
        backend.options.maintenance_waker = worker.waker();
        errdefer backend.options.maintenance_waker = null;
        try worker.start();
        return worker;
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
            run.path = try repository_mod.persistRunFileWithStorage(dest.storage.?, dest.allocator, root_dir, &run, dest.options.table_block_compression, dest.options.table_prefix_extractor);
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

fn appendSyntheticLevelRunsForTest(backend: *Backend, level: u32, count: usize, size_bytes: u64) !void {
    for (0..count) |idx| {
        try backend.runs.append(backend.allocator, .{
            .id = @intCast(idx + 1),
            .level = level,
            .size_bytes = size_bytes,
            .path = null,
            .smallest_namespace_name = null,
            .smallest_key = &.{},
            .largest_namespace_name = null,
            .largest_key = &.{},
            .entry_count = 1,
            .bloom_filter = null,
            .owns_metadata = false,
            .state = null,
        });
    }
}

fn appendStateLevelRunsForTest(backend: *Backend, level: u32, count: usize) !void {
    for (0..count) |idx| {
        var state: State = .{};
        errdefer state.deinit(backend.allocator);
        var key_buf: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "doc:{d:0>4}", .{idx});
        var value_buf: [32]u8 = undefined;
        const value = try std.fmt.bufPrint(&value_buf, "V{d}", .{idx});
        try state.appendUpsert(backend.allocator, .{ .name = "docs" }, key, value, false);
        const run = try compaction_mod.makeRunAtLevel(Backend, backend, state, level);
        state = .{};
        try backend.runs.append(backend.allocator, run);
    }
    compaction_mod.sortRuns(backend.runs.items);
}

fn sleepForTest(duration_ns: u64) void {
    if (comptime builtin.os.tag == .freestanding) return;
    var req = std.posix.timespec{
        .sec = @intCast(duration_ns / std.time.ns_per_s),
        .nsec = @intCast(duration_ns % std.time.ns_per_s),
    };
    while (true) switch (std.posix.errno(std.posix.system.nanosleep(&req, &req))) {
        .SUCCESS => return,
        .INTR => continue,
        else => return,
    };
}

fn pathExistsForTest(path: []const u8) bool {
    std.Io.Dir.cwd().access(std.testing.io, path, .{}) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return false,
    };
    return true;
}

fn writeMarkerForTest(path: []const u8) !void {
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = path, .data = "1" });
}

fn waitForPathForTest(path: []const u8, timeout_ns: u64) !void {
    var waited_ns: u64 = 0;
    const poll_ns: u64 = 5 * std.time.ns_per_ms;
    while (waited_ns < timeout_ns) : (waited_ns += poll_ns) {
        if (pathExistsForTest(path)) return;
        sleepForTest(poll_ns);
    }
    return error.TestTimedOutWaitingForPath;
}

fn makePipeForTest() ![2]std.posix.fd_t {
    var fds: [2]std.posix.fd_t = undefined;
    while (true) switch (std.posix.errno(std.posix.system.pipe(&fds))) {
        .SUCCESS => return fds,
        .INTR => continue,
        else => return error.Unexpected,
    };
}

fn writeSignalForTest(fd: std.posix.fd_t) !void {
    const byte: [1]u8 = .{1};
    while (true) {
        const rc = std.posix.system.write(fd, &byte, byte.len);
        switch (std.posix.errno(rc)) {
            .SUCCESS => {
                if (rc != 1) return error.Unexpected;
                return;
            },
            .INTR => continue,
            else => return error.Unexpected,
        }
    }
}

fn waitSignalForTest(fd: std.posix.fd_t) !void {
    var byte: [1]u8 = undefined;
    while (true) {
        const rc = std.posix.system.read(fd, &byte, byte.len);
        switch (std.posix.errno(rc)) {
            .SUCCESS => {
                if (rc != 1) return error.Unexpected;
                return;
            },
            .INTR => continue,
            else => return error.Unexpected,
        }
    }
}

test "lsm backend default base level target absorbs L0 pressure output" {
    var backend = Backend.init(std.testing.allocator, .{});
    defer backend.close();
    try appendSyntheticLevelRunsForTest(&backend, 1, 28, 20 * 1024);

    const stats = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(@as(u64, 0), stats.level_overflow_runs);
    try std.testing.expectEqual(@as(u64, 0), stats.level_overflow_bytes);
    try std.testing.expectEqual(@as(u64, 28), stats.lower_level_runs);
}

test "lsm backend caches exact L0 overlap score until immutable run IDs change" {
    var backend = Backend.init(std.testing.allocator, .{
        .l0_overlap_compact_threshold_runs = 2,
        .l0_hard_limit_runs = 16,
    });
    defer backend.close();
    try appendSyntheticLevelRunsForTest(&backend, 0, 4, 1024);
    compaction_mod.sortRuns(backend.runs.items);

    var stats = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(@as(u64, 4), stats.overlapping_l0_runs);
    try std.testing.expect(backend.l0_overlap_cache.valid);
    try std.testing.expectEqual(@as(usize, 4), backend.l0_overlap_cache.run_count);

    // Run metadata is immutable for an ID. Changing the complete ID sequence
    // models publication of a different run set with the same count and must
    // invalidate the cached result rather than relying on length alone.
    backend.runs.items[0].id = 99;
    stats = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(@as(u64, 4), stats.overlapping_l0_runs);
    try std.testing.expectEqual(@as(u64, 99), backend.l0_overlap_cache.run_ids[0]);

    backend.runs.items[0].level = 1;
    compaction_mod.sortRuns(backend.runs.items);
    stats = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(@as(u64, 3), stats.overlapping_l0_runs);
    try std.testing.expectEqual(@as(usize, 3), backend.l0_overlap_cache.run_count);
}

test "lsm backend bounds exact overlap scoring above soft L0 pressure" {
    var backend = Backend.init(std.testing.allocator, .{
        .compact_threshold_runs = 4,
        .l0_overlap_compact_threshold_runs = 2,
        .l0_soft_limit_runs = 4,
        .l0_hard_limit_runs = 16,
    });
    defer backend.close();
    try appendSyntheticLevelRunsForTest(&backend, 0, 5, 1024);
    compaction_mod.sortRuns(backend.runs.items);

    const stats = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(@as(u64, 5), stats.l0_runs);
    try std.testing.expectEqual(@as(u64, 1), stats.compactable_l0_runs);
    try std.testing.expectEqual(@as(u64, 0), stats.overlapping_l0_runs);
    try std.testing.expect(!backend.l0_overlap_cache.valid);
    try std.testing.expect(backend.maintenanceScore() > 0);
}

test "lsm backend tight base level target reports lower-level overflow" {
    var backend = Backend.init(std.testing.allocator, .{
        .level_target_runs_base = 4,
        .level_target_bytes_base = 128 * 1024,
    });
    defer backend.close();
    try appendSyntheticLevelRunsForTest(&backend, 1, 28, 20 * 1024);

    const stats = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(@as(u64, 24), stats.level_overflow_runs);
    try std.testing.expect(stats.level_overflow_bytes > 0);
}

test "lsm backend scheduled compaction admits lower-level plans over scheduler run-id stack cap" {
    var backend = Backend.init(std.testing.allocator, .{
        .level_target_runs_base = 32,
        .level_target_runs_multiplier = 4,
        .compaction_scheduler = .{
            .max_concurrent_jobs = 1,
            .resource_reservation_bytes = 0,
        },
    });
    defer backend.close();

    try appendStateLevelRunsForTest(&backend, 1, 128);
    const before_runs = countLevelRuns(backend.runs.items, 1);
    try std.testing.expectEqual(@as(usize, 128), before_runs);

    const progressed = blk: {
        const locked = runtime_mod.lockBackend(Backend, &backend);
        defer runtime_mod.unlockBackend(Backend, &backend, locked);
        break :blk try compaction_mod.maybeCompactRunsScheduled(Backend, &backend, 1);
    };
    try std.testing.expect(progressed);
    try std.testing.expect(countLevelRuns(backend.runs.items, 1) < before_runs);

    const maintenance = backend.snapshotMaintenanceStats();
    try std.testing.expect(maintenance.compaction_scheduler_grants > 0);
    try std.testing.expectEqual(maintenance.compaction_scheduler_grants, maintenance.compaction_scheduler_completions);
    try std.testing.expectEqual(@as(u64, 0), maintenance.compaction_scheduler_conflict_denials);
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

test "lsm backend heap handle can own a detached background runtime" {
    if (builtin.os.tag == .freestanding or builtin.single_threaded) return;

    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var handle = try BackendHandle.openWithConfig(
        std.testing.allocator,
        "/lsm-handle-owned-runtime-install-test",
        .{ .storage = storage.storage() },
        .{ .background_runtime = .{ .backend = .io_threaded } },
    );
    defer handle.close();

    const backend = handle.ptr();
    try std.testing.expect(handle.ownedBackgroundRuntime() != null);
    try std.testing.expect(backend.background_executor.canRunDetached());
    try std.testing.expect(backend.background_executor.jobs != null);
    try std.testing.expect(backend.background_executor.owner_id != 0);
}

test "lsm backend heap handle owned runtime wakes deferred immutable flush" {
    if (builtin.os.tag == .freestanding or builtin.single_threaded) return;

    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var handle = try BackendHandle.openWithConfig(
        std.testing.allocator,
        "/lsm-handle-owned-runtime-flush-test",
        .{
            .storage = storage.storage(),
            .flush_threshold = 1,
            .defer_flush_on_commit = true,
        },
        .{ .background_runtime = .{ .backend = .io_threaded } },
    );
    defer handle.close();

    const backend = handle.ptr();
    {
        var txn = try backend.beginWrite();
        try txn.put(.{}, "key", "value");
        try txn.commit();
    }

    var attempts: usize = 0;
    while (attempts < 200) : (attempts += 1) {
        _ = try backend.background_executor.poll(8);
        const maintenance = backend.snapshotMaintenanceStats();
        if (maintenance.immutable_memtables == 0 and maintenance.total_runs > 0) break;
        sleepForTest(5 * std.time.ns_per_ms);
    }

    const maintenance = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(@as(u64, 0), maintenance.immutable_memtables);
    try std.testing.expect(maintenance.total_runs > 0);
    try std.testing.expectEqualStrings("value", try backend.getMergedWithMutable(&backend.mutable, .{}, "key"));
}

test "lsm backend heap handle internal flush worker wakes deferred immutable flush" {
    if (builtin.os.tag == .freestanding or builtin.single_threaded) return;

    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var handle = try BackendHandle.openWithConfig(
        std.testing.allocator,
        "/lsm-handle-internal-flush-worker-wake-test",
        .{
            .storage = storage.storage(),
            .flush_threshold = 1,
            .defer_flush_on_commit = true,
        },
        .{ .internal_flush_worker = true },
    );
    defer handle.close();

    const backend = handle.ptr();
    try std.testing.expect(backend.options.maintenance_waker != null);
    try std.testing.expect(!backend.background_executor.canRunDetached());

    {
        var txn = try backend.beginWrite();
        try txn.put(.{}, "key", "value");
        try txn.commit();
    }

    var attempts: usize = 0;
    while (attempts < 200) : (attempts += 1) {
        const maintenance = backend.snapshotMaintenanceStats();
        if (maintenance.immutable_memtables == 0 and maintenance.total_runs > 0) break;
        sleepForTest(5 * std.time.ns_per_ms);
    }

    const worker_stats = handle.stopInternalFlushWorkerForTest().?;
    const maintenance = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(@as(u64, 0), maintenance.immutable_memtables);
    try std.testing.expect(maintenance.total_runs > 0);
    try std.testing.expect(worker_stats.wakeups > 0);
    try std.testing.expect(worker_stats.maintenance_steps > 0);
    try std.testing.expectEqual(@as(u64, 0), worker_stats.errors);
    try std.testing.expectEqualStrings("value", try backend.getMergedWithMutable(&backend.mutable, .{}, "key"));
}

test "lsm backend heap handle internal flush worker stop joins after final drain" {
    if (builtin.os.tag == .freestanding or builtin.single_threaded) return;

    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var handle = try BackendHandle.openWithConfig(
        std.testing.allocator,
        "/lsm-handle-internal-flush-worker-stop-test",
        .{
            .storage = storage.storage(),
            .flush_threshold = 1,
            .defer_flush_on_commit = true,
        },
        .{ .internal_flush_worker = true },
    );
    defer handle.close();

    const backend = handle.ptr();
    {
        var txn = try backend.beginWrite();
        try txn.put(.{}, "key", "value");
        try txn.commit();
    }

    const worker_stats = handle.stopInternalFlushWorkerForTest().?;
    const maintenance = backend.snapshotMaintenanceStats();
    try std.testing.expect(worker_stats.joined);
    try std.testing.expect(worker_stats.wakeups > 0);
    try std.testing.expectEqual(@as(u64, 0), worker_stats.errors);
    try std.testing.expectEqual(@as(u64, 0), maintenance.immutable_memtables);
    try std.testing.expect(maintenance.total_runs > 0);
    try std.testing.expect(backend.options.maintenance_waker == null);
}

test "lsm backend heap handle internal flush worker compacts soft L0 pressure" {
    if (builtin.os.tag == .freestanding or builtin.single_threaded) return;

    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var handle = try BackendHandle.openWithConfig(
        std.testing.allocator,
        "/lsm-handle-internal-flush-worker-pressure-test",
        .{
            .storage = storage.storage(),
            .flush_threshold = 1,
            .compact_threshold_runs = 100,
            .l0_soft_limit_runs = 1,
            .l0_hard_limit_runs = 100,
        },
        .{ .internal_flush_worker = true },
    );
    defer handle.close();

    const backend = handle.ptr();
    for (0..6) |i| {
        var key_buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "key:{d}", .{i});
        var txn = try backend.beginWrite();
        try txn.put(.{}, key, "value");
        try txn.commit();
    }

    var maintenance = backend.snapshotMaintenanceStats();
    var attempts: usize = 0;
    while (attempts < 200) : (attempts += 1) {
        maintenance = backend.snapshotMaintenanceStats();
        if (maintenance.compaction_scheduler_grants > 0 and maintenance.l0_runs <= 1) break;
        sleepForTest(5 * std.time.ns_per_ms);
    }

    const worker_stats = handle.stopInternalFlushWorkerForTest().?;
    maintenance = backend.snapshotMaintenanceStats();
    try std.testing.expect(worker_stats.wakeups > 0);
    try std.testing.expect(worker_stats.maintenance_steps > 0);
    try std.testing.expectEqual(@as(u64, 0), worker_stats.errors);
    try std.testing.expect(maintenance.compaction_scheduler_grants > 0);
    try std.testing.expect(maintenance.l0_runs <= 1);
    try std.testing.expectEqualStrings("value", try backend.getMergedWithMutable(&backend.mutable, .{}, "key:0"));
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
            .close_owner = drainOwner,
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
            .close_owner = drainOwner,
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
            .close_owner = drainOwner,
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
            .close_owner = drainOwner,
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

test "lsm backend deferred immutable queue enforces aggregate byte limit" {
    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var backend = try Backend.open(std.testing.allocator, "/lsm-background-queue-byte-limit-test", .{
        .storage = storage.storage(),
        .flush_threshold = 1,
        .defer_flush_on_commit = true,
        .max_deferred_immutable_memtables = 8,
    });
    defer backend.close();

    {
        var txn = try backend.beginWrite();
        try txn.put(.{}, "key:a", "a");
        try txn.commit();
    }
    try std.testing.expectEqual(@as(usize, 1), backend.activeImmutableMemtableCount());
    const one_memtable_bytes = backend.activeImmutableMemtableBytes();
    try std.testing.expect(one_memtable_bytes > 0);
    backend.options.max_deferred_immutable_bytes = one_memtable_bytes;

    {
        var txn = try backend.beginWrite();
        try txn.put(.{}, "key:b", "b");
        try txn.commit();
    }

    try std.testing.expectEqual(@as(usize, 1), backend.activeImmutableMemtableCount());
    try std.testing.expectEqual(@as(usize, 1), backend.runs.items.len);
    try std.testing.expect(backend.activeImmutableMemtableBytes() <= one_memtable_bytes);
}

test "lsm backend resource manager throttles projected immutable state" {
    var sample: ActiveMemTable = .{};
    defer sample.deinit(std.testing.allocator);
    try sample.upsert(std.testing.allocator, .{}, "key:a", "a", false);
    const one_memtable_bytes = Backend.estimateStateBytes(&sample);
    try std.testing.expect(one_memtable_bytes > 0);

    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@intFromEnum(resource_manager_mod.Slice.lsm_in_memory_state)] = .{
        .soft_limit_bytes = one_memtable_bytes + one_memtable_bytes / 2,
        .hard_limit_bytes = one_memtable_bytes * 3,
    };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var backend = try Backend.open(std.testing.allocator, "/lsm-resource-throttle-test", .{
        .storage = storage.storage(),
        .resource_manager = &manager,
        .flush_threshold = 1,
        .defer_flush_on_commit = true,
        .max_deferred_immutable_memtables = 8,
    });
    defer backend.close();

    {
        var txn = try backend.beginWrite();
        try txn.put(.{}, "key:a", "a");
        try txn.commit();
    }
    try std.testing.expectEqual(@as(usize, 1), backend.activeImmutableMemtableCount());
    try std.testing.expectEqual(@as(usize, 0), backend.runs.items.len);

    {
        var txn = try backend.beginWrite();
        try txn.put(.{}, "key:b", "b");
        try txn.commit();
    }
    try std.testing.expectEqual(@as(usize, 1), backend.activeImmutableMemtableCount());
    try std.testing.expectEqual(@as(usize, 1), backend.runs.items.len);
    const stats = manager.sliceStats(.lsm_in_memory_state);
    try std.testing.expect(stats.used_bytes <= one_memtable_bytes + one_memtable_bytes / 2);
    try std.testing.expectEqual(@as(u64, 0), stats.hard_limit_rejections);
}

test "lsm backend does not wait on aggregate soft pressure owned elsewhere" {
    var sample: ActiveMemTable = .{};
    defer sample.deinit(std.testing.allocator);
    try sample.upsert(std.testing.allocator, .{}, "key:a", "a", false);
    const incoming_bytes = Backend.estimateStateBytes(&sample);
    try std.testing.expect(incoming_bytes > 0);

    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@intFromEnum(resource_manager_mod.Slice.lsm_in_memory_state)] = .{
        .soft_limit_bytes = incoming_bytes + incoming_bytes / 2,
        .hard_limit_bytes = incoming_bytes * 4,
    };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    var external_usage: u64 = 0;
    manager.observeUsage(.lsm_in_memory_state, &external_usage, incoming_bytes);

    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();
    var backend = try Backend.open(std.testing.allocator, "/lsm-resource-aggregate-soft-test", .{
        .storage = storage.storage(),
        .resource_manager = &manager,
        .flush_threshold = 64,
    });
    defer backend.close();

    {
        var txn = try backend.beginWrite();
        try txn.put(.{}, "key:a", "a");
        try txn.commit();
    }
    try std.testing.expectEqualStrings("a", try backend.getMergedWithMutable(&backend.mutable, .{}, "key:a"));
    try std.testing.expectEqual(resource_manager_mod.Pressure.soft, manager.sliceStats(.lsm_in_memory_state).pressure);

    manager.observeUsage(.lsm_in_memory_state, &external_usage, 0);
}

test "lsm backend rejects aggregate hard throttle without waiting" {
    var sample: ActiveMemTable = .{};
    defer sample.deinit(std.testing.allocator);
    try sample.upsert(std.testing.allocator, .{}, "key:a", "a", false);
    const incoming_bytes = Backend.estimateStateBytes(&sample);
    try std.testing.expect(incoming_bytes > 0);

    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@intFromEnum(resource_manager_mod.Slice.lsm_in_memory_state)] = .{
        .soft_limit_bytes = incoming_bytes,
        .hard_limit_bytes = incoming_bytes + incoming_bytes / 2,
    };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    var external_usage: u64 = 0;
    manager.observeUsage(.lsm_in_memory_state, &external_usage, incoming_bytes);

    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();
    var backend = try Backend.open(std.testing.allocator, "/lsm-resource-aggregate-hard-test", .{
        .storage = storage.storage(),
        .resource_manager = &manager,
        .flush_threshold = 64,
    });
    defer backend.close();

    {
        var txn = try backend.beginWrite();
        try txn.put(.{}, "key:a", "a");
        try std.testing.expectError(error.ResourceBudgetExceeded, txn.commit());
    }
    try std.testing.expectError(error.NotFound, backend.getMergedWithMutable(&backend.mutable, .{}, "key:a"));

    manager.observeUsage(.lsm_in_memory_state, &external_usage, 0);
}

test "lsm backend resource manager rejects before wal apply" {
    var sample: ActiveMemTable = .{};
    defer sample.deinit(std.testing.allocator);
    try sample.upsert(std.testing.allocator, .{}, "key:a", "a", false);
    const one_memtable_bytes = Backend.estimateStateBytes(&sample);

    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@intFromEnum(resource_manager_mod.Slice.lsm_in_memory_state)] = .{
        .soft_limit_bytes = one_memtable_bytes,
        .hard_limit_bytes = one_memtable_bytes + one_memtable_bytes / 2,
    };
    var policies = resource_manager_mod.Options.defaultPolicies();
    policies[@intFromEnum(resource_manager_mod.Slice.lsm_in_memory_state)].hard_action = .reject_work;
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets, .policies = policies });
    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var backend = try Backend.open(std.testing.allocator, "/lsm-resource-reject-test", .{
        .storage = storage.storage(),
        .resource_manager = &manager,
        .flush_threshold = 1,
        .defer_flush_on_commit = true,
        .max_deferred_immutable_memtables = 8,
    });
    defer backend.close();

    {
        var txn = try backend.beginWrite();
        try txn.put(.{}, "key:a", "a");
        try txn.commit();
    }
    {
        var txn = try backend.beginWrite();
        try txn.put(.{}, "key:b", "b");
        try std.testing.expectError(error.ResourceBudgetExceeded, txn.commit());
    }
    try std.testing.expectError(error.NotFound, backend.getMergedWithMutable(&backend.mutable, .{}, "key:b"));
    try std.testing.expectEqual(@as(usize, 0), backend.runs.items.len);
}

test "lsm backend deferred immutable backpressure waits for in-flight build" {
    if (!supports_waitable_immutable_flush or builtin.single_threaded) return;

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
            .close_owner = drainOwner,
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
    const ClearBuild = struct {
        fn run(target: *Backend) void {
            sleepForTest(10 * std.time.ns_per_ms);
            const locked = runtime_mod.lockBackend(Backend, target);
            defer runtime_mod.unlockBackend(Backend, target, locked);
            target.finishImmutableFlushBuildLocked();
        }
    };
    const clear_thread = try std.Thread.spawn(.{}, ClearBuild.run, .{&backend});
    {
        var txn = try backend.beginWrite();
        try txn.put(.{}, "key:b", "b");
        try txn.commit();
    }
    clear_thread.join();
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

test "lsm backend schedules detached maintenance job for compaction debt" {
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
            .close_owner = drainOwner,
            .poll = poll,
        };
    };

    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var lane = FakeLane{};
    const executor = BackgroundExecutor.initLane(lane.lane(), 782);
    var backend = try Backend.open(std.testing.allocator, "/lsm-background-maintenance-job-test", .{
        .storage = storage.storage(),
        .flush_threshold = 1,
        .compact_threshold_runs = 1,
        .l0_hard_limit_runs = 100,
        .background_executor = &executor,
    });
    defer backend.close();

    {
        var txn = try backend.beginWrite();
        try txn.put(.{}, "key:a", "a");
        try txn.commit();
    }
    try std.testing.expect(lane.submitted_job == null);
    try std.testing.expect(!backend.maintenance_job_in_flight);
    try std.testing.expectEqual(@as(usize, 1), countLevelRuns(backend.runs.items, 0));

    {
        var txn = try backend.beginWrite();
        try txn.put(.{}, "key:b", "b");
        try txn.commit();
    }
    try std.testing.expect(lane.submitted_job != null);
    try std.testing.expect(backend.maintenance_job_in_flight);
    try std.testing.expectEqual(@as(usize, 2), countLevelRuns(backend.runs.items, 0));

    var job = lane.submitted_job.?;
    lane.submitted_job = null;
    try job.run(job.ptr);
    job.deinit(job.ptr);

    try std.testing.expect(!backend.maintenance_job_in_flight);
    try std.testing.expect(backend.compaction_stats.compactions > 0);
    try std.testing.expect(countLevelRuns(backend.runs.items, 0) <= 1);
    try std.testing.expectEqualStrings("a", try backend.getMergedWithMutable(&backend.mutable, .{}, "key:a"));
    try std.testing.expectEqualStrings("b", try backend.getMergedWithMutable(&backend.mutable, .{}, "key:b"));
}

test "lsm backend note potential maintenance debt schedules detached job" {
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
            .close_owner = drainOwner,
            .poll = poll,
        };
    };

    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var backend = try Backend.open(std.testing.allocator, "/lsm-background-maintenance-note-debt-test", .{
        .storage = storage.storage(),
        .flush_threshold = 1,
        .compact_threshold_runs = 1,
        .l0_hard_limit_runs = 100,
    });
    defer backend.close();

    {
        var txn = try backend.beginWrite();
        try txn.put(.{}, "key:a", "a");
        try txn.commit();
    }
    {
        var txn = try backend.beginWrite();
        try txn.put(.{}, "key:b", "b");
        try txn.commit();
    }
    try backend.flushAllImmutableMemtables();
    try std.testing.expectEqual(@as(usize, 0), backend.activeImmutableMemtableCount());
    try std.testing.expectEqual(@as(usize, 2), countLevelRuns(backend.runs.items, 0));
    try std.testing.expect(backend.maintenanceScore() > 0);

    var lane = FakeLane{};
    backend.background_executor = BackgroundExecutor.initLane(lane.lane(), 783);
    try std.testing.expect(lane.submitted_job == null);
    backend.notePotentialMaintenanceDebt();
    try std.testing.expect(lane.submitted_job != null);
    try std.testing.expect(backend.maintenance_job_in_flight);
}

test "lsm backend detached maintenance jobs reschedule while debt remains" {
    const FakeLane = struct {
        submitted_jobs: [16]background_runtime_mod.Job = undefined,
        submitted_count: usize = 0,

        fn lane(self: *@This()) background_runtime_mod.DurableJobLane {
            return .{
                .ptr = self,
                .vtable = &vtable,
            };
        }

        fn submit(ptr: *anyopaque, job: background_runtime_mod.Job) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expect(self.submitted_count < self.submitted_jobs.len);
            self.submitted_jobs[self.submitted_count] = job;
            self.submitted_count += 1;
        }

        fn pop(self: *@This()) ?background_runtime_mod.Job {
            if (self.submitted_count == 0) return null;
            const job = self.submitted_jobs[0];
            var i: usize = 1;
            while (i < self.submitted_count) : (i += 1) {
                self.submitted_jobs[i - 1] = self.submitted_jobs[i];
            }
            self.submitted_count -= 1;
            return job;
        }

        fn drainOwner(_: *anyopaque, _: u64) void {}

        fn poll(_: *anyopaque, _: usize) !usize {
            return 0;
        }

        const vtable = background_runtime_mod.DurableJobLane.VTable{
            .submit = submit,
            .drain_owner = drainOwner,
            .close_owner = drainOwner,
            .poll = poll,
        };
    };

    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var lane = FakeLane{};
    const executor = BackgroundExecutor.initLane(lane.lane(), 783);
    var backend = try Backend.open(std.testing.allocator, "/lsm-background-maintenance-reschedule-test", .{
        .storage = storage.storage(),
        .flush_threshold = 1,
        .compact_threshold_runs = 100,
        .l0_soft_limit_runs = 1,
        .l0_hard_limit_runs = 100,
        .background_maintenance_max_steps = 1,
        .max_compaction_input_bytes = 1,
        .background_executor = &executor,
    });
    defer backend.close();

    var key_buf: [16]u8 = undefined;
    for (0..6) |i| {
        const key = try std.fmt.bufPrint(&key_buf, "key:{d}", .{i});
        var txn = try backend.beginWrite();
        try txn.put(.{}, key, "value");
        try txn.commit();
    }

    try std.testing.expectEqual(@as(usize, 1), lane.submitted_count);
    try std.testing.expectEqual(@as(usize, 6), countLevelRuns(backend.runs.items, 0));

    var first_job = lane.pop().?;
    try first_job.run(first_job.ptr);
    first_job.deinit(first_job.ptr);

    try std.testing.expect(backend.maintenance_job_in_flight);
    try std.testing.expect(backend.maintenanceScore() > 0);
    try std.testing.expectEqual(@as(usize, 1), lane.submitted_count);

    var drain_steps: usize = 0;
    while (lane.pop()) |job_const| {
        var job = job_const;
        try job.run(job.ptr);
        job.deinit(job.ptr);
        drain_steps += 1;
        try std.testing.expect(drain_steps < 16);
    }

    try std.testing.expect(!backend.maintenance_job_in_flight);
    try std.testing.expectEqual(@as(u64, 0), backend.maintenanceScore());
    try std.testing.expect(countLevelRuns(backend.runs.items, 0) <= 1);
}

test "lsm backend detached no-op maintenance does not resubmit forever" {
    const FakeLane = struct {
        submitted_job: ?background_runtime_mod.Job = null,
        submitted_count: usize = 0,

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
            self.submitted_count += 1;
        }

        fn drainOwner(_: *anyopaque, _: u64) void {}

        fn poll(_: *anyopaque, _: usize) !usize {
            return 0;
        }

        const vtable = background_runtime_mod.DurableJobLane.VTable{
            .submit = submit,
            .drain_owner = drainOwner,
            .close_owner = drainOwner,
            .poll = poll,
        };
    };

    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var lane = FakeLane{};
    const executor = BackgroundExecutor.initLane(lane.lane(), 784);
    var backend = try Backend.open(std.testing.allocator, "/lsm-background-maintenance-no-op-test", .{
        .storage = storage.storage(),
        .flush_threshold = 1,
        .compact_threshold_runs = 100,
        .l0_soft_limit_runs = 1,
        .l0_hard_limit_runs = 100,
        .background_executor = &executor,
    });
    defer backend.close();

    for (0..2) |i| {
        var key_buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "key:{d}", .{i});
        var txn = try backend.beginWrite();
        try txn.put(.{}, key, "value");
        try txn.commit();
    }
    try std.testing.expectEqual(@as(usize, 1), lane.submitted_count);

    // The job was admitted while work was possible, but a bulk session makes
    // the actual pass a no-op. It must not busy-resubmit itself.
    try backend.beginBulkIngestSession();
    var job = lane.submitted_job.?;
    lane.submitted_job = null;
    try job.run(job.ptr);
    job.deinit(job.ptr);

    try std.testing.expect(backend.maintenanceScore() > 0);
    try std.testing.expect(!backend.maintenance_job_in_flight);
    try std.testing.expect(lane.submitted_job == null);
    try std.testing.expectEqual(@as(usize, 1), lane.submitted_count);

    // Ending the transient blocker must give existing debt exactly one fresh
    // opportunity to run. If the hint is still non-actionable, that job stops
    // without recreating the original loop.
    backend.abortBulkIngestSession();
    try std.testing.expect(backend.maintenance_job_in_flight);
    try std.testing.expect(lane.submitted_job != null);
    try std.testing.expectEqual(@as(usize, 2), lane.submitted_count);
    try backend.beginBulkIngestSession();
    var retry_after_abort = lane.submitted_job.?;
    lane.submitted_job = null;
    try retry_after_abort.run(retry_after_abort.ptr);
    retry_after_abort.deinit(retry_after_abort.ptr);
    try std.testing.expect(!backend.maintenance_job_in_flight);
    try std.testing.expect(lane.submitted_job == null);

    try backend.finishBulkIngestSessionWithOptions(.{ .compact = false, .flush = false });
    try std.testing.expect(backend.maintenance_job_in_flight);
    try std.testing.expect(lane.submitted_job != null);
    try std.testing.expectEqual(@as(usize, 3), lane.submitted_count);
    var retry_after_finish = lane.submitted_job.?;
    lane.submitted_job = null;
    try retry_after_finish.run(retry_after_finish.ptr);
    retry_after_finish.deinit(retry_after_finish.ptr);
    try std.testing.expect(!backend.maintenance_job_in_flight);
    try std.testing.expect(lane.submitted_job == null);
}

test "lsm backends share one threaded runtime durable lane" {
    if (builtin.os.tag == .freestanding) return;

    var runtime = try background_runtime_mod.BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    defer runtime.deinit();

    var first_storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer first_storage.deinit();
    var second_storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer second_storage.deinit();

    const first_executor = BackgroundExecutor.init(runtime.ptr(), try runtime.ptr().allocOwnerId());
    const second_executor = BackgroundExecutor.init(runtime.ptr(), try runtime.ptr().allocOwnerId());
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
    var open_stats = backend.snapshotOpenStats();
    try std.testing.expectEqual(Backend.OpenPhase.ready, open_stats.phase);
    try std.testing.expectEqual(@as(u64, 1), open_stats.completed);
    try std.testing.expect(!open_stats.loaded_manifest);
    try std.testing.expect(open_stats.total_ns > 0);
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
    open_stats = backend.snapshotOpenStats();
    try std.testing.expectEqual(Backend.OpenPhase.ready, open_stats.phase);
    try std.testing.expectEqual(@as(u64, 1), open_stats.completed);
    try std.testing.expect(!open_stats.loaded_manifest);
    try std.testing.expectEqual(@as(u64, 1), open_stats.mutable_entries_after_replay);
    try std.testing.expect(open_stats.wal_replay_records > 0);
    try std.testing.expect(open_stats.wal_replay_entries > 0);
    try std.testing.expect(open_stats.wal_replay_bytes > 0);
    try std.testing.expectEqual(backend.write_stats.wal_replay_records, open_stats.wal_replay_records);
    try std.testing.expectEqual(backend.write_stats.wal_replay_entries, open_stats.wal_replay_entries);
    try std.testing.expectEqual(backend.write_stats.wal_replay_bytes, open_stats.wal_replay_bytes);
    try std.testing.expectEqual(backend.write_stats.wal_replay_ns, open_stats.wal_replay_ns);
    try std.testing.expectEqual(backend.write_stats.wal_replay_truncated_tail_bytes, open_stats.wal_replay_truncated_tail_bytes);
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
    try std.testing.expectEqual(@as(u64, 0), maintenance.wal_checkpoint_covered_through_segment);
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
    try std.testing.expectEqual(@as(u64, 0), maintenance.wal_checkpoint_covered_through_segment);
    try std.testing.expectEqual(@as(u64, 1), maintenance.wal_checkpoint_current_segment);
    try std.testing.expectEqual(@as(u64, 0), maintenance.wal_checkpoint_lag_segments);
    try std.testing.expect(backend.write_stats.wal_replay_records > 0);

    try backend.resetWalAfterManifestCheckpoint();
    maintenance = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(@as(u64, 0), maintenance.wal_retained_segments);
    try std.testing.expectEqual(@as(u64, 0), maintenance.wal_retained_bytes);
    try std.testing.expectEqual(@as(u64, 1), maintenance.wal_checkpoint_oldest_retained_segment);
    try std.testing.expectEqual(@as(u64, 0), maintenance.wal_checkpoint_covered_through_segment);
    try std.testing.expectEqual(@as(u64, 1), maintenance.wal_checkpoint_current_segment);
    try std.testing.expectEqual(@as(u64, 0), maintenance.wal_checkpoint_lag_segments);
}

test "lsm backend idle mutable deadline checkpoints retained wal" {
    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    const root_dir = "/lsm-idle-mutable-checkpoint";
    var backend = try Backend.open(std.testing.allocator, root_dir, .{
        .flush_threshold = 1024,
        .mutable_idle_flush_after_ns = 100,
        .storage = storage.storage(),
    });
    defer backend.close();

    {
        var txn = try backend.beginWrite();
        defer txn.abort();
        try txn.put(.{ .name = "docs" }, "doc:a", "alpha");
        try txn.commit();
    }

    try std.testing.expectEqual(@as(usize, 1), backend.mutable.entries.items.len);
    try std.testing.expectEqual(@as(usize, 0), backend.runs.items.len);
    try std.testing.expect((backend.nextMaintenanceWakeDelayNsBestEffort() orelse 0) > 0);

    const first_deadline = backend.mutable_idle_flush_deadline_ns;
    storage.tick = first_deadline - 1;
    {
        var txn = try backend.beginWrite();
        defer txn.abort();
        try txn.put(.{ .name = "docs" }, "doc:b", "bravo");
        try txn.commit();
    }
    try std.testing.expect(backend.mutable_idle_flush_deadline_ns > first_deadline);

    storage.tick = backend.mutable_idle_flush_deadline_ns;
    try std.testing.expectEqual(@as(?u64, 0), backend.nextMaintenanceWakeDelayNsBestEffort());
    try std.testing.expect(try backend.runMaintenanceStep());

    try std.testing.expectEqual(@as(usize, 0), backend.mutable.entries.items.len);
    try std.testing.expectEqual(@as(u64, 0), backend.mutable_idle_flush_deadline_ns);
    try std.testing.expectEqual(@as(usize, 1), backend.runs.items.len);
    try std.testing.expectEqualStrings("alpha", try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "doc:a"));
    try std.testing.expectEqualStrings("bravo", try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "doc:b"));

    const maintenance = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(@as(u64, 0), maintenance.wal_retained_bytes);
}

test "lsm backend idle checkpoint accumulates small writes and bounds dirty age" {
    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    const root_dir = "/lsm-adaptive-idle-mutable-checkpoint";
    var backend = try Backend.open(std.testing.allocator, root_dir, .{
        .flush_threshold_bytes = 1024 * 1024,
        .mutable_idle_flush_after_ns = 100,
        .mutable_idle_flush_min_bytes = 1024,
        .mutable_idle_flush_max_age_ns = 1000,
        .storage = storage.storage(),
    });
    defer backend.close();

    {
        var txn = try backend.beginWrite();
        defer txn.abort();
        try txn.put(.{ .name = "docs" }, "doc:a", "alpha");
        try txn.commit();
    }
    const max_deadline = backend.mutable_idle_flush_max_deadline_ns;
    try std.testing.expect(max_deadline > 0);
    try std.testing.expectEqual(max_deadline, backend.mutable_idle_flush_deadline_ns);

    // A second low-rate write below the byte floor does not renew the maximum
    // age or create an early tiny-run deadline.
    storage.tick = 200;
    {
        var txn = try backend.beginWrite();
        defer txn.abort();
        try txn.put(.{ .name = "docs" }, "doc:b", "bravo");
        try txn.commit();
    }
    try std.testing.expectEqual(max_deadline, backend.mutable_idle_flush_deadline_ns);
    try std.testing.expect(!(try backend.runMaintenanceStep()));
    try std.testing.expectEqual(@as(usize, 0), backend.runs.items.len);

    // Crossing the byte floor activates the short idle deadline, which is
    // renewed by another write but never beyond the original maximum age.
    var large_value: [2048]u8 = undefined;
    @memset(&large_value, 'v');
    storage.tick = 300;
    {
        var txn = try backend.beginWrite();
        defer txn.abort();
        try txn.put(.{ .name = "docs" }, "doc:c", &large_value);
        try txn.commit();
    }
    try std.testing.expectEqual(@as(u64, 400), backend.mutable_idle_flush_deadline_ns);
    storage.tick = 350;
    {
        var txn = try backend.beginWrite();
        defer txn.abort();
        try txn.put(.{ .name = "docs" }, "doc:d", "delta");
        try txn.commit();
    }
    try std.testing.expectEqual(@as(u64, 450), backend.mutable_idle_flush_deadline_ns);
    storage.tick = 450;
    try std.testing.expect(try backend.runMaintenanceStep());
    try std.testing.expectEqual(@as(usize, 1), backend.runs.items.len);
    try std.testing.expectEqual(@as(u64, 0), backend.mutable_idle_flush_deadline_ns);
    try std.testing.expectEqual(@as(u64, 0), backend.mutable_idle_flush_max_deadline_ns);

    // A later tiny write still checkpoints at its maximum dirty age.
    storage.tick = 500;
    {
        var txn = try backend.beginWrite();
        defer txn.abort();
        try txn.put(.{ .name = "docs" }, "doc:e", "echo");
        try txn.commit();
    }
    try std.testing.expectEqual(@as(u64, 1500), backend.mutable_idle_flush_deadline_ns);
    storage.tick = 1499;
    try std.testing.expect(!(try backend.runMaintenanceStep()));
    storage.tick = 1500;
    try std.testing.expect(try backend.runMaintenanceStep());
    try std.testing.expectEqual(@as(u64, 0), backend.snapshotMaintenanceStats().wal_retained_bytes);
}

fn expectWalRetentionCacheMatchesStorage(backend: *Backend) !void {
    const cached_primary = try backend.cachedWalRetentionLocked();
    const fresh_primary = try wal_mod.snapshotRetention(backend.storage.?, backend.allocator, backend.root_dir.?);
    try std.testing.expectEqualDeep(fresh_primary, cached_primary);

    const cached_replay = try backend.cachedWalReplayRetentionLocked();
    const fresh_replay = try wal_mod.snapshotReplayRetention(backend.storage.?, backend.allocator, backend.root_dir.?);
    try std.testing.expectEqualDeep(fresh_replay, cached_replay);
}

test "lsm backend wal retention cache stays coherent across append retire and reset" {
    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    const root_dir = "/lsm-wal-retention-cache-coherence";
    var backend = try Backend.open(std.testing.allocator, root_dir, .{
        .flush_threshold = 1024,
        .wal_segment_bytes = 32,
        .storage = storage.storage(),
    });
    defer backend.close();

    // Populate the empty cache first so the commits exercise the O(1) update
    // path rather than merely causing a later lazy refresh.
    try expectWalRetentionCacheMatchesStorage(&backend);
    for ([_][]const u8{ "doc:a", "doc:b" }) |key| {
        var txn = try backend.beginWrite();
        defer txn.abort();
        try txn.put(.{ .name = "docs" }, key, "value");
        try txn.commit();
        try expectWalRetentionCacheMatchesStorage(&backend);
    }

    try backend.rotateMutableToImmutable();
    {
        var txn = try backend.beginWrite();
        defer txn.abort();
        try txn.put(.{ .name = "docs" }, "doc:c", "value");
        try txn.commit();
    }
    try expectWalRetentionCacheMatchesStorage(&backend);

    {
        const locked = runtime_mod.lockBackend(Backend, &backend);
        defer runtime_mod.unlockBackend(Backend, &backend, locked);
        try std.testing.expect(try backend.flushOldestImmutableMemtable());
    }
    try expectWalRetentionCacheMatchesStorage(&backend);

    try backend.checkpointWalAfterDurableBoundary();
    try expectWalRetentionCacheMatchesStorage(&backend);
}

test "lsm backend durable boundary checkpoint flushes mutable state and retires wal" {
    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    const root_dir = "/lsm-durable-boundary-checkpoint-flushes-mutable";
    var backend = try Backend.open(std.testing.allocator, root_dir, .{
        .flush_threshold = 1024,
        .storage = storage.storage(),
    });
    defer backend.close();

    {
        var txn = try backend.beginWrite();
        defer txn.abort();
        try txn.put(.{ .name = "docs" }, "doc:a", "alpha");
        try txn.commit();
    }

    var maintenance = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(@as(u64, 1), maintenance.wal_retained_segments);
    try std.testing.expect(maintenance.wal_retained_bytes > 0);
    try std.testing.expectEqual(@as(usize, 1), backend.mutable.entries.items.len);

    try backend.checkpointWalAfterDurableBoundary();

    maintenance = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(@as(u64, 0), maintenance.wal_retained_segments);
    try std.testing.expectEqual(@as(u64, 0), maintenance.wal_retained_bytes);
    try std.testing.expectEqual(@as(usize, 0), backend.mutable.entries.items.len);
    try std.testing.expectEqual(@as(usize, 0), backend.activeImmutableMemtableCount());
    try std.testing.expectEqualStrings("alpha", try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "doc:a"));
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

test "lsm backend accounts table builder working set during persisted flush" {
    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var manager = resource_manager_mod.ResourceManager.init(.{});
    const root_dir = "/lsm-table-builder-resource-accounting";
    const options = Options{
        .flush_threshold = 1024,
        .storage = storage.storage(),
        .resource_manager = &manager,
        .table_prefix_extractor = .none,
    };

    var backend = try Backend.open(std.testing.allocator, root_dir, options);
    defer backend.close();

    var value_buf: [512]u8 = undefined;
    @memset(&value_buf, 'v');
    {
        var txn = try backend.beginWrite();
        defer txn.abort();
        var i: usize = 0;
        while (i < 64) : (i += 1) {
            var key_buf: [32]u8 = undefined;
            const key = try std.fmt.bufPrint(&key_buf, "doc:{d:0>4}", .{i});
            try txn.put(.{ .name = "docs" }, key, &value_buf);
        }
        try txn.commit();
    }

    {
        const locked = runtime_mod.lockBackend(Backend, &backend);
        defer runtime_mod.unlockBackend(Backend, &backend, locked);
        try backend.rotateMutableToImmutable();
        try std.testing.expect(try backend.flushOldestImmutableMemtable());
    }
    const builder_stats = manager.sliceStats(.lsm_table_builder_working_set);
    const compaction_work_stats = manager.sliceStats(.lsm_compaction_work);
    try std.testing.expectEqual(@as(u64, 0), builder_stats.used_bytes);
    try std.testing.expect(builder_stats.peak_bytes >= 256 * 1024);
    try std.testing.expectEqual(@as(u64, 0), compaction_work_stats.peak_bytes);
    try std.testing.expect(backend.runs.items.len > 0);
    try std.testing.expect(backend.runs.items[0].path != null);

    var index = try repository_mod.loadRunTableIndexAllocWithStorage(storage.storage(), std.testing.allocator, backend.runs.items[0].path.?);
    defer index.deinit(std.testing.allocator);
    try std.testing.expectEqual(lsm_table_file.PrefixExtractor.none, index.prefix_extractor);
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
    try std.testing.expectEqual(@as(u64, 2), maintenance.wal_checkpoint_covered_through_segment);
    try std.testing.expectEqual(@as(u64, 3), maintenance.wal_checkpoint_current_segment);
    try std.testing.expectEqual(@as(u64, 0), maintenance.wal_checkpoint_lag_segments);

    backend.options.backend.read_only = true;
    backend.close();

    backend = try Backend.open(std.testing.allocator, root_dir, options);
    defer backend.close();
    maintenance = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(@as(u64, 1), maintenance.wal_retained_segments);
    try std.testing.expectEqual(@as(u64, 3), maintenance.wal_checkpoint_oldest_retained_segment);
    try std.testing.expectEqual(@as(u64, 2), maintenance.wal_checkpoint_covered_through_segment);
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
    try std.testing.expectEqual(@as(u64, 0), maintenance.wal_checkpoint_covered_through_segment);
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
    try std.testing.expectEqual(@as(u64, 0), maintenance.wal_checkpoint_covered_through_segment);
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
    try std.testing.expectEqual(@as(u64, 0), maintenance.wal_checkpoint_covered_through_segment);
    try std.testing.expectEqual(@as(u64, 1), maintenance.wal_checkpoint_current_segment);
    try std.testing.expectEqual(@as(u64, 0), maintenance.wal_checkpoint_lag_segments);

    var read = try backend.beginRead();
    defer read.abort();
    try std.testing.expectEqualStrings("alpha", try read.get(.{ .name = "docs" }, "doc:a"));
    try std.testing.expectEqualStrings("beta", try read.get(.{ .name = "docs" }, "doc:b"));
}

test "lsm backend optional soft wal pressure checkpoints one bounded flush on commit" {
    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    const root_dir = "/lsm-soft-wal-pressure-commit";
    const options = Options{
        .flush_threshold = 1024,
        .storage = storage.storage(),
        .wal_segment_bytes = 32,
        .wal_soft_limit_segments = 1,
        .wal_hard_limit_segments = 100,
        .foreground_soft_wal_checkpoint = true,
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
    backend.last_wal_retention_enforce_ns = 0;
    {
        var txn = try backend.beginWrite();
        defer txn.abort();
        try txn.put(.{ .name = "docs" }, "doc:b", "beta");
        try txn.commit();
    }

    const write_stats = backend.snapshotWriteStats();
    try std.testing.expectEqual(@as(u64, 1), write_stats.wal_pressure_flushes);
    try std.testing.expect(write_stats.wal_pressure_ns > 0);

    const maintenance = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(@as(u64, 0), maintenance.mutable_entries);
    try std.testing.expectEqual(@as(u64, 0), maintenance.immutable_memtables);
    try std.testing.expectEqual(@as(u64, 0), maintenance.wal_retained_segments);
    try std.testing.expectEqual(@as(u64, 0), maintenance.wal_retained_bytes);

    var read = try backend.beginRead();
    defer read.abort();
    try std.testing.expectEqualStrings("alpha", try read.get(.{ .name = "docs" }, "doc:a"));
    try std.testing.expectEqualStrings("beta", try read.get(.{ .name = "docs" }, "doc:b"));
}

test "lsm backend repeated checkpointed reopen cycles do not accumulate retained wal" {
    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    const root_dir = "/lsm-repeated-checkpointed-reopen";
    const options = Options{
        .flush_threshold = 1024,
        .storage = storage.storage(),
        .wal_segment_bytes = 32,
        .compact_threshold_runs = 100,
    };

    var cycle: usize = 0;
    while (cycle < 3) : (cycle += 1) {
        var backend = try Backend.open(std.testing.allocator, root_dir, options);
        defer backend.close();

        const open_stats = backend.snapshotOpenStats();
        try std.testing.expectEqual(Backend.OpenPhase.ready, open_stats.phase);
        try std.testing.expectEqual(@as(u64, 0), open_stats.wal_replay_records);
        try std.testing.expectEqual(@as(u64, 0), open_stats.wal_replay_bytes);

        var key_buf: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "doc:{d}", .{cycle});
        {
            var txn = try backend.beginWrite();
            defer txn.abort();
            try txn.put(.{ .name = "docs" }, key, "value");
            try txn.commit();
        }

        const before_checkpoint = backend.snapshotMaintenanceStats();
        try std.testing.expect(before_checkpoint.wal_retained_segments > 0);
        try std.testing.expect(before_checkpoint.wal_retained_bytes > 0);

        try backend.checkpointWalAfterDurableBoundary();

        const after_checkpoint = backend.snapshotMaintenanceStats();
        try std.testing.expectEqual(@as(u64, 0), after_checkpoint.wal_retained_segments);
        try std.testing.expectEqual(@as(u64, 0), after_checkpoint.wal_retained_bytes);
        try std.testing.expectEqual(@as(u64, 0), after_checkpoint.wal_checkpoint_lag_segments);
        try std.testing.expectEqual(@as(u64, 0), after_checkpoint.mutable_entries);
        try std.testing.expectEqual(@as(u64, 0), after_checkpoint.immutable_memtables);

        var read = try backend.beginRead();
        defer read.abort();
        var seen: usize = 0;
        while (seen <= cycle) : (seen += 1) {
            var seen_key_buf: [32]u8 = undefined;
            const seen_key = try std.fmt.bufPrint(&seen_key_buf, "doc:{d}", .{seen});
            try std.testing.expectEqualStrings("value", try read.get(.{ .name = "docs" }, seen_key));
        }
    }

    var reopened = try Backend.open(std.testing.allocator, root_dir, options);
    defer reopened.close();

    const final_open = reopened.snapshotOpenStats();
    try std.testing.expectEqual(Backend.OpenPhase.ready, final_open.phase);
    try std.testing.expectEqual(@as(u64, 0), final_open.wal_replay_records);
    try std.testing.expectEqual(@as(u64, 0), final_open.wal_replay_bytes);

    const final_maintenance = reopened.snapshotMaintenanceStats();
    try std.testing.expectEqual(@as(u64, 0), final_maintenance.wal_retained_segments);
    try std.testing.expectEqual(@as(u64, 0), final_maintenance.wal_retained_bytes);

    var read = try reopened.beginRead();
    defer read.abort();
    cycle = 0;
    while (cycle < 3) : (cycle += 1) {
        var key_buf: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "doc:{d}", .{cycle});
        try std.testing.expectEqualStrings("value", try read.get(.{ .name = "docs" }, key));
    }
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

test "lsm backend direct bulk append duplicate keys preserve last write wins" {
    var backend = Backend.init(std.testing.allocator, .{
        .flush_threshold = 1,
        .bulk_ingest_flush_threshold_multiplier = 2,
    });
    defer backend.close();

    {
        var txn = try backend.beginBatchWithOptions(.{ .mode = .bulk_ingest });
        try txn.appendPut(.{ .name = "docs" }, "doc:a", "A1");
        try txn.appendPut(.{ .name = "docs" }, "doc:b", "B");
        try txn.appendPut(.{ .name = "docs" }, "doc:a", "A2");
        try std.testing.expectEqualStrings("A2", try txn.get(.{ .name = "docs" }, "doc:a"));
        try txn.commit();
    }

    const stats = backend.snapshotWriteStats();
    try std.testing.expectEqual(@as(u64, 1), stats.bulk_append_fallback_duplicate_keys);
    try std.testing.expectEqualStrings("A2", try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "doc:a"));
    try std.testing.expectEqualStrings("B", try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "doc:b"));
}

test "lsm backend bulk append mixed deletes use normal overlay semantics" {
    var backend = Backend.init(std.testing.allocator, .{
        .flush_threshold = 1,
        .bulk_ingest_flush_threshold_multiplier = 2,
    });
    defer backend.close();

    {
        var txn = try backend.beginBatchWithOptions(.{ .mode = .bulk_ingest });
        try txn.appendPut(.{ .name = "docs" }, "doc:a", "A");
        try txn.delete(.{ .name = "docs" }, "doc:a");
        try std.testing.expectError(error.NotFound, txn.get(.{ .name = "docs" }, "doc:a"));
        try txn.commit();
    }

    try std.testing.expectError(error.NotFound, backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "doc:a"));
}

test "lsm runtime bulk append after put preserves write order" {
    var backend = Backend.init(std.testing.allocator, .{
        .flush_threshold = 1,
        .bulk_ingest_flush_threshold_multiplier = 2,
    });
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    {
        var txn = try runtime.beginBatchWithOptions(.{ .mode = .bulk_ingest });
        try txn.put("doc:a", "A1");
        try txn.appendPut("doc:a", "A2");
        try std.testing.expectEqualStrings("A2", try txn.get("doc:a"));
        try txn.commit();
    }

    try std.testing.expectEqualStrings("A2", try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "doc:a"));
}

test "lsm backend direct bulk ingest drains existing mutable before threshold batch" {
    var backend = Backend.init(std.testing.allocator, .{
        .flush_threshold = 1,
        .bulk_ingest_flush_threshold_multiplier = 4,
    });
    defer backend.close();

    try backend.beginBulkIngestSession();
    var bulk_active = true;
    errdefer if (bulk_active) backend.abortBulkIngestSession();

    {
        var txn = try backend.beginBatchWithOptions(.{ .mode = .bulk_ingest });
        try txn.put(.{ .name = "docs" }, "doc:a", "A");
        try txn.put(.{ .name = "docs" }, "doc:b", "B");
        try txn.commit();
    }

    try std.testing.expectEqual(@as(usize, 2), backend.mutable.entries.items.len);
    try std.testing.expectEqual(@as(usize, 0), backend.runs.items.len);

    {
        var txn = try backend.beginBatchWithOptions(.{ .mode = .bulk_ingest });
        try txn.put(.{ .name = "docs" }, "doc:c", "C");
        try txn.put(.{ .name = "docs" }, "doc:d", "D");
        try txn.put(.{ .name = "docs" }, "doc:e", "E");
        try txn.put(.{ .name = "docs" }, "doc:f", "F");
        try txn.commit();
    }

    try backend.finishBulkIngestSessionWithOptions(.{ .compact = false });
    bulk_active = false;

    const stats = backend.snapshotWriteStats();
    try std.testing.expectEqual(@as(usize, 0), backend.mutable.entries.items.len);
    try std.testing.expectEqual(@as(usize, 2), backend.runs.items.len);
    try std.testing.expectEqual(@as(u64, 0), stats.flushes);
    try std.testing.expectEqual(@as(u64, 2), stats.sorted_ingest_runs);
    try std.testing.expectEqual(@as(u64, 2), stats.direct_bulk_ingest_attempts);
    try std.testing.expectEqual(@as(u64, 1), stats.direct_bulk_ingest_fallback_below_threshold);
    try std.testing.expectEqual(@as(u64, 1), stats.direct_bulk_ingest_successes);
    try std.testing.expectEqualStrings("A", try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "doc:a"));
    try std.testing.expectEqualStrings("F", try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "doc:f"));
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

test "lsm backend reopens overlapping direct ingest l0 runs with newest values visible" {
    var path_buf: [256]u8 = undefined;
    const path = repository_mod.tmpPath(&path_buf, "direct-ingest-overlap-reopen");
    defer repository_mod.cleanupTmp(path);

    var packed_node_key: [12]u8 = undefined;
    packed_node_key[0] = 'n';
    packed_node_key[1] = ':';
    std.mem.writeInt(u64, packed_node_key[2..10], 1, .big);
    packed_node_key[10] = ':';
    packed_node_key[11] = 'p';
    var node_range_key = packed_node_key;
    node_range_key[11] = 'r';
    var node_posting_key = packed_node_key;
    node_posting_key[11] = 'o';
    var vec_meta_keys: [4][10]u8 = undefined;
    for (&vec_meta_keys, 0..) |*key, i| {
        key[0] = 'm';
        key[1] = ':';
        std.mem.writeInt(u64, key[2..10], @as(u64, @intCast(i + 1)), .big);
    }

    {
        var backend = try Backend.open(std.testing.allocator, std.mem.span(path), .{
            .flush_threshold = 1,
            .bulk_ingest_flush_threshold_multiplier = 1,
            .compact_threshold_runs = 999,
            .foreground_soft_compaction = false,
        });
        defer backend.close();

        {
            var txn = try backend.beginBatchWithOptions(.{ .mode = .bulk_ingest });
            try txn.put(.{ .name = "docs" }, "doc:a", "A1");
            try txn.put(.{ .name = "docs" }, "doc:b", "B1");
            try txn.put(.{ .name = "hbc_nodes" }, packed_node_key[0..], "old-packed-node");
            try txn.put(.{ .name = "hbc_nodes" }, node_range_key[0..], "old-range");
            try txn.put(.{ .name = "hbc_nodes" }, node_posting_key[0..], "old-posting");
            for (&vec_meta_keys, 0..) |*key, i| {
                const value = switch (i) {
                    0 => "meta-1",
                    1 => "meta-2",
                    2 => "meta-3",
                    else => "meta-4",
                };
                try txn.put(.{ .name = "hbc_vecs" }, key[0..], value);
            }
            try txn.commit();
        }
        {
            var txn = try backend.beginBatchWithOptions(.{ .mode = .bulk_ingest });
            try txn.put(.{ .name = "docs" }, "doc:a", "A2");
            try txn.put(.{ .name = "docs" }, "doc:c", "C2");
            try txn.put(.{ .name = "hbc_nodes" }, packed_node_key[0..], "new-packed-node");
            try txn.put(.{ .name = "hbc_nodes" }, node_range_key[0..], "new-range");
            try txn.put(.{ .name = "hbc_nodes" }, node_posting_key[0..], "new-posting");
            for (&vec_meta_keys, 0..) |*key, i| {
                const value = switch (i) {
                    0 => "meta-1b",
                    1 => "meta-2b",
                    2 => "meta-3b",
                    else => "meta-4b",
                };
                try txn.put(.{ .name = "hbc_vecs" }, key[0..], value);
            }
            try txn.commit();
        }

        try std.testing.expect(backend.runs.items.len >= 2);
        try backend.persistManifest();
    }

    {
        var reopened = try Backend.open(std.testing.allocator, std.mem.span(path), .{
            .flush_threshold = 1,
            .bulk_ingest_flush_threshold_multiplier = 1,
            .compact_threshold_runs = 999,
            .foreground_soft_compaction = false,
        });
        defer reopened.close();

        var runtime = try reopened.runtimeStore(std.testing.allocator, .{ .name = "docs" });
        defer runtime.deinit();
        var read = try runtime.beginRead();
        defer read.abort();

        try std.testing.expectEqualStrings("A2", try read.get("doc:a"));
        {
            var namespace_read = try reopened.beginRead();
            defer namespace_read.abort();
            try std.testing.expectEqualStrings("new-packed-node", try namespace_read.get(.{ .name = "hbc_nodes" }, packed_node_key[0..]));
            try std.testing.expectEqualStrings("new-range", try namespace_read.get(.{ .name = "hbc_nodes" }, node_range_key[0..]));
            try std.testing.expectEqualStrings("meta-4b", try namespace_read.get(.{ .name = "hbc_vecs" }, vec_meta_keys[3][0..]));
        }

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

        var visible = try reopened.materializeVisibleState();
        defer visible.deinit(std.testing.allocator);
        try std.testing.expectEqualStrings("A2", try visible.get(.{ .name = "docs" }, "doc:a"));
        try std.testing.expectEqualStrings("new-packed-node", try visible.get(.{ .name = "hbc_nodes" }, packed_node_key[0..]));
        try std.testing.expectEqualStrings("new-range", try visible.get(.{ .name = "hbc_nodes" }, node_range_key[0..]));
        try std.testing.expectEqualStrings("meta-4b", try visible.get(.{ .name = "hbc_vecs" }, vec_meta_keys[3][0..]));
    }
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

test "lsm backend mutable byte estimate includes arena and hash index capacity" {
    var mutable: ActiveMemTable = .{};
    defer mutable.deinit(std.testing.allocator);

    try mutable.upsert(std.testing.allocator, .{ .name = "docs" }, "doc:a", "one", false);
    try mutable.upsert(std.testing.allocator, .{ .name = "docs" }, "doc:b", "two", false);

    const arena_bytes: u64 = @intCast(mutable.arena_owner.?.queryCapacity());
    const entries_bytes: u64 = @as(u64, @intCast(mutable.entries.capacity)) * @sizeOf(state_mod.OwnedEntry);
    const index_bytes = mutable.estimatedIndexMemoryBytes();
    const logical_bytes = 2 * @sizeOf(state_mod.OwnedEntry) +
        2 * "docs".len + "doc:a".len + "one".len + "doc:b".len + "two".len;
    try std.testing.expect(index_bytes > 0);
    try std.testing.expectEqual(@as(u64, logical_bytes), mutable.estimatedLogicalBytes());
    try std.testing.expectEqual(entries_bytes +| arena_bytes +| index_bytes, Backend.estimateStateBytes(&mutable));

    try mutable.upsert(std.testing.allocator, .{ .name = "docs" }, "doc:a", "replacement", false);
    try std.testing.expectEqual(
        @as(u64, logical_bytes - "one".len + "replacement".len),
        mutable.estimatedLogicalBytes(),
    );
}

test "lsm backend preserves logical flush sizing with an actual memory guard" {
    var mutable: ActiveMemTable = .{};
    defer mutable.deinit(std.testing.allocator);

    const value = [_]u8{'x'} ** (256 * 1024);
    for (0..8) |i| {
        // Replacements leave the superseded value in the arena until the
        // memtable is released, while the logical state remains one entry.
        const value_len: usize = if (i % 2 == 0) 64 * 1024 else value.len;
        try mutable.upsert(std.testing.allocator, .{ .name = "docs" }, "hot-key", value[0..value_len], false);
    }

    const logical_bytes = Backend.estimateStateLogicalBytes(&mutable);
    const actual_bytes = Backend.estimateStateBytes(&mutable);
    try std.testing.expect(actual_bytes >= (logical_bytes + 1) * mutable_memory_guard_multiplier);

    // Capacity alone below the guard must not change normal run geometry.
    try std.testing.expect(!Backend.stateMeetsByteFlushThreshold(&mutable, actual_bytes));
    // Arena growth beyond 2x the logical target activates the safety flush.
    try std.testing.expect(Backend.stateMeetsByteFlushThreshold(&mutable, logical_bytes + 1));
}

test "lsm backend probe owns immutable point values without retaining generations" {
    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();
    var backend = try Backend.open(std.testing.allocator, "/lsm-immutable-probe-borrow", .{
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
    try std.testing.expectEqual(@as(usize, 1), backend.activeImmutableMemtableCount());

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();
    var probe = try runtime.beginProbe();
    defer probe.abort();

    const owned_value = try probe.get("doc:a");
    try std.testing.expectEqualStrings(value[0..], owned_value);

    var read_stats = backend.snapshotReadStats();
    try std.testing.expectEqual(@as(u64, 0), read_stats.point_value_borrows);
    try std.testing.expectEqual(@as(u64, 1), read_stats.point_value_copies);

    try std.testing.expect(try backend.runMaintenanceStep());
    try std.testing.expectEqual(@as(usize, 0), backend.activeImmutableMemtableCount());
    try std.testing.expectEqual(@as(usize, 0), backend.retired_immutable_memtables.items.len);
    try std.testing.expectEqualStrings(value[0..], owned_value);

    read_stats = backend.snapshotReadStats();
    try std.testing.expectEqual(@as(u64, 0), read_stats.point_value_borrows);
    try std.testing.expectEqual(@as(u64, 1), read_stats.point_value_copies);
}

test "lsm backend reclaims a retired immutable when its exact reader exits" {
    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();
    var manager = resource_manager_mod.ResourceManager.init(.{});
    var backend = try Backend.open(std.testing.allocator, "/lsm-immutable-generation-pins", .{
        .flush_threshold = 1000,
        .flush_threshold_bytes = 256,
        .storage = storage.storage(),
        .resource_manager = &manager,
    });
    defer backend.close();

    const value_a = [_]u8{'a'} ** 300;
    {
        var txn = try backend.beginWrite();
        try txn.put(.{ .name = "docs" }, "doc:a", value_a[0..]);
        try txn.commit();
    }

    var old_reader = try backend.beginRead();
    var old_reader_open = true;
    defer if (old_reader_open) old_reader.abort();
    try std.testing.expectEqualStrings(value_a[0..], try old_reader.get(.{ .name = "docs" }, "doc:a"));

    try std.testing.expect(try backend.runMaintenanceStep());
    try std.testing.expectEqual(@as(usize, 1), backend.retired_immutable_memtables.items.len);
    const retired_bytes = Backend.estimateStateBytes(backend.retired_immutable_memtables.items[0]);
    const retired_stats = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(@as(u64, 1), retired_stats.retired_immutable_memtables);
    try std.testing.expectEqual(retired_bytes, retired_stats.retired_immutable_bytes);
    try std.testing.expectEqual(@as(u64, 1), retired_stats.immutable_pinned_generations);

    const value_b = [_]u8{'b'} ** 300;
    {
        var txn = try backend.beginWrite();
        try txn.put(.{ .name = "docs" }, "doc:b", value_b[0..]);
        try txn.commit();
    }
    var newer_reader = try backend.beginRead();
    defer newer_reader.abort();
    try std.testing.expect(backend.active_readers > 1);
    const before_old_release = manager.sliceStats(.lsm_in_memory_state).used_bytes;

    old_reader.abort();
    old_reader_open = false;

    // The newer reader never borrowed the old immutable generation. Releasing
    // the old reader must reclaim it even though another reader remains open.
    try std.testing.expectEqual(@as(usize, 0), backend.retired_immutable_memtables.items.len);
    try std.testing.expectEqual(
        before_old_release -| retired_bytes,
        manager.sliceStats(.lsm_in_memory_state).used_bytes,
    );
    try std.testing.expectEqualStrings(value_a[0..], try newer_reader.get(.{ .name = "docs" }, "doc:a"));
    try std.testing.expectEqualStrings(value_b[0..], try newer_reader.get(.{ .name = "docs" }, "doc:b"));
}

test "lsm backend probe owns active mutable point values across later writes" {
    var backend = Backend.init(std.testing.allocator, .{
        .flush_threshold = 1000,
        .wal_enabled = false,
    });
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    {
        var txn = try runtime.beginWrite();
        try txn.put("doc:a", "A");
        try txn.commit();
    }

    var probe = try runtime.beginProbe();
    defer probe.abort();

    const before = backend.snapshotReadStats();
    const owned_a = try probe.get("doc:a");
    try std.testing.expectEqualStrings("A", owned_a);
    try std.testing.expectEqual(@as(usize, 1), backend.mutable.entries.items.len);
    try std.testing.expectEqual(@as(usize, 0), backend.activeImmutableMemtableCount());

    {
        var txn = try runtime.beginWrite();
        try txn.put("doc:a", "B");
        try txn.commit();
    }

    try std.testing.expectEqualStrings("A", owned_a);
    try std.testing.expectEqual(@as(usize, 1), backend.mutable.entries.items.len);
    try std.testing.expectEqual(@as(usize, 0), backend.activeImmutableMemtableCount());

    const owned_b = try probe.get("doc:a");
    try std.testing.expectEqualStrings("B", owned_b);

    const after = backend.snapshotReadStats();
    try std.testing.expectEqual(@as(u64, 0), after.point_value_borrows - before.point_value_borrows);
    try std.testing.expectEqual(@as(u64, 2), after.point_value_copies - before.point_value_copies);
}

test "lsm backend probe owns active mutable point values during bulk ingest" {
    var backend = Backend.init(std.testing.allocator, .{
        .flush_threshold = 1000,
        .wal_enabled = false,
    });
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    {
        var txn = try runtime.beginWrite();
        try txn.put("doc:a", "A");
        try txn.commit();
    }

    try backend.beginBulkIngestSession();
    var bulk_active = true;
    errdefer if (bulk_active) backend.abortBulkIngestSession();

    var probe = try runtime.beginProbe();
    errdefer probe.abort();

    const before = backend.snapshotReadStats();
    const copied_a = try probe.get("doc:a");
    try std.testing.expectEqualStrings("A", copied_a);
    try std.testing.expectEqual(@as(usize, 1), backend.mutable.entries.items.len);
    try std.testing.expectEqual(@as(usize, 0), backend.activeImmutableMemtableCount());

    {
        var txn = try runtime.beginBatchWithOptions(.{ .mode = .bulk_ingest });
        try txn.put("doc:a", "B");
        try txn.commit();
    }

    try std.testing.expectEqualStrings("A", copied_a);
    try std.testing.expectEqual(@as(usize, 1), backend.mutable.entries.items.len);
    try std.testing.expectEqual(@as(usize, 0), backend.activeImmutableMemtableCount());

    const after = backend.snapshotReadStats();
    try std.testing.expectEqual(@as(u64, 0), after.point_value_borrows - before.point_value_borrows);
    try std.testing.expectEqual(@as(u64, 1), after.point_value_copies - before.point_value_copies);

    probe.abort();
    try backend.finishBulkIngestSessionWithOptions(.{ .compact = false, .flush = false });
    bulk_active = false;
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
            .backend = .{ .durability = .none },
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
        var backend = try Backend.open(std.testing.allocator, "/lsm-wal-full-durability-stats", .{
            .storage = storage.storage(),
            .flush_threshold = 1024,
        });
        defer backend.close();

        var txn = try backend.beginWrite();
        try txn.put(.{ .name = "docs" }, "doc:a", "A");
        try txn.commit();

        const stats = backend.snapshotWriteStats();
        try std.testing.expectEqual(@as(u64, 1), stats.wal_append_records);
        try std.testing.expectEqual(@as(u64, 1), stats.wal_sync_records);
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
    try std.testing.expect(stats.write_pressure_l0_run_debt > 0);
}

test "lsm backend write pressure records bounded overload when input budget denies a foreground step" {
    var backend = Backend.init(std.testing.allocator, .{
        .flush_threshold = 1,
        .compact_threshold_runs = 100,
        // Accumulate a deterministic backlog before lowering the hard limit;
        // otherwise inline soft maintenance may legitimately drain it first.
        .l0_soft_limit_runs = 100,
        .l0_hard_limit_runs = 100,
        .write_pressure_max_compaction_steps = 1,
        .max_compaction_input_bytes = 1,
    });
    defer backend.close();

    for (0..10) |i| {
        var key_buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "doc:{d}", .{i});
        var txn = try backend.beginWrite();
        try txn.put(.{ .name = "docs" }, key, "value");
        try txn.commit();
    }

    backend.options.l0_hard_limit_runs = 2;
    try backend.finalizeDeferredStorageWork();

    const stats = backend.snapshotWriteStats();
    try std.testing.expectEqual(@as(u64, 1), stats.write_pressure_events);
    try std.testing.expectEqual(@as(u64, 0), stats.write_pressure_compaction_steps);
    try std.testing.expectEqual(@as(u64, 1), stats.write_pressure_overloads);
    try std.testing.expect(stats.write_pressure_l0_run_debt > 0);
    try std.testing.expect(stats.write_pressure_overload_l0_run_debt > 0);
    try std.testing.expectEqual(@as(u64, 0), stats.write_pressure_rejections);
    try std.testing.expect(countLevelRuns(backend.runs.items, 0) > 2);
    try std.testing.expect(backend.snapshotMaintenanceStats().write_stall_l0_run_debt > 0);
}

test "lsm backend write pressure can reject when overload remains after budget" {
    var backend = Backend.init(std.testing.allocator, .{
        .flush_threshold = 1,
        .compact_threshold_runs = 100,
        // Accumulate a deterministic backlog before lowering the hard limit;
        // otherwise inline soft maintenance may legitimately drain it first.
        .l0_soft_limit_runs = 100,
        .l0_hard_limit_runs = 100,
        .write_pressure_max_compaction_steps = 1,
        .write_pressure_reject_on_overload = true,
        .max_compaction_input_bytes = 1,
    });
    defer backend.close();

    for (0..10) |i| {
        var key_buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "doc:{d}", .{i});
        var txn = try backend.beginWrite();
        try txn.put(.{ .name = "docs" }, key, "value");
        try txn.commit();
    }

    backend.options.l0_hard_limit_runs = 2;
    try std.testing.expectError(error.WritePressureExceeded, backend.finalizeDeferredStorageWork());

    const stats = backend.snapshotWriteStats();
    try std.testing.expectEqual(@as(u64, 1), stats.write_pressure_events);
    try std.testing.expectEqual(@as(u64, 1), stats.write_pressure_overloads);
    try std.testing.expect(stats.write_pressure_l0_run_debt > 0);
    try std.testing.expect(stats.write_pressure_overload_l0_run_debt > 0);
    try std.testing.expectEqual(@as(u64, 1), stats.write_pressure_rejections);
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
                platform.time.yieldBriefly();
            }
            var spin: usize = 0;
            while (spin < 128) : (spin += 1) {
                platform.time.yieldBriefly();
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

        fn readFileTrailerAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, len: usize) !storage_io.FileTrailer {
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

        fn syncFileContentsAbsolute(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().syncFileContentsAbsolute(path);
        }

        fn syncParentAbsolute(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().syncParentAbsolute(path);
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
        .sync_contents_absolute = CountingStorage.syncFileContentsAbsolute,
        .sync_parent_absolute = CountingStorage.syncParentAbsolute,
        .rename_is_atomic = true,
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
    try std.testing.expect(maintenance.compaction_scheduler_remembered_pending_runs > 0);
    try std.testing.expect(maintenance.compaction_scheduler_remembered_pending_bytes > 0);
    try std.testing.expect(maintenance.compaction_scheduler_remembered_candidates > 0);

    backend.compaction_scheduler.options.max_in_flight_input_bytes = 1024 * 1024;
    try std.testing.expect(try backend.runMaintenanceStep());
    maintenance = backend.snapshotMaintenanceStats();
    try std.testing.expect(maintenance.compaction_scheduler_grants > 0);
    try std.testing.expectEqual(maintenance.compaction_scheduler_grants, maintenance.compaction_scheduler_completions);
    try std.testing.expectEqual(@as(u64, 0), maintenance.compaction_scheduler_remembered_pending);
    try std.testing.expectEqual(@as(u64, 0), maintenance.compaction_scheduler_remembered_pending_runs);
    try std.testing.expectEqual(@as(u64, 0), maintenance.compaction_scheduler_remembered_pending_bytes);
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
    try std.testing.expect(maintenance.compaction_scheduler_remembered_pending_runs > 0);
    try std.testing.expect(maintenance.compaction_scheduler_remembered_pending_bytes > 0);
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
        .max_compaction_input_allow_oversized_single_job = false,
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
    try std.testing.expect(maintenance.compaction_scheduler_oversized_skips > 0);
    try std.testing.expectEqual(@as(u64, 0), maintenance.compaction_scheduler_remembered_pending);
    try std.testing.expectEqual(@as(usize, 3), countLevelRuns(backend.runs.items, 0));

    backend.options.max_compaction_input_bytes = 1024 * 1024;
    try std.testing.expect(try backend.runMaintenanceStep());
    maintenance = backend.snapshotMaintenanceStats();
    try std.testing.expect(maintenance.compaction_scheduler_grants > 0);
    try std.testing.expect(backend.compaction_stats.compactions > 0);
    try std.testing.expect(countLevelRuns(backend.runs.items, 0) < 3);
}

test "lsm backend max compaction input bytes allows oversized minimum L0 job" {
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
    try std.testing.expect(try backend.runMaintenanceStep());
    const maintenance = backend.snapshotMaintenanceStats();
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
    try std.testing.expectEqual(@as(u64, 1), stats.bulk_append_attempts);
    try std.testing.expectEqual(@as(u64, 4), stats.bulk_append_entries);
    try std.testing.expectEqual(@as(u64, 1), stats.bulk_append_direct_successes);
    try std.testing.expectEqual(@as(u64, 4), stats.bulk_append_direct_entries);
    try std.testing.expectEqual(@as(u64, 0), stats.direct_bulk_ingest_attempts);
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
    try std.testing.expect(backend.maintenanceDebtHint() > 0);
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

    try std.testing.expect(countLevelRuns(backend.runs.items, 0) <= 2);
    try std.testing.expectEqual(@as(usize, 1), backend.compaction_stats.compactions);
    try std.testing.expectEqualStrings("value", try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "doc:0"));
    try std.testing.expectEqualStrings("value", try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "doc:4"));
}

test "lsm backend hard L0 pressure applies one wide step after publish not inside compact false finish" {
    var backend = Backend.init(std.testing.allocator, .{
        .flush_threshold = 1,
        .bulk_ingest_flush_threshold_multiplier = 1,
        .compact_threshold_runs = 1,
        .l0_hard_limit_runs = 2,
        .write_pressure_max_compaction_steps = 1,
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

    try std.testing.expect(countLevelRuns(backend.runs.items, 0) <= backend.options.l0_hard_limit_runs);
    try std.testing.expectEqual(@as(usize, 1), backend.compaction_stats.compactions);
    try std.testing.expect(backend.snapshotWriteStats().write_pressure_compactions > 0);
    try std.testing.expectEqualStrings("value", try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "bulk:0"));
    try std.testing.expectEqualStrings("value", try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "normal:0"));
}

test "lsm backend opt-in hard L0 pressure applies during active bulk flush" {
    var backend = Backend.init(std.testing.allocator, .{
        .flush_threshold = 1,
        .bulk_ingest_flush_threshold_multiplier = 1,
        .compact_threshold_runs = 100,
        .l0_soft_limit_runs = 1,
        .l0_hard_limit_runs = 2,
        .write_pressure_max_compaction_steps = 1,
        .write_pressure_during_bulk_ingest = true,
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

    try std.testing.expect(backend.bulkIngestActive());
    try std.testing.expect(countLevelRuns(backend.runs.items, 0) < 5);
    try std.testing.expect(backend.compaction_stats.compactions > 0);
    try std.testing.expect(backend.snapshotWriteStats().write_pressure_compactions > 0);

    try backend.finishBulkIngestSessionWithOptions(.{ .compact = false });
    bulk_active = false;
    try std.testing.expectEqualStrings("value", try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "bulk:0"));
    try std.testing.expectEqualStrings("value", try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "bulk:4"));
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
    try std.testing.expect(stats.sorted_ingest_runs > 0);
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

        fn readFileTrailerAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, len: usize) !storage_io.FileTrailer {
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

        fn syncFileContentsAbsolute(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().syncFileContentsAbsolute(path);
        }

        fn syncParentAbsolute(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().syncParentAbsolute(path);
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
        .sync_contents_absolute = Context.syncFileContentsAbsolute,
        .sync_parent_absolute = Context.syncParentAbsolute,
        .rename_is_atomic = true,
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
    try std.testing.expectEqual(@as(usize, 0), backend.obsolete_paths.items.len);
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

        fn syncFileContentsAbsolute(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().syncFileContentsAbsolute(path);
        }

        fn syncParentAbsolute(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().syncParentAbsolute(path);
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
        .sync_contents_absolute = CountingHostContext.syncFileContentsAbsolute,
        .sync_parent_absolute = CountingHostContext.syncParentAbsolute,
        .rename_is_atomic = true,
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

        fn readFileTrailerAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, len: usize) !storage_io.FileTrailer {
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

        fn syncFileContentsAbsolute(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().syncFileContentsAbsolute(path);
        }

        fn syncParentAbsolute(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().syncParentAbsolute(path);
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
        .sync_contents_absolute = Context.syncFileContentsAbsolute,
        .sync_parent_absolute = Context.syncParentAbsolute,
        .rename_is_atomic = true,
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

        const filter = try runs.items[0].ensureBloomFilterWithStorage(alloc, backing.storage());
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

        fn readFileTrailerAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, len: usize) !storage_io.FileTrailer {
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

        fn readFileTrailerAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, len: usize) !storage_io.FileTrailer {
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

        const read_stats = backend.snapshotReadStats();
        try std.testing.expectEqual(@as(u64, 1), read_stats.point_run_prechecks);
        try std.testing.expectEqual(@as(u64, 1), read_stats.point_run_precheck_survivors);
        try std.testing.expectEqual(@as(u64, 1), read_stats.point_run_survivor_reads);
        try std.testing.expectEqual(@as(u64, 1), read_stats.point_run_survivor_hits);
        try std.testing.expectEqual(@as(u64, 0), read_stats.point_run_survivor_misses);
        try std.testing.expectEqual(@as(u64, 0), read_stats.point_run_survivor_tombstones);
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

        fn readFileTrailerAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, len: usize) !storage_io.FileTrailer {
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

        fn syncFileContentsAbsolute(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().syncFileContentsAbsolute(path);
        }

        fn syncParentAbsolute(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().syncParentAbsolute(path);
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
        .sync_contents_absolute = Context.syncFileContentsAbsolute,
        .sync_parent_absolute = Context.syncParentAbsolute,
        .rename_is_atomic = true,
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

        const after_forward_scan = backend.snapshotReadStats();
        try std.testing.expectEqualStrings("doc:002", (try cur.seekAtOrAfter("doc:002")).?.key);
        try std.testing.expectEqualStrings("doc:004", (try cur.seekAtOrAfter("doc:004")).?.key);
        try std.testing.expectEqualStrings("doc:003", (try cur.seekAtOrAfter("doc:003")).?.key);

        const read_stats = backend.snapshotReadStats();
        try std.testing.expect(read_stats.table_entry_parses > 0);
        try std.testing.expect(read_stats.table_block_loads > 0);
        try std.testing.expect(read_stats.table_block_bytes > 0);
        try std.testing.expect(read_stats.cursor_block_loads > 0);
        try std.testing.expect(read_stats.cursor_block_reuses > 0);
        try std.testing.expect(read_stats.cursor_block_readaheads > 0);
        try std.testing.expectEqual(@as(u64, 1), read_stats.cursor_table_index_misses);
        try std.testing.expectEqual(after_forward_scan.run_group_builds, read_stats.run_group_builds);
        try std.testing.expectEqual(after_forward_scan.cursor_table_index_misses, read_stats.cursor_table_index_misses);
        try std.testing.expect(read_stats.cursor_table_index_hits > after_forward_scan.cursor_table_index_hits);
        try std.testing.expect(read_stats.cursor_table_index_hits > read_stats.cursor_table_index_misses);
        try std.testing.expect(read_stats.cursor_value_borrows > 0);
        try std.testing.expectEqual(@as(u64, 0), read_stats.cursor_value_copies);
        try std.testing.expectEqual(@as(usize, 0), backend.run_index_cache.items.len);
        const cache_stats = cache.snapshotStats();
        try std.testing.expectEqual(@as(u64, 1), cache_stats.run_table_index.inserts);
        try std.testing.expect(cache_stats.run_table_index.used_bytes > 0);
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
        var reverse_cache = Cache.init(alloc, DefaultCacheSizeBytes);
        defer reverse_cache.deinit();

        var backend = try Backend.open(alloc, root_dir, .{
            .storage = host.storage(),
            .flush_threshold = 6,
            .cache = &reverse_cache,
        });
        defer backend.close();

        var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginRead();
        defer txn.abort();
        var cur = try txn.openCursor();
        defer cur.close();

        try std.testing.expectEqualStrings("doc:004", (try cur.seekAtOrBefore("doc:004")).?.key);
        try std.testing.expectEqualStrings("doc:003", (try cur.prev()).?.key);
        try std.testing.expectEqualStrings("doc:002", (try cur.prev()).?.key);

        const read_stats = backend.snapshotReadStats();
        try std.testing.expect(read_stats.table_entry_parses > 0);
        try std.testing.expect(read_stats.table_block_loads > 0);
        try std.testing.expect(read_stats.table_block_bytes > 0);
        try std.testing.expect(read_stats.cursor_block_loads > 0);
        try std.testing.expect(read_stats.cursor_block_reuses > 0);
        try std.testing.expectEqual(@as(u64, 1), read_stats.cursor_table_index_misses);
        try std.testing.expect(read_stats.cursor_table_index_hits > read_stats.cursor_table_index_misses);
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
    try std.testing.expect(ctx.run_range_reads <= 18);
    try std.testing.expect(ctx.run_trailer_reads <= 8);
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
    try std.testing.expectEqual(read_stats.bloom_negatives, read_stats.prefix_bloom_negatives + read_stats.block_prefix_bloom_negatives);
    try std.testing.expect(read_stats.prefix_bloom_negatives > 0 or read_stats.block_prefix_bloom_negatives > 0);
    try std.testing.expectEqual(@as(u64, 0), read_stats.cursor_block_loads);
    try std.testing.expectEqual(@as(u64, 0), read_stats.table_block_loads);
}

test "lsm backend shared cache point reads search prefix-compressed physical blocks directly" {
    const alloc = std.testing.allocator;
    var backing = storage_io.MemoryStorage.init(alloc);
    defer backing.deinit();
    var cache = Cache.init(alloc, DefaultCacheSizeBytes);
    defer cache.deinit();

    const root_dir = "/lsm-shared-cache-prefix-physical-point";
    const count = 96;
    const keys = try alloc.alloc([]u8, count);
    defer {
        for (keys) |key| alloc.free(key);
        alloc.free(keys);
    }

    {
        var backend = try Backend.open(alloc, root_dir, .{
            .storage = backing.storage(),
            .flush_threshold = count + 1,
            .table_block_compression = .snappy_adaptive,
        });
        defer backend.close();

        var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginWrite();
        for (keys, 0..) |*key_slot, i| {
            const key = try std.fmt.allocPrint(
                alloc,
                "tenant:docs:collection:very-long-shared-prefix:segment:{d:0>6}:field:dense-vector",
                .{i},
            );
            key_slot.* = key;
            try txn.put(key, "v");
        }
        try txn.commit();
        try backend.sync(true);
    }

    var backend = try Backend.open(alloc, root_dir, .{
        .storage = backing.storage(),
        .flush_threshold = count + 1,
        .table_block_compression = .snappy_adaptive,
        .cache = &cache,
    });
    defer backend.close();

    var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime.deinit();

    var txn = try runtime.beginRead();
    defer txn.abort();
    try std.testing.expectEqualStrings("v", try txn.get(keys[37]));

    const stats = cache.snapshotStats();
    try std.testing.expect(stats.run_table_index.inserts > 0);
    try std.testing.expect(stats.run_table_physical_block.inserts > 0);
    try std.testing.expectEqual(@as(u64, 0), stats.run_table_block.inserts);
}

test "lsm backend async point reads issue overlapping survivors in source order" {
    const alloc = std.testing.allocator;
    var backing = storage_io.MemoryStorage.init(alloc);
    defer backing.deinit();
    var cache = Cache.init(alloc, DefaultCacheSizeBytes);
    defer cache.deinit();

    const root_dir = "/lsm-async-point-survivors";
    {
        var backend = try Backend.open(alloc, root_dir, .{
            .storage = backing.storage(),
            .flush_threshold = 1,
            .table_block_compression = .snappy_adaptive,
        });
        defer backend.close();

        var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer runtime.deinit();

        {
            var txn = try runtime.beginWrite();
            try txn.put("tenant:collection:shared-prefix:doc:000001", "older");
            try txn.commit();
            try backend.sync(true);
        }
        {
            var txn = try runtime.beginWrite();
            try txn.put("tenant:collection:shared-prefix:doc:000001", "newer");
            try txn.commit();
            try backend.sync(true);
        }
    }

    var backend = try Backend.open(alloc, root_dir, .{
        .storage = backing.storage(),
        .flush_threshold = 1,
        .table_block_compression = .snappy_adaptive,
        .cache = &cache,
        .max_concurrent_point_block_reads = 4,
    });
    defer backend.close();

    var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime.deinit();

    var txn = try runtime.beginRead();
    defer txn.abort();
    try std.testing.expectEqualStrings("newer", try txn.get("tenant:collection:shared-prefix:doc:000001"));

    const read_stats = backend.snapshotReadStats();
    try std.testing.expect(read_stats.point_run_async_batches > 0);
    try std.testing.expect(read_stats.point_run_async_reads_issued >= 2);
    try std.testing.expect(read_stats.point_run_async_reads_canceled > 0);

    const cache_stats = cache.snapshotStats();
    try std.testing.expect(cache_stats.run_table_physical_block.inserts > 0);
    try std.testing.expectEqual(@as(u64, 0), cache_stats.run_table_block.inserts);
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

        fn readFileTrailerAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, len: usize) !storage_io.FileTrailer {
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

        const read_stats = backend.snapshotReadStats();
        try std.testing.expectEqual(@as(u64, 1), read_stats.point_run_prechecks);
        try std.testing.expectEqual(@as(u64, 1), read_stats.point_run_precheck_survivors);
        try std.testing.expectEqual(@as(u64, 1), read_stats.point_run_survivor_reads);
        try std.testing.expectEqual(@as(u64, 0), read_stats.point_run_survivor_hits);
        try std.testing.expectEqual(@as(u64, 1), read_stats.point_run_survivor_misses);
        try std.testing.expectEqual(@as(u64, 0), read_stats.point_run_survivor_tombstones);
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

test "lsm backend current scan borrows frozen mutable values" {
    var backend = Backend.init(std.testing.allocator, .{
        .flush_threshold = 1024,
        .wal_enabled = false,
    });
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    var write = try runtime.beginWrite();
    try write.put("doc:001", "value-1");
    try write.put("doc:002", "value-2");
    try write.commit();

    const before_scan = backend.snapshotReadStats();
    var scan = try runtime.beginCurrentScan();
    defer scan.abort();
    var cur = try scan.openCursor();
    defer cur.close();

    var maybe_entry = try cur.seekAtOrAfter("doc:001");
    var count: usize = 0;
    while (maybe_entry) |entry| {
        if (!std.mem.startsWith(u8, entry.key, "doc:")) break;
        count += 1;
        maybe_entry = try cur.next();
    }
    try std.testing.expectEqual(@as(usize, 2), count);

    const after_scan = backend.snapshotReadStats();
    try std.testing.expectEqual(@as(u64, 2), after_scan.cursor_value_borrows - before_scan.cursor_value_borrows);
    try std.testing.expectEqual(@as(u64, 0), after_scan.cursor_value_copies - before_scan.cursor_value_copies);
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

test "lsm backend current scan reuses cached mutable read snapshot under rotation threshold" {
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
    try std.testing.expectEqual(before_maintenance.mutable_snapshot_clone_calls + 1, after_open.mutable_snapshot_clone_calls);
    try std.testing.expectEqual(before_maintenance.mutable_snapshot_clone_by_reason[mutableSnapshotReasonIndex(.current_scan)].calls + 1, after_open.mutable_snapshot_clone_by_reason[mutableSnapshotReasonIndex(.current_scan)].calls);
    try std.testing.expect(backend.mutable_read_snapshot != null);

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

test "lsm backend current scan rotates mutable above read snapshot cap" {
    var backend = Backend.init(std.testing.allocator, .{
        .flush_threshold = 1024,
        .read_snapshot_rotate_mutable_bytes = 1,
    });
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

    var scan = try runtime.beginCurrentScan();
    defer scan.abort();

    const after_open = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(before_writes.immutable_rotations + 1, backend.snapshotWriteStats().immutable_rotations);
    try std.testing.expectEqual(@as(usize, 1), backend.activeImmutableMemtableCount());
    try std.testing.expectEqual(before_maintenance.mutable_snapshot_clone_calls, after_open.mutable_snapshot_clone_calls);
    try std.testing.expectEqual(before_maintenance.read_snapshot_mutable_rotations + 1, after_open.read_snapshot_mutable_rotations);

    var cursor = try scan.openCursor();
    defer cursor.close();
    try std.testing.expectEqualStrings("doc:a", (try cursor.seekAtOrAfter("doc:")).?.key);
}

test "lsm backend current scan helpers reuse cached mutable read snapshot" {
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
    try std.testing.expectEqual(before.mutable_snapshot_clone_calls + 1, after.mutable_snapshot_clone_calls);
    try std.testing.expectEqual(before.mutable_snapshot_clone_by_reason[mutableSnapshotReasonIndex(.current_scan)].calls + 1, after.mutable_snapshot_clone_by_reason[mutableSnapshotReasonIndex(.current_scan)].calls);
}

test "lsm backend current scan keeps frozen mutable values across later writes" {
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

    {
        var write = try runtime.beginWrite();
        try write.put("doc:b", "B");
        try write.commit();
    }

    const second = try cursor.next() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("doc:c", second.key);
}

test "lsm backend bulk current scan clones mutable under memory cap" {
    var manager = resource_manager_mod.ResourceManager.init(.{});
    var backend = Backend.init(std.testing.allocator, .{
        .flush_threshold = 1024,
        .bulk_ingest_current_scan_clone_max_bytes = 1024 * 1024,
        .resource_manager = &manager,
    });
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    try backend.beginBulkIngestSession();
    var bulk_active = true;
    defer if (bulk_active) backend.abortBulkIngestSession();

    {
        var write = try runtime.beginWrite();
        try write.put("doc:a", "A");
        try write.put("doc:c", "C");
        try write.commit();
    }

    const before_maintenance = backend.snapshotMaintenanceStats();
    const before_writes = backend.snapshotWriteStats();

    {
        var scan = try runtime.beginCurrentScan();
        defer scan.abort();

        const after_open = backend.snapshotMaintenanceStats();
        try std.testing.expectEqual(before_writes.immutable_rotations, backend.snapshotWriteStats().immutable_rotations);
        try std.testing.expectEqual(@as(usize, 0), backend.activeImmutableMemtableCount());
        try std.testing.expectEqual(before_maintenance.mutable_snapshot_clone_calls + 1, after_open.mutable_snapshot_clone_calls);
        try std.testing.expect(after_open.bulk_ingest_current_scan_clone_active_bytes > 0);
        try std.testing.expectEqual(after_open.bulk_ingest_current_scan_clone_active_bytes, after_open.bulk_ingest_current_scan_clone_peak_active_bytes);
        try std.testing.expectEqual(
            after_open.mutable_bytes +| after_open.immutable_bytes +| after_open.bulk_ingest_current_scan_clone_active_bytes,
            manager.sliceStats(.lsm_in_memory_state).used_bytes,
        );

        var cursor = try scan.openCursor();
        defer cursor.close();

        const first = try cursor.seekAtOrAfter("doc:a") orelse return error.TestUnexpectedResult;
        try std.testing.expectEqualStrings("doc:a", first.key);

        {
            var write = try runtime.beginWrite();
            try write.put("doc:b", "B");
            try write.commit();
        }

        const second = try cursor.next() orelse return error.TestUnexpectedResult;
        try std.testing.expectEqualStrings("doc:c", second.key);
    }

    const after_close = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(@as(u64, 0), after_close.bulk_ingest_current_scan_clone_active_bytes);
    try std.testing.expect(after_close.bulk_ingest_current_scan_clone_peak_active_bytes > 0);
    try std.testing.expectEqual(
        after_close.mutable_bytes +| after_close.immutable_bytes,
        manager.sliceStats(.lsm_in_memory_state).used_bytes,
    );

    try backend.finishBulkIngestSessionWithOptions(.{ .compact = false });
    bulk_active = false;
}

test "lsm backend bulk current scan rotates mutable above aggregate clone memory cap" {
    var backend = Backend.init(std.testing.allocator, .{
        .flush_threshold = 1024,
        .bulk_ingest_current_scan_clone_max_bytes = 1024 * 1024,
        .bulk_ingest_current_scan_clone_total_max_bytes = 1,
    });
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    try backend.beginBulkIngestSession();
    var bulk_active = true;
    defer if (bulk_active) backend.abortBulkIngestSession();

    {
        var write = try runtime.beginWrite();
        try write.put("doc:a", "A");
        try write.commit();
    }

    const before_maintenance = backend.snapshotMaintenanceStats();
    const before_writes = backend.snapshotWriteStats();

    {
        var scan = try runtime.beginCurrentScan();
        defer scan.abort();

        const after_open = backend.snapshotMaintenanceStats();
        try std.testing.expectEqual(before_writes.immutable_rotations + 1, backend.snapshotWriteStats().immutable_rotations);
        try std.testing.expectEqual(@as(usize, 1), backend.activeImmutableMemtableCount());
        try std.testing.expectEqual(before_maintenance.mutable_snapshot_clone_calls, after_open.mutable_snapshot_clone_calls);
        try std.testing.expectEqual(before_maintenance.bulk_ingest_current_scan_clone_budget_denials + 1, after_open.bulk_ingest_current_scan_clone_budget_denials);
        try std.testing.expectEqual(@as(u64, 0), after_open.bulk_ingest_current_scan_clone_active_bytes);

        var cursor = try scan.openCursor();
        defer cursor.close();
        try std.testing.expectEqualStrings("doc:a", (try cursor.seekAtOrAfter("doc:a")).?.key);
    }

    try backend.finishBulkIngestSessionWithOptions(.{ .compact = false });
    bulk_active = false;
}

test "lsm backend bulk current scan rotates mutable above clone memory cap" {
    var backend = Backend.init(std.testing.allocator, .{
        .flush_threshold = 1024,
        .bulk_ingest_current_scan_clone_max_bytes = 1,
    });
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    try backend.beginBulkIngestSession();
    var bulk_active = true;
    defer if (bulk_active) backend.abortBulkIngestSession();

    {
        var write = try runtime.beginWrite();
        try write.put("doc:a", "A");
        try write.commit();
    }

    const before_maintenance = backend.snapshotMaintenanceStats();
    const before_writes = backend.snapshotWriteStats();

    {
        var scan = try runtime.beginCurrentScan();
        defer scan.abort();

        const after_open = backend.snapshotMaintenanceStats();
        try std.testing.expectEqual(before_writes.immutable_rotations + 1, backend.snapshotWriteStats().immutable_rotations);
        try std.testing.expectEqual(@as(usize, 1), backend.activeImmutableMemtableCount());
        try std.testing.expectEqual(before_maintenance.mutable_snapshot_clone_calls, after_open.mutable_snapshot_clone_calls);

        var cursor = try scan.openCursor();
        defer cursor.close();
        try std.testing.expectEqualStrings("doc:a", (try cursor.seekAtOrAfter("doc:a")).?.key);
    }

    try backend.finishBulkIngestSessionWithOptions(.{ .compact = false });
    bulk_active = false;
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
    try std.testing.expectEqual(@as(u64, 0), stats.cursor_block_loads);

    const cache_stats = cache.snapshotStats();
    try std.testing.expect(cache_stats.run_table_block.inserts + cache_stats.run_table_physical_block.inserts > 0);
}

test "lsm backend point getManySorted reuses cached run blocks below sorted threshold" {
    const alloc = std.testing.allocator;
    var storage = storage_io.MemoryStorage.init(alloc);
    defer storage.deinit();
    var cache = Cache.init(alloc, DefaultCacheSizeBytes);
    defer cache.deinit();

    const root_dir = "/lsm-point-batch-forward-cache";
    const count = 64;
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

    const batch_count = 31;
    var key_storage: [batch_count][64]u8 = undefined;
    var keys: [batch_count][]const u8 = undefined;
    var values: [batch_count]?[]const u8 = undefined;
    for (&keys, 0..) |*key, i| {
        key.* = try std.fmt.bufPrint(&key_storage[i], "artifact:{d:0>8}:dense", .{i * 10});
    }

    var read = try backend.beginRead();
    defer read.abort();
    try read.getManySorted(.{ .name = "docs" }, &keys, &values);
    try std.testing.expectEqualStrings("value-0", values[0].?);
    try std.testing.expectEqualStrings("value-30", values[batch_count - 1].?);

    const stats = backend.snapshotReadStats();
    try std.testing.expectEqual(@as(u64, 1), stats.get_many_sorted_plan_point);
    try std.testing.expectEqual(@as(u64, 0), stats.get_many_sorted_plan_sorted_by_run);
    try std.testing.expectEqual(@as(u64, 0), stats.cursor_block_loads);

    const cache_stats = cache.snapshotStats();
    try std.testing.expect(cache_stats.run_table_block.inserts + cache_stats.run_table_physical_block.inserts > 0);
}

test "lsm backend cache-backed batch probes do not require the backend mutex" {
    if (builtin.single_threaded) return error.SkipZigTest;

    const alloc = std.testing.allocator;
    var storage = storage_io.MemoryStorage.init(alloc);
    defer storage.deinit();
    var cache = Cache.init(alloc, DefaultCacheSizeBytes);
    defer cache.deinit();

    const root_dir = "/lsm-cache-batch-with-maintenance-lock";
    {
        var backend = try Backend.open(alloc, root_dir, .{
            .storage = storage.storage(),
            .flush_threshold = 1,
            .cache = &cache,
        });
        defer backend.close();

        var txn = try backend.beginWrite();
        try txn.put(.{ .name = "docs" }, "artifact:00000000:dense", "value-0");
        try txn.commit();
    }

    var backend = try Backend.open(alloc, root_dir, .{
        .storage = storage.storage(),
        .flush_threshold = 1,
        .cache = &cache,
    });
    defer backend.close();

    var read = try backend.beginRead();
    defer read.abort();
    const ReadTxn = @TypeOf(read);
    const Worker = struct {
        const Context = struct {
            txn: *ReadTxn,
            stage: std.atomic.Value(u8) = .init(0),
            value: ?[]const u8 = null,
            result: ?anyerror = null,
        };

        fn run(ctx: *Context) void {
            const keys = [_][]const u8{"artifact:00000000:dense"};
            var values: [1]?[]const u8 = .{null};
            ctx.stage.store(1, .release);
            ctx.txn.getManySorted(.{ .name = "docs" }, &keys, &values) catch |err| {
                ctx.result = err;
                ctx.stage.store(2, .release);
                return;
            };
            ctx.value = values[0];
            ctx.stage.store(2, .release);
        }
    };

    var ctx = Worker.Context{ .txn = &read };
    const locked = runtime_mod.lockBackend(Backend, &backend);
    var thread = try std.Thread.spawn(.{}, Worker.run, .{&ctx});
    var joined = false;
    defer if (!joined) thread.join();

    while (ctx.stage.load(.acquire) == 0) platform.time.yieldBriefly();
    sleepForTest(100 * std.time.ns_per_ms);
    const completed_without_backend_lock = ctx.stage.load(.acquire) == 2;

    runtime_mod.unlockBackend(Backend, &backend, locked);
    thread.join();
    joined = true;

    try std.testing.expect(completed_without_backend_lock);
    if (ctx.result) |err| return err;
    try std.testing.expectEqualStrings("value-0", ctx.value.?);
}

test "lsm backend getManySorted searches prefix-compressed physical blocks directly" {
    const alloc = std.testing.allocator;
    var storage = storage_io.MemoryStorage.init(alloc);
    defer storage.deinit();
    var cache = Cache.init(alloc, DefaultCacheSizeBytes);
    defer cache.deinit();

    const root_dir = "/lsm-batch-prefix-physical-cache";
    const count = 96;
    const keys = try alloc.alloc([]u8, count);
    defer {
        for (keys) |key| alloc.free(key);
        alloc.free(keys);
    }

    {
        var backend = try Backend.open(alloc, root_dir, .{
            .storage = storage.storage(),
            .flush_threshold = count + 1,
            .table_block_compression = .snappy_adaptive,
        });
        defer backend.close();

        var txn = try backend.beginWrite();
        for (keys, 0..) |*key_slot, i| {
            const key = try std.fmt.allocPrint(
                alloc,
                "tenant:docs:collection:very-long-shared-prefix:segment:{d:0>6}:field:dense-vector",
                .{i},
            );
            key_slot.* = key;
            try txn.put(.{ .name = "docs" }, key, "v");
        }
        try txn.commit();
        try backend.sync(true);
    }

    var backend = try Backend.open(alloc, root_dir, .{
        .storage = storage.storage(),
        .flush_threshold = count + 1,
        .table_block_compression = .snappy_adaptive,
        .cache = &cache,
    });
    defer backend.close();

    const values = try alloc.alloc(?[]const u8, count);
    defer alloc.free(values);

    var read = try backend.beginRead();
    defer read.abort();
    try read.getManySorted(.{ .name = "docs" }, keys, values);
    for (values) |maybe_value| try std.testing.expectEqualStrings("v", maybe_value.?);

    const stats = backend.snapshotReadStats();
    try std.testing.expectEqual(@as(u64, 1), stats.get_many_sorted_plan_sorted_by_run);
    try std.testing.expectEqual(@as(u64, 0), stats.cursor_block_loads);

    const cache_stats = cache.snapshotStats();
    try std.testing.expect(cache_stats.run_table_physical_block.inserts > 0);
    try std.testing.expectEqual(@as(u64, 0), cache_stats.run_table_block.inserts);
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

test "lsm backend reclaims unreferenced mutable snapshot generations independently" {
    var backend = Backend.init(std.testing.allocator, .{});
    defer backend.close();

    {
        var writer = try backend.beginWrite();
        try writer.put(.{ .name = "docs" }, "doc:a", "A");
        try writer.commit();
    }

    var read_a = try backend.beginRead();
    var read_a_open = true;
    defer if (read_a_open) read_a.abort();
    try std.testing.expectEqualStrings("A", try read_a.get(.{ .name = "docs" }, "doc:a"));

    {
        var writer = try backend.beginWrite();
        try writer.put(.{ .name = "docs" }, "doc:b", "B");
        try writer.commit();
    }
    try std.testing.expectEqual(@as(usize, 1), backend.retired_mutable_snapshots.items.len);

    {
        var read_b = try backend.beginRead();
        try std.testing.expectEqualStrings("B", try read_b.get(.{ .name = "docs" }, "doc:b"));
        read_b.abort();
    }
    {
        var writer = try backend.beginWrite();
        try writer.put(.{ .name = "docs" }, "doc:c", "C");
        try writer.commit();
    }

    // The reader of generation A must not retain the unreferenced generation B.
    try std.testing.expectEqual(@as(usize, 1), backend.retired_mutable_snapshots.items.len);
    try std.testing.expectEqualStrings("A", try read_a.get(.{ .name = "docs" }, "doc:a"));
    try std.testing.expectError(error.NotFound, read_a.get(.{ .name = "docs" }, "doc:c"));

    read_a.abort();
    read_a_open = false;
    try std.testing.expectEqual(@as(usize, 0), backend.retired_mutable_snapshots.items.len);
    try std.testing.expectEqual(@as(usize, 0), backend.mutable_snapshot_reader_refs.items.len);
    try std.testing.expectEqual(@as(usize, 0), backend.mutable_snapshot_reader_ref_by_state.count());
    try std.testing.expectEqual(@as(usize, 0), backend.retired_mutable_snapshot_by_state.count());
}

test "lsm backend close drains generation readers before teardown" {
    const CloseState = struct {
        backend: *Backend,
        started: std.atomic.Value(bool) = .init(false),
        finished: std.atomic.Value(bool) = .init(false),

        fn run(self: *@This()) void {
            self.started.store(true, .release);
            self.backend.close();
            self.finished.store(true, .release);
        }
    };

    var backend = Backend.init(std.testing.allocator, .{});
    var read = try backend.beginRead();
    var read_released = false;
    var state = CloseState{ .backend = &backend };
    var thread = try std.Thread.spawn(.{}, CloseState.run, .{&state});
    var thread_joined = false;
    defer if (!thread_joined) thread.join();
    defer if (!read_released) read.abort();

    while (!state.started.load(.acquire)) platform.time.yieldBriefly();
    for (0..10) |_| platform.time.yieldBriefly();
    try std.testing.expect(!state.finished.load(.acquire));
    try std.testing.expectError(error.BackendClosing, backend.beginRead());

    read.abort();
    read_released = true;
    thread.join();
    thread_joined = true;
    try std.testing.expect(state.finished.load(.acquire));
}

test "lsm backend resource manager accounts pinned mutable read snapshots" {
    var manager = resource_manager_mod.ResourceManager.init(.{});
    var backend = Backend.init(std.testing.allocator, .{ .resource_manager = &manager });
    defer backend.close();

    {
        var txn = try backend.beginWrite();
        try txn.put(.{ .name = "docs" }, "doc:a", "A");
        try txn.commit();
    }

    const base_bytes = manager.sliceStats(.lsm_in_memory_state).used_bytes;
    try std.testing.expect(base_bytes > 0);

    var read_a = try backend.beginRead();
    var read_a_open = true;
    defer if (read_a_open) read_a.abort();
    try std.testing.expectEqualStrings("A", try read_a.get(.{ .name = "docs" }, "doc:a"));

    const with_snapshot = manager.sliceStats(.lsm_in_memory_state).used_bytes;
    try std.testing.expect(with_snapshot > base_bytes);

    {
        var txn = try backend.beginWrite();
        try txn.put(.{ .name = "docs" }, "doc:b", "B");
        try txn.commit();
    }

    const with_retired = manager.sliceStats(.lsm_in_memory_state).used_bytes;
    try std.testing.expect(with_retired >= with_snapshot);
    try std.testing.expectEqual(@as(?*State, null), backend.mutable_read_snapshot);
    try std.testing.expectEqual(@as(usize, 1), backend.retired_mutable_snapshots.items.len);

    read_a.abort();
    read_a_open = false;

    const after_release = manager.sliceStats(.lsm_in_memory_state).used_bytes;
    const maintenance = backend.snapshotMaintenanceStats();
    try std.testing.expect(after_release < with_retired);
    try std.testing.expectEqual(@as(usize, 0), backend.retired_mutable_snapshots.items.len);
    try std.testing.expectEqual(maintenance.mutable_bytes +| maintenance.immutable_bytes, after_release);
}

test "lsm backend reclaims mutable generations independently of unrelated probe readers" {
    var manager = resource_manager_mod.ResourceManager.init(.{});
    var backend = Backend.init(std.testing.allocator, .{ .resource_manager = &manager });
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    {
        var txn = try backend.beginWrite();
        try txn.put(.{ .name = "docs" }, "doc:a", "A");
        try txn.commit();
    }

    // This reader never borrows a mutable snapshot. It must not extend the
    // lifetime of mutable generations borrowed by the two read transactions.
    var probe = try runtime.beginProbe();
    defer probe.abort();

    var read_a = try backend.beginRead();
    var read_a_open = true;
    defer if (read_a_open) read_a.abort();
    try std.testing.expectEqualStrings("A", try read_a.get(.{ .name = "docs" }, "doc:a"));

    {
        var txn = try backend.beginWrite();
        try txn.put(.{ .name = "docs" }, "doc:b", "B");
        try txn.commit();
    }
    try std.testing.expectEqual(@as(usize, 1), backend.retired_mutable_snapshots.items.len);

    var read_b = try backend.beginRead();
    var read_b_open = true;
    defer if (read_b_open) read_b.abort();
    try std.testing.expectEqualStrings("B", try read_b.get(.{ .name = "docs" }, "doc:b"));

    {
        var txn = try backend.beginWrite();
        try txn.put(.{ .name = "docs" }, "doc:c", "C");
        try txn.commit();
    }
    try std.testing.expectEqual(@as(usize, 2), backend.retired_mutable_snapshots.items.len);
    const both_retired_bytes = manager.sliceStats(.lsm_in_memory_state).used_bytes;

    read_a.abort();
    read_a_open = false;
    try std.testing.expectEqual(@as(usize, 1), backend.retired_mutable_snapshots.items.len);
    try std.testing.expect(manager.sliceStats(.lsm_in_memory_state).used_bytes < both_retired_bytes);

    read_b.abort();
    read_b_open = false;
    try std.testing.expectEqual(@as(usize, 0), backend.retired_mutable_snapshots.items.len);
    try std.testing.expectEqual(@as(usize, 1), backend.active_readers_by_kind[readerPinKindIndex(.probe_txn)]);
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
        try std.testing.expectEqual(@as(usize, 0), backend.activeVersionReaders());
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
        try std.testing.expectEqual(@as(usize, 2), backend.active_readers);
        try std.testing.expectEqual(@as(usize, 1), backend.activeVersionReaders());
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

test "lsm backend write txn guard does not retain obsolete versions" {
    var backend = Backend.init(std.testing.allocator, .{});
    defer backend.close();

    {
        var seed = try backend.beginWrite();
        try seed.put(.{ .name = "docs" }, "doc:a", "A");
        try seed.commit();
    }

    var long_write = try backend.beginWrite();
    defer long_write.abort();
    try std.testing.expectEqual(@as(usize, 1), backend.active_readers);
    try std.testing.expectEqual(@as(usize, 0), backend.activeVersionReaders());

    {
        var read = try backend.beginRead();
        try std.testing.expectEqualStrings("A", try read.get(.{ .name = "docs" }, "doc:a"));
        read.abort();
    }
    try std.testing.expect(backend.mutable_read_snapshot != null);

    {
        var writer = try backend.beginWrite();
        try writer.put(.{ .name = "docs" }, "doc:b", "B");
        try writer.commit();
    }

    try std.testing.expectEqual(@as(?*State, null), backend.mutable_read_snapshot);
    try std.testing.expectEqual(@as(usize, 0), backend.retired_mutable_snapshots.items.len);
    try std.testing.expectEqualStrings("A", try long_write.get(.{ .name = "docs" }, "doc:a"));
    const stats = backend.snapshotReadStats();
    try std.testing.expect(stats.point_value_copies > 0);
    try std.testing.expectEqual(@as(u64, 0), stats.point_value_borrows);
}

test "lsm backend close reclaims eligible queued obsolete files" {
    var path_buf: [256]u8 = undefined;
    const path = repository_mod.tmpPath(&path_buf, "obsolete-retain");
    defer repository_mod.cleanupTmp(path);

    var backend = try Backend.open(std.testing.allocator, std.mem.span(path), .{ .obsolete_retention_ns = 0 });
    const obsolete_path = try repository_mod.runPath(std.testing.allocator, std.mem.span(path), 9999);
    defer std.testing.allocator.free(obsolete_path);
    try repository_mod.writeFileAbsoluteWithStorage(backend.storage.?, obsolete_path, "obsolete");
    try backend.queueObsoleteFilePath(try std.testing.allocator.dupe(u8, obsolete_path));
    backend.close();

    var native = try storage_io.NativeStorage.init(std.heap.page_allocator, .threaded);
    defer native.deinit();
    try std.testing.expectError(error.FileNotFound, native.storage().readFileAlloc(std.testing.allocator, obsolete_path, 1024));
}

test "lsm backend open removes recovered atomic table temp files" {
    var path_buf: [256]u8 = undefined;
    const path = repository_mod.tmpPath(&path_buf, "recovered-table-temp");
    defer repository_mod.cleanupTmp(path);

    var native = try storage_io.NativeStorage.init(std.heap.page_allocator, .threaded);
    defer native.deinit();

    const root_dir = std.mem.span(path);
    const runs_dir = try std.fs.path.join(std.testing.allocator, &.{ root_dir, "runs" });
    defer std.testing.allocator.free(runs_dir);
    try native.storage().createDirPath(runs_dir);

    const live_run_path = try std.fs.path.join(std.testing.allocator, &.{ runs_dir, "1.tbl" });
    defer std.testing.allocator.free(live_run_path);
    const stale_tmp_path = try std.fs.path.join(std.testing.allocator, &.{ runs_dir, "1.tbl.tmp-42" });
    defer std.testing.allocator.free(stale_tmp_path);
    const malformed_tmp_path = try std.fs.path.join(std.testing.allocator, &.{ runs_dir, "not-a-run.tbl.tmp-42" });
    defer std.testing.allocator.free(malformed_tmp_path);

    try repository_mod.writeFileAbsoluteWithStorage(native.storage(), live_run_path, "live");
    try repository_mod.writeFileAbsoluteWithStorage(native.storage(), stale_tmp_path, "stale");
    try repository_mod.writeFileAbsoluteWithStorage(native.storage(), malformed_tmp_path, "malformed");

    var backend = try Backend.open(std.testing.allocator, root_dir, .{});
    const open_stats = backend.snapshotOpenStats();
    try std.testing.expectEqual(@as(u64, 1), open_stats.recovered_table_temp_files_deleted);
    try std.testing.expect(open_stats.recovered_table_temp_bytes_deleted > 0);
    try std.testing.expectEqual(@as(u64, 1), open_stats.recovered_table_temp_files_deleted_before_replay);
    try std.testing.expect(open_stats.recovered_table_temp_bytes_deleted_before_replay > 0);
    try std.testing.expect(open_stats.cleaning_recovered_run_temps_ns > 0);
    backend.close();

    try std.testing.expectError(error.FileNotFound, native.storage().readFileAlloc(std.testing.allocator, stale_tmp_path, 1024));
    const live = try native.storage().readFileAlloc(std.testing.allocator, live_run_path, 1024);
    defer std.testing.allocator.free(live);
    try std.testing.expectEqualStrings("live", live);
    const malformed = try native.storage().readFileAlloc(std.testing.allocator, malformed_tmp_path, 1024);
    defer std.testing.allocator.free(malformed);
    try std.testing.expectEqualStrings("malformed", malformed);
}

test "lsm backend cleans recovered table temp files before wal replay" {
    var path_buf: [256]u8 = undefined;
    const path = repository_mod.tmpPath(&path_buf, "recovered-table-temp-before-replay");
    defer repository_mod.cleanupTmp(path);

    var native = try storage_io.NativeStorage.init(std.heap.page_allocator, .threaded);
    defer native.deinit();

    const root_dir = std.mem.span(path);
    try native.storage().createDirPath(root_dir);
    const runs_dir = try std.fs.path.join(std.testing.allocator, &.{ root_dir, "runs" });
    defer std.testing.allocator.free(runs_dir);
    try native.storage().createDirPath(runs_dir);

    const stale_tmp_path = try std.fs.path.join(std.testing.allocator, &.{ runs_dir, "1.tbl.tmp-42" });
    defer std.testing.allocator.free(stale_tmp_path);
    try repository_mod.writeFileAbsoluteWithStorage(native.storage(), stale_tmp_path, "stale");

    var state: State = .{};
    defer state.deinit(std.testing.allocator);
    try state.upsert(std.testing.allocator, .{ .name = "docs" }, "doc:a", "alpha", false);
    _ = try wal_mod.appendStateWithOptions(
        native.storage(),
        std.testing.allocator,
        root_dir,
        &state,
        false,
        .{ .segment_bytes = 512 },
    );

    var backend = try Backend.open(std.testing.allocator, root_dir, .{
        .flush_threshold = 1024,
        .storage = native.storage(),
    });
    defer backend.close();

    const open_stats = backend.snapshotOpenStats();
    try std.testing.expect(open_stats.wal_replay_records > 0);
    try std.testing.expect(open_stats.wal_replay_entries > 0);
    try std.testing.expectEqual(@as(u64, 1), open_stats.mutable_entries_after_replay);
    try std.testing.expectEqual(@as(u64, 1), open_stats.recovered_table_temp_files_deleted);
    try std.testing.expectEqual(@as(u64, 1), open_stats.recovered_table_temp_files_deleted_before_replay);
    try std.testing.expect(open_stats.recovered_table_temp_bytes_deleted_before_replay > 0);
    try std.testing.expect(open_stats.cleaning_recovered_run_temps_ns > 0);

    try std.testing.expectError(error.FileNotFound, native.storage().readFileAlloc(std.testing.allocator, stale_tmp_path, 1024));
    try std.testing.expectEqualStrings("alpha", try backend.getMergedWithMutable(&backend.mutable, .{ .name = "docs" }, "doc:a"));
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
        const loaded_filter = try runs.items[0].ensureBloomFilterWithStorage(alloc, host.storage());
        try std.testing.expect(lsm_table_file.maybeContains(loaded_filter, "docs", "doc:a"));
        try std.testing.expectEqual(@as(usize, 0), obsolete_paths.items.len);
    }

    try std.testing.expect(ctx.run_reads >= 4);
}

test "lsm repository accepts manifests without run bloom filters" {
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

    try std.testing.expect(try repository_mod.loadManifestIfPresentWithStorage(
        backing.storage(),
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

        fn readFileTrailerAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, len: usize) !storage_io.FileTrailer {
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

    try std.testing.expectEqual(@as(usize, 2), index.entryCount());
    try std.testing.expectEqual(@as(usize, 2), index.block_entry_offsets.len);
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
        .backend = .{ .read_only = true },
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

test "lsm backend rejects concurrent writable opens for one native root" {
    var path_buf: [256]u8 = undefined;
    const path = repository_mod.tmpPath(&path_buf, "single-writer-root");
    defer repository_mod.cleanupTmp(path);

    var writer = try Backend.open(std.testing.allocator, std.mem.span(path), .{
        .flush_threshold = 1,
    });
    defer writer.close();

    {
        var runtime = try writer.runtimeStore(std.testing.allocator, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginWrite();
        try txn.put("doc:a", "A");
        try txn.commit();
    }

    try std.testing.expectError(error.LsmRootWriterAlreadyOpen, Backend.open(std.testing.allocator, std.mem.span(path), .{}));

    const root_path = std.mem.span(path);
    const same_root_path = try std.fs.path.join(std.testing.allocator, &.{
        std.fs.path.dirname(root_path) orelse ".",
        ".",
        std.fs.path.basename(root_path),
    });
    defer std.testing.allocator.free(same_root_path);
    try std.testing.expectError(error.LsmRootWriterAlreadyOpen, Backend.open(std.testing.allocator, same_root_path, .{}));

    var reader = try Backend.open(std.testing.allocator, std.mem.span(path), .{
        .backend = .{
            .read_only = true,
            .create_if_missing = false,
        },
    });
    defer reader.close();

    var runtime = try reader.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();
    var read_txn = try runtime.beginRead();
    defer read_txn.abort();
    try std.testing.expectEqualStrings("A", try read_txn.get("doc:a"));
}

test "lsm backend cross-process writer lock child helper" {
    const root_dir = platform.env.getenv("ANTFLY_LSM_WRITER_LOCK_CHILD_ROOT") orelse return error.SkipZigTest;
    const ready_path = platform.env.getenv("ANTFLY_LSM_WRITER_LOCK_CHILD_READY") orelse return error.SkipZigTest;
    const release_path = platform.env.getenv("ANTFLY_LSM_WRITER_LOCK_CHILD_RELEASE") orelse return error.SkipZigTest;

    var writer = try Backend.open(std.testing.allocator, root_dir, .{
        .flush_threshold = 1,
    });
    defer writer.close();

    try writeMarkerForTest(ready_path);
    try waitForPathForTest(release_path, 30 * std.time.ns_per_s);
}

test "lsm backend native writer lock rejects writable opens across processes" {
    if (builtin.os.tag == .freestanding or builtin.os.tag == .wasi or builtin.os.tag == .windows) return error.SkipZigTest;

    var path_buf: [256]u8 = undefined;
    const path = repository_mod.tmpPath(&path_buf, "cross-process-single-writer-root");
    defer repository_mod.cleanupTmp(path);

    const root_path = std.mem.span(path);
    const child_ready = try makePipeForTest();
    defer {
        _ = std.posix.system.close(child_ready[0]);
        _ = std.posix.system.close(child_ready[1]);
    }
    const child_release = try makePipeForTest();
    defer {
        _ = std.posix.system.close(child_release[0]);
        _ = std.posix.system.close(child_release[1]);
    }

    const pid = std.posix.system.fork();
    if (pid == 0) {
        _ = std.posix.system.close(child_ready[0]);
        _ = std.posix.system.close(child_release[1]);
        var writer = Backend.open(std.heap.page_allocator, root_path, .{ .flush_threshold = 1 }) catch std.posix.system.exit(1);
        writeSignalForTest(child_ready[1]) catch std.posix.system.exit(2);
        waitSignalForTest(child_release[0]) catch std.posix.system.exit(3);
        writer.close();
        std.posix.system.exit(0);
    }
    if (pid < 0) return error.Unexpected;
    var child_released = false;
    defer if (!child_released) writeSignalForTest(child_release[1]) catch {};

    try waitSignalForTest(child_ready[0]);

    const same_root_path = try std.fs.path.join(std.testing.allocator, &.{
        std.fs.path.dirname(root_path) orelse ".",
        ".",
        std.fs.path.basename(root_path),
    });
    defer std.testing.allocator.free(same_root_path);
    try std.testing.expectError(error.LsmRootWriterAlreadyOpen, Backend.open(std.testing.allocator, same_root_path, .{}));

    try writeSignalForTest(child_release[1]);
    child_released = true;

    const WaitStatus = if (builtin.link_libc) c_int else u32;
    var status: WaitStatus = 0;
    while (true) switch (std.posix.errno(std.posix.system.waitpid(@intCast(pid), &status, 0))) {
        .SUCCESS => break,
        .INTR => continue,
        else => return error.Unexpected,
    };
    try std.testing.expectEqual(@as(WaitStatus, 0), status);
}

test "lsm backend wal operation lock blocks read-only replay during live append critical section" {
    var path_buf: [256]u8 = undefined;
    const path = repository_mod.tmpPath(&path_buf, "wal-operation-lock-blocks-replay");
    defer repository_mod.cleanupTmp(path);

    var writer = try Backend.open(std.testing.allocator, std.mem.span(path), .{
        .flush_threshold = 1024,
    });
    defer writer.close();

    var reader = try Backend.open(std.testing.allocator, std.mem.span(path), .{
        .backend = .{
            .read_only = true,
            .create_if_missing = false,
        },
    });
    defer reader.close();

    var held = try writer.acquireWalOperationLock(.exclusive);
    var held_active = true;
    defer if (held_active) held.release();

    const blocked = try reader.tryAcquireWalOperationLock(.shared);
    try std.testing.expect(blocked == null);

    held.release();
    held_active = false;

    var acquired = (try reader.tryAcquireWalOperationLock(.shared)) orelse return error.TestExpectedWalOperationLock;
    defer acquired.release();
}

test "lsm backend read-only native open creates missing wal operation lock for legacy roots" {
    if (builtin.os.tag == .freestanding or builtin.os.tag == .wasi) return error.SkipZigTest;

    var path_buf: [256]u8 = undefined;
    const path = repository_mod.tmpPath(&path_buf, "read-only-creates-missing-wal-lock");
    defer repository_mod.cleanupTmp(path);

    const root_path = std.mem.span(path);
    {
        var writer = try Backend.open(std.testing.allocator, root_path, .{
            .flush_threshold = 1024,
        });
        defer writer.close();

        var runtime = try writer.runtimeStore(std.testing.allocator, .{ .name = "docs" });
        defer runtime.deinit();
        var txn = try runtime.beginWrite();
        try txn.put("doc:a", "A");
        try txn.commit();
    }

    const lock_path = try walOperationLockPathAlloc(std.testing.allocator, root_path);
    defer std.testing.allocator.free(lock_path);
    try std.Io.Dir.deleteFileAbsolute(std.testing.io, lock_path);
    try std.testing.expect(!pathExistsForTest(lock_path));

    var reader = try Backend.open(std.testing.allocator, root_path, .{
        .backend = .{
            .read_only = true,
            .create_if_missing = false,
        },
    });
    defer reader.close();
    try std.testing.expect(pathExistsForTest(lock_path));

    var writer = try Backend.open(std.testing.allocator, root_path, .{
        .flush_threshold = 1024,
    });
    defer writer.close();
    try std.testing.expect(pathExistsForTest(lock_path));

    var held = try reader.acquireWalOperationLock(.shared);
    var held_active = true;
    defer if (held_active) held.release();

    const blocked = try writer.tryAcquireWalOperationLock(.exclusive);
    try std.testing.expect(blocked == null);

    held.release();
    held_active = false;

    var acquired = (try writer.tryAcquireWalOperationLock(.exclusive)) orelse return error.TestExpectedWalOperationLock;
    defer acquired.release();
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

        fn syncFileContentsAbsolute(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().syncFileContentsAbsolute(path);
        }

        fn syncParentAbsolute(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().syncParentAbsolute(path);
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
        .sync_contents_absolute = Context.syncFileContentsAbsolute,
        .sync_parent_absolute = Context.syncParentAbsolute,
        .rename_is_atomic = true,
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

test "lsm backend reclaims an obsolete generation while a newer reader remains active" {
    const alloc = std.testing.allocator;
    var backing = storage_io.MemoryStorage.init(alloc);
    defer backing.deinit();

    const root_dir = "/memory/lsm-reader-generation-obsolete-gc";
    const first_run_path = try repository_mod.runPath(alloc, root_dir, 1);
    defer alloc.free(first_run_path);

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

    var old_reader = try runtime.beginRead();
    try std.testing.expectEqualStrings("A", try old_reader.get("doc:a"));

    {
        var txn = try runtime.beginWrite();
        try txn.delete("doc:a");
        try txn.put("doc:b", "B");
        try txn.commit();
    }

    var new_reader = try runtime.beginRead();
    try std.testing.expectEqualStrings("B", try new_reader.get("doc:b"));
    try std.testing.expectEqual(@as(usize, 2), backend.active_readers);
    try std.testing.expect(!backend.pathTrackedByActiveRunsLocked(first_run_path));
    {
        const bytes = try backing.storage().readFileAlloc(alloc, first_run_path, 1024);
        defer alloc.free(bytes);
        try std.testing.expect(bytes.len > 0);
    }

    old_reader.abort();
    try std.testing.expectEqual(@as(usize, 1), backend.active_readers);
    try std.testing.expect(!run_snapshot_refs.isRetained(first_run_path));
    if (backing.storage().readFileAlloc(alloc, first_run_path, 1024)) |unexpected| {
        alloc.free(unexpected);
        return error.TestExpectedObsoleteRunReclaimed;
    } else |err| {
        try std.testing.expectEqual(error.FileNotFound, err);
    }
    try std.testing.expectEqualStrings("B", try new_reader.get("doc:b"));

    new_reader.abort();
    try std.testing.expectEqual(@as(usize, 0), backend.active_readers);
}

test "lsm backend open manifest version refs pin obsolete files across handles" {
    const alloc = std.testing.allocator;
    var backing = storage_io.MemoryStorage.init(alloc);
    defer backing.deinit();

    const root_dir = "/memory/lsm-version-ref-obsolete-gc";
    const obsolete_path = try repository_mod.runPath(alloc, root_dir, 1);
    defer alloc.free(obsolete_path);

    var writer = try Backend.open(alloc, root_dir, .{
        .storage = backing.storage(),
        .flush_threshold = 1,
        .compact_threshold_runs = 1,
        .foreground_soft_compaction = true,
        .obsolete_retention_ns = 0,
    });
    defer writer.close();

    {
        var runtime = try writer.runtimeStore(alloc, .{ .name = "docs" });
        defer runtime.deinit();
        var txn = try runtime.beginWrite();
        try txn.put("doc:a", "A");
        try txn.commit();
    }

    var reader = try Backend.open(alloc, root_dir, .{
        .storage = backing.storage(),
        .backend = .{ .read_only = true },
        .obsolete_retention_ns = 0,
    });

    {
        var runtime = try writer.runtimeStore(alloc, .{ .name = "docs" });
        defer runtime.deinit();
        var txn = try runtime.beginWrite();
        try txn.delete("doc:a");
        try txn.put("doc:b", "B");
        try txn.commit();
    }

    var stats = writer.snapshotMaintenanceStats();
    try std.testing.expectEqual(@as(u64, 1), stats.obsolete_paths);
    try std.testing.expectEqual(@as(u64, 1), stats.obsolete_paths_pinned_by_versions);
    try std.testing.expectEqual(@as(u64, 0), stats.obsolete_paths_reclaimable);
    // The obsolete path is durably recorded but is not itself dirty while an
    // open generation pins it. Maintenance must wait for the pin release
    // instead of rewriting an identical manifest on every scheduler pass.
    try std.testing.expect(!stats.obsolete_manifest_dirty);
    const pinned_manifest_writes = writer.write_stats.manifest_writes;
    try std.testing.expect(!try writer.runMaintenanceStep());
    try std.testing.expectEqual(pinned_manifest_writes, writer.write_stats.manifest_writes);
    {
        const bytes = try backing.storage().readFileAlloc(alloc, obsolete_path, 1024);
        defer alloc.free(bytes);
        try std.testing.expect(bytes.len > 0);
    }

    reader.close();
    try std.testing.expect(try writer.runMaintenanceStep());
    stats = writer.snapshotMaintenanceStats();
    try std.testing.expectEqual(@as(u64, 0), stats.obsolete_paths);
    try std.testing.expectEqual(@as(u64, 0), stats.obsolete_paths_pinned_by_versions);
    try std.testing.expectError(error.FileNotFound, backing.storage().readFileAlloc(alloc, obsolete_path, 1024));
}

test "lsm backend reopen reclaim stress preserves manifest referenced runs" {
    const alloc = std.testing.allocator;
    var backing = storage_io.MemoryStorage.init(alloc);
    defer backing.deinit();

    const root_dir = "/memory/lsm-reopen-reclaim-stress";

    for (0..8) |i| {
        var writer = try Backend.open(alloc, root_dir, .{
            .storage = backing.storage(),
            .flush_threshold = 1,
            .compact_threshold_runs = 1,
            .foreground_soft_compaction = true,
            .obsolete_retention_ns = 0,
        });
        var writer_open = true;
        defer if (writer_open) writer.close();

        {
            var runtime = try writer.runtimeStore(alloc, .{ .name = "docs" });
            defer runtime.deinit();
            var seed_txn = try runtime.beginWrite();
            const seed_value = try std.fmt.allocPrint(alloc, "value-{d}-seed", .{i});
            defer alloc.free(seed_value);
            try seed_txn.put("doc:stable", seed_value);
            try seed_txn.commit();
        }

        var reader = try Backend.open(alloc, root_dir, .{
            .storage = backing.storage(),
            .backend = .{ .read_only = true },
            .obsolete_retention_ns = 0,
        });
        var reader_open = true;
        defer if (reader_open) reader.close();

        {
            var runtime = try writer.runtimeStore(alloc, .{ .name = "docs" });
            defer runtime.deinit();
            var update_txn = try runtime.beginWrite();
            const update_value = try std.fmt.allocPrint(alloc, "value-{d}-updated", .{i});
            defer alloc.free(update_value);
            try update_txn.put("doc:stable", update_value);
            try update_txn.commit();
        }

        const pinned = writer.snapshotMaintenanceStats();
        try std.testing.expect(pinned.obsolete_paths == 0 or pinned.obsolete_paths_pinned_by_versions > 0);
        reader.close();
        reader_open = false;
        while (try writer.runMaintenanceStep()) {}

        const clean = writer.snapshotMaintenanceStats();
        try std.testing.expectEqual(@as(u64, 0), clean.obsolete_paths_pinned_by_versions);
        writer.close();
        writer_open = false;
    }

    var final_reader = try Backend.open(alloc, root_dir, .{
        .storage = backing.storage(),
        .backend = .{ .read_only = true },
        .obsolete_retention_ns = 0,
    });
    defer final_reader.close();
    var runtime = try final_reader.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime.deinit();
    var txn = try runtime.beginRead();
    defer txn.abort();
    try std.testing.expectEqualStrings("value-7-updated", try txn.get("doc:stable"));
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

test "lsm backend maintenance step reclaims expired clean obsolete paths" {
    const alloc = std.testing.allocator;
    var backing = storage_io.MemoryStorage.init(alloc);
    defer backing.deinit();

    const root_dir = "/memory/lsm-maintenance-clean-obsolete-gc";
    const obsolete_path = try repository_mod.runPath(alloc, root_dir, 999);
    defer alloc.free(obsolete_path);

    var backend = try Backend.open(alloc, root_dir, .{
        .storage = backing.storage(),
        .obsolete_retention_ns = 0,
    });
    defer backend.close();

    try repository_mod.writeFileAbsoluteWithStorage(backend.storage.?, obsolete_path, "obsolete");
    try backend.queueObsoleteFilePath(try alloc.dupe(u8, obsolete_path));
    backend.manifest_dirty = false;

    try std.testing.expectEqual(@as(usize, 1), backend.obsolete_paths.items.len);
    try std.testing.expect(try backend.runMaintenanceStep());
    try std.testing.expectEqual(@as(usize, 0), backend.obsolete_paths.items.len);
    try std.testing.expectError(error.FileNotFound, backing.storage().readFileAlloc(alloc, obsolete_path, 1024));
}

test "lsm backend maintenance step prioritizes due obsolete reclaim before compaction" {
    const alloc = std.testing.allocator;
    var backing = storage_io.MemoryStorage.init(alloc);
    defer backing.deinit();

    const root_dir = "/memory/lsm-maintenance-obsolete-before-compaction";
    var backend = try Backend.open(alloc, root_dir, .{
        .storage = backing.storage(),
        .flush_threshold = 1,
        .defer_flush_on_commit = true,
        .l0_soft_limit_runs = 100,
        .obsolete_retention_ns = 0,
    });
    defer backend.close();

    var key_buf: [16]u8 = undefined;
    for (0..4) |i| {
        var txn = try backend.beginWrite();
        const key = try std.fmt.bufPrint(&key_buf, "k:{d}", .{i});
        try txn.put(.{ .name = "docs" }, key, "value");
        try txn.commit();
        try backend.sync(true);
    }
    try std.testing.expect(countLevelRuns(backend.runs.items, 0) > 1);

    backend.options.l0_soft_limit_runs = 1;

    const obsolete_path = try repository_mod.runPath(alloc, root_dir, 999);
    defer alloc.free(obsolete_path);
    try repository_mod.writeFileAbsoluteWithStorage(backend.storage.?, obsolete_path, "obsolete");
    try backend.queueObsoleteFilePath(try alloc.dupe(u8, obsolete_path));
    backend.manifest_dirty = false;

    const before_compactions = backend.compaction_stats.compactions;
    try std.testing.expect(try backend.runMaintenanceStep());
    try std.testing.expectEqual(before_compactions, backend.compaction_stats.compactions);
    try std.testing.expectEqual(@as(usize, 0), backend.obsolete_paths.items.len);
    try std.testing.expectError(error.FileNotFound, backing.storage().readFileAlloc(alloc, obsolete_path, 1024));
}

test "lsm backend finalization prioritizes due obsolete reclaim before compaction" {
    const alloc = std.testing.allocator;
    var backing = storage_io.MemoryStorage.init(alloc);
    defer backing.deinit();

    const root_dir = "/memory/lsm-finalize-obsolete-before-compaction";
    var backend = try Backend.open(alloc, root_dir, .{
        .storage = backing.storage(),
        .flush_threshold = 1,
        .defer_flush_on_commit = true,
        .l0_soft_limit_runs = 100,
        .obsolete_retention_ns = 0,
    });
    defer backend.close();

    var key_buf: [16]u8 = undefined;
    for (0..4) |i| {
        var txn = try backend.beginWrite();
        const key = try std.fmt.bufPrint(&key_buf, "k:{d}", .{i});
        try txn.put(.{ .name = "docs" }, key, "value");
        try txn.commit();
        try backend.sync(true);
    }
    try std.testing.expect(countLevelRuns(backend.runs.items, 0) > 1);

    backend.options.l0_soft_limit_runs = 1;

    const obsolete_path = try repository_mod.runPath(alloc, root_dir, 1000);
    defer alloc.free(obsolete_path);
    try repository_mod.writeFileAbsoluteWithStorage(backend.storage.?, obsolete_path, "obsolete");
    try backend.queueObsoleteFilePath(try alloc.dupe(u8, obsolete_path));
    backend.manifest_dirty = false;

    const before_compactions = backend.compaction_stats.compactions;
    try backend.finalizeDeferredStorageWork();
    try std.testing.expectEqual(before_compactions, backend.compaction_stats.compactions);
    try std.testing.expectEqual(@as(usize, 0), backend.obsolete_paths.items.len);
    try std.testing.expectError(error.FileNotFound, backing.storage().readFileAlloc(alloc, obsolete_path, 1024));
}

test "lsm backend internal worker reclaims idle obsolete paths after retention" {
    if (builtin.os.tag == .freestanding or builtin.single_threaded) return;

    const alloc = std.testing.allocator;
    var backing = storage_io.MemoryStorage.init(alloc);
    defer backing.deinit();

    const root_dir = "/memory/lsm-worker-idle-obsolete-gc";
    const obsolete_path = try repository_mod.runPath(alloc, root_dir, 999);
    defer alloc.free(obsolete_path);

    var handle = try BackendHandle.openWithConfig(
        alloc,
        root_dir,
        .{
            .storage = backing.storage(),
            // MemoryStorage advances time per nowNs call, so one tick is enough
            // to exercise delayed worker reclamation without relying on many
            // scheduler turns under the shared test suite.
            .obsolete_retention_ns = 1,
        },
        .{ .internal_flush_worker = true },
    );
    defer handle.close();

    const backend = handle.ptr();
    try repository_mod.writeFileAbsoluteWithStorage(backend.storage.?, obsolete_path, "obsolete");
    {
        const locked = runtime_mod.lockBackend(Backend, backend);
        defer runtime_mod.unlockBackend(Backend, backend, locked);
        try backend.queueObsoleteFilePath(try alloc.dupe(u8, obsolete_path));
        backend.manifest_dirty = false;
    }

    var reclaimed = false;
    var attempts: usize = 0;
    while (attempts < 200) : (attempts += 1) {
        const bytes = backing.storage().readFileAlloc(alloc, obsolete_path, 1024) catch |err| switch (err) {
            error.FileNotFound => {
                reclaimed = true;
                break;
            },
            else => return err,
        };
        alloc.free(bytes);
        sleepForTest(5 * std.time.ns_per_ms);
    }

    const worker_stats = handle.stopInternalFlushWorkerForTest().?;
    try std.testing.expect(reclaimed);
    try std.testing.expect(worker_stats.maintenance_steps > 0);
    try std.testing.expectEqual(@as(u64, 0), worker_stats.errors);
    try std.testing.expectEqual(@as(usize, 0), backend.obsolete_paths.items.len);
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

        const maintenance = backend.snapshotMaintenanceStats();
        try std.testing.expect(maintenance.current_manifest_bytes > 0);
    }

    {
        var reopened = try Backend.open(std.testing.allocator, root_dir, .{
            .flush_threshold = 1,
            .storage = memory_storage.storage(),
        });
        defer reopened.close();

        const open_stats = reopened.snapshotOpenStats();
        try std.testing.expectEqual(Backend.OpenPhase.ready, open_stats.phase);
        try std.testing.expect(open_stats.loaded_manifest);
        try std.testing.expect(open_stats.loaded_runs > 0);
        try std.testing.expect(reopened.snapshotMaintenanceStats().current_manifest_bytes > 0);

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

test "lsm backend physically bounds metadata-heavy flush runs" {
    const alloc = std.testing.allocator;
    var memory_storage = storage_io.MemoryStorage.init(alloc);
    defer memory_storage.deinit();

    const root_dir = "/memory/physical-size-bounded-flush";
    const count = 64;
    var backend = try Backend.open(alloc, root_dir, .{
        .flush_threshold = count,
        .compact_threshold_runs = 100,
        .max_run_file_bytes = 1024 * 1024,
        .max_run_file_physical_bytes = 1024,
        .storage = memory_storage.storage(),
    });
    defer backend.close();

    var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime.deinit();
    var txn = try runtime.beginWrite();
    var key_buf: [32]u8 = undefined;
    for (0..count) |i| {
        const key = try std.fmt.bufPrint(&key_buf, "tenant:{d:0>4}", .{i});
        try txn.put(key, "v");
    }
    try txn.commit();

    try std.testing.expect(backend.runs.items.len > 1);
    var total_entries: usize = 0;
    for (backend.runs.items) |run| {
        try std.testing.expect(run.size_bytes <= 1024);
        total_entries += run.entry_count;
    }
    try std.testing.expectEqual(@as(usize, count), total_entries);
}

test "lsm backend partitions persisted runs at configured key family boundaries" {
    const alloc = std.testing.allocator;
    var memory_storage = storage_io.MemoryStorage.init(alloc);
    defer memory_storage.deinit();

    const root_dir = "/memory/key-family-partition";
    const options = Options{
        .flush_threshold = 3,
        .compact_threshold_runs = 100,
        .max_run_file_bytes = 1024 * 1024,
        .run_partition_prefix_bytes = 1,
        .storage = memory_storage.storage(),
    };
    {
        var backend = try Backend.open(alloc, root_dir, options);
        defer backend.close();

        var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer runtime.deinit();
        var txn = try runtime.beginWrite();
        try txn.put("\x00metadata", "m");
        try txn.put("\x01document", "d");
        try txn.put("\x03artifact", "a");
        try txn.commit();

        try std.testing.expectEqual(@as(usize, 3), backend.runs.items.len);
        for (backend.runs.items) |run| {
            try std.testing.expectEqual(@as(usize, 1), run.entry_count);
            try std.testing.expectEqual(run.smallest_key[0], run.largest_key[0]);
        }
    }

    var reopened = try Backend.open(alloc, root_dir, options);
    defer reopened.close();
    var runtime = try reopened.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime.deinit();
    var read = try runtime.beginRead();
    defer read.abort();
    try std.testing.expectEqualStrings("m", try read.get("\x00metadata"));
    try std.testing.expectEqualStrings("d", try read.get("\x01document"));
    try std.testing.expectEqualStrings("a", try read.get("\x03artifact"));
}

test "lsm backend preserves key family partitions in streaming compaction output" {
    const alloc = std.testing.allocator;
    var memory_storage = storage_io.MemoryStorage.init(alloc);
    defer memory_storage.deinit();

    var backend = try Backend.open(alloc, "/memory/key-family-compaction", .{
        .flush_threshold = 3,
        .compact_threshold_runs = 2,
        .foreground_soft_compaction = true,
        .level_target_runs_base = 100,
        .level_target_bytes_base = 0,
        .max_run_file_bytes = 1024 * 1024,
        .run_partition_prefix_bytes = 1,
        .storage = memory_storage.storage(),
    });
    defer backend.close();
    var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime.deinit();

    for (0..3) |batch| {
        var txn = try runtime.beginWrite();
        var keys: [3][32]u8 = undefined;
        const metadata = try std.fmt.bufPrint(&keys[0], "\x00metadata:{d}", .{batch});
        const document = try std.fmt.bufPrint(&keys[1], "\x01document:{d}", .{batch});
        const artifact = try std.fmt.bufPrint(&keys[2], "\x03artifact:{d}", .{batch});
        try txn.put(metadata, "m");
        try txn.put(document, "d");
        try txn.put(artifact, "a");
        try txn.commit();
    }
    try backend.finalizeDeferredStorageWork();
    try std.testing.expect(backend.compaction_stats.compactions > 0);
    for (backend.runs.items) |run| {
        try std.testing.expect(run.smallest_key.len > 0);
        try std.testing.expect(run.largest_key.len > 0);
        try std.testing.expectEqual(run.smallest_key[0], run.largest_key[0]);
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
            .max_run_file_physical_bytes = 512,
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
            try std.testing.expect(run.size_bytes <= 512);
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
            .max_run_file_physical_bytes = 512,
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

    var manager = resource_manager_mod.ResourceManager.init(.{});
    {
        var reopened = try Backend.open(std.testing.allocator, root_dir, .{
            .flush_threshold = 2,
            .recovery_replay_flush_threshold = 2,
            .storage = memory_storage.storage(),
            .resource_manager = &manager,
        });
        defer reopened.close();

        const recovery_stats = manager.sliceStats(.lsm_recovery_working_set);
        try std.testing.expectEqual(@as(u64, 0), recovery_stats.used_bytes);
        try std.testing.expect(recovery_stats.peak_bytes >= wal_mod.default_replay_scratch_retained_cap_bytes);

        const stats = reopened.snapshotMaintenanceStats();
        try std.testing.expectEqual(@as(u64, 0), stats.mutable_entries);
        try std.testing.expectEqual(@as(u64, 0), stats.immutable_memtables);
        try std.testing.expect(reopened.runs.items.len > 0);
        const write_stats = reopened.snapshotWriteStats();
        try std.testing.expect(write_stats.wal_replay_recovery_flushes > 0);
        try std.testing.expect(write_stats.wal_replay_recovery_entry_bytes > 0);
        try std.testing.expect(write_stats.wal_replay_recovery_window_peak_bytes > 0);

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

test "lsm backend recovery replay stores mutable entries in a flush-scoped arena" {
    var memory_storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer memory_storage.deinit();

    const root_dir = "/memory/recovery-replay-arena";
    try memory_storage.storage().createDirPath(root_dir);

    var state: State = .{};
    defer state.deinit(std.testing.allocator);
    try state.upsert(std.testing.allocator, .{ .name = "docs" }, "doc:a", "alpha", false);
    try state.upsert(std.testing.allocator, .{ .name = "docs" }, "doc:b", "bravo", false);
    _ = try wal_mod.appendStateWithOptions(
        memory_storage.storage(),
        std.testing.allocator,
        root_dir,
        &state,
        false,
        .{ .segment_bytes = 512 },
    );

    var reopened = try Backend.open(std.testing.allocator, root_dir, .{
        .flush_threshold = 1024,
        .storage = memory_storage.storage(),
    });
    defer reopened.close();

    try std.testing.expect(reopened.mutable.arena_owner != null);
    try std.testing.expectEqual(@as(usize, 2), reopened.mutable.entries.items.len);
    for (reopened.mutable.entries.items) |entry| {
        try std.testing.expect(entry.namespace_from_arena);
        try std.testing.expect(entry.key_from_arena);
        try std.testing.expect(entry.value_from_arena);
    }
    try std.testing.expectEqualStrings("alpha", try reopened.getMergedWithMutable(&reopened.mutable, .{ .name = "docs" }, "doc:a"));

    try reopened.finalizeDeferredStorageWork();
    try std.testing.expect(reopened.mutable.arena_owner == null);
    try std.testing.expectEqual(@as(usize, 0), reopened.mutable.entries.items.len);
    try std.testing.expectEqual(@as(usize, 0), reopened.activeImmutableMemtableCount());
    try std.testing.expect(reopened.runs.items.len > 0);
}

test "lsm backend recovery replay byte threshold flushes within a large wal record" {
    const alloc = std.testing.allocator;
    var memory_storage = storage_io.MemoryStorage.init(alloc);
    defer memory_storage.deinit();

    const root_dir = "/memory/recovery-replay-byte-flush-inside-record";
    try memory_storage.storage().createDirPath(root_dir);

    const large_value = try alloc.alloc(u8, 128);
    defer alloc.free(large_value);
    @memset(large_value, 'v');

    var state: State = .{};
    defer state.deinit(alloc);
    try state.upsert(alloc, .{ .name = "docs" }, "doc:a", large_value, false);
    try state.upsert(alloc, .{ .name = "docs" }, "doc:b", large_value, false);
    try state.upsert(alloc, .{ .name = "docs" }, "doc:c", large_value, false);
    _ = try wal_mod.appendStateWithOptions(
        memory_storage.storage(),
        alloc,
        root_dir,
        &state,
        false,
        .{ .segment_bytes = 4096 },
    );

    var reopened = try Backend.open(alloc, root_dir, .{
        .flush_threshold = 1024,
        .flush_threshold_bytes = 96,
        .storage = memory_storage.storage(),
    });
    defer reopened.close();

    const maintenance = reopened.snapshotMaintenanceStats();
    try std.testing.expectEqual(@as(u64, 0), maintenance.mutable_entries);
    try std.testing.expectEqual(@as(u64, 0), maintenance.immutable_memtables);
    try std.testing.expect(reopened.runs.items.len >= 2);

    const write_stats = reopened.snapshotWriteStats();
    try std.testing.expect(write_stats.immutable_flushes >= 2);
    try std.testing.expect(write_stats.wal_replay_recovery_flushes >= 2);
    try std.testing.expect(write_stats.wal_replay_recovery_entry_bytes >= large_value.len * 3);
    try std.testing.expect(write_stats.wal_replay_recovery_window_peak_bytes >= large_value.len);

    var runtime = try reopened.runtimeNamespaceStore(alloc);
    defer runtime.deinit();
    var txn = try runtime.beginRead();
    defer txn.abort();
    try std.testing.expectEqualStrings(large_value, try txn.get(.{ .name = "docs" }, "doc:a"));
    try std.testing.expectEqualStrings(large_value, try txn.get(.{ .name = "docs" }, "doc:b"));
    try std.testing.expectEqualStrings(large_value, try txn.get(.{ .name = "docs" }, "doc:c"));
}

test "lsm backend recovery batches large tombstone wal record and checkpoints replay" {
    const alloc = std.testing.allocator;
    var memory_storage = storage_io.MemoryStorage.init(alloc);
    defer memory_storage.deinit();

    const root_dir = "/memory/recovery-replay-large-tombstone-record";
    try memory_storage.storage().createDirPath(root_dir);

    var seed: State = .{};
    defer seed.deinit(alloc);
    var i: u64 = 1;
    while (i <= 4096) : (i += 1) {
        var key_buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &key_buf, i, .big);
        try seed.upsert(alloc, .{}, &key_buf, "present", false);
    }
    _ = try wal_mod.appendStateWithOptions(
        memory_storage.storage(),
        alloc,
        root_dir,
        &seed,
        false,
        .{ .segment_bytes = 256 * 1024 },
    );

    {
        var seeded = try Backend.open(alloc, root_dir, .{
            .flush_threshold = 64,
            .storage = memory_storage.storage(),
        });
        defer seeded.close();
    }

    var deletes: State = .{};
    defer deletes.deinit(alloc);
    i = 1;
    while (i <= 4096) : (i += 1) {
        var key_buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &key_buf, i, .big);
        try deletes.upsert(alloc, .{}, &key_buf, "", true);
    }
    _ = try wal_mod.appendStateWithOptions(
        memory_storage.storage(),
        alloc,
        root_dir,
        &deletes,
        false,
        .{ .segment_bytes = 256 * 1024 },
    );

    {
        var reopened = try Backend.open(alloc, root_dir, .{
            .flush_threshold = 64,
            .recovery_replay_flush_threshold = 1024,
            .storage = memory_storage.storage(),
        });
        defer reopened.close();

        const stats = reopened.snapshotWriteStats();
        try std.testing.expectEqual(@as(u64, 1), stats.wal_replay_records);
        try std.testing.expectEqual(@as(u64, 4096), stats.wal_replay_entries);
        try std.testing.expectEqual(@as(u64, 4096), stats.wal_replay_recovery_entries_applied);
        try std.testing.expect(stats.wal_replay_recovery_flushes <= 5);
        try std.testing.expect(stats.wal_replay_recovery_flushes > 0);

        i = 1;
        while (i <= 4096) : (i += 1) {
            var key_buf: [8]u8 = undefined;
            std.mem.writeInt(u64, &key_buf, i, .big);
            try std.testing.expectError(error.NotFound, reopened.getMergedWithMutable(&reopened.mutable, .{}, &key_buf));
        }
    }

    {
        var reopened = try Backend.open(alloc, root_dir, .{
            .flush_threshold = 64,
            .storage = memory_storage.storage(),
        });
        defer reopened.close();

        const stats = reopened.snapshotWriteStats();
        try std.testing.expectEqual(@as(u64, 0), stats.wal_replay_records);
        try std.testing.expectEqual(@as(u64, 0), stats.wal_replay_bytes);
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

        fn syncFileContentsAbsolute(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().syncFileContentsAbsolute(path);
        }

        fn syncParentAbsolute(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().syncParentAbsolute(path);
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
        .sync_contents_absolute = HostContext.syncFileContentsAbsolute,
        .sync_parent_absolute = HostContext.syncParentAbsolute,
        .rename_is_atomic = true,
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
test "lsm backend reclaims orphaned committed run files after manifest recovery" {
    var path_buf: [256]u8 = undefined;
    const path = repository_mod.tmpPath(&path_buf, "orphan-run-cleanup");
    defer repository_mod.cleanupTmp(path);

    const root_dir = std.mem.span(path);
    const orphan_path = try repository_mod.runPath(std.testing.allocator, root_dir, 9999);
    defer std.testing.allocator.free(orphan_path);

    {
        var backend = try Backend.open(std.testing.allocator, root_dir, .{
            .flush_threshold = 1,
            .compact_threshold_runs = 2,
        });
        defer backend.close();

        var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginWrite();
        try txn.put("doc:a", "A");
        try txn.commit();
    }

    const orphan_entries = [_]lsm_table_file.Entry{
        .{ .namespace_name = "docs", .key = "doc:orphan", .value = "O", .tombstone = false },
    };
    const encoded = try lsm_table_file.encodeAlloc(std.testing.allocator, &orphan_entries);
    defer std.testing.allocator.free(encoded);
    try repository_mod.writeFileAbsolute(orphan_path, encoded);

    {
        var reopened = try Backend.open(std.testing.allocator, root_dir, .{
            .flush_threshold = 1,
            .compact_threshold_runs = 2,
        });
        defer reopened.close();

        try std.testing.expectEqual(@as(usize, 1), reopened.runs.items.len);
        try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, orphan_path, .{}));

        var runtime = try reopened.runtimeStore(std.testing.allocator, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginRead();
        defer txn.abort();
        try std.testing.expectEqualStrings("A", try txn.get("doc:a"));
        try std.testing.expectError(error.NotFound, txn.get("doc:orphan"));
    }
}

test "lsm backend removes expired obsolete run files on reopen" {
    var path_buf: [256]u8 = undefined;
    const path = repository_mod.tmpPath(&path_buf, "obsolete-run-cleanup");
    defer repository_mod.cleanupTmp(path);

    const root_dir = std.mem.span(path);
    const obsolete_path = try repository_mod.runPath(std.testing.allocator, root_dir, 9999);
    defer std.testing.allocator.free(obsolete_path);

    {
        var backend = try Backend.open(std.testing.allocator, root_dir, .{
            .flush_threshold = 1,
            .compact_threshold_runs = 2,
        });
        defer backend.close();

        var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginWrite();
        try txn.put("doc:a", "A");
        try txn.commit();

        const obsolete_entries = [_]lsm_table_file.Entry{
            .{ .namespace_name = "docs", .key = "doc:obsolete", .value = "O", .tombstone = false },
        };
        const encoded = try lsm_table_file.encodeAlloc(std.testing.allocator, &obsolete_entries);
        defer std.testing.allocator.free(encoded);
        try repository_mod.writeFileAbsolute(obsolete_path, encoded);

        const obsolete = [_]ObsoletePath{.{ .path = obsolete_path, .delete_after_ns = 0 }};
        _ = try repository_mod.persistManifestWithStorageCount(
            backend.storage.?,
            std.testing.allocator,
            root_dir,
            backend.next_run_id,
            backend.runs.items,
            &obsolete,
        );
    }

    {
        var reopened = try Backend.open(std.testing.allocator, root_dir, .{
            .flush_threshold = 1,
            .compact_threshold_runs = 2,
        });
        defer reopened.close();

        try std.testing.expectEqual(@as(usize, 1), reopened.runs.items.len);
        try std.testing.expectEqual(@as(usize, 1), reopened.obsolete_paths.items.len);
        try std.Io.Dir.cwd().access(std.testing.io, obsolete_path, .{});
        try std.testing.expect(try reopened.runMaintenanceStep());
        try std.testing.expectEqual(@as(usize, 0), reopened.obsolete_paths.items.len);
        try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, obsolete_path, .{}));

        var runtime = try reopened.runtimeStore(std.testing.allocator, .{ .name = "docs" });
        defer runtime.deinit();

        var txn = try runtime.beginRead();
        defer txn.abort();
        try std.testing.expectEqualStrings("A", try txn.get("doc:a"));
        try std.testing.expectError(error.NotFound, txn.get("doc:obsolete"));
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
