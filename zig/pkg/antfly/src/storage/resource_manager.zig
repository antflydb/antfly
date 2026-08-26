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
const platform_time = @import("antfly_platform").time;
const shared_platform_time = @import("antfly_platform").time;
const cache_budget = @import("../common/cache_budget.zig");

const MiB: u64 = 1024 * 1024;
const dense_replay_window_min_bytes: u64 = 16 * MiB;
const dense_replay_window_growth_numerator: u64 = 5;
const dense_replay_window_growth_denominator: u64 = 4;
const dense_replay_window_shrink_numerator: u64 = 3;
const dense_replay_window_shrink_denominator: u64 = 4;
const dense_replay_finish_target_ns: u64 = 3 * std.time.ns_per_s;
const dense_replay_finish_hard_ns: u64 = 8 * std.time.ns_per_s;
const dense_replay_write_pressure_hard_ns: u64 = std.time.ns_per_s;
const dense_replay_soft_compaction_quiet_ns: u64 = 500 * std.time.ns_per_ms;
const soft_throttle_delay_ns: u64 = 10 * std.time.ns_per_ms;
const supports_pressure_wait = builtin.os.tag != .freestanding and
    builtin.link_libc and
    @hasDecl(std.c, "pthread_cond_wait");

const PressureChange = if (supports_pressure_wait)
    struct {
        mutex: std.c.pthread_mutex_t = std.c.PTHREAD_MUTEX_INITIALIZER,
        cond: std.c.pthread_cond_t = std.c.PTHREAD_COND_INITIALIZER,
        epoch: std.atomic.Value(u64) = .init(0),

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
        epoch: std.atomic.Value(u64) = .init(0),

        fn snapshot(self: *@This()) u64 {
            return self.epoch.load(.acquire);
        }

        fn waitForChange(_: *@This(), _: u64) void {}

        fn advance(self: *@This()) void {
            _ = self.epoch.fetchAdd(1, .release);
        }
    };
const default_disk_safety_floor_bytes: u64 = 1024 * MiB;
const default_disk_safety_floor_divisor: u64 = 20;

pub const Slice = enum(u8) {
    lsm_block_table_cache,
    lsm_compaction_work,
    lsm_table_builder_working_set,
    lsm_in_memory_state,
    lsm_wal_write_working_set,
    lsm_wal_retention,
    lsm_recovery_working_set,
    hbc_node_metadata_cache,
    dense_search_working_set,
    dense_apply_working_set,
    dense_routing_working_set,
    derived_replay_window,
    full_text_pending_segments,
    full_text_build_working_set,
    full_text_segment_residency,
    document_extraction_working_set,
    derived_backlog,
    text_merge_buffers,
    algebraic_tensor_accumulators,
    sparse_apply_working_set,
    lite_native_page_cache,
    lite_native_link_cache,
    lite_docstore_snapshot_cache,
    inference_prompt_cache,
    inference_tokenizer_cache,
    inference_model_residency,
    inference_kv_working_set,
    inference_scratch_working_set,
    dense_repair_working_set,
    shard_transition_working_set,

    pub fn name(self: Slice) []const u8 {
        return switch (self) {
            .lsm_block_table_cache => "lsm.block_table_cache",
            .lsm_compaction_work => "lsm.compaction_work",
            .lsm_table_builder_working_set => "lsm.table_builder_working_set",
            .lsm_in_memory_state => "lsm.in_memory_state",
            .lsm_wal_write_working_set => "lsm.wal_write_working_set",
            .lsm_wal_retention => "lsm.wal_retention",
            .lsm_recovery_working_set => "lsm.recovery_working_set",
            .hbc_node_metadata_cache => "hbc.node_metadata_cache",
            .dense_search_working_set => "dense.search_working_set",
            .dense_apply_working_set => "dense.apply_working_set",
            .dense_routing_working_set => "dense.routing_working_set",
            .derived_replay_window => "derived.replay_window",
            .full_text_pending_segments => "full_text.pending_segments",
            .full_text_build_working_set => "full_text.build_working_set",
            .full_text_segment_residency => "full_text.segment_residency",
            .document_extraction_working_set => "document_extraction.working_set",
            .derived_backlog => "derived.backlog",
            .text_merge_buffers => "text_merge.buffers",
            .algebraic_tensor_accumulators => "algebraic.tensor_accumulators",
            .sparse_apply_working_set => "sparse.apply_working_set",
            .lite_native_page_cache => "lite.native_page_cache",
            .lite_native_link_cache => "lite.native_link_cache",
            .lite_docstore_snapshot_cache => "lite.docstore_snapshot_cache",
            .inference_prompt_cache => "inference.prompt_cache",
            .inference_tokenizer_cache => "inference.tokenizer_cache",
            .inference_model_residency => "inference.model_residency",
            .inference_kv_working_set => "inference.kv_working_set",
            .inference_scratch_working_set => "inference.scratch_working_set",
            .dense_repair_working_set => "dense_repair.working_set",
            .shard_transition_working_set => "shard_transition.working_set",
        };
    }
};

pub const slice_count: usize = @typeInfo(Slice).@"enum".fields.len;

pub const Budget = struct {
    soft_limit_bytes: u64 = 0,
    hard_limit_bytes: u64 = 0,
};

pub const SliceAmount = struct {
    slice: Slice,
    bytes: u64,
};

/// An exactly-once batch ownership token. Callers retain this value for the
/// lifetime of the admitted memory instead of reconstructing release amounts.
/// This prevents a duplicate teardown from consuming an equivalent reservation
/// owned by unrelated work in the same logical slices.
pub const BatchReservation = struct {
    manager: *ResourceManager,
    identity: u64,
    amounts: [slice_count]u64,
    host_charge_bytes: u64,
    released: bool = false,

    pub fn retain(
        self: *BatchReservation,
        retained: []const SliceAmount,
        retained_host_charge_bytes: u64,
    ) !void {
        if (self.released) return error.ReservationReleased;
        const normalized = ResourceManager.normalizeSliceAmounts(retained) catch |err| {
            self.manager.recordAccountingError();
            return err;
        };
        try self.manager.retainBatchReservation(
            self,
            normalized,
            retained_host_charge_bytes,
        );
    }

    pub fn release(self: *BatchReservation) void {
        if (self.released) return;
        _ = self.manager.releaseBatchReservation(self);
        // A failed accounting validation intentionally retains capacity, but
        // the ownership token is still consumed so retries cannot repeatedly
        // mutate or inflate the error counter.
        self.released = true;
    }
};

/// Internal HBC cache safety ceilings. These are not index configuration:
/// byte admission remains authoritative and shared across every index using
/// this manager. The ceilings only bound the local CLOCK bookkeeping arrays.
pub const HbcCacheLimits = struct {
    max_cached_nodes: usize,
    max_cached_vectors: usize,
    max_cached_metadata: usize,
};

/// ResourceManager-owned policy for the reclaimable cache behind all HBC
/// indexes in one process. These are value classes, not separate byte
/// ledgers: the aggregate `hbc_node_metadata_cache` slice remains the single
/// physical charge, while protected targets determine which class gives bytes
/// back first. Unused protected bytes are therefore borrowable by any class.
pub const HbcCacheClass = enum(u8) {
    node,
    quantized,
    vector,
    metadata,
};

pub const HbcCachePolicy = struct {
    /// The normal-pressure target. The cache may briefly grow above this value
    /// up to the hard slice limit, then converges through its shrink action.
    target_bytes: u64 = 0,
    /// Minimum working-set targets used only for victim ordering. They are not
    /// reservations and do not prevent aggregate reclamation.
    node_protected_bytes: u64 = 0,
    quantized_protected_bytes: u64 = 0,
    vector_protected_bytes: u64 = 0,
    metadata_protected_bytes: u64 = 0,
    adaptive: bool = false,
    /// Concurrent exact-vector misses may otherwise serialize every search on
    /// cache admission while the cache is already under pressure. One admits
    /// every miss; zero disables optional concurrent admission.
    concurrent_vector_admission_stride: u32 = 1,

    pub fn protectedBytes(self: HbcCachePolicy, class: HbcCacheClass) u64 {
        return switch (class) {
            .node => self.node_protected_bytes,
            .quantized => self.quantized_protected_bytes,
            .vector => self.vector_protected_bytes,
            .metadata => self.metadata_protected_bytes,
        };
    }
};

pub const HbcCacheBenefitSample = struct {
    hits: u64 = 0,
    misses: u64 = 0,
    miss_service_ns: u64 = 0,
    resident_bytes: u64 = 0,
};

const HbcCacheBenefitState = struct {
    score: u64 = 0,
    miss_service_ns_per_miss: u64 = 0,
    observations: u64 = 0,
};

const hbc_max_clock_entries: u64 = 100_000;
const hbc_estimated_node_entry_bytes: u64 = 1024;
const hbc_estimated_metadata_entry_bytes: u64 = 256;
const hbc_estimated_vector_overhead_bytes: u64 = 64;

pub const Pressure = enum(u8) {
    normal,
    soft,
    hard,
};

pub const PressureAction = enum(u8) {
    report,
    shrink_cache,
    defer_background_work,
    throttle_writes,
    reject_work,

    pub fn name(self: PressureAction) []const u8 {
        return switch (self) {
            .report => "report",
            .shrink_cache => "shrink_cache",
            .defer_background_work => "defer_background_work",
            .throttle_writes => "throttle_writes",
            .reject_work => "reject_work",
        };
    }
};

pub const PressureDecision = struct {
    pressure: Pressure = .normal,
    action: PressureAction = .report,
    used_bytes: u64 = 0,
    soft_limit_bytes: u64 = 0,
    hard_limit_bytes: u64 = 0,
    change_epoch: u64 = 0,
};

pub const Policy = struct {
    soft_action: PressureAction = .report,
    hard_action: PressureAction = .report,
};

pub const Options = struct {
    /// Aggregate host-memory budget for every memory slice owned by this
    /// manager. A zero limit preserves the legacy unlimited behavior for
    /// tests and callers that do not own a process envelope.
    memory_budget: Budget = .{},
    budgets: [slice_count]Budget = defaultBudgets(),
    policies: [slice_count]Policy = defaultPolicies(),
    /// Bound durable replay debt by record count as well as encoded bytes.
    /// These are runtime policy values, not index or API configuration.
    derived_backlog_high_sequences: usize = 200,
    derived_backlog_resume_sequences: usize = 100,
    /// Bound how much sequence-only replay debt one foreground admission must
    /// inherit. Byte and aggregate LSM pressure can still request a larger
    /// drain; this window only prevents a healthy async index backlog from
    /// concentrating minutes of work into one public write acknowledgement.
    derived_backlog_throttle_window_sequences: usize = 16,
    /// Node-owned filesystem growth policy. This is deliberately not table or
    /// index configuration. The larger of the fixed floor and this fraction
    /// of observed capacity is kept available for WAL, checkpoints, and
    /// foreground durability.
    disk_safety_floor_bytes: u64 = default_disk_safety_floor_bytes,
    disk_safety_floor_divisor: u64 = default_disk_safety_floor_divisor,
    /// Internal query-embedding cache policy. Serving layers consume this
    /// policy but cannot override it independently of the node manager.
    query_embedding_cache_bytes: usize = 64 * 1024 * 1024,
    query_embedding_cache_ttl_ns: u64 = 5 * std.time.ns_per_min,
    query_embedding_max_inflight: usize = 16,
    /// Allocates identity tables only as concurrent reservations/observers
    /// exceed their previous high-water mark. Production owners should pass
    /// their lifetime allocator; the page allocator keeps lightweight tests
    /// source-compatible.
    identity_allocator: std.mem.Allocator = std.heap.page_allocator,

    pub fn defaultBudgets() [slice_count]Budget {
        return .{
            .{ .soft_limit_bytes = 192 * 1024 * 1024, .hard_limit_bytes = 256 * 1024 * 1024 },
            .{ .soft_limit_bytes = 512 * 1024 * 1024, .hard_limit_bytes = 768 * 1024 * 1024 },
            .{ .soft_limit_bytes = 128 * 1024 * 1024, .hard_limit_bytes = 256 * 1024 * 1024 },
            .{ .soft_limit_bytes = 512 * 1024 * 1024, .hard_limit_bytes = 768 * 1024 * 1024 },
            .{ .soft_limit_bytes = 256 * 1024 * 1024, .hard_limit_bytes = 512 * 1024 * 1024 },
            .{ .soft_limit_bytes = 512 * 1024 * 1024, .hard_limit_bytes = 1024 * 1024 * 1024 },
            .{ .soft_limit_bytes = 128 * 1024 * 1024, .hard_limit_bytes = 256 * 1024 * 1024 },
            .{ .soft_limit_bytes = 384 * 1024 * 1024, .hard_limit_bytes = 512 * 1024 * 1024 },
            .{ .soft_limit_bytes = 96 * 1024 * 1024, .hard_limit_bytes = 160 * 1024 * 1024 },
            .{ .soft_limit_bytes = 128 * 1024 * 1024, .hard_limit_bytes = 256 * 1024 * 1024 },
            .{ .soft_limit_bytes = 128 * 1024 * 1024, .hard_limit_bytes = 256 * 1024 * 1024 },
            .{ .soft_limit_bytes = 96 * 1024 * 1024, .hard_limit_bytes = 160 * 1024 * 1024 },
            .{ .soft_limit_bytes = 192 * 1024 * 1024, .hard_limit_bytes = 256 * 1024 * 1024 },
            .{ .soft_limit_bytes = 256 * 1024 * 1024, .hard_limit_bytes = 512 * 1024 * 1024 },
            .{ .soft_limit_bytes = 512 * 1024 * 1024, .hard_limit_bytes = 768 * 1024 * 1024 },
            .{ .soft_limit_bytes = 192 * 1024 * 1024, .hard_limit_bytes = 256 * 1024 * 1024 },
            .{ .soft_limit_bytes = 128 * 1024 * 1024, .hard_limit_bytes = 192 * 1024 * 1024 },
            .{ .soft_limit_bytes = 128 * 1024 * 1024, .hard_limit_bytes = 192 * 1024 * 1024 },
            .{ .soft_limit_bytes = 96 * 1024 * 1024, .hard_limit_bytes = 160 * 1024 * 1024 },
            .{ .soft_limit_bytes = 128 * 1024 * 1024, .hard_limit_bytes = 256 * 1024 * 1024 },
            .{ .soft_limit_bytes = 48 * 1024 * 1024, .hard_limit_bytes = 64 * 1024 * 1024 },
            .{ .soft_limit_bytes = 12 * 1024 * 1024, .hard_limit_bytes = 16 * 1024 * 1024 },
            .{ .soft_limit_bytes = 192 * 1024 * 1024, .hard_limit_bytes = 256 * 1024 * 1024 },
            .{ .soft_limit_bytes = 512 * 1024 * 1024, .hard_limit_bytes = 768 * 1024 * 1024 },
            .{ .soft_limit_bytes = 64 * 1024 * 1024, .hard_limit_bytes = 128 * 1024 * 1024 },
            // ModelManager owns hardware-aware host/backend limits. These
            // owner-bridge slices are unlimited by default, while deployments
            // may set coordinated node budgets through ResourceManager options.
            .{},
            .{},
            .{},
            .{ .soft_limit_bytes = 128 * 1024 * 1024, .hard_limit_bytes = 256 * 1024 * 1024 },
            .{ .soft_limit_bytes = 128 * 1024 * 1024, .hard_limit_bytes = 256 * 1024 * 1024 },
        };
    }

    pub fn defaultPolicies() [slice_count]Policy {
        return .{
            .{ .soft_action = .shrink_cache, .hard_action = .shrink_cache },
            .{ .soft_action = .defer_background_work, .hard_action = .reject_work },
            .{ .soft_action = .defer_background_work, .hard_action = .reject_work },
            .{ .soft_action = .throttle_writes, .hard_action = .throttle_writes },
            .{ .soft_action = .report, .hard_action = .throttle_writes },
            .{ .soft_action = .report, .hard_action = .throttle_writes },
            .{ .soft_action = .report, .hard_action = .report },
            .{ .soft_action = .shrink_cache, .hard_action = .shrink_cache },
            .{ .soft_action = .report, .hard_action = .throttle_writes },
            .{ .soft_action = .report, .hard_action = .throttle_writes },
            .{ .soft_action = .report, .hard_action = .throttle_writes },
            .{ .soft_action = .report, .hard_action = .reject_work },
            .{ .soft_action = .defer_background_work, .hard_action = .defer_background_work },
            .{ .soft_action = .throttle_writes, .hard_action = .reject_work },
            .{ .soft_action = .shrink_cache, .hard_action = .shrink_cache },
            .{ .soft_action = .defer_background_work, .hard_action = .reject_work },
            .{ .soft_action = .throttle_writes, .hard_action = .throttle_writes },
            .{ .soft_action = .defer_background_work, .hard_action = .reject_work },
            .{ .soft_action = .throttle_writes, .hard_action = .reject_work },
            .{ .soft_action = .report, .hard_action = .throttle_writes },
            .{ .soft_action = .shrink_cache, .hard_action = .shrink_cache },
            .{ .soft_action = .shrink_cache, .hard_action = .shrink_cache },
            .{ .soft_action = .shrink_cache, .hard_action = .shrink_cache },
            .{ .soft_action = .shrink_cache, .hard_action = .shrink_cache },
            .{ .soft_action = .shrink_cache, .hard_action = .shrink_cache },
            .{ .soft_action = .report, .hard_action = .reject_work },
            .{ .soft_action = .report, .hard_action = .reject_work },
            .{ .soft_action = .report, .hard_action = .reject_work },
            .{ .soft_action = .defer_background_work, .hard_action = .reject_work },
            .{ .soft_action = .defer_background_work, .hard_action = .reject_work },
        };
    }
};

pub const SliceStats = struct {
    name: []const u8,
    used_bytes: u64 = 0,
    peak_bytes: u64 = 0,
    soft_limit_bytes: u64 = 0,
    hard_limit_bytes: u64 = 0,
    soft_limit_events: u64 = 0,
    hard_limit_rejections: u64 = 0,
    oversized_single_grants: u64 = 0,
    pressure: Pressure = .normal,
    soft_action: PressureAction = .report,
    hard_action: PressureAction = .report,
};

pub const Stats = struct {
    memory: MemoryStats,
    slices: [slice_count]SliceStats,
    reclaim_requests: u64 = 0,
    reclaimed_bytes: u64 = 0,
};

pub const MemoryStats = struct {
    used_bytes: u64 = 0,
    peak_bytes: u64 = 0,
    soft_limit_bytes: u64 = 0,
    hard_limit_bytes: u64 = 0,
    soft_limit_events: u64 = 0,
    hard_limit_rejections: u64 = 0,
    accounting_errors: u64 = 0,
    pressure: Pressure = .normal,
};

/// Stable identity for one independently exhausted storage capacity domain.
/// The storage layer should derive this from the volume/quota namespace, not
/// from a table, shard, or repair identifier.
pub const CapacityDomainId = u128;

pub const CapacityObservation = struct {
    /// Null means the backend cannot observe capacity. Reservations are still
    /// coordinated and measured, but cannot be rejected on free-space grounds.
    available_bytes: ?u64 = null,
    capacity_bytes: ?u64 = null,
    /// Monotonic timestamp in the caller's process clock domain. Capacity
    /// freshness is process-local; durable wall-clock deadlines are translated
    /// before reaching ResourceManager.
    observed_at_ns: u64 = 0,
    /// Zero disables age checking (used by deterministic tests and backends
    /// whose observation is synchronously obtained during admission).
    valid_for_ns: u64 = 0,

    fn stale(self: @This(), now_ns: u64) bool {
        if (self.available_bytes == null or self.valid_for_ns == 0) return false;
        if (now_ns < self.observed_at_ns) return true;
        return now_ns - self.observed_at_ns > self.valid_for_ns;
    }
};

pub const CapacitySource = struct {
    ptr: *anyopaque,
    domain_id: CapacityDomainId,
    observe: *const fn (ptr: *anyopaque) anyerror!CapacityObservation,

    pub fn current(self: @This()) !CapacityObservation {
        return try self.observe(self.ptr);
    }
};

pub const CapacityDomainStats = struct {
    domain_id: CapacityDomainId,
    reserved_bytes: u64 = 0,
    peak_reserved_bytes: u64 = 0,
    reservations: u64 = 0,
    denials: u64 = 0,
    growth_denials: u64 = 0,
    stale_observations: u64 = 0,
    last_available_bytes: ?u64 = null,
    last_capacity_bytes: ?u64 = null,
    last_safety_floor_bytes: u64 = 0,
    last_observed_at_ns: u64 = 0,
};

pub const CapacityStats = struct {
    reserved_bytes: u64 = 0,
    peak_reserved_bytes: u64 = 0,
    reservations: u64 = 0,
    denials: u64 = 0,
    growth_denials: u64 = 0,
    stale_observations: u64 = 0,
    domain_count: usize = 0,
};

const MutableCapacityDomain = struct {
    reserved_bytes: u64 = 0,
    peak_reserved_bytes: u64 = 0,
    reservations: u64 = 0,
    denials: u64 = 0,
    growth_denials: u64 = 0,
    stale_observations: u64 = 0,
    last_available_bytes: ?u64 = null,
    last_capacity_bytes: ?u64 = null,
    last_safety_floor_bytes: u64 = 0,
    last_observed_at_ns: u64 = 0,
};

pub const DenseReplayWindowBudgetOptions = struct {
    default_bytes: u64,
    max_bytes: u64,
    min_bytes: u64 = dense_replay_window_min_bytes,
};

pub const DenseReplayWindowResult = struct {
    finish_ns: u64 = 0,
    write_pressure_ns: u64 = 0,
    write_pressure_compactions: u64 = 0,
};

pub const DerivedBacklogLimits = struct {
    high_sequences: usize,
    resume_sequences: usize,
    throttle_window_sequences: usize,
};

pub const QueryEmbeddingPolicy = struct {
    enabled: bool,
    ttl_ns: u64,
    max_inflight: usize,
};

pub const IndexRepairActivationStats = struct {
    attempts: u64 = 0,
    overruns: u64 = 0,
    last_pause_ns: u64 = 0,
    max_pause_ns: u64 = 0,
    last_budget_ns: u64 = 0,
};

pub const DerivedRecoverableRetryStats = struct {
    total: u64 = 0,
    writer_locked: u64 = 0,
    resource_budget: u64 = 0,
    replay_document_not_visible: u64 = 0,
    artifact_repair_required: u64 = 0,
    not_found: u64 = 0,
};

const DerivedRecoverableRetryCounters = struct {
    total: std.atomic.Value(u64) = .init(0),
    writer_locked: std.atomic.Value(u64) = .init(0),
    resource_budget: std.atomic.Value(u64) = .init(0),
    replay_document_not_visible: std.atomic.Value(u64) = .init(0),
    artifact_repair_required: std.atomic.Value(u64) = .init(0),
    not_found: std.atomic.Value(u64) = .init(0),

    fn record(self: *@This(), err: anyerror) void {
        _ = self.total.fetchAdd(1, .monotonic);
        switch (err) {
            error.WriterLocked => _ = self.writer_locked.fetchAdd(1, .monotonic),
            error.ResourceBudgetExceeded,
            error.PersistentDescriptorAdmissionExhausted,
            error.TextMergeBackpressureTimeout,
            error.TextMergeBackpressureUnavailable,
            error.TextMergeRuntimeShutdown,
            => _ = self.resource_budget.fetchAdd(1, .monotonic),
            error.ReplayDocumentNotVisible => _ = self.replay_document_not_visible.fetchAdd(1, .monotonic),
            error.ArtifactRepairRequired => _ = self.artifact_repair_required.fetchAdd(1, .monotonic),
            error.NotFound => _ = self.not_found.fetchAdd(1, .monotonic),
            else => {},
        }
    }

    fn snapshot(self: *const @This()) DerivedRecoverableRetryStats {
        return .{
            .total = self.total.load(.monotonic),
            .writer_locked = self.writer_locked.load(.monotonic),
            .resource_budget = self.resource_budget.load(.monotonic),
            .replay_document_not_visible = self.replay_document_not_visible.load(.monotonic),
            .artifact_repair_required = self.artifact_repair_required.load(.monotonic),
            .not_found = self.not_found.load(.monotonic),
        };
    }
};

const MutableSlice = struct {
    budget: Budget = .{},
    policy: Policy = .{},
    used_bytes: u64 = 0,
    peak_bytes: u64 = 0,
    soft_limit_events: u64 = 0,
    hard_limit_rejections: u64 = 0,
    oversized_single_grants: u64 = 0,
};

const MutableMemory = struct {
    budget: Budget = .{},
    used_bytes: u64 = 0,
    peak_bytes: u64 = 0,
    soft_limit_events: u64 = 0,
    hard_limit_rejections: u64 = 0,
    accounting_errors: u64 = 0,
};

const ReservationIdentity = struct {
    slice: Slice,
    bytes: u64,
};

const BatchReservationIdentity = struct {
    amounts: [slice_count]u64,
    host_charge_bytes: u64,
};

const ObserverIdentity = struct {
    current: u64,
};

const ObserverKey = struct {
    slice: Slice,
    identity: usize,
};

/// A cache-owned, synchronous shrink callback. ResourceManager never invokes
/// it while holding either the accounting or reclaimer mutex. Unregistration
/// retires the fixed slot and waits for its callback leases to drain, so an
/// owner can destroy its callback context immediately after unregister returns.
pub const ReclaimerFn = *const fn (context: *anyopaque, target_bytes: u64) u64;

const ReclaimerSlot = struct {
    identity: u64 = 0,
    slice: Slice = .hbc_node_metadata_cache,
    context: ?*anyopaque = null,
    reclaim: ?ReclaimerFn = null,
    weight: u32 = 1,
    in_flight: u32 = 0,
    retiring: bool = false,
};

const ReclaimerInvocation = struct {
    slot_index: usize,
    identity: u64,
    context: *anyopaque,
    reclaim: ReclaimerFn,
    weight: u32,
};

pub const ReclaimerOptions = struct {
    weight: u32 = 1,
};

const max_reclaimers: usize = 128;

pub const ResourceManager = struct {
    mutex: std.atomic.Mutex = .unlocked,
    reclaimer_mutex: std.atomic.Mutex = .unlocked,
    reclaimers: [max_reclaimers]ReclaimerSlot = .{ReclaimerSlot{}} ** max_reclaimers,
    next_reclaimer_identity: u64 = 1,
    reclaimer_cursor: usize = 0,
    reclaim_requests: std.atomic.Value(u64) = .init(0),
    reclaimed_bytes: std.atomic.Value(u64) = .init(0),
    hbc_benefit_sample_counter: std.atomic.Value(u64) = .init(0),
    hbc_cache_benefit: [@typeInfo(HbcCacheClass).@"enum".fields.len]HbcCacheBenefitState = .{HbcCacheBenefitState{}} ** @typeInfo(HbcCacheClass).@"enum".fields.len,
    pressure_change: PressureChange = .{},
    memory: MutableMemory,
    latency_sensitive_derived_replay_sessions: std.atomic.Value(u64) = .init(0),
    latency_sensitive_derived_replay_quiet_until_ns: std.atomic.Value(u64) = .init(0),
    slices: [slice_count]MutableSlice,
    dense_replay_window_budget_bytes: u64 = 0,
    dense_replay_last_finish_ns: u64 = 0,
    dense_replay_last_write_pressure_ns: u64 = 0,
    dense_replay_last_write_pressure_compactions: u64 = 0,
    derived_backlog_high_sequences: usize,
    derived_backlog_resume_sequences: usize,
    derived_backlog_throttle_window_sequences: usize,
    disk_safety_floor_bytes: u64,
    disk_safety_floor_divisor: u64,
    capacity_domains: std.AutoHashMapUnmanaged(CapacityDomainId, MutableCapacityDomain) = .empty,
    query_embedding_cache_budget: cache_budget.CacheBudget,
    query_embedding_cache_ttl_ns: u64,
    query_embedding_max_inflight: usize,
    index_repair_activation: IndexRepairActivationStats = .{},
    derived_recoverable_retry_counters: DerivedRecoverableRetryCounters = .{},
    capacity_source: ?CapacitySource = null,
    identity_allocator: std.mem.Allocator,
    next_identity: u64 = 1,
    reservation_identities: std.AutoHashMapUnmanaged(u64, ReservationIdentity) = .empty,
    batch_reservation_identities: std.AutoHashMapUnmanaged(u64, BatchReservationIdentity) = .empty,
    observer_identities: std.AutoHashMapUnmanaged(ObserverKey, ObserverIdentity) = .empty,

    pub fn init(options: Options) ResourceManager {
        var slices: [slice_count]MutableSlice = undefined;
        for (&slices, 0..) |*slice, i| {
            slice.* = .{
                .budget = options.budgets[i],
                .policy = options.policies[i],
            };
        }
        const high_sequences = options.derived_backlog_high_sequences;
        return .{
            .memory = .{ .budget = options.memory_budget },
            .slices = slices,
            .derived_backlog_high_sequences = high_sequences,
            .derived_backlog_resume_sequences = if (high_sequences == 0)
                0
            else
                @min(options.derived_backlog_resume_sequences, high_sequences - 1),
            .derived_backlog_throttle_window_sequences = options.derived_backlog_throttle_window_sequences,
            .disk_safety_floor_bytes = options.disk_safety_floor_bytes,
            .disk_safety_floor_divisor = options.disk_safety_floor_divisor,
            .query_embedding_cache_budget = cache_budget.CacheBudget.init(options.query_embedding_cache_bytes),
            .query_embedding_cache_ttl_ns = options.query_embedding_cache_ttl_ns,
            .query_embedding_max_inflight = @max(@as(usize, 1), options.query_embedding_max_inflight),
            .identity_allocator = options.identity_allocator,
        };
    }

    pub fn queryEmbeddingCacheBudget(self: *ResourceManager) *cache_budget.CacheBudget {
        return &self.query_embedding_cache_budget;
    }

    pub fn registerReclaimer(
        self: *ResourceManager,
        slice: Slice,
        context: *anyopaque,
        reclaim: ReclaimerFn,
    ) !u64 {
        return try self.registerReclaimerWithOptions(slice, context, reclaim, .{});
    }

    pub fn registerReclaimerWithOptions(
        self: *ResourceManager,
        slice: Slice,
        context: *anyopaque,
        reclaim: ReclaimerFn,
        options: ReclaimerOptions,
    ) !u64 {
        lockAtomic(&self.reclaimer_mutex);
        defer self.reclaimer_mutex.unlock();
        for (&self.reclaimers) |*slot| {
            if (slot.identity != 0) continue;
            const identity = self.next_reclaimer_identity;
            self.next_reclaimer_identity +%= 1;
            if (self.next_reclaimer_identity == 0) self.next_reclaimer_identity = 1;
            slot.* = .{
                .identity = identity,
                .slice = slice,
                .context = context,
                .reclaim = reclaim,
                .weight = @max(@as(u32, 1), options.weight),
            };
            return identity;
        }
        return error.TooManyResourceReclaimers;
    }

    pub fn unregisterReclaimer(self: *ResourceManager, identity: u64) void {
        if (identity == 0) return;
        while (true) {
            lockAtomic(&self.reclaimer_mutex);
            var found = false;
            for (&self.reclaimers) |*slot| {
                if (slot.identity != identity) continue;
                found = true;
                slot.retiring = true;
                if (slot.in_flight == 0) slot.* = .{};
                break;
            }
            self.reclaimer_mutex.unlock();
            if (!found) return;
            if (comptime builtin.os.tag == .freestanding) {
                std.atomic.spinLoopHint();
            } else {
                std.Thread.yield() catch {};
            }
        }
    }

    fn releaseReclaimerInvocation(self: *ResourceManager, invocation: ReclaimerInvocation) void {
        lockAtomic(&self.reclaimer_mutex);
        defer self.reclaimer_mutex.unlock();
        const slot = &self.reclaimers[invocation.slot_index];
        if (slot.identity != invocation.identity) return;
        std.debug.assert(slot.in_flight > 0);
        slot.in_flight -= 1;
        if (slot.retiring and slot.in_flight == 0) slot.* = .{};
    }

    /// Reclaim enough aggregate cache memory to make one allocation retry
    /// meaningful. Cache callbacks run outside the accounting mutex and may
    /// therefore publish their byte decreases through normal accounting APIs.
    pub fn reclaimForAllocation(self: *ResourceManager, requester: Slice, additional_bytes: u64) u64 {
        if (additional_bytes == 0) return 0;
        const target_bytes = blk: {
            lockAtomic(&self.mutex);
            defer self.mutex.unlock();
            const hard = self.memory.budget.hard_limit_bytes;
            if (hard == 0) return 0;
            const projected = self.memory.used_bytes +| additional_bytes;
            if (projected <= hard) return 0;
            break :blk projected - hard;
        };

        _ = self.reclaim_requests.fetchAdd(1, .monotonic);
        var reclaimed: u64 = 0;
        var visited_identities: [max_reclaimers]u64 = undefined;
        var visited_count: usize = 0;
        lockAtomic(&self.reclaimer_mutex);
        const start_cursor = self.reclaimer_cursor;
        self.reclaimer_cursor = (self.reclaimer_cursor + 1) % self.reclaimers.len;
        self.reclaimer_mutex.unlock();
        // HBC exact vectors are the cheapest derivative, followed by LSM
        // blocks. Two passes make this priority independent of registration
        // order while still permitting either cache to borrow unused bytes.
        for ([_]Slice{ .hbc_node_metadata_cache, .lsm_block_table_cache }) |candidate_slice| {
            if (candidate_slice == requester) continue;
            while (visited_count < max_reclaimers) {
                lockAtomic(&self.reclaimer_mutex);
                var remaining_weight: u64 = 0;
                var selected_index: ?usize = null;
                for (0..self.reclaimers.len) |offset| {
                    const index = (start_cursor + offset) % self.reclaimers.len;
                    const slot = &self.reclaimers[index];
                    if (slot.identity == 0 or slot.retiring or slot.slice != candidate_slice) continue;
                    if (std.mem.indexOfScalar(u64, visited_identities[0..visited_count], slot.identity) != null) continue;
                    remaining_weight +|= slot.weight;
                    if (selected_index == null) selected_index = index;
                }
                const index = selected_index orelse {
                    self.reclaimer_mutex.unlock();
                    break;
                };
                const slot = &self.reclaimers[index];
                const invocation = ReclaimerInvocation{
                    .slot_index = index,
                    .identity = slot.identity,
                    .context = slot.context.?,
                    .reclaim = slot.reclaim.?,
                    .weight = slot.weight,
                };
                slot.in_flight += 1;
                visited_identities[visited_count] = slot.identity;
                visited_count += 1;
                const remaining_target = target_bytes -| reclaimed;
                const fair_target = if (remaining_weight <= invocation.weight)
                    remaining_target
                else
                    @max(@as(u64, 1), mulDivSaturating(remaining_target, invocation.weight, remaining_weight));
                self.reclaimer_mutex.unlock();

                reclaimed +|= invocation.reclaim(invocation.context, fair_target);
                self.releaseReclaimerInvocation(invocation);
                if (reclaimed >= target_bytes) break;
            }
            if (reclaimed >= target_bytes) break;
        }
        _ = self.reclaimed_bytes.fetchAdd(reclaimed, .monotonic);
        return reclaimed;
    }

    /// Marks a replay session whose query-visible projection work should take
    /// precedence over optional background compaction. This does not relax LSM
    /// durability or hard write-pressure limits; it only lets backends defer
    /// their soft compaction lane while replay is actively consuming the same
    /// CPU and storage bandwidth.
    pub fn beginLatencySensitiveDerivedReplay(self: *ResourceManager) void {
        _ = self.latency_sensitive_derived_replay_sessions.fetchAdd(1, .release);
    }

    pub fn finishLatencySensitiveDerivedReplay(self: *ResourceManager) void {
        const previous = self.latency_sensitive_derived_replay_sessions.fetchSub(1, .acq_rel);
        std.debug.assert(previous > 0);
        if (previous == 1) {
            const now_ns = platform_time.monotonicNs();
            self.latency_sensitive_derived_replay_quiet_until_ns.store(
                now_ns +| dense_replay_soft_compaction_quiet_ns,
                .release,
            );
        }
    }

    pub fn shouldDeferSoftCompactionForDerivedReplay(self: *const ResourceManager) bool {
        if (self.latency_sensitive_derived_replay_sessions.load(.acquire) != 0) return true;
        return platform_time.monotonicNs() < self.latency_sensitive_derived_replay_quiet_until_ns.load(.acquire);
    }

    pub fn queryEmbeddingPolicy(self: *const ResourceManager) QueryEmbeddingPolicy {
        return .{
            .enabled = self.query_embedding_cache_budget.max_bytes != 0 and self.query_embedding_cache_ttl_ns != 0,
            .ttl_ns = self.query_embedding_cache_ttl_ns,
            .max_inflight = self.query_embedding_max_inflight,
        };
    }

    pub fn recordIndexRepairActivation(self: *ResourceManager, pause_ns: u64, budget_ns: u64) void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        self.index_repair_activation.attempts +|= 1;
        if (pause_ns > budget_ns) self.index_repair_activation.overruns +|= 1;
        self.index_repair_activation.last_pause_ns = pause_ns;
        self.index_repair_activation.max_pause_ns = @max(self.index_repair_activation.max_pause_ns, pause_ns);
        self.index_repair_activation.last_budget_ns = budget_ns;
    }

    pub fn indexRepairActivationStats(self: *ResourceManager) IndexRepairActivationStats {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        return self.index_repair_activation;
    }

    /// Process-lifetime retry counters for the provisioned storage domain.
    /// Per-DB executors can be evicted and reopened, so Prometheus counters
    /// must be owned by this longer-lived manager instead of those executors.
    pub fn recordDerivedRecoverableRetry(self: *ResourceManager, err: anyerror) void {
        self.derived_recoverable_retry_counters.record(err);
    }

    pub fn derivedRecoverableRetryStats(self: *const ResourceManager) DerivedRecoverableRetryStats {
        return self.derived_recoverable_retry_counters.snapshot();
    }

    /// Install the capacity source for this manager's storage domain.
    ///
    /// A source is part of the manager's lifetime contract: DBs copy the
    /// callback when they open and reservations are coordinated by its domain
    /// identity. Replacing it at runtime could split one physical domain into
    /// inconsistent admission views, so installation is immutable. Repeating
    /// the exact same installation is harmless and supports idempotent runtime
    /// composition.
    pub fn installCapacitySource(self: *ResourceManager, source: CapacitySource) !void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        if (self.capacity_source) |existing| {
            if (existing.ptr == source.ptr and
                existing.domain_id == source.domain_id and
                existing.observe == source.observe)
            {
                return;
            }
            return error.CapacitySourceAlreadyInstalled;
        }
        if (self.capacity_domains.count() != 0) return error.CapacitySourceInstalledAfterAdmission;
        self.capacity_source = source;
    }

    pub fn capacitySource(self: *ResourceManager) ?CapacitySource {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        return self.capacity_source;
    }

    /// Required only for managers that have admitted capacity reservations.
    /// Owners must call this after all reservations have been released.
    ///
    /// Observations are manager-owned accounting snapshots rather than leases:
    /// destroying the manager is the terminal cancellation boundary for any
    /// observer that was not explicitly reconciled to zero. This matters for
    /// shutdown and failed-open paths, where the observed allocation and its
    /// manager are torn down together and there is no later consumer of the
    /// ledger. Reservation handles remain strict because they can outlive the
    /// backing allocation and must be released before their manager.
    pub fn deinit(self: *ResourceManager, alloc: std.mem.Allocator) void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        var it = self.capacity_domains.valueIterator();
        while (it.next()) |domain| {
            if (domain.reserved_bytes != 0)
                @panic("resource manager deinitialized with live capacity reservations");
        }
        if (self.reservation_identities.count() != 0)
            @panic("resource manager deinitialized with live reservations");
        if (self.batch_reservation_identities.count() != 0)
            @panic("resource manager deinitialized with live batch reservations");
        self.capacity_domains.deinit(alloc);
        self.capacity_domains = .empty;
        self.reservation_identities.deinit(self.identity_allocator);
        self.reservation_identities = .empty;
        self.batch_reservation_identities.deinit(self.identity_allocator);
        self.batch_reservation_identities = .empty;
        self.observer_identities.deinit(self.identity_allocator);
        self.observer_identities = .empty;
    }

    pub fn reserveCapacity(
        self: *ResourceManager,
        alloc: std.mem.Allocator,
        domain_id: CapacityDomainId,
        bytes: u64,
        observation: CapacityObservation,
        now_ns: u64,
    ) !CapacityReservation {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const entry = try self.capacity_domains.getOrPut(alloc, domain_id);
        if (!entry.found_existing) entry.value_ptr.* = .{};
        const domain = entry.value_ptr;
        self.observeCapacityLocked(domain, observation);
        domain.last_safety_floor_bytes = self.capacitySafetyFloorLocked(observation);
        if (observation.stale(now_ns)) {
            domain.stale_observations +|= 1;
            domain.denials +|= 1;
            return error.CapacityObservationStale;
        }
        const next = std.math.add(u64, domain.reserved_bytes, bytes) catch {
            domain.denials +|= 1;
            return error.CapacityUnavailable;
        };
        if (!self.capacityFitsLocked(next, observation)) {
            domain.denials +|= 1;
            return error.CapacityUnavailable;
        }
        domain.reserved_bytes = next;
        domain.peak_reserved_bytes = @max(domain.peak_reserved_bytes, next);
        domain.reservations +|= 1;
        return .{ .manager = self, .domain_id = domain_id, .bytes = bytes };
    }

    fn growCapacityReservation(
        self: *ResourceManager,
        domain_id: CapacityDomainId,
        additional_bytes: u64,
        observation: CapacityObservation,
        now_ns: u64,
    ) !void {
        if (additional_bytes == 0) return;
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const domain = self.capacity_domains.getPtr(domain_id) orelse return error.CapacityReservationReleased;
        self.observeCapacityLocked(domain, observation);
        domain.last_safety_floor_bytes = self.capacitySafetyFloorLocked(observation);
        if (observation.stale(now_ns)) {
            domain.stale_observations +|= 1;
            domain.denials +|= 1;
            domain.growth_denials +|= 1;
            return error.CapacityObservationStale;
        }
        const next = std.math.add(u64, domain.reserved_bytes, additional_bytes) catch {
            domain.denials +|= 1;
            domain.growth_denials +|= 1;
            return error.CapacityUnavailable;
        };
        if (!self.capacityFitsLocked(next, observation)) {
            domain.denials +|= 1;
            domain.growth_denials +|= 1;
            return error.CapacityUnavailable;
        }
        domain.reserved_bytes = next;
        domain.peak_reserved_bytes = @max(domain.peak_reserved_bytes, next);
    }

    fn revalidateCapacityReservation(
        self: *ResourceManager,
        domain_id: CapacityDomainId,
        observation: CapacityObservation,
        now_ns: u64,
    ) !void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const domain = self.capacity_domains.getPtr(domain_id) orelse return error.CapacityReservationReleased;
        self.observeCapacityLocked(domain, observation);
        domain.last_safety_floor_bytes = self.capacitySafetyFloorLocked(observation);
        if (observation.stale(now_ns)) {
            domain.stale_observations +|= 1;
            domain.denials +|= 1;
            domain.growth_denials +|= 1;
            return error.CapacityObservationStale;
        }
        if (!self.capacityFitsLocked(domain.reserved_bytes, observation)) {
            domain.denials +|= 1;
            domain.growth_denials +|= 1;
            return error.CapacityUnavailable;
        }
    }

    fn capacityReservationFits(
        self: *ResourceManager,
        domain_id: CapacityDomainId,
        observation: CapacityObservation,
        now_ns: u64,
    ) !bool {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const domain = self.capacity_domains.getPtr(domain_id) orelse return error.CapacityReservationReleased;
        self.observeCapacityLocked(domain, observation);
        domain.last_safety_floor_bytes = self.capacitySafetyFloorLocked(observation);
        if (observation.stale(now_ns)) {
            // Staleness cannot be corrected by reconciling materialized bytes;
            // it is a real fail-closed admission event.
            domain.stale_observations +|= 1;
            domain.denials +|= 1;
            domain.growth_denials +|= 1;
            return error.CapacityObservationStale;
        }
        return self.capacityFitsLocked(domain.reserved_bytes, observation);
    }

    fn releaseCapacity(self: *ResourceManager, domain_id: CapacityDomainId, bytes: u64) void {
        if (bytes == 0) return;
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const domain = self.capacity_domains.getPtr(domain_id) orelse unreachable;
        std.debug.assert(domain.reserved_bytes >= bytes);
        domain.reserved_bytes -= bytes;
    }

    fn observeCapacityLocked(_: *ResourceManager, domain: *MutableCapacityDomain, observation: CapacityObservation) void {
        domain.last_available_bytes = observation.available_bytes;
        domain.last_capacity_bytes = observation.capacity_bytes;
        domain.last_observed_at_ns = observation.observed_at_ns;
    }

    fn capacityFitsLocked(self: *ResourceManager, reserved_bytes: u64, observation: CapacityObservation) bool {
        const available = observation.available_bytes orelse return true;
        return reserved_bytes <= available -| self.capacitySafetyFloorLocked(observation);
    }

    fn capacitySafetyFloorLocked(self: *ResourceManager, observation: CapacityObservation) u64 {
        var floor = self.disk_safety_floor_bytes;
        if (self.disk_safety_floor_divisor != 0) {
            if (observation.capacity_bytes) |capacity| {
                floor = @max(floor, capacity / self.disk_safety_floor_divisor);
            }
        }
        return floor;
    }

    pub fn capacityStats(self: *ResourceManager) CapacityStats {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        var out = CapacityStats{ .domain_count = self.capacity_domains.count() };
        var it = self.capacity_domains.valueIterator();
        while (it.next()) |domain| {
            out.reserved_bytes +|= domain.reserved_bytes;
            out.peak_reserved_bytes +|= domain.peak_reserved_bytes;
            out.reservations +|= domain.reservations;
            out.denials +|= domain.denials;
            out.growth_denials +|= domain.growth_denials;
            out.stale_observations +|= domain.stale_observations;
        }
        return out;
    }

    pub fn capacityDomainStats(self: *ResourceManager, alloc: std.mem.Allocator) ![]CapacityDomainStats {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const out = try alloc.alloc(CapacityDomainStats, self.capacity_domains.count());
        var index: usize = 0;
        var it = self.capacity_domains.iterator();
        while (it.next()) |entry| : (index += 1) {
            const domain = entry.value_ptr.*;
            out[index] = .{
                .domain_id = entry.key_ptr.*,
                .reserved_bytes = domain.reserved_bytes,
                .peak_reserved_bytes = domain.peak_reserved_bytes,
                .reservations = domain.reservations,
                .denials = domain.denials,
                .growth_denials = domain.growth_denials,
                .stale_observations = domain.stale_observations,
                .last_available_bytes = domain.last_available_bytes,
                .last_capacity_bytes = domain.last_capacity_bytes,
                .last_safety_floor_bytes = domain.last_safety_floor_bytes,
                .last_observed_at_ns = domain.last_observed_at_ns,
            };
        }
        return out;
    }

    pub fn derivedBacklogLimits(self: *const ResourceManager) DerivedBacklogLimits {
        return .{
            .high_sequences = self.derived_backlog_high_sequences,
            .resume_sequences = self.derived_backlog_resume_sequences,
            .throttle_window_sequences = self.derived_backlog_throttle_window_sequences,
        };
    }

    pub const ClassifiedBatchReserveError = error{
        DuplicateResourceSlice,
        ResourceRequestTooLarge,
        ResourceTemporarilyUnavailable,
    };

    fn normalizeSliceAmounts(
        amounts: []const SliceAmount,
    ) error{DuplicateResourceSlice}![slice_count]u64 {
        var normalized = [_]u64{0} ** slice_count;
        for (amounts) |amount| {
            if (amount.bytes == 0) continue;
            const index = sliceIndex(amount.slice);
            if (normalized[index] != 0) return error.DuplicateResourceSlice;
            normalized[index] = amount.bytes;
        }
        return normalized;
    }

    fn hostChargeForAmounts(amounts: []const SliceAmount) ClassifiedBatchReserveError!u64 {
        var total: u64 = 0;
        for (amounts) |amount| {
            total = std.math.add(u64, total, amount.bytes) catch
                return error.ResourceRequestTooLarge;
        }
        return total;
    }

    fn commitMemoryLocked(self: *ResourceManager, next: u64) void {
        self.memory.used_bytes = next;
        self.memory.peak_bytes = @max(self.memory.peak_bytes, next);
        if (self.memory.budget.soft_limit_bytes > 0 and next > self.memory.budget.soft_limit_bytes)
            self.memory.soft_limit_events +|= 1;
    }

    fn checkMemoryLocked(self: *ResourceManager, additional_bytes: u64) !u64 {
        const next = std.math.add(u64, self.memory.used_bytes, additional_bytes) catch {
            self.memory.hard_limit_rejections +|= 1;
            return error.ResourceBudgetExceeded;
        };
        if (self.memory.budget.hard_limit_bytes > 0 and next > self.memory.budget.hard_limit_bytes) {
            self.memory.hard_limit_rejections +|= 1;
            return error.ResourceBudgetExceeded;
        }
        return next;
    }

    fn issueIdentityLocked(self: *ResourceManager) u64 {
        var identity = self.next_identity;
        while (identity == 0 or
            self.reservation_identities.contains(identity) or
            self.batch_reservation_identities.contains(identity))
        {
            identity +%= 1;
        }
        self.next_identity = identity +% 1;
        if (self.next_identity == 0) self.next_identity = 1;
        return identity;
    }

    fn registerReservationIdentityLocked(
        self: *ResourceManager,
        slice: Slice,
        bytes: u64,
    ) !u64 {
        const identity = self.issueIdentityLocked();
        try self.reservation_identities.put(
            self.identity_allocator,
            identity,
            .{ .slice = slice, .bytes = bytes },
        );
        return identity;
    }

    fn reservationIdentityLocked(
        self: *ResourceManager,
        reservation: *Reservation,
    ) !*ReservationIdentity {
        if (reservation.identity == 0) {
            if (reservation.bytes != 0) {
                self.memory.accounting_errors +|= 1;
                return error.ResourceAccountingMismatch;
            }
            reservation.identity = try self.registerReservationIdentityLocked(
                reservation.slice,
                0,
            );
        }
        const owned = self.reservation_identities.getPtr(reservation.identity) orelse {
            self.memory.accounting_errors +|= 1;
            return error.ReservationReleased;
        };
        if (owned.slice != reservation.slice or owned.bytes != reservation.bytes) {
            self.memory.accounting_errors +|= 1;
            return error.ResourceAccountingMismatch;
        }
        return owned;
    }

    /// Atomically reserve several independent slices while distinguishing a
    /// request that can never fit from temporary contention with reservations
    /// already held by other work.
    ///
    /// The intrinsic-size pass deliberately precedes the contention pass so a
    /// multi-slice request has a stable classification independent of slice
    /// order. Both passes and the commit occur under one lock.
    pub fn reserveBatchClassified(
        self: *ResourceManager,
        amounts: []const SliceAmount,
    ) ClassifiedBatchReserveError!BatchReservation {
        const host_charge_bytes = try hostChargeForAmounts(amounts);
        return self.reserveBatchClassifiedWithHostCharge(amounts, host_charge_bytes);
    }

    /// Reserve logical slices while charging only their physical host-memory
    /// component to the process aggregate. This is used by bridges whose
    /// logical metrics include accelerator bytes; ordinary callers should use
    /// `reserveBatchClassified`, which charges the full batch.
    pub fn reserveBatchClassifiedWithHostCharge(
        self: *ResourceManager,
        amounts: []const SliceAmount,
        host_charge_bytes: u64,
    ) ClassifiedBatchReserveError!BatchReservation {
        return self.reserveBatchClassifiedWithHostChargeOnce(amounts, host_charge_bytes) catch |err| {
            if (err != error.ResourceTemporarilyUnavailable) return err;
            const requester = blk: {
                for (amounts) |amount| switch (amount.slice) {
                    .hbc_node_metadata_cache, .lsm_block_table_cache => {},
                    else => break :blk amount.slice,
                };
                break :blk if (amounts.len == 0) Slice.dense_apply_working_set else amounts[0].slice;
            };
            if (self.reclaimForAllocation(requester, host_charge_bytes) == 0) return err;
            return self.reserveBatchClassifiedWithHostChargeOnce(amounts, host_charge_bytes);
        };
    }

    fn reserveBatchClassifiedWithHostChargeOnce(
        self: *ResourceManager,
        amounts: []const SliceAmount,
        host_charge_bytes: u64,
    ) ClassifiedBatchReserveError!BatchReservation {
        const normalized = try normalizeSliceAmounts(amounts);

        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const memory_hard_limit = self.memory.budget.hard_limit_bytes;
        if (memory_hard_limit > 0 and host_charge_bytes > memory_hard_limit) {
            self.memory.hard_limit_rejections +|= 1;
            return error.ResourceRequestTooLarge;
        }

        for (amounts) |amount| {
            if (amount.bytes == 0) continue;
            const state = &self.slices[sliceIndex(amount.slice)];
            const hard_limit = state.budget.hard_limit_bytes;
            if (hard_limit > 0 and amount.bytes > hard_limit) {
                state.hard_limit_rejections +|= 1;
                return error.ResourceRequestTooLarge;
            }
        }

        const next_memory = std.math.add(u64, self.memory.used_bytes, host_charge_bytes) catch {
            self.memory.hard_limit_rejections +|= 1;
            return error.ResourceTemporarilyUnavailable;
        };
        if (memory_hard_limit > 0 and next_memory > memory_hard_limit) {
            self.memory.hard_limit_rejections +|= 1;
            return error.ResourceTemporarilyUnavailable;
        }

        for (amounts) |amount| {
            if (amount.bytes == 0) continue;
            const state = &self.slices[sliceIndex(amount.slice)];
            const next = std.math.add(u64, state.used_bytes, amount.bytes) catch {
                state.hard_limit_rejections +|= 1;
                return error.ResourceTemporarilyUnavailable;
            };
            if (state.budget.hard_limit_bytes > 0 and next > state.budget.hard_limit_bytes) {
                state.hard_limit_rejections +|= 1;
                return error.ResourceTemporarilyUnavailable;
            }
        }

        const identity = self.issueIdentityLocked();
        self.batch_reservation_identities.put(
            self.identity_allocator,
            identity,
            .{
                .amounts = normalized,
                .host_charge_bytes = host_charge_bytes,
            },
        ) catch {
            self.memory.hard_limit_rejections +|= 1;
            return error.ResourceTemporarilyUnavailable;
        };

        for (amounts) |amount| {
            if (amount.bytes == 0) continue;
            const state = &self.slices[sliceIndex(amount.slice)];
            state.used_bytes += amount.bytes;
            state.peak_bytes = @max(state.peak_bytes, state.used_bytes);
            if (state.budget.soft_limit_bytes > 0 and state.used_bytes > state.budget.soft_limit_bytes)
                state.soft_limit_events +|= 1;
        }
        self.commitMemoryLocked(next_memory);
        self.pressure_change.advance();
        return .{
            .manager = self,
            .identity = identity,
            .amounts = normalized,
            .host_charge_bytes = host_charge_bytes,
        };
    }

    /// Compatibility wrapper for callers that do not need denial
    /// classification.
    pub fn reserveBatch(self: *ResourceManager, amounts: []const SliceAmount) !BatchReservation {
        return self.reserveBatchClassified(amounts) catch |err| switch (err) {
            error.DuplicateResourceSlice => error.DuplicateResourceSlice,
            error.ResourceRequestTooLarge,
            error.ResourceTemporarilyUnavailable,
            => error.ResourceBudgetExceeded,
        };
    }

    fn retainBatchReservation(
        self: *ResourceManager,
        reservation: *BatchReservation,
        retained: [slice_count]u64,
        retained_host_charge_bytes: u64,
    ) !void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const owned = self.batch_reservation_identities.getPtr(reservation.identity) orelse {
            self.memory.accounting_errors +|= 1;
            return error.ReservationReleased;
        };
        if (!std.meta.eql(owned.amounts, reservation.amounts) or
            owned.host_charge_bytes != reservation.host_charge_bytes)
        {
            self.memory.accounting_errors +|= 1;
            return error.ResourceAccountingMismatch;
        }

        if (retained_host_charge_bytes > reservation.host_charge_bytes) {
            self.memory.accounting_errors +|= 1;
            return error.InvalidReservationReduction;
        }
        inline for (0..slice_count) |index| {
            if (retained[index] > reservation.amounts[index]) {
                self.memory.accounting_errors +|= 1;
                return error.InvalidReservationReduction;
            }
        }

        const released_host = reservation.host_charge_bytes - retained_host_charge_bytes;
        if (released_host > self.memory.used_bytes) {
            self.memory.accounting_errors +|= 1;
            return error.ResourceAccountingMismatch;
        }
        inline for (0..slice_count) |index| {
            const released = reservation.amounts[index] - retained[index];
            if (released > self.slices[index].used_bytes) {
                self.memory.accounting_errors +|= 1;
                return error.ResourceAccountingMismatch;
            }
        }
        inline for (0..slice_count) |index| {
            self.slices[index].used_bytes -= reservation.amounts[index] - retained[index];
        }
        self.memory.used_bytes -= released_host;
        reservation.amounts = retained;
        reservation.host_charge_bytes = retained_host_charge_bytes;
        owned.amounts = retained;
        owned.host_charge_bytes = retained_host_charge_bytes;
        self.pressure_change.advance();
    }

    pub fn recordAccountingError(self: *ResourceManager) void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        self.memory.accounting_errors +|= 1;
    }

    fn releaseBatchReservation(
        self: *ResourceManager,
        reservation: *const BatchReservation,
    ) bool {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const owned = self.batch_reservation_identities.get(reservation.identity) orelse {
            self.memory.accounting_errors +|= 1;
            return false;
        };
        if (!std.meta.eql(owned.amounts, reservation.amounts) or
            owned.host_charge_bytes != reservation.host_charge_bytes)
        {
            self.memory.accounting_errors +|= 1;
            return false;
        }

        if (reservation.host_charge_bytes > self.memory.used_bytes) {
            self.memory.accounting_errors +|= 1;
            return false;
        }
        inline for (0..slice_count) |index| {
            if (reservation.amounts[index] > self.slices[index].used_bytes) {
                self.memory.accounting_errors +|= 1;
                return false;
            }
        }
        inline for (0..slice_count) |index|
            self.slices[index].used_bytes -= reservation.amounts[index];
        self.memory.used_bytes -= reservation.host_charge_bytes;
        _ = self.batch_reservation_identities.remove(reservation.identity);
        self.pressure_change.advance();
        return true;
    }

    pub fn reserve(self: *ResourceManager, slice: Slice, bytes: u64) !Reservation {
        return self.reserveOnce(slice, bytes) catch |err| {
            if (err != error.ResourceBudgetExceeded) return err;
            if (self.reclaimForAllocation(slice, bytes) == 0) return err;
            return self.reserveOnce(slice, bytes);
        };
    }

    fn reserveOnce(self: *ResourceManager, slice: Slice, bytes: u64) !Reservation {
        if (bytes == 0) return .{ .manager = self, .identity = 0, .slice = slice, .bytes = 0 };

        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const idx = sliceIndex(slice);
        const state = &self.slices[idx];
        const next = std.math.add(u64, state.used_bytes, bytes) catch {
            state.hard_limit_rejections += 1;
            return error.ResourceBudgetExceeded;
        };
        if (state.budget.hard_limit_bytes > 0 and next > state.budget.hard_limit_bytes) {
            state.hard_limit_rejections += 1;
            return error.ResourceBudgetExceeded;
        }
        const next_memory = try self.checkMemoryLocked(bytes);
        const identity = try self.registerReservationIdentityLocked(slice, bytes);
        state.used_bytes = next;
        state.peak_bytes = @max(state.peak_bytes, next);
        if (state.budget.soft_limit_bytes > 0 and next > state.budget.soft_limit_bytes) {
            state.soft_limit_events += 1;
        }
        self.commitMemoryLocked(next_memory);
        self.pressure_change.advance();
        return .{ .manager = self, .identity = identity, .slice = slice, .bytes = bytes };
    }

    /// Admits one bounded minimum-progress operation even when its working set
    /// is larger than the slice's normal hard limit. The slice must otherwise
    /// be idle, so this cannot multiply memory through concurrent oversized
    /// jobs. Usage remains fully accounted and therefore exposes hard pressure
    /// until the reservation is released.
    pub fn reserveBoundedOversizedSingle(
        self: *ResourceManager,
        slice: Slice,
        bytes: u64,
        max_hard_limit_multiple: u64,
    ) !Reservation {
        if (bytes == 0) return .{ .manager = self, .identity = 0, .slice = slice, .bytes = 0 };

        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const state = &self.slices[sliceIndex(slice)];
        const next = std.math.add(u64, state.used_bytes, bytes) catch {
            state.hard_limit_rejections +|= 1;
            return error.ResourceBudgetExceeded;
        };
        const hard_limit = state.budget.hard_limit_bytes;
        var oversized_grant = false;
        if (hard_limit > 0 and next > hard_limit) {
            const bounded_limit = std.math.mul(u64, hard_limit, max_hard_limit_multiple) catch std.math.maxInt(u64);
            if (max_hard_limit_multiple <= 1 or state.used_bytes != 0 or bytes > bounded_limit) {
                state.hard_limit_rejections +|= 1;
                return error.ResourceBudgetExceeded;
            }
            oversized_grant = true;
        }
        // Minimum-progress exceptions are local slice policy; they may never
        // bypass the process-wide physical-memory envelope.
        const next_memory = try self.checkMemoryLocked(bytes);
        const identity = try self.registerReservationIdentityLocked(slice, bytes);
        if (oversized_grant) state.oversized_single_grants +|= 1;
        state.used_bytes = next;
        state.peak_bytes = @max(state.peak_bytes, next);
        if (state.budget.soft_limit_bytes > 0 and next > state.budget.soft_limit_bytes) {
            state.soft_limit_events +|= 1;
        }
        self.commitMemoryLocked(next_memory);
        self.pressure_change.advance();
        return .{ .manager = self, .identity = identity, .slice = slice, .bytes = bytes };
    }

    fn growReservationBoundedOversized(
        self: *ResourceManager,
        reservation: *Reservation,
        additional_bytes: u64,
        max_hard_limit_multiple: u64,
    ) !void {
        if (additional_bytes == 0) return;
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const owned = try self.reservationIdentityLocked(reservation);
        const state = &self.slices[sliceIndex(reservation.slice)];
        const next = std.math.add(u64, state.used_bytes, additional_bytes) catch {
            state.hard_limit_rejections +|= 1;
            return error.ResourceBudgetExceeded;
        };
        const next_reservation = std.math.add(u64, reservation.bytes, additional_bytes) catch {
            state.hard_limit_rejections +|= 1;
            return error.ResourceBudgetExceeded;
        };
        const hard_limit = state.budget.hard_limit_bytes;
        var oversized_grant = false;
        if (hard_limit > 0 and next > hard_limit) {
            const bounded_limit = std.math.mul(u64, hard_limit, max_hard_limit_multiple) catch std.math.maxInt(u64);
            const reservation_is_sole_user = state.used_bytes == reservation.bytes;
            if (max_hard_limit_multiple <= 1 or !reservation_is_sole_user or next_reservation > bounded_limit) {
                state.hard_limit_rejections +|= 1;
                return error.ResourceBudgetExceeded;
            }
            oversized_grant = reservation.bytes <= hard_limit;
        }
        const next_memory = try self.checkMemoryLocked(additional_bytes);
        if (oversized_grant) state.oversized_single_grants +|= 1;
        state.used_bytes = next;
        reservation.bytes = next_reservation;
        owned.bytes = next_reservation;
        state.peak_bytes = @max(state.peak_bytes, next);
        if (state.budget.soft_limit_bytes > 0 and next > state.budget.soft_limit_bytes) {
            state.soft_limit_events +|= 1;
        }
        self.commitMemoryLocked(next_memory);
        self.pressure_change.advance();
    }

    fn growReservationAmortized(
        self: *ResourceManager,
        reservation: *Reservation,
        minimum_bytes: u64,
        preferred_bytes: u64,
        max_hard_limit_multiple: u64,
    ) !u64 {
        if (minimum_bytes == 0) return 0;
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const owned = try self.reservationIdentityLocked(reservation);
        const state = &self.slices[sliceIndex(reservation.slice)];
        const hard_limit = state.budget.hard_limit_bytes;
        const bounded_limit = if (hard_limit == 0)
            std.math.maxInt(u64)
        else
            std.math.mul(u64, hard_limit, max_hard_limit_multiple) catch std.math.maxInt(u64);
        const reservation_is_sole_user = state.used_bytes == reservation.bytes;

        const Candidate = struct {
            fn allowed(
                current_used: u64,
                current_reservation: u64,
                additional: u64,
                hard: u64,
                bounded: u64,
                sole_user: bool,
                multiple: u64,
            ) bool {
                const next = std.math.add(u64, current_used, additional) catch return false;
                if (hard == 0 or next <= hard) return true;
                if (multiple <= 1 or !sole_user) return false;
                const next_reservation = std.math.add(u64, current_reservation, additional) catch return false;
                return next_reservation <= bounded;
            }
        };

        var granted = @max(minimum_bytes, preferred_bytes);
        if (!Candidate.allowed(
            state.used_bytes,
            reservation.bytes,
            granted,
            hard_limit,
            bounded_limit,
            reservation_is_sole_user,
            max_hard_limit_multiple,
        )) {
            granted = minimum_bytes;
            if (!Candidate.allowed(
                state.used_bytes,
                reservation.bytes,
                granted,
                hard_limit,
                bounded_limit,
                reservation_is_sole_user,
                max_hard_limit_multiple,
            )) {
                state.hard_limit_rejections +|= 1;
                return error.ResourceBudgetExceeded;
            }
        }

        const MemoryCandidate = struct {
            fn allowed(current: u64, additional: u64, hard: u64) bool {
                const next = std.math.add(u64, current, additional) catch return false;
                return hard == 0 or next <= hard;
            }
        };
        if (!MemoryCandidate.allowed(self.memory.used_bytes, granted, self.memory.budget.hard_limit_bytes)) {
            granted = minimum_bytes;
            if (!Candidate.allowed(
                state.used_bytes,
                reservation.bytes,
                granted,
                hard_limit,
                bounded_limit,
                reservation_is_sole_user,
                max_hard_limit_multiple,
            ) or !MemoryCandidate.allowed(
                self.memory.used_bytes,
                granted,
                self.memory.budget.hard_limit_bytes,
            )) {
                self.memory.hard_limit_rejections +|= 1;
                return error.ResourceBudgetExceeded;
            }
        }

        const previous_reservation = reservation.bytes;
        const next_memory = std.math.add(u64, self.memory.used_bytes, granted) catch unreachable;
        state.used_bytes += granted;
        reservation.bytes += granted;
        owned.bytes = reservation.bytes;
        state.peak_bytes = @max(state.peak_bytes, state.used_bytes);
        if (hard_limit > 0 and state.used_bytes > hard_limit and previous_reservation <= hard_limit)
            state.oversized_single_grants +|= 1;
        if (state.budget.soft_limit_bytes > 0 and state.used_bytes > state.budget.soft_limit_bytes)
            state.soft_limit_events +|= 1;
        self.commitMemoryLocked(next_memory);
        self.pressure_change.advance();
        return granted;
    }

    fn releaseReservation(self: *ResourceManager, reservation: *const Reservation) bool {
        if (reservation.identity == 0 and reservation.bytes == 0) return true;
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const owned = self.reservation_identities.get(reservation.identity) orelse {
            self.memory.accounting_errors +|= 1;
            return false;
        };
        if (owned.slice != reservation.slice or owned.bytes != reservation.bytes) {
            self.memory.accounting_errors +|= 1;
            return false;
        }
        const state = &self.slices[sliceIndex(owned.slice)];
        if (owned.bytes > state.used_bytes or owned.bytes > self.memory.used_bytes) {
            self.memory.accounting_errors +|= 1;
            return false;
        }
        state.used_bytes -= owned.bytes;
        self.memory.used_bytes -= owned.bytes;
        _ = self.reservation_identities.remove(reservation.identity);
        self.pressure_change.advance();
        return true;
    }

    fn shrinkReservation(self: *ResourceManager, reservation: *Reservation, bytes: u64) bool {
        if (bytes == 0 or reservation.bytes == 0) return true;
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const owned = self.reservation_identities.getPtr(reservation.identity) orelse {
            self.memory.accounting_errors +|= 1;
            return false;
        };
        if (owned.slice != reservation.slice or owned.bytes != reservation.bytes) {
            self.memory.accounting_errors +|= 1;
            return false;
        }
        const released = @min(bytes, owned.bytes);
        const state = &self.slices[sliceIndex(owned.slice)];
        if (released > state.used_bytes or released > self.memory.used_bytes) {
            self.memory.accounting_errors +|= 1;
            return false;
        }
        state.used_bytes -= released;
        self.memory.used_bytes -= released;
        owned.bytes -= released;
        reservation.bytes -= released;
        self.pressure_change.advance();
        return true;
    }

    fn reconcileUsageLocked(
        self: *ResourceManager,
        slice: Slice,
        observer_id: usize,
        previous: u64,
        next: u64,
        enforce_limits: bool,
    ) !void {
        const key = ObserverKey{ .slice = slice, .identity = observer_id };
        var inserted = false;
        const owned = self.observer_identities.getPtr(key) orelse blk: {
            if (previous != 0) {
                self.memory.accounting_errors +|= 1;
                return error.ResourceAccountingMismatch;
            }
            if (next == 0) return;
            const entry = try self.observer_identities.getOrPut(
                self.identity_allocator,
                key,
            );
            entry.value_ptr.* = .{ .current = 0 };
            inserted = true;
            break :blk entry.value_ptr;
        };
        errdefer {
            if (inserted) _ = self.observer_identities.remove(key);
        }
        if (owned.current != previous) {
            self.memory.accounting_errors +|= 1;
            return error.ResourceAccountingMismatch;
        }

        const state = &self.slices[sliceIndex(slice)];
        if (previous > state.used_bytes or previous > self.memory.used_bytes) {
            self.memory.accounting_errors +|= 1;
            return error.ResourceAccountingMismatch;
        }
        const slice_next = std.math.add(u64, state.used_bytes - previous, next) catch
            return error.ResourceBudgetExceeded;
        const memory_next = std.math.add(u64, self.memory.used_bytes - previous, next) catch
            return error.ResourceBudgetExceeded;
        // Observation-only callers may report allocations that already exist,
        // but admission callers must reject growth before allocation. A
        // validated decrease is always accepted, even while the aggregate is
        // still above a hard limit, so owners can converge out of pressure.
        if (enforce_limits and next > previous) {
            if ((state.budget.hard_limit_bytes > 0 and slice_next > state.budget.hard_limit_bytes) or
                (self.memory.budget.hard_limit_bytes > 0 and memory_next > self.memory.budget.hard_limit_bytes))
            {
                state.hard_limit_rejections +|= 1;
                if (self.memory.budget.hard_limit_bytes > 0 and memory_next > self.memory.budget.hard_limit_bytes)
                    self.memory.hard_limit_rejections +|= 1;
                return error.ResourceBudgetExceeded;
            }
        }

        state.used_bytes = slice_next;
        self.memory.used_bytes = memory_next;
        owned.current = next;
        state.peak_bytes = @max(state.peak_bytes, slice_next);
        self.memory.peak_bytes = @max(self.memory.peak_bytes, memory_next);
        if (state.budget.soft_limit_bytes > 0 and slice_next > state.budget.soft_limit_bytes)
            state.soft_limit_events +|= 1;
        if (!enforce_limits and state.budget.hard_limit_bytes > 0 and slice_next > state.budget.hard_limit_bytes)
            state.hard_limit_rejections +|= 1;
        if (self.memory.budget.soft_limit_bytes > 0 and memory_next > self.memory.budget.soft_limit_bytes)
            self.memory.soft_limit_events +|= 1;
        if (!enforce_limits and self.memory.budget.hard_limit_bytes > 0 and memory_next > self.memory.budget.hard_limit_bytes)
            self.memory.hard_limit_rejections +|= 1;
        if (next == 0) _ = self.observer_identities.remove(key);
        self.pressure_change.advance();
    }

    pub fn adjustUsage(self: *ResourceManager, slice: Slice, current: *u64, next: u64) !void {
        self.adjustUsageOnce(slice, current, next) catch |err| {
            if (err != error.ResourceBudgetExceeded or next <= current.*) return err;
            if (self.reclaimForAllocation(slice, next - current.*) == 0) return err;
            return self.adjustUsageOnce(slice, current, next);
        };
    }

    fn adjustUsageOnce(self: *ResourceManager, slice: Slice, current: *u64, next: u64) !void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        try self.reconcileUsageLocked(slice, @intFromPtr(current), current.*, next, true);
        current.* = next;
    }

    pub fn observeUsage(self: *ResourceManager, slice: Slice, current: *u64, next: u64) void {
        _ = self.tryObserveUsage(slice, current, next);
    }

    /// Reconcile one observer-owned contribution without allowing stale input
    /// to debit bytes belonging to another observer or slice. On failure both
    /// ledgers and the caller's current value remain unchanged.
    pub fn tryObserveUsage(self: *ResourceManager, slice: Slice, current: *u64, next: u64) bool {
        if (!self.tryObserveUsageIdentity(slice, @intFromPtr(current), current.*, next))
            return false;
        current.* = next;
        return true;
    }

    /// Reconcile an owner-issued stable observer identity. ABI adapters use
    /// this form because their local hash-map values may move during growth;
    /// identity remains stable and stale previous totals still fail closed.
    pub fn tryObserveUsageIdentity(
        self: *ResourceManager,
        slice: Slice,
        observer_id: usize,
        previous: u64,
        next: u64,
    ) bool {
        if (observer_id == 0) {
            self.recordAccountingError();
            return false;
        }
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        self.reconcileUsageLocked(slice, observer_id, previous, next, false) catch {
            return false;
        };
        return true;
    }

    /// Reconcile an owner-issued stable identity as an admission operation.
    /// Growth is rejected before it crosses either the slice or aggregate hard
    /// limit. Validated shrink and teardown always remain possible so pressure
    /// cannot strand cache memory above a newly tightened limit.
    pub fn tryAdjustUsageIdentity(
        self: *ResourceManager,
        slice: Slice,
        observer_id: usize,
        previous: u64,
        next: u64,
    ) bool {
        if (observer_id == 0) {
            self.recordAccountingError();
            return false;
        }
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        self.reconcileUsageLocked(slice, observer_id, previous, next, true) catch {
            return false;
        };
        return true;
    }

    pub fn snapshot(self: *ResourceManager) Stats {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        var stats: [slice_count]SliceStats = undefined;
        inline for (std.enums.values(Slice), 0..) |slice, i| {
            const state = self.slices[i];
            stats[i] = .{
                .name = slice.name(),
                .used_bytes = state.used_bytes,
                .peak_bytes = state.peak_bytes,
                .soft_limit_bytes = state.budget.soft_limit_bytes,
                .hard_limit_bytes = state.budget.hard_limit_bytes,
                .soft_limit_events = state.soft_limit_events,
                .hard_limit_rejections = state.hard_limit_rejections,
                .oversized_single_grants = state.oversized_single_grants,
                .pressure = pressureFor(state.budget, state.used_bytes),
                .soft_action = state.policy.soft_action,
                .hard_action = state.policy.hard_action,
            };
        }
        return .{
            .memory = .{
                .used_bytes = self.memory.used_bytes,
                .peak_bytes = self.memory.peak_bytes,
                .soft_limit_bytes = self.memory.budget.soft_limit_bytes,
                .hard_limit_bytes = self.memory.budget.hard_limit_bytes,
                .soft_limit_events = self.memory.soft_limit_events,
                .hard_limit_rejections = self.memory.hard_limit_rejections,
                .accounting_errors = self.memory.accounting_errors,
                .pressure = pressureFor(self.memory.budget, self.memory.used_bytes),
            },
            .slices = stats,
            .reclaim_requests = self.reclaim_requests.load(.monotonic),
            .reclaimed_bytes = self.reclaimed_bytes.load(.monotonic),
        };
    }

    pub fn sliceStats(self: *ResourceManager, slice: Slice) SliceStats {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const state = self.slices[sliceIndex(slice)];
        return sliceStatsFromState(slice, state);
    }

    /// Returns the configured response for the slice's current pressure. Usage
    /// observed outside ResourceManager (for example allocator-backed LSM
    /// state) must consult this decision at its admission boundary; observing
    /// usage alone intentionally does not reject an allocation after the fact.
    pub fn pressureDecision(self: *ResourceManager, slice: Slice) PressureDecision {
        return self.admissionDecision(slice, 0);
    }

    /// Returns whether the configured policy requires background work to yield
    /// at the slice's current pressure. Rejection is also a deferral signal for
    /// maintenance: foreground admission owns the remaining capacity.
    pub fn shouldDeferBackgroundWork(self: *ResourceManager, slice: Slice) bool {
        const decision = self.pressureDecision(slice);
        if (decision.pressure == .normal) return false;
        return decision.action == .defer_background_work or decision.action == .reject_work;
    }

    /// Evaluates an allocation before it joins an externally accounted slice.
    /// The returned epoch can be used to sleep until some producer releases or
    /// otherwise changes accounted usage.
    pub fn admissionDecision(self: *ResourceManager, slice: Slice, additional_bytes: u64) PressureDecision {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const state = self.slices[sliceIndex(slice)];
        const projected_bytes = state.used_bytes +| additional_bytes;
        const slice_pressure = pressureFor(state.budget, projected_bytes);
        const projected_memory = self.memory.used_bytes +| additional_bytes;
        const memory_pressure = pressureFor(self.memory.budget, projected_memory);
        const aggregate_dominates = memory_pressure != .normal and
            @intFromEnum(memory_pressure) >= @intFromEnum(slice_pressure);
        const pressure = if (aggregate_dominates) memory_pressure else slice_pressure;
        return .{
            .pressure = pressure,
            .action = if (aggregate_dominates and pressure == .hard)
                .reject_work
            else switch (pressure) {
                .normal => .report,
                .soft => state.policy.soft_action,
                .hard => state.policy.hard_action,
            },
            .used_bytes = if (aggregate_dominates) projected_memory else projected_bytes,
            .soft_limit_bytes = if (aggregate_dominates) self.memory.budget.soft_limit_bytes else state.budget.soft_limit_bytes,
            .hard_limit_bytes = if (aggregate_dominates) self.memory.budget.hard_limit_bytes else state.budget.hard_limit_bytes,
            .change_epoch = self.pressure_change.snapshot(),
        };
    }

    pub fn canWaitForPressureChange(_: *const ResourceManager) bool {
        return supports_pressure_wait;
    }

    pub fn waitForPressureChange(self: *ResourceManager, observed_epoch: u64) void {
        self.pressure_change.waitForChange(observed_epoch);
    }

    /// Applies a slice's admission policy at a boundary that is safe to
    /// block. This does not reserve `additional_bytes`; callers must still
    /// re-check or reserve at their mutation/allocation boundary. Its purpose
    /// is to keep pressure waits above higher-level critical sections while a
    /// lower layer retains a non-blocking hard guard.
    pub fn awaitAdmission(self: *ResourceManager, slice: Slice, additional_bytes: u64) !void {
        while (true) {
            const decision = self.admissionDecision(slice, additional_bytes);
            // No amount of waiting can admit a single request whose projected
            // footprint exceeds the entire slice. Avoid an unbounded wait when
            // the configured hard action is write throttling.
            if (decision.hard_limit_bytes > 0 and additional_bytes > decision.hard_limit_bytes) {
                return error.ResourceBudgetExceeded;
            }
            switch (decision.action) {
                .report, .shrink_cache, .defer_background_work => return,
                .reject_work => return error.ResourceBudgetExceeded,
                .throttle_writes => {
                    // Soft pressure is pacing, not a request to hold an HTTP
                    // write until an arbitrarily large compaction publishes.
                    // Apply a small bounded delay, then let the caller reach
                    // its non-blocking projected hard guard. Hard pressure
                    // keeps waiting on an ownership change because admitting
                    // it would only be rejected before WAL append below.
                    if (decision.pressure == .soft) {
                        shared_platform_time.sleepNs(soft_throttle_delay_ns);
                        return;
                    }
                },
            }
            if (!self.canWaitForPressureChange()) {
                if (decision.pressure == .soft) return;
                return error.ResourceBudgetExceeded;
            }
            self.waitForPressureChange(decision.change_epoch);
        }
    }

    /// Returns whether replay retention has reached a hard limit while a
    /// repair is pinning the projection cursor. Keeping this policy here
    /// prevents write paths from owning resource thresholds or taking the
    /// manager mutex once per slice.
    pub fn denseRepairReplayPressureIsHard(self: *ResourceManager) bool {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const wal = self.slices[sliceIndex(.lsm_wal_retention)];
        const backlog = self.slices[sliceIndex(.derived_backlog)];
        return pressureFor(wal.budget, wal.used_bytes) == .hard or
            pressureFor(backlog.budget, backlog.used_bytes) == .hard;
    }

    /// Derive HBC's internal count ceilings from the resource manager's byte
    /// budget. A shared cache owns its own dynamic CLOCK, so local arrays only
    /// need a non-zero gate. Local caches retain bounded arrays while exact
    /// entry bytes are admitted through `hbc_node_metadata_cache`.
    pub fn hbcCacheLimits(self: *ResourceManager, dims: u32, shared_cache: bool) HbcCacheLimits {
        if (shared_cache) {
            return .{
                .max_cached_nodes = 1,
                .max_cached_vectors = 1,
                .max_cached_metadata = 1,
            };
        }

        const stats = self.sliceStats(.hbc_node_metadata_cache);
        const budget_bytes = if (stats.hard_limit_bytes > 0)
            stats.hard_limit_bytes
        else if (stats.soft_limit_bytes > 0)
            stats.soft_limit_bytes
        else
            std.math.maxInt(u64);
        const vector_bytes = @max(
            hbc_estimated_vector_overhead_bytes,
            @as(u64, dims) * @sizeOf(f32) + hbc_estimated_vector_overhead_bytes,
        );
        return .{
            .max_cached_nodes = hbcClockEntries(budget_bytes, hbc_estimated_node_entry_bytes),
            .max_cached_vectors = hbcClockEntries(budget_bytes, vector_bytes),
            .max_cached_metadata = hbcClockEntries(budget_bytes, hbc_estimated_metadata_entry_bytes),
        };
    }

    /// Derive the internal HBC cache-class policy from the process-owned byte
    /// envelope. Routing nodes and quantized routing payloads keep protected
    /// working-set targets; exact vectors consume the elastic remainder and
    /// are the first reclaim source. These targets deliberately sum to less
    /// than the aggregate target so hot classes can borrow unused capacity.
    pub fn hbcCachePolicy(self: *ResourceManager) HbcCachePolicy {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const state = self.slices[sliceIndex(.hbc_node_metadata_cache)];
        const target_bytes = if (state.budget.soft_limit_bytes > 0)
            state.budget.soft_limit_bytes
        else
            state.budget.hard_limit_bytes;
        const slice_pressure = pressureFor(state.budget, state.used_bytes);
        const memory_pressure = pressureFor(self.memory.budget, self.memory.used_bytes);
        const pressure = if (slice_pressure == .hard or memory_pressure == .hard)
            Pressure.hard
        else if (slice_pressure == .soft or memory_pressure == .soft)
            Pressure.soft
        else
            Pressure.normal;

        var policy: HbcCachePolicy = .{
            .target_bytes = target_bytes,
            .node_protected_bytes = target_bytes / 8,
            .quantized_protected_bytes = target_bytes / 4,
            .metadata_protected_bytes = target_bytes / 32,
            .concurrent_vector_admission_stride = switch (pressure) {
                .normal => 1,
                .soft => 8,
                .hard => 0,
            },
        };
        var total_score: u64 = 0;
        for (self.hbc_cache_benefit) |benefit| total_score +|= benefit.score;
        if (total_score == 0 or target_bytes == 0) return policy;

        // Adapt only a bounded quarter of the target. The static priority-band
        // minima above remain intact, so noisy feedback cannot starve routing
        // state or make one observation swing the whole cache.
        const adaptive_pool = target_bytes / 4;
        const node_share = mulDivSaturating(adaptive_pool, self.hbc_cache_benefit[@intFromEnum(HbcCacheClass.node)].score, total_score);
        const quantized_share = mulDivSaturating(adaptive_pool, self.hbc_cache_benefit[@intFromEnum(HbcCacheClass.quantized)].score, total_score);
        const vector_share = mulDivSaturating(adaptive_pool, self.hbc_cache_benefit[@intFromEnum(HbcCacheClass.vector)].score, total_score);
        const metadata_share = mulDivSaturating(adaptive_pool, self.hbc_cache_benefit[@intFromEnum(HbcCacheClass.metadata)].score, total_score);
        policy.node_protected_bytes = @min(target_bytes / 3, policy.node_protected_bytes +| node_share);
        policy.quantized_protected_bytes = @min(target_bytes / 2, policy.quantized_protected_bytes +| quantized_share);
        policy.vector_protected_bytes = @min(target_bytes / 2, vector_share);
        policy.metadata_protected_bytes = @min(target_bytes / 8, policy.metadata_protected_bytes +| metadata_share);
        policy.adaptive = true;
        return policy;
    }

    /// Claim the deliberately sparse cache-feedback sample. Callers use this
    /// before collecting resident-byte snapshots so the 63 unsampled queries
    /// do not pay cache-map locking or aggregation overhead.
    pub fn beginHbcCacheBenefitSample(self: *ResourceManager) bool {
        const ticket = self.hbc_benefit_sample_counter.fetchAdd(1, .monotonic);
        return ticket & 63 == 0;
    }

    pub fn observeHbcCacheBenefitSampled(self: *ResourceManager, samples: [@typeInfo(HbcCacheClass).@"enum".fields.len]HbcCacheBenefitSample) void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        for (samples, 0..) |sample, i| {
            const state = &self.hbc_cache_benefit[i];
            if (sample.hits == 0 and sample.misses == 0) continue;
            if (sample.misses > 0 and sample.miss_service_ns > 0) {
                const observed_service = @max(@as(u64, 1), sample.miss_service_ns / sample.misses);
                state.miss_service_ns_per_miss = if (state.miss_service_ns_per_miss == 0)
                    observed_service
                else
                    state.miss_service_ns_per_miss - state.miss_service_ns_per_miss / 8 + observed_service / 8;
            }
            const raw_score = cacheBenefitPerByte(sample, state.miss_service_ns_per_miss);
            state.score = if (state.observations == 0)
                raw_score
            else if (raw_score == 0)
                state.score - state.score / 16
            else
                state.score - state.score / 8 + raw_score / 8;
            state.observations +|= 1;
        }
    }

    /// Convenience entry point for non-hot-path callers and tests.
    pub fn observeHbcCacheBenefit(self: *ResourceManager, samples: [@typeInfo(HbcCacheClass).@"enum".fields.len]HbcCacheBenefitSample) void {
        if (!self.beginHbcCacheBenefitSample()) return;
        self.observeHbcCacheBenefitSampled(samples);
    }

    pub fn denseReplayWindowBudget(self: *ResourceManager, options: DenseReplayWindowBudgetOptions) u64 {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const cap = self.denseReplayWindowHardCapLocked(options);
        var current = self.dense_replay_window_budget_bytes;
        if (current == 0) current = options.default_bytes;
        current = clampU64(current, options.min_bytes, cap);

        const memory_pressure = self.slicePressureLocked(.derived_replay_window) != .normal or
            self.slicePressureLocked(.dense_apply_working_set) != .normal or
            self.slicePressureLocked(.dense_routing_working_set) != .normal;
        const finish_too_slow = self.dense_replay_last_finish_ns > dense_replay_finish_hard_ns;
        const write_pressure_too_high = self.dense_replay_last_write_pressure_compactions > 0 and
            self.dense_replay_last_write_pressure_ns > dense_replay_write_pressure_hard_ns;

        if (memory_pressure or finish_too_slow or write_pressure_too_high) {
            current = current * dense_replay_window_shrink_numerator / dense_replay_window_shrink_denominator;
        } else if (self.dense_replay_last_finish_ns == 0 or self.dense_replay_last_finish_ns < dense_replay_finish_target_ns) {
            current = current * dense_replay_window_growth_numerator / dense_replay_window_growth_denominator;
        }

        current = clampU64(current, options.min_bytes, cap);
        self.dense_replay_window_budget_bytes = current;
        return current;
    }

    pub fn noteDenseReplayWindowResult(self: *ResourceManager, result: DenseReplayWindowResult) void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        self.dense_replay_last_finish_ns = result.finish_ns;
        self.dense_replay_last_write_pressure_ns = result.write_pressure_ns;
        self.dense_replay_last_write_pressure_compactions = result.write_pressure_compactions;
    }

    fn denseReplayWindowHardCapLocked(self: *ResourceManager, options: DenseReplayWindowBudgetOptions) u64 {
        var cap = @max(options.min_bytes, options.max_bytes);
        cap = @min(cap, self.sliceWindowCapLocked(.derived_replay_window, 1));
        cap = @min(cap, self.sliceWindowCapLocked(.dense_apply_working_set, 2));
        cap = @min(cap, self.sliceWindowCapLocked(.dense_routing_working_set, 2));
        return @max(options.min_bytes, cap);
    }

    fn sliceWindowCapLocked(self: *ResourceManager, slice: Slice, reserve_divisor: u64) u64 {
        const state = self.slices[sliceIndex(slice)];
        if (state.budget.hard_limit_bytes == 0) return std.math.maxInt(u64);
        const available = state.budget.hard_limit_bytes -| state.used_bytes;
        const reserved = if (reserve_divisor == 0) available else available / reserve_divisor;
        return @max(@as(u64, 1), reserved);
    }

    fn slicePressureLocked(self: *ResourceManager, slice: Slice) Pressure {
        const state = self.slices[sliceIndex(slice)];
        return pressureFor(state.budget, state.used_bytes);
    }
};

pub const CapacityReservation = struct {
    manager: *ResourceManager,
    domain_id: CapacityDomainId,
    bytes: u64,
    released: bool = false,

    pub fn grow(self: *CapacityReservation, additional_bytes: u64, observation: CapacityObservation, now_ns: u64) !void {
        if (self.released) return error.CapacityReservationReleased;
        try self.manager.growCapacityReservation(self.domain_id, additional_bytes, observation, now_ns);
        self.bytes = std.math.add(u64, self.bytes, additional_bytes) catch unreachable;
    }

    /// Reconcile a future-growth claim as planned bytes materialize on disk.
    /// Shrinking is admission-free because the filesystem observation already
    /// reflects materialized bytes; growing revalidates current capacity.
    pub fn resize(self: *CapacityReservation, target_bytes: u64, observation: CapacityObservation, now_ns: u64) !void {
        if (self.released) return error.CapacityReservationReleased;
        if (target_bytes == self.bytes) return;
        if (target_bytes > self.bytes) {
            return try self.grow(target_bytes - self.bytes, observation, now_ns);
        }
        self.manager.releaseCapacity(self.domain_id, self.bytes - target_bytes);
        self.bytes = target_bytes;
    }

    pub fn revalidate(self: *CapacityReservation, observation: CapacityObservation, now_ns: u64) !void {
        if (self.released) return error.CapacityReservationReleased;
        try self.manager.revalidateCapacityReservation(self.domain_id, observation, now_ns);
    }

    /// Checks whether the aggregate claim fits without recording a denial.
    /// Repair admission uses this before exact materialized-byte reconciliation;
    /// only the final post-reconciliation decision contributes denial metrics.
    pub fn fits(self: *CapacityReservation, observation: CapacityObservation, now_ns: u64) !bool {
        if (self.released) return error.CapacityReservationReleased;
        return try self.manager.capacityReservationFits(self.domain_id, observation, now_ns);
    }

    pub fn release(self: *CapacityReservation) void {
        if (self.released) return;
        self.manager.releaseCapacity(self.domain_id, self.bytes);
        self.released = true;
    }
};

fn hbcClockEntries(budget_bytes: u64, estimated_entry_bytes: u64) usize {
    const entries = @min(hbc_max_clock_entries, @max(@as(u64, 1), budget_bytes / estimated_entry_bytes));
    return @intCast(entries);
}

pub const Reservation = struct {
    manager: *ResourceManager,
    identity: u64,
    slice: Slice,
    bytes: u64,
    released: bool = false,

    pub fn release(self: *Reservation) void {
        if (self.released) return;
        _ = self.manager.releaseReservation(self);
        self.released = true;
    }

    pub fn growBoundedOversized(
        self: *Reservation,
        additional_bytes: u64,
        max_hard_limit_multiple: u64,
    ) !void {
        if (self.released) return error.ReservationReleased;
        try self.manager.growReservationBoundedOversized(
            self,
            additional_bytes,
            max_hard_limit_multiple,
        );
    }

    pub fn shrink(self: *Reservation, bytes: u64) void {
        if (self.released or bytes == 0) return;
        _ = self.manager.shrinkReservation(self, bytes);
    }
};

/// Accounts allocator-backed working sets before each allocation reaches the
/// backing allocator. One operation may make bounded progress above the normal
/// hard limit only while it is the slice's sole user.
pub const BudgetedAllocator = struct {
    backing: std.mem.Allocator,
    reservation: Reservation,
    max_hard_limit_multiple: u64,
    live_bytes: u64 = 0,
    credit_quantum: u64,
    budget_denied: bool = false,

    pub fn init(
        manager: *ResourceManager,
        slice: Slice,
        backing: std.mem.Allocator,
        max_hard_limit_multiple: u64,
    ) BudgetedAllocator {
        const stats = manager.sliceStats(slice);
        const credit_quantum = if (stats.hard_limit_bytes == 0)
            1024 * 1024
        else
            @max(@as(u64, 1), @min(@as(u64, 1024 * 1024), stats.hard_limit_bytes / 64));
        return .{
            .backing = backing,
            .reservation = .{
                .manager = manager,
                .identity = 0,
                .slice = slice,
                .bytes = 0,
            },
            .max_hard_limit_multiple = max_hard_limit_multiple,
            .credit_quantum = credit_quantum,
        };
    }

    pub fn deinit(self: *BudgetedAllocator) void {
        self.reservation.release();
        self.* = undefined;
    }

    pub fn allocator(self: *BudgetedAllocator) std.mem.Allocator {
        return .{
            .ptr = self,
            .vtable = &.{
                .alloc = alloc,
                .resize = resize,
                .remap = remap,
                .free = free,
            },
        };
    }

    pub fn denied(self: *const BudgetedAllocator) bool {
        return self.budget_denied;
    }

    fn reserveGrowth(self: *BudgetedAllocator, bytes: usize) bool {
        const amount = std.math.cast(u64, bytes) orelse {
            self.budget_denied = true;
            return false;
        };
        const next_live = std.math.add(u64, self.live_bytes, amount) catch {
            self.budget_denied = true;
            return false;
        };
        if (next_live > self.reservation.bytes) {
            const minimum = next_live - self.reservation.bytes;
            _ = self.reservation.manager.growReservationAmortized(
                &self.reservation,
                minimum,
                @max(minimum, self.credit_quantum),
                self.max_hard_limit_multiple,
            ) catch {
                self.budget_denied = true;
                return false;
            };
        }
        self.live_bytes = next_live;
        return true;
    }

    fn releaseBytes(self: *BudgetedAllocator, bytes: usize) void {
        const amount = std.math.cast(u64, bytes) orelse std.math.maxInt(u64);
        self.live_bytes -|= amount;
        if (self.live_bytes == 0) {
            self.reservation.shrink(self.reservation.bytes);
            return;
        }
        const spare = self.reservation.bytes -| self.live_bytes;
        if (spare < self.credit_quantum *| 2) return;
        const retained_spare = @min(self.credit_quantum, self.reservation.bytes);
        const target = self.live_bytes +| retained_spare;
        if (self.reservation.bytes > target)
            self.reservation.shrink(self.reservation.bytes - target);
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *BudgetedAllocator = @ptrCast(@alignCast(ctx));
        if (!self.reserveGrowth(len)) return null;
        return self.backing.rawAlloc(len, alignment, ret_addr) orelse {
            self.releaseBytes(len);
            return null;
        };
    }

    fn resize(
        ctx: *anyopaque,
        memory: []u8,
        alignment: std.mem.Alignment,
        new_len: usize,
        ret_addr: usize,
    ) bool {
        const self: *BudgetedAllocator = @ptrCast(@alignCast(ctx));
        const growth = new_len -| memory.len;
        if (growth > 0 and !self.reserveGrowth(growth)) return false;
        if (!self.backing.rawResize(memory, alignment, new_len, ret_addr)) {
            if (growth > 0) self.releaseBytes(growth);
            return false;
        }
        if (new_len < memory.len) self.releaseBytes(memory.len - new_len);
        return true;
    }

    fn remap(
        ctx: *anyopaque,
        memory: []u8,
        alignment: std.mem.Alignment,
        new_len: usize,
        ret_addr: usize,
    ) ?[*]u8 {
        const self: *BudgetedAllocator = @ptrCast(@alignCast(ctx));
        const growth = new_len -| memory.len;
        if (growth > 0 and !self.reserveGrowth(growth)) return null;
        const ptr = self.backing.rawRemap(memory, alignment, new_len, ret_addr) orelse {
            if (growth > 0) self.releaseBytes(growth);
            return null;
        };
        if (new_len < memory.len) self.releaseBytes(memory.len - new_len);
        return ptr;
    }

    fn free(
        ctx: *anyopaque,
        memory: []u8,
        alignment: std.mem.Alignment,
        ret_addr: usize,
    ) void {
        const self: *BudgetedAllocator = @ptrCast(@alignCast(ctx));
        self.backing.rawFree(memory, alignment, ret_addr);
        self.releaseBytes(memory.len);
    }
};

test "default tokenizer cache budget is aligned with its resource slice" {
    const budgets = Options.defaultBudgets();
    const policies = Options.defaultPolicies();
    const tokenizer_idx = @intFromEnum(Slice.inference_tokenizer_cache);
    try std.testing.expectEqual(
        @as(u64, 64 * 1024 * 1024),
        budgets[tokenizer_idx].soft_limit_bytes,
    );
    try std.testing.expectEqual(
        @as(u64, 128 * 1024 * 1024),
        budgets[tokenizer_idx].hard_limit_bytes,
    );
    try std.testing.expectEqual(
        PressureAction.shrink_cache,
        policies[tokenizer_idx].soft_action,
    );
    try std.testing.expectEqual(
        PressureAction.shrink_cache,
        policies[tokenizer_idx].hard_action,
    );
}

test "identity allocation failure rolls back every memory ledger" {
    var identity_storage: [1]u8 = undefined;
    var identity_fba = std.heap.FixedBufferAllocator.init(&identity_storage);
    var manager = ResourceManager.init(.{
        .identity_allocator = identity_fba.allocator(),
    });
    defer manager.deinit(std.testing.allocator);

    try std.testing.expectError(
        error.OutOfMemory,
        manager.reserve(.inference_tokenizer_cache, 10),
    );
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    try std.testing.expectError(
        error.ResourceTemporarilyUnavailable,
        manager.reserveBatchClassified(&.{.{
            .slice = .inference_model_residency,
            .bytes = 10,
        }}),
    );
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);

    var observed: u64 = 0;
    try std.testing.expectError(
        error.OutOfMemory,
        manager.adjustUsage(.inference_prompt_cache, &observed, 10),
    );
    try std.testing.expectEqual(@as(u64, 0), observed);
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
}

test "manager teardown retires live observer snapshots" {
    var manager = ResourceManager.init(.{
        .identity_allocator = std.testing.allocator,
    });
    var observed: u64 = 0;
    try std.testing.expect(manager.tryObserveUsage(
        .inference_prompt_cache,
        &observed,
        4096,
    ));
    try std.testing.expectEqual(@as(u64, 4096), manager.snapshot().memory.used_bytes);

    // The observed allocation and manager share this shutdown boundary. The
    // manager owns and frees the identity registry even when no final zero
    // observation is useful to a caller.
    manager.deinit(std.testing.allocator);
}

fn sliceIndex(slice: Slice) usize {
    return @intFromEnum(slice);
}

fn pressureFor(budget: Budget, used_bytes: u64) Pressure {
    if (budget.hard_limit_bytes > 0 and used_bytes > budget.hard_limit_bytes) return .hard;
    if (budget.soft_limit_bytes > 0 and used_bytes > budget.soft_limit_bytes) return .soft;
    return .normal;
}

fn sliceStatsFromState(slice: Slice, state: MutableSlice) SliceStats {
    return .{
        .name = slice.name(),
        .used_bytes = state.used_bytes,
        .peak_bytes = state.peak_bytes,
        .soft_limit_bytes = state.budget.soft_limit_bytes,
        .hard_limit_bytes = state.budget.hard_limit_bytes,
        .soft_limit_events = state.soft_limit_events,
        .hard_limit_rejections = state.hard_limit_rejections,
        .oversized_single_grants = state.oversized_single_grants,
        .pressure = pressureFor(state.budget, state.used_bytes),
        .soft_action = state.policy.soft_action,
        .hard_action = state.policy.hard_action,
    };
}

fn clampU64(value: u64, min_value: u64, max_value: u64) u64 {
    if (max_value <= min_value) return min_value;
    return @min(@max(value, min_value), max_value);
}

fn mulDivSaturating(value: u64, numerator: u64, denominator: u64) u64 {
    if (value == 0 or numerator == 0 or denominator == 0) return 0;
    const product = @as(u128, value) * @as(u128, numerator);
    return @intCast(@min(@as(u128, std.math.maxInt(u64)), product / denominator));
}

fn cacheBenefitPerByte(sample: HbcCacheBenefitSample, miss_service_ns_per_miss: u64) u64 {
    if (sample.hits == 0 or miss_service_ns_per_miss == 0 or sample.resident_bytes == 0) return 0;
    const avoided_ns = @as(u128, sample.hits) * @as(u128, miss_service_ns_per_miss);
    const resident_kib = @max(@as(u64, 1), sample.resident_bytes / 1024);
    return @intCast(@min(@as(u128, std.math.maxInt(u64)), avoided_ns / resident_kib));
}

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) {
        if (comptime builtin.os.tag == .freestanding) {
            std.atomic.spinLoopHint();
            continue;
        }
        std.Thread.yield() catch {};
    }
}

test "resource manager tracks reservations and releases" {
    var manager = ResourceManager.init(.{});

    var reservation = try manager.reserve(.full_text_pending_segments, 4096);
    var stats = manager.snapshot();
    try std.testing.expectEqual(@as(u64, 4096), stats.slices[sliceIndex(.full_text_pending_segments)].used_bytes);
    try std.testing.expectEqual(@as(u64, 4096), stats.slices[sliceIndex(.full_text_pending_segments)].peak_bytes);

    reservation.release();
    stats = manager.snapshot();
    try std.testing.expectEqual(@as(u64, 0), stats.slices[sliceIndex(.full_text_pending_segments)].used_bytes);
}

test "batch reservation is atomic across inference resource slices" {
    var budgets = Options.defaultBudgets();
    budgets[sliceIndex(.inference_model_residency)] = .{ .hard_limit_bytes = 100 };
    budgets[sliceIndex(.inference_kv_working_set)] = .{ .hard_limit_bytes = 20 };
    var manager = ResourceManager.init(.{ .budgets = budgets });

    try std.testing.expectError(
        error.ResourceBudgetExceeded,
        manager.reserveBatch(&.{
            .{ .slice = .inference_model_residency, .bytes = 80 },
            .{ .slice = .inference_kv_working_set, .bytes = 21 },
        }),
    );
    try std.testing.expectEqual(
        @as(u64, 0),
        manager.sliceStats(.inference_model_residency).used_bytes,
    );

    const admitted = [_]SliceAmount{
        .{ .slice = .inference_model_residency, .bytes = 80 },
        .{ .slice = .inference_kv_working_set, .bytes = 20 },
    };
    var reservation = try manager.reserveBatch(&admitted);
    try std.testing.expectEqual(
        @as(u64, 80),
        manager.sliceStats(.inference_model_residency).used_bytes,
    );
    reservation.release();
    try std.testing.expectEqual(
        @as(u64, 0),
        manager.sliceStats(.inference_model_residency).used_bytes,
    );
}

test "classified batch reservation distinguishes size from contention" {
    var budgets = Options.defaultBudgets();
    budgets[sliceIndex(.inference_model_residency)] = .{ .hard_limit_bytes = 100 };
    var manager = ResourceManager.init(.{ .budgets = budgets });

    try std.testing.expectError(
        error.ResourceRequestTooLarge,
        manager.reserveBatchClassified(&.{
            .{ .slice = .inference_model_residency, .bytes = 101 },
        }),
    );

    const admitted = [_]SliceAmount{
        .{ .slice = .inference_model_residency, .bytes = 80 },
    };
    var reservation = try manager.reserveBatchClassified(&admitted);
    try std.testing.expectError(
        error.ResourceTemporarilyUnavailable,
        manager.reserveBatchClassified(&.{
            .{ .slice = .inference_model_residency, .bytes = 21 },
        }),
    );
    try std.testing.expectEqual(
        @as(u64, 80),
        manager.sliceStats(.inference_model_residency).used_bytes,
    );

    reservation.release();
    const after_release = [_]SliceAmount{
        .{ .slice = .inference_model_residency, .bytes = 21 },
    };
    var after_release_reservation = try manager.reserveBatchClassified(&after_release);
    after_release_reservation.release();
}

test "aggregate host memory admission is atomic across slices" {
    var manager = ResourceManager.init(.{
        .memory_budget = .{ .soft_limit_bytes = 75, .hard_limit_bytes = 100 },
    });

    var first = try manager.reserve(.full_text_build_working_set, 70);
    defer first.release();
    try std.testing.expectError(
        error.ResourceTemporarilyUnavailable,
        manager.reserveBatchClassified(&.{
            .{ .slice = .lsm_compaction_work, .bytes = 20 },
            .{ .slice = .dense_apply_working_set, .bytes = 11 },
        }),
    );
    const stats = manager.snapshot();
    try std.testing.expectEqual(@as(u64, 70), stats.memory.used_bytes);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_compaction_work).used_bytes);
    try std.testing.expectEqual(@as(u64, 1), stats.memory.hard_limit_rejections);
}

test "logical inference slices can charge only physical host memory" {
    var manager = ResourceManager.init(.{
        .memory_budget = .{ .hard_limit_bytes = 100 },
    });
    const logical = [_]SliceAmount{
        .{ .slice = .inference_model_residency, .bytes = 240 },
        .{ .slice = .inference_kv_working_set, .bytes = 60 },
    };
    var reservation = try manager.reserveBatchClassifiedWithHostCharge(&logical, 80);
    try std.testing.expectEqual(@as(u64, 300), manager.sliceStats(.inference_model_residency).used_bytes +
        manager.sliceStats(.inference_kv_working_set).used_bytes);
    try std.testing.expectEqual(@as(u64, 80), manager.snapshot().memory.used_bytes);
    reservation.release();
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
}

test "batch release accounting errors fail closed" {
    var manager = ResourceManager.init(.{});
    const admitted = [_]SliceAmount{
        .{ .slice = .inference_model_residency, .bytes = 20 },
        .{ .slice = .inference_kv_working_set, .bytes = 10 },
    };
    var first = try manager.reserveBatchClassifiedWithHostCharge(&admitted, 15);
    var second = try manager.reserveBatchClassifiedWithHostCharge(&admitted, 15);
    var first_copy = first;

    first.release();
    first.release();
    first_copy.release();
    var stats = manager.snapshot();
    try std.testing.expectEqual(@as(u64, 15), stats.memory.used_bytes);
    try std.testing.expectEqual(@as(u64, 20), manager.sliceStats(.inference_model_residency).used_bytes);
    try std.testing.expectEqual(@as(u64, 1), stats.memory.accounting_errors);

    try std.testing.expectError(
        error.InvalidReservationReduction,
        second.retain(&.{.{ .slice = .inference_model_residency, .bytes = 21 }}, 15),
    );
    try std.testing.expectError(
        error.DuplicateResourceSlice,
        second.retain(&.{
            .{ .slice = .inference_model_residency, .bytes = 10 },
            .{ .slice = .inference_model_residency, .bytes = 10 },
        }, 15),
    );
    try std.testing.expectEqual(@as(u64, 15), manager.snapshot().memory.used_bytes);

    // Simulate a damaged aggregate ledger. Release must retain every slice
    // reservation and make the mismatch observable rather than saturating.
    manager.memory.used_bytes = 14;
    second.release();
    stats = manager.snapshot();
    try std.testing.expectEqual(@as(u64, 14), stats.memory.used_bytes);
    try std.testing.expectEqual(@as(u64, 20), manager.sliceStats(.inference_model_residency).used_bytes);
    try std.testing.expectEqual(@as(u64, 4), stats.memory.accounting_errors);
}

test "single release and observer mismatch cannot debit unrelated memory" {
    var manager = ResourceManager.init(.{});
    var first = try manager.reserve(.inference_tokenizer_cache, 10);
    var unrelated = try manager.reserve(.inference_model_residency, 20);

    var corrupt_copy = first;
    corrupt_copy.bytes = 11;
    corrupt_copy.release();
    var stats = manager.snapshot();
    try std.testing.expectEqual(@as(u64, 30), stats.memory.used_bytes);
    try std.testing.expectEqual(@as(u64, 10), manager.sliceStats(.inference_tokenizer_cache).used_bytes);

    var stale_observation: u64 = 11;
    try std.testing.expect(!manager.tryObserveUsage(
        .inference_tokenizer_cache,
        &stale_observation,
        0,
    ));
    stats = manager.snapshot();
    try std.testing.expectEqual(@as(u64, 30), stats.memory.used_bytes);
    try std.testing.expectEqual(@as(u64, 11), stale_observation);
    try std.testing.expectEqual(@as(u64, 2), stats.memory.accounting_errors);

    var observed_first: u64 = 0;
    var observed_second: u64 = 0;
    try std.testing.expect(manager.tryObserveUsage(
        .inference_prompt_cache,
        &observed_first,
        10,
    ));
    try std.testing.expect(manager.tryObserveUsage(
        .inference_prompt_cache,
        &observed_second,
        20,
    ));
    observed_first = 0;
    try std.testing.expect(!manager.tryObserveUsage(
        .inference_prompt_cache,
        &observed_first,
        0,
    ));
    try std.testing.expectEqual(
        @as(u64, 30),
        manager.sliceStats(.inference_prompt_cache).used_bytes,
    );
    observed_first = 10;
    try std.testing.expect(manager.tryObserveUsage(.inference_prompt_cache, &observed_first, 0));
    try std.testing.expect(manager.tryObserveUsage(.inference_prompt_cache, &observed_second, 0));

    first.release();
    unrelated.release();
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
}

test "bounded oversized progress cannot bypass aggregate host memory" {
    var budgets = Options.defaultBudgets();
    budgets[sliceIndex(.text_merge_buffers)] = .{ .hard_limit_bytes = 10 };
    var manager = ResourceManager.init(.{
        .memory_budget = .{ .hard_limit_bytes = 15 },
        .budgets = budgets,
    });
    try std.testing.expectError(
        error.ResourceBudgetExceeded,
        manager.reserveBoundedOversizedSingle(.text_merge_buffers, 18, 2),
    );
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
}

test "resource manager coordinates growable capacity by physical domain" {
    var manager = ResourceManager.init(.{
        .disk_safety_floor_bytes = 10,
        .disk_safety_floor_divisor = 0,
    });
    defer manager.deinit(std.testing.allocator);
    const observation = CapacityObservation{
        .available_bytes = 100,
        .capacity_bytes = 100,
        .observed_at_ns = 100,
        .valid_for_ns = 50,
    };

    var first = try manager.reserveCapacity(std.testing.allocator, 7, 60, observation, 100);
    defer first.release();
    try std.testing.expectError(error.CapacityUnavailable, manager.reserveCapacity(std.testing.allocator, 7, 31, observation, 100));
    var independent = try manager.reserveCapacity(std.testing.allocator, 8, 80, observation, 100);
    independent.release();

    try first.grow(20, observation, 100);
    try std.testing.expectError(error.CapacityUnavailable, first.grow(11, observation, 100));
    try first.resize(50, observation, 100);
    const stats = manager.capacityStats();
    try std.testing.expectEqual(@as(u64, 50), stats.reserved_bytes);
    try std.testing.expectEqual(@as(u64, 2), stats.denials);
    try std.testing.expectEqual(@as(u64, 1), stats.growth_denials);
    try std.testing.expectEqual(@as(usize, 2), stats.domain_count);
}

test "capacity reservation revalidation fails closed when available space falls" {
    var manager = ResourceManager.init(.{
        .disk_safety_floor_bytes = 10,
        .disk_safety_floor_divisor = 0,
    });
    defer manager.deinit(std.testing.allocator);

    var reservation = try manager.reserveCapacity(std.testing.allocator, 7, 60, .{
        .available_bytes = 100,
        .capacity_bytes = 100,
        .observed_at_ns = 100,
        .valid_for_ns = 100,
    }, 100);
    defer reservation.release();

    try reservation.revalidate(.{
        .available_bytes = 75,
        .capacity_bytes = 100,
        .observed_at_ns = 110,
        .valid_for_ns = 100,
    }, 110);
    try std.testing.expect(!try reservation.fits(.{
        .available_bytes = 65,
        .capacity_bytes = 100,
        .observed_at_ns = 120,
        .valid_for_ns = 100,
    }, 120));
    try std.testing.expectEqual(@as(u64, 0), manager.capacityStats().denials);
    try std.testing.expectError(error.CapacityUnavailable, reservation.revalidate(.{
        .available_bytes = 65,
        .capacity_bytes = 100,
        .observed_at_ns = 120,
        .valid_for_ns = 100,
    }, 120));

    const stats = manager.capacityStats();
    try std.testing.expectEqual(@as(u64, 60), stats.reserved_bytes);
    try std.testing.expectEqual(@as(u64, 1), stats.denials);
    try std.testing.expectEqual(@as(u64, 1), stats.growth_denials);
}

test "resource manager rejects stale known capacity but accounts unknown capacity" {
    var manager = ResourceManager.init(.{ .disk_safety_floor_bytes = 0 });
    defer manager.deinit(std.testing.allocator);
    try std.testing.expectError(error.CapacityObservationStale, manager.reserveCapacity(
        std.testing.allocator,
        1,
        1,
        .{ .available_bytes = 100, .observed_at_ns = 10, .valid_for_ns = 5 },
        16,
    ));
    var accounting_only = try manager.reserveCapacity(std.testing.allocator, 1, 1000, .{}, 16);
    accounting_only.release();
    try std.testing.expectEqual(@as(u64, 1), manager.capacityStats().stale_observations);
}

test "resource manager capacity source is immutable after composition" {
    const Source = struct {
        fn observe(_: *anyopaque) anyerror!CapacityObservation {
            return .{};
        }
    };
    var first_context: u8 = 0;
    var second_context: u8 = 0;
    const first = CapacitySource{ .ptr = &first_context, .domain_id = 7, .observe = Source.observe };
    const second = CapacitySource{ .ptr = &second_context, .domain_id = 8, .observe = Source.observe };

    var manager = ResourceManager.init(.{});
    defer manager.deinit(std.testing.allocator);
    try manager.installCapacitySource(first);
    try manager.installCapacitySource(first);
    try std.testing.expectError(error.CapacitySourceAlreadyInstalled, manager.installCapacitySource(second));

    var admitted = ResourceManager.init(.{ .disk_safety_floor_bytes = 0 });
    defer admitted.deinit(std.testing.allocator);
    var reservation = try admitted.reserveCapacity(std.testing.allocator, 1, 1, .{}, 0);
    reservation.release();
    try std.testing.expectError(error.CapacitySourceInstalledAfterAdmission, admitted.installCapacitySource(first));
}

test "resource manager tracks full text build working set independently" {
    var manager = ResourceManager.init(.{});
    var current: u64 = 0;

    try manager.adjustUsage(.full_text_build_working_set, &current, 8192);
    try std.testing.expectEqual(@as(u64, 8192), current);

    const build_stats = manager.sliceStats(.full_text_build_working_set);
    const pending_stats = manager.sliceStats(.full_text_pending_segments);
    try std.testing.expectEqual(@as(u64, 8192), build_stats.used_bytes);
    try std.testing.expectEqual(@as(u64, 0), pending_stats.used_bytes);

    try manager.adjustUsage(.full_text_build_working_set, &current, 0);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.full_text_build_working_set).used_bytes);
}

test "resource manager tracks inference prompt cache usage" {
    var manager = ResourceManager.init(.{});
    var current: u64 = 0;

    manager.observeUsage(.inference_prompt_cache, &current, 8192);
    try std.testing.expectEqual(@as(u64, 8192), manager.sliceStats(.inference_prompt_cache).used_bytes);

    manager.observeUsage(.inference_prompt_cache, &current, 0);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.inference_prompt_cache).used_bytes);
}

test "resource manager records soft and hard budget pressure" {
    var budgets = Options.defaultBudgets();
    budgets[sliceIndex(.derived_backlog)] = .{
        .soft_limit_bytes = 10,
        .hard_limit_bytes = 20,
    };
    var manager = ResourceManager.init(.{ .budgets = budgets });

    var first = try manager.reserve(.derived_backlog, 12);
    defer first.release();
    var stats = manager.snapshot();
    try std.testing.expectEqual(@as(u64, 1), stats.slices[sliceIndex(.derived_backlog)].soft_limit_events);

    try std.testing.expectError(error.ResourceBudgetExceeded, manager.reserve(.derived_backlog, 9));
    stats = manager.snapshot();
    try std.testing.expectEqual(@as(u64, 1), stats.slices[sliceIndex(.derived_backlog)].hard_limit_rejections);
}

test "latency-sensitive derived replay defers only optional background work" {
    var manager = ResourceManager.init(.{});
    defer manager.deinit(std.testing.allocator);

    try std.testing.expect(!manager.shouldDeferSoftCompactionForDerivedReplay());
    manager.beginLatencySensitiveDerivedReplay();
    manager.beginLatencySensitiveDerivedReplay();
    try std.testing.expect(manager.shouldDeferSoftCompactionForDerivedReplay());
    manager.finishLatencySensitiveDerivedReplay();
    try std.testing.expect(manager.shouldDeferSoftCompactionForDerivedReplay());
    manager.finishLatencySensitiveDerivedReplay();
    try std.testing.expect(manager.shouldDeferSoftCompactionForDerivedReplay());
    manager.latency_sensitive_derived_replay_quiet_until_ns.store(0, .release);
    try std.testing.expect(!manager.shouldDeferSoftCompactionForDerivedReplay());
}

test "resource manager background deferral follows slice policy" {
    var budgets = Options.defaultBudgets();
    var policies = Options.defaultPolicies();
    budgets[sliceIndex(.lsm_compaction_work)] = .{
        .soft_limit_bytes = 10,
        .hard_limit_bytes = 20,
    };
    policies[sliceIndex(.lsm_compaction_work)] = .{
        .soft_action = .defer_background_work,
        .hard_action = .reject_work,
    };
    var manager = ResourceManager.init(.{ .budgets = budgets, .policies = policies });

    var observed: u64 = 0;
    try std.testing.expect(!manager.shouldDeferBackgroundWork(.lsm_compaction_work));
    manager.observeUsage(.lsm_compaction_work, &observed, 11);
    try std.testing.expect(manager.shouldDeferBackgroundWork(.lsm_compaction_work));
    manager.observeUsage(.lsm_compaction_work, &observed, 21);
    try std.testing.expect(manager.shouldDeferBackgroundWork(.lsm_compaction_work));
    manager.observeUsage(.lsm_compaction_work, &observed, 0);
    try std.testing.expect(!manager.shouldDeferBackgroundWork(.lsm_compaction_work));
}

test "resource manager bounds and serializes oversized minimum progress" {
    var budgets = Options.defaultBudgets();
    budgets[sliceIndex(.text_merge_buffers)] = .{
        .soft_limit_bytes = 8,
        .hard_limit_bytes = 10,
    };
    var manager = ResourceManager.init(.{ .budgets = budgets });

    try std.testing.expectError(
        error.ResourceBudgetExceeded,
        manager.reserve(.text_merge_buffers, 18),
    );

    var oversized = try manager.reserveBoundedOversizedSingle(.text_merge_buffers, 18, 2);
    var stats = manager.sliceStats(.text_merge_buffers);
    try std.testing.expectEqual(@as(u64, 18), stats.used_bytes);
    try std.testing.expectEqual(@as(u64, 18), stats.peak_bytes);
    try std.testing.expectEqual(@as(u64, 1), stats.oversized_single_grants);
    try std.testing.expectEqual(Pressure.hard, stats.pressure);

    // A second job cannot join the slice while its oversized grant is active.
    try std.testing.expectError(
        error.ResourceBudgetExceeded,
        manager.reserveBoundedOversizedSingle(.text_merge_buffers, 1, 2),
    );
    oversized.release();
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.text_merge_buffers).used_bytes);

    // The exception is bounded; it cannot silently turn the slice unlimited.
    try std.testing.expectError(
        error.ResourceBudgetExceeded,
        manager.reserveBoundedOversizedSingle(.text_merge_buffers, 21, 2),
    );
    stats = manager.sliceStats(.text_merge_buffers);
    try std.testing.expectEqual(@as(u64, 1), stats.oversized_single_grants);
    try std.testing.expectEqual(@as(u64, 3), stats.hard_limit_rejections);
}

test "resource manager owns dense repair replay pressure policy" {
    var budgets = Options.defaultBudgets();
    budgets[sliceIndex(.lsm_wal_retention)] = .{ .hard_limit_bytes = 10 };
    budgets[sliceIndex(.derived_backlog)] = .{ .hard_limit_bytes = 20 };
    var manager = ResourceManager.init(.{ .budgets = budgets });

    var wal_bytes: u64 = 0;
    var backlog_bytes: u64 = 0;
    manager.observeUsage(.lsm_wal_retention, &wal_bytes, 10);
    manager.observeUsage(.derived_backlog, &backlog_bytes, 20);
    try std.testing.expect(!manager.denseRepairReplayPressureIsHard());

    manager.observeUsage(.derived_backlog, &backlog_bytes, 21);
    try std.testing.expect(manager.denseRepairReplayPressureIsHard());
    manager.observeUsage(.derived_backlog, &backlog_bytes, 0);
    manager.observeUsage(.lsm_wal_retention, &wal_bytes, 11);
    try std.testing.expect(manager.denseRepairReplayPressureIsHard());
}

test "resource manager owns HBC cache ceilings" {
    var budgets = Options.defaultBudgets();
    budgets[sliceIndex(.hbc_node_metadata_cache)] = .{
        .soft_limit_bytes = 2048,
        .hard_limit_bytes = 4096,
    };
    var manager = ResourceManager.init(.{ .budgets = budgets });

    const local = manager.hbcCacheLimits(128, false);
    try std.testing.expectEqual(@as(usize, 4), local.max_cached_nodes);
    try std.testing.expectEqual(@as(usize, 7), local.max_cached_vectors);
    try std.testing.expectEqual(@as(usize, 16), local.max_cached_metadata);

    const shared = manager.hbcCacheLimits(128, true);
    try std.testing.expectEqual(@as(usize, 1), shared.max_cached_nodes);
    try std.testing.expectEqual(@as(usize, 1), shared.max_cached_vectors);
    try std.testing.expectEqual(@as(usize, 1), shared.max_cached_metadata);
}

test "resource manager derives elastic HBC cache-class policy from pressure" {
    var budgets = Options.defaultBudgets();
    budgets[sliceIndex(.hbc_node_metadata_cache)] = .{
        .soft_limit_bytes = 800,
        .hard_limit_bytes = 1000,
    };
    var manager = ResourceManager.init(.{ .budgets = budgets });
    defer manager.deinit(std.testing.allocator);

    var policy = manager.hbcCachePolicy();
    try std.testing.expectEqual(@as(u64, 800), policy.target_bytes);
    try std.testing.expectEqual(@as(u64, 100), policy.protectedBytes(.node));
    try std.testing.expectEqual(@as(u64, 200), policy.protectedBytes(.quantized));
    try std.testing.expectEqual(@as(u64, 25), policy.protectedBytes(.metadata));
    try std.testing.expectEqual(@as(u64, 0), policy.protectedBytes(.vector));
    try std.testing.expectEqual(@as(u32, 1), policy.concurrent_vector_admission_stride);

    var observed: u64 = 0;
    manager.observeUsage(.hbc_node_metadata_cache, &observed, 900);
    policy = manager.hbcCachePolicy();
    try std.testing.expectEqual(@as(u32, 8), policy.concurrent_vector_admission_stride);

    manager.observeUsage(.hbc_node_metadata_cache, &observed, 1001);
    policy = manager.hbcCachePolicy();
    try std.testing.expectEqual(@as(u32, 0), policy.concurrent_vector_admission_stride);
    manager.observeUsage(.hbc_node_metadata_cache, &observed, 0);
}

test "resource manager bounds adaptive HBC benefit-per-byte targets" {
    var budgets = Options.defaultBudgets();
    budgets[sliceIndex(.hbc_node_metadata_cache)] = .{
        .soft_limit_bytes = 8000,
        .hard_limit_bytes = 10_000,
    };
    var manager = ResourceManager.init(.{ .budgets = budgets });
    defer manager.deinit(std.testing.allocator);

    manager.observeHbcCacheBenefit(.{
        .{ .hits = 100, .misses = 10, .miss_service_ns = 100_000, .resident_bytes = 1024 },
        .{ .hits = 25, .misses = 5, .miss_service_ns = 25_000, .resident_bytes = 1024 },
        .{ .hits = 200, .misses = 20, .miss_service_ns = 400_000, .resident_bytes = 4096 },
        .{},
    });
    const policy = manager.hbcCachePolicy();
    try std.testing.expect(policy.adaptive);
    try std.testing.expect(policy.protectedBytes(.node) > 8000 / 8);
    try std.testing.expect(policy.protectedBytes(.vector) > 0);
    try std.testing.expect(policy.protectedBytes(.node) <= 8000 / 3);
    try std.testing.expect(policy.protectedBytes(.quantized) <= 8000 / 2);
    try std.testing.expect(policy.protectedBytes(.vector) <= 8000 / 2);
    try std.testing.expect(policy.protectedBytes(.metadata) <= 8000 / 8);
}

test "adaptive HBC benefit retains miss cost through all-hit samples" {
    var manager = ResourceManager.init(.{});
    defer manager.deinit(std.testing.allocator);
    manager.observeHbcCacheBenefitSampled(.{
        .{},
        .{},
        .{ .hits = 8, .misses = 8, .miss_service_ns = 80_000, .resident_bytes = 4096 },
        .{},
    });
    const before = manager.hbc_cache_benefit[@intFromEnum(HbcCacheClass.vector)].score;
    try std.testing.expect(before > 0);

    manager.observeHbcCacheBenefitSampled(.{
        .{},
        .{},
        .{ .hits = 16, .resident_bytes = 4096 },
        .{},
    });
    const after = manager.hbc_cache_benefit[@intFromEnum(HbcCacheClass.vector)].score;
    try std.testing.expect(after >= before);
}

test "foreground admission reclaims cache bytes and retries atomically" {
    const ReclaimContext = struct {
        manager: *ResourceManager,
        accounted: u64,

        fn reclaim(raw: *anyopaque, target: u64) u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const released = @min(target, self.accounted);
            self.manager.observeUsage(.hbc_node_metadata_cache, &self.accounted, self.accounted - released);
            return released;
        }
    };

    var budgets = Options.defaultBudgets();
    budgets[sliceIndex(.hbc_node_metadata_cache)] = .{ .soft_limit_bytes = 80, .hard_limit_bytes = 100 };
    budgets[sliceIndex(.dense_apply_working_set)] = .{ .soft_limit_bytes = 80, .hard_limit_bytes = 100 };
    var manager = ResourceManager.init(.{
        .memory_budget = .{ .soft_limit_bytes = 90, .hard_limit_bytes = 100 },
        .budgets = budgets,
    });
    defer manager.deinit(std.testing.allocator);

    var context = ReclaimContext{ .manager = &manager, .accounted = 0 };
    manager.observeUsage(.hbc_node_metadata_cache, &context.accounted, 80);
    const identity = try manager.registerReclaimer(.hbc_node_metadata_cache, &context, ReclaimContext.reclaim);
    defer manager.unregisterReclaimer(identity);

    var foreground = try manager.reserve(.dense_apply_working_set, 30);
    defer foreground.release();
    try std.testing.expectEqual(@as(u64, 70), context.accounted);
    const stats = manager.snapshot();
    try std.testing.expectEqual(@as(u64, 100), stats.memory.used_bytes);
    try std.testing.expectEqual(@as(u64, 1), stats.reclaim_requests);
    try std.testing.expectEqual(@as(u64, 10), stats.reclaimed_bytes);
}

test "resource manager apportions reclaim across weighted cache owners" {
    const ReclaimContext = struct {
        manager: *ResourceManager,
        accounted: u64,
        requested: u64 = 0,

        fn reclaim(raw: *anyopaque, target: u64) u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.requested = target;
            const released = @min(target, self.accounted);
            self.manager.observeUsage(.hbc_node_metadata_cache, &self.accounted, self.accounted - released);
            return released;
        }
    };

    var manager = ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 300 } });
    defer manager.deinit(std.testing.allocator);
    var first = ReclaimContext{ .manager = &manager, .accounted = 0 };
    var second = ReclaimContext{ .manager = &manager, .accounted = 0 };
    manager.observeUsage(.hbc_node_metadata_cache, &first.accounted, 75);
    manager.observeUsage(.hbc_node_metadata_cache, &second.accounted, 225);
    const first_id = try manager.registerReclaimerWithOptions(.hbc_node_metadata_cache, &first, ReclaimContext.reclaim, .{ .weight = 1 });
    defer manager.unregisterReclaimer(first_id);
    const second_id = try manager.registerReclaimerWithOptions(.hbc_node_metadata_cache, &second, ReclaimContext.reclaim, .{ .weight = 3 });
    defer manager.unregisterReclaimer(second_id);

    try std.testing.expectEqual(@as(u64, 120), manager.reclaimForAllocation(.dense_apply_working_set, 120));
    try std.testing.expectEqual(@as(u64, 30), first.requested);
    try std.testing.expectEqual(@as(u64, 90), second.requested);
}

test "resource manager invokes reclaimers without holding registry mutex" {
    const PassiveContext = struct {
        calls: u64 = 0,

        fn reclaim(raw: *anyopaque, target: u64) u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return target;
        }
    };
    const RetiringContext = struct {
        manager: *ResourceManager,
        retire_identity: u64 = 0,
        calls: u64 = 0,

        fn reclaim(raw: *anyopaque, target: u64) u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            self.manager.unregisterReclaimer(self.retire_identity);
            return target;
        }
    };

    var manager = ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 100 } });
    defer manager.deinit(std.testing.allocator);
    var retiring = RetiringContext{ .manager = &manager };
    var passive = PassiveContext{};
    const retiring_id = try manager.registerReclaimer(.hbc_node_metadata_cache, &retiring, RetiringContext.reclaim);
    defer manager.unregisterReclaimer(retiring_id);
    retiring.retire_identity = try manager.registerReclaimer(.hbc_node_metadata_cache, &passive, PassiveContext.reclaim);

    var accounted: u64 = 0;
    manager.observeUsage(.hbc_node_metadata_cache, &accounted, 100);
    try std.testing.expectEqual(@as(u64, 5), manager.reclaimForAllocation(.dense_apply_working_set, 10));
    try std.testing.expectEqual(@as(u64, 1), retiring.calls);
    try std.testing.expectEqual(@as(u64, 0), passive.calls);
}

test "classified batch chooses foreground requester when cache slice is first" {
    const ReclaimContext = struct {
        manager: *ResourceManager,
        accounted: u64,

        fn reclaim(raw: *anyopaque, target: u64) u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const released = @min(target, self.accounted);
            self.manager.observeUsage(.hbc_node_metadata_cache, &self.accounted, self.accounted - released);
            return released;
        }
    };

    var manager = ResourceManager.init(.{
        .memory_budget = .{ .soft_limit_bytes = 90, .hard_limit_bytes = 100 },
    });
    defer manager.deinit(std.testing.allocator);

    var context = ReclaimContext{ .manager = &manager, .accounted = 0 };
    manager.observeUsage(.hbc_node_metadata_cache, &context.accounted, 80);
    const identity = try manager.registerReclaimer(.hbc_node_metadata_cache, &context, ReclaimContext.reclaim);
    defer manager.unregisterReclaimer(identity);

    var foreground = try manager.reserveBatchClassified(&.{
        .{ .slice = .hbc_node_metadata_cache, .bytes = 1 },
        .{ .slice = .dense_apply_working_set, .bytes = 29 },
    });
    defer foreground.release();
    try std.testing.expectEqual(@as(u64, 70), context.accounted);
    try std.testing.expectEqual(@as(u64, 100), manager.snapshot().memory.used_bytes);
}

test "resource manager adjusts tracked usage" {
    var budgets = Options.defaultBudgets();
    budgets[sliceIndex(.text_merge_buffers)] = .{
        .soft_limit_bytes = 10,
        .hard_limit_bytes = 20,
    };
    var manager = ResourceManager.init(.{ .budgets = budgets });
    var current: u64 = 0;

    try manager.adjustUsage(.text_merge_buffers, &current, 12);
    var stats = manager.snapshot();
    try std.testing.expectEqual(@as(u64, 12), current);
    try std.testing.expectEqual(@as(u64, 12), stats.slices[sliceIndex(.text_merge_buffers)].used_bytes);
    try std.testing.expectEqual(@as(u64, 1), stats.slices[sliceIndex(.text_merge_buffers)].soft_limit_events);
    try std.testing.expectEqual(Pressure.soft, stats.slices[sliceIndex(.text_merge_buffers)].pressure);
    try std.testing.expectEqual(PressureAction.defer_background_work, stats.slices[sliceIndex(.text_merge_buffers)].soft_action);
    try std.testing.expectEqual(PressureAction.reject_work, stats.slices[sliceIndex(.text_merge_buffers)].hard_action);

    try std.testing.expectError(error.ResourceBudgetExceeded, manager.adjustUsage(.text_merge_buffers, &current, 21));
    stats = manager.snapshot();
    try std.testing.expectEqual(@as(u64, 12), current);
    try std.testing.expectEqual(@as(u64, 12), stats.slices[sliceIndex(.text_merge_buffers)].used_bytes);
    try std.testing.expectEqual(@as(u64, 1), stats.slices[sliceIndex(.text_merge_buffers)].hard_limit_rejections);

    try manager.adjustUsage(.text_merge_buffers, &current, 4);
    stats = manager.snapshot();
    try std.testing.expectEqual(@as(u64, 4), current);
    try std.testing.expectEqual(@as(u64, 4), stats.slices[sliceIndex(.text_merge_buffers)].used_bytes);
}

test "resource manager observes over-budget external usage" {
    var budgets = Options.defaultBudgets();
    budgets[sliceIndex(.lsm_block_table_cache)] = .{
        .soft_limit_bytes = 10,
        .hard_limit_bytes = 20,
    };
    var manager = ResourceManager.init(.{ .budgets = budgets });
    var current: u64 = 0;

    manager.observeUsage(.lsm_block_table_cache, &current, 25);
    var stats = manager.snapshot();
    try std.testing.expectEqual(@as(u64, 25), current);
    try std.testing.expectEqual(@as(u64, 25), stats.slices[sliceIndex(.lsm_block_table_cache)].used_bytes);
    try std.testing.expectEqual(@as(u64, 1), stats.slices[sliceIndex(.lsm_block_table_cache)].soft_limit_events);
    try std.testing.expectEqual(@as(u64, 1), stats.slices[sliceIndex(.lsm_block_table_cache)].hard_limit_rejections);
    try std.testing.expectEqual(Pressure.hard, stats.slices[sliceIndex(.lsm_block_table_cache)].pressure);
    try std.testing.expectEqual(PressureAction.shrink_cache, stats.slices[sliceIndex(.lsm_block_table_cache)].hard_action);

    manager.observeUsage(.lsm_block_table_cache, &current, 5);
    stats = manager.snapshot();
    try std.testing.expectEqual(@as(u64, 5), current);
    try std.testing.expectEqual(@as(u64, 5), stats.slices[sliceIndex(.lsm_block_table_cache)].used_bytes);
    try std.testing.expectEqual(Pressure.normal, stats.slices[sliceIndex(.lsm_block_table_cache)].pressure);
}

test "identity-aware cache admission rejects growth and always permits shrink" {
    var budgets = Options.defaultBudgets();
    budgets[sliceIndex(.inference_tokenizer_cache)] = .{
        .soft_limit_bytes = 10,
        .hard_limit_bytes = 20,
    };
    var manager = ResourceManager.init(.{
        .memory_budget = .{ .hard_limit_bytes = 24 },
        .budgets = budgets,
        .identity_allocator = std.testing.allocator,
    });
    defer manager.deinit(std.testing.allocator);

    try std.testing.expect(manager.tryAdjustUsageIdentity(
        .inference_tokenizer_cache,
        41,
        0,
        18,
    ));
    try std.testing.expect(!manager.tryAdjustUsageIdentity(
        .inference_tokenizer_cache,
        41,
        18,
        21,
    ));
    try std.testing.expectEqual(
        @as(u64, 18),
        manager.sliceStats(.inference_tokenizer_cache).used_bytes,
    );
    try std.testing.expect(!manager.tryAdjustUsageIdentity(
        .inference_tokenizer_cache,
        42,
        0,
        7,
    ));
    try std.testing.expect(manager.tryAdjustUsageIdentity(
        .inference_tokenizer_cache,
        42,
        0,
        0,
    ));

    // Simulate an allocation that was observed after an operator tightened a
    // limit. Admission must still allow the owner to converge to zero.
    try std.testing.expect(manager.tryObserveUsageIdentity(
        .inference_tokenizer_cache,
        41,
        18,
        30,
    ));
    try std.testing.expect(manager.tryAdjustUsageIdentity(
        .inference_tokenizer_cache,
        41,
        30,
        12,
    ));
    try std.testing.expect(manager.tryAdjustUsageIdentity(
        .inference_tokenizer_cache,
        41,
        12,
        0,
    ));
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
}

test "resource manager evaluates projected admission with configured action" {
    var budgets = Options.defaultBudgets();
    budgets[sliceIndex(.lsm_in_memory_state)] = .{
        .soft_limit_bytes = 10,
        .hard_limit_bytes = 20,
    };
    var policies = Options.defaultPolicies();
    policies[sliceIndex(.lsm_in_memory_state)] = .{
        .soft_action = .report,
        .hard_action = .reject_work,
    };
    var manager = ResourceManager.init(.{ .budgets = budgets, .policies = policies });
    var current: u64 = 0;

    manager.observeUsage(.lsm_in_memory_state, &current, 12);
    const current_decision = manager.pressureDecision(.lsm_in_memory_state);
    try std.testing.expectEqual(Pressure.soft, current_decision.pressure);
    try std.testing.expectEqual(PressureAction.report, current_decision.action);

    const projected = manager.admissionDecision(.lsm_in_memory_state, 9);
    try std.testing.expectEqual(@as(u64, 21), projected.used_bytes);
    try std.testing.expectEqual(Pressure.hard, projected.pressure);
    try std.testing.expectEqual(PressureAction.reject_work, projected.action);
    try std.testing.expect(projected.change_epoch >= current_decision.change_epoch);
}

test "resource manager bounds soft write throttling without waiting for compaction publication" {
    var budgets = Options.defaultBudgets();
    budgets[sliceIndex(.lsm_in_memory_state)] = .{
        .soft_limit_bytes = 10,
        .hard_limit_bytes = 20,
    };
    var manager = ResourceManager.init(.{ .budgets = budgets });
    var current: u64 = 0;
    manager.observeUsage(.lsm_in_memory_state, &current, 12);

    const started_ns = platform_time.monotonicNs();
    try manager.awaitAdmission(.lsm_in_memory_state, 0);
    const elapsed_ns = platform_time.monotonicNs() - started_ns;
    try std.testing.expect(elapsed_ns < std.time.ns_per_s);
    try std.testing.expectEqual(Pressure.soft, manager.sliceStats(.lsm_in_memory_state).pressure);
}

test "resource manager rejects an impossible projected admission without waiting" {
    var budgets = Options.defaultBudgets();
    budgets[sliceIndex(.lsm_in_memory_state)] = .{
        .soft_limit_bytes = 10,
        .hard_limit_bytes = 20,
    };
    var manager = ResourceManager.init(.{ .budgets = budgets });

    try std.testing.expectError(
        error.ResourceBudgetExceeded,
        manager.awaitAdmission(.lsm_in_memory_state, 21),
    );
}

test "budgeted allocator admits before allocation and releases exact live bytes" {
    var budgets = Options.defaultBudgets();
    budgets[sliceIndex(.shard_transition_working_set)] = .{
        .soft_limit_bytes = 8,
        .hard_limit_bytes = 16,
    };
    var manager = ResourceManager.init(.{ .budgets = budgets });
    var budgeted = BudgetedAllocator.init(
        &manager,
        .shard_transition_working_set,
        std.testing.allocator,
        2,
    );
    defer budgeted.deinit();
    const alloc = budgeted.allocator();

    const first = try alloc.alloc(u8, 12);
    try std.testing.expectEqual(@as(u64, 12), manager.sliceStats(.shard_transition_working_set).used_bytes);
    const oversized = try alloc.alloc(u8, 12);
    try std.testing.expectEqual(@as(u64, 24), manager.sliceStats(.shard_transition_working_set).used_bytes);
    try std.testing.expectError(error.OutOfMemory, alloc.alloc(u8, 9));
    try std.testing.expect(budgeted.denied());
    try std.testing.expectEqual(@as(u64, 24), manager.sliceStats(.shard_transition_working_set).used_bytes);

    alloc.free(oversized);
    alloc.free(first);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.shard_transition_working_set).used_bytes);
}

test "budgeted allocator allows concurrent operations within the shared hard limit" {
    var budgets = Options.defaultBudgets();
    budgets[sliceIndex(.shard_transition_working_set)] = .{
        .soft_limit_bytes = 12,
        .hard_limit_bytes = 32,
    };
    var manager = ResourceManager.init(.{ .budgets = budgets });
    var first = BudgetedAllocator.init(&manager, .shard_transition_working_set, std.testing.allocator, 2);
    defer first.deinit();
    var second = BudgetedAllocator.init(&manager, .shard_transition_working_set, std.testing.allocator, 2);
    defer second.deinit();

    const first_bytes = try first.allocator().alloc(u8, 16);
    defer first.allocator().free(first_bytes);
    const second_bytes = try second.allocator().alloc(u8, 16);
    defer second.allocator().free(second_bytes);
    try std.testing.expectEqual(@as(u64, 32), manager.sliceStats(.shard_transition_working_set).used_bytes);
    try std.testing.expectError(error.OutOfMemory, second.allocator().alloc(u8, 1));
}

test "budgeted allocator amortizes manager reservations and releases idle credit" {
    var budgets = Options.defaultBudgets();
    budgets[sliceIndex(.shard_transition_working_set)] = .{
        .soft_limit_bytes = 64 * 1024 * 1024,
        .hard_limit_bytes = 128 * 1024 * 1024,
    };
    var manager = ResourceManager.init(.{ .budgets = budgets });
    var budgeted = BudgetedAllocator.init(
        &manager,
        .shard_transition_working_set,
        std.testing.allocator,
        1,
    );
    defer budgeted.deinit();
    const alloc = budgeted.allocator();

    const first = try alloc.alloc(u8, 1);
    const reserved = manager.sliceStats(.shard_transition_working_set).used_bytes;
    try std.testing.expectEqual(@as(u64, 1024 * 1024), reserved);
    const second = try alloc.alloc(u8, 4096);
    try std.testing.expectEqual(reserved, manager.sliceStats(.shard_transition_working_set).used_bytes);

    alloc.free(first);
    try std.testing.expectEqual(reserved, manager.sliceStats(.shard_transition_working_set).used_bytes);
    alloc.free(second);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.shard_transition_working_set).used_bytes);
}

test "resource manager records index repair activation pause separately from cleanup" {
    var manager = ResourceManager.init(.{});
    manager.recordIndexRepairActivation(20 * std.time.ns_per_ms, 25 * std.time.ns_per_ms);
    manager.recordIndexRepairActivation(30 * std.time.ns_per_ms, 25 * std.time.ns_per_ms);

    const stats = manager.indexRepairActivationStats();
    try std.testing.expectEqual(@as(u64, 2), stats.attempts);
    try std.testing.expectEqual(@as(u64, 1), stats.overruns);
    try std.testing.expectEqual(@as(u64, 30 * std.time.ns_per_ms), stats.last_pause_ns);
    try std.testing.expectEqual(@as(u64, 30 * std.time.ns_per_ms), stats.max_pause_ns);
    try std.testing.expectEqual(@as(u64, 25 * std.time.ns_per_ms), stats.last_budget_ns);
}
