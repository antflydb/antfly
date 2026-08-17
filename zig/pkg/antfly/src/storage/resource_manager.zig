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

pub const ResourceManager = struct {
    mutex: std.atomic.Mutex = .unlocked,
    pressure_change: PressureChange = .{},
    memory: MutableMemory,
    slices: [slice_count]MutableSlice,
    dense_replay_window_budget_bytes: u64 = 0,
    dense_replay_last_finish_ns: u64 = 0,
    dense_replay_last_write_pressure_ns: u64 = 0,
    dense_replay_last_write_pressure_compactions: u64 = 0,
    derived_backlog_high_sequences: usize,
    derived_backlog_resume_sequences: usize,
    disk_safety_floor_bytes: u64,
    disk_safety_floor_divisor: u64,
    capacity_domains: std.AutoHashMapUnmanaged(CapacityDomainId, MutableCapacityDomain) = .empty,
    query_embedding_cache_budget: cache_budget.CacheBudget,
    query_embedding_cache_ttl_ns: u64,
    query_embedding_max_inflight: usize,
    index_repair_activation: IndexRepairActivationStats = .{},
    derived_recoverable_retry_counters: DerivedRecoverableRetryCounters = .{},
    capacity_source: ?CapacitySource = null,

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
            .disk_safety_floor_bytes = options.disk_safety_floor_bytes,
            .disk_safety_floor_divisor = options.disk_safety_floor_divisor,
            .query_embedding_cache_budget = cache_budget.CacheBudget.init(options.query_embedding_cache_bytes),
            .query_embedding_cache_ttl_ns = options.query_embedding_cache_ttl_ns,
            .query_embedding_max_inflight = @max(@as(usize, 1), options.query_embedding_max_inflight),
        };
    }

    pub fn queryEmbeddingCacheBudget(self: *ResourceManager) *cache_budget.CacheBudget {
        return &self.query_embedding_cache_budget;
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
    pub fn deinit(self: *ResourceManager, alloc: std.mem.Allocator) void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        var it = self.capacity_domains.valueIterator();
        while (it.next()) |domain| std.debug.assert(domain.reserved_bytes == 0);
        self.capacity_domains.deinit(alloc);
        self.capacity_domains = .empty;
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
        self.pressure_change.advance();
        return true;
    }

    pub fn reserve(self: *ResourceManager, slice: Slice, bytes: u64) !Reservation {
        if (bytes == 0) return .{ .manager = self, .slice = slice, .bytes = 0 };

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
        state.used_bytes = next;
        state.peak_bytes = @max(state.peak_bytes, next);
        if (state.budget.soft_limit_bytes > 0 and next > state.budget.soft_limit_bytes) {
            state.soft_limit_events += 1;
        }
        self.commitMemoryLocked(next_memory);
        self.pressure_change.advance();
        return .{ .manager = self, .slice = slice, .bytes = bytes };
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
        if (bytes == 0) return .{ .manager = self, .slice = slice, .bytes = 0 };

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
        if (oversized_grant) state.oversized_single_grants +|= 1;
        state.used_bytes = next;
        state.peak_bytes = @max(state.peak_bytes, next);
        if (state.budget.soft_limit_bytes > 0 and next > state.budget.soft_limit_bytes) {
            state.soft_limit_events +|= 1;
        }
        self.commitMemoryLocked(next_memory);
        self.pressure_change.advance();
        return .{ .manager = self, .slice = slice, .bytes = bytes };
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
        state.peak_bytes = @max(state.peak_bytes, state.used_bytes);
        if (hard_limit > 0 and state.used_bytes > hard_limit and previous_reservation <= hard_limit)
            state.oversized_single_grants +|= 1;
        if (state.budget.soft_limit_bytes > 0 and state.used_bytes > state.budget.soft_limit_bytes)
            state.soft_limit_events +|= 1;
        self.commitMemoryLocked(next_memory);
        self.pressure_change.advance();
        return granted;
    }

    fn releaseReservationBytes(self: *ResourceManager, slice: Slice, bytes: u64) void {
        _ = self.tryReleaseReservationBytes(slice, bytes);
    }

    fn tryReleaseReservationBytes(self: *ResourceManager, slice: Slice, bytes: u64) bool {
        if (bytes == 0) return true;
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const state = &self.slices[sliceIndex(slice)];
        if (bytes > state.used_bytes or bytes > self.memory.used_bytes) {
            self.memory.accounting_errors +|= 1;
            return false;
        }
        state.used_bytes -= bytes;
        self.memory.used_bytes -= bytes;
        self.pressure_change.advance();
        return true;
    }

    pub fn adjustUsage(self: *ResourceManager, slice: Slice, current: *u64, next: u64) !void {
        if (next == current.*) return;
        if (next < current.*) {
            if (!self.tryReleaseReservationBytes(slice, current.* - next))
                return error.ResourceAccountingMismatch;
            current.* = next;
            return;
        }

        const delta = next - current.*;
        var reservation = try self.reserve(slice, delta);
        reservation.released = true;
        current.* = next;
    }

    pub fn observeUsage(self: *ResourceManager, slice: Slice, current: *u64, next: u64) void {
        _ = self.tryObserveUsage(slice, current, next);
    }

    /// Reconcile one observer-owned contribution without allowing stale input
    /// to debit bytes belonging to another observer or slice. On failure both
    /// ledgers and the caller's current value remain unchanged.
    pub fn tryObserveUsage(self: *ResourceManager, slice: Slice, current: *u64, next: u64) bool {
        if (next == current.*) return true;
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const state = &self.slices[sliceIndex(slice)];
        if (current.* > state.used_bytes or current.* > self.memory.used_bytes) {
            self.memory.accounting_errors +|= 1;
            return false;
        }
        const slice_base = state.used_bytes - current.*;
        const memory_base = self.memory.used_bytes - current.*;
        const slice_next = std.math.add(u64, slice_base, next) catch {
            self.memory.accounting_errors +|= 1;
            return false;
        };
        const memory_next = std.math.add(u64, memory_base, next) catch {
            self.memory.accounting_errors +|= 1;
            return false;
        };
        state.used_bytes = slice_next;
        self.memory.used_bytes = memory_next;
        current.* = next;
        state.peak_bytes = @max(state.peak_bytes, state.used_bytes);
        self.memory.peak_bytes = @max(self.memory.peak_bytes, self.memory.used_bytes);
        if (state.budget.soft_limit_bytes > 0 and state.used_bytes > state.budget.soft_limit_bytes) {
            state.soft_limit_events += 1;
        }
        if (state.budget.hard_limit_bytes > 0 and state.used_bytes > state.budget.hard_limit_bytes) {
            state.hard_limit_rejections += 1;
        }
        if (self.memory.budget.soft_limit_bytes > 0 and self.memory.used_bytes > self.memory.budget.soft_limit_bytes)
            self.memory.soft_limit_events +|= 1;
        if (self.memory.budget.hard_limit_bytes > 0 and self.memory.used_bytes > self.memory.budget.hard_limit_bytes)
            self.memory.hard_limit_rejections +|= 1;
        self.pressure_change.advance();
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
    slice: Slice,
    bytes: u64,
    released: bool = false,

    pub fn release(self: *Reservation) void {
        if (self.released) return;
        self.manager.releaseReservationBytes(self.slice, self.bytes);
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
        const released_bytes = @min(bytes, self.bytes);
        self.manager.releaseReservationBytes(self.slice, released_bytes);
        self.bytes -= released_bytes;
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

    first.release();
    first.release();
    var stats = manager.snapshot();
    try std.testing.expectEqual(@as(u64, 15), stats.memory.used_bytes);
    try std.testing.expectEqual(@as(u64, 20), manager.sliceStats(.inference_model_residency).used_bytes);
    try std.testing.expectEqual(@as(u64, 0), stats.memory.accounting_errors);

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
    try std.testing.expectEqual(@as(u64, 3), stats.memory.accounting_errors);
}

test "single release and observer mismatch cannot debit unrelated memory" {
    var manager = ResourceManager.init(.{});
    var first = try manager.reserve(.inference_tokenizer_cache, 10);
    var unrelated = try manager.reserve(.inference_model_residency, 20);

    try std.testing.expect(!manager.tryReleaseReservationBytes(.inference_tokenizer_cache, 11));
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
