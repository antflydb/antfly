// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");
const read_admission = @import("../common/read_admission.zig");
const builtin = @import("builtin");
const platform_time = @import("antfly_platform").time;
const shared_platform_time = @import("antfly_platform").time;
const cache_budget = @import("antfly_cache_budget");
pub const DenseWorkAdmission = @import("dense_work_admission.zig");
const admission = @import("admission_waiter.zig");
const dense_perf = @import("dense_perf_experiments.zig");
pub const ProjectionPageCache = @import("projection_page_cache.zig");

const MiB: u64 = 1024 * 1024;

/// Resource identities churn for the entire process lifetime. std.HashMap
/// restores insertion capacity on removal, but retains tombstones: absent-key
/// probes can eventually traverse the whole table while holding the admission
/// mutex. Bound deleted slots independently of live occupancy. Rehashing is
/// allocation-free and amortized over at least capacity/8 successful removals.
/// As with insertion, removal may invalidate pointers into this ledger.
fn IdentityLedger(comptime K: type, comptime V: type) type {
    return struct {
        const Self = @This();
        const Map = std.AutoHashMapUnmanaged(K, V);
        map: Map = .empty,
        removals: usize = 0,
        rehashes: u64 = 0,
        const empty: Self = .{};

        fn count(self: *const Self) u32 {
            return self.map.count();
        }

        fn contains(self: *const Self, key: K) bool {
            return self.map.contains(key);
        }

        fn get(self: *const Self, key: K) ?V {
            return self.map.get(key);
        }

        fn getPtr(self: *const Self, key: K) ?*V {
            return self.map.getPtr(key);
        }

        fn put(self: *Self, manager: *ResourceManager, key: K, value: V) !void {
            return self.putWithParentPolicy(manager, key, value, false);
        }

        fn putWithParentPolicy(self: *Self, manager: *ResourceManager, key: K, value: V, bounded_parent: bool) !void {
            var admission_context = IdentityAllocation{ .manager = manager, .bounded_parent = bounded_parent };
            self.map.put(admission_context.allocator(), key, value) catch return admission_context.failure orelse error.OutOfMemory;
        }

        fn getOrPut(self: *Self, manager: *ResourceManager, key: K) !Map.GetOrPutResult {
            return self.getOrPutWithParentPolicy(manager, key, false);
        }

        fn getOrPutWithParentPolicy(self: *Self, manager: *ResourceManager, key: K, bounded_parent: bool) !Map.GetOrPutResult {
            var admission_context = IdentityAllocation{ .manager = manager, .bounded_parent = bounded_parent };
            return self.map.getOrPut(admission_context.allocator(), key) catch return admission_context.failure orelse error.OutOfMemory;
        }

        fn ensureUnusedCapacity(self: *Self, manager: *ResourceManager, additional: u32) !void {
            var admission_context = IdentityAllocation{ .manager = manager };
            self.map.ensureUnusedCapacity(admission_context.allocator(), additional) catch return admission_context.failure orelse error.OutOfMemory;
        }

        fn remove(self: *Self, key: K) bool {
            if (!self.map.remove(key)) return false;
            self.removals += 1;
            if (self.removals >= @max(1, self.map.capacity() / 8)) {
                self.map.rehash(std.hash_map.AutoContext(K){});
                self.removals = 0;
                self.rehashes +|= 1;
            }
            return true;
        }

        pub fn deinit(self: *Self, manager: *ResourceManager) void {
            var admission_context = IdentityAllocation{ .manager = manager };
            self.map.deinit(admission_context.allocator());
            self.* = .empty;
        }
    };
}
/// Operation-local adapter: unmanaged maps retain no allocator pointer, so a
/// ResourceManager may move during startup without capturing its old address.
/// Every call holds manager.mutex. Charge the complete new allocation before
/// asking the backing allocator; the old table stays charged through rehash.
const IdentityAllocation = struct {
    manager: *ResourceManager,
    failure: ?anyerror = null,
    bounded_parent: bool = false,

    fn allocator(self: *@This()) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = std.mem.Allocator.noResize, .remap = std.mem.Allocator.noRemap, .free = free } };
    }

    fn alloc(ptr: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const manager = self.manager;
        manager.drainIdentityMetadataRetirementLocked(self.bounded_parent) catch |err| {
            self.failure = err;
            return null;
        };
        const metadata_next = std.math.add(u64, manager.identity_metadata_bytes, len) catch {
            self.failure = error.ResourceBudgetExceeded;
            return null;
        };
        if (metadata_next > manager.identity_metadata_limit_bytes or
            manager.control_object_bytes > manager.identity_metadata_limit_bytes -| metadata_next)
        {
            self.failure = error.ResourceBudgetExceeded;
            return null;
        }
        const memory_next = manager.checkMemoryLocked(len) catch |err| {
            self.failure = err;
            return null;
        };
        (if (self.bounded_parent) manager.reserveParentWithoutWaiting(len) else manager.reserveParentLocked(len)) catch |err| {
            self.failure = err;
            return null;
        };
        manager.commitMemoryLocked(memory_next);
        manager.identity_metadata_bytes = metadata_next;
        return manager.identity_allocator.rawAlloc(len, alignment, ret_addr) orelse {
            manager.identity_metadata_bytes -= len;
            manager.retireIdentityMetadataLocked(len, self.bounded_parent);
            return null;
        };
    }

    fn free(ptr: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.manager.identity_allocator.rawFree(bytes, alignment, ret_addr);
        self.manager.identity_metadata_bytes -= bytes.len;
        self.manager.retireIdentityMetadataLocked(bytes.len, self.bounded_parent);
    }
};
/// An operation-local allocator for node-owned control objects and unmanaged
/// wrapper maps. Containers must not retain this allocator/context. Allocation
/// and free use the same backing allocator, and the manager must outlive every
/// object. Provisional and cached storage shares the identity-table byte cap.
/// Supply a raw lifetime backing allocator, not one already charging this
/// manager or its parent. It runs outside the accounting mutex.
pub const ControlMetadataAllocation = struct {
    manager: *ResourceManager,
    backing: std.mem.Allocator,
    failure: ?anyerror = null,

    pub fn allocator(self: *@This()) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = std.mem.Allocator.noResize, .remap = std.mem.Allocator.noRemap, .free = free } };
    }

    pub fn allocationError(self: *const @This(), err: anyerror) anyerror {
        return self.failure orelse err;
    }

    fn admit(self: *@This(), len: usize) !void {
        const manager = self.manager;
        lockAtomic(&manager.mutex);
        defer manager.mutex.unlock();
        try manager.drainIdentityMetadataRetirementLocked(false);
        const active = std.math.add(u64, manager.identity_metadata_bytes, manager.control_object_bytes) catch return error.ResourceBudgetExceeded;
        const total = std.math.add(u64, active, len) catch return error.ResourceBudgetExceeded;
        if (total > manager.identity_metadata_limit_bytes) return error.ResourceBudgetExceeded;
        const next = try manager.checkMemoryLocked(len);
        try manager.reserveParentLocked(len);
        manager.control_object_bytes += len;
        manager.commitMemoryLocked(next);
    }

    fn retire(self: *@This(), len: usize) void {
        const manager = self.manager;
        lockAtomic(&manager.mutex);
        std.debug.assert(manager.control_object_bytes >= len);
        manager.control_object_bytes -= len;
        manager.retireIdentityMetadataLocked(len, false);
        manager.mutex.unlock();
        // Provisional admission is observable while the backing allocator
        // runs. Return its capacity and wake waiters after physical retirement.
        manager.notifyPressureChange();
    }

    fn alloc(ptr: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.failure = null;
        self.admit(len) catch |err| {
            self.failure = err;
            return null;
        };
        return self.backing.rawAlloc(len, alignment, ret_addr) orelse {
            self.retire(len);
            return null;
        };
    }

    fn free(ptr: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.backing.rawFree(bytes, alignment, ret_addr);
        self.retire(bytes.len);
    }
};

pub const metadata_fixture = if (builtin.is_test) @import("identity_metadata_test_fixture.zig") else struct {};
const dense_replay_window_min_bytes: u64 = 16 * MiB;
const dense_replay_window_growth_numerator: u64 = 5;
const dense_replay_window_growth_denominator: u64 = 4;
const dense_replay_window_shrink_numerator: u64 = 3;
const dense_replay_window_shrink_denominator: u64 = 4;
const dense_replay_finish_target_ns: u64 = 3 * std.time.ns_per_s;
const dense_replay_finish_hard_ns: u64 = 8 * std.time.ns_per_s;
const dense_replay_write_pressure_hard_ns: u64 = std.time.ns_per_s;
const dense_replay_soft_compaction_quiet_ns: u64 = 500 * std.time.ns_per_ms;
const foreground_query_maintenance_quiet_ns: u64 = 25 * std.time.ns_per_ms;
const foreground_write_maintenance_quiet_ns: u64 = 2 * std.time.ns_per_ms;
// A two-millisecond cap is shorter than one cold vector query and lets a
// multi-GiB compactor repeatedly reclaim the storage device before the query
// can fault its native posting/vector pages. Fifty milliseconds still gives a
// sustained query stream bounded compaction progress at every merge quantum,
// while allowing ordinary foreground reads to finish in one yield window.
const foreground_query_compaction_yield_max_ns: u64 = 50 * std.time.ns_per_ms;
const soft_throttle_delay_ns: u64 = 10 * std.time.ns_per_ms;
const supports_pressure_wait = builtin.os.tag != .freestanding and
    builtin.os.tag != .wasi and
    !builtin.single_threaded and
    builtin.link_libc and
    @hasDecl(std.c, "pthread_cond_wait");

const PressureChange = if (supports_pressure_wait) WaitingPressureChange else PollingPressureChange;
const WaitingPressureChange = struct {
    mutex: std.c.pthread_mutex_t = std.c.PTHREAD_MUTEX_INITIALIZER,
    cond: std.c.pthread_cond_t = std.c.PTHREAD_COND_INITIALIZER,
    epoch: @import("antfly_platform").atomic.Value(u64) = .init(0),

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

    fn tryLock(self: *@This()) bool {
        return std.c.pthread_mutex_trylock(&self.mutex) == .SUCCESS;
    }
    fn unlock(self: *@This()) void {
        if (std.c.pthread_mutex_unlock(&self.mutex) != .SUCCESS) unreachable;
    }
    fn advanceLocked(self: *@This()) void {
        _ = self.epoch.fetchAdd(1, .release);
        if (std.c.pthread_cond_broadcast(&self.cond) != .SUCCESS) unreachable;
    }
    fn advance(self: *@This()) void {
        if (std.c.pthread_mutex_lock(&self.mutex) != .SUCCESS) unreachable;
        defer self.unlock();
        self.advanceLocked();
    }
};
const PollingPressureChange = struct {
    epoch: @import("antfly_platform").atomic.Value(u64) = .init(0),

    fn snapshot(self: *@This()) u64 {
        return self.epoch.load(.acquire);
    }

    fn waitForChange(_: *@This(), _: u64) void {}

    fn tryLock(_: *@This()) bool {
        return true;
    }
    fn unlock(_: *@This()) void {}
    fn advanceLocked(self: *@This()) void {
        self.advance();
    }
    fn advance(self: *@This()) void {
        _ = self.epoch.fetchAdd(1, .release);
    }
};
const default_disk_safety_floor_bytes: u64 = 1024 * MiB;
const default_disk_safety_floor_divisor: u64 = 20;
// Percentage-only reserves strand tens or hundreds of GiB on large volumes
// and can prevent the small repair that restores query service. Planned
// candidate bytes are reserved independently, so this cap is additional
// emergency durability headroom rather than the maintenance work budget.
const default_disk_safety_floor_max_bytes: u64 = 16 * 1024 * MiB;

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
    relational_preparation_working_set,
    /// Transient heap used to project primary embedding artifacts into an
    /// immutable mmap exact-vector generation. This is separate from dense
    /// apply so readiness publication cannot consume foreground mutation
    /// admission, and every builder allocation remains inside the aggregate
    /// host-memory envelope.
    dense_vector_block_build_working_set,
    dense_source_payload_state,
    /// Pending persistent object-range cache writes. The durable bytes use
    /// the capacity-domain ledger; this slice owns only queued key/payload
    /// memory until the cache worker completes or drops the write.
    lake_range_cache_queue,
    /// Decoder scratch and uncached decoded payload leases. Appended to keep
    /// existing resource IDs stable and separate reads from mutable admission.
    lsm_read_working_set,
    /// Concurrent SQL execution and retained delivery state share host admission.
    sql_execution_working_set,

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
            .relational_preparation_working_set => "relational.preparation_working_set",
            .dense_vector_block_build_working_set => "dense.vector_block_build_working_set",
            .dense_source_payload_state => "dense.source_payload_state",
            .lake_range_cache_queue => "lake.range_cache_queue",
            .lsm_read_working_set => "lsm.read_working_set",
            .sql_execution_working_set => "sql.execution_working_set",
        };
    }
};

pub const slice_count: usize = @typeInfo(Slice).@"enum".field_names.len;

pub const Budget = struct {
    soft_limit_bytes: u64 = 0,
    hard_limit_bytes: u64 = 0,
};

pub const SliceAmount = struct {
    slice: Slice,
    bytes: u64,
};

/// Metadata prepared before entering an executor lock. Preparation owns no
/// working-set capacity. A successful attempt moves the same identity into a
/// BatchReservation; no insertion, allocator, reclaimer, wait or legacy parent
/// callback occurs in that attempt. Callers serialize this token's lifetime.
/// CPU/I/O bundle coordination and continuation progress belong to the caller.
pub const PreparedBatchReservation = struct {
    manager: *ResourceManager,
    identity: u64,
    amounts: [slice_count]u64,
    host_charge_bytes: u64,
    consumed: bool = false,

    pub const Probe = enum { ready, wait, unknown, too_large, unsupported, mismatch };

    pub fn probe(self: *const PreparedBatchReservation) Probe {
        return self.manager.probePreparedBatch(self);
    }

    pub const Attempt = union(enum) {
        acquired: BatchReservation,
        busy,
        denied,
        too_large,
        unsupported,
        mismatch,
    };

    pub fn tryAcquire(self: *PreparedBatchReservation) Attempt {
        return self.manager.tryAcquirePreparedBatch(self);
    }

    /// Retire unused metadata outside executor locks: ledger removal can
    /// compact its hash table. A stale copy cannot retire an acquired lease.
    pub fn deinit(self: *PreparedBatchReservation) void {
        if (self.consumed) return;
        self.manager.discardPreparedBatch(self);
        self.consumed = true;
    }
};

/// A dense scan's memory and bandwidth are checked under one lock order.
/// The parent memory attempt is the last fallible action; bandwidth is charged
/// only after it succeeds. CPU publication follows in the executor lock.
/// These leases survive CPU parking and retire only after physical users join.
pub const PreparedDenseBundle = struct {
    memory: PreparedBatchReservation,
    requested_bandwidth_bytes: u64,
    lease: ?struct { memory: BatchReservation, bandwidth: DenseSearchAdmissionLease } = null,
    pub const Acquire = enum { acquired, wait, too_large, unsupported, mismatch };

    pub fn probe(self: *const PreparedDenseBundle) PreparedBatchReservation.Probe {
        if (self.lease != null) return .mismatch;
        const manager = self.memory.manager;
        if (!manager.dense_search_admission_mutex.tryLock()) return .wait;
        defer manager.dense_search_admission_mutex.unlock();
        const bytes = manager.denseSearchGrantBytes(self.requested_bandwidth_bytes);
        if (bytes != 0 and (manager.dense_search_waiters.head != null or !manager.canGrantDenseSearchLocked(bytes))) return .wait;
        return self.memory.probe();
    }

    pub fn tryAcquire(self: *PreparedDenseBundle) Acquire {
        if (self.lease != null) return .mismatch;
        const manager = self.memory.manager;
        if (!manager.dense_search_admission_mutex.tryLock()) return .wait;
        defer manager.dense_search_admission_mutex.unlock();
        const bytes = manager.denseSearchGrantBytes(self.requested_bandwidth_bytes);
        if (bytes != 0 and (manager.dense_search_waiters.head != null or !manager.canGrantDenseSearchLocked(bytes))) return .wait;
        const credit = switch (self.memory.tryAcquire()) {
            .acquired => |credit| credit,
            .busy, .denied => return .wait,
            .too_large => return .too_large,
            .unsupported => return .unsupported,
            .mismatch => return .mismatch,
        };
        // No fallible work, callback, allocation or publication after memory
        // admission until both non-CPU resources have a single physical owner.
        if (bytes != 0) manager.noteDenseSearchGrantLocked(bytes);
        self.lease = .{ .memory = credit, .bandwidth = if (bytes == 0) .{} else .{ .manager = manager, .bytes = bytes } };
        return .acquired;
    }

    /// Call outside executor/resource locks after every physical user joined.
    pub fn deinitAfterJoin(self: *PreparedDenseBundle) void {
        if (self.lease) |*lease| {
            if (!lease.memory.released) {
                // The memory identity is also this bundle's retirement fence.
                // A stale copied bundle cannot refund another scan's bandwidth.
                const released = lease.memory.manager.releaseBatchReservation(&lease.memory);
                lease.memory.released = true;
                if (released) lease.bandwidth.release();
            }
        }
        self.memory.deinit();
    }
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

    /// Partition admitted credit into another registered live owner. Prepare
    /// the destination (possibly empty) before handoff: this operation never
    /// allocates, reacquires capacity, or changes aggregate/ancestor usage.
    /// Logical slices and physical host charge remain independent quantities.
    pub fn transferCreditTo(
        self: *BatchReservation,
        destination: *BatchReservation,
        amounts: []const SliceAmount,
        host_charge_bytes: u64,
    ) !void {
        const normalized = ResourceManager.normalizeSliceAmounts(amounts) catch |err| {
            self.manager.recordAccountingError();
            return err;
        };
        try self.manager.transferBatchReservationCredit(self, destination, normalized, host_charge_bytes);
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
    /// Bound the asynchronous urgency target emitted when sequence-only replay
    /// debt crosses its high watermark. Foreground requests never drain this
    /// window themselves; payload plus tracker metadata remain independently
    /// bounded by the derived-backlog byte budget.
    derived_backlog_throttle_window_sequences: usize = 16,
    /// Node-owned filesystem growth policy. This is deliberately not table or
    /// index configuration. The larger of the fixed floor and the bounded
    /// capacity fraction is kept available for WAL, checkpoints, and
    /// foreground durability. A zero max preserves an uncapped fraction.
    disk_safety_floor_bytes: u64 = default_disk_safety_floor_bytes,
    disk_safety_floor_divisor: u64 = default_disk_safety_floor_divisor,
    disk_safety_floor_max_bytes: u64 = default_disk_safety_floor_max_bytes,
    /// Internal query-embedding cache policy. Serving layers consume this
    /// policy but cannot override it independently of the node manager.
    query_embedding_cache_bytes: usize = 64 * 1024 * 1024,
    query_embedding_cache_ttl_ns: u64 = 5 * std.time.ns_per_min,
    query_embedding_max_inflight: usize = 16,
    /// Node-wide bandwidth budget for concurrently scanning dense candidate
    /// planes. Null derives the budget from the dense-search soft byte limit;
    /// zero disables bandwidth admission. This is intentionally separate from
    /// retained-memory accounting: permits model bytes simultaneously streamed
    /// through shared caches and memory channels.
    dense_search_bandwidth_capacity_bytes: ?u64 = null,
    /// Additional std.Io read workers, beyond already-admitted query callers.
    /// Null derives from logical CPUs; zero keeps caller-only execution.
    dense_read_extra_task_limit: ?u32 = null,
    /// Allocates identity tables and the reclaimer registry only as concurrent
    /// owners exceed their previous high-water mark. Production owners should
    /// pass their lifetime allocator; the page allocator keeps lightweight
    /// tests source-compatible.
    identity_allocator: std.mem.Allocator = platformAllocator(),
    /// Shared high-water storage for reservation, queued batch and observer
    /// identities plus provisional/cached control objects and wrapper maps,
    /// including the simultaneous old/new tables during growth.
    /// This finite cap applies even when memory_budget is unlimited; with a
    /// process envelope these bytes also consume that same envelope.
    identity_metadata_limit_bytes: u64 = 64 * MiB,

    pub fn defaultBudgets() [slice_count]Budget {
        // Name every slice: additions cannot silently shift another owner's policy.
        return std.enums.EnumArray(Slice, Budget).init(.{
            .lsm_block_table_cache = .{ .soft_limit_bytes = 192 * 1024 * 1024, .hard_limit_bytes = 256 * 1024 * 1024 },
            .lsm_compaction_work = .{ .soft_limit_bytes = 512 * 1024 * 1024, .hard_limit_bytes = 768 * 1024 * 1024 },
            .lsm_table_builder_working_set = .{ .soft_limit_bytes = 128 * 1024 * 1024, .hard_limit_bytes = 256 * 1024 * 1024 },
            .lsm_in_memory_state = .{ .soft_limit_bytes = 512 * 1024 * 1024, .hard_limit_bytes = 768 * 1024 * 1024 },
            .lsm_wal_write_working_set = .{ .soft_limit_bytes = 256 * 1024 * 1024, .hard_limit_bytes = 512 * 1024 * 1024 },
            .lsm_wal_retention = .{ .soft_limit_bytes = 512 * 1024 * 1024, .hard_limit_bytes = 1024 * 1024 * 1024 },
            .lsm_recovery_working_set = .{ .soft_limit_bytes = 128 * 1024 * 1024, .hard_limit_bytes = 256 * 1024 * 1024 },
            .hbc_node_metadata_cache = .{ .soft_limit_bytes = 384 * 1024 * 1024, .hard_limit_bytes = 512 * 1024 * 1024 },
            .dense_search_working_set = .{ .soft_limit_bytes = 96 * 1024 * 1024, .hard_limit_bytes = 160 * 1024 * 1024 },
            .dense_apply_working_set = .{ .soft_limit_bytes = 128 * 1024 * 1024, .hard_limit_bytes = 256 * 1024 * 1024 },
            .dense_routing_working_set = .{ .soft_limit_bytes = 128 * 1024 * 1024, .hard_limit_bytes = 256 * 1024 * 1024 },
            .derived_replay_window = .{ .soft_limit_bytes = 96 * 1024 * 1024, .hard_limit_bytes = 160 * 1024 * 1024 },
            .full_text_pending_segments = .{ .soft_limit_bytes = 192 * 1024 * 1024, .hard_limit_bytes = 256 * 1024 * 1024 },
            .full_text_build_working_set = .{ .soft_limit_bytes = 256 * 1024 * 1024, .hard_limit_bytes = 512 * 1024 * 1024 },
            .full_text_segment_residency = .{ .soft_limit_bytes = 512 * 1024 * 1024, .hard_limit_bytes = 768 * 1024 * 1024 },
            .document_extraction_working_set = .{ .soft_limit_bytes = 192 * 1024 * 1024, .hard_limit_bytes = 256 * 1024 * 1024 },
            .derived_backlog = .{ .soft_limit_bytes = 128 * 1024 * 1024, .hard_limit_bytes = 192 * 1024 * 1024 },
            .text_merge_buffers = .{ .soft_limit_bytes = 128 * 1024 * 1024, .hard_limit_bytes = 192 * 1024 * 1024 },
            .algebraic_tensor_accumulators = .{ .soft_limit_bytes = 96 * 1024 * 1024, .hard_limit_bytes = 160 * 1024 * 1024 },
            .sparse_apply_working_set = .{ .soft_limit_bytes = 128 * 1024 * 1024, .hard_limit_bytes = 256 * 1024 * 1024 },
            .lite_native_page_cache = .{ .soft_limit_bytes = 48 * 1024 * 1024, .hard_limit_bytes = 64 * 1024 * 1024 },
            .lite_native_link_cache = .{ .soft_limit_bytes = 12 * 1024 * 1024, .hard_limit_bytes = 16 * 1024 * 1024 },
            .lite_docstore_snapshot_cache = .{ .soft_limit_bytes = 192 * 1024 * 1024, .hard_limit_bytes = 256 * 1024 * 1024 },
            .inference_prompt_cache = .{ .soft_limit_bytes = 512 * 1024 * 1024, .hard_limit_bytes = 768 * 1024 * 1024 },
            .inference_tokenizer_cache = .{ .soft_limit_bytes = 64 * 1024 * 1024, .hard_limit_bytes = 128 * 1024 * 1024 },
            .inference_model_residency = .{},
            .inference_kv_working_set = .{},
            .inference_scratch_working_set = .{},
            .dense_repair_working_set = .{ .soft_limit_bytes = 128 * 1024 * 1024, .hard_limit_bytes = 256 * 1024 * 1024 },
            .shard_transition_working_set = .{ .soft_limit_bytes = 128 * 1024 * 1024, .hard_limit_bytes = 256 * 1024 * 1024 },
            .dense_vector_block_build_working_set = .{ .soft_limit_bytes = 96 * 1024 * 1024, .hard_limit_bytes = 128 * 1024 * 1024 },
            .dense_source_payload_state = .{ .soft_limit_bytes = 192 * 1024 * 1024, .hard_limit_bytes = 384 * 1024 * 1024 },
            .relational_preparation_working_set = .{ .soft_limit_bytes = 128 * 1024 * 1024, .hard_limit_bytes = 256 * 1024 * 1024 },
            .lake_range_cache_queue = .{ .soft_limit_bytes = 384 * 1024 * 1024, .hard_limit_bytes = 512 * 1024 * 1024 },
            .lsm_read_working_set = .{ .soft_limit_bytes = 64 * 1024 * 1024, .hard_limit_bytes = 128 * 1024 * 1024 },
            .sql_execution_working_set = .{ .soft_limit_bytes = 256 * 1024 * 1024, .hard_limit_bytes = 512 * 1024 * 1024 },
        }).values;
    }

    pub fn defaultPolicies() [slice_count]Policy {
        // Name every slice: additions cannot silently shift another owner's policy.
        return std.enums.EnumArray(Slice, Policy).init(.{
            .lsm_block_table_cache = .{ .soft_action = .shrink_cache, .hard_action = .shrink_cache },
            .lsm_compaction_work = .{ .soft_action = .defer_background_work, .hard_action = .reject_work },
            .lsm_table_builder_working_set = .{ .soft_action = .defer_background_work, .hard_action = .reject_work },
            .lsm_in_memory_state = .{ .soft_action = .throttle_writes, .hard_action = .throttle_writes },
            .lsm_wal_write_working_set = .{ .soft_action = .report, .hard_action = .throttle_writes },
            .lsm_wal_retention = .{ .soft_action = .report, .hard_action = .throttle_writes },
            .lsm_recovery_working_set = .{ .soft_action = .report, .hard_action = .report },
            .hbc_node_metadata_cache = .{ .soft_action = .shrink_cache, .hard_action = .shrink_cache },
            .dense_search_working_set = .{ .soft_action = .report, .hard_action = .throttle_writes },
            .dense_apply_working_set = .{ .soft_action = .report, .hard_action = .throttle_writes },
            .dense_routing_working_set = .{ .soft_action = .report, .hard_action = .throttle_writes },
            .derived_replay_window = .{ .soft_action = .report, .hard_action = .reject_work },
            .full_text_pending_segments = .{ .soft_action = .defer_background_work, .hard_action = .defer_background_work },
            .full_text_build_working_set = .{ .soft_action = .throttle_writes, .hard_action = .reject_work },
            .full_text_segment_residency = .{ .soft_action = .shrink_cache, .hard_action = .shrink_cache },
            .document_extraction_working_set = .{ .soft_action = .defer_background_work, .hard_action = .reject_work },
            .derived_backlog = .{ .soft_action = .throttle_writes, .hard_action = .throttle_writes },
            .text_merge_buffers = .{ .soft_action = .defer_background_work, .hard_action = .reject_work },
            .algebraic_tensor_accumulators = .{ .soft_action = .throttle_writes, .hard_action = .reject_work },
            .sparse_apply_working_set = .{ .soft_action = .report, .hard_action = .throttle_writes },
            .lite_native_page_cache = .{ .soft_action = .shrink_cache, .hard_action = .shrink_cache },
            .lite_native_link_cache = .{ .soft_action = .shrink_cache, .hard_action = .shrink_cache },
            .lite_docstore_snapshot_cache = .{ .soft_action = .shrink_cache, .hard_action = .shrink_cache },
            .inference_prompt_cache = .{ .soft_action = .shrink_cache, .hard_action = .shrink_cache },
            .inference_tokenizer_cache = .{ .soft_action = .shrink_cache, .hard_action = .shrink_cache },
            .inference_model_residency = .{ .soft_action = .report, .hard_action = .reject_work },
            .inference_kv_working_set = .{ .soft_action = .report, .hard_action = .reject_work },
            .inference_scratch_working_set = .{ .soft_action = .report, .hard_action = .reject_work },
            .dense_repair_working_set = .{ .soft_action = .defer_background_work, .hard_action = .reject_work },
            .shard_transition_working_set = .{ .soft_action = .defer_background_work, .hard_action = .reject_work },
            .dense_vector_block_build_working_set = .{ .soft_action = .report, .hard_action = .reject_work },
            .dense_source_payload_state = .{ .soft_action = .report, .hard_action = .throttle_writes },
            .relational_preparation_working_set = .{ .soft_action = .report, .hard_action = .reject_work },
            .lake_range_cache_queue = .{ .soft_action = .report, .hard_action = .reject_work },
            .lsm_read_working_set = .{ .soft_action = .report, .hard_action = .reject_work },
            .sql_execution_working_set = .{ .soft_action = .report, .hard_action = .reject_work },
        }).values;
    }
};

fn platformAllocator() std.mem.Allocator {
    if (comptime builtin.cpu.arch == .wasm32 or builtin.cpu.arch == .wasm64) {
        return std.heap.wasm_allocator;
    }
    return std.heap.page_allocator;
}

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
    control_object_bytes: u64 = 0,
    identity_metadata_bytes: u64 = 0,
    identity_metadata_retirement_bytes: u64 = 0,
    identity_metadata_limit_bytes: u64 = 0,
    slices: [slice_count]SliceStats,
    reclaim_requests: u64 = 0,
    reclaimed_bytes: u64 = 0,
    dense_search_admission: DenseSearchAdmissionStats = .{},
    dense_read_tasks: DenseReadTaskStats = .{},
};

pub const DenseReadTaskStats = struct {
    limit: u32 = 0,
    active: u32 = 0,
    peak_active: u32 = 0,
    denied: u64 = 0,
    physically_ordered_batches: u64 = 0,
    physically_ordered_requests: u64 = 0,
};

pub const DenseSearchAdmissionStats = struct {
    capacity_bytes: u64 = 0,
    active_bytes: u64 = 0,
    peak_active_bytes: u64 = 0,
    active_queries: u64 = 0,
    peak_active_queries: u64 = 0,
    queued_queries: u64 = 0,
    peak_queued_queries: u64 = 0,
    admissions: u64 = 0,
    waits: u64 = 0,
    cancellations: u64 = 0,
    wait_ns: u64 = 0,
};

pub const DenseSearchCancellation = admission.Cancellation;

pub const DenseSearchAdmissionLease = struct {
    manager: ?*ResourceManager = null,
    bytes: u64 = 0,

    pub fn release(self: *@This()) void {
        const manager = self.manager orelse return;
        manager.releaseDenseSearchBandwidth(self.bytes);
        self.* = .{};
    }
};

// This payload appears in ResourceManager's cross-archive layout contract.
// An anonymous generic argument gains an unstable compiler-generated name.
const DenseSearchWaitRequest = struct {
    bytes: u64,
    enqueued_ns: u64,
};
const DenseSearchWaiters = admission.Fifo(DenseSearchWaitRequest);
const DenseSearchWaiter = DenseSearchWaiters.Waiter;

pub const MemoryStats = struct {
    external_bytes: u64 = 0,
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
    working_set_factor: u64 = 1,
};

pub const DenseReplayWindowLimits = struct {
    work_bytes: u64,
    working_set_bytes: u64,
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
    total: @import("antfly_platform").atomic.Value(u64) = .init(0),
    writer_locked: @import("antfly_platform").atomic.Value(u64) = .init(0),
    resource_budget: @import("antfly_platform").atomic.Value(u64) = .init(0),
    replay_document_not_visible: @import("antfly_platform").atomic.Value(u64) = .init(0),
    artifact_repair_required: @import("antfly_platform").atomic.Value(u64) = .init(0),
    not_found: @import("antfly_platform").atomic.Value(u64) = .init(0),

    fn record(self: *@This(), err: anyerror) void {
        _ = self.total.fetchAdd(1, .monotonic);
        switch (err) {
            error.WriterLocked, error.WouldBlock, error.FileBusy => _ = self.writer_locked.fetchAdd(1, .monotonic),
            error.ResourceBudgetExceeded,
            error.SchedulerFull,
            error.ResourceTemporarilyUnavailable,
            error.PostingWalTooLarge,
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

fn replayEscalationEnvU64(name: [:0]const u8, default: u64) u64 {
    const raw_z = if (builtin.link_libc) std.c.getenv(name) else null;
    const raw = std.mem.span(raw_z orelse return default);
    if (raw.len == 0) return default;
    return std.fmt.parseUnsigned(u64, raw, 10) catch default;
}

// catch_up_policy.RecoverableRetryBackoff's delay doubles from 10ms and caps
// at 250ms after the 6th attempt (10+20+40+80+160+250 = 560ms), so every
// retry after that adds a flat 250ms. 1200 retries is therefore about
// 560ms + 1194 * 250ms =~ 299s, five minutes. Ordinary replay-visibility lag
// between a primary-store commit and the derived journal record landing is
// sub-second to low seconds even under heavy write pressure; a document
// still missing after five full minutes of continuous retrying is not a
// transient commit-ordering race, it is a document that is never going to
// gain content (issue #938's `_edges`-only shape, or another producer like
// #928's never-written chunk). Minutes, not seconds, so a slow but honest
// catch-up window is never mistaken for one of these.
const replay_document_not_visible_default_max_retries: u64 = 1200;

fn replayDocumentNotVisibleMaxRetries() u32 {
    const configured = replayEscalationEnvU64(
        "ANTFLY_REPLAY_DOCUMENT_NOT_VISIBLE_MAX_RETRIES",
        replay_document_not_visible_default_max_retries,
    );
    return @intCast(@min(configured, std.math.maxInt(u32)));
}

/// One index's replay-visibility tracking: how many consecutive times its
/// current stuck window has retried, and how many documents it has ever
/// given up on entirely.
const ReplayWindowState = struct {
    sequence: u64 = 0,
    consecutive_failures: u32 = 0,
    /// Cumulative documents this index has given up on across every window
    /// that has ever escalated. Never reset by recordSuccess -- this is the
    /// operator-visible "documents given up on" count, not a liveness signal.
    skipped_total: u64 = 0,
};

/// One index's cumulative replay-document-not-visible skip count, summed
/// across every owner that has an index of this name, as returned by
/// `snapshotSkipped` for /metrics. The caller owns `index_name`.
pub const IndexReplayDocumentNotVisibleSkipped = struct {
    index_name: []u8,
    count: u64,
};

/// Opaque per-owner identity for ReplayNotVisibleTracker: distinguishes two
/// storage owners (table/shard replicas, each with its own DB-lifetime
/// IndexManager) that happen to have same-named indexes -- the default
/// `full_text_index_v0` is the common case -- or that happen to be at the
/// same replay sequence by coincidence. PR #957 review: without this, the
/// tracker (keyed only by index name) let two such owners alternately reset
/// or combine each other's retry counts, so neither ever escalated.
///
/// Callers pass `replayOwnerIdFromPtr` on their IndexManager (or another
/// pointer stable for exactly that owner's lifetime). The identity is only
/// ever compared for equality within one process's lifetime, never
/// persisted or compared across restarts.
pub const ReplayOwnerId = usize;

pub fn replayOwnerIdFromPtr(ptr: anytype) ReplayOwnerId {
    return @intFromPtr(ptr);
}

/// Bounds how long the derived full-text/algebraic/dense/sparse replay
/// workers retry `error.ReplayDocumentNotVisible` for the same replay
/// window, tracked independently per (owner, index), before giving up on
/// the still-missing documents there and letting the window advance
/// without them.
///
/// Without this, a document that will never gain visible content wedges the
/// worker -- and anything waiting on it to drain, such as Lite's
/// run_until_idle() -- forever. Issue #938 is one concrete trigger (a
/// document whose only content was `_edges` was reconstructed by the replay
/// window as a synthetic full-text candidate with no primary row to find);
/// this is the backstop for that whole class of bug, not a fix for one shape
/// of document. This ResourceManager, and therefore this tracker, is shared
/// process-wide across every storage owner, so tracking must be keyed by
/// (owner, index name), not index name alone -- see ReplayOwnerId.
pub const ReplayNotVisibleTracker = struct {
    mutex: std.atomic.Mutex = .unlocked,
    owners: std.AutoHashMapUnmanaged(ReplayOwnerId, std.StringHashMapUnmanaged(ReplayWindowState)) = .empty,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        lockAtomic(&self.mutex);
        var owner_it = self.owners.valueIterator();
        while (owner_it.next()) |windows| freeWindows(alloc, windows);
        self.owners.deinit(alloc);
        self.owners = .empty;
        self.mutex.unlock();
    }

    fn freeWindows(alloc: std.mem.Allocator, windows: *std.StringHashMapUnmanaged(ReplayWindowState)) void {
        var key_it = windows.keyIterator();
        while (key_it.next()) |key_ptr| alloc.free(@constCast(key_ptr.*));
        windows.deinit(alloc);
    }

    fn getOrPutLocked(self: *@This(), alloc: std.mem.Allocator, owner: ReplayOwnerId, index_name: []const u8) ?*ReplayWindowState {
        const owner_gop = self.owners.getOrPut(alloc, owner) catch return null;
        if (!owner_gop.found_existing) owner_gop.value_ptr.* = .empty;
        const gop = owner_gop.value_ptr.getOrPut(alloc, index_name) catch return null;
        if (!gop.found_existing) {
            gop.key_ptr.* = alloc.dupe(u8, index_name) catch {
                _ = owner_gop.value_ptr.remove(index_name);
                return null;
            };
            gop.value_ptr.* = .{};
        }
        return gop.value_ptr;
    }

    /// Records one more failed attempt at `owner`'s `index_name` replay
    /// window ending at `sequence` and reports whether that (owner, index)
    /// pair has now retried more than `max_retries` times in a row without
    /// making progress. A different window on the SAME (owner, index) pair
    /// (a new sequence) resets that pair's count; a different owner or a
    /// different index name is tracked entirely independently, even if the
    /// index name or the sequence number happens to coincide.
    fn recordFailure(self: *@This(), alloc: std.mem.Allocator, owner: ReplayOwnerId, index_name: []const u8, sequence: u64, max_retries: u32) bool {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const state = self.getOrPutLocked(alloc, owner, index_name) orelse return false;
        if (state.sequence != sequence) {
            state.sequence = sequence;
            state.consecutive_failures = 0;
        }
        state.consecutive_failures +|= 1;
        return state.consecutive_failures > max_retries;
    }

    /// Clears the tracked window once it makes progress, so a later stall on
    /// the same (owner, index, sequence) is not mistaken for a continuation
    /// of an already-escalated stall. The cumulative skipped_total is
    /// untouched.
    fn recordSuccess(self: *@This(), owner: ReplayOwnerId, index_name: []const u8, sequence: u64) void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const windows = self.owners.getPtr(owner) orelse return;
        const state = windows.getPtr(index_name) orelse return;
        if (state.sequence == sequence) state.consecutive_failures = 0;
    }

    /// Records that one document in `owner`'s `index_name` was given up on
    /// (its content will never arrive) rather than indexed, for operator
    /// visibility through index status and /metrics.
    fn recordSkipped(self: *@This(), alloc: std.mem.Allocator, owner: ReplayOwnerId, index_name: []const u8) void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const state = self.getOrPutLocked(alloc, owner, index_name) orelse return;
        state.skipped_total +|= 1;
    }

    fn skippedTotal(self: *@This(), owner: ReplayOwnerId, index_name: []const u8) u64 {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const windows = self.owners.getPtr(owner) orelse return 0;
        const state = windows.getPtr(index_name) orelse return 0;
        return state.skipped_total;
    }

    fn skippedTotalAll(self: *@This()) u64 {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        var total: u64 = 0;
        var owner_it = self.owners.valueIterator();
        while (owner_it.next()) |windows| {
            var it = windows.valueIterator();
            while (it.next()) |state| total +|= state.skipped_total;
        }
        return total;
    }

    /// Snapshots skipped counts summed by index name across every owner.
    /// Owner identity is not a meaningful /metrics label -- there can be
    /// many owners, and ReplayOwnerId is not stable across restarts -- so
    /// this aggregates by index name, matching what operators scanning
    /// /metrics for "is this index losing documents" actually want. The
    /// caller frees each entry's `index_name` and the returned slice.
    fn snapshotSkipped(self: *@This(), alloc: std.mem.Allocator) ![]IndexReplayDocumentNotVisibleSkipped {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        var totals = std.StringHashMapUnmanaged(u64).empty;
        defer totals.deinit(alloc);
        var owner_it = self.owners.valueIterator();
        while (owner_it.next()) |windows| {
            var it = windows.iterator();
            while (it.next()) |entry| {
                if (entry.value_ptr.skipped_total == 0) continue;
                const gop = try totals.getOrPut(alloc, entry.key_ptr.*);
                if (!gop.found_existing) gop.value_ptr.* = 0;
                gop.value_ptr.* +|= entry.value_ptr.skipped_total;
            }
        }
        // The final size is known: reserve the output before copying names,
        // then transfer each allocation directly into an initialized entry.
        const out = try alloc.alloc(IndexReplayDocumentNotVisibleSkipped, totals.count());
        var initialized: usize = 0;
        errdefer {
            for (out[0..initialized]) |item| alloc.free(item.index_name);
            alloc.free(out);
        }
        var totals_it = totals.iterator();
        while (totals_it.next()) |entry| {
            out[initialized] = .{
                .index_name = try alloc.dupe(u8, entry.key_ptr.*),
                .count = entry.value_ptr.*,
            };
            initialized += 1;
        }
        return out;
    }

    /// Removes every tracked window for `owner` (called when its
    /// IndexManager/DB closes), so the tracker cannot grow without bound
    /// across DB/table open-close churn, and so a later owner reusing the
    /// same ReplayOwnerId (e.g. a pointer address reused by the allocator)
    /// never inherits a closed owner's retry or skip history.
    fn removeOwner(self: *@This(), alloc: std.mem.Allocator, owner: ReplayOwnerId) void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        var removed = self.owners.fetchRemove(owner) orelse return;
        freeWindows(alloc, &removed.value);
    }
};

const MutableMemory = struct {
    external_bytes: u64 = 0,
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
    host_charge: bool,
};

const BatchReservationIdentity = struct {
    prepared: bool = false,
    amounts: [slice_count]u64,
    host_charge_bytes: u64,
};

const ObserverIdentity = struct {
    current: u64,
    host_current: u64 = 0,
};

const ObserverKey = struct {
    slice: Slice,
    identity: usize,
};

/// A cache-owned, synchronous shrink callback. ResourceManager never invokes
/// it while holding either the accounting or reclaimer mutex. Unregistration
/// retires the stable slot and waits for its callback leases to drain, so an
/// owner can destroy its callback context immediately after unregister returns.
pub const ReclaimerFn = *const fn (context: *anyopaque, target_bytes: u64) u64;

const ReclaimerSlot = struct {
    identity: u64 = 0,
    slice: Slice = .hbc_node_metadata_cache,
    context: ?*anyopaque = null,
    reclaim: ?ReclaimerFn = null,
    weight: u32 = 1,
    in_flight: std.atomic.Value(u32) = .init(0),
    retiring: bool = false,
};

// Allocate slots in chunks so registry growth cannot move a callback's lease.
// Chunking also avoids a page-sized allocation for every cache owner.
const ReclaimerChunk = struct {
    credit: ?Reservation = null,
    next: ?*ReclaimerChunk = null,
    slots: [64]ReclaimerSlot = @splat(.{}),
};

const ReclaimerPointers = struct {
    allocator: std.mem.Allocator,
    entries: []*ReclaimerSlot,
    credit: ?Reservation = null,
    retirement: PayloadRetirement = .{},

    fn bytes(count: usize) !usize {
        return std.math.add(usize, @sizeOf(ReclaimerPointers), try std.math.mul(usize, count, @sizeOf(*ReclaimerSlot)));
    }
    noinline fn create(manager: *ResourceManager, slice: Slice, count: usize, control: read_admission.Port) !*ReclaimerPointers {
        var construction = try manager.reserveConstructionWithAdmission(slice, try bytes(count), control);
        defer construction.retire();
        const self = try manager.identity_allocator.create(ReclaimerPointers);
        errdefer manager.identity_allocator.destroy(self);
        const entries = try manager.identity_allocator.alloc(*ReclaimerSlot, count);
        errdefer manager.identity_allocator.free(entries);
        self.* = .{ .allocator = manager.identity_allocator, .entries = entries };
        try manager.registerPayloadRetirementWithAdmission(&self.retirement, &self.credit, &construction, self, destroy, control);
        return self;
    }
    fn destroy(raw: *anyopaque) void {
        const self: *ReclaimerPointers = @ptrCast(@alignCast(raw));
        const allocator = self.allocator;
        allocator.free(self.entries);
        allocator.destroy(self);
    }
    fn release(self: *ReclaimerPointers) void {
        self.retirement.publish();
    }
};

const ReclaimerInvocation = struct {
    slot: *ReclaimerSlot,
    context: *anyopaque,
    reclaim: ReclaimerFn,
    weight: u32,
};

pub const ReclaimerOptions = struct {
    weight: u32 = 1,
};

test "dense aggregate resource manager bounds callers and helpers together" {
    var manager = ResourceManager.init(.{ .dense_read_extra_task_limit = 4 });
    defer manager.deinit(std.testing.allocator);
    manager.dense_aggregate_admission = true;
    manager.dense_driver_admission.capacity = 2;
    var caller = try manager.dense_driver_admission.acquire(null, null);
    defer caller.release();
    try std.testing.expect(manager.tryAcquireDenseReadTask());
    try std.testing.expect(!manager.tryAcquireDenseReadTask());
    try std.testing.expectEqual(@as(u32, 1), manager.denseReadTaskStats().active);
    manager.releaseDenseReadTask();
    try std.testing.expect(manager.tryAcquireDenseReadTask());
    manager.releaseDenseReadTask();
    caller.release();
    manager.dense_driver_admission.assertIdle();
}

/// Stable ownership for managers created by embedded convenience entrypoints.
/// File and provider IO may retain this manager after their public owner closes.
pub const SharedResourceManager = struct {
    allocator: std.mem.Allocator,
    manager: *ResourceManager,
    references: std.atomic.Value(usize) = .init(1),

    pub fn create(allocator: std.mem.Allocator, options: Options) !*ResourceManager {
        const manager = try allocator.create(ResourceManager);
        errdefer allocator.destroy(manager);
        manager.* = ResourceManager.init(options);
        errdefer manager.deinit(allocator);
        if (!manager.reserveExternal(@sizeOf(SharedResourceManager))) return error.ResourceBudgetExceeded;
        errdefer manager.releaseExternal(@sizeOf(SharedResourceManager));
        const self = try allocator.create(SharedResourceManager);
        self.* = .{ .allocator = allocator, .manager = manager };
        manager.shared_owner = self;
        return manager;
    }
    pub fn retain(self: *SharedResourceManager) *SharedResourceManager {
        const previous = self.references.fetchAdd(1, .acq_rel);
        std.debug.assert(previous != 0 and previous != std.math.maxInt(usize));
        return self;
    }
    pub fn release(self: *SharedResourceManager) void {
        if (self.references.fetchSub(1, .acq_rel) != 1) return;
        const allocator = self.allocator;
        const manager = self.manager;
        manager.shared_owner = null;
        allocator.destroy(self);
        manager.releaseExternal(@sizeOf(SharedResourceManager));
        manager.deinit(allocator);
        allocator.destroy(manager);
    }
};

pub const ResourceManager = struct {
    pub fn retainLifetime(self: *ResourceManager) ?*SharedResourceManager {
        return if (self.shared_owner) |owner| owner.retain() else null;
    }
    /// Release a convenience owner or destroy a legacy independently allocated
    /// manager. Borrowed managers must never be passed to this function.
    pub fn releaseOwned(self: *ResourceManager, allocator: std.mem.Allocator) void {
        if (self.shared_owner) |owner| owner.release() else {
            self.deinit(allocator);
            allocator.destroy(self);
        }
    }

    pub const MemoryParent = @import("runtime_memory_abi").Parent;
    shared_owner: ?*SharedResourceManager = null,
    memory_parent: ?MemoryParent = null,
    // Construction claims are admitted before a local decoder starts. Slots
    // hold only accounting credit after physical failure, never a read scope.
    construction_cleanup: [64]ConstructionCleanup = @splat(.{}),
    payload_retirement_mutex: std.atomic.Mutex = .unlocked,
    payload_retirement_head: ?*PayloadRetirement = null,
    payload_retirement_cursor: ?*PayloadRetirement = null,
    cleanup_consumer_bound: std.atomic.Value(bool) = .init(false),
    cleanup_epoch: std.atomic.Value(u64) = .init(0),
    payload_retirement_ready: std.atomic.Value(usize) = .init(0),

    dense_checkpoint_ready: @import("maintenance_signal.zig").Signal = .{},
    mutex: std.atomic.Mutex = .unlocked,
    reclaimer_mutex: std.atomic.Mutex = .unlocked,
    // Slots are never compacted while the manager is live: an invocation can
    // safely retain its stable cell while its callback runs without holding the
    // registry mutex. Empty slots are reused, and the list grows on the
    // registration path instead of imposing a process-wide index-count cap.
    reclaimers: std.ArrayListUnmanaged(*ReclaimerSlot) = .empty,
    reclaimer_pointers: ?*ReclaimerPointers = null,
    reclaimer_metadata_admitted: bool = false,
    reclaimer_chunks: ?*ReclaimerChunk = null,
    next_reclaimer_identity: u64 = 1,
    reclaimer_cursor: usize = 0,
    reclaim_requests: @import("antfly_platform").atomic.Value(u64) = .init(0),
    reclaimed_bytes: @import("antfly_platform").atomic.Value(u64) = .init(0),
    hbc_benefit_sample_counter: @import("antfly_platform").atomic.Value(u64) = .init(0),
    hbc_cache_benefit: [@typeInfo(HbcCacheClass).@"enum".field_names.len]HbcCacheBenefitState = @splat(HbcCacheBenefitState{}),
    pressure_change: PressureChange = .{},
    memory: MutableMemory,
    latency_sensitive_derived_replay_sessions: @import("antfly_platform").atomic.Value(u64) = .init(0),
    latency_sensitive_derived_replay_quiet_until_ns: @import("antfly_platform").atomic.Value(u64) = .init(0),
    foreground_query_sessions: @import("antfly_platform").atomic.Value(u64) = .init(0),
    foreground_query_quiet_until_ns: @import("antfly_platform").atomic.Value(u64) = .init(0),
    foreground_write_sessions: @import("antfly_platform").atomic.Value(u64) = .init(0),
    foreground_write_quiet_until_ns: @import("antfly_platform").atomic.Value(u64) = .init(0),
    dense_search_admission_mutex: std.atomic.Mutex = .unlocked,
    dense_search_bandwidth_capacity_bytes: u64 = 0,
    dense_search_active_bytes: u64 = 0,
    dense_search_peak_active_bytes: u64 = 0,
    dense_search_active_queries: u64 = 0,
    dense_search_peak_active_queries: u64 = 0,
    dense_search_waiters: DenseSearchWaiters = .{},
    dense_search_queued_queries: u64 = 0,
    dense_search_peak_queued_queries: u64 = 0,
    dense_search_admissions: u64 = 0,
    dense_search_waits: u64 = 0,
    dense_search_cancellations: u64 = 0,
    dense_search_wait_ns: u64 = 0,
    dense_read_extra_task_limit: u32 = 0,
    dense_rerank_admission: DenseWorkAdmission.Queue = .{},
    dense_driver_admission: DenseWorkAdmission.Queue = .{},
    dense_aggregate_admission: bool = false,
    dense_phase_admission: bool = false,
    dense_scan_prediction: bool = false,
    dense_fused_no_copy: bool = false,
    dense_angular_bounds: bool = false,
    dense_quantized_routing: bool = false,
    dense_centered_routing: bool = false,
    dense_subgroup_count: u8 = 0,
    dense_global_subgroup_routing: bool = false,
    dense_compact_subgroup_routing: bool = false,
    dense_certified_subgroups: bool = false,
    dense_deferred_source_capture: bool = false,
    dense_coalesced_replay_deletes: bool = false,
    dense_reused_delete_vectors: bool = false,
    dense_stable_posting_origins: bool = false,
    dense_posting_row_deltas: bool = false,
    dense_projection_pages_enabled: bool = false,
    dense_projection_borrow_enabled: bool = false,
    dense_projection_trace_enabled: bool = false,
    dense_grouped_fallbacks: bool = false,
    dense_physical_rerank_order: bool = false,
    dense_member_bindings: bool = true,
    dense_query_snapshot: bool = false,
    dense_query_snapshot_native_only: bool = false,
    dense_configured_bulk_build: bool = false,
    dense_unified_fetch: bool = false,
    dense_exact_mapped: bool = true,
    dense_read_inline: bool = false,
    dense_read_single_helper: bool = true,
    dense_read_adaptive: bool = false,
    dense_read_profile: bool = false,
    dense_projection_pages: std.atomic.Value(?*ProjectionPageCache.Cache) = .init(null),
    dense_projection_pages_mutex: std.atomic.Mutex = .unlocked,
    dense_read_extra_tasks: std.atomic.Value(u32) = .init(0),
    dense_read_peak_extra_tasks: std.atomic.Value(u32) = .init(0),
    dense_read_denied_tasks: @import("antfly_platform").atomic.Value(u64) = .init(0),
    dense_physically_ordered_batches: @import("antfly_platform").atomic.Value(u64) = .init(0),
    dense_physically_ordered_requests: @import("antfly_platform").atomic.Value(u64) = .init(0),
    slices: [slice_count]MutableSlice,
    /// Adaptive window size in original work units, independent of storage estimates.
    dense_replay_window_budget_bytes: u64 = 0,
    dense_replay_last_finish_ns: u64 = 0,
    dense_replay_last_write_pressure_ns: u64 = 0,
    dense_replay_last_write_pressure_compactions: u64 = 0,
    derived_backlog_high_sequences: usize,
    derived_backlog_resume_sequences: usize,
    derived_backlog_throttle_window_sequences: usize,
    disk_safety_floor_bytes: u64,
    disk_safety_floor_divisor: u64,
    disk_safety_floor_max_bytes: u64,
    capacity_domains: std.AutoHashMapUnmanaged(CapacityDomainId, MutableCapacityDomain) = .empty,
    query_embedding_cache_budget: cache_budget.CacheBudget,
    query_embedding_cache_ttl_ns: u64,
    query_embedding_max_inflight: usize,
    index_repair_activation: IndexRepairActivationStats = .{},
    derived_recoverable_retry_counters: DerivedRecoverableRetryCounters = .{},
    replay_not_visible_tracker: ReplayNotVisibleTracker = .{},
    capacity_source: ?CapacitySource = null,
    identity_allocator: std.mem.Allocator,
    control_object_bytes: u64 = 0,
    identity_metadata_bytes: u64 = 0,
    identity_metadata_retirement_bytes: u64 = 0,
    identity_metadata_limit_bytes: u64,
    next_identity: u64 = 1,
    reservation_identities: IdentityLedger(u64, ReservationIdentity) = .empty,
    batch_reservation_identities: IdentityLedger(u64, BatchReservationIdentity) = .empty,
    observer_identities: IdentityLedger(ObserverKey, ObserverIdentity) = .empty,

    /// Install before publishing this owner. Existing startup charges join the
    /// parent atomically; failure leaves both ledgers unchanged.
    pub fn installMemoryParent(self: *ResourceManager, parent: MemoryParent) !void {
        if (!parent.valid()) return error.UnsupportedVersion;
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        if (parent.ptr == @as(*anyopaque, @ptrCast(self))) return error.InvalidMemoryParent;
        if (self.memory_parent) |existing| {
            if (existing.ptr == parent.ptr and existing.reserve_fn == parent.reserve_fn) return;
            return error.MemoryParentAlreadyInstalled;
        }
        if (!parent.reserve_fn(parent.ptr, self.memory.used_bytes)) return error.ResourceBudgetExceeded;
        self.memory_parent = parent;
    }

    /// Stable callback table; the manager address and its pressure event must
    /// outlive all children. Child-to-parent is the sole lock ordering.
    pub fn memoryParentPort(self: *ResourceManager) MemoryParent {
        return .{
            .ptr = self,
            .reserve_fn = ParentCallbacks.reserve,
            .try_reserve_fn = ParentCallbacks.tryReserve,
            .reclaim_fn = ParentCallbacks.reclaim,
            .reclaim_admission = &ParentCallbacks.reclaim_admission_payload,
            .observe_fn = ParentCallbacks.observe,
            .release_fn = ParentCallbacks.release,
            .try_release_fn = ParentCallbacks.tryRelease,
            .snapshot_fn = ParentCallbacks.snapshot,
            .try_snapshot_fn = ParentCallbacks.trySnapshot,
            .epoch_fn = ParentCallbacks.epoch,
            .wait_fn = ParentCallbacks.wait,
            .notify_fn = ParentCallbacks.notify,
        };
    }

    const ParentCallbacks = struct {
        fn owner(ptr: *anyopaque) *ResourceManager {
            return @ptrCast(@alignCast(ptr));
        }
        fn reserve(ptr: *anyopaque, bytes: u64) callconv(.c) bool {
            return owner(ptr).reserveExternal(bytes);
        }
        fn tryReserve(ptr: *anyopaque, bytes: u64) callconv(.c) MemoryParent.ReserveResult {
            return owner(ptr).tryReserveExternal(bytes);
        }
        fn reclaim(ptr: *anyopaque, bytes: u64) callconv(.c) u64 {
            // This requester has no local cache policy: reclaim only aggregate
            // memory from registered owners, using the normal bounded passes.
            return owner(ptr).reclaimForAllocation(.dense_apply_working_set, bytes);
        }
        const reclaim_admission_mod = @import("../common/parent_reclaim_admission.zig");
        const reclaim_admission_payload = reclaim_admission_mod.Payload{ .vtable = &.{ .reclaim = reclaimWithAdmission } };
        fn reclaimWithAdmission(ptr: *anyopaque, bytes: u64, control: read_admission.Port) !u64 {
            return owner(ptr).reclaimForAllocationControlled(.dense_apply_working_set, bytes, control);
        }
        fn observe(ptr: *anyopaque, bytes: u64) callconv(.c) void {
            owner(ptr).observeExternal(bytes);
        }
        fn release(ptr: *anyopaque, bytes: u64) callconv(.c) void {
            owner(ptr).releaseExternal(bytes);
        }
        fn tryRelease(ptr: *anyopaque, bytes: u64) callconv(.c) MemoryParent.ReleaseResult {
            return owner(ptr).tryReleaseExternal(bytes);
        }
        fn snapshot(ptr: *anyopaque, out: *@import("runtime_memory_abi").ParentSnapshot) callconv(.c) void {
            const self = owner(ptr);
            lockAtomic(&self.mutex);
            defer self.mutex.unlock();
            out.* = .{ .used_bytes = self.memory.used_bytes, .soft_limit_bytes = self.memory.budget.soft_limit_bytes, .hard_limit_bytes = self.memory.budget.hard_limit_bytes };
        }
        fn trySnapshot(ptr: *anyopaque, out: *@import("runtime_memory_abi").ParentSnapshot) callconv(.c) MemoryParent.SnapshotResult {
            const self = owner(ptr);
            if (!self.mutex.tryLock()) return .busy;
            defer self.mutex.unlock();
            out.* = .{ .used_bytes = self.memory.used_bytes, .soft_limit_bytes = self.memory.budget.soft_limit_bytes, .hard_limit_bytes = self.memory.budget.hard_limit_bytes };
            return .ready;
        }
        fn epoch(ptr: *anyopaque) callconv(.c) u64 {
            return owner(ptr).pressureEpoch();
        }
        fn wait(ptr: *anyopaque, observed: u64) callconv(.c) void {
            owner(ptr).waitForPressureChange(observed);
        }
        fn notify(ptr: *anyopaque) callconv(.c) void {
            owner(ptr).notifyPressureChange();
        }
    };

    fn reserveParentLocked(self: *ResourceManager, bytes: u64) !void {
        if (self.memory_parent) |parent| {
            if (!parent.reserve_fn(parent.ptr, bytes)) {
                self.memory.hard_limit_rejections +|= 1;
                return error.ResourceBudgetExceeded;
            }
        }
    }

    fn reserveParentWithoutWaiting(self: *ResourceManager, bytes: u64) !void {
        const parent = self.memory_parent orelse return;
        const attempt = parent.try_reserve_fn orelse return error.ExecutionWaitUnsupported;
        switch (attempt(parent.ptr, bytes)) {
            .reserved => {},
            .busy => return error.ParentMemoryBusy,
            .denied => {
                self.memory.hard_limit_rejections +|= 1;
                return error.ResourceBudgetExceeded;
            },
            .unsupported => return error.ExecutionWaitUnsupported,
        }
    }

    fn releaseParentWithoutWaiting(self: *ResourceManager, bytes: u64) !void {
        const parent = self.memory_parent orelse return;
        if (!parent.valid()) return error.ExecutionWaitUnsupported;
        const attempt = parent.try_release_fn orelse return error.ExecutionWaitUnsupported;
        switch (attempt(parent.ptr, bytes)) {
            .released => {},
            .busy => return error.ParentMemoryBusy,
            .mismatch => return error.ResourceAccountingMismatch,
            .unsupported => return error.ExecutionWaitUnsupported,
        }
    }

    pub fn controlMetadataAllocation(self: *ResourceManager, backing: std.mem.Allocator) ControlMetadataAllocation {
        return .{ .manager = self, .backing = backing };
    }

    fn identityMetadataCharge(self: *const ResourceManager) u64 {
        return self.identity_metadata_bytes + self.control_object_bytes + self.identity_metadata_retirement_bytes;
    }

    /// Physical free precedes credit return. A bounded operation must never
    /// enter the legacy parent callback: busy ancestors leave exact debt in
    /// fixed manager storage for an independent cleanup pass. New metadata
    /// allocations first retire this debt, so repeated OOM cannot accumulate
    /// additional charges beyond the finite metadata cap.
    fn retireIdentityMetadataLocked(self: *ResourceManager, bytes: u64, bounded_parent: bool) void {
        self.identity_metadata_retirement_bytes += bytes;
        self.drainIdentityMetadataRetirementLocked(bounded_parent) catch |err| {
            if (err == error.ResourceAccountingMismatch) self.memory.accounting_errors +|= 1;
        };
    }

    fn drainIdentityMetadataRetirementLocked(self: *ResourceManager, bounded_parent: bool) !void {
        const bytes = self.identity_metadata_retirement_bytes;
        if (bytes == 0) return;
        if (bounded_parent) {
            try self.releaseParentWithoutWaiting(bytes);
            self.memory.used_bytes -= bytes;
        } else {
            self.releaseMemoryLocked(bytes);
        }
        self.identity_metadata_retirement_bytes = 0;
    }

    fn tryDrainIdentityMetadataRetirement(self: *ResourceManager) bool {
        if (!self.mutex.tryLock()) return false;
        defer self.mutex.unlock();
        if (self.identity_metadata_retirement_bytes == 0) return true;
        if (!self.pressure_change.tryLock()) return false;
        defer self.pressure_change.unlock();
        self.drainIdentityMetadataRetirementLocked(true) catch return false;
        self.pressure_change.advanceLocked();
        return true;
    }

    fn releaseMemoryLocked(self: *ResourceManager, bytes: u64) void {
        self.memory.used_bytes -= bytes;
        if (self.memory_parent) |parent| parent.release_fn(parent.ptr, bytes);
    }

    fn pressureEpoch(self: *ResourceManager) u64 {
        return if (self.memory_parent) |parent| parent.epoch_fn(parent.ptr) else self.pressure_change.snapshot();
    }

    fn notifyPressureChange(self: *ResourceManager) void {
        if (self.memory_parent) |parent| parent.notify_fn(parent.ptr) else self.pressure_change.advance();
    }

    pub fn init(options: Options) ResourceManager {
        const cpu_count = if (comptime builtin.os.tag == .freestanding) 1 else std.Thread.getCpuCount() catch 1;
        var slices: [slice_count]MutableSlice = undefined;
        for (&slices, 0..) |*slice, i| {
            slice.* = .{
                .budget = options.budgets[i],
                .policy = options.policies[i],
            };
        }
        // Whole source documents are buffered in this slice before parsing, so
        // the default caps ingestible remote documents at roughly 256 MiB.
        // Operators ingesting larger sources can raise it explicitly.
        const extraction_hard = @import("antfly_platform").env.getenvU64("ANTFLY_DOCUMENT_EXTRACTION_WORKING_SET_BYTES") orelse 0;
        if (extraction_hard > 0) slices[sliceIndex(.document_extraction_working_set)].budget = .{
            .soft_limit_bytes = extraction_hard / 4 * 3,
            .hard_limit_bytes = extraction_hard,
        };
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
            .disk_safety_floor_max_bytes = options.disk_safety_floor_max_bytes,
            .query_embedding_cache_budget = cache_budget.CacheBudget.init(options.query_embedding_cache_bytes),
            .query_embedding_cache_ttl_ns = options.query_embedding_cache_ttl_ns,
            .query_embedding_max_inflight = @max(@as(usize, 1), options.query_embedding_max_inflight),
            .dense_search_bandwidth_capacity_bytes = options.dense_search_bandwidth_capacity_bytes orelse
                options.budgets[@backingInt(Slice.dense_search_working_set)].soft_limit_bytes,
            .dense_read_extra_task_limit = options.dense_read_extra_task_limit orelse
                @intCast(@min(std.math.maxInt(u32), cpu_count *| 2)),
            .dense_rerank_admission = .{ .capacity = @intCast(cpu_count) },
            .dense_driver_admission = .{ .capacity = @intCast(cpu_count) },
            .dense_aggregate_admission = dense_perf.enabled("ANTFLY_EXPERIMENT_AGGREGATE_ADMISSION"),
            .dense_query_snapshot = dense_perf.enabled("ANTFLY_EXPERIMENT_QUERY_SNAPSHOT"),
            .dense_query_snapshot_native_only = dense_perf.enabled("ANTFLY_EXPERIMENT_QUERY_SNAPSHOT_NATIVE_ONLY"),
            .dense_configured_bulk_build = dense_perf.enabled("ANTFLY_EXPERIMENT_CONFIGURED_BULK_BUILD"),
            .dense_unified_fetch = dense_perf.enabled("ANTFLY_EXPERIMENT_UNIFIED_VECTOR_FETCH"),
            .dense_exact_mapped = dense_perf.enabledDefault("ANTFLY_EXPERIMENT_EXACT_MAPPED", true),
            .dense_phase_admission = dense_perf.enabled("ANTFLY_EXPERIMENT_PHASE_ADMISSION"),
            .dense_scan_prediction = dense_perf.enabled("ANTFLY_EXPERIMENT_SCAN_PREDICTION"),
            .dense_fused_no_copy = dense_perf.enabled("ANTFLY_EXPERIMENT_FUSED_NO_COPY"),
            .dense_angular_bounds = dense_perf.enabled("ANTFLY_EXPERIMENT_ANGULAR_BOUNDS"),
            .dense_quantized_routing = dense_perf.enabled("ANTFLY_EXPERIMENT_QUANTIZED_ROUTING"),
            .dense_centered_routing = dense_perf.enabled("ANTFLY_EXPERIMENT_CENTERED_ROUTING"),
            .dense_subgroup_count = if (dense_perf.enabled("ANTFLY_EXPERIMENT_SUBGROUPS_16")) 16 else if (dense_perf.enabled("ANTFLY_EXPERIMENT_SUBGROUPS_8")) 8 else if (dense_perf.enabled("ANTFLY_EXPERIMENT_SUBGROUPS_4")) 4 else 0,
            .dense_global_subgroup_routing = dense_perf.enabled("ANTFLY_EXPERIMENT_GLOBAL_SUBGROUP_ROUTING"),
            .dense_compact_subgroup_routing = dense_perf.enabled("ANTFLY_EXPERIMENT_COMPACT_SUBGROUP_ROUTING"),
            .dense_certified_subgroups = dense_perf.enabled("ANTFLY_EXPERIMENT_CERTIFIED_SUBGROUPS"),
            .dense_deferred_source_capture = dense_perf.enabled("ANTFLY_EXPERIMENT_DEFER_SOURCE_CAPTURE"),
            .dense_coalesced_replay_deletes = dense_perf.enabled("ANTFLY_EXPERIMENT_COALESCE_REPLAY_DELETES"),
            .dense_reused_delete_vectors = dense_perf.enabled("ANTFLY_EXPERIMENT_REUSE_DELETE_VECTORS"),
            .dense_stable_posting_origins = dense_perf.enabled("ANTFLY_EXPERIMENT_STABLE_POSTING_ORIGINS"),
            .dense_posting_row_deltas = dense_perf.enabled("ANTFLY_EXPERIMENT_POSTING_ROW_DELTAS"),
            .dense_projection_pages_enabled = dense_perf.enabled("ANTFLY_EXPERIMENT_PROJECTION_PAGES"),
            .dense_projection_borrow_enabled = dense_perf.enabled("ANTFLY_EXPERIMENT_PROJECTION_BORROW"),
            .dense_projection_trace_enabled = dense_perf.enabled("ANTFLY_EXPERIMENT_PROJECTION_TRACE"),
            .dense_grouped_fallbacks = dense_perf.enabled("ANTFLY_EXPERIMENT_GROUPED_FALLBACKS"),
            .dense_physical_rerank_order = dense_perf.enabled("ANTFLY_SOURCE_VECTOR_PHYSICAL_RERANK_ORDER"),
            .dense_member_bindings = dense_perf.enabledDefault("ANTFLY_SOURCE_VECTOR_MEMBER_BINDINGS", true),
            .dense_read_inline = dense_perf.enabled("ANTFLY_EXPERIMENT_VECTOR_READ_INLINE"),
            .dense_read_single_helper = dense_perf.enabledDefault("ANTFLY_EXPERIMENT_VECTOR_READ_SINGLE_HELPER", true),
            .dense_read_adaptive = dense_perf.enabled("ANTFLY_EXPERIMENT_VECTOR_READ_ADAPTIVE"),
            .dense_read_profile = dense_perf.enabled("ANTFLY_EXPERIMENT_VECTOR_READ_PROFILE"),
            .identity_allocator = options.identity_allocator,
            .identity_metadata_limit_bytes = options.identity_metadata_limit_bytes,
        };
    }

    pub fn queryEmbeddingCacheBudget(self: *ResourceManager) *cache_budget.CacheBudget {
        return &self.query_embedding_cache_budget;
    }

    pub fn projectionPageCache(self: *ResourceManager) ?*ProjectionPageCache.Cache {
        if (!self.dense_projection_pages_enabled) return null;
        if (self.dense_projection_pages.load(.acquire)) |cache| return cache;
        if (!self.dense_projection_pages_mutex.tryLock()) return null;
        defer self.dense_projection_pages_mutex.unlock();
        if (self.dense_projection_pages.load(.acquire)) |cache| return cache;
        const cache = ProjectionPageCache.Cache.create(self) catch return null;
        self.dense_projection_pages.store(cache, .release);
        return cache;
    }

    /// Optional parallelism is nonblocking. The admitted query caller always
    /// drains its queue, so no generation/scratch lease waits for more workers.
    pub fn tryAcquireDenseReadTask(self: *ResourceManager) bool {
        var driver: DenseWorkAdmission.Queue.Lease = .{};
        if (self.dense_aggregate_admission) {
            driver = self.dense_driver_admission.tryAcquire() orelse {
                _ = self.dense_read_denied_tasks.fetchAdd(1, .monotonic);
                return false;
            };
        }
        var granted = false;
        defer if (!granted) driver.release();
        var active = self.dense_read_extra_tasks.load(.monotonic);
        while (active < self.dense_read_extra_task_limit) {
            if (self.dense_read_extra_tasks.cmpxchgWeak(active, active + 1, .acquire, .monotonic)) |updated| {
                active = updated;
            } else {
                _ = self.dense_read_peak_extra_tasks.fetchMax(active + 1, .monotonic);
                granted = true;
                return true;
            }
        }
        _ = self.dense_read_denied_tasks.fetchAdd(1, .monotonic);
        return false;
    }

    pub fn releaseDenseReadTask(self: *ResourceManager) void {
        const previous = self.dense_read_extra_tasks.fetchSub(1, .release);
        std.debug.assert(previous != 0);
        if (self.dense_aggregate_admission) {
            var driver: DenseWorkAdmission.Queue.Lease = .{ .queue = &self.dense_driver_admission };
            driver.release();
        }
    }

    pub fn denseReadTaskStats(self: *const ResourceManager) DenseReadTaskStats {
        return .{
            .limit = self.dense_read_extra_task_limit,
            .active = self.dense_read_extra_tasks.load(.monotonic),
            .peak_active = self.dense_read_peak_extra_tasks.load(.monotonic),
            .denied = self.dense_read_denied_tasks.load(.monotonic),
            .physically_ordered_batches = self.dense_physically_ordered_batches.load(.monotonic),
            .physically_ordered_requests = self.dense_physically_ordered_requests.load(.monotonic),
        };
    }

    pub fn notePhysicallyOrderedReads(self: *ResourceManager, count: usize) void {
        _ = self.dense_physically_ordered_batches.fetchAdd(1, .monotonic);
        _ = self.dense_physically_ordered_requests.fetchAdd(@intCast(count), .monotonic);
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
        return self.registerReclaimerControlled(slice, context, reclaim, options, null);
    }

    pub fn registerReclaimerWithAdmission(self: *ResourceManager, slice: Slice, context: *anyopaque, reclaim: ReclaimerFn, control: read_admission.Port) !u64 {
        return self.registerReclaimerControlled(slice, context, reclaim, .{}, control);
    }

    noinline fn registerReclaimerControlled(self: *ResourceManager, slice: Slice, context: *anyopaque, reclaim: ReclaimerFn, options: ReclaimerOptions, control: ?read_admission.Port) !u64 {
        if (control) |budget| return self.registerReclaimerAdmitted(slice, context, reclaim, options, budget);
        if (control) |budget| try @import("../common/bounded_mutex.zig").lockAtomic(&self.reclaimer_mutex, budget) else lockAtomic(&self.reclaimer_mutex);
        defer self.reclaimer_mutex.unlock();
        for (self.reclaimers.items) |slot| {
            if (control) |budget| try budget.checkpoint();
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
        if (self.reclaimer_metadata_admitted or self.reclaimer_pointers != null) {
            self.reclaimer_mutex.unlock();
            const Legacy = struct {
                fn checkpoint(_: *anyopaque) !void {}
                fn wait(_: *anyopaque) !void {
                    @import("antfly_platform").time.yieldNow();
                }
            };
            var marker: u8 = 0;
            const payload: read_admission.Payload = .{ .ptr = &marker, .vtable = &.{ .checkpoint = Legacy.checkpoint, .wait_for_progress = Legacy.wait } };
            // This branch has no admitted read borrow. Scoped callers always
            // enter the admitted path with their original Port above.
            const result = self.registerReclaimerAdmitted(slice, context, reclaim, options, read_admission.Port.init(&payload));
            lockAtomic(&self.reclaimer_mutex);
            return result;
        }
        // Reserve the pointer before allocating a chunk: failure publishes no
        // owner and leaks no chunk. Existing cells and leases remain stable.
        if (control) |budget| try budget.checkpoint();
        try self.reclaimers.ensureUnusedCapacity(self.identity_allocator, 1);
        if (control) |budget| try budget.checkpoint();
        const chunk_offset = self.reclaimers.items.len % 64;
        if (chunk_offset == 0) {
            const chunk = try self.identity_allocator.create(ReclaimerChunk);
            chunk.* = .{ .next = self.reclaimer_chunks };
            self.reclaimer_chunks = chunk;
        }
        const slot = &self.reclaimer_chunks.?.slots[chunk_offset];
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
        self.reclaimers.appendAssumeCapacity(slot);
        return identity;
    }

    /// Activate charged registry ownership before pooled server publication.
    /// Existing callback cells and pointer storage remain at their addresses.
    pub fn admitReclaimerMetadataWithAdmission(self: *ResourceManager, control: read_admission.Port) !void {
        try self.admitLegacyReclaimerMetadata(.lsm_block_table_cache, control);
    }

    noinline fn admitLegacyReclaimerMetadata(self: *ResourceManager, slice: Slice, control: read_admission.Port) !void {
        const Target = union(enum) { done, chunk: *ReclaimerChunk, pointers: []*ReclaimerSlot };
        while (true) {
            const target: Target = blk: {
                try @import("../common/bounded_mutex.zig").lockAtomic(&self.reclaimer_mutex, control);
                defer self.reclaimer_mutex.unlock();
                var link = self.reclaimer_chunks;
                while (link) |chunk| : (link = chunk.next) {
                    try control.checkpoint();
                    if (chunk.credit == null) break :blk .{ .chunk = chunk };
                }
                if (self.reclaimer_pointers == null and self.reclaimers.capacity != 0) break :blk .{ .pointers = self.reclaimers.allocatedSlice() };
                try control.checkpoint();
                self.reclaimer_metadata_admitted = true;
                break :blk .done;
            };
            switch (target) {
                .done => return,
                .chunk => |chunk| {
                    // Chunks never move or disappear before producer/callback
                    // join. Reserve outside the registry, then recheck the claim.
                    var construction = try self.reserveConstructionWithAdmission(slice, @sizeOf(ReclaimerChunk), control);
                    defer construction.retire();
                    try @import("../common/bounded_mutex.zig").lockAtomic(&self.reclaimer_mutex, control);
                    defer self.reclaimer_mutex.unlock();
                    try control.checkpoint();
                    if (chunk.credit == null) chunk.credit = construction.transfer();
                },
                .pointers => |entries| {
                    var construction = try self.reserveConstructionWithAdmission(slice, try ReclaimerPointers.bytes(entries.len), control);
                    defer construction.retire();
                    const owned = try self.identity_allocator.create(ReclaimerPointers);
                    var published = false;
                    defer if (!published) self.identity_allocator.destroy(owned);
                    owned.* = .{ .allocator = self.identity_allocator, .entries = entries };
                    try @import("../common/bounded_mutex.zig").lockAtomic(&self.reclaimer_mutex, control);
                    defer self.reclaimer_mutex.unlock();
                    // A legacy resize can invalidate only preparation. Never
                    // free or register a stale buffer from the earlier snapshot.
                    if (self.reclaimer_pointers != null or self.reclaimers.items.ptr != entries.ptr or self.reclaimers.capacity != entries.len) continue;
                    try self.registerPayloadRetirementWithAdmission(&owned.retirement, &owned.credit, &construction, owned, ReclaimerPointers.destroy, control);
                    self.reclaimer_pointers = owned;
                    published = true;
                    // Registration transfers the exact charge with no later
                    // fallible checkpoint or retained foreground scope.
                },
            }
        }
    }

    noinline fn registerReclaimerAdmitted(self: *ResourceManager, slice: Slice, context: *anyopaque, reclaim: ReclaimerFn, options: ReclaimerOptions, control: read_admission.Port) !u64 {
        try self.admitLegacyReclaimerMetadata(slice, control);
        var construction: ?ConstructionCredit = null;
        defer if (construction) |*credit| credit.retire();
        var prepared: ?*ReclaimerChunk = null;
        defer if (prepared) |chunk| self.identity_allocator.destroy(chunk);
        var pointers: ?*ReclaimerPointers = null;
        defer if (pointers) |owned| owned.release();
        var legacy_credit: ?ConstructionCredit = null;
        defer if (legacy_credit) |*credit| credit.retire();
        var legacy_ptr: ?[*]*ReclaimerSlot = null;
        var legacy_capacity: usize = 0;
        while (true) {
            try @import("../common/bounded_mutex.zig").lockAtomic(&self.reclaimer_mutex, control);
            var held = true;
            defer if (held) self.reclaimer_mutex.unlock();
            if (self.reclaimer_chunks) |head| {
                if (head.credit == null) {
                    // A legacy writer may have published a chunk after the
                    // initial adoption snapshot. Admit it before cell reuse.
                    self.reclaimer_mutex.unlock();
                    held = false;
                    try self.admitLegacyReclaimerMetadata(slice, control);
                    continue;
                }
            }
            for (self.reclaimers.items) |slot| {
                try control.checkpoint();
                if (slot.identity != 0) continue;
                const identity = self.nextReclaimerIdentity();
                slot.* = .{ .identity = identity, .slice = slice, .context = context, .reclaim = reclaim, .weight = @max(@as(u32, 1), options.weight) };
                return identity;
            }
            const offset = self.reclaimers.items.len % 64;
            if (offset == 0 and prepared == null) {
                // Admission and allocation run outside the shared registry.
                // A racing unregister may make this prepared chunk unnecessary.
                self.reclaimer_mutex.unlock();
                held = false;
                construction = try self.reserveConstructionWithAdmission(slice, @sizeOf(ReclaimerChunk), control);
                prepared = try self.identity_allocator.create(ReclaimerChunk);
                prepared.?.* = .{};
                continue;
            }
            const replace = self.reclaimer_pointers == null or self.reclaimers.items.len == self.reclaimers.capacity;
            if (replace) {
                const capacity = if (self.reclaimers.capacity == 0) 64 else try std.math.mul(usize, self.reclaimers.capacity, 2);
                if (pointers == null or pointers.?.entries.len < capacity) {
                    self.reclaimer_mutex.unlock();
                    held = false;
                    if (pointers) |owned| owned.release();
                    pointers = null;
                    pointers = try ReclaimerPointers.create(self, slice, capacity, control);
                    continue;
                }
                if (self.reclaimer_pointers == null and self.reclaimers.capacity != 0 and
                    (legacy_credit == null or legacy_ptr != self.reclaimers.items.ptr or legacy_capacity != self.reclaimers.capacity))
                {
                    // Precharge a legacy source allocation before copying and
                    // freeing it. A racing resize invalidates only this plan.
                    const old_ptr = self.reclaimers.items.ptr;
                    const old_capacity = self.reclaimers.capacity;
                    self.reclaimer_mutex.unlock();
                    held = false;
                    if (legacy_credit) |*credit| credit.retire();
                    legacy_credit = null;
                    legacy_credit = try self.reserveConstructionWithAdmission(slice, try std.math.mul(usize, old_capacity, @sizeOf(*ReclaimerSlot)), control);
                    legacy_ptr = old_ptr;
                    legacy_capacity = old_capacity;
                    continue;
                }
                for (self.reclaimers.items, 0..) |slot, i| {
                    try control.checkpoint();
                    pointers.?.entries[i] = slot;
                }
            }
            try control.checkpoint();
            const old_pointers = if (replace) self.reclaimer_pointers else null;
            const old_legacy = if (replace and self.reclaimer_pointers == null and self.reclaimers.capacity != 0) self.reclaimers.allocatedSlice() else null;
            if (replace) {
                const length = self.reclaimers.items.len;
                const owned = pointers.?;
                self.reclaimers = .empty;
                self.reclaimers.items = owned.entries[0..length];
                self.reclaimers.capacity = owned.entries.len;
                self.reclaimer_pointers = owned;
                pointers = null;
            }
            if (offset == 0) {
                const chunk = prepared.?;
                chunk.credit = construction.?.transfer();
                chunk.next = self.reclaimer_chunks;
                self.reclaimer_chunks = chunk;
                prepared = null;
            }
            const slot = &self.reclaimer_chunks.?.slots[offset];
            const identity = self.nextReclaimerIdentity();
            slot.* = .{ .identity = identity, .slice = slice, .context = context, .reclaim = reclaim, .weight = @max(@as(u32, 1), options.weight) };
            self.reclaimers.appendAssumeCapacity(slot);
            // All fallible authority checks precede publication. Physical
            // retirement runs outside the registry, with its credit still owned.
            self.reclaimer_mutex.unlock();
            held = false;
            if (old_pointers) |owned| owned.release();
            if (old_legacy) |entries| self.identity_allocator.free(entries);
            return identity;
        }
    }

    fn nextReclaimerIdentity(self: *ResourceManager) u64 {
        const identity = self.next_reclaimer_identity;
        self.next_reclaimer_identity +%= 1;
        if (self.next_reclaimer_identity == 0) self.next_reclaimer_identity = 1;
        return identity;
    }

    pub fn qualifyReclaimerInitialBytes() usize {
        if (!builtin.is_test) @compileError("qualification requires a test build");
        return @sizeOf(ReclaimerChunk) + (ReclaimerPointers.bytes(64) catch unreachable);
    }

    pub fn qualifyReclaimerChunkBytes() usize {
        if (!builtin.is_test) @compileError("qualification requires a test build");
        return @sizeOf(ReclaimerChunk);
    }

    /// Fence new callback leases, then lend the caller's CPU while existing
    /// invocations retire. A rejected wait retains the fenced slot and callback
    /// context; the owner must retry before destroying either. No slot pointer
    /// escapes the registry lock: another unregister may retire and reuse it.
    pub fn unregisterReclaimerWithAdmission(self: *ResourceManager, identity: u64, io: std.Io, original: read_admission.Port) !void {
        if (identity == 0) return;
        const wait = @import("../common/cleanup_wait.zig");
        var trace: read_admission.RejectionTrace = .{};
        const control = trace.bind(original).?;
        while (true) {
            const pending = blk: {
                try wait.lockAtomic(&self.reclaimer_mutex, io, control);
                defer self.reclaimer_mutex.unlock();
                for (self.reclaimers.items) |slot| {
                    // Lifetime checks cannot lend execution under this guard.
                    // A large registry still respects the original deadline.
                    try control.checkLifetime();
                    if (slot.identity != identity) continue;
                    slot.retiring = true;
                    const busy = slot.in_flight.load(.acquire) != 0;
                    if (!busy) slot.* = .{};
                    break :blk busy;
                }
                break :blk false;
            };
            if (!pending) return;
            try wait.waitForRetry(io, control);
        }
    }

    pub fn unregisterReclaimer(self: *ResourceManager, identity: u64) void {
        if (identity == 0) return;
        while (true) {
            lockAtomic(&self.reclaimer_mutex);
            var found = false;
            for (self.reclaimers.items) |slot| {
                if (slot.identity != identity) continue;
                found = true;
                slot.retiring = true;
                if (slot.in_flight.load(.acquire) == 0) slot.* = .{};
                break;
            }
            self.reclaimer_mutex.unlock();
            if (!found) return;
            if (comptime builtin.os.tag == .freestanding) {
                std.atomic.spinLoopHint();
            } else {
                @import("antfly_platform").time.yieldNow();
            }
        }
    }

    fn releaseReclaimerInvocation(self: *ResourceManager, invocation: ReclaimerInvocation) void {
        _ = self;
        // This is mandatory cleanup, independent of admission and the registry
        // mutex. Unregistration's acquire load observes callback completion.
        // Do not touch the cell after decrement: the last lease permits reuse.
        const previous = invocation.slot.in_flight.fetchSub(1, .release);
        std.debug.assert(previous > 0);
    }

    // Callback return values can include logical slice credit. Only ledger
    // deficits establish whether both host and requester admission now fit.
    fn allocationReclaimDebt(self: *ResourceManager, requester: Slice, additional_bytes: u64) struct { total: u64, slice: u64 } {
        return self.allocationReclaimDebtControlled(requester, additional_bytes, null) catch unreachable;
    }

    fn allocationReclaimDebtControlled(self: *ResourceManager, requester: Slice, additional_bytes: u64, control: ?read_admission.Port) !struct { total: u64, slice: u64 } {
        // Pressure targets are advisory; reservation remains authoritative.
        // Obtain the parent observation before acquiring the child ledger so
        // original-budget progress cannot reenter a held child accounting lock.
        const admitted_parent_snapshot = if (control) |budget|
            if (self.memory_parent) |parent| try snapshotParentWithAdmission(parent, budget) else null
        else
            null;
        return blk: {
            if (control) |budget| try @import("../common/bounded_mutex.zig").lockAtomic(&self.mutex, budget) else lockAtomic(&self.mutex);
            defer self.mutex.unlock();
            const parent_snapshot = if (control != null) admitted_parent_snapshot else if (self.memory_parent) |parent| parent.snapshot() else null;
            const memory_hard = if (parent_snapshot) |parent| parent.hard_limit_bytes else self.memory.budget.hard_limit_bytes;
            const memory_target = if (memory_hard == 0)
                0
            else
                ((if (parent_snapshot) |parent| parent.used_bytes else self.memory.used_bytes) +| additional_bytes) -| memory_hard;
            const requester_state = &self.slices[sliceIndex(requester)];
            const slice_hard = requester_state.budget.hard_limit_bytes;
            // These slices mix active allocations with reclaimable retained
            // state. Satisfy their slice pressure before aggregate pressure:
            // evicting an unrelated cache cannot raise this slice's ceiling.
            const requester_slice_reclaimable = requester == .dense_search_working_set or
                requester == .hbc_node_metadata_cache or
                requester == .relational_preparation_working_set or
                requester == .document_extraction_working_set or
                requester == .lite_native_page_cache or requester == .lite_docstore_snapshot_cache;
            const slice_target = if (!requester_slice_reclaimable or slice_hard == 0)
                0
            else
                (requester_state.used_bytes +| additional_bytes) -| slice_hard;
            const target = @max(memory_target, slice_target);
            break :blk .{ .total = target, .slice = slice_target };
        };
    }

    /// Reclaim enough aggregate cache memory to make one allocation retry
    /// meaningful. Callbacks publish their decreases through accounting APIs;
    /// logical slice relief never substitutes for required host memory relief.
    pub fn reclaimForAllocation(self: *ResourceManager, requester: Slice, additional_bytes: u64) u64 {
        return self.reclaimForAllocationControlled(requester, additional_bytes, null) catch unreachable;
    }

    fn reclaimForAllocationControlled(self: *ResourceManager, requester: Slice, additional_bytes: u64, control: ?read_admission.Port) !u64 {
        const local_reclaimed = try self.reclaimLocalForAllocationControlled(requester, additional_bytes, control);
        const parent = self.memory_parent orelse return local_reclaimed;
        const fits_local = blk: {
            if (control) |budget| try @import("../common/bounded_mutex.zig").lockAtomic(&self.mutex, budget) else lockAtomic(&self.mutex);
            defer self.mutex.unlock();
            const state = self.slices[sliceIndex(requester)];
            break :blk (state.budget.hard_limit_bytes == 0 or state.used_bytes +| additional_bytes <= state.budget.hard_limit_bytes) and
                (self.memory.budget.hard_limit_bytes == 0 or self.memory.used_bytes +| additional_bytes <= self.memory.budget.hard_limit_bytes);
        };
        // Parent eviction cannot repair a child ceiling. Never call a cache
        // owner while holding the child's accounting or reclaimer mutex.
        if (!fits_local) return local_reclaimed;
        if (control) |budget| {
            try budget.checkpoint();
            const payload = parent.reclaim_admission orelse return error.ExecutionWaitUnsupported;
            return local_reclaimed +| try @import("../common/parent_reclaim_admission.zig").reclaim(payload, parent.ptr, additional_bytes, budget);
        }
        return local_reclaimed +| parent.reclaim_fn(parent.ptr, additional_bytes);
    }

    fn reclaimLocalForAllocation(self: *ResourceManager, requester: Slice, additional_bytes: u64) u64 {
        return self.reclaimLocalForAllocationControlled(requester, additional_bytes, null) catch unreachable;
    }

    fn snapshotParentWithAdmission(parent: MemoryParent, control: read_admission.Port) !@import("runtime_memory_abi").ParentSnapshot {
        if (!parent.valid()) return error.UnsupportedVersion;
        const attempt = parent.try_snapshot_fn orelse return error.ExecutionWaitUnsupported;
        while (true) {
            try control.checkpoint();
            var pressure_snapshot: @import("runtime_memory_abi").ParentSnapshot = undefined;
            switch (attempt(parent.ptr, &pressure_snapshot)) {
                .ready => {
                    try control.checkpoint();
                    return pressure_snapshot;
                },
                .busy => try control.waitForProgress(),
                .unsupported => return error.ExecutionWaitUnsupported,
            }
        }
    }

    fn reclaimLocalForAllocationControlled(self: *ResourceManager, requester: Slice, additional_bytes: u64, control: ?read_admission.Port) !u64 {
        if (additional_bytes == 0) return 0;
        const targets = try self.allocationReclaimDebtControlled(requester, additional_bytes, control);
        if (targets.total == 0) return 0;
        var debt = targets;

        _ = self.reclaim_requests.fetchAdd(1, .monotonic);
        var reclaimed: u64 = 0;
        defer _ = self.reclaimed_bytes.fetchAdd(reclaimed, .monotonic);
        if (control) |budget| try @import("../common/bounded_mutex.zig").lockAtomic(&self.reclaimer_mutex, budget) else lockAtomic(&self.reclaimer_mutex);
        const scan_len = self.reclaimers.items.len;
        if (scan_len == 0) {
            self.reclaimer_mutex.unlock();
            return 0;
        }
        const start_cursor = self.reclaimer_cursor % scan_len;
        self.reclaimer_cursor = (start_cursor + 1) % scan_len;
        // A registration can reuse an empty slot while callbacks execute.
        // Restrict this bounded pass to identities that existed at its start,
        // so the pressure path neither revisits a slot nor allocates memory.
        const identity_cutoff = self.next_reclaimer_identity -% 1;
        self.reclaimer_mutex.unlock();
        // Idle search scratch is cheaper to reconstruct than cached index or
        // table content. Include the requester's own search slice so one index
        // can reuse capacity retained by another; callbacks use non-blocking
        // owner locks and therefore skip an in-flight scratch handle safely.
        const candidates = [_]Slice{ requester, .dense_search_working_set, .document_extraction_working_set, .relational_preparation_working_set, .hbc_node_metadata_cache, .lsm_block_table_cache, .lite_native_page_cache, .lite_docstore_snapshot_cache };
        for (candidates, 0..) |candidate_slice, candidate_index| {
            if (control) |budget| try budget.checkpoint();
            if (candidate_index == 0) {
                if (targets.slice == 0) continue;
            } else if (targets.slice != 0) {
                // The first pass is the only one that can resolve local
                // pressure. Do not discard useful unrelated cache contents
                // when live owners prevented that pass from making progress.
                if (debt.slice != 0) break;
                if (candidate_slice == requester) continue;
            }
            if (candidate_slice == requester and
                candidate_slice != .dense_search_working_set and
                candidate_slice != .hbc_node_metadata_cache and
                candidate_slice != .relational_preparation_working_set and
                candidate_slice != .document_extraction_working_set and
                candidate_slice != .lite_native_page_cache and candidate_slice != .lite_docstore_snapshot_cache) continue;
            // Preserve weighted fairness on the first pass. If some owners
            // cannot use their share, offer the remaining debt once more to
            // available owners before rejecting otherwise admissible work.
            // Both passes use the original identity fence and non-blocking
            // callbacks; no allocation or unbounded pressure wait is added.
            for (0..2) |pass| {
                const before_pass = reclaimed;
                var divided_target = false;
                var remaining_weight: u64 = 0;
                if (control) |budget| try @import("../common/bounded_mutex.zig").lockAtomic(&self.reclaimer_mutex, budget) else lockAtomic(&self.reclaimer_mutex);
                for (0..scan_len) |offset| {
                    const index = (start_cursor + offset) % scan_len;
                    const slot = self.reclaimers.items[index];
                    if (slot.identity == 0 or slot.identity > identity_cutoff or
                        slot.retiring or slot.slice != candidate_slice) continue;
                    remaining_weight +|= slot.weight;
                }
                self.reclaimer_mutex.unlock();

                for (0..scan_len) |offset| {
                    if (control) |budget| try @import("../common/bounded_mutex.zig").lockAtomic(&self.reclaimer_mutex, budget) else lockAtomic(&self.reclaimer_mutex);
                    const index = (start_cursor + offset) % scan_len;
                    const slot = self.reclaimers.items[index];
                    if (slot.identity == 0 or slot.identity > identity_cutoff or
                        slot.retiring or slot.slice != candidate_slice)
                    {
                        self.reclaimer_mutex.unlock();
                        continue;
                    }
                    const invocation = ReclaimerInvocation{
                        .slot = slot,
                        .context = slot.context.?,
                        .reclaim = slot.reclaim.?,
                        .weight = slot.weight,
                    };
                    const previous = slot.in_flight.fetchAdd(1, .monotonic);
                    std.debug.assert(previous != std.math.maxInt(u32));
                    const remaining_target = debt.total;
                    if (pass == 0 and remaining_weight > invocation.weight) divided_target = true;
                    const fair_target = if (pass != 0 or remaining_weight <= invocation.weight)
                        remaining_target
                    else
                        @max(@as(u64, 1), mulDivSaturating(remaining_target, invocation.weight, remaining_weight));
                    remaining_weight -|= invocation.weight;
                    self.reclaimer_mutex.unlock();

                    reclaimed +|= invocation.reclaim(invocation.context, fair_target);
                    self.releaseReclaimerInvocation(invocation);
                    if (control) |budget| try budget.checkpoint();
                    debt = try self.allocationReclaimDebtControlled(requester, additional_bytes, control);
                    if (debt.total == 0) break;
                }
                if (debt.total == 0 or reclaimed == before_pass or !divided_target) break;
            }
            if (debt.total == 0) break;
        }
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

    /// Foreground searches share storage bandwidth with LSM compaction and
    /// whole-HBC checkpoint construction. Optional maintenance yields while a
    /// query is active and for a short coalescing interval afterward. Hard WAL
    /// and write-pressure work deliberately does not consult this signal.
    pub fn beginForegroundQuery(self: *ResourceManager) void {
        _ = self.foreground_query_sessions.fetchAdd(1, .release);
    }

    pub fn finishForegroundQuery(self: *ResourceManager) void {
        const previous = self.foreground_query_sessions.fetchSub(1, .acq_rel);
        std.debug.assert(previous > 0);
        if (previous == 1) {
            self.foreground_query_quiet_until_ns.store(
                platform_time.monotonicNs() +| foreground_query_maintenance_quiet_ns,
                .release,
            );
        }
    }

    pub const ForegroundWriteLease = struct {
        manager: ?*ResourceManager = null,

        pub fn release(self: *ForegroundWriteLease) void {
            const manager = self.manager orelse return;
            const previous = manager.foreground_write_sessions.fetchSub(1, .acq_rel);
            std.debug.assert(previous > 0);
            if (previous == 1) {
                manager.foreground_write_quiet_until_ns.store(
                    platform_time.monotonicNs() +| foreground_write_maintenance_quiet_ns,
                    .release,
                );
            }
            self.manager = null;
        }
    };

    /// Marks only the latency-sensitive mutation portion of a request. Callers
    /// release this lease immediately after primary WAL publication, before
    /// any explicit full-index visibility wait.
    pub fn beginForegroundWrite(self: *ResourceManager) ForegroundWriteLease {
        _ = self.foreground_write_sessions.fetchAdd(1, .release);
        return .{ .manager = self };
    }

    fn denseSearchGrantBytes(self: *const ResourceManager, requested_bytes: u64) u64 {
        const capacity = self.dense_search_bandwidth_capacity_bytes;
        if (capacity == 0) return 0;
        return @min(@max(requested_bytes, 1), capacity);
    }

    fn canGrantDenseSearchLocked(self: *const ResourceManager, bytes: u64) bool {
        return bytes <= self.dense_search_bandwidth_capacity_bytes -| self.dense_search_active_bytes;
    }

    fn noteDenseSearchGrantLocked(self: *ResourceManager, bytes: u64) void {
        self.dense_search_active_bytes += bytes;
        self.dense_search_active_queries += 1;
        self.dense_search_peak_active_bytes = @max(self.dense_search_peak_active_bytes, self.dense_search_active_bytes);
        self.dense_search_peak_active_queries = @max(self.dense_search_peak_active_queries, self.dense_search_active_queries);
        self.dense_search_admissions +|= 1;
    }

    fn admitDenseSearchWaitersLocked(self: *ResourceManager) void {
        while (self.dense_search_waiters.head) |head| {
            if (!self.canGrantDenseSearchLocked(head.payload.bytes)) return;
            const waiter = self.dense_search_waiters.pop().?;
            self.dense_search_queued_queries -|= 1;
            self.noteDenseSearchGrantLocked(waiter.payload.bytes);
            self.dense_search_wait_ns +|= platform_time.monotonicNs() -| waiter.payload.enqueued_ns;
            waiter.handoff.publish();
        }
    }

    fn cancelDenseSearchWaiter(self: *ResourceManager, waiter: *DenseSearchWaiter) void {
        lockAtomic(&self.dense_search_admission_mutex);
        defer self.dense_search_admission_mutex.unlock();
        if (waiter.handoff.isAdmitted()) {
            // Cancellation was observed before grant but lost the queue-lock
            // race. No lease escapes: return its charge here exactly once.
            std.debug.assert(self.dense_search_active_queries > 0);
            std.debug.assert(self.dense_search_active_bytes >= waiter.payload.bytes);
            self.dense_search_active_queries -= 1;
            self.dense_search_active_bytes -= waiter.payload.bytes;
        } else {
            const removed = self.dense_search_waiters.remove(waiter);
            std.debug.assert(removed);
            self.dense_search_queued_queries -|= 1;
            self.dense_search_wait_ns +|= platform_time.monotonicNs() -| waiter.payload.enqueued_ns;
        }
        self.dense_search_cancellations +|= 1;
        self.admitDenseSearchWaitersLocked();
    }

    /// Acquire a fair node-wide permit for the estimated candidate bytes one
    /// dense search will scan. Waiters must not hold mutable index/cache locks
    /// or a publication fence. Native two-phase searches may retain immutable
    /// generation leases and separately budgeted request scratch. Oversized searches consume the
    /// whole budget and therefore retain correctness progress without letting
    /// smaller requests starve them indefinitely.
    pub fn acquireDenseSearchBandwidth(
        self: *ResourceManager,
        requested_bytes: u64,
        cancellation: ?DenseSearchCancellation,
        wait_io: ?std.Io,
    ) !DenseSearchAdmissionLease {
        const bytes = self.denseSearchGrantBytes(requested_bytes);
        if (bytes == 0) return .{};
        if (cancellation) |token| {
            if (token.is_cancelled(token.ptr)) return error.Cancelled;
        }

        lockAtomic(&self.dense_search_admission_mutex);
        if (self.dense_search_waiters.head == null and self.canGrantDenseSearchLocked(bytes)) {
            self.noteDenseSearchGrantLocked(bytes);
            self.dense_search_admission_mutex.unlock();
            return .{ .manager = self, .bytes = bytes };
        }
        var waiter = DenseSearchWaiter{
            .payload = .{ .bytes = bytes, .enqueued_ns = platform_time.monotonicNs() },
            .handoff = .{ .io = wait_io },
        };
        self.dense_search_waiters.enqueue(&waiter);
        self.dense_search_queued_queries += 1;
        self.dense_search_peak_queued_queries = @max(self.dense_search_peak_queued_queries, self.dense_search_queued_queries);
        self.dense_search_waits +|= 1;
        self.dense_search_admission_mutex.unlock();

        waiter.handoff.wait(cancellation) catch |err| {
            self.cancelDenseSearchWaiter(&waiter);
            return err;
        };
        return .{ .manager = self, .bytes = bytes };
    }

    fn releaseDenseSearchBandwidth(self: *ResourceManager, bytes: u64) void {
        if (bytes == 0) return;
        lockAtomic(&self.dense_search_admission_mutex);
        std.debug.assert(self.dense_search_active_queries > 0);
        std.debug.assert(self.dense_search_active_bytes >= bytes);
        self.dense_search_active_queries -= 1;
        self.dense_search_active_bytes -= bytes;
        self.admitDenseSearchWaitersLocked();
        self.dense_search_admission_mutex.unlock();
    }

    pub fn denseSearchAdmissionStats(self: *ResourceManager) DenseSearchAdmissionStats {
        lockAtomic(&self.dense_search_admission_mutex);
        defer self.dense_search_admission_mutex.unlock();
        return .{
            .capacity_bytes = self.dense_search_bandwidth_capacity_bytes,
            .active_bytes = self.dense_search_active_bytes,
            .peak_active_bytes = self.dense_search_peak_active_bytes,
            .active_queries = self.dense_search_active_queries,
            .peak_active_queries = self.dense_search_peak_active_queries,
            .queued_queries = self.dense_search_queued_queries,
            .peak_queued_queries = self.dense_search_peak_queued_queries,
            .admissions = self.dense_search_admissions,
            .waits = self.dense_search_waits,
            .cancellations = self.dense_search_cancellations,
            .wait_ns = self.dense_search_wait_ns,
        };
    }

    pub fn shouldDeferOptionalMaintenanceForForegroundTraffic(self: *const ResourceManager) bool {
        if (self.foreground_query_sessions.load(.acquire) != 0) return true;
        return platform_time.monotonicNs() < self.foreground_query_quiet_until_ns.load(.acquire) or
            self.shouldDeferPostingRefreshForForegroundWrites();
    }

    /// A small posting refresh admitted through the DB's nonblocking apply
    /// fence may use a gap between queries without waiting for a query-quiet
    /// interval that a steady serving workload might never provide.
    pub fn shouldDeferPostingRefreshForForegroundWrites(self: *const ResourceManager) bool {
        if (self.foreground_write_sessions.load(.acquire) != 0) return true;
        return platform_time.monotonicNs() < self.foreground_write_quiet_until_ns.load(.acquire);
    }

    /// Soft compaction builders call this at bounded work intervals. Each
    /// pause is capped: a compaction selected before hard pressure developed
    /// must continue making progress even under a sustained query stream.
    pub fn yieldOptionalMaintenanceForForegroundQuery(self: *const ResourceManager) void {
        const deadline_ns = platform_time.monotonicNs() +| foreground_query_compaction_yield_max_ns;
        while (self.shouldDeferOptionalMaintenanceForForegroundTraffic()) {
            if (platform_time.monotonicNs() >= deadline_ns) return;
            platform_time.yieldBriefly();
        }
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

    /// Reports whether replay of `owner`'s `index_name` window ending at
    /// `sequence` has now failed with `error.ReplayDocumentNotVisible` more
    /// times in a row than `ANTFLY_REPLAY_DOCUMENT_NOT_VISIBLE_MAX_RETRIES`
    /// allows. The caller is expected to treat this as "stop waiting for the
    /// still-missing documents in this window" rather than retrying again --
    /// see ReplayNotVisibleTracker's doc comment. `owner` must distinguish
    /// this storage owner from any other that might have a same-named index
    /// or coincidentally be at the same sequence (see ReplayOwnerId).
    pub fn shouldEscalateReplayDocumentNotVisible(self: *ResourceManager, owner: ReplayOwnerId, index_name: []const u8, sequence: u64) bool {
        return self.replay_not_visible_tracker.recordFailure(self.identity_allocator, owner, index_name, sequence, replayDocumentNotVisibleMaxRetries());
    }

    /// Clears escalation tracking for `owner`'s `index_name` window ending
    /// at `sequence` once it has actually applied successfully.
    pub fn clearReplayDocumentNotVisibleEscalation(self: *ResourceManager, owner: ReplayOwnerId, index_name: []const u8, sequence: u64) void {
        self.replay_not_visible_tracker.recordSuccess(owner, index_name, sequence);
    }

    /// Records that one document in `owner`'s `index_name` was given up on
    /// by replay (its content will never arrive) rather than indexed.
    /// Surfaced through
    /// `replayDocumentNotVisibleSkippedTotal`/`...All`/`snapshotReplayDocumentNotVisibleSkipped`
    /// for index status and /metrics.
    pub fn recordReplayDocumentNotVisibleSkipped(self: *ResourceManager, owner: ReplayOwnerId, index_name: []const u8) void {
        self.replay_not_visible_tracker.recordSkipped(self.identity_allocator, owner, index_name);
    }

    /// Cumulative documents `owner`'s `index_name` has ever given up on via
    /// bounded replay-document-not-visible escalation.
    pub fn replayDocumentNotVisibleSkippedTotal(self: *ResourceManager, owner: ReplayOwnerId, index_name: []const u8) u64 {
        return self.replay_not_visible_tracker.skippedTotal(owner, index_name);
    }

    /// Sum of `replayDocumentNotVisibleSkippedTotal` across every owner and
    /// index, for a single process-wide /metrics counter.
    pub fn replayDocumentNotVisibleSkippedTotalAll(self: *ResourceManager) u64 {
        return self.replay_not_visible_tracker.skippedTotalAll();
    }

    /// Per-index breakdown (summed across owners) for a labeled /metrics
    /// sample. The caller frees each entry's `index_name` and the returned
    /// slice.
    pub fn snapshotReplayDocumentNotVisibleSkipped(self: *ResourceManager, alloc: std.mem.Allocator) ![]IndexReplayDocumentNotVisibleSkipped {
        return self.replay_not_visible_tracker.snapshotSkipped(alloc);
    }

    /// Removes every tracked replay-document-not-visible window and skip
    /// count for `owner`. Callers must invoke this when the owner (its
    /// IndexManager/DB) closes, so this process-wide tracker cannot grow
    /// without bound across DB/table open-close churn.
    pub fn removeReplayNotVisibleOwner(self: *ResourceManager, owner: ReplayOwnerId) void {
        self.replay_not_visible_tracker.removeOwner(self.identity_allocator, owner);
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
        std.debug.assert(!self.cleanup_consumer_bound.load(.acquire));
        std.debug.assert(self.memory.external_bytes == 0);
        if (self.control_object_bytes != 0) @panic("resource manager closed with live control metadata objects");
        self.dense_checkpoint_ready.assertUnbound();
        self.dense_rerank_admission.assertIdle();
        self.dense_driver_admission.assertIdle();
        if (self.dense_projection_pages.load(.acquire)) |cache| cache.deinit();
        if (self.dense_read_extra_tasks.load(.acquire) != 0)
            @panic("resource manager deinitialized with active dense read workers");
        lockAtomic(&self.dense_search_admission_mutex);
        if (self.dense_search_active_queries != 0 or self.dense_search_waiters.head != null)
            @panic("resource manager deinitialized with active dense search admission");
        self.dense_search_admission_mutex.unlock();

        _ = alloc;
        lockAtomic(&self.reclaimer_mutex);
        for (self.reclaimers.items) |slot| {
            if (slot.identity != 0 or slot.in_flight.load(.acquire) != 0)
                @panic("resource manager deinitialized with live reclaimers");
        }
        if (self.reclaimer_pointers) |owned| owned.release() else self.reclaimers.deinit(self.identity_allocator);
        self.reclaimer_pointers = null;
        self.reclaimers = .empty;
        while (self.reclaimer_chunks) |chunk| {
            self.reclaimer_chunks = chunk.next;
            var credit = chunk.credit;
            self.identity_allocator.destroy(chunk);
            if (credit) |*owned| owned.release();
        }
        self.reclaimer_mutex.unlock();

        // Quiescent teardown: producers and admitted cleanup have joined.
        while (self.payload_retirement_head) |node| {
            if (!node.ready.load(.acquire)) @panic("resource manager closed with live payload owners");
            var credit = node.credit.?.*;
            self.unlinkPayloadRetirement(node);
            const destroy = node.destroy.?;
            const context = node.context.?;
            destroy(context);
            _ = self.payload_retirement_ready.fetchSub(1, .acq_rel);
            credit.release();
        }
        for (&self.construction_cleanup) |*slot| {
            switch (slot.state.load(.acquire)) {
                .free => {},
                .ready => {
                    slot.credit.?.release();
                    slot.credit = null;
                    slot.state.store(.free, .release);
                },
                else => @panic("resource manager closed with live construction cleanup"),
            }
        }

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
        self.drainIdentityMetadataRetirementLocked(false) catch unreachable;
        self.capacity_domains.deinit(self.identity_allocator);
        self.capacity_domains = .empty;
        self.reservation_identities.deinit(self);
        self.reservation_identities = .empty;
        self.batch_reservation_identities.deinit(self);
        self.batch_reservation_identities = .empty;
        self.observer_identities.deinit(self);
        self.observer_identities = .empty;
        // Observations end with their backing owners; strict leases were
        // checked above. Return the terminal observer balance to the parent.
        self.releaseMemoryLocked(self.memory.used_bytes);
        self.memory_parent = null;
        self.replay_not_visible_tracker.deinit(self.identity_allocator);
        self.replay_not_visible_tracker = .{};
    }

    pub fn reserveCapacity(
        self: *ResourceManager,
        alloc: std.mem.Allocator,
        domain_id: CapacityDomainId,
        bytes: u64,
        observation: CapacityObservation,
        now_ns: u64,
    ) !CapacityReservation {
        _ = alloc;
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const entry = try self.capacity_domains.getOrPut(self.identity_allocator, domain_id);
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
                var proportional = capacity / self.disk_safety_floor_divisor;
                if (self.disk_safety_floor_max_bytes != 0) {
                    proportional = @min(proportional, self.disk_safety_floor_max_bytes);
                }
                floor = @max(floor, proportional);
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
        var normalized = @as([slice_count]u64, @splat(0));
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

    /// Allocation-free accounting for a stable external allocator owner.
    /// Includes API allocations in the host ceiling enforced by this manager.
    /// The caller owns exact single-release accounting, not a replayable handle.
    pub fn reserveExternal(self: *ResourceManager, bytes: u64) bool {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const next = self.checkMemoryLocked(bytes) catch return false;
        self.reserveParentLocked(bytes) catch return false;
        self.commitMemoryLocked(next);
        self.memory.external_bytes += bytes;
        return true;
    }

    /// Try the complete child-to-parent reservation chain without waiting.
    /// Ancestors commit first; every local commit thereafter is infallible.
    /// A rejected/busy chain leaves all capacity ownership unchanged.
    pub fn tryReserveExternal(self: *ResourceManager, bytes: u64) MemoryParent.ReserveResult {
        if (!self.mutex.tryLock()) return .busy;
        defer self.mutex.unlock();
        // Reserve notification serialization before any ancestor commits.
        if (!self.pressure_change.tryLock()) return .busy;
        defer self.pressure_change.unlock();
        const next = self.checkMemoryLocked(bytes) catch return .denied;
        if (self.memory_parent) |parent| {
            if (!parent.valid()) return .unsupported;
            const attempt = parent.try_reserve_fn orelse return .unsupported;
            const result = attempt(parent.ptr, bytes);
            if (result != .reserved) return result;
        }
        self.commitMemoryLocked(next);
        self.memory.external_bytes += bytes;
        self.pressure_change.advanceLocked();
        return .reserved;
    }

    /// Try an exact external-charge release through all ancestors. Physical
    /// bytes must already be retired. Validation and try-lock acquisition
    /// precede any release; ancestors commit first, then local debit cannot fail.
    /// No allocator, reclaim callback, blocking accounting or partial debit.
    pub fn tryReleaseExternal(self: *ResourceManager, bytes: u64) MemoryParent.ReleaseResult {
        if (!self.mutex.tryLock()) return .busy;
        defer self.mutex.unlock();
        // Reserve notification serialization before any ancestor commits.
        if (!self.pressure_change.tryLock()) return .busy;
        defer self.pressure_change.unlock();
        if (bytes > self.memory.external_bytes or bytes > self.memory.used_bytes -| self.identityMetadataCharge()) return .mismatch;
        if (self.memory_parent) |parent| {
            if (!parent.valid()) return .unsupported;
            const attempt = parent.try_release_fn orelse return .unsupported;
            const result = attempt(parent.ptr, bytes);
            if (result != .released) return result;
        }
        self.memory.external_bytes -= bytes;
        self.memory.used_bytes -= bytes;
        self.pressure_change.advanceLocked();
        return .released;
    }

    /// Observation reports existing memory even above a ceiling. It is never
    /// used to authorize allocation; strict callers use reserveExternal.
    fn observeExternal(self: *ResourceManager, bytes: u64) void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const next = std.math.add(u64, self.memory.used_bytes, bytes) catch @panic("external memory accounting overflow");
        if (self.memory_parent) |parent| parent.observe_fn(parent.ptr, bytes);
        self.commitMemoryLocked(next);
        self.memory.external_bytes += bytes;
        if (self.memory.budget.hard_limit_bytes != 0 and next > self.memory.budget.hard_limit_bytes) self.memory.hard_limit_rejections +|= 1;
        self.notifyPressureChange();
    }

    pub fn releaseExternal(self: *ResourceManager, bytes: u64) void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        std.debug.assert(bytes <= self.memory.external_bytes);
        self.memory.external_bytes -= bytes;
        self.releaseMemoryLocked(bytes);
        self.notifyPressureChange();
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
        host_charge: bool,
        bounded_parent: bool,
    ) !u64 {
        const identity = self.issueIdentityLocked();
        try self.reservation_identities.putWithParentPolicy(
            self,
            identity,
            .{ .slice = slice, .bytes = bytes, .host_charge = host_charge },
            bounded_parent,
        );
        return identity;
    }

    fn reservationIdentityLocked(
        self: *ResourceManager,
        reservation: *Reservation,
        bounded_parent: bool,
    ) !*ReservationIdentity {
        if (reservation.identity == 0) {
            if (reservation.bytes != 0) {
                self.memory.accounting_errors +|= 1;
                return error.ResourceAccountingMismatch;
            }
            reservation.identity = try self.registerReservationIdentityLocked(
                reservation.slice,
                0,
                reservation.host_charge,
                bounded_parent,
            );
        }
        const owned = self.reservation_identities.getPtr(reservation.identity) orelse {
            self.memory.accounting_errors +|= 1;
            return error.ReservationReleased;
        };
        if (owned.slice != reservation.slice or owned.bytes != reservation.bytes or owned.host_charge != reservation.host_charge) {
            self.memory.accounting_errors +|= 1;
            return error.ResourceAccountingMismatch;
        }
        return owned;
    }

    /// Prepare the complete normalized request, including its ownership
    /// identity, outside scheduler locks. No working-set credit is held while
    /// waiting; queued metadata must remain covered by operation admission.
    pub fn prepareBatch(self: *ResourceManager, amounts: []const SliceAmount) !PreparedBatchReservation {
        const normalized = try normalizeSliceAmounts(amounts);
        const host_charge_bytes = try hostChargeForAmounts(amounts);
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const identity = self.issueIdentityLocked();
        try self.batch_reservation_identities.put(self, identity, .{
            .prepared = true,
            .amounts = normalized,
            .host_charge_bytes = host_charge_bytes,
        });
        return .{ .manager = self, .identity = identity, .amounts = normalized, .host_charge_bytes = host_charge_bytes };
    }

    pub fn prepareDenseBundle(self: *ResourceManager, amounts: []const SliceAmount, requested_bandwidth_bytes: u64) !PreparedDenseBundle {
        return .{ .memory = try self.prepareBatch(amounts), .requested_bandwidth_bytes = requested_bandwidth_bytes };
    }

    /// A local snapshot can disprove fit. An arbitrary parent chain cannot be
    /// proven by one parent's local snapshot; report unknown and permit a full
    /// atomic attempt when CPU fits, without suppressing owner continuations.
    fn probePreparedBatch(self: *ResourceManager, prepared: *const PreparedBatchReservation) PreparedBatchReservation.Probe {
        if (prepared.manager != self or prepared.consumed) return .mismatch;
        if (!self.mutex.tryLock()) return .wait;
        defer self.mutex.unlock();
        const owned = self.batch_reservation_identities.get(prepared.identity) orelse return .mismatch;
        if (!owned.prepared or !std.meta.eql(owned.amounts, prepared.amounts) or
            owned.host_charge_bytes != prepared.host_charge_bytes) return .mismatch;
        const limit = self.memory.budget.hard_limit_bytes;
        if (limit != 0 and prepared.host_charge_bytes > limit -| self.identity_metadata_bytes) return .too_large;
        for (prepared.amounts, 0..) |bytes, index| {
            const hard = self.slices[index].budget.hard_limit_bytes;
            if (hard != 0 and bytes > hard) return .too_large;
        }
        const next = std.math.add(u64, self.memory.used_bytes, prepared.host_charge_bytes) catch return .wait;
        if (limit != 0 and next > limit) return .wait;
        for (prepared.amounts, 0..) |bytes, index| {
            const state = &self.slices[index];
            const next_slice = std.math.add(u64, state.used_bytes, bytes) catch return .wait;
            if (state.budget.hard_limit_bytes != 0 and next_slice > state.budget.hard_limit_bytes) return .wait;
        }
        if (prepared.host_charge_bytes != 0) if (self.memory_parent) |parent| {
            if (parent.try_reserve_fn == null) return .unsupported;
            return .unknown;
        };
        return .ready;
    }

    fn discardPreparedBatch(self: *ResourceManager, prepared: *const PreparedBatchReservation) void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const owned = self.batch_reservation_identities.get(prepared.identity) orelse return;
        if (owned.prepared and std.meta.eql(owned.amounts, prepared.amounts) and
            owned.host_charge_bytes == prepared.host_charge_bytes)
            _ = self.batch_reservation_identities.remove(prepared.identity);
    }

    /// One bounded attempt, with scheduler -> child -> parent lock order.
    /// Ancestor admission is the last fallible action: failure commits no
    /// slice/aggregate usage or ownership change at any level of the chain.
    fn tryAcquirePreparedBatch(self: *ResourceManager, prepared: *PreparedBatchReservation) PreparedBatchReservation.Attempt {
        if (prepared.manager != self or prepared.consumed) return .mismatch;
        if (!self.mutex.tryLock()) return .busy;
        defer self.mutex.unlock();
        if (!self.pressure_change.tryLock()) return .busy;
        defer self.pressure_change.unlock();
        const owned = self.batch_reservation_identities.getPtr(prepared.identity) orelse return .mismatch;
        if (!owned.prepared or !std.meta.eql(owned.amounts, prepared.amounts) or
            owned.host_charge_bytes != prepared.host_charge_bytes) return .mismatch;

        const memory_limit = self.memory.budget.hard_limit_bytes;
        if (memory_limit != 0 and prepared.host_charge_bytes > memory_limit -| self.identity_metadata_bytes) return .too_large;
        // Check intrinsic size for every slice before testing occupancy.
        for (prepared.amounts, 0..) |bytes, index| {
            const limit = self.slices[index].budget.hard_limit_bytes;
            if (limit != 0 and bytes > limit) return .too_large;
        }
        const next_memory = std.math.add(u64, self.memory.used_bytes, prepared.host_charge_bytes) catch return .denied;
        if (memory_limit != 0 and next_memory > memory_limit) return .denied;
        for (prepared.amounts, 0..) |bytes, index| {
            const state = &self.slices[index];
            const next = std.math.add(u64, state.used_bytes, bytes) catch return .denied;
            if (state.budget.hard_limit_bytes != 0 and next > state.budget.hard_limit_bytes) return .denied;
        }
        if (prepared.host_charge_bytes != 0) self.reserveParentWithoutWaiting(prepared.host_charge_bytes) catch |err| return switch (err) {
            error.ParentMemoryBusy => .busy,
            error.ExecutionWaitUnsupported => .unsupported,
            else => .denied,
        };
        for (prepared.amounts, 0..) |bytes, index| {
            if (bytes == 0) continue;
            const state = &self.slices[index];
            state.used_bytes += bytes;
            state.peak_bytes = @max(state.peak_bytes, state.used_bytes);
            if (state.budget.soft_limit_bytes != 0 and state.used_bytes > state.budget.soft_limit_bytes)
                state.soft_limit_events +|= 1;
        }
        self.commitMemoryLocked(next_memory);
        owned.prepared = false;
        prepared.consumed = true;
        self.pressure_change.advanceLocked();
        return .{ .acquired = .{
            .manager = self,
            .identity = prepared.identity,
            .amounts = prepared.amounts,
            .host_charge_bytes = prepared.host_charge_bytes,
        } };
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
        return self.reserveBatchWithHostCharge(false, amounts, host_charge_bytes);
    }

    /// Preserve physical identity-ledger allocation failure separately from a
    /// budget refusal. Legacy classified callers retain their existing policy.
    pub fn reserveBatchCheckedWithHostCharge(
        self: *ResourceManager,
        amounts: []const SliceAmount,
        host_charge_bytes: u64,
    ) (ClassifiedBatchReserveError || error{OutOfMemory})!BatchReservation {
        return self.reserveBatchWithHostCharge(true, amounts, host_charge_bytes);
    }

    fn reserveBatchWithHostCharge(
        self: *ResourceManager,
        comptime checked_metadata: bool,
        amounts: []const SliceAmount,
        host_charge_bytes: u64,
    ) (if (checked_metadata) ClassifiedBatchReserveError || error{OutOfMemory} else ClassifiedBatchReserveError)!BatchReservation {
        return self.reserveBatchClassifiedWithHostChargeOnce(checked_metadata, amounts, host_charge_bytes) catch |err| {
            if (err != error.ResourceTemporarilyUnavailable) return err;
            const requester = blk: {
                for (amounts) |amount| switch (amount.slice) {
                    .hbc_node_metadata_cache, .lsm_block_table_cache => {},
                    else => break :blk amount.slice,
                };
                break :blk if (amounts.len == 0) Slice.dense_apply_working_set else amounts[0].slice;
            };
            if (self.reclaimForAllocation(requester, host_charge_bytes) == 0) return err;
            return self.reserveBatchClassifiedWithHostChargeOnce(checked_metadata, amounts, host_charge_bytes);
        };
    }

    fn reserveBatchClassifiedWithHostChargeOnce(
        self: *ResourceManager,
        comptime checked_metadata: bool,
        amounts: []const SliceAmount,
        host_charge_bytes: u64,
    ) (if (checked_metadata) ClassifiedBatchReserveError || error{OutOfMemory} else ClassifiedBatchReserveError)!BatchReservation {
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

        const next_memory_preflight = std.math.add(u64, self.memory.used_bytes, host_charge_bytes) catch {
            self.memory.hard_limit_rejections +|= 1;
            return error.ResourceTemporarilyUnavailable;
        };
        if (memory_hard_limit > 0 and next_memory_preflight > memory_hard_limit) {
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
            self,
            identity,
            .{
                .amounts = normalized,
                .host_charge_bytes = host_charge_bytes,
            },
        ) catch |err| {
            if (checked_metadata and err == error.OutOfMemory) return error.OutOfMemory;
            self.memory.hard_limit_rejections +|= 1;
            return error.ResourceTemporarilyUnavailable;
        };

        const next_memory = self.checkMemoryLocked(host_charge_bytes) catch {
            _ = self.batch_reservation_identities.remove(identity);
            return error.ResourceTemporarilyUnavailable;
        };
        self.reserveParentLocked(host_charge_bytes) catch {
            _ = self.batch_reservation_identities.remove(identity);
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
        self.notifyPressureChange();
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
        if (owned.prepared or !std.meta.eql(owned.amounts, reservation.amounts) or
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
        if (released_host > self.memory.used_bytes -| self.identityMetadataCharge()) {
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
        self.releaseMemoryLocked(released_host);
        reservation.amounts = retained;
        reservation.host_charge_bytes = retained_host_charge_bytes;
        owned.amounts = retained;
        owned.host_charge_bytes = retained_host_charge_bytes;
        self.notifyPressureChange();
    }

    fn transferBatchReservationCredit(
        self: *ResourceManager,
        source: *BatchReservation,
        destination: *BatchReservation,
        amounts: [slice_count]u64,
        host_charge_bytes: u64,
    ) !void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        // Validate even a zero-byte move: it must not bless a stale, aliased,
        // prepared, foreign, or already-consumed ownership descriptor.
        if (source.manager != self or destination.manager != self or
            source == destination or source.identity == destination.identity)
        {
            self.memory.accounting_errors +|= 1;
            return error.ResourceAccountingMismatch;
        }
        if (source.released or destination.released) return error.ReservationReleased;
        const source_owned = self.batch_reservation_identities.getPtr(source.identity) orelse
            return error.ReservationReleased;
        const destination_owned = self.batch_reservation_identities.getPtr(destination.identity) orelse
            return error.ReservationReleased;
        if (source_owned.prepared or destination_owned.prepared or
            !std.meta.eql(source_owned.amounts, source.amounts) or
            !std.meta.eql(destination_owned.amounts, destination.amounts) or
            source_owned.host_charge_bytes != source.host_charge_bytes or
            destination_owned.host_charge_bytes != destination.host_charge_bytes)
        {
            self.memory.accounting_errors +|= 1;
            return error.ResourceAccountingMismatch;
        }

        // Compute every result before publishing either owner. No fallible
        // work, parent callback, or resource notification follows mutation.
        var next_source = source.amounts;
        var next_destination = destination.amounts;
        inline for (0..slice_count) |index| {
            if (amounts[index] > source.amounts[index]) return error.InvalidReservationReduction;
            next_destination[index] = std.math.add(u64, destination.amounts[index], amounts[index]) catch
                return error.ResourceAccountingMismatch;
            next_source[index] -= amounts[index];
        }
        if (host_charge_bytes > source.host_charge_bytes) return error.InvalidReservationReduction;
        const next_destination_host = std.math.add(u64, destination.host_charge_bytes, host_charge_bytes) catch
            return error.ResourceAccountingMismatch;
        const next_source_host = source.host_charge_bytes - host_charge_bytes;
        source.amounts = next_source;
        source.host_charge_bytes = next_source_host;
        destination.amounts = next_destination;
        destination.host_charge_bytes = next_destination_host;
        source_owned.amounts = next_source;
        source_owned.host_charge_bytes = next_source_host;
        destination_owned.amounts = next_destination;
        destination_owned.host_charge_bytes = next_destination_host;
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
        if (owned.prepared or !std.meta.eql(owned.amounts, reservation.amounts) or
            owned.host_charge_bytes != reservation.host_charge_bytes)
        {
            self.memory.accounting_errors +|= 1;
            return false;
        }

        if (reservation.host_charge_bytes > self.memory.used_bytes -| self.identityMetadataCharge()) {
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
        self.releaseMemoryLocked(reservation.host_charge_bytes);
        _ = self.batch_reservation_identities.remove(reservation.identity);
        self.notifyPressureChange();
        return true;
    }

    pub fn reserve(self: *ResourceManager, slice: Slice, bytes: u64) !Reservation {
        return self.reserveOnce(slice, bytes) catch |err| {
            if (err != error.ResourceBudgetExceeded) return err;
            if (self.reclaimForAllocation(slice, bytes) == 0) return err;
            return self.reserveOnce(slice, bytes);
        };
    }

    /// Performs the final non-blocking admission check at a commit boundary.
    /// Unlike `reserve`, this never invokes reclaimers: callers may use it
    /// while holding a short mutation fence when failure still precedes WAL
    /// publication. Any slower pacing or reclamation belongs above that fence.
    pub fn reserveImmediate(self: *ResourceManager, slice: Slice, bytes: u64) !Reservation {
        return try self.reserveOnce(slice, bytes);
    }

    /// Attempt admission exactly once and never invoke a reclaimer callback.
    /// Use this while holding a storage transaction or another subsystem lock
    /// whose lock order cannot safely include arbitrary cache owners.
    pub fn reserveWithoutReclaim(self: *ResourceManager, slice: Slice, bytes: u64) !Reservation {
        return self.reserveOnce(slice, bytes);
    }

    /// No reclaim callbacks. Ledger/parent contention spends only the original
    /// scope; every failed attempt leaves capacity and ownership unchanged.
    pub fn reserveWithoutReclaimWithAdmission(self: *ResourceManager, slice: Slice, bytes: u64, control: read_admission.Port) !Reservation {
        while (true) {
            return self.reserveOnceControlled(slice, bytes, control) catch |err| {
                if (err != error.ParentMemoryBusy) return err;
                try control.waitForProgress();
                continue;
            };
        }
    }

    /// A single joined consumer owns manager lifetime and independent IO.
    pub fn bindCleanupConsumer(self: *ResourceManager) !void {
        if (self.cleanup_consumer_bound.cmpxchgStrong(false, true, .acq_rel, .acquire) != null) return error.ManagerCleanupAlreadyBound;
    }
    pub fn unbindCleanupConsumer(self: *ResourceManager) void {
        std.debug.assert(self.cleanup_consumer_bound.swap(false, .acq_rel));
    }
    pub fn notifyCleanupReady(self: *ResourceManager) void {
        _ = self.cleanup_epoch.fetchAdd(1, .release);
    }
    pub fn cleanupPending(self: *const ResourceManager) bool {
        if (self.payload_retirement_ready.load(.acquire) != 0) return true;
        for (&self.construction_cleanup) |*slot| {
            const state = slot.state.load(.acquire);
            if (state == .ready or state == .draining) return true;
        }
        return false;
    }

    /// Register embedded, already charged metadata before publication. No
    /// heap registry allocation and no scope borrow survives this call.
    pub fn registerPayloadRetirementWithAdmission(self: *ResourceManager, node: *PayloadRetirement, destination: *?Reservation, construction: *ConstructionCredit, context: *anyopaque, destroy: *const fn (*anyopaque) void, control: read_admission.Port) !void {
        try @import("../common/bounded_mutex.zig").lockAtomic(&self.payload_retirement_mutex, control);
        defer self.payload_retirement_mutex.unlock();
        std.debug.assert(construction.manager == self and node.credit == null and destination.* == null);
        destination.* = construction.transfer();
        node.* = .{ .credit = &destination.*.?, .context = context, .destroy = destroy, .next = self.payload_retirement_head };
        if (self.payload_retirement_head) |head| head.previous = node;
        self.payload_retirement_head = node;
        if (self.payload_retirement_cursor == null) self.payload_retirement_cursor = node;
        // No fallible callback after transferring the sole credit owner.
    }

    /// Upgrade a live legacy payload before scoped eviction drops its cache
    /// reference. Its existing credit already covers the embedded record.
    pub fn registerExistingPayloadRetirementWithAdmission(self: *ResourceManager, node: *PayloadRetirement, credit: *Reservation, context: *anyopaque, destroy: *const fn (*anyopaque) void, control: read_admission.Port) !void {
        if (node.credit != null) return;
        try @import("../common/bounded_mutex.zig").lockAtomic(&self.payload_retirement_mutex, control);
        defer self.payload_retirement_mutex.unlock();
        if (node.credit != null) return;
        if (credit.manager != self or credit.released) return error.ResourceAccountingMismatch;
        node.* = .{ .credit = credit, .context = context, .destroy = destroy, .next = self.payload_retirement_head };
        if (self.payload_retirement_head) |head| head.previous = node;
        self.payload_retirement_head = node;
        if (self.payload_retirement_cursor == null) self.payload_retirement_cursor = node;
    }

    fn unlinkPayloadRetirement(self: *ResourceManager, node: *PayloadRetirement) void {
        if (node.previous) |previous| previous.next = node.next else self.payload_retirement_head = node.next;
        if (node.next) |next| next.previous = node.previous;
        if (self.payload_retirement_cursor == node) self.payload_retirement_cursor = node.next orelse self.payload_retirement_head;
    }

    /// One admitted, bounded pass over stable embedded owners. The cursor
    /// prevents active or newly registered buffers from starving older ready
    /// buffers. Physical destruction precedes accounting release, and a busy
    /// parent leaves credit in fixed manager metadata after destruction.
    pub fn drainPayloadRetirementWithAdmission(self: *ResourceManager, control: read_admission.Port) !usize {
        return self.drainPayloadRetirement(control);
    }

    /// Explicit quiesced fallback only. Owners must remain alive until their
    /// callbacks finish; no application execution or IO-wait authority is used.
    pub fn drainPayloadRetirementQuiesced(self: *ResourceManager) usize {
        return self.drainPayloadRetirement(null) catch unreachable;
    }

    fn drainPayloadRetirement(self: *ResourceManager, control: ?read_admission.Port) !usize {
        var retired: usize = 0;
        for (0..64) |_| {
            if (control) |original| try @import("../common/bounded_mutex.zig").lockAtomic(&self.payload_retirement_mutex, original) else lockAtomic(&self.payload_retirement_mutex);
            var registry_held = true;
            defer if (registry_held) self.payload_retirement_mutex.unlock();
            // lockAtomic checkpoints before acquisition. A cooperative yield
            // here could park while excluding every unrelated registration.
            // This fixed-size ledger pass uses lifetime checks under the guard.
            if (control) |original| try original.checkLifetime();
            _ = self.tryDrainIdentityMetadataRetirement();
            const node = self.payload_retirement_cursor orelse break;
            self.payload_retirement_cursor = node.next orelse self.payload_retirement_head;
            if (!node.ready.load(.acquire)) continue;
            // Own stable retirement debt before destroying embedded metadata.
            var debt: ?ConstructionCredit = null;
            for (&self.construction_cleanup) |*slot| {
                if (control) |original| try original.checkLifetime();
                _ = self.tryDrainConstructionSlot(slot);
                if (slot.state.cmpxchgStrong(.free, .active, .acquire, .monotonic) == null) {
                    slot.credit = node.credit.?.*;
                    debt = .{ .manager = self, .slot = slot };
                    break;
                }
            }
            if (debt == null) return retired;
            self.unlinkPayloadRetirement(node);
            const destroy = node.destroy.?;
            const context = node.context.?;
            // The stable debt slot now owns the exact charge. Unlink under
            // the registry lock, then release it before the allocator callback:
            // an uninterruptible destructor must not pin unrelated registration.
            self.payload_retirement_mutex.unlock();
            registry_held = false;
            // No fallible checkpoint from unlink through physical destruction
            // and credit publication, even if the deadline expires meanwhile.
            destroy(context);
            _ = self.payload_retirement_ready.fetchSub(1, .acq_rel);
            debt.?.retire();
            retired += 1;
        }
        return retired;
    }

    /// Pooled foreground work publishes retirement; only the joined consumer
    /// performs allocator callbacks. Standalone managers retain synchronous
    /// collection under the caller's original admission.
    pub fn drainForegroundPayloadRetirementWithAdmission(self: *ResourceManager, control: read_admission.Port) !usize {
        if (self.cleanup_consumer_bound.load(.acquire)) {
            try control.checkpoint();
            return 0;
        }
        return self.drainPayloadRetirementWithAdmission(control);
    }

    /// Reserve an independent retirement owner before reserving decoder bytes.
    /// Saturation refuses construction before allocation or physical work.
    pub fn reserveConstructionWithAdmission(self: *ResourceManager, slice: Slice, bytes: u64, control: read_admission.Port) !ConstructionCredit {
        _ = try self.drainForegroundPayloadRetirementWithAdmission(control);
        // Visit all completed slots before electing a free one. Returning at
        // the first free slot would indefinitely strand later retirement debt
        // under a steady stream of low-concurrency reads.
        for (&self.construction_cleanup) |*slot| {
            try control.checkpoint();
            _ = self.tryDrainConstructionSlot(slot);
        }
        for (&self.construction_cleanup) |*slot| {
            try control.checkpoint();
            if (slot.state.cmpxchgStrong(.free, .active, .acquire, .monotonic) != null) continue;
            errdefer slot.state.store(.free, .release);
            slot.credit = try self.reserveWithoutReclaimWithAdmission(slice, bytes, control);
            // No fallible checkpoint after credit publication and before the
            // caller can install its mandatory retirement defer.
            return .{ .manager = self, .slot = slot };
        }
        return error.ResourceBudgetExceeded;
    }

    fn tryDrainConstructionSlot(self: *ResourceManager, slot: *ConstructionCleanup) bool {
        if (slot.state.cmpxchgStrong(.ready, .draining, .acquire, .monotonic) != null) return slot.state.load(.acquire) == .free;
        switch (self.tryReleaseReservation(&slot.credit.?)) {
            .released => {
                slot.credit = null;
                slot.state.store(.free, .release);
                return true;
            },
            .mismatch => @panic("construction cleanup lost reservation ownership"),
            .busy, .unsupported => {
                slot.state.store(.ready, .release);
                return false;
            },
        }
    }

    /// Borrow a fresh cleanup admission for each synchronous drain. A rejected
    /// cleanup leaves every unretired identity charged in manager-owned slots.
    pub fn drainConstructionCleanupWithAdmission(self: *ResourceManager, control: read_admission.Port) !void {
        while (true) {
            var pending = false;
            for (&self.construction_cleanup) |*slot| {
                try control.checkpoint();
                if (!self.tryDrainConstructionSlot(slot)) {
                    const state = slot.state.load(.acquire);
                    pending = pending or state == .ready or state == .draining;
                }
            }
            if (!pending) return;
            try control.waitForProgress();
        }
    }

    /// Atomically owns a required secondary credit and as much of the requested
    /// primary credit as currently fits. The full request gets the normal cache
    /// reclamation opportunity before the partial fallback is considered.
    ///
    /// Unlike a "leave N bytes free" calculation, both returned credits belong
    /// to this reservation until release, so concurrent work cannot consume the
    /// secondary credit between admission and allocation.
    pub fn reserveOwnedSplitAtMost(
        self: *ResourceManager,
        slice: Slice,
        requested_primary_bytes: u64,
        required_secondary_bytes: u64,
    ) !OwnedSplitReservation {
        return self.reserveOwnedSplitAtMostWithPolicy(slice, requested_primary_bytes, required_secondary_bytes, true);
    }

    /// Preserve the mandatory retained-output requirement when distinguishing
    /// permanent oversize from another owner's temporary use of the envelope.
    pub fn reserveOwnedSplitAtMostClassified(self: *ResourceManager, slice: Slice, requested_primary_bytes: u64, required_secondary_bytes: u64) !OwnedSplitReservation {
        return self.reserveOwnedSplitAtMost(slice, requested_primary_bytes, required_secondary_bytes) catch |err| {
            if (err != error.ResourceBudgetExceeded) return err;
            const minimum = std.math.add(u64, required_secondary_bytes, @intFromBool(requested_primary_bytes > 0)) catch return error.ResourceRequestTooLarge;
            return self.classifyRequestDenial(slice, minimum);
        };
    }

    /// Call only after an admission failure. Classification depends on the
    /// request's own demand and immutable limits, never on a racy usage sample.
    pub fn classifyRequestDenial(self: *ResourceManager, slice: Slice, bytes: u64) error{ ResourceRequestTooLarge, ResourceTemporarilyUnavailable } {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const limit = self.slices[sliceIndex(slice)].budget.hard_limit_bytes;
        const host_limit = self.memory.budget.hard_limit_bytes;
        return if ((limit != 0 and bytes > limit) or (host_limit != 0 and bytes > host_limit)) error.ResourceRequestTooLarge else error.ResourceTemporarilyUnavailable;
    }

    /// Atomically grant available headroom up to the requested ceiling. Like
    /// reserveWithoutReclaim, this never calls cache owners while the caller
    /// holds another subsystem's resources. A nonzero request requires a
    /// nonzero grant; partial grants remain ordinary owned reservations.
    pub fn reserveAtMostWithoutReclaim(self: *ResourceManager, slice: Slice, requested_bytes: u64) !Reservation {
        const split = try self.reserveOwnedSplitAtMostWithPolicy(slice, requested_bytes, 0, false);
        return split.reservation;
    }

    fn reserveOwnedSplitAtMostWithPolicy(
        self: *ResourceManager,
        slice: Slice,
        requested_primary_bytes: u64,
        required_secondary_bytes: u64,
        allow_reclaim: bool,
    ) !OwnedSplitReservation {
        const requested_total = std.math.add(u64, requested_primary_bytes, required_secondary_bytes) catch
            return error.ResourceBudgetExceeded;
        if (requested_total == 0) return .{
            .reservation = .{ .manager = self, .identity = 0, .slice = slice, .bytes = 0 },
            .primary_bytes = 0,
            .secondary_bytes = 0,
        };

        if (if (allow_reclaim) self.reserve(slice, requested_total) else self.reserveWithoutReclaim(slice, requested_total)) |reservation| {
            return .{
                .reservation = reservation,
                .primary_bytes = requested_primary_bytes,
                .secondary_bytes = required_secondary_bytes,
            };
        } else |err| {
            if (err != error.ResourceBudgetExceeded) return err;
        }

        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        // Admit any identity-table growth before computing payload headroom.
        // A rejected full request need not have allocated its identity table.
        try self.reservation_identities.ensureUnusedCapacity(self, 1);
        const state = &self.slices[sliceIndex(slice)];
        const slice_available = if (state.budget.hard_limit_bytes == 0)
            requested_total
        else
            state.budget.hard_limit_bytes -| state.used_bytes;
        const memory_available = if (self.memory.budget.hard_limit_bytes == 0)
            requested_total
        else
            self.memory.budget.hard_limit_bytes -| self.memory.used_bytes;
        const available = @min(@min(slice_available, memory_available), if (self.memory_parent) |parent| parent.available() else std.math.maxInt(u64));
        if (available < required_secondary_bytes) return error.ResourceBudgetExceeded;
        const primary_bytes = @min(requested_primary_bytes, available - required_secondary_bytes);
        if (requested_primary_bytes > 0 and primary_bytes == 0) return error.ResourceBudgetExceeded;
        const granted = std.math.add(u64, primary_bytes, required_secondary_bytes) catch
            return error.ResourceBudgetExceeded;

        const next_slice = std.math.add(u64, state.used_bytes, granted) catch {
            state.hard_limit_rejections +|= 1;
            return error.ResourceBudgetExceeded;
        };
        _ = std.math.add(u64, self.memory.used_bytes, granted) catch {
            self.memory.hard_limit_rejections +|= 1;
            return error.ResourceBudgetExceeded;
        };

        const identity = try self.registerReservationIdentityLocked(slice, granted, true, false);
        errdefer _ = self.reservation_identities.remove(identity);
        const admitted_memory = try self.checkMemoryLocked(granted);
        self.reserveParentLocked(granted) catch |err| {
            _ = self.reservation_identities.remove(identity);
            return err;
        };
        state.used_bytes = next_slice;
        state.peak_bytes = @max(state.peak_bytes, state.used_bytes);
        if (state.budget.soft_limit_bytes > 0 and state.used_bytes > state.budget.soft_limit_bytes)
            state.soft_limit_events +|= 1;
        self.memory.used_bytes = admitted_memory;
        self.memory.peak_bytes = @max(self.memory.peak_bytes, self.memory.used_bytes);
        if (self.memory.budget.soft_limit_bytes > 0 and self.memory.used_bytes > self.memory.budget.soft_limit_bytes)
            self.memory.soft_limit_events +|= 1;
        self.notifyPressureChange();
        return .{
            .reservation = .{ .manager = self, .identity = identity, .slice = slice, .bytes = granted },
            .primary_bytes = primary_bytes,
            .secondary_bytes = required_secondary_bytes,
        };
    }

    fn reserveOnce(self: *ResourceManager, slice: Slice, bytes: u64) !Reservation {
        return self.reserveOnceControlled(slice, bytes, null);
    }

    fn reserveOnceControlled(self: *ResourceManager, slice: Slice, bytes: u64, control: ?read_admission.Port) !Reservation {
        if (control) |budget| try budget.checkpoint();
        if (bytes == 0) return .{ .manager = self, .identity = 0, .slice = slice, .bytes = 0 };

        if (control) |budget| try @import("../common/bounded_mutex.zig").lockAtomic(&self.mutex, budget) else lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        if (control != null and !self.pressure_change.tryLock()) return error.ParentMemoryBusy;
        defer if (control != null) self.pressure_change.unlock();

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
        _ = try self.checkMemoryLocked(bytes);
        const identity = try self.registerReservationIdentityLocked(slice, bytes, true, control != null);
        errdefer _ = self.reservation_identities.remove(identity);
        const next_memory = try self.checkMemoryLocked(bytes);
        (if (control != null) self.reserveParentWithoutWaiting(bytes) else self.reserveParentLocked(bytes)) catch |err| {
            _ = self.reservation_identities.remove(identity);
            return err;
        };
        state.used_bytes = next;
        state.peak_bytes = @max(state.peak_bytes, next);
        if (state.budget.soft_limit_bytes > 0 and next > state.budget.soft_limit_bytes) {
            state.soft_limit_events += 1;
        }
        self.commitMemoryLocked(next_memory);
        if (control != null) self.pressure_change.advanceLocked() else self.notifyPressureChange();
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
        _ = try self.checkMemoryLocked(bytes);
        const identity = try self.registerReservationIdentityLocked(slice, bytes, true, false);
        errdefer _ = self.reservation_identities.remove(identity);
        const next_memory = try self.checkMemoryLocked(bytes);
        self.reserveParentLocked(bytes) catch |err| {
            _ = self.reservation_identities.remove(identity);
            return err;
        };
        if (oversized_grant) state.oversized_single_grants +|= 1;
        state.used_bytes = next;
        state.peak_bytes = @max(state.peak_bytes, next);
        if (state.budget.soft_limit_bytes > 0 and next > state.budget.soft_limit_bytes) {
            state.soft_limit_events +|= 1;
        }
        self.commitMemoryLocked(next_memory);
        self.notifyPressureChange();
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

        const owned = try self.reservationIdentityLocked(reservation, false);
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
        const next_memory = try self.checkMemoryLocked(if (reservation.host_charge) additional_bytes else 0);
        if (reservation.host_charge) try self.reserveParentLocked(additional_bytes);
        if (oversized_grant) state.oversized_single_grants +|= 1;
        state.used_bytes = next;
        reservation.bytes = next_reservation;
        owned.bytes = next_reservation;
        state.peak_bytes = @max(state.peak_bytes, next);
        if (state.budget.soft_limit_bytes > 0 and next > state.budget.soft_limit_bytes) {
            state.soft_limit_events +|= 1;
        }
        self.commitMemoryLocked(next_memory);
        self.notifyPressureChange();
    }

    fn growReservationAmortized(
        self: *ResourceManager,
        reservation: *Reservation,
        minimum_bytes: u64,
        preferred_bytes: u64,
        max_hard_limit_multiple: u64,
        other_owner_bytes: u64,
    ) !u64 {
        if (minimum_bytes == 0) return 0;
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const owned = try self.reservationIdentityLocked(reservation, false);
        const state = &self.slices[sliceIndex(reservation.slice)];
        const hard_limit = state.budget.hard_limit_bytes;
        const bounded_limit = if (hard_limit == 0)
            std.math.maxInt(u64)
        else
            std.math.mul(u64, hard_limit, max_hard_limit_multiple) catch std.math.maxInt(u64);
        const owner_bytes = std.math.add(u64, reservation.bytes, other_owner_bytes) catch return error.ResourceBudgetExceeded;
        const reservation_is_sole_user = state.used_bytes == owner_bytes;

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
            owner_bytes,
            granted,
            hard_limit,
            bounded_limit,
            reservation_is_sole_user,
            max_hard_limit_multiple,
        )) {
            granted = minimum_bytes;
            if (!Candidate.allowed(
                state.used_bytes,
                owner_bytes,
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
        if (reservation.host_charge and !MemoryCandidate.allowed(self.memory.used_bytes, granted, self.memory.budget.hard_limit_bytes)) {
            granted = minimum_bytes;
            if (!Candidate.allowed(
                state.used_bytes,
                owner_bytes,
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

        if (reservation.host_charge) self.reserveParentLocked(granted) catch {
            granted = minimum_bytes;
            try self.reserveParentLocked(granted);
        };
        const previous_reservation = reservation.bytes;
        const next_memory = std.math.add(u64, self.memory.used_bytes, if (reservation.host_charge) granted else 0) catch unreachable;
        state.used_bytes += granted;
        reservation.bytes += granted;
        owned.bytes = reservation.bytes;
        state.peak_bytes = @max(state.peak_bytes, state.used_bytes);
        if (hard_limit > 0 and state.used_bytes > hard_limit and previous_reservation <= hard_limit)
            state.oversized_single_grants +|= 1;
        if (state.budget.soft_limit_bytes > 0 and state.used_bytes > state.budget.soft_limit_bytes)
            state.soft_limit_events +|= 1;
        self.commitMemoryLocked(next_memory);
        self.notifyPressureChange();
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
        if (owned.slice != reservation.slice or owned.bytes != reservation.bytes or owned.host_charge != reservation.host_charge) {
            self.memory.accounting_errors +|= 1;
            return false;
        }
        const state = &self.slices[sliceIndex(owned.slice)];
        if (owned.bytes > state.used_bytes or (owned.host_charge and owned.bytes > self.memory.used_bytes -| self.identityMetadataCharge())) {
            self.memory.accounting_errors +|= 1;
            return false;
        }
        state.used_bytes -= owned.bytes;
        if (owned.host_charge) self.releaseMemoryLocked(owned.bytes);
        _ = self.reservation_identities.remove(reservation.identity);
        self.notifyPressureChange();
        return true;
    }

    /// The physical owner has retired. A busy ledger/parent keeps this exact
    /// reservation charged in its independent cleanup owner; no waiting,
    /// reclamation, scope callback or legacy parent callback occurs here.
    pub fn tryReleaseReservation(self: *ResourceManager, reservation: *Reservation) MemoryParent.ReleaseResult {
        if (reservation.manager != self) return .mismatch;
        if (reservation.released) return .released;
        if (reservation.identity == 0 and reservation.bytes == 0) {
            reservation.released = true;
            return .released;
        }
        if (!self.mutex.tryLock()) return .busy;
        defer self.mutex.unlock();
        // Reserve notification serialization before any ancestor commits.
        if (!self.pressure_change.tryLock()) return .busy;
        defer self.pressure_change.unlock();
        const owned = self.reservation_identities.get(reservation.identity) orelse return .mismatch;
        const state = &self.slices[sliceIndex(owned.slice)];
        if (owned.slice != reservation.slice or owned.bytes != reservation.bytes or owned.host_charge != reservation.host_charge or owned.bytes > state.used_bytes or (owned.host_charge and owned.bytes > self.memory.used_bytes -| self.identityMetadataCharge())) return .mismatch;
        if (owned.host_charge and owned.bytes != 0) self.releaseParentWithoutWaiting(owned.bytes) catch |err| return switch (err) {
            error.ParentMemoryBusy => .busy,
            error.ExecutionWaitUnsupported => .unsupported,
            else => .mismatch,
        };
        state.used_bytes -= owned.bytes;
        if (owned.host_charge) self.memory.used_bytes -= owned.bytes;
        _ = self.reservation_identities.remove(reservation.identity);
        reservation.released = true;
        self.pressure_change.advanceLocked();
        return .released;
    }

    fn shrinkReservation(self: *ResourceManager, reservation: *Reservation, bytes: u64) bool {
        if (bytes == 0 or reservation.bytes == 0) return true;
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const owned = self.reservation_identities.getPtr(reservation.identity) orelse {
            self.memory.accounting_errors +|= 1;
            return false;
        };
        if (owned.slice != reservation.slice or owned.bytes != reservation.bytes or owned.host_charge != reservation.host_charge) {
            self.memory.accounting_errors +|= 1;
            return false;
        }
        const released = @min(bytes, owned.bytes);
        const state = &self.slices[sliceIndex(owned.slice)];
        if (released > state.used_bytes or (owned.host_charge and released > self.memory.used_bytes -| self.identityMetadataCharge())) {
            self.memory.accounting_errors +|= 1;
            return false;
        }
        state.used_bytes -= released;
        if (owned.host_charge) self.releaseMemoryLocked(released);
        owned.bytes -= released;
        reservation.bytes -= released;
        self.notifyPressureChange();
        return true;
    }

    /// Reclassify an existing owner atomically without a second host charge.
    /// No allocation or callback occurs; destination slice admission still applies.
    fn reclassifyReservation(self: *ResourceManager, reservation: *Reservation, destination: Slice) !void {
        return self.reclassifyReservationControlled(reservation, destination, null);
    }

    fn reclassifyReservationControlled(self: *ResourceManager, reservation: *Reservation, destination: Slice, control: ?read_admission.Port) !void {
        if (control) |budget| try @import("../common/bounded_mutex.zig").lockAtomic(&self.mutex, budget) else lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        if (control != null and !self.pressure_change.tryLock()) return error.ParentMemoryBusy;
        defer if (control != null) self.pressure_change.unlock();
        const owned = self.reservation_identities.getPtr(reservation.identity) orelse return error.ReservationReleased;
        if (reservation.manager != self or reservation.released or owned.slice != reservation.slice or owned.bytes != reservation.bytes or owned.host_charge != reservation.host_charge)
            return error.ResourceAccountingMismatch;
        if (destination == reservation.slice) return;
        const source = &self.slices[sliceIndex(reservation.slice)];
        const target = &self.slices[sliceIndex(destination)];
        const next = std.math.add(u64, target.used_bytes, reservation.bytes) catch return error.ResourceBudgetExceeded;
        if (target.budget.hard_limit_bytes != 0 and next > target.budget.hard_limit_bytes) return error.ResourceBudgetExceeded;
        if (source.used_bytes < reservation.bytes) return error.ResourceAccountingMismatch;
        source.used_bytes -= reservation.bytes;
        target.used_bytes = next;
        target.peak_bytes = @max(target.peak_bytes, next);
        if (target.budget.soft_limit_bytes != 0 and next > target.budget.soft_limit_bytes) target.soft_limit_events +|= 1;
        owned.slice = destination;
        reservation.slice = destination;
        if (control != null) self.pressure_change.advanceLocked() else self.pressure_change.advance();
    }

    /// Move already-accounted credit between two live reservations without
    /// changing slice or host usage. This is the ownership handoff used when
    /// operation admission pre-reserves allocator headroom before the
    /// destination BudgetedAllocator exists or begins allocating.
    fn transferReservationCredit(
        self: *ResourceManager,
        source: *Reservation,
        destination: *Reservation,
        bytes: u64,
    ) !void {
        if (bytes == 0) return;
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        if (source.manager != self or destination.manager != self or
            source.slice != destination.slice or source.host_charge != destination.host_charge or source == destination or
            (source.identity != 0 and source.identity == destination.identity) or
            source.released or destination.released)
        {
            self.memory.accounting_errors +|= 1;
            return error.ResourceAccountingMismatch;
        }

        // Register an empty destination first. The insertion may rehash the
        // identity map, so acquire both entry pointers only afterwards.
        _ = try self.reservationIdentityLocked(destination, false);
        const source_owned = self.reservation_identities.getPtr(source.identity) orelse {
            self.memory.accounting_errors +|= 1;
            return error.ReservationReleased;
        };
        const destination_owned = self.reservation_identities.getPtr(destination.identity) orelse {
            self.memory.accounting_errors +|= 1;
            return error.ReservationReleased;
        };
        if (source_owned.slice != source.slice or source_owned.bytes != source.bytes or source_owned.host_charge != source.host_charge or
            destination_owned.slice != destination.slice or destination_owned.bytes != destination.bytes or destination_owned.host_charge != destination.host_charge or
            bytes > source.bytes)
        {
            self.memory.accounting_errors +|= 1;
            return error.ResourceAccountingMismatch;
        }
        const next_destination = std.math.add(u64, destination.bytes, bytes) catch {
            self.memory.accounting_errors +|= 1;
            return error.ResourceAccountingMismatch;
        };
        source.bytes -= bytes;
        source_owned.bytes = source.bytes;
        destination.bytes = next_destination;
        destination_owned.bytes = next_destination;
    }

    fn reconcileUsageLocked(
        self: *ResourceManager,
        slice: Slice,
        observer_id: usize,
        previous: u64,
        next: u64,
        enforce_limits: bool,
    ) !void {
        return self.reconcileUsageLockedWithParentAdmission(slice, observer_id, previous, next, enforce_limits, false);
    }

    fn reconcileUsageLockedWithParentAdmission(
        self: *ResourceManager,
        slice: Slice,
        observer_id: usize,
        previous: u64,
        next: u64,
        enforce_limits: bool,
        bounded_parent: bool,
    ) !void {
        return self.reconcileUsageWithHostChargeAndParentAdmissionLocked(slice, observer_id, previous, next, next, enforce_limits, bounded_parent);
    }

    fn reconcileUsageWithHostChargeLocked(
        self: *ResourceManager,
        slice: Slice,
        observer_id: usize,
        previous: u64,
        next: u64,
        host_next: u64,
        enforce_limits: bool,
    ) !void {
        return self.reconcileUsageWithHostChargeAndParentAdmissionLocked(slice, observer_id, previous, next, host_next, enforce_limits, false);
    }

    fn reconcileUsageWithHostChargeAndParentAdmissionLocked(
        self: *ResourceManager,
        slice: Slice,
        observer_id: usize,
        previous: u64,
        next: u64,
        host_next: u64,
        enforce_limits: bool,
        bounded_parent: bool,
    ) !void {
        if (bounded_parent and !self.pressure_change.tryLock()) return error.ParentMemoryBusy;
        defer if (bounded_parent) self.pressure_change.unlock();
        if (host_next > next) return error.ResourceAccountingMismatch;
        const key = ObserverKey{ .slice = slice, .identity = observer_id };
        var inserted = false;
        const owned = self.observer_identities.getPtr(key) orelse blk: {
            if (previous != 0) {
                self.memory.accounting_errors +|= 1;
                return error.ResourceAccountingMismatch;
            }
            if (next == 0) return;
            const entry = try self.observer_identities.getOrPutWithParentPolicy(
                self,
                key,
                bounded_parent,
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
        if (previous > state.used_bytes or owned.host_current > self.memory.used_bytes -| self.identityMetadataCharge()) {
            self.memory.accounting_errors +|= 1;
            return error.ResourceAccountingMismatch;
        }
        const slice_next = std.math.add(u64, state.used_bytes - previous, next) catch
            return error.ResourceBudgetExceeded;
        const memory_next = std.math.add(u64, self.memory.used_bytes - owned.host_current, host_next) catch
            return error.ResourceBudgetExceeded;
        // Observation-only callers may report allocations that already exist,
        // but admission callers must reject growth before allocation. A
        // validated decrease is always accepted, even while the aggregate is
        // still above a hard limit, so owners can converge out of pressure.
        if (enforce_limits and (next > previous or host_next > owned.host_current)) {
            if ((state.budget.hard_limit_bytes > 0 and slice_next > state.budget.hard_limit_bytes) or
                (self.memory.budget.hard_limit_bytes > 0 and memory_next > self.memory.budget.hard_limit_bytes))
            {
                state.hard_limit_rejections +|= 1;
                if (self.memory.budget.hard_limit_bytes > 0 and memory_next > self.memory.budget.hard_limit_bytes)
                    self.memory.hard_limit_rejections +|= 1;
                return error.ResourceBudgetExceeded;
            }
        }

        if (memory_next > self.memory.used_bytes) {
            const growth = memory_next - self.memory.used_bytes;
            if (enforce_limits) {
                if (bounded_parent) try self.reserveParentWithoutWaiting(growth) else try self.reserveParentLocked(growth);
            } else if (self.memory_parent) |parent| parent.observe_fn(parent.ptr, growth);
        } else if (memory_next < self.memory.used_bytes) {
            const released = self.memory.used_bytes - memory_next;
            if (bounded_parent) {
                // A busy parent leaves the observer and every charge intact.
                // The controlled caller releases this ledger before waiting.
                try self.releaseParentWithoutWaiting(released);
            } else if (self.memory_parent) |parent| parent.release_fn(parent.ptr, released);
        }

        const observed_host = host_next != 0 or owned.host_current != 0;
        state.used_bytes = slice_next;
        self.memory.used_bytes = memory_next;
        owned.current = next;
        owned.host_current = host_next;
        state.peak_bytes = @max(state.peak_bytes, slice_next);
        self.memory.peak_bytes = @max(self.memory.peak_bytes, memory_next);
        if (state.budget.soft_limit_bytes > 0 and slice_next > state.budget.soft_limit_bytes)
            state.soft_limit_events +|= 1;
        if (!enforce_limits and observed_host and state.budget.hard_limit_bytes > 0 and slice_next > state.budget.hard_limit_bytes)
            state.hard_limit_rejections +|= 1;
        if (self.memory.budget.soft_limit_bytes > 0 and memory_next > self.memory.budget.soft_limit_bytes)
            self.memory.soft_limit_events +|= 1;
        if (!enforce_limits and observed_host and self.memory.budget.hard_limit_bytes > 0 and memory_next > self.memory.budget.hard_limit_bytes)
            self.memory.hard_limit_rejections +|= 1;
        if (next == 0) _ = self.observer_identities.remove(key);
        if (bounded_parent) self.pressure_change.advanceLocked() else self.notifyPressureChange();
    }

    /// Reserve a stable observer's ledger metadata before physical construction.
    /// A zero-valued identity remains registered until reconciliation retires
    /// it; subsequent adoption needs no observer-map growth or allocation.
    pub fn prepareUsageWithAdmission(self: *ResourceManager, slice: Slice, current: *u64, control: read_admission.Port) !void {
        while (true) {
            self.prepareUsageOnceWithAdmission(slice, current, control) catch |cause| {
                if (cause != error.ParentMemoryBusy) return cause;
                try control.waitForProgress();
                continue;
            };
            return;
        }
    }

    fn prepareUsageOnceWithAdmission(self: *ResourceManager, slice: Slice, current: *u64, control: read_admission.Port) !void {
        try @import("../common/bounded_mutex.zig").lockAtomic(&self.mutex, control);
        defer self.mutex.unlock();
        if (!self.pressure_change.tryLock()) return error.ParentMemoryBusy;
        defer self.pressure_change.unlock();
        const key: ObserverKey = .{ .slice = slice, .identity = @intFromPtr(current) };
        if (self.observer_identities.getPtr(key)) |owned| {
            if (owned.current != current.* or owned.host_current != current.*) return error.ResourceAccountingMismatch;
            return;
        }
        if (current.* != 0) return error.ResourceAccountingMismatch;
        const entry = try self.observer_identities.getOrPutWithParentPolicy(self, key, true);
        std.debug.assert(!entry.found_existing);
        entry.value_ptr.* = .{ .current = 0 };
    }

    /// Move construction credit into an observed retained owner atomically.
    /// Destination growth includes metadata; only its excess over transferred
    /// credit adds host usage. No reclamation callback runs under this lock.
    pub fn adoptReservationUsage(self: *ResourceManager, reservation: *Reservation, destination: Slice, current: *u64, added: u64) !void {
        return self.adoptReservationUsageOnce(reservation, destination, current, added, null);
    }

    pub fn adoptReservationUsageWithAdmission(self: *ResourceManager, reservation: *Reservation, destination: Slice, current: *u64, added: u64, control: read_admission.Port) !void {
        while (true) {
            self.adoptReservationUsageOnce(reservation, destination, current, added, control) catch |err| {
                if (err != error.ParentMemoryBusy) return err;
                try control.waitForProgress();
                continue;
            };
            return;
        }
    }

    fn adoptReservationUsageOnce(self: *ResourceManager, reservation: *Reservation, destination: Slice, current: *u64, added: u64, control: ?read_admission.Port) !void {
        if (control) |budget| try @import("../common/bounded_mutex.zig").lockAtomic(&self.mutex, budget) else lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        if (control != null and !self.pressure_change.tryLock()) return error.ParentMemoryBusy;
        defer if (control != null) self.pressure_change.unlock();
        if (reservation.manager != self or reservation.released or !reservation.host_charge) return error.ResourceAccountingMismatch;
        const source_owned = try self.reservationIdentityLocked(reservation, control != null);
        const credit = @min(reservation.bytes, added);
        const key: ObserverKey = .{ .slice = destination, .identity = @intFromPtr(current) };
        var inserted = false;
        const observed = self.observer_identities.getPtr(key) orelse blk: {
            if (current.* != 0) return error.ResourceAccountingMismatch;
            const entry = try self.observer_identities.getOrPutWithParentPolicy(self, key, control != null);
            entry.value_ptr.* = .{ .current = 0 };
            inserted = true;
            break :blk entry.value_ptr;
        };
        errdefer {
            if (inserted) _ = self.observer_identities.remove(key);
        }
        if (observed.current != current.* or observed.host_current != observed.current) return error.ResourceAccountingMismatch;
        const source = &self.slices[sliceIndex(reservation.slice)];
        const target = &self.slices[sliceIndex(destination)];
        if (source.used_bytes < credit or self.memory.used_bytes -| self.identityMetadataCharge() < credit) return error.ResourceAccountingMismatch;
        const target_previous = target.used_bytes - if (reservation.slice == destination) credit else @as(u64, 0);
        const target_next = std.math.add(u64, target_previous, added) catch return error.ResourceBudgetExceeded;
        const memory_next = std.math.add(u64, self.memory.used_bytes - credit, added) catch return error.ResourceBudgetExceeded;
        const observed_next = std.math.add(u64, current.*, added) catch return error.ResourceBudgetExceeded;
        if ((target.budget.hard_limit_bytes != 0 and target_next > target.budget.hard_limit_bytes) or
            (self.memory.budget.hard_limit_bytes != 0 and memory_next > self.memory.budget.hard_limit_bytes))
        {
            target.hard_limit_rejections +|= 1;
            if (self.memory.budget.hard_limit_bytes != 0 and memory_next > self.memory.budget.hard_limit_bytes) self.memory.hard_limit_rejections +|= 1;
            return error.ResourceBudgetExceeded;
        }
        // Construction already charged the parent for credit. Only metadata
        // growth needs new capacity; admit it before publishing either owner.
        // Failure leaves the reservation and observer unchanged.
        if (added > credit) {
            if (control != null) try self.reserveParentWithoutWaiting(added - credit) else try self.reserveParentLocked(added - credit);
        }
        source.used_bytes -= credit;
        target.used_bytes = target_next;
        target.peak_bytes = @max(target.peak_bytes, target_next);
        if (target.budget.soft_limit_bytes != 0 and target_next > target.budget.soft_limit_bytes) target.soft_limit_events +|= 1;
        reservation.bytes -= credit;
        source_owned.bytes = reservation.bytes;
        observed.current = observed_next;
        observed.host_current = observed_next;
        current.* = observed_next;
        self.commitMemoryLocked(memory_next);
        if (control != null) self.pressure_change.advanceLocked() else self.pressure_change.advance();
    }

    pub fn adjustUsage(self: *ResourceManager, slice: Slice, current: *u64, next: u64) !void {
        return self.adjustUsageControlled(slice, current, next, null);
    }

    pub fn adjustUsageWithAdmission(self: *ResourceManager, slice: Slice, current: *u64, next: u64, control: read_admission.Port) !void {
        return self.adjustUsageControlled(slice, current, next, control);
    }

    /// Admit a mandatory physical retirement before releasing its charge.
    /// The infallible callback executes under this manager's ledger lock and
    /// must not reenter the manager. Validation and all scoped waits precede
    /// destruction; the existing parent release remains mandatory afterwards.
    pub fn retireUsageWithAdmission(self: *ResourceManager, slice: Slice, current: *u64, next: u64, control: read_admission.Port, retirement: anytype) !void {
        try @import("../common/bounded_mutex.zig").lockAtomic(&self.mutex, control);
        defer self.mutex.unlock();
        if (next > current.*) return error.InvalidArgument;
        const key = ObserverKey{ .slice = slice, .identity = @intFromPtr(current) };
        const owned = self.observer_identities.getPtr(key) orelse return error.ResourceAccountingMismatch;
        const state = &self.slices[sliceIndex(slice)];
        if (owned.current != current.* or current.* > state.used_bytes or current.* > self.memory.used_bytes -| self.identityMetadataCharge())
            return error.ResourceAccountingMismatch;
        // Decreasing an existing validated identity cannot allocate or exceed
        // a budget. Physical bytes stop existing before their capacity becomes
        // available to another owner. No admission checkpoint follows retire.
        retirement.retire();
        self.reconcileUsageLocked(slice, key.identity, current.*, next, true) catch unreachable;
        current.* = next;
    }

    /// Retire physical bytes into caller-owned accounting debt. The caller
    /// serializes current/debt and retains both until reconciliation completes.
    /// Validation and scoped ledger admission precede infallible destruction;
    /// no parent callback or capacity release occurs here. Debt is published
    /// before destruction so cancellation after this call cannot lose its owner.
    pub fn retireUsageToDebtWithAdmission(self: *ResourceManager, slice: Slice, current: *u64, debt: *u64, amount: u64, control: read_admission.Port, retirement: anytype) !void {
        try @import("../common/bounded_mutex.zig").lockAtomic(&self.mutex, control);
        defer self.mutex.unlock();
        try self.retireUsageToDebtLocked(slice, current, debt, amount, retirement);
    }

    /// Retire with a nonwaiting ledger-release fast path. If any parent is
    /// unavailable, keep all bytes charged in debt for scoped retry after the
    /// caller releases its physical-owner locks. No progress callback follows
    /// destruction here, and no blocking parent accounting is permitted.
    pub fn retireUsageWithRetainedDebtWithAdmission(self: *ResourceManager, slice: Slice, current: *u64, debt: *u64, amount: u64, control: read_admission.Port, retirement: anytype) !void {
        try @import("../common/bounded_mutex.zig").lockAtomic(&self.mutex, control);
        defer self.mutex.unlock();
        try self.retireUsageToDebtLocked(slice, current, debt, amount, retirement);
        _ = self.tryFinishRetirementDebtLocked(slice, current, debt);
    }

    pub const PhysicalRetirementResult = enum { retired, busy, mismatch };

    /// A finite accounting-lock attempt for optional reclamation. Busy or
    /// invalid ownership never destroys the value. A retired result means
    /// physical destruction happened exactly once; any uncommitted charge is
    /// still owned by the caller's debt, not promised as reclaimed capacity.
    pub fn tryRetireUsageWithRetainedDebt(self: *ResourceManager, slice: Slice, current: *u64, debt: *u64, amount: u64, retirement: anytype) PhysicalRetirementResult {
        if (!self.mutex.tryLock()) return .busy;
        defer self.mutex.unlock();
        self.retireUsageToDebtLocked(slice, current, debt, amount, retirement) catch return .mismatch;
        _ = self.tryFinishRetirementDebtLocked(slice, current, debt);
        return .retired;
    }

    /// Caller serializes current/debt and keeps them alive. No waits,
    /// allocations, reclamation or legacy parent callback are permitted.
    pub fn tryFinishRetirementDebt(self: *ResourceManager, slice: Slice, current: *u64, debt: *u64) MemoryParent.ReleaseResult {
        if (debt.* == 0) return .released;
        if (!self.mutex.tryLock()) return .busy;
        defer self.mutex.unlock();
        return self.tryFinishRetirementDebtLocked(slice, current, debt);
    }

    fn tryFinishRetirementDebtLocked(self: *ResourceManager, slice: Slice, current: *u64, debt: *u64) MemoryParent.ReleaseResult {
        if (debt.* == 0) return .released;
        if (debt.* > current.*) return .mismatch;
        const next = current.* - debt.*;
        self.reconcileUsageLockedWithParentAdmission(slice, @intFromPtr(current), current.*, next, true, true) catch |err| return switch (err) {
            error.ParentMemoryBusy => .busy,
            error.ExecutionWaitUnsupported => .unsupported,
            else => .mismatch,
        };
        current.* = next;
        debt.* = 0;
        return .released;
    }

    fn retireUsageToDebtLocked(self: *ResourceManager, slice: Slice, current: *u64, debt: *u64, amount: u64, retirement: anytype) !void {
        const key = ObserverKey{ .slice = slice, .identity = @intFromPtr(current) };
        const owned = self.observer_identities.getPtr(key) orelse return error.ResourceAccountingMismatch;
        const state = &self.slices[sliceIndex(slice)];
        if (owned.current != current.* or current.* > state.used_bytes or current.* > self.memory.used_bytes -| self.identityMetadataCharge() or
            debt.* > current.* or amount > current.* - debt.*)
            return error.ResourceAccountingMismatch;
        debt.* += amount;
        retirement.retire();
    }

    fn adjustUsageControlled(self: *ResourceManager, slice: Slice, current: *u64, next: u64, control: ?read_admission.Port) !void {
        errdefer if (control == null and builtin.link_libc) {
            if (std.c.getenv("ANTFLY_RESOURCE_ALLOCATION_DIAGNOSTICS")) |raw| {
                if (std.mem.eql(u8, std.mem.span(raw), "1")) {
                    const snapshot_stats = self.snapshot();
                    const state = snapshot_stats.slices[sliceIndex(slice)];
                    std.log.warn("observer admission denied slice={s} current={d} next={d} slice_used={d} slice_limit={d} aggregate_used={d} aggregate_limit={d}", .{ @tagName(slice), current.*, next, state.used_bytes, state.hard_limit_bytes, snapshot_stats.memory.used_bytes, snapshot_stats.memory.hard_limit_bytes });
                    std.debug.dumpCurrentStackTrace(.{});
                }
            }
        };
        while (true) {
            self.adjustUsageOnceControlled(slice, current, next, control) catch |err| {
                if (err == error.ParentMemoryBusy) {
                    // The failed attempt released the child ledger lock and
                    // published no reservation. Spend only the caller's scope.
                    try control.?.waitForProgress();
                    continue;
                }
                if (err != error.ResourceBudgetExceeded or next <= current.*) return err;
                if (try self.reclaimForAllocationControlled(slice, next - current.*, control) == 0) return err;
                // A parent can become busy after reclamation; retry through
                // the same original budget, not an unbounded second attempt.
                self.adjustUsageOnceControlled(slice, current, next, control) catch |retry_err| {
                    if (retry_err == error.ParentMemoryBusy) {
                        try control.?.waitForProgress();
                        continue;
                    }
                    return retry_err;
                };
            };
            return;
        }
    }

    /// Atomically admits as much of a requested observer growth as the slice
    /// and aggregate memory limits currently permit. A full admission first
    /// gets the normal reclamation opportunity; the partial fallback is
    /// selected and recorded under one lock so concurrent slice users cannot
    /// invalidate a stats-based estimate between observation and reservation.
    pub fn adjustUsageAtMost(
        self: *ResourceManager,
        slice: Slice,
        current: *u64,
        requested_additional: u64,
    ) !u64 {
        if (requested_additional == 0) return 0;
        const requested_next = std.math.add(u64, current.*, requested_additional) catch
            return error.ResourceBudgetExceeded;
        self.adjustUsage(slice, current, requested_next) catch |err| {
            if (err != error.ResourceBudgetExceeded) return err;
            return self.adjustUsageAtMostOnce(slice, current, requested_additional);
        };
        return requested_additional;
    }

    fn adjustUsageAtMostOnce(
        self: *ResourceManager,
        slice: Slice,
        current: *u64,
        requested_additional: u64,
    ) !u64 {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const state = &self.slices[sliceIndex(slice)];
        const slice_available = if (state.budget.hard_limit_bytes == 0)
            requested_additional
        else
            state.budget.hard_limit_bytes -| state.used_bytes;
        const memory_available = if (self.memory.budget.hard_limit_bytes == 0)
            requested_additional
        else
            self.memory.budget.hard_limit_bytes -| self.memory.used_bytes;
        const granted = @min(requested_additional, @min(@min(slice_available, memory_available), if (self.memory_parent) |parent| parent.available() else std.math.maxInt(u64)));
        if (granted == 0) return error.ResourceBudgetExceeded;
        const next = std.math.add(u64, current.*, granted) catch return error.ResourceBudgetExceeded;
        try self.reconcileUsageLocked(slice, @intFromPtr(current), current.*, next, true);
        current.* = next;
        return granted;
    }

    /// Transfer an unchanged charge between stable, already prepared owners.
    /// No allocation, parent callback, quota change or raw waiting occurs here.
    /// Publish the matching physical ownership receipt before another fallible
    /// operation. Both zero identities remain registered for explicit retirement.
    pub fn transferPreparedUsageWithAdmission(self: *ResourceManager, slice: Slice, source: *u64, destination: *u64, amount: u64, control: read_admission.Port) !void {
        if (source == destination) return error.InvalidArgument;
        while (true) {
            self.transferPreparedUsageOnce(slice, source, destination, amount, control) catch |cause| {
                if (cause != error.ParentMemoryBusy) return cause;
                try control.waitForProgress();
                continue;
            };
            return;
        }
    }

    fn transferPreparedUsageOnce(self: *ResourceManager, slice: Slice, source: *u64, destination: *u64, amount: u64, control: read_admission.Port) !void {
        try @import("../common/bounded_mutex.zig").lockAtomic(&self.mutex, control);
        defer self.mutex.unlock();
        if (!self.pressure_change.tryLock()) return error.ParentMemoryBusy;
        defer self.pressure_change.unlock();
        const from = self.observer_identities.getPtr(.{ .slice = slice, .identity = @intFromPtr(source) }) orelse return error.ResourceAccountingMismatch;
        const to = self.observer_identities.getPtr(.{ .slice = slice, .identity = @intFromPtr(destination) }) orelse return error.ResourceAccountingMismatch;
        if (from.current != source.* or from.host_current != source.* or
            to.current != destination.* or to.host_current != destination.* or amount > source.*)
            return error.ResourceAccountingMismatch;
        const next = std.math.add(u64, destination.*, amount) catch return error.ResourceBudgetExceeded;
        from.current -= amount;
        from.host_current -= amount;
        to.current = next;
        to.host_current = next;
        source.* = from.current;
        destination.* = next;
        self.pressure_change.advanceLocked();
    }

    /// Atomically moves accounting between two observer-owned contributions in
    /// the same slice. This is the ownership handoff used when a pre-admitted
    /// retained reservation becomes a live cache object: the allocation is
    /// never invisible, but it is also never double-counted in pressure or peak
    /// telemetry. The destination identity may be created on this fallible path
    /// before the object is published.
    pub fn transferUsage(
        self: *ResourceManager,
        slice: Slice,
        source: *u64,
        source_next: u64,
        destination: *u64,
        destination_next: u64,
    ) !void {
        if (source == destination) return error.InvalidArgument;
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const source_key = ObserverKey{ .slice = slice, .identity = @intFromPtr(source) };
        const destination_key = ObserverKey{ .slice = slice, .identity = @intFromPtr(destination) };
        const source_owned = self.observer_identities.get(source_key) orelse {
            self.memory.accounting_errors +|= 1;
            return error.ResourceAccountingMismatch;
        };
        if (source_owned.current != source.* or source_next > source_owned.current) {
            self.memory.accounting_errors +|= 1;
            return error.ResourceAccountingMismatch;
        }
        if (source_owned.host_current != source_owned.current) return error.ResourceAccountingMismatch;
        const destination_previous = if (self.observer_identities.get(destination_key)) |owned| blk: {
            if (owned.current != destination.* or owned.host_current != owned.current) {
                self.memory.accounting_errors +|= 1;
                return error.ResourceAccountingMismatch;
            }
            break :blk owned.current;
        } else blk: {
            if (destination.* != 0) {
                self.memory.accounting_errors +|= 1;
                return error.ResourceAccountingMismatch;
            }
            break :blk 0;
        };

        var inserted_destination = false;
        if (destination_previous == 0 and destination_next != 0 and
            !self.observer_identities.contains(destination_key))
        {
            const entry = try self.observer_identities.getOrPut(self, destination_key);
            entry.value_ptr.* = .{ .current = 0 };
            inserted_destination = true;
        }
        errdefer {
            if (inserted_destination) _ = self.observer_identities.remove(destination_key);
        }

        const previous_total = std.math.add(u64, source_owned.current, destination_previous) catch {
            self.memory.accounting_errors +|= 1;
            return error.ResourceAccountingMismatch;
        };
        const next_total = std.math.add(u64, source_next, destination_next) catch
            return error.ResourceBudgetExceeded;
        const state = &self.slices[sliceIndex(slice)];
        if (previous_total > state.used_bytes or previous_total > self.memory.used_bytes -| self.identityMetadataCharge()) {
            self.memory.accounting_errors +|= 1;
            return error.ResourceAccountingMismatch;
        }
        const slice_next = std.math.add(u64, state.used_bytes - previous_total, next_total) catch
            return error.ResourceBudgetExceeded;
        const memory_next = std.math.add(u64, self.memory.used_bytes - previous_total, next_total) catch
            return error.ResourceBudgetExceeded;
        if (next_total > previous_total and
            ((state.budget.hard_limit_bytes > 0 and slice_next > state.budget.hard_limit_bytes) or
                (self.memory.budget.hard_limit_bytes > 0 and memory_next > self.memory.budget.hard_limit_bytes)))
        {
            state.hard_limit_rejections +|= 1;
            if (self.memory.budget.hard_limit_bytes > 0 and memory_next > self.memory.budget.hard_limit_bytes)
                self.memory.hard_limit_rejections +|= 1;
            return error.ResourceBudgetExceeded;
        }

        if (memory_next > self.memory.used_bytes) {
            try self.reserveParentLocked(memory_next - self.memory.used_bytes);
        } else if (memory_next < self.memory.used_bytes) {
            if (self.memory_parent) |parent| parent.release_fn(parent.ptr, self.memory.used_bytes - memory_next);
        }

        if (source_next == 0) {
            _ = self.observer_identities.remove(source_key);
        } else {
            self.observer_identities.getPtr(source_key).?.current = source_next;
            self.observer_identities.getPtr(source_key).?.host_current = source_next;
        }
        if (destination_next == 0) {
            _ = self.observer_identities.remove(destination_key);
        } else {
            self.observer_identities.getPtr(destination_key).?.current = destination_next;
            self.observer_identities.getPtr(destination_key).?.host_current = destination_next;
        }
        source.* = source_next;
        destination.* = destination_next;
        state.used_bytes = slice_next;
        self.memory.used_bytes = memory_next;
        state.peak_bytes = @max(state.peak_bytes, slice_next);
        self.memory.peak_bytes = @max(self.memory.peak_bytes, memory_next);
        if (state.budget.soft_limit_bytes > 0 and slice_next > state.budget.soft_limit_bytes)
            state.soft_limit_events +|= 1;
        if (self.memory.budget.soft_limit_bytes > 0 and memory_next > self.memory.budget.soft_limit_bytes)
            self.memory.soft_limit_events +|= 1;
        self.notifyPressureChange();
    }

    fn adjustUsageOnce(self: *ResourceManager, slice: Slice, current: *u64, next: u64) !void {
        return self.adjustUsageOnceControlled(slice, current, next, null);
    }

    fn adjustUsageOnceControlled(self: *ResourceManager, slice: Slice, current: *u64, next: u64, control: ?read_admission.Port) !void {
        if (control) |budget| try @import("../common/bounded_mutex.zig").lockAtomic(&self.mutex, budget) else lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        try self.reconcileUsageLockedWithParentAdmission(slice, @intFromPtr(current), current.*, next, true, control != null);
        current.* = next;
    }

    /// Keep logical residency telemetry separate from owned host memory.
    /// File-backed mappings are reclaimable address space, not heap claims.
    pub fn observeUsageWithHostCharge(self: *ResourceManager, slice: Slice, current: *u64, next: u64, host_next: u64) void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        self.reconcileUsageWithHostChargeLocked(slice, @intFromPtr(current), current.*, next, host_next, false) catch return;
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
            .control_object_bytes = self.control_object_bytes,
            .identity_metadata_bytes = self.identity_metadata_bytes,
            .identity_metadata_retirement_bytes = self.identity_metadata_retirement_bytes,
            .identity_metadata_limit_bytes = self.identity_metadata_limit_bytes,
            .memory = .{
                .used_bytes = self.memory.used_bytes,
                .peak_bytes = self.memory.peak_bytes,
                .soft_limit_bytes = self.memory.budget.soft_limit_bytes,
                .hard_limit_bytes = self.memory.budget.hard_limit_bytes,
                .soft_limit_events = self.memory.soft_limit_events,
                .hard_limit_rejections = self.memory.hard_limit_rejections,
                .external_bytes = self.memory.external_bytes,
                .accounting_errors = self.memory.accounting_errors,
                .pressure = pressureFor(self.memory.budget, self.memory.used_bytes),
            },
            .slices = stats,
            .reclaim_requests = self.reclaim_requests.load(.monotonic),
            .reclaimed_bytes = self.reclaimed_bytes.load(.monotonic),
            .dense_search_admission = self.denseSearchAdmissionStats(),
            .dense_read_tasks = self.denseReadTaskStats(),
        };
    }

    pub fn sliceStats(self: *ResourceManager, slice: Slice) SliceStats {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const state = self.slices[sliceIndex(slice)];
        return sliceStatsFromState(slice, state);
    }

    pub fn sliceStatsWithAdmission(self: *ResourceManager, slice: Slice, control: read_admission.Port) !SliceStats {
        try @import("../common/bounded_mutex.zig").lockAtomic(&self.mutex, control);
        defer self.mutex.unlock();
        return sliceStatsFromState(slice, self.slices[sliceIndex(slice)]);
    }

    /// Advisory headroom for sizing a bounded unit of work. Admission still
    /// checks both limits atomically; concurrent users can consume this space.
    /// Unlike snapshot(), this avoids collecting unrelated slice statistics.
    pub fn availableAdmissionBytes(self: *ResourceManager, slice: Slice) u64 {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const state = self.slices[sliceIndex(slice)];
        const slice_available = if (state.budget.hard_limit_bytes == 0)
            std.math.maxInt(u64)
        else
            state.budget.hard_limit_bytes -| state.used_bytes;
        const memory_available = if (self.memory.budget.hard_limit_bytes == 0)
            std.math.maxInt(u64)
        else
            self.memory.budget.hard_limit_bytes -| self.memory.used_bytes;
        return @min(@min(slice_available, memory_available), if (self.memory_parent) |parent| parent.available() else std.math.maxInt(u64));
    }

    /// Stable capacity, not momentary free space: durable transaction admission
    /// must not turn unrelated concurrent requests into permanent size limits.
    /// Zero means neither the node nor this slice has a hard limit.
    pub fn memoryHardLimitForSlice(self: *ResourceManager, slice: Slice) u64 {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const parent_hard = if (self.memory_parent) |parent| parent.snapshot().hard_limit_bytes else 0;
        const own_hard = self.memory.budget.hard_limit_bytes;
        const node = if (own_hard == 0) parent_hard else if (parent_hard == 0) own_hard else @min(own_hard, parent_hard);
        const local = self.slices[sliceIndex(slice)].budget.hard_limit_bytes;
        if (node == 0) return local;
        if (local == 0) return node;
        return @min(node, local);
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
        return self.admissionDecisionLocked(slice, additional_bytes, if (self.memory_parent) |parent| parent.snapshot() else null, self.pressureEpoch());
    }

    /// Observe the parent before acquiring the child ledger. Progress must never
    /// reenter a held child lock; parent and child contention use the same port.
    /// Controlled callers poll pressure, so they need no legacy epoch callback.
    pub fn admissionDecisionWithAdmission(self: *ResourceManager, slice: Slice, additional_bytes: u64, control: read_admission.Port) !PressureDecision {
        const parent_snapshot = if (self.memory_parent) |parent| try snapshotParentWithAdmission(parent, control) else null;
        try @import("../common/bounded_mutex.zig").lockAtomic(&self.mutex, control);
        defer self.mutex.unlock();
        const decision = self.admissionDecisionLocked(slice, additional_bytes, parent_snapshot, 0);
        try control.checkpoint();
        return decision;
    }

    fn admissionDecisionLocked(self: *ResourceManager, slice: Slice, additional_bytes: u64, parent_snapshot: ?@import("runtime_memory_abi").ParentSnapshot, change_epoch: u64) PressureDecision {
        const state = self.slices[sliceIndex(slice)];
        const projected_bytes = state.used_bytes +| additional_bytes;
        const slice_pressure = pressureFor(state.budget, projected_bytes);
        const memory_budget: Budget = if (parent_snapshot) |parent| .{
            .soft_limit_bytes = parent.soft_limit_bytes,
            .hard_limit_bytes = parent.hard_limit_bytes,
        } else self.memory.budget;
        const projected_memory = (if (parent_snapshot) |parent| parent.used_bytes else self.memory.used_bytes) +| additional_bytes;
        const memory_pressure = pressureFor(memory_budget, projected_memory);
        const aggregate_dominates = memory_pressure != .normal and
            @backingInt(memory_pressure) >= @backingInt(slice_pressure);
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
            .soft_limit_bytes = if (aggregate_dominates) memory_budget.soft_limit_bytes else state.budget.soft_limit_bytes,
            .hard_limit_bytes = if (aggregate_dominates) memory_budget.hard_limit_bytes else state.budget.hard_limit_bytes,
            .change_epoch = change_epoch,
        };
    }

    pub fn canWaitForPressureChange(_: *const ResourceManager) bool {
        return supports_pressure_wait;
    }

    pub fn waitForPressureChange(self: *ResourceManager, observed_epoch: u64) void {
        if (self.memory_parent) |parent| parent.wait_fn(parent.ptr, observed_epoch) else self.pressure_change.waitForChange(observed_epoch);
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

    /// Original request authority bounds pressure waiting. The synchronous port
    /// is never retained, and admission still reserves no bytes. Recheck at the
    /// actual allocation/WAL boundary as for legacy awaitAdmission.
    pub fn awaitAdmissionWithAdmission(self: *ResourceManager, slice: Slice, additional_bytes: u64, control: read_admission.Port) !void {
        try @import("pressure_admission.zig").awaitAdmission(self, slice, additional_bytes, control);
    }

    /// Explicit pre-decision policy; this creates no reservation or new scope.
    /// Non-lending soft pacing is skipped, while the allocation/WAL guard still
    /// rejects hard overcommit. Mandatory callers keep the existing method.
    pub fn awaitAdmissionPreDecisionWithAdmission(self: *ResourceManager, slice: Slice, additional_bytes: u64, control: read_admission.Port) !void {
        try @import("pressure_admission.zig").awaitAdmissionPreDecision(self, slice, additional_bytes, control);
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
                // Normal-pressure serial searches should warm decoded
                // residency eagerly. The cache adds an overlap-aware
                // doorkeeper while another decoded fill is active and also
                // samples replacement once the byte target is full.
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
        const node_share = mulDivSaturating(adaptive_pool, self.hbc_cache_benefit[@backingInt(HbcCacheClass.node)].score, total_score);
        const quantized_share = mulDivSaturating(adaptive_pool, self.hbc_cache_benefit[@backingInt(HbcCacheClass.quantized)].score, total_score);
        const vector_share = mulDivSaturating(adaptive_pool, self.hbc_cache_benefit[@backingInt(HbcCacheClass.vector)].score, total_score);
        const metadata_share = mulDivSaturating(adaptive_pool, self.hbc_cache_benefit[@backingInt(HbcCacheClass.metadata)].score, total_score);
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

    pub fn observeHbcCacheBenefitSampled(self: *ResourceManager, samples: [@typeInfo(HbcCacheClass).@"enum".field_names.len]HbcCacheBenefitSample) void {
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
    pub fn observeHbcCacheBenefit(self: *ResourceManager, samples: [@typeInfo(HbcCacheClass).@"enum".field_names.len]HbcCacheBenefitSample) void {
        if (!self.beginHbcCacheBenefitSample()) return;
        self.observeHbcCacheBenefitSampled(samples);
    }

    pub fn denseReplayWindowBudget(self: *ResourceManager, options: DenseReplayWindowBudgetOptions) u64 {
        return self.denseReplayWindowLimits(options).working_set_bytes;
    }

    /// Adapt in original work units. Scale only the memory estimate, then
    /// cap it against the same live slice headroom under this lock.
    pub fn denseReplayWindowLimits(self: *ResourceManager, options: DenseReplayWindowBudgetOptions) DenseReplayWindowLimits {
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
        const factor = @max(@as(u64, 1), options.working_set_factor);
        if (factor == 1) return .{ .work_bytes = current, .working_set_bytes = current };
        const memory_cap = self.denseReplayWindowHardCapLocked(.{
            .default_bytes = options.default_bytes *| factor,
            .max_bytes = options.max_bytes *| factor,
            .min_bytes = options.min_bytes,
        });
        return .{ .work_bytes = current, .working_set_bytes = @min(current *| factor, memory_cap) };
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

/// Embedded in a charged physical owner. Registration is serialized by the
/// manager; final release mutates only readiness, then never accesses the node.
pub const PayloadRetirement = struct {
    previous: ?*PayloadRetirement = null,
    next: ?*PayloadRetirement = null,
    credit: ?*Reservation = null,
    context: ?*anyopaque = null,
    destroy: ?*const fn (*anyopaque) void = null,
    ready: std.atomic.Value(bool) = .init(false),

    pub fn publish(self: *PayloadRetirement) void {
        const manager = self.credit.?.manager;
        _ = manager.payload_retirement_ready.fetchAdd(1, .acq_rel);
        self.ready.store(true, .release); // Final access to the physical owner.
        manager.notifyCleanupReady();
    }
};

const ConstructionCleanup = struct {
    const State = enum(u8) { free, active, ready, draining };
    state: std.atomic.Value(State) = .init(.free),
    credit: ?Reservation = null,
};

/// The manager outlives the construction attempt and any retained retirement
/// debt. This token contains no borrowed admission or allocator callback.
pub const ConstructionCredit = struct {
    manager: *ResourceManager,
    slot: ?*ConstructionCleanup,

    /// Adopt only prepaid bytes into a stable retained observer. Keep the
    /// cleanup slot and unused reservation owned until construction/retirement
    /// finishes, including when the original rejects a ledger or parent wait.
    pub fn adoptUsageWithAdmission(self: *ConstructionCredit, destination: Slice, current: *u64, bytes: u64, control: read_admission.Port) !void {
        const slot = self.slot orelse return error.ReservationReleased;
        const credit = &slot.credit.?;
        if (bytes > credit.bytes) return error.ResourceBudgetExceeded;
        if (bytes == 0) return control.checkpoint();
        try self.manager.adoptReservationUsageWithAdmission(credit, destination, current, bytes, control);
    }

    /// Transfer successful construction credit to the published physical owner.
    pub fn transfer(self: *ConstructionCredit) Reservation {
        const slot = self.slot orelse @panic("construction credit already consumed");
        const credit = slot.credit.?;
        slot.credit = null;
        self.slot = null;
        slot.state.store(.free, .release);
        return credit;
    }

    /// Call after failed physical work has joined and its allocations are gone.
    /// One finite accounting attempt; contention retains the exact charge.
    pub fn retire(self: *ConstructionCredit) void {
        const slot = self.slot orelse return;
        self.slot = null;
        slot.state.store(.ready, .release);
        self.manager.notifyCleanupReady();
        _ = self.manager.tryDrainConstructionSlot(slot);
    }
};

pub const Reservation = struct {
    manager: *ResourceManager,
    identity: u64,
    slice: Slice,
    bytes: u64,
    /// Admission against a slice may refer to residency already charged by another owner.
    host_charge: bool = true,
    released: bool = false,

    pub fn reservedBytes(self: *const Reservation) u64 {
        return self.bytes;
    }

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

    pub fn reclassify(self: *Reservation, destination: Slice) !void {
        if (self.released) return error.ReservationReleased;
        if (self.bytes == 0 and self.identity == 0) {
            self.slice = destination;
            return;
        }
        try self.manager.reclassifyReservation(self, destination);
    }

    pub fn reclassifyWithAdmission(self: *Reservation, destination: Slice, control: read_admission.Port) !void {
        if (self.released) return error.ReservationReleased;
        if (self.bytes == 0 and self.identity == 0) {
            try control.checkpoint();
            self.slice = destination;
            return;
        }
        while (true) {
            self.manager.reclassifyReservationControlled(self, destination, control) catch |err| {
                if (err != error.ParentMemoryBusy) return err;
                try control.waitForProgress();
                continue;
            };
            return;
        }
    }

    pub fn transferCreditTo(self: *Reservation, destination: *Reservation, bytes: u64) !void {
        if (self.released or destination.released) return error.ReservationReleased;
        try self.manager.transferReservationCredit(self, destination, bytes);
    }
};

/// One ResourceManager reservation split into independently enforced owner
/// credits. The manager accounts the sum; callers cap the native and allocator
/// owners at `primary_bytes` and `secondary_bytes` respectively.
pub const OwnedSplitReservation = struct {
    reservation: Reservation,
    primary_bytes: u64,
    secondary_bytes: u64,

    pub fn release(self: *OwnedSplitReservation) void {
        self.reservation.release();
    }

    /// Hand secondary credit to an allocator using its normal idle-credit
    /// reclamation policy. The ResourceManager total is unchanged; only
    /// reservation ownership moves.
    pub fn transferSecondaryTo(self: *OwnedSplitReservation, destination: *BudgetedAllocator) !void {
        if (self.secondary_bytes == 0) return;
        try destination.adoptReservationCredit(&self.reservation, self.secondary_bytes);
        self.secondary_bytes = 0;
    }

    /// Transfer a credit that must remain available across several bounded
    /// allocation windows. The destination releases it only at deinit.
    pub fn transferSecondaryToPinned(self: *OwnedSplitReservation, destination: *BudgetedAllocator) !void {
        if (self.secondary_bytes == 0) return;
        try destination.adoptPinnedReservationCredit(&self.reservation, self.secondary_bytes);
        self.secondary_bytes = 0;
    }
};

/// Accounts allocator-backed working sets before each allocation reaches the
/// backing allocator. One operation may make bounded progress above the normal
/// hard limit only while it is the slice's sole user.
pub const BudgetedAllocator = struct {
    pub const AllocationFailure = struct {
        cause: enum { admission, backing },
        requested_bytes: usize,
        live_bytes: u64,
        slice_used_bytes: u64,
        slice_limit_bytes: u64,
        aggregate_used_bytes: u64,
        aggregate_limit_bytes: u64,
        return_address: usize,
    };

    allocator_mutex: std.atomic.Mutex = .unlocked,
    backing: std.mem.Allocator,
    reservation: Reservation,
    max_hard_limit_multiple: u64,
    live_bytes: u64 = 0,
    borrowed_bytes: u64 = 0,
    pinned_bytes: u64 = 0,
    credit_quantum: u64,
    budget_denied: bool = false,
    denial_generation: u64 = 0,
    reclaim_on_denial: bool = false,
    reclaim_deficit: u64 = 0,
    last_allocation_failure: ?AllocationFailure = null,
    reservation_floor: u64 = 0,

    /// Independent admission for borrowed working storage. Unlike nested
    /// scratch floors, these leases may be released in any order.
    pub const BorrowedScratch = struct {
        owner: *BudgetedAllocator,
        bytes: usize = 0,
        reservation: Reservation,

        pub fn growTo(self: *BorrowedScratch, bytes: usize) !void {
            if (self.reservation.released) return error.ReservationReleased;
            if (bytes <= self.bytes) return;
            lockAtomic(&self.owner.allocator_mutex);
            defer self.owner.allocator_mutex.unlock();
            const growth = bytes - self.bytes;
            const other_borrowed = std.math.sub(u64, self.owner.borrowed_bytes, self.bytes) catch return error.ResourceAccountingMismatch;
            const other = std.math.add(u64, self.owner.reservation.bytes, other_borrowed) catch return error.ResourceBudgetExceeded;
            _ = self.reservation.manager.growReservationAmortized(&self.reservation, growth, growth, self.owner.max_hard_limit_multiple, other) catch |err| {
                self.owner.recordDenial();
                self.owner.recordAllocationFailure(.admission, growth, @returnAddress());
                return err;
            };
            self.owner.borrowed_bytes += growth;
            self.owner.live_bytes += growth;
            self.bytes = bytes;
        }

        pub fn shrinkTo(self: *BorrowedScratch, bytes: usize) void {
            std.debug.assert(bytes <= self.bytes);
            lockAtomic(&self.owner.allocator_mutex);
            defer self.owner.allocator_mutex.unlock();
            const released = self.bytes - bytes;
            if (!self.reservation.manager.shrinkReservation(&self.reservation, released)) return;
            self.owner.borrowed_bytes -= released;
            self.owner.live_bytes -= released;
            self.bytes = bytes;
            self.owner.releaseBytes(0);
        }

        pub fn release(self: *BorrowedScratch) void {
            if (self.reservation.released) return;
            self.shrinkTo(0);
            self.reservation.release();
        }
    };

    pub fn borrowScratch(self: *BudgetedAllocator, bytes: usize) !BorrowedScratch {
        return self.borrowResidentScratch(bytes, null);
    }

    /// Enforce query admission without charging already accounted residency twice.
    pub fn borrowResidentScratch(self: *BudgetedAllocator, bytes: usize, resident_manager: ?*ResourceManager) !BorrowedScratch {
        var receipt = BorrowedScratch{ .owner = self, .reservation = .{
            .manager = self.reservation.manager,
            .identity = 0,
            .slice = self.reservation.slice,
            .bytes = 0,
            .host_charge = resident_manager != self.reservation.manager,
        } };
        errdefer receipt.reservation.release();
        try receipt.growTo(bytes);
        return receipt;
    }

    pub const ScratchReservation = struct {
        owner: *BudgetedAllocator,
        previous_floor: u64,

        pub fn release(self: ScratchReservation) void {
            lockAtomic(&self.owner.allocator_mutex);
            defer self.owner.allocator_mutex.unlock();
            self.owner.reservation_floor = self.previous_floor;
            self.owner.releaseBytes(0);
        }
    };

    /// Admit an operation's temporary working set before durable mutation.
    /// Allocations consume these same credits instead of being charged twice.
    /// Scopes nest and release in reverse order under owner serialization.
    pub fn reserveScratch(self: *BudgetedAllocator, bytes: usize) !ScratchReservation {
        for (0..2) |attempt| {
            lockAtomic(&self.allocator_mutex);
            const previous_floor = self.reservation_floor;
            if (self.reserveGrowth(bytes, false)) {
                self.reservation_floor = @max(previous_floor, self.live_bytes - self.borrowed_bytes);
                self.live_bytes -= bytes;
                self.allocator_mutex.unlock();
                return .{ .owner = self, .previous_floor = previous_floor };
            }
            self.recordAllocationFailure(.admission, bytes, @returnAddress());
            const deficit = self.reclaim_deficit;
            self.allocator_mutex.unlock();
            if (attempt == 1 or !self.reclaim_on_denial or deficit == 0 or self.reservation.manager.reclaimForAllocation(self.reservation.slice, deficit) == 0)
                return error.ResourceBudgetExceeded;
        }
        unreachable;
    }

    pub fn allocationFailureThreadSafe(self: *BudgetedAllocator) ?AllocationFailure {
        lockAtomic(&self.allocator_mutex);
        defer self.allocator_mutex.unlock();
        return self.last_allocation_failure;
    }

    fn recordAllocationFailure(self: *BudgetedAllocator, cause: @FieldType(AllocationFailure, "cause"), bytes: usize, ret_addr: usize) void {
        const snapshot = self.reservation.manager.snapshot();
        const slice = snapshot.slices[@backingInt(self.reservation.slice)];
        self.last_allocation_failure = .{
            .cause = cause,
            .requested_bytes = bytes,
            .live_bytes = self.live_bytes,
            .slice_used_bytes = slice.used_bytes,
            .slice_limit_bytes = slice.hard_limit_bytes,
            .aggregate_used_bytes = snapshot.memory.used_bytes,
            .aggregate_limit_bytes = snapshot.memory.hard_limit_bytes,
            .return_address = ret_addr,
        };
        // Opt-in stack diagnostics allocate through the debug runtime, never
        // this allocator. Normal admission records only a bounded receipt.
        if (@import("builtin").link_libc) {
            const general = if (std.c.getenv("ANTFLY_RESOURCE_ALLOCATION_DIAGNOSTICS")) |raw| std.mem.eql(u8, std.mem.span(raw), "1") else false;
            const source = self.reservation.slice == .dense_source_payload_state and if (std.c.getenv("ANTFLY_SOURCE_VECTOR_ALLOCATION_DIAGNOSTICS")) |raw| std.mem.eql(u8, std.mem.span(raw), "1") else false;
            if (general or source) {
                std.log.warn("allocation denied slice={s} cause={s} requested={d} live={d} slice_used={d} slice_limit={d} aggregate_used={d} aggregate_limit={d} caller=0x{x}", .{ @tagName(self.reservation.slice), @tagName(cause), bytes, self.live_bytes, slice.used_bytes, slice.hard_limit_bytes, snapshot.memory.used_bytes, snapshot.memory.hard_limit_bytes, ret_addr });
                std.debug.dumpCurrentStackTrace(.{});
            }
        }
    }

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

    /// Opt in only at allocation sites whose lock order permits invoking cache
    /// owners. Ordinary init remains callback-free for storage transactions and
    /// cache-internal allocators. Admission retries once, without raising limits.
    pub fn initReclaiming(
        manager: *ResourceManager,
        slice: Slice,
        backing: std.mem.Allocator,
        max_hard_limit_multiple: u64,
    ) BudgetedAllocator {
        var self = init(manager, slice, backing, max_hard_limit_multiple);
        self.reclaim_on_denial = true;
        return self;
    }

    pub fn deinit(self: *BudgetedAllocator) void {
        std.debug.assert(self.borrowed_bytes == 0);
        self.reservation.release();
        self.* = undefined;
    }

    /// Recognize only our own allocator and standard arena wrappers. Unknown
    /// wrappers retain their original allocation path and budget semantics.
    pub fn fromAllocator(candidate: std.mem.Allocator) ?*BudgetedAllocator {
        var current = candidate;
        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        const arena_vtable = arena.allocator().vtable;
        for (0..8) |_| {
            if (current.vtable.alloc == alloc or current.vtable.alloc == lockedAlloc or current.vtable.alloc == lockedNoReclaimAlloc)
                return @ptrCast(@alignCast(current.ptr));
            if (current.vtable != arena_vtable) return null;
            const wrapped: *std.heap.ArenaAllocator = @ptrCast(@alignCast(current.ptr));
            current = wrapped.child_allocator;
        }
        return null;
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

    /// Shared immutable generations may release their final allocation on a
    /// query thread while the source writer allocates a successor. Owners that
    /// publish such leases must use this interface for every allocation.
    /// Reclamation runs outside the owner lock; admission is revalidated on retry.
    pub fn threadSafeAllocator(self: *BudgetedAllocator) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = lockedAlloc, .resize = lockedResize, .remap = lockedRemap, .free = lockedFree } };
    }

    /// Share the same ledger and physical owner while suppressing this
    /// budget's reclamation hooks for every operation. Suitable for scratch
    /// under storage dependencies; the backing allocator must obey that lock
    /// contract too. Does not mutate the owner's policy or allocate a wrapper.
    pub fn threadSafeNoReclaimAllocator(self: *BudgetedAllocator) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = lockedNoReclaimAlloc, .resize = lockedNoReclaimResize, .remap = lockedNoReclaimRemap, .free = lockedFree } };
    }

    pub fn liveBytesThreadSafe(self: *BudgetedAllocator) u64 {
        lockAtomic(&self.allocator_mutex);
        defer self.allocator_mutex.unlock();
        return self.live_bytes;
    }

    const LockedAllocationResult = struct { ptr: ?[*]u8, cause: ?@FieldType(AllocationFailure, "cause") };
    fn lockedAlloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        return lockedAllocOutcome(ctx, len, alignment, ret_addr, true).ptr;
    }
    fn lockedNoReclaimAlloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        return lockedAllocOutcome(ctx, len, alignment, ret_addr, false).ptr;
    }
    fn lockedAllocOutcome(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize, allow_reclaim: bool) LockedAllocationResult {
        const self: *BudgetedAllocator = @ptrCast(@alignCast(ctx));
        lockAtomic(&self.allocator_mutex);
        const generation = self.denial_generation;
        const ptr = allocImpl(ctx, len, alignment, ret_addr, false);
        const result = LockedAllocationResult{ .ptr = ptr, .cause = if (ptr == null) self.last_allocation_failure.?.cause else null };
        const reclaim = allow_reclaim and ptr == null and self.reclaim_on_denial and self.denial_generation != generation;
        const deficit = if (reclaim) self.reclaim_deficit else 0;
        self.allocator_mutex.unlock();
        if (!reclaim or deficit == 0 or self.reservation.manager.reclaimForAllocation(self.reservation.slice, deficit) == 0) return result;
        lockAtomic(&self.allocator_mutex);
        defer self.allocator_mutex.unlock();
        const retried = allocImpl(ctx, len, alignment, ret_addr, false);
        return .{ .ptr = retried, .cause = if (retried == null) self.last_allocation_failure.?.cause else null };
    }
    fn lockedResize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        return lockedResizeWithPolicy(ctx, memory, alignment, new_len, ret_addr, true);
    }
    fn lockedNoReclaimResize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        return lockedResizeWithPolicy(ctx, memory, alignment, new_len, ret_addr, false);
    }
    fn lockedResizeWithPolicy(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize, allow_reclaim: bool) bool {
        const self: *BudgetedAllocator = @ptrCast(@alignCast(ctx));
        lockAtomic(&self.allocator_mutex);
        const generation = self.denial_generation;
        const result = resizeImpl(ctx, memory, alignment, new_len, ret_addr, false);
        const reclaim = allow_reclaim and !result and self.reclaim_on_denial and self.denial_generation != generation;
        const deficit = if (reclaim) self.reclaim_deficit else 0;
        self.allocator_mutex.unlock();
        if (!reclaim or deficit == 0 or self.reservation.manager.reclaimForAllocation(self.reservation.slice, deficit) == 0) return result;
        lockAtomic(&self.allocator_mutex);
        defer self.allocator_mutex.unlock();
        return resizeImpl(ctx, memory, alignment, new_len, ret_addr, false);
    }
    fn lockedRemap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        return lockedRemapWithPolicy(ctx, memory, alignment, new_len, ret_addr, true);
    }
    fn lockedNoReclaimRemap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        return lockedRemapWithPolicy(ctx, memory, alignment, new_len, ret_addr, false);
    }
    fn lockedRemapWithPolicy(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize, allow_reclaim: bool) ?[*]u8 {
        const self: *BudgetedAllocator = @ptrCast(@alignCast(ctx));
        lockAtomic(&self.allocator_mutex);
        const generation = self.denial_generation;
        const result = remapImpl(ctx, memory, alignment, new_len, ret_addr, false);
        const reclaim = allow_reclaim and result == null and self.reclaim_on_denial and self.denial_generation != generation;
        const deficit = if (reclaim) self.reclaim_deficit else 0;
        self.allocator_mutex.unlock();
        if (!reclaim or deficit == 0 or self.reservation.manager.reclaimForAllocation(self.reservation.slice, deficit) == 0) return result;
        lockAtomic(&self.allocator_mutex);
        defer self.allocator_mutex.unlock();
        return remapImpl(ctx, memory, alignment, new_len, ret_addr, false);
    }
    fn lockedFree(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *BudgetedAllocator = @ptrCast(@alignCast(ctx));
        lockAtomic(&self.allocator_mutex);
        defer self.allocator_mutex.unlock();
        free(ctx, memory, alignment, ret_addr);
    }

    /// Operation-local raw allocation failure receipt, captured under the allocator lock.
    /// Resize/remap refusals are advisory; a failed fallback alloc records its cause.
    /// Shared owners' failures cannot misclassify this operation's backing OOM
    /// as admission pressure. Storage remains owned by the original allocator.
    pub const FailureTrackingAllocator = struct {
        owner: *BudgetedAllocator,
        last_failure: ?@FieldType(AllocationFailure, "cause") = null,

        pub fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{ .alloc = trackedAlloc, .resize = trackedResize, .remap = trackedRemap, .free = trackedFree } };
        }

        fn trackedAlloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            const result = lockedAllocOutcome(self.owner, len, alignment, ra, true);
            self.last_failure = result.cause;
            return result.ptr;
        }
        fn trackedResize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ra: usize) bool {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            return lockedResize(self.owner, memory, alignment, new_len, ra);
        }
        fn trackedRemap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ra: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            return lockedRemap(self.owner, memory, alignment, new_len, ra);
        }
        fn trackedFree(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            lockedFree(self.owner, memory, alignment, ra);
        }
    };

    pub fn denied(self: *const BudgetedAllocator) bool {
        return self.budget_denied;
    }

    /// True when the latest denial would also happen with this allocator as
    /// the slice's sole owner: its own live bytes plus the failed request
    /// exceed what a sole owner may reserve. False means other owners' usage
    /// filled the slice, which is transient contention rather than evidence
    /// that the operation itself is oversized.
    pub fn deniedByOwnDemand(self: *const BudgetedAllocator) bool {
        std.debug.assert(self.budget_denied);
        const failure = self.last_allocation_failure orelse return true;
        if (failure.cause != .admission) return true;
        const requested = std.math.cast(u64, failure.requested_bytes) orelse return true;
        const own_need = std.math.add(u64, failure.live_bytes, requested) catch return true;
        // A sole owner may exceed the slice limit by the allocator's multiple,
        // but never the node-wide memory limit.
        if (failure.aggregate_limit_bytes > 0 and own_need > failure.aggregate_limit_bytes) return true;
        const limit = failure.slice_limit_bytes;
        if (limit == 0) return failure.aggregate_limit_bytes == 0;
        const sole_owner_limit = if (self.max_hard_limit_multiple <= 1)
            limit
        else
            std.math.mul(u64, limit, self.max_hard_limit_multiple) catch std.math.maxInt(u64);
        return own_need > sole_owner_limit;
    }

    /// Distinguish a new admission failure from backing-allocator resize or
    /// remap refusal without clearing the operation's sticky denied flag.
    pub fn denialGeneration(self: *const BudgetedAllocator) u64 {
        return self.denial_generation;
    }

    /// Pressure reclamation must return amortized spare credit as well as
    /// freed buffers, without disturbing live bytes or an invocation's pin.
    pub fn releaseUnusedCredit(self: *BudgetedAllocator) u64 {
        const bytes = self.reservation.bytes -| @max(self.live_bytes - self.borrowed_bytes, @max(self.pinned_bytes, self.reservation_floor));
        self.reservation.shrink(bytes);
        return bytes;
    }

    pub fn releaseUnusedCreditThreadSafe(self: *BudgetedAllocator) u64 {
        lockAtomic(&self.allocator_mutex);
        defer self.allocator_mutex.unlock();
        return self.releaseUnusedCredit();
    }

    fn recordDenial(self: *BudgetedAllocator) void {
        self.budget_denied = true;
        self.denial_generation +%= 1;
    }

    pub fn adoptReservationCredit(self: *BudgetedAllocator, source: *Reservation, bytes: u64) !void {
        try source.transferCreditTo(&self.reservation, bytes);
    }

    /// Adopt operation-lifetime credit. Ordinary frees may return any growth
    /// above this floor, but cannot discard the next window's admission.
    pub fn adoptPinnedReservationCredit(self: *BudgetedAllocator, source: *Reservation, bytes: u64) !void {
        const pinned = std.math.add(u64, self.pinned_bytes, bytes) catch
            return error.ResourceBudgetExceeded;
        try source.transferCreditTo(&self.reservation, bytes);
        self.pinned_bytes = pinned;
    }

    fn reserveGrowth(self: *BudgetedAllocator, bytes: usize, allow_reclaim: bool) bool {
        self.reclaim_deficit = 0;
        const amount = std.math.cast(u64, bytes) orelse {
            self.recordDenial();
            return false;
        };
        const next_live = std.math.add(u64, self.live_bytes, amount) catch {
            self.recordDenial();
            return false;
        };
        const physical_live = next_live - self.borrowed_bytes;
        if (physical_live > self.reservation.bytes) {
            const minimum = physical_live - self.reservation.bytes;
            self.reclaim_deficit = minimum;
            _ = self.reservation.manager.growReservationAmortized(
                &self.reservation,
                minimum,
                @max(minimum, self.credit_quantum),
                self.max_hard_limit_multiple,
                self.borrowed_bytes,
            ) catch |err| {
                // growReservationAmortized has released the accounting lock.
                // Ask only for the actual deficit, not amortized spare credit.
                if (!allow_reclaim or !self.reclaim_on_denial or err != error.ResourceBudgetExceeded or
                    self.reservation.manager.reclaimForAllocation(self.reservation.slice, minimum) == 0)
                {
                    self.recordDenial();
                    return false;
                }
                return self.reserveGrowth(bytes, false);
            };
        }
        self.live_bytes = next_live;
        return true;
    }

    fn releaseBytes(self: *BudgetedAllocator, bytes: usize) void {
        const amount = std.math.cast(u64, bytes) orelse std.math.maxInt(u64);
        self.live_bytes -|= amount;
        const physical_live = self.live_bytes - self.borrowed_bytes;
        if (physical_live == 0) {
            self.reservation.shrink(self.reservation.bytes -| @max(self.pinned_bytes, self.reservation_floor));
            return;
        }
        const spare = self.reservation.bytes -| physical_live;
        if (spare < self.credit_quantum *| 2) return;
        const retained_spare = @min(self.credit_quantum, self.reservation.bytes);
        const target = @max(@max(self.pinned_bytes, self.reservation_floor), physical_live +| retained_spare);
        if (self.reservation.bytes > target)
            self.reservation.shrink(self.reservation.bytes - target);
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        return allocImpl(ctx, len, alignment, ret_addr, true);
    }

    fn allocImpl(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize, allow_reclaim: bool) ?[*]u8 {
        const self: *BudgetedAllocator = @ptrCast(@alignCast(ctx));
        if (!self.reserveGrowth(len, allow_reclaim)) {
            self.recordAllocationFailure(.admission, len, ret_addr);
            return null;
        }
        return self.backing.rawAlloc(len, alignment, ret_addr) orelse {
            self.releaseBytes(len);
            self.recordAllocationFailure(.backing, len, ret_addr);
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
        return resizeImpl(ctx, memory, alignment, new_len, ret_addr, true);
    }

    fn resizeImpl(
        ctx: *anyopaque,
        memory: []u8,
        alignment: std.mem.Alignment,
        new_len: usize,
        ret_addr: usize,
        allow_reclaim: bool,
    ) bool {
        const self: *BudgetedAllocator = @ptrCast(@alignCast(ctx));
        const growth = new_len -| memory.len;
        if (growth > 0 and !self.reserveGrowth(growth, allow_reclaim)) {
            self.recordAllocationFailure(.admission, growth, ret_addr);
            return false;
        }
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
        return remapImpl(ctx, memory, alignment, new_len, ret_addr, true);
    }

    fn remapImpl(
        ctx: *anyopaque,
        memory: []u8,
        alignment: std.mem.Alignment,
        new_len: usize,
        ret_addr: usize,
        allow_reclaim: bool,
    ) ?[*]u8 {
        const self: *BudgetedAllocator = @ptrCast(@alignCast(ctx));
        const growth = new_len -| memory.len;
        if (growth > 0 and !self.reserveGrowth(growth, allow_reclaim)) {
            self.recordAllocationFailure(.admission, growth, ret_addr);
            return null;
        }
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

test "durable admission capacity honors both node and slice limits" {
    for ([_][3]u64{ .{ 0, 0, 0 }, .{ 1024, 0, 1024 }, .{ 0, 2048, 2048 }, .{ 1024, 2048, 1024 }, .{ 4096, 2048, 2048 } }) |limits| {
        var options = Options{ .identity_allocator = std.testing.allocator, .memory_budget = .{ .hard_limit_bytes = limits[0] } };
        options.budgets[sliceIndex(.relational_preparation_working_set)] = .{ .hard_limit_bytes = limits[1] };
        var manager = ResourceManager.init(options);
        defer manager.deinit(std.testing.allocator);
        try std.testing.expectEqual(limits[2], manager.memoryHardLimitForSlice(.relational_preparation_working_set));
    }
}

test "source vector payloads scratch admission accounts credits and records denial cause" {
    const alloc = std.testing.allocator;
    var budgets = Options.defaultBudgets();
    budgets[@backingInt(Slice.dense_source_payload_state)] = .{ .hard_limit_bytes = 4096 };
    var manager = ResourceManager.init(.{ .budgets = budgets });
    defer manager.deinit(alloc);
    var budget = BudgetedAllocator.init(&manager, .dense_source_payload_state, alloc, 1);
    defer budget.deinit();
    {
        const scratch = try budget.reserveScratch(1024);
        defer scratch.release();
        const buffer = try budget.threadSafeAllocator().alloc(u8, 512);
        budget.threadSafeAllocator().free(buffer);
        try std.testing.expect(budget.reservation.bytes >= 1024);
        try std.testing.expectError(error.ResourceBudgetExceeded, budget.reserveScratch(4097));
        try std.testing.expectEqual(@as(usize, 4097), budget.allocationFailureThreadSafe().?.requested_bytes);
        try std.testing.expect(budget.allocationFailureThreadSafe().?.cause == .admission);
    }
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.dense_source_payload_state).used_bytes);
    var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = 0 });
    var denied_backing = BudgetedAllocator.init(&manager, .dense_source_payload_state, failing.allocator(), 1);
    defer denied_backing.deinit();
    try std.testing.expectError(error.OutOfMemory, denied_backing.threadSafeAllocator().alloc(u8, 1));
    try std.testing.expect(denied_backing.allocationFailureThreadSafe().?.cause == .backing);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.dense_source_payload_state).used_bytes);
}

test "budgeted allocator denial separates slice contention from own oversize demand" {
    const alloc = std.testing.allocator;
    var budgets = Options.defaultBudgets();
    budgets[@backingInt(Slice.document_extraction_working_set)] = .{ .hard_limit_bytes = 4096 };
    var manager = ResourceManager.init(.{ .budgets = budgets });
    defer manager.deinit(alloc);

    var other = try manager.reserve(.document_extraction_working_set, 3900);
    var contended = BudgetedAllocator.init(&manager, .document_extraction_working_set, alloc, 1);
    defer contended.deinit();
    try std.testing.expectError(error.OutOfMemory, contended.threadSafeAllocator().alloc(u8, 512));
    try std.testing.expect(contended.denied());
    try std.testing.expect(!contended.deniedByOwnDemand());
    other.release();

    var oversized = BudgetedAllocator.init(&manager, .document_extraction_working_set, alloc, 1);
    defer oversized.deinit();
    try std.testing.expectError(error.OutOfMemory, oversized.threadSafeAllocator().alloc(u8, 4097));
    try std.testing.expect(oversized.denied());
    try std.testing.expect(oversized.deniedByOwnDemand());
}

test "budgeted allocator denial over the node memory limit is own demand" {
    const alloc = std.testing.allocator;
    var budgets = Options.defaultBudgets();
    budgets[@backingInt(Slice.document_extraction_working_set)] = .{ .hard_limit_bytes = 200 };
    var manager = ResourceManager.init(.{ .budgets = budgets, .memory_budget = .{ .hard_limit_bytes = 100 } });
    defer manager.deinit(alloc);

    var budgeted = BudgetedAllocator.init(&manager, .document_extraction_working_set, alloc, 1);
    defer budgeted.deinit();
    try std.testing.expectError(error.OutOfMemory, budgeted.threadSafeAllocator().alloc(u8, 150));
    try std.testing.expect(budgeted.denied());
    try std.testing.expect(budgeted.deniedByOwnDemand());
}

test "default tokenizer cache budget is aligned with its resource slice" {
    const budgets = Options.defaultBudgets();
    const policies = Options.defaultPolicies();
    const tokenizer_idx = @backingInt(Slice.inference_tokenizer_cache);
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

test "default lake range cache queue budget keeps its stable resource identity" {
    const budgets = Options.defaultBudgets();
    const policies = Options.defaultPolicies();
    const index = @backingInt(Slice.lake_range_cache_queue);
    try std.testing.expectEqual(@as(usize, 33), index);
    try std.testing.expectEqual(@as(u64, 384 * 1024 * 1024), budgets[index].soft_limit_bytes);
    try std.testing.expectEqual(@as(u64, 512 * 1024 * 1024), budgets[index].hard_limit_bytes);
    try std.testing.expectEqual(PressureAction.report, policies[index].soft_action);
    try std.testing.expectEqual(PressureAction.reject_work, policies[index].hard_action);
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
    try std.testing.expectEqual(@as(u64, 4096), metadata_fixture.payloadUsage(&manager));

    // The observed allocation and manager share this shutdown boundary. The
    // manager owns and frees the identity registry even when no final zero
    // observation is useful to a caller.
    manager.deinit(std.testing.allocator);
}

fn sliceIndex(slice: Slice) usize {
    return @backingInt(slice);
}

pub fn pressureFor(budget: Budget, used_bytes: u64) Pressure {
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
    @import("antfly_platform").sync.lockYielding(mutex);
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
    var manager = try metadata_fixture.init(.{ .budgets = budgets });

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
    var manager = try metadata_fixture.init(.{ .budgets = budgets });

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
    var manager = try metadata_fixture.init(.{
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
    try std.testing.expectEqual(@as(u64, 70), (stats.memory.used_bytes - manager.identity_metadata_bytes));
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_compaction_work).used_bytes);
    try std.testing.expectEqual(@as(u64, 1), stats.memory.hard_limit_rejections);
}

test "logical inference slices can charge only physical host memory" {
    var manager = try metadata_fixture.init(.{
        .memory_budget = .{ .hard_limit_bytes = 100 },
    });
    const logical = [_]SliceAmount{
        .{ .slice = .inference_model_residency, .bytes = 240 },
        .{ .slice = .inference_kv_working_set, .bytes = 60 },
    };
    var reservation = try manager.reserveBatchClassifiedWithHostCharge(&logical, 80);
    try std.testing.expectEqual(@as(u64, 300), manager.sliceStats(.inference_model_residency).used_bytes +
        manager.sliceStats(.inference_kv_working_set).used_bytes);
    try std.testing.expectEqual(@as(u64, 80), metadata_fixture.payloadUsage(&manager));
    reservation.release();
    try std.testing.expectEqual(@as(u64, 0), metadata_fixture.payloadUsage(&manager));
}

test "batch release accounting errors fail closed" {
    var manager = try metadata_fixture.init(.{});
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
    try std.testing.expectEqual(@as(u64, 15), (stats.memory.used_bytes - manager.identity_metadata_bytes));
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
    try std.testing.expectEqual(@as(u64, 15), metadata_fixture.payloadUsage(&manager));

    // Simulate a damaged aggregate ledger. Release must retain every slice
    // reservation and make the mismatch observable rather than saturating.
    manager.memory.used_bytes = manager.identity_metadata_bytes + 14;
    second.release();
    stats = manager.snapshot();
    try std.testing.expectEqual(@as(u64, 14), (stats.memory.used_bytes - manager.identity_metadata_bytes));
    try std.testing.expectEqual(@as(u64, 20), manager.sliceStats(.inference_model_residency).used_bytes);
    try std.testing.expectEqual(@as(u64, 4), stats.memory.accounting_errors);
}

test "resource identity ledgers bound tombstones across churn and reject stale owners" {
    var manager = try metadata_fixture.init(.{ .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    var pinned = try manager.reserve(.dense_apply_working_set, 7);
    defer pinned.release();
    var stale: ?Reservation = null;
    var stale_batch: ?BatchReservation = null;
    for (0..8192) |i| {
        var reservation = try manager.reserve(.dense_apply_working_set, 3);
        var batch = try manager.reserveBatch(&.{.{ .slice = .dense_apply_working_set, .bytes = 5 }});
        if (i == 0) {
            stale = reservation;
            stale_batch = batch;
        }
        const key = i + 1;
        try std.testing.expect(manager.tryObserveUsageIdentity(.dense_apply_working_set, key, 0, 11));
        try std.testing.expect(manager.tryObserveUsageIdentity(.dense_apply_working_set, key, 11, 0));
        reservation.release();
        batch.release();
        try std.testing.expectEqual(@as(u64, 7), metadata_fixture.payloadUsage(&manager));
        try std.testing.expect(manager.reservation_identities.removals < @max(1, manager.reservation_identities.map.capacity() / 8));
        try std.testing.expect(manager.batch_reservation_identities.removals < @max(1, manager.batch_reservation_identities.map.capacity() / 8));
        try std.testing.expect(manager.observer_identities.removals < @max(1, manager.observer_identities.map.capacity() / 8));
    }
    stale.?.release();
    stale_batch.?.release();
    try std.testing.expectEqual(@as(u64, 7), metadata_fixture.payloadUsage(&manager));
    try std.testing.expectEqual(@as(u64, 2), manager.snapshot().memory.accounting_errors);
    try std.testing.expect(manager.reservation_identities.rehashes > 0);
    try std.testing.expect(manager.batch_reservation_identities.rehashes > 0);
    try std.testing.expect(manager.observer_identities.rehashes > 0);
}

test "single release and observer mismatch cannot debit unrelated memory" {
    var manager = try metadata_fixture.init(.{});
    var first = try manager.reserve(.inference_tokenizer_cache, 10);
    var unrelated = try manager.reserve(.inference_model_residency, 20);

    var corrupt_copy = first;
    corrupt_copy.bytes = 11;
    corrupt_copy.release();
    var stats = manager.snapshot();
    try std.testing.expectEqual(@as(u64, 30), (stats.memory.used_bytes - manager.identity_metadata_bytes));
    try std.testing.expectEqual(@as(u64, 10), manager.sliceStats(.inference_tokenizer_cache).used_bytes);

    var stale_observation: u64 = 11;
    try std.testing.expect(!manager.tryObserveUsage(
        .inference_tokenizer_cache,
        &stale_observation,
        0,
    ));
    stats = manager.snapshot();
    try std.testing.expectEqual(@as(u64, 30), (stats.memory.used_bytes - manager.identity_metadata_bytes));
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
    try std.testing.expectEqual(@as(u64, 0), metadata_fixture.payloadUsage(&manager));
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

test "dense search bandwidth admission is FIFO and work weighted" {
    var manager = ResourceManager.init(.{ .dense_search_bandwidth_capacity_bytes = 10 });
    defer manager.deinit(std.testing.allocator);
    var first = try manager.acquireDenseSearchBandwidth(7, null, null);

    const State = struct {
        manager: *ResourceManager,
        bytes: u64,
        order_counter: *std.atomic.Value(u32),
        order: std.atomic.Value(u32) = .init(0),
        release: std.atomic.Value(bool) = .init(false),

        fn run(self: *@This()) void {
            var lease = self.manager.acquireDenseSearchBandwidth(self.bytes, null, null) catch unreachable;
            const order = self.order_counter.fetchAdd(1, .acq_rel) + 1;
            self.order.store(order, .release);
            while (!self.release.load(.acquire)) platform_time.yieldBriefly();
            lease.release();
        }
    };
    var order_counter = std.atomic.Value(u32).init(0);
    var heavy = State{ .manager = &manager, .bytes = 8, .order_counter = &order_counter };
    const heavy_thread = try std.Thread.spawn(.{}, State.run, .{&heavy});
    while (manager.denseSearchAdmissionStats().queued_queries != 1) platform_time.yieldBriefly();
    var light = State{ .manager = &manager, .bytes = 3, .order_counter = &order_counter };
    const light_thread = try std.Thread.spawn(.{}, State.run, .{&light});
    while (manager.denseSearchAdmissionStats().queued_queries != 2) platform_time.yieldBriefly();

    // The light request would fit beside the active request, but cannot barge
    // ahead of the older heavy request.
    try std.testing.expectEqual(@as(u32, 0), light.order.load(.acquire));
    first.release();
    while (heavy.order.load(.acquire) == 0) platform_time.yieldBriefly();
    try std.testing.expectEqual(@as(u32, 1), heavy.order.load(.acquire));
    try std.testing.expectEqual(@as(u32, 0), light.order.load(.acquire));
    heavy.release.store(true, .release);
    while (light.order.load(.acquire) == 0) platform_time.yieldBriefly();
    try std.testing.expectEqual(@as(u32, 2), light.order.load(.acquire));
    light.release.store(true, .release);
    heavy_thread.join();
    light_thread.join();

    const stats = manager.denseSearchAdmissionStats();
    try std.testing.expectEqual(@as(u64, 8), stats.peak_active_bytes);
    try std.testing.expectEqual(@as(u64, 1), stats.peak_active_queries);
    try std.testing.expectEqual(@as(u64, 2), stats.waits);
}

test "dense search bandwidth admission removes cancelled waiters" {
    var manager = ResourceManager.init(.{ .dense_search_bandwidth_capacity_bytes = 1 });
    defer manager.deinit(std.testing.allocator);
    var first = try manager.acquireDenseSearchBandwidth(1, null, null);

    const State = struct {
        manager: *ResourceManager,
        cancelled: std.atomic.Value(bool) = .init(false),
        saw_cancelled: std.atomic.Value(bool) = .init(false),

        fn isCancelled(raw: *const anyopaque) bool {
            const signal: *const std.atomic.Value(bool) = @ptrCast(@alignCast(raw));
            return signal.load(.acquire);
        }

        fn run(self: *@This()) void {
            var lease = self.manager.acquireDenseSearchBandwidth(1, .{
                .ptr = &self.cancelled,
                .is_cancelled = isCancelled,
            }, std.Io.Threaded.global_single_threaded.io()) catch |err| {
                std.debug.assert(err == error.Cancelled);
                self.saw_cancelled.store(true, .release);
                return;
            };
            lease.release();
        }
    };
    var state = State{ .manager = &manager };
    const thread = try std.Thread.spawn(.{}, State.run, .{&state});
    while (manager.denseSearchAdmissionStats().queued_queries != 1) platform_time.yieldBriefly();
    state.cancelled.store(true, .release);
    thread.join();
    try std.testing.expect(state.saw_cancelled.load(.acquire));
    const stats = manager.denseSearchAdmissionStats();
    try std.testing.expectEqual(@as(u64, 0), stats.queued_queries);
    try std.testing.expectEqual(@as(u64, 1), stats.cancellations);
    first.release();
}

test "dense search bandwidth cancellation racing a grant returns its full charge" {
    var manager = ResourceManager.init(.{ .dense_search_bandwidth_capacity_bytes = 10 });
    defer manager.deinit(std.testing.allocator);
    var blocker = try manager.acquireDenseSearchBandwidth(10, null, null);
    defer blocker.release();
    const Cancel = struct {
        blocker: *DenseSearchAdmissionLease,
        calls: usize = 0,
        fn check(ptr: *const anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(@constCast(ptr)));
            self.calls += 1;
            if (self.calls == 1) return false;
            self.blocker.release(); // synchronous grant between observing cancellation and removing
            return true;
        }
    };
    var cancel = Cancel{ .blocker = &blocker };
    try std.testing.expectError(error.Cancelled, manager.acquireDenseSearchBandwidth(7, .{ .ptr = &cancel, .is_cancelled = Cancel.check }, null));
    try std.testing.expectEqual(@as(usize, 2), cancel.calls);
    const stats = manager.denseSearchAdmissionStats();
    try std.testing.expectEqual(@as(u64, 0), stats.active_bytes);
    try std.testing.expectEqual(@as(u64, 0), stats.active_queries);
    try std.testing.expectEqual(@as(u64, 0), stats.queued_queries);
    try std.testing.expectEqual(@as(u64, 1), stats.cancellations);
    var next = try manager.acquireDenseSearchBandwidth(10, null, null);
    next.release();
    next.release();
    try std.testing.expectEqual(@as(u64, 0), manager.denseSearchAdmissionStats().active_bytes);
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

test "capacity percentage safety floor is capped on large volumes" {
    var manager = ResourceManager.init(.{
        .disk_safety_floor_bytes = 10,
        .disk_safety_floor_divisor = 20,
        .disk_safety_floor_max_bytes = 30,
    });
    defer manager.deinit(std.testing.allocator);
    const observation = CapacityObservation{
        .available_bytes = 100,
        .capacity_bytes = 1000,
    };

    // Five percent would reserve 50 bytes. The independent 30-byte emergency
    // cap leaves room for 70 bytes of explicitly accounted maintenance.
    var admitted = try manager.reserveCapacity(std.testing.allocator, 7, 70, observation, 0);
    defer admitted.release();
    try std.testing.expectError(
        error.CapacityUnavailable,
        manager.reserveCapacity(std.testing.allocator, 7, 1, observation, 0),
    );
    const domains = try manager.capacityDomainStats(std.testing.allocator);
    defer std.testing.allocator.free(domains);
    try std.testing.expectEqual(@as(usize, 1), domains.len);
    try std.testing.expectEqual(@as(u64, 30), domains[0].last_safety_floor_bytes);
}

test "resource manager owns capacity domains independently of consumer allocator" {
    var manager = ResourceManager.init(.{
        .identity_allocator = std.testing.allocator,
        .disk_safety_floor_bytes = 0,
        .disk_safety_floor_divisor = 0,
    });
    defer manager.deinit(std.testing.allocator);

    var reservation = try manager.reserveCapacity(std.heap.page_allocator, 7, 1, .{}, 0);
    reservation.release();
    try std.testing.expectEqual(@as(usize, 1), manager.capacityStats().domain_count);
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

test "bounded observer growth grants aggregate slice and host capacity atomically" {
    var budgets = Options.defaultBudgets();
    budgets[sliceIndex(.document_extraction_working_set)] = .{ .hard_limit_bytes = 100 };
    var manager = try metadata_fixture.init(.{
        .budgets = budgets,
        .memory_budget = .{ .hard_limit_bytes = 80 },
    });
    var requester: u64 = 0;
    var competing_slice_owner: u64 = 0;
    var other_slice_owner: u64 = 0;
    defer manager.observeUsage(.document_extraction_working_set, &requester, 0);
    defer manager.observeUsage(.document_extraction_working_set, &competing_slice_owner, 0);
    defer manager.observeUsage(.full_text_build_working_set, &other_slice_owner, 0);

    try manager.adjustUsage(.document_extraction_working_set, &requester, 10);
    try manager.adjustUsage(.document_extraction_working_set, &competing_slice_owner, 30);
    try manager.adjustUsage(.full_text_build_working_set, &other_slice_owner, 20);
    try std.testing.expectEqual(
        @as(u64, 20),
        try manager.adjustUsageAtMost(.document_extraction_working_set, &requester, 100),
    );
    try std.testing.expectEqual(@as(u64, 30), requester);
    try std.testing.expectEqual(@as(u64, 60), manager.sliceStats(.document_extraction_working_set).used_bytes);
    try std.testing.expectEqual(@as(u64, 80), metadata_fixture.payloadUsage(&manager));
    try std.testing.expectError(
        error.ResourceBudgetExceeded,
        manager.adjustUsageAtMost(.document_extraction_working_set, &requester, 1),
    );
}

test "owned split reservations prevent concurrent headroom theft" {
    var budgets = Options.defaultBudgets();
    budgets[sliceIndex(.document_extraction_working_set)] = .{ .hard_limit_bytes = 100 };
    var manager = try metadata_fixture.init(.{
        .budgets = budgets,
        .memory_budget = .{ .hard_limit_bytes = 90 },
    });

    var reservation = try manager.reserveOwnedSplitAtMost(.document_extraction_working_set, 100, 30);
    defer reservation.release();
    try std.testing.expectEqual(@as(u64, 60), reservation.primary_bytes);
    try std.testing.expectEqual(@as(u64, 30), reservation.secondary_bytes);
    try std.testing.expectEqual(@as(u64, 90), reservation.reservation.reservedBytes());
    try std.testing.expectEqual(@as(u64, 90), manager.sliceStats(.document_extraction_working_set).used_bytes);
    try std.testing.expectEqual(@as(u64, 90), metadata_fixture.payloadUsage(&manager));
    try std.testing.expectError(error.ResourceBudgetExceeded, manager.reserve(.document_extraction_working_set, 1));
    if (!builtin.single_threaded and builtin.os.tag != .freestanding) {
        const Concurrent = struct {
            manager: *ResourceManager,
            acquired: std.atomic.Value(bool) = .init(false),

            fn run(self: *@This()) void {
                var competing = self.manager.reserve(.document_extraction_working_set, 1) catch return;
                self.acquired.store(true, .release);
                competing.release();
            }
        };
        var concurrent = Concurrent{ .manager = &manager };
        const thread = try std.Thread.spawn(.{}, Concurrent.run, .{&concurrent});
        thread.join();
        try std.testing.expect(!concurrent.acquired.load(.acquire));
    }
}

test "nonreclaiming partial reservations atomically respect slice and host headroom" {
    const Probe = struct {
        calls: usize = 0,
        fn reclaim(raw: *anyopaque, _: u64) u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return 0;
        }
    };
    for ([_]u64{ 90, 200 }) |host_limit| {
        var budgets = Options.defaultBudgets();
        budgets[sliceIndex(.document_extraction_working_set)] = .{ .hard_limit_bytes = 100 };
        var manager = try metadata_fixture.init(.{ .budgets = budgets, .memory_budget = .{ .hard_limit_bytes = host_limit } });
        defer manager.deinit(std.testing.allocator);
        var probe = Probe{};
        const id = try manager.registerReclaimer(.document_extraction_working_set, &probe, Probe.reclaim);
        defer manager.unregisterReclaimer(id);
        var held = try manager.reserveWithoutReclaim(.document_extraction_working_set, 40);
        defer held.release();
        var other = try manager.reserveWithoutReclaim(.hbc_node_metadata_cache, 10);
        defer other.release();
        var growth = try manager.reserveAtMostWithoutReclaim(.document_extraction_working_set, 100);
        try std.testing.expectEqual(@as(u64, if (host_limit == 90) 40 else 60), growth.bytes);
        try std.testing.expectError(error.ResourceBudgetExceeded, manager.reserveAtMostWithoutReclaim(.document_extraction_working_set, 1));
        try std.testing.expectEqual(@as(usize, 0), probe.calls);
        growth.release();
        try std.testing.expectEqual(@as(u64, 40), manager.sliceStats(.document_extraction_working_set).used_bytes);
        var zero = try manager.reserveAtMostWithoutReclaim(.document_extraction_working_set, 0);
        zero.release();
    }
}

test "owned split reservations reclaim before partial fallback for slice pressure" {
    const Context = struct {
        manager: *ResourceManager,
        slice: Slice,
        accounted: u64 = 0,
        pinned: bool = false,
        calls: usize = 0,

        fn reclaim(raw: *anyopaque, target: u64) u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (self.pinned) return 0;
            const released = @min(target, self.accounted);
            self.manager.observeUsage(self.slice, &self.accounted, self.accounted - released);
            return released;
        }
    };
    var budgets = Options.defaultBudgets();
    budgets[sliceIndex(.document_extraction_working_set)] = .{ .hard_limit_bytes = 100 };
    var manager = try metadata_fixture.init(.{ .budgets = budgets, .memory_budget = .{ .hard_limit_bytes = 200 } });
    defer manager.deinit(std.testing.allocator);
    var unrelated = Context{ .manager = &manager, .slice = .hbc_node_metadata_cache };
    manager.observeUsage(unrelated.slice, &unrelated.accounted, 40);
    const unrelated_id = try manager.registerReclaimer(unrelated.slice, &unrelated, Context.reclaim);
    defer manager.unregisterReclaimer(unrelated_id);
    var document = Context{ .manager = &manager, .slice = .document_extraction_working_set, .pinned = true };
    manager.observeUsage(document.slice, &document.accounted, 60);
    const document_id = try manager.registerReclaimer(document.slice, &document, Context.reclaim);
    defer manager.unregisterReclaimer(document_id);

    try std.testing.expectError(error.ResourceBudgetExceeded, manager.reserveOwnedSplitAtMost(document.slice, 1, 70));
    try std.testing.expectEqual(@as(usize, 0), unrelated.calls);
    document.pinned = false;
    var reservation = try manager.reserveOwnedSplitAtMost(document.slice, 1, 70);
    defer reservation.release();
    try std.testing.expectEqual(@as(u64, 29), document.accounted);
    try std.testing.expectEqual(@as(u64, 40), unrelated.accounted);
    try std.testing.expectEqual(@as(usize, 0), unrelated.calls);
    try std.testing.expectEqual(@as(u64, 1), reservation.primary_bytes);
    try std.testing.expectEqual(@as(u64, 70), reservation.secondary_bytes);
}

test "owned split reservations reclaim before partial fallback" {
    const ReclaimContext = struct {
        manager: *ResourceManager,
        accounted: u64 = 0,

        fn reclaim(raw: *anyopaque, target: u64) u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const released = @min(target, self.accounted);
            self.manager.observeUsage(.hbc_node_metadata_cache, &self.accounted, self.accounted - released);
            return released;
        }
    };

    var budgets = Options.defaultBudgets();
    budgets[sliceIndex(.document_extraction_working_set)] = .{ .hard_limit_bytes = 200 };
    var manager = try metadata_fixture.init(.{
        .budgets = budgets,
        .memory_budget = .{ .hard_limit_bytes = 100 },
    });
    defer manager.deinit(std.testing.allocator);
    var reclaimable = ReclaimContext{ .manager = &manager };
    manager.observeUsage(.hbc_node_metadata_cache, &reclaimable.accounted, 40);
    const reclaimer_id = try manager.registerReclaimer(.hbc_node_metadata_cache, &reclaimable, ReclaimContext.reclaim);
    defer manager.unregisterReclaimer(reclaimer_id);

    var reservation = try manager.reserveOwnedSplitAtMost(.document_extraction_working_set, 70, 30);
    defer reservation.release();
    try std.testing.expectEqual(@as(u64, 70), reservation.primary_bytes);
    try std.testing.expectEqual(@as(u64, 30), reservation.secondary_bytes);
    try std.testing.expectEqual(@as(u64, 0), reclaimable.accounted);
    try std.testing.expectEqual(@as(u64, 100), metadata_fixture.payloadUsage(&manager));
}

test "owned split secondary credit transfers into a budgeted allocator" {
    var budgets = Options.defaultBudgets();
    budgets[sliceIndex(.document_extraction_working_set)] = .{ .hard_limit_bytes = 100 };
    var manager = ResourceManager.init(.{ .budgets = budgets });
    defer manager.deinit(std.testing.allocator);

    var split = try manager.reserveOwnedSplitAtMost(.document_extraction_working_set, 70, 30);
    defer split.release();
    var retained = BudgetedAllocator.init(
        &manager,
        .document_extraction_working_set,
        std.testing.allocator,
        1,
    );
    defer retained.deinit();

    var aliased_source = split.reservation;
    try std.testing.expectError(
        error.ResourceAccountingMismatch,
        split.reservation.transferCreditTo(&aliased_source, 1),
    );
    try split.transferSecondaryTo(&retained);
    try std.testing.expectEqual(@as(u64, 0), split.secondary_bytes);
    try std.testing.expectEqual(@as(u64, 70), split.reservation.bytes);
    try std.testing.expectEqual(@as(u64, 30), retained.reservation.bytes);
    try std.testing.expectEqual(@as(u64, 100), manager.sliceStats(.document_extraction_working_set).used_bytes);

    const retained_alloc = retained.allocator();
    const value = try retained_alloc.alloc(u8, 30);
    split.release();
    try std.testing.expectEqual(@as(u64, 30), manager.sliceStats(.document_extraction_working_set).used_bytes);
    retained_alloc.free(value);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.document_extraction_working_set).used_bytes);
}

test "pinned split credit survives idle allocation windows" {
    var budgets = Options.defaultBudgets();
    budgets[sliceIndex(.document_extraction_working_set)] = .{ .hard_limit_bytes = 100 };
    var manager = ResourceManager.init(.{ .budgets = budgets });
    defer manager.deinit(std.testing.allocator);

    var split = try manager.reserveOwnedSplitAtMost(.document_extraction_working_set, 70, 30);
    defer split.release();
    var invocation = BudgetedAllocator.init(
        &manager,
        .document_extraction_working_set,
        std.testing.allocator,
        1,
    );
    try split.transferSecondaryToPinned(&invocation);
    const invocation_alloc = invocation.allocator();
    const first_window = try invocation_alloc.alloc(u8, 30);
    invocation_alloc.free(first_window);
    try std.testing.expectEqual(@as(u64, 100), manager.sliceStats(.document_extraction_working_set).used_bytes);
    const second_window = try invocation_alloc.alloc(u8, 30);
    invocation_alloc.free(second_window);
    split.release();
    try std.testing.expectEqual(@as(u64, 30), manager.sliceStats(.document_extraction_working_set).used_bytes);
    invocation.deinit();
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.document_extraction_working_set).used_bytes);
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

test "foreground traffic defers optional maintenance through bounded quiet windows" {
    var manager = ResourceManager.init(.{});
    defer manager.deinit(std.testing.allocator);

    try std.testing.expect(!manager.shouldDeferOptionalMaintenanceForForegroundTraffic());
    manager.beginForegroundQuery();
    manager.beginForegroundQuery();
    try std.testing.expect(manager.shouldDeferOptionalMaintenanceForForegroundTraffic());
    manager.finishForegroundQuery();
    try std.testing.expect(manager.shouldDeferOptionalMaintenanceForForegroundTraffic());
    manager.finishForegroundQuery();
    try std.testing.expect(manager.shouldDeferOptionalMaintenanceForForegroundTraffic());
    manager.foreground_query_quiet_until_ns.store(0, .release);
    try std.testing.expect(!manager.shouldDeferOptionalMaintenanceForForegroundTraffic());

    var write = manager.beginForegroundWrite();
    try std.testing.expect(manager.shouldDeferOptionalMaintenanceForForegroundTraffic());
    write.release();
    try std.testing.expect(manager.shouldDeferOptionalMaintenanceForForegroundTraffic());
    manager.foreground_write_quiet_until_ns.store(0, .release);
    try std.testing.expect(!manager.shouldDeferOptionalMaintenanceForForegroundTraffic());
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
    const before = manager.hbc_cache_benefit[@backingInt(HbcCacheClass.vector)].score;
    try std.testing.expect(before > 0);

    manager.observeHbcCacheBenefitSampled(.{
        .{},
        .{},
        .{ .hits = 16, .resident_bytes = 4096 },
        .{},
    });
    const after = manager.hbc_cache_benefit[@backingInt(HbcCacheClass.vector)].score;
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
    var manager = try metadata_fixture.init(.{
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
    try std.testing.expectEqual(@as(u64, 100), (stats.memory.used_bytes - manager.identity_metadata_bytes));
    try std.testing.expectEqual(@as(u64, 1), stats.reclaim_requests);
    try std.testing.expectEqual(@as(u64, 10), stats.reclaimed_bytes);
}

test "non-reclaiming reservation never invokes callbacks" {
    const ReclaimContext = struct {
        invoked: bool = false,

        fn reclaim(raw: *anyopaque, _: u64) u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.invoked = true;
            return 0;
        }
    };

    var budgets = Options.defaultBudgets();
    budgets[sliceIndex(.hbc_node_metadata_cache)] = .{ .hard_limit_bytes = 100 };
    budgets[sliceIndex(.shard_transition_working_set)] = .{ .hard_limit_bytes = 100 };
    var manager = ResourceManager.init(.{
        .memory_budget = .{ .hard_limit_bytes = 100 },
        .budgets = budgets,
    });
    defer manager.deinit(std.testing.allocator);

    var context = ReclaimContext{};
    var cache_bytes: u64 = 0;
    manager.observeUsage(.hbc_node_metadata_cache, &cache_bytes, 80);
    const identity = try manager.registerReclaimer(
        .hbc_node_metadata_cache,
        &context,
        ReclaimContext.reclaim,
    );
    defer manager.unregisterReclaimer(identity);

    try std.testing.expectError(
        error.ResourceBudgetExceeded,
        manager.reserveWithoutReclaim(.shard_transition_working_set, 30),
    );
    try std.testing.expect(!context.invoked);
}

test "dense search admission reclaims retained scratch from its own slice" {
    const ReclaimContext = struct {
        manager: *ResourceManager,
        accounted: u64,

        fn reclaim(raw: *anyopaque, target: u64) u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const released = @min(target, self.accounted);
            self.manager.observeUsage(.dense_search_working_set, &self.accounted, self.accounted - released);
            return released;
        }
    };

    var budgets = Options.defaultBudgets();
    budgets[sliceIndex(.dense_search_working_set)] = .{ .hard_limit_bytes = 100 };
    var manager = ResourceManager.init(.{ .budgets = budgets });
    defer manager.deinit(std.testing.allocator);
    var retained = ReclaimContext{ .manager = &manager, .accounted = 0 };
    manager.observeUsage(.dense_search_working_set, &retained.accounted, 80);
    const identity = try manager.registerReclaimer(.dense_search_working_set, &retained, ReclaimContext.reclaim);
    defer manager.unregisterReclaimer(identity);

    var active: u64 = 0;
    try manager.adjustUsage(.dense_search_working_set, &active, 30);
    try std.testing.expectEqual(@as(u64, 70), retained.accounted);
    try std.testing.expectEqual(@as(u64, 30), active);
    try std.testing.expectEqual(@as(u64, 100), manager.sliceStats(.dense_search_working_set).used_bytes);
}

test "source vector payloads admission redistributes idle scratch shares" {
    const Owner = struct {
        manager: *ResourceManager,
        bytes: u64 = 0,
        calls: usize = 0,

        fn reclaim(raw: *anyopaque, target: u64) u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            const released = @min(target, self.bytes);
            self.manager.observeUsage(.dense_search_working_set, &self.bytes, self.bytes - released);
            return released;
        }
    };
    var budgets = Options.defaultBudgets();
    budgets[sliceIndex(.dense_search_working_set)] = .{ .hard_limit_bytes = 100 };
    var manager = ResourceManager.init(.{ .budgets = budgets });
    defer manager.deinit(std.testing.allocator);
    var retained = Owner{ .manager = &manager };
    var empty = Owner{ .manager = &manager };
    // Observation can report grown query workspaces above their slice limit.
    // The first owner can release all of it; the second owns no idle memory.
    manager.observeUsage(.dense_search_working_set, &retained.bytes, 120);
    defer manager.observeUsage(.dense_search_working_set, &retained.bytes, 0);
    const first = try manager.registerReclaimer(.dense_search_working_set, &retained, Owner.reclaim);
    defer manager.unregisterReclaimer(first);
    const second = try manager.registerReclaimer(.dense_search_working_set, &empty, Owner.reclaim);
    defer manager.unregisterReclaimer(second);
    var active: u64 = 0;
    defer manager.observeUsage(.dense_search_working_set, &active, 0);
    try manager.adjustUsage(.dense_search_working_set, &active, 20);
    try std.testing.expectEqual(@as(u64, 80), retained.bytes);
    try std.testing.expectEqual(@as(u64, 20), active);
    try std.testing.expectEqual(@as(u64, 100), manager.sliceStats(.dense_search_working_set).used_bytes);
    try std.testing.expectEqual(@as(usize, 2), retained.calls);
}

test "HBC admission reclaims retained metadata from its own slice" {
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
    budgets[sliceIndex(.hbc_node_metadata_cache)] = .{ .hard_limit_bytes = 100 };
    var manager = ResourceManager.init(.{ .budgets = budgets });
    defer manager.deinit(std.testing.allocator);
    var retained = ReclaimContext{ .manager = &manager, .accounted = 0 };
    manager.observeUsage(.hbc_node_metadata_cache, &retained.accounted, 80);
    const identity = try manager.registerReclaimer(.hbc_node_metadata_cache, &retained, ReclaimContext.reclaim);
    defer manager.unregisterReclaimer(identity);

    var active: u64 = 0;
    try manager.adjustUsage(.hbc_node_metadata_cache, &active, 30);
    try std.testing.expectEqual(@as(u64, 70), retained.accounted);
    try std.testing.expectEqual(@as(u64, 30), active);
    try std.testing.expectEqual(@as(u64, 100), manager.sliceStats(.hbc_node_metadata_cache).used_bytes);
}

test "resource manager atomically transfers retained observer ownership" {
    var budgets = Options.defaultBudgets();
    budgets[sliceIndex(.hbc_node_metadata_cache)] = .{ .hard_limit_bytes = 80 };
    var manager = ResourceManager.init(.{ .budgets = budgets });
    defer manager.deinit(std.testing.allocator);

    var reservation: u64 = 0;
    var directory: u64 = 0;
    try manager.adjustUsage(.hbc_node_metadata_cache, &reservation, 80);
    try manager.transferUsage(
        .hbc_node_metadata_cache,
        &reservation,
        0,
        &directory,
        60,
    );

    const stats = manager.sliceStats(.hbc_node_metadata_cache);
    try std.testing.expectEqual(@as(u64, 0), reservation);
    try std.testing.expectEqual(@as(u64, 60), directory);
    try std.testing.expectEqual(@as(u64, 60), stats.used_bytes);
    try std.testing.expectEqual(@as(u64, 80), stats.peak_bytes);
    try std.testing.expectEqual(@as(u64, 0), stats.hard_limit_rejections);
    manager.observeUsage(.hbc_node_metadata_cache, &directory, 0);
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

    var manager = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 300 } });
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
        accounted: u64 = 0,
        retire_identity: u64 = 0,
        calls: u64 = 0,

        fn reclaim(raw: *anyopaque, target: u64) u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            self.manager.unregisterReclaimer(self.retire_identity);
            const released = @min(target, self.accounted);
            self.manager.observeUsage(.hbc_node_metadata_cache, &self.accounted, self.accounted - released);
            return released;
        }
    };

    var manager = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 100 } });
    defer manager.deinit(std.testing.allocator);
    var retiring = RetiringContext{ .manager = &manager };
    var passive = PassiveContext{};
    const retiring_id = try manager.registerReclaimer(.hbc_node_metadata_cache, &retiring, RetiringContext.reclaim);
    defer manager.unregisterReclaimer(retiring_id);
    retiring.retire_identity = try manager.registerReclaimer(.hbc_node_metadata_cache, &passive, PassiveContext.reclaim);
    defer manager.unregisterReclaimer(retiring.retire_identity);

    manager.observeUsage(.hbc_node_metadata_cache, &retiring.accounted, 100);
    // The second bounded pass can revisit the surviving owner, but never
    // invokes the registry slot retired by its first callback.
    try std.testing.expectEqual(@as(u64, 10), manager.reclaimForAllocation(.dense_apply_working_set, 10));
    try std.testing.expectEqual(@as(u64, 2), retiring.calls);
    try std.testing.expectEqual(@as(u64, 0), passive.calls);
}

test "resource manager grows reclaimer registry beyond 128 owners" {
    const owner_count = 257;
    const PassiveContext = struct {
        calls: u64 = 0,

        fn reclaim(raw: *anyopaque, _: u64) u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return 0;
        }
    };

    var manager = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 100 } });
    defer manager.deinit(std.testing.allocator);
    var contexts = @as([owner_count]PassiveContext, @splat(.{}));
    var identities: [owner_count]u64 = undefined;
    for (&contexts, 0..) |*context, index| {
        identities[index] = try manager.registerReclaimer(
            .hbc_node_metadata_cache,
            context,
            PassiveContext.reclaim,
        );
    }
    defer for (identities) |identity| manager.unregisterReclaimer(identity);

    var accounted: u64 = 0;
    manager.observeUsage(.hbc_node_metadata_cache, &accounted, 100);
    try std.testing.expectEqual(@as(u64, 0), manager.reclaimForAllocation(.dense_apply_working_set, 1));
    var calls: u64 = 0;
    for (contexts) |context| calls += context.calls;
    try std.testing.expectEqual(@as(u64, owner_count), calls);
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

    var manager = try metadata_fixture.init(.{
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
    try std.testing.expectEqual(@as(u64, 100), metadata_fixture.payloadUsage(&manager));
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
    var manager = try metadata_fixture.init(.{
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
    try std.testing.expectEqual(@as(u64, 0), metadata_fixture.payloadUsage(&manager));
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

test "controlled pressure admission resource manager preserves original authority across parent and child contention" {
    var parent = ResourceManager.init(.{});
    var child = ResourceManager.init(.{});
    try child.installMemoryParent(parent.memoryParentPort());
    const Authority = struct {
        child: *ResourceManager,
        target: *std.atomic.Mutex,
        target_is_parent: bool,
        held: *bool,
        checks: usize = 0,
        waits: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        cause: anyerror = error.Canceled,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks == self.fail_at) return self.cause;
        }
        fn wait(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.waits += 1;
            if (self.target_is_parent) {
                // Parent contention must never retain the child's ledger lock.
                try std.testing.expect(self.child.mutex.tryLock());
                self.child.mutex.unlock();
            }
            try std.testing.expect(self.held.*);
            self.target.unlock();
            self.held.* = false;
        }
    };
    for ([_]bool{ true, false }) |target_is_parent| {
        const target = if (target_is_parent) &parent.mutex else &child.mutex;
        try std.testing.expect(target.tryLock());
        var held = true;
        defer if (held) target.unlock();
        var authority: Authority = .{ .child = &child, .target = target, .target_is_parent = target_is_parent, .held = &held };
        const payload: read_admission.Payload = .{ .ptr = &authority, .vtable = &.{ .checkpoint = Authority.check, .wait_for_progress = Authority.wait } };
        const control = read_admission.Port.init(&payload);
        var decision = try child.admissionDecisionWithAdmission(.lsm_in_memory_state, 1, control);
        try std.testing.expectEqual(@as(usize, 1), authority.waits);
        try std.testing.expect(!held);
        // Epoch is deliberately unused for controlled polling. Pressure and
        // thresholds otherwise match the legacy decision exactly.
        decision.change_epoch = child.pressureEpoch();
        try std.testing.expectEqualDeep(child.admissionDecision(.lsm_in_memory_state, 1), decision);
        const checks = authority.checks;
        for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed }) |cause| {
            for (1..checks + 1) |fail_at| {
                try std.testing.expect(target.tryLock());
                held = true;
                authority = .{ .child = &child, .target = target, .target_is_parent = target_is_parent, .held = &held, .fail_at = fail_at, .cause = cause };
                try std.testing.expectError(cause, child.admissionDecisionWithAdmission(.lsm_in_memory_state, 1, control));
                if (held) {
                    // Rejection before progress cannot unlock another owner.
                    try std.testing.expect(!target.tryLock());
                    target.unlock();
                    held = false;
                } else {
                    // Rejection racing acquisition must retire our own lock.
                    try std.testing.expect(target.tryLock());
                    target.unlock();
                }
            }
        }
        authority = .{ .child = &child, .target = target, .target_is_parent = target_is_parent, .held = &held };
        try child.awaitAdmissionWithAdmission(.lsm_in_memory_state, 1, control);
        try std.testing.expectEqual(@as(usize, 0), authority.waits);
    }
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
    try std.testing.expectEqual(@as(u64, 0), budgeted.denialGeneration());
    try std.testing.expectError(error.OutOfMemory, alloc.alloc(u8, 9));
    try std.testing.expect(budgeted.denied());
    try std.testing.expectEqual(@as(u64, 1), budgeted.denialGeneration());
    try std.testing.expectError(error.OutOfMemory, alloc.alloc(u8, 9));
    try std.testing.expectEqual(@as(u64, 2), budgeted.denialGeneration());
    try std.testing.expectEqual(@as(u64, 24), manager.sliceStats(.shard_transition_working_set).used_bytes);

    alloc.free(oversized);
    alloc.free(first);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.shard_transition_working_set).used_bytes);
    const recovered = try alloc.alloc(u8, 8);
    defer alloc.free(recovered);
    try std.testing.expect(budgeted.denied());
    try std.testing.expectEqual(@as(u64, 2), budgeted.denialGeneration());
}

test "budgeted allocator reclamation is opt in and retries without raising limits" {
    const Cache = struct {
        retained: Reservation,
        calls: usize = 0,

        fn reclaim(raw: *anyopaque, target: u64) u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            const bytes = @min(target, self.retained.bytes);
            self.retained.shrink(bytes);
            return bytes;
        }
    };
    var budgets = Options.defaultBudgets();
    budgets[sliceIndex(.document_extraction_working_set)] = .{ .hard_limit_bytes = 64 };
    var manager = ResourceManager.init(.{ .budgets = budgets });
    defer manager.deinit(std.testing.allocator);
    var cache = Cache{ .retained = try manager.reserve(.document_extraction_working_set, 48) };
    defer cache.retained.release();
    const identity = try manager.registerReclaimer(.document_extraction_working_set, &cache, Cache.reclaim);
    defer manager.unregisterReclaimer(identity);

    var ordinary = BudgetedAllocator.init(&manager, .document_extraction_working_set, std.testing.allocator, 1);
    defer ordinary.deinit();
    try std.testing.expectError(error.OutOfMemory, ordinary.allocator().alloc(u8, 32));
    try std.testing.expectEqual(@as(usize, 0), cache.calls);
    try std.testing.expectEqual(@as(u64, 48), cache.retained.bytes);

    var required = BudgetedAllocator.initReclaiming(&manager, .document_extraction_working_set, std.testing.allocator, 1);
    defer required.deinit();
    const bytes = try required.allocator().alloc(u8, 32);
    defer required.allocator().free(bytes);
    try std.testing.expectEqual(@as(usize, 1), cache.calls);
    try std.testing.expectEqual(@as(u64, 32), cache.retained.bytes);
    try std.testing.expectEqual(@as(u64, 64), manager.sliceStats(.document_extraction_working_set).used_bytes);
    try std.testing.expectEqual(@as(u64, 0), required.denialGeneration());

    // Even evicting all remaining optional bytes cannot admit this request.
    // The allocator retries once, retains its original live allocation, and
    // never increases the configured hard limit or loops over reclaimers.
    try std.testing.expectError(error.OutOfMemory, required.allocator().alloc(u8, 33));
    try std.testing.expectEqual(@as(usize, 2), cache.calls);
    try std.testing.expectEqual(@as(u64, 0), cache.retained.bytes);
    try std.testing.expectEqual(@as(u64, 32), required.live_bytes);
    try std.testing.expectEqual(@as(u64, 32), manager.sliceStats(.document_extraction_working_set).used_bytes);
    try std.testing.expectEqual(@as(u64, 1), required.denialGeneration());
}

test "budgeted allocator reclaim denial does not retry a busy cache" {
    const Cache = struct {
        calls: usize = 0,

        fn reclaim(raw: *anyopaque, _: u64) u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return 0;
        }
    };
    var budgets = Options.defaultBudgets();
    budgets[sliceIndex(.document_extraction_working_set)] = .{ .hard_limit_bytes = 64 };
    var manager = ResourceManager.init(.{ .budgets = budgets });
    defer manager.deinit(std.testing.allocator);
    var retained = try manager.reserve(.document_extraction_working_set, 48);
    defer retained.release();
    var cache: Cache = .{};
    const identity = try manager.registerReclaimer(.document_extraction_working_set, &cache, Cache.reclaim);
    defer manager.unregisterReclaimer(identity);
    var required = BudgetedAllocator.initReclaiming(&manager, .document_extraction_working_set, std.testing.allocator, 1);
    defer required.deinit();
    try std.testing.expectError(error.OutOfMemory, required.allocator().alloc(u8, 32));
    try std.testing.expectEqual(@as(usize, 1), cache.calls);
    try std.testing.expectEqual(@as(u64, 1), required.denialGeneration());
    try std.testing.expectEqual(@as(u64, 0), required.live_bytes);
    try std.testing.expectEqual(@as(u64, 48), retained.bytes);
}

test "budgeted allocator reclaims aggregate cache before denying growth" {
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
    budgets[sliceIndex(.dense_vector_block_build_working_set)] = .{ .soft_limit_bytes = 80, .hard_limit_bytes = 100 };
    var manager = try metadata_fixture.init(.{
        .memory_budget = .{ .soft_limit_bytes = 90, .hard_limit_bytes = 100 },
        .budgets = budgets,
    });
    defer manager.deinit(std.testing.allocator);

    var context = ReclaimContext{ .manager = &manager, .accounted = 0 };
    manager.observeUsage(.hbc_node_metadata_cache, &context.accounted, 80);
    const identity = try manager.registerReclaimer(.hbc_node_metadata_cache, &context, ReclaimContext.reclaim);
    defer manager.unregisterReclaimer(identity);

    var budgeted = BudgetedAllocator.initReclaiming(
        &manager,
        .dense_vector_block_build_working_set,
        std.testing.allocator,
        1,
    );
    defer budgeted.deinit();
    const bytes = try budgeted.allocator().alloc(u8, 30);
    defer budgeted.allocator().free(bytes);

    try std.testing.expectEqual(@as(u64, 70), context.accounted);
    try std.testing.expectEqual(@as(u64, 30), budgeted.live_bytes);
    const stats = manager.snapshot();
    try std.testing.expectEqual(@as(u64, 100), (stats.memory.used_bytes - manager.identity_metadata_bytes));
    try std.testing.expectEqual(@as(u64, 1), stats.reclaim_requests);
    try std.testing.expectEqual(@as(u64, 10), stats.reclaimed_bytes);
}

test "budgeted allocator retains both scratch and pinned credit floors" {
    var manager = ResourceManager.init(.{});
    defer manager.deinit(std.testing.allocator);
    var pinned = try manager.reserve(.dense_source_payload_state, 32);
    defer pinned.release();
    var budget = BudgetedAllocator.init(&manager, .dense_source_payload_state, std.testing.allocator, 1);
    defer budget.deinit();
    try budget.adoptPinnedReservationCredit(&pinned, 32);
    const bytes = try budget.allocator().alloc(u8, 8);
    const scratch = try budget.reserveScratch(64);
    _ = budget.releaseUnusedCredit();
    try std.testing.expectEqual(@as(u64, 72), budget.reservation.bytes);
    budget.allocator().free(bytes);
    try std.testing.expectEqual(@as(u64, 72), budget.reservation.bytes);
    scratch.release();
    try std.testing.expectEqual(@as(u64, 32), budget.reservation.bytes);
    try std.testing.expectEqual(@as(u64, 0), budget.releaseUnusedCredit());
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
    try std.testing.expectEqual(reserved - second.len, budgeted.releaseUnusedCredit());
    try std.testing.expectEqual(@as(u64, second.len), manager.sliceStats(.shard_transition_working_set).used_bytes);
    try std.testing.expectEqual(@as(u64, 0), budgeted.releaseUnusedCredit());
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

const test_owner_a: ReplayOwnerId = 0x1001;
const test_owner_b: ReplayOwnerId = 0x2002;

test "resource manager escalates replay-document-not-visible only after bounded retries on one window" {
    var manager = ResourceManager.init(.{});

    var attempt: u64 = 0;
    while (attempt < replay_document_not_visible_default_max_retries) : (attempt += 1) {
        try std.testing.expect(!manager.shouldEscalateReplayDocumentNotVisible(test_owner_a, "full_text_index_v0", 7));
    }
    // One more consecutive failure on the SAME (owner, index, sequence)
    // window finally escalates -- this is the bounded backstop for issue
    // #938's whole class of bug: a document that will never gain visible
    // content must not wedge the replay worker (and anything waiting for
    // it to drain, like Lite's run_until_idle()) forever.
    try std.testing.expect(manager.shouldEscalateReplayDocumentNotVisible(test_owner_a, "full_text_index_v0", 7));

    // A different sequence is a different window: it starts back at zero.
    try std.testing.expect(!manager.shouldEscalateReplayDocumentNotVisible(test_owner_a, "full_text_index_v0", 8));

    // A different index on the same owner is tracked independently too.
    try std.testing.expect(!manager.shouldEscalateReplayDocumentNotVisible(test_owner_a, "other_index", 7));
}

test "resource manager clears replay-document-not-visible escalation once a window succeeds" {
    var manager = ResourceManager.init(.{});

    var attempt: u64 = 0;
    while (attempt <= replay_document_not_visible_default_max_retries) : (attempt += 1) {
        _ = manager.shouldEscalateReplayDocumentNotVisible(test_owner_a, "full_text_index_v0", 7);
    }
    manager.clearReplayDocumentNotVisibleEscalation(test_owner_a, "full_text_index_v0", 7);

    // A later stall that happens to land on the same window starts counting
    // from zero again instead of escalating immediately.
    try std.testing.expect(!manager.shouldEscalateReplayDocumentNotVisible(test_owner_a, "full_text_index_v0", 7));
}

test "resource manager tracks replay-document-not-visible escalation independently per index" {
    var manager = ResourceManager.init(.{});

    // Drive two different indexes' windows (same owner) in lockstep.
    // Per-index tracking means interleaving one index's failures must not
    // reset or otherwise perturb the other's count.
    var attempt: u64 = 0;
    while (attempt < replay_document_not_visible_default_max_retries) : (attempt += 1) {
        try std.testing.expect(!manager.shouldEscalateReplayDocumentNotVisible(test_owner_a, "index_a", 1));
        try std.testing.expect(!manager.shouldEscalateReplayDocumentNotVisible(test_owner_a, "index_b", 1));
    }
    // Both failed exactly as many times, so both cross their threshold on
    // the same next attempt -- neither is ahead of or behind the other.
    try std.testing.expect(manager.shouldEscalateReplayDocumentNotVisible(test_owner_a, "index_a", 1));
    try std.testing.expect(manager.shouldEscalateReplayDocumentNotVisible(test_owner_a, "index_b", 1));
}

// PR #957 review on the #938 escalation commit: two storage owners
// (table/shard replicas) commonly have same-named indexes -- the default
// `full_text_index_v0` is the common case -- and this ResourceManager is
// shared process-wide across every owner. Keying the tracker by index name
// alone let two such owners alternately reset each other's count (neither
// ever escalated) or, when their sequences happened to coincide, combine
// into one count. Both must now be impossible.

test "resource manager escalates two owners with the same index name independently when they alternate failures" {
    var manager = ResourceManager.init(.{});

    // Alternate failures between two owners that both have an index named
    // "full_text_index_v0", at DIFFERENT sequences, mirroring the reviewer's
    // tracker-level repro (ten alternating failures per owner at a limit of
    // two). Before the fix this reset each owner's count on every turn, so
    // neither ever escalated no matter how many rounds ran.
    var round: u64 = 0;
    while (round < replay_document_not_visible_default_max_retries) : (round += 1) {
        try std.testing.expect(!manager.shouldEscalateReplayDocumentNotVisible(test_owner_a, "full_text_index_v0", 100));
        try std.testing.expect(!manager.shouldEscalateReplayDocumentNotVisible(test_owner_b, "full_text_index_v0", 200));
    }
    try std.testing.expect(manager.shouldEscalateReplayDocumentNotVisible(test_owner_a, "full_text_index_v0", 100));
    try std.testing.expect(manager.shouldEscalateReplayDocumentNotVisible(test_owner_b, "full_text_index_v0", 200));
}

test "resource manager does not combine two owners' attempts when their sequences coincide" {
    var manager = ResourceManager.init(.{});

    // Two owners, same index name, same sequence number by coincidence
    // (sequence counters are per owner, not global). Owner A alone drives
    // all the way to its threshold; owner B must not have inherited any of
    // owner A's progress just because the (index, sequence) pair matched.
    var attempt: u64 = 0;
    while (attempt < replay_document_not_visible_default_max_retries) : (attempt += 1) {
        try std.testing.expect(!manager.shouldEscalateReplayDocumentNotVisible(test_owner_a, "full_text_index_v0", 42));
    }
    try std.testing.expect(manager.shouldEscalateReplayDocumentNotVisible(test_owner_a, "full_text_index_v0", 42));

    // Owner B's first-ever attempt at the identical (index, sequence) pair
    // starts from zero, not from owner A's already-escalated count.
    try std.testing.expect(!manager.shouldEscalateReplayDocumentNotVisible(test_owner_b, "full_text_index_v0", 42));
}

test "resource manager records and snapshots replay-document-not-visible skips per owner and index" {
    var manager = ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);

    manager.recordReplayDocumentNotVisibleSkipped(test_owner_a, "full_text_index_v0");
    manager.recordReplayDocumentNotVisibleSkipped(test_owner_a, "full_text_index_v0");
    manager.recordReplayDocumentNotVisibleSkipped(test_owner_a, "other_index");
    // owner_b has an index of the SAME name as owner_a's; its count must
    // stay distinct per-owner but fold into the same /metrics label.
    manager.recordReplayDocumentNotVisibleSkipped(test_owner_b, "full_text_index_v0");

    // A skipped document is not silently lost: it is counted per owner and
    // index...
    try std.testing.expectEqual(@as(u64, 2), manager.replayDocumentNotVisibleSkippedTotal(test_owner_a, "full_text_index_v0"));
    try std.testing.expectEqual(@as(u64, 1), manager.replayDocumentNotVisibleSkippedTotal(test_owner_a, "other_index"));
    try std.testing.expectEqual(@as(u64, 0), manager.replayDocumentNotVisibleSkippedTotal(test_owner_a, "never_skipped"));
    try std.testing.expectEqual(@as(u64, 1), manager.replayDocumentNotVisibleSkippedTotal(test_owner_b, "full_text_index_v0"));
    // ...and summed for the single process-wide /metrics counter.
    try std.testing.expectEqual(@as(u64, 4), manager.replayDocumentNotVisibleSkippedTotalAll());

    const snapshot = try manager.snapshotReplayDocumentNotVisibleSkipped(std.testing.allocator);
    defer {
        for (snapshot) |entry| std.testing.allocator.free(entry.index_name);
        std.testing.allocator.free(snapshot);
    }
    // owner_a's and owner_b's "full_text_index_v0" counts fold into one
    // labeled /metrics sample (owner identity is not a meaningful label).
    try std.testing.expectEqual(@as(usize, 2), snapshot.len);
    var saw_full_text = false;
    var saw_other = false;
    for (snapshot) |entry| {
        if (std.mem.eql(u8, entry.index_name, "full_text_index_v0")) {
            try std.testing.expectEqual(@as(u64, 3), entry.count);
            saw_full_text = true;
        } else if (std.mem.eql(u8, entry.index_name, "other_index")) {
            try std.testing.expectEqual(@as(u64, 1), entry.count);
            saw_other = true;
        }
    }
    try std.testing.expect(saw_full_text);
    try std.testing.expect(saw_other);
}

test "resource manager forgets an owner's replay-document-not-visible state when it is removed" {
    var manager = ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);

    manager.recordReplayDocumentNotVisibleSkipped(test_owner_a, "full_text_index_v0");
    _ = manager.shouldEscalateReplayDocumentNotVisible(test_owner_a, "full_text_index_v0", 7);
    try std.testing.expectEqual(@as(u64, 1), manager.replayDocumentNotVisibleSkippedTotal(test_owner_a, "full_text_index_v0"));

    // Closing the owner (its IndexManager/DB) must drop every window and
    // skip count it ever accumulated -- otherwise this process-wide tracker
    // grows without bound across repeated DB/table open-close churn.
    manager.removeReplayNotVisibleOwner(test_owner_a);
    try std.testing.expectEqual(@as(u64, 0), manager.replayDocumentNotVisibleSkippedTotal(test_owner_a, "full_text_index_v0"));
    try std.testing.expectEqual(@as(u64, 0), manager.replayDocumentNotVisibleSkippedTotalAll());

    // A later owner that happens to reuse the same ReplayOwnerId (e.g. an
    // allocator reusing a freed IndexManager's address) starts completely
    // fresh, never inheriting the removed owner's retry count.
    var attempt: u64 = 0;
    while (attempt < replay_document_not_visible_default_max_retries) : (attempt += 1) {
        try std.testing.expect(!manager.shouldEscalateReplayDocumentNotVisible(test_owner_a, "full_text_index_v0", 7));
    }
    try std.testing.expect(manager.shouldEscalateReplayDocumentNotVisible(test_owner_a, "full_text_index_v0", 7));
}

test "resource manager replay skip snapshots clean up every allocation failure" {
    const Case = struct {
        fn validate(snapshot: []const IndexReplayDocumentNotVisibleSkipped, index_count: usize) !void {
            try std.testing.expectEqual(index_count, snapshot.len);
            var seen = @as([17]bool, @splat(false));
            for (snapshot) |entry| {
                try std.testing.expect(std.mem.startsWith(u8, entry.index_name, "index_"));
                const index = try std.fmt.parseUnsigned(usize, entry.index_name[6..], 10);
                try std.testing.expect(index < index_count);
                try std.testing.expect(!seen[index]);
                seen[index] = true;
                try std.testing.expectEqual(@as(u64, 3), entry.count);
            }
        }

        fn run(alloc: std.mem.Allocator, index_count: usize) !void {
            // Populate the tracker independently of the failing snapshot
            // allocator, so every injected failure exercises snapshot cleanup.
            var manager = ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
            defer manager.deinit(std.testing.allocator);
            for (0..index_count) |i| {
                var buf: [32]u8 = undefined;
                const name = try std.fmt.bufPrint(&buf, "index_{d}", .{i});
                manager.recordReplayDocumentNotVisibleSkipped(test_owner_a, name);
                manager.recordReplayDocumentNotVisibleSkipped(test_owner_a, name);
                manager.recordReplayDocumentNotVisibleSkipped(test_owner_b, name);
            }
            // A tracked retry without a skip must not appear in the snapshot.
            _ = manager.shouldEscalateReplayDocumentNotVisible(test_owner_a, "unskipped", 7);

            const snapshot = manager.snapshotReplayDocumentNotVisibleSkipped(alloc) catch |err| {
                // Failure cannot consume tracker state or prevent a later
                // metrics scrape from recovering the same aggregate counts.
                try std.testing.expectEqual(@as(u64, @intCast(index_count * 3)), manager.replayDocumentNotVisibleSkippedTotalAll());
                const recovered = try manager.snapshotReplayDocumentNotVisibleSkipped(std.testing.allocator);
                defer {
                    for (recovered) |entry| std.testing.allocator.free(entry.index_name);
                    std.testing.allocator.free(recovered);
                }
                try validate(recovered, index_count);
                return err;
            };
            defer {
                for (snapshot) |entry| alloc.free(entry.index_name);
                alloc.free(snapshot);
            }
            try validate(snapshot, index_count);
            try std.testing.expectEqual(@as(u64, @intCast(index_count * 3)), manager.replayDocumentNotVisibleSkippedTotalAll());
        }
    };
    // Cover empty output, the original single-entry leak, map growth, and
    // cleanup after any prefix of successfully copied names.
    for ([_]usize{ 0, 1, 17 }) |index_count| {
        try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{index_count});
    }
}

test "source vector payloads scoped allocator receipts distinguish admission and backing across shared failures" {
    const alloc = std.testing.allocator;
    var budgets = Options.defaultBudgets();
    budgets[@backingInt(Slice.dense_source_payload_state)] = .{ .hard_limit_bytes = 64 };
    var manager = ResourceManager.init(.{ .budgets = budgets });
    defer manager.deinit(alloc);
    var budget = BudgetedAllocator.init(&manager, .dense_source_payload_state, alloc, 1);
    defer budget.deinit();
    var first: BudgetedAllocator.FailureTrackingAllocator = .{ .owner = &budget };
    var second: BudgetedAllocator.FailureTrackingAllocator = .{ .owner = &budget };
    var held = try manager.reserve(.dense_source_payload_state, 64);
    defer held.release();
    try std.testing.expectError(error.OutOfMemory, first.allocator().alloc(u8, 1));
    try std.testing.expect(first.last_failure == .admission);
    held.release();
    var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = 0 });
    budget.backing = failing.allocator();
    try std.testing.expectError(error.OutOfMemory, second.allocator().alloc(u8, 1));
    try std.testing.expect(second.last_failure == .backing);
    try std.testing.expect(first.last_failure == .admission);
    budget.backing = alloc;
    const memory = try first.allocator().alloc(u8, 1);
    // Allocations and frees are interchangeable with the original allocator.
    budget.threadSafeAllocator().free(memory);
    try std.testing.expect(first.last_failure == null);
}

test "resource manager external allocator shares storage ceiling and preserves pinned control" {
    var manager = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 100 }, .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    // Pin control headroom before publishing foreground work. Its allocations
    // consume this reservation, so they must not charge it again.
    try std.testing.expect(manager.reserveExternal(10));
    try std.testing.expect(manager.reserveExternal(30));
    var storage = try manager.reserveWithoutReclaim(.lsm_compaction_work, 60);
    try std.testing.expect(!manager.reserveExternal(1));
    try std.testing.expectError(error.ResourceBudgetExceeded, manager.reserveWithoutReclaim(.dense_search_working_set, 1));
    try std.testing.expectEqual(@as(u64, 100), metadata_fixture.payloadUsage(&manager));
    try std.testing.expectEqual(@as(u64, 40), manager.snapshot().memory.external_bytes);
    manager.releaseExternal(30);
    storage.release();
    try std.testing.expectEqual(@as(u64, 10), metadata_fixture.payloadUsage(&manager));
    manager.releaseExternal(10);
    try std.testing.expectEqual(@as(u64, 0), metadata_fixture.payloadUsage(&manager));
}

test "resource manager parent reclaims sibling cache outside child locks and preserves local ceilings" {
    var root = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 100 }, .identity_allocator = std.testing.allocator });
    defer root.deinit(std.testing.allocator);
    var budgets = Options.defaultBudgets();
    budgets[sliceIndex(.inference_tokenizer_cache)] = .{ .hard_limit_bytes = 5 };
    var child = try metadata_fixture.init(.{ .budgets = budgets, .identity_allocator = std.testing.allocator });
    defer child.deinit(std.testing.allocator);
    try metadata_fixture.installParent(&child, &root);
    const Cache = struct {
        root: *ResourceManager,
        child: *ResourceManager,
        bytes: u64 = 0,
        calls: usize = 0,

        fn reclaim(ptr: *anyopaque, requested: u64) u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            // This takes the child accounting lock: callback invocation must
            // happen after the child releases it, even at aggregate pressure.
            _ = self.child.snapshot();
            self.calls += 1;
            const released = @min(requested, self.bytes);
            self.root.observeUsage(.lsm_block_table_cache, &self.bytes, self.bytes - released);
            return released;
        }
    };
    var cache: Cache = .{ .root = &root, .child = &child };
    root.observeUsage(.lsm_block_table_cache, &cache.bytes, 100);
    defer root.observeUsage(.lsm_block_table_cache, &cache.bytes, 0);
    const registration = try root.registerReclaimer(.lsm_block_table_cache, &cache, Cache.reclaim);
    defer root.unregisterReclaimer(registration);
    try std.testing.expectError(error.ResourceBudgetExceeded, child.reserveWithoutReclaim(.inference_model_residency, 10));
    try std.testing.expectError(error.ResourceBudgetExceeded, child.reserve(.inference_tokenizer_cache, 10));
    try std.testing.expectEqual(@as(usize, 0), cache.calls);
    var model = try child.reserve(.inference_model_residency, 10);
    defer model.release();
    try std.testing.expectEqual(@as(usize, 1), cache.calls);
    try std.testing.expectEqual(@as(u64, 90), cache.bytes);
    try std.testing.expectEqual(@as(u64, 100), (metadata_fixture.payloadUsage(&root) - child.identity_metadata_bytes));
    try std.testing.expectEqual(@as(u64, 10), metadata_fixture.payloadUsage(&child));
}

test "resource manager parent atomically coordinates storage inference and API memory" {
    var root = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 100 }, .identity_allocator = std.testing.allocator });
    defer root.deinit(std.testing.allocator);
    var inference = try metadata_fixture.init(.{ .identity_allocator = std.testing.allocator });
    defer inference.deinit(std.testing.allocator);
    var storage = try metadata_fixture.init(.{ .identity_allocator = std.testing.allocator });
    defer storage.deinit(std.testing.allocator);
    try metadata_fixture.installParent(&inference, &root);
    try metadata_fixture.installParent(&storage, &root);
    var model = try inference.reserveWithoutReclaim(.inference_model_residency, 40);
    defer model.release();
    try std.testing.expect(root.reserveExternal(10));
    defer root.releaseExternal(10);
    var work = try storage.reserveWithoutReclaim(.lsm_wal_write_working_set, 50);
    try std.testing.expectEqual(@as(u64, 100), (metadata_fixture.payloadUsage(&root) - inference.identity_metadata_bytes - storage.identity_metadata_bytes));
    try std.testing.expectEqual(@as(u64, 100), (root.snapshot().memory.external_bytes - inference.identity_metadata_bytes - storage.identity_metadata_bytes));
    try std.testing.expectError(error.ResourceBudgetExceeded, model.growBoundedOversized(1, 2));
    try std.testing.expectEqual(@as(u64, 40), model.bytes);
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, storage.reserveBatchClassified(&.{.{ .slice = .dense_search_working_set, .bytes = 1 }}));
    try std.testing.expectEqual(@as(u32, 0), storage.batch_reservation_identities.count());
    try std.testing.expectEqual(@as(u64, 0), storage.availableAdmissionBytes(.dense_search_working_set));
    try std.testing.expectEqual(Pressure.hard, storage.admissionDecision(.dense_search_working_set, 1).pressure);
    work.shrink(20);
    try std.testing.expectEqual(@as(u64, 80), (metadata_fixture.payloadUsage(&root) - inference.identity_metadata_bytes - storage.identity_metadata_bytes));
    var split = try storage.reserveOwnedSplitAtMost(.document_extraction_working_set, 80, 5);
    try std.testing.expectEqual(@as(u64, 15), split.primary_bytes);
    split.release();
    work.release();
    try std.testing.expectEqual(@as(u64, 50), (metadata_fixture.payloadUsage(&root) - inference.identity_metadata_bytes - storage.identity_metadata_bytes));
}

test "resource manager parent retains over-budget observations and returns terminal balance" {
    var root = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 100 }, .identity_allocator = std.testing.allocator });
    defer root.deinit(std.testing.allocator);
    var child = try metadata_fixture.init(.{ .identity_allocator = std.testing.allocator });
    var observed: u64 = 0;
    child.observeUsage(.inference_model_residency, &observed, 20);
    try metadata_fixture.installParent(&child, &root);
    try std.testing.expectEqual(@as(u64, 20), (metadata_fixture.payloadUsage(&root) - child.identity_metadata_bytes));
    try std.testing.expect(root.reserveExternal(80));
    try std.testing.expectError(error.ResourceBudgetExceeded, child.adjustUsage(.inference_model_residency, &observed, 21));
    try std.testing.expectEqual(@as(u64, 20), observed);
    child.observeUsage(.inference_model_residency, &observed, 30);
    try std.testing.expectEqual(@as(u64, 110), (metadata_fixture.payloadUsage(&root) - child.identity_metadata_bytes));
    child.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u64, 80), (metadata_fixture.payloadUsage(&root) - child.identity_metadata_bytes));
    root.releaseExternal(80);
}

test "resource manager parent wakes child waiters when another owner releases memory" {
    if (!supports_pressure_wait) return error.SkipZigTest;
    var root = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 100 }, .identity_allocator = std.testing.allocator });
    defer root.deinit(std.testing.allocator);
    var child = try metadata_fixture.init(.{ .identity_allocator = std.testing.allocator });
    defer child.deinit(std.testing.allocator);
    try metadata_fixture.installParent(&child, &root);
    try std.testing.expect(root.reserveExternal(100));
    const epoch = child.admissionDecision(.dense_search_working_set, 1).change_epoch;
    const Waiter = struct {
        fn run(manager: *ResourceManager, observed: u64) void {
            manager.waitForPressureChange(observed);
        }
    };
    const thread = try std.Thread.spawn(.{}, Waiter.run, .{ &child, epoch });
    root.releaseExternal(100);
    thread.join();
    var work = try child.reserveWithoutReclaim(.dense_search_working_set, 1);
    work.release();
}

test "resource manager reclassifies owned read credit without a second host charge" {
    const a = std.testing.allocator;
    for ([_]u64{ 128, 256 }) |limit| {
        var budgets = Options.defaultBudgets();
        budgets[@backingInt(Slice.lsm_block_table_cache)] = .{ .hard_limit_bytes = limit };
        var manager = try metadata_fixture.init(.{ .budgets = budgets, .memory_budget = .{ .hard_limit_bytes = 150 } });
        defer manager.deinit(a);
        var credit = try manager.reserveWithoutReclaim(.lsm_read_working_set, 150);
        defer credit.release();
        if (limit < 150) {
            try std.testing.expectError(error.ResourceBudgetExceeded, credit.reclassify(.lsm_block_table_cache));
            try std.testing.expectEqual(Slice.lsm_read_working_set, credit.slice);
            try std.testing.expectEqual(@as(u64, 150), manager.sliceStats(.lsm_read_working_set).used_bytes);
            try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_block_table_cache).used_bytes);
        } else {
            try credit.reclassify(.lsm_block_table_cache);
            try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_read_working_set).used_bytes);
            try std.testing.expectEqual(@as(u64, 150), manager.sliceStats(.lsm_block_table_cache).used_bytes);
        }
        try std.testing.expectEqual(@as(u64, 150), (manager.snapshot().memory.peak_bytes - manager.identity_metadata_bytes));
        credit.release();
        try std.testing.expectEqual(@as(u64, 0), metadata_fixture.payloadUsage(&manager));
        try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.accounting_errors);
    }
}

test "resource manager adopts construction credit without a second host charge" {
    const a = std.testing.allocator;
    for ([_]Slice{ .lsm_read_working_set, .lsm_block_table_cache }) |source| {
        var manager = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 140 } });
        defer manager.deinit(a);
        var credit = try manager.reserveWithoutReclaim(source, 128);
        defer credit.release();
        var retained: u64 = 0;
        try manager.adoptReservationUsage(&credit, .lsm_block_table_cache, &retained, 136);
        try std.testing.expectEqual(@as(u64, 0), credit.bytes);
        try std.testing.expectEqual(@as(u64, 136), retained);
        try std.testing.expectEqual(@as(u64, 136), (manager.snapshot().memory.peak_bytes - manager.identity_metadata_bytes));
        credit.release();
        try std.testing.expectEqual(@as(u64, 136), metadata_fixture.payloadUsage(&manager));
        try manager.adjustUsage(.lsm_block_table_cache, &retained, 0);
        try std.testing.expectEqual(@as(u64, 0), metadata_fixture.payloadUsage(&manager));
        try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.accounting_errors);
    }
}

pub fn qualifyParentConstructionCredit() !void {
    if (!builtin.is_test) @compileError("qualification requires a test build");
    const a = std.testing.allocator;
    for ([_]Slice{ .lsm_read_working_set, .lsm_block_table_cache }) |source| {
        for ([_]u64{ 128, 136, 140 }) |parent_limit| {
            var parent = try metadata_fixture.init(.{ .identity_allocator = a, .memory_budget = .{ .hard_limit_bytes = parent_limit } });
            defer parent.deinit(a);
            var child = try metadata_fixture.init(.{ .identity_allocator = a });
            defer child.deinit(a);
            try metadata_fixture.installParent(&child, &parent);
            var credit = try child.reserveWithoutReclaim(source, 128);
            defer credit.release();
            var retained: u64 = 0;
            defer child.observeUsage(.lsm_block_table_cache, &retained, 0);
            if (parent_limit == 128) {
                try std.testing.expectError(error.ResourceBudgetExceeded, child.adoptReservationUsage(&credit, .lsm_block_table_cache, &retained, 136));
                try std.testing.expectEqual(@as(u64, 128), credit.bytes);
                try std.testing.expectEqual(@as(u64, 0), retained);
                try std.testing.expectEqual(@as(u64, 128), metadata_fixture.payloadUsage(&child));
                try std.testing.expectEqual(@as(usize, 0), child.observer_identities.count());
                // An exact-credit handoff needs no new parent capacity.
                try child.adoptReservationUsage(&credit, .lsm_block_table_cache, &retained, 128);
            } else {
                try child.adoptReservationUsage(&credit, .lsm_block_table_cache, &retained, 136);
            }
            try std.testing.expectEqual(retained, (metadata_fixture.payloadUsage(&parent) - child.identity_metadata_bytes));
            try std.testing.expectEqual(retained, (parent.snapshot().memory.peak_bytes - parent.identity_metadata_bytes - child.identity_metadata_bytes));
            try std.testing.expectEqual(retained, metadata_fixture.payloadUsage(&child));
            credit.release();
            try std.testing.expectEqual(retained, (metadata_fixture.payloadUsage(&parent) - child.identity_metadata_bytes));
            try child.adjustUsage(.lsm_block_table_cache, &retained, 0);
            try std.testing.expectEqual(@as(u64, 0), (metadata_fixture.payloadUsage(&parent) - child.identity_metadata_bytes));
            try std.testing.expectEqual(@as(u64, 0), metadata_fixture.payloadUsage(&child));
            try std.testing.expectEqual(@as(u64, 0), parent.snapshot().memory.accounting_errors);
            try std.testing.expectEqual(@as(u64, 0), child.snapshot().memory.accounting_errors);
        }
    }
}

pub fn qualifyNoLiveReservations(manager: *const ResourceManager) !void {
    if (!builtin.is_test) @compileError("qualification requires a test build");
    try std.testing.expectEqual(@as(u32, 0), manager.reservation_identities.count());
}

pub fn qualifyControlledCreditNotifications(control: read_admission.Port) !void {
    if (!builtin.is_test) @compileError("qualification requires a test build");
    const a = std.testing.allocator;
    var root = ResourceManager.init(.{ .identity_allocator = a });
    defer root.deinit(a);
    var middle = ResourceManager.init(.{ .identity_allocator = a });
    defer middle.deinit(a);
    var child = ResourceManager.init(.{ .identity_allocator = a });
    defer child.deinit(a);
    try middle.installMemoryParent(root.memoryParentPort());
    try child.installMemoryParent(middle.memoryParentPort());
    const Legacy = struct {
        fn notify(_: *anyopaque) callconv(.c) void {
            @panic("controlled credit invoked a legacy parent notification");
        }
    };
    const middle_notify = middle.memory_parent.?.notify_fn;
    const child_notify = child.memory_parent.?.notify_fn;
    defer {
        if (middle.memory_parent) |*parent| parent.notify_fn = middle_notify;
        if (child.memory_parent) |*parent| parent.notify_fn = child_notify;
    }
    if (middle.memory_parent) |*parent| parent.notify_fn = Legacy.notify;
    if (child.memory_parent) |*parent| parent.notify_fn = Legacy.notify;
    const before = root.pressure_change.snapshot();
    var credit = try child.reserveWithoutReclaimWithAdmission(.lsm_read_working_set, 128, control);
    defer credit.release();
    var retained: u64 = 0;
    try child.adoptReservationUsageWithAdmission(&credit, .lsm_block_table_cache, &retained, 136, control);
    try metadata_fixture.expectUsage(@as(u64, 136), &root, &.{ &root, &middle, &child });
    try std.testing.expect(root.pressure_change.snapshot() != before);
    try std.testing.expectEqual(ResourceManager.MemoryParent.ReleaseResult.released, child.tryReleaseReservation(&credit));
    try child.adjustUsageWithAdmission(.lsm_block_table_cache, &retained, 0, control);
    try metadata_fixture.expectUsage(@as(u64, 0), &root, &.{ &root, &middle, &child });
    try metadata_fixture.expectUsage(@as(u64, 0), &middle, &.{ &middle, &child });
    try metadata_fixture.expectUsage(@as(u64, 0), &child, &.{&child});
    try qualifyNoLiveReservations(&child);
}

test "resource manager construction credit preserves hierarchical parent capacity" {
    try qualifyParentConstructionCredit();
}

test "resource manager adopts construction credit only within both destination limits" {
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |aggregate| {
        var budgets = Options.defaultBudgets();
        budgets[@backingInt(Slice.lsm_block_table_cache)] = .{ .hard_limit_bytes = if (aggregate) 0 else 128 };
        var manager = try metadata_fixture.init(.{ .budgets = budgets, .memory_budget = .{ .hard_limit_bytes = if (aggregate) 128 else 140 } });
        defer manager.deinit(a);
        var credit = try manager.reserveWithoutReclaim(.lsm_read_working_set, 128);
        defer credit.release();
        var retained: u64 = 0;
        try std.testing.expectError(error.ResourceBudgetExceeded, manager.adoptReservationUsage(&credit, .lsm_block_table_cache, &retained, 136));
        try std.testing.expectEqual(@as(u64, 128), credit.bytes);
        try std.testing.expectEqual(@as(u64, 0), retained);
        try std.testing.expectEqual(@as(u64, 128), metadata_fixture.payloadUsage(&manager));
        try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_block_table_cache).used_bytes);
        credit.release();
        try std.testing.expectEqual(@as(u64, 0), metadata_fixture.payloadUsage(&manager));
        try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.accounting_errors);
    }
}

test "resource manager adopts construction credit preserves ownership on identity OOM" {
    const a = std.testing.allocator;
    var failing = std.testing.FailingAllocator.init(a, .{});
    var manager = ResourceManager.init(.{ .identity_allocator = failing.allocator() });
    defer manager.deinit(a);
    var credit = try manager.reserveWithoutReclaim(.lsm_read_working_set, 128);
    defer credit.release();
    failing.fail_index = failing.alloc_index;
    var retained: u64 = 0;
    try std.testing.expectError(error.OutOfMemory, manager.adoptReservationUsage(&credit, .lsm_block_table_cache, &retained, 136));
    try std.testing.expectEqual(@as(u64, 128), credit.bytes);
    try std.testing.expectEqual(@as(u64, 0), retained);
    try std.testing.expectEqual(@as(u64, 128), metadata_fixture.payloadUsage(&manager));
    failing.fail_index = std.math.maxInt(usize);
    try manager.adoptReservationUsage(&credit, .lsm_block_table_cache, &retained, 136);
    credit.release();
    try manager.adjustUsage(.lsm_block_table_cache, &retained, 0);
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.accounting_errors);
}

pub fn qualifyConstructionRetirement(control: read_admission.Port, parent_busy: bool) !void {
    if (!builtin.is_test) @compileError("qualification requires a test build");
    const a = std.testing.allocator;
    var parent = ResourceManager.init(.{ .identity_allocator = a });
    defer parent.deinit(a);
    var manager = ResourceManager.init(.{ .identity_allocator = a });
    defer manager.deinit(a);
    try manager.installMemoryParent(parent.memoryParentPort());
    var claims: [64]?ConstructionCredit = @splat(null);
    defer for (&claims) |*claim| if (claim.*) |*credit| credit.retire();
    for (&claims) |*claim| claim.* = try manager.reserveConstructionWithAdmission(.lsm_read_working_set, 8, control);
    try std.testing.expectError(error.ResourceBudgetExceeded, manager.reserveConstructionWithAdmission(.lsm_read_working_set, 8, control));
    const before = parent.snapshot().memory.used_bytes;
    try metadata_fixture.expectUsage(512, &parent, &.{ &parent, &manager });
    // Completion cannot enter either held ledger. Rejecting the original
    // scope still leaves a live accounting owner, never an uncharged buffer.
    const result = blk: {
        const mutex = if (parent_busy) &parent.mutex else &manager.mutex;
        std.debug.assert(mutex.tryLock());
        defer mutex.unlock();
        const checkpoint = control.checkpoint();
        for (&claims) |*claim| if (claim.*) |*credit| {
            credit.retire();
            credit.retire();
        };
        for (&manager.construction_cleanup) |*slot| try std.testing.expectEqual(ConstructionCleanup.State.ready, slot.state.load(.acquire));
        break :blk checkpoint;
    };
    try std.testing.expectEqual(before, manager.snapshot().memory.used_bytes);
    try std.testing.expectEqual(before, parent.snapshot().memory.used_bytes);
    // Independent cleanup admission does not retain or reenter the caller.
    const Cleanup = struct {
        fn checkpoint(_: *anyopaque) !void {}
        fn wait(_: *anyopaque) !void {
            return error.TestUnexpectedWait;
        }
    };
    var context: u8 = 0;
    const payload: read_admission.Payload = .{ .ptr = &context, .vtable = &.{ .checkpoint = Cleanup.checkpoint, .wait_for_progress = Cleanup.wait } };
    const cleanup = read_admission.Port.init(&payload);
    // Ordinary construction must visit and release every completed slot before
    // taking the first free slot; later debt cannot be starved by slot zero.
    var retry = try manager.reserveConstructionWithAdmission(.lsm_read_working_set, 8, cleanup);
    try metadata_fixture.expectUsage(@as(u64, 8), &manager, &.{&manager});
    try metadata_fixture.expectUsage(@as(u64, 8), &parent, &.{ &parent, &manager });
    // Reuse and successful ownership transfer free the slot without releasing
    // the payload's reservation; stale consumed tokens cannot retire it.
    var published = retry.transfer();
    retry.retire();
    try metadata_fixture.expectUsage(@as(u64, 8), &manager, &.{&manager});
    published.release();
    var final = try manager.reserveConstructionWithAdmission(.lsm_read_working_set, 8, cleanup);
    std.debug.assert(manager.mutex.tryLock());
    final.retire();
    manager.mutex.unlock();
    try metadata_fixture.expectUsage(@as(u64, 8), &manager, &.{&manager});
    try manager.drainConstructionCleanupWithAdmission(cleanup);
    try qualifyNoLiveReservations(&manager);
    try metadata_fixture.expectUsage(@as(u64, 0), &parent, &.{ &parent, &manager });
    try result;
}

pub fn qualifyPressurePayloadRetirement(control: read_admission.Port, fixture: usize) !void {
    if (!builtin.is_test) @compileError("qualification requires a test build");
    if (!supports_pressure_wait) return;
    const SharedBytes = @import("lsm_backend/shared_bytes.zig").SharedBytes;
    const a = std.testing.allocator;
    var root = ResourceManager.init(.{ .identity_allocator = a });
    defer root.deinit(a);
    var parent = ResourceManager.init(.{ .identity_allocator = a });
    defer parent.deinit(a);
    try parent.installMemoryParent(root.memoryParentPort());
    var manager = ResourceManager.init(.{ .identity_allocator = a });
    defer manager.deinit(a);
    try manager.installMemoryParent(parent.memoryParentPort());
    var credit = try manager.reserveConstructionWithAdmission(.lsm_read_working_set, @sizeOf(SharedBytes) + 8, control);
    defer credit.retire();
    var value: ?*SharedBytes = blk: {
        const bytes = try a.dupe(u8, "retained");
        errdefer a.free(bytes);
        break :blk try SharedBytes.create(a, bytes);
    };
    defer if (value) |lease| lease.release();
    try value.?.registerRetirementWithAdmission(&credit, control);
    const before = manager.snapshot().memory.used_bytes;
    const held = switch (fixture) {
        0 => &manager.pressure_change,
        1 => &parent.pressure_change,
        else => &root.pressure_change,
    };
    const epoch = held.snapshot();
    const Cleanup = struct {
        fn checkpoint(_: *anyopaque) !void {}
        fn wait(_: *anyopaque) !void {
            return error.TestUnexpectedWait;
        }
    };
    var context: u8 = 0;
    const payload: read_admission.Payload = .{ .ptr = &context, .vtable = &.{ .checkpoint = Cleanup.checkpoint, .wait_for_progress = Cleanup.wait } };
    const cleanup = read_admission.Port.init(&payload);
    const result = blk: {
        std.debug.assert(held.tryLock());
        defer held.unlock();
        const checkpoint = control.checkpoint();
        const lease = value.?;
        value = null;
        lease.release();
        // The final reference must not notify pressure or acquire its mutex.
        try std.testing.expectEqual(epoch, held.snapshot());
        try std.testing.expectEqual(@as(usize, 1), try manager.drainPayloadRetirementWithAdmission(cleanup));
        // Physical allocation is gone, but notification contention preserves
        // the exact reservation through the entire three-level parent chain.
        try std.testing.expectEqual(before, manager.snapshot().memory.used_bytes);
        try std.testing.expectEqual(before, parent.snapshot().memory.used_bytes);
        try std.testing.expectEqual(before, root.snapshot().memory.used_bytes);
        if (supports_pressure_wait) {
            try std.testing.expectEqual(ResourceManager.MemoryParent.ReserveResult.busy, manager.tryReserveExternal(8));
            try std.testing.expectEqual(epoch, held.snapshot());
        }
        break :blk checkpoint;
    };
    try manager.drainConstructionCleanupWithAdmission(cleanup);
    try metadata_fixture.expectUsage(@as(u64, 0), &manager, &.{&manager});
    try metadata_fixture.expectUsage(@as(u64, 0), &parent, &.{ &parent, &manager });
    try metadata_fixture.expectUsage(@as(u64, 0), &root, &.{ &root, &parent, &manager });
    if (supports_pressure_wait) try std.testing.expect(held.snapshot() != epoch);
    try result;
}

pub fn qualifyControlledReclaimerChunks(control: read_admission.Port, grow: bool) !void {
    if (!builtin.is_test) @compileError("qualification requires a test build");
    const a = std.testing.allocator;
    const cost = ResourceManager.qualifyReclaimerInitialBytes();
    const grown = 2 * @sizeOf(ReclaimerChunk) + (try ReclaimerPointers.bytes(128));
    const growth_peak = grown + (try ReclaimerPointers.bytes(64));
    var parent = ResourceManager.init(.{ .identity_allocator = a, .memory_budget = .{ .hard_limit_bytes = if (grow) growth_peak else cost } });
    defer parent.deinit(a);
    var manager = ResourceManager.init(.{ .identity_allocator = a });
    var manager_live = true;
    defer if (manager_live) manager.deinit(a);
    try manager.installMemoryParent(parent.memoryParentPort());
    try manager.reservation_identities.ensureUnusedCapacity(&manager, 4);
    parent.memory.budget.hard_limit_bytes += manager.identity_metadata_bytes;
    var calls: u64 = 0;
    const Callback = struct {
        fn reclaim(raw: *anyopaque, _: u64) u64 {
            const count: *u64 = @ptrCast(@alignCast(raw));
            count.* += 1;
            return 0;
        }
    };
    var ids: [65]u64 = @splat(0);
    defer if (manager_live) for (ids) |id| if (id != 0) manager.unregisterReclaimer(id);
    for (ids[0..64]) |*id| id.* = try manager.registerReclaimerWithAdmission(.lsm_block_table_cache, &calls, Callback.reclaim, control);
    const first = manager.reclaimers.items[0];
    try metadata_fixture.expectUsage(@as(u64, cost), &manager, &.{&manager});
    try metadata_fixture.expectUsage(@as(u64, cost), &parent, &.{ &parent, &manager });
    if (grow) {
        ids[64] = try manager.registerReclaimerWithAdmission(.lsm_block_table_cache, &calls, Callback.reclaim, control);
        try std.testing.expectEqual(first, manager.reclaimers.items[0]);
        try metadata_fixture.expectUsage(@as(u64, growth_peak), &manager, &.{&manager});
        try metadata_fixture.expectUsage(@as(u64, growth_peak), &parent, &.{ &parent, &manager });
        try std.testing.expectEqual(@as(usize, 1), try manager.drainPayloadRetirementWithAdmission(control));
        try metadata_fixture.expectUsage(@as(u64, grown), &parent, &.{ &parent, &manager });
    } else {
        var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
        manager.identity_allocator = failing.allocator();
        const result = manager.registerReclaimerWithAdmission(.lsm_block_table_cache, &calls, Callback.reclaim, control);
        manager.identity_allocator = a;
        try std.testing.expectError(error.ResourceBudgetExceeded, result);
        try std.testing.expect(!failing.has_induced_failure);
        try std.testing.expectEqual(@as(usize, 64), manager.reclaimers.items.len);
        try metadata_fixture.expectUsage(@as(u64, cost), &parent, &.{ &parent, &manager });
        manager.unregisterReclaimer(ids[0]);
        ids[0] = 0;
        manager.identity_allocator = failing.allocator();
        const reused = manager.registerReclaimerWithAdmission(.lsm_block_table_cache, &calls, Callback.reclaim, control);
        manager.identity_allocator = a;
        ids[64] = try reused;
        try std.testing.expect(!failing.has_induced_failure);
        try std.testing.expectEqual(first, manager.reclaimers.items[0]);
        try metadata_fixture.expectUsage(@as(u64, cost), &parent, &.{ &parent, &manager });
    }
    for (&ids) |*id| {
        if (id.* != 0) manager.unregisterReclaimer(id.*);
        id.* = 0;
    }
    // Empty slots remain physically allocated and charged for later reuse.
    try metadata_fixture.expectUsage(@as(u64, if (grow) grown else cost), &parent, &.{ &parent, &manager });
    try std.testing.expectEqual(@as(u64, 0), calls);
    manager.deinit(a);
    manager_live = false;
    try std.testing.expectEqual(@as(u64, 0), parent.snapshot().memory.used_bytes);
}

pub fn qualifyControlledReclaimerPointers(control: read_admission.Port, fixture: usize) !void {
    if (!builtin.is_test) @compileError("qualification requires a test build");
    const a = std.testing.allocator;
    var parent = ResourceManager.init(.{ .identity_allocator = a });
    defer parent.deinit(a);
    var manager = ResourceManager.init(.{ .identity_allocator = a });
    var live = true;
    defer if (live) manager.deinit(a);
    try manager.installMemoryParent(parent.memoryParentPort());
    var context: u8 = 0;
    const Callback = struct {
        fn reclaim(_: *anyopaque, _: u64) u64 {
            return 0;
        }
    };
    var ids: std.ArrayList(u64) = .empty;
    defer ids.deinit(a);
    defer if (live) for (ids.items) |id| manager.unregisterReclaimer(id);
    if (fixture == 0) {
        // Fill the exact legacy pointer capacity, then adopt it through scoped
        // growth. Existing callbacks keep their original stable cell addresses.
        try ids.append(a, try manager.registerReclaimer(.lsm_block_table_cache, &context, Callback.reclaim));
        const capacity = manager.reclaimers.capacity;
        while (ids.items.len < capacity) try ids.append(a, try manager.registerReclaimer(.lsm_block_table_cache, &context, Callback.reclaim));
        const first = manager.reclaimers.items[0];
        const expected = try ReclaimerPointers.bytes(capacity * 2);
        try ids.append(a, try manager.registerReclaimerWithAdmission(.lsm_block_table_cache, &context, Callback.reclaim, control));
        try std.testing.expectEqual(first, manager.reclaimers.items[0]);
        const chunks = @divTrunc(capacity + 63, 64) * @sizeOf(ReclaimerChunk);
        const old_owner_bytes = try ReclaimerPointers.bytes(capacity);
        try metadata_fixture.expectUsage(@as(u64, chunks + expected + old_owner_bytes), &manager, &.{&manager});
        try std.testing.expectEqual(@as(u64, chunks + expected + old_owner_bytes) + manager.identity_metadata_bytes + parent.identity_metadata_bytes, parent.snapshot().memory.peak_bytes);
        try std.testing.expectEqual(@as(usize, 1), try manager.drainPayloadRetirementWithAdmission(control));
        try metadata_fixture.expectUsage(@as(u64, chunks + expected), &parent, &.{ &parent, &manager });
    } else if (fixture == 1) {
        for (0..64) |_| try ids.append(a, try manager.registerReclaimerWithAdmission(.lsm_block_table_cache, &context, Callback.reclaim, control));
        const first = manager.reclaimers.items[0];
        try ids.append(a, try manager.registerReclaimer(.lsm_block_table_cache, &context, Callback.reclaim));
        try std.testing.expectEqual(first, manager.reclaimers.items[0]);
        const expected = 2 * @sizeOf(ReclaimerChunk) + (try ReclaimerPointers.bytes(128));
        try metadata_fixture.expectUsage(@as(u64, expected + (try ReclaimerPointers.bytes(64))), &manager, &.{&manager});
        try std.testing.expectEqual(@as(usize, 1), try manager.drainPayloadRetirementWithAdmission(control));
        try metadata_fixture.expectUsage(@as(u64, expected), &parent, &.{ &parent, &manager });
    } else {
        // Failure may occur in credit identity, chunk, pointer header or array
        // preparation. No callback identity may escape a failed preparation.
        var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = fixture - 2 });
        manager.identity_allocator = failing.allocator();
        const result = manager.registerReclaimerWithAdmission(.lsm_block_table_cache, &context, Callback.reclaim, control);
        manager.identity_allocator = a;
        if (result) |id| {
            try ids.append(a, id);
        } else |err| {
            if (err != error.OutOfMemory) return err;
            try std.testing.expectEqual(@as(usize, 0), manager.reclaimers.items.len);
            _ = try manager.drainPayloadRetirementWithAdmission(control);
            try manager.drainConstructionCleanupWithAdmission(control);
            try metadata_fixture.expectUsage(@as(u64, 0), &parent, &.{ &parent, &manager });
        }
        // Captured allocator contexts remain live through physical retirement.
        for (ids.items) |id| manager.unregisterReclaimer(id);
        ids.clearRetainingCapacity();
        manager.deinit(a);
        live = false;
        try std.testing.expectEqual(@as(u64, 0), parent.snapshot().memory.used_bytes);
        return;
    }
    for (ids.items) |id| manager.unregisterReclaimer(id);
    ids.clearRetainingCapacity();
    manager.deinit(a);
    live = false;
    try std.testing.expectEqual(@as(u64, 0), parent.snapshot().memory.used_bytes);
}

pub fn qualifyLegacyReclaimerAdoption(control: read_admission.Port, fixture: usize) !void {
    if (!builtin.is_test) @compileError("qualification requires a test build");
    const a = std.testing.allocator;
    var parent = ResourceManager.init(.{ .identity_allocator = a });
    defer parent.deinit(a);
    var manager = ResourceManager.init(.{ .identity_allocator = a });
    var live = true;
    defer if (live) manager.deinit(a);
    try manager.installMemoryParent(parent.memoryParentPort());
    var context: u8 = 0;
    const Callback = struct {
        fn reclaim(_: *anyopaque, _: u64) u64 {
            return 0;
        }
    };
    var ids: [65]u64 = @splat(0);
    defer if (live) for (ids) |id| if (id != 0) manager.unregisterReclaimer(id);
    for (&ids) |*id| id.* = try manager.registerReclaimer(.lsm_block_table_cache, &context, Callback.reclaim);
    const first = manager.reclaimers.items[0];
    const buffer = manager.reclaimers.items.ptr;
    const expected = 2 * @sizeOf(ReclaimerChunk) + (try ReclaimerPointers.bytes(manager.reclaimers.capacity));
    manager.unregisterReclaimer(ids[0]);
    ids[0] = 0;
    // Prime accounting identities so the fault probe observes registry header
    // allocation, rather than the separately retained accounting map itself.
    try manager.reservation_identities.ensureUnusedCapacity(&manager, 4);
    parent.memory.budget.hard_limit_bytes = (if (fixture == 1) @sizeOf(ReclaimerChunk) - 1 else expected) + manager.identity_metadata_bytes;
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    if (fixture != 0) manager.identity_allocator = failing.allocator();
    const result = manager.registerReclaimerWithAdmission(.lsm_block_table_cache, &context, Callback.reclaim, control);
    manager.identity_allocator = a;
    if (fixture == 0) {
        ids[0] = try result;
        try metadata_fixture.expectUsage(@as(u64, expected), &manager, &.{&manager});
        try metadata_fixture.expectUsage(@as(u64, expected), &parent, &.{ &parent, &manager });
        try std.testing.expectEqual(buffer, manager.reclaimers.items.ptr);
        try std.testing.expect(manager.reclaimer_pointers != null);
    } else {
        if (result) |_| return error.TestUnexpectedPublishedIdentity else |err| {
            const wanted = if (fixture == 1) error.ResourceBudgetExceeded else error.OutOfMemory;
            if (err != wanted) return err;
        }
        try std.testing.expectEqual(@as(u64, 0), first.identity);
        try std.testing.expectEqual(buffer, manager.reclaimers.items.ptr);
        try std.testing.expectEqual(fixture == 2, failing.has_induced_failure);
        const charged: u64 = if (fixture == 1) 0 else 2 * @sizeOf(ReclaimerChunk);
        // Partial migration keeps credit for chunks that remain physically live.
        try metadata_fixture.expectUsage(charged, &manager, &.{&manager});
        try metadata_fixture.expectUsage(charged, &parent, &.{ &parent, &manager });
        try std.testing.expect(manager.reclaimer_pointers == null);
    }
    try std.testing.expectEqual(first, manager.reclaimers.items[0]);
    for (&ids) |*id| {
        if (id.* != 0) manager.unregisterReclaimer(id.*);
        id.* = 0;
    }
    manager.deinit(a);
    live = false;
    try std.testing.expectEqual(@as(u64, 0), parent.snapshot().memory.used_bytes);
}

pub fn qualifyBoundConsumerConstruction(control: read_admission.Port, deny: bool) !void {
    if (!builtin.is_test) @compileError("qualification requires a test build");
    const a = std.testing.allocator;
    const Physical = struct {
        retirement: PayloadRetirement = .{},
        credit: ?Reservation = null,
        destroyed: *usize,
        fn destroy(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.destroyed.* += 1;
            std.testing.allocator.destroy(self);
        }
    };
    var parent = ResourceManager.init(.{ .identity_allocator = a });
    defer parent.deinit(a);
    var manager = ResourceManager.init(.{ .identity_allocator = a });
    defer manager.deinit(a);
    try manager.installMemoryParent(parent.memoryParentPort());
    var destroyed: usize = 0;
    var construction = try manager.reserveConstructionWithAdmission(.lsm_read_working_set, @sizeOf(Physical), control);
    defer construction.retire();
    const physical = try a.create(Physical);
    var published = false;
    defer if (!published) a.destroy(physical);
    physical.* = .{ .destroyed = &destroyed };
    try manager.registerPayloadRetirementWithAdmission(&physical.retirement, &physical.credit, &construction, physical, Physical.destroy, control);
    published = true;
    try manager.bindCleanupConsumer();
    defer manager.unbindCleanupConsumer();
    physical.retirement.publish();
    const before = manager.snapshot().memory.used_bytes;
    if (deny) parent.memory.budget.hard_limit_bytes = before;
    try std.testing.expectEqual(@as(usize, 0), try manager.drainForegroundPayloadRetirementWithAdmission(control));
    const result = manager.reserveConstructionWithAdmission(.lsm_read_working_set, 16, control);
    if (deny) {
        try std.testing.expectError(error.ResourceBudgetExceeded, result);
    } else {
        var extra = try result;
        extra.retire();
    }
    try std.testing.expectEqual(@as(usize, 0), destroyed);
    try std.testing.expectEqual(before, manager.snapshot().memory.used_bytes);
    try std.testing.expectEqual(before, parent.snapshot().memory.used_bytes);
    try std.testing.expect(manager.cleanupPending());
    // Only the independent consumer API can perform physical destruction.
    try std.testing.expectEqual(@as(usize, 1), try manager.drainPayloadRetirementWithAdmission(control));
    try std.testing.expectEqual(@as(usize, 1), destroyed);
    try metadata_fixture.expectUsage(@as(u64, 0), &manager, &.{&manager});
    try metadata_fixture.expectUsage(@as(u64, 0), &parent, &.{ &parent, &manager });
}

test "logical resident credit cleanup preserves host charge through ledger contention" {
    const a = std.testing.allocator;
    var manager = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 128 } });
    defer manager.deinit(a);
    var resident = try manager.reserveWithoutReclaim(.lite_docstore_snapshot_cache, 128);
    defer resident.release();
    var logical: Reservation = .{ .manager = &manager, .identity = 0, .slice = .relational_preparation_working_set, .bytes = 0, .host_charge = false };
    defer logical.release();
    _ = try manager.growReservationAmortized(&logical, 256, 256, 1, 0);
    try std.testing.expectEqual(@as(u64, 128), metadata_fixture.payloadUsage(&manager));
    lockAtomic(&manager.mutex);
    const busy = manager.tryReleaseReservation(&logical);
    manager.mutex.unlock();
    try std.testing.expectEqual(ResourceManager.MemoryParent.ReleaseResult.busy, busy);
    try std.testing.expectEqual(@as(u64, 256), logical.bytes);
    try std.testing.expect(!logical.released);
    try std.testing.expectEqual(ResourceManager.MemoryParent.ReleaseResult.released, manager.tryReleaseReservation(&logical));
    try std.testing.expectEqual(@as(u64, 128), metadata_fixture.payloadUsage(&manager));
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.relational_preparation_working_set).used_bytes);
    resident.release();
    try std.testing.expectEqual(@as(u64, 0), metadata_fixture.payloadUsage(&manager));
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.accounting_errors);
}

test "logical resident credit growth shrink and identity reject forged host ownership" {
    const a = std.testing.allocator;
    var manager = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 128 } });
    defer manager.deinit(a);
    var resident = try manager.reserveWithoutReclaim(.lite_docstore_snapshot_cache, 128);
    defer resident.release();
    var logical: Reservation = .{ .manager = &manager, .identity = 0, .slice = .relational_preparation_working_set, .bytes = 0, .host_charge = false };
    defer logical.release();
    _ = try manager.growReservationAmortized(&logical, 128, 128, 1, 0);
    try manager.growReservationBoundedOversized(&logical, 64, 1);
    logical.shrink(32);
    try std.testing.expectEqual(@as(u64, 160), logical.bytes);
    var forged = logical;
    forged.host_charge = true;
    try std.testing.expectEqual(ResourceManager.MemoryParent.ReleaseResult.mismatch, manager.tryReleaseReservation(&forged));
    try std.testing.expect(!logical.released);
    try std.testing.expectEqual(@as(u64, 128), metadata_fixture.payloadUsage(&manager));
    logical.release();
    try std.testing.expectEqual(@as(u64, 128), metadata_fixture.payloadUsage(&manager));
    resident.release();
    try std.testing.expectEqual(@as(u64, 0), metadata_fixture.payloadUsage(&manager));
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.accounting_errors);
}

test "prepared memory batch acquires without allocation and keeps failures atomic through ancestors" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var root = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 100 }, .identity_allocator = failing.allocator() });
    defer root.deinit(std.testing.allocator);
    var middle = try metadata_fixture.init(.{ .identity_allocator = failing.allocator() });
    defer middle.deinit(std.testing.allocator);
    try metadata_fixture.installParent(&middle, &root);
    var child = try metadata_fixture.init(.{ .identity_allocator = failing.allocator() });
    defer child.deinit(std.testing.allocator);
    try metadata_fixture.installParent(&child, &middle);
    var prepared = try child.prepareBatch(&.{
        .{ .slice = .lsm_read_working_set, .bytes = 40 },
        .{ .slice = .text_merge_buffers, .bytes = 20 },
    });
    defer prepared.deinit();
    var stale = prepared;
    const allocations = failing.alloc_index;
    failing.fail_index = allocations;
    try std.testing.expectEqual(@as(u64, 0), metadata_fixture.payloadUsage(&child));
    const epoch = child.pressureEpoch();
    // Exercise both local serialization points and each ancestor, including
    // its notification lock. These calls must return while locks stay held.
    const locks = [_]*std.atomic.Mutex{ &child.mutex, &middle.mutex, &root.mutex };
    for (locks) |mutex| {
        try std.testing.expect(mutex.tryLock());
        const attempt = prepared.tryAcquire();
        mutex.unlock();
        try std.testing.expect(attempt == .busy);
    }
    const changes = [_]*PressureChange{ &child.pressure_change, &middle.pressure_change, &root.pressure_change };
    for (changes) |change| {
        try std.testing.expect(change.tryLock());
        const attempt = prepared.tryAcquire();
        change.unlock();
        try std.testing.expect(attempt == .busy);
    }
    try std.testing.expectEqual(epoch, child.pressureEpoch());
    try std.testing.expectEqual(@as(u64, 0), metadata_fixture.payloadUsage(&child));
    try std.testing.expectEqual(@as(u64, 0), (metadata_fixture.payloadUsage(&middle) - child.identity_metadata_bytes));
    try std.testing.expectEqual(@as(u64, 0), (metadata_fixture.payloadUsage(&root) - middle.identity_metadata_bytes - child.identity_metadata_bytes));
    var credit = switch (prepared.tryAcquire()) {
        .acquired => |credit| credit,
        else => return error.TestExpectedEqual,
    };
    defer credit.release();
    try std.testing.expectEqual(allocations, failing.alloc_index);
    try std.testing.expectEqual(@as(u64, 60), metadata_fixture.payloadUsage(&child));
    try std.testing.expectEqual(@as(u64, 60), (metadata_fixture.payloadUsage(&middle) - child.identity_metadata_bytes));
    try std.testing.expectEqual(@as(u64, 60), (metadata_fixture.payloadUsage(&root) - middle.identity_metadata_bytes - child.identity_metadata_bytes));
    try std.testing.expect(prepared.tryAcquire() == .mismatch);
    try std.testing.expect(stale.tryAcquire() == .mismatch);
    stale.deinit(); // Cannot remove the now-active identity.
    credit.release();
    credit.release();
    try std.testing.expectEqual(@as(u64, 0), (metadata_fixture.payloadUsage(&root) - middle.identity_metadata_bytes - child.identity_metadata_bytes));
    try std.testing.expectEqual(@as(usize, 0), child.batch_reservation_identities.count());
    try std.testing.expectEqual(@as(u64, 0), child.snapshot().memory.accounting_errors);
}

test "prepared memory batch distinguishes intrinsic demand and retries without partial credit" {
    var budgets = Options.defaultBudgets();
    budgets[sliceIndex(.lsm_read_working_set)].hard_limit_bytes = 60;
    budgets[sliceIndex(.text_merge_buffers)].hard_limit_bytes = 60;
    var root = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 1000 }, .identity_allocator = std.testing.allocator });
    defer root.deinit(std.testing.allocator);
    var child = try metadata_fixture.init(.{ .budgets = budgets, .memory_budget = .{ .hard_limit_bytes = 1000 }, .identity_allocator = std.testing.allocator });
    defer child.deinit(std.testing.allocator);
    try metadata_fixture.installParent(&child, &root);
    var prepared = try child.prepareBatch(&.{
        .{ .slice = .lsm_read_working_set, .bytes = 40 },
        .{ .slice = .text_merge_buffers, .bytes = 20 },
    });
    defer prepared.deinit();
    var blocker = try child.reserveWithoutReclaim(.text_merge_buffers, 50);
    try std.testing.expect(prepared.tryAcquire() == .denied);
    try std.testing.expectEqual(@as(u64, 0), child.sliceStats(.lsm_read_working_set).used_bytes);
    try std.testing.expectEqual(@as(u64, 50), (metadata_fixture.payloadUsage(&root) - child.identity_metadata_bytes));
    // Intrinsically impossible second slices win over occupancy in the first.
    child.slices[sliceIndex(.text_merge_buffers)].budget.hard_limit_bytes = 19;
    try std.testing.expect(prepared.tryAcquire() == .too_large);
    child.slices[sliceIndex(.text_merge_buffers)].budget.hard_limit_bytes = 60;
    blocker.release();
    metadata_fixture.setPayloadLimit(&root, 100, &.{&child});
    var sibling = try root.reserveWithoutReclaim(.text_merge_buffers, 50);
    try std.testing.expect(prepared.tryAcquire() == .denied);
    try std.testing.expectEqual(@as(u64, 0), metadata_fixture.payloadUsage(&child));
    try std.testing.expectEqual(@as(u64, 50), (metadata_fixture.payloadUsage(&root) - child.identity_metadata_bytes));
    sibling.release();
    metadata_fixture.setPayloadLimit(&child, 59, &.{});
    try std.testing.expect(prepared.tryAcquire() == .too_large);
    metadata_fixture.setPayloadLimit(&child, 100, &.{});
    var credit = switch (prepared.tryAcquire()) {
        .acquired => |credit| credit,
        else => return error.TestExpectedEqual,
    };
    defer credit.release();
    try credit.retain(&.{.{ .slice = .lsm_read_working_set, .bytes = 30 }}, 30);
    try std.testing.expectEqual(@as(u64, 30), (metadata_fixture.payloadUsage(&root) - child.identity_metadata_bytes));
    credit.release();
    try std.testing.expectEqual(@as(u64, 0), (metadata_fixture.payloadUsage(&root) - child.identity_metadata_bytes));
}

test "prepared memory batch rejects unsupported parent and inactive release without consuming other credit" {
    var root = try metadata_fixture.init(.{ .identity_allocator = std.testing.allocator });
    defer root.deinit(std.testing.allocator);
    var child = try metadata_fixture.init(.{ .identity_allocator = std.testing.allocator });
    defer child.deinit(std.testing.allocator);
    var port = root.memoryParentPort();
    port.try_reserve_fn = null;
    try child.installMemoryParent(port);
    var active = try child.reserveBatch(&.{.{ .slice = .text_merge_buffers, .bytes = 20 }});
    defer active.release();
    var prepared = try child.prepareBatch(&.{.{ .slice = .text_merge_buffers, .bytes = 20 }});
    defer prepared.deinit();
    try std.testing.expect(prepared.tryAcquire() == .unsupported);
    // Same amounts are not ownership. An unused identity is never a lease.
    var forged = BatchReservation{ .manager = &child, .identity = prepared.identity, .amounts = prepared.amounts, .host_charge_bytes = prepared.host_charge_bytes };
    try std.testing.expectError(error.ResourceAccountingMismatch, forged.retain(&.{}, 0));
    forged.release();
    try std.testing.expectEqual(@as(u64, 20), metadata_fixture.payloadUsage(&child));
    try std.testing.expectEqual(@as(u64, 20), (metadata_fixture.payloadUsage(&root) - child.identity_metadata_bytes));
    prepared.deinit();
    prepared.deinit();
    active.release();
    try std.testing.expectEqual(@as(u64, 0), (metadata_fixture.payloadUsage(&root) - child.identity_metadata_bytes));
    try std.testing.expectEqual(@as(usize, 0), child.batch_reservation_identities.count());
    try std.testing.expectError(error.DuplicateResourceSlice, child.prepareBatch(&.{
        .{ .slice = .text_merge_buffers, .bytes = 1 },
        .{ .slice = .text_merge_buffers, .bytes = 2 },
    }));
}

test "shared control metadata charges prepared identity storage through the parent until physical free" {
    const alloc = std.testing.allocator;
    var tracked = std.testing.FailingAllocator.init(alloc, .{});
    var parent = ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 1024 * 1024 }, .identity_allocator = alloc });
    defer parent.deinit(alloc);
    var child = ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 1024 * 1024 }, .identity_allocator = tracked.allocator() });
    var child_live = true;
    defer if (child_live) child.deinit(alloc);
    try child.installMemoryParent(parent.memoryParentPort());
    var prepared = try child.prepareBatch(&.{});
    defer prepared.deinit();
    const allocated = tracked.allocated_bytes - tracked.freed_bytes;
    try std.testing.expect(allocated > 0);
    // Zero payload does not mean zero physical bookkeeping. Neither owner may
    // omit the actual allocation made by its identity table.
    try std.testing.expect(child.snapshot().memory.used_bytes >= allocated);
    try std.testing.expect(parent.snapshot().memory.used_bytes >= allocated);
    prepared.deinit();
    const retained = tracked.allocated_bytes - tracked.freed_bytes;
    try std.testing.expect(child.snapshot().memory.used_bytes >= retained);
    try std.testing.expect(parent.snapshot().memory.used_bytes >= retained);
    child.deinit(alloc);
    child_live = false;
    try std.testing.expectEqual(tracked.allocated_bytes, tracked.freed_bytes);
}

test "shared control metadata refuses table growth beyond the envelope before allocation or publication" {
    const alloc = std.testing.allocator;
    var tracked = std.testing.FailingAllocator.init(alloc, .{});
    var manager = ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 64 }, .identity_allocator = tracked.allocator() });
    defer manager.deinit(alloc);
    const before = manager.snapshot().memory.used_bytes;
    // A complete identity table cannot fit in this envelope even for an empty
    // request. Retire unexpected success before failing so the red regression
    // does not lose a live identity during teardown.
    if (manager.prepareBatch(&.{})) |value| {
        var unexpected = value;
        unexpected.deinit();
        return error.TestExpectedError;
    } else |err| {
        try std.testing.expectEqual(error.ResourceBudgetExceeded, err);
    }
    try std.testing.expectEqual(@as(usize, 0), tracked.allocated_bytes);
    try std.testing.expectEqual(@as(usize, 0), manager.batch_reservation_identities.count());
    try std.testing.expectEqual(before, manager.snapshot().memory.used_bytes);
}

test "shared control metadata charges ordinary reservation and observer tables without overwriting physical admission" {
    const a = std.testing.allocator;
    var backing = std.testing.FailingAllocator.init(a, .{});
    var parent = ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = MiB } });
    defer parent.deinit(a);
    var manager = ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = MiB }, .identity_allocator = backing.allocator() });
    var live = true;
    defer if (live) manager.deinit(a);
    try manager.installMemoryParent(parent.memoryParentPort());
    var reservation = try manager.reserveImmediate(.text_merge_buffers, 37);
    defer reservation.release();
    var batch = try manager.reserveBatchCheckedWithHostCharge(&.{}, 19);
    defer batch.release();
    var current: u64 = 0;
    try manager.adjustUsage(.inference_prompt_cache, &current, 11);
    defer if (live) manager.adjustUsage(.inference_prompt_cache, &current, 0) catch unreachable;
    const physical = backing.allocated_bytes - backing.freed_bytes;
    try std.testing.expectEqual(physical, manager.identity_metadata_bytes);
    try std.testing.expectEqual(physical + 67, manager.snapshot().memory.used_bytes);
    try std.testing.expectEqual(physical + 67, parent.snapshot().memory.used_bytes);
    try manager.adjustUsage(.inference_prompt_cache, &current, 0);
    reservation.release();
    batch.release();
    try std.testing.expectEqual(physical, manager.snapshot().memory.used_bytes);
    manager.deinit(a);
    live = false;
    try std.testing.expectEqual(backing.allocated_bytes, backing.freed_bytes);
    try std.testing.expectEqual(@as(u64, 0), parent.snapshot().memory.used_bytes);
}

test "shared control metadata parent refusal and backing OOM roll back before identity publication" {
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |deny_backing| {
        var backing = std.testing.FailingAllocator.init(a, .{ .fail_index = if (deny_backing) 0 else std.math.maxInt(usize) });
        var parent = ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = if (deny_backing) MiB else 64 } });
        defer parent.deinit(a);
        var manager = ResourceManager.init(.{ .identity_allocator = backing.allocator() });
        defer manager.deinit(a);
        try manager.installMemoryParent(parent.memoryParentPort());
        if (deny_backing) {
            try std.testing.expectError(error.OutOfMemory, manager.prepareBatch(&.{}));
        } else {
            try std.testing.expectError(error.ResourceBudgetExceeded, manager.prepareBatch(&.{}));
        }
        try std.testing.expectEqual(@as(usize, 0), backing.allocated_bytes);
        try std.testing.expectEqual(@as(u32, 0), manager.batch_reservation_identities.count());
        try std.testing.expectEqual(@as(u64, 0), manager.identity_metadata_bytes);
        try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
        try std.testing.expectEqual(@as(u64, 0), parent.snapshot().memory.used_bytes);
    }
}

test "shared control metadata bounds simultaneous growth storage and preserves old identities on refusal" {
    const a = std.testing.allocator;
    var final_size: u64 = 0;
    var growth_peak: u64 = 0;
    for ([_]bool{ false, true }) |limited| {
        var backing = std.testing.FailingAllocator.init(a, .{});
        var manager = ResourceManager.init(.{ .identity_allocator = backing.allocator(), .identity_metadata_limit_bytes = if (limited) final_size else 64 * MiB });
        defer manager.deinit(a);
        var prepared: std.ArrayList(PreparedBatchReservation) = .empty;
        defer {
            for (prepared.items) |*item| item.deinit();
            prepared.deinit(a);
        }
        try prepared.ensureUnusedCapacity(a, 1);
        prepared.appendAssumeCapacity(try manager.prepareBatch(&.{}));
        const initial = manager.identity_metadata_bytes;
        while (manager.batch_reservation_identities.map.available > 0) {
            try prepared.ensureUnusedCapacity(a, 1);
            prepared.appendAssumeCapacity(try manager.prepareBatch(&.{}));
        }
        try prepared.ensureUnusedCapacity(a, 1);
        if (limited) {
            const count = manager.batch_reservation_identities.count();
            const allocations = backing.alloc_index;
            try std.testing.expectError(error.ResourceBudgetExceeded, manager.prepareBatch(&.{}));
            try std.testing.expectEqual(allocations, backing.alloc_index);
            try std.testing.expectEqual(count, manager.batch_reservation_identities.count());
            try std.testing.expectEqual(initial, manager.identity_metadata_bytes);
            try std.testing.expectEqual(initial, manager.snapshot().memory.used_bytes);
            manager.identity_metadata_limit_bytes = growth_peak;
        }
        prepared.appendAssumeCapacity(try manager.prepareBatch(&.{}));
        const physical = backing.allocated_bytes - backing.freed_bytes;
        try std.testing.expectEqual(physical, manager.identity_metadata_bytes);
        try std.testing.expect(physical > initial);
        try std.testing.expectEqual(initial + physical, manager.snapshot().memory.peak_bytes);
        if (!limited) {
            final_size = physical;
            growth_peak = initial + physical;
        }
        for (prepared.items) |*item| {
            var acquired = switch (item.tryAcquire()) {
                .acquired => |credit| credit,
                else => return error.TestExpectedEqual,
            };
            acquired.release();
        }
        try std.testing.expectEqual(@as(u32, 0), manager.batch_reservation_identities.count());
        try std.testing.expectEqual(physical, manager.snapshot().memory.used_bytes);
    }
}

test "shared control metadata finite cap applies without an aggregate memory limit" {
    const a = std.testing.allocator;
    var backing = std.testing.FailingAllocator.init(a, .{});
    var manager = ResourceManager.init(.{ .identity_allocator = backing.allocator(), .identity_metadata_limit_bytes = 64 });
    defer manager.deinit(a);
    try std.testing.expectError(error.ResourceBudgetExceeded, manager.prepareBatch(&.{}));
    try std.testing.expectEqual(@as(usize, 0), backing.allocated_bytes);
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
}

test "shared control metadata operation allocator survives startup move and late parent installation" {
    const a = std.testing.allocator;
    const Startup = struct {
        fn init() !ResourceManager {
            var manager = ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
            errdefer manager.deinit(std.testing.allocator);
            var prepared = try manager.prepareBatch(&.{});
            prepared.deinit();
            return manager;
        }
    };
    var parent = ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = MiB } });
    defer parent.deinit(a);
    var manager = try Startup.init();
    var live = true;
    defer if (live) manager.deinit(a);
    const physical = manager.identity_metadata_bytes;
    try std.testing.expect(physical > 0);
    try manager.installMemoryParent(parent.memoryParentPort());
    try std.testing.expectEqual(physical, parent.snapshot().memory.used_bytes);
    var prepared = try manager.prepareBatch(&.{});
    prepared.deinit();
    manager.deinit(a);
    live = false;
    try std.testing.expectEqual(@as(u64, 0), parent.snapshot().memory.used_bytes);
}

test "shared control metadata bounded observer admission refuses a busy parent without allocation" {
    const a = std.testing.allocator;
    var backing = std.testing.FailingAllocator.init(a, .{});
    var parent = ResourceManager.init(.{});
    defer parent.deinit(a);
    var manager = ResourceManager.init(.{ .identity_allocator = backing.allocator() });
    defer manager.deinit(a);
    try manager.installMemoryParent(parent.memoryParentPort());
    try std.testing.expect(manager.mutex.tryLock());
    try std.testing.expect(parent.mutex.tryLock());
    const result = manager.reconcileUsageLockedWithParentAdmission(.inference_prompt_cache, 123, 0, 1, true, true);
    parent.mutex.unlock();
    manager.mutex.unlock();
    try std.testing.expectError(error.ParentMemoryBusy, result);
    try std.testing.expectEqual(@as(usize, 0), backing.allocated_bytes);
    try std.testing.expectEqual(@as(u32, 0), manager.observer_identities.count());
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
}

test "shared control metadata partial grants compute headroom after first identity allocation" {
    const a = std.testing.allocator;
    var probe = ResourceManager.init(.{ .identity_allocator = a });
    var initial = try probe.reserveImmediate(.text_merge_buffers, 1);
    initial.release();
    const table_bytes = probe.identity_metadata_bytes;
    probe.deinit(a);
    var manager = ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = table_bytes + 50 }, .identity_allocator = a });
    defer manager.deinit(a);
    var split = try manager.reserveOwnedSplitAtMost(.text_merge_buffers, 10000, 10);
    defer split.release();
    try std.testing.expectEqual(@as(u64, 40), split.primary_bytes);
    try std.testing.expectEqual(@as(u64, 10), split.secondary_bytes);
    try std.testing.expectEqual(table_bytes + 50, manager.snapshot().memory.used_bytes);
}

test "shared control metadata checked batch reports quota refusal separately from backing OOM" {
    const a = std.testing.allocator;
    var backing = std.testing.FailingAllocator.init(a, .{});
    var manager = ResourceManager.init(.{ .identity_allocator = backing.allocator(), .identity_metadata_limit_bytes = 64 });
    defer manager.deinit(a);
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, manager.reserveBatchCheckedWithHostCharge(&.{}, 0));
    try std.testing.expectEqual(@as(usize, 0), backing.allocated_bytes);
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
}

test "shared control metadata bounded allocation failure never calls a legacy parent release" {
    const a = std.testing.allocator;
    const Legacy = struct {
        var releases: usize = 0;
        fn release(ptr: *anyopaque, bytes: u64) callconv(.c) void {
            releases += 1;
            const owner: *ResourceManager = @ptrCast(@alignCast(ptr));
            owner.releaseExternal(bytes);
        }
    };
    Legacy.releases = 0;
    var backing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    var parent = ResourceManager.init(.{});
    defer parent.deinit(a);
    var manager = ResourceManager.init(.{ .identity_allocator = backing.allocator() });
    defer manager.deinit(a);
    var port = parent.memoryParentPort();
    port.release_fn = Legacy.release;
    try manager.installMemoryParent(port);
    try std.testing.expect(manager.mutex.tryLock());
    const result = manager.reconcileUsageLockedWithParentAdmission(.inference_prompt_cache, 123, 0, 1, true, true);
    manager.mutex.unlock();
    try std.testing.expectError(error.OutOfMemory, result);
    try std.testing.expectEqual(@as(usize, 0), Legacy.releases);
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    try std.testing.expectEqual(@as(u64, 0), parent.snapshot().memory.used_bytes);
}

test "shared control metadata bounded allocation failure retains busy parent debt until independent cleanup" {
    const a = std.testing.allocator;
    const Gate = struct {
        var busy = true;
        var legacy_releases: usize = 0;
        fn release(ptr: *anyopaque, bytes: u64) callconv(.c) void {
            legacy_releases += 1;
            const owner: *ResourceManager = @ptrCast(@alignCast(ptr));
            owner.releaseExternal(bytes);
        }
        fn tryRelease(ptr: *anyopaque, bytes: u64) callconv(.c) ResourceManager.MemoryParent.ReleaseResult {
            if (busy) return .busy;
            const owner: *ResourceManager = @ptrCast(@alignCast(ptr));
            return owner.tryReleaseExternal(bytes);
        }
        fn checkpoint(_: *anyopaque) !void {}
    };
    Gate.busy = true;
    Gate.legacy_releases = 0;
    var backing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    var parent = ResourceManager.init(.{});
    defer parent.deinit(a);
    var manager = ResourceManager.init(.{ .identity_allocator = backing.allocator() });
    defer manager.deinit(a);
    var port = parent.memoryParentPort();
    port.release_fn = Gate.release;
    port.try_release_fn = Gate.tryRelease;
    try manager.installMemoryParent(port);
    try std.testing.expect(manager.mutex.tryLock());
    const result = manager.reconcileUsageLockedWithParentAdmission(.inference_prompt_cache, 123, 0, 1, true, true);
    manager.mutex.unlock();
    try std.testing.expectError(error.OutOfMemory, result);
    const debt = manager.snapshot().memory.used_bytes;
    try std.testing.expect(debt > 0);
    try std.testing.expectEqual(debt, parent.snapshot().memory.used_bytes);
    try std.testing.expectEqual(@as(usize, 0), Gate.legacy_releases);
    try std.testing.expectEqual(@as(usize, 0), backing.allocated_bytes);
    var context: u8 = 0;
    const payload: read_admission.Payload = .{ .ptr = &context, .vtable = &.{ .checkpoint = Gate.checkpoint } };
    const cleanup = read_admission.Port.init(&payload);
    try std.testing.expectEqual(@as(usize, 0), try manager.drainPayloadRetirementWithAdmission(cleanup));
    try std.testing.expectEqual(debt, manager.snapshot().memory.used_bytes);
    Gate.busy = false;
    try std.testing.expectEqual(@as(usize, 0), try manager.drainPayloadRetirementWithAdmission(cleanup));
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    try std.testing.expectEqual(@as(u64, 0), parent.snapshot().memory.used_bytes);
    try std.testing.expectEqual(@as(usize, 0), Gate.legacy_releases);
}

test "shared control metadata bounded rehash retains old table retirement through a busy parent" {
    const a = std.testing.allocator;
    const Gate = struct {
        var busy = true;
        var legacy_releases: usize = 0;
        fn release(ptr: *anyopaque, bytes: u64) callconv(.c) void {
            legacy_releases += 1;
            const owner: *ResourceManager = @ptrCast(@alignCast(ptr));
            owner.releaseExternal(bytes);
        }
        fn tryRelease(ptr: *anyopaque, bytes: u64) callconv(.c) ResourceManager.MemoryParent.ReleaseResult {
            if (busy) return .busy;
            const owner: *ResourceManager = @ptrCast(@alignCast(ptr));
            return owner.tryReleaseExternal(bytes);
        }
        fn checkpoint(_: *anyopaque) !void {}
        fn insert(manager: *ResourceManager, id: usize) !void {
            try std.testing.expect(manager.mutex.tryLock());
            defer manager.mutex.unlock();
            try manager.reconcileUsageLockedWithParentAdmission(.inference_prompt_cache, id, 0, 1, true, true);
        }
    };
    Gate.busy = true;
    Gate.legacy_releases = 0;
    var backing = std.testing.FailingAllocator.init(a, .{});
    var parent = ResourceManager.init(.{});
    defer parent.deinit(a);
    var manager = ResourceManager.init(.{ .identity_allocator = backing.allocator() });
    defer manager.deinit(a);
    var port = parent.memoryParentPort();
    port.release_fn = Gate.release;
    port.try_release_fn = Gate.tryRelease;
    try manager.installMemoryParent(port);
    var count: usize = 1;
    try Gate.insert(&manager, count);
    const initial_table = backing.allocated_bytes - backing.freed_bytes;
    while (manager.observer_identities.map.available > 0) {
        count += 1;
        try Gate.insert(&manager, count);
    }
    count += 1;
    try Gate.insert(&manager, count);
    const current_table = backing.allocated_bytes - backing.freed_bytes;
    try std.testing.expect(current_table > initial_table);
    try std.testing.expectEqual(@as(usize, 0), Gate.legacy_releases);
    try std.testing.expectEqual(current_table + initial_table + count, manager.snapshot().memory.used_bytes);
    try std.testing.expectEqual(manager.snapshot().memory.used_bytes, parent.snapshot().memory.used_bytes);
    var context: u8 = 0;
    const payload: read_admission.Payload = .{ .ptr = &context, .vtable = &.{ .checkpoint = Gate.checkpoint } };
    const cleanup = read_admission.Port.init(&payload);
    Gate.busy = false;
    try std.testing.expectEqual(@as(usize, 0), try manager.drainPayloadRetirementWithAdmission(cleanup));
    try std.testing.expectEqual(current_table + count, manager.snapshot().memory.used_bytes);
    try std.testing.expectEqual(manager.snapshot().memory.used_bytes, parent.snapshot().memory.used_bytes);
    try std.testing.expectEqual(@as(usize, 0), Gate.legacy_releases);
}

test "shared control metadata object backing runs unlocked with admission held through physical free" {
    const a = std.testing.allocator;
    const Backing = struct {
        manager: *ResourceManager,
        admitted: bool = false,
        retired_after_free: bool = false,
        fn alloc(ptr: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (!self.manager.mutex.tryLock()) return null;
            self.admitted = self.manager.control_object_bytes >= len and self.manager.memory.used_bytes >= len;
            self.manager.mutex.unlock();
            return std.testing.allocator.rawAlloc(len, alignment, ra);
        }
        fn free(ptr: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.manager.mutex.tryLock()) {
                self.retired_after_free = self.manager.control_object_bytes >= bytes.len and self.manager.memory.used_bytes >= bytes.len;
                self.manager.mutex.unlock();
            }
            std.testing.allocator.rawFree(bytes, alignment, ra);
        }
        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = std.mem.Allocator.noResize, .remap = std.mem.Allocator.noRemap, .free = free } };
        }
    };
    var parent = ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 4096 } });
    defer parent.deinit(a);
    var manager = ResourceManager.init(.{});
    defer manager.deinit(a);
    try manager.installMemoryParent(parent.memoryParentPort());
    var backing = Backing{ .manager = &manager };
    var operation = manager.controlMetadataAllocation(backing.allocator());
    const alloc = operation.allocator();
    const bytes = try alloc.alloc(u8, 73);
    var live = true;
    defer if (live) alloc.free(bytes);
    try std.testing.expect(backing.admitted);
    try std.testing.expectEqual(@as(u64, 73), parent.snapshot().memory.used_bytes);
    try std.testing.expectEqual(@as(u64, 73), manager.snapshot().control_object_bytes);
    alloc.free(bytes);
    live = false;
    try std.testing.expect(backing.retired_after_free);
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    try std.testing.expectEqual(@as(u64, 0), parent.snapshot().memory.used_bytes);
}

test "shared control metadata objects and identity tables compete for one finite cap" {
    const a = std.testing.allocator;
    var probe = ResourceManager.init(.{ .identity_allocator = a });
    var prepared_probe = try probe.prepareBatch(&.{});
    prepared_probe.deinit();
    const table_bytes = probe.identity_metadata_bytes;
    probe.deinit(a);
    var backing = std.testing.FailingAllocator.init(a, .{});
    var manager = ResourceManager.init(.{ .identity_allocator = backing.allocator(), .identity_metadata_limit_bytes = table_bytes });
    defer manager.deinit(a);
    var operation = manager.controlMetadataAllocation(backing.allocator());
    const alloc = operation.allocator();
    const bytes = try alloc.alloc(u8, 1);
    var live = true;
    defer if (live) alloc.free(bytes);
    const allocations = backing.alloc_index;
    try std.testing.expectError(error.ResourceBudgetExceeded, manager.prepareBatch(&.{}));
    try std.testing.expectEqual(allocations, backing.alloc_index);
    try std.testing.expectEqual(@as(u64, 1), manager.snapshot().memory.used_bytes);
    alloc.free(bytes);
    live = false;
    var prepared = try manager.prepareBatch(&.{});
    defer prepared.deinit();
    try std.testing.expectEqual(table_bytes, manager.snapshot().memory.used_bytes);
    try std.testing.expectError(error.OutOfMemory, alloc.alloc(u8, 1));
    try std.testing.expectEqual(error.ResourceBudgetExceeded, operation.allocationError(error.OutOfMemory));
    try std.testing.expectEqual(table_bytes, manager.snapshot().memory.used_bytes);
}

test "shared control metadata object OOM and parent quota refusal preserve distinct causes and exact rollback" {
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |deny_parent| {
        var backing = std.testing.FailingAllocator.init(a, .{ .fail_index = if (deny_parent) std.math.maxInt(usize) else 0 });
        var parent = ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = if (deny_parent) 1 else 4096 } });
        defer parent.deinit(a);
        var manager = ResourceManager.init(.{});
        defer manager.deinit(a);
        try manager.installMemoryParent(parent.memoryParentPort());
        var operation = manager.controlMetadataAllocation(backing.allocator());
        try std.testing.expectError(error.OutOfMemory, operation.allocator().alloc(u8, 128));
        try std.testing.expectEqual(if (deny_parent) error.ResourceBudgetExceeded else error.OutOfMemory, operation.allocationError(error.OutOfMemory));
        try std.testing.expectEqual(@as(usize, 0), backing.allocated_bytes);
        try std.testing.expectEqual(@as(u64, 0), manager.snapshot().control_object_bytes);
        try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
        try std.testing.expectEqual(@as(u64, 0), parent.snapshot().memory.used_bytes);
    }
}

test "shared control metadata objects survive startup relocation and join a late parent exactly once" {
    const a = std.testing.allocator;
    const Startup = struct {
        const Owned = struct { manager: ResourceManager, bytes: []u8 };
        fn init() !Owned {
            var manager = ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
            var operation = manager.controlMetadataAllocation(std.testing.allocator);
            const bytes = try operation.allocator().alloc(u8, 91);
            return .{ .manager = manager, .bytes = bytes };
        }
    };
    var parent = ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 4096 } });
    defer parent.deinit(a);
    var value = try Startup.init();
    var operation = value.manager.controlMetadataAllocation(a);
    defer value.manager.deinit(a);
    var live = true;
    defer if (live) operation.allocator().free(value.bytes);
    // Deinit order is explicit below: physical storage outlives its startup
    // stack frame, then joins and leaves the published parent's envelope.
    try value.manager.installMemoryParent(parent.memoryParentPort());
    try std.testing.expectEqual(@as(u64, 91), parent.snapshot().memory.used_bytes);
    try value.manager.installMemoryParent(parent.memoryParentPort());
    try std.testing.expectEqual(@as(u64, 91), parent.snapshot().memory.used_bytes);
    operation.allocator().free(value.bytes);
    live = false;
    try std.testing.expectEqual(@as(u64, 0), parent.snapshot().memory.used_bytes);
}

test "prepared memory batch rejects metadata OOM and overflow before capacity admission" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var manager = ResourceManager.init(.{ .identity_allocator = failing.allocator() });
    defer manager.deinit(std.testing.allocator);
    try std.testing.expectError(error.OutOfMemory, manager.prepareBatch(&.{.{ .slice = .text_merge_buffers, .bytes = 10 }}));
    try std.testing.expectError(error.ResourceRequestTooLarge, manager.prepareBatch(&.{
        .{ .slice = .text_merge_buffers, .bytes = std.math.maxInt(u64) },
        .{ .slice = .lsm_read_working_set, .bytes = 1 },
    }));
    try std.testing.expectEqual(@as(usize, 0), manager.batch_reservation_identities.count());
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    failing.fail_index = std.math.maxInt(usize);
    var empty = try manager.prepareBatch(&.{});
    defer empty.deinit();
    var credit = switch (empty.tryAcquire()) {
        .acquired => |credit| credit,
        else => return error.TestExpectedEqual,
    };
    credit.release();
    try std.testing.expectEqual(@as(usize, 0), manager.batch_reservation_identities.count());
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.accounting_errors);
}

test "batch reservation transfer preserves full-capacity ancestors and exact independent retirement" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var parent = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 100 }, .identity_allocator = std.testing.allocator });
    defer parent.deinit(std.testing.allocator);
    var manager = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 100 }, .identity_allocator = failing.allocator() });
    defer manager.deinit(std.testing.allocator);
    try metadata_fixture.installParent(&manager, &parent);
    var source = try manager.reserveBatchCheckedWithHostCharge(&.{
        .{ .slice = .inference_model_residency, .bytes = 240 },
        .{ .slice = .inference_kv_working_set, .bytes = 60 },
    }, 100);
    defer source.release();
    var destination = try manager.reserveBatchCheckedWithHostCharge(&.{}, 0);
    defer destination.release();
    const before = manager.snapshot();
    const epoch = manager.pressureEpoch();
    const parent_epoch = parent.pressureEpoch();
    const allocations = failing.alloc_index;
    failing.fail_index = allocations;
    // The ancestor is completely full and all further identity allocations
    // fail. Physical KV credit can still leave the model-load reservation.
    try source.transferCreditTo(&destination, &.{.{ .slice = .inference_kv_working_set, .bytes = 60 }}, 20);
    try std.testing.expectEqual(allocations, failing.alloc_index);
    try std.testing.expectEqualDeep(before, manager.snapshot());
    try std.testing.expectEqual(epoch, manager.pressureEpoch());
    try std.testing.expectEqual(parent_epoch, parent.pressureEpoch());
    try std.testing.expectEqual(@as(u64, 100), (metadata_fixture.payloadUsage(&parent) - manager.identity_metadata_bytes));
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, manager.reserveBatchCheckedWithHostCharge(&.{}, 1));
    // A stale copy taken after this handoff cannot retire credit after another
    // partition changes its registered descriptor.
    var stale_destination = destination;
    try destination.transferCreditTo(&source, &.{.{ .slice = .inference_kv_working_set, .bytes = 10 }}, 3);
    stale_destination.release();
    try std.testing.expectEqual(@as(u64, 100), (metadata_fixture.payloadUsage(&parent) - manager.identity_metadata_bytes));
    source.release();
    try std.testing.expectEqual(@as(u64, 17), metadata_fixture.payloadUsage(&manager));
    try std.testing.expectEqual(@as(u64, 17), (metadata_fixture.payloadUsage(&parent) - manager.identity_metadata_bytes));
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.inference_model_residency).used_bytes);
    try std.testing.expectEqual(@as(u64, 50), manager.sliceStats(.inference_kv_working_set).used_bytes);
    destination.release();
    destination.release();
    source.release();
    try std.testing.expectEqual(@as(u64, 0), (metadata_fixture.payloadUsage(&parent) - manager.identity_metadata_bytes));
    try std.testing.expectEqual(@as(u64, 0), metadata_fixture.payloadUsage(&manager));
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.inference_kv_working_set).used_bytes);
    try std.testing.expectEqual(@as(usize, 0), manager.batch_reservation_identities.count());
    try std.testing.expectEqual(@as(u64, 1), manager.snapshot().memory.accounting_errors);
}

test "batch reservation transfer rejects invalid owners and partial moves atomically" {
    var manager = try metadata_fixture.init(.{ .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    var foreign = try metadata_fixture.init(.{ .identity_allocator = std.testing.allocator });
    defer foreign.deinit(std.testing.allocator);
    var source = try manager.reserveBatchCheckedWithHostCharge(&.{
        .{ .slice = .inference_model_residency, .bytes = 20 },
        .{ .slice = .inference_kv_working_set, .bytes = 10 },
    }, 15);
    defer source.release();
    var destination = try manager.reserveBatchCheckedWithHostCharge(&.{}, 0);
    defer destination.release();
    const source_before = source;
    const destination_before = destination;
    const epoch = manager.pressureEpoch();
    // Earlier valid slices must not be moved when a later slice or host
    // component is invalid.
    try std.testing.expectError(error.InvalidReservationReduction, source.transferCreditTo(&destination, &.{
        .{ .slice = .inference_model_residency, .bytes = 5 },
        .{ .slice = .inference_kv_working_set, .bytes = 11 },
    }, 1));
    try std.testing.expectError(error.InvalidReservationReduction, source.transferCreditTo(&destination, &.{.{ .slice = .inference_model_residency, .bytes = 5 }}, 16));
    try std.testing.expectError(error.DuplicateResourceSlice, source.transferCreditTo(&destination, &.{
        .{ .slice = .inference_model_residency, .bytes = 1 },
        .{ .slice = .inference_model_residency, .bytes = 1 },
    }, 1));
    try std.testing.expectError(error.ResourceAccountingMismatch, source.transferCreditTo(&source, &.{}, 0));
    var alias = source;
    try std.testing.expectError(error.ResourceAccountingMismatch, source.transferCreditTo(&alias, &.{}, 0));
    var foreign_destination = try foreign.reserveBatchCheckedWithHostCharge(&.{}, 0);
    defer foreign_destination.release();
    try std.testing.expectError(error.ResourceAccountingMismatch, source.transferCreditTo(&foreign_destination, &.{}, 0));
    var stale_source = source;
    stale_source.amounts[sliceIndex(.inference_kv_working_set)] -= 1;
    try std.testing.expectError(error.ResourceAccountingMismatch, stale_source.transferCreditTo(&destination, &.{}, 0));
    var stale_destination = destination;
    stale_destination.host_charge_bytes = 1;
    try std.testing.expectError(error.ResourceAccountingMismatch, source.transferCreditTo(&stale_destination, &.{}, 0));
    var prepared = try manager.prepareBatch(&.{});
    defer prepared.deinit();
    var unpublished: BatchReservation = .{ .manager = &manager, .identity = prepared.identity, .amounts = prepared.amounts, .host_charge_bytes = prepared.host_charge_bytes };
    try std.testing.expectError(error.ResourceAccountingMismatch, source.transferCreditTo(&unpublished, &.{}, 0));
    try std.testing.expectError(error.ResourceAccountingMismatch, unpublished.transferCreditTo(&destination, &.{}, 0));
    var retired = try manager.reserveBatchCheckedWithHostCharge(&.{}, 0);
    var stale_retired = retired;
    retired.release();
    try std.testing.expectError(error.ReservationReleased, source.transferCreditTo(&retired, &.{}, 0));
    try std.testing.expectError(error.ReservationReleased, stale_retired.transferCreditTo(&destination, &.{}, 0));
    try std.testing.expectError(error.ReservationReleased, source.transferCreditTo(&stale_retired, &.{}, 0));
    try std.testing.expectEqualDeep(source_before, source);
    try std.testing.expectEqualDeep(destination_before, destination);
    try std.testing.expectEqual(@as(u64, 15), metadata_fixture.payloadUsage(&manager));
    try std.testing.expectEqual(@as(u64, 20), manager.sliceStats(.inference_model_residency).used_bytes);
    try std.testing.expectEqual(@as(u64, 10), manager.sliceStats(.inference_kv_working_set).used_bytes);
    // Only explicit preparation/release above may advance the wait epoch.
    const after_setup_epoch = manager.pressureEpoch();
    try std.testing.expect(after_setup_epoch >= epoch);
    try source.transferCreditTo(&destination, &.{}, 0);
    try std.testing.expectEqual(after_setup_epoch, manager.pressureEpoch());
}

test "batch reservation transfer checks destination arithmetic before publishing either owner" {
    var manager = try metadata_fixture.init(.{ .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    var source = try manager.reserveBatchCheckedWithHostCharge(&.{.{ .slice = .inference_kv_working_set, .bytes = 2 }}, 2);
    defer source.release();
    var destination = try manager.reserveBatchCheckedWithHostCharge(&.{}, 0);
    defer destination.release();
    // Valid aggregate admission cannot overflow a destination. Inject matching
    // descriptor/identity damage to exercise the defensive arithmetic checks;
    // restore it before cleanup, leaving real aggregate ownership untouched.
    const index = sliceIndex(.inference_kv_working_set);
    const owned = manager.batch_reservation_identities.getPtr(destination.identity).?;
    destination.amounts[index] = std.math.maxInt(u64);
    owned.amounts = destination.amounts;
    try std.testing.expectError(error.ResourceAccountingMismatch, source.transferCreditTo(&destination, &.{.{ .slice = .inference_kv_working_set, .bytes = 1 }}, 1));
    try std.testing.expectEqual(@as(u64, 2), source.amounts[index]);
    try std.testing.expectEqual(@as(u64, 0), destination.host_charge_bytes);
    destination.amounts[index] = 0;
    owned.amounts = destination.amounts;
    destination.host_charge_bytes = std.math.maxInt(u64);
    owned.host_charge_bytes = destination.host_charge_bytes;
    try std.testing.expectError(error.ResourceAccountingMismatch, source.transferCreditTo(&destination, &.{.{ .slice = .inference_kv_working_set, .bytes = 1 }}, 1));
    try std.testing.expectEqual(@as(u64, 2), source.host_charge_bytes);
    try std.testing.expectEqual(@as(u64, 0), destination.amounts[index]);
    destination.host_charge_bytes = 0;
    owned.host_charge_bytes = 0;
    try std.testing.expectEqual(@as(u64, 2), metadata_fixture.payloadUsage(&manager));
}

test "batch reservation transfer destination preparation failure preserves source credit" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var manager = ResourceManager.init(.{ .identity_allocator = failing.allocator() });
    defer manager.deinit(std.testing.allocator);
    var source = try manager.reserveBatchCheckedWithHostCharge(&.{.{ .slice = .inference_kv_working_set, .bytes = 40 }}, 20);
    defer source.release();
    const before = source;
    // Fill existing identity capacity without allocating, then deny the next
    // expansion. Empty destination identities still require admitted metadata.
    var destinations: std.ArrayList(BatchReservation) = .empty;
    defer {
        for (destinations.items) |*reservation| reservation.release();
        destinations.deinit(std.testing.allocator);
    }
    while (manager.batch_reservation_identities.map.available > 0) {
        try destinations.ensureUnusedCapacity(std.testing.allocator, 1);
        destinations.appendAssumeCapacity(try manager.reserveBatchCheckedWithHostCharge(&.{}, 0));
    }
    failing.fail_index = failing.alloc_index;
    try std.testing.expectError(error.OutOfMemory, manager.reserveBatchCheckedWithHostCharge(&.{}, 0));
    try std.testing.expectEqualDeep(before, source);
    try std.testing.expectEqual(@as(u64, 20), metadata_fixture.payloadUsage(&manager));
    try std.testing.expectEqual(@as(u64, 40), manager.sliceStats(.inference_kv_working_set).used_bytes);
    source.release();
    // Empty destination tokens also own live identities and must be retired.
    try std.testing.expectEqual(@as(u64, 0), metadata_fixture.payloadUsage(&manager));
}

test "budgeted allocator no reclaim view preserves shared ownership and ordinary reclamation" {
    const Cache = struct {
        retained: Reservation,
        owner: ?*BudgetedAllocator = null,
        dependency: std.atomic.Mutex = .unlocked,
        calls: usize = 0,
        locked_calls: usize = 0,
        fn reclaim(raw: *anyopaque, target: u64) u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (!self.dependency.tryLock()) {
                self.locked_calls += 1;
                return 0;
            }
            defer self.dependency.unlock();
            if (!self.owner.?.allocator_mutex.tryLock()) {
                self.locked_calls += 1;
                return 0;
            }
            self.owner.?.allocator_mutex.unlock();
            const bytes = @min(target, self.retained.bytes);
            self.retained.shrink(bytes);
            return bytes;
        }
    };
    var budgets = Options.defaultBudgets();
    budgets[sliceIndex(.document_extraction_working_set)] = .{ .hard_limit_bytes = 64 };
    var manager = ResourceManager.init(.{ .budgets = budgets });
    defer manager.deinit(std.testing.allocator);
    var cache: Cache = .{ .retained = try manager.reserve(.document_extraction_working_set, 48) };
    defer cache.retained.release();
    const identity = try manager.registerReclaimer(.document_extraction_working_set, &cache, Cache.reclaim);
    defer manager.unregisterReclaimer(identity);
    var backing_bytes: [4096]u8 = undefined;
    var backing = std.heap.FixedBufferAllocator.init(&backing_bytes);
    var owner = BudgetedAllocator.initReclaiming(&manager, .document_extraction_working_set, backing.allocator(), 1);
    defer owner.deinit();
    owner.credit_quantum = 1;
    cache.owner = &owner;
    const ordinary = owner.threadSafeAllocator();
    const quiet = owner.threadSafeNoReclaimAllocator();
    try std.testing.expect(BudgetedAllocator.fromAllocator(quiet).? == &owner);
    var arena = std.heap.ArenaAllocator.init(quiet);
    defer arena.deinit();
    try std.testing.expect(BudgetedAllocator.fromAllocator(arena.allocator()).? == &owner);
    var bytes = try quiet.alloc(u8, 8);
    @memset(bytes, 19);
    {
        try std.testing.expect(cache.dependency.tryLock());
        defer cache.dependency.unlock();
        try std.testing.expectError(error.OutOfMemory, quiet.alloc(u8, 32));
        try std.testing.expect(!quiet.rawResize(bytes, .of(u8), 32, @returnAddress()));
        try std.testing.expect(quiet.rawRemap(bytes, .of(u8), 32, @returnAddress()) == null);
        try std.testing.expectEqual(@as(usize, 0), cache.calls);
        try std.testing.expectEqual(@as(u64, 48), cache.retained.bytes);
        try std.testing.expectEqual(@as(u64, 8), owner.liveBytesThreadSafe());
        try std.testing.expect(owner.reclaim_on_denial);
        try std.testing.expect(quiet.rawResize(bytes, .of(u8), 4, @returnAddress()));
        bytes = bytes.ptr[0..4];
        const remapped = quiet.rawRemap(bytes, .of(u8), 8, @returnAddress()).?;
        bytes = remapped[0..8];
    }
    // Same physical allocation and ledger, even when the other view resizes,
    // remaps or frees it. Reclaim runs outside both allocator and dependency.
    try std.testing.expect(ordinary.rawResize(bytes, .of(u8), 32, @returnAddress()));
    bytes = bytes.ptr[0..32];
    try std.testing.expectEqual(@as(usize, 1), cache.calls);
    bytes = ordinary.rawRemap(bytes, .of(u8), 48, @returnAddress()).?[0..48];
    try std.testing.expectEqual(@as(usize, 2), cache.calls);
    try std.testing.expectEqual(@as(usize, 0), cache.locked_calls);
    try std.testing.expectEqual(@as(u8, 19), bytes[0]);
    quiet.free(bytes);
    try std.testing.expectEqual(@as(u64, 0), owner.liveBytesThreadSafe());
    try std.testing.expect(owner.reclaim_on_denial);
    const last = try ordinary.alloc(u8, 64);
    try std.testing.expectEqual(@as(usize, 3), cache.calls);
    quiet.free(last);
    try std.testing.expectEqual(@as(u64, 0), owner.liveBytesThreadSafe());
    try std.testing.expectEqual(@as(usize, 0), backing.end_index);
}

test "mapped residency observations never consume inference host admission" {
    var manager = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 8 * 1024 * 1024 * 1024 } });
    defer manager.deinit(std.testing.allocator);
    var mapped: u64 = 0;
    manager.observeUsageWithHostCharge(.full_text_segment_residency, &mapped, 16 * 1024 * 1024 * 1024, 0);
    try std.testing.expectEqual(@as(u64, 16 * 1024 * 1024 * 1024), manager.sliceStats(.full_text_segment_residency).used_bytes);
    try std.testing.expectEqual(@as(u64, 0), metadata_fixture.payloadUsage(&manager));
    var model = try manager.reserve(.inference_model_residency, 2 * 1024 * 1024 * 1024);
    defer model.release();
    try std.testing.expectError(error.ResourceBudgetExceeded, manager.reserve(.inference_scratch_working_set, 7 * 1024 * 1024 * 1024));
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.full_text_segment_residency).hard_limit_rejections);
    manager.observeUsageWithHostCharge(.full_text_segment_residency, &mapped, 0, 0);
    try std.testing.expectEqual(@as(u64, 2 * 1024 * 1024 * 1024), metadata_fixture.payloadUsage(&manager));
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.accounting_errors);
}
