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

// Owner-scoped context allocation accounting. Do not use process-global deltas
// as request evidence; this tracker observes only allocations made through its
// allocator and explicitly registered no-copy Metal backing.
const std = @import("std");
const builtin = @import("builtin");

var next_owner_identity: std.atomic.Value(u64) = .init(1);
pub const context_allocation_scope_schema_version: u32 = 1;
pub const context_batch_allocation_scope_schema_version: u32 = 1;

pub const ContextBatchKind = enum {
    exact_siblings,
    attention_matching_singleton,
};

fn allocateOwnerIdentity() u64 {
    var current = next_owner_identity.load(.monotonic);
    while (true) {
        if (current == std.math.maxInt(u64)) @panic("context allocation owner identity exhausted");
        if (next_owner_identity.cmpxchgWeak(current, current + 1, .monotonic, .monotonic)) |observed| {
            current = observed;
            continue;
        }
        return current;
    }
}

pub const Snapshot = struct {
    /// True when this value came from an owner tracker rather than the default
    /// unavailable value.
    available: bool = false,
    /// Process-unique identity of the tracker that produced this snapshot.
    /// A committed snapshot is accepted only by its originating owner.
    owner_identity: u64 = 0,
    /// malloc_size is target-specific. Logical counters remain valid when
    /// this is false; physical fields stay zero and must not be estimated.
    physical_available: bool = false,
    logical_live_bytes: u64 = 0,
    logical_peak_bytes: u64 = 0,
    /// Sum of malloc_size for live allocations. This is the allocator's
    /// actual usable allocation size. It excludes malloc-zone metadata and
    /// does not claim resident or page-exclusive physical memory.
    physical_live_bytes: u64 = 0,
    physical_peak_bytes: u64 = 0,
    shared_backing_live_bytes: u64 = 0,
    shared_backing_peak_bytes: u64 = 0,
    /// Request-exclusive device allocations. Shared/no-copy buffers are
    /// represented only by shared_backing_* and must never be added here.
    private_device_live_bytes: u64 = 0,
    private_device_peak_bytes: u64 = 0,
    /// Covers malloc usable bytes, no-copy shared backing, and typed
    /// request-exclusive device backing. It still excludes opaque command
    /// queue, pipeline, and runtime object storage.
    tracked_combined_physical_available: bool = false,
    tracked_combined_live_bytes: u64 = 0,
    /// Exact maximum of host usable bytes plus no-copy shared backing at each
    /// owner mutation, rather than the sum of two noncoincident peaks.
    tracked_combined_peak_bytes: u64 = 0,
};

pub const DeviceBacking = enum {
    /// A no-copy Metal buffer backed by host-visible pages. Counted once in
    /// shared_backing_*; callers must not also publish it as private device.
    shared_no_copy,
    /// StorageModePrivate backing leased exclusively to this request owner.
    private_device,
};

/// Owner-scoped C allocator with exact requested and Darwin malloc allocation
/// accounting. The tracker must remain at a stable address until every slice
/// allocated through allocator() has been freed.
pub const HostAllocationTracker = struct {
    owner_identity: u64,
    mutex: std.atomic.Mutex = .unlocked,
    logical_limit_bytes: ?usize = null,
    logical_live_bytes: usize = 0,
    logical_peak_bytes: usize = 0,
    physical_live_bytes: usize = 0,
    physical_peak_bytes: usize = 0,
    shared_backing_live_bytes: usize = 0,
    shared_backing_peak_bytes: usize = 0,
    private_device_live_bytes: usize = 0,
    private_device_peak_bytes: usize = 0,
    total_peak_bytes: usize = 0,

    const vtable: std.mem.Allocator.VTable = .{
        .alloc = alloc,
        .resize = resize,
        .remap = remap,
        .free = free,
    };

    pub fn init(logical_limit_bytes: ?usize, external_control_physical_bytes: usize) HostAllocationTracker {
        return .{
            .owner_identity = allocateOwnerIdentity(),
            .logical_limit_bytes = logical_limit_bytes,
            .physical_live_bytes = external_control_physical_bytes,
            .physical_peak_bytes = external_control_physical_bytes,
            .total_peak_bytes = external_control_physical_bytes,
        };
    }

    pub fn allocator(self: *HostAllocationTracker) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    /// Test-binary-only request-local fault arm. Existing allocations remain
    /// valid and releasable; the next positive allocation through this exact
    /// tracker fails. No process/global allocator state is changed.
    pub fn armNextPositiveAllocationFailure(self: *HostAllocationTracker) !void {
        if (!builtin.is_test) return error.UnqualifiedFaultInjection;
        self.lock();
        defer self.mutex.unlock();
        self.logical_limit_bytes = self.logical_live_bytes;
    }

    pub fn snapshot(self: *HostAllocationTracker) Snapshot {
        self.lock();
        defer self.mutex.unlock();
        const physical_available = comptime physicalAccountingAvailable();
        return .{
            .available = true,
            .owner_identity = self.owner_identity,
            .physical_available = physical_available,
            .logical_live_bytes = @intCast(self.logical_live_bytes),
            .logical_peak_bytes = @intCast(self.logical_peak_bytes),
            .physical_live_bytes = if (physical_available) @intCast(self.physical_live_bytes) else 0,
            .physical_peak_bytes = if (physical_available) @intCast(self.physical_peak_bytes) else 0,
            .shared_backing_live_bytes = @intCast(self.shared_backing_live_bytes),
            .shared_backing_peak_bytes = @intCast(self.shared_backing_peak_bytes),
            .private_device_live_bytes = @intCast(self.private_device_live_bytes),
            .private_device_peak_bytes = @intCast(self.private_device_peak_bytes),
            .tracked_combined_physical_available = physical_available,
            .tracked_combined_live_bytes = if (physical_available) @intCast(
                self.physical_live_bytes + self.shared_backing_live_bytes + self.private_device_live_bytes,
            ) else 0,
            .tracked_combined_peak_bytes = if (physical_available) @intCast(self.total_peak_bytes) else 0,
        };
    }

    fn lock(self: *HostAllocationTracker) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    fn physicalAccountingAvailable() bool {
        return switch (builtin.os.tag) {
            .driverkit, .ios, .maccatalyst, .macos, .tvos, .visionos, .watchos => true,
            else => false,
        };
    }

    fn physicalSize(ptr: [*]u8) usize {
        return switch (builtin.os.tag) {
            .driverkit, .ios, .maccatalyst, .macos, .tvos, .visionos, .watchos => std.c.malloc_size(@ptrCast(ptr)),
            else => 0,
        };
    }

    pub fn externalAllocationPhysicalSize(ptr: *anyopaque) ?usize {
        if (!physicalAccountingAvailable()) return null;
        return physicalSize(@ptrCast(ptr));
    }

    fn canAdd(self: *const HostAllocationTracker, logical: usize) bool {
        const next = std.math.add(usize, self.logical_live_bytes, logical) catch return false;
        return if (self.logical_limit_bytes) |limit| next <= limit else true;
    }

    fn recordAlloc(self: *HostAllocationTracker, logical: usize, physical: usize) bool {
        const new_logical = std.math.add(usize, self.logical_live_bytes, logical) catch return false;
        const new_physical = std.math.add(usize, self.physical_live_bytes, physical) catch return false;
        self.logical_live_bytes = new_logical;
        self.logical_peak_bytes = @max(self.logical_peak_bytes, new_logical);
        self.physical_live_bytes = new_physical;
        self.physical_peak_bytes = @max(self.physical_peak_bytes, new_physical);
        self.recordTotalPeak();
        return true;
    }

    fn recordTotalPeak(self: *HostAllocationTracker) void {
        const host_and_shared = std.math.add(usize, self.physical_live_bytes, self.shared_backing_live_bytes) catch @panic("owner allocation total overflow");
        const total = std.math.add(usize, host_and_shared, self.private_device_live_bytes) catch @panic("owner allocation total overflow");
        self.total_peak_bytes = @max(self.total_peak_bytes, total);
    }

    /// Serialize a no-copy MTLBuffer allocation with all host metadata
    /// mutations, so snapshots and the combined peak cannot miss overlap.
    pub fn beginSharedMutation(self: *HostAllocationTracker) void {
        self.lock();
    }
    pub fn endSharedMutation(self: *HostAllocationTracker) void {
        self.mutex.unlock();
    }
    pub fn recordSharedAllocLocked(self: *HostAllocationTracker, bytes: usize) !void {
        self.shared_backing_live_bytes = try std.math.add(usize, self.shared_backing_live_bytes, bytes);
        self.shared_backing_peak_bytes = @max(self.shared_backing_peak_bytes, self.shared_backing_live_bytes);
        self.recordTotalPeak();
    }
    pub fn recordSharedFreeLocked(self: *HostAllocationTracker, bytes: usize) void {
        if (bytes > self.shared_backing_live_bytes) @panic("shared backing allocation tracker underflow");
        self.shared_backing_live_bytes -= bytes;
    }

    /// Publish an exact device allocation transition under the same ledger
    /// mutex as host and shared allocations. `newly_allocated_bytes` is the
    /// actual allocatedSize of new private buffers created while the old
    /// backing was still live. `final_live_bytes` includes any old generation
    /// retained by an active/submitted frame after the backend call returns.
    /// This records the real overlap peak before reconciling to final live.
    pub fn recordDeviceTransition(
        self: *HostAllocationTracker,
        backing: DeviceBacking,
        prior_live_bytes: usize,
        newly_allocated_bytes: usize,
        final_live_bytes: usize,
    ) !void {
        self.lock();
        defer self.mutex.unlock();
        const live = switch (backing) {
            .shared_no_copy => &self.shared_backing_live_bytes,
            .private_device => &self.private_device_live_bytes,
        };
        const peak = switch (backing) {
            .shared_no_copy => &self.shared_backing_peak_bytes,
            .private_device => &self.private_device_peak_bytes,
        };
        const global_before = live.*;
        if (global_before < prior_live_bytes) return error.InvalidOwnerAllocationTransition;
        const transition_live = try std.math.add(usize, prior_live_bytes, newly_allocated_bytes);
        _ = transition_live;
        const global_overlap = try std.math.add(usize, global_before, newly_allocated_bytes);
        const without_prior = global_before - prior_live_bytes;
        const global_final = try std.math.add(usize, without_prior, final_live_bytes);
        const other_live = switch (backing) {
            .shared_no_copy => try std.math.add(usize, self.physical_live_bytes, self.private_device_live_bytes),
            .private_device => try std.math.add(usize, self.physical_live_bytes, self.shared_backing_live_bytes),
        };
        const total_overlap = try std.math.add(usize, other_live, global_overlap);
        const total_final = try std.math.add(usize, other_live, global_final);
        live.* = global_overlap;
        peak.* = @max(peak.*, global_overlap);
        self.total_peak_bytes = @max(self.total_peak_bytes, total_overlap);
        live.* = global_final;
        peak.* = @max(peak.*, global_final);
        self.total_peak_bytes = @max(self.total_peak_bytes, total_final);
    }

    /// Reconcile frame-retained generations after a backend drain. No new
    /// allocation occurred, so this cannot raise the overlap peak.
    pub fn reconcileDeviceLive(
        self: *HostAllocationTracker,
        backing: DeviceBacking,
        prior_live_bytes: usize,
        final_live_bytes: usize,
    ) !void {
        try self.recordDeviceTransition(backing, prior_live_bytes, 0, final_live_bytes);
    }

    fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *HostAllocationTracker = @ptrCast(@alignCast(raw));
        self.lock();
        defer self.mutex.unlock();
        if (!self.canAdd(len)) return null;
        // Keep the backing operation and counter publication in the same
        // critical section. A snapshot can never miss an in-flight allocation
        // and concurrent callers cannot transiently exceed the logical limit.
        const ptr = std.heap.c_allocator.rawAlloc(len, alignment, ret_addr) orelse return null;
        const physical = physicalSize(ptr);
        const recorded = self.canAdd(len) and self.recordAlloc(len, physical);
        if (!recorded) {
            std.heap.c_allocator.rawFree(ptr[0..len], alignment, ret_addr);
            return null;
        }
        return ptr;
    }

    fn resize(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *HostAllocationTracker = @ptrCast(@alignCast(raw));
        self.lock();
        defer self.mutex.unlock();
        // Growing in place would hide the real old/new coexistence from the
        // peak counter. Decline every growth so Allocator.realloc takes the
        // tracked alloc-copy-free path. Shrinking remains address preserving.
        if (new_len > memory.len) return false;
        const old_physical = physicalSize(memory.ptr);
        if (!std.heap.c_allocator.rawResize(memory, alignment, new_len, ret_addr)) return false;

        // c_allocator resize is address preserving; malloc_size therefore
        // remains attributable to the same allocation.
        const new_physical = physicalSize(memory.ptr);
        self.logical_live_bytes = self.logical_live_bytes - memory.len + new_len;
        self.logical_peak_bytes = @max(self.logical_peak_bytes, self.logical_live_bytes);
        self.physical_live_bytes = self.physical_live_bytes - old_physical + new_physical;
        self.physical_peak_bytes = @max(self.physical_peak_bytes, self.physical_live_bytes);
        self.recordTotalPeak();
        return true;
    }

    fn remap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
        // Force Allocator.realloc to alloc-copy-free so the old/new overlap is
        // visible to the tracker and included in its peak.
        return null;
    }

    fn free(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *HostAllocationTracker = @ptrCast(@alignCast(raw));
        const physical = physicalSize(memory.ptr);
        self.lock();
        if (self.logical_live_bytes < memory.len or self.physical_live_bytes < physical)
            @panic("host allocation tracker underflow");
        // Serialize the backing free with counter mutation. Otherwise another
        // thread could allocate between the counter decrement and C free and
        // the reported peak would miss a real overlap.
        std.heap.c_allocator.rawFree(memory, alignment, ret_addr);
        self.logical_live_bytes -= memory.len;
        self.physical_live_bytes -= physical;
        self.mutex.unlock();
    }
};

/// Stable heap owner for a request ledger that begins before compaction
/// payload preparation. Every allocator-backed compaction object and the
/// packed session may borrow tracker; release is valid only after all of them
/// have been destroyed. Opaque backend objects keep the total-physical claim
/// unavailable until their lifetime is moved under measured model ownership.
pub const RequestAllocationOwner = struct {
    tracker: *HostAllocationTracker,
    initial: Snapshot,
    committed: ?Snapshot = null,
    context_scope: ?ContextScopeState = null,

    /// Move-only by convention. Every mutation is checked against the live
    /// owner state, so stale copies cannot complete a different owner.
    pub const ContextScopeWitness = struct {
        owner_identity: u64,
        load_generation: u64,
    };

    const ContextScopeState = struct {
        load_generation: u64,
        isolated_route_bound: bool = false,
        prepared_allocator_bound: bool = false,
        execution_pool_bound: bool = false,
        execution_pool_admission_version: u32 = 0,
        execution_pool_admitted_backend_bytes: usize = 0,
        typed_storage_bound: bool = false,
        committed_drained: bool = false,
        batch: ?BatchScopeState = null,
        invalidated: bool = false,
    };

    const BatchScopeState = struct {
        kind: ContextBatchKind,
        expected_items: usize,
        completed_items: usize = 0,
    };

    pub const Measurement = struct {
        available: bool = false,
        committed_live_bytes: u64 = 0,
        request_peak_bytes: u64 = 0,
        released_baseline_bytes: ?u64 = null,
    };

    pub const ReleaseReceipt = struct {
        released: bool = false,
        /// Versioned request-exclusive context-allocation scope. These are
        /// allocator sizes, not RSS or total process/GPU residency.
        context_allocation_scope_version: u32 = context_allocation_scope_schema_version,
        context_allocation_scope_complete: bool = false,
        context_batch_scope_version: u32 = 0,
        context_batch_kind: ?ContextBatchKind = null,
        context_batch_expected_items: u64 = 0,
        context_batch_completed_items: u64 = 0,
        model_execution_pool_admission_version: u32 = 0,
        model_execution_pool_admitted_backend_bytes: u64 = 0,
        full_owner_physical_available: bool = false,
        initial: Snapshot = .{},
        committed: Snapshot = .{},
        final: Snapshot = .{},
        measurement: Measurement = .{},
        allocation: Snapshot = .{},
        post_release_live_bytes: ?u64 = null,
    };

    pub fn init(logical_limit_bytes: ?usize) !RequestAllocationOwner {
        const tracker = try std.heap.c_allocator.create(HostAllocationTracker);
        errdefer std.heap.c_allocator.destroy(tracker);
        const control = HostAllocationTracker.externalAllocationPhysicalSize(tracker) orelse 0;
        tracker.* = HostAllocationTracker.init(logical_limit_bytes, control);
        return .{ .tracker = tracker, .initial = tracker.snapshot() };
    }

    pub fn allocator(self: *RequestAllocationOwner) std.mem.Allocator {
        return self.tracker.allocator();
    }

    pub fn beginContextScope(self: *RequestAllocationOwner, load_generation: u64) !ContextScopeWitness {
        if (load_generation == 0 or self.context_scope != null or self.committed != null)
            return error.InvalidContextOwnerScope;
        if (!self.initial.available or !self.initial.physical_available or
            self.initial.owner_identity != self.tracker.owner_identity)
            return error.ContextPhysicalAccountingUnavailable;
        self.context_scope = .{ .load_generation = load_generation };
        return .{ .owner_identity = self.initial.owner_identity, .load_generation = load_generation };
    }

    fn scopeState(self: *RequestAllocationOwner, witness: ContextScopeWitness) !*ContextScopeState {
        if (witness.owner_identity == 0 or witness.owner_identity != self.initial.owner_identity)
            return error.StaleContextOwnerScope;
        const state = if (self.context_scope) |*value| value else return error.InvalidContextOwnerScope;
        if (witness.load_generation == 0 or witness.load_generation != state.load_generation)
            return error.StaleContextOwnerScope;
        return state;
    }

    /// Bind the allocator that owns rendered/tokenized context provenance and
    /// PreparedPlan/Request allocations. A general pipeline allocator cannot
    /// satisfy this check.
    pub fn bindPreparedAllocator(
        self: *RequestAllocationOwner,
        witness: ContextScopeWitness,
        prepared_allocator: std.mem.Allocator,
    ) !void {
        const state = try self.scopeState(witness);
        if (prepared_allocator.ptr != @as(*anyopaque, @ptrCast(self.tracker)) or
            prepared_allocator.vtable != &HostAllocationTracker.vtable)
            return error.ContextOwnerAllocatorMismatch;
        state.prepared_allocator_bound = true;
    }

    pub const IsolatedRouteFacts = struct {
        metal: bool,
        f16_kv: bool,
        prompt_cache_disabled: bool,
        media_disabled: bool,
        speculation_disabled: bool,
        draft_disabled: bool,
        legacy_qualification_disabled: bool,
        retained_prepared_payload: bool,
    };

    /// Bind facts derived from the internal serving route, never request JSON.
    pub fn bindIsolatedRoute(
        self: *RequestAllocationOwner,
        witness: ContextScopeWitness,
        facts: IsolatedRouteFacts,
    ) !void {
        const state = try self.scopeState(witness);
        if (!facts.metal or !facts.f16_kv or !facts.prompt_cache_disabled or
            !facts.media_disabled or !facts.speculation_disabled or
            !facts.draft_disabled or !facts.legacy_qualification_disabled or
            !facts.retained_prepared_payload)
            return error.IncompleteContextOwnerScope;
        state.isolated_route_bound = true;
    }

    /// Called by a live ContextExecutionPool.Lease after it validates its own
    /// generation and active nonce. The admitted bytes are disclosed model
    /// residency and are deliberately not added to request context bytes.
    pub fn bindExecutionPool(
        self: *RequestAllocationOwner,
        witness: ContextScopeWitness,
        pool_generation: u64,
        admission_version: u32,
        admitted_backend_bytes: usize,
    ) !void {
        const state = try self.scopeState(witness);
        if (pool_generation != state.load_generation or admission_version == 0 or admitted_backend_bytes == 0)
            return error.StaleContextExecutionLease;
        state.execution_pool_bound = true;
        state.execution_pool_admission_version = admission_version;
        state.execution_pool_admitted_backend_bytes = admitted_backend_bytes;
    }

    /// Called by a request-local KvStorageRuntime only after a typed device
    /// hook with exact allocation transitions and owner drain is installed.
    pub fn bindTypedStorage(
        self: *RequestAllocationOwner,
        witness: ContextScopeWitness,
        storage_tracker: *HostAllocationTracker,
    ) !void {
        const state = try self.scopeState(witness);
        if (storage_tracker != self.tracker) return error.ContextFrameOwnershipMismatch;
        state.typed_storage_bound = true;
    }

    /// Bind one allocation ledger to the executable unit that owns it. Exact
    /// siblings share one serial KV runtime and therefore one owner. An
    /// attention-matching group is deliberately a singleton and stays bound
    /// to that request's owner.
    pub fn bindBatchGroup(
        self: *RequestAllocationOwner,
        witness: ContextScopeWitness,
        kind: ContextBatchKind,
        expected_items: usize,
    ) !void {
        const state = try self.scopeState(witness);
        if (state.batch != null or expected_items == 0) return error.InvalidContextBatchScope;
        if (kind == .attention_matching_singleton and expected_items != 1)
            return error.InvalidContextBatchScope;
        state.batch = .{ .kind = kind, .expected_items = expected_items };
    }

    pub fn completeBatchItem(
        self: *RequestAllocationOwner,
        witness: ContextScopeWitness,
    ) !void {
        const state = try self.scopeState(witness);
        const batch = if (state.batch) |*value| value else return error.InvalidContextBatchScope;
        if (state.invalidated or batch.completed_items >= batch.expected_items)
            return error.InvalidContextBatchScope;
        batch.completed_items += 1;
    }

    /// Mark a group receipt unusable after cancellation, admission failure, or
    /// an item execution failure. Cleanup still uses the ordinary release path
    /// and must return to the exact baseline.
    pub fn invalidateContextScope(
        self: *RequestAllocationOwner,
        witness: ContextScopeWitness,
    ) !void {
        const state = try self.scopeState(witness);
        state.invalidated = true;
    }

    /// Capture the drained, still-committed context state. Call only after all
    /// backend queues that can retain KV generations have completed and before
    /// destroying the KV manager/storage/session and prepared payload.
    pub fn captureCommitted(self: *RequestAllocationOwner) Snapshot {
        const value = self.tracker.snapshot();
        self.committed = value;
        return value;
    }

    /// Preserve a snapshot captured by an inner owner-scoped session immediately
    /// after its queues drained and before that session released its buffers.
    /// This is required when the serving caller regains control only after the
    /// packed session has already torn down.
    pub fn setCommittedSnapshot(self: *RequestAllocationOwner, value: Snapshot) !void {
        try self.validateCommittedSnapshot(value);
        self.committed = value;
    }

    pub fn setDrainedCommittedSnapshot(
        self: *RequestAllocationOwner,
        witness: ContextScopeWitness,
        value: Snapshot,
    ) !void {
        const state = try self.scopeState(witness);
        try self.setCommittedSnapshot(value);
        state.committed_drained = true;
    }

    /// Serial exact siblings can have different KV sizes. Preserve the
    /// largest drained live allocation while retaining the tracker's true
    /// coincident peak. Every candidate must originate from this owner.
    pub fn setDrainedCommittedSnapshotMax(
        self: *RequestAllocationOwner,
        witness: ContextScopeWitness,
        value: Snapshot,
    ) !void {
        const state = try self.scopeState(witness);
        try self.validateCommittedSnapshot(value);
        if (self.committed == null or
            value.tracked_combined_live_bytes > self.committed.?.tracked_combined_live_bytes)
            self.committed = value;
        state.committed_drained = true;
    }

    fn validateCommittedSnapshot(self: *RequestAllocationOwner, value: Snapshot) !void {
        if (!value.available or
            value.owner_identity == 0 or
            value.owner_identity != self.initial.owner_identity or
            value.logical_live_bytes < self.initial.logical_live_bytes or
            value.shared_backing_live_bytes < self.initial.shared_backing_live_bytes or
            value.private_device_live_bytes < self.initial.private_device_live_bytes or
            value.physical_available != self.initial.physical_available)
            return error.InvalidOwnerCommittedSnapshot;
    }

    fn measurement(self: *const RequestAllocationOwner, committed: Snapshot, final: Snapshot) Measurement {
        if (!self.initial.tracked_combined_physical_available or
            !committed.tracked_combined_physical_available or
            !final.tracked_combined_physical_available) return .{};
        const request_peak = @max(committed.tracked_combined_peak_bytes, final.tracked_combined_peak_bytes);
        if (committed.tracked_combined_live_bytes < self.initial.tracked_combined_live_bytes or
            request_peak < self.initial.tracked_combined_live_bytes or
            final.tracked_combined_live_bytes < self.initial.tracked_combined_live_bytes) return .{};
        return .{
            .available = true,
            .committed_live_bytes = committed.tracked_combined_live_bytes - self.initial.tracked_combined_live_bytes,
            .request_peak_bytes = request_peak - self.initial.tracked_combined_live_bytes,
            .released_baseline_bytes = final.tracked_combined_live_bytes - self.initial.tracked_combined_live_bytes,
        };
    }

    pub fn release(self: *RequestAllocationOwner) ReleaseReceipt {
        const final = self.tracker.snapshot();
        const has_committed = self.committed != null;
        const committed = self.committed orelse final;
        if (final.logical_live_bytes != self.initial.logical_live_bytes or
            final.shared_backing_live_bytes != self.initial.shared_backing_live_bytes or
            final.private_device_live_bytes != self.initial.private_device_live_bytes or
            (final.physical_available and final.physical_live_bytes != self.initial.physical_live_bytes))
            @panic("context request owner released with live allocations");
        const measured = if (has_committed) self.measurement(committed, final) else Measurement{};
        // The owner retains scope progress so every error-path defer can use
        // this same release. Missing stages fail closed without requiring the
        // caller to preserve or select a special release function.
        const scope_complete = if (self.context_scope) |state|
            !state.invalidated and state.isolated_route_bound and state.prepared_allocator_bound and
                state.execution_pool_bound and state.typed_storage_bound and state.committed_drained and
                has_committed and (if (state.batch) |batch| batch.completed_items == batch.expected_items else true)
        else
            false;
        const full_context_physical_available = scope_complete and measured.available and
            measured.committed_live_bytes > 0 and
            measured.released_baseline_bytes != null and measured.released_baseline_bytes.? == 0;
        std.heap.c_allocator.destroy(self.tracker);
        const initial = self.initial;
        const execution_pool_admission_version = if (self.context_scope) |state|
            state.execution_pool_admission_version
        else
            0;
        const execution_pool_admitted_backend_bytes: u64 = if (self.context_scope) |state|
            @intCast(state.execution_pool_admitted_backend_bytes)
        else
            0;
        const batch = if (self.context_scope) |state| state.batch else null;
        self.* = undefined;
        return .{
            .released = true,
            .context_allocation_scope_complete = scope_complete,
            .context_batch_scope_version = if (batch != null) context_batch_allocation_scope_schema_version else 0,
            .context_batch_kind = if (batch) |value| value.kind else null,
            .context_batch_expected_items = if (batch) |value| @intCast(value.expected_items) else 0,
            .context_batch_completed_items = if (batch) |value| @intCast(value.completed_items) else 0,
            .model_execution_pool_admission_version = execution_pool_admission_version,
            .model_execution_pool_admitted_backend_bytes = execution_pool_admitted_backend_bytes,
            .full_owner_physical_available = full_context_physical_available,
            .initial = initial,
            .committed = committed,
            .final = final,
            .measurement = measured,
            .allocation = final,
            .post_release_live_bytes = if (final.tracked_combined_physical_available) 0 else null,
        };
    }
};

test "context host allocation tracker starts and returns to zero" {
    var tracker = HostAllocationTracker.init(null, 0);
    const allocator = tracker.allocator();
    const initial = tracker.snapshot();
    try std.testing.expectEqual(@as(u64, 0), initial.logical_live_bytes);
    try std.testing.expectEqual(@as(u64, 0), initial.physical_live_bytes);
    const bytes = try allocator.alloc(u8, 37);
    try std.testing.expectEqual(@as(u64, 37), tracker.snapshot().logical_live_bytes);
    allocator.free(bytes);
    const final = tracker.snapshot();
    try std.testing.expectEqual(@as(u64, 0), final.logical_live_bytes);
    try std.testing.expectEqual(@as(u64, 0), final.physical_live_bytes);
}

test "context host allocation tracker observes realloc overlap peak" {
    var tracker = HostAllocationTracker.init(null, 0);
    const allocator = tracker.allocator();
    var bytes = try allocator.alloc(u8, 32);
    bytes = try allocator.realloc(bytes, 8192);
    const snapshot = tracker.snapshot();
    try std.testing.expectEqual(@as(u64, 8192), snapshot.logical_live_bytes);
    try std.testing.expect(snapshot.logical_peak_bytes >= 8224);
    allocator.free(bytes);
}

test "context owner tracker records exact combined host and shared peak" {
    var tracker = HostAllocationTracker.init(null, 64);
    const allocator = tracker.allocator();
    const host = try allocator.alloc(u8, 100);
    tracker.beginSharedMutation();
    try tracker.recordSharedAllocLocked(4096);
    tracker.endSharedMutation();
    const together = tracker.snapshot();
    try std.testing.expectEqual(@as(u64, 4096), together.shared_backing_live_bytes);
    if (together.physical_available)
        try std.testing.expectEqual(together.physical_live_bytes + 4096, together.tracked_combined_peak_bytes);
    allocator.free(host);
    tracker.beginSharedMutation();
    tracker.recordSharedFreeLocked(4096);
    tracker.endSharedMutation();
    const final = tracker.snapshot();
    try std.testing.expectEqual(@as(u64, 0), final.logical_live_bytes);
    try std.testing.expectEqual(@as(u64, if (final.physical_available) 64 else 0), final.physical_live_bytes);
    try std.testing.expectEqual(@as(u64, 0), final.shared_backing_live_bytes);
}

test "context owner tracker records private-device replacement overlap exactly once" {
    var tracker = HostAllocationTracker.init(null, 0);
    try tracker.recordDeviceTransition(.private_device, 0, 4096, 4096);
    try tracker.recordDeviceTransition(.private_device, 4096, 8192, 8192);
    const grown = tracker.snapshot();
    try std.testing.expectEqual(@as(u64, 8192), grown.private_device_live_bytes);
    try std.testing.expectEqual(@as(u64, 12288), grown.private_device_peak_bytes);
    try std.testing.expectEqual(@as(u64, 12288), grown.tracked_combined_peak_bytes);
    try tracker.reconcileDeviceLive(.private_device, 8192, 0);
    try std.testing.expectEqual(@as(u64, 0), tracker.snapshot().private_device_live_bytes);
}

test "context owner tracker never double-counts shared backing as private device" {
    var tracker = HostAllocationTracker.init(null, 0);
    try tracker.recordDeviceTransition(.shared_no_copy, 0, 4096, 4096);
    const shared = tracker.snapshot();
    try std.testing.expectEqual(@as(u64, 4096), shared.shared_backing_live_bytes);
    try std.testing.expectEqual(@as(u64, 0), shared.private_device_live_bytes);
    try std.testing.expectEqual(@as(u64, 4096), shared.tracked_combined_live_bytes);
    try tracker.reconcileDeviceLive(.shared_no_copy, 4096, 0);
}

test "context owner tracker failed device transition leaves counters unchanged" {
    var tracker = HostAllocationTracker.init(null, 0);
    try std.testing.expectError(
        error.InvalidOwnerAllocationTransition,
        tracker.recordDeviceTransition(.private_device, 1, 4096, 4096),
    );
    const snapshot = tracker.snapshot();
    try std.testing.expectEqual(@as(u64, 0), snapshot.private_device_live_bytes);
    try std.testing.expectEqual(@as(u64, 0), snapshot.private_device_peak_bytes);
}

test "context owner tracker overflow rolls back without mutating the ledger" {
    var tracker = HostAllocationTracker.init(null, 0);
    tracker.private_device_live_bytes = std.math.maxInt(usize) - 3;
    tracker.private_device_peak_bytes = tracker.private_device_live_bytes;
    tracker.total_peak_bytes = tracker.private_device_live_bytes;
    const before = tracker.snapshot();
    try std.testing.expectError(
        error.Overflow,
        tracker.recordDeviceTransition(.private_device, tracker.private_device_live_bytes, 8, 8),
    );
    const after = tracker.snapshot();
    try std.testing.expectEqual(before.private_device_live_bytes, after.private_device_live_bytes);
    try std.testing.expectEqual(before.private_device_peak_bytes, after.private_device_peak_bytes);
    try std.testing.expectEqual(before.tracked_combined_peak_bytes, after.tracked_combined_peak_bytes);
}

test "context owner tracker cache lease attach and release is exclusive only while held" {
    var tracker = HostAllocationTracker.init(null, 0);
    // The model runtime already owns this cached allocation; leasing it makes
    // the backing exclusive without claiming a new allocation event.
    try tracker.recordDeviceTransition(.private_device, 0, 0, 16384);
    try std.testing.expectEqual(@as(u64, 16384), tracker.snapshot().private_device_live_bytes);
    try tracker.reconcileDeviceLive(.private_device, 16384, 0);
    try std.testing.expectEqual(@as(u64, 0), tracker.snapshot().private_device_live_bytes);
    // Reusing the cache again must not accumulate or double-count residency.
    try tracker.recordDeviceTransition(.private_device, 0, 0, 16384);
    const reused = tracker.snapshot();
    try std.testing.expectEqual(@as(u64, 16384), reused.private_device_live_bytes);
    try std.testing.expectEqual(@as(u64, 16384), reused.private_device_peak_bytes);
    try tracker.reconcileDeviceLive(.private_device, 16384, 0);
}

test "context host allocation tracker records in-place resize" {
    var tracker = HostAllocationTracker.init(null, 0);
    const allocator = tracker.allocator();
    var bytes = try allocator.alloc(u8, 128);
    try std.testing.expect(allocator.resize(bytes, 31));
    bytes = bytes[0..31];
    const snapshot = tracker.snapshot();
    try std.testing.expectEqual(@as(u64, 31), snapshot.logical_live_bytes);
    try std.testing.expectEqual(@as(u64, 128), snapshot.logical_peak_bytes);
    allocator.free(bytes);
}

test "context host allocation tracker enforces logical limit without fallback" {
    var tracker = HostAllocationTracker.init(64, 0);
    const allocator = tracker.allocator();
    const first = try allocator.alloc(u8, 48);
    try std.testing.expectError(error.OutOfMemory, allocator.alloc(u8, 17));
    allocator.free(first);
    try std.testing.expectEqual(@as(u64, 0), tracker.snapshot().logical_live_bytes);
}

test "context host allocation tracker supports overaligned allocations" {
    var tracker = HostAllocationTracker.init(null, 0);
    const allocator = tracker.allocator();
    const bytes = try allocator.alignedAlloc(u8, .@"64", 97);
    try std.testing.expectEqual(@as(usize, 0), @intFromPtr(bytes.ptr) % 64);
    try std.testing.expectEqual(@as(u64, 97), tracker.snapshot().logical_live_bytes);
    allocator.free(bytes);
}

test "context request allocation owner spans preparation and borrowed phases" {
    var owner = try RequestAllocationOwner.init(null);
    const allocator = owner.allocator();
    const prepared = try allocator.alloc(u8, 31);
    const preparation = owner.tracker.snapshot();
    const session = try allocator.alloc(u8, 4097);
    const active = owner.tracker.snapshot();
    try std.testing.expect(active.logical_live_bytes > preparation.logical_live_bytes);
    allocator.free(session);
    try std.testing.expectEqual(preparation.logical_live_bytes, owner.tracker.snapshot().logical_live_bytes);
    allocator.free(prepared);
    const receipt = owner.release();
    try std.testing.expect(receipt.released);
    try std.testing.expect(!receipt.full_owner_physical_available);
    try std.testing.expect(!receipt.measurement.available);
    if (receipt.allocation.tracked_combined_physical_available)
        try std.testing.expectEqual(@as(?u64, 0), receipt.post_release_live_bytes);
}

test "context request owner keeps committed peak distinct from released baseline" {
    var owner = try RequestAllocationOwner.init(null);
    const metadata = try owner.allocator().alloc(u8, 257);
    try owner.tracker.recordDeviceTransition(.private_device, 0, 8192, 8192);
    const committed = owner.captureCommitted();
    try std.testing.expect(committed.private_device_live_bytes == 8192);
    try owner.tracker.reconcileDeviceLive(.private_device, 8192, 0);
    owner.allocator().free(metadata);
    const receipt = owner.release();
    try std.testing.expect(receipt.measurement.available);
    try std.testing.expect(receipt.measurement.committed_live_bytes >= 8192);
    try std.testing.expect(receipt.measurement.request_peak_bytes >= receipt.measurement.committed_live_bytes);
    try std.testing.expectEqual(@as(u64, 0), receipt.measurement.released_baseline_bytes.?);
    try std.testing.expect(!receipt.full_owner_physical_available);
}

test "context request owner rejects an invalid inner committed snapshot" {
    var owner = try RequestAllocationOwner.init(null);
    try std.testing.expectError(error.InvalidOwnerCommittedSnapshot, owner.setCommittedSnapshot(.{}));
    const receipt = owner.release();
    try std.testing.expect(receipt.released);
}

test "context request owner rejects a committed snapshot from another owner" {
    var first = try RequestAllocationOwner.init(null);
    const foreign = first.tracker.snapshot();
    const first_receipt = first.release();
    try std.testing.expect(first_receipt.released);

    var second = try RequestAllocationOwner.init(null);
    try std.testing.expectError(error.InvalidOwnerCommittedSnapshot, second.setCommittedSnapshot(foreign));
    const second_receipt = second.release();
    try std.testing.expect(second_receipt.released);
    try std.testing.expect(!second_receipt.measurement.available);
}

test "context request owner preserves drained inner session snapshot through teardown" {
    var owner = try RequestAllocationOwner.init(null);
    const session = try owner.allocator().alloc(u8, 4097);
    try owner.tracker.recordDeviceTransition(.private_device, 0, 16384, 16384);
    const drained_while_live = owner.tracker.snapshot();
    try owner.setCommittedSnapshot(drained_while_live);
    try owner.tracker.reconcileDeviceLive(.private_device, 16384, 0);
    owner.allocator().free(session);
    const receipt = owner.release();
    try std.testing.expect(receipt.measurement.available);
    try std.testing.expect(receipt.measurement.committed_live_bytes > 0);
    try std.testing.expectEqual(@as(u64, 0), receipt.measurement.released_baseline_bytes.?);
    try std.testing.expectEqual(drained_while_live.tracked_combined_live_bytes, receipt.committed.tracked_combined_live_bytes);
}

test "context request owner cancellation cleanup returns to baseline" {
    var owner = try RequestAllocationOwner.init(512);
    const first = try owner.allocator().alloc(u8, 384);
    try std.testing.expectError(error.OutOfMemory, owner.allocator().alloc(u8, 256));
    try owner.tracker.recordDeviceTransition(.private_device, 0, 4096, 4096);
    // Model a cancellation drain followed by KV lease release.
    try owner.tracker.reconcileDeviceLive(.private_device, 4096, 0);
    owner.allocator().free(first);
    _ = owner.captureCommitted();
    const receipt = owner.release();
    try std.testing.expect(receipt.released);
    try std.testing.expectEqual(receipt.initial.tracked_combined_live_bytes, receipt.final.tracked_combined_live_bytes);
    try std.testing.expect(!receipt.full_owner_physical_available);
}

test "context host allocation tracker exposes no fake physical fallback" {
    var tracker = HostAllocationTracker.init(null, 0);
    const allocator = tracker.allocator();
    const bytes = try allocator.alloc(u8, 19);
    defer allocator.free(bytes);
    const snapshot = tracker.snapshot();
    if (comptime HostAllocationTracker.physicalAccountingAvailable()) {
        try std.testing.expect(snapshot.available and snapshot.physical_available);
        try std.testing.expect(snapshot.physical_live_bytes >= snapshot.logical_live_bytes);
    } else {
        try std.testing.expect(snapshot.available);
        try std.testing.expect(!snapshot.physical_available);
        try std.testing.expectEqual(@as(u64, 0), snapshot.physical_live_bytes);
        try std.testing.expectEqual(@as(u64, 0), snapshot.physical_peak_bytes);
    }
}

test "context scope witness requires every typed binding and drained committed snapshot" {
    if (comptime !HostAllocationTracker.physicalAccountingAvailable()) return error.SkipZigTest;
    var owner = try RequestAllocationOwner.init(null);
    const witness = try owner.beginContextScope(41);
    try owner.bindIsolatedRoute(witness, .{ .metal = true, .f16_kv = true, .prompt_cache_disabled = true, .media_disabled = true, .speculation_disabled = true, .draft_disabled = true, .legacy_qualification_disabled = true, .retained_prepared_payload = true });
    try owner.bindPreparedAllocator(witness, owner.allocator());
    try owner.bindExecutionPool(witness, 41, 1, 64 * 1024 * 1024);
    try owner.bindTypedStorage(witness, owner.tracker);
    const bytes = try owner.allocator().alloc(u8, 17);
    try owner.setDrainedCommittedSnapshot(witness, owner.tracker.snapshot());
    owner.allocator().free(bytes);
    const receipt = owner.release();
    try std.testing.expect(receipt.context_allocation_scope_complete);
    try std.testing.expectEqual(@as(u32, 1), receipt.context_allocation_scope_version);
    try std.testing.expectEqual(@as(u32, 1), receipt.model_execution_pool_admission_version);
    try std.testing.expectEqual(@as(u64, 64 * 1024 * 1024), receipt.model_execution_pool_admitted_backend_bytes);
    try std.testing.expect(receipt.full_owner_physical_available);
    try std.testing.expect(receipt.measurement.committed_live_bytes > 0);
    try std.testing.expectEqual(@as(?u64, 0), receipt.measurement.released_baseline_bytes);
}

test "context scope witness fails closed for missing binding and stale identity" {
    if (comptime !HostAllocationTracker.physicalAccountingAvailable()) return error.SkipZigTest;
    var owner = try RequestAllocationOwner.init(null);
    const witness = try owner.beginContextScope(9);
    var stale = witness;
    stale.owner_identity +%= 1;
    try std.testing.expectError(error.StaleContextOwnerScope, owner.bindPreparedAllocator(stale, owner.allocator()));
    stale = witness;
    stale.load_generation +%= 1;
    try std.testing.expectError(error.StaleContextOwnerScope, owner.bindPreparedAllocator(stale, owner.allocator()));
    try owner.bindPreparedAllocator(witness, owner.allocator());
    const bytes = try owner.allocator().alloc(u8, 17);
    try owner.setDrainedCommittedSnapshot(witness, owner.tracker.snapshot());
    owner.allocator().free(bytes);
    // Missing execution-pool and typed-storage proofs keep legacy evidence
    // diagnostic even though committed and released snapshots are otherwise valid.
    const receipt = owner.release();
    try std.testing.expect(!receipt.context_allocation_scope_complete);
    try std.testing.expect(!receipt.full_owner_physical_available);
}

test "foreign context scope witness cannot bind another owner" {
    if (comptime !HostAllocationTracker.physicalAccountingAvailable()) return error.SkipZigTest;
    var first = try RequestAllocationOwner.init(null);
    defer _ = first.release();
    const witness = try first.beginContextScope(7);
    var second = try RequestAllocationOwner.init(null);
    defer _ = second.release();
    _ = try second.beginContextScope(7);
    try std.testing.expectError(error.StaleContextOwnerScope, second.bindPreparedAllocator(witness, second.allocator()));
}

test "each missing context scope stage independently fails closed" {
    if (comptime !HostAllocationTracker.physicalAccountingAvailable()) return error.SkipZigTest;
    const Stage = enum { route, allocator, execution, storage, committed };
    inline for (std.meta.tags(Stage)) |missing| {
        var owner = try RequestAllocationOwner.init(null);
        const witness = try owner.beginContextScope(17);
        if (missing != .route) try owner.bindIsolatedRoute(witness, .{ .metal = true, .f16_kv = true, .prompt_cache_disabled = true, .media_disabled = true, .speculation_disabled = true, .draft_disabled = true, .legacy_qualification_disabled = true, .retained_prepared_payload = true });
        if (missing != .allocator) try owner.bindPreparedAllocator(witness, owner.allocator());
        if (missing != .execution) try owner.bindExecutionPool(witness, 17, 1, 64 * 1024 * 1024);
        if (missing != .storage) try owner.bindTypedStorage(witness, owner.tracker);
        const bytes = try owner.allocator().alloc(u8, 17);
        if (missing != .committed) try owner.setDrainedCommittedSnapshot(witness, owner.tracker.snapshot());
        owner.allocator().free(bytes);
        const receipt = owner.release();
        try std.testing.expect(!receipt.context_allocation_scope_complete);
        try std.testing.expect(!receipt.full_owner_physical_available);
    }
}

test "context exact sibling batch preserves largest committed snapshot once" {
    if (comptime !HostAllocationTracker.physicalAccountingAvailable()) return error.SkipZigTest;
    var owner = try RequestAllocationOwner.init(null);
    const witness = try owner.beginContextScope(51);
    try owner.bindIsolatedRoute(witness, .{ .metal = true, .f16_kv = true, .prompt_cache_disabled = true, .media_disabled = true, .speculation_disabled = true, .draft_disabled = true, .legacy_qualification_disabled = true, .retained_prepared_payload = true });
    try owner.bindPreparedAllocator(witness, owner.allocator());
    try owner.bindExecutionPool(witness, 51, 1, 64 * 1024 * 1024);
    try owner.bindTypedStorage(witness, owner.tracker);
    try owner.bindBatchGroup(witness, .exact_siblings, 2);

    const first = try owner.allocator().alloc(u8, 17);
    const first_snapshot = owner.tracker.snapshot();
    try owner.setDrainedCommittedSnapshotMax(witness, first_snapshot);
    try owner.completeBatchItem(witness);
    const second = try owner.allocator().alloc(u8, 129);
    const second_snapshot = owner.tracker.snapshot();
    try owner.setDrainedCommittedSnapshotMax(witness, second_snapshot);
    try owner.completeBatchItem(witness);
    owner.allocator().free(second);
    owner.allocator().free(first);

    const receipt = owner.release();
    try std.testing.expect(receipt.context_allocation_scope_complete);
    try std.testing.expect(receipt.full_owner_physical_available);
    try std.testing.expectEqual(context_batch_allocation_scope_schema_version, receipt.context_batch_scope_version);
    try std.testing.expectEqual(ContextBatchKind.exact_siblings, receipt.context_batch_kind.?);
    try std.testing.expectEqual(@as(u64, 2), receipt.context_batch_expected_items);
    try std.testing.expectEqual(@as(u64, 2), receipt.context_batch_completed_items);
    try std.testing.expectEqual(second_snapshot.tracked_combined_live_bytes, receipt.committed.tracked_combined_live_bytes);
}

test "context attention matching batch is singleton and failure stays incomplete" {
    if (comptime !HostAllocationTracker.physicalAccountingAvailable()) return error.SkipZigTest;
    var owner = try RequestAllocationOwner.init(null);
    const witness = try owner.beginContextScope(52);
    try std.testing.expectError(error.InvalidContextBatchScope, owner.bindBatchGroup(witness, .attention_matching_singleton, 2));
    try owner.bindBatchGroup(witness, .attention_matching_singleton, 1);
    try owner.invalidateContextScope(witness);
    const receipt = owner.release();
    try std.testing.expect(!receipt.context_allocation_scope_complete);
    try std.testing.expect(!receipt.full_owner_physical_available);
    try std.testing.expectEqual(ContextBatchKind.attention_matching_singleton, receipt.context_batch_kind.?);
    try std.testing.expectEqual(@as(u64, 0), receipt.context_batch_completed_items);
}

test "context batch rejects foreign committed snapshot and missing completion" {
    if (comptime !HostAllocationTracker.physicalAccountingAvailable()) return error.SkipZigTest;
    var first = try RequestAllocationOwner.init(null);
    const witness = try first.beginContextScope(53);
    try first.bindIsolatedRoute(witness, .{ .metal = true, .f16_kv = true, .prompt_cache_disabled = true, .media_disabled = true, .speculation_disabled = true, .draft_disabled = true, .legacy_qualification_disabled = true, .retained_prepared_payload = true });
    try first.bindPreparedAllocator(witness, first.allocator());
    try first.bindExecutionPool(witness, 53, 1, 64 * 1024 * 1024);
    try first.bindTypedStorage(witness, first.tracker);
    try first.bindBatchGroup(witness, .exact_siblings, 1);
    var second = try RequestAllocationOwner.init(null);
    defer _ = second.release();
    try std.testing.expectError(
        error.InvalidOwnerCommittedSnapshot,
        first.setDrainedCommittedSnapshotMax(witness, second.tracker.snapshot()),
    );
    const bytes = try first.allocator().alloc(u8, 17);
    try first.setDrainedCommittedSnapshotMax(witness, first.tracker.snapshot());
    first.allocator().free(bytes);
    const receipt = first.release();
    try std.testing.expect(!receipt.context_allocation_scope_complete);
    try std.testing.expect(!receipt.full_owner_physical_available);
}

test "context request-local allocation fault preserves existing allocations and cleanup" {
    var tracker = HostAllocationTracker.init(null, 0);
    const allocator = tracker.allocator();
    const existing = try allocator.alloc(u8, 37);
    try tracker.armNextPositiveAllocationFailure();
    try std.testing.expectError(error.OutOfMemory, allocator.alloc(u8, 1));
    allocator.free(existing);
    try std.testing.expectEqual(@as(u64, 0), tracker.snapshot().logical_live_bytes);
}
