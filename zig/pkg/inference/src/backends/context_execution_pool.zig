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

//! One generation-pinned Metal context execution lease.
//!
//! This object owns only reusable command queues/runtimes and compiled
//! pipelines. Request KV, dummy/status words, descriptor arenas, split scratch,
//! replay tail/query pools and fitter staging remain request-owned and must be
//! charged by the request allocation tracker.
//!
//! The pool is deliberately bounded to one lease with no overflow creation.
//! A LoadedModel embeds one Pool and retains its separate admission lease until
//! Pool.deinit. A ModelHandle must remain pinned through Lease.release.
const std = @import("std");
const mr = @import("metal_runtime.zig");
const shared = @import("metal_generated_kernels.zig");
const flash = @import("context_flash.zig");
const owner_alloc = @import("../runtime/kv/host_allocation_tracker.zig");

pub const Source = enum { storage, attention, replay, fitter, native_fitter };
/// Conservative admission policy, not measured driver residency. Receipts
/// disclose this shared model cost independently of request context savings.
pub const admission_version: u32 = 2;
pub const admitted_backend_bytes: usize = 80 * 1024 * 1024;

/// Model-resident execution resources. Execution owns runtime+pipeline;
/// `shared.Kernels.initBorrowedTracked` creates fresh request-local mutable state and
/// a tracked dummy buffer around these immutable/exclusive resources.
pub const Bundle = struct {
    generation: u64,
    storage: shared.Execution,
    attention: shared.Execution,
    replay: shared.Execution,
    fitter: shared.Execution,
    native_fitter: shared.Execution,
    flash_prefill: flash.Execution,

    fn init(generation: u64) !Bundle {
        if (generation == 0) return error.UnpublishedModelGeneration;
        var storage = try shared.Execution.initSource(@embedFile("context_storage.metal"), "context_storage");
        errdefer storage.deinit();
        var attention = try shared.Execution.initSource(@embedFile("context_attention.metal"), "context_attention");
        errdefer attention.deinit();
        var replay = try shared.Execution.initSource(@embedFile("context_replay.metal"), "context_replay");
        errdefer replay.deinit();
        var fitter = try shared.Execution.initSource(@embedFile("context_compaction.metal"), "context_compaction");
        errdefer fitter.deinit();
        var native_fitter = try shared.Execution.initSource(@embedFile("context_attention_match.metal"), "context_attention_match");
        errdefer native_fitter.deinit();
        return .{
            .generation = generation,
            .storage = storage,
            .attention = attention,
            .replay = replay,
            .fitter = fitter,
            .native_fitter = native_fitter,
            .flash_prefill = try flash.Execution.init(),
        };
    }

    fn execution(self: *Bundle, source: Source) *shared.Execution {
        return switch (source) {
            .storage => &self.storage,
            .attention => &self.attention,
            .replay => &self.replay,
            .fitter => &self.fitter,
            .native_fitter => &self.native_fitter,
        };
    }

    /// Drain actual C runtime state, rather than trusting a request wrapper's
    /// local batch flags. Once a command buffer was committed, wait it even if
    /// it completed with an error so retained resources cannot escape a lease.
    fn drain(self: *Bundle) !void {
        var failed = false;
        inline for (.{ Source.storage, Source.attention, Source.replay, Source.fitter, Source.native_fitter }) |source| {
            const runtime = self.execution(source).runtime;
            if (mr.hasActiveFrame(runtime)) mr.cancelFrame(runtime) catch {
                failed = true;
            };
            // Attempt every committed wait even if cancellation of a different
            // runtime failed; no other queue may retain request buffers.
            if (mr.hasSubmittedFrame(runtime)) mr.waitFrame(runtime) catch {
                failed = true;
            };
            if (mr.hasActiveFrame(runtime) or mr.hasSubmittedFrame(runtime)) failed = true;
        }
        if (failed) return error.ContextExecutionDrainFailed;
    }

    fn deinit(self: *Bundle) void {
        // LoadedModel destruction is only legal after the last request handle
        // and lease. Still drain defensively so driver-retained frame resources
        // never outlive the execution objects.
        // A command failure can be fully drained. Preserve the poisoned state
        // at the pool boundary without treating terminal GPU status as a leak.
        self.drain() catch {};
        inline for (.{ Source.storage, Source.attention, Source.replay, Source.fitter, Source.native_fitter }) |source| {
            const runtime = self.execution(source).runtime;
            if (mr.hasActiveFrame(runtime) or mr.hasSubmittedFrame(runtime))
                @panic("context execution bundle retained GPU work during destruction");
        }
        self.flash_prefill.deinit();
        self.native_fitter.deinit();
        self.fitter.deinit();
        self.replay.deinit();
        self.attention.deinit();
        self.storage.deinit();
        self.* = undefined;
    }
};

pub const Residency = struct {
    /// Versioned conservative admission charged before Bundle.init. This is a
    /// model-resident backend-scratch amount, not request context bytes.
    admitted_backend_bytes: usize,
    runtime_count: u8 = 5,
    pipeline_count: u8 = 7,
    /// Diagnostic only. MTLDevice.currentAllocatedSize is process/device wide,
    /// so the creation delta is never substituted for the admitted amount.
    creation_device_delta_bytes: ?u64 = null,
};

pub const Pool = struct {
    mutex: std.atomic.Mutex = .unlocked,
    bundle: ?Bundle = null,
    generation: u64,
    residency: Residency,
    // Atomic invalidation prevents a stale copied lease from unlocking a
    // subsequent request's lane. A live Lease remains a single-owner value;
    // sharing execution operations concurrently is not supported.
    active_nonce: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    drained_nonce: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    /// Packed attention also borrows the model decoder's active command
    /// buffer. Keep its owner alive through request teardown and drain it
    /// before any descriptor, status, or no-copy backing is released.
    external_runtime: ?*mr.RawMetalDecodeRuntime = null,
    next_nonce: u64 = 1,
    poisoned: bool = false,

    pub fn init(generation: u64, reservation_bytes: usize) !Pool {
        if (generation == 0 or reservation_bytes == 0) return error.InvalidContextExecutionAdmission;
        return .{
            .generation = generation,
            .residency = .{ .admitted_backend_bytes = reservation_bytes },
        };
    }

    /// Called only after the LoadedModel has acquired and retained the separate
    /// model-residency AdmissionLease. Lazy construction avoids startup cost for
    /// models that never opt into packed context, while the retained admission
    /// prevents hiding that fixed cost from later requests.
    pub fn warm(self: *Pool, device_bytes_before: ?u64) !void {
        // The LoadedModel owner mutex serializes first construction. Do not
        // wait behind a request lease here; a caller can retry cooperatively.
        if (!self.mutex.tryLock()) return error.ContextExecutionPoolBusy;
        defer self.mutex.unlock();
        if (self.poisoned) return error.ContextExecutionPoolPoisoned;
        if (self.bundle != null) return;
        var bundle = try Bundle.init(self.generation);
        errdefer bundle.deinit();
        const after = mr.currentAllocatedSize(bundle.storage.runtime);
        self.residency.creation_device_delta_bytes = if (device_bytes_before) |before|
            (if (after >= before) after - before else null)
        else
            null;
        self.bundle = bundle;
    }

    pub fn acquire(self: *Pool, generation: u64) !Lease {
        if (!self.mutex.tryLock()) return error.ContextExecutionPoolBusy;
        errdefer self.mutex.unlock();
        if (generation == 0 or generation != self.generation) return error.StaleModelGeneration;
        if (self.poisoned) return error.ContextExecutionPoolPoisoned;
        if (self.active_nonce.load(.acquire) != 0) return error.ContextExecutionPoolBusy;
        const bundle = if (self.bundle) |*value| value else return error.ContextExecutionPoolNotWarm;
        bundle.drain() catch |err| {
            self.poisoned = true;
            return err;
        };
        const nonce = self.next_nonce;
        if (nonce == 0) return error.ContextExecutionLeaseExhausted;
        self.next_nonce = if (nonce == std.math.maxInt(u64)) 0 else nonce + 1;
        self.active_nonce.store(nonce, .release);
        self.drained_nonce.store(0, .release);
        // Keep the mutex locked for the lease lifetime. There is one bounded
        // lane and no overflow allocation or request cross-talk.
        return .{ .pool = self, .bundle = bundle, .generation = generation, .nonce = nonce };
    }

    pub fn deinit(self: *Pool) void {
        // Last ModelHandle ownership makes contention impossible here. Do not
        // spin forever in ReleaseFast if that lifetime contract is violated.
        if (!self.mutex.tryLock()) @panic("destroying leased context execution pool");
        if (self.active_nonce.load(.acquire) != 0) @panic("destroying active context execution lease");
        if (self.external_runtime != null) @panic("destroying context pool with external frame owner");
        if (self.bundle) |*bundle| bundle.deinit();
        self.bundle = null;
        self.mutex.unlock();
        self.* = undefined;
    }
};

pub const Lease = struct {
    pool: ?*Pool,
    bundle: *Bundle,
    generation: u64,
    nonce: u64,

    /// Bind this active, generation-pinned lease to the context accounting
    /// witness. Pool residency remains separately disclosed and is not added
    /// to request context bytes.
    pub fn bindContextOwner(
        self: *Lease,
        owner: *owner_alloc.RequestAllocationOwner,
        witness: owner_alloc.RequestAllocationOwner.ContextScopeWitness,
    ) !void {
        const pool = self.pool orelse return error.ContextExecutionLeaseReleased;
        if (pool.active_nonce.load(.acquire) != self.nonce or
            self.generation != pool.generation or self.generation != witness.load_generation)
            return error.StaleContextExecutionLease;
        try owner.bindExecutionPool(
            witness,
            pool.generation,
            admission_version,
            pool.residency.admitted_backend_bytes,
        );
    }

    pub fn execution(self: *Lease, source: Source) !*shared.Execution {
        const pool = self.pool orelse return error.ContextExecutionLeaseReleased;
        if (pool.active_nonce.load(.acquire) != self.nonce or self.generation != pool.generation or
            self.generation != self.bundle.generation) return error.StaleContextExecutionLease;
        pool.drained_nonce.store(0, .release);
        return self.bundle.execution(source);
    }

    pub fn registerExternalRuntime(self: *Lease, runtime: *mr.RawMetalDecodeRuntime) !void {
        const pool = self.pool orelse return error.ContextExecutionLeaseReleased;
        if (pool.active_nonce.load(.acquire) != self.nonce or self.generation != pool.generation)
            return error.StaleContextExecutionLease;
        if (pool.external_runtime) |previous| if (previous != runtime)
            return error.ContextExecutionFrameOwnerMismatch;
        pool.external_runtime = runtime;
        pool.drained_nonce.store(0, .release);
    }

    pub fn flashExecution(self: *Lease) !*flash.Execution {
        _ = try self.execution(.attention);
        return &self.bundle.flash_prefill;
    }

    /// Must run before any request-owned Metal buffer is freed. This drains all
    /// pool queues while the lease and request buffers are still alive.
    pub fn drainForBufferRelease(self: *Lease) !void {
        const pool = self.pool orelse return error.ContextExecutionLeaseReleased;
        if (pool.active_nonce.load(.acquire) != self.nonce or self.generation != pool.generation)
            return error.StaleContextExecutionLease;
        var external_failed = false;
        if (pool.external_runtime) |runtime| {
            if (mr.hasActiveFrame(runtime)) mr.cancelFrame(runtime) catch {
                external_failed = true;
            };
            if (mr.hasSubmittedFrame(runtime)) mr.waitFrame(runtime) catch {
                external_failed = true;
            };
            if (mr.hasActiveFrame(runtime) or mr.hasSubmittedFrame(runtime))
                @panic("context execution cannot release external frame buffers");
            pool.external_runtime = null;
        }
        const drained = self.bundle.drain();
        if (drained) |_| {} else |_| pool.poisoned = true;
        // Callers may continue teardown after a terminal GPU error, but never
        // while a command buffer still retains request-owned memory.
        inline for (.{ Source.storage, Source.attention, Source.replay, Source.fitter, Source.native_fitter }) |source| {
            const runtime = self.bundle.execution(source).runtime;
            if (mr.hasActiveFrame(runtime) or mr.hasSubmittedFrame(runtime))
                @panic("context execution cannot release buffers with pending GPU work");
        }
        pool.drained_nonce.store(self.nonce, .release);
        if (external_failed) {
            pool.poisoned = true;
            return error.ContextExecutionDrainFailed;
        }
        try drained;
    }

    fn claimRelease(self: *Lease) !?*Pool {
        const pool = self.pool orelse return null;
        if (self.nonce == 0 or self.generation != pool.generation or
            pool.active_nonce.cmpxchgStrong(self.nonce, 0, .acq_rel, .acquire) != null)
            return error.StaleContextExecutionLease;
        self.pool = null;
        return pool;
    }

    /// Request buffers and sessions must already be destroyed. A failed drain
    /// poisons the pool permanently; it is never handed to another request.
    pub fn release(self: *Lease) !void {
        if (self.pool) |pool| {
            if (pool.active_nonce.load(.acquire) != self.nonce)
                return error.StaleContextExecutionLease;
            if (pool.drained_nonce.load(.acquire) != self.nonce or pool.external_runtime != null)
                return error.ContextExecutionBuffersNotDrained;
        }
        const pool = (try self.claimRelease()) orelse return;
        const drained = self.bundle.drain();
        if (drained) |_| {} else |_| pool.poisoned = true;
        pool.mutex.unlock();
        try drained;
    }

    pub fn poisonAndRelease(self: *Lease) void {
        self.drainForBufferRelease() catch {};
        const pool = (self.claimRelease() catch return) orelse return;
        pool.poisoned = true;
        self.bundle.drain() catch {};
        pool.mutex.unlock();
    }
};

test "context execution lease release cannot unlock a subsequent owner" {
    var pool = try Pool.init(7, 1024);
    defer pool.deinit();
    // Only the release-claim state is exercised; no GPU object is accessed.
    pool.active_nonce.store(1, .release);
    var lease = Lease{ .pool = &pool, .bundle = undefined, .generation = 7, .nonce = 1 };
    var stale = lease;
    try std.testing.expect((try lease.claimRelease()).? == &pool);
    try std.testing.expect((try lease.claimRelease()) == null);
    pool.active_nonce.store(2, .release);
    try std.testing.expectError(error.StaleContextExecutionLease, stale.claimRelease());
    try std.testing.expectEqual(@as(u64, 2), pool.active_nonce.load(.acquire));
    pool.active_nonce.store(0, .release);
}

test "context execution Metal pool reuses execution with isolated request state" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    const external = mr.termite_metal_decode_runtime_create() orelse return error.MetalRuntimeUnavailable;
    defer mr.termite_metal_decode_runtime_destroy(external);
    var pool = try Pool.init(7, admitted_backend_bytes);
    defer pool.deinit();
    try std.testing.expectError(error.StaleModelGeneration, pool.acquire(8));
    try std.testing.expectError(error.ContextExecutionPoolNotWarm, pool.acquire(7));
    try pool.warm(null);
    var first = try pool.acquire(7);
    defer first.poisonAndRelease();
    try std.testing.expectError(error.ContextExecutionPoolBusy, pool.acquire(7));
    var stale = first;
    const execution = try first.execution(.storage);
    {
        var wrapper = try shared.Kernels.initBorrowedTracked(execution, null);
        defer wrapper.deinit();
        wrapper.pair_metrics.dispatches = 9;
        try std.testing.expectError(error.ContextExecutionBuffersNotDrained, first.release());
        // Pre-release drain cancels an unsubmitted frame before buffers are freed.
        try mr.beginFrame(execution.runtime);
        try first.registerExternalRuntime(external);
        try mr.beginFrame(external);
        try first.drainForBufferRelease();
        try std.testing.expect(!mr.hasActiveFrame(external) and !mr.hasSubmittedFrame(external));
    }
    try first.release();
    var second = try pool.acquire(7);
    defer second.poisonAndRelease();
    try std.testing.expectError(error.StaleContextExecutionLease, stale.release());
    try std.testing.expect((try second.execution(.storage)) == execution);
    {
        var fresh = try shared.Kernels.initBorrowedTracked(try second.execution(.storage), null);
        defer fresh.deinit();
        try std.testing.expect(fresh.cancelled == null and !fresh.batch_active and !fresh.batch_submitted);
        try std.testing.expectEqual(@as(u64, 0), fresh.pair_metrics.dispatches);
        try mr.beginFrame(execution.runtime);
        try mr.submitFrame(execution.runtime);
        try second.drainForBufferRelease();
    }
    try second.release();
    var failed = try pool.acquire(7);
    failed.poisonAndRelease();
    try std.testing.expectError(error.ContextExecutionPoolPoisoned, pool.acquire(7));
}
