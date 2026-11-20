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

//! Epoch-local owner for a bounded two-token decoder graph. The caller holds
//! mutex across preflight, real execution, and the final ID read. No borrowed
//! host page or tensor descriptor is retained by this owner.
const std = @import("std");
const context = @import("context.zig");
const driver = @import("driver.zig");
const buffer = @import("buffer.zig");
const memory = @import("../../runtime/tier/memory.zig");
const plan_mod = @import("execution_arena.zig");
const quota_mod = @import("cache_quota.zig");
const funding_mod = @import("prepared_cache.zig");
const retirement = @import("../../execution/resource_retirement.zig");
const Cleanup = @import("../../execution/cleanup_context.zig").Context;
pub const max_rows = 128;

/// Exact independently retained buffers for a captured two-column sequence.
/// Pure geometry is available to cold admission before any SDK teardown or
/// allocation. It contains no page/key/weight pointer and grants no byte credit.
pub const StorageLayout = struct {
    bank_offset: usize,
    positions_offset: usize,
    progress_offset: usize,
    metadata_bytes: usize,
    host_bytes: usize,
    head_bytes: usize,

    pub fn init(rows: usize, hidden: usize, vocab: usize, key_words: usize) !StorageLayout {
        if (rows < 2 or rows > max_rows or hidden == 0 or vocab == 0 or key_words == 0) return error.InvalidShape;
        const input_bytes = try std.math.mul(usize, try std.math.mul(usize, rows, hidden), @sizeOf(f32));
        const bank_offset = try aligned(input_bytes);
        const positions_offset = try std.math.add(usize, bank_offset, try aligned(rows * 2 * @sizeOf(u32)));
        const progress_offset = try aligned(try std.math.add(usize, positions_offset, rows * 3 * @sizeOf(u32)));
        const metadata_bytes = try std.math.add(usize, progress_offset, 256);
        const host_bytes = try std.math.add(usize, @sizeOf(plan_mod.Plan), try std.math.mul(usize, key_words, @sizeOf(u64)));
        const head_bytes = if (rows > 64) try std.math.mul(usize, try std.math.mul(usize, rows, vocab), @sizeOf(f32)) else 0;
        if (head_bytes > plan_mod.limit) return error.InvalidShape;
        return .{ .bank_offset = bank_offset, .positions_offset = positions_offset, .progress_offset = progress_offset, .metadata_bytes = metadata_bytes, .host_bytes = host_bytes, .head_bytes = head_bytes };
    }
    fn aligned(bytes: usize) !usize {
        return (try std.math.add(usize, bytes, 255)) & ~@as(usize, 255);
    }
};

/// The scheduler may rotate the same cohort between claims. Capture uses a
/// stable sequence order, while publication retains the original claim order.
pub fn canonicalOrder(sequences: []const u64, order: []usize) !void {
    if (sequences.len < 2 or sequences.len > max_rows or order.len != sequences.len) return error.InvalidShape;
    for (sequences, 0..) |sequence, i| {
        if (sequence == 0) return error.InvalidPagedKvBatch;
        order[i] = i;
        var j = i;
        while (j > 0 and sequences[order[j - 1]] > sequence) : (j -= 1) {
            order[j] = order[j - 1];
        }
        if (j > 0 and sequences[order[j - 1]] == sequence) return error.InvalidPagedKvBatch;
        order[j] = i;
    }
}

test "LightOnOCR3 CUDA graph canonical cohorts restore both token columns after scheduler rotation" {
    // The two claims name the same states in different scheduler orders.
    // Canonical decoder rows and each original caller's IDs must agree.
    const claims = [_][4]u64{ .{ 43, 7, 29, 11 }, .{ 11, 43, 7, 29 } };
    for (claims) |sequences| {
        var order: [4]usize = undefined;
        try canonicalOrder(&sequences, &order);
        var canonical: [4]u64 = undefined;
        var published: [2][4]u64 = undefined;
        for (order, 0..) |original, row| {
            canonical[row] = sequences[original];
            published[0][original] = sequences[original] * 10;
            published[1][original] = sequences[original] * 10 + 1;
        }
        try std.testing.expectEqualSlices(u64, &.{ 7, 11, 29, 43 }, &canonical);
        for (sequences, 0..) |sequence, original| {
            try std.testing.expectEqual(sequence * 10, published[0][original]);
            try std.testing.expectEqual(sequence * 10 + 1, published[1][original]);
        }
    }
    var order: [4]usize = undefined;
    try std.testing.expectError(error.InvalidPagedKvBatch, canonicalOrder(&.{ 7, 29, 7, 43 }, &order));
    try std.testing.expectError(error.InvalidPagedKvBatch, canonicalOrder(&.{ 7, 0, 29, 43 }, &order));
}
pub const Phase = enum { warm, learn, capture, replay, disabled };
/// Borrowed only through one final-address Executor phase. Full key/positions
/// remain synchronous under the held worker step; no such borrow enters Module.
pub const Descriptor = struct {
    epoch: u64,
    key: []const u64,
    positions: []const u32,
    rows: usize,
    hidden: usize,
    vocab: usize,
    future_tokens: usize,
    attention_layers: usize,
    recurrent_layers: usize,
    pub fn layout(self: Descriptor) !StorageLayout {
        const value = try StorageLayout.init(self.rows, self.hidden, self.vocab, self.key.len);
        if (self.epoch == 0 or self.positions.len != self.rows * 3) return error.InvalidShape;
        return value;
    }
    pub fn storageRequest(self: Descriptor) !funding_mod.Request {
        const value = try self.layout();
        return .{ .captured_storage = .{ .epoch = self.epoch, .key_hash = std.hash.Wyhash.hash(0, std.mem.sliceAsBytes(self.key)), .key_words = self.key.len, .rows = self.rows, .hidden = self.hidden, .vocab = self.vocab, .metadata_bytes = value.metadata_bytes, .host_bytes = value.host_bytes, .head_bytes = value.head_bytes } };
    }
};

pub const Snapshot = struct {
    invalidations: u64,
    epoch: u64,
    phase: Phase,
    metadata: usize,
    arena: usize,
    head: usize,
    graph: usize,
    pending_graph: usize,
    plan: usize,
    peak: usize,
    events: usize,
    metadata_quota: quota_mod.Quota,
    arena_quota: quota_mod.Quota,
    head_quota: quota_mod.Quota,
    pub fn hasRaw(self: Snapshot) bool {
        return self.metadata_quota == .raw or self.arena_quota == .raw or self.head_quota == .raw;
    }
};

/// Pure heap metadata retired by the CURRENT caller's unsealed B. It never
/// borrows SDK/model/Io/request-allocator state, and never stores an Epoch.
const ColdPhase = struct {
    // Production is the stateless process allocator. The private test helper
    // injects allocation failure only while its complete B lifetime is live.
    allocator: std.mem.Allocator = std.heap.c_allocator,
    node: retirement.Node = undefined,
    funding: ?*funding_mod.Funding = null,
    key: []u64 = &.{},
    plan: ?*plan_mod.Plan = null,
    upgrades: [3]?quota_mod.RawUpgrade = @splat(null),
    fn destroyOutside(self: *ColdPhase) void {
        if (self.funding) |value| value.finishOutsideExecution();
        for (&self.upgrades) |*slot| if (slot.*) |*upgrade| upgrade.deinitOutsideExecution();
        if (self.plan) |value| self.allocator.destroy(value);
        if (self.key.len != 0) self.allocator.free(self.key);
        self.allocator.destroy(self);
    }
    fn retire(raw: *anyopaque) callconv(.c) void {
        const self: *ColdPhase = @ptrCast(@alignCast(raw));
        self.destroyOutside();
    }
};

pub const GraphPhase = struct {
    descriptor: Descriptor,
    mode: enum { none, upgrade, storage, arena } = .none,
    expected_compute: usize = 0,
    old: Snapshot = undefined,
    admission: ?@import("video_cache_budget.zig").Admission = null,
    cold: ?*ColdPhase = null,
    bound: bool = false,
    pub fn initInto(self: *GraphPhase, descriptor: Descriptor) !void {
        _ = try descriptor.layout();
        self.* = .{ .descriptor = descriptor };
    }
    pub fn needsOutside(self: *const GraphPhase) bool {
        return self.mode != .none;
    }
    /// Caller has dropped EVERY model/provider/reader lane but retains the
    /// exact model lifetime and SAME worker step. All callback-capable quota,
    /// Domain and process metadata ownership is created here.
    pub fn prepareOutside(self: *GraphPhase, model: *const @import("../../backends/session.zig").Session.ModelLifetime) !void {
        return self.prepareOutsideUsing(model, std.heap.c_allocator);
    }
    fn prepareOutsideUsing(self: *GraphPhase, model: *const @import("../../backends/session.zig").Session.ModelLifetime, allocator: std.mem.Allocator) !void {
        if (self.cold != null or self.bound or self.mode == .none) return error.InvalidPreparedCacheState;
        const cold = try allocator.create(ColdPhase);
        cold.* = .{ .allocator = allocator };
        errdefer cold.destroyOutside();
        if (self.mode == .upgrade) {
            const previous = [_]quota_mod.Quota{ self.old.metadata_quota, self.old.arena_quota, self.old.head_quota };
            for (previous, 0..) |quota, index| if (quota == .raw) {
                const lease = quota.raw;
                const controller = lease.controller orelse return error.MissingResourceRetirementLease;
                const pin = model.retainAdmissionDomain(controller) orelse return error.MissingCacheRetirementDomain;
                defer pin.release();
                cold.upgrades[index] = try quota_mod.RawUpgrade.prepare(lease, &pin);
            };
        } else {
            const request = if (self.mode == .storage) try self.descriptor.storageRequest() else funding_mod.Request{ .captured_arena = .{ .epoch = self.descriptor.epoch, .key_hash = std.hash.Wyhash.hash(0, std.mem.sliceAsBytes(self.descriptor.key)), .bytes = self.old.peak, .events = self.old.events } };
            const amounts = switch (request) {
                .captured_storage => |value| try value.amounts(),
                .captured_arena => |value| value.amounts(),
                else => unreachable,
            };
            const funding = try funding_mod.Funding.createWithAllocator(allocator, .{ .requests = &.{request} });
            cold.funding = funding;
            try funding.noteMiss(0, amounts, 0);
            funding.admission = self.admission;
            funding.compute_identity = self.expected_compute;
            funding.state = .snapped;
            var pin: ?@import("../../runtime/tier/admission_domain.zig").Pin = null;
            if (self.admission) |policy| pin = model.retainAdmissionDomain(policy.controller) orelse return error.MissingCacheRetirementDomain;
            defer if (pin) |value| value.release();
            try funding.fundOutsideExecution(if (pin) |*value| value else null);
            if (self.mode == .storage) {
                cold.key = try allocator.dupe(u64, self.descriptor.key);
                cold.plan = try allocator.create(plan_mod.Plan);
                cold.plan.?.* = .{};
            }
        }
        self.cold = cold;
    }
    /// Metadata-only. Concrete CUDA owner validates full table/context,
    /// geometry/key/policy/snapshot before exposing this funding to reset.
    pub fn bind(self: *GraphPhase, current: Cleanup) !void {
        if (self.bound or self.mode == .upgrade) return error.InvalidPreparedCacheState;
        const epoch = current.boundary orelse return error.MissingCurrentCleanupEpoch;
        if (current.outside_execution or try epoch.isSealed()) return error.InvalidPreparedCacheState;
        const cold = self.cold orelse return error.InvalidPreparedCacheState;
        try cold.funding.?.bind(epoch);
        self.bound = true;
    }
    /// Call immediately after beginGraph consumed/published its tokens, before
    /// later numerical reads may park again. No persistent graph retains this
    /// phase or Funding pointer. On error the caller still owns cold metadata.
    pub fn close(self: *GraphPhase, current: Cleanup) !void {
        const cold = self.cold orelse return;
        const epoch = current.boundary orelse return error.MissingCurrentCleanupEpoch;
        if (current.outside_execution or try epoch.isSealed()) return error.InvalidPreparedCacheState;
        if (self.bound) {
            cold.funding.?.unbind();
            self.bound = false;
        }
        cold.node = .{ .context = cold, .release_fn = ColdPhase.retire };
        const boundary = try epoch.boundary();
        self.cold = null; // consume public custody before callback-capable registration
        errdefer self.cold = cold; // a rejected registration transferred no ownership
        try boundary.register(boundary.context, boundary.nonce, &cold.node);
    }
    pub fn destroyOutside(self: *GraphPhase) void {
        std.debug.assert(!self.bound);
        const cold = self.cold orelse return;
        self.cold = null;
        cold.destroyOutside();
    }
};

pub const Module = struct {
    mutex: std.atomic.Mutex = .unlocked,
    epoch_id: u64 = 0,
    captured_roots_identity: usize = 0,
    disabled_epoch: u64 = 0,
    key: []u64 = &.{},
    plan: ?*plan_mod.Plan = null,
    phase: Phase = .warm,
    metadata: buffer.DeviceBuffer = .{},
    arena: buffer.DeviceBuffer = .{},
    metadata_quota: quota_mod.Quota = .none,
    arena_quota: quota_mod.Quota = .none,
    graph: driver.CUgraphExec = null,
    pending_graph: driver.CUgraph = null,
    host_allocator: std.mem.Allocator = std.heap.c_allocator,
    rows: usize = 0,
    hidden: usize = 0,
    vocab: usize = 0,
    // Wide logits live outside the bounded temporary arena. Both dependent
    // steps reuse this independently admitted output after their argmax.
    head: buffer.DeviceBuffer = .{},
    head_quota: quota_mod.Quota = .none,
    future_tokens: usize = 0,
    expected_attention_calls: usize = 0,
    expected_ragged_packets: usize = 0,
    bank_offset: usize = 0,
    positions_offset: usize = 0,
    progress_offset: usize = 0,
    base_positions: [3 * max_rows]u32 = @splat(0),
    next_positions: [3 * max_rows]u32 = @splat(0),
    progress: u32 = 0,
    capture_step: u32 = 0,
    active: bool = false,
    captures: u64 = 0,
    replays: u64 = 0,
    wide_replays: u64 = 0,
    max_replay_rows: usize = 0,
    invalidations: u64 = 0,
    admission_denials: u64 = 0,
    weight_declines_logged: usize = 0,
    state_declines_logged: usize = 0,
    resets_logged: usize = 0,

    /// Explicit historical raw owner only. A managed cache must receive its
    /// CURRENT caller B before any SDK mutation; null is never outside proof.
    pub fn clear(self: *Module, ctx: *context.CudaContext) !void {
        try self.metadata_quota.validateLegacyRaw();
        try self.arena_quota.validateLegacyRaw();
        try self.head_quota.validateLegacyRaw();
        return self.clearWithCleanup(ctx, .{ .outside_execution = true });
    }
    pub fn validateCleanup(self: *const Module, current: Cleanup) !void {
        try self.metadata_quota.validateCleanup(current);
        try self.arena_quota.validateCleanup(current);
        try self.head_quota.validateCleanup(current);
    }
    pub fn clearWithCleanup(self: *Module, ctx: *context.CudaContext, current: Cleanup) !void {
        if (current.synchronous_outside_raw) |pending| {
            const epoch = current.boundary orelse return error.MissingCurrentCleanupEpoch;
            // Exact metadata/arena/head provenance and capacity are certified
            // together before ANY checked SDK destruction can begin.
            try pending.bindGraph(@intFromPtr(self), .{
                if (self.metadata_quota == .raw) &self.metadata_quota.raw else null,
                if (self.arena_quota == .raw) &self.arena_quota.raw else null,
                if (self.head_quota == .raw) &self.head_quota.raw else null,
            }, epoch);
        }
        try self.validateCleanup(current);
        if (self.active) return error.CapturedGraphActive;
        const live = self.head.ptr != 0 or self.metadata.ptr != 0 or self.arena.ptr != 0 or self.graph != null or self.pending_graph != null;
        if (live and (ctx.driver.fns.cuStreamIsCapturing == null or try ctx.streamCaptureActive())) return error.CapturedGraphActive;
        self.phase = .disabled;
        self.active = false;
        if (self.head.ptr != 0 or self.metadata.ptr != 0 or self.arena.ptr != 0 or self.graph != null or self.pending_graph != null) try ctx.synchronize();
        // Every failed SDK handle remains in this actual owner. Neither quota
        // nor host metadata is dropped until ALL independently retained backing
        // has checked completion/destruction. No best-effort free certifies it.
        try ctx.destroyGraphExecChecked(&self.graph);
        try ctx.destroyGraphChecked(&self.pending_graph);
        _ = try self.head.freeChecked(ctx);
        _ = try self.arena.freeChecked(ctx);
        _ = try self.metadata.freeChecked(ctx);
        if (self.plan) |plan| self.host_allocator.destroy(plan);
        self.plan = null;
        if (self.key.len != 0) self.host_allocator.free(self.key);
        self.key = &.{};
        try self.head_quota.releaseAfterSdkDeath(current);
        try self.arena_quota.releaseAfterSdkDeath(current);
        try self.metadata_quota.releaseAfterSdkDeath(current);
        self.captured_roots_identity = 0;
        self.rows = 0;
        self.hidden = 0;
        self.vocab = 0;
        self.progress = 0;
        self.invalidations += 1;
    }
    pub fn snapshot(self: *const Module) Snapshot {
        return .{ .invalidations = self.invalidations, .epoch = self.epoch_id, .phase = self.phase, .metadata = self.metadata.ptr, .arena = self.arena.ptr, .head = self.head.ptr, .graph = if (self.graph) |value| @intFromPtr(value) else 0, .pending_graph = if (self.pending_graph) |value| @intFromPtr(value) else 0, .plan = if (self.plan) |value| @intFromPtr(value) else 0, .peak = if (self.plan) |value| value.peak else 0, .events = if (self.plan) |value| value.event_count else 0, .metadata_quota = self.metadata_quota, .arena_quota = self.arena_quota, .head_quota = self.head_quota };
    }
    pub fn invalidate(self: *Module, ctx: *context.CudaContext) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        self.clear(ctx) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
    }
    pub fn matches(self: *const Module, key: []const u64, host_positions: []const u32) bool {
        return self.key.len != 0 and std.mem.eql(u64, self.key, key) and
            host_positions.len == 3 * self.rows and
            std.mem.eql(u32, self.next_positions[0..host_positions.len], host_positions);
    }
    /// Explicit legacy raw producer. Managed execution uses resetPrepared.
    pub fn reset(self: *Module, ctx: *context.CudaContext, key: []const u64, host_positions: []const u32, rows: usize, hidden: usize, vocab: usize) !void {
        const layout = try StorageLayout.init(rows, hidden, vocab, key.len);
        if (host_positions.len != rows * 3) return error.InvalidShape;
        try self.clear(ctx);
        errdefer self.clear(ctx) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
        if (ctx.video_cache_budget.admission) |policy|
            self.metadata_quota = quota_mod.Quota.fromRawAdmission(try policy.controller.tryAcquire(.gpu, policy.limits, .{ .backend_scratch_bytes = layout.metadata_bytes, .host_scratch_bytes = layout.host_bytes }, policy.check_live_memory));
        self.host_allocator = std.heap.c_allocator;
        self.key = try std.heap.c_allocator.dupe(u64, key);
        self.plan = try std.heap.c_allocator.create(plan_mod.Plan);
        self.plan.?.* = .{};
        self.metadata = try buffer.DeviceBuffer.alloc(ctx, layout.metadata_bytes);
        if (layout.head_bytes != 0) {
            if (ctx.video_cache_budget.admission) |policy|
                self.head_quota = quota_mod.Quota.fromRawAdmission(try policy.controller.tryAcquire(.gpu, policy.limits, .{ .backend_scratch_bytes = layout.head_bytes }, policy.check_live_memory));
            self.head = try buffer.DeviceBuffer.alloc(ctx, layout.head_bytes);
        }
        self.rows = rows;
        self.hidden = hidden;
        self.vocab = vocab;
        self.bank_offset = layout.bank_offset;
        self.positions_offset = layout.positions_offset;
        self.progress_offset = layout.progress_offset;
        @memcpy(self.base_positions[0..host_positions.len], host_positions);
        @memcpy(self.next_positions[0..host_positions.len], host_positions);
        self.phase = .warm;
    }
    pub fn reserveArena(self: *Module, ctx: *context.CudaContext) !void {
        if (self.arena.ptr != 0) return;
        const plan = self.plan orelse return error.InvalidState;
        if (!plan.valid or plan.peak == 0 or plan.peak > plan_mod.limit)
            return error.InvalidExecutionPlan;
        var lease: ?memory.AdmissionLease = null;
        if (ctx.video_cache_budget.admission) |policy|
            lease = try policy.controller.tryAcquire(.gpu, policy.limits, .{
                .backend_scratch_bytes = plan.peak,
            }, policy.check_live_memory);
        errdefer if (lease) |*value| value.release();
        self.arena = try buffer.DeviceBuffer.alloc(ctx, plan.peak);
        self.arena_quota = quota_mod.Quota.fromRawAdmission(lease);
    }
    pub fn resetPrepared(self: *Module, ctx: *context.CudaContext, phase: *GraphPhase) !void {
        const descriptor = phase.descriptor;
        const layout = try descriptor.layout();
        if (!phase.bound or phase.mode != .storage or self.metadata.ptr != 0 or self.head.ptr != 0 or self.arena.ptr != 0 or self.graph != null or self.pending_graph != null or self.plan != null or self.key.len != 0) return error.InvalidPreparedCacheState;
        const cold = phase.cold orelse return error.InvalidPreparedCacheState;
        if (cold.plan == null or !std.mem.eql(u64, cold.key, descriptor.key)) return error.PreparedCachePlanChanged;
        const request = try descriptor.storageRequest();
        var lease = try cold.funding.?.take(request, try request.captured_storage.amounts());
        errdefer lease.releaseAfterSdkDeath();
        var metadata = try buffer.DeviceBuffer.alloc(ctx, layout.metadata_bytes);
        errdefer _ = metadata.freeChecked(ctx) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
        var head = buffer.DeviceBuffer{};
        errdefer _ = head.freeChecked(ctx) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
        if (layout.head_bytes != 0) head = try buffer.DeviceBuffer.alloc(ctx, layout.head_bytes);
        self.metadata = metadata;
        self.head = head;
        self.metadata_quota = lease.publishQuota(); // SAME preallocated cold shell
        self.host_allocator = cold.allocator;
        self.key = cold.key;
        cold.key = &.{};
        self.plan = cold.plan;
        cold.plan = null;
        self.rows = descriptor.rows;
        self.hidden = descriptor.hidden;
        self.vocab = descriptor.vocab;
        self.bank_offset = layout.bank_offset;
        self.positions_offset = layout.positions_offset;
        self.progress_offset = layout.progress_offset;
        @memcpy(self.base_positions[0..descriptor.positions.len], descriptor.positions);
        @memcpy(self.next_positions[0..descriptor.positions.len], descriptor.positions);
        self.phase = .warm;
    }
    pub fn reserveArenaPrepared(self: *Module, ctx: *context.CudaContext, phase: *GraphPhase) !void {
        if (self.arena.ptr != 0) return;
        const plan = self.plan orelse return error.InvalidState;
        if (!phase.bound or phase.mode != .arena or !plan.valid or plan.peak == 0 or plan.peak > plan_mod.limit or plan.peak != phase.old.peak or plan.event_count != phase.old.events) return error.InvalidPreparedCacheState;
        const request = funding_mod.Request{ .captured_arena = .{ .epoch = phase.descriptor.epoch, .key_hash = std.hash.Wyhash.hash(0, std.mem.sliceAsBytes(phase.descriptor.key)), .bytes = plan.peak, .events = plan.event_count } };
        var lease = try phase.cold.?.funding.?.take(request, request.captured_arena.amounts());
        errdefer lease.releaseAfterSdkDeath();
        const arena = try buffer.DeviceBuffer.alloc(ctx, plan.peak);
        self.arena = arena;
        self.arena_quota = lease.publishQuota();
    }

    pub fn input(self: *const Module) buffer.DeviceBuffer {
        return .{ .ptr = self.metadata.ptr, .len = self.rows * self.hidden * 4, .borrowed = true };
    }
    pub fn bank(self: *const Module) buffer.DeviceBuffer {
        return .{ .ptr = self.metadata.ptr + self.bank_offset, .len = self.rows * 2 * 4, .borrowed = true };
    }
    pub fn positions(self: *const Module) buffer.DeviceBuffer {
        return .{ .ptr = self.metadata.ptr + self.positions_offset, .len = self.rows * 3 * 4, .borrowed = true };
    }
    pub fn counter(self: *const Module) buffer.DeviceBuffer {
        return .{ .ptr = self.metadata.ptr + self.progress_offset, .len = 4, .borrowed = true };
    }
    pub fn notePair(self: *Module) !void {
        const next = try std.math.add(u32, self.progress, 2);
        for (self.next_positions[0 .. self.rows * 3]) |*position|
            position.* = try std.math.add(u32, position.*, 2);
        self.progress = next;
    }
};

test "captured sequence storage layout preserves existing narrow wide and aligned physical planes" {
    const cases = [_]struct { rows: usize, hidden: usize, vocab: usize, bank: usize, positions: usize, progress: usize, bytes: usize, head: usize }{
        .{ .rows = 2, .hidden = 1024, .vocab = 8, .bank = 8192, .positions = 8448, .progress = 8704, .bytes = 8960, .head = 0 },
        .{ .rows = 64, .hidden = 1024, .vocab = 151936, .bank = 262144, .positions = 262656, .progress = 263424, .bytes = 263680, .head = 0 },
        .{ .rows = 128, .hidden = 2560, .vocab = 151936, .bank = 1310720, .positions = 1311744, .progress = 1313280, .bytes = 1313536, .head = 77791232 },
    };
    for (cases) |value| {
        const actual = try StorageLayout.init(value.rows, value.hidden, value.vocab, 24576);
        try std.testing.expectEqual(value.bank, actual.bank_offset);
        try std.testing.expectEqual(value.positions, actual.positions_offset);
        try std.testing.expectEqual(value.progress, actual.progress_offset);
        try std.testing.expectEqual(value.bytes, actual.metadata_bytes);
        try std.testing.expectEqual(value.head, actual.head_bytes);
        try std.testing.expectEqual(@sizeOf(plan_mod.Plan) + 196608, actual.host_bytes);
    }
}

test "captured sequence malformed replacement refuses before old graph or SDK mutation" {
    // Negative-only sentinels prove no context access or destruction. There
    // is no SDK/GPU endpoint and this does not qualify a numerical replay.
    var ctx: context.CudaContext = undefined;
    var owner = Module{ .phase = .replay, .graph = @ptrFromInt(1), .metadata = .{ .ptr = 256, .len = 256 } };
    try std.testing.expectError(error.InvalidShape, owner.reset(&ctx, &.{1}, &.{}, 1, 1024, 8));
    try std.testing.expectError(error.InvalidShape, owner.reset(&ctx, &.{1}, &.{}, 2, 1024, 8));
    try std.testing.expectError(error.Overflow, owner.reset(&ctx, &.{1}, &.{}, 128, std.math.maxInt(usize), 8));
    try std.testing.expectError(error.Overflow, owner.reset(&ctx, &.{1}, &.{}, 128, 1024, std.math.maxInt(usize)));
    try std.testing.expectError(error.InvalidShape, owner.reset(&ctx, &.{1}, &.{}, 128, 1024, 262145));
    try std.testing.expect(owner.phase == .replay and owner.graph == @as(driver.CUgraphExec, @ptrFromInt(1)));
    try std.testing.expectEqual(@as(u64, 256), owner.metadata.ptr);
}

const GraphTestDriver = struct {
    var next: usize = 0x1000;
    var allocations: usize = 0;
    var frees: usize = 0;
    var exec_destroys: usize = 0;
    var graph_destroys: usize = 0;
    var fail_free: bool = false;
    var fail_exec: bool = false;
    var fail_graph: bool = false;
    var fail_sync: bool = false;
    var capture_active: bool = false;
    fn current(_: driver.CUcontext) callconv(.c) driver.CUresult {
        return 0;
    }
    fn sync(_: driver.CUstream) callconv(.c) driver.CUresult {
        return if (fail_sync) 700 else 0;
    }
    fn capture(_: driver.CUstream, status: *driver.CUstreamCaptureStatus) callconv(.c) driver.CUresult {
        status.* = if (capture_active) driver.CU_STREAM_CAPTURE_STATUS_ACTIVE else driver.CU_STREAM_CAPTURE_STATUS_NONE;
        return 0;
    }
    fn alloc(out: *driver.CUdeviceptr, _: usize) callconv(.c) driver.CUresult {
        allocations += 1;
        out.* = next;
        next += 0x1000;
        return 0;
    }
    fn free(_: driver.CUdeviceptr) callconv(.c) driver.CUresult {
        if (fail_free) return 700;
        frees += 1;
        return 0;
    }
    fn destroyExec(_: driver.CUgraphExec) callconv(.c) driver.CUresult {
        if (fail_exec) return 700;
        exec_destroys += 1;
        return 0;
    }
    fn destroyGraph(_: driver.CUgraph) callconv(.c) driver.CUresult {
        if (fail_graph) return 700;
        graph_destroys += 1;
        return 0;
    }
    fn errorText(_: driver.CUresult, out: *?[*:0]const u8) callconv(.c) driver.CUresult {
        out.* = "injected graph lifecycle failure";
        return 0;
    }
    fn reset() void {
        next = 0x1000;
        allocations = 0;
        frees = 0;
        exec_destroys = 0;
        graph_destroys = 0;
        fail_free = false;
        fail_exec = false;
        fail_graph = false;
        fail_sync = false;
        capture_active = false;
    }
    fn contextValue() context.CudaContext {
        var ctx: context.CudaContext = undefined;
        ctx.ctx = @ptrFromInt(1);
        ctx.stream = @ptrFromInt(2);
        ctx.stats = .{};
        ctx.observation.store(null, .release);
        ctx.observation_enable_failed.store(false, .release);
        ctx.device_allocations = .{ .limit = 1024 * 1024 };
        ctx.prepared_cache_funding = null;
        ctx.video_cache_budget = .{};
        ctx.driver.fns.cuCtxSetCurrent = current;
        ctx.driver.fns.cuStreamSynchronize = sync;
        ctx.driver.fns.cuStreamIsCapturing = capture;
        ctx.driver.fns.cuMemAlloc = alloc;
        ctx.driver.fns.cuMemFree = free;
        ctx.driver.fns.cuGraphExecDestroy = destroyExec;
        ctx.driver.fns.cuGraphDestroy = destroyGraph;
        ctx.driver.fns.cuGetErrorName = errorText;
        ctx.driver.fns.cuGetErrorString = errorText;
        return ctx;
    }
};

fn qualifyGraphPhaseOom(allocator: std.mem.Allocator) !void {
    const Domain = @import("../../runtime/tier/admission_domain.zig").Domain;
    const Session = @import("../../backends/session.zig").Session;
    const domain = try Domain.create();
    defer domain.release();
    var marker: u8 = 0;
    const Owner = struct {
        fn close(_: *anyopaque) void {}
    };
    var model = Session.ModelLifetime{ .owner = &marker, .destroy_fn = Owner.close, .admission_domain = domain.pin() };
    defer model.release();
    const key = [_]u64{ 3, 5, 7 };
    const positions = [_]u32{ 1, 2, 3, 4, 5, 6 };
    var phase: GraphPhase = undefined;
    try phase.initInto(.{ .epoch = 1, .key = &key, .positions = &positions, .rows = 2, .hidden = 1024, .vocab = 8, .future_tokens = 16, .attention_layers = 1, .recurrent_layers = 1 });
    phase.mode = .storage;
    phase.expected_compute = 1;
    phase.admission = .{ .controller = &domain.controller, .limits = .{}, .check_live_memory = false };
    phase.prepareOutsideUsing(&model, allocator) catch |err| {
        try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, domain.controller.snapshot());
        try std.testing.expect(phase.cold == null);
        return err;
    };
    phase.destroyOutside();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, domain.controller.snapshot());
}

test "captured graph cold process metadata key and plan all OOM unwind exact original quota before SDK" {
    try qualifyGraphPhaseOom(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, qualifyGraphPhaseOom, .{});
}

test "captured graph exact funding precedes host plan allocation and quota final death uses current B" {
    const Domain = @import("../../runtime/tier/admission_domain.zig").Domain;
    const Session = @import("../../backends/session.zig").Session;
    const Sink = @import("../../execution/retirement_sink.zig");
    GraphTestDriver.reset();
    var ctx = GraphTestDriver.contextValue();
    const domain = try Domain.create();
    defer domain.release();
    var marker: u8 = 0;
    const Owner = struct {
        fn close(_: *anyopaque) void {}
    };
    var model = Session.ModelLifetime{ .owner = &marker, .destroy_fn = Owner.close, .admission_domain = domain.pin() };
    defer model.release();
    const key = [_]u64{ 3, 5, 7 };
    const positions = [_]u32{ 1, 2, 3, 4, 5, 6 };
    const descriptor = Descriptor{ .epoch = 1, .key = &key, .positions = &positions, .rows = 2, .hidden = 1024, .vocab = 8, .future_tokens = 16, .attention_layers = 1, .recurrent_layers = 1 };
    const request = try descriptor.storageRequest();
    const exact = try request.captured_storage.amounts();
    var phase: GraphPhase = undefined;
    try phase.initInto(descriptor);
    phase.mode = .storage;
    phase.expected_compute = 1;
    phase.admission = .{ .controller = &domain.controller, .limits = .{ .backend_limit_bytes = exact.backend_scratch_bytes - 1 }, .check_live_memory = false };
    try std.testing.expectError(error.ResourceLimitExceeded, phase.prepareOutside(&model));
    try std.testing.expect(phase.cold == null);
    try std.testing.expectEqual(@as(usize, 0), GraphTestDriver.allocations);
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, domain.controller.snapshot());
    phase.admission.?.limits.backend_limit_bytes += 1;
    try phase.prepareOutside(&model);
    defer phase.destroyOutside();
    try std.testing.expectEqualDeep(exact, domain.controller.snapshot());
    try std.testing.expectEqual(@as(usize, 0), GraphTestDriver.allocations);
    var sink = try Sink.Ref.create();
    defer sink.release();
    var first = try sink.enter();
    defer first.finish();
    var a = try first.retainEpoch();
    defer a.release();
    try phase.bind(.{ .boundary = &a });
    var owner = Module{};
    defer owner.clearWithCleanup(&ctx, .{ .outside_execution = true }) catch unreachable;
    try owner.resetPrepared(&ctx, &phase);
    try phase.close(.{ .boundary = &a });
    first.finish();
    try std.testing.expectEqualDeep(exact, domain.controller.snapshot());
    // Missing/legacy authority is rejected BEFORE graph/buffer SDK mutation.
    const allocations = GraphTestDriver.allocations;
    try std.testing.expectError(error.CacheRetirementCleanupAuthorityRequired, owner.clear(&ctx));
    try std.testing.expectEqual(allocations, GraphTestDriver.allocations);
    try std.testing.expectEqual(@as(usize, 0), GraphTestDriver.frees);
    var last = try sink.enter();
    defer last.finish();
    var b = try last.retainEpoch();
    defer b.release();
    try std.testing.expectError(error.RetirementEpochSealed, owner.clearWithCleanup(&ctx, .{ .boundary = &a }));
    const syncs = ctx.stats.stream_syncs;
    GraphTestDriver.capture_active = true;
    try std.testing.expectError(error.CapturedGraphActive, owner.clearWithCleanup(&ctx, .{ .boundary = &b }));
    try std.testing.expectEqual(syncs, ctx.stats.stream_syncs);
    try std.testing.expectEqual(@as(usize, 0), GraphTestDriver.frees);
    GraphTestDriver.capture_active = false;
    try owner.clearWithCleanup(&ctx, .{ .boundary = &b });
    try std.testing.expectEqual(@as(usize, 1), GraphTestDriver.frees);
    try std.testing.expectEqualDeep(exact, domain.controller.snapshot());
    last.finish();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, domain.controller.snapshot());
}

test "captured graph checked SDK death preserves failed exec graph buffer handles and same quota" {
    const Domain = @import("../../runtime/tier/admission_domain.zig").Domain;
    const Session = @import("../../backends/session.zig").Session;
    const Sink = @import("../../execution/retirement_sink.zig");
    GraphTestDriver.reset();
    var ctx = GraphTestDriver.contextValue();
    const domain = try Domain.create();
    defer domain.release();
    var marker: u8 = 0;
    const Owner = struct {
        fn close(_: *anyopaque) void {}
    };
    var model = Session.ModelLifetime{ .owner = &marker, .destroy_fn = Owner.close, .admission_domain = domain.pin() };
    defer model.release();
    const key = [_]u64{ 11, 13 };
    const positions = [_]u32{ 1, 2, 3, 4, 5, 6 };
    var phase: GraphPhase = undefined;
    try phase.initInto(.{ .epoch = 9, .key = &key, .positions = &positions, .rows = 2, .hidden = 1024, .vocab = 8, .future_tokens = 16, .attention_layers = 1, .recurrent_layers = 1 });
    phase.mode = .storage;
    phase.expected_compute = 1;
    phase.admission = .{ .controller = &domain.controller, .limits = .{}, .check_live_memory = false };
    try phase.prepareOutside(&model);
    defer phase.destroyOutside();
    var sink = try Sink.Ref.create();
    defer sink.release();
    var guard = try sink.enter();
    defer guard.finish();
    var epoch = try guard.retainEpoch();
    defer epoch.release();
    try phase.bind(.{ .boundary = &epoch });
    var owner = Module{};
    defer owner.clearWithCleanup(&ctx, .{ .outside_execution = true }) catch unreachable;
    try owner.resetPrepared(&ctx, &phase);
    try phase.close(.{ .boundary = &epoch });
    const before = domain.controller.snapshot();
    const physical = ctx.device_allocations.snapshot();
    const pointer = owner.metadata.ptr;
    owner.graph = @ptrFromInt(0x100);
    owner.pending_graph = @ptrFromInt(0x200);
    @import("../../test_error_logs.zig").expectErrorLogs(3);
    GraphTestDriver.fail_sync = true;
    try std.testing.expectError(error.CudaDriverError, owner.clearWithCleanup(&ctx, .{ .boundary = &epoch }));
    GraphTestDriver.fail_sync = false;
    GraphTestDriver.fail_exec = true;
    try std.testing.expectError(error.CudaDriverError, owner.clearWithCleanup(&ctx, .{ .boundary = &epoch }));
    try std.testing.expect(owner.graph != null and owner.pending_graph != null and owner.metadata.ptr == pointer);
    GraphTestDriver.fail_exec = false;
    GraphTestDriver.fail_graph = true;
    try std.testing.expectError(error.CudaDriverError, owner.clearWithCleanup(&ctx, .{ .boundary = &epoch }));
    try std.testing.expect(owner.graph == null and owner.pending_graph != null and owner.metadata.ptr == pointer);
    GraphTestDriver.fail_graph = false;
    GraphTestDriver.fail_free = true;
    try std.testing.expectError(error.CudaDriverError, owner.clearWithCleanup(&ctx, .{ .boundary = &epoch }));
    try std.testing.expect(owner.metadata.ptr == pointer);
    try std.testing.expectEqualDeep(before, domain.controller.snapshot());
    try std.testing.expectEqual(physical.live, ctx.device_allocations.snapshot().live);
    GraphTestDriver.fail_free = false;
    try owner.clearWithCleanup(&ctx, .{ .boundary = &epoch });
    try std.testing.expectEqual(@as(usize, 1), GraphTestDriver.exec_destroys);
    try std.testing.expectEqual(@as(usize, 1), GraphTestDriver.graph_destroys);
    try std.testing.expectEqual(@as(usize, 1), GraphTestDriver.frees);
    try std.testing.expectEqualDeep(before, domain.controller.snapshot());
    guard.finish();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, domain.controller.snapshot());
}

test "captured graph wide head and learned arena consume separate exact prefunded payload scopes" {
    const Domain = @import("../../runtime/tier/admission_domain.zig").Domain;
    const Session = @import("../../backends/session.zig").Session;
    const Sink = @import("../../execution/retirement_sink.zig");
    GraphTestDriver.reset();
    var ctx = GraphTestDriver.contextValue();
    const domain = try Domain.create();
    defer domain.release();
    var marker: u8 = 0;
    const Owner = struct {
        fn close(_: *anyopaque) void {}
    };
    var model = Session.ModelLifetime{ .owner = &marker, .destroy_fn = Owner.close, .admission_domain = domain.pin() };
    defer model.release();
    const positions: [384]u32 = @splat(1);
    const key = [_]u64{ 17, 23 };
    const descriptor = Descriptor{ .epoch = 42, .key = &key, .positions = &positions, .rows = 128, .hidden = 1024, .vocab = 8, .future_tokens = 16, .attention_layers = 1, .recurrent_layers = 1 };
    const request = try descriptor.storageRequest();
    const exact = try request.captured_storage.amounts();
    var storage: GraphPhase = undefined;
    try storage.initInto(descriptor);
    storage.mode = .storage;
    storage.expected_compute = 1;
    storage.admission = .{ .controller = &domain.controller, .limits = .{}, .check_live_memory = false };
    try storage.prepareOutside(&model);
    defer storage.destroyOutside();
    var sink = try Sink.Ref.create();
    defer sink.release();
    var initial = try sink.enter();
    defer initial.finish();
    var a = try initial.retainEpoch();
    defer a.release();
    try storage.bind(.{ .boundary = &a });
    var owner = Module{};
    defer owner.clearWithCleanup(&ctx, .{ .outside_execution = true }) catch unreachable;
    try owner.resetPrepared(&ctx, &storage);
    owner.epoch_id = descriptor.epoch;
    try storage.close(.{ .boundary = &a });
    initial.finish();
    try std.testing.expectEqual(@as(usize, 128 * 8 * 4), owner.head.len);
    try std.testing.expectEqual(@as(usize, 2), GraphTestDriver.allocations);
    try std.testing.expectEqualDeep(exact, domain.controller.snapshot());
    owner.plan.?.allocate(1024, 0x1000000);
    owner.plan.?.release(0x1000000);
    try std.testing.expect(owner.plan.?.finishNoOutput());
    owner.phase = .capture;
    var arena: GraphPhase = undefined;
    try arena.initInto(descriptor);
    arena.mode = .arena;
    arena.expected_compute = 1;
    arena.old = owner.snapshot();
    arena.admission = storage.admission;
    try arena.prepareOutside(&model);
    defer arena.destroyOutside();
    var current = try sink.enter();
    defer current.finish();
    var b = try current.retainEpoch();
    defer b.release();
    try arena.bind(.{ .boundary = &b });
    try owner.reserveArenaPrepared(&ctx, &arena);
    const pointer = owner.arena.ptr;
    try owner.reserveArenaPrepared(&ctx, &arena); // warm path consumes no token twice
    try std.testing.expectEqual(pointer, owner.arena.ptr);
    try std.testing.expectEqual(@as(usize, 3), GraphTestDriver.allocations);
    try arena.close(.{ .boundary = &b });
    const total = try exact.merge(.{ .backend_scratch_bytes = owner.plan.?.peak });
    try std.testing.expectEqualDeep(total, domain.controller.snapshot());
    try owner.clearWithCleanup(&ctx, .{ .boundary = &b });
    try std.testing.expectEqual(@as(usize, 3), GraphTestDriver.frees);
    try std.testing.expectEqualDeep(total, domain.controller.snapshot());
    current.finish();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, domain.controller.snapshot());
    try std.testing.expect(ctx.prepared_cache_funding == null); // norm binding never overridden
}
