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

//! Per-owner physical DeviceBuffer accounting. Cached and deferred-free buffers
//! stay charged until cuMemFree succeeds. Driver/library-private memory is not
//! visible here and requires separate deployment headroom.
const std = @import("std");
const observer = @import("../../execution/allocation_observer.zig");
const roots = @import("../../execution/device_allocation_registry.zig");

pub const OwnedPurpose = @import("../../execution/storage_accounting.zig").OwnedPurpose;
const purpose_count = @import("../../execution/storage_accounting.zig").owned_purpose_count;

pub const Budget = struct {
    mutex: std.atomic.Mutex = .unlocked,
    limit: usize = std.math.maxInt(usize),
    live: usize = 0,
    peak: usize = 0,
    physical_live: usize = 0,
    physical_peak: usize = 0,
    purpose_live: [purpose_count]u64 = @splat(0),
    purpose_peak: [purpose_count]u64 = @splat(0),
    allocations: u64 = 0,
    frees: u64 = 0,
    observation: ?*observer.State = null,
    handoffs_in: u64 = 0,
    handoffs_out: u64 = 0,

    fn observedSnapshot(self: *Budget) observer.Snapshot {
        return .{ .purpose_live_bytes = self.purpose_live, .purpose_lifetime_peak_bytes = self.purpose_peak, .requested_live_bytes = self.physical_live, .requested_lifetime_peak_bytes = self.physical_peak, .allocations = self.allocations, .frees = self.frees, .custody_transfers_in = self.handoffs_in, .custody_transfers_out = self.handoffs_out };
    }
    fn observeCommit(self: *Budget) void {
        if (self.observation) |state| state.commitExternal(self.observedSnapshot());
    }
    /// Budget -> State is the only lock order; every operation under those
    /// locks is pure metadata. Candidate allocation/destruction and registry
    /// binding (including overflow allocation) occur outside these locks.
    pub const ObservationPin = struct { state: *observer.State, installed: bool };
    pub fn retainObservation(self: *Budget) ?*observer.State {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        return if (self.observation) |state| state.retain() else null;
    }
    /// A true installed result transfers candidate's original ref to Budget.
    /// In either case the returned pin is independently retained for the caller.
    pub fn installObservation(self: *Budget, candidate: *observer.State) ObservationPin {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        const installed = self.observation == null;
        if (installed) {
            candidate.commitExternal(self.observedSnapshot());
            self.observation = candidate;
        }
        return .{ .state = self.observation.?.retain(), .installed = installed };
    }
    pub fn enableObservation(self: *Budget, metadata: std.mem.Allocator) bool {
        if (self.retainObservation()) |existing| {
            existing.release();
            return true;
        }
        const candidate = observer.State.createExternal(metadata, .{}) catch {
            // A competing successful installer can satisfy an optional OOM.
            if (self.retainObservation()) |existing| {
                existing.release();
                return true;
            }
            return false;
        };
        const result = self.installObservation(candidate);
        if (!result.installed) candidate.release();
        result.state.release();
        return true;
    }
    pub fn registerObservation(self: *Budget) void {
        if (!roots.enabled()) return;
        const metadata = @import("../../execution/allocation_registry.zig").metadataAllocator();
        if (!self.enableObservation(metadata)) {
            roots.missing();
            return;
        }
        const state = self.retainObservation() orelse {
            roots.missing();
            return;
        };
        defer state.release();
        roots.bind(.{ .state = state, .backing = .cuda_successful_cuMemAlloc_owned_spans });
    }
    pub fn releaseObservation(self: *Budget) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        const state = self.observation;
        self.observation = null;
        self.mutex.unlock();
        if (state) |value| value.release();
    }

    /// Move existing physical ownership without inventing SDK allocation/free
    /// events. A cleanup-only provider can outlive the original compute owner.
    pub fn handoffTo(self: *Budget, destination: *Budget, bytes: usize) !void {
        return self.handoffToWithPurpose(destination, bytes, .unclassified);
    }
    pub fn handoffToWithPurpose(self: *Budget, destination: *Budget, bytes: usize, purpose: OwnedPurpose) !void {
        if (self == destination or bytes == 0) return;
        const first = if (@intFromPtr(self) < @intFromPtr(destination)) self else destination;
        const second = if (first == self) destination else self;
        while (!first.mutex.tryLock()) std.atomic.spinLoopHint();
        defer first.mutex.unlock();
        while (!second.mutex.tryLock()) std.atomic.spinLoopHint();
        defer second.mutex.unlock();
        const index = @backingInt(purpose);
        if (bytes > self.physical_live or bytes > self.live or bytes > self.purpose_live[index] or bytes > destination.limit - destination.live) return error.InvalidCudaPhysicalHandoff;
        self.physical_live -= bytes;
        self.live -= bytes;
        self.purpose_live[index] -= bytes;
        destination.purpose_live[index] += bytes;
        destination.purpose_peak[index] = @max(destination.purpose_peak[index], destination.purpose_live[index]);
        destination.physical_live += bytes;
        destination.live += bytes;
        destination.peak = @max(destination.peak, destination.live);
        destination.physical_peak = @max(destination.physical_peak, destination.physical_live);
        self.handoffs_out +%= 1;
        destination.handoffs_in +%= 1;
        self.observeCommit();
        destination.observeCommit();
    }

    pub const Snapshot = struct { limit: usize, live: usize, peak: usize, physical_live: usize, physical_peak: usize, allocations: u64, frees: u64, purpose_live: [purpose_count]u64, purpose_peak: [purpose_count]u64 };

    pub fn snapshot(self: *Budget) Snapshot {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        return .{ .limit = self.limit, .live = self.live, .peak = self.peak, .physical_live = self.physical_live, .physical_peak = self.physical_peak, .allocations = self.allocations, .frees = self.frees, .purpose_live = self.purpose_live, .purpose_peak = self.purpose_peak };
    }

    pub fn setLimit(self: *Budget, limit: usize) error{CudaMemoryLimitExceeded}!void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        if (limit == 0 or self.live > limit) return error.CudaMemoryLimitExceeded;
        self.limit = limit;
    }

    pub fn reserve(self: *Budget, bytes: usize) error{CudaMemoryLimitExceeded}!void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        if (bytes > self.limit - self.live) return error.CudaMemoryLimitExceeded;
        self.live += bytes;
        self.peak = @max(self.peak, self.live);
    }

    /// Call only after a successful physical driver allocation. Reservation
    /// rollback must not inflate successful allocation counts or memory peaks.
    pub fn allocated(self: *Budget, bytes: usize) void {
        self.allocatedWithPurpose(bytes, .unclassified);
    }
    pub fn allocatedWithPurpose(self: *Budget, bytes: usize, purpose: OwnedPurpose) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        std.debug.assert(bytes <= self.live - self.physical_live);
        self.physical_live += bytes;
        const index = @backingInt(purpose);
        self.purpose_live[index] += bytes;
        self.purpose_peak[index] = @max(self.purpose_peak[index], self.purpose_live[index]);
        self.physical_peak = @max(self.physical_peak, self.physical_live);
        self.allocations +%= 1;
        self.observeCommit();
    }

    /// Successful frees release both physical ownership and its reservation.
    /// Failed driver frees call neither this nor release.
    pub fn freed(self: *Budget, bytes: usize) void {
        self.freedWithPurpose(bytes, .unclassified);
    }
    pub fn freedWithPurpose(self: *Budget, bytes: usize, purpose: OwnedPurpose) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        const index = @backingInt(purpose);
        std.debug.assert(bytes <= self.physical_live and bytes <= self.purpose_live[index]);
        self.purpose_live[index] -= bytes;
        self.physical_live -= bytes;
        self.live -= bytes;
        self.frees +%= 1;
        self.observeCommit();
    }

    pub fn release(self: *Budget, bytes: usize) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        std.debug.assert(bytes <= self.live);
        self.live -= bytes;
    }
};

test "CUDA boundary physical allocation budget rejects overflow and preserves failed reservations" {
    var budget: Budget = .{};
    try budget.reserve(12);
    try std.testing.expectError(error.CudaMemoryLimitExceeded, budget.setLimit(11));
    try budget.setLimit(32);
    try budget.reserve(20);
    try std.testing.expectError(error.CudaMemoryLimitExceeded, budget.reserve(1));
    try std.testing.expectError(error.CudaMemoryLimitExceeded, budget.reserve(std.math.maxInt(usize)));
    try std.testing.expectEqual(@as(usize, 32), budget.snapshot().live);
    budget.release(20);
    try budget.reserve(20);
    budget.release(32);
    try std.testing.expectEqual(@as(usize, 0), budget.snapshot().live);
    try std.testing.expectEqual(@as(usize, 32), budget.snapshot().peak);
}

test "CUDA physical counters exclude rolled-back reservations and keep failed frees charged" {
    var budget: Budget = .{};
    try budget.reserve(64);
    budget.release(64); // Failed cuMemAlloc.
    try std.testing.expectEqual(@as(u64, 0), budget.snapshot().allocations);
    try std.testing.expectEqual(@as(usize, 0), budget.snapshot().physical_peak);
    try budget.reserve(32);
    budget.allocated(32);
    try std.testing.expectEqual(@as(usize, 32), budget.snapshot().physical_live);
    // A failed cuMemFree leaves the ownership and quota untouched.
    try std.testing.expectEqual(@as(u64, 0), budget.snapshot().frees);
    budget.freed(32);
    const final = budget.snapshot();
    try std.testing.expectEqual(@as(usize, 0), final.live);
    try std.testing.expectEqual(@as(usize, 0), final.physical_live);
    try std.testing.expectEqual(@as(usize, 32), final.physical_peak);
    try std.testing.expectEqual(@as(u64, 1), final.allocations);
    try std.testing.expectEqual(@as(u64, 1), final.frees);
}

test "CUDA retirement accounting handoff is exact and failure atomic" {
    var source: Budget = .{};
    var target: Budget = .{};
    try source.reserve(64);
    source.allocated(64);
    try target.setLimit(32);
    const before = source.snapshot();
    try std.testing.expectError(error.InvalidCudaPhysicalHandoff, source.handoffTo(&target, 33));
    try std.testing.expectEqualDeep(before, source.snapshot());
    try std.testing.expectEqual(@as(usize, 0), target.snapshot().live);
    try std.testing.expectError(error.InvalidCudaPhysicalHandoff, source.handoffTo(&target, 65));
    try std.testing.expectEqualDeep(before, source.snapshot());
    try source.handoffTo(&target, 16);
    try std.testing.expectEqual(@as(usize, 48), source.snapshot().physical_live);
    try std.testing.expectEqual(@as(usize, 16), target.snapshot().physical_live);
    try std.testing.expectEqual(@as(u64, 1), source.snapshot().allocations);
    try std.testing.expectEqual(@as(u64, 0), target.snapshot().allocations);
    try std.testing.expectEqual(@as(u64, 0), source.snapshot().frees);
    target.freed(16);
    source.freed(48);
    try std.testing.expectEqual(@as(usize, 0), target.snapshot().live);
    try std.testing.expectEqual(@as(usize, 0), source.snapshot().live);
}

test "CUDA owned device intervals preserve failed alloc and free charge and real handoff units" {
    const a = std.testing.allocator;
    var source: Budget = .{};
    defer source.releaseObservation();
    var destination: Budget = .{};
    defer destination.releaseObservation();
    try std.testing.expect(source.enableObservation(a));
    try std.testing.expect(destination.enableObservation(a));
    var outer: roots.Registry = undefined;
    outer.begin(a);
    source.registerObservation();
    destination.registerObservation();
    try source.reserve(64);
    source.release(64); // driver allocation failed
    try source.reserve(12);
    source.allocated(12);
    try source.reserve(20);
    source.allocated(20);
    // Failed cuMemFree does not call freed; the entire 32 remains charged.
    try source.handoffTo(&destination, 20);
    source.freed(12);
    destination.freed(20);
    const result = outer.end();
    try std.testing.expectEqual(@as(usize, 2), result.roots);
    try std.testing.expectEqual(@as(u64, 32), result.reports[0].interval_peak_bytes);
    try std.testing.expectEqual(@as(u64, 20), result.reports[1].interval_peak_bytes);
    try std.testing.expectEqual(@as(u64, 2), result.reports[0].allocations);
    try std.testing.expectEqual(@as(u64, 1), result.reports[1].frees);
    try std.testing.expectEqual(@as(u64, 0), result.reports[1].allocations);
    try std.testing.expectEqual(@as(u64, 1), result.reports[0].handoffs_out);
    try std.testing.expectEqual(@as(u64, 1), result.reports[1].handoffs_in);
}

test "CUDA owned device root interval sees simultaneous independent worker commits without summing peaks" {
    const a = std.testing.allocator;
    var budget: Budget = .{};
    defer budget.releaseObservation();
    try std.testing.expect(budget.enableObservation(a));
    var registry: roots.Registry = undefined;
    registry.begin(a);
    var registry_live = true;
    defer if (registry_live) {
        _ = registry.end();
    };
    budget.registerObservation();
    var ready: std.atomic.Value(usize) = .init(0);
    var release: std.atomic.Value(bool) = .init(false);
    const Work = struct {
        fn run(owner: *Budget, entered: *std.atomic.Value(usize), released: *std.atomic.Value(bool)) void {
            owner.reserve(16) catch unreachable;
            owner.allocated(16);
            _ = entered.fetchAdd(1, .release);
            while (!released.load(.acquire)) std.atomic.spinLoopHint();
            owner.freed(16);
        }
    };
    var workers: [4]std.Thread = undefined;
    var started: usize = 0;
    defer {
        release.store(true, .release);
        for (workers[0..started]) |worker| worker.join();
    }
    for (&workers) |*worker| {
        worker.* = try std.Thread.spawn(.{}, Work.run, .{ &budget, &ready, &release });
        started += 1;
    }
    while (ready.load(.acquire) != workers.len) std.atomic.spinLoopHint();
    const held = registry.snapshot();
    try std.testing.expectEqual(@as(u64, 64), held.reports[0].live_bytes);
    try std.testing.expectEqual(@as(u64, 64), held.reports[0].interval_peak_bytes);
    release.store(true, .release);
    for (workers[0..started]) |worker| worker.join();
    started = 0;
    const result = registry.end();
    registry_live = false;
    try std.testing.expectEqual(@as(u64, 0), result.reports[0].live_bytes);
    try std.testing.expectEqual(@as(u64, 64), result.reports[0].interval_peak_bytes);
    try std.testing.expectEqual(@as(u64, 4), result.reports[0].allocations);
    try std.testing.expectEqual(@as(u64, 4), result.reports[0].frees);
}

test "CUDA device root optional metadata failure preserves numerical budget and disabled commits allocate nothing" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var budget: Budget = .{};
    defer budget.releaseObservation();
    try budget.reserve(8);
    budget.allocated(8);
    budget.freed(8);
    try std.testing.expect(!failing.has_induced_failure);
    const before = budget.snapshot();
    try std.testing.expect(!budget.enableObservation(failing.allocator()));
    try std.testing.expectEqualDeep(before, budget.snapshot());
    try std.testing.expect(budget.observation == null);
    try budget.reserve(12);
    budget.allocated(12);
    budget.freed(12);
    try std.testing.expectEqual(@as(usize, 0), budget.snapshot().physical_live);
    try std.testing.expectEqual(@as(u64, 2), budget.snapshot().allocations);
}

test "CUDA device root cold metadata and overflow callbacks run outside budget and ledger locks" {
    const Meta = struct {
        backing: std.mem.Allocator,
        budget: *Budget,
        owner: ?*observer.State = null,
        callbacks: usize = 0,
        fn check(self: *@This()) void {
            _ = self.budget.snapshot();
            if (self.owner) |state| _ = state.snapshot();
            self.callbacks += 1;
        }
        fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ret: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.check();
            return self.backing.rawAlloc(len, alignment, ret);
        }
        fn resize(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, len: usize, ret: usize) bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.check();
            return self.backing.rawResize(memory, alignment, len, ret);
        }
        fn remap(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, len: usize, ret: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.check();
            return self.backing.rawRemap(memory, alignment, len, ret);
        }
        fn free(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.check();
            self.backing.rawFree(memory, alignment, ret);
        }
        const vtable: std.mem.Allocator.VTable = .{ .alloc = alloc, .resize = resize, .remap = remap, .free = free };
    };
    var budget: Budget = .{};
    var meta = Meta{ .backing = std.testing.allocator, .budget = &budget };
    try std.testing.expect(budget.enableObservation(.{ .ptr = &meta, .vtable = &Meta.vtable }));
    const pin = budget.retainObservation().?;
    meta.owner = pin;
    const interval = try pin.beginInterval();
    try budget.reserve(23);
    budget.allocated(23);
    budget.freed(23);
    const measured = interval.finish();
    try std.testing.expectEqual(@as(u64, 23), measured.peak);
    // State still has the explicit pin while Budget's ref retires. Metadata
    // destructor may sample Budget and State without lock inversion.
    budget.releaseObservation();
    meta.owner = null;
    pin.release();
    try std.testing.expect(meta.callbacks >= 4);
}

test "CUDA exact KV purpose intervals preserve nested roots custody and nonadditive peaks" {
    var source: Budget = .{};
    var destination: Budget = .{};
    defer source.releaseObservation();
    defer destination.releaseObservation();
    var outer: roots.Registry = undefined;
    outer.begin(std.testing.allocator);
    defer if (outer.attached) {
        _ = outer.end();
    };
    source.registerObservation();
    destination.registerObservation();
    try source.reserve(100);
    source.allocated(100);
    source.freed(100);
    var inner: roots.Registry = undefined;
    inner.begin(std.testing.allocator);
    defer if (inner.attached) {
        _ = inner.end();
    };
    source.registerObservation();
    destination.registerObservation();
    try source.reserve(80);
    source.allocatedWithPurpose(80, .kv_state);
    const before = source.snapshot();
    try std.testing.expectError(error.InvalidCudaPhysicalHandoff, source.handoffTo(&destination, 1));
    try std.testing.expectEqualDeep(before, source.snapshot());
    try source.handoffToWithPurpose(&destination, 80, .kv_state);
    const held = inner.end();
    source.releaseObservation(); // independent interval retains the real source root
    destination.freedWithPurpose(80, .kv_state);
    const ended = outer.end();
    const unknown = @backingInt(OwnedPurpose.unclassified);
    const kv = @backingInt(OwnedPurpose.kv_state);
    try std.testing.expectEqual(@as(u64, 100), ended.reports[0].interval_peak_bytes);
    try std.testing.expectEqual(@as(u64, 100), ended.reports[0].purpose_interval_peak_bytes[unknown]);
    try std.testing.expectEqual(@as(u64, 80), ended.reports[0].purpose_interval_peak_bytes[kv]);
    try std.testing.expectEqual(@as(u64, 0), ended.reports[0].live_bytes);
    try std.testing.expectEqual(@as(u64, 80), held.reports[1].purpose_live_bytes[kv]);
    try std.testing.expectEqual(@as(u64, 0), ended.reports[1].purpose_live_bytes[kv]);
    try std.testing.expectEqual(@as(u64, 1), ended.reports[0].handoffs_out);
    try std.testing.expectEqual(@as(u64, 1), ended.reports[1].handoffs_in);
}
test "CUDA exact purpose updates concurrent allocators without new metadata allocations" {
    var budget: Budget = .{};
    defer budget.releaseObservation();
    try std.testing.expect(budget.enableObservation(std.testing.allocator));
    const state = budget.retainObservation().?;
    defer state.release();
    var interval: observer.State.Interval = undefined;
    state.beginIntervalInto(&interval);
    var interval_live = true;
    defer if (interval_live) {
        _ = interval.finish();
    };
    const Worker = struct {
        fn run(owner: *Budget, purpose: OwnedPurpose) !void {
            for (0..31) |_| {
                try owner.reserve(16);
                owner.allocatedWithPurpose(16, purpose);
                owner.freedWithPurpose(16, purpose);
            }
        }
    };
    var first = try std.testing.io.concurrent(Worker.run, .{ &budget, OwnedPurpose.kv_state });
    var first_live = true;
    defer if (first_live) {
        _ = first.cancel(std.testing.io) catch {};
    };
    var second = try std.testing.io.concurrent(Worker.run, .{ &budget, OwnedPurpose.unclassified });
    var second_live = true;
    defer if (second_live) {
        _ = second.cancel(std.testing.io) catch {};
    };
    try first.await(std.testing.io);
    first_live = false;
    try second.await(std.testing.io);
    second_live = false;
    const report = interval.finish();
    interval_live = false;
    try std.testing.expectEqual(@as(u64, 62), report.usage.allocations);
    try std.testing.expectEqual(@as(u64, 62), report.usage.frees);
    try std.testing.expectEqual(@as(u64, 0), report.usage.requested_live_bytes);
    for (report.usage.purpose_live_bytes) |live| try std.testing.expectEqual(@as(u64, 0), live);
    for (report.purpose_peak, 0..) |peak, i| {
        if (i <= @backingInt(OwnedPurpose.kv_state)) try std.testing.expect(peak >= 16 and peak <= report.peak) else try std.testing.expectEqual(@as(u64, 0), peak);
    }
}
