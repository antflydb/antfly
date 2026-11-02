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

//! Stable, caller-owned adapter. Prepare before publishing the port, keep the
//! adapter alive through owning finish, then join physical users before deinit.
const std = @import("std");
const resources = @import("resource_manager.zig");
const runnable = @import("../common/runnable_scheduler.zig");

pub const Bundle = struct {
    const Phase = enum(u8) { pending, acquiring, granted, closing, releasing, retired };
    resources: resources.PreparedDenseBundle,
    phase: std.atomic.Value(Phase) = .init(.pending),
    allocation_mutex: std.atomic.Mutex = .unlocked,
    allocation_owner: ?*Bundle = null,
    allocated: [resources.slice_count]u64 = @splat(0),
    live_allocations: usize = 0,

    fn lockAllocations(self: *Bundle) void {
        while (!self.allocation_mutex.tryLock()) std.atomic.spinLoopHint();
    }
    fn bindOwner(self: *Bundle) void {
        self.lockAllocations();
        defer self.allocation_mutex.unlock();
        if (self.allocation_owner == null) self.allocation_owner = self;
    }
    /// Create views before publication; keep each view at a stable address
    /// while its std allocator is borrowed. All views share the bundle quota.
    /// Backing must be the physical allocator, without another resource charge.
    pub fn allocationView(self: *Bundle, slice: resources.Slice, backing: std.mem.Allocator) AllocationView {
        self.bindOwner();
        return .{ .bundle = self, .manager = self.resources.memory.manager, .identity = self.resources.memory.identity, .slice = slice, .backing = backing };
    }
    pub fn allocatedBytes(self: *Bundle, slice: resources.Slice) u64 {
        self.lockAllocations();
        defer self.allocation_mutex.unlock();
        return self.allocated[@backingInt(slice)];
    }
    fn reserveAllocation(self: *Bundle, view: *const AllocationView, bytes: usize, new_allocation: bool) bool {
        self.lockAllocations();
        defer self.allocation_mutex.unlock();
        if (self.phase.load(.acquire) != .granted) return false;
        // A copied owner cannot create a second independently enforced quota.
        // Copies of allocator views still point to this original shared owner.
        if (self.allocation_owner != self or view.manager != self.resources.memory.manager or
            view.identity != self.resources.memory.identity) return false;
        const amount = std.math.cast(u64, bytes) orelse return false;
        const index = @backingInt(view.slice);
        const next = std.math.add(u64, self.allocated[index], amount) catch return false;
        if (next > self.resources.memory.amounts[index]) return false;
        const count = if (new_allocation) std.math.add(usize, self.live_allocations, 1) catch return false else self.live_allocations;
        self.allocated[index] = next;
        self.live_allocations = count;
        return true;
    }
    fn viewLive(self: *Bundle, view: *const AllocationView, closing_allowed: bool) bool {
        self.lockAllocations();
        defer self.allocation_mutex.unlock();
        const phase = self.phase.load(.acquire);
        return (phase == .granted or (closing_allowed and phase == .closing)) and self.allocation_owner == self and
            view.manager == self.resources.memory.manager and view.identity == self.resources.memory.identity;
    }
    fn returnAllocation(self: *Bundle, view: *const AllocationView, bytes: usize, freed: bool) void {
        self.lockAllocations();
        defer self.allocation_mutex.unlock();
        if (view.manager != self.resources.memory.manager or view.identity != self.resources.memory.identity)
            @panic("stale bundle allocator view");
        const index = @backingInt(view.slice);
        if (bytes > self.allocated[index] or (freed and self.live_allocations == 0))
            @panic("bundle allocation ownership mismatch");
        self.allocated[index] -= bytes;
        if (freed) self.live_allocations -= 1;
    }

    pub fn init(manager: *resources.ResourceManager, amounts: []const resources.SliceAmount, bandwidth_bytes: u64) !Bundle {
        return .{ .resources = try manager.prepareDenseBundle(amounts, bandwidth_bytes) };
    }
    pub fn port(self: *Bundle) runnable.GrantResources {
        self.bindOwner();
        return .{ .context = self, .probe = probe, .acquire = acquire };
    }
    fn probe(context: *anyopaque) callconv(.c) c_int {
        const self: *Bundle = @ptrCast(@alignCast(context));
        if (self.allocation_owner != self) return @backingInt(runnable.GrantResources.Probe.mismatch);
        switch (self.phase.load(.acquire)) {
            .pending => {},
            .acquiring => return @backingInt(runnable.GrantResources.Probe.wait),
            else => return @backingInt(runnable.GrantResources.Probe.mismatch),
        }
        const result: runnable.GrantResources.Probe = switch (self.resources.probe()) {
            .ready => .ready,
            .wait => .wait,
            .unknown => .unknown,
            .too_large => .too_large,
            .unsupported => .unsupported,
            .mismatch => .mismatch,
        };
        return @backingInt(result);
    }
    fn acquire(context: *anyopaque) callconv(.c) c_int {
        const self: *Bundle = @ptrCast(@alignCast(context));
        if (self.allocation_owner != self) return @backingInt(runnable.GrantResources.Acquire.mismatch);
        if (self.phase.cmpxchgStrong(.pending, .acquiring, .acq_rel, .acquire) != null) return @backingInt(runnable.GrantResources.Acquire.mismatch);
        var granted = false;
        defer if (!granted) self.phase.store(.pending, .release);
        const result: runnable.GrantResources.Acquire = switch (self.resources.tryAcquire()) {
            .acquired => blk: {
                self.phase.store(.granted, .release);
                granted = true;
                break :blk .acquired;
            },
            .wait => .wait,
            .too_large => .too_large,
            .unsupported => .unsupported,
            .mismatch => .mismatch,
        };
        return @backingInt(result);
    }
    /// First stop/detach any still-pending executor participant and join its
    /// physical users. Closing fences new allocations even when buffers remain;
    /// existing frees/shrinks can finish and retry retirement without CPU.
    pub fn tryDeinitAfterJoin(self: *Bundle) !void {
        self.lockAllocations();
        const phase = self.phase.load(.acquire);
        if (phase != .retired and self.allocation_owner != null and self.allocation_owner != self) {
            self.phase.store(.retired, .release);
            self.allocation_mutex.unlock();
            self.resources.memory.manager.recordAccountingError();
            return error.ResourceOwnerCopied;
        }
        switch (phase) {
            .retired => {
                self.allocation_mutex.unlock();
                return;
            },
            .acquiring, .releasing => {
                self.allocation_mutex.unlock();
                return error.ResourcesRetiring;
            },
            .pending, .granted => {
                if (self.phase.cmpxchgStrong(phase, .closing, .acq_rel, .acquire) != null) {
                    self.allocation_mutex.unlock();
                    return error.ResourcesRetiring;
                }
            },
            .closing => {},
        }
        if (self.live_allocations != 0) {
            self.allocation_mutex.unlock();
            return error.ResourcesStillLive;
        }
        self.phase.store(.releasing, .release);
        self.allocation_mutex.unlock();
        // No backing allocator or resource callback runs under the quota lock.
        self.resources.deinitAfterJoin();
        self.phase.store(.retired, .release);
    }
    pub fn deinitAfterJoin(self: *Bundle) void {
        self.tryDeinitAfterJoin() catch |err| switch (err) {
            error.ResourceOwnerCopied => return,
            else => @panic("bundle retired before physical allocation join"),
        };
    }
};

/// One physical allocator view into a granted bundle slice. The fixed quota
/// includes the sum across all views/workers. Allocations never reserve again
/// from the manager, grow the grant, reclaim, or borrow another slice's credit.
pub const AllocationView = struct {
    pub const Failure = enum(u8) { none, capacity, backing };
    bundle: *Bundle,
    manager: *resources.ResourceManager,
    identity: u64,
    slice: resources.Slice,
    backing: std.mem.Allocator,
    failure: std.atomic.Value(Failure) = .init(.none),

    pub fn allocator(self: *AllocationView) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }
    pub fn lastFailure(self: *const AllocationView) Failure {
        return self.failure.load(.acquire);
    }
    fn alloc(context: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *AllocationView = @ptrCast(@alignCast(context));
        if (len == 0 or !self.bundle.reserveAllocation(self, len, true)) {
            self.failure.store(.capacity, .release);
            return null;
        }
        const pointer = self.backing.rawAlloc(len, alignment, ra) orelse {
            self.bundle.returnAllocation(self, len, true);
            self.failure.store(.backing, .release);
            return null;
        };
        // The physical allocation can finish after a concurrent close. Keep
        // its full credit until the backing buffer is freed, and do not expose
        // a new pointer from that already fenced allocation operation.
        if (!self.bundle.viewLive(self, false)) {
            self.backing.rawFree(pointer[0..len], alignment, ra);
            self.bundle.returnAllocation(self, len, true);
            self.failure.store(.capacity, .release);
            return null;
        }
        self.failure.store(.none, .release);
        return pointer;
    }
    fn resize(context: *anyopaque, memory: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) bool {
        const self: *AllocationView = @ptrCast(@alignCast(context));
        if (len == 0 or !self.bundle.viewLive(self, true)) return false;
        const growth = len -| memory.len;
        if (growth != 0 and !self.bundle.reserveAllocation(self, growth, false)) {
            self.failure.store(.capacity, .release);
            return false;
        }
        if (!self.backing.rawResize(memory, alignment, len, ra)) {
            if (growth != 0) self.bundle.returnAllocation(self, growth, false);
            return false;
        }
        if (len < memory.len) self.bundle.returnAllocation(self, memory.len - len, false);
        self.failure.store(.none, .release);
        return true;
    }
    fn remap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
        // Opaque moving remap can temporarily retain old and new allocations.
        // Force the ordinary realloc fallback so both are charged until free.
        return null;
    }
    fn free(context: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *AllocationView = @ptrCast(@alignCast(context));
        if (!self.bundle.viewLive(self, true)) @panic("stale bundle allocator free");
        self.backing.rawFree(memory, alignment, ra);
        self.bundle.returnAllocation(self, memory.len, true);
    }
};
