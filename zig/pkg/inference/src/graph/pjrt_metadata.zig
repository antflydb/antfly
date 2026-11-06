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
const platform = @import("antfly_platform");
const admission = @import("pjrt_admission.zig");
const memory = @import("../runtime/tier/memory.zig");
const Manager = @import("../server/model_manager.zig").ModelManager;

/// Stable owner for all selection metadata, including parser scratch and
/// retained arenas. Charge before allocation and refund after physical free.
/// Refusing resize makes realloc account for both old and new buffers at peak.
pub const Budget = struct {
    backing: std.mem.Allocator,
    lifetime: Manager.ResourceLifetime,
    limits: memory.Limits,
    lease: memory.AdmissionLease,
    denial: ?anyerror = null,
    /// Actual requested backing bytes, including the owner and block headers.
    live_bytes: usize = 0,
    capacity: ?memory.LiveReservation = null,

    const Header = struct { lease: memory.AdmissionLease, residency: memory.LiveReservation.Residency };

    pub fn create(backing: std.mem.Allocator, context: *const admission.Context) !*Budget {
        const lifetime = try context.manager.retainResourceLifetime();
        errdefer lifetime.deinit();
        const limits = admission.ArtifactDeviceAdmission.limitsFor(context, true);
        var lease = try context.manager.acquireRunResourceAmounts(.cpu, limits, .{ .host_weight_bytes = @sizeOf(Budget) });
        errdefer lease.release();
        const reclaiming = platform.allocator.reclaimingScratch(backing);
        const self = try reclaiming.create(Budget);
        errdefer reclaiming.destroy(self);
        try lease.retain(lease.amounts);
        self.* = .{ .backing = reclaiming, .lifetime = lifetime, .limits = limits, .lease = lease, .live_bytes = @sizeOf(Budget) };
        return self;
    }

    /// End a construction phase before delegating to another admission owner.
    pub fn releaseUnusedCapacity(a: std.mem.Allocator) void {
        if (a.vtable != &vtable) return;
        const self: *Budget = @ptrCast(@alignCast(a.ptr));
        self.releaseCapacity();
    }

    pub fn releaseCapacity(self: *Budget) void {
        if (self.capacity) |*capacity| capacity.release();
        self.capacity = null;
    }

    fn ensureCapacity(self: *Budget, bytes: usize) !void {
        if (self.capacity) |capacity| if (capacity.remaining >= bytes) return;
        self.releaseCapacity();
        const batch = @max(bytes, 64 * 1024);
        self.capacity = self.lifetime.reserveMetadataCapacity(batch) catch |err| blk: {
            // Batching must not reject a small allocation that fits on its own.
            if (batch == bytes or err != error.ResourceTemporarilyUnavailable) return err;
            break :blk try self.lifetime.reserveMetadataCapacity(bytes);
        };
    }

    pub fn destroy(self: *Budget) void {
        std.debug.assert(self.live_bytes == @sizeOf(Budget));
        self.releaseCapacity();
        const backing = self.backing;
        const lifetime = self.lifetime;
        var lease = self.lease;
        backing.destroy(self);
        lease.release();
        lifetime.deinit();
    }

    pub fn translate(self: *const Budget, err: anyerror) anyerror {
        return if (err == error.OutOfMemory) self.denial orelse err else err;
    }

    pub fn allocator(self: *Budget) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *Budget = @ptrCast(@alignCast(raw));
        self.denial = null;
        const offset = alignment.forward(@sizeOf(Header));
        const total = std.math.add(usize, offset, len) catch {
            self.denial = error.ResourceLimitExceeded;
            return null;
        };
        var lease = self.lifetime.acquireMetadata(self.limits, total) catch |err| {
            self.denial = err;
            return null;
        };
        self.ensureCapacity(total) catch |err| {
            lease.release();
            self.denial = err;
            return null;
        };
        const block = self.backing.rawAlloc(total, alignment.max(.of(Header)), ra) orelse {
            lease.release();
            return null;
        };
        const residency = self.capacity.?.materialize(total);
        const header: *Header = @ptrCast(@alignCast(block));
        header.* = .{ .lease = lease, .residency = residency };
        self.live_bytes += total;
        return block + offset;
    }

    fn free(raw: *anyopaque, buf: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *Budget = @ptrCast(@alignCast(raw));
        const offset = alignment.forward(@sizeOf(Header));
        const block = buf.ptr - offset;
        const header: *Header = @ptrCast(@alignCast(block));
        var lease = header.lease;
        var residency = header.residency;
        self.backing.rawFree(block[0 .. offset + buf.len], alignment.max(.of(Header)), ra);
        self.live_bytes -= offset + buf.len;
        residency.release();
        lease.release();
    }

    const vtable: std.mem.Allocator.VTable = .{
        .alloc = alloc,
        .resize = std.mem.Allocator.noResize,
        .remap = std.mem.Allocator.noRemap,
        .free = free,
    };
};

test "managed PJRT metadata batches physical headroom without exempting small allocations" {
    const a = std.testing.allocator;
    var manager = Manager.init(a, @import("../backends/backends.zig").SessionManager.init(a));
    defer manager.deinit();
    const policy = @import("../server/model_manager.zig").ModelLoadPolicy{ .limits = .{ .host_limit_bytes = 1024 * 1024 } };
    try policy.configure(&manager);
    const context = admission.Context{ .allocator = a, .manager = &manager, .policy = policy };
    var failure = std.testing.FailingAllocator.init(a, .{});
    const budget = try Budget.create(failure.allocator(), &context);
    defer budget.destroy();
    const controller = &budget.lifetime.domain.admission;
    const capacity = controller.live_capacity_bytes;
    const pending = controller.live_pending_bytes;
    // An outstanding concurrent reservation prevents a new OS sample and
    // makes the epoch's remaining capacity authoritative.
    controller.live_capacity_bytes = 1;
    controller.live_pending_bytes = 1;
    defer {
        controller.live_capacity_bytes = capacity;
        controller.live_pending_bytes = pending;
    }
    const before = manager.currentAdmissionAmounts();
    try std.testing.expectError(error.OutOfMemory, budget.allocator().alloc(u8, 32 * 1024));
    try std.testing.expectEqual(error.ResourceTemporarilyUnavailable, budget.translate(error.OutOfMemory));
    try std.testing.expectEqualDeep(before, manager.currentAdmissionAmounts());
    controller.live_capacity_bytes = 128 * 1024 + 1;
    const first = try budget.allocator().alloc(u8, 1024);
    const first_pending = controller.live_pending_bytes;
    const increment = manager.currentAdmissionAmounts().host_weight_bytes - before.host_weight_bytes;
    const second = try budget.allocator().alloc(u8, 1024);
    try std.testing.expectEqual(first_pending - increment, controller.live_pending_bytes);
    budget.allocator().free(first);
    budget.allocator().free(second);
    budget.releaseCapacity();
    try std.testing.expectEqual(@as(usize, 1), controller.live_pending_bytes);
    try std.testing.expectEqual(@as(usize, 128 * 1024 + 1), controller.live_capacity_bytes);
    try std.testing.expectEqualDeep(before, manager.currentAdmissionAmounts());
    // When batching is too large, retry exactly the actual allocation size.
    const exact = 1024 + @sizeOf(Budget.Header);
    controller.live_capacity_bytes = exact + 1;
    const small = try budget.allocator().alloc(u8, 1024);
    budget.allocator().free(small);
    budget.releaseCapacity();
    try std.testing.expectEqual(@as(usize, 1), controller.live_pending_bytes);
    try std.testing.expectEqual(exact + 1, controller.live_capacity_bytes);
    controller.live_capacity_bytes = 128 * 1024 + 1;
    failure.fail_index = failure.alloc_index;
    try std.testing.expectError(error.OutOfMemory, budget.allocator().alloc(u8, 1024));
    try std.testing.expectEqual(error.OutOfMemory, budget.translate(error.OutOfMemory));
    budget.releaseCapacity();
    try std.testing.expectEqual(@as(usize, 1), controller.live_pending_bytes);
    try std.testing.expectEqual(@as(usize, 128 * 1024 + 1), controller.live_capacity_bytes);
    try std.testing.expectEqualDeep(before, manager.currentAdmissionAmounts());
}
