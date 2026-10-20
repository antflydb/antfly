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

//! Request-local allocation admission, including arena capacity and page data.
//! The owner must have a stable address and outlive every allocated object.
const std = @import("std");
const Budget = @This();
backing: std.mem.Allocator,
limit: usize,
live: usize = 0,
peak: usize = 0,
/// Request regions may retain freed backing buffers. In this mode frees and
/// shrinks never refund admission, and remap may only resize in place.
monotonic: bool = false,
spent: usize = 0,
exhausted: bool = false,
mutex: std.atomic.Mutex = .unlocked,
/// Optional shared admission for the live allocations of an active operation.
/// Reservations grow with actual allocator capacity, never a size estimate.
admission: ?struct { ptr: *anyopaque, reserve: *const fn (*anyopaque, usize) bool, release: *const fn (*anyopaque, usize) void } = null,
admission_exhausted: bool = false,

pub fn finishAdmission(self: *Budget) void {
    self.lock();
    defer self.mutex.unlock();
    if (self.admission) |owner| owner.release(owner.ptr, self.live);
    self.admission = null;
}

fn lock(self: *Budget) void {
    while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
}

pub fn allocator(self: *Budget) std.mem.Allocator {
    return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
}
/// Reusable child owners distinguish local failed admission from a parent's
/// historical failure; restoring parent headroom can admit healthy new work.
pub fn isLocallyExhausted(self: *Budget) bool {
    self.lock();
    defer self.mutex.unlock();
    return self.exhausted or self.admission_exhausted;
}
/// Temporary owners may bypass standard arenas without bypassing this budget
/// or any enclosing budget/failure allocator. Returned objects must be freed
/// through this allocator, never through the original region allocator.
pub fn reclaimingAllocator(a: std.mem.Allocator) std.mem.Allocator {
    return reclaiming(a, 0);
}
fn reclaiming(a: std.mem.Allocator, depth: usize) std.mem.Allocator {
    if (depth == 16) return a;
    const unwrapped = @import("antfly_platform").allocator.reclaimingScratch(a);
    if (unwrapped.ptr != a.ptr or unwrapped.vtable != a.vtable) return reclaiming(unwrapped, depth + 1);
    if (a.vtable.alloc != alloc) return a;
    const self: *Budget = @ptrCast(@alignCast(a.ptr));
    const backing = reclaiming(self.backing, depth + 1);
    if (backing.ptr == self.backing.ptr and backing.vtable == self.backing.vtable) return a;
    return .{ .ptr = self, .vtable = &.{ .alloc = scratchAlloc, .resize = scratchResize, .remap = scratchRemap, .free = scratchFree } };
}
fn scratchAlloc(p: *anyopaque, n: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
    return allocate(true, p, n, alignment, ra);
}
fn scratchResize(p: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, n: usize, ra: usize) bool {
    return resizeMode(true, p, bytes, alignment, n, ra);
}
fn scratchRemap(p: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, n: usize, ra: usize) ?[*]u8 {
    return remapMode(true, p, bytes, alignment, n, ra);
}
fn scratchFree(p: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
    freeMode(true, p, bytes, alignment, ra);
}
pub fn isExhausted(self: *Budget) bool {
    self.lock();
    const exhausted = self.exhausted or self.admission_exhausted;
    const backing = self.backing;
    self.mutex.unlock();
    // A child can be below its own limit when shared parent admission fails.
    // Preserve genuine allocator OOMs: only known Budget owners carry policy.
    return exhausted or allocatorExhausted(backing);
}
fn allocatorExhausted(a: std.mem.Allocator) bool {
    if (@import("parallel_scheduler.zig").LockedAllocator.parentAllocator(a)) |parent| return allocatorExhausted(parent);
    if (@import("spill.zig").Manager.parentAllocator(a)) |parent| return allocatorExhausted(parent);
    if (a.vtable.alloc != alloc and a.vtable.alloc != scratchAlloc) return false;
    const parent: *Budget = @ptrCast(@alignCast(a.ptr));
    const backing = if (a.vtable.alloc == scratchAlloc) reclaimingAllocator(parent.backing) else parent.backing;
    return parent.isLocallyExhausted() or allocatorExhausted(backing);
}
/// Conservative region footprint, including intermediate buffers retained by
/// an enclosing arena. Ordinary reclaiming owners retain peak-live accounting.
pub fn footprint(self: *Budget) usize {
    self.lock();
    defer self.mutex.unlock();
    return if (self.monotonic) self.spent else self.peak;
}
/// Live headroom of this allocator's budget chain, when known. Unknown backing
/// allocators provide no extra bound; they never imply unlimited admission.
pub fn headroom(a: std.mem.Allocator) ?usize {
    if (@import("parallel_scheduler.zig").LockedAllocator.parentAllocator(a)) |parent| return headroom(parent);
    if (@import("spill.zig").Manager.parentAllocator(a)) |parent| return headroom(parent);
    if (a.vtable.alloc != alloc and a.vtable.alloc != scratchAlloc) return null;
    const self: *Budget = @ptrCast(@alignCast(a.ptr));
    self.lock();
    const available = self.limit -| if (self.monotonic) self.spent else self.live;
    const backing = self.backing;
    self.mutex.unlock();
    // Scratch forwards through reclaimed arena backings; admission introspection
    // must follow that same allocator path rather than stopping at the region.
    const actual = if (a.vtable.alloc == scratchAlloc) reclaimingAllocator(backing) else backing;
    return if (headroom(actual)) |parent| @min(available, parent) else available;
}
/// Inspect only this known budget layer so other allocation owners can compose
/// complete headroom chains without treating opaque backings as unbounded.
/// External reservation owners have no locally provable spare capacity.
pub fn optionalLocalHeadroom(a: std.mem.Allocator) ?struct { available: usize, backing: std.mem.Allocator } {
    if (a.vtable.alloc != alloc and a.vtable.alloc != scratchAlloc) return null;
    const self: *Budget = @ptrCast(@alignCast(a.ptr));
    self.lock();
    defer self.mutex.unlock();
    if (self.admission != null) return null;
    return .{ .available = self.limit -| if (self.monotonic) self.spent else self.live, .backing = self.backing };
}
/// Optional acceleration must not speculate through an opaque quota wrapper.
/// Recognize only our complete budget chain and standard unbounded backings;
/// mandatory allocations continue to use the normal allocator error contract.
pub fn optionalHeadroom(a: std.mem.Allocator) ?usize {
    if (optionalLocalHeadroom(a)) |layer| {
        const parent = optionalHeadroom(layer.backing) orelse return null;
        return @min(layer.available, parent);
    }
    if (a.vtable == std.heap.page_allocator.vtable) return std.math.maxInt(usize);
    if (comptime !@import("builtin").single_threaded) {
        if (a.vtable == std.heap.smp_allocator.vtable) return std.math.maxInt(usize);
    }
    if (comptime @import("builtin").link_libc) {
        if (a.vtable == std.heap.c_allocator.vtable) return std.math.maxInt(usize);
    }
    return null;
}
fn admit(self: *Budget, growth: usize) bool {
    if (growth > self.limit -| if (self.monotonic) self.spent else self.live) {
        self.exhausted = true;
        return false;
    }
    if (self.admission) |owner| if (!owner.reserve(owner.ptr, growth)) {
        self.admission_exhausted = true;
        return false;
    };
    return true;
}
fn releaseAdmission(self: *Budget, bytes: usize) void {
    if (self.admission) |owner| owner.release(owner.ptr, bytes);
}
fn account(self: *Budget, old: usize, new: usize) void {
    if (self.monotonic) self.spent += new -| old;
    self.live = self.live - old + new;
    self.peak = @max(self.peak, self.live);
}
fn alloc(ptr: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
    return allocate(false, ptr, len, alignment, ra);
}
fn allocate(comptime scratch: bool, ptr: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
    const self: *Budget = @ptrCast(@alignCast(ptr));
    self.lock();
    defer self.mutex.unlock();
    const backing = if (scratch) reclaimingAllocator(self.backing) else self.backing;
    if (!self.admit(len)) return null;
    const result = backing.rawAlloc(len, alignment, ra) orelse {
        self.releaseAdmission(len);
        return null;
    };
    self.account(0, len);
    return result;
}
fn resize(ptr: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) bool {
    return resizeMode(false, ptr, bytes, alignment, len, ra);
}
fn resizeMode(comptime scratch: bool, ptr: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) bool {
    const self: *Budget = @ptrCast(@alignCast(ptr));
    self.lock();
    defer self.mutex.unlock();
    const backing = if (scratch) reclaimingAllocator(self.backing) else self.backing;
    if (!self.admit(len -| bytes.len)) return false;
    if (!backing.rawResize(bytes, alignment, len, ra)) {
        self.releaseAdmission(len -| bytes.len);
        return false;
    }
    self.releaseAdmission(bytes.len -| len);
    self.account(bytes.len, len);
    return true;
}
fn remap(ptr: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
    return remapMode(false, ptr, bytes, alignment, len, ra);
}
fn remapMode(comptime scratch: bool, ptr: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
    const self: *Budget = @ptrCast(@alignCast(ptr));
    self.lock();
    defer self.mutex.unlock();
    const backing = if (scratch) reclaimingAllocator(self.backing) else self.backing;
    if (!self.admit(len -| bytes.len)) return null;
    const result = if (self.monotonic) blk: {
        // A moving remap may leave the old allocation in a parent arena.
        // Force allocator fallback to reserve the complete replacement.
        if (!backing.rawResize(bytes, alignment, len, ra)) {
            self.releaseAdmission(len -| bytes.len);
            return null;
        }
        break :blk bytes.ptr;
    } else backing.rawRemap(bytes, alignment, len, ra) orelse {
        self.releaseAdmission(len -| bytes.len);
        return null;
    };
    self.releaseAdmission(bytes.len -| len);
    self.account(bytes.len, len);
    return result;
}
fn free(ptr: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
    return freeMode(false, ptr, bytes, alignment, ra);
}
fn freeMode(comptime scratch: bool, ptr: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
    const self: *Budget = @ptrCast(@alignCast(ptr));
    self.lock();
    defer self.mutex.unlock();
    const backing = if (scratch) reclaimingAllocator(self.backing) else self.backing;
    backing.rawFree(bytes, alignment, ra);
    self.releaseAdmission(bytes.len);
    self.account(bytes.len, 0);
}

test "SQL monotonic region budget charges freed and replacement buffers without refund" {
    var region = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer region.deinit();
    var budget: Budget = .{ .backing = region.allocator(), .limit = 256, .monotonic = true };
    const a = budget.allocator();
    const first = try a.alloc(u8, 100);
    const separator = try a.alloc(u8, 10);
    @memset(first, 42);
    // Not the arena tail: realloc must own a complete replacement, not merely
    // charge its 20-byte growth while retaining the original 100 bytes.
    const replacement = try a.realloc(first, 120);
    for (replacement[0..100]) |byte| try std.testing.expectEqual(@as(u8, 42), byte);
    try std.testing.expectEqual(@as(usize, 230), budget.footprint());
    a.free(separator);
    a.free(replacement);
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    try std.testing.expectEqual(@as(?usize, 26), headroom(a));
    try std.testing.expectError(error.OutOfMemory, a.alloc(u8, 27));
    const last = try a.alloc(u8, 26);
    a.free(last);
    try std.testing.expectEqual(@as(usize, 256), budget.footprint());
    try std.testing.expectEqual(@as(?usize, 0), headroom(a));
}

test "SQL monotonic region budget admits in-place growth and never refunds shrinking" {
    var buffer: [256]u8 = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&buffer);
    var budget: Budget = .{ .backing = fixed.allocator(), .limit = 128, .monotonic = true };
    const a = budget.allocator();
    const original = try a.alloc(u8, 64);
    const grown = try a.realloc(original, 96);
    try std.testing.expectEqual(original.ptr, grown.ptr);
    try std.testing.expectEqual(@as(usize, 96), budget.footprint());
    const shrunk = try a.realloc(grown, 32);
    try std.testing.expectEqual(@as(usize, 96), budget.footprint());
    a.free(shrunk);
    const final = try a.alloc(u8, 32);
    a.free(final);
    try std.testing.expectEqual(@as(usize, 128), budget.footprint());
}

test "SQL nested memory budget headroom respects the live shared parent" {
    var parent: Budget = .{ .backing = std.testing.allocator, .limit = 4096 };
    var child: Budget = .{ .backing = parent.allocator(), .limit = 8192 };
    const a = child.allocator();
    try std.testing.expectEqual(@as(?usize, 4096), headroom(a));
    const bytes = try a.alloc(u8, 128);
    try std.testing.expectEqual(@as(?usize, 3968), headroom(a));
    a.free(bytes);
    try std.testing.expectEqual(@as(?usize, 4096), headroom(a));
    try std.testing.expect(headroom(std.testing.allocator) == null);
}

test "SQL memory budget rejects before allocation and reclaims page capacity" {
    var budget: Budget = .{ .backing = std.testing.allocator, .limit = 128 };
    const a = budget.allocator();
    const first = try a.alloc(u8, 100);
    try std.testing.expectError(error.OutOfMemory, a.alloc(u8, 29));
    try std.testing.expectEqual(@as(usize, 100), budget.live);
    a.free(first);
    const second = try a.alloc(u8, 128);
    a.free(second);
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    try std.testing.expectEqual(@as(usize, 128), budget.peak);
}

test "SQL memory headroom follows synchronization and spill allocator parents" {
    const a = std.testing.allocator;
    var parent: Budget = .{ .backing = a, .limit = 1024 };
    var locked: @import("parallel_scheduler.zig").LockedAllocator = .{ .backing = parent.allocator() };
    var manager: @import("spill.zig").Manager = .{ .alloc = locked.allocator(), .io = std.testing.io, .context = undefined, .checkpoint = struct {
        fn check(_: *anyopaque) !void {}
    }.check };
    defer manager.deinit();
    const reservation = try parent.allocator().alloc(u8, 512);
    defer parent.allocator().free(reservation);
    try std.testing.expectEqual(@as(?usize, 512), headroom(manager.allocator()));
    var child: Budget = .{ .backing = manager.allocator(), .limit = 2048 };
    const bytes = try child.allocator().alloc(u8, 128);
    try std.testing.expectEqual(@as(?usize, 384), headroom(child.allocator()));
    child.allocator().free(bytes);
    try std.testing.expectEqual(@as(?usize, 512), headroom(locked.allocator()));
    try std.testing.expect(headroom(a) == null);
}

test "SQL reclaimable scratch preserves nested quotas and failure allocators across regions" {
    const Fixture = struct {
        fn run(a: std.mem.Allocator) !void {
            var parent: Budget = .{ .backing = a, .limit = 4096 };
            var region = std.heap.ArenaAllocator.init(parent.allocator());
            defer region.deinit();
            var child: Budget = .{ .backing = region.allocator(), .limit = 1024 };
            const scratch = reclaimingAllocator(child.allocator());
            var bytes = try scratch.alloc(u8, 512);
            defer scratch.free(bytes);
            try std.testing.expectEqual(@as(usize, 512), child.live);
            try std.testing.expectEqual(child.live, parent.live);
            bytes = try scratch.realloc(bytes, 768);
            try std.testing.expectEqual(@as(usize, 768), child.live);
            try std.testing.expectEqual(child.live, parent.live);
            try std.testing.expectError(error.OutOfMemory, scratch.alloc(u8, 512));
            // A shared parent also retains admission authority after unwrapping.
            parent.limit = parent.live;
            try std.testing.expectError(error.OutOfMemory, scratch.alloc(u8, 1));
            try std.testing.expectEqual(@as(usize, 768), child.live);
        }
    };
    try Fixture.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "SQL memory budget reports parent admission without misclassifying allocator OOM" {
    var parent: Budget = .{ .backing = std.testing.allocator, .limit = 32 };
    var child: Budget = .{ .backing = parent.allocator(), .limit = 128 };
    const bytes = try child.allocator().alloc(u8, 16);
    defer child.allocator().free(bytes);
    try std.testing.expectError(error.OutOfMemory, child.allocator().alloc(u8, 17));
    try std.testing.expect(child.isExhausted());
    try std.testing.expect(!child.isLocallyExhausted());
    try std.testing.expect(!child.exhausted);
    try std.testing.expectEqual(@as(usize, 16), parent.live);
    var empty: [0]u8 = .{};
    var fixed = std.heap.FixedBufferAllocator.init(&empty);
    var allocator_oom: Budget = .{ .backing = fixed.allocator(), .limit = 128 };
    try std.testing.expectError(error.OutOfMemory, allocator_oom.allocator().alloc(u8, 1));
    try std.testing.expect(!allocator_oom.isExhausted());
    // Scheduler and spill wrappers preserve the same parent policy evidence.
    var locked: @import("parallel_scheduler.zig").LockedAllocator = .{ .backing = parent.allocator() };
    var manager: @import("spill.zig").Manager = .{ .alloc = locked.allocator(), .io = std.testing.io, .context = undefined, .checkpoint = struct {
        fn check(_: *anyopaque) !void {}
    }.check };
    defer manager.deinit();
    var wrapped: Budget = .{ .backing = manager.allocator(), .limit = 128 };
    try std.testing.expectError(error.OutOfMemory, wrapped.allocator().alloc(u8, 17));
    try std.testing.expect(wrapped.isExhausted());
    try std.testing.expect(!wrapped.isLocallyExhausted());
    try std.testing.expect(!wrapped.exhausted);
    locked.backing = fixed.allocator();
    try std.testing.expectError(error.OutOfMemory, wrapped.allocator().alloc(u8, 1));
    try std.testing.expect(!wrapped.isExhausted());
}

test "SQL reclaimable scratch headroom follows quota owners behind regions" {
    var parent: Budget = .{ .backing = std.testing.allocator, .limit = 8192 };
    var arena = std.heap.ArenaAllocator.init(parent.allocator());
    defer arena.deinit();
    var child: Budget = .{ .backing = arena.allocator(), .limit = 4096 };
    const scratch = reclaimingAllocator(child.allocator());
    var nested: Budget = .{ .backing = scratch, .limit = 8192 };
    const bytes = try nested.allocator().alloc(u8, 512);
    defer nested.allocator().free(bytes);
    parent.limit = parent.live + 64;
    try std.testing.expectEqual(@as(?usize, 64), headroom(scratch));
    try std.testing.expectEqual(@as(?usize, 64), headroom(nested.allocator()));
    try std.testing.expectError(error.OutOfMemory, nested.allocator().alloc(u8, 65));
    try std.testing.expect(nested.isExhausted());
    try std.testing.expectEqual(@as(usize, 512), parent.live);
}
