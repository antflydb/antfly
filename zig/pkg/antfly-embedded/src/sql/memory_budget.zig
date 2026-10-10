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
pub fn isExhausted(self: *Budget) bool {
    self.lock();
    defer self.mutex.unlock();
    return self.exhausted or self.admission_exhausted;
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
    if (a.vtable.alloc != alloc) return null;
    const self: *Budget = @ptrCast(@alignCast(a.ptr));
    self.lock();
    const available = self.limit -| if (self.monotonic) self.spent else self.live;
    const backing = self.backing;
    self.mutex.unlock();
    return if (headroom(backing)) |parent| @min(available, parent) else available;
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
    const self: *Budget = @ptrCast(@alignCast(ptr));
    self.lock();
    defer self.mutex.unlock();
    if (!self.admit(len)) return null;
    const result = self.backing.rawAlloc(len, alignment, ra) orelse {
        self.releaseAdmission(len);
        return null;
    };
    self.account(0, len);
    return result;
}
fn resize(ptr: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) bool {
    const self: *Budget = @ptrCast(@alignCast(ptr));
    self.lock();
    defer self.mutex.unlock();
    if (!self.admit(len -| bytes.len)) return false;
    if (!self.backing.rawResize(bytes, alignment, len, ra)) {
        self.releaseAdmission(len -| bytes.len);
        return false;
    }
    self.releaseAdmission(bytes.len -| len);
    self.account(bytes.len, len);
    return true;
}
fn remap(ptr: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
    const self: *Budget = @ptrCast(@alignCast(ptr));
    self.lock();
    defer self.mutex.unlock();
    if (!self.admit(len -| bytes.len)) return null;
    const result = if (self.monotonic) blk: {
        // A moving remap may leave the old allocation in a parent arena.
        // Force allocator fallback to reserve the complete replacement.
        if (!self.backing.rawResize(bytes, alignment, len, ra)) {
            self.releaseAdmission(len -| bytes.len);
            return null;
        }
        break :blk bytes.ptr;
    } else self.backing.rawRemap(bytes, alignment, len, ra) orelse {
        self.releaseAdmission(len -| bytes.len);
        return null;
    };
    self.releaseAdmission(bytes.len -| len);
    self.account(bytes.len, len);
    return result;
}
fn free(ptr: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
    const self: *Budget = @ptrCast(@alignCast(ptr));
    self.lock();
    defer self.mutex.unlock();
    self.backing.rawFree(bytes, alignment, ra);
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
