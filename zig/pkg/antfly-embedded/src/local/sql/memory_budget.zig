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
exhausted: bool = false,
mutex: std.atomic.Mutex = .unlocked,

fn lock(self: *Budget) void {
    while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
}

pub fn allocator(self: *Budget) std.mem.Allocator {
    return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
}
fn admit(self: *Budget, growth: usize) bool {
    if (growth > self.limit - self.live) {
        self.exhausted = true;
        return false;
    }
    return true;
}
fn account(self: *Budget, old: usize, new: usize) void {
    self.live = self.live - old + new;
    self.peak = @max(self.peak, self.live);
}
fn alloc(ptr: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
    const self: *Budget = @ptrCast(@alignCast(ptr));
    self.lock();
    defer self.mutex.unlock();
    if (!self.admit(len)) return null;
    const result = self.backing.rawAlloc(len, alignment, ra) orelse return null;
    self.account(0, len);
    return result;
}
fn resize(ptr: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) bool {
    const self: *Budget = @ptrCast(@alignCast(ptr));
    self.lock();
    defer self.mutex.unlock();
    if (!self.admit(len -| bytes.len)) return false;
    if (!self.backing.rawResize(bytes, alignment, len, ra)) return false;
    self.account(bytes.len, len);
    return true;
}
fn remap(ptr: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
    const self: *Budget = @ptrCast(@alignCast(ptr));
    self.lock();
    defer self.mutex.unlock();
    if (!self.admit(len -| bytes.len)) return null;
    const result = self.backing.rawRemap(bytes, alignment, len, ra) orelse return null;
    self.account(bytes.len, len);
    return result;
}
fn free(ptr: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
    const self: *Budget = @ptrCast(@alignCast(ptr));
    self.lock();
    defer self.mutex.unlock();
    self.backing.rawFree(bytes, alignment, ra);
    self.account(bytes.len, 0);
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
