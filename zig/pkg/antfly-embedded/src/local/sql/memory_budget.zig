// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Request-local allocation admission, including arena capacity and page data.
//! The owner must have a stable address and outlive every allocated object.
const std = @import("std");
const Budget = @This();
backing: std.mem.Allocator,
limit: usize,
live: usize = 0,
peak: usize = 0,
exhausted: bool = false,

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
    if (!self.admit(len)) return null;
    const result = self.backing.rawAlloc(len, alignment, ra) orelse return null;
    self.account(0, len);
    return result;
}
fn resize(ptr: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) bool {
    const self: *Budget = @ptrCast(@alignCast(ptr));
    if (!self.admit(len -| bytes.len)) return false;
    if (!self.backing.rawResize(bytes, alignment, len, ra)) return false;
    self.account(bytes.len, len);
    return true;
}
fn remap(ptr: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
    const self: *Budget = @ptrCast(@alignCast(ptr));
    if (!self.admit(len -| bytes.len)) return null;
    const result = self.backing.rawRemap(bytes, alignment, len, ra) orelse return null;
    self.account(bytes.len, len);
    return result;
}
fn free(ptr: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
    const self: *Budget = @ptrCast(@alignCast(ptr));
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
