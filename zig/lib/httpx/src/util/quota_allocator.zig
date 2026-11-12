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

/// A scoped live-byte quota with no allocation headers. Successful allocations
/// may be released through the backing allocator after this scope ends.
pub const QuotaAllocator = struct {
    backing: std.mem.Allocator,
    limit: usize,
    live: usize = 0,
    exceeded: bool = false,
    mutex: std.atomic.Mutex = .unlocked,

    pub fn allocator(self: *@This()) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }
    const vtable: std.mem.Allocator.VTable = .{ .alloc = alloc, .resize = resize, .remap = remap, .free = free };
    fn lock(self: *@This()) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    fn permit(self: *@This(), additional: usize) bool {
        if (additional <= self.limit -| self.live) return true;
        self.exceeded = true;
        return false;
    }
    fn alloc(raw: *anyopaque, n: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.lock();
        defer self.mutex.unlock();
        if (!self.permit(n)) return null;
        const result = self.backing.rawAlloc(n, alignment, ra) orelse return null;
        self.live += n;
        return result;
    }
    fn resize(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, n: usize, ra: usize) bool {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.lock();
        defer self.mutex.unlock();
        if (!self.permit(n -| bytes.len)) return false;
        if (!self.backing.rawResize(bytes, alignment, n, ra)) return false;
        self.live = self.live - bytes.len + n;
        return true;
    }
    fn remap(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, n: usize, ra: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.lock();
        defer self.mutex.unlock();
        if (!self.permit(n -| bytes.len)) return null;
        const result = self.backing.rawRemap(bytes, alignment, n, ra) orelse return null;
        self.live = self.live - bytes.len + n;
        return result;
    }
    fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.lock();
        defer self.mutex.unlock();
        self.backing.rawFree(bytes, alignment, ra);
        self.live -= bytes.len;
    }
};

test "quota retains capacity on rejected growth and releases ownership" {
    var quota = QuotaAllocator{ .backing = std.testing.allocator, .limit = 192 };
    const a = quota.allocator();
    var bytes = try a.alloc(u8, 64);
    bytes = try a.realloc(bytes, 80);
    try std.testing.expectError(error.OutOfMemory, a.alloc(u8, 113));
    try std.testing.expectEqual(@as(usize, 80), quota.live);
    a.free(bytes);
    try std.testing.expectEqual(@as(usize, 0), quota.live);
    const exact = try a.alloc(u8, 192);
    a.free(exact);
    try std.testing.expectEqual(@as(usize, 0), quota.live);
}
