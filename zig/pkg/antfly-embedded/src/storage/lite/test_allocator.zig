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
const maintenance = @import("../maintenance.zig");
const Allocator = std.mem.Allocator;

/// Test allocator that bounds total live heap usage, and can request cancellation
/// after allocations have started. Uses a caller-owned I/O runtime in these tests.
pub const BudgetAllocator = struct {
    backing: Allocator,
    live: usize = 0,
    peak: usize = 0,
    alloc_calls: usize = 0,
    limit: usize = std.math.maxInt(usize),
    cancel: ?*maintenance.CancelToken = null,
    cancel_after: usize = std.math.maxInt(usize),

    pub fn allocator(self: *@This()) Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }

    fn account(self: *@This(), old: usize, new: usize) void {
        self.live = self.live - old + new;
        self.peak = @max(self.peak, self.live);
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        if (self.cancel_after == 0) {
            if (self.cancel) |token| token.request();
        } else self.cancel_after -= 1;
        if (len > self.limit -| self.live) return null;
        const result = self.backing.rawAlloc(len, alignment, ra) orelse return null;
        self.alloc_calls += 1;
        self.account(0, len);
        return result;
    }

    fn resize(ctx: *anyopaque, buf: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) bool {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        if (len > (self.limit -| self.live) + buf.len) return false;
        if (!self.backing.rawResize(buf, alignment, len, ra)) return false;
        self.account(buf.len, len);
        return true;
    }

    fn remap(ctx: *anyopaque, buf: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        if (len > (self.limit -| self.live) + buf.len) return null;
        const result = self.backing.rawRemap(buf, alignment, len, ra) orelse return null;
        self.account(buf.len, len);
        return result;
    }

    fn free(ctx: *anyopaque, buf: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        self.backing.rawFree(buf, alignment, ra);
        self.account(buf.len, 0);
    }
};

/// Make allocation-failure inventories independent of the backing allocator's
/// ability to grow or remap a particular address in place.
pub const NoResizeAllocator = struct {
    backing: Allocator,

    pub fn allocator(self: *@This()) Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }
    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        return self.backing.rawAlloc(len, alignment, ra);
    }
    fn resize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
        return false;
    }
    fn remap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
        return null;
    }
    fn free(ctx: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        self.backing.rawFree(bytes, alignment, ra);
    }
};
