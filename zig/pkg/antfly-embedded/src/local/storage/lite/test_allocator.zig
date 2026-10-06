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

const std = @import("std");
const maintenance = @import("../maintenance.zig");
const Allocator = std.mem.Allocator;

/// Test allocator that bounds total live heap usage, and can request cancellation
/// after allocations have started. Uses a caller-owned I/O runtime in these tests.
pub const BudgetAllocator = struct {
    backing: Allocator,
    live: usize = 0,
    peak: usize = 0,
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
