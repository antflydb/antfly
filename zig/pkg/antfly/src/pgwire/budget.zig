// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Per-connection allocation cap shared by messages, prepared statements,
//! portals and backend response arenas. No unbounded connection-lifetime arena.
const std = @import("std");

/// Aggregate allocations through all connection budgets on one listener.
/// The owner outlives joined sessions; backend-global plan/cache allocations
/// retain their own budgets and are not included in this counter.
pub const Shared = @import("../common/allocation_budget.zig").Shared;

pub const Budget = struct {
    child: std.mem.Allocator,
    limit: usize,
    used: usize = 0,
    /// Optional independent aggregate retained-state charge.
    shared: ?*Shared = null,

    pub fn allocator(self: *Budget) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }

    fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *Budget = @ptrCast(@alignCast(raw));
        if (len > self.limit - self.used) return null;
        if (self.shared) |shared| if (!shared.reserve(len)) return null;
        const result = self.child.rawAlloc(len, alignment, ret_addr) orelse {
            if (self.shared) |shared| shared.release(len);
            return null;
        };
        self.used += len;
        return result;
    }

    fn resize(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *Budget = @ptrCast(@alignCast(raw));
        if (new_len > memory.len and new_len - memory.len > self.limit - self.used) return false;
        const growth = new_len -| memory.len;
        if (self.shared) |shared| if (!shared.reserve(growth)) return false;
        if (!self.child.rawResize(memory, alignment, new_len, ret_addr)) {
            if (self.shared) |shared| shared.release(growth);
            return false;
        }
        if (self.shared) |shared| shared.release(memory.len -| new_len);
        self.used = self.used - memory.len + new_len;
        return true;
    }

    fn remap(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *Budget = @ptrCast(@alignCast(raw));
        if (new_len > memory.len and new_len - memory.len > self.limit - self.used) return null;
        const growth = new_len -| memory.len;
        if (self.shared) |shared| if (!shared.reserve(growth)) return null;
        const result = self.child.rawRemap(memory, alignment, new_len, ret_addr) orelse {
            if (self.shared) |shared| shared.release(growth);
            return null;
        };
        if (self.shared) |shared| shared.release(memory.len -| new_len);
        self.used = self.used - memory.len + new_len;
        return result;
    }

    fn free(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *Budget = @ptrCast(@alignCast(raw));
        self.child.rawFree(memory, alignment, ret_addr);
        self.used -= memory.len;
        if (self.shared) |shared| shared.release(memory.len);
    }
};

test "pgwire memory admission bounds and reclaims connection storage" {
    var budget = Budget{ .child = std.testing.allocator, .limit = 16 };
    const alloc = budget.allocator();
    const first = try alloc.alloc(u8, 12);
    try std.testing.expectError(error.OutOfMemory, alloc.alloc(u8, 5));
    alloc.free(first);
    const second = try alloc.alloc(u8, 16);
    alloc.free(second);
    try std.testing.expectEqual(@as(usize, 0), budget.used);
}

test "pgwire shared session budget bounds aggregate connections and reclaims capacity" {
    var shared: Shared = .{ .child = std.testing.allocator, .limit = 20 };
    var first: Budget = .{ .child = shared.allocator(), .limit = 16 };
    var second: Budget = .{ .child = shared.allocator(), .limit = 16 };
    const a = try first.allocator().alloc(u8, 12);
    try std.testing.expectError(error.OutOfMemory, second.allocator().alloc(u8, 9));
    const b = try second.allocator().alloc(u8, 8);
    try std.testing.expectEqual(@as(usize, 20), shared.used.load(.acquire));
    try std.testing.expect(!first.allocator().resize(a, 13));
    second.allocator().free(b);
    first.allocator().free(a);
    const c = try second.allocator().alloc(u8, 16);
    second.allocator().free(c);
    try std.testing.expectEqual(@as(usize, 0), shared.used.load(.acquire));
}

test "pgwire shared session budget rolls back a failed backing allocation" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var shared: Shared = .{ .child = failing.allocator(), .limit = 20 };
    try std.testing.expectError(error.OutOfMemory, shared.allocator().alloc(u8, 12));
    try std.testing.expectEqual(@as(usize, 0), shared.used.load(.acquire));
}

test "pgwire retained budget independently charges aggregate and connection allocation" {
    var memory: Shared = .{ .child = std.testing.allocator, .limit = 256 };
    var retained: Shared = .{ .child = std.testing.allocator, .limit = 20 };
    var connection: Budget = .{ .child = memory.allocator(), .limit = 128 };
    var a: Budget = .{ .child = connection.allocator(), .limit = 16, .shared = &retained };
    var b: Budget = .{ .child = connection.allocator(), .limit = 16, .shared = &retained };
    const first = try a.allocator().alloc(u8, 12);
    try std.testing.expectError(error.OutOfMemory, b.allocator().alloc(u8, 9));
    const second = try b.allocator().alloc(u8, 8);
    try std.testing.expect(!a.allocator().resize(first, 13));
    // Transient response/frame memory remains available for errors and controls.
    const transient = try connection.allocator().alloc(u8, 32);
    try std.testing.expectEqual(@as(usize, 20), retained.used.load(.acquire));
    b.allocator().free(second);
    a.allocator().free(first);
    connection.allocator().free(transient);
    try std.testing.expectEqual(@as(usize, 0), retained.used.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), memory.used.load(.acquire));
}

test "pgwire retained budget rolls back failed backing allocation" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var retained: Shared = .{ .child = std.testing.allocator, .limit = 100 };
    var budget: Budget = .{ .child = failing.allocator(), .limit = 100, .shared = &retained };
    try std.testing.expectError(error.OutOfMemory, budget.allocator().alloc(u8, 20));
    try std.testing.expectEqual(@as(usize, 0), retained.used.load(.acquire));
}
