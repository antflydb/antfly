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

//! Per-connection allocation cap shared by messages, prepared statements,
//! portals and backend response arenas. No unbounded connection-lifetime arena.
const std = @import("std");

pub const Budget = struct {
    child: std.mem.Allocator,
    limit: usize,
    used: usize = 0,

    pub fn allocator(self: *Budget) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }

    fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *Budget = @ptrCast(@alignCast(raw));
        if (len > self.limit - self.used) return null;
        const result = self.child.rawAlloc(len, alignment, ret_addr) orelse return null;
        self.used += len;
        return result;
    }

    fn resize(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *Budget = @ptrCast(@alignCast(raw));
        if (new_len > memory.len and new_len - memory.len > self.limit - self.used) return false;
        if (!self.child.rawResize(memory, alignment, new_len, ret_addr)) return false;
        self.used = self.used - memory.len + new_len;
        return true;
    }

    fn remap(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *Budget = @ptrCast(@alignCast(raw));
        if (new_len > memory.len and new_len - memory.len > self.limit - self.used) return null;
        const result = self.child.rawRemap(memory, alignment, new_len, ret_addr) orelse return null;
        self.used = self.used - memory.len + new_len;
        return result;
    }

    fn free(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *Budget = @ptrCast(@alignCast(raw));
        self.child.rawFree(memory, alignment, ret_addr);
        self.used -= memory.len;
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
