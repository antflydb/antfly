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
//! portals and backend response arenas. Parallel SQL workers share this cap;
//! admission, child allocation and reclamation are serialized together.
//! The budget must keep its address until all workers and response leases close.
const std = @import("std");

pub const Budget = struct {
    child: std.mem.Allocator,
    limit: usize,
    used: usize = 0,
    mutex: std.atomic.Mutex = .unlocked,

    fn lock(self: *Budget) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    pub fn allocator(self: *Budget) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }

    fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *Budget = @ptrCast(@alignCast(raw));
        self.lock();
        defer self.mutex.unlock();
        if (len > self.limit - self.used) return null;
        const result = self.child.rawAlloc(len, alignment, ret_addr) orelse return null;
        self.used += len;
        return result;
    }

    fn resize(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *Budget = @ptrCast(@alignCast(raw));
        self.lock();
        defer self.mutex.unlock();
        if (new_len > memory.len and new_len - memory.len > self.limit - self.used) return false;
        if (!self.child.rawResize(memory, alignment, new_len, ret_addr)) return false;
        self.used = self.used - memory.len + new_len;
        return true;
    }

    fn remap(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *Budget = @ptrCast(@alignCast(raw));
        self.lock();
        defer self.mutex.unlock();
        if (new_len > memory.len and new_len - memory.len > self.limit - self.used) return null;
        const result = self.child.rawRemap(memory, alignment, new_len, ret_addr) orelse return null;
        self.used = self.used - memory.len + new_len;
        return result;
    }

    fn free(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *Budget = @ptrCast(@alignCast(raw));
        self.lock();
        defer self.mutex.unlock();
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

test "pgwire nested connection budgets serialize concurrent allocation and reclamation" {
    var parent: Budget = .{ .child = std.testing.allocator, .limit = 2048 };
    var budget: Budget = .{ .child = parent.allocator(), .limit = 1024 };
    const Worker = struct {
        a: std.mem.Allocator,
        failure: std.atomic.Value(bool) = .init(false),
        fn run(self: *@This()) void {
            for (0..1024) |_| {
                const initial = self.a.alloc(u8, 64) catch {
                    self.failure.store(true, .release);
                    return;
                };
                const grown = self.a.realloc(initial, 128) catch {
                    self.a.free(initial);
                    self.failure.store(true, .release);
                    return;
                };
                const shrunk = self.a.realloc(grown, 32) catch {
                    self.a.free(grown);
                    self.failure.store(true, .release);
                    return;
                };
                self.a.free(shrunk);
                if (self.a.alloc(u8, 2049)) |invalid| {
                    self.a.free(invalid);
                    self.failure.store(true, .release);
                    return;
                } else |err| {
                    if (err != error.OutOfMemory) {
                        self.failure.store(true, .release);
                        return;
                    }
                }
            }
        }
    };
    var worker: Worker = .{ .a = budget.allocator() };
    var threads: [4]std.Thread = undefined;
    var started: usize = 0;
    {
        defer for (threads[0..started]) |thread| thread.join();
        for (&threads) |*thread| {
            thread.* = try std.Thread.spawn(.{}, Worker.run, .{&worker});
            started += 1;
        }
    }
    try std.testing.expect(!worker.failure.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), budget.used);
    try std.testing.expectEqual(@as(usize, 0), parent.used);
}
