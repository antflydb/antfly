// Copyright 2026 Antfly, Inc.
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
const builtin = @import("builtin");
const platform = @import("antfly_platform");

pub fn Queue(comptime Item: type) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        items: std.ArrayListUnmanaged(Item) = .empty,
        mutex: std.atomic.Mutex = .unlocked,
        worker: ?std.Thread = null,
        stop_worker: bool = false,
        process_ctx: *anyopaque,
        process_fn: *const fn (ctx: *anyopaque, item: Item) void,
        priority_fn: ?*const fn (item: Item) u64 = null,

        pub fn init(
            allocator: std.mem.Allocator,
            process_ctx: *anyopaque,
            process_fn: *const fn (ctx: *anyopaque, item: Item) void,
        ) Self {
            return .{
                .allocator = allocator,
                .process_ctx = process_ctx,
                .process_fn = process_fn,
            };
        }

        pub fn initWithPriority(
            allocator: std.mem.Allocator,
            process_ctx: *anyopaque,
            process_fn: *const fn (ctx: *anyopaque, item: Item) void,
            priority_fn: *const fn (item: Item) u64,
        ) Self {
            var self = init(allocator, process_ctx, process_fn);
            self.priority_fn = priority_fn;
            return self;
        }

        pub fn deinit(self: *Self) void {
            self.stop();
            self.items.deinit(self.allocator);
        }

        pub fn mutexPtr(self: *Self) *std.atomic.Mutex {
            return &self.mutex;
        }

        pub fn lock(self: *Self) void {
            while (!self.mutex.tryLock()) {
                platform.time.yieldBriefly();
            }
        }

        pub fn unlock(self: *Self) void {
            self.mutex.unlock();
        }

        pub fn signal(self: *Self) void {
            _ = self;
        }

        pub fn appendLocked(self: *Self, item: Item) !void {
            try self.items.append(self.allocator, item);
        }

        pub fn start(self: *Self) !void {
            if (builtin.is_test or self.worker != null) return;
            self.stop_worker = false;
            self.worker = try std.Thread.spawn(.{}, workerMain, .{self});
        }

        pub fn stop(self: *Self) void {
            if (self.worker) |worker| {
                self.lock();
                self.stop_worker = true;
                self.unlock();
                worker.join();
                self.worker = null;
                self.stop_worker = false;
            }
        }

        pub fn drainBudget(self: *Self, max_items: usize) void {
            self.lock();
            defer self.unlock();
            if (self.worker != null) {
                return;
            }
            self.drainBudgetLocked(max_items);
        }

        fn workerMain(self: *Self) void {
            while (true) {
                self.lock();
                if (self.stop_worker) {
                    self.unlock();
                    return;
                }
                if (self.items.items.len > 0) {
                    const item = self.items.orderedRemove(self.pickIndexLocked());
                    self.process_fn(self.process_ctx, item);
                    self.unlock();
                    continue;
                }
                self.unlock();
                platform.time.yieldBriefly();
            }
        }

        fn drainBudgetLocked(self: *Self, max_items: usize) void {
            var remaining = @min(max_items, self.items.items.len);
            while (remaining > 0 and self.items.items.len > 0) : (remaining -= 1) {
                const item = self.items.orderedRemove(self.pickIndexLocked());
                self.process_fn(self.process_ctx, item);
            }
        }

        fn pickIndexLocked(self: *Self) usize {
            const priority_fn = self.priority_fn orelse return 0;
            var best_index: usize = 0;
            var best_priority = priority_fn(self.items.items[0]);
            for (self.items.items[1..], 1..) |item, index| {
                const priority = priority_fn(item);
                if (priority > best_priority) {
                    best_priority = priority;
                    best_index = index;
                }
            }
            return best_index;
        }
    };
}

test "prefetch queue drains inline without worker in tests" {
    const QueueU32 = Queue(u32);
    var total: u32 = 0;
    const Ctx = struct {
        fn process(ctx: *anyopaque, item: u32) void {
            const sum: *u32 = @ptrCast(@alignCast(ctx));
            sum.* += item;
        }
    };

    var queue = QueueU32.init(std.testing.allocator, &total, &Ctx.process);
    defer queue.deinit();

    queue.lock();
    try queue.appendLocked(2);
    try queue.appendLocked(3);
    queue.signal();
    queue.unlock();

    queue.drainBudget(1);
    try std.testing.expectEqual(@as(u32, 2), total);
    queue.lock();
    try std.testing.expectEqual(@as(usize, 1), queue.items.items.len);
    queue.unlock();
}
