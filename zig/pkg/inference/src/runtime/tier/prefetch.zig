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
/// Borrowed queue synchronization. The queue must stay at its final address
/// and outlive every handle, just as it outlives the weights it guards.
pub const LockHandle = struct {
    io: std.Io,
    mutex: *std.Io.Mutex,

    pub fn lock(self: LockHandle) void {
        self.mutex.lockUncancelable(self.io);
    }

    pub fn unlock(self: LockHandle) void {
        self.mutex.unlock(self.io);
    }
};

pub fn Queue(comptime Item: type) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        items: std.ArrayListUnmanaged(Item) = .empty,
        io_impl: std.Io.Threaded,
        mutex: std.Io.Mutex = .init,
        lifecycle_mutex: std.Io.Mutex = .init,
        wake: std.Io.Event = .unset,
        worker: ?std.Io.Future(void) = null,
        stop_worker: bool = false,
        process_ctx: *anyopaque,
        process_fn: *const fn (ctx: *anyopaque, item: Item) void,
        priority_fn: ?*const fn (item: Item) u64 = null,
        process_with_lock: bool = true,

        pub fn init(
            allocator: std.mem.Allocator,
            process_ctx: *anyopaque,
            process_fn: *const fn (ctx: *anyopaque, item: Item) void,
        ) Self {
            return .{
                .allocator = allocator,
                .io_impl = std.Io.Threaded.init(allocator, .{
                    .async_limit = .nothing,
                    .concurrent_limit = .limited(1),
                }),
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

        pub fn initWithPriorityUnlocked(
            allocator: std.mem.Allocator,
            process_ctx: *anyopaque,
            process_fn: *const fn (ctx: *anyopaque, item: Item) void,
            priority_fn: *const fn (item: Item) u64,
        ) Self {
            var self = initWithPriority(allocator, process_ctx, process_fn, priority_fn);
            self.process_with_lock = false;
            return self;
        }

        pub fn deinit(self: *Self) void {
            self.stop();
            self.items.deinit(self.allocator);
            self.io_impl.deinit();
        }

        pub fn lockHandle(self: *Self) LockHandle {
            return .{ .io = self.io_impl.io(), .mutex = &self.mutex };
        }

        pub fn lock(self: *Self) void {
            self.lockHandle().lock();
        }

        pub fn unlock(self: *Self) void {
            self.lockHandle().unlock();
        }

        pub fn signal(self: *Self) void {
            self.wake.set(self.io_impl.io());
        }

        pub fn appendLocked(self: *Self, item: Item) !void {
            try self.items.append(self.allocator, item);
        }

        pub fn start(self: *Self) !void {
            if (builtin.is_test) return;
            try self.startWorker();
        }

        fn startWorker(self: *Self) !void {
            const io = self.io_impl.io();
            self.lifecycle_mutex.lockUncancelable(io);
            defer self.lifecycle_mutex.unlock(io);
            if (self.worker != null) return;
            self.lock();
            self.stop_worker = false;
            self.wake.reset();
            self.unlock();
            self.worker = try io.concurrent(workerMain, .{self});
        }

        pub fn stop(self: *Self) void {
            const io = self.io_impl.io();
            self.lifecycle_mutex.lockUncancelable(io);
            defer self.lifecycle_mutex.unlock(io);
            if (self.worker) |*worker| {
                self.lock();
                self.stop_worker = true;
                self.signal();
                self.unlock();
                worker.await(io);
                self.worker = null;
                self.stop_worker = false;
            }
        }

        pub fn drainBudget(self: *Self, max_items: usize) void {
            const io = self.io_impl.io();
            self.lifecycle_mutex.lockUncancelable(io);
            defer self.lifecycle_mutex.unlock(io);
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
                    if (self.process_with_lock) {
                        self.process_fn(self.process_ctx, item);
                    } else {
                        self.unlock();
                        self.process_fn(self.process_ctx, item);
                        continue;
                    }
                    self.unlock();
                    continue;
                }
                // Reset while holding the append/stop mutex so an arriving
                // notification remains latched through the following wait.
                self.wake.reset();
                self.unlock();
                self.wake.waitUncancelable(self.io_impl.io());
            }
        }

        fn drainBudgetLocked(self: *Self, max_items: usize) void {
            var remaining = @min(max_items, self.items.items.len);
            while (remaining > 0 and self.items.items.len > 0) : (remaining -= 1) {
                const item = self.items.orderedRemove(self.pickIndexLocked());
                if (self.process_with_lock) {
                    self.process_fn(self.process_ctx, item);
                } else {
                    self.unlock();
                    self.process_fn(self.process_ctx, item);
                    self.lock();
                }
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

test "prefetch queue background wake stop and restart preserve lock policy" {
    const QueueU32 = Queue(u32);
    const Context = struct {
        queue: *QueueU32 = undefined,
        unlocked: bool,
        total: std.atomic.Value(u32) = .init(0),
        done: std.Io.Event = .unset,

        fn process(ptr: *anyopaque, item: u32) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.unlocked) {
                self.queue.lock();
                self.queue.unlock();
            }
            _ = self.total.fetchAdd(item, .monotonic);
            self.done.set(std.testing.io);
        }
    };
    for ([_]bool{ false, true }) |unlocked| {
        var ctx = Context{ .unlocked = unlocked };
        var queue = QueueU32.init(std.testing.allocator, &ctx, Context.process);
        defer queue.deinit();
        ctx.queue = &queue;
        queue.process_with_lock = !unlocked;
        for (0..2) |round| {
            ctx.done.reset();
            try queue.startWorker();
            queue.lock();
            queue.appendLocked(7) catch |err| {
                queue.unlock();
                return err;
            };
            queue.signal();
            queue.unlock();
            try ctx.done.waitTimeout(std.testing.io, .{ .duration = .{
                .raw = .fromSeconds(5),
                .clock = .awake,
            } });
            queue.stop();
            try std.testing.expect(queue.worker == null);
            try std.testing.expectEqual(@as(u32, @intCast((round + 1) * 7)), ctx.total.load(.monotonic));
        }
    }
}

test "prefetch queue scheduling failure retains pending work for manual draining" {
    const Context = struct {
        fn process(ptr: *anyopaque, item: u32) void {
            const total: *u32 = @ptrCast(@alignCast(ptr));
            total.* += item;
        }
    };
    var total: u32 = 0;
    var queue = Queue(u32).init(std.testing.allocator, &total, Context.process);
    defer queue.deinit();
    queue.io_impl.concurrent_limit = .nothing;
    try queue.appendLocked(9);
    try std.testing.expectError(error.ConcurrencyUnavailable, queue.startWorker());
    try std.testing.expect(queue.worker == null);
    queue.drainBudget(1);
    try std.testing.expectEqual(@as(u32, 9), total);
}

test "prefetch queue manual draining honors priority and budget" {
    const Context = struct {
        values: [3]u32 = undefined,
        len: usize = 0,
        fn process(ptr: *anyopaque, item: u32) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.values[self.len] = item;
            self.len += 1;
        }
        fn priority(item: u32) u64 {
            return item;
        }
    };
    var ctx = Context{};
    var queue = Queue(u32).initWithPriority(std.testing.allocator, &ctx, Context.process, Context.priority);
    defer queue.deinit();
    try queue.start();
    try std.testing.expect(queue.worker == null);
    try queue.appendLocked(1);
    try queue.appendLocked(3);
    try queue.appendLocked(2);
    queue.drainBudget(2);
    try std.testing.expectEqualSlices(u32, &.{ 3, 2 }, ctx.values[0..ctx.len]);
    queue.drainBudget(2);
    try std.testing.expectEqualSlices(u32, &.{ 3, 2, 1 }, ctx.values[0..ctx.len]);
}
