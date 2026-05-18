//! Task Executor for httpx.zig
//!
//! Provides async task execution capabilities:
//!
//! - Thread pool for parallel execution
//! - Task queuing and scheduling
//! - Work stealing for load balancing
//! - Cross-platform thread management

const std = @import("std");
const Allocator = std.mem.Allocator;
const Thread = std.Thread;

pub const ExecutorError = error{
    TaskQueueFull,
};

/// Task function type.
pub const TaskFn = *const fn (?*anyopaque) void;

/// Task with function and context.
pub const Task = struct {
    func: TaskFn,
    context: ?*anyopaque = null,
    priority: u8 = 0,
};

/// Executor configuration.
pub const ExecutorConfig = struct {
    num_threads: u32 = 0,
    task_queue_size: usize = 1024,
    idle_timeout_ms: u64 = 60_000,
};

/// Thread pool executor for parallel task execution.
pub const Executor = struct {
    allocator: Allocator,
    config: ExecutorConfig,
    tasks: std.ArrayListUnmanaged(Task) = .empty,
    running: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    threads: []Thread = &.{},
    mutex: Thread.Mutex = .{},
    cond: Thread.Condition = .{},

    const Self = @This();

    /// Creates an executor with default configuration.
    pub fn init(allocator: Allocator) Self {
        return initWithConfig(allocator, .{});
    }

    /// Creates an executor with custom configuration.
    pub fn initWithConfig(allocator: Allocator, config: ExecutorConfig) Self {
        var cfg = config;
        if (cfg.num_threads == 0) {
            const cpu_count = std.Thread.getCpuCount() catch 4;
            cfg.num_threads = @max(1, @as(u32, @intCast(cpu_count)));
        }
        return .{
            .allocator = allocator,
            .config = cfg,
        };
    }

    /// Releases executor resources.
    pub fn deinit(self: *Self) void {
        self.stop();
        self.tasks.deinit(self.allocator);
    }

    /// Submits a task for execution.
    pub fn submit(self: *Self, task: Task) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.tasks.items.len >= self.config.task_queue_size) {
            return ExecutorError.TaskQueueFull;
        }

        try self.tasks.append(self.allocator, task);
        self.cond.signal();
    }

    /// Submits a function for execution.
    pub fn execute(self: *Self, func: TaskFn, context: ?*anyopaque) !void {
        try self.submit(.{ .func = func, .context = context });
    }

    /// Starts the executor threads.
    pub fn start(self: *Self) !void {
        if (self.running.swap(true, .acq_rel)) return;

        self.threads = try self.allocator.alloc(Thread, self.config.num_threads);
        var spawned: usize = 0;
        errdefer {
            // On partial spawn failure, join already-started threads and free.
            self.mutex.lock();
            self.running.store(false, .release);
            self.cond.broadcast();
            self.mutex.unlock();
            for (self.threads[0..spawned]) |thread| thread.join();
            self.allocator.free(self.threads);
            self.threads = &.{};
        }

        for (self.threads) |*thread| {
            thread.* = try Thread.spawn(.{}, workerLoop, .{self});
            spawned += 1;
        }
    }

    /// Stops all executor threads and frees the thread handle slice.
    pub fn stop(self: *Self) void {
        if (!self.running.swap(false, .acq_rel)) return;
        self.mutex.lock();
        self.cond.broadcast();
        self.mutex.unlock();

        for (self.threads) |thread| thread.join();
        self.allocator.free(self.threads);
        self.threads = &.{};
    }

    /// Returns the number of pending tasks.
    pub fn pendingCount(self: *Self) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.tasks.items.len;
    }

    /// Runs all tasks synchronously in FIFO order.
    pub fn runAll(self: *Self) void {
        while (true) {
            self.mutex.lock();
            if (self.tasks.items.len == 0) {
                self.mutex.unlock();
                break;
            }
            const task = self.tasks.orderedRemove(0);
            self.mutex.unlock();

            task.func(task.context);
        }
    }

    fn workerLoop(self: *Self) void {
        while (true) {
            self.mutex.lock();
            while (self.running.load(.acquire) and self.tasks.items.len == 0) {
                self.cond.wait(&self.mutex);
            }
            if (!self.running.load(.acquire)) {
                self.mutex.unlock();
                break;
            }

            const task = self.tasks.orderedRemove(0);
            self.mutex.unlock();

            task.func(task.context);
        }
    }
};

/// Future representing a pending result.
/// Uses a condition variable for blocking wait and an atomic flag for
/// lock-free polling via `isDone()` and `get()`.
pub fn Future(comptime T: type) type {
    return struct {
        result: ?T = null,
        error_val: ?anyerror = null,
        completed: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
        mutex: Thread.Mutex = .{},
        cond: Thread.Condition = .{},

        const Self = @This();

        /// Waits for the future to complete (blocks the calling thread).
        pub fn wait(self: *Self) !T {
            // Fast path: already done.
            if (self.completed.load(.acquire)) {
                if (self.error_val) |err| return err;
                return self.result.?;
            }
            self.mutex.lock();
            defer self.mutex.unlock();
            while (!self.completed.load(.acquire)) {
                self.cond.wait(&self.mutex);
            }
            if (self.error_val) |err| {
                return err;
            }
            return self.result.?;
        }

        /// Completes the future with a value.
        pub fn complete(self: *Self, value: T) void {
            self.mutex.lock();
            defer self.mutex.unlock();
            self.result = value;
            self.completed.store(true, .release);
            self.cond.signal();
        }

        /// Completes the future with an error.
        pub fn completeWithError(self: *Self, err: anyerror) void {
            self.mutex.lock();
            defer self.mutex.unlock();
            self.error_val = err;
            self.completed.store(true, .release);
            self.cond.signal();
        }

        /// Returns the result if available (non-blocking, lock-free).
        pub fn get(self: *Self) ?T {
            if (!self.completed.load(.acquire)) return null;
            if (self.error_val != null) return null;
            return self.result;
        }

        /// Returns true if the future is completed (non-blocking, lock-free).
        pub fn isDone(self: *Self) bool {
            return self.completed.load(.acquire);
        }
    };
}

test "Executor initialization" {
    const allocator = std.testing.allocator;
    var exec = Executor.init(allocator);
    defer exec.deinit();

    try std.testing.expect(exec.config.num_threads > 0);
}

test "Executor task submission" {
    const allocator = std.testing.allocator;
    var exec = Executor.init(allocator);
    defer exec.deinit();

    var counter: u32 = 0;
    const Counter = struct {
        fn increment(ctx: ?*anyopaque) void {
            const c: *u32 = @ptrCast(@alignCast(ctx.?));
            c.* += 1;
        }
    };

    try exec.execute(Counter.increment, &counter);
    exec.runAll();

    try std.testing.expectEqual(@as(u32, 1), counter);
}

test "Future" {
    var future = Future(i32){};

    try std.testing.expect(!future.isDone());
    try std.testing.expect(future.get() == null);

    future.complete(42);

    try std.testing.expect(future.isDone());
    try std.testing.expectEqual(@as(i32, 42), future.get().?);
}
