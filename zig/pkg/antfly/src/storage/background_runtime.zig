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

const builtin = @import("builtin");
const std = @import("std");
const platform = @import("antfly_platform");
const runtime_backend = @import("runtime_backend.zig");

const Allocator = std.mem.Allocator;
const Io = std.Io;
const AtomicU64 = platform.atomic.Value(u64);

pub const Backend = runtime_backend.Backend;
pub const IoImpl = if (builtin.os.tag == .freestanding) void else Io.Threaded;

pub const Config = struct {
    backend: Backend = runtime_backend.defaultExecutorBackend(),
};

pub const DurableJobLane = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        submit: *const fn (ptr: *anyopaque, job: Job) anyerror!void,
        drain_owner: *const fn (ptr: *anyopaque, owner_id: u64) void,
        poll: *const fn (ptr: *anyopaque, max_jobs: usize) anyerror!usize,
    };

    pub fn submit(self: DurableJobLane, job: Job) !void {
        return try self.vtable.submit(self.ptr, job);
    }

    pub fn drainOwner(self: DurableJobLane, owner_id: u64) void {
        self.vtable.drain_owner(self.ptr, owner_id);
    }

    pub fn poll(self: DurableJobLane, max_jobs: usize) !usize {
        return try self.vtable.poll(self.ptr, max_jobs);
    }
};

pub const Job = struct {
    owner_id: u64,
    class: Class,
    ptr: *anyopaque,
    run: *const fn (ptr: *anyopaque) anyerror!void,
    deinit: *const fn (ptr: *anyopaque) void,

    pub const Class = enum {
        commit_durable,
        maintenance,
        cleanup,
    };
};

pub const BackendRuntime = struct {
    alloc: Allocator,
    backend: Backend,
    next_owner_id: AtomicU64 = .init(1),
    io_impl: ?*IoImpl = null,
    inline_jobs: InlineDurableJobLane = .{},
    threaded_jobs: ?*ThreadedDurableJobLane = null,
    durable_jobs: DurableJobLane,

    pub fn init(alloc: Allocator, config: Config) !BackendRuntime {
        try runtime_backend.ensureExecutorBackendAvailable(config.backend);

        var runtime = BackendRuntime{
            .alloc = alloc,
            .backend = config.backend,
            .durable_jobs = undefined,
        };
        runtime.durable_jobs = runtime.inline_jobs.lane();

        if (config.backend != .manual) {
            if (comptime builtin.os.tag == .freestanding) {
                return error.UnsupportedPlatform;
            } else {
                const io_impl = try alloc.create(IoImpl);
                errdefer alloc.destroy(io_impl);
                io_impl.* = Io.Threaded.init(alloc, .{});

                const threaded_jobs = try alloc.create(ThreadedDurableJobLane);
                errdefer alloc.destroy(threaded_jobs);
                threaded_jobs.* = ThreadedDurableJobLane.init(alloc, io_impl);
                try threaded_jobs.start();
                errdefer threaded_jobs.deinit();

                runtime.io_impl = io_impl;
                runtime.threaded_jobs = threaded_jobs;
                runtime.durable_jobs = threaded_jobs.lane();
            }
        }

        return runtime;
    }

    pub fn deinit(self: *BackendRuntime) void {
        if (self.threaded_jobs) |jobs| {
            jobs.deinit();
            self.alloc.destroy(jobs);
            self.threaded_jobs = null;
        }
        if (self.io_impl) |io_impl| {
            if (comptime builtin.os.tag != .freestanding) {
                io_impl.deinit();
            }
            self.alloc.destroy(io_impl);
            self.io_impl = null;
        }
        self.* = undefined;
    }

    pub fn io(self: *BackendRuntime) ?Io {
        if (comptime builtin.os.tag == .freestanding) return null;
        return if (self.io_impl) |io_impl| io_impl.io() else null;
    }

    pub fn allocOwnerId(self: *BackendRuntime) u64 {
        return self.next_owner_id.fetchAdd(1, .monotonic);
    }
};

pub const BackendRuntimeHandle = struct {
    alloc: Allocator,
    runtime: *BackendRuntime,

    pub fn init(alloc: Allocator, config: Config) !BackendRuntimeHandle {
        const runtime = try alloc.create(BackendRuntime);
        errdefer alloc.destroy(runtime);
        runtime.* = try BackendRuntime.init(alloc, config);
        return .{
            .alloc = alloc,
            .runtime = runtime,
        };
    }

    pub fn deinit(self: *BackendRuntimeHandle) void {
        self.runtime.deinit();
        self.alloc.destroy(self.runtime);
        self.* = undefined;
    }

    pub fn ptr(self: *BackendRuntimeHandle) *BackendRuntime {
        return self.runtime;
    }
};

const InlineDurableJobLane = struct {
    fn lane(self: *InlineDurableJobLane) DurableJobLane {
        return .{
            .ptr = self,
            .vtable = &inline_vtable,
        };
    }

    fn submit(_: *anyopaque, job: Job) !void {
        defer job.deinit(job.ptr);
        return try job.run(job.ptr);
    }

    fn drainOwner(_: *anyopaque, _: u64) void {}

    fn poll(_: *anyopaque, _: usize) !usize {
        return 0;
    }
};

const inline_vtable = DurableJobLane.VTable{
    .submit = InlineDurableJobLane.submit,
    .drain_owner = InlineDurableJobLane.drainOwner,
    .poll = InlineDurableJobLane.poll,
};

const ThreadedDurableJobLane = if (builtin.os.tag == .freestanding) struct {
    fn init(_: Allocator, _: *IoImpl) ThreadedDurableJobLane {
        return .{};
    }

    fn start(_: *ThreadedDurableJobLane) !void {}

    fn lane(self: *ThreadedDurableJobLane) DurableJobLane {
        return .{
            .ptr = self,
            .vtable = &threaded_vtable,
        };
    }

    fn deinit(_: *ThreadedDurableJobLane) void {}

    fn submit(_: *anyopaque, _: Job) !void {
        return error.UnsupportedPlatform;
    }

    fn drainOwner(_: *anyopaque, _: u64) void {}

    fn poll(_: *anyopaque, _: usize) !usize {
        return 0;
    }
} else struct {
    const Entry = struct {
        job: Job,
        future: Io.Future(void),
        completed: std.atomic.Value(bool) = .init(false),

        fn deinitJobOnce(self: *Entry) void {
            self.job.deinit(self.job.ptr);
        }
    };

    alloc: Allocator,
    io_impl: *IoImpl,
    mutex: std.atomic.Mutex = .unlocked,
    reap_mutex: std.atomic.Mutex = .unlocked,
    shutdown_reaper: std.atomic.Value(bool) = .init(false),
    reaper_future: ?Io.Future(void) = null,
    entries: std.ArrayListUnmanaged(*Entry) = .empty,

    fn init(alloc: Allocator, io_impl: *IoImpl) ThreadedDurableJobLane {
        return .{
            .alloc = alloc,
            .io_impl = io_impl,
        };
    }

    fn start(self: *ThreadedDurableJobLane) !void {
        self.reaper_future = try self.io_impl.io().concurrent(reaperLoop, .{self});
    }

    fn lane(self: *ThreadedDurableJobLane) DurableJobLane {
        return .{
            .ptr = self,
            .vtable = &threaded_vtable,
        };
    }

    fn deinit(self: *ThreadedDurableJobLane) void {
        self.shutdown_reaper.store(true, .release);
        if (self.reaper_future) |*future| {
            _ = future.await(self.io_impl.io());
            self.reaper_future = null;
        }
        self.drainAll();
        self.entries.deinit(self.alloc);
        self.* = undefined;
    }

    fn submit(ptr: *anyopaque, job: Job) !void {
        const self: *ThreadedDurableJobLane = @ptrCast(@alignCast(ptr));
        _ = self.reapCompleted(8);

        const entry = try self.alloc.create(Entry);
        errdefer self.alloc.destroy(entry);

        entry.* = .{
            .job = job,
            .future = try self.io_impl.io().concurrent(runEntry, .{entry}),
        };
        errdefer {
            _ = entry.future.await(self.io_impl.io());
            entry.deinitJobOnce();
        }

        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        try self.entries.append(self.alloc, entry);
    }

    fn drainOwner(ptr: *anyopaque, owner_id: u64) void {
        const self: *ThreadedDurableJobLane = @ptrCast(@alignCast(ptr));
        self.drainMatching(owner_id);
    }

    fn poll(ptr: *anyopaque, max_jobs: usize) !usize {
        const self: *ThreadedDurableJobLane = @ptrCast(@alignCast(ptr));
        return self.reapCompleted(max_jobs);
    }

    fn runEntry(entry: *Entry) void {
        entry.job.run(entry.job.ptr) catch |err| {
            std.log.warn("background durable job failed owner={} class={s} err={s}", .{
                entry.job.owner_id,
                @tagName(entry.job.class),
                @errorName(err),
            });
        };
        entry.completed.store(true, .release);
    }

    fn reaperLoop(self: *ThreadedDurableJobLane) void {
        while (!self.shutdown_reaper.load(.acquire)) {
            _ = self.reapCompleted(32);
            self.io_impl.io().sleep(Io.Duration.fromMilliseconds(50), .awake) catch {};
        }
        while (self.reapCompleted(32) > 0) {}
    }

    fn drainAll(self: *ThreadedDurableJobLane) void {
        lockAtomic(&self.reap_mutex);
        defer self.reap_mutex.unlock();
        while (true) {
            const entry = self.popAny() orelse return;
            self.awaitAndDestroy(entry);
        }
    }

    fn drainMatching(self: *ThreadedDurableJobLane, owner_id: u64) void {
        lockAtomic(&self.reap_mutex);
        defer self.reap_mutex.unlock();
        while (true) {
            const entry = self.popOwner(owner_id) orelse return;
            self.awaitAndDestroy(entry);
        }
    }

    fn reapCompleted(self: *ThreadedDurableJobLane, max_jobs: usize) usize {
        lockAtomic(&self.reap_mutex);
        defer self.reap_mutex.unlock();
        var reaped: usize = 0;
        while (reaped < max_jobs) : (reaped += 1) {
            const entry = self.popCompleted() orelse return reaped;
            self.awaitAndDestroy(entry);
        }
        return reaped;
    }

    fn popAny(self: *ThreadedDurableJobLane) ?*Entry {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        if (self.entries.items.len == 0) return null;
        return self.entries.orderedRemove(0);
    }

    fn popOwner(self: *ThreadedDurableJobLane, owner_id: u64) ?*Entry {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        for (self.entries.items, 0..) |entry, idx| {
            if (entry.job.owner_id == owner_id) return self.entries.orderedRemove(idx);
        }
        return null;
    }

    fn popCompleted(self: *ThreadedDurableJobLane) ?*Entry {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        for (self.entries.items, 0..) |entry, idx| {
            if (entry.completed.load(.acquire)) return self.entries.orderedRemove(idx);
        }
        return null;
    }

    fn awaitAndDestroy(self: *ThreadedDurableJobLane, entry: *Entry) void {
        _ = entry.future.await(self.io_impl.io());
        entry.deinitJobOnce();
        self.alloc.destroy(entry);
    }
};

const threaded_vtable = DurableJobLane.VTable{
    .submit = ThreadedDurableJobLane.submit,
    .drain_owner = ThreadedDurableJobLane.drainOwner,
    .poll = ThreadedDurableJobLane.poll,
};

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) {
        if (builtin.os.tag == .freestanding or builtin.single_threaded) {
            std.atomic.spinLoopHint();
            continue;
        }
        std.Thread.yield() catch {};
    }
}

test "backend runtime handle owns a stable runtime pointer" {
    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .manual });
    defer handle.deinit();

    const first = handle.ptr();
    const second = handle.ptr();
    try std.testing.expect(first == second);
    try std.testing.expect(first.io_impl == null);
}

test "backend runtime durable lane runs inline jobs" {
    const Ctx = struct {
        ran: bool = false,
        deinit_called: bool = false,
    };
    const Fns = struct {
        fn run(ptr: *anyopaque) !void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            ctx.ran = true;
        }

        fn deinit(ptr: *anyopaque) void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            ctx.deinit_called = true;
        }
    };

    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .manual });
    defer handle.deinit();

    var ctx = Ctx{};
    try handle.ptr().durable_jobs.submit(.{
        .owner_id = 1,
        .class = .maintenance,
        .ptr = &ctx,
        .run = Fns.run,
        .deinit = Fns.deinit,
    });

    try std.testing.expect(ctx.ran);
    try std.testing.expect(ctx.deinit_called);
}

test "backend runtime allocates stable nonzero owner ids" {
    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .manual });
    defer handle.deinit();

    const first = handle.ptr().allocOwnerId();
    const second = handle.ptr().allocOwnerId();

    try std.testing.expect(first != 0);
    try std.testing.expectEqual(first + 1, second);
}

test "backend runtime durable lane drains threaded jobs by owner" {
    if (builtin.os.tag == .freestanding) return;

    const Ctx = struct {
        value: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
        deinits: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    };
    const Fns = struct {
        fn run(ptr: *anyopaque) !void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            _ = ctx.value.fetchAdd(1, .monotonic);
        }

        fn deinit(ptr: *anyopaque) void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            _ = ctx.deinits.fetchAdd(1, .monotonic);
        }
    };

    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    defer handle.deinit();

    var first = Ctx{};
    var second = Ctx{};
    try handle.ptr().durable_jobs.submit(.{
        .owner_id = 7,
        .class = .cleanup,
        .ptr = &first,
        .run = Fns.run,
        .deinit = Fns.deinit,
    });
    try handle.ptr().durable_jobs.submit(.{
        .owner_id = 8,
        .class = .cleanup,
        .ptr = &second,
        .run = Fns.run,
        .deinit = Fns.deinit,
    });

    handle.ptr().durable_jobs.drainOwner(7);
    try std.testing.expectEqual(@as(u32, 1), first.value.load(.monotonic));
    try std.testing.expectEqual(@as(u32, 1), first.deinits.load(.monotonic));

    handle.ptr().durable_jobs.drainOwner(8);
    try std.testing.expectEqual(@as(u32, 1), second.value.load(.monotonic));
    try std.testing.expectEqual(@as(u32, 1), second.deinits.load(.monotonic));
}

test "backend runtime durable lane deinits threaded job payload after completion" {
    if (builtin.os.tag == .freestanding) return;

    const Ctx = struct {
        ran: std.atomic.Value(u32) = .init(0),
        deinits: std.atomic.Value(u32) = .init(0),
    };
    const Fns = struct {
        fn run(ptr: *anyopaque) !void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            _ = ctx.ran.fetchAdd(1, .release);
        }

        fn deinit(ptr: *anyopaque) void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            _ = ctx.deinits.fetchAdd(1, .release);
        }
    };

    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    defer handle.deinit();

    var ctx = Ctx{};
    try handle.ptr().durable_jobs.submit(.{
        .owner_id = 9,
        .class = .maintenance,
        .ptr = &ctx,
        .run = Fns.run,
        .deinit = Fns.deinit,
    });

    var attempts: usize = 0;
    while (ctx.deinits.load(.acquire) == 0 and attempts < 200) : (attempts += 1) {
        _ = try handle.ptr().durable_jobs.poll(8);
        if (handle.ptr().io()) |io| io.sleep(Io.Duration.fromMilliseconds(2), .awake) catch {};
    }
    handle.ptr().durable_jobs.drainOwner(9);

    try std.testing.expectEqual(@as(u32, 1), ctx.ran.load(.acquire));
    try std.testing.expectEqual(@as(u32, 1), ctx.deinits.load(.acquire));
}
