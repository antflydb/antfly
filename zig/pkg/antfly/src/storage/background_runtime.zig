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
const storage_io = @import("lsm_backend/storage_io.zig");
const threaded_io_limits = @import("../common/threaded_io_limits.zig");

const Allocator = std.mem.Allocator;
const Io = std.Io;
const AtomicU64 = platform.atomic.Value(u64);

pub const Backend = runtime_backend.Backend;
pub const IoImpl = if (builtin.os.tag == .freestanding) void else Io.Threaded;
pub const default_io_concurrent_limit: u32 = threaded_io_limits.service;

pub const Config = struct {
    backend: Backend = runtime_backend.defaultExecutorBackend(),
};

/// Process-local hook used by composed runtimes to replace a filesystem DB
/// open with another storage implementation. The options pointer is opaque here
/// to keep the executor layer independent of the DB module; DB.open is the sole
/// caller and passes a `*db.OpenOptions`.
pub const DbOpenConfigurator = struct {
    ptr: *anyopaque,
    configure_fn: *const fn (ptr: *anyopaque, path: []const u8, options: *anyopaque) anyerror!void,

    pub fn configure(self: @This(), path: []const u8, options: anytype) !void {
        try self.configure_fn(self.ptr, path, @ptrCast(options));
    }
};

pub const DurableJobLane = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        submit: *const fn (ptr: *anyopaque, job: Job) anyerror!void,
        drain_owner: *const fn (ptr: *anyopaque, owner_id: u64) void,
        close_owner: *const fn (ptr: *anyopaque, owner_id: u64) void,
        poll: *const fn (ptr: *anyopaque, max_jobs: usize) anyerror!usize,
        executes_inline: bool = false,
    };

    /// On success, the lane owns `job` and will call `job.deinit`.
    /// On error, ownership remains with the caller.
    pub fn submit(self: DurableJobLane, job: Job) !void {
        return try self.vtable.submit(self.ptr, job);
    }

    pub fn drainOwner(self: DurableJobLane, owner_id: u64) void {
        self.vtable.drain_owner(self.ptr, owner_id);
    }

    pub fn closeOwner(self: DurableJobLane, owner_id: u64) void {
        self.vtable.close_owner(self.ptr, owner_id);
    }

    pub fn poll(self: DurableJobLane, max_jobs: usize) !usize {
        return try self.vtable.poll(self.ptr, max_jobs);
    }

    /// Manual runtimes execute submissions on the caller's stack. Workers
    /// that page durable work must leave the marker pending for a later
    /// explicit poll/reopen instead of recursively submitting their successor.
    pub fn executesInline(self: DurableJobLane) bool {
        return self.vtable.executes_inline;
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

fn initIoLane(alloc: Allocator) !*IoImpl {
    if (comptime builtin.os.tag == .freestanding) {
        return error.UnsupportedPlatform;
    } else {
        const io_impl = try alloc.create(IoImpl);
        errdefer alloc.destroy(io_impl);
        // Backend runtimes are process-long and own several independent I/O
        // lanes. Threaded retains concurrent workers until deinit, so a finite
        // ceiling prevents any lane from converting a transient fan-out spike
        // into an unbounded kernel-thread/stack reservation ratchet.
        io_impl.* = Io.Threaded.init(alloc, .{
            .concurrent_limit = .limited(default_io_concurrent_limit),
        });
        return io_impl;
    }
}

fn deinitIoLane(alloc: Allocator, io_impl: *IoImpl) void {
    if (comptime builtin.os.tag != .freestanding) {
        io_impl.deinit();
    }
    alloc.destroy(io_impl);
}

const OwnerRegistry = struct {
    const State = struct {
        closing: bool = false,
        in_flight: usize = 0,
    };

    alloc: Allocator,
    mutex: std.atomic.Mutex = .unlocked,
    states: std.AutoHashMapUnmanaged(u64, State) = .empty,

    fn init(alloc: Allocator) OwnerRegistry {
        return .{ .alloc = alloc };
    }

    fn deinit(self: *OwnerRegistry) void {
        var iterator = self.states.valueIterator();
        while (iterator.next()) |state| std.debug.assert(state.in_flight == 0);
        self.states.deinit(self.alloc);
        self.* = undefined;
    }

    fn register(self: *OwnerRegistry, owner_id: u64) !void {
        if (owner_id == 0) return error.InvalidBackgroundOwner;
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        if (self.states.contains(owner_id)) return error.BackgroundOwnerIdExhausted;
        try self.states.putNoClobber(self.alloc, owner_id, .{});
    }

    fn beginJob(self: *OwnerRegistry, owner_id: u64) !void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const state = self.states.getPtr(owner_id) orelse return error.BackgroundOwnerClosed;
        if (state.closing) return error.BackgroundOwnerClosing;
        if (state.in_flight == std.math.maxInt(usize)) return error.BackgroundOwnerCapacityExceeded;
        state.in_flight += 1;
    }

    fn finishJob(self: *OwnerRegistry, owner_id: u64) void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const state = self.states.getPtr(owner_id) orelse {
            std.debug.panic("background owner {} retired with a job in flight", .{owner_id});
        };
        std.debug.assert(state.in_flight > 0);
        state.in_flight -= 1;
    }

    fn beginClose(self: *OwnerRegistry, owner_id: u64) bool {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const state = self.states.getPtr(owner_id) orelse return false;
        state.closing = true;
        return true;
    }

    fn waitIdle(self: *OwnerRegistry, owner_id: u64) void {
        while (true) {
            lockAtomic(&self.mutex);
            const idle = if (self.states.getPtr(owner_id)) |state|
                state.in_flight == 0
            else
                true;
            self.mutex.unlock();
            if (idle) return;
            if (builtin.os.tag == .freestanding or builtin.single_threaded) {
                std.atomic.spinLoopHint();
            } else {
                std.Thread.yield() catch {};
            }
        }
    }

    fn retireClosed(self: *OwnerRegistry, owner_id: u64) void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const state = self.states.getPtr(owner_id) orelse return;
        std.debug.assert(state.closing);
        std.debug.assert(state.in_flight == 0);
        _ = self.states.remove(owner_id);
    }

    fn close(self: *OwnerRegistry, owner_id: u64) void {
        if (!self.beginClose(owner_id)) return;
        self.waitIdle(owner_id);
        self.retireClosed(owner_id);
    }
};

pub const BackendRuntime = struct {
    alloc: Allocator,
    backend: Backend,
    next_owner_id: AtomicU64,
    retired_generation_cleanup_owner_id: u64,
    owner_registry: *OwnerRegistry,
    native_storage_pool: *storage_io.NativeStoragePool,
    io_impl: ?*IoImpl = null,
    raft_inbound_io_impl: ?*IoImpl = null,
    raft_outbound_io_impl: ?*IoImpl = null,
    api_io_impl: ?*IoImpl = null,
    threaded_jobs: ?*ThreadedDurableJobLane = null,
    durable_jobs: DurableJobLane,
    db_open_configurator: ?DbOpenConfigurator = null,

    pub fn init(alloc: Allocator, config: Config) !BackendRuntime {
        try runtime_backend.ensureExecutorBackendAvailable(config.backend);

        const owner_registry = try alloc.create(OwnerRegistry);
        errdefer alloc.destroy(owner_registry);
        owner_registry.* = OwnerRegistry.init(alloc);
        errdefer owner_registry.deinit();
        const retired_generation_cleanup_owner_id: u64 = 1;
        try owner_registry.register(retired_generation_cleanup_owner_id);

        const native_storage_pool = try alloc.create(storage_io.NativeStoragePool);
        errdefer alloc.destroy(native_storage_pool);
        native_storage_pool.* = storage_io.NativeStoragePool.init(alloc);
        errdefer native_storage_pool.deinit();

        var runtime = BackendRuntime{
            .alloc = alloc,
            .backend = config.backend,
            .next_owner_id = .init(retired_generation_cleanup_owner_id + 1),
            .retired_generation_cleanup_owner_id = retired_generation_cleanup_owner_id,
            .owner_registry = owner_registry,
            .native_storage_pool = native_storage_pool,
            .durable_jobs = undefined,
        };
        runtime.durable_jobs = InlineDurableJobLane.lane(owner_registry);

        if (config.backend != .manual) {
            if (comptime builtin.os.tag == .freestanding) {
                return error.UnsupportedPlatform;
            } else {
                const io_impl = try initIoLane(alloc);
                errdefer deinitIoLane(alloc, io_impl);
                const raft_inbound_io_impl = try initIoLane(alloc);
                errdefer deinitIoLane(alloc, raft_inbound_io_impl);
                const raft_outbound_io_impl = try initIoLane(alloc);
                errdefer deinitIoLane(alloc, raft_outbound_io_impl);
                const api_io_impl = try initIoLane(alloc);
                errdefer deinitIoLane(alloc, api_io_impl);

                const threaded_jobs = try alloc.create(ThreadedDurableJobLane);
                errdefer alloc.destroy(threaded_jobs);
                threaded_jobs.* = ThreadedDurableJobLane.init(alloc, io_impl, owner_registry);
                try threaded_jobs.start();
                errdefer threaded_jobs.deinit();

                runtime.io_impl = io_impl;
                runtime.raft_inbound_io_impl = raft_inbound_io_impl;
                runtime.raft_outbound_io_impl = raft_outbound_io_impl;
                runtime.api_io_impl = api_io_impl;
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
        if (self.api_io_impl) |io_impl| {
            deinitIoLane(self.alloc, io_impl);
            self.api_io_impl = null;
        }
        if (self.raft_outbound_io_impl) |io_impl| {
            deinitIoLane(self.alloc, io_impl);
            self.raft_outbound_io_impl = null;
        }
        if (self.raft_inbound_io_impl) |io_impl| {
            deinitIoLane(self.alloc, io_impl);
            self.raft_inbound_io_impl = null;
        }
        if (self.io_impl) |io_impl| {
            deinitIoLane(self.alloc, io_impl);
            self.io_impl = null;
        }
        self.native_storage_pool.deinit();
        self.alloc.destroy(self.native_storage_pool);
        self.owner_registry.deinit();
        self.alloc.destroy(self.owner_registry);
        self.* = undefined;
    }

    pub fn io(self: *BackendRuntime) ?Io {
        if (comptime builtin.os.tag == .freestanding) return null;
        return if (self.io_impl) |io_impl| io_impl.io() else null;
    }

    pub fn nativeStoragePool(self: *BackendRuntime) *storage_io.NativeStoragePool {
        return self.native_storage_pool;
    }

    pub fn snapshotNativeStorageStats(self: *const BackendRuntime) storage_io.NativeStorageStats {
        return self.native_storage_pool.snapshotStats();
    }

    pub fn raftInboundIo(self: *BackendRuntime) ?Io {
        if (comptime builtin.os.tag == .freestanding) return null;
        return if (self.raft_inbound_io_impl) |io_impl| io_impl.io() else self.io();
    }

    pub fn raftInboundIoImpl(self: *BackendRuntime) ?*IoImpl {
        if (comptime builtin.os.tag == .freestanding) return null;
        return self.raft_inbound_io_impl orelse self.io_impl;
    }

    pub fn raftOutboundIo(self: *BackendRuntime) ?Io {
        if (comptime builtin.os.tag == .freestanding) return null;
        return if (self.raft_outbound_io_impl) |io_impl| io_impl.io() else self.io();
    }

    pub fn raftOutboundIoImpl(self: *BackendRuntime) ?*IoImpl {
        if (comptime builtin.os.tag == .freestanding) return null;
        return self.raft_outbound_io_impl orelse self.io_impl;
    }

    pub fn apiIoImpl(self: *BackendRuntime) ?*IoImpl {
        if (comptime builtin.os.tag == .freestanding) return null;
        return self.api_io_impl orelse self.io_impl;
    }

    pub fn allocOwnerId(self: *BackendRuntime) !u64 {
        while (true) {
            const owner_id = self.next_owner_id.fetchAdd(1, .monotonic);
            if (owner_id == 0) continue;
            try self.owner_registry.register(owner_id);
            return owner_id;
        }
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
    fn lane(owners: *OwnerRegistry) DurableJobLane {
        return .{
            .ptr = owners,
            .vtable = &inline_vtable,
        };
    }

    fn submit(ptr: *anyopaque, job: Job) !void {
        const owners: *OwnerRegistry = @ptrCast(@alignCast(ptr));
        try owners.beginJob(job.owner_id);
        defer owners.finishJob(job.owner_id);
        try job.run(job.ptr);
        job.deinit(job.ptr);
    }

    fn drainOwner(ptr: *anyopaque, owner_id: u64) void {
        const owners: *OwnerRegistry = @ptrCast(@alignCast(ptr));
        owners.waitIdle(owner_id);
    }

    fn closeOwner(ptr: *anyopaque, owner_id: u64) void {
        const owners: *OwnerRegistry = @ptrCast(@alignCast(ptr));
        owners.close(owner_id);
    }

    fn poll(_: *anyopaque, _: usize) !usize {
        return 0;
    }
};

const inline_vtable = DurableJobLane.VTable{
    .submit = InlineDurableJobLane.submit,
    .drain_owner = InlineDurableJobLane.drainOwner,
    .close_owner = InlineDurableJobLane.closeOwner,
    .poll = InlineDurableJobLane.poll,
    .executes_inline = true,
};

const ThreadedDurableJobLane = if (builtin.os.tag == .freestanding) struct {
    fn init(_: Allocator, _: *IoImpl, _: *OwnerRegistry) ThreadedDurableJobLane {
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

    fn closeOwner(_: *anyopaque, _: u64) void {}

    fn poll(_: *anyopaque, _: usize) !usize {
        return 0;
    }
} else struct {
    const Entry = struct {
        lane: *ThreadedDurableJobLane,
        job: Job,
        future: Io.Future(void),
        completed: std.atomic.Value(bool) = .init(false),
        job_deinited: std.atomic.Value(bool) = .init(false),

        fn deinitJobOnce(self: *Entry) void {
            if (self.job_deinited.swap(true, .acq_rel)) return;
            self.job.deinit(self.job.ptr);
        }
    };

    const reap_batch_limit: usize = 4096;
    const idle_reap_interval_ms: u64 = 10;

    alloc: Allocator,
    io_impl: *IoImpl,
    owners: *OwnerRegistry,
    mutex: std.atomic.Mutex = .unlocked,
    reap_mutex: std.atomic.Mutex = .unlocked,
    shutdown_reaper: std.atomic.Value(bool) = .init(false),
    completed_count: std.atomic.Value(usize) = .init(0),
    reaper_future: ?Io.Future(void) = null,
    entries: std.ArrayListUnmanaged(*Entry) = .empty,

    fn init(alloc: Allocator, io_impl: *IoImpl, owners: *OwnerRegistry) ThreadedDurableJobLane {
        return .{
            .alloc = alloc,
            .io_impl = io_impl,
            .owners = owners,
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
        try self.owners.beginJob(job.owner_id);
        errdefer self.owners.finishJob(job.owner_id);
        const entry = try self.alloc.create(Entry);
        entry.* = .{
            .lane = self,
            .job = job,
            .future = undefined,
        };
        errdefer self.alloc.destroy(entry);

        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        try self.entries.ensureUnusedCapacity(self.alloc, 1);
        entry.future = try self.io_impl.io().concurrent(runEntry, .{entry});
        self.entries.appendAssumeCapacity(entry);
    }

    fn drainOwner(ptr: *anyopaque, owner_id: u64) void {
        const self: *ThreadedDurableJobLane = @ptrCast(@alignCast(ptr));
        self.drainMatching(owner_id);
    }

    fn closeOwner(ptr: *anyopaque, owner_id: u64) void {
        const self: *ThreadedDurableJobLane = @ptrCast(@alignCast(ptr));
        if (!self.owners.beginClose(owner_id)) return;
        self.drainMatching(owner_id);
        self.owners.waitIdle(owner_id);
        self.owners.retireClosed(owner_id);
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
        // The payload often owns the transaction state and buffers that make
        // a durable job large. Release it on the worker at the actual lifetime
        // boundary instead of retaining it until the bookkeeping reaper joins
        // the already-completed future. `deinitJobOnce` also makes concurrent
        // owner drains safe.
        entry.deinitJobOnce();
        // Publish the count first. It is only a wake/drain hint; the release
        // store below remains authoritative. Publishing in this order also
        // prevents a reaper from freeing `entry` before this worker's last
        // access to it.
        entry.lane.owners.finishJob(entry.job.owner_id);
        _ = entry.lane.completed_count.fetchAdd(1, .monotonic);
        entry.completed.store(true, .release);
    }

    fn reaperLoop(self: *ThreadedDurableJobLane) void {
        while (!self.shutdown_reaper.load(.acquire)) {
            const reaped = self.reapCompleted(reap_batch_limit);
            // Drain a backlog without an artificial rate cap. At idle, a
            // short sleep avoids scanning the active set continuously.
            if (reaped == reap_batch_limit or self.completed_count.load(.monotonic) > 0) continue;
            self.io_impl.io().sleep(Io.Duration.fromMilliseconds(idle_reap_interval_ms), .awake) catch {};
        }
        while (self.reapCompleted(reap_batch_limit) > 0) {}
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
        if (max_jobs == 0 or self.completed_count.load(.monotonic) == 0) return 0;
        lockAtomic(&self.reap_mutex);
        defer self.reap_mutex.unlock();

        // Detach completed entries in one pass. The previous implementation
        // repeatedly called orderedRemove, shifting the entire tail for every
        // completed job. A large ingest could therefore retain millions of
        // finished payloads while spending most of a core in memmove.
        var detached: [reap_batch_limit]*Entry = undefined;
        const target = @min(max_jobs, detached.len);
        var detached_count: usize = 0;
        lockAtomic(&self.mutex);
        var idx: usize = 0;
        while (idx < self.entries.items.len and detached_count < target) {
            const entry = self.entries.items[idx];
            if (!entry.completed.load(.acquire)) {
                idx += 1;
                continue;
            }
            detached[detached_count] = entry;
            detached_count += 1;
            _ = self.entries.swapRemove(idx);
        }
        self.mutex.unlock();

        for (detached[0..detached_count]) |entry| self.awaitAndDestroy(entry);
        return detached_count;
    }

    fn popAny(self: *ThreadedDurableJobLane) ?*Entry {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        if (self.entries.items.len == 0) return null;
        return self.entries.swapRemove(0);
    }

    fn popOwner(self: *ThreadedDurableJobLane, owner_id: u64) ?*Entry {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        for (self.entries.items, 0..) |entry, idx| {
            if (entry.job.owner_id == owner_id) return self.entries.swapRemove(idx);
        }
        return null;
    }

    fn awaitAndDestroy(self: *ThreadedDurableJobLane, entry: *Entry) void {
        _ = entry.future.await(self.io_impl.io());
        if (entry.completed.swap(false, .acq_rel)) {
            _ = self.completed_count.fetchSub(1, .monotonic);
        }
        entry.deinitJobOnce();
        self.alloc.destroy(entry);
    }
};

const threaded_vtable = DurableJobLane.VTable{
    .submit = ThreadedDurableJobLane.submit,
    .drain_owner = ThreadedDurableJobLane.drainOwner,
    .close_owner = ThreadedDurableJobLane.closeOwner,
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

    try std.testing.expect(handle.ptr().durable_jobs.executesInline());
    const owner_id = try handle.ptr().allocOwnerId();
    var ctx = Ctx{};
    try handle.ptr().durable_jobs.submit(.{
        .owner_id = owner_id,
        .class = .maintenance,
        .ptr = &ctx,
        .run = Fns.run,
        .deinit = Fns.deinit,
    });

    try std.testing.expect(ctx.ran);
    try std.testing.expect(ctx.deinit_called);
}

test "backend runtime durable lane leaves inline failed jobs owned by caller" {
    const Ctx = struct {
        ran: bool = false,
        deinit_called: bool = false,
    };
    const Fns = struct {
        fn run(ptr: *anyopaque) !void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            ctx.ran = true;
            return error.ExpectedFailure;
        }

        fn deinit(ptr: *anyopaque) void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            ctx.deinit_called = true;
        }
    };

    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .manual });
    defer handle.deinit();

    const owner_id = try handle.ptr().allocOwnerId();
    var ctx = Ctx{};
    try std.testing.expectError(error.ExpectedFailure, handle.ptr().durable_jobs.submit(.{
        .owner_id = owner_id,
        .class = .maintenance,
        .ptr = &ctx,
        .run = Fns.run,
        .deinit = Fns.deinit,
    }));

    try std.testing.expect(ctx.ran);
    try std.testing.expect(!ctx.deinit_called);
    Fns.deinit(&ctx);
    try std.testing.expect(ctx.deinit_called);
}

test "backend runtime threaded durable lane sees initialized jobs" {
    if (builtin.os.tag == .freestanding) return error.SkipZigTest;

    const Ctx = struct {
        ran: std.atomic.Value(bool) = .init(false),
        deinit_called: std.atomic.Value(bool) = .init(false),
    };
    const Fns = struct {
        fn run(ptr: *anyopaque) !void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            ctx.ran.store(true, .release);
        }

        fn deinit(ptr: *anyopaque) void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            ctx.deinit_called.store(true, .release);
        }
    };

    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    defer handle.deinit();

    const owner_id = try handle.ptr().allocOwnerId();
    var ctxs: [64]Ctx = [_]Ctx{.{}} ** 64;
    for (&ctxs) |*ctx| {
        try handle.ptr().durable_jobs.submit(.{
            .owner_id = owner_id,
            .class = .commit_durable,
            .ptr = ctx,
            .run = Fns.run,
            .deinit = Fns.deinit,
        });
    }
    handle.ptr().durable_jobs.drainOwner(owner_id);

    for (&ctxs) |*ctx| {
        try std.testing.expect(ctx.ran.load(.acquire));
        try std.testing.expect(ctx.deinit_called.load(.acquire));
    }
}

test "backend runtime allocates stable nonzero owner ids" {
    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .manual });
    defer handle.deinit();

    const first = try handle.ptr().allocOwnerId();
    const second = try handle.ptr().allocOwnerId();

    try std.testing.expect(first != 0);
    try std.testing.expectEqual(first + 1, second);
}

test "backend runtime retires closed owner registry state" {
    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .manual });
    defer handle.deinit();

    const shared_owner_count = handle.ptr().owner_registry.states.count();
    for (0..1024) |_| {
        const owner_id = try handle.ptr().allocOwnerId();
        handle.ptr().durable_jobs.closeOwner(owner_id);
    }
    try std.testing.expectEqual(shared_owner_count, handle.ptr().owner_registry.states.count());
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

    const first_owner_id = try handle.ptr().allocOwnerId();
    const second_owner_id = try handle.ptr().allocOwnerId();
    var first = Ctx{};
    var second = Ctx{};
    try handle.ptr().durable_jobs.submit(.{
        .owner_id = first_owner_id,
        .class = .cleanup,
        .ptr = &first,
        .run = Fns.run,
        .deinit = Fns.deinit,
    });
    try handle.ptr().durable_jobs.submit(.{
        .owner_id = second_owner_id,
        .class = .cleanup,
        .ptr = &second,
        .run = Fns.run,
        .deinit = Fns.deinit,
    });

    handle.ptr().durable_jobs.drainOwner(first_owner_id);
    try std.testing.expectEqual(@as(u32, 1), first.value.load(.monotonic));
    try std.testing.expectEqual(@as(u32, 1), first.deinits.load(.monotonic));

    handle.ptr().durable_jobs.drainOwner(second_owner_id);
    try std.testing.expectEqual(@as(u32, 1), second.value.load(.monotonic));
    try std.testing.expectEqual(@as(u32, 1), second.deinits.load(.monotonic));
}

test "backend runtime threaded durable lane rejects jobs after owner close" {
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

    const owner_id = try handle.ptr().allocOwnerId();
    var ctx = Ctx{};
    try handle.ptr().durable_jobs.submit(.{
        .owner_id = owner_id,
        .class = .maintenance,
        .ptr = &ctx,
        .run = Fns.run,
        .deinit = Fns.deinit,
    });
    handle.ptr().durable_jobs.closeOwner(owner_id);

    try std.testing.expectEqual(@as(u32, 1), ctx.ran.load(.acquire));
    try std.testing.expectEqual(@as(u32, 1), ctx.deinits.load(.acquire));
    try std.testing.expect(!handle.ptr().owner_registry.states.contains(owner_id));
    try std.testing.expectError(error.BackgroundOwnerClosed, handle.ptr().durable_jobs.submit(.{
        .owner_id = owner_id,
        .class = .maintenance,
        .ptr = &ctx,
        .run = Fns.run,
        .deinit = Fns.deinit,
    }));
    try std.testing.expectEqual(@as(u32, 1), ctx.deinits.load(.acquire));
}

test "backend runtime owner close rejects recursive submit from draining job" {
    if (builtin.os.tag == .freestanding) return;

    const Ctx = struct {
        lane: DurableJobLane,
        owner_id: u64,
        started: std.atomic.Value(bool) = .init(false),
        allow_submit: std.atomic.Value(bool) = .init(false),
        submit_rejected: std.atomic.Value(bool) = .init(false),
        run_count: std.atomic.Value(u32) = .init(0),
        deinits: std.atomic.Value(u32) = .init(0),
    };
    const Fns = struct {
        fn run(ptr: *anyopaque) !void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            _ = ctx.run_count.fetchAdd(1, .release);
            ctx.started.store(true, .release);
            while (!ctx.allow_submit.load(.acquire)) {
                std.atomic.spinLoopHint();
            }
            ctx.lane.submit(.{
                .owner_id = ctx.owner_id,
                .class = .maintenance,
                .ptr = ctx,
                .run = run,
                .deinit = deinit,
            }) catch |err| switch (err) {
                error.BackgroundOwnerClosing => {
                    ctx.submit_rejected.store(true, .release);
                    return;
                },
                else => return err,
            };
        }

        fn deinit(ptr: *anyopaque) void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            _ = ctx.deinits.fetchAdd(1, .release);
        }
    };

    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    defer handle.deinit();

    const owner_id = try handle.ptr().allocOwnerId();
    var ctx = Ctx{ .lane = handle.ptr().durable_jobs, .owner_id = owner_id };
    try handle.ptr().durable_jobs.submit(.{
        .owner_id = owner_id,
        .class = .maintenance,
        .ptr = &ctx,
        .run = Fns.run,
        .deinit = Fns.deinit,
    });
    while (!ctx.started.load(.acquire)) {
        std.atomic.spinLoopHint();
    }
    try std.testing.expect(handle.ptr().owner_registry.beginClose(owner_id));
    ctx.allow_submit.store(true, .release);
    handle.ptr().durable_jobs.drainOwner(owner_id);
    handle.ptr().owner_registry.waitIdle(owner_id);
    handle.ptr().owner_registry.retireClosed(owner_id);

    try std.testing.expect(ctx.submit_rejected.load(.acquire));
    const run_count = ctx.run_count.load(.acquire);
    try std.testing.expect(run_count >= 1);
    try std.testing.expectEqual(run_count, ctx.deinits.load(.acquire));
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

    const owner_id = try handle.ptr().allocOwnerId();
    var ctx = Ctx{};
    try handle.ptr().durable_jobs.submit(.{
        .owner_id = owner_id,
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
    handle.ptr().durable_jobs.drainOwner(owner_id);

    try std.testing.expectEqual(@as(u32, 1), ctx.ran.load(.acquire));
    try std.testing.expectEqual(@as(u32, 1), ctx.deinits.load(.acquire));
}

test "backend runtime threaded worker releases payload before reaper joins" {
    if (builtin.os.tag == .freestanding) return;

    const Ctx = struct {
        ran: std.atomic.Value(bool) = .init(false),
        deinit_called: std.atomic.Value(bool) = .init(false),
    };
    const Fns = struct {
        fn run(ptr: *anyopaque) !void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            ctx.ran.store(true, .release);
        }

        fn deinit(ptr: *anyopaque) void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            ctx.deinit_called.store(true, .release);
        }
    };

    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    defer handle.deinit();

    // Prevent both the background reaper and explicit poll from joining the
    // future. The worker must still release the owned payload promptly.
    const jobs = handle.ptr().threaded_jobs.?;
    lockAtomic(&jobs.reap_mutex);
    defer jobs.reap_mutex.unlock();

    const owner_id = try handle.ptr().allocOwnerId();
    var ctx = Ctx{};
    try handle.ptr().durable_jobs.submit(.{
        .owner_id = owner_id,
        .class = .commit_durable,
        .ptr = &ctx,
        .run = Fns.run,
        .deinit = Fns.deinit,
    });

    var attempts: usize = 0;
    while (!ctx.deinit_called.load(.acquire) and attempts < 200) : (attempts += 1) {
        if (handle.ptr().io()) |io| io.sleep(Io.Duration.fromMilliseconds(2), .awake) catch {};
    }
    try std.testing.expect(ctx.ran.load(.acquire));
    try std.testing.expect(ctx.deinit_called.load(.acquire));
}
