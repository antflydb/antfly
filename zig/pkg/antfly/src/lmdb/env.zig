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
const builtin = @import("builtin");
const build_options = @import("build_options");
const format = @import("format.zig");
const meta = @import("meta.zig");
const page = @import("page.zig");
const readers = @import("readers.zig");
const c = if (builtin.link_libc) @cImport({
    @cInclude("pthread.h");
}) else struct {
    pub const pthread_mutex_t = usize;
    pub const pthread_cond_t = usize;

    pub fn pthread_mutex_init(_: *pthread_mutex_t, _: ?*anyopaque) c_int {
        unreachable;
    }
    pub fn pthread_mutex_destroy(_: *pthread_mutex_t) c_int {
        unreachable;
    }
    pub fn pthread_mutex_lock(_: *pthread_mutex_t) c_int {
        unreachable;
    }
    pub fn pthread_mutex_unlock(_: *pthread_mutex_t) c_int {
        unreachable;
    }
    pub fn pthread_cond_init(_: *pthread_cond_t, _: ?*anyopaque) c_int {
        unreachable;
    }
    pub fn pthread_cond_destroy(_: *pthread_cond_t) c_int {
        unreachable;
    }
    pub fn pthread_cond_wait(_: *pthread_cond_t, _: *pthread_mutex_t) c_int {
        unreachable;
    }
    pub fn pthread_cond_signal(_: *pthread_cond_t) c_int {
        unreachable;
    }
    pub fn pthread_cond_broadcast(_: *pthread_cond_t) c_int {
        unreachable;
    }
};

fn heapAllocator() std.mem.Allocator {
    if (builtin.link_libc) return std.heap.c_allocator;
    return std.heap.smp_allocator;
}

const use_evented_async_runtime =
    build_options.lmdb_evented_async_io and
    builtin.os.tag == .macos and
    std.Io.Evented != void;

const AsyncRuntime = if (use_evented_async_runtime) std.Io.Evented else std.Io.Threaded;
pub const uses_evented_async_runtime = use_evented_async_runtime;
pub const AsyncRuntimeType = AsyncRuntime;

pub const Error = std.posix.OpenError || std.posix.MMapError || std.posix.MadviseError || std.posix.MSyncError || page.Error || readers.Error || error{
    PathTooLong,
    EmptyDataFile,
    SizeOverflow,
    NoValidMetaPages,
    PageOutOfBounds,
    OutOfMemory,
    Incompatible,
};

pub const CommitBackend = enum {
    sync,
    worker_thread,
    async_io,
    adaptive,
};

pub const EnvironmentOptions = struct {
    no_subdir: bool = false,
    read_only: bool = true,
    fixed_map: bool = false,
    write_map: bool = false,
    map_async: bool = false,
    no_read_ahead: bool = false,
    no_sync: bool = false,
    no_meta_sync: bool = false,
    no_tls: bool = false,
    no_lock: bool = false,
    no_mem_init: bool = false,
    defer_page_mutation: bool = false,
    artificial_sync_delay_ns: u64 = 0,
    commit_backend: CommitBackend = .sync,
};

pub const MetaSet = struct {
    meta0: ?meta.Parsed,
    meta1: ?meta.Parsed,
    active: meta.Parsed,
    inactive: ?meta.Parsed,

    pub fn pageSize(self: MetaSet) usize {
        return self.active.pageSize();
    }
};

pub const MappedBytes = []align(std.heap.page_size_min) u8;

pub const CommitStats = struct {
    publish_calls: u64 = 0,
    full_publish_calls: u64 = 0,
    selected_sync_calls: u64 = 0,
    selected_worker_thread_calls: u64 = 0,
    selected_async_io_calls: u64 = 0,
    page_images_written: u64 = 0,
    bytes_written: u64 = 0,
    data_sync_calls: u64 = 0,
    meta_sync_calls: u64 = 0,
    total_page_write_ns: u64 = 0,
    total_data_sync_ns: u64 = 0,
    total_meta_write_ns: u64 = 0,
    total_meta_sync_ns: u64 = 0,
    total_publish_ns: u64 = 0,
};

pub const CommitStatsDelta = CommitStats;

pub const CommitTask = struct {
    ctx: *anyopaque,
    run: *const fn (*CommitWorker, *anyopaque) void,
};

pub const CommitWorker = struct {
    mutex: c.pthread_mutex_t = undefined,
    ready_cond: c.pthread_cond_t = undefined,
    work_cond: c.pthread_cond_t = undefined,
    idle_cond: c.pthread_cond_t = undefined,
    stop: bool = false,
    busy: bool = false,
    ready: bool = false,
    use_async_runtime: bool = false,
    init_err: ?Error = null,
    pending: ?CommitTask = null,
    thread: ?std.Thread = null,
    io_runtime: ?*AsyncRuntime = null,

    pub fn create(use_async_runtime: bool) Error!*CommitWorker {
        if (!builtin.link_libc) return error.Incompatible;
        const alloc = heapAllocator();
        const self = alloc.create(CommitWorker) catch return error.OutOfMemory;
        errdefer alloc.destroy(self);
        self.* = undefined;
        if (c.pthread_mutex_init(&self.mutex, null) != 0) return error.Unexpected;
        errdefer _ = c.pthread_mutex_destroy(&self.mutex);
        if (c.pthread_cond_init(&self.ready_cond, null) != 0) return error.Unexpected;
        errdefer _ = c.pthread_cond_destroy(&self.ready_cond);
        if (c.pthread_cond_init(&self.work_cond, null) != 0) return error.Unexpected;
        errdefer _ = c.pthread_cond_destroy(&self.work_cond);
        if (c.pthread_cond_init(&self.idle_cond, null) != 0) return error.Unexpected;
        errdefer _ = c.pthread_cond_destroy(&self.idle_cond);
        self.stop = false;
        self.busy = false;
        self.ready = false;
        self.use_async_runtime = use_async_runtime;
        self.init_err = null;
        self.pending = null;
        self.thread = null;
        self.io_runtime = null;
        self.thread = std.Thread.spawn(.{}, run, .{self}) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            error.LockedMemoryLimitExceeded, error.SystemResources, error.ThreadQuotaExceeded => return error.OutOfMemory,
            else => return error.Unexpected,
        };
        workerLock(&self.mutex);
        while (!self.ready) {
            workerWait(&self.ready_cond, &self.mutex);
        }
        if (self.init_err) |err| {
            workerUnlock(&self.mutex);
            if (self.thread) |thread| thread.join();
            _ = c.pthread_cond_destroy(&self.idle_cond);
            _ = c.pthread_cond_destroy(&self.work_cond);
            _ = c.pthread_cond_destroy(&self.ready_cond);
            _ = c.pthread_mutex_destroy(&self.mutex);
            alloc.destroy(self);
            return err;
        }
        workerUnlock(&self.mutex);
        return self;
    }

    pub fn destroy(self: *CommitWorker) void {
        workerLock(&self.mutex);
        self.stop = true;
        workerSignal(&self.work_cond);
        workerUnlock(&self.mutex);
        if (self.thread) |thread| thread.join();
        _ = c.pthread_cond_destroy(&self.idle_cond);
        _ = c.pthread_cond_destroy(&self.work_cond);
        _ = c.pthread_cond_destroy(&self.ready_cond);
        _ = c.pthread_mutex_destroy(&self.mutex);
        heapAllocator().destroy(self);
    }

    pub fn submit(self: *CommitWorker, task: CommitTask) void {
        workerLock(&self.mutex);
        defer workerUnlock(&self.mutex);

        while (self.pending != null or self.busy) {
            workerWait(&self.idle_cond, &self.mutex);
        }
        self.pending = task;
        workerSignal(&self.work_cond);

        while (self.pending != null or self.busy) {
            workerWait(&self.idle_cond, &self.mutex);
        }
    }

    fn run(self: *CommitWorker) void {
        var runtime_storage: AsyncRuntime = undefined;
        if (self.use_async_runtime) {
            initAsyncRuntime(&runtime_storage) catch |err| {
                workerLock(&self.mutex);
                self.init_err = err;
                self.ready = true;
                workerBroadcast(&self.ready_cond);
                workerUnlock(&self.mutex);
                return;
            };
            self.io_runtime = &runtime_storage;
        }
        workerLock(&self.mutex);
        self.ready = true;
        workerBroadcast(&self.ready_cond);
        workerUnlock(&self.mutex);
        defer if (self.use_async_runtime) {
            deinitAsyncRuntime(&runtime_storage);
            self.io_runtime = null;
        };

        while (true) {
            workerLock(&self.mutex);
            while (self.pending == null and !self.stop) {
                workerWait(&self.work_cond, &self.mutex);
            }
            if (self.stop and self.pending == null) {
                workerUnlock(&self.mutex);
                return;
            }
            const task = self.pending.?;
            self.pending = null;
            self.busy = true;
            workerUnlock(&self.mutex);

            task.run(self, task.ctx);

            workerLock(&self.mutex);
            self.busy = false;
            workerBroadcast(&self.idle_cond);
            workerUnlock(&self.mutex);
        }
    }
};

pub const Environment = struct {
    fd: std.posix.fd_t,
    mapped: MappedBytes,
    metas: MetaSet,
    data_path: [:0]u8,
    opts: EnvironmentOptions,
    reader_registry: ?readers.Registry = null,
    commit_worker: ?*CommitWorker = null,
    io_runtime: ?*AsyncRuntime = null,
    mapping_mutex: std.atomic.Mutex = .unlocked,
    local_readers: usize = 0,
    retired_mappings: std.ArrayListUnmanaged(MappedBytes) = .empty,
    commit_resource_mutex: std.atomic.Mutex = .unlocked,
    adaptive_mutex: std.atomic.Mutex = .unlocked,
    adaptive_backend: CommitBackend = .sync,
    adaptive_recheck_after: u64 = 0,
    commit_stats_mutex: std.atomic.Mutex = .unlocked,
    commit_stats: CommitStats = .{},

    pub fn open(path: []const u8, opts: EnvironmentOptions) Error!Environment {
        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const data_path = try dataFilePath(&path_buf, path, opts);
        const flags: std.posix.O = .{
            .ACCMODE = if (opts.read_only) .RDONLY else .RDWR,
        };

        const fd = try std.posix.openat(std.posix.AT.FDCWD, data_path, flags, 0);
        errdefer _ = std.posix.system.close(fd);

        const file_len = try fileLength(fd);
        if (file_len == 0) return error.EmptyDataFile;

        var mapped = try std.posix.mmap(
            null,
            file_len,
            .{
                .READ = true,
                .WRITE = !opts.read_only,
            },
            .{ .TYPE = .SHARED },
            fd,
            0,
        );
        errdefer std.posix.munmap(mapped);

        if (opts.no_read_ahead) {
            try std.posix.madvise(mapped.ptr, mapped.len, std.posix.MADV.RANDOM);
        }

        const alloc = heapAllocator();
        const data_path_owned = alloc.dupeZ(u8, data_path) catch return error.OutOfMemory;
        errdefer alloc.free(data_path_owned);

        var metas = try selectMetas(mapped);
        if (opts.fixed_map) {
            if (metas.active.meta.mm_address) |map_address| {
                const current_address: *anyopaque = @ptrCast(mapped.ptr);
                if (current_address != map_address) {
                    std.posix.munmap(mapped);
                    mapped = try mapFile(fd, file_len, opts, @ptrCast(@alignCast(map_address)));
                    metas = try selectMetas(mapped);
                    const remapped_address: *anyopaque = @ptrCast(mapped.ptr);
                    if (remapped_address != map_address) return error.Incompatible;
                }
            }
        }
        var reader_registry: ?readers.Registry = null;
        if (!opts.no_lock and !opts.no_tls) {
            reader_registry = try readers.Registry.open(data_path_owned);
        }
        return .{
            .fd = fd,
            .mapped = mapped,
            .metas = metas,
            .data_path = data_path_owned,
            .opts = opts,
            .reader_registry = reader_registry,
        };
    }

    pub fn close(self: *Environment) void {
        if (self.io_runtime) |runtime| {
            deinitAsyncRuntime(runtime);
            heapAllocator().destroy(runtime);
        }
        if (self.commit_worker) |worker| worker.destroy();
        if (self.reader_registry) |*registry| registry.close();
        self.releaseRetiredMappings();
        std.posix.munmap(self.mapped);
        _ = std.posix.system.close(self.fd);
        heapAllocator().free(self.data_path);
        self.* = undefined;
    }

    pub fn refresh(self: *Environment) Error!void {
        const file_len = try fileLength(self.fd);
        if (file_len == 0) return error.EmptyDataFile;

        if (file_len != self.mapped.len) {
            if (self.opts.fixed_map) {
                const preferred_address = self.mapped.ptr;
                std.posix.munmap(self.mapped);
                self.mapped = try mapFile(
                    self.fd,
                    file_len,
                    self.opts,
                    preferred_address,
                );
            } else {
                const remapped = try mapFile(
                    self.fd,
                    file_len,
                    self.opts,
                    null,
                );
                errdefer std.posix.munmap(remapped);
                try self.installRemappedMapping(remapped);
            }
        }

        self.metas = try selectMetas(self.mapped);
    }

    pub fn ensureMappedSize(self: *Environment, size: usize) Error!void {
        if (self.mapped.len >= size) return;
        if (self.opts.read_only) return error.Incompatible;

        while (true) switch (std.posix.errno(std.posix.system.ftruncate(self.fd, @intCast(size)))) {
            .SUCCESS => break,
            .INTR => continue,
            else => return error.Unexpected,
        };
        if (self.opts.fixed_map) {
            const preferred_address = self.mapped.ptr;
            std.posix.munmap(self.mapped);
            self.mapped = try mapFile(
                self.fd,
                size,
                self.opts,
                preferred_address,
            );
        } else {
            const remapped = try mapFile(
                self.fd,
                size,
                self.opts,
                null,
            );
            errdefer std.posix.munmap(remapped);
            try self.installRemappedMapping(remapped);
        }
        self.metas = try selectMetas(self.mapped);
    }

    pub fn writeMapped(self: *Environment, offset: usize, bytes: []const u8) Error!void {
        const end = std.math.add(usize, offset, bytes.len) catch return error.PageOutOfBounds;
        if (end > self.mapped.len) return error.PageOutOfBounds;
        @memcpy(self.mapped[offset..end], bytes);
    }

    pub fn syncMapped(self: *Environment, async: bool) Error!void {
        try std.posix.msync(self.mapped, if (async) std.posix.MSF.ASYNC else std.posix.MSF.SYNC);
    }

    pub fn data(self: *const Environment) []const u8 {
        return self.mapped;
    }

    pub fn localReaderEnter(self: *Environment) void {
        lockAtomic(&self.mapping_mutex);
        defer self.mapping_mutex.unlock();
        self.local_readers += 1;
    }

    pub fn localReaderLeave(self: *Environment) void {
        lockAtomic(&self.mapping_mutex);
        defer self.mapping_mutex.unlock();
        std.debug.assert(self.local_readers > 0);
        self.local_readers -= 1;
        if (self.local_readers == 0) self.releaseRetiredMappingsLocked();
    }

    pub fn pageSize(self: *const Environment) usize {
        return self.metas.pageSize();
    }

    pub fn activeMeta(self: *const Environment) meta.Parsed {
        return self.metas.active;
    }

    pub fn inactiveMeta(self: *const Environment) ?meta.Parsed {
        return self.metas.inactive;
    }

    pub fn pageBytes(self: *const Environment, pgno: format.Pgno) Error![]const u8 {
        const offset = std.math.mul(usize, pgno, self.pageSize()) catch return error.PageOutOfBounds;
        const end = std.math.add(usize, offset, self.pageSize()) catch return error.PageOutOfBounds;
        if (end > self.mapped.len) return error.PageOutOfBounds;
        return self.mapped[offset..end];
    }

    pub fn pageView(self: *const Environment, pgno: format.Pgno) Error!page.View {
        return page.View.init(try self.pageBytes(pgno));
    }

    pub fn commitStatsSnapshot(self: *Environment) CommitStats {
        lockAtomic(&self.commit_stats_mutex);
        defer self.commit_stats_mutex.unlock();
        return self.commit_stats;
    }

    pub fn recordCommitStats(self: *Environment, delta: CommitStatsDelta) void {
        lockAtomic(&self.commit_stats_mutex);
        defer self.commit_stats_mutex.unlock();
        self.commit_stats.publish_calls += delta.publish_calls;
        self.commit_stats.full_publish_calls += delta.full_publish_calls;
        self.commit_stats.selected_sync_calls += delta.selected_sync_calls;
        self.commit_stats.selected_worker_thread_calls += delta.selected_worker_thread_calls;
        self.commit_stats.selected_async_io_calls += delta.selected_async_io_calls;
        self.commit_stats.page_images_written += delta.page_images_written;
        self.commit_stats.bytes_written += delta.bytes_written;
        self.commit_stats.data_sync_calls += delta.data_sync_calls;
        self.commit_stats.meta_sync_calls += delta.meta_sync_calls;
        self.commit_stats.total_page_write_ns += delta.total_page_write_ns;
        self.commit_stats.total_data_sync_ns += delta.total_data_sync_ns;
        self.commit_stats.total_meta_write_ns += delta.total_meta_write_ns;
        self.commit_stats.total_meta_sync_ns += delta.total_meta_sync_ns;
        self.commit_stats.total_publish_ns += delta.total_publish_ns;
    }

    pub fn ensureCommitWorker(self: *Environment, use_async_runtime: bool) Error!*CommitWorker {
        if (self.opts.read_only) return error.Incompatible;
        lockAtomic(&self.commit_resource_mutex);
        defer self.commit_resource_mutex.unlock();
        if (self.commit_worker == null) {
            self.commit_worker = try CommitWorker.create(use_async_runtime);
        }
        return self.commit_worker.?;
    }

    pub fn ensureAsyncRuntime(self: *Environment) Error!*AsyncRuntime {
        if (self.opts.read_only) return error.Incompatible;
        lockAtomic(&self.commit_resource_mutex);
        defer self.commit_resource_mutex.unlock();
        if (self.io_runtime == null) {
            const runtime = heapAllocator().create(AsyncRuntime) catch return error.OutOfMemory;
            errdefer heapAllocator().destroy(runtime);
            try initAsyncRuntime(runtime);
            self.io_runtime = runtime;
        }
        return self.io_runtime.?;
    }

    pub fn selectedCommitBackend(self: *Environment) CommitBackend {
        return switch (self.opts.commit_backend) {
            .adaptive => self.selectAdaptiveCommitBackendCached(),
            else => self.opts.commit_backend,
        };
    }

    fn selectAdaptiveCommitBackendCached(self: *Environment) CommitBackend {
        const stats = self.commitStatsSnapshot();
        lockAtomic(&self.adaptive_mutex);
        defer self.adaptive_mutex.unlock();
        if (stats.publish_calls < self.adaptive_recheck_after) {
            return self.adaptive_backend;
        }
        self.adaptive_backend = self.computeAdaptiveCommitBackend(stats);
        self.adaptive_recheck_after = stats.publish_calls + 32;
        return self.adaptive_backend;
    }

    fn computeAdaptiveCommitBackend(self: *Environment, stats: CommitStats) CommitBackend {
        if (self.opts.write_map or self.opts.no_sync or self.opts.no_meta_sync) return .sync;
        if (comptime use_evented_async_runtime) return .sync;
        if (stats.publish_calls < 16) return .sync;

        const avg_publish_ns = stats.total_publish_ns / stats.publish_calls;
        // Stay on the direct path unless publish cost is meaningfully high.
        // Repeated stress runs showed that ~5ms publish latency is still not enough
        // on its own to justify switching off sync for the current single-writer flow.
        if (avg_publish_ns < 8 * std.time.ns_per_ms) return .sync;

        const full_calls = if (stats.full_publish_calls == 0) stats.publish_calls else stats.full_publish_calls;
        const avg_page_write_ns = stats.total_page_write_ns / stats.publish_calls;
        const avg_sync_ns = (stats.total_data_sync_ns + stats.total_meta_sync_ns) / full_calls;

        if (avg_sync_ns >= 8 * std.time.ns_per_ms and avg_sync_ns * 2 >= avg_page_write_ns * 3) {
            return .worker_thread;
        }
        if (avg_page_write_ns > avg_sync_ns and avg_publish_ns >= 10 * std.time.ns_per_ms) {
            return .async_io;
        }
        return .sync;
    }

    fn installRemappedMapping(self: *Environment, remapped: MappedBytes) Error!void {
        lockAtomic(&self.mapping_mutex);
        defer self.mapping_mutex.unlock();

        const previous = self.mapped;
        if (self.local_readers == 0) {
            self.mapped = remapped;
            std.posix.munmap(previous);
            return;
        }

        try self.retired_mappings.append(heapAllocator(), previous);
        self.mapped = remapped;
    }

    fn releaseRetiredMappings(self: *Environment) void {
        lockAtomic(&self.mapping_mutex);
        defer self.mapping_mutex.unlock();
        self.releaseRetiredMappingsLocked();
        self.retired_mappings.deinit(heapAllocator());
    }

    fn releaseRetiredMappingsLocked(self: *Environment) void {
        for (self.retired_mappings.items) |mapping| std.posix.munmap(mapping);
        self.retired_mappings.clearRetainingCapacity();
    }
};

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) {
        std.Thread.yield() catch {};
    }
}

fn workerLock(mutex: *c.pthread_mutex_t) void {
    if (c.pthread_mutex_lock(mutex) != 0) unreachable;
}

fn workerUnlock(mutex: *c.pthread_mutex_t) void {
    if (c.pthread_mutex_unlock(mutex) != 0) unreachable;
}

fn workerWait(cond: *c.pthread_cond_t, mutex: *c.pthread_mutex_t) void {
    if (c.pthread_cond_wait(cond, mutex) != 0) unreachable;
}

fn workerSignal(cond: *c.pthread_cond_t) void {
    if (c.pthread_cond_signal(cond) != 0) unreachable;
}

fn workerBroadcast(cond: *c.pthread_cond_t) void {
    if (c.pthread_cond_broadcast(cond) != 0) unreachable;
}

pub fn initAsyncRuntime(runtime: *AsyncRuntime) Error!void {
    if (comptime use_evented_async_runtime) {
        try AsyncRuntime.init(runtime, heapAllocator(), .{});
    } else {
        runtime.* = AsyncRuntime.init(heapAllocator(), .{});
    }
}

pub fn deinitAsyncRuntime(runtime: *AsyncRuntime) void {
    runtime.deinit();
}

fn dataFilePath(buf: []u8, path: []const u8, opts: EnvironmentOptions) Error![]const u8 {
    if (opts.no_subdir) return path;
    return std.fmt.bufPrint(buf, "{s}/data.mdb", .{path}) catch error.PathTooLong;
}

fn mapFile(
    fd: std.posix.fd_t,
    len: usize,
    opts: EnvironmentOptions,
    preferred_address: ?[*]align(std.heap.page_size_min) u8,
) Error![]align(std.heap.page_size_min) u8 {
    return std.posix.mmap(
        preferred_address,
        len,
        .{
            .READ = true,
            .WRITE = !opts.read_only,
        },
        .{ .TYPE = .SHARED },
        fd,
        0,
    );
}

fn fileLength(fd: std.posix.fd_t) Error!usize {
    const size = std.posix.system.lseek(fd, 0, std.posix.SEEK.END);
    if (size < 0) return error.Unexpected;
    return std.math.cast(usize, size) orelse error.SizeOverflow;
}

fn selectMetas(mapped: []const u8) Error!MetaSet {
    var meta0 = parseMetaCandidate(mapped, 0, 0);
    var meta1: ?meta.Parsed = null;

    if (meta0) |m0| {
        meta1 = parseMetaCandidate(mapped, m0.pageSize(), 1);
        if (meta1) |m1| {
            if (m1.pageSize() != m0.pageSize()) {
                meta1 = null;
            }
        }
    }

    if (meta0 == null or meta1 == null) {
        var candidate_page_size: usize = 512;
        const max_candidate = @min(format.max_pagesize, mapped.len / format.num_metas);
        while (candidate_page_size <= max_candidate) : (candidate_page_size *= 2) {
            if (meta0 == null) {
                if (parseMetaCandidate(mapped, 0, 0)) |candidate| {
                    if (candidate.pageSize() == candidate_page_size) meta0 = candidate;
                }
            }
            if (meta1 == null) {
                if (parseMetaCandidate(mapped, candidate_page_size, 1)) |candidate| {
                    if (candidate.pageSize() == candidate_page_size) meta1 = candidate;
                }
            }
            if (meta0 != null and meta1 != null) break;
        }
    }

    if (meta0 == null and meta1 == null) return error.NoValidMetaPages;

    const active = if (meta0) |m0|
        if (meta1) |m1| meta.newer(m0, m1) else m0
    else
        meta1.?;

    const inactive = if (meta0) |m0|
        if (meta1) |m1|
            if (active.txnid() == m0.txnid()) m1 else m0
        else
            null
    else
        null;

    return .{
        .meta0 = meta0,
        .meta1 = meta1,
        .active = active,
        .inactive = inactive,
    };
}

fn parseMetaCandidate(mapped: []const u8, offset: usize, expected_pgno: format.Pgno) ?meta.Parsed {
    if (offset >= mapped.len) return null;
    const candidate = meta.parse(mapped[offset..]) catch return null;
    if (candidate.header.mp_pgno != expected_pgno) return null;
    if (candidate.pageSize() == 0) return null;

    const page_size = candidate.pageSize();
    if (page_size > mapped.len) return null;
    const page_end = std.math.add(usize, offset, page_size) catch return null;
    if (page_end > mapped.len) return null;
    return candidate;
}

fn writeMetaPage(page_bytes: []u8, pgno: format.Pgno, page_size: u32, txnid: format.Txnid) void {
    @memset(page_bytes, 0);
    const hdr = format.PageHeader{
        .mp_pgno = pgno,
        .mp_pad = 0,
        .mp_flags = format.PageFlags.meta,
        .mp_lower = 0,
        .mp_upper = 0,
    };
    format.writeStruct(format.PageHeader, page_bytes[0..format.page_header_size], hdr);

    const free_db = format.Db{
        .md_pad = page_size,
        .md_flags = 0,
        .md_depth = 0,
        .md_branch_pages = 0,
        .md_leaf_pages = 0,
        .md_overflow_pages = 0,
        .md_entries = 0,
        .md_root = format.invalid_pgno,
    };
    const main_db = format.Db{
        .md_pad = 0,
        .md_flags = 0,
        .md_depth = 0,
        .md_branch_pages = 0,
        .md_leaf_pages = 0,
        .md_overflow_pages = 0,
        .md_entries = 0,
        .md_root = format.invalid_pgno,
    };
    const meta_value = format.Meta{
        .mm_magic = format.mdb_magic,
        .mm_version = format.mdb_data_version,
        .mm_address = null,
        .mm_mapsize = page_bytes.len,
        .mm_dbs = .{ free_db, main_db },
        .mm_last_pg = format.num_metas - 1,
        .mm_txnid = txnid,
    };
    format.writeStruct(format.Meta, page_bytes[format.page_header_size..][0..format.meta_body_size], meta_value);
}

test "environment opens directory-backed data file and selects newest meta" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var env_dir = try tmp.dir.createDirPathOpen(std.testing.io, "env", .{});
    defer env_dir.close(std.testing.io);

    var bytes: [4096 * 2]u8 = undefined;
    writeMetaPage(bytes[0..4096], 0, 4096, 3);
    writeMetaPage(bytes[4096..8192], 1, 4096, 7);
    try env_dir.writeFile(std.testing.io, .{ .sub_path = "data.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const env_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/env", .{tmp.sub_path});

    var env = try Environment.open(env_path, .{});
    defer env.close();

    try std.testing.expectEqual(@as(usize, 4096), env.pageSize());
    try std.testing.expectEqual(@as(format.Txnid, 7), env.activeMeta().txnid());
    try std.testing.expect(env.inactiveMeta() != null);
    try std.testing.expectEqual(@as(format.Txnid, 3), env.inactiveMeta().?.txnid());
    try std.testing.expectEqual(page.Kind.meta, (try env.pageView(1)).kind());
}

test "environment opens MDB_NOSUBDIR-style file path" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var bytes: [2048 * 2]u8 = undefined;
    writeMetaPage(bytes[0..2048], 0, 2048, 1);
    writeMetaPage(bytes[2048..4096], 1, 2048, 2);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "single.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/single.mdb", .{tmp.sub_path});

    var env = try Environment.open(file_path, .{ .no_subdir = true });
    defer env.close();

    try std.testing.expectEqual(@as(usize, 2048), env.pageSize());
    try std.testing.expectEqual(@as(format.Txnid, 2), env.activeMeta().txnid());
    try std.testing.expectEqual(page.Kind.meta, (try env.pageView(0)).kind());
}

test "environment supports no_read_ahead advisory" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var bytes: [2048 * 2]u8 = undefined;
    writeMetaPage(bytes[0..2048], 0, 2048, 1);
    writeMetaPage(bytes[2048..4096], 1, 2048, 2);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "random.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/random.mdb", .{tmp.sub_path});

    var env = try Environment.open(file_path, .{
        .no_subdir = true,
        .no_read_ahead = true,
    });
    defer env.close();

    try std.testing.expectEqual(@as(format.Txnid, 2), env.activeMeta().txnid());
}

test "environment rejects files without valid meta pages" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const bytes = [_]u8{0} ** 4096;
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "broken.mdb", .data = &bytes });

    var path_buf: [256]u8 = undefined;
    const file_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/broken.mdb", .{tmp.sub_path});

    try std.testing.expectError(error.NoValidMetaPages, Environment.open(file_path, .{ .no_subdir = true }));
}
