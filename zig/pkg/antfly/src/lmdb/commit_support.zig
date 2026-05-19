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
const env_mod = @import("env.zig");
const format = @import("format.zig");
const mutate_leaf = @import("mutate_leaf.zig");
const rebalance_branch = @import("rebalance_branch.zig");
const support = @import("txn_support.zig");

pub const Error = env_mod.Error || std.mem.Allocator.Error || error{
    Unexpected,
    MapFull,
    Incompatible,
};

pub const PageImage = union(enum) {
    leaf: struct {
        pgno: format.Pgno,
        entries: []const mutate_leaf.SerializedLeafEntry,
    },
    leaf2: struct {
        pgno: format.Pgno,
        key_size: u16,
        values: []const []const u8,
    },
    branch: struct {
        pgno: format.Pgno,
        entries: []const rebalance_branch.BranchPageEntry,
    },
    overflow: struct {
        pgno: format.Pgno,
        page_count: u32,
        data: []const u8,
    },
};

pub const CommitPublishPhase = enum {
    before_data_sync,
    after_data_sync_before_meta,
    after_meta_write_before_meta_sync,
    fully_published,
};

pub const SerializedPage = struct {
    offset: usize,
    bytes: []const u8,
};

pub const SerializedWriteSpan = struct {
    offset: usize,
    bytes: []const u8,
    page_count: u32,
};

pub const PreparedCommit = struct {
    arena: std.heap.ArenaAllocator,
    page_size: usize = 0,
    total_size: usize = 0,
    page_images: []const PageImage = &.{},
    serialized_pages: []const SerializedPage = &.{},
    serialized_spans: []const SerializedWriteSpan = &.{},
    meta_pgno: format.Pgno = 0,
    meta_page: []u8 = &.{},

    pub fn deinit(self: *PreparedCommit) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

const PublishFd = struct {
    fd: std.posix.fd_t,
    owned: bool,
};

pub fn openWriteFd(path: [:0]const u8) Error!std.posix.fd_t {
    return std.posix.openat(std.posix.AT.FDCWD, path, .{
        .ACCMODE = .RDWR,
    }, 0);
}

pub fn publishPreparedCommit(env: *env_mod.Environment, prepared: *const PreparedCommit, phase: CommitPublishPhase) Error!void {
    const backend = env.selectedCommitBackend();
    return switch (backend) {
        .sync => publishPreparedCommitSync(env, prepared, phase, .sync),
        .worker_thread => publishPreparedCommitWorkerThread(env, prepared, phase),
        .async_io => publishPreparedCommitAsyncIo(env, prepared, phase),
        .adaptive => unreachable,
    };
}

fn publishPreparedCommitSync(
    env: *env_mod.Environment,
    prepared: *const PreparedCommit,
    phase: CommitPublishPhase,
    selected_backend: env_mod.CommitBackend,
) Error!void {
    const publish_started = nowNs();
    var stats = env_mod.CommitStatsDelta{
        .publish_calls = 1,
        .full_publish_calls = if (phase == .fully_published) 1 else 0,
        .selected_sync_calls = if (selected_backend == .sync) 1 else 0,
        .selected_worker_thread_calls = if (selected_backend == .worker_thread) 1 else 0,
    };
    if (!env.opts.write_map) {
        const publish_fd = try openPublishFd(env);
        defer {
            if (publish_fd.owned) {
                _ = std.posix.system.close(publish_fd.fd);
            }
        }
        try publishPreparedCommitFd(publish_fd.fd, prepared, env.opts, phase, &stats);
        if (phase == .fully_published) try env.refresh();
        stats.total_publish_ns = elapsedSince(publish_started);
        env.recordCommitStats(stats);
        return;
    }

    try ensureFileSize(env, try requiredPublishSize(prepared));

    for (prepared.serialized_spans) |page_span| {
        const write_started = nowNs();
        try writeSerializedSpan(env, page_span, &stats);
        stats.total_page_write_ns += elapsedSince(write_started);
    }
    if (phase == .before_data_sync) {
        stats.total_publish_ns = elapsedSince(publish_started);
        env.recordCommitStats(stats);
        return;
    }

    if (!env.opts.no_sync) {
        const data_sync_started = nowNs();
        try syncData(env);
        stats.data_sync_calls += 1;
        stats.total_data_sync_ns += elapsedSince(data_sync_started);
    }
    if (phase == .after_data_sync_before_meta) {
        stats.total_publish_ns = elapsedSince(publish_started);
        env.recordCommitStats(stats);
        return;
    }

    const meta_write_started = nowNs();
    try writeAllAtOffset(env, prepared.meta_page, prepared.meta_pgno * prepared.page_size);
    stats.bytes_written += prepared.meta_page.len;
    stats.total_meta_write_ns += elapsedSince(meta_write_started);
    if (phase == .after_meta_write_before_meta_sync) {
        stats.total_publish_ns = elapsedSince(publish_started);
        env.recordCommitStats(stats);
        return;
    }

    if (!env.opts.no_sync and !env.opts.no_meta_sync) {
        const meta_sync_started = nowNs();
        try syncMeta(env);
        stats.meta_sync_calls += 1;
        stats.total_meta_sync_ns += elapsedSince(meta_sync_started);
    }
    try env.refresh();
    stats.total_publish_ns = elapsedSince(publish_started);
    env.recordCommitStats(stats);
}

fn publishPreparedCommitWorkerThread(env: *env_mod.Environment, prepared: *const PreparedCommit, phase: CommitPublishPhase) Error!void {
    const Context = struct {
        env: *env_mod.Environment,
        prepared: *const PreparedCommit,
        phase: CommitPublishPhase,
        err: ?Error = null,

        fn run(_: *env_mod.CommitWorker, ctx: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            publishPreparedCommitSync(self.env, self.prepared, self.phase, .worker_thread) catch |err| {
                self.err = err;
                return;
            };
            self.err = null;
        }
    };

    const worker = env.ensureCommitWorker(false) catch |err| switch (err) {
        error.Incompatible => return publishPreparedCommitSync(env, prepared, phase, .sync),
        else => return err,
    };
    var ctx = Context{
        .env = env,
        .prepared = prepared,
        .phase = phase,
    };
    worker.submit(.{
        .ctx = &ctx,
        .run = Context.run,
    });
    if (ctx.err) |err| return err;
}

const AsyncPageWriteResult = struct {
    bytes_written: u64 = 0,
    page_images_written: u64 = 0,
    elapsed_ns: u64 = 0,
};

pub fn serializePageImages(
    allocator: std.mem.Allocator,
    page_size: usize,
    opts: env_mod.EnvironmentOptions,
    page_images: []const PageImage,
) Error![]const SerializedPage {
    const serialized_pages = try allocator.alloc(SerializedPage, page_images.len);
    for (page_images, 0..) |page_image, idx| {
        serialized_pages[idx] = try serializePageImage(allocator, page_size, opts, page_image);
    }
    std.sort.insertion(SerializedPage, serialized_pages, {}, serializedPageLessThan);
    return serialized_pages;
}

pub fn coalesceSerializedPages(
    allocator: std.mem.Allocator,
    serialized_pages: []const SerializedPage,
) Error![]const SerializedWriteSpan {
    if (serialized_pages.len == 0) return &.{};

    const spans = try allocator.alloc(SerializedWriteSpan, serialized_pages.len);
    var span_count: usize = 0;
    var start_idx: usize = 0;
    var idx: usize = 1;
    while (idx <= serialized_pages.len) : (idx += 1) {
        const should_split = idx == serialized_pages.len or
            serialized_pages[idx - 1].offset + serialized_pages[idx - 1].bytes.len != serialized_pages[idx].offset;
        if (!should_split) continue;

        var total_len: usize = 0;
        var page_count: u32 = 0;
        for (serialized_pages[start_idx..idx]) |page| {
            total_len += page.bytes.len;
            page_count += 1;
        }
        const span_bytes = try allocator.alloc(u8, total_len);
        var written: usize = 0;
        for (serialized_pages[start_idx..idx]) |page| {
            @memcpy(span_bytes[written .. written + page.bytes.len], page.bytes);
            written += page.bytes.len;
        }
        spans[span_count] = .{
            .offset = serialized_pages[start_idx].offset,
            .bytes = span_bytes,
            .page_count = page_count,
        };
        span_count += 1;
        start_idx = idx;
    }
    return spans[0..span_count];
}

fn serializedPageLessThan(_: void, lhs: SerializedPage, rhs: SerializedPage) bool {
    return lhs.offset < rhs.offset;
}

fn serializePageImage(
    allocator: std.mem.Allocator,
    page_size: usize,
    opts: env_mod.EnvironmentOptions,
    page_image: PageImage,
) Error!SerializedPage {
    switch (page_image) {
        .leaf => |leaf_page| {
            const page_bytes = try allocator.alloc(u8, page_size);
            try mutate_leaf.writePageOptions(page_bytes, leaf_page.pgno, leaf_page.entries, !opts.no_mem_init);
            return .{ .offset = leaf_page.pgno * page_size, .bytes = page_bytes };
        },
        .leaf2 => |leaf2_page| {
            const page_bytes = try allocator.alloc(u8, page_size);
            try support.writeLeaf2PageOptions(page_bytes, leaf2_page.pgno, leaf2_page.key_size, leaf2_page.values, !opts.no_mem_init);
            return .{ .offset = leaf2_page.pgno * page_size, .bytes = page_bytes };
        },
        .branch => |branch_page| {
            const page_bytes = try allocator.alloc(u8, page_size);
            try rebalance_branch.writePageOptions(page_bytes, branch_page.pgno, branch_page.entries, !opts.no_mem_init);
            return .{ .offset = branch_page.pgno * page_size, .bytes = page_bytes };
        },
        .overflow => |overflow| {
            const page_count = @as(usize, overflow.page_count);
            const page_bytes = try allocator.alloc(u8, page_count * page_size);
            support.writeOverflowPagesOptions(page_bytes, overflow.pgno, overflow.page_count, overflow.data, !opts.no_mem_init);
            return .{ .offset = overflow.pgno * page_size, .bytes = page_bytes };
        },
    }
}

fn openPublishFd(env: *env_mod.Environment) Error!PublishFd {
    if (!env.opts.read_only) {
        return .{ .fd = env.fd, .owned = false };
    }
    return .{
        .fd = try openWriteFd(env.data_path),
        .owned = true,
    };
}

fn publishPreparedCommitAsyncIo(env: *env_mod.Environment, prepared: *const PreparedCommit, phase: CommitPublishPhase) Error!void {
    if (env.opts.write_map) return publishPreparedCommitSync(env, prepared, phase, .sync);
    if (comptime env_mod.uses_evented_async_runtime) {
        return publishPreparedCommitAsyncIoWorkerThread(env, prepared, phase);
    }
    const runtime = env.ensureAsyncRuntime() catch |err| switch (err) {
        error.Incompatible => return publishPreparedCommitSync(env, prepared, phase, .sync),
        else => return err,
    };
    return publishPreparedCommitAsyncIoWithRuntime(env, prepared, phase, runtime);
}

fn publishPreparedCommitAsyncIoWorkerThread(env: *env_mod.Environment, prepared: *const PreparedCommit, phase: CommitPublishPhase) Error!void {
    const Context = struct {
        env: *env_mod.Environment,
        prepared: *const PreparedCommit,
        phase: CommitPublishPhase,
        err: ?Error = null,

        fn run(worker: *env_mod.CommitWorker, ctx: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            const runtime = worker.io_runtime orelse {
                self.err = error.Unexpected;
                return;
            };
            publishPreparedCommitAsyncIoWithRuntime(self.env, self.prepared, self.phase, runtime) catch |err| {
                self.err = err;
                return;
            };
            self.err = null;
        }
    };

    const worker = env.ensureCommitWorker(true) catch |err| switch (err) {
        error.Incompatible => return publishPreparedCommitSync(env, prepared, phase, .sync),
        else => return err,
    };
    var ctx = Context{
        .env = env,
        .prepared = prepared,
        .phase = phase,
    };
    worker.submit(.{
        .ctx = &ctx,
        .run = Context.run,
    });
    if (ctx.err) |err| return err;
}

fn publishPreparedCommitAsyncIoWithRuntime(
    env: *env_mod.Environment,
    prepared: *const PreparedCommit,
    phase: CommitPublishPhase,
    runtime: *env_mod.AsyncRuntimeType,
) Error!void {
    const publish_fd = try openPublishFd(env);
    defer {
        if (publish_fd.owned) {
            _ = std.posix.system.close(publish_fd.fd);
        }
    }
    const io = runtime.io();
    const file = std.Io.File{
        .handle = publish_fd.fd,
        .flags = .{ .nonblocking = false },
    };

    const publish_started = nowNs();
    var stats = env_mod.CommitStatsDelta{
        .publish_calls = 1,
        .full_publish_calls = if (phase == .fully_published) 1 else 0,
        .selected_async_io_calls = 1,
    };

    try ensureFileSizeFd(publish_fd.fd, try requiredPublishSize(prepared));

    var page_write_future = std.Io.async(io, writeAllSerializedPagesAsyncIoTask, .{
        file,
        io,
        prepared.serialized_spans,
    });
    const page_write_result = try page_write_future.await(io);
    stats.page_images_written += page_write_result.page_images_written;
    stats.bytes_written += page_write_result.bytes_written;
    stats.total_page_write_ns += page_write_result.elapsed_ns;
    if (phase == .before_data_sync) {
        stats.total_publish_ns = elapsedSince(publish_started);
        env.recordCommitStats(stats);
        return;
    }

    if (!env.opts.no_sync) {
        const data_sync_started = nowNs();
        try syncFileDataFd(publish_fd.fd);
        try applyArtificialSyncDelay(env.opts.artificial_sync_delay_ns);
        stats.data_sync_calls += 1;
        stats.total_data_sync_ns += elapsedSince(data_sync_started);
    }
    if (phase == .after_data_sync_before_meta) {
        stats.total_publish_ns = elapsedSince(publish_started);
        env.recordCommitStats(stats);
        return;
    }

    const meta_write_started = nowNs();
    try writeAllAtOffsetAsyncIo(file, io, prepared.meta_page, prepared.meta_pgno * prepared.page_size);
    stats.bytes_written += prepared.meta_page.len;
    stats.total_meta_write_ns += elapsedSince(meta_write_started);
    if (phase == .after_meta_write_before_meta_sync) {
        stats.total_publish_ns = elapsedSince(publish_started);
        env.recordCommitStats(stats);
        return;
    }

    if (!env.opts.no_sync and !env.opts.no_meta_sync) {
        const meta_sync_started = nowNs();
        try syncFileAsyncIo(file, io);
        try applyArtificialSyncDelay(env.opts.artificial_sync_delay_ns);
        stats.meta_sync_calls += 1;
        stats.total_meta_sync_ns += elapsedSince(meta_sync_started);
    }
    try env.refresh();
    stats.total_publish_ns = elapsedSince(publish_started);
    env.recordCommitStats(stats);
}

fn publishPreparedCommitFd(
    fd: std.posix.fd_t,
    prepared: *const PreparedCommit,
    opts: env_mod.EnvironmentOptions,
    phase: CommitPublishPhase,
    stats: *env_mod.CommitStatsDelta,
) Error!void {
    try ensureFileSizeFd(fd, try requiredPublishSize(prepared));

    for (prepared.serialized_spans) |page_span| {
        const write_started = nowNs();
        try writeSerializedSpanFd(fd, page_span, stats);
        stats.total_page_write_ns += elapsedSince(write_started);
    }
    if (phase == .before_data_sync) return;

    if (!opts.no_sync) {
        const data_sync_started = nowNs();
        try syncFileDataFd(fd);
        try applyArtificialSyncDelay(opts.artificial_sync_delay_ns);
        stats.data_sync_calls += 1;
        stats.total_data_sync_ns += elapsedSince(data_sync_started);
    }
    if (phase == .after_data_sync_before_meta) return;

    const meta_write_started = nowNs();
    try writeAllAtOffsetFd(fd, prepared.meta_page, prepared.meta_pgno * prepared.page_size);
    stats.bytes_written += prepared.meta_page.len;
    stats.total_meta_write_ns += elapsedSince(meta_write_started);
    if (phase == .after_meta_write_before_meta_sync) return;

    if (!opts.no_sync and !opts.no_meta_sync) {
        const meta_sync_started = nowNs();
        try syncFileFd(fd);
        try applyArtificialSyncDelay(opts.artificial_sync_delay_ns);
        stats.meta_sync_calls += 1;
        stats.total_meta_sync_ns += elapsedSince(meta_sync_started);
    }
}

fn writeSerializedSpanAsyncIoTask(
    file: std.Io.File,
    io: std.Io,
    span: SerializedWriteSpan,
) Error!AsyncPageWriteResult {
    const started = nowNs();
    try writeAllAtOffsetAsyncIo(file, io, span.bytes, span.offset);
    return .{
        .bytes_written = span.bytes.len,
        .page_images_written = span.page_count,
        .elapsed_ns = elapsedSince(started),
    };
}

fn writeAllSerializedPagesAsyncIoTask(
    file: std.Io.File,
    io: std.Io,
    page_spans: []const SerializedWriteSpan,
) Error!AsyncPageWriteResult {
    var result = AsyncPageWriteResult{};
    for (page_spans) |page_span| {
        const each = try writeSerializedSpanAsyncIoTask(file, io, page_span);
        result.bytes_written += each.bytes_written;
        result.page_images_written += each.page_images_written;
        result.elapsed_ns += each.elapsed_ns;
    }
    return result;
}

fn ensureFileSize(env: *env_mod.Environment, size: usize) Error!void {
    if (env.opts.write_map) {
        try env.ensureMappedSize(size);
        return;
    }

    const current_size = std.posix.system.lseek(env.fd, 0, std.posix.SEEK.END);
    if (current_size < 0) return error.Unexpected;
    if (@as(usize, @intCast(current_size)) >= size) return;
    while (true) switch (std.posix.errno(std.posix.system.ftruncate(env.fd, @intCast(size)))) {
        .SUCCESS => return,
        .INTR => continue,
        else => return error.Unexpected,
    };
}

fn ensureFileSizeFd(fd: std.posix.fd_t, size: usize) Error!void {
    const current_size = std.posix.system.lseek(fd, 0, std.posix.SEEK.END);
    if (current_size < 0) return error.Unexpected;
    if (@as(usize, @intCast(current_size)) >= size) return;
    while (true) switch (std.posix.errno(std.posix.system.ftruncate(fd, @intCast(size)))) {
        .SUCCESS => return,
        .INTR => continue,
        else => return error.Unexpected,
    };
}

fn requiredPublishSize(prepared: *const PreparedCommit) Error!usize {
    var size = prepared.total_size;
    for (prepared.serialized_spans) |span| {
        const end = std.math.add(usize, span.offset, span.bytes.len) catch return error.MapFull;
        size = @max(size, end);
    }
    const meta_pgno = std.math.cast(usize, prepared.meta_pgno) orelse return error.MapFull;
    const meta_offset = std.math.mul(usize, meta_pgno, prepared.page_size) catch return error.MapFull;
    const meta_end = std.math.add(usize, meta_offset, prepared.meta_page.len) catch return error.MapFull;
    return @max(size, meta_end);
}

fn writeAllAtOffset(env: *env_mod.Environment, bytes: []const u8, offset: usize) Error!void {
    if (env.opts.write_map) {
        return env.writeMapped(offset, bytes);
    }

    return writeAllAtOffsetFd(env.fd, bytes, offset);
}

fn writeAllAtOffsetFd(fd: std.posix.fd_t, bytes: []const u8, offset: usize) Error!void {
    var written: usize = 0;
    while (written < bytes.len) {
        const rc = std.posix.system.pwrite(fd, bytes.ptr + written, bytes.len - written, @intCast(offset + written));
        switch (std.posix.errno(rc)) {
            .SUCCESS => written += @intCast(rc),
            .INTR => continue,
            else => return error.Unexpected,
        }
    }
}

fn writeAllAtOffsetAsyncIo(file: std.Io.File, io: std.Io, bytes: []const u8, offset: usize) Error!void {
    file.writePositionalAll(io, bytes, offset) catch |err| switch (err) {
        else => return error.Unexpected,
    };
}

fn syncData(env: *env_mod.Environment) Error!void {
    if (env.opts.write_map) {
        try env.syncMapped(env.opts.map_async);
        try applyArtificialSyncDelay(env.opts.artificial_sync_delay_ns);
        return;
    }
    try syncFileDataFd(env.fd);
    try applyArtificialSyncDelay(env.opts.artificial_sync_delay_ns);
}

fn syncMeta(env: *env_mod.Environment) Error!void {
    if (env.opts.write_map) {
        try env.syncMapped(env.opts.map_async);
        try applyArtificialSyncDelay(env.opts.artificial_sync_delay_ns);
        return;
    }
    try syncFileFd(env.fd);
    try applyArtificialSyncDelay(env.opts.artificial_sync_delay_ns);
}

fn syncFileFd(fd: std.posix.fd_t) Error!void {
    while (true) switch (std.posix.errno(std.posix.system.fsync(fd))) {
        .SUCCESS => return,
        .INTR => continue,
        else => return error.Unexpected,
    };
}

fn syncFileDataFd(fd: std.posix.fd_t) Error!void {
    std.posix.fdatasync(fd) catch return error.Unexpected;
}

fn syncFileAsyncIo(file: std.Io.File, io: std.Io) Error!void {
    file.sync(io) catch |err| switch (err) {
        else => return error.Unexpected,
    };
}

fn applyArtificialSyncDelay(delay_ns: u64) Error!void {
    if (delay_ns == 0) return;
    var req = std.posix.timespec{
        .sec = @intCast(delay_ns / std.time.ns_per_s),
        .nsec = @intCast(delay_ns % std.time.ns_per_s),
    };
    while (true) switch (std.posix.errno(std.posix.system.nanosleep(&req, &req))) {
        .SUCCESS => return,
        .INTR => continue,
        else => return error.Unexpected,
    };
}

fn writeSerializedSpan(
    env: *env_mod.Environment,
    span: SerializedWriteSpan,
    stats: *env_mod.CommitStatsDelta,
) Error!void {
    try writeAllAtOffset(env, span.bytes, span.offset);
    stats.page_images_written += span.page_count;
    stats.bytes_written += span.bytes.len;
}

fn writeSerializedSpanFd(
    fd: std.posix.fd_t,
    span: SerializedWriteSpan,
    stats: *env_mod.CommitStatsDelta,
) Error!void {
    try writeAllAtOffsetFd(fd, span.bytes, span.offset);
    stats.page_images_written += span.page_count;
    stats.bytes_written += span.bytes.len;
}

fn nowNs() u64 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(.MONOTONIC, &ts))) {
        .SUCCESS => {},
        else => unreachable,
    }
    return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s + @as(u64, @intCast(ts.nsec));
}

fn elapsedSince(started: u64) u64 {
    return nowNs() - started;
}
