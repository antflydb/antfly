// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! CPU-cost companion to `write_amplification_bench_test.zig`.
//!
//! `write_amplification_bench_test.zig` (and its `zig build
//! lite-write-amplification` step) measures Lite's write path with
//! `std.testing.allocator`, whose `std.heap.DebugAllocator` defaults to
//! capturing a stack trace on every single allocation and free in Debug
//! builds (`default_sys_stack_trace_frames = 6` when
//! `std.debug.sys_can_stack_trace`, see `lib/std/heap/debug_allocator.zig`).
//! That makes the existing harness's own
//! `indicative_writes_per_second_debug` number dominated by allocator
//! bookkeeping rather than by the write path it is trying to measure: ~125ms
//! per single-document write in Debug is far more than one small commit
//! should cost even unoptimized.
//!
//! This file re-opens the same public embedding seam
//! (`capi.openLiteHandleWithRuntime`) for just the `primary_only` and
//! `full_text` modes (the two write_amplification_bench_test.zig covers that
//! this file's task is scoped to), but:
//!
//!   - uses `std.heap.DebugAllocator(.{ .stack_trace_frames = 0 })` instead
//!     of `std.testing.allocator`, matching the pattern already used for the
//!     same reason in `storage/db/db.zig` (grep
//!     `DebugAllocator(.{ .stack_trace_frames = 0 })`) -- keeps safety
//!     checks (double-free/use-after-free/leak detection) but removes the
//!     per-allocation unwind.
//!   - counts allocator calls (alloc/resize/remap/free) through a thin
//!     counting wrapper, so "allocations/write" is a real, build-mode-
//!     independent number instead of an estimate.
//!   - counts raw syscalls (pwrite, pread, fsync, open/create, flock) the
//!     same way `write_amplification_bench_test.zig`'s `CountingIo` counts
//!     sync/write, but broader (pread/open/flock too), again build-mode
//!     independent.
//!   - measures CPU time (not wall clock) via `getrusage(RUSAGE_SELF)`
//!     (user+system across the whole process, so it is correct for both the
//!     single-writer and the 8-writer modes -- see `cpuNowNs` below), the
//!     one number in this file's report that *is* sensitive to build mode
//!     and machine load, exactly as `write_amplification_bench_test.zig`'s
//!     own `indicative_writes_per_second_debug` is labeled "indicative
//!     only".
//!
//! See `docs/design/lite-performance/README.md` for how this relates to the
//! syscall/op-count harness and the SQLite-matched throughput comparison.

const std = @import("std");
const api = @import("../../capi/db.zig");
const capi_types = @import("../../capi/types.zig");
const db_mod = api.db_mod;
const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;

const default_updates: usize = 128;
const default_writers: usize = 8;
const text_bytes: usize = 1024;

// ---------------------------------------------------------------------------
// CPU time: process-wide user+system CPU, not wall clock. Correct for both
// the single-writer modes (one thread doing all the work) and the 8-writer
// modes (getrusage(RUSAGE_SELF) sums all threads in the calling process), so
// one function covers every mode this file measures. Unlike wall-clock
// timing, this is not diluted by scheduler contention from the rest of this
// (possibly loaded, shared) host.
// ---------------------------------------------------------------------------

fn cpuNowNs() u64 {
    const usage = std.posix.getrusage(std.posix.rusage.SELF);
    const user_ns: i128 = @as(i128, usage.utime.sec) * std.time.ns_per_s + @as(i128, usage.utime.usec) * 1000;
    const sys_ns: i128 = @as(i128, usage.stime.sec) * std.time.ns_per_s + @as(i128, usage.stime.usec) * 1000;
    return @intCast(@max(0, user_ns + sys_ns));
}

// ---------------------------------------------------------------------------
// Allocator call counting: wraps a child allocator (the CPU-friendly
// DebugAllocator below) and counts alloc/resize/remap/free calls and bytes
// requested, independent of build mode. `std.heap.DebugAllocator` itself
// does not expose a call-count API, only byte totals when
// `enable_memory_limit` is set, so this thin wrapper is the simplest way to
// get "allocations/write" as a real count rather than an estimate.
// ---------------------------------------------------------------------------

const CountingAllocator = struct {
    child: std.mem.Allocator,
    allocs: std.atomic.Value(u64) = .init(0),
    resizes: std.atomic.Value(u64) = .init(0),
    remaps: std.atomic.Value(u64) = .init(0),
    frees: std.atomic.Value(u64) = .init(0),
    bytes_requested: std.atomic.Value(u64) = .init(0),

    fn allocator(self: *CountingAllocator) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn reset(self: *CountingAllocator) void {
        self.allocs.store(0, .monotonic);
        self.resizes.store(0, .monotonic);
        self.remaps.store(0, .monotonic);
        self.frees.store(0, .monotonic);
        self.bytes_requested.store(0, .monotonic);
    }

    const vtable: std.mem.Allocator.VTable = .{
        .alloc = allocImpl,
        .resize = resizeImpl,
        .remap = remapImpl,
        .free = freeImpl,
    };

    fn allocImpl(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        const result = self.child.vtable.alloc(self.child.ptr, len, alignment, ret_addr);
        if (result != null) {
            _ = self.allocs.fetchAdd(1, .monotonic);
            _ = self.bytes_requested.fetchAdd(len, .monotonic);
        }
        return result;
    }
    fn resizeImpl(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        const ok = self.child.vtable.resize(self.child.ptr, memory, alignment, new_len, ret_addr);
        if (ok) _ = self.resizes.fetchAdd(1, .monotonic);
        return ok;
    }
    fn remapImpl(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        const result = self.child.vtable.remap(self.child.ptr, memory, alignment, new_len, ret_addr);
        if (result != null) _ = self.remaps.fetchAdd(1, .monotonic);
        return result;
    }
    fn freeImpl(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        self.child.vtable.free(self.child.ptr, memory, alignment, ret_addr);
        _ = self.frees.fetchAdd(1, .monotonic);
    }
};

// CPU-friendly allocator: real safety checks (double-free, use-after-free,
// leak detection), but no per-allocation stack-trace capture. Matches
// `storage/db/db.zig`'s existing `DebugAllocator(.{ .stack_trace_frames = 0
// })` instantiation.
var cpu_gpa: std.heap.DebugAllocator(.{ .stack_trace_frames = 0 }) = .init;
var counting = CountingAllocator{ .child = undefined };
var counting_ready = false;

fn cpuAllocator() std.mem.Allocator {
    if (!counting_ready) {
        counting = .{ .child = cpu_gpa.allocator() };
        counting_ready = true;
    }
    return counting.allocator();
}

// ---------------------------------------------------------------------------
// Syscall counting: broader than write_amplification_bench_test.zig's own
// CountingIo (which only tracks fsync/pwrite for its stage-attribution
// report) -- this also counts pread, file open/create and flock, so
// "syscalls/write by type" in this file's report is complete for the task
// of attributing per-write CPU to subsystems (an open() or flock() per write
// is itself a CPU/syscall cost even when it issues no durable I/O).
// ---------------------------------------------------------------------------

const SyscallCounts = struct {
    pwrite: std.atomic.Value(u64) = .init(0),
    pread: std.atomic.Value(u64) = .init(0),
    fsync: std.atomic.Value(u64) = .init(0),
    open: std.atomic.Value(u64) = .init(0),
    flock: std.atomic.Value(u64) = .init(0),
    write_bytes: std.atomic.Value(u64) = .init(0),

    fn reset(self: *SyscallCounts) void {
        self.pwrite.store(0, .monotonic);
        self.pread.store(0, .monotonic);
        self.fsync.store(0, .monotonic);
        self.open.store(0, .monotonic);
        self.flock.store(0, .monotonic);
        self.write_bytes.store(0, .monotonic);
    }
};
var syscalls: SyscallCounts = .{};

var counted_vtable: std.Io.VTable = undefined;
var counted_vtable_ready = std.atomic.Value(bool).init(false);

const CountingShim = struct {
    fn fileSync(userdata: ?*anyopaque, file: std.Io.File) std.Io.File.SyncError!void {
        _ = userdata;
        try std.testing.io.vtable.fileSync(std.testing.io.userdata, file);
        _ = syscalls.fsync.fetchAdd(1, .monotonic);
    }
    fn fileWritePositional(
        userdata: ?*anyopaque,
        file: std.Io.File,
        header: []const u8,
        data: []const []const u8,
        splat: usize,
        offset: u64,
    ) std.Io.File.WritePositionalError!usize {
        _ = userdata;
        const written = try std.testing.io.vtable.fileWritePositional(std.testing.io.userdata, file, header, data, splat, offset);
        _ = syscalls.pwrite.fetchAdd(1, .monotonic);
        _ = syscalls.write_bytes.fetchAdd(written, .monotonic);
        return written;
    }
    fn fileReadPositional(
        userdata: ?*anyopaque,
        file: std.Io.File,
        data: []const []u8,
        offset: u64,
    ) std.Io.File.ReadPositionalError!usize {
        _ = userdata;
        const got = try std.testing.io.vtable.fileReadPositional(std.testing.io.userdata, file, data, offset);
        _ = syscalls.pread.fetchAdd(1, .monotonic);
        return got;
    }
    fn dirOpenFile(
        userdata: ?*anyopaque,
        dir: std.Io.Dir,
        sub_path: []const u8,
        options: std.Io.Dir.OpenFileOptions,
    ) std.Io.File.OpenError!std.Io.File {
        _ = userdata;
        const f = try std.testing.io.vtable.dirOpenFile(std.testing.io.userdata, dir, sub_path, options);
        _ = syscalls.open.fetchAdd(1, .monotonic);
        return f;
    }
    fn dirCreateFile(
        userdata: ?*anyopaque,
        dir: std.Io.Dir,
        sub_path: []const u8,
        options: std.Io.Dir.CreateFileOptions,
    ) std.Io.File.OpenError!std.Io.File {
        _ = userdata;
        const f = try std.testing.io.vtable.dirCreateFile(std.testing.io.userdata, dir, sub_path, options);
        _ = syscalls.open.fetchAdd(1, .monotonic);
        return f;
    }
    fn fileLock(userdata: ?*anyopaque, file: std.Io.File, lock: std.Io.File.Lock) std.Io.File.LockError!void {
        _ = userdata;
        try std.testing.io.vtable.fileLock(std.testing.io.userdata, file, lock);
        _ = syscalls.flock.fetchAdd(1, .monotonic);
    }
    fn fileTryLock(userdata: ?*anyopaque, file: std.Io.File, lock: std.Io.File.Lock) std.Io.File.LockError!bool {
        _ = userdata;
        const ok = try std.testing.io.vtable.fileTryLock(std.testing.io.userdata, file, lock);
        if (ok) _ = syscalls.flock.fetchAdd(1, .monotonic);
        return ok;
    }
};

// Only ever called from the harness's own single setup thread, before any
// writer threads are spawned, same precondition as
// write_amplification_bench_test.zig's `countingIo`.
fn countingIo() std.Io {
    if (!counted_vtable_ready.load(.monotonic)) {
        counted_vtable = std.testing.io.vtable.*;
        counted_vtable.fileSync = CountingShim.fileSync;
        counted_vtable.fileWritePositional = CountingShim.fileWritePositional;
        counted_vtable.fileReadPositional = CountingShim.fileReadPositional;
        counted_vtable.dirOpenFile = CountingShim.dirOpenFile;
        counted_vtable.dirCreateFile = CountingShim.dirCreateFile;
        counted_vtable.fileLock = CountingShim.fileLock;
        counted_vtable.fileTryLock = CountingShim.fileTryLock;
        counted_vtable_ready.store(true, .monotonic);
    }
    return .{ .userdata = std.testing.io.userdata, .vtable = &counted_vtable };
}

// ---------------------------------------------------------------------------
// Fixture: a durable Lite handle through the same public embedding seam the
// SDKs reach through the C ABI, opened with the CPU-friendly allocator and
// the syscall-counting std.Io above. Only `primary_only`/`full_text` -- the
// two modes this file is scoped to.
// ---------------------------------------------------------------------------

const Mode = enum { primary_only, full_text };

const Fixture = struct {
    alloc: std.mem.Allocator,
    directory: TestDirectory,
    path: []const u8,
    runtime: db_mod.background_runtime.BackendRuntimeHandle,
    handle: *anyopaque,

    fn open(alloc: std.mem.Allocator, comptime name: []const u8, mode: Mode) !Fixture {
        var directory = try TestDirectory.init(name);
        errdefer directory.cleanup();
        try std.Io.Dir.cwd().createDirPath(std.testing.io, directory.path());
        const path = try std.fmt.allocPrint(alloc, "{s}/db.aflite", .{directory.path()});
        errdefer alloc.free(path);

        var runtime = try db_mod.background_runtime.BackendRuntimeHandle.init(alloc, .{
            .backend = .manual,
            .borrowed_io = .{ .general = countingIo() },
        });
        errdefer runtime.deinit();

        const handle = try api.openLiteHandleWithRuntime(alloc, path, countingIo(), runtime.ptr(), .{
            .create = true,
            .hosted = false,
            .no_sync = false,
        });

        if (mode != .full_text) {
            var removed = false;
            try std.testing.expectEqual(capi_types.ErrorCode.ok, api.antfly_db_delete_index(handle, .fromSlice("full_text_index_v0"), &removed));
        }

        return .{ .alloc = alloc, .directory = directory, .path = path, .runtime = runtime, .handle = handle };
    }

    fn close(self: *Fixture) void {
        api.antfly_db_close(self.handle);
        self.runtime.deinit();
        self.alloc.free(self.path);
        self.directory.cleanup();
    }
};

fn payload(buffer: []u8, worker: usize, sequence: usize) []const u8 {
    const prefix = std.fmt.bufPrint(buffer, "matchedworker{d}sequence{d} ", .{ worker, sequence }) catch unreachable;
    var offset = prefix.len;
    const filler = "searchable churn alpha beta gamma delta ";
    while (offset < text_bytes) {
        const remaining = text_bytes - offset;
        const chunk = if (remaining < filler.len) filler[0..remaining] else filler;
        @memcpy(buffer[offset .. offset + chunk.len], chunk);
        offset += chunk.len;
    }
    return buffer[0..text_bytes];
}

fn jsonString(alloc: std.mem.Allocator, value: []const u8) ![]const u8 {
    return std.json.Stringify.valueAlloc(alloc, value, .{});
}

fn insertOne(alloc: std.mem.Allocator, handle: *anyopaque, key: []const u8, text: []const u8, sync_level: []const u8) !void {
    const key_json = try jsonString(alloc, key);
    defer alloc.free(key_json);
    const text_json = try jsonString(alloc, text);
    defer alloc.free(text_json);
    const sync_level_json = try jsonString(alloc, sync_level);
    defer alloc.free(sync_level_json);
    const body = try std.fmt.allocPrint(alloc, "{{\"inserts\":{{{s}:{{\"body\":{s}}}}},\"sync_level\":{s}}}", .{
        key_json,
        text_json,
        sync_level_json,
    });
    defer alloc.free(body);
    var output: capi_types.Buffer = .{};
    defer api.antfly_buffer_free(&output);
    try std.testing.expectEqual(capi_types.ErrorCode.ok, api.antfly_db_batch_json(handle, .fromSlice(body), &output));
}

// ---------------------------------------------------------------------------
// Measurement
// ---------------------------------------------------------------------------

const CpuReport = struct {
    updates: usize,
    cpu_ns: u64,
    allocs: u64,
    resizes: u64,
    remaps: u64,
    frees: u64,
    bytes_requested: u64,
    pwrite: u64,
    pread: u64,
    fsync: u64,
    open: u64,
    flock: u64,
    write_bytes: u64,
    elapsed_ns: u64,

    fn cpuUsPerWrite(self: CpuReport) f64 {
        return @as(f64, @floatFromInt(self.cpu_ns)) / 1000.0 / @as(f64, @floatFromInt(self.updates));
    }
    fn allocsPerWrite(self: CpuReport) f64 {
        return @as(f64, @floatFromInt(self.allocs)) / @as(f64, @floatFromInt(self.updates));
    }
    fn freesPerWrite(self: CpuReport) f64 {
        return @as(f64, @floatFromInt(self.frees)) / @as(f64, @floatFromInt(self.updates));
    }
    fn bytesRequestedPerWrite(self: CpuReport) f64 {
        return @as(f64, @floatFromInt(self.bytes_requested)) / @as(f64, @floatFromInt(self.updates));
    }
    fn writeBytesPerWrite(self: CpuReport) f64 {
        return @as(f64, @floatFromInt(self.write_bytes)) / @as(f64, @floatFromInt(self.updates));
    }
    fn syscallsPerWrite(self: CpuReport) f64 {
        const total = self.pwrite + self.pread + self.fsync + self.open + self.flock;
        return @as(f64, @floatFromInt(total)) / @as(f64, @floatFromInt(self.updates));
    }
    fn writesPerSecondIndicative(self: CpuReport) f64 {
        if (self.elapsed_ns == 0) return 0;
        const seconds = @as(f64, @floatFromInt(self.elapsed_ns)) / 1_000_000_000.0;
        if (seconds <= 0) return 0;
        return @as(f64, @floatFromInt(self.updates)) / seconds;
    }
};

fn snapshotReport(updates: usize, cpu_ns: u64, elapsed_ns: u64) CpuReport {
    return .{
        .updates = updates,
        .cpu_ns = cpu_ns,
        .allocs = counting.allocs.load(.monotonic),
        .resizes = counting.resizes.load(.monotonic),
        .remaps = counting.remaps.load(.monotonic),
        .frees = counting.frees.load(.monotonic),
        .bytes_requested = counting.bytes_requested.load(.monotonic),
        .pwrite = syscalls.pwrite.load(.monotonic),
        .pread = syscalls.pread.load(.monotonic),
        .fsync = syscalls.fsync.load(.monotonic),
        .open = syscalls.open.load(.monotonic),
        .flock = syscalls.flock.load(.monotonic),
        .write_bytes = syscalls.write_bytes.load(.monotonic),
        .elapsed_ns = elapsed_ns,
    };
}

fn resetCounters() void {
    counting.reset();
    syscalls.reset();
}

fn measureSingleWriter(alloc: std.mem.Allocator, comptime name: []const u8, mode: Mode, updates: usize) !CpuReport {
    var fixture = try Fixture.open(alloc, name, mode);
    defer fixture.close();

    const sync_level: []const u8 = if (mode == .full_text) "full_index" else "write";
    var buffer: [text_bytes]u8 = undefined;
    var key_buffer: [32]u8 = undefined;
    resetCounters();
    const cpu_start = cpuNowNs();
    const wall_start = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
    for (0..updates) |i| {
        const key = try std.fmt.bufPrint(&key_buffer, "doc-{d}", .{i % 128});
        const text = payload(&buffer, 0, i);
        try insertOne(alloc, fixture.handle, key, text, sync_level);
    }
    const wall_elapsed: u64 = @intCast(@max(0, std.Io.Clock.awake.now(std.testing.io).nanoseconds - wall_start));
    const cpu_elapsed = cpuNowNs() - cpu_start;
    return snapshotReport(updates, cpu_elapsed, wall_elapsed);
}

const Barrier = struct {
    participants: usize,
    count: std.atomic.Value(usize) = .init(0),
    generation: std.atomic.Value(usize) = .init(0),

    fn wait(self: *Barrier) void {
        const gen = self.generation.load(.acquire);
        const arrived = self.count.fetchAdd(1, .acq_rel) + 1;
        if (arrived == self.participants) {
            self.count.store(0, .release);
            _ = self.generation.fetchAdd(1, .release);
        } else {
            while (self.generation.load(.acquire) == gen) std.Thread.yield() catch {};
        }
    }
};

const ConcurrentWorker = struct {
    alloc: std.mem.Allocator,
    handle: *anyopaque,
    worker: usize,
    updates_per_writer: usize,
    sync_level: []const u8,
    barrier: *Barrier,
    err: ?anyerror = null,

    fn run(self: *ConcurrentWorker) void {
        var buffer: [text_bytes]u8 = undefined;
        var key_buffer: [32]u8 = undefined;
        for (0..self.updates_per_writer) |i| {
            self.barrier.wait();
            const key = std.fmt.bufPrint(&key_buffer, "w{d}-doc-{d}", .{ self.worker, i % 128 }) catch |err| {
                self.err = err;
                return;
            };
            const text = payload(&buffer, self.worker, i);
            insertOne(self.alloc, self.handle, key, text, self.sync_level) catch |err| {
                self.err = err;
                return;
            };
        }
    }
};

fn measureConcurrent(alloc: std.mem.Allocator, comptime name: []const u8, mode: Mode, writers: usize, updates_per_writer: usize) !CpuReport {
    var fixture = try Fixture.open(alloc, name, mode);
    defer fixture.close();

    resetCounters();
    var barrier = Barrier{ .participants = writers };
    var workers = try alloc.alloc(ConcurrentWorker, writers);
    defer alloc.free(workers);
    var threads = try alloc.alloc(std.Thread, writers);
    defer alloc.free(threads);
    const sync_level: []const u8 = if (mode == .full_text) "full_index" else "write";
    for (0..writers) |w| workers[w] = .{
        .alloc = alloc,
        .handle = fixture.handle,
        .worker = w,
        .updates_per_writer = updates_per_writer,
        .sync_level = sync_level,
        .barrier = &barrier,
    };
    const cpu_start = cpuNowNs();
    const wall_start = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
    for (0..writers) |w| threads[w] = try std.Thread.spawn(.{}, ConcurrentWorker.run, .{&workers[w]});
    for (threads) |thread| thread.join();
    const wall_elapsed: u64 = @intCast(@max(0, std.Io.Clock.awake.now(std.testing.io).nanoseconds - wall_start));
    const cpu_elapsed = cpuNowNs() - cpu_start;
    for (workers) |worker| if (worker.err) |err| return err;
    return snapshotReport(writers * updates_per_writer, cpu_elapsed, wall_elapsed);
}

fn printReport(comptime label: []const u8, report: CpuReport) void {
    std.debug.print(
        "  {s}: updates={d} cpu_us/write={d:.1} allocs/write={d:.2} frees/write={d:.2} " ++
            "resizes/write={d:.2} bytes_requested/write={d:.0} write_bytes/write={d:.0} " ++
            "syscalls/write={d:.2} (pwrite={d:.2} pread={d:.2} fsync={d:.2} open={d:.2} flock={d:.2}) " ++
            "writes/s(indicative)={d:.1}\n",
        .{
            label,
            report.updates,
            report.cpuUsPerWrite(),
            report.allocsPerWrite(),
            report.freesPerWrite(),
            @as(f64, @floatFromInt(report.resizes)) / @as(f64, @floatFromInt(report.updates)),
            report.bytesRequestedPerWrite(),
            report.writeBytesPerWrite(),
            report.syscallsPerWrite(),
            @as(f64, @floatFromInt(report.pwrite)) / @as(f64, @floatFromInt(report.updates)),
            @as(f64, @floatFromInt(report.pread)) / @as(f64, @floatFromInt(report.updates)),
            @as(f64, @floatFromInt(report.fsync)) / @as(f64, @floatFromInt(report.updates)),
            @as(f64, @floatFromInt(report.open)) / @as(f64, @floatFromInt(report.updates)),
            @as(f64, @floatFromInt(report.flock)) / @as(f64, @floatFromInt(report.updates)),
            report.writesPerSecondIndicative(),
        },
    );
}

const ModeReportJson = struct {
    updates: usize,
    cpu_us_per_write: f64,
    allocations_per_write: f64,
    frees_per_write: f64,
    resizes_per_write: f64,
    bytes_requested_per_write: f64,
    write_bytes_per_write: f64,
    syscalls_per_write: f64,
    pwrite_per_write: f64,
    pread_per_write: f64,
    fsync_per_write: f64,
    open_per_write: f64,
    flock_per_write: f64,
    indicative_writes_per_second_debug: f64,
};

fn modeJson(report: CpuReport) ModeReportJson {
    return .{
        .updates = report.updates,
        .cpu_us_per_write = report.cpuUsPerWrite(),
        .allocations_per_write = report.allocsPerWrite(),
        .frees_per_write = report.freesPerWrite(),
        .resizes_per_write = @as(f64, @floatFromInt(report.resizes)) / @as(f64, @floatFromInt(report.updates)),
        .bytes_requested_per_write = report.bytesRequestedPerWrite(),
        .write_bytes_per_write = report.writeBytesPerWrite(),
        .syscalls_per_write = report.syscallsPerWrite(),
        .pwrite_per_write = @as(f64, @floatFromInt(report.pwrite)) / @as(f64, @floatFromInt(report.updates)),
        .pread_per_write = @as(f64, @floatFromInt(report.pread)) / @as(f64, @floatFromInt(report.updates)),
        .fsync_per_write = @as(f64, @floatFromInt(report.fsync)) / @as(f64, @floatFromInt(report.updates)),
        .open_per_write = @as(f64, @floatFromInt(report.open)) / @as(f64, @floatFromInt(report.updates)),
        .flock_per_write = @as(f64, @floatFromInt(report.flock)) / @as(f64, @floatFromInt(report.updates)),
        .indicative_writes_per_second_debug = report.writesPerSecondIndicative(),
    };
}

const ReportJson = struct {
    text_bytes: usize,
    primary_only: ModeReportJson,
    primary_only_8_writers: ModeReportJson,
    full_text: ModeReportJson,
    full_text_8_writers: ModeReportJson,
};

test "lite write amplification cpu report" {
    // `cpu_gpa` is a single process-wide global shared with the "ceilings"
    // test below (and, if the test runner batches both into one process,
    // this is sequential, not concurrent use) -- it is intentionally never
    // deinitialized here. Calling `.deinit()` once this test's measurements
    // are done would poison the allocator for whichever test the runner
    // executes next in the same process (deinit tears down the safety
    // bookkeeping `cpuAllocator()` hands out a live reference to), which is
    // exactly the hang this file shipped with initially: the second test to
    // run froze inside an alloc call against an already-deinitialized GPA.
    // The process exit at the end of the test binary reclaims everything,
    // so skipping a leak check here (unlike `std.testing.allocator`'s
    // automatic one) costs nothing this diagnostic harness needs.
    const alloc = cpuAllocator();
    const updates = default_updates;

    const primary = try measureSingleWriter(alloc, "lite-write-amp-cpu-primary", .primary_only, updates);
    const primary_concurrent = try measureConcurrent(alloc, "lite-write-amp-cpu-primary-concurrent", .primary_only, default_writers, updates / default_writers);
    const full_text = try measureSingleWriter(alloc, "lite-write-amp-cpu-full-text", .full_text, updates);
    const full_text_concurrent = try measureConcurrent(alloc, "lite-write-amp-cpu-full-text-concurrent", .full_text, default_writers, updates / default_writers);

    std.debug.print("lite write amplification CPU report (updates={d}, text_bytes={d}):\n", .{ updates, text_bytes });
    printReport("primary-only", primary);
    printReport("primary-only, 8 writers", primary_concurrent);
    printReport("full-text", full_text);
    printReport("full-text, 8 writers", full_text_concurrent);

    const report: ReportJson = .{
        .text_bytes = text_bytes,
        .primary_only = modeJson(primary),
        .primary_only_8_writers = modeJson(primary_concurrent),
        .full_text = modeJson(full_text),
        .full_text_8_writers = modeJson(full_text_concurrent),
    };
    const json_text = try std.json.Stringify.valueAlloc(alloc, report, .{ .whitespace = .indent_2 });
    defer alloc.free(json_text);
    std.debug.print("lite_write_amplification_cpu_json_begin\n{s}\nlite_write_amplification_cpu_json_end\n", .{json_text});
}

// Diagnostic-only (not part of the report/ceilings gates): isolates
// one much longer steady-state primary-only loop purely so an external
// sampler (`sample` on macOS, `perf record` on Linux) attached to this
// process's PID has several seconds of real, representative work to
// capture instead of the report/ceilings tests' sub-second run. Not
// filtered into `lite-write-amplification-cpu`'s default run; invoke
// directly with `--test-filter "lite write amplification cpu profile loop"`.
test "lite write amplification cpu profile loop" {
    const alloc = cpuAllocator();
    const profile_updates: usize = 6000;
    const report = try measureSingleWriter(alloc, "lite-write-amp-cpu-profile", .primary_only, profile_updates);
    printReport("profile loop: primary-only", report);
}

// Regression gate: allocations/write and syscalls/write are build-mode
// independent (they are operation counts, not timings), so -- unlike
// cpu_us_per_write, which this test deliberately does not gate on -- they
// make a real ceiling. Numbers start at the as-measured baseline plus 25%
// slack, matching write_amplification_bench_test.zig's existing convention
// for its own ceilings test. Tighten these as the write path's allocation
// count comes down.
test "lite write amplification cpu ceilings" {
    // See the "lite write amplification cpu report" test above: `cpu_gpa` is
    // a shared global and must not be deinitialized mid-process.
    const alloc = cpuAllocator();
    const updates = default_updates;

    // Measured baseline (two runs on an unloaded-for-this-process, but
    // host-shared, machine): primary-only ~816-899 allocs/write, ~30-31
    // syscalls/write; full-text ~1355-1426 allocs/write, ~39-39.5
    // syscalls/write. Ceilings below are the higher observed value plus 25%
    // slack.
    const primary = try measureSingleWriter(alloc, "lite-write-amp-cpu-ceil-primary", .primary_only, updates);
    try std.testing.expect(primary.allocsPerWrite() < 1125.0);
    try std.testing.expect(primary.syscallsPerWrite() < 39.0);

    const full_text = try measureSingleWriter(alloc, "lite-write-amp-cpu-ceil-fulltext", .full_text, updates);
    try std.testing.expect(full_text.allocsPerWrite() < 1785.0);
    try std.testing.expect(full_text.syscallsPerWrite() < 50.0);
}
